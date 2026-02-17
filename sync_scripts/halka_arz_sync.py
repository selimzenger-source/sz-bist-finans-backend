#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Halka Arz Tavan/Taban Takip — Excel -> Backend Canli Sync
==========================================================

Her 15 saniyede bir Matriks Excel'den canli fiyat verilerini okuyup
tavan/taban durumunu analiz eder, 5 bildirim tipini yonetir.

Excel Formati (Matriks — 12 sutun):
  A: ILK ISLEM  (22.Oca.26 gibi)
  B: HISSE      (AKHAN, NETCD, UCAYM)
  C: TAVAN      (Tavan limit fiyati)
  D: TABAN      (Taban limit fiyati)
  E: ALIS       (Alis Kademe fiyati — bos/#YOK olabilir)
  F: SATIS      (Satis Kademe fiyati — bos/#YOK olabilir)
  G: SON        (Son fiyat / Kapanis)
  H: %G FARK    (Gunluk % degisim)
  I: TARIH      (Verinin tarihi)
  J: G.EN YUKSEK (Gun ici en yuksek fiyat)
  K: ALIS LOT   (1. kademe alis lotu — canli)
  L: SATIS LOT  (1. kademe satis lotu — canli)

5 Bildirim Servisi:
  1. TAVAN ACILINCA / KiTLENiNCE (tavan_bozulma)  — 10 TL / 5 TL ilk HA
     - Tavana kitlendi → bildirim
     - Tavan cozuldu → bildirim
     - 5dk gecti kilitleyemedi → bildirim
     - Tavana kitledi (tekrar) → ayni mesaj

  2. TABAN ACILINCA (taban_acilma)  — 10 TL / 5 TL ilk HA
     - Tabana kitlendi → bildirim
     - Taban cozuldu → bildirim
     - 5dk gecti kilitleyemedi → bildirim
     - Tabana kitlendi (tekrar) → ayni mesaj

  3. GUNLUK ACILIS / KAPANIS (gunluk_acilis_kapanis)  — 5 TL / 3 TL ilk HA
     - 09:56 acilis bildirimi: "AKHAN tavan acti!" / "normal islem ile acildi"
     - 18:08 kapanis bildirimi: "AKHAN tavan kapatti!" / "normal islem ile kapatti"

  4. EN YUKSEGINDEN % DUSUS (yuzde_dusus)  — 20 TL / 10 TL ilk HA
     - Tek hizmet, 2 esik bildirimi:
       a) %3-4 dustugunde → 1. bildirim (sub_event: pct4)
       b) %6-7 dustugunde → 2. bildirim (sub_event: pct7)
     - Gunde max 2 bildirim (once %4, sonra %7)

ONEMLI: Bildirim mesajlarinda FIYAT BILGISI YOKTUR!

NOT: win32com ile ACIK OLAN Excel'den canli veri okur (Matriks DDE/RTD).
"""

import sys
import os
import time
import datetime as dt
import requests
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

# Windows encoding fix
if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass

# Excel okuma icin win32com (pywin32) gerekli
try:
    import win32com.client
    USE_WIN32COM = True
except ImportError:
    USE_WIN32COM = False
    print("=" * 60)
    print("HATA: pywin32 kurulu degil!")
    print("Kurulum: pip install pywin32")
    print("=" * 60)
    sys.exit(1)


# ============================================
# AYARLAR
# ============================================

# Excel dosya yolu
EXCEL_FILE_PATH = r"C:\Users\PC\Desktop\halka arz TAVAN TABAN.xlsm"

# Backend API endpointleri
API_BASE_URL = os.getenv("BIST_API_URL", "https://sz-bist-finans-api.onrender.com")
API_NOTIF_URL = f"{API_BASE_URL}/api/v1/realtime-notification"
API_CEILING_URL = f"{API_BASE_URL}/api/v1/ceiling-track"

# Admin sifresi
ADMIN_PASSWORD = os.getenv("BIST_ADMIN_PW", "")

# Sync araligi (saniye) — 15 saniyede bir
SYNC_INTERVAL = 15

# Tavan/taban bozulma sonrasi bekleme suresi (saniye) — 5 dakika
RELOCK_WAIT_SECONDS = 300

# Piyasa calisma saatleri
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 56   # Acilis bildirimi saati
SEANS_START_HOUR = 10
SEANS_START_MIN = 0
SEANS_END_HOUR = 18
SEANS_END_MIN = 10
KAPANIS_HOUR = 18
KAPANIS_MIN = 8        # Kapanis bildirimi saati

# Retry ayarlari
RETRY_DELAY = 5
MAX_RETRIES = 2

# Tavan/taban eslesme toleransi (kurus)
PRICE_TOLERANCE = 0.02


# ============================================
# EXCEL SUTUN YAPILANDIRMASI (Matriks 10 sutun)
# ============================================

# A: ILK ISLEM  (index 0 — kullanilmiyor, bilgi amacli)
# B: HISSE      (index 1)
# C: TAVAN      (index 2)
# D: TABAN      (index 3)
# E: ALIS       (index 4 — Alis Kademe fiyati)
# F: SATIS      (index 5 — Satis Kademe fiyati)
# G: SON        (index 6 — Son islem fiyati)
# H: %G FARK    (index 7 — Gunluk yuzde degisim)
# I: TARIH      (index 8)
# J: G.EN YUKSEK (index 9 — Gun ici en yuksek fiyat, %4/%7 dusus icin)

ILK_ISLEM_SUTUN = "A"
HISSE_SUTUN = "B"
TAVAN_SUTUN = "C"
TABAN_SUTUN = "D"
ALIS_KADEME_SUTUN = "E"
SATIS_KADEME_SUTUN = "F"
SON_FIYAT_SUTUN = "G"
GUN_FARK_SUTUN = "H"
TARIH_SUTUN = "I"
GUN_EN_YUKSEK_SUTUN = "J"
ALIS_LOT_SUTUN = "K"       # 1. kademe alis lotu
SATIS_LOT_SUTUN = "L"      # 1. kademe satis lotu

BASLIK_SATIR = 1
VERI_BASLANGIC = 2
MAX_SATIR = 50  # Halka arz hisse sayisi siniri


# ============================================
# VERI YAPILARI
# ============================================

@dataclass
class StockState:
    """Bir hissenin anlik durumu."""
    ticker: str
    tavan: float = 0.0
    taban: float = 0.0
    son_fiyat: float = 0.0
    alis_kademe: Optional[str] = None   # Alis kademe fiyati veya "#YOK"/None
    satis_kademe: Optional[str] = None  # Satis kademe fiyati veya "#YOK"/None
    gun_fark: float = 0.0              # Gunluk % degisim
    gun_en_yuksek: float = 0.0         # Gun ici en yuksek fiyat (J sutunu)
    alis_lot: int = 0                  # 1. kademe alis lotu (K sutunu)
    satis_lot: int = 0                 # 1. kademe satis lotu (L sutunu)
    is_ceiling_locked: bool = False     # Tavana kilitli mi?
    is_floor_locked: bool = False       # Tabana kilitli mi?
    tarih: Optional[dt.date] = None    # Excel I sutunu — verinin tarihi


@dataclass
class TrackingState:
    """Bir hissenin takip durumu — bildirim gecmisi."""
    ticker: str

    # --- Tavan takibi ---
    was_ceiling_locked: bool = False      # Onceki durumda tavana kilitli miydi?
    ceiling_broke_at: Optional[dt.datetime] = None  # Tavan ne zaman bozuldu?
    notified_ceiling_first_lock: bool = False  # Ilk tavana kilit bildirimi
    notified_ceiling_break: bool = False  # Tavan cozuldu bildirimi
    notified_ceiling_5min: bool = False   # 5dk gecti bildirimi
    notified_relock_ceiling: bool = False # Tekrar tavana kitledi bildirimi
    last_ceiling_notif_at: Optional[dt.datetime] = None  # Son tavan bildirimi zamani (cooldown icin)
    ceiling_5min_checked: bool = False  # 5dk kontrol yapildi mi?

    # --- Taban takibi ---
    was_floor_locked: bool = False
    floor_broke_at: Optional[dt.datetime] = None
    notified_floor_first_lock: bool = False
    notified_floor_break: bool = False
    notified_floor_5min: bool = False
    notified_relock_floor: bool = False
    last_floor_notif_at: Optional[dt.datetime] = None  # Son taban bildirimi zamani (cooldown icin)
    floor_5min_checked: bool = False  # 5dk kontrol yapildi mi?

    # --- %4/%7 dusus takibi ---
    day_high: float = 0.0               # Gunun en yuksek fiyati
    notified_drop_4pct: bool = False     # %4 dusus bildirimi gonderildi mi? (gunde 1 kez)
    notified_drop_7pct: bool = False     # %7 dusus bildirimi gonderildi mi? (gunde 1 kez)

    # --- Acilis/kapanis ---
    opening_notified: bool = False       # Acilis bildirimi gonderildi mi?
    closing_notified: bool = False       # Kapanis bildirimi gonderildi mi?

    # --- Genel ---
    trading_day: int = 1                 # Kacinci islem gunu
    day_open_price: float = 0.0          # Gunun acilis fiyati
    first_read_done: bool = False        # Gun icin ilk okuma yapildi mi?


# Global takip durumu — her hisse icin
tracking_states: dict[str, TrackingState] = {}


# ============================================
# LOG YONETIMI
# ============================================

LOG_FILE = Path(__file__).parent / "halka_arz_sync.log"


def log(msg: str):
    """Konsol + dosyaya log."""
    ts = dt.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
    except Exception:
        pass


# ============================================
# EXCEL OKUMA (win32com — canli Matriks verisi)
# ============================================

def read_excel_data() -> list[StockState]:
    """
    WIN32COM ile ACIK OLAN Excel'den halka arz hisselerini oku.
    Matriks DDE/RTD canli verilerini alir.

    Sutunlar: A=ILK ISLEM, B=HISSE, C=TAVAN, D=TABAN, E=ALIS, F=SATIS, G=SON, H=%GFARK, I=TARIH
    """
    try:
        excel = win32com.client.GetObject(Class="Excel.Application")
        file_name = Path(EXCEL_FILE_PATH).name

        wb = None
        for workbook in excel.Workbooks:
            if workbook.Name.lower() == file_name.lower():
                wb = workbook
                break

        if wb is None:
            return []

        sheet = wb.ActiveSheet
        stocks = []
        satir = VERI_BASLANGIC

        while satir <= MAX_SATIR + VERI_BASLANGIC:
            # B: HISSE
            ticker = sheet.Range(f"{HISSE_SUTUN}{satir}").Value
            if ticker is None or str(ticker).strip() == "":
                break

            ticker = str(ticker).strip().upper()

            # C: TAVAN, D: TABAN
            tavan = safe_float(sheet.Range(f"{TAVAN_SUTUN}{satir}").Value)
            taban = safe_float(sheet.Range(f"{TABAN_SUTUN}{satir}").Value)

            # E: ALIS KADEME, F: SATIS KADEME
            alis_kademe_val = sheet.Range(f"{ALIS_KADEME_SUTUN}{satir}").Value
            satis_kademe_val = sheet.Range(f"{SATIS_KADEME_SUTUN}{satir}").Value

            # G: SON FIYAT
            son_fiyat = safe_float(sheet.Range(f"{SON_FIYAT_SUTUN}{satir}").Value)

            # H: %G FARK
            gun_fark = safe_float(sheet.Range(f"{GUN_FARK_SUTUN}{satir}").Value)

            # J: G.EN YUKSEK (gun ici en yuksek fiyat)
            gun_en_yuksek = safe_float(sheet.Range(f"{GUN_EN_YUKSEK_SUTUN}{satir}").Value)

            # I: TARIH (verinin tarihi — borsa kapali gunu kontrol icin)
            tarih_val = sheet.Range(f"{TARIH_SUTUN}{satir}").Value
            tarih_date = None
            if tarih_val is not None:
                try:
                    if isinstance(tarih_val, dt.datetime):
                        tarih_date = tarih_val.date()
                    elif hasattr(tarih_val, 'date'):
                        # pywintypes.datetime — .date() metodu var
                        tarih_date = tarih_val.date()
                    elif isinstance(tarih_val, (int, float)):
                        # Excel serial date number
                        import math
                        tarih_date = dt.datetime(1899, 12, 30) + dt.timedelta(days=int(tarih_val))
                        tarih_date = tarih_date.date()
                    elif isinstance(tarih_val, str):
                        # String olarak gelirse parse et
                        tarih_val = tarih_val.strip()
                        for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                            try:
                                tarih_date = dt.datetime.strptime(tarih_val.split('.')[0], fmt).date()
                                break
                            except ValueError:
                                continue
                except Exception:
                    pass

            # K: ALIS LOT, L: SATIS LOT (1. kademe lot verileri)
            alis_lot = int(safe_float(sheet.Range(f"{ALIS_LOT_SUTUN}{satir}").Value))
            satis_lot = int(safe_float(sheet.Range(f"{SATIS_LOT_SUTUN}{satir}").Value))

            # Kademe degerlerini parse et
            alis_kademe = parse_kademe(alis_kademe_val)
            satis_kademe = parse_kademe(satis_kademe_val)

            # Tavan/Taban kilit tespiti
            is_ceiling_locked = check_ceiling_lock(tavan, satis_kademe, alis_kademe)
            is_floor_locked = check_floor_lock(taban, satis_kademe, alis_kademe)

            stock = StockState(
                ticker=ticker,
                tavan=tavan,
                taban=taban,
                son_fiyat=son_fiyat,
                alis_kademe=alis_kademe,
                satis_kademe=satis_kademe,
                gun_fark=gun_fark,
                gun_en_yuksek=gun_en_yuksek,
                alis_lot=alis_lot,
                satis_lot=satis_lot,
                is_ceiling_locked=is_ceiling_locked,
                is_floor_locked=is_floor_locked,
                tarih=tarih_date,
            )
            stocks.append(stock)
            satir += 1

        return stocks

    except Exception as e:
        error_msg = str(e)
        if "operation unavailable" in error_msg.lower() or "moniker" in error_msg.lower():
            pass  # Excel kapali, sessizce gec
        else:
            log(f"Excel okuma hatasi: {e}")
        return []


def safe_float(val) -> float:
    """Guvenli float donusumu."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def parse_kademe(val) -> Optional[str]:
    """Kademe degerini parse et. #YOK, None, veya fiyat degeri."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s == "0" or s == "0.0":
        return None
    return s


def is_kademe_empty(kademe: Optional[str]) -> bool:
    """Kademe degeri bos/yok mu? Tavan/taban kilit tespiti icin."""
    if kademe is None:
        return True
    s = kademe.upper().strip()
    return s in ("", "#YOK", "#N/A", "YOK", "-", "0", "0.0")


def check_ceiling_lock(tavan: float, satis_kademe: Optional[str], alis_kademe: Optional[str]) -> bool:
    """Tavana kilitli mi?

    KURAL:
    - Satis Kademe = #YOK (veya None/bos)  → Satici yok
    - Alis Kademe = Tavan fiyati ile esit    → Alicilar tavan fiyatindan bekliyor
    → Bu durumda TAVANA KILITLI
    """
    if tavan <= 0:
        return False

    # Satis kademe bos olmali (satici yok)
    if not is_kademe_empty(satis_kademe):
        return False

    # Alis kademe tavan fiyatina esit olmali
    if alis_kademe is None:
        return False

    try:
        alis_fiyat = float(alis_kademe.replace(",", "."))
        return abs(alis_fiyat - tavan) < PRICE_TOLERANCE
    except (ValueError, TypeError):
        return False


def check_floor_lock(taban: float, satis_kademe: Optional[str], alis_kademe: Optional[str]) -> bool:
    """Tabana kilitli mi?

    KURAL:
    - Alis Kademe = #YOK (veya None/bos)  → Alici yok
    - Satis Kademe = Taban fiyati ile esit  → Saticilar taban fiyatindan bekliyor
    → Bu durumda TABANA KILITLI
    """
    if taban <= 0:
        return False

    # Alis kademe bos olmali (alici yok)
    if not is_kademe_empty(alis_kademe):
        return False

    # Satis kademe taban fiyatina esit olmali
    if satis_kademe is None:
        return False

    try:
        satis_fiyat = float(satis_kademe.replace(",", "."))
        return abs(satis_fiyat - taban) < PRICE_TOLERANCE
    except (ValueError, TypeError):
        return False


# ============================================
# BACKEND API GONDERIMI
# ============================================

def send_notification_to_backend(
    ticker: str,
    notif_type: str,
    title: str,
    body: str,
    sub_event: str = None,
):
    """
    Backend /api/v1/realtime-notification endpoint'ine bildirim gonder.
    Backend dogru abonelere FCM push gonderir.

    yuzde_dusus icin sub_event: "pct4" veya "pct7"

    ONEMLI: Mesajlarda FIYAT BILGISI YOKTUR!
    """
    try:
        payload = {
            "admin_password": ADMIN_PASSWORD,
            "ticker": ticker,
            "notification_type": notif_type,
            "title": title,
            "body": body,
        }
        if sub_event:
            payload["sub_event"] = sub_event

        resp = requests.post(API_NOTIF_URL, json=payload, timeout=10)
        if resp.status_code == 200:
            result = resp.json()
            subs = result.get("active_subscribers", 0)
            sent = result.get("notifications_sent", 0)
            if subs > 0:
                log(f"  {ticker} [{notif_type}]: {sent}/{subs} bildirim gonderildi")
        elif resp.status_code == 404:
            pass  # IPO bulunamadi — normal
        else:
            log(f"  {ticker} [{notif_type}]: Backend hata {resp.status_code}")
    except Exception as e:
        log(f"  {ticker} [{notif_type}]: Baglanti hatasi: {e}")


def fetch_trading_days_from_api() -> dict[str, int]:
    """Backend'den tum trading IPO'larin trading_day_count degerlerini cek.

    trading_day_count gun sonu (18:20) guncellenir, gun icinde
    bugunun islemi henuz sayilmaz. O yuzden +1 ekliyoruz.
    """
    try:
        resp = requests.get(f"{API_BASE_URL}/api/v1/ipos", timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            result = {}
            for ipo in data:
                if ipo.get("status") == "trading" and ipo.get("ticker"):
                    count = ipo.get("trading_day_count") or 0
                    result[ipo["ticker"]] = count + 1  # +1 = bugunun islemi
            return result
    except Exception as e:
        log(f"API trading_day cekme hatasi: {e}")
    return {}


def send_ceiling_data_to_backend(stock: StockState, hit_ceiling: bool, hit_floor: bool, state: TrackingState):
    """Backend /api/v1/ceiling-track endpoint'ine tavan/taban verisini gonder."""
    try:
        today = dt.date.today()
        payload = {
            "ticker": stock.ticker,
            "trading_day": state.trading_day,
            "trade_date": today.isoformat(),
            "close_price": stock.son_fiyat,
            "high_price": stock.tavan,
            "low_price": stock.taban,
            "hit_ceiling": hit_ceiling,
            "hit_floor": hit_floor,
            "alis_lot": stock.alis_lot,
            "satis_lot": stock.satis_lot,
            "pct_change": stock.gun_fark,  # Excel'den direkt gunluk % degisim
        }
        if state.day_open_price > 0:
            payload["open_price"] = state.day_open_price

        response = requests.post(API_CEILING_URL, json=payload, timeout=10)
        if response.status_code == 200:
            result = response.json()
            subs = result.get("notifications_sent", 0)
            if subs > 0:
                log(f"  Backend ceiling: {stock.ticker} -> {subs} bildirim")
        elif response.status_code != 404:
            log(f"  Backend ceiling hata: {response.status_code}")
    except Exception as e:
        log(f"  Backend ceiling baglanti hatasi: {e}")


# ============================================
# BILDIRIM YONETIMI
# ============================================

def process_stock(stock: StockState, now: dt.datetime):
    """Bir hissenin durumunu analiz edip gerekirse bildirim gonder."""
    ticker = stock.ticker

    # Tracking state olustur (yoksa)
    if ticker not in tracking_states:
        tracking_states[ticker] = TrackingState(ticker=ticker)

    state = tracking_states[ticker]

    # Ilk okumada acilis fiyatini kaydet
    if not state.first_read_done and stock.son_fiyat > 0:
        state.day_open_price = stock.son_fiyat
        state.first_read_done = True

    # =====================
    # %4 / %7 DUSUS TAKIBI
    # =====================
    # J sutunundan gun ici en yuksek fiyati al (Excel'den direkt)
    gun_high = stock.gun_en_yuksek
    if gun_high <= 0:
        # J sutunu bossa kendi takibimizi kullan (fallback)
        if stock.son_fiyat > state.day_high:
            state.day_high = stock.son_fiyat
        gun_high = state.day_high
    else:
        state.day_high = gun_high  # Senkronize tut

    if stock.son_fiyat > 0 and gun_high > 0:
        # Dusus yuzdesini hesapla: (en_yuksek - son) / en_yuksek * 100
        drop_pct = ((gun_high - stock.son_fiyat) / gun_high) * 100

        # %4 dusus → yuzde_dusus servisine gonder (sub_event: pct4)
        if drop_pct >= 4.0 and not state.notified_drop_4pct:
            send_notification_to_backend(
                ticker=ticker,
                notif_type="yuzde_dusus",
                title=f"{ticker} En Yukseginden %4 Dustu!",
                body=f"{ticker} gun ici en yukseginden %4 dustu!",
                sub_event="pct4",
            )
            state.notified_drop_4pct = True
            log(f"  >>> {ticker} EN YUKSEGINDEN %{drop_pct:.1f} DUSTU! (yuzde_dusus/pct4)")

        # %7 dusus → yuzde_dusus servisine gonder (sub_event: pct7)
        if drop_pct >= 7.0 and not state.notified_drop_7pct:
            send_notification_to_backend(
                ticker=ticker,
                notif_type="yuzde_dusus",
                title=f"{ticker} En Yukseginden %7 Dustu!",
                body=f"{ticker} gun ici en yukseginden %7 dustu!",
                sub_event="pct7",
            )
            state.notified_drop_7pct = True
            log(f"  >>> {ticker} EN YUKSEGINDEN %{drop_pct:.1f} DUSTU! (yuzde_dusus/pct7)")

    # =====================
    # TAVAN TAKIBI
    # =====================

    # Ilk kez tavana kitlendi
    if stock.is_ceiling_locked and not state.was_ceiling_locked and not state.notified_ceiling_first_lock:
        send_notification_to_backend(
            ticker=ticker,
            notif_type="tavan_bozulma",
            title=f"{ticker} Tavana Kitlendi!",
            body=f"{ticker} tavana kitlendi!",
        )
        send_ceiling_data_to_backend(stock, hit_ceiling=True, hit_floor=False, state=state)
        state.notified_ceiling_first_lock = True
        state.last_ceiling_notif_at = now
        log(f"  >>> {ticker} TAVANA KITLENDI!")

    # Daha once tavana kilitliydi, simdi cozuldu
    # Bildirim HEMEN gider, sonra 5dk bekleme baslar
    if state.was_ceiling_locked and not stock.is_ceiling_locked:
        if not state.ceiling_broke_at:  # Ilk cozulme
            send_notification_to_backend(
                ticker=ticker,
                notif_type="tavan_bozulma",
                title=f"{ticker} Tavan Cozuldu!",
                body=f"{ticker} tavan cozuldu!",
            )
            send_ceiling_data_to_backend(stock, hit_ceiling=False, hit_floor=stock.is_floor_locked, state=state)
            state.last_ceiling_notif_at = now
            log(f"  >>> {ticker} TAVAN COZULDU!")
        state.ceiling_broke_at = now
        state.ceiling_5min_checked = False

    # Tavan cozuldukten 5dk gecti — duruma gore bildirim at
    if (state.ceiling_broke_at
        and not state.ceiling_5min_checked
        and (now - state.ceiling_broke_at).total_seconds() >= RELOCK_WAIT_SECONDS):
        state.ceiling_5min_checked = True
        if stock.is_ceiling_locked:
            # 5dk icinde tekrar tavana kitlemis
            send_notification_to_backend(
                ticker=ticker,
                notif_type="tavan_bozulma",
                title=f"{ticker} Tavana Kitlendi!",
                body=f"{ticker} tavana kitlendi!",
            )
            send_ceiling_data_to_backend(stock, hit_ceiling=True, hit_floor=False, state=state)
            state.last_ceiling_notif_at = now
            state.ceiling_broke_at = None  # Yeni dongu icin sifirla
            log(f"  >>> {ticker} TAVANA KİTLEDİ! (5dk kontrol)")
        else:
            # 5dk gecti hala kilitleyemedi
            send_notification_to_backend(
                ticker=ticker,
                notif_type="tavan_bozulma",
                title=f"{ticker} 5dk gecti, tavana kilitleyemedi!",
                body=f"{ticker} tavan cozuldukten 5 dakika gecti, tavana kilitleyemedi!",
            )
            state.last_ceiling_notif_at = now
            state.ceiling_broke_at = None  # Yeni dongu icin sifirla
            log(f"  >>> {ticker} 5DK GECTI, TAVANA KILITLEYEMEDI!")

    # =====================
    # TABAN TAKIBI
    # =====================

    # Ilk kez tabana kitlendi
    if stock.is_floor_locked and not state.was_floor_locked and not state.notified_floor_first_lock:
        send_notification_to_backend(
            ticker=ticker,
            notif_type="taban_acilma",
            title=f"{ticker} Tabana Kitlendi!",
            body=f"{ticker} tabana kitlendi!",
        )
        send_ceiling_data_to_backend(stock, hit_ceiling=False, hit_floor=True, state=state)
        state.notified_floor_first_lock = True
        state.last_floor_notif_at = now
        log(f"  >>> {ticker} TABANA KITLENDI!")

    # Daha once tabana kilitliydi, simdi cozuldu
    # Bildirim HEMEN gider, sonra 5dk bekleme baslar
    if state.was_floor_locked and not stock.is_floor_locked:
        if not state.floor_broke_at:  # Ilk cozulme
            send_notification_to_backend(
                ticker=ticker,
                notif_type="taban_acilma",
                title=f"{ticker} Taban Cozuldu!",
                body=f"{ticker} taban cozuldu!",
            )
            send_ceiling_data_to_backend(stock, hit_ceiling=stock.is_ceiling_locked, hit_floor=False, state=state)
            state.last_floor_notif_at = now
            log(f"  >>> {ticker} TABAN COZULDU!")
        state.floor_broke_at = now
        state.floor_5min_checked = False

    # Taban cozuldukten 5dk gecti — duruma gore bildirim at
    if (state.floor_broke_at
        and not state.floor_5min_checked
        and (now - state.floor_broke_at).total_seconds() >= RELOCK_WAIT_SECONDS):
        state.floor_5min_checked = True
        if stock.is_floor_locked:
            # 5dk icinde tekrar tabana kitlemis
            send_notification_to_backend(
                ticker=ticker,
                notif_type="taban_acilma",
                title=f"{ticker} Tabana Kitlendi!",
                body=f"{ticker} tabana kitlendi!",
            )
            send_ceiling_data_to_backend(stock, hit_ceiling=False, hit_floor=True, state=state)
            state.last_floor_notif_at = now
            state.floor_broke_at = None  # Yeni dongu icin sifirla
            log(f"  >>> {ticker} TABANA KİTLENDİ! (5dk kontrol)")
        else:
            # 5dk gecti hala kilitleyemedi
            send_notification_to_backend(
                ticker=ticker,
                notif_type="taban_acilma",
                title=f"{ticker} 5dk gecti, tabana kilitleyemedi!",
                body=f"{ticker} taban cozuldukten 5 dakika gecti, tabana kilitleyemedi!",
            )
            state.last_floor_notif_at = now
            state.floor_broke_at = None  # Yeni dongu icin sifirla
            log(f"  >>> {ticker} 5DK GECTI, TABANA KILITLEYEMEDI!")

    # Mevcut durumu kaydet (bir sonraki tick icin)
    state.was_ceiling_locked = stock.is_ceiling_locked
    state.was_floor_locked = stock.is_floor_locked


# ============================================
# ACILIS + KAPANIS BILDIRIMI
# ============================================

def send_opening_notifications(stocks: list[StockState]):
    """
    Her sabah 09:56'da acilis bildirimi gonder.
    Her hisse icin: "AKHAN tavan acti!", "NETCD normal islem ile acildi"

    Bildirim tipi: gunluk_acilis_kapanis
    FIYAT BILGISI YOK!
    """
    log(f"\n  {'='*60}")
    log(f"  ACILIS RAPORU ({dt.datetime.now().strftime('%d.%m.%Y %H:%M')})")
    log(f"  {'='*60}")

    for stock in stocks:
        ticker = stock.ticker
        if stock.son_fiyat <= 0:
            continue

        # Tracking state
        if ticker not in tracking_states:
            tracking_states[ticker] = TrackingState(ticker=ticker)
        state = tracking_states[ticker]
        state.day_open_price = stock.son_fiyat
        state.first_read_done = True

        # Gunluk en yuksek fiyati baslat
        state.day_high = stock.son_fiyat

        # Acilis durumu
        if stock.is_ceiling_locked:
            title = f"{ticker} Tavan Acti!"
            body = f"{ticker} tavan acti!"
            # Acilista tavandaysa gun ici "tavana kitledi" bildirimi atma (zaten bildirdik)
            state.notified_ceiling_first_lock = True
            state.was_ceiling_locked = True
            state.last_ceiling_notif_at = dt.datetime.now()
            log(f"  {ticker}: TAVAN ACTI!")
        elif stock.is_floor_locked:
            title = f"{ticker} Taban Acti!"
            body = f"{ticker} taban acti!"
            # Acilista tabandaysa gun ici "tabana kitledi" bildirimi atma (zaten bildirdik)
            state.notified_floor_first_lock = True
            state.was_floor_locked = True
            state.last_floor_notif_at = dt.datetime.now()
            log(f"  {ticker}: TABAN ACTI!")
        else:
            title = f"{ticker} Acilis"
            body = f"{ticker} normal islem ile acildi"
            log(f"  {ticker}: Normal acilis")

        send_notification_to_backend(ticker, "gunluk_acilis_kapanis", title, body)
        state.opening_notified = True

    log(f"  {'='*60}\n")


def send_closing_notifications(stocks: list[StockState]):
    """
    Her aksam 18:08'de kapanis bildirimi gonder.
    Her hisse icin: "AKHAN tavan kapatti!", "NETCD normal islem ile kapatti"

    Bildirim tipi: gunluk_acilis_kapanis
    FIYAT BILGISI YOK!
    """
    log(f"\n  {'='*60}")
    log(f"  KAPANIS RAPORU ({dt.datetime.now().strftime('%d.%m.%Y %H:%M')})")
    log(f"  {'='*60}")

    for stock in stocks:
        ticker = stock.ticker
        if stock.son_fiyat <= 0:
            continue

        # Tracking state
        if ticker not in tracking_states:
            tracking_states[ticker] = TrackingState(ticker=ticker)
        state = tracking_states[ticker]

        # Kapanis durumu
        if stock.is_ceiling_locked:
            title = f"{ticker} Tavan Kapatti!"
            body = f"{ticker} tavan kapatti!"
            log(f"  {ticker}: TAVAN KAPATTI!")
        elif stock.is_floor_locked:
            title = f"{ticker} Taban Kapatti!"
            body = f"{ticker} taban kapatti!"
            log(f"  {ticker}: TABAN KAPATTI!")
        else:
            title = f"{ticker} Kapanis"
            body = f"{ticker} normal islem ile kapatti"
            log(f"  {ticker}: Normal kapanis")

        send_notification_to_backend(ticker, "gunluk_acilis_kapanis", title, body)
        state.closing_notified = True

    log(f"  {'='*60}\n")


# ============================================
# GUNLUK SIFIRLAMA
# ============================================

def reset_daily_states():
    """Her gun baslangicinda takip durumlarini sifirla."""
    for ticker, state in tracking_states.items():
        # Tavan/taban bildirim flagleri sifirla
        state.notified_ceiling_first_lock = False
        state.notified_ceiling_break = False
        state.notified_ceiling_5min = False
        state.notified_relock_ceiling = False
        state.ceiling_broke_at = None

        state.notified_floor_first_lock = False
        state.notified_floor_break = False
        state.notified_floor_5min = False
        state.notified_relock_floor = False
        state.floor_broke_at = None

        # %4/%7 dusus sifirla
        state.day_high = 0.0
        state.notified_drop_4pct = False
        state.notified_drop_7pct = False

        # Acilis/kapanis sifirla
        state.opening_notified = False
        state.closing_notified = False

        # Onceki durum
        state.was_ceiling_locked = False
        state.was_floor_locked = False
        state.day_open_price = 0.0
        state.first_read_done = False

    log("Gunluk takip durumlari sifirlandi.")


# ============================================
# ANA DONGU
# ============================================

def print_stock_table(stocks: list[StockState]):
    """Hisse durumlarini tablo olarak goster."""
    now = dt.datetime.now()
    print(f"\n[{now.strftime('%H:%M:%S')}] {len(stocks)} hisse okundu:")
    print(f"  {'HISSE':<8s} {'TAVAN':>8s} {'TABAN':>8s} {'SON':>8s} {'G.HIGH':>8s} {'ALIS K.':>10s} {'SATIS K.':>10s} {'A.LOT':>10s} {'S.LOT':>10s} {'DURUM'}")
    print(f"  {'-'*100}")
    for s in stocks:
        durum = ""
        if s.is_ceiling_locked:
            durum = "TAVANA KILITLI"
        elif s.is_floor_locked:
            durum = "TABANA KILITLI"
        else:
            durum = "Normal"

        ak = s.alis_kademe or "-"
        sk = s.satis_kademe or "-"
        al = f"{s.alis_lot:,}".replace(",", ".") if s.alis_lot else "-"
        sl = f"{s.satis_lot:,}".replace(",", ".") if s.satis_lot else "-"

        # %4/%7 dusus durumunu goster (J sutunundan gun_en_yuksek)
        gun_high = s.gun_en_yuksek
        if gun_high > 0 and s.son_fiyat > 0:
            drop = ((gun_high - s.son_fiyat) / gun_high) * 100
            if drop >= 1.0:
                durum += f" (-%{drop:.1f})"

        print(f"  {s.ticker:<8s} {s.tavan:>8.2f} {s.taban:>8.2f} {s.son_fiyat:>8.2f} {gun_high:>8.2f} {ak:>10s} {sk:>10s} {al:>10s} {sl:>10s} {durum}")


def is_market_hours() -> bool:
    """Piyasa saatleri icinde mi? (09:55 — 18:10)"""
    now = dt.datetime.now()
    current_min = now.hour * 60 + now.minute
    start_min = SEANS_START_HOUR * 60 + SEANS_START_MIN - 5  # 09:55
    end_min = SEANS_END_HOUR * 60 + SEANS_END_MIN
    return start_min <= current_min <= end_min


def is_opening_time() -> bool:
    """Acilis bildirimi zamani mi? (09:56)"""
    now = dt.datetime.now()
    return now.hour == MARKET_OPEN_HOUR and now.minute == MARKET_OPEN_MIN


def is_closing_time() -> bool:
    """Kapanis bildirimi zamani mi? (18:08)"""
    now = dt.datetime.now()
    return now.hour == KAPANIS_HOUR and now.minute == KAPANIS_MIN


def is_weekend() -> bool:
    """Hafta sonu mu?"""
    return dt.datetime.now().weekday() >= 5


# Global gunluk flagler
opening_sent_today = False
closing_sent_today = False
daily_reset_done = False


def run():
    """Ana calisma dongusu — 15 saniyede bir Excel'den oku, analiz et."""
    global opening_sent_today, closing_sent_today, daily_reset_done

    now = dt.datetime.now()
    print("=" * 60)
    print(f"  Halka Arz Tavan/Taban Takip — Canli Sync")
    print(f"  Excel: {Path(EXCEL_FILE_PATH).name}")
    print(f"  Sync Araligi: {SYNC_INTERVAL} saniye")
    print(f"  Piyasa: {SEANS_START_HOUR:02d}:{SEANS_START_MIN:02d} - {SEANS_END_HOUR:02d}:{SEANS_END_MIN:02d}")
    print(f"  Acilis Bildirimi: {MARKET_OPEN_HOUR:02d}:{MARKET_OPEN_MIN:02d}")
    print(f"  Kapanis Bildirimi: {KAPANIS_HOUR:02d}:{KAPANIS_MIN:02d}")
    print(f"  Tavan Bozulma Bekleme: {RELOCK_WAIT_SECONDS // 60} dakika")
    print(f"  Backend: {API_BASE_URL}")
    print(f"  [{now.strftime('%Y-%m-%d %H:%M:%S')}]")
    print("=" * 60)

    log("SYSTEM: Halka Arz Sync baslatildi")

    # Script baslangicinda trading_day degerlerini API'den cek
    trading_days = fetch_trading_days_from_api()
    if trading_days:
        log(f"API'den {len(trading_days)} hisse icin trading_day cekildi:")
        for ticker, day in sorted(trading_days.items()):
            if ticker not in tracking_states:
                tracking_states[ticker] = TrackingState(ticker=ticker)
            tracking_states[ticker].trading_day = day
            log(f"  {ticker}: trading_day = {day}")
    else:
        log("UYARI: API'den trading_day cekilemedi — varsayilan 1 kullanilacak")

    tick_count = 0

    while True:
        try:
            now = dt.datetime.now()

            # Hafta sonu kontrolu
            if is_weekend():
                print(f"\r  [{now.strftime('%H:%M:%S')}] Hafta sonu - bekleniyor...", end="", flush=True)
                time.sleep(60)
                continue

            # Gun degisimi — gunluk sifirlama
            if now.hour < 9 and not daily_reset_done:
                reset_daily_states()
                opening_sent_today = False
                closing_sent_today = False
                daily_reset_done = True
            elif now.hour >= 9:
                daily_reset_done = False  # Ertesi gun icin sifirla

            # Piyasa saatleri disinda bekle
            if not is_market_hours():
                if now.hour < SEANS_START_HOUR:
                    print(f"\r  [{now.strftime('%H:%M:%S')}] Piyasa acilisi bekleniyor...", end="", flush=True)
                else:
                    print(f"\r  [{now.strftime('%H:%M:%S')}] Piyasa kapali.", end="", flush=True)
                time.sleep(30)
                continue

            # Excel'den oku
            stocks = read_excel_data()

            if not stocks:
                print(f"\r  [{now.strftime('%H:%M:%S')}] Excel'den veri okunamadi", end="", flush=True)
                time.sleep(SYNC_INTERVAL)
                continue

            # TARIH kontrolu — Excel'deki tarih bugune esit degilse borsa kapali (tatil/cumartesi)
            bugun = dt.date.today()
            excel_tarih = stocks[0].tarih if stocks else None
            if excel_tarih and excel_tarih != bugun:
                if tick_count % 20 == 0:  # 5 dakikada bir logla (20 tick * 15sn)
                    log(f"TARIH UYUMSUZLUGU: Excel={excel_tarih}, Bugun={bugun} — borsa kapali, veri gonderilmiyor")
                print(f"\r  [{now.strftime('%H:%M:%S')}] Borsa kapali (Excel tarih: {excel_tarih})", end="", flush=True)
                tick_count += 1
                time.sleep(SYNC_INTERVAL)
                continue

            # 09:56 Acilis bildirimi
            if is_opening_time() and not opening_sent_today:
                send_opening_notifications(stocks)
                opening_sent_today = True

            # 18:08 Kapanis bildirimi
            if is_closing_time() and not closing_sent_today:
                send_closing_notifications(stocks)
                closing_sent_today = True

            # Her hisseyi analiz et (tavan/taban/dusus)
            for stock in stocks:
                process_stock(stock, now)

            tick_count += 1

            # Her 4 tick'te bir (60 saniyede bir) tum hisselerin ceiling data'sini gonder
            if tick_count % 4 == 0:
                for stock in stocks:
                    if stock.son_fiyat <= 0:
                        continue
                    ticker = stock.ticker
                    if ticker not in tracking_states:
                        tracking_states[ticker] = TrackingState(ticker=ticker)
                    state = tracking_states[ticker]
                    send_ceiling_data_to_backend(
                        stock, stock.is_ceiling_locked, stock.is_floor_locked, state
                    )

            # Her 4 tick'te bir (60 saniyede bir) tablo goster
            if tick_count % 4 == 0:
                print_stock_table(stocks)

                # Kilitli hisseleri kisa ozet
                ceiling = [s.ticker for s in stocks if s.is_ceiling_locked]
                floor = [s.ticker for s in stocks if s.is_floor_locked]
                if ceiling:
                    print(f"  >> Tavanda: {', '.join(ceiling)}")
                if floor:
                    print(f"  >> Tabanda: {', '.join(floor)}")
            else:
                # Kisa durum satiri
                ceiling_count = sum(1 for s in stocks if s.is_ceiling_locked)
                floor_count = sum(1 for s in stocks if s.is_floor_locked)
                print(f"\r  [{now.strftime('%H:%M:%S')}] {len(stocks)} hisse | "
                      f"Tavan: {ceiling_count} | Taban: {floor_count} | "
                      f"Tick #{tick_count}", end="", flush=True)

            time.sleep(SYNC_INTERVAL)

        except KeyboardInterrupt:
            print(f"\n\n  Halka Arz Sync durduruldu (Ctrl+C)")
            log("SYSTEM: Sync durduruldu (Ctrl+C)")
            break
        except Exception as e:
            print(f"\n  Beklenmeyen hata: {e}")
            log(f"SYSTEM HATA: {e}")
            time.sleep(30)


if __name__ == "__main__":
    run()
