#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Halka Arz Tavan/Taban Takip - Excel -> Backend Canli Sync

Her 10 saniyede bir 'halka arz TAVAN TABAN.xlsm' dosyasindan
canli fiyat verilerini okuyup tavan/taban durumunu analiz eder.

TAVAN KİLİT TESPİTİ:
  - Satis Kademe = #YOK (veya bos) VE Alis Kademe = Tavan → TAVANA KİLİTLİ
  - Bu denge bozulursa → 1. BİLDİRİM: "Tavan cozuldu!"
  - 5 dakika bekle → hala tavana kilitlemediyse → 2. BİLDİRİM: "5dk gecti, kilitleyemedi"
  - Taban icin ayni mantik (tersi)

TABAN KİLİT TESPİTİ:
  - Alis Kademe = #YOK (veya bos) VE Satis Kademe = Taban → TABANA KİLİTLİ

ACILIS BİLDİRİMİ:
  - Her sabah 09:56'da acilis bilgisi gonderir

NOT: win32com ile ACIK OLAN Excel'den canli veri okur (Matriks DDE/RTD).
"""

import sys
import os
import time
import datetime as dt
import requests
import json
from pathlib import Path
from decimal import Decimal
from dataclasses import dataclass, field
from typing import Optional

# Windows encoding fix
if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    if hasattr(sys.stdout, 'reconfigure'):
        try:
            sys.stdout.reconfigure(encoding='utf-8', errors='replace')
            sys.stderr.reconfigure(encoding='utf-8', errors='replace')
        except:
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

# Backend API endpoint
API_URL = "http://localhost:8000/api/v1/ceiling-track"
# Production: API_URL = "https://bist-finans-backend.onrender.com/api/v1/ceiling-track"

# Sync araligi (saniye) - 10 saniye
SYNC_INTERVAL = 10

# Tavan bozulma sonrasi bekleme suresi (saniye) - 5 dakika
RELOCK_WAIT_SECONDS = 300

# Piyasa calisma saatleri
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 56  # Acilis bildirimi saati
SEANS_START_HOUR = 10
SEANS_START_MIN = 0
SEANS_END_HOUR = 18
SEANS_END_MIN = 10

# Retry ayarlari
RETRY_DELAY = 5
MAX_RETRIES = 2


# ============================================
# EXCEL SUTUN YAPILANDIRMASI
# ============================================

# A: HISSE (ticker)
# B: TAVAN (tavan fiyati)
# C: TABAN (taban fiyati)
# D: SON FIYAT
# E: SATIS KADEME (#YOK = tavana kilitli)
# F: ALIS KADEME

HISSE_SUTUN = "A"
TAVAN_SUTUN = "B"
TABAN_SUTUN = "C"
SON_FIYAT_SUTUN = "D"
SATIS_KADEME_SUTUN = "E"
ALIS_KADEME_SUTUN = "F"

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
    satis_kademe: Optional[str] = None  # "#YOK" veya fiyat
    alis_kademe: Optional[str] = None   # "#YOK" veya fiyat
    is_ceiling_locked: bool = False      # Tavana kilitli mi?
    is_floor_locked: bool = False        # Tabana kilitli mi?
    ipo_price: float = 0.0              # Halka arz fiyati (DB'den alinacak)
    opening_price: float = 0.0          # Gunun acilis fiyati


@dataclass
class TrackingState:
    """Bir hissenin takip durumu — bildirim gecmisi."""
    ticker: str
    was_ceiling_locked: bool = False      # Onceki durumda tavana kilitli miydi?
    was_floor_locked: bool = False        # Onceki durumda tabana kilitli miydi?
    ceiling_broke_at: Optional[dt.datetime] = None  # Tavan ne zaman bozuldu?
    floor_broke_at: Optional[dt.datetime] = None    # Taban ne zaman bozuldu?
    notified_ceiling_break: bool = False  # 1. bildirim gonderildi mi?
    notified_ceiling_5min: bool = False   # 5dk bildirim gonderildi mi?
    notified_floor_break: bool = False
    notified_floor_5min: bool = False
    notified_relock_ceiling: bool = False  # Tekrar tavana kitledi bildirimi
    notified_relock_floor: bool = False
    notified_ceiling_first_lock: bool = False  # Ilk tavana kilit bildirimi
    notified_floor_first_lock: bool = False    # Ilk tabana kilit bildirimi
    opening_notified: bool = False         # Acilis bildirimi gonderildi mi?
    trading_day: int = 1                   # Kacinci islem gunu
    day_open_price: float = 0.0           # Gunun acilis fiyati (ilk okunan)
    first_read_done: bool = False          # Gun icin ilk okuma yapildi mi?


# Global takip durumu — her hisse icin
tracking_states: dict[str, TrackingState] = {}


# ============================================
# EXCEL OKUMA
# ============================================

def read_excel_data() -> list[StockState]:
    """
    WIN32COM ile ACIK OLAN Excel'den halka arz hisselerini oku.
    Matriks DDE/RTD canli verilerini alir.
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
            ticker = sheet.Range(f"{HISSE_SUTUN}{satir}").Value
            if ticker is None or str(ticker).strip() == "":
                break

            ticker = str(ticker).strip().upper()

            # Fiyat verilerini oku
            tavan_val = sheet.Range(f"{TAVAN_SUTUN}{satir}").Value
            taban_val = sheet.Range(f"{TABAN_SUTUN}{satir}").Value
            son_fiyat_val = sheet.Range(f"{SON_FIYAT_SUTUN}{satir}").Value
            satis_kademe_val = sheet.Range(f"{SATIS_KADEME_SUTUN}{satir}").Value
            alis_kademe_val = sheet.Range(f"{ALIS_KADEME_SUTUN}{satir}").Value

            # Fiyatlari parse et
            tavan = safe_float(tavan_val)
            taban = safe_float(taban_val)
            son_fiyat = safe_float(son_fiyat_val)

            # Kademe degerlerini string olarak tut
            satis_kademe = parse_kademe(satis_kademe_val)
            alis_kademe = parse_kademe(alis_kademe_val)

            # Tavan/Taban kilit tespiti
            is_ceiling_locked = check_ceiling_lock(tavan, satis_kademe, alis_kademe)
            is_floor_locked = check_floor_lock(taban, satis_kademe, alis_kademe)

            stock = StockState(
                ticker=ticker,
                tavan=tavan,
                taban=taban,
                son_fiyat=son_fiyat,
                satis_kademe=satis_kademe,
                alis_kademe=alis_kademe,
                is_ceiling_locked=is_ceiling_locked,
                is_floor_locked=is_floor_locked,
            )
            stocks.append(stock)
            satir += 1

        return stocks

    except Exception as e:
        error_msg = str(e)
        if "operation unavailable" in error_msg.lower() or "moniker" in error_msg.lower():
            pass  # Excel kapali, sessizce gec
        else:
            print(f"  Excel okuma hatasi: {e}")
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
    if s == "" or s == "0":
        return None
    return s


def check_ceiling_lock(tavan: float, satis_kademe: Optional[str], alis_kademe: Optional[str]) -> bool:
    """Tavana kilitli mi?

    KURAL:
    - Satis Kademe = #YOK (veya None/bos)  → Alici yok, satici beklemiyor
    - Alis Kademe = Tavan fiyati ile esit    → Alicilar tavan fiyatindan bekliyor
    → Bu durumda TAVANA KİLİTLİ
    """
    if tavan <= 0:
        return False

    # Satis kademe #YOK veya bos olmali
    satis_yok = (satis_kademe is None or satis_kademe.upper() in ("#YOK", "#N/A", "YOK", "-"))

    if not satis_yok:
        return False

    # Alis kademe tavan fiyatina esit olmali
    if alis_kademe is None:
        return False

    try:
        alis_fiyat = float(alis_kademe.replace(",", "."))
        # Tavan ile esit mi? (kucuk tolerans)
        return abs(alis_fiyat - tavan) < 0.02
    except (ValueError, TypeError):
        return False


def check_floor_lock(taban: float, satis_kademe: Optional[str], alis_kademe: Optional[str]) -> bool:
    """Tabana kilitli mi?

    KURAL:
    - Alis Kademe = #YOK (veya None/bos)  → Alici yok
    - Satis Kademe = Taban fiyati ile esit  → Saticilar taban fiyatindan bekliyor
    → Bu durumda TABANA KİLİTLİ
    """
    if taban <= 0:
        return False

    # Alis kademe #YOK veya bos olmali
    alis_yok = (alis_kademe is None or alis_kademe.upper() in ("#YOK", "#N/A", "YOK", "-"))

    if not alis_yok:
        return False

    # Satis kademe taban fiyatina esit olmali
    if satis_kademe is None:
        return False

    try:
        satis_fiyat = float(satis_kademe.replace(",", "."))
        return abs(satis_fiyat - taban) < 0.02
    except (ValueError, TypeError):
        return False


# ============================================
# BİLDİRİM YÖNETİMİ
# ============================================

def process_stock(stock: StockState, now: dt.datetime):
    """Bir hissenin durumunu analiz edip gerekirse bildirim gonder."""
    ticker = stock.ticker

    # Tracking state olustur (yoksa)
    if ticker not in tracking_states:
        tracking_states[ticker] = TrackingState(ticker=ticker)

    state = tracking_states[ticker]

    # =====================
    # TAVAN TAKİBİ
    # =====================

    # Ilk kez tavana kitlendi (acilistan sonra)
    if stock.is_ceiling_locked and not state.was_ceiling_locked and not state.notified_ceiling_first_lock:
        send_notification(
            ticker=ticker,
            title=f"{ticker} TAVANA KİTLENDİ!",
            body=f"{ticker} tavana kitlendi! Fiyat: {stock.son_fiyat:.2f} TL = Tavan: {stock.tavan:.2f} TL",
            notif_type="ceiling_first_lock",
        )
        state.notified_ceiling_first_lock = True
        print(f"  >>> {ticker} TAVANA KİTLENDİ! {stock.son_fiyat:.2f} = {stock.tavan:.2f}")

    # Ilk kez tabana kitlendi
    if stock.is_floor_locked and not state.was_floor_locked and not state.notified_floor_first_lock:
        send_notification(
            ticker=ticker,
            title=f"{ticker} TABANA KİTLENDİ!",
            body=f"{ticker} tabana kitlendi! Fiyat: {stock.son_fiyat:.2f} TL = Taban: {stock.taban:.2f} TL",
            notif_type="floor_first_lock",
        )
        state.notified_floor_first_lock = True
        print(f"  >>> {ticker} TABANA KİTLENDİ! {stock.son_fiyat:.2f} = {stock.taban:.2f}")

    # Daha once tavana kilitliydi, simdi cozuldu
    if state.was_ceiling_locked and not stock.is_ceiling_locked:
        if not state.notified_ceiling_break:
            # 1. BİLDİRİM — Tavan cozuldu!
            send_notification(
                ticker=ticker,
                title=f"{ticker} TAVAN COZULDU!",
                body=f"{ticker} tavan cozuldu! Son fiyat: {stock.son_fiyat:.2f} TL (Tavan: {stock.tavan:.2f})",
                notif_type="ceiling_break",
            )
            send_to_backend(stock, hit_ceiling=False, hit_floor=stock.is_floor_locked, state=state)
            state.ceiling_broke_at = now
            state.notified_ceiling_break = True
            state.notified_ceiling_5min = False
            state.notified_relock_ceiling = False
            print(f"  >>> {ticker} TAVAN COZULDU! Son: {stock.son_fiyat:.2f} (Tavan: {stock.tavan:.2f})")

    # Tavan cozuldukten 5 dakika gecti, hala kilitlemediyse
    if (state.ceiling_broke_at
        and not stock.is_ceiling_locked
        and not state.notified_ceiling_5min
        and (now - state.ceiling_broke_at).total_seconds() >= RELOCK_WAIT_SECONDS):
        # 2. BİLDİRİM — 5 dakika gecti, tavana kilitleyemedi
        send_notification(
            ticker=ticker,
            title=f"{ticker} 5dk gecti, tavana kilitleyemedi!",
            body=f"{ticker} tavan cozuldukten 5 dakika gecti, hala tavana kilitleyemedi. Son: {stock.son_fiyat:.2f} TL",
            notif_type="ceiling_5min_warning",
        )
        state.notified_ceiling_5min = True
        print(f"  >>> {ticker} 5DK GECTİ, TAVANA KİLİTLEYEMEDİ! Son: {stock.son_fiyat:.2f}")

    # Tavan cozuldukten sonra tekrar tavana kitlendi
    if (state.ceiling_broke_at
        and stock.is_ceiling_locked
        and not state.notified_relock_ceiling):
        relock_seconds = (now - state.ceiling_broke_at).total_seconds()
        send_notification(
            ticker=ticker,
            title=f"{ticker} TEKRAR TAVANA KİTLENDİ!",
            body=f"{ticker} tekrar tavana kitlendi! ({int(relock_seconds)}sn sonra). Son: {stock.son_fiyat:.2f} TL",
            notif_type="ceiling_relock",
        )
        send_to_backend(stock, hit_ceiling=True, hit_floor=False, state=state)
        state.notified_relock_ceiling = True
        state.ceiling_broke_at = None
        state.notified_ceiling_break = False
        state.notified_ceiling_5min = False
        print(f"  >>> {ticker} TEKRAR TAVANA KİTLENDİ! ({int(relock_seconds)}sn sonra)")

    # =====================
    # TABAN TAKİBİ (ayni mantik)
    # =====================

    # Daha once tabana kilitliydi, simdi cozuldu
    if state.was_floor_locked and not stock.is_floor_locked:
        if not state.notified_floor_break:
            send_notification(
                ticker=ticker,
                title=f"{ticker} TABAN COZULDU!",
                body=f"{ticker} taban cozuldu! Son fiyat: {stock.son_fiyat:.2f} TL (Taban: {stock.taban:.2f})",
                notif_type="floor_break",
            )
            send_to_backend(stock, hit_ceiling=stock.is_ceiling_locked, hit_floor=False, state=state)
            state.floor_broke_at = now
            state.notified_floor_break = True
            state.notified_floor_5min = False
            state.notified_relock_floor = False
            print(f"  >>> {ticker} TABAN COZULDU! Son: {stock.son_fiyat:.2f} (Taban: {stock.taban:.2f})")

    # Taban cozuldukten 5 dakika gecti, hala kilitlemediyse
    if (state.floor_broke_at
        and not stock.is_floor_locked
        and not state.notified_floor_5min
        and (now - state.floor_broke_at).total_seconds() >= RELOCK_WAIT_SECONDS):
        send_notification(
            ticker=ticker,
            title=f"{ticker} 5dk gecti, tabana kilitleyemedi!",
            body=f"{ticker} taban cozuldukten 5 dakika gecti. Son: {stock.son_fiyat:.2f} TL",
            notif_type="floor_5min_warning",
        )
        state.notified_floor_5min = True
        print(f"  >>> {ticker} 5DK GECTİ, TABANA KİLİTLEYEMEDİ! Son: {stock.son_fiyat:.2f}")

    # Taban cozuldukten sonra tekrar tabana kitlendi
    if (state.floor_broke_at
        and stock.is_floor_locked
        and not state.notified_relock_floor):
        relock_seconds = (now - state.floor_broke_at).total_seconds()
        send_notification(
            ticker=ticker,
            title=f"{ticker} TEKRAR TABANA KİTLENDİ!",
            body=f"{ticker} tekrar tabana kitlendi! ({int(relock_seconds)}sn sonra)",
            notif_type="floor_relock",
        )
        send_to_backend(stock, hit_ceiling=False, hit_floor=True, state=state)
        state.notified_relock_floor = True
        state.floor_broke_at = None
        state.notified_floor_break = False
        state.notified_floor_5min = False
        print(f"  >>> {ticker} TEKRAR TABANA KİTLENDİ! ({int(relock_seconds)}sn sonra)")

    # Mevcut durumu kaydet (bir sonraki tick icin)
    state.was_ceiling_locked = stock.is_ceiling_locked
    state.was_floor_locked = stock.is_floor_locked


# ============================================
# BACKEND API GONDERIMI
# ============================================

def send_to_backend(stock: StockState, hit_ceiling: bool, hit_floor: bool, state: TrackingState):
    """Backend API'ye tavan/taban bilgisini gonder."""
    try:
        today = dt.date.today()
        payload = {
            "ticker": stock.ticker,
            "trading_day": state.trading_day,
            "trade_date": today.isoformat(),
            "open_price": stock.son_fiyat,  # Acilis fiyati ayrica izlenecek
            "close_price": stock.son_fiyat,
            "high_price": stock.tavan,
            "low_price": stock.taban,
            "hit_ceiling": hit_ceiling,
            "hit_floor": hit_floor,
        }

        response = requests.post(API_URL, json=payload, timeout=10)
        if response.status_code == 200:
            result = response.json()
            subs = result.get("active_subscribers", 0)
            notifs = result.get("notifications_sent", 0)
            if subs > 0:
                print(f"    Backend: {stock.ticker} -> {subs} abone, {notifs} bildirim gonderildi")
        elif response.status_code == 404:
            pass  # IPO bulunamadi — normal, yeni hisse olabilir
        else:
            print(f"    Backend hata: {response.status_code}")

    except Exception as e:
        print(f"    Backend baglanti hatasi: {e}")


def send_notification(ticker: str, title: str, body: str, notif_type: str):
    """Push bildirim gonder (backend uzerinden).

    Backend zaten tavan takip aboneleri icin FCM bildirimlerini yonetiyor.
    Burada ek olarak topic-based bildirim gonderebiliriz.
    """
    try:
        # Backend ceiling-track endpoint'i zaten bildirimleri gonderiyor
        # Ek bildirim gerekirse burada topic-based gonderim yapilabilir
        log_notification(ticker, title, body, notif_type)
    except Exception as e:
        print(f"    Bildirim hatasi: {e}")


def log_notification(ticker: str, title: str, body: str, notif_type: str):
    """Bildirimi log dosyasina yaz."""
    log_file = Path(__file__).parent / "halka_arz_sync.log"
    now = dt.datetime.now()
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] [{notif_type}] {title} | {body}\n")


# ============================================
# ACILIS BİLDİRİMİ (09:56)
# ============================================

def get_ipo_prices() -> dict[str, float]:
    """Backend'den halka arz fiyatlarini cek (gunluk 1 kez yeterli)."""
    try:
        response = requests.get(
            API_URL.replace("/ceiling-track", "/ipos?status=active&limit=50"),
            timeout=10,
        )
        if response.status_code == 200:
            ipos = response.json()
            return {
                ipo["ticker"]: float(ipo.get("ipo_price") or 0)
                for ipo in ipos
                if ipo.get("ticker") and ipo.get("ipo_price")
            }
    except Exception as e:
        print(f"  IPO fiyatlari alinamadi: {e}")
    return {}


# Gunluk IPO fiyatlari cache
_ipo_prices_cache: dict[str, float] = {}
_ipo_prices_loaded = False


def send_opening_notification(stocks: list[StockState]):
    """Her sabah 09:56'da acilis bilgisi gonder.

    Her hisse icin ayri bildirim:
    - "AKHAN tavan acti!" (acilis fiyati = tavan)
    - "NETCD %8.5 yukselisle acti!" (yuksek acilis ama tavan degil)
    - "FRMPL -%3.2 dususle acti!" (dusuk acilis)
    """
    global _ipo_prices_cache, _ipo_prices_loaded
    now = dt.datetime.now()

    # IPO fiyatlarini cache'le
    if not _ipo_prices_loaded:
        _ipo_prices_cache = get_ipo_prices()
        _ipo_prices_loaded = True

    print(f"\n  {'='*60}")
    print(f"  ACILIS RAPORU ({now.strftime('%d.%m.%Y %H:%M')})")
    print(f"  {'='*60}")

    for stock in stocks:
        ticker = stock.ticker
        ipo_price = _ipo_prices_cache.get(ticker, 0)

        # Tracking state guncelle — acilis fiyatini kaydet
        if ticker not in tracking_states:
            tracking_states[ticker] = TrackingState(ticker=ticker)
        state = tracking_states[ticker]
        state.day_open_price = stock.son_fiyat
        state.first_read_done = True

        # Acilis analizi
        if stock.son_fiyat >= stock.tavan and stock.tavan > 0:
            # TAVAN ACTI!
            pct = ""
            if ipo_price > 0:
                change = ((stock.son_fiyat - ipo_price) / ipo_price) * 100
                pct = f" (Halka arz fiyatindan %{change:.1f})"

            send_notification(
                ticker=ticker,
                title=f"{ticker} TAVAN ACTI!",
                body=f"{ticker} tavan fiyatindan acildi! Acilis: {stock.son_fiyat:.2f} TL (Tavan: {stock.tavan:.2f}){pct}",
                notif_type="opening_ceiling",
            )
            print(f"  {ticker}: TAVAN ACTI! {stock.son_fiyat:.2f} TL{pct}")

        elif stock.son_fiyat <= stock.taban and stock.taban > 0:
            # TABAN ACTI!
            pct = ""
            if ipo_price > 0:
                change = ((stock.son_fiyat - ipo_price) / ipo_price) * 100
                pct = f" (Halka arz fiyatindan %{change:.1f})"

            send_notification(
                ticker=ticker,
                title=f"{ticker} TABAN ACTI!",
                body=f"{ticker} taban fiyatindan acildi! Acilis: {stock.son_fiyat:.2f} TL (Taban: {stock.taban:.2f}){pct}",
                notif_type="opening_floor",
            )
            print(f"  {ticker}: TABAN ACTI! {stock.son_fiyat:.2f} TL{pct}")

        else:
            # Normal acilis — yuzde degisim goster
            if ipo_price > 0:
                change = ((stock.son_fiyat - ipo_price) / ipo_price) * 100
                direction = "yukselisle" if change > 0 else "dususle"
                send_notification(
                    ticker=ticker,
                    title=f"{ticker} %{abs(change):.1f} {direction} acildi",
                    body=f"{ticker} acilis: {stock.son_fiyat:.2f} TL (Halka arz: {ipo_price:.2f} TL, %{change:+.1f})",
                    notif_type="opening_normal",
                )
                print(f"  {ticker}: %{change:+.1f} acildi -> {stock.son_fiyat:.2f} TL (HA: {ipo_price:.2f})")
            else:
                print(f"  {ticker}: Acilis {stock.son_fiyat:.2f} TL (Tavan: {stock.tavan:.2f}, Taban: {stock.taban:.2f})")

        state.opening_notified = True

    print(f"  {'='*60}\n")


# ============================================
# ANA DONGU
# ============================================

def print_stock_table(stocks: list[StockState]):
    """Hisse durumlarini tablo olarak goster."""
    now = dt.datetime.now()
    print(f"\n[{now.strftime('%H:%M:%S')}] {len(stocks)} hisse okundu:")
    print(f"  {'HISSE':<8s} {'TAVAN':>8s} {'TABAN':>8s} {'SON':>8s} {'SATIS K.':>10s} {'ALIS K.':>10s} {'DURUM'}")
    print(f"  {'-'*70}")
    for s in stocks:
        durum = ""
        if s.is_ceiling_locked:
            durum = "TAVANA KILITLI"
        elif s.is_floor_locked:
            durum = "TABANA KILITLI"
        else:
            durum = "Normal"
        sk = s.satis_kademe or "-"
        ak = s.alis_kademe or "-"
        print(f"  {s.ticker:<8s} {s.tavan:>8.2f} {s.taban:>8.2f} {s.son_fiyat:>8.2f} {sk:>10s} {ak:>10s} {durum}")


def is_market_hours() -> bool:
    """Piyasa saatleri icinde mi?"""
    now = dt.datetime.now()
    current_min = now.hour * 60 + now.minute
    start_min = SEANS_START_HOUR * 60 + SEANS_START_MIN - 5  # 09:55
    end_min = SEANS_END_HOUR * 60 + SEANS_END_MIN
    return start_min <= current_min <= end_min


def is_opening_time() -> bool:
    """Acilis bildirimi zamani mi? (09:56)"""
    now = dt.datetime.now()
    return now.hour == MARKET_OPEN_HOUR and now.minute == MARKET_OPEN_MIN


def is_weekend() -> bool:
    """Hafta sonu mu?"""
    return dt.datetime.now().weekday() >= 5


opening_sent_today = False


def run():
    """Ana calisma dongusu — 10 saniyede bir Excel'den oku, analiz et."""
    global opening_sent_today

    now = dt.datetime.now()
    print("=" * 60)
    print(f"  Halka Arz Tavan/Taban Takip Sync")
    print(f"  Excel: {Path(EXCEL_FILE_PATH).name}")
    print(f"  Sync Araligi: {SYNC_INTERVAL} saniye")
    print(f"  Piyasa: {SEANS_START_HOUR:02d}:{SEANS_START_MIN:02d} - {SEANS_END_HOUR:02d}:{SEANS_END_MIN:02d}")
    print(f"  Acilis Bildirimi: {MARKET_OPEN_HOUR:02d}:{MARKET_OPEN_MIN:02d}")
    print(f"  Tavan Bozulma Bekleme: {RELOCK_WAIT_SECONDS // 60} dakika")
    print(f"  [{now.strftime('%Y-%m-%d %H:%M:%S')}]")
    print("=" * 60)

    log_notification("SYSTEM", "Halka Arz Sync Baslatildi", f"Interval: {SYNC_INTERVAL}s", "startup")

    tick_count = 0

    while True:
        try:
            now = dt.datetime.now()

            # Hafta sonu kontrolu
            if is_weekend():
                print(f"\r  [{now.strftime('%H:%M:%S')}] Hafta sonu - bekleniyor...", end="", flush=True)
                time.sleep(60)
                continue

            # Gun degisimi — acilis bildirimini sifirla
            if now.hour < 9:
                opening_sent_today = False

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

            # 09:56 Acilis bildirimi
            if is_opening_time() and not opening_sent_today:
                send_opening_notification(stocks)
                opening_sent_today = True

            # Her hisseyi analiz et
            for stock in stocks:
                process_stock(stock, now)

            tick_count += 1

            # Her 6 tick'te bir (60 saniyede bir) tablo goster
            if tick_count % 6 == 0:
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
            log_notification("SYSTEM", "Sync Durduruldu", "Ctrl+C", "shutdown")
            break
        except Exception as e:
            print(f"\n  Beklenmeyen hata: {e}")
            log_notification("SYSTEM", "Hata", str(e), "error")
            time.sleep(30)


if __name__ == "__main__":
    run()
