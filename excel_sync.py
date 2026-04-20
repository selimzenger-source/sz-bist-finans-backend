"""
BIST Finans — Matriks Excel Sync Script
=========================================
Matriks Excel'den tavan takip verilerini okuyup Render API'ye gonder.

Excel Formati (Matriks):
  A: ILK ISLEM     (22.Oca.26 gibi)
  B: HISSE         (AKHAN, NETCD, UCAYM)
  C: TAVAN         (Tavan limit fiyati)
  D: TABAN         (Taban limit fiyati)
  E: ALIS          (Alis Kademe fiyati, 0=islem yok)
  F: SATIS         (Satis Kademe fiyati, 0=islem yok)
  G: SON           (Son fiyat / Kapanis, 0=borsa kapali)
  H: %G FARK       (Gunluk % degisim — oran veya yuzde)
  I: TARIH         (Verinin tarihi — 18/02/2026 00:00:00.00000)
  J: G.EN YUKSEK   (Gun ici en yuksek fiyat)
  K: ALISTAKI LOT  (1. kademe alis lotu)
  L: SATISTAKI LOT (1. kademe satis lotu)
  M: GUNLUK ADET   (Gunluk islem adedi — E.D.O icin)
  N: SENET SAYISI   (Toplam senet sayisi — E.D.O icin)

Canli mod (--live):
  win32com ile acik Excel'den canli Matriks verilerini okur.
  Matriks terminali DDE/RTD ile Excel'i anlik besler.
  15 saniyede bir fiyat degisimlerini Render API'ye push eder.
  Tavan bozulma, taban acilma, yuzde dusus bildirimlerini tetikler.

Calismasi:
  1. Excel'i okur
  2. Her satir icin TARIH = bugun mu kontrol eder
  3. SON = 0 ise → borsa kapali, atla
  4. SON > 0 ise → veriyi hazirla:
     - SON == TAVAN fiyati → hit_ceiling = True
     - SON == TABAN fiyati → hit_floor = True
     - Yoksa normal kapanis
  5. API'deki mevcut gun sayisina bakarak trading_day hesaplar
  6. Bulk endpoint ile Render'a gonderir

Kullanim:
  python excel_sync.py                          # Otomatik (bugunun verisini yukle)
  python excel_sync.py --file C:\\tavan.xlsx     # Belirli dosyadan oku
  python excel_sync.py --dry-run                # Gonderme, sadece goster

Task Scheduler ile 18:20'de otomatik calistirmak icin:
  schtasks /create /tn "BistFinans_TavanSync" /tr "python C:\\bist-finans-backend\\excel_sync.py" /sc daily /st 18:20
"""

import os
import sys
import json
import time
import argparse
import requests
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pathlib import Path

# ============================================
# Ayarlar
# ============================================

API_URL = os.getenv("BIST_API_URL", "https://sz-bist-finans-api.onrender.com")
ADMIN_PASSWORD = os.getenv("BIST_ADMIN_PW", "zenger7245175")

# Varsayilan Excel dosya yolu — masaustunde
DEFAULT_EXCEL_PATH = str(Path.home() / "Desktop" / "halka arz TAVAN TABAN.xlsm")

# Tavan/taban eslesme toleransi (kurus)
# 0.004 TL = yarım kuruşun altı → SADECE birebir eşleşme sayılır
# BIST en küçük tick = 0.01 TL, dolayısıyla 1 tick uzaktaki fiyat asla match olmaz
PRICE_TOLERANCE = Decimal("0.004")

# Veri gelmezse Telegram uyarisi (saniye)
STALE_DATA_TIMEOUT = 180  # 3 dakika


# Restart-safe state dosyasi — script ile ayni dizinde
# Elektrik gidip PC yeniden acildiginda bildirim state'i buradan yuklenir
STATE_FILE = str(Path(__file__).parent / "bist_sync_state.json")


def log(msg):
    """Zaman damgali log."""
    ts = datetime.now().strftime("%H:%M:%S")
    try:
        print(f"[{ts}] {msg}")
    except UnicodeEncodeError:
        # Windows console charmap sorunu — ASCII'ye donustur
        safe = msg.encode("ascii", errors="replace").decode("ascii")
        print(f"[{ts}] {safe}")


def _send_telegram_alert(text: str):
    """Admin'e Telegram uyarisi gonder — Render API uzerinden.

    Lokal PC'de token olmayabilir, bu yuzden backend'deki
    /api/v1/admin/send-telegram endpointini kullanir.
    Endpoint yoksa direkt Telegram API'yi dener (env var varsa).
    """
    # Yol 1: Render API uzerinden gonder
    try:
        resp = requests.post(
            f"{API_URL}/api/v1/admin/send-telegram",
            json={
                "admin_password": ADMIN_PASSWORD,
                "text": text,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            log("  Telegram uyarisi gonderildi (API uzerinden)")
            return
        log(f"  Telegram API endpoint hata: {resp.status_code}")
    except Exception as e:
        log(f"  Telegram API gonderim hatasi: {e}")

    # Yol 2: Direkt Telegram API (env var varsa)
    bot_token = os.getenv("ADMIN_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("ADMIN_TELEGRAM_CHAT_ID", "")
    if bot_token and chat_id:
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
                timeout=10,
            )
            if resp.status_code == 200:
                log("  Telegram uyarisi gonderildi (direkt)")
        except Exception as e:
            log(f"  Telegram direkt gonderim hatasi: {e}")


def _load_state(today_str: str) -> dict | None:
    """Restart-safe: onceki session state'ini dosyadan yukle (sadece bugunkunu).

    Elektrik/program kapandiktan sonra yeniden baslarken mevcut tavan/taban
    durumlarinin 'yeni olay' sayilmamasi icin onceki state yuklenir.
    Dosya baska gunun state'ini iceriyorsa None doner (yeni gun, temiz basla).
    """
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        if state.get("date") != today_str:
            log(f"  State dosyasi baska gun ({state.get('date')}) — yeni gun, temiz basliyor")
            return None
        log(f"  Restart state yuklendi: ceiling={list(state.get('prev_hit_ceiling', {}).keys())}"
            f" | floor={list(state.get('prev_hit_floor', {}).keys())}"
            f" | pct={state.get('pct_alerts_sent', {})}"
            f" | opening={state.get('opening_notif_sent')} closing={state.get('closing_notif_sent')}")
        return state
    except FileNotFoundError:
        return None
    except Exception as e:
        log(f"  State yukle HATA: {e} — temiz basliyor")
        return None


def _save_state(today_str: str, prev_hit_ceiling: dict, prev_hit_floor: dict,
                pct_alerts_sent: dict, opening_notif_sent: bool, closing_notif_sent: bool):
    """Mevcut bildirim state'ini dosyaya kaydet — sonraki restart'ta yuklenecek."""
    try:
        state = {
            "date": today_str,
            "prev_hit_ceiling": prev_hit_ceiling,
            "prev_hit_floor": prev_hit_floor,
            # set → list (JSON serializable)
            "pct_alerts_sent": {k: list(v) for k, v in pct_alerts_sent.items()},
            "opening_notif_sent": opening_notif_sent,
            "closing_notif_sent": closing_notif_sent,
        }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception as e:
        log(f"  State kaydet HATA: {e}")


def parse_price(val):
    """Herhangi bir fiyat degerini Decimal'e cevir."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return Decimal(str(val))
    s = str(val).strip().replace(",", ".").replace(" ", "")
    if not s or s == "0" or s == "0.0":
        return Decimal("0")
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def parse_date_cell(val):
    """Excel tarih hucresini date objesine cevir."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    # "09/02/2026 00:00:00.00000" formati
    for fmt in ["%d/%m/%Y %H:%M:%S.%f", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(s.split(".")[0] if "." in s and len(s) > 15 else s, fmt.split(".")[0] if "." in fmt else fmt).date()
        except ValueError:
            continue
    return None


# Borsa Istanbul resmi tatil gunleri (2026)
# Hafta sonlarina denk gelenler zaten weekday filtresinde elenecek
BORSA_TATIL_2026 = {
    date(2026, 1, 1),   # Yilbasi
    date(2026, 3, 20),  # Nevruz Bayrami (Cuma) — borsa kapali
    date(2026, 4, 23),  # Ulusal Egemenlik ve Cocuk Bayrami
    date(2026, 5, 1),   # Emek ve Dayanisma Gunu
    date(2026, 5, 19),  # Ataturk'u Anma, Genclik ve Spor Bayrami
    date(2026, 6, 5),   # Kurban Bayrami Arefesi (yarim gun — tam tatil say)
    date(2026, 6, 6),   # Kurban Bayrami 1. gun (Cumartesi zaten)
    date(2026, 6, 7),   # Kurban Bayrami 2. gun (Pazar zaten)
    date(2026, 6, 8),   # Kurban Bayrami 3. gun (Pazartesi)
    date(2026, 6, 9),   # Kurban Bayrami 4. gun (Sali)
    date(2026, 7, 15),  # Demokrasi ve Milli Birlik Gunu
    date(2026, 8, 30),  # Zafer Bayrami (Pazar zaten)
    date(2026, 10, 29), # Cumhuriyet Bayrami
}


def _count_business_days(start_date, end_date):
    """Iki tarih arasindaki is gunu sayisini hesapla (tatiller haric, her ikisi dahil)."""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    count = 0
    current = start_date
    from datetime import timedelta as td
    while current <= end_date:
        if current.weekday() < 5 and current not in BORSA_TATIL_2026:
            count += 1
        current += td(days=1)
    return count


def read_matriks_excel_live(filepath):
    """
    WIN32COM ile ACIK OLAN Excel'den canli Matriks verilerini oku.
    Matriks terminali DDE/RTD ile Excel'i anlik besliyor — bu fonksiyon
    formul sonuclarini (canli fiyatlari) okur.

    Sutunlar:
    A: ILK ISLEM | B: HISSE | C: TAVAN | D: TABAN | E: ALIS | F: SATIS
    G: SON | H: %G FARK | I: TARIH | J: G.EN YUKSEK | K: ALISTAKI LOT | L: SATISTAKI LOT
    M: GUNLUK ADET | N: SENET SAYISI

    Returns: list of dict
    """
    import win32com.client

    file_name = Path(filepath).name

    # Calisan Excel uygulamasina baglan
    excel = win32com.client.GetObject(Class="Excel.Application")

    # Acik workbook'lar arasinda dosyayi ara
    wb = None
    for workbook in excel.Workbooks:
        if workbook.Name.lower() == file_name.lower():
            wb = workbook
            break

    if wb is None:
        log(f"HATA: Excel acik ama '{file_name}' dosyasi acik degil!")
        return []

    sheet = wb.ActiveSheet
    rows = []
    row_idx = 2  # 1. satir header

    while row_idx <= 50:  # Max 50 satir (guvenlik siniri)
        # B: HISSE
        ticker_val = sheet.Range(f"B{row_idx}").Value
        if ticker_val is None or str(ticker_val).strip() == "":
            break

        ticker = str(ticker_val).strip().upper()

        # C: TAVAN, D: TABAN
        tavan_limit = parse_price(sheet.Range(f"C{row_idx}").Value)
        taban_limit = parse_price(sheet.Range(f"D{row_idx}").Value)

        # E: ALIS (kademe fiyati), F: SATIS (kademe fiyati)
        alis_fiyat = parse_price(sheet.Range(f"E{row_idx}").Value)
        satis_fiyat = parse_price(sheet.Range(f"F{row_idx}").Value)

        # G: SON (anlik/kapanis fiyati)
        son_price = parse_price(sheet.Range(f"G{row_idx}").Value)

        # H: %G FARK (gunluk degisim)
        # Matriks Excel formati: %G FARK hucresinin NumberFormat'i "%"
        # iceriyorsa deger oran olarak gelir (0.10 = %10), yoksa zaten yuzde (10 = %10).
        # Guvenli yontem: hucrenin NumberFormat'ina bak.
        pct_cell = sheet.Range(f"H{row_idx}")
        pct_raw = pct_cell.Value
        daily_pct = None
        if pct_raw is not None:
            try:
                pct_float = float(pct_raw)
                number_format = str(pct_cell.NumberFormat)
                if "%" in number_format:
                    # Oran formati: 0.10 → %10
                    daily_pct = Decimal(str(round(pct_float * 100, 4)))
                else:
                    # Zaten yuzde: 10.0 → %10
                    daily_pct = Decimal(str(round(pct_float, 4)))
            except (ValueError, TypeError):
                pass

        # I: TARIH
        tarih_val = sheet.Range(f"I{row_idx}").Value
        tarih = parse_date_cell(tarih_val)

        # J: GUN ICI EN YUKSEK
        gun_en_yuksek = parse_price(sheet.Range(f"J{row_idx}").Value)

        # K: ALISTAKI LOT
        alis_lot_val = sheet.Range(f"K{row_idx}").Value
        alis_lot = int(float(alis_lot_val)) if alis_lot_val and float(alis_lot_val) > 0 else None

        # L: SATISTAKI LOT
        satis_lot_val = sheet.Range(f"L{row_idx}").Value
        satis_lot = int(float(satis_lot_val)) if satis_lot_val and float(satis_lot_val) > 0 else None

        # A: ILK ISLEM tarihi (E.D.O filtresi icin)
        # Sadece 10 Mart 2026 ve sonrasi IPO'lar icin EDO verisi gonder
        ilk_islem_val = sheet.Range(f"A{row_idx}").Value
        edo_eligible = False
        if ilk_islem_val:
            try:
                from datetime import datetime as _dt
                if hasattr(ilk_islem_val, 'year'):
                    ilk_islem_date = ilk_islem_val
                else:
                    ilk_islem_date = _dt.strptime(str(ilk_islem_val).strip(), "%d.%m.%Y")
                # 10 Mart 2026 ve sonrasi → EDO aktif
                if ilk_islem_date.year > 2026 or (ilk_islem_date.year == 2026 and (ilk_islem_date.month > 3 or (ilk_islem_date.month == 3 and ilk_islem_date.day >= 10))):
                    edo_eligible = True
            except Exception:
                pass

        # M: GUNLUK ADET (E.D.O icin — sadece eligible IPO'lar)
        gunluk_adet = None
        senet_sayisi = None
        if edo_eligible:
            gunluk_adet_val = sheet.Range(f"M{row_idx}").Value
            gunluk_adet = int(float(gunluk_adet_val)) if gunluk_adet_val and float(gunluk_adet_val) > 0 else None

            # N: SENET SAYISI (E.D.O icin)
            senet_sayisi_val = sheet.Range(f"N{row_idx}").Value
            senet_sayisi = int(float(senet_sayisi_val)) if senet_sayisi_val and float(senet_sayisi_val) > 0 else None

        rows.append({
            "ticker": ticker,
            "tavan_limit": tavan_limit,
            "taban_limit": taban_limit,
            "alis_fiyat": alis_fiyat,
            "satis_fiyat": satis_fiyat,
            "son": son_price,
            "daily_pct": daily_pct,
            "tarih": tarih,
            "gun_en_yuksek": gun_en_yuksek,
            "alis_lot": alis_lot,
            "satis_lot": satis_lot,
            "gunluk_adet": gunluk_adet,
            "senet_sayisi": senet_sayisi,
            "row_idx": row_idx,
        })

        row_idx += 1

    log(f"  {len(rows)} satir okundu (canli)")
    return rows


def read_matriks_excel(filepath):
    """
    Matriks Excel formatini oku — openpyxl ile (kaydedilmis veri).
    NOT: Canli mod (--live) icin read_matriks_excel_live() kullanilir.

    Beklenen sutunlar:
    A: ILK ISLEM | B: HISSE | C: TAVAN | D: TABAN | E: ALIS | F: SATIS | G: SON | H: %G FARK | I: TARIH | J: G.EN YUKSEK

    Returns: list of dict
    """
    try:
        import openpyxl
    except ImportError:
        log("HATA: openpyxl kurulu degil. Kur: pip install openpyxl")
        sys.exit(1)

    log(f"Excel okunuyor: {filepath}")
    wb = openpyxl.load_workbook(filepath, data_only=True, keep_links=False)
    ws = wb.active

    rows = []
    for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        if not row or len(row) < 7:
            continue

        # B: HISSE
        ticker = str(row[1]).strip().upper() if row[1] else None
        if not ticker:
            continue

        # C: TAVAN, D: TABAN
        tavan_limit = parse_price(row[2])
        taban_limit = parse_price(row[3])

        # G: SON (kapanis fiyati)
        son_price = parse_price(row[6])

        # H: %G FARK (gunluk degisim)
        daily_pct = parse_price(row[7]) if len(row) > 7 else None

        # I: TARIH
        tarih = parse_date_cell(row[8]) if len(row) > 8 else None

        # J: G.EN YUKSEK (gun ici en yuksek fiyat)
        gun_en_yuksek = parse_price(row[9]) if len(row) > 9 else None

        rows.append({
            "ticker": ticker,
            "tavan_limit": tavan_limit,
            "taban_limit": taban_limit,
            "alis_fiyat": parse_price(row[4]),
            "satis_fiyat": parse_price(row[5]),
            "son": son_price,
            "daily_pct": daily_pct,
            "tarih": tarih,
            "gun_en_yuksek": gun_en_yuksek,
            "alis_lot": None,
            "satis_lot": None,
            "row_idx": row_idx,
        })

    wb.close()
    log(f"  {len(rows)} satir okundu")
    return rows


def get_active_trading_ipos():
    """API'den aktif islem goren halka arzlari getir — her biri icin son trading_day.

    Sections endpoint'inin hem 'trading' hem 'performance_archive' bolumlerini okur.
    Boylece 25 takvim gunu filtresi nedeniyle trading'den dusen IPO'lar da yakalanir.
    """
    try:
        resp = requests.get(f"{API_URL}/api/v1/ipos/sections", timeout=60)
        resp.raise_for_status()
        data = resp.json()
        result = {}

        # Hem trading hem performance_archive bolumlerini tara
        all_ipos = []
        all_ipos.extend(data.get("trading", []))
        all_ipos.extend(data.get("performance_archive", []))

        for ipo in all_ipos:
            ticker = ipo.get("ticker")
            if not ticker:
                continue
            # Arsivlenmis olanlari atla — sadece aktif trading olanlari al
            if ipo.get("archived"):
                continue

            tracks = ipo.get("ceiling_tracks", [])
            max_day_from_tracks = max((t["trading_day"] for t in tracks), default=0) if tracks else 0

            # DB'deki resmi trading_day_count degerini de al
            db_day_count = ipo.get("trading_day_count") or 0

            # En yuksek degeri kullan
            effective_day = max(max_day_from_tracks, db_day_count)

            result[ticker] = {
                "ipo_id": ipo["id"],
                "ipo_price": parse_price(ipo.get("ipo_price")),
                "trading_day_count": effective_day,
                "trading_start": ipo.get("trading_start"),
                "last_close": None,
            }
            if tracks:
                last_track = max(tracks, key=lambda t: t["trading_day"])
                result[ticker]["last_close"] = parse_price(last_track.get("close_price"))

            log(f"  API: {ticker} — tracks_max={max_day_from_tracks}, db_count={db_day_count} → effective={effective_day}")
        return result
    except Exception as e:
        log(f"HATA: API baglantisi basarisiz: {e}")
        return {}


def upload_tracks(tracks):
    """Ceiling track verilerini API'ye yukle."""
    if not tracks:
        log("Yuklenecek veri yok.")
        return None

    payload = {
        "admin_password": ADMIN_PASSWORD,
        "tracks": tracks,
    }

    try:
        log(f"API'ye {len(tracks)} kayit gonderiliyor...")
        resp = requests.post(
            f"{API_URL}/api/v1/admin/bulk-ceiling-track",
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        result = resp.json()
        log(f"  Yuklendi: {result.get('loaded', 0)} | Hata: {result.get('errors', 0)}")
        if result.get("error_details"):
            for err in result["error_details"]:
                log(f"    HATA: {err}")
        return result
    except Exception as e:
        log(f"  HATA: Yukleme basarisiz: {e}")
        return None


_WARMUP_ACTIVE = True  # live_sync() tarafindan False yapilir

def _send_realtime_notification(ticker, notif_type, title, body, sub_event=None):
    """Render API'ye anlik bildirim gonder (tavan bozulma, taban acilma, yuzde dusus)."""
    global _WARMUP_ACTIVE
    if _WARMUP_ACTIVE:
        log(f"  ⏳ WARMUP SKIP: {ticker} {notif_type} — bildirim gönderilmedi")
        return
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
        resp = requests.post(
            f"{API_URL}/api/v1/realtime-notification",
            json=payload,
            timeout=60,  # Render cold start icin 60sn (eskiden 30sn)
        )
        if resp.status_code == 200:
            data = resp.json()
            sent = data.get('notifications_sent', 0)
            subs = data.get('active_subscribers', 0)
            errs = data.get('errors', 0)
            status = data.get('status', 'ok')
            skip = data.get('skip_reasons', {})

            if status == "skipped":
                reason = data.get('reason', '?')
                log(f"  ⏭ Bildirim SKIP: {ticker} {notif_type} → reason={reason}")
            elif sent > 0:
                log(f"  📢 Bildirim OK: {ticker} {notif_type} → gönderildi={sent} abone={subs}")
            else:
                # 0 gönderildi — detaylı nedenleri göster
                skip_str = ", ".join(f"{k}={v}" for k, v in skip.items() if v > 0) if skip else "bilinmiyor"
                log(f"  ⚠ Bildirim 0: {ticker} {notif_type} → abone={subs} hata={errs} | {skip_str}")
        else:
            log(f"  ⚠ Bildirim HTTP hatasi: {ticker} {notif_type} → HTTP {resp.status_code}")
            try:
                log(f"    Detay: {resp.text[:200]}")
            except Exception:
                pass
    except requests.exceptions.Timeout:
        log(f"  ⚠ Bildirim TIMEOUT: {ticker} {notif_type} → 60sn icinde cevap gelmedi (Render uyuyor olabilir)")
    except Exception as e:
        log(f"  ⚠ Bildirim gonderilemedi: {ticker} {notif_type} → {e}")


def live_sync(filepath, interval=15):
    """Canli sync modu — Excel'i belirli aralikla okuyup degisen fiyatlari API'ye gonderir.

    Matriks terminali acikken Excel anlik guncellenir.
    Bu fonksiyon Excel'i 'interval' saniyede bir okur,
    degisen fiyatlari tespit eder ve sadece degisenleri API'ye push eder.

    Seans saatleri: Hafta ici 09:54 - 18:20 arasi calisir.
    Hafta sonu ve seans disi saatlerde uyku moduna gecer.
    Her yeni is gunu basinda cache temizlenir ve IPO listesi yenilenir.

    Ek olarak durum degisikliklerini tespit eder:
    - Tavan bozulma (tavan → normal/taban)
    - Taban acilma (taban → normal/tavan)
    - Yuzde dusus (%4 ve %7 esik)
    ve Render'a realtime bildirim gonderir.
    """
    log("=" * 55)
    log(f"BIST Finans — CANLI SYNC MODU")
    log(f"Excel: {filepath}")
    log(f"API: {API_URL}")
    log(f"Guncelleme araligi: {interval} saniye")
    log(f"Durdurmak icin Ctrl+C")
    log("=" * 55)

    # win32com dosyanin ACIK olmasini gerektirir — os.path.exists kontrolu yetersiz
    # Excel'e baglanarak kontrol edilir (read_matriks_excel_live icinde)

    # API'den aktif IPO bilgilerini al (bir kere)
    log("\nAPI'den aktif IPO bilgileri aliniyor...")
    active_ipos = get_active_trading_ipos()
    if not active_ipos:
        log("HATA: API'de aktif IPO bulunamadi!")
        sys.exit(1)
    log(f"  {len(active_ipos)} aktif IPO bulundu: {', '.join(active_ipos.keys())}")

    # Onceki durumlari tut — degisiklikleri tespit icin
    prev_prices = {}       # {ticker: Decimal(son_fiyat)}
    prev_hit_ceiling = {}  # {ticker: bool}
    prev_hit_floor = {}    # {ticker: bool}
    prev_lots = {}         # {ticker: (alis_lot, satis_lot)} — lot degisimi takibi
    pct_alerts_sent = {}   # {ticker: set("pct4","pct7")} — gun ici tekrar gonderme
    ceiling_streak = {}    # {ticker: int} — pozitif=tavanda ardışık döngü, negatif=tavandan uzak ardışık döngü
    floor_streak = {}      # {ticker: int} — pozitif=tabanda ardışık döngü, negatif=tabandan uzak ardışık döngü
    CONFIRM_CYCLES = 2     # 2 ardışık döngü (30sn) onay — sınır zıplaması spam engeli
    TIME_FALLBACK_SEC = 300  # 5 dakika — bu süre geçtiyse tek döngü (±1) yeterli
    # Son bildirim zamanı ve yönü — time-based fallback için
    ceiling_last_notif: dict[str, tuple[float, str]] = {}  # {ticker: (timestamp, "locked"|"opened")}
    floor_last_notif: dict[str, tuple[float, str]] = {}    # {ticker: (timestamp, "locked"|"opened")}
    opening_notif_sent = False   # Gunluk acilis bildirimi gonderildi mi
    closing_notif_sent = False   # Gunluk kapanis bildirimi gonderildi mi
    cycle = 0
    last_session_date = None  # Seans gun degisiminde cache temizle + IPO yenile
    last_data_time = None        # Son veri degisikligi zamani
    stale_alert_sent = False     # 3dk uyarisi gonderildi mi (tekrar spam engeli)

    # ── Restart-safe state yukle ──────────────────────────────────────
    # Elektrik gidip/program kapanip yeniden acilinca mevcut tavan/taban
    # durumlarinin 'yeni olay' sayilmamasi icin onceki state dosyadan yuklenir.
    today_str = date.today().isoformat()
    saved = _load_state(today_str)
    if saved:
        prev_hit_ceiling = saved.get("prev_hit_ceiling", {})
        prev_hit_floor = saved.get("prev_hit_floor", {})
        pct_alerts_sent = {k: set(v) for k, v in saved.get("pct_alerts_sent", {}).items()}
        opening_notif_sent = saved.get("opening_notif_sent", False)
        closing_notif_sent = saved.get("closing_notif_sent", False)
        last_session_date = date.today()  # State bugune ait → gun degisimi tetiklemeyi engelle
        # Streak'leri kayıtlı state'ten başlat (zaten onaylanmış durumlar)
        for t, val in prev_hit_ceiling.items():
            ceiling_streak[t] = CONFIRM_CYCLES if val else -CONFIRM_CYCLES
        for t, val in prev_hit_floor.items():
            floor_streak[t] = CONFIRM_CYCLES if val else -CONFIRM_CYCLES
        log("  ✅ Restart-safe: onceki bildirim state'i yuklendi — duplicate bildirim engellendi")
    else:
        log("  ℹ State dosyasi yok/baska gun — temiz basliyor")

    # ── Warmup: ilk 5 döngü sadece state topla, bildirim gönderme ──
    global _WARMUP_ACTIVE
    _WARMUP_ACTIVE = True
    WARMUP_CYCLES = 5  # 5 x 15sn = 75sn
    _warmup_done = False

    try:
        while True:
            cycle += 1
            now_dt = datetime.now()
            now = now_dt.strftime("%H:%M:%S")

            if not _warmup_done and cycle <= WARMUP_CYCLES:
                # Warmup modunda: state topla ama bildirim gönderme
                if cycle == 1:
                    log(f"[{now}] ⏳ WARMUP: ilk {WARMUP_CYCLES} döngü state toplanıyor, bildirim gönderilmeyecek...")
            elif not _warmup_done and cycle > WARMUP_CYCLES:
                _warmup_done = True
                _WARMUP_ACTIVE = False
                log(f"[{now}] ✅ WARMUP tamamlandı — bildirimler aktif")

            # ── Seans disi calismayi engelle ──
            # Hafta ici: 09:54 - 18:20 arasi calis
            # Hafta sonu (Cumartesi=5, Pazar=6): hic calisma
            weekday = now_dt.weekday()  # 0=Pzt ... 6=Pzr
            hour_min = now_dt.hour * 100 + now_dt.minute  # 0954, 1820 gibi

            if weekday >= 5:
                # Hafta sonu — 10 dakikada bir kontrol et
                if cycle == 1 or cycle % 40 == 0:
                    log(f"[{now}] Hafta sonu — seans yok, bekleniyor...")
                time.sleep(interval)
                continue

            if hour_min < 954:
                # Seans oncesi — 09:54'u bekle
                if cycle == 1 or cycle % 40 == 0:
                    log(f"[{now}] Seans oncesi — 09:54'te baslanacak...")
                time.sleep(interval)
                continue

            if hour_min > 1820:
                # Seans sonrasi — 18:20'den sonra yapacak is yok
                if cycle == 1 or cycle % 40 == 0:
                    log(f"[{now}] Seans kapandi — yarin 09:54'te devam edilecek...")
                time.sleep(interval)
                continue

            # ── Gun degisiminde cache temizle + IPO listesini yenile ──
            today = date.today()
            today_str = today.isoformat()
            if last_session_date != today:
                log(f"[{now}] Yeni seans gunu: {today_str} — cache temizleniyor, IPO listesi yenileniyor...")
                prev_prices.clear()
                prev_hit_ceiling.clear()
                prev_hit_floor.clear()
                prev_lots.clear()
                pct_alerts_sent.clear()
                ceiling_streak.clear()
                floor_streak.clear()
                opening_notif_sent = False
                closing_notif_sent = False
                last_data_time = None
                stale_alert_sent = False
                # Yeni gun icin temiz state kaydet
                _save_state(today_str, {}, {}, {}, False, False)
                try:
                    fresh = get_active_trading_ipos()
                    if fresh:
                        active_ipos = fresh
                        log(f"  {len(active_ipos)} aktif IPO: {', '.join(active_ipos.keys())}")
                except Exception as e:
                    log(f"  IPO yenileme hatasi (mevcut liste kullanilacak): {e}")
                last_session_date = today

            try:
                rows = read_matriks_excel_live(filepath)
            except Exception as e:
                log(f"[{now}] Excel okuma hatasi: {e} — {interval}s sonra tekrar...")
                _consecutive_empty_reads = locals().get("_consecutive_empty_reads", 0) + 1
                time.sleep(interval)
                continue

            if not rows:
                log(f"[{now}] Excel bos — {interval}s sonra tekrar...")
                _consecutive_empty_reads = locals().get("_consecutive_empty_reads", 0) + 1
                time.sleep(interval)
                continue

            # Excel bos/hata sonrasi veri gelince warmup'i sifirla
            # (kullanici Excel'i kapatip/acinca ilk gelen veri tavan/taban cozuldu/kalkti gibi
            #  yanlis bildirim tetiklemesin)
            _prev_empty = locals().get("_consecutive_empty_reads", 0)
            if _prev_empty >= 3 and _warmup_done:
                _warmup_done = False
                _WARMUP_ACTIVE = True
                cycle = 0  # Warmup sayacini sifirla
                log(f"[{now}] 🔄 Excel verisi geri geldi ({_prev_empty} bos okumadan sonra) — WARMUP sifirlandi, {WARMUP_CYCLES} dongu bildirim yok")
            _consecutive_empty_reads = 0
            changed_tracks = []

            for row in rows:
                ticker = row["ticker"]
                son = row.get("son")
                tarih = row.get("tarih")

                # Tarih kontrolu — sadece bugunun verisini al
                if tarih and tarih != today:
                    continue

                # SON = 0 → borsa kapali
                if son is None or son == 0:
                    continue

                # Fiyat veya lot degisti mi?
                alis_lot_raw = row.get("alis_lot")
                satis_lot_raw = row.get("satis_lot")
                lot_key = (alis_lot_raw, satis_lot_raw)
                price_same = ticker in prev_prices and prev_prices[ticker] == son
                lot_same = prev_lots.get(ticker) == lot_key

                if price_same and lot_same:
                    continue  # Fiyat ve lot ayni, atla

                # Degismis — track hazirla
                prev_prices[ticker] = son
                prev_lots[ticker] = lot_key

                tavan_limit = row.get("tavan_limit")
                taban_limit = row.get("taban_limit")

                hit_ceiling = bool(tavan_limit and son and abs(son - tavan_limit) <= PRICE_TOLERANCE)
                hit_floor = bool(taban_limit and son and abs(son - taban_limit) <= PRICE_TOLERANCE)

                ipo_info = active_ipos.get(ticker)
                if not ipo_info:
                    continue

                next_day = ipo_info["trading_day_count"] + 1
                # Guvenlik: trading_start'tan HER ZAMAN dogrula (stale count koruması)
                trading_start = ipo_info.get("trading_start")
                if trading_start:
                    from_start = _count_business_days(trading_start, today)
                    if from_start > 0 and from_start != next_day:
                        if cycle == 1:  # Sadece ilk dongude logla (spam onleme)
                            log(f"  DUZELTME: {ticker} — DB next_day={next_day}, trading_start'tan hesap={from_start} → {from_start} kullaniliyor")
                        next_day = from_start

                track = {
                    "ticker": ticker,
                    "trading_day": next_day,
                    "trade_date": today.isoformat(),
                    "close_price": str(son),
                    "hit_ceiling": hit_ceiling,
                    "hit_floor": hit_floor,
                }

                gun_en_yuksek = row.get("gun_en_yuksek")
                if gun_en_yuksek and gun_en_yuksek > 0:
                    track["high_price"] = str(gun_en_yuksek)
                elif tavan_limit and tavan_limit > 0:
                    track["high_price"] = str(tavan_limit)
                if taban_limit and taban_limit > 0:
                    track["low_price"] = str(taban_limit)

                # Gunluk % degisim
                daily_pct = row.get("daily_pct")
                if daily_pct is not None:
                    track["pct_change"] = str(daily_pct)

                # Alis/satis lot (K ve L sutunlari — yukarida alis_lot_raw/satis_lot_raw okundu)
                if alis_lot_raw:
                    track["alis_lot"] = alis_lot_raw
                if satis_lot_raw:
                    track["satis_lot"] = satis_lot_raw

                # E.D.O: Gunluk adet ve senet sayisi (M ve N sutunlari)
                gunluk_adet_raw = row.get("gunluk_adet")
                senet_sayisi_raw = row.get("senet_sayisi")
                if gunluk_adet_raw:
                    track["gunluk_adet"] = gunluk_adet_raw
                if senet_sayisi_raw:
                    track["senet_sayisi"] = senet_sayisi_raw

                status = "TAVAN" if hit_ceiling else ("TABAN" if hit_floor else "NORMAL")
                changed_tracks.append(track)
                pct_str = f" %{float(daily_pct):+.1f}" if daily_pct else ""
                log(f"  {ticker}: {son} TL {status}{pct_str}")

                # ── Anlık bildirim tespiti ──

                pct_val = float(daily_pct) if daily_pct is not None else 0.0
                fark_str = f"%+{abs(pct_val):.1f}" if pct_val >= 0 else f"%-{abs(pct_val):.1f}"
                son_str = f"{float(son):.2f}" if son else ""

                # ════════════════════════════════════════════════════════
                # TAVAN / TABAN BİLDİRİM SİSTEMİ — STREAK DEBOUNCE
                #
                # Cooldown yerine "streak" (ardışık döngü sayacı) kullanır.
                # Tavanda → streak pozitif artar (+1, +2, +3...)
                # Tavandan uzak → streak negatif artar (-1, -2, -3...)
                # Yön değişince streak sıfırdan başlar.
                #
                # Bildirim SADECE streak tam CONFIRM_CYCLES'a ulaşınca gider.
                # Bu sayede:
                #   ✅ Gerçek tavan: 30sn sonra onaylanır, bildirim gider
                #   ✅ Gerçek bozulma: 30sn sonra onaylanır, bildirim gider
                #   ✅ Tekrar tavan: yeni streak başlar, 30sn sonra bildirim gider
                #   ❌ Sınır zıplaması: streak hiç ±2'ye ulaşamaz, spam olmaz
                # ════════════════════════════════════════════════════════

                # ── TAVAN STREAK ──
                c_str = ceiling_streak.get(ticker, 0)
                if hit_ceiling:
                    c_str = (c_str + 1) if c_str >= 0 else 1
                else:
                    c_str = (c_str - 1) if c_str <= 0 else -1
                ceiling_streak[ticker] = c_str

                can_notify = opening_notif_sent or hour_min > 1000

                # Time-based fallback: 5dk geçtiyse tek döngü (±1) yeterli
                c_last = ceiling_last_notif.get(ticker)
                c_time_ok = (c_last is None) or (time.time() - c_last[0] >= TIME_FALLBACK_SEC)
                c_dir_changed = (c_last is None) or (
                    (c_str > 0 and c_last[1] != "locked") or
                    (c_str < 0 and c_last[1] != "opened")
                )
                c_threshold = 1 if (c_dir_changed or c_time_ok) else CONFIRM_CYCLES

                if c_str > 0 and c_str == c_threshold:
                    # Tavanda kaldı → onaylandı
                    if can_notify:
                        log(f"  🔔 TAVANA KİTLEDİ: {ticker} ({son_str} TL) [threshold={c_threshold}]")
                        _send_realtime_notification(
                            ticker, "tavan_bozulma",
                            f"🔒 {ticker} Tavana Kitledi!",
                            f"Son: {son_str} TL | Fark: {fark_str}",
                        )
                        ceiling_last_notif[ticker] = (time.time(), "locked")
                        # Streak'i threshold ötesine atla — çift bildirim engeli
                        c_str = CONFIRM_CYCLES + 1
                        ceiling_streak[ticker] = c_str
                    else:
                        log(f"  ⏳ TAVANA KİTLEDİ ama açılış bildirimi bekleniyor: {ticker} ({son_str} TL)")

                elif c_str < 0 and c_str == -c_threshold:
                    # Tavandan uzak kaldı → onaylandı
                    if can_notify:
                        log(f"  🔔 TAVAN ÇÖZÜLDÜ: {ticker} ({son_str} TL) [threshold={c_threshold}]")
                        _send_realtime_notification(
                            ticker, "tavan_bozulma",
                            f"🔓 {ticker} Tavan Çözüldü!",
                            f"Son: {son_str} TL | Fark: {fark_str}",
                        )
                        ceiling_last_notif[ticker] = (time.time(), "opened")
                        c_str = -(CONFIRM_CYCLES + 1)
                        ceiling_streak[ticker] = c_str
                    else:
                        log(f"  ⏳ TAVAN ÇÖZÜLDÜ ama açılış bildirimi bekleniyor: {ticker} ({son_str} TL)")

                # Tavan state güncelle (onaylanmış durumlardan)
                if c_str >= CONFIRM_CYCLES:
                    prev_hit_ceiling[ticker] = True
                elif c_str <= -CONFIRM_CYCLES:
                    prev_hit_ceiling[ticker] = False

                # ── TABAN STREAK ──
                f_str = floor_streak.get(ticker, 0)
                if hit_floor:
                    f_str = (f_str + 1) if f_str >= 0 else 1
                else:
                    f_str = (f_str - 1) if f_str <= 0 else -1
                floor_streak[ticker] = f_str

                # Time-based fallback: 5dk geçtiyse tek döngü (±1) yeterli
                f_last = floor_last_notif.get(ticker)
                f_time_ok = (f_last is None) or (time.time() - f_last[0] >= TIME_FALLBACK_SEC)
                f_dir_changed = (f_last is None) or (
                    (f_str > 0 and f_last[1] != "locked") or
                    (f_str < 0 and f_last[1] != "opened")
                )
                f_threshold = 1 if (f_dir_changed or f_time_ok) else CONFIRM_CYCLES

                if f_str > 0 and f_str == f_threshold:
                    if can_notify:
                        log(f"  🔔 TABANA KİTLEDİ: {ticker} ({son_str} TL) [threshold={f_threshold}]")
                        _send_realtime_notification(
                            ticker, "taban_acilma",
                            f"🔒 {ticker} Tabana Kitledi!",
                            f"Son: {son_str} TL | Fark: {fark_str}",
                        )
                        floor_last_notif[ticker] = (time.time(), "locked")
                        # Streak'i threshold ötesine atla — çift bildirim engeli
                        f_str = CONFIRM_CYCLES + 1
                        floor_streak[ticker] = f_str
                    else:
                        log(f"  ⏳ TABANA KİTLEDİ ama açılış bildirimi bekleniyor: {ticker} ({son_str} TL)")

                elif f_str < 0 and f_str == -f_threshold:
                    if can_notify:
                        log(f"  🔔 TABAN KALKTI: {ticker} ({son_str} TL) [threshold={f_threshold}]")
                        _send_realtime_notification(
                            ticker, "taban_acilma",
                            f"📈 {ticker} Taban Kalktı!",
                            f"Son: {son_str} TL | Fark: {fark_str}",
                        )
                        floor_last_notif[ticker] = (time.time(), "opened")
                        f_str = -(CONFIRM_CYCLES + 1)
                        floor_streak[ticker] = f_str
                    else:
                        log(f"  ⏳ TABAN KALKTI ama açılış bildirimi bekleniyor: {ticker} ({son_str} TL)")

                # Taban state güncelle (onaylanmış durumlardan)
                if f_str >= CONFIRM_CYCLES:
                    prev_hit_floor[ticker] = True
                elif f_str <= -CONFIRM_CYCLES:
                    prev_hit_floor[ticker] = False

                # 3. Yüzde düşüş: Günün en yükseğinden %4 ve %7 eşik (gün içi 1 kere)
                gun_ey = row.get("gun_en_yuksek")
                if gun_ey and float(gun_ey) > 0 and son and float(son) > 0:
                    drop_from_high = ((float(son) - float(gun_ey)) / float(gun_ey)) * 100
                    sent = pct_alerts_sent.get(ticker, set())
                    ey_str = f"{float(gun_ey):.2f}"
                    son_str = f"{float(son):.2f}"

                    if drop_from_high <= -7.0 and "pct7" not in sent:
                        log(f"  🔔 %7 DÜŞÜŞ (G.En Yüksek {ey_str}'den): {ticker} %{drop_from_high:.1f}")
                        _send_realtime_notification(
                            ticker, "yuzde_dusus",
                            f"🔻 {ticker} G.En Yüksek {ey_str} TL'den %7 Düştü!",
                            f"G.En Yüksek: {ey_str} TL → Şu an: {son_str} TL (%-{abs(drop_from_high):.1f})",
                            sub_event="pct7",
                        )
                        sent.add("pct7")
                        sent.add("pct4")
                        pct_alerts_sent[ticker] = sent

                    elif drop_from_high <= -4.0 and "pct4" not in sent:
                        log(f"  🔔 %4 DÜŞÜŞ (G.En Yüksek {ey_str}'den): {ticker} %{drop_from_high:.1f}")
                        _send_realtime_notification(
                            ticker, "yuzde_dusus",
                            f"⚠️ {ticker} G.En Yüksek {ey_str} TL'den %4 Düştü!",
                            f"G.En Yüksek: {ey_str} TL → Şu an: {son_str} TL (%-{abs(drop_from_high):.1f})",
                            sub_event="pct4",
                        )
                        sent.add("pct4")
                        pct_alerts_sent[ticker] = sent

            # Bildirim state'ini diske kaydet (restart-safe)
            # Her dongude kaydeder — maliyet dusuk (kucuk JSON dosyasi)
            _save_state(today_str, prev_hit_ceiling, prev_hit_floor,
                        pct_alerts_sent, opening_notif_sent, closing_notif_sent)

            # Degisen varsa gonder
            if changed_tracks:
                log(f"[{now}] #{cycle} — {len(changed_tracks)} hisse degisti, gonderiliyor...")
                result = upload_tracks(changed_tracks)
                if result and result.get("status") == "ok":
                    log(f"  ✓ {result.get('loaded', 0)} kayit yuklendi")
                else:
                    log(f"  ✗ Yukleme basarisiz")
                # Veri geldi → zamani guncelle, uyari sifirla
                last_data_time = time.monotonic()
                stale_alert_sent = False
            else:
                # Her 4 donguede bir (1 dakika) sessiz log
                if cycle % 4 == 0:
                    log(f"[{now}] #{cycle} — Degisiklik yok")

            # ── Stale data kontrolu (3 dk veri gelmezse Telegram uyarisi) ──
            # Sadece 10:00:30 - 17:59:30 arasi (seans icinde)
            hour_min_sec = now_dt.hour * 10000 + now_dt.minute * 100 + now_dt.second  # 100030, 175930
            if 100030 <= hour_min_sec <= 175930:
                if last_data_time is None and hour_min_sec >= 100330:
                    # Seans basladi ama hic veri gelmedi (10:00:30'dan 3dk sonra = 10:03:30)
                    if not stale_alert_sent:
                        _send_telegram_alert(
                            f"⚠️ <b>Excel Sync Uyarisi</b>\n"
                            f"Seans basladi ama henuz hic veri gelmedi!\n"
                            f"Matriks veya Excel baglantisini kontrol edin."
                        )
                        stale_alert_sent = True
                        log(f"  ⚠ STALE DATA: Seans basladi ama hic veri gelmedi — Telegram uyarisi gonderildi")
                elif last_data_time is not None:
                    elapsed = time.monotonic() - last_data_time
                    if elapsed >= STALE_DATA_TIMEOUT and not stale_alert_sent:
                        mins = int(elapsed // 60)
                        _send_telegram_alert(
                            f"⚠️ <b>Excel Sync Uyarisi</b>\n"
                            f"Son {mins} dakikadir veri degisikligi yok!\n"
                            f"Matriks veya Excel baglantisini kontrol edin."
                        )
                        stale_alert_sent = True
                        log(f"  ⚠ STALE DATA: {mins} dakikadir veri gelmedi — Telegram uyarisi gonderildi")

            # ── Günlük açılış bildirimi (09:56) ──
            # Seans açılışında abonelere push bildirim gönder
            # Format: Tavan/Taban → direkt, Alıcılı/Satıcılı → Açılış Gap: +/-%X.XX
            # Not: %0.00 gap nötr — bildirim gönderilmez
            if not opening_notif_sent and 956 <= hour_min <= 1000 and prev_prices:
                log(f"  {'='*50}")
                log(f"  AÇILIŞ BİLDİRİMİ GÖNDERİLİYOR")
                log(f"  {'='*50}")

                opening_count = 0
                for row in rows:
                    ticker = row["ticker"]
                    son = row.get("son")
                    if son is None or son == 0:
                        continue
                    tavan_limit = row.get("tavan_limit")
                    taban_limit = row.get("taban_limit")
                    daily_pct = row.get("daily_pct")
                    is_ceiling = bool(tavan_limit and son and abs(son - tavan_limit) <= PRICE_TOLERANCE)
                    is_floor = bool(taban_limit and son and abs(son - taban_limit) <= PRICE_TOLERANCE)

                    if is_ceiling:
                        title = f"🚀 Seans Açılış: {ticker} Tavan Açtı!"
                        body = f"{ticker} tavan fiyatından açıldı 🎯"
                        log(f"  {ticker}: TAVAN AÇTI!")
                    elif is_floor:
                        title = f"📉 Seans Açılış: {ticker} Taban Açtı!"
                        body = f"{ticker} taban fiyatından açıldı ⚠️"
                        log(f"  {ticker}: TABAN AÇTI!")
                    else:
                        pct_val = float(daily_pct) if daily_pct is not None else 0.0
                        # %0.00 nötr — bildirim gönderme
                        if abs(pct_val) < 0.005:
                            log(f"  {ticker}: NÖTR AÇILIŞ %0.00 — atlanıyor")
                            continue
                        gap_str = f"%+{abs(pct_val):.2f}" if pct_val >= 0 else f"%-{abs(pct_val):.2f}"
                        if pct_val >= 0:
                            title = f"🟢 Seans Açılış: {ticker} Alıcılı Açtı"
                            body = f"Açılış Gap: {gap_str}"
                            log(f"  {ticker}: ALICILI AÇTI {gap_str}")
                        else:
                            title = f"🔴 Seans Açılış: {ticker} Satıcılı Açtı"
                            body = f"Açılış Gap: {gap_str}"
                            log(f"  {ticker}: SATICILI AÇTI {gap_str}")

                    _send_realtime_notification(ticker, "gunluk_acilis_kapanis", title, body)
                    opening_count += 1
                    # Birden fazla hisse takip eden kullanıcıya spam olmaması için
                    # her hisse arasında 5 sn bekle
                    if opening_count < len(rows):
                        time.sleep(5)
                if opening_count > 0:
                    opening_notif_sent = True
                    # Streak'leri mevcut durumla senkronize et — açılış bildirimi
                    # zaten tavan/taban durumunu duyurdu, tekrar bildirim göndermeyi engelle
                    for row in rows:
                        t = row["ticker"]
                        s = row.get("son")
                        if s and s > 0:
                            tv = row.get("tavan_limit")
                            tb = row.get("taban_limit")
                            is_c = bool(tv and abs(s - tv) <= PRICE_TOLERANCE)
                            is_f = bool(tb and abs(s - tb) <= PRICE_TOLERANCE)
                            ceiling_streak[t] = CONFIRM_CYCLES if is_c else -CONFIRM_CYCLES
                            floor_streak[t] = CONFIRM_CYCLES if is_f else -CONFIRM_CYCLES
                            prev_hit_ceiling[t] = is_c
                            prev_hit_floor[t] = is_f
                    _save_state(today_str, prev_hit_ceiling, prev_hit_floor,
                                pct_alerts_sent, opening_notif_sent, closing_notif_sent)
                    log(f"  Açılış bildirimi: {opening_count} hisse için gönderildi (~{opening_count * 5}sn yayılarak)")
                log(f"  {'='*50}")

            # ── Günlük kapanış bildirimi (18:08) ──
            # Seans kapanışında abonelere push bildirim gönder
            # Format: Tavan/Taban → direkt, Alıcılı/Satıcılı → Günsonu Fark: +/-%X.XX
            # Not: %0.00 fark nötr — bildirim gönderilmez
            if not closing_notif_sent and 1808 <= hour_min <= 1820 and prev_prices:
                log(f"  {'='*50}")
                log(f"  KAPANIŞ BİLDİRİMİ GÖNDERİLİYOR")
                log(f"  {'='*50}")

                closing_count = 0
                for row in rows:
                    ticker = row["ticker"]
                    son = row.get("son")
                    if son is None or son == 0:
                        continue
                    tavan_limit = row.get("tavan_limit")
                    taban_limit = row.get("taban_limit")
                    daily_pct = row.get("daily_pct")
                    is_ceiling = bool(tavan_limit and son and abs(son - tavan_limit) <= PRICE_TOLERANCE)
                    is_floor = bool(taban_limit and son and abs(son - taban_limit) <= PRICE_TOLERANCE)

                    if is_ceiling:
                        title = f"🏆 Günsonu Kapanış: {ticker} Tavan Kapattı!"
                        body = f"{ticker} tavan fiyatından kapattı 🎯"
                        log(f"  {ticker}: TAVAN KAPATTI!")
                    elif is_floor:
                        title = f"📉 Günsonu Kapanış: {ticker} Taban Kapattı!"
                        body = f"{ticker} taban fiyatından kapattı ⚠️"
                        log(f"  {ticker}: TABAN KAPATTI!")
                    else:
                        pct_val = float(daily_pct) if daily_pct is not None else 0.0
                        # %0.00 nötr — bildirim gönderme
                        if abs(pct_val) < 0.005:
                            log(f"  {ticker}: NÖTR KAPANIŞ %0.00 — atlanıyor")
                            continue
                        fark_str = f"%+{abs(pct_val):.2f}" if pct_val >= 0 else f"%-{abs(pct_val):.2f}"
                        if pct_val >= 0:
                            title = f"🟢 Günsonu Kapanış: {ticker} Alıcılı Kapattı"
                            body = f"Günsonu Fark: {fark_str}"
                            log(f"  {ticker}: ALICILI KAPATTI {fark_str}")
                        else:
                            title = f"🔴 Günsonu Kapanış: {ticker} Satıcılı Kapattı"
                            body = f"Günsonu Fark: {fark_str}"
                            log(f"  {ticker}: SATICILI KAPATTI {fark_str}")

                    _send_realtime_notification(ticker, "gunluk_acilis_kapanis", title, body)
                    closing_count += 1
                    # Birden fazla hisse takip eden kullanıcıya spam olmaması için
                    # her hisse arasında 5 sn bekle
                    if closing_count < len(rows):
                        time.sleep(5)
                if closing_count > 0:
                    closing_notif_sent = True
                    _save_state(today_str, prev_hit_ceiling, prev_hit_floor,
                                pct_alerts_sent, opening_notif_sent, closing_notif_sent)
                    log(f"  Kapanış bildirimi: {closing_count} hisse için gönderildi (~{closing_count * 5}sn yayılarak)")
                log(f"  {'='*50}")

            time.sleep(interval)

    except KeyboardInterrupt:
        log("\n\nCanli sync durduruldu. (Ctrl+C)")
        log(f"Toplam {cycle} dongu calistirildi.")


def main():
    parser = argparse.ArgumentParser(description="BIST Finans — Matriks Excel → Render Sync")
    parser.add_argument("--file", "-f", default=DEFAULT_EXCEL_PATH, help="Excel dosya yolu")
    parser.add_argument("--dry-run", action="store_true", help="Gonderme, sadece goster")
    parser.add_argument("--force", action="store_true", help="Tarih kontrolunu atla")
    parser.add_argument("--live", action="store_true", help="Canli sync modu (15sn aralikla)")
    parser.add_argument("--interval", type=int, default=15, help="Canli sync guncelleme araligi (saniye)")
    args = parser.parse_args()

    # Canli sync modu
    if args.live:
        live_sync(args.file, args.interval)
        return

    today = date.today()
    log("=" * 55)
    log(f"BIST Finans — Matriks Excel Sync")
    log(f"Tarih: {today.strftime('%d.%m.%Y')} | API: {API_URL}")
    log("=" * 55)

    # 1. Excel'i oku
    if not os.path.exists(args.file):
        log(f"HATA: Excel dosyasi bulunamadi: {args.file}")
        log(f"  Varsayilan yol: {DEFAULT_EXCEL_PATH}")
        log(f"  --file parametresi ile belirt")
        sys.exit(1)

    excel_rows = read_matriks_excel(args.file)

    if not excel_rows:
        log("Excel'de veri bulunamadi!")
        sys.exit(1)

    # 2. Tarih kontrolu — sadece bugunun verisini al
    valid_rows = []
    for row in excel_rows:
        tarih = row.get("tarih")

        # Tarih kontrolu
        if tarih and tarih != today and not args.force:
            log(f"  {row['ticker']}: Tarih farkli ({tarih} != {today}) — ATLANDI (tatil/kapali)")
            continue

        # SON = 0 kontrolu
        son = row.get("son")
        if son is None or son == 0:
            log(f"  {row['ticker']}: SON=0 — borsa kapali, ATLANDI")
            continue

        valid_rows.append(row)

    if not valid_rows:
        log("\nBugun icin yuklenecek veri yok (borsa kapali veya tatil).")
        return

    log(f"\n{len(valid_rows)} hisse icin veri hazir:")

    # 3. API'den mevcut durumlari al
    log("\nAPI'den mevcut veriler aliniyor...")
    active_ipos = get_active_trading_ipos()

    # 4. Her hisse icin ceiling track verisi hazirla
    tracks_to_upload = []

    for row in valid_rows:
        ticker = row["ticker"]
        son = row["son"]
        tavan_limit = row.get("tavan_limit")
        taban_limit = row.get("taban_limit")

        # Tavan/taban kontrolu
        hit_ceiling = False
        hit_floor = False

        if tavan_limit and son and abs(son - tavan_limit) <= PRICE_TOLERANCE:
            hit_ceiling = True
        if taban_limit and son and abs(son - taban_limit) <= PRICE_TOLERANCE:
            hit_floor = True

        # Trading day hesapla
        ipo_info = active_ipos.get(ticker)
        if ipo_info:
            next_day = ipo_info["trading_day_count"] + 1
            # Guvenlik: trading_start'tan HER ZAMAN dogrula (stale count koruması)
            trading_start = ipo_info.get("trading_start")
            if trading_start:
                from_start = _count_business_days(trading_start, today)
                if from_start > 0 and from_start != next_day:
                    log(f"  DUZELTME: {ticker} — DB next_day={next_day}, trading_start'tan hesap={from_start} → {from_start} kullaniliyor")
                    next_day = from_start
        else:
            log(f"  UYARI: {ticker} API'de trading bolumunde bulunamadi — ATLANIYOR")
            continue

        status = "TAVAN" if hit_ceiling else ("TABAN" if hit_floor else "NORMAL")

        track = {
            "ticker": ticker,
            "trading_day": next_day,
            "trade_date": today.isoformat(),
            "close_price": str(son),
            "hit_ceiling": hit_ceiling,
            "hit_floor": hit_floor,
        }

        # Alis/Satis varsa open_price olarak ekle
        alis = row.get("alis_fiyat")
        if alis and alis > 0:
            track["open_price"] = str(alis)

        # G.EN YUKSEK varsa high_price olarak kullan, yoksa tavan limitini
        gun_en_yuksek = row.get("gun_en_yuksek")
        if gun_en_yuksek and gun_en_yuksek > 0:
            track["high_price"] = str(gun_en_yuksek)
        elif tavan_limit and tavan_limit > 0:
            track["high_price"] = str(tavan_limit)
        if taban_limit and taban_limit > 0:
            track["low_price"] = str(taban_limit)

        tracks_to_upload.append(track)
        log(f"  {ticker} Gun {next_day}: SON={son} {status}")

    # 5. Gonder
    if args.dry_run:
        log("\n[DRY RUN] Gonderilmedi. Veri:")
        safe_payload = {"admin_password": "***", "tracks": tracks_to_upload}
        print(json.dumps(safe_payload, indent=2, ensure_ascii=False, default=str))
        return

    log("")
    result = upload_tracks(tracks_to_upload)

    if result and result.get("status") == "ok":
        log(f"\n✓ Basarili! {result.get('loaded', 0)} kayit yuklendi.")
    else:
        log("\n✗ Yukleme basarisiz!")
        sys.exit(1)


if __name__ == "__main__":
    main()
