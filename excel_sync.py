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
PRICE_TOLERANCE = Decimal("0.02")

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


def _count_business_days(start_date, end_date):
    """Iki tarih arasindaki is gunu sayisini hesapla (her ikisi dahil)."""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    count = 0
    current = start_date
    from datetime import timedelta as td
    while current <= end_date:
        if current.weekday() < 5:  # Pazartesi=0 ... Cuma=4
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


def _send_realtime_notification(ticker, notif_type, title, body, sub_event=None):
    """Render API'ye anlik bildirim gonder (tavan bozulma, taban acilma, yuzde dusus)."""
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
    ceiling_cooldown = {}  # {ticker: float(timestamp)} — son tavan bildirimi zamani (5dk cooldown)
    floor_cooldown = {}    # {ticker: float(timestamp)} — son taban bildirimi zamani (5dk cooldown)
    COOLDOWN_SECONDS = 300  # 5 dakika — tavan/taban bildirimleri arasi bekleme
    opening_notif_sent = False   # Gunluk acilis bildirimi gonderildi mi
    closing_notif_sent = False   # Gunluk kapanis bildirimi gonderildi mi
    cycle = 0
    last_session_date = None  # Seans gun degisiminde cache temizle + IPO yenile

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
        log("  ✅ Restart-safe: onceki bildirim state'i yuklendi — duplicate bildirim engellendi")
    else:
        log("  ℹ State dosyasi yok/baska gun — temiz basliyor")

    try:
        while True:
            cycle += 1
            now_dt = datetime.now()
            now = now_dt.strftime("%H:%M:%S")

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
                ceiling_cooldown.clear()
                floor_cooldown.clear()
                opening_notif_sent = False
                closing_notif_sent = False
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
                time.sleep(interval)
                continue

            if not rows:
                log(f"[{now}] Excel bos — {interval}s sonra tekrar...")
                time.sleep(interval)
                continue
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

                status = "TAVAN" if hit_ceiling else ("TABAN" if hit_floor else "NORMAL")
                changed_tracks.append(track)
                pct_str = f" %{float(daily_pct):+.1f}" if daily_pct else ""
                log(f"  {ticker}: {son} TL {status}{pct_str}")

                # ── Anlik bildirim tespiti (Tavan/Taban + 5dk Cooldown) ──

                was_ceiling = prev_hit_ceiling.get(ticker, False)
                was_floor = prev_hit_floor.get(ticker, False)
                pct_val = float(daily_pct) if daily_pct is not None else 0.0
                fark_str = f"%+{abs(pct_val):.1f}" if pct_val >= 0 else f"%-{abs(pct_val):.1f}"
                son_str = f"{float(son):.2f}" if son else ""

                # Pre-opening suppression: bildirim susturulduysa prev state'i guncelleme
                # Boylece acilis bildirimi sonrasi 10:00:01'de tavan/taban kitledi ayrica gider
                # (Farkli kullanicilar acilis paketi vs tavan/taban bildirimlerine abone olabilir)
                suppress_ceiling_update = False
                suppress_floor_update = False

                # 1a. Tavana Kitledi: önceki döngü tavanda değildi, şimdi tavanda
                if not was_ceiling and hit_ceiling:
                    if not opening_notif_sent and hour_min <= 1000:
                        log(f"  ⏳ TAVANA KİTLEDİ ama açılış bildirimi bekleniyor: {ticker} ({son_str} TL)")
                        suppress_ceiling_update = True
                    else:
                        last_t = ceiling_cooldown.get(ticker, 0)
                        if time.time() - last_t >= COOLDOWN_SECONDS:
                            log(f"  🔔 TAVANA KİTLEDİ: {ticker} ({son_str} TL)")
                            _send_realtime_notification(
                                ticker, "tavan_bozulma",
                                f"🔒 {ticker} Tavana Kitledi!",
                                f"Son: {son_str} TL | Fark: {fark_str}",
                            )
                            ceiling_cooldown[ticker] = time.time()
                        else:
                            remaining = COOLDOWN_SECONDS - (time.time() - last_t)
                            log(f"  ⏳ TAVANA KİTLEDİ ama cooldown: {ticker} ({remaining:.0f}sn kaldı)")

                # 1b. Tavan Çözüldü: önceki döngü tavandaydı, şimdi değil
                elif was_ceiling and not hit_ceiling:
                    if not opening_notif_sent and hour_min <= 1000:
                        log(f"  ⏳ TAVAN ÇÖZÜLDÜ ama açılış bildirimi bekleniyor: {ticker} ({son_str} TL)")
                        suppress_ceiling_update = True
                    else:
                        last_t = ceiling_cooldown.get(ticker, 0)
                        if time.time() - last_t >= COOLDOWN_SECONDS:
                            log(f"  🔔 TAVAN ÇÖZÜLDÜ: {ticker} ({son_str} TL)")
                            _send_realtime_notification(
                                ticker, "tavan_bozulma",
                                f"🔓 {ticker} Tavan Çözüldü!",
                                f"Son: {son_str} TL | Fark: {fark_str}",
                            )
                            ceiling_cooldown[ticker] = time.time()
                        else:
                            remaining = COOLDOWN_SECONDS - (time.time() - last_t)
                            log(f"  ⏳ TAVAN ÇÖZÜLDÜ ama cooldown: {ticker} ({remaining:.0f}sn kaldı)")

                # 2a. Tabana Kitledi: önceki döngü tabanda değildi, şimdi tabanda
                if not was_floor and hit_floor:
                    if not opening_notif_sent and hour_min <= 1000:
                        log(f"  ⏳ TABANA KİTLEDİ ama açılış bildirimi bekleniyor: {ticker} ({son_str} TL)")
                        suppress_floor_update = True
                    else:
                        last_t = floor_cooldown.get(ticker, 0)
                        if time.time() - last_t >= COOLDOWN_SECONDS:
                            log(f"  🔔 TABANA KİTLEDİ: {ticker} ({son_str} TL)")
                            _send_realtime_notification(
                                ticker, "taban_acilma",
                                f"🔒 {ticker} Tabana Kitledi!",
                                f"Son: {son_str} TL | Fark: {fark_str}",
                            )
                            floor_cooldown[ticker] = time.time()
                        else:
                            remaining = COOLDOWN_SECONDS - (time.time() - last_t)
                            log(f"  ⏳ TABANA KİTLEDİ ama cooldown: {ticker} ({remaining:.0f}sn kaldı)")

                # 2b. Taban Kalktı: önceki döngü tabandaydı, şimdi değil
                elif was_floor and not hit_floor:
                    if not opening_notif_sent and hour_min <= 1000:
                        log(f"  ⏳ TABAN KALKTI ama açılış bildirimi bekleniyor: {ticker} ({son_str} TL)")
                        suppress_floor_update = True
                    else:
                        last_t = floor_cooldown.get(ticker, 0)
                        if time.time() - last_t >= COOLDOWN_SECONDS:
                            log(f"  🔔 TABAN KALKTI: {ticker} ({son_str} TL)")
                            _send_realtime_notification(
                                ticker, "taban_acilma",
                                f"📈 {ticker} Taban Kalktı!",
                                f"Son: {son_str} TL | Fark: {fark_str}",
                            )
                            floor_cooldown[ticker] = time.time()
                        else:
                            remaining = COOLDOWN_SECONDS - (time.time() - last_t)
                            log(f"  ⏳ TABAN KALKTI ama cooldown: {ticker} ({remaining:.0f}sn kaldı)")

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

                # Durumlari guncelle — acilis oncesi susturulan bildirimler icin
                # state'i guncelleme ki 10:00:01'de tavan/taban kitledi ayrica gidebilsin
                if not suppress_ceiling_update:
                    prev_hit_ceiling[ticker] = hit_ceiling
                if not suppress_floor_update:
                    prev_hit_floor[ticker] = hit_floor

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
            else:
                # Her 4 donguede bir (1 dakika) sessiz log
                if cycle % 4 == 0:
                    log(f"[{now}] #{cycle} — Degisiklik yok")

            # ── Günlük açılış bildirimi (09:56) ──
            # Seans açılışında abonelere push bildirim gönder
            # Format: Tavan/Taban → direkt, Alıcılı/Satıcılı → Açılış Gap: +/-%X.XX
            # Not: %0.00 gap nötr — bildirim gönderilmez
            if not opening_notif_sent and 956 <= hour_min <= 1000 and prev_prices:
                log(f"  {'='*50}")
                log(f"  AÇILIŞ BİLDİRİMİ GÖNDERİLİYOR")
                log(f"  {'='*50}")

                # Tüm hisselerin açılış bilgisini topla
                opening_lines = []  # [(ticker, emoji, kısa_durum)]
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
                        opening_lines.append((ticker, "🔒", "Tavan"))
                        log(f"  {ticker}: TAVAN AÇTI!")
                    elif is_floor:
                        opening_lines.append((ticker, "🔒", "Taban"))
                        log(f"  {ticker}: TABAN AÇTI!")
                    else:
                        pct_val = float(daily_pct) if daily_pct is not None else 0.0
                        if abs(pct_val) < 0.005:
                            log(f"  {ticker}: NÖTR AÇILIŞ %0.00 — atlanıyor")
                            continue
                        gap_str = f"%{pct_val:+.1f}"
                        if pct_val >= 0:
                            opening_lines.append((ticker, "🟢", gap_str))
                            log(f"  {ticker}: ALICILI AÇTI {gap_str}")
                        else:
                            opening_lines.append((ticker, "🔴", gap_str))
                            log(f"  {ticker}: SATICILI AÇTI {gap_str}")

                if opening_lines:
                    # Özet body oluştur: "🔒SVGYO Tavan | 🔴AKHAN %-0.8 | ..."
                    summary_parts = [f"{em}{t} {s}" for t, em, s in opening_lines]
                    summary_body = " | ".join(summary_parts)
                    # Push bildirim karakter limiti (~240 char body) — uzunsa kırp
                    if len(summary_body) > 220:
                        summary_body = summary_body[:217] + "..."

                    summary_title = f"📊 Seans Açılış: {len(opening_lines)} Halka Arz Hissesi"

                    # Her hisse için ayrı bildirim yerine, her hisseye TEK özet bildirim gönder
                    # Böylece kullanıcı takip ettiği hisselerden birinin bildirimiyle özeti alır
                    sent_tickers = set()
                    for ticker, _, _ in opening_lines:
                        _send_realtime_notification(
                            ticker, "gunluk_acilis_kapanis",
                            summary_title, summary_body,
                        )
                        sent_tickers.add(ticker)
                        # Hisseler arası kısa bekleme (aynı kullanıcıya duplikat push engeli)
                        time.sleep(2)

                    opening_notif_sent = True
                    _save_state(today_str, prev_hit_ceiling, prev_hit_floor,
                                pct_alerts_sent, opening_notif_sent, closing_notif_sent)
                    log(f"  Açılış bildirimi: {len(opening_lines)} hisse özet olarak gönderildi")
                log(f"  {'='*50}")

            # ── Günlük kapanış bildirimi (18:08) ──
            # Seans kapanışında abonelere push bildirim gönder
            # Format: Tavan/Taban → direkt, Alıcılı/Satıcılı → Günsonu Fark: +/-%X.XX
            # Not: %0.00 fark nötr — bildirim gönderilmez
            if not closing_notif_sent and 1808 <= hour_min <= 1820 and prev_prices:
                log(f"  {'='*50}")
                log(f"  KAPANIŞ BİLDİRİMİ GÖNDERİLİYOR")
                log(f"  {'='*50}")

                # Tüm hisselerin kapanış bilgisini topla
                closing_lines = []
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
                        closing_lines.append((ticker, "🔒", "Tavan"))
                        log(f"  {ticker}: TAVAN KAPATTI!")
                    elif is_floor:
                        closing_lines.append((ticker, "🔒", "Taban"))
                        log(f"  {ticker}: TABAN KAPATTI!")
                    else:
                        pct_val = float(daily_pct) if daily_pct is not None else 0.0
                        if abs(pct_val) < 0.005:
                            log(f"  {ticker}: NÖTR KAPANIŞ %0.00 — atlanıyor")
                            continue
                        gap_str = f"%{pct_val:+.1f}"
                        if pct_val >= 0:
                            closing_lines.append((ticker, "🟢", gap_str))
                            log(f"  {ticker}: ALICILI KAPATTI {gap_str}")
                        else:
                            closing_lines.append((ticker, "🔴", gap_str))
                            log(f"  {ticker}: SATICILI KAPATTI {gap_str}")

                if closing_lines:
                    summary_parts = [f"{em}{t} {s}" for t, em, s in closing_lines]
                    summary_body = " | ".join(summary_parts)
                    if len(summary_body) > 220:
                        summary_body = summary_body[:217] + "..."

                    summary_title = f"📊 Günsonu Kapanış: {len(closing_lines)} Halka Arz Hissesi"

                    for ticker, _, _ in closing_lines:
                        _send_realtime_notification(
                            ticker, "gunluk_acilis_kapanis",
                            summary_title, summary_body,
                        )
                        time.sleep(2)

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
