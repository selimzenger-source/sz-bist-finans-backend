"""
BIST Finans — Matriks Excel Sync Script
=========================================
Matriks Excel'den tavan takip verilerini okuyup Render API'ye gonder.

Excel Formati (Matriks):
  A: ILK ISLEM  (22.Oca.26 gibi)
  B: HISSE      (AKHAN, NETCD, UCAYM)
  C: TAVAN      (Tavan limit fiyati)
  D: TABAN      (Taban limit fiyati)
  E: ALIS       (Alis Kademe fiyati, 0=islem yok)
  F: SATIS      (Satis Kademe fiyati, 0=islem yok)
  G: SON        (Son fiyat / Kapanis, 0=borsa kapali)
  H: %G FARK    (Gunluk % degisim)
  I: TARIH      (Verinin tarihi — 09/02/2026 00:00:00.00000)
  J: G.EN YUKSEK (Gun ici en yuksek fiyat)

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
import argparse
import requests
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from pathlib import Path

# ============================================
# Ayarlar
# ============================================

API_URL = os.getenv("BIST_API_URL", "https://sz-bist-finans-api.onrender.com")
ADMIN_PASSWORD = os.getenv("BIST_ADMIN_PW", "SzBist2026Admin!")

# Varsayilan Excel dosya yolu — masaustunde
DEFAULT_EXCEL_PATH = str(Path.home() / "Desktop" / "tavan_takip.xlsx")

# Tavan/taban eslesme toleransi (kurus)
PRICE_TOLERANCE = Decimal("0.02")


def log(msg):
    """Zaman damgali log."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


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


def read_matriks_excel(filepath):
    """
    Matriks Excel formatini oku.

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
    wb = openpyxl.load_workbook(filepath, data_only=True)
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
            "alis": parse_price(row[4]),
            "satis": parse_price(row[5]),
            "son": son_price,
            "daily_pct": daily_pct,
            "tarih": tarih,
            "gun_en_yuksek": gun_en_yuksek,
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


def main():
    parser = argparse.ArgumentParser(description="BIST Finans — Matriks Excel → Render Sync")
    parser.add_argument("--file", "-f", default=DEFAULT_EXCEL_PATH, help="Excel dosya yolu")
    parser.add_argument("--dry-run", action="store_true", help="Gonderme, sadece goster")
    parser.add_argument("--force", action="store_true", help="Tarih kontrolunu atla")
    args = parser.parse_args()

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
            # Guvenlik: trading_start'tan hesaplanan is gunu ile karsilastir
            trading_start = ipo_info.get("trading_start")
            if trading_start and next_day == 1:
                # ceiling_tracks ve trading_day_count ikisi de 0 → trading_start'tan hesapla
                from_start = _count_business_days(trading_start, today)
                if from_start > 1:
                    log(f"  UYARI: {ticker} — DB'de track/count yok ama trading_start'tan {from_start} is gunu gecmis → gun {from_start} kullaniliyor")
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
        alis = row.get("alis")
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
