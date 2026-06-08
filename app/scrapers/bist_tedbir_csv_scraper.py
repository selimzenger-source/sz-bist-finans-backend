"""Borsa Istanbul resmi tedbir CSV scraper.

Kaynak: https://www.borsaistanbul.com/erd/menkul_tedbir_listesi.csv

CSV format (UTF-8 BOM, ; separated):
  Satir 1: timestamp (orn: "14.05.2026 08:29:00;")
  Satir 2: header
  Satir 3+: data
  Pay Adi; Islem Kodu; Tedbir Kodu; Tedbir Adi; Ilk Tarih; Son Tarih

Bir ticker icin coklu tedbir satiri var (her tedbir ayri row).
Bu scraper:
  1. CSV indirir
  2. Ticker bazli aggregate eder (tum tedbir kodlari tek satira)
  3. cautious_stocks tablosuna upsert eder
  4. CSV'de olmayan kayitlari is_active=False isaretler

Scheduler: TR 09:40, 19:00, 00:00 (UTC 06:40, 16:00, 21:00).
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime, timezone
from typing import Optional

import httpx
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import async_session
from app.models.cautious_stock import CautiousStock

logger = logging.getLogger(__name__)

CSV_URL = "https://www.borsaistanbul.com/erd/menkul_tedbir_listesi.csv"

# BIST tedbir kodu -> kisa etiket (mevcut sistemle uyumlu)
TEDBIR_CODE_TO_TAG: dict[str, str] = {
    "PKISY":  "KRD",   # Kredili Islem Yasagi
    "PASKI":  "ACS",   # Aciga Satis Yasagi
    "PBRUT":  "BRT",   # Brut Takas
    "PEIAK":  "EMR",   # Emir Iptali / Miktar Azaltimi / Fiyati Kotulestirme
    "PPEGK":  "PEM",   # Piyasa Emri ile Piyasadan Limite Emir Girisi Kisitlamasi
    "PYAYN":  "VEY",   # Veri Yayininin Emir Toplama Seansinda Kisitlanmasi
    "PTEKF":  "TEK",   # Tek Fiyat
    "PSURP":  "SUP",   # Surekli Pazar Yasagi (varsa)
    "PVOLM":  "VOL",   # Volatilite bazli (varsa)
}


def _parse_tr_date(s: str) -> Optional[date]:
    """DD.MM.YYYY formatini parse et."""
    if not s or not s.strip():
        return None
    s = s.strip()
    try:
        d, m, y = s.split(".")
        return date(int(y), int(m), int(d))
    except (ValueError, AttributeError):
        return None


async def fetch_bist_tedbir_csv() -> list[dict]:
    """BIST CSV'sini cekip parse eder.

    Returns:
        Ticker bazli aggregate edilmis liste:
        [{"ticker": "PRZMA", "company_name": "PRIZMA PRESS MATBAACILIK",
          "tags": "KRD,PEM,EMR,BRT,VEY,ACS",
          "start_date": date(2026,3,25), "end_date": date(2026,5,26),
          "raw_codes": ["PKISY","PPEGK","PEIAK","PBRUT","PYAYN","PASKI"]}]
    """
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(CSV_URL)
            resp.raise_for_status()
            # BOM destegi icin utf-8-sig
            text = resp.content.decode("utf-8-sig", errors="replace")
    except Exception as e:
        logger.error("BIST tedbir CSV fetch hatasi: %s", e)
        return []

    lines = text.splitlines()
    if len(lines) < 3:
        logger.warning("BIST tedbir CSV cok kisa (%d satir)", len(lines))
        return []

    # Satir 1: timestamp, Satir 2: header. Data satir 3'ten baslar.
    reader = csv.reader(io.StringIO("\n".join(lines[2:])), delimiter=";")

    # Ticker bazli aggregate
    aggregated: dict[str, dict] = {}

    for row in reader:
        if not row or len(row) < 6:
            continue
        # Bos satir / footer atla
        if not row[0].strip() or not row[1].strip():
            continue

        company_name = row[0].strip()
        ticker = row[1].strip().upper()
        tedbir_code = row[2].strip().upper()
        tedbir_adi = row[3].strip()
        start_date = _parse_tr_date(row[4])
        end_date = _parse_tr_date(row[5])

        if not ticker:
            continue

        # ───── VARANT / WARRANT FILTRESI ─────
        # Varantlar hisse degil turev urun — tedbirli hisseler listesinde gosterilmez.
        # Tipik isaretler:
        #   - Ticker'in son karakteri rakam ve oncesinde V/W var (orn: AKBNK_V1, GARAN.W2)
        #   - Company name 'VARANT' iceriyor
        _name_upper = company_name.upper()
        if "VARANT" in _name_upper or "WARRANT" in _name_upper:
            continue
        # Ticker pattern: harfler + V/W + rakam (orn: AKBNV1, GARANV2)
        # BIST normal ticker max 5-6 karakter, varantlar genelde 7-8 karakter ve sonu V[0-9] veya W[0-9]
        if len(ticker) >= 6 and ticker[-2] in ("V", "W") and ticker[-1].isdigit():
            continue
        # Bazi varantlar nokta veya alt cizgi ile: 'AKBNK.V1', 'AKBNK_W2'
        if "." in ticker or "_" in ticker:
            _suffix = ticker.split(".")[-1].split("_")[-1]
            if _suffix and _suffix[0] in ("V", "W") and any(c.isdigit() for c in _suffix):
                continue

        tag = TEDBIR_CODE_TO_TAG.get(tedbir_code, tedbir_code[:3])  # Bilinmeyen kod ilk 3 harfi

        if ticker not in aggregated:
            aggregated[ticker] = {
                "ticker": ticker,
                "company_name": company_name,
                "tags_set": set(),
                "raw_codes": [],
                "tedbir_names": [],
                "start_date": start_date,
                "end_date": end_date,
            }

        item = aggregated[ticker]
        item["tags_set"].add(tag)
        item["raw_codes"].append(tedbir_code)
        item["tedbir_names"].append(tedbir_adi)
        # EN YENI start (BIST'in son tedbir karari) + EN GEC end (tum tedbirler
        # sona erene kadar). Kullanici en guncel tarihi gormeli — OZATD ornegi:
        # 30.04 ve 18.05 birden aktifse 18.05 gosterilir.
        if start_date and (not item["start_date"] or start_date > item["start_date"]):
            item["start_date"] = start_date
        if end_date and (not item["end_date"] or end_date > item["end_date"]):
            item["end_date"] = end_date

    # Final list: tags_set -> CSV string
    result = []
    for ticker, item in aggregated.items():
        result.append({
            "ticker": ticker,
            "company_name": item["company_name"],
            "tags": ",".join(sorted(item["tags_set"])),
            "raw_codes": item["raw_codes"],
            "start_date": item["start_date"],
            "end_date": item["end_date"],
        })

    logger.info("BIST tedbir CSV parse: %d ticker (toplam %d tedbir satiri)",
                len(result), sum(len(r["raw_codes"]) for r in result))
    return result


async def sync_bist_tedbir() -> dict:
    """CSV'den okuyup cautious_stocks tablosuna upsert eder.

    - Yeni ticker: insert
    - Mevcut ticker: update (tags, end_date, is_active=True)
    - CSV'de olmayan eski aktif ticker: is_active=False isaretlenir

    Returns: {"fetched": int, "inserted": int, "updated": int, "deactivated": int, "errors": int}
    """
    csv_data = await fetch_bist_tedbir_csv()
    if not csv_data:
        return {"fetched": 0, "inserted": 0, "updated": 0, "deactivated": 0, "errors": 0,
                "note": "CSV bos veya fetch basarisiz"}

    # CSV'deki aktif tedbirlerin (ticker,start,end) anahtar kümesi
    csv_keys = {(row["ticker"], row["start_date"], row["end_date"]) for row in csv_data}
    inserted = 0
    updated = 0
    errors = 0

    async with async_session() as db:
        # TÜM kayitlari cek (aktif + inaktif) — DUPLICATE ÖNLEME: aktif/inaktif fark
        # etmeden (ticker,start,end) ile eslestir. Eski kod sadece AKTIF + ticker
        # bazinda esliyordu; tedbir bitip tekrar gelince yeni satir aciyordu (dup).
        existing_q = await db.execute(select(CautiousStock))
        existing_rows = existing_q.scalars().all()
        existing_by_key = {
            (r.ticker, r.start_date, r.end_date): r for r in existing_rows
        }

        # Insert/update — anahtar: (ticker, start_date, end_date)
        for row in csv_data:
            ticker = row["ticker"]
            key = (ticker, row["start_date"], row["end_date"])
            try:
                rec = existing_by_key.get(key)
                if rec is not None:
                    rec.company_name = row["company_name"]
                    rec.tags = row["tags"]
                    rec.is_active = True
                    rec.source = "bist_csv"
                    updated += 1
                else:
                    new_rec = CautiousStock(
                        ticker=ticker,
                        company_name=row["company_name"],
                        tags=row["tags"],
                        start_date=row["start_date"],
                        end_date=row["end_date"],
                        is_active=True,
                        source="bist_csv",
                    )
                    db.add(new_rec)
                    existing_by_key[key] = new_rec  # ayni sync icinde tekrar gelirse dup yok
                    inserted += 1
            except Exception as e:
                errors += 1
                logger.warning("BIST tedbir upsert hatasi (%s): %s", ticker, e)

        # Deaktivasyon — LIFT MANTIĞI ile:
        #  - Engel KALKMIŞSA (now >= lift 10:00) → is_active=False (doğal bitiş)
        #  - CSV'den çıkmış AMA end_date'ten ÖNCE çıkmışsa → erken iptal → is_active=False
        #  - CSV'de yok ama henüz kalkmamış (Cuma bitti, Pzt açılış öncesi) → AKTİF KALSIN
        #    (kullanıcı "BU SEANS" görsün; lift geçince sonraki sync pasifler)
        from app.utils.bist_holidays import tedbir_lift_datetime, _now_tr
        now = _now_tr()
        deactivated = 0
        for key, rec in existing_by_key.items():
            if not rec.is_active:
                continue
            if rec.source not in ("bist_csv", "halkarz", "manual_import"):
                continue
            lift_dt = tedbir_lift_datetime(rec.end_date)
            lifted = lift_dt is not None and now >= lift_dt
            dropped = key not in csv_keys
            early_cancel = (
                dropped and rec.end_date is not None and now.date() < rec.end_date
            )
            if lifted or early_cancel:
                rec.is_active = False
                deactivated += 1

        try:
            await db.commit()
        except Exception as e:
            logger.error("BIST tedbir commit hatasi: %s", e)
            await db.rollback()
            errors += 1

    stats = {
        "fetched": len(csv_data),
        "inserted": inserted,
        "updated": updated,
        "deactivated": deactivated,
        "errors": errors,
    }
    logger.info("BIST tedbir CSV sync: %s", stats)
    return stats


async def deactivate_lifted_cautious() -> dict:
    """Engeli KALKMIŞ tedbirleri is_active=False yapar (Bitenler'e geçer).

    LIFT MANTIĞI: tedbir, end_date'ten sonraki ilk İŞLEM GÜNÜ seans açılışında
    (~10:00, tatil/hafta sonu atlanır) kalkar. now >= lift_datetime olanlar pasiflenir.
    Bu işi seans açılışından hemen sonra (TR 10:05) çalıştır → AYCES gibi hisseler
    10:00'da DB'de de 'Bitenler'e düşer.
    """
    from app.utils.bist_holidays import tedbir_lift_datetime, _now_tr
    now = _now_tr()
    changed = 0
    async with async_session() as db:
        rows = (await db.execute(
            select(CautiousStock).where(CautiousStock.is_active == True)
        )).scalars().all()
        for r in rows:
            lift_dt = tedbir_lift_datetime(r.end_date)
            if lift_dt is not None and now >= lift_dt:
                r.is_active = False
                changed += 1
        await db.commit()
    if changed:
        logger.info("Tedbir lift: %d hisse engeli kalkti -> Bitenler", changed)
    return {"deactivated": changed}


# Geriye uyumluluk: mevcut halkarz_tedbirli_scraper.sync_to_db ile ayni isim
async def sync_to_db() -> dict:
    return await sync_bist_tedbir()
