"""Resmi BIST hisse-endeks CSV'sinden ticker→sektör & endeks üyeliği çeker.

Kaynak: https://borsaistanbul.com/datum/hisse_endeks_ds.csv
Format: BILESEN KODU;BULTEN_ADI;ENDEKS KODU;ENDEKS ADI;...
        AEFES.E;ANADOLU EFES;XU100;BIST 100;...

Her hisse birden çok endekse üye (her satır bir üyelik). Bu scraper hisse
bazında gruplayıp en spesifik SEKTÖR endeksini (örn XTEKS=Tekstil) seçer ve
stock_sectors tablosuna upsert eder.
"""

import csv
import io
import logging
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

CSV_URL = "https://borsaistanbul.com/datum/hisse_endeks_ds.csv"

# Spesifik ALT-SEKTÖR endeksleri (öncelikli — en açıklayıcı sektör)
SUB_SECTOR_MAP: dict[str, str] = {
    "XBANK": "Banka",
    "XSGRT": "Sigorta",
    "XFINK": "Finansal Kiralama, Faktoring",
    "XAKUR": "Aracı Kurumlar",
    "XHOLD": "Holding ve Yatırım",
    "XGMYO": "Gayrimenkul Y.O.",
    "XGSYO": "Girişim Sermayesi Y.O.",
    "XGIDA": "Gıda, İçecek",
    "XTEKS": "Tekstil, Deri",
    "XKMYA": "Kimya, Petrol, Plastik",
    "XMADN": "Madencilik",
    "XMANA": "Metal Ana Sanayi",
    "XMESY": "Metal Eşya, Makine",
    "XTAST": "Taş, Toprak",
    "XKAGT": "Orman, Kağıt, Basım",
    "XELKT": "Elektrik",
    "XULAS": "Ulaştırma",
    "XILTM": "İletişim",
    "XBLSM": "Bilişim",
    "XINSA": "İnşaat",
    "XTCRT": "Ticaret",
    "XPTIC": "Perakende Ticaret",
    "XTTIC": "Toptan Ticaret",
    "XTRZM": "Turizm",
    "XKNKL": "Konaklama",
    "XYIHZ": "Yiyecek, İçecek Hizmetleri",
    "XSPOR": "Spor",
}

# Geniş ana sektörler (alt-sektör bulunamazsa fallback)
BROAD_SECTOR_MAP: dict[str, str] = {
    "XUSIN": "Sınai",
    "XUMAL": "Mali",
    "XUHIZ": "Hizmetler",
    "XUTEK": "Teknoloji",
}

# Endeks üyelik bayrakları için
IDX_BIST30 = "XU030"
IDX_BIST50 = "XU050"
IDX_BIST100 = "XU100"


async def fetch_sector_csv() -> str | None:
    """CSV'yi indir (retry'li). İçeriği string döndürür."""
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=40, follow_redirects=True) as client:
                resp = await client.get(CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning("BIST sektör CSV %d döndü (deneme %d)", resp.status_code, attempt + 1)
        except Exception as e:
            logger.warning("BIST sektör CSV indirme hatası (deneme %d): %s", attempt + 1, e)
    return None


def parse_sector_csv(content: str) -> dict[str, dict]:
    """CSV içeriğini ticker bazlı sözlüğe çevir.

    Returns: { "YUNSA": {"name":..., "sector_name":..., "sector_index":...,
                          "indices":[...], "in_bist30":bool, ...}, ... }
    """
    reader = csv.reader(io.StringIO(content), delimiter=";")
    rows = list(reader)
    # İlk 2 satır başlık (TR + EN). Veri 3. satırdan başlar.
    data_rows = rows[2:] if len(rows) > 2 else []

    by_ticker: dict[str, dict] = {}
    for r in data_rows:
        if len(r) < 4:
            continue
        raw_code = (r[0] or "").strip()
        name = (r[1] or "").strip()
        idx_code = (r[2] or "").strip().upper()
        if not raw_code or not idx_code:
            continue
        # "YUNSA.E" → "YUNSA"
        ticker = raw_code.split(".")[0].strip().upper()
        if not ticker:
            continue
        slot = by_ticker.setdefault(ticker, {
            "name": name, "indices": set(),
        })
        if name and not slot.get("name"):
            slot["name"] = name
        slot["indices"].add(idx_code)

    # Sektör ve bayrakları türet
    result: dict[str, dict] = {}
    for ticker, slot in by_ticker.items():
        codes = slot["indices"]
        sector_name = None
        sector_index = None
        # Önce spesifik alt-sektör
        for code in codes:
            if code in SUB_SECTOR_MAP:
                sector_name = SUB_SECTOR_MAP[code]
                sector_index = code
                break
        # Bulunamazsa geniş ana sektör
        if not sector_name:
            for code in codes:
                if code in BROAD_SECTOR_MAP:
                    sector_name = BROAD_SECTOR_MAP[code]
                    sector_index = code
                    break
        result[ticker] = {
            "company_name": slot["name"][:120] if slot["name"] else None,
            "sector_name": sector_name,
            "sector_index": sector_index,
            "indices": ",".join(sorted(codes))[:2000],
            "in_bist30": IDX_BIST30 in codes,
            "in_bist50": IDX_BIST50 in codes,
            "in_bist100": IDX_BIST100 in codes,
        }
    return result


async def update_stock_sectors() -> dict:
    """CSV indir → parse → stock_sectors tablosuna upsert. Cron + manuel için."""
    from app.database import async_session
    from sqlalchemy import text as sa_text

    content = await fetch_sector_csv()
    if not content:
        logger.error("BIST sektör CSV indirilemedi — güncelleme atlandı")
        return {"ok": False, "error": "csv_indirilemedi"}

    parsed = parse_sector_csv(content)
    if not parsed:
        logger.error("BIST sektör CSV parse boş döndü")
        return {"ok": False, "error": "parse_bos"}

    now = datetime.now(timezone.utc)
    upserted = 0
    async with async_session() as db:
        for ticker, info in parsed.items():
            await db.execute(sa_text("""
                INSERT INTO stock_sectors
                    (ticker, company_name, sector_name, sector_index, indices,
                     in_bist30, in_bist50, in_bist100, updated_at)
                VALUES
                    (:ticker, :company_name, :sector_name, :sector_index, :indices,
                     :in_bist30, :in_bist50, :in_bist100, :updated_at)
                ON CONFLICT (ticker) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector_name  = EXCLUDED.sector_name,
                    sector_index = EXCLUDED.sector_index,
                    indices      = EXCLUDED.indices,
                    in_bist30    = EXCLUDED.in_bist30,
                    in_bist50    = EXCLUDED.in_bist50,
                    in_bist100   = EXCLUDED.in_bist100,
                    updated_at   = EXCLUDED.updated_at
            """), {"ticker": ticker, **info, "updated_at": now})
            upserted += 1
        await db.commit()

    logger.info("BIST sektör güncellendi: %d hisse upsert edildi", upserted)
    return {"ok": True, "count": upserted}
