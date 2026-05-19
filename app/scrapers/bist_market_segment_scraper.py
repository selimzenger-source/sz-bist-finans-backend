"""BIST Hisse Pazar Segmenti CSV Scraper.

Kaynak: https://borsaistanbul.com/datum/hisse_endeks_ds.csv
CSV format (utf-8-sig BOM'lu):
  - Satir 1 (header TR): BILESEN KODU;BULTEN_ADI;ENDEKS KODU;ENDEKS ADI;...
  - Satir 2 (header EN): CONSTITUENT CODE;CONSTITUENT NAME;INDEX CODE;...
  - Satir 3+: Data, semicolon ';' ile ayrili

Her hisse 1+ endeks uyesi. Her satir bir uyelik kaydi.
Ornek:
  AEFES.E;ANADOLU EFES;XU100;BIST 100;...
  AEFES.E;ANADOLU EFES;XYLDZ;BIST YILDIZ;...

Pazar belirleme (per ticker):
  XYLDZ uyesi -> 'yildiz_pazar'
  XBANA uyesi -> 'ana_pazar'
  hicbiri    -> 'diger' (alt pazar, yakin izleme vs.)

Ticker AEFES.E -> AEFES'e normalize edilir (.E suffix kaldirilir).
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

BIST_URL = "https://borsaistanbul.com/datum/hisse_endeks_ds.csv"

# Pazar endeks kodlari (BIST resmi)
_MARKET_INDEX_MAP = {
    "XYLDZ": "yildiz_pazar",
    "XBANA": "ana_pazar",
    # Diger ozel pazarlar (zaman icinde tespit edersek ekleriz)
}


def _normalize_ticker(raw: str) -> str:
    """AEFES.E -> AEFES (BIST .E suffix kaldirilir)."""
    t = (raw or "").strip().upper()
    # .E, .F, .Y gibi suffixleri kaldir
    if "." in t:
        t = t.split(".")[0]
    return t


async def fetch_bist_market_csv() -> dict[str, dict[str, Any]]:
    """BIST CSV'sini cek + her ticker icin uyeligi oldugu endeksleri grupla.

    Returns:
        {ticker: {company_name, market_segment, indexes_list}}
    """
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(BIST_URL)
            resp.raise_for_status()
            text = resp.content.decode("utf-8-sig", errors="replace")
    except Exception as e:
        logger.error("BIST market CSV fetch hatasi: %s", e)
        return {}

    lines = text.splitlines()
    if len(lines) < 3:
        logger.warning("BIST market CSV cok kisa (%d satir)", len(lines))
        return {}

    # Header'i atla — satir 0 (TR) ve satir 1 (EN) header
    # Data satir 2'den baslar. Kolon sirasi: ticker, name, index_code, index_tr_name, ...
    reader = csv.reader(io.StringIO("\n".join(lines[2:])), delimiter=";")
    grouped: dict[str, dict[str, Any]] = {}

    for row in reader:
        if not row or len(row) < 3:
            continue
        ticker = _normalize_ticker(row[0])
        if not ticker:
            continue
        company_name = (row[1] or "").strip()
        index_code = (row[2] or "").strip().upper()
        if not index_code:
            continue

        entry = grouped.setdefault(ticker, {
            "ticker": ticker,
            "company_name": company_name or None,
            "market_segment": "diger",
            "indexes": [],
        })
        if index_code not in entry["indexes"]:
            entry["indexes"].append(index_code)
        if company_name and not entry.get("company_name"):
            entry["company_name"] = company_name
        # Pazar belirleme — XYLDZ veya XBANA varsa pazar set edilir
        if index_code in _MARKET_INDEX_MAP:
            seg = _MARKET_INDEX_MAP[index_code]
            # XYLDZ > XBANA oncelikli olmaz, her ikisi ayri pazar — ilk gelen kalir
            if entry["market_segment"] == "diger":
                entry["market_segment"] = seg

    logger.info("BIST market CSV: %d unique ticker", len(grouped))
    return grouped


async def sync_bist_markets(db: AsyncSession) -> dict[str, int]:
    """BIST CSV'sini DB'ye senkronize et. (insert/update)."""
    from app.models.stock_market import StockMarket

    items_map = await fetch_bist_market_csv()
    if not items_map:
        return {"fetched": 0, "inserted": 0, "updated": 0}

    inserted = 0
    updated = 0
    existing_rows = (await db.execute(select(StockMarket))).scalars().all()
    by_ticker = {r.ticker: r for r in existing_rows}

    for tk, it in items_map.items():
        indexes_csv = ",".join(it["indexes"])[:500]
        existing = by_ticker.get(tk)
        if existing:
            changed = False
            if existing.market_segment != it["market_segment"]:
                existing.market_segment = it["market_segment"]
                changed = True
            if it.get("company_name") and existing.company_name != it["company_name"]:
                existing.company_name = it["company_name"]
                changed = True
            if existing.indexes != indexes_csv:
                existing.indexes = indexes_csv
                changed = True
            if changed:
                updated += 1
        else:
            row = StockMarket(
                ticker=tk,
                company_name=it.get("company_name"),
                market_segment=it["market_segment"],
                indexes=indexes_csv or None,
            )
            db.add(row)
            inserted += 1

    await db.commit()
    logger.info(
        "BIST market segment sync: %d fetched, %d inserted, %d updated",
        len(items_map), inserted, updated,
    )
    return {"fetched": len(items_map), "inserted": inserted, "updated": updated}
