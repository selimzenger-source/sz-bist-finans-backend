"""BIST Hisse Pazar Segmenti CSV Scraper.

Kaynak: https://borsaistanbul.com/datum/hisse_endeks_ds.csv
CSV format (utf-8-sig BOM'lu):
  - Satir 1: Header (Borsa İstanbul A.Ş ... timestamp)
  - Satir 2: Kolon basliklari (HISSE KODU; SIRKET ADI; ESHAM KISITLI ESHAM ENDEKSLERI;
             PAZAR/PIYASA; ...)
  - Satir 3+: Data, semicolon ';' ile ayrili

PAZAR/PIYASA degerleri:
  - Yildiz Pazar
  - Ana Pazar
  - Alt Pazar
  - Yakin Izleme Pazari
  - GIP Aday Pazari
  - Yapilandirilmis Urunler ve Fon Pazari
  - Pre-Market Trading Platformu (PMTP)
  - vb.

Mapping → stock_markets.market_segment:
  'Yildiz' → 'yildiz_pazar'
  'Ana'    → 'ana_pazar'
  'Alt'    → 'alt_pazar'
  'Yakin'  → 'yakin_izleme'
  diger    → 'diger'
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)

BIST_URL = "https://borsaistanbul.com/datum/hisse_endeks_ds.csv"


def _normalize_segment(raw: str) -> str:
    if not raw:
        return "diger"
    s = lower_tr(raw)
    if "yildiz" in s:
        return "yildiz_pazar"
    if "ana" in s and "pazar" in s:
        return "ana_pazar"
    if "alt" in s and "pazar" in s:
        return "alt_pazar"
    if "yakin" in s and "izleme" in s:
        return "yakin_izleme"
    if "kollektif" in s or "yatirim urun" in s or "gip" in s or "fon pazar" in s:
        return "kollektif_yat"
    return "diger"


async def fetch_bist_market_csv() -> list[dict[str, Any]]:
    """BIST CSV'sini cek ve parse et."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(BIST_URL)
            resp.raise_for_status()
            text = resp.content.decode("utf-8-sig", errors="replace")
    except Exception as e:
        logger.error("BIST market CSV fetch hatasi: %s", e)
        return []

    lines = text.splitlines()
    if len(lines) < 3:
        logger.warning("BIST market CSV cok kisa (%d satir)", len(lines))
        return []

    # Header analizi — kolon konumlarini bul
    header_line = lines[1] if len(lines) >= 2 else ""
    header_cols = [c.strip() for c in header_line.split(";")]
    idx = {name: i for i, name in enumerate(header_cols)}

    # Kolon isimleri tam Turkce — esleyelim
    def _find_col(*candidates: str) -> int | None:
        for cand in candidates:
            cl = lower_tr(cand)
            for i, h in enumerate(header_cols):
                if cl in lower_tr(h):
                    return i
        return None

    col_ticker = _find_col("hisse kodu", "kod")
    col_name = _find_col("sirket adi", "sirket")
    col_market = _find_col("pazar", "piyasa")
    col_index = _find_col("endeks")

    if col_ticker is None or col_market is None:
        logger.warning(
            "BIST market CSV kolon bulunamadi (header: %s)",
            header_cols[:5],
        )
        return []

    items: list[dict[str, Any]] = []
    reader = csv.reader(io.StringIO("\n".join(lines[2:])), delimiter=";")
    for row in reader:
        if not row or len(row) <= col_market:
            continue
        ticker = (row[col_ticker] or "").strip().upper()
        if not ticker:
            continue
        name = (row[col_name] or "").strip() if col_name is not None else None
        market_raw = (row[col_market] or "").strip()
        index_raw = (row[col_index] or "").strip() if col_index is not None else None
        items.append({
            "ticker": ticker,
            "company_name": name,
            "market_segment": _normalize_segment(market_raw),
            "market_raw": market_raw,
            "indexes": index_raw,
        })
    return items


async def sync_bist_markets(db: AsyncSession) -> dict[str, int]:
    """BIST CSV'sini DB'ye senkronize et. (insert/update)."""
    from app.models.stock_market import StockMarket

    items = await fetch_bist_market_csv()
    if not items:
        return {"fetched": 0, "inserted": 0, "updated": 0}

    inserted = 0
    updated = 0
    # Tek seferde tum tickerlari cek
    existing_q = select(StockMarket)
    existing_rows = (await db.execute(existing_q)).scalars().all()
    by_ticker = {r.ticker: r for r in existing_rows}

    for it in items:
        tk = it["ticker"]
        existing = by_ticker.get(tk)
        if existing:
            changed = False
            if existing.market_segment != it["market_segment"]:
                existing.market_segment = it["market_segment"]
                changed = True
            if it.get("company_name") and existing.company_name != it["company_name"]:
                existing.company_name = it["company_name"]
                changed = True
            if it.get("indexes") and existing.indexes != it["indexes"]:
                existing.indexes = it["indexes"][:500]
                changed = True
            if changed:
                updated += 1
        else:
            row = StockMarket(
                ticker=tk,
                company_name=it.get("company_name"),
                market_segment=it["market_segment"],
                indexes=(it.get("indexes") or "")[:500] or None,
            )
            db.add(row)
            inserted += 1

    await db.commit()
    logger.info(
        "BIST market segment sync: %d fetched, %d inserted, %d updated",
        len(items), inserted, updated,
    )
    return {"fetched": len(items), "inserted": inserted, "updated": updated}
