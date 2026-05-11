"""Mynet Finans Hisse Oranlari Scraper.

Kaynak: https://finans.mynet.com/borsa/hisseler/{slug}/
Cikti: F/K, PD/DD, FD/FAVOK, Piyasa Degeri, Fiyat → financial_ratios tablosu

Slug pattern: '{ticker-lowercase}-{company-slug}'
Master liste: https://finans.mynet.com/borsa/hisseler/ — tum BIST tickerlari icin link var.

Akis:
1. Master liste cek → ticker→slug map olustur (cache 1 gun)
2. Her ticker icin sayfayi cek
3. Descriptive text'ten regex ile metrikleri parse et
4. financial_ratios tablosuna upsert (UTC tarih ile)
"""

from __future__ import annotations
import asyncio
import logging
import re
from datetime import date, datetime, timezone
from decimal import Decimal

import httpx
from sqlalchemy import select

from app.database import async_session
from app.models.company_financial import FinancialRatio

logger = logging.getLogger(__name__)

BASE_URL = "https://finans.mynet.com"
LIST_URL = f"{BASE_URL}/borsa/hisseler/"
HISSE_URL = f"{BASE_URL}/borsa/hisseler/{{slug}}/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

_TIMEOUT = 25
_RATE_LIMIT = 0.4  # saniye, hisseler arasi


# ─── slug map cache ────────────────────────────────────────────────────────
_slug_cache: dict[str, str] = {}
_slug_cache_at: datetime | None = None
_SLUG_CACHE_TTL = 24 * 3600


async def fetch_slug_map(client: httpx.AsyncClient) -> dict[str, str]:
    """Tum BIST tickerlari icin ticker→slug haritasi cek (master liste)."""
    global _slug_cache, _slug_cache_at
    if _slug_cache_at and (datetime.now(timezone.utc) - _slug_cache_at).total_seconds() < _SLUG_CACHE_TTL:
        return _slug_cache

    try:
        resp = await client.get(LIST_URL, headers=HEADERS, timeout=_TIMEOUT)
        resp.raise_for_status()
        html = resp.text
        # href="https://finans.mynet.com/borsa/hisseler/aagyo-agaoglu-gmyo/"
        pattern = re.compile(r'/borsa/hisseler/([a-z0-9]+)-([a-z0-9-]+)/"')
        slug_map: dict[str, str] = {}
        for m in pattern.finditer(html):
            ticker = m.group(1).upper()
            slug = f"{m.group(1)}-{m.group(2)}"
            slug_map[ticker] = slug
        _slug_cache = slug_map
        _slug_cache_at = datetime.now(timezone.utc)
        logger.info("Mynet slug haritasi: %d ticker", len(slug_map))
        return slug_map
    except Exception as e:
        logger.error("Mynet slug haritasi hatasi: %s", e)
        return {}


# ─── ratio parsing ────────────────────────────────────────────────────────
_TR_NUM = r"(\d+(?:\.\d{3})*(?:,\d+)?)"  # 1.234.567,89 veya 0,55 veya 12

def _to_float(s: str) -> float | None:
    """'1.234,56' → 1234.56"""
    if not s:
        return None
    try:
        return float(s.replace(".", "").replace(",", "."))
    except ValueError:
        return None


def parse_ratios(html: str) -> dict[str, float | None]:
    """Mynet HTML'den oranlari cikar."""
    out = {"fk": None, "pddd": None, "fd_favok": None, "piyasa_degeri": None, "price": None}

    # PD/DD: "PD/DD) oranı ise 0,55 olup"
    m = re.search(r"PD/DD\)\s*oran\w*\s*ise\s*" + _TR_NUM, html)
    if m:
        v = _to_float(m.group(1))
        if v is not None and v > 0:
            out["pddd"] = v

    # F/K: "F/K) oranı 0,00 seviyesindedir" → 0 ise null
    m = re.search(r"F/K\)\s*oran\w*\s*" + _TR_NUM + r"\s*seviye", html)
    if m:
        v = _to_float(m.group(1))
        if v is not None and v > 0:
            out["fk"] = v

    # FD/FAVOK: "FD/FAVÖK) oranı ise X olup"
    m = re.search(r"FD/FAVÖK\)\s*oran\w*\s*ise\s*" + _TR_NUM, html)
    if m:
        v = _to_float(m.group(1))
        if v is not None and v > 0:
            out["fd_favok"] = v

    # Piyasa Degeri: "piyasa değeri 2,7 milyar TL" or "164.4 milyar"
    m = re.search(
        r"piyasa\s*de[ğg]eri\s*" + _TR_NUM + r"\s*(milyar|milyon|trilyon|bin)\s*TL",
        html, re.IGNORECASE,
    )
    if m:
        v = _to_float(m.group(1))
        unit = m.group(2).lower()
        if v is not None:
            mult = {"trilyon": 1e12, "milyar": 1e9, "milyon": 1e6, "bin": 1e3}[unit]
            out["piyasa_degeri"] = v * mult

    # Fiyat: "son fiyatı 32,72 TL"
    m = re.search(r"(?:son|g[üu]ncel)\s*fiyat\w*\s*" + _TR_NUM + r"\s*TL", html, re.IGNORECASE)
    if m:
        v = _to_float(m.group(1))
        if v is not None and v > 0:
            out["price"] = v

    return out


# ─── DB upsert ────────────────────────────────────────────────────────────
async def upsert_ratios(session, ticker: str, ratios: dict, today: date):
    """financial_ratios tablosuna gunluk kayit upsert."""
    stmt = select(FinancialRatio).where(
        FinancialRatio.ticker == ticker,
        FinancialRatio.date == today,
    )
    existing = (await session.execute(stmt)).scalar_one_or_none()

    def _dec(v):
        return Decimal(str(round(v, 4))) if v is not None else None

    if existing:
        if ratios.get("fk") is not None:
            existing.fk = _dec(ratios["fk"])
        if ratios.get("pddd") is not None:
            existing.pddd = _dec(ratios["pddd"])
        if ratios.get("fd_favok") is not None:
            existing.fd_favok = _dec(ratios["fd_favok"])
        if ratios.get("piyasa_degeri") is not None:
            existing.piyasa_degeri = Decimal(str(round(ratios["piyasa_degeri"], 2)))
        existing.source = "mynet"
    else:
        new = FinancialRatio(
            ticker=ticker,
            date=datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc),
            fk=_dec(ratios.get("fk")),
            pddd=_dec(ratios.get("pddd")),
            fd_favok=_dec(ratios.get("fd_favok")),
            piyasa_degeri=Decimal(str(round(ratios["piyasa_degeri"], 2))) if ratios.get("piyasa_degeri") else None,
            source="mynet",
        )
        session.add(new)


# ─── Tek hisse ─────────────────────────────────────────────────────────────
async def fetch_ticker_ratios(client: httpx.AsyncClient, ticker: str, slug: str) -> dict | None:
    """Tek hisse icin oranlari cek + parse + return."""
    url = HISSE_URL.format(slug=slug)
    try:
        resp = await client.get(url, headers=HEADERS, timeout=_TIMEOUT)
        if resp.status_code != 200:
            return None
        html = resp.text
        return parse_ratios(html)
    except Exception as e:
        logger.debug("mynet %s hatasi: %s", ticker, e)
        return None


# ─── Batch ────────────────────────────────────────────────────────────────
async def scrape_all_ratios(limit: int | None = None) -> dict:
    """DEVRE DISI — Borsa Istanbul veri lisansi gerekliligi nedeniyle
    Mynet'ten F/K, PD/DD, FD/FAVOK, Piyasa Degeri, Fiyat cekimi kapatildi.

    Lisansli vendor entegre edilince geri acilacak.
    """
    logger.info("mynet ratios DEVRE DISI (BIST lisansi sureci) — atlandi")
    return {"total": 0, "ok": 0, "no_data": 0, "errors": 0, "disabled": True}


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    print(asyncio.run(scrape_all_ratios(limit=n)))
