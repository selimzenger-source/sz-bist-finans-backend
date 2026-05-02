"""GCM Yatirim Yurt Ici Bilanco Takvimi Scraper.

Veri kaynagi:
  https://www.gcmyatirim.com.tr/modules/yurticibilanco            -> period listesi
  https://www.gcmyatirim.com.tr/modules/yurticibilanco/{period}   -> hisse listesi

Periyot formati: "2026_01" (2026 1. Ceyrek), "2025_04" (2025 4. Ceyrek)
DB period formati: "2026-Q1", "2025-Q4"

Ikinci scraper calisinda mevcut kayitlar UPSERT ile guncellenir.
"""

import asyncio
import logging
from datetime import date, datetime

import httpx
from sqlalchemy import select

from app.database import async_session
from app.models.earnings_calendar import EarningsCalendar

logger = logging.getLogger(__name__)

BASE_URL = "https://www.gcmyatirim.com.tr"
PERIOD_LIST_URL = f"{BASE_URL}/modules/yurticibilanco"
PERIOD_DATA_URL = f"{BASE_URL}/modules/yurticibilanco/{{period}}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.gcmyatirim.com.tr/arastirma-analiz/yurt-ici-bilanco-takvimi",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}


def _format_period(gcm_period: str) -> str:
    """'2026_01' -> '2026-Q1'"""
    try:
        year, quarter = gcm_period.split("_")
        q = int(quarter)
        return f"{year}-Q{q}"
    except Exception:
        return gcm_period


def _parse_date(s: str) -> date | None:
    """'20-04-2026' -> date(2026,4,20). '01-01-1970' (sentinel) -> None."""
    if not s or s == "01-01-1970":
        return None
    try:
        d, m, y = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


async def fetch_periods(client: httpx.AsyncClient) -> dict:
    """Mevcut donemleri cek. {'2026_01': '2026 1. Ceyrek', ...}"""
    resp = await client.get(PERIOD_LIST_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    return resp.json()


async def fetch_period_data(client: httpx.AsyncClient, period: str) -> list[dict]:
    """Belli bir donemin sirket listesini cek."""
    resp = await client.get(PERIOD_DATA_URL.format(period=period), headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("TR", [])


async def upsert_calendar_row(session, ticker: str, company_name: str, period: str, expected_dt: date | None, is_announced: bool):
    """Tek satir upsert."""
    existing = await session.execute(
        select(EarningsCalendar).where(
            EarningsCalendar.ticker == ticker,
            EarningsCalendar.period == period,
        )
    )
    row = existing.scalar_one_or_none()

    if row:
        if expected_dt and row.expected_date != expected_dt:
            row.expected_date = expected_dt
        if company_name and not row.company_name:
            row.company_name = company_name
        if is_announced and not row.is_announced:
            row.is_announced = True
            row.announced_date = expected_dt
    else:
        session.add(EarningsCalendar(
            ticker=ticker,
            company_name=company_name,
            period=period,
            expected_date=expected_dt,
            is_announced=is_announced,
            announced_date=expected_dt if is_announced else None,
            source="gcmyatirim",
        ))


async def scrape_earnings_calendar(periods_to_scrape: int = 3) -> dict:
    """Son N donem icin takvimi cek ve DB'ye yaz.

    Args:
        periods_to_scrape: Kac son donem cekilsin (varsayilan 3 — gelecek + son 2).

    Returns:
        {"periods": [...], "rows_total": int, "errors": int}
    """
    stats = {"periods": [], "rows_total": 0, "errors": 0}

    async with httpx.AsyncClient(headers=HEADERS) as client:
        try:
            periods = await fetch_periods(client)
        except Exception as e:
            logger.error("gcm period listesi alinamadi: %s", e)
            return {**stats, "error": str(e)[:200]}

        # En yeni N donemi al (string sort calisir cunku format "YYYY_NN")
        sorted_periods = sorted(periods.keys(), reverse=True)[:periods_to_scrape]

        for gcm_period in sorted_periods:
            db_period = _format_period(gcm_period)
            stats["periods"].append(db_period)

            try:
                items = await fetch_period_data(client, gcm_period)
            except Exception as e:
                logger.warning("gcm donem %s alinamadi: %s", gcm_period, e)
                stats["errors"] += 1
                continue

            async with async_session() as session:
                for item in items:
                    try:
                        ticker = (item.get("sembol") or "").upper().strip()
                        if not ticker:
                            continue
                        company = (item.get("post_title") or "").strip().rstrip("*").strip()
                        expected_dt = _parse_date(item.get("tarih") or "")
                        # Aciklanan kolonu doluysa "is_announced=TRUE"
                        aciklanan = item.get("aciklanan")
                        is_announced = bool(aciklanan and aciklanan.strip())

                        await upsert_calendar_row(session, ticker, company, db_period, expected_dt, is_announced)
                        stats["rows_total"] += 1
                    except Exception as e:
                        logger.debug("gcm row hatasi: %s", e)
                        stats["errors"] += 1
                await session.commit()

            await asyncio.sleep(0.5)

    logger.info("gcm earnings calendar tamamlandi: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(scrape_earnings_calendar())
