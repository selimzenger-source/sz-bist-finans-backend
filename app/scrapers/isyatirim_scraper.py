"""
IsYatirim Bilanco & Temettu Scraper — TAMAMEN DEVRE DISI
=========================================================

BIST veri lisansi gerekliligi nedeniyle IsYatirim undocumented JSON API
uzerinden bilanco, temettu, hisse listesi vb. cekimi KAPATILDI.

Bizim akis:
  - Bilanco verisi: KAP'tan dogrudan parse edilen `company_financials` DB
  - Temettu verisi: KAP'tan parse edilen `dividend_calendar` + `dividend_history`
  - BIST hisse listesi: app/data/ticker_names.json (statik)

Tum fonksiyonlar bos veri doner. Lisansli vendor entegre edilince geri acilir.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def fetch_bilanco(ticker: str, *args, **kwargs) -> list[dict]:
    """DEVRE DISI — BIST veri lisansi."""
    logger.debug("isyatirim.fetch_bilanco DEVRE DISI (%s)", ticker)
    return []


async def fetch_temettu_gecmisi(ticker: str, *args, **kwargs) -> list[dict]:
    """DEVRE DISI — BIST veri lisansi."""
    logger.debug("isyatirim.fetch_temettu_gecmisi DEVRE DISI (%s)", ticker)
    return []


async def fetch_bilanco_batch(*args, **kwargs) -> dict:
    """DEVRE DISI — BIST veri lisansi."""
    logger.info("isyatirim.fetch_bilanco_batch DEVRE DISI — atlandi")
    return {"total": 0, "ok": 0, "errors": 0, "disabled": True}


async def fetch_temettu_batch(*args, **kwargs) -> dict:
    """DEVRE DISI — BIST veri lisansi."""
    logger.info("isyatirim.fetch_temettu_batch DEVRE DISI — atlandi")
    return {"total": 0, "ok": 0, "errors": 0, "disabled": True}


async def on_bilanco_bildirimi(ticker: str, *args, **kwargs) -> dict | None:
    """DEVRE DISI — KAP bilanco bildirimi geldiginde bizim
    bilanco_kap_scraper devreye girer. IsYatirim'a gerek yok.
    """
    return None


async def fetch_all_bist_tickers() -> list[str]:
    """DEVRE DISI — app/data/ticker_names.json kullanilir."""
    try:
        import json, os
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base_dir, "data", "ticker_names.json")
        with open(path, "r", encoding="utf-8") as f:
            return list(json.load(f).keys())
    except Exception:
        return []
