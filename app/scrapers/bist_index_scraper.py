"""BIST 30/50/100 endeks bilesen scraper.

Kaynak: https://infoyatirim.com/canli-borsa/
Her ayin 1'inde scheduler tarafindan calistirilir.
"""

import logging

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

INDEX_URLS = {
    "BIST 30": "https://infoyatirim.com/canli-borsa/xu030-bist-30-hisseleri",
    "BIST 50": "https://infoyatirim.com/canli-borsa/xu050-bist-50-hisseleri",
    "BIST 100": "https://infoyatirim.com/canli-borsa/xu100-bist-100-hisseleri",
}

INDEX_MIN_COUNTS = {
    "BIST 30": 25,
    "BIST 50": 40,
    "BIST 100": 80,
}


async def _fetch_index_tickers(url: str, index_name: str, min_count: int) -> set[str]:
    """infoyatirim.com'dan endeks hisselerini cek."""
    async with httpx.AsyncClient(timeout=30.0, headers=HEADERS, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    tickers: set[str] = set()
    rows = soup.select("tr[data-symbol]")
    for row in rows:
        symbol = row.get("data-symbol", "").strip().upper()
        if symbol:
            tickers.add(symbol)

    if not tickers:
        tbody = soup.find("tbody", id="tableBody")
        if tbody:
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if tds:
                    code = tds[0].get_text(strip=True).upper()
                    if code and code.isalpha() and len(code) <= 6:
                        tickers.add(code)

    if len(tickers) < min_count:
        raise ValueError(
            f"{index_name} scrape basarisiz: sadece {len(tickers)} hisse bulundu "
            f"(minimum {min_count} bekleniyor). URL: {url}"
        )

    logger.info("%s scrape basarili: %d hisse", index_name, len(tickers))
    return tickers


async def fetch_bist30_tickers() -> set[str]:
    return await _fetch_index_tickers(INDEX_URLS["BIST 30"], "BIST 30", INDEX_MIN_COUNTS["BIST 30"])


async def fetch_bist50_tickers() -> set[str]:
    return await _fetch_index_tickers(INDEX_URLS["BIST 50"], "BIST 50", INDEX_MIN_COUNTS["BIST 50"])


async def fetch_bist100_tickers() -> set[str]:
    return await _fetch_index_tickers(INDEX_URLS["BIST 100"], "BIST 100", INDEX_MIN_COUNTS["BIST 100"])


async def fetch_all_indices() -> dict[str, set[str]]:
    """Tum endeksleri tek seferde cek."""
    results = {}
    for name, url in INDEX_URLS.items():
        try:
            results[name] = await _fetch_index_tickers(url, name, INDEX_MIN_COUNTS[name])
        except Exception as e:
            logger.error("%s scrape hatasi: %s", name, e)
    return results
