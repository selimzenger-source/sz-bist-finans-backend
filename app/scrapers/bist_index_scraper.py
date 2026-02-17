"""BIST 50 endeks bilesen scraper.

Kaynak: https://infoyatirim.com/canli-borsa/xu050-bist-50-hisseleri
HTML tablosundaki data-symbol attribute'larindan hisse kodlarini ceker.

Her ayin 1'inde scheduler tarafindan calistirilir.
"""

import logging

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BIST50_URL = "https://infoyatirim.com/canli-borsa/xu050-bist-50-hisseleri"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


async def fetch_bist50_tickers() -> set[str]:
    """infoyatirim.com'dan guncel BIST 50 hisselerini cek.

    Returns:
        50 elemanli set[str] — hisse kodlari (ornek: {"AKBNK", "THYAO", ...})

    Raises:
        ValueError: Sayfa parse edilemezse veya 50 hisse bulunamazsa
    """
    async with httpx.AsyncClient(timeout=30.0, headers=HEADERS, follow_redirects=True) as client:
        resp = await client.get(BIST50_URL)
        resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")

    # Yontem 1: data-symbol attribute'lari
    tickers: set[str] = set()
    rows = soup.select("tr[data-symbol]")
    for row in rows:
        symbol = row.get("data-symbol", "").strip().upper()
        if symbol:
            tickers.add(symbol)

    if not tickers:
        # Yontem 2: Tablo icerisinden ilk sutun (hisse kodu)
        tbody = soup.find("tbody", id="tableBody")
        if tbody:
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if tds:
                    code = tds[0].get_text(strip=True).upper()
                    if code and code.isalpha() and len(code) <= 6:
                        tickers.add(code)

    if len(tickers) < 40:
        raise ValueError(
            f"BIST 50 scrape basarisiz: sadece {len(tickers)} hisse bulundu "
            f"(minimum 40 bekleniyor). URL: {BIST50_URL}"
        )

    logger.info("BIST 50 scrape basarili: %d hisse", len(tickers))
    return tickers
