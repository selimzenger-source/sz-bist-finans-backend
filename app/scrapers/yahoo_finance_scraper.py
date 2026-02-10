"""Yahoo Finance OHLC veri scraper — BIST halka arz hisseleri icin.

Kaynak: Yahoo Finance Chart API
URL:    https://query1.finance.yahoo.com/v8/finance/chart/{TICKER}.IS

BIST hisseleri icin ticker sonuna .IS eklenir (Istanbul Stock Exchange).
1 aylik gunluk OHLC verisi cekilir ve ipo_ceiling_tracks tablosuna yazilir.
"""

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}


class YahooFinanceScraper:
    """Yahoo Finance gunluk OHLC veri scraper."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def fetch_daily_ohlc(
        self,
        ticker: str,
        range_str: str = "3mo",
    ) -> list[dict]:
        """Bir hissenin gunluk OHLC verisini cekilir.

        Args:
            ticker: BIST hisse kodu (ornegin "UCAYM")
            range_str: Veri araligi ("1mo", "3mo", "6mo", "1y")

        Returns:
            Tarih sirasina gore OHLC listesi:
            [{
                "date": date(2026, 1, 22),
                "open": Decimal("19.80"),
                "high": Decimal("19.80"),
                "low": Decimal("19.80"),
                "close": Decimal("19.80"),
                "volume": 112554,
            }, ...]
        """
        yahoo_ticker = f"{ticker}.IS"
        url = f"{YAHOO_CHART_URL}/{yahoo_ticker}"

        try:
            resp = await self.client.get(url, params={
                "interval": "1d",
                "range": range_str,
            })

            if resp.status_code != 200:
                logger.warning(
                    "Yahoo Finance yaniti: %d — %s", resp.status_code, yahoo_ticker
                )
                return []

            data = resp.json()
            chart = data.get("chart", {})
            results = chart.get("result", [])

            if not results:
                logger.warning("Yahoo Finance bos sonuc: %s", yahoo_ticker)
                return []

            result = results[0]
            timestamps = result.get("timestamp", [])
            indicators = result.get("indicators", {})
            quote = indicators.get("quote", [{}])[0]

            opens = quote.get("open", [])
            highs = quote.get("high", [])
            lows = quote.get("low", [])
            closes = quote.get("close", [])
            volumes = quote.get("volume", [])

            daily_data = []
            for i, ts in enumerate(timestamps):
                if ts is None:
                    continue

                # Unix timestamp → date
                trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()

                o = opens[i] if i < len(opens) and opens[i] is not None else None
                h = highs[i] if i < len(highs) and highs[i] is not None else None
                l = lows[i] if i < len(lows) and lows[i] is not None else None
                c = closes[i] if i < len(closes) and closes[i] is not None else None
                v = volumes[i] if i < len(volumes) and volumes[i] is not None else 0

                if c is None:
                    continue  # Kapanisi olmayan gun atla

                daily_data.append({
                    "date": trade_date,
                    "open": Decimal(str(round(o, 2))) if o else None,
                    "high": Decimal(str(round(h, 2))) if h else None,
                    "low": Decimal(str(round(l, 2))) if l else None,
                    "close": Decimal(str(round(c, 2))),
                    "volume": v,
                })

            # Tarihe gore sirala
            daily_data.sort(key=lambda x: x["date"])

            logger.info(
                "Yahoo Finance: %s — %d gunluk veri cekildi",
                yahoo_ticker, len(daily_data),
            )
            return daily_data

        except Exception as e:
            logger.error("Yahoo Finance hatasi (%s): %s", yahoo_ticker, e)
            return []

    async def fetch_ohlc_since_trading_start(
        self,
        ticker: str,
        trading_start: date,
        max_days: int = 25,
    ) -> list[dict]:
        """Trading start tarihinden itibaren ilk N is gununun OHLC verisini getirir.

        Args:
            ticker: BIST hisse kodu
            trading_start: Borsada islem gormeye basladigi tarih
            max_days: Maksimum gun sayisi (default 25)

        Returns:
            [{
                "trading_day": 1,
                "date": date(2026, 1, 22),
                "open": Decimal("19.80"),
                "close": Decimal("19.80"),
                "high": Decimal("19.80"),
                "low": Decimal("19.80"),
                "volume": 112554,
            }, ...]
        """
        # Yeterli veri icin 3 aylik cek
        all_data = await self.fetch_daily_ohlc(ticker, range_str="6mo")

        if not all_data:
            return []

        # trading_start tarihinden itibaren filtrele
        filtered = [
            d for d in all_data
            if d["date"] >= trading_start
        ]

        # Ilk max_days gunu al ve trading_day numarasi ver
        result = []
        for i, day_data in enumerate(filtered[:max_days]):
            result.append({
                "trading_day": i + 1,
                **day_data,
            })

        logger.info(
            "%s: trading_start=%s, %d gun veri",
            ticker, trading_start, len(result),
        )
        return result


def detect_ceiling_floor(
    close_price: Decimal,
    prev_close: Optional[Decimal],
    high_price: Optional[Decimal] = None,
    low_price: Optional[Decimal] = None,
) -> dict:
    """Tavan/taban tespiti yapar.

    BIST kurallari:
    - Ilk 5 gun: +/- %10 fiyat limiti (halka arz sonrasi)
    - Normal gunler: +/- %10

    Basit tespit: Kapanisin onceki gune gore degisim orani
    - >= %9.5 → tavan (kucuk tolerans)
    - <= -%9.5 → taban

    Returns:
        {"hit_ceiling": bool, "hit_floor": bool, "pct_change": Decimal, "durum": str}
    """
    if prev_close is None or prev_close == 0:
        return {
            "hit_ceiling": True,  # Ilk gun genellikle tavan
            "hit_floor": False,
            "pct_change": Decimal("0"),
            "durum": "tavan",
        }

    pct_change = ((close_price - prev_close) / prev_close * 100).quantize(Decimal("0.01"))

    # Tavan tespiti: kapanış >= önceki gün * 1.095 (toleransli)
    ceiling_threshold = Decimal("9.5")
    floor_threshold = Decimal("-9.5")

    hit_ceiling = pct_change >= ceiling_threshold
    hit_floor = pct_change <= floor_threshold

    # Durum belirleme
    if hit_ceiling:
        durum = "tavan"
    elif hit_floor:
        durum = "taban"
    elif pct_change > Decimal("0"):
        durum = "alici_kapatti"
    elif pct_change < Decimal("0"):
        durum = "satici_kapatti"
    else:
        durum = "not_kapatti"

    return {
        "hit_ceiling": hit_ceiling,
        "hit_floor": hit_floor,
        "pct_change": pct_change,
        "durum": durum,
    }
