"""AI Piyasa Raporu Servisi — Sabah Acilis + Aksam Kapanis Raporu

Sabah 08:30 TR: Onceki gun verileri + bugunun beklentileri
Aksam 20:00 TR: Gunun kapanis verileri + degerlendirme
Her ikisi de X (Twitter) uzerinden gorsel + metin tweet olarak paylasilir.
"""

import json
import logging
import os
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────
# Sabitler
# ────────────────────────────────────────────

_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_AI_MODEL = "gpt-4.1"
_AI_TIMEOUT = 30

# Yahoo Finance ticker'lari
_MARKET_TICKERS = {
    "XU100": "XU100.IS",
    "SP500": "^GSPC",
    "NASDAQ": "^IXIC",
    "GOLD": "GC=F",
    "USDTRY": "USDTRY=X",
}

_YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
_YAHOO_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}

# Gorsel dosya yollari
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ACILIS_IMAGE = os.path.join(_BASE_DIR, "static", "img", "acilis_raporu_banner.png")
_KAPANIS_IMAGE = os.path.join(_BASE_DIR, "static", "img", "kapanis_raporu_banner.png")


# ────────────────────────────────────────────
# Piyasa Verisi Cekme (Yahoo Finance)
# ────────────────────────────────────────────

async def fetch_market_snapshot() -> dict:
    """Tum piyasa verilerini Yahoo Finance'den ceker.

    Returns:
        {
            "XU100": {"close": 9823.45, "prev_close": 9750.12, "change_pct": 0.75, "open": 9760.00},
            "SP500": {...},
            "NASDAQ": {...},
            "GOLD": {...},
            "USDTRY": {...},
            "fetched_at": "2026-02-24T17:00:00Z",
        }
    """
    result = {}
    async with httpx.AsyncClient(timeout=20.0, headers=_YAHOO_HEADERS, follow_redirects=True) as client:
        for name, ticker in _MARKET_TICKERS.items():
            try:
                resp = await client.get(
                    f"{_YAHOO_CHART_URL}/{ticker}",
                    params={"interval": "1d", "range": "5d"},
                )
                if resp.status_code != 200:
                    logger.warning("Yahoo %s: HTTP %d", name, resp.status_code)
                    result[name] = None
                    continue

                data = resp.json()
                chart_results = data.get("chart", {}).get("result", [])
                if not chart_results:
                    result[name] = None
                    continue

                meta = chart_results[0].get("meta", {})
                quote = chart_results[0].get("indicators", {}).get("quote", [{}])[0]
                timestamps = chart_results[0].get("timestamp", [])

                close_price = meta.get("regularMarketPrice", 0)
                prev_close = meta.get("chartPreviousClose", meta.get("previousClose", 0))

                # Son 2 gunun kapanis fiyatlarini al (daha guvenilir)
                closes = quote.get("close", [])
                opens = quote.get("open", [])

                if len(closes) >= 2 and closes[-1] is not None:
                    close_price = round(closes[-1], 2)
                    prev_close = round(closes[-2], 2) if closes[-2] else prev_close

                open_price = round(opens[-1], 2) if opens and opens[-1] else close_price

                change_pct = 0
                if prev_close and prev_close != 0:
                    change_pct = round((close_price - prev_close) / prev_close * 100, 2)

                result[name] = {
                    "close": close_price,
                    "prev_close": round(prev_close, 2) if prev_close else None,
                    "change_pct": change_pct,
                    "open": open_price,
                }
                logger.debug("Yahoo %s: close=%.2f, change=%.2f%%", name, close_price, change_pct)

            except Exception as e:
                logger.error("Yahoo %s hatasi: %s", name, e)
                result[name] = None

    result["fetched_at"] = datetime.now(timezone.utc).isoformat()
    return result


async def get_active_ipos_performance() -> list[dict]:
    """Islemde olan (trading, <25 gun) IPO'larin performans verisini getirir."""
    try:
        from app.database import async_session
        from app.models.ipo import IPO
        from sqlalchemy import select, and_

        async with async_session() as session:
            result = await session.execute(
                select(IPO).where(
                    and_(
                        IPO.status == "trading",
                        IPO.trading_day_count < 25,
                    )
                )
            )
            ipos = list(result.scalars().all())

            ipo_data = []
            for ipo in ipos:
                pct_from_ipo = None
                if ipo.ipo_price and ipo.first_day_close_price:
                    pct_from_ipo = round(
                        float((ipo.first_day_close_price - ipo.ipo_price) / ipo.ipo_price * 100), 1
                    )

                ipo_data.append({
                    "ticker": ipo.ticker,
                    "company": ipo.company_name,
                    "ipo_price": float(ipo.ipo_price) if ipo.ipo_price else None,
                    "trading_day_count": ipo.trading_day_count or 0,
                    "ceiling_broken": ipo.ceiling_broken,
                    "pct_from_ipo_price": pct_from_ipo,
                    "high_from_start": float(ipo.high_from_start) if ipo.high_from_start else None,
                })

            return ipo_data

    except Exception as e:
        logger.error("IPO performans verisi hatasi: %s", e)
        return []


# ────────────────────────────────────────────
# AI Rapor Uretme
# ────────────────────────────────────────────

_MORNING_SYSTEM_PROMPT = """Sen SZ Algo Trade'in piyasa analisti yapay zekasisin. Her sabah piyasa acilmadan once yatirimcilara ozet rapor yaziyorsun.

KURAL:
1. Turkce yaz, samimi ve profesyonel
2. Emoji kullan ama asiri degil (her bolumde 1-2)
3. Rakamlar ve yuzde degisimler net olsun
4. Halka arz performanslarini da dahil et (varsa)
5. Hic yatirim tavsiyesi VERME — "yatirim tavsiyesi degildir" notu ekle
6. Tweet formati: max 3800 karakter (gorsel ile 4000 limiti var)
7. Yapilandirilmis format kullan: basliklar ve maddeler ile
8. Rapor EN AZ 120 kelime olmali — detayli ve icerikli yaz, cok kisa tutma
9. Sonda mutlaka szalgo.net.tr linki ve hashtag'ler olmali

FORMAT:
📊 AÇILIŞ RAPORU — [tarih]

🇹🇷 BIST 100 (XU100)
[onceki kapanis, degisim, analiz]

🇺🇸 ABD Piyasalari
[S&P 500 ve Nasdaq verisi + analiz]

💰 Dolar & Altin
[USD/TRY ve altin verisi]

🏦 Halka Arz Takibi
[islemdeki IPO'lar, performans]

📌 Gunun Beklentileri
[kisa ozet, dikkat edilecekler]

⚠️ Yatirim tavsiyesi degildir.

📲 szalgo.net.tr

#BIST100 #xauusd #altin #DowJones [+ konuya gore 2-3 ek hashtag: #HalkaArz #SP500 #dolar #nasdaq #enflasyon gibi — tekrar etme, hep farkli sec]"""

_EVENING_SYSTEM_PROMPT = """Sen SZ Algo Trade'in piyasa analisti yapay zekasisin. Her aksam piyasa kapandiktan sonra gun sonu degerlendirme raporu yaziyorsun.

KURAL:
1. Turkce yaz, samimi ve profesyonel
2. Emoji kullan ama asiri degil (her bolumde 1-2)
3. Rakamlar ve yuzde degisimler net olsun
4. Halka arz performanslarini da dahil et (varsa)
5. Hic yatirim tavsiyesi VERME — "yatirim tavsiyesi degildir" notu ekle
6. Tweet formati: max 3800 karakter (gorsel ile 4000 limiti var)
7. Yapilandirilmis format kullan: basliklar ve maddeler ile

FORMAT:
📊 KAPANIŞ RAPORU — [tarih]

🇹🇷 BIST 100 (XU100)
[kapanis, degisim, hacim degerlendirme]

🇺🇸 ABD Piyasalari
[S&P 500 ve Nasdaq verileri + gunduz akisi]

💰 Dolar & Altin
[USD/TRY ve altin kapanis]

🏦 Halka Arz Takibi
[islemdeki IPO'lar, gunun performansi]

📌 Genel Degerlendirme
[gunun ozeti, onemli gelismeler]

⚠️ Yatirim tavsiyesi degildir.

📲 szalgo.net.tr

#BIST100 #xauusd #altin #DowJones [+ konuya gore 2-3 ek hashtag: #HalkaArz #SP500 #dolar #nasdaq #enflasyon gibi — tekrar etme, hep farkli sec]"""


def _format_market_context(market_data: dict, ipos: list[dict], report_type: str) -> str:
    """AI'a gonderilecek piyasa veri ozetini formatlar."""
    lines = [f"RAPOR TURU: {'Sabah Acilis' if report_type == 'morning' else 'Aksam Kapanis'}"]
    lines.append(f"TARIH: {date.today().isoformat()}")
    lines.append("")

    for name, label in [
        ("XU100", "BIST 100"),
        ("SP500", "S&P 500"),
        ("NASDAQ", "Nasdaq"),
        ("GOLD", "Altin (USD)"),
        ("USDTRY", "USD/TRY"),
    ]:
        data = market_data.get(name)
        if data:
            lines.append(
                f"{label}: Kapanis={data['close']}, "
                f"Onceki={data['prev_close']}, "
                f"Degisim=%{data['change_pct']}, "
                f"Acilis={data['open']}"
            )
        else:
            lines.append(f"{label}: Veri alinamadi")

    if ipos:
        lines.append("")
        lines.append("ISLEMDEKI HALKA ARZLAR:")
        for ipo in ipos:
            status = "Tavan bozulmadi" if not ipo["ceiling_broken"] else "Tavan bozuldu"
            lines.append(
                f"  {ipo['ticker']} ({ipo['company']}): "
                f"Halka arz fiyati={ipo['ipo_price']} TL, "
                f"{ipo['trading_day_count']}. islem gunu, "
                f"{status}"
            )
            if ipo.get("high_from_start"):
                lines.append(f"    Baslangictan en yuksek: {ipo['high_from_start']} TL")
    else:
        lines.append("")
        lines.append("ISLEMDEKI HALKA ARZLAR: Su an 25 gun altinda islem goren halka arz yok.")

    return "\n".join(lines)


async def _generate_report(market_data: dict, ipos: list[dict], report_type: str) -> str | None:
    """AI ile piyasa raporu uretir."""
    api_key = get_settings().ABACUS_API_KEY
    if not api_key:
        logger.error("Abacus API key yok — rapor uretilemedi")
        return None

    system_prompt = _MORNING_SYSTEM_PROMPT if report_type == "morning" else _EVENING_SYSTEM_PROMPT
    context = _format_market_context(market_data, ipos, report_type)

    user_message = f"Asagidaki piyasa verilerini kullanarak rapor yaz:\n\n{context}"

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _ABACUS_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _AI_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    "temperature": 0.5,
                    "max_tokens": 1000,
                },
            )

            if resp.status_code != 200:
                logger.error("AI rapor hatasi: HTTP %d — %s", resp.status_code, resp.text[:200])
                return None

            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

            if not content:
                logger.error("AI bos rapor dondu")
                return None

            # Tweet karakter limiti (gorsel ile 4000)
            if len(content) > 3900:
                content = content[:3897] + "..."

            logger.info("AI %s raporu uretildi: %d karakter", report_type, len(content))
            return content.strip()

    except Exception as e:
        logger.error("AI rapor uretme hatasi: %s", e)
        return None


# ────────────────────────────────────────────
# Tweet Gonderme
# ────────────────────────────────────────────

async def send_morning_report_tweet():
    """Sabah acilis raporu tweeti gonderir (08:30 TR)."""
    logger.info("Sabah acilis raporu hazirlaniyor...")

    market_data = await fetch_market_snapshot()
    ipos = await get_active_ipos_performance()

    report_text = await _generate_report(market_data, ipos, "morning")
    if not report_text:
        logger.error("Sabah raporu uretilemedi — tweet atilmadi")
        return

    # Tweet gonder (gorsel ile)
    from app.services.twitter_service import _safe_tweet_with_media
    success = _safe_tweet_with_media(
        report_text,
        _ACILIS_IMAGE,
        source="morning_market_report",
        force_send=True,  # Onay beklemeden direkt at
    )

    if success:
        logger.info("Sabah acilis raporu tweeti basarili!")
    else:
        logger.error("Sabah acilis raporu tweeti BASARISIZ")


async def send_evening_report_tweet():
    """Aksam kapanis raporu tweeti gonderir (20:00 TR)."""
    logger.info("Aksam kapanis raporu hazirlaniyor...")

    market_data = await fetch_market_snapshot()
    ipos = await get_active_ipos_performance()

    report_text = await _generate_report(market_data, ipos, "evening")
    if not report_text:
        logger.error("Aksam raporu uretilemedi — tweet atilmadi")
        return

    # Tweet gonder (gorsel ile)
    from app.services.twitter_service import _safe_tweet_with_media
    success = _safe_tweet_with_media(
        report_text,
        _KAPANIS_IMAGE,
        source="evening_market_report",
        force_send=True,  # Onay beklemeden direkt at
    )

    if success:
        logger.info("Aksam kapanis raporu tweeti basarili!")
    else:
        logger.error("Aksam kapanis raporu tweeti BASARISIZ")
