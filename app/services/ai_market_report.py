"""AI Piyasa Raporu Servisi — Sabah Acilis + Aksam Kapanis Raporu

Sabah 08:15 TR: Onceki gun verileri + bugunun beklentileri
Aksam 20:45 TR: Gunun kapanis verileri + degerlendirme
Her ikisi de X (Twitter) uzerinden gorsel + metin tweet olarak paylasilir.

v7: Etkileşim odakli guncelleme: Guclu HOOK + SORU satiri kurali (her iki prompt)
    + Aksam raporu cache → sabah beklenti baglantisi (last_reports.json)
    + Sabah: 11 kaynak (ana 7 + KAP>=8 + Tavily + aksam cache)
v6: Sabah raporu guclendirildi: KAP yuksek etki (>=8) + Tavily web arastirmasi (2 sorgu)
    + Agresif hashtag stratejisi (min 8-15 hashtag, IPO+KAP+genel finansal)
v5: BigPara RSS eklendi + hashtag kurali guncellendi (dogal, seyrek kullanim)
v4: Ekonomik takvim (doviz.com) + yaklasan IPO etkinlikleri + resmi kaynak referanslari
    + Gercek haber kaynaklari (DB + dis kaynak RSS) + ceiling_tracks + halusinasyon korumasi
"""

import json
import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

from app.config import get_settings

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────
# Rapor Cache — aksam raporu → sabah raporu baglantisi
# ────────────────────────────────────────────
_REPORT_CACHE_FILE = Path(__file__).parent.parent / "static" / "last_reports.json"


def save_report_to_cache(report_type: str, text: str) -> None:
    """Son raporu JSON cache'e kaydet (aksam → sabah beklenti akisi icin)."""
    try:
        cache: dict = {}
        if _REPORT_CACHE_FILE.exists():
            try:
                cache = json.loads(_REPORT_CACHE_FILE.read_text(encoding="utf-8"))
            except Exception:
                cache = {}
        cache[report_type] = {
            "text": text,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        _REPORT_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _REPORT_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Rapor cache'e kaydedildi: %s (%d karakter)", report_type, len(text))
    except Exception as e:
        logger.warning("Rapor cache kayit hatasi: %s", e)


def get_last_report_from_cache(report_type: str) -> dict | None:
    """Son raporu JSON cache'ten oku (sabah raporu icin aksam cache'ini okur)."""
    try:
        if not _REPORT_CACHE_FILE.exists():
            return None
        cache = json.loads(_REPORT_CACHE_FILE.read_text(encoding="utf-8"))
        entry = cache.get(report_type)
        if not entry:
            return None
        # 30 saatten eskiyse kullanma (stale)
        saved_at = datetime.fromisoformat(entry["saved_at"])
        age_hours = (datetime.now(timezone.utc) - saved_at).total_seconds() / 3600
        if age_hours > 30:
            logger.info("Rapor cache bayat (%.1f saat) — kullanilmadi", age_hours)
            return None
        return entry
    except Exception as e:
        logger.warning("Rapor cache okuma hatasi: %s", e)
        return None

# ────────────────────────────────────────────
# Sabitler
# ────────────────────────────────────────────

_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_AI_MODEL = "gpt-5.2"
_AI_TIMEOUT = 60  # Daha fazla veri isliyor, daha uzun sure

# Gemini 2.5 Pro — birincil (OpenAI uyumlu endpoint)
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-pro"

# Anthropic Claude Sonnet 4 — 3. yedek (direkt API)
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-sonnet-4-20250514"

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

# RSS haber kaynaklari (onem sirasina gore)
_RSS_SOURCES = [
    {
        "name": "Bloomberg HT",
        "url": "https://www.bloomberght.com/rss",
        "max_items": 8,
    },
    {
        "name": "Dunya Gazetesi",
        "url": "https://www.dunya.com/rss",
        "max_items": 5,
    },
    {
        "name": "BigPara",
        "url": "https://bigpara.hurriyet.com.tr/rss/",
        "max_items": 6,
    },
]

# Gorsel dosya yollari
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ACILIS_IMAGE = os.path.join(_BASE_DIR, "static", "img", "acilis_analizi_banner.png")
_KAPANIS_IMAGE = os.path.join(_BASE_DIR, "static", "img", "kapanis_raporu_banner.png")

# Turkce gun adlari
_TR_DAY_NAMES = {
    0: "Pazartesi", 1: "Sali", 2: "Carsamba",
    3: "Persembe", 4: "Cuma", 5: "Cumartesi", 6: "Pazar",
}

# Turkce durum aciklamalari
_DURUM_MAP = {
    "tavan": "Tavan (gun boyu tavanda kaldi)",
    "alici_kapatti": "Alici kapatti (pozitif kapanis)",
    "not_kapatti": "Notr kapanis (yatay)",
    "satici_kapatti": "Satici kapatti (negatif kapanis)",
    "taban": "Taban (gun boyu tabanda kaldi)",
}


# ────────────────────────────────────────────
# 1) Piyasa Verisi Cekme (Yahoo Finance)
# ────────────────────────────────────────────

async def fetch_market_snapshot() -> dict:
    """Tum piyasa verilerini Yahoo Finance'den ceker."""
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

                close_price = meta.get("regularMarketPrice", 0)
                prev_close = meta.get("chartPreviousClose", meta.get("previousClose", 0))

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


# ────────────────────────────────────────────
# 2) Halka Arz Verisi (DB — ceiling_tracks ile)
# ────────────────────────────────────────────

async def get_active_ipos_performance() -> list[dict]:
    """Islemde olan (trading, <25 gun) IPO'larin performans + ceiling_tracks verisini getirir."""
    try:
        from app.database import async_session
        from app.models.ipo import IPO, IPOCeilingTrack
        from sqlalchemy import select, and_
        from sqlalchemy.orm import selectinload

        async with async_session() as session:
            result = await session.execute(
                select(IPO)
                .options(selectinload(IPO.ceiling_tracks))
                .where(
                    and_(
                        IPO.status == "trading",
                        IPO.trading_day_count < 25,
                    )
                )
            )
            ipos = list(result.scalars().all())

            ipo_data = []
            for ipo in ipos:
                # Son fiyat
                last_close = None
                sorted_tracks = []
                if ipo.ceiling_tracks:
                    sorted_tracks = sorted(ipo.ceiling_tracks, key=lambda t: t.trading_day)
                    last_track = sorted_tracks[-1]
                    last_close = float(last_track.close_price) if last_track.close_price else None

                # Halka arz fiyatindan degisim
                pct_from_ipo = None
                if ipo.ipo_price and last_close:
                    pct_from_ipo = round(
                        (last_close - float(ipo.ipo_price)) / float(ipo.ipo_price) * 100, 1
                    )
                elif ipo.ipo_price and ipo.first_day_close_price:
                    pct_from_ipo = round(
                        float((ipo.first_day_close_price - ipo.ipo_price) / ipo.ipo_price * 100), 1
                    )

                # Tavan serisi hesapla
                tavan_seri = 0
                for t in sorted_tracks:
                    if t.durum == "tavan" or t.hit_ceiling:
                        tavan_seri += 1
                    else:
                        break

                # Gunluk ceiling_tracks detayi
                daily_tracks = []
                for t in sorted_tracks:
                    durum_label = _DURUM_MAP.get(t.durum, t.durum)
                    daily_tracks.append({
                        "gun": t.trading_day,
                        "tarih": t.trade_date.isoformat() if t.trade_date else None,
                        "kapanis": float(t.close_price) if t.close_price else None,
                        "durum": t.durum,
                        "durum_aciklama": durum_label,
                        "tavan_yapti_mi": t.hit_ceiling,
                        "taban_yapti_mi": t.hit_floor,
                        "pct_degisim": float(t.pct_change) if t.pct_change else None,
                    })

                ipo_data.append({
                    "ticker": ipo.ticker,
                    "company": ipo.company_name,
                    "ipo_price": float(ipo.ipo_price) if ipo.ipo_price else None,
                    "trading_day_count": ipo.trading_day_count or 0,
                    "ceiling_broken": ipo.ceiling_broken,
                    "ceiling_broken_at": ipo.ceiling_broken_at.isoformat() if ipo.ceiling_broken_at else None,
                    "pct_from_ipo_price": pct_from_ipo,
                    "last_close_price": last_close,
                    "high_from_start": float(ipo.high_from_start) if ipo.high_from_start else None,
                    "tavan_seri_gun": tavan_seri,
                    "daily_tracks": daily_tracks,
                })

            return ipo_data

    except Exception as e:
        logger.error("IPO performans verisi hatasi: %s", e)
        return []


# ────────────────────────────────────────────
# 3) DB Haber Kaynaklari (KAP + Telegram AI)
# ────────────────────────────────────────────

async def get_recent_kap_news(hours: int = 24, limit: int = 10) -> list[dict]:
    """Son 24 saatteki onemli KAP haberlerini getirir."""
    try:
        from app.database import async_session
        from app.models.news import KapNews
        from sqlalchemy import select, and_, desc

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        async with async_session() as session:
            result = await session.execute(
                select(KapNews)
                .where(KapNews.created_at >= cutoff)
                .order_by(desc(KapNews.created_at))
                .limit(limit)
            )
            news = list(result.scalars().all())

            return [
                {
                    "ticker": n.ticker,
                    "title": n.news_title or "(baslik yok)",
                    "detail": (n.news_detail or "")[:200],  # max 200 karakter
                    "sentiment": n.sentiment,
                    "type": n.news_type,
                    "published": n.published_at.isoformat() if n.published_at else None,
                }
                for n in news
            ]
    except Exception as e:
        logger.error("KAP haber cekme hatasi: %s", e)
        return []


async def get_high_impact_kap_disclosures(min_score: float = 8.0, hours: int = 20, limit: int = 12) -> list[dict]:
    """Son 20 saatteki yüksek etkili KAP bildirimlerini getirir (ai_impact_score >= 8).

    Sabah raporu için kullanılır — önemli şirket haberlerini öne çıkarır.
    Akşam raporu / tavan-taban analizinde KULLANILMAZ.
    """
    try:
        from app.database import async_session
        from app.models.kap_all_disclosure import KapAllDisclosure
        from sqlalchemy import select, and_, desc

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        async with async_session() as session:
            result = await session.execute(
                select(KapAllDisclosure)
                .where(
                    and_(
                        KapAllDisclosure.created_at >= cutoff,
                        KapAllDisclosure.ai_impact_score >= min_score,
                    )
                )
                .order_by(desc(KapAllDisclosure.ai_impact_score), desc(KapAllDisclosure.published_at))
                .limit(limit)
            )
            disclosures = list(result.scalars().all())

            return [
                {
                    "ticker": d.company_code,
                    "title": d.title,
                    "summary": (d.ai_summary or "")[:250],
                    "score": d.ai_impact_score,
                    "sentiment": d.ai_sentiment,
                    "category": d.category or "",
                    "published": d.published_at.isoformat() if d.published_at else None,
                }
                for d in disclosures
            ]
    except Exception as e:
        logger.error("KAP yüksek etki bildirim çekme hatası: %s", e)
        return []


async def fetch_tavily_morning_news() -> list[dict]:
    """Tavily ile sabah raporu icin web arastirmasi yapar.

    2 sorgu paralel: genel BIST piyasa haberleri + ekonomi/doviz.
    Sadece sabah raporu (send_morning_report_tweet) tarafindan kullanilir.
    Aksam raporu / tavan-taban analizinde KULLANILMAZ.
    """
    try:
        _settings = get_settings()
        tavily_key = getattr(_settings, "TAVILY_API_KEY", None) or ""
        if not tavily_key:
            logger.warning("Tavily API key yok — sabah web arastirmasi atlanıyor")
            return []

        import asyncio

        _TAVILY_URL = "https://api.tavily.com/search"
        queries = [
            "Borsa İstanbul BIST piyasa açılış haber analiz bugün",
            "Türk ekonomisi kur faiz merkez bankası son gelişmeler",
        ]

        async def _tavily_query(q: str) -> list[dict]:
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.post(
                        _TAVILY_URL,
                        json={
                            "api_key": tavily_key,
                            "query": q,
                            "search_depth": "basic",
                            "max_results": 5,
                            "include_answer": False,
                        },
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        results = []
                        for r in data.get("results", []):
                            results.append({
                                "title": r.get("title", ""),
                                "content": (r.get("content", "") or "")[:250],
                                "url": r.get("url", ""),
                            })
                        return results
                    else:
                        logger.warning("Tavily sorgu hatasi HTTP %d: %s", resp.status_code, q[:50])
                        return []
            except Exception as ex:
                logger.warning("Tavily sorgu istisna: %s — %s", ex, q[:50])
                return []

        results_list = await asyncio.gather(*[_tavily_query(q) for q in queries])
        all_results = []
        for r in results_list:
            all_results.extend(r)

        logger.info("Tavily sabah arastirması: %d sonuc bulundu", len(all_results))
        return all_results[:10]  # max 10 sonuc

    except Exception as e:
        logger.error("Tavily sabah arastirma genel hata: %s", e)
        return []


async def get_recent_telegram_news(hours: int = 24, limit: int = 10) -> list[dict]:
    """Son 24 saatteki yuksek puanli Telegram AI haberlerini getirir."""
    try:
        from app.database import async_session
        from app.models.telegram_news import TelegramNews
        from sqlalchemy import select, and_, desc

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        async with async_session() as session:
            result = await session.execute(
                select(TelegramNews)
                .where(
                    and_(
                        TelegramNews.created_at >= cutoff,
                        TelegramNews.ai_score.isnot(None),
                    )
                )
                .order_by(desc(TelegramNews.ai_score))
                .limit(limit)
            )
            news = list(result.scalars().all())

            return [
                {
                    "ticker": n.ticker,
                    "title": n.parsed_title or "(baslik yok)",
                    "summary": (n.ai_summary or "")[:200],
                    "sentiment": n.sentiment,
                    "ai_score": n.ai_score,
                    "type": n.message_type,
                    "date": n.message_date.isoformat() if n.message_date else None,
                }
                for n in news
            ]
    except Exception as e:
        logger.error("Telegram haber cekme hatasi: %s", e)
        return []


# ────────────────────────────────────────────
# 4) Dis Kaynak Haberler (RSS — Bloomberg HT, Dunya)
# ────────────────────────────────────────────

async def fetch_rss_headlines() -> list[dict]:
    """Bloomberg HT, Dunya Gazetesi RSS'lerinden son haberleri ceker."""
    all_headlines = []

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for source in _RSS_SOURCES:
            try:
                resp = await client.get(
                    source["url"],
                    headers={"User-Agent": _YAHOO_HEADERS["User-Agent"]},
                )
                if resp.status_code != 200:
                    logger.warning("RSS %s: HTTP %d", source["name"], resp.status_code)
                    continue

                # XML parse
                root = ET.fromstring(resp.text)

                # RSS 2.0 format: /rss/channel/item
                items = root.findall(".//item")
                if not items:
                    # Atom format fallback
                    ns = {"atom": "http://www.w3.org/2005/Atom"}
                    items = root.findall(".//atom:entry", ns)

                count = 0
                for item in items:
                    if count >= source["max_items"]:
                        break

                    # RSS 2.0
                    title = item.findtext("title", "")
                    pub_date = item.findtext("pubDate", "")
                    description = item.findtext("description", "")

                    # Atom fallback
                    if not title:
                        title = item.findtext("{http://www.w3.org/2005/Atom}title", "")
                    if not pub_date:
                        pub_date = item.findtext("{http://www.w3.org/2005/Atom}updated", "")

                    if not title:
                        continue

                    # Finans/ekonomi/borsa ile ilgili mi kontrol et
                    title_lower = title.lower()
                    keywords = [
                        "bist", "borsa", "endeks", "hisse", "dolar", "euro",
                        "altin", "faiz", "enflasyon", "merkez bankasi", "tcmb",
                        "ekonomi", "piyasa", "kur", "petrol", "hazine",
                        "ihracat", "ithalat", "buyume", "gsyih", "isletme",
                        "halka arz", "spk", "kap", "sermaye", "yatirim",
                        "s&p", "nasdaq", "dow", "fed", "wall street",
                        "kripto", "bitcoin", "tahvil", "bono", "opec",
                    ]

                    is_relevant = any(kw in title_lower for kw in keywords)
                    if not is_relevant:
                        # Genel ekonomi haberleri de aliyoruz
                        description_lower = (description or "").lower()[:300]
                        is_relevant = any(kw in description_lower for kw in keywords)

                    if is_relevant:
                        # HTML tag temizligi
                        clean_title = title.strip()
                        if "<" in clean_title:
                            from bs4 import BeautifulSoup
                            clean_title = BeautifulSoup(clean_title, "html.parser").get_text()

                        all_headlines.append({
                            "source": source["name"],
                            "title": clean_title[:200],
                            "date": pub_date[:25] if pub_date else None,
                        })
                        count += 1

                logger.info("RSS %s: %d haber alindi", source["name"], count)

            except ET.ParseError as e:
                logger.warning("RSS %s XML parse hatasi: %s", source["name"], e)
            except Exception as e:
                logger.error("RSS %s hatasi: %s", source["name"], e)

    return all_headlines


# ────────────────────────────────────────────
# 5) Ekonomik Takvim (doviz.com)
# ────────────────────────────────────────────

async def fetch_economic_calendar() -> dict:
    """doviz.com'dan bugun ve yarinin ekonomik etkinliklerini ceker.

    Returns:
        {
            "today": [{"time": "10:00", "event": "TUIK Enflasyon", "importance": "Yuksek", ...}],
            "tomorrow": [...],
        }
    """
    result = {"today": [], "tomorrow": []}

    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            resp = await client.post(
                "https://www.doviz.com/calendar/getCalendarEvents",
                data={"country": ""},  # Tum ulkeler — TR + global
                headers={
                    "User-Agent": _YAHOO_HEADERS["User-Agent"],
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": "https://www.doviz.com/ekonomik-takvim",
                },
            )

            if resp.status_code != 200:
                logger.warning("doviz.com takvim: HTTP %d", resp.status_code)
                return result

            data = resp.json()
            calendar_html = data.get("calendarHTML", "")
            if not calendar_html:
                logger.warning("doviz.com takvim: bos HTML")
                return result

            soup = BeautifulSoup(calendar_html, "lxml")

            # calendar-content-0 = Bugun, calendar-content-1 = Yarin
            for section_id, key in [("calendar-content-0", "today"), ("calendar-content-1", "tomorrow")]:
                section = soup.find(id=section_id)
                if not section:
                    continue

                # Tarih basligi
                date_header = section.find(class_="text-bold")
                date_str = date_header.text.strip() if date_header else ""

                rows = section.select("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 7:
                        continue

                    time_str = cells[0].text.strip()
                    country = cells[1].text.strip()
                    importance_span = cells[2].find("span", class_="importance")
                    importance = importance_span.get("title", "") if importance_span else ""
                    event_name = cells[3].text.strip()
                    actual = cells[4].text.strip()
                    forecast = cells[5].text.strip()
                    previous = cells[6].text.strip()

                    if not event_name:
                        continue

                    # Sadece onemli olanlari al (Yuksek ve Orta)
                    # Ama TR etkinliklerini her zaman al
                    is_turkey = "türkiye" in country.lower() or "turkey" in country.lower()
                    is_important = importance in ("Yüksek", "Orta", "High", "Medium")

                    if is_turkey or is_important:
                        result[key].append({
                            "time": time_str,
                            "country": country,
                            "event": event_name,
                            "importance": importance,
                            "actual": actual if actual and actual != "-" else None,
                            "forecast": forecast if forecast and forecast != "-" else None,
                            "previous": previous if previous and previous != "-" else None,
                        })

                logger.info("doviz.com %s: %d etkinlik", key, len(result[key]))

    except Exception as e:
        logger.error("Ekonomik takvim hatasi: %s", e)

    return result


# ────────────────────────────────────────────
# 6) Yaklasan IPO Etkinlikleri (DB)
# ────────────────────────────────────────────

async def get_upcoming_ipo_events() -> list[dict]:
    """Bugun ve yarinki onemli IPO etkinliklerini getirir:
    - Basvuru son gunu (subscription_end)
    - Ilk islem gunu (trading_start)
    - Dagitim baslayanlar (in_distribution)
    - Sonuc aciklananlar (awaiting_trading)
    """
    try:
        from app.database import async_session
        from app.models.ipo import IPO
        from sqlalchemy import select, or_, and_

        today = date.today()
        tomorrow = today + timedelta(days=1)

        async with async_session() as session:
            # Dagitim surecinde olanlar
            result_dist = await session.execute(
                select(IPO).where(IPO.status == "in_distribution")
            )
            in_dist = list(result_dist.scalars().all())

            # Islem gunu beklenenler
            result_await = await session.execute(
                select(IPO).where(IPO.status == "awaiting_trading")
            )
            awaiting = list(result_await.scalars().all())

            # Yeni onaylananlar
            result_new = await session.execute(
                select(IPO).where(IPO.status == "newly_approved")
            )
            newly_approved = list(result_new.scalars().all())

            events = []

            for ipo in in_dist:
                # Basvuru son gunu bugün veya yarın mı?
                if ipo.subscription_end:
                    sub_end = ipo.subscription_end if isinstance(ipo.subscription_end, date) else None
                    if sub_end == today:
                        events.append({
                            "type": "SON_GUN",
                            "ticker": ipo.ticker or ipo.company_name,
                            "company": ipo.company_name,
                            "detail": f"Basvuru SON GUN! Saat {ipo.subscription_hours or '17:00'}'e kadar",
                            "date": today.isoformat(),
                        })
                    elif sub_end == tomorrow:
                        events.append({
                            "type": "SON_GUN_YARIN",
                            "ticker": ipo.ticker or ipo.company_name,
                            "company": ipo.company_name,
                            "detail": f"YARIN son gun — basvuruyu kacirmayin",
                            "date": tomorrow.isoformat(),
                        })

                # Dagitimda olanlar genel bilgi
                events.append({
                    "type": "DAGITIMDA",
                    "ticker": ipo.ticker or ipo.company_name,
                    "company": ipo.company_name,
                    "detail": f"Talep toplama devam ediyor — Fiyat: {ipo.ipo_price} TL"
                              + (f", Son gun: {ipo.subscription_end}" if ipo.subscription_end else ""),
                    "date": today.isoformat(),
                })

            for ipo in awaiting:
                # Islem gunu ilani beklenen
                if ipo.expected_trading_date:
                    exp_date = ipo.expected_trading_date if isinstance(ipo.expected_trading_date, date) else None
                    if exp_date == today:
                        events.append({
                            "type": "ILK_ISLEM",
                            "ticker": ipo.ticker or ipo.company_name,
                            "company": ipo.company_name,
                            "detail": f"BUGUN borsada ilk islem gunu! Halka arz fiyati: {ipo.ipo_price} TL",
                            "date": today.isoformat(),
                        })
                    elif exp_date == tomorrow:
                        events.append({
                            "type": "ILK_ISLEM_YARIN",
                            "ticker": ipo.ticker or ipo.company_name,
                            "company": ipo.company_name,
                            "detail": f"YARIN borsada ilk islem gunu",
                            "date": tomorrow.isoformat(),
                        })

                events.append({
                    "type": "ISLEM_BEKLENIYOR",
                    "ticker": ipo.ticker or ipo.company_name,
                    "company": ipo.company_name,
                    "detail": f"Islem gunu ilani bekleniyor"
                              + (f" — Beklenen: {ipo.expected_trading_date}" if ipo.expected_trading_date else ""),
                    "date": today.isoformat(),
                })

            for ipo in newly_approved:
                price_info = f" — Beklenen fiyat: {ipo.ipo_price} TL" if ipo.ipo_price else ""
                events.append({
                    "type": "YENI_ONAY",
                    "ticker": ipo.ticker or ipo.company_name,
                    "company": ipo.company_name,
                    "detail": f"SPK onayi aldi, dagitim sureci bekleniyor{price_info}. Yatirimci ilgisi yuksek.",
                    "date": today.isoformat(),
                })

            return events

    except Exception as e:
        logger.error("IPO etkinlik hatasi: %s", e)
        return []


# ────────────────────────────────────────────
# 7) AI Rapor Uretme
# ────────────────────────────────────────────

_HALLUCINATION_GUARD = """
⚠️ KRITIK KURAL — HALUSINASYON YASAGI:
- SADECE asagida verilen verileri kullan. Hicbir bilgiyi UYDURMA.
- Bir veri yoksa o konuyu TAMAMEN ATLA. "Veri bulunamadi", "veri yok", "degerlendirme yapilamadi",
  "hacim verisi olmadigi icin", "yeterli veri yok" gibi ifadeler KESINLIKLE YASAK.
  Yoklugu rapor etme — var olani yaz, olmayanı hic yazma.
- Halka arz tavan/taban verisi ceiling_tracks olarak gun gun verilmistir.
  Eger bir hisse icin ceiling_tracks verisi yoksa, tavan serisi/bozulma yorumu YAPMA — o hisseyi atla.
- Tavan serisi suresini SADECE "tavan_seri_gun" ve "daily_tracks" verilerinden oku.
  Islem gunu sayisi (trading_day_count) tavan serisi demak DEGILDIR!
- Haftanin hangi gunu oldugu TARIH satirinda yazilidir, buna uy.
- KAP/Telegram haberlerini ve RSS haberlerini referans alarak piyasa yorumu yap.
- Rakamlari yuvarlarken virgulden sonra max 2 basamak kullan.
- Halka arz verilerinde SADECE verilen bilgileri kullan. Kendi bilgini ekleme.
- Ekonomik takvimden bugun/yarin onemli etkinlikler varsa bahset.
- IPO etkinlikleri (son gun, ilk islem) varsa MUTLAKA vurgula — yatirimci icin kritik.
- Yazdiginda on dogrulama yap: "Bu bilgi verilen veride var mi?" — yoksa YAZMA.
- HICBIR ZAMAN "...icin degerlendirme yapilamadi" veya "...hakkinda bilgi yok" gibi bosluk doldurucu cumle kurma.

🚫 KAYNAK/REKLAM YASAGI — EN HASSAS KURAL — KESINLIKLE IHLAL ETME:
Acilis, kapanis, ogle veya herhangi bir raporda DIS SITE ADI, URL, KAYNAK ATFI veya
REKLAM niteliginde ifade KULLANMA. Bu kural istisnasizdir.

KESINLIKLE YASAK olan ornekler:
  ✗ "(Kaynak: halkarz.com, SPK)"
  ✗ "(Kaynak: ...)"
  ✗ "Kaynak: Bloomberg HT"
  ✗ "halkarz.com verilerine gore"
  ✗ "KAP.gov.tr'ye gore"
  ✗ "doviz.com, Bloomberg HT, Foreks" gibi site/platform adlari
  ✗ Herhangi bir URL veya web adresi (szalgo.net.tr HARIC)

KABUL EDILEBILIR ifadeler:
  ✓ "Piyasa verilerine gore..."
  ✓ "Aciklanan verilere gore..."
  ✓ "Borsada islem verileri..."
  ✓ "SPK onay sinyali aldiktan sonra..."  (site adi degil eylem bazli)
  ✓ szalgo.net.tr (sadece sondaki link satirinda)

BU KURALI IHLAL EDEN CIKTI KABUL EDILMEZ.
"""

_MORNING_SYSTEM_PROMPT = """Sen SZ Algo Trade'in kidemli piyasa analisti yapay zekasisin. Her sabah piyasa acilmadan once yatirimcilara profesyonel, detayli ve DOGRU ACILIS RAPORU yaziyorsun.

SABAH RAPORUNUN AMACI: Bugunku piyasa acilisina hazirlik + beklentiler + gelecek odakli analiz.
- Dun ne oldu degil, BUGUN ne olabilir odakli yaz.
- Global piyasalar + dolar/altin + BIST100 uzerinden acilis senaryolarini degerlendir.
- Halka arz takibinde: tavan devam eder mi, kar realizasyonu gelir mi, ilk islem gunu beklentisi ne?
- Yatirimciya aksiyon fikrini ACIK ver (ama tavsiye degil, "izlenebilir", "dikkat edilmeli" gibi).
- Dun aksamki kapanis analizi verilmisse, onu beklenti odakli yorumla — "Dun X kapandı, bugun Y bekleniyor" bagini kur.

ETKIILENIM KURALI — X ALGORITMASINI KAZANMANIN SIRRI:
🔥 HOOK (ILK 2 SATIR): Tweet'in ilk 2 satiri tek basina gorulecek — EN KRITIK KISIM!
- "📊 Açılış Analizi" gibi kuru baslik KESINLIKLE YASAK — kimse okumaz!
- Bunun yerine: merak, soru, carpici rakam veya surpriz bilgi ile bas
- Ornekler:
  ✅ "🚨 BIST BU SABAH KRİTİK SEVIYEDE! 3 gelişme gözde:"
  ✅ "📈 [TICKER] bugün tavan kırar mı? + BIST açılış beklentisi:"
  ✅ "⚡ Bu sabah piyasayı etkileyecek [N] kritik haber:"
  ✅ "💥 Dün [X]'de kapandı — bugün ne olur?"
  ✅ "🔑 Bugün dikkat: [somut gelisme]. Detaylar 👇"
- ASLA: "Merhaba 👋", "Günün açılış analizi", "Sabah raporu", "İyi günler" ile baslamak

❓ SON SATIR — SORU ZORUNLU (etkileşimi 3-5x artiriyor):
- Rapora MUTLAKA bir soru ile son ver (⚠️ tavsiye notundan ONCE veya SONRA)
- Ornekler:
  ✅ "Siz bugün BIST'te ne bekliyorsunuz? 👇"
  ✅ "Bu gelişmeye katılıyor musunuz? Yorumlara bırakın 💬"
  ✅ "Sizce #THYAO bu seviyelerde fırsat mı, risk mi? 👇"
  ✅ "Bugün hangi sektör öne çıkar? Düşünceleriniz? 💬"

KURAL:
1. Turkce yaz, profesyonel analist uslubunda — ciddi ve gercekci
2. Emoji kullan ama asiri degil (her bolumde 1-2)
3. Rakamlar ve yuzde degisimler net olsun
4. Halka arz performanslarini SADECE verilen verilerden yaz
5. Gunun onemli haberlerini RSS + KAP + Tavily kaynaklarindan ozetle
6. Hic yatirim tavsiyesi VERME — "yatirim tavsiyesi degildir" notu ekle
7. Tweet formati: max 3800 karakter (gorsel ile 4000 limiti var)
8. Yapilandirilmis format kullan: basliklar ve maddeler ile
9. Rapor EN AZ 150 kelime olmali — detayli ve icerikli yaz
10. Sonda mutlaka szalgo.net.tr linki olmali
11. Dis site adi veya URL YAZMA — sadece szalgo.net.tr kullan
12. Veri yoksa o konuyu ATLA — "veri yok", "degerlendirme yapilamadi" ASLA yazma

HASHTAG KURALI (COK ONEMLI — ERISIMI ARTIRAN EN KRITIK KURAL):
- Hashtag'leri AGRESIF kullan! X algoritmasi hashtag'li icerikleri KATKAT daha fazla kisi gosteriyor.
- Bahsettigin HER hisse kodunu hashtag yap: #THYAO #SASA #KCHOL #EREGL vb. — cumle icerisinde dogal
- IPO hisselerini MUTLAKA hashtag yap: #BESTE #ATATR gibi (bunlar en cok ilgi cekenler!)
- Yuksek etkili KAP haberlerindeki sirket kodlarini hashtag yap
- Tweet SONUNDA genel finansal hashtag yigini: #BIST100 #borsa #hisse #yatirim #BorsaIstanbul #HalkaArz #piyasa #finans
- Her paragrafta bahsedilen hisse, konu veya sektore gore hashtag kullan
- MINIMUM 8, IDEAL 12-15 hashtag hedefle — bunlar erisimi direk artiriyor
- Rapora profesyonel ton koru ama hashtag'lerden KESINLIKLE cekinme

AKILLI ANALIZ KURALLARI (FEW-SHOT ORNEKLER):
Asagidaki iyi/kotu ornek cifti, beklenen analiz kalitesini gostermektedir.

❌ KOTU ornek (dunu anlatiyor, oval yorum yok, yuzeysel):
"BESTE dun tavan yapti, 7. gun tamamlandi. EMPAE halka arz sureci devam ediyor."

✅ IYI ornek — HALKA ARZ BÖLÜMÜ (bugunü ve beklentiyi anlatiyor, oval yorum var):
"EMPAE (Empa Teknoloji): Bugün borsadaki ilk islem gunu! Acilis seansinda tavan serisi
baslatip baslatamaycagi merakla izlenecek. Sektor genelindeki alici istahi acilis icin
belirleyici olacak — ilk dakikalarda olusan derinlige gore pozisyon almak daha saglikli.

Tavan Serilerinde Kritik Esik:
• BESTE: 7. islem gunune giriyor. Bu noktada tavan surme ihtimali devam etse de kar
  realizasyonu riskini gormezden gelmemek gerekiyor; 34,26 TL destek seviyesi onemli.
• ATATR: 5 gunluk tavan serisinde %60 primiyle dikkat cekiyor. Bugün satici baskisi
  gelebilir — islem hacmini takip etmek kritik.

Yeni SPK Onaylılar: Gentaş ve Metropal dagitim takvimini bekliyor. Bu hafta aciklama
gelmesi halinde talep yogunlasacak; yatirimci radarinda tutulmali."

Halka arz bolumunde DAIMA bu tarzda yaz:
- Ilk islem gunu hisseleri icin acilis beklentisi + sektor yorumu
- Tavan serisi hisseleri icin bugun ne olabilir (kar realizasyonu? tavan devam? destek?)
- Yeni onaylılar icin kisa 1-2 cumle beklenti
- ASLA sadece "dun tavan yapti" deme — BUGUN ne olacak anlat
""" + _HALLUCINATION_GUARD + """
FORMAT:
⚡ [CARPICI_HOOK — MERAK UYANDIRICI ILK SATIR — TARIHIN DE OLDUGU KISA BASLIK]
[Ikinci satirda en onemli 1-2 piyasa verisi veya gelisme — somut rakamlarla]

🇹🇷 BIST 100 (XU100)
[onceki kapanis, degisim, bugun beklenti ve analiz]

🇺🇸 ABD Piyasalari
[S&P 500 ve Nasdaq verisi + acilis uzerindeki etkisi]

💰 Dolar & Altin
[USD/TRY ve altin verisi + yorumu]

📰 Gunun Onemli Gelismeleri
[RSS, KAP ve Tavily haberlerinden en onemli 2-3 gelisme — hisse hashtag'leriyle]

📅 Ekonomik Takvim
[Bugun ve yarin onemli ekonomik veri aciklamalari varsa yaz — TCMB, TUIK, Fed vs.]
[Etkinlik yoksa bu bolumu atla]

🏦 Halka Arz Takibi

⭐ BU BOLUM ICIN ZORUNLU YAZIM TARZI — ASAGIDAKI SOMUT ORNEGI AYNEN TAKLIT ET:

✅ MUKEMMEL ORNEK (bu stili yaz — rakamlar/hisseler farkli olacak ama ton ve yaklasim AYNI olmali):
---
🏦 Halka Arz Takibi

EMPAE (Empa Teknoloji): Bugün borsadaki ilk işlem günü! Gözler açılış seansında oluşacak
tavan serisi beklentisinde. Teknoloji sektöründeki güçlü hava EMPAE için pozitif bir
rüzgar yaratabilir — ancak piyasa geneli belirleyici olacak.

SVGYO (Sevgi GYO): Talep toplama sürecinde ikinci gün. Yarın (Cuma) son gün olduğu için
bugün katılımın zirve yapması bekleniyor. GYO sektörüne olan ilgi ve portföy yapısı,
katılım sayısını belirleyecek ana unsur.

Tavan Serilerinde Kritik Eşik:
• BESTE: Dün gelen ilk taban kapanışı sonrası bugün 34,26 TL seviyesinde dengelenip
  dengelenmeyeceği izlenecek. Kurumsal taraftan gelecek alımlar takibimizde.
• ATATR: 5 gündür tavan giden hissede bugün kâr realizasyonu riskine karşı dikkatli
  olunmalı. %60,9 primle piyasa ortalamasının üzerinde bir performans sergiliyor.

Yeni SPK Onaylılar: LXGYO, Gentaş ve Metropal dağıtım takvimini bekliyor.
Yatırımcı radarında olan bu 3 halka arz için takvim netleştikçe ilgi artacak.
---

KURALLARI:
- Ilk islem gunu: "Bugün borsadaki ilk islem gunu! Gözler... pozitif/temkinli rüzgar" tarzı
- Dagitimda: "Talep toplama X. gün, [son gun varsa 'Yarın son gün!']... belirleyici unsur"
- Tavan serisi: "X. islem gununde, bugün kar realizasyonu riski / tavan kırılma ihtimali..."
- Yeni onaylar: "dağıtım takvimini bekliyor, yatırımcı radarında"
- YASAK: "Dün tavan yaptı" gibi dünü anlatma — bugünü ve beklentiyi anlat
- YASAK: Veri olmayan hisse için kesin tavan garantisi verme — "olabilir", "bekleniyor" kullan
- Kapanış fiyatini daily_tracks'ten oku ve yaz (orn "34,26 TL seviyesinde")
- Tavan serisi sayisini SADECE tavan_seri_gun alanından al

📌 Bugünün Kritik Noktaları
[kisa ozet, en onemli 2-3 dikkat edilecek sey — somut hisse/seviye belirt]

💬 [SORU — ornekler: "Siz ne bekliyorsunuz? 👇" / "Bu gelişmeye katılıyor musunuz? 💬" / "Hangi hisse radarınızda? 👇"]

⚠️ Yatirim tavsiyesi degildir.

📲 szalgo.net.tr

#BIST100 #borsa #HalkaArz #hisse #yatirim #BorsaIstanbul"""

_EVENING_SYSTEM_PROMPT = """Sen SZ Algo Trade'in kidemli piyasa analisti yapay zekasisin. Her aksam piyasa kapandiktan sonra GUN SONU DEGERLENDIRME raporu yaziyorsun.

AKSAM RAPORUNUN AMACI: Gunun tam degerlendirmesi + ne oldu + neden oldu + yarin ne bekleniyor.
- BIST100 kapanis, hacim, piyasa derinligi yorumla.
- Halka arz hisselerinin gun ici performansini degerlendir: tavan mi, kirildi mi, neden?
- Global piyasa akisi + doviz/altin kapanis + yarin beklenti.
- Gunun KAP haberleri icinde piyasayi etkileyenleri YORUMLA — sadece siralama degil.
- DERINLIKLI analiz: neden yukseldi/dustu, hacim neyi gosteriyor, yarin ne bekleniyor?

ETKIILENIM KURALI — X ALGORITMASINI KAZANMANIN SIRRI:
🔥 HOOK (ILK 2 SATIR): Tweet'in ilk 2 satiri tek basina gorulecek — EN KRITIK KISIM!
- "📊 Kapanış Raporu" gibi kuru baslik KESINLIKLE YASAK — kimse okumaz!
- Bunun yerine: carpici rakam, surpriz gelisme veya merak uyandirici soru ile bas
- Ornekler:
  ✅ "🔴 BIST bugün [X] puanda kapandı — sebebi ne? 3 kritik gelişme:"
  ✅ "📉 #TICKER bugün taban! Neden? + Yarın ne bekleniyor?"
  ✅ "🚨 Piyasayı sarsan [N] haber — gün sonu özeti:"
  ✅ "💡 Bugün BIST [%X] ile [yükseldi/düştü] — ardındaki 3 sebep:"
  ✅ "⚡ [somut gelisme] — Yarın için bilmen gereken her şey 👇"
- ASLA: "Akşam raporu", "Kapanış analizi", "Gün sonu değerlendirme" ile baslama

❓ SON SATIR — SORU ZORUNLU (etkileşimi 3-5x artiriyor):
- Rapora MUTLAKA bir soru ile son ver
- Ornekler:
  ✅ "Yarın BIST ne yapar? Tahminleriniz? 👇"
  ✅ "Bugünkü hareketi bekliyor muydunuz? 💬"
  ✅ "Bu düşüşü fırsat olarak görüyor musunuz? 👇"
  ✅ "Yarın hangi hisse öne çıkar sizce? 💬"

KURAL:
1. Turkce yaz, profesyonel analist uslubunda — ciddi ve gercekci
2. Emoji kullan ama asiri degil (her bolumde 1-2)
3. Rakamlar ve yuzde degisimler net olsun
4. Halka arz performanslarini SADECE verilen verilerden yaz
5. Gunun onemli haberlerini RSS + KAP kaynaklarindan ozetle
6. Hic yatirim tavsiyesi VERME — "yatirim tavsiyesi degildir" notu ekle
7. Tweet formati: max 3800 karakter (gorsel ile 4000 limiti var)
8. Yapilandirilmis format kullan: basliklar ve maddeler ile
9. Rapor EN AZ 150 kelime olmali — detayli ve icerikli yaz
10. Sonda mutlaka szalgo.net.tr linki olmali
11. Dis site adi veya URL YAZMA — sadece szalgo.net.tr kullan
12. Veri yoksa o konuyu ATLA — "veri yok", "degerlendirme yapilamadi" ASLA yazma

HASHTAG KURALI (COK ONEMLI — ERISIMI ARTIRAN EN KRITIK KURAL):
- Hashtag'leri AGRESIF kullan! X algoritmasi hashtag'li icerikleri KATKAT daha fazla kisi gosteriyor.
- Bahsettigin HER hisse kodunu hashtag yap: #THYAO #SASA #KCHOL #EREGL vb. — cumle icerisinde dogal
- IPO hisselerini MUTLAKA hashtag yap: #BESTE #ATATR gibi (bunlar en cok ilgi cekenler!)
- Tweet SONUNDA genel finansal hashtag yigini: #BIST100 #borsa #hisse #yatirim #BorsaIstanbul #HalkaArz #piyasa #finans
- Her paragrafta bahsedilen hisse, konu veya sektore gore hashtag kullan
- MINIMUM 8, IDEAL 12-15 hashtag hedefle — bunlar erisimi direk artiriyor
- Rapora profesyonel ton koru ama hashtag'lerden KESINLIKLE cekinme

AKILLI ANALIZ KURALLARI (FEW-SHOT ORNEKLER):
Asagidaki iyi/kotu ornek cifti, beklenen analiz kalitesini gostermektedir.

❌ KOTU ornek (hallusinasyon + kaynak atfi + yuzeysel):
"Piyasalar bugun dusus yasamadi. (Kaynak: doviz.com) BIST 100 yukseldi. Yarinki acilis olumlu bekleniyor."

✅ IYI ornek (veri bazli + kaynak yok + derinlikli):
"BIST 100 gun sonunda %1.2 dususle 9.710 puanda kapandı. Islem hacmi 42 milyar TL ile ortalamanın
%15 uzerinde gerçeklesti — satıs baskısı hacimle desteklendi. Halka arz tarafinda AKHAN bugün
dağıtım sürecini tamamladı; 3. islem gununde %8.5 prim ile kapanması olumlu bir giriş sinyali."

Ciktin her zaman ikinci ornege yakin olmali: somut rakamlar, veri bazli yorumlar, kaynak atfi YOK.
""" + _HALLUCINATION_GUARD + """
FORMAT:
⚡ [CARPICI_HOOK — BIST kapanış rakamı + carpici soru veya surpriz — ILK SATIR MERAK UYANDIRSIN]
[Ikinci satirda en onemli 1-2 piyasa gercegi — somut rakamlarla]

🇹🇷 BIST 100 (XU100)
[kapanis, degisim, neden yukseldi/dustu — derinlikli]

🇺🇸 ABD Piyasalari
[S&P 500 ve Nasdaq verileri + Turk piyasasina etkisi]

💰 Dolar & Altin
[USD/TRY ve altin kapanis + yorumu]

📰 Gunun Onemli Gelismeleri
[RSS ve KAP haberlerinden en onemli 2-3 gelisme — ilgili hisse hashtag'leriyle]

📅 Ekonomik Takvim
[Bugun aciklanan veriler ve yarin beklenen onemli veriler varsa yaz]
[Etkinlik yoksa bu bolumu atla]

🏦 Halka Arz Takibi
[islemdeki IPO'lar — SADECE ceiling_tracks verisine dayanarak]
[Hisse kodlarini dogal cumle icerisinde hashtag ile yaz: "BESTE bugün..."]
[Tavan serisi bilgisini sadece tavan_seri_gun ve daily_tracks'ten al]
[Basvuru son gunu veya ilk islem gunu varsa VURGULA — yatirimci icin kritik bilgi!]

📌 Yarın İçin Beklentiler
[onemli gelismeler, yarin dikkat edilecekler — somut hisse/seviye belirt]

💬 [SORU — "Yarın ne bekliyorsunuz? 👇" / "Bugünkü hareketi bekliyor muydunuz? 💬" / özel soru]

⚠️ Yatirim tavsiyesi degildir.

📲 szalgo.net.tr

#BIST100 #borsa #HalkaArz #hisse #yatirim #BorsaIstanbul"""


def _format_full_context(
    market_data: dict,
    ipos: list[dict],
    kap_news: list[dict],
    telegram_news: list[dict],
    rss_headlines: list[dict],
    econ_calendar: dict,
    ipo_events: list[dict],
    report_type: str,
    high_impact_kap: list[dict] | None = None,
    tavily_news: list[dict] | None = None,
    last_evening_report: dict | None = None,
) -> str:
    """AI'a gonderilecek zengin veri kontekstini formatlar — 11 kaynak (sabah: +KAP>=8 + Tavily + aksam cache)."""
    today = date.today()
    day_name = _TR_DAY_NAMES.get(today.weekday(), "Bilinmiyor")

    lines = [f"RAPOR TURU: {'Sabah Acilis' if report_type == 'morning' else 'Aksam Kapanis'}"]
    lines.append(f"TARIH: {today.isoformat()} ({day_name})")
    lines.append(f"HAFTANIN GUNU: {day_name} (haftanin {today.weekday() + 1}. is gunu)")
    if report_type == "morning":
        lines.append("")
        lines.append("⚠️ SABAH RAPORU OZEL TALIMAT — HALKA ARZ BOLUMU:")
        lines.append("  Bu sabah raporu. Halka arz bolumunde DUN ne oldu deil, BUGUN ne bekleniyor yaz.")
        lines.append("  Islemdeki hisseler icin 'bugün X. gününde, tavan sürmesi/kırılması bekleniyor' tarzı oval yorum ekle.")
        lines.append("  YENI_ONAY / DAGITIMDA / ISLEM_BEKLENIYOR IPO'larini da 🏦 bolumunde ozet ver.")
        lines.append("  Yatirimci beklenti odakli yorum yap — 'Piyasanin geneline gore performans...' gibi.")

    # ── Dünkü Kapanış Raporu (SADECE sabah raporu — beklenti bağlantısı için) ──
    if last_evening_report and report_type == "morning":
        lines.append("")
        lines.append("=" * 50)
        saved_at = last_evening_report.get("saved_at", "?")
        lines.append(f"DUN AKSAMIN KAPANIS RAPORU (beklenti analizi icin kullan — kaydedilme: {saved_at[:16]})")
        lines.append("Bu raporu oku ve sabah raporunda 'Dun X oldu, bugun Y bekleniyor' baglantisini kur.")
        lines.append("Hook (ilk satir) icin bu rapordan carpici bir bilgi al!")
        lines.append("=" * 50)
        evening_text = last_evening_report.get("text", "")[:2000]  # Max 2000 char context
        lines.append(evening_text)
        lines.append("(Dun kapanis raporu sonu)")

    lines.append("")

    # ── Piyasa Verileri ──
    lines.append("=" * 50)
    lines.append("PIYASA VERILERI")
    lines.append("=" * 50)
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

    # ── RSS Haberleri ──
    lines.append("")
    lines.append("=" * 50)
    lines.append("HABERLER (RSS)")
    lines.append("=" * 50)
    if rss_headlines:
        for h in rss_headlines:
            lines.append(f"  [{h['source']}] {h['title']}")
    else:
        lines.append("  Haber alinamadi.")

    # ── KAP Haberleri (DB) ──
    lines.append("")
    lines.append("=" * 50)
    lines.append("KAP BILDIRIMLERI (Son 24 saat)")
    lines.append("=" * 50)
    if kap_news:
        for n in kap_news:
            sentiment_icon = {"positive": "+", "negative": "-", "neutral": "~"}.get(n["sentiment"], "?")
            lines.append(f"  [{sentiment_icon}] {n['ticker']}: {n['title']}")
            if n.get("detail"):
                lines.append(f"      Detay: {n['detail']}")
    else:
        lines.append("  Son 24 saatte onemli KAP haberi yok.")

    # ── Telegram AI Haberleri (DB) ──
    lines.append("")
    lines.append("=" * 50)
    lines.append("AI HABER TARAMASI (Telegram kaynakli, son 24 saat)")
    lines.append("=" * 50)
    if telegram_news:
        for n in telegram_news:
            score = f"AI Skor: {n['ai_score']:.1f}/10" if n.get("ai_score") else ""
            lines.append(f"  {n['ticker'] or '?'}: {n['title']} {score}")
            if n.get("summary"):
                lines.append(f"      Ozet: {n['summary']}")
    else:
        lines.append("  Son 24 saatte AI taramasindan haber yok.")

    # ── Halka Arz Verileri (DB + ceiling_tracks) ──
    lines.append("")
    lines.append("=" * 50)
    lines.append("ISLEMDEKI HALKA ARZLAR (ceiling_tracks ile)")
    lines.append("=" * 50)

    if ipos:
        for ipo in ipos:
            ticker = ipo["ticker"] or "?"
            lines.append("")
            lines.append(f"--- {ticker} ({ipo['company']}) ---")
            lines.append(f"  Halka arz fiyati: {ipo['ipo_price']} TL")
            lines.append(f"  Bugun {ipo['trading_day_count']}. islem gunu")
            lines.append(f"  Son kapanis fiyati: {ipo.get('last_close_price', 'Bilinmiyor')} TL")

            if ipo["pct_from_ipo_price"] is not None:
                lines.append(f"  Halka arz fiyatindan toplam degisim: %{ipo['pct_from_ipo_price']}")

            if ipo.get("high_from_start"):
                lines.append(f"  Baslangictan en yuksek fiyat: {ipo['high_from_start']} TL")

            # Tavan serisi
            tavan_seri = ipo.get("tavan_seri_gun", 0)
            ceiling_broken = ipo.get("ceiling_broken", False)
            if tavan_seri > 0:
                if ceiling_broken:
                    lines.append(f"  Tavan serisi: {tavan_seri} gun tavan yapti, sonra BOZULDU")
                    if ipo.get("ceiling_broken_at"):
                        lines.append(f"  Tavan bozulma zamani: {ipo['ceiling_broken_at']}")
                else:
                    lines.append(f"  Tavan serisi: {tavan_seri} gun ust uste tavan (DEVAM EDIYOR)")
            else:
                lines.append("  Tavan serisi: Yok (baslangicta hic tavan yapmadi)")

            # Gunluk detay
            daily = ipo.get("daily_tracks", [])
            if daily:
                lines.append(f"  Gunluk islem detayi ({len(daily)} gun):")
                for d in daily:
                    pct = f", degisim: %{d['pct_degisim']}" if d.get("pct_degisim") is not None else ""
                    lines.append(
                        f"    {d['gun']}. gun ({d.get('tarih', '?')}): "
                        f"{d['durum_aciklama']}"
                        f" — Kapanis: {d.get('kapanis', '?')} TL{pct}"
                    )
            else:
                lines.append("  Gunluk islem detayi: Henuz veri yok")
    else:
        lines.append("Su an 25 gun altinda islem goren halka arz yok.")

    # ── Yaklasan IPO Etkinlikleri (DB) ──
    lines.append("")
    lines.append("=" * 50)
    lines.append("YAKLASAN HALKA ARZ ETKINLIKLERI")
    lines.append("=" * 50)
    if ipo_events:
        for ev in ipo_events:
            type_icon = {
                "SON_GUN": "🔴 SON GUN",
                "SON_GUN_YARIN": "🟡 YARIN SON GUN",
                "ILK_ISLEM": "🟢 ILK ISLEM GUNU",
                "ILK_ISLEM_YARIN": "🟢 YARIN ILK ISLEM",
                "DAGITIMDA": "📋 DAGITIMDA",
                "ISLEM_BEKLENIYOR": "⏳ ISLEM BEKLENIYOR",
                "YENI_ONAY": "✅ YENI SPK ONAYI",
            }.get(ev["type"], ev["type"])
            lines.append(f"  {type_icon} — {ev['ticker']} ({ev['company']})")
            lines.append(f"    {ev['detail']}")
    else:
        lines.append("  Yaklasan onemli IPO etkinligi yok.")

    # ── Ekonomik Takvim (doviz.com) ──
    lines.append("")
    lines.append("=" * 50)
    lines.append("EKONOMIK TAKVIM")
    lines.append("=" * 50)

    today_events = econ_calendar.get("today", [])
    tomorrow_events = econ_calendar.get("tomorrow", [])

    if today_events:
        lines.append("BUGUN:")
        for ev in today_events[:12]:  # Max 12 etkinlik
            importance_tag = f"[{ev['importance']}]" if ev.get("importance") else ""
            actual = f" Gerceklesen: {ev['actual']}" if ev.get("actual") else ""
            forecast = f" Beklenti: {ev['forecast']}" if ev.get("forecast") else ""
            previous = f" Onceki: {ev['previous']}" if ev.get("previous") else ""
            lines.append(
                f"  {ev['time']} {ev['country']} {importance_tag} {ev['event']}"
                f"{actual}{forecast}{previous}"
            )
    else:
        lines.append("BUGUN: Onemli ekonomik veri aciklamasi yok.")

    if tomorrow_events:
        lines.append("YARIN:")
        for ev in tomorrow_events[:8]:
            importance_tag = f"[{ev['importance']}]" if ev.get("importance") else ""
            forecast = f" Beklenti: {ev['forecast']}" if ev.get("forecast") else ""
            previous = f" Onceki: {ev['previous']}" if ev.get("previous") else ""
            lines.append(
                f"  {ev['time']} {ev['country']} {importance_tag} {ev['event']}"
                f"{forecast}{previous}"
            )
    elif not today_events:
        lines.append("YARIN: Ekonomik takvim verisi alinamadi.")

    # ── Yüksek Etkili KAP Bildirimleri (SADECE sabah raporu) ──
    if high_impact_kap is not None:
        lines.append("")
        lines.append("=" * 50)
        lines.append("YUKSEK ETKILI KAP BILDIRIMLERI (ai_impact_score >= 8, Son 20 saat)")
        lines.append("Bunlar en onemli sirket haberleri — mutlaka analiz et ve hashtag'lerle vurgula!")
        lines.append("=" * 50)
        if high_impact_kap:
            for d in high_impact_kap:
                sentiment_icon = {"Olumlu": "📈", "Olumsuz": "📉", "Notr": "~"}.get(d.get("sentiment", ""), "❓")
                lines.append(
                    f"  {sentiment_icon} #{d['ticker']} — {d['title']} "
                    f"(Skor: {d.get('score', '?'):.1f}/10, {d.get('sentiment', '?')}, {d.get('category', '?')})"
                )
                if d.get("summary"):
                    lines.append(f"      Ozet: {d['summary']}")
        else:
            lines.append("  Son 20 saatte yuksek etkili (>=8) KAP bildirimi yok.")

    # ── Tavily Web Araştırması (SADECE sabah raporu) ──
    if tavily_news is not None:
        lines.append("")
        lines.append("=" * 50)
        lines.append("WEB ARASTIRMASI (Tavily — Guncel Haberler)")
        lines.append("Bu verileri kullanarak raporu daha guncel ve zengin yap. Kaynak adi YAZMA.")
        lines.append("=" * 50)
        if tavily_news:
            for t in tavily_news:
                lines.append(f"  • {t['title']}")
                if t.get("content"):
                    lines.append(f"      {t['content']}")
        else:
            lines.append("  Web arastirmasi sonucu alinmadi.")

    return "\n".join(lines)


async def _generate_report(
    market_data: dict,
    ipos: list[dict],
    kap_news: list[dict],
    telegram_news: list[dict],
    rss_headlines: list[dict],
    econ_calendar: dict,
    ipo_events: list[dict],
    report_type: str,
    high_impact_kap: list[dict] | None = None,
    tavily_news: list[dict] | None = None,
    last_evening_report: dict | None = None,
) -> str | None:
    """AI ile piyasa raporu uretir — 11 kaynak zengin veri konteksti ile (sabah: +KAP>=8 + Tavily + aksam cache)."""
    _settings = get_settings()
    gemini_key = _settings.GEMINI_API_KEY if _settings.GEMINI_API_KEY else None
    api_key = _settings.ABACUS_API_KEY
    anthropic_key = getattr(_settings, "ANTHROPIC_API_KEY", None) or None

    if not gemini_key and not api_key and not anthropic_key:
        logger.error("API key yok (Gemini/Abacus/Claude) — rapor uretilemedi")
        return None

    system_prompt = _MORNING_SYSTEM_PROMPT if report_type == "morning" else _EVENING_SYSTEM_PROMPT
    context = _format_full_context(
        market_data, ipos, kap_news, telegram_news, rss_headlines,
        econ_calendar, ipo_events, report_type,
        high_impact_kap=high_impact_kap,
        tavily_news=tavily_news,
        last_evening_report=last_evening_report,
    )

    user_message = (
        "Asagidaki GERCEK verileri kullanarak rapor yaz.\n"
        "SADECE verilen verilere dayan — hicbir bilgiyi UYDURMA.\n"
        "Halka arz tavan serisi bilgisini SADECE daily_tracks verilerinden oku.\n"
        "Dis site adi veya URL kullanma, sadece szalgo.net.tr.\n\n"
        f"{context}"
    )

    logger.info(
        "AI %s raporu kontekst: %d kar, %d IPO, %d KAP, %d Telegram, %d RSS, %d+%d takvim, %d IPO-event",
        report_type, len(context), len(ipos), len(kap_news), len(telegram_news),
        len(rss_headlines), len(econ_calendar.get("today", [])),
        len(econ_calendar.get("tomorrow", [])), len(ipo_events),
    )

    # OpenAI-uyumlu payload (Gemini + Abacus icin)
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message},
    ]
    payload_base = {
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": 8192,  # Gemini 2.5 thinking token yiyor
    }

    ai_content = None

    # ── 1. Birincil: Gemini 2.5 Pro ──
    if gemini_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                resp = await client.post(
                    _GEMINI_URL,
                    headers={
                        "Authorization": f"Bearer {gemini_key}",
                        "Content-Type": "application/json",
                    },
                    json={**payload_base, "model": _GEMINI_MODEL},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    ai_content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    if ai_content:
                        logger.info("AI %s raporu [Gemini-Pro] uretildi: %d karakter", report_type, len(ai_content))
                else:
                    logger.warning("AI rapor Gemini hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
        except Exception as e:
            logger.warning("AI rapor Gemini hata: %s", e)

    # ── 2. Yedek: Abacus AI ──
    if not ai_content and api_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                resp = await client.post(
                    _ABACUS_URL,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json={**payload_base, "model": _AI_MODEL},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    ai_content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    if ai_content:
                        logger.info("AI %s raporu [Abacus] uretildi: %d karakter", report_type, len(ai_content))
                else:
                    logger.warning("AI rapor Abacus hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
        except Exception as e:
            logger.warning("AI rapor Abacus hata: %s", e)

    # ── 3. Yedek: Anthropic Claude Sonnet 4 ──
    if not ai_content and anthropic_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                resp = await client.post(
                    _ANTHROPIC_URL,
                    headers={
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": _CLAUDE_MODEL,
                        "max_tokens": 8192,  # Gemini 2.5 thinking token yiyor
                        "system": system_prompt,
                        "messages": [{"role": "user", "content": user_message}],
                        "temperature": 0.2,
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for block in data.get("content", []):
                        if block.get("type") == "text":
                            ai_content = block.get("text", "").strip()
                            break
                    if ai_content:
                        logger.info("AI %s raporu [Claude-Sonnet] uretildi: %d karakter", report_type, len(ai_content))
                else:
                    logger.error("AI rapor Claude hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
        except Exception as e:
            logger.error("AI rapor Claude hata: %s", e)

    if not ai_content:
        logger.error("AI rapor: Tum providerlar basarisiz")
        return None

    # ── Hashtag zenginleştirme: IPO + yüksek etkili KAP + genel finansal ──
    extra_tags: list[str] = []

    # 1) IPO ticker hashtag'leri
    if ipos:
        for ipo in ipos:
            ticker = ipo.get("ticker") if isinstance(ipo, dict) else getattr(ipo, "ticker", None)
            if ticker and ticker.strip():
                tag = f"#{ticker.strip().upper()}"
                if tag not in extra_tags:
                    extra_tags.append(tag)

    # 2) Yüksek etkili KAP ticker hashtag'leri (sadece sabah raporu)
    if high_impact_kap:
        for d in high_impact_kap:
            ticker = d.get("ticker", "")
            if ticker and ticker.strip():
                tag = f"#{ticker.strip().upper()}"
                if tag not in extra_tags:
                    extra_tags.append(tag)

    # 3) Genel finansal hashtag'ler — erisimi en cok artiran bunlar
    general_tags = [
        "#BIST100", "#borsa", "#hisse", "#yatirim",
        "#BorsaIstanbul", "#HalkaArz", "#piyasa", "#finans",
    ]
    for gt in general_tags:
        if gt not in extra_tags and gt.lower() not in ai_content.lower():
            extra_tags.append(gt)

    # Tum ek hashtag'ler — max 20 toplam
    all_extra = " ".join(extra_tags[:20])
    hashtag_suffix = ("\n" + all_extra) if all_extra else ""

    # Tweet karakter limiti (gorsel ile 4000)
    content_with_tags = ai_content.strip() + hashtag_suffix
    if len(content_with_tags) > 3900:
        # Once genel hashtag'leri kirp, sadece IPO+KAP birak
        short_tags = " ".join(extra_tags[:10])
        content_short = ai_content.strip() + ("\n" + short_tags if short_tags else "")
        if len(content_short) <= 3900:
            return content_short
        # Hala sigmazsa yalnizca metin
        if len(ai_content) > 3900:
            ai_content = ai_content[:3897] + "..."
        return ai_content.strip()

    return content_with_tags


# ────────────────────────────────────────────
# 6) Tweet Gonderme
# ────────────────────────────────────────────

async def _collect_all_data() -> tuple:
    """Tum 8 veri kaynagini paralel toplar (BigPara RSS dahil)."""
    import asyncio

    # 7 kaynak paralel calissin
    market_task = asyncio.create_task(fetch_market_snapshot())
    ipos_task = asyncio.create_task(get_active_ipos_performance())
    kap_task = asyncio.create_task(get_recent_kap_news(hours=24, limit=10))
    telegram_task = asyncio.create_task(get_recent_telegram_news(hours=24, limit=10))
    rss_task = asyncio.create_task(fetch_rss_headlines())
    calendar_task = asyncio.create_task(fetch_economic_calendar())
    ipo_events_task = asyncio.create_task(get_upcoming_ipo_events())

    market_data = await market_task
    ipos = await ipos_task
    kap_news = await kap_task
    telegram_news = await telegram_task
    rss_headlines = await rss_task
    econ_calendar = await calendar_task
    ipo_events = await ipo_events_task

    return market_data, ipos, kap_news, telegram_news, rss_headlines, econ_calendar, ipo_events


async def send_morning_report_tweet():
    """Sabah acilis raporu tweeti gonderir (08:15 TR).

    Toplanan veri kaynaklari (10 kaynak):
    1-7) _collect_all_data — market, IPO, KAP, Telegram, RSS, takvim, IPO-events
    8)   get_high_impact_kap_disclosures — ai_impact_score>=8 onemli KAP haberleri
    9-10) fetch_tavily_morning_news — Tavily ile web arastirmasi (2 sorgu)
    """
    import asyncio

    logger.info("Sabah acilis raporu hazirlaniyor — 10 kaynak toplanıyor...")

    # Ana veri kaynaklari + sabah ozel veri kaynaklari paralel
    main_task = asyncio.create_task(_collect_all_data())
    kap_impact_task = asyncio.create_task(get_high_impact_kap_disclosures(min_score=8.0, hours=20, limit=12))
    tavily_task = asyncio.create_task(fetch_tavily_morning_news())

    (market_data, ipos, kap_news, telegram_news,
     rss_headlines, econ_calendar, ipo_events) = await main_task
    high_impact_kap = await kap_impact_task
    tavily_news = await tavily_task

    # Dünkü akşam kapanış raporu — beklenti bağlantısı için
    last_evening_report = get_last_report_from_cache("evening")
    if last_evening_report:
        logger.info("Dün aksamin kapanis raporu cache'ten alindi — sabaha tasiniyor")
    else:
        logger.info("Dün aksamin kapanis raporu cache'de yok — ilk rapor veya eski")

    logger.info(
        "Sabah veri toplama tamam: %d yuksek-KAP, %d Tavily sonucu, aksam cache=%s",
        len(high_impact_kap), len(tavily_news), "VAR" if last_evening_report else "YOK"
    )

    report_text = await _generate_report(
        market_data, ipos, kap_news, telegram_news, rss_headlines,
        econ_calendar, ipo_events, "morning",
        high_impact_kap=high_impact_kap,
        tavily_news=tavily_news,
        last_evening_report=last_evening_report,
    )
    if not report_text:
        logger.error("Sabah raporu uretilemedi — tweet atilmadi")
        return

    from app.services.twitter_service import _safe_tweet_with_media
    success = _safe_tweet_with_media(
        report_text,
        _ACILIS_IMAGE,
        source="morning_market_report",
        force_send=True,
    )

    if success:
        logger.info("Sabah acilis raporu tweeti basarili!")
    else:
        logger.error("Sabah acilis raporu tweeti BASARISIZ")


async def send_evening_report_tweet():
    """Aksam kapanis raporu tweeti gonderir (20:45 TR).

    Rapor olusturulduktan sonra cache'e kaydedilir — sabah raporu
    bunu okuyarak 'dun kapanis → bugun beklenti' bagini kurar.
    """
    logger.info("Aksam kapanis raporu hazirlaniyor — 7 kaynak toplanıyor...")

    (market_data, ipos, kap_news, telegram_news,
     rss_headlines, econ_calendar, ipo_events) = await _collect_all_data()

    report_text = await _generate_report(
        market_data, ipos, kap_news, telegram_news, rss_headlines,
        econ_calendar, ipo_events, "evening",
    )
    if not report_text:
        logger.error("Aksam raporu uretilemedi — tweet atilmadi")
        return

    # Aksam raporunu cache'e kaydet — sabah raporu kullanacak
    save_report_to_cache("evening", report_text)

    from app.services.twitter_service import _safe_tweet_with_media
    success = _safe_tweet_with_media(
        report_text,
        _KAPANIS_IMAGE,
        source="evening_market_report",
        force_send=True,
    )

    if success:
        logger.info("Aksam kapanis raporu tweeti basarili!")
    else:
        logger.error("Aksam kapanis raporu tweeti BASARISIZ")
