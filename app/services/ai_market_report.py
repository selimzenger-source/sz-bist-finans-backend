"""AI Piyasa Raporu Servisi — Sabah Acilis + Aksam Kapanis Raporu

Sabah 08:15 TR: Onceki gun verileri + bugunun beklentileri
Aksam 20:45 TR: Gunun kapanis verileri + degerlendirme
Her ikisi de X (Twitter) uzerinden gorsel + metin tweet olarak paylasilir.

v3: Gercek haber kaynaklari (DB + dis kaynak RSS) + ceiling_tracks + halusinasyon korumasi
"""

import logging
import os
import xml.etree.ElementTree as ET
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
_AI_TIMEOUT = 60  # Daha fazla veri isliyor, daha uzun sure

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
]

# Gorsel dosya yollari
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ACILIS_IMAGE = os.path.join(_BASE_DIR, "static", "img", "acilis_raporu_banner.png")
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
# 5) AI Rapor Uretme
# ────────────────────────────────────────────

_HALLUCINATION_GUARD = """
⚠️ KRITIK KURAL — HALUSINASYON YASAGI:
- SADECE asagida verilen verileri kullan. Hicbir bilgiyi UYDURMA.
- Bir veri yoksa o konuyu atla veya "veri bulunamadi" yaz.
- Halka arz tavan/taban verisi ceiling_tracks olarak gun gun verilmistir.
  Eger bir hisse icin ceiling_tracks verisi yoksa, tavan serisi yorumu YAPMA.
- Tavan serisi suresini SADECE "tavan_seri_gun" ve "daily_tracks" verilerinden oku.
  Islem gunu sayisi (trading_day_count) tavan serisi demek DEGILDIR!
- Haftanin hangi gunu oldugu TARIH satirinda yazilidir, buna uy.
- KAP/Telegram haberlerini ve RSS haberlerini referans alarak piyasa yorumu yap.
- Haber kaynaklarini belirt (orn: "Bloomberg HT'ye gore...", "KAP bildirimlerine gore...").
- Rakamlari yuvarlarken virgulden sonra max 2 basamak kullan.
- Halka arz verilerinde SADECE verilen bilgileri kullan. Kendi bilgini ekleme.
"""

_MORNING_SYSTEM_PROMPT = """Sen SZ Algo Trade'in kidemli piyasa analisti yapay zekasisin. Her sabah piyasa acilmadan once yatirimcilara profesyonel, detayli ve DOGRU rapor yaziyorsun.

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
10. Sonda mutlaka szalgo.net.tr linki ve hashtag'ler olmali
11. Haber kaynaklarini referans goster (Bloomberg HT, KAP vs.)
""" + _HALLUCINATION_GUARD + """
FORMAT:
📊 AÇILIŞ RAPORU — [gun_adi], [tarih]

🇹🇷 BIST 100 (XU100)
[onceki kapanis, degisim, analiz]

🇺🇸 ABD Piyasalari
[S&P 500 ve Nasdaq verisi + analiz]

💰 Dolar & Altin
[USD/TRY ve altin verisi]

📰 Gunun Onemli Gelismeleri
[RSS ve KAP haberlerinden en onemli 2-3 gelisme]

🏦 Halka Arz Takibi
[islemdeki IPO'lar — SADECE ceiling_tracks verisine dayanarak]
[Her hisse kodunu #TICKER formatiyla yaz, orn: #BESTE, #AKHAN]
[Tavan serisi bilgisini sadece tavan_seri_gun ve daily_tracks'ten al]

📌 Gunun Beklentileri
[kisa ozet, dikkat edilecekler]

⚠️ Yatirim tavsiyesi degildir.

📲 szalgo.net.tr

#BIST100 #borsa #bist #xauusd #altin #HalkaArz [+ konuya gore 2-3 ek: #SP500 #dolar #nasdaq #enflasyon — tekrar etme]"""

_EVENING_SYSTEM_PROMPT = """Sen SZ Algo Trade'in kidemli piyasa analisti yapay zekasisin. Her aksam piyasa kapandiktan sonra gun sonu degerlendirme raporu yaziyorsun.

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
10. Sonda mutlaka szalgo.net.tr linki ve hashtag'ler olmali
11. Haber kaynaklarini referans goster (Bloomberg HT, KAP vs.)
""" + _HALLUCINATION_GUARD + """
FORMAT:
📊 KAPANIŞ RAPORU — [gun_adi], [tarih]

🇹🇷 BIST 100 (XU100)
[kapanis, degisim, hacim degerlendirme]

🇺🇸 ABD Piyasalari
[S&P 500 ve Nasdaq verileri + gunduz akisi]

💰 Dolar & Altin
[USD/TRY ve altin kapanis]

📰 Gunun Onemli Gelismeleri
[RSS ve KAP haberlerinden en onemli 2-3 gelisme]

🏦 Halka Arz Takibi
[islemdeki IPO'lar — SADECE ceiling_tracks verisine dayanarak]
[Her hisse kodunu #TICKER formatiyla yaz, orn: #BESTE, #AKHAN]
[Tavan serisi bilgisini sadece tavan_seri_gun ve daily_tracks'ten al]

📌 Genel Degerlendirme
[gunun ozeti, onemli gelismeler]

⚠️ Yatirim tavsiyesi degildir.

📲 szalgo.net.tr

#BIST100 #borsa #bist #xauusd #altin #HalkaArz [+ konuya gore 2-3 ek: #SP500 #dolar #nasdaq #enflasyon — tekrar etme]"""


def _format_full_context(
    market_data: dict,
    ipos: list[dict],
    kap_news: list[dict],
    telegram_news: list[dict],
    rss_headlines: list[dict],
    report_type: str,
) -> str:
    """AI'a gonderilecek zengin veri kontekstini formatlar."""
    today = date.today()
    day_name = _TR_DAY_NAMES.get(today.weekday(), "Bilinmiyor")

    lines = [f"RAPOR TURU: {'Sabah Acilis' if report_type == 'morning' else 'Aksam Kapanis'}"]
    lines.append(f"TARIH: {today.isoformat()} ({day_name})")
    lines.append(f"HAFTANIN GUNU: {day_name} (haftanin {today.weekday() + 1}. is gunu)")
    lines.append("")

    # ── Piyasa Verileri ──
    lines.append("=" * 50)
    lines.append("PIYASA VERILERI (Yahoo Finance)")
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

    # ── RSS Haberleri (Dis Kaynak) ──
    lines.append("")
    lines.append("=" * 50)
    lines.append("DIS KAYNAK HABERLER (RSS)")
    lines.append("=" * 50)
    if rss_headlines:
        for h in rss_headlines:
            lines.append(f"  [{h['source']}] {h['title']}")
    else:
        lines.append("  Dis kaynak haber alinamadi.")

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

    return "\n".join(lines)


async def _generate_report(
    market_data: dict,
    ipos: list[dict],
    kap_news: list[dict],
    telegram_news: list[dict],
    rss_headlines: list[dict],
    report_type: str,
) -> str | None:
    """AI ile piyasa raporu uretir — zengin veri konteksti ile."""
    api_key = get_settings().ABACUS_API_KEY
    if not api_key:
        logger.error("Abacus API key yok — rapor uretilemedi")
        return None

    system_prompt = _MORNING_SYSTEM_PROMPT if report_type == "morning" else _EVENING_SYSTEM_PROMPT
    context = _format_full_context(market_data, ipos, kap_news, telegram_news, rss_headlines, report_type)

    user_message = (
        "Asagidaki GERCEK verileri kullanarak rapor yaz.\n"
        "SADECE verilen verilere dayan — hicbir bilgiyi UYDURMA.\n"
        "Halka arz tavan serisi bilgisini SADECE daily_tracks verilerinden oku.\n"
        "Haber kaynaklarini referans goster.\n\n"
        f"{context}"
    )

    logger.info("AI %s raporu icin kontekst: %d karakter, %d IPO, %d KAP, %d Telegram, %d RSS",
                report_type, len(context), len(ipos), len(kap_news), len(telegram_news), len(rss_headlines))

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
                    "temperature": 0.2,
                    "max_tokens": 1500,
                },
            )

            if resp.status_code != 200:
                logger.error("AI rapor hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
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
# 6) Tweet Gonderme
# ────────────────────────────────────────────

async def _collect_all_data() -> tuple:
    """Tum veri kaynaklarini paralel toplar."""
    import asyncio

    # Paralel calistiralim
    market_task = asyncio.create_task(fetch_market_snapshot())
    ipos_task = asyncio.create_task(get_active_ipos_performance())
    kap_task = asyncio.create_task(get_recent_kap_news(hours=24, limit=10))
    telegram_task = asyncio.create_task(get_recent_telegram_news(hours=24, limit=10))
    rss_task = asyncio.create_task(fetch_rss_headlines())

    market_data = await market_task
    ipos = await ipos_task
    kap_news = await kap_task
    telegram_news = await telegram_task
    rss_headlines = await rss_task

    return market_data, ipos, kap_news, telegram_news, rss_headlines


async def send_morning_report_tweet():
    """Sabah acilis raporu tweeti gonderir (08:15 TR)."""
    logger.info("Sabah acilis raporu hazirlaniyor — tum kaynaklar toplanıyor...")

    market_data, ipos, kap_news, telegram_news, rss_headlines = await _collect_all_data()

    report_text = await _generate_report(
        market_data, ipos, kap_news, telegram_news, rss_headlines, "morning"
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
    """Aksam kapanis raporu tweeti gonderir (20:45 TR)."""
    logger.info("Aksam kapanis raporu hazirlaniyor — tum kaynaklar toplanıyor...")

    market_data, ipos, kap_news, telegram_news, rss_headlines = await _collect_all_data()

    report_text = await _generate_report(
        market_data, ipos, kap_news, telegram_news, rss_headlines, "evening"
    )
    if not report_text:
        logger.error("Aksam raporu uretilemedi — tweet atilmadi")
        return

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
