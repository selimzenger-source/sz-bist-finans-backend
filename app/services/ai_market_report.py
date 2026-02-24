"""AI Piyasa Raporu Servisi — Sabah Acilis + Aksam Kapanis Raporu

Sabah 08:15 TR: Onceki gun verileri + bugunun beklentileri
Aksam 20:45 TR: Gunun kapanis verileri + degerlendirme
Her ikisi de X (Twitter) uzerinden gorsel + metin tweet olarak paylasilir.

v2: Gercek ceiling_tracks verisi + gun adi + halusinasyon korumasi
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
_AI_TIMEOUT = 45

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

# Turkce gun adlari (weekday() 0=Pazartesi)
_TR_DAY_NAMES = {
    0: "Pazartesi",
    1: "Sali",
    2: "Carsamba",
    3: "Persembe",
    4: "Cuma",
    5: "Cumartesi",
    6: "Pazar",
}

# Turkce durum aciklamalari
_DURUM_MAP = {
    "tavan": "Tavan (gun boyu tavanda kaldi)",
    "alici_kapatti": "Alici kapatti (tavana yakin pozitif kapanis)",
    "not_kapatti": "Notr kapanis (yatay veya hafif degisim)",
    "satici_kapatti": "Satici kapatti (negatif kapanis, baskili)",
    "taban": "Taban (gun boyu tabanda kaldi)",
}


# ────────────────────────────────────────────
# Piyasa Verisi Cekme (Yahoo Finance)
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
                # Halka arz fiyatindan toplam degisim %
                pct_from_ipo = None
                last_close = None
                if ipo.ceiling_tracks:
                    # Son ceiling_track'ten guncel kapanis fiyati
                    sorted_tracks = sorted(ipo.ceiling_tracks, key=lambda t: t.trading_day)
                    last_track = sorted_tracks[-1]
                    last_close = float(last_track.close_price) if last_track.close_price else None

                if ipo.ipo_price and last_close:
                    pct_from_ipo = round(
                        (last_close - float(ipo.ipo_price)) / float(ipo.ipo_price) * 100, 1
                    )
                elif ipo.ipo_price and ipo.first_day_close_price:
                    pct_from_ipo = round(
                        float((ipo.first_day_close_price - ipo.ipo_price) / ipo.ipo_price * 100), 1
                    )

                # Tavan serisi hesapla — baslangictan kac gun ust uste tavan yapti
                tavan_seri = 0
                if ipo.ceiling_tracks:
                    sorted_tracks = sorted(ipo.ceiling_tracks, key=lambda t: t.trading_day)
                    for t in sorted_tracks:
                        if t.durum == "tavan" or t.hit_ceiling:
                            tavan_seri += 1
                        else:
                            break  # Ilk tavan olmayan gunde dur

                # Gunluk ceiling_tracks detayi — AI'in gercek veriyle rapor yazmasi icin
                daily_tracks = []
                if ipo.ceiling_tracks:
                    sorted_tracks = sorted(ipo.ceiling_tracks, key=lambda t: t.trading_day)
                    for t in sorted_tracks:
                        durum_label = _DURUM_MAP.get(t.durum, t.durum)
                        daily_tracks.append({
                            "gun": t.trading_day,
                            "tarih": t.trade_date.isoformat() if t.trade_date else None,
                            "acilis": float(t.open_price) if t.open_price else None,
                            "kapanis": float(t.close_price) if t.close_price else None,
                            "en_yuksek": float(t.high_price) if t.high_price else None,
                            "en_dusuk": float(t.low_price) if t.low_price else None,
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
# AI Rapor Uretme
# ────────────────────────────────────────────

_HALLUCINATION_GUARD = """
⚠️ KRITIK KURAL — HALUSINASYON YASAGI:
- SADECE asagida verilen verileri kullan. Hicbir bilgiyi UYDURMA.
- Bir veri yoksa "veri bulunamadi" yaz, tahminde bulunma.
- Halka arz tavan/taban verisi ceiling_tracks olarak gun gun verilmistir.
  Eger bir hisse icin ceiling_tracks verisi yoksa, o hisse hakkinda tavan serisi/bozulma yorumu YAPMA.
- Tavan serisi suresini SADECE verilen "tavan_seri_gun" ve "daily_tracks" verilerinden oku.
  Islem gunu sayisi (trading_day_count) tavan serisi demek DEGILDIR!
- Haftanin hangi gunu oldugu TARIH satirinda yazilidir, buna uy. Haftanin sonu/basi yorumunu buna gore yap.
- Rakamlari yuvarlarken virgulden sonra max 2 basamak kullan.
"""

_MORNING_SYSTEM_PROMPT = """Sen SZ Algo Trade'in kidemli piyasa analisti yapay zekasisin. Her sabah piyasa acilmadan once yatirimcilara profesyonel, detayli ve dogru rapor yaziyorsun.

KURAL:
1. Turkce yaz, profesyonel analist uslubunda — samimi ama ciddi
2. Emoji kullan ama asiri degil (her bolumde 1-2)
3. Rakamlar ve yuzde degisimler net olsun
4. Halka arz performanslarini da dahil et (varsa)
5. Hic yatirim tavsiyesi VERME — "yatirim tavsiyesi degildir" notu ekle
6. Tweet formati: max 3800 karakter (gorsel ile 4000 limiti var)
7. Yapilandirilmis format kullan: basliklar ve maddeler ile
8. Rapor EN AZ 150 kelime olmali — detayli ve icerikli yaz, cok kisa tutma
9. Sonda mutlaka szalgo.net.tr linki ve hashtag'ler olmali
""" + _HALLUCINATION_GUARD + """
FORMAT:
📊 AÇILIŞ RAPORU — [gun_adi], [tarih]

🇹🇷 BIST 100 (XU100)
[onceki kapanis, degisim, analiz]

🇺🇸 ABD Piyasalari
[S&P 500 ve Nasdaq verisi + analiz]

💰 Dolar & Altin
[USD/TRY ve altin verisi]

🏦 Halka Arz Takibi
[islemdeki IPO'lar — SADECE verilen ceiling_tracks verisine dayanarak yaz]

📌 Gunun Beklentileri
[kisa ozet, dikkat edilecekler]

⚠️ Yatirim tavsiyesi degildir.

📲 szalgo.net.tr

#BIST100 #xauusd #altin #DowJones [+ konuya gore 2-3 ek hashtag: #HalkaArz #SP500 #dolar #nasdaq #enflasyon gibi — tekrar etme, hep farkli sec]"""

_EVENING_SYSTEM_PROMPT = """Sen SZ Algo Trade'in kidemli piyasa analisti yapay zekasisin. Her aksam piyasa kapandiktan sonra gun sonu degerlendirme raporu yaziyorsun.

KURAL:
1. Turkce yaz, profesyonel analist uslubunda — samimi ama ciddi
2. Emoji kullan ama asiri degil (her bolumde 1-2)
3. Rakamlar ve yuzde degisimler net olsun
4. Halka arz performanslarini da dahil et (varsa)
5. Hic yatirim tavsiyesi VERME — "yatirim tavsiyesi degildir" notu ekle
6. Tweet formati: max 3800 karakter (gorsel ile 4000 limiti var)
7. Yapilandirilmis format kullan: basliklar ve maddeler ile
8. Rapor EN AZ 150 kelime olmali — detayli ve icerikli yaz, cok kisa tutma
9. Sonda mutlaka szalgo.net.tr linki ve hashtag'ler olmali
""" + _HALLUCINATION_GUARD + """
FORMAT:
📊 KAPANIŞ RAPORU — [gun_adi], [tarih]

🇹🇷 BIST 100 (XU100)
[kapanis, degisim, hacim degerlendirme]

🇺🇸 ABD Piyasalari
[S&P 500 ve Nasdaq verileri + gunduz akisi]

💰 Dolar & Altin
[USD/TRY ve altin kapanis]

🏦 Halka Arz Takibi
[islemdeki IPO'lar — SADECE verilen ceiling_tracks verisine dayanarak yaz]

📌 Genel Degerlendirme
[gunun ozeti, onemli gelismeler]

⚠️ Yatirim tavsiyesi degildir.

📲 szalgo.net.tr

#BIST100 #xauusd #altin #DowJones [+ konuya gore 2-3 ek hashtag: #HalkaArz #SP500 #dolar #nasdaq #enflasyon gibi — tekrar etme, hep farkli sec]"""


def _format_market_context(market_data: dict, ipos: list[dict], report_type: str) -> str:
    """AI'a gonderilecek piyasa veri ozetini formatlar — zengin veri ile."""
    today = date.today()
    day_name = _TR_DAY_NAMES.get(today.weekday(), "Bilinmiyor")

    lines = [f"RAPOR TURU: {'Sabah Acilis' if report_type == 'morning' else 'Aksam Kapanis'}"]
    lines.append(f"TARIH: {today.isoformat()} ({day_name})")
    lines.append(f"HAFTANIN GUNU: {day_name} (haftanin {today.weekday() + 1}. is gunu)")
    lines.append("")

    # Piyasa verileri
    lines.append("=== PIYASA VERILERI ===")
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

    # Halka arz verileri — detayli ceiling_tracks ile
    lines.append("")
    lines.append("=== ISLEMDEKI HALKA ARZLAR ===")

    if ipos:
        for ipo in ipos:
            lines.append("")
            ticker = ipo["ticker"] or "?"
            lines.append(f"--- {ticker} ({ipo['company']}) ---")
            lines.append(f"  Halka arz fiyati: {ipo['ipo_price']} TL")
            lines.append(f"  Bugun {ipo['trading_day_count']}. islem gunu")
            lines.append(f"  Son kapanis fiyati: {ipo.get('last_close_price', 'Bilinmiyor')} TL")

            if ipo["pct_from_ipo_price"] is not None:
                lines.append(f"  Halka arz fiyatindan toplam degisim: %{ipo['pct_from_ipo_price']}")

            if ipo.get("high_from_start"):
                lines.append(f"  Baslangictan en yuksek fiyat: {ipo['high_from_start']} TL")

            # Tavan serisi bilgisi
            tavan_seri = ipo.get("tavan_seri_gun", 0)
            ceiling_broken = ipo.get("ceiling_broken", False)
            if tavan_seri > 0:
                if ceiling_broken:
                    lines.append(f"  Tavan serisi: {tavan_seri} gun tavan yapti, sonra tavan BOZULDU")
                    if ipo.get("ceiling_broken_at"):
                        lines.append(f"  Tavan bozulma zamani: {ipo['ceiling_broken_at']}")
                else:
                    lines.append(f"  Tavan serisi: {tavan_seri} gun ust uste tavan (hala devam)")
            else:
                lines.append("  Tavan serisi: Yok (hic tavan yapmadi)")

            # Gunluk islem detayi
            daily = ipo.get("daily_tracks", [])
            if daily:
                lines.append(f"  Gunluk islem detayi ({len(daily)} gun):")
                for d in daily:
                    pct = f", %{d['pct_degisim']}" if d.get("pct_degisim") is not None else ""
                    lines.append(
                        f"    {d['gun']}. gun ({d.get('tarih', '?')}): "
                        f"{d['durum_aciklama']}"
                        f" — Kapanis: {d.get('kapanis', '?')} TL{pct}"
                    )
            else:
                lines.append("  Gunluk islem detayi: Veri yok")
    else:
        lines.append("Su an 25 gun altinda islem goren halka arz yok.")

    return "\n".join(lines)


async def _generate_report(market_data: dict, ipos: list[dict], report_type: str) -> str | None:
    """AI ile piyasa raporu uretir."""
    api_key = get_settings().ABACUS_API_KEY
    if not api_key:
        logger.error("Abacus API key yok — rapor uretilemedi")
        return None

    system_prompt = _MORNING_SYSTEM_PROMPT if report_type == "morning" else _EVENING_SYSTEM_PROMPT
    context = _format_market_context(market_data, ipos, report_type)

    user_message = (
        "Asagidaki GERCEK piyasa verilerini kullanarak rapor yaz. "
        "SADECE verilen verilere dayan, hicbir bilgiyi UYDURMA. "
        "Halka arz tavan serisi bilgisini SADECE ceiling_tracks verilerinden oku.\n\n"
        f"{context}"
    )

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
                    "temperature": 0.2,  # Dusuk temp = daha dogru, daha az halusinasyon
                    "max_tokens": 1500,
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
    """Sabah acilis raporu tweeti gonderir (08:15 TR)."""
    logger.info("Sabah acilis raporu hazirlaniyor...")

    market_data = await fetch_market_snapshot()
    ipos = await get_active_ipos_performance()

    report_text = await _generate_report(market_data, ipos, "morning")
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
    logger.info("Aksam kapanis raporu hazirlaniyor...")

    market_data = await fetch_market_snapshot()
    ipos = await get_active_ipos_performance()

    report_text = await _generate_report(market_data, ipos, "evening")
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
