"""Haber tarama servisi — RSS + Gemini AI analiz + dedup.

Local Twitter bot'taki news_scanner.py'nin Render backend versiyonu.
10 dakikada bir RSS kaynaklarini tarar, AI ile puanlar,
onemli haberleri Telegram'a gonderir, onay sonrasi tweet atar.
"""

import asyncio
import base64
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import feedparser

from app.config import get_settings
from app.services.news_cover_generator import generate_news_cover

logger = logging.getLogger(__name__)

_TR_TZ = timezone(timedelta(hours=3))

# ── RSS Kaynaklari ──────────────────────────────────────
# Genel ekonomi/piyasa kaynaklari
RSS_FEEDS = [
    ("Bloomberg HT", "https://www.bloomberght.com/rss"),
    ("Dunya", "https://www.dunya.com/rss"),
    ("Bigpara", "https://bigpara.hurriyet.com.tr/rss/"),
    ("Para Analiz", "https://www.paraanaliz.com/feed/"),
    ("Ekonomim", "https://www.ekonomim.com/rss"),
    ("Investing.com", "https://tr.investing.com/rss/news_25.rss"),
    ("Haberturk", "https://www.haberturk.com/rss/ekonomi.xml"),
    ("AA Ekonomi", "https://www.aa.com.tr/tr/rss/default?cat=ekonomi"),
    # Sirket odakli / borsa haberleri
    ("Finans Gundem", "https://www.finansgundem.com/rss"),
    ("Sozcu Ekonomi", "https://www.sozcu.com.tr/rss/ekonomi.xml"),
    ("Hurriyet Ekonomi", "https://www.hurriyet.com.tr/rss/ekonomi"),
    ("NTV Ekonomi", "https://www.ntv.com.tr/ekonomi.rss"),
    ("Milliyet Ekonomi", "https://www.milliyet.com.tr/rss/rssnew/ekonomiall.xml"),
]

# ── Sabitler ────────────────────────────────────────────
_MIN_IMPORTANCE_SCORE = 8.5
_MAX_DAILY_LOCAL_TWEETS = 5
_MAX_DAILY_GLOBAL_TWEETS = 2

# Kategori bazli tweet aralik limitleri (dakika)
_CATEGORY_COOLDOWNS = {
    "TURKIYE_GUNDEM": 120,
    "GLOBAL": 180,
}
_DEFAULT_COOLDOWN = 40

# ── Dedup State (memory-based, Render restart'ta sifirlanir) ──
_seen_url_hashes: set[str] = set()
_seen_topic_hashes: dict[str, datetime] = {}  # topic_hash -> last_seen
_recent_titles: list[tuple[str, datetime]] = []  # (title, time)
_daily_counts: dict[str, int] = {}  # category -> count
_daily_counts_date: str = ""  # YYYY-MM-DD
_last_tweet_times: dict[str, datetime] = {}  # category -> last_tweet_time

# ── Pending news — FIFO kuyruk (max 5 haber) ──
_MAX_QUEUE_SIZE = 5
_pending_news: list[dict] = []  # En yeni basta, en eski sonda

# ── Sektor → Hisse Mapping ──────────────────────────────
_SECTOR_STOCKS = {
    "ILAC": ["ECILC", "DEVA", "TRILC", "SELEC", "GENIL", "RTALB"],
    "ENERJI": ["TUPRS", "AYGAZ", "AKSEN", "ODAS", "ZOREN", "AKSA"],
    "BANKA": ["GARAN", "AKBNK", "YKBNK", "ISCTR", "VAKBN", "HALKB"],
    "OTOMOTIV": ["TOASO", "FROTO", "DOAS", "OTKAR", "TTRAK", "ASUZU"],
    "PERAKENDE": ["BIMAS", "MGROS", "SOKM", "MAVI", "VAKKO", "BIZIM"],
    "TEKNOLOJI": ["ASELS", "LOGO", "INDES", "PAPIL", "ARDYZ", "SMART"],
    "INSAAT": ["ENKAI", "KOLIN", "TKFEN", "YEOTK", "SANEL", "EDIP"],
    "DEMIR_CELIK": ["EREGL", "KRDMD", "KARSN", "BRSAN", "CELHA", "IZMDC"],
    "HOLDING": ["SAHOL", "KCHOL", "TAVHL", "DOHOL", "KOZAL", "NETAS"],
    "SIGORTA": ["AKGRT", "ANHYT", "TURSG", "GUSGR", "RAYSG", "ANSGR"],
    "GIDA": ["ULKER", "BANVT", "TATGD", "CCOLA", "PETUN", "KERVT"],
    "TELEKOM": ["TCELL", "TTKOM", "NETAS"],
    "HAVACILIK": ["THYAO", "PGSUS", "TAVHL"],
    "MADENCILIK": ["KOZAL", "IPEKE", "KOZAA"],
}

# Kategori → emoji + etiket
_CATEGORY_PREFIX = {
    "HALKA_ARZ": "HALKA ARZ",
    "TURKIYE_GUNDEM": "SEKTOR & DUZENLEME",
    "SIRKET_HABERI": "SIRKET HABERI",
    "PIYASA": "PIYASA",
    "GLOBAL": "GLOBAL GUNDEM",
    "SEKTOR": "SEKTOR",
}

_CATEGORY_HASHTAGS = {
    "HALKA_ARZ": "#HalkaArz #Borsa #BIST100",
    "TURKIYE_GUNDEM": "#Borsa #BIST100 #Ekonomi",
    "SIRKET_HABERI": "#Borsa #BIST100 #Hisse",
    "PIYASA": "#Borsa #BIST100 #Piyasa",
    "GLOBAL": "#SonDakika #Dunya #Piyasa",
    "SEKTOR": "#Borsa #BIST100 #Sektor",
}

_TWEET_EMOJI = {
    "SON_DAKIKA": "\U0001f534 SON DAKIKA",
    "HALKA_ARZ": "\U0001f4ca HALKA ARZ",
    "SIRKET_HABERI": "\U0001f3e2 OZEL HABER",
    "TURKIYE_GUNDEM": "\U0001f4e2 GUNDEM",
    "PIYASA": "\U0001f4c8 PIYASA",
    "GLOBAL": "\U0001f30d GLOBAL GUNDEM",
    "SEKTOR": "\U0001f3ed SEKTOR",
}

# Store linkleri
_ANDROID_LINK = "https://play.google.com/store/apps/details?id=com.bistfinans.app"
_IOS_LINK = "https://apps.apple.com/tr/app/borsa-cebimde-haber-arz/id6760570446?l=tr"
_WEB_LINK = "https://borsacebimde.app/"


def _hash_url(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:16]


def _hash_topic(topic: str) -> str:
    return hashlib.md5(topic.strip().lower().encode()).hexdigest()[:16]


def _jaccard_similarity(a: str, b: str) -> float:
    """Iki baslik arasindaki Jaccard benzerlik skoru."""
    wa = set(a.lower().split())
    wb = set(b.lower().split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def _reset_daily_if_needed():
    """Gun degismisse sayaclari sifirla."""
    global _daily_counts, _daily_counts_date
    today = datetime.now(_TR_TZ).strftime("%Y-%m-%d")
    if _daily_counts_date != today:
        _daily_counts = {}
        _daily_counts_date = today


def _is_on_cooldown(category: str) -> bool:
    """Kategori bazli tweet cooldown kontrolu."""
    last = _last_tweet_times.get(category)
    if not last:
        return False
    cooldown_min = _CATEGORY_COOLDOWNS.get(category, _DEFAULT_COOLDOWN)
    return (datetime.now(_TR_TZ) - last).total_seconds() < cooldown_min * 60


def _is_similar_to_recent(title: str, threshold: float = 0.5) -> bool:
    """Son 24 saatteki basliklarla benzerlik kontrolu."""
    cutoff = datetime.now(_TR_TZ) - timedelta(hours=24)
    for recent_title, ts in _recent_titles:
        if ts < cutoff:
            continue
        if _jaccard_similarity(title, recent_title) >= threshold:
            return True
    return False


# ── RSS Parsing ─────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


async def _fetch_rss_entries(source_name: str, url: str) -> list[dict]:
    """Tek bir RSS kaynagindan son 25 dakikadaki haberleri al."""
    try:
        async with httpx.AsyncClient(timeout=15, headers=HEADERS, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=25)

        entries = []
        for entry in feed.entries[:20]:
            # Tarih kontrolu
            pub_date = None
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            elif hasattr(entry, "updated_parsed") and entry.updated_parsed:
                pub_date = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)

            if pub_date and pub_date < cutoff:
                continue

            title = getattr(entry, "title", "").strip()
            link = getattr(entry, "link", "").strip()
            summary = getattr(entry, "summary", "").strip()
            # HTML tag temizle
            summary = re.sub(r"<[^>]+>", "", summary).strip()

            # Cerez/cookie/veri politikasi metni RSS summary'ye siziyorsa temizle
            _junk_patterns = [
                r"çerez", r"cookie", r"veri politika", r"gizlilik politika",
                r"kişisel veri", r"kvkk", r"aydınlatma metni", r"çerez konumlandır",
            ]
            if summary and any(re.search(p, summary, re.IGNORECASE) for p in _junk_patterns):
                logger.info("RSS junk summary temizlendi (%s): %s", source_name, title[:60])
                summary = ""

            if not title or not link:
                continue

            entries.append({
                "title": title,
                "link": link,
                "summary": summary[:500],
                "source": source_name,
                "published": pub_date,
            })

        return entries
    except Exception as e:
        logger.warning("RSS fetch hatasi (%s): %s", source_name, e)
        return []


async def _fetch_all_rss() -> list[dict]:
    """Tum RSS kaynaklarini paralel tara."""
    tasks = [_fetch_rss_entries(name, url) for name, url in RSS_FEEDS]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_entries = []
    for result in results:
        if isinstance(result, list):
            all_entries.extend(result)

    return all_entries


# ── Gemini AI Analiz ────────────────────────────────────

_IMPORTANCE_PROMPT = """Sen bir finans haberi analizcisisin. Haberin BIST borsa yatirimcilari icin onemini 1-10 arasi puanla.

PUANLAMA KRITERLERI:
- 9-10: BIST sirket haberi (kar/zarar, ortaklik, satin alma, halka arz), TCMB faiz karari, buyuk regülasyon
- 8-9: Sektor haberleri (enerji, banka, otomotiv vb.), Turkiye ekonomisi (enflasyon, buyume, ihracat), sert kur hareketi
- 8+: Global flas (savas, surpriz faiz, finansal kriz) — gunde max 1
- 5-7: Orta onem, rutin kararlar, kucuk sirket haberleri, genel ekonomi
- 1-4: Kapsam disi (spor, magazin, yabanci sirket, rutin diplomasi, genel siyaset)

ONCELIK: Sirket haberleri ve Turkiye ekonomisi haberleri EN YUKSEK oncelikli.
Global haberler sadece piyasayi dogrudan etkileyecekse yuksek puan al.

DIKKAT: Eger ozet kismi cerez politikasi, gizlilik politikasi, KVKK veya site kullanim kosullari ile ilgiliyse
bu bir HABER DEGILDIR — PUAN 0 ver. RSS feed bazen site yasal metinlerini icerir, bunlari yoksay.

Asagidaki haberi puanla:
Baslik: {title}
Ozet: {summary}
Kaynak: {source}

SADECE asagidaki formatta cevap ver:
PUAN: [1-10]
KONU: [2-4 kelimelik konu etiketi, ayni olay icin ayni etiket kullan]
KATEGORI: [HALKA_ARZ|TURKIYE_GUNDEM|SIRKET_HABERI|PIYASA|GLOBAL|SEKTOR]
BANNER: [SON_DAKIKA|HALKA_ARZ|SIRKET_HABERI|SEKTOR|GLOBAL|PIYASA|TURKIYE_GUNDEM]
SEKTOR: [BANKA|ENERJI|ILAC|OTOMOTIV|PERAKENDE|TEKNOLOJI|INSAAT|DEMIR_CELIK|HOLDING|SIGORTA|GIDA|TELEKOM|HAVACILIK|MADENCILIK|YOK]"""


async def _ai_evaluate(news: dict) -> dict | None:
    """Gemini ile haberi puanla."""
    settings = get_settings()
    api_key = settings.GEMINI_API_KEY
    if not api_key:
        logger.warning("GEMINI_API_KEY yok, haber puanlama atlaniyor")
        return None

    prompt = _IMPORTANCE_PROMPT.format(
        title=news["title"],
        summary=news.get("summary", "")[:300],
        source=news["source"],
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gemini-2.5-flash",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0.1,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        text = data["choices"][0]["message"]["content"].strip()

        # Parse response
        score_match = re.search(r"PUAN:\s*(\d+(?:\.\d+)?)", text)
        topic_match = re.search(r"KONU:\s*(.+)", text)
        cat_match = re.search(r"KATEGORI:\s*(\w+)", text)
        banner_match = re.search(r"BANNER:\s*(\w+)", text)
        sector_match = re.search(r"SEKTOR:\s*(\w+)", text)

        if not score_match:
            return None

        return {
            "score": float(score_match.group(1)),
            "topic": topic_match.group(1).strip() if topic_match else "",
            "category": cat_match.group(1).strip() if cat_match else "PIYASA",
            "banner": banner_match.group(1).strip() if banner_match else None,
            "sector": sector_match.group(1).strip() if sector_match else "YOK",
        }
    except Exception as e:
        logger.error("AI haber puanlama hatasi: %s", e)
        return None


# ── Tweet Metin + Kapak Resmi ───────────────────────────

_TWEET_PROMPT = """Sen bir BIST borsa analizcisi ve finans haber editorusun. Asagidaki haberi Turk borsasi yatirimcilari icin YORUMLAYARAK tweet haline getir.

Haber:
Baslik: {title}
Ozet: {summary}
Kaynak: {source}
Kategori: {category}
Sektor: {sector}

KURALLAR:
- BASLIK: Max 50 karakter, buyuk harf, ! ile bitir. Clickbait OLMASIN. Haberin ozunu yansitsin.
- OZET: 5-7 cumle. Sadece ozetleme — YORUM KAT, BAGLAM KUR:
  * Haberin BIST'teki hangi sektoru/sirketleri etkileyecegini yaz
  * "Bu gelisme ... sektorunu olumlu/olumsuz etkileyebilir" gibi yorumlar ekle
  * Sirket haberi ise: sirketin BIST performansi, sektor pozisyonu hakkinda kisa baglam ver
  * Turkiye ekonomisi haberi ise: TCMB, faiz, enflasyon, kur etkisi baglami kur
  * Sektor haberi ise: sektordeki BIST sirketlerini (THYAO, GARAN vb.) yorumla
  * ISTISNA: Cok buyuk global olaylar (savas, deprem, finansal kriz) icin sadece ozetle, sektor baglami zorlama
  ONEMLI: Cerez politikasi, gizlilik metni, site kullanim kosullari ASLA tweet icerigine yazilmaz. Bunlar haber degildir.
- SIRKETLER: SADECE haberde DOGRUDAN bahsedilen BIST hisse kodlarini yaz (orn: THYAO, GARAN, EREGL).
  Haberde sirket gecmiyorsa ama sektor belliyse, o sektordeki en buyuk 2-3 BIST sirketini yaz.
  Sirket adini biliyorsan BIST ticker koduna cevir (orn: Turk Hava Yollari → THYAO).

Format:
BASLIK: [baslik]
OZET: [ozet]
SIRKETLER: [sirketler]"""


async def _generate_tweet_content(news: dict, ai_result: dict) -> dict | None:
    """Gemini ile tweet metni + kapak resmi olustur."""
    settings = get_settings()
    api_key = settings.GEMINI_API_KEY
    if not api_key:
        return None

    prompt = _TWEET_PROMPT.format(
        title=news["title"],
        summary=news.get("summary", "")[:500],
        source=news["source"],
        category=ai_result["category"],
        sector=ai_result.get("sector", "YOK"),
    )

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gemini-2.5-flash",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 800,
                    "temperature": 0.3,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        text = data["choices"][0]["message"]["content"].strip()

        baslik_match = re.search(r"BASLIK:\s*(.+)", text)
        ozet_match = re.search(r"OZET:\s*(.+)", text, re.DOTALL)
        sirket_match = re.search(r"SIRKETLER:\s*(.+)", text)

        baslik = baslik_match.group(1).strip() if baslik_match else news["title"][:50]
        ozet = ozet_match.group(1).strip() if ozet_match else news.get("summary", "")[:300]

        # SIRKETLER satırından sonraki kısmı OZET'ten temizle
        if sirket_match and ozet_match:
            sirket_pos = text.find("SIRKETLER:")
            ozet_pos = text.find("OZET:")
            if sirket_pos > ozet_pos:
                ozet_raw = text[ozet_pos + 5:sirket_pos].strip()
                ozet = ozet_raw

        # Sirket hashtag'leri
        sirketler = ""
        if sirket_match:
            sirketler = sirket_match.group(1).strip()

        ticker_tags = []
        if sirketler and sirketler.upper() != "YOK":
            for s in sirketler.split(","):
                s = s.strip().upper()
                if s and len(s) <= 6 and s.isalpha():
                    ticker_tags.append(f"#{s}")

        # Sektor bazli hisse ekle
        sector = ai_result.get("sector", "YOK")
        if sector != "YOK" and sector in _SECTOR_STOCKS:
            for t in _SECTOR_STOCKS[sector][:3]:
                tag = f"#{t}"
                if tag not in ticker_tags:
                    ticker_tags.append(tag)

        category = ai_result["category"]
        banner = ai_result.get("banner") or category
        prefix = _TWEET_EMOJI.get(banner, _TWEET_EMOJI.get(category, "\U0001f4f0 HABER"))

        # Tweet metni
        hashtags = _CATEGORY_HASHTAGS.get(category, "#Borsa #BIST100")
        ticker_str = " ".join(ticker_tags[:6])

        tweet_text = (
            f"{prefix}\n\n"
            f"{ozet}\n\n"
            f"Kaynak: {news['source']}\n\n"
            f"\U0001f4f2 Android: {_ANDROID_LINK}\n"
            f"\U0001f34f iOS: {_IOS_LINK}\n"
            f"\U0001f310 Web: {_WEB_LINK}\n\n"
            f"{hashtags}"
        )
        if ticker_str:
            tweet_text += f" {ticker_str}"

        # Kapak resmi
        cover_path = await generate_news_cover(
            headline=baslik,
            category=category,
            source=news["source"],
            banner=banner,
        )

        return {
            "tweet_text": tweet_text,
            "cover_path": cover_path,
            "headline": baslik,
            "category": category,
            "banner": banner,
            "source": news["source"],
            "source_url": news["link"],
            "score": ai_result["score"],
        }
    except Exception as e:
        logger.error("Tweet icerik olusturma hatasi: %s", e)
        return None


# ── Telegram Onay Mekanizmasi ───────────────────────────

async def _send_telegram_photo(image_path: str, caption: str) -> bool:
    """Telegram'a foto gonder."""
    settings = get_settings()
    bot_token = settings.ADMIN_TELEGRAM_BOT_TOKEN or settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.ADMIN_TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        logger.warning("Telegram yapilandirilmamis")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            with open(image_path, "rb") as f:
                resp = await client.post(
                    url,
                    data={"chat_id": chat_id, "caption": caption[:1024], "parse_mode": "HTML"},
                    files={"photo": ("cover.png", f, "image/png")},
                )
            return resp.status_code == 200
    except Exception as e:
        logger.error("Telegram foto gonderim hatasi: %s", e)
        return False


async def _send_telegram_message(text: str, disable_preview: bool = False) -> bool:
    """Telegram'a mesaj gonder."""
    settings = get_settings()
    bot_token = settings.ADMIN_TELEGRAM_BOT_TOKEN or settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.ADMIN_TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload: dict = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if disable_preview:
        payload["disable_web_page_preview"] = True
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            return resp.status_code == 200
    except Exception as e:
        logger.error("Telegram mesaj hatasi: %s", e)
        return False


# ── Tweet Gonderim ──────────────────────────────────────

async def _post_tweet(tweet_data: dict) -> bool:
    """Tweet at — twitter_service uzerinden."""
    try:
        from app.services.twitter_service import _safe_tweet_with_media, _safe_tweet

        text = tweet_data["tweet_text"]
        cover_path = tweet_data.get("cover_path")

        if cover_path and os.path.exists(cover_path):
            success = _safe_tweet_with_media(text, cover_path, source="news_scanner")
        else:
            success = _safe_tweet(text, source="news_scanner")

        if success:
            logger.info("Haber tweeti atildi: %s", tweet_data.get("headline", "?")[:50])
        return success
    except Exception as e:
        logger.error("Tweet gonderim hatasi: %s", e)
        return False


# ── Cloudinary Upload ───────────────────────────────────

async def _upload_cover_image(image_path: str) -> str | None:
    """Kapak resmini Cloudinary'ye yukle, URL dondur.

    CLOUDINARY_URL env var set degilse, local /static/img/'ye kopyalar.
    """
    if not image_path or not os.path.exists(image_path):
        return None

    cloudinary_url = os.environ.get("CLOUDINARY_URL", "")

    if cloudinary_url:
        # Cloudinary upload
        try:
            # CLOUDINARY_URL format: cloudinary://api_key:api_secret@cloud_name
            import re as _re
            match = _re.match(
                r"cloudinary://(\d+):([^@]+)@(.+)", cloudinary_url
            )
            if not match:
                logger.error("CLOUDINARY_URL format hatasi")
                return None

            api_key, api_secret, cloud_name = match.groups()
            upload_url = f"https://api.cloudinary.com/v1_1/{cloud_name}/image/upload"

            with open(image_path, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()

            # Unsigned upload with upload_preset OR signed upload
            import hashlib as _hl
            timestamp = str(int(time.time()))
            folder = "news_covers"
            params_to_sign = f"folder={folder}&timestamp={timestamp}{api_secret}"
            signature = _hl.sha1(params_to_sign.encode()).hexdigest()

            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(upload_url, data={
                    "file": f"data:image/png;base64,{img_b64}",
                    "api_key": api_key,
                    "timestamp": timestamp,
                    "signature": signature,
                    "folder": folder,
                })
                resp.raise_for_status()
                data = resp.json()
                url = data.get("secure_url")
                logger.info("Cloudinary upload basarili: %s", url)
                return url

        except Exception as e:
            logger.error("Cloudinary upload hatasi: %s", e)
            # Fallback: local

    # Local fallback: /static/img/ altina kopyala
    try:
        static_dir = os.path.join(
            os.path.dirname(__file__), "..", "..", "static", "img"
        )
        os.makedirs(static_dir, exist_ok=True)
        filename = os.path.basename(image_path)
        dest = os.path.join(static_dir, filename)

        import shutil
        shutil.copy2(image_path, dest)
        logger.info("Kapak resmi local'e kopyalandi: %s", dest)
        return f"/static/img/{filename}"
    except Exception as e:
        logger.error("Local image kopyalama hatasi: %s", e)
        return None


# ── Ana Tarama Fonksiyonu ───────────────────────────────

async def scan_news() -> list[dict]:
    """RSS tarama + AI analiz + dedup. Onemli haberleri dondur.

    Returns:
        Onemli haberlerin listesi (score >= 8.5)
    """
    _reset_daily_if_needed()

    # 1. RSS fetch
    entries = await _fetch_all_rss()
    logger.info("RSS tarama: %d haber bulundu", len(entries))

    if not entries:
        return []

    # 2. URL dedup
    new_entries = []
    for entry in entries:
        url_hash = _hash_url(entry["link"])
        if url_hash not in _seen_url_hashes:
            new_entries.append(entry)

    if not new_entries:
        logger.debug("Yeni haber yok (tumu gorulmus)")
        return []

    logger.info("Yeni haber: %d (toplam %d)", len(new_entries), len(entries))

    # 3. AI analiz (max 20 haber, maliyet optimizasyonu)
    important = []
    for entry in new_entries[:20]:
        ai_result = await _ai_evaluate(entry)
        url_hash = _hash_url(entry["link"])
        _seen_url_hashes.add(url_hash)

        if not ai_result:
            continue

        score = ai_result["score"]
        if score < _MIN_IMPORTANCE_SCORE:
            continue

        # Topic dedup (12-24 saat)
        topic = ai_result.get("topic", "")
        if topic:
            topic_hash = _hash_topic(topic)
            category = ai_result["category"]
            dedup_hours = 24 if category == "GLOBAL" else 12
            cutoff = datetime.now(_TR_TZ) - timedelta(hours=dedup_hours)

            if topic_hash in _seen_topic_hashes and _seen_topic_hashes[topic_hash] > cutoff:
                logger.debug("Topic dedup: %s (%s)", topic, entry["title"][:40])
                continue
            _seen_topic_hashes[topic_hash] = datetime.now(_TR_TZ)

        # Title benzerlik dedup
        if _is_similar_to_recent(entry["title"]):
            logger.debug("Baslik benzerlik dedup: %s", entry["title"][:40])
            continue

        _recent_titles.append((entry["title"], datetime.now(_TR_TZ)))

        entry["ai"] = ai_result
        important.append(entry)

    # Skora gore sirala, max 3
    important.sort(key=lambda x: x["ai"]["score"], reverse=True)
    important = important[:3]

    if important:
        logger.info(
            "Onemli haber: %d [%s]",
            len(important),
            ", ".join(f"{n['title'][:30]}({n['ai']['score']})" for n in important),
        )

    return important


async def process_important_news(news_list: list[dict], auto_tweet: bool = False) -> list[dict]:
    """Onemli haberleri isle: tweet olustur, Telegram'a gonder veya otomatik tweet at.

    Args:
        news_list: scan_news() ciktisi
        auto_tweet: True ise direkt tweet at, False ise Telegram onaya gonder

    Returns:
        Islenen haber verilerinin listesi
    """
    global _pending_news

    processed = []
    for news in news_list:
        ai = news["ai"]
        category = ai["category"]

        # Gunluk limit kontrolu
        _reset_daily_if_needed()
        cat_key = "GLOBAL" if category == "GLOBAL" else "LOCAL"
        max_limit = _MAX_DAILY_GLOBAL_TWEETS if cat_key == "GLOBAL" else _MAX_DAILY_LOCAL_TWEETS
        if _daily_counts.get(cat_key, 0) >= max_limit:
            logger.info("Gunluk limit doldu (%s): %s", cat_key, news["title"][:40])
            continue

        # Cooldown kontrolu
        if _is_on_cooldown(category):
            logger.info("Cooldown: %s — %s", category, news["title"][:40])
            continue

        # Tweet icerik olustur
        tweet_data = await _generate_tweet_content(news, ai)
        if not tweet_data:
            continue

        # Kapak resmini yukle (Cloudinary veya local)
        cover_url = await _upload_cover_image(tweet_data.get("cover_path"))
        tweet_data["cover_url"] = cover_url

        if auto_tweet:
            # Direkt tweet at
            success = await _post_tweet(tweet_data)
            if success:
                _daily_counts[cat_key] = _daily_counts.get(cat_key, 0) + 1
                _last_tweet_times[category] = datetime.now(_TR_TZ)

                # DB'ye kaydet
                await _save_news_to_db(tweet_data)
        else:
            # FIFO kuyruga ekle (max 5, en eski duser)
            _pending_news.insert(0, tweet_data)
            if len(_pending_news) > _MAX_QUEUE_SIZE:
                dropped = _pending_news.pop()
                logger.info("Kuyruk dolu, en eski haber dustu: %s", dropped.get("headline", "?")[:40])
            # Telegram'a kuyruk bilgisi gonder
            await _send_news_to_telegram(tweet_data)

        processed.append(tweet_data)

        # Temizlik: gecici dosya
        cover_path = tweet_data.get("cover_path")
        if cover_path and os.path.exists(cover_path):
            try:
                os.unlink(cover_path)
            except Exception:
                pass

    return processed


async def _send_news_to_telegram(tweet_data: dict):
    """Haber onizlemesini Telegram'a gonder + kuyruk listesi."""
    headline = tweet_data.get("headline", "?")
    category = tweet_data.get("category", "?")
    score = tweet_data.get("score", 0)
    source = tweet_data.get("source", "?")
    text_len = len(tweet_data.get("tweet_text", ""))
    now_str = datetime.now(_TR_TZ).strftime("%d.%m.%Y %H:%M")

    caption = (
        f"<b>\U0001f4f0 Yeni Haber Kuyruga Eklendi</b>\n\n"
        f"<b>{headline}</b>\n"
        f"Kategori: {category} | Puan: {score}/10\n"
        f"Kaynak: {source} | {text_len} karakter\n"
        f"\U0001f4c5 {now_str}\n"
    )

    cover = tweet_data.get("cover_path")
    if cover and os.path.exists(cover):
        await _send_telegram_photo(cover, caption)
    else:
        await _send_telegram_message(caption)

    # Tweet metnini ayri mesaj — code blogu ile (link preview onlenir)
    tweet_text = tweet_data.get("tweet_text", "")
    if tweet_text:
        await _send_telegram_message(f"<code>{tweet_text[:4000]}</code>", disable_preview=True)

    # Kuyruk ozeti gonder
    await _send_queue_summary()


async def _send_queue_summary():
    """Mevcut kuyruk ozetini Telegram'a gonder."""
    if not _pending_news:
        return

    lines = [f"<b>\U0001f4cb Haber Kuyrugu ({len(_pending_news)}/{_MAX_QUEUE_SIZE})</b>\n"]
    for i, n in enumerate(_pending_news):
        emoji = {
            "GLOBAL": "\U0001f30d", "SIRKET_HABERI": "\U0001f3e2",
            "PIYASA": "\U0001f4c8", "HALKA_ARZ": "\U0001f4ca",
            "TURKIYE_GUNDEM": "\U0001f1f9\U0001f1f7", "SEKTOR": "\U0001f3ed",
        }.get(n.get("category", ""), "\U0001f4f0")
        score = n.get("score", 0)
        headline = n.get("headline", "?")[:45]
        lines.append(f"  {i+1}. {emoji} {headline} ({score})")

    lines.append("")
    lines.append("<b>Komutlar:</b>")
    lines.append("<code>/haber_at 1</code> — 1. haberi tweetle")
    lines.append("<code>/haber_at 3</code> — 3. haberi tweetle")
    lines.append("<code>/haber_sil 2</code> — 2. haberi kuyruktan cikar")
    lines.append("<code>/haber_liste</code> — kuyrugu goster")

    await _send_telegram_message("\n".join(lines))


async def _save_news_to_db(tweet_data: dict):
    """Tweet verisini pending_tweets tablosuna kaydet."""
    try:
        from app.database import async_session_maker
        from app.models.pending_tweet import PendingTweet

        async with async_session_maker() as session:
            pt = PendingTweet(
                text=tweet_data["tweet_text"],
                image_path=tweet_data.get("cover_url", ""),
                source="news_scanner",
                status="sent",
                sent_at=datetime.now(timezone.utc),
            )
            session.add(pt)
            await session.commit()
            logger.info("Haber DB'ye kaydedildi: %s", tweet_data.get("headline", "?")[:40])
    except Exception as e:
        logger.error("Haber DB kayit hatasi: %s", e)


# ── Onay/Red Fonksiyonlari (API'den cagrilir) ──────────

async def approve_news(index: int) -> dict:
    """Haberi tweetle. index 1-based (Telegram: /haber_at 1)."""
    global _pending_news

    idx = index - 1  # 1-based -> 0-based
    if idx < 0 or idx >= len(_pending_news):
        return {"error": f"Gecersiz numara: {index}, kuyrukta {len(_pending_news)} haber var"}

    tweet_data = _pending_news[idx]

    # Tweet at
    success = await _post_tweet(tweet_data)

    if success:
        category = tweet_data.get("category", "PIYASA")
        cat_key = "GLOBAL" if category == "GLOBAL" else "LOCAL"
        _daily_counts[cat_key] = _daily_counts.get(cat_key, 0) + 1
        _last_tweet_times[category] = datetime.now(_TR_TZ)

        # DB'ye kaydet
        await _save_news_to_db(tweet_data)

        # Push bildirim — onemli haber
        try:
            from app.database import async_session
            from app.services.notification import NotificationService
            headline = tweet_data.get("headline", "Önemli gelişme")
            async with async_session() as notif_session:
                notif_svc = NotificationService(notif_session)
                await notif_svc.notify_market_news(headline)
                await notif_session.commit()
            logger.info("Piyasa haberi push bildirim gonderildi: %s", headline[:50])
        except Exception as e:
            logger.error("Piyasa haberi push bildirim hatasi: %s", e)

        # Kuyruktan cikar
        _pending_news.pop(idx)

        await _send_telegram_message(
            f"\u2705 Haber tweeti atildi: {tweet_data.get('headline', '?')[:50]}"
        )

        # Geri kalan kuyrugu goster
        if _pending_news:
            await _send_queue_summary()

        return {"status": "ok", "headline": tweet_data.get("headline")}
    else:
        await _send_telegram_message(
            f"\u274c Tweet gonderilemedi: {tweet_data.get('headline', '?')[:50]}"
        )
        return {"error": "Tweet gonderilemedi"}


async def reject_news(index: int) -> dict:
    """Haberi kuyruktan cikar. index 1-based."""
    global _pending_news

    idx = index - 1
    if idx < 0 or idx >= len(_pending_news):
        return {"error": f"Gecersiz numara: {index}, kuyrukta {len(_pending_news)} haber var"}

    removed = _pending_news.pop(idx)
    await _send_telegram_message(
        f"\u274c Haber silindi: {removed.get('headline', '?')[:50]}"
    )

    if _pending_news:
        await _send_queue_summary()
    else:
        await _send_telegram_message("\U0001f4cb Kuyruk bos.")

    return {"status": "rejected", "headline": removed.get("headline")}


async def show_queue() -> dict:
    """Kuyruk ozetini Telegram'a gonder."""
    if not _pending_news:
        await _send_telegram_message("\U0001f4cb Kuyruk bos — bekleyen haber yok.")
        return {"queue": []}

    await _send_queue_summary()
    return {"queue": get_queue()}


def get_queue() -> list[dict]:
    """Bekleyen haber kuyruğunu döndür (1-based index)."""
    return [
        {
            "index": i + 1,  # 1-based
            "headline": n.get("headline", "?"),
            "category": n.get("category", "?"),
            "score": n.get("score", 0),
            "source": n.get("source", "?"),
        }
        for i, n in enumerate(_pending_news)
    ]


# ── Cleanup ─────────────────────────────────────────────

def cleanup_old_state():
    """Eski state verilerini temizle (gunde 1 cagrilir)."""
    global _recent_titles

    cutoff = datetime.now(_TR_TZ) - timedelta(hours=48)

    # Eski basliklari temizle
    _recent_titles = [(t, ts) for t, ts in _recent_titles if ts > cutoff]

    # Eski topic hash'leri temizle
    for key in list(_seen_topic_hashes.keys()):
        if _seen_topic_hashes[key] < cutoff:
            del _seen_topic_hashes[key]

    # URL hash limiti (5000'den fazlaysa eski yarisihi sil)
    if len(_seen_url_hashes) > 5000:
        # Set'ten deterministik silme yapamayiz, komple temizle
        # (Render restart'ta zaten sifirlanir)
        _seen_url_hashes.clear()
        logger.info("URL hash cache temizlendi (>5000)")

    logger.info(
        "State temizligi: %d baslik, %d topic, %d url",
        len(_recent_titles), len(_seen_topic_hashes), len(_seen_url_hashes),
    )


# ── Admin Telegram Komut Handler ───────────────────────
# /haber_at N, /haber_sil N, /haber_liste, /onay, /iptal komutlarini dinler

_admin_cmd_last_update_id: Optional[int] = None


async def poll_admin_commands():
    """Admin Telegram chat'ten komutlari okur ve isler.

    Scheduler tarafindan 5 saniyede bir cagrilir.
    Admin bot token ile getUpdates yapar.
    """
    global _admin_cmd_last_update_id

    settings = get_settings()
    bot_token = settings.ADMIN_TELEGRAM_BOT_TOKEN
    chat_id = settings.ADMIN_TELEGRAM_CHAT_ID

    if not bot_token or not chat_id:
        return

    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    params: dict = {"timeout": 0, "allowed_updates": ["message"]}
    if _admin_cmd_last_update_id is not None:
        params["offset"] = _admin_cmd_last_update_id

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, params=params)
            if resp.status_code != 200:
                return
            data = resp.json()
    except Exception as e:
        logger.error("Admin komut poll hatasi: %s", e)
        return

    updates = data.get("result", [])
    if not updates:
        return

    for update in updates:
        update_id = update.get("update_id", 0)
        _admin_cmd_last_update_id = update_id + 1

        message = update.get("message")
        if not message:
            continue

        msg_chat_id = str(message.get("chat", {}).get("id", ""))
        if msg_chat_id != str(chat_id):
            continue

        text = (message.get("text") or "").strip()
        if not text.startswith("/"):
            continue

        try:
            await _handle_admin_command(text)
        except Exception as e:
            logger.error("Admin komut isleme hatasi: %s — komut: %s", e, text)


async def _handle_admin_command(text: str):
    """Tek bir admin komutunu isler."""
    parts = text.split()
    cmd = parts[0].lower()

    if cmd == "/haber_at" or cmd == "/onay":
        if len(parts) < 2:
            await _send_telegram_message("⚠️ Kullanim: /haber_at <numara>")
            return
        try:
            index = int(parts[1])
        except ValueError:
            await _send_telegram_message("⚠️ Gecersiz numara")
            return
        result = await approve_news(index)
        if "error" in result:
            await _send_telegram_message(f"⚠️ {result['error']}")

    elif cmd == "/haber_sil" or cmd == "/iptal":
        if len(parts) < 2:
            await _send_telegram_message("⚠️ Kullanim: /haber_sil <numara>")
            return
        try:
            index = int(parts[1])
        except ValueError:
            await _send_telegram_message("⚠️ Gecersiz numara")
            return
        result = await reject_news(index)
        if "error" in result:
            await _send_telegram_message(f"⚠️ {result['error']}")

    elif cmd == "/haber_liste":
        await show_queue()

    # Bilinmeyen komutlari sessizce atla
