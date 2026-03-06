"""X (Twitter) Otomatik Tweet Servisi — @SZAlgoFinans

14 farkli tweet tipi ile halka arz ve KAP haberlerini X'e otomatik atar.
Mevcut sistemi ASLA bozmamalı — tum cagrılar try/except ile korunur.

Tweet Tipleri:
1.  Yeni Halka Arz (SPK onayi)
2.  Dagitima Cikis (in_distribution)
3.  Kesinlesen Dagitim Sonuclari
4.  Son 4 Saat Hatirlatma
5.  Son 30 Dakika Hatirlatma
6.  Ilk Islem Gunu (09:00 gong)
7.  Acilis Fiyati (09:56 sadece ilk islem gunu)
8.  Gunluk Takip (18:20 her islem gunu)
9.  25 Gün Performans Ozeti (25. gunde bir kez)
10. Yillik Halka Arz Ozeti (her ayin 1'i 20:00, ocak haric)
11. BIST 50 KAP Haberi (aninda)
12. Son Gun Sabah Tweeti (07:30 — hafif uyari tonu)
13. Sirket Tanitim Tweeti (ertesi gun 20:00 — izahname sonrasi)
14. SPK Bekleyenler Gorselli Tweet (her ayin 1'i — gorsel ile)
"""

import logging
import time
import hashlib
import hmac
import base64
import urllib.parse
import uuid
import json
from datetime import datetime, date
from zoneinfo import ZoneInfo

_TR_TZ = ZoneInfo("Europe/Istanbul")
from typing import Optional

import os
import tempfile
import httpx

import asyncio

logger = logging.getLogger(__name__)

# Twitter API v2 endpoint
_TWITTER_TWEET_URL = "https://api.twitter.com/2/tweets"

# ── Global Tweet Rate Limiter ──
# Dakikada max 3 tweet — ban riskini önler (reply + normal tweet dahil)
import threading

_TWEET_RATE_MAX = 3        # dakikada max tweet
_TWEET_RATE_WINDOW = 60    # saniye
_tweet_timestamps: list[float] = []
_tweet_rate_lock = threading.Lock()


def _check_tweet_rate_limit() -> bool:
    """Dakikada max 3 tweet kontrolü. True = gönderilebilir, False = beklemeli."""
    now = time.time()
    with _tweet_rate_lock:
        # Eski timestamp'leri temizle
        _tweet_timestamps[:] = [t for t in _tweet_timestamps if now - t < _TWEET_RATE_WINDOW]
        return len(_tweet_timestamps) < _TWEET_RATE_MAX


def _wait_for_tweet_rate_limit():
    """Rate limit doluysa en eski tweet'in süresinin dolmasını bekler."""
    now = time.time()
    with _tweet_rate_lock:
        _tweet_timestamps[:] = [t for t in _tweet_timestamps if now - t < _TWEET_RATE_WINDOW]
        if len(_tweet_timestamps) >= _TWEET_RATE_MAX:
            oldest = min(_tweet_timestamps)
            wait_secs = _TWEET_RATE_WINDOW - (now - oldest) + 1
            if wait_secs > 0:
                logger.info(f"Tweet rate limit: {wait_secs:.0f}sn bekleniyor (dakikada max {_TWEET_RATE_MAX})")
                return wait_secs
    return 0


def _record_tweet_sent():
    """Başarılı tweet gönderimini kaydet."""
    with _tweet_rate_lock:
        _tweet_timestamps.append(time.time())


def _queue_tweet(text: str, image_path: str | None = None, source: str = "unknown") -> bool:
    """Tweet'i kuyruğa ekler (DB'ye PendingTweet kaydeder).

    TWITTER_AUTO_SEND=False iken tum tweetler buraya yonlendirilir.
    Admin panelinden onaylaninca atilir.

    Sync DB (psycopg2) kullanir — async event loop icinden de guvenle cagrilabilir.
    """
    try:
        from app.config import get_settings
        from app.models.pending_tweet import PendingTweet

        settings = get_settings()
        db_url = str(settings.DATABASE_URL)

        # async URL'yi sync'e cevir
        sync_url = db_url.replace("postgresql+asyncpg://", "postgresql://").replace("postgres://", "postgresql://")

        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session

        engine = create_engine(sync_url, pool_pre_ping=True)
        with Session(engine) as db:
            tweet = PendingTweet(
                text=text,
                image_path=image_path,
                source=source,
                status="pending",
            )
            db.add(tweet)
            db.commit()
            logger.info("[TWEET-KUYRUK] Kuyruğa eklendi (%s): %s", source, text[:60])

        engine.dispose()
        return True
    except Exception as e:
        logger.error("[TWEET-KUYRUK] Kuyruğa eklenemedi: %s", e)
        return False

# Banner gorsel yollari — static/img/ altinda
_IMG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
BANNER_SPK_BEKLEYENLER = os.path.join(_IMG_DIR, "spk_bekleyenler_banner.png")
BANNER_SON_BASVURU_GUNU = os.path.join(_IMG_DIR, "son_basvuru_gunu_banner.png")
BANNER_SON_4_SAAT = os.path.join(_IMG_DIR, "son_4_saat_banner.png")
BANNER_HALKA_ARZ_HAKKINDA = os.path.join(_IMG_DIR, "halka_arz_hakkinda_banner.png")
BANNER_GUNLUK_TAKIP = os.path.join(_IMG_DIR, "gunluk_takip_banner.png")
BANNER_25_GUN_PERFORMANS = os.path.join(_IMG_DIR, "25_gun_performans_banner.png")
BANNER_DAGITIM_SONUCLARI = os.path.join(_IMG_DIR, "dagitim_sonuclari_banner.png")
BANNER_GONG_CALIYOR = os.path.join(_IMG_DIR, "gong_caliyor_banner.png")
BANNER_BASVURULAR_BASLIYOR = os.path.join(_IMG_DIR, "basvurular_basliyor_banner.png")
BANNER_ACILIS_FIYATI = os.path.join(_IMG_DIR, "acilis_fiyati_banner.png")
BANNER_SON_30_DAKIKA = os.path.join(_IMG_DIR, "son_30_dakika_banner.png")
BANNER_SPK_ONAYI = os.path.join(_IMG_DIR, "spk_onayi_banner.png")
BANNER_AY_SONU_RAPOR = os.path.join(_IMG_DIR, "ay_sonu_rapor_banner.png")
BANNER_OGLE_ARASI = os.path.join(_IMG_DIR, "ogle_arasi_banner.png")
BANNER_TRADING_DATE_TESPIT = os.path.join(_IMG_DIR, "trading_date_tespit_banner.png")

# Credentials cache — lazy init
_credentials = None
_init_attempted = False

# Duplicate tweet korumasi — ayni tweeti tekrar atmamak icin
# Key: tweet text hash, Value: timestamp (unix)
# 24 saat icinde ayni tweet atilmaz
_tweet_sent_cache: dict[str, float] = {}
_TWEET_DEDUP_HOURS = 24

# Son atilan tweet ID — reply/thread için (restartta sıfırlanır, sadece kısa vadeli)
_last_tweet_id: str = ""


def _is_duplicate_tweet(text: str) -> bool:
    """Ayni tweet son 24 saat icinde atildiysa True doner."""
    import time as _time
    text_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
    now = _time.time()

    # Eski kayitlari temizle (24 saatten eski)
    expired = [k for k, v in _tweet_sent_cache.items() if now - v > _TWEET_DEDUP_HOURS * 3600]
    for k in expired:
        del _tweet_sent_cache[k]

    if text_hash in _tweet_sent_cache:
        age_min = (now - _tweet_sent_cache[text_hash]) / 60
        logger.warning("DUPLICATE tweet engellendi (%.0f dk once atildi): %s...", age_min, text[:60])
        return True

    return False


def _mark_tweet_sent(text: str):
    """Basarili tweet'i cache'e kaydet."""
    import time as _time
    text_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
    _tweet_sent_cache[text_hash] = _time.time()


def _validate_ipo_for_tweet(ipo, required_fields: list[str], tweet_type: str) -> bool:
    """IPO verisinin tweet icin yeterli olup olmadigini kontrol eder.

    Eksik veri varsa tweet ATILMAZ, Telegram'a raporlanir.

    Args:
        ipo: IPO model instance
        required_fields: Zorunlu alan listesi (orn: ["company_name", "ticker"])
        tweet_type: Tweet tipi adi (orn: "Son Gun Sabah")

    Returns:
        True: veri yeterli, tweet atilabilir
        False: veri eksik, tweet atilmaz
    """
    missing = []
    for field in required_fields:
        val = getattr(ipo, field, None)
        if val is None or (isinstance(val, str) and not val.strip()):
            missing.append(field)

    if missing:
        ipo_name = getattr(ipo, "company_name", "?") or "?"
        msg = (
            f"Tweet ATILMADI — eksik veri!\n\n"
            f"Tweet tipi: {tweet_type}\n"
            f"IPO: {ipo_name}\n"
            f"Eksik alanlar: {', '.join(missing)}"
        )
        logger.warning(msg)
        _notify_tweet_failure(f"[{tweet_type}] {ipo_name}", f"Eksik veri: {', '.join(missing)}")
        return False

    return True


def _load_credentials() -> Optional[dict]:
    """Twitter API anahtarlarini yukler (tek seferlik)."""
    global _credentials, _init_attempted
    if _init_attempted:
        return _credentials
    _init_attempted = True

    try:
        from app.config import get_settings
        settings = get_settings()

        api_key = settings.X_API_KEY
        api_secret = settings.X_API_SECRET
        access_token = settings.X_ACCESS_TOKEN
        access_token_secret = settings.X_ACCESS_TOKEN_SECRET

        if not all([api_key, api_secret, access_token, access_token_secret]):
            logger.warning("Twitter API anahtarlari eksik — tweet atma devre disi")
            return None

        _credentials = {
            "api_key": api_key,
            "api_secret": api_secret,
            "access_token": access_token,
            "access_token_secret": access_token_secret,
        }
        logger.info("Twitter credentials yuklendi (@SZAlgoFinans)")
        return _credentials

    except Exception as e:
        logger.error(f"Twitter credentials yuklenemedi: {e}")
        return None


def _generate_oauth_signature(
    method: str,
    url: str,
    oauth_params: dict,
    consumer_secret: str,
    token_secret: str,
) -> str:
    """OAuth 1.0a HMAC-SHA1 imza uretir."""
    # Parameter string olustur (sirali)
    sorted_params = sorted(oauth_params.items())
    param_string = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted_params
    )

    # Base string
    base_string = (
        f"{method.upper()}&"
        f"{urllib.parse.quote(url, safe='')}&"
        f"{urllib.parse.quote(param_string, safe='')}"
    )

    # Signing key
    signing_key = (
        f"{urllib.parse.quote(consumer_secret, safe='')}&"
        f"{urllib.parse.quote(token_secret, safe='')}"
    )

    # HMAC-SHA1
    hashed = hmac.new(
        signing_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha1,
    )
    return base64.b64encode(hashed.digest()).decode("utf-8")


def _build_oauth_header(creds: dict) -> str:
    """OAuth 1.0a Authorization header olusturur."""
    oauth_params = {
        "oauth_consumer_key": creds["api_key"],
        "oauth_nonce": uuid.uuid4().hex,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": creds["access_token"],
        "oauth_version": "1.0",
    }

    # Imza olustur
    signature = _generate_oauth_signature(
        method="POST",
        url=_TWITTER_TWEET_URL,
        oauth_params=oauth_params,
        consumer_secret=creds["api_secret"],
        token_secret=creds["access_token_secret"],
    )
    oauth_params["oauth_signature"] = signature

    # Header string
    header_parts = ", ".join(
        f'{urllib.parse.quote(k, safe="")}="{urllib.parse.quote(v, safe="")}"'
        for k, v in sorted(oauth_params.items())
    )
    return f"OAuth {header_parts}"


def _notify_tweet_failure(text: str, error_detail: str):
    """Tweet basarisiz olunca Telegram'a bildirim gonder."""
    try:
        from app.services.admin_telegram import notify_scraper_error
        import asyncio
        msg = f"Tweet atilamadi!\n\nTweet: {text[:100]}...\n\nHata: {error_detail[:200]}"
        # sync context'te async fonksiyon cagirmak icin
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(notify_scraper_error("Twitter Tweet Hatası", msg))
        except RuntimeError:
            # Event loop yoksa yeni olustur
            asyncio.run(notify_scraper_error("Twitter Tweet Hatası", msg))
    except Exception:
        pass  # Telegram bildirimi de basarisiz olursa sessizce gec


def _safe_reply_tweet(reply_text: str, in_reply_to_tweet_id: str) -> bool:
    """Mevcut bir tweete reply (thread devamı) atar — sadece metin, resim yok.

    Returns:
        True: reply başarılı
        False: reply başarısız (ana tweet başarısı etkilenmez)
    """
    try:
        if not in_reply_to_tweet_id or in_reply_to_tweet_id == "?":
            logger.warning("Reply tweet: geçersiz in_reply_to_tweet_id")
            return False

        creds = _load_credentials()
        if not creds:
            logger.info("[TWITTER-DRY-RUN-REPLY] %s...", reply_text[:60])
            return False

        wait = _wait_for_tweet_rate_limit()
        if wait > 0:
            time.sleep(wait)

        auth_header = _build_oauth_header(creds)
        resp = httpx.post(
            _TWITTER_TWEET_URL,
            json={
                "text": reply_text,
                "reply": {"in_reply_to_tweet_id": in_reply_to_tweet_id},
            },
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json",
            },
            timeout=15.0,
        )

        if resp.status_code in (200, 201):
            reply_id = resp.json().get("data", {}).get("id", "?")
            logger.info("Reply tweet başarılı (id=%s, reply_to=%s)", reply_id, in_reply_to_tweet_id)
            _mark_tweet_sent(reply_text)
            _record_tweet_sent()
            return True
        else:
            logger.error("Reply tweet hatası HTTP %d: %s", resp.status_code, resp.text[:200])
            return False

    except Exception as e:
        logger.error("Reply tweet hatası (sistem etkilenmez): %s", e)
        return False


def _safe_tweet(text: str, source: str = "unknown", force_send: bool = False) -> bool:
    """Tweet atar — ASLA hata firlatmaz, sadece log'a yazar.
    Basarisiz olursa Telegram'a bildirim gonderir.

    TWITTER_AUTO_SEND=False iken tweet kuyruğa eklenir (admin onay bekler).
    force_send=True ise auto_send kontrolunu atlar (admin onay'dan gonderim icin).

    httpx + OAuth 1.0a HMAC-SHA1 ile Twitter API v2 kullanir.
    tweepy gerektirmez — Python 3.13 uyumlu.

    Returns:
        True: tweet basarili / kuyruğa eklendi
        False: tweet basarisiz (ama sistem etkilenmez)
    """
    try:
        # KILL SWITCH — admin panelden tüm tweetler durduruldu
        if not force_send and is_tweets_killed():
            logger.warning("[TWEET KILL SWITCH] Tweet durduruldu: %s", text[:60])
            return False

        # Onay modu — kuyruğa ekle, direkt atma (DB'den okunur, restart'a dayanıklı)
        if not force_send and not is_auto_send():
            # Caller fonksiyon adini otomatik tespit et
            import inspect
            caller = inspect.stack()[1].function if source == "unknown" else source
            return _queue_tweet(text, image_path=None, source=caller)

        # Duplicate kontrolu — ayni tweeti 24 saat icinde tekrar atma
        # Zaten atilmis tweet "basarisiz" degil, atlanmali → True don
        if _is_duplicate_tweet(text):
            return True

        creds = _load_credentials()
        if not creds:
            logger.info(f"[TWITTER-DRY-RUN] {text[:80]}...")
            return False

        # ── Rate limit: dakikada max 3 tweet ──
        wait = _wait_for_tweet_rate_limit()
        if wait > 0:
            time.sleep(wait)

        # Twitter karakter limiti: 4000 (Blue Tick)
        if len(text) > 4000:
            text = text[:3997] + "..."

        auth_header = _build_oauth_header(creds)

        response = httpx.post(
            _TWITTER_TWEET_URL,
            json={"text": text},
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json",
            },
            timeout=15.0,
        )

        if response.status_code in (200, 201):
            tweet_id = response.json().get("data", {}).get("id", "?")
            logger.info(f"Tweet basarili (id={tweet_id}): {text[:60]}...")
            _mark_tweet_sent(text)
            _record_tweet_sent()
            return True
        else:
            error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
            logger.error("Tweet API hatasi: %s", error_msg)
            _notify_tweet_failure(text, error_msg)
            return False

    except Exception as e:
        logger.error(f"Tweet hatasi (sistem etkilenmez): {e}")
        _notify_tweet_failure(text, str(e))
        return False


# ================================================================
# APP LINK & SABIT DEGERLER — Admin panelden duzenlenebilir
# ================================================================
_DEFAULTS = {
    "APP_LINK": "szalgo.net.tr",
    "SLOGAN": "\U0001F514 İlk bilen siz olun!",
    "DISCLAIMER": "\u26A0\uFE0F Yapay zek\u00e2 destekli otomatik bildirimdir, yat\u0131r\u0131m tavsiyesi i\u00e7ermez.",
    "DISCLAIMER_SHORT": "\u26A0\uFE0F YZ destekli bildirimdir, yat\u0131r\u0131m tavsiyesi i\u00e7ermez.",
    "HASHTAGS": "#HalkaArz #BIST100 #borsa #yatırım",
    # Tweet şablonları
    "T1_BASLIK": "\U0001F6A8 SPK Bülteni Yayımlandı!",
    "T1_ACIKLAMA": "için halka arz başvurusu SPK tarafından onaylandı.",
    "T1_CTA": "\U0001F4F2 Bilgiler geldikçe bildirim göndereceğiz.",
    "T2_BASLIK": "\U0001F4CB Halka Arz Başvuruları Başladı!",
    "T2_ACIKLAMA": "için talep toplama süreci başlamıştır.",
    "T3_BASLIK": "✅ Kesinleşen Dağıtım Sonuçları",
    "T4_BASLIK": "\u23F0 Son 4 Saat!",
    "T4_ACIKLAMA": "halka arz başvurusu için kapanışa son 4 saat kaldı!",
    "T5_BASLIK": "\U0001F6A8 Son 30 Dakika!",
    "T5_ACIKLAMA": "halka arz başvurusu kapanmak üzere!",
    "T6_BASLIK": "\U0001F514 Gong Çalıyor!",
    "T6_ACIKLAMA": "bugün borsada işleme başlıyor!",
    "T6_CTA": "25 günlük tavan/taban takibini uygulamamızdan yapabilirsiniz.",
    "T7_BASLIK": "\U0001F4C8 Açılış Fiyatı Belli Oldu!",
    "T11_TANITIM": "350+ hisse senedini tarayan sistemimiz çok yakında AppStore ve GoogleStore'da!",
    "T11_CTA": "Ücretsiz BIST 50 bildirimleri için:",
    "T12_BASLIK": "\U0001F4E2 Son Başvuru Günü!",
    "T12_CTA": "\u23F0 Son anlara kadar hatırlatma yapacağız.",
    "T13_BASLIK": "\U0001F4CB Halka Arz Hakkında",
    "T14_ACIKLAMA": "Güncel listeyi uygulamamızdan takip edebilirsiniz.",
    "T15_BASLIK": "📊 Öğle Arası",
    "T16_BASLIK": "📊 Yeni Halka Arzlar — Açılış Bilgileri",
    "LOT_DISCLAIMER": "tahmini değerdir",
    # Tweet modu — "true" iken otomatik atılır, "false" iken kuyruğa düşer
    "TWITTER_AUTO_SEND": "false",
}

# Settings cache — 5 dk
_settings_cache: dict[str, str] = {}
_settings_cache_ts: float = 0
_SETTINGS_CACHE_TTL = 300  # 5 dakika


def _get_setting(key: str) -> str:
    """DB'den ayar degerini okur (5dk cache). Yoksa default doner."""
    global _settings_cache, _settings_cache_ts
    import time as _t

    now = _t.time()
    if now - _settings_cache_ts > _SETTINGS_CACHE_TTL:
        # Cache expired — DB'den yenile
        try:
            from app.config import get_settings
            db_url = str(get_settings().DATABASE_URL)
            sync_url = db_url.replace("postgresql+asyncpg://", "postgresql://").replace("postgres://", "postgresql://")

            from sqlalchemy import create_engine, text as sa_text
            engine = create_engine(sync_url, pool_pre_ping=True)
            with engine.connect() as conn:
                rows = conn.execute(sa_text("SELECT key, value FROM app_settings")).fetchall()
                _settings_cache = {r[0]: r[1] for r in rows}
            engine.dispose()
            _settings_cache_ts = now
        except Exception as e:
            logger.debug("AppSetting cache yenilenemedi (default kullanılacak): %s", e)
            _settings_cache_ts = now  # Hata durumunda da cache'i sifirla, surekli retry yapmasin

    return _settings_cache.get(key, _DEFAULTS.get(key, ""))


def clear_settings_cache():
    """Admin ayar değiştirdiğinde cache'i sıfırla."""
    global _settings_cache_ts
    _settings_cache_ts = 0


def is_auto_send() -> bool:
    """TWITTER_AUTO_SEND durumunu DB'den okur (5dk cache ile).

    True  → Otomatik mod (tweetler direkt X'e atılır)
    False → Onay modu (tweetler kuyruğa düşer, admin onaylar)

    Restart'tan etkilenmez — değer app_settings tablosunda saklanır.
    """
    val = _get_setting("TWITTER_AUTO_SEND")
    return val.lower() in ("true", "1", "yes")


# ─── KILL SWITCH FONKSİYONLARI ───
def is_notifications_killed() -> bool:
    """Bildirim kill switch durumu. True ise TÜM push bildirimler durdurulur."""
    val = _get_setting("NOTIFICATIONS_KILL_SWITCH")
    return val.lower() in ("true", "1", "yes")


def is_tweets_killed() -> bool:
    """Tweet kill switch durumu. True ise TÜM tweetler durdurulur (kuyruga da eklenmez)."""
    val = _get_setting("TWEETS_KILL_SWITCH")
    return val.lower() in ("true", "1", "yes")


# Backward-compatible property'ler
class _DynSetting:
    """Lazy ayar okuyucu — APP_LINK gibi kullanildiginda DB'den guncel degeri doner."""
    def __init__(self, key: str):
        self._key = key
    def __str__(self):
        return _get_setting(self._key)
    def __repr__(self):
        return _get_setting(self._key)
    def __format__(self, format_spec):
        return format(_get_setting(self._key), format_spec)
    def __eq__(self, other):
        return str(self) == str(other)
    def __add__(self, other):
        return str(self) + str(other)
    def __radd__(self, other):
        return str(other) + str(self)


# f-string icinde {APP_LINK} yazildiginda otomatik DB'den okur
APP_LINK = _DynSetting("APP_LINK")
HALKAARZ_LINK = "https://szalgo.net.tr/halkaarz"
HALKAARZ_BEKLEYENLER_LINK = "https://szalgo.net.tr/halkaarz/onay-bekleyenler"
KAP_HABER_LINK = "https://szalgo.net.tr/telegram-kap"
SLOGAN = _DynSetting("SLOGAN")
DISCLAIMER = _DynSetting("DISCLAIMER")
DISCLAIMER_SHORT = _DynSetting("DISCLAIMER_SHORT")


# ================================================================
# 1. YENI HALKA ARZ (SPK Onayi)
# ================================================================
def _build_ipo_approval_image(ipos: list, bulletin_no: str) -> Optional[str]:
    """Halka arz onayları için özel görsel oluşturur.

    IPO objelerini approvals dict formatına çevirir ve generate_spk_onay_image çağırır.
    Başarısızlık durumunda None döner (fallback: BANNER_SPK_ONAYI kullanılır).
    """
    try:
        from app.services.chart_image_generator import generate_spk_onay_image
        approvals = []
        for ipo in ipos:
            approvals.append({
                "company_name": ipo.company_name or "Bilinmiyor",
                "existing_capital": getattr(ipo, "existing_capital", None),
                "new_capital": getattr(ipo, "new_capital", None),
                "sale_price": getattr(ipo, "ipo_price", None),
            })
        return generate_spk_onay_image(approvals, bulletin_no)
    except Exception as e:
        logger.warning("IPO approval image olusturulamadi: %s", e)
        return None


def tweet_new_ipo(ipo) -> bool:
    """SPK'dan yeni halka arz onayi geldiginde tweet atar."""
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Yeni Halka Arz"):
            return False

        # Bülten numarasını bul (varsa)
        bulletin_no = getattr(ipo, "spk_bulletin_no", None) or "SPK"

        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        price_text = ""
        if ipo.ipo_price:
            price_text = f"\n\U0001F4B0 Halka arz fiyatı: {ipo.ipo_price} TL"

        text = (
            f"{_get_setting('T1_BASLIK')}\n\n"
            f"{ipo.company_name}{ticker_text} {_get_setting('T1_ACIKLAMA')}"
            f"{price_text}\n\n"
            f"{_get_setting('T1_CTA')}\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #borsa #yatırım"
        )
        # Özel görsel oluştur
        img_path = _build_ipo_approval_image([ipo], bulletin_no)
        if img_path:
            return _safe_tweet_with_media(text, img_path, source="tweet_new_ipo")
        return _safe_tweet_with_media(text, BANNER_SPK_ONAYI, source="tweet_new_ipo")
    except Exception as e:
        logger.error(f"tweet_new_ipo hatasi: {e}")
        return False


def tweet_new_ipos_batch(ipos: list, bulletin_no: str) -> bool:
    """Ayni bultendeki halka arz onaylarini tek tweet'te atar.

    1 adet ise: eski format (tweet_new_ipo)
    2+ adet ise: birlesik format (liste halinde) + özel görsel

    Args:
        ipos: Yeni olusturulan IPO objeleri listesi
        bulletin_no: Bulten numarasi (orn: "2026/10")
    """
    try:
        if not ipos:
            return False

        # Tek onay → eski format
        if len(ipos) == 1:
            return tweet_new_ipo(ipos[0])

        # 2+ onay → birlesik format
        lines = []
        for ipo in ipos:
            price = f" — {ipo.ipo_price} TL" if ipo.ipo_price else ""
            lines.append(f"✅ {ipo.company_name}{price}")

        text = (
            f"{_get_setting('T1_BASLIK')}\n\n"
            f"{bulletin_no} Bülteninde {len(ipos)} adet onaylanan halka arz "
            f"{_get_setting('T1_ACIKLAMA')}\n\n"
            + "\n".join(lines) + "\n\n"
            f"{_get_setting('T1_CTA')}\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #borsa #yatırım"
        )
        # Özel görsel oluştur
        img_path = _build_ipo_approval_image(ipos, bulletin_no)
        if img_path:
            return _safe_tweet_with_media(text, img_path, source="tweet_new_ipos_batch")
        return _safe_tweet_with_media(text, BANNER_SPK_ONAYI, source="tweet_new_ipos_batch")
    except Exception as e:
        logger.error(f"tweet_new_ipos_batch hatasi: {e}")
        return False


# ================================================================
# 2. DAGITIMA CIKIS
# ================================================================
def _get_rejected_brokers(ipo_id: int) -> list[str]:
    """IPO'ya ait katılınamayacak kurum isimlerini senkron DB ile çeker."""
    try:
        from app.config import get_settings
        db_url = str(get_settings().DATABASE_URL)
        sync_url = db_url.replace("postgresql+asyncpg://", "postgresql://").replace("postgres://", "postgresql://")
        from sqlalchemy import create_engine, text as sa_text
        engine = create_engine(sync_url, pool_pre_ping=True)
        with engine.connect() as conn:
            rows = conn.execute(
                sa_text("SELECT broker_name FROM ipo_brokers WHERE ipo_id = :iid AND is_rejected = true"),
                {"iid": ipo_id},
            ).fetchall()
        engine.dispose()
        return [r[0] for r in rows]
    except Exception as e:
        logger.debug("Rejected brokers alinamadi: %s", e)
        return []


def tweet_distribution_start(ipo) -> bool:
    """Dağıtım süreci başladığında TEK tweet atar (thread yok).

    Tüm bilgiler tek tweette:
    - Şirket bilgileri, fiyat, tarih, tahmini lot
    - Katılınamayacak kurumlar (varsa)
    - Kurum kısaltma hashtag'leri (#Akbank #Garanti #Bizim vb.)
    - Link ve genel hashtag'ler
    """
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Dağıtıma Çıkış"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        end_date = ""
        if ipo.subscription_end:
            end_date = f"\n📅 Son başvuru: {ipo.subscription_end.strftime('%d.%m.%Y')}"
        price_text = f"\n💰 Fiyatı: {ipo.ipo_price} TL" if ipo.ipo_price else ""

        # Tahmini lot bilgisi varsa ekle
        lot_text = ""
        if ipo.estimated_lots_per_person:
            lot_text = f"\n📊 Tahmini dağıtım: ~{ipo.estimated_lots_per_person} lot/kişi (tahminidir)"

        # Katılınamayacak kurumlar
        rejected = _get_rejected_brokers(ipo.id) if ipo.id else []
        rejected_section = ""
        broker_hashtags = ""
        if rejected:
            broker_lines = "\n".join(rejected)
            rejected_section = f"\n\n❌ Katılınamayacak Kurumlar:\n{broker_lines}"

            # Kurum adlarından hashtag üret: ilk kelimeyi al, Türkçe karakterleri düzelt
            broker_tags = set()
            for name in rejected:
                first_word = name.split()[0] if name.split() else ""
                # Sadece harf içeren kısa isimleri hashtag yap
                clean = first_word.replace(".", "").replace(",", "").strip()
                if clean and len(clean) >= 2 and clean.isalpha():
                    broker_tags.add(f"#{clean}")
            if broker_tags:
                # Çok fazla hashtag olmasın — max 8
                sorted_tags = sorted(broker_tags)[:8]
                broker_hashtags = " ".join(sorted_tags)

        # Tek tweet: Her şey bir arada
        text = (
            f"{_get_setting('T2_BASLIK')}\n\n"
            f"{ipo.company_name}{ticker_text} {_get_setting('T2_ACIKLAMA')}"
            f"{price_text}{end_date}{lot_text}\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}"
            f"{rejected_section}\n\n"
            f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #yatırım"
        )

        # Kurum hashtag'leri sığarsa ekle (4000 char limiti — Blue Tick)
        if broker_hashtags and len(text) + len(broker_hashtags) + 1 < 3900:
            text += f" {broker_hashtags}"

        ok = _safe_tweet_with_media(text, BANNER_BASVURULAR_BASLIYOR, source="tweet_distribution_start")
        return ok
    except Exception as e:
        logger.error(f"tweet_distribution_start hatası: {e}")
        return False


# ================================================================
# 3. KESİNLEŞEN DAĞITIM SONUÇLARI
# ================================================================
def tweet_allocation_results(ipo, allocations: list = None) -> bool:
    """Kesinleşen dağıtım sonuçları tweet atar.

    allocations: IPOAllocation listesi veya dict listesi
      Her biri: group_name, allocation_pct, allocated_lots, participant_count, avg_lot_per_person
    """
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Tahsisat tablosu — kurumsal yurt içi/dışı + bireysel
        table_lines = []
        bireysel_avg_lot = None
        total_applicants = getattr(ipo, "total_applicants", None)

        if allocations:
            for a in allocations:
                # dict veya ORM objesi destekle
                if isinstance(a, dict):
                    grp = a.get("group_name", "")
                    pct = a.get("allocation_pct")
                    lots = a.get("allocated_lots")
                    participants = a.get("participant_count")
                    avg_lot = a.get("avg_lot_per_person")
                else:
                    grp = a.group_name
                    pct = a.allocation_pct
                    lots = a.allocated_lots
                    participants = a.participant_count
                    avg_lot = a.avg_lot_per_person

                # Grup adi Turkce
                grp_labels = {
                    "bireysel": "Bireysel",
                    "yuksek_basvurulu": "Yüksek Başvurulu",
                    "kurumsal_yurtici": "Kurumsal Yurt İçi",
                    "kurumsal_yurtdisi": "Kurumsal Yurt Dışı",
                }
                label = grp_labels.get(grp, grp)

                # Her grup icin detayli bilgi: Kisi, Lot, Oran
                line = f"📌 {label}"
                details = []
                if participants:
                    details.append(f"Kişi: {int(participants):,}".replace(",", "."))
                if lots:
                    details.append(f"Lot: {int(lots):,}".replace(",", "."))
                if pct:
                    details.append(f"Oran: %{float(pct):.0f}")
                if details:
                    line += "\n" + " | ".join(details)
                table_lines.append(line)

                # Bireysel yatırımcıya düşen ort lot
                if grp == "bireysel":
                    if avg_lot:
                        bireysel_avg_lot = avg_lot
                    elif lots and participants and int(participants) > 0:
                        bireysel_avg_lot = int(lots) / int(participants)

        table_text = "\n\n".join(table_lines) if table_lines else ""

        # Bireysel yatırımcı sonucu
        bireysel_text = ""
        if bireysel_avg_lot:
            bireysel_text = f"\n\n👤 Bireysel yatırımcıya düşen: ~{round(float(bireysel_avg_lot))} lot/kişi"

        # Toplam başvuran
        applicant_text = ""
        if total_applicants:
            applicant_text = f"\n📊 Toplam başvuran: {int(total_applicants):,}".replace(",", ".") + " kişi"

        text = (
            f"{_get_setting('T3_BASLIK')}\n\n"
            f"{ipo.company_name}{ticker_text}\n\n"
            f"{table_text}"
            f"{bireysel_text}"
            f"{applicant_text}\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #hisse"
        )

        return _safe_tweet_with_media(text, BANNER_DAGITIM_SONUCLARI)
    except Exception as e:
        logger.error(f"tweet_allocation_results hatası: {e}")
        return False


# ================================================================
# 4. SON 4 SAAT HATIRLATMA
# ================================================================
def tweet_last_4_hours(ipo) -> bool:
    """Son 4 saat kala hatirlatma tweeti."""
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Son 4 Saat"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Kapanis saatini goster
        end_hour = "17:00"
        if ipo.subscription_hours:
            parts = str(ipo.subscription_hours).split("-")
            if len(parts) >= 2:
                end_hour = parts[-1].strip()

        # Tahmini lot bilgisi varsa ekle
        lot_text = ""
        if getattr(ipo, 'estimated_lots_per_person', None):
            lot_text = f"\n📊 Tahmini: ~{ipo.estimated_lots_per_person} lot/kişi ({_get_setting('LOT_DISCLAIMER')})"

        text = (
            f"{_get_setting('T4_BASLIK')}\n\n"
            f"{ipo.company_name}{ticker_text} "
            f"{_get_setting('T4_ACIKLAMA')}"
            f"{lot_text}\n\n"
            f"⏳ Başvurular saat {end_hour}'a kadar devam ediyor.\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #yatırım"
        )
        return _safe_tweet_with_media(text, BANNER_SON_4_SAAT)
    except Exception as e:
        logger.error(f"tweet_last_4_hours hatasi: {e}")
        return False


# ================================================================
# 5. SON 30 DAKIKA HATIRLATMA
# ================================================================
def tweet_last_30_min(ipo) -> bool:
    """Son 30 dakika kala hatirlatma tweeti."""
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Son 30 Dakika"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Kapanis saatini goster
        end_hour = "17:00"
        if ipo.subscription_hours:
            parts = str(ipo.subscription_hours).split("-")
            if len(parts) >= 2:
                end_hour = parts[-1].strip()

        # Tahmini lot bilgisi varsa ekle
        lot_text = ""
        if getattr(ipo, 'estimated_lots_per_person', None):
            lot_text = f"\n📊 Tahmini: ~{ipo.estimated_lots_per_person} lot/kişi ({_get_setting('LOT_DISCLAIMER')})"

        text = (
            f"{_get_setting('T5_BASLIK')}\n\n"
            f"{ipo.company_name}{ticker_text} {_get_setting('T5_ACIKLAMA')}"
            f"{lot_text}\n\n"
            f"Saat {end_hour}'da başvurular kapanıyor, acele edin!\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #yatırım"
        )
        return _safe_tweet_with_media(text, BANNER_SON_30_DAKIKA)
    except Exception as e:
        logger.error(f"tweet_last_30_min hatasi: {e}")
        return False


# ================================================================
# 6. ILK ISLEM GUNU (09:00 — Gong caliyor!)
# ================================================================
def tweet_first_trading_day(ipo) -> bool:
    """Ilk islem gunu sabahi gong tweeti."""
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "İlk İşlem Günü"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        price_text = ""
        if ipo.ipo_price:
            price_text = f"\n\U0001F4B0 Halka arz fiyatı: {ipo.ipo_price} TL"

        text = (
            f"{_get_setting('T6_BASLIK')}\n\n"
            f"{ipo.company_name}{ticker_text} {_get_setting('T6_ACIKLAMA')}"
            f"{price_text}\n\n"
            f"{_get_setting('T6_CTA')}\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #hisse"
        )
        return _safe_tweet_with_media(text, BANNER_GONG_CALIYOR)
    except Exception as e:
        logger.error(f"tweet_first_trading_day hatasi: {e}")
        return False


# ================================================================
# 6b. ISLEM TARIHI TESPIT (scraper'dan aninda — trading_start ilk set)
# ================================================================
def tweet_trading_date_detected(ipo) -> bool:
    """Ilk islem tarihi tespit edildi tweeti — HalkArz scraper'dan hemen."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Pazar bilgisi
        pazar_map = {
            "yildiz_pazar": "Yildiz Pazar",
            "ana_pazar": "Ana Pazar",
            "alt_pazar": "Alt Pazar",
        }
        pazar = pazar_map.get(ipo.market_segment or "", "")
        pazar_line = f"\n\U0001F4CD {pazar}'da islem gorecek" if pazar else ""

        # Tarih formati — Turkce
        _AYLAR = ["Ocak", "Subat", "Mart", "Nisan", "Mayis", "Haziran",
                  "Temmuz", "Agustos", "Eylul", "Ekim", "Kasim", "Aralik"]
        tarih_line = ""
        if ipo.trading_start:
            d = ipo.trading_start
            tarih_line = f"\n\U0001F4C5 Ilk islem: {d.day} {_AYLAR[d.month - 1]} {d.year}"

        text = (
            f"\U0001F4CA Islem Tarihi Belli Oldu!\n\n"
            f"{ipo.company_name}{ticker_text}"
            f"{tarih_line}"
            f"{pazar_line}\n\n"
            f"Borsada islem gormeye basliyor! \U0001F514\n"
            f"#HalkaArz #BIST100 #borsa"
        )
        if ipo.ticker:
            text += f" #{ipo.ticker}"

        return _safe_tweet_with_media(text, BANNER_TRADING_DATE_TESPIT)
    except Exception as e:
        logger.error(f"tweet_trading_date_detected hatasi: {e}")
        return False


# ================================================================
# 7. ACILIS FIYATI (09:56 — sadece ilk islem gunu)
# ================================================================
def tweet_opening_price(ipo, open_price: float, pct_change: float) -> bool:
    """Ilk islem gunu acilis fiyati tweeti (09:56)."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0

        # Durum belirle
        if pct_change >= 9.5:
            durum = "\U0001F7E2 Tavandan açıldı!"
        elif pct_change > 0:
            durum = f"\U0001F7E2 %{pct_change:+.2f} yükselişle açıldı"
        elif pct_change == 0:
            durum = f"\U0001F7E1 Halka arz fiyatından açıldı"
        else:
            durum = f"\U0001F534 %{pct_change:+.2f} düşüşle açıldı"

        text = (
            f"{_get_setting('T7_BASLIK')}\n\n"
            f"{ipo.company_name}{ticker_text}\n\n"
            f"\u2022 Halka arz fiyatı: {ipo_price:.2f} TL\n"
            f"\u2022 Açılış fiyatı: {open_price:.2f} TL\n"
            f"\u2022 {durum}\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #hisse"
        )
        return _safe_tweet_with_media(text, BANNER_ACILIS_FIYATI)
    except Exception as e:
        logger.error(f"tweet_opening_price hatasi: {e}")
        return False


# ================================================================
# 8. GUNLUK TAKIP (18:20 — her islem gunu)
# ================================================================
def tweet_daily_tracking(ipo, trading_day: int, close_price: float,
                         pct_change: float, durum: str,
                         days_data: list = None,
                         ceiling_days: int = 0,
                         floor_days: int = 0) -> bool:
    """Her islem gunu 18:20'de gunluk takip tweeti.

    - 1-5. gun: Eski metin format + statik banner
    - 6-24. gun: Dinamik PNG gorsel + kisa metin
    """
    try:
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0

        # Durum emoji
        durum_map = {
            "tavan": "\U0001F7E2 Tavan",
            "alici_kapatti": "\U0001F7E2 Alıcı kapattı",
            "not_kapatti": "\U0001F7E1 Not kapattı",
            "satici_kapatti": "\U0001F534 Satıcı kapattı",
            "taban": "\U0001F534 Taban",
        }
        durum_text = durum_map.get(durum, durum)
        daily_emoji = "\U0001F7E2" if pct_change >= 0 else "\U0001F534"

        # ── 6+ gun: Dinamik PNG gorsel ───────────────────
        image_path = None
        if trading_day >= 6 and days_data and ipo_price > 0:
            try:
                from app.services.chart_image_generator import generate_daily_tracking_image
                image_path = generate_daily_tracking_image(
                    ipo, days_data, ceiling_days, floor_days, trading_day,
                )
            except Exception as img_err:
                logger.warning("Gunluk takip gorsel olusturulamadi: %s", img_err)

        if image_path:
            # Kisa tweet metni — gorsel detayi iceriyor
            cum_pct = ((close_price - ipo_price) / ipo_price) * 100 if ipo_price > 0 else 0
            normal_d = trading_day - ceiling_days - floor_days
            text = (
                f"\U0001F4CA #{ipo.ticker or ipo.company_name} \u2014 {trading_day}/25 Gün Sonu\n\n"
                f"Halka Arz: {ipo_price:.2f} TL\n"
                f"{daily_emoji} Kapanış: {close_price:.2f} TL | %{pct_change:+.2f} | {durum_text}\n"
                f"Kümülatif: %{cum_pct:+.1f}\n\n"
                f"Tavan: {ceiling_days} | Taban: {floor_days} | Normal İşlem Aralığı: {normal_d}\n\n"
                f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
                f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #hisse"
            )
            banner = image_path
        else:
            # ── 1-5 gun (veya gorsel hata): Eski metin format ──
            table_lines = []
            if days_data and ipo_price > 0:
                for d in days_data:
                    day_num = d["trading_day"]
                    day_close = float(d["close"])
                    cum_pct = ((day_close - ipo_price) / ipo_price) * 100
                    emoji = "\U0001F7E2" if cum_pct >= 0 else "\U0001F534"
                    if day_num == trading_day:
                        table_lines.append(f"{day_num}. {emoji} %{cum_pct:+.1f} \u25C0")
                    else:
                        table_lines.append(f"{day_num}. {emoji} %{cum_pct:+.1f}")
            else:
                if ipo_price > 0:
                    cum_change = ((close_price - ipo_price) / ipo_price) * 100
                else:
                    cum_change = 0
                table_lines.append(f"{trading_day}. %{cum_change:+.1f}")

            table_text = "\n".join(table_lines)

            text = (
                f"\U0001F4CA #{ipo.ticker or ipo.company_name} \u2014 {trading_day}. Gün Sonu\n\n"
                f"Kümülatif Toplam:\n"
                f"{table_text}"
                f"\n\n{daily_emoji} Kapanış: {close_price:.2f} TL | %{pct_change:+.2f} | {durum_text}\n\n"
                f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
                f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #hisse"
            )
            banner = BANNER_GUNLUK_TAKIP

        # Kuyruk modunda temp dosyayi silme
        auto_send = is_auto_send()

        result = _safe_tweet_with_media(text, banner)

        # Temp dosya temizligi — sadece auto_send modunda
        if image_path and auto_send:
            try:
                os.remove(image_path)
            except OSError:
                pass

        return result
    except Exception as e:
        logger.error(f"tweet_daily_tracking hatasi: {e}")
        return False


# ================================================================
# 9. 25 GUN PERFORMANS OZETI (25. gun tamamlandiginda bir kez)
# ================================================================
def tweet_25_day_performance(
    ipo,
    close_price_25: float,
    total_pct: float,
    ceiling_days: int,
    floor_days: int,
    avg_lot: Optional[float] = None,
    days_data: list = None,
) -> bool:
    """25 islem gunu tamamlandiginda dinamik tablo gorseli + kisa metin tweeti."""
    try:
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
        ticker = ipo.ticker or ipo.company_name
        normal_days = 25 - ceiling_days - floor_days

        # Lot kazanc hesabi
        lot_text = ""
        if avg_lot and ipo_price > 0:
            lot_count = round(float(avg_lot))
            total_profit = (close_price_25 - ipo_price) * lot_count  # lot = adet
            if total_profit >= 0:
                lot_text = f"\nOrt Lotla Karne: +{total_profit:,.0f} TL (%{total_pct:+.1f})"
            else:
                lot_text = f"\nOrt Lotla Karne: {total_profit:,.0f} TL (%{total_pct:+.1f})"

        # Dinamik gorsel olustur (days_data varsa)
        image_path = None
        if days_data and ipo_price > 0:
            try:
                from app.services.chart_image_generator import generate_25day_image
                image_path = generate_25day_image(
                    ipo, days_data, ceiling_days, floor_days, avg_lot,
                )
            except Exception as img_err:
                logger.warning("25 gun gorsel olusturulamadi, statik banner kullanilacak: %s", img_err)

        # Kisa tweet metni (gorsel detayi veriyor)
        text = (
            f"\U0001F4CB #{ticker} \u2014 25 G\u00fcn\u00fc Bitirdi\n\n"
            f"Halka Arz: {ipo_price:.2f} TL"
        )
        if avg_lot:
            text += f"\nKi\u015fi Ba\u015f\u0131 Ort Lot: {round(float(avg_lot))}"
        text += lot_text
        text += (
            f"\n\nTavan: {ceiling_days} | Taban: {floor_days} | Normal İşlem Aralığı: {normal_days}\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #{ticker} #hisse"
        )

        # Dinamik gorsel varsa onu kullan, yoksa statik banner
        banner = image_path if image_path else BANNER_25_GUN_PERFORMANS

        # Kuyruk modunda temp dosyayi silme — admin onayindan sonra lazim
        auto_send = is_auto_send()

        result = _safe_tweet_with_media(text, banner)

        # Temp dosyayi temizle — sadece auto_send modunda (kuyrukta dosya lazim)
        if image_path and auto_send:
            try:
                os.remove(image_path)
            except OSError:
                pass

        return result
    except Exception as e:
        logger.error(f"tweet_25_day_performance hatasi: {e}")
        return False


# ================================================================
# 10. AY SONU HALKA ARZ RAPORU (Her ayin son gunu gece yarisi)
# ================================================================
def tweet_yearly_summary(
    year: int,
    month_name: str,
    total_ipos: int,
    avg_return_pct: float,
    best_ticker: str,
    best_return_pct: float,
    worst_ticker: str,
    worst_return_pct: float,
    total_completed: int,
    positive_count: int,
    median_return_pct: float = 0.0,
    all_returns: list = None,
) -> bool:
    """Ay sonu halka arz raporu tweeti — her ayin son gunu gece yarisi.
    all_returns: [{"ticker": "ZGYO", "pct": 237.8}, ...] — sıralama tablosu için
    """
    try:
        negative_count = total_completed - positive_count
        win_rate = (positive_count / total_completed * 100) if total_completed > 0 else 0

        # Performans emoji
        if avg_return_pct >= 10:
            perf_emoji = "🔥"
        elif avg_return_pct >= 0:
            perf_emoji = "🟢"
        else:
            perf_emoji = "🔴"

        # Başarı oranı emoji
        if win_rate >= 80:
            rate_emoji = "🏆"
        elif win_rate >= 50:
            rate_emoji = "🎯"
        else:
            rate_emoji = "⚠️"

        # ── Performans Sıralaması Tablosu ──
        ranking_section = ""
        ticker_hashtags = ""
        if all_returns and len(all_returns) > 0:
            sorted_rets = sorted(all_returns, key=lambda r: r["pct"], reverse=True)

            def _perf_emoji(pct):
                if pct >= 100: return "🔥"
                if pct >= 50: return "🚀"
                if pct >= 0: return "📈"
                return "📉"

            ranking_lines = []
            for i, r in enumerate(sorted_rets, 1):
                ranking_lines.append(
                    f"{i}. #{r['ticker']} → %{r['pct']:+.1f} {_perf_emoji(r['pct'])}"
                )
            ranking_section = "\n📈 Performans Sıralaması\n" + "\n".join(ranking_lines) + "\n"

            # Tüm hisse ticker'larını hashtag olarak ekle
            ticker_tags = " ".join(f"#{r['ticker']}" for r in sorted_rets)
            ticker_hashtags = f" {ticker_tags}"

        def _build_text(rank_sec, t_hashtags):
            return (
                f"📊 {year} Halka Arz — {month_name} Raporu\n\n"
                f"📋 Genel Bakış\n"
                f"• Toplam halka arz: {total_ipos}\n"
                f"• 25 işlem günü doldu: {total_completed}\n\n"
                f"✅ Kâr eden: {positive_count} hisse\n"
                f"❌ Zarar eden: {negative_count} hisse\n"
                f"{rate_emoji} Başarı oranı: %{win_rate:.0f}\n\n"
                f"💰 Getiri Analizi\n"
                f"• Ortalama: {perf_emoji} %{avg_return_pct:+.1f}\n"
                f"• Medyan: %{median_return_pct:+.1f}\n"
                f"{rank_sec}\n"
                f"⚠️ İlk 25 işlem günü baz alınmıştır.\n\n"
                f"📲 Detaylar için: {HALKAARZ_LINK}\n"
                f"#HalkaArz #BIST100 #borsa #yatırım{t_hashtags}"
            )

        text = _build_text(ranking_section, ticker_hashtags)

        # Karakter limiti kontrolü — sıralama çok uzunsa kısalt
        if len(text) > 3800 and ranking_section and all_returns:
            sorted_rets = sorted(all_returns, key=lambda r: r["pct"], reverse=True)
            top5 = sorted_rets[:5]
            short_lines = []
            for i, r in enumerate(top5, 1):
                short_lines.append(
                    f"{i}. #{r['ticker']} → %{r['pct']:+.1f} {_perf_emoji(r['pct'])}"
                )
            if len(sorted_rets) > 5:
                short_lines.append(f"... ve {len(sorted_rets) - 5} hisse daha")
            ranking_section = "\n📈 Performans Sıralaması\n" + "\n".join(short_lines) + "\n"
            text = _build_text(ranking_section, ticker_hashtags)

        # Hâlâ çok uzunsa hashtag'leri kısalt
        if len(text) > 3900:
            text = _build_text(ranking_section, "")

        return _safe_tweet_with_media(text, BANNER_AY_SONU_RAPOR, source="tweet_yearly_summary")
    except Exception as e:
        logger.error(f"tweet_yearly_summary hatasi: {e}")
        return False


# ================================================================
# 11. BIST 50 KAP HABERI
# ================================================================
def tweet_bist30_news(
    ticker: str,
    matched_keyword: str,
    sentiment: str,
    ai_score: float | None = None,
    ai_summary: str | None = None,
    kap_url: str | None = None,
    ai_hashtags: list | None = None,
) -> bool:
    """BIST 50 hissesi icin KAP haberi tweeti.

    AI skoru, ozeti ve hashtag'leri varsa tweet'e dahil edilir.
    AI skoru yoksa eski formata fallback.
    Blue Tick hesap — 4000 karakter limiti.
    """
    try:
        # Görsel yolu
        img_path = os.path.join(_IMG_DIR, "kap_bildirim.png")
        if not os.path.exists(img_path):
            img_path = None  # Gorsel yoksa sadece text at

        # Anlik zaman — Turkiye saati (UTC+3)
        now_str = datetime.now(_TR_TZ).strftime("%H:%M:%S")

        if sentiment == "positive":
            emoji = "\U0001F7E2"  # Yesil top
        else:
            emoji = "\U0001F534"  # Kirmizi top

        # Keyword temizligi (Haber Detayı Bulunamadı vb.)
        clean_kw = matched_keyword
        if not clean_kw or "BULUNAMADI" in clean_kw.upper() or clean_kw == ticker:
            clean_kw = "Yeni KAP Bildirimi"

        # AI skoru emojisi
        if ai_score and ai_score >= 8:
            score_emoji = "🔥"
        elif ai_score and ai_score >= 6:
            score_emoji = "📊"
        else:
            score_emoji = ""

        # AI bolumu (varsa)
        # Blue Tick = 4000 karakter — AI ozeti TAMAM olarak goster (500 char)
        ai_section = ""
        if ai_score is not None:
            ai_section += f"\n{score_emoji} AI Puanı: {ai_score:.1f}/10\n"
        if ai_summary:
            summary_text = ai_summary[:500]
            if len(ai_summary) > 500:
                summary_text += "..."
            ai_section += f"\n💬 {summary_text}\n"

        # KAP link bolumu (varsa)
        kap_section = ""
        if kap_url:
            if "kap.org.tr" in kap_url:
                kap_section = f"\n📎 KAP: {kap_url}\n"
            else:
                kap_section = f"\n📎 Kaynak: {kap_url}\n"

        # AI tarafindan uretilen icerik hashtag'leri (sektor, konu vb.)
        extra_hashtags = ""
        if ai_hashtags:
            tags = " ".join(f"#{t}" for t in ai_hashtags[:5])  # max 5 hashtag
            extra_hashtags = f" {tags}"

        text = (
            f"{emoji} #{ticker} — Haber Bildirimi\n\n"
            f"Anlık Haber Yakalandı {now_str}\n\n"
            f"İlişkili Kelime : {clean_kw}\n"
            f"{ai_section}"
            f"{kap_section}\n"
            f"{_get_setting('T11_CTA')}\n"
            f"📲 {KAP_HABER_LINK}\n"
            f"⚠️YT değildir\n"
            f"#BIST50 #{ticker} #KAP #BorsaIstanbul{extra_hashtags}"
        )

        # Blue Tick 4000 karakter limiti — AI ozeti ile birlikte sigmazsa kirp
        if len(text) > 3800:
            # AI ozeti yariya indir
            ai_section_mid = ""
            if ai_score is not None:
                ai_section_mid = f"\n{score_emoji} AI Puanı: {ai_score:.1f}/10\n"
            if ai_summary:
                short_sum = ai_summary[:250] + ("..." if len(ai_summary) > 250 else "")
                ai_section_mid += f"\n💬 {short_sum}\n"
            text = (
                f"{emoji} #{ticker} — Haber Bildirimi\n\n"
                f"Anlık Haber Yakalandı {now_str}\n\n"
                f"İlişkili Kelime : {clean_kw}\n"
                f"{ai_section_mid}"
                f"{kap_section}\n"
                f"{_get_setting('T11_CTA')}\n"
                f"📲 {KAP_HABER_LINK}\n"
                f"⚠️YT değildir\n"
                f"#BIST50 #{ticker} #KAP #BorsaIstanbul{extra_hashtags}"
            )

        # Hala cok uzunsa: sadece skor, ozet yok
        if len(text) > 3800:
            ai_section_short = ""
            if ai_score is not None:
                ai_section_short = f"\n{score_emoji} AI Puanı: {ai_score:.1f}/10\n"
            text = (
                f"{emoji} #{ticker} — Haber Bildirimi\n\n"
                f"Anlık Haber Yakalandı {now_str}\n\n"
                f"İlişkili Kelime : {clean_kw}\n"
                f"{ai_section_short}"
                f"{kap_section}\n"
                f"{_get_setting('T11_CTA')}\n"
                f"📲 {KAP_HABER_LINK}\n"
                f"⚠️YT değildir\n"
                f"#BIST50 #{ticker} #KAP #BorsaIstanbul{extra_hashtags}"
            )

        # KAP haberleri anlik bildirim — kuyrukta beklemesi anlamsiz
        # force_send=True ile TWITTER_AUTO_SEND'den bagimsiz direkt atar
        logger.info(
            "[KAP-TWEET] %s — text=%d char, img=%s, ai_score=%s",
            ticker, len(text), bool(img_path), ai_score,
        )
        if img_path:
            return _safe_tweet_with_media(text, img_path, source="tweet_bist30_news", force_send=True)
        else:
            return _safe_tweet(text, source="tweet_bist30_news", force_send=True)
    except Exception as e:
        logger.error(f"tweet_bist50_news hatasi: {e}")
        return False


# ================================================================
# 12. SON GUN SABAH TWEETI (05:00 — hafif uyari tonu)
# ================================================================
def tweet_last_day_morning(ipo) -> bool:
    """Son gun sabahi 05:00'da hafif uyari tonunda tweet.

    Kirmizi degil, turuncu/sari uyari tonu — bilgilendirici.
    Son 30 dk kala hatirlatma atilacagini da belirtir.
    """
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Son Gün Sabah"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Bitis saatini belirle
        end_hour = "17:00"
        if ipo.subscription_hours:
            parts = str(ipo.subscription_hours).split("-")
            if len(parts) >= 2:
                end_hour = parts[-1].strip()

        price_text = f"\n💰 Fiyat: {ipo.ipo_price} TL" if ipo.ipo_price else ""

        text = (
            f"{_get_setting('T12_BASLIK')}\n\n"
            f"{ipo.company_name}{ticker_text} için halka arz başvuruları"
            f" bugün saat {end_hour}'a kadar devam ediyor."
            f"{price_text}\n\n"
            f"{_get_setting('T12_CTA')}\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #yatırım"
        )
        return _safe_tweet_with_media(text, BANNER_SON_BASVURU_GUNU)
    except Exception as e:
        logger.error(f"tweet_last_day_morning hatasi: {e}")
        return False


# ================================================================
# 13. SIRKET TANITIM TWEETI (ertesi gun 20:00 — izahname sonrasi)
# ================================================================
def tweet_company_intro(ipo) -> bool:
    """Taslak izahname aciklandiktan sonra ertesi gun 20:00'de
    sirket tanitim tweeti — samimi, bilgilendirici ton.

    IPO.company_description'dan ilk paragrafı alir.
    Cok uzunsa son cumleyi kirpar (cumle bazli truncation).
    """
    try:
        # DB flag kontrolu — zaten atilmissa tekrar atma
        if getattr(ipo, "intro_tweeted", False):
            logger.debug("tweet_company_intro ATLANDI: %s — zaten atilmis (intro_tweeted=True)", getattr(ipo, "company_name", "?"))
            return False

        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Şirket Tanıtım"):
            return False

        # Sirket bilgisi yoksa tweet atma — bos tweet atmanin anlami yok
        if not ipo.company_description and not ipo.sector and not ipo.ipo_price:
            logger.info(
                "tweet_company_intro ATLANDI: %s — sirket bilgisi (description/sector/price) henuz yok",
                ipo.company_name,
            )
            return False

        # company_name temizligi — SPK bultenden gelen \n ve fazla bosluklari temizle
        clean_name = " ".join(ipo.company_name.replace("\n", " ").replace("\r", " ").split())

        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Sektor bilgisi
        sector_text = ""
        if ipo.sector:
            sector_text = f"\n🏭 Sektör: {ipo.sector}"

        # SPK onay bilgisi (sektor/fiyat yoksa en azindan bu bilgiyi gosterelim)
        spk_text = ""
        if ipo.spk_approval_date and not ipo.sector and not ipo.ipo_price:
            spk_text = f"\n📅 SPK Onay: {ipo.spk_approval_date.strftime('%d.%m.%Y') if hasattr(ipo.spk_approval_date, 'strftime') else ipo.spk_approval_date}"
            if ipo.spk_bulletin_no:
                spk_text += f" (Bülten {ipo.spk_bulletin_no})"

        # Fon kullanim yerleri — varsa tweette ONCE goster (sirket tanitiminin onunde)
        fund_usage_text = ""
        raw_fund = getattr(ipo, "fund_usage", None)
        if raw_fund:
            try:
                import json as _json
                fu = _json.loads(raw_fund) if isinstance(raw_fund, str) else raw_fund
                if isinstance(fu, list) and fu:
                    lines = []
                    for item in fu[:5]:  # max 5 madde
                        if isinstance(item, dict):
                            oran = item.get("oran") or item.get("rate") or item.get("percentage") or ""
                            hedef = item.get("hedef") or item.get("target") or item.get("description") or ""
                            if hedef:
                                lines.append(f"• %{oran} {hedef}" if oran else f"• {hedef}")
                        elif isinstance(item, str) and item.strip():
                            lines.append(f"• {item.strip()}")
                    if lines:
                        fund_usage_text = "\n\n💼 Fon Kullanım Yerleri:\n" + "\n".join(lines)
                elif isinstance(fu, str) and fu.strip():
                    fund_usage_text = f"\n\n💼 Fon Kullanım Yerleri:\n{fu.strip()}"
            except Exception:
                pass  # JSON parse hatasi — sessizce atla

        # Sirket aciklamasi — paragraf gecisleri korunarak ilk 1-2 paragraf
        desc_text = ""
        if ipo.company_description:
            full_desc = str(ipo.company_description).strip()
            # Paragraf ayirici olarak \n\n kullan (baslik ile metin arasindaski)
            # Hem \n\n hem de tek \n ile ayrilmis olabilir, ikisini de destekle
            raw_paragraphs = [p.strip() for p in full_desc.split("\n\n") if p.strip()]
            if not raw_paragraphs:
                raw_paragraphs = [p.strip() for p in full_desc.split("\n") if p.strip()]

            # Tweette en fazla ilk 2 paragraf — baslik varsa o da dahil
            selected = raw_paragraphs[:2]
            tweet_desc = "\n\n".join(selected)

            # Max 900 karakter — fund_usage eklenince toplam uzunluk asmasın
            max_desc_len = 900
            if len(tweet_desc) > max_desc_len:
                tweet_desc = tweet_desc[:max_desc_len - 3] + "..."

            desc_text = f"\n\n{tweet_desc}"

        price_text = ""
        if ipo.ipo_price:
            price_text = f"\n💰 Halka arz fiyatı: {ipo.ipo_price} TL"

        text = (
            f"{_get_setting('T13_BASLIK')}\n\n"
            f"{clean_name}{ticker_text}"
            f"{spk_text}{sector_text}{price_text}"
            f"{fund_usage_text}"
            f"{desc_text}\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #{ipo.ticker or 'borsa'} #yatırım"
        )

        return _safe_tweet_with_media(text, BANNER_HALKA_ARZ_HAKKINDA)
    except Exception as e:
        logger.error(f"tweet_company_intro hatasi: {e}")
        return False


# ================================================================
# 14. SPK BEKLEYENLER GORSELLI TWEET (her ayin 1'i)
# ================================================================
def tweet_spk_pending_with_image(pending_count: int, image_path: str = None) -> bool:
    """Her ayin 1'inde SPK onayi bekleyenler gorselli tweet.

    image_path: Yerel gorsel dosya yolu (opsiyonel).
    Gorsel varsa media upload yapilir, yoksa sadece metin atilir.
    """
    try:
        text = (
            f"📊 SPK Onay Bekleyenler\n\n"
            f"Şu an {pending_count} şirket SPK onayı beklemektedir.\n\n"
            f"Güncel listeyi 📲 {HALKAARZ_BEKLEYENLER_LINK}\n"
            f"sitesinden ve uygulamamızdan takip edebilirsiniz.\n"
            f"#HalkaArz #SPK #BIST100 #borsa #yatırım"
        )

        if image_path:
            return _safe_tweet_with_media(text, image_path)
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_spk_pending_with_image hatasi: {e}")
        return False


def _safe_tweet_with_media(text: str, image_path: str, source: str = "unknown", force_send: bool = False) -> bool:
    """Gorsel + metin tweeti atar.

    TWITTER_AUTO_SEND=False iken tweet kuyruğa eklenir (admin onay bekler).
    force_send=True ise auto_send kontrolunu atlar (admin onay'dan gonderim icin).

    1. Twitter v1.1 media/upload ile gorseli yukle → media_id al
    2. Twitter v2 tweets ile tweet at (media_ids ekleyerek)
    """
    try:
        # KILL SWITCH — admin panelden tüm tweetler durduruldu
        if not force_send and is_tweets_killed():
            logger.warning("[TWEET KILL SWITCH] Tweet+media durduruldu: %s", text[:60])
            return False

        # Onay modu — kuyruğa ekle, direkt atma (DB'den okunur, restart'a dayanıklı)
        if not force_send and not is_auto_send():
            import inspect
            caller = inspect.stack()[1].function if source == "unknown" else source
            # /tmp goruntusu deploy/restart ile silinir — kalici dizine kopyala
            if image_path and os.path.exists(image_path) and image_path.startswith(tempfile.gettempdir()):
                persist_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "tmp")
                os.makedirs(persist_dir, exist_ok=True)
                persist_path = os.path.join(persist_dir, os.path.basename(image_path))
                import shutil
                shutil.copy2(image_path, persist_path)
                image_path = persist_path
                logger.info("Kuyruk modu: gorsel kalici dizine kopyalandi: %s", persist_path)
            return _queue_tweet(text, image_path=image_path, source=caller)

        # Duplicate kontrolu — ayni tweeti 24 saat icinde tekrar atma
        # Zaten atilmis tweet "basarisiz" degil, atlanmali → True don
        if _is_duplicate_tweet(text):
            return True

        creds = _load_credentials()
        if not creds:
            logger.info(f"[TWITTER-DRY-RUN-MEDIA] {text[:60]}... (image={image_path})")
            return False

        if not os.path.exists(image_path):
            logger.warning(f"Gorsel bulunamadi: {image_path}, sadece metin atiliyor")
            return _safe_tweet(text, source=source, force_send=force_send)

        # ── Rate limit: dakikada max 3 tweet ──
        wait = _wait_for_tweet_rate_limit()
        if wait > 0:
            time.sleep(wait)

        # 1. Media Upload (v1.1 — multipart/form-data)
        upload_url = "https://upload.twitter.com/1.1/media/upload.json"

        # OAuth header — upload icin ozel
        oauth_params = {
            "oauth_consumer_key": creds["api_key"],
            "oauth_nonce": uuid.uuid4().hex,
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": str(int(time.time())),
            "oauth_token": creds["access_token"],
            "oauth_version": "1.0",
        }
        signature = _generate_oauth_signature(
            "POST", upload_url, oauth_params,
            creds["api_secret"], creds["access_token_secret"],
        )
        oauth_params["oauth_signature"] = signature
        header_parts = ", ".join(
            f'{k}="{urllib.parse.quote(v, safe="")}"'
            for k, v in sorted(oauth_params.items())
        )
        auth_header = f"OAuth {header_parts}"

        # Twitter media upload limiti ~5MB — buyuk PNG'leri otomatik kucult
        _upload_path = image_path
        try:
            file_size = os.path.getsize(image_path)
            if file_size > 4_800_000:  # 4.8 MB ustu → compress
                from PIL import Image as PILImage
                import io
                with PILImage.open(image_path) as img:
                    # RGBA → RGB cevir (JPEG icin gerekli)
                    if img.mode in ("RGBA", "P"):
                        img = img.convert("RGB")
                    buf = io.BytesIO()
                    img.save(buf, format="JPEG", quality=85, optimize=True)
                    buf.seek(0)
                    _upload_path = image_path + ".compressed.jpg"
                    with open(_upload_path, "wb") as cf:
                        cf.write(buf.getvalue())
                    logger.info("Banner compress: %.1f MB → %.1f MB (%s)",
                                file_size / 1_000_000, os.path.getsize(_upload_path) / 1_000_000, _upload_path)
        except Exception as comp_err:
            logger.warning("Banner compress hatasi (orijinal kullanilacak): %s", comp_err)
            _upload_path = image_path

        with open(_upload_path, "rb") as f:
            mime_type = "image/jpeg" if _upload_path.endswith(".jpg") else "image/png"
            fname = "image.jpg" if _upload_path.endswith(".jpg") else "image.png"
            files = {"media": (fname, f, mime_type)}
            upload_resp = httpx.post(
                upload_url,
                files=files,
                headers={"Authorization": auth_header},
                timeout=30.0,
            )

        # Gecici compress dosyasini temizle
        if _upload_path != image_path and os.path.exists(_upload_path):
            try:
                os.remove(_upload_path)
            except OSError:
                pass

        if upload_resp.status_code not in (200, 201):
            logger.error(f"Media upload hatasi ({upload_resp.status_code}): {upload_resp.text[:200]}")
            return _safe_tweet(text, source=source, force_send=force_send)  # Gorsel basarisiz → sadece metin at

        media_id = upload_resp.json().get("media_id_string")
        if not media_id:
            logger.error("Media upload: media_id alinamadi")
            return _safe_tweet(text, source=source, force_send=force_send)

        logger.info(f"Media upload basarili: media_id={media_id}")

        # 2. Tweet at — media_ids ile
        if len(text) > 4000:
            text = text[:3997] + "..."

        tweet_auth = _build_oauth_header(creds)
        tweet_resp = httpx.post(
            _TWITTER_TWEET_URL,
            json={
                "text": text,
                "media": {"media_ids": [media_id]},
            },
            headers={
                "Authorization": tweet_auth,
                "Content-Type": "application/json",
            },
            timeout=15.0,
        )

        if tweet_resp.status_code in (200, 201):
            tweet_id = tweet_resp.json().get("data", {}).get("id", "?")
            logger.info(f"Gorselli tweet basarili (id={tweet_id}): {text[:60]}...")
            _mark_tweet_sent(text)
            _record_tweet_sent()
            global _last_tweet_id
            _last_tweet_id = tweet_id
            return True
        else:
            error_msg = f"HTTP {tweet_resp.status_code}: {tweet_resp.text[:200]}"
            logger.error(f"Gorselli tweet hatasi: {error_msg}")
            _notify_tweet_failure(text, f"[MEDIA] {error_msg}")
            return False

    except Exception as e:
        logger.error(f"Gorselli tweet hatasi (sistem etkilenmez): {e}")
        _notify_tweet_failure(text, f"[MEDIA] {str(e)}")
        return False


def _safe_tweet_with_multi_media(text: str, image_paths: list[str], source: str = "unknown", force_send: bool = False) -> bool:
    """Birden fazla gorsel ile tek tweet atar (Twitter max 4 gorsel destekler).

    Her gorseli ayri ayri upload eder, hepsinin media_id'sini tek tweette gonderir.
    """
    if not image_paths:
        return _safe_tweet(text, source=source, force_send=force_send)
    if len(image_paths) == 1:
        return _safe_tweet_with_media(text, image_paths[0], source=source, force_send=force_send)

    try:
        # KILL SWITCH
        if not force_send and is_tweets_killed():
            logger.warning("[TWEET KILL SWITCH] Tweet+multi-media durduruldu: %s", text[:60])
            return False

        # Onay modu — ilk gorselle kuyruğa ekle (multi-media kuyruk desteği yok)
        if not force_send and not is_auto_send():
            return _safe_tweet_with_media(text, image_paths[0], source=source, force_send=False)

        # Duplicate kontrolu
        if _is_duplicate_tweet(text):
            return True

        creds = _load_credentials()
        if not creds:
            logger.info(f"[TWITTER-DRY-RUN-MULTI] {text[:60]}... (images={len(image_paths)})")
            return False

        # Rate limit
        wait = _wait_for_tweet_rate_limit()
        if wait > 0:
            time.sleep(wait)

        # Her gorseli upload et
        media_ids = []
        for img_path in image_paths[:4]:  # Twitter max 4
            if not os.path.exists(img_path):
                logger.warning(f"Multi-media: gorsel bulunamadi, atlaniyor: {img_path}")
                continue

            upload_url = "https://upload.twitter.com/1.1/media/upload.json"
            oauth_params = {
                "oauth_consumer_key": creds["api_key"],
                "oauth_nonce": uuid.uuid4().hex,
                "oauth_signature_method": "HMAC-SHA1",
                "oauth_timestamp": str(int(time.time())),
                "oauth_token": creds["access_token"],
                "oauth_version": "1.0",
            }
            signature = _generate_oauth_signature(
                "POST", upload_url, oauth_params,
                creds["api_secret"], creds["access_token_secret"],
            )
            oauth_params["oauth_signature"] = signature
            header_parts = ", ".join(
                f'{k}="{urllib.parse.quote(v, safe="")}"'
                for k, v in sorted(oauth_params.items())
            )
            auth_header = f"OAuth {header_parts}"

            # Buyuk dosya compress
            _upload_path = img_path
            try:
                file_size = os.path.getsize(img_path)
                if file_size > 4_800_000:
                    from PIL import Image as PILImage
                    import io
                    with PILImage.open(img_path) as img:
                        if img.mode in ("RGBA", "P"):
                            img = img.convert("RGB")
                        buf = io.BytesIO()
                        img.save(buf, format="JPEG", quality=85, optimize=True)
                        buf.seek(0)
                        _upload_path = img_path + ".compressed.jpg"
                        with open(_upload_path, "wb") as cf:
                            cf.write(buf.getvalue())
            except Exception:
                _upload_path = img_path

            with open(_upload_path, "rb") as f:
                mime_type = "image/jpeg" if _upload_path.endswith(".jpg") else "image/png"
                fname = "image.jpg" if _upload_path.endswith(".jpg") else "image.png"
                files = {"media": (fname, f, mime_type)}
                upload_resp = httpx.post(upload_url, files=files, headers={"Authorization": auth_header}, timeout=30.0)

            # Gecici compress dosyasini temizle
            if _upload_path != img_path and os.path.exists(_upload_path):
                try:
                    os.remove(_upload_path)
                except OSError:
                    pass

            if upload_resp.status_code in (200, 201):
                mid = upload_resp.json().get("media_id_string")
                if mid:
                    media_ids.append(mid)
                    logger.info(f"Multi-media upload basarili: media_id={mid} ({img_path})")
            else:
                logger.warning(f"Multi-media upload hatasi ({upload_resp.status_code}): {upload_resp.text[:150]}")

        if not media_ids:
            logger.warning("Hic gorsel yuklenemedi, sadece metin tweet atiliyor")
            return _safe_tweet(text, source=source, force_send=force_send)

        # Tweet at — tum media_ids ile
        if len(text) > 4000:
            text = text[:3997] + "..."

        tweet_auth = _build_oauth_header(creds)
        tweet_resp = httpx.post(
            _TWITTER_TWEET_URL,
            json={
                "text": text,
                "media": {"media_ids": media_ids},
            },
            headers={
                "Authorization": tweet_auth,
                "Content-Type": "application/json",
            },
            timeout=15.0,
        )

        if tweet_resp.status_code in (200, 201):
            tweet_id = tweet_resp.json().get("data", {}).get("id", "?")
            logger.info(f"Multi-media tweet basarili (id={tweet_id}, {len(media_ids)} gorsel): {text[:60]}...")
            _mark_tweet_sent(text)
            _record_tweet_sent()
            global _last_tweet_id
            _last_tweet_id = tweet_id
            return True
        else:
            error_msg = f"HTTP {tweet_resp.status_code}: {tweet_resp.text[:200]}"
            logger.error(f"Multi-media tweet hatasi: {error_msg}")
            _notify_tweet_failure(text, f"[MULTI-MEDIA] {error_msg}")
            return False

    except Exception as e:
        logger.error(f"Multi-media tweet hatasi: {e}")
        _notify_tweet_failure(text, f"[MULTI-MEDIA] {str(e)}")
        return False


# ================================================================
# YARDIMCI FONKSIYONLAR
# ================================================================

def _get_turkish_month(month: int) -> str:
    """Ay numarasini Turkce ay adina cevirir."""
    months = {
        1: "Ocak", 2: "Şubat", 3: "Mart", 4: "Nisan",
        5: "Mayıs", 6: "Haziran", 7: "Temmuz", 8: "Ağustos",
        9: "Eylül", 10: "Ekim", 11: "Kasım", 12: "Aralık",
    }
    return months.get(month, "")


# ================================================================
# Telegram Mesaj Sablonu — SPK Onayi (admin_telegram'dan cagrilir)
# ================================================================
def format_spk_approval_telegram(company_name: str, bulletin_no: str, price: str = "") -> str:
    """SPK onayi icin Telegram mesaj sablonu."""
    price_line = f"\n💰 Halka arz fiyatı: {price} TL" if price else ""
    return (
        f"🚨 <b>SPK Bülteni Yayımlandı!</b>\n\n"
        f"<b>{company_name}</b> için halka arz başvurusu SPK tarafından onaylandı."
        f"{price_line}\n\n"
        f"📋 Bülten No: {bulletin_no}\n\n"
        f"📲 Bilgiler geldikçe bildirim göndereceğiz.\n"
        f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
        f"#HalkaArz #BIST100 #borsa #yatırım"
    )


# ================================================================
# 15. OGLE ARASI MARKET SNAPSHOT (14:00 — tum islem goren hisseler)
# ================================================================
def tweet_market_snapshot(snapshot_data: list, image_path: str) -> bool:
    """Saat 14:00 ogle arasi market snapshot tweeti.

    Dinamik PNG gorsel + kisa ozet metin.

    Args:
        snapshot_data: Her hisse icin dict listesi
            [{ticker, trading_day, close_price, pct_change, durum, ...}]
        image_path: generate_market_snapshot_image() ciktisi
    """
    try:
        if not snapshot_data or not image_path:
            return False

        count = len(snapshot_data)
        tavan_count = sum(1 for s in snapshot_data if s.get("durum") == "tavan")
        taban_count = sum(1 for s in snapshot_data if s.get("durum") == "taban")
        normal_count = count - tavan_count - taban_count

        # Her hisse icin 2 satirlik blok (ticker+gun / pct), aralarinda bos satir
        blocks = []
        for s in snapshot_data:
            pct = float(s.get("pct_change", 0))
            blocks.append(f"#{s['ticker']}  {s['trading_day']}. Gün/25\n%{pct:+.1f}")
        hisse_satiri = "\n\n".join(blocks)

        text = (
            f"{_get_setting('T15_BASLIK')} — {count} Hisse\n\n"
            f"{hisse_satiri}\n\n"
            f"Tavan: {tavan_count} | Taban: {taban_count} | Normal: {normal_count}\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #borsa #hisse"
        )

        # Kuyruk modunda temp dosyayi silme
        auto_send = is_auto_send()

        result = _safe_tweet_with_media(text, image_path)

        # Temp dosya temizligi — sadece auto_send modunda
        if auto_send:
            try:
                os.remove(image_path)
            except OSError:
                pass

        return result
    except Exception as e:
        logger.error(f"tweet_market_snapshot hatasi: {e}")
        return False


# ================================================================
# 16. YENI HALKA ARZLAR ACILIS BILGILERI (Excel sync sonrasi)
# ================================================================
def tweet_opening_summary(stocks: list) -> bool:
    """Ilk 5 gun icindeki hisselerin acilis bilgilerini tweet atar.

    Excel sync bittiginde /admin/trigger-opening-tweet endpoint'i calistirir.

    Args:
        stocks: [
            {
                "ticker": "ASELS",
                "company_name": "Aselsan A.Ş.",
                "trading_day": 3,
                "ipo_price": 38.00,
                "open_price": 42.50,
                "pct_change": +11.8,
                "durum": "tavan",
                "ceiling_days": 2,
                "floor_days": 0,
                "normal_days": 1,
            }
        ]
    """
    try:
        if not stocks:
            logger.info("tweet_opening_summary: Ilk 5 gun icinde hisse yok, tweet atilmadi.")
            return False

        # Gorsel olustur
        from app.services.chart_image_generator import generate_opening_summary_image
        image_path = generate_opening_summary_image(stocks)

        # Tweet metni — lot + daily % bilgili
        lines = []
        for s in stocks:
            daily_pct = float(s.get("daily_pct", s.get("pct_change", 0)))
            emoji = "\U0001F7E2" if daily_pct >= 0 else "\U0001F534"
            durum = s.get("durum", "")
            durum_tag = ""
            if durum == "tavan":
                durum_tag = " TAVAN"
            elif durum == "taban":
                durum_tag = " TABAN"

            line = (
                f"{emoji} #{s['ticker']} {s['trading_day']}. Gün | "
                f"Açılış: {float(s['open_price']):.2f} TL | "
                f"%{daily_pct:+.1f}{durum_tag}"
            )
            lines.append(line)

        text = (
            f"{_get_setting('T16_BASLIK')}\n\n"
            + "\n".join(lines) + "\n\n"
            f"Daha detaylı bilgiler için 📲 {HALKAARZ_LINK}\n"
            f"#HalkaArz #BIST100 #borsa #hisse"
        )

        # Kuyruk modunda temp dosyayi silme
        auto_send = is_auto_send()

        result = _safe_tweet_with_media(text, image_path) if image_path else _safe_tweet(text)

        # Temp dosya temizligi
        if auto_send and image_path:
            try:
                os.remove(image_path)
            except OSError:
                pass

        return result
    except Exception as e:
        logger.error(f"tweet_opening_summary hatasi: {e}")
        return False


# ================================================================
# 17. SPK BULTEN ANALIZ TWEETI (AI ile kapsamli bulten analizi)
# ================================================================

BANNER_SPK_BULTEN_ANALIZ = os.path.join(_IMG_DIR, "spk_bulten_analiz.png")

# Abacus AI sabitleri (ai_market_report.py ile ayni)
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_BULLETIN_AI_MODEL = "gpt-4.1"

# Anthropic Claude Sonnet 4 — 2. yedek (direkt API)
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Gemini 2.5 Pro — 3. yedek (OpenAI uyumlu endpoint)
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-pro"

_BULLETIN_ANALYSIS_SYSTEM_PROMPT = """Sen deneyimli bir SPK bülten analistisin. Verilen SPK bülteni içeriğini analiz edip, yatırımcıları ilgilendiren önemli kararları özetleyeceksin.

TWEET FORMATI:
- Türkçe, sade, akıcı ve bilgilendirici cümleler kur
- Emoji kullan ama aşırı değil
- Max 3500 karakter
- Her madde kısa, net ve anlaşılır olsun

DAHİL ET (önem sırasına göre):
1. Halka Arz Onayları — varsa şirket adı, sermaye artırım tutarı, pay satış tutarı ve fiyat bilgisi. Her şirketi TEK BİR KERE yaz. Yoksa "Bu bültende yeni halka arz onayı bulunmuyor" yaz.
2. Bedelli sermaye artırımları — şirket adı + artırım miktarı (varsa)
3. Bedelsiz sermaye artırımları — şirket adı + oran (varsa)
4. Şirketlere verilen önemli idari para cezaları (İPC) — şirket adı + ceza tutarı + kısa neden (orn: piyasa bozucu eylem, yanıltıcı bilgi vb.)

ÖNEMLİ:
- "Halka Açık Ortaklıkların Pay İhraçları" bölümünü AYRICA YAZMA — halka arz onayları bölümünde zaten yer alıyor, tekrar etme.
- Her şirket sadece 1 kez geçsin, aynı bilgiyi farklı başlıklar altında tekrarlama.

KESİNLİKLE HARİÇ TUT (bunları YAZMA):
- Eurobond ihraçları
- Borsada işlem yasağı / site yasakları
- Fon yöneticisi veya gerçek kişi bazlı cezalar (sadece ŞİRKET bazlı cezalar dahil)
- Borçlanma araçları
- Gayrimenkul sertifikaları / Kira sertifikaları
- Yatırım fonu kuruluş/tadil işlemleri
- Portföy yönetim şirketlerinin rutin işlemleri

FORMAT KURALLARI:
- Borsada işlemi olan şirketlerin ticker sembollerini biliyorsan #TICKER formatında kullan
- Bilmediğin ticker'ı UYDURMA, sadece şirket adını yaz
- Her bölümü emoji + başlık ile ayır (örn: 📊 Sermaye Artırımları)
- Bültende ilgili içerik YOKSA o bölümü hiç yazma (boş bölüm olmasın)
- Cümleleri düzgün kur, madde işareti kullanırken bile anlaşılır ifadeler yaz
- Uydurmaya GEREK YOK — sadece bültendeki verileri kullan"""


def _generate_bulletin_analysis_sync(bulletin_text: str, bulletin_no: str) -> str | None:
    """AI ile bulten icerigini analiz eder, tweet metni uretir (senkron).
    Sirasi: Abacus AI → Claude Sonnet → Gemini Pro."""
    try:
        from app.config import get_settings
        settings = get_settings()
        api_key = settings.ABACUS_API_KEY
        anthropic_key = getattr(settings, "ANTHROPIC_API_KEY", None) or None
        gemini_key = settings.GEMINI_API_KEY if settings.GEMINI_API_KEY else None

        if not api_key and not anthropic_key and not gemini_key:
            logger.error("SPK bulten analiz: API key yok (Abacus/Claude/Gemini)")
            return None

        user_message = (
            f"SPK Bulteni {bulletin_no} icerigini analiz et.\n"
            f"SADECE bultendeki GERCEK verilere dayan, hicbir bilgiyi UYDURMA.\n\n"
            f"--- BULTEN ICERIGI ---\n{bulletin_text}"
        )

        messages = [
            {"role": "system", "content": _BULLETIN_ANALYSIS_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]
        payload_base = {
            "messages": messages,
            "temperature": 0.3,
            "max_tokens": 8192,  # Gemini 2.5 thinking token yiyor
        }

        content = None

        # ── 1. Birincil: Abacus AI ──
        if api_key:
            try:
                resp = httpx.post(
                    _ABACUS_URL,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json={**payload_base, "model": _BULLETIN_AI_MODEL},
                    timeout=60.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    if content:
                        logger.info("SPK bulten AI [Abacus] analiz uretildi: %d karakter", len(content))
                else:
                    logger.warning("SPK bulten Abacus hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
            except Exception as e:
                logger.warning("SPK bulten Abacus hata: %s", e)

        # ── 2. Yedek: Anthropic Claude Sonnet 4 ──
        if not content and anthropic_key:
            try:
                resp = httpx.post(
                    _ANTHROPIC_URL,
                    headers={
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": _CLAUDE_MODEL,
                        "max_tokens": 8192,  # Gemini 2.5 thinking token yiyor
                        "system": _BULLETIN_ANALYSIS_SYSTEM_PROMPT,
                        "messages": [{"role": "user", "content": user_message}],
                        "temperature": 0.3,
                    },
                    timeout=60.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for block in data.get("content", []):
                        if block.get("type") == "text":
                            content = block.get("text", "").strip()
                            break
                    if content:
                        logger.info("SPK bulten AI [Claude-Sonnet] analiz uretildi: %d karakter", len(content))
                else:
                    logger.warning("SPK bulten Claude hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
            except Exception as e:
                logger.warning("SPK bulten Claude hata: %s", e)

        # ── 3. Yedek: Gemini 2.5 Pro ──
        if not content and gemini_key:
            try:
                resp = httpx.post(
                    _GEMINI_URL,
                    headers={
                        "Authorization": f"Bearer {gemini_key}",
                        "Content-Type": "application/json",
                    },
                    json={**payload_base, "model": _GEMINI_MODEL},
                    timeout=60.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    if content:
                        logger.info("SPK bulten AI [Gemini-Pro] analiz uretildi: %d karakter", len(content))
                else:
                    logger.error("SPK bulten Gemini hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
            except Exception as e:
                logger.error("SPK bulten Gemini hata: %s", e)

        if not content:
            logger.error("SPK bulten AI: Tum providerlar basarisiz")
            return None

        # Max 3500 karakter (link + hashtag icin bosluk birak)
        if len(content) > 3500:
            content = content[:3497] + "..."

        return content.strip()

    except Exception as e:
        logger.error("SPK bulten AI analiz hatasi: %s", e)
        return None


def tweet_spk_bulletin_analysis(bulletin_text: str, bulletin_no: str) -> bool:
    """SPK bulten analiz tweetini atar — AI analiz + gorsel + metin.

    Mevcut halka arz tweetinden BAGIMSIZ calisir.
    Bultenin tamami AI ile analiz edilip onemli kararlar ozetlenir.

    Args:
        bulletin_text: format_tables_for_analysis() ciktisi
        bulletin_no: Bulten numarasi (orn: "2026/8")
    """
    try:
        if not bulletin_text or len(bulletin_text) < 50:
            logger.warning("SPK bulten analiz: Bulten icerigi cok kisa (%d char), tweet atilmadi",
                           len(bulletin_text) if bulletin_text else 0)
            return False

        # 1. AI analiz uret
        ai_text = _generate_bulletin_analysis_sync(bulletin_text, bulletin_no)
        if not ai_text:
            logger.warning("SPK bulten analiz: AI metin uretilemedi, tweet atilmadi")
            return False

        # 2. Tweet metnini olustur
        text = (
            f"📋 SPK Bülteni {bulletin_no} — Analiz\n\n"
            f"{ai_text}\n\n"
            f"📲 {APP_LINK}\n"
            f"#SPK #BultenAnaliz #HalkaArz #BIST100 #borsa"
        )

        # 4000 karakter Twitter limiti
        if len(text) > 3950:
            # AI metnini kirp
            max_ai = 3950 - len(text) + len(ai_text) - 10
            ai_text = ai_text[:max_ai] + "..."
            text = (
                f"📋 SPK Bülteni {bulletin_no} — Analiz\n\n"
                f"{ai_text}\n\n"
                f"📲 {APP_LINK}\n"
                f"#SPK #BultenAnaliz #HalkaArz #BIST100 #borsa"
            )

        # 3. Gorselli tweet at
        if os.path.exists(BANNER_SPK_BULTEN_ANALIZ):
            return _safe_tweet_with_media(text, BANNER_SPK_BULTEN_ANALIZ, source="tweet_spk_bulletin_analysis")
        else:
            logger.warning("SPK bulten analiz: Gorsel bulunamadi (%s), sadece metin", BANNER_SPK_BULTEN_ANALIZ)
            return _safe_tweet(text, source="tweet_spk_bulletin_analysis")

    except Exception as e:
        logger.error("tweet_spk_bulletin_analysis hatasi: %s", e)
        return False


# ================================================================
# 17. İZAHNAME ANALİZ TWEETİ (AI prospektüs analizi — görsel)
# ================================================================
def tweet_izahname_analysis(ipo, analysis: dict, img_path: str) -> bool:
    """İzahname AI analizi tamamlandığında görsel + metin tweeti atar.

    Tweet formatı: Şirket tanıtımı (3-4 cümle) + hashtag'ler.
    Olumlu/olumsuz detaylar resimde zaten gösteriliyor — tweet'te tekrarlanmaz.

    analysis: {"company_brief": str, "positives": [...], "negatives": [...],
               "summary": str, "risk_level": str, "key_risk": str}
    """
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "İzahname Analizi"):
            return False

        # 0 bulgu kontrolü — analiz boş/başarısızsa tweet atma
        pos = analysis.get("positives", [])
        neg = analysis.get("negatives", [])
        if len(pos) == 0 and len(neg) == 0:
            logger.warning(
                "tweet_izahname_analysis: %s — 0 olumlu + 0 olumsuz bulgu, tweet atılmıyor",
                ipo.company_name,
            )
            return False

        # Şirket adı ve ticker hashtag
        clean_name = " ".join(ipo.company_name.replace("\n", " ").replace("\r", " ").split())
        ticker = ipo.ticker or ""
        ticker_hashtag = f"#{ticker}" if ticker else ""

        # Risk seviyesi emoji
        risk_level = analysis.get("risk_level", "ORTA").upper()
        risk_emoji = {
            "DÜŞÜK": "🟢", "ORTA": "🟡",
            "YÜKSEK": "🔴", "ÇOK YÜKSEK": "🔴",
        }.get(risk_level, "🟡")

        # Şirket tanıtım metni — AI'dan gelen company_brief, yoksa summary kullan
        company_brief = analysis.get("company_brief", "")
        if not company_brief or len(company_brief) < 20:
            company_brief = analysis.get("summary", "")
        # Yarım cümle koruması — son cümle nokta ile bitmiyorsa kırp
        if company_brief and not company_brief.rstrip().endswith((".","!","?")):
            last_dot = company_brief.rfind(".")
            if last_dot > 30:
                company_brief = company_brief[:last_dot + 1]

        # Halka arz fiyatı (varsa)
        price_line = f"💰 Halka arz fiyatı: {ipo.ipo_price} TL\n" if ipo.ipo_price else ""

        # Olumlu/Olumsuz detay sayıları
        pos_count = len(analysis.get("positives", []))
        neg_count = len(analysis.get("negatives", []))
        total_details = pos_count + neg_count
        details_line = f"✅ Olumlu: {pos_count}  ❌ Olumsuz: {neg_count}  📊 Toplam {total_details} detay\n"

        def _build_text(brief_str):
            header = f"📋 {ticker_hashtag} #İzahname Analizi" if ticker_hashtag else f"📋 {clean_name} #İzahname Analizi"
            return (
                f"{header}\n\n"
                f"🏢 {clean_name}\n"
                f"{price_line}"
                f"{risk_emoji} Risk: {risk_level.title()}\n"
                f"{details_line}\n"
                f"{brief_str}\n\n"
                f"⚠️ Yatırım tavsiyesi değildir.\n"
                f"📲 {_get_setting('APP_LINK')}\n\n"
                f"#HalkaArz {ticker_hashtag} #borsa #BIST #yatırım #hisse"
            )

        text = _build_text(company_brief)

        # 3950 karakter limiti — gerekirse brief kırp
        if len(text) > 3950:
            max_brief = 3950 - (len(text) - len(company_brief)) - 10
            company_brief = company_brief[:max(max_brief, 60)]
            # Yarım cümle olmasın
            last_dot = company_brief.rfind(".")
            if last_dot > 30:
                company_brief = company_brief[:last_dot + 1]
            else:
                company_brief = company_brief + "…"
            text = _build_text(company_brief)

        # Görsel varsa medialı tweet, yoksa sadece metin
        import os
        if img_path and os.path.exists(img_path):
            return _safe_tweet_with_media(text, img_path, source="tweet_izahname_analysis")
        else:
            logger.warning(
                "tweet_izahname_analysis: %s — görsel bulunamadı (%s), sadece metin",
                clean_name, img_path,
            )
            return _safe_tweet(text, source="tweet_izahname_analysis")

    except Exception as e:
        logger.error("tweet_izahname_analysis hatasi: %s", e)
        return False


# ================================================================
# 18. SPK BAŞVURU TWEETİ (AI şirket araştırması ile)
# ================================================================

_SPK_APP_BANNER = os.path.join(_IMG_DIR, "spk_basvuru_banner.png")

_SPK_APP_AI_SYSTEM_PROMPT = """Sen Türkiye'deki şirketler, sektörler ve finans piyasası hakkında derin bilgi sahibi bir araştırmacı ve ekonomi editörüsün.

Görevin: SPK'ya halka arz onay başvurusu yapan bir şirket hakkında DETAYLI ve BİLGİLENDİRİCİ bir yazı hazırlamak.

═══ İÇERİK GEREKSİNİMLERİ ═══

Yazında şu bilgileri MUTLAKA içer (bildiklerini):

1. 🏢 Şirket tanıtımı: Ne iş yapıyor, hangi sektörde faaliyet gösteriyor
2. 📍 Merkezi nerede, kaç yıldır faaliyette (biliniyorsa)
3. 🔧 Ana ürün/hizmetleri neler — somut örneklerle açıkla
4. 📊 Sektördeki konumu: Türkiye'de veya bölgesinde kaçıncı büyük, pazar payı vs.
5. 🌐 Varsa önemli müşterileri, projeleri veya iş ortaklıkları
6. 💡 Şirketi ilginç/farklı kılan bir detay (varsa)

═══ FORMAT KURALLARI ═══

- SADECE tweet metnini yaz — açıklama, not, yorum EKLEME
- 120-250 kelime arası (ortalama 150 kelime ideal) — hashtag/link hariç
- 1-2 paragraf, akıcı ve bilgilendirici
- İlk satır MUTLAKA: "📝 SPK Halka Arz Onay Başvurusu" başlığı ile başla
- İkinci satırda şirket adını kalın/net ver
- Türkçe, sade ama detaylı — gazeteci üslubu
- "halka arz onay BAŞVURUSU" ifadesini kullan (bu ONAY değil, başvuru aşaması)
- Yatırım tavsiyesi VERME, sadece bilgilendir
- Bilmediğin bilgiyi UYDURMA — emin olmadığın detayı yazma
- Emoji başlıklarda kullan, metin içinde fazla kullanma

═══ ★ YARIM CÜMLE YASAĞI (ÇOK ÖNEMLİ) ★ ═══

- Her cümle MUTLAKA nokta (.) ile bitmeli — yarım, kopuk, anlamsız cümle ASLA kabul edilmez
- Kelime sayısına ulaşmak için cümleyi uzatma, sığmıyorsa KISALT
- Son cümle de TAM olmalı — "... ve bu şirketin" gibi yarım biten metin YASAK
- Saçma sapan, anlamsız veya tekrarlayan cümle yazma
- Akıcı, düzgün, gazete haberi kalitesinde Türkçe kullan
- Kontrol: Metni okuyunca her cümle kendi başına anlamlı olmalı

═══ ÖRNEK ÇIKTI ═══

📝 SPK Halka Arz Onay Başvurusu

🏢 Tatilbudur Seyahat Acenteliği ve Turizm AŞ

Türkiye'nin en bilinen online seyahat platformlarından biri olan Tatilbudur, SPK'ya halka arz onay başvurusunda bulundu. 2000 yılında kurulan şirket, tatil ve tur paketleri, uçak bileti, otel rezervasyonu ve transfer hizmetleri sunuyor.

Tatilbudur, yıllık milyonlarca kullanıcıya hizmet veren dijital platformuyla Türkiye'nin online seyahat sektörünün öncü isimlerinden. Hem yurt içi hem yurt dışı tatil seçenekleri sunan şirket, web sitesi ve mobil uygulaması üzerinden 7/24 rezervasyon imkânı sağlıyor. Şirketin halka arz başvurusu SPK tarafından değerlendirilecek."""

_SPK_APP_AI_MODEL = "gpt-4.1"


def _generate_spk_app_tweet_ai(company_name: str) -> str | None:
    """AI ile SPK basvuru sirketini arastirip tweet metni uretir (senkron).
    Sirasi: Abacus AI → Claude Sonnet → Gemini Pro."""
    try:
        from app.config import get_settings
        settings = get_settings()
        api_key = settings.ABACUS_API_KEY
        anthropic_key = getattr(settings, "ANTHROPIC_API_KEY", None) or None
        gemini_key = settings.GEMINI_API_KEY if settings.GEMINI_API_KEY else None

        if not api_key and not anthropic_key and not gemini_key:
            logger.error("SPK basvuru tweet AI: API key yok (Abacus/Claude/Gemini)")
            return None

        user_message = (
            f"Şirket: {company_name}\n\n"
            f"Bu şirket SPK'ya halka arz onay başvurusunda bulundu.\n"
            f"Şirket hakkında detaylı araştırma yap ve bilgilendirici tweet hazırla.\n"
            f"1-2 paragraf, 120-250 kelime arası (ortalama 150 kelime), somut bilgilerle zengin bir metin yaz.\n"
            f"SADECE tweet metnini yaz — açıklama veya not ekleme."
        )

        messages = [
            {"role": "system", "content": _SPK_APP_AI_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]
        payload_base = {
            "messages": messages,
            "temperature": 0.4,
            "max_tokens": 8192,  # Gemini 2.5 thinking token yiyor
        }

        content = None

        # ── 1. Birincil: Abacus AI ──
        if api_key:
            try:
                resp = httpx.post(
                    _ABACUS_URL,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json={**payload_base, "model": _SPK_APP_AI_MODEL},
                    timeout=45.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                    if content:
                        logger.info("SPK basvuru AI [Abacus]: %d karakter", len(content))
                else:
                    logger.warning("SPK basvuru Abacus hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
            except Exception as e:
                logger.warning("SPK basvuru Abacus hata: %s", e)

        # ── 2. Yedek: Anthropic Claude Sonnet 4 ──
        if not content and anthropic_key:
            try:
                resp = httpx.post(
                    _ANTHROPIC_URL,
                    headers={
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": _CLAUDE_MODEL,
                        "max_tokens": 8192,  # Gemini 2.5 thinking token yiyor
                        "system": _SPK_APP_AI_SYSTEM_PROMPT,
                        "messages": [{"role": "user", "content": user_message}],
                        "temperature": 0.4,
                    },
                    timeout=45.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for block in data.get("content", []):
                        if block.get("type") == "text":
                            content = block.get("text", "").strip()
                            break
                    if content:
                        logger.info("SPK basvuru AI [Claude-Sonnet]: %d karakter", len(content))
                else:
                    logger.warning("SPK basvuru Claude hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
            except Exception as e:
                logger.warning("SPK basvuru Claude hata: %s", e)

        # ── 3. Yedek: Gemini 2.5 Pro ──
        if not content and gemini_key:
            try:
                resp = httpx.post(
                    _GEMINI_URL,
                    headers={
                        "Authorization": f"Bearer {gemini_key}",
                        "Content-Type": "application/json",
                    },
                    json={**payload_base, "model": _GEMINI_MODEL},
                    timeout=45.0,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
                    if content:
                        logger.info("SPK basvuru AI [Gemini-Pro]: %d karakter", len(content))
                else:
                    logger.error("SPK basvuru Gemini hatasi: HTTP %d — %s", resp.status_code, resp.text[:300])
            except Exception as e:
                logger.error("SPK basvuru Gemini hata: %s", e)

        if not content or len(content) < 50:
            logger.error("SPK basvuru AI: Tum providerlar basarisiz veya kisa metin")
            return None

        # Temizle — AI bazen tirnak isareti veya extra bosluk ekliyor
        content = content.strip('"\'').strip()

        # Max 3500 karakter (hashtag/link icin 500 char birak — Twitter limiti 4000)
        if len(content) > 3500:
            # Son cümle noktasına kadar kes
            cut = content[:3500]
            last_dot = max(cut.rfind('.'), cut.rfind('!'))
            if last_dot > 2000:
                content = cut[:last_dot + 1]
            else:
                content = cut.rstrip() + "…"

        logger.info("SPK basvuru AI tweet uretildi: %d karakter — %s", len(content), company_name)
        return content

    except Exception as e:
        logger.error("SPK basvuru AI tweet hatasi: %s", e)
        return None


def tweet_spk_application(company_name: str) -> bool:
    """SPK'ya halka arz onay basvurusu yapan sirket icin tweet atar.

    AI ile sirket arastirmasi yapar, basarisiz olursa fallback metin kullanir.
    Banner gorseli varsa media'li tweet atar.

    Args:
        company_name: Sirket adi (SPKApplication.company_name)
    """
    try:
        if not company_name or len(company_name.strip()) < 3:
            logger.warning("tweet_spk_application: Gecersiz sirket adi")
            return False

        clean_name = " ".join(company_name.replace("\n", " ").replace("\r", " ").split())

        # AI ile tweet metni uret
        ai_text = _generate_spk_app_tweet_ai(clean_name)

        if ai_text:
            text = ai_text
        else:
            # Fallback — AI basarisiz
            text = (
                f"📝 SPK Halka Arz Başvurusu\n\n"
                f"{clean_name}, SPK'ya halka arz onay başvurusunda bulundu."
            )

        # Hashtag + linkler ekle
        site_link = "https://szalgo.net.tr/halkaarz/onay-bekleyenler"
        app_link = _get_setting("APP_LINK")
        suffix = f"\n\n🔗 {site_link}"
        if app_link:
            suffix += f"\n📲 {app_link}"
        suffix += "\n#HalkaArz #SPK #Borsa"
        text = text + suffix

        # Banner gorseli varsa media'li tweet
        if os.path.exists(_SPK_APP_BANNER):
            return _safe_tweet_with_media(text, _SPK_APP_BANNER, source="tweet_spk_application")
        else:
            return _safe_tweet(text, source="tweet_spk_application")

    except Exception as e:
        logger.error("tweet_spk_application hatasi (%s): %s", company_name, e)
        return False
