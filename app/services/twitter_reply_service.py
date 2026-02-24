"""X (Twitter) AI Reply Servisi — Manuel + Otomatik

Manuel mod: Admin panelden tweet URL gir → AI 3 reply önerisi → admin seçer → gönderir
Otomatik mod: Scheduler 5dk'da bir takip edilen hesapları tarar → AI reply üretir → otomatik atar

Mevcut 14 tweet tipinin otomatik/onay modundan TAMAMEN BAĞIMSIZ.
"""

import asyncio
import json
import logging
import random
import re
import time
import hashlib
import hmac
import base64
import urllib.parse
import uuid
from datetime import datetime, timezone, timedelta

import httpx

logger = logging.getLogger(__name__)

# Abacus AI RouteLLM endpoint (OpenAI compat)
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_AI_MODEL = "gpt-4o"
_AI_TIMEOUT = 25

# Twitter API v2
_TWITTER_TWEET_URL = "https://api.twitter.com/2/tweets"
_TWITTER_TWEET_LOOKUP_URL = "https://api.twitter.com/2/tweets/{tweet_id}"
_TWITTER_USER_LOOKUP_URL = "https://api.twitter.com/2/users/by/username/{username}"
_TWITTER_USER_TWEETS_URL = "https://api.twitter.com/2/users/{user_id}/tweets"
_TWITTER_LIKE_URL = "https://api.twitter.com/2/users/{user_id}/likes"

# Otomatik reply ayarlari
_AUTO_REPLY_DAILY_LIMIT_DEFAULT = 20  # DB'de yoksa bu kullanilir
_AUTO_REPLY_LOCK = asyncio.Lock()

# Jitter araliklari (saniye) — dogal gorunum icin dagitilmis
_JITTER_MIN = 30
_JITTER_MAX = 180  # 3 dakika

# Begeni sikligi: her N reply'da 1 begeni
_LIKE_EVERY_N = 5


# -------------------------------------------------------
# Yardımcı: Credentials
# -------------------------------------------------------

def _get_api_key() -> str | None:
    """Abacus AI API key'i al."""
    try:
        from app.config import get_settings
        key = get_settings().ABACUS_API_KEY
        return key if key else None
    except Exception:
        return None


def _load_credentials() -> dict | None:
    """Twitter API anahtarlarını yükler."""
    try:
        from app.config import get_settings
        settings = get_settings()

        api_key = settings.X_API_KEY
        api_secret = settings.X_API_SECRET
        access_token = settings.X_ACCESS_TOKEN
        access_token_secret = settings.X_ACCESS_TOKEN_SECRET

        if not all([api_key, api_secret, access_token, access_token_secret]):
            logger.warning("Twitter API anahtarları eksik — reply devre dışı")
            return None

        return {
            "api_key": api_key,
            "api_secret": api_secret,
            "access_token": access_token,
            "access_token_secret": access_token_secret,
        }
    except Exception as e:
        logger.error(f"Twitter credentials yüklenemedi: {e}")
        return None


def _generate_oauth_signature(
    method: str,
    url: str,
    oauth_params: dict,
    consumer_secret: str,
    token_secret: str,
) -> str:
    """OAuth 1.0a HMAC-SHA1 imza üretir."""
    sorted_params = sorted(oauth_params.items())
    param_string = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted_params
    )

    base_string = (
        f"{method.upper()}&"
        f"{urllib.parse.quote(url, safe='')}&"
        f"{urllib.parse.quote(param_string, safe='')}"
    )

    signing_key = (
        f"{urllib.parse.quote(consumer_secret, safe='')}&"
        f"{urllib.parse.quote(token_secret, safe='')}"
    )

    hashed = hmac.new(
        signing_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha1,
    )
    return base64.b64encode(hashed.digest()).decode("utf-8")


def _build_oauth_header(
    creds: dict,
    method: str = "POST",
    url: str = _TWITTER_TWEET_URL,
) -> str:
    """OAuth 1.0a Authorization header oluşturur.

    method ve url parametreleri ile GET/POST ve farklı endpointler desteklenir.
    """
    oauth_params = {
        "oauth_consumer_key": creds["api_key"],
        "oauth_nonce": uuid.uuid4().hex,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": creds["access_token"],
        "oauth_version": "1.0",
    }

    signature = _generate_oauth_signature(
        method=method,
        url=url,
        oauth_params=oauth_params,
        consumer_secret=creds["api_secret"],
        token_secret=creds["access_token_secret"],
    )
    oauth_params["oauth_signature"] = signature

    header_parts = ", ".join(
        f'{urllib.parse.quote(k, safe="")}="{urllib.parse.quote(v, safe="")}"'
        for k, v in sorted(oauth_params.items())
    )
    return f"OAuth {header_parts}"


# -------------------------------------------------------
# 1. Tweet Çekme (Twitter API v2)
# -------------------------------------------------------

def _extract_tweet_id(tweet_url: str) -> str | None:
    """Tweet URL'sinden tweet ID'sini çıkarır.

    Desteklenen formatlar:
    - https://x.com/user/status/123456789
    - https://twitter.com/user/status/123456789
    - https://x.com/user/status/123456789?s=20
    """
    match = re.search(r"(?:twitter\.com|x\.com)/\w+/status/(\d+)", tweet_url)
    return match.group(1) if match else None


async def fetch_tweet_by_url(tweet_url: str) -> dict:
    """Tweet URL'sinden tweet bilgilerini çeker.

    Returns:
        {
            "success": True,
            "tweet_id": str,
            "text": str,
            "author_username": str,
            "author_name": str,
            "likes": int,
            "retweets": int,
        }
        veya hata durumunda:
        {"success": False, "error": str}
    """
    tweet_id = _extract_tweet_id(tweet_url)
    if not tweet_id:
        return {"success": False, "error": "Geçersiz tweet URL'si. Desteklenen format: https://x.com/kullanici/status/123..."}

    creds = _load_credentials()
    if not creds:
        return {"success": False, "error": "Twitter API anahtarları yapılandırılmamış."}

    # Twitter API v2 — Tweet lookup
    lookup_url = _TWITTER_TWEET_LOOKUP_URL.format(tweet_id=tweet_id)
    params_url = f"{lookup_url}?tweet.fields=text,author_id,public_metrics&expansions=author_id&user.fields=username,name"

    # OAuth header — GET isteği, parametre olmayan base URL kullanılır
    # Ancak query parametreleri de imzaya dahil edilmeli
    query_params = {
        "tweet.fields": "text,author_id,public_metrics",
        "expansions": "author_id",
        "user.fields": "username,name",
    }

    # OAuth params + query params birlikte imzalanır
    oauth_params_base = {
        "oauth_consumer_key": creds["api_key"],
        "oauth_nonce": uuid.uuid4().hex,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": creds["access_token"],
        "oauth_version": "1.0",
    }

    # İmza için oauth + query params birleştirilir
    all_params = {**oauth_params_base, **query_params}

    signature = _generate_oauth_signature(
        method="GET",
        url=lookup_url,
        oauth_params=all_params,
        consumer_secret=creds["api_secret"],
        token_secret=creds["access_token_secret"],
    )
    oauth_params_base["oauth_signature"] = signature

    auth_header_parts = ", ".join(
        f'{urllib.parse.quote(k, safe="")}="{urllib.parse.quote(v, safe="")}"'
        for k, v in sorted(oauth_params_base.items())
    )
    auth_header = f"OAuth {auth_header_parts}"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                lookup_url,
                params=query_params,
                headers={"Authorization": auth_header},
            )

        if response.status_code != 200:
            error_text = response.text[:300]
            logger.error(f"Tweet çekme hatası: HTTP {response.status_code} — {error_text}")
            return {"success": False, "error": f"Twitter API hatası (HTTP {response.status_code})"}

        data = response.json()
        tweet_data = data.get("data", {})
        includes = data.get("includes", {})
        users = includes.get("users", [{}])
        author = users[0] if users else {}

        metrics = tweet_data.get("public_metrics", {})

        return {
            "success": True,
            "tweet_id": tweet_data.get("id", tweet_id),
            "text": tweet_data.get("text", ""),
            "author_username": author.get("username", "bilinmiyor"),
            "author_name": author.get("name", "Bilinmiyor"),
            "likes": metrics.get("like_count", 0),
            "retweets": metrics.get("retweet_count", 0),
        }

    except httpx.TimeoutException:
        logger.error("Tweet çekme zaman aşımı")
        return {"success": False, "error": "Twitter API zaman aşımı — tekrar deneyin."}
    except Exception as e:
        logger.error(f"Tweet çekme hatası: {e}")
        return {"success": False, "error": f"Beklenmeyen hata: {str(e)[:200]}"}


# -------------------------------------------------------
# 2. AI Reply Önerisi Üretme (Abacus AI)
# -------------------------------------------------------

_SYSTEM_PROMPT = """Sen borsa takipçisi bir Twitter kullanıcısısın.

GÖREV: Tweet'e KISA ve DOĞAL bir reply yaz. 3 farklı öneri üret.

KESİN KURALLAR:
1. Her reply 7-12 kelime olmalı — ASLA daha uzun yazma
2. HİÇBİR RAKAM veya FİYAT YAZMA — ne seviye, ne yüzde, ne hedef fiyat
3. Genel borsa/hisse/şirket haberi/halka arz yorumu yap
4. Samimi, doğal, insansı yaz — robot gibi yazma
5. Kısa cümle, sohbet havası — "Aynen", "Güzel tespit", "Katılıyorum" tarzı
6. Max 1 emoji, abartma
7. YT değildir YAZMA

YASAK KONULAR (is_safe: false yap):
- Siyaset, politika, seçim, parti, siyasi kişiler
- Spor, magazin, kişisel konular
- Hakaret, nefret söylemi
- Borsa/finans/ekonomi DIŞI her şey

ÖRNEK İYİ REPLY'LAR:
- "Hacim artışı devam ederse momentum güçlenir 📈"
- "Güzel tespit, sektörde hareketlilik var"
- "Bu haberi yakından takip etmek lazım"
- "Katılıyorum, bilanço dönemi belirleyici olacak"
- "Piyasa bunu fiyatlamaya başlamış gibi"

KÖTÜ REPLY (YAPMA):
- "5000 üstü tutunma kritik" ← RAKAM VAR, YASAK
- "Hedef 47.50 TL görünüyor" ← FİYAT VAR, YASAK
- "%3.5 üstü kapanış önemli" ← YÜZDE VAR, YASAK

JSON ÇIKTI:
{"is_safe": true, "reason": "", "replies": ["reply1", "reply2", "reply3"]}
veya
{"is_safe": false, "reason": "Borsa dışı konu", "replies": []}"""


async def generate_reply_suggestions(tweet_text: str) -> dict:
    """AI ile tweet'e 3 reply önerisi üretir.

    Args:
        tweet_text: Orijinal tweet metni

    Returns:
        {
            "success": True,
            "is_safe": bool,
            "reason": str,
            "replies": [str, str, str],
        }
        veya hata durumunda:
        {"success": False, "error": str}
    """
    api_key = _get_api_key()
    if not api_key:
        return {"success": False, "error": "Abacus AI API key yapılandırılmamış."}

    user_message = f"Aşağıdaki tweet'e reply önerisi üret:\n\n---\n{tweet_text}\n---"

    payload = {
        "model": _AI_MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.7,
        "max_tokens": 500,
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            response = await client.post(_ABACUS_URL, json=payload, headers=headers)

        if response.status_code != 200:
            logger.error(f"AI reply hatası: HTTP {response.status_code} — {response.text[:200]}")
            return {"success": False, "error": f"AI servisi hatası (HTTP {response.status_code})"}

        data = response.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

        if not content:
            return {"success": False, "error": "AI boş yanıt döndü."}

        # JSON parse — ```json ... ``` bloğunu da destekle
        json_text = content.strip()
        if json_text.startswith("```"):
            # ```json\n{...}\n``` formatını temizle
            json_text = re.sub(r"^```(?:json)?\s*", "", json_text)
            json_text = re.sub(r"\s*```$", "", json_text)

        result = json.loads(json_text)

        # Doğrulama
        is_safe = result.get("is_safe", False)
        reason = result.get("reason", "")
        replies = result.get("replies", [])

        # is_safe bool kontrolü
        if isinstance(is_safe, str):
            is_safe = is_safe.lower() in ("true", "1", "yes", "evet")

        # Güvenli değilse direkt dön
        if not is_safe:
            return {
                "success": True,
                "is_safe": False,
                "reason": reason or "AI tarafından reddedildi.",
                "replies": [],
            }

        # Reply validasyonu
        if not isinstance(replies, list) or len(replies) == 0:
            return {"success": False, "error": "AI geçerli reply üretemedi."}

        # Her reply'ı max 280 karaktere kırp
        validated_replies = []
        for r in replies[:3]:  # Max 3 reply
            if isinstance(r, str) and r.strip():
                reply_text = r.strip()
                if len(reply_text) > 280:
                    reply_text = reply_text[:277] + "..."
                validated_replies.append(reply_text)

        if not validated_replies:
            return {"success": False, "error": "AI geçerli reply üretemedi."}

        return {
            "success": True,
            "is_safe": True,
            "reason": "",
            "replies": validated_replies,
        }

    except json.JSONDecodeError as e:
        logger.error(f"AI reply JSON parse hatası: {e}")
        return {"success": False, "error": "AI yanıtı JSON formatında değil — tekrar deneyin."}
    except httpx.TimeoutException:
        logger.error("AI reply zaman aşımı")
        return {"success": False, "error": "AI servisi zaman aşımı — tekrar deneyin."}
    except Exception as e:
        logger.error(f"AI reply hatası: {e}")
        return {"success": False, "error": f"Beklenmeyen hata: {str(e)[:200]}"}


# -------------------------------------------------------
# 3. Reply Gönderme (Twitter API v2)
# -------------------------------------------------------

async def send_reply(tweet_id: str, reply_text: str) -> dict:
    """Tweet'e reply atar.

    Args:
        tweet_id: Yanıtlanacak tweet'in ID'si
        reply_text: Reply metni

    Returns:
        {
            "success": True,
            "reply_tweet_id": str,
        }
        veya hata durumunda:
        {"success": False, "error": str}
    """
    if not tweet_id or not reply_text or not reply_text.strip():
        return {"success": False, "error": "Tweet ID ve reply metni gerekli."}

    reply_text = reply_text.strip()

    # 280 karakter limiti (reply'lar Blue Tick'ten bağımsız olarak 280'e kısıtlı olmayabilir ama güvenli sınır)
    if len(reply_text) > 4000:
        reply_text = reply_text[:3997] + "..."

    creds = _load_credentials()
    if not creds:
        return {"success": False, "error": "Twitter API anahtarları yapılandırılmamış."}

    auth_header = _build_oauth_header(creds, method="POST", url=_TWITTER_TWEET_URL)

    payload = {
        "text": reply_text,
        "reply": {
            "in_reply_to_tweet_id": tweet_id,
        },
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                _TWITTER_TWEET_URL,
                json=payload,
                headers={
                    "Authorization": auth_header,
                    "Content-Type": "application/json",
                },
            )

        if response.status_code in (200, 201):
            data = response.json()
            reply_id = data.get("data", {}).get("id", "?")
            logger.info(f"Reply başarılı (id={reply_id}) → tweet {tweet_id}")
            return {
                "success": True,
                "reply_tweet_id": reply_id,
            }
        else:
            error_text = response.text[:300]
            logger.error(f"Reply hatası: HTTP {response.status_code} — {error_text}")
            return {"success": False, "error": f"Twitter API hatası (HTTP {response.status_code}): {error_text}"}

    except httpx.TimeoutException:
        logger.error("Reply gönderme zaman aşımı")
        return {"success": False, "error": "Twitter API zaman aşımı — tekrar deneyin."}
    except Exception as e:
        logger.error(f"Reply gönderme hatası: {e}")
        return {"success": False, "error": f"Beklenmeyen hata: {str(e)[:200]}"}


# -------------------------------------------------------
# 4. Otomatik Reply — Kullanıcı ID Çözümleme
# -------------------------------------------------------

async def get_user_id_by_username(username: str) -> str | None:
    """Twitter API v2 — @username'den user_id çözer.

    Returns:
        user_id string veya None (hata durumunda)
    """
    creds = _load_credentials()
    if not creds:
        return None

    lookup_url = _TWITTER_USER_LOOKUP_URL.format(username=username)

    # OAuth 1.0a GET — query param yok, sadece base URL imzalanır
    auth_header = _build_oauth_header(creds, method="GET", url=lookup_url)

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                lookup_url,
                headers={"Authorization": auth_header},
            )

        if response.status_code == 200:
            data = response.json()
            user_id = data.get("data", {}).get("id")
            if user_id:
                logger.info(f"Twitter user ID çözümlendi: @{username} → {user_id}")
                return user_id
        elif response.status_code == 404:
            logger.warning(f"Twitter kullanıcı bulunamadı: @{username}")
        else:
            logger.error(f"Twitter user lookup hatası: @{username} — HTTP {response.status_code}")

        return None

    except Exception as e:
        logger.error(f"Twitter user lookup hatası: @{username} — {e}")
        return None


# -------------------------------------------------------
# 5. Otomatik Reply — Kullanıcı Son Tweetleri
# -------------------------------------------------------

async def fetch_user_recent_tweets(
    user_id: str,
    since_id: str | None = None,
) -> list[dict]:
    """Kullanıcının son tweetlerini çeker.

    Args:
        user_id: Twitter user ID
        since_id: Bu ID'den sonraki tweetleri getir (None = son 10)

    Returns:
        [{"id": str, "text": str, "created_at": str}, ...]
    """
    creds = _load_credentials()
    if not creds:
        return []

    tweets_url = _TWITTER_USER_TWEETS_URL.format(user_id=user_id)

    # Query parametreleri
    query_params = {
        "max_results": "10",
        "tweet.fields": "text,created_at",
        "exclude": "retweets,replies",  # Sadece orijinal tweetler
    }
    if since_id:
        query_params["since_id"] = since_id

    # OAuth 1.0a GET — query params dahil imza
    oauth_params_base = {
        "oauth_consumer_key": creds["api_key"],
        "oauth_nonce": uuid.uuid4().hex,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": creds["access_token"],
        "oauth_version": "1.0",
    }

    # İmza için oauth + query params birleşir
    all_params = {**oauth_params_base, **query_params}

    signature = _generate_oauth_signature(
        method="GET",
        url=tweets_url,
        oauth_params=all_params,
        consumer_secret=creds["api_secret"],
        token_secret=creds["access_token_secret"],
    )
    oauth_params_base["oauth_signature"] = signature

    auth_header_parts = ", ".join(
        f'{urllib.parse.quote(k, safe="")}="{urllib.parse.quote(v, safe="")}"'
        for k, v in sorted(oauth_params_base.items())
    )
    auth_header = f"OAuth {auth_header_parts}"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(
                tweets_url,
                params=query_params,
                headers={"Authorization": auth_header},
            )

        if response.status_code != 200:
            logger.error(f"User tweets hatası: {user_id} — HTTP {response.status_code}")
            return []

        data = response.json()
        tweets = data.get("data", [])

        if not tweets:
            return []

        result = []
        for tweet in tweets:
            result.append({
                "id": tweet.get("id", ""),
                "text": tweet.get("text", ""),
                "created_at": tweet.get("created_at", ""),
            })

        return result

    except Exception as e:
        logger.error(f"User tweets hatası: {user_id} — {e}")
        return []


# -------------------------------------------------------
# 6. Otomatik Reply — Ana Döngü (Scheduler'dan çağrılır)
# -------------------------------------------------------

async def _seed_default_targets():
    """Başlangıç reply hedeflerini DB'ye ekler (yoksa)."""
    try:
        from app.database import async_session
        from app.models.user import ReplyTarget, DEFAULT_REPLY_TARGETS
        from sqlalchemy import select

        async with async_session() as session:
            for username in DEFAULT_REPLY_TARGETS:
                existing = await session.execute(
                    select(ReplyTarget).where(ReplyTarget.username == username)
                )
                if not existing.scalar_one_or_none():
                    session.add(ReplyTarget(username=username, is_active=True))
                    logger.info(f"Reply hedefi eklendi: @{username}")
            await session.commit()

    except Exception as e:
        logger.error(f"Reply hedef seed hatası: {e}")


async def _is_auto_reply_enabled() -> bool:
    """Auto-reply toggle durumunu DB'den kontrol eder."""
    try:
        from app.database import async_session
        from app.models.app_setting import AppSetting
        from sqlalchemy import select

        async with async_session() as session:
            result = await session.execute(
                select(AppSetting).where(AppSetting.key == "AUTO_REPLY_ENABLED")
            )
            setting = result.scalar_one_or_none()
            if setting:
                return setting.value.lower() in ("true", "1", "yes")
            return True  # Default: açık

    except Exception:
        return True  # Hata durumunda açık varsay


async def _get_daily_limit() -> int:
    """Günlük reply limitini DB'den okur (admin panelden ayarlanır)."""
    try:
        from app.database import async_session
        from app.models.app_setting import AppSetting
        from sqlalchemy import select

        async with async_session() as session:
            result = await session.execute(
                select(AppSetting).where(AppSetting.key == "AUTO_REPLY_DAILY_LIMIT")
            )
            setting = result.scalar_one_or_none()
            if setting:
                return int(setting.value)
    except Exception:
        pass
    return _AUTO_REPLY_DAILY_LIMIT_DEFAULT


async def _get_today_reply_count() -> int:
    """Bugün kaç reply atıldığını sayar."""
    try:
        from app.database import async_session
        from app.models.user import AutoReply
        from sqlalchemy import select, func

        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        async with async_session() as session:
            result = await session.execute(
                select(func.count(AutoReply.id)).where(
                    AutoReply.status == "replied",
                    AutoReply.created_at >= today_start,
                )
            )
            return result.scalar() or 0

    except Exception:
        return 0


async def _like_tweet(tweet_id: str) -> bool:
    """Tweet'i beğenir (Twitter API v2 — POST /2/users/:id/likes).

    Returns True if successful.
    """
    creds = _load_credentials()
    if not creds:
        return False

    # Kendi user ID'mizi almamız lazım — oauth token sahibi
    # Twitter API v2: POST /2/users/{authenticated_user_id}/likes
    # Authenticated user ID'yi token'dan çıkarmak yerine, ayarlardan alalım
    try:
        from app.config import get_settings
        my_user_id = getattr(get_settings(), "X_USER_ID", None)
        if not my_user_id:
            # User ID yoksa /2/users/me endpoint'inden çek
            me_url = "https://api.twitter.com/2/users/me"
            auth_header = _build_oauth_header(creds, method="GET", url=me_url)
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(me_url, headers={"Authorization": auth_header})
            if resp.status_code == 200:
                my_user_id = resp.json().get("data", {}).get("id")
            if not my_user_id:
                logger.error("Kendi user ID alınamadı — beğeni atılamıyor")
                return False
    except Exception as e:
        logger.error(f"User ID alma hatası: {e}")
        return False

    like_url = _TWITTER_LIKE_URL.format(user_id=my_user_id)
    auth_header = _build_oauth_header(creds, method="POST", url=like_url)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                like_url,
                json={"tweet_id": tweet_id},
                headers={
                    "Authorization": auth_header,
                    "Content-Type": "application/json",
                },
            )

        if resp.status_code in (200, 201):
            logger.info(f"Tweet beğenildi: {tweet_id}")
            return True
        else:
            logger.warning(f"Beğeni hatası: HTTP {resp.status_code} — {resp.text[:200]}")
            return False

    except Exception as e:
        logger.error(f"Beğeni hatası: {e}")
        return False


async def auto_reply_cycle():
    """Otomatik reply ana döngüsü — scheduler'dan 5dk'da bir çağrılır.

    KRİTİK KURALLAR:
    - Sadece YENİ tweetlere reply atar (last_seen_tweet_id'den sonrakiler)
    - İlk çalışmada: mevcut tweetlerin ID'sini kaydeder, reply ATMAZ
    - Her reply arasında 30sn-3dk rastgele jitter (doğal görünüm)
    - Her 5 reply'da 1 beğeni atar
    - Günlük 20 reply limiti (gece 00:00'da sıfırlanır)
    - Siyasi/borsa dışı tweetlere reply ATMAZ (AI filtresi)
    """
    if _AUTO_REPLY_LOCK.locked():
        logger.debug("Auto-reply zaten çalışıyor, atlıyorum")
        return

    async with _AUTO_REPLY_LOCK:
        try:
            # Toggle kontrolü
            if not await _is_auto_reply_enabled():
                logger.debug("Auto-reply devre dışı (toggle kapalı)")
                return

            # Seed default targets (ilk çalışmada)
            await _seed_default_targets()

            from app.database import async_session
            from app.models.user import ReplyTarget, AutoReply
            from sqlalchemy import select

            # Günlük limit (DB'den okunur — admin panelden ayarlanabilir)
            daily_limit = await _get_daily_limit()
            today_count = await _get_today_reply_count()
            if today_count >= daily_limit:
                logger.info(f"Günlük reply limiti doldu: {today_count}/{daily_limit}")
                return

            remaining = daily_limit - today_count

            async with async_session() as session:
                # Aktif hedefleri çek
                result = await session.execute(
                    select(ReplyTarget).where(ReplyTarget.is_active == True)
                )
                targets = list(result.scalars().all())

                if not targets:
                    logger.debug("Aktif reply hedefi yok")
                    return

                logger.info(
                    f"Auto-reply tarama: {len(targets)} hedef, bugün {today_count} reply, kalan {remaining}"
                )

                # Hedefleri karıştır — her seferinde farklı sıra
                random.shuffle(targets)

                replies_sent = 0
                total_liked = 0

                for target in targets:
                    if replies_sent >= remaining:
                        break

                    # User ID çözümle (cache'de yoksa API'den çek)
                    user_id = target.twitter_user_id
                    if not user_id:
                        user_id = await get_user_id_by_username(target.username)
                        if user_id:
                            target.twitter_user_id = user_id
                            await session.flush()
                        else:
                            logger.warning(f"User ID çözümlenemedi: @{target.username}")
                            continue

                    # ─── İLK ÇALIŞMA KONTROLÜ ───
                    # last_seen_tweet_id yoksa: mevcut tweetlerin en yenisini kaydet, reply ATMA
                    if not target.last_seen_tweet_id:
                        init_tweets = await fetch_user_recent_tweets(user_id)
                        if init_tweets:
                            # En yeni tweet ID'yi kaydet
                            newest_id = max(t["id"] for t in init_tweets)
                            target.last_seen_tweet_id = newest_id
                            await session.flush()
                            logger.info(
                                f"İlk tarama @{target.username}: {len(init_tweets)} mevcut tweet atlandı, "
                                f"since_id={newest_id} kaydedildi"
                            )
                        else:
                            logger.info(f"İlk tarama @{target.username}: tweet bulunamadı")
                        continue  # Bu hedef için bu döngüde reply ATMA

                    # ─── YENİ TWEETLERİ ÇEK (since_id ile) ───
                    tweets = await fetch_user_recent_tweets(
                        user_id,
                        since_id=target.last_seen_tweet_id,
                    )

                    if not tweets:
                        continue

                    # En yeni tweet ID'yi güncelle (bir sonraki döngü için)
                    newest_id = max(t["id"] for t in tweets)
                    target.last_seen_tweet_id = newest_id
                    await session.flush()

                    logger.info(
                        f"@{target.username}: {len(tweets)} yeni tweet bulundu"
                    )

                    for tweet in tweets:
                        if replies_sent >= remaining:
                            break

                        tweet_id = tweet["id"]
                        tweet_text = tweet["text"]

                        # Çok kısa tweetleri atla
                        if len(tweet_text.strip()) < 15:
                            continue

                        # Zaten reply atılmış mı?
                        existing = await session.execute(
                            select(AutoReply).where(AutoReply.target_tweet_id == tweet_id)
                        )
                        if existing.scalar_one_or_none():
                            continue

                        # AI reply üret
                        ai_result = await generate_reply_suggestions(tweet_text)

                        if not ai_result.get("success"):
                            session.add(AutoReply(
                                target_tweet_id=tweet_id,
                                target_username=target.username,
                                target_text=tweet_text[:1000],
                                reply_text="",
                                status="failed",
                                error_message=ai_result.get("error", "AI hatası"),
                            ))
                            await session.flush()
                            continue

                        if not ai_result.get("is_safe"):
                            session.add(AutoReply(
                                target_tweet_id=tweet_id,
                                target_username=target.username,
                                target_text=tweet_text[:1000],
                                reply_text="",
                                status="unsafe",
                                error_message=ai_result.get("reason", "Güvenli değil"),
                            ))
                            await session.flush()
                            logger.info(
                                f"Unsafe tweet atlandı: @{target.username} — {ai_result.get('reason')}"
                            )
                            continue

                        # Rastgele reply seç (her seferinde farklı ton)
                        reply_options = ai_result.get("replies", [])
                        if not reply_options:
                            continue

                        chosen_reply = random.choice(reply_options)

                        # ─── JITTER: Doğal gecikme (30sn - 3dk) ───
                        jitter = random.uniform(_JITTER_MIN, _JITTER_MAX)
                        logger.info(
                            f"Reply gönderilecek: @{target.username} tweet {tweet_id} "
                            f"— {jitter:.0f}sn sonra → \"{chosen_reply[:50]}...\""
                        )
                        await asyncio.sleep(jitter)

                        # Reply gönder
                        send_result = await send_reply(tweet_id, chosen_reply)

                        if send_result.get("success"):
                            session.add(AutoReply(
                                target_tweet_id=tweet_id,
                                target_username=target.username,
                                target_text=tweet_text[:1000],
                                reply_text=chosen_reply,
                                reply_tweet_id=send_result.get("reply_tweet_id"),
                                status="replied",
                            ))
                            replies_sent += 1
                            logger.info(
                                f"Auto-reply #{replies_sent}: @{target.username} → \"{chosen_reply[:60]}\""
                            )

                            # ─── BEĞENİ: Her 5 reply'da 1 beğeni ───
                            if replies_sent % _LIKE_EVERY_N == 0:
                                like_ok = await _like_tweet(tweet_id)
                                if like_ok:
                                    total_liked += 1
                        else:
                            session.add(AutoReply(
                                target_tweet_id=tweet_id,
                                target_username=target.username,
                                target_text=tweet_text[:1000],
                                reply_text=chosen_reply,
                                status="failed",
                                error_message=send_result.get("error", "Gönderim hatası"),
                            ))
                            logger.error(
                                f"Auto-reply başarısız: @{target.username} — {send_result.get('error')}"
                            )

                        await session.flush()

                await session.commit()

            logger.info(
                f"Auto-reply döngü tamamlandı: {replies_sent} reply, {total_liked} beğeni"
            )

        except Exception as e:
            logger.error(f"Auto-reply döngü hatası: {e}", exc_info=True)
