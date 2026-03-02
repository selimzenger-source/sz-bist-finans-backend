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

# Gemini 2.5 Flash — birincil (OpenAI uyumlu endpoint)
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"

# Gemini 2.5 Pro — yedek
_GEMINI_PRO_MODEL = "gemini-2.5-pro"
_AI_TIMEOUT = 25

# Twitter API v2
_TWITTER_TWEET_URL = "https://api.twitter.com/2/tweets"
_TWITTER_TWEET_LOOKUP_URL = "https://api.twitter.com/2/tweets/{tweet_id}"
_TWITTER_USER_LOOKUP_URL = "https://api.twitter.com/2/users/by/username/{username}"
_TWITTER_USER_TWEETS_URL = "https://api.twitter.com/2/users/{user_id}/tweets"
_TWITTER_LIKE_URL = "https://api.twitter.com/2/users/{user_id}/likes"

# Otomatik reply ayarlari
_AUTO_REPLY_DAILY_LIMIT_DEFAULT = 24  # DB'de yoksa bu kullanilir
_AUTO_REPLY_LOCK = asyncio.Lock()

# Jitter araliklari (saniye) — dogal gorunum icin dagitilmis
_JITTER_MIN = 30
_JITTER_MAX = 180  # 3 dakika

# Begeni sikligi: her N reply'da 1 begeni
_LIKE_EVERY_N = 5

# Tweet yaş limiti — bu süreden eski tweetlere reply ATMA (dakika)
# Deploy sonrası biriken eski tweetlere spam yapmayı önler
_MAX_TWEET_AGE_MINUTES = 30  # Sadece son 30dk'daki tweetlere reply at

# Min kelime sayısı — çok kısa tweetlere reply atma
_MIN_TWEET_WORDS = 6  # En az 6 kelime olmalı — kısa tweetlere anlamlı reply üretilemez

# Twitter API rate limit — Basic tier (user OAuth 1.0a):
#   GET /2/users/:id/tweets = 900 req / 15 dk
#   POST /2/tweets = ~100 / 24 saat (reply dahil)
#   POST /2/users/:id/likes = 50 / 15 dk, 1000 / 24 saat
# KRİTİK: GET istekleri de ÜCRETL — Read kredit tüketir!
# 30dk × 8 hedef = max 384 GET/gün (~$0.10-0.25 maliyet)
# Limit dolunca (24 reply) GET yapmaz → 0 ek maliyet
_MAX_TARGETS_PER_CYCLE = 8   # API kredi tasarrufu — 8 hedef/döngü yeterli (akıllı sıralama ile)
_MAX_REPLIES_PER_CYCLE = 2   # Döngü başına max 2 reply — zamana yayar, spam önler
                              # 30dk interval × 2 reply = saatte max 4 reply (doğal görünüm)
_twitter_rate_limited = False  # 429 alınca True olur, sonraki döngüde resetlenir


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


def _get_gemini_key() -> str | None:
    """Gemini API key'i al."""
    try:
        from app.config import get_settings
        key = get_settings().GEMINI_API_KEY
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

_SYSTEM_PROMPT = """Sen Türk borsasını yakından takip eden, piyasa bilgisi güçlü, düşünceli bir yatırımcısın. Twitter'da finans çevresinde aktifsin. Amacın kaliteli, düşündürücü ve konuyla doğrudan ilgili yorumlar yapmak.

GÖREV: Tweet'e 3 FARKLI reply önerisi üret. Her biri farklı ton ve uzunlukta olsun.

═══ KİMLİĞİN ═══
- Piyasayı günlük takip eden, halka arzları ve ekonomiyi bilen biri
- Düşünceli ve bilgili — yorum yaparken konuya gerçekten hakim olduğun anlaşılmalı
- Bazen kendi görüşünü ekler, bazen soru sorar, bazen kısa bir onay verir
- Asla guru değilsin, asla tavsiye vermezsin — sadece kaliteli sohbet edersin

═══ HİTAP — TAMAMEN YASAK ═══
- "hocam", "üstat", "üstad", "abi", "reis", "kardeşim", "dostum" HİÇBİR ZAMAN KULLANMA
- Hitap olmadan direkt konuya gir — hiçbir reply "Hocam," diye başlamasın

═══ DİL KALİTESİ — ÇOK ÖNEMLİ ═══
- Doğal, akıcı Türkçe yaz — samimi ama kaliteli
- YASAK KELİMELER: "hocam", "valla", "vallahi", "billa", "harbiden", "baya", "bi" (bir yerine), "abi", "lan", "ya" (cümle başı dolgu), "heh", "eee", "üstat", "reis"
- Bunlar yerine doğal alternatifler kullan: "gerçekten", "oldukça", "bir", "aslında", "açıkçası", "cidden"
- "aynen", "bence de", "doğru" gibi onay kelimeleri kullanabilirsin ama her reply'da değil
- "sizce" kelimesini KULLANMA — çok resmi
- İnsansı ve samimi ol ama ARGO KULLANMA — eğitimli, piyasayı takip eden birisi gibi konuş

═══ DOĞAL DİL — ROBOT GİBİ GÖRÜNME ═══
- "-leri", "-ları", "-lerin", "-ların" eklerini aynı cümlede TEKRARLAMA — robot gibi görünür
  KÖTÜ: "piyasaların hareketleri yatırımcıların beklentilerini etkiliyor"
  İYİ: "piyasa hareketi yatırımcı beklentisini etkiliyor"
- Aynı eki yan yana kullanma — cümleyi kısalt veya yeniden kur
- Gereksiz uzatma yapma — "olarak değerlendirilebilir", "şeklinde yorumlanabilir" gibi bürokrat dili YASAK
- Kısa, doğal, günlük konuşma diliyle yaz — Twitter'da kimse makale yazmaz
- Virgülü fazla kullanma — 1 cümlede max 1 virgül yeterli

═══ YAZI TARZI ═══
- Doğal ama kaliteli Twitter dili — ne çok resmi ne çok argo
- Kısa, net cümleler kur — ama ANLAMLI olsun, boş kalıp olmasın
- Her reply tweet'in KONUSUYLA DOĞRUDAN İLGİLİ olmalı — genel geçer yorum yapma
- Tweet'teki spesifik bir noktaya değin veya kendi bilgini ekle
- Aşırı noktalama KULLANMA — virgül, noktalı virgül fazla koymak robot gibi görünür

═══ 3 REPLY FORMATI ═══
1. UZUN YORUM (12-20 kelime): Tweet'teki konuya kendi bakış açını ekle. SPESİFİK ol — tweet'teki bilgiyi genişlet, farklı bir açı getir veya bir bağlam ekle.
2. KISA TEPKİ (4-8 kelime): Doğal ama düzgün insan tepkisi. "Bunu bekliyordum açıkçası", "Güzel gelişme", "İlk seans önemli olacak"
3. SORU / KATILIM (8-15 kelime): Konuya akıllı bir soru sor veya kendi bilgini ekleyerek katıl.

═══ KONU FİLTRESİ ═══
SADECE bunlara reply yaz (is_safe: true):
- Halka arz haberleri, yeni onaylar, dağıtım sonuçları
- Şirket haberleri, bilanço, finansal gelişmeler
- Borsa genel yorumları, piyasa değerlendirmesi, endeks yorumu
- Ekonomi haberleri (faiz kararı, enflasyon, merkez bankası, kur)
- Sektörel haberler, yatırım dünyası genel

YASAK — kesinlikle reply ATMA (is_safe: false):
- Teknik analiz, grafik analizi, formasyon, destek/direnç, indikatör (RSI, MACD, fibonacci vb.)
- Grafik/chart paylaşan tweetler
- Siyaset, politika, seçim, parti, siyasi kişiler, tartışma
- Spor, magazin, kişisel hayat, din, taziye, başsağlığı
- Hakaret, provokasyon, nefret söylemi, kavga
- Finans/borsa/ekonomi DIŞI her konu
- Tweet çok kısa veya belirsiz — ne hakkında olduğu net anlaşılmıyorsa → is_safe: false
- Tweet sadece link/görsel/hashtag paylaşıyorsa, metin yoksa → is_safe: false
- Konuyu tam anlayamıyorsan, zorlama — is_safe: false dön

═══ KESİN KURALLAR ═══
1. HİÇBİR RAKAM / FİYAT / YÜZDE YAZMA — "5000 puan", "hedef 47 TL", "%3.5" gibi şeyler YASAK
2. "YT değildir" YAZMA
3. Emoji: 3 reply'dan en fazla 1 tanesinde, sadece 1 emoji (📈 📉 🔥 💪 👀 🤔 👏)
4. Tavsiye verme — "al", "sat", "gir", "çık" gibi yönlendirme YASAK
5. Aynı kalıp cümleleri tekrarlama — "yakından takip etmek lazım" gibi şeyleri her seferinde yazma
6. Karşı tarafın fikrini saygıyla karşıla, kavga etme, tartışma
7. Tweet'e ANLAMLI ve SPESİFİK bir şey ekleyemiyorsan is_safe: false dön — ZORLAMA reply ATMA
8. Tweet çok kısa/belirsizse veya sadece link/görselse → is_safe: false
9. Reply'ların her biri tam ve anlamlı cümle olmalı — yarım bırakma, eksik bırakma
10. Konuyla alakasız genel yorum YAPMA — her reply tweet'in içeriğiyle DOĞRUDAN bağlantılı olmalı

═══ ÖRNEK İYİ REPLY'LAR ═══
Tweet: "BIST güne alıcılı başladı"
→ "sabah seansı güzel açıldı bakalım öğleden sonra da tutunabilecek mi"
→ "güzel başlangıç 📈"
→ "dış piyasalardan da destek gelince böyle oluyor genelde"

Tweet: "X şirketinin bilançosu beklentilerin üstünde geldi"
→ "bunu bekliyordum aslında sektördeki genel trend de olumlu zaten"
→ "güçlü bilanço geldi açıkçası"
→ "bir sonraki çeyrek için beklentiler nasıl acaba"

Tweet: "Yeni halka arz onaylandı: ABC Teknoloji"
→ "teknoloji sektöründen bir halka arz daha ilgi artıyor bu tarafta"
→ "detaylarına bakmak lazım"
→ "halka arz takvimi iyice yoğunlaştı bu aralar güzel hareketlilik var"

Tweet: "Merkez Bankası faiz kararını açıkladı"
→ "piyasa bunu nasıl fiyatlayacak merak ediyorum ilk tepkiler karışık gibi"
→ "beklentiler dahilindeydi aslında"
→ "faiz tarafında sürpriz olmadı ama asıl mesele ileriye dönük mesajlar bence"

═══ KÖTÜ REPLY (YAPMA) ═══
- "5000 seviyesi kritik" ← RAKAM, YASAK
- "Hedef 47.50 TL" ← FİYAT, YASAK
- "Destek seviyesinden dönüş olabilir" ← TEKNİK ANALİZ, YASAK
- "valla baya iyi geldi" ← ARGO, DÜZELTİLMELİ → "gerçekten iyi geldi"
- "harbiden tempo yüksek" ← ARGO, DÜZELTİLMELİ → "tempo gerçekten yüksek"
- "baya net verilermiş" ← BOŞ, İÇERİKSİZ — konuya spesifik yorum yap
- "Güzel tespit, yakından takip etmek lazım" ← AYNI KALIBI HER SEFERINDE KULLANMA
- 3 reply'ın hepsi aynı tonda ve uzunlukta ← ÇEŞİTLİLİK YOK
- "Bu gelişme, sektördeki trend doğrultusunda, olumlu bir sinyal veriyor." ← ÇOK RESMİ, YAPMA
- Tweet'le alakasız genel geçer yorum ← KONUYLA İLGİSİZ, SPAM GİBİ GÖRÜNÜR
- Yarım kalan veya anlamsız cümleler ← HER CÜMLE TAM VE ANLAMLI OLMALI

═══ JSON ÇIKTI ═══
{"is_safe": true, "reason": "", "replies": ["uzun yorum", "kısa tepki", "soru/katılım"]}
veya
{"is_safe": false, "reason": "Teknik analiz tweeti / siyasi içerik / konu dışı / tweet çok kısa / konu belirsiz", "replies": []}"""


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
    gemini_key = _get_gemini_key()
    if not gemini_key:
        return {"success": False, "error": "Gemini API key yapılandırılmamış."}

    user_message = f"Aşağıdaki tweet'e reply önerisi üret:\n\n---\n{tweet_text}\n---"

    payload = {
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.85,
        "max_tokens": 4096,  # Gemini 2.5 thinking token yiyor
    }

    gemini_headers = {
        "Authorization": f"Bearer {gemini_key}",
        "Content-Type": "application/json",
    }

    # ── AI çağrısı: Gemini Flash birincil, Gemini Pro yedek ──
    content = None

    # ── 1. Birincil: Gemini 2.5 Flash ──
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            response = await client.post(
                _GEMINI_URL, json={**payload, "model": _GEMINI_MODEL}, headers=gemini_headers
            )
        if response.status_code == 200:
            data = response.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if content:
                logger.info("AI reply [Gemini-Flash] kullanildi")
        else:
            logger.warning(f"AI reply Gemini-Flash hatası: HTTP {response.status_code} — {response.text[:200]}")
    except Exception as e:
        logger.warning(f"AI reply Gemini-Flash hata: {e}")

    # ── 2. Yedek: Gemini 2.5 Pro ──
    if not content:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                response = await client.post(
                    _GEMINI_URL, json={**payload, "model": _GEMINI_PRO_MODEL}, headers=gemini_headers
                )
            if response.status_code == 200:
                data = response.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                if content:
                    logger.info("AI reply [Gemini-Pro] kullanildi")
            else:
                logger.error(f"AI reply Gemini-Pro hatası: HTTP {response.status_code} — {response.text[:200]}")
        except Exception as e:
            logger.error(f"AI reply Gemini-Pro hata: {e}")

    if not content:
        return {"success": False, "error": "AI tum providerlar basarisiz."}

    try:
        from app.services.ai_json_helper import safe_parse_json

        result = safe_parse_json(content, required_key="is_safe")
        if result is None:
            logger.error("AI reply JSON parse basarisiz — icerik: %s", content[:200])
            return {"success": False, "error": "AI yanıtı JSON formatında değil — tekrar deneyin."}

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
    """Tweet'e direkt reply atar (thread yorumu). Fallback yok.

    GÜVENLİK: tweet_id boş/geçersiz ise ASLA tweet atmaz (standalone tweet önleme).
    Başarılı POST sonrası response'ta reply bağlamı doğrulanır.

    Args:
        tweet_id: Yanıtlanacak tweet'in ID'si (numerik string)
        reply_text: Reply metni

    Returns:
        {
            "success": True,
            "reply_tweet_id": str,
            "method": "reply",
        }
        veya hata durumunda:
        {"success": False, "error": str}
    """
    if not reply_text or not reply_text.strip():
        return {"success": False, "error": "Reply metni gerekli."}

    # ── GÜVENLİK: tweet_id validasyonu — boş/geçersiz ise standalone tweet riski ──
    if not tweet_id or not isinstance(tweet_id, str) or not tweet_id.strip().isdigit():
        logger.error(
            "STANDALONE TWEET ÖNLENDİ: geçersiz tweet_id=%r — reply atılmadı, text: %s",
            tweet_id, reply_text[:60],
        )
        return {"success": False, "error": f"Geçersiz tweet_id: {tweet_id!r}"}

    tweet_id = tweet_id.strip()
    reply_text = reply_text.strip()

    if len(reply_text) > 4000:
        reply_text = reply_text[:3997] + "..."

    creds = _load_credentials()
    if not creds:
        return {"success": False, "error": "Twitter API anahtarları yapılandırılmamış."}

    # ── Global rate limit: dakikada max 3 tweet (reply dahil) ──
    from app.services.twitter_service import _wait_for_tweet_rate_limit, _record_tweet_sent
    wait = _wait_for_tweet_rate_limit()
    if wait > 0:
        import asyncio
        await asyncio.sleep(wait)

    auth_header = _build_oauth_header(creds, method="POST", url=_TWITTER_TWEET_URL)

    payload = {
        "text": reply_text,
        "reply": {
            "in_reply_to_tweet_id": tweet_id,
        },
    }

    logger.info("Reply gönderiliyor: in_reply_to=%s, text=%s", tweet_id, reply_text[:60])

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
            resp_json = response.json()
            reply_data = resp_json.get("data", {})
            reply_id = reply_data.get("id", "?")

            # ── REPLY DOĞRULAMASI ──
            ref_tweets = reply_data.get("referenced_tweets", [])
            is_actual_reply = any(
                r.get("type") == "replied_to" for r in ref_tweets
            ) if ref_tweets else False

            if not is_actual_reply:
                logger.warning(
                    "⚠️ Reply gönderildi ama referenced_tweets bulunamadı! "
                    "Standalone tweet olabilir. reply_id=%s, in_reply_to=%s, response=%s",
                    reply_id, tweet_id, str(resp_json)[:300],
                )

            logger.info(
                "Reply başarılı (id=%s → reply_to=%s, verified=%s)",
                reply_id, tweet_id, is_actual_reply,
            )
            _record_tweet_sent()
            return {
                "success": True,
                "reply_tweet_id": reply_id,
                "method": "reply",
            }

        error_text = response.text[:300]

        # 403 veya diğer hatalar — fallback yok, direkt hata döndür
        if response.status_code == 403:
            logger.warning("Reply 403 (tweet %s): %s", tweet_id, error_text[:200])
            return {
                "success": False,
                "error": f"Reply 403 — Twitter API kısıtlaması: {error_text[:200]}",
            }

        logger.error(f"Reply hatası: HTTP {response.status_code} — {error_text}")
        return {"success": False, "error": f"Twitter API hatası (HTTP {response.status_code}): {error_text[:200]}"}

    except httpx.TimeoutException:
        logger.error("Reply gönderme zaman aşımı")
        return {"success": False, "error": "Twitter API zaman aşımı — tekrar deneyin."}
    except Exception as e:
        logger.error(f"Reply gönderme hatası: {e}")
        return {"success": False, "error": f"Beklenmeyen hata: {str(e)[:200]}"}


async def _send_quote_tweet(tweet_id: str, text: str, creds: dict) -> dict:
    """Reply yerine quote tweet gönderir (fallback 1)."""
    from app.services.twitter_service import _record_tweet_sent

    auth_header = _build_oauth_header(creds, method="POST", url=_TWITTER_TWEET_URL)
    payload = {"text": text, "quote_tweet_id": tweet_id}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                _TWITTER_TWEET_URL,
                json=payload,
                headers={"Authorization": auth_header, "Content-Type": "application/json"},
            )

        if response.status_code in (200, 201):
            data = response.json()
            qt_id = data.get("data", {}).get("id", "?")
            logger.info(f"Quote tweet başarılı (id={qt_id}) → tweet {tweet_id}")
            _record_tweet_sent()
            return {"success": True, "reply_tweet_id": qt_id, "method": "quote_tweet"}

        error_text = response.text[:300]
        logger.warning(f"Quote tweet 403/hata: HTTP {response.status_code}")
        return {"success": False, "error": f"qt_403:{response.status_code}:{error_text}"}

    except Exception as e:
        return {"success": False, "error": f"qt_err:{str(e)[:200]}"}


async def _get_tweet_author_username(tweet_id: str, creds: dict) -> str | None:
    """Tweet yazarının @username'ini Twitter API v2'den çeker."""
    try:
        lookup_url = f"https://api.twitter.com/2/tweets/{tweet_id}"
        auth_header = _build_oauth_header(creds, method="GET", url=lookup_url)
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(
                lookup_url,
                params={"expansions": "author_id", "user.fields": "username"},
                headers={"Authorization": auth_header},
            )
        if r.status_code == 200:
            data = r.json()
            users = data.get("includes", {}).get("users", [])
            if users:
                return users[0].get("username")
    except Exception as e:
        logger.warning("Tweet yazar username alınamadı: %s", e)
    return None


async def _send_mention_fallback(tweet_id: str, text: str, creds: dict) -> dict:
    """Reply & quote tweet 403 sonrası son fallback: @mention regular tweet.

    Twitter API Basic tier'da reply/quote kısıtlaması var (mention edilmeden reply yasak).
    Çözüm: @username text tweet_url formatında regular tweet gönder.
    Bu kullanıcıya bildirim gider + tweet bağlamı görünür.
    """
    from app.services.twitter_service import _record_tweet_sent

    # Tweet yazarının username'ini çek
    username = await _get_tweet_author_username(tweet_id, creds)
    tweet_url = f"https://x.com/i/status/{tweet_id}"

    # Mention tweet metni oluştur
    if username:
        prefix = f"@{username} "
        suffix = f"\n↩️ {tweet_url}"
        max_body = 280 - len(prefix) - len(suffix) - 3
        body = text[:max_body] + "..." if len(text) > max_body else text
        mention_text = f"{prefix}{body}{suffix}"
    else:
        suffix = f"\n↩️ {tweet_url}"
        max_body = 280 - len(suffix) - 3
        body = text[:max_body] + "..." if len(text) > max_body else text
        mention_text = f"{body}{suffix}"

    auth_header = _build_oauth_header(creds, method="POST", url=_TWITTER_TWEET_URL)
    payload = {"text": mention_text}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                _TWITTER_TWEET_URL,
                json=payload,
                headers={"Authorization": auth_header, "Content-Type": "application/json"},
            )

        if response.status_code in (200, 201):
            data = response.json()
            new_id = data.get("data", {}).get("id", "?")
            logger.info(
                "Mention fallback başarılı (id=%s) → @%s tweet %s",
                new_id, username or "?", tweet_id,
            )
            _record_tweet_sent()
            return {"success": True, "reply_tweet_id": new_id, "method": "mention_tweet"}

        error_text = response.text[:300]
        logger.error("Mention fallback hatası: HTTP %s — %s", response.status_code, error_text)
        return {"success": False, "error": f"Mention tweet hatası (HTTP {response.status_code}): {error_text}"}

    except Exception as e:
        logger.error("Mention fallback exception: %s", e)
        return {"success": False, "error": f"Mention tweet exception: {str(e)[:200]}"}


# -------------------------------------------------------
# 4. Quote Tweet + Analiz (Bağımsız özellik)
# -------------------------------------------------------

_QUOTE_ANALYSIS_PROMPT = """Sen BIST ve Türk ekonomisini derinlemesine takip eden, analitik düşünen bir finans yorumcususun. @SZAlgoFinans hesabı adına tweet'leri alıntılayarak özgün, değer katan analizler yapıyorsun.

GÖREV: Verilen tweet hakkında 2 FARKLI Türkçe alıntı analizi üret.

═══ ANALİZ FORMATI ═══
Her analiz:
- 5 cümle: Konuyla doğrudan ilgili, özgün ve bilgilendirici
- Her cümle farklı bir açıdan konuya yaklaşsın (arka plan, bağlam, piyasa etkisi, sektör, beklenti)
- Son satıra yeni satırla 2-3 alakalı hashtag ekle (#BIST #HalkaArz vb.)
- İki analiz birbirinden TON ve ODAK noktasıyla belirgin şekilde farklı olsun
  → Analiz 1: Daha geniş piyasa/sektör perspektifi
  → Analiz 2: Şirket/olay odaklı, spesifik

═══ DİL VE ÜSLUP ═══
- Profesyonel ama anlaşılır Türkçe
- Yapay zekanın değil, piyasayı iyi bilen bir yorumcunun sesi
- Net, kısa ve güçlü cümleler kur — dolgu kelime kullanma
- "yakından takip etmek lazım", "ilginç gelişme" gibi klişelerden kaçın
- Gerçekten değer katan, okuyucuyu düşündüren bir analiz yaz

═══ KESİN YASAKLAR ═══
- Rakam / fiyat / yüzde YAZMA — "hedef 47 TL", "%5 artış bekliyorum" YASAK
- Tavsiye verme — "al", "sat", "gir", "çık" YASAK
- Teknik analiz — destek/direnç/indikatör/formasyon YASAK
- "YT değildir" YAZMA

═══ HASHTAG KURALI ═══
- Her analizin sonuna yeni satırda 2-3 hashtag
- Örnekler: #BIST100 #HalkaArz #BorseIstanbul #Hisse #Bankacılık #Enerji #Teknoloji #Ekonomi
- Konuyla gerçekten alakalı hashtag seç — rastgele koyma

═══ JSON ÇIKTI ═══
{"is_safe": true, "analyses": ["birinci analiz 5 cümle\n#hashtag1 #hashtag2", "ikinci analiz 5 cümle\n#hashtag1 #hashtag2"]}
veya
{"is_safe": false, "analyses": []}

is_safe: false döndür:
- Siyasi içerik
- Finans/borsa/ekonomi dışı konu
- Teknik analiz tweeti (grafik/formasyon/indikatör)
- Tweet çok kısa veya belirsiz"""


async def generate_quote_analysis(tweet_text: str, author_username: str) -> dict:
    """AI ile tweet için 2 farklı alıntı analizi üretir (5 cümle + hashtag).

    Args:
        tweet_text: Orijinal tweet metni
        author_username: Tweet yazarının @kullanıcıadı

    Returns:
        {
            "success": True,
            "is_safe": bool,
            "analyses": [str, str],   # 2 farklı analiz seçeneği
        }
        veya hata durumunda:
        {"success": False, "error": str}
    """
    gemini_key = _get_gemini_key()
    if not gemini_key:
        return {"success": False, "error": "Gemini API key yok."}

    user_message = f"Tweet (@{author_username}):\n\n{tweet_text}"

    payload = {
        "messages": [
            {"role": "system", "content": _QUOTE_ANALYSIS_PROMPT},
            {"role": "user", "content": user_message},
        ],
        "temperature": 0.80,
        "max_tokens": 4096,  # Gemini 2.5 thinking token yiyor
    }

    gemini_headers = {
        "Authorization": f"Bearer {gemini_key}",
        "Content-Type": "application/json",
    }

    # ── 1. Birincil: Gemini 2.5 Flash ──
    content = None

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            response = await client.post(
                _GEMINI_URL, json={**payload, "model": _GEMINI_MODEL}, headers=gemini_headers
            )
        if response.status_code == 200:
            data = response.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if content:
                logger.info("AI quote [Gemini-Flash] kullanildi")
        else:
            logger.warning(f"AI quote Gemini-Flash hatası: HTTP {response.status_code} — {response.text[:200]}")
    except Exception as e:
        logger.warning(f"AI quote Gemini-Flash hata: {e}")

    # ── 2. Yedek: Gemini 2.5 Pro ──
    if not content:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                response = await client.post(
                    _GEMINI_URL, json={**payload, "model": _GEMINI_PRO_MODEL}, headers=gemini_headers
                )
            if response.status_code == 200:
                data = response.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                if content:
                    logger.info("AI quote [Gemini-Pro] kullanildi")
            else:
                logger.error(f"AI quote Gemini-Pro hatası: HTTP {response.status_code} — {response.text[:200]}")
        except Exception as e:
            logger.error(f"AI quote Gemini-Pro hata: {e}")

    if not content:
        return {"success": False, "error": "AI tum providerlar basarisiz."}

    try:
        from app.services.ai_json_helper import safe_parse_json

        result = safe_parse_json(content, required_key="is_safe")
        if result is None:
            logger.error("AI quote JSON parse basarisiz — icerik: %s", content[:200])
            return {"success": False, "error": "AI yanıtı JSON formatında değil — tekrar deneyin."}

        is_safe = result.get("is_safe", False)
        if isinstance(is_safe, str):
            is_safe = is_safe.lower() in ("true", "1", "yes", "evet")

        if not is_safe:
            return {
                "success": True,
                "is_safe": False,
                "analyses": [],
            }

        analyses_raw = result.get("analyses", [])
        if not isinstance(analyses_raw, list) or len(analyses_raw) == 0:
            return {"success": False, "error": "AI analiz üretemedi."}

        # Doğrula ve temizle
        validated = []
        for item in analyses_raw[:2]:
            if isinstance(item, str) and item.strip():
                text = item.strip()
                if len(text) > 500:
                    text = text[:497] + "..."
                validated.append(text)

        if not validated:
            return {"success": False, "error": "AI geçerli analiz üretemedi."}

        return {
            "success": True,
            "is_safe": True,
            "analyses": validated,
        }

    except json.JSONDecodeError as e:
        logger.error(f"AI quote analiz JSON parse hatası: {e}")
        return {"success": False, "error": "AI yanıtı JSON formatında değil — tekrar deneyin."}
    except httpx.TimeoutException:
        logger.error("AI quote analiz zaman aşımı")
        return {"success": False, "error": "AI servisi zaman aşımı — tekrar deneyin."}
    except Exception as e:
        logger.error(f"AI quote analiz hatası: {e}")
        return {"success": False, "error": f"Beklenmeyen hata: {str(e)[:200]}"}


async def send_quote_analysis_tweet(tweet_url: str, analysis_text: str) -> dict:
    """Tweet'i alıntılayarak AI analiziyle quote tweet atar.

    Args:
        tweet_url: Alıntılanacak tweet URL'si (https://x.com/...)
        analysis_text: AI'ın ürettiği analiz metni (5 cümle + hashtag)

    Returns:
        {
            "success": True,
            "quote_tweet_id": str,
            "tweet_url": str,
        }
        veya hata durumunda:
        {"success": False, "error": str}
    """
    tweet_id = _extract_tweet_id(tweet_url)
    if not tweet_id:
        return {"success": False, "error": "Geçersiz tweet URL'si."}

    if not analysis_text or not analysis_text.strip():
        return {"success": False, "error": "Analiz metni gerekli."}

    analysis_text = analysis_text.strip()
    if len(analysis_text) > 4000:
        analysis_text = analysis_text[:3997] + "..."

    creds = _load_credentials()
    if not creds:
        return {"success": False, "error": "Twitter API anahtarları yapılandırılmamış."}

    from app.services.twitter_service import _wait_for_tweet_rate_limit, _record_tweet_sent
    wait = _wait_for_tweet_rate_limit()
    if wait > 0:
        await asyncio.sleep(wait)

    auth_header = _build_oauth_header(creds, method="POST", url=_TWITTER_TWEET_URL)
    payload = {
        "text": analysis_text,
        "quote_tweet_id": tweet_id,
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
            qt_id = data.get("data", {}).get("id", "?")
            logger.info(f"Quote analiz tweet başarılı (id={qt_id}) → tweet {tweet_id}")
            _record_tweet_sent()
            return {
                "success": True,
                "quote_tweet_id": qt_id,
                "tweet_url": f"https://x.com/SZAlgoFinans/status/{qt_id}",
            }

        error_text = response.text[:300]
        logger.error(f"Quote tweet hatası: HTTP {response.status_code} — {error_text}")
        return {
            "success": False,
            "error": f"Twitter API hatası (HTTP {response.status_code}): {error_text[:200]}",
        }

    except httpx.TimeoutException:
        logger.error("Quote tweet gönderme zaman aşımı")
        return {"success": False, "error": "Twitter API zaman aşımı — tekrar deneyin."}
    except Exception as e:
        logger.error(f"Quote tweet gönderme hatası: {e}")
        return {"success": False, "error": f"Beklenmeyen hata: {str(e)[:200]}"}


# -------------------------------------------------------
# 5. Otomatik Reply — Kullanıcı ID Çözümleme
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
        elif response.status_code == 402:
            global _twitter_rate_limited
            _twitter_rate_limited = True
            logger.warning(
                "Twitter API ödeme hatası (user lookup): @%s, HTTP 402 — "
                "bu döngüdeki kalan hedefler atlanacak",
                username,
            )
        elif response.status_code == 403:
            # 403 = korumalı/askıya alınmış hesap → sadece atla (döngüyü durdurma)
            logger.info("Twitter user lookup 403 (korumalı?): @%s — atlanıyor", username)
        elif response.status_code == 429:
            _twitter_rate_limited = True
            logger.warning(f"Twitter API RATE LIMITED (429): user lookup @{username}")
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
        "tweet.fields": "text,created_at,reply_settings",
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

        if response.status_code == 402:
            global _twitter_rate_limited
            _twitter_rate_limited = True
            logger.warning(
                "Twitter API ödeme hatası (GET tweets): user_id=%s, HTTP 402 — "
                "bu döngüdeki kalan hedefler atlanacak",
                user_id,
            )
            return []

        if response.status_code == 403:
            # 403 = korumalı hesap veya erişim yok → sadece bu hesabı atla
            logger.info(
                "Twitter GET tweets 403 (korumalı hesap?): user_id=%s — atlanıyor",
                user_id,
            )
            return []

        if response.status_code == 429:
            _twitter_rate_limited = True
            reset_ts = response.headers.get("x-rate-limit-reset", "?")
            logger.warning(
                "Twitter API RATE LIMITED (429): user_id=%s, reset=%s — "
                "bu döngüdeki kalan hedefler atlanacak",
                user_id, reset_ts,
            )
            return []

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
                "reply_settings": tweet.get("reply_settings", "everyone"),
            })

        return result

    except Exception as e:
        logger.error(f"User tweets hatası: {user_id} — {e}")
        return []


# -------------------------------------------------------
# 6. Otomatik Reply — Ana Döngü (Scheduler'dan çağrılır)
# -------------------------------------------------------

async def _seed_default_targets(session):
    """Başlangıç reply hedeflerini DB'ye ekler (yoksa).

    Args:
        session: Mevcut AsyncSession (dışarıdan verilir — bağımsız session AÇMAZ)
    """
    try:
        from app.models.user import ReplyTarget, DEFAULT_REPLY_TARGETS
        from sqlalchemy import select

        for username in DEFAULT_REPLY_TARGETS:
            existing = await session.execute(
                select(ReplyTarget).where(ReplyTarget.username == username)
            )
            if not existing.scalar_one_or_none():
                session.add(ReplyTarget(username=username, is_active=True))
                logger.info(f"Reply hedefi eklendi: @{username}")
        await session.flush()

    except Exception as e:
        logger.error(f"Reply hedef seed hatası: {e}")


async def _is_auto_reply_enabled(session) -> bool:
    """Auto-reply toggle durumunu DB'den kontrol eder.

    Args:
        session: Mevcut AsyncSession (dışarıdan verilir — bağımsız session AÇMAZ)
    """
    try:
        from app.models.app_setting import AppSetting
        from sqlalchemy import select

        result = await session.execute(
            select(AppSetting).where(AppSetting.key == "AUTO_REPLY_ENABLED")
        )
        setting = result.scalar_one_or_none()
        if setting:
            return setting.value.lower() in ("true", "1", "yes")
        return True  # Default: açık

    except Exception:
        return True  # Hata durumunda açık varsay


async def _get_daily_limit(session) -> int:
    """Günlük reply limitini DB'den okur (admin panelden ayarlanır).

    Args:
        session: Mevcut AsyncSession (dışarıdan verilir — bağımsız session AÇMAZ)
    """
    try:
        from app.models.app_setting import AppSetting
        from sqlalchemy import select

        result = await session.execute(
            select(AppSetting).where(AppSetting.key == "AUTO_REPLY_DAILY_LIMIT")
        )
        setting = result.scalar_one_or_none()
        if setting:
            limit_val = int(setting.value)
            logger.info("Günlük reply limiti (DB): %d", limit_val)
            return limit_val
    except Exception as e:
        logger.warning("Günlük reply limiti okunamadı: %s", e)
    logger.info("Günlük reply limiti (varsayılan): %d", _AUTO_REPLY_DAILY_LIMIT_DEFAULT)
    return _AUTO_REPLY_DAILY_LIMIT_DEFAULT


async def _get_today_reply_count(session) -> int:
    """Bugün kaç reply atıldığını sayar.

    Args:
        session: Mevcut AsyncSession (dışarıdan verilir — bağımsız session AÇMAZ)
    """
    try:
        from app.models.user import AutoReply
        from sqlalchemy import select, func

        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

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
    """Otomatik reply ana döngüsü — scheduler'dan 30dk'da bir çağrılır.

    KRİTİK KURALLAR:
    - Sadece YENİ tweetlere reply atar (last_seen_tweet_id'den sonrakiler)
    - İlk çalışmada: mevcut tweetlerin ID'sini kaydeder, reply ATMAZ
    - Her reply arasında 30sn-3dk rastgele jitter (doğal görünüm)
    - Her 5 reply'da 1 beğeni atar
    - Günlük reply limiti (DB'den, gece 00:00'da sıfırlanır)
    - Siyasi/borsa dışı tweetlere reply ATMAZ (AI filtresi)

    SESSION CONSOLIDATION:
    - TEK bir async session açılır ve tüm helper fonksiyonlara iletilir
    - Jitter bekleme sırasında session kapatılır, sonra yeniden açılır
    - Bu sayede QueuePool overflow önlenir (eski: 5+ ayrı session)

    Twitter API Kredi Tasarrufu:
    - 30dk interval × 8 hedef/döngü = max 384 GET/gün (~150-250 gerçek)
    - 24 reply limiti dolunca GET yapmaz → 0 maliyet
    - 402/403 alınca döngü hemen durur (circuit breaker)
    - 429 alınca döngü hemen durur (rate limit)
    """
    global _twitter_rate_limited

    if _AUTO_REPLY_LOCK.locked():
        logger.debug("Auto-reply zaten çalışıyor, atlıyorum")
        return

    async with _AUTO_REPLY_LOCK:
        try:
            # Her döngü başında rate limit flag'ini resetle
            _twitter_rate_limited = False

            from app.database import async_session
            from app.models.user import ReplyTarget, AutoReply
            from sqlalchemy import select

            # ─── TEK SESSION: Tüm DB işlemleri bu session üzerinden ───
            async with async_session() as session:

                # Toggle kontrolü (aynı session ile)
                if not await _is_auto_reply_enabled(session):
                    logger.info("Auto-reply devre dışı (toggle kapalı)")
                    return

                # Seed default targets (aynı session ile)
                await _seed_default_targets(session)

                # Günlük limit (aynı session ile)
                daily_limit = await _get_daily_limit(session)
                today_count = await _get_today_reply_count(session)
                logger.info(
                    "Reply durum: bugün %d/%d, rate_limit=30dk/hesap, "
                    "max_hedef/döngü=%d, max_reply/döngü=%d",
                    today_count, daily_limit, _MAX_TARGETS_PER_CYCLE,
                    _MAX_REPLIES_PER_CYCLE,
                )
                if today_count >= daily_limit:
                    logger.info(f"Günlük reply limiti doldu: {today_count}/{daily_limit} — durduruluyor")
                    return

                remaining = daily_limit - today_count

                # Aktif hedefleri çek
                result = await session.execute(
                    select(ReplyTarget).where(ReplyTarget.is_active == True)
                )
                targets = list(result.scalars().all())

                if not targets:
                    logger.info("Aktif reply hedefi yok — döngü bitti")
                    return

                # ─── AKILLI SIRALAMA ───
                # En uzun süredir kontrol edilmeyen hedefler önce
                targets.sort(
                    key=lambda t: t.last_reply_at or datetime.min.replace(tzinfo=timezone.utc)
                )

                logger.info(
                    "Auto-reply: %d hedef toplam, kalan quota %d, "
                    "bu döngü max %d hedef kontrol edilecek",
                    len(targets), remaining, _MAX_TARGETS_PER_CYCLE,
                )

                replies_sent = 0
                total_liked = 0
                api_calls = 0
                skipped_rate_limit = 0
                skipped_no_tweets = 0
                skipped_init = 0

                # ─── Reply bekleyen tweetleri topla (jitter öncesi) ───
                # Önce TÜM hedefleri tara, reply atılacak tweetleri listeye al
                # Sonra session'ı kapat, jitter bekle, yeni session ile reply at
                pending_replies = []  # [(target_id, target_username, tweet_id, tweet_text, chosen_reply)]

                for target in targets:
                    if len(pending_replies) >= _MAX_REPLIES_PER_CYCLE:
                        break  # Bu döngüde yeterli reply toplandı, geri kalanı sonraki döngüye
                    if len(pending_replies) + replies_sent >= remaining:
                        break

                    # ─── TWITTER API RATE LIMIT KORUMASI ───
                    if _twitter_rate_limited:
                        logger.warning("Twitter API rate limited — kalan hedefler sonraki döngüye")
                        break

                    if api_calls >= _MAX_TARGETS_PER_CYCLE:
                        logger.info(
                            "Döngü API limiti doldu (%d/%d) — kalan hedefler sonraki döngüye",
                            api_calls, _MAX_TARGETS_PER_CYCLE,
                        )
                        break

                    # ─── HESAP BAZLI RATE LIMIT (30 dk) ───
                    now_utc = datetime.now(timezone.utc)
                    if target.last_reply_at:
                        seconds_since = (now_utc - target.last_reply_at).total_seconds()
                        if seconds_since < 1800:
                            skipped_rate_limit += 1
                            continue

                    # User ID çözümle
                    user_id = target.twitter_user_id
                    if not user_id:
                        user_id = await get_user_id_by_username(target.username)
                        api_calls += 1
                        if _twitter_rate_limited:
                            break
                        if user_id:
                            target.twitter_user_id = user_id
                            await session.flush()
                        else:
                            logger.warning(f"User ID çözümlenemedi: @{target.username}")
                            continue

                    # ─── İLK ÇALIŞMA KONTROLÜ ───
                    if not target.last_seen_tweet_id:
                        init_tweets = await fetch_user_recent_tweets(user_id)
                        api_calls += 1
                        if _twitter_rate_limited:
                            break
                        if init_tweets:
                            newest_id = max(t["id"] for t in init_tweets)
                            target.last_seen_tweet_id = newest_id
                            await session.flush()
                            logger.info(
                                "İlk tarama @%s: %d tweet atlandı, since_id=%s",
                                target.username, len(init_tweets), newest_id,
                            )
                        else:
                            logger.info(f"İlk tarama @{target.username}: tweet bulunamadı")
                        skipped_init += 1
                        continue

                    # ─── YENİ TWEETLERİ ÇEK ───
                    tweets = await fetch_user_recent_tweets(
                        user_id,
                        since_id=target.last_seen_tweet_id,
                    )
                    api_calls += 1

                    if _twitter_rate_limited:
                        break

                    if not tweets:
                        skipped_no_tweets += 1
                        continue

                    # En yeni tweet ID'yi güncelle
                    newest_id = max(t["id"] for t in tweets)
                    target.last_seen_tweet_id = newest_id
                    await session.flush()

                    logger.info(f"@{target.username}: {len(tweets)} yeni tweet bulundu")

                    for tweet in tweets:
                        if len(pending_replies) >= _MAX_REPLIES_PER_CYCLE:
                            break
                        if len(pending_replies) + replies_sent >= remaining:
                            break

                        tweet_id = tweet["id"]
                        tweet_text = tweet["text"]
                        tweet_created = tweet.get("created_at", "")

                        # ─── ESKİ TWEET KONTROLÜ ───
                        # Sadece son _MAX_TWEET_AGE_MINUTES dk'daki tweetlere reply at
                        # Deploy sonrası biriken eski tweetlere spam yapmayı önler
                        if tweet_created:
                            try:
                                tweet_time = datetime.fromisoformat(
                                    tweet_created.replace("Z", "+00:00")
                                )
                                age_minutes = (
                                    datetime.now(timezone.utc) - tweet_time
                                ).total_seconds() / 60
                                if age_minutes > _MAX_TWEET_AGE_MINUTES:
                                    continue  # Eski tweet, atla
                            except (ValueError, TypeError):
                                pass  # Parse edilemezse devam et

                        # Çok kısa tweetleri atla (karakter + kelime kontrolü)
                        clean_text = tweet_text.strip()
                        if len(clean_text) < 15:
                            continue
                        word_count = len(clean_text.split())
                        if word_count < _MIN_TWEET_WORDS:
                            continue  # Yeterli kelime yok, anlamlı reply üretilemez

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

                        # Rastgele reply seç
                        reply_options = ai_result.get("replies", [])
                        if not reply_options:
                            continue

                        chosen_reply = random.choice(reply_options)
                        pending_replies.append((
                            target.id, target.username, tweet_id, tweet_text, chosen_reply
                        ))

                # Tarama kısmını commit'le (last_seen_tweet_id güncellemeleri)
                await session.commit()

            # ─── SESSION KAPANDI — Artık jitter beklemesi pool'u TUTMAZ ───

            # Bekleyen reply'ları gönder (her biri için kısa session aç-kapa)
            for target_id, target_username, tweet_id, tweet_text, chosen_reply in pending_replies:
                if replies_sent >= remaining:
                    break

                if _twitter_rate_limited:
                    break

                # ─── JITTER: Doğal gecikme (30sn - 3dk) ───
                jitter = random.uniform(_JITTER_MIN, _JITTER_MAX)
                logger.info(
                    f"Reply gönderilecek: @{target_username} tweet {tweet_id} "
                    f"— {jitter:.0f}sn sonra → \"{chosen_reply[:50]}...\""
                )
                await asyncio.sleep(jitter)

                # Reply gönder (Twitter API — session gerektirmez)
                send_result = await send_reply(tweet_id, chosen_reply)

                # ── CIRCUIT BREAKER: Ödeme/yetki hatası yönetimi ──
                error_msg = send_result.get("error", "")

                # 402 = ödeme sorunu → TÜM döngüyü durdur (hesap seviye)
                if "HTTP 402" in error_msg:
                    logger.warning(
                        "Twitter API ödeme hatası (402) — kalan reply'lar iptal: %s",
                        error_msg[:100],
                    )
                    async with async_session() as err_session:
                        err_session.add(AutoReply(
                            target_tweet_id=tweet_id,
                            target_username=target_username,
                            target_text=tweet_text[:1000],
                            reply_text=chosen_reply,
                            status="failed",
                            error_message=error_msg[:500],
                        ))
                        await err_session.commit()
                    break  # Kalan pending_replies'ı atla — kredi yok

                # 403 → bu tweeti atla (mention edilmeden reply yasak — tweet-level kısıtlama)
                if "403" in error_msg:
                    logger.info(
                        "Reply 403: @%s tweet %s — atlanıyor: %s",
                        target_username, tweet_id, error_msg[:100],
                    )
                    async with async_session() as err_session:
                        err_session.add(AutoReply(
                            target_tweet_id=tweet_id,
                            target_username=target_username,
                            target_text=tweet_text[:1000],
                            reply_text=chosen_reply,
                            status="skipped",
                            error_message=f"Reply 403: {error_msg[:400]}",
                        ))
                        await err_session.commit()
                    continue  # Bu tweeti atla, sonraki reply'a devam et

                # Sonucu DB'ye kaydet (KISA session — hemen kapanır)
                async with async_session() as session:
                    if send_result.get("success"):
                        session.add(AutoReply(
                            target_tweet_id=tweet_id,
                            target_username=target_username,
                            target_text=tweet_text[:1000],
                            reply_text=chosen_reply,
                            reply_tweet_id=send_result.get("reply_tweet_id"),
                            status="replied",
                        ))
                        # Target'ın last_reply_at güncelle
                        target_obj = await session.get(ReplyTarget, target_id)
                        if target_obj:
                            target_obj.last_reply_at = datetime.now(timezone.utc)
                        await session.commit()
                        replies_sent += 1
                        method = send_result.get("method", "reply")
                        logger.info(
                            f"Auto-reply #{today_count + replies_sent} ({method}): "
                            f"@{target_username} → \"{chosen_reply[:60]}\""
                        )

                        # ─── BEĞENİ: Her 5 reply'da 1 beğeni ───
                        if replies_sent % _LIKE_EVERY_N == 0:
                            like_ok = await _like_tweet(tweet_id)
                            if like_ok:
                                total_liked += 1
                    else:
                        session.add(AutoReply(
                            target_tweet_id=tweet_id,
                            target_username=target_username,
                            target_text=tweet_text[:1000],
                            reply_text=chosen_reply,
                            status="failed",
                            error_message=send_result.get("error", "Gönderim hatası"),
                        ))
                        await session.commit()
                        logger.error(
                            f"Auto-reply başarısız: @{target_username} — {send_result.get('error')}"
                        )

            logger.info(
                "Auto-reply döngü bitti: %d reply, %d beğeni, %d API call, "
                "atlandı: %d rate-limit, %d tweet-yok, %d ilk-tarama, "
                "bekleyen: %d",
                replies_sent, total_liked, api_calls,
                skipped_rate_limit, skipped_no_tweets, skipped_init,
                len(pending_replies) - replies_sent,
            )

        except Exception as e:
            logger.error(f"Auto-reply döngü hatası: {e}", exc_info=True)
