"""X (Twitter) Mentions Auto-Reply Servisi

Bize mention atan veya tweet'lerimize yorum yapan kullanıcılara
Yapay Zeka ile otomatik Türkçe yanıt üretir ve gönderir.

Fark: Bu servis yalnızca BİZE GELEN etkileşimlere cevap verir —
karşı taraf zaten bizi mention ettiği için reply kısıtlaması (403) YOK.

Akış:
  1. GET /2/users/{X_USER_ID}/mentions — son 5dk'daki yeni mention'ları çek
  2. Zaten yanıtlananları atla (AutoReply tablosu)
  3. Tweet metni + bağlantılı orijinal tweetimiz + kişinin profiline bak
  4. AI ile Türkçe reply üret (3 seçenek → rastgele 1 tane)
  5. 25-55 saniye jitter ile gönder
  6. AutoReply tablosuna kaydet
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

# ───────────────────────────────────────────────
# Sabitler
# ───────────────────────────────────────────────

_ABACUS_URL   = "https://routellm.abacus.ai/v1/chat/completions"
_AI_MODEL     = "gpt-4.1"
_AI_TIMEOUT   = 25

_MENTIONS_URL     = "https://api.twitter.com/2/users/{user_id}/mentions"
_TWEET_LOOKUP_URL = "https://api.twitter.com/2/tweets/{tweet_id}"
_TWEET_POST_URL   = "https://api.twitter.com/2/tweets"
_USER_LOOKUP_URL  = "https://api.twitter.com/2/users/{user_id}"

JITTER_MIN = 45    # saniye — doğal görünüm için rastgele gecikme
JITTER_MAX = 180   # saniye (3 dk) — çok hızlı cevap robot gibi görünür

_MAX_TWEET_AGE_MINUTES = 360  # 6 saat — Render restart sonrası kaçırılan mention'ları yakalamak için genişletildi (since_id ile zaten sadece yeniler gelir)
_MAX_REPLIES_PER_CYCLE = 3    # tek döngüde max 3 yanıt (spam önleme)

_MENTIONS_REPLY_LOCK   = asyncio.Lock()

# AppSetting key'leri
_SETTING_ENABLED  = "MENTIONS_REPLY_ENABLED"
_SETTING_SINCE_ID = "MENTIONS_LAST_SEEN_ID"

# ───────────────────────────────────────────────
# OAuth 1.0a yardımcıları (mevcut servis ile aynı)
# ───────────────────────────────────────────────

def _load_creds() -> dict | None:
    try:
        from app.config import get_settings
        s = get_settings()
        if not all([s.X_API_KEY, s.X_API_SECRET, s.X_ACCESS_TOKEN, s.X_ACCESS_TOKEN_SECRET]):
            return None
        return {
            "api_key":            s.X_API_KEY,
            "api_secret":         s.X_API_SECRET,
            "access_token":       s.X_ACCESS_TOKEN,
            "access_token_secret": s.X_ACCESS_TOKEN_SECRET,
        }
    except Exception:
        return None


def _get_abacus_key() -> str | None:
    try:
        from app.config import get_settings
        k = get_settings().ABACUS_API_KEY
        return k if k else None
    except Exception:
        return None


def _get_my_user_id() -> str | None:
    try:
        from app.config import get_settings
        uid = getattr(get_settings(), "X_USER_ID", None)
        return str(uid) if uid else None
    except Exception:
        return None


def _oauth_sign(method: str, url: str, all_params: dict,
                api_secret: str, token_secret: str) -> str:
    sorted_p = sorted(all_params.items())
    param_str = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted_p
    )
    base = (
        f"{method.upper()}&"
        f"{urllib.parse.quote(url, safe='')}&"
        f"{urllib.parse.quote(param_str, safe='')}"
    )
    key = (
        f"{urllib.parse.quote(api_secret, safe='')}&"
        f"{urllib.parse.quote(token_secret, safe='')}"
    )
    h = hmac.new(key.encode(), base.encode(), hashlib.sha1)
    return base64.b64encode(h.digest()).decode()


def _oauth_header(creds: dict, method: str, url: str,
                  extra_params: dict | None = None) -> str:
    """OAuth 1.0a Authorization header. extra_params = query params imzaya dahil edilir."""
    base_oauth = {
        "oauth_consumer_key":     creds["api_key"],
        "oauth_nonce":            uuid.uuid4().hex,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp":        str(int(time.time())),
        "oauth_token":            creds["access_token"],
        "oauth_version":          "1.0",
    }
    sign_params = {**base_oauth, **(extra_params or {})}
    sig = _oauth_sign(method, url, sign_params,
                      creds["api_secret"], creds["access_token_secret"])
    base_oauth["oauth_signature"] = sig
    parts = ", ".join(
        f'{urllib.parse.quote(k, safe="")}="{urllib.parse.quote(v, safe="")}"'
        for k, v in sorted(base_oauth.items())
    )
    return f"OAuth {parts}"


# ───────────────────────────────────────────────
# DB yardımcıları
# ───────────────────────────────────────────────

async def _get_setting(session, key: str) -> str | None:
    try:
        from app.models.app_setting import AppSetting
        from sqlalchemy import select
        r = await session.execute(select(AppSetting).where(AppSetting.key == key))
        s = r.scalar_one_or_none()
        return s.value if s else None
    except Exception:
        return None


async def _set_setting(session, key: str, value: str) -> None:
    try:
        from app.models.app_setting import AppSetting
        from sqlalchemy import select
        r = await session.execute(select(AppSetting).where(AppSetting.key == key))
        s = r.scalar_one_or_none()
        if s:
            s.value = value
        else:
            session.add(AppSetting(key=key, value=value))
        await session.commit()
    except Exception as e:
        logger.error(f"AppSetting kaydetme hatası: {e}")


async def _is_already_replied(session, tweet_id: str) -> bool:
    try:
        from app.models.user import AutoReply
        from sqlalchemy import select
        r = await session.execute(
            select(AutoReply).where(AutoReply.target_tweet_id == tweet_id)
        )
        return r.scalar_one_or_none() is not None
    except Exception:
        return False


async def _log_reply(session, tweet_id: str, username: str,
                     tweet_text: str, reply_text: str,
                     reply_tweet_id: str | None, status: str,
                     error: str | None = None) -> None:
    try:
        from app.models.user import AutoReply
        session.add(AutoReply(
            target_tweet_id=tweet_id,
            target_username=username,
            target_text=tweet_text[:500],
            reply_text=reply_text[:500],
            reply_tweet_id=reply_tweet_id,
            status=status,
            error_message=error,
        ))
        await session.commit()
    except Exception as e:
        logger.error(f"AutoReply kaydetme hatası: {e}")


# ───────────────────────────────────────────────
# Twitter API çağrıları
# ───────────────────────────────────────────────

async def _fetch_mentions(user_id: str, since_id: str | None,
                           creds: dict) -> list[dict]:
    """GET /2/users/{user_id}/mentions — yeni mention'ları çeker."""
    url = _MENTIONS_URL.format(user_id=user_id)
    query: dict[str, str] = {
        "max_results":        "10",
        "tweet.fields":       "text,author_id,in_reply_to_user_id,referenced_tweets,created_at",
        "expansions":         "author_id,referenced_tweets.id",
        "user.fields":        "username,name,description",
    }
    if since_id:
        query["since_id"] = since_id

    auth = _oauth_header(creds, "GET", url, query)
    try:
        async with httpx.AsyncClient(timeout=15.0) as c:
            resp = await c.get(url, params=query, headers={"Authorization": auth})
        if resp.status_code == 200:
            return resp.json()  # tam JSON — data + includes
        logger.warning(f"Mentions API HTTP {resp.status_code}: {resp.text[:200]}")
        return []
    except Exception as e:
        logger.error(f"fetch_mentions hata: {e}")
        return []


async def _fetch_tweet_text(tweet_id: str, creds: dict) -> str | None:
    """Belirli bir tweet'in metnini çeker (orijinal tweetimizi almak için)."""
    url = _TWEET_LOOKUP_URL.format(tweet_id=tweet_id)
    query = {"tweet.fields": "text"}
    auth = _oauth_header(creds, "GET", url, query)
    try:
        async with httpx.AsyncClient(timeout=10.0) as c:
            resp = await c.get(url, params=query, headers={"Authorization": auth})
        if resp.status_code == 200:
            return resp.json().get("data", {}).get("text")
        return None
    except Exception:
        return None


async def _post_reply(tweet_id: str, text: str, creds: dict) -> dict:
    """Tweet'e direkt reply atar. Mention yapan kişi bizi engage ettiği için 403 yok."""
    auth = _oauth_header(creds, "POST", _TWEET_POST_URL)
    body = {"text": text, "reply": {"in_reply_to_tweet_id": tweet_id}}
    try:
        async with httpx.AsyncClient(timeout=15.0) as c:
            resp = await c.post(
                _TWEET_POST_URL,
                json=body,
                headers={"Authorization": auth, "Content-Type": "application/json"},
            )
        if resp.status_code in (200, 201):
            data = resp.json().get("data", {})
            return {"success": True, "tweet_id": data.get("id")}
        logger.error(f"Reply POST {resp.status_code}: {resp.text[:300]}")
        return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
    except Exception as e:
        logger.error(f"post_reply hata: {e}")
        return {"success": False, "error": str(e)}


# ───────────────────────────────────────────────
# AI Reply Üretme
# ───────────────────────────────────────────────

_MENTIONS_SYSTEM_PROMPT = """Sen BIST (Borsa İstanbul) odaklı bir finans bilgi hesabısın: @SZAlgoFinans.
Birisi sana mention attı ya da paylaştığın tweete yorum yaptı. Bu kişiye kaliteli, kısa ve konuyla ilgili Türkçe cevap yazacaksın.

═══ KİMLİĞİN ═══
- BIST halka arz ve piyasa verilerini takip eden, algoritmalı finans botu
- Bilgilendirici, samimi, doğal Türkçe konuşan
- Asla yatırım tavsiyesi vermeyen — sadece bilgi paylaşan

═══ DİL KALİTESİ ═══
- Doğal, akıcı Türkçe — ne çok resmi ne çok argo
- YASAK: "valla", "harbiden", "baya", "bi" (bir yerine), "abi", "lan"
- Kısa ve net cümleler — Twitter'a uygun
- Emoji: 3 öneriden en fazla 1'inde, sadece 1 emoji

═══ 3 REPLY ÖNERİSİ ÜRET ═══
1. BİLGİLENDİRİCİ (12-20 kelime): Konuyla ilgili faydalı bilgi veya açıklama ekle
2. KISA TEPKİ (4-8 kelime): Doğal, samimi ve kısa bir yanıt
3. SORU/KATILIM (8-15 kelime): Konuya dahil eden veya merak uyandıran bir soru/yorum

═══ KONU FİLTRESİ ═══
SADECE bunlara yanıt ver (is_safe: true):
- Halka arz soruları ve yorumları
- BIST/borsa genel soruları
- Ekonomi, piyasa, şirket haberleri
- Bizim paylaştığımız içeriklerle ilgili sorular/yorumlar

YASAK (is_safe: false):
- Teknik analiz (grafik, RSI, formasyon, destek/direnç)
- Siyaset, din, spor, magazin
- Hakaret, tartışma, provokasyon
- Yatırım tavsiyesi talepleri ("ne alayım", "satar mısın" vb. — kibarca "YT değiliz" de)
- Anlamsız veya spam tweetler

═══ KESİN KURALLAR ═══
1. Fiyat hedefi, yüzde, rakam YAZMA
2. "Yatırım tavsiyesi değildir" YAZMA (gereksiz uzatır)
3. Zorlama — konuyu anlayamıyorsan is_safe: false dön
4. Eğer yatırım tavsiyesi soruluyorsa kibarca "Yatırım tavsiyesi vermiyoruz, bilgi için takipte kalın 📊" de

═══ ÇIKTI FORMATI (JSON) ═══
{
  "is_safe": true/false,
  "reason": "kısa açıklama (is_safe: false ise)",
  "replies": ["reply1", "reply2", "reply3"]
}"""


async def _generate_reply(mention_text: str, our_tweet_text: str | None,
                          author_name: str, author_bio: str | None) -> dict:
    """Abacus AI ile mention'a yanıt üretir."""
    api_key = _get_abacus_key()
    if not api_key:
        return {"is_safe": False, "reason": "API key yok"}

    context_parts = [f"Bize yazan kişi: {author_name}"]
    if author_bio:
        context_parts.append(f"Bio: {author_bio[:100]}")
    if our_tweet_text:
        context_parts.append(f"Bizim twetimiz (yorumladığı): {our_tweet_text[:200]}")
    context_parts.append(f"Gelen mention/yorum: {mention_text}")

    user_msg = "\n".join(context_parts)

    payload = {
        "model": _AI_MODEL,
        "messages": [
            {"role": "system", "content": _MENTIONS_SYSTEM_PROMPT},
            {"role": "user",   "content": user_msg},
        ],
        "temperature": 0.85,
        "max_tokens":  400,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as c:
            resp = await c.post(_ABACUS_URL, json=payload, headers=headers)
        if resp.status_code != 200:
            logger.error(f"AI yanıt üretilemedi: HTTP {resp.status_code}")
            return {"is_safe": False, "reason": f"AI HTTP {resp.status_code}"}

        raw = resp.json()["choices"][0]["message"]["content"].strip()
        # JSON bloğu varsa çıkar
        m = re.search(r"\{[\s\S]*\}", raw)
        if m:
            raw = m.group(0)
        result = json.loads(raw)
        return result
    except json.JSONDecodeError:
        logger.error(f"AI JSON parse hatası: {raw[:200]}")
        return {"is_safe": False, "reason": "JSON parse hatası"}
    except Exception as e:
        logger.error(f"AI hata: {e}")
        return {"is_safe": False, "reason": str(e)}


# ───────────────────────────────────────────────
# Ana döngü
# ───────────────────────────────────────────────

async def mentions_reply_cycle() -> None:
    """Scheduler tarafından her 5 dakikada çağrılır.

    1. MENTIONS_REPLY_ENABLED kontrolü
    2. Yeni mention'ları çek
    3. Her biri için AI reply üret ve gönder (jitter ile)
    """
    if _MENTIONS_REPLY_LOCK.locked():
        logger.debug("Mentions reply döngüsü zaten çalışıyor, atlandı")
        return

    async with _MENTIONS_REPLY_LOCK:
        try:
            await _run_mentions_cycle()
        except Exception as e:
            logger.error(f"mentions_reply_cycle kritik hata: {e}", exc_info=True)


async def _run_mentions_cycle() -> None:
    from app.database import async_session

    async with async_session() as session:
        # ── 1. Etkin mi? ──
        enabled_str = await _get_setting(session, _SETTING_ENABLED)
        if not enabled_str or enabled_str.lower() not in ("1", "true", "yes"):
            logger.info("MENTIONS_REPLY_ENABLED kapalı (değer: %s) — admin panelden açılmalı", enabled_str)
            return

        # ── 2. Credentials ──
        creds = _load_creds()
        if not creds:
            logger.warning("Twitter credentials eksik — mentions reply durdu")
            return

        user_id = _get_my_user_id()
        if not user_id:
            logger.warning("X_USER_ID tanımlı değil — mentions reply durdu")
            return

        # ── 3. Son görülen ID ──
        since_id = await _get_setting(session, _SETTING_SINCE_ID)

        # ── 4. Mention'ları çek ──
        raw = await _fetch_mentions(user_id, since_id, creds)
        if not raw or not isinstance(raw, dict):
            return

        mentions = raw.get("data", [])
        if not mentions:
            logger.debug("Yeni mention yok")
            return

        includes   = raw.get("includes", {})
        users_map  = {u["id"]: u for u in includes.get("users", [])}
        tweets_map = {t["id"]: t for t in includes.get("tweets", [])}

        # En yeni ID'yi since_id olarak kaydet (her zaman güncelle)
        newest_id = max(m["id"] for m in mentions)
        await _set_setting(session, _SETTING_SINCE_ID, newest_id)

        logger.info(f"Mentions: {len(mentions)} yeni mention bulundu")

        # ── 5. Her mention'a yanıt ──
        replies_sent = 0
        for mention in mentions:
            if replies_sent >= _MAX_REPLIES_PER_CYCLE:
                logger.info(f"Döngü limiti ({_MAX_REPLIES_PER_CYCLE}) doldu, bekleniyor")
                break

            tweet_id   = mention["id"]
            tweet_text = mention.get("text", "")
            author_id  = mention.get("author_id", "")
            created_at = mention.get("created_at", "")

            # Yaş kontrolü — sadece son 5 dakikadaki mention'lara cevap ver
            if created_at:
                try:
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    age_minutes = (datetime.now(timezone.utc) - dt).total_seconds() / 60
                    if age_minutes > _MAX_TWEET_AGE_MINUTES:
                        logger.debug(f"  {tweet_id} çok eski ({age_minutes:.1f}dk), atlandı")
                        continue
                except Exception:
                    pass

            # Zaten yanıtlandı mı?
            if await _is_already_replied(session, tweet_id):
                logger.debug(f"  {tweet_id} zaten yanıtlanmış, atlandı")
                continue

            # Yazar bilgisi
            author    = users_map.get(author_id, {})
            username  = author.get("username", "unknown")
            author_name = author.get("name", username)
            author_bio  = author.get("description", "")

            # Bağlantılı tweetimiz var mı? (bize reply ise parent tweet metnini al)
            our_tweet_text: str | None = None
            ref_tweets = mention.get("referenced_tweets", [])
            for ref in ref_tweets:
                if ref.get("type") == "replied_to":
                    # includes.tweets içinde var mı?
                    ref_data = tweets_map.get(ref["id"])
                    if ref_data:
                        our_tweet_text = ref_data.get("text")
                    else:
                        # API'den çek
                        our_tweet_text = await _fetch_tweet_text(ref["id"], creds)
                    break

            logger.info(f"  @{username}: {tweet_text[:60]}...")

            # ── AI reply üret ──
            ai_result = await _generate_reply(
                mention_text=tweet_text,
                our_tweet_text=our_tweet_text,
                author_name=author_name,
                author_bio=author_bio,
            )

            if not ai_result.get("is_safe"):
                reason = ai_result.get("reason", "güvensiz")
                logger.info(f"  @{username} → is_safe=false ({reason}), atlandı")
                await _log_reply(session, tweet_id, username, tweet_text,
                                 "", None, "skipped", reason)
                continue

            replies = ai_result.get("replies", [])
            if not replies:
                logger.warning(f"  @{username} → AI boş reply döndü")
                continue

            chosen = random.choice(replies)

            # ── Jitter ──
            delay = random.randint(JITTER_MIN, JITTER_MAX)
            logger.info(f"  @{username} → {delay}sn bekleyip reply atılıyor: {chosen[:50]}...")
            await asyncio.sleep(delay)

            # ── Gönder ──
            result = await _post_reply(tweet_id, chosen, creds)

            if result.get("success"):
                replies_sent += 1
                logger.info(f"  ✅ @{username} yanıtlandı (ID: {result.get('tweet_id')})")
                await _log_reply(session, tweet_id, username, tweet_text,
                                 chosen, result.get("tweet_id"), "replied")
            else:
                err = result.get("error", "bilinmeyen hata")
                logger.error(f"  ❌ @{username} yanıt gönderilemedi: {err}")
                await _log_reply(session, tweet_id, username, tweet_text,
                                 chosen, None, "failed", err)

        logger.info(f"Mentions reply döngüsü tamamlandı: {replies_sent} yanıt gönderildi")
