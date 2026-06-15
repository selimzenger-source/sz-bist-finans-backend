"""Meta (Facebook Page + Instagram) Graph API — çapraz paylaşım.

X thread admin onayında ('Gönder'), aynı içeriği:
  - Facebook Page'e TAM POST (parça parça değil; tek gönderi, görsel + tam metin)
  - Instagram'a RESİMLİ POST (kapak görseli + tam açıklama caption)

Token-bazlı Graph API — tarayıcı/oturum gerektirmez, Render'da çalışır.
Kaynak: auto-video-pipeline/social_poster.py (kanıtlanmış akış) — backend'e port.

Token'lar Render env'inde: FB_PAGE_ID, FB_PAGE_ACCESS_TOKEN,
INSTAGRAM_ACCESS_TOKEN, INSTAGRAM_BUSINESS_ACCOUNT_ID. Yoksa platform atlanır.
"""
from __future__ import annotations

import logging
import os

import requests

from app.config import get_settings

logger = logging.getLogger(__name__)

_GRAPH = "https://graph.facebook.com/v25.0"
# IG, görseli public URL'den çeker — backend /static'i public sunar.
_PUBLIC_BASE = "https://sz-bist-finans-api.onrender.com"


def _cfg() -> dict:
    s = get_settings()
    return {
        "fb_page_id": (getattr(s, "FB_PAGE_ID", "") or "").strip(),
        "fb_token": (getattr(s, "FB_PAGE_ACCESS_TOKEN", "") or "").strip(),
        "ig_account": (getattr(s, "INSTAGRAM_BUSINESS_ACCOUNT_ID", "") or "").strip(),
        "ig_token": (getattr(s, "INSTAGRAM_ACCESS_TOKEN", "") or "").strip(),
    }


def _image_public_url(image_path: str | None) -> str | None:
    """static/tmp altındaki görselin public URL'ini üretir (IG için)."""
    if not image_path:
        return None
    norm = image_path.replace("\\", "/")
    idx = norm.find("/static/")
    if idx >= 0:
        return _PUBLIC_BASE + norm[idx:]
    # static/tmp dışındaysa basename ile /static/tmp varsay
    return f"{_PUBLIC_BASE}/static/tmp/{os.path.basename(norm)}"


def post_to_facebook(message: str, image_path: str | None = None) -> str | None:
    """Facebook Page'e TAM gönderi (görsel varsa /photos, yoksa /feed)."""
    c = _cfg()
    if not c["fb_page_id"] or not c["fb_token"]:
        logger.warning("[META] FB token/page yok — Facebook paylaşımı atlandı")
        return None
    try:
        if image_path and os.path.exists(image_path):
            with open(image_path, "rb") as f:
                resp = requests.post(
                    f"{_GRAPH}/{c['fb_page_id']}/photos",
                    data={"message": (message or "")[:8000], "access_token": c["fb_token"]},
                    files={"source": (os.path.basename(image_path), f, "image/png")},
                    timeout=90,
                )
        else:
            resp = requests.post(
                f"{_GRAPH}/{c['fb_page_id']}/feed",
                data={"message": (message or "")[:8000], "access_token": c["fb_token"]},
                timeout=60,
            )
        resp.raise_for_status()
        data = resp.json()
        pid = data.get("post_id") or data.get("id")
        logger.info("[META] Facebook gönderi: %s", pid)
        return str(pid) if pid else None
    except requests.exceptions.HTTPError as e:
        logger.error("[META] Facebook HTTP hata: %s — %s", e.response.status_code if e.response else "?",
                     (e.response.text[:300] if e.response else str(e)))
        return None
    except Exception as e:
        logger.error("[META] Facebook hata: %s", e)
        return None


def post_to_instagram(image_url: str, caption: str) -> str | None:
    """Instagram'a resimli gönderi: media container (image_url) → media_publish."""
    c = _cfg()
    if not c["ig_account"] or not c["ig_token"]:
        logger.warning("[META] IG token/account yok — Instagram paylaşımı atlandı")
        return None
    if not image_url:
        logger.warning("[META] IG görsel URL yok — atlandı")
        return None
    try:
        cr = requests.post(
            f"{_GRAPH}/{c['ig_account']}/media",
            data={"image_url": image_url, "caption": (caption or "")[:2200], "access_token": c["ig_token"]},
            timeout=90,
        )
        cr.raise_for_status()
        container = cr.json().get("id")
        if not container:
            logger.error("[META] IG container oluşmadı: %s", cr.text[:300])
            return None
        pub = requests.post(
            f"{_GRAPH}/{c['ig_account']}/media_publish",
            data={"creation_id": container, "access_token": c["ig_token"]},
            timeout=90,
        )
        pub.raise_for_status()
        mid = pub.json().get("id")
        logger.info("[META] Instagram gönderi: %s", mid)
        return str(mid) if mid else None
    except requests.exceptions.HTTPError as e:
        logger.error("[META] Instagram HTTP hata: %s — %s", e.response.status_code if e.response else "?",
                     (e.response.text[:300] if e.response else str(e)))
        return None
    except Exception as e:
        logger.error("[META] Instagram hata: %s", e)
        return None


async def cross_post_thread(full_text: str, image_path: str | None) -> dict:
    """X thread onayında çağrılır: Facebook (tam post) + Instagram (resimli).

    full_text: thread tweetlerinin birleştirilmiş TAM hali (parça parça değil).
    image_path: kapak görseli (static/tmp). FB'ye dosya, IG'ye public URL gider.
    Bittiğinde görseli temizler. Telegram'a kısa rapor düşer.
    """
    import asyncio
    c = _cfg()
    if not (c["fb_page_id"] or c["ig_account"]):
        logger.info("[META] Hiçbir Meta token yok — çapraz paylaşım atlandı")
        return {"facebook": None, "instagram": None}

    ig_url = _image_public_url(image_path)
    # Sync (requests) çağrıları thread'de çalıştır — event loop bloklanmasın
    fb_id = await asyncio.to_thread(post_to_facebook, full_text, image_path)
    ig_id = await asyncio.to_thread(post_to_instagram, ig_url, full_text) if ig_url else None

    # Görseli temizle (FB+IG bitti)
    try:
        if image_path and os.path.exists(image_path):
            os.remove(image_path)
    except OSError:
        pass

    # Telegram rapor
    try:
        from app.services.admin_telegram import send_admin_message
        _fb = "✅" if fb_id else ("⏭️" if not c["fb_page_id"] else "❌")
        _ig = "✅" if ig_id else ("⏭️" if not c["ig_account"] else "❌")
        await send_admin_message(
            f"📣 <b>Çapraz Paylaşım</b>\nFacebook: {_fb}  ·  Instagram: {_ig}"
        )
    except Exception as e:
        logger.debug("[META] Telegram rapor hata: %s", e)

    return {"facebook": fb_id, "instagram": ig_id}
