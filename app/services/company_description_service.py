"""Sirket Hakkinda Aciklama Uretici — Claude Haiku."""

import logging
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_MODEL = "claude-haiku-4-5-20251001"
_TIMEOUT = 30

_SYSTEM_PROMPT = "Verilen Türk şirketini 2 paragrafta tanıt. Toplam 4-6 cümle yaz. Sektör, faaliyet alanı ve halka arz motivasyonunu belirt. Şirket adından sektörü çıkarabilirsin. Düz metin, emoji yok, başlık yok."


async def generate_company_description(company_name: str) -> Optional[str]:
    """Sirket adi icin Claude Haiku ile tanitim metni uretir."""
    settings = get_settings()

    api_key = getattr(settings, "ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set")
        return None

    user_msg = f"Şirket: {company_name}\n\n2 paragraf tanıtım yaz."

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(
                _ANTHROPIC_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": _MODEL,
                    "max_tokens": 800,
                    "system": _SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": user_msg},
                    ],
                },
            )

            if resp.status_code == 200:
                data = resp.json()
                text = data["content"][0]["text"].strip()
                if len(text) > 80:
                    logger.info(f"Desc OK: {company_name} ({len(text)} chars)")
                    return text
                logger.warning(f"Desc too short: {company_name} ({len(text)} chars)")
            else:
                logger.error(f"Claude API {resp.status_code}: {resp.text[:200]}")

    except Exception as e:
        logger.error(f"Claude fail: {company_name}: {e}")

    return None
