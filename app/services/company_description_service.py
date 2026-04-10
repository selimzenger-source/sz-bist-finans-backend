"""Sirket Hakkinda Aciklama Uretici — Gemini birincil."""

import logging
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_TIMEOUT = 30

_SYSTEM_PROMPT = "Verilen Türk şirketini 2 paragrafta tanıt. Toplam 4-6 cümle yaz. Sektör, faaliyet alanı ve halka arz motivasyonunu belirt. Şirket adından sektörü çıkarabilirsin. Düz metin, emoji yok, başlık yok."


async def generate_company_description(company_name: str) -> Optional[str]:
    """Sirket adi icin AI ile tanitim metni uretir. Gemini birincil."""
    settings = get_settings()

    user_msg = f"Şirket: {company_name}\n\n2 paragraf tanıtım yaz."

    # Gemini (birincil — hızlı ve güvenilir)
    gemini_key = getattr(settings, "GEMINI_API_KEY", "")
    if gemini_key:
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    _GEMINI_URL,
                    headers={"Authorization": f"Bearer {gemini_key}", "Content-Type": "application/json"},
                    json={
                        "model": _GEMINI_MODEL,
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PROMPT},
                            {"role": "user", "content": user_msg},
                        ],
                        "max_tokens": 800,
                        "temperature": 0.7,
                    },
                )
                if resp.status_code == 200:
                    text = resp.json()["choices"][0]["message"]["content"].strip()
                    if len(text) > 80:
                        logger.info(f"Desc OK: {company_name} ({len(text)} chars)")
                        return text
                    logger.warning(f"Desc too short: {company_name} ({len(text)} chars)")
                else:
                    logger.error(f"Gemini {resp.status_code}: {resp.text[:100]}")
        except Exception as e:
            logger.error(f"Gemini fail: {company_name}: {e}")

    # Abacus fallback
    abacus_key = getattr(settings, "OPENAI_API_KEY", "")
    if abacus_key:
        try:
            async with httpx.AsyncClient(timeout=45) as client:
                resp = await client.post(
                    "https://routellm.abacus.ai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {abacus_key}", "Content-Type": "application/json"},
                    json={
                        "model": "claude-sonnet-4-6",
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PROMPT},
                            {"role": "user", "content": user_msg},
                        ],
                        "max_tokens": 800,
                        "temperature": 0.7,
                    },
                )
                if resp.status_code == 200:
                    text = resp.json()["choices"][0]["message"]["content"].strip()
                    if len(text) > 80:
                        logger.info(f"Desc OK (Abacus): {company_name} ({len(text)} chars)")
                        return text
        except Exception as e:
            logger.error(f"Abacus fail: {company_name}: {e}")

    return None
