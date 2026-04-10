"""Sirket Hakkinda Aciklama Uretici.

SPK basvurusu yapan sirketler icin AI ile 1-2 paragraf tanitim metni uretir.
Tweet ve web/mobil uygulama icin kullanilir.

Model: gemini-2.5-flash (hizli ve ucuz)
"""

import logging
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_TIMEOUT = 60

_SYSTEM_PROMPT = """Sen bir Türk finans araştırma uzmanısın. Sana verilen şirket adına göre 2 paragraf (toplam 100-150 kelime) Türkçe şirket tanıtım metni yaz.

Kurallar:
- İlk paragrafta şirketin ne iş yaptığını, hangi sektörde faaliyet gösterdiğini, merkezinin nerede olduğunu ve kuruluş/tarihçe bilgisini yaz.
- İkinci paragrafta şirketin sektördeki konumunu, üretim/hizmet kapasitesini, ihracat durumunu ve halka arz motivasyonunu yaz.
- Şirket hakkında kesin bilgin yoksa, şirket adından sektörü çıkar ve genel sektör bilgisiyle mantıklı bir tanıtım yaz.
- Sadece gerçekçi ve mantıklı bilgiler yaz, uydurma.
- Yatırım tavsiyesi verme.
- Emoji kullanma.
- Sadece düz metin yaz, başlık veya madde işareti kullanma."""


async def generate_company_description(company_name: str) -> Optional[str]:
    """Sirket adi icin AI ile tanitim metni uretir."""
    settings = get_settings()

    user_msg = f"Şirket: {company_name}\n\nBu şirket hakkında 2 paragraf tanıtım metni yaz."

    # 1. Abacus/OpenAI (birincil — her zaman calisiyor)
    abacus_key = getattr(settings, "OPENAI_API_KEY", "")
    if abacus_key:
        try:
            abacus_url = "https://routellm.abacus.ai/v1/chat/completions"
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    abacus_url,
                    headers={
                        "Authorization": f"Bearer {abacus_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "claude-sonnet-4-6",
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PROMPT},
                            {"role": "user", "content": user_msg},
                        ],
                        "max_tokens": 1000,
                        "temperature": 0.7,
                    },
                )

                if resp.status_code == 200:
                    data = resp.json()
                    text = data["choices"][0]["message"]["content"].strip()
                    logger.info(f"Company description generated for: {company_name} ({len(text)} chars)")
                    return text
                else:
                    logger.error(f"Abacus API error {resp.status_code}: {resp.text[:200]}")

        except Exception as e:
            logger.error(f"Abacus company description failed for {company_name}: {e}")

    # 2. Gemini fallback
    gemini_key = getattr(settings, "GEMINI_API_KEY", "")
    if gemini_key:
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    _GEMINI_URL,
                    headers={
                        "Authorization": f"Bearer {gemini_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": _GEMINI_MODEL,
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PROMPT},
                            {"role": "user", "content": user_msg},
                        ],
                        "max_tokens": 1000,
                        "temperature": 0.7,
                    },
                )

                if resp.status_code == 200:
                    data = resp.json()
                    text = data["choices"][0]["message"]["content"].strip()
                    logger.info(f"Company description generated (Gemini) for: {company_name} ({len(text)} chars)")
                    return text
                else:
                    logger.error(f"Gemini API error {resp.status_code}: {resp.text[:200]}")

        except Exception as e:
            logger.error(f"Gemini company description failed for {company_name}: {e}")

    logger.warning(f"No AI API key available for company description: {company_name}")
    return None
