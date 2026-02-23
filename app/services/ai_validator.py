"""Abacus AI (RouteLLM) — Scraper veri dogrulama servisi.

HalkArz scraper'dan gelen tarih ve sayisal verileri AI ile
dogrular. Sadece yeni/degisen veriler icin cagrilir (her taramada degil).

Kullanim:
    result = await validate_ipo_dates(raw_texts, parsed_values)
    if not result["valid"]:
        # Duzeltilmis degerleri kullan veya admin'e uyar

Abacus AI RouteLLM: OpenAI uyumlu API, ucretsiz tier yeterli.
"""

import json
import logging
from datetime import date

import httpx

logger = logging.getLogger(__name__)

# Abacus AI RouteLLM endpoint (OpenAI uyumlu)
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_TIMEOUT = 30  # saniye


def _get_api_key() -> str | None:
    """Config'den Abacus API key'i al."""
    try:
        from app.config import get_settings
        key = get_settings().ABACUS_API_KEY
        return key if key else None
    except Exception:
        return None


async def validate_ipo_dates(
    raw_texts: dict,
    parsed_values: dict,
    company_name: str = "",
) -> dict:
    """HalkArz'dan parse edilen tarih verilerini AI ile dogrula.

    Args:
        raw_texts: Scraper'dan gelen ham metin degerleri
            {"subscription_dates": "26,27 Subat 2026 09:00-17:00", ...}
        parsed_values: Parse edilmis tarih degerleri
            {"subscription_start": "2026-02-26", "subscription_end": "2026-02-27", ...}
        company_name: Sirket adi (log icin)

    Returns:
        {
            "valid": True/False,
            "corrections": {"subscription_start": "2026-02-26", ...} veya {},
            "reason": "Aciklama" veya "",
            "ai_used": True/False
        }
    """
    api_key = _get_api_key()
    if not api_key:
        logger.debug("AI Validator: ABACUS_API_KEY bos, devre disi")
        return {"valid": True, "corrections": {}, "reason": "", "ai_used": False}

    try:
        prompt = _build_validation_prompt(raw_texts, parsed_values, company_name)

        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(
                _ABACUS_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "gpt-4o-mini",
                    "messages": [
                        {"role": "system", "content": "Sen bir veri dogrulama asistanisin. Sadece JSON formatinda yanit ver."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0,
                    "max_tokens": 500,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        # OpenAI format: choices[0].message.content
        content = data["choices"][0]["message"]["content"]
        return _parse_ai_response(content)

    except Exception as e:
        logger.warning(f"AI Validator: Abacus hatasi ({company_name}) — {e}")
        # AI hatasi durumunda veriyi kabul et (bloklamamak icin)
        return {"valid": True, "corrections": {}, "reason": f"AI hatasi: {e}", "ai_used": False}


def _build_validation_prompt(raw_texts: dict, parsed_values: dict, company_name: str) -> str:
    """AI icin validation promptu."""
    # date nesnelerini string'e cevir
    safe_parsed = {}
    for k, v in parsed_values.items():
        safe_parsed[k] = v.isoformat() if isinstance(v, date) else str(v) if v else None

    return f"""Halka arz web sitesinden cekilen ham metin ve parse edilen tarih degerlerini kontrol et.

Sirket: {company_name}

Ham metin verileri:
{json.dumps(raw_texts, ensure_ascii=False, indent=2)}

Parse sonucu:
{json.dumps(safe_parsed, ensure_ascii=False, indent=2)}

Bugunun tarihi: {date.today().isoformat()}

KURALLAR:
- Basvuru suresi genelde 1-5 gun arasindadir. 7 gunden uzun olamaz.
- Basvuru baslangic tarihi bugunun tarihinden en fazla 2 gun once olabilir (zaten baslamis olabilir).
- Basvuru bitis tarihi baslangictan once olamaz.
- Islem baslangic tarihi basvuru bitisinden once olamaz.
- Ham metindeki tarihleri dikkatlice oku ve parse sonucuyla karsilastir.
- Saat degerleri (09:00, 17:00 gibi) gun olarak alinmamali.

SADECE asagidaki JSON formatinda yanit ver:
{{"valid": true}} veya
{{"valid": false, "corrections": {{"subscription_start": "YYYY-MM-DD", "subscription_end": "YYYY-MM-DD"}}, "reason": "kisa aciklama"}}"""


def _parse_ai_response(response_text: str) -> dict:
    """AI yanitini parse eder."""
    try:
        text = response_text.strip()
        # JSON blogu bul
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        data = json.loads(text)

        is_valid = data.get("valid", True)
        corrections = data.get("corrections", {})
        reason = data.get("reason", "")

        logger.info(
            "AI Validator: valid=%s, corrections=%s, reason=%s",
            is_valid, corrections, reason,
        )

        return {
            "valid": is_valid,
            "corrections": corrections,
            "reason": reason,
            "ai_used": True,
        }

    except (json.JSONDecodeError, KeyError, IndexError) as e:
        logger.warning(f"AI Validator: Yanit parse hatasi — {e}, raw: {response_text[:200]}")
        return {"valid": True, "corrections": {}, "reason": f"Parse hatasi: {e}", "ai_used": False}


def sanity_check_dates(parsed_values: dict) -> dict:
    """Hizli sanity check — AI olmadan, basit kurallarla.

    Returns:
        {"passed": True/False, "issues": ["aciklama", ...]}
    """
    issues = []
    today = date.today()

    sub_start = parsed_values.get("subscription_start")
    sub_end = parsed_values.get("subscription_end")
    trading_start = parsed_values.get("trading_start")

    # 1. Basvuru baslangici gecmiste olmamali (en fazla 2 gun tolerans)
    if sub_start and isinstance(sub_start, date):
        from datetime import timedelta
        if sub_start < today - timedelta(days=2):
            issues.append(
                f"subscription_start ({sub_start}) gecmiste — "
                f"bugun: {today}, fark: {(today - sub_start).days} gun"
            )

    # 2. Basvuru suresi 0-7 gun arasi olmali
    if sub_start and sub_end and isinstance(sub_start, date) and isinstance(sub_end, date):
        duration = (sub_end - sub_start).days
        if duration < 0:
            issues.append(f"subscription_end ({sub_end}) baslangictan once!")
        elif duration > 7:
            issues.append(
                f"Basvuru suresi {duration} gun — normale gore cok uzun "
                f"({sub_start} -> {sub_end})"
            )

    # 3. Islem tarihi basvuru bitisinden sonra olmali
    if trading_start and sub_end and isinstance(trading_start, date) and isinstance(sub_end, date):
        if trading_start <= sub_end:
            issues.append(
                f"trading_start ({trading_start}) <= subscription_end ({sub_end})"
            )

    return {
        "passed": len(issues) == 0,
        "issues": issues,
    }
