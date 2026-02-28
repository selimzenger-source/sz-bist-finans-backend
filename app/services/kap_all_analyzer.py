"""Abacus AI (RouteLLM) — Tum KAP Bildirimleri Sentiment Analizi.

Akis:
1. kap_all_scraper'dan gelen bildirim alınır
2. is_bilanco=True → AI atla (bilanco analizi henuz yok)
3. Abacus AI (Claude Sonnet) ile sentiment + impact_score + ozet uret
4. Fallback: Kural tabanli basit analiz

Sonuc: {"sentiment": str, "impact_score": float, "summary": str}
"""

import json
import logging

import httpx

logger = logging.getLogger(__name__)

# Abacus AI RouteLLM endpoint (OpenAI uyumlu)
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_AI_MODEL = "claude-sonnet-4-5"
_AI_TIMEOUT = 25


def _get_api_key() -> str | None:
    """Config'den Abacus API key'i al."""
    try:
        from app.config import get_settings
        key = get_settings().ABACUS_API_KEY
        return key if key else None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════
# Kural tabanli fallback (AI basarisiz olursa)
# ═══════════════════════════════════════════════════════════════════

_POSITIVE_KEYWORDS = [
    "kar artisi", "kar artışı", "kâr artışı",
    "sozlesme", "sözleşme",
    "ihale", "anlaşma", "anlaşması",
    "ihracat", "satış artışı",
    "temettü", "kar payı", "kâr payı",
    "bedelsiz", "sermaye artırımı",
    "ortaklık", "iş birliği",
    "yeni yatırım", "kapasite artışı",
]

_NEGATIVE_KEYWORDS = [
    "zarar", "ceza", "dava",
    "borç", "iflas", "konkordato",
    "sermaye erimesi", "olumsuz",
    "fesih", "iptal", "azaltım",
]


def _rule_based_analyze(title: str, body: str) -> dict:
    """Basit kural tabanli sentiment analizi (fallback)."""
    text = f"{title} {body}".lower()

    pos = sum(1 for kw in _POSITIVE_KEYWORDS if kw in text)
    neg = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in text)

    if pos > neg:
        return {"sentiment": "Olumlu", "impact_score": 6.0, "summary": None}
    elif neg > pos:
        return {"sentiment": "Olumsuz", "impact_score": 3.5, "summary": None}
    return {"sentiment": "Notr", "impact_score": 5.0, "summary": None}


# ═══════════════════════════════════════════════════════════════════
# AI Analiz (Abacus RouteLLM — Claude Sonnet)
# ═══════════════════════════════════════════════════════════════════

async def analyze_disclosure(
    company_code: str,
    title: str,
    body: str,
    is_bilanco: bool = False,
) -> dict:
    """KAP bildirimini AI ile analiz et.

    Args:
        company_code: Hisse kodu (orn: "THYAO")
        title: Bildirim basligi
        body: Bildirim tam metni
        is_bilanco: Bilanco/Finansal Rapor mu (True ise AI atla)

    Returns:
        {
            "sentiment": "Olumlu" | "Olumsuz" | "Notr",
            "impact_score": float (1.0-10.0),
            "summary": str | None,
        }
    """
    # Bilanco bildirimleri icin AI atla — henuz desteklenmiyor
    if is_bilanco:
        logger.info("KAP Analyzer: Bilanco bildirimi, AI atla (%s)", company_code)
        return {"sentiment": "Notr", "impact_score": 5.0, "summary": None}

    api_key = _get_api_key()
    if not api_key:
        logger.error("KAP Analyzer: ABACUS_API_KEY bos — fallback kullaniliyor (%s)", company_code)
        return _rule_based_analyze(title, body)

    content = f"{title}\n\n{body}".strip()[:4000]
    if not content:
        return {"sentiment": "Notr", "impact_score": 5.0, "summary": None}

    prompt = f"""Borsa Istanbul (BIST) KAP bildirimi analizi.

Hisse: {company_code}

--- BILDIRIM BASLANGIC ---
{content}
--- BILDIRIM BITIS ---

GOREV: Bu KAP bildirimini yatirimci bakis acisiyla analiz et.

SENTIMENT (duygusal ton):
- "Olumlu": Sirket icin iyi haber — kar artisi, sozlesme, ihale, temettu, bedelsiz, ihracat, buyume
- "Olumsuz": Sirket icin kotu haber — zarar, ceza, dava, borc, fesih, sermaye erimesi
- "Notr": Rutin bildirim — genel kurul, yonetim degisikligi, SPK onay, uyum raporu, bilgi formu

ETKI PUANI (1.0-10.0 arasi, 0.1 hassasiyetle):
  1.0-3.0: Ciddi olumsuz (zarar, ceza, iflas riski)
  3.1-4.9: Hafif olumsuz (kucuk zarar, belirsiz)
  5.0-5.9: Notr (rutin bildirim)
  6.0-6.9: Hafif olumlu (kucuk sozlesme, standart is birligi)
  7.0-7.9: Olumlu (buyuk sozlesme, guclu ihracat, onemli ihale)
  8.0-8.9: Cok olumlu (yuksek kar artisi, buyuk bedelsiz, temettu)
  9.0-10.0: Olaganustu (rekor kar, mega ihale, sektor degistirecek haber)

OZET:
- 2-3 cumle Turkce ozet
- Haberin ne oldugunu, sirket icin ne anlama geldigini acikla
- Onemli rakamlari (tutar, oran, yuzde) dahil et

SADECE asagidaki JSON formatinda yanit ver:
{{"sentiment": "Olumlu", "impact_score": 7.3, "summary": "2-3 cumle Turkce ozet."}}"""

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _ABACUS_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _AI_MODEL,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "Sen deneyimli bir Borsa Istanbul kurumsal yatirimci analistisin. "
                                "KAP bildirimlerini objektif analiz eder, sentiment ve etki puani verirsin. "
                                "Sadece JSON formatinda yanit ver."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 400,
                },
            )

            if resp.status_code != 200:
                logger.error(
                    "KAP Analyzer: Abacus HTTP %s (%s) — %s",
                    resp.status_code, company_code, resp.text[:200],
                )
                return _rule_based_analyze(title, body)

            data = resp.json()

        # OpenAI format: choices[0].message.content
        text = data["choices"][0]["message"]["content"].strip()

        # JSON blogu temizle
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        result = json.loads(text)

        sentiment = result.get("sentiment", "Notr")
        if sentiment not in ("Olumlu", "Olumsuz", "Notr"):
            sentiment = "Notr"

        impact_score = result.get("impact_score")
        if isinstance(impact_score, (int, float)) and 1.0 <= impact_score <= 10.0:
            impact_score = round(float(impact_score), 1)
        else:
            impact_score = 5.0

        summary = result.get("summary")
        if not isinstance(summary, str) or not summary.strip():
            summary = None

        logger.info(
            "KAP Analyzer: %s — sentiment=%s, score=%s, ozet=%s",
            company_code, sentiment, impact_score,
            (summary[:60] + "...") if summary and len(summary) > 60 else summary,
        )

        return {"sentiment": sentiment, "impact_score": impact_score, "summary": summary}

    except httpx.TimeoutException:
        logger.error("KAP Analyzer: Abacus zaman asimi (%s)", company_code)
        return _rule_based_analyze(title, body)
    except json.JSONDecodeError as e:
        logger.error("KAP Analyzer: JSON parse hatasi (%s) — %s", company_code, e)
        return _rule_based_analyze(title, body)
    except Exception as e:
        logger.error("KAP Analyzer: Beklenmeyen hata (%s) — %s", company_code, e)
        return _rule_based_analyze(title, body)
