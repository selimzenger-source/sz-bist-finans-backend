"""Abacus AI (RouteLLM) — KAP Haber Puanlama & Yorum Servisi.

Telegram poller'dan gelen haberleri AI ile puanlar ve yorumlar.
KAP bildirim detayina erisebilirse onu kullanir, yoksa Telegram ham metnini.

Kullanim:
    kap_content = await fetch_kap_content(kap_id)
    result = await score_news("ENDAE", raw_text, kap_content)
    # result = {"score": 8, "summary": "Iki cumlelik ozet."}

Abacus AI RouteLLM: OpenAI uyumlu API, ucretsiz tier yeterli.
"""

import json
import logging

import httpx

logger = logging.getLogger(__name__)

# Abacus AI RouteLLM endpoint (OpenAI uyumlu)
_ABACUS_URL = "https://api.abacus.ai/api/v0/openai/chat/completions"
_TIMEOUT = 15  # saniye


def _get_api_key() -> str | None:
    """Config'den Abacus API key'i al."""
    try:
        from app.config import get_settings
        key = get_settings().ABACUS_API_KEY
        return key if key else None
    except Exception:
        return None


async def fetch_kap_content(kap_id: str | None) -> str | None:
    """KAP bildirim detay sayfasindan icerik cek.

    Args:
        kap_id: KAP bildirim ID (orn: "1234567")

    Returns:
        Bildirim tam metni veya None (hata/bulunamadiysa)
    """
    if not kap_id:
        return None

    try:
        from app.scrapers.kap_scraper import KAPScraper
        scraper = KAPScraper()
        try:
            detail = await scraper.fetch_disclosure_detail(kap_id)
            if detail and detail.get("full_text"):
                logger.info("KAP icerik alindi: %s (%d karakter)", kap_id, len(detail["full_text"]))
                return detail["full_text"]
            return None
        finally:
            await scraper.close()
    except Exception as e:
        logger.warning("KAP icerik cekme hatasi (%s): %s", kap_id, e)
        return None


async def score_news(
    ticker: str,
    raw_text: str,
    kap_content: str | None = None,
) -> dict:
    """Haberi AI ile puanla ve yorumla.

    Args:
        ticker: Hisse kodu (orn: "ENDAE")
        raw_text: Telegram mesajinin ham metni
        kap_content: KAP bildirim tam metni (varsa)

    Returns:
        {"score": int|None, "summary": str|None}
        Hata durumunda her iki deger None olur — akis kirilmaz.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.debug("AI News Scorer: ABACUS_API_KEY bos, devre disi")
        return {"score": None, "summary": None}

    # KAP icerigi varsa onu kullan, yoksa Telegram ham metnini
    content = kap_content if kap_content else raw_text
    # Cok uzun metinleri kes (token limiti icin)
    content = content[:3000] if content else ""

    if not content.strip():
        return {"score": None, "summary": None}

    prompt = f"""Borsa Istanbul KAP bildirimi analizi:

Hisse: {ticker}
Icerik:
{content}

GOREV:
1. Bu haberin hisse fiyatina etkisini 1-10 arasi puanla:
   1-3: Olumsuz etki (kotu haber)
   4-5: Notr / belirsiz
   6-7: Hafif olumlu
   8-10: Cok olumlu (iyi haber)
2. Haberi Turkce 2 cumle ile ozetle (yatirimci bakis acisiyla)

SADECE asagidaki JSON formatinda yanit ver:
{{"score": 8, "summary": "Iki cumlelik Turkce ozet."}}"""

    try:
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
                        {
                            "role": "system",
                            "content": "Sen bir borsa haberi analiz uzmanisin. Sadece JSON formatinda yanit ver.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0,
                    "max_tokens": 300,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        # OpenAI format: choices[0].message.content
        text = data["choices"][0]["message"]["content"].strip()

        # JSON blogu temizle
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        result = json.loads(text)
        score = result.get("score")
        summary = result.get("summary")

        # Score validation: 1-10 arasinda olmali
        if isinstance(score, (int, float)) and 1 <= score <= 10:
            score = int(score)
        else:
            logger.warning("AI News Scorer: Gecersiz skor=%s (%s)", score, ticker)
            score = None

        # Summary validation
        if not isinstance(summary, str) or not summary.strip():
            summary = None

        logger.info(
            "AI News Scorer: %s — skor=%s, ozet=%s",
            ticker, score, (summary[:60] + "...") if summary and len(summary) > 60 else summary,
        )

        return {"score": score, "summary": summary}

    except Exception as e:
        logger.warning("AI News Scorer: Abacus hatasi (%s) — %s", ticker, e)
        return {"score": None, "summary": None}
