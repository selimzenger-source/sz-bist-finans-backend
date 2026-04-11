"""Tum KAP Bildirimleri Sentiment Analizi.

Akis:
1. kap_all_scraper'dan gelen bildirim alınır
2. is_bilanco=True → AI atla (bilanco analizi henuz yok)
3. Gemini 2.5 Flash (birincil) ile sentiment + impact_score + ozet uret
4. Fallback 1: Abacus AI (RouteLLM)
5. Fallback 2: Kural tabanli basit analiz

Sonuc: {"sentiment": str, "impact_score": float, "summary": str}
"""

import json
import logging

import httpx

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
# System Prompt Yönetimi
# ═══════════════════════════════════════════════════════════════════

_DEFAULT_SYSTEM_PROMPT = (
    "Sen deneyimli bir Borsa Istanbul kurumsal yatirimci analistisin. "
    "KAP bildirimlerini objektif analiz eder, sentiment ve etki puani verirsin. "
    "SADECE JSON formatinda yanit ver. Markdown, aciklama, yorum YAZMA. "
    "Sadece tek satirlik JSON objesi don."
)

_custom_system_prompt: str | None = None


def get_system_prompt() -> str:
    return _custom_system_prompt if _custom_system_prompt is not None else _DEFAULT_SYSTEM_PROMPT


def set_system_prompt(new_prompt: str | None) -> None:
    global _custom_system_prompt
    _custom_system_prompt = new_prompt


def get_default_system_prompt() -> str:
    return _DEFAULT_SYSTEM_PROMPT


# Gemini 2.5 Flash — birincil (OpenAI uyumlu endpoint)
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"

# Gemini 2.5 Pro — yedek
_GEMINI_PRO_MODEL = "gemini-2.5-pro"

# Claude Haiku — Gemini basarisiz olursa son AI fallback
_HAIKU_URL = "https://api.anthropic.com/v1/messages"
_HAIKU_MODEL = "claude-haiku-4-5-20251001"

_AI_TIMEOUT = 45


def _get_keys() -> tuple[str | None, str | None]:
    """Config'den Gemini ve Anthropic API key'lerini al."""
    try:
        from app.config import get_settings
        s = get_settings()
        gemini = s.GEMINI_API_KEY if s.GEMINI_API_KEY else None
        anthropic = s.ANTHROPIC_API_KEY if s.ANTHROPIC_API_KEY else None
        return (gemini, anthropic)
    except Exception:
        return (None, None)


# ═══════════════════════════════════════════════════════════════════
# Kural tabanli fallback (AI basarisiz olursa)
# ═══════════════════════════════════════════════════════════════════

_POSITIVE_KEYWORDS = [
    "kar artisi", "kar artışı", "kâr artışı",
    "ihale kazandı", "ihale aldı",
    "anlaşma imzaladı", "anlaşma yapıldı",
    "ihracat", "satış artışı",
    "temettü", "kar payı dağıtım", "kâr payı dağıtım",
    "bedelsiz", "sermaye artırımı",
    "iş birliği anlaşması",
    "yeni yatırım", "kapasite artışı",
]

# Rutin/notr haberler — olumlu veya olumsuz sayilmamali
_NEUTRAL_KEYWORDS = [
    "esas sözleşme", "bilgi formu", "yönetim kurulu",
    "genel kurul", "sorumluluk beyanı", "faaliyet raporu",
    "finansal rapor", "bağımsız denetim", "komite",
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

    # Oncelikle rutin/notr haber mi kontrol et
    neutral_hit = sum(1 for kw in _NEUTRAL_KEYWORDS if kw in text)
    if neutral_hit > 0:
        return {"sentiment": "Notr", "impact_score": 5.0, "summary": None}

    pos = sum(1 for kw in _POSITIVE_KEYWORDS if kw in text)
    neg = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in text)

    if pos > neg:
        return {"sentiment": "Olumlu", "impact_score": 6.5, "summary": None}
    elif neg > pos:
        return {"sentiment": "Olumsuz", "impact_score": 3.5, "summary": None}
    return {"sentiment": "Notr", "impact_score": 5.0, "summary": None}


# ═══════════════════════════════════════════════════════════════════
# AI Analiz (Gemini Flash / Pro + Kural Tabanli Fallback)
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
    # Bilanco bildirimleri icin AI atla — ilerleyen safhada eklenecek
    if is_bilanco:
        logger.info("KAP Analyzer: Bilanco bildirimi, AI atla (%s)", company_code)
        return {"sentiment": "Notr", "impact_score": 5.0, "summary": None}

    # ── Devre Kesici: AI'ya gonderme, sabit skor + metin don ──
    combined_text = f"{title} {body}".lower()
    if "devre kesici" in combined_text or "tek fiyat emir toplama" in combined_text:
        logger.info("KAP Analyzer: Devre kesici tespit edildi, AI atla (%s)", company_code)
        return {
            "sentiment": "Notr",
            "impact_score": 5.0,
            "summary": f"Borsa Istanbul, {company_code} hissesinde yasanan ani ve yuksek fiyat hareketi nedeniyle Pay Bazinda Devre Kesici uygulamasinin devreye girdigini bildirmistir. Bu bildirim, sirketin temel faaliyetleriyle ilgili bir gelisme olmayip, hisse senedinde anlik yuksek volatiliteyi kontrol altina almayi amaclayan standart bir borsa mekanizmasidir.",
        }

    # ── Telegram Eşleşmesi (Sonnet skorlarını senkronize et) ──
    # NOT: Sadece ticker degil, baslik kelime eslesmesi de kontrol edilir.
    # Ayni ticker icin farkli KAP bildirimleri (orn: Finansal Rapor vs Uyum Raporu)
    # farkli AI analizleri almalidir.
    try:
        from app.database import async_session
        from app.models.telegram_news import TelegramNews
        from sqlalchemy import select, desc
        from datetime import datetime, timezone, timedelta

        async with async_session() as session:
            twelve_hours_ago = datetime.now(timezone.utc) - timedelta(hours=12)
            query = (
                select(TelegramNews)
                .where(
                    TelegramNews.ticker == company_code,
                    TelegramNews.ai_score.isnot(None),
                    TelegramNews.message_date >= twelve_hours_ago
                )
                .order_by(desc(TelegramNews.message_date))
                .limit(5)
            )
            result = await session.execute(query)
            recent_news_list = list(result.scalars().all())

            # Baslik eslesmesi: KAP bildirim basligindaki anahtar kelimeler
            # TelegramNews iceriginde de geciyorsa eslesme var demektir
            title_lower = title.lower()
            title_words = {w for w in title_lower.split() if len(w) > 3}

            for recent_news in recent_news_list:
                telegram_text = (recent_news.parsed_title or recent_news.raw_text or "").lower()
                telegram_words = {w for w in telegram_text.split() if len(w) > 3}
                common = title_words & telegram_words
                if len(common) >= 2:
                    logger.info(
                        "KAP Analyzer: TelegramNews eslesmesi (%s), skor: %s, ortak: %s",
                        company_code, recent_news.ai_score, common,
                    )
                    impact_score = recent_news.ai_score

                    if impact_score >= 6.0:
                        sentiment = "Olumlu"
                    elif impact_score < 4.5:
                        sentiment = "Olumsuz"
                    else:
                        sentiment = "Notr"

                    return {
                        "sentiment": sentiment,
                        "impact_score": impact_score,
                        "summary": recent_news.ai_summary
                    }

            # Eslesen TelegramNews bulunamadi — AI analiz devam edecek
            if recent_news_list:
                logger.debug(
                    "KAP Analyzer: TelegramNews eslesmesi bulunamadi (%s, baslik: %s), AI analiz yapilacak",
                    company_code, title[:50],
                )
    except Exception as e:
        logger.warning("KAP Analyzer: TelegramNews senkronizasyon hatasi (%s): %s", company_code, e)

    (gemini_key, anthropic_key) = _get_keys()
    if not gemini_key and not anthropic_key:
        logger.error("KAP Analyzer: Hic API key yok — fallback (%s)", company_code)
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
- "Olumlu": SADECE sirket icin somut olumlu gelisme varsa — kar artisi, buyuk sozlesme imzalama, ihale kazanma, temettu dagitimi, bedelsiz sermaye artirimi, guclu ihracat verisi
- "Olumsuz": Sirket icin somut olumsuz gelisme — zarar, ceza, dava, borc, fesih, sermaye erimesi, SPK tedbir karari
- "Notr": Hisse fiyatini dogrudan etkilemeyecek rutin bildirim. ASAGIDAKILER KESINLIKLE NOTR'dur:
  * Sirket Genel Bilgi Formu, Bilgi Formu
  * Esas Sozlesme Tadili, Esas Sozlesme
  * Yonetim Kurulu Komiteleri, Yonetim Kurulu Uye Secimi
  * Genel Kurul cagrisi/sonucu
  * Bagimsiz Denetim Raporu, Sorumluluk Beyani
  * Faaliyet Raporu, Finansal Rapor
  * SPK/Borsa onay/tescil bildirimleri (icerik belirtilmedikce)
  * Organizasyon seması, Imza sirkuleri

KRITIK KURAL: Eger bildirim sadece yasal/idari bir zorunluluk ise (bilgi formu, sozlesme tadili, komite atamasi vb.), sentiment KESINLIKLE "Notr" ve puan 5.0 olmali. Olumlu/Olumsuz icin haberin somut finansal veya operasyonel bir etki icermesi SART.

ETKI PUANI (1.0-10.0 arasi, 0.1 hassasiyetle):
  1.0-3.0: Ciddi olumsuz (buyuk zarar, agir ceza, iflas riski)
  3.1-4.9: Hafif olumsuz (kucuk zarar, belirsizlik)
  5.0-5.5: Notr/Rutin (bilgi formu, sozlesme tadili, komite, genel kurul)
  5.6-5.9: Hafif pozitif niyetli ama somut etki yok
  6.0-6.9: Hafif olumlu (kucuk sozlesme, standart is birligi)
  7.0-7.9: Olumlu (buyuk sozlesme, guclu ihracat, onemli ihale)
  8.0-8.9: Cok olumlu (yuksek kar artisi, buyuk bedelsiz, temettu)
  9.0-10.0: Olaganustu (rekor kar, mega ihale, sektor degistirecek haber)

OZET:
- 2-3 cumle Turkce ozet
- Haberin ne oldugunu, sirket icin ne anlama geldigini acikla
- Onemli rakamlari (tutar, oran, yuzde) varsa dahil et

SADECE asagidaki JSON formatinda yanit ver:
{{"sentiment": "Olumlu", "impact_score": 7.3, "summary": "2-3 cumle Turkce ozet."}}"""

    messages = [
        {"role": "system", "content": get_system_prompt()},
        {"role": "user", "content": prompt},
    ]
    payload = {
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 4096,  # Gemini 2.5 thinking token'ları da max_tokens'tan yer — düşük olunca content=null döner
    }

    # ── Birincil: Gemini 2.5 Flash ──
    ai_text = None
    provider_used = None

    if gemini_key:
        for model_name, model_label in [(_GEMINI_PRO_MODEL, "Pro"), (_GEMINI_MODEL, "Flash")]:
            if ai_text:
                break
            try:
                async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                    resp = await client.post(
                        _GEMINI_URL,
                        headers={
                            "Authorization": f"Bearer {gemini_key}",
                            "Content-Type": "application/json",
                        },
                        json={**payload, "model": model_name},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        # Gemini 2.5 bazen content=null döner (thinking mode)
                        choice = data.get("choices", [{}])[0]
                        msg = choice.get("message", {})
                        content = msg.get("content")
                        if content and content.strip():
                            ai_text = content.strip()
                            provider_used = f"Gemini-{model_label}"
                        else:
                            logger.warning(
                                "KAP Analyzer: Gemini-%s content bos (%s) — response keys: %s",
                                model_label, company_code,
                                list(msg.keys()) if msg else "no message",
                            )
                    else:
                        logger.warning(
                            "KAP Analyzer: Gemini-%s HTTP %s (%s) — %s",
                            model_label, resp.status_code, company_code, resp.text[:200],
                        )
            except Exception as e:
                logger.warning("KAP Analyzer: Gemini-%s hata (%s) — %s", model_label, company_code, e)

    # ── Gemini basarisiz → Claude Haiku fallback ──
    if not ai_text and anthropic_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                haiku_resp = await client.post(
                    _HAIKU_URL,
                    headers={
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": _HAIKU_MODEL,
                        "max_tokens": 500,
                        "temperature": 0.1,
                        "system": get_system_prompt(),
                        "messages": [{"role": "user", "content": prompt}],
                    },
                )
                if haiku_resp.status_code == 200:
                    haiku_data = haiku_resp.json()
                    haiku_content = haiku_data.get("content", [])
                    if haiku_content and haiku_content[0].get("text"):
                        ai_text = haiku_content[0]["text"].strip()
                        provider_used = "Haiku"
                        logger.info("KAP Analyzer: Haiku fallback basarili (%s)", company_code)
                else:
                    logger.warning(
                        "KAP Analyzer: Haiku HTTP %s (%s) — %s",
                        haiku_resp.status_code, company_code, haiku_resp.text[:200],
                    )
        except Exception as e:
            logger.warning("KAP Analyzer: Haiku hata (%s) — %s", company_code, e)

    # ── Tum AI modelleri basarisiz → kural tabanli fallback ──
    if not ai_text:
        logger.error("KAP Analyzer: AI basarisiz (Gemini+Haiku) — kural fallback (%s)", company_code)
        return _rule_based_analyze(title, body)

    # JSON parse — bozuk JSON'u da kurtarmaya calis
    from app.services.ai_json_helper import safe_parse_json
    result = safe_parse_json(ai_text, required_key="sentiment")
    if result is None:
        logger.error("KAP Analyzer: [%s] JSON parse basarisiz (%s) — icerik: %s",
                      provider_used, company_code, ai_text[:150])
        return _rule_based_analyze(title, body)

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
        "KAP Analyzer [%s]: %s — sentiment=%s, score=%s, ozet=%s",
        provider_used, company_code, sentiment, impact_score,
        (summary[:60] + "...") if summary and len(summary) > 60 else summary,
    )

    return {"sentiment": sentiment, "impact_score": impact_score, "summary": summary}
