"""
Bilanço AI Analiz Servisi
Hisse senedi bilanço verilerini Claude AI ile analiz eder.
"""

import httpx
import json
import logging
from decimal import Decimal
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_AI_MODEL = "claude-sonnet-4-6"
_CLAUDE_MODEL = "claude-sonnet-4-20250514"
_AI_TIMEOUT = 120


_SYSTEM_PROMPT = """Sen bir Türk borsası (BIST) finansal analiz uzmanısın.
Görevin, verilen bilanço ve gelir tablosu verilerini analiz ederek yatırımcıya anlaşılır bir değerlendirme sunmak.

KURALLAR:
- Türkçe yaz, sade ve anlaşılır bir dil kullan.
- Teknik terimleri parantez içinde açıkla.
- Kesinlikle yatırım tavsiyesi VERME. Sadece finansal durumu değerlendir.
- Veride olmayan bilgiyi uydurma.
- Her zaman "Bu analiz yatırım tavsiyesi değildir" uyarısını ekle.

ÇIKTI FORMATI (JSON):
{
    "overall_health_score": 1-10 arası puan,
    "overall_health_label": "Güçlü" / "İyi" / "Orta" / "Zayıf" / "Riskli",
    "revenue_trend": "Büyüyen ciro trendi açıklaması...",
    "profitability_analysis": "Kârlılık durumu değerlendirmesi...",
    "debt_analysis": "Borç yapısı ve risk değerlendirmesi...",
    "sector_comparison": "Sektör ortalamasına göre konum...",
    "key_strengths": ["Güçlü yön 1", "Güçlü yön 2"],
    "key_risks": ["Risk 1", "Risk 2"],
    "summary": "2-3 cümlelik genel değerlendirme",
    "disclaimer": "Bu analiz yatırım tavsiyesi değildir. Yatırım kararlarınızı kendi araştırmanıza dayandırın."
}
"""


def _build_bilanco_context(ticker: str, financials: list[dict], ratios: dict | None = None) -> str:
    """Bilanço verilerinden AI için context oluşturur."""
    lines = [f"## {ticker} Finansal Verileri\n"]

    if financials:
        lines.append("### Çeyreklik Veriler (son dönemden eskiye)")
        for f in financials[:8]:  # Son 8 çeyrek (2 yıl)
            lines.append(f"\n**{f.get('period', '?')}**")
            if f.get("revenue"):
                lines.append(f"- Ciro: {f['revenue']:,.0f} TL")
            if f.get("gross_profit"):
                lines.append(f"- Brüt Kâr: {f['gross_profit']:,.0f} TL")
            if f.get("net_income"):
                lines.append(f"- Net Kâr: {f['net_income']:,.0f} TL")
            if f.get("ebitda"):
                lines.append(f"- FAVÖK: {f['ebitda']:,.0f} TL")
            if f.get("total_assets"):
                lines.append(f"- Toplam Aktif: {f['total_assets']:,.0f} TL")
            if f.get("total_equity"):
                lines.append(f"- Özkaynaklar: {f['total_equity']:,.0f} TL")
            if f.get("net_debt"):
                lines.append(f"- Net Borç: {f['net_debt']:,.0f} TL")
            if f.get("gross_margin_pct"):
                lines.append(f"- Brüt Kâr Marjı: %{f['gross_margin_pct']:.1f}")
            if f.get("net_margin_pct"):
                lines.append(f"- Net Kâr Marjı: %{f['net_margin_pct']:.1f}")
            if f.get("roe_pct"):
                lines.append(f"- ROE: %{f['roe_pct']:.1f}")

    if ratios:
        lines.append("\n### Güncel Değerleme Çarpanları")
        if ratios.get("fk"):
            lines.append(f"- F/K: {ratios['fk']:.2f}")
        if ratios.get("pddd"):
            lines.append(f"- PD/DD: {ratios['pddd']:.2f}")
        if ratios.get("fd_favok"):
            lines.append(f"- FD/FAVÖK: {ratios['fd_favok']:.2f}")
        if ratios.get("piyasa_degeri"):
            lines.append(f"- Piyasa Değeri: {ratios['piyasa_degeri']:,.0f} TL")
        if ratios.get("sector"):
            lines.append(f"- Sektör: {ratios['sector']}")
        if ratios.get("sector_avg_fk"):
            lines.append(f"- Sektör Ort. F/K: {ratios['sector_avg_fk']:.2f}")
        if ratios.get("sector_avg_pddd"):
            lines.append(f"- Sektör Ort. PD/DD: {ratios['sector_avg_pddd']:.2f}")

    return "\n".join(lines)


async def _call_ai_abacus(system_prompt: str, user_message: str) -> str | None:
    """Abacus RouteLLM üzerinden Claude çağrısı."""
    abacus_key = settings.ABACUS_API_KEY
    if not abacus_key:
        logger.warning("ABACUS_API_KEY tanımlı değil")
        return None
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _ABACUS_URL,
                headers={"Authorization": f"Bearer {abacus_key}", "Content-Type": "application/json"},
                json={
                    "model": _AI_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    "temperature": 0.12,
                    "max_tokens": 4000,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            logger.warning("Abacus bilanço AI %d döndü", resp.status_code)
    except Exception as e:
        logger.exception("Abacus bilanço AI hatası: %s", e)
    return None


async def _call_ai_anthropic(system_prompt: str, user_message: str) -> str | None:
    """Doğrudan Anthropic API fallback."""
    api_key = settings.ANTHROPIC_API_KEY
    if not api_key:
        return None
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _ANTHROPIC_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _CLAUDE_MODEL,
                    "max_tokens": 4000,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_message}],
                    "temperature": 0.12,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                return data["content"][0]["text"]
            logger.warning("Anthropic bilanço AI %d döndü", resp.status_code)
    except Exception as e:
        logger.exception("Anthropic bilanço AI hatası: %s", e)
    return None


async def analyze_bilanco(ticker: str, financials: list[dict], ratios: dict | None = None) -> dict | None:
    """
    Bilanço verilerini AI ile analiz eder.

    Args:
        ticker: Hisse kodu
        financials: Çeyreklik bilanço verileri listesi
        ratios: Güncel F/K, PD/DD gibi oranlar

    Returns:
        dict — AI analiz sonucu (JSON) veya None
    """
    if not financials:
        logger.warning("Bilanço analizi için veri yok: %s", ticker)
        return None

    context = _build_bilanco_context(ticker, financials, ratios)
    user_message = f"Aşağıdaki {ticker} hissesinin finansal verilerini analiz et:\n\n{context}"

    # Önce Abacus, sonra Anthropic fallback
    content = await _call_ai_abacus(_SYSTEM_PROMPT, user_message)
    if not content:
        logger.info("Abacus başarısız, Anthropic deneniyor: %s", ticker)
        content = await _call_ai_anthropic(_SYSTEM_PROMPT, user_message)

    if not content:
        logger.error("Bilanço AI analizi başarısız: %s", ticker)
        return None

    # JSON parse
    try:
        # Claude bazen ```json ... ``` wrapper ile döner
        clean = content.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1]
            clean = clean.rsplit("```", 1)[0]
        return json.loads(clean)
    except json.JSONDecodeError:
        logger.warning("Bilanço AI JSON parse hatası %s, raw content döndürülüyor", ticker)
        return {"summary": content, "disclaimer": "Bu analiz yatırım tavsiyesi değildir."}


# ═══════════════════════════════════════════════════════════════════════════════
#  KAP BİLDİRİMİNDEN BİLANÇO RAKAMLARINI PARSE ET
# ═══════════════════════════════════════════════════════════════════════════════

_PARSE_SYSTEM_PROMPT = """Sen bir finansal veri çıkarma uzmanısın.
KAP bildiriminin metin içeriğinden bilanço ve gelir tablosu rakamlarını çıkarmalısın.

KURALLAR:
- Sadece metinde geçen rakamları kullan, uydurma.
- Rakamlar TL cinsindendir. Bin TL, milyon TL gibi birimleri tam TL'ye çevir.
- Eğer metinde rakam bulunamıyorsa o alan için null döndür.
- Sadece SON döneme ait verileri çıkar (en güncel bilanço).

ÇIKTI FORMATI (JSON, sadece bu formatı döndür):
{
    "period": "2025-Q4",
    "revenue": null veya rakam (TL),
    "gross_profit": null veya rakam,
    "operating_profit": null veya rakam,
    "net_income": null veya rakam,
    "ebitda": null veya rakam,
    "total_assets": null veya rakam,
    "total_equity": null veya rakam,
    "total_debt": null veya rakam,
    "net_debt": null veya rakam,
    "cash_and_equivalents": null veya rakam,
    "confidence": "high" veya "medium" veya "low"
}

Eğer bildirim bir bilanço/faaliyet raporu DEĞİLSE: {"error": "not_bilanco"}
"""


async def parse_bilanco_from_kap(ticker: str, kap_content: str) -> dict | None:
    """
    KAP bildirim metninden bilanço rakamlarını AI ile parse eder.

    Kullanım: KAP'ta bilanço bildirimi yakalandığında, IsYatirim'den veri gelmeden
    ÖNCE bu fonksiyon çağrılır → anında rakamlar DB'ye yazılır.

    IsYatirim verisi 1-2 gün sonra geldiğinde doğrulama yapılır ve güncellenir.

    Args:
        ticker: Hisse kodu
        kap_content: KAP bildirim metin içeriği (body)

    Returns:
        dict — Parse edilmiş bilanço rakamları veya None
    """
    if not kap_content or len(kap_content) < 50:
        logger.warning("KAP parse: %s — içerik çok kısa", ticker)
        return None

    # Finansal Rapor body'i 30K+ karakter olabilir — AI prompt limitine sığsın diye 30K cap
    user_message = (
        f"Aşağıdaki {ticker} hissesinin KAP Finansal Rapor metninden "
        f"bilanço/gelir tablosu rakamlarını çıkar:\n\n"
        f"---\n{kap_content[:30000]}\n---"
    )

    content = await _call_ai_abacus(_PARSE_SYSTEM_PROMPT, user_message)
    if not content:
        content = await _call_ai_anthropic(_PARSE_SYSTEM_PROMPT, user_message)

    if not content:
        logger.error("KAP bilanço parse başarısız: %s", ticker)
        return None

    try:
        clean = content.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1]
            clean = clean.rsplit("```", 1)[0]
        result = json.loads(clean)

        if result.get("error") == "not_bilanco":
            logger.info("KAP parse: %s — bilanço bildirimi değil", ticker)
            return None

        # Confidence kontrolü
        confidence = result.get("confidence", "low")
        if confidence == "low":
            logger.warning("KAP parse: %s — düşük güvenilirlik, doğrulama gerekli", ticker)

        result["ticker"] = ticker
        result["source"] = "kap_ai_parse"
        result["needs_verification"] = True  # IsYatirim ile doğrulanacak

        logger.info(
            "KAP bilanço parse OK: %s %s — Ciro: %s, Net Kâr: %s (güven: %s)",
            ticker, result.get("period", "?"),
            result.get("revenue"), result.get("net_income"), confidence,
        )
        return result

    except json.JSONDecodeError:
        logger.warning("KAP bilanço parse JSON hatası: %s", ticker)
        return None


async def save_parsed_bilanco(ticker: str, parsed: dict) -> bool:
    """
    AI ile parse edilen bilanço rakamlarını DB'ye kaydeder.

    needs_verification=True ile kaydedilir — IsYatirim'den kesin veri
    geldiğinde üzerine yazılır.
    """
    try:
        from app.database import async_session
        from app.models.company_financial import CompanyFinancial
        from sqlalchemy import select

        period = parsed.get("period")
        if not period:
            return False

        async with async_session() as db:
            # Var mı kontrol et
            stmt = select(CompanyFinancial).where(
                CompanyFinancial.ticker == ticker,
                CompanyFinancial.period == period,
            )
            existing = (await db.execute(stmt)).scalar_one_or_none()

            if existing and existing.source == "isyatirim":
                # IsYatirim verisi zaten var — üzerine yazma
                logger.info("KAP parse: %s %s — IsYatirim verisi mevcut, atlanıyor", ticker, period)
                return False

            from datetime import datetime, timezone

            if existing:
                # AI parse verisini güncelle (henüz IsYatirim gelmemişse)
                for field in ["revenue", "gross_profit", "operating_profit", "net_income",
                              "ebitda", "total_assets", "total_equity", "total_debt",
                              "net_debt", "cash_and_equivalents"]:
                    val = parsed.get(field)
                    if val is not None:
                        setattr(existing, field, val)
                existing.source = "kap_ai_parse"
                existing.updated_at = datetime.now(timezone.utc)
            else:
                new_record = CompanyFinancial(
                    ticker=ticker,
                    period=period,
                    revenue=parsed.get("revenue"),
                    gross_profit=parsed.get("gross_profit"),
                    operating_profit=parsed.get("operating_profit"),
                    net_income=parsed.get("net_income"),
                    ebitda=parsed.get("ebitda"),
                    total_assets=parsed.get("total_assets"),
                    total_equity=parsed.get("total_equity"),
                    total_debt=parsed.get("total_debt"),
                    net_debt=parsed.get("net_debt"),
                    cash_and_equivalents=parsed.get("cash_and_equivalents"),
                    source="kap_ai_parse",
                )
                db.add(new_record)

            await db.commit()
            logger.info("KAP parse DB kayıt: %s %s", ticker, period)
            return True

    except Exception as e:
        logger.exception("KAP parse DB hatası %s: %s", ticker, e)
        return False
