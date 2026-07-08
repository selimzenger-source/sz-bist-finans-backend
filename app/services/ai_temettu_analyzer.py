"""
Temettü AI Analiz Servisi
Hisse senedi temettü geçmişini ve sürdürülebilirliğini Claude AI ile analiz eder.
"""

import httpx
import json
import logging
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_AI_MODEL = "claude-sonnet-4-6"
_CLAUDE_MODEL = "claude-sonnet-4-6"
_AI_TIMEOUT = 120


_SYSTEM_PROMPT = """Sen bir Türk borsası (BIST) temettü analiz uzmanısın.
Görevin, verilen temettü geçmişi ve bilanço verilerini değerlendirerek yatırımcıya temettü sürdürülebilirliği hakkında analiz sunmak.

KURALLAR:
- Türkçe yaz, sade ve anlaşılır bir dil kullan.
- Kesinlikle yatırım tavsiyesi VERME.
- Veride olmayan bilgiyi uydurma.
- "Bu analiz yatırım tavsiyesi değildir" uyarısını ekle.

ÇIKTI FORMATI (JSON):
{
    "yield_score": 1-10 arası puan (temettü verimi değerlendirmesi),
    "sustainability_score": 1-10 arası puan (sürdürülebilirlik),
    "growth_trend": "Büyüyen" / "Sabit" / "Azalan" / "Düzensiz",
    "growth_analysis": "Temettü büyüme trendi detaylı açıklama...",
    "sustainability_analysis": "Temettü sürdürülebilirliği değerlendirmesi...",
    "sector_comparison": "Sektör ortalamasına göre temettü verimi karşılaştırma...",
    "dividend_history_summary": "Son 5 yılın temettü özeti...",
    "key_positives": ["Olumlu yön 1", "Olumlu yön 2"],
    "key_concerns": ["Endişe 1", "Endişe 2"],
    "summary": "2-3 cümlelik genel değerlendirme",
    "disclaimer": "Bu analiz yatırım tavsiyesi değildir. Yatırım kararlarınızı kendi araştırmanıza dayandırın."
}
"""


def _build_temettu_context(ticker: str, dividend_history: list[dict], current_data: dict | None = None, bilanco_summary: dict | None = None) -> str:
    """Temettü verilerinden AI için context oluşturur."""
    lines = [f"## {ticker} Temettü Verileri\n"]

    if dividend_history:
        lines.append("### Temettü Geçmişi (yeniden eskiye)")
        for d in dividend_history:
            year = d.get("payment_year", "?")
            lines.append(f"\n**{year}**")
            if d.get("gross_dividend_per_share"):
                lines.append(f"- Brüt Temettü/Hisse: {d['gross_dividend_per_share']:.4f} TL")
            if d.get("net_dividend_per_share"):
                lines.append(f"- Net Temettü/Hisse: {d['net_dividend_per_share']:.4f} TL")
            if d.get("dividend_yield_pct"):
                lines.append(f"- Temettü Verimi: %{d['dividend_yield_pct']:.2f}")
            if d.get("payment_date"):
                lines.append(f"- Ödeme Tarihi: {d['payment_date']}")

    if current_data:
        lines.append("\n### Güncel Temettü Bilgileri")
        if current_data.get("expected_dividend_yield_pct"):
            lines.append(f"- Beklenen Temettü Verimi: %{current_data['expected_dividend_yield_pct']:.2f}")
        if current_data.get("avg_2y_yield_pct"):
            lines.append(f"- 2 Yıllık Ort. Verim: %{current_data['avg_2y_yield_pct']:.2f}")
        if current_data.get("payout_ratio"):
            lines.append(f"- Dağıtım Oranı: %{current_data['payout_ratio']:.1f}")
        if current_data.get("consecutive_years"):
            lines.append(f"- Üst Üste Temettü Yılı: {current_data['consecutive_years']}")

    if bilanco_summary:
        lines.append("\n### Bilanço Özeti (Temettü Sürdürülebilirlik Bağlamı)")
        if bilanco_summary.get("net_income"):
            lines.append(f"- Son Dönem Net Kâr: {bilanco_summary['net_income']:,.0f} TL")
        if bilanco_summary.get("net_debt"):
            lines.append(f"- Net Borç: {bilanco_summary['net_debt']:,.0f} TL")
        if bilanco_summary.get("roe_pct"):
            lines.append(f"- ROE: %{bilanco_summary['roe_pct']:.1f}")

    return "\n".join(lines)


async def _call_ai(system_prompt: str, user_message: str) -> str | None:
    """AI çağrısı — Abacus öncelikli, Anthropic fallback."""
    # Abacus
    abacus_key = settings.ABACUS_API_KEY
    if abacus_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                resp = await client.post(
                    _ABACUS_URL,
                    headers={"Authorization": f"Bearer {abacus_key}", "Content-Type": "application/json"},
                    json={
                        "model": _AI_MODEL,
                        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_message}],
                        "temperature": 0.12,
                        "max_tokens": 4000,
                    },
                )
                if resp.status_code == 200:
                    return resp.json()["choices"][0]["message"]["content"]
        except Exception as e:
            logger.warning("Abacus temettü AI hatası: %s", e)

    # Anthropic fallback
    api_key = settings.ANTHROPIC_API_KEY
    if api_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                resp = await client.post(
                    _ANTHROPIC_URL,
                    headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "Content-Type": "application/json"},
                    json={
                        "model": _CLAUDE_MODEL,
                        "max_tokens": 4000,
                        "system": system_prompt,
                        "messages": [{"role": "user", "content": user_message}],
                        "temperature": 0.12,
                    },
                )
                if resp.status_code == 200:
                    return resp.json()["content"][0]["text"]
        except Exception as e:
            logger.warning("Anthropic temettü AI hatası: %s", e)

    return None


async def analyze_temettu(ticker: str, dividend_history: list[dict], current_data: dict | None = None, bilanco_summary: dict | None = None) -> dict | None:
    """
    Temettü verilerini AI ile analiz eder.

    Args:
        ticker: Hisse kodu
        dividend_history: Geçmiş temettü ödemeleri listesi
        current_data: Güncel temettü bilgileri
        bilanco_summary: Bilanço özeti (sürdürülebilirlik bağlamı)

    Returns:
        dict — AI analiz sonucu (JSON) veya None
    """
    if not dividend_history and not current_data:
        logger.warning("Temettü analizi için veri yok: %s", ticker)
        return None

    context = _build_temettu_context(ticker, dividend_history, current_data, bilanco_summary)
    user_message = f"Aşağıdaki {ticker} hissesinin temettü verilerini analiz et:\n\n{context}"

    content = await _call_ai(_SYSTEM_PROMPT, user_message)
    if not content:
        logger.error("Temettü AI analizi başarısız: %s", ticker)
        return None

    try:
        clean = content.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1]
            clean = clean.rsplit("```", 1)[0]
        return json.loads(clean)
    except json.JSONDecodeError:
        logger.warning("Temettü AI JSON parse hatası %s", ticker)
        return {"summary": content, "disclaimer": "Bu analiz yatırım tavsiyesi değildir."}
