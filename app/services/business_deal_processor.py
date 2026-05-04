"""İş Anlaşması Processor — KAP'tan AI parse + TRY çevrim (TCMB).

Title patterns + Gemini ile body'den tutar + para birimi çıkarır.
TCMB güncel kur ile TRY'a çevirir.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.business_deal import BusinessDeal
from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


# ─── Title patterns — iş anlaşması ile ilgili KAP başlıkları ───
_TITLE_PATTERNS = [
    # KAP'ta gerçek başlıklar (production DB analizinden)
    "yeni iş ilişkisi", "yeni is iliskisi",          # 21 kayit son 30 gun
    "ihale süreci", "ihale sonucu", "ihale sonuçland",  # 5+
    "ihale alındı", "ihale alınmış",
    "sözleşme imzalan", "sozlesme imzaland",
    "iş anlaşması", "is anlasmasi",
    "yeni müşteri", "yeni musteri",
    "önemli nitelikteki işlem", "onemli nitelikteki",
    "işbirliği", "isbirligi",
    "tedarik anlaşması", "tedarik sözleşmesi",
    "satış sözleşmesi", "satis sozlesmesi",
]


def is_business_deal(title: str) -> bool:
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _TITLE_PATTERNS)


# ─── Gemini AI ───
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_AI_TIMEOUT = 30


def _get_gemini_key() -> Optional[str]:
    try:
        from app.config import get_settings
        return get_settings().GEMINI_API_KEY or None
    except Exception:
        return None


_PARSE_PROMPT = """Asagidaki KAP is anlasmasi/sozlesme bildirimini analiz et ve yapilandirilmis JSON dondur.

KAP BILDIRIMI:
Hisse: {ticker}
Baslik: {title}
Icerik:
{body}

Donen JSON sablonu (bilgi yoksa null):
{{
  "amount_original": <sozlesme tutari sayi>,
  "currency": "TRY" | "USD" | "EUR" | "GBP",
  "deal_date": "YYYY-MM-DD",
  "counterparty": "Karsi taraf (musteri/satici) adi",
  "summary": "Kisa Turkce ozet (max 150 char)"
}}

KURALLAR:
- SADECE JSON dondur.
- Tutar yoksa null. KDV hariç tutar tercih edilir.
- Para birimi: TL/Lira/₺ -> TRY, dolar/USD -> USD, vs.
- Tarih bildirimde gecen sozlesme tarihi (rapor tarihi degil).
- Bilinmeyenler null.
"""


async def ai_parse_business_deal(ticker: str, title: str, body: str) -> dict[str, Any]:
    """KAP body'sinden iş anlaşması yapılandırılmış veri çıkar."""
    out: dict[str, Any] = {
        "amount_original": None, "currency": None,
        "deal_date": None, "counterparty": None, "summary": None,
    }
    gemini_key = _get_gemini_key()
    if not gemini_key or not body:
        return out
    prompt = _PARSE_PROMPT.format(ticker=ticker, title=title or "", body=(body or "")[:30000])
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _GEMINI_URL,
                headers={"Authorization": f"Bearer {gemini_key}", "Content-Type": "application/json"},
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Sen finansal verileri yapilandirilmis JSON'a ceviren bir analizcisin. SADECE JSON dondur."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1, "max_tokens": 1024,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                parsed = _parse_ai_json(content.strip()) if content else None
                if parsed:
                    if isinstance(parsed.get("amount_original"), (int, float)):
                        out["amount_original"] = float(parsed["amount_original"])
                    cur = parsed.get("currency")
                    if isinstance(cur, str) and cur.upper() in ("TRY", "USD", "EUR", "GBP"):
                        out["currency"] = cur.upper()
                    if isinstance(parsed.get("deal_date"), str):
                        try:
                            out["deal_date"] = date.fromisoformat(parsed["deal_date"])
                        except ValueError:
                            pass
                    cp = parsed.get("counterparty")
                    if isinstance(cp, str):
                        out["counterparty"] = cp[:500]
                    s = parsed.get("summary")
                    if isinstance(s, str):
                        out["summary"] = s[:300]
    except Exception as e:
        logger.warning("BusinessDeal AI hata: %s", e)
    return out


def _parse_ai_json(text: str) -> Optional[dict[str, Any]]:
    if not text:
        return None
    if "```" in text:
        text = re.sub(r"```(?:json)?\s*", "", text)
        text = text.replace("```", "")
    s = text.find("{")
    e = text.rfind("}")
    if s < 0 or e < 0 or e < s:
        return None
    try:
        return json.loads(text[s:e + 1])
    except json.JSONDecodeError:
        return None


# ─── Anlik kur servisi (ucretsiz, auth'suz) ───
# Geçmiş işlem için bile bugünkü kur kullanılır (kullanıcı tercihi).
# Birincil: exchangerate.host (ECB tabanlı). Yedek: Frankfurter (ECB).
_RATES_CACHE: dict[str, tuple[float, datetime]] = {}
_RATES_TTL = 6 * 3600  # 6 saat


async def _fetch_from_exchangerate_host(currency: str) -> Optional[float]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.exchangerate.host/latest",
                params={"base": currency, "symbols": "TRY"},
            )
            if r.status_code == 200:
                data = r.json()
                rate = data.get("rates", {}).get("TRY")
                if isinstance(rate, (int, float)) and rate > 0:
                    return float(rate)
    except Exception as e:
        logger.debug("exchangerate.host fail (%s): %s", currency, e)
    return None


async def _fetch_from_frankfurter(currency: str) -> Optional[float]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.frankfurter.app/latest",
                params={"from": currency, "to": "TRY"},
            )
            if r.status_code == 200:
                data = r.json()
                rate = data.get("rates", {}).get("TRY")
                if isinstance(rate, (int, float)) and rate > 0:
                    return float(rate)
    except Exception as e:
        logger.debug("frankfurter fail (%s): %s", currency, e)
    return None


async def get_exchange_rate(currency: str) -> tuple[Optional[float], Optional[date]]:
    """Anlık kur (TRY karşılığı) — cache'li.

    Kullanılan API'ler:
        1. exchangerate.host (birincil, ücretsiz, auth'suz)
        2. Frankfurter (yedek, ECB)

    Returns: (rate, fetch_date) veya (None, None)
    """
    if not currency or currency == "TRY":
        return (1.0, date.today())
    now = datetime.now()
    if currency in _RATES_CACHE:
        rate, ts = _RATES_CACHE[currency]
        if (now - ts).total_seconds() < _RATES_TTL:
            return (rate, ts.date())

    # Önce exchangerate.host
    rate = await _fetch_from_exchangerate_host(currency)
    # Yedek: Frankfurter
    if not rate:
        rate = await _fetch_from_frankfurter(currency)

    if rate:
        _RATES_CACHE[currency] = (rate, now)
        return (rate, now.date())

    logger.warning("Kur bulunamadi (her iki kaynak da basarisiz): %s", currency)
    return (None, None)


# Geriye dönük uyumluluk için eski isim
get_tcmb_rate = get_exchange_rate


# ─── State machine ───
async def process_kap_disclosure(
    db: AsyncSession,
    *,
    disclosure_id: int,
    ticker: str,
    company_name: Optional[str],
    title: str,
    body: Optional[str],
    kap_url: Optional[str],
    published_at: Optional[datetime],
) -> Optional[BusinessDeal]:
    """KAP bildirimini iş anlaşması state machine'e gonder.

    Iş anlaşması değilse None döner.
    """
    if not is_business_deal(title):
        return None

    # Mevcut kayit — varsa amount_try doluysa skip, bossa re-parse + UPDATE
    existing = None
    if disclosure_id:
        stmt = select(BusinessDeal).where(BusinessDeal.kap_disclosure_id == disclosure_id).limit(1)
        existing = (await db.execute(stmt)).scalar_one_or_none()
        if existing and existing.amount_try is not None:
            return existing  # Tutar dolu — atla

    parsed = await ai_parse_business_deal(ticker, title, body or "")

    deal_date = parsed.get("deal_date") or (published_at.date() if published_at else date.today())
    currency = parsed.get("currency") or "TRY"
    amount_original = parsed.get("amount_original")

    amount_try = None
    rate_used = None
    rate_date = None
    if amount_original and currency:
        if currency == "TRY":
            amount_try = amount_original
            rate_used = 1.0
            rate_date = deal_date
        else:
            rate_used, rate_date = await get_exchange_rate(currency)
            if rate_used:
                amount_try = amount_original * rate_used

    # Mevcut kaydi UPDATE et — yeni AI parse sonucuyla
    if existing:
        if amount_original is not None:
            existing.amount_original = amount_original
            existing.currency = currency
            existing.amount_try = amount_try
            existing.exchange_rate_used = rate_used
            existing.rate_date = rate_date
        if parsed.get("counterparty") and not existing.counterparty:
            existing.counterparty = parsed["counterparty"]
        if parsed.get("summary") and not existing.summary:
            existing.summary = parsed["summary"]
        await db.flush()
        logger.info("BusinessDeal: UPDATE (%s, %s %s)", ticker, amount_original, currency)
        return existing

    new_row = BusinessDeal(
        ticker=ticker,
        company_name=company_name,
        title=(title or "")[:500],
        summary=parsed.get("summary"),
        amount_original=amount_original,
        currency=currency,
        amount_try=amount_try,
        exchange_rate_used=rate_used,
        rate_date=rate_date,
        deal_date=deal_date,
        counterparty=parsed.get("counterparty"),
        kap_disclosure_id=disclosure_id,
        kap_url=kap_url,
        source="kap_ai_parse",
    )
    db.add(new_row)
    await db.flush()
    logger.info("BusinessDeal: yeni (%s, %s %s = %s TRY)", ticker, amount_original, currency, amount_try)
    return new_row
