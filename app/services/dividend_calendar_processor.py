"""Temettü Takvimi Processor — KAP bildirimlerinden state machine.

is_capital_increase() ile ayni mantik — title pattern + classify event + AI parse.

Etkinlikler:
  ykk         — Yonetim kurulu temettu karari (yeni satir)
  ga_approval — Genel kurul onayi
  rejection   — Reddedildi/iptal
  payment     — Odeme/hak kullanim tarihi ilan edildi
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dividend_calendar import DividendCalendar
from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# Title pattern siniflandirici
# ═══════════════════════════════════════════════════════════════════

_DIVIDEND_TITLE_PATTERNS = [
    # KAP gerçek başlıkları (production DB analizinden)
    "temettü", "temettu",
    "kar payı dağıtım", "kar payı dagitim",                    # 70+ kayit
    "kar dağıtım", "kar dagitim",                                  # 4+
    "kâr payı", "kâr dağıtım",
    "hak kullanımı", "hak kullanim",                               # 14 kayit (temettu hak kullanim tarihi)
    "mali hak kullan", "pay mali hak",                             # 11+ kayit (nakit odeme)
    "pay mali hak kullanım", "pay mali hak kullanim",
    "nakit ödeme", "nakit odeme",
    "temettü ödeme", "temettu odeme",
]

_PATTERN_GA_APPROVAL = [
    "genel kurul", "olağan genel kurul",
    "genel kurulda kabul",
]
_PATTERN_REJECTION = [
    "reddedil", "iptal edil", "vazgeç",
    "dağıtılmamasına", "dagitilmamasina",
]
_PATTERN_PAYMENT = [
    "ödeme tarihi", "odeme tarihi",
    "hak kullanım", "hak kullanim",
    "dağıtım tarihi", "dagitim tarihi",
    "kupon kesim",
]
_PATTERN_YKK = [
    "yönetim kurulu kararı", "yonetim kurulu karari",
    "yönetim kurulu", "yonetim kurulu",
]


def is_dividend(title: str) -> bool:
    """Title temettu ile ilgili mi?"""
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _DIVIDEND_TITLE_PATTERNS)


def classify_event(title: str) -> str:
    """Etkinlik tipini belirler.

    Returns: 'ykk' | 'ga_approval' | 'rejection' | 'payment' | 'unknown'
    """
    if not title:
        return "unknown"
    t = lower_tr(title)
    if any(p in t for p in _PATTERN_REJECTION):
        return "rejection"
    if any(p in t for p in _PATTERN_PAYMENT):
        return "payment"
    if any(p in t for p in _PATTERN_GA_APPROVAL):
        return "ga_approval"
    if any(p in t for p in _PATTERN_YKK):
        return "ykk"
    return "unknown"


# ═══════════════════════════════════════════════════════════════════
# Gemini AI parser
# ═══════════════════════════════════════════════════════════════════

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_AI_TIMEOUT = 30


def _get_gemini_key() -> Optional[str]:
    try:
        from app.config import get_settings
        return get_settings().GEMINI_API_KEY or None
    except Exception:
        return None


_PARSE_PROMPT = """Asagidaki KAP temettu bildirimini analiz et ve yapilandirilmis JSON dondur.

KAP BILDIRIMI:
Hisse: {ticker}
Baslik: {title}
Icerik:
{body}

Donen JSON sablonu (bilgi yoksa null):
{{
  "period": "2025" veya "2025-Q4" (donem),
  "gross_amount_per_share": <hisse basi brut temettu TL>,
  "net_amount_per_share": <hisse basi net temettu TL>,
  "gross_yield_pct": <brut verim yuzdesi>,
  "net_yield_pct": <net verim yuzdesi>,
  "total_amount_tl": <toplam dagitilacak TL>,
  "ykk_date": "YYYY-MM-DD",
  "general_assembly_date": "YYYY-MM-DD",
  "payment_date": "YYYY-MM-DD"
}}

KURALLAR:
- SADECE JSON dondur.
- Brut/net rakamlar TL cinsinden hisse basi.
- Yuzde verim: temettu/hisse_fiyat * 100.
- Tarihler bildirimde gecen tarihler.
- Bilinmeyenler null.
"""


_DATE_REGEX = re.compile(r"([0-3]?[0-9])[./]([0-1]?[0-9])[./](20[0-9]{2})")


async def ai_parse_dividend(
    ticker: str,
    title: str,
    body: str,
) -> dict[str, Any]:
    """KAP body'sinden temettu yapilandirilmis veri cikar."""
    out: dict[str, Any] = {
        "period": None,
        "gross_amount_per_share": None,
        "net_amount_per_share": None,
        "gross_yield_pct": None,
        "net_yield_pct": None,
        "total_amount_tl": None,
        "ykk_date": None,
        "general_assembly_date": None,
        "payment_date": None,
    }

    gemini_key = _get_gemini_key()
    if not gemini_key or not body:
        return out

    prompt = _PARSE_PROMPT.format(
        ticker=ticker,
        title=title or "",
        body=(body or "")[:4000],
    )

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _GEMINI_URL,
                headers={
                    "Authorization": f"Bearer {gemini_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Sen finansal verileri yapilandirilmis JSON'a ceviren bir analizcisin. SADECE JSON dondur."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 1024,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                parsed = _parse_ai_json(content.strip()) if content else None
                if parsed:
                    if isinstance(parsed.get("period"), str):
                        out["period"] = parsed["period"][:20]
                    for k in ("gross_amount_per_share", "net_amount_per_share",
                              "gross_yield_pct", "net_yield_pct", "total_amount_tl"):
                        v = parsed.get(k)
                        if isinstance(v, (int, float)) and v > 0:
                            out[k] = float(v)
                    for k in ("ykk_date", "general_assembly_date", "payment_date"):
                        d = parsed.get(k)
                        if isinstance(d, str):
                            try:
                                out[k] = date.fromisoformat(d)
                            except ValueError:
                                pass
            else:
                logger.warning("Dividend AI: HTTP %s — %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("Dividend AI hata: %s", e)

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


# ═══════════════════════════════════════════════════════════════════
# State machine
# ═══════════════════════════════════════════════════════════════════

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
) -> Optional[DividendCalendar]:
    """KAP bildirimini temettu state machine'e gonder.

    Temettu degilse None doner.
    """
    if not is_dividend(title):
        return None

    event_type = classify_event(title)
    if event_type == "unknown":
        event_type = "ykk"

    parsed = await ai_parse_dividend(ticker, title, body or "")

    period = parsed.get("period")
    ykk_dt = parsed.get("ykk_date") or (published_at.date() if published_at and event_type == "ykk" else None)

    # Mevcut kayit ara
    stmt = (
        select(DividendCalendar)
        .where(DividendCalendar.ticker == ticker)
        .where(DividendCalendar.status.notin_(["tamamlandi", "reddedildi"]))
        .order_by(DividendCalendar.created_at.desc())
        .limit(1)
    )
    res = await db.execute(stmt)
    existing = res.scalar_one_or_none()

    today = date.today()

    if event_type == "ykk":
        if not existing:
            new_row = DividendCalendar(
                ticker=ticker,
                company_name=company_name,
                period=period,
                gross_amount_per_share=parsed.get("gross_amount_per_share"),
                net_amount_per_share=parsed.get("net_amount_per_share"),
                gross_yield_pct=parsed.get("gross_yield_pct"),
                net_yield_pct=parsed.get("net_yield_pct"),
                total_amount_tl=parsed.get("total_amount_tl"),
                ykk_date=ykk_dt,
                ykk_kap_disclosure_id=disclosure_id,
                ykk_kap_url=kap_url,
                status="ykk_alindi",
            )
            db.add(new_row)
            await db.flush()
            logger.info("Dividend: yeni YKK (%s, period=%s)", ticker, period)
            return new_row
        # Mevcudu zenginlestir
        for k in ("period", "gross_amount_per_share", "net_amount_per_share",
                  "gross_yield_pct", "net_yield_pct", "total_amount_tl"):
            v = parsed.get(k)
            if v and not getattr(existing, k):
                setattr(existing, k, v)
        if not existing.ykk_date and ykk_dt:
            existing.ykk_date = ykk_dt
            existing.ykk_kap_disclosure_id = disclosure_id
            existing.ykk_kap_url = kap_url
        return existing

    if event_type == "ga_approval":
        if not existing:
            existing = DividendCalendar(
                ticker=ticker, company_name=company_name, period=period,
                status="ykk_alindi",
            )
            db.add(existing)
            await db.flush()
        existing.general_assembly_date = parsed.get("general_assembly_date") or (
            published_at.date() if published_at else None
        )
        existing.general_assembly_kap_disclosure_id = disclosure_id
        existing.general_assembly_kap_url = kap_url
        if existing.status == "ykk_alindi":
            existing.status = "genel_kurul_onayli"
        # Eger ayni bildirimde odeme tarihi de varsa
        pay_dt = parsed.get("payment_date")
        if pay_dt and not existing.payment_date:
            existing.payment_date = pay_dt
            existing.status = "tarih_belli"
        logger.info("Dividend: GK onay (%s)", ticker)
        return existing

    if event_type == "rejection":
        if not existing:
            existing = DividendCalendar(
                ticker=ticker, company_name=company_name, period=period,
                status="reddedildi",
            )
            db.add(existing)
            await db.flush()
        existing.status = "reddedildi"
        existing.rejected_at = datetime.now(timezone.utc)
        existing.rejection_kap_disclosure_id = disclosure_id
        existing.rejection_kap_url = kap_url
        logger.info("Dividend: red (%s)", ticker)
        return existing

    if event_type == "payment":
        if not existing:
            existing = DividendCalendar(
                ticker=ticker, company_name=company_name, period=period,
                status="ykk_alindi",
            )
            db.add(existing)
            await db.flush()
        pay_dt = parsed.get("payment_date")
        if pay_dt:
            existing.payment_date = pay_dt
            existing.payment_kap_disclosure_id = disclosure_id
            existing.payment_kap_url = kap_url
            if pay_dt > today:
                existing.status = "tarih_belli"
            elif pay_dt == today:
                existing.status = "odeniyor"
            else:
                existing.status = "tamamlandi"
        # Yeni amounts varsa guncelle
        for k in ("gross_amount_per_share", "net_amount_per_share",
                  "gross_yield_pct", "net_yield_pct", "total_amount_tl"):
            v = parsed.get(k)
            if v and not getattr(existing, k):
                setattr(existing, k, v)
        logger.info("Dividend: odeme tarihi (%s, %s)", ticker, pay_dt)
        return existing

    return existing


async def update_payment_statuses(db: AsyncSession) -> int:
    """Gunluk gorev — odeme tarihi gelenleri 'odeniyor', gecmis tarihleri 'tamamlandi' yap."""
    today = date.today()
    updated = 0

    stmt = (
        select(DividendCalendar)
        .where(DividendCalendar.status.in_(["tarih_belli", "odeniyor"]))
        .where(DividendCalendar.payment_date.isnot(None))
    )
    res = await db.execute(stmt)
    for row in res.scalars().all():
        if row.payment_date == today and row.status != "odeniyor":
            row.status = "odeniyor"
            updated += 1
        elif row.payment_date < today and row.status != "tamamlandi":
            row.status = "tamamlandi"
            updated += 1

    if updated:
        await db.flush()
    return updated
