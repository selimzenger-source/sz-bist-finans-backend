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
    "reddedil", "iptal edil", "vazgeç", "vazgec",
    "dağıtılmamasına", "dagitilmamasina",
    "dağıtmama kararı", "dagitmama karari",
    "dağıtmaması", "dagitmamasi",
    "dağıtılmaması", "dagitilmamasi",
    "dağıtım yapılmaması", "dagitim yapilmamasi",
    "kar payı dağıtılmama", "kar payi dagitilmama",
    "temettü dağıtılmama", "temettu dagitilmama",
    "kar dağıtmama", "kar dagitmama",
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
            # YKK'da amounts varsa hemen dividend_history'ye yansit
            try:
                if new_row.gross_amount_per_share or new_row.net_amount_per_share:
                    await mirror_to_dividend_history(db, new_row)
            except Exception as _e:
                logger.warning("ykk mirror hatasi (%s): %s", ticker, _e)
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
        # Guncellenmis amounts varsa mirror
        try:
            if existing.gross_amount_per_share or existing.net_amount_per_share:
                await mirror_to_dividend_history(db, existing)
        except Exception as _e:
            logger.warning("ykk mirror hatasi (%s): %s", ticker, _e)
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
        # GK onayi/odeme tarihi -> dividend_history'ye de yansit (Temettu sayfasi besleme)
        try:
            if event_type in ("ga_approval", "payment") and existing:
                await mirror_to_dividend_history(db, existing)
        except Exception as _e:
            logger.warning("dividend_history mirror hatasi (%s): %s", ticker, _e)
        return existing

    # YKK ve diger event'lerde de mirror dene (amounts varsa)
    try:
        if existing and (existing.gross_amount_per_share or existing.net_amount_per_share):
            await mirror_to_dividend_history(db, existing)
    except Exception:
        pass
    return existing


async def mirror_to_dividend_history(db: AsyncSession, row: "DividendCalendar") -> bool:
    """KAP'tan gelen GK temettu kararini dividend_history tablosuna da yansit.

    Bu sayede:
    - /api/v1/temettu/{ticker} endpoint'i bu odemeyi gosterir
    - /api/v1/temettu-takvim takvimi bu odemeyi listeler
    - temettuhisseleri.com batch'i ile cakismaz (source='kap_gk' ile ayri tutulur)

    Args:
        row: DividendCalendar satiri (process_kap_disclosure cikti)
    """
    from app.models.dividend import DividendHistory

    if not row or not row.ticker:
        return False

    # Payment date veya GK date'den yili cikar
    pay_dt = row.payment_date
    year = (pay_dt.year if pay_dt else None) or (
        row.general_assembly_date.year if row.general_assembly_date else None
    ) or (row.ykk_date.year if row.ykk_date else None)
    if not year:
        return False

    gross = row.gross_amount_per_share
    net = row.net_amount_per_share
    yield_pct = row.gross_yield_pct

    # Duplicate koruma stratejisi:
    # 1. pay_dt VAR → exact (ticker+year+payment_date) eslesmesi ara, yoksa
    #    null-date satirini guncelle (taksit varsa amount + yil ile dogrula)
    # 2. pay_dt YOK → sadece null-date + ayni amount ile match (taksit ayrimi),
    #    bulunamazsa yeni satir
    existing = None
    if pay_dt is not None:
        # Önce tarih+yil ile ara (exact)
        exact = await db.execute(select(DividendHistory).where(
            DividendHistory.ticker == row.ticker,
            DividendHistory.payment_year == year,
            DividendHistory.payment_date == pay_dt,
        ))
        existing = exact.scalars().first()
        if not existing:
            # null-date kaydi varsa ve amount eslesirse uzerine yaz (taksit zenginlestirme)
            null_q = await db.execute(select(DividendHistory).where(
                DividendHistory.ticker == row.ticker,
                DividendHistory.payment_year == year,
                DividendHistory.payment_date.is_(None),
            ))
            for cand in null_q.scalars().all():
                # Amount yakin (~%5) ise ayni odemedir varsayalim
                if gross and cand.gross_dividend_per_share:
                    diff = abs(float(cand.gross_dividend_per_share) - float(gross))
                    if diff / max(float(gross), 0.0001) < 0.05:
                        existing = cand
                        break
                elif not cand.gross_dividend_per_share:
                    existing = cand
                    break
    else:
        # pay_dt yok — sadece null-date + amount match ile birlestir, yoksa yeni
        null_q = await db.execute(select(DividendHistory).where(
            DividendHistory.ticker == row.ticker,
            DividendHistory.payment_year == year,
            DividendHistory.payment_date.is_(None),
        ))
        for cand in null_q.scalars().all():
            if gross and cand.gross_dividend_per_share:
                diff = abs(float(cand.gross_dividend_per_share) - float(gross))
                if diff / max(float(gross), 0.0001) < 0.05:
                    existing = cand
                    break

    if existing:
        if gross and not existing.gross_dividend_per_share:
            existing.gross_dividend_per_share = gross
        if net and not existing.net_dividend_per_share:
            existing.net_dividend_per_share = net
        if yield_pct and not existing.dividend_yield_pct:
            existing.dividend_yield_pct = yield_pct
        if pay_dt and not existing.payment_date:
            existing.payment_date = pay_dt
        # Source'u guncellenmis goster
        if existing.source != "temettuhisseleri":
            existing.source = "kap_gk"
    else:
        new_hist = DividendHistory(
            ticker=row.ticker,
            payment_year=year,
            gross_dividend_per_share=gross,
            net_dividend_per_share=net,
            dividend_yield_pct=yield_pct,
            payment_date=pay_dt,
            source="kap_gk",
        )
        db.add(new_hist)
    return True


# ═══════════════════════════════════════════════════════════════════
# BIST/MKK Temettü Ödeme Duyurusu — RSC scrape (AI YOK)
# ═══════════════════════════════════════════════════════════════════
#
# Örnek: https://www.kap.org.tr/tr/Bildirim/1600207
# Title: "BISTECH Pay Piyasası Alım Satım Sistemi Duyurusu"
# Body : "ALARK.E Pay Başına Brüt Temettü: 3,185 TL Teorik Fiyat: 92,465 TL"
#         "EGGUB.E Pay Başına Brüt Temettü: 2,5 TL Teorik Fiyat: 124,3 TL"
#         "KFEIN.E Pay Başına Brüt Temettü: 0,0202531 TL Teorik Fiyat: 8,69 TL"
#
# Bu bildirim, temettü ödemesinin BIST sistemine düştüğünü gösterir.
# DividendCalendar'da ilgili (ticker, gross_amount_per_share) kayıtlarını
# 'tamamlandi' / 'odeniyor' duruma çek.

_PAYMENT_RE = re.compile(
    r"\b([A-Z]{2,6})\.E\s+(?:Pay\s+Başına\s+Brüt\s+Temettü|Gross\s+Dividend\s+Payment\s+per\s+share)\s*:\s*"
    r"([0-9]+(?:[.,][0-9]+)?)\s*TL",
    re.IGNORECASE,
)


def parse_dividend_payment_announcement(body: str) -> list[dict[str, Any]]:
    """KAP/MKK pay piyasası duyurusundan ödenen temettüleri çıkar.

    Returns:
        [{"ticker": "ALARK", "gross_amount_per_share": 3.185}, ...]
    """
    if not body:
        return []
    seen: set[str] = set()
    results: list[dict[str, Any]] = []
    for m in _PAYMENT_RE.finditer(body):
        ticker = m.group(1).upper()
        if ticker in seen:
            continue
        seen.add(ticker)
        raw_amt = m.group(2).replace(".", "").replace(",", ".")
        try:
            amount = float(raw_amt)
        except ValueError:
            continue
        results.append({"ticker": ticker, "gross_amount_per_share": amount})
    return results


def is_dividend_payment_announcement(title: str, body: str) -> bool:
    """Title + body üzerinden temettü ödeme duyurusu mu?"""
    if not body:
        return False
    has_pattern = bool(_PAYMENT_RE.search(body))
    return has_pattern


async def process_dividend_payment_announcement(
    db: AsyncSession,
    *,
    body: str,
    kap_url: Optional[str],
    disclosure_id: Optional[int],
    published_at: Optional[datetime],
) -> dict[str, Any]:
    """Body'den ödenen ticker'ları çıkar, DividendCalendar status'ları güncelle.

    Returns:
        {"matched": N, "updated": N, "tickers": [...]}
    """
    items = parse_dividend_payment_announcement(body or "")
    if not items:
        return {"matched": 0, "updated": 0, "tickers": []}

    today = published_at.date() if published_at else date.today()
    updated_tickers: list[str] = []
    not_found: list[str] = []

    for item in items:
        ticker = item["ticker"]
        gross = item["gross_amount_per_share"]

        # Eşleşme stratejisi: ticker — TÜM statuslerden son kaydı al
        # (Eskiden sadece tarih_belli/odeniyor — ama ya hiç YKK girilmemişse?)
        # ±%5 gross match önce, yoksa en güncel kayıt.
        stmt = (
            select(DividendCalendar)
            .where(DividendCalendar.ticker == ticker)
            .order_by(DividendCalendar.created_at.desc())
            .limit(10)
        )
        rows = (await db.execute(stmt)).scalars().all()

        target = None
        if rows:
            # Önce ±%5 gross match
            for r in rows:
                if r.gross_amount_per_share and abs(r.gross_amount_per_share - gross) / max(gross, 1e-9) < 0.05:
                    target = r
                    break
            # Match yoksa en yeni kaydı al — AMA tamamlandi/reddedildi DEĞİL
            if target is None:
                for r in rows:
                    if r.status not in ("tamamlandi", "reddedildi"):
                        target = r
                        break

        if target:
            target.status = "tamamlandi"
            if not target.gross_amount_per_share:
                target.gross_amount_per_share = gross
            if not target.payment_date:
                target.payment_date = today
            if disclosure_id and not target.payment_kap_disclosure_id:
                target.payment_kap_disclosure_id = disclosure_id
            if kap_url and not target.payment_kap_url:
                target.payment_kap_url = kap_url
            updated_tickers.append(ticker)
            logger.info("DividendPayment: %s tamamlandi (gross=%s)", ticker, gross)
        else:
            # Hiç DividendCalendar kaydı yok — yeni satır oluştur
            new_row = DividendCalendar(
                ticker=ticker,
                gross_amount_per_share=gross,
                payment_date=today,
                payment_kap_disclosure_id=disclosure_id,
                payment_kap_url=kap_url,
                status="tamamlandi",
            )
            db.add(new_row)
            updated_tickers.append(ticker)
            logger.info("DividendPayment: %s YENİ tamamlandi kayıt (gross=%s)", ticker, gross)

    if updated_tickers:
        await db.flush()

    return {
        "matched": len(items),
        "updated": len(updated_tickers),
        "tickers": updated_tickers,
        "not_found": not_found,
    }


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
