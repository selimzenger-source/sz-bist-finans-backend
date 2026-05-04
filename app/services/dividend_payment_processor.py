"""Temettu Odeme Gerceklesti Processor — KAP "Hak Kullanimi" / "Pay Piyasasi"
bildirimlerinde, ilgili dividend_calendar kaydinin status'unu 'tamamlandi' yapar.

Format ornegi (KAP body'sinden):
  ALARK.E Pay Basina Brut Temettu: 3,185 TL Teorik Fiyat: 92,465 TL
  EGGUB.E Pay Basina Brut Temettu: 2,5 TL Teorik Fiyat: 124,3 TL
  KFEIN.E Pay Basina Brut Temettu: 0,0202531 TL Teorik Fiyat: 8,69 TL

AI'a gerek yok — regex parse yeterli.

State machine notu: dividend_calendar.status enum'i: ykk_alindi |
genel_kurul_onayli | tarih_belli | odeniyor | tamamlandi | reddedildi.
"Odendi" enum'a yok — odeme gerceklesti = "tamamlandi".
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.dividend_calendar import DividendCalendar
from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


# Title pattern siniflandirici — bu processor sadece "ödeme/hak kullanim
# GERCEKLESTI" KAP bildirimlerine bakar (MKK / Pay Piyasasi tarafindan).
_TITLE_PATTERNS = [
    "hak kullanımı", "hak kullanim",
    "pay piyasası alım satım", "pay piyasasi alim satim",
    "merkezi kayıt kuruluşu", "merkezi kayit kurulusu",
    "temettü ödeme", "temettu odeme",
    "nakit kar payı ödeme", "nakit kar payi odeme",
]


def is_dividend_payment(title: str) -> bool:
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _TITLE_PATTERNS)


def _parse_tr_decimal(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.replace(" TL", "").replace("%", "").strip()
    # Turk formati: nokta = binlik, virgul = ondalik
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


# Pattern: "ALARK.E Pay Basina Brut Temettu: 3,185 TL Teorik Fiyat: 92,465 TL"
# Ticker kismi: 3-6 buyuk harf, opsiyonel ".E"/".K" suffix
_PAYMENT_RE = re.compile(
    r"([A-ZÇĞİÖŞÜ]{2,6})(?:\.[A-Z])?\s+"
    r"Pay\s+Ba[şs]ına?\s+Br[üu]t\s+Temett[üu]\s*[:=]?\s*"
    r"([\d.,]+)\s*TL"
    r".*?Teorik\s+Fiyat\s*[:=]?\s*([\d.,]+)\s*TL",
    re.IGNORECASE | re.DOTALL,
)


def parse_dividend_payments(body: str) -> list[dict]:
    """Body icinden tum (ticker, brut_temettu, teorik_fiyat) ucluleri cikar.

    Returns: [{"ticker": "ALARK", "gross_amount": 3.185, "theoretical_price": 92.465}, ...]
    """
    if not body:
        return []
    out = []
    seen = set()
    for m in _PAYMENT_RE.finditer(body):
        ticker = m.group(1).upper()
        if ticker in seen:
            continue  # ayni body icinde TR/EN duplikat var
        gross = _parse_tr_decimal(m.group(2))
        theo = _parse_tr_decimal(m.group(3))
        if gross is None or gross <= 0:
            continue
        seen.add(ticker)
        out.append({
            "ticker": ticker,
            "gross_amount": gross,
            "theoretical_price": theo,
        })
    return out


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
    dry_run: bool = False,
) -> Optional[list[dict]]:
    """KAP odeme bildiriminden ilgili dividend_calendar kayitlarini 'tamamlandi'
    yap. AI YOK — regex parse.

    Returns: islenen ticker listesi (dry_run=True ise sadece parse cikti) veya None.
    """
    if not is_dividend_payment(title):
        return None

    # Body kismini RSC extractor'dan al (bos gelirse)
    if (not body or len(body) < 100) and kap_url:
        try:
            extracted = await fetch_kap_disclosure(kap_url)
            if extracted:
                body = extracted.get("full_text") or body
        except Exception as e:
            logger.warning("DivPayment body fetch hata (%s): %s", kap_url, e)

    payments = parse_dividend_payments(body or "")
    if not payments:
        logger.info("DivPayment: parse'tan ticker cikmadi (%s)", kap_url)
        return None

    logger.info("DivPayment: %d ticker bulundu — %s", len(payments),
                [p["ticker"] for p in payments])

    if dry_run:
        return payments

    today = date.today()
    pay_dt = published_at.date() if published_at else today

    updated = []
    for p in payments:
        tk = p["ticker"]
        gross = p["gross_amount"]

        # Mevcut kaydi bul: ayni ticker + brut temettu (~%5 tolerans)
        # ve odeniyor / tarih_belli / genel_kurul_onayli statuslerden
        stmt = (
            select(DividendCalendar)
            .where(DividendCalendar.ticker == tk)
            .where(DividendCalendar.status.in_([
                "tarih_belli", "odeniyor", "genel_kurul_onayli", "ykk_alindi",
            ]))
            .order_by(DividendCalendar.created_at.desc())
        )
        rows = (await db.execute(stmt)).scalars().all()

        target = None
        for row in rows:
            db_gross = row.gross_amount_per_share
            if db_gross and gross and abs(float(db_gross) - gross) / max(gross, 0.0001) < 0.05:
                target = row
                break
        # Tutar eslesmesi yoksa, en son aktif kaydi al
        if not target and rows:
            target = rows[0]

        if not target:
            # Hicbir mevcut yok — yeni satir olustur (state machine atla)
            target = DividendCalendar(
                ticker=tk,
                company_name=None,
                gross_amount_per_share=gross,
                payment_date=pay_dt,
                payment_kap_disclosure_id=disclosure_id,
                payment_kap_url=kap_url,
                status="tamamlandi",
            )
            db.add(target)
            await db.flush()
            logger.info("DivPayment: yeni 'tamamlandi' kaydi (%s, %s TL)", tk, gross)
        else:
            target.payment_date = target.payment_date or pay_dt
            target.payment_kap_disclosure_id = (
                target.payment_kap_disclosure_id or disclosure_id
            )
            target.payment_kap_url = target.payment_kap_url or kap_url
            target.status = "tamamlandi"
            if not target.gross_amount_per_share and gross:
                target.gross_amount_per_share = gross
            logger.info("DivPayment: tamamlandi (%s, %s TL)", tk, gross)
        updated.append({"ticker": tk, "gross": gross, "id": target.id})

    return updated
