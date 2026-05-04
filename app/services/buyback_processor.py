"""KAP Pay Geri Alımı (Buyback) Processor.

KAP "Payların Geri Alınmasına İlişkin Bildirim" tipi:
  - Şirket kendi paylarını borsadan geri alır
  - Body örneği:
    "04.05.2026 (bugün) tarihinde Borsa İstanbul A.Ş. nezdinde pay başına
     26,90 TL – 27,00 TL fiyat aralığından (ağırlıklı ortalama 26,988 TL)
     toplam 227.291 adet pay geri alınmıştır."
  - Tablo: tarihler + nominal lot + sermayeye oran + işlem fiyatı

Hedef: share_transaction_details tablosuna kaydet — party_name="Şirket Geri Alımı",
       transaction_type="geri_alim", lot/fiyat/oran dolu.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


_BUYBACK_TITLE_PATTERNS = [
    "payların geri alın", "paylarn geri alin",
    "pay geri alım", "pay geri alim",
    "geri alım programı", "geri alim programi",
    "geri alım işlem", "geri alim islem",
]


def is_buyback(title: str) -> bool:
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _BUYBACK_TITLE_PATTERNS)


# Pattern: "DD.MM.YYYY (bugün) tarihinde ... pay başına X TL – Y TL ... ağırlıklı ortalama Z TL ... toplam N adet pay geri alın"
_TODAY_RE = re.compile(
    r"(\d{1,2}\.\d{1,2}\.\d{4})\s*(?:\([^)]*\))?\s*tarihinde[^.]*?"
    r"pay\s+başına\s+([\d.,]+)\s*TL\s*[–-]\s*([\d.,]+)\s*TL[^.]*?"
    r"(?:ağırlıklı\s+ortalama\s+([\d.,]+)\s*TL[^.]*?)?"
    r"toplam\s+([\d.,]+)\s*adet\s+pay\s+geri\s+al",
    re.IGNORECASE | re.DOTALL,
)

# Tablo satır pattern: "B Grubu, AHGAZ, TREAHLA00019 04.05.2026 227.291 0,00874 26,988"
# RSC body'de pipe ayraçlı: "B Grubu, AHGAZ | 04.05.2026 | 227.291 | 0,00874 | 26,988"
_ROW_RE = re.compile(
    r"([A-Z]{2,6})\s*,?\s*[A-Z]*\s*\|?\s*(\d{1,2}\.\d{1,2}\.\d{4})\s*\|?\s*"
    r"([\d.]+(?:,\d+)?)\s*\|?\s*([\d.,]+)\s*\|?\s*([\d.,]+)",
)


def _parse_tr_num(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(" ", "")
    if "," in s:
        int_part, dec_part = s.rsplit(",", 1)
        int_part = int_part.replace(".", "")
        try:
            return float(f"{int_part}.{dec_part}")
        except ValueError:
            return None
    s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_dt(s: str) -> Optional[date]:
    for fmt in ("%d.%m.%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_buyback_today(body: str) -> Optional[dict]:
    """Body'den BUGÜNKÜ geri alım işlemini çıkar.

    Returns: {transaction_date, lot, price_low, price_high, price_avg}
    """
    if not body:
        return None

    m = _TODAY_RE.search(body)
    if not m:
        return None

    return {
        "transaction_date": _parse_dt(m.group(1)),
        "price_low": _parse_tr_num(m.group(2)),
        "price_high": _parse_tr_num(m.group(3)),
        "price_avg": _parse_tr_num(m.group(4)) if m.group(4) else None,
        "lot": int(_parse_tr_num(m.group(5)) or 0) or None,
    }


async def process_buyback(
    db: AsyncSession,
    *,
    ticker: str,
    body: str,
    kap_url: Optional[str],
    disclosure_id: Optional[int],
    published_at: Optional[datetime] = None,
) -> bool:
    """KAP buyback bildirimini share_transaction_details'a kaydet.

    Sadece BUGÜNKÜ işlemi yazar (tabloda eski tarihler varsa onları atla).
    """
    parsed = parse_buyback_today(body or "")
    if not parsed or not parsed.get("transaction_date"):
        logger.info("Buyback parse fail: %s", ticker)
        return False

    tx_date = parsed["transaction_date"]
    lot = parsed.get("lot")
    price_low = parsed.get("price_low")
    price_high = parsed.get("price_high")
    price_avg = parsed.get("price_avg")
    party = "Şirket Geri Alımı"

    # UPSERT — ticker + tx_date + party kombinasyonu
    check = await db.execute(sa_text("""
        SELECT id FROM share_transaction_details
        WHERE ticker=:tk AND transaction_date=:dt AND party_name=:pn
        LIMIT 1
    """), {"tk": ticker.upper(), "dt": tx_date, "pn": party})
    existing_id = check.scalar()

    payload = {
        "tk": ticker.upper(),
        "dt": tx_date,
        "tt": "geri_alim",
        "pn": party,
        "lot": lot,
        "pl": price_low,
        "ph": price_high,
        "kap": kap_url,
        "did": disclosure_id,
        "raw": (body or "")[:1000],
    }

    if existing_id:
        await db.execute(sa_text("""
            UPDATE share_transaction_details
            SET transaction_type=:tt, nominal_lot=:lot,
                price_low=:pl, price_high=:ph,
                kap_url=:kap, kap_disclosure_id=COALESCE(:did, kap_disclosure_id),
                source='kap_buyback', raw_excerpt=:raw
            WHERE id=:id
        """), {**payload, "id": existing_id})
    else:
        await db.execute(sa_text("""
            INSERT INTO share_transaction_details(
                ticker, transaction_date, transaction_type, party_name,
                nominal_lot, price_low, price_high,
                kap_url, kap_disclosure_id, source, raw_excerpt, created_at
            ) VALUES(:tk, :dt, :tt, :pn, :lot, :pl, :ph, :kap, :did, 'kap_buyback', :raw, NOW())
        """), payload)

    logger.info("Buyback islendi: %s %s lot=%s avg=%s", ticker, tx_date, lot, price_avg)
    return True
