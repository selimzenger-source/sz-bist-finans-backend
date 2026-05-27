"""Toptan + Tip Dönüşüm + Tedbirli için text parser'lar.

Ucretsizderinlikbot'tan kopyalanan ham metni yapilandirilmis kayitlara cevirir.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Optional

from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)

# ─── Common helpers ───
_DATE_RE = re.compile(r"^([0-3]?\d)\.([01]?\d)\.(20\d{2})$")
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{2,5}$")
_LOT_RE = re.compile(r"([\d.]+)\s*Lot", re.IGNORECASE)
_TL_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*TL", re.IGNORECASE)
_PRICE_TL_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*₺")
_PCT_RE = re.compile(r"^([+-]?\d+(?:[.,]\d+)?)%?$")

_TR_MONTHS = {
    "oca": 1, "şub": 2, "sub": 2, "mar": 3, "nis": 4, "may": 5, "haz": 6,
    "tem": 7, "agu": 8, "ağu": 8, "eyl": 9, "eki": 10, "kas": 11, "ara": 12,
}


def _parse_tr_number(s: str) -> Optional[float]:
    """Türkçe sayı: "12.100.000,50"→12100000.5, "95.000"→95000, "3.5"→3.5
    KRITIK: tek nokta sonrasi 3 hane ise binlik ayraç, degilse ondalık.
    Onceki bug: "95.000" -> 95.0 idi (1000x kucuk).
    """
    if not s:
        return None
    s = s.strip().replace(" ", "")
    if "," in s:
        int_part, dec_part = s.rsplit(",", 1)
        int_part = int_part.replace(".", "")
        try:
            return float(f"{int_part}.{dec_part}")
        except (ValueError, TypeError):
            return None
    if "." in s:
        parts = s.split(".")
        if len(parts) >= 2 and all(len(p) == 3 and p.isdigit() for p in parts[1:]):
            try:
                return float(s.replace(".", ""))
            except (ValueError, TypeError):
                return None
        try:
            return float(s)
        except (ValueError, TypeError):
            return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _parse_int_lot(s: str) -> Optional[int]:
    m = _LOT_RE.search(s or "")
    if not m:
        return None
    cleaned = m.group(1).replace(".", "").replace(",", "")
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_date(s: str) -> Optional[date]:
    m = _DATE_RE.match(s.strip())
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except (ValueError, TypeError):
        return None


def _parse_tr_short_date(s: str, default_year: int) -> Optional[date]:
    """'06 Mar' -> date(default_year, 3, 6)"""
    parts = s.strip().split()
    if len(parts) != 2:
        return None
    try:
        day = int(parts[0])
        month = _TR_MONTHS.get(lower_tr(parts[1])[:3])
        if not month:
            return None
        return date(default_year, month, day)
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════
# 1. TIP DONUSUM (Borsada İşlem Gören Tipe Dönüşüm)
# ═══════════════════════════════════════════════════════════════════

def parse_type_conversions(raw_text: str) -> list[dict]:
    """Format:
    TICKER
    COMPANY
    DD.MM.YYYY
    Yatırımcı
    INVESTOR_NAME
    Dönüştürülen Lot
    X.XXX Lot
    """
    if not raw_text:
        return []

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    records: list[dict] = []
    i, n = 0, len(lines)

    while i < n:
        if not _TICKER_RE.match(lines[i]):
            i += 1
            continue

        ticker = lines[i]
        i += 1

        company_name = None
        if i < n and not _DATE_RE.match(lines[i]) and not _TICKER_RE.match(lines[i]):
            company_name = lines[i]
            i += 1

        if i >= n: break
        d = _parse_date(lines[i])
        if not d:
            continue
        i += 1

        # Yatırımcı satırı
        if i < n and lower_tr(lines[i]).startswith("yatırımcı"):
            i += 1

        if i >= n: break
        investor_name = lines[i]
        i += 1

        # Dönüştürülen Lot satırı
        if i < n and lower_tr(lines[i]).startswith("dönüştürülen"):
            i += 1

        converted_lot: Optional[int] = None
        if i < n:
            converted_lot = _parse_int_lot(lines[i])
            if converted_lot:
                i += 1

        records.append({
            "ticker": ticker,
            "company_name": company_name,
            "transaction_date": d,
            "investor_name": investor_name,
            "converted_lot": converted_lot,
        })

    return records


# ═══════════════════════════════════════════════════════════════════
# 2. TOPTAN ALIM SATIM
# ═══════════════════════════════════════════════════════════════════

def parse_block_trades(raw_text: str) -> list[dict]:
    """Format:
    TICKER
    COMPANY
    DD.MM.YYYY
    İşlem Tipi
    Satış|Alış
    Aracı Kurum
    BROKER
    Alıcılar (or Satıcılar)
    PARTIES (uzun olabilir, virgülle ayrılı, bazen tek satırda)
    Lot Miktarı
    X.XXX.XXX Lot
    Maliyet Fiyatı
    XX,XX TL
    """
    if not raw_text:
        return []

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    records: list[dict] = []
    i, n = 0, len(lines)

    while i < n:
        if not _TICKER_RE.match(lines[i]):
            i += 1
            continue

        ticker = lines[i]
        i += 1

        company_name = None
        if i < n and not _DATE_RE.match(lines[i]) and not _TICKER_RE.match(lines[i]):
            company_name = lines[i]
            i += 1

        if i >= n: break
        d = _parse_date(lines[i])
        if not d:
            continue
        i += 1

        # İşlem Tipi label
        if i < n and lower_tr(lines[i]).startswith("işlem tipi"):
            i += 1

        if i >= n: break
        ttype_raw = lower_tr(lines[i])
        transaction_type = "alis" if "al" in ttype_raw else "satis"
        i += 1

        # Aracı Kurum label
        broker = None
        if i < n and lower_tr(lines[i]).startswith("aracı kurum"):
            i += 1
            if i < n:
                broker = lines[i]
                i += 1

        # Alıcılar / Satıcılar label + uzun parties metni
        counterparties = None
        if i < n and (lower_tr(lines[i]).startswith("alıcılar") or lower_tr(lines[i]).startswith("satıcılar")):
            i += 1
            # Parties'in tek satır olduğunu varsay (genelde virgülle çok uzun)
            if i < n:
                counterparties = lines[i]
                i += 1

        # Lot Miktarı label
        lot_amount: Optional[int] = None
        if i < n and lower_tr(lines[i]).startswith("lot miktarı"):
            i += 1
            if i < n:
                lot_amount = _parse_int_lot(lines[i])
                if lot_amount:
                    i += 1

        # Maliyet Fiyatı label
        cost_price: Optional[float] = None
        if i < n and lower_tr(lines[i]).startswith("maliyet fiyatı"):
            i += 1
            if i < n:
                m = _TL_RE.search(lines[i])
                if m:
                    cost_price = _parse_tr_number(m.group(1))
                    i += 1

        records.append({
            "ticker": ticker,
            "company_name": company_name,
            "transaction_date": d,
            "transaction_type": transaction_type,
            "broker": broker,
            "counterparties": counterparties,
            "lot_amount": lot_amount,
            "cost_price": cost_price,
        })

    return records


# ═══════════════════════════════════════════════════════════════════
# 3. TEDBIRLI HISSELER
# ═══════════════════════════════════════════════════════════════════

# Bilinen tag'ler
_KNOWN_TAGS = {"KRD", "AÇS", "ACS", "BRT", "EMR", "PEM", "VEY", "TEK", "EPT", "IEY"}


def parse_cautious_stocks(raw_text: str, default_year: int = None) -> list[dict]:
    """Format:
    TICKER
    COMPANY
    PRICE ₺
    +X.XX% (veya -X.XX%, 0.18%)
    DD MMM → DD MMM
    TAG1
    [TAG2]
    [TAG3]
    """
    if not raw_text:
        return []

    if default_year is None:
        default_year = date.today().year

    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    records: list[dict] = []
    i, n = 0, len(lines)

    while i < n:
        if not _TICKER_RE.match(lines[i]):
            i += 1
            continue

        ticker = lines[i]
        i += 1

        # Company name
        company_name = None
        if i < n and not _PRICE_TL_RE.search(lines[i]) and not _TICKER_RE.match(lines[i]):
            company_name = lines[i]
            i += 1

        # Price ₺
        last_price = None
        if i < n:
            m = _PRICE_TL_RE.search(lines[i])
            if m:
                last_price = _parse_tr_number(m.group(1))
                i += 1

        # Pct change
        pct_change = None
        if i < n:
            ml = lines[i].replace("%", "").replace(" ", "")
            m = _PCT_RE.match(ml)
            if m:
                pct_change = _parse_tr_number(m.group(1))
                i += 1

        # Date range "06 Mar → 05 May"
        start_date = end_date = None
        if i < n and "→" in lines[i]:
            parts = [p.strip() for p in lines[i].split("→")]
            if len(parts) == 2:
                start_date = _parse_tr_short_date(parts[0], default_year)
                end_date = _parse_tr_short_date(parts[1], default_year)
                # Yıl atlaması — start > end ise end bir sonraki yıl
                if start_date and end_date and end_date < start_date:
                    end_date = date(default_year + 1, end_date.month, end_date.day)
            i += 1

        # Tags (1-3 tane art arda gelebilir)
        tags: list[str] = []
        while i < n:
            tag_candidate = lines[i].upper().strip()
            if tag_candidate in _KNOWN_TAGS:
                tags.append(tag_candidate.replace("Ç", "C"))
                i += 1
            else:
                break

        today = date.today()
        is_active = bool(end_date and end_date >= today)

        records.append({
            "ticker": ticker,
            "company_name": company_name,
            "last_price": last_price,
            "pct_change": pct_change,
            "start_date": start_date,
            "end_date": end_date,
            "tags": ",".join(tags) if tags else None,
            "is_active": is_active,
        })

    return records
