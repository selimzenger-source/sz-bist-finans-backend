"""Pay Alım Satım Text Parser.

Kullanim:
    Ucretsizderinlikbot MiniApp'inden veya KAP'tan kopyalanan ham metni
    yapilandirilmis ShareTransactionDetail kayitlarina cevirir.

Format:
    TICKER
    SIRKET ADI (opsiyonel)
    DD.MM.YYYY
    Alici|Satici
    Taraf adi (kisi veya sirket)
    [Görev: ...] (opsiyonel)
    [Fiyat: 15,60 - 15,63 TL veya 15,60 TL]
    [Nominal: X.XXX.XXX Lot]
    Oy Hakki
    %X.XX
    +X.XX% veya -X.XX%
    Pay Orani
    %X.XX
    +X.XX%

Iki kayit arasi bos satir veya ticker satiri.
"""

from __future__ import annotations

import logging
import re
from datetime import date
from typing import Iterator, Optional

from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


# ─── Regex'ler ───
_DATE_RE = re.compile(r"^([0-3]?\d)\.([01]?\d)\.(20\d{2})$")
_PCT_RE = re.compile(r"^%(-?\d+(?:[.,]\d+)?)$")
_PCT_CHANGE_RE = re.compile(r"^([+-]?\d+(?:[.,]\d+)?)%$")
_PRICE_RE = re.compile(r"(\d+(?:[.,]\d+)?)(?:\s*-\s*(\d+(?:[.,]\d+)?))?\s*TL", re.IGNORECASE)
_LOT_RE = re.compile(r"([\d.]+)\s*Lot", re.IGNORECASE)
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{2,5}$")  # 3-6 buyuk harf/rakam
_TYPE_RE = re.compile(r"^(Al[ıi]c[ıi]|Sat[ıi]c[ıi])$", re.IGNORECASE)


def _parse_tr_number(s: str) -> Optional[float]:
    """Turkce sayi: "1.978.375"→1978375, "15,60"→15.60, "95.000"→95000, "3.5"→3.5
    Tek nokta sonrasi 3 hane ise binlik, degilse ondalik. ("95.000"→95.0 bug fix)
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
    """500.000 Lot -> 500000"""
    m = _LOT_RE.search(s or "")
    if not m:
        return None
    cleaned = m.group(1).replace(".", "").replace(",", "")
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_price_range(s: str) -> tuple[Optional[float], Optional[float]]:
    """Fiyat: '15,60 - 15,63 TL' veya '15,60 TL' -> (low, high) ya da (low, None)"""
    m = _PRICE_RE.search(s or "")
    if not m:
        return (None, None)
    low = _parse_tr_number(m.group(1))
    high = _parse_tr_number(m.group(2)) if m.group(2) else None
    return (low, high)


def _parse_date(s: str) -> Optional[date]:
    m = _DATE_RE.match(s.strip())
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except (ValueError, TypeError):
        return None


# ─── Parser ───

def parse_records(raw_text: str) -> list[dict]:
    """Ham metinden ShareTransactionDetail kayit listesi cikarir.

    Returns:
        list of dicts (DB'ye yazilabilir alanlar)
    """
    if not raw_text:
        return []

    # Satirlara bol
    lines = [ln.strip() for ln in raw_text.splitlines() if ln.strip()]
    records: list[dict] = []
    i = 0
    n = len(lines)

    while i < n:
        # Bir kayit ticker satiri ile baslar (3-6 buyuk harf)
        if not _TICKER_RE.match(lines[i]):
            i += 1
            continue

        ticker = lines[i]
        i += 1

        # Sirket adi — bir sonraki satirda buyuk harfle yazilmis cumledir,
        # ama tarih de olabilir. Tarih kontrolu yap.
        company_name = None
        if i < n and not _DATE_RE.match(lines[i]) and not _TICKER_RE.match(lines[i]):
            company_name = lines[i]
            i += 1

        # Tarih
        if i >= n:
            break
        transaction_date = _parse_date(lines[i])
        if not transaction_date:
            # Format bozuk, ticker arar gibi devam et
            continue
        i += 1

        # Islem tipi: Alıcı | Satıcı
        if i >= n:
            break
        type_match = _TYPE_RE.match(lines[i])
        if not type_match:
            continue
        ttype_raw = lower_tr(type_match.group(1))
        transaction_type = "alici" if "al" in ttype_raw else "satici"
        i += 1

        # Taraf adi
        if i >= n:
            break
        party_name = lines[i]
        i += 1

        # Opsiyonel: Görev
        party_role = None
        if i < n and lower_tr(lines[i]) == "görev":
            i += 1
            if i < n:
                party_role = lines[i]
                i += 1

        # Opsiyonel: Fiyat
        price_low: Optional[float] = None
        price_high: Optional[float] = None
        if i < n and lower_tr(lines[i]) == "fiyat":
            i += 1
            if i < n:
                price_low, price_high = _parse_price_range(lines[i])
                i += 1

        # Opsiyonel: Nominal
        nominal_lot: Optional[int] = None
        if i < n and lower_tr(lines[i]) == "nominal":
            i += 1
            if i < n:
                nominal_lot = _parse_int_lot(lines[i])
                i += 1

        # Oy Hakki
        oy_hakki_pct: Optional[float] = None
        oy_hakki_change_pct: Optional[float] = None
        if i < n and lower_tr(lines[i]).startswith("oy hakk"):
            i += 1
            if i < n:
                m = _PCT_RE.match(lines[i].replace(" ", ""))
                if m:
                    oy_hakki_pct = _parse_tr_number(m.group(1))
                    i += 1
            if i < n:
                m = _PCT_CHANGE_RE.match(lines[i].replace(" ", ""))
                if m:
                    oy_hakki_change_pct = _parse_tr_number(m.group(1))
                    i += 1

        # Pay Orani
        pay_orani_pct: Optional[float] = None
        pay_orani_change_pct: Optional[float] = None
        if i < n and lower_tr(lines[i]).startswith("pay oran"):
            i += 1
            if i < n:
                m = _PCT_RE.match(lines[i].replace(" ", ""))
                if m:
                    pay_orani_pct = _parse_tr_number(m.group(1))
                    i += 1
            if i < n:
                m = _PCT_CHANGE_RE.match(lines[i].replace(" ", ""))
                if m:
                    pay_orani_change_pct = _parse_tr_number(m.group(1))
                    i += 1

        records.append({
            "ticker": ticker,
            "company_name": company_name,
            "transaction_date": transaction_date,
            "transaction_type": transaction_type,
            "party_name": party_name,
            "party_role": party_role,
            "price_low": price_low,
            "price_high": price_high,
            "nominal_lot": nominal_lot,
            "oy_hakki_pct": oy_hakki_pct,
            "oy_hakki_change_pct": oy_hakki_change_pct,
            "pay_orani_pct": pay_orani_pct,
            "pay_orani_change_pct": pay_orani_change_pct,
        })

    return records


def make_dedup_key(rec: dict) -> tuple:
    """Tekrar gelen ayni kaydi tespit etmek icin anahtar."""
    return (
        rec["ticker"],
        rec["transaction_date"].isoformat() if rec.get("transaction_date") else "",
        rec["transaction_type"],
        rec["party_name"],
        rec.get("nominal_lot") or 0,
    )
