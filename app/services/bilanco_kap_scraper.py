"""KAP Finansal Rapor Scraper — XBRL etiketleri uzerinden regex parse.

AI'ya gerek yok. KAP body'sinde her satir su patternde:
    XBRL_etiket|http://..label
    Turkce aciklama (opsiyonel)
    Dipnot referansi (opsiyonel sayi)
    Cari Donem rakami
    Onceki Donem rakami

XBRL etiketi sabit (ifrs-full_*, kap-fr_*) — etiket bulunduktan sonra
satir sonrasinda gelen ilk 2 sayi cari/onceki donem.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


# ── XBRL etiket eslesmeleri ──────────────────────────────────────────────────
# Etiket adina gore hangi DB alanina gidecegi
TAG_TO_FIELD: dict[str, str] = {
    # Bilanço (Finansal Durum Tablosu)
    "ifrs-full_Assets|": "total_assets",
    "ifrs-full_CurrentAssets|": "current_assets",
    "ifrs-full_NoncurrentAssets|": "non_current_assets",
    "ifrs-full_Equity|": "_total_equity_consolidated",  # NCI dahil — Fintables ana ortaklik kullanir
    "ifrs-full_EquityAttributableToOwnersOfParent|": "total_equity",  # Ana Ortaklığa Ait — TERCIH
    "ifrs-full_Liabilities|": "total_debt",
    "ifrs-full_CashAndCashEquivalents|": "cash_and_equivalents",
    # Net borc icin finansal borç bilesenleri (Fintables formulu):
    #   Net Borç = CurrentBorrowings + CurrentPortionOfNoncurrentBorrowings + LongtermBorrowings - Cash
    "kap-fr_CurrentBorowings|": "_current_borrowings",  # KAP'taki tag'de typo: tek "r"
    "kap-fr_CurrentBorrowings|": "_current_borrowings",  # ikili olasilik
    "kap-fr_CurrentPortionOfNoncurrentBorrowings|": "_current_portion_lt_borrowings",
    "ifrs-full_LongtermBorrowings|": "_longterm_borrowings",
    "ifrs-full_NoncurrentLoansReceived|": "_longterm_borrowings",  # bazi sirketler
    # Gelir Tablosu
    "ifrs-full_Revenue|": "revenue",
    "ifrs-full_GrossProfit|": "gross_profit",
    "ifrs-full_ProfitLossFromOperatingActivities|": "operating_profit",
    "ifrs-full_ProfitLoss|": "_net_income_consolidated",  # NCI dahil
    "ifrs-full_ProfitLossAttributableToOwnersOfParent|": "net_income",  # Ana Ortaklik — TERCIH
    # Amortisman (FAVOK = operating_profit + amortisman)
    "ifrs-full_AdjustmentsForDepreciationAndAmortisationExpense|": "_depreciation_amortization",
    "ifrs-full_DepreciationAndAmortisationExpense|": "_depreciation_amortization",
}

# Sayi formati — Turkce 1.234.567,89
_NUM_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})+|-?\d+,\d+|-?\d+")


def _parse_number(s: str) -> Optional[float]:
    """'506.840.805' → 506840805.0 / '12,34' → 12.34 / '0' → 0.0"""
    s = s.strip()
    if not s:
        return None
    # Yuzde isaretli olabilir
    s = s.replace("%", "").strip()
    # Türk formati: nokta = binlik ayraci, virgul = ondalik
    if "," in s and "." in s:
        # 1.234,56 → 1234.56
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # 12,34 → 12.34
        s = s.replace(",", ".")
    elif "." in s:
        # 1.234 (binlik) veya 1.5 (ondalik)?
        # Eger en sondaki gruptan sonra 3 hane varsa binlik ayraci
        parts = s.split(".")
        if all(len(p) == 3 for p in parts[1:]):
            s = "".join(parts)
        # else: tek ondalik nokta — olduğu gibi bırak
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _detect_period(body: str) -> Optional[str]:
    """Body'de 'Cari Dönem 01.01.2026 - 31.03.2026' kalibindan donem cikar."""
    # Once "Cari Donem" sonrasi tarih ara
    pat = re.compile(r"Cari\s+D[öo]nem\s+(\d{2})\.(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{2})\.(\d{4})", re.IGNORECASE)
    m = pat.search(body)
    end_month = end_year = None
    if m:
        end_month = int(m.group(5))
        end_year = int(m.group(6))
    else:
        # Bilanço sadece tek tarih: "Cari Dönem 31.03.2026"
        pat2 = re.compile(r"Cari\s+D[öo]nem\s+(\d{2})\.(\d{2})\.(\d{4})", re.IGNORECASE)
        m2 = pat2.search(body)
        if m2:
            end_month = int(m2.group(2))
            end_year = int(m2.group(3))

    if not end_month or not end_year:
        return None

    q = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}.get(end_month)
    if not q:
        return None
    return f"{end_year}-{q}"


_BIG_NUM_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})+(?:,\d+)?|-?\d{4,}(?:,\d+)?")
# Yalnizca binlik ayracli (1.234.567) veya 4+ haneli (1234) sayilar — XBRL URL'sindeki
# "2003" yil rakami eslesmesin diye buyuk_num pattern.

# Sonraki XBRL etiketi (yeni satir baslangici) — bu noktadan sonra ilerleme dur
_NEXT_TAG_RE = re.compile(r"\n(?:ifrs-full_|kap-fr_)\w+\|", re.IGNORECASE)


def _extract_value_after_tag(body: str, tag: str) -> Optional[float]:
    """XBRL etiketi gecen yerden sonra Cari Donem sayisini cikar.

    KAP Finansal Rapor body formati:
      tag|http://...role/totalLabel |  | TUR aciklama |  |  | TUR | EN |  |  |  | CARI | ONCEKI

    Cari Donem = pipe-ayrali son 2 sayidan ILKI (sondan ikinci sayi).
    Sayi formati: 353.794.589 (binlik ayraci) veya 353.794.589,50 (ondalik).

    XBRL URL'sindeki "2003" yil rakamlarini eslesmemek icin _BIG_NUM_RE kullanilir
    (en az 4 hane veya binlik ayraci).
    """
    idx = body.find(tag)
    if idx == -1:
        return None

    # Etiketten sonraki bolume bak (sonraki XBRL etiketine kadar)
    after = body[idx + len(tag):]
    next_tag = _NEXT_TAG_RE.search(after)
    chunk = after[:next_tag.start()] if next_tag else after[:1500]

    # Buyuk sayilari topla (URL'deki '2003' yil eslesmesin)
    big_nums = _BIG_NUM_RE.findall(chunk)

    valid = []
    for n in big_nums:
        v = _parse_number(n)
        if v is None:
            continue
        valid.append(v)

    if not valid:
        return None

    # KAP formati: ... | Cari | Onceki
    # Genellikle son 2 sayi cari/onceki donem. Cari = sondan 2.
    if len(valid) >= 2:
        return valid[-2]
    # Tek sayi varsa onu kullan
    return valid[-1]


def parse_kap_finansal_rapor(body: str) -> dict:
    """KAP Finansal Rapor body'sinden Cari Donem finansal verilerini cikar.

    Returns: dict — period + tum DB alanlari (eksikler None)
    """
    out: dict = {
        "period": None,
        "revenue": None,
        "gross_profit": None,
        "operating_profit": None,
        "net_income": None,
        "ebitda": None,  # KAP Finansal Rapor'da direkt yok, operating_profit fallback
        "total_assets": None,
        "current_assets": None,
        "non_current_assets": None,
        "total_equity": None,
        "total_debt": None,
        "net_debt": None,
        "cash_and_equivalents": None,
        "source": "kap_xbrl_scrape",
        "confidence": "low",
    }

    if not body or len(body) < 100:
        return out

    out["period"] = _detect_period(body)

    # Sunum Para Birimi carpani — "1.000 TL" / "1.000.000 TL" / "TL"
    # FROTO, BRISA gibi sirketler degerlerini binlik olarak sunar.
    multiplier = 1
    pb = re.search(r"Sunum\s+Para\s+Birimi\s*\|\s*([0-9.,]+)?\s*TL", body, re.IGNORECASE)
    if pb and pb.group(1):
        raw = pb.group(1).replace(".", "").replace(",", "").strip()
        try:
            n = int(raw)
            if n in (1000, 1000000):
                multiplier = n
        except ValueError:
            pass

    # XBRL etiketlerini tek tek çek
    # Onceliklendirme: tercih edilen alan (parent equity, parent net income) bulunmazsa
    # _consolidated fallback'i kullanilir.
    aux: dict = {}
    for tag, field in TAG_TO_FIELD.items():
        v = _extract_value_after_tag(body, tag)
        if v is None:
            continue
        v_scaled = v * multiplier
        if field.startswith("_"):
            # Yardimci alanlar (NCI dahil consolidated, borclanmalar, amortisman)
            aux[field] = v_scaled
        else:
            # Tercih edilen alan — sadece henuz dolu degilse yaz (parent over consolidated)
            if out.get(field) is None:
                out[field] = v_scaled

    # Parent equity yoksa consolidated'i kullan
    if out["total_equity"] is None and aux.get("_total_equity_consolidated") is not None:
        out["total_equity"] = aux["_total_equity_consolidated"]
    # Parent net income yoksa consolidated kullan
    if out["net_income"] is None and aux.get("_net_income_consolidated") is not None:
        out["net_income"] = aux["_net_income_consolidated"]

    # Net Borç = Finansal Borçlar - Nakit (Fintables formulu)
    fin_debt = sum(
        v for v in (
            aux.get("_current_borrowings"),
            aux.get("_current_portion_lt_borrowings"),
            aux.get("_longterm_borrowings"),
        ) if v is not None
    )
    if fin_debt > 0 and out["cash_and_equivalents"] is not None:
        out["net_debt"] = fin_debt - out["cash_and_equivalents"]
    elif out["total_debt"] is not None and out["cash_and_equivalents"] is not None:
        # Fallback: finansal borc tagi yoksa toplam yukumluluk - nakit
        out["net_debt"] = out["total_debt"] - out["cash_and_equivalents"]

    # FAVOK = Operating Profit + Amortisman (eger amortisman varsa)
    dep = aux.get("_depreciation_amortization")
    if out["operating_profit"] is not None:
        if dep is not None:
            out["ebitda"] = out["operating_profit"] + abs(dep)  # amortisman pozitif eklenir
        else:
            out["ebitda"] = out["operating_profit"]

    # Confidence — kac alan dolduruldu?
    filled = sum(1 for k in ("revenue", "net_income", "total_assets", "total_equity") if out.get(k) is not None)
    if filled >= 3:
        out["confidence"] = "high"
    elif filled >= 1:
        out["confidence"] = "medium"

    return out
