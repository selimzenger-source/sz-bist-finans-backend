"""KAP Finansal Rapor Scraper v2 — Sektör-aware, doğru parse.

DÜZELTMELER (v1 → v2):
1. `_NEXT_TAG_RE` artık `\n` aramıyor — KAP RSC body tek satır olduğu için doğru
2. Chunk içinden SON 2 sayı alınıyor (Cari + Önceki) — eskiden ilk sayı alınıyordu
   ve URL'deki yıl numaraları (2003, 2015) yanlışlıkla yakalanıyordu
3. Sektör tespiti: banka/sigorta için farklı XBRL etiketleri
4. Birim çarpan: "Sunum Para Birimi | 1.000 TL" varsa ×1000

Test: KLGYO 2026-Q1 → 12/12 alan birebir xlsx ile eşleşti.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ─── Sektör tipi ───────────────────────────────────────────────────────────────
SECTOR_INDUSTRIAL = "industrial"  # Sanayi/ticaret/GYO/holding/aracı kurum
SECTOR_BANK = "bank"
SECTOR_INSURANCE = "insurance"


# ─── XBRL Etiket → DB Alan Eşleşmeleri ──────────────────────────────────────────

# SANAYI / GYO / HOLDING (industrial)
INDUSTRIAL_TAG_TO_FIELD: dict[str, str] = {
    # Bilanço (Finansal Durum Tablosu)
    "ifrs-full_Assets|": "total_assets",
    "ifrs-full_CurrentAssets|": "current_assets",
    "ifrs-full_NoncurrentAssets|": "non_current_assets",
    "ifrs-full_Equity|": "_total_equity_consolidated",
    "ifrs-full_EquityAttributableToOwnersOfParent|": "total_equity",
    "ifrs-full_Liabilities|": "total_debt",  # Toplam yükümlülük (financial debt değil)
    "ifrs-full_CashAndCashEquivalents|": "cash_and_equivalents",
    # Finansal borç (Net debt hesabı için)
    "kap-fr_CurrentBorowings|": "_current_borrowings",
    "kap-fr_CurrentBorrowings|": "_current_borrowings",
    "kap-fr_CurrentPortionOfNoncurrentBorrowings|": "_current_portion_lt_borrowings",
    "ifrs-full_LongtermBorrowings|": "_longterm_borrowings",
    "ifrs-full_NoncurrentLoansReceived|": "_longterm_borrowings",
    # Gelir Tablosu
    "ifrs-full_Revenue|": "revenue",
    "ifrs-full_GrossProfit|": "gross_profit",
    "ifrs-full_ProfitLossFromOperatingActivities|": "operating_profit",
    "ifrs-full_ProfitLoss|": "_net_income_consolidated",
    "ifrs-full_ProfitLossAttributableToOwnersOfParent|": "net_income",
    # SG&A — gerçek EBITDA hesabı için (KAP'ta gerçek etiket isimleri)
    "ifrs-full_AdministrativeExpense|": "_sga_general",
    "ifrs-full_GeneralAndAdministrativeExpense|": "_sga_general",  # alternatif
    "kap-fr_MarketingExpense|": "_sga_marketing",
    "ifrs-full_DistributionCosts|": "_sga_marketing",  # alternatif
    "kap-fr_ResearchAndDevelopmentExpenses|": "_sga_rd",
    "ifrs-full_ResearchAndDevelopmentExpense|": "_sga_rd",
    # Amortisman
    "ifrs-full_AdjustmentsForDepreciationAndAmortisationExpense|": "_depreciation_amortization",
    "ifrs-full_DepreciationAndAmortisationExpense|": "_depreciation_amortization",
}

# BANKA (BDDK formatı) — XBRL etiketleri farklı
BANK_TAG_TO_FIELD: dict[str, str] = {
    # Bilanço
    "ifrs-full_Assets|": "total_assets",  # "VARLIKLAR TOPLAMI"
    "ifrs-full_Equity|": "total_equity",
    "ifrs-full_Liabilities|": "total_debt",
    "ifrs-full_CashAndCashEquivalents|": "cash_and_equivalents",
    # Banka spesifik
    "kap-fr_Loans|": "loans",
    "kap-fr_Deposits|": "deposits",
    # Gelir Tablosu (Banka)
    "kap-fr_NetInterestIncomeExpense|": "net_interest_income",
    "kap-fr_NetFeesAndCommissionsIncomeExpense|": "net_fees_commissions",
    "kap-fr_OperatingGrossProfitLoss|": "gross_profit",
    "kap-fr_NetOperatingProfitLoss|": "operating_profit",
    "ifrs-full_ProfitLoss|": "net_income",
}

# SIGORTA — XBRL etiketleri farklı
INSURANCE_TAG_TO_FIELD: dict[str, str] = {
    # Bilanço
    "ifrs-full_Assets|": "total_assets",
    "ifrs-full_Equity|": "total_equity",
    "ifrs-full_Liabilities|": "total_debt",
    "ifrs-full_CashAndCashEquivalents|": "cash_and_equivalents",
    # Sigorta spesifik (varsa)
    "kap-fr_GrossWrittenPremiums|": "gross_premiums",
    "kap-fr_TechnicalBalance|": "technical_balance",
    "ifrs-full_ProfitLoss|": "net_income",
}


# ─── Regex'ler ────────────────────────────────────────────────────────────────

# Türkçe sayı: -1.234.567,89  /  -1.234.567  /  -1234,56
_NUM_RE = re.compile(r"-?\d{1,3}(?:\.\d{3})+(?:,\d+)?|-?\d+,\d+|-?\d+")

# Sonraki XBRL etiketi — '\n' YOK (KAP body tek satır olabilir)
_NEXT_TAG_RE = re.compile(r"(?:ifrs-full_|kap-fr_|kap_)[\w-]+\|", re.IGNORECASE)

# Birim çarpan tespiti — "Sunum Para Birimi | 1.000 TL"
_UNIT_MULTIPLIER_RE = re.compile(
    r"Sunum\s+Para\s+Birimi\s*\|\s*([0-9.,]+)?\s*TL", re.IGNORECASE
)


# ─── Yardımcı Fonksiyonlar ──────────────────────────────────────────────────────

def _parse_number(s: str) -> Optional[float]:
    """Türkçe formatlı sayı → float. '37.285.166' → 37285166.0"""
    s = s.strip().replace("%", "")
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    elif "." in s:
        parts = s.split(".")
        if all(len(p) == 3 for p in parts[1:]):
            s = "".join(parts)
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _detect_period(body: str) -> Optional[str]:
    """Body'de 'Cari Dönem ... 31.03.2026' kalıbından dönem çıkar.

    Birden fazla 'Cari Dönem' bloğu olabilir (Bilanço + Gelir Tablosu).
    EN YENİ tarihi seç (2026-Q1 vs 2025-Q4 birlikte olabilir).
    """
    candidates: list[tuple[int, int]] = []
    # Pattern: "Cari Donem 31.03.2026" veya "Cari Donem  ... 31.03.2026"
    for m in re.finditer(r"Cari\s*D[öo]nem[^|]{0,300}?(\d{2})\.(\d{2})\.(20\d{2})", body, re.IGNORECASE):
        try:
            day = int(m.group(1))
            month = int(m.group(2))
            year = int(m.group(3))
            candidates.append((year, month))
        except (ValueError, IndexError):
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    end_year, end_month = candidates[0]
    q = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}.get(end_month)
    if not q:
        return None
    return f"{end_year}-{q}"


def _detect_multiplier(body: str) -> int:
    """'1.000 TL' → 1000, '1.000.000 TL' → 1000000, yoksa 1"""
    m = _UNIT_MULTIPLIER_RE.search(body)
    if not m or not m.group(1):
        return 1
    raw = m.group(1).replace(".", "").replace(",", "").strip()
    try:
        n = int(raw)
        if n in (1000, 1000000):
            return n
    except ValueError:
        pass
    return 1


def _detect_sector(body: str) -> str:
    """KAP body'sinde hangi sektör? (XBRL etiket varlığına göre)"""
    # Banka — Net Faiz Geliri etiketi var
    if "kap-fr_NetInterestIncomeExpense|" in body or "ifrs-full_InterestIncome|" in body:
        return SECTOR_BANK
    # Sigorta — Teknik Bölüm Dengesi veya Brüt Yazılan Primler
    if "kap-fr_TechnicalBalance|" in body or "kap-fr_GrossWrittenPremiums|" in body:
        return SECTOR_INSURANCE
    return SECTOR_INDUSTRIAL


def _extract_value_after_tag(body: str, tag: str) -> Optional[float]:
    """XBRL etiketin SONUNDAKİ chunk'ta Cari Dönem sayısını çıkar.

    Strateji:
    1. Etiketten sonra BİR SONRAKİ XBRL etikete kadar olan içeriği al
    2. Bu içerikteki TÜM sayıları regex ile bul
    3. SON 2 sayı = (cari, önceki). Cari = sondan 2., önceki = son.
       Mantık: KAP RSC formatında her satır sonunda 'Cari | Önceki' kolonları var.

    BUG FIX (v2): Önceki versiyon ILK büyük sayıyı alıyordu — URL içindeki
    yıl numaralarını (2003, 2015) yanlışlıkla yakalıyordu.
    """
    idx = body.find(tag)
    if idx == -1:
        return None

    search_from = idx + len(tag)
    next_match = _NEXT_TAG_RE.search(body, search_from)
    end_idx = next_match.start() if next_match else search_from + 1500
    chunk = body[search_from:end_idx]

    # Tüm sayıları topla, filtreleri uygula
    nums = _NUM_RE.findall(chunk)
    valid: list[float] = []
    for raw in nums:
        v = _parse_number(raw)
        if v is None:
            continue
        # Çok küçük (1-9) ve nokta/virgül içermiyorsa muhtemelen dipnot referansı (1, 2, 3, 23)
        if abs(v) < 10 and "." not in raw and "," not in raw:
            continue
        # 4 haneli ve tam sayı ise URL'deki yıl olabilir (2003, 2015, 2026)
        # Ama gerçek finansal değer de 4 haneli olabilir (örn 4567). Bunu ayırt etmek için:
        # noktasız 4 haneli + 2000-2030 aralığında = yıl numarası (URL'den gelen)
        if "." not in raw and "," not in raw and 2000 <= abs(v) <= 2030:
            # Yıl numarası gibi görünüyor — atla
            continue
        valid.append(v)

    if len(valid) < 1:
        return None

    # SON 2 sayı = (cari, önceki). Tek sayı varsa o cari.
    return valid[-2] if len(valid) >= 2 else valid[-1]


# ─── Ana Parse Fonksiyonu ──────────────────────────────────────────────────────

def parse_kap_finansal_rapor(body: str) -> dict:
    """KAP Finansal Rapor body'sinden Cari Dönem finansal verilerini çıkar.

    Sektör tespiti yapar (industrial/bank/insurance), uygun etiket setini kullanır.

    Returns: dict — period + tüm DB alanları (eksikler None)
    """
    out: dict = {
        "period": None,
        "sector_type": None,
        # Standart alanlar
        "revenue": None,
        "gross_profit": None,
        "operating_profit": None,
        "net_income": None,
        "ebitda": None,
        "total_assets": None,
        "current_assets": None,
        "non_current_assets": None,
        "total_equity": None,
        "total_debt": None,
        "net_debt": None,
        "cash_and_equivalents": None,
        # Banka spesifik
        "net_interest_income": None,
        "net_fees_commissions": None,
        "loans": None,
        "deposits": None,
        # Sigorta spesifik
        "gross_premiums": None,
        "technical_balance": None,
        "source": "kap_xbrl_scrape",
        "confidence": "low",
    }

    if not body or len(body) < 100:
        return out

    out["period"] = _detect_period(body)
    multiplier = _detect_multiplier(body)
    sector = _detect_sector(body)
    out["sector_type"] = sector

    # Sektöre göre etiket seti
    if sector == SECTOR_BANK:
        tag_map = BANK_TAG_TO_FIELD
    elif sector == SECTOR_INSURANCE:
        tag_map = INSURANCE_TAG_TO_FIELD
    else:
        tag_map = INDUSTRIAL_TAG_TO_FIELD

    # XBRL etiketlerinden ham değerleri çek
    aux: dict = {}
    for tag, field in tag_map.items():
        v = _extract_value_after_tag(body, tag)
        if v is None:
            continue
        v_scaled = v * multiplier
        if field.startswith("_"):
            aux[field] = v_scaled
        else:
            if out.get(field) is None:
                out[field] = v_scaled

    # Industrial: parent equity/net income yoksa consolidated fallback
    if sector == SECTOR_INDUSTRIAL:
        if out["total_equity"] is None and aux.get("_total_equity_consolidated") is not None:
            out["total_equity"] = aux["_total_equity_consolidated"]
        if out["net_income"] is None and aux.get("_net_income_consolidated") is not None:
            out["net_income"] = aux["_net_income_consolidated"]

        # Net Borç = Finansal Borçlar - Nakit (Fintables formülü)
        fin_debt_parts = [
            aux.get("_current_borrowings"),
            aux.get("_current_portion_lt_borrowings"),
            aux.get("_longterm_borrowings"),
        ]
        fin_debt = sum(v for v in fin_debt_parts if v is not None)
        if fin_debt > 0:
            cash = out["cash_and_equivalents"] or 0
            out["net_debt"] = fin_debt - cash
            # total_debt artık financial debt olarak kaydedilir (kullanıcı tercihi)
            out["total_debt"] = fin_debt

        # Gerçek EBITDA = Brüt Kar - SG&A + Amortisman (REIT için doğru)
        # NOT: KAP body'sinde SG&A pozitif geliyor (label 'negatedLabel' ile).
        # xlsx'te negatif. Bu yüzden her zaman ABS ile çıkar.
        gross = out.get("gross_profit")
        if gross is not None:
            sga_total = (abs(aux.get("_sga_general") or 0)
                         + abs(aux.get("_sga_marketing") or 0)
                         + abs(aux.get("_sga_rd") or 0))
            depr = abs(aux.get("_depreciation_amortization") or 0)
            ebitda = gross - sga_total + depr
            out["ebitda"] = ebitda

    elif sector == SECTOR_BANK:
        # Banka EBITDA — Faaliyet Brüt Karı yaklaşık
        if out.get("gross_profit") is not None:
            out["ebitda"] = out["gross_profit"]
        # Banka için revenue = Net Faiz + Komisyon (Fintables tarzı)
        if out.get("revenue") is None and (out.get("net_interest_income") or out.get("net_fees_commissions")):
            nii = out.get("net_interest_income") or 0
            nfc = out.get("net_fees_commissions") or 0
            out["revenue"] = nii + nfc

    # Confidence: kritik alanlar dolu mu?
    critical = [out["period"], out["revenue"] or out["net_interest_income"],
                out["total_assets"], out["total_equity"]]
    filled = sum(1 for x in critical if x is not None)
    if filled >= 3:
        out["confidence"] = "high"
    elif filled >= 2:
        out["confidence"] = "medium"

    logger.info(
        "KAP parse: sector=%s period=%s revenue=%s net_income=%s assets=%s (conf=%s)",
        sector, out["period"], out["revenue"], out["net_income"], out["total_assets"], out["confidence"]
    )

    return out
