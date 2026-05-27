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
    "kap-fr_OperatingIncome|": "revenue",  # Faktöring/Leasing — ESAS FAALİYET GELİRLERİ
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

# BANKA (BDDK formatı) — XBRL etiketleri farklı (gerçek KAP body'sinden teyit edildi)
BANK_TAG_TO_FIELD: dict[str, str] = {
    # Bilanço (TP / YP / Toplam — 3 sütunlu format)
    "ifrs-full_Assets|": "total_assets",         # VARLIKLAR TOPLAMI
    "ifrs-full_Equity|": "total_equity",         # ÖZKAYNAKLAR
    "ifrs-full_EquityAndLiabilities|": "_total_eq_and_liab",  # YÜKÜMLÜLÜKLER TOPLAMI
    "ifrs-full_CashAndCashEquivalents|": "cash_and_equivalents",
    # Banka spesifik
    "kap-fr_Loans|": "loans",                     # Krediler
    "kap-fr_Deposits|": "deposits",               # Mevduat (yükümlülük)
    # Gelir Tablosu (Banka — BDDK)
    "kap-fr_InterestIncomeOrExpense|": "net_interest_income",      # NET FAİZ GELİRİ VEYA GİDERİ
    "kap-fr_FeeAndCommissionIncomeOrExpenses|": "net_fees_commissions",  # NET ÜCRET VE KOMİSYON
    "kap-fr_GrossProfitLossFromOperatingActivitiesForBankingSector|": "gross_profit",  # FAALİYET BRÜT KARI
    "ifrs-full_ProfitLossFromOperatingActivities|": "operating_profit",  # NET FAALİYET KARI
    "ifrs-full_ProfitLoss|": "net_income",        # DÖNEM KARI
}

# SIGORTA — XBRL etiketleri farklı (gerçek KAP body'sinden teyit edildi)
INSURANCE_TAG_TO_FIELD: dict[str, str] = {
    # Bilanço
    "ifrs-full_Assets|": "total_assets",
    "ifrs-full_Equity|": "total_equity",
    "ifrs-full_Liabilities|": "total_debt",
    "ifrs-full_CashAndCashEquivalents|": "cash_and_equivalents",
    # Sigorta gelir tablosu — teknik GELIR (brut) — net teknik dengeden farkli
    "kap-fr_NonlifeTechnicalIncome|": "_nonlife_tech_income",
    "kap-fr_LifeTechnicalIncome|": "_life_tech_income",
    # Net teknik bölüm dengesi — Hayat Dışı + Hayat (income − expense net'i)
    # Bu, parser onceki versiyonda 82% sapma yapiyordu (ham toplam aliyordu).
    "kap-fr_NonlifeTechnicalSectionBalance|": "_nonlife_tech_balance",
    "kap-fr_LifeTechnicalSectionBalance|": "_life_tech_balance",
    "kap-fr_TechnicalBalance|": "_total_tech_balance",  # Eger varsa direkt al
    # Brüt Yazılan Prim — Hayat Dışı + Hayat ayrı satır
    "kap-fr_GrossWrittenPremiumsClassifiedAsNonlifeTechnicalIncome|": "_gross_premium_nonlife",
    "kap-fr_GrossWrittenPremiumsClassifiedAsLifeTechnicalIncome|": "_gross_premium_life",
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
    # Banka — BDDK formatı (gerçek KAP body etiketleri)
    bank_markers = [
        "kap-fr_InterestIncomeOrExpense|",
        "kap-fr_GrossProfitLossFromOperatingActivitiesForBankingSector|",
        "kap-fr_FeeAndCommissionIncomeOrExpenses|",
    ]
    if any(m in body for m in bank_markers):
        return SECTOR_BANK
    # Sigorta — Teknik Bölüm Dengesi veya Brüt Yazılan Primler
    insurance_markers = [
        "kap-fr_TechnicalBalance|",
        "kap-fr_GrossWrittenPremiums|",
        "HAYAT DIŞI TEKNİK GELİR",
        "TEKNİK BÖLÜM DENGESİ",
    ]
    if any(m in body for m in insurance_markers):
        return SECTOR_INSURANCE
    return SECTOR_INDUSTRIAL


def _detect_column_count(body: str) -> int:
    """KAP body'sinde tablo kaç sütunlu? (sanayi: 2, banka: 6 TP/YP/Toplam ×2)

    Banka format: Cari Dönem TP | Cari YP | Cari Toplam | Önceki TP | Önceki YP | Önceki Toplam
    """
    # 'Türk Lirası' + 'Yabancı Para' başlığı varsa banka tarzı 6 sütunlu
    has_tp_yp = ("Türk Lirası" in body or "TP|" in body) and ("Yabancı Para" in body or "YP|" in body)
    if has_tp_yp:
        return 6
    return 2


def _extract_value_after_tag(body: str, tag: str, col_count: int = 2) -> Optional[float]:
    """XBRL etiketin SONUNDAKİ chunk'ta Cari Dönem sayısını çıkar.

    UNIVERSAL strateji:
    1. Etiketten sonra BİR SONRAKİ XBRL etikete kadar olan içeriği al
    2. İçerikteki TÜM filtrelenmiş sayıları bul (n adet)
    3. Cari Dönem TOPLAM seç:
       - n çift sayı → n//2 indeks = ilk yarının son sayısı = Cari Toplam
         (sanayi 2 sütun: cari=[0], önceki=[1]; banka 6 sütun: cari=[2] (TP/YP/Toplam))
       - n tek (1) → o tek değer cari
       - n=0 → None

    col_count parametresi backward-compat için, artık otomatik tespit ediliyor.
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
        # Dipnot referansları (1-200 arası, nokta/virgül yok) — atla
        # Finansal değerler ya binlik (286.641) ya ondalık ya çok büyük (10.000+)
        if "." not in raw and "," not in raw and abs(v) < 1000:
            continue
        # URL'deki yıl rakamları (2000-2030, nokta/virgül yok)
        if "." not in raw and "," not in raw and 2000 <= abs(v) <= 2030:
            continue
        valid.append(v)

    n = len(valid)
    if n == 0:
        return None
    if n == 1:
        return valid[0]
    # n çift ise n//2 indeks = ilk yarının son sayısı = Cari Toplam
    half = n // 2
    return valid[half - 1] if half >= 1 else valid[0]


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
    col_count = _detect_column_count(body)

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
        v = _extract_value_after_tag(body, tag, col_count)
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

    elif sector == SECTOR_INSURANCE:
        # Sigorta: gross_premiums = Hayat Dışı + Hayat brüt yazılan prim toplamı
        gp_nl = aux.get("_gross_premium_nonlife") or 0
        gp_life = aux.get("_gross_premium_life") or 0
        if gp_nl or gp_life:
            out["gross_premiums"] = gp_nl + gp_life
            out["revenue"] = out["gross_premiums"]  # Fintables tarzı
        # Net Teknik Bölüm Dengesi — gerçek değer (gelir - gider net'i, brüt toplam DEĞİL).
        # Onceki versiyon "_nonlife_tech_income + _life_tech_income" yapiyordu (brut toplam) → %82 sapma.
        # Yeni: oncelik sirasiyla:
        # 1) kap-fr_TechnicalBalance (eger varsa direkt) → net dengeyi verir
        # 2) NonlifeTechnicalSectionBalance + LifeTechnicalSectionBalance toplami
        # 3) HiC degilse: technical_balance = None (yanlis veri yazma)
        total_tb = aux.get("_total_tech_balance")
        if total_tb:
            out["technical_balance"] = total_tb
        else:
            nl_bal = aux.get("_nonlife_tech_balance") or 0
            l_bal = aux.get("_life_tech_balance") or 0
            if nl_bal or l_bal:
                out["technical_balance"] = nl_bal + l_bal
            # Aksi takdirde None birak — yanlis aliasi yazmayalim
        # Sigorta icin gross_profit ve ebitda kavramlari banka gibi tartismali —
        # technical_balance varsa ona esitlemek yaniltici (sigorta net karliligi
        # bundan farkli hesaplanir). UI tarafinda gizlenecek olan alanlara hayalet
        # veri yazmaktan kacin: sadece technical_balance set et.
        # out["gross_profit"] = out["ebitda"] = None  (kasten doldurmuyoruz)

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
