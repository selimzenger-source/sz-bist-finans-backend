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


def _span_to_quarter(span_months: int) -> Optional[str]:
    """Gelir tablosu kümülatif (YTD) dönem uzunluğundan çeyrek belirle.
    Bu, hesap dönemi başlangıç ayından BAĞIMSIZ çalışır:
      ~3 ay → Q1, ~6 ay → Q2, ~9 ay → Q3, ~12 ay → Q4 (yıllık).
    Özel hesap dönemli şirketler (örn. Nisan–Mart) için de doğru.
    """
    if span_months <= 4:
        return "Q1"
    if span_months <= 7:
        return "Q2"
    if span_months <= 10:
        return "Q3"
    return "Q4"


def _detect_period(body: str) -> Optional[str]:
    """KAP gelir tablosu Cari Dönem tarih ARALIĞINDAN dönem çıkar.

    ÖNEMLİ: Sadece bitiş tarihine bakmak yanlıştır — özel hesap dönemli
    şirketler (örn. MRGYO Nisan–Mart) yıllık raporda '31.03.2026' bitişi
    gösterir ama bu Q1 DEĞİL, YILLIK (12 ay) dönemdir.

    Doğru yöntem: '01.04.2025 - 31.03.2026' aralığını oku → span 12 ay →
    Q4 (yıllık). Yıl = başlangıç yılı (mali yılın adı), Fintables konvansiyonu
    ile uyumlu (01.04.2025–31.03.2026 → 2025-Q4 ≈ '2025/12').

    KAP gelir tablosu kümülatiftir; aynı bitiş tarihli birden fazla aralık
    varsa (kümülatif + 3-aylık kolonlar) EN UZUN span (kümülatif) seçilir.
    """
    # 1) Tarih aralıklarını topla: "DD.MM.YYYY - DD.MM.YYYY" (tire/en-dash)
    ranges: list[tuple[int, int, int, int]] = []  # (end_ord, start_ord, start_year, span)
    for m in re.finditer(
        r"(\d{2})\.(\d{2})\.(20\d{2})\s*[-–—]\s*(\d{2})\.(\d{2})\.(20\d{2})", body
    ):
        try:
            s_m, s_y = int(m.group(2)), int(m.group(3))
            e_m, e_y = int(m.group(5)), int(m.group(6))
        except (ValueError, IndexError):
            continue
        start_ord = s_y * 12 + s_m
        end_ord = e_y * 12 + e_m
        span = end_ord - start_ord + 1
        if span < 1 or span > 13:  # geçersiz / ters aralık
            continue
        ranges.append((end_ord, start_ord, s_y, span))

    if ranges:
        # En yeni bitiş; eşitse en uzun span (kümülatif YTD)
        ranges.sort(key=lambda r: (r[0], r[3]), reverse=True)
        _, _, start_year, span = ranges[0]
        q = _span_to_quarter(span)
        return f"{start_year}-{q}" if q else None

    # 2) Aralık yoksa (örn. yalnızca bilanço snapshot) — eski tekil-tarih fallback
    candidates: list[tuple[int, int]] = []
    for m in re.finditer(r"Cari\s*D[öo]nem[^|]{0,300}?(\d{2})\.(\d{2})\.(20\d{2})", body, re.IGNORECASE):
        try:
            candidates.append((int(m.group(3)), int(m.group(2))))
        except (ValueError, IndexError):
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    end_year, end_month = candidates[0]
    q = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}.get(end_month)
    return f"{end_year}-{q}" if q else None


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


def _extract_value_after_tag(body: str, tag: str, col_count: int = 2, prev: bool = False) -> Optional[float]:
    """XBRL etiketin SONUNDAKİ chunk'ta Cari (veya Önceki) Dönem sayısını çıkar.

    UNIVERSAL strateji:
    1. Etiketten sonra BİR SONRAKİ XBRL etikete kadar olan içeriği al
    2. İçerikteki TÜM filtrelenmiş sayıları bul (n adet)
    3. Cari Dönem TOPLAM seç:
       - n çift sayı → n//2 indeks = ilk yarının son sayısı = Cari Toplam
         (sanayi 2 sütun: cari=[0], önceki=[1]; banka 6 sütun: cari=[2] (TP/YP/Toplam))
       - n tek (1) → o tek değer cari
       - n=0 → None
    4. prev=True → ÖNCEKİ Dönem (ikinci yarının son sayısı = valid[n-1]).
       Enflasyon muhasebesinde bu, GÜNCEL döneme göre yeniden düzeltilmiş (restated)
       karşılaştırma değeridir — Fintables de bunu kullanır.

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
        # ★ KOK FIX (REEDR 11.06.2026): Dipnot REFERANS KOLONU virgullu de
        # olabiliyor — "11,12,19" (Not 11, 12 ve 19) listesinden "11,12"
        # yakalanip 11.12 TL 'amortisman' saniliyordu (FAVOK ~95mn eksik).
        # Finansal satir degerleri HAM TL'dir: |v| < 1000 hicbir gercek
        # tablo degeri olamaz (tag_map'te pay-basina/oran alani yok) —
        # virgullu/noktali olsa bile atla. Eski filtre sadece duz sayilari
        # ("11") atiyordu, virgullu dipnot listesi ("11,12") geciyordu.
        if abs(v) < 1000:
            continue
        # URL'deki yıl rakamları (2000-2030, nokta/virgül yok)
        if "." not in raw and "," not in raw and 2000 <= abs(v) <= 2030:
            continue
        valid.append(v)

    n = len(valid)
    if n == 0:
        return None
    if n == 1:
        return None if prev else valid[0]
    if prev:
        # Önceki Dönem = ikinci yarının son sayısı (Cari ile simetrik)
        return valid[n - 1]
    # n çift ise n//2 indeks = ilk yarının son sayısı = Cari Toplam
    half = n // 2
    return valid[half - 1] if half >= 1 else valid[0]


# ─── Ana Parse Fonksiyonu ──────────────────────────────────────────────────────

def parse_kap_finansal_rapor(body: str, ticker: str = "") -> dict:
    """KAP Finansal Rapor body'sinden Cari Dönem finansal verilerini çıkar.

    ticker: FAVÖK hesabını sektöre göre dogru yapmak icin (GYO tespiti).

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
    # ★ KOK FIX (REEDR FAVOK, 11.06.2026): aux alanlari KOSULSUZ uzerine
    # yaziliyordu — tag_map'te ayni alana birden fazla etiket varsa (orn.
    # amortisman: once NakitAkis 'Adjustments...' dogru 95.6mn'yi buldu,
    # sonra genel 'DepreciationAndAmortisationExpense' dipnottaki 11.12'yi
    # USTUNE YAZDI) → FAVOK ~95mn eksik cikiyordu. Kural artik out ile ayni:
    # tag_map SIRASI = oncelik; ILK bulunan deger korunur.
    aux: dict = {}
    for tag, field in tag_map.items():
        v = _extract_value_after_tag(body, tag, col_count)
        if v is None:
            continue
        v_scaled = v * multiplier
        if field.startswith("_"):
            if aux.get(field) is None:
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

        # FAVÖK — FINTABLES TANIMI (MARTI dogrulamasi ile duzeltildi):
        #   FAVÖK = Brut Kar − Faaliyet Giderleri (GYG + Pazarlama + ArGe) + Amortisman
        # "Esas faaliyetlerden DIGER gelir/gider" HARIC — Fintables 2024 MARTI
        # FAVÖK'u (88.1mn) esas faaliyet karindan (103.3mn) KUCUK; yani Fintables
        # diger gelir/gideri katmiyor. Eski formul (op + depr) MARTI'da 82mn
        # sapma uretiyordu (kur/degerleme kaynakli oynak "diger" kalemi yuzunden).
        # GYO dahil TUM sanayi/ticaret/holding ayni formul; op + depr sadece
        # SG&A hic parse edilemediyse fallback.
        gross = out.get("gross_profit")
        op = out.get("operating_profit")
        # ★ REEDR fix (11.06.2026): amortisman tag'i GUNCEL kolonda parse
        # edilemezse (None) FAVÖK ~amortisman kadar EKSIK hesaplaniyordu
        # (REEDR 26Q1: -25mn basildi, Fintables +70.5mn — fark tam amortisman).
        # Onceki kolon parse edildigi icin onceki donem dogruydu → tutarsiz
        # gorunum. Kural: amortisman tag'i YOKSA FAVÖK hesaplanmaz (None →
        # gorselde N/A) — yanlis deger basmaktansa bos birak.
        _depr_raw = aux.get("_depreciation_amortization")
        depr = abs(_depr_raw or 0)
        _has_sga = any(aux.get(k) is not None for k in ("_sga_general", "_sga_marketing", "_sga_rd"))
        # Debug: FAVOK bilesenleri (test-bilanco-parse ciktisinda gorunur;
        # save_parsed_bilanco alan listesinde olmadigi icin DB'ye YAZILMAZ)
        out["_debug_favok"] = {
            "gross": gross, "op": op, "depr_raw": _depr_raw,
            "sga_general": aux.get("_sga_general"),
            "sga_marketing": aux.get("_sga_marketing"),
            "sga_rd": aux.get("_sga_rd"),
        }
        # ★ SACMA-DEGER korumasi (REEDR 11.06.2026): amortisman tag'i bazen
        # YANLIS satiri/olcegi yakaliyor (REEDR: 11.12 TL "amortisman"!) ve
        # FAVÖK ~95mn eksik cikiyordu. Amortisman, brut karin binde 1'inden
        # ve 100K TL'den kucukse guvenilmez → FAVÖK hesaplanmaz (N/A).
        _depr_suspicious = (
            _depr_raw is not None
            and abs(_depr_raw) < max(100_000.0, 0.001 * abs(gross or 0))
        )
        if _depr_raw is None or _depr_suspicious:
            out["ebitda"] = None
            logger.warning(
                "FAVÖK hesaplanamadi (amortisman %s) — N/A birakildi",
                "tag'i yok" if _depr_raw is None else f"degeri supheli: {_depr_raw}",
            )
        elif gross is not None and _has_sga:
            sga_total = (abs(aux.get("_sga_general") or 0)
                         + abs(aux.get("_sga_marketing") or 0)
                         + abs(aux.get("_sga_rd") or 0))
            out["ebitda"] = gross - sga_total + depr
        elif op is not None:
            out["ebitda"] = op + depr
        elif gross is not None:
            out["ebitda"] = gross + depr

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

    # ── ÖNCEKİ DÖNEM (restated) — Fintables karşılaştırma değerleri ──
    # Güncel raporun "Önceki Dönem" kolonu enflasyona göre yeniden düzeltilmiştir.
    # Kart karşılaştırması (YoY gelir + önceki-dönem bilanço) BUNDAN gelir → solo/
    # konsolide ve enflasyon-restatement farkı olmadan Fintables ile birebir.
    prev: dict = {}
    aux_prev: dict = {}
    for tag, field in tag_map.items():
        v = _extract_value_after_tag(body, tag, col_count, prev=True)
        if v is None:
            continue
        v_scaled = v * multiplier
        if field.startswith("_"):
            # KOK FIX: current ile ayni kural — ilk bulunan deger korunur
            if aux_prev.get(field) is None:
                aux_prev[field] = v_scaled
        elif prev.get(field) is None:
            prev[field] = v_scaled
    if sector == SECTOR_INDUSTRIAL:
        if prev.get("total_equity") is None and aux_prev.get("_total_equity_consolidated") is not None:
            prev["total_equity"] = aux_prev["_total_equity_consolidated"]
        if prev.get("net_income") is None and aux_prev.get("_net_income_consolidated") is not None:
            prev["net_income"] = aux_prev["_net_income_consolidated"]
        _fd = sum(v for v in (aux_prev.get("_current_borrowings"), aux_prev.get("_current_portion_lt_borrowings"), aux_prev.get("_longterm_borrowings")) if v is not None)
        if _fd > 0:
            prev["net_debt"] = _fd - (prev.get("cash_and_equivalents") or 0)
        # FAVÖK prev — current ile AYNI Fintables formulu: Brut − SG&A + Amortisman
        # REEDR fix: amortisman tag'i yoksa FAVÖK hesaplanmaz (None) — current
        # ile ayni kural, asimetrik/yaniltici karsilastirma olusmasin.
        _g = prev.get("gross_profit")
        _op = prev.get("operating_profit")
        _dep_raw_p = aux_prev.get("_depreciation_amortization")
        _dep = abs(_dep_raw_p or 0)
        _has_sga_p = any(aux_prev.get(k) is not None for k in ("_sga_general", "_sga_marketing", "_sga_rd"))
        _dep_suspicious_p = (
            _dep_raw_p is not None
            and abs(_dep_raw_p) < max(100_000.0, 0.001 * abs(_g or 0))
        )
        if _dep_raw_p is None or _dep_suspicious_p:
            prev["ebitda"] = None
        elif _g is not None and _has_sga_p:
            _sga = abs(aux_prev.get("_sga_general") or 0) + abs(aux_prev.get("_sga_marketing") or 0) + abs(aux_prev.get("_sga_rd") or 0)
            prev["ebitda"] = _g - _sga + _dep
        elif _op is not None:
            prev["ebitda"] = _op + _dep
        elif _g is not None:
            prev["ebitda"] = _g + _dep
    elif sector == SECTOR_BANK:
        if prev.get("gross_profit") is not None:
            prev["ebitda"] = prev["gross_profit"]
        if prev.get("revenue") is None and (prev.get("net_interest_income") or prev.get("net_fees_commissions")):
            prev["revenue"] = (prev.get("net_interest_income") or 0) + (prev.get("net_fees_commissions") or 0)
    elif sector == SECTOR_INSURANCE:
        _gpnl = aux_prev.get("_gross_premium_nonlife") or 0
        _gplife = aux_prev.get("_gross_premium_life") or 0
        if _gpnl or _gplife:
            prev["gross_premiums"] = _gpnl + _gplife
            prev["revenue"] = prev["gross_premiums"]
    out["prev_period_values"] = {k: v for k, v in prev.items() if v is not None}

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
