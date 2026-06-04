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


def is_fund_ticker(ticker: str) -> bool:
    """Yatırım fonu / ETF ticker'ı mı?

    BIST hisse senedi kuralları:
    - 4-5 karakter, başında 1-2 sayı OLABİLİR (A1CAP, A1YEN gerçek hisse)
    - Sayı içerse bile 5 karaktere kadar = hisse

    Fon kuralları:
    - 6+ karakter (THY30FX gibi)
    - 6+ karakter ve içinde sayı (ZTM25F gibi) — kesin fon
    - F/FX/FN/FY ile biten 6+ karakterli ticker'lar
    """
    if not ticker:
        return False
    t = ticker.upper().strip()
    # Çok kısa veya çok uzun → fon
    if len(t) < 4 or len(t) > 8:
        return True
    # 5 karaktere kadar olan ticker'lar genelde hisse (A1CAP, THYAO, BANVT)
    if len(t) <= 5:
        return False
    # 6-8 karakter:
    # - Sayı içerirse fon (ZTM25F, AKBN30F)
    if any(c.isdigit() for c in t):
        return True
    # - F/FX/FN/FY ile biterse fon (BISTHFX, AKETFFN)
    if t.endswith("FX") or t.endswith("FN") or t.endswith("FY") or t.endswith("F"):
        return True
    return False


def is_dividend(title: str, body: str = "", ticker: str = "") -> bool:
    """Title temettu ile ilgili mi?

    "Hak Kullanımı" generic — hem temettü hem bedelsiz sermaye artırımı için kullanılıyor.
    Bu durumda body'ye bakıp ayırt et.

    Yatırım fonu / ETF / GYO bildirimleri filtre dışı (gerçek temettü değil).
    """
    if not title:
        return False
    # ★ Fon ticker'ı ise temettü olarak işleme — kullanıcının ZTM25F vakası
    if ticker and is_fund_ticker(ticker):
        return False
    t = lower_tr(title)

    # ★ MKK "Pay Mali Hak Kullanım İşlemi - Nakit Ödeme" — Merkezi Kayıt Kuruluşu'nun
    # ödeme SONRASI registry teyididir (ertesi gün gelir). Asıl ödeme zaten ilk gün
    # BIST/BISTECH "Pay Piyasası" bildirimiyle işlenir. Bu ikincil MKK bildirimi
    # REDUNDANT'tır VE %-tablosunu (Temettü Brüt Oran %188) yanlış parse edip hisse
    # başı TL sanıyordu (AYES 0,35 → 35,29 bug + Dağıtım Kararları'na düşme).
    # → Temettü olarak İŞLEME (calendar'a yazma).
    if "mali hak kullanım işlem" in t or "mali hak kullanim islem" in t:
        return False

    # ★ "Kâr/Kar (Payı) Dağıtım Politikası" — şirketin temettü POLİTİKASI dokümanı/güncellemesi.
    # Bu bir dağıtım KARARI veya ödeme DEĞİL; kurumsal yönetim/politika metnidir (oran/tutar
    # içermez). Temettü takvimine (Dağıtım/Dağıtmama/Ödeme) hiçbir kategoriye uymaz → İŞLEME.
    if "dağıtım politikası" in t or "dagitim politikasi" in t or "dağıtım politika" in t or "dagitim politika" in t:
        return False

    # Title "Merkezi Kayıt Kuruluşu" / "Hak Kullanım İşlemleri" → büyük olasılıkla bedelsiz
    # Bunlar temettü değil eğer body bedelsiz sermaye artırımı diyorsa
    title_generic_kkk = any(s in t for s in [
        "merkezi kayıt kuruluşu", "merkezi kayit kurulusu",
        "hak kullanım işlemleri", "hak kullanim islemleri",
    ])

    # Body sermaye artırımı sinyali veriyorsa temettü değil
    if body:
        b = lower_tr(body)
        capital_signals = [
            "bedelsiz pay alma orani", "bedelsiz pay alma oranı",
            "bedelsiz sermaye artırım", "bedelsiz sermaye artirim",
            "bedelsiz sermaye artir", "bedelsiz sermaye artırışı",
            "bonus issue", "bonus share",
            "rüçhan hakkı kullan", "ruchan hakki kullan",
            "sermaye azaltım oranı", "sermaye azaltim orani",
            "sermaye artırımı karşılığı", "sermaye artirimi karsiligi",
            "kayitlilesmis pay senetlerinin artirim", "kaydileşmiş pay senetlerinin artırım",
            "alacak kaydedilmiştir", "alacak kaydedilmistir",
        ]
        # Body'de "Pay Başına Brüt Temettü" yoksa AMA bedelsiz sinyali varsa → temettü değil
        is_capital_payload = any(s in b for s in capital_signals)
        is_dividend_payload = any(s in b for s in [
            "pay başına brüt temettü", "pay basina brut temettu",
            "gross dividend payment per share",
            "kar payı dağıt", "kar payi dagit", "kâr payı dağıt",
            "temettü dağıt", "temettu dagit",
            "temettü ödem", "temettu odem",
        ])
        if is_capital_payload and not is_dividend_payload:
            return False
        # Generic title (Merkezi Kayıt) + body'de net temettü ifadesi yoksa → temettü değil
        if title_generic_kkk and not is_dividend_payload:
            return False

    if any(p in t for p in _DIVIDEND_TITLE_PATTERNS):
        return True

    # BISTECH bulk announcement — title "BISTECH Pay Piyasası Alım Satım Sistemi
    # Duyurusu" generic, body'de "Pay Başına Brüt Temettü" geçiyorsa temettü
    # bulk ödeme/karar duyurusudur (multi-ticker).
    # NOT: lower_tr "BISTECH" → "bıstech" (dotsuz ı) yapar; iki yazimi yakala.
    # ÖNEMLI: KAP bazı bulk bildirimlerde body'yi sadece İngilizce yazıyor
    # (örn. 1605925 ALGYO/ASUZU/CCOLA/GIPTA/OZGYO/TEZOL → "Gross Dividend
    # Payment per share" ifadesi). İngilizce variantlar dahil.
    if ("bistech" in t or "bıstech" in t) and body:
        b = lower_tr(body)
        bistech_dividend_signals = [
            # Türkçe
            "pay başına brüt temettü", "pay basina brut temettu",
            "pay başına net temettü", "pay basina net temettu",
            "teorik fiyat", "teorik fiyati",
            "kar payı dağıt", "kar payi dagit",
            "temettü dağıt", "temettu dagit",
            "temettü ödem", "temettu odem",
            "brüt temettü", "brut temettu",
            # İngilizce (KAP multi-language bulk)
            "gross dividend payment per share",
            "net dividend payment per share",
            "theoretical price",
            "dividend payment",
        ]
        if any(s in b for s in bistech_dividend_signals):
            return True

    return False


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


def classify_event_with_body(title: str, body: str) -> str:
    """Title yetersizse BODY'ye bakarak siniflandir.

    KAP basliklari coğunlukla generic ("Kar Payı Dağıtım İşlemlerine İlişkin Bildirim"),
    fakat body'de "kar payı dağıtılmamasına karar verilmiştir" / "dağıtım yapılmasına"
    gibi acik ifadeler var.
    """
    # Once title-based classify
    by_title = classify_event(title or "")

    # Title net 'rejection' veya 'payment' dediyse o
    if by_title in ("rejection", "payment", "ga_approval"):
        return by_title

    # Body'de dağıtmama ipucu ara
    if body:
        b = lower_tr(body)
        if any(p in b for p in _PATTERN_REJECTION):
            return "rejection"
        # Body'de "dağıtılmaması", "dağıtmama", "kar payı dağıtmama" vb.
        rejection_phrases = [
            "kar payı dağıtılmama", "kar payi dagitilmama",
            "kâr dağıtılmama", "kar dagitilmama",
            "dağıtılmamasına karar", "dagitilmamasina karar",
            "dağıtılmaması", "dagitilmamasi",
            "dağıtmama kararı", "dagitmama karari",
            "kar payı dağıtmama", "kar payi dagitmama",
            "temettü dağıtılmaya", "temettu dagitilmaya",
            "kar payı dağıtım yapılmama", "kar payi dagitim yapilmama",
            # YENI — "kar dağıtımı yapılmamasına" tipi
            "dağıtımı yapılmamasına", "dagitimi yapilmamasina",
            "dağıtım yapılmamasına", "dagitim yapilmamasina",
            "dağıtılmaya", "dagitilmaya",
            "dağıtılmasına yer verilmemesi", "dagitilmasina yer verilmemesi",
            "kar payı dağıtılmamasına", "kar payi dagitilmamasina",
            "kâr payı dağıtılmamasına", "kar payi dagitilmamasina",
            "sıfır temettü", "sifir temettu",
            "temettü dağıtmama", "temettu dagitmama",
            # YENI v2 — daha kapsamli (BANVIT vakasi ve benzerleri)
            "dağıtım yapılmaması", "dagitim yapilmamasi",
            "dağıtılmamasına oybirliği", "dagitilmamasina oybirligi",
            "dağıtılmaması yönünde", "dagitilmamasi yonunde",
            "dağıtılmasına yer olmadığına", "dagitilmasina yer olmadigina",
            "dağıtım yapılmayacak", "dagitim yapilmayacak",
            "dağıtılmasına gerek olmadığı", "dagitilmasina gerek olmadigi",
            "kar payı ödenmeyecek", "kar payi odenmeyecek",
            "temettü ödenmeyecek", "temettu odenmeyecek",
            # KAP STANDART ALANI: "Nakit Kar Payı Ödeme Şekli: Ödenmeyecek" — en kesin
            # dağıtmama sinyali (KONKA 1611556 örneği: tabloda brüt tutar olsa BİLE ödeme
            # şekli 'Ödenmeyecek' ise temettü DAĞITILMAYACAK demektir → rejection).
            "ödeme şekli ödenmeyecek", "odeme sekli odenmeyecek",
            "ödeme şekli: ödenmeyecek", "odeme sekli: odenmeyecek",
            "kar payı ödeme şekli ödenmeyecek", "kar payi odeme sekli odenmeyecek",
            "kar dağıtmama yönünde", "kar dagitmama yonunde",
            "dağıtım gerçekleştirilmemesine", "dagitim gerceklestirilmemesine",
            "dağıtım gerçekleştirmeme", "dagitim gerceklestirmeme",
            "kar dağıtım planlanmamış", "kar dagitim planlanmamis",
            "kar payı dağıtım planlanmamış", "kar payi dagitim planlanmamis",
            "kar dağıtımı yapılmamasına", "kar dagitimi yapilmamasina",
            "kâr dağıtımı yapılmamasına", "kar dagitimi yapilmamasina",
            "kar dağıtmamasına", "kar dagitmamasina",
            "dağıtmamasına karar", "dagitmamasina karar",
            "dağıtılmamasının uygun", "dagitilmamasinin uygun",
            "kar payı dağıtmamasına", "kar payi dagitmamasina",
        ]
        if any(p in b for p in rejection_phrases):
            return "rejection"

        # ── GENEL KURUL ONAYI tespiti (body) ──────────────────────────────
        # YKK kararı önceden alınır; genel kurul ONAYLAR. Body'de "genel kurulda
        # ... kabul/onay/karar" geçiyorsa bu bir GK onay bildirimidir. Bu durumda
        # ilk karar zaten ilan edildiğinden yeni fiyat etkisi taşımaz → ga_approval.
        # NOT: rejection_phrases yukarıda kontrol edildi; buraya gelen body dağıtmama
        # DEĞİL. Yani GK onayı = dağıtım onayı (dağıtmama onayı rejection olarak döner).
        ga_body_phrases = [
            "genel kurulda kabul", "genel kurulda onay",
            "genel kurulda karara bağlan", "genel kurulda görüşül",
            "genel kurul tarafından kabul", "genel kurul tarafindan kabul",
            "genel kurul tarafından onay", "genel kurul tarafindan onay",
            "genel kurulca kabul", "genel kurulca onay",
            "genel kurulda kararlaştırıl", "genel kurulda kararlastiril",
            "genel kurul toplantısında kabul", "genel kurul toplantisinda kabul",
            "olağan genel kurul toplantısında", "olagan genel kurul toplantisinda",
            "genel kurulun onayı", "genel kurulun onayi",
            "genel kurulda görüşülerek kabul", "genel kurulda gorusulerek kabul",
            # KAP "Özet Bilgi" başlığı: "...Dağıtımına İlişkin Genel Kurul Kararı"
            # (kararın GENEL KURUL'da ALINDIĞI = onay aşaması; YKK önerisi "Yönetim
            # Kurulu Kararı" der). YKK'nın "genel kurula sunulacaktır" ifadesinden
            # ayrılır çünkü orada "genel kurul kararı" GEÇMİŞ/alınmış değildir.
            "dağıtımına ilişkin genel kurul karar", "dagitimina iliskin genel kurul karar",
            "ilişkin genel kurul kararı", "iliskin genel kurul karari",
            "kar payına ilişkin genel kurul karar", "kar payina iliskin genel kurul karar",
        ]
        if any(p in b for p in ga_body_phrases):
            return "ga_approval"

    return by_title


# Genel Kurul (onay/teyit) AŞAMASI sinyalleri — YKK önerisinden (ilk karar) ayırır.
# Karar GENEL KURUL'da ALINMIŞSA bu bir onay/teyittir (yeni fiyat etkisi yok). YKK
# önerisinde özet "Yönetim Kurulu Kararı"/"...Öneri" der ve GK henüz toplanmamıştır.
_GA_STAGE_SIGNALS = [
    "ilişkin genel kurul kararı", "iliskin genel kurul karari",
    "dağıtımına ilişkin genel kurul karar", "dagitimina iliskin genel kurul karar",
    "dağıtmamasına ilişkin genel kurul karar", "dagitmamasina iliskin genel kurul karar",
    "kar payına ilişkin genel kurul karar", "kar payina iliskin genel kurul karar",
    "genel kurulda kabul", "genel kurulda onay", "genel kurulda görüşül", "genel kurulda gorusul",
    "genel kurulda kararlaştırıl", "genel kurulda kararlastiril",
    "genel kurulda karara bağlan", "genel kurulda karara baglan",
    "genel kurul tarafından kabul", "genel kurul tarafindan kabul",
    "genel kurul tarafından onay", "genel kurul tarafindan onay",
    "genel kurulca kabul", "genel kurulca onay",
    "genel kurul toplantısında kabul", "genel kurul toplantisinda kabul",
    "konusu görüşüldü", "konusu gorusuldu",
]


def is_genel_kurul_decision(body: str) -> bool:
    """Bildirim GENEL KURUL aşaması mı (karar GK'da ALINMIŞ = onay/teyit)?

    True  → GK onayı/teyidi (yeni fiyat etkisi yok; dağıtım onayı nötr 5.5,
            dağıtmama onayı nötr 4.5).
    False → YKK önerisi / ilk karar (asıl fiyat etkisi; dağıtım pozitif, dağıtmama negatif).
    """
    if not body:
        return False
    # Apostrofları temizle: "Genel Kurul'da onaylandı" → "genel kurulda onaylandi"
    # (AI özeti apostrof kullanır, ham KAP body kullanmaz — ikisini de yakala).
    b = lower_tr(body).replace("'", "").replace("’", "").replace("`", "")
    return any(s in b for s in _GA_STAGE_SIGNALS)


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
  "payment_date": "YYYY-MM-DD",
  "payment_type": "cash" | "stock" | "cash_and_stock" | "none",
  "stock_ratio_text": "1 lota 2 lot" veya "%200" gibi serbest metin (sadece pay dagitimi varsa)
}}

KURALLAR:
- SADECE JSON dondur.
- Brut/net rakamlar TL cinsinden hisse basi.
- Yuzde verim: temettu/hisse_fiyat * 100.
- Tarihler bildirimde gecen tarihler.
- payment_type:
    * "cash" = sadece nakit temettu (cogu sirket)
    * "stock" = sadece bedelsiz pay/hisse (nakit yok)
    * "cash_and_stock" = hem nakit hem bedelsiz pay
    * "none" = dagitim yapilmayacak / red karari
- stock_ratio_text: bedelsiz pay verilecekse oranini metin olarak yaz, ornegin
  "1 lota 2 lot", "%200", "1.000.000 TL nominal 2.000.000 TL". Nakit ise null.
- Bilinmeyenler null.

★ KRITIK: DAGITMAMA KARARI TESPITI ★
Eger bildirim "kar payi DAGITILMAMASI", "DAGITILMAMASINA karar verilmistir",
"DAGITIM YAPILMAYACAK", "kar payi ODENMEYECEK", "DAGITMAMA karari" vb. ifadeler
iceriyorsa → bu bir DAGITMAMA (red) bildirimidir.
Bu durumda TUM sayisal alanlar NULL olmalidir:
  gross_amount_per_share = null
  net_amount_per_share = null
  gross_yield_pct = null
  net_yield_pct = null
  total_amount_tl = null
Cunku dagitim yapilmiyor — herhangi bir sayisal deger yanlis bilgi verir.
"""


_DATE_REGEX = re.compile(r"([0-3]?[0-9])[./]([0-1]?[0-9])[./](20[0-9]{2})")


def _parse_kap_dividend_table(body: str, ticker: str = "") -> dict[str, Any]:
    """KAP standart 'Nakit Kar Payı Ödeme Tutar ve Oranları' tablosundan hisse başı
    brüt/net temettüyü çıkar — AI'dan DAHA GÜVENİLİR (sabit kolon düzeni).

    Tablo formatı (kolonlar): Brüt(TL) | Brüt(%) | Stopaj(%) | Net(TL) | Net(%)
    Taksitli ödemelerde her grup için bir 'TOPLAM' satırı vardır; tek ödemede tek satır.
    İşlem gören ticker'ın grubunu tercih eder (A/B grubu aynı tutarsa fark etmez).

    Örnek (BVSAN): 'B Grubu, BVSAN, TRE... TOPLAM 1,4529342 145,29342 15 1,2349940 123,4994'
      → gross=1.4529342, net=1.2349940

    NOT: Brüt(%) kolonu NOMINAL'e göredir (fiyat verimi DEĞİL); yield hesabı frontend'de
    canlı fiyatla yapılır, bu yüzden yüzde alınmaz.
    """
    out: dict[str, Any] = {}
    if not body:
        return out
    tkr = (ticker or "").upper().strip()

    def _num(s: str) -> Optional[float]:
        try:
            v = float(s.replace(".", "").replace(",", "."))
            return v if 0 < v < 100000 else None
        except (ValueError, AttributeError):
            return None

    # Aday kalıplar (öncelik sırası):
    #   1) İşlem gören ticker'ın TOPLAM satırı (taksitli)
    #   2) Herhangi bir TOPLAM satırı
    #   3) İşlem gören ticker'ın tek ödeme satırı (Peşin/Tek/1. Taksit)
    #   4) "TOPLAM <brüt> <brüt%> <stopaj> <net>" (grup bilgisi olmadan)
    patterns: list[re.Pattern] = []
    if tkr:
        patterns.append(re.compile(rf"{re.escape(tkr)}[^|]*?TOPLAM\s+([\d.,]+)\s+[\d.,]+\s+\d+\s+([\d.,]+)"))
    patterns.append(re.compile(r"TOPLAM\s+([\d.,]+)\s+[\d.,]+\s+\d+\s+([\d.,]+)"))
    if tkr:
        patterns.append(re.compile(rf"{re.escape(tkr)}[^|]*?(?:Pe[şs]in|Tek\b|1\.\s*Taksit)\s+([\d.,]+)\s+[\d.,]+\s+\d+\s+([\d.,]+)"))

    for pat in patterns:
        m = pat.search(body)
        if not m:
            continue
        g = _num(m.group(1))
        n = _num(m.group(2))
        if g is not None:
            out["gross_amount_per_share"] = g
            if n is not None and n <= g:
                out["net_amount_per_share"] = n
            out["payment_type"] = "cash"
            break

    # Taksit sayısı (özet için faydalı; serbest metin)
    mt = re.search(r"(\d+)\s*Taksit", body, re.IGNORECASE)
    if mt:
        try:
            out["taksit_count"] = int(mt.group(1))
        except ValueError:
            pass

    return out


def _regex_parse_dividend_fallback(body: str) -> dict[str, Any]:
    """AI fail durumunda KAP body'den temettu alanlarini regex ile cikar.

    Production'da Gemini bazen fail oluyor ve tum alanlar NULL kaliyordu.
    Bu fallback en azindan payment_date + gross/net per share + payment_type
    cikararak takvim akisini calistirir.
    """
    out: dict[str, Any] = {}
    if not body:
        return out

    # ── Pay basina brut/net temettu ──────────────────────────────────────
    # Tipik patterns: "Pay başına brüt temettü: 1,2500 TL" / "Hisse Başına Brüt: 0,85 TL"
    gross_patterns = [
        r"(?:pay|hisse|1\s*tl\s*nominal)\s*ba[şs][ıi]na\s*br[üu]t(?:\s*kar\s*pay[ıi])?[\s:]+([\d.,]+)\s*(?:tl|tl\.|₺)",
        r"br[üu]t\s*(?:nakit\s*)?(?:kar\s*pay[ıi]|temett[uü])\s*tutar[ıi][\s:]+([\d.,]+)",
        r"1\s*tl\s*nominal\s*degerli\s*paya\s*br[üu]t[\s:]+([\d.,]+)",
    ]
    for pat in gross_patterns:
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            try:
                raw = m.group(1).replace(".", "").replace(",", ".")
                v = float(raw)
                if 0 < v < 10000:  # makul aralik
                    out["gross_amount_per_share"] = v
                    break
            except ValueError:
                pass

    net_patterns = [
        r"(?:pay|hisse|1\s*tl\s*nominal)\s*ba[şs][ıi]na\s*net(?:\s*kar\s*pay[ıi])?[\s:]+([\d.,]+)\s*(?:tl|tl\.|₺)",
        r"net\s*(?:nakit\s*)?(?:kar\s*pay[ıi]|temett[uü])\s*tutar[ıi][\s:]+([\d.,]+)",
        r"1\s*tl\s*nominal\s*degerli\s*paya\s*net[\s:]+([\d.,]+)",
    ]
    for pat in net_patterns:
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            try:
                raw = m.group(1).replace(".", "").replace(",", ".")
                v = float(raw)
                if 0 < v < 10000:
                    out["net_amount_per_share"] = v
                    break
            except ValueError:
                pass

    # ── Odeme tarihi ─────────────────────────────────────────────────────
    # "Ödeme Tarihi: 15.06.2026" / "Nakit Kar Payi Odeme Tarihi: 01.07.2026"
    pay_date_patterns = [
        r"(?:nakit\s*(?:kar\s*pay[ıi]\s*)?)?[öo]deme\s*(?:ba[şs]lang[ıi]c\s*)?tarihi[\s:]+([0-3]?[0-9])[./]([0-1]?[0-9])[./](20[0-9]{2})",
        r"hak\s*kullan[ıi]m\s*tarihi[\s:]+([0-3]?[0-9])[./]([0-1]?[0-9])[./](20[0-9]{2})",
        r"temett[uü](?:\s*[öo]deme)?\s*tarihi[\s:]+([0-3]?[0-9])[./]([0-1]?[0-9])[./](20[0-9]{2})",
    ]
    for pat in pay_date_patterns:
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            try:
                dd, mm, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                out["payment_date"] = date(yy, mm, dd)
                break
            except (ValueError, IndexError):
                pass

    # ── Genel Kurul tarihi ───────────────────────────────────────────────
    gk_patterns = [
        r"genel\s*kurul\s*(?:topla)?(?:nt[ıi])?\s*tarihi[\s:]+([0-3]?[0-9])[./]([0-1]?[0-9])[./](20[0-9]{2})",
        r"olagan\s*genel\s*kurul.*?([0-3]?[0-9])[./]([0-1]?[0-9])[./](20[0-9]{2})",
    ]
    for pat in gk_patterns:
        m = re.search(pat, body, re.IGNORECASE | re.DOTALL)
        if m:
            try:
                dd, mm, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                out["general_assembly_date"] = date(yy, mm, dd)
                break
            except (ValueError, IndexError):
                pass

    # ── Period (donem) ───────────────────────────────────────────────────
    # "2025 yılı kar payı" / "2025 hesap dönemi"
    m = re.search(r"(20[0-9]{2})\s*(?:y[ıi]l[ıi]|hesap\s*d[öo]nemi)\s*(?:kar\s*pay[ıi]|temett[uü])", body, re.IGNORECASE)
    if m:
        out["period"] = m.group(1)

    # ── Payment type ─────────────────────────────────────────────────────
    body_l = body.lower()
    if "bedelsiz" in body_l or "ic kaynaklar" in body_l or "iç kaynaklar" in body_l:
        if out.get("gross_amount_per_share"):
            out["payment_type"] = "cash_and_stock"
        else:
            out["payment_type"] = "stock"
    elif out.get("gross_amount_per_share"):
        out["payment_type"] = "cash"

    return out


async def ai_parse_dividend(
    ticker: str,
    title: str,
    body: str,
) -> dict[str, Any]:
    """KAP body'sinden temettu yapilandirilmis veri cikar.

    Once AI dener, AI bos donerse veya hata verirse regex fallback'i kullanir.
    Production'da AI fail rate yuksek oldugu icin fallback CALISTIRMA SART.
    """
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
        "payment_type": None,
        "stock_ratio_text": None,
    }

    # Once regex ile fallback degerleri cikar — sonra AI override etsin
    regex_out = _regex_parse_dividend_fallback(body or "")

    gemini_key = _get_gemini_key()
    if not gemini_key or not body:
        # AI yok — regex sonucunu dondur
        out.update({k: v for k, v in regex_out.items() if v is not None})
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
                    # payment_type & stock_ratio_text
                    pt = parsed.get("payment_type")
                    if isinstance(pt, str) and pt.lower() in ("cash", "stock", "cash_and_stock", "none"):
                        out["payment_type"] = pt.lower()
                    sr = parsed.get("stock_ratio_text")
                    if isinstance(sr, str) and sr.strip():
                        out["stock_ratio_text"] = sr.strip()[:80]
            else:
                logger.warning("Dividend AI: HTTP %s — %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("Dividend AI hata: %s", e)

    # AI'da bos kalan alanlari regex fallback ile doldur (sadece None olanlar)
    for k, v in regex_out.items():
        if out.get(k) is None and v is not None:
            out[k] = v

    # ★ KAP STANDART TABLO — en güvenilir kaynak: brüt/net/ödeme tipini OVERRIDE eder.
    # AI taksitli tabloyu yanlış okuyabiliyor (BVSAN: gross=None kaldı), tablo parser
    # sabit kolon düzeninden kesin çıkarır.
    table_out = _parse_kap_dividend_table(body or "", ticker)
    if table_out.get("gross_amount_per_share") is not None:
        out["gross_amount_per_share"] = table_out["gross_amount_per_share"]
        if table_out.get("net_amount_per_share") is not None:
            out["net_amount_per_share"] = table_out["net_amount_per_share"]
        out["payment_type"] = table_out.get("payment_type") or out.get("payment_type") or "cash"

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
    # ★ BULK ÖDEME DUYURUSU REDIRECTION ★
    # "BISTECH Pay Piyasası Alım Satım Sistemi Duyurusu" / "Borsa İstanbul A.Ş."
    # tipi bildirimler MULTI-SYMBOL bulk duyurular. Bunlar tek bir "issuer" (ISE/BIST)
    # için ayrı kayıt oluşturmamalı — body içindeki HER ticker'ın DividendCalendar
    # kaydı 'ödendi' olarak güncellenmeli.
    GENERIC_ISSUERS = {"ISE", "BIST", "BORSA", "MKK", "KAP"}
    _title_lo = (title or "").lower()
    is_bulk_announcement = (
        ticker.upper() in GENERIC_ISSUERS
        or "bistech pay piyasas" in _title_lo
        or "bıstech pay piyasas" in _title_lo
        or "borsa istanbul a.ş." in _title_lo
        or "borsa istanbul a.s." in _title_lo
    )
    if is_bulk_announcement and body and is_dividend_payment_announcement(title or "", body):
        logger.info(
            "Dividend BULK ödeme duyurusu tespit edildi (issuer=%s) — multi-ticker process'e yönlendiriliyor",
            ticker,
        )
        try:
            await process_dividend_payment_announcement(
                db,
                body=body,
                kap_url=kap_url,
                disclosure_id=disclosure_id,
                published_at=published_at,
            )
        except Exception as _e:
            logger.warning("Bulk ödeme duyurusu process hatası: %s", _e)
        # Issuer (ISE/BIST) için DividendCalendar kaydı AÇMA — sadece ticker'lar güncellendi
        return None

    # is_dividend body'ye + ticker'a da bakıyor — bedelsiz/sermaye artırımı/fon durumunda False döner
    if not is_dividend(title, body or "", ticker):
        return None

    # Body bos veya cok kisa ise (KAP fetch fail) — orphan kayit oluşturma
    # Çünkü gross/period bilgisi olmadan kayıt yararlı değil
    if not body or len(body) < 50:
        logger.info("Dividend skip — body bos/cok kisa (orphan oluşmasin): %s", ticker)
        return None

    # ── HAM KAP BODY (sınıflandırma için) ──────────────────────────────────
    # Dağıtmama kararının kesin sinyali "Nakit Kar Payı Ödeme Şekli: Ödenmeyecek"
    # SADECE ham KAP body'sinde var. Poller buraya AI ÖZETİ geçiyor; özet bu detayı
    # kaybediyor → DAĞITMAMA kararı 'ykk_alindi' (dağıtacak) gibi işleniyordu (KONKA
    # 1611556 bug'ı: temettü dağıtmadığı halde temettü listesinde görünüyordu).
    # Ham body'yi çek ve SINIFLANDIRMA için kullan (tutar parse'ı body ile aynı kalır).
    _classify_body = body or ""
    if kap_url:
        try:
            from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
            _disc = await fetch_kap_disclosure(kap_url)
            _raw = (_disc or {}).get("full_text") or ""
            if _raw and len(_raw) > len(_classify_body):
                _classify_body = _raw
                # Ham body daha zenginse tutar parse'ı da ondan daha iyi olur
                if not body or len(body) < len(_raw):
                    body = _raw
        except Exception as _e:
            logger.warning("Dividend ham body fetch hata (%s): %s", ticker, _e)

    # Title yetersizse body'ye bak (dağıtmama coğunlukla body'de gizli)
    event_type = classify_event_with_body(title, _classify_body)
    if event_type == "unknown":
        event_type = "ykk"

    # ★ REJECTION ise AI'a parse ettirme — sayisal alan ZATEN OLMAMALI
    #   AI body'de "X TL temettu odenmeyecek" cumlesinden X'i gross_amount olarak
    #   yakalayabilir, bu yanlis. Rejection icin tum sayisal alanlar NULL kalsin.
    if event_type == "rejection":
        parsed = {
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
    else:
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
                payment_type=parsed.get("payment_type"),
                stock_ratio_text=parsed.get("stock_ratio_text"),
                source_title=(title or "")[:255],
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
                  "gross_yield_pct", "net_yield_pct", "total_amount_tl",
                  "payment_type", "stock_ratio_text"):
            v = parsed.get(k)
            if v and not getattr(existing, k, None):
                setattr(existing, k, v)
        if title and not getattr(existing, "source_title", None):
            existing.source_title = title[:255]
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
                source_title=(title or "")[:255],
            )
            db.add(existing)
            await db.flush()
        existing.general_assembly_date = parsed.get("general_assembly_date") or (
            published_at.date() if published_at else None
        )
        existing.general_assembly_kap_disclosure_id = disclosure_id
        existing.general_assembly_kap_url = kap_url
        # AI parse'tan gelen amounts + payment_type/stock_ratio_text — GA bildiriminde
        # genelde tüm detaylar netleşir, bu yüzden mevcut alanları zenginleştir.
        for k in ("gross_amount_per_share", "net_amount_per_share",
                  "gross_yield_pct", "net_yield_pct", "total_amount_tl",
                  "payment_type", "stock_ratio_text"):
            v = parsed.get(k)
            if v and not getattr(existing, k, None):
                setattr(existing, k, v)
        if title and not getattr(existing, "source_title", None):
            existing.source_title = title[:255]
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
                source_title=(title or "")[:255],
            )
            db.add(existing)
            await db.flush()
        existing.status = "reddedildi"
        existing.rejected_at = datetime.now(timezone.utc)
        existing.rejection_kap_disclosure_id = disclosure_id
        existing.rejection_kap_url = kap_url
        existing.payment_type = "none"
        if title and not getattr(existing, "source_title", None):
            existing.source_title = title[:255]
        logger.info("Dividend: red (%s)", ticker)
        return existing

    if event_type == "payment":
        if not existing:
            existing = DividendCalendar(
                ticker=ticker, company_name=company_name, period=period,
                status="ykk_alindi",
                source_title=(title or "")[:255],
            )
            db.add(existing)
            await db.flush()
        if title and not getattr(existing, "source_title", None):
            existing.source_title = title[:255]
        # Payment_type fallback — sadece nakit/pay ödeme bildirimleri için
        if parsed.get("payment_type") and not getattr(existing, "payment_type", None):
            existing.payment_type = parsed.get("payment_type")
        if parsed.get("stock_ratio_text") and not getattr(existing, "stock_ratio_text", None):
            existing.stock_ratio_text = parsed.get("stock_ratio_text")
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

    # ── Telegram alert: kritik eksiklikler ──
    try:
        if existing:
            _missing = []
            # Odeme tarihi event'i ama payment_date NULL — buyuk sorun
            if event_type == "payment" and not existing.payment_date:
                _missing.append("payment_date")
            # GK onayinda brut TL ve odeme tarihi belli olmali
            if event_type == "ga_approval":
                if not existing.gross_amount_per_share and existing.payment_type != "stock" and existing.payment_type != "none":
                    _missing.append("gross_amount_per_share")
                if not existing.payment_date:
                    _missing.append("payment_date")
            if _missing:
                from app.services.admin_telegram import notify_kap_parse_issue
                await notify_kap_parse_issue(
                    "temettu", ticker, kap_url, _missing,
                    detail=f"event={event_type} status={existing.status} period={existing.period}",
                )
    except Exception:
        pass

    return existing


# Tek-kaynak kurali: dividend_history yalnizca temettuhisseleri.com'dan beslenir.
# Bu flag False iken KAP -> dividend_history mirror DEVRE DISI (cift kayit riski yok).
# Geri acmak istersen True yap (o zaman 2 yazar + app-dedup'a doner).
MIRROR_KAP_TO_HISTORY = False


async def mirror_to_dividend_history(db: AsyncSession, row: "DividendCalendar") -> bool:
    """KAP'tan gelen GK temettu kararini dividend_history tablosuna da yansit.

    Bu sayede:
    - /api/v1/temettu/{ticker} endpoint'i bu odemeyi gosterir
    - /api/v1/temettu-takvim takvimi bu odemeyi listeler
    - temettuhisseleri.com batch'i ile cakismaz (source='kap_gk' ile ayri tutulur)

    Args:
        row: DividendCalendar satiri (process_kap_disclosure cikti)
    """
    # ── TEK-KAYNAK KURALI ──
    # dividend_history artik SADECE temettuhisseleri.com (2 saatte bir) tarafindan beslenir.
    # KAP -> dividend_history mirror'i KAPATILDI -> cift kayit riski yapisal olarak biter.
    # KAP yalnizca dividend_calendar (canli takvim/TAB) besler; gecmis arsivi temettuhisseleri'nde.
    # NOT: calendar guncellemesi cagiranlarda devam eder; burasi sadece history-yazimini no-op yapar.
    if not MIRROR_KAP_TO_HISTORY:
        return False

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

# Alternatif kalip 1: ".E" suffix yok — Borsa Istanbul "Hak Kullanimi" duyurularinda
# "TICKER Pay Basina Brut Temettu: X,XX TL"
_PAYMENT_RE_NO_SUFFIX = re.compile(
    r"\b([A-Z]{2,6})\s+(?:Pay\s+Başına\s+Brüt\s+Temettü|Pay\s+Basina\s+Brut\s+Temettu)\s*:?\s*"
    r"([0-9]+(?:[.,][0-9]+)?)\s*(?:TL|₺)",
    re.IGNORECASE,
)

# Alternatif kalip 2: Tablo "TICKER ... X,XX TL" satir bazli
# "KLKIM        0,47 TL    %1,62"
_PAYMENT_RE_TABLE = re.compile(
    r"^\s*([A-Z]{3,6})\s+[\d.]*[,]?\d*\s*(?:TL|₺|TRY)\s+",
    re.MULTILINE,
)


def parse_dividend_payment_announcement(body: str) -> list[dict[str, Any]]:
    """KAP/MKK pay piyasası duyurusundan ödenen temettüleri çıkar.

    Birden fazla format destekler:
    1. "TICKER.E Pay Başına Brüt Temettü: X,XX TL" (BISTECH duyurusu)
    2. "TICKER Pay Başına Brüt Temettü: X,XX TL" (Borsa İstanbul Hak Kullanımı)
    3. Tablo satır bazlı (yedek)

    Returns:
        [{"ticker": "ALARK", "gross_amount_per_share": 3.185}, ...]
    """
    if not body:
        return []
    seen: set[str] = set()
    results: list[dict[str, Any]] = []

    # Birincil: .E suffixli
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

    # Ikincil: .E olmadan (Borsa Istanbul Hak Kullanimi duyurusu)
    for m in _PAYMENT_RE_NO_SUFFIX.finditer(body):
        ticker = m.group(1).upper()
        if ticker in seen:
            continue
        # Cok kisa veya endeks ticker'lari ele
        if len(ticker) < 3 or ticker in ("BIST", "TL", "USD", "EUR"):
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
    """Title + body üzerinden temettü ödeme duyurusu mu?

    Title kontrolü de eklendi: "Borsa İstanbul A.Ş. ... Hak Kullanımı" tipi
    bildirimler temettü ödeme duyurusu olabilir.
    """
    if not body:
        return False
    # Title sinyalleri
    if title:
        t = lower_tr(title)
        title_signals = [
            "bistech pay piyasasi", "bistech pay piyasası",
            "bıstech pay piyasasi", "bıstech pay piyasası",  # lower_tr dotless variant
            "borsa istanbul a.ş.", "borsa istanbul a.s.",
            "borsa ıstanbul a.ş.", "borsa ıstanbul a.s.",
            "hak kullanım işlemleri", "hak kullanim islemleri",
            "temettü ödeme", "temettu odeme",
            "nakit temettü hak kullan", "nakit temettu hak kullan",
        ]
        has_title = any(s in t for s in title_signals)
        if has_title and (_PAYMENT_RE.search(body) or _PAYMENT_RE_NO_SUFFIX.search(body)):
            return True
    # Title olmasa bile body'de pattern varsa
    has_pattern = bool(_PAYMENT_RE.search(body) or _PAYMENT_RE_NO_SUFFIX.search(body))
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
