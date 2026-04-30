"""Tum KAP Bildirimleri Sentiment Analizi.

Akis:
1. kap_all_scraper'dan gelen bildirim alınır
2. is_bilanco=True → AI atla (bilanco analizi henuz yok)
3. Gemini 2.5 Flash (birincil) ile sentiment + impact_score + ozet uret
4. Fallback 1: Abacus AI (RouteLLM)
5. Fallback 2: Kural tabanli basit analiz

Sonuc: {"sentiment": str, "impact_score": float, "summary": str}
"""

import json
import logging

import httpx

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
# System Prompt Yönetimi
# ═══════════════════════════════════════════════════════════════════

_DEFAULT_SYSTEM_PROMPT = """Sen CFA unvanli, 20+ yil buy-side+sell-side deneyimli, Borsa Istanbul'da uzmanlasmis SENIOR KURUMSAL YATIRIM ANALISTISIN.
KAP bildirimlerini analiz edip retail + profesyonel yatirimcilar icin yuksek kaliteli puan + ozet uretirsin.

═══ TEMEL YAKLASIM ═══
• FORWARD-LOOKING: Anlik mali etkiye degil, POTANSIYEL buyume/risk sinyallerine de bak.
• AKTIF PUANLAMA: Cogunlugu 4.5-5.5 sikismasindan KACIN. Cesur ol, ayristir.
• NUANS: "Rutin", "etkisiz", "somut gelisme yok" tarzi dismissive ifadelerden KACIN.
  Yerine: "kisa vadede sinirli etki, orta vadede X potansiyeli" tipi olculu yorum.
• BAGLAM: Kucuk cap icin yeni anlasma = buyuk pozitif; mega cap icin sinirli — ölçekle.

═══ ANALIZ ADIMLARI (her bildirim icin SIRASIYLA dusun) ═══
1. BILDIRIM TURU: Sozlesme/ihale, sermaye artirimi, bedelsiz, temettu, kar/zarar, dava-ceza,
   M&A, yonetim degisikligi, lisans-ruhsat, sermaye kaybi (TTK 376), idari/usul, vs.
2. NICELIKSEL ETKI: TL tutari, %, sözlesme buyuklugu — sayi yoksa turun kendisi sinyal.
3. SIRKET BAGLAMI: Mega cap icin 100M TL rutin, kucuk cap icin devasa olabilir.
4. FORWARD-LOOKING: Yeni musteri → ciro potansiyeli; yeni tesis → 2-3 yillik buyume; vs.
5. SURPRIZ MI BEKLENEN MI: Ilk kez aciklanan vs tekrar; beklenti uzeri/alti.
6. NIHAI PUAN: 1.0-10.0 arasi 0.1 hassasiyetle. Cesur ol.

═══ PUANLAMA RUBRIGI (1.0 — 10.0) ═══
KRITIK OLUMSUZ (1.0-2.4): TTK 376/3 borca batiklik, iflas, islem yasagi, sermaye kaybi %67+
OLUMSUZ (2.5-4.4):
  2.5-3.4: Net olumsuz — buyuk dava, donem zarari, uretim durdurma, lisans kaybi
  3.5-4.4: Hafif olumsuz — kucuk zarar, kucuk ceza (<5M TL), olumsuz denetci notu
NOTR (4.5-5.9):
  4.5-5.4: Tam notr — rutin bildirim, genel kurul, yonetim degisikligi, adres
  5.5-5.9: Notr+ — icerik belirsiz, SPK onay, personel alimi, kurumsal uyum
OLUMLU (6.0-7.9):
  6.0-6.4: Hafif olumlu — kucuk sozlesme (<50M TL), yeni isbirligi, lisans alimi
  6.5-6.9: Olumlu — orta sozlesme (50-200M TL), kapasite artirimi, yeni tesis
  7.0-7.4: Iyi — buyuk sozlesme (200M-1B TL), %10-20 kar artisi, bedelsiz %10-30
  7.5-7.9: Cok iyi — %20-40 kar artisi, buyuk ihale (>1B TL), bedelsiz %30-50
GUCLU OLUMLU (8.0-10.0):
  8.0-8.4: Guclu — %40-70 kar, bedelsiz %50-75, stratejik M&A
  8.5-8.9: Cok guclu — %70-100 kar, bedelsiz %75-100, mega ihale
  9.0-10.0: Olaganustu — %100+ kar, devasa M&A, sektor degistirecek olay

═══ KATEGORI KURALI (her bildirim icin ZORUNLU) ═══
"finansal" → kar/zarar, temettu, bedelsiz, sermaye artirimi, sozlesme/ihale tutari, ceza,
            dava, vergi, sermaye kaybi (rakamsal/finansal etki)
"strateji" → M&A, yeni tesis, yeni urun, lisans, kapasite artirimi, sektor liderligi,
            stratejik ortaklik (is modeli/rekabet konumu)
"bilgi"   → idari/usul: sorumluluk beyani, faaliyet raporu, genel kurul, yonetim komiteleri,
            esas sozlesme tadili, bilgi formu, bagimsiz denetim, sermaye piyasasi araci notu,
            imza sirkuleri, atama/gorev degisikligi (rutin), tescil, organizasyon semasi
            → Bunlar fiyat hareketi yaratmaz. Sentiment="Notr", score=4.8-5.2.

═══ SOZLESME / IHALE TUTAR OLCEKLEMESI (KRITIK) ═══

PARA BIRIMI CEVIRISI — ZORUNLU ILK ADIM:
Eger tutar USD/EUR/GBP/JPY/CHF gibi DOVIZ ile verilmisse, MUTLAKA TL'ye cevir.
Yaklasik kurlar (sıralama icin yeterli):
  1 USD ≈ 40 TL  | 1 EUR ≈ 43 TL  | 1 GBP ≈ 50 TL  | 1 JPY ≈ 0.27 TL  | 1 CHF ≈ 45 TL
Cevirmeden direkt sayiyi TL eşiklerine uygulamak BUYUK HATA.
Ornek: "5 milyon USD ihale" → 5 × 40 = 200 milyon TL → 6.7-7.2 bandi (orta-buyuk)
Ornek: "10 milyon EUR sozlesme" → 10 × 43 = 430 milyon TL → 6.7-7.2 bandi
Ornek: "1.5 milyar TL anlasma" → 7.5-8.5 bandi (cok buyuk) — ceviri gerekmiyor

Mutlak tutar (TL — ceviri sonrasi):
  >5 milyar     → 8.5-9.5 (mega)
  1-5 milyar    → 7.5-8.5 (cok buyuk)
  500M-1B       → 7.0-7.7 (buyuk)
  200-500M      → 6.7-7.2 (orta-buyuk)
  100-200M      → 6.4-6.8 (orta)
  50-100M       → 6.1-6.5 (orta-kucuk)
  25-50M        → 5.8-6.2 (kucuk)
  <25M          → 5.4-5.8 (cok kucuk — minimal etki)
Ciro orani: >%30 → +0.5 | %15-30 → +0.3 | <%5 → -0.2

═══ OZEL DURUMLAR ═══
• YENI TICARI ILISKI tutar belirsiz: DEFAULT 5.9-6.3 (HAFIF POZITIF) — ASLA 5.0 verme
• BEDELSIZ %100+ → 9.0-9.5 | %50-99 → 8.0-8.9 | %10-49 → 7.0-7.9

• TEMETTU PUANLAMA (KRITIK — VERIM ORANI uzerinden, sistem TEMETTU VERIM HESABI bolumunde
  hisse fiyatini ve verim%'sini sana verir; TL miktari TEK BASINA YANILTICIDIR):
  Verim ≥%10        → 8.5-9.5 (cok iyi — yatirimci icin cazip, BIST ortalamasinin uzerinde)
  Verim %7-10       → 7.8-8.5 (iyi — guclu temettu)
  Verim %5-7        → 7.0-7.7 (BIST ortalama ustu, olumlu)
  Verim %3-5        → 6.3-7.0 (BIST ortalamasi, hafif olumlu)
  Verim %2-3        → 5.7-6.3 (zayif pozitif)
  Verim %1-2        → 5.2-5.7 (notr+, sembolik)
  Verim %0.5-1      → 4.5-5.2 (zayif notr — yetersiz)
  Verim <%0.5       → 3.0-4.5 (NEGATIF — sembolik temettu, sirket gerçek temettu vermek
                                istemiyor sinyali; retail için "neden bu kadar az?" tepkisi)
  Temettu YOK/iptal → 3.0-4.5 (sirket karini dagıtmıyor — duruma gore cesur ol)
  Ilk kez temettu   → +0.3 bonus (yukaridaki banda ekle)
  Temettu artisi    → +0.2 bonus (gecen yıla gore artis varsa)

  ORNEKLER:
  - EREGL 35 TL hisse, 5 TL brut temettu = %14.3 verim → 8.8
  - EREGL 35 TL hisse, 0.50 TL temettu = %1.4 verim → 5.4
  - ECZYT 70 TL hisse, 5.71 TL brut = %8.2 verim → 8.2
  - XYZ 20 TL hisse, 0.05 TL temettu (5 kurus) = %0.25 verim → 3.5 (NEGATIF)
• SERMAYE KAYBI: TTK 376/1 (%50) → 2.0-2.5 | 376/2 (%67) → 1.5-2.0 | 376/3 (borca batik) → 1.0-1.4
• DEVRE KESICI: HER ZAMAN 5.0 Notr (sirket faaliyetiyle ilgisiz, otomatik mekanizma)
• IC KONSOLIDASYON (%100 bagli ortak devralma) → 5.1-5.5 (mali etki sinirli ama retail ilgi)

═══ TR RETAIL DAVRANIS KATMANI (+/- 0.1-0.2 ayarlama) ═══
"Bedelsiz", "birlesme", "devralma" kelimesi → +0.2 (retail favori)
Kucuk-orta cap (<5B TL mcap) + pozitif → +0.2 (volatilite yuksek)
Mega cap + kucuk tutar → -0.1
"Erteleme", "inceleniyor", "degerlendirilecek" → -0.1 (belirsiz dil)
SPK/BDDK yeni onay → +0.2 (momentum)

═══ HASHTAG KURALLARI ═══
2-3 adet, # isareti OLMADAN, ticker'i tekrar verme.
Sektor: gayrimenkul, enerji, teknoloji, insaat, gida, saglik, otomotiv, banka, havacilik,
       perakende, celik, kimya, iletisim, savunmasanayi, madencilik, finans
Konu: temettu, bedelsiz, sermayeartirimi, karaciklamasi, ihale, sozlesme, ortaklik, satis,
     yatirim, dava, ceza, ihracat, ithalat

═══ KRITIK KURALLAR ═══
• HALLUSINASYON YASAK: Haberde olmayan bilgi UYDURMA.
• ANTI-NOTR KUMELENMESI: 4.5-5.5 sikismasindan KACIN.
• OLCEKLEME: 100M$ ile 1M$ ASLA AYNI puan alamaz.
• "Olumlu/Olumsuz olabilir" gibi belirsiz ifadeler kullanma.
• SADECE JSON formatinda yanit ver — markdown, aciklama, yorum YAZMA."""

_custom_system_prompt: str | None = None


def get_system_prompt() -> str:
    return _custom_system_prompt if _custom_system_prompt is not None else _DEFAULT_SYSTEM_PROMPT


def set_system_prompt(new_prompt: str | None) -> None:
    global _custom_system_prompt
    _custom_system_prompt = new_prompt


def get_default_system_prompt() -> str:
    return _DEFAULT_SYSTEM_PROMPT


# Gemini 2.5 Flash — birincil (OpenAI uyumlu endpoint)
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"

# Gemini 2.5 Pro — yedek
_GEMINI_PRO_MODEL = "gemini-2.5-pro"

# Claude Haiku — Gemini basarisiz olursa son AI fallback
_HAIKU_URL = "https://api.anthropic.com/v1/messages"
_HAIKU_MODEL = "claude-haiku-4-5-20251001"

_AI_TIMEOUT = 45


def _get_keys() -> tuple[str | None, str | None]:
    """Config'den Gemini ve Anthropic API key'lerini al."""
    try:
        from app.config import get_settings
        s = get_settings()
        gemini = s.GEMINI_API_KEY if s.GEMINI_API_KEY else None
        anthropic = s.ANTHROPIC_API_KEY if s.ANTHROPIC_API_KEY else None
        return (gemini, anthropic)
    except Exception:
        return (None, None)


# ═══════════════════════════════════════════════════════════════════
# Kural tabanli fallback (AI basarisiz olursa)
# ═══════════════════════════════════════════════════════════════════

_POSITIVE_KEYWORDS = [
    "kar artisi", "kar artışı", "kâr artışı",
    "ihale kazandı", "ihale aldı",
    "anlaşma imzaladı", "anlaşma yapıldı",
    "ihracat", "satış artışı",
    "temettü", "kar payı dağıtım", "kâr payı dağıtım",
    "bedelsiz", "sermaye artırımı",
    "iş birliği anlaşması",
    "yeni yatırım", "kapasite artışı",
]

# Rutin/notr haberler — olumlu veya olumsuz sayilmamali
_NEUTRAL_KEYWORDS = [
    "esas sözleşme", "bilgi formu", "yönetim kurulu",
    "genel kurul", "sorumluluk beyanı", "faaliyet raporu",
    "finansal rapor", "bağımsız denetim", "komite",
]

# ═══════════════════════════════════════════════════════════════════
# Pre-filter — AI'a gitmeden rutin/idari basliklari yakala
# Token tasarrufu: tipik gunde ~%40-50 KAP bildirimi rutin formaliteler.
# Bu basliklara AI cagirmak hem para hem zaman israfidir.
# ═══════════════════════════════════════════════════════════════════
_ROUTINE_TITLE_PATTERNS = [
    "sorumluluk beyan",
    "faaliyet raporu",
    "yönetim kurulu komite",
    "yonetim kurulu komite",
    "genel kurul i̇şlem",
    "genel kurul islem",
    "genel kurul cagri",
    "genel kurul çağrı",
    "genel kurul sonu",
    "genel kurul toplanti",
    "esas sözleşme tadil",
    "esas sozlesme tadil",
    "şirket genel bilgi formu",
    "sirket genel bilgi formu",
    "kurumsal yonetim uyum",
    "kurumsal yönetim uyum",
    "bağımsız denetim raporu",
    "bagimsiz denetim raporu",
    "i̇mza sirküleri",
    "imza sirküleri",
    "imza sirkuleri",
    "organizasyon şema",
    "organizasyon sema",
    "bilgilendirme politika",
    "ücretlendirme politika",
    "ucretlendirme politika",
    "kar dagitim politikasi",
    "kâr dağıtım politikası",
    "sermaye piyasasi araci notu",
    "sermaye piyasası aracı notu",
    "yatirimci iliskileri",
    "yatırımcı ilişkileri",
    "yetki belgesi",
]


# İSTİSNA — bu kelimeler basliktaysa "rutin" sayma, AI'a gitsin
# Ornek: "Kar Payi Dagitimina Iliskin Genel Kurul Karari" — "genel kurul" geciyor
# ama "kar payi" istisna ile kurtaracagiz, AI temettu olarak puanlayacak.
_FINANCIAL_OVERRIDE_KEYWORDS = [
    "kar payı", "kar payi", "kâr payı", "kar dagitim", "kâr dağıtım",
    "temettü", "temettu",
    "bedelsiz",
    "sermaye artırım", "sermaye artirim",
    "birleşme", "birlesme", "devralma", "satın alma", "satin alma",
    "ihale", "sözleşme imza", "sozlesme imza",
    "ortaklık kur", "ortaklik kur",
    "yatırım kararı", "yatirim karari",
    "i̇hracat", "ihracat",
    "lisans alın", "lisans alin",
    "kar açık", "kâr açık", "kar acik",
    "zarar açık", "zarar acik",
    "ceza", "dava",
]


def _is_routine_admin_disclosure(title: str, body: str = "") -> bool:
    """Rutin/idari bildirim mi? AI'a gitmeden Notr/5.0 donmek icin.

    1. Title routine pattern'i match ediyorsa → rutin (AI atla)
    2. AMA: financial override kelimesi varsa → rutin DEGIL, AI'a git
       (Ornek: "Kar Payi Dagitimina Iliskin Genel Kurul Karari" → temettu, AI'a git)
    """
    if not title:
        return False
    title_norm = title.lower().strip()

    # Once routine pattern var mi
    is_routine = any(pattern in title_norm for pattern in _ROUTINE_TITLE_PATTERNS)
    if not is_routine:
        return False

    # Routine ama financial override kelimesi varsa: AI'a git
    has_financial_signal = any(kw in title_norm for kw in _FINANCIAL_OVERRIDE_KEYWORDS)
    if has_financial_signal:
        return False  # AI'a gitsin — onemli karar var

    return True

_NEGATIVE_KEYWORDS = [
    "zarar", "ceza", "dava",
    "borç", "iflas", "konkordato",
    "sermaye erimesi", "olumsuz",
    "fesih", "iptal", "azaltım",
]


def _rule_based_analyze(title: str, body: str) -> dict:
    """Basit kural tabanli sentiment analizi (fallback)."""
    text = f"{title} {body}".lower()

    # Oncelikle rutin/notr haber mi kontrol et
    neutral_hit = sum(1 for kw in _NEUTRAL_KEYWORDS if kw in text)
    if neutral_hit > 0:
        return {"sentiment": "Notr", "impact_score": 5.0, "summary": None, "category": "bilgi", "hashtags": []}

    pos = sum(1 for kw in _POSITIVE_KEYWORDS if kw in text)
    neg = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in text)

    if pos > neg:
        return {"sentiment": "Olumlu", "impact_score": 6.5, "summary": None, "category": "finansal", "hashtags": []}
    elif neg > pos:
        return {"sentiment": "Olumsuz", "impact_score": 3.5, "summary": None, "category": "finansal", "hashtags": []}
    return {"sentiment": "Notr", "impact_score": 5.0, "summary": None, "category": "bilgi", "hashtags": []}


# ═══════════════════════════════════════════════════════════════════
# Temettu Tespit + Verim Hesabi
# Bildirim "kar payi / temettu" iceriyorsa hisse fiyatini cek,
# AI prompt'una verim%'sini ekle. Boylece AI sadece TL miktarina degil,
# verim oranina gore puanlar.
# ═══════════════════════════════════════════════════════════════════

def _is_dividend_disclosure(title: str, body: str = "") -> bool:
    """Temettu/Kar Payi bildirimi mi?"""
    text = f"{title} {body}".lower()
    return any(kw in text for kw in (
        "kar payı", "kar payi", "kâr payı",
        "temettü", "temettu",
        "kar dağıtım", "kar dagitim", "kâr dağıtım",
    ))


def _extract_dividend_brut_tl(title: str, body: str) -> float | None:
    """KAP temettu bildiriminden 'brut TL/pay' miktarini cek.

    KAP formati: '1 TL Nominal Degerli Paya Odenecek Nakit Kar Payi - Brut(TL): 5,7142857'
    """
    import re
    text = f"{title}\n{body}"
    # Pattern 1: "Brüt(TL)" sonrasi ilk sayi
    patterns = [
        r"Br[üu]t\s*\(TL\)[\s\S]{0,200}?([\d]+[.,]\d+)",
        r"Br[üu]t\s*TL[\s\S]{0,100}?([\d]+[.,]\d+)",
        r"hisse\s*ba[şs][ıi]\s*br[üu]t[\s\S]{0,80}?([\d]+[.,]\d+)\s*TL",
        r"pay\s*ba[şs][ıi]\s*br[üu]t[\s\S]{0,80}?([\d]+[.,]\d+)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                # TR format: 5,7142857 → 5.7142857
                val = float(m.group(1).replace(".", "").replace(",", "."))
                if 0 < val < 1000:  # Mantikli aralik
                    return val
            except (ValueError, AttributeError):
                continue
    return None


async def _get_recent_stock_price(ticker: str) -> float | None:
    """Hisse anlik fiyatini cek (Yahoo + Mynet fallback).

    Cache yok burada — sadece temettu analizi sirasinda cagrilir, nadiren.
    Hata sessizce yutulur, None doner.
    """
    try:
        # main.py'deki helper'lari yeniden import et — cevrim kalmasin
        from app.main import _fetch_yahoo_v8, _fetch_mynet
        for fn in (_fetch_yahoo_v8, _fetch_mynet):
            try:
                p = await fn(ticker)
                if p is not None and p > 0:
                    return float(p)
            except Exception:
                continue
    except Exception as e:
        logger.warning("KAP Analyzer: Fiyat cekimi basarisiz (%s) — %s", ticker, e)
    return None


# ═══════════════════════════════════════════════════════════════════
# AI Analiz (Gemini Flash / Pro + Kural Tabanli Fallback)
# ═══════════════════════════════════════════════════════════════════

async def analyze_disclosure(
    company_code: str,
    title: str,
    body: str,
    is_bilanco: bool = False,
) -> dict:
    """KAP bildirimini AI ile analiz et.

    Args:
        company_code: Hisse kodu (orn: "THYAO")
        title: Bildirim basligi
        body: Bildirim tam metni
        is_bilanco: Bilanco/Finansal Rapor mu (True ise AI atla)

    Returns:
        {
            "sentiment": "Olumlu" | "Olumsuz" | "Notr",
            "impact_score": float (1.0-10.0),
            "summary": str | None,
        }
    """
    # Bilanco bildirimleri icin AI atla — ilerleyen safhada eklenecek
    if is_bilanco:
        logger.info("KAP Analyzer: Bilanco bildirimi, AI atla (%s)", company_code)
        return {"sentiment": "Notr", "impact_score": 5.0, "summary": None, "category": "bilgi", "hashtags": []}

    # ── Devre Kesici: AI'ya gonderme, sabit skor + metin don ──
    combined_text = f"{title} {body}".lower()
    if "devre kesici" in combined_text or "tek fiyat emir toplama" in combined_text:
        logger.info("KAP Analyzer: Devre kesici tespit edildi, AI atla (%s)", company_code)
        return {
            "sentiment": "Notr",
            "impact_score": 5.0,
            "summary": f"{company_code} hissesinde seans icinde devre kesici devreye girmistir.",
            "category": "bilgi",
            "hashtags": ["devrekesici"],
        }

    # ── Rutin/Idari Bildirim: AI'ya gonderme, sabit Notr/5.0 don ──
    # Sorumluluk Beyani, Faaliyet Raporu, Genel Kurul, Komiteler vs.
    # Token tasarrufu — tipik gunde %40-50 KAP bildirim bu kategoride.
    if _is_routine_admin_disclosure(title, body):
        logger.info(
            "KAP Analyzer: Rutin/idari bildirim — AI atla (%s) — '%s'",
            company_code, title[:60],
        )
        return {
            "sentiment": "Notr",
            "impact_score": 5.0,
            "summary": f"{company_code} tarafindan yapilan {title.strip()} bildirimi rutin/idari bir formalitedir. Hisse fiyatina dogrudan etkisi beklenmemektedir.",
            "category": "bilgi",
            "hashtags": [],
        }

    # ── Telegram Eşleşmesi (Sonnet skorlarını senkronize et) ──
    # NOT: Sadece ticker degil, baslik kelime eslesmesi de kontrol edilir.
    # Ayni ticker icin farkli KAP bildirimleri (orn: Finansal Rapor vs Uyum Raporu)
    # farkli AI analizleri almalidir.
    try:
        from app.database import async_session
        from app.models.telegram_news import TelegramNews
        from sqlalchemy import select, desc
        from datetime import datetime, timezone, timedelta

        async with async_session() as session:
            twelve_hours_ago = datetime.now(timezone.utc) - timedelta(hours=12)
            query = (
                select(TelegramNews)
                .where(
                    TelegramNews.ticker == company_code,
                    TelegramNews.ai_score.isnot(None),
                    TelegramNews.message_date >= twelve_hours_ago
                )
                .order_by(desc(TelegramNews.message_date))
                .limit(5)
            )
            result = await session.execute(query)
            recent_news_list = list(result.scalars().all())

            # Baslik eslesmesi: KAP bildirim basligindaki anahtar kelimeler
            # TelegramNews iceriginde de geciyorsa eslesme var demektir
            title_lower = title.lower()
            title_words = {w for w in title_lower.split() if len(w) > 3}

            for recent_news in recent_news_list:
                telegram_text = (recent_news.parsed_title or recent_news.raw_text or "").lower()
                telegram_words = {w for w in telegram_text.split() if len(w) > 3}
                common = title_words & telegram_words
                if len(common) >= 2:
                    logger.info(
                        "KAP Analyzer: TelegramNews eslesmesi (%s), skor: %s, ortak: %s",
                        company_code, recent_news.ai_score, common,
                    )
                    impact_score = recent_news.ai_score

                    if impact_score >= 6.0:
                        sentiment = "Olumlu"
                    elif impact_score < 4.5:
                        sentiment = "Olumsuz"
                    else:
                        sentiment = "Notr"

                    return {
                        "sentiment": sentiment,
                        "impact_score": impact_score,
                        "summary": recent_news.ai_summary,
                        "category": "finansal" if sentiment != "Notr" else "bilgi",
                        "hashtags": [],
                    }

            # Eslesen TelegramNews bulunamadi — AI analiz devam edecek
            if recent_news_list:
                logger.debug(
                    "KAP Analyzer: TelegramNews eslesmesi bulunamadi (%s, baslik: %s), AI analiz yapilacak",
                    company_code, title[:50],
                )
    except Exception as e:
        logger.warning("KAP Analyzer: TelegramNews senkronizasyon hatasi (%s): %s", company_code, e)

    (gemini_key, anthropic_key) = _get_keys()
    if not gemini_key and not anthropic_key:
        logger.error("KAP Analyzer: Hic API key yok — fallback (%s)", company_code)
        return _rule_based_analyze(title, body)

    content = f"{title}\n\n{body}".strip()[:4000]
    if not content:
        return {"sentiment": "Notr", "impact_score": 5.0, "summary": None, "category": "bilgi", "hashtags": []}

    # ── Temettu Bildirimi ise: hisse fiyatini cek + verim hesabi ekle ──
    dividend_context = ""
    if _is_dividend_disclosure(title, body):
        brut_tl = _extract_dividend_brut_tl(title, body)
        price = await _get_recent_stock_price(company_code)
        if brut_tl is not None and price is not None and price > 0:
            verim_pct = (brut_tl / price) * 100
            dividend_context = (
                f"\n\n═══ TEMETTU VERIM HESABI (sistem tarafindan onceden hesaplandi) ═══\n"
                f"Hisse anlik fiyati: {price:.2f} TL\n"
                f"Brut temettu (1 TL nominal paya): {brut_tl:.4f} TL\n"
                f"BRUT VERIM ORANI: %{verim_pct:.2f}\n"
                f"\n→ AI: Bu verim oranina gore TEMETTU PUANLAMA REHBERI'ni uygula. "
                f"TL miktari kuçuk gorunse de verim yuksekse iyi, TL buyuk gorunse de "
                f"verim dusukse zayif/negatif puan ver.\n"
            )
            logger.info(
                "KAP Analyzer: Temettu verim hesaplandi (%s) — %.2f TL / %.2f TL = %%%.2f",
                company_code, brut_tl, price, verim_pct,
            )
        elif brut_tl is not None:
            dividend_context = (
                f"\n\n═══ TEMETTU TESPITI ═══\n"
                f"Brut temettu: {brut_tl:.4f} TL/pay (hisse fiyati cekilemedi — verim hesaplanamadi)\n"
                f"→ AI: Hisse fiyatini bilmedigin halde puanlarken DIKKATLI ol, "
                f"TL miktari yaniltici olabilir. Sirketin tipik fiyat aralıgını dusunup tahmin et.\n"
            )

    prompt = f"""Borsa Istanbul (BIST) KAP bildirimi analizi.

Hisse: {company_code}

--- BILDIRIM BASLANGIC ---
{content}
--- BILDIRIM BITIS ---{dividend_context}

GOREV: Bu KAP bildirimini yatirimci bakis acisiyla analiz et. (Detayli rubrik system prompt'unda.)

CIKTI ALANLARI:
1. sentiment: "Olumlu" | "Olumsuz" | "Notr"
2. impact_score: 1.0-10.0 arasi 0.1 hassasiyetle (sysem prompt'undaki rubrik)
3. category: "finansal" | "strateji" | "bilgi"
4. summary: 3-5 cumle Turkce ozet (tweet-ready). Ne oldugunu, sirket icin ne anlama geldigini,
   yatirimci icin neden onemli oldugunu acikla. Onemli rakamlari (tutar, %, oran) dahil et.
5. hashtags: 2-3 adet (# isareti olmadan, ticker'i tekrar verme — sektor + konu)

SADECE asagidaki JSON formatinda yanit ver:
{{"sentiment": "Olumlu", "impact_score": 7.3, "category": "finansal", "summary": "3-5 cumle Turkce ozet.", "hashtags": ["sektor", "konu"]}}"""

    messages = [
        {"role": "system", "content": get_system_prompt()},
        {"role": "user", "content": prompt},
    ]
    payload = {
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 4096,  # Gemini 2.5 thinking token'ları da max_tokens'tan yer — düşük olunca content=null döner
    }

    # ── Birincil: Gemini 2.5 Flash ──
    ai_text = None
    provider_used = None

    if gemini_key:
        for model_name, model_label in [(_GEMINI_PRO_MODEL, "Pro"), (_GEMINI_MODEL, "Flash")]:
            if ai_text:
                break
            try:
                async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                    resp = await client.post(
                        _GEMINI_URL,
                        headers={
                            "Authorization": f"Bearer {gemini_key}",
                            "Content-Type": "application/json",
                        },
                        json={**payload, "model": model_name},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        # Gemini 2.5 bazen content=null döner (thinking mode)
                        choice = data.get("choices", [{}])[0]
                        msg = choice.get("message", {})
                        content = msg.get("content")
                        if content and content.strip():
                            ai_text = content.strip()
                            provider_used = f"Gemini-{model_label}"
                        else:
                            logger.warning(
                                "KAP Analyzer: Gemini-%s content bos (%s) — response keys: %s",
                                model_label, company_code,
                                list(msg.keys()) if msg else "no message",
                            )
                    else:
                        logger.warning(
                            "KAP Analyzer: Gemini-%s HTTP %s (%s) — %s",
                            model_label, resp.status_code, company_code, resp.text[:200],
                        )
            except Exception as e:
                logger.warning("KAP Analyzer: Gemini-%s hata (%s) — %s", model_label, company_code, e)

    # ── Gemini basarisiz → Claude Haiku fallback ──
    if not ai_text and anthropic_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                haiku_resp = await client.post(
                    _HAIKU_URL,
                    headers={
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": _HAIKU_MODEL,
                        "max_tokens": 500,
                        "temperature": 0.1,
                        "system": get_system_prompt(),
                        "messages": [{"role": "user", "content": prompt}],
                    },
                )
                if haiku_resp.status_code == 200:
                    haiku_data = haiku_resp.json()
                    haiku_content = haiku_data.get("content", [])
                    if haiku_content and haiku_content[0].get("text"):
                        ai_text = haiku_content[0]["text"].strip()
                        provider_used = "Haiku"
                        logger.info("KAP Analyzer: Haiku fallback basarili (%s)", company_code)
                else:
                    logger.warning(
                        "KAP Analyzer: Haiku HTTP %s (%s) — %s",
                        haiku_resp.status_code, company_code, haiku_resp.text[:200],
                    )
        except Exception as e:
            logger.warning("KAP Analyzer: Haiku hata (%s) — %s", company_code, e)

    # ── Tum AI modelleri basarisiz → kural tabanli fallback ──
    if not ai_text:
        logger.error("KAP Analyzer: AI basarisiz (Gemini+Haiku) — kural fallback (%s)", company_code)
        return _rule_based_analyze(title, body)

    # JSON parse — bozuk JSON'u da kurtarmaya calis
    from app.services.ai_json_helper import safe_parse_json
    result = safe_parse_json(ai_text, required_key="sentiment")
    if result is None:
        logger.error("KAP Analyzer: [%s] JSON parse basarisiz (%s) — icerik: %s",
                      provider_used, company_code, ai_text[:150])
        return _rule_based_analyze(title, body)

    sentiment = result.get("sentiment", "Notr")
    if sentiment not in ("Olumlu", "Olumsuz", "Notr"):
        sentiment = "Notr"

    impact_score = result.get("impact_score")
    if isinstance(impact_score, (int, float)) and 1.0 <= impact_score <= 10.0:
        impact_score = round(float(impact_score), 1)
    else:
        impact_score = 5.0

    summary = result.get("summary")
    if not isinstance(summary, str) or not summary.strip():
        summary = None

    # Yeni alanlar: category + hashtags
    category = result.get("category", "bilgi")
    if category not in ("finansal", "strateji", "bilgi"):
        category = "bilgi"

    hashtags_raw = result.get("hashtags", [])
    if isinstance(hashtags_raw, list):
        # Sadece string ve makul uzunlukta olanlari al, # isaretini temizle
        hashtags = []
        for h in hashtags_raw[:5]:
            if isinstance(h, str):
                cleaned = h.strip().lstrip("#").lower()
                if 2 <= len(cleaned) <= 30 and cleaned != company_code.lower():
                    hashtags.append(cleaned)
    else:
        hashtags = []

    logger.info(
        "KAP Analyzer [%s]: %s — sentiment=%s, score=%s, cat=%s, ozet=%s",
        provider_used, company_code, sentiment, impact_score, category,
        (summary[:60] + "...") if summary and len(summary) > 60 else summary,
    )

    return {
        "sentiment": sentiment,
        "impact_score": impact_score,
        "summary": summary,
        "category": category,
        "hashtags": hashtags,
    }
