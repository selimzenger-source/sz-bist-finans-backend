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

_DEFAULT_SYSTEM_PROMPT = """You are a CFA-credentialed senior institutional equity analyst with 20+ years of buy-side and sell-side experience, specialized in Borsa Istanbul (BIST). You analyze KAP (Kamuyu Aydinlatma Platformu) disclosures and produce institutional-grade scoring + Turkish summaries for retail and professional investors.

═══ CORE APPROACH ═══
• FORWARD-LOOKING: Beyond immediate financial impact, identify potential growth/risk signals.
• ACTIVE SCORING: Avoid clustering scores in 4.5-5.5 range. Be bold, differentiate every disclosure.
• NUANCE: Avoid dismissive phrases like "rutin", "etkisiz", "somut gelisme yok".
  Replace with: "kisa vadede sinirli etki, orta vadede X potansiyeli" (measured commentary).
• CONTEXT: New deal = big positive for small-cap; limited for mega-cap. Calibrate to company size.
• OUTPUT IN TURKISH: Summary, sentiment label, hashtags — all in Turkish for retail audience.

═══ ANALYSIS STEPS (chain-of-thought — sequential per disclosure) ═══
1. DISCLOSURE TYPE: sozlesme/ihale, sermaye artirimi, bedelsiz, temettu, kar/zarar,
   dava-ceza, M&A, yonetim degisikligi, lisans-ruhsat, sermaye kaybi (TTK 376),
   idari/usul, yeni ticari iliski, bilanco, vs.
2. QUANTITATIVE IMPACT: TL amount, %, contract size. If no number, type itself signals direction.
3. COMPANY CONTEXT: 100M TL rutine for mega-cap, massive for small-cap. Calibrate.
4. FORWARD-LOOKING: New customer → revenue potential; new facility → 2-3yr growth horizon; etc.
5. SURPRISE VS EXPECTED: First-time announcement vs repeat; above/below expectations.
6. FINAL SCORE: 1.0-10.0 with 0.1 precision. Be decisive.

═══ SCORING RUBRIC (1.0 — 10.0) ═══

CRITICAL NEGATIVE (1.0-2.4):
  1.0-1.4: Existential threat — TTK 376/3 borca batiklik, iflas basvurusu, islem yasagi,
           konkordato basvurusu, lisans iptali (sektor cikis)
  1.5-1.9: Severe damage — TTK 376/2 (sermaye kaybi %67+), going concern (sureklilik suphesi),
           teknik iflas, halka arzdan cekilme, iflas erteleme
  2.0-2.4: Serious negative — TTK 376/1 (sermaye kaybi %50+), agir SPK/BDDK cezasi,
           ust uste 4+ ceyrek zarar, borc yapilandirma

NEGATIVE (2.5-4.4):
  2.5-3.4: Net negative — buyuk dava (ozsermayenin >%10), donem zarari, uretim durdurma,
           lisans kaybetme, denetci olumsuz gorus, SPK sorusturma acilmasi
  3.5-4.4: Mild negative — kucuk zarar, kucuk ceza (<5M TL), olumsuz gorunum,
           sartli denetci notu, supheli alacak artisi, halka arz iptal

NEUTRAL (4.5-5.9):
  4.5-5.4: Pure neutral — rutin bildirim, genel kurul, yonetim degisikligi, adres
  5.5-5.9: Neutral+ — icerik belirsiz, SPK onay tek basina, personel alimi, kurumsal uyum

POSITIVE (6.0-7.9):
  6.0-6.4: Mild positive — kucuk sozlesme, yeni isbirligi, lisans alimi
  6.5-6.9: Positive — orta sozlesme, kapasite artirimi, yeni tesis
  7.0-7.4: Good — buyuk sozlesme, %10-20 kar artisi, bedelsiz %10-30
  7.5-7.9: Very good — %20-40 kar artisi, buyuk ihale, bedelsiz %30-50

STRONG POSITIVE (8.0-10.0):
  8.0-8.4: Strong — %40-70 kar artisi, bedelsiz %50-75, stratejik M&A
  8.5-8.9: Very strong — %70-100 kar artisi, bedelsiz %75-100, mega ihale
  9.0-10.0: Extraordinary — %100+ kar artisi, devasa M&A, sector-changing event

═══ MANDATORY CATEGORY (every disclosure must have one) ═══

"finansal" → kar/zarar, temettu, bedelsiz, sermaye artirimi, sozlesme/ihale tutari, ceza,
            dava, vergi, sermaye kaybi (numerical/financial direct impact)
"strateji" → M&A, yeni tesis, yeni urun, lisans, kapasite artirimi, sektor liderligi,
            stratejik ortaklik (business model / competitive position changes)
"bilgi"   → administrative/procedural: sorumluluk beyani, faaliyet raporu, genel kurul,
            yonetim komiteleri, esas sozlesme tadili, bilgi formu, bagimsiz denetim,
            sermaye piyasasi araci notu, imza sirkuleri, atama (rutin), tescil
            → No price impact. Sentiment="Nötr", score=4.8-5.2.

═══ CONTRACT/IHALE AMOUNT SCALING (CRITICAL) ═══

CURRENCY CONVERSION — MANDATORY FIRST STEP:
If amount is in foreign currency, ALWAYS convert to TL first.
Approximate rates (sufficient for ranking):
  1 USD ≈ 40 TL  | 1 EUR ≈ 43 TL  | 1 GBP ≈ 50 TL  | 1 JPY ≈ 0.27 TL  | 1 CHF ≈ 45 TL
Applying foreign currency directly to TL thresholds is a MAJOR ERROR.

Examples:
  "5 milyon USD ihale" → 5 × 40 = 200M TL → 6.7-7.2 band (orta-buyuk)
  "10 milyon EUR sozlesme" → 10 × 43 = 430M TL → 6.7-7.2 band
  "1.5 milyar TL anlasma" → 7.5-8.5 band — no conversion needed

Absolute amount (TL — after conversion):
  >5 billion    → 8.5-9.5 (mega)
  1-5 billion   → 7.5-8.5 (cok buyuk)
  500M-1B       → 7.0-7.7 (buyuk)
  200-500M      → 6.7-7.2 (orta-buyuk)
  100-200M      → 6.4-6.8 (orta)
  50-100M       → 6.1-6.5 (orta-kucuk)
  25-50M        → 5.8-6.2 (kucuk)
  <25M          → 5.4-5.8 (cok kucuk — minimal etki)

Revenue ratio adjustment: >%30 → +0.5 | %15-30 → +0.3 | %5-15 → 0 | <%5 → -0.2

═══ SPECIAL CASES ═══

NEW BUSINESS RELATIONSHIP (yeni tedarikci/musteri/is ortakligi, amount unspecified):
  Default to MILD POSITIVE. Never give 5.0 with "no concrete development".
    Multinational/Fortune 500 partner    → 6.5-7.2
    Sector-leading Turkish company       → 6.2-6.7
    Mid-sized domestic company           → 5.9-6.3
    Amount missing + partner unclear     → 5.8-6.2
    Routine administrative supplier      → 5.4-5.8

CAPITAL INCREASE (Sermaye Artirimi):
  Bedelsiz (free issue):
    %100+         → 9.0-9.5
    %50-99        → 8.0-8.9
    %10-49        → 7.0-7.9
  Bedelli (rights issue):
    Fair to existing shareholders        → 5.5-6.5
    General offering (dilution risk)     → 4.0-5.0

DIVIDEND (Temettu/Kar Payi) — YIELD-BASED SCORING (CRITICAL):
The system pre-calculates dividend yield% (brut TL / current price) when available.
USE YIELD, not just TL. TL amount alone is misleading without share price context.
  Yield ≥%10        → 8.5-9.5 (excellent — attractive, above BIST average)
  Yield %7-10       → 7.8-8.5 (good — strong dividend)
  Yield %5-7        → 7.0-7.7 (above BIST average, positive)
  Yield %3-5        → 6.3-7.0 (BIST average, mild positive)
  Yield %2-3        → 5.7-6.3 (weak positive)
  Yield %1-2        → 5.2-5.7 (neutral+, symbolic)
  Yield %0.5-1      → 4.5-5.2 (weak neutral — insufficient)
  Yield <%0.5       → 3.0-4.5 (NEGATIVE — symbolic dividend, signal that company
                                 doesn't want to pay; retail reaction "neden bu kadar az?")
  Dividend cancelled/none → 3.0-4.5
  First-time dividend     → +0.3 bonus
  YoY dividend increase   → +0.2 bonus

  Examples:
  - EREGL 35 TL share, 5 TL gross = %14.3 yield → 8.8
  - EREGL 35 TL share, 0.50 TL gross = %1.4 yield → 5.4
  - ECZYT 70 TL share, 5.71 TL gross = %8.2 yield → 8.2
  - XYZ 20 TL share, 0.05 TL (5 kurus) = %0.25 yield → 3.5 (NEGATIVE)

PROFIT/LOSS:
  Profit increase >%100 → 9.0+ | %50-100 → 8.0-9.0 | %20-50 → 7.0-8.0 | %5-20 → 6.0-7.0
  Profit decline %5-20 → 4.0-5.0 | %20-50 → 3.0-4.0 | %50+ → 2.0-3.0
  Switch profit→loss → 2.5-3.5 | Consecutive losses → 2.0-3.0

SERMAYE KAYBI (TTK 376):
  376/1 (sermaye %50 kayip)   → 2.0-2.5
  376/2 (sermaye %67 kayip)   → 1.5-2.0
  376/3 (borca batiklik)      → 1.0-1.4

LITIGATION/PENALTIES:
  Lawsuit / equity ratio: >%50 → 1.0-1.5 | %20-50 → 1.5-2.5 | %10-20 → 2.5-3.5
                          %5-10 → 3.5-4.0 | <%5 → 4.0-4.5
  SPK administrative penalty: >10M TL → 2.0-3.0 | 1-10M TL → 3.0-4.0 | <1M TL → 4.0-4.5

AUDITOR OPINION:
  Olumlu (standart)              → 5.0
  Sartli gorus (qualified)       → 3.0-3.5
  Olumsuz gorus                  → 1.5-2.5
  Going concern (sureklilik suphesi) → 1.5-2.5

RELATED PARTY TRANSACTIONS:
  >%10 of total assets → 2.5-3.5 | %5-10 → 3.5-4.0 | <%5 → 4.5-5.0

M&A (Birlesme/Devralma):
  Strategic, high-premium → 8.0-9.5 | Normal → 6.5-8.0
  Subsidiary sale (small) → 5.5-6.5
  Internal consolidation (%100 owned subsidiary) → 5.1-5.5
    Note: Limited financial impact but draws retail attention; usually 1-2 sessions
    upward (sometimes ceiling). Score reflects price-action reality.
  SPK approval (previously announced M&A) → +0.2 momentum bonus

MANAGEMENT CHANGE:
  CEO/GM change → 4.5-5.5 (context-dependent)
  Board change → 4.5-5.0
  Routine appointment → 5.0

CIRCUIT BREAKER (Devre Kesici):
  ALWAYS 5.0 neutral — automatic mechanism, unrelated to fundamentals.

INDEX MEMBERSHIP:
  Index inclusion → 6.5-7.5 | Removal → 3.5-4.5 | Periodic review (no change) → 5.0

═══ TR RETAIL BEHAVIOR LAYER (+/- 0.1-0.2 ADJUSTMENTS) ═══
Apply small adjustments AFTER fundamental score:
  • "Bedelsiz", "birlesme", "devralma" keyword → +0.2 (retail favorite)
  • Small-mid cap (<5B TL mcap) + positive news → +0.2 (high volatility)
  • Mega cap + small amount → -0.1
  • "Erteleme", "inceleniyor", "degerlendirilecek" (vague) → -0.1
  • SPK/BDDK new approval (momentum) → +0.2

═══ HASHTAG RULES ═══
Generate 2-3 hashtags (NO # symbol, do NOT repeat ticker).
Sectors: gayrimenkul, enerji, teknoloji, insaat, gida, saglik, otomotiv, banka, havacilik,
         perakende, celik, kimya, iletisim, savunmasanayi, madencilik, finans, lojistik
Topics: temettu, bedelsiz, sermayeartirimi, karaciklamasi, ihale, sozlesme, ortaklik,
        satis, yatirim, dava, ceza, ihracat, ithalat, m&a, birlesme

═══ CRITICAL RULES ═══
• NO HALLUCINATION: Use only information present in the disclosure text. NEVER fabricate.
• ANTI-NEUTRAL CLUSTERING: Avoid 4.5-5.5 cluster. Differentiate every disclosure.
• SCALE PROPERLY: 100M$ contract ≠ 1M$ contract. Always calibrate by absolute amount.
• NO HEDGING: Don't say "Olumlu/Olumsuz olabilir". Be decisive.
• AVOID DISMISSIVE LANGUAGE: Replace "rutin", "etkisiz", "somut gelisme yok"
  with "kisa vadede sinirli etki, orta vadede X potansiyeli".
• OUTPUT IN TURKISH: Summary, sentiment, hashtags — all Turkish.
• JSON ONLY: Respond with ONLY valid JSON. No markdown, explanations, or commentary.

═══ CALIBRATION EXAMPLES ═══

Ex.1: "THYAO 2025 net kari 42.8 milyar TL, gecen yil 28.1 milyar (%52 artis)"
→ {{"score": 8.7, "category": "finansal", "summary": "...", "hashtags": ["havacilik", "karaciklamasi"]}}

Ex.2: "EREGL hisse basi brut 2.50 TL temettu, gecen yil 1.80 TL (%39 artis)"
→ {{"score": 7.4, "category": "finansal", "summary": "...", "hashtags": ["temettu", "celik"]}}
   (yield-dependent — system provides yield% in TEMETTU VERIM section when applicable)

Ex.3: "SASA 500 milyon TL yeni uretim tesisi yatirimi karari"
→ {{"score": 6.8, "category": "strateji", "summary": "...", "hashtags": ["yatirim", "kimya"]}}

Ex.4: "KOZAL yonetim kurulu uyesi degisikligi"
→ {{"score": 4.8, "category": "bilgi", "summary": "...", "hashtags": ["yonetim", "madencilik"]}}

Ex.5: "BRSAN aleyhine 85M TL dava (ozsermaye 1.2B TL, oran %7)"
→ {{"score": 3.4, "category": "finansal", "summary": "...", "hashtags": ["dava", "celik"]}}

Ex.6: "MPARK son 3 ceyrek zarar; sermaye kaybi TTK 376/1 sinirini asti"
→ {{"score": 2.2, "category": "finansal", "summary": "...", "hashtags": ["sermayekaybi", "saglik"]}}

Ex.7: "ENKAI 3.2 milyar TL'lik Irak dogalgaz santral ihalesi"
→ {{"score": 8.2, "category": "finansal", "summary": "...", "hashtags": ["ihale", "enerji"]}}

Ex.8: "ALFAS %200 bedelsiz sermaye artirimi"
→ {{"score": 9.3, "category": "finansal", "summary": "...", "hashtags": ["bedelsiz", "otomotiv"]}}

Ex.9 (NEW BUSINESS — no amount): "EDATA, D3 Security ile yeni tedarikci anlasmasi"
→ {{"score": 6.1, "category": "strateji", "summary": "Yeni tedarikci iliskisi ticari kapasiteyi destekliyor; kisa vadede sinirli etki ancak orta vadede hizmet portfoy genislemesi potansiyeli.", "hashtags": ["tedarikci", "teknoloji"]}}

Ex.10 (INTERNAL CONSOLIDATION): "CLEBI %100 bagli ortakligi Celebi Kargo'yu devraliyor"
→ {{"score": 5.1, "category": "strateji", "summary": "Grup ici yasal birlesme; mali etki sinirli ancak retail ilgi olusturabilir.", "hashtags": ["birlesme", "lojistik"]}}

Ex.11 (USD CONVERSION): "ENKAI 25 milyar TL'lik petrokimya ihalesi"
→ {{"score": 9.1, "category": "finansal", "summary": "...", "hashtags": ["ihale", "insaat"]}}

Ex.12 (SMALL CONTRACT): "XYZAA 8 milyon TL'lik ihale kazandi"
→ {{"score": 5.7, "category": "finansal", "summary": "...", "hashtags": ["ihale"]}}

Ex.13 (REGISTERED CAPITAL CEILING): "SEGYO kayitli sermaye tavanini 3B'den 5B TL'ye yukseltti"
→ {{"score": 4.9, "category": "bilgi", "summary": "Kayitli sermaye tavani yasal izin; fiili ihrac degil. Gelecekte potansiyel seyreltme riski sinyali.", "hashtags": ["sermayetavani", "gyo"]}}

Ex.14 (LOW DIVIDEND YIELD — NEGATIVE): "ABC 0.10 TL temettu (hisse 18 TL)" — system: yield = %0.56
→ {{"score": 4.2, "category": "finansal", "summary": "Sembolik temettu (verim %0.56) — sirket gercek anlamda kar dagitmiyor sinyali.", "hashtags": ["temettu"]}}

Ex.15 (GOING CONCERN): "DEF denetci raporunda surekliligi konusunda onemli supheler"
→ {{"score": 1.8, "category": "finansal", "summary": "Going concern (sureklilik suphesi) — denetci sirketin mali yapisinda ciddi risk gormus, kritik olumsuz sinyal.", "hashtags": ["sureklilik", "risk"]}}

Respond with ONLY the JSON specified by the user prompt. No other text."""

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

    # ── BILANCO DONEMI FINANSAL TABLOLAR ──
    # Tek tek AI'a verilirse parcali rakamlar yorumlanir, kotu puan cikar.
    # Ileride "Bilanco AI Analizi" feature ile butun olarak yorumlanacak.
    # Su an Notr/5.0 + frontend "ÇOK YAKINDA" badge ile gosteriliyor.
    "özkaynaklar değişim tablosu",
    "ozkaynaklar degisim tablosu",
    "nakit akış tablosu",
    "nakit akis tablosu",
    "finansal durum tablosu",
    "bilanço",
    "bilanco",
    "kar veya zarar tablosu",
    "kâr veya zarar tablosu",
    "kar veya zarar ve diğer kapsamli",
    "kâr veya zarar ve diğer kapsamlı",
    "kapsamlı gelir tablosu",
    "kapsamli gelir tablosu",
    "ara dönem finansal rapor",
    "ara donem finansal rapor",
    "finansal rapor",
    "mali tablo",
]


# ═══════════════════════════════════════════════════════════════════
# PRE-FILTER NOTR PATTERNS — financial override'i bypass et
#
# Bu listedeki basliklar Notr/5.0 atanir, AI'a hic gitmez.
#
# DAHIL EDILEN:
# - Borsa/MKK/KAP operasyonel sistem duyurulari: temettu/sermaye rakami
#   icerse bile bu zaten ONCEDEN ilan edilmis, bu sadece ex-div gunu /
#   kayit tescili / teknik fiyat adjusti. Tekrar haber degil → fiyat
#   etkisi yok.
# - Borclanma araci ihraci: gelir/kar getirmez.
#
# DAHIL EDILMEYEN (AI'a gider):
# - "...Islemlerine Iliskin Bildirim" — sirketin kendi resmi bildirimi
#   olabilir, icerikte ilk karar (ornegin "kar payi dagitilmamasi") yer
#   alabilir. Icerige bakilmali, AI puanlamalidir.
# - "Genel Kurul Karari" / "Yonetim Kurulu Karari" — ilk karar.
# ═══════════════════════════════════════════════════════════════════
_EXECUTION_STAGE_PATTERNS = [
    # NOT: BISTECH / MKK / KAP / Takasbank sistem duyurulari pre-filter'da
    # YOK — cunku icerikte sirkete ozgu temettu/teorik fiyat rakami olabilir
    # ve AI bunlari "onceden ilan edilmis ex-div gunu bildirimi" olarak
    # anlamli bir notr ozet uretebilsin. Pre-filter default text "rutin/idari
    # bildirim" yetersiz kaliyordu. AI prompt'unda BISTECH/MKK kurali var.

    # ── BORCLANMA ARACI IHRACI ──
    # Sirketin BORC alma yetkisi/uygulamasi — gelir/kar getirmez, fiyata
    # pozitif etki yoktur. AI yanlislikla "yeni finansman" diye olumlu
    # puanlayabildiginden pre-filter'da Notr/5.0.
    "tertip ihraç belgesi",
    "tertip ihrac belgesi",
    "ihraç belgesi",
    "ihrac belgesi",
    "borçlanma aracı",
    "borclanma araci",
    "finansman bonosu",
    "özel sektör tahvili",
    "ozel sektor tahvili",
    "banka bonosu",
    "kira sertifikası",
    "kira sertifikasi",
    "varlığa dayalı menkul kıymet",
    "varliga dayali menkul kiymet",
    "vdmk ihrac",
    "vdmk ihraç",
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

    Oncelik sirasi:
    1. EXECUTION-STAGE / SISTEM DUYURUSU → kesin rutin (financial override
       bypass edilir). Ornek: "Merkezi Kayit Kurulusu Duyurusu", "BISTECH
       Pay Piyasasi Duyurusu", "Kar Payi Dagitim Islemlerine Iliskin Bildirim".
       Bunlar onceden alinmis kararin uygulama asamasi → fiyat etkisi yok.
    2. Title routine pattern'i match ediyorsa → rutin (AI atla)
    3. AMA: financial override kelimesi varsa → rutin DEGIL, AI'a git
       (Ornek: "Kar Payi Dagitimina Iliskin Genel Kurul Karari" → ilk karar,
        AI'a git ve temettu olarak puanla)
    """
    if not title:
        return False
    # Turkce 'İ' .lower() ile 'i̇' (i + birlesik ust nokta) verir.
    # Pattern'larla eslesme icin bu birlesik isareti kaldiriyoruz.
    title_norm = title.lower().replace("̇", "").strip()

    # 1) Execution-stage / sistem duyurusu — financial override'i bypass et
    if any(pattern in title_norm for pattern in _EXECUTION_STAGE_PATTERNS):
        return True

    # 2) Once routine pattern var mi
    is_routine = any(pattern in title_norm for pattern in _ROUTINE_TITLE_PATTERNS)
    if not is_routine:
        return False

    # 3) Routine ama financial override kelimesi varsa: AI'a git
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
        return {"sentiment": "Nötr", "impact_score": 5.0, "summary": None, "category": "bilgi", "hashtags": []}

    pos = sum(1 for kw in _POSITIVE_KEYWORDS if kw in text)
    neg = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in text)

    if pos > neg:
        return {"sentiment": "Olumlu", "impact_score": 6.5, "summary": None, "category": "finansal", "hashtags": []}
    elif neg > pos:
        return {"sentiment": "Olumsuz", "impact_score": 3.5, "summary": None, "category": "finansal", "hashtags": []}
    return {"sentiment": "Nötr", "impact_score": 5.0, "summary": None, "category": "bilgi", "hashtags": []}


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
            "sentiment": "Olumlu" | "Olumsuz" | "Nötr",
            "impact_score": float (1.0-10.0),
            "summary": str | None,
        }
    """
    # Bilanco bildirimleri icin AI atla — ilerleyen safhada eklenecek
    if is_bilanco:
        logger.info("KAP Analyzer: Bilanco bildirimi, AI atla (%s)", company_code)
        return {"sentiment": "Nötr", "impact_score": 5.0, "summary": None, "category": "bilgi", "hashtags": []}

    # ── Devre Kesici: AI'ya gonderme, sabit skor + metin don ──
    combined_text = f"{title} {body}".lower()
    if "devre kesici" in combined_text or "tek fiyat emir toplama" in combined_text:
        logger.info("KAP Analyzer: Devre kesici tespit edildi, AI atla (%s)", company_code)
        return {
            "sentiment": "Nötr",
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
            "sentiment": "Nötr",
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
                        sentiment = "Nötr"

                    return {
                        "sentiment": sentiment,
                        "impact_score": impact_score,
                        "summary": recent_news.ai_summary,
                        "category": "finansal" if sentiment != "Nötr" else "bilgi",
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
        return {"sentiment": "Nötr", "impact_score": 5.0, "summary": None, "category": "bilgi", "hashtags": []}

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
1. sentiment: "Olumlu" | "Olumsuz" | "Nötr"
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

    sentiment = result.get("sentiment", "Nötr")
    if sentiment not in ("Olumlu", "Olumsuz", "Nötr"):
        sentiment = "Nötr"

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
