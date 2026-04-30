"""Abacus AI (RouteLLM) — KAP Haber Puanlama & Yorum Servisi V5.

Akis:
1. Telegram'dan Matriks HaberId (kap_notification_id) gelir
2. TradingView'dan haber icerigini cek (matriks:{id}:0/ URL)
3. TradingView basarisizsa → KAP.org.tr direkt erisim (borsapy yontemi)
4. Abacus AI (claude-sonnet-4-6) ile 1.0-10.0 ondalik puan + ozet uret
5. Sonuc: {"score": float, "summary": str, "kap_url": str|None}

V5 Degisiklikler (Arastirma bazli):
- Model: claude-sonnet-4-5 → claude-sonnet-4-6
- Chain-of-thought analiz adimlari (bildirim turu → nicelik → etki)
- Anti-notr-kumeleme direktifi (skorlarin cogu 4-6 arasi OLMAMALI)
- TTK 376 sermaye kaybi seviyeleri (1/2/3)
- 8 kalibrasyon ornegi (tam skor araligini kapsayan)
- KAP ozel durum aciklamalari, is iliskileri, sermaye artirimi ayrimi
- Post-processing: skor dogrulama + ozet kalite filtresi

Icerik Kaynagi (Oncelik sirasi):
- Oncelik 1: TradingView haber sayfasi (matriks ID ile)
- Oncelik 2: KAP.org.tr direkt erisim (borsapy yontemi — bildirim-sorgu-sonuc)
- Fallback: Telegram ham metni (TradingView + KAP basarisizsa)

Hata Toleransi:
- TradingView erisimi basarisiz → KAP.org.tr direkt dene
- KAP.org.tr de basarisiz → Telegram metniyle devam
- AI basarisiz → score=None, summary=None don
- Hicbir hata akisi durdurmaz
"""

import json
import logging
import re

import httpx

logger = logging.getLogger(__name__)

# Abacus AI RouteLLM endpoint — birincil (OpenAI uyumlu)
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"

# Anthropic Claude Sonnet 4 — 2. yedek (direkt API)
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Gemini 2.5 Pro — 3. yedek (OpenAI uyumlu endpoint)
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-pro"

# Versiyon — deploy dogrulama icin
_SCORER_VERSION = "v5-research"

# AI model — claude-sonnet-4-6 (Abacus RouteLLM uzerinden)
_AI_MODEL = "claude-sonnet-4-6"

# Timeouts
_TV_TIMEOUT = 15   # TradingView icin
_AI_TIMEOUT = 30   # AI icin (chain-of-thought analiz icin arttirildi)

# TradingView base URL
TV_NEWS_BASE = "https://tr.tradingview.com/news"

# Browser benzeri headers (TradingView icin)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
}


def _get_api_key() -> str | None:
    """Config'den Abacus API key'i al."""
    try:
        from app.config import get_settings
        key = get_settings().ABACUS_API_KEY
        return key if key else None
    except Exception:
        return None


def _get_anthropic_key() -> str | None:
    """Config'den Anthropic API key'i al."""
    try:
        from app.config import get_settings
        key = getattr(get_settings(), "ANTHROPIC_API_KEY", None)
        return key if key else None
    except Exception:
        return None


def _get_gemini_key() -> str | None:
    """Config'den Gemini API key'i al."""
    try:
        from app.config import get_settings
        key = get_settings().GEMINI_API_KEY
        return key if key else None
    except Exception:
        return None


# -------------------------------------------------------
# ADIM 1: TradingView'dan Icerik Cek (Matriks ID ile)
# -------------------------------------------------------

async def fetch_tradingview_content(matriks_id: str) -> dict | None:
    """TradingView haber sayfasindan icerik cek.

    URL format: https://tr.tradingview.com/news/matriks:{id}:0/

    Args:
        matriks_id: Matriks Haber ID'si (orn: "6225961")

    Returns:
        {
            "full_text": str,   # Haber tam metni
            "tv_url": str,      # TradingView linki
            "title": str,       # Haber basligi
        }
        Basarisizsa None doner.
    """
    if not matriks_id:
        return None

    tv_url = f"{TV_NEWS_BASE}/matriks:{matriks_id}:0/"

    try:
        async with httpx.AsyncClient(
            timeout=_TV_TIMEOUT,
            headers=_HEADERS,
            follow_redirects=True,
        ) as client:
            resp = await client.get(tv_url)

            if resp.status_code != 200:
                logger.warning(
                    "TradingView %s status: %s",
                    matriks_id, resp.status_code,
                )
                return None

            # HTML'den metin cikart
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")

            # Baslik
            title = ""
            title_el = soup.select_one("h1, .title, [class*='title']")
            if title_el:
                title = title_el.get_text(strip=True)

            # Icerik — TradingView haber sayfasi yapisi
            full_text = ""

            # Ana icerik bolumu
            content_el = (
                soup.select_one("article")
                or soup.select_one("[class*='body']")
                or soup.select_one("[class*='content']")
                or soup.select_one("main")
            )

            if content_el:
                # Script ve style etiketlerini kaldir
                for tag in content_el.find_all(["script", "style", "nav", "footer"]):
                    tag.decompose()
                full_text = content_el.get_text(separator="\n", strip=True)

            # Fallback: tum body'den cek
            if not full_text or len(full_text) < 30:
                body = soup.find("body")
                if body:
                    for tag in body.find_all(["script", "style", "nav", "footer", "header"]):
                        tag.decompose()
                    full_text = body.get_text(separator="\n", strip=True)

            # Cok kisa icerik = basarisiz
            if not full_text or len(full_text) < 30:
                logger.warning("TradingView icerik cok kisa (%s): %d karakter", matriks_id, len(full_text or ""))
                return None

            # 5000 karakterle sinirla
            full_text = full_text[:5000]

            # --- Gercek KAP bildirim linkini cikart (cok katmanli arama) ---
            import re as _re
            real_kap_url = None

            # Katman 1: <a> tag'lerinde kap.org.tr linki ara
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "kap.org.tr" in href and "/Bildirim/" in href:
                    real_kap_url = href
                    break

            # Katman 2: Icerik metninden regex ile kap linkini bul
            # /tr/ ve /en/ opsiyonel — bazen kap.org.tr/Bildirim/123 formati olabilir
            _KAP_REGEX = r'https?://(?:www\.)?kap\.org\.tr/(?:(?:tr|en)/)?Bildirim/(\d+)'
            if not real_kap_url:
                kap_match = _re.search(_KAP_REGEX, resp.text)
                if kap_match:
                    # Normalize: her zaman /tr/ ile dondur
                    real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_match.group(1)}"

            # Katman 3: JSON-LD / <script> tag'lerinde kap.org.tr linkini ara
            if not real_kap_url:
                for script_tag in soup.find_all("script"):
                    script_text = script_tag.string or ""
                    if "kap.org.tr" in script_text:
                        kap_match = _re.search(_KAP_REGEX, script_text)
                        if kap_match:
                            real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_match.group(1)}"
                            break

            # Katman 4: Meta tag'lerden KAP linki ara (og:url, canonical, og:see_also)
            if not real_kap_url:
                for meta_tag in soup.find_all("meta"):
                    content = meta_tag.get("content", "")
                    if "kap.org.tr" in content and "Bildirim" in content:
                        kap_match = _re.search(_KAP_REGEX, content)
                        if kap_match:
                            real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_match.group(1)}"
                            break

            # Katman 5: Tam HTML'de genis regex (encoded URL'ler, parcali URL'ler dahil)
            if not real_kap_url:
                kap_match = _re.search(
                    r'kap\.org\.tr[^"\'<>\s]*?Bildirim/(\d+)',
                    resp.text,
                )
                if kap_match:
                    real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_match.group(1)}"

            if real_kap_url:
                # /en/ → /tr/ normalize (TradingView bazen Ingilizce KAP linki veriyor)
                real_kap_url = real_kap_url.replace("/en/Bildirim/", "/tr/Bildirim/")
                real_kap_url = real_kap_url.replace("/en/bildirim/", "/tr/Bildirim/")
                logger.info(
                    "KAP bildirim linki bulundu: matriks:%s → %s",
                    matriks_id, real_kap_url,
                )
            else:
                logger.warning(
                    "KAP bildirim linki bulunamadi: matriks:%s — TradingView fallback kullanilacak",
                    matriks_id,
                )

            logger.info(
                "TradingView icerik basarili: matriks:%s (%d karakter)",
                matriks_id, len(full_text),
            )

            return {
                "full_text": full_text,
                "tv_url": tv_url,
                "title": title,
                "real_kap_url": real_kap_url,
            }

    except Exception as e:
        logger.warning("TradingView icerik hatasi (matriks:%s): %s", matriks_id, e)
        return None


# -------------------------------------------------------
# ADIM 1b: KAP.org.tr Direkt Erisim (borsapy yontemi)
# TradingView basarisiz oldugunda yedek kaynak
# -------------------------------------------------------

# OID cache — {ticker: mkkMemberOid}  (24 saat gecerli)
_oid_cache: dict[str, str] = {}
_oid_cache_time: float = 0
_OID_CACHE_TTL = 86400  # 24 saat

_KAP_BIST_URL = "https://www.kap.org.tr/tr/bist-sirketler"
_KAP_DISCLOSURE_URL = "https://www.kap.org.tr/tr/bildirim-sorgu-sonuc"
_KAP_TIMEOUT = 15


async def _refresh_oid_cache() -> dict[str, str]:
    """KAP bist-sirketler sayfasindan mkkMemberOid haritasini guncelle.

    Next.js SSR HTML'de escaped JSON icinde stockCode ve mkkMemberOid eslesmesi var.
    Sonuc 24 saat cache'lenir.
    """
    import time as _time

    global _oid_cache, _oid_cache_time

    now = _time.time()
    if _oid_cache and (now - _oid_cache_time) < _OID_CACHE_TTL:
        return _oid_cache

    try:
        async with httpx.AsyncClient(timeout=_KAP_TIMEOUT, headers=_HEADERS, follow_redirects=True) as client:
            resp = await client.get(_KAP_BIST_URL)
            if resp.status_code != 200:
                logger.warning("KAP bist-sirketler HTTP %d", resp.status_code)
                return _oid_cache

            # Parse: \"mkkMemberOid\":\"xxx\",...,\"stockCode\":\"THYAO\"
            pattern = (
                r'\\"mkkMemberOid\\":\\"([^\\"]+)\\",'
                r'\\"kapMemberTitle\\":\\"[^\\"]+\\",'
                r'\\"relatedMemberTitle\\":\\"[^\\"]*\\",'
                r'\\"stockCode\\":\\"([^\\"]+)\\"'
            )
            matches = re.findall(pattern, resp.text)

            new_map: dict[str, str] = {}
            for oid, codes_str in matches:
                for code in codes_str.split(","):
                    code = code.strip()
                    if code:
                        new_map[code] = oid

            if new_map:
                _oid_cache = new_map
                _oid_cache_time = now
                logger.info("KAP OID cache guncellendi: %d sirket", len(new_map))
            else:
                logger.warning("KAP bist-sirketler parse sonucu bos")

            return _oid_cache

    except Exception as e:
        logger.warning("KAP OID cache hatasi: %s", e)
        return _oid_cache


async def fetch_kap_direct_content(ticker: str) -> dict | None:
    """KAP.org.tr'den direkt bildirim icerigi cek (borsapy yontemi).

    TradingView fallback'i olarak kullanilir.

    Akis:
    1. bist-sirketler'den mkkMemberOid al (cache'li)
    2. bildirim-sorgu-sonuc?member={OID} ile son bildirimleri cek
    3. En son bildirimi sec
    4. Bildirim sayfasindan icerik cek (fetch_kap_page_content)

    Args:
        ticker: Hisse kodu (orn: "ASTOR")

    Returns:
        {"full_text": str, "kap_url": str, "title": str, "disclosure_index": str}
        Basarisizsa None doner.
    """
    ticker = ticker.upper()

    # Adim 1: OID al
    oid_map = await _refresh_oid_cache()
    oid = oid_map.get(ticker)
    if not oid:
        logger.info("KAP direkt: %s icin OID bulunamadi", ticker)
        return None

    # Adim 2: Son bildirimleri cek
    disc_url = f"{_KAP_DISCLOSURE_URL}?member={oid}"

    try:
        async with httpx.AsyncClient(timeout=_KAP_TIMEOUT, headers=_HEADERS, follow_redirects=True) as client:
            resp = await client.get(disc_url)
            if resp.status_code != 200:
                logger.warning("KAP bildirim-sorgu-sonuc HTTP %d (%s)", resp.status_code, ticker)
                return None

            # Parse: publishDate\":\"29.12.2025 19:21:18\",...disclosureIndex\":1530826,...title\":\"...\"
            pattern = (
                r'publishDate\\":\\"([^\\"]+)\\".*?'
                r'disclosureIndex\\":(\d+).*?'
                r'title\\":\\"([^\\"]+)\\"'
            )
            matches = re.findall(pattern, resp.text, re.DOTALL)

            if not matches:
                logger.info("KAP direkt: %s icin bildirim bulunamadi", ticker)
                return None

            # En son bildirimi al (ilk sirada — varsayilan sira yeniden eskiye)
            date_str, disc_idx, title = matches[0]
            kap_url = f"https://www.kap.org.tr/tr/Bildirim/{disc_idx}"

            logger.info(
                "KAP direkt: %s — %s (%s) [%s]",
                ticker, title[:50], disc_idx, date_str,
            )

    except Exception as e:
        logger.warning("KAP bildirim-sorgu-sonuc hatasi (%s): %s", ticker, e)
        return None

    # Adim 3: Bildirim sayfasindan icerik cek
    try:
        from app.scrapers.kap_all_scraper import fetch_kap_page_content
        content = await fetch_kap_page_content(kap_url)
        if content and len(content) > 30:
            logger.info(
                "KAP direkt icerik basarili: %s — %s (%d karakter)",
                ticker, disc_idx, len(content),
            )
            return {
                "full_text": content[:5000],
                "kap_url": kap_url,
                "title": title,
                "disclosure_index": disc_idx,
            }
        else:
            logger.info("KAP direkt: %s bildirim icerigi yetersiz (%s)", ticker, disc_idx)
            # Icerik yetersiz olsa bile KAP URL'yi don — en azindan link dogru olsun
            return {
                "full_text": "",
                "kap_url": kap_url,
                "title": title,
                "disclosure_index": disc_idx,
            }
    except Exception as e:
        logger.warning("KAP direkt icerik hatasi (%s): %s", ticker, e)
        return None


# -------------------------------------------------------
# ADIM 2: AI Puanlama (Abacus RouteLLM — gpt-4o)
# -------------------------------------------------------

# ── Prompt Override Mekanizması ──
_custom_system_prompt: str | None = None


def get_system_prompt() -> str:
    """Aktif system prompt'u döndürür (custom varsa onu, yoksa default)."""
    return _custom_system_prompt if _custom_system_prompt is not None else _DEFAULT_SYSTEM_PROMPT


def set_system_prompt(new_prompt: str | None) -> None:
    """System prompt'u günceller. None gönderilirse default'a döner."""
    global _custom_system_prompt
    _custom_system_prompt = new_prompt
    logger.info("KAP News Scorer system prompt %s", "güncellendi" if new_prompt else "default'a döndürüldü")


def get_default_system_prompt() -> str:
    """Default (hardcoded) system prompt'u döndürür."""
    return _DEFAULT_SYSTEM_PROMPT


# -------------------------------------------------------
# SYSTEM PROMPT — Chain-of-Thought + Anti-Notr-Kumeleme
# -------------------------------------------------------

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
            → No price impact. Sentiment="Notr", score=4.8-5.2.

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


# -------------------------------------------------------
# ADIM 2: AI Puanlama (Abacus RouteLLM)
# -------------------------------------------------------

async def score_news(
    ticker: str,
    raw_text: str,
    tv_content: str | None = None,
    kap_url: str | None = None,
) -> dict:
    """Haberi AI ile puanla ve yorumla.

    Args:
        ticker: Hisse kodu (orn: "ENDAE")
        raw_text: Telegram mesajinin ham metni
        tv_content: TradingView'dan cekilmis bildirim tam metni (varsa)
        kap_url: TradingView/KAP linki (varsa)

    Returns:
        {"score": float|None, "summary": str|None, "kap_url": str|None}
        Hata durumunda score+summary None olur — akis kirilmaz.
    """
    api_key = _get_api_key()
    anthropic_key = _get_anthropic_key()
    gemini_key = _get_gemini_key()
    if not api_key and not anthropic_key and not gemini_key:
        logger.error("AI News Scorer: API key yok (Abacus/Claude/Gemini) — devre disi! (%s)", ticker)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}

    # TradingView icerigi varsa birincil kaynak, yoksa Telegram metni
    has_tv = bool(tv_content and len(tv_content.strip()) > 50)
    content = tv_content if has_tv else raw_text
    content = content[:5000] if content else ""  # claude-sonnet-4-6 uzun metin isleyebilir

    if not content.strip():
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}

    # ── Devre Kesici: AI'ya gonderme, sabit skor + metin don ──
    if re.search(r"devre\s*kesici|tek\s*fiyat\s*emir\s*toplama", content.lower()):
        logger.info("Devre kesici tespit edildi, AI atlaniyor (%s)", ticker)
        return {
            "score": 5.0,
            "summary": f"Borsa Istanbul, {ticker} hissesinde yasanan ani ve yuksek fiyat hareketi nedeniyle Pay Bazinda Devre Kesici uygulamasinin devreye girdigini bildirmistir. Bu bildirim, sirketin temel faaliyetleriyle ilgili bir gelisme olmayip, hisse senedinde anlik yuksek volatiliteyi kontrol altina almayi amaclayan standart bir borsa mekanizmasidir.",
            "kap_url": kap_url,
            "hashtags": ["devrekesici"],
        }

    # Kaynak bilgisini prompt'a ekle
    source_info = "KAP Bildirim Tam Metni (TradingView)" if has_tv else "Telegram Kanal Ozeti (detay erisilemedi)"

    prompt = f"""Borsa Istanbul (BIST) KAP bildirimi analizi.

Hisse: {ticker}
Kaynak: {source_info}

--- ICERIK BASLANGIC ---
{content}
--- ICERIK BITIS ---

GOREV:
1. Haberi yatirimci bakis acisiyla Turkce EN AZ 3, EN FAZLA 5 cumle ile ozetle.
2. Onemli rakamlari ozete dahil et (tutar, oran, yuzde).
3. Haberin ne oldugunu, sirket icin ne anlama geldigini ve yatirimci icin neden onemli oldugunu acikla.

HASHTAG KURALLARI:
- 2-3 adet Twitter hashtag uret (# isareti OLMADAN)
- Sirket ticker'i ({ticker}) zaten ekleniyor, onu TEKRAR verme
- Sektor ve konu bazli sec: gayrimenkul, enerji, teknoloji, insaat, gida, saglik, otomotiv,
  ihracat, ithalat, temettü, bedelsiz, sermayeartirimi, karaciklamasi, ihale, sozlesme,
  ortaklik, satis, alim, uretim, yatirim, dava, ceza, madencilik, finans, banka,
  havacilik, perakende, celik, kimya, iletisim, savunmasanayi vb.

SADECE asagidaki JSON formatinda yanit ver:
{{"score": 7.3, "category": "finansal", "summary": "3-5 cumle Turkce ozet.", "hashtags": ["sektor", "konu"]}}

NOTLAR:
- "category" zorunlu: "finansal" / "strateji" / "bilgi" (system prompt'taki rehbere gore)
- "score" 1.0-10.0 arasi 0.1 hassasiyet
- "summary" 3-5 cumle Turkce, onemli rakamlari icermeli
- "hashtags" 2-3 adet, # isareti olmadan, ticker tekrarlanmasin"""

    messages = [
        {"role": "system", "content": get_system_prompt()},
        {"role": "user", "content": prompt},
    ]
    payload_base = {
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 4096,  # Gemini 2.5 thinking token yiyor
    }

    # ── Birincil: Abacus AI ──
    text = None
    provider_used = None

    if api_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                resp = await client.post(
                    _ABACUS_URL,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json={**payload_base, "model": _AI_MODEL},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    text = data["choices"][0]["message"]["content"].strip()
                    provider_used = "Abacus"
                else:
                    logger.warning(
                        "AI News Scorer: Abacus HTTP %s (%s) — %s",
                        resp.status_code, ticker, resp.text[:200],
                    )
        except Exception as e:
            logger.warning("AI News Scorer: Abacus hata (%s) — %s", ticker, e)

    # ── Yedek: Anthropic Claude Sonnet 4 (direkt API) ──
    if not text and anthropic_key:
        try:
            # Anthropic Messages API formatı (OpenAI'den farklı)
            system_content = messages[0]["content"] if messages and messages[0]["role"] == "system" else ""
            user_content = messages[-1]["content"] if messages else ""

            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                resp = await client.post(
                    _ANTHROPIC_URL,
                    headers={
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": _CLAUDE_MODEL,
                        "max_tokens": 4096,  # Gemini 2.5 thinking token yiyor
                        "system": system_content,
                        "messages": [{"role": "user", "content": user_content}],
                        "temperature": 0.1,
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for block in data.get("content", []):
                        if block.get("type") == "text":
                            text = block.get("text", "").strip()
                            break
                    provider_used = "Claude-Sonnet"
                else:
                    logger.error(
                        "AI News Scorer: Claude HTTP %s (%s) — %s",
                        resp.status_code, ticker, resp.text[:200],
                    )
        except Exception as e:
            logger.error("AI News Scorer: Claude hata (%s) — %s", ticker, e)

    # ── 3. Yedek: Gemini 2.5 Pro ──
    if not text and gemini_key:
        try:
            async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                resp = await client.post(
                    _GEMINI_URL,
                    headers={
                        "Authorization": f"Bearer {gemini_key}",
                        "Content-Type": "application/json",
                    },
                    json={**payload_base, "model": _GEMINI_MODEL},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    text = data["choices"][0]["message"]["content"].strip()
                    provider_used = "Gemini-Pro"
                else:
                    logger.error(
                        "AI News Scorer: Gemini HTTP %s (%s) — %s",
                        resp.status_code, ticker, resp.text[:200],
                    )
        except Exception as e:
            logger.error("AI News Scorer: Gemini hata (%s) — %s", ticker, e)

    if not text:
        logger.error("AI News Scorer: Tum AI providerlar basarisiz (%s)", ticker)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}

    try:
        from app.services.ai_json_helper import safe_parse_json

        result = safe_parse_json(text, required_key="score")
        if result is None:
            logger.error("AI News Scorer: JSON parse basarisiz (%s) — icerik: %s", ticker, text[:200])
            return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}

        score = result.get("score")
        summary = result.get("summary")
        hashtags = result.get("hashtags", [])
        category = result.get("category", "bilgi")
        if category not in ("finansal", "strateji", "bilgi"):
            category = "bilgi"

        # ─── Score validation: 1.0-10.0 arasinda olmali (ondalik) ───
        if isinstance(score, (int, float)) and 1.0 <= score <= 10.0:
            score = round(float(score), 1)  # 1 ondalik basamak
        else:
            logger.warning("AI News Scorer: Gecersiz skor=%s (%s)", score, ticker)
            score = None

        # ─── Summary validation ───
        if not isinstance(summary, str) or not summary.strip():
            summary = None
        elif summary:
            # Hallusinasyon filtresi: ozette haber metniyle ilgisiz bilgi olmasin
            _HALLUC_PATTERNS = [
                "bilgi bulunamadi", "detay mevcut degil", "aciklama yapilmadi",
                "yeterli bilgi yok", "net bir degerlendirme",
            ]
            for pat in _HALLUC_PATTERNS:
                if pat in summary.lower():
                    summary = summary.replace(pat, "").strip()

        # ─── Hashtags validation — max 3, her biri string ───
        if isinstance(hashtags, list):
            clean_tags = []
            for tag in hashtags[:3]:
                if isinstance(tag, str) and tag.strip():
                    clean = tag.strip().lstrip("#").replace(" ", "").lower()
                    if clean and clean.upper() != ticker.upper() and len(clean) <= 25:
                        clean_tags.append(clean)
            hashtags = clean_tags
        else:
            hashtags = []

        # ─── Post-processing: bildirim tipi bazli skor dogrulama ───
        if score is not None and content:
            score = _validate_score_against_content(score, content, ticker)

        logger.info(
            "AI News Scorer [%s]: %s — skor=%s, kaynak=%s, hashtags=%s, ozet=%s",
            provider_used, ticker, score,
            "TradingView" if has_tv else "Telegram",
            hashtags,
            (summary[:60] + "...") if summary and len(summary) > 60 else summary,
        )

        return {"score": score, "summary": summary, "kap_url": kap_url, "hashtags": hashtags, "category": category}

    except json.JSONDecodeError as e:
        logger.error("AI News Scorer: JSON parse hatasi (%s) — %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}
    except Exception as e:
        logger.error("AI News Scorer: Beklenmeyen hata (%s) — %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}


# -------------------------------------------------------
# POST-PROCESSING: Skor Dogrulama
# -------------------------------------------------------

# Negatif bildirim kaliplari — skor tavan sinirlamasi
_CRITICAL_NEGATIVE_PATTERNS = [
    (r"(?:ttk|türk ticaret kanunu)\s*(?:madde\s*)?376\s*/?\s*3|borca\s*bat[ıi]k", 1.4),
    (r"(?:ttk|türk ticaret kanunu)\s*(?:madde\s*)?376\s*/?\s*2|sermaye(?:nin)?\s*(?:üçte ikisi|2/3|%67)", 2.0),
    (r"(?:ttk|türk ticaret kanunu)\s*(?:madde\s*)?376\s*/?\s*1|sermaye(?:nin)?\s*(?:yarısı|%50)", 2.5),
    (r"iflas\s*(?:basvur|karar|talep|ilan)", 1.5),
    (r"i[sş]lem(?:e)?\s*(?:kapat|durdur|yasak)", 2.0),
    (r"teknik\s*iflas", 1.8),
    (r"going\s*concern|süreklili[gğ]e?\s*(?:iliskin\s*)?(?:şüphe|belirsizlik)", 2.5),
]

# Pozitif bildirim kaliplari — skor taban garantisi
_STRONG_POSITIVE_PATTERNS = [
    (r"bedelsiz\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:1\d{2}|[2-9]\d{2}|\d{4,})", 9.0),  # %100+ bedelsiz
    (r"bedelsiz\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:[5-9]\d)", 8.0),  # %50-99 bedelsiz
    (r"(?:net\s*)?k[aâ]r[ıi]?\s*%\s*(?:1\d{2}|[2-9]\d{2}|\d{4,})\s*art", 9.0),  # %100+ kar artisi
    (r"rekor\s*(?:k[aâ]r|gelir|has[ıi]lat)", 8.0),
]


def _validate_score_against_content(score: float, content: str, ticker: str) -> float:
    """Icerik patirnlerine gore skoru dogrular ve gerekirse duzeltir.

    Kritik negatif haberler icin skoru tavan sinirlar,
    guclu pozitif haberler icin taban garantisi uygular.
    Notr bildirimler (devre kesici vb.) icin 5.0'a ceker.
    """
    content_lower = content.lower()

    # ── Nötr bildirimler — skor 5.0 olmali ──
    _NEUTRAL_PATTERNS = [
        r"devre\s*kesici",
        r"pay\s*baz[ıi]nda\s*devre\s*kesici",
        r"tek\s*fiyat\s*emir\s*toplama",
    ]
    for pattern in _NEUTRAL_PATTERNS:
        if re.search(pattern, content_lower):
            if score < 4.5 or score > 5.5:
                logger.info(
                    "Skor dogrulama (notr): %s skor %.1f → 5.0 (devre kesici)",
                    ticker, score,
                )
                return 5.0
            return score

    # Kritik negatif bildirimler — skor asla tavanin uzerine cikmamali
    for pattern, max_score in _CRITICAL_NEGATIVE_PATTERNS:
        if re.search(pattern, content_lower):
            if score > max_score:
                logger.info(
                    "Skor dogrulama: %s skor %.1f → %.1f (pattern: %s)",
                    ticker, score, max_score, pattern[:30],
                )
                return max_score

    # Guclu pozitif bildirimler — skor asla tabanin altina dusmemeli
    for pattern, min_score in _STRONG_POSITIVE_PATTERNS:
        if re.search(pattern, content_lower):
            if score < min_score:
                logger.info(
                    "Skor dogrulama: %s skor %.1f → %.1f (pattern: %s)",
                    ticker, score, min_score, pattern[:30],
                )
                return min_score

    return score


# -------------------------------------------------------
# MASTER FONKSIYON: TradingView Icerik + AI Puanla
# -------------------------------------------------------

async def analyze_news(
    ticker: str,
    raw_text: str,
    matriks_id: str | None = None,
) -> dict:
    """Tam AI analiz pipeline'i: TradingView → KAP direkt → Telegram.

    Oncelik sirasi:
    1. TradingView'dan tam haber metni cek (Matriks ID ile)
    2. TradingView basarisizsa → KAP.org.tr direkt erisim (borsapy yontemi)
    3. Ikisi de basarisizsa → Telegram ham metniyle AI puanlama

    Args:
        ticker: Hisse kodu
        raw_text: Telegram ham mesaj metni
        matriks_id: Telegram mesajindaki kap_notification_id (Matriks HaberId)

    Returns:
        {
            "score": float | None,
            "summary": str | None,
            "kap_url": str | None,
            "hashtags": list[str],
        }
    """
    tv_content = None
    kap_url = None

    # ── Oncelik 1: TradingView'dan icerik cek (Matriks ID varsa) ──
    if matriks_id:
        # Fallback olarak TradingView linki (gercek KAP linki bulunursa degisir)
        kap_url = f"https://tr.tradingview.com/news/matriks:{matriks_id}:0/"

        try:
            tv_result = await fetch_tradingview_content(matriks_id)
            if tv_result and tv_result.get("full_text"):
                tv_content = tv_result["full_text"]
                # Gercek KAP bildirim linkini kullan (TradingView'dan cikarildi)
                if tv_result.get("real_kap_url"):
                    kap_url = tv_result["real_kap_url"]
                    logger.info(
                        "Gercek KAP linki kullaniliyor: %s → %s",
                        ticker, kap_url,
                    )
                logger.info(
                    "TradingView eslestirme basarili: %s → matriks:%s (%d karakter)",
                    ticker, matriks_id, len(tv_content),
                )
        except Exception as e:
            logger.warning("TradingView hatasi (%s): %s", ticker, e)

    # ── Oncelik 2: KAP.org.tr direkt erisim (TradingView basarisizsa) ──
    if not tv_content and ticker:
        try:
            kap_result = await fetch_kap_direct_content(ticker)
            if kap_result:
                # KAP URL'yi her zaman al (icerik bos olsa bile link dogru)
                if kap_result.get("kap_url"):
                    kap_url = kap_result["kap_url"]

                # Icerik yeterli mi?
                if kap_result.get("full_text") and len(kap_result["full_text"]) > 30:
                    tv_content = kap_result["full_text"]
                    logger.info(
                        "KAP direkt fallback basarili: %s → %s (%d karakter)",
                        ticker, kap_url, len(tv_content),
                    )
                else:
                    logger.info(
                        "KAP direkt: %s — URL bulundu (%s) ama icerik yetersiz, Telegram ile devam",
                        ticker, kap_url,
                    )
        except Exception as e:
            logger.warning("KAP direkt hatasi (%s): %s", ticker, e)

    # ── Fallback log ──
    if not tv_content:
        logger.info(
            "Icerik kaynagi: Telegram ham metni (%s) — TradingView ve KAP direkt basarisiz",
            ticker,
        )

    # ── Adim 3: AI puanlama (TradingView/KAP icerigi veya Telegram metni ile) ──
    try:
        result = await score_news(ticker, raw_text, tv_content, kap_url)
        return result
    except Exception as e:
        logger.warning("AI puanlama hatasi (%s): %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}
