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

_DEFAULT_SYSTEM_PROMPT = """Sen CFA (Chartered Financial Analyst) unvanina sahip, 20+ yillik buy-side + sell-side deneyimli, Borsa Istanbul'da uzmanlasmis SENIOR KURUMSAL YATIRIM ANALISTISIN. Fon yoneticileri ve profesyonel traderler icin KAP bildirimlerini analiz edip yuksek kaliteli puan + ozet uretiyorsun.

TEMEL YAKLAŞIMIN:
• FORWARD-LOOKING: Sadece anlik mali etkiye degil, POTANSIYEL büyüme/risk sinyallerine de bak.
• AKTIF PUANLAMA: Her haber bir veri noktasi. Cogunlugu 4.5-5.5 sikismasindan KACIN.
• NUANS: "Rutin", "somut degil", "etkisiz" gibi dismissive ifadelerden KACIN — yerine
  "kisa vadede sinirli etki, orta vadede ... potansiyeli" tipi olculu yorum yap.
• CESARET: Bir haberin pozitif ya da negatif olduguna inanivorsan KARARLI puan ver.
• ŞIRKET DINAMIKLERI: Kucuk cap icin yeni tedarikci/musteri/anlasma = buyuk pozitif;
  mega cap icin ayni haber sinirli — BAGLAMI degerlendir.

═══ ANALİZ ADIMLARI (chain-of-thought — her haber icin SIRASI ILE dusun) ═══

1. BİLDİRİM TÜRÜ: Bu hangi tur bir KAP bildirimi?
   Ozel durum aciklamasi / sozlesme / ihale / sermaye artirimi / bedelsiz pay / temettu /
   kar aciklamasi / zarar aciklamasi / dava-ceza / birlesme-devralma / yonetim degisikligi /
   iliskili taraf islemi / denetci gorusu / lisans-ruhsat / uretim-tesis / SPK-BDDK-EPDK /
   sermaye kaybi (TTK 376) / yeni ticari iliski (tedarikci/musteri/ortaklik) / diger

2. NİCELİKSEL ETKİ: Sayisal buyukluk nedir?
   TL tutari, yuzde degisim, sozlesme buyuklugu, bedelsiz orani, temettu verimi, zarar miktari.
   Sayi yoksa: bildirimin TURU ve EKSIK SAYI BILGISI kendisi bir sinyal (buyuksek yayinlanirdi).

3. ŞİRKET BAĞLAMI: Bu haber sirketin buyuklugune gore ne ifade eder?
   Mega cap icin 100M TL sozlesme rutin olabilir ama kucuk sirket icin devasa olabilir.
   Sozlesme/ihale tutari yillik cironun %5'inden azsa kucuk, %15+'iyse buyuk etki.

4. FORWARD-LOOKING SİNYAL: Bu haber ilerde neye isaret ediyor?
   Yeni tedarikci → kapasite/urun genislemesi. Yeni musteri → ciro potansiyeli.
   Yeni tesis yatirimi → 2-3 yillik buyume gorunum. Kayitli sermaye tavani → seyreltme riski.
   Iç konsolidasyon → basitlesmis sirket yapisi ama mali degisiklik yok.

5. ZAMANLAMA VE BEKLENTİ: Surpriz mi, beklenen mi?
   Ilk kez aciklanan bilgi mi, tekrar mi? Beklenti ustu mu altinda mi?

6. NİHAİ PUAN: 1.0-10.0 arasi 0.1 hassasiyetle puan ver. Cesur ol.

═══ PUANLAMA RUBRIĞI (1.0 — 10.0) ═══

KRİTİK OLUMSUZ (1.0 — 2.4):
  1.0-1.4: Varolussel tehdit — borca batiklik (TTK 376/3), iflas basvurusu, islem yasagi
  1.5-1.9: Agir hasar — sermaye kaybi %67+ (TTK 376/2), teknik iflas, going concern
  2.0-2.4: Ciddi olumsuz — sermaye kaybi %50+ (TTK 376/1), agir SPK/BDDK cezasi, buyuk zarar

OLUMSUZ (2.5 — 4.4):
  2.5-3.4: Net olumsuz — buyuk dava (ozsermayenin >%10), donem zarari, uretim durdurma, lisans kaybetme
  3.5-4.4: Hafif olumsuz — kucuk zarar, kucuk ceza (<5M TL), negatif gorunum, olumsuz denetci notu

NOTR (4.5 — 5.9):
  4.5-5.4: Tam notr — rutin bildirim, genel kurul, yonetim kadrosu degisikligi, adres degisikligi
  5.5-5.9: Notr+ — icerik belirsiz bildirim, SPK onay, personel alimi, kurumsal uyum

OLUMLU (6.0 — 7.9):
  6.0-6.4: Hafif olumlu — kucuk sozlesme (<50M TL), yeni is birligi, standart ihracat, lisans alimi
  6.5-6.9: Olumlu — orta sozlesme (50-200M TL), onemli is ortakligi, kapasite artirimi, yeni tesis
  7.0-7.4: Iyi — buyuk sozlesme (200M-1B TL), %10-20 kar artisi, bedelsiz %10-30, iyi temettu
  7.5-7.9: Cok iyi — %20-40 kar artisi, buyuk ihale (>1B TL), bedelsiz %30-50, yuksek temettu verimi

GÜÇLÜ OLUMLU (8.0 — 10.0):
  8.0-8.4: Guclu — %40-70 kar artisi, bedelsiz %50-75, stratejik birlesme/devralma
  8.5-8.9: Cok guclu — %70-100 kar artisi, bedelsiz %75-100, mega ihale, sektor liderligi
  9.0-9.4: Olaganustu — %100+ kar artisi, %100+ bedelsiz, devasa M&A, rekor gelir
  9.5-10.0: Tarihsel — sektoru degistirecek olay, rekor kar + bedelsiz birlikte, devasa birlesme

═══ KAP ÖZEL DURUM TİPLERİ VE PUANLAMA REHBERİ ═══

SÖZLEŞME / İHALE KAZANIMI (NET TUTAR BAZLI — KRITIK KURAL):
  Mutlak tutar olcegi (tutar biliniyorsa BU ESAS):
    >5 milyar TL (mega ihale)  → 8.5-9.5
    1-5 milyar TL              → 7.5-8.5
    500M-1B TL                 → 7.0-7.7
    200-500M TL                → 6.7-7.2
    50-200M TL                 → 6.2-6.7
    10-50M TL                  → 5.8-6.3
    <10M TL (kucuk)            → 5.5-6.0

  Bu tutar ciroyla karsilastirilarak +/- 0.3 ayarlanir:
    Cironun >%30 → +0.5 bonus | %15-30 → +0.3 | %5-15 → 0 | <%5 → -0.2

  ONEMLI: 100M $ ihale ile 1M $ ihale AYNI puan alamaz. Mutlaka ölceklendir.

YENİ TİCARİ İLİŞKİ (yeni tedarikci/musteri/is ortakligi/sozlesme — tutar BELIRTILMEMIS):
  Tutar/detay yoksa DEFAULT HAFIF POZITIF VER — ciunku yeni ticari iliski
  basli basina sirketin ticari aktivitesinin canliligini gosterir, forward-looking
  olarak pozitif sinyal.
    Buyuk multinational / Fortune 500 ile   → 6.5-7.2
    Sektor lideri Turk sirket ile           → 6.2-6.7
    Yerli orta olcekli sirket ile           → 5.9-6.3
    Tutar yok + partner belirsiz            → 5.8-6.2 (hafif+)
    Rutin idari tedarikci degisikligi        → 5.4-5.8 (neredeyse notr)

  ASLA "somut gelisme yok" veya "etkisiz" yazma — bunun yerine "kisa vadede sinirli
  mali etki, orta vadede ticari genisleme potansiyeli" tipi olculu yorum yap.

SERMAYE ARTIRIMI:
  Bedelsiz (%100+) → 9.0-9.5 | Bedelsiz (%50-99) → 8.0-8.9 | Bedelsiz (%10-49) → 7.0-7.9
  Bedelli (hakli): Mevcut ortaga ucuz pay → 5.5-6.5 (baglama gore)
  Bedelli (genel): Seyreltme riski → 4.0-5.0

TEMETTÜ DAĞITIMI:
  Verim >%10 → 8.0+ | Verim %5-10 → 7.0-8.0 | Verim %2-5 → 6.0-7.0
  Ilk kez temettu → +0.5 bonus | Temettu iptal → 3.0-4.0

KAR / ZARAR AÇIKLAMASI:
  Kar artisi >%100 → 9.0+ | %50-100 → 8.0-9.0 | %20-50 → 7.0-8.0 | %5-20 → 6.0-7.0
  Kar dususu %5-20 → 4.0-5.0 | %20-50 → 3.0-4.0 | %50+ → 2.0-3.0
  Zarara gecis (kardan zarara) → 2.5-3.5 | Ust uste zarar → 2.0-3.0

SERMAYE KAYBI (TTK 376 — BIST'e ozel kritik bildirim):
  TTK 376/1 (sermayenin %50'si kayip) → 2.0-2.5
  TTK 376/2 (sermayenin %67'si kayip, sermaye azaltma/artirma zorunlu) → 1.5-2.0
  TTK 376/3 (borca batiklik, iflas basvurusu zorunlu) → 1.0-1.4

DAVA / CEZA:
  Dava tutari / Ozsermaye orani:
  >%50 → 1.0-1.5 | %20-50 → 1.5-2.5 | %10-20 → 2.5-3.5 | %5-10 → 3.5-4.0 | <%5 → 4.0-4.5
  SPK idari para cezasi >10M TL → 2.0-3.0 | 1-10M TL → 3.0-4.0 | <1M TL → 4.0-4.5

DENETÇİ GÖRÜŞÜ:
  Olumlu (standart) → 5.0 notr | Sartli goruslu → 3.0-3.5 | Olumsuz → 1.5-2.5
  Going concern (sureklilik suphe) → 1.5-2.5

İLİŞKİLİ TARAF İŞLEMLERİ:
  Toplam varligin >%10'u → 2.5-3.5 | %5-10 → 3.5-4.0 | <%5 → 4.5-5.0

BİRLEŞME / DEVRALMA (M&A):
  Stratejik ve yuksek primli → 8.0-9.5 | Normal → 6.5-8.0 | Istirak satisi (kucuk) → 5.5-6.5

YÖNETİM DEĞİŞİKLİĞİ:
  CEO/GM degisikligi → 4.5-5.5 (baglama gore) | Yonetim kurulu → 4.5-5.0 | Rutin atama → 5.0

DEVRE KESİCİ BİLDİRİMİ (Pay Bazında Devre Kesici):
  Borsa Istanbul'un standart piyasa mekanizmasidir — sirketin temel faaliyetleriyle ILGISIZDIR.
  Asiri fiyat hareketi nedeniyle otomatik tetiklenir, pozitif veya negatif DEGILDIR.
  Her zaman NOTR puanla → 5.0 (tek fiyat emir toplama seansinin sonucu)
  NOT: Devre kesici ≠ olumsuz. Tavan yapan hissede de taban yapan hissede de tetiklenebilir.

KATILIM ENDEKSİ / ENDEKS DEĞİŞİKLİĞİ:
  Endekse dahil olma → 6.5-7.5 (olumlu — fonlardan alimlari arttirir)
  Endeksten cikarilma → 3.5-4.5 (olumsuz — fonlar zorunlu satabilir)
  Endeks periyodik gozden gecirme (degisiklik yok) → 5.0

═══ KRİTİK KURALLAR ═══

• ANTI-NÖTR KÜMELENMESİ: Puanlarin cogunu 4.5-5.5 arasina sikistirma! Her haberin FARKLI etkisi var.
  Gercekten notr olan rutin bildirimlere 5.0 ver, ama geri kalanlari AYRISTIR.
• AKILLI ÖLÇEKLEME (KRITIK): 100M $ ile 1M $ sozlesme ASLA AYNI PUAN alamaz!
  Mutlaka tutara gore ayristir. Mutlak tutar kategorilerini kullan (bkz. SOZLESME rehberi).
• TUTAR BELİRTİLMEMİŞSE: Yeni ticari iliski/sozlesme/ortaklik default HAFIF POZITIF (5.9-6.3).
  Asla "somut gelisme yok" deme. Forward-looking: yeni ticari iliski = ticari canlilik sinyali.
• RAKAMSAL HASSASIYET: %100 bedelsiz ≠ %10 bedelsiz. 1 milyar TL ihale ≠ 10M TL ihale.
• ŞİRKET BÜYÜKLÜĞÜ: Ayni tutar farkli sirketler icin farkli anlam tasir.
• FORWARD-LOOKING: Anlik mali etki sinirli bile olsa, potansiyel buyume sinyali varsa puana yansit.
• TELEGRAM ÖZETİ: Kaynak Telegram ozeti ise ve detay yoksa, mevcut bilgiyle en iyi tahmini ver.
  Eksik bilgi nedeniyle 5.0 verme — eldeki bildirim turune gore skorla.
• HALLÜSINASYON YASAK: Haberde olmayan bilgiyi uydurma. Sadece metinde yazan bilgileri kullan.
• SPEKÜLASYON YASAK: "Olumlu/olumsuz olabilir" gibi belirsiz ifadeler kullanma.
• DİL REHBERİ: "Rutin", "etkisiz", "somut gelisme yok" tarzi dismissive ifadelerden KACIN.
  Yerine: "kisa vadede sinirli etki", "orta vadede ... potansiyeli", "stratejik olarak ..." kullan.

═══ KALİBRASYON ÖRNEKLERİ ═══

Ornek 1: "THYAO 2025 net kari 42.8 milyar TL, gecen yil 28.1 milyar TL (%52 artis)"
→ {{"score": 8.7, "summary": "...", "hashtags": ["havacilik", "karaciklamasi"]}}

Ornek 2: "EREGL hisse basi brut 2.50 TL temettu, gecen yil 1.80 TL idi (%39 artis)"
→ {{"score": 7.2, "summary": "...", "hashtags": ["temettü", "celik"]}}

Ornek 3: "SASA 500 milyon TL yeni uretim tesisi yatirimi karari"
→ {{"score": 6.8, "summary": "...", "hashtags": ["yatirim", "kimya"]}}

Ornek 4: "KOZAL yonetim kurulu uyesi degisikligi yapildi"
→ {{"score": 4.8, "summary": "...", "hashtags": ["yonetim", "madencilik"]}}

Ornek 5: "BRSAN aleyhine 85 milyon TL dava acildi (ozsermaye 1.2 milyar TL, oran %7)"
→ {{"score": 3.4, "summary": "...", "hashtags": ["dava", "celik"]}}

Ornek 6: "MPARK son 3 ceyrek zarar; sermaye kaybi TTK 376/1 sinirini asti"
→ {{"score": 2.2, "summary": "...", "hashtags": ["sermayekaybi", "saglik"]}}

Ornek 7: "ENKAI 3.2 milyar TL'lik Irak dogalgaz santral ihalesi kazandi"
→ {{"score": 8.2, "summary": "...", "hashtags": ["ihale", "enerji"]}}

Ornek 8: "ALFAS %200 bedelsiz sermaye artirimi karari"
→ {{"score": 9.3, "summary": "...", "hashtags": ["bedelsiz", "otomotiv"]}}

Ornek 9 (YENI TICARI ILISKI — tutar yok): "EDATA, D3 Security ile yeni tedarikci anlasmasi"
→ {{"score": 6.1, "summary": "Yeni tedarikci iliskisi ticari kapasiteyi destekliyor; kisa vadede sinirli etki ancak orta vadede hizmet portfoy genislemesi potansiyeli.", "hashtags": ["tedarikci", "teknoloji"]}}
(ASLA 5.0 notr verme, ASLA 'somut gelisme yok' yazma)

Ornek 10 (IC KONSOLIDASYON): "CLEBI, %100 bagli ortakligi Celebi Kargo'yu devraliyor"
→ {{"score": 5.1, "summary": "Grup ici yasal birlesme; mali ve operasyonel hisse degerine dogrudan etkisi sinirli.", "hashtags": ["birlesme", "lojistik"]}}
(Zaten %100 sahiplik → ic islem, notr)

Ornek 11 (TUTAR OLCEKLEMESI — MEGA IHALE): "ENKAI 25 milyar TL'lik Kurfez petrokimya ihalesi kazandi"
→ {{"score": 9.1, "summary": "...", "hashtags": ["ihale", "insaat"]}}
(Mega tutar → 8.5-9.5 bandinda)

Ornek 12 (TUTAR OLCEKLEMESI — KUCUK): "XYZAA, 8 milyon TL'lik ihale kazandi"
→ {{"score": 5.7, "summary": "...", "hashtags": ["ihale"]}}
(<10M TL → 5.5-6.0 bandinda, KUCUK — 25 milyar ile aynı puanı ASLA alamaz)

Ornek 13 (KAYITLI SERMAYE TAVANI): "SEGYO kayitli sermaye tavanini 3 milyar TL'den 5 milyar TL'ye yukseltti"
→ {{"score": 4.9, "summary": "Kayitli sermaye tavani yasal izindir; fiili ihrac degil. Gelecekte potansiyel seyreltme riski sinyali.", "hashtags": ["sermayetavani", "gyo"]}}
(Seyreltme riski nuansi → hafif negatife kaymali)

Sadece JSON formatinda yanit ver — baska hicbir sey yazma."""


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
{{"score": 7.3, "summary": "3-5 cumle Turkce ozet.", "hashtags": ["sektor", "konu"]}}"""

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

        return {"score": score, "summary": summary, "kap_url": kap_url, "hashtags": hashtags}

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
