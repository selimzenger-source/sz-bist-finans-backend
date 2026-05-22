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

import asyncio
import json
import logging
import re
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

# Abacus AI RouteLLM endpoint — birincil (OpenAI uyumlu)
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"

# Anthropic Claude Sonnet 4 — 2. yedek (direkt API)
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Gemini 2.5 Pro — 3. yedek (OpenAI uyumlu endpoint)
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"  # Pro yerine Flash — KAP scoring icin yeterli, 10x daha ucuz

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

            # TradingView uyelik duvari tespiti — icerik AI'a gitmemeli
            _PAYWALL_SIGNALS = [
                "sadece üyeler içindir",
                "sadece üyeler icin",
                "giriş yapın veya ücretsiz",
                "giris yapin veya ucretsiz",
                "ücretsiz bir hesap oluşturun",
                "ucretsiz bir hesap olusturun",
                "members only",
                "sign in to read",
                "create a free account",
            ]
            _ft_lower = full_text.lower()
            if any(sig in _ft_lower for sig in _PAYWALL_SIGNALS):
                logger.warning(
                    "TradingView paywall tespit edildi (matriks:%s) — icerik AI'a gonderilmiyor",
                    matriks_id,
                )
                full_text = ""  # KAP URL arayisini sürdur ama icerik bosalt

            # 5000 karakterle sinirla
            if full_text:
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
                # KAP URL'sini her zaman /tr/Bildirim/{id} formatina zorla
                # Bazi URL'lerde dil prefix'i yok (kap.org.tr/Bildirim/123) → KAP browser
                # diline gore acar (Ingilizce browser → Ingilizce sayfa). Bunu onlemek
                # icin URL'den ID'yi cikar, /tr/ ile yeniden olustur.
                _id_match = _re.search(r'Bildirim/(\d+)', real_kap_url)
                if _id_match:
                    real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{_id_match.group(1)}"
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

# ─── ROUTINE PRE-FILTER ───────────────────────────────────────────────────────
# Fiyat hareketine sebep olmayan, sirket fundamentals'inden bagimsiz teknik/idari
# bildirimler. Bu pattern'lar tespit edilirse AI'ya gitmeden Notr 5.0 doner.
# Her yil binlerce KAP bildirimi var, %60+'i bu kategoride → AI kredisi tasarrufu.
#
# Her entry: (regex pattern, kategori, standart Turkce summary, hashtag listesi)
# Pattern eslesirse score=5.0, ai_pending=False, ai_atlandi=True olarak isaretlenir.

_ROUTINE_FILTERS: list[tuple[str, str, str, list[str]]] = [
    # --- FON / NET AKTIF DEGER (rutin raporlama, fiyat etkisi yok) ---
    (
        r"net\s*aktif\s*deger|pay\s*basina\s*net\s*aktif",
        "Net Aktif Deger Aciklama",
        "Yatırım fonu/ortaklığı pay başına net aktif değer açıklaması — günlük/haftalık rutin değerleme raporudur. Fiyata yeni bilgi katmaz; sadece portföy değerinin güncel tespitidir.",
        ["netaktifdeger"],
    ),

    # --- BORSA / MKK MEKANIZMALARI (fiyat hareketi ile alakali ama temel etki yok) ---
    (
        r"devre\s*kesici|tek\s*fiyat\s*emir\s*toplama|pay\s*bazinda\s*devre\s*kesici",
        "Devre Kesici",
        "Borsa İstanbul, hissede yaşanan ani ve yüksek fiyat hareketi nedeniyle Pay Bazında Devre Kesici uygulamasının devreye girdiğini bildirmiştir. Bu bildirim şirketin temel faaliyetleriyle ilgili bir gelişme olmayıp, hisse senedinde anlık yüksek volatiliteyi kontrol altına almayı amaçlayan standart bir borsa mekanizmasıdır. Yatırımcı açısından doğrudan pozitif veya negatif etkisi bulunmaz.",
        ["devrekesici"],
    ),
    # --- YENİ HALKA ARZ / İLK İŞLEM GÜNÜ (BISTECH teknik bildirimi + Baz Fiyat) ---
    # "BISTECH Pay Piyasası Alım Satım Sistemi Duyurusu" + "Baz Fiyat: XX TL" → IPO ilk gün mekanik bildirimi
    # NOT: "piyasas" yazıyoruz — "piyasası" (ı) veya "piyasası" her iki biçimi yakalar.
    (
        r"bistech.*piyasa.*al[ıi]m\s*sat[ıi]m|baz\s*fiyat.*maksimum\s*emir|maksimum\s*emir.*baz\s*fiyat"
        r"|islem\s*gormeye\s*baslayacak|i[sş]lem\s*g[oö]rmeye\s*ba[sş]layacak",
        "Yeni Halka Arz İlk İşlem Günü",
        "Bu bildirim, hissenin Borsa İstanbul'da ilk kez işlem görmeye başladığına dair teknik bir BISTECH sistemi duyurusudur. Baz fiyat ve maksimum emir değeri belirlenerek işleme açılır; hisse için analiz edilecek yeni bir temel gelişme içermez.",
        ["halkaarz", "bistech", "borsaistanbul"],
    ),
    # --- ENDEKSLERİNDE DEĞİŞİKLİK — Yeni listelenme (IPO günü index dahil) ---
    # Not: Mevcut hisse index'e giriyorsa gerçek pozitif haberdir (filtre etme).
    # Yalnızca "BISTECH Pay Piyasası" ile aynı gün gelen index değişikliğini yakalamak
    # için bağımsız bir filter eklemek yerine bu kategoriyi DÜŞÜK SKOR (4.0) ile bırakıyoruz.
    # AI bu durumu zaten DÜŞÜK SKORLASIN diye system prompt'a kural ekledik (aşağıda).
    # --- BISTECH / MKK / TAKASBANK — Rutin teknik bildirimler (ex-div, tescil vb.) ---
    (
        r"bistech.*pay\s*piyasa|merkezi\s*kayit\s*kurulu[sş]u\s*duyurusu|takasbank\s*duyurusu|mkk\s*duyurusu",
        "BISTECH/MKK/Takasbank Duyurusu",
        "Bu duyuru Borsa İstanbul/MKK'nin teknik bir bildirimi olup, temettü/bedelsiz/bölünme miktarı zaten önceden ilan edilmiştir. Sadece ex-div günü veya kayıt tescili niteliğinde olup hisse fiyatına ek pozitif etki beklenmez.",
        ["bistech"],
    ),
    # --- IDARI / USUL BILDIRIMLERI (sirket icin sifir mali etki) ---
    (
        r"sorumluluk\s*beyani",
        "Sorumluluk Beyani",
        "Sorumluluk beyanı, finansal raporların doğruluğu konusunda yönetim kurulu ve mali işler sorumlusunun verdiği standart imza beyanıdır. İdari/usul bildirimi olup hisse fiyatına doğrudan etkisi beklenmemektedir.",
        ["bilgilendirme"],
    ),
    (
        r"faaliyet\s*raporu(?!\s*hakkinda)",
        "Faaliyet Raporu",
        "Yıllık veya dönemsel faaliyet raporunun yayınlandığı bildirimi. Rapor içeriği önceden bilinen finansal verileri yansıtır; rakamlar ayrıca açıklanmadığı sürece fiyata yeni bilgi katmaz.",
        ["faaliyetraporu"],
    ),
    (
        r"genel\s*kurul\s*(cagrisi|ilan|davet|toplant[ıi]\s*cagrisi)",
        "Genel Kurul Cagrisi",
        "Genel Kurul çağrı/ilan bildirimi. Toplantı gündeminde temettü/bedelsiz/sermaye artırımı gibi spesifik kararlar varsa ayrı bir bildirimde açıklanır. Bu sadece çağrı/davet niteliğinde, fiyata doğrudan etkisi yoktur.",
        ["genelkurul"],
    ),
    (
        r"genel\s*kurul\s*(toplanti\s*sonuc|sonuc\s*bildirim|tutanak)",
        "Genel Kurul Sonuc",
        "Genel Kurul toplantı sonuç bildirimi. Onaylanan kararlar önceden gündeme alınmış ve ayrıca açıklanmıştır. Bu bildirim sadece formal tescil niteliğinde olup yeni bir karar içermiyorsa fiyata etkisi sınırlıdır.",
        ["genelkurul"],
    ),
    (
        r"esas\s*sozlesme(\s*tadil|degis)",
        "Esas Sozlesme Tadili",
        "Esas sözleşme değişikliği bildirimi. Genellikle SPK uyumluluğu/kurumsal yönetim ilkeleri kapsamında yapılan teknik düzenleme olup, şirket faaliyetleri veya finansal yapıda açık bir değişim yaratmadığı sürece fiyata doğrudan etkisi beklenmez.",
        ["esassozlesme"],
    ),
    (
        r"imza\s*sirkuleri|temsil\s*ve\s*ilzam",
        "Imza Sirkuleri",
        "Yönetim kurulu imza yetkilerinin güncellenmesine ilişkin formal bildirim. Tamamen idari/hukuki nitelikli olup şirket faaliyetleri ve fiyat üzerinde doğrudan etkisi yoktur.",
        ["yonetim"],
    ),
    (
        r"sirket\s*genel\s*bilgi\s*formu",
        "Genel Bilgi Formu",
        "SPK mevzuatı gereği periyodik olarak güncellenen şirket bilgi formu. Yeni stratejik karar veya finansal bilgi içermedikçe hisse fiyatına yansıyacak bir bilgi taşımaz.",
        ["bilgilendirme"],
    ),
    (
        r"yonetim\s*kurulu(nun)?\s*(komite\s*atama|komite\s*olusum|alt\s*komite)",
        "Yönetim Kurulu Komite",
        "Yönetim kurulu denetim/risk/kurumsal yönetim komitelerinin atama ve yeniden yapılandırma bildirimi. Standart kurumsal yönetim işlemi olup fiyata etkisi yoktur.",
        ["yonetim"],
    ),
    (
        r"kurumsal\s*yonetim\s*uyum\s*raporu|kurumsal\s*yonetim\s*ilkeleri",
        "Kurumsal Yonetim Uyum",
        "Kurumsal yönetim ilkelerine uyum raporunun yayınlandığı standart bildirimi. Rapor içeriği şirket faaliyetlerini etkilemez, sadece formel uyum amaçlıdır.",
        ["kurumsalyonetim"],
    ),
    (
        r"yatirimci\s*sunumu|investor\s*presentation",
        "Yatirimci Sunumu",
        "Yatırımcı sunumunun KAP'ta yayınlandığı bildirim. Sunum genellikle önceden açıklanmış finansal sonuç ve stratejiyi özetler; yeni bir karar içermediği sürece fiyata bilgi katmaz.",
        ["bilgilendirme"],
    ),
    (
        r"bagimsiz\s*denetim\s*kurulusu\s*sec|denetim\s*sirketi\s*sec|denetci\s*sec",
        "Bagimsiz Denetim Secimi",
        "Bağımsız denetim kuruluşu seçimi/atama bildirimi. Standart yıllık mevzuat gereği olup şirket fundamentals'ina etkisi yoktur.",
        ["bilgilendirme"],
    ),
    (
        r"finansal\s*raporlar?in?\s*sunumu|finansal\s*tablolar?in?\s*sunumu",
        "Finansal Rapor Sunumu",
        "Periyodik finansal raporların SPK formatında sunumuna ilişkin bildirimi. Rakamlar önceden açıklanmış ana finansal verileri tekrar eder; yeni bilgi katmaz.",
        ["faaliyetraporu"],
    ),
    (
        r"ortaklik\s*yapisi(?!\s*degis)|sermaye\s*ve\s*ortaklik\s*yapisi(?!\s*degis)",
        "Ortaklik Yapisi Bildirimi",
        "Şirket ortaklık yapısının periyodik veya güncel halini gösteren formel bildirim. Yeni bir hissedar değişikliği/satım yoksa fiyata etkisi yoktur.",
        ["bilgilendirme"],
    ),
    (
        r"kar\s*payi\s*dagitim\s*tablosu(?!\s*kararla|\s*kararl)",
        "Kar Payi Dagitim Tablosu",
        "Kar payı dağıtım tablosunun SPK formatında yayınlandığı formel bildirim. Dağıtılacak temettü miktarı ayrıca yönetim kurulu kararı ile açıklanır.",
        ["temettu"],
    ),
    (
        r"kayitli\s*sermaye\s*tavani\s*(arttirim|yukseltil|degis)",
        "Kayitli Sermaye Tavani",
        "Şirketin kayıtlı sermaye tavanının yükseltilmesi/uzatılması bildirimi. Bu yalnızca SPK iznidir; fiili sermaye artırımı (bedelli/bedelsiz) değildir, ayrıca yapılırsa o zaman açıklanır.",
        ["sermayetavani"],
    ),
    (
        r"sermaye\s*piyasasi\s*araci\s*notu|izahname\s*onayi(?!\s*halka\s*arz)",
        "Sermaye Piyasasi Araci Notu",
        "Sermaye piyasası aracı notu/izahname onayı bildirimi. Standart prosedür olup bağımsız ek bilgi katmadan sadece hukuki formaliteyi belgeler.",
        ["bilgilendirme"],
    ),

    # --- TEMETTU PROSEDUR ADIMLARI (ilk karar sonrasi takip bildirimleri) ---
    # Bu bildirimler ZATEN onceden ilan edilmis temettu kararinin teknik islemleri.
    # Yatirimci icin yeni bilgi katmaz — fiyat zaten ilk karardan sonra fiyatlandi.
    # Bunlari tekrar tekrar "olumlu" puanlamak yatirimciyi yaniltir.
    (
        r"kar\s*pay[ıi]\s*odeme\s*tarihi|kar\s*pay[ıi]\s*odeme\s*bildirim|"
        r"temettu\s*odeme\s*tarihi|temettu\s*odeme\s*bildirim|"
        r"pay\s*basina\s*brut\s*temettu(?!.*onayland|.*karar)",
        "Temettu Odeme Prosedur",
        "Önceden Genel Kurul'da onaylanmış temettü dağıtımının ödeme tarihi/teknik bildirimi. Yeni bir karar olmayıp yalnızca duyurusu yapılan miktar ve tarihin tescili niteliğinde. Hisse fiyatı ilk karar açıklandığında fiyatlandı; bu bildirimle ek pozitif etki beklenmez.",
        ["temettu"],
    ),
    (
        r"hak\s*kullan[ıi]m(?:\s*tarihi|\s*surec)|temettu\s*hak\s*kazanim|"
        r"ex.?(?:dividend|date)|ex.?temettu",
        "Hak Kullanim Tarihi",
        "Hak kullanım/ex-temettü tarih bildirimi. Bu tarihte hisseyi elinde tutan yatırımcılar temettü hak sahibi olur — teknik tescil bildirimi olup ilk karar zaten önceden ilan edildiğinden fiyata yeni etki katmaz.",
        ["temettu"],
    ),
    (
        r"kar\s*pay[ıi]\s*dag[ıi]tim\s*(?:tescil|gerceklesti|tamamland)|"
        r"temettu\s*dag[ıi]tim[ıi]\s*(?:tescil|gerceklesti|tamamland)",
        "Temettu Dagitim Tamamlandi",
        "Temettü dağıtımının tamamlandığı/tescil edildiği bildirimi. Tamamen prosedürel bir adım olup miktar ve tarih önceden ilan edilmiştir. Hisse fiyatına yeni etki yaratmaz.",
        ["temettu"],
    ),

    # --- SERMAYE ARTIRIMI PROSEDUR ADIMLARI (ilk karar sonrasi takip) ---
    # Bedelli/bedelsiz sermaye artiriminin ilk YK karari pozitif veya negatif
    # puanlanir. Sonrasindaki tum adimlar (ihraç belgesi, kullanim suresi,
    # tescil, dagitim gerceklesti) ZATEN o ilk kararda fiyatlandi. Tekrar
    # pozitif olarak puanlamak yatirimciyi yaniltir.
    (
        r"sermaye\s*art[ıi]r[ıi]m[ıi]\s*(?:tescil|tamamland|gerceklesti)|"
        r"sermaye\s*art[ıi]r[ıi]m[ıi]\s*(?:islemleri\s*)?ticaret\s*sicil",
        "Sermaye Artirimi Tescil",
        "Önceden karar verilmiş sermaye artırımının Ticaret Sicili'nde tescili/tamamlanması bildirimi. Karar ve oran önceden açıklandığında fiyat zaten reaksiyon verdi — bu bildirim teknik tescil adımı olup yeni etki yaratmaz.",
        ["sermayeartirimi"],
    ),
    (
        r"ihrac\s*belgesi\s*(?:onay|verilm|alin)|"
        r"spk\s*(?:tarafindan\s*)?ihrac\s*belgesi|"
        r"bedelli.*ihrac\s*belge|bedelsiz.*ihrac\s*belge",
        "Ihrac Belgesi SPK Onayi",
        "Önceden duyurulan sermaye artırımının SPK ihraç belgesinin onayı/teslimi. İlk karar duyurusunda fiyat zaten reaksiyon verdi. Bu adım sadece şirketin SPK izniyle ihracı başlatabileceğini gösterir, yeni stratejik bilgi katmaz.",
        ["sermayeartirimi"],
    ),
    (
        r"r[uü][cç]han\s*hakk[ıi]\s*kullan[ıi]m\s*(?:suresi|tarihi|baslang|bitis|baslad)|"
        r"r[uü][cç]han\s*hakk[ıi]\s*(?:satis|alimi)\s*baslad",
        "Ruchan Hakki Kullanim Donemi",
        "Önceden ilan edilmiş bedelli sermaye artırımının rüçhan hakkı kullanım süresi bildirimi. İlk karar duyurusunda fiyat reaksiyon verdi (negatif), bu sadece kullanım periyodu tescili. Yatırımcı için yeni bilgi katmaz.",
        ["bedelli"],
    ),
    (
        r"bedelsiz\s*pay\s*(?:dag[ıi]t[ıi]m[ıi])?\s*(?:tarihinin\s*tescil|tescil|gerceklesti|tamamland)|"
        r"bedelsiz\s*pay\s*dagit[ıi]m[ıi]?\s*tarih",
        "Bedelsiz Pay Dagitim Tescili",
        "Önceden duyurulmuş bedelsiz sermaye artırımının pay dağıtım tarihinin tescili/uygulaması. Oran ve karar ilk bildirimi takiben fiyatlandı — bu adım sadece teknik kayıt niteliğinde olup yeni reaksiyon beklenmez.",
        ["bedelsiz"],
    ),
    (
        r"sermaye\s*art[ıi]r[ıi]m[ıi]\s*tutar(?:in)?\s*tahsilat|"
        r"bedelli\s*sermaye\s*art[ıi]r[ıi]m[ıi]\s*nakit\s*girisi",
        "Bedelli Tahsilat",
        "Bedelli sermaye artırımı sonucu şirkete nakit girişi tescili. Bu prosedürel bir kapanış bildirisidir; finansman amacı ilk karar duyurusundan beri biliniyordu.",
        ["bedelli"],
    ),

    # --- PAY GERI ALIM PROSEDUR (gunluk islemler) ---
    # NOT: analyze_news icinde BUYBACK BYPASS deterministik skor ile hallediyor.
    # Bu routine pattern KALDIRILDI — eskiden 'geri alim programi kapsamında'
    # geçen TUM bildirimleri 5.0 Notr yapiyordu, hatta buyuk tutarli olanlari
    # bile. Artik buyback_processor TL tutarina gore esik bazli skor veriyor
    # (kucuk -> 5.0 Notr, buyuk -> 7.0+ Olumlu).

    # --- YENI EKLENEN PATTERN'LAR (son 30 gun analizi sonrasi en sik tekrarlayan Notr basliklar) ---

    # 1. Pay Disinda Sermaye Piyasasi Araci Islemlerine Iliskin Bildirim (Faiz Iceren/Faizsiz)
    # 19 ornek son 30 gunde. Genelde bono/finansman bonosu/tahvil islem bildirimi — sirket
    # geliri/kari ile ilgili degil, sadece kayit/teknik islem.
    (
        r"pay\s*d[ıi][sş][ıi]nda\s*sermaye\s*piyasas[ıi]\s*arac[ıi]\s*i[sş]lemleri",
        "Pay Dışında Sermaye Piyasası Aracı İşlemleri",
        "Pay dışındaki sermaye piyasası aracı (bono, finansman bonosu, tahvil, sukuk) işlem bildirimi. Bu duyuru ihraç/itfa kapsamında teknik kayıt niteliğindedir; şirketin geliri veya kârı ile doğrudan ilgili değildir. Yatırımcı açısından hisse fiyatına etki yaratacak yeni bir bilgi içermez.",
        ["bilgilendirme"],
    ),

    # 2. Herhangi Bir Otoriteye Mali Tablo Verilmesi
    # SPK/EPDK/BDDK gibi otoritelere mali tablo gonderim kaydi. Bilgisel.
    (
        r"herhangi\s*bir\s*otoriteye\s*mali\s*tablo|otoriteye\s*finansal\s*tablo",
        "Otoriteye Mali Tablo Verilmesi",
        "SPK, BDDK, EPDK gibi düzenleyici otoritelere periyodik mali tablo gönderildiğinin tescili. Tablo içeriği ayrı bildirimle KAP'a yayınlanmadığı sürece yeni bilgi katmaz; tamamen formal/idari bir kayıttır.",
        ["bilgilendirme"],
    ),

    # 3. Piyasa Yapiciligi Kapsaminda Gerceklestirilen Islemler
    # Piyasa yapici (market maker) sirketin gunluk islem raporu. Manipulatif degil — gunluk kayit.
    (
        r"piyasa\s*yap[ıi]c[ıi]l[ıi][gğ][ıi]\s*kapsam[ıi]nda|piyasa\s*yap[ıi]c[ıi]s[ıi]\s*i[sş]lem",
        "Piyasa Yapıcılığı Kapsamında İşlemler",
        "Piyasa yapıcısı şirketin günlük likidite sağlama amaçlı işlem bildirimi. SPK düzenlemesi gereği şeffaflık amaçlı yapılan rutin kayıt olup şirketin temel faaliyetleri veya kârlılığı ile ilgili değildir.",
        ["bilgilendirme"],
    ),

    # 4. KAP Genel Duyurusu (Kamuyu Aydinlatma Platformu Duyurusu)
    # Mevcut bistech pattern yetersiz — "KAP Duyurusu" basligi ayri olabiliyor.
    (
        r"kamuyu\s*ayd[ıi]nlatma\s*platformu\s*duyuru|kap\s*duyuru(?:\s*-\s*\d+)?",
        "KAP Genel Duyurusu",
        "Kamuyu Aydınlatma Platformu'nun teknik veya sistem düzeyinde duyurusu. Şirket bazlı bir karar değil, KAP işleyişi ile ilgili bilgilendirme niteliğindedir. Hisse fiyatına doğrudan etkisi bulunmaz.",
        ["bilgilendirme"],
    ),

    # 5. Yönetim Kurulu Numarali Toplanti ("4. Yönetim Kurulu-II" gibi)
    # Periyodik yonetim kurulu toplantilari — gundem ayrı bildirimle aciklanir.
    (
        r"\d+\.?\s*y[öo]netim\s*kurulu\s*(?:-\s*[iı]+)?(?!\s*karar)",
        "Numaralı Yönetim Kurulu Toplantısı",
        "Şirketin periyodik (numaralı) Yönetim Kurulu toplantısı bildirimi. Toplantı gündemindeki spesifik karar varsa ayrı bir KAP bildirimi ile açıklanır. Bu duyuru sadece toplantının yapıldığını teyit eder, finansal etkisi yoktur.",
        ["yönetim"],
    ),

    # 6. Ozkaynaklar Degisim Tablosu (mali tablo eki)
    # Ana finansal tablonun ekidir, ayri analizi gerektirmez.
    (
        r"[öo]zkaynaklar\s*de[gğ]i[sş]im\s*tablosu",
        "Özkaynaklar Değişim Tablosu",
        "Finansal raporların ekinde yer alan özkaynak hareket tablosunun KAP'a sunumu. Ana finansal sonuçlar (kâr/zarar, gelir tablosu) ayrıca açıklandığı için yeni bilgi katmaz.",
        ["faaliyetraporu"],
    ),

    # 7. Tertip Ihrac Belgesi (borçlanma aracı ihrac — Notr)
    # Bono/sukuk/finansman bonosu ihraç belgesi. Borc ihraci = gelir/kar degil.
    (
        r"tertip\s*ihra[cç]\s*belgesi|borclanma\s*arac[ıi]\s*ihra[cç]|"
        r"finansman\s*bonosu\s*ihra[cç]|kira\s*sertifikas[ıi]\s*ihra[cç]",
        "Tertip İhraç Belgesi (Borçlanma)",
        "Borçlanma aracı (bono, finansman bonosu, sukuk, kira sertifikası) ihraç belgesi bildirimi. Şirket gelir veya kârı değildir — yalnızca finansman ihtiyacını karşılamak için borç ihracı yetkisidir. Borç yükünü artırabilir; hisse fiyatına doğrudan pozitif etkisi beklenmez.",
        ["borclanma"],
    ),
]


async def _fetch_context_data(ticker: str, content: str) -> str:
    """Bildirim icerigine gore ilgili gecmis veriyi DB'den cek + AI prompt'a inject.

    Temettu bildirimleri icin: son 3 yil temettu gecmisi (TL ve yield%)
    Sermaye artirimi / yeni is ilişkisi icin: son ozsermaye (oran hesabi icin)
    Pay geri alımı icin: önceki geri alim programi durumu

    Returns: AI prompt'a eklenmek uzere ek context metni (bos string de olabilir)
    """
    if not ticker or not content:
        return ""

    content_lower = content.lower()
    context_parts: list[str] = []

    try:
        from app.database import async_session
        from sqlalchemy import select, desc

        # ─── TEMETTU GECMISI (yield-bazli ve gecmis karsilastirma) ───
        if any(kw in content_lower for kw in [
            "kar payi", "kar payı", "kâr payı", "temettu", "temettü",
            "pay basina brut", "pay başına brüt", "kar dagitim", "kar dağıtım",
        ]):
            try:
                from app.models.dividend import DividendHistory
                async with async_session() as db:
                    result = await db.execute(
                        select(DividendHistory)
                        .where(DividendHistory.ticker == ticker.upper())
                        .order_by(desc(DividendHistory.payment_year))
                        .limit(5)
                    )
                    history = result.scalars().all()
                    if history:
                        lines = ["═══ TEMETTU GECMISI (son 5 yil — AI: bu veriyi kullan):"]
                        for h in history:
                            gross = float(h.gross_dividend_per_share) if h.gross_dividend_per_share else None
                            yield_pct = float(h.dividend_yield_pct) if h.dividend_yield_pct else None
                            if gross is not None:
                                line = f"  - {h.payment_year}: {gross:.4f} TL/hisse"
                                if yield_pct is not None:
                                    line += f" (verim %{yield_pct:.2f})"
                                lines.append(line)
                        if len(lines) > 1:
                            context_parts.append("\n".join(lines))
                            # Trend hesabi
                            if len(history) >= 2:
                                latest = float(history[0].gross_dividend_per_share or 0)
                                prior = float(history[1].gross_dividend_per_share or 0)
                                if latest > 0 and prior > 0:
                                    pct_change = ((latest - prior) / prior) * 100
                                    context_parts.append(
                                        f"  TREND: son yil ({history[0].payment_year}) "
                                        f"vs onceki yil ({history[1].payment_year}): "
                                        f"%{pct_change:+.1f} degisim"
                                    )
                            elif len(history) == 1:
                                context_parts.append(
                                    "  NOT: Sirket gecmiste sadece 1 kez temettu dagitmis "
                                    "(neredeyse ilk kez)"
                                )
                    else:
                        context_parts.append(
                            "═══ TEMETTU GECMISI: BOSH — sirket hic temettu dagitmamis "
                            "(ILK KEZ TEMETTU sinyali, base score +2.0 bonusu uygulanmali)"
                        )
            except Exception as _div_err:
                logger.debug("Temettu gecmis fetch hata (%s): %s", ticker, _div_err)

        # ─── OZSERMAYE (yeni is iliskisi / sermaye artirimi / pay geri alim oran hesabi) ───
        if any(kw in content_lower for kw in [
            "yeni is iliskisi", "yeni iş ilişkisi",
            "sermaye artir", "sermaye artır",
            "bedelli", "bedelsiz",
            "sozlesme imzalan", "sözleşme imzalan",
            "anlasma imzalan", "anlaşma imzalan",
            "ihale kazan", "ihale al",
            "pay geri al", "geri alim programi",
            "tedarikci", "tedarikçi", "musteri", "müşteri",
        ]):
            try:
                from app.models.company_financial import CompanyFinancial
                async with async_session() as db:
                    result = await db.execute(
                        select(CompanyFinancial)
                        .where(CompanyFinancial.ticker == ticker.upper())
                        .where(CompanyFinancial.total_equity.is_not(None))
                        .order_by(desc(CompanyFinancial.period))
                        .limit(1)
                    )
                    cf = result.scalar_one_or_none()
                    if cf and cf.total_equity:
                        eq = float(cf.total_equity)
                        # Insan-okunabilir format
                        if eq >= 1_000_000_000:
                            eq_str = f"{eq/1_000_000_000:.2f} milyar TL"
                        elif eq >= 1_000_000:
                            eq_str = f"{eq/1_000_000:.1f} milyon TL"
                        else:
                            eq_str = f"{eq:,.0f} TL"
                        context_parts.append(
                            f"═══ SIRKET OZSERMAYESI (son donem {cf.period}): {eq_str}\n"
                            f"  AI: yeni is/sermaye/pay alim tutar(lar)ini bu ozsermayeye "
                            f"oranla — oran %X = (tutar/ozsermaye)*100. Puanlama icin "
                            f"system prompt'taki oran tablosunu kullan."
                        )
                    else:
                        # Ozsermaye verisi yok — segment tahmini icin ipucu
                        context_parts.append(
                            "═══ SIRKET OZSERMAYESI: Veri bulunamadi — "
                            "ticker buyukluk segmenti uzerinden tahmin yap "
                            "(small-cap=500M-2B, mid-cap=5-20B, large-cap=30B+ TL)"
                        )
            except Exception as _cf_err:
                logger.debug("Ozsermaye fetch hata (%s): %s", ticker, _cf_err)

        # ─── ONCEKI POZITIF KARARLAR (takip bildirimi tespiti icin AI'ya ipucu) ───
        # (DB-based check zaten _check_followup_notification'da yapiliyor,
        # bu sadece AI'nin context'inde daha bilincli karar vermesi icin not.)
        try:
            from app.models.kap_all_disclosure import KapAllDisclosure
            from datetime import timedelta

            cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            async with async_session() as db:
                result = await db.execute(
                    select(KapAllDisclosure.title, KapAllDisclosure.ai_impact_score, KapAllDisclosure.published_at)
                    .where(KapAllDisclosure.company_code == ticker.upper())
                    .where(KapAllDisclosure.published_at >= cutoff)
                    .where(KapAllDisclosure.ai_impact_score >= 6.0)
                    .order_by(desc(KapAllDisclosure.published_at))
                    .limit(5)
                )
                priors = result.fetchall()
                if priors:
                    lines = ["═══ SON 30 GUN POZITIF KARARLAR (AI: bunlarin TAKIP bildirimleri ise NOTR 5.0 ver, tekrar yuksek puanlama):"]
                    for prior_title, prior_score, prior_date in priors[:5]:
                        date_str = prior_date.strftime("%Y-%m-%d") if prior_date else "?"
                        title_short = (prior_title or "")[:80]
                        lines.append(f"  - {date_str} (skor {prior_score:.1f}): {title_short}")
                    context_parts.append("\n".join(lines))
        except Exception as _prior_err:
            logger.debug("Onceki pozitif fetch hata (%s): %s", ticker, _prior_err)

    except Exception as e:
        logger.debug("Context data fetch genel hata (%s): %s", ticker, e)

    return "\n\n".join(context_parts) if context_parts else ""


def _check_routine_pattern(content: str, ticker: str) -> dict | None:
    """Rutin bildirim mi diye kontrol et. Eslesme varsa hazir cevap don.

    Returns:
        {"category": str, "summary": str, "hashtags": list[str]} ya da None.
    """
    if not content:
        return None
    # ★ Turkce-aware lowercase: "İ".lower() Python'da "i̇" (combining dot above)
    # uretiyor — pattern'deki "i" ile eslesmiyor. lower_tr "i" donduruyor.
    try:
        from app.utils.tr_text import lower_tr
        text_lower = lower_tr(content)
    except Exception:
        text_lower = content.lower()
    for pattern, category, summary, hashtags in _ROUTINE_FILTERS:
        if re.search(pattern, text_lower):
            # Ticker'i summary'nin basina ekle (kullanici ne hisse oldugunu bilsin)
            full_summary = summary
            return {
                "category": category,
                "summary": full_summary,
                "hashtags": hashtags,
            }
    return None


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
• ACTIVE SCORING: Avoid clustering scores in 4.5-5.5 range AND 6.0-6.3 range. Be bold,
  differentiate every disclosure. ASLA "DEFAULT 6.2" ATAMA YAPMA. Eger haber gercekten
  Hafif Olumlu degil de Olumlu (7.0+) ise CESARETLE 7.0+ ver. Kucuk farklar onemlidir:
  6.2 vs 7.4 vs 8.6 puan kategorisi (Hafif Olumlu / Olumlu / Cok Olumlu) yatirimci icin
  cok farkli bilgi tasir.
• NUANCE: Avoid dismissive phrases like "rutin", "etkisiz", "somut gelisme yok".
  Replace with: "kisa vadede sinirli etki, orta vadede X potansiyeli" (measured commentary).
• CONTEXT: New deal = big positive for small-cap; limited for mega-cap. Calibrate to company size.
• OUTPUT IN TURKISH: Summary, sentiment label, hashtags — all in Turkish for retail audience.

═══ ANTI-CLUSTERING UYARISI (ZORUNLU) ═══
6.0-6.5 araliginda topraklamayin. Asagidaki vakalardan biri varsa MINIMUM 7.0 zorunlu:
  • Yield %10+ olan temettu → 8.5-9.5 (asla 7.0'in altinda olmasin)
  • Kurumsal yatirimci blok alimi (>%5 esik asilmis, >50M TL net alim) → 7.0-7.5
  • Bedelsiz %50+ → 8.0+
  • Sirket satin alma/M&A (premium ile) → 7.5+
  • Devlet kurumu sozlesmesi + Savunma/Teknoloji sektor → en az 6.5 + sektor bonusu
  • >100M TL ihale/sozlesme (tutar acisindan buyuk) → 7.0+

Eger AI cevirip 6.0-6.5'e koymak istiyorsa, KENDISINE SOR: "Bu haber gercekten BIR
KATEGORI YUKARI tasidigim icin bir adim atamam mi?" — atabiliyorsan AT.

═══ TAKIP BILDIRIMI FARKINDALIGI — KRITIK ═══

ASLA AYNI KARARI 2 KEZ POZITIF PUANLAMA.

Bir sirket pozitif bir karar acikladiginda (orn: "%50 bedelsiz", "2 TL temettu",
"500M TL ihale") fiyata reaksiyon o ANDA verilir. Sonrasinda gelen ADIM ADIM
prosedur bildirimleri ZATEN fiyatlandi — yatirimci icin yeni bilgi degildir.

PROSEDUR ADIMLARI (HER ZAMAN NOTR 5.0):
  TEMETTU:
    Ilk YK karari ("kar payi dagitilmasi onayland") → POZITIF (gerçek değer)
    Sonra gelen:
      - "Kar payi odeme tarihi bildirim"           → NOTR 5.0
      - "Pay basina brut temettu X TL" (tek basina, karar yok) → NOTR 5.0
      - "Hak kullanim tarihi tescili"              → NOTR 5.0
      - "Temettu dagitim tamamlandi"               → NOTR 5.0
      - "Ex-temettu tarihi"                         → NOTR 5.0

  BEDELSIZ SERMAYE ARTIRIMI:
    Ilk YK karari "%X bedelsiz onayland"          → POZITIF (gerçek değer)
    Sonra gelen:
      - "SPK ihraç belgesi onayi"                   → NOTR 5.0
      - "Bedelsiz pay dagitim tarihinin tescili"    → NOTR 5.0
      - "Bedelsiz pay dagitimi gerceklesti"         → NOTR 5.0
      - "Sermaye artirimi Ticaret Sicili tescili"   → NOTR 5.0

  BEDELLI SERMAYE ARTIRIMI:
    Ilk YK karari "%X bedelli onayland"           → NEGATIF (gerçek seyreltme sinyali)
    Sonra gelen:
      - "SPK ihraç belgesi onayi"                   → NOTR 5.0
      - "Ruçhan hakki kullanim suresi baslangici"   → NOTR 5.0
      - "Bedelli sermaye artirimi nakit girisi"     → NOTR 5.0
      - "Sermaye artirimi tescil edildi"            → NOTR 5.0

  PAY GERI ALIMI:
    Program duyurusu / ilk buyuk alim             → POZITIF (gerçek değer)
    Sonra gelen kucuk gunluk alimlar              → NOTR 5.0-5.4 (ZATEN BILINIYOR)
    Ancak: cok buyuk tutarli ozel alim (>%5 sirket pay) → POZITIF kalir

NASIL TANIRSIN PROSEDUR/TAKIP BILDIRIMINI?
  - Sistem context'inde "SON 30 GUN POZITIF KARARLAR" listesi gosterilir.
  - O listede ayni konuda bir bildirim varsa BU TAKIP/PROSEDUR'dur → NOTR 5.0
  - Baslikta "tescil", "tamamlandi", "kullanim", "odeme tarihi", "ihraç belgesi",
    "gerceklesti", "tescil edildi" gecmesi guclu prosedur sinyalidir.
  - Yeni bir oran/tutar VAR mi? Yoksa zaten bilinen miktarin uygulamasi mi?

═══ YENİ LISTELENME / IPO ILK GUN KURALI (KRİTİK) ═══

Bir hisse BUGUN Borsa Istanbul'da ILK KEZ islem gormeye basladiysa:
  - "BISTECH Pay Piyasasi Alim Satim Sistemi Duyurusu" + "Baz Fiyat" → NOTR 5.0
    (Bu bildirim tamamen mekanik: baz fiyat ve maksimum emir degerini borsa sistemi atar.
    Sirketle ilgili yeni bilgi icermez. AI ANALIZI YAPMA, SKOR 5.0 VER.)

  - "Endeks Sirketlerinde Degisiklik" → IPO gunu eklenme → NOTR 5.0
    (Yeni listelenen her hisse otomatik olarak BIST Tum, BIST Halka Arz vb. endekslere girer.
    Bu zorunlu/otomatik bir prosedurdu, yatirimci icin yeni bilgi degildir.)

    ANCAK: Mevcut ve uzun suredir islem goren bir hisse BIST100 veya BIST30 gibi
    onemli bir endekse yeni giriyorsa → POZITIF 6.5-7.5 (gercek fonksiyon alimi tetikler).
    Hissenin yeni mi listenlendigi yoksa eski mi oldugunu icerikteki "Baz Fiyat" /
    "ilk kez islem" ifadelerinden veya bildirim tarihinden anlarsın.

═══ DUAL PERSPECTIVE — MANDATORY (HER VAKADA UYGULA) ═══
HER bildirim icin iki acidan dusun:
  A) SIRKET ACISINDAN: Bilanco, ciro, nakit akisi, borc yuku, operasyonel guc.
  B) YATIRIMCI/HISSE ACISINDAN: Seyreltme, arz baskisi, momentum, retail algi,
     fiyat reaksiyonu, ileriye donuk sinyal.

Final skor BU IKI ACININ BIRLESIMI olmalidir. Cogu zaman ayni yone gider; ama
bazi olaylar sirket icin "iyi" gorulse de yatirimci icin "kotu" olabilir:
  • Bedelli sermaye artirimi → sirkete nakit gelir AMA hisse seyrelir → NEGATIVE
  • Holding pay satisi → sirkete dogrudan etki yok AMA arz baskisi → NEGATIVE
  • Borc ihraci → sirkete finansman AMA borc yuku, ciro/kar etkisi yok → NOTR
  • Buyuk sozlesme → sirket geliri artar VE retail algilar olumlu → POZITIF (gucland)
Yatirimci acisi her zaman BASKINDIR (puan asgari %60 buradan).

═══ ANALYSIS STEPS (chain-of-thought — sequential per disclosure) ═══
1. DISCLOSURE TYPE: sozlesme/ihale, sermaye artirimi, bedelsiz, temettu, kâr/zarar,
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

"finansal" → kâr/zarar, temettu, bedelsiz, sermaye artirimi, sozlesme/ihale tutari, ceza,
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

Absolute amount (TL — after conversion). HER ZAMAN HEM ŞİRKET KASASI/CIRO ETKİSİ
HEM DE YATIRIMCI PRİZMA (algı, momentum, hype) AÇISINDAN DEĞERLENDİR:
  >10 billion    → 9.3-9.7 (mega — sektör değiştiren, hisse 1-2 hafta yukarı)
  5-10 billion   → 8.8-9.3 (devasa)
  1-5 billion    → 8.2-8.8 (cok buyuk — yatırımcı çok güçlü algılar)
  500M-1B        → 7.6-8.2 (buyuk — pozitif sürpriz, ciddi haber)
  200-500M       → 7.0-7.6 (orta-buyuk)
  100-200M       → 6.5-7.0 (orta)
  50-100M        → 6.1-6.5 (orta-kucuk)
  25-50M         → 5.8-6.2 (kucuk)
  10-25M         → 5.5-5.9 (cok kucuk)
  <10M           → 5.2-5.5 (semboik — minimal etki)

Revenue ratio adjustment (sirket kasasi acisindan etki):
  >%50 → +0.8 (transformatif)
  %30-50 → +0.5
  %15-30 → +0.3
  %5-15 → 0
  <%5 → -0.2 (mega-cap için anlamsız)

Investor perception bonus (TR retail davranis layer):
  +0.2 ekstra if amount kategorisi 7.5+ AND mid-small cap (<10B TL mcap)
  +0.1 ekstra if amount kategorisi 8.0+ AND ihale/sozlesme yabanci/multinational

═══ SPECIAL CASES ═══

NEW BUSINESS RELATIONSHIP (Yeni Tedarikci/Musteri/Is Ortakligi) — DUAL SCORING:

KRITIK KURAL: AI Asla 6.0-6.5 araliginda topraklamayin. Yeni is iliskisi
SEKTOR/MUSTERI CESITLILIGI ve GELIR DIVERSIFIKASYONU acisindan onemlidir.

ASIL SISTEM: MAX(mutlak_tutar_skoru, oran_skoru) — iki kanaldan en yuksek skor.

KANAL 1 — MUTLAK TUTAR (TL — currency conversion sonrasi):
Sirket buyuklugune bakilmaksizin yatirimci icin "duyulmaya deger" olan tutarlar:
  >1 milyar TL        → 8.5-9.0 (devasa is iliskisi)
  500M-1B             → 8.0-8.5 (cok buyuk)
  200-500M            → 7.5-8.0 (buyuk)
  100-200M            → 7.2-7.5 (anlamli)
  50-100M             → 7.0-7.2 (olumlu — kesin minimum 7.0)
  25-50M              → 6.7-7.0 (orta-olumlu)
  10-25M              → 6.5-6.8 (hafif olumlu UST sinir)
  5-10M               → 6.3-6.5
  1-5M                → 6.0-6.3 (hafif olumlu)
  <1M ama duyurulmus  → 5.8-6.0

KANAL 2 — OZSERMAYE/CIRO ORANI (yan dogrulayici):
  oran >%50          → 8.5-9.0 (transformatif)
  %25-50             → 8.0-8.5
  %15-25             → 7.5-8.0
  %10-15             → 7.0-7.5
  %5-10              → 6.7-7.0
  %2-5               → 6.3-6.7
  <%2                → tutar skorunu kullan

FINAL: max(kanal_1, kanal_2) — yani iki kanaldan yuksek olani. Boylece
buyuk sirketin kucuk gozuken sozlesmesi tutar acisindan hala anlamli olur.

PARTNER PRESTIJ BONUSU (CUMULATIF UYGULA — TUM bonuslari topla):
  + Multinational/Fortune 500 partner → +0.3
  + Sektor lideri yerli sirket        → +0.2
  + Kamu (devlet kurumlari, SSB, TSK, vb.) → +0.3 (garantili odeme + referans)
  + Yuksek teknoloji urunu (5G, uydu, AI, savunma)  → +0.3
  + Ihracat sozlesmesi (USD/EUR/GBP)  → +0.2 (TR retail seviyor)
  + Cok yillik / uzun vade            → +0.2 (kalici gelir)
  + Stratejik ortaklik / JV           → +0.3
  + Backlog %5+ artisi                → +0.4
  TOPLAM bonus tavani: +1.0 (asla 1'in uzerine cikmasin)

ORNEKLER (yeni kurallar):
  - 22.5M TL savunma sozlesmesi (devlet+teknoloji): kanal_1=6.5 + 0.3 (kamu)
    + 0.3 (teknoloji) + 0.2 (ihracat USD) = 7.3 → "Olumlu" ✓
  - 100M TL Fortune 500 musteri: 7.2 + 0.3 (multinational) + 0.2 (uzun vade)
    = 7.7 → "Olumlu" ✓
  - 5M TL kucuk anlasma: 6.3 → "Hafif Olumlu" — burada kalmasi OK

ORNEKLER:
  - Ozsermaye 1M TL, anlasma 5M TL (oran %500) → 9.0 (transformatif kucuk sirket)
  - Ozsermaye 100M TL, anlasma 5M TL (oran %5) → 6.7 (orta-olumlu)
  - Ozsermaye 10B TL, anlasma 10M TL (oran %0.1) → 6.0 (minimum hafif olumlu)
  - Fortune 500 ile + tutar belirsiz                 → 6.8 (multinational bonus)
  - Yerli mid-sized + 50M TL anlasma + ozsermaye 500M → 7.5 (oran %10)

PARTNER PRESTIJ BONUSU:
  + Multinational/Fortune 500 partner → +0.3
  + Sektor lideri yerli sirket        → +0.2
  + Kamu (devlet kurumlari)           → +0.2 (genelde garantili odeme)

SHARE BUYBACK (Pay Geri Alimi) — TL TUTARI BAZINDA SCALE:

KRITIK FORMUL: alim_tutari = ortalama_fiyat × geri_alinan_pay_adedi
Genelde KAP bildiriminde fiyat aralik olarak verilir (orn: 12.50 - 13.20).
Ortalama_fiyat = (min + max) / 2. Adet de bildirimde olur.

PUAN TABLOSU:
  <500 bin TL    → 5.0-5.3 (sembolik — gostermelik geri alim, fiyat etkisi yok)
  500K-1M TL     → 5.3-5.5 (cok kucuk — Notr+)
  1-5M TL        → 5.5-6.0 (kucuk — Notr-Hafif olumlu sinir)
  5-10M TL       → 6.0-6.5 (hafif olumlu — kullanicinin tarifi)
  10-25M TL      → 6.5-7.0 (olumlu — kullanicinin tarifi)
  25-50M TL      → 7.0-7.5 (ciddi olumlu — kullanicinin tarifi)
  50-100M TL     → 7.5-8.0 (cok pozitif — kullanicinin tarifi)
  100-250M TL    → 8.0-8.5 (mega — fiyata destek garantili)
  >250M TL       → 8.5-9.0 (devasa — manset olur)

MODIFIER'LAR:
  + Toplam programin ilk hareketi → +0.0 (sade tutar)
  + Programin son turlari, program tamamlandi → +0.2 (kararlilik sinyali)
  + Ortalama fiyat son 30 gun TAVANINDA → +0.2 (yonetim "hisse pahali olmasina ragmen aliyor" sinyali)
  + Ortalama fiyat son 30 gun TABANINDA → -0.1 (sadece "ucuz yerden topla" rutini)
  + Fiyat araliginda fitiklilik (cok dar) → +0.1 (kararli, profesyonelce yapilmis)
  + Cok sik gunluk alim (5+ gun ust uste) → +0.2

ORNEK 1: Sirket 5 TL fiyatla 100.000 lot aldi → tutar = 500K TL → 5.1 (sembolik)
ORNEK 2: Sirket 8.5 TL fiyatla 1M lot aldi → tutar = 8.5M TL → 6.2 (hafif olumlu)
ORNEK 3: Sirket 12 TL fiyatla 1.5M lot aldi → tutar = 18M TL → 6.7 (olumlu)
ORNEK 4: Sirket 25 TL fiyatla 1.5M lot aldi → tutar = 37.5M TL → 7.3 (ciddi olumlu)
ORNEK 5: Sirket 50 TL fiyatla 2M lot aldi → tutar = 100M TL → 8.0 (cok pozitif)

POZITIF EVENT KUTUPHANESI (Sektoreller — etki tahmini icin rehber):

  ARGE MERKEZI KURULMASI / TUBITAK projesi:
    Sektor ne olursa olsun → 6.5-7.3 (orta-uzun vadeli teknoloji yatirimi)
    + Devlet destegi / hibe alindi → +0.2

  SIRKET SATIN ALMA (M&A — bagli ortaklik haricinde):
    Hedef sirket var mi degerlendir:
      Stratejik (yeni sektor/cografya) + premium → 8.0-9.0
      Tamamlayici (mevcut faaliyete deger katiyor) → 7.0-7.8
      Bagli ortaklik (%100 zaten sahip) → 5.1-5.5 (limited mali etki)

  ARSA / GAYRIMENKUL SATISI:
    Stratejik atil/kullanilmayan varlik satisi → 6.0-6.8 (nakit girisi pozitif)
    Faaliyet alani satisi (uretim tesisi vs.)  → 4.0-5.0 (kapasite kaybi)
    Tutarin ozsermayeye orani:
      >%20 → +0.5 ekstra pozitif
      >%50 → +1.0 ekstra (mega varlik takasi)

  FINANSAL DURAN VARLIK EDINME (Hisse/Bono alimi):
    Stratejik ortakliga giris (partner sirket hissesi) → 6.5-7.5
    Pasif portfoy yatirimi (kucuk kupur)               → 5.0-5.5
    Devlet tahvili / bono                              → 4.8-5.2 (nötr — atil nakit park)

  ELEKTRIK URETIM LISANSI / Yenilenebilir Enerji projesi:
    Yeni lisans alindi → 6.8-7.5 (uzun vadeli gelir kanali)
    Lisans onayi (basvuru gecmisi) → 6.0-6.5
    Lisans iptali / red → 3.0-4.0 (NEGATIF)

  CED OLUMLU RAPORU (Cevresel Etki Degerlendirme):
    Buyuk yatirim projesi onayi (orn: maden, enerji, fabrika) → 6.5-7.5
    Sirket bunu yatirim onayinin son adimi olarak gorur — proje baslayabilir
    + Hedeflenen yatirim tutari >%20 ozsermaye → +0.5

  PATENT / MARKA TESCILI:
    Stratejik teknoloji patenti → 6.0-6.8
    Marka tescili (rutin) → 5.0-5.3

  YENI URUN LANSE / TICARI URETIME BASLAMA:
    Yeni urun mass-market giriyor → 6.3-7.0
    Niche / kucuk urun → 5.5-6.0

  KAPASITE ARTIRIMI / Yeni Tesis Kurulumu:
    Mevcudun >%30'u kadar kapasite eklenmesi → 7.0-7.8
    %10-30 kapasite                          → 6.3-7.0
    <%10                                     → 5.8-6.3

  IHRACAT ANLASMASI (yeni ulkeye / yeni musteriye):
    Coke buyuk volumlu → 7.0-8.0 (currency conversion sonrasi tutara gore)
    Standart pilot     → 6.0-6.5

  STRATEJIK ORTAKLIK / Joint Venture:
    Multinational partner + cash injection → 7.5-8.5
    Yerli stratejik partner                → 6.8-7.5
    Niyet anlasmasi / mutabakat (henuz baglayici degil) → 5.8-6.2

  FAALIYETLERIN SONLANDIRILMASI / Tesis Kapatma (NEGATIF):
    Tum faaliyet durdurulmasi              → 1.5-2.5 (kritik negatif)
    Belirli urun hatti / fabrika kapatma   → 3.0-4.0 (kayip cirosuna gore)
    Bagli ortaklik tasfiyesi (kucuk)       → 4.0-4.7
    + Kaybedilen ciro >%30                 → -0.5 ekstra negatif

  LISANS IPTALI / Ruhsat Kaybi:
    Faaliyet izni iptal (BDDK/SPK/EPDK)    → 1.5-2.5 (sektorden cikis riski)
    Marka tescili iptali                   → 3.5-4.5
    Lisans suresinin uzatilmamasi          → 2.5-3.5

  SPK / BDDK YAPTIRIMLARI:
    Faaliyet izninin geri alinmasi → 1.0-1.5 (existential)
    Idari para cezasi >10M TL      → 2.5-3.5
    Idari para cezasi 1-10M TL     → 3.5-4.0
    Uyari / kucuk ceza <1M TL      → 4.5-5.0

  IS KAZASI / Cevre Felaketi:
    Olumlu kaza + uretim duruyor → 1.5-2.5
    Cevre felaketi (sektor riski) → 2.0-3.0
    Mahkemeden tedbir alindi → 2.5-3.5

CAPITAL INCREASE (Sermaye Artirimi) — SCALE-DRIVEN:

  Bedelsiz (free issue — POZITIF, retail favorisi, sirket icin guclu pozitif sinyal):
    Bedelsiz orani arttikca puan dogrusal olarak yukselir. Oran buyukluk
    icin "pay-coklama" etkisi yaratir, sermaye ic kaynaklardan dagitilir
    — guclu nakit/yedek sinyali.
    ≥%500         → 9.5-10.0 (mega bedelsiz — devasa retail ilgi)
    %200-499      → 9.0-9.5 (cok buyuk, sektor manseti)
    %100-199      → 8.5-9.0 (buyuk pozitif)
    %50-99        → 8.0-8.5 (guclu pozitif)
    %20-49        → 7.0-8.0 (orta-buyuk pozitif)
    %10-19        → 6.5-7.0 (orta pozitif)
    <%10          → 6.2-6.5 (sembolik ama yine pozitif)
    + Sirk ilk kez bedelsiz dagitiyor → +0.2 (yeni temettu/bedelsiz alistirmasi)
    + Bedelsiz + temettu paralel → +0.2 (cift hediye)

  Bedelli (rights issue — NEGATIF, yatirimci icin SEYRELTME + EK NAKIT YUKU):
    Oran arttikca seyreltme dramatiklesir → puan dustukce duser.
    Sirket kasasi guclenir AMA hisse fiyati acisindan ASLA pozitif degildir
    (ruçhan price indirimi + dilution + ek nakit cikisi).

    ≥%200         → 2.0-2.5 (devasa seyreltme — "baya negatif")
    %100-199      → 2.5-3.0 (cok agir seyreltme — negatif)
    %50-99        → 3.0-3.5 (hafif negatif — kullanicinin tarifi)
    %20-49        → 3.5-4.0 (mid dilution — negatif)
    %10-19        → 4.0-4.3 (mild seyreltme — yine negatif)
    <%10          → 4.2-4.5 (minimal seyreltme — yine negatif)

    Modifier'lar:
      + Sermaye kaybi nedeniyle zorunlu ise (TTK 376) → -0.3 ekstra negatif
      + Halka acik teklif (genel arz) → -0.2 ekstra (mevcut paydas korunmuyor)
      + Rüçhan hakki kullanim suresi uzatildi → -0.1
      + Iptal edildi → 3.0-4.0 (yine negatif — finansman ihtiyaci hala var)
      + M&A finansmani / yeni tesis kurulumu icin → +0.5 (productive use)
      + Borc geri odeme icin → 0 nötr (no hidden upside)
    NEVER above 5.0 for bedelli unless ozel durum (stratejik M&A finansmani)

  Tahsisli (private placement — case-by-case):
    Stratejik yatirimci (mevcut paydas + lock-up 1+ yil) → 6.5-7.5
    General + dilution                                   → 4.0-5.0
    Halka arz iptal sonrasi tahsisli                     → 5.5-6.0

DIVIDEND (Temettu/Kar Payi) — HISTORICAL COMPARISON + YIELD HYBRID:

KRITIK: Hem YIELD% hem de GECMIS YILLARLA KIYAS sirketin gercek puanini belirler.
Sistem yield% (brut TL / current price) ve dividend_history'den son 2-3 yil
verisini onceden hazirlar. Bunlari beraber degerlendir:

  ADIM 1: YIELD-BASED BASE SCORE:
    Yield ≥%10        → 8.5-9.5 (excellent)
    Yield %7-10       → 7.8-8.5 (good)
    Yield %5-7        → 7.0-7.7 (above BIST avg)
    Yield %3-5        → 6.3-7.0 (BIST avg, mild positive)
    Yield %2-3        → 5.7-6.3 (weak positive)
    Yield %1-2        → 5.2-5.7 (neutral+)
    Yield %0.5-1      → 4.5-5.2 (weak neutral)
    Yield <%0.5       → 3.0-4.5 (NEGATIVE sembolik)
    Dividend yok      → 3.0-4.5 (NEGATIVE)
    Yield bilinmiyor  → TL bazinda hesapla (ortalama 30-50 TL fiyat varsay)

  ADIM 2: HISTORICAL ADJUSTMENT (son 2-3 yil) — KRITIK:
    Onceki yillarla kiyas yapilarak temel yield skoru ayarlanir.
    Eger sistem dividend_history saglarsa:

      ILK KEZ TEMETTU (gecmisinde hic dagitmamis):
        → BASE +2.0 (en kotu 7.0, cogu vakada 8.0+ — "gizli kasayi acti" sinyali)
        → Sentiment her durumda Olumlu (>= 7.0)
        Ornek: HEKTS hic dagitmamis, ilk kez 1.5 TL → yield %5 base 7.0 + 2.0 = 9.0

      SON YIL > ONCEKI YIL ARTIS:
        Artis ≥%100 (iki katina cikti) → BASE + 1.0
        Artis %50-100                  → BASE + 0.7
        Artis %20-50                   → BASE + 0.4
        Artis %5-20                    → BASE + 0.2 (kullanicinin tarifi: 2.1 → 2.6 → 3.0 hafif artis = hafif olumlu)
        Artis <%5                       → BASE + 0 (yatay)

      SON YIL < ONCEKI YIL DUSUS:
        Dusus <%20    → BASE - 0.3 (zayif sinyal ama tolere edilebilir)
        Dusus %20-50  → BASE - 0.7 (dikkat cekici dusus)
        Dusus %50-80  → BASE - 1.5 (ciddi dusus — kullanici "%50+ dramatik dusus" diyor → ASGARI 4.0'a cek, NEGATIF)
        Dusus ≥%80    → BASE - 2.5 (yok denecek seviyede — 2.5-3.5 NEGATIF)

      DAGITMAMA KARARI (Yönetim Kurulu "kar dagitilmamasini onayladi"):
        Eger gecen yil dagitildi → 3.0-3.5 (kotu surprise — NEGATIF)
        Eger gecen yil da dagitilmadi → 4.0-4.5 (rutin — Notr alt)

  ADIM 3: PATTERN BONUSES:
      + Bedelsiz + temettu beraber → +0.3
      + Stopajsiz / mukerrer        → +0.2
      + Nakit + bedelsiz secenek    → +0.2

  ORNEKLER:
  - EREGL 35 TL, 5 TL temettu, gecen yil 4.2 TL (artis %19) → yield %14.3 = 8.8 base + 0.2 = 9.0
  - HEKTS 20 TL, ILK KEZ 1.5 TL dagitiyor → yield %7.5 base 7.9 + 2.0 (ilk kez) = 9.9
  - SAHOL 25 TL, 2.1 TL gecen yil 3.0 TL (DUSUS %30) → yield %8.4 base 8.2 - 0.7 = 7.5
  - ABCD 18 TL, 0.10 TL (yield %0.56), gecen yil 0.50 TL (DUSUS %80) → 3.5 base - 2.5 = 2.5 (cok negatif)
  - XYZAB gecen yil 2 TL bu yil dagitma karari → 3.2 NEGATIF

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

MAJOR SHAREHOLDER PAY SATISI / HOLDING SECONDARY OFFERING (CRITICAL — MILD NEGATIVE):
  Sirketin BUYUK HISSEDARI (holding, kurucu, %5+ pay sahibi) kendi paylarini
  satarsa veya kurumsal yatırımcılara block sale yaparsa → BU NEGATIF SINYALDIR.
  Sebep:
    a) Insider selling — yonetim/holding "fiyat zirvede" sinyali verir
    b) Float artisi → arz baskisi
    c) Gelecekte daha fazla satim ihtimali (lock-up sonrasi)
  "Kurumsal yatirimci ilgisi" / "talep coklugu" gibi POZITIF gibi sunan ifadeler
  YANILTICIDIR — esasen pay satim = arz artisi.

  Pattern triggers:
    • "Holding ... hisselerini ... satti" / "block sale"
    • "Sermayenin %X'i kurumsal yatırımcılara satildi"
    • "Hizlandirilmis talep toplama" (accelerated bookbuilding)
    • "Kurucu/hakim ortak ... pay satti"
    • "Hisse satisi sonrasi pay orani %X'e dustu"

  Score:
    Satilan oran <%5    → 4.0-4.5 (mild negative)
    %5-10               → 3.3-4.0 (negative)
    %10-25              → 2.5-3.3 (significant negative)
    >%25                → 1.8-2.5 (major float dump)
  Lock-up varsa +0.3 (90+ gun satmama taahhudu = piyasa rahatlatici)
  ASLA "olumlu" olarak puanlamayin, "Notr+" da degil — NEGATIVE.

CIRCUIT BREAKER (Devre Kesici):
  ALWAYS 5.0 neutral — automatic mechanism, unrelated to fundamentals.

BISTECH / PAY PIYASASI / MKK / KAP SISTEM DUYURULARI (CRITICAL — neutral 5.0-5.4):
  Title patterns:
    • "BISTECH Pay Piyasasi Alim Satim Sistemi Duyurusu"
    • "Pay Piyasasi Alim Satim Sistemi Duyurusu"
    • "Merkezi Kayit Kurulusu Duyurusu" (MKK)
    • "Kamuyu Aydinlatma Platformu Duyurusu" (sistem-genel)
    • "Takasbank Duyurusu"

  Bu basliklar borsa/saklama-kurulus operasyonel duyurulari. Icerikte
  temettu (Pay Basina Brut Temettu), teorik fiyat, bedelsiz orani,
  pay bolunmesi orani gibi rakamlar GORULSE BILE bunlar SIRKET
  TARAFINDAN ZATEN HAFTALAR/AYLAR ONCE ILAN EDILMIS, fiyatlanmistir.
  Bu duyuru sadece ex-div gunu / kayit tescili / teknik fiyat adjusti.

  Score: ALWAYS 5.0-5.4 (Nötr). NEVER higher, even if dividend yield is high.
  Summary kisaca aciklamali (3-4 cumle): bu duyuru borsanin/MKK'nin teknik
  bildirimi olup, temettü/bedelsiz/bölünme miktarı ZATEN onceden ilan
  edilmistir. Bu yuzden hisse fiyatina ek pozitif etki beklenmemektedir.

  Examples:
  Ex.F: "ALARK BISTECH duyurusu — Pay Basina Brut Temettu: 3.185 TL,
        Teorik Fiyat: 92.465 TL"
        → 5.1 (Nötr — temettu zaten onceden ilan, bu sadece ex-div gunu
        teorik fiyat bildirimi)
  Ex.G: "MKK Duyurusu — pay bolunmesi tescili"
        → 5.1 (Nötr — kayit tescili, ilk karar degil)

DEBT INSTRUMENT ISSUANCE / BORCLANMA ARACI IHRACI (CRITICAL — neutral 4.5-5.4):
  Bu KAP bildirimleri sirketin BORC alma yetkisi/uygulamasi icindir — gelir
  veya kar getirmez, fiyat etkisi sinirlidir. Asla "olumlu haber" sayilmamalidir.
  Hisse fiyatina doğrudan pozitif etkisi yoktur; aksine seyreltme/borc yuku
  sinyali olabilir.

  Triggering keywords/patterns in title or body:
    • "Tertip Ihrac Belgesi" / "ihraç belgesi"
    • "Borçlanma Araci Ihrac Limiti / Tavani"
    • "Finansman Bonosu" ihraci / itfa
    • "Ozel Sektor Tahvili" ihraci
    • "Banka Bonosu" ihraci
    • "Kira Sertifikasi" ihraci (sukuk)
    • "VDMK" / "Varliga Dayali Menkul Kiymet"
    • "Bono / Tahvil ihrac" yetki / SPK basvuru
    • "Borçlanma Araci Ihracina Iliskin Yönetim Kurulu Karari"

  Score: 4.7-5.3 (Notr). ABSOLUTELY NEVER above 5.5. Sentiment="Nötr".
  ASLA "Olumlu" sentiment vermeyiniz — bu BORC ihracidir, gelir/kar degil.
  AI 6.0+ verirse o yanlistir; tertip/finansman/tahvil ihraci her zaman notr.
  Summary should clarify: bu bir borçlanma aracı (borc) ihracidir, ciroya/kara
  dogrudan etkisi yoktur; finansman ihtiyacını karşılamak icin yapilir, borc
  yukunu artirir.

  Examples:
  Ex.D: "TMSN Tertip Ihrac Belgesi (200M TL sukuk)" → 5.0 (NOTR, asla 6.1 degil)
       Summary: "Sirketin borçlanma aracı ihracina iliskin SPK belgesi;
                 ek finansman saglar fakat ciro/kar artisi degildir, fiyata
                 doğrudan pozitif etkisi beklenmez, borc yukunu artirir."
  Ex.E: "ABCD 500M TL finansman bonosu ihraci" → 4.9 (NOTR)
       Summary: "Kisa vadeli borclanma; yatirimci icin notr — borc maliyeti
                 ve geri odeme riski yaratabilir."

"ISLEMLERINE ILISKIN BILDIRIM" HEADERS — READ THE CONTENT:
  Titles like "Kar Payi Dagitim Islemlerine Iliskin Bildirim", "Sermaye
  Artirimi Islemlerine Iliskin Bildirim", "Bedelsiz Pay Dagitim Islemlerine
  Iliskin Bildirim", "Pay Bolunmesi Islemlerine Iliskin Bildirim" are
  generic — score by CONTENT, not title.

  CRITICAL: Bu basliklar altinda sirket ya:
    (a) ILK KEZ kararini ilan ediyor olabilir (ornegin "Yönetim Kurulu kar
        payi DAGITILMAMASINI onayladi" → bu yeni karar, AI puanla); veya
    (b) Onceden ilan edilen miktarin uygulamasi/tekrari olabilir (zaten
        fiyatlanmis → notr-yakin).

  Eger icerik:
    • Pay Basina Brut Temettu X TL veriyor → DIVIDEND yield-based scoring
      AMA content "ay/hafta once ilan edildi" / "GK karari uyarinca" gibi
      tekrar sinyali iceriyorsa → 5.0-5.6 (zaten fiyatlanmis)
    • "kar payi dagitilmamasi" / "dagitmama" karari → 3.5-4.5 (NEGATIVE —
      temettu beklentisi olan yatirimci icin olumsuz)
    • Bedelsiz X% / Bedelli X% YENI orani → CAPITAL INCREASE scoring
    • Sadece prosedur, somut rakam yok → 5.0-5.4 (Notr)

  Examples:
  Ex.A: Title "Kar Payi Dagitim Islemlerine Iliskin Bildirim" + content
        "Yönetim Kurulu 2025 yili kar payi DAGITILMAMASINI onaylamistir"
        → 3.8 (NEGATIVE — yeni karar, sifir verim)
  Ex.B: Title "Kar Payi Dagitim Islemlerine Iliskin Bildirim" + content
        "X tarihinde aciklanan brut Y TL temettu odemesi gerceklesecektir"
        → 5.2 (NOTR — onceden ilan edilen miktarin uygulamasi)
  Ex.C: Title "Sermaye Artirimi Islemlerine Iliskin Bildirim" + content
        ilk kez bedelsiz %50 oran aciklamasi → 8.5 (positive)

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

Ex.16 (TEMETTU ILK KARAR — context'te gecmis yok):
Title: "Kar Payi Dagitim Karari"
Body: "YK 2025 yili icin pay basina 2.50 TL brut temettu dagitimini onayladi"
Context: "TEMETTU GECMISI: BOSH — sirket hic temettu dagitmamis"
→ {{"score": 9.2, "category": "finansal", "summary": "Sirket hayatinda ILK KEZ temettu dagitiyor — 2.50 TL/hisse brut. Yatirimci icin guclu pozitif sinyal: kar dagitma kultu basliyor. Gecmis verim hesabi olmadigi icin marjinal etki tahmini guc ama 'ilk kez temettu' tek basina manset-degerinde haberdir.", "hashtags": ["temettu", "ilkkez"]}}

Ex.17 (TEMETTU TAKIP — odeme tarihi):
Title: "Kar Payi Odeme Tarihi Bildirimi"
Body: "Onceki YK karari uyarinca 2.50 TL temettu 25 Mayis 2026'da odenecektir"
Context: "SON 30 GUN POZITIF KARARLAR: - 2026-04-15 (skor 9.2): Kar Payi Dagitim Karari (2.50 TL onayland)"
→ {{"score": 5.0, "category": "bilgi", "summary": "Onceden 15 Nisan'da Genel Kurul'da onaylanan 2.50 TL temettu dagitiminin odeme tarihi tescili. Karar ve miktar onceden ilan edildiginde fiyat reaksiyon verdi — bu sadece teknik takip bildirimi olup yeni etki yaratmaz.", "hashtags": ["temettu"]}}

Ex.18 (TEMETTU ARTIS — gecmis veriyle):
Title: "Kar Payi Dagitim Karari"
Body: "YK 2025 yili icin 3.00 TL brut temettu dagitimini onayladi"
Context: "TEMETTU GECMISI: 2024: 2.60 TL, 2023: 2.10 TL — TREND: +%15 artis"
→ {{"score": 7.6, "category": "finansal", "summary": "2025 yili icin 3.00 TL temettu — gecen yila gore %15 artis, sirket sureklilik gostererek dagitim tutarini yukseltti. Kalici temettu odeyici sirket profili pozitif.", "hashtags": ["temettu"]}}

Ex.19 (TEMETTU DRAMATIK DUSUS):
Title: "Kar Payi Dagitim Karari"
Body: "YK 2025 yili icin 0.50 TL brut temettu dagitimini onayladi"
Context: "TEMETTU GECMISI: 2024: 2.50 TL, 2023: 2.30 TL — TREND: -%80 dusus"
→ {{"score": 3.2, "category": "finansal", "summary": "2025 temettu sadece 0.50 TL — gecen yil 2.50 TL idi (-%80 dramatik dusus). Sirket kar dagitma kapasitesinde ciddi azalma sinyali; kasanin daralma veya stratejik nakit korumayi tercih sinyali.", "hashtags": ["temettu"]}}

Ex.20 (BEDELLI %200 — DEVASA NEGATIF):
Title: "Bedelli Sermaye Artirimi Karari"
Body: "YK %200 oraninda bedelli sermaye artirimi onayladi"
→ {{"score": 2.2, "category": "finansal", "summary": "%200 bedelli sermaye artirimi — yatirimci icin devasa seyreltme + ek nakit yatirim yukumlulugu. Sirket kasasi 3 katina cikar AMA hisse fiyati ruçhan price indirimi ve dilution nedeniyle kuvvetli negatif reaksiyon verir.", "hashtags": ["bedelli"]}}

Ex.21 (BEDELLI TAKIP):
Title: "Sermaye Artirimi Tescil Edildi"
Body: "Onceki YK karari uyarinca bedelli sermaye artirimi Ticaret Sicili'nde tescil edildi"
Context: "SON 30 GUN: - 2026-04-10 (skor 2.2): Bedelli Sermaye Artirimi Karari"
→ {{"score": 5.0, "category": "bilgi", "summary": "Onceden duyurulmus bedelli sermaye artiriminin Ticaret Sicili tescili. Karar 1 ay once aciklandiginda fiyat zaten reaksiyon verdi (negatif yonde) — bu adim teknik kapanis niteliginde olup yeni etki yaratmaz.", "hashtags": ["sermayeartirimi"]}}

Ex.22 (BEDELSIZ %500 — MEGA POZITIF):
Title: "Bedelsiz Sermaye Artirimi Karari"
Body: "YK %500 oraninda bedelsiz sermaye artirimi onayladi"
→ {{"score": 9.7, "category": "finansal", "summary": "%500 bedelsiz sermaye artirimi — devasa pay coklamasi. Yedeklerden dagitilan bu sermaye sirketin nakit/yedek dolulugunu gosterir; retail icin manset-degerinde pozitif.", "hashtags": ["bedelsiz"]}}

Ex.23 (YENI IS ILISKISI — kucuk sirket buyuk anlasma):
Title: "Yeni Is Iliskisi"
Body: "Sirketimiz ABCD A.S. ile 5M TL'lik tedarik anlasmasi imzalamistir"
Context: "OZSERMAYESI: 1.5 milyon TL"
→ {{"score": 8.7, "category": "strateji", "summary": "5M TL'lik yeni tedarik anlasmasi sirketin 1.5M TL ozsermayesinin %333'u — transformatif buyuklukte. Bu duzeyde sozlesme sirketin gelir tabanini ve operasyonel olcegini kalici olarak buyutebilir.", "hashtags": ["sozlesme", "yeniisiliskisi"]}}

Ex.24 (YENI IS ILISKISI — buyuk sirket kucuk anlasma):
Title: "Yeni Is Iliskisi"
Body: "Sirketimiz XYZE Holding ile 5M TL'lik tedarik anlasmasi imzalamistir"
Context: "OZSERMAYESI: 10 milyar TL"
→ {{"score": 6.0, "category": "strateji", "summary": "5M TL'lik tedarik anlasmasi 10B TL ozsermaye ile karsilastirildiginda %0.05 — sembolik nitelikte. Yeni musteri kazanmak yine de pozitif sinyal olarak degerlendirilir (en az hafif olumlu).", "hashtags": ["sozlesme"]}}

Ex.25 (PAY GERI ALIM — 15M TL):
Title: "Pay Geri Alim Programi Kapsaminda Islemler"
Body: "Sirketimiz 25 TL ortalama fiyatla 600.000 lot pay geri almistir"
→ {{"score": 6.7, "category": "finansal", "summary": "15M TL'lik pay geri alimi (25 TL × 600K lot) — orta buyuklukte yatirim. Yonetimin hisseyi degerli gordugunu ve mevcut fiyat seviyesinde alici oldugunu gosterir; orta-vade fiyat destekleyici.", "hashtags": ["paygerialim"]}}

Ex.26 (PAY GERI ALIM — 500K TL sembolik):
Title: "Pay Geri Alim Programi Kapsaminda Islemler"
Body: "Sirketimiz 5 TL ortalama fiyatla 100.000 lot pay geri almistir"
→ {{"score": 5.1, "category": "finansal", "summary": "500K TL'lik kucuk geri alim (5 TL × 100K lot) — sembolik islem. Buyuk olcekte fiyat etkisi yaratacak buyuklukte degildir; geri alim programinin rutin gunluk uygulamasi.", "hashtags": ["paygerialim"]}}

Ex.27 (ARGE MERKEZI):
Title: "Arge Merkezi Kurulmasi"
Body: "Sirketimiz Bilim Sanayi ve Teknoloji Bakanligi'ndan Arge Merkezi belgesi almistir"
→ {{"score": 6.9, "category": "strateji", "summary": "Sanayi Bakanligi onayli Arge Merkezi belgesi — vergi tesvigi ve devlet destegine erisim saglar. Uzun vadeli teknoloji yetkinligini buyutme yatirimi; orta vadeli pozitif.", "hashtags": ["arge"]}}

Ex.28 (CED OLUMLU RAPORU):
Title: "Yatirim Projesi CED Olumlu Karari"
Body: "Sirketimizin planlamis oldugu rüzgar enerjisi santral yatirimi icin CED Olumlu kararı verilmistir"
→ {{"score": 7.1, "category": "strateji", "summary": "Buyuk olcekli yatirim projesinin CED onayi — projenin son izninin alinmasi anlamina gelir. Uzun vadeli gelir/kapasite katkisi acisindan pozitif.", "hashtags": ["enerji", "yatirim"]}}

Ex.29 (FAALIYET SONLANDIRMA — NEGATIF):
Title: "Tesis Faaliyetlerinin Durdurulmasi"
Body: "Sirketimiz Bursa fabrikasi faaliyetlerinin daimi olarak sonlandirilmasini onaylamistir"
→ {{"score": 2.8, "category": "strateji", "summary": "Bursa fabrikasi daimi olarak kapatildi — kapasite ve gelir tabaninda ciddi azalma. Personel cikarmalari ve sabit varlik kayiplari ile birlikte ciddi negatif sinyal.", "hashtags": ["kapanis"]}}

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

    # ─── PRE-FILTER: Rutin/idari bildirimleri AI'ya gonderme ───
    # Sabit Notr 5.0 + standart aciklama don. AI kredisi tasarrufu icin kritik.
    # Bu pattern'lar fiyat hareketine sebep olmayan teknik/idari duyurular.
    # HEM Telegram raw_text HEM de TV/KAP content kontrol edilir — JANTS
    # ornegi: Telegram baslıgı 'Devre Kesici' iken KAP fallback yanlis 3 gun
    # onceki sermaye artırımı bildirimini cekti -> content'te 'devre kesici'
    # yoktu -> pre-filter eslesmedi -> AI yanlis 7.9 verdi.
    _routine_filter = _check_routine_pattern(raw_text or "", ticker) or _check_routine_pattern(content, ticker)
    if _routine_filter is not None:
        logger.info("AI pre-filter: %s — '%s' (AI atlandi)", ticker, _routine_filter["category"])
        return {
            "score": 5.0,
            "summary": _routine_filter["summary"],
            "kap_url": kap_url,
            "hashtags": _routine_filter["hashtags"],
        }

    # Kaynak bilgisini prompt'a ekle
    source_info = "KAP Bildirim Tam Metni (TradingView)" if has_tv else "Telegram Kanal Ozeti (detay erisilemedi)"

    # ─── CONTEXT DATA INJECTION ─────────────────────────────────────────
    # Temettu gecmisi, ozsermaye, son 30 gun pozitif kararlar — bu veriler
    # AI'nin yield-bazli ve oran-bazli puanlamasini dogru yapmasi icin kritik.
    context_data = await _fetch_context_data(ticker, content)

    prompt = f"""Borsa Istanbul (BIST) KAP bildirimi analizi.

Hisse: {ticker}
Kaynak: {source_info}

--- ICERIK BASLANGIC ---
{content}
--- ICERIK BITIS ---

{context_data}

GOREV:
1. Haberi yatirimci bakis acisiyla Turkce ozetle. Cumle sayisi PUANA gore:
   • POZITIF (score >= 6.0) veya NEGATIF (score < 4.5): 7-8 cumle (detayli analiz)
   • NOTR (score 4.5-5.9): 3-4 cumle (kisa, oz)
2. Onemli rakamlari ozete dahil et (tutar, oran, yuzde).
3. Haberin ne oldugunu, sirket icin ne anlama geldigini ve yatirimci icin neden onemli oldugunu acikla.
4. Notr durumda sadece "ne oldugu" + "neden notr/etkisiz" yeterli — gereksiz uzatma.

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

    # ── Birincil: Gemini 2.5 Flash (~10x ucuz, KAP scoring icin yeterli) ──
    text = None
    provider_used = None

    if gemini_key:
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
                    provider_used = "Gemini-Flash"
                else:
                    logger.warning(
                        "AI News Scorer: Gemini HTTP %s (%s) — %s",
                        resp.status_code, ticker, resp.text[:200],
                    )
        except Exception as e:
            logger.warning("AI News Scorer: Gemini hata (%s) — %s", ticker, e)

    # ── Yedek 1: Anthropic Claude Sonnet 4 (Gemini fail olursa) ──
    # 503 (overloaded) gecici hata — 1 retry yap (2 sn beklemeli).
    # PROMPT CACHING aktif: 5000+ token system prompt cache'lenir,
    # 5 dakika icindeki sonraki Claude cagrilarinda %90 input maliyeti tasarrufu.
    if not text and anthropic_key:
        system_content = messages[0]["content"] if messages and messages[0]["role"] == "system" else ""
        user_content = messages[-1]["content"] if messages else ""
        _claude_payload = {
            "model": _CLAUDE_MODEL,
            "max_tokens": 4096,
            # Prompt caching: system'i dizi yap + cache_control isareti
            "system": [
                {
                    "type": "text",
                    "text": system_content,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            "messages": [{"role": "user", "content": user_content}],
            "temperature": 0.1,
        }
        _claude_headers = {
            "x-api-key": anthropic_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        for _attempt in (1, 2):
            try:
                async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                    resp = await client.post(_ANTHROPIC_URL, headers=_claude_headers, json=_claude_payload)
                    if resp.status_code == 200:
                        data = resp.json()
                        for block in data.get("content", []):
                            if block.get("type") == "text":
                                text = block.get("text", "").strip()
                                break
                        provider_used = "Claude-Sonnet"
                        break
                    elif resp.status_code in (503, 529, 429) and _attempt == 1:
                        # Gecici hata — kisa bekle ve tekrar dene
                        logger.warning(
                            "AI News Scorer: Claude HTTP %s (%s) — 2sn bekleyip retry",
                            resp.status_code, ticker,
                        )
                        await asyncio.sleep(2)
                        continue
                    else:
                        logger.error(
                            "AI News Scorer: Claude HTTP %s (%s) — %s",
                            resp.status_code, ticker, resp.text[:200],
                        )
                        break
            except Exception as e:
                logger.error("AI News Scorer: Claude hata (%s, attempt %d) — %s", ticker, _attempt, e)
                if _attempt == 1:
                    await asyncio.sleep(2)
                    continue
                break

    # ── Yedek 2: Abacus RouteLLM (kredi varsa) ──
    if not text and api_key:
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
                    logger.error(
                        "AI News Scorer: Abacus HTTP %s (%s) — %s",
                        resp.status_code, ticker, resp.text[:200],
                    )
        except Exception as e:
            logger.error("AI News Scorer: Abacus hata (%s) — %s", ticker, e)

    if not text:
        logger.error("AI News Scorer: Tum AI providerlar basarisiz (%s)", ticker)
        # FALLBACK: AI tamamen erisilemez ise akisi kirma — Notr/5.0 + reprocess flag.
        # Bildirim yine kap_all_disclosures'a + telegram_news'e yazilir, kullanici
        # haberi gorur ama AI yorumu yerine "yeniden denenecek" mesaji gozukur.
        # Bir cron job ileride ai_impact_score=5.0 + ai_summary'i kontrol edip yeniden puanlayabilir.
        return {
            "score": 5.0,
            "summary": (
                f"{ticker} icin KAP bildirimi alindi ancak AI analizi su anda "
                "yapilamadi (servis gecici hata). Bildirim icerigine KAP linkinden "
                "ulasabilirsiniz; AI yorumu daha sonra otomatik yeniden uretilecektir."
            ),
            "kap_url": kap_url,
            "hashtags": [],
            "ai_pending": True,  # Reprocess kuyrugu icin isaretleyici
        }

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

        # ─── TEKRAR EDEN BILDIRIM DAMPER (STRICT) ───
        # Ayni ticker icin son 30 gunde ayni konuda yuksek skor verilmisse,
        # bu yeni bildirim takip-bildirimdir. Skor TAMAMEN NOTR (5.0) yapilir
        # ve sentiment "Notr" olur — push/tweet/grup spam'i onlenir.
        #
        # Kullanici talebi: temettu kararindan sonra hak kullanim/odeme/tescil
        # gibi prosedur bildirimleri TEKRAR pozitif sayilmamali. Bedelli/
        # bedelsiz icin de ayni.
        if score is not None and score >= 6.0 and content:
            try:
                # FRESH KARAR BYPASS — yeni GK/YK karari + buyuk oran (%X) varsa
                # bu prosedurel takip degil, gercek pozitif karardir.
                # Ornek: AKFIS "GK ile %500 bedelsiz" => takip-damper'a takilmamali.
                _content_low = content.lower()
                _is_fresh_karar = False
                if (
                    ("genel kurul" in _content_low and "karar" in _content_low) or
                    ("yonetim kurulu" in _content_low and "karar" in _content_low) or
                    ("yönetim kurulu" in _content_low and "karar" in _content_low)
                ):
                    # %X oran var mi? (yuzde 50+ buyuk oranli artirimlar fresh karar)
                    import re as _re
                    _pct_match = _re.search(r"%\s*(\d{2,3}(?:[.,]\d+)?)", content)
                    if _pct_match:
                        try:
                            _pct = float(_pct_match.group(1).replace(",", "."))
                            if _pct >= 25.0:  # %25+ artirim/karar = fresh, prosedurel degil
                                _is_fresh_karar = True
                        except (ValueError, TypeError):
                            pass

                is_followup, prior_topic = await _check_followup_notification(ticker, content) if not _is_fresh_karar else (False, None)
                if _is_fresh_karar:
                    logger.info(
                        "AI News Scorer [FRESH-KARAR-BYPASS] %s: skor %.1f korundu "
                        "(GK/YK karar + %%X tespit edildi, takip-damper atlandi)",
                        ticker, score,
                    )
                if is_followup:
                    original_score = score
                    score = 5.0  # TAM NOTR — 5.5 degil, kullanici "pozitif gozukmesin" istiyor
                    logger.info(
                        "AI News Scorer [TAKIP-DAMPER-STRICT] %s: score %.1f -> 5.0 NOTR "
                        "(konu: %s, son 30 gunde benzer pozitif karar var — duplicate engellendi)",
                        ticker, original_score, prior_topic,
                    )
                    if summary:
                        summary = (
                            f"[Onceden duyurulmus {prior_topic} kararinin takip/prosedur bildirimi — "
                            f"ilk karar fiyatlandi, ek pozitif etki beklenmez] {summary}"
                        )
            except Exception as _follow_err:
                logger.debug("Followup check hata (%s): %s", ticker, _follow_err)

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
    # Bedelsiz sermaye artirimi oran-bazli
    (r"bedelsiz\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:[5-9]\d{2}|\d{4,})", 9.5),  # %500+ bedelsiz mega
    (r"bedelsiz\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:[2-4]\d{2})", 9.0),  # %200-499 bedelsiz
    (r"bedelsiz\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:1\d{2})", 8.5),  # %100-199 bedelsiz
    (r"bedelsiz\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:[5-9]\d)", 8.0),  # %50-99 bedelsiz
    # Kar artisi
    (r"(?:net\s*)?k[aâ]r[ıi]?\s*%\s*(?:1\d{2}|[2-9]\d{2}|\d{4,})\s*art", 9.0),  # %100+ kar artisi
    (r"rekor\s*(?:k[aâ]r|gelir|has[ıi]lat)", 8.0),
    # Yuksek yield temettu — yield% format'i icerikte gecerse
    (r"(?:verim|yield)\s*%\s*(?:[2-9]\d|\d{3,})\b", 9.0),  # >=%20 yield
    (r"(?:verim|yield)\s*%\s*(?:1[0-9])\b", 8.5),  # %10-19 yield
    (r"kar\s*pay[ıi]\s*oran[ıi]\s*%\s*(?:[2-9]\d|\d{3,})", 9.0),  # "kar payi orani %20+"
    (r"kar\s*pay[ıi]\s*oran[ıi]\s*%\s*(?:1[0-9])", 8.5),  # %10-19
    # Kurumsal block alim: >%5 esik asma sinyali
    (r"(?:%\s*5\s*esi[gğ]i?\s*a[sş]t|esik\s*a[sş][ıi]ld[ıi]|payi.*%\s*(?:[2-9]\d|\d{3,}).*y[üu]kseld)", 7.0),
]

# Bedelli sermaye artirimi — negatif TAVAN sinirlamasi
# (score asla bu degeri asmamali — bedelli her zaman negatif)
_BEDELLI_NEGATIVE_CAPS = [
    (r"bedelli\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:[2-9]\d{2}|\d{4,})", 2.5),  # %200+ baya negatif
    (r"bedelli\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:1\d{2})", 3.0),  # %100-199
    (r"bedelli\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:[5-9]\d)", 3.5),  # %50-99 hafif negatif
    (r"bedelli\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:[2-4]\d)", 4.0),  # %20-49
    (r"bedelli\s*(?:sermaye\s*art[ıi]r[ıi]m[ıi])?\s*%\s*(?:1\d)", 4.3),  # %10-19
]


_FOLLOWUP_TOPICS = {
    "bedelsiz_sermaye_artirimi": [
        "bedelsiz", "iç kaynak", "ic kaynak", "sermaye artırımı bedelsiz",
        "bedelsiz pay dagitim", "bedelsiz pay dağıtım",
        "bedelsiz sermaye artirim", "bedelsiz sermaye artırım",
    ],
    "bedelli_sermaye_artirimi": [
        "bedelli sermaye", "rüçhan hakkı", "ruchan hakki",
        "bedelli pay", "bedelli sermaye artirim", "bedelli sermaye artırım",
        "ihraç belgesi bedelli", "ihraç belgesi bedelli",
        "yeni pay alma hakki", "yeni pay alma hakkı",
    ],
    "temettu_kararı": [
        "kar payı", "kar payi", "kâr payı",
        "temettü", "temettu",
        "pay başına brüt", "pay basina brut",
        "kar dağıtım", "kar dagitim",
        "kar payı dağıtım", "kar payi dagitim",
        "ex-dividend", "ex-temettu", "hak kullanım", "hak kullanim",
    ],
    "spk_onay": ["spk onay", "sermaye piyasası kurulu onay", "spk kabul"],
    "spk_başvuru": ["spk başvuru", "spk basvuru", "kurul'a başvuru"],
    # NOT: "ihrac belgesi" KALDIRILDI — sermaye artirimi disclosure'larinin
    # body'sinde dogal olarak gecer ve yanlislikla halka_arz takip-bildirimi
    # zannedip mega-pozitif kararlari (orn. GK ile %500 bedelsiz) Notr'a cekiyordu.
    "halka_arz": ["halka arz", "halka acilma", "halka açılma"],
    "sözleşme": ["sözleşme imzaland", "sozlesme imzaland", "anlaşma imzaland", "ihale kazan", "ihale alın"],
    "satın_alma": ["satın al", "satin al", "iktisap", "devralın", "devralin"],
    "yeni_iş_ilişkisi": ["yeni iş ilişkisi", "yeni is iliskisi", "yeni müşteri", "yeni musteri"],
    "kapasite": ["kapasite artır", "kapasite artir", "yeni tesis", "yatırım planı"],
    "pay_geri_alimi": [
        "pay geri alım", "pay geri alim",
        "geri alım programı", "geri alim programi",
        "kendi paylarini geri", "kendi paylarını geri",
        "buyback",
    ],
}


async def _check_followup_notification(ticker: str, content: str) -> tuple[bool, str | None]:
    """Son 30 gunde ayni ticker icin ayni konuda yuksek skorlu (>=6.0) ya da
    cok dusuk skorlu (<=3.5) bildirim varsa True doner — bu yeni bildirim
    takip-bildirimdir.

    Window 30 gun: temettu/bedelli/bedelsiz prosedur bildirimleri ilk karardan
    haftalar/aylar sonra gelir (GK karari -> SPK -> ihraç belgesi -> kullanim ->
    tescil zinciri 60+ gune yayilabilir; ama dampera 30 gun pratik standartdir).

    Returns: (is_followup, topic_name)
    """
    if not ticker or not content:
        return (False, None)

    content_lower = content.lower()
    new_topics = []
    for topic, keywords in _FOLLOWUP_TOPICS.items():
        if any(kw in content_lower for kw in keywords):
            new_topics.append(topic)
    if not new_topics:
        return (False, None)

    try:
        from app.database import async_session
        from app.models.kap_all_disclosure import KapAllDisclosure
        from sqlalchemy import select, desc, or_, and_
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        async with async_session() as db:
            # Hem pozitif (>=6.0) hem ciddi negatif (<=3.5) ilk kararlari kapsa —
            # bedelli sermaye artirimi gibi negatif kararin sonraki adimlari da
            # tekrar negatif puanlanmasin.
            result = await db.execute(
                select(KapAllDisclosure)
                .where(KapAllDisclosure.company_code == ticker.upper())
                .where(KapAllDisclosure.published_at >= cutoff)
                .where(
                    or_(
                        KapAllDisclosure.ai_impact_score >= 6.0,
                        KapAllDisclosure.ai_impact_score <= 3.5,
                    )
                )
                .order_by(desc(KapAllDisclosure.published_at))
                .limit(30)
            )
            recent = result.scalars().all()

            for prior in recent:
                prior_text = ((prior.title or "") + " " + (prior.body or "") + " " + (prior.ai_summary or "")).lower()
                for topic in new_topics:
                    keywords = _FOLLOWUP_TOPICS[topic]
                    if any(kw in prior_text for kw in keywords):
                        return (True, topic)
    except Exception as e:
        logger.debug("Followup DB sorgu hata (%s): %s", ticker, e)

    return (False, None)


def _extract_dividend_yield_pct(content: str) -> float | None:
    """Icerikten temettu yield% degerini cikar (varsa).

    Pattern'lar:
      "kar payi orani %19.87" -> 19.87
      "verim %14.3" / "yield %8.2" -> tek sayi
      "Pay basina brut 5 TL" + "fiyat 50 TL" → asla — yield ozette agirlikla yazilir
    """
    if not content:
        return None
    lc = content.lower()
    # Birden cok pattern dene
    patterns = [
        r"(?:kar\s*pay[ıi]\s*oran[ıi]?|temettu\s*verim|yield|verim|brut\s*verim|net\s*verim)\s*%?\s*([0-9]{1,3}(?:[.,][0-9]+)?)",
        r"%\s*([0-9]{1,3}(?:[.,][0-9]+)?)\s*(?:seviye|brut\s*verim|net\s*verim|temettu\s*verim)",
    ]
    for pat in patterns:
        m = re.search(pat, lc)
        if m:
            try:
                val = float(m.group(1).replace(",", "."))
                if 0 <= val <= 100:
                    return val
            except (ValueError, TypeError):
                pass
    return None


def _validate_score_against_content(score: float, content: str, ticker: str) -> float:
    """Icerik patirnlerine gore skoru dogrular ve gerekirse duzeltir.

    Kritik negatif haberler icin skoru tavan sinirlar,
    guclu pozitif haberler icin taban garantisi uygular.
    Notr bildirimler (devre kesici vb.) icin 5.0'a ceker.
    """
    content_lower = content.lower()

    # ─── KURUMSAL YONETIM DERECELENDIRME — HARD CAP (max 6.5) ──────
    # Bu notlar (SAHA, JCR-Eurasia vs.) sirketin yatirimci iliskileri/raporlama
    # kalitesini olcer — fiyat etkisi minimaldir. AI bunlara 7.0+ verirse fazla
    # yuksek olur. Maximum Hafif Olumlu (6.4) seviyesine cek.
    is_governance_rating = (
        ("kurumsal yonetim" in content_lower or "kurumsal yönetim" in content_lower)
        and ("derecelendirme" in content_lower or "rating" in content_lower or "not" in content_lower)
    )
    if is_governance_rating and score > 6.4:
        logger.info(
            "AI News Scorer [GOVERNANCE-CAP] %s: %.1f -> 6.4 "
            "(kurumsal yonetim derecelendirme max Hafif Olumlu)",
            ticker, score,
        )
        score = 6.4

    # ─── KREDI DERECELENDIRME — NOTR'a CEK (çok büyük artırım yoksa) ──────
    # Fitch, Moody's, S&P, JCR gibi kuruluşların kredi notu açıklamaları
    # genelde önceden beklenmektedir, fiyat etkisi sınırlı. Kullanıcı isteği:
    # çok büyük not değişikliği yoksa → NOTR (5.0).
    rating_agencies = (
        "fitch", "moody", "s&p", "standard & poor", "standard&poor",
        "jcr", "saha", "scope", "kredi notu", "credit rating",
    )
    is_credit_rating = (
        any(ag in content_lower for ag in rating_agencies)
        and not is_governance_rating  # zaten cap'lendi
    )
    if is_credit_rating:
        # Çok büyük not değişikliği indikatörleri
        big_upgrade = any(kw in content_lower for kw in [
            "yatırım yapılabilir", "yatirim yapilabilir",
            "yatırım yapılabilir kategori", "investment grade",
            "iki kademe", "üç kademe", "uc kademe",
            "iki basamak", "üç basamak", "uc basamak",
            "2 kademe yükselt", "3 kademe yükselt",
            "görünüm pozitif", "görünüm pozitife",
        ])
        big_downgrade = any(kw in content_lower for kw in [
            "yatırım dışı", "spekülatif kategori",
            "junk", "default", "temerrüt", "temerrut",
            "görünüm negatif", "görünüm negatife",
            "iki kademe düşür", "iki kademe düşür",
        ])
        if not big_upgrade and not big_downgrade:
            # Küçük değişiklik / teyit / stabil → NOTR'a çek
            if score > 5.4 or score < 4.6:
                old_score = score
                score = 5.0
                logger.info(
                    "AI News Scorer [CREDIT-RATING-NEUTRAL] %s: %.1f -> 5.0 "
                    "(kredi derecelendirme + büyük değişiklik yok = Notr)",
                    ticker, old_score,
                )
        elif big_upgrade:
            # Büyük artırım → max 7.5 (Olumlu)
            if score > 7.5:
                logger.info(
                    "AI News Scorer [CREDIT-UPGRADE-CAP] %s: %.1f -> 7.5",
                    ticker, score,
                )
                score = 7.5
        elif big_downgrade:
            # Büyük düşürme → min 3.0 (Olumsuz)
            if score > 3.5:
                logger.info(
                    "AI News Scorer [CREDIT-DOWNGRADE-FLOOR] %s: %.1f -> 3.0",
                    ticker, score,
                )
                score = 3.0

    # ─── YENI IS ILISKISI / SOZLESME — Mutlak tutar HARD FLOOR ──────
    # AI'in 6.0-6.5 kumelemesini zorla cozer. Tutar tespit edilirse minimum skor garanti.
    is_yeni_is = any(kw in content_lower for kw in [
        "yeni is iliskisi", "yeni iş ilişkisi",
        "sozlesme imzaland", "sözleşme imzalan",
        "anlasma imzaland", "anlaşma imzalan",
        "ihale kazan", "ihale al",
        "siparis ald", "sipariş aldı",
        "tedarik anlasm", "tedarik anlaşm",
        "yeni musteri", "yeni müşteri",
        "is ortakligi", "iş ortaklığı",
    ])
    if is_yeni_is:
        # TL tutari cikar — milyon/milyar bazli
        amount_tl_m = None  # milyon TL bazli
        # "X milyon TL" / "X milyar TL"
        m1 = re.search(r"(\d+(?:[.,]\d+)?)\s*milyar\s*tl", content_lower)
        if m1:
            try:
                amount_tl_m = float(m1.group(1).replace(",", ".")) * 1000
            except (ValueError, TypeError):
                pass
        if amount_tl_m is None:
            m2 = re.search(r"(\d+(?:[.,]\d+)?)\s*milyon\s*tl", content_lower)
            if m2:
                try:
                    amount_tl_m = float(m2.group(1).replace(",", "."))
                except (ValueError, TypeError):
                    pass
        # USD/EUR varsa TL'ye cevir (yaklasik: 1 USD = 40 TL, 1 EUR = 43 TL)
        if amount_tl_m is None:
            m_usd = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:milyon\s*)?usd", content_lower)
            if m_usd:
                try:
                    val = float(m_usd.group(1).replace(",", "."))
                    # "milyon USD" mu yoksa "USD" mi?
                    if "milyon" in content_lower[:max(0, m_usd.start()-20):m_usd.end()+5]:
                        amount_tl_m = val * 40  # milyon USD * 40 TL = milyon TL
                    elif val > 100_000:
                        amount_tl_m = (val * 40) / 1_000_000  # USD → milyon TL
                except (ValueError, TypeError):
                    pass

        if amount_tl_m is not None:
            # Mutlak tutara gore hard floor (milyon TL bazli)
            if amount_tl_m >= 1000 and score < 8.5:
                logger.info("Skor [YENI-IS]: %s %.0fM TL -> 8.5", ticker, amount_tl_m)
                return 8.5
            elif amount_tl_m >= 500 and score < 8.0:
                logger.info("Skor [YENI-IS]: %s %.0fM TL -> 8.0", ticker, amount_tl_m)
                return 8.0
            elif amount_tl_m >= 200 and score < 7.5:
                logger.info("Skor [YENI-IS]: %s %.0fM TL -> 7.5", ticker, amount_tl_m)
                return 7.5
            elif amount_tl_m >= 100 and score < 7.2:
                logger.info("Skor [YENI-IS]: %s %.0fM TL -> 7.2", ticker, amount_tl_m)
                return 7.2
            elif amount_tl_m >= 50 and score < 7.0:
                logger.info("Skor [YENI-IS]: %s %.0fM TL -> 7.0", ticker, amount_tl_m)
                return 7.0
            elif amount_tl_m >= 25 and score < 6.7:
                logger.info("Skor [YENI-IS]: %s %.0fM TL -> 6.7", ticker, amount_tl_m)
                return 6.7
            elif amount_tl_m >= 10 and score < 6.5:
                return 6.5

    # ─── Kurumsal block alim — HARD FLOOR ──────────────────────────────
    # Yatirim/portfoy fonu %5 esigi asar veya buyuk net alim yaparsa → 7.0+ zorunlu
    # PEKGY/TATEN tipi vakalari yakala
    is_kurumsal_alim = (
        ("portfoy yonetimi" in content_lower or "portföy yönetimi" in content_lower or "fonlar" in content_lower)
        and ("alim" in content_lower or "alım" in content_lower or "satın" in content_lower)
        and ("yukseldi" in content_lower or "yükseldi" in content_lower or "esik" in content_lower or "eşik" in content_lower)
    )
    if is_kurumsal_alim:
        # Tutar tespiti — milyon TL bazli
        m_amount = re.search(r"(\d+(?:[.,]\d+)?)\s*milyon\s*tl\s*(?:nominal|tutar)", content_lower)
        if m_amount:
            try:
                amount_m = float(m_amount.group(1).replace(",", "."))
                if amount_m >= 100 and score < 7.5:
                    logger.info(
                        "Skor dogrulama [KURUMSAL-ALIM]: %s %dM TL net alim, skor %.1f -> 7.5",
                        ticker, amount_m, score,
                    )
                    return 7.5
                elif amount_m >= 50 and score < 7.0:
                    logger.info(
                        "Skor dogrulama [KURUMSAL-ALIM]: %s %dM TL net alim, skor %.1f -> 7.0",
                        ticker, amount_m, score,
                    )
                    return 7.0
                elif amount_m >= 25 and score < 6.7:
                    return 6.7
            except (ValueError, TypeError):
                pass

    # ─── Temettu yield% bazli HARD FLOOR ──────────────────────────────
    # OZKGY-tipi vakayi engelle: %19.87 yield iken AI 7.2 vermesin
    yield_pct = _extract_dividend_yield_pct(content)
    if yield_pct is not None:
        # Sadece temettu bildirimi olduğundan emin ol
        if any(kw in content_lower for kw in ["kar payi", "kar payı", "temettu", "temettü", "pay basina", "pay başına"]):
            if yield_pct >= 20:
                min_floor = 9.0
            elif yield_pct >= 10:
                min_floor = 8.5
            elif yield_pct >= 7:
                min_floor = 7.8
            elif yield_pct >= 5:
                min_floor = 7.0
            else:
                min_floor = 0  # Dusuk yield icin floor uygulama
            if min_floor > 0 and score < min_floor:
                logger.info(
                    "Skor dogrulama [TEMETTU-YIELD]: %s yield=%%%.2f, skor %.1f -> %.1f",
                    ticker, yield_pct, score, min_floor,
                )
                return min_floor

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

    # Bedelli sermaye artirimi — skor asla tavanin uzerine cikamaz (her zaman negatif)
    for pattern, max_score in _BEDELLI_NEGATIVE_CAPS:
        if re.search(pattern, content_lower):
            if score > max_score:
                logger.info(
                    "Skor dogrulama [BEDELLI]: %s skor %.1f → %.1f (oran patterni: %s)",
                    ticker, score, max_score, pattern[:40],
                )
                return max_score

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

    # ★ BUYBACK BYPASS: Pay geri alimi bildirimleri AI'a gitmeden once
    # deterministik olarak skorlanir. TL tutarina gore esik bazli skor +
    # standart ozet. AI cagrilmaz — hizli, ucuz, dogru. Tablo karismasi yok.
    try:
        # raw_text'in baslarinda title olur (orn "⚡ Seans Disi Pozitif Haber Yakalandi - ENERY\nPaylarin Geri Alinmasina Iliskin Bildirim ...")
        _title_check = raw_text[:300] if raw_text else ""
        from app.services.buyback_processor import is_buyback as _is_bb
        if _is_bb(_title_check) and ticker:
            # KAP body fetch (TradingView eski/silinmis olabilir, KAP direkt deneyelim)
            _bb_body = ""
            if matriks_id:
                try:
                    _tv = await fetch_tradingview_content(matriks_id)
                    if _tv and _tv.get("full_text"):
                        _bb_body = _tv["full_text"]
                        if _tv.get("real_kap_url"):
                            kap_url = _tv["real_kap_url"]
                except Exception:
                    pass
            if not _bb_body:
                try:
                    _kd = await fetch_kap_direct_content(ticker)
                    if _kd:
                        _bb_body = _kd.get("full_text") or ""
                        if _kd.get("kap_url"):
                            kap_url = _kd["kap_url"]
                except Exception:
                    pass
            if _bb_body:
                from app.services.buyback_processor import (
                    parse_buyback_today, buyback_score_and_summary,
                )
                _bb_parsed = parse_buyback_today(_bb_body)
                if _bb_parsed and _bb_parsed.get("lot"):
                    _lot = _bb_parsed["lot"]
                    _pavg = _bb_parsed.get("price_avg") or 0
                    if not _pavg and _bb_parsed.get("price_low") and _bb_parsed.get("price_high"):
                        _pavg = (_bb_parsed["price_low"] + _bb_parsed["price_high"]) / 2
                    _bb_parsed["total_tl"] = _lot * _pavg if _pavg else 0
                    if _pavg:
                        _bb_parsed["price_avg"] = _pavg
                    _bb_score, _bb_summary = buyback_score_and_summary(_bb_parsed, ticker)
                    logger.info(
                        "Buyback deterministik skor: %s — lot=%s avg=%.2f total=%.0f -> %.1f",
                        ticker, _lot, _pavg or 0, _bb_parsed.get("total_tl", 0), _bb_score,
                    )
                    return {
                        "score": _bb_score,
                        "summary": _bb_summary,
                        "kap_url": kap_url,
                        "hashtags": ["paygerialim"],
                    }
                else:
                    logger.info(
                        "Buyback parse fail (lot/fiyat cikarilamadi) — AI scorer'a devam: %s",
                        ticker,
                    )
    except Exception as _bb_err:
        logger.warning("Buyback bypass hata (%s): %s — normal akisa donulu yor", ticker, _bb_err)

    # ── Oncelik 1: TradingView'dan KAP URL'yi cikart, icerik varsa kullan ──
    if matriks_id:
        kap_url = f"https://tr.tradingview.com/news/matriks:{matriks_id}:0/"
        try:
            tv_result = await fetch_tradingview_content(matriks_id)
            if tv_result:
                # KAP URL her zaman al (paywall olsa bile link HTML'de bulunur)
                if tv_result.get("real_kap_url"):
                    kap_url = tv_result["real_kap_url"]
                    logger.info("KAP linki TV'den alindi: %s → %s", ticker, kap_url)
                # Icerik sadece paywall degil ve doluysa kullan
                if tv_result.get("full_text"):
                    tv_content = tv_result["full_text"]
                    logger.info(
                        "TradingView icerik basarili: %s → matriks:%s (%d karakter)",
                        ticker, matriks_id, len(tv_content),
                    )
        except Exception as e:
            logger.warning("TradingView hatasi (%s): %s", ticker, e)

    # ── Oncelik 2: KAP.org.tr direkt URL ile icerik cek (TV paywall veya basarisizsa) ──
    # TV'den real_kap_url alindiysa DIREKT o URL'e git — ticker bazli degil, spesifik bildirim
    if not tv_content and kap_url and "kap.org.tr" in kap_url:
        try:
            from app.scrapers.kap_all_scraper import fetch_kap_page_content as _fkpc
            kap_direct_text = await _fkpc(kap_url)
            if kap_direct_text and len(kap_direct_text) > 50:
                tv_content = kap_direct_text
                logger.info(
                    "KAP.org.tr direkt URL basarili: %s → %s (%d karakter)",
                    ticker, kap_url, len(tv_content),
                )
        except Exception as e:
            logger.warning("KAP URL direkt hatasi (%s): %s", ticker, e)

    # ── Oncelik 3: KAP.org.tr ticker bazli (spesifik URL de basarisizsa) ──
    if not tv_content and ticker:
        try:
            kap_result = await fetch_kap_direct_content(ticker)
            if kap_result:
                if kap_result.get("kap_url") and "kap.org.tr" not in (kap_url or ""):
                    kap_url = kap_result["kap_url"]
                if kap_result.get("full_text") and len(kap_result["full_text"]) > 30:
                    tv_content = kap_result["full_text"]
                    logger.info(
                        "KAP ticker-bazli fallback basarili: %s (%d karakter)",
                        ticker, len(tv_content),
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
