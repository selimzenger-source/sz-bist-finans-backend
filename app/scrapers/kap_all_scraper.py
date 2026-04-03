"""KAP Haberleri Scraper — Uzmanpara (ana) + Mynet Finans (yedek).

Akis:
1. Uzmanpara listing sayfasindan yeni KAP bildirimlerini alir
2. 2 ard arda Uzmanpara hatasi -> Mynet Finans yedek kaynaga gecer
3. Yeni bildirim icin detay sayfasindan KAP.org.tr linki + icerik cekilir (AI icin)

Her ~50 saniyede bir calisir (scheduler).
"""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_TR_TZ = ZoneInfo("Europe/Istanbul")

# ─── URL'ler ────────────────────────────────────────────────────────────────
UZMANPARA_URL = "https://uzmanpara.milliyet.com.tr/kap-haberleri/"
MYNET_URL = "https://finans.mynet.com/borsa/kaphaberleri/"
MYNET_DETAIL_BASE = "https://finans.mynet.com/borsa/haberdetay/"
BIST_INDEX_BASE = "https://bigpara.hurriyet.com.tr/borsa/hisse-fiyatlari/bist-tum-endeksi/"

# ─── Failure tracking — 2 ard arda hata -> yedek kaynaga gec ──────────────
_uzmanpara_fail_count: int = 0
_FAILOVER_THRESHOLD = 2  # 2 ard arda hata sonrasi Mynet'e gec

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ─── BIST whitelist (12 saat cache) ────────────────────────────────────────
_BIST_SYMBOLS: set[str] = set()
_BIST_TS: float = 0.0
_BIST_TTL = 12 * 3600


async def _refresh_bist_symbols() -> set[str]:
    """BigPara BIST Tum endeksinden tum hisse kodlarini ceker (sayfali)."""
    global _BIST_SYMBOLS, _BIST_TS

    now = time.time()
    if _BIST_SYMBOLS and (now - _BIST_TS < _BIST_TTL):
        return _BIST_SYMBOLS

    symbols: set[str] = set()
    page = 1
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, headers=HEADERS) as client:
        while page <= 20:
            url = BIST_INDEX_BASE if page == 1 else f"{BIST_INDEX_BASE}{page}/"
            try:
                r = await client.get(url)
                soup = BeautifulSoup(r.text, "lxml")
                links = soup.find_all(
                    "a", href=lambda h: h and "/borsa/hisse-fiyatlari/" in h and "-detay/" in h
                )
                codes = {
                    a.get_text(strip=True)
                    for a in links
                    if re.match(r"^[A-Z][A-Z0-9]{1,9}$", a.get_text(strip=True))
                }
                if not codes:
                    break
                symbols.update(codes)
                page += 1
            except Exception as exc:
                logger.warning("BIST sayfa %d hata: %s", page, exc)
                break

    if symbols:
        _BIST_SYMBOLS = symbols
        _BIST_TS = now
        logger.info("BIST whitelist guncellendi: %d sembol", len(symbols))
    else:
        logger.warning("BIST whitelist bos geldi, eski liste korunuyor (%d)", len(_BIST_SYMBOLS))

    return _BIST_SYMBOLS


# ═════════════════════════════════════════════════════════════════════════════
# Uzmanpara — tek kaynak (anlik takip icin)
# ═════════════════════════════════════════════════════════════════════════════

async def _uzmanpara_fetch() -> list[dict[str, Any]]:
    """Uzmanpara KAP haberleri listing sayfasindan bildirim listesi.

    Sadece listing sayfasini tarar — detay sayfasi ACILMAZ.

    HTML yapisi:
      <li>
        <a class="hisse" href="/kap-haberi/.../">TICKER</a>
        <a href="/kap-haberi/.../">Baslik</a>
        <span class="date">01.03.2026<br/>20:31:25</span>
      </li>

    Returns:
        [{title, company_code, kap_url, published_at, source, category, is_bilanco}, ...]
    """
    global _uzmanpara_fail_count

    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True, headers=HEADERS) as client:
        try:
            r = await client.get(UZMANPARA_URL)
            r.raise_for_status()
        except Exception as exc:
            _uzmanpara_fail_count += 1
            logger.warning("Uzmanpara hata (#%d): %s", _uzmanpara_fail_count, exc)
            return []

        soup = BeautifulSoup(r.text, "lxml")

        # Ticker linklerini bul (class="hisse")
        hisse_links = soup.find_all("a", class_="hisse", href=re.compile(r"/kap-haberi/"))
        if not hisse_links:
            _uzmanpara_fail_count += 1
            logger.warning("Uzmanpara: haber linki bulunamadi (#%d)", _uzmanpara_fail_count)
            return []

        # Basarili — counter sifirla
        _uzmanpara_fail_count = 0

        results: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for a_hisse in hisse_links:
            if len(results) >= 30:
                break

            li = a_hisse.parent
            if not li or li.name != "li":
                continue

            company_code = a_hisse.get_text(strip=True).upper()
            if not company_code or len(company_code) < 2 or len(company_code) > 10:
                continue

            # Baslik: ikinci <a> (class="hisse" olmayan)
            all_links = li.find_all("a", href=re.compile(r"/kap-haberi/"))
            title = ""
            detail_href = ""
            for link in all_links:
                if "hisse" not in (link.get("class") or []):
                    title = link.get_text(strip=True)
                    detail_href = link.get("href", "")
                    break

            if not title:
                continue

            # Uzmanpara detay URL'si (KAP bildirim ID sonra cekilecek)
            uzmanpara_detail_url = (
                f"https://uzmanpara.milliyet.com.tr{detail_href}"
                if detail_href.startswith("/") else detail_href
            )
            kap_url = uzmanpara_detail_url

            # Tarih — Uzmanpara Turkey saati gosterir
            date_span = li.find("span", class_="date")
            published_at = None
            if date_span:
                date_text = date_span.get_text(" ", strip=True)
                try:
                    naive_dt = datetime.strptime(date_text, "%d.%m.%Y %H:%M:%S")
                    # Turkey saati olarak sakla — UTC'ye cevirme
                    published_at = naive_dt.replace(tzinfo=_TR_TZ)
                except ValueError:
                    pass

            # Dedup
            dedup_key = f"{company_code}|{title[:40]}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            # Kategori cikar
            category = _infer_category(title)
            is_bilanco = category in ("Bilanço/Finansal Rapor", "Faaliyet Raporu")

            item = {
                "title": title,
                "company_code": company_code,
                "body": "",  # Body bos — AI analizi KAP sayfasindan yapilacak
                "kap_url": kap_url,
                "category": category,
                "is_bilanco": is_bilanco,
                "source": "uzmanpara",
            }
            if published_at:
                item["published_at"] = published_at

            results.append(item)

    logger.info("Uzmanpara -> %d haber", len(results))
    return results


# ═════════════════════════════════════════════════════════════════════════════
# Mynet Finans — yedek kaynak (Uzmanpara 2x ard arda fail ederse)
# ═════════════════════════════════════════════════════════════════════════════

_TR_MONTHS = {
    "Oca": 1, "Şub": 2, "Mar": 3, "Nis": 4, "May": 5, "Haz": 6,
    "Tem": 7, "Ağu": 8, "Eyl": 9, "Eki": 10, "Kas": 11, "Ara": 12,
}


async def _mynet_fetch() -> list[dict[str, Any]]:
    """Mynet Finans KAP haberleri listing — yedek kaynak.

    HTML yapisi:
      <li>
        <a href="https://finans.mynet.com/borsa/haberdetay/HEXID/"
           data-id="HEXID"
           title="***TICKER*** SIRKET ADI (Bildirim Basligi)">
          <em class="title">***TICKER*** SIRKET ADI (Bildirim Basligi)</em>
          <span class="date">02 Mar 2026 19:32</span>
        </a>
      </li>
    """
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True, headers=HEADERS) as client:
        try:
            r = await client.get(MYNET_URL)
            r.raise_for_status()
        except Exception as exc:
            logger.warning("Mynet Finans hata: %s", exc)
            return []

        soup = BeautifulSoup(r.text, "lxml")

        # data-action="news-loader" altindaki <li> ogelerini bul
        news_ul = soup.find("ul", id="new-list-ul")
        if not news_ul:
            logger.warning("Mynet: haber listesi bulunamadi")
            return []

        results: list[dict[str, Any]] = []
        seen_keys: set[str] = set()

        for li in news_ul.find_all("li", limit=40):
            a_tag = li.find("a", href=True)
            if not a_tag:
                continue

            raw_title = a_tag.get("title", "") or ""
            if not raw_title:
                em = a_tag.find("em", class_="title")
                raw_title = em.get_text(strip=True) if em else ""

            if not raw_title:
                continue

            # Ticker cikar: ***TICKER*** veya ***T1 ** T2*** formatinda
            # Ilk ticker kodu yeterli
            ticker_match = re.search(r"\*{3}(\w{2,10})", raw_title)
            if not ticker_match:
                continue
            company_code = ticker_match.group(1).upper()

            # Baslik cikar: parantez icindeki son kisim (greedy — ic ice parantez destekli)
            title_match = re.search(r"\((.+)\)\s*$", raw_title)
            title = title_match.group(1).strip() if title_match else raw_title
            # Baslik cok uzunsa kisalt
            if len(title) > 120:
                title = title[:117] + "..."

            # Detay URL
            detail_url = a_tag.get("href", "")
            if not detail_url.startswith("http"):
                detail_url = f"https://finans.mynet.com{detail_url}"

            # Tarih — "02 Mar 2026 19:32" formatinda
            date_span = a_tag.find("span", class_="date")
            published_at = None
            if date_span:
                date_text = date_span.get_text(strip=True)
                published_at = _parse_mynet_date(date_text)

            # Dedup
            dedup_key = f"{company_code}|{title[:40]}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            category = _infer_category(title)
            is_bilanco = category in ("Bilanço/Finansal Rapor", "Faaliyet Raporu")

            item = {
                "title": title,
                "company_code": company_code,
                "body": "",
                "kap_url": detail_url,  # Mynet detay URL — KAP linki yok
                "category": category,
                "is_bilanco": is_bilanco,
                "source": "mynet",
            }
            if published_at:
                item["published_at"] = published_at

            results.append(item)

    logger.info("Mynet Finans -> %d haber (yedek kaynak)", len(results))
    return results


def _parse_mynet_date(text: str) -> datetime | None:
    """Mynet tarih formatini parse eder: '02 Mar 2026 19:32'."""
    try:
        # "02 Mar 2026 19:32"
        parts = text.strip().split()
        if len(parts) < 4:
            return None
        day = int(parts[0])
        month = _TR_MONTHS.get(parts[1], 0)
        if not month:
            return None
        year = int(parts[2])
        time_parts = parts[3].split(":")
        hour = int(time_parts[0])
        minute = int(time_parts[1]) if len(time_parts) > 1 else 0
        naive_dt = datetime(year, month, day, hour, minute)
        return naive_dt.replace(tzinfo=_TR_TZ)
    except (ValueError, IndexError):
        return None


async def fetch_mynet_detail_content(detail_url: str) -> str:
    """Mynet Finans detay sayfasindan bildirim icerigi cekmek (AI icin).

    Mynet detay sayfasinda KAP bildiriminin tam icerigi tablo formatinda var.
    """
    if not detail_url or "mynet" not in detail_url:
        return ""

    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True, headers=HEADERS) as client:
            r = await client.get(detail_url)
            if r.status_code != 200:
                return ""

            soup = BeautifulSoup(r.text, "lxml")

            # KAP icerik: div.dataText.tblKAP
            content_div = soup.find("div", class_="tblKAP")
            if not content_div:
                content_div = soup.find("div", class_="kap-detail-news-page")

            if not content_div:
                return ""

            # Script/style kaldir
            for tag in content_div(["script", "style"]):
                tag.decompose()

            import html as _html
            text = _html.unescape(content_div.get_text(" ", strip=True))
            if text and len(text) > 30:
                logger.info("Mynet detay icerigi: %d karakter", len(text))
                return text[:5000]

    except Exception as exc:
        logger.debug("Mynet detay hatasi: %s", exc)

    return ""


# ═════════════════════════════════════════════════════════════════════════════
# Uzmanpara detay sayfasindan KAP bildirim ID cikartma
# ═════════════════════════════════════════════════════════════════════════════

async def resolve_kap_url(uzmanpara_url: str) -> str:
    """Uzmanpara detay sayfasindan KAP.org.tr bildirim linkini cikarir.

    Uzmanpara URL format degisikligi (2026): hex MongoDB ID kullaniliyor.
    Detay sayfasinda 'kap.org.tr/Bildirim/XXXXXXX' linki mevcut.

    Args:
        uzmanpara_url: https://uzmanpara.milliyet.com.tr/kap-haberi/.../HEXID/

    Returns:
        https://www.kap.org.tr/tr/Bildirim/1564332 veya bos string
    """
    if not uzmanpara_url or "uzmanpara" not in uzmanpara_url:
        return ""

    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True, headers=HEADERS) as client:
            r = await client.get(uzmanpara_url)
            if r.status_code != 200:
                return ""

            # kap.org.tr/Bildirim/1564332 veya kap.org.tr/tr/Bildirim/1564332
            match = re.search(r"kap\.org\.tr/(?:tr/)?Bildirim/(\d+)", r.text)
            if match:
                kap_id = match.group(1)
                return f"https://www.kap.org.tr/tr/Bildirim/{kap_id}"

    except Exception as exc:
        logger.debug("Uzmanpara detay KAP ID hatasi: %s", exc)

    return ""


# ═════════════════════════════════════════════════════════════════════════════
# KAP.org.tr sayfa icerigi (AI analizi icin)
# ═════════════════════════════════════════════════════════════════════════════

async def fetch_kap_page_content(kap_url: str) -> str:
    """KAP.org.tr bildirim sayfasindan icerik cekmek.

    kap.org.tr API endpoint'i kullanir — HTML degil JSON donebilir.
    Bildirim metni, sirket bilgileri, tarih vb. cekilir.

    Args:
        kap_url: https://www.kap.org.tr/tr/Bildirim/1564254

    Returns:
        Bildirim icerigi (text) veya bos string
    """
    if not kap_url or "kap.org.tr" not in kap_url:
        return ""

    # KAP bildirim ID'yi cikar
    m = re.search(r"Bildirim/(\d+)", kap_url)
    if not m:
        return ""

    bildirim_id = m.group(1)

    # KAP API endpoint — bildirim detay JSON
    # kap.org.tr/tr/Bildirim/XXXXX sayfasi React/Angular — direkt HTML yetersiz
    # Bunun yerine KAP'in arka plan API'sini deneriz
    api_urls = [
        f"https://www.kap.org.tr/tr/Bildirim/{bildirim_id}",
    ]

    headers = {
        **HEADERS,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.kap.org.tr/",
    }

    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers=headers) as client:
        for url in api_urls:
            try:
                r = await client.get(url)
                if r.status_code != 200:
                    logger.debug("KAP sayfa %s HTTP %d", bildirim_id, r.status_code)
                    continue

                soup = BeautifulSoup(r.text, "lxml")

                # KAP sayfasi genellikle su div'lerde icerik tasiyor:
                # - div.modal-info (bildirim ozeti)
                # - div.disclosure-text (tam metin)
                # - div#bildirimDetayIcerik
                # - div.w-clearfix (bildirim kutulari)
                body_parts = []

                # Method 1: Tum metin kutularini topla
                for sel in [
                    "div.modal-info",
                    "div.disclosure-text",
                    "div#bildirimDetayIcerik",
                    "div.w-clearfix.comp-cell-row-div",
                    "div.bildirimContent",
                ]:
                    for div in soup.select(sel):
                        text = div.get_text(" ", strip=True)
                        if text and len(text) > 10:
                            body_parts.append(text)

                # Method 2: Genel body text (fallback)
                if not body_parts:
                    # Tum sayfa text'ini al (HTML'den temizle)
                    for tag in soup(["script", "style", "nav", "footer", "header"]):
                        tag.decompose()
                    page_text = soup.get_text(" ", strip=True)
                    # Sadece anlamli kisimlari al (min 50 karakter)
                    if page_text and len(page_text) > 50:
                        # Cok uzun ise ilk 3000 karakteri al
                        body_parts.append(page_text[:3000])

                body = "\n".join(body_parts).strip()
                if body and len(body) > 20:
                    logger.info("KAP sayfa icerigi alindi: %s (%d karakter)", bildirim_id, len(body))
                    return body[:4000]  # Max 4000 karakter (AI prompt siniri)

            except Exception as exc:
                logger.debug("KAP sayfa hatasi (%s): %s", bildirim_id, exc)
                continue

    logger.debug("KAP sayfa icerigi alinamadi: %s", bildirim_id)
    return ""


# ═════════════════════════════════════════════════════════════════════════════
# Ortak yardimcilar
# ═════════════════════════════════════════════════════════════════════════════

def _infer_category(title: str) -> str:
    """Bildirim basligindan kategori cikarir."""
    t = title.lower()
    if any(k in t for k in ["finansal rapor", "bilanço", "finansal tablo", "sorumluluk beyanı", "mali tablo"]):
        return "Bilanço/Finansal Rapor"
    if any(k in t for k in ["temettü", "kar payı", "kâr payı"]):
        return "Temettü"
    if "genel kurul" in t:
        return "Genel Kurul"
    if "özel durum" in t:
        return "Özel Durum Açıklaması"
    if any(k in t for k in ["sermaye artırımı", "sermaye azaltımı"]):
        return "Sermaye Artırımı"
    if any(k in t for k in ["kurumsal yönetim", "uyum raporu"]):
        return "Kurumsal Yönetim"
    if any(k in t for k in ["bilgi formu", "genel bilgi"]):
        return "Bilgi Formu"
    if any(k in t for k in ["yönetim kurulu", "komite"]):
        return "Yönetim Kurulu"
    if "denetim" in t:
        return "Bağımsız Denetim"
    if "faaliyet raporu" in t:
        return "Faaliyet Raporu"
    if "devre kesici" in t:
        return "Devre Kesici"
    if any(k in t for k in ["ihraç belgesi", "tahvil", "bono"]):
        return "Borçlanma Aracı"
    if "likidite sağlayıcılık" in t:
        return "Likidite Sağlayıcılık"
    if "esas sözleşme" in t:
        return "Esas Sözleşme"
    if "tasarruf sahiplerine" in t:
        return "Halka Arz"
    return "Genel"


def _extract_kap_id(kap_url: str) -> str:
    """KAP URL'den bildirim numarasini cikarir (dedup icin)."""
    m = re.search(r"Bildirim/(\d+)", kap_url)
    return m.group(1) if m else ""


def _apply_bist_filter(data: list[dict[str, Any]], bist: set[str]) -> list[dict[str, Any]]:
    """BIST whitelist filtresi uygular."""
    if not bist:
        return data
    filtered = [d for d in data if d.get("company_code", "").strip() in bist]
    logger.info("BIST filtre: %d -> %d haber", len(data), len(filtered))
    return filtered


# ═════════════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════════════

async def scrape_uzmanpara_only() -> list[dict[str, Any]]:
    """KAP haberlerini ceker — Uzmanpara (ana) + Mynet Finans (yedek).

    Her ~50 saniyede bir cagirilir.
    Uzmanpara 2 ard arda fail ederse Mynet Finans yedek kaynaga gecer.
    BIST whitelist ile filtrelenir.

    Returns:
        list[dict]: Bildirim listesi
    """
    bist = await _refresh_bist_symbols()

    # Ana kaynak: Uzmanpara
    data = await _uzmanpara_fetch()

    # Yedek kaynak: Mynet Finans (2 ard arda hata sonrasi)
    if not data and _uzmanpara_fail_count >= _FAILOVER_THRESHOLD:
        logger.warning(
            "Uzmanpara %d kez basarisiz — Mynet Finans yedek kaynaga geciliyor",
            _uzmanpara_fail_count,
        )
        data = await _mynet_fetch()

    if not data:
        return []

    data = _apply_bist_filter(data, bist)
    return data[:30]
