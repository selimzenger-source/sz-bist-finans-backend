"""KAP Haberleri Scraper — Uzmanpara listing + KAP.org.tr AI analizi.

Akis:
1. Uzmanpara listing sayfasindan yeni KAP bildirimlerini alir
   (ticker, baslik, KAP bildirim ID, published_at)
2. KAP bildirim ID'den kap.org.tr URL'i olusturur
3. AI analizi icin kap.org.tr sayfasindan bildirim icerigi cekilir

Her ~50 saniyede bir calisir (scheduler).
BigPara ve Bloomberg HT KALDIRILDI — sadece Uzmanpara kaynak.
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
BIST_INDEX_BASE = "https://bigpara.hurriyet.com.tr/borsa/hisse-fiyatlari/bist-tum-endeksi/"

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
    KAP bildirim numarasi Uzmanpara URL'den cikarilir.

    HTML yapisi:
      <li>
        <a class="hisse" href="/kap-haberi/.../">TICKER</a>
        <a href="/kap-haberi/.../">Baslik</a>
        <span class="date">01.03.2026<br/>20:31:25</span>
      </li>

    Returns:
        [{title, company_code, kap_url, published_at, source, category, is_bilanco}, ...]
    """
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True, headers=HEADERS) as client:
        try:
            r = await client.get(UZMANPARA_URL)
            r.raise_for_status()
        except Exception as exc:
            logger.warning("Uzmanpara hata: %s", exc)
            return []

        soup = BeautifulSoup(r.text, "lxml")

        # Ticker linklerini bul (class="hisse")
        hisse_links = soup.find_all("a", class_="hisse", href=re.compile(r"/kap-haberi/"))
        if not hisse_links:
            logger.warning("Uzmanpara: haber linki bulunamadi")
            return []

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

            # KAP bildirim ID'yi Uzmanpara URL'den cikar
            # Ornek: /kap-haberi/atagy-finansal-rapor-1564240/ -> 1564240
            kap_id = ""
            kap_url = ""
            id_match = re.search(r"-(\d{6,})/?$", detail_href)
            if id_match:
                kap_id = id_match.group(1)
                kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_id}"

            if not kap_url:
                # Fallback: Uzmanpara URL'yi kullan
                kap_url = (
                    f"https://uzmanpara.milliyet.com.tr{detail_href}"
                    if detail_href.startswith("/") else detail_href
                )

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
    """Uzmanpara'dan KAP haberlerini ceker.

    Ana ve tek kaynak — her ~50 saniyede bir cagirilir.
    BIST whitelist ile filtrelenir.

    Returns:
        list[dict]: Bildirim listesi
    """
    bist = await _refresh_bist_symbols()
    data = await _uzmanpara_fetch()

    if not data:
        return []

    data = _apply_bist_filter(data, bist)
    return data[:30]
