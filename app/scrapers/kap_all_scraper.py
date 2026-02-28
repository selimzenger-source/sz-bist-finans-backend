"""Tum KAP Haberleri Scraper — BigPara + Bloomberg HT.

KAP-TEST projesindeki kap_fetcher.py'den production-ready async port.
BigPara primary, Bloomberg HT fallback. BIST whitelist filtresi.

Scheduler tarafindan her 5 dakikada bir cagirilir.
Yeni bildirimler kap_all_disclosures tablosuna kaydedilir.
"""

import logging
import re
import time
from datetime import datetime
from typing import Any

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ─── URL'ler ────────────────────────────────────────────────────────────────
BIGPARA_URL = "https://bigpara.hurriyet.com.tr/haberler/kap-haberleri/"
BLOOMBERG_URL = "https://www.bloomberght.com/borsa/hisseler/kap-haberleri"
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
                    if re.match(r"^[A-Z]{2,10}$", a.get_text(strip=True))
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
# 1. ASIL: BigPara
# ═════════════════════════════════════════════════════════════════════════════

async def _fetch_detail_text(client: httpx.AsyncClient, url: str) -> tuple[str, str]:
    """BigPara detay sayfasindan tam metin + gercek KAP linkini ceker."""
    try:
        r = await client.get(url)
        r.raise_for_status()
    except Exception as exc:
        logger.debug("Detay sayfa hata (%s): %s", url[:60], exc)
        return "", ""

    soup = BeautifulSoup(r.text, "lxml")

    # Tam metin
    detail_div = soup.find("div", class_="news-detail-text")
    body = detail_div.get_text(" ", strip=True) if detail_div else ""

    # Gercek KAP linki
    real_kap = ""
    kap_link = soup.find("a", href=re.compile(r"kap\.org\.tr.*Bildirim/\d+"))
    if kap_link:
        real_kap = kap_link.get("href", "")
    else:
        kap_m = re.search(r"(https?://(?:www\.)?kap\.org\.tr/\S*Bildirim/\d+)", r.text)
        if kap_m:
            real_kap = kap_m.group(1)

    # URL normalizasyonu
    if real_kap:
        real_kap = real_kap.replace("/en/Bildirim/", "/tr/Bildirim/")
        if "kap.org.tr/Bildirim/" in real_kap:
            real_kap = real_kap.replace("kap.org.tr/Bildirim/", "kap.org.tr/tr/Bildirim/")

    return body, real_kap


async def _bigpara_fetch(max_items: int = 20) -> list[dict[str, Any]]:
    """BigPara KAP haberleri + detay sayfalarindan tam metin.

    Args:
        max_items: Maksimum haber sayisi (varsayilan 20, seed icin 50+).
    """
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True, headers=HEADERS) as client:
        seen_keys: set[str] = set()
        results: list[dict[str, Any]] = []
        max_pages = (max_items // 15) + 2  # ~15-20 haber/sayfa

        for page in range(1, max_pages + 1):
            if len(results) >= max_items:
                break

            page_url = BIGPARA_URL if page == 1 else f"{BIGPARA_URL}{page}/"
            try:
                r = await client.get(page_url)
                r.raise_for_status()
            except Exception as exc:
                logger.warning("[1-ASIL] BigPara sayfa %d hata: %s", page, exc)
                break

            soup = BeautifulSoup(r.text, "lxml")
            links = soup.find_all("a", href=re.compile(r"/haberler/kap-haberleri/.+_ID\d+"))
            if not links:
                logger.debug("BigPara sayfa %d: haber yok, durduruluyor", page)
                break

            page_added = 0
            for a in links:
                if len(results) >= max_items:
                    break

                href = a.get("href", "")
                full = (
                    f"https://bigpara.hurriyet.com.tr{href}"
                    if href.startswith("/") else href
                )

                text = a.get_text(" ", strip=True)

                # Ticker + baslik bazli dedup
                ticker_m = re.search(r"\*{1,3}([A-Z]{2,10})\*{1,3}", text)
                pre_code = ticker_m.group(1) if ticker_m else ""
                title_m_pre = re.search(r"\(([^)]+)\)", text)
                pre_title = title_m_pre.group(1).strip() if title_m_pre else text[:40]
                dedup_key = f"{pre_code}|{pre_title}"
                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)

                if not ticker_m:
                    continue
                company_code = ticker_m.group(1)

                # Baslik: parantez icindeki bildirim turu
                title_m = re.search(r"\(([^)]+)\)", text)
                title = title_m.group(1).strip() if title_m else text

                # Detay sayfasindan tam metin + gercek KAP linki
                body, real_kap = await _fetch_detail_text(client, full)

                if not real_kap:
                    real_kap = full

                results.append(_make_item(
                    title=title,
                    company_code=company_code,
                    kap_url=real_kap,
                    source="bigpara",
                    body=body or title,
                ))
                page_added += 1

            logger.info("[1-ASIL] BigPara sayfa %d -> %d yeni haber", page, page_added)

    logger.info("[1-ASIL] BigPara toplam -> %d haber (tam metin ile)", len(results))
    return results


# ═════════════════════════════════════════════════════════════════════════════
# 2. YEDEK: Bloomberg HT
# ═════════════════════════════════════════════════════════════════════════════

async def _bloomberg_fetch() -> list[dict[str, Any]]:
    """Bloomberg HT KAP haberleri (yedek kaynak)."""
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True, headers=HEADERS) as client:
        try:
            r = await client.get(BLOOMBERG_URL)
            r.raise_for_status()
        except Exception as exc:
            logger.warning("[2-YEDEK] Bloomberg HT hata: %s", exc)
            return []

    soup = BeautifulSoup(r.text, "lxml")
    links = soup.find_all("a", href=re.compile(r"/borsa/hisse/.+/kap-haberi/\d+"))
    if not links:
        logger.warning("[2-YEDEK] Bloomberg HT: haber linki bulunamadi")
        return []

    results: list[dict[str, Any]] = []
    for a in links:
        if len(results) >= 20:
            break

        href = a.get("href", "")
        full = f"https://www.bloomberght.com{href}" if href.startswith("/") else href
        text = a.get_text(" ", strip=True)
        parts = [p.strip() for p in re.split(r"\s{2,}|\t+", text) if p.strip()]

        company_code = title = ""

        if len(parts) >= 3:
            m = re.match(r"([A-Z]{2,10})/", parts[0])
            company_code = m.group(1) if m else parts[0].split("/")[0].strip()
            title = " ".join(parts[1:-1])
        else:
            dm = re.search(r"(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})", text)
            date_raw = dm.group(1) if dm else ""
            remaining = text.replace(date_raw, "").strip() if date_raw else text
            tm = re.match(r"([A-Z]{2,10})/", remaining)
            if tm:
                company_code = tm.group(1)
                title = remaining[tm.end():].strip()

        if not company_code:
            continue

        im = re.search(r"/kap-haberi/(\d+)", href)
        kap_url = f"https://www.kap.org.tr/tr/Bildirim/{im.group(1)}" if im else full

        results.append(_make_item(
            title=title,
            company_code=company_code,
            kap_url=kap_url,
            source="bloomberght",
        ))

    logger.info("[2-YEDEK] Bloomberg HT -> %d haber", len(results))
    return results


# ═════════════════════════════════════════════════════════════════════════════
# Ortak yardimcilar
# ═════════════════════════════════════════════════════════════════════════════

def _make_item(*, title: str, company_code: str, kap_url: str,
               source: str, body: str = "") -> dict[str, Any]:
    """Standart bildirim dict'i olusturur."""
    category = _infer_category(title)
    is_bilanco = category == "Bilanço/Finansal Rapor"
    return {
        "title": title,
        "company_code": company_code,
        "body": body or title,
        "kap_url": kap_url,
        "category": category,
        "is_bilanco": is_bilanco,
        "source": source,
        "published_at": datetime.utcnow(),
    }


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


# ═════════════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════════════

async def scrape_all_kap_disclosures() -> list[dict[str, Any]]:
    """Tum KAP bildirimlerini ceker (BigPara primary, Bloomberg HT fallback).

    BIST whitelist ile filtrelenir. Max 20 haber doner.
    Scheduler tarafindan cagirilir.

    Returns:
        list[dict]: Bildirim listesi (title, company_code, body, kap_url, category, is_bilanco, source, published_at)
    """
    # BIST whitelist'i guncelle
    bist = await _refresh_bist_symbols()

    # BigPara -> Bloomberg HT
    data = await _bigpara_fetch()
    if not data:
        logger.warning("BigPara bos/hatali -> Bloomberg HT deneniyor...")
        data = await _bloomberg_fetch()

    if not data:
        logger.warning("Tum kaynaklar bos — 0 KAP haberi")
        return []

    # BIST whitelist filtresi
    if bist:
        filtered = [d for d in data if d.get("company_code", "").strip() in bist]
        logger.info("BIST filtre: %d -> %d haber", len(data), len(filtered))
        data = filtered

    return data[:20]


async def seed_initial_disclosures(target: int = 50) -> list[dict[str, Any]]:
    """Ilk kurulumda DB'ye seed icin BigPara'dan sayfalama ile haber ceker.

    Normal scrape'den farki: birden fazla sayfa tarar (target kadar haber).
    Startup'ta DB bossa bir kez cagirilir.

    Args:
        target: Hedef haber sayisi (varsayilan 50)

    Returns:
        list[dict]: Bildirim listesi
    """
    bist = await _refresh_bist_symbols()

    data = await _bigpara_fetch(max_items=target + 10)  # BIST filtresi sonrasi hedef icin fazla cek

    if not data:
        logger.warning("Seed: BigPara bos — Bloomberg HT deneniyor...")
        data = await _bloomberg_fetch()

    if not data:
        logger.warning("Seed: Tum kaynaklar bos — 0 haber")
        return []

    if bist:
        filtered = [d for d in data if d.get("company_code", "").strip() in bist]
        logger.info("Seed BIST filtre: %d -> %d haber", len(data), len(filtered))
        data = filtered

    logger.info("Seed: %d haber hazir", min(len(data), target))
    return data[:target]
