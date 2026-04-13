"""Kurum Onerileri Scraper — hedeffiyat.com.tr

Calisma Mantigi:
1. /kurumlar sayfasindan tum araci kurum URL'lerini cek
2. Her kurum icin /kurum/[slug]-[id] sayfasina git
3. div.kurumPagelist-item'lardan hisse onerileri parse et
4. ticker, hedef_fiyat, oneri, tarih, kurum bilgilerini dondur

Kaynak: https://hedeffiyat.com.tr
"""

import asyncio
import re
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import httpx
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

BASE_URL = "https://hedeffiyat.com.tr"
KURUMLAR_URL = f"{BASE_URL}/kurumlar"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Referer": BASE_URL,
}

# Turk ay isimleri
TR_MONTHS = {
    "ocak": 1, "şubat": 2, "subat": 2, "mart": 3, "nisan": 4,
    "mayıs": 5, "mayis": 5, "haziran": 6, "temmuz": 7,
    "ağustos": 8, "agustos": 8, "eylül": 9, "eylul": 9,
    "ekim": 10, "kasım": 11, "kasim": 11, "aralık": 12, "aralik": 12,
}

# Concurrency limiti — siteyi ezmemek icin
MAX_CONCURRENT = 3
REQUEST_DELAY = 1.0  # Istekler arasi bekleme (saniye)


class KurumOneriScraper:
    """hedeffiyat.com.tr kurum onerileri scraper."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )
        self._semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def close(self):
        await self.client.aclose()

    # ─── Ana Akis ─────────────────────────────────────────────

    async def fetch_all_recommendations(self) -> list[dict]:
        """Tum kurumlarin onerilerini topla."""
        institution_urls = await self._fetch_institution_urls()
        if not institution_urls:
            logger.error("Kurum URL'leri alinamadi")
            return []

        logger.info("Toplam %d kurum bulundu, scrape basliyor...", len(institution_urls))

        all_recs: list[dict] = []
        tasks = [
            self._fetch_institution_recs(name, url)
            for name, url in institution_urls
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.warning("Kurum scrape hatasi: %s", result)
                continue
            if result:
                all_recs.extend(result)

        logger.info("Toplam %d oneri scrape edildi", len(all_recs))
        return all_recs

    # ─── Kurum Listesi ────────────────────────────────────────

    async def _fetch_institution_urls(self) -> list[tuple[str, str]]:
        """Kurumlar sayfasindan tum kurum URL'lerini cek."""
        try:
            resp = await self.client.get(KURUMLAR_URL)
            resp.raise_for_status()
        except Exception as e:
            logger.error("Kurumlar sayfasi alinamadi: %s", e)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        institutions = []

        # div.kurumPagelist > div.kurumPagelist-item > a[href]
        items = soup.select("div.kurumPagelist-item a[href]")
        if not items:
            # Alternatif: tum a etiketlerinden /kurum/ icerenleri bul
            items = soup.select('a[href*="/kurum/"]')

        for a_tag in items:
            href = a_tag.get("href", "")
            if not href or "/kurum/" not in href:
                continue
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            # Kurum adini img alt veya text'ten al
            img = a_tag.find("img")
            name = ""
            if img and img.get("alt"):
                name = img["alt"].strip()
            if not name:
                name = a_tag.get_text(strip=True)
            if not name:
                # URL'den slug cikart: /kurum/garanti-bbva-31 → garanti bbva
                slug = href.rstrip("/").split("/")[-1]
                slug = re.sub(r"-\d+$", "", slug)
                name = slug.replace("-", " ").title()

            institutions.append((name, full_url))

        # Duplicate URL temizligi
        seen = set()
        unique = []
        for name, url in institutions:
            if url not in seen:
                seen.add(url)
                unique.append((name, url))

        return unique

    # ─── Tek Kurum Sayfasi ────────────────────────────────────

    async def _fetch_institution_recs(
        self, institution_name: str, url: str
    ) -> list[dict]:
        """Bir kurum sayfasindan tum hisse onerilerini parse et."""
        async with self._semaphore:
            await asyncio.sleep(REQUEST_DELAY)
            try:
                resp = await self.client.get(url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(
                    "Kurum sayfasi alinamadi [%s]: %s", institution_name, e
                )
                return []

        soup = BeautifulSoup(resp.text, "lxml")
        recs = []

        # div.kurumPagelist-item icerisinde her hisse bir satir
        items = soup.select("div.kurumPagelist-item")
        if not items:
            # Alternatif: table satirlari
            items = soup.select("table tbody tr")

        for item in items:
            rec = self._parse_recommendation_item(item, institution_name)
            if rec:
                recs.append(rec)

        if recs:
            logger.debug(
                "%s: %d oneri parse edildi", institution_name, len(recs)
            )

        return recs

    # ─── Parse Helpers ────────────────────────────────────────

    def _parse_recommendation_item(
        self, item: Tag, institution_name: str
    ) -> Optional[dict]:
        """Bir hisse oneri satirini parse et."""
        try:
            texts = [el.get_text(strip=True) for el in item.find_all(["td", "div", "span", "p", "h4", "h5", "a"])]
            if len(texts) < 3:
                return None

            # Yapiyi anlamaya calis
            ticker = None
            company_name = None
            target_price = None
            recommendation = None
            report_date = None
            source_url = None

            # Link varsa kaynak URL
            link = item.find("a", href=True)
            if link:
                href = link["href"]
                if href and not href.startswith("#"):
                    source_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            # Tum text parcalarini tara
            for txt in texts:
                if not txt:
                    continue

                # Ticker tespiti: 3-6 buyuk harf (THYAO, AKBNK, SISE)
                if not ticker and re.match(r"^[A-ZÇĞİÖŞÜ]{3,6}$", txt):
                    ticker = txt
                    continue

                # Hedef fiyat: "123,45 TL" veya "123.45"
                if not target_price:
                    price = self._parse_price(txt)
                    if price:
                        target_price = price
                        continue

                # Oneri tespiti
                if not recommendation:
                    rec = self._parse_recommendation(txt)
                    if rec:
                        recommendation = rec
                        continue

                # Tarih tespiti
                if not report_date:
                    dt = self._parse_date(txt)
                    if dt:
                        report_date = dt
                        continue

                # Sirket adi (uzun text, ticker degilse)
                if not company_name and len(txt) > 5 and not re.match(r"^[\d.,% ]+$", txt):
                    company_name = txt

            if not ticker:
                return None

            return {
                "ticker": ticker.upper(),
                "company_name": company_name,
                "institution_name": institution_name,
                "recommendation": recommendation,
                "target_price": target_price,
                "report_date": report_date or date.today(),
                "source_url": source_url,
            }
        except Exception as e:
            logger.debug("Parse hatasi: %s", e)
            return None

    def _parse_price(self, text: str) -> Optional[Decimal]:
        """Fiyat metnini Decimal'e donustur. '123,45 TL' → 123.45"""
        text = text.strip()
        # "TL" kaldir
        text = re.sub(r"\s*TL\s*$", "", text, flags=re.IGNORECASE)
        # "%" iceren metinler fiyat degil
        if "%" in text:
            return None
        # Virgulu noktaya cevir
        text = text.replace(".", "").replace(",", ".")
        try:
            val = Decimal(text)
            if val > 0:
                return val
        except (InvalidOperation, ValueError):
            pass
        return None

    def _parse_recommendation(self, text: str) -> Optional[str]:
        """Oneri metnini normalize et."""
        text_lower = text.strip().lower()
        # Bilinen oneri turleri
        rec_map = {
            "al": "Al",
            "sat": "Sat",
            "tut": "Tut",
            "nötr": "Nötr",
            "notr": "Nötr",
            "endeks üstü getiri": "Endeks Üstü Getiri",
            "endeks ustu getiri": "Endeks Üstü Getiri",
            "endekse paralel getiri": "Endekse Paralel Getiri",
            "endeks altı getiri": "Endeks Altı Getiri",
            "endeks alti getiri": "Endeks Altı Getiri",
            "güçlü al": "Güçlü Al",
            "guclu al": "Güçlü Al",
            "outperform": "Endeks Üstü Getiri",
            "market perform": "Endekse Paralel Getiri",
            "underperform": "Endeks Altı Getiri",
            "buy": "Al",
            "sell": "Sat",
            "hold": "Tut",
            "neutral": "Nötr",
            "ekle": "Ekle",
            "azalt": "Azalt",
        }
        for key, val in rec_map.items():
            if text_lower == key or text_lower.startswith(key):
                return val
        # Baska oneri varyantlari
        if "üstü" in text_lower or "ustu" in text_lower:
            return "Endeks Üstü Getiri"
        if "paralel" in text_lower:
            return "Endekse Paralel Getiri"
        if "altı" in text_lower or "alti" in text_lower:
            return "Endeks Altı Getiri"
        return None

    def _parse_date(self, text: str) -> Optional[date]:
        """Tarih metnini date objesine donustur.

        Desteklenen formatlar:
        - 13.04.2026
        - 13/04/2026
        - 13 Nisan 2026
        - 2026-04-13
        """
        text = text.strip()

        # DD.MM.YYYY veya DD/MM/YYYY
        m = re.match(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass

        # YYYY-MM-DD
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
        if m:
            try:
                return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                pass

        # Turkce tarih: 13 Nisan 2026
        m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
        if m:
            month = TR_MONTHS.get(m.group(2).lower())
            if month:
                try:
                    return date(int(m.group(3)), month, int(m.group(1)))
                except ValueError:
                    pass

        return None
