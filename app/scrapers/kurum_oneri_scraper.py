"""Kurum Onerileri Scraper — hedeffiyat.com.tr

Calisma Mantigi:
1. /kurumlar sayfasindan tum araci kurum URL'lerini cek
   - Selektor: .kurumbox a[href*="/kurum/"] → title attr = kurum adi
2. Her kurum icin /kurum/[slug]-[id] sayfasina git
3. div.Tahminlerlist-item > div.senetbox icinden verileri parse et:
   - .senetbox-title strong → ticker
   - .senetbox-title → sirket adi
   - .hedeffiyat span → hedef fiyat
   - .col-8 a.btn → oneri (Al/Tut/Sat/Endeks Ustu Get.)
   - .senettarih → tarih ("Pazartesi, 13 Nisan 2026")
   - .col-8 a.btn[href] → rapor linki
4. ticker, hedef_fiyat, oneri, tarih, kurum bilgilerini dondur

Kaynak: https://hedeffiyat.com.tr
"""

import asyncio
import re
import logging
from datetime import date
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

# Atlanacak slug'lar (kurum degil, kategori sayfasi)
SKIP_SLUGS = {"akilli-raporlama", "halka-arzlar", "ilave-okumalar", "tum-kurumlar"}

# Concurrency limiti — siteyi ezmemek icin
MAX_CONCURRENT = 5
REQUEST_DELAY = 0.8  # Istekler arasi bekleme (saniye)


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
        """Kurumlar sayfasindan tum kurum URL'lerini cek.

        Selektor: .kurumbox a[href*="/kurum/"]
        Kurum adi: a[title] veya img[alt]
        """
        try:
            resp = await self.client.get(KURUMLAR_URL)
            resp.raise_for_status()
        except Exception as e:
            logger.error("Kurumlar sayfasi alinamadi: %s", e)
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        institutions = []

        # .kurumbox a → href="/kurum/{slug}-{id}", title="{name}"
        links = soup.select('.kurumbox a[href*="/kurum/"]')
        if not links:
            # Fallback: tum /kurum/ linkleri
            links = soup.select('a[href*="/kurum/"]')

        for a_tag in links:
            href = a_tag.get("href", "")
            if not href or "/kurum/" not in href:
                continue

            # Kategori sayfalarini atla
            slug_part = href.rstrip("/").split("/")[-1]
            slug_name = re.sub(r"-\d+$", "", slug_part)
            if slug_name in SKIP_SLUGS:
                continue

            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            # Kurum adi: title attr > img alt > slug'dan uret
            name = a_tag.get("title", "").strip()
            if not name:
                img = a_tag.find("img")
                if img:
                    name = (img.get("alt") or "").strip()
            if not name:
                name = slug_name.replace("-", " ").title()

            institutions.append((name, full_url))

        # Duplicate temizligi
        seen = set()
        unique = []
        for name, url in institutions:
            if url not in seen:
                seen.add(url)
                unique.append((name, url))

        logger.info("Kurumlar sayfasindan %d kurum URL'si alindi", len(unique))
        return unique

    # ─── Tek Kurum Sayfasi ────────────────────────────────────

    async def _fetch_institution_recs(
        self, institution_name: str, url: str
    ) -> list[dict]:
        """Bir kurum sayfasindan tum hisse onerilerini parse et.

        Selektor: div.Tahminlerlist-item > div.senetbox
        """
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

        # Ana selektor: div.Tahminlerlist-item icerisindeki div.senetbox
        cards = soup.select("div.Tahminlerlist-item div.senetbox")
        if not cards:
            # Fallback: sadece .senetbox dene
            cards = soup.select("div.senetbox")

        for card in cards:
            rec = self._parse_senetbox(card, institution_name)
            if rec:
                recs.append(rec)

        if recs:
            logger.debug(
                "%s: %d oneri parse edildi", institution_name, len(recs)
            )
        else:
            logger.debug("%s: 0 oneri (bos sayfa veya parse hatasi)", institution_name)

        return recs

    # ─── Parse: Senetbox Karti ────────────────────────────────

    def _parse_senetbox(
        self, card: Tag, institution_name: str
    ) -> Optional[dict]:
        """Bir senetbox kartindan veri cikar.

        Yapi:
        - .senetbox-title strong → TICKER
        - .senetbox-title text → "TICKER - Sirket Adi"
        - .sonfiyat span → "118.80 ₺" (oneri anindaki fiyat)
        - .hedeffiyat span → "169.30 ₺"
        - .potansiyelgetiri span → "%42.51"
        - .col-8 a.btn → "Al" / "Tut" / "Endeks Ustu Get."
        - .col-8 a.btn[href] → rapor linki
        - .senettarih → "Pazartesi, 13 Nisan 2026"
        """
        try:
            # ── Ticker ──
            ticker_el = card.select_one(".senetbox-title strong")
            if not ticker_el:
                return None
            ticker = ticker_el.get_text(strip=True).upper()
            if not ticker or len(ticker) < 2 or len(ticker) > 8:
                return None

            # ── Sirket Adi ──
            title_el = card.select_one(".senetbox-title")
            company_name = None
            if title_el:
                full_text = title_el.get_text(strip=True)
                # "THYAO - THY AO A.S." formatindan sirket adini cikar
                parts = full_text.split(" - ", 1)
                if len(parts) > 1:
                    company_name = parts[1].strip()

            # ── Son Fiyat (oneri anindaki fiyat) ──
            current_price = None
            sf_el = card.select_one(".sonfiyat span")
            if sf_el:
                current_price = self._parse_price(sf_el.get_text(strip=True))

            # ── Hedef Fiyat ──
            target_price = None
            hf_el = card.select_one(".hedeffiyat span")
            if hf_el:
                target_price = self._parse_price(hf_el.get_text(strip=True))

            # ── Potansiyel Getiri ──
            potential_return = None
            pg_el = card.select_one(".potansiyelgetiri span")
            if pg_el:
                potential_return = self._parse_percentage(pg_el.get_text(strip=True))

            # ── Oneri (Al/Tut/Sat) ──
            recommendation = None
            source_url = None
            rec_btn = card.select_one(".col-8 a.btn")
            if rec_btn:
                rec_text = rec_btn.get_text(strip=True)
                recommendation = self._normalize_recommendation(rec_text)
                # Rapor linki
                href = rec_btn.get("href", "")
                if href and href.startswith("/"):
                    source_url = f"{BASE_URL}{href}"

            # ── Tarih ──
            report_date = None
            date_el = card.select_one(".senettarih")
            if date_el:
                report_date = self._parse_turkish_date(date_el.get_text(strip=True))

            return {
                "ticker": ticker,
                "company_name": company_name,
                "institution_name": institution_name,
                "recommendation": recommendation,
                "target_price": target_price,
                "current_price": current_price,
                "potential_return": potential_return,
                "report_date": report_date or date.today(),
                "source_url": source_url,
            }
        except Exception as e:
            logger.debug("Senetbox parse hatasi: %s", e)
            return None

    # ─── Helpers ──────────────────────────────────────────────

    def _parse_price(self, text: str) -> Optional[Decimal]:
        """Fiyat metnini Decimal'e donustur. '169.30 ₺' → 169.30"""
        text = text.strip()
        # ₺, TL kaldir
        text = re.sub(r"[\s₺]+$", "", text)
        text = re.sub(r"\s*TL\s*$", "", text, flags=re.IGNORECASE)
        # "%" iceriyorsa fiyat degil
        if "%" in text:
            return None
        # Binlik ayirici nokta, ondalik virgul: "1.234,56" → "1234.56"
        if "," in text and "." in text:
            text = text.replace(".", "").replace(",", ".")
        elif "," in text:
            text = text.replace(",", ".")
        # Sadece nokta varsa oldugu gibi birak (orn: "169.30")
        try:
            val = Decimal(text)
            if val > 0:
                return val
        except (InvalidOperation, ValueError):
            pass
        return None

    def _parse_percentage(self, text: str) -> Optional[Decimal]:
        """Yuzde metnini Decimal'e donustur. '%42.51' → 42.51"""
        text = text.strip()
        text = text.replace("%", "").replace("₺", "").strip()
        if not text:
            return None
        # Virgulu noktaya cevir
        text = text.replace(",", ".")
        try:
            val = Decimal(text)
            return val  # Negatif getiri de olabilir
        except (InvalidOperation, ValueError):
            pass
        return None

    def _normalize_recommendation(self, text: str) -> Optional[str]:
        """Oneri metnini normalize et.

        Sitedeki varyantlar:
        - "Al" → Al
        - "Tut" → Tut
        - "Sat" → Sat
        - "Endeks Ustu Get." → Endeks Üstü Getiri
        - "Endekse Paralel Get." → Endekse Paralel Getiri
        - "Endeks Alti Get." → Endeks Altı Getiri
        """
        text = text.strip()
        if not text:
            return None
        text_lower = text.lower()

        rec_map = {
            "al": "Al",
            "sat": "Sat",
            "tut": "Tut",
            "nötr": "Nötr",
            "notr": "Nötr",
            "ekle": "Ekle",
            "azalt": "Azalt",
        }
        if text_lower in rec_map:
            return rec_map[text_lower]

        # Kısaltmalı varyantlar
        if "üstü" in text_lower or "ustu" in text_lower:
            return "Endeks Üstü Getiri"
        if "paralel" in text_lower:
            return "Endekse Paralel Getiri"
        if "altı" in text_lower or "alti" in text_lower:
            return "Endeks Altı Getiri"
        if "güçlü" in text_lower or "guclu" in text_lower:
            return "Güçlü Al"

        # Ingilizce
        eng_map = {
            "buy": "Al",
            "sell": "Sat",
            "hold": "Tut",
            "outperform": "Endeks Üstü Getiri",
            "underperform": "Endeks Altı Getiri",
            "neutral": "Nötr",
            "overweight": "Endeks Üstü Getiri",
            "underweight": "Endeks Altı Getiri",
        }
        if text_lower in eng_map:
            return eng_map[text_lower]

        # Taninamayan ama bos olmayan → oldugu gibi dondur
        return text

    def _parse_turkish_date(self, text: str) -> Optional[date]:
        """Turkce tarih parse et.

        Formatlar:
        - "Pazartesi, 13 Nisan 2026"
        - "13 Nisan 2026"
        - "13.04.2026"
        """
        text = text.strip()

        # Gun ismi varsa kaldir: "Pazartesi, 13 Nisan 2026" → "13 Nisan 2026"
        if "," in text:
            text = text.split(",", 1)[1].strip()

        # DD.MM.YYYY
        m = re.match(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass

        # "13 Nisan 2026"
        m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
        if m:
            month = TR_MONTHS.get(m.group(2).lower())
            if month:
                try:
                    return date(int(m.group(3)), month, int(m.group(1)))
                except ValueError:
                    pass

        return None
