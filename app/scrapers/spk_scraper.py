"""SPK (Sermaye Piyasasi Kurulu) scraper.

Kaynak: https://spk.gov.tr/istatistikler/basvurular/ilk-halka-arz-basvurusu
Basit HTML tablo: sira no, sirket adi, basvuru tarihi.
131+ bekleyen halka arz basvurusu listeleniyor.
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SPK_BASE = "https://spk.gov.tr"
SPK_IPO_URL = f"{SPK_BASE}/istatistikler/basvurular/ilk-halka-arz-basvurusu"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


class SPKScraper:
    """SPK Ilk Halka Arz Basvurusu listesi scraper."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
            verify=False,  # SPK SSL sertifika sorunu
        )

    async def close(self):
        await self.client.aclose()

    async def fetch_ipo_applications(self) -> list[dict]:
        """SPK'daki tum halka arz basvurularini getirir.

        Tablo yapisi:
        - Sira No
        - Sirketler (sirket adi)
        - Basvuru Tarihi (dd.mm.yyyy)

        Returns:
            [{company_name, application_date, row_number}, ...]
        """
        results = []

        try:
            resp = await self.client.get(SPK_IPO_URL)
            if resp.status_code != 200:
                logger.warning(f"SPK sayfa yaniti: {resp.status_code}")
                return results

            soup = BeautifulSoup(resp.text, "lxml")

            # Ana tabloyu bul — "Sirketler" basligini iceren tablo
            table = None
            for t in soup.find_all("table"):
                header_text = t.get_text(strip=True).lower()
                if "şirketler" in header_text or "sirketler" in header_text:
                    table = t
                    break

            if not table:
                # Fallback: sayfadaki en buyuk tabloyu al
                tables = soup.find_all("table")
                if tables:
                    table = max(tables, key=lambda t: len(t.find_all("tr")))

            if not table:
                logger.warning("SPK: Tablo bulunamadi")
                return results

            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue

                row_num = cells[0].get_text(strip=True)
                company_name = cells[1].get_text(strip=True)
                date_str = cells[2].get_text(strip=True)

                # Sira numarasi kontrolu — baslik satirini atla
                if not row_num.isdigit():
                    continue

                # Tarih parse
                app_date = self._parse_date(date_str)

                results.append({
                    "source": "spk",
                    "row_number": int(row_num),
                    "company_name": company_name,
                    "application_date": app_date,
                    "application_date_str": date_str,
                })

            logger.info(f"SPK: {len(results)} halka arz basvurusu bulundu")

        except Exception as e:
            logger.error(f"SPK scraping hatasi: {e}")

        return results

    def _parse_date(self, date_str: str) -> Optional[date]:
        """dd.mm.yyyy formatindaki tarihi parse eder."""
        try:
            return datetime.strptime(date_str.strip(), "%d.%m.%Y").date()
        except (ValueError, AttributeError):
            return None
