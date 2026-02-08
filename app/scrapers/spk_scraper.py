"""SPK (Sermaye Piyasasi Kurulu) scraper.

SPK resmi sitesinden onaylanan halka arz listesini cekmek icin kullanilir.
SPK bultenleri ve haftalik karar ozetleri taranir.
"""

import logging
import re
from datetime import date
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SPK_BASE = "https://www.spk.gov.tr"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


class SPKScraper:
    """SPK onay listesi scraper."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def fetch_approved_ipos(self) -> list[dict]:
        """SPK'nin onayladigi halka arzlari getirir.

        SPK haftalik bulten ve karar ozetlerinde halka arz onaylari yer alir.
        """
        results = []

        try:
            # SPK Bulten sayfasi
            resp = await self.client.get(f"{SPK_BASE}/Bulten")
            if resp.status_code != 200:
                logger.warning(f"SPK bulten sayfasi: {resp.status_code}")
                return results

            soup = BeautifulSoup(resp.text, "lxml")

            # Bulten linklerini tara
            for link in soup.select("a[href*='Bulten']"):
                href = link.get("href", "")
                title = link.get_text(strip=True).lower()

                # Halka arz ile ilgili bultenleri filtrele
                if any(kw in title for kw in ["halka arz", "izahname", "pay satışı", "pay satisi"]):
                    full_url = href if href.startswith("http") else SPK_BASE + href
                    detail = await self._parse_bulletin(full_url)
                    if detail:
                        results.append(detail)

        except Exception as e:
            logger.error(f"SPK scraping hatasi: {e}")

        # Ayrica SPK karar ozeti sayfasini da kontrol et
        try:
            decision_results = await self._fetch_decisions()
            results.extend(decision_results)
        except Exception as e:
            logger.error(f"SPK karar ozeti hatasi: {e}")

        logger.info(f"SPK: {len(results)} onay bulundu")
        return results

    async def _fetch_decisions(self) -> list[dict]:
        """SPK haftalik karar ozetlerinden halka arz onaylarini cekmek."""
        results = []

        try:
            # SPK Kurul Karar Ozeti sayfasi
            resp = await self.client.get(f"{SPK_BASE}/Sayfa/KurulKararOzeti")
            if resp.status_code != 200:
                return results

            soup = BeautifulSoup(resp.text, "lxml")

            # Karar ozeti iceriginde halka arz ile ilgili bolumler
            content = soup.get_text(separator="\n", strip=True)

            # "Halka arz" iceren paragrafları bul
            paragraphs = content.split("\n")
            for i, para in enumerate(paragraphs):
                if "halka arz" in para.lower() and any(
                    kw in para.lower() for kw in ["onay", "uygun", "kabul", "izahname"]
                ):
                    result = self._parse_decision_paragraph(para, paragraphs, i)
                    if result:
                        results.append(result)

        except Exception as e:
            logger.error(f"SPK karar ozeti scraping hatasi: {e}")

        return results

    async def _parse_bulletin(self, url: str) -> Optional[dict]:
        """Tek bir SPK bultenini parse eder."""
        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "lxml")
            content = soup.get_text(separator="\n", strip=True)

            # Sirket adini cikar
            company_match = re.search(
                r"([A-ZÇĞİÖŞÜa-zçğıöşü\s.]+?)\s*(?:A\.?Ş\.?|Anonim\s*Şirketi)",
                content
            )

            return {
                "source": "spk_bulletin",
                "company_name": company_match.group(0).strip() if company_match else None,
                "approval_type": "ipo_approval",
                "url": url,
                "raw_text": content[:2000],  # Ilk 2000 karakter
                "scraped_at": date.today().isoformat(),
            }

        except Exception as e:
            logger.error(f"SPK bulten parse hatasi ({url}): {e}")
            return None

    def _parse_decision_paragraph(
        self, para: str, all_paragraphs: list[str], index: int
    ) -> Optional[dict]:
        """Karar ozetindeki halka arz paragrafini parse eder."""
        try:
            # Context icin onceki ve sonraki paragraflari da al
            context = "\n".join(all_paragraphs[max(0, index - 2):index + 3])

            company_match = re.search(
                r"([A-ZÇĞİÖŞÜa-zçğıöşü\s.]+?)\s*(?:A\.?Ş\.?|Anonim)",
                context
            )

            return {
                "source": "spk_decision",
                "company_name": company_match.group(0).strip() if company_match else None,
                "approval_type": "ipo_decision",
                "raw_text": context[:2000],
                "scraped_at": date.today().isoformat(),
            }
        except Exception:
            return None
