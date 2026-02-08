"""KAP (Kamuyu Aydinlatma Platformu) scraper.

KAP'in resmi API'si bulunmadigi icin web scraping kullanilir.
Iki ana islem yapar:
1. Halka arz bildirimlerini tarar (30 dk aralik)
2. Seans ici/disi KAP haberlerini tarar (30 sn aralik)
"""

import logging
import re
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# KAP base URL
KAP_BASE = "https://www.kap.org.tr"
KAP_DISCLOSURES = f"{KAP_BASE}/tr/bildirim-sorgu"

# KAP API (JSON endpoint — ayni veriyi sunuyor)
KAP_API_BASE = f"{KAP_BASE}/tr/api"
KAP_DISCLOSURE_API = f"{KAP_API_BASE}/disclosure"

# HTTP headers — KAP bot tespiti yapar
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Referer": f"{KAP_BASE}/tr/bildirim-sorgu",
    "X-Requested-With": "XMLHttpRequest",
}


class KAPScraper:
    """KAP bildirim scraper."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    # -------------------------------------------------------
    # Halka Arz Bildirimleri
    # -------------------------------------------------------

    async def fetch_ipo_disclosures(self, from_date: Optional[date] = None) -> list[dict]:
        """KAP'tan halka arz ile ilgili bildirimleri getirir.

        KAP bildirim API'si — disclosure type ile halka arz filtrelenir.
        """
        results = []

        try:
            # KAP disclosure query — halka arz konulu bildirimler
            # disclosureType: HalkAArz (halka arz kategori kodu)
            params = {
                "fromDate": (from_date or date.today()).strftime("%Y-%m-%d"),
                "toDate": date.today().strftime("%Y-%m-%d"),
                "subject": "halka arz",  # Konu filtresi
            }

            resp = await self.client.post(
                f"{KAP_API_BASE}/memberDisclosureQuery",
                json=params,
            )

            if resp.status_code == 200:
                data = resp.json()
                disclosures = data if isinstance(data, list) else data.get("data", [])
                for d in disclosures:
                    parsed = self._parse_ipo_disclosure(d)
                    if parsed:
                        results.append(parsed)
            else:
                logger.warning(f"KAP API response: {resp.status_code}")

        except Exception as e:
            logger.error(f"KAP halka arz scraping hatasi: {e}")

        # Fallback: HTML scraping
        if not results:
            results = await self._scrape_ipo_html()

        logger.info(f"KAP halka arz: {len(results)} bildirim bulundu")
        return results

    async def _scrape_ipo_html(self) -> list[dict]:
        """HTML fallback — KAP bildirim sayfasindan scrape."""
        results = []
        try:
            resp = await self.client.get(
                KAP_DISCLOSURES,
                params={"subject": "halka arz"}
            )
            if resp.status_code != 200:
                return results

            soup = BeautifulSoup(resp.text, "lxml")
            rows = soup.select("table.notification-table tbody tr")

            for row in rows:
                cells = row.select("td")
                if len(cells) < 5:
                    continue

                try:
                    results.append({
                        "kap_id": cells[0].get_text(strip=True),
                        "company_name": cells[1].get_text(strip=True),
                        "ticker": cells[2].get_text(strip=True),
                        "subject": cells[3].get_text(strip=True),
                        "published_at": cells[4].get_text(strip=True),
                        "url": KAP_BASE + (cells[0].select_one("a") or {}).get("href", ""),
                    })
                except Exception:
                    continue

        except Exception as e:
            logger.error(f"KAP HTML scraping hatasi: {e}")

        return results

    def _parse_ipo_disclosure(self, raw: dict) -> Optional[dict]:
        """KAP API yanit objesini IPO bilgisine donusturur."""
        try:
            title = raw.get("disclosureTitle", "") or raw.get("subject", "")
            company = raw.get("companyName", "") or raw.get("memberName", "")
            ticker = raw.get("stockCode", "") or raw.get("memberCode", "")

            # Halka arz ile ilgili mi kontrol et
            ipo_keywords = [
                "halka arz", "izahname", "tahsisat", "talep toplama",
                "fiyat araligi", "satis suresi", "dagitim listesi"
            ]
            text_lower = (title + " " + company).lower()
            if not any(kw in text_lower for kw in ipo_keywords):
                return None

            return {
                "kap_id": str(raw.get("disclosureIndex", raw.get("id", ""))),
                "company_name": company,
                "ticker": ticker.upper() if ticker else None,
                "subject": title,
                "published_at": raw.get("publishDate", raw.get("disclosureDate")),
                "url": f"{KAP_BASE}/tr/Bildirim/{raw.get('disclosureIndex', '')}",
                "raw": raw,
            }
        except Exception as e:
            logger.error(f"KAP disclosure parse hatasi: {e}")
            return None

    # -------------------------------------------------------
    # Bildirim Detay Sayfasi — Ek Bilgiler
    # -------------------------------------------------------

    async def fetch_disclosure_detail(self, kap_id: str) -> Optional[dict]:
        """Tek bir KAP bildiriminin detay sayfasini getirir.

        Halka arz izahnamesi, fiyat, tarih gibi detay bilgileri icin.
        """
        try:
            url = f"{KAP_BASE}/tr/Bildirim/{kap_id}"
            resp = await self.client.get(url)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "lxml")

            # Bildirim metin icerigini al
            content_div = soup.select_one(".disclosure-content, .sub-content, #divContent")
            if not content_div:
                return None

            text = content_div.get_text(separator="\n", strip=True)

            # Metin icerisinden halka arz detaylarini cikar
            detail = {
                "kap_id": kap_id,
                "full_text": text,
                "ipo_price": self._extract_price(text),
                "subscription_dates": self._extract_dates(text),
                "total_lots": self._extract_lots(text),
                "distribution_method": self._extract_distribution(text),
            }

            # PDF baglantilari (izahname vs)
            pdf_links = []
            for a in soup.select("a[href$='.pdf']"):
                href = a.get("href", "")
                if not href.startswith("http"):
                    href = KAP_BASE + href
                pdf_links.append({
                    "title": a.get_text(strip=True),
                    "url": href,
                })
            detail["pdf_links"] = pdf_links

            return detail

        except Exception as e:
            logger.error(f"KAP detay scraping hatasi ({kap_id}): {e}")
            return None

    # -------------------------------------------------------
    # KAP Haber Scraper (30 saniye aralik)
    # -------------------------------------------------------

    async def fetch_latest_disclosures(self, minutes: int = 5) -> list[dict]:
        """Son X dakikadaki tum KAP bildirimlerini getirir.

        Bu veriler daha sonra news_service tarafindan keyword ile filtrelenecek.
        """
        results = []
        try:
            params = {
                "fromDate": date.today().strftime("%Y-%m-%d"),
                "toDate": date.today().strftime("%Y-%m-%d"),
            }

            resp = await self.client.post(
                f"{KAP_API_BASE}/memberDisclosureQuery",
                json=params,
            )

            if resp.status_code == 200:
                data = resp.json()
                disclosures = data if isinstance(data, list) else data.get("data", [])

                for d in disclosures:
                    try:
                        results.append({
                            "kap_id": str(d.get("disclosureIndex", d.get("id", ""))),
                            "company_name": d.get("companyName", d.get("memberName", "")),
                            "ticker": (d.get("stockCode", d.get("memberCode", "")) or "").upper(),
                            "subject": d.get("disclosureTitle", d.get("subject", "")),
                            "published_at": d.get("publishDate", d.get("disclosureDate")),
                            "url": f"{KAP_BASE}/tr/Bildirim/{d.get('disclosureIndex', '')}",
                        })
                    except Exception:
                        continue
            else:
                logger.warning(f"KAP latest API: {resp.status_code}")

        except Exception as e:
            logger.error(f"KAP latest disclosures hatasi: {e}")

        return results

    # -------------------------------------------------------
    # Yardimci Fonksiyonlar — Metin Icerisinden Bilgi Cikarma
    # -------------------------------------------------------

    def _extract_price(self, text: str) -> Optional[Decimal]:
        """Metinden halka arz fiyatini cikarir."""
        patterns = [
            r"(?:halka\s*arz\s*fiyat[ıi]|pay\s*ba[şs][ıi]na\s*fiyat|birim\s*pay\s*fiyat[ıi])\s*[:=]?\s*(\d+[.,]\d{2})\s*(?:TL|tl)",
            r"(\d+[.,]\d{2})\s*TL\s*(?:olarak|fiyat)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                price_str = match.group(1).replace(",", ".")
                return Decimal(price_str)
        return None

    def _extract_dates(self, text: str) -> dict:
        """Metinden basvuru tarihlerini cikarir."""
        dates = {}
        # dd.mm.yyyy veya dd/mm/yyyy formatinda tarih ara
        date_pattern = r"(\d{1,2}[./]\d{1,2}[./]\d{4})"
        found = re.findall(date_pattern, text)
        if len(found) >= 2:
            dates["start"] = found[0]
            dates["end"] = found[-1]
        return dates

    def _extract_lots(self, text: str) -> Optional[int]:
        """Metinden toplam lot/pay miktarini cikarir."""
        patterns = [
            r"(\d[\d.]*)\s*(?:adet|lot|pay)\s*(?:halka\s*arz|satisa\s*sunul)",
            r"toplam\s*(\d[\d.]*)\s*(?:adet|lot|pay)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                num_str = match.group(1).replace(".", "")
                return int(num_str)
        return None

    def _extract_distribution(self, text: str) -> Optional[str]:
        """Dagitim yontemini tespit eder."""
        text_lower = text.lower()
        if "eşit dağıtım" in text_lower or "esit dagitim" in text_lower:
            return "esit"
        elif "oransal" in text_lower:
            return "oransal"
        elif "karma" in text_lower:
            return "karma"
        return None
