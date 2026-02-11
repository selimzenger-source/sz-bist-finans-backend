"""HalkArz.com Scraper — WordPress REST API + RSS.

Kaynak: https://halkarz.com
Calisma Zamani: Her 4 saatte bir (Gedik ile beraber)
Is: Sirket bilgileri, analist raporlari, izahname, detay bilgi

HalkArz.com WordPress tabanli bir site.
- WordPress REST API: /wp-json/wp/v2/posts
- RSS Feed: /feed/
- Ayrica ozel sayfalar: /halka-arz/{sirket-slug}/
"""

import re
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HALKARZ_BASE = "https://halkarz.com"
HALKARZ_WP_API = f"{HALKARZ_BASE}/wp-json/wp/v2"
HALKARZ_RSS = f"{HALKARZ_BASE}/feed/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


class HalkArzScraper:
    """HalkArz.com WordPress scraper."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    # -------------------------------------------------------
    # WordPress REST API
    # -------------------------------------------------------

    async def fetch_wp_posts(self, per_page: int = 20, page: int = 1) -> list[dict]:
        """WordPress REST API ile son yazilari getirir.

        Returns:
            [{title, url, content, date, categories, ...}, ...]
        """
        results = []
        try:
            # Halka arz kategorisindeki yazilari cek
            resp = await self.client.get(
                f"{HALKARZ_WP_API}/posts",
                params={
                    "per_page": per_page,
                    "page": page,
                    "orderby": "date",
                    "order": "desc",
                    "_embed": "",  # Kategori ve medya bilgisi dahil
                }
            )

            if resp.status_code != 200:
                logger.warning("HalkArz WP API yaniti: %d", resp.status_code)
                return results

            posts = resp.json()
            if not isinstance(posts, list):
                return results

            for post in posts:
                title = self._clean_html(
                    post.get("title", {}).get("rendered", "")
                )
                content = self._clean_html(
                    post.get("content", {}).get("rendered", "")
                )
                excerpt = self._clean_html(
                    post.get("excerpt", {}).get("rendered", "")
                )

                result = {
                    "source": "halkarz",
                    "wp_id": post.get("id"),
                    "title": title,
                    "url": post.get("link", ""),
                    "content": content,
                    "excerpt": excerpt,
                    "date": post.get("date"),
                    "modified": post.get("modified"),
                    "slug": post.get("slug", ""),
                }

                # Kategori bilgisi
                embedded = post.get("_embedded", {})
                terms = embedded.get("wp:term", [])
                categories = []
                for term_group in terms:
                    if isinstance(term_group, list):
                        for term in term_group:
                            categories.append(term.get("name", ""))
                result["categories"] = categories

                results.append(result)

            logger.info("HalkArz WP API: %d yazi bulundu", len(results))

        except Exception as e:
            logger.error("HalkArz WP API hatasi: %s", e)

        return results

    # -------------------------------------------------------
    # RSS Feed
    # -------------------------------------------------------

    async def fetch_rss_feed(self) -> list[dict]:
        """RSS feed'den son yazilari getirir."""
        results = []
        try:
            resp = await self.client.get(HALKARZ_RSS)
            if resp.status_code != 200:
                logger.warning("HalkArz RSS yaniti: %d", resp.status_code)
                return results

            soup = BeautifulSoup(resp.text, "xml")
            items = soup.find_all("item")

            for item in items:
                title = item.find("title")
                link = item.find("link")
                desc = item.find("description")
                pub_date = item.find("pubDate")
                content = item.find("content:encoded")

                result = {
                    "source": "halkarz_rss",
                    "title": title.get_text(strip=True) if title else "",
                    "url": link.get_text(strip=True) if link else "",
                    "excerpt": self._clean_html(desc.get_text(strip=True) if desc else ""),
                    "content": self._clean_html(content.get_text(strip=True) if content else ""),
                    "date": pub_date.get_text(strip=True) if pub_date else "",
                }

                # Kategoriler
                categories = [c.get_text(strip=True) for c in item.find_all("category")]
                result["categories"] = categories

                results.append(result)

            logger.info("HalkArz RSS: %d yazi bulundu", len(results))

        except Exception as e:
            logger.error("HalkArz RSS hatasi: %s", e)

        return results

    # -------------------------------------------------------
    # Halka Arz Detay Sayfasi
    # -------------------------------------------------------

    async def fetch_ipo_detail_page(self, url: str) -> dict | None:
        """Halka arz detay sayfasindan bilgi cikarir.

        HalkArz.com'da her halka arzin detay sayfasi bulunur:
        - Sirket bilgisi
        - Halka arz fiyati
        - Basvuru tarihleri
        - Araci kurum listesi
        - Sektor bilgisi
        - Izahname linki
        """
        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(separator="\n", strip=True)

            detail = {
                "url": url,
            }

            # Sirket adi — h1 veya baslik
            h1 = soup.find("h1")
            if h1:
                detail["company_name"] = h1.get_text(strip=True)

            # Tablo verilerini cikar (HalkArz genellikle tablo kullanir)
            tables = soup.find_all("table")
            for table in tables:
                self._parse_detail_table(table, detail)

            # Eger tablo yoksa metin icinden cikar
            if "ipo_price" not in detail:
                detail["ipo_price"] = self._extract_price(text)

            if "subscription_start" not in detail:
                dates = self._extract_dates(text)
                if dates.get("start"):
                    detail["subscription_start"] = dates["start"]
                if dates.get("end"):
                    detail["subscription_end"] = dates["end"]

            if "lead_broker" not in detail:
                detail["lead_broker"] = self._extract_broker(text)

            if "sector" not in detail:
                detail["sector"] = self._extract_sector(text)

            if "ticker" not in detail:
                detail["ticker"] = self._extract_ticker(text)

            # Izahname / PDF linkleri
            pdf_links = []
            for a in soup.find_all("a", href=True):
                href = a.get("href", "")
                link_text = a.get_text(strip=True).lower()
                if href.endswith(".pdf") or "izahname" in link_text or "prospekt" in link_text:
                    full_url = href if href.startswith("http") else HALKARZ_BASE + href
                    pdf_links.append({
                        "title": a.get_text(strip=True),
                        "url": full_url,
                    })
            detail["pdf_links"] = pdf_links

            if pdf_links:
                detail["prospectus_url"] = pdf_links[0]["url"]

            # Sirket tanitim metni
            article = soup.find("article") or soup.find(class_="entry-content")
            if article:
                paragraphs = article.find_all("p")
                description_parts = []
                for p in paragraphs[:5]:  # Ilk 5 paragraf
                    p_text = p.get_text(strip=True)
                    if len(p_text) > 30:
                        description_parts.append(p_text)
                if description_parts:
                    detail["company_description"] = "\n".join(description_parts)

            return detail

        except Exception as e:
            logger.error("HalkArz detay sayfasi hatasi (%s): %s", url, e)
            return None

    # -------------------------------------------------------
    # Halka Arz Listesi Sayfasi
    # -------------------------------------------------------

    async def fetch_ipo_list_page(self) -> list[dict]:
        """Ana halka arz listesi sayfasindan bilgi cikarir."""
        results = []
        try:
            # HalkArz.com ana sayfasi veya halka arz listesi
            for url in [HALKARZ_BASE, f"{HALKARZ_BASE}/halka-arzlar/",
                        f"{HALKARZ_BASE}/category/halka-arz/"]:
                resp = await self.client.get(url)
                if resp.status_code != 200:
                    continue

                soup = BeautifulSoup(resp.text, "lxml")

                # Yazi/kart linkleri bul
                for link in soup.find_all("a", href=True):
                    href = link.get("href", "")
                    text = link.get_text(strip=True)

                    if not text or len(text) < 5:
                        continue

                    # Halka arz detay sayfasi linki mi?
                    is_ipo_link = (
                        "halka-arz" in href.lower()
                        and href != url
                        and not href.endswith("/category/halka-arz/")
                        and len(text) > 3
                    )

                    if is_ipo_link:
                        full_url = href if href.startswith("http") else HALKARZ_BASE + href
                        # Ayni URL'yi tekrar ekleme
                        if not any(r["url"] == full_url for r in results):
                            results.append({
                                "source": "halkarz",
                                "title": text,
                                "url": full_url,
                            })

                if results:
                    break

            logger.info("HalkArz liste sayfasi: %d link bulundu", len(results))

        except Exception as e:
            logger.error("HalkArz liste sayfasi hatasi: %s", e)

        return results

    # -------------------------------------------------------
    # Parse Yardimcilari
    # -------------------------------------------------------

    def _parse_detail_table(self, table, detail: dict):
        """Detay sayfasindaki tabloyu parse eder.

        HalkArz genellikle key-value tablo yapisi kullanir:
        | Sirket Adi    | XYZ A.S.     |
        | Borsa Kodu    | XYZYZ        |
        | Fiyat         | 25.50 TL     |
        """
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue

            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)

            if not label or not value:
                continue

            # Fiyat
            if any(k in label for k in ["fiyat", "pay bedeli", "birim pay"]):
                price = self._parse_decimal(value)
                if price:
                    detail["ipo_price"] = price

            # Borsa kodu / ticker
            elif any(k in label for k in ["borsa kodu", "hisse kodu", "ticker", "sembol"]):
                ticker = re.sub(r"[^A-Z]", "", value.upper())
                if 3 <= len(ticker) <= 10:
                    detail["ticker"] = ticker

            # Basvuru tarihi
            elif any(k in label for k in ["başvuru", "basvuru", "talep toplama"]):
                dates = self._extract_dates(value)
                if dates.get("start"):
                    detail["subscription_start"] = dates["start"]
                if dates.get("end"):
                    detail["subscription_end"] = dates["end"]

            # Islem baslangic
            elif any(k in label for k in ["işlem", "islem", "borsada"]):
                d = self._parse_tr_date(value)
                if d:
                    detail["trading_start"] = d

            # Lot / Pay
            elif any(k in label for k in ["toplam lot", "pay sayisi", "pay miktari", "adet"]):
                lots = self._parse_int(value)
                if lots:
                    detail["total_lots"] = lots

            # Araci kurum
            elif any(k in label for k in ["aracı", "araci", "konsorsiyum"]):
                detail["lead_broker"] = value

            # Sektor
            elif "sektör" in label or "sektor" in label:
                detail["sector"] = value

            # Dagitim yontemi
            elif "dağıtım" in label or "dagitim" in label:
                detail["distribution_method"] = self._normalize_distribution(value)

            # Halka aciklik orani
            elif "halka açıklık" in label or "halka aciklik" in label:
                pct = self._parse_decimal(value)
                if pct:
                    detail["public_float_pct"] = pct

            # Sirket adi
            elif any(k in label for k in ["şirket", "sirket"]) and "company_name" not in detail:
                detail["company_name"] = value

    def _clean_html(self, html: str) -> str:
        """HTML tag'larini temizler."""
        if not html:
            return ""
        soup = BeautifulSoup(html, "lxml")
        return soup.get_text(separator=" ", strip=True)

    def _extract_price(self, text: str) -> Decimal | None:
        patterns = [
            r"(?:halka\s*arz\s*fiyat[ıi]|birim\s*pay)\s*[:=]?\s*(\d+[.,]\d+)",
            r"(\d+[.,]\d+)\s*(?:TL|₺)\s*(?:fiyat|olarak)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return self._parse_decimal(match.group(1))
        return None

    def _extract_dates(self, text: str) -> dict:
        dates_found = re.findall(r"(\d{1,2}[./]\d{1,2}[./]\d{4})", text)
        result = {}
        if len(dates_found) >= 1:
            result["start"] = self._parse_tr_date(dates_found[0])
        if len(dates_found) >= 2:
            result["end"] = self._parse_tr_date(dates_found[-1])
        return result

    def _extract_broker(self, text: str) -> str | None:
        patterns = [
            r"[Aa]racı?\s*[Kk]urum\s*[:=]?\s*(.+?)(?:\n|$)",
            r"[Kk]onsorsiyum\s*[:=]?\s*(.+?)(?:\n|$)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                broker = match.group(1).strip()
                if len(broker) > 3:
                    return broker
        return None

    def _extract_sector(self, text: str) -> str | None:
        match = re.search(r"[Ss]ektör\s*[:=]?\s*(.+?)(?:\n|$)", text)
        if match:
            return match.group(1).strip()
        return None

    def _extract_ticker(self, text: str) -> str | None:
        match = re.search(r"[Bb]orsa\s*[Kk]odu\s*[:=]?\s*([A-Z]{3,10})", text)
        if match:
            return match.group(1).upper()
        return None

    def _parse_decimal(self, val: str) -> Decimal | None:
        if not val:
            return None
        try:
            # Sadece rakam, nokta ve virgul birak
            cleaned = re.sub(r"[^\d.,]", "", val)
            cleaned = cleaned.replace(",", ".")
            return Decimal(cleaned)
        except Exception:
            return None

    def _parse_int(self, val: str) -> int | None:
        if not val:
            return None
        try:
            cleaned = re.sub(r"[^\d]", "", val)
            return int(cleaned) if cleaned else None
        except Exception:
            return None

    def _parse_tr_date(self, date_str: str) -> date | None:
        match = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", date_str)
        if match:
            try:
                return date(
                    int(match.group(3)),
                    int(match.group(2)),
                    int(match.group(1)),
                )
            except ValueError:
                pass
        return None

    def _normalize_distribution(self, text: str) -> str:
        text_lower = text.lower()
        if "eşit" in text_lower or "esit" in text_lower:
            return "esit"
        elif "oransal" in text_lower:
            return "oransal"
        elif "karma" in text_lower:
            return "karma"
        return text


# -------------------------------------------------------
# Scheduler Entrypoint
# -------------------------------------------------------

async def scrape_halkarz():
    """Scheduler tarafindan cagirilir — HalkArz.com'dan bilgi ceker."""
    from app.database import async_session
    from app.services.ipo_service import IPOService

    scraper = HalkArzScraper()
    try:
        # 1. WordPress API ile yazilari cek
        posts = await scraper.fetch_wp_posts(per_page=20)

        # 2. Fallback: RSS feed
        if not posts:
            rss_items = await scraper.fetch_rss_feed()
            posts = rss_items

        # 3. Her yazi icin halka arz bilgisi cikar
        async with async_session() as db:
            ipo_service = IPOService(db)
            updated_count = 0

            for post in posts:
                url = post.get("url", "")
                if not url:
                    continue

                # Halka arz detay sayfasi mi kontrol et
                title = post.get("title", "").lower()
                categories = [c.lower() for c in post.get("categories", [])]

                is_ipo_related = (
                    "halka arz" in title
                    or any("halka" in c for c in categories)
                    or "halka-arz" in url.lower()
                )

                if not is_ipo_related:
                    continue

                # Detay sayfasindan ek bilgi cek
                detail = await scraper.fetch_ipo_detail_page(url)
                if not detail:
                    continue

                company_name = detail.get("company_name") or post.get("title")
                if not company_name:
                    continue

                # Veritabanina kaydet/guncelle
                update_data = {"company_name": company_name}

                for field in ["ticker", "ipo_price", "subscription_start",
                              "subscription_end", "trading_start", "total_lots",
                              "sector", "lead_broker", "distribution_method",
                              "public_float_pct", "company_description",
                              "prospectus_url"]:
                    if detail.get(field) is not None:
                        update_data[field] = detail[field]

                await ipo_service.create_or_update_ipo(update_data)
                updated_count += 1

            await db.commit()
            logger.info("HalkArz: %d halka arz bilgisi guncellendi", updated_count)

    except Exception as e:
        logger.error("HalkArz scraper hatasi: %s", e)
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("HalkArz Scraper", str(e))
        except Exception:
            pass
    finally:
        await scraper.close()
