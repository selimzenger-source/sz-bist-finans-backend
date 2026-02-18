"""InfoYatirim halka arz takvimi scraper.

Kaynak: https://infoyatirim.com/halka-arz-takvimi
Sayfalanmis tablo (3 sayfa, ~25 satir/sayfa):
  Sirket Adi, Hisse Kodu, Durumu, Katilim Tarihi/Saati,
  Fiyati, Katilan Kisi Sayisi, Dagitilacak Lot Adedi,
  Isleme B.Tarihi, Katilim Endeksi, Dagitim Yontemi, Katilim Yontemi

Her satirin detay sayfasi da var: /halka-arz-takvimi/{slug}
"""

import logging
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://infoyatirim.com"
CALENDAR_URL = f"{BASE_URL}/halka-arz-takvimi"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


class InfoYatirimScraper:
    """InfoYatirim halka arz takvimi scraper."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def fetch_all_ipos(self, max_pages: int = 5) -> list[dict]:
        """Tum sayfalardaki halka arzlari getirir.

        Returns:
            [{company_name, ticker, status, dates, price, ...}, ...]
        """
        all_results = []

        for page in range(1, max_pages + 1):
            url = CALENDAR_URL if page == 1 else f"{CALENDAR_URL}/page/{page}"
            results = await self._scrape_page(url)

            if not results:
                break  # Bos sayfa — son sayfaya ulastik

            all_results.extend(results)
            logger.info(f"InfoYatirim sayfa {page}: {len(results)} halka arz")

        logger.info(f"InfoYatirim toplam: {len(all_results)} halka arz")
        return all_results

    async def _scrape_page(self, url: str) -> list[dict]:
        """Tek bir sayfayi scrape eder."""
        results = []

        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"InfoYatirim sayfa yaniti: {resp.status_code} — {url}")
                return results

            soup = BeautifulSoup(resp.text, "lxml")

            # Halka arz tablosunu bul — "Sirket Adi" basligini iceren tablo
            table = None
            for t in soup.find_all("table"):
                header_text = t.get_text(strip=True).lower()
                if "şirket adı" in header_text or "sirket adi" in header_text or "hisse kodu" in header_text:
                    table = t
                    break

            if not table:
                return results

            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 8:
                    continue

                parsed = self._parse_row(cells, row)
                if parsed:
                    results.append(parsed)

        except Exception as e:
            logger.error(f"InfoYatirim scraping hatasi ({url}): {e}")

        return results

    def _parse_row(self, cells, row) -> Optional[dict]:
        """Tablo satirini parse eder."""
        try:
            company_name = cells[0].get_text(strip=True)
            ticker = cells[1].get_text(strip=True).upper()
            status_raw = cells[2].get_text(strip=True)
            dates_raw = cells[3].get_text(strip=True)
            price_raw = cells[4].get_text(strip=True)
            participants_raw = cells[5].get_text(strip=True)
            lots_raw = cells[6].get_text(strip=True)
            trading_start_raw = cells[7].get_text(strip=True)

            # Opsiyonel kolonlar
            endeks_raw = cells[8].get_text(strip=True) if len(cells) > 8 else ""
            dagitim_raw = cells[9].get_text(strip=True) if len(cells) > 9 else ""
            katilim_raw = cells[10].get_text(strip=True) if len(cells) > 10 else ""

            # Detay sayfasi linki
            detail_link = row.find("a")
            detail_url = None
            if detail_link:
                href = detail_link.get("href", "")
                detail_url = href if href.startswith("http") else BASE_URL + href

            # Durum mapping
            status = self._map_status(status_raw)

            # Fiyat parse
            price = self._parse_price(price_raw)

            # Lot parse
            total_lots = self._parse_number(lots_raw)

            # Katilimci sayisi
            participants = self._parse_number(participants_raw)

            # Tarih parse (ornek: "5-6 Subat 2026", "28-29-30 Ocak 2026")
            subscription_start, subscription_end = self._parse_date_range(dates_raw)

            # Isleme baslama tarihi
            trading_start = self._parse_single_date(trading_start_raw)

            # Dagitim yontemi (kod + aciklama)
            distribution_code = self._map_distribution(dagitim_raw)
            distribution_desc = self._distribution_description(dagitim_raw)

            # Katilim yontemi (kod + aciklama)
            participation_code = self._map_participation(katilim_raw)
            participation_desc = self._participation_description(katilim_raw)

            return {
                "source": "infoyatirim",
                "company_name": company_name,
                "ticker": ticker if ticker and ticker != "-" else None,
                "status": status,
                "ipo_price": price,
                "total_lots": total_lots,
                "total_applicants": participants,
                "subscription_start": subscription_start,
                "subscription_end": subscription_end,
                "subscription_dates_raw": dates_raw,
                "trading_start": trading_start,
                "distribution_method": distribution_code,
                "distribution_description": distribution_desc,
                "distribution_raw": dagitim_raw,
                "participation_method": participation_code,
                "participation_description": participation_desc,
                "participation_raw": katilim_raw,
                "katilim_endeksi": "uygun" if "uygundur" in endeks_raw.lower() else "uygun_degil" if endeks_raw else None,
                "detail_url": detail_url,
            }

        except Exception as e:
            logger.error(f"InfoYatirim row parse hatasi: {e}")
            return None

    def _map_status(self, raw: str) -> str:
        """Durum metnini standart statuse cevirir."""
        raw_lower = raw.lower()
        if "talep" in raw_lower and "toplan" in raw_lower:
            return "active"
        elif "tamamland" in raw_lower:
            return "completed"
        elif "iptal" in raw_lower:
            return "cancelled"
        elif "ertelend" in raw_lower:
            return "postponed"
        return "upcoming"

    def _parse_price(self, raw: str) -> Optional[Decimal]:
        """Fiyat parse — '14.70 TL', '21,50 TL', '14,70\nTL' formatlarini destekler.

        Turkce format: nokta binlik ayirici, virgul ondalik
        Ama bazi kaynaklarda nokta ondalik ayirici olarak kullanilir (14.70)
        Kural: Son isaret virgul ise → virgul ondalik. Nokta ise ve sonrasinda 2 hane varsa → nokta ondalik.
        """
        if not raw or raw.strip() in ("-", ""):
            return None
        try:
            clean = raw.replace("TL", "").replace("tl", "").replace("\n", "").replace("\r", "").strip()

            # "14.70" → nokta ondalik (son isaret nokta ve sonrasinda 1-2 hane)
            if re.match(r"^\d+\.\d{1,2}$", clean):
                return Decimal(clean)

            # "21,50" → virgul ondalik
            if re.match(r"^\d+,\d{1,2}$", clean):
                return Decimal(clean.replace(",", "."))

            # "1.234,56" → binlik nokta, ondalik virgul
            if "," in clean:
                clean = clean.replace(".", "").replace(",", ".")
                return Decimal(clean)

            # "1234" → tam sayi
            clean = clean.replace(".", "")
            return Decimal(clean)
        except Exception:
            return None

    def _parse_number(self, raw: str) -> Optional[int]:
        """Sayi parse — '54.578.570', '959.375' formatlarini destekler."""
        if not raw or raw.strip() in ("-", "", "***"):
            return None
        try:
            clean = raw.replace(".", "").replace(",", "").strip()
            return int(clean)
        except Exception:
            return None

    def _parse_date_range(self, raw: str) -> tuple[Optional[date], Optional[date]]:
        """Tarih araligi parse.

        Ornekler:
            '5-6 Subat 2026' → (2026-02-05, 2026-02-06)
            '28-29-30 Ocak 2026' → (2026-01-28, 2026-01-30)
            '12 - 13 - 14 Kasim 2025' → (2025-11-12, 2025-11-14)
        """
        if not raw or raw.strip() in ("-", ""):
            return None, None

        try:
            # Ay adlarini ay numarasina cevir
            months = {
                "ocak": 1, "subat": 2, "şubat": 2, "mart": 3, "nisan": 4,
                "mayis": 5, "mayıs": 5, "haziran": 6, "temmuz": 7,
                "agustos": 8, "ağustos": 8, "eylul": 9, "eylül": 9,
                "ekim": 10, "kasim": 11, "kasım": 11, "aralik": 12, "aralık": 12,
            }

            raw_lower = raw.lower().strip()

            # Yili bul
            year_match = re.search(r"(\d{4})", raw)
            if not year_match:
                return None, None
            year = int(year_match.group(1))

            # Ay bul
            month = None
            for ay_name, ay_num in months.items():
                if ay_name in raw_lower:
                    month = ay_num
                    break

            if not month:
                return None, None

            # Gun numaralarini bul
            days = re.findall(r"\b(\d{1,2})\b", raw)
            # Yili cikar
            days = [int(d) for d in days if int(d) <= 31]

            if not days:
                return None, None

            start = date(year, month, days[0])
            end = date(year, month, days[-1]) if len(days) > 1 else start
            return start, end

        except Exception:
            return None, None

    def _parse_single_date(self, raw: str) -> Optional[date]:
        """Tek tarih parse — '06 Subat 2026', '18 Aralik 2025' vs."""
        if not raw or raw.strip() in ("-", ""):
            return None
        start, _ = self._parse_date_range(raw)
        return start

    def _map_distribution(self, raw: str) -> Optional[str]:
        """Dagitim yontemi → standart kod."""
        raw_lower = raw.lower()
        if not raw_lower or raw_lower.strip() in ("-", ""):
            return None
        if "tamamı eşit" in raw_lower or "tamami esit" in raw_lower:
            return "tamami_esit"
        if ("bireysel" in raw_lower) and ("eşit" in raw_lower or "esit" in raw_lower):
            if "oransal" in raw_lower:
                return "karma"  # bireysele esit + yuksek basvuruya oransal
            return "bireysele_esit"
        if "eşit" in raw_lower or "esit" in raw_lower:
            return "esit"
        if "oransal" in raw_lower:
            return "oransal"
        if "karma" in raw_lower:
            return "karma"
        return raw.strip()

    def _distribution_description(self, raw: str) -> Optional[str]:
        """Dagitim yontemi → kullaniciya anlasilir Turkce aciklama."""
        raw_lower = raw.lower()
        if not raw_lower or raw_lower.strip() in ("-", ""):
            return None

        if "tamamı eşit" in raw_lower or "tamami esit" in raw_lower:
            return (
                "Herkese esit lot dagitilir. Kac kisi basvurursa basvursun, "
                "toplam lot sayisi basvuran kisi sayisina bolunur ve herkes "
                "ayni miktarda hisse alir."
            )

        if ("bireysel" in raw_lower) and ("eşit" in raw_lower or "esit" in raw_lower):
            if "oransal" in raw_lower:
                return (
                    "Bireysel yatirimcilara esit dagitim yapilir — herkes ayni "
                    "miktarda lot alir. Yuksek basvuru yapan buyuk yatirimcilara "
                    "ise oransal dagitim uygulanir (ne kadar cok para yatirirsan "
                    "o kadar cok lot). Kucuk yatirimci icin avantajli!"
                )
            return (
                "Bireysel (kucuk) yatirimcilara esit dagitim yapilir. "
                "Herkes ayni miktarda lot alir — 1.000 TL yatiran da "
                "100.000 TL yatiran da ayni sayida hisse alir. "
                "Kucuk yatirimci icin en avantajli dagitim yontemi!"
            )

        if "eşit" in raw_lower or "esit" in raw_lower:
            return (
                "Esit dagitim — tum basvuranlara ayni miktarda lot verilir. "
                "Yatirdigin tutar farketmez, herkes esit hisse alir."
            )

        if "oransal" in raw_lower:
            return (
                "Oransal dagitim — ne kadar para yatirirsan o kadar cok hisse "
                "alirsin. Buyuk yatirimcilar daha fazla lot alir. Kucuk yatirimci "
                "icin dezavantajli olabilir."
            )

        return raw.strip()

    def _map_participation(self, raw: str) -> Optional[str]:
        """Katilim yontemi → standart kod."""
        raw_lower = raw.lower()
        if not raw_lower or raw_lower.strip() in ("-", ""):
            return None
        if "borsada" in raw_lower or "borsa" in raw_lower:
            return "borsada_satis"
        if "talep" in raw_lower:
            return "talep_toplama"
        return raw.strip()

    def _participation_description(self, raw: str) -> Optional[str]:
        """Katilim yontemi → kullaniciya anlasilir Turkce aciklama."""
        raw_lower = raw.lower()
        if not raw_lower or raw_lower.strip() in ("-", ""):
            return None

        if "borsada" in raw_lower or "borsa" in raw_lower:
            return (
                "Borsada satış yöntemiyle katılım yapılır. Normal hisse senedi "
                "alır gibi, aracı kurum uygulamanızdan borsa kodunu (örneğin BESTE) "
                "yazıp 'AL' emri verirsiniz. Talep toplama tarihlerinde borsa "
                "üzerinden işlem yapılır. Herhangi bir aracı kurumdan başvurabilirsiniz."
            )

        if "talep" in raw_lower:
            return (
                "Talep toplama yöntemiyle katılım yapılır. Aracı kurumunuzun "
                "(banka veya yatırım kuruluşu) uygulamasından 'Halka Arz' "
                "bölümüne girin ve başvurunuzu yapın. Talep toplama tarihleri "
                "arasında başvuru yapmanız gerekir."
            )

        return raw.strip()


# -------------------------------------------------------
# Scheduler Entrypoint
# -------------------------------------------------------

async def scrape_infoyatirim():
    """Scheduler tarafindan cagirilir — InfoYatirim'dan halka arz bilgisi ceker.

    InfoYatirim halka arz takvimi:
    - Sirket Adi, Hisse Kodu, Durum, Talep Toplama Tarihi
    - Fiyat, Katilimci Sayisi, Dagitilacak Lot, Islem Baslangic Tarihi
    - Dagitim Yontemi, Katilim Yontemi

    HalkArz.com ve Gedik'in yedegi olarak kullanilir (2. alternatif kaynak).
    """
    from app.database import async_session
    from app.services.ipo_service import IPOService

    scraper = InfoYatirimScraper()
    try:
        ipos = await scraper.fetch_all_ipos(max_pages=3)
        if not ipos:
            logger.info("InfoYatirim: Halka arz bulunamadi")
            return

        async with async_session() as db:
            ipo_service = IPOService(db)
            updated_count = 0

            for ipo_data in ipos:
                company_name = ipo_data.get("company_name")
                if not company_name:
                    continue

                # Veritabanina kaydet/guncelle
                update_data = {
                    "company_name": company_name,
                }

                # None olmayan alanlari ekle
                for field in ["ticker", "ipo_price", "subscription_start",
                              "subscription_end", "trading_start", "total_lots",
                              "distribution_method", "distribution_description",
                              "participation_method", "participation_description",
                              "katilim_endeksi"]:
                    if ipo_data.get(field) is not None:
                        update_data[field] = ipo_data[field]

                # total_applicants → extra bilgi olarak
                if ipo_data.get("total_applicants"):
                    update_data["total_applicants"] = ipo_data["total_applicants"]

                # Status guard: InfoYatirim mevcut IPO'larin statusunu
                # override etmez — bu is auto_update_statuses() tarafindan yonetilir.

                await ipo_service.create_or_update_ipo(update_data)
                updated_count += 1

            await db.commit()
            logger.info("InfoYatirim: %d halka arz guncellendi", updated_count)

    except Exception as e:
        logger.error("InfoYatirim scraper hatasi: %s", e)
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("InfoYatirim Scraper", str(e))
        except Exception:
            pass
    finally:
        await scraper.close()
