"""Gedik Yatirim halka arz takvimi scraper (3. alternatif kaynak).

Kaynak: https://gedik.com/halka-arz-takvimi
Detay:  https://gedik.com/halka-arzlar/{slug}

Liste sayfasi (Chakra UI):
  - Ticker (p.companyName), Sirket Adi (p.companyDetails)
  - Durum (p.badgeText: AKTİF, YAKINDA vb.)
  - Tarih + Fiyat (p elementleri, card icerisinde)
  - Detay linki (/halka-arzlar/{slug})

Detay sayfasi (key-value):
  - Sirket Adi, Borsa Kodu, Talep Toplama Tarihleri
  - Halka Arz Fiyati, Dagitim Yontemi, Pazar
  - Toplam Lot, Halka Arz Buyuklugu, Halka Aciklik Orani
  - Lot Tahmini (350.000 Kisi Katilirsa → estimated_lots_per_person)
  - Tahsisat Gruplari (bireysel %, kurumsal % vs.)

HalkArz.com ve InfoYatirim'a 3. alternatif olarak kullanilir.
CloudFlare yok — httpx ile dogrudan cekilebilir.
"""

import logging
import re
from datetime import date
from decimal import Decimal
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://gedik.com"
CALENDAR_URL = f"{BASE_URL}/halka-arz-takvimi"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


class GedikScraper:
    """Gedik Yatirim halka arz scraper."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    # -------------------------------------------------------
    # Liste Sayfasi
    # -------------------------------------------------------

    async def fetch_ipo_list(self) -> list[dict]:
        """Ana halka arz takvimi sayfasindan kart bilgilerini ceker.

        Returns:
            [{ticker, company_name, status, dates_raw, ipo_price, detail_url}, ...]
        """
        results = []

        try:
            resp = await self.client.get(CALENDAR_URL)
            if resp.status_code != 200:
                logger.warning("Gedik liste sayfasi yaniti: %d", resp.status_code)
                return results

            soup = BeautifulSoup(resp.text, "lxml")

            # Her kart bir <a> ici — ticker'lar p.companyName ile bulunur
            ticker_elements = soup.find_all("p", class_="companyName")

            seen_tickers = set()
            for ticker_el in ticker_elements:
                ticker = ticker_el.get_text(strip=True).upper()
                if not ticker or ticker in seen_tickers:
                    continue
                seen_tickers.add(ticker)

                # Kartin parent <a> tagini bul
                card_link = self._find_parent_link(ticker_el)
                if not card_link:
                    continue

                # Sirket adi
                company_el = card_link.find("p", class_="companyDetails")
                company_name = company_el.get_text(strip=True) if company_el else None

                # Durum (badge)
                badge_el = card_link.find("p", class_="badgeText")
                status_raw = badge_el.get_text(strip=True) if badge_el else ""
                status = self._map_status(status_raw)

                # Tarih ve fiyat — css-h3tw1x class'li p'ler
                info_elements = card_link.find_all("p", class_=re.compile(r"css-h3tw1x"))
                dates_raw = ""
                price_raw = ""
                if len(info_elements) >= 1:
                    dates_raw = info_elements[0].get_text(strip=True)
                if len(info_elements) >= 2:
                    price_raw = info_elements[1].get_text(strip=True)

                # Detay URL
                href = card_link.get("href", "")
                detail_url = href if href.startswith("http") else BASE_URL + href

                # Tarih parse
                subscription_start, subscription_end = self._parse_date_range(dates_raw)

                # Fiyat parse
                ipo_price = self._parse_price(price_raw)

                results.append({
                    "source": "gedik",
                    "ticker": ticker,
                    "company_name": company_name,
                    "status": status,
                    "status_raw": status_raw,
                    "ipo_price": ipo_price,
                    "subscription_start": subscription_start,
                    "subscription_end": subscription_end,
                    "dates_raw": dates_raw,
                    "detail_url": detail_url,
                })

            logger.info("Gedik liste: %d halka arz bulundu", len(results))

        except Exception as e:
            logger.error("Gedik liste scraping hatasi: %s", e)

        return results

    # -------------------------------------------------------
    # Detay Sayfasi
    # -------------------------------------------------------

    async def fetch_ipo_detail(self, url: str) -> dict | None:
        """Detay sayfasindan zengin bilgi cikarir.

        Gedik detay sayfasi key-value yapida:
        p.css-1s4g5fq → label (orn: "Halka Arz Fiyatı:")
        p.css-12ghvue → value (orn: "11,20 TL")

        Ayrica lot tahmini tablosu:
        "350.000 Kişi Katılırsa (lot) : 320"

        Returns:
            {ticker, company_name, ipo_price, distribution_method,
             market_segment, total_lots, estimated_lots_per_person, ...}
        """
        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "lxml")
            text = soup.get_text(separator="\n", strip=True)

            detail = {"url": url}

            # -------------------------------------------------------
            # Hero section (ust kisim) — Dagitim Yontemi, Pazar, Pay
            # css-xmbsbf (label), css-3gmn66 (value)
            # -------------------------------------------------------
            hero_labels = soup.find_all("p", class_=re.compile(r"css-xmbsbf"))
            hero_values = soup.find_all("p", class_=re.compile(r"css-3gmn66"))

            for hl, hv in zip(hero_labels, hero_values):
                h_label = hl.get_text(strip=True).rstrip(":").lower()
                h_value = hv.get_text(strip=True)
                if not h_label or not h_value:
                    continue

                if "dağıtım" in h_label or "dagitim" in h_label:
                    detail["distribution_method"] = self._normalize_distribution(h_value)
                elif "pazar" in h_label:
                    detail["market_segment"] = self._normalize_market(h_value)

            # -------------------------------------------------------
            # Detay tablosu — key-value ciftleri
            # css-1s4g5fq (label), css-12ghvue (value)
            # -------------------------------------------------------
            labels = soup.find_all("p", class_=re.compile(r"css-1s4g5fq"))
            values = soup.find_all("p", class_=re.compile(r"css-12ghvue"))

            kv_pairs = {}
            for label_el, value_el in zip(labels, values):
                label = label_el.get_text(strip=True).rstrip(":")
                value = value_el.get_text(strip=True)
                if label and value:
                    kv_pairs[label.lower()] = value

            # Ticker / Borsa Kodu
            for key in ["borsa kodu", "bist kodu"]:
                if key in kv_pairs:
                    ticker = re.sub(r"[^A-Z]", "", kv_pairs[key].upper())
                    if 3 <= len(ticker) <= 10:
                        detail["ticker"] = ticker
                    break

            # Sirket adi
            if "şirket adı" in kv_pairs:
                detail["company_name"] = kv_pairs["şirket adı"]

            # Fiyat
            for key in ["halka arz fiyatı", "fiyat"]:
                if key in kv_pairs:
                    price = self._parse_price(kv_pairs[key])
                    if price:
                        detail["ipo_price"] = price
                    break

            # Dagitim yontemi (hero'dan gelmediyse detay tablosundan al)
            if "distribution_method" not in detail and "dağıtım yöntemi" in kv_pairs:
                detail["distribution_method"] = self._normalize_distribution(kv_pairs["dağıtım yöntemi"])

            # Pazar (hero'dan gelmediyse detay tablosundan al)
            if "market_segment" not in detail:
                for key in ["borsada işlem göreceği pazar", "pazar"]:
                    if key in kv_pairs:
                        detail["market_segment"] = self._normalize_market(kv_pairs[key])
                        break

            # Toplam lot
            if "toplam lot sayısı" in kv_pairs:
                lots = self._parse_number(kv_pairs["toplam lot sayısı"])
                if lots:
                    detail["total_lots"] = lots

            # Halka arz buyuklugu
            if "halka arz büyüklüğü" in kv_pairs:
                detail["offering_size_raw"] = kv_pairs["halka arz büyüklüğü"]

            # Halka aciklik orani
            if "halka açıklık oranı" in kv_pairs:
                pct = self._parse_pct(kv_pairs["halka açıklık oranı"])
                if pct:
                    detail["public_float_pct"] = pct

            # Talep toplama tarihleri
            if "talep toplama tarihleri" in kv_pairs:
                start, end = self._parse_date_range(kv_pairs["talep toplama tarihleri"])
                if start:
                    detail["subscription_start"] = start
                if end:
                    detail["subscription_end"] = end

            # Halka arz yontemi (sermaye artirimi / ortak satisi)
            if "halka arz yöntemi" in kv_pairs:
                detail["ipo_method"] = kv_pairs["halka arz yöntemi"]

            # Fiyat istikrari
            if "fiyat istikrarı işlemleri" in kv_pairs:
                stability = kv_pairs["fiyat istikrarı işlemleri"]
                days_match = re.search(r"(\d+)", stability)
                if days_match:
                    detail["price_stability_days"] = int(days_match.group(1))

            # Katilim endeksi
            if "katılım endeksi" in kv_pairs:
                raw = kv_pairs["katılım endeksi"].lower()
                detail["katilim_endeksi"] = "uygun" if "uygun" in raw and "değil" not in raw else "uygun_degil"

            # -------------------------------------------------------
            # LOT TAHMINI — "500.000 Kişi Katılırsa (lot) : X"
            # -------------------------------------------------------
            lot_500k = self._extract_lot_estimate(text, 500_000)
            if lot_500k is not None:
                detail["estimated_lots_per_person"] = lot_500k

            # Diger lot tahminleri de kaydet (referans icin)
            lot_estimates = {}
            for threshold in [100_000, 150_000, 200_000, 250_000, 300_000, 350_000, 500_000]:
                lot = self._extract_lot_estimate(text, threshold)
                if lot is not None:
                    lot_estimates[threshold] = lot
            if lot_estimates:
                detail["lot_estimates"] = lot_estimates

            # -------------------------------------------------------
            # Tahsisat Gruplari (bireysel %, kurumsal % vs.)
            # -------------------------------------------------------
            allocations = self._extract_allocations(text)
            if allocations:
                detail["allocations"] = allocations

            # Sermaye artirimi / ortak satisi detay
            self._extract_offering_details(text, detail)

            return detail

        except Exception as e:
            logger.error("Gedik detay sayfasi hatasi (%s): %s", url, e)
            return None

    # -------------------------------------------------------
    # Parse Yardimcilari
    # -------------------------------------------------------

    def _find_parent_link(self, element) -> Optional[object]:
        """Elementin parent <a> tagini bulur."""
        el = element
        for _ in range(10):
            el = el.parent
            if el is None:
                return None
            if el.name == "a":
                return el
        return None

    def _map_status(self, raw: str) -> str:
        """Gedik durum metnini standart status'e cevirir."""
        raw_lower = raw.lower()
        if "aktif" in raw_lower or "talep" in raw_lower:
            return "in_distribution"
        elif "yakında" in raw_lower:
            return "newly_approved"
        elif "tamaml" in raw_lower:
            return "awaiting_trading"
        elif "işlem" in raw_lower:
            return "trading"
        return "newly_approved"

    def _parse_price(self, raw: str) -> Optional[Decimal]:
        """Fiyat parse — '11,20 TL', '46.00 TL' formatlarini destekler."""
        if not raw or raw.strip() in ("-", ""):
            return None
        try:
            clean = raw.replace("TL", "").replace("tl", "").replace("₺", "").strip()

            # "11,20" → virgul ondalik
            if re.match(r"^\d+,\d{1,2}$", clean):
                return Decimal(clean.replace(",", "."))

            # "46.00" → nokta ondalik
            if re.match(r"^\d+\.\d{1,2}$", clean):
                return Decimal(clean)

            # "1.234,56" → binlik nokta, ondalik virgul
            if "," in clean:
                clean = clean.replace(".", "").replace(",", ".")
                return Decimal(clean)

            # Tam sayi
            clean = clean.replace(".", "")
            return Decimal(clean)
        except Exception:
            return None

    def _parse_number(self, raw: str) -> Optional[int]:
        """Sayi parse — '280.000.000', '60.000.000' formatlarini destekler."""
        if not raw or raw.strip() in ("-", ""):
            return None
        try:
            clean = raw.replace(".", "").replace(",", "").strip()
            return int(clean)
        except Exception:
            return None

    def _parse_pct(self, raw: str) -> Optional[Decimal]:
        """Yuzde parse — '34,88 %', '%26.67' formatlarini destekler."""
        if not raw:
            return None
        try:
            clean = raw.replace("%", "").replace(",", ".").strip()
            return Decimal(clean)
        except Exception:
            return None

    def _parse_date_range(self, raw: str) -> tuple[Optional[date], Optional[date]]:
        """Tarih araligi parse.

        Ornekler:
            '11-12-13 Şubat 2026' → (2026-02-11, 2026-02-13)
            '28-29-30 Ocak 2026' → (2026-01-28, 2026-01-30)
            '7-8-9 Ocak 2026' → (2026-01-07, 2026-01-09)
        """
        if not raw or raw.strip() in ("-", ""):
            return None, None

        try:
            months = {
                "ocak": 1, "şubat": 2, "subat": 2, "mart": 3, "nisan": 4,
                "mayıs": 5, "mayis": 5, "haziran": 6, "temmuz": 7,
                "ağustos": 8, "agustos": 8, "eylül": 9, "eylul": 9,
                "ekim": 10, "kasım": 11, "kasim": 11, "aralık": 12, "aralik": 12,
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
            days = [int(d) for d in days if int(d) <= 31]

            if not days:
                return None, None

            start = date(year, month, days[0])
            end = date(year, month, days[-1]) if len(days) > 1 else start
            return start, end

        except Exception:
            return None, None

    def _normalize_distribution(self, raw: str) -> str:
        """Dagitim yontemi normalizasyonu."""
        raw_lower = raw.lower()
        if "eşit" in raw_lower or "esit" in raw_lower:
            return "esit"
        elif "oransal" in raw_lower:
            return "oransal"
        elif "karma" in raw_lower:
            return "karma"
        return raw.strip()

    def _normalize_market(self, raw: str) -> str:
        """Pazar adini normalize eder."""
        raw_lower = raw.lower()
        if "yıldız" in raw_lower or "yildiz" in raw_lower:
            return "yildiz_pazar"
        elif "ana" in raw_lower:
            return "ana_pazar"
        elif "alt" in raw_lower:
            return "alt_pazar"
        return raw.strip()

    def _extract_lot_estimate(self, text: str, threshold: int) -> Optional[int]:
        """Metinden lot tahminini cikarir.

        Ornek metin: "350.000 Kişi Katılırsa (lot) :\\n320"
        """
        # Threshold'u formatla: 350000 → "350.000" veya "350000"
        formatted = f"{threshold:,}".replace(",", ".")
        formatted_alt = str(threshold)

        for fmt in [formatted, formatted_alt]:
            # Pattern: "350.000 Kişi Katılırsa (lot) : 320"
            pattern = re.escape(fmt) + r"\s*Kişi\s*Katılırsa\s*\(lot\)\s*:?\s*(\d[\d.]*)"
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                lot_str = match.group(1).replace(".", "")
                try:
                    return int(lot_str)
                except ValueError:
                    pass

        return None

    def _extract_allocations(self, text: str) -> list[dict]:
        """Tahsisat gruplarinı cikarir.

        Ornek:
          Yurt İçi Bireysel Yatırımcılar: %40
          Yüksek Başvurulu Yatırımcılar: %10
          Yurt İçi Kurumsal Yatırımcılar: %40
          Yurt Dışı Kurumsal Yatırımcılar: %10
        """
        allocations = []

        patterns = [
            (r"Yurt\s*İçi\s*Bireysel\s*Yat[ıi]r[ıi]mc[ıi]lar[ıi]?\s*:?\s*%\s*(\d+)", "bireysel"),
            (r"Yüksek\s*Başvurulu\s*Yat[ıi]r[ıi]mc[ıi]lar\s*:?\s*%\s*(\d+)", "yuksek_basvurulu"),
            (r"Yurt\s*İçi\s*Kurumsal\s*Yat[ıi]r[ıi]mc[ıi]lar[ıi]?\s*:?\s*%\s*(\d+)", "kurumsal_yurtici"),
            (r"Yurt\s*Dışı\s*Kurumsal\s*Yat[ıi]r[ıi]mc[ıi]lar[ıi]?\s*:?\s*%\s*(\d+)", "kurumsal_yurtdisi"),
        ]

        for pattern, group_name in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    pct = Decimal(match.group(1))
                    allocations.append({
                        "group_name": group_name,
                        "allocation_pct": pct,
                    })
                except Exception:
                    pass

        return allocations

    def _extract_offering_details(self, text: str, detail: dict):
        """Sermaye artirimi / ortak satisi tutarlarini cikarir."""
        # Sermaye Artırımı: 240.000.000 TL
        match = re.search(r"Sermaye\s*Art[ıi]r[ıi]m[ıi]\s*:?\s*([\d.]+)\s*TL", text, re.IGNORECASE)
        if match:
            val = self._parse_number(match.group(1))
            if val:
                detail["capital_increase_tl"] = val

        # Ortak Satışı: 40.000.000 TL
        match = re.search(r"Ortak\s*Sat[ıi][şs][ıi]\s*:?\s*([\d.]+)\s*TL", text, re.IGNORECASE)
        if match:
            val = self._parse_number(match.group(1))
            if val:
                detail["partner_sale_tl"] = val


# -------------------------------------------------------
# Scheduler Entrypoint
# -------------------------------------------------------

async def scrape_gedik():
    """Scheduler tarafindan cagirilir — Gedik Yatirim'dan halka arz bilgisi ceker.

    1. Liste sayfasindan temel bilgileri ceker (ticker, fiyat, tarih)
    2. Her halka arz icin detay sayfasindan zengin bilgi ceker
       - Dagitim yontemi, pazar, lot tahmini (350K kisi), tahsisat gruplari
    3. Veritabanini gunceller (mevcut bilgileri override etmez)
    """
    from app.database import async_session
    from app.services.ipo_service import IPOService

    scraper = GedikScraper()
    try:
        # 1. Liste sayfasi
        ipos = await scraper.fetch_ipo_list()
        if not ipos:
            logger.info("Gedik: Halka arz bulunamadi")
            return

        async with async_session() as db:
            ipo_service = IPOService(db)
            updated_count = 0

            for ipo_data in ipos:
                ticker = ipo_data.get("ticker")
                company_name = ipo_data.get("company_name")
                if not company_name and not ticker:
                    continue

                # 2. Detay sayfasindan ek bilgi cek
                detail_url = ipo_data.get("detail_url")
                detail = None
                if detail_url:
                    detail = await scraper.fetch_ipo_detail(detail_url)

                # 3. Guncelleme verisi olustur
                update_data = {}
                if company_name:
                    update_data["company_name"] = company_name
                if ticker:
                    update_data["ticker"] = ticker

                # Liste verileri
                for field in ["ipo_price", "subscription_start", "subscription_end"]:
                    if ipo_data.get(field) is not None:
                        update_data[field] = ipo_data[field]

                # Detay verileri (varsa)
                if detail:
                    for field in ["distribution_method", "market_segment", "total_lots",
                                  "public_float_pct", "price_stability_days",
                                  "estimated_lots_per_person"]:
                        if detail.get(field) is not None:
                            update_data[field] = detail[field]

                    # Eger listede fiyat yoksa detaydan al
                    if "ipo_price" not in update_data and detail.get("ipo_price"):
                        update_data["ipo_price"] = detail["ipo_price"]

                if len(update_data) > 1:  # company_name disinda en az 1 alan
                    await ipo_service.create_or_update_ipo(update_data)
                    updated_count += 1

            await db.commit()
            logger.info("Gedik: %d halka arz guncellendi", updated_count)

    except Exception as e:
        logger.error("Gedik scraper hatasi: %s", e)
    finally:
        await scraper.close()
