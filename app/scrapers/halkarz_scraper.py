"""HalkArz.com Scraper v2 — Detay Sayfasi Odakli.

Calisma Mantigi:
1. WordPress REST API ile tum halka arz postlarini al (slug + link)
2. DB'deki aktif IPO'larla fuzzy matching yap
3. Eslesen her IPO icin detay sayfasina git
4. table.sp-table'dan temel bilgileri cek
5. table.as-table'dan sonuc verilerini cek
6. H5 basliklarindan alt bolum bilgilerini cek
7. Admin korumasi: manual_fields'ta listelenen alanlar ATLANIR

Kaynak: https://halkarz.com
"""

import json
import re
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

import httpx
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

HALKARZ_BASE = "https://halkarz.com"
HALKARZ_WP_API = f"{HALKARZ_BASE}/wp-json/wp/v2"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

# Turk ay isimleri
TR_MONTHS = {
    "ocak": 1, "şubat": 2, "subat": 2, "mart": 3, "nisan": 4,
    "mayıs": 5, "mayis": 5, "haziran": 6, "temmuz": 7,
    "ağustos": 8, "agustos": 8, "eylül": 9, "eylul": 9,
    "ekim": 10, "kasım": 11, "kasim": 11, "aralık": 12, "aralik": 12,
}


# ============================================================
# HTML DETAIL PAGE PARSER
# ============================================================

class HalkArzDetailParser:
    """HalkArz.com detay sayfasindan veri cikarir.

    Sayfa Yapisi:
    - h2.il-bist-kod → Ticker (EMPAE)
    - h1.il-halka-arz-sirket → Sirket Adi
    - table.sp-table → Temel bilgiler (label:value satirlari)
    - table.as-table → Halka arz sonuclari (varsa)
    - table.fs-extra → Finansal tablo
    - h5 basliklar → Alt bolumler (Halka Arz Sekli, Tahsisat, vs.)
    """

    def __init__(self, html: str, url: str):
        self.soup = BeautifulSoup(html, "lxml")
        self.url = url
        self.data: dict = {"source_url": url}

    def parse(self) -> dict:
        """Tum veriyi cikarir ve doner."""
        self._parse_header()
        self._parse_main_table()
        self._parse_results_table()
        # Finansal tablo (fs-extra) kasitli olarak atlanıyor — kullanilmiyor.
        self._parse_sections()
        self._parse_pdf_links()
        self._parse_brokers()
        return self.data

    # --- BOLUM 1: Header ---
    def _parse_header(self):
        """h2.il-bist-kod ve h1.il-halka-arz-sirket'ten ticker ve isim."""
        ticker_el = self.soup.select_one("h2.il-bist-kod")
        if ticker_el:
            ticker = ticker_el.get_text(strip=True)
            ticker = re.sub(r"[^A-Z]", "", ticker.upper())
            if 3 <= len(ticker) <= 10:
                self.data["ticker"] = ticker

        name_el = self.soup.select_one("h1.il-halka-arz-sirket")
        if name_el:
            self.data["company_name"] = name_el.get_text(strip=True)

    # --- BOLUM 2: Temel Bilgiler Tablosu (sp-table) ---
    def _parse_main_table(self):
        """table.sp-table → key:value satirlari."""
        table = self.soup.select_one("table.sp-table")
        if not table:
            return

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            label = cells[0].get_text(strip=True).lower().rstrip(" :")
            value = cells[1].get_text(strip=True)

            if not label or not value:
                continue

            # Halka Arz Tarihi — "19-20 Şubat 2026  09:00-17:00"
            if "halka arz tarihi" in label:
                dates = self._parse_date_range(value)
                if dates.get("start"):
                    self.data["subscription_start"] = dates["start"]
                if dates.get("end"):
                    self.data["subscription_end"] = dates["end"]
                # Saat bilgisi
                hours_match = re.search(r"(\d{2}:\d{2})\s*[-–]\s*(\d{2}:\d{2})", value)
                if hours_match:
                    self.data["subscription_hours"] = f"{hours_match.group(1)}-{hours_match.group(2)}"

            # Fiyat — "22,00 TL"
            elif "fiyat" in label or "aralığ" in label:
                price = self._parse_price(value)
                if price:
                    self.data["ipo_price"] = price

            # Dagitim Yontemi — "Eşit Dağıtım **"
            elif "dağıtım" in label or "dagitim" in label:
                self.data["distribution_method"] = self._normalize_distribution(value)
                self.data["distribution_raw"] = value.rstrip(" *")

            # Pay — "38.000.000 Lot"
            elif label == "pay" or "pay miktarı" in label:
                lots = self._parse_number(value)
                if lots:
                    self.data["total_lots"] = lots

            # Araci Kurum
            elif "aracı kurum" in label or "araci kurum" in label:
                self.data["lead_broker"] = value

            # Fiili Dolasim Pay Orani
            elif "fiili dolaşımdaki pay oranı" in label or "fiili dolasim" in label:
                pct = self._parse_pct(value)
                if pct:
                    self.data["public_float_pct"] = pct

            # Bist Kodu
            elif "bist kodu" in label or "borsa kodu" in label:
                ticker = re.sub(r"[^A-Z]", "", value.upper())
                if 3 <= len(ticker) <= 10:
                    self.data["ticker"] = ticker

            # Pazar — "Ana Pazar"
            elif "pazar" in label:
                self.data["market_segment"] = self._normalize_market(value)

            # Bist Ilk Islem Tarihi — "11 Şubat 2026"
            elif "bist" in label and "işlem" in label:
                d = self._parse_single_date(value)
                if d:
                    self.data["trading_start"] = d

    # --- BOLUM 3: Sonuc Tablosu (as-table) ---
    def _parse_results_table(self):
        """table.as-table → Halka arz sonuclari (dagitim sonuclari).

        Tablo yapisi: Grup | Kisi Sayisi | Lot Miktari
        Satirlar: Yurt Ici Bireysel, Yuksek Basvurulu, Kurumsal Yurt Ici,
                  Kurumsal Yurt Disi, Toplam
        """
        table = self.soup.select_one("table.as-table")
        if not table:
            return

        self.data["has_results"] = True
        self.data["allocation_groups"] = []  # Grup bazli sonuclar
        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            label = cells[0].get_text(strip=True).lower()
            kisi = self._parse_number(cells[1].get_text(strip=True))
            lot = self._parse_number(cells[2].get_text(strip=True))

            # Oran (varsa — 4. sutun)
            oran = None
            if len(cells) >= 4:
                oran = self._parse_pct(cells[3].get_text(strip=True))

            # Yurt Ici Bireysel
            if "yurt içi bireysel" in label or "yurt ici bireysel" in label or ("bireysel" in label and "yüksek" not in label):
                self.data["result_bireysel_kisi"] = kisi
                self.data["result_bireysel_lot"] = lot
                self.data["allocation_groups"].append({
                    "group": "bireysel",
                    "participant_count": kisi,
                    "allocated_lots": lot,
                    "allocation_pct": oran,
                })

            # Yuksek Basvurulu Bireysel
            elif "yüksek başvurulu" in label or "yuksek basvurulu" in label or "yüksek" in label:
                self.data["allocation_groups"].append({
                    "group": "yuksek_basvurulu",
                    "participant_count": kisi,
                    "allocated_lots": lot,
                    "allocation_pct": oran,
                })

            # Kurumsal Yurt Ici
            elif "kurumsal" in label and ("yurt içi" in label or "yurt ici" in label or "yurtiçi" in label or ("yurt" not in label and "dış" not in label and "disi" not in label)):
                # "Yurt İçi Kurumsal" veya sadece "Kurumsal" (yurtdisi olmayan)
                self.data["allocation_groups"].append({
                    "group": "kurumsal_yurtici",
                    "participant_count": kisi,
                    "allocated_lots": lot,
                    "allocation_pct": oran,
                })

            # Kurumsal Yurt Disi
            elif "kurumsal" in label and ("yurt dışı" in label or "yurt disi" in label or "yurtdışı" in label or "dış" in label):
                self.data["allocation_groups"].append({
                    "group": "kurumsal_yurtdisi",
                    "participant_count": kisi,
                    "allocated_lots": lot,
                    "allocation_pct": oran,
                })

            # Toplam
            elif "toplam" in label:
                self.data["total_applicants"] = kisi
                self.data["result_toplam_lot"] = lot

    # --- BOLUM 4: Finansal Tablo (fs-extra) ---
    def _parse_financial_table(self):
        """table.fs-extra → Hasilat ve Brut Kar."""
        table = self.soup.select_one("table.fs-extra")
        if not table:
            return

        rows = table.find_all("tr")
        if len(rows) < 2:
            return

        # Header: Finansal Tablo | 2025/9 | 2024 | 2023
        # Row 1: Hasilat | x | y | z
        # Row 2: Brut Kar | x | y | z
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).lower()
            value = cells[1].get_text(strip=True)  # En guncel deger

            if "hasılat" in label or "hasilat" in label:
                self.data["revenue_current_year"] = self._parse_financial_value(value)
            elif "brüt" in label or "brut" in label:
                self.data["gross_profit"] = self._parse_financial_value(value)

    # --- BOLUM 5: Alt Bolumler (h5 baslikli) ---
    def _parse_sections(self):
        """h5 basliklarindan: Halka Arz Sekli, Tahsisat, Lot Tahmini, vs."""
        body_text = self.soup.get_text(separator="\n", strip=True)

        # Katilim Endeksi
        katilim_match = re.search(
            r"Katılım\s+Endeks[ieİ]ne\s+(uygun|uygun\s+değil)",
            body_text, re.IGNORECASE
        )
        if katilim_match:
            is_uygun = "uygun" in katilim_match.group(1).lower() and "değil" not in katilim_match.group(1).lower()
            self.data["katilim_endeksi"] = "uygun" if is_uygun else "uygun_degil"

        # Halka Aciklik
        aciklik_match = re.search(r"Halka\s+Açıklık\s*[-–:]?\s*[%]?([\d,]+)", body_text, re.IGNORECASE)
        if aciklik_match:
            pct = self._parse_pct("%" + aciklik_match.group(1))
            if pct:
                self.data["public_float_pct"] = pct

        # Iskonto
        iskonto_match = re.search(r"[İi]skonto\w*\s*[-–:]?\s*[%]?([\d,]+)", body_text, re.IGNORECASE)
        if iskonto_match:
            pct = self._parse_pct("%" + iskonto_match.group(1))
            if pct:
                self.data["discount_pct"] = pct

        # Fiyat Istikrari suresi
        fiyat_match = re.search(r"Fiyat\s+İstikrar[ıi]\s*[-–:]?\s*(\d+)\s*gün", body_text, re.IGNORECASE)
        if fiyat_match:
            self.data["price_stability_days"] = int(fiyat_match.group(1))

        # Satmama Taahhut (lock-up)
        lockup_match = re.search(r"Satmama\s+Taahhüd[üu]\s*.*?(\d+)\s*(?:Yıl|yıl|Ay|ay)", body_text, re.IGNORECASE)
        if lockup_match:
            val = int(lockup_match.group(1))
            if "yıl" in body_text[lockup_match.start():lockup_match.end() + 10].lower():
                self.data["lock_up_period_days"] = val * 365
            else:
                self.data["lock_up_period_days"] = val * 30

        # Sermaye Artirimi / Ortak Satisi lot miktarlari
        sa_match = re.search(r"Sermaye\s+Artırımı\s*:\s*([\d.]+)\s*Lot", body_text, re.IGNORECASE)
        if sa_match:
            self.data["capital_increase_lots"] = self._parse_number(sa_match.group(1))

        os_lots = re.findall(r"Ortak\s+Satışı\s*:\s*([\d.]+)\s*Lot", body_text, re.IGNORECASE)
        if os_lots:
            total_partner = sum(self._parse_number(x) or 0 for x in os_lots)
            if total_partner > 0:
                self.data["partner_sale_lots"] = total_partner

        # Dagitilacak Pay Miktari — lot tahminleri
        lot_estimates = {}
        for m in re.finditer(
            r"([\d.,]+)\s*(?:Bin|Milyon)\s*katılım\s*[~≈→-]+\s*(\d+)\s*Lot",
            body_text, re.IGNORECASE
        ):
            threshold = m.group(1).replace(".", "").replace(",", "")
            lots = int(m.group(2))
            lot_estimates[threshold] = lots

        # 500 Bin varsa bunu tahmini lot olarak kullan
        for key in ["500", "500000"]:
            if key in lot_estimates:
                self.data["estimated_lots_per_person"] = lot_estimates[key]
                break

        # Fonun Kullanim Yeri
        fund_usage = []
        for m in re.finditer(r"-\s*%(\d+)\s+(.+?)(?:\n|$)", body_text):
            fund_usage.append(f"%{m.group(1)} {m.group(2).strip()}")
        if fund_usage:
            self.data["fund_usage"] = json.dumps(fund_usage, ensure_ascii=False)

        # Sirket Hakkinda — accordion icinden en uzun <p> paragrafini al
        # (ilk <p> bazen sadece sirket adi olabiliyor)
        for summary_el in self.soup.find_all("summary", class_="acc-header"):
            if "irket" in summary_el.get_text() and "akkında" in summary_el.get_text():
                acc_body = summary_el.find_next_sibling("div", class_="acc-body")
                if acc_body:
                    best = ""
                    for p in acc_body.find_all("p"):
                        txt = p.get_text(strip=True)
                        if len(txt) > len(best):
                            best = txt
                    if len(best) > 50:
                        self.data["company_description"] = best
                break

    # --- PDF Linkleri ---
    def _parse_pdf_links(self):
        """Izahname ve diger PDF linklerini cikar."""
        for a in self.soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            if href.endswith(".pdf") or "izahname" in text or "prospekt" in text:
                full_url = href if href.startswith("http") else HALKARZ_BASE + href
                self.data["prospectus_url"] = full_url
                break

    # --- Basvuru Yerleri (Sadece REJECTED broker listesi) ---
    def _parse_brokers(self):
        """details.acc > Basvuru Yerleri → SADECE basvurulamaz broker'lari topla.

        Amac: Kullaniciya "buradan basvuramazsin" bilgisi vermek.
        Temiz (allowed) broker'lar kaydedilMEZ — sadece rejected olanlar saklanir.

        Rejected tespiti:
        - class="unlist" → rejected
        - <s> etiketi icinde → rejected
        - style="text-decoration: line-through" → rejected
        - class="cross" veya icinde <i> carpi ikonu → rejected
        """
        rejected_list: list[dict] = []

        for details in self.soup.find_all("details", class_="acc"):
            summary = details.find("summary")
            if not summary or "başvuru" not in summary.get_text(strip=True).lower():
                continue

            for li in details.find_all("li"):
                name = li.get_text(strip=True)

                # Placeholder metinleri atla
                if not name or name.startswith("*") or "tamamlanacak" in name.lower():
                    continue

                cls = li.get("class", [])
                if isinstance(cls, list):
                    cls_str = " ".join(cls)
                else:
                    cls_str = str(cls)

                style = li.get("style", "")
                has_s_tag = li.find("s") is not None
                has_cross_icon = li.find("i", class_=lambda c: c and "times" in c) is not None

                # Rejected kontrol: class=unlist, <s>, line-through, cross icon
                is_rejected = (
                    "unlist" in cls_str
                    or "cross" in cls_str
                    or has_s_tag
                    or "line-through" in style
                    or has_cross_icon
                )

                if not is_rejected:
                    continue  # Temiz broker'lari kaydetmiyoruz

                # Broker tipi tespit
                broker_type = "araci_kurum"
                if any(kw in name.lower() for kw in ["bank", "banka"]):
                    broker_type = "banka"

                rejected_list.append({
                    "name": name,
                    "type": broker_type,
                    "is_rejected": True,
                })

            break  # Sadece ilk Basvuru Yerleri accordion'u

        if rejected_list:
            self.data["brokers_rejected"] = rejected_list
            logger.info(
                "Basvuru Yerleri: %d basvurulamaz broker tespit edildi",
                len(rejected_list),
            )

    # ============================================================
    # PARSE YARDIMCILARI
    # ============================================================

    def _parse_date_range(self, text: str) -> dict:
        """'19-20 Şubat 2026' veya '5-6 Şubat 2026' formatini parse eder."""
        result = {}

        # Ay ve yil bul
        month = None
        year = None
        for month_name, month_num in TR_MONTHS.items():
            if month_name in text.lower():
                month = month_num
                break

        year_match = re.search(r"(20\d{2})", text)
        if year_match:
            year = int(year_match.group(1))

        if not month or not year:
            # Fallback: DD/MM/YYYY formati
            dates = re.findall(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
            if dates:
                try:
                    result["start"] = date(int(dates[0][2]), int(dates[0][1]), int(dates[0][0]))
                except ValueError:
                    pass
                if len(dates) > 1:
                    try:
                        result["end"] = date(int(dates[-1][2]), int(dates[-1][1]), int(dates[-1][0]))
                    except ValueError:
                        pass
            return result

        # Gunleri bul: "19-20" veya "5-6-7"
        days = re.findall(r"\b(\d{1,2})\b", text.split(str(year))[0] if str(year) in text else text)
        days = [int(d) for d in days if 1 <= int(d) <= 31]

        if days:
            try:
                result["start"] = date(year, month, min(days))
                result["end"] = date(year, month, max(days))
            except ValueError:
                pass

        return result

    def _parse_single_date(self, text: str) -> date | None:
        """'11 Şubat 2026' formatini parse eder."""
        for month_name, month_num in TR_MONTHS.items():
            if month_name in text.lower():
                day_match = re.search(r"(\d{1,2})", text)
                year_match = re.search(r"(20\d{2})", text)
                if day_match and year_match:
                    try:
                        return date(int(year_match.group(1)), month_num, int(day_match.group(1)))
                    except ValueError:
                        pass
        # Fallback: DD/MM/YYYY
        m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass
        return None

    def _parse_price(self, text: str) -> Decimal | None:
        """'22,00 TL' veya '14.70 TL' formatini parse eder."""
        cleaned = re.sub(r"[^\d.,]", "", text)
        if not cleaned:
            return None
        # Turk formati: virgul ondalik ayirici
        if "," in cleaned and "." in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        elif "," in cleaned:
            cleaned = cleaned.replace(",", ".")
        try:
            val = Decimal(cleaned)
            if val > 0:
                return val
        except (InvalidOperation, ValueError):
            pass
        return None

    def _parse_number(self, text: str) -> int | None:
        """'38.000.000' veya '795.046' formatini parse eder."""
        if not text:
            return None
        cleaned = re.sub(r"[^\d]", "", text)
        try:
            return int(cleaned) if cleaned else None
        except ValueError:
            return None

    def _parse_pct(self, text: str) -> Decimal | None:
        """%28,99 veya %22.35 formatini parse eder."""
        match = re.search(r"[%]?\s*([\d]+[.,]?\d*)", text)
        if match:
            val = match.group(1).replace(",", ".")
            try:
                return Decimal(val)
            except (InvalidOperation, ValueError):
                pass
        return None

    def _parse_financial_value(self, text: str) -> Decimal | None:
        """'2,4 Milyar TL' veya '527,0 Milyon TL' formatini parse eder."""
        multiplier = 1
        if "milyar" in text.lower():
            multiplier = 1_000_000_000
        elif "milyon" in text.lower():
            multiplier = 1_000_000

        match = re.search(r"([\d]+[.,]?\d*)", text)
        if match:
            val = match.group(1).replace(",", ".")
            try:
                return Decimal(val) * multiplier
            except (InvalidOperation, ValueError):
                pass
        return None

    def _normalize_distribution(self, text: str) -> str:
        """Dagitim yontemi normalizasyonu."""
        t = text.lower()
        if "eşit" in t or "esit" in t:
            if "tamamı" in t or "tamami" in t:
                return "tamami_esit"
            if "bireysel" in t:
                return "bireysele_esit"
            return "esit"
        if "oransal" in t:
            return "oransal"
        if "karma" in t:
            return "karma"
        return text.strip().rstrip(" *")

    def _normalize_market(self, text: str) -> str:
        """Pazar segmenti normalizasyonu."""
        t = text.lower()
        if "yıldız" in t or "yildiz" in t:
            return "yildiz_pazar"
        if "alt" in t:
            return "alt_pazar"
        if "ana" in t:
            return "ana_pazar"
        return text.strip()


# ============================================================
# MAIN SCRAPER ORCHESTRATOR
# ============================================================

class HalkArzScraper:
    """HalkArz.com scraper — WP API ile liste, detay sayfasi ile veri."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0, headers=HEADERS, follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    async def fetch_all_posts(self) -> list[dict]:
        """WordPress REST API ile tum halka arz postlarini getirir."""
        all_posts = []
        page = 1
        while page <= 5:  # Max 5 sayfa (250 post)
            try:
                resp = await self.client.get(
                    f"{HALKARZ_WP_API}/posts",
                    params={
                        "per_page": 50,
                        "page": page,
                        "_fields": "id,slug,title,link",
                    },
                )
                if resp.status_code != 200:
                    break

                posts = resp.json()
                if not isinstance(posts, list) or not posts:
                    break

                for post in posts:
                    all_posts.append({
                        "wp_id": post.get("id"),
                        "slug": post.get("slug", ""),
                        "title": post.get("title", {}).get("rendered", ""),
                        "link": post.get("link", ""),
                    })

                page += 1
            except Exception as e:
                logger.warning("HalkArz WP API sayfa %d hatasi: %s", page, e)
                break

        logger.info("HalkArz WP API: %d post bulundu", len(all_posts))
        return all_posts

    async def fetch_detail_page(self, url: str) -> dict | None:
        """Detay sayfasini indirip parse eder."""
        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                logger.warning("HalkArz detay %d: %s", resp.status_code, url)
                return None

            parser = HalkArzDetailParser(resp.text, url)
            return parser.parse()

        except Exception as e:
            logger.error("HalkArz detay hatasi (%s): %s", url, e)
            return None

    def match_post_to_ipo(self, post_title: str, ipo_name: str) -> bool:
        """Fuzzy matching: DB'deki IPO ismi ile site post basligi eslesir mi?"""
        # Normalize
        def normalize(s):
            s = s.lower().strip()
            # A.Ş., A.S., San., Tic. gibi kisaltmalari sil
            s = re.sub(r"\b(a\.ş\.|a\.s\.|san\.|tic\.|ve |ltd\.|şti\.|dış |iç )", "", s)
            s = re.sub(r"[^\w\s]", "", s)
            s = re.sub(r"\s+", " ", s).strip()
            return s

        norm_post = normalize(post_title)
        norm_ipo = normalize(ipo_name)

        # 1. Tam eslesme
        if norm_post == norm_ipo:
            return True

        # 2. Biri digerini icerir
        if norm_ipo in norm_post or norm_post in norm_ipo:
            return True

        # 3. Ilk kelime eslesme (en az 4 karakter)
        first_word_post = norm_post.split()[0] if norm_post.split() else ""
        first_word_ipo = norm_ipo.split()[0] if norm_ipo.split() else ""

        if len(first_word_post) >= 4 and first_word_post == first_word_ipo:
            return True

        # 4. Ilk 2 kelime eslesme
        words_post = norm_post.split()[:2]
        words_ipo = norm_ipo.split()[:2]
        if len(words_post) >= 2 and words_post == words_ipo:
            return True

        return False


# ============================================================
# ADMIN KORUMA YARDIMCISI
# ============================================================

def get_manual_fields(ipo) -> set:
    """IPO'nun manual_fields JSON'indan admin kilitli alanlari dondurur."""
    if not ipo.manual_fields:
        return set()
    try:
        fields = json.loads(ipo.manual_fields)
        return set(fields) if isinstance(fields, list) else set()
    except (json.JSONDecodeError, TypeError):
        return set()


def filter_scraper_data(scraped: dict, manual_fields: set) -> dict:
    """Admin kilitli alanlari scraped data'dan cikarir."""
    if not manual_fields:
        return scraped

    filtered = {}
    for key, value in scraped.items():
        if key in manual_fields:
            logger.debug("Admin kilidi: %s alani atlanıyor", key)
            continue
        filtered[key] = value
    return filtered


# ============================================================
# SCHEDULER ENTRYPOINT
# ============================================================

async def scrape_halkarz():
    """Scheduler tarafindan cagirilir — HalkArz.com'dan bilgi ceker.

    Mantik:
    1. WP API'den tum postlari al
    2. DB'deki aktif (arsivlenmemis) IPO'lari al
    3. Her IPO icin eslesen postu bul
    4. Eslesen postun detay sayfasina git → veri cek
    5. Admin kilitli alanlari atla, geri kalani guncelle
    """
    from app.database import async_session
    from app.services.ipo_service import IPOService
    from sqlalchemy import select, and_, delete
    from app.models.ipo import IPO, IPOBroker

    scraper = HalkArzScraper()
    try:
        # 1. WP API'den tum postlari al
        posts = await scraper.fetch_all_posts()
        if not posts:
            logger.warning("HalkArz: Hic post bulunamadi")
            return

        # 2. DB'deki aktif IPO'lari al
        async with async_session() as db:
            ipo_service = IPOService(db)

            # Aktif IPO'lari cek — trading olanlar da dahil (dagitim sonuclari icin)
            # allocation_announced=False olan trading IPO'lari da taranir
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.archived == False,
                        IPO.status.in_(["newly_approved", "in_distribution", "awaiting_trading", "trading"]),
                    )
                )
            )
            active_ipos = result.scalars().all()

            if not active_ipos:
                logger.info("HalkArz: Aktif IPO yok, cikiliyor")
                return

            updated_count = 0

            # 3. Her IPO icin eslesen postu bul
            for ipo in active_ipos:
                matched_post = None

                for post in posts:
                    if scraper.match_post_to_ipo(post["title"], ipo.company_name):
                        matched_post = post
                        break

                if not matched_post:
                    logger.debug("HalkArz: %s icin eslesen post bulunamadi", ipo.company_name)
                    continue

                # 4. Detay sayfasina git
                detail = await scraper.fetch_detail_page(matched_post["link"])
                if not detail:
                    continue

                # 5. Admin kilitli alanlari atla
                manual = get_manual_fields(ipo)
                safe_data = filter_scraper_data(detail, manual)

                # 6. Veritabanini guncelle
                update_fields = {}
                field_mapping = {
                    "ticker": "ticker",
                    "ipo_price": "ipo_price",
                    "subscription_start": "subscription_start",
                    "subscription_end": "subscription_end",
                    "subscription_hours": "subscription_hours",
                    "trading_start": "trading_start",
                    "total_lots": "total_lots",
                    "lead_broker": "lead_broker",
                    "distribution_method": "distribution_method",
                    "market_segment": "market_segment",
                    "public_float_pct": "public_float_pct",
                    "discount_pct": "discount_pct",
                    "capital_increase_lots": "capital_increase_lots",
                    "partner_sale_lots": "partner_sale_lots",
                    "estimated_lots_per_person": "estimated_lots_per_person",
                    "price_stability_days": "price_stability_days",
                    "lock_up_period_days": "lock_up_period_days",
                    "prospectus_url": "prospectus_url",
                    "total_applicants": "total_applicants",
                    # Finansal veriler (revenue, gross_profit) kasitli olarak atlanıyor.
                    "fund_usage": "fund_usage",
                    "company_description": "company_description",
                    "katilim_endeksi": None,  # DB'de yok, logla
                }

                for scrape_key, db_field in field_mapping.items():
                    if scrape_key in safe_data and safe_data[scrape_key] is not None:
                        if db_field is None:
                            continue
                        current_val = getattr(ipo, db_field, None)
                        new_val = safe_data[scrape_key]

                        # Sadece bos alanlari doldur VEYA guncelleme varsa yaz
                        if current_val is None or current_val != new_val:
                            setattr(ipo, db_field, new_val)
                            update_fields[db_field] = new_val

                if update_fields:
                    ipo.updated_at = datetime.utcnow()
                    updated_count += 1
                    logger.info(
                        "HalkArz: %s guncellendi — %s",
                        ipo.ticker or ipo.company_name,
                        list(update_fields.keys()),
                    )

                # 7. Rejected broker listesini senkronize et (admin kilidi: "brokers")
                # Sadece basvurulamaz broker'lar kaydedilir (kullaniciya uyari icin)
                if "brokers" not in manual:
                    rejected_brokers = detail.get("brokers_rejected", [])
                    if rejected_brokers:
                        # Mevcut rejected brokerlari sil ve yeniden yaz
                        await db.execute(
                            delete(IPOBroker).where(
                                IPOBroker.ipo_id == ipo.id,
                                IPOBroker.is_rejected == True,
                            )
                        )
                        for b in rejected_brokers:
                            db.add(IPOBroker(
                                ipo_id=ipo.id,
                                broker_name=b["name"],
                                broker_type=b["type"],
                                is_rejected=True,
                            ))
                        logger.info(
                            "HalkArz: %s — %d basvurulamaz broker kaydedildi",
                            ipo.ticker or ipo.company_name,
                            len(rejected_brokers),
                        )

                # 8. Dagitim sonuclari → IPOAllocation tablosuna otomatik kayit
                # has_results varsa ve allocation_announced degilse → sonuclari kaydet
                if detail.get("has_results") and "allocations" not in manual:
                    allocation_groups = detail.get("allocation_groups", [])
                    if allocation_groups and not ipo.allocation_announced:
                        # Onceki scraper kayitlarini temizle (varsa)
                        from app.models.ipo import IPOAllocation
                        await db.execute(
                            delete(IPOAllocation).where(
                                IPOAllocation.ipo_id == ipo.id,
                            )
                        )

                        for grp in allocation_groups:
                            if grp.get("participant_count") or grp.get("allocated_lots"):
                                alloc = IPOAllocation(
                                    ipo_id=ipo.id,
                                    group_name=grp["group"],
                                    participant_count=grp.get("participant_count"),
                                    allocated_lots=grp.get("allocated_lots"),
                                    allocation_pct=grp.get("allocation_pct"),
                                )
                                # Kisi basi ortalama lot hesapla
                                if grp.get("participant_count") and grp.get("allocated_lots") and grp["participant_count"] > 0:
                                    from decimal import Decimal
                                    alloc.avg_lot_per_person = Decimal(str(
                                        round(grp["allocated_lots"] / grp["participant_count"], 2)
                                    ))
                                db.add(alloc)

                        # allocation_announced flag'ini otomatik True yap
                        ipo.allocation_announced = True
                        ipo.updated_at = datetime.utcnow()
                        logger.info(
                            "HalkArz: %s — dagitim sonuclari otomatik kaydedildi (%d grup)",
                            ipo.ticker or ipo.company_name,
                            len(allocation_groups),
                        )

            await db.commit()
            logger.info("HalkArz: %d IPO guncellendi", updated_count)

    except Exception as e:
        logger.error("HalkArz scraper hatasi: %s", e)
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("HalkArz Scraper", str(e))
        except Exception:
            pass
    finally:
        await scraper.close()
