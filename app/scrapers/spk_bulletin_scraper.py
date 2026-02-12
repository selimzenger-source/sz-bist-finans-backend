"""SPK Bulten Scraper — Yeni halka arz onayi tespiti.

SPK her gun mesai sonrasi (genellikle 20:00-05:00 arasi) bulten yayinlar.
Bu scraper SPK bulten sayfasini tarar, yeni bultenleri tespit eder,
icindeki halka arz onaylarini cikarir ve veritabanina kaydeder.

Kaynak: https://spk.gov.tr/spk-bultenleri/{yil}-yili-spk-bultenleri
Calisma Zamani: Her 5 dakikada bir (20:00 — 05:00 arasi)

Bulten Yapisi (HTML):
- Ana sayfada bulten listesi (tarih + link)
- Her bulten bir alt sayfada detayli metin
- "Halka arz" / "izahname" / "kayda alinma" aranarak
  yeni halka arz onayi tespit edilir.
"""

import re
import logging
from datetime import date, datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

SPK_BASE = "https://spk.gov.tr"
SPK_BULLETIN_URL_TEMPLATE = f"{SPK_BASE}/spk-bultenleri/{{year}}-yili-spk-bultenleri"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

# Halka arz tespit anahtar kelimeleri
IPO_KEYWORDS = [
    "halka arz",
    "izahname",
    "kayda al",   # "kayda alınmasına" icin
    "pay satis",
    "pay satış",
    "sermaye piyasasi arac",
    "ihraç belgesi",
    "ihrac belgesi",
    "onay",
]


class SPKBulletinScraper:
    """SPK Bulten scraper — yeni halka arz onayi tespiti."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
            verify=False,  # SPK SSL sertifika sorunu olabiliyor
        )

    async def close(self):
        await self.client.aclose()

    async def fetch_bulletin_list(self, year: int | None = None) -> list[dict]:
        """SPK bulten listesini getirir.

        Returns:
            [{title, url, date_str, bulletin_date}, ...]
        """
        if year is None:
            year = date.today().year

        url = SPK_BULLETIN_URL_TEMPLATE.format(year=year)
        results = []

        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                logger.warning("SPK bulten sayfasi yanitlamadi: %d", resp.status_code)
                return results

            soup = BeautifulSoup(resp.text, "lxml")

            # Bulten linkleri — SPK sayfasinda farkli yapilar olabilir
            # Genellikle bir liste (<ul>/<li>) veya tablo icerisinde
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                text = link.get_text(strip=True)

                # Bulten linki mi kontrol et
                if not text:
                    continue

                # "Bülten" veya "bulten" iceren linkler veya tarih formatli
                is_bulletin = (
                    "bulten" in text.lower()
                    or "bülten" in text.lower()
                    or re.search(r"\d{1,2}[./]\d{1,2}[./]\d{4}", text)
                    or "spk-bultenleri" in href.lower()
                )

                if not is_bulletin:
                    continue

                # URL'yi tam yap
                if href.startswith("/"):
                    href = SPK_BASE + href
                elif not href.startswith("http"):
                    continue

                # Tarih cikarmayi dene
                bulletin_date = self._extract_date(text)

                results.append({
                    "title": text,
                    "url": href,
                    "date_str": text,
                    "bulletin_date": bulletin_date,
                })

            logger.info("SPK: %d bulten linki bulundu", len(results))

        except Exception as e:
            logger.error("SPK bulten listesi hatasi: %s", e)

        return results

    async def fetch_bulletin_content(self, bulletin_url: str) -> str | None:
        """Tek bir bultenin icerigini getirir."""
        try:
            resp = await self.client.get(bulletin_url)
            if resp.status_code != 200:
                logger.warning("SPK bulten icerik alinamadi: %s -> %d", bulletin_url, resp.status_code)
                return None

            soup = BeautifulSoup(resp.text, "lxml")

            # Ana icerik alanini bul
            content = (
                soup.select_one(".content-area")
                or soup.select_one(".page-content")
                or soup.select_one("article")
                or soup.select_one("main")
                or soup.select_one("body")
            )

            if content:
                return content.get_text(separator="\n", strip=True)

            return soup.get_text(separator="\n", strip=True)

        except Exception as e:
            logger.error("SPK bulten icerik hatasi: %s", e)
            return None

    def extract_ipo_approvals(self, bulletin_text: str) -> list[dict]:
        """Bulten metninden halka arz onaylarini cikarir.

        Returns:
            [{company_name, approval_type, detail_text}, ...]
        """
        if not bulletin_text:
            return []

        approvals = []
        text_lower = bulletin_text.lower()

        # Halka arz ile ilgili kelimeler var mi?
        has_ipo_content = any(kw in text_lower for kw in IPO_KEYWORDS)
        if not has_ipo_content:
            return []

        # Paragraf paragraf incele
        paragraphs = bulletin_text.split("\n")
        relevant_sections = []
        current_section = []
        in_relevant = False

        for para in paragraphs:
            para_lower = para.lower().strip()
            if not para_lower:
                if in_relevant and current_section:
                    relevant_sections.append("\n".join(current_section))
                    current_section = []
                    in_relevant = False
                continue

            if any(kw in para_lower for kw in IPO_KEYWORDS):
                in_relevant = True

            if in_relevant:
                current_section.append(para.strip())

        if current_section:
            relevant_sections.append("\n".join(current_section))

        # Her ilgili bolumden sirket adi cikar
        for section in relevant_sections:
            company_name = self._extract_company_name(section)
            approval_type = self._detect_approval_type(section)

            if company_name or approval_type:
                approvals.append({
                    "company_name": company_name or "Bilinmiyor",
                    "approval_type": approval_type,
                    "detail_text": section[:500],
                    "source": "spk_bulten",
                })

        return approvals

    async def check_new_bulletins(self, known_urls: set[str] | None = None) -> list[dict]:
        """Yeni bultenleri kontrol eder ve halka arz onaylarini dondurur.

        Args:
            known_urls: Daha once islenmis bulten URL'leri

        Returns:
            Yeni halka arz onaylari listesi
        """
        if known_urls is None:
            known_urls = set()

        bulletins = await self.fetch_bulletin_list()
        all_approvals = []

        # Sadece bugune ait veya bilinen olmayan bultenleri isle
        today = date.today()

        for bulletin in bulletins:
            url = bulletin["url"]

            # Zaten islenmis mi?
            if url in known_urls:
                continue

            # Bugunun bulteni mi? (tarih yoksa yine de isle)
            b_date = bulletin.get("bulletin_date")
            if b_date and b_date != today:
                continue

            # Icerik cek
            content = await self.fetch_bulletin_content(url)
            if not content:
                continue

            # Halka arz onayi ara
            approvals = self.extract_ipo_approvals(content)
            for approval in approvals:
                approval["bulletin_url"] = url
                approval["bulletin_title"] = bulletin["title"]
                approval["bulletin_date"] = b_date

            all_approvals.extend(approvals)

            logger.info(
                "SPK bulten islendi: %s — %d onay bulundu",
                bulletin["title"][:60], len(approvals)
            )

        return all_approvals

    # -------------------------------------------------------
    # Yardimci Fonksiyonlar
    # -------------------------------------------------------

    def _extract_date(self, text: str) -> date | None:
        """Metinden tarih cikarir (dd.mm.yyyy veya dd/mm/yyyy)."""
        match = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", text)
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

    def _extract_company_name(self, text: str) -> str | None:
        """Metinden sirket adini cikarmayi dener.

        Turkce sirket isimleri genellikle:
        - "... A.S." veya "... A.Ş."
        - Buyuk harflerle basliyor
        """
        patterns = [
            # "XYZ Anonim Sirketi" veya "XYZ A.S."
            r"([A-ZÇĞİÖŞÜ][A-ZÇĞİÖŞÜa-zçğıöşü\s.&]+(?:A\.?[SŞ]\.?|Anonim\s+[ŞS]irketi))",
            # "TICKER Menkul Kiymetler" gibi
            r"([A-ZÇĞİÖŞÜ][A-ZÇĞİÖŞÜa-zçğıöşü\s]+(?:Menkul|Yatirim|Holding|Teknoloji|Enerji|Gida))",
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                name = match.group(1).strip()
                if len(name) > 5:  # Cok kisa isimleri atla
                    return name

        return None

    def _detect_approval_type(self, text: str) -> str:
        """Onay tipini belirle."""
        text_lower = text.lower()

        if "izahname" in text_lower and "onay" in text_lower:
            return "izahname_onay"
        elif "kayda al" in text_lower:
            return "kayda_alma"
        elif "ihraç belgesi" in text_lower or "ihrac belgesi" in text_lower:
            return "ihrac_belgesi"
        elif "halka arz" in text_lower and "onay" in text_lower:
            return "halka_arz_onay"
        elif "pay sat" in text_lower:
            return "pay_satisi"

        return "genel"


# -------------------------------------------------------
# DB-tabanli Bulten Takibi
# -------------------------------------------------------

SCRAPER_STATE_KEY = "spk_processed_bulletin_urls"


async def _get_processed_urls(db) -> set[str]:
    """Veritabanindan islenmis bulten URL'lerini getirir."""
    from sqlalchemy import select
    from app.models.scraper_state import ScraperState
    import json

    result = await db.execute(
        select(ScraperState).where(ScraperState.key == SCRAPER_STATE_KEY)
    )
    state = result.scalar_one_or_none()
    if state and state.value:
        try:
            return set(json.loads(state.value))
        except (json.JSONDecodeError, TypeError):
            return set()
    return set()


async def _save_processed_urls(db, urls: set[str]):
    """Islenmis bulten URL'lerini veritabanina kaydeder."""
    from sqlalchemy import select
    from app.models.scraper_state import ScraperState
    import json

    result = await db.execute(
        select(ScraperState).where(ScraperState.key == SCRAPER_STATE_KEY)
    )
    state = result.scalar_one_or_none()
    if state:
        state.value = json.dumps(list(urls))
        state.updated_at = datetime.utcnow()
    else:
        state = ScraperState(
            key=SCRAPER_STATE_KEY,
            value=json.dumps(list(urls)),
        )
        db.add(state)


# -------------------------------------------------------
# Scheduler Entrypoint
# -------------------------------------------------------

async def check_spk_bulletins():
    """Scheduler tarafindan cagirilir — yeni bultenleri tarar.

    Son islenmis bulten URL'leri veritabaninda saklanir,
    boylece sunucu restart olsa bile tekrar islenmez.
    """
    from app.database import async_session
    from app.services.ipo_service import IPOService
    from app.services.notification import NotificationService

    scraper = SPKBulletinScraper()
    try:
        async with async_session() as db:
            # DB'den islenmis URL'leri al
            processed_urls = await _get_processed_urls(db)

            approvals = await scraper.check_new_bulletins(known_urls=processed_urls)

            if not approvals:
                return

            ipo_service = IPOService(db)
            notif_service = NotificationService(db)

            for approval in approvals:
                # IPO olustur veya guncelle — SPK bulten TEK yetkilendirilmis kaynak
                ipo = await ipo_service.create_or_update_ipo({
                    "company_name": approval["company_name"],
                    "spk_bulletin_url": approval.get("bulletin_url"),
                    "status": "newly_approved",
                }, allow_create=True)

                # Yeni IPO ise bildirim gonder
                if ipo and ipo.created_at and (
                    datetime.utcnow() - ipo.created_at.replace(tzinfo=None)
                ).total_seconds() < 60:
                    await notif_service.notify_new_ipo(ipo)

                    # Admin'e SPK onay bildirimi
                    try:
                        from app.services.admin_telegram import notify_spk_approval
                        await notify_spk_approval(
                            company_name=approval["company_name"],
                            approval_type="SPK Bülten Onayı",
                        )
                    except Exception:
                        pass  # Admin mesaj hatasi ana akisi bozmasin

                # Bu URL'yi islenmis olarak isaretle
                if approval.get("bulletin_url"):
                    processed_urls.add(approval["bulletin_url"])

            # Islenmis URL'leri DB'ye kaydet
            await _save_processed_urls(db, processed_urls)
            await db.commit()

        logger.info("SPK bulten: %d yeni halka arz onayi tespit edildi", len(approvals))

    except Exception as e:
        logger.error("SPK bulten scraper hatasi: %s", e)
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("SPK Bülten Monitor", str(e))
        except Exception:
            pass
    finally:
        await scraper.close()
