"""SPK Bulten Scraper — Yeni halka arz onayi tespiti (PDF tabanli).

SPK her gun mesai sonrasi (genellikle 20:00-05:00 arasi) bulten yayinlar.
Bu scraper SPK bulten sayfasini tarar, SADECE yeni bulteni tespit eder,
PDF'ini indirip parse eder, "Ilk Halka Arzlar" tablosunu okur ve
veritabanina kaydeder.

Calisma mantigi:
1. DB'den son islenmis bulten numarasini oku (orn: 2026/7)
2. SPK sayfasindan mevcut bultenleri listele
3. Yeni bulten varsa (orn: 2026/8) PDF'i indir ve parse et
4. "Ilk Halka Arzlar" tablosundaki sirketleri cikar
5. IPO olustur + Telegram + Tweet gonder
6. Son islenmis numarayi DB'ye kaydet

Yil gecisi: 2026/N -> 2027/1 otomatik desteklenir.

Kaynak: https://spk.gov.tr/spk-bultenleri/{yil}-yili-spk-bultenleri
"""

import io
import re
import json
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import httpx
import pdfplumber
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

# DB state key — son islenmis bulten numarasi
SCRAPER_STATE_KEY = "spk_last_bulletin_no"


# -------------------------------------------------------
# Yardimci: Bulten numarasi parse/karsilastirma
# -------------------------------------------------------

def parse_bulletin_no(text: str) -> tuple[int, int] | None:
    """'2026/8' veya 'Bulten No : 2026/8' gibi text'ten (yil, no) cikarir."""
    m = re.search(r"(\d{4})\s*/\s*(\d+)", text)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return None


def bulletin_no_str(year: int, no: int) -> str:
    """(2026, 8) -> '2026/8'"""
    return f"{year}/{no}"


def is_newer(candidate: tuple[int, int], current: tuple[int, int] | None) -> bool:
    """candidate, current'tan buyuk mu?"""
    if current is None:
        return True
    if candidate[0] > current[0]:
        return True  # yil gecisi
    if candidate[0] == current[0] and candidate[1] > current[1]:
        return True
    return False


# -------------------------------------------------------
# PDF Icerik Okuma
# -------------------------------------------------------

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """PDF bytes'indan tum text'i cikarir (pdfplumber)."""
    text_parts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        logger.error("PDF text cikarma hatasi: %s", e)
    return "\n".join(text_parts)


def extract_tables_from_pdf(pdf_bytes: bytes) -> list[list[list[str]]]:
    """PDF'den tum tablolari cikarir.

    Returns:
        [table1, table2, ...] — her tablo = [[cell, cell, ...], ...]
    """
    all_tables = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                if tables:
                    all_tables.extend(tables)
    except Exception as e:
        logger.error("PDF tablo cikarma hatasi: %s", e)
    return all_tables


# -------------------------------------------------------
# Ilk Halka Arz Tablosu Parse
# -------------------------------------------------------

def _clean_number(val: str | None) -> Decimal | None:
    """'141.000.000' veya '22,00' -> Decimal."""
    if not val:
        return None
    val = val.strip().replace(" ", "")
    if val in ("-", "\u2013", ""):
        return None
    # Turkce format: nokta = binlik, virgul = ondalik
    # 141.000.000 -> 141000000
    # 22,00 -> 22.00
    val = val.replace(".", "").replace(",", ".")
    try:
        return Decimal(val)
    except (InvalidOperation, ValueError):
        return None


def find_ilk_halka_arz_table(tables: list[list[list[str]]], full_text: str) -> list[dict]:
    """'Ilk Halka Arzlar' tablosunu bulup sirketleri cikarir.

    PDF'deki "1. Ilk Halka Arzlar" bolumundeki tabloyu arar.
    Tablo yapisi:
    | Ortaklik | Mevcut Sermaye | Yeni Sermaye | Sermaye Artirimi Bedelli | Bedelsiz |
    | Mevcut Pay Satisi | Ek Pay Satisi | Satis Fiyati |

    Returns:
        [{"company_name", "existing_capital", "new_capital",
          "sale_price"}, ...]
    """
    results = []

    for table in tables:
        if not table or len(table) < 2:
            continue

        # Header satirini bul — "Ortaklik" veya "Mevcut Sermaye" iceren satir
        header_idx = None
        for i, row in enumerate(table):
            row_text = " ".join(str(c or "") for c in row).lower()
            if "ortakl" in row_text and ("sermaye" in row_text or "mevcut" in row_text):
                header_idx = i
                break

        if header_idx is None:
            continue

        # Bu tablo "Ilk Halka Arz" mi kontrol et —
        # header'dan once veya text'te "ilk halka arz" olacak
        context_text = ""
        for i in range(max(0, header_idx - 2), header_idx):
            if i < len(table):
                context_text += " ".join(str(c or "") for c in table[i]) + " "
        context_lower = context_text.lower() + full_text.lower()

        # Header'dan sonraki satirlar = veri
        for row in table[header_idx + 1:]:
            if not row or not any(row):
                continue

            # Ilk sutun = sirket adi
            company_name = str(row[0] or "").strip()
            if not company_name or len(company_name) < 3:
                continue

            # Kisa notlari temizle: "(1)", "(2)" gibi dipnotlar
            company_name = re.sub(r"\s*\(\d+\)\s*$", "", company_name).strip()

            # "Ortaklik" header kelimesini atla
            if "ortakl" in company_name.lower():
                continue

            existing_capital = None
            new_capital = None
            sale_price = None

            if len(row) >= 2:
                existing_capital = _clean_number(str(row[1] or ""))
            if len(row) >= 3:
                new_capital = _clean_number(str(row[2] or ""))

            # Satis fiyati — genellikle son veya sondan bir onceki sutun
            # Turkce tablo: Ortaklik | Mevcut | Yeni | Bedelli | Bedelsiz | Pay Satisi | Ek Pay | Fiyat
            for col_idx in range(len(row) - 1, 2, -1):
                val = _clean_number(str(row[col_idx] or ""))
                if val is not None and val < 10000:  # Satis fiyati < 10.000 TL mantikli
                    sale_price = val
                    break

            results.append({
                "company_name": company_name,
                "existing_capital": existing_capital,
                "new_capital": new_capital,
                "sale_price": sale_price,
                "source": "spk_bulten",
            })

            logger.info(
                "SPK PDF tablo: %s — Mevcut: %s, Yeni: %s, Fiyat: %s",
                company_name, existing_capital, new_capital, sale_price,
            )

    return results


def find_halka_acik_pay_ihraclari(tables: list[list[list[str]]]) -> list[dict]:
    """'Halka Acik Ortakliklarin Pay Ihraclari' tablosundan
    SADECE 'Halka Arz' olarak isaretlenmis satirlari cikarir.
    """
    results = []

    for table in tables:
        if not table or len(table) < 2:
            continue

        header_idx = None
        for i, row in enumerate(table):
            row_text = " ".join(str(c or "") for c in row).lower()
            if "ortakl" in row_text and ("sat" in row_text) and ("tur" in row_text or "tür" in row_text):
                header_idx = i
                break

        if header_idx is None:
            continue

        for row in table[header_idx + 1:]:
            if not row or not any(row):
                continue

            # "Halka Arz" satirlarini ara
            row_text = " ".join(str(c or "") for c in row).lower()
            if "halka" not in row_text:
                continue

            company_name = str(row[0] or "").strip()
            company_name = re.sub(r"\s*\(\d+\)\s*$", "", company_name).strip()
            if not company_name or len(company_name) < 3:
                continue

            existing_capital = _clean_number(str(row[1] or "")) if len(row) >= 2 else None
            new_capital = _clean_number(str(row[2] or "")) if len(row) >= 3 else None

            results.append({
                "company_name": company_name,
                "existing_capital": existing_capital,
                "new_capital": new_capital,
                "approval_type": "halka_acik_pay_ihraci",
                "source": "spk_bulten",
            })

    return results


# -------------------------------------------------------
# SPK Scraper Class
# -------------------------------------------------------

class SPKBulletinScraper:
    """SPK Bulten scraper — numara tabanli, PDF parse."""

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
        """SPK bulten sayfasindan bulten listesini getirir.

        Returns:
            [{bulletin_no: (year, no), title, pdf_url}, ...] — no'ya gore sirali
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

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                text = link.get_text(strip=True)

                if not text:
                    continue

                # Bulten numarasi cikar
                bno = parse_bulletin_no(text)
                if not bno:
                    # href'ten de dene: .../2026-8.pdf
                    bno = parse_bulletin_no(href.replace("-", "/"))
                if not bno:
                    continue

                # URL'yi tam yap
                if href.startswith("/"):
                    href = SPK_BASE + href
                elif not href.startswith("http"):
                    continue

                results.append({
                    "bulletin_no": bno,
                    "title": text,
                    "pdf_url": href,
                })

            # Numara sirasina gore sirala
            results.sort(key=lambda x: (x["bulletin_no"][0], x["bulletin_no"][1]))
            logger.info("SPK: %d bulten listelendi (yil=%d)", len(results), year)

        except Exception as e:
            logger.error("SPK bulten listesi hatasi: %s", e)

        return results

    async def download_pdf(self, pdf_url: str) -> bytes | None:
        """PDF dosyasini indirir."""
        try:
            resp = await self.client.get(pdf_url)
            if resp.status_code != 200:
                logger.warning("SPK PDF indirilemedi: %s -> %d", pdf_url, resp.status_code)
                return None

            content_type = resp.headers.get("content-type", "")
            if "pdf" not in content_type and not pdf_url.endswith(".pdf"):
                logger.warning("SPK: beklenen PDF degil: %s (%s)", pdf_url, content_type)
                return None

            logger.info("SPK PDF indirildi: %s (%d bytes)", pdf_url, len(resp.content))
            return resp.content

        except Exception as e:
            logger.error("SPK PDF indirme hatasi: %s", e)
            return None

    async def process_bulletin(self, pdf_url: str, bulletin_no: tuple[int, int]) -> list[dict]:
        """Tek bir bulteni indir, parse et, halka arz bilgilerini cikar."""
        bno_str = bulletin_no_str(*bulletin_no)

        # 1. PDF indir
        pdf_bytes = await self.download_pdf(pdf_url)
        if not pdf_bytes:
            logger.warning("SPK bulten %s: PDF indirilemedi", bno_str)
            return []

        # 2. PDF'den text + tablo cikar
        full_text = extract_text_from_pdf(pdf_bytes)
        tables = extract_tables_from_pdf(pdf_bytes)

        logger.info(
            "SPK bulten %s parse: %d karakter text, %d tablo",
            bno_str, len(full_text), len(tables),
        )

        if not full_text and not tables:
            logger.warning("SPK bulten %s: PDF bos veya okunamadi", bno_str)
            return []

        # 3. Ilk Halka Arzlar tablosu
        ipo_approvals = find_ilk_halka_arz_table(tables, full_text)

        # 4. Halka Acik Pay Ihraclari tablosundaki "Halka Arz" satirlari
        secondary_approvals = find_halka_acik_pay_ihraclari(tables)

        all_approvals = ipo_approvals + secondary_approvals

        # Meta bilgi ekle
        for approval in all_approvals:
            approval["bulletin_no"] = bno_str
            approval["bulletin_url"] = pdf_url
            if "approval_type" not in approval:
                approval["approval_type"] = "ilk_halka_arz"

        logger.info(
            "SPK bulten %s: %d halka arz onayi tespit edildi",
            bno_str, len(all_approvals),
        )

        return all_approvals


# -------------------------------------------------------
# DB-tabanli Bulten Numara Takibi
# -------------------------------------------------------

async def _get_last_bulletin_no(db) -> tuple[int, int] | None:
    """DB'den son islenmis bulten numarasini getirir."""
    from sqlalchemy import select
    from app.models.scraper_state import ScraperState

    result = await db.execute(
        select(ScraperState).where(ScraperState.key == SCRAPER_STATE_KEY)
    )
    state = result.scalar_one_or_none()
    if state and state.value:
        return parse_bulletin_no(state.value)
    return None


async def _save_last_bulletin_no(db, bulletin_no: tuple[int, int]):
    """Son islenmis bulten numarasini DB'ye kaydeder."""
    from sqlalchemy import select
    from app.models.scraper_state import ScraperState

    bno_str = bulletin_no_str(*bulletin_no)
    result = await db.execute(
        select(ScraperState).where(ScraperState.key == SCRAPER_STATE_KEY)
    )
    state = result.scalar_one_or_none()
    if state:
        state.value = bno_str
        state.updated_at = datetime.utcnow()
    else:
        state = ScraperState(
            key=SCRAPER_STATE_KEY,
            value=bno_str,
        )
        db.add(state)


# -------------------------------------------------------
# Scheduler Entrypoint
# -------------------------------------------------------

async def check_spk_bulletins():
    """Scheduler tarafindan cagirilir — yeni bulteni yakalar.

    Mantik:
    1. DB'den son numarayi oku (orn: 2026/7)
    2. SPK sayfasindan bulten listele
    3. Son numaradan buyuk olanlar = yeni
    4. Her yeni bultenin PDF'ini indir -> parse et -> IPO olustur
    5. Son numarayi guncelle

    Yil gecisi: current year + next year listesi kontrol edilir.
    """
    from app.database import async_session
    from app.services.ipo_service import IPOService
    from app.services.notification import NotificationService

    scraper = SPKBulletinScraper()
    try:
        async with async_session() as db:
            # 1. Son islenmis numarayi al
            last_no = await _get_last_bulletin_no(db)

            # 2. Bulten listesi al (mevcut yil)
            current_year = date.today().year
            bulletins = await scraper.fetch_bulletin_list(year=current_year)

            # Yil gecisi kontrolu
            today = date.today()
            if today.month == 12 and today.day >= 15:
                next_year_bulletins = await scraper.fetch_bulletin_list(year=current_year + 1)
                bulletins.extend(next_year_bulletins)
            if today.month == 1 and today.day <= 15:
                prev_year_bulletins = await scraper.fetch_bulletin_list(year=current_year - 1)
                bulletins.extend(prev_year_bulletins)

            if not bulletins:
                logger.info("SPK Monitor: sayfada hic bulten yok")
                return

            # Ilk calisma — DB'de numara yok → en son bulteni "islenmis" say,
            # boylece SADECE bundan sonra gelecek yeni bulteni yakalar.
            # Ama eger sayfadaki en son bulten bugun yayinlandiysa onu isle.
            if last_no is None:
                max_b = max(bulletins, key=lambda x: (x["bulletin_no"][0], x["bulletin_no"][1]))
                max_no = max_b["bulletin_no"]
                # Son bulteni islenmis olarak kaydet — bir sonraki yeni geldiginde yakalanir
                last_no = (max_no[0], max_no[1] - 1) if max_no[1] > 1 else (max_no[0] - 1, 999)
                await _save_last_bulletin_no(db, last_no)
                await db.commit()
                logger.info(
                    "SPK Monitor: ilk calisma — baseline set: %s (sayfadaki son: %s)",
                    bulletin_no_str(*last_no), bulletin_no_str(*max_no),
                )

            # 3. Yeni bultenleri filtrele
            new_bulletins = [
                b for b in bulletins
                if is_newer(b["bulletin_no"], last_no)
            ]

            if not new_bulletins:
                max_b = max(bulletins, key=lambda x: (x["bulletin_no"][0], x["bulletin_no"][1]))
                logger.info(
                    "SPK Monitor: yeni bulten yok (son: %s, islenmis: %s)",
                    bulletin_no_str(*max_b["bulletin_no"]),
                    bulletin_no_str(*last_no) if last_no else "yok",
                )
                return

            logger.info(
                "SPK Monitor: %d YENI BULTEN! %s",
                len(new_bulletins),
                [bulletin_no_str(*b["bulletin_no"]) for b in new_bulletins],
            )

            # 4. Her yeni bulteni isle
            ipo_service = IPOService(db)
            notif_service = NotificationService(db)
            total_approvals = 0
            highest_no = last_no

            for bulletin in sorted(new_bulletins, key=lambda x: x["bulletin_no"]):
                bno = bulletin["bulletin_no"]
                bno_str_val = bulletin_no_str(*bno)

                approvals = await scraper.process_bulletin(
                    bulletin["pdf_url"], bno,
                )

                for approval in approvals:
                    ipo_data = {
                        "company_name": approval["company_name"],
                        "spk_bulletin_url": approval.get("bulletin_url"),
                        "spk_bulletin_no": approval.get("bulletin_no"),
                        "spk_approval_date": today,
                        "status": "newly_approved",
                    }
                    if approval.get("sale_price"):
                        ipo_data["ipo_price"] = float(approval["sale_price"])

                    ipo = await ipo_service.create_or_update_ipo(
                        ipo_data, allow_create=True,
                    )

                    if ipo and ipo.created_at and (
                        datetime.utcnow() - ipo.created_at.replace(tzinfo=None)
                    ).total_seconds() < 60:
                        await notif_service.notify_new_ipo(ipo)
                        try:
                            from app.services.admin_telegram import notify_spk_approval
                            await notify_spk_approval(
                                company_name=approval["company_name"],
                                approval_type=f"SPK Bulten {bno_str_val}",
                            )
                        except Exception:
                            pass

                    total_approvals += 1

                if highest_no is None or is_newer(bno, highest_no):
                    highest_no = bno

            # 5. Son numarayi DB'ye kaydet
            if highest_no and (last_no is None or is_newer(highest_no, last_no)):
                await _save_last_bulletin_no(db, highest_no)

            await db.commit()

            logger.info(
                "SPK Monitor tamamlandi: %d bulten, %d onay, son=%s",
                len(new_bulletins), total_approvals,
                bulletin_no_str(*highest_no) if highest_no else "yok",
            )

    except Exception as e:
        logger.error("SPK bulten scraper hatasi: %s", e)
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("SPK Bulten Monitor", str(e))
        except Exception:
            pass
    finally:
        await scraper.close()
