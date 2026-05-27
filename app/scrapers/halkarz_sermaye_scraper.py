"""halkarz.com Sermaye Artırımı scraper.

Kaynak: https://halkarz.com/sermaye-artirimi/
3 tablo: Bedelsiz | Bedelli | Tahsisli

Her satır: BIST Kodu+Şirket adı (birleşik), Yüzde%, Tutar, [Rüçhan], YKK,
SPK Onay, Tarih (dağıtım tarihi)

DB upsert: capital_increases (ticker+type unique). source='halkarz' olarak işaretlenir.
Her 5 dakikada bir scheduler tetikler.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select

logger = logging.getLogger(__name__)

URL = "https://halkarz.com/sermaye-artirimi/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.5",
}


# ─── Yardımcı parser'lar ───────────────────────────────────────────────────────

def _parse_ticker_and_company(cell_text: str) -> tuple[Optional[str], Optional[str]]:
    """'ENTRAIc Enterra Yenilenebilir Enerji A.Ş.' → ('ENTRA', 'Iç Enterra...')

    BIST kodu genelde 4-6 büyük harf + rakam, sonrasında şirket ismi başlar.
    """
    if not cell_text:
        return None, None
    cell_text = cell_text.strip()
    # İlk 6 karakter içinde BIST kodu ara (büyük harf serisi)
    m = re.match(r"^([A-Z][A-Z0-9]{2,5})([A-Z][a-zA-ZçğıöşüÇĞİÖŞÜ].*)$", cell_text)
    if m:
        return m.group(1), m.group(2).strip()
    # Tüm cell zaten BIST kodu olabilir
    if cell_text.isupper() and 3 <= len(cell_text) <= 6:
        return cell_text, None
    return None, cell_text


def _parse_pct(s: str) -> Optional[float]:
    """'%53,88' veya '%100' → 53.88 / 100.0"""
    if not s:
        return None
    s = s.replace("%", "").strip()
    s = s.replace(".", "").replace(",", ".")  # Türkçe: 1.234,56 → 1234.56
    try:
        return float(s)
    except ValueError:
        return None


def _parse_tutar(s: str) -> Optional[float]:
    """'1.845.000.000 TL' → 1845000000.0"""
    if not s:
        return None
    s = s.replace("TL", "").replace("tl", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(s: str) -> Optional[date]:
    """'23.09.2025' → date(2025, 9, 23). '...' veya boş → None"""
    if not s:
        return None
    s = s.strip()
    if s in ("...", "-", "—", ""):
        return None
    # Bedelli "Tarih" sütununda "01.06.2026Bitiş : 15.06.2026" gibi birleşik olabilir
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", s)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except (ValueError, IndexError):
        return None


# ─── HTML Parser ────────────────────────────────────────────────────────────────

def parse_halkarz_html(html: str) -> list[dict]:
    """HTML body'sinden tüm sermaye artırımı satırlarını çıkar.

    Returns: list of dict — her dict bir capital_increase kaydı için hazır.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return []

    # Tabloların önündeki başlığa göre tip belirle (Bedelsiz / Bedelli / Tahsisli)
    # halkarz.com bazen tüm başlıkları 'Tahsisli' olarak gösteriyor (CSS bug),
    # bu yüzden tablo sırası ile sabit tip ata.
    # Sayfada sıra: 1) Bedelsiz  2) Bedelli  3) Tahsisli
    type_by_index = {0: "bedelsiz", 1: "bedelli", 2: "tahsisli"}

    records: list[dict] = []
    for tbl_idx, table in enumerate(tables[:3]):  # ilk 3 tablo
        cap_type = type_by_index.get(tbl_idx, "bedelsiz")
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        # Header satırı atla
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            if len(cells) < 5:
                continue

            # Bedelli: 7 sütun (Rüçhan ekli) — diğerleri 6 sütun
            if cap_type == "bedelli" and len(cells) >= 7:
                ticker_company, pct_s, tutar_s, ruchan_s, ykk_s, spk_s, tarih_s = cells[:7]
            else:
                if len(cells) < 6:
                    continue
                ticker_company, pct_s, tutar_s, ykk_s, spk_s, tarih_s = cells[:6]
                ruchan_s = None

            ticker, company = _parse_ticker_and_company(ticker_company)
            if not ticker:
                continue

            pct = _parse_pct(pct_s)
            tutar = _parse_tutar(tutar_s)
            ykk_date = _parse_date(ykk_s)
            spk_date = _parse_date(spk_s)
            dist_date = _parse_date(tarih_s)
            ruchan_price = _parse_tutar(ruchan_s) if ruchan_s else None

            records.append({
                "ticker": ticker,
                "company_name": company,
                "type": cap_type,
                "percentage": pct,
                "amount_tl": tutar,
                "ruchan_price": ruchan_price,
                "ykk_date": ykk_date,
                "spk_approval_date": spk_date,
                "distribution_date": dist_date,
                "source": "halkarz",
                "scraped_at": datetime.now(timezone.utc),
            })

    return records


# ─── HTTP Fetch ────────────────────────────────────────────────────────────────

async def fetch_html() -> Optional[str]:
    """halkarz.com sayfasını indir."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(URL, headers=HEADERS)
            if resp.status_code == 200:
                return resp.text
            logger.warning("halkarz scrape: HTTP %d", resp.status_code)
            return None
    except Exception as e:
        logger.warning("halkarz fetch hata: %s", e)
        return None


# ─── DB Upsert ─────────────────────────────────────────────────────────────────

async def upsert_records(records: list[dict]) -> dict:
    """capital_increases tablosuna upsert et.

    Unique constraint: (ticker, type, ykk_date) — KAP scraper ile aynı.
    Mevcut kayıt varsa EKSİK alanları doldur (KAP state machine'ini bozmaz).
    """
    from app.database import async_session
    from app.models.capital_increase import CapitalIncrease

    inserted = 0
    updated = 0
    skipped = 0

    async with async_session() as db:
        for rec in records:
            ticker = rec["ticker"]
            cap_type = rec["type"]
            ykk_date = rec.get("ykk_date")

            # YKK tarihi yoksa atla (unique key gerekli)
            if not ykk_date:
                skipped += 1
                continue

            # (ticker, type, ykk_date) unique key ile mevcut kayıt ara
            existing_q = select(CapitalIncrease).where(
                CapitalIncrease.ticker == ticker,
                CapitalIncrease.type == cap_type,
                CapitalIncrease.ykk_date == ykk_date,
            )
            existing = (await db.execute(existing_q)).scalar_one_or_none()

            pct = rec.get("percentage")
            amount = rec.get("amount_tl")
            pct_field = f"{cap_type}_pct"

            if existing:
                changed = False
                # Tarihler — KAP doldurmamışsa Halkarz'dan al
                if rec.get("spk_approval_date") and not existing.spk_approval_date:
                    existing.spk_approval_date = rec["spk_approval_date"]; changed = True
                if rec.get("distribution_date") and not existing.distribution_date:
                    existing.distribution_date = rec["distribution_date"]; changed = True
                # Yüzde — boşsa doldur
                if pct is not None and hasattr(existing, pct_field):
                    if getattr(existing, pct_field) is None:
                        setattr(existing, pct_field, pct); changed = True
                # Tutar — boşsa doldur
                if amount is not None and existing.bolunme_sonrasi_sermaye_tl is None:
                    existing.bolunme_sonrasi_sermaye_tl = amount; changed = True
                # Şirket adı boşsa
                if rec.get("company_name") and not existing.company_name:
                    existing.company_name = rec["company_name"]; changed = True
                # Status — yeni evreye geçtiyse update
                if rec.get("distribution_date") and existing.status in ("ykk_alindi", "spk_onayli"):
                    existing.status = "tarih_belli"; changed = True
                elif rec.get("spk_approval_date") and existing.status == "ykk_alindi":
                    existing.status = "spk_onayli"; changed = True

                if changed:
                    existing.updated_at = datetime.now(timezone.utc)
                    updated += 1
                else:
                    skipped += 1
            else:
                new = CapitalIncrease(
                    ticker=ticker,
                    company_name=rec.get("company_name"),
                    type=cap_type,
                    ykk_date=ykk_date,
                )
                if pct is not None and hasattr(new, pct_field):
                    setattr(new, pct_field, pct)
                if amount is not None:
                    new.bolunme_sonrasi_sermaye_tl = amount
                if rec.get("spk_approval_date"):
                    new.spk_approval_date = rec["spk_approval_date"]
                if rec.get("distribution_date"):
                    new.distribution_date = rec["distribution_date"]

                # Status
                if rec.get("distribution_date"):
                    new.status = "tarih_belli"
                elif rec.get("spk_approval_date"):
                    new.status = "spk_onayli"
                else:
                    new.status = "ykk_alindi"

                db.add(new)
                inserted += 1

        await db.commit()

    return {"inserted": inserted, "updated": updated, "skipped": skipped, "total": len(records)}


# ─── Ana Çalıştırıcı ──────────────────────────────────────────────────────────

async def _mark_missing_as_completed(active_keys: set) -> int:
    """Halkarz'da artik olmayan 'tarih_belli' kayitlari 'tamamlandi' olarak isaretle.

    active_keys: bu scrape'te halkarz'da bulunan (ticker, type) tuple seti.
    Halkarz dagitim sonrasi tabloyu temizler — bizdeki kayitlar da bittirilmeli.
    Sadece source='halkarz' veya halkarz tarafindan dokunulmus olanlar etkilenir.
    """
    from app.database import async_session
    from app.models.capital_increase import CapitalIncrease
    from datetime import date as _date, timedelta as _td
    from sqlalchemy import and_ as _and, or_ as _or, update as _upd

    # Sadece distribution_date gectikten 7 gun sonra missing olanlari complete et
    # (Halkarz bazi kayitlari 1-2 gun once kaldirabilir, hemen bittirmeyelim)
    today = _date.today()
    cutoff = today - _td(days=7)

    async with async_session() as db:
        rows = (await db.execute(
            select(CapitalIncrease).where(
                CapitalIncrease.status.in_(["tarih_belli", "spk_onayli", "ykk_alindi"])
            )
        )).scalars().all()

        completed = 0
        for r in rows:
            key = (r.ticker.upper(), r.type)
            if key in active_keys:
                continue  # Halkarz'da hala var, dokunma
            # Dagitim tarihi gectiyse VE son 7 gunden eskiyse tamamlandi say
            if r.distribution_date and r.distribution_date < cutoff:
                r.status = "tamamlandi"
                r.updated_at = datetime.now(timezone.utc)
                completed += 1
        if completed > 0:
            await db.commit()
            logger.info("Halkarz: %d eski kayit 'tamamlandi' olarak isaretlendi", completed)
        return completed


async def scrape_halkarz_sermaye() -> dict:
    """Tek seferde scrape + upsert. Scheduler bu fonksiyonu çağırır."""
    html = await fetch_html()
    if not html:
        return {"status": "error", "msg": "HTML alınamadı"}

    records = parse_halkarz_html(html)
    if not records:
        return {"status": "warn", "msg": "0 satır parse edildi"}

    try:
        stats = await upsert_records(records)
        # Halkarz'da artik gorunmeyenleri tamamlandi say (dagitim tarihi gecmis olanlar)
        active_keys = {(r["ticker"].upper(), r["type"]) for r in records}
        completed = await _mark_missing_as_completed(active_keys)
        stats["completed"] = completed
        logger.info(
            "Halkarz sermaye: %d kayıt parse, %d yeni, %d güncellendi, %d atlandi, %d tamamlandi",
            stats["total"], stats["inserted"], stats["updated"], stats["skipped"], completed,
        )
        return {"status": "ok", **stats}
    except Exception as e:
        logger.exception("Halkarz upsert hata: %s", e)
        return {"status": "error", "msg": str(e)[:200]}
