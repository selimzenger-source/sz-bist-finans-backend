"""HalkArz.com sermaye artırımı scraper.

URL: https://halkarz.com/sermaye-artirimi/
3 tablo:
  - Bedelsiz sermaye artırımı yapacak şirketler
  - Bedelli sermaye artırımı yapacak şirketler
  - Tahsisli bedelli sermaye artırımı yapacak şirketler

Format:
  Bist Kodu | Yüzde(%) | Tutar | [Rüçhan] | YKK | SPK Onay | Tarih

Bu scraper sermaye artırımı verilerini CapitalIncrease tablosuna upsert eder.
KAP linki: KAP'tan gelmiyor ama YKK tarihinden mevcut kayıt eşleşir.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.capital_increase import CapitalIncrease

logger = logging.getLogger(__name__)

URL = "https://halkarz.com/sermaye-artirimi/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

_DATE_RE = re.compile(r"^([0-3]?\d)\.([01]?\d)\.(20\d{2})$")
_PCT_RE = re.compile(r"%([\d.,]+)")
_AMOUNT_RE = re.compile(r"([\d.,]+)\s*TL", re.IGNORECASE)


def _parse_pct(s: str) -> Optional[float]:
    if not s:
        return None
    m = _PCT_RE.search(s)
    if not m:
        return None
    raw = m.group(1).replace(".", "").replace(",", ".") if "," in m.group(1) else m.group(1).replace(",", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_amount(s: str) -> Optional[float]:
    if not s:
        return None
    m = _AMOUNT_RE.search(s.replace("\xa0", " "))
    if not m:
        return None
    raw = m.group(1).replace(".", "").replace(",", ".") if "," in m.group(1) else m.group(1).replace(".", "")
    try:
        return float(raw)
    except ValueError:
        return None


def _parse_date(s: str) -> Optional[date]:
    if not s:
        return None
    s = s.strip()
    m = _DATE_RE.match(s)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except (ValueError, TypeError):
        return None


async def fetch_halkarz_capital() -> list[dict]:
    """halkarz.com/sermaye-artirimi/ sayfasını çek + parse et.

    Returns:
        [{ticker, company_name, type, percentage, amount_tl, ykk_date, spk_approval_date, distribution_date}, ...]
    """
    records: list[dict] = []
    try:
        async with httpx.AsyncClient(timeout=30, headers=HEADERS, follow_redirects=True) as c:
            r = await c.get(URL)
            if r.status_code != 200:
                logger.warning("halkarz HTTP %s", r.status_code)
                return []
            html = r.text
    except Exception as e:
        logger.error("halkarz fetch hata: %s", e)
        return []

    soup = BeautifulSoup(html, "html.parser")

    # 3 tablo var — her tablo bir <h2> başlığının ardından gelir
    # Bedelsiz / Bedelli / Tahsisli
    type_map = {
        "bedelsiz": "bedelsiz",
        "bedelli": "bedelli",
        "tahsisli": "tahsisli",
    }

    # Tüm tabloları bul, içlerinden başlığa göre type belirle
    # Strategy: her başlık (h2/h3/h4) ile sonraki <table> ilişkilendir
    headers = soup.find_all(["h1","h2","h3","h4","h5"])
    for h in headers:
        ht = (h.get_text() or "").strip().lower()
        sa_type = None
        if "tahsisli" in ht:
            sa_type = "tahsisli"
        elif "bedelli" in ht:
            sa_type = "bedelli"
        elif "bedelsiz" in ht:
            sa_type = "bedelsiz"
        if not sa_type:
            continue

        # Bu başlığın ardındaki ilk tabloyu bul
        table = h.find_next("table")
        if not table:
            continue

        for tr in table.find_all("tr"):
            cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
            if len(cells) < 4:
                continue
            ticker = cells[0]
            # İlk hücre header değilse devam (ticker 3-6 büyük harf)
            if not re.match(r"^[A-Z][A-Z0-9]{2,5}$", ticker):
                continue

            # Şirket adı ya 2. hücrede ya da ticker'la birlikte
            # Kolon yapısı: Ticker | (Şirket?) | Yüzde | Tutar | [Rüçhan] | YKK | SPK | Tarih
            # halkarz'da genelde Ticker tek kolonda, şirket adı tooltip'te. cells uzunluğu değişir
            company_name = None
            idx = 1

            # Yüzde kolonu (% içerir)
            if idx < len(cells) and "%" not in cells[idx]:
                # bu şirket adı olabilir
                company_name = cells[idx]
                idx += 1

            percentage = _parse_pct(cells[idx]) if idx < len(cells) else None
            idx += 1

            amount_tl = _parse_amount(cells[idx]) if idx < len(cells) else None
            idx += 1

            # Bedelli için: Rüçhan kolonu (1 TL gibi) — atla
            if sa_type == "bedelli" and idx < len(cells):
                # Rüçhan değeri "1 TL" gibi olabilir veya tarih
                if not _parse_date(cells[idx]):
                    idx += 1

            ykk_date = _parse_date(cells[idx]) if idx < len(cells) else None
            idx += 1

            spk_approval_date = _parse_date(cells[idx]) if idx < len(cells) else None
            idx += 1

            distribution_date = _parse_date(cells[idx]) if idx < len(cells) else None

            records.append({
                "ticker": ticker,
                "company_name": company_name,
                "type": sa_type,
                "percentage": percentage,
                "amount_tl": amount_tl,
                "ykk_date": ykk_date,
                "spk_approval_date": spk_approval_date,
                "distribution_date": distribution_date,
            })

    return records


async def upsert_capital_increases(db: AsyncSession, records: list[dict]) -> dict:
    """halkarz kayıtlarını CapitalIncrease tablosuna upsert.

    Match key: (ticker, type) + ykk_date — varsa update, yoksa yeni.
    """
    today = date.today()
    inserted = 0
    updated = 0
    errors = 0

    for rec in records:
        try:
            # Match: ticker + type + ykk_date (varsa)
            stmt = select(CapitalIncrease).where(
                CapitalIncrease.ticker == rec["ticker"],
                CapitalIncrease.type == rec["type"],
            ).order_by(CapitalIncrease.created_at.desc()).limit(1)
            existing = (await db.execute(stmt)).scalar_one_or_none()

            # Status hesapla
            status = "ykk_alindi"
            if rec.get("distribution_date"):
                if rec["distribution_date"] == today:
                    status = "dagitiliyor"
                elif rec["distribution_date"] < today:
                    status = "tamamlandi"
                else:
                    status = "tarih_belli"
            elif rec.get("spk_approval_date"):
                status = "spk_onayli"

            if existing:
                # Mevcut kaydı zenginleştir
                if rec.get("percentage") and not existing.percentage:
                    existing.percentage = rec["percentage"]
                if rec.get("amount_tl") and not existing.amount_tl:
                    existing.amount_tl = rec["amount_tl"]
                if rec.get("ykk_date") and not existing.ykk_date:
                    existing.ykk_date = rec["ykk_date"]
                if rec.get("spk_approval_date") and not existing.spk_approval_date:
                    existing.spk_approval_date = rec["spk_approval_date"]
                if rec.get("distribution_date") and not existing.distribution_date:
                    existing.distribution_date = rec["distribution_date"]
                if rec.get("company_name") and not existing.company_name:
                    existing.company_name = rec["company_name"]
                # Status sadece ileri taşı (geri alma)
                _order = ["ykk_alindi","spk_onayli","tarih_belli","dagitiliyor","tamamlandi","reddedildi"]
                try:
                    if _order.index(status) > _order.index(existing.status or "ykk_alindi"):
                        existing.status = status
                except ValueError:
                    existing.status = status
                updated += 1
            else:
                new_row = CapitalIncrease(
                    ticker=rec["ticker"],
                    company_name=rec.get("company_name"),
                    type=rec["type"],
                    percentage=rec.get("percentage"),
                    amount_tl=rec.get("amount_tl"),
                    ykk_date=rec.get("ykk_date"),
                    spk_approval_date=rec.get("spk_approval_date"),
                    distribution_date=rec.get("distribution_date"),
                    status=status,
                )
                db.add(new_row)
                inserted += 1
        except Exception as e:
            errors += 1
            logger.warning("halkarz upsert hata (%s): %s", rec.get("ticker"), e)

    await db.flush()
    return {"inserted": inserted, "updated": updated, "errors": errors, "total": len(records)}
