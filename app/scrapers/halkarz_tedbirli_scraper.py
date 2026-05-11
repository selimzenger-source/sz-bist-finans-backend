"""halkarz.com/tedbirli-hisseler sayfasından tedbirli hisseleri scrape eder.

KAP'a düşmeyen / Telegram'a düşmeyen VBTS bildirimleri için alternatif kaynak.

Format (her hisse için):
  Bist Kodu: ALGYO
  Şirket: Alarko Gayrimenkul Yatırım Ortaklığı A.Ş.
  Başlangıç: 11.05.2026
  Bitiş: 10.06.2026
  Açığa Satış & Kredili İşlem: ✗
  Brüt Takas: (boş)
  Tek Fiyat: (boş)
  Emir Paketi: (boş)
  İnternet Emir: (boş)
"""

import re
import logging
from datetime import datetime, date, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


def _parse_date(s: str) -> Optional[date]:
    """11.05.2026 → date(2026, 5, 11)."""
    try:
        d, m, y = s.strip().split(".")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def parse_halkarz_html(html: str) -> list[dict]:
    """halkarz.com HTML'inden tedbirli hisse listesini çıkar.

    Her hisse için: {ticker, company_name, start_date, end_date, tags}
    tags: ['ACS', 'KRD', 'BRT', 'TEK', 'EPT', 'IEY']
    """
    results = []

    # Bist Kodu — şirket — tarih1 — tarih2 — tedbir sütunları pattern
    # HTML temizliği için tag'leri sil, sonra block bloklarına ayır
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)

    # Her hisse "Bist Kodu" sonrası ticker ile başlar
    # Pattern: Bist Kodu... TICKER...şirket adı...tarih1...tarih2...tedbir sütunları
    block_re = re.compile(
        r"Bist\s*Kodu[:\s]*</[^>]+>\s*(?:<[^>]+>)*\s*([A-Z]{3,6})\s*</[^>]+>"
        r".*?(\d{2}\.\d{2}\.\d{4}).*?(\d{2}\.\d{2}\.\d{4})",
        re.DOTALL | re.IGNORECASE,
    )

    for m in block_re.finditer(text):
        ticker = m.group(1).strip().upper()
        start_d = _parse_date(m.group(2))
        end_d = _parse_date(m.group(3))

        if not ticker or not start_d:
            continue

        # Block içinde tedbir tipleri için ✗ veya x ara
        # Her hisse satırında sütun sırası: Açığa Satış & Kredili / Brüt Takas / Tek Fiyat / Emir Paketi / İnternet Emir
        block_end = min(m.end() + 1500, len(text))
        block_text = text[m.end():block_end]

        # ✗ / × / X karakterlerini say
        x_count = len(re.findall(r"[✗×x]\b|✗", block_text[:600]))

        # Basit tag tespiti: ✗ varsa ACS+KRD ikili olduğunu kabul ediyoruz (halkarz.com tek sütun)
        # NOT: halkarz.com tedbir sütunlarını tek tek listeliyor — bunu sırayla yakalamak gerek
        tags = []
        # Daha güvenilir: column-bazlı ✗ pozisyonunu yakala (henüz basit, geliştirilmeli)
        if x_count >= 1:
            tags.append("ACS")
            tags.append("KRD")

        results.append({
            "ticker": ticker,
            "company_name": None,  # şirket adı için ek parse gerek
            "start_date": start_d,
            "end_date": end_d,
            "tags": tags,
            "source": "halkarz",
        })

    return results


async def fetch_halkarz_tedbirli() -> list[dict]:
    """halkarz.com/tedbirli-hisseler sayfasını fetch et + parse et."""
    url = "https://halkarz.com/tedbirli-hisseler/"
    try:
        async with httpx.AsyncClient(timeout=30.0, headers=HEADERS, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return parse_halkarz_html(resp.text)
    except Exception as e:
        logger.error("halkarz.com fetch hata: %s", e)
        return []


async def sync_to_db():
    """halkarz.com'dan çekip cautious_stocks tablosuna eksik olanları ekle.

    Returns: dict — eklenen sayı + örnek liste
    """
    from app.database import async_session
    from app.models.cautious_stock import CautiousStock
    from sqlalchemy import select

    items = await fetch_halkarz_tedbirli()
    summary = {"fetched": len(items), "inserted": 0, "skipped_existing": 0, "added": []}

    if not items:
        return summary

    async with async_session() as db:
        for it in items:
            # Mevcut mu kontrol — ticker + start_date kombinasyonu
            existing = await db.execute(
                select(CautiousStock).where(
                    CautiousStock.ticker == it["ticker"],
                    CautiousStock.start_date == it["start_date"],
                ).limit(1)
            )
            if existing.scalar_one_or_none():
                summary["skipped_existing"] += 1
                continue

            tag_str = ",".join(it.get("tags", []))
            new = CautiousStock(
                ticker=it["ticker"],
                company_name=it.get("company_name"),
                start_date=it["start_date"],
                end_date=it.get("end_date"),
                tags=tag_str,
                is_active=True,
                source="halkarz",
            )
            db.add(new)
            summary["inserted"] += 1
            if len(summary["added"]) < 30:
                summary["added"].append({
                    "ticker": it["ticker"],
                    "start_date": it["start_date"].isoformat() if it["start_date"] else None,
                    "end_date": it["end_date"].isoformat() if it["end_date"] else None,
                    "tags": it["tags"],
                })

        await db.commit()

    return summary
