"""halkarz.com/tedbirli-hisseler — kolon-bazlı parser.

Sayfa yapısı (table tabanlı):
  | Bist Kodu | Şirket | Başlangıç | Bitiş |
  | Açığa Satış & Kredili İşlem | Brüt Takas | Tek Fiyat | Emir Paketi | İnternet Emir |

Her hisse satırında 5 tedbir sütunu var; ✗ varsa o tedbir aktiftir.
Tag mapping:
  Açığa Satış & Kredili İşlem → ACS + KRD
  Brüt Takas → BRT
  Tek Fiyat → TEK
  Emir Paketi → EPT
  İnternet Emir → IEY
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

URL = "https://halkarz.com/tedbirli-hisseler/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

# Header eşleme — halkarz.com'da görünebilecek farklı yazılışlar
_HEADER_TO_TAGS = {
    "açığa satış & kredili işlem": ["ACS", "KRD"],
    "acigasatis & kredili islem":  ["ACS", "KRD"],
    "açığa satış kredili":          ["ACS", "KRD"],
    "açığa satış":                  ["ACS"],
    "kredili işlem":                ["KRD"],
    "kredili":                       ["KRD"],
    "brüt takas":                   ["BRT"],
    "brut takas":                   ["BRT"],
    "tek fiyat":                    ["TEK"],
    "emir paketi":                  ["EPT"],
    "emir iptali":                  ["EMR"],
    "i̇nternet emir":                ["IEY"],
    "internet emir":                ["IEY"],
    "piyasa emri":                  ["PYS"],
    "veri yayını":                  ["VER"],
    "veri yayini":                  ["VER"],
}


def _parse_date(s: str) -> Optional[date]:
    """11.05.2026 / 11/05/2026 / 2026-05-11 → date."""
    s = (s or "").strip()
    if not s:
        return None
    # ISO
    try:
        return date.fromisoformat(s)
    except ValueError:
        pass
    # DD.MM.YYYY veya DD/MM/YYYY
    for sep in (".", "/"):
        if sep in s:
            try:
                d, m, y = s.split(sep)
                return date(int(y), int(m), int(d))
            except Exception:
                continue
    return None


def _has_x(cell_text: str) -> bool:
    """Bir hücre içeriği ✗ / X / × benzeri 'evet bu tedbir aktif' işareti içeriyor mu?"""
    if not cell_text:
        return False
    t = cell_text.strip()
    if not t:
        return False
    # Boş hücre işaretleri
    if t in {"-", "—", "–", ""}:
        return False
    # ✗, ×, X, ✓, x
    return any(c in t for c in ("✗", "×", "X", "x", "✓"))


def _normalize_header(s: str) -> str:
    return " ".join((s or "").lower().strip().split())


def parse_halkarz_html(html: str) -> list[dict]:
    """halkarz.com HTML'inden tedbirli hisseleri kolon-bazlı parse et.

    Returns: list of {ticker, company_name, start_date, end_date, tags, source}
    """
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []

    # Sayfada birden fazla tablo olabilir; tedbir tablosunu en fazla satıra
    # sahip ve "Bist" + "Bitiş" içerikli header'a sahip olan diye bul.
    tables = soup.find_all("table")
    target = None
    for tbl in tables:
        headers = [_normalize_header(th.get_text(" ", strip=True)) for th in tbl.find_all("th")]
        if not headers:
            # th yoksa ilk satırı header kabul et
            first_row = tbl.find("tr")
            if first_row:
                headers = [_normalize_header(td.get_text(" ", strip=True)) for td in first_row.find_all(["th", "td"])]
        joined = " ".join(headers)
        if "bist" in joined and ("bitiş" in joined or "bitis" in joined):
            target = (tbl, headers)
            break

    if not target:
        logger.warning("halkarz: tedbir tablosu bulunamadi")
        return results

    tbl, headers = target

    # Kolon index'lerini eşle
    def _find_col(*needles: str) -> int | None:
        for i, h in enumerate(headers):
            for n in needles:
                if n in h:
                    return i
        return None

    col_ticker = _find_col("bist kodu", "bist")
    col_company = _find_col("şirket", "sirket")
    col_start = _find_col("başlangıç", "baslangic")
    col_end = _find_col("bitiş", "bitis")

    # Tedbir kolonlarını eşle (header text → tag list)
    measure_cols: list[tuple[int, list[str]]] = []  # (col_index, tag_list)
    for i, h in enumerate(headers):
        # Skip non-measure columns
        if i in {col_ticker, col_company, col_start, col_end}:
            continue
        # Match against known headers (partial)
        tags_for_col = None
        for key, tags in _HEADER_TO_TAGS.items():
            if key in h:
                tags_for_col = tags
                break
        if tags_for_col:
            measure_cols.append((i, tags_for_col))

    if col_ticker is None:
        logger.warning("halkarz: 'Bist Kodu' kolonu bulunamadi (headers=%s)", headers)
        return results

    rows = tbl.find_all("tr")
    for tr in rows[1:]:  # skip header row
        cells = tr.find_all(["td", "th"])
        if not cells or len(cells) <= col_ticker:
            continue

        cell_texts = [c.get_text(" ", strip=True) for c in cells]
        ticker = (cell_texts[col_ticker] if col_ticker is not None else "").upper().strip()
        # Filter boş veya garip ticker'ları
        if not ticker or len(ticker) < 2 or len(ticker) > 8:
            continue
        if not ticker.replace(".", "").isalnum():
            continue

        company = cell_texts[col_company] if (col_company is not None and len(cell_texts) > col_company) else None
        start_d = _parse_date(cell_texts[col_start]) if col_start is not None and len(cell_texts) > col_start else None
        end_d = _parse_date(cell_texts[col_end]) if col_end is not None and len(cell_texts) > col_end else None

        # Tedbir tag'lerini topla
        tags: list[str] = []
        for idx, tag_list in measure_cols:
            if len(cell_texts) > idx and _has_x(cell_texts[idx]):
                for t in tag_list:
                    if t not in tags:
                        tags.append(t)

        # Tag yoksa hisseyi atla (aktif tedbir yok demek)
        if not tags:
            continue

        results.append({
            "ticker": ticker,
            "company_name": (company[:120] if company else None),
            "start_date": start_d,
            "end_date": end_d,
            "tags": tags,
            "source": "halkarz",
        })

    logger.info("halkarz parse: %d hisse, %d tedbir tipi", len(results),
                len({t for r in results for t in r["tags"]}))
    return results


async def fetch_halkarz_tedbirli() -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=30.0, headers=HEADERS, follow_redirects=True) as client:
            resp = await client.get(URL)
            resp.raise_for_status()
            return parse_halkarz_html(resp.text)
    except Exception as e:
        logger.error("halkarz.com fetch hata: %s", e)
        return []


async def sync_to_db() -> dict:
    """halkarz.com'dan çek → cautious_stocks tablosunu güncelle.

    Mantık:
      - halkarz'da olan ve DB'de aynı (ticker, start_date) ile YOK ise → INSERT
      - halkarz'da olan ve DB'de var ise → tag listesi farklıysa UPDATE
      - halkarz'da olmayan AKTİF kayıtlar → değişmez (KAP'tan gelmiş olabilir)
    """
    from sqlalchemy import select
    from app.database import async_session
    from app.models.cautious_stock import CautiousStock

    items = await fetch_halkarz_tedbirli()
    summary = {
        "fetched": len(items),
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "tag_dist": {},
        "added": [],
    }

    if not items:
        return summary

    async with async_session() as db:
        for it in items:
            tag_str = ",".join(it.get("tags", []))
            for t in it.get("tags", []):
                summary["tag_dist"][t] = summary["tag_dist"].get(t, 0) + 1

            res = await db.execute(
                select(CautiousStock).where(
                    CautiousStock.ticker == it["ticker"],
                    CautiousStock.start_date == it["start_date"],
                ).limit(1)
            )
            existing = res.scalar_one_or_none()

            if existing:
                changed = False
                if (existing.tags or "") != tag_str:
                    existing.tags = tag_str
                    changed = True
                if existing.end_date != it.get("end_date") and it.get("end_date"):
                    existing.end_date = it["end_date"]
                    changed = True
                if not existing.company_name and it.get("company_name"):
                    existing.company_name = it["company_name"]
                    changed = True
                if not existing.is_active:
                    existing.is_active = True
                    changed = True
                if changed:
                    summary["updated"] += 1
                else:
                    summary["unchanged"] += 1
            else:
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
