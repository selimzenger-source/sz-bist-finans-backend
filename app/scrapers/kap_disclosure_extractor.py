"""KAP Bildirim sayfasindan yapilandirilmis veri cikarici.

KAP (kap.org.tr) Next.js (RSC) ile render ediyor. Sayfa HTML'inde
`self.__next_f.push([1, "..."])` chunk'lari icinde tum bildirim verisi var:
  - Bildirim turu, tarih, sirket bilgisi
  - "text-block-value" div'lerinde aciklama
  - Tablo verileri ("table-block" / "comp-row")
  - PDF linkleri (/api/file/download/...)

Bu modul:
  1. Next.js RSC chunk'larini decode eder
  2. Yapilandirilmis veriyi cikarir (body text, tablolar, PDF link)
  3. Sonra her processor (temettu, bilanco, business_deal, vs.) bu yapiyi kullanir

Boylece her processor ayri ayri scrape yapmak yerine tek kaynaktan beslenir.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Referer": "https://www.kap.org.tr/",
}


def _decode_rsc_chunks(html: str) -> str:
    """`self.__next_f.push([1, "..."])` chunk'larini birlestir + unicode decode.

    Cikti: tek string (HTML + escaped icerik), her chunk concat.
    """
    # Tum push chunk'larini topla
    chunks = re.findall(
        r'self\.__next_f\.push\(\[\s*1\s*,\s*"((?:\\.|[^"\\])*)"\s*\]\)',
        html,
    )

    if not chunks:
        return ""

    import json as _json
    decoded_parts = []
    for chunk in chunks:
        try:
            # JSON string escape decode (< → <, \" → ", \\n → \n)
            # UTF-8 karakterleri zaten dogru kodlanmis; json.loads'a sarip cevirmek
            # \uXXXX escape'leri de halleder.
            decoded = _json.loads('"' + chunk + '"')
            decoded_parts.append(decoded)
        except Exception:
            # JSON parse failed — manuel \uXXXX decode dene
            try:
                decoded = re.sub(
                    r'\\u([0-9a-fA-F]{4})',
                    lambda mm: chr(int(mm.group(1), 16)),
                    chunk,
                )
                decoded_parts.append(decoded)
            except Exception:
                decoded_parts.append(chunk)

    return "\n".join(decoded_parts)


def _extract_text_blocks(decoded: str) -> list[str]:
    """text-block-value class'li div'lerin icerigini cikarir.

    Bunlar bildirim formundaki SERBEST METIN alanlari (ozet bilgi, hak kullanim aciklamasi vs.)
    """
    if not decoded:
        return []

    # text-block-value icindeki HTML icerigi yakala
    # Ornek: <div class="text-block-value"><div>ALARK.E Pay Basina Brut Temettu: 3,185 TL...</div></div>
    pattern = r'class=\\?"text-block-value\\?"[^>]*>(.*?)</div>\s*</div>'
    matches = re.findall(pattern, decoded, re.DOTALL)

    blocks = []
    for raw_html in matches:
        # HTML tag'lerini temizle, dustur'a cevir
        cleaned = BeautifulSoup(raw_html, "html.parser").get_text(" ", strip=True)
        if cleaned and len(cleaned) > 5:
            blocks.append(cleaned)

    return blocks


def _extract_table_rows(decoded: str) -> list[dict]:
    """KAP tablo satirlarini cikarir.

    KAP'ta tablo `table-block` veya `comp-row` div'leriyle render edilir.
    Her satir: {"columns": [...], "header": bool}
    """
    if not decoded:
        return []

    soup = BeautifulSoup(decoded, "html.parser")

    rows: list[dict] = []
    # Variation 1: <div class="comp-row"><div class="comp-cell">...</div>...</div>
    for row in soup.select("div.comp-row, .table-row, tr"):
        cells = []
        for cell in row.select("div.comp-cell, .table-cell, td, th"):
            text = cell.get_text(" ", strip=True)
            cells.append(text)
        if cells and any(c for c in cells):
            rows.append({"columns": cells})

    return rows


def _extract_pdf_links(decoded: str, html: str) -> list[str]:
    """PDF / dosya download linklerini cikarir."""
    sources = (decoded or "") + "\n" + (html or "")
    pdf_links = set()

    # /api/file/download/{hash} formati
    for m in re.finditer(r'/api/file/download/[a-f0-9]+', sources):
        pdf_links.add(f"https://www.kap.org.tr{m.group(0)}")

    # /api/BildirimPdf/{id}
    for m in re.finditer(r'/api/BildirimPdf/\d+', sources):
        pdf_links.add(f"https://www.kap.org.tr{m.group(0)}")

    return sorted(pdf_links)


async def fetch_kap_disclosure(
    kap_url: str,
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[dict]:
    """KAP bildirim sayfasini cek + yapilandirilmis veriyi dondur.

    Returns:
        {
            "bildirim_id": "1600207",
            "url": "https://www.kap.org.tr/tr/Bildirim/1600207",
            "raw_decoded": "...",          # Tum decode edilmis RSC icerigi
            "text_blocks": ["..."],         # text-block-value icerikleri
            "tables": [{"columns": [...]}], # tablo satirlari
            "pdf_links": ["..."],            # PDF download linkleri
            "full_text": "...",              # Tum text birlesmis
        }
        Veya None (basarisiz)
    """
    if not kap_url or "kap.org.tr" not in kap_url:
        return None

    m = re.search(r"Bildirim/(\d+)", kap_url)
    if not m:
        return None
    bildirim_id = m.group(1)

    canonical_url = f"https://www.kap.org.tr/tr/Bildirim/{bildirim_id}"
    own_client = client is None
    try:
        if own_client:
            client = httpx.AsyncClient(
                timeout=20.0,
                follow_redirects=True,
                headers=DEFAULT_HEADERS,
            )

        try:
            r = await client.get(canonical_url)
            if r.status_code != 200:
                logger.warning("KAP %s HTTP %d", bildirim_id, r.status_code)
                return None
            html = r.text
        except Exception as exc:
            logger.warning("KAP %s fetch hata: %s", bildirim_id, exc)
            return None

        decoded = _decode_rsc_chunks(html)
        text_blocks = _extract_text_blocks(decoded)
        tables = _extract_table_rows(decoded)
        pdf_links = _extract_pdf_links(decoded, html)

        # full_text — text_blocks + tablo satirlari (AI prompt + regex parse icin tek kaynak)
        parts: list[str] = []
        parts.extend(text_blocks)
        for row in tables:
            parts.append(" | ".join(row.get("columns", [])))
        full_text = "\n".join(p for p in parts if p).strip()

        # Hicbir sey bulamazsak fallback: raw decoded'dan HTML temizle
        if not full_text and decoded:
            soup = BeautifulSoup(decoded, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            full_text = soup.get_text(" ", strip=True)[:30000]

        return {
            "bildirim_id": bildirim_id,
            "url": canonical_url,
            "raw_decoded": decoded[:300000],  # Cap to avoid memory blow
            "text_blocks": text_blocks,
            "tables": tables,
            "pdf_links": pdf_links,
            "full_text": full_text[:200000],  # Bilanço için tam: balance sheet + income statement
            "html_length": len(html),
        }
    finally:
        if own_client and client is not None:
            await client.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# CLI test
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import asyncio
    import json
    import sys

    # Force UTF-8 stdout on Windows
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    async def main():
        urls = sys.argv[1:] or [
            "https://www.kap.org.tr/tr/Bildirim/1600195",   # SASA finansal rapor
            "https://www.kap.org.tr/tr/Bildirim/1600202",   # PDF ekli MKK
            "https://www.kap.org.tr/tr/Bildirim/1600207",   # Temettu odeme (ALARK/EGGUB/KFEIN)
            "https://www.kap.org.tr/tr/Bildirim/1600267",   # SMRVA bolunme gerceklesme
        ]
        for url in urls:
            print(f"\n{'='*80}\n{url}\n{'='*80}")
            result = await fetch_kap_disclosure(url)
            if not result:
                print("FAIL")
                continue
            print(f"text_blocks ({len(result['text_blocks'])}):")
            for tb in result["text_blocks"][:5]:
                print(f"  - {tb[:200]}")
            print(f"tables ({len(result['tables'])}):")
            for t in result["tables"][:3]:
                print(f"  - {t}")
            print(f"pdf_links: {result['pdf_links']}")
            print(f"full_text length: {len(result['full_text'])}")
            print(f"full_text head: {result['full_text'][:300]}")

    asyncio.run(main())
