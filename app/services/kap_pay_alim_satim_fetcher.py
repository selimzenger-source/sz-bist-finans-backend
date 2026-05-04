"""KAP Pay Alım Satım Fetcher.

KAP URL'sinden Pay Alım Satım Bildirimi'ni otomatik fetch edip
structured data'yı share_transaction_details tablosuna yazar.

Telegram poller'dan veya admin tetikleyiciden cagrilir.

KAP Format:
  Tablo: İşlem Tarihi | Alım Nominal | Satım Nominal | Net Nominal |
         Gün Başı Nominal | Gün Sonu Nominal |
         Gün Başı Pay Oranı (%) | Gün Başı Oy Hakkı (%) |
         Gün Sonu Pay Oranı (%) | Gün Sonu Oy Hakkı (%)

  Body (Açıklamalar): party name, transaction details (regex/AI ile çekilir)
"""

import re
import logging
from datetime import date, datetime
from typing import Optional

import httpx
from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


def _normalize_url(url: str) -> str:
    """KAP URL'sini /tr/Bildirim/ formatina cevir."""
    url = url.strip().rstrip(".,;)")
    # /Bildirim/ -> /tr/Bildirim/
    url = re.sub(r"kap\.org\.tr/(?!tr/|en/)Bildirim/", "kap.org.tr/tr/Bildirim/", url)
    url = re.sub(r"kap\.org\.tr/en/Bildirim/", "kap.org.tr/tr/Bildirim/", url)
    return url


def _parse_tr_decimal(s: str) -> Optional[float]:
    """'4.773.736,55' -> 4773736.55 / '%4,15' -> 4.15"""
    if not s:
        return None
    s = s.replace("%", "").replace(" TL", "").replace(" ", "").strip()
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _parse_tr_int(s: str) -> Optional[int]:
    if not s:
        return None
    s = re.sub(r"[^\d]", "", s)
    try:
        return int(s) if s else None
    except Exception:
        return None


def _parse_kap_date(s: str) -> Optional[date]:
    """'30/04/2026' veya '30.04.2026' -> date"""
    s = s.strip()
    for sep in ("/", "."):
        if sep in s:
            try:
                d, m, y = s.split(sep)
                return date(int(y), int(m), int(d))
            except Exception:
                pass
    return None


def parse_kap_html(html: str) -> Optional[dict]:
    """KAP HTML'inden Pay Alım Satım structured data çikarir.

    Returns:
        dict | None — {transaction_date, alim_nominal, satim_nominal, net_nominal,
                       beginning_nominal, end_nominal,
                       beginning_pay_oran_pct, beginning_oy_hakki_pct,
                       end_pay_oran_pct, end_oy_hakki_pct,
                       ticker, party_name, body_text}
    """
    # Tum <td> degerlerini cek (BOS HUCRELERI KORU - pozisyon onemli)
    tds = re.findall(r"<td[^>]*>(.*?)</td>", html, re.DOTALL)
    cleaned = [re.sub(r"<[^>]+>", "", t).replace("\xa0", " ").strip() for t in tds]

    # İlgili Şirketler — ticker (UPPERCASE only — IGNORECASE kucuk 'div' falan yakalardi)
    ticker = None
    # Once "İlgili Şirketler [TICKER]" pattern (kose parantezli)
    m = re.search(r"İlgili\s+Şirketler[^\[]{0,30}\[([A-Z]{2,6})", html, re.DOTALL)
    if m:
        ticker = m.group(1).strip()
    if not ticker:
        # Fallback: ilk kose parantezli ticker
        m = re.search(r"\[([A-Z]{2,6})(?:\s*,|\])", html)
        if m:
            ticker = m.group(1).strip()

    # KAP tablo yapisi (10 sutun):
    # [0] Tarih  [1] Alim Nominal  [2] Satim Nominal  [3] Net Nominal
    # [4] Gun Basi Nominal  [5] Gun Sonu Nominal
    # [6] Gun Basi Pay Oran  [7] Gun Basi Oy Hakki
    # [8] Gun Sonu Pay Oran  [9] Gun Sonu Oy Hakki
    #
    # Date pattern'i bul, sonraki 9 td positional al
    date_idx = None
    for i, c in enumerate(cleaned):
        if re.match(r"^\d{2}/\d{2}/\d{4}$", c) or re.match(r"^\d{2}\.\d{2}\.\d{4}$", c):
            date_idx = i
            break

    if date_idx is None or date_idx + 9 >= len(cleaned):
        return {"ticker": ticker, "body_text": _extract_body_text(html), "_no_table": True}

    # Pozisyonel — bos hucreler bile sayilir
    cells = cleaned[date_idx:date_idx + 10]
    return {
        "ticker": ticker,
        "transaction_date": _parse_kap_date(cells[0]),
        "alim_nominal": _parse_tr_decimal(cells[1]) if cells[1] else None,
        "satim_nominal": _parse_tr_decimal(cells[2]) if cells[2] else None,
        "net_nominal": _parse_tr_decimal(cells[3]) if cells[3] else None,
        "beginning_nominal": _parse_tr_decimal(cells[4]) if cells[4] else None,
        "end_nominal": _parse_tr_decimal(cells[5]) if cells[5] else None,
        "beginning_pay_oran_pct": _parse_tr_decimal(cells[6]) if cells[6] else None,
        "beginning_oy_hakki_pct": _parse_tr_decimal(cells[7]) if cells[7] else None,
        "end_pay_oran_pct": _parse_tr_decimal(cells[8]) if cells[8] else None,
        "end_oy_hakki_pct": _parse_tr_decimal(cells[9]) if cells[9] else None,
        "body_text": _extract_body_text(html),
    }


def _extract_body_text(html: str) -> str:
    """Açıklamalar bölümündeki metni cek."""
    # "Açıklamalar" header'inin sonrasini al
    m = re.search(r"A[çc]ıklamalar(.*?)(?:Pay Al|Yukarıdaki|İmza)", html, re.DOTALL)
    if not m:
        return ""
    text = re.sub(r"<[^>]+>", " ", m.group(1))
    text = re.sub(r"\s+", " ", text).strip()
    return text[:2000]


def extract_party_name(body: str) -> Optional[str]:
    """Body metninden party adını çıkar (basit regex).

    Ornek: "Pardus Portföy Yönetimi AŞ.'nin kurucusu olduğu yatırım fonlarının..."
    -> "Pardus Portföy Yönetimi AŞ"
    """
    if not body:
        return None
    # "X A.Ş.'nin" / "X A.Ş.nin" pattern
    patterns = [
        r"([A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü\.\s\-&]{5,80}?)\s*(?:A\.\s*Ş\.|AŞ\.|A\.Ş)\.?'?\s*[ni]?n[ıi]?n",
        r"([A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü\.\s\-&]{5,80}?)\s+kurucu",
    ]
    for pat in patterns:
        m = re.search(pat, body)
        if m:
            name = m.group(1).strip()
            if "A.Ş" not in name and "AŞ" not in name:
                name += " A.Ş."
            return name[:200]
    return None


async def fetch_kap_pay_alim_satim(kap_url: str, client: Optional[httpx.AsyncClient] = None) -> Optional[dict]:
    """KAP URL'sinden Pay Alım Satım structured data fetch eder.

    KAP Next.js (RSC) ile render ediyor; raw HTML'de <td> yok.
    Önce RSC chunk'larını decode edip içindeki <td>'lerle çalış.
    """
    url = _normalize_url(kap_url)
    own = client is None
    try:
        if own:
            client = httpx.AsyncClient(timeout=15.0, headers=HEADERS, follow_redirects=True)
        resp = await client.get(url)
        resp.raise_for_status()
        html = resp.text

        # Önce RSC decode dene (yeni KAP)
        try:
            from app.scrapers.kap_disclosure_extractor import _decode_rsc_chunks
            decoded = _decode_rsc_chunks(html)
            if decoded and "<td" in decoded.lower():
                result = parse_kap_html(decoded)
                if result:
                    return result
        except Exception as de:
            logger.debug("RSC decode hata, raw HTML'e düşülüyor: %s", de)

        # Fallback: raw HTML
        return parse_kap_html(html)
    except Exception as e:
        logger.warning("KAP fetch hata (%s): %s", url, e)
        return None
    finally:
        if own and client:
            await client.aclose()


async def upsert_pay_alim_satim_from_kap(
    db: AsyncSession,
    kap_url: str,
    company_code: str,
    title: str,
    published_at: Optional[datetime] = None,
    disclosure_id: Optional[int] = None,
) -> bool:
    """Telegram poller'dan cagrilir.

    KAP URL'den fetch -> parse -> share_transaction_details'e UPSERT.

    Args:
        db: Async session
        kap_url: KAP bildirim URL'i
        company_code: Hisse kodu (KAP'tan da alinir, yedek)
        title: KAP basligi
        published_at: Yayim tarihi
        disclosure_id: kap_all_disclosures.id (FK)

    Returns:
        bool — basarili mi
    """
    parsed = await fetch_kap_pay_alim_satim(kap_url)
    if not parsed:
        return False

    ticker = (parsed.get("ticker") or company_code or "").upper()
    if not ticker:
        return False

    # Transaction type (alim/satim nominal'a gore)
    alim = parsed.get("alim_nominal") or 0
    satim = parsed.get("satim_nominal") or 0
    if alim > 0 and satim == 0:
        tx_type = "alis"
    elif satim > 0 and alim == 0:
        tx_type = "satis"
    elif alim > satim:
        tx_type = "alis"
    elif satim > alim:
        tx_type = "satis"
    else:
        tx_type = "alis"  # default

    nominal_lot = int(parsed.get("alim_nominal") or parsed.get("satim_nominal") or 0)
    body = parsed.get("body_text") or ""
    party_name = extract_party_name(body)

    # Oranlar — gun sonu degerleri "current"
    end_pay = parsed.get("end_pay_oran_pct")
    end_oy = parsed.get("end_oy_hakki_pct")
    beg_pay = parsed.get("beginning_pay_oran_pct")
    beg_oy = parsed.get("beginning_oy_hakki_pct")
    pay_change = (end_pay - beg_pay) if (end_pay is not None and beg_pay is not None) else None
    oy_change = (end_oy - beg_oy) if (end_oy is not None and beg_oy is not None) else None

    tx_date = parsed.get("transaction_date") or (published_at.date() if published_at else None)
    if not tx_date:
        return False

    # UPSERT
    check = await db.execute(sa_text("""
        SELECT id FROM share_transaction_details
        WHERE ticker=:tk AND transaction_date=:dt
          AND COALESCE(party_name,'')=:pn
    """), {"tk": ticker, "dt": tx_date, "pn": party_name or ""})
    existing_id = check.scalar()

    if existing_id:
        await db.execute(sa_text("""
            UPDATE share_transaction_details
            SET transaction_type=:tt, party_name=COALESCE(:pn, party_name),
                nominal_lot=:lot,
                oy_hakki_pct=:oy, oy_hakki_change_pct=:oyc,
                pay_orani_pct=:po, pay_orani_change_pct=:poc,
                kap_url=:kap, kap_disclosure_id=COALESCE(:did, kap_disclosure_id),
                source='kap_auto', raw_excerpt=:raw
            WHERE id=:id
        """), {
            "id": existing_id, "tt": tx_type, "pn": party_name,
            "lot": nominal_lot or None,
            "oy": end_oy, "oyc": oy_change, "po": end_pay, "poc": pay_change,
            "kap": kap_url, "did": disclosure_id, "raw": body[:1000],
        })
    else:
        await db.execute(sa_text("""
            INSERT INTO share_transaction_details(ticker, transaction_date,
                transaction_type, party_name, nominal_lot,
                oy_hakki_pct, oy_hakki_change_pct,
                pay_orani_pct, pay_orani_change_pct,
                kap_url, kap_disclosure_id, source, raw_excerpt, created_at)
            VALUES(:tk, :dt, :tt, :pn, :lot, :oy, :oyc, :po, :poc,
                   :kap, :did, 'kap_auto', :raw, NOW())
        """), {
            "tk": ticker, "dt": tx_date, "tt": tx_type, "pn": party_name,
            "lot": nominal_lot or None,
            "oy": end_oy, "oyc": oy_change, "po": end_pay, "poc": pay_change,
            "kap": kap_url, "did": disclosure_id, "raw": body[:1000],
        })

    logger.info("KAP Pay Alim Satim islendi: %s %s %s party=%s pay=%s",
                ticker, tx_date, tx_type, party_name, end_pay)
    return True


# Test
if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def t():
        # MERKO test
        result = await fetch_kap_pay_alim_satim("https://www.kap.org.tr/tr/Bildirim/1599966")
        if result:
            for k, v in result.items():
                if k != "body_text":
                    print(f"  {k}: {v}")
            print(f"  body_text[:200]: {(result.get('body_text') or '')[:200]}")
            party = extract_party_name(result.get("body_text") or "")
            print(f"  parsed party: {party}")

    asyncio.run(t())
