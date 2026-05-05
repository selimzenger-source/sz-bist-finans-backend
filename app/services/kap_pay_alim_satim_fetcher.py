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
    # Fallback KALDIRILDI — body'de rastgele [DIV], [TR] gibi metinleri yakalayip
    # yanlis ticker olusturuyordu. KAP'in resmi "İlgili Şirketler" alanini bulamazsa
    # ticker None doner, upsert company_code parametresine fallback yapar.

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


def extract_price_range(body: str) -> tuple[Optional[float], Optional[float]]:
    """Body'den fiyat aralığı çikarir.

    Ornekler:
      "15,45-15,80 TL fiyat aralığından" -> (15.45, 15.80)
      "21 TL fiyattan" -> (21.0, 21.0)
      "15,73 - 15,90 TL araliginda" -> (15.73, 15.90)
    """
    if not body:
        return (None, None)
    # Range: "X,XX-Y,YY TL" or "X,XX - Y,YY TL"
    m = re.search(r"(\d{1,4}(?:[.,]\d{1,4})?)\s*[-–]\s*(\d{1,4}(?:[.,]\d{1,4})?)\s*TL", body)
    if m:
        lo = _parse_tr_decimal(m.group(1))
        hi = _parse_tr_decimal(m.group(2))
        if lo is not None and hi is not None and 0 < lo <= hi < 100000:
            return (lo, hi)
    # Single: "21 TL fiyattan" / "X,XX TL fiyat"
    m = re.search(r"(\d{1,4}(?:[.,]\d{1,4})?)\s*TL\s*fiyat", body, re.IGNORECASE)
    if m:
        v = _parse_tr_decimal(m.group(1))
        if v is not None and 0 < v < 100000:
            return (v, v)
    return (None, None)


def extract_nominal_from_body(body: str) -> Optional[int]:
    """Body'den nominal lot/adet çikar (tablo yoksa fallback).

    "1.154.631 adet payın" -> 1154631
    "30.925.229,00 TL toplam nominal" -> 30925229 (TL ise lot ozdes)
    """
    if not body:
        return None
    m = re.search(r"([\d\.]{4,20})\s*adet\s*pay", body, re.IGNORECASE)
    if m:
        v = _parse_tr_int(m.group(1))
        if v and v > 100:
            return v
    m = re.search(r"([\d\.]{4,20})(?:,\d+)?\s*TL\s*toplam\s*nominal", body, re.IGNORECASE)
    if m:
        v = _parse_tr_int(m.group(1))
        if v and v > 100:
            return v
    return None


def extract_party_from_html_header(html: str) -> Optional[str]:
    """KAP HTML'inden 'Bildirimi Yapan' şirket adını cek.

    KAP Next.js href slug'undan veya direkt başlık'tan.
    href: /tr/sirket-bilgileri/ozet/2354-atlas-portfoy-yonetimi-a-s
    -> ATLAS PORTFÖY YÖNETİMİ A.Ş.
    """
    if not html:
        return None
    # Slug'lardan firma adı (ozet/XXXX-slug-name)
    m = re.search(r"/sirket-bilgileri/ozet/\d+-([a-z0-9\-]+)", html)
    if m:
        slug = m.group(1)
        # slug -> Title Case + TR replace
        words = slug.replace("-", " ").split()
        # Bilinen kisaltma map
        map_short = {"a": "A.", "s": "Ş.", "as": "A.Ş.", "ltd": "Ltd.", "sti": "Şti."}
        out = []
        i = 0
        while i < len(words):
            w = words[i]
            # "a s" -> "A.Ş."
            if w == "a" and i + 1 < len(words) and words[i+1] == "s":
                out.append("A.Ş.")
                i += 2
                continue
            if w in map_short:
                out.append(map_short[w])
            else:
                out.append(w.upper())  # Türkçe karakter eşlemesi yapay zekanın yerine kelime küçük gelirse capitalize yeter
            i += 1
        name = " ".join(out)
        # Common TR substitutions in slug
        name = name.replace("YONETIMI", "YÖNETİMİ").replace("YONETIM", "YÖNETİM")
        name = name.replace("PORTFOY", "PORTFÖY").replace("ISYATIRIM", "İŞ YATIRIM")
        name = name.replace("TICARET", "TİCARET").replace("URETIM", "ÜRETİM")
        name = name.replace("INSAAT", "İNŞAAT").replace("SANAYI", "SANAYİ")
        name = name.replace("ELEKTRIK", "ELEKTRİK").replace("KIMYA", "KİMYA")
        name = name.replace("TURIZM", "TURİZM").replace("ITHALAT", "İTHALAT")
        name = name.replace("IHRACAT", "İHRACAT").replace("ISLETME", "İŞLETME")
        name = name.replace("ISLETMELERI", "İŞLETMELERİ")
        name = name.replace("DENIZCILIK", "DENİZCİLİK")
        name = name.replace("HIZMETLERI", "HİZMETLERİ")
        if "A.Ş." not in name and "AŞ" not in name:
            name += " A.Ş."
        if 5 <= len(name) <= 200:
            return name
    return None


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


async def _fetch_pdf_text(pdf_url: str, client: httpx.AsyncClient, kap_url: Optional[str] = None) -> Optional[str]:
    """KAP PDF'i indirip pdfplumber ile text extract et.

    KAP Referer + cookie warmup gerektiriyor — yoksa 404.
    """
    try:
        # Referer header KAP sayfasini gostermeli — yoksa KAP 404 doner
        pdf_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/pdf,*/*",
            "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
        }
        if kap_url:
            pdf_headers["Referer"] = kap_url
            # Cookie warmup — KAP sayfasini ziyaret et (cookie alir)
            try:
                await client.get(kap_url, headers={"User-Agent": pdf_headers["User-Agent"]}, timeout=15.0)
            except Exception:
                pass
        resp = await client.get(pdf_url, headers=pdf_headers, timeout=20.0)
        if resp.status_code != 200 or not resp.content:
            return None
        if "pdf" not in (resp.headers.get("content-type") or "").lower():
            return None
        import io
        import pdfplumber
        text_parts: list[str] = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages[:5]:  # max 5 sayfa
                t = page.extract_text() or ""
                if t:
                    text_parts.append(t)
        return "\n".join(text_parts)
    except Exception as e:
        logger.warning("PDF parse hata (%s): %s", pdf_url, e)
        return None


def parse_pdf_pay_alim_satim(text: str) -> Optional[dict]:
    """KAP PDF text'inden Pay Alım Satım structured data cikar.

    PDF formatI (örnek GEN İLAÇ 1600871):
        Bildirime Konu Borsa Şirketi : GEN İLAÇ VE SAĞLIK ÜRÜNLERİ SANAYİ VE TİCARET A.Ş.
        Ad Soyad / Ticaret Ünvanı : ŞÜKRÜ TÜRKMEN
        Açıklama: 04.05.2026 ... 8,7272 TL ortalama fiyat ile 290.978 TL toplam nominal alış işlemi
        Tablo: 04/05/2026 290.978 0 290.978 50.957.677 51.248.655 1,13 0,65 1,14 0,66
    """
    if not text:
        return None
    out: dict = {}

    # Ticker (köşe parantezli) — body'den
    m = re.search(r"\[([A-Z]{2,6})(?:\s*,|\])", text)
    if m:
        out["ticker"] = m.group(1).strip()

    # Bildirimi yapan kişi (party)
    party = None
    m = re.search(r"Ad\s+Soyad\s*/\s*Ticaret\s+[ÜU]nvan[ıi]\s*:?\s*([^\n\r]{3,150}?)\s*(?:T[üu]zel|G[öo]rev|$)", text, re.IGNORECASE)
    if m:
        party = m.group(1).strip().rstrip(".,;:")

    # Bildirime Konu Borsa Şirketi (ticker firma adı)
    company = None
    m = re.search(r"Bildirime\s+Konu\s+Borsa\s+[ŞS]irketi\s*:?\s*([^\n\r]{3,200}?)\s*(?:A\.\s*[ŞS]\.|A[ŞS]\.|A\.[ŞS])", text, re.IGNORECASE)
    if m:
        company = (m.group(1).strip() + " A.Ş.").strip()

    # Body içinde party (Ad Soyad fail olursa)
    if not party:
        m = re.search(r"([A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü\s\.]{4,80}?)\s+taraf[ıi]ndan\s+Kurulu[şs]umuza", text)
        if m:
            party = m.group(1).strip()
    out["party_name"] = party
    out["company_name"] = company
    out["body_text"] = text[:2000]

    # Fiyat (Ortalama fiyat / aralık)
    out["price_low"], out["price_high"] = extract_price_range(text)
    # Body'de "8,7272 TL ortalama fiyat" gibi tek fiyat
    if not out["price_low"]:
        m = re.search(r"(\d{1,4}[,]\d{1,4})\s*TL\s+ortalama\s+fiyat", text, re.IGNORECASE)
        if m:
            v = _parse_tr_decimal(m.group(1))
            if v:
                out["price_low"] = out["price_high"] = v

    # Tablo: PDF text'te bir satirda 10 sayi (tarih + 9)
    # 04/05/2026 290.978 0 290.978 50.957.677 51.248.655 1,13 0,65 1,14 0,66
    # Tek satir veya cok satir olabilir
    table_pat = re.compile(
        r"(\d{2}[/.]\d{2}[/.]\d{4})\s+"
        r"([\d.]+(?:,\d+)?|0)\s+([\d.]+(?:,\d+)?|0)\s+([\d.]+(?:,\d+)?|0)\s+"
        r"([\d.]+(?:,\d+)?|0)\s+([\d.]+(?:,\d+)?|0)\s+"
        r"(\d+(?:,\d+)?)\s+(\d+(?:,\d+)?)\s+(\d+(?:,\d+)?)\s+(\d+(?:,\d+)?)"
    )
    m = table_pat.search(text)
    if m:
        out["transaction_date"] = _parse_kap_date(m.group(1))
        out["alim_nominal"] = _parse_tr_decimal(m.group(2))
        out["satim_nominal"] = _parse_tr_decimal(m.group(3))
        out["net_nominal"] = _parse_tr_decimal(m.group(4))
        out["beginning_nominal"] = _parse_tr_decimal(m.group(5))
        out["end_nominal"] = _parse_tr_decimal(m.group(6))
        out["beginning_pay_oran_pct"] = _parse_tr_decimal(m.group(7))
        out["beginning_oy_hakki_pct"] = _parse_tr_decimal(m.group(8))
        out["end_pay_oran_pct"] = _parse_tr_decimal(m.group(9))
        out["end_oy_hakki_pct"] = _parse_tr_decimal(m.group(10))
    return out


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
        decoded_html = None
        try:
            from app.scrapers.kap_disclosure_extractor import _decode_rsc_chunks
            decoded_html = _decode_rsc_chunks(html)
        except Exception as de:
            logger.debug("RSC decode hata: %s", de)

        result = None
        if decoded_html and "<td" in decoded_html.lower():
            result = parse_kap_html(decoded_html)
        if result is None:
            # Fallback: raw HTML
            result = parse_kap_html(html)
        if result is None:
            result = {"body_text": _extract_body_text(decoded_html or html), "_no_table": True}
        # Header'dan party (slug bazli) — body fail olursa kullanilacak
        result["party_name_header"] = extract_party_from_html_header(decoded_html or html)

        # PDF FALLBACK: HTML'de tablo yoksa veya alim/satim nominal eksikse PDF'e bak
        no_table = result.get("_no_table", False)
        no_alim = (result.get("alim_nominal") in (None, 0)) and (result.get("satim_nominal") in (None, 0))
        if no_table or no_alim:
            try:
                # KAP PDF URL'lerini bul (extractor body'sinde mevcut)
                from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
                disc_full = await fetch_kap_disclosure(url)
                pdf_links = (disc_full or {}).get("pdf_links") or []
                # /api/file/download/ formatini tercih et (gercek PDF)
                pdf_url = None
                for p in pdf_links:
                    if "/api/file/download/" in p:
                        pdf_url = p; break
                if not pdf_url and pdf_links:
                    pdf_url = pdf_links[0]
                # KAP /tr/ prefix gerektiriyor — yoksa 404 doner
                if pdf_url and "/tr/api/" not in pdf_url:
                    pdf_url = pdf_url.replace("kap.org.tr/api/", "kap.org.tr/tr/api/")
                if pdf_url:
                    pdf_text = await _fetch_pdf_text(pdf_url, client, kap_url=url)
                    if pdf_text:
                        pdf_parsed = parse_pdf_pay_alim_satim(pdf_text)
                        if pdf_parsed:
                            # Boş alanlari PDF'ten doldur
                            for k, v in pdf_parsed.items():
                                if v is not None and (result.get(k) in (None, 0)):
                                    result[k] = v
                            logger.info("PDF parse ile pay_alim_satim alanlari dolduruldu: %s", url)
            except Exception as pe:
                logger.debug("PDF fallback hata: %s", pe)
        return result
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

    # Transaction type + net nominal (her ikisi de varsa NET = alim - satim)
    alim = parsed.get("alim_nominal") or 0
    satim = parsed.get("satim_nominal") or 0
    net = alim - satim
    if alim > 0 and satim > 0:
        # Hem alim hem satim var → net miktar gosterilir
        tx_type = "alis" if net >= 0 else "satis"
        nominal_lot = int(abs(net))
    elif alim > 0 and satim == 0:
        tx_type = "alis"
        nominal_lot = int(alim)
    elif satim > 0 and alim == 0:
        tx_type = "satis"
        nominal_lot = int(satim)
    else:
        tx_type = "alis"  # default
        nominal_lot = 0
    body = parsed.get("body_text") or ""
    # Tablo yoksa body'den nominal cek
    if not nominal_lot:
        nb = extract_nominal_from_body(body)
        if nb:
            nominal_lot = nb

    # Sira: 0) PDF party, 1) HTML header slug (KAP "Bildirimi Yapan"), 2) Ortagi pattern, 3) generic, 4) Bilinmiyor
    party_name = parsed.get("party_name")  # PDF'ten gelmis olabilir
    import re as _re
    # 1) Header slug — KAP'in resmi "Bildirimi Yapan" alani (PDF yoksa en guvenilir kaynak)
    if not party_name:
        hdr = parsed.get("party_name_header")
        if hdr and ticker:
            hdr_collapsed = _re.sub(r"[^A-ZÇĞİÖŞÜ]", "", hdr.upper())
            # Ticker'in kendi sirketi degilse header'i kullan
            if ticker not in hdr_collapsed[:len(ticker)+3]:
                party_name = hdr
        elif hdr:
            party_name = hdr
    # 2) Ortagi/kurucu/araciliyla — KAP body'nin standart kalibi
    body_clean = body.replace("\xa0", " ")
    for pat in [
        # "Pardus Portföy Yönetimi AŞ.nin kurucusu olduğu" -> Pardus Portföy Yönetimi
        r"([A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü\s\.\-&]{4,80}?)\s*(?:A\.\s*Ş\.?|AŞ\.?)['’]?\s*(?:ni[nm]|in)\s+kurucusu",
        # "Ortağı X A.Ş.'den gelen yazı" -> X
        r"Ortağı\s+([A-ZÇĞİÖŞÜ][^'’\.]{4,120}?(?:A\.\s*Ş\.?|AŞ|Holding|Ltd\.?))['’]?(?:den|dan)\s+gelen",
        # "X kurucu" generic
        r"([A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü\s\.\-&]{4,80}?\s+(?:A\.\s*Ş\.?|AŞ\.?|Holding))['’]?(?:nin|nın|in|ın)\s+kurucu",
    ]:
        m = _re.search(pat, body_clean, _re.IGNORECASE)
        if m:
            party_name = m.group(1).strip().rstrip(".,;")
            if "A.Ş" not in party_name and "AŞ" not in party_name and "Holding" not in party_name:
                party_name += " A.Ş."
            break
    # 3) Generic body pattern (eski extract_party_name) — sadece kısa match'ler
    if not party_name:
        cand = extract_party_name(body_clean)
        if cand and len(cand) < 80 and not _re.match(r"^(TL|adet|payın|oranı)", cand, _re.IGNORECASE):
            party_name = cand
    # Junk filter — aciklama metni / aciklama parcasi gibi yakalanmislari at
    if party_name:
        junk_markers = [
            "toplam nominal tutarlı",
            "tutarlı alış işlemi",
            "tutarlı satış işlemi",
            "alış işlemi",
            "satış işlemi",
        ]
        if any(m in party_name.lower() for m in junk_markers):
            party_name = None

    # 4) Son care fallback — NOT NULL constraint için
    if not party_name:
        party_name = "Bilinmiyor"

    # Body'den / PDF'ten fiyat aralığı (PDF onceligi)
    price_low = parsed.get("price_low")
    price_high = parsed.get("price_high")
    if price_low is None or price_high is None:
        pl, ph = extract_price_range(body)
        price_low = price_low or pl
        price_high = price_high or ph

    # Tx type body fallback
    if not (alim or satim):
        body_lower = body.lower()
        if "alış" in body_lower or "alis " in body_lower or "satin alma" in body_lower:
            tx_type = "alis"
        elif "satış" in body_lower or "satis " in body_lower:
            tx_type = "satis"

    # Oranlar — gun sonu degerleri "current"
    end_pay = parsed.get("end_pay_oran_pct")
    end_oy = parsed.get("end_oy_hakki_pct")
    beg_pay = parsed.get("beginning_pay_oran_pct")
    beg_oy = parsed.get("beginning_oy_hakki_pct")
    pay_change = (end_pay - beg_pay) if (end_pay is not None and beg_pay is not None) else None
    oy_change = (end_oy - beg_oy) if (end_oy is not None and beg_oy is not None) else None

    # CRITICAL: tx_type override — oran degisimi yonu en guvenilir sinyal
    # +pay_change/oy_change → ortak hisse aldi (alis), -change → satis
    _change_signal = pay_change if pay_change is not None else oy_change
    if _change_signal is not None and abs(_change_signal) > 1e-6:
        tx_type = "alis" if _change_signal > 0 else "satis"

    tx_date = parsed.get("transaction_date") or (published_at.date() if published_at else None)
    if not tx_date:
        return False

    # GUARD: party "Bilinmiyor" + nominal=0 + oy/pay None ise = hicbir gercek veri yok
    # PDF parse fail olmus demektir, placeholder kayit yapma
    has_real_data = bool(
        nominal_lot or end_pay or end_oy or
        (party_name and party_name != "Bilinmiyor")
    )
    if not has_real_data:
        logger.info("Pay alim satim: %s icin gercek veri yok (PDF parse fail?), kayit atlandi", ticker)
        return False

    # UPSERT — once kap_url'a gore ara (ayni KAP iki kez insert olmasin)
    check = await db.execute(sa_text("""
        SELECT id FROM share_transaction_details
        WHERE kap_url=:kap
        ORDER BY id DESC LIMIT 1
    """), {"kap": kap_url})
    existing_id = check.scalar()
    # Eski kayit ayni gun + ayni party varsa onu da yakala (kap_url eksik olabilir)
    if not existing_id:
        check2 = await db.execute(sa_text("""
            SELECT id FROM share_transaction_details
            WHERE ticker=:tk AND transaction_date=:dt
              AND COALESCE(party_name,'')=:pn
            ORDER BY id DESC LIMIT 1
        """), {"tk": ticker, "dt": tx_date, "pn": party_name or ""})
        existing_id = check2.scalar()
    # Hala yok? Ayni gun + "?"/Bilinmiyor placeholder kayit varsa onu OVERWRITE et
    if not existing_id:
        check3 = await db.execute(sa_text("""
            SELECT id FROM share_transaction_details
            WHERE ticker=:tk AND transaction_date=:dt
              AND (party_name IN ('?', 'Bilinmiyor', '') OR party_name IS NULL)
            ORDER BY id DESC LIMIT 1
        """), {"tk": ticker, "dt": tx_date})
        existing_id = check3.scalar()

    if existing_id:
        await db.execute(sa_text("""
            UPDATE share_transaction_details
            SET transaction_type=:tt, party_name=:pn,
                nominal_lot=:lot,
                price_low=COALESCE(:plo, price_low),
                price_high=COALESCE(:phi, price_high),
                oy_hakki_pct=:oy, oy_hakki_change_pct=:oyc,
                pay_orani_pct=:po, pay_orani_change_pct=:poc,
                kap_url=:kap, kap_disclosure_id=COALESCE(:did, kap_disclosure_id),
                source='kap_auto', raw_excerpt=:raw
            WHERE id=:id
        """), {
            "id": existing_id, "tt": tx_type, "pn": party_name,
            "lot": nominal_lot or None,
            "plo": price_low, "phi": price_high,
            "oy": end_oy, "oyc": oy_change, "po": end_pay, "poc": pay_change,
            "kap": kap_url, "did": disclosure_id, "raw": body[:1000],
        })
    else:
        await db.execute(sa_text("""
            INSERT INTO share_transaction_details(ticker, transaction_date,
                transaction_type, party_name, nominal_lot,
                price_low, price_high,
                oy_hakki_pct, oy_hakki_change_pct,
                pay_orani_pct, pay_orani_change_pct,
                kap_url, kap_disclosure_id, source, raw_excerpt, created_at)
            VALUES(:tk, :dt, :tt, :pn, :lot, :plo, :phi, :oy, :oyc, :po, :poc,
                   :kap, :did, 'kap_auto', :raw, NOW())
        """), {
            "tk": ticker, "dt": tx_date, "tt": tx_type, "pn": party_name,
            "lot": nominal_lot or None,
            "plo": price_low, "phi": price_high,
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
