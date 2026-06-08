"""Tipe Dönüşüm BAŞVURUSU (Pay Satış Bilgi Formu) işleyici.

Tip 2 bildirimi: ortak, imtiyazlı/borsada işlem GÖRMEYEN payını "borsada işlem gören
niteliğe dönüştürmek / borsada satışa konu etmek" için SPK'ya BAŞVURUR. Henüz
gerçekleşmedi → gelecekteki ARZ sinyali (negatif, orana göre).

Gerçekleşen dönüşümlerden (Tip 1, kişi+lot) AYRIDIR; aynı tabloda record_type='basvuru'
ile saklanır. Frontend "Başvurular" sekmesinde gösterilir.
"""

import logging
import re
from datetime import date, datetime

from sqlalchemy import select
from app.models.share_type_conversion import ShareTypeConversion

logger = logging.getLogger(__name__)

# Başlık genelde generic ("Özel Durum Açıklaması (Genel)") olduğu için BODY/EK içeriğine bakılır.
_APP_KEYWORDS = [
    "pay satış bilgi formu", "pay satis bilgi formu",
    "borsada işlem gören nitel", "borsada islem goren nitel",
    "niteliğe dönüştür", "nitelige donustur", "niteliğe donus", "nitelige dönüş",
    "borsada satışa konu", "borsada satisa konu",
]


def is_conversion_application(title: str | None, body: str | None) -> bool:
    """Tipe dönüşüm BAŞVURUSU (pay satış bilgi formu) mu?"""
    t = ((title or "") + " " + (body or "")).lower().replace("İ", "i").replace("I", "ı")
    return any(k in t for k in _APP_KEYWORDS)


def _parse_tr_number(s: str) -> float | None:
    """'10.000.000' / '10.000.000,50' → 10000000.0 (TR binlik nokta, ondalık virgül)."""
    if not s:
        return None
    s = s.strip().replace(" ", "")
    # virgül ondalık → noktayı binlik kabul et
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(".", "")
    try:
        return float(s)
    except Exception:
        return None


def parse_application(body: str) -> dict:
    """EK/body metninden oran (%), nominal (TL) ve satıcı tarafı çıkar.

    Döner: {"ratio_pct": float|None, "nominal_tl": float|None, "seller": str|None}
    """
    out = {"ratio_pct": None, "nominal_tl": None, "seller": None}
    if not body:
        return out
    txt = body.replace("\n", " ")
    low = txt.lower()

    # Oran: "%5'ine tekabül" / "sermayesinin %5" / "%5,00 oranında"
    m = re.search(r"%\s*(\d{1,3}(?:[.,]\d+)?)\s*(?:'?[ie]ne|'?ne|oran|tekab|sine|sına)", low)
    if not m:
        m = re.search(r"sermaye\w*\s+(?:olan\s+)?%\s*(\d{1,3}(?:[.,]\d+)?)", low)
    if not m:
        m = re.search(r"%\s*(\d{1,2}(?:[.,]\d+)?)", low)  # son çare: ilk makul %
    if m:
        try:
            out["ratio_pct"] = float(m.group(1).replace(",", "."))
        except Exception:
            pass

    # Nominal: "10.000.000 TL nominal" / "nominal değerli 10.000.000 TL"
    m2 = re.search(r"([\d.]+(?:,\d+)?)\s*tl\s*nominal", low)
    if not m2:
        m2 = re.search(r"nominal\s*değerli\s*([\d.]+(?:,\d+)?)\s*tl", low)
    if not m2:
        m2 = re.search(r"nominal\s*değeri\s*([\d.]+(?:,\d+)?)\s*tl", low)
    if m2:
        out["nominal_tl"] = _parse_tr_number(m2.group(1))

    # Satıcı taraf: "<X A.Ş.> tarafından" — "tarafından"dan hemen önceki büyük-harfli
    # kelime zinciri (en fazla 5 kelime) + A.Ş. (önceki cümleyi kapmasın diye lowercase
    # bağlaçlar zinciri kırar: "... ile ilgili olarak Ege Yapı A.Ş. tarafından" → "Ege Yapı A.Ş.")
    ms = re.search(
        r"((?:[A-ZÇĞİÖŞÜ][\wçğıöşüÇĞİÖŞÜ&]*\s+){1,5}A\.?\s*Ş\.?)\s+tarafından", txt)
    if ms:
        out["seller"] = re.sub(r"\s+", " ", ms.group(1)).strip()
    return out


async def process_conversion_application(
    session, *, ticker: str, company_name: str | None,
    title: str | None, body: str | None, kap_url: str | None,
    published_at=None, ai_summary: str | None = None,
) -> dict | None:
    """Başvuruyu share_type_conversions'a record_type='basvuru' olarak yaz."""
    if not is_conversion_application(title, body):
        return None
    parsed = parse_application(body or "")
    tx_date = (published_at.date() if isinstance(published_at, datetime)
               else (published_at if isinstance(published_at, date) else date.today()))
    seller = parsed.get("seller") or (company_name or "Pay sahibi")
    try:
        # Aynı (kap_url, ticker, investor) varsa güncelle
        existing = None
        if kap_url:
            existing = (await session.execute(
                select(ShareTypeConversion).where(
                    ShareTypeConversion.kap_url == kap_url,
                    ShareTypeConversion.ticker == ticker.upper(),
                    ShareTypeConversion.investor_name == seller,
                )
            )).scalars().first()
        if existing:
            existing.record_type = "basvuru"
            existing.ratio_pct = parsed.get("ratio_pct")
            existing.nominal_tl = parsed.get("nominal_tl")
            existing.transaction_date = tx_date
            if ai_summary:
                existing.ai_summary = ai_summary
        else:
            session.add(ShareTypeConversion(
                ticker=ticker.upper(), company_name=company_name,
                transaction_date=tx_date, investor_name=seller,
                converted_lot=None, kap_url=kap_url, source="kap_basvuru",
                record_type="basvuru",
                ratio_pct=parsed.get("ratio_pct"),
                nominal_tl=parsed.get("nominal_tl"),
                ai_summary=ai_summary,
            ))
        await session.flush()
        logger.info("Tipe dönüşüm BAŞVURU işlendi: %s — %%%s / %s TL nominal (%s)",
                    ticker, parsed.get("ratio_pct"), parsed.get("nominal_tl"), seller)
        return parsed
    except Exception as e:
        logger.warning("Tipe dönüşüm başvuru işleme hata (%s): %s", ticker, e)
        return None
