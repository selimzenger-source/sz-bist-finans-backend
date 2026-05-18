"""KAP Pay Geri Alımı (Buyback) Processor.

KAP "Payların Geri Alınmasına İlişkin Bildirim" tipi:
  - Şirket kendi paylarını borsadan geri alır
  - Body örneği:
    "04.05.2026 (bugün) tarihinde Borsa İstanbul A.Ş. nezdinde pay başına
     26,90 TL – 27,00 TL fiyat aralığından (ağırlıklı ortalama 26,988 TL)
     toplam 227.291 adet pay geri alınmıştır."
  - Tablo: tarihler + nominal lot + sermayeye oran + işlem fiyatı

Hedef: share_transaction_details tablosuna kaydet — party_name="Şirket Geri Alımı",
       transaction_type="geri_alim", lot/fiyat/oran dolu.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Optional

from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


_BUYBACK_TITLE_PATTERNS = [
    "payların geri alın", "paylarn geri alin",
    "pay geri alım", "pay geri alim",
    "geri alım programı", "geri alim programi",
    "geri alım işlem", "geri alim islem",
]


def is_buyback(title: str) -> bool:
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _BUYBACK_TITLE_PATTERNS)


# Pattern 1 (klasik): "DD.MM.YYYY (bugün) tarihinde ... pay başına X TL – Y TL ... toplam N adet pay geri alın"
_TODAY_RE = re.compile(
    r"(\d{1,2}\.\d{1,2}\.\d{4})\s*(?:\([^)]*\))?\s*tarihinde[^.]*?"
    r"pay\s+başına\s+([\d.,]+)\s*TL\s*[–-]\s*([\d.,]+)\s*TL[^.]*?"
    r"(?:ağırlıklı\s+ortalama\s+([\d.,]+)\s*TL[^.]*?)?"
    r"toplam\s+([\d.,]+)\s*adet\s+pay\s+geri\s+al",
    re.IGNORECASE | re.DOTALL,
)

# Pattern 2 (gevsek): "DD.MM.YYYY tarihinde X TL – Y TL fiyat ... N adet"
# "pay başına" zorunlu degil. GLYHO Ek Aciklamalar format'i icin.
_TODAY_RE_LOOSE = re.compile(
    r"(\d{1,2}\.\d{1,2}\.\d{4})\s*(?:\([^)]*\))?\s*tarihinde[^.]*?"
    r"([\d.,]+)\s*TL\s*[–-]\s*([\d.,]+)\s*TL[^.]*?"
    r"(?:fiyat[ \w]*?)?[^.]*?"
    r"([\d.,]+)\s*adet[^.]*?(?:pay|sirket(?:imiz)?\s*pay[ıi])",
    re.IGNORECASE | re.DOTALL,
)

# Pattern 3 (cok gevsek): Ek Aciklamalar bolumunden "N adet ... pay geri alin" formati
_LOT_FALLBACK_RE = re.compile(
    r"([\d.,]+)\s*adet[^.]*?(?:pay\s+geri\s+al|sirket(?:imiz)?\s*pay[ıi]\s+geri\s+al)",
    re.IGNORECASE | re.DOTALL,
)
_PRICE_FALLBACK_RE = re.compile(
    r"([\d.,]+)\s*TL\s*[–-]\s*([\d.,]+)\s*TL",
    re.IGNORECASE,
)

# Tablo satır pattern: "B Grubu, AHGAZ, TREAHLA00019 04.05.2026 227.291 0,00874 26,988"
# RSC body'de pipe ayraçlı: "B Grubu, AHGAZ | 04.05.2026 | 227.291 | 0,00874 | 26,988"
_ROW_RE = re.compile(
    r"([A-Z]{2,6})\s*,?\s*[A-Z]*\s*\|?\s*(\d{1,2}\.\d{1,2}\.\d{4})\s*\|?\s*"
    r"([\d.]+(?:,\d+)?)\s*\|?\s*([\d.,]+)\s*\|?\s*([\d.,]+)",
)


def _parse_tr_num(s: str) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(" ", "")
    if "," in s:
        int_part, dec_part = s.rsplit(",", 1)
        int_part = int_part.replace(".", "")
        try:
            return float(f"{int_part}.{dec_part}")
        except ValueError:
            return None
    s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_dt(s: str) -> Optional[date]:
    for fmt in ("%d.%m.%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _extract_ek_aciklamalar(body: str) -> str:
    """Body'den 'Ek Açıklamalar' bolumunu cikar.

    KAP form'unda 'Ek Açıklamalar' baslıgindan sonra gercek bugunku islem
    aciklamasi gelir. Uzun tablolarin AI'yi karistirmasini onlemek icin
    sadece bu bolum yorum/parse'a girer.
    """
    if not body:
        return ""
    # 'Ek Açıklamalar' veya benzeri header sonrasi metin
    m = re.search(
        r"Ek\s*A[çc][ıi]klama(?:lar)?[\s:|]*([\s\S]+?)(?:Y(?:uk|uk)ar[ıi]daki|İmzal[ıi]\s*G[öo]r[üu]nt[üu]le|\Z)",
        body, re.IGNORECASE,
    )
    if m:
        return m.group(1).strip()[:3000]
    return ""


def parse_buyback_today(body: str) -> Optional[dict]:
    """Body'den BUGÜNKÜ geri alım işlemini çıkar.

    Returns: {transaction_date, lot, price_low, price_high, price_avg}
    """
    if not body:
        return None

    # Once Ek Aciklamalar bolumune odakla (uzun tablodan kacin)
    ek = _extract_ek_aciklamalar(body)
    search_target = ek if ek else body

    # Pattern 1: klasik "pay başına X TL"
    m = _TODAY_RE.search(search_target)
    if m:
        return {
            "transaction_date": _parse_dt(m.group(1)),
            "price_low": _parse_tr_num(m.group(2)),
            "price_high": _parse_tr_num(m.group(3)),
            "price_avg": _parse_tr_num(m.group(4)) if m.group(4) else None,
            "lot": int(_parse_tr_num(m.group(5)) or 0) or None,
        }
    # Pattern 2: gevsek (Ek Aciklamalar tipik formati)
    m2 = _TODAY_RE_LOOSE.search(search_target)
    if m2:
        return {
            "transaction_date": _parse_dt(m2.group(1)),
            "price_low": _parse_tr_num(m2.group(2)),
            "price_high": _parse_tr_num(m2.group(3)),
            "price_avg": None,
            "lot": int(_parse_tr_num(m2.group(4)) or 0) or None,
        }
    # Pattern 3: en gevsek — sadece lot + fiyat yakala
    m_lot = _LOT_FALLBACK_RE.search(search_target)
    if m_lot:
        m_price = _PRICE_FALLBACK_RE.search(search_target)
        return {
            "transaction_date": date.today(),
            "price_low": _parse_tr_num(m_price.group(1)) if m_price else None,
            "price_high": _parse_tr_num(m_price.group(2)) if m_price else None,
            "price_avg": None,
            "lot": int(_parse_tr_num(m_lot.group(1)) or 0) or None,
        }
    return None


async def process_buyback(
    db: AsyncSession,
    *,
    ticker: str,
    body: str,
    kap_url: Optional[str],
    disclosure_id: Optional[int],
    published_at: Optional[datetime] = None,
) -> Optional[dict]:
    """KAP buyback bildirimini share_transaction_details'a kaydet.

    Sadece BUGÜNKÜ işlemi yazar (tabloda eski tarihler varsa onları atla).
    Returns: parsed buyback data (lot/price/total_tl) ya da None.
    """
    parsed = parse_buyback_today(body or "")
    if not parsed or not parsed.get("transaction_date"):
        logger.info("Buyback parse fail: %s", ticker)
        return None

    tx_date = parsed["transaction_date"]
    lot = parsed.get("lot")
    price_low = parsed.get("price_low")
    price_high = parsed.get("price_high")
    price_avg = parsed.get("price_avg")
    party = "Şirket Geri Alımı"

    # UPSERT — ticker + tx_date + party kombinasyonu
    check = await db.execute(sa_text("""
        SELECT id FROM share_transaction_details
        WHERE ticker=:tk AND transaction_date=:dt AND party_name=:pn
        LIMIT 1
    """), {"tk": ticker.upper(), "dt": tx_date, "pn": party})
    existing_id = check.scalar()

    payload = {
        "tk": ticker.upper(),
        "dt": tx_date,
        "tt": "geri_alim",
        "pn": party,
        "lot": lot,
        "pl": price_low,
        "ph": price_high,
        "kap": kap_url,
        "did": disclosure_id,
        "raw": (body or "")[:1000],
    }

    if existing_id:
        await db.execute(sa_text("""
            UPDATE share_transaction_details
            SET transaction_type=:tt, nominal_lot=:lot,
                price_low=:pl, price_high=:ph,
                kap_url=:kap, kap_disclosure_id=COALESCE(:did, kap_disclosure_id),
                source='kap_buyback', raw_excerpt=:raw
            WHERE id=:id
        """), {**payload, "id": existing_id})
    else:
        await db.execute(sa_text("""
            INSERT INTO share_transaction_details(
                ticker, transaction_date, transaction_type, party_name,
                nominal_lot, price_low, price_high,
                kap_url, kap_disclosure_id, source, raw_excerpt, created_at
            ) VALUES(:tk, :dt, :tt, :pn, :lot, :pl, :ph, :kap, :did, 'kap_buyback', :raw, NOW())
        """), payload)

    logger.info("Buyback islendi: %s %s lot=%s avg=%s", ticker, tx_date, lot, price_avg)
    # TL tutar hesabi — fallback skor icin
    total_tl = None
    if lot and price_avg:
        total_tl = lot * price_avg
    elif lot and price_low and price_high:
        total_tl = lot * ((price_low + price_high) / 2)
    return {
        "transaction_date": tx_date,
        "lot": lot,
        "price_low": price_low,
        "price_high": price_high,
        "price_avg": price_avg,
        "total_tl": total_tl,
    }


def buyback_score_and_summary(parsed: dict, ticker: str) -> tuple[float, str]:
    """Buyback TL tutarina gore deterministik skor + standart ozet uret.

    Esikler:
      < 1M TL       -> 5.0  (sembolik, Notr)
      1M - 10M TL   -> 5.5  (gostermelik, Notr)
      10M - 50M TL  -> 6.3  (Hafif Olumlu)
      50M - 200M TL -> 7.0  (Olumlu)
      200M - 1B TL  -> 7.8  (Olumlu/Cok Olumlu sinir)
      > 1B TL       -> 8.3  (Cok Olumlu — sirket guveni)
    """
    total_tl = parsed.get("total_tl") or 0
    lot = parsed.get("lot") or 0
    price_avg = parsed.get("price_avg") or 0
    if total_tl < 1_000_000:
        score = 5.0
        tier = "sembolik"
    elif total_tl < 10_000_000:
        score = 5.5
        tier = "gostermelik"
    elif total_tl < 50_000_000:
        score = 6.3
        tier = "anlamli"
    elif total_tl < 200_000_000:
        score = 7.0
        tier = "buyuk"
    elif total_tl < 1_000_000_000:
        score = 7.8
        tier = "cok_buyuk"
    else:
        score = 8.3
        tier = "guclu_guven"

    # TR formatli sayi
    def _fmt(n: float | None) -> str:
        if not n:
            return "?"
        if n >= 1_000_000_000:
            return f"{n/1_000_000_000:.1f} milyar"
        if n >= 1_000_000:
            return f"{n/1_000_000:.1f} milyon"
        if n >= 1_000:
            return f"{n/1_000:.0f} bin"
        return f"{n:.0f}"

    summary = (
        f"{ticker} bugün kendi paylarından {lot:,} lot geri aldı "
        f"(ortalama {price_avg:.2f} TL/pay, toplam ~{_fmt(total_tl)} TL). "
    )
    if score >= 7.8:
        summary += (
            "Bu çok büyük tutarlı bir geri alım — şirket yönetiminin paya olan "
            "güveni güçlü bir sinyal veriyor. Arzı azaltıcı etkiyle birlikte "
            "fiyat üzerinde olumlu baskı yaratabilir."
        )
    elif score >= 7.0:
        summary += (
            "Tutar büyüklüğü dikkat çekici — şirket güveni sinyali olarak "
            "yorumlanabilir, fiyat üzerinde destekleyici etki beklenir."
        )
    elif score >= 6.3:
        summary += (
            "Tutar anlamlı seviyede; şirket güveni gösteren bir adım, ancak "
            "tek başına büyük fiyat hareketi yaratacak ölçüde değil."
        )
    else:
        summary += (
            "Tutar görece küçük, sembolik nitelikte. Önceden duyurulan geri "
            "alım programının rutin günlük işlem bildirimidir."
        )
    return score, summary
