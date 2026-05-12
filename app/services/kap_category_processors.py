"""KAP'tan otomatik AI processor'lar — 3 kategori:

1. Toptan Alım Satım → BlockTrade
2. Borsada Tipe Dönüşüm → ShareTypeConversion
3. Tedbirli Hisseler → CautiousStock (KAP'ta tedbir kararları)

Pattern: title detect + Gemini AI body parse + DB insert.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.block_trade import BlockTrade
from app.models.share_type_conversion import ShareTypeConversion
from app.models.cautious_stock import CautiousStock
from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_AI_TIMEOUT = 30


def _get_gemini_key() -> Optional[str]:
    try:
        from app.config import get_settings
        return get_settings().GEMINI_API_KEY or None
    except Exception:
        return None


def _parse_json(text: str) -> Optional[dict]:
    if not text:
        return None
    if "```" in text:
        text = re.sub(r"```(?:json)?\s*", "", text).replace("```", "")
    s, e = text.find("{"), text.rfind("}")
    if s < 0 or e < 0:
        return None
    try:
        return json.loads(text[s:e + 1])
    except json.JSONDecodeError:
        return None


async def _call_gemini(prompt: str) -> Optional[dict]:
    key = _get_gemini_key()
    if not key:
        return None
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as c:
            r = await c.post(
                _GEMINI_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Yapilandirilmis JSON dondur. SADECE JSON."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1, "max_tokens": 1024,
                },
            )
            if r.status_code == 200:
                txt = r.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                return _parse_json(txt)
    except Exception as e:
        logger.warning("Gemini hata: %s", e)
    return None


# ═══════════════════════════════════════════════════════════════════
# 1. TOPTAN ALIM SATIM
# ═══════════════════════════════════════════════════════════════════

_BT_TITLE_PATTERNS = [
    # KAP gerçek başlıkları (production DB analizinden)
    # NOT: lower_tr 'I' → 'ı' (dotsuz) yaptığı için ASCII başlıklarda variant ekle.
    "toptan alış satış", "toptan alis satis", "toptan alıs satıs",
    "toptan satış", "toptan alış", "toptan alım",
    "toptan satıs", "toptan alıs", "toptan alım", "toptan alım",
    "toptan işlem", "toptan ışlem",
    "toptan satim", "toptan satım",
    "toptan alim satim", "toptan alım satım", "toptan alım satim",
    # Borsa dışı pay devri — toptan işlem niteliği taşır
    "borsa dışı pay devr", "borsa disi pay devr", "borsa dısı pay devr",
    "borsa dışında pay devr", "borsa disinda pay devr",
    "pay devri bildirimi", "pay devri bildirim",
    "block trade",
]

# Body içinde toptan/block trade sinyalleri — title generic ise (Pay Alım Satım
# Bildirimi gibi) body'de aranır. Multi-symbol bulk veya yanlış title durumunda
# da yakalayabilir.
_BT_BODY_PATTERNS = [
    "toptan alış satış", "toptan alis satis",
    "toptan alım satım", "toptan alim satim",
    "toptan satış işlemi", "toptan satis islemi",
    "toptan alış işlemi", "toptan alis islemi",
    "toptan işlem", "toptan islem",
    "toptan satış", "toptan satis",
    "toptan alış", "toptan alis",
    "toptan alım", "toptan alim",
    "borsa dışı pay devri", "borsa disi pay devri",
    "borsada işlem görmeyen pay", "borsada islem gormeyen pay",
    "borsa dışında gerçekleş", "borsa disinda gerceklesti",
    "block trade",
]

# Kombo pattern'lar — birden fazlasi birlikte gecerse block_trade kabul edilir.
# Tek başına "alıcılar:" share_transaction'da da olabilir; ancak "aracı kurum:" +
# "alıcılar:" + "lot miktarı:" + "maliyet fiyatı:" toptan alım satım iskeleti.
_BT_COMBO_PATTERNS = [
    "alıcılar:", "alicilar:",
    "satıcılar:", "saticilar:",
    "aracı kurum:", "araci kurum:",
    "lot miktarı:", "lot miktari:",
    "maliyet fiyatı:", "maliyet fiyati:",
    "lot miktari", "maliyet fiyati",
]


def is_block_trade(title: str, body: str = "") -> bool:
    """Toptan alim satim mi?

    Title'da kalip varsa direkt True. Title generic (örn. "Pay Alım Satım
    Bildirimi") ise body'de:
      a) "toptan alış satış" gibi tek başına güçlü sinyal varsa True
      b) Combo pattern'lardan en az 3 tanesi varsa True (alıcılar/satıcılar
         + aracı kurum + lot miktarı + maliyet fiyatı iskeleti)
    """
    if title:
        t = lower_tr(title)
        if any(p in t for p in _BT_TITLE_PATTERNS):
            return True
    if body:
        b = lower_tr(body)
        # Güçlü tek sinyal
        if any(p in b for p in _BT_BODY_PATTERNS):
            return True
        # Combo: en az 3 farklı kalıp birlikte → toptan iskeleti
        combo_hits = sum(1 for p in _BT_COMBO_PATTERNS if p in b)
        if combo_hits >= 3:
            return True
    return False


_BT_PROMPT = """KAP toptan alim satim bildirimini analiz et. JSON dondur:

Hisse: {ticker}
Baslik: {title}
Icerik: {body}

JSON:
{{
  "transaction_type": "alis" | "satis",
  "transaction_date": "YYYY-MM-DD",
  "broker": "Aracı kurum adi",
  "counterparties": "Alıcılar veya satıcılar listesi (virgulle ayrili tek string)",
  "lot_amount": <int sayi>,
  "cost_price": <sayi>
}}

KURALLAR: Bilinmeyenler null. SADECE JSON.
"""


def _parse_block_trade_regex(body: str) -> dict:
    """KAP toptan alim satim body'sinden REGEX ile alanlari cikar (AI fallback).

    Body kalibi (KAP form):
      İŞLEM TİPİ        Satış / Alış
      ARACI KURUM       İnfo Yatırım Menkul Değerler A.Ş.
      ALICILAR          Shield Capital Fund SPC
      SATICILAR         (varsa)
      LOT MİKTARI       17.272.728
      MALİYET FİYATI    71,00 TL
    """
    out: dict = {}
    if not body:
        return out
    b = body

    def _find(*labels: str, max_len: int = 300) -> Optional[str]:
        for lbl in labels:
            # Pattern: "LABEL ... değer" — değer label'dan sonraki anlamlı kelimeleri al
            m = re.search(
                rf"{re.escape(lbl)}[\s\:\|]+(.+?)(?=\n(?:[A-ZÇĞİÖŞÜ][A-ZÇĞİÖŞÜ\s/]{{4,40}}[\s\:\|]|$))",
                b, re.IGNORECASE | re.DOTALL,
            )
            if m:
                v = m.group(1).strip().rstrip(".|").strip()
                if v and len(v) < max_len:
                    return v
        return None

    # transaction_type
    tx_t = _find("İŞLEM TİPİ", "ISLEM TIPI", "Tip", max_len=20)
    if tx_t:
        tl = tx_t.lower()
        if "satış" in tl or "satis" in tl:
            out["transaction_type"] = "satis"
        elif "alış" in tl or "alis" in tl or "alım" in tl or "alim" in tl:
            out["transaction_type"] = "alis"

    # broker
    broker = _find("ARACI KURUM", "ARACI KURUMLAR")
    if broker:
        out["broker"] = broker[:255]

    # counterparties (alicilar veya saticilar)
    alicilar = _find("ALICILAR", "ALICI")
    saticilar = _find("SATICILAR", "SATICI")
    parts = [p for p in (alicilar, saticilar) if p]
    if parts:
        out["counterparties"] = " | ".join(parts)[:1000]

    # lot_amount: "17.272.728 Lot" veya "17.272.728"
    m_lot = re.search(r"LOT\s*M[İI]KTAR[İI][\s\:\|]+([\d\.\,]+)\s*(?:Lot)?", b, re.IGNORECASE)
    if m_lot:
        try:
            out["lot_amount"] = int(m_lot.group(1).replace(".", "").replace(",", ""))
        except ValueError:
            pass

    # cost_price: "71,00 TL" / "71.00 TL" / "71,00 ₺"
    m_price = re.search(r"MAL[İI]YET\s*F[İI]YAT[İI][\s\:\|]+([\d\.\,]+)\s*(?:TL|₺)?", b, re.IGNORECASE)
    if m_price:
        try:
            raw = m_price.group(1)
            # Turkce format: 1.234,56 → 1234.56
            if "," in raw and "." in raw:
                raw = raw.replace(".", "").replace(",", ".")
            elif "," in raw:
                raw = raw.replace(",", ".")
            out["cost_price"] = float(raw)
        except ValueError:
            pass

    # transaction_date — "12.05.2026" / "12/05/2026"
    m_date = re.search(r"(\d{2})[\./](\d{2})[\./](\d{4})", b)
    if m_date:
        try:
            out["transaction_date"] = f"{m_date.group(3)}-{m_date.group(2)}-{m_date.group(1)}"
        except Exception:
            pass

    return out


async def process_block_trade(
    db: AsyncSession, *, disclosure_id: int, ticker: str, company_name: Optional[str],
    title: str, body: Optional[str], kap_url: Optional[str], published_at: Optional[datetime],
) -> Optional[BlockTrade]:
    if not is_block_trade(title or "", body or ""):
        return None
    if disclosure_id:
        stmt = select(BlockTrade).where(BlockTrade.kap_url == kap_url).limit(1) if kap_url else select(BlockTrade).where(False)
        if kap_url and (await db.execute(stmt)).scalar_one_or_none():
            return None

    # AI parse
    parsed = await _call_gemini(_BT_PROMPT.format(ticker=ticker, title=title or "", body=(body or "")[:3500])) or {}

    # Regex fallback — AI'nın yakalayamadığı alanları KAP form yapısından al
    regex_parsed = _parse_block_trade_regex(body or "")
    # AI'da null/eksik olan her alan için regex'i kullan
    for k in ("transaction_type", "transaction_date", "broker", "counterparties", "lot_amount", "cost_price"):
        if not parsed.get(k) and regex_parsed.get(k):
            parsed[k] = regex_parsed[k]

    tx_type = parsed.get("transaction_type") if parsed.get("transaction_type") in ("alis", "satis") else "satis"
    tx_date = None
    if isinstance(parsed.get("transaction_date"), str):
        try:
            tx_date = date.fromisoformat(parsed["transaction_date"])
        except ValueError:
            pass
    if not tx_date:
        tx_date = published_at.date() if published_at else date.today()

    new_row = BlockTrade(
        ticker=ticker,
        company_name=company_name,
        transaction_date=tx_date,
        transaction_type=tx_type,
        broker=(parsed.get("broker") or None),
        counterparties=(parsed.get("counterparties") or None),
        lot_amount=(int(parsed["lot_amount"]) if isinstance(parsed.get("lot_amount"), (int, float)) else None),
        cost_price=(float(parsed["cost_price"]) if isinstance(parsed.get("cost_price"), (int, float)) else None),
        kap_url=kap_url,
        source="kap_ai_parse",
    )
    db.add(new_row)
    await db.flush()
    logger.info("BlockTrade: yeni (%s, %s)", ticker, tx_type)
    return new_row


# ═══════════════════════════════════════════════════════════════════
# 2. BORSADA TIPE DÖNÜŞÜM
# ═══════════════════════════════════════════════════════════════════

_TC_TITLE_PATTERNS = [
    # KAP gerçek başlıkları (production DB analizinden)
    "borsada işlem gören tipe dönüşüm",  # 11+ kayit
    "borsada işlem gören tipe",
    "borsada islem goren tipe",
    "tipe dönüşüm duyurusu",
    "tipe dönüş", "tipe donusum",
]


def is_type_conversion(title: str) -> bool:
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _TC_TITLE_PATTERNS)


_TC_PROMPT = """KAP borsada tipe donusum bildirimini analiz et. JSON dondur:

Hisse: {ticker}
Baslik: {title}
Icerik: {body}

JSON:
{{
  "transaction_date": "YYYY-MM-DD",
  "investor_name": "Yatırımcı kişi/şirket adı",
  "converted_lot": <int dönüştürülen lot>
}}

KURALLAR: Bilinmeyenler null. SADECE JSON.
"""


def _parse_tc_table(body: str) -> list[dict]:
    """KAP Tipe Dönüşüm tablosundan TÜM satırları çıkar.

    Tablo formatı (RSC decoded, " | " ayraçlı):
      Borsa Kodu | Sıra No | Pay Unvanı | Grubu | Yatırımcı | Nominal Tutar (TL)

    Örnek satır:
      CEMZY | 1 | CEM ZEYTİN A..Ş. | E | ERKAN AKTAŞ | 6.620.814,900

    Returns: [{"ticker", "company_name", "investor_name", "nominal_tl"}, ...]
    """
    if not body:
        return []

    # Pattern: <TICKER (3-6 büyük harf)> | <sira_no (1-3 digit)> | <şirket adı> | <grup> | <yatırımcı> | <nominal>
    # nominal: 6.620.814,900 / 13,000 / 100.000,000
    pattern = re.compile(
        r"\b([A-Z]{3,6})\s*\|\s*"            # Borsa Kodu
        r"(\d{1,4})\s*\|\s*"                   # Sıra No
        r"([^|]{3,200}?)\s*\|\s*"              # Pay Unvanı (şirket)
        r"([A-Z])\s*\|\s*"                     # Grubu (E/Y vb.)
        r"([^|]{2,100}?)\s*\|\s*"              # Yatırımcı
        r"([\d.]+(?:,\d+)?)"                   # Nominal Tutar
    )

    results = []
    seen = set()
    for m in pattern.finditer(body):
        ticker = m.group(1).strip().upper()
        company_name = m.group(3).strip().rstrip(".")
        investor = m.group(5).strip()
        nominal_raw = m.group(6).strip()
        # Nominal: "6.620.814,900" → 6620814.9
        try:
            nominal_val = float(nominal_raw.replace(".", "").replace(",", "."))
        except ValueError:
            nominal_val = None

        key = (ticker, m.group(2), investor)  # ticker + sıra + yatırımcı eşleştirme
        if key in seen:
            continue
        seen.add(key)

        results.append({
            "ticker": ticker,
            "company_name": company_name[:255],
            "investor_name": investor[:255],
            "nominal_tl": nominal_val,
            "row_no": int(m.group(2)),
        })

    return results


async def process_type_conversion(
    db: AsyncSession, *, disclosure_id: int, ticker: str, company_name: Optional[str],
    title: str, body: Optional[str], kap_url: Optional[str], published_at: Optional[datetime],
) -> Optional[list[ShareTypeConversion]]:
    """KAP Tipe Dönüşüm bildirimi → tablodaki TÜM satırları işle (multi-ticker, multi-investor)."""
    if not is_type_conversion(title):
        return None

    # Body yoksa RSC extractor'dan canli cek
    body_text = body or ""
    if (not body_text or len(body_text) < 200) and kap_url:
        try:
            from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
            disclosure = await fetch_kap_disclosure(kap_url)
            if disclosure and disclosure.get("full_text"):
                body_text = disclosure["full_text"]
        except Exception as e:
            logger.warning("TypeConversion body fetch hata: %s", e)

    # Tablodaki TÜM satırları parse et
    rows_data = _parse_tc_table(body_text)
    if not rows_data:
        # Tablo bulunamadı — eski tek-ticker AI fallback
        logger.warning("TypeConversion: tablo parse boş, AI fallback (%s)", kap_url)
        parsed = await _call_gemini(_TC_PROMPT.format(ticker=ticker, title=title or "", body=body_text[:3000])) or {}
        investor = parsed.get("investor_name") or "?"
        converted = parsed.get("converted_lot")
        rows_data = [{
            "ticker": ticker,
            "company_name": company_name or "",
            "investor_name": str(investor)[:255],
            "nominal_tl": float(converted) if isinstance(converted, (int, float)) else None,
            "row_no": 1,
        }]

    # Tarih
    tx_date = published_at.date() if published_at else date.today()

    inserted_rows: list[ShareTypeConversion] = []
    for d in rows_data:
        # Duplicate kontrolü: kap_url + ticker + investor + nominal kombinasyonu
        if kap_url:
            check = await db.execute(
                select(ShareTypeConversion).where(
                    ShareTypeConversion.kap_url == kap_url,
                    ShareTypeConversion.ticker == d["ticker"],
                    ShareTypeConversion.investor_name == d["investor_name"],
                ).limit(1)
            )
            if check.scalar_one_or_none():
                continue  # zaten var

        new_row = ShareTypeConversion(
            ticker=d["ticker"],
            company_name=d.get("company_name"),
            transaction_date=tx_date,
            investor_name=d["investor_name"],
            converted_lot=int(d["nominal_tl"]) if d.get("nominal_tl") else None,
            kap_url=kap_url,
            source="kap_table_parse",
        )
        db.add(new_row)
        inserted_rows.append(new_row)

    if inserted_rows:
        await db.flush()
        tickers = [r.ticker for r in inserted_rows]
        logger.info("TypeConversion: %d satir eklendi - tickers=%s", len(inserted_rows), tickers)

    return inserted_rows


# ═══════════════════════════════════════════════════════════════════
# 3. TEDBIRLI HISSELER (KAP)
# ═══════════════════════════════════════════════════════════════════

_CS_TITLE_PATTERNS = [
    # KAP gerçek başlıkları — sadece kalici tedbir/önlem kararları.
    # Devre kesici DAHİL DEĞİL (anlık trading halt, tedbir sayılmaz).
    "tedbir kararı", "tedbir karari",
    "spk tedbir", "sermaye piyasası kurulu tedbir",
    # NOT: lower_tr() ASCII 'I' harfini dotsuz 'ı'ya çevirdiği için "BISTECH"
    # → "bıstech" olur. İki yazımı da yakalamak için pattern'a ekleniyor.
    "bistech pay piyasası", "bıstech pay piyasası",
    "bistech pay piyasasi", "bıstech pay piyasasi",
    "borsa istanbul a.ş. duyuru", "borsa istanbul duyuru",
    "borsa ıstanbul a.ş. duyuru", "borsa ıstanbul duyuru",
    "brüt takas", "brut takas",
    "açığa satış", "aciga satis", "açığa satış yasağı",
    "kredili işlem", "kredili islem",
    "tedbir uygulan", "önlem uygulan", "onlem uygulan",
    "emir iptali", "piyasa emri", "tek fiyat",
    "tedbirli", "borsa istanbul tedbir",
    # YENI — VBTS başlıkları (kullanıcı bildirimi: KAP 1601348 ve sonrası)
    "volatilite bazlı tedbir", "volatilite bazli tedbir",
    "vbts kapsamında", "vbts kapsaminda",
    "pay piyasasında volatilite", "pay piyasasinda volatilite",
    " vbts ", "(vbts)",
    # Genel "tedbir" kelimesi başlıkta geçiyorsa yakala (devre kesici hariç tutulur)
    "tedbir sistemi",
]


def is_cautious(title: str) -> bool:
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _CS_TITLE_PATTERNS)


_CS_PROMPT = """KAP tedbir/kredili/açığa satış/brut takas bildirimini analiz et. JSON dondur:

Hisse: {ticker}
Baslik: {title}
Icerik: {body}

JSON:
{{
  "tags": ["KRD", "ACS", "BRT", "TEK", "EPT", "IEY"] (uygulanan tedbir tipleri),
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD"
}}

KURALLAR (BIST tedbir tipleri):
- KRD = Kredili Alım Yasağı
- ACS = Açığa Satış Yasağı
- BRT = Brüt Takas Uygulaması
- TEK = Tek Fiyat Uygulaması
- EPT = Emir Paketi Tedbiri
- IEY = İnternet Emir Yasağı
- EMR = Emir İptali Tedbiri (eski)
- PEM = Piyasa Emri Yasağı (eski)
- VEY = Veri Yayını Tedbiri (eski)

Bilinmeyenler null. SADECE JSON dondur.
"""


async def process_cautious(
    db: AsyncSession, *, disclosure_id: int, ticker: str, company_name: Optional[str],
    title: str, body: Optional[str], kap_url: Optional[str], published_at: Optional[datetime],
) -> Optional[CautiousStock]:
    if not is_cautious(title):
        return None

    parsed = await _call_gemini(_CS_PROMPT.format(ticker=ticker, title=title or "", body=(body or "")[:3000])) or {}

    tags = parsed.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    _VALID = {"KRD", "ACS", "BRT", "EMR", "PEM", "VEY", "TEK", "EPT", "IEY"}
    valid_tags = [t.upper().replace("Ç", "C") for t in tags if isinstance(t, str) and t.upper().replace("Ç", "C") in _VALID]

    start_date = None
    end_date = None
    if isinstance(parsed.get("start_date"), str):
        try:
            start_date = date.fromisoformat(parsed["start_date"])
        except ValueError:
            pass
    if isinstance(parsed.get("end_date"), str):
        try:
            end_date = date.fromisoformat(parsed["end_date"])
        except ValueError:
            pass

    # AI body'den tedbir tipi cikarilamadiysa atla — anlik devre kesici
    # gibi gercek tedbir olmayan bildirimler eklenmesin.
    if not valid_tags and not start_date and not end_date:
        logger.debug("Cautious skip: AI tedbir bilgisi cikaramadi (%s)", ticker)
        return None

    today = date.today()
    is_active = bool(end_date and end_date >= today)

    # Mevcut aktif kayit varsa update, yoksa yeni
    stmt = select(CautiousStock).where(
        CautiousStock.ticker == ticker,
        CautiousStock.is_active == True,
    ).order_by(CautiousStock.id.desc()).limit(1)
    existing = (await db.execute(stmt)).scalar_one_or_none()

    if existing:
        # Tag birlestir
        cur_tags = set((existing.tags or "").split(",")) - {""}
        new_set = cur_tags | set(valid_tags)
        existing.tags = ",".join(sorted(new_set)) if new_set else None
        if start_date and not existing.start_date:
            existing.start_date = start_date
        if end_date:
            existing.end_date = end_date
            existing.is_active = end_date >= today
        if kap_url and not existing.kap_url:
            existing.kap_url = kap_url
        return existing

    new_row = CautiousStock(
        ticker=ticker,
        company_name=company_name,
        start_date=start_date,
        end_date=end_date,
        tags=",".join(valid_tags) if valid_tags else None,
        is_active=is_active,
        kap_url=kap_url,
        source="kap_ai_parse",
    )
    db.add(new_row)
    await db.flush()
    logger.info("Cautious: yeni (%s, tags=%s)", ticker, valid_tags)
    return new_row


# ─── BISTECH Multi-Ticker (VBTS) ─────────────────────────────────────────────
# KAP'taki BISTECH Pay Piyasasi Alim Satim Sistemi Duyurulari company_code=DKB
# olarak gelir, body'de "BIGEN.E ve ICUGS.E paylarinda ... brut takas..." gibi
# birden fazla ticker bahsedilir. process_cautious tek ticker calistigi icin
# bunlar yakalanmazdi. Bu wrapper body'den tum ticker'lari cikartip her biri
# icin merge eder.

_TICKER_DOT_E_RE = re.compile(r'\b([A-Z]{3,6})\.E\b')


def is_bistech_vbts(title: str, body: Optional[str]) -> bool:
    # NOT: lower_tr 'I' → 'ı' (dotless) cevirir, "bistech" match olmaz.
    # Bu yuzden ascii lower (.lower()) kullanilir.
    t = (title or "").lower()
    b = (body or "")[:3000].lower()
    has_bistech = "bistech" in t or "bıstech" in t  # iki olasilik
    if not has_bistech:
        return False
    return ("volatilite bazl" in b or "volatilite bazl" in t or
            "vbts" in b or "vbts" in t or
            "brüt takas" in b or "brut takas" in b or
            "tedbir" in t)


async def process_cautious_bistech_multi(
    db: AsyncSession, *, disclosure_id: int, title: str, body: Optional[str],
    kap_url: Optional[str], published_at: Optional[datetime],
) -> list[CautiousStock]:
    """BISTECH VBTS bildiriminden body'deki tum ticker'lari cikart, her biri icin
    cautious_stocks kaydini merge et. Tek AI cagrisiyla ortak tag/tarih cikarilir.
    """
    if not body or not is_bistech_vbts(title, body):
        return []

    tickers = sorted(set(_TICKER_DOT_E_RE.findall(body)))
    if not tickers:
        return []

    # Regex ile body'den tag/tarih cikar (AI'ya gerek yok — VBTS bildirimleri
    # yapisi sabit: "brut takas", "aciga satis", "kredili islem" gibi anahtar
    # kelimeler + "DD/MM/YYYY ... DD/MM/YYYY" tarih araligi)
    valid_tags: list[str] = []
    body_lo = body.lower()
    if "brüt takas" in body_lo or "brut takas" in body_lo or "gross settlement" in body_lo:
        valid_tags.append("BRT")
    if "açığa satış" in body_lo or "aciga satis" in body_lo or "short selling" in body_lo:
        valid_tags.append("ACS")
    if "kredili işlem" in body_lo or "kredili islem" in body_lo or "margn trading" in body_lo or "margin trading" in body_lo:
        valid_tags.append("KRD")
    if "tek fiyat" in body_lo:
        valid_tags.append("TEK")
    if "emir paketi" in body_lo:
        valid_tags.append("EPT")
    if "internet emir" in body_lo:
        valid_tags.append("IEY")

    # Tarih araligi: ilk iki "DD/MM/YYYY tarihli ... DD/MM/YYYY tarihli" eslesmesi
    start_date = None
    end_date = None
    date_re = re.compile(r"(\d{2})/(\d{2})/(\d{4})")
    matches = date_re.findall(body)
    if len(matches) >= 2:
        try:
            d1, m1, y1 = map(int, matches[0])
            d2, m2, y2 = map(int, matches[1])
            start_date = date(y1, m1, d1)
            end_date = date(y2, m2, d2)
        except (ValueError, TypeError):
            pass

    if not valid_tags and not start_date and not end_date:
        logger.debug("BISTECH VBTS skip: regex bilgi cikartamadi (tickers=%s)", tickers)
        return []

    today = date.today()
    results: list[CautiousStock] = []

    for tk in tickers:
        stmt = select(CautiousStock).where(
            CautiousStock.ticker == tk,
            CautiousStock.is_active == True,
        ).order_by(CautiousStock.id.desc()).limit(1)
        existing = (await db.execute(stmt)).scalar_one_or_none()

        if existing:
            cur_tags = set((existing.tags or "").split(",")) - {""}
            new_set = cur_tags | set(valid_tags)
            existing.tags = ",".join(sorted(new_set)) if new_set else None
            # start_date — yeni KAP'tan gelen daha yeni ise gunceller (en son
            # tedbir aksiyonunun tarihi). NULL kontrolu degil.
            if start_date and (not existing.start_date or start_date > existing.start_date):
                existing.start_date = start_date
            if end_date:
                existing.end_date = end_date
                existing.is_active = end_date >= today
            if kap_url:
                existing.kap_url = kap_url  # her zaman en son KAP linki
            results.append(existing)
            logger.info("BISTECH VBTS merge: %s tags=%s start=%s end=%s", tk, sorted(new_set), start_date, end_date)
        else:
            new_row = CautiousStock(
                ticker=tk,
                start_date=start_date,
                end_date=end_date,
                tags=",".join(valid_tags) if valid_tags else None,
                is_active=bool(end_date and end_date >= today),
                kap_url=kap_url,
                source="kap_ai_parse",
            )
            db.add(new_row)
            await db.flush()
            results.append(new_row)
            logger.info("BISTECH VBTS yeni: %s tags=%s", tk, valid_tags)

    return results
