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
    "toptan alış satış", "toptan alis satis",                  # 5+ kayit
    "toptan satış", "toptan alış", "toptan alım",
    "toptan işlem", "toptan satim",
    "toptan alim satim",
]


def is_block_trade(title: str) -> bool:
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _BT_TITLE_PATTERNS)


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


async def process_block_trade(
    db: AsyncSession, *, disclosure_id: int, ticker: str, company_name: Optional[str],
    title: str, body: Optional[str], kap_url: Optional[str], published_at: Optional[datetime],
) -> Optional[BlockTrade]:
    if not is_block_trade(title):
        return None
    if disclosure_id:
        stmt = select(BlockTrade).where(BlockTrade.kap_url == kap_url).limit(1) if kap_url else select(BlockTrade).where(False)
        if kap_url and (await db.execute(stmt)).scalar_one_or_none():
            return None

    parsed = await _call_gemini(_BT_PROMPT.format(ticker=ticker, title=title or "", body=(body or "")[:3500])) or {}

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


async def process_type_conversion(
    db: AsyncSession, *, disclosure_id: int, ticker: str, company_name: Optional[str],
    title: str, body: Optional[str], kap_url: Optional[str], published_at: Optional[datetime],
) -> Optional[ShareTypeConversion]:
    if not is_type_conversion(title):
        return None
    if kap_url:
        stmt = select(ShareTypeConversion).where(ShareTypeConversion.kap_url == kap_url).limit(1)
        if (await db.execute(stmt)).scalar_one_or_none():
            return None

    parsed = await _call_gemini(_TC_PROMPT.format(ticker=ticker, title=title or "", body=(body or "")[:3000])) or {}

    tx_date = None
    if isinstance(parsed.get("transaction_date"), str):
        try:
            tx_date = date.fromisoformat(parsed["transaction_date"])
        except ValueError:
            pass
    if not tx_date:
        tx_date = published_at.date() if published_at else date.today()

    investor = parsed.get("investor_name") or "?"
    converted = parsed.get("converted_lot")

    new_row = ShareTypeConversion(
        ticker=ticker,
        company_name=company_name,
        transaction_date=tx_date,
        investor_name=str(investor)[:255],
        converted_lot=(int(converted) if isinstance(converted, (int, float)) else None),
        kap_url=kap_url,
        source="kap_ai_parse",
    )
    db.add(new_row)
    await db.flush()
    logger.info("TypeConversion: yeni (%s, %s)", ticker, investor[:30])
    return new_row


# ═══════════════════════════════════════════════════════════════════
# 3. TEDBIRLI HISSELER (KAP)
# ═══════════════════════════════════════════════════════════════════

_CS_TITLE_PATTERNS = [
    # KAP gerçek başlıkları — sadece kalici tedbir/önlem kararları.
    # Devre kesici DAHİL DEĞİL (anlık trading halt, tedbir sayılmaz).
    "tedbir kararı", "tedbir karari",
    "spk tedbir", "sermaye piyasası kurulu tedbir",
    "bistech pay piyasası",                                        # BIST tedbir uygulamalarını duyurur
    "borsa istanbul a.ş. duyuru", "borsa istanbul duyuru",
    "brüt takas", "brut takas",
    "açığa satış", "aciga satis", "açığa satış yasağı",
    "kredili işlem", "kredili islem",
    "tedbir uygulan", "önlem uygulan", "onlem uygulan",
    "emir iptali", "piyasa emri", "tek fiyat",
    "tedbirli", "borsa istanbul tedbir",
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
