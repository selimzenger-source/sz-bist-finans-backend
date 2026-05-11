"""Pay Alım Satım — KAP body AI parse → ShareTransactionDetail."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.share_transaction_detail import ShareTransactionDetail
from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)

_TITLE_PATTERNS = [
    # KAP gerçek başlıkları (production DB analizinden)
    "pay alım satım bildirimi", "pay alim satim bildirimi",       # 41 kayit son 30 gun
    "pay alım satım", "pay alim satim",
    # NOT: "geri alın" / "payların geri alın" KALDIRILDI — bunlar `buyback_processor`
    # tarafından işleniyor ve şirketin kendi paylarını geri alımı (farklı kategori).
    # Önceki çakışma duplicate kayıt yaratıyordu (hem buybacks hem
    # share_transaction_details tablosuna yazılıyordu).
    "pay alımı", "pay alimi",
    "pay satışı", "pay satisi",
    "önemli paydaş", "onemli paydas",
]

# Body içinde aranacak pay alım satım sinyalleri
# Multi-symbol bulk duyurularda title generic olabilir ("Kamuyu Aydınlatma")
# ama body'de Pay Alım Satım kalıbı geçer.
_BODY_PATTERNS = [
    "pay alım satım bildirimi", "pay alim satim bildirimi",
    "alım nominal", "satım nominal", "alim nominal", "satim nominal",
    "günü içinde",  # KAP standart pay alım satım açıklama kalıbı
    "fiyat aralığından", "fiyat aralıgindan",
    "pay başına ortalama fiyat", "pay basina ortalama fiyat",
    "oy hakkı oranı", "oy hakki orani",
    "pay oranı", "pay orani",
]


def is_share_transaction(title: str, body: str = "") -> bool:
    """Pay Alım Satım Bildirimi mi?

    Title'da kalıp varsa direkt True. Title generic ise ("Kamuyu Aydınlatma
    Platformu Duyurusu") body'de pay alım satım kalıbı arar — multi-symbol
    bulk duyurular için.
    """
    if title:
        t = lower_tr(title)
        if any(p in t for p in _TITLE_PATTERNS):
            return True
    # Title yetersiz — body'de ara
    if body:
        b = lower_tr(body)
        # En az 2 farklı body sinyali olmalı (yanlış pozitif önle)
        matches = sum(1 for p in _BODY_PATTERNS if p in b)
        if matches >= 2:
            return True
    return False


_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_AI_TIMEOUT = 30


def _get_gemini_key() -> Optional[str]:
    try:
        from app.config import get_settings
        return get_settings().GEMINI_API_KEY or None
    except Exception:
        return None


_PROMPT = """Asagidaki KAP pay alim satim bildirimini analiz et ve YAPILANDIRILMIS JSON dondur.

KAP BILDIRIMI:
Hisse: {ticker}
Baslik: {title}
Icerik:
{body}

Donen JSON sablonu (eksikler null):
{{
  "transaction_type": "alici" | "satici",
  "transaction_date": "YYYY-MM-DD",
  "party_name": "Alan/satan kisi veya sirket",
  "party_role": "Görev (ornegin Yonetim Kurulu Baskani veya Vice President)",
  "price_low": <sayi>,
  "price_high": <sayi> (aralik ust degeri),
  "nominal_lot": <int>,
  "oy_hakki_pct": <yuzde sayi>,
  "oy_hakki_change_pct": <degisim yuzde, +/->,
  "pay_orani_pct": <yuzde>,
  "pay_orani_change_pct": <degisim>
}}

KURALLAR:
- SADECE JSON dondur.
- Bilinmeyenler null. Tahmin etme.
- transaction_type: pay alanlar icin "alici", satanlar icin "satici".
"""


async def ai_parse(ticker: str, title: str, body: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    key = _get_gemini_key()
    if not key or not body:
        return out
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as c:
            r = await c.post(
                _GEMINI_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Yapilandirilmis JSON dondur. SADECE JSON."},
                        {"role": "user", "content": _PROMPT.format(ticker=ticker, title=title or "", body=(body or "")[:3500])},
                    ],
                    "temperature": 0.1, "max_tokens": 1024,
                },
            )
            if r.status_code == 200:
                txt = r.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                p = _parse_json(txt)
                if p:
                    if p.get("transaction_type") in ("alici", "satici"):
                        out["transaction_type"] = p["transaction_type"]
                    if isinstance(p.get("transaction_date"), str):
                        try:
                            out["transaction_date"] = date.fromisoformat(p["transaction_date"])
                        except ValueError:
                            pass
                    for k in ("party_name", "party_role"):
                        v = p.get(k)
                        if isinstance(v, str) and v.strip():
                            out[k] = v.strip()[:255]
                    for k in ("price_low", "price_high", "oy_hakki_pct", "oy_hakki_change_pct", "pay_orani_pct", "pay_orani_change_pct"):
                        v = p.get(k)
                        if isinstance(v, (int, float)):
                            out[k] = float(v)
                    nl = p.get("nominal_lot")
                    if isinstance(nl, (int, float)):
                        out["nominal_lot"] = int(nl)
    except Exception as e:
        logger.warning("ShareTx AI hata: %s", e)
    return out


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


async def process_kap_disclosure(
    db: AsyncSession, *, disclosure_id: int, ticker: str, company_name: Optional[str],
    title: str, body: Optional[str], kap_url: Optional[str], published_at: Optional[datetime],
) -> Optional[ShareTransactionDetail]:
    if not is_share_transaction(title):
        return None

    # Mevcut KAP id ile kayit varsa skip
    if disclosure_id:
        stmt = select(ShareTransactionDetail).where(ShareTransactionDetail.kap_disclosure_id == disclosure_id).limit(1)
        if (await db.execute(stmt)).scalar_one_or_none():
            return None

    parsed = await ai_parse(ticker, title, body or "")

    # transaction_type: AI > body keyword > heuristik (pay_orani_change_pct işareti)
    transaction_type = parsed.get("transaction_type")
    if transaction_type not in ("alici", "satici"):
        # Body içinde "Alıcı"/"Satıcı" geçiyor mu? (KAP form alanı)
        bl = lower_tr(body or "")
        if "alıcı" in bl or "alici" in bl or "alimi" in bl or "alımı" in bl:
            transaction_type = "alici"
        elif "satıcı" in bl or "satici" in bl or "satışı" in bl or "satisi" in bl:
            transaction_type = "satici"
    if transaction_type not in ("alici", "satici"):
        # Pay/oy oranı artıyorsa alıcı, azalıyorsa satıcı
        pay_chg = parsed.get("pay_orani_change_pct")
        oy_chg = parsed.get("oy_hakki_change_pct")
        chg = pay_chg if isinstance(pay_chg, (int, float)) and pay_chg != 0 else (oy_chg if isinstance(oy_chg, (int, float)) else None)
        if isinstance(chg, (int, float)):
            transaction_type = "alici" if chg > 0 else "satici"
        else:
            transaction_type = "satici"  # son çare

    tx_date = parsed.get("transaction_date") or (published_at.date() if published_at else date.today())
    party_name = parsed.get("party_name") or "?"

    new_row = ShareTransactionDetail(
        ticker=ticker,
        company_name=company_name,
        transaction_date=tx_date,
        transaction_type=transaction_type,
        party_name=party_name,
        party_role=parsed.get("party_role"),
        price_low=parsed.get("price_low"),
        price_high=parsed.get("price_high"),
        nominal_lot=parsed.get("nominal_lot"),
        oy_hakki_pct=parsed.get("oy_hakki_pct"),
        oy_hakki_change_pct=parsed.get("oy_hakki_change_pct"),
        pay_orani_pct=parsed.get("pay_orani_pct"),
        pay_orani_change_pct=parsed.get("pay_orani_change_pct"),
        kap_disclosure_id=disclosure_id,
        kap_url=kap_url,
        source="kap_ai_parse",
    )
    db.add(new_row)
    await db.flush()
    logger.info("ShareTx: yeni (%s, %s, %s)", ticker, transaction_type, party_name[:30])
    return new_row
