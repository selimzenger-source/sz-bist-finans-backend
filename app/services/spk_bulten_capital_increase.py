"""SPK Bulten -> Capital Increase state machine guncellemesi.

AI ile bulten metninden sermaye artirimi onaylari + reddi cikarir, sonra
capital_increases tablosundaki ilgili kaydi spk_onayli / reddedildi'ye ceker.

Kullanim:
  decisions = await extract_capital_decisions_from_bulletin(bulletin_text)
  applied   = await apply_decisions_to_db(db, decisions, bulten_no=bulten_no)
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date as _date, datetime as _dt, timezone as _tz
from typing import Any, Optional

import httpx
from sqlalchemy import select, or_, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.capital_increase import CapitalIncrease

logger = logging.getLogger(__name__)


_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_AI_TIMEOUT = 60


def _get_gemini_key() -> Optional[str]:
    try:
        from app.config import get_settings
        s = get_settings()
        return s.GEMINI_API_KEY if s.GEMINI_API_KEY else None
    except Exception:
        return None


_PROMPT = """Asagidaki SPK Bulteninin TAM metnini incele ve "Halka Acik Ortakliklarin Pay Ihraclari" / "Sermaye Artirimi Onaylari/Reddi" bolumlerinden sermaye artirimi kararlarini yapilandirilmis JSON listesi olarak dondur.

KURALLAR:
- SADECE sermaye artirimi kararlarini dondur. Halka Arz / IPO satirlarini ATLA.
- "Halka Acik Ortakligin Pay Ihraci" olarak gecen sermaye artirimi onaylaridir → decision: "approved"
- "Reddedil/iade/uygun bulunmamis/olumsuz" gecen → decision: "rejected"
- Eger hem onay hem red varsa, hepsini ayri ayri listele.
- Sirket adi tam yazilsin (orn: "Plastikkart Akilli Kart Iletisim Sistemleri Sanayi ve Ticaret A.S.")
- type: "bedelsiz", "bedelli", "tahsisli" — emin degilsen null

CIKTI FORMATI (JSON only, baska metin yok):
{
  "decisions": [
    {
      "company_name": "Plastikkart Akilli Kart Iletisim Sistemleri Sanayi ve Ticaret A.S.",
      "decision": "approved",
      "type": "bedelsiz"
    }
  ]
}

BULTEN METNI:
"""


async def extract_capital_decisions_from_bulletin(bulletin_text: str) -> list[dict[str, Any]]:
    """AI ile bulten metninden sermaye artirimi kararlarini cek.

    Returns: list of {company_name, decision: 'approved'|'rejected', type: str|None}
    """
    api_key = _get_gemini_key()
    if not api_key or not bulletin_text:
        logger.warning("spk_bulten_cap_inc: AI key veya metin yok")
        return []

    # Metin cok uzunsa kısalt — AI quota
    if len(bulletin_text) > 25000:
        bulletin_text = bulletin_text[:25000]

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": _GEMINI_MODEL,
        "messages": [{"role": "user", "content": _PROMPT + bulletin_text}],
        "temperature": 0.0,
        "response_format": {"type": "json_object"},
    }

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            r = await client.post(_GEMINI_URL, headers=headers, json=payload)
            if r.status_code != 200:
                logger.warning("spk_bulten_cap_inc AI status: %s", r.status_code)
                return []
            data = r.json()
            content = data["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            decs = parsed.get("decisions", [])
            if not isinstance(decs, list):
                return []
            # Validate
            out = []
            for d in decs:
                if not isinstance(d, dict): continue
                cn = (d.get("company_name") or "").strip()
                dec = (d.get("decision") or "").lower()
                if not cn or dec not in ("approved", "rejected"):
                    continue
                out.append({
                    "company_name": cn,
                    "decision": dec,
                    "type": d.get("type"),
                })
            logger.info("spk_bulten_cap_inc AI: %d karar cekildi", len(out))
            return out
    except Exception as e:
        logger.warning("spk_bulten_cap_inc AI hata: %s", str(e)[:200])
        return []


# ═══════════════════════════════════════════════════════════════════
# Sirket adi -> ticker matching
# ═══════════════════════════════════════════════════════════════════

def _normalize_name(s: str) -> str:
    """Sirket adini matching icin normalize et."""
    s = (s or "").lower()
    s = re.sub(r"[^a-zçğıöşü0-9\s]", " ", s, flags=re.IGNORECASE)
    # Yaygin sonek/onekleri kaldir
    for suffix in [
        " a.s.", " a.s", " a s", " as", " anonim sirketi", " anonim",
        " ticaret", " sanayi", " ve ticaret", " sanayi ve ticaret",
        " holding", " yatirim", " ortaklik", " ortakliklari",
    ]:
        s = s.replace(suffix, " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


async def _resolve_ticker(db: AsyncSession, company_name: str) -> Optional[str]:
    """Sirket adindan ticker bul. Once capital_increases'a, sonra kap_all_disclosures'a bak."""
    if not company_name:
        return None
    nn = _normalize_name(company_name)
    if len(nn) < 4:
        return None

    # Strateji 1: capital_increases.company_name LIKE
    rows = (await db.execute(
        select(CapitalIncrease.ticker, CapitalIncrease.company_name)
        .where(CapitalIncrease.company_name.is_not(None))
    )).all()
    for ticker, cn in rows:
        if not cn:
            continue
        if _normalize_name(cn).startswith(nn[:20]) or nn.startswith(_normalize_name(cn)[:20]):
            return ticker

    # Strateji 2: kap_all_disclosures.company_name (varsa)
    try:
        from app.models.kap_all_disclosure import KapAllDisclosure
        rows2 = (await db.execute(
            select(KapAllDisclosure.ticker, KapAllDisclosure.company_name)
            .where(KapAllDisclosure.company_name.is_not(None))
            .where(func.lower(KapAllDisclosure.company_name).like(f"%{nn[:15]}%"))
            .limit(5)
        )).all()
        for ticker, cn in rows2:
            if not cn or not ticker:
                continue
            if _normalize_name(cn).startswith(nn[:20]) or nn.startswith(_normalize_name(cn)[:20]):
                return ticker
    except Exception as e:
        logger.debug("kap_all_disclosures lookup hatasi: %s", e)

    return None


# ═══════════════════════════════════════════════════════════════════
# Apply decisions
# ═══════════════════════════════════════════════════════════════════

async def apply_decisions_to_db(
    db: AsyncSession,
    decisions: list[dict[str, Any]],
    bulten_no: Optional[str] = None,
) -> dict[str, Any]:
    """Onay/red kararlarini capital_increases tablosuna uygula.

    Akis:
      approved -> ykk_alindi olan kayit -> spk_onayli + spk_approval_date=today
      rejected -> ykk_alindi olan kayit -> reddedildi + rejected_at=now

    Returns:
      {"approved": int, "rejected": int, "skipped": list[str]}
    """
    today = _date.today()
    now = _dt.now(_tz.utc)
    approved_count = 0
    rejected_count = 0
    skipped: list[str] = []

    for d in decisions:
        company_name = d.get("company_name") or ""
        decision = d.get("decision")

        ticker = await _resolve_ticker(db, company_name)
        if not ticker:
            skipped.append(company_name)
            logger.info("spk_bulten_cap_inc: ticker bulunamadi - %s", company_name[:60])
            continue

        # Aktif kayit bul
        existing = (await db.execute(
            select(CapitalIncrease).where(
                CapitalIncrease.ticker == ticker,
                CapitalIncrease.status == "ykk_alindi",
            ).order_by(CapitalIncrease.created_at.desc())
        )).scalars().first()

        if not existing:
            # ykk_alindi yok ama spk_onayli olabilir (zaten onaylanmis)
            skipped.append(f"{ticker} ({company_name[:30]}) — ykk_alindi yok")
            continue

        if decision == "approved":
            existing.status = "spk_onayli"
            existing.spk_approval_date = today
            if company_name and not existing.company_name:
                existing.company_name = company_name
            approved_count += 1
            logger.info("cap_inc spk_onayli: %s id=%s", ticker, existing.id)

        elif decision == "rejected":
            existing.status = "reddedildi"
            existing.rejected_at = now
            if company_name and not existing.company_name:
                existing.company_name = company_name
            rejected_count += 1
            logger.info("cap_inc reddedildi: %s id=%s", ticker, existing.id)

    await db.commit()
    return {"approved": approved_count, "rejected": rejected_count, "skipped": skipped}
