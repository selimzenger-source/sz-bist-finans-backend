"""Kurum onerileri AI yorum servisi — Claude Sonnet 4.6 ile 3-4 cumlelik
yatirimci yorumu uretir. Hedef fiyat potansiyel getirisini TCMB gecelik
faiz orani ile karsilastirir, mantik zinciri kurar.
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
from datetime import datetime, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.kurum_oneri import KurumOneri

logger = logging.getLogger("kurum_oneri_ai")

# TCMB politika faizi — gecelik referans olarak kullanilir (yaklasik deger;
# onemli olan siralama, tam kesinlik degil). Guncelleme gerektikce buradan
# degistir. 2026 Nisan itibariyla politika faizi ~42.5%.
_TCMB_NIGHT_RATE = 42.5


def _build_prompt(oneri: KurumOneri, night_rate: float = _TCMB_NIGHT_RATE) -> str:
    ticker = oneri.ticker or "?"
    company = oneri.company_name or ticker
    inst = oneri.institution_name or "Araci Kurum"
    rec = oneri.recommendation or "Belirsiz"
    target = float(oneri.target_price) if oneri.target_price else None
    current = float(oneri.current_price) if oneri.current_price else None
    pot = float(oneri.potential_return) if oneri.potential_return else None
    report_date = oneri.report_date.isoformat() if oneri.report_date else "?"

    # Yillik getiri karsilastirmasi
    comparison_hint = ""
    if pot is not None:
        if pot >= night_rate:
            diff = pot - night_rate
            comparison_hint = (
                f"Hedef getiri (%{pot:.1f}) TCMB gecelik faizin (%{night_rate:.1f}) "
                f"USTUNDE — reel pozitif getiri potansiyeli var (+%{diff:.1f} farkla)."
            )
        elif pot >= night_rate / 2:
            comparison_hint = (
                f"Hedef getiri (%{pot:.1f}) TCMB gecelik faizin (%{night_rate:.1f}) "
                "altinda ama yakin — temettu + sermaye kazanci birlikte faize yaklasabilir."
            )
        else:
            diff = night_rate - pot
            comparison_hint = (
                f"Hedef getiri (%{pot:.1f}) TCMB gecelik faizin (%{night_rate:.1f}) "
                f"altinda (-%{diff:.1f}) — risk-getiri bakimindan nakit/mevduat daha rekabetci."
            )

    return f"""BIST analizcisin. Asagidaki kurum onerisi icin MAX 3 CUMLE yorum yaz.

Hisse: {ticker} ({company})
Kurum: {inst} | Oneri: {rec}
Son: {current if current is not None else 'N/A'} TL | Hedef: {target if target is not None else 'N/A'} TL | Potansiyel: %{pot if pot is not None else 'N/A'}
TCMB gecelik faiz: %{night_rate:.1f}

{comparison_hint}

KURALLAR:
- MAKSIMUM 3 cumle. KISA tut.
- 1. cumle: oneri + hedef getiri ozet
- 2. cumle: getiri vs faiz karsilastirma (yukarida/altinda)
- 3. cumle (opsiyonel): oneri-getiri tutarliligi veya kisa risk notu
- Yatirim tavsiyesi degildir, soyleme. Clickbait yok.
- SADECE yorumu yaz."""


async def generate_ai_comment(oneri: KurumOneri) -> str | None:
    """Tek bir kurum onerisi icin Claude Sonnet ile yorum uret."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        logger.error("ANTHROPIC_API_KEY yok — kurum oneri AI yorumu uretilemiyor")
        return None

    prompt = _build_prompt(oneri)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            if resp.status_code != 200:
                logger.warning("Claude HTTP %s: %s", resp.status_code, resp.text[:200])
                return None
            data = resp.json()
            text = data.get("content", [{}])[0].get("text", "").strip()
            del data, resp  # Bellek temizle
            if not text:
                return None
            # Temizle: Baslangictaki tirnaklari, gereksiz markdown kaldir
            text = text.strip('"\' \n')
            # Max 3 cumle ile sınırla — Haiku bazen 4-5 cumle yazabiliyor
            import re as _re
            _sentences = _re.split(r'(?<=[.!?])\s+', text)
            if len(_sentences) > 3:
                text = " ".join(_sentences[:3]).strip()
            return text[:800]
    except Exception as e:
        logger.error("Claude hata (ticker=%s): %s", oneri.ticker, e)
        return None


async def backfill_comments(db: AsyncSession, limit: int = 30) -> dict:
    """AI yorumu eksik olan kayitlara Claude ile yorum ekle."""
    result = await db.execute(
        select(KurumOneri)
        .where(KurumOneri.ai_comment.is_(None))
        .order_by(KurumOneri.created_at.desc())
        .limit(limit)
    )
    rows = result.scalars().all()

    done = 0
    failed = 0
    for row in rows:
        comment = await generate_ai_comment(row)
        if comment:
            row.ai_comment = comment
            row.ai_comment_at = datetime.now(timezone.utc)
            done += 1
        else:
            failed += 1
        gc.collect()  # Her API cagrisindan sonra bellek temizle
        await asyncio.sleep(30)  # Bellek rahatlasin, OOM engeli
    await db.commit()
    return {"scanned": len(rows), "done": done, "failed": failed}


async def comment_and_tweet_new(db: AsyncSession, oneri: KurumOneri) -> bool:
    """Yeni gelen kurum onerisi icin AI yorumu uret, DB'ye yaz. Tweet ayri
    akista handle edilir."""
    if oneri.ai_comment:
        return True
    comment = await generate_ai_comment(oneri)
    if not comment:
        return False
    oneri.ai_comment = comment
    oneri.ai_comment_at = datetime.now(timezone.utc)
    await db.commit()
    return True
