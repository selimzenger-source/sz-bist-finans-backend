"""Capital increase KAP processor — telegram_poller'dan cagrilir.

Ana fonksiyon: process_kap_for_capital_increase(db, ticker, kap_url, title, body)
  - Stage detect
  - Parse
  - DB INSERT/UPDATE (state machine)

State machine:
  application      -> ykk_alindi   (INSERT yeni kayit)
  distribution_date -> tarih_belli (UPDATE distribution_date, status)
  split_completed  -> tamamlandi   (UPDATE distribution_date, status)
"""

from __future__ import annotations

import logging
from datetime import date as _date, datetime as _dt, timezone as _tz
from typing import Optional

from sqlalchemy import select, text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.capital_increase import CapitalIncrease
from app.services.capital_increase_kap_parser import (
    detect_stage,
    parse_application,
    parse_distribution_date,
    parse_split_completed,
)

logger = logging.getLogger(__name__)


async def process_kap_for_capital_increase(
    db: AsyncSession,
    ticker: str,
    kap_url: str,
    title: str,
    body: str,
) -> Optional[dict]:
    """KAP bildirimi sermaye artirimi ile alakali mi check et + DB guncelle.

    Returns:
        {"stage": str, "action": "insert" | "update", "id": int} basariliysa
        None alakasizsa
    """
    if not body:
        return None

    stage = detect_stage(title, body)
    if not stage:
        return None

    if not ticker:
        logger.warning("cap_inc KAP'da ticker yok: %s", kap_url)
        return None

    ticker = ticker.upper().strip()

    if stage == "application":
        return await _handle_application(db, ticker, kap_url, title, body)
    elif stage == "distribution_date":
        return await _handle_distribution_date(db, ticker, kap_url, body)
    elif stage == "split_completed":
        return await _handle_split_completed(db, ticker, kap_url, body)

    return None


async def _handle_application(
    db: AsyncSession, ticker: str, kap_url: str, title: str, body: str,
) -> Optional[dict]:
    """Stage 1: SPK basvuru — yeni kayit olustur (ya da varsa guncelle)."""
    parsed = parse_application(title, body)
    if not parsed.get("type"):
        logger.warning("cap_inc application: tip belirlenemedi %s", ticker)
        return None

    # Active kayit var mi? (status: ykk_alindi/spk_onayli/tarih_belli)
    existing = (await db.execute(
        select(CapitalIncrease).where(
            CapitalIncrease.ticker == ticker,
            CapitalIncrease.status.in_(["ykk_alindi", "spk_onayli", "tarih_belli", "dagitiliyor"]),
        )
    )).scalars().first()

    type_primary = parsed["type"]

    if existing:
        # Mevcut active kayit guncelle (oran/sermaye/tarih ekle)
        if parsed.get("ykk_date") and not existing.ykk_date:
            existing.ykk_date = parsed["ykk_date"]
        if not existing.ykk_kap_url:
            existing.ykk_kap_url = kap_url
        if parsed.get("ulasilacak_sermaye_tl") and not existing.bolunme_sonrasi_sermaye_tl:
            existing.bolunme_sonrasi_sermaye_tl = parsed["ulasilacak_sermaye_tl"]
        if parsed.get("bedelli_pct") and not existing.bedelli_pct:
            existing.bedelli_pct = parsed["bedelli_pct"]
        if parsed.get("bedelsiz_pct") and not existing.bedelsiz_pct:
            existing.bedelsiz_pct = parsed["bedelsiz_pct"]
        if parsed.get("tahsisli_pct") and not existing.tahsisli_pct:
            existing.tahsisli_pct = parsed["tahsisli_pct"]
        await db.commit()
        logger.info("cap_inc application UPDATE: %s id=%s type=%s", ticker, existing.id, type_primary)
        return {"stage": "application", "action": "update", "id": existing.id}

    # Yeni kayit — status=ykk_alindi
    new = CapitalIncrease(
        ticker=ticker,
        type=type_primary,
        status="ykk_alindi",
        ykk_date=parsed.get("ykk_date"),
        ykk_kap_url=kap_url,
        bedelli_pct=parsed.get("bedelli_pct"),
        bedelsiz_pct=parsed.get("bedelsiz_pct"),
        tahsisli_pct=parsed.get("tahsisli_pct"),
        bolunme_sonrasi_sermaye_tl=parsed.get("ulasilacak_sermaye_tl"),
    )
    db.add(new)
    try:
        await db.commit()
        await db.refresh(new)
        logger.info("cap_inc application INSERT: %s id=%s type=%s", ticker, new.id, type_primary)
        return {"stage": "application", "action": "insert", "id": new.id}
    except Exception as e:
        await db.rollback()
        logger.warning("cap_inc application INSERT fail %s: %s", ticker, str(e)[:200])
        return None


async def _handle_distribution_date(
    db: AsyncSession, ticker: str, kap_url: str, body: str,
) -> Optional[dict]:
    """Stage 3: Dagitim tarihi ilani — distribution_date set + status=tarih_belli."""
    dist_date = parse_distribution_date(body)
    if not dist_date:
        logger.warning("cap_inc distribution_date: tarih bulunamadi %s", ticker)
        return None

    # Active kayit (ykk_alindi/spk_onayli) bul
    existing = (await db.execute(
        select(CapitalIncrease).where(
            CapitalIncrease.ticker == ticker,
            CapitalIncrease.status.in_(["ykk_alindi", "spk_onayli"]),
        )
    )).scalars().first()

    if not existing:
        # Active kayit yok — yeni olustur (KAP'tan first-touch durumu)
        existing = CapitalIncrease(
            ticker=ticker,
            type="bedelsiz",  # tip body'den infer et alttaki override eder
            status="tarih_belli",
            distribution_date=dist_date,
            distribution_kap_url=kap_url,
        )
        db.add(existing)
        await db.commit()
        await db.refresh(existing)
        logger.info("cap_inc distribution_date INSERT: %s id=%s date=%s", ticker, existing.id, dist_date)
        return {"stage": "distribution_date", "action": "insert", "id": existing.id}

    existing.distribution_date = dist_date
    existing.distribution_kap_url = kap_url
    existing.status = "tarih_belli"
    await db.commit()
    logger.info("cap_inc distribution_date UPDATE: %s id=%s date=%s", ticker, existing.id, dist_date)
    return {"stage": "distribution_date", "action": "update", "id": existing.id}


async def _handle_split_completed(
    db: AsyncSession, ticker: str, kap_url: str, body: str,
) -> Optional[dict]:
    """Stage 4: Bolunme gunu — status=tamamlandi."""
    parsed = parse_split_completed(body)
    if not parsed:
        return None

    # Body'deki ticker hisse ile eslessin
    if parsed["ticker"] != ticker:
        logger.warning("cap_inc split: ticker uyusmazligi body=%s param=%s", parsed["ticker"], ticker)
        # Body'deki ticker'i kullan (KAP genelde dogru)
        ticker = parsed["ticker"]

    # Active kayit (her statu, dahil tamamlandi - duplicate koruma)
    existing = (await db.execute(
        select(CapitalIncrease).where(
            CapitalIncrease.ticker == ticker,
            CapitalIncrease.status.in_(["ykk_alindi", "spk_onayli", "tarih_belli", "dagitiliyor", "tamamlandi"]),
        ).order_by(CapitalIncrease.created_at.desc())
    )).scalars().first()

    # Zaten tamamlanmis ayni oran ile -> idempotent skip
    if existing and existing.status == "tamamlandi":
        existing_pct = existing.bedelli_pct or existing.bedelsiz_pct or existing.tahsisli_pct
        if existing_pct and abs((existing_pct or 0) - pct) < 0.5:
            logger.info("cap_inc split SKIP (zaten tamamlandi): %s id=%s", ticker, existing.id)
            return {"stage": "split_completed", "action": "skip", "id": existing.id}

    today = _date.today()
    pct = parsed["percentage"]
    tip = parsed["type"]

    if not existing:
        # Aktif kayit yok — yeni olustur (degerlendirme isleminin once dustugu durum)
        existing = CapitalIncrease(
            ticker=ticker,
            type=tip,
            status="tamamlandi",
            distribution_date=today,
            distribution_kap_url=kap_url,
        )
        if tip == "bedelli":
            existing.bedelli_pct = pct
        elif tip == "bedelsiz":
            existing.bedelsiz_pct = pct
        elif tip == "tahsisli":
            existing.tahsisli_pct = pct
        db.add(existing)
        await db.commit()
        await db.refresh(existing)
        logger.info("cap_inc split INSERT: %s id=%s type=%s pct=%s", ticker, existing.id, tip, pct)
        return {"stage": "split_completed", "action": "insert", "id": existing.id}

    # Mevcut kaydi tamamlandi'ya cevir
    existing.status = "tamamlandi"
    existing.distribution_date = existing.distribution_date or today
    if not existing.distribution_kap_url:
        existing.distribution_kap_url = kap_url
    # Daha kesin oran KAP'tan geldi — guncelle
    if tip == "bedelli" and pct:
        existing.bedelli_pct = pct
    elif tip == "bedelsiz" and pct:
        existing.bedelsiz_pct = pct
    elif tip == "tahsisli" and pct:
        existing.tahsisli_pct = pct
    await db.commit()
    logger.info("cap_inc split UPDATE: %s id=%s -> tamamlandi", ticker, existing.id)
    return {"stage": "split_completed", "action": "update", "id": existing.id}
