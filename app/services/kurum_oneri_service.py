"""Kurum Onerileri Service — CRUD + Bildirim.

hedeffiyat.com.tr'den gelen araci kurum onerilerini DB'ye yazar.
Yeni oneri tespit edildiginde bildirim gonderir.
"""

import logging
from datetime import date, timedelta
from typing import Optional

from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.kurum_oneri import KurumOneri

logger = logging.getLogger(__name__)


class KurumOneriService:
    """Kurum onerileri CRUD ve bildirim servisi."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_or_update(self, data: dict) -> tuple[KurumOneri, bool]:
        """Oneriyi olustur veya guncelle. (oneri, is_new) doner."""
        ticker = data.get("ticker", "").upper()
        institution = data.get("institution_name", "")
        report_date = data.get("report_date")

        if not ticker or not institution or not report_date:
            logger.debug("Eksik veri: ticker=%s, institution=%s, date=%s", ticker, institution, report_date)
            return None, False

        # Mevcut kayit var mi?
        result = await self.db.execute(
            select(KurumOneri).where(
                and_(
                    KurumOneri.ticker == ticker,
                    KurumOneri.institution_name == institution,
                    KurumOneri.report_date == report_date,
                )
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            # Guncelle
            changed = False
            for field in ["recommendation", "target_price", "current_price", "potential_return", "company_name", "source_url"]:
                new_val = data.get(field)
                if new_val is not None and new_val != getattr(existing, field):
                    setattr(existing, field, new_val)
                    changed = True
            return existing, False
        else:
            # Yeni kayit
            oneri = KurumOneri(
                ticker=ticker,
                company_name=data.get("company_name"),
                institution_name=institution,
                recommendation=data.get("recommendation"),
                target_price=data.get("target_price"),
                current_price=data.get("current_price"),
                potential_return=data.get("potential_return"),
                report_date=report_date,
                source_url=data.get("source_url"),
            )
            self.db.add(oneri)
            return oneri, True

    async def get_recommendations(
        self,
        period: str = "today",
        ticker: Optional[str] = None,
        institution: Optional[str] = None,
        recommendation: Optional[str] = None,
        page: int = 1,
        limit: int = 100,
    ) -> tuple[list[KurumOneri], int]:
        """Filtrelenebilir oneri listesi dondur. (items, total_count)"""
        query = select(KurumOneri)
        count_query = select(func.count(KurumOneri.id))

        # Periyot filtresi
        today = date.today()
        if period == "today":
            query = query.where(KurumOneri.report_date == today)
            count_query = count_query.where(KurumOneri.report_date == today)
        elif period == "week":
            week_start = today - timedelta(days=today.weekday())  # Pazartesi
            query = query.where(KurumOneri.report_date >= week_start)
            count_query = count_query.where(KurumOneri.report_date >= week_start)
        elif period == "month":
            month_start = today.replace(day=1)
            query = query.where(KurumOneri.report_date >= month_start)
            count_query = count_query.where(KurumOneri.report_date >= month_start)
        elif period == "all":
            # Son 3 ay (free kullanicilar icin)
            three_months_ago = today - timedelta(days=90)
            query = query.where(KurumOneri.report_date >= three_months_ago)
            count_query = count_query.where(KurumOneri.report_date >= three_months_ago)

        # Ek filtreler
        if ticker:
            query = query.where(KurumOneri.ticker == ticker.upper())
            count_query = count_query.where(KurumOneri.ticker == ticker.upper())
        if institution:
            query = query.where(KurumOneri.institution_name.ilike(f"%{institution}%"))
            count_query = count_query.where(KurumOneri.institution_name.ilike(f"%{institution}%"))
        if recommendation:
            query = query.where(KurumOneri.recommendation.ilike(f"%{recommendation}%"))
            count_query = count_query.where(KurumOneri.recommendation.ilike(f"%{recommendation}%"))

        # Toplam sayi
        total_result = await self.db.execute(count_query)
        total = total_result.scalar() or 0

        # Sayfalama + siralama (en yeni onceye)
        offset = (page - 1) * limit
        query = query.order_by(
            KurumOneri.report_date.desc(),
            KurumOneri.created_at.desc(),
        ).offset(offset).limit(limit)

        result = await self.db.execute(query)
        items = list(result.scalars().all())

        return items, total

    async def get_institutions(self) -> list[str]:
        """Benzersiz kurum isimlerini dondur (filtre icin)."""
        result = await self.db.execute(
            select(KurumOneri.institution_name)
            .distinct()
            .order_by(KurumOneri.institution_name)
        )
        return [row[0] for row in result.all()]

    async def get_tickers(self) -> list[str]:
        """Benzersiz hisse kodlarini dondur (filtre icin)."""
        result = await self.db.execute(
            select(KurumOneri.ticker)
            .distinct()
            .order_by(KurumOneri.ticker)
        )
        return [row[0] for row in result.all()]

    async def get_stats(self) -> dict:
        """Genel istatistikler."""
        today = date.today()

        today_count = await self.db.execute(
            select(func.count(KurumOneri.id)).where(
                KurumOneri.report_date == today
            )
        )
        total_count = await self.db.execute(
            select(func.count(KurumOneri.id))
        )
        institution_count = await self.db.execute(
            select(func.count(func.distinct(KurumOneri.institution_name)))
        )

        return {
            "today_count": today_count.scalar() or 0,
            "total_count": total_count.scalar() or 0,
            "institution_count": institution_count.scalar() or 0,
        }
