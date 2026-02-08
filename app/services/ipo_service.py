"""Halka Arz (IPO) is mantigi servisi.

KAP ve SPK scraper'larindan gelen verileri islayarak
veritabanina kaydeder ve gunceller.
"""

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ipo import IPO, IPOBroker, IPOAllocation, IPOCeilingTrack

logger = logging.getLogger(__name__)


class IPOService:
    """Halka arz islemleri servisi."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # -------------------------------------------------------
    # Listeleme
    # -------------------------------------------------------

    async def get_all_ipos(
        self,
        status: Optional[str] = None,
        year: Optional[int] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[IPO]:
        """Halka arz listesini getirir, filtreli."""
        query = select(IPO).order_by(IPO.subscription_start.desc().nullslast())

        if status:
            query = query.where(IPO.status == status)

        if year:
            query = query.where(
                or_(
                    IPO.subscription_start >= date(year, 1, 1),
                    IPO.trading_start >= date(year, 1, 1),
                )
            )

        query = query.limit(limit).offset(offset)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_upcoming_ipos(self) -> list[IPO]:
        """Yaklasan ve aktif halka arzlari getirir."""
        query = select(IPO).where(
            IPO.status.in_(["upcoming", "active"])
        ).order_by(IPO.subscription_start.asc().nullslast())

        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_ipo_by_id(self, ipo_id: int) -> Optional[IPO]:
        """ID ile halka arz detayini getirir."""
        result = await self.db.execute(select(IPO).where(IPO.id == ipo_id))
        return result.scalar_one_or_none()

    async def get_ipo_by_ticker(self, ticker: str) -> Optional[IPO]:
        """Hisse kodu ile halka arz getirir."""
        result = await self.db.execute(
            select(IPO).where(IPO.ticker == ticker.upper())
        )
        return result.scalar_one_or_none()

    # -------------------------------------------------------
    # Olusturma & Guncelleme
    # -------------------------------------------------------

    async def create_or_update_ipo(self, data: dict) -> IPO:
        """KAP/SPK verisinden halka arz olusturur veya gunceller.

        Eger ayni kap_notification_url veya ticker varsa gunceller,
        yoksa yeni olusturur.
        """
        # Mevcut kaydi kontrol et
        existing = None
        if data.get("ticker"):
            existing = await self.get_ipo_by_ticker(data["ticker"])

        if not existing and data.get("kap_notification_url"):
            result = await self.db.execute(
                select(IPO).where(IPO.kap_notification_url == data["kap_notification_url"])
            )
            existing = result.scalar_one_or_none()

        if existing:
            # Guncelle — sadece None olmayan alanlari
            for key, value in data.items():
                if value is not None and hasattr(existing, key):
                    setattr(existing, key, value)
            existing.updated_at = datetime.utcnow()
            ipo = existing
            logger.info(f"IPO guncellendi: {ipo.ticker or ipo.company_name}")
        else:
            # Yeni olustur
            ipo = IPO(**{k: v for k, v in data.items() if hasattr(IPO, k) and v is not None})
            self.db.add(ipo)
            logger.info(f"Yeni IPO olusturuldu: {ipo.ticker or ipo.company_name}")

        await self.db.flush()
        return ipo

    async def update_ipo_status(self, ipo_id: int, new_status: str) -> Optional[IPO]:
        """Halka arz durumunu gunceller."""
        ipo = await self.get_ipo_by_id(ipo_id)
        if not ipo:
            return None

        ipo.status = new_status
        ipo.updated_at = datetime.utcnow()

        logger.info(f"IPO durum guncellendi: {ipo.ticker} -> {new_status}")
        return ipo

    # -------------------------------------------------------
    # Araci Kurum (Broker) Islemleri
    # -------------------------------------------------------

    async def set_brokers(self, ipo_id: int, brokers: list[dict]) -> list[IPOBroker]:
        """Halka arz icin araci kurum/banka listesini ayarlar."""
        # Mevcut brokerları sil
        result = await self.db.execute(
            select(IPOBroker).where(IPOBroker.ipo_id == ipo_id)
        )
        for existing in result.scalars().all():
            await self.db.delete(existing)

        # Yeni broker'lari ekle
        new_brokers = []
        for b in brokers:
            broker = IPOBroker(
                ipo_id=ipo_id,
                broker_name=b["name"],
                broker_type=b.get("type"),
                application_url=b.get("url"),
                phone=b.get("phone"),
            )
            self.db.add(broker)
            new_brokers.append(broker)

        await self.db.flush()
        return new_brokers

    # -------------------------------------------------------
    # Tahsisat Sonuclari
    # -------------------------------------------------------

    async def set_allocation_results(self, ipo_id: int, allocations: list[dict]) -> list[IPOAllocation]:
        """Tahsisat sonuclarini kaydeder."""
        result = await self.db.execute(
            select(IPOAllocation).where(IPOAllocation.ipo_id == ipo_id)
        )
        for existing in result.scalars().all():
            await self.db.delete(existing)

        new_allocations = []
        for a in allocations:
            alloc = IPOAllocation(
                ipo_id=ipo_id,
                group_name=a["group"],
                allocation_pct=a.get("pct"),
                allocated_lots=a.get("lots"),
                participant_count=a.get("participants"),
                avg_lot_per_person=a.get("avg_lot"),
            )
            self.db.add(alloc)
            new_allocations.append(alloc)

        # IPO'yu guncelle
        ipo = await self.get_ipo_by_id(ipo_id)
        if ipo:
            ipo.allocation_announced = True
            ipo.updated_at = datetime.utcnow()

        await self.db.flush()
        return new_allocations

    # -------------------------------------------------------
    # Tavan Takip
    # -------------------------------------------------------

    async def update_ceiling_track(
        self,
        ipo_id: int,
        trading_day: int,
        trade_date: date,
        close_price: Decimal,
        hit_ceiling: bool,
    ) -> IPOCeilingTrack:
        """Tavan takip bilgisini gunceller veya olusturur."""
        result = await self.db.execute(
            select(IPOCeilingTrack).where(
                and_(
                    IPOCeilingTrack.ipo_id == ipo_id,
                    IPOCeilingTrack.trading_day == trading_day,
                )
            )
        )
        track = result.scalar_one_or_none()

        if track:
            track.close_price = close_price
            track.hit_ceiling = hit_ceiling
            if not hit_ceiling and not track.ceiling_broken_at:
                track.ceiling_broken_at = datetime.utcnow()
        else:
            track = IPOCeilingTrack(
                ipo_id=ipo_id,
                trading_day=trading_day,
                trade_date=trade_date,
                close_price=close_price,
                hit_ceiling=hit_ceiling,
                ceiling_broken_at=datetime.utcnow() if not hit_ceiling else None,
            )
            self.db.add(track)

        # Ana IPO kaydini da guncelle
        if not hit_ceiling:
            ipo = await self.get_ipo_by_id(ipo_id)
            if ipo and not ipo.ceiling_broken:
                ipo.ceiling_broken = True
                ipo.ceiling_broken_at = datetime.utcnow()

        await self.db.flush()
        return track

    # -------------------------------------------------------
    # Otomatik Durum Guncelleme
    # -------------------------------------------------------

    async def auto_update_statuses(self):
        """Tarihlere gore IPO durumlarini otomatik gunceller.

        - upcoming → active: basvuru baslangic tarihi geldiyse
        - active → completed: basvuru bitis tarihi gectiyse
        """
        today = date.today()

        # upcoming → active
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "upcoming",
                    IPO.subscription_start <= today,
                )
            )
        )
        for ipo in result.scalars().all():
            ipo.status = "active"
            ipo.updated_at = datetime.utcnow()
            logger.info(f"IPO aktif oldu: {ipo.ticker}")

        # active → completed
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "active",
                    IPO.subscription_end < today,
                )
            )
        )
        for ipo in result.scalars().all():
            ipo.status = "completed"
            ipo.updated_at = datetime.utcnow()
            logger.info(f"IPO tamamlandi: {ipo.ticker}")

        await self.db.flush()

    async def get_last_day_ipos(self) -> list[IPO]:
        """Yarin son gunu olan aktif halka arzlari dondurur."""
        tomorrow = date.today() + timedelta(days=1)
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "active",
                    IPO.subscription_end == tomorrow,
                )
            )
        )
        return list(result.scalars().all())

    async def get_ceiling_tracking_ipos(self) -> list[IPO]:
        """Tavan takibi aktif olan halka arzlari getirir."""
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.ceiling_tracking_active == True,
                    IPO.ceiling_broken == False,
                )
            )
        )
        return list(result.scalars().all())
