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
        """Yaklasan ve dagitim surecindeki halka arzlari getirir."""
        query = select(IPO).where(
            IPO.status.in_(["newly_approved", "in_distribution", "upcoming", "active"])
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

        Eslestirme onceligi:
        1. ticker (hisse kodu)
        2. kap_notification_url (KAP bildirim linki)
        3. company_name (sirket adi — duplikat onleme)
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

        # company_name ile de eslestir — duplikat onleme
        if not existing and data.get("company_name"):
            result = await self.db.execute(
                select(IPO).where(IPO.company_name == data["company_name"])
            )
            existing = result.scalar_one_or_none()

        if existing:
            # Guncelle — sadece None olmayan alanlari
            # status alanini scraper'dan gelen veriyle GERI almayiz
            # (auto_update_statuses zaten dogru statusu ayarlar)
            protected_fields = {"status", "id", "created_at", "archived", "archived_at"}
            for key, value in data.items():
                if value is not None and hasattr(existing, key) and key not in protected_fields:
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
        open_price: Optional[Decimal] = None,
        high_price: Optional[Decimal] = None,
        low_price: Optional[Decimal] = None,
        hit_floor: bool = False,
    ) -> IPOCeilingTrack:
        """Tavan/taban takip bilgisini gunceller veya olusturur.

        OHLC fiyat verileri + tavan/taban durumu kaydedilir.
        Onceki gun tavan degildi ama bugun tavan ise → relock (tekrar kitlendi).
        """
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
            # Mevcut kaydi guncelle
            track.trade_date = trade_date
            track.open_price = open_price
            track.close_price = close_price
            track.high_price = high_price
            track.low_price = low_price
            track.hit_ceiling = hit_ceiling
            track.hit_floor = hit_floor

            if not hit_ceiling and not track.ceiling_broken_at:
                track.ceiling_broken_at = datetime.utcnow()

            if hit_floor and not track.floor_hit_at:
                track.floor_hit_at = datetime.utcnow()

        else:
            track = IPOCeilingTrack(
                ipo_id=ipo_id,
                trading_day=trading_day,
                trade_date=trade_date,
                open_price=open_price,
                close_price=close_price,
                high_price=high_price,
                low_price=low_price,
                hit_ceiling=hit_ceiling,
                hit_floor=hit_floor,
                ceiling_broken_at=datetime.utcnow() if not hit_ceiling else None,
                floor_hit_at=datetime.utcnow() if hit_floor else None,
            )
            self.db.add(track)

        # Relock kontrolu — onceki gun tavan degildi, bugun tavan
        if hit_ceiling and trading_day > 1:
            prev_result = await self.db.execute(
                select(IPOCeilingTrack).where(
                    and_(
                        IPOCeilingTrack.ipo_id == ipo_id,
                        IPOCeilingTrack.trading_day == trading_day - 1,
                    )
                )
            )
            prev_track = prev_result.scalar_one_or_none()
            if prev_track and not prev_track.hit_ceiling:
                track.relocked = True
                track.relocked_at = datetime.utcnow()

        # Ana IPO kaydini al
        ipo = await self.get_ipo_by_id(ipo_id)

        # Ana IPO kaydini da guncelle
        if not hit_ceiling:
            if ipo and not ipo.ceiling_broken:
                ipo.ceiling_broken = True
                ipo.ceiling_broken_at = datetime.utcnow()

        # Ilk gun kapanis fiyatini kaydet
        if trading_day == 1:
            if ipo and not ipo.first_day_close_price:
                ipo.first_day_close_price = close_price

        # v4: Gunluk % degisim hesapla (onceki gun kapanisina gore)
        ipo_price = ipo.ipo_price if ipo else None
        daily_pct = None

        if trading_day > 1:
            prev_result = await self.db.execute(
                select(IPOCeilingTrack).where(
                    and_(
                        IPOCeilingTrack.ipo_id == ipo_id,
                        IPOCeilingTrack.trading_day == trading_day - 1,
                    )
                )
            )
            prev_track = prev_result.scalar_one_or_none()
            if prev_track and prev_track.close_price and prev_track.close_price > 0:
                daily_pct = ((close_price - prev_track.close_price) / prev_track.close_price) * 100
        elif trading_day == 1 and ipo_price and ipo_price > 0:
            daily_pct = ((close_price - ipo_price) / ipo_price) * 100

        # pct_change = gunluk degisim (eski: kumulatif)
        track.pct_change = daily_pct

        # v3: 5 durum — gunluk degisime gore
        if hit_ceiling:
            track.durum = "tavan"
        elif hit_floor:
            track.durum = "taban"
        elif daily_pct is not None and daily_pct == 0:
            track.durum = "not_kapatti"
        elif daily_pct is not None and daily_pct > 0:
            track.durum = "alici_kapatti"
        else:
            track.durum = "satici_kapatti"

        await self.db.flush()
        return track

    # -------------------------------------------------------
    # Otomatik Durum Guncelleme
    # -------------------------------------------------------

    async def auto_update_statuses(self):
        """Tarihlere gore IPO durumlarini otomatik gunceller — 5 bolumlu akis.

        Yeni akis (5 bolum):
        1. newly_approved → in_distribution:  subscription_start geldiyse
        2. in_distribution → awaiting_trading: subscription_end gectiyse
        3. awaiting_trading → trading:         trading_start geldiyse

        Geriye donuk uyumluluk (eski status degerleri):
        - upcoming → newly_approved  (one-time migration)
        - active → in_distribution   (one-time migration)
        - completed → trading        (eger trading_start varsa, one-time migration)
        """
        today = date.today()

        # ==========================================
        # GERIYE DONUK UYUMLULUK — eski statuslari yeni sisteme tasi
        # ==========================================

        # upcoming → newly_approved
        result = await self.db.execute(
            select(IPO).where(IPO.status == "upcoming")
        )
        for ipo in result.scalars().all():
            ipo.status = "newly_approved"
            ipo.updated_at = datetime.utcnow()
            logger.info(f"Eski status migration: {ipo.ticker} upcoming → newly_approved")

        # active → in_distribution
        result = await self.db.execute(
            select(IPO).where(IPO.status == "active")
        )
        for ipo in result.scalars().all():
            ipo.status = "in_distribution"
            ipo.updated_at = datetime.utcnow()
            logger.info(f"Eski status migration: {ipo.ticker} active → in_distribution")

        # completed → trading (sadece trading_start varsa)
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "completed",
                    IPO.trading_start.isnot(None),
                )
            )
        )
        for ipo in result.scalars().all():
            ipo.status = "trading"
            ipo.updated_at = datetime.utcnow()
            logger.info(f"Eski status migration: {ipo.ticker} completed → trading")

        # ==========================================
        # 1. newly_approved → in_distribution
        #    Kosul: subscription_start geldiyse
        # ==========================================
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "newly_approved",
                    IPO.subscription_start.isnot(None),
                    IPO.subscription_start <= today,
                )
            )
        )
        for ipo in result.scalars().all():
            ipo.status = "in_distribution"
            ipo.updated_at = datetime.utcnow()
            logger.info(f"IPO dagitim surecinde: {ipo.ticker or ipo.company_name}")

        # ==========================================
        # 2. in_distribution → awaiting_trading
        #    Kosul: subscription_end gectiyse
        # ==========================================
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "in_distribution",
                    IPO.subscription_end.isnot(None),
                    IPO.subscription_end < today,
                )
            )
        )
        for ipo in result.scalars().all():
            ipo.status = "awaiting_trading"
            ipo.distribution_completed = True
            ipo.updated_at = datetime.utcnow()
            logger.info(f"IPO islem gunu bekliyor: {ipo.ticker or ipo.company_name}")

        # ==========================================
        # 3. awaiting_trading → trading
        #    Kosul: trading_start set edilmis VE bugune esit veya gecmis
        # ==========================================
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "awaiting_trading",
                    IPO.trading_start.isnot(None),
                    IPO.trading_start <= today,
                )
            )
        )
        for ipo in result.scalars().all():
            ipo.status = "trading"
            ipo.ceiling_tracking_active = True
            ipo.updated_at = datetime.utcnow()
            logger.info(f"IPO isleme basladi: {ipo.ticker or ipo.company_name}")

        await self.db.flush()

    async def get_last_day_ipos(self) -> list[IPO]:
        """Yarin son gunu olan dagitim surecindeki halka arzlari dondurur."""
        tomorrow = date.today() + timedelta(days=1)
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status.in_(["in_distribution", "active"]),  # yeni + geriye uyumluluk
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
