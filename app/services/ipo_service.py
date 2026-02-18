"""Halka Arz (IPO) is mantigi servisi.

KAP ve SPK scraper'larindan gelen verileri islayarak
veritabanina kaydeder ve gunceller.
"""

import logging
import re
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ipo import IPO, IPOBroker, IPOAllocation, IPOCeilingTrack, DeletedIPO

logger = logging.getLogger(__name__)

# Kara liste — silinen IPO ilk 2 kelime karsilastirmasi
_SKIP_WORDS = {"a.ş.", "a.s.", "aş", "san.", "tic.", "ve", "ltd.", "şti.", "sti."}


def _first_two_words_match(name1: str, name2: str) -> bool:
    """Iki sirket adinin ilk 2 anlamli kelimesi eslesiyor mu?"""
    def _get_words(n: str) -> list[str]:
        words = re.sub(r"\s+", " ", n.strip()).lower().split()
        return [w for w in words if w not in _SKIP_WORDS][:2]
    w1 = _get_words(name1 or "")
    w2 = _get_words(name2 or "")
    return len(w1) >= 2 and len(w2) >= 2 and w1 == w2


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

    async def create_or_update_ipo(self, data: dict, allow_create: bool = False) -> IPO | None:
        """KAP/SPK verisinden halka arz olusturur veya gunceller.

        ONEMLI: Yeni IPO olusturma SADECE iki kaynaktan yapilabilir:
        1. SPK Bulten scraper (allow_create=True)
        2. Admin panel (allow_create=True)

        Diger scraper'lar (HalkArz, Gedik, InfoYatirim, SPK Ihrac) sadece
        mevcut IPO'lari guncelleyebilir (allow_create=False).

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
        # Oncelik 1: birebir eslestir
        # Oncelik 2: normalize edilmis fuzzy eslestirme (\n, kisaltmalar, bosluklar)
        if not existing and data.get("company_name"):
            result = await self.db.execute(
                select(IPO).where(IPO.company_name == data["company_name"])
            )
            existing = result.scalar_one_or_none()

            # Fuzzy eslestirme — \n temizligi, kisaltma farkliliklari
            if not existing:
                import re
                def _norm(n: str) -> str:
                    """Sirket adini normalize et: kucuk harf, \n temizle, kisaltmalari ac."""
                    n = n.replace("\n", " ").replace("\r", " ")
                    n = re.sub(r"\s+", " ", n).strip().lower()
                    # Yaygin kisaltmalari kaldir (eslesme kolayligi)
                    for abbr in ["a.ş.", "aş", "a.s.", "san.", "tic.", "ve", "ltd.", "şti."]:
                        n = n.replace(abbr, "")
                    return re.sub(r"\s+", " ", n).strip()

                incoming_norm = _norm(data["company_name"])
                if len(incoming_norm) >= 4:  # Cok kisa isimlerde false match onle
                    all_ipos_result = await self.db.execute(select(IPO))
                    for ipo_row in all_ipos_result.scalars().all():
                        db_norm = _norm(ipo_row.company_name or "")
                        if not db_norm:
                            continue
                        # Birebir normalize eslestirme veya biri digerini iceriyor
                        if (db_norm == incoming_norm
                            or incoming_norm.startswith(db_norm[:15])
                            or db_norm.startswith(incoming_norm[:15])):
                            existing = ipo_row
                            logger.info(
                                "IPO fuzzy eslesti: '%s' → '%s'",
                                data["company_name"], ipo_row.company_name,
                            )
                            break

        if existing:
            # GUARD: trading durumundaki IPO'lari scraper'lar guncelleyemez.
            # Islem basladiktan sonra bilgi cekmeye gerek yok — veri tamamlanmis.
            # Sadece admin (allow_create=True) bu korumayı bypass edebilir.
            if (
                not allow_create
                and existing.status == "trading"
                and existing.trading_start is not None
            ):
                logger.debug(
                    "IPO trading durumunda, guncelleme atlanıyor: %s",
                    existing.ticker or existing.company_name,
                )
                return existing

            # Guncelle — sadece None olmayan alanlari
            # status alanini scraper'dan gelen veriyle GERI almayiz
            # (auto_update_statuses zaten dogru statusu ayarlar)
            protected_fields = {"status", "id", "created_at", "archived", "archived_at"}

            # Admin korumasi: manual_fields'ta listelenen alanlar scraper tarafindan ezilemez
            manual_locked = set()
            if not allow_create and existing.manual_fields:
                try:
                    import json as _json
                    fields = _json.loads(existing.manual_fields)
                    if isinstance(fields, list):
                        manual_locked = set(fields)
                except (ValueError, TypeError):
                    pass

            for key, value in data.items():
                if value is not None and hasattr(existing, key) and key not in protected_fields:
                    if key in manual_locked:
                        continue  # Admin kilidi — dokunma
                    setattr(existing, key, value)
            existing.updated_at = datetime.utcnow()

            # ONEMLI: Arsivlenmis bir IPO'ya yeni veriler (subscription_start, ticker vb.)
            # geliyorsa → arsivden cikar. Scraper yeni bilgi getirdiyse IPO hala aktif demektir.
            if existing.archived:
                new_has_dates = data.get("subscription_start") or data.get("subscription_end") or data.get("trading_start")
                new_has_ticker = data.get("ticker")
                if new_has_dates or new_has_ticker:
                    existing.archived = False
                    existing.archived_at = None
                    logger.info(
                        "IPO arsivden cikarildi (yeni veri geldi): %s — ticker=%s, sub_start=%s",
                        existing.company_name, data.get("ticker"), data.get("subscription_start"),
                    )
                    # Arsivden cikinca status'u da kontrol et
                    # subscription_start varsa ve bugun veya gecmisse → in_distribution
                    # yoksa → newly_approved'a geri al
                    if not existing.status or existing.status in ("archived",):
                        existing.status = "newly_approved"

            ipo = existing
            logger.info(f"IPO guncellendi: {ipo.ticker or ipo.company_name}")
        else:
            if not allow_create:
                # Yeni IPO olusturma izni yok — sadece SPK bulten ve admin yapabilir
                logger.info(
                    f"IPO bulunamadi, olusturma atlanıyor (allow_create=False): "
                    f"{data.get('ticker') or data.get('company_name')}"
                )
                return None

            # Kara liste kontrolu — admin tarafindan silinen sirketleri tekrar ekleme
            incoming_name = data.get("company_name", "")
            if incoming_name:
                deleted_result = await self.db.execute(select(DeletedIPO))
                for del_row in deleted_result.scalars().all():
                    if _first_two_words_match(incoming_name, del_row.company_name):
                        logger.info(
                            "IPO kara listede, eklenmedi: '%s' ≈ '%s' (silindi: %s)",
                            incoming_name, del_row.company_name,
                            del_row.deleted_at.strftime("%Y-%m-%d") if del_row.deleted_at else "?",
                        )
                        return None

            # Son kontrol — DB'ye flush yaparak race condition'da duplicate onle
            # (ayni company_name + spk_bulletin_no kombinasyonu zaten varsa ekleme)
            if data.get("company_name") and data.get("spk_bulletin_no"):
                from sqlalchemy import and_ as _and2
                _dup_result = await self.db.execute(
                    select(IPO).where(
                        _and2(
                            IPO.company_name == data["company_name"],
                            IPO.spk_bulletin_no == data["spk_bulletin_no"],
                        )
                    )
                )
                _dup_existing = _dup_result.scalar_one_or_none()
                if _dup_existing:
                    logger.warning(
                        "IPO duplicate onlendi (ayni bulten+isim): %s (%s)",
                        data["company_name"], data["spk_bulletin_no"],
                    )
                    return _dup_existing

            # Yeni olustur — sadece SPK bulten veya admin kaynaklarından
            ipo = IPO(**{k: v for k, v in data.items() if hasattr(IPO, k) and v is not None})
            self.db.add(ipo)
            logger.info(f"Yeni IPO olusturuldu: {ipo.ticker or ipo.company_name}")

            # NOT: Tweet artik burada atilmiyor.
            # check_spk_bulletins() icerisinde tum onaylar toplandiktan sonra
            # tweet_new_ipos_batch() ile tek tweet olarak atiliyor.

            # SPKApplication tablosunda varsa → approved yap
            try:
                from sqlalchemy import and_ as _and
                from app.models.spk_application import SPKApplication as _SPKApp
                _spk_result = await self.db.execute(
                    select(_SPKApp).where(
                        _and(
                            _SPKApp.status == "pending",
                            _SPKApp.company_name.ilike(
                                f"%{ipo.company_name[:30]}%"
                            ),
                        )
                    )
                )
                for _spk_app in _spk_result.scalars().all():
                    _spk_app.status = "approved"
                    logger.info("SPKApplication approved: %s (id=%d)", _spk_app.company_name, _spk_app.id)
            except Exception:
                pass  # SPK listesi guncelleme hatasi sistemi etkilemez

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

        # Tweet #3: Kesinlesen Dagitim Sonuclari
        if ipo:
            try:
                from app.services.twitter_service import tweet_allocation_results
                from app.services.admin_telegram import notify_tweet_sent
                tw_ok = tweet_allocation_results(ipo, new_allocations)
                await notify_tweet_sent("dagitim_sonucu", ipo.ticker or ipo.company_name, tw_ok)
            except Exception:
                pass  # Tweet hatasi sistemi etkilemez

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
        alis_lot: Optional[int] = None,
        satis_lot: Optional[int] = None,
        pct_change: Optional[float] = None,
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
            track.alis_lot = alis_lot
            track.satis_lot = satis_lot

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
                alis_lot=alis_lot,
                satis_lot=satis_lot,
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
        # Hesaplanan deger yoksa Excel'den gelen gun_fark degerini kullan
        if daily_pct is None and pct_change is not None:
            daily_pct = pct_change
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
        # TUTARSIZLIK DUZELTME — status ile tarihler uyusmuyor
        # KURAL: Adim atlamak YASAK! Her gecis onceki adimdaki tarihi gerektirir.
        # newly_approved → in_distribution: subscription_start <= today
        # in_distribution → awaiting_trading: subscription_end < today
        # awaiting_trading → trading: trading_start <= today
        # ==========================================

        # Tutarsiz IPO'lari bul (tarihler status'un ilerisinde)
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status.in_(["newly_approved", "in_distribution", "awaiting_trading"]),
                    IPO.archived == False,
                )
            )
        )
        for ipo in result.scalars().all():
            old_status = ipo.status
            new_status = old_status

            # Adim 1: newly_approved → in_distribution
            # SADECE subscription_start gelmisse
            if new_status == "newly_approved":
                if ipo.subscription_start and ipo.subscription_start <= today:
                    new_status = "in_distribution"
                else:
                    continue  # subscription_start gelmeden hicbir yere gidemez

            # Adim 2: in_distribution → awaiting_trading
            # SADECE subscription_end gecmisse
            if new_status == "in_distribution":
                if ipo.subscription_end and ipo.subscription_end < today:
                    new_status = "awaiting_trading"
                    ipo.distribution_completed = True
                else:
                    # Henuz dagitim bitmedi, bu adimda kal
                    if new_status != old_status:
                        # Sadece 1. adimda ilerleme olduysa kaydet
                        ipo.status = new_status
                        ipo.updated_at = datetime.utcnow()
                        logger.info(
                            f"Tutarsizlik duzeltme: {ipo.ticker} {old_status} → {new_status}"
                        )
                    continue

            # Adim 3: awaiting_trading → trading
            # SADECE trading_start gelmisse
            if new_status == "awaiting_trading":
                if ipo.trading_start and ipo.trading_start <= today:
                    new_status = "trading"
                    ipo.ceiling_tracking_active = True

            # Degisiklik varsa kaydet
            if new_status != old_status:
                ipo.status = new_status
                ipo.updated_at = datetime.utcnow()
                logger.info(
                    f"Tutarsizlik duzeltme: {ipo.ticker} {old_status} → {new_status} "
                    f"(sub_start={ipo.subscription_start}, sub_end={ipo.subscription_end}, "
                    f"trading_start={ipo.trading_start})"
                )

        # subscription_end 90+ gun gecmis VE hala awaiting_trading'de takili kalanlar
        # → trading_start yoksa arsivle (DNYVA gibi: sub_end=2025-01-24, trading_start=None)
        # DIKKAT: subscription_start gelecekte olan IPO'lari arsivleme! (henuz dagitim baslamadi)
        stale_cutoff = today - timedelta(days=90)
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status.in_(["newly_approved", "in_distribution", "awaiting_trading"]),
                    IPO.trading_start.is_(None),
                    IPO.archived == False,
                    # subscription_start veya subscription_end gelecekte ise arsivleme
                    or_(
                        IPO.subscription_start.is_(None),
                        IPO.subscription_start < today,
                    ),
                    or_(
                        IPO.subscription_end.is_(None),
                        IPO.subscription_end < today,
                    ),
                    or_(
                        and_(IPO.subscription_end.isnot(None), IPO.subscription_end < stale_cutoff),
                        and_(IPO.spk_approval_date.isnot(None), IPO.spk_approval_date < stale_cutoff),
                        and_(
                            IPO.subscription_end.is_(None),
                            IPO.spk_approval_date.is_(None),
                            IPO.created_at.isnot(None),
                            IPO.created_at < datetime.utcnow() - timedelta(days=90),
                        ),
                    ),
                )
            )
        )
        for ipo in result.scalars().all():
            # Son guvenlik kontrolu: subscription_start veya subscription_end
            # gelecekte ise bu IPO hala aktif, arsivleme
            if ipo.subscription_start and ipo.subscription_start >= today:
                continue
            if ipo.subscription_end and ipo.subscription_end >= today:
                continue

            old_status = ipo.status
            ipo.archived = True
            ipo.archived_at = datetime.utcnow()
            ipo.updated_at = datetime.utcnow()
            logger.info(
                f"Eski IPO arsivlendi: {ipo.ticker} {old_status} → archived "
                f"(sub_end={ipo.subscription_end}, spk_date={ipo.spk_approval_date})"
            )
            try:
                from app.services.admin_telegram import notify_scraper_error
                await notify_scraper_error(
                    "IPO Arşivlendi",
                    f"{ipo.ticker or ipo.company_name} — {old_status} → archived"
                )
            except Exception:
                pass

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
                    IPO.archived == False,
                    IPO.subscription_start.isnot(None),
                    IPO.subscription_start <= today,
                )
            )
        )
        for ipo in result.scalars().all():
            ipo.status = "in_distribution"
            ipo.updated_at = datetime.utcnow()
            logger.info(f"IPO dagitim surecinde: {ipo.ticker or ipo.company_name}")
            try:
                from app.services.admin_telegram import notify_ipo_status_change
                await notify_ipo_status_change(
                    ipo.ticker, ipo.company_name,
                    "newly_approved", "in_distribution",
                    f"Basvuru: {ipo.subscription_start}" if ipo.subscription_start else "",
                )
            except Exception:
                pass
            # Tweet #2: Dagitima Cikis
            try:
                from app.services.twitter_service import tweet_distribution_start
                from app.services.admin_telegram import notify_tweet_sent
                tw_ok = tweet_distribution_start(ipo)
                await notify_tweet_sent("dagitim_baslangic", ipo.ticker or ipo.company_name, tw_ok)
            except Exception:
                pass  # Tweet hatasi sistemi etkilemez

        # ==========================================
        # 2. in_distribution → awaiting_trading
        #    Kosul: subscription_end gectiyse
        # ==========================================
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "in_distribution",
                    IPO.archived == False,
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
            try:
                from app.services.admin_telegram import notify_ipo_status_change
                await notify_ipo_status_change(
                    ipo.ticker, ipo.company_name,
                    "in_distribution", "awaiting_trading",
                )
            except Exception:
                pass

        # ==========================================
        # 3. awaiting_trading → trading
        #    Kosul: trading_start set edilmis VE bugune esit veya gecmis
        # ==========================================
        result = await self.db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "awaiting_trading",
                    IPO.archived == False,
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
            try:
                from app.services.admin_telegram import notify_ipo_status_change
                await notify_ipo_status_change(
                    ipo.ticker, ipo.company_name,
                    "awaiting_trading", "trading",
                    f"Islem tarihi: {ipo.trading_start}" if ipo.trading_start else "",
                )
            except Exception:
                pass

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
