"""APScheduler — periyodik scraping gorevleri.

1. KAP Halka Arz: her 30 dakikada bir
2. KAP Haberler: her 30 saniyede bir
3. SPK Bulten Monitor: her 5 dk (20:00-05:00 arasi)
4. SPK Basvuru Listesi: gunluk 08:00 (SPKApplication tablosuna)
5. HalkArz + Gedik: her 4 saatte bir
6. Telegram Poller: her 10 saniyede bir
7. IPO Durum Guncelleme: her saat (5 bolumlu status gecisleri)
8. 25 Is Gunu Arsiv: her gece 00:00
9. Hatirlatma Zamani Kontrol: her 15 dakika
10. SPK Ihrac Verileri: her 2 saatte bir (islem tarihi tespiti)
11. InfoYatirim: her 6 saatte bir (yedek veri kaynagi)
12. Son Gun Uyarisi: her gun 09:00 ve 17:00
"""

import logging
from datetime import datetime, date, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from app.config import get_settings
from app.database import async_session

logger = logging.getLogger(__name__)

# Startup'ta ilk calismadan once DB'nin hazir olmasini bekle
_STARTUP_DELAY_SECONDS = 30

scheduler = AsyncIOScheduler()


async def scrape_kap_ipo():
    """KAP'tan halka arz bildirimlerini tarar."""
    logger.info("KAP halka arz scraper calisiyor...")
    try:
        from app.scrapers.kap_scraper import KAPScraper
        from app.services.ipo_service import IPOService
        from app.services.notification import NotificationService

        scraper = KAPScraper()
        try:
            disclosures = await scraper.fetch_ipo_disclosures()

            async with async_session() as db:
                ipo_service = IPOService(db)
                notif_service = NotificationService(db)

                for disclosure in disclosures:
                    ipo = await ipo_service.create_or_update_ipo({
                        "company_name": disclosure.get("company_name", ""),
                        "ticker": disclosure.get("ticker"),
                        "kap_notification_url": disclosure.get("url"),
                        "status": "newly_approved",
                    })

                    if ipo and ipo.created_at and (
                        datetime.utcnow() - ipo.created_at.replace(tzinfo=None)
                    ).total_seconds() < 60:
                        await notif_service.notify_new_ipo(ipo)

                await db.commit()

            logger.info(f"KAP halka arz: {len(disclosures)} bildirim islendi")
        finally:
            await scraper.close()

    except Exception as e:
        logger.error(f"KAP halka arz scraper hatasi: {e}")


async def scrape_kap_news():
    """KAP'tan son haberleri tarar ve keyword ile filtreler."""
    try:
        from app.scrapers.kap_scraper import KAPScraper
        from app.services.news_service import NewsFilterService
        from app.services.notification import NotificationService
        from app.models.news import KapNews

        scraper = KAPScraper()
        try:
            disclosures = await scraper.fetch_latest_disclosures(minutes=2)
            filter_service = NewsFilterService()

            async with async_session() as db:
                notif_service = NotificationService(db)

                for disclosure in disclosures:
                    matched = filter_service.filter_disclosure(disclosure)
                    if not matched:
                        continue

                    from sqlalchemy import select
                    existing = await db.execute(
                        select(KapNews).where(
                            KapNews.kap_notification_id == matched["kap_notification_id"]
                        )
                    )
                    if existing.scalar_one_or_none():
                        continue

                    news = KapNews(
                        ticker=matched["ticker"],
                        kap_notification_id=matched["kap_notification_id"],
                        news_title=matched.get("news_title"),
                        news_detail=matched.get("news_detail"),
                        matched_keyword=matched["matched_keyword"],
                        news_type=matched["news_type"],
                        sentiment=matched["sentiment"],
                        raw_text=matched.get("raw_text"),
                        kap_url=matched.get("kap_url"),
                    )
                    db.add(news)

                    await notif_service.notify_kap_news(
                        ticker=matched["ticker"],
                        price=None,
                        kap_id=matched["kap_notification_id"] or "",
                        matched_keyword=matched["matched_keyword"],
                        sentiment=matched["sentiment"],
                        news_type=matched["news_type"],
                    )

                await db.commit()

        finally:
            await scraper.close()

    except Exception as e:
        logger.error(f"KAP haber scraper hatasi: {e}")


async def scrape_spk():
    """SPK basvuru listesini TAM SENKRONiZE eder — SPKApplication tablosuna yazar.

    Bu scraper SPK'daki bekleyen halka arz basvurularini tarar ve
    SPKApplication tablosuna kaydeder (IPO tablosuna DEGIL).
    SPK bulteni ile onaylananlar ayri olarak spk_bulletin_scraper tarafindan
    IPO tablosuna 'newly_approved' olarak eklenir.

    Senkronizasyon mantigi:
    1. SPK sitesinden tum basvurulari cek
    2. DB'de olmayan yenileri ekle
    3. SPK'dan kalkmis olanlari 'approved' olarak isaretle (onay aldigi icin listeden cikmis)
    4. Mevcut kayitlarin tarihini guncelle
    """
    logger.info("SPK scraper calisiyor...")
    try:
        from app.scrapers.spk_scraper import SPKScraper
        from app.models.spk_application import SPKApplication
        from sqlalchemy import select

        scraper = SPKScraper()
        try:
            applications = await scraper.fetch_ipo_applications()
            if not applications:
                logger.warning("SPK: Hic basvuru bulunamadi, senkronizasyon atlaniyor")
                return

            # SPK'daki guncel sirket listesi
            spk_company_names = {
                app_data["company_name"] for app_data in applications
                if app_data.get("company_name")
            }

            async with async_session() as db:
                new_count = 0
                updated_count = 0
                removed_count = 0

                # 1. Yeni ekle + mevcut guncelle
                for app_data in applications:
                    company_name = app_data.get("company_name")
                    if not company_name:
                        continue

                    result = await db.execute(
                        select(SPKApplication).where(
                            SPKApplication.company_name == company_name
                        )
                    )
                    existing = result.scalar_one_or_none()

                    if existing:
                        # Mevcut kaydi guncelle (tarih degismis olabilir)
                        new_date = app_data.get("application_date")
                        if new_date and existing.application_date != new_date:
                            existing.application_date = new_date
                            updated_count += 1
                        # Daha once approved/rejected olmus ama tekrar listede ise pending'e cevir
                        if existing.status != "pending":
                            existing.status = "pending"
                            updated_count += 1
                    else:
                        db.add(SPKApplication(
                            company_name=company_name,
                            application_date=app_data.get("application_date"),
                            status="pending",
                        ))
                        new_count += 1

                # 2. SPK listesinden kalkmis olanlari isaretle
                # (onay alip listeden ciktiklari icin approved yapiyoruz)
                all_pending = await db.execute(
                    select(SPKApplication).where(
                        SPKApplication.status == "pending"
                    )
                )
                for app in all_pending.scalars().all():
                    if app.company_name not in spk_company_names:
                        app.status = "approved"
                        removed_count += 1

                await db.commit()

            logger.info(
                "SPK: %d basvuru tarandi — %d yeni, %d guncellendi, %d onaylandi (listeden cikti)",
                len(applications), new_count, updated_count, removed_count,
            )
        finally:
            await scraper.close()

    except Exception as e:
        logger.error(f"SPK scraper hatasi: {e}")


async def check_spk_bulletins_job():
    """SPK bulten monitor — yeni halka arz onayi tespiti (20:00-05:00)."""
    try:
        from app.scrapers.spk_bulletin_scraper import check_spk_bulletins
        await check_spk_bulletins()
    except Exception as e:
        logger.error(f"SPK bulten monitor hatasi: {e}")


async def scrape_halkarz_gedik():
    """HalkArz.com + Gedik Yatirim scraper — halka arz detay bilgileri.

    1. HalkArz.com (birincil): WP API ile detay bilgi (fiyat, tarih, sektor, izahname)
    2. Gedik Yatirim (3. alternatif): ticker, fiyat, tarih bilgisi
    """
    logger.info("HalkArz + Gedik scraper calisiyor...")
    try:
        from app.scrapers.halkarz_scraper import scrape_halkarz
        from app.scrapers.gedik_scraper import scrape_gedik

        await scrape_halkarz()
        await scrape_gedik()

    except Exception as e:
        logger.error(f"HalkArz/Gedik scraper hatasi: {e}")


async def poll_telegram_job():
    """Telegram kanalindan mesajlari ceker ve DB'ye yazar."""
    try:
        from app.scrapers.telegram_poller import poll_telegram
        await poll_telegram()
    except Exception as e:
        logger.error(f"Telegram poller hatasi: {e}")


async def auto_update_ipo_statuses():
    """Tarihlere gore IPO durumlarini otomatik gunceller."""
    try:
        from app.services.ipo_service import IPOService

        async with async_session() as db:
            ipo_service = IPOService(db)
            await ipo_service.auto_update_statuses()
            await db.commit()

    except Exception as e:
        logger.error(f"IPO durum guncelleme hatasi: {e}")


async def archive_old_ipos():
    """25 is gunu gecen halka arzlari arsivler.

    Her gece 00:00'da calisir.
    trading_start tarihi ~37 takvim gunu oncesinde olan
    ve archived=False olan IPO'lari arsivler.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO

        async with async_session() as db:
            # ~37 takvim gunu ~ 25 is gunu
            cutoff = date.today() - timedelta(days=37)

            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.archived == False,
                        IPO.trading_start.isnot(None),
                        IPO.trading_start <= cutoff,
                        IPO.status.in_(["trading", "completed"]),  # geriye uyumluluk
                    )
                )
            )

            archived_count = 0
            for ipo in result.scalars().all():
                ipo.archived = True
                ipo.archived_at = datetime.utcnow()
                archived_count += 1
                logger.info(f"IPO arsivlendi: {ipo.ticker or ipo.company_name}")

            if archived_count > 0:
                await db.commit()
                logger.info(f"Arsiv: {archived_count} IPO arsivlendi")

    except Exception as e:
        logger.error(f"IPO arsiv hatasi: {e}")


async def check_reminders():
    """Hatirlatma zamani kontrolu — son gun oncesi bildirim.

    Kullanicinin sectigi zamanlama ayarlarina gore bildirim gonderir:
    - 30 dk oncesi
    - 1 saat oncesi
    - 2 saat oncesi
    - 4 saat oncesi
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO
        from app.models.user import User
        from app.services.notification import NotificationService

        async with async_session() as db:
            today = date.today()

            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status.in_(["in_distribution", "active"]),  # yeni + geriye uyumluluk
                        IPO.subscription_end == today,
                    )
                )
            )
            last_day_ipos = list(result.scalars().all())

            if not last_day_ipos:
                return

            notif_service = NotificationService(db)

            now = datetime.now()
            from datetime import time as Time
            closing_time = datetime.combine(today, Time(17, 0))
            remaining_minutes = (closing_time - now).total_seconds() / 60

            if remaining_minutes < 0:
                return

            # Hangi hatirlatma zamanindayiz?
            reminder_check = None
            if 25 <= remaining_minutes <= 35:
                reminder_check = "reminder_30min"
            elif 55 <= remaining_minutes <= 65:
                reminder_check = "reminder_1h"
            elif 115 <= remaining_minutes <= 125:
                reminder_check = "reminder_2h"
            elif 235 <= remaining_minutes <= 245:
                reminder_check = "reminder_4h"

            if not reminder_check:
                return

            users_result = await db.execute(
                select(User).where(
                    and_(
                        getattr(User, reminder_check) == True,
                        User.fcm_token.isnot(None),
                    )
                )
            )
            users = list(users_result.scalars().all())

            if not users:
                return

            time_labels = {
                "reminder_30min": "30 dakika",
                "reminder_1h": "1 saat",
                "reminder_2h": "2 saat",
                "reminder_4h": "4 saat",
            }
            time_label = time_labels.get(reminder_check, "")

            for ipo in last_day_ipos:
                for user in users:
                    await notif_service.send_to_device(
                        token=user.fcm_token,
                        title=f"Son Gun Hatirlatma",
                        body=f"{ipo.ticker or ipo.company_name} icin basvuru son gun! Kapanisa {time_label} kaldi.",
                        data={
                            "type": "reminder",
                            "ipo_id": str(ipo.id),
                            "ticker": ipo.ticker or "",
                        },
                    )

            logger.info(f"Hatirlatma: {len(last_day_ipos)} IPO, {len(users)} kullanici")

    except Exception as e:
        logger.error(f"Hatirlatma kontrol hatasi: {e}")


async def check_spk_ihrac_data():
    """SPK ihrac verileri REST API'den islem tarihi ve detay bilgi tespiti.

    API: https://ws.spk.gov.tr/BorclanmaAraclari/api/IlkHalkaArzVerileri?yil={yil}

    1. awaiting_trading statusundaki IPO'larin islem tarihi aciklaninca
       otomatik olarak trading_start alanini set eder.
    2. Mevcut IPO'larin eksik detay bilgilerini (pazar, araci kurum, buyukluk) gunceller.

    auto_update_statuses bir sonraki calismasinda bu IPO'yu trading'e gecirir.
    """
    try:
        from app.scrapers.spk_ihrac_scraper import SPKIhracScraper
        from sqlalchemy import select, or_
        from app.models.ipo import IPO

        scraper = SPKIhracScraper()
        try:
            trading_data = await scraper.fetch_trading_dates()

            if not trading_data:
                return

            async with async_session() as db:
                # awaiting_trading + trading statusundaki IPO'lari al
                result = await db.execute(
                    select(IPO).where(
                        IPO.status.in_(["awaiting_trading", "trading", "in_distribution", "newly_approved"])
                    )
                )
                ipos = list(result.scalars().all())

                if not ipos:
                    return

                updated = 0
                for ipo in ipos:
                    for data in trading_data:
                        # Oncelik 1: Ticker ile eslesme (en guvenilir)
                        ticker_match = (
                            ipo.ticker and data.get("ticker") and
                            ipo.ticker.upper() == data["ticker"].upper()
                        )

                        # Oncelik 2: Sirket adi eslesme (fuzzy)
                        name_match = False
                        if not ticker_match and ipo.company_name and data.get("company_name"):
                            ipo_name = ipo.company_name.lower()
                            spk_name = data["company_name"].lower()
                            name_match = (
                                spk_name in ipo_name or
                                ipo_name in spk_name
                            )

                        if not (ticker_match or name_match):
                            continue

                        changed = False

                        # Islem tarihi guncelle
                        if data.get("trading_start_date") and not ipo.trading_start:
                            ipo.trading_start = data["trading_start_date"]
                            ipo.expected_trading_date = data["trading_start_date"]
                            changed = True
                            logger.info(
                                "SPK ihrac: %s islem tarihi tespit edildi: %s",
                                ipo.ticker or ipo.company_name,
                                data["trading_start_date"],
                            )

                        # Eksik detay bilgileri guncelle
                        if data.get("lead_broker") and not ipo.lead_broker:
                            ipo.lead_broker = data["lead_broker"]
                            changed = True
                        if data.get("market_segment") and not ipo.market_segment:
                            ipo.market_segment = data["market_segment"]
                            changed = True
                        if data.get("offering_size_tl") and not ipo.offering_size_tl:
                            ipo.offering_size_tl = data["offering_size_tl"]
                            changed = True
                        if data.get("public_float_pct") and not ipo.public_float_pct:
                            ipo.public_float_pct = data["public_float_pct"]
                            changed = True

                        if changed:
                            updated += 1

                        break  # Eslesen veriyi bulduk, sonrakine gec

                if updated > 0:
                    await db.commit()
                    logger.info("SPK ihrac: %d IPO guncellendi", updated)

        finally:
            await scraper.close()

    except Exception as e:
        logger.error(f"SPK ihrac verileri hatasi: {e}")


async def scrape_infoyatirim():
    """InfoYatirim.com — halka arz detay bilgileri (2. alternatif kaynak).

    25+ halka arz verisi cekilir:
    fiyat, tarih, lot, araci kurum, dagitim yontemi, islem tarihi.
    """
    try:
        from app.scrapers.infoyatirim_scraper import scrape_infoyatirim as _run
        await _run()
    except Exception as e:
        logger.error(f"InfoYatirim scraper hatasi: {e}")


async def send_last_day_warnings():
    """Yarin son gunu olan halka arzlar icin uyari gonder."""
    try:
        from app.services.ipo_service import IPOService
        from app.services.notification import NotificationService

        async with async_session() as db:
            ipo_service = IPOService(db)
            notif_service = NotificationService(db)

            last_day_ipos = await ipo_service.get_last_day_ipos()
            for ipo in last_day_ipos:
                await notif_service.notify_ipo_last_day(ipo)

            await db.commit()

        logger.info(f"Son gun uyarisi: {len(last_day_ipos)} halka arz")

    except Exception as e:
        logger.error(f"Son gun uyarisi hatasi: {e}")


def setup_scheduler():
    """Tum zamanlanmis gorevleri ayarlar."""
    try:
        _setup_scheduler_impl()
    except Exception as e:
        logger.error("Scheduler baslatilamadi: %s", e)


def _setup_scheduler_impl():
    """Scheduler icin tum job tanimlamalari."""
    settings = get_settings()

    # 1. KAP Halka Arz — her 30 dakika
    scheduler.add_job(
        scrape_kap_ipo,
        IntervalTrigger(seconds=settings.KAP_SCRAPE_INTERVAL_SECONDS),
        id="kap_ipo_scraper",
        name="KAP Halka Arz Scraper",
        replace_existing=True,
    )

    # 2. KAP Haber — her 30 saniye
    scheduler.add_job(
        scrape_kap_news,
        IntervalTrigger(seconds=settings.NEWS_SCRAPE_INTERVAL_SECONDS),
        id="kap_news_scraper",
        name="KAP Haber Scraper",
        replace_existing=True,
    )

    # 3. SPK Bulten Monitor — her 5 dk (20:00-05:00)
    scheduler.add_job(
        check_spk_bulletins_job,
        CronTrigger(minute="*/5", hour="20-23,0-4"),
        id="spk_bulletin_monitor",
        name="SPK Bulten Monitor",
        replace_existing=True,
    )

    # 4. SPK Onay Listesi — her 4 saatte bir + baslangicta kisa gecikme ile calistir
    scheduler.add_job(
        scrape_spk,
        IntervalTrigger(hours=4),
        id="spk_scraper",
        name="SPK Onay Scraper (4 saatte bir)",
        replace_existing=True,
        next_run_time=datetime.now() + timedelta(seconds=_STARTUP_DELAY_SECONDS),
    )

    # 5. HalkArz + Gedik — her 4 saatte bir
    scheduler.add_job(
        scrape_halkarz_gedik,
        IntervalTrigger(hours=4),
        id="halkarz_gedik_scraper",
        name="HalkArz + Gedik Scraper",
        replace_existing=True,
    )

    # 6. Telegram Poller — her 10 saniyede bir
    scheduler.add_job(
        poll_telegram_job,
        IntervalTrigger(seconds=10),
        id="telegram_poller",
        name="Telegram Kanal Poller",
        replace_existing=True,
    )

    # 7. IPO Durum Guncelleme — her saat
    scheduler.add_job(
        auto_update_ipo_statuses,
        IntervalTrigger(hours=1),
        id="ipo_status_updater",
        name="IPO Durum Guncelleyici",
        replace_existing=True,
    )

    # 8. 25 Is Gunu Arsiv — her gece 00:00
    scheduler.add_job(
        archive_old_ipos,
        CronTrigger(hour=0, minute=0),
        id="ipo_archiver",
        name="IPO Arsivleyici (25 Is Gunu)",
        replace_existing=True,
    )

    # 9. Hatirlatma Zamani Kontrol — her 15 dakika
    scheduler.add_job(
        check_reminders,
        IntervalTrigger(minutes=15),
        id="reminder_checker",
        name="Hatirlatma Kontrol (30dk/1h/2h/4h)",
        replace_existing=True,
    )

    # 10. SPK Ihrac Verileri — her 2 saatte bir (islem tarihi tespiti)
    scheduler.add_job(
        check_spk_ihrac_data,
        IntervalTrigger(hours=2),
        id="spk_ihrac_checker",
        name="SPK Ihrac Verileri (Islem Tarihi)",
        replace_existing=True,
    )

    # 11. InfoYatirim — her 6 saatte bir (yedek veri kaynagi)
    scheduler.add_job(
        scrape_infoyatirim,
        IntervalTrigger(hours=6),
        id="infoyatirim_scraper",
        name="InfoYatirim Halka Arz Detay",
        replace_existing=True,
    )

    # 12. Son gun uyarisi — her gun 09:00 ve 17:00
    scheduler.add_job(
        send_last_day_warnings,
        CronTrigger(hour=9, minute=0),
        id="last_day_warning_morning",
        name="Son Gun Uyarisi (Sabah)",
        replace_existing=True,
    )
    scheduler.add_job(
        send_last_day_warnings,
        CronTrigger(hour=17, minute=0),
        id="last_day_warning_evening",
        name="Son Gun Uyarisi (Aksam)",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        "Scheduler baslatildi — %d gorev ayarlandi",
        len(scheduler.get_jobs()),
    )


def shutdown_scheduler():
    """Scheduler'i durdurur."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler durduruldu")
