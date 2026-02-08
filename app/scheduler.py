"""APScheduler — periyodik scraping gorevleri.

1. KAP Halka Arz: her 30 dakikada bir
2. KAP Haberler: her 30 saniyede bir
3. SPK Kontrol: gunluk
4. IPO Durum Guncelleme: her saat
5. Son Gun Uyarisi: her gun 09:00 ve 17:00
"""

import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from app.config import get_settings
from app.database import async_session

logger = logging.getLogger(__name__)

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
                    # IPO olustur veya guncelle
                    ipo = await ipo_service.create_or_update_ipo({
                        "company_name": disclosure.get("company_name", ""),
                        "ticker": disclosure.get("ticker"),
                        "kap_notification_url": disclosure.get("url"),
                        "status": "upcoming",
                    })

                    # Yeni IPO ise bildirim gonder
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
                    # Keyword filtrele
                    matched = filter_service.filter_disclosure(disclosure)
                    if not matched:
                        continue

                    # Daha once kaydedilmis mi kontrol et
                    from sqlalchemy import select
                    existing = await db.execute(
                        select(KapNews).where(
                            KapNews.kap_notification_id == matched["kap_notification_id"]
                        )
                    )
                    if existing.scalar_one_or_none():
                        continue

                    # Yeni haber kaydet
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

                    # Push bildirim gonder
                    await notif_service.notify_kap_news(
                        ticker=matched["ticker"],
                        price=None,  # Fiyat bilgisi ayri kaynaktan gelecek
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
    """SPK onay listesini kontrol eder."""
    logger.info("SPK scraper calisiyor...")
    try:
        from app.scrapers.spk_scraper import SPKScraper
        from app.services.ipo_service import IPOService

        scraper = SPKScraper()
        try:
            approvals = await scraper.fetch_approved_ipos()

            async with async_session() as db:
                ipo_service = IPOService(db)
                for approval in approvals:
                    if approval.get("company_name"):
                        await ipo_service.create_or_update_ipo({
                            "company_name": approval["company_name"],
                            "spk_bulletin_url": approval.get("url"),
                            "status": "upcoming",
                        })
                await db.commit()

            logger.info(f"SPK: {len(approvals)} onay islendi")
        finally:
            await scraper.close()

    except Exception as e:
        logger.error(f"SPK scraper hatasi: {e}")


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

    # 3. SPK — gunluk sabah 08:00
    scheduler.add_job(
        scrape_spk,
        CronTrigger(hour=8, minute=0),
        id="spk_scraper",
        name="SPK Onay Scraper",
        replace_existing=True,
    )

    # 4. IPO Durum Guncelleme — her saat
    scheduler.add_job(
        auto_update_ipo_statuses,
        IntervalTrigger(hours=1),
        id="ipo_status_updater",
        name="IPO Durum Guncelleyici",
        replace_existing=True,
    )

    # 5. Son gun uyarisi — her gun 09:00 ve 17:00
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
    logger.info("Scheduler baslatildi — tum gorevler ayarlandi")


def shutdown_scheduler():
    """Scheduler'i durdurur."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler durduruldu")
