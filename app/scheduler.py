"""APScheduler — periyodik scraping gorevleri.

1. SPK Halka Arz (eski KAP): her 30 dakikada bir — SPK ihrac API
2. KAP Haberler: DEVRE DISI (KAP API bozuk, 404)
3. SPK Bulten Monitor: her 5 dk (20:00-05:00 arasi)
4. SPK Basvuru Listesi: gunluk 08:00 (SPKApplication tablosuna)
5. HalkArz + Gedik: her 2 saatte bir
6. Telegram Poller: her 10 saniyede bir
7. IPO Durum Guncelleme: her saat (5 bolumlu status gecisleri)
8. 25 Is Gunu Arsiv: her gece 00:00
9. Hatirlatma Zamani Kontrol: her 15 dakika
10. SPK Ihrac Verileri: her 2 saatte bir (islem tarihi tespiti)
11. InfoYatirim: her 6 saatte bir (yedek veri kaynagi)
12. Son Gun Uyarisi: her gun 09:00 ve 17:00
13. Tavan Takip Gun Sonu: her gun 18:20 (UTC 15:20) Pzt-Cuma
14. Sabah Scraper: her gun 09:00 (UTC 06:00) — tum scraper'lar + status update
15. Ilk Islem Gunu Bildirimi: her gun 09:30 (UTC 06:30) — trading_start == bugun
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
    """SPK ihrac verileri API'den halka arz bilgilerini ceker.

    Eski KAP API (memberDisclosureQuery) 404 donuyor.
    Yeni kaynak: https://ws.spk.gov.tr/BorclanmaAraclari/api/IlkHalkaArzVerileri
    fetch_all_years() ile mevcut yil + onceki yili ceker (yil gecisi korunmasi).
    """
    logger.info("SPK halka arz scraper calisiyor...")
    try:
        from app.scrapers.spk_ihrac_scraper import SPKIhracScraper
        from app.services.ipo_service import IPOService
        from app.services.notification import NotificationService

        scraper = SPKIhracScraper()
        try:
            # fetch_all_years() → mevcut yil + onceki yil (yil gecisi korunmasi)
            all_data = await scraper.fetch_all_years()

            if not all_data:
                logger.warning("SPK ihrac API: Veri gelmedi")
                return

            async with async_session() as db:
                ipo_service = IPOService(db)
                notif_service = NotificationService(db)

                for item in all_data:
                    ipo = await ipo_service.create_or_update_ipo({
                        "company_name": item.get("company_name", ""),
                        "ticker": item.get("ticker"),
                        "ipo_price": item.get("ipo_price"),
                        "trading_start": item.get("trading_start_date"),
                        "market_segment": item.get("market_segment"),
                        "lead_broker": item.get("lead_broker"),
                        "offering_size_tl": item.get("offering_size_tl"),
                        "status": "trading",
                    })

                    # Yeni eklenen IPO ise bildirim gonder
                    if ipo and ipo.created_at and (
                        datetime.utcnow() - ipo.created_at.replace(tzinfo=None)
                    ).total_seconds() < 60:
                        await notif_service.notify_new_ipo(ipo)

                await db.commit()

            logger.info(f"SPK halka arz: {len(all_data)} kayit islendi")
        finally:
            await scraper.close()

    except Exception as e:
        logger.error(f"SPK halka arz scraper hatasi: {e}")


async def scrape_kap_news():
    """KAP haber scraper — gecici olarak devre disi.

    KAP sitesi Next.js'e gecti, eski API (memberDisclosureQuery) 404 donuyor.
    Halka arz verileri artik SPK ihrac API'den geliyor (scrape_kap_ipo).
    KAP haberleri icin yeni bir kaynak bulunana kadar bu job bos calisir.
    """
    # KAP API bozuk — gereksiz 404 hatalari log'u kirletmesin
    return


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
        from sqlalchemy import select, and_, or_
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
                        User.notifications_enabled == True,
                        getattr(User, reminder_check) == True,
                        or_(
                            User.expo_push_token.isnot(None),
                            User.fcm_token.isnot(None),
                        ),
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


async def daily_ceiling_update():
    """Gun sonu tavan takip guncellemesi — 18:20 (UTC 15:20).

    Borsa 18:00'de kapanir, 18:20'de kapanis verileri kesinlesir.
    Yahoo Finance'den gunluk OHLC verilerini cekip ipo_ceiling_tracks
    tablosuna yazar.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO
        from app.services.ipo_service import IPOService
        from app.scrapers.yahoo_finance_scraper import YahooFinanceScraper, detect_ceiling_floor

        async with async_session() as db:
            # Isleme baslayan ve henuz arsivlenmemis IPO'lari bul
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status == "trading",
                        IPO.archived == False,
                        IPO.trading_start.isnot(None),
                    )
                )
            )
            active_ipos = result.scalars().all()

            if not active_ipos:
                logger.info("Tavan takip: Aktif islem goren IPO yok")
                return

            tickers = [ipo.ticker for ipo in active_ipos if ipo.ticker]
            logger.info(
                "Tavan takip gun sonu: %d aktif IPO — %s",
                len(active_ipos),
                ", ".join(tickers),
            )

            scraper = YahooFinanceScraper()
            ipo_service = IPOService(db)

            try:
                for ipo in active_ipos:
                    if not ipo.ticker or not ipo.trading_start:
                        continue

                    # Yahoo Finance'den OHLC verisi cek
                    days_data = await scraper.fetch_ohlc_since_trading_start(
                        ticker=ipo.ticker,
                        trading_start=ipo.trading_start,
                        max_days=25,
                    )

                    if not days_data:
                        logger.warning("Tavan takip: %s icin veri cekilemedi", ipo.ticker)
                        continue

                    # Her gun icin ceiling track kaydi olustur/guncelle
                    prev_close = ipo.ipo_price  # Ilk gun referansi

                    for day in days_data:
                        detection = detect_ceiling_floor(
                            close_price=day["close"],
                            prev_close=prev_close,
                            high_price=day.get("high"),
                            low_price=day.get("low"),
                        )

                        track = await ipo_service.update_ceiling_track(
                            ipo_id=ipo.id,
                            trading_day=day["trading_day"],
                            trade_date=day["date"],
                            close_price=day["close"],
                            hit_ceiling=detection["hit_ceiling"],
                            open_price=day.get("open"),
                            high_price=day.get("high"),
                            low_price=day.get("low"),
                            hit_floor=detection["hit_floor"],
                        )

                        if track:
                            track.durum = detection["durum"]
                            track.pct_change = detection["pct_change"]

                        prev_close = day["close"]

                    # trading_day_count guncelle
                    ipo.trading_day_count = len(days_data)

                    if days_data and not ipo.first_day_close_price:
                        ipo.first_day_close_price = days_data[0]["close"]

                    logger.info(
                        "Tavan takip: %s — %d gun guncellendi",
                        ipo.ticker, len(days_data),
                    )

            finally:
                await scraper.close()

            await db.commit()
            logger.info("Tavan takip gun sonu tamamlandi — %d IPO islendi", len(active_ipos))

    except Exception as e:
        logger.error("Tavan takip gun sonu hatasi: %s", e)


async def morning_scraper_run():
    """Sabah 09:00 (UTC 06:00) — tum scraper'lari calistir + status guncelle.

    Borsa acilmadan once verilerin guncel olmasini garanti eder.
    Sirayla calistirir: HalkArz+Gedik → SPK Ihrac → InfoYatirim → Status Update
    """
    logger.info("=== SABAH SCRAPER BASLADI (09:00) ===")
    try:
        await scrape_halkarz_gedik()
    except Exception as e:
        logger.error(f"Sabah scraper — HalkArz/Gedik hatasi: {e}")

    try:
        await check_spk_ihrac_data()
    except Exception as e:
        logger.error(f"Sabah scraper — SPK ihrac hatasi: {e}")

    try:
        await scrape_infoyatirim()
    except Exception as e:
        logger.error(f"Sabah scraper — InfoYatirim hatasi: {e}")

    try:
        await auto_update_ipo_statuses()
    except Exception as e:
        logger.error(f"Sabah scraper — Status update hatasi: {e}")

    logger.info("=== SABAH SCRAPER TAMAMLANDI ===")


async def send_first_trading_day_notifications():
    """Ilk islem gunu bildirimi — her gun 09:30 (UTC 06:30).

    trading_start == bugun olan IPO'lari bulur ve
    notify_first_trading_day = True olan kullanicilara bildirim gonderir.
    Her IPO icin tek 1 bildirim.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO
        from app.services.notification import NotificationService

        async with async_session() as db:
            today = date.today()

            # Bugun isleme baslayan IPO'lari bul
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.trading_start == today,
                        IPO.status.in_(["trading", "awaiting_trading"]),
                    )
                )
            )
            todays_ipos = list(result.scalars().all())

            if not todays_ipos:
                logger.info("Ilk islem gunu: Bugun baslayan IPO yok")
                return

            notif_service = NotificationService(db)

            total_sent = 0
            for ipo in todays_ipos:
                sent = await notif_service.notify_first_trading_day(ipo)
                total_sent += sent
                logger.info(
                    "Ilk islem gunu bildirimi: %s — %d kullaniciya gonderildi",
                    ipo.ticker or ipo.company_name, sent,
                )

            logger.info(
                "Ilk islem gunu bildirimi tamamlandi: %d IPO, %d bildirim",
                len(todays_ipos), total_sent,
            )

    except Exception as e:
        logger.error(f"Ilk islem gunu bildirim hatasi: {e}")


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

    # 5. HalkArz + Gedik — her 2 saatte bir (trading_start hizli tespiti icin)
    scheduler.add_job(
        scrape_halkarz_gedik,
        IntervalTrigger(hours=2),
        id="halkarz_gedik_scraper",
        name="HalkArz + Gedik Scraper",
        replace_existing=True,
    )

    # 6. Telegram Poller — her 10 saniyede bir
    # max_instances=1: APScheduler ayni anda sadece 1 instance calistirir
    # Ek olarak telegram_poller.py icinde asyncio.Lock koruması var
    scheduler.add_job(
        poll_telegram_job,
        IntervalTrigger(seconds=10),
        id="telegram_poller",
        name="Telegram Kanal Poller",
        replace_existing=True,
        max_instances=1,
        coalesce=True,  # Biriken cagrilari birlestir
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

    # 13. Tavan Takip Gun Sonu — her gun 18:20 (UTC 15:20) Pzt-Cuma
    scheduler.add_job(
        daily_ceiling_update,
        CronTrigger(hour=15, minute=20, day_of_week="mon-fri"),
        id="daily_ceiling_update",
        name="Tavan Takip Gun Sonu (18:20)",
        replace_existing=True,
    )

    # 14. Sabah Scraper — her gun 09:00 Turkiye (UTC 06:00) Pzt-Cuma
    # Borsa acilmadan once tum verileri guncellemek icin
    scheduler.add_job(
        morning_scraper_run,
        CronTrigger(hour=6, minute=0, day_of_week="mon-fri"),
        id="morning_scraper",
        name="Sabah Scraper (09:00 TR)",
        replace_existing=True,
    )

    # 15. Ilk Islem Gunu Bildirimi — her gun 09:30 Turkiye (UTC 06:30) Pzt-Cuma
    # trading_start == bugun olan IPO'lar icin tek 1 bildirim
    scheduler.add_job(
        send_first_trading_day_notifications,
        CronTrigger(hour=6, minute=30, day_of_week="mon-fri"),
        id="first_trading_day_notif",
        name="Ilk Islem Gunu Bildirimi (09:30 TR)",
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
