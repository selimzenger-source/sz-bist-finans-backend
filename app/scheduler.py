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
13b. Tavan Takip Retry: 18:30, 19:00, 20:00 ... 24:00 (basarisiz olursa)
14. Sabah Scraper: her gun 09:00 (UTC 06:00) — tum scraper'lar + status update
15. Ilk Islem Gunu Bildirimi: her gun 09:30 (UTC 06:30) — trading_start == bugun

Admin Telegram bildirimleri: Tum kritik hatalar ve durum gecisleri admin'e bildirilir.
"""

import asyncio
import logging
import random
from datetime import datetime, date, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from app.config import get_settings
from app.database import async_session

logger = logging.getLogger(__name__)

# Startup'ta ilk calismadan once DB'nin hazir olmasini bekle
_STARTUP_DELAY_SECONDS = 30

# Ceiling update retry — basarisiz olursa saatte bir tekrar dene (24:00'a kadar)
_ceiling_retry_pending = False

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
                    # allow_create=False: SPK ihrac API sadece mevcut IPO'lari gunceller
                    # Yeni IPO olusturma SADECE SPK bulten veya admin panelden yapilir
                    ipo = await ipo_service.create_or_update_ipo({
                        "company_name": item.get("company_name", ""),
                        "ticker": item.get("ticker"),
                        "ipo_price": item.get("ipo_price"),
                        "trading_start": item.get("trading_start_date"),
                        "market_segment": item.get("market_segment"),
                        "lead_broker": item.get("lead_broker"),
                        "offering_size_tl": item.get("offering_size_tl"),
                    })

                    if not ipo:
                        continue  # DB'de eslesen IPO bulunamadi, atla

                await db.commit()

            logger.info(f"SPK halka arz: {len(all_data)} kayit islendi")
        finally:
            await scraper.close()

    except Exception as e:
        logger.error(f"SPK halka arz scraper hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("SPK Halka Arz Scraper", str(e))
        except Exception:
            pass


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
                    existing = result.scalars().first()

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
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("SPK Basvuru Listesi", str(e))
        except Exception:
            pass


async def check_spk_bulletins_job():
    """SPK bulten monitor — yeni halka arz onayi tespiti (20:00-05:00)."""
    try:
        from app.scrapers.spk_bulletin_scraper import check_spk_bulletins
        await check_spk_bulletins()
    except Exception as e:
        logger.error(f"SPK bulten monitor hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("SPK Bülten Monitor (Scheduler)", str(e))
        except Exception:
            pass


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
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("HalkArz + Gedik Scraper", str(e))
        except Exception:
            pass


async def poll_telegram_job():
    """Telegram kanalindan mesajlari ceker ve DB'ye yazar."""
    try:
        from app.scrapers.telegram_poller import poll_telegram
        await poll_telegram()
    except Exception as e:
        logger.error(f"Telegram poller hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("Telegram Poller (Scheduler)", str(e))
        except Exception:
            pass


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
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("IPO Durum Güncelleme", str(e))
        except Exception:
            pass


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
                        # trading_start 37+ gun gecmis olan TUM statuslardaki IPO'lar arsivlenmeli
                        # (eski: sadece trading/completed — DMLKT gibi newly_approved'da takilanlar kaciriliyordu)
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
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("IPO Arşiv", str(e))
        except Exception:
            pass


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

            _r_tweet_idx = 0
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

                # Tweet at — Son 4 Saat veya Son 30 Dakika (her IPO icin bir kez)
                # Jitter — ilk IPO haric 50-55 sn bekle
                try:
                    from app.services.twitter_service import tweet_last_4_hours, tweet_last_30_min
                    if _r_tweet_idx > 0:
                        jitter = random.uniform(50, 55)
                        logger.info("Hatirlatma tweet jitter: %.1f sn bekleniyor (%s)", jitter, ipo.ticker or ipo.company_name)
                        await asyncio.sleep(jitter)
                    if reminder_check == "reminder_4h":
                        tweet_last_4_hours(ipo)
                        _r_tweet_idx += 1
                    elif reminder_check == "reminder_30min":
                        tweet_last_30_min(ipo)
                        _r_tweet_idx += 1
                except Exception:
                    pass  # Tweet hatasi sistemi etkilemez

            logger.info(f"Hatirlatma: {len(last_day_ipos)} IPO, {len(users)} kullanici")

    except Exception as e:
        logger.error(f"Hatirlatma kontrol hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("Hatırlatma Kontrolü", str(e))
        except Exception:
            pass


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
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("SPK İhraç Verileri", str(e))
        except Exception:
            pass


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
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("InfoYatirim Scraper", str(e))
        except Exception:
            pass


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
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("Son Gün Uyarısı", str(e))
        except Exception:
            pass


async def daily_ceiling_update():
    """Gun sonu tavan takip guncellemesi — 18:20 (UTC 15:20).

    Borsa 18:00'de kapanir, 18:20'de kapanis verileri kesinlesir.
    Yahoo Finance'den gunluk OHLC verilerini cekip ipo_ceiling_tracks
    tablosuna yazar. Basarisiz olursa retry mekanizmasi devreye girer.
    """
    global _ceiling_retry_pending
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
                _ceiling_retry_pending = False
                return

            tickers = [ipo.ticker for ipo in active_ipos if ipo.ticker]
            logger.info(
                "Tavan takip gun sonu: %d aktif IPO — %s",
                len(active_ipos),
                ", ".join(tickers),
            )

            scraper = YahooFinanceScraper()
            ipo_service = IPOService(db)

            success_count = 0
            fail_count = 0
            failed_tickers = []

            try:
                for ipo in active_ipos:
                    if not ipo.ticker or not ipo.trading_start:
                        continue

                    try:
                        # Yahoo Finance'den OHLC verisi cek
                        days_data = await scraper.fetch_ohlc_since_trading_start(
                            ticker=ipo.ticker,
                            trading_start=ipo.trading_start,
                            max_days=25,
                        )

                        if not days_data:
                            logger.warning("Tavan takip: %s icin veri cekilemedi", ipo.ticker)
                            fail_count += 1
                            failed_tickers.append(ipo.ticker)
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

                        success_count += 1
                        logger.info(
                            "Tavan takip: %s — %d gun guncellendi",
                            ipo.ticker, len(days_data),
                        )

                        # Tweet at — Gunluk Takip (tweet #8) ve 25 Gun Performans (tweet #9)
                        # Jitter: birden fazla IPO varsa tweetler arasi 50-55 sn bekle
                        try:
                            from app.services.twitter_service import (
                                tweet_daily_tracking, tweet_25_day_performance,
                            )
                            if days_data:
                                last_day = days_data[-1]
                                current_day = len(days_data)
                                last_close = float(last_day["close"])

                                # Gunluk % degisim hesapla
                                if len(days_data) > 1:
                                    prev_c = float(days_data[-2]["close"])
                                else:
                                    prev_c = float(ipo.ipo_price) if ipo.ipo_price else 0
                                daily_pct = (
                                    ((last_close - prev_c) / prev_c) * 100
                                    if prev_c > 0 else 0
                                )

                                last_det = detect_ceiling_floor(
                                    close_price=last_day["close"],
                                    prev_close=prev_c,
                                    high_price=last_day.get("high"),
                                    low_price=last_day.get("low"),
                                )

                                # Jitter — ilk IPO haric tweetler arasi 50-55 sn bekle
                                if success_count > 1:
                                    jitter = random.uniform(50, 55)
                                    logger.info("Tweet jitter: %.1f sn bekleniyor (%s)", jitter, ipo.ticker)
                                    await asyncio.sleep(jitter)

                                # Tweet #8: Gunluk takip (her gun)
                                tweet_daily_tracking(
                                    ipo, current_day, last_close,
                                    daily_pct, last_det["durum"],
                                )

                                # Tweet #9: 25 gun performans (sadece 25. gunde bir kez)
                                if current_day >= 25:
                                    # 25 gun tweeti icin de jitter bekle
                                    jitter9 = random.uniform(50, 55)
                                    logger.info("Tweet #9 jitter: %.1f sn bekleniyor (%s)", jitter9, ipo.ticker)
                                    await asyncio.sleep(jitter9)

                                    ipo_price_f = float(ipo.ipo_price) if ipo.ipo_price else 0
                                    total_pct = (
                                        ((last_close - ipo_price_f) / ipo_price_f) * 100
                                        if ipo_price_f > 0 else 0
                                    )
                                    # Tavan/taban gun sayisi — days_data'dan hesapla
                                    ceiling_d = 0
                                    floor_d = 0
                                    prev_close_calc = ipo_price_f
                                    for d in days_data:
                                        det = detect_ceiling_floor(
                                            d["close"], prev_close_calc,
                                            d.get("high"), d.get("low"),
                                        )
                                        if det["hit_ceiling"]:
                                            ceiling_d += 1
                                        if det["hit_floor"]:
                                            floor_d += 1
                                        prev_close_calc = float(d["close"])

                                    avg_lot = (
                                        float(ipo.estimated_lots_per_person)
                                        if ipo.estimated_lots_per_person else None
                                    )
                                    tweet_25_day_performance(
                                        ipo, last_close, total_pct,
                                        ceiling_d, floor_d, avg_lot,
                                    )
                        except Exception as tweet_err:
                            logger.error("Tweet hatasi (sistemi etkilemez): %s", tweet_err)
                    except Exception as ticker_err:
                        logger.error("Tavan takip %s hatasi: %s", ipo.ticker, ticker_err)
                        fail_count += 1
                        failed_tickers.append(ipo.ticker)

            finally:
                await scraper.close()

            await db.commit()
            logger.info("Tavan takip gun sonu tamamlandi — %d IPO islendi", len(active_ipos))

            # Sonucu admin'e bildir
            try:
                from app.services.admin_telegram import notify_ceiling_update_result
                await notify_ceiling_update_result(
                    total=len(active_ipos),
                    success=success_count,
                    failed=fail_count,
                    failed_tickers=failed_tickers if failed_tickers else None,
                )
            except Exception:
                pass

            # Retry gerekli mi?
            if fail_count > 0:
                _ceiling_retry_pending = True
            else:
                _ceiling_retry_pending = False

    except Exception as e:
        logger.error("Tavan takip gun sonu hatasi: %s", e)
        _ceiling_retry_pending = True
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("Tavan Takip Gün Sonu", str(e))
        except Exception:
            pass


async def ceiling_update_retry():
    """Tavan takip retry — basarisiz olursa saatte bir tekrar dene.

    18:30, 19:00, 20:00, 21:00, 22:00, 23:00, 24:00 saatlerinde calisir.
    _ceiling_retry_pending True ise daily_ceiling_update'i tekrar calistirir.
    """
    global _ceiling_retry_pending
    if not _ceiling_retry_pending:
        return

    logger.info("Tavan takip RETRY calisiyor...")
    try:
        from app.services.admin_telegram import send_admin_message
        await send_admin_message(
            "🔄 <b>Tavan Takip Retry</b>\nÖnceki güncelleme başarısız — tekrar deneniyor...",
            silent=True,
        )
    except Exception:
        pass

    await daily_ceiling_update()

    if not _ceiling_retry_pending:
        try:
            from app.services.admin_telegram import send_admin_message
            await send_admin_message(
                "✅ <b>Tavan Takip Retry Başarılı</b>\nGüncelleme tamamlandı.",
            )
        except Exception:
            pass


async def morning_scraper_run():
    """Sabah 09:00 (UTC 06:00) — tum scraper'lari calistir + status guncelle.

    Borsa acilmadan once verilerin guncel olmasini garanti eder.
    Sirayla calistirir: HalkArz+Gedik → SPK Ihrac → InfoYatirim → Status Update
    """
    logger.info("=== SABAH SCRAPER BASLADI (09:00) ===")
    errors = []

    try:
        await scrape_halkarz_gedik()
    except Exception as e:
        logger.error(f"Sabah scraper — HalkArz/Gedik hatasi: {e}")
        errors.append(f"HalkArz/Gedik: {e}")

    try:
        await check_spk_ihrac_data()
    except Exception as e:
        logger.error(f"Sabah scraper — SPK ihrac hatasi: {e}")
        errors.append(f"SPK İhraç: {e}")

    try:
        await scrape_infoyatirim()
    except Exception as e:
        logger.error(f"Sabah scraper — InfoYatirim hatasi: {e}")
        errors.append(f"InfoYatirim: {e}")

    try:
        await auto_update_ipo_statuses()
    except Exception as e:
        logger.error(f"Sabah scraper — Status update hatasi: {e}")
        errors.append(f"Status Update: {e}")

    # Sabah scraper sonucu admin'e bildir
    if errors:
        try:
            from app.services.admin_telegram import send_admin_message
            error_text = "\n".join(f"• {e}" for e in errors)
            await send_admin_message(
                f"⚠️ <b>Sabah Scraper (09:00)</b>\n"
                f"{len(errors)} hata oluştu:\n{error_text}"
            )
        except Exception:
            pass

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
            _ft_tweet_idx = 0
            for ipo in todays_ipos:
                sent = await notif_service.notify_first_trading_day(ipo)
                total_sent += sent
                logger.info(
                    "Ilk islem gunu bildirimi: %s — %d kullaniciya gonderildi",
                    ipo.ticker or ipo.company_name, sent,
                )

                # Tweet at — Ilk Islem Gunu Gong (tweet #6)
                # Jitter — ilk IPO haric 50-55 sn bekle
                try:
                    from app.services.twitter_service import tweet_first_trading_day
                    if _ft_tweet_idx > 0:
                        jitter = random.uniform(50, 55)
                        logger.info("Ilk islem tweet jitter: %.1f sn bekleniyor (%s)", jitter, ipo.ticker or ipo.company_name)
                        await asyncio.sleep(jitter)
                    tweet_first_trading_day(ipo)
                    _ft_tweet_idx += 1
                except Exception:
                    pass  # Tweet hatasi sistemi etkilemez

            logger.info(
                "Ilk islem gunu bildirimi tamamlandi: %d IPO, %d bildirim",
                len(todays_ipos), total_sent,
            )

    except Exception as e:
        logger.error(f"Ilk islem gunu bildirim hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("İlk İşlem Günü Bildirimi", str(e))
        except Exception:
            pass


async def tweet_opening_price_job():
    """Ilk islem gunu acilis fiyati tweeti — 09:56 (UTC 06:56).

    Sadece bugun trading_start olan IPO'lar icin calisir.
    Yahoo Finance'den acilis fiyatini cekip tweet atar.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO
        from app.scrapers.yahoo_finance_scraper import YahooFinanceScraper

        async with async_session() as db:
            today = date.today()
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.trading_start == today,
                        IPO.ticker.isnot(None),
                    )
                )
            )
            todays_ipos = list(result.scalars().all())

            if not todays_ipos:
                return

            scraper = YahooFinanceScraper()
            try:
                _tweet_idx = 0
                for ipo in todays_ipos:
                    try:
                        days_data = await scraper.fetch_ohlc_since_trading_start(
                            ticker=ipo.ticker,
                            trading_start=ipo.trading_start,
                            max_days=1,
                        )
                        if days_data and days_data[0].get("open"):
                            open_price = float(days_data[0]["open"])
                            ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
                            pct_change = (
                                ((open_price - ipo_price) / ipo_price) * 100
                                if ipo_price > 0 else 0
                            )
                            # Jitter — ilk tweet haric 50-55 sn bekle
                            if _tweet_idx > 0:
                                jitter = random.uniform(50, 55)
                                logger.info("Acilis tweet jitter: %.1f sn bekleniyor (%s)", jitter, ipo.ticker)
                                await asyncio.sleep(jitter)
                            from app.services.twitter_service import tweet_opening_price
                            tweet_opening_price(ipo, open_price, pct_change)
                            _tweet_idx += 1
                    except Exception as e:
                        logger.error("Acilis fiyati tweet hatasi %s: %s", ipo.ticker, e)
            finally:
                await scraper.close()

    except Exception as e:
        logger.error("Acilis fiyati tweet job hatasi: %s", e)


async def monthly_yearly_summary_tweet():
    """Aylik yillik halka arz ozeti tweeti — her ayin 1'i 20:00 (UTC 17:00).

    Ocak haric (yil basinda veri yok).
    O yilin tum 25 gunu tamamlanan halka arzlarinin performans ozetini tweet atar.
    """
    try:
        now = datetime.now()
        # Ocak ayinda tweet atma (yil basinda veri yok)
        if now.month == 1:
            logger.info("Yillik ozet: Ocak ayi — tweet atilmadi")
            return

        from sqlalchemy import select, and_, func
        from app.models.ipo import IPO, IPOCeilingTrack

        current_year = now.year

        async with async_session() as db:
            # Bu yil isleme baslayan ve 25 gunu tamamlayan IPO'lar
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.trading_start.isnot(None),
                        IPO.trading_start >= date(current_year, 1, 1),
                        IPO.trading_day_count >= 25,
                        IPO.ipo_price.isnot(None),
                        IPO.ipo_price > 0,
                    )
                )
            )
            completed_ipos = list(result.scalars().all())

            # Bu yil toplam halka arz sayisi (tum statuslar)
            total_result = await db.execute(
                select(func.count(IPO.id)).where(
                    and_(
                        IPO.trading_start.isnot(None),
                        IPO.trading_start >= date(current_year, 1, 1),
                    )
                )
            )
            total_ipos = total_result.scalar() or 0

            if not completed_ipos:
                logger.info("Yillik ozet: 25 gunu tamamlayan IPO yok")
                return

            # Her IPO icin 25. gun kapanis fiyati ve getiri hesapla
            returns = []
            for ipo in completed_ipos:
                # 25. gun ceiling track kaydini bul
                track_result = await db.execute(
                    select(IPOCeilingTrack).where(
                        and_(
                            IPOCeilingTrack.ipo_id == ipo.id,
                            IPOCeilingTrack.trading_day == 25,
                        )
                    )
                )
                track_25 = track_result.scalar_one_or_none()

                if track_25 and track_25.close_price:
                    ipo_price = float(ipo.ipo_price)
                    close_25 = float(track_25.close_price)
                    pct = ((close_25 - ipo_price) / ipo_price) * 100
                    returns.append({
                        "ticker": ipo.ticker or ipo.company_name,
                        "pct": pct,
                    })

            if not returns:
                return

            avg_return = sum(r["pct"] for r in returns) / len(returns)
            best = max(returns, key=lambda r: r["pct"])
            worst = min(returns, key=lambda r: r["pct"])
            positive_count = sum(1 for r in returns if r["pct"] > 0)

            from app.services.twitter_service import tweet_yearly_summary
            tweet_yearly_summary(
                year=current_year,
                total_ipos=total_ipos,
                avg_return_pct=avg_return,
                best_ticker=best["ticker"],
                best_return_pct=best["pct"],
                worst_ticker=worst["ticker"],
                worst_return_pct=worst["pct"],
                total_completed=len(returns),
                positive_count=positive_count,
            )

    except Exception as e:
        logger.error("Yillik ozet tweet hatasi: %s", e)


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

    # 13b. Tavan Takip Retry — basarisiz olursa saatte bir tekrar dene
    # 18:30 (UTC 15:30), 19:00 (UTC 16:00), 20:00 (UTC 17:00), 21:00 (UTC 18:00),
    # 22:00 (UTC 19:00), 23:00 (UTC 20:00), 24:00 (UTC 21:00)
    retry_utc_hours = [
        (15, 30),  # 18:30 TR
        (16, 0),   # 19:00 TR
        (17, 0),   # 20:00 TR
        (18, 0),   # 21:00 TR
        (19, 0),   # 22:00 TR
        (20, 0),   # 23:00 TR
        (21, 0),   # 24:00 TR
    ]
    for idx, (h, m) in enumerate(retry_utc_hours):
        scheduler.add_job(
            ceiling_update_retry,
            CronTrigger(hour=h, minute=m, day_of_week="mon-fri"),
            id=f"ceiling_retry_{idx}",
            name=f"Tavan Takip Retry ({h+3:02d}:{m:02d} TR)",
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

    # 16. Acilis Fiyati Tweet — her gun 09:56 Turkiye (UTC 06:56) Pzt-Cuma
    # Sadece ilk islem gunu olan IPO'lar icin acilis fiyati tweeti
    scheduler.add_job(
        tweet_opening_price_job,
        CronTrigger(hour=6, minute=56, day_of_week="mon-fri"),
        id="opening_price_tweet",
        name="Acilis Fiyati Tweet (09:56 TR)",
        replace_existing=True,
    )

    # 17. Aylik Yillik Ozet Tweet — her ayin 1'i 20:00 Turkiye (UTC 17:00)
    # Ocak haric — yil basinda veri yok
    scheduler.add_job(
        monthly_yearly_summary_tweet,
        CronTrigger(day=1, hour=17, minute=0),
        id="monthly_yearly_summary_tweet",
        name="Yillik Halka Arz Ozeti Tweet (Ayin 1'i 20:00 TR)",
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
