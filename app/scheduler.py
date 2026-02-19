"""APScheduler — periyodik scraping gorevleri.

1. SPK Halka Arz (eski KAP): her 30 dakikada bir — SPK ihrac API
2. KAP Haberler: DEVRE DISI (KAP API bozuk, 404)
3. SPK Bulten Monitor: 21:00-03:00 TR her 1 dk, 03:00-08:00 TR her 5 dk
4. SPK Basvuru Listesi: gunluk 08:00 (SPKApplication tablosuna)
5. HalkArz + Gedik: her 2 saatte bir
6. Telegram Poller: her 10 saniyede bir
7. IPO Durum Guncelleme: her saat (5 bolumlu status gecisleri)
8. 25 Is Gunu Arsiv + Tweet: her gun 12:00 TR (UTC 09:00)
9. Hatirlatma Zamani Kontrol: her 15 dakika
10. SPK Ihrac Verileri: her 2 saatte bir (islem tarihi tespiti)
11. InfoYatirim: her 6 saatte bir (yedek veri kaynagi)
12. Son Gun Uyarisi: her gun 09:00 TR (UTC 06:00) — bugun son gun bildirim
13. Tavan Takip Gun Sonu: her gun 18:20 (UTC 15:20) Pzt-Cuma
13b. Tavan Takip Retry: 18:30, 19:00, 20:00 ... 24:00 (basarisiz olursa)
14. Sabah Scraper: her gun 09:00 (UTC 06:00) — tum scraper'lar + status update
15. Ilk Islem Gunu Bildirimi: her gun 09:30 (UTC 06:30) — trading_start == bugun

Admin Telegram bildirimleri: Tum kritik hatalar ve durum gecisleri admin'e bildirilir.
"""

import asyncio
import logging
import random
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo

# Turkiye saat dilimi — tum tarih islemlerinde bu kullanilmali
_TR_TZ = ZoneInfo("Europe/Istanbul")


def _today_tr() -> date:
    """Turkiye saatine gore bugunun tarihini dondurur (UTC yerine)."""
    return datetime.now(_TR_TZ).date()

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

# --- Scraper Boost Modu ---
# Yeni IPO tespit edilince halkarz+gedik scraper'i 12 saat boyunca 15dk'da 1 calistirir
BOOST_INTERVAL_MINUTES = 15
BOOST_DURATION_HOURS = 12
NORMAL_INTERVAL_HOURS = 2
_boost_active = False


async def activate_scraper_boost():
    """SPK bulteninden yeni IPO tespit edilince scraper sikligini artir.

    halkarz_gedik_scraper: 2 saat → 15 dakika (12 saat boyunca)
    infoyatirim_scraper: 6 saat → 30 dakika (12 saat boyunca)
    """
    global _boost_active
    if _boost_active:
        logger.info("Scraper boost zaten aktif, atlaniyor")
        return

    try:
        # ScraperState'e boost bitis zamanini kaydet
        from app.database import async_session
        from app.models.scraper_state import ScraperState
        from sqlalchemy import select

        boost_until = datetime.utcnow() + timedelta(hours=BOOST_DURATION_HOURS)

        async with async_session() as db:
            result = await db.execute(
                select(ScraperState).where(ScraperState.key == "scraper_boost_until")
            )
            state = result.scalar_one_or_none()
            if state:
                state.value = boost_until.isoformat()
            else:
                db.add(ScraperState(key="scraper_boost_until", value=boost_until.isoformat()))
            await db.commit()

        # Job'lari hizlandir
        scheduler.reschedule_job(
            "halkarz_gedik_scraper",
            trigger=IntervalTrigger(minutes=BOOST_INTERVAL_MINUTES),
        )
        scheduler.reschedule_job(
            "infoyatirim_scraper",
            trigger=IntervalTrigger(minutes=30),
        )
        _boost_active = True
        logger.warning(
            "🚀 Scraper boost AKTIF: halkarz=%ddk, infoyatirim=30dk — %s'e kadar",
            BOOST_INTERVAL_MINUTES,
            boost_until.strftime("%H:%M UTC"),
        )
    except Exception as e:
        logger.error("Scraper boost aktivasyon hatasi: %s", e)


async def _check_scraper_boost_expiry():
    """Boost suresi dolduysa normal frekanslara don."""
    global _boost_active
    if not _boost_active:
        return

    try:
        from app.database import async_session
        from app.models.scraper_state import ScraperState
        from sqlalchemy import select

        async with async_session() as db:
            result = await db.execute(
                select(ScraperState).where(ScraperState.key == "scraper_boost_until")
            )
            state = result.scalar_one_or_none()
            if not state or not state.value:
                _boost_active = False
                return

            boost_until = datetime.fromisoformat(state.value)
            if datetime.utcnow() >= boost_until:
                # Boost suresi doldu — normal frekanslara don
                scheduler.reschedule_job(
                    "halkarz_gedik_scraper",
                    trigger=IntervalTrigger(hours=NORMAL_INTERVAL_HOURS),
                )
                scheduler.reschedule_job(
                    "infoyatirim_scraper",
                    trigger=IntervalTrigger(hours=6),
                )
                state.value = None
                await db.commit()
                _boost_active = False
                logger.warning("⏱️ Scraper boost BITTI — normal frekanslara donuldu")
    except Exception as e:
        logger.error("Boost expiry kontrol hatasi: %s", e)


async def _restore_boost_on_startup():
    """Uygulama yeniden basladiginda onceki boost suresi hala aktifse devam et."""
    global _boost_active
    try:
        await asyncio.sleep(10)  # DB hazir olsun
        from app.database import async_session
        from app.models.scraper_state import ScraperState
        from sqlalchemy import select

        async with async_session() as db:
            result = await db.execute(
                select(ScraperState).where(ScraperState.key == "scraper_boost_until")
            )
            state = result.scalar_one_or_none()
            if state and state.value:
                boost_until = datetime.fromisoformat(state.value)
                if datetime.utcnow() < boost_until:
                    scheduler.reschedule_job(
                        "halkarz_gedik_scraper",
                        trigger=IntervalTrigger(minutes=BOOST_INTERVAL_MINUTES),
                    )
                    scheduler.reschedule_job(
                        "infoyatirim_scraper",
                        trigger=IntervalTrigger(minutes=30),
                    )
                    _boost_active = True
                    remaining = boost_until - datetime.utcnow()
                    logger.warning(
                        "🔄 Scraper boost DEVAM: %.0f dakika kaldi",
                        remaining.total_seconds() / 60,
                    )
                else:
                    state.value = None
                    await db.commit()
    except Exception as e:
        logger.error("Boost startup restore hatasi: %s", e)


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
        import re
        from app.scrapers.spk_scraper import SPKScraper
        from app.models.spk_application import SPKApplication
        from app.models.ipo import IPO, DeletedIPO
        from sqlalchemy import select

        def _normalize(name: str) -> str:
            """Sirket ismini normalize et — bosluk/satir sonu/harf farklarini gider."""
            return re.sub(r"\s+", " ", name.strip()).lower() if name else ""

        def _name_in_set(spk_name: str, name_set: set) -> bool:
            """SPK ismi verilen settekilerden biriyle eslesiyor mu?
            1. birebir  2. startswith (kisa isim)  3. ilk 3 kelime
            """
            n = _normalize(spk_name)
            if not n:
                return False
            if n in name_set:
                return True
            for ref_n in name_set:
                if n.startswith(ref_n) or ref_n.startswith(n):
                    return True
            skip = {"a.ş.", "a.s.", "aş", "as", "san.", "tic.", "ve", "ve/veya", "ltd.", "şti.", "sti."}
            spk_words = [w for w in n.split() if w not in skip][:3]
            if len(spk_words) < 2:
                return False
            spk_key = " ".join(spk_words)
            for ref_n in name_set:
                ref_words = [w for w in ref_n.split() if w not in skip][:3]
                if " ".join(ref_words) == spk_key:
                    return True
            return False

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
                skipped_ipo = 0
                processed_names = set()  # Ayni scrape icinde duplike onle

                # IPO tablosundaki TUM sirketleri al (SPK'dan gecmis, tekrar eklenmemeli)
                ipo_result = await db.execute(select(IPO.company_name))
                ipo_names_normalized = set()
                for (name,) in ipo_result.all():
                    if name:
                        ipo_names_normalized.add(_normalize(name))

                # Silinen IPO kara listesi — scraper tekrar eklemesin
                deleted_result = await db.execute(select(DeletedIPO.company_name))
                deleted_names_normalized = set()
                for (name,) in deleted_result.all():
                    if name:
                        deleted_names_normalized.add(_normalize(name))

                skipped_deleted = 0

                # 1. Yeni ekle + mevcut guncelle
                for app_data in applications:
                    company_name = app_data.get("company_name", "").strip()
                    if not company_name:
                        continue

                    # Ayni scrape icinde ayni ismi tekrar isleme
                    if company_name in processed_names:
                        continue
                    processed_names.add(company_name)

                    # IPO tablosunda zaten var — SPK'dan gecmis, pending'e ekleme
                    if _name_in_set(company_name, ipo_names_normalized):
                        skipped_ipo += 1
                        continue

                    # Kara listede mi? (admin silmis — tekrar ekleme)
                    if _name_in_set(company_name, deleted_names_normalized):
                        skipped_deleted += 1
                        continue

                    result = await db.execute(
                        select(SPKApplication).where(
                            SPKApplication.company_name == company_name
                        )
                    )
                    existing = result.scalars().first()

                    if existing:
                        # Admin silmis ise DOKUNMA — tekrar pending yapma
                        if existing.status == "deleted":
                            skipped_deleted += 1
                            continue
                        # Mevcut kaydi guncelle (tarih degismis olabilir)
                        new_date = app_data.get("application_date")
                        if new_date and existing.application_date != new_date:
                            existing.application_date = new_date
                            updated_count += 1
                        # Daha once approved/rejected olmus ama tekrar listede ise pending'e cevir
                        if existing.status not in ("pending",):
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
                "SPK: %d basvuru tarandi — %d yeni, %d guncellendi, %d onaylandi (listeden cikti), %d IPO'da mevcut, %d kara listede (atlandi)",
                len(applications), new_count, updated_count, removed_count, skipped_ipo, skipped_deleted,
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


async def tweet_distribution_morning_job():
    """Dagitim gunu sabahi 08:00 (TR) — tweet #2 tekrar at.

    subscription_start == bugun olan IPO'lar icin dagitim tweeti atar.
    Gece yarisi auto_update_statuses zaten newly_approved → in_distribution
    gecisini yapar ve tweet atar ama o gece yarisi olur.
    Bu job sabah 08:00'de dagitim bilgisi netlestikten sonra tekrar atar.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO

        today = _today_tr()

        async with async_session() as db:
            # Bugun dagitima baslayan IPO'lar (subscription_start == today)
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status == "in_distribution",
                        IPO.subscription_start == today,
                        IPO.archived == False,
                    )
                )
            )
            ipos = list(result.scalars().all())

            if not ipos:
                return

            from app.services.twitter_service import tweet_distribution_start
            from app.services.admin_telegram import notify_tweet_sent

            for ipo in ipos:
                try:
                    tw_ok = tweet_distribution_start(ipo)
                    logger.info("Dagitim sabah tweeti: %s", ipo.ticker or ipo.company_name)
                    await notify_tweet_sent("dagitim_baslangic", ipo.ticker or ipo.company_name, tw_ok)
                except Exception as e:
                    logger.error("Dagitim sabah tweet hatasi (%s): %s", ipo.ticker, e)

                if len(ipos) > 1:
                    await asyncio.sleep(random.uniform(50, 55))

    except Exception as e:
        logger.error(f"Dagitim sabah tweet job hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("Dagitim Sabah Tweet", str(e))
        except Exception:
            pass


async def archive_old_ipos():
    """25 is gunu gecen halka arzlari arsivler + 25/25 performans tweeti atar.

    Her gun 12:00 TR (UTC 09:00) calisir.
    Iki kosuldan biri yeterlii:
      1) trading_start tarihi ~37 takvim gunu oncesinde olan
      2) trading_day_count >= 25 olan (DB'de ceiling track verisi ile doldurulur)

    Arsivlemeden ONCE 25/25 performans tweetini atar (sadece trading_day_count == 25 olanlara).
    """
    try:
        from sqlalchemy import select, and_, or_
        from decimal import Decimal
        from app.models.ipo import IPO, IPOCeilingTrack

        async with async_session() as db:
            # ~37 takvim gunu ~ 25 is gunu
            cutoff = _today_tr() - timedelta(days=37)

            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.archived == False,
                        or_(
                            # Kosul 1: trading_start 37+ takvim gunu gecmis
                            and_(
                                IPO.trading_start.isnot(None),
                                IPO.trading_start <= cutoff,
                            ),
                            # Kosul 2: 25 islem gunu tamamlanmis (DB ceiling track verisi)
                            and_(
                                IPO.trading_day_count.isnot(None),
                                IPO.trading_day_count >= 25,
                            ),
                        ),
                    )
                )
            )

            archived_count = 0
            # Eski IPO filtresi: 40 takvim gunundan once baslayanlar "eski" sayilir
            fresh_cutoff = _today_tr() - timedelta(days=40)

            for ipo in result.scalars().all():
                # --- 25/25 Performans Tweeti (arsivlemeden once) ---
                # Sadece tam 25 gun tamamlayanlar icin at (eskileri atlamak icin)
                if ipo.trading_day_count and ipo.trading_day_count == 25 and ipo.ticker:
                    # Ek guvenlik: cok eski IPO'lara tweet ATMA
                    if not ipo.trading_start or ipo.trading_start < fresh_cutoff:
                        logger.info(
                            "Arsiv: %s — eski IPO (trading_start=%s), 25/25 tweet atlaniyor",
                            ipo.ticker, ipo.trading_start,
                        )
                    else:
                        try:
                            from app.services.twitter_service import tweet_25_day_performance

                            # Ceiling track verilerini oku
                            track_result = await db.execute(
                                select(IPOCeilingTrack)
                                .where(IPOCeilingTrack.ipo_id == ipo.id)
                                .order_by(IPOCeilingTrack.trading_day.asc())
                                .limit(25)
                            )
                            tracks = track_result.scalars().all()

                            if tracks:
                                ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
                                days_data = []
                                for t in tracks:
                                    days_data.append({
                                        "trading_day": t.trading_day,
                                        "date": t.trade_date,
                                        "open": t.open_price or t.close_price,
                                        "high": t.high_price or t.close_price,
                                        "low": t.low_price or t.close_price,
                                        "close": t.close_price,
                                        "volume": 0,
                                        "durum": t.durum or "",
                                    })

                                if days_data and ipo_price > 0:
                                    last_close = float(days_data[-1]["close"])
                                    total_pct = ((last_close - ipo_price) / ipo_price) * 100
                                    ceiling_d = sum(1 for t in tracks if t.hit_ceiling)
                                    floor_d = sum(1 for t in tracks if t.hit_floor)
                                    avg_lot = (
                                        float(ipo.estimated_lots_per_person)
                                        if ipo.estimated_lots_per_person else None
                                    )

                                    tweet_ok = tweet_25_day_performance(
                                        ipo, last_close, total_pct,
                                        ceiling_d, floor_d, avg_lot,
                                        days_data=days_data,
                                    )
                                    logger.info(
                                        "Arsiv: %s — 25/25 performans tweeti atildi",
                                        ipo.ticker,
                                    )

                                    # Admin Telegram bildirim
                                    try:
                                        from app.services.admin_telegram import notify_tweet_sent
                                        await notify_tweet_sent(
                                            "25_gun_performans",
                                            ipo.ticker,
                                            tweet_ok,
                                            f"Toplam: %{total_pct:+.1f} | Tavan: {ceiling_d} | Taban: {floor_d}",
                                        )
                                    except Exception:
                                        pass

                                    # Tweetler arasi jitter
                                    await asyncio.sleep(random.uniform(50, 55))
                        except Exception as tweet_err:
                            logger.warning(
                                "Arsiv: %s — 25/25 tweet hatasi: %s",
                                ipo.ticker, tweet_err,
                            )

                # --- Arsivle ---
                ipo.archived = True
                ipo.archived_at = datetime.now(timezone.utc)
                # 26 = "kayit tamamlandi" marker (25 gun verisi alindi, arsivlendi)
                if ipo.trading_day_count and ipo.trading_day_count <= 25:
                    ipo.trading_day_count = 26
                archived_count += 1
                logger.info(f"IPO arsivlendi: {ipo.ticker or ipo.company_name}")

            if archived_count > 0:
                await db.commit()
                logger.info(f"Arsiv: {archived_count} IPO arsivlendi")
            else:
                logger.info("Arsiv: Arsivlenecek IPO yok")

    except Exception as e:
        logger.error(f"IPO arsiv hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("IPO Arşiv", str(e))
        except Exception:
            pass


# -------------------------------------------------------
# IPO Zamanlama Yardimcilari — subscription_hours bazli
# -------------------------------------------------------

def _get_closing_time(ipo) -> tuple:
    """IPO'nun kapanis saatini dondurur (hour, minute).
    subscription_hours = "09:00-17:00" → (17, 0)
    Yoksa default (17, 0)
    """
    if ipo.subscription_hours:
        import re as _re
        parts = str(ipo.subscription_hours).split("-")
        if len(parts) >= 2:
            time_str = parts[-1].strip()
            match = _re.match(r"(\d{1,2}):(\d{2})", time_str)
            if match:
                return int(match.group(1)), int(match.group(2))
    return 17, 0  # default


def _get_opening_time(ipo) -> tuple:
    """IPO'nun acilis saatini dondurur (hour, minute).
    subscription_hours = "09:00-17:00" → (9, 0)
    Yoksa default (9, 0)
    """
    if ipo.subscription_hours:
        import re as _re
        parts = str(ipo.subscription_hours).split("-")
        if parts:
            time_str = parts[0].strip()
            match = _re.match(r"(\d{1,2}):(\d{2})", time_str)
            if match:
                return int(match.group(1)), int(match.group(2))
    return 9, 0  # default


# In-memory dedup — ayni IPO + event tipi icin tekrar gonderim engelle
# Key: "ipo_{id}_{event_type}" → Value: gonderim tarihi (date)
# Her gun sifirlanir (date farkliysa cache miss olur)
_timing_sent: dict[str, date] = {}


def _timing_already_sent(ipo_id: int, event_type: str) -> bool:
    """Bu IPO + event bugün zaten gönderildi mi?"""
    key = f"ipo_{ipo_id}_{event_type}"
    sent_date = _timing_sent.get(key)
    return sent_date == _today_tr()


def _timing_mark_sent(ipo_id: int, event_type: str):
    """Bu IPO + event'i bugün gönderildi olarak işaretle."""
    key = f"ipo_{ipo_id}_{event_type}"
    _timing_sent[key] = _today_tr()


def _get_active_reminder(remaining_minutes: float) -> str | None:
    """Kalan dakikaya göre aktif hatırlatma tipini döndürür."""
    if 25 <= remaining_minutes <= 35:
        return "reminder_30min"
    elif 55 <= remaining_minutes <= 65:
        return "reminder_1h"
    elif 115 <= remaining_minutes <= 125:
        return "reminder_2h"
    elif 235 <= remaining_minutes <= 245:
        return "reminder_4h"
    return None


async def check_reminders():
    """Hatirlatma zamani kontrolu — subscription_hours bazli.

    Her IPO'nun kendi kapanis saatine gore hesaplar:
    - 30 dk oncesi (kapanis - 30dk)
    - 1 saat oncesi (kapanis - 1h)
    - 2 saat oncesi (kapanis - 2h)
    - 4 saat oncesi (kapanis - 4h)

    Ayrica tweet atar: 4h kala + 30dk kala (resimli).
    """
    try:
        from zoneinfo import ZoneInfo
        from sqlalchemy import select, and_, or_
        from app.models.ipo import IPO
        from app.models.user import User
        from app.services.notification import NotificationService

        TR_TZ = ZoneInfo("Europe/Istanbul")

        async with async_session() as db:
            today = _today_tr()
            now_tr = datetime.now(TR_TZ)
            from datetime import time as Time

            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status.in_(["in_distribution", "active"]),
                        IPO.subscription_end == today,
                    )
                )
            )
            last_day_ipos = list(result.scalars().all())

            if not last_day_ipos:
                return

            notif_service = NotificationService(db)

            time_labels = {
                "reminder_30min": "30 dakika",
                "reminder_1h": "1 saat",
                "reminder_2h": "2 saat",
                "reminder_4h": "4 saat",
            }

            _r_tweet_idx = 0

            for ipo in last_day_ipos:
                # Her IPO'nun kendi kapanis saati
                close_h, close_m = _get_closing_time(ipo)
                closing_time = now_tr.replace(
                    hour=close_h, minute=close_m, second=0, microsecond=0,
                )
                remaining_minutes = (closing_time - now_tr).total_seconds() / 60

                if remaining_minutes < 0:
                    continue  # Bu IPO'nun suresi dolmus

                reminder_check = _get_active_reminder(remaining_minutes)
                if not reminder_check:
                    continue

                # Dedup — bu IPO + bu reminder bugün zaten gönderildi mi?
                if _timing_already_sent(ipo.id, reminder_check):
                    continue

                # Bu reminder tipini seçmiş kullanıcıları bul (FCM veya Expo token)
                users_result = await db.execute(
                    select(User).where(
                        and_(
                            User.notifications_enabled == True,
                            User.deleted == False,
                            getattr(User, reminder_check) == True,
                            or_(
                                and_(User.fcm_token.isnot(None), User.fcm_token != ""),
                                and_(User.expo_push_token.isnot(None), User.expo_push_token != ""),
                            ),
                        )
                    )
                )
                users = list(users_result.scalars().all())

                if not users:
                    _timing_mark_sent(ipo.id, reminder_check)
                    continue

                time_label = time_labels.get(reminder_check, "")
                close_time_str = f"{close_h:02d}:{close_m:02d}"

                for user in users:
                    await notif_service._send_to_user(
                        user=user,
                        title=f"Son Gun Hatirlatma",
                        body=f"{ipo.ticker or ipo.company_name} icin basvuru son gun! Saat {close_time_str}'a {time_label} kaldi.",
                        data={
                            "type": "reminder",
                            "ipo_id": str(ipo.id),
                            "ticker": ipo.ticker or "",
                        },
                        channel_id="ipo_alerts_v2",
                    )

                _timing_mark_sent(ipo.id, reminder_check)

                # Tweet at — Son 4 Saat veya Son 30 Dakika
                try:
                    from app.services.twitter_service import tweet_last_4_hours, tweet_last_30_min
                    from app.services.admin_telegram import notify_tweet_sent
                    if _r_tweet_idx > 0:
                        jitter = random.uniform(50, 55)
                        logger.info("Hatirlatma tweet jitter: %.1f sn (%s)", jitter, ipo.ticker or ipo.company_name)
                        await asyncio.sleep(jitter)
                    if reminder_check == "reminder_4h":
                        tw_ok = tweet_last_4_hours(ipo)
                        _r_tweet_idx += 1
                        await notify_tweet_sent("son_4_saat", ipo.ticker or ipo.company_name, tw_ok)
                    elif reminder_check == "reminder_30min":
                        tw_ok = tweet_last_30_min(ipo)
                        _r_tweet_idx += 1
                        await notify_tweet_sent("son_30_dk", ipo.ticker or ipo.company_name, tw_ok)
                except Exception:
                    pass

            logger.info(f"Hatirlatma: {len(last_day_ipos)} IPO kontrol edildi (TR: {now_tr.strftime('%H:%M')})")

    except Exception as e:
        logger.error(f"Hatirlatma kontrol hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("Hatırlatma Kontrolü", str(e))
        except Exception:
            pass


async def check_morning_tweets():
    """Sabah tweet zamanlama kontrolu — subscription_hours bazli.

    Her 5 dakikada calisir ve su kontrolleri yapar:
    1. Dagitim sabah tweeti: subscription_start == bugun, acilisa 1 saat kala
    2. Son gun sabah tweeti: subscription_end == bugun, acilisa 1 saat kala

    IPO'nun acilis saatine gore dinamik zamanlama yapar.
    Dedup: _timing_sent ile ayni gun tekrar tweet atilmaz.
    """
    try:
        from zoneinfo import ZoneInfo
        from sqlalchemy import select, and_, or_
        from app.models.ipo import IPO

        TR_TZ = ZoneInfo("Europe/Istanbul")
        now_tr = datetime.now(TR_TZ)
        today = _today_tr()

        async with async_session() as db:
            # Bugun dagitima baslayan VEYA bugun son gun olan IPO'lar
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status.in_(["in_distribution", "active"]),
                        IPO.archived == False,
                        or_(
                            IPO.subscription_start == today,
                            IPO.subscription_end == today,
                        ),
                    )
                )
            )
            ipos = list(result.scalars().all())

            if not ipos:
                return

            from app.services.twitter_service import tweet_distribution_start
            from app.services.admin_telegram import notify_tweet_sent

            tweet_idx = 0

            for ipo in ipos:
                open_h, open_m = _get_opening_time(ipo)
                opening_time = now_tr.replace(
                    hour=open_h, minute=open_m, second=0, microsecond=0,
                )
                minutes_to_open = (opening_time - now_tr).total_seconds() / 60

                # --- Dagitim sabah tweeti: acilisa 55-65 dk kala (1 saat) ---
                if (ipo.subscription_start == today
                        and 55 <= minutes_to_open <= 65
                        and not _timing_already_sent(ipo.id, "distribution_morning_tweet")):
                    if tweet_idx > 0:
                        await asyncio.sleep(random.uniform(50, 55))
                    tw_ok = tweet_distribution_start(ipo)
                    await notify_tweet_sent("dagitim_sabah_timing", ipo.ticker or ipo.company_name, tw_ok)
                    _timing_mark_sent(ipo.id, "distribution_morning_tweet")
                    tweet_idx += 1
                    logger.info(
                        "Dagitim sabah tweeti (timing): %s — acilisa %d dk",
                        ipo.ticker or ipo.company_name, int(minutes_to_open),
                    )

                # --- Son gun sabah tweeti: artik send_last_day_warnings() icinde 09:00 TR'de ---
                # Bildirim ve tweet ayni anda atilir (check_morning_tweets yerine)

        if tweet_idx > 0:
            logger.info("Sabah tweet kontrolu: %d tweet atildi", tweet_idx)

    except Exception as e:
        logger.error(f"Sabah tweet kontrol hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("Sabah Tweet Kontrol", str(e))
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


async def check_trading_start_halkarz():
    """Saatlik islem tarihi kontrolu — sadece awaiting_trading IPO'lar icin.

    HalkArz.com detay sayfasindan 'Bist Ilk Islem Tarihi' alanini kontrol eder.
    Tespit edilince trading_start alanini set eder + Telegram'a bildirir.
    auto_update_statuses sonraki calismasinda IPO'yu 'trading'e gecirir.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO
        from app.scrapers.halkarz_scraper import HalkArzScraper

        async with async_session() as db:
            # Sadece awaiting_trading + trading_start bos olan IPO'lar
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status == "awaiting_trading",
                        IPO.archived == False,
                        IPO.trading_start.is_(None),
                    )
                )
            )
            ipos = list(result.scalars().all())

            if not ipos:
                return  # Kontrol edilecek IPO yok

            logger.info(
                "Trading start kontrolu: %d awaiting_trading IPO kontrol edilecek",
                len(ipos),
            )

            scraper = HalkArzScraper()
            try:
                # WP API'den postlari al
                posts = await scraper.fetch_all_posts()
                if not posts:
                    return

                updated = 0
                for ipo in ipos:
                    # Eslesen postu bul
                    matched_post = None
                    for post in posts:
                        if scraper.match_post_to_ipo(post["title"], ipo.company_name):
                            matched_post = post
                            break

                    if not matched_post:
                        continue

                    # Detay sayfasina git
                    detail = await scraper.fetch_detail_page(matched_post["link"])
                    if not detail:
                        continue

                    trading_start = detail.get("trading_start")
                    if trading_start and not ipo.trading_start:
                        ipo.trading_start = trading_start
                        ipo.expected_trading_date = trading_start
                        ipo.updated_at = datetime.utcnow()
                        updated += 1
                        logger.info(
                            "HalkArz islem tarihi tespit: %s → %s",
                            ipo.ticker or ipo.company_name,
                            trading_start,
                        )

                        # Telegram bildir
                        try:
                            from app.services.admin_telegram import send_admin_message
                            await send_admin_message(
                                f"📊 <b>İşlem Tarihi Tespit Edildi</b>\n\n"
                                f"<b>{ipo.ticker or 'N/A'}</b> — {ipo.company_name}\n"
                                f"<b>Bist İlk İşlem:</b> {trading_start}\n"
                                f"<b>Kaynak:</b> HalkArz.com"
                            )
                        except Exception:
                            pass

                if updated > 0:
                    await db.commit()
                    logger.info("HalkArz trading start: %d IPO guncellendi", updated)

            finally:
                await scraper.close()

    except Exception as e:
        logger.error("HalkArz trading start kontrol hatasi: %s", e)
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("HalkArz Trading Start Kontrol", str(e))
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
    """BUGUN son gunu olan halka arzlar icin sabah uyarisi + tweet gonder.

    Sabah 09:00 TR'de (UTC 06:00) calisir:
    1. Push bildirim: "BUGUN son gun!" (notify_ipo_last_day)
    2. Tweet: son basvuru gunu gorseli ile X'e tweet (tweet_last_day_morning)

    Ikisi de ayni anda atilir — kullanici hem bildirim hem tweet alir.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO
        from app.services.notification import NotificationService

        async with async_session() as db:
            today = _today_tr()
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status.in_(["in_distribution", "active"]),
                        IPO.subscription_end == today,
                    )
                )
            )
            last_day_ipos = list(result.scalars().all())

            if last_day_ipos:
                # 1. Push bildirimler
                notif_service = NotificationService(db)
                for ipo in last_day_ipos:
                    await notif_service.notify_ipo_last_day(ipo)
                await db.commit()

                # 2. Son gun sabah tweeti — bildirimle ayni anda
                from app.services.twitter_service import tweet_last_day_morning
                from app.services.admin_telegram import notify_tweet_sent
                for idx, ipo in enumerate(last_day_ipos):
                    if idx > 0:
                        await asyncio.sleep(random.uniform(50, 55))
                    tw_ok = tweet_last_day_morning(ipo)
                    await notify_tweet_sent("son_gun_sabah", ipo.ticker or ipo.company_name, tw_ok)
                    _timing_mark_sent(ipo.id, "last_day_morning_tweet")
                    logger.info("Son gun sabah tweeti: %s", ipo.ticker or ipo.company_name)

        logger.info(f"Son gun uyarisi: {len(last_day_ipos)} halka arz (bugun son gun)")

    except Exception as e:
        logger.error(f"Son gun uyarisi hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("Son Gün Uyarısı", str(e))
        except Exception:
            pass


async def tweet_spk_approval_intro_job():
    """SPK onayi geldikten 12 saat sonra sirket tanitim tweeti.

    Her saat calisir. created_at + 12 saat gecmis ama henuz tweet atilmamis
    newly_approved IPO'lar icin halka_arz_hakkinda_banner.png gorseli ile tweet atar.

    Ornek: SPK onayi 23:00'te gelirse → ertesi gun 11:00'de tweet atar.
    SPK onayi 01:00'da gelirse → ayni gun 13:00'da tweet atar.

    ONEMLI: tweet_company_intro sirket bilgisi (description/sector/price) yoksa
    tweet atmaz — scraper'larin bilgiyi doldurmasi beklenir (72 saate kadar).

    Duplicate koruma: DB'de intro_tweeted flag + _is_duplicate_tweet cache.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO
        from datetime import timedelta

        DELAY_HOURS = 12  # Sisteme eklenme saatinden 12 saat sonra

        async with async_session() as db:
            now = datetime.now(timezone.utc)

            # newly_approved, son 72 saat icinde olusturulmus, henuz tweet atilmamis
            cutoff = now - timedelta(hours=72)
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status == "newly_approved",
                        IPO.created_at >= cutoff,
                        IPO.intro_tweeted == False,
                    )
                )
            )
            new_ipos = list(result.scalars().all())

            if not new_ipos:
                return

            from app.services.twitter_service import tweet_company_intro
            from app.services.admin_telegram import notify_tweet_sent
            import asyncio

            tweeted = 0
            for ipo in new_ipos:
                if not ipo.created_at:
                    continue

                # 12 saat gecti mi?
                tweet_time = ipo.created_at + timedelta(hours=DELAY_HOURS)
                if now < tweet_time:
                    continue

                # Cok gec olmasin — 72 saatten eski ise atla
                if now > ipo.created_at + timedelta(hours=72):
                    continue

                if tweeted > 0:
                    await asyncio.sleep(random.uniform(2700, 2750))  # ~45 dk arayla tweet

                success = tweet_company_intro(ipo)
                await notify_tweet_sent("sirket_tanitim_spk", ipo.ticker or ipo.company_name, success)
                if success:
                    ipo.intro_tweeted = True
                    await db.commit()
                    tweeted += 1

            if tweeted > 0:
                logger.info(f"SPK onay tanitim tweeti: {tweeted} IPO")

    except Exception as e:
        logger.error(f"SPK onay tanitim tweet hatasi: {e}")
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("SPK Onay Tanıtım Tweet", str(e))
        except Exception:
            pass


async def tweet_last_day_morning_job():
    """Son gun sabahi 05:00'da tweet — hafif uyari tonu.

    Bugun subscription_end olan in_distribution IPO'lar icin tweet atar.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO

        async with async_session() as db:
            today = _today_tr()
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status.in_(["in_distribution", "active"]),
                        IPO.subscription_end == today,
                    )
                )
            )
            last_day_ipos = list(result.scalars().all())

            if not last_day_ipos:
                return

            from app.services.twitter_service import tweet_last_day_morning
            from app.services.admin_telegram import notify_tweet_sent
            for idx, ipo in enumerate(last_day_ipos):
                if idx > 0:
                    import asyncio
                    await asyncio.sleep(random.uniform(50, 55))  # Jitter
                tw_ok = tweet_last_day_morning(ipo)
                await notify_tweet_sent("son_gun_sabah", ipo.ticker or ipo.company_name, tw_ok)

            logger.info(f"Son gun sabah tweeti: {len(last_day_ipos)} IPO")

    except Exception as e:
        logger.error(f"Son gun sabah tweet hatasi: {e}")


async def tweet_company_intro_job():
    """Dagitima cikan IPO'lar icin ertesi gun 20:00'de sirket tanitim tweeti.

    Dun in_distribution'a gecen (subscription_start == dun) IPO'lar icin tweet atar.
    intro_tweeted flag ile duplicate koruma saglanir.
    """
    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO
        from datetime import timedelta

        async with async_session() as db:
            yesterday = _today_tr() - timedelta(days=1)
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.status.in_(["in_distribution", "active", "newly_approved"]),
                        IPO.subscription_start == yesterday,
                        IPO.intro_tweeted == False,
                    )
                )
            )
            new_ipos = list(result.scalars().all())

            if not new_ipos:
                return

            from app.services.twitter_service import tweet_company_intro
            from app.services.admin_telegram import notify_tweet_sent
            tweeted = 0
            for idx, ipo in enumerate(new_ipos):
                if idx > 0:
                    import asyncio
                    await asyncio.sleep(random.uniform(2700, 2750))  # ~45 dk arayla tweet
                success = tweet_company_intro(ipo)
                await notify_tweet_sent("sirket_tanitim", ipo.ticker or ipo.company_name, success)
                if success:
                    ipo.intro_tweeted = True
                    await db.commit()
                    tweeted += 1

            logger.info(f"Sirket tanitim tweeti: {tweeted}/{len(new_ipos)} IPO")

    except Exception as e:
        logger.error(f"Sirket tanitim tweet hatasi: {e}")


async def tweet_spk_pending_monthly_job():
    """Her ayin 1'inde SPK onayi bekleyenler gorselli tweet.

    static/img/spk_bekleyenler_banner.png gorselini kullanir.
    """
    try:
        import os
        from sqlalchemy import select, func
        from app.models.spk_application import SPKApplication

        async with async_session() as db:
            result = await db.execute(
                select(func.count()).select_from(SPKApplication).where(
                    SPKApplication.status == "pending"
                )
            )
            pending_count = result.scalar() or 0

            if pending_count == 0:
                return

            # Gorsel yolu — Render'da cwd app/
            image_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "app", "static", "img", "spk_bekleyenler_banner.png"
            )
            # Alternatif yol
            if not os.path.exists(image_path):
                image_path = os.path.join("app", "static", "img", "spk_bekleyenler_banner.png")
            if not os.path.exists(image_path):
                image_path = None  # Gorsel bulunamazsa sadece metin

            from app.services.twitter_service import tweet_spk_pending_with_image
            from app.services.admin_telegram import notify_tweet_sent
            tw_ok = tweet_spk_pending_with_image(pending_count, image_path)
            await notify_tweet_sent("spk_bekleyenler_aylik", f"{pending_count} basvuru", tw_ok)

            logger.info(f"SPK bekleyenler aylik tweet: {pending_count} basvuru")

    except Exception as e:
        logger.error(f"SPK bekleyenler tweet hatasi: {e}")


async def push_health_report_job():
    """Push bildirim saglik raporu — 4 saatte bir.

    Toplam kullanici, FCM token durumu, stale token sayisi.
    Telegram admin'e gonderilir.
    """
    try:
        from sqlalchemy import select, and_, func
        from app.models.user import User
        from app.services.admin_telegram import notify_push_health_report

        async with async_session() as session:
            # Toplam kullanici
            total_result = await session.execute(
                select(func.count(User.id)).where(User.deleted == False)
            )
            total_users = total_result.scalar() or 0

            # FCM token olan
            fcm_result = await session.execute(
                select(func.count(User.id)).where(
                    and_(
                        User.deleted == False,
                        User.fcm_token.isnot(None),
                        User.fcm_token != "",
                    )
                )
            )
            with_fcm = fcm_result.scalar() or 0

            # Expo token olan
            expo_result = await session.execute(
                select(func.count(User.id)).where(
                    and_(
                        User.deleted == False,
                        User.expo_push_token.isnot(None),
                        User.expo_push_token != "",
                    )
                )
            )
            with_expo = expo_result.scalar() or 0

            # Bildirim acik olan
            notif_result = await session.execute(
                select(func.count(User.id)).where(
                    and_(
                        User.deleted == False,
                        User.notifications_enabled == True,
                    )
                )
            )
            notif_enabled = notif_result.scalar() or 0

            await notify_push_health_report(
                total_users=total_users,
                with_fcm_token=with_fcm,
                with_expo_token=with_expo,
                notifications_enabled=notif_enabled,
            )
    except Exception as e:
        logger.error(f"Push health report hatasi: {e}")


async def _market_snapshot_attempt(retry_num: int = 0):
    """Tek bir snapshot denemesi — retry mantigi market_snapshot_tweet'te.

    Returns dict with "message", "error", or "retry" key.
    """
    from sqlalchemy import select, and_, func
    from decimal import Decimal
    from datetime import date as date_type
    from app.models.ipo import IPO, IPOCeilingTrack

    async with async_session() as db:
        # Status = "trading" olan tum IPO'lari bul
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
            logger.info("Ogle arasi snapshot: Aktif islem goren IPO yok")
            return {"error": "Aktif islem goren IPO yok (status=trading)"}

        today = date_type.today()
        snapshot_data = []
        skip_reasons = []
        no_today_track_count = 0  # Bugunku track'i olmayan hisse sayisi

        for ipo in active_ipos:
            if not ipo.ticker:
                skip_reasons.append(f"[?] ticker yok (id={ipo.id})")
                continue

            # 25 gun dolmus IPO'lar snapshot'ta olmasin (25/25 dahil)
            if ipo.trading_day_count and ipo.trading_day_count >= 25:
                skip_reasons.append(f"[{ipo.ticker}] trading_day_count={ipo.trading_day_count} >=25 skip")
                continue

            # trading_day: IPO tablosundaki trading_day_count + 1 (bugun icin)
            # Bu deger sync script'ten bagimsiz, admin panelden yonetiliyor
            real_trading_day = (ipo.trading_day_count or 0) + 1

            # Bugunun trading_day'i 25+ ise atla
            if real_trading_day >= 26:
                skip_reasons.append(f"[{ipo.ticker}] real_trading_day={real_trading_day} >=26 skip")
                continue

            # Bugunun ceiling track kaydini al (en son guncellenen)
            track_result = await db.execute(
                select(IPOCeilingTrack).where(
                    and_(
                        IPOCeilingTrack.ipo_id == ipo.id,
                        IPOCeilingTrack.trade_date == today,
                    )
                ).order_by(IPOCeilingTrack.id.desc()).limit(1)
            )
            today_track = track_result.scalar_one_or_none()

            # Track yoksa son track'i dene (belki sync henuz calismadi)
            if not today_track:
                no_today_track_count += 1
                last_track_result = await db.execute(
                    select(IPOCeilingTrack).where(
                        IPOCeilingTrack.ipo_id == ipo.id,
                    ).order_by(IPOCeilingTrack.id.desc()).limit(1)
                )
                last_track = last_track_result.scalar_one_or_none()
                if not last_track:
                    # Hic track yok — ilk islem gunu olabilir (trading_day_count=0)
                    # Yine de snapshot'a ekle, HA fiyati ve 0% ile goster
                    if (ipo.trading_day_count or 0) == 0:
                        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
                        snapshot_data.append({
                            "ticker": ipo.ticker,
                            "trading_day": real_trading_day,
                            "close_price": ipo_price,  # Henuz kapanıs yok, HA fiyati goster
                            "pct_change": 0.0,
                            "cum_pct": 0.0,
                            "durum": "ilk_gun",
                            "alis_lot": None,
                            "satis_lot": None,
                            "ipo_price": ipo_price,
                        })
                        skip_reasons.append(f"[{ipo.ticker}] ilk islem gunu, track yok, HA fiyati ile eklendi")
                    else:
                        skip_reasons.append(f"[{ipo.ticker}] hic track yok (today={today})")
                    continue
                # Son track'in trade_date bugun degilse → borsa kapali veya sync calismadi
                # Yine de goster, son bilinen veriyle
                today_track = last_track
                skip_reasons.append(f"[{ipo.ticker}] bugun track yok, son track={last_track.trade_date} kullanildi")

            # Kumulatif % hesapla (HA fiyatindan)
            ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
            close_price = float(today_track.close_price) if today_track.close_price else 0
            cum_pct = 0.0
            if ipo_price > 0 and close_price > 0:
                cum_pct = ((close_price - ipo_price) / ipo_price) * 100

            # Gunluk % — H sutunundan (pct_change) anlik geliyor
            daily_pct = float(today_track.pct_change) if today_track.pct_change is not None else 0.0

            # Durum
            durum = today_track.durum or "not_kapatti"

            snapshot_data.append({
                "ticker": ipo.ticker,
                "trading_day": real_trading_day,
                "close_price": close_price,
                "pct_change": daily_pct,
                "cum_pct": cum_pct,
                "durum": durum,
                "alis_lot": today_track.alis_lot,
                "satis_lot": today_track.satis_lot,
                "ipo_price": ipo_price,
            })

        # Eger hic bugunku track yoksa ve henuz retry hakki varsa → "retry" sinyali don
        eligible_count = len(snapshot_data) + no_today_track_count
        if no_today_track_count > 0 and no_today_track_count == eligible_count and retry_num < 2:
            msg = f"Hic bugunku track yok ({no_today_track_count} hisse), veri bekleniyor... (deneme {retry_num+1}/3)"
            logger.info("Ogle arasi snapshot: %s", msg)
            return {"retry": msg}

        if not snapshot_data:
            msg = f"Bugun islem goren hisse yok (today={today}, aktif={len(active_ipos)})"
            if skip_reasons:
                msg += " | " + "; ".join(skip_reasons[:10])
            logger.info("Ogle arasi snapshot: %s", msg)
            return {"error": msg}

        logger.info(
            "Ogle arasi snapshot: %d hisse — %s",
            len(snapshot_data),
            ", ".join(s["ticker"] for s in snapshot_data),
        )
        if skip_reasons:
            logger.info("Snapshot skip detay: %s", "; ".join(skip_reasons[:10]))

        # Gorsel olustur
        from app.services.chart_image_generator import generate_market_snapshot_image
        image_path = generate_market_snapshot_image(snapshot_data)

        if not image_path:
            logger.error("Ogle arasi snapshot: Gorsel olusturulamadi")
            return {"error": "Gorsel olusturulamadi (generate_market_snapshot_image None dondu)"}

        # Tweet at
        from app.services.twitter_service import tweet_market_snapshot
        from app.services.admin_telegram import notify_tweet_sent
        tw_ok = tweet_market_snapshot(snapshot_data, image_path)
        await notify_tweet_sent("piyasa_snapshot", f"{len(snapshot_data)} hisse", tw_ok)

        tickers_str = ", ".join(s["ticker"] for s in snapshot_data)
        if tw_ok:
            logger.info("Ogle arasi snapshot tweet basarili: %d hisse", len(snapshot_data))
            return {"message": f"Tweet olusturuldu — {len(snapshot_data)} hisse ({tickers_str})"}
        else:
            logger.error("Ogle arasi snapshot tweet BASARISIZ — tw_ok=False")
            return {"error": f"tweet_market_snapshot False dondu — {len(snapshot_data)} hisse ({tickers_str})"}


async def market_snapshot_tweet():
    """Ogle arasi market snapshot tweet — 14:00 TR (UTC 11:00).

    Islemde olan tum halka arz hisselerinin anlik durumunu
    kart bazli gorsel ile tweet atar.

    Veri henuz gelmemisse (bugunun trade_date'i yoksa) 60sn bekleyip
    2 kez daha dener (toplam 3 deneme, maks ~2dk bekleme).

    Returns dict with "message" or "error" key for debug.
    """
    import asyncio

    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = await _market_snapshot_attempt(retry_num=attempt)

            if result and result.get("retry"):
                logger.info("Snapshot retry %d/3 — 60sn bekleniyor: %s", attempt + 1, result["retry"])
                await asyncio.sleep(60)
                continue

            return result

        except Exception as e:
            logger.error(f"Ogle arasi snapshot hatasi (deneme {attempt+1}): {e}", exc_info=True)
            return {"error": f"Exception: {str(e)}"}

    return {"error": f"3 deneme sonrasi veri gelmedi — tweet olusturulamadi"}


# ═══════════════════════════════════════════════════════════════
# T16 — ACILIS BILGILERI (09:57 TR / 06:57 UTC)
# Ilk 5 islem gunu icindeki hisselerin acilis fiyatlarini tweet atar
# ═══════════════════════════════════════════════════════════════

async def _opening_summary_attempt(retry_num: int = 0):
    """Tek bir acilis bilgileri denemesi — retry mantigi opening_summary_tweet'te."""
    from datetime import date as date_type
    from sqlalchemy import select, and_
    from app.database import AsyncSessionLocal
    from app.models.ipo import IPO, IPOCeilingTrack

    async with AsyncSessionLocal() as db:
        today = date_type.today()

        result = await db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "trading",
                    IPO.archived == False,
                    IPO.ticker.isnot(None),
                    IPO.ceiling_tracking_active == True,
                )
            )
        )
        trading_ipos = list(result.scalars().all())

        stocks = []
        no_open_count = 0

        for ipo in trading_ipos:
            tracks_result = await db.execute(
                select(IPOCeilingTrack).where(
                    IPOCeilingTrack.ipo_id == ipo.id
                ).order_by(IPOCeilingTrack.trading_day.asc())
            )
            tracks = list(tracks_result.scalars().all())
            if not tracks:
                continue

            latest = tracks[-1]
            current_day = latest.trading_day
            if current_day > 5:
                continue

            today_track = None
            for t in tracks:
                if t.trade_date == today:
                    today_track = t
                    break

            if not today_track or not today_track.open_price:
                no_open_count += 1
                continue

            ceiling_days = sum(1 for t in tracks if t.hit_ceiling)
            floor_days = sum(1 for t in tracks if t.hit_floor)
            normal_days = len(tracks) - ceiling_days - floor_days

            prev_close = 0.0
            for t in tracks:
                if t.trade_date < today and t.close_price:
                    prev_close = float(t.close_price)

            ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
            if prev_close <= 0:
                prev_close = ipo_price

            open_price = float(today_track.open_price)
            pct_change = ((open_price - ipo_price) / ipo_price * 100) if ipo_price > 0 else 0
            daily_pct = ((open_price - prev_close) / prev_close * 100) if prev_close > 0 else 0

            alis_lot = today_track.alis_lot or 0
            satis_lot = today_track.satis_lot or 0

            if pct_change >= 9.5:
                durum = "tavan"
            elif pct_change <= -9.5:
                durum = "taban"
            elif pct_change > 0:
                durum = "alici_kapatti"
            elif pct_change < 0:
                durum = "satici_kapatti"
            else:
                durum = "not_kapatti"

            stocks.append({
                "ticker": ipo.ticker,
                "company_name": ipo.company_name,
                "trading_day": current_day,
                "ipo_price": ipo_price,
                "open_price": open_price,
                "prev_close": round(prev_close, 2),
                "pct_change": round(pct_change, 2),
                "daily_pct": round(daily_pct, 2),
                "durum": durum,
                "ceiling_days": ceiling_days,
                "floor_days": floor_days,
                "normal_days": normal_days,
                "alis_lot": alis_lot,
                "satis_lot": satis_lot,
            })

        if not stocks and no_open_count > 0 and retry_num < 2:
            return {"retry": f"Acilis fiyati olan hisse yok ({no_open_count} hisse veri bekliyor)"}

        if not stocks:
            logger.info("T16: Ilk 5 gun icinde hisse yok, tweet atilmadi.")
            return {"message": "Ilk 5 gun icinde hisse yok, tweet atilmadi."}

        tickers_str = ", ".join(s["ticker"] for s in stocks)

        from app.services.twitter_service import tweet_opening_summary
        from app.services.admin_telegram import notify_tweet_sent

        tw_ok = tweet_opening_summary(stocks)
        await notify_tweet_sent("acilis_ozet", tickers_str, tw_ok, f"{len(stocks)} hisse")

        if tw_ok:
            logger.info("T16 Acilis bilgileri tweet BASARILI — %d hisse (%s)", len(stocks), tickers_str)
            return {"message": f"T16 tweet olusturuldu — {len(stocks)} hisse ({tickers_str})"}
        else:
            logger.error("T16 Acilis bilgileri tweet BASARISIZ — tw_ok=False")
            return {"error": f"tweet_opening_summary False dondu — {len(stocks)} hisse ({tickers_str})"}


async def opening_summary_tweet():
    """Acilis bilgileri tweet — 09:57 TR (UTC 06:57).

    Ilk 5 islem gunu icindeki hisselerin acilis fiyatlarini
    grid layout gorsel ile tweet atar.

    Veri henuz gelmemisse 60sn bekleyip 2 kez daha dener.

    Returns dict with "message" or "error" key for debug.
    """
    import asyncio

    max_retries = 3
    for attempt in range(max_retries):
        try:
            result = await _opening_summary_attempt(retry_num=attempt)

            if result and result.get("retry"):
                logger.info("T16 retry %d/3 — 60sn bekleniyor: %s", attempt + 1, result["retry"])
                await asyncio.sleep(60)
                continue

            return result

        except Exception as e:
            logger.error(f"T16 acilis bilgileri hatasi (deneme {attempt+1}): {e}", exc_info=True)
            return {"error": f"Exception: {str(e)}"}

    return {"error": "T16: 3 deneme sonrasi veri gelmedi — tweet olusturulamadi"}


async def daily_ceiling_update():
    """Gun sonu tavan takip tweet — 18:20 (UTC 15:20).

    Borsa 18:00'de kapanir, 18:20'de kapanis verileri kesinlesir.
    Excel sync ile ipo_ceiling_tracks tablosuna yazilmis veriyi okuyarak
    gunluk takip ve 25 gun performans tweetlerini atar.
    Yahoo Finance KULLANILMAZ — veri kaynagi yerel DB (Matriks Excel sync).
    """
    global _ceiling_retry_pending
    try:
        from sqlalchemy import select, and_
        from decimal import Decimal
        from app.models.ipo import IPO, IPOCeilingTrack
        from app.scrapers.yahoo_finance_scraper import detect_ceiling_floor

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

            success_count = 0
            fail_count = 0
            failed_tickers = []

            for ipo in active_ipos:
                if not ipo.ticker or not ipo.trading_start:
                    continue

                # 25 gun tamamlanan IPO'lar icin tweet ATMA (sadece 1-24 arasi)
                if ipo.trading_day_count and ipo.trading_day_count >= 25:
                    logger.info(
                        "Tavan takip: %s — KAYIT TAMAMLANDI (gun=%d), gunluk tweet atlaniyor",
                        ipo.ticker, ipo.trading_day_count,
                    )
                    continue

                try:
                    # DB'den ceiling track verilerini oku (Excel sync ile doldurulmus)
                    track_result = await db.execute(
                        select(IPOCeilingTrack)
                        .where(IPOCeilingTrack.ipo_id == ipo.id)
                        .order_by(IPOCeilingTrack.trading_day.asc())
                        .limit(25)
                    )
                    tracks = track_result.scalars().all()

                    if not tracks:
                        logger.warning(
                            "Tavan takip: %s icin DB'de ceiling track verisi yok (Excel sync bekleniyor)",
                            ipo.ticker,
                        )
                        fail_count += 1
                        failed_tickers.append(ipo.ticker)
                        continue

                    # Track'leri days_data formatina donustur (tweet fonksiyonlari bu formati bekliyor)
                    days_data = []
                    for t in tracks:
                        days_data.append({
                            "trading_day": t.trading_day,
                            "date": t.trade_date,
                            "open": t.open_price if t.open_price is not None else t.close_price,
                            "high": t.high_price if t.high_price is not None else t.close_price,
                            "low": t.low_price if t.low_price is not None else t.close_price,
                            "close": t.close_price,
                            "volume": 0,
                            "durum": t.durum or "",
                        })

                    if not days_data:
                        logger.warning("Tavan takip: %s — days_data bos", ipo.ticker)
                        fail_count += 1
                        failed_tickers.append(ipo.ticker)
                        continue

                    # trading_day_count guncelle
                    ipo.trading_day_count = len(days_data)

                    if days_data and not ipo.first_day_close_price:
                        ipo.first_day_close_price = days_data[0]["close"]

                    success_count += 1
                    logger.info(
                        "Tavan takip: %s — DB'den %d gun okundu",
                        ipo.ticker, len(days_data),
                    )

                    # Tweet at — Gunluk Takip (tweet #8) ve 25 Gun Performans (tweet #9)
                    # Jitter: birden fazla IPO varsa tweetler arasi 50-55 sn bekle
                    try:
                        from app.services.twitter_service import tweet_daily_tracking
                        if days_data:
                            last_day = days_data[-1]
                            current_day = len(days_data)
                            last_close = float(last_day["close"])

                            # Gunluk % degisim hesapla (Decimal tipinde — detect_ceiling_floor Decimal bekler)
                            if len(days_data) > 1:
                                prev_c = Decimal(str(days_data[-2]["close"]))
                            else:
                                prev_c = Decimal(str(ipo.ipo_price)) if ipo.ipo_price else Decimal("0")
                            prev_c_f = float(prev_c)
                            daily_pct = (
                                ((last_close - prev_c_f) / prev_c_f) * 100
                                if prev_c_f > 0 else 0
                            )

                            last_det = detect_ceiling_floor(
                                close_price=Decimal(str(last_day["close"])),
                                prev_close=prev_c,
                                high_price=Decimal(str(last_day["high"])) if last_day.get("high") is not None else None,
                                low_price=Decimal(str(last_day["low"])) if last_day.get("low") is not None else None,
                            )

                            # Jitter — ilk IPO haric tweetler arasi 100-120 sn (2 dk) bekle
                            if success_count > 1:
                                jitter = random.uniform(100, 120)
                                logger.info("Tweet jitter: %.1f sn bekleniyor (%s)", jitter, ipo.ticker)
                                await asyncio.sleep(jitter)

                            # Tweet #8: Gunluk takip (1/25 — 24/25 arasi)
                            # 25. gun ve sonrasi icin gunluk tweet ATMA
                            # 25/25 performans tweeti gece 00:00 arsivleme sirasinda atilir
                            if current_day <= 24:
                                # Ceiling/floor sayisi hesapla (gorsel icin)
                                ceiling_d = sum(1 for t in tracks if t.hit_ceiling)
                                floor_d = sum(1 for t in tracks if t.hit_floor)

                                tweet_ok = tweet_daily_tracking(
                                    ipo, current_day, last_close,
                                    daily_pct, last_det["durum"],
                                    days_data=days_data,
                                    ceiling_days=ceiling_d,
                                    floor_days=floor_d,
                                )
                                # Admin Telegram bildirim
                                try:
                                    from app.services.admin_telegram import notify_tweet_sent
                                    await notify_tweet_sent(
                                        "gunluk_takip",
                                        ipo.ticker,
                                        tweet_ok,
                                        f"Gun: {current_day}/25 | %{daily_pct:+.2f} | {last_det['durum']}",
                                    )
                                except Exception:
                                    pass
                            else:
                                logger.info(
                                    "Tavan takip: %s — %d. gun, gunluk tweet atlaniyor (25/25 gece atilacak)",
                                    ipo.ticker, current_day,
                                )
                    except Exception as tweet_err:
                        logger.error("Tweet hatasi (sistemi etkilemez): %s", tweet_err)
                except Exception as ticker_err:
                    logger.error("Tavan takip %s hatasi: %s", ipo.ticker, ticker_err)
                    fail_count += 1
                    failed_tickers.append(ipo.ticker)

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
            today = _today_tr()

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
                    from app.services.admin_telegram import notify_tweet_sent
                    if _ft_tweet_idx > 0:
                        jitter = random.uniform(50, 55)
                        logger.info("Ilk islem tweet jitter: %.1f sn bekleniyor (%s)", jitter, ipo.ticker or ipo.company_name)
                        await asyncio.sleep(jitter)
                    tw_ok = tweet_first_trading_day(ipo)
                    _ft_tweet_idx += 1
                    await notify_tweet_sent("ilk_islem_gunu", ipo.ticker or ipo.company_name, tw_ok)
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
            today = _today_tr()
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
                            from app.services.admin_telegram import notify_tweet_sent
                            tw_ok = tweet_opening_price(ipo, open_price, pct_change)
                            _tweet_idx += 1
                            await notify_tweet_sent("acilis_fiyati", ipo.ticker or ipo.company_name, tw_ok, f"Açılış: {open_price}₺ ({pct_change:+.1f}%)")
                    except Exception as e:
                        logger.error("Acilis fiyati tweet hatasi %s: %s", ipo.ticker, e)
            finally:
                await scraper.close()

    except Exception as e:
        logger.error("Acilis fiyati tweet job hatasi: %s", e)


async def monthly_yearly_summary_tweet():
    """Ay sonu halka arz raporu — her ayin 1'i 00:00 TR (UTC 21:00 onceki gun).

    Ayin son gunu gece yarisi calisir.
    O yilin tum 25 gunu tamamlanan halka arzlarinin performans ozetini tweet atar.
    Ocak 1'de calisirsa onceki yilin verisini raporlar.
    """
    try:
        from sqlalchemy import select, and_, func
        from app.models.ipo import IPO, IPOCeilingTrack

        # TR saati gece yarisi = onceki ayin son gunu
        # UTC 21:00 = TR 00:00 (ertesi gun)
        # Rapor yili: Ocak 1 gece yarisi → onceki yil, diger aylar → bu yil
        now = datetime.now()
        # TR zamani = UTC + 3
        tr_now = now + timedelta(hours=3)

        if tr_now.month == 1 and tr_now.day == 1:
            # Ocak 1 gece yarisi → Aralik sonu → onceki yilin raporu
            report_year = tr_now.year - 1
        else:
            report_year = tr_now.year

        # Onceki ayin adi (TR gece yarisi = yeni ay, rapor onceki ay icin)
        prev_month = tr_now.month - 1 if tr_now.month > 1 else 12
        from app.services.twitter_service import _get_turkish_month
        month_name = _get_turkish_month(prev_month)

        async with async_session() as db:
            # Rapor yilinda isleme baslayan ve 25 gunu tamamlayan IPO'lar
            result = await db.execute(
                select(IPO).where(
                    and_(
                        IPO.trading_start.isnot(None),
                        IPO.trading_start >= date(report_year, 1, 1),
                        IPO.trading_start < date(report_year + 1, 1, 1),
                        IPO.trading_day_count >= 25,
                        IPO.ipo_price.isnot(None),
                        IPO.ipo_price > 0,
                    )
                )
            )
            completed_ipos = list(result.scalars().all())

            # Rapor yilinda toplam halka arz sayisi
            total_result = await db.execute(
                select(func.count(IPO.id)).where(
                    and_(
                        IPO.trading_start.isnot(None),
                        IPO.trading_start >= date(report_year, 1, 1),
                        IPO.trading_start < date(report_year + 1, 1, 1),
                    )
                )
            )
            total_ipos = total_result.scalar() or 0

            if not completed_ipos:
                logger.info("Ay sonu raporu: 25 gunu tamamlayan IPO yok (%d)", report_year)
                return

            # Her IPO icin 25. gun kapanis fiyati ve getiri hesapla
            returns = []
            for ipo in completed_ipos:
                track_result = await db.execute(
                    select(IPOCeilingTrack).where(
                        and_(
                            IPOCeilingTrack.ipo_id == ipo.id,
                            IPOCeilingTrack.trading_day == 25,
                        )
                    ).order_by(IPOCeilingTrack.id.desc()).limit(1)
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
            from app.services.admin_telegram import notify_tweet_sent
            tw_ok = tweet_yearly_summary(
                year=report_year,
                month_name=month_name,
                total_ipos=total_ipos,
                avg_return_pct=avg_return,
                best_ticker=best["ticker"],
                best_return_pct=best["pct"],
                worst_ticker=worst["ticker"],
                worst_return_pct=worst["pct"],
                total_completed=len(returns),
                positive_count=positive_count,
            )
            await notify_tweet_sent("aylik_rapor", f"{month_name} {report_year}", tw_ok, f"Toplam: {total_ipos} IPO, Ort: {avg_return:.1f}%")

    except Exception as e:
        logger.error("Ay sonu rapor tweet hatasi: %s", e)


async def update_bist50_index_job():
    """Her ayin 1'inde BIST 50 listesini infoyatirim.com'dan guncelle.

    Mevcut liste ile karsilastirir, degisiklik varsa DB'ye kaydeder
    ve admin Telegram'a bildirim gonderir.
    """
    try:
        from app.scrapers.bist_index_scraper import fetch_bist50_tickers
        from app.services.news_service import (
            get_bist50_tickers_sync, save_bist50_to_db, set_bist50_cache,
        )
        from app.services.admin_telegram import send_admin_message

        new_tickers = await fetch_bist50_tickers()
        old_tickers = get_bist50_tickers_sync()

        added = new_tickers - old_tickers
        removed = old_tickers - new_tickers

        if added or removed:
            async with async_session() as db:
                await save_bist50_to_db(db, new_tickers)

            # Admin Telegram bildirimi
            parts = [f"📊 <b>BIST 50 Guncellendi</b> ({len(new_tickers)} hisse)"]
            if added:
                parts.append(f"\n✅ Eklenen: {', '.join(sorted(added))}")
            if removed:
                parts.append(f"\n❌ Çıkan: {', '.join(sorted(removed))}")
            await send_admin_message("\n".join(parts))
            logger.info(
                "BIST50 guncellendi: +%d -%d (toplam %d)",
                len(added), len(removed), len(new_tickers),
            )
        else:
            logger.info("BIST50 degisiklik yok (%d hisse)", len(new_tickers))
            await send_admin_message(
                f"📊 BIST 50 kontrol edildi — değişiklik yok ({len(new_tickers)} hisse)"
            )

    except Exception as e:
        logger.error("BIST50 guncelleme hatasi: %s", e)
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error("bist50_index_update", str(e))
        except Exception:
            pass


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

    # 3a. SPK Bulten Monitor — YOGUN: her 1 dk (18:00-00:00 UTC = 21:00-03:00 TR)
    scheduler.add_job(
        check_spk_bulletins_job,
        CronTrigger(minute="*/1", hour="18-23"),
        id="spk_bulletin_monitor_peak",
        name="SPK Bulten Monitor (Yogun)",
        replace_existing=True,
    )
    # 3b. SPK Bulten Monitor — GECE: her 5 dk (00:00-05:00 UTC = 03:00-08:00 TR)
    scheduler.add_job(
        check_spk_bulletins_job,
        CronTrigger(minute="*/5", hour="0-4"),
        id="spk_bulletin_monitor_night",
        name="SPK Bulten Monitor (Gece)",
        replace_existing=True,
    )

    # 4. SPK Onay Listesi — 6 saatte bir (IPO'daki sirketler otomatik atlanir)
    scheduler.add_job(
        scrape_spk,
        IntervalTrigger(hours=6),
        id="spk_scraper",
        name="SPK Onay Scraper (6 saatte bir)",
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
        IntervalTrigger(seconds=5),
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

    # 7b. IPO Durum Guncelleme — gece yarisi 00:05 (subscription_start gunu aninda gecis)
    scheduler.add_job(
        auto_update_ipo_statuses,
        CronTrigger(hour=21, minute=5),  # UTC 21:05 = TR 00:05
        id="ipo_status_midnight",
        name="IPO Durum Gece Yarisi (Dagitim Gecis)",
        replace_existing=True,
    )

    # 7c. Sabah Tweet Zamanlama — her 5 dakika (acilis saatine gore)
    # Dagitim sabah tweeti + son gun sabah tweeti — IPO'nun acilis saatine 1 saat kala
    scheduler.add_job(
        check_morning_tweets,
        IntervalTrigger(minutes=5),
        id="morning_tweet_checker",
        name="Sabah Tweet Kontrol (acilisa 1h kala)",
        replace_existing=True,
    )

    # 8. 25 Is Gunu Arsiv + 25/25 Tweet — her gun 12:00 TR (UTC 09:00)
    scheduler.add_job(
        archive_old_ipos,
        CronTrigger(hour=9, minute=0),
        id="ipo_archiver",
        name="IPO Arsivleyici + 25 Gun Tweet (12:00 TR)",
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

    # 10b. HalkArz Trading Start Kontrol — her saat
    # awaiting_trading + trading_start bos IPO'lar icin saatlik kontrol
    scheduler.add_job(
        check_trading_start_halkarz,
        IntervalTrigger(hours=1),
        id="halkarz_trading_start_checker",
        name="HalkArz Islem Tarihi Kontrol (Saatlik)",
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

    # 11b. Scraper Boost Kontrol — her 15 dakikada boost suresi dolmus mu diye bak
    scheduler.add_job(
        _check_scraper_boost_expiry,
        IntervalTrigger(minutes=15),
        id="scraper_boost_checker",
        name="Scraper Boost Süre Kontrolü",
        replace_existing=True,
    )

    # 12. Son gun uyarisi — BUGUN son gun olanlara sabah 09:00 TR (UTC 06:00)
    # Eski: UTC 09:00 = TR 12:00 (gec) + UTC 17:00 = TR 20:00 (aksam oncesi gun)
    # Yeni: tek job, gercek son gunde sabah 09:00 TR'de bildirim
    scheduler.add_job(
        send_last_day_warnings,
        CronTrigger(hour=6, minute=0),  # UTC 06:00 = TR 09:00
        id="last_day_warning_morning",
        name="Son Gun Uyarisi (09:00 TR)",
        replace_existing=True,
    )

    # 13. Tavan Takip Gun Sonu — 18:07 TR (UTC 15:07) Pzt-Cuma
    scheduler.add_job(
        daily_ceiling_update,
        CronTrigger(hour=15, minute=7, day_of_week="mon-fri"),
        id="daily_ceiling_update",
        name="Tavan Takip Gun Sonu (18:07 TR)",
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

    # 17. Ay Sonu Raporu Tweet — her ayin 1'i 00:00 Turkiye (UTC 21:00 onceki gun)
    # Ayin son gunu gece yarisi = yeni ayin 1'i 00:00 TR
    scheduler.add_job(
        monthly_yearly_summary_tweet,
        CronTrigger(day=1, hour=21, minute=0),
        id="monthly_yearly_summary_tweet",
        name="Ay Sonu Halka Arz Raporu (Ayin 1'i 00:00 TR)",
        replace_existing=True,
    )

    # 18. SPK Onay Tanitim Tweeti — her saat kontrol (created_at + 13 saat sonra)
    # SPK onayi gece gelse bile 13 saat sonra tweet atar (duplicate korumali)
    scheduler.add_job(
        tweet_spk_approval_intro_job,
        IntervalTrigger(hours=1),
        id="spk_approval_intro_tweet",
        name="SPK Onay Tanitim Tweet (created_at + 13h)",
        replace_existing=True,
    )

    # 19. Son Gun Sabah Tweeti — artik check_morning_tweets() ile yonetiliyor
    # Eski sabit CronTrigger (05:00 TR) kaldirildi — acilis saatine gore dinamik
    # tweet_last_day_morning_job hala yedek olarak mevcut (fallback olarak kalabilir)

    # 20. Sirket Tanitim Tweeti — her gun 12:00 Turkiye (UTC 09:00)
    # Dun dagitima cikan IPO icin ogle vakti sirket tanitimi
    scheduler.add_job(
        tweet_company_intro_job,
        CronTrigger(hour=9, minute=0),
        id="company_intro_tweet",
        name="Sirket Tanitim Tweet (12:00 TR)",
        replace_existing=True,
    )

    # 21. SPK Bekleyenler Gorselli Tweet — her ayin 1'i 20:00 TR (UTC 17:00)
    scheduler.add_job(
        tweet_spk_pending_monthly_job,
        CronTrigger(day=1, hour=17, minute=0),
        id="spk_pending_monthly_tweet",
        name="SPK Bekleyenler Aylik Tweet (Ayin 1'i 20:00 TR)",
        replace_existing=True,
    )

    # 22. Ogle Arasi Market Snapshot — her gun 14:00 TR (UTC 11:00) Pzt-Cuma
    # Islemdeki tum halka arz hisselerinin anlik durumunu gorsel tweet atar
    # Borsa kapali ise (bugunun trade_date'i yoksa) tweet atilmaz
    scheduler.add_job(
        market_snapshot_tweet,
        CronTrigger(hour=11, minute=0, day_of_week="mon-fri"),
        id="market_snapshot_tweet",
        name="Ogle Arasi Market Snapshot (14:00 TR)",
        replace_existing=True,
    )

    # 22b. T16 Acilis Bilgileri — 09:57 TR (UTC 06:57) Pzt-Cuma
    # Ilk 5 islem gunundeki hisselerin acilis fiyatlarini gorsel tweet atar
    # Sync verisi gelmemisse 60sn bekleyip 2 kez daha dener
    scheduler.add_job(
        opening_summary_tweet,
        CronTrigger(hour=6, minute=57, day_of_week="mon-fri"),
        id="opening_summary_tweet",
        name="T16 Acilis Bilgileri (09:57 TR)",
        replace_existing=True,
    )

    # 23. Push Bildirim Saglik Raporu — 4 saatte bir
    scheduler.add_job(
        push_health_report_job,
        CronTrigger(hour="3,7,11,15,19,23", minute=0),
        id="push_health_report",
        name="Push Saglik Raporu (4 saatte bir)",
        replace_existing=True,
    )

    # 24. BIST 50 Endeks Guncelleme — her ayin 1'i 09:00 TR (UTC 06:00)
    scheduler.add_job(
        update_bist50_index_job,
        CronTrigger(day=1, hour=6, minute=0),
        id="bist50_index_update",
        name="BIST 50 Endeks Guncelleme (Ayin 1'i 09:00 TR)",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        "Scheduler baslatildi — %d gorev ayarlandi",
        len(scheduler.get_jobs()),
    )

    # Startup: Onceki boost suresi hala aktif mi kontrol et
    import asyncio
    asyncio.get_event_loop().create_task(_restore_boost_on_startup())


def shutdown_scheduler():
    """Scheduler'i durdurur."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler durduruldu")
