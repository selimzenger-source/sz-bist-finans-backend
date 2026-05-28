"""BIST Finans Backend — FastAPI Ana Uygulama.

Servisler:
1. Halka Arz Takip (ucretsiz)
2. Tavan/Taban Takip — hisse bazli ucretli paketler (5/10/15/20 gun)
3. Hisse Bazli Bildirim Aboneligi — 4 tip (tavan_bozulma/taban_acilma/gunluk_acilis_kapanis/yuzde_dusus)
   + Kombo (44 TL) + 3 Aylik (90 TL) + Yillik (245 TL)
4. Yapay Zeka Haber Takibi — Telegram kanal entegrasyonu (yildiz/ana_yildiz)
5. KAP Haber Bildirimleri
"""

import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import hmac

from fastapi import FastAPI, BackgroundTasks, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy import select, delete, update, desc, and_, or_, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database import get_db, init_db, async_session
from app.models import (
    IPO, IPOBroker, IPOAllocation, IPOCeilingTrack,
    SPKApplication,
    KapNews, User, UserSubscription, UserIPOAlert,
    CeilingTrackSubscription, CEILING_TIER_PRICES,
    TelegramNews, StockNotificationSubscription,
    NOTIFICATION_TIER_PRICES, NEWS_TIER_PRICES,
    COMBO_PRICE, QUARTERLY_PRICE,
    ANNUAL_BUNDLE_PRICE, COMBINED_ANNUAL_DISCOUNT_PCT,
    WalletTransaction, Coupon, WALLET_COUPONS,
    WALLET_REWARD_AMOUNT, WALLET_COOLDOWN_SECONDS, WALLET_MAX_DAILY_ADS,
    Dividend, DividendHistory,
    KapAllDisclosure, UserWatchlist,
    FeatureInterest,
    DailyStockMarketStat,
    PendingTweet,
    CompanyFinancial, FinancialRatio, IPOVote, AIAssistantUsage,
    EarningsCalendar,
)
from app.schemas import (
    IPOListOut, IPODetailOut, IPOSectionsOut,
    SPKApplicationOut, BlogPostOut,
    KapNewsOut, TelegramNewsOut,
    UserRegister, UserUpdate, UserOut, SubscriptionInfo,
    ReminderSettingsUpdate,
    IPOAlertCreate, CeilingTrackUpdate,
    CeilingTierOut, CeilingSubscriptionCreate, CeilingSubscriptionOut,
    NotificationTierOut, NewsTierOut,
    StockNotificationCreate, StockNotificationOut, StockNotificationSyncRequest,
    RealtimeNotifRequest,
    DividendOut,
    WalletBalanceOut, WalletEarnRequest, WalletSpendRequest,
    WalletCouponRequest, WalletTransactionOut,
    KapAllDisclosureOut, WatchlistItemOut, WatchlistAddRequest,
    CompanyFinancialOut, FinancialRatioOut, BilancoListItem, BilancoAnalysisOut,
    DividendHistoryOut, TemettuCalendarItem, TemettuDetailOut,
    EarningsCalendarOut,
    IPOVoteRequest, IPOVoteResultOut,
    AIAssistantChatRequest, AIAssistantChatResponse,
)
from app.scheduler import setup_scheduler, shutdown_scheduler
from app.admin.routes import router as admin_router

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

settings = get_settings()

import re as _re
_DEVICE_ID_PATTERN = _re.compile(r'^[a-zA-Z0-9_\-:.]{8,256}$')

def _validate_device_id_param(device_id: str) -> str:
    """Path parametresindeki device_id'yi dogrula. Gecersizse 400 dondur."""
    if not _DEVICE_ID_PATTERN.match(device_id):
        raise HTTPException(status_code=400, detail="Gecersiz device_id formati")
    return device_id


# -------------------------------------------------------
# Uygulama Yasam Dongusu
# -------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Uygulama baslangic ve kapanis islemleri."""
    logger.info("BIST Finans Backend baslatiliyor...")

    # async_session'i fonksiyon basinda import et — tum bloklar icin gerekli
    from app.database import async_session
    from sqlalchemy import text as sa_text

    # Veritabani tablolarini olustur
    try:
        await init_db()
    except Exception as e:
        logger.error("Veritabani init hatasi: %s", e)

    # User — notify_rehber kolonu ekle (migration)
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_rehber BOOLEAN DEFAULT TRUE"
            ))
            await db.commit()
    except Exception as e:
        logger.warning("notify_rehber migration atlandi: %s", e)

    # AI skor filtresi — kullanicinin push tercihi
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS kap_min_score FLOAT DEFAULT 6.0"
            ))
            # Bildirim tercihleri — market + seans + onboarding flag
            await db.execute(sa_text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_market_filter VARCHAR(16) DEFAULT 'all'"
            ))
            await db.execute(sa_text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_seans_filter VARCHAR(16) DEFAULT 'all'"
            ))
            await db.execute(sa_text(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS notif_onboarding_completed BOOLEAN DEFAULT FALSE"
            ))
            # stock_markets tablosu — KAP haber pazar filtreleme icin
            await db.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS stock_markets (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL UNIQUE,
                    company_name VARCHAR(255),
                    market_segment VARCHAR(32) NOT NULL DEFAULT 'diger',
                    indexes VARCHAR(500),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await db.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS idx_stock_market_segment ON stock_markets(market_segment)"
            ))
            await db.execute(sa_text(
                "CREATE INDEX IF NOT EXISTS idx_stock_market_ticker ON stock_markets(ticker)"
            ))
            await db.commit()
    except Exception as e:
        logger.warning("kap_min_score/notify_filter/stock_markets migration atlandi: %s", e)

    # BIST lisans uyumu: temel_analiz tablosundan piyasa kaynakli alanlari DROP et
    # (dolasim_lot, piyasa_degeri, fk — fiyat/piyasa kaynakli, lisans gerektiriyor)
    try:
        async with async_session() as db:
            for col in ("dolasim_lot", "piyasa_degeri", "fk"):
                await db.execute(sa_text(
                    f"ALTER TABLE temel_analiz DROP COLUMN IF EXISTS {col}"
                ))
            await db.commit()
    except Exception as e:
        logger.warning("temel_analiz lisans drop migration atlandi: %s", e)

    # BIST lisans uyumu: daily_stock_market_stats.close_price DROP
    # (gun sonu kapanis fiyati — BIST lisans gerektiriyor)
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "ALTER TABLE daily_stock_market_stats DROP COLUMN IF EXISTS close_price"
            ))
            await db.commit()
    except Exception as e:
        logger.warning("daily_stock_market_stats close_price drop atlandi: %s", e)

    # IPO poll bildirim takibi — 07:00 (hype) ve 17:00 (ceiling) ayri timestamp'ler
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "ALTER TABLE ipos ADD COLUMN IF NOT EXISTS hype_poll_notified_at TIMESTAMPTZ"
            ))
            await db.execute(sa_text(
                "ALTER TABLE ipos ADD COLUMN IF NOT EXISTS ceiling_poll_notified_at TIMESTAMPTZ"
            ))
            await db.execute(sa_text(
                "ALTER TABLE ipos ADD COLUMN IF NOT EXISTS hype_6h_notified_at TIMESTAMPTZ"
            ))
            await db.commit()
    except Exception as e:
        logger.warning("ipo poll notif migration atlandi: %s", e)

    # SPK Applications — company_description kolonu ekle (migration)
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "ALTER TABLE spk_applications ADD COLUMN IF NOT EXISTS company_description TEXT"
            ))
            await db.commit()
            logger.info("SPK: company_description kolonu kontrol edildi/eklendi")
    except Exception as e:
        logger.warning("SPK company_description migration atlandi: %s", e)

    # ★ KRITIK: dividend_calendar payment_type kolonlari (BORSK bug fix)
    # Bu kolonlar olmadiginda KAP haber routerı transaction'i abort ediyor
    # ve kap_all_disclosures kaydı KAYBOLUYOR. database.py'deki migration
    # bloğu çalışmazsa burada force eklenir.
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "ALTER TABLE dividend_calendar ADD COLUMN IF NOT EXISTS payment_type VARCHAR(20)"
            ))
            await db.execute(sa_text(
                "ALTER TABLE dividend_calendar ADD COLUMN IF NOT EXISTS stock_ratio_text VARCHAR(80)"
            ))
            await db.execute(sa_text(
                "ALTER TABLE dividend_calendar ADD COLUMN IF NOT EXISTS source_title VARCHAR(255)"
            ))
            await db.commit()
            logger.info("dividend_calendar payment_type/stock_ratio_text/source_title kolonlari kontrol edildi (force)")
    except Exception as e:
        logger.warning("dividend_calendar force migration atlandi: %s", e)

    # Kurum Onerileri — bildirim/tweet cift gonderim korumasi kolonlari
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "ALTER TABLE kurum_onerileri ADD COLUMN IF NOT EXISTS notification_sent_at TIMESTAMPTZ"
            ))
            await db.execute(sa_text(
                "ALTER TABLE kurum_onerileri ADD COLUMN IF NOT EXISTS tweet_sent_at TIMESTAMPTZ"
            ))
            # Mevcut tum kayitlari "gonderildi" olarak isaretle (tekrar bildirim/tweet onleme)
            await db.execute(sa_text(
                "UPDATE kurum_onerileri SET notification_sent_at = NOW(), tweet_sent_at = NOW() WHERE notification_sent_at IS NULL"
            ))
            await db.commit()
            logger.info("Kurum onerileri: kolonlar kontrol edildi + mevcut kayitlar sent olarak isaretlendi")

            # Bir seferlik: katilim_endeksi hepsini null yap (yanlis veri temizligi)
            await db.execute(sa_text("UPDATE ipos SET katilim_endeksi = NULL WHERE katilim_endeksi IS NOT NULL"))
            await db.commit()
            logger.info("IPO katilim_endeksi temizlendi (scraper tekrar dolduracak)")
    except Exception as e:
        logger.warning("Kurum onerileri migration atlandi: %s", e)

    # Blog Posts tablosu olustur (migration)
    try:
        async with async_session() as db:
            await db.execute(sa_text("""
                CREATE TABLE IF NOT EXISTS blog_posts (
                    id SERIAL PRIMARY KEY,
                    slug VARCHAR(255) UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    meta_description VARCHAR(300),
                    cover_image_url TEXT,
                    category VARCHAR(50) DEFAULT 'borsa_rehberi',
                    author_name VARCHAR(100) DEFAULT 'Borsa Cebimde',
                    is_published BOOLEAN DEFAULT FALSE,
                    ai_generated BOOLEAN DEFAULT TRUE,
                    published_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await db.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_blog_slug ON blog_posts(slug)"))
            await db.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_blog_published ON blog_posts(is_published)"))
            await db.execute(sa_text("CREATE INDEX IF NOT EXISTS idx_blog_category ON blog_posts(category)"))
            await db.commit()
            logger.info("Blog: blog_posts tablosu kontrol edildi/olusturuldu")
    except Exception as e:
        logger.warning("Blog tablosu migration atlandi: %s", e)

    # KAP bildirimleri — UNIQUE constraint + eski hatalı verileri temizle
    try:
        async with async_session() as db:
            # Eski seed verileri temizle (hatalı timestamp'li, deploy oncesi)
            # Bu migration sadece 1 kez calisir, sonra silinecek
            cleanup_res = await db.execute(sa_text(
                "DELETE FROM kap_all_disclosures WHERE id <= 243"
            ))
            if cleanup_res.rowcount > 0:
                logger.info("Eski KAP seed verileri temizlendi: %d kayit", cleanup_res.rowcount)
            await db.commit()

            # ❌ DEPRECATED: (company_code, title) uniq constraint bug'liyidi —
            # ayni sirketin ayni basliktaki farkli tarihli haberlerini siliyor/rediyordu
            # (ornek: "Yonetim Kurulu Karari", "Faaliyet Raporu" gibi periyodik basliklar).
            # Bu sebeple 2 ay once gelen haberler kayboldu, eski tarihler bos goruniyor.
            #
            # DOGRU index zaten var: idx_kap_all_dedup (company_code, title, published_at)
            # — database.py v40 migration'da olusturuluyor. Uzerine yanlis index'i siliyoruz.
            try:
                await db.execute(sa_text("DROP INDEX IF EXISTS uq_kap_company_title"))
                await db.commit()
                logger.info("KAP eski yanlis unique index (uq_kap_company_title) silindi")
            except Exception as e:
                logger.warning("uq_kap_company_title silinemedi: %s", e)
    except Exception as e:
        logger.warning("KAP unique constraint olusturulamadi: %s", e)

    # IPO modeline last_day_tweeted/last_day_notified kolonlarini ekle (migration olmadan)
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "ALTER TABLE ipos ADD COLUMN IF NOT EXISTS last_day_tweeted BOOLEAN DEFAULT FALSE"
            ))
            await db.execute(sa_text(
                "ALTER TABLE ipos ADD COLUMN IF NOT EXISTS last_day_notified BOOLEAN DEFAULT FALSE"
            ))
            # SADECE gecmis IPO'lar icin flag set et (bugunkuler HARIC — onlarin countdown'u henuz calismamis olabilir)
            await db.execute(sa_text(
                "UPDATE ipos SET last_day_tweeted = TRUE, last_day_notified = TRUE "
                "WHERE subscription_end < CURRENT_DATE AND subscription_end IS NOT NULL "
                "AND last_day_tweeted = FALSE"
            ))
            await db.commit()
            logger.info("IPO last_day_tweeted/last_day_notified kolonlari OK")
    except Exception as e:
        logger.warning("IPO last_day kolonlari eklenemedi (muhtemelen zaten var): %s", e)

    # Bildirim tercihi yeni kolonları ekle (notify_news, notify_viop, notify_tavan_taban, notify_spk_bulten, notify_edo_paid)
    try:
        async with async_session() as db:
            for col in ["notify_news", "notify_viop", "notify_tavan_taban", "notify_spk_bulten", "notify_edo_paid"]:
                await db.execute(sa_text(
                    f'ALTER TABLE users ADD COLUMN IF NOT EXISTS {col} BOOLEAN DEFAULT TRUE'
                ))
            await db.commit()
            logger.info("Bildirim tercihi yeni kolonlari OK (notify_news/viop/tavan_taban/spk_bulten/edo_paid)")
    except Exception as e:
        logger.warning("Bildirim tercihi kolonlari eklenemedi (muhtemelen zaten var): %s", e)

    # E.D.O: trading_start backfill — NULL olan trading IPO'lar icin ilk track tarihinden set et
    try:
        async with async_session() as db:
            await db.execute(sa_text(
                "UPDATE ipos SET trading_start = sub.first_date "
                "FROM (SELECT ipo_id, MIN(trade_date) as first_date "
                "      FROM ipo_ceiling_tracks GROUP BY ipo_id) sub "
                "WHERE ipos.id = sub.ipo_id "
                "AND ipos.trading_start IS NULL AND ipos.status = 'trading'"
            ))
            await db.commit()
            logger.info("E.D.O: trading_start backfill OK")
    except Exception as e:
        logger.warning("E.D.O trading_start backfill hatasi: %s", e)

    # E.D.O temizligi: KESIN eski IPO'larin EDO verisini sifirla
    # DIKKAT: trading_start IS NULL olanlara DOKUNMA (yeni/backfill eksik olabilir)
    # EDO_START_DATE: tek merkezi sabit (app/config.py)
    try:
        from app.config import EDO_START_DATE
        _edo_cutoff = str(EDO_START_DATE)  # '2026-03-10'
        async with async_session() as db:
            await db.execute(sa_text(
                "UPDATE ipos SET senet_sayisi = NULL, cumulative_volume = NULL "
                f"WHERE trading_start IS NOT NULL AND trading_start < '{_edo_cutoff}' "
                "AND senet_sayisi IS NOT NULL"
            ))
            await db.execute(sa_text(
                "UPDATE ipo_ceiling_tracks SET gunluk_adet = NULL, senet_sayisi = NULL, "
                "cumulative_edo_pct = NULL "
                "WHERE ipo_id IN ("
                "  SELECT id FROM ipos "
                f"  WHERE trading_start IS NOT NULL AND trading_start < '{_edo_cutoff}'"
                ")"
            ))
            await db.commit()
            logger.info("E.D.O temizligi OK (trading_start < %s)", _edo_cutoff)
    except Exception as e:
        logger.warning("E.D.O temizligi hatasi: %s", e)

    # E.D.O: Gecmis veri kurtarma — startup sirasinda kaybolan gunluk_adet degerlerini geri yukle
    # Sadece gunluk_adet NULL/0 ise set eder (tekrar tekrar calissa bile guvenli — idempotent)
    # Her IPO icin {trading_day: gunluk_adet} eslesmesi
    _EDO_RESTORE = {
        "MCARD": {1: 22678},                  # 1. gun %0.12 (senet ~18.9M)
        "LXGYO": {1: 59996, 2: 227983, 3: 2003852},  # 1.gun %0.05, 2.gun %0.19, 3.gun %1.67 (senet ~120M)
    }
    for _ticker, _day_map in _EDO_RESTORE.items():
        try:
            async with async_session() as db:
                any_fixed = False
                for _day, _adet in _day_map.items():
                    fix_res = await db.execute(sa_text(
                        f"UPDATE ipo_ceiling_tracks SET gunluk_adet = {_adet} "
                        f"WHERE ipo_id = (SELECT id FROM ipos WHERE ticker = '{_ticker}' LIMIT 1) "
                        f"AND trading_day = {_day} "
                        "AND (gunluk_adet IS NULL OR gunluk_adet = 0)"
                    ))
                    if fix_res.rowcount > 0:
                        any_fixed = True

                if any_fixed:
                    # Kumulatif EDO'yu yeniden hesapla (running total)
                    ipo_res = await db.execute(sa_text(
                        f"SELECT id, senet_sayisi FROM ipos WHERE ticker = '{_ticker}' LIMIT 1"
                    ))
                    ipo_row = ipo_res.fetchone()
                    if ipo_row and ipo_row[1] and ipo_row[1] > 0:
                        _ipo_id, _senet = ipo_row[0], ipo_row[1]
                        tracks_res = await db.execute(sa_text(
                            "SELECT id, trading_day, gunluk_adet FROM ipo_ceiling_tracks "
                            "WHERE ipo_id = :ipo_id ORDER BY trading_day"
                        ), {"ipo_id": _ipo_id})
                        running = 0
                        for tr in tracks_res.fetchall():
                            running += (tr[2] or 0)
                            pct = round(running / _senet * 100, 2)
                            await db.execute(sa_text(
                                "UPDATE ipo_ceiling_tracks SET cumulative_edo_pct = :pct "
                                "WHERE id = :tid"
                            ), {"pct": pct, "tid": tr[0]})
                        await db.execute(sa_text(
                            "UPDATE ipos SET cumulative_volume = :vol WHERE id = :iid"
                        ), {"vol": running, "iid": _ipo_id})
                    await db.commit()
                    logger.info("E.D.O: %s gecmis veri geri yuklendi (%s)", _ticker, _day_map)
        except Exception as e:
            logger.warning("E.D.O %s fix hatasi: %s", _ticker, e)

    # BIST50 cache'ini DB'den yukle
    try:
        from app.services.news_service import load_bist50_from_db
        async with async_session() as db:
            await load_bist50_from_db(db)
    except Exception as e:
        logger.warning("BIST50 cache yukleme hatasi (fallback kullanilacak): %s", e)

    # Arşivlenmiş ama ceiling_tracking_active=True kalan IPO'ları düzelt
    try:
        from sqlalchemy import select, and_, delete
        from app.models.ipo import IPO, IPOCeilingTrack
        async with async_session() as db:
            stale_res = await db.execute(
                select(IPO).where(
                    and_(IPO.archived == True, IPO.ceiling_tracking_active == True)
                )
            )
            stale_list = list(stale_res.scalars().all())
            if stale_list:
                for s in stale_list:
                    s.ceiling_tracking_active = False
                await db.commit()
                logger.info(
                    "Startup düzeltici: %d IPO ceiling_tracking_active=False yapıldı: %s",
                    len(stale_list),
                    [s.ticker for s in stale_list],
                )
    except Exception as e:
        logger.warning("Startup ceiling düzeltici hatası: %s", e)

    # StockNotificationSubscription — NULL muted/is_active düzeltici
    # Sonradan eklenen muted kolonu eski kayıtlarda NULL olabilir → bildirim sorgusunu bozar
    try:
        from sqlalchemy import update
        from app.models.user import StockNotificationSubscription as _SNS
        async with async_session() as db:
            # muted=NULL → muted=False
            r1 = await db.execute(
                update(_SNS).where(_SNS.muted.is_(None)).values(muted=False)
            )
            # muted_types=NULL zaten OK, dokunma
            if r1.rowcount > 0:
                await db.commit()
                logger.info(
                    "Startup düzeltici: %d StockNotificationSubscription muted=NULL → False yapıldı",
                    r1.rowcount,
                )
    except Exception as e:
        logger.warning("Startup muted düzeltici hatası: %s", e)

    # Eski puan abonelikleri temizleyici — expires_at=NULL + 35+ gün geçmiş wallet subs
    try:
        from sqlalchemy import and_, update
        from app.models.user import UserSubscription
        _stale_cutoff = datetime.now(timezone.utc) - timedelta(days=35)
        async with async_session() as db:
            stale_result = await db.execute(
                update(UserSubscription)
                .where(
                    and_(
                        UserSubscription.is_active == True,
                        UserSubscription.store == "wallet",
                        UserSubscription.expires_at.is_(None),
                        UserSubscription.started_at.isnot(None),
                        UserSubscription.started_at < _stale_cutoff,
                    )
                )
                .values(is_active=False)
            )
            if stale_result.rowcount > 0:
                await db.commit()
                logger.warning(
                    "Startup: %d eski puan aboneliği deaktif edildi (expires_at=NULL, 35+ gün)",
                    stale_result.rowcount,
                )
    except Exception as e:
        logger.warning("Startup eski puan temizleyici hatası: %s", e)

    # 26+ gün verisi temizleyici — hiçbir IPO'da trading_day > 25 olmamalı
    try:
        from sqlalchemy import delete
        from app.models.ipo import IPOCeilingTrack
        async with async_session() as db:
            del_result = await db.execute(
                delete(IPOCeilingTrack).where(IPOCeilingTrack.trading_day > 25)
            )
            if del_result.rowcount > 0:
                await db.commit()
                logger.warning(
                    "Startup 26+ gün temizleyici: %d adet trading_day>25 kayıt silindi!",
                    del_result.rowcount,
                )
            else:
                logger.info("Startup 26+ gün temizleyici: temiz, silinecek kayıt yok.")
    except Exception as e:
        logger.warning("Startup 26+ gün temizleyici hatası: %s", e)

    # Scheduler'i baslat
    try:
        setup_scheduler()
    except Exception as e:
        logger.error("Scheduler baslatilamadi: %s", e)

    # ── Startup: Tavan/taban tweet — scheduler'a bırakılıyor ──
    # Startup'ta ağır AI analiz çalıştırmak Render health check'i bozuyor.
    # Bunun yerine sadece scheduler (18:50 TR) ve admin trigger endpoint kullanılır.
    logger.info("Startup: Tavan/taban tweet kontrolü scheduler'a bırakıldı (startup'ta çalıştırılmayacak).")

    yield

    # Kapanis
    try:
        shutdown_scheduler()
    except Exception:
        pass
    logger.info("BIST Finans Backend kapatildi.")


# -------------------------------------------------------
# FastAPI Uygulamasi
# -------------------------------------------------------

import time as _time_startup
_APP_START_TIME = _time_startup.time()

app = FastAPI(
    title="BIST Finans API",
    description="Halka Arz Takip + Hisse Bildirim + AI Haber Takibi",
    version="2.0.0",
    lifespan=lifespan,
    # Production'da API dokumantasyonunu kapat
    docs_url=None if settings.is_production else "/docs",
    redoc_url=None if settings.is_production else "/redoc",
    openapi_url=None if settings.is_production else "/openapi.json",
)

# Rate Limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Global Exception Handler — production'da stack trace sizintisini engelle
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Beklenmedik hatalarda stack trace yerine genel hata mesaji don."""
    logger.error("Beklenmedik hata [%s %s]: %s", request.method, request.url.path, exc, exc_info=True)

    # Telegram admin bildirimi — 1 saat dedup ile (spam koruma servis icinde)
    try:
        from app.services.admin_telegram import notify_backend_error
        # device_id query/header'dan cikar
        _did = None
        try:
            _did = request.query_params.get("device_id") or request.headers.get("X-Device-Id")
        except Exception:
            pass
        await notify_backend_error(
            method=request.method,
            path=str(request.url.path),
            error_type=type(exc).__name__,
            error_message=str(exc),
            user_device_id=_did,
        )
    except Exception:
        pass  # Bildirim hatasi response'u bozmasin

    from fastapi.responses import JSONResponse
    if settings.is_production:
        return JSONResponse(
            status_code=500,
            content={"detail": "Sunucu hatasi. Lutfen daha sonra tekrar deneyin."},
        )
    # Development'ta detayli hata goster
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )


# CORS — production'da spesifik origin, gelistirmede *
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With", "X-App-Key", "X-Platform"],
)


# Guvenlik Header Middleware
@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if settings.is_production:
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# Static dosyalar (admin panel logo vb.)
import os
_static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# Admin Panel
app.include_router(admin_router)


# -------------------------------------------------------
# Public Endpoints — Herkese acik veri endpoint'leri
# -------------------------------------------------------


@app.get("/api/v1/public/news-feed")
@limiter.limit("30/minute")
async def get_public_news_feed(
    request: Request,
    db: AsyncSession = Depends(get_db),
    days: int = Query(15, le=60),
    limit: int = Query(100, le=200),
    source: str = Query(None, description="Kaynak filtresi: news_scanner, kap_news, tweet_kap_news vb."),
):
    """Son N gunun haber tweetlerini blog formatta doner (sadece news sources)."""
    import re
    # Sadece haber kaynaklari — VİOP, tavan/taban, EDO, snapshot, IPO haric
    ALL_NEWS_SOURCES = {"news_scanner", "tweet_bist30_news", "tweet_kap_news", "kap_news", "tweet_spk_application", "tweet_spk_bulletin", "tweet_spk_bulletin_analysis"}
    # source parametresi verilmişse sadece o kaynağı filtrele
    if source and source in ALL_NEWS_SOURCES:
        NEWS_SOURCES = {source}
    else:
        NEWS_SOURCES = ALL_NEWS_SOURCES
    # bot_proxy kaynaginda VİOP dahil, tavan/taban haric
    _TAVAN_KEYWORDS = ["%tavan yap%", "%taban yap%", "%Günün Tavan%", "%Günün Taban%", "%EDO eşiğ%"]
    _JUNK_KEYWORDS = [
        "%Kapanış Raporu%", "%kapanış raporu%", "%Piyasa Kapanış%",
        "%Günlük Takip%", "%günlük takip%",
        "%Dağıtım Sonuç%", "%dağıtım sonuç%",
        "%Açılış Fiyat%", "%açılış fiyat%",
        "%25 Gün Performans%", "%25 gün performans%",
        "%Piyasa Özet%", "%piyasa özet%",
        "%Halka arz takip%", "%İşlem Görmeye Başla%",
    ]
    cutoff = datetime.utcnow() - timedelta(days=days)

    # 1) Bilinen haber source'lari
    stmt1 = (
        select(PendingTweet)
        .where(
            PendingTweet.status == "sent",
            PendingTweet.sent_at >= cutoff,
            PendingTweet.source.in_(NEWS_SOURCES),
        )
    )
    # 2) bot_proxy'den sadece haber olanlar (VİOP + tavan/taban haric)
    _VIOP_KEYWORDS = ["%VİOP%", "%VIOP%", "%X30YVADE%"]
    bot_proxy_filter = [
        PendingTweet.status == "sent",
        PendingTweet.sent_at >= cutoff,
        PendingTweet.source == "bot_proxy",
    ]
    for kw in _VIOP_KEYWORDS + _TAVAN_KEYWORDS + _JUNK_KEYWORDS:
        bot_proxy_filter.append(~PendingTweet.text.ilike(kw))

    stmt2 = select(PendingTweet).where(*bot_proxy_filter)

    from sqlalchemy import union_all
    combined = union_all(stmt1, stmt2).subquery()
    stmt = (
        select(PendingTweet)
        .join(combined, PendingTweet.id == combined.c.id)
        .order_by(desc(PendingTweet.sent_at))
        .limit(limit)
    )
    result = await db.execute(stmt)
    tweets = result.scalars().all()

    def clean_text(text: str) -> str:
        # Sadece platform hashtag'leri sil — ticker hashtag'leri (#CEMZY, #TRILC)
        # SPK bülten ve haber metinlerinde hisseyi tanımladığı için KORUNMALI.
        text = re.sub(
            r'#(SPK|BIST100|BIST|borsa|BultenAnaliz|HalkaArz|Borsa|BorsaCebimde|SzAlgo|szalgo)\b',
            '', text, flags=re.IGNORECASE,
        )
        # Remove URLs (store links, t.co links, etc.)
        text = re.sub(r'https?://\S+', '', text)
        # Remove Android/iOS/Web store references
        text = re.sub(r'📲?\s*Android:?\s*', '', text)
        text = re.sub(r'🍏?\s*iOS:?\s*', '', text)
        text = re.sub(r'🌐?\s*Web:?\s*', '', text)
        # Remove multiple spaces/newlines
        text = re.sub(r'\n{3,}', '\n\n', text.strip())
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def _image_url(path: str | None) -> str | None:
        if not path:
            return None
        if path.startswith("http"):
            return path
        # Local /static/ path'leri de dondur
        if path.startswith("/static/"):
            return path
        return None

    return [
        {
            "id": t.id,
            "text": clean_text(t.text),
            "image_url": _image_url(t.image_path),
            "source": t.source,
            "sent_at": t.sent_at.isoformat() if t.sent_at else None,
            "created_at": t.created_at.isoformat() if t.created_at else None,
        }
        for t in tweets
    ]


@app.get("/api/v1/public/daily-market-stats")
@limiter.limit("30/minute")
async def get_public_daily_market_stats(
    request: Request,
    db: AsyncSession = Depends(get_db),
    days: int = Query(30, le=90),
):
    """Son N gunun tavan/taban istatistiklerini tarih bazli doner."""
    from datetime import date as date_type
    cutoff = date_type.today() - timedelta(days=days)
    stmt = (
        select(DailyStockMarketStat)
        .where(
            DailyStockMarketStat.date >= cutoff,
            or_(
                DailyStockMarketStat.is_ceiling == True,
                DailyStockMarketStat.is_floor == True,
            ),
        )
        .order_by(desc(DailyStockMarketStat.date), desc(DailyStockMarketStat.percent_change))
    )
    result = await db.execute(stmt)
    stats = result.scalars().all()

    return [
        {
            "id": s.id,
            "ticker": s.ticker,
            "date": s.date.isoformat(),
            # close_price KALDIRILDI (BIST lisans), percent_change response'a dahil edilmez
            "is_ceiling": s.is_ceiling,
            "is_floor": s.is_floor,
            "consecutive_ceiling_count": s.consecutive_ceiling_count,
            "monthly_ceiling_count": s.monthly_ceiling_count,
            "consecutive_floor_count": s.consecutive_floor_count,
            "monthly_floor_count": s.monthly_floor_count,
            "reason": s.reason,
        }
        for s in stats
    ]


# ─── BIST hisse fiyat proxy (halka-arz-defteri icin) ───────────
# Frontend eskiden Yahoo'yu dogrudan cekiyordu; Yahoo v7 kapatildi, v8 calisiyor.
# Yeni servisler degisirse app build gerekmesin diye proxy edelim.
_STOCK_PRICE_CACHE: dict = {}  # ticker -> {"price": float, "source": str, "at": float}
_STOCK_PRICE_TTL = 5 * 60  # 5 dk


async def _fetch_yahoo_v8(ticker: str) -> float | None:
    """DEVRE DISI — Borsa Istanbul veri lisansi gerekliligi.
    Lisansli vendor entegre edilince geri acilacak.
    """
    return None


async def _fetch_mynet(ticker: str) -> float | None:
    """DEVRE DISI — Borsa Istanbul veri lisansi gerekliligi."""
    return None


@app.get("/api/v1/public/stock-price/{ticker}")
@limiter.limit("120/minute")
async def get_stock_price(request: Request, ticker: str):
    """BIST hisse fiyat — Borsa Istanbul veri lisansi gerekliligi nedeniyle
    Yahoo/Mynet uzerinden cekim DEVRE DISI. Lisansli vendor entegre edilince
    geri acilacak.

    Frontend bu endpoint'i hala cagirabilir; her zaman price=None doner.
    """
    tk = ticker.upper().strip()
    return {"ticker": tk, "price": None, "source": "disabled_license", "cached": False}


@app.get("/api/v1/public/stock-prices")
@limiter.limit("60/minute")
async def get_stock_prices(request: Request, tickers: str = Query(..., description="virgulle ayrilmis ticker listesi")):
    """Toplu fiyat — lisans nedeniyle devre disi; her ticker icin None doner."""
    ticker_list = [t.upper().strip() for t in tickers.split(",") if t.strip() and t.strip().isalnum()][:50]
    results: dict[str, dict] = {
        tk: {"price": None, "source": "disabled_license", "cached": False}
        for tk in ticker_list
    }
    return {"prices": results}


@app.get("/api/v1/public/viop-night-session")
@limiter.limit("30/minute")
async def get_public_viop_night_session(
    request: Request,
    db: AsyncSession = Depends(get_db),
    days: int = Query(5, le=30),
    limit: int = Query(100, le=200),
):
    """VIOP tweetlerini son N gun icin doner (pending_tweets tablosundan)."""
    import re
    cutoff = datetime.utcnow() - timedelta(days=days)
    stmt = (
        select(PendingTweet)
        .where(
            PendingTweet.status == "sent",
            PendingTweet.sent_at >= cutoff,
            PendingTweet.text.ilike("%VİOP%"),
        )
        .order_by(desc(PendingTweet.sent_at))
        .limit(limit)
    )
    result = await db.execute(stmt)
    tweets = result.scalars().all()

    def clean_text(text: str) -> str:
        # Ticker hashtag'leri korunur, sadece platform hashtag'leri silinir
        text = re.sub(
            r'#(SPK|BIST100|BIST|borsa|BultenAnaliz|HalkaArz|Borsa|BorsaCebimde|SzAlgo|szalgo)\b',
            '', text, flags=re.IGNORECASE,
        )
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text.strip())
        return text.strip()

    return [
        {
            "id": t.id,
            "text": clean_text(t.text),
            "image_url": f"/static/img/{t.image_path.split('/')[-1]}" if t.image_path else None,
            "source": t.source,
            "sent_at": t.sent_at.isoformat() if t.sent_at else None,
        }
        for t in tweets
    ]


@app.get("/api/v1/public/spk-bulletin-analyses")
@limiter.limit("30/minute")
async def get_spk_bulletin_analyses(
    request: Request,
    db: AsyncSession = Depends(get_db),
    limit: int = Query(20, le=50),
):
    """SPK bulten analizlerini doner — bulten numarasina gore gruplu."""
    import re
    stmt = (
        select(PendingTweet)
        .where(
            PendingTweet.status == "sent",
            PendingTweet.source.in_(["tweet_spk_bulletin_analysis", "tweet_spk_pending_visual", "tweet_spk_application"]),
        )
        .order_by(desc(PendingTweet.sent_at))
        .limit(limit)
    )
    result = await db.execute(stmt)
    tweets = result.scalars().all()

    def clean_text(text: str) -> str:
        # ÖNEMLİ: Ticker hashtag'leri (#CEMZY, #TRILC vb.) KORUNMALI —
        # sermaye artırımı/karar satırlarında hissenin kim olduğunu gösterir.
        # Sadece platform hashtag'lerini sil.
        text = re.sub(
            r'#(SPK|BIST100|BIST|borsa|BultenAnaliz|HalkaArz|Borsa|BorsaCebimde|SzAlgo|szalgo)\b',
            '', text, flags=re.IGNORECASE,
        )
        text = re.sub(r'https?://\S+', '', text)
        text = re.sub(r'📲?\s*(Detaylar\s*görselde|Android|szalgo)[^\n]*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'🍏?\s*iOS:?[^\n]*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'🌐?\s*Web:?[^\n]*', '', text, flags=re.IGNORECASE)
        text = re.sub(r'szalgo\.net\.tr', '', text, flags=re.IGNORECASE)
        text = re.sub(r'👇', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text.strip())
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    # Bulten numarasini tweet metninden cikarmaya calis
    def extract_bulletin_no(text: str) -> str | None:
        # "2026/5", "Bülten No: 2026/5", "SPK 2026/5" gibi kaliplari ara
        match = re.search(r'(\d{4})/(\d{1,3})', text)
        if match:
            return f"{match.group(1)}/{match.group(2)}"
        return None

    return [
        {
            "id": t.id,
            "text": clean_text(t.text),
            "image_url": t.image_path if t.image_path and t.image_path.startswith("http") else (f"/static/img/{t.image_path.split('/')[-1]}" if t.image_path else None),
            "source": t.source,
            "bulletin_no": extract_bulletin_no(t.text),
            "sent_at": t.sent_at.isoformat() if t.sent_at else None,
        }
        for t in tweets
    ]


# -------------------------------------------------------
# Admin sifre dogrulama — timing-safe + brute-force korumasi
# -------------------------------------------------------

_admin_fail_counts: dict[str, int] = {}      # IP → yanlis giris sayisi
_admin_block_until: dict[str, float] = {}    # IP → engel bitis zamani (epoch)
_ADMIN_MAX_ATTEMPTS = 5                       # Max yanlis giris
_ADMIN_BLOCK_SECONDS = 3600                   # 1 saat engel

def _verify_admin_password(provided: str) -> bool:
    """Timing-safe admin sifre dogrulama. Brute force'a karsi hmac kullanir."""
    admin_pw = settings.ADMIN_PASSWORD
    if not admin_pw or not provided:
        return False
    return hmac.compare_digest(provided.encode("utf-8"), admin_pw.encode("utf-8"))

def _get_client_ip(request: Request) -> str:
    """Client IP adresini al (proxy arkasinda X-Forwarded-For)."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


# -------------------------------------------------------
# Admin Sifre Dogrulama Endpoint
# -------------------------------------------------------

@app.post("/api/v1/admin/verify")
@limiter.limit("10/minute")
async def verify_admin_password(request: Request, payload: dict = Body(...)):
    """Admin sifresini dogrular — brute-force korumasi + Telegram bildirimi."""
    import time as _time
    from app.services.admin_telegram import send_admin_message

    client_ip = _get_client_ip(request)

    # Engel kontrolu
    block_time = _admin_block_until.get(client_ip, 0)
    if _time.time() < block_time:
        remaining_min = int((block_time - _time.time()) / 60)
        raise HTTPException(
            status_code=429,
            detail=f"Cok fazla yanlis giris. {remaining_min} dakika sonra tekrar deneyin.",
        )

    if not _verify_admin_password(payload.get("admin_password", "")):
        # Yanlis giris sayacini artir
        _admin_fail_counts[client_ip] = _admin_fail_counts.get(client_ip, 0) + 1
        attempts = _admin_fail_counts[client_ip]

        if attempts >= _ADMIN_MAX_ATTEMPTS:
            # 1 saat engelle
            _admin_block_until[client_ip] = _time.time() + _ADMIN_BLOCK_SECONDS
            _admin_fail_counts[client_ip] = 0

            # Telegram bildirimi gonder
            try:
                await send_admin_message(
                    f"🚨 <b>Admin Brute-Force Tespit!</b>\n"
                    f"IP: <code>{client_ip}</code>\n"
                    f"Yanlis giris: {_ADMIN_MAX_ATTEMPTS} kez\n"
                    f"Engel suresi: 1 saat\n"
                    f"⚠️ Sifre kirma denemesi olabilir!",
                )
            except Exception:
                pass  # Telegram hatasi login akisini bozmasin

            raise HTTPException(
                status_code=429,
                detail="Cok fazla yanlis giris. 1 saat engellendi.",
            )

        raise HTTPException(status_code=401, detail="Gecersiz admin sifresi")

    # Basarili giris — sayaci sifirla
    _admin_fail_counts.pop(client_ip, None)
    _admin_block_until.pop(client_ip, None)
    return {"status": "ok", "message": "Admin dogrulandi"}


# -------------------------------------------------------
# Service Status (Kill Switch) — Uygulama Admin
# -------------------------------------------------------

@app.post("/api/v1/admin/service-status")
@limiter.limit("30/minute")
async def get_service_status(request: Request, payload: dict = Body(...)):
    """Servis durumunu oku — bildirim/tweet kill switch + auto_send."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=401, detail="Gecersiz admin sifresi")

    from app.services.twitter_service import is_notifications_killed, is_tweets_killed, is_auto_send
    return {
        "notifications_killed": is_notifications_killed(),
        "tweets_killed": is_tweets_killed(),
        "auto_send": is_auto_send(),
    }


@app.post("/api/v1/admin/toggle-kill-switch")
@limiter.limit("10/minute")
async def toggle_kill_switch(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Kill switch toggle — uygulama admin'inden kullanılır.

    payload: { admin_password, switch_type: "notifications" | "tweets" }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=401, detail="Gecersiz admin sifresi")

    switch_type = payload.get("switch_type", "")
    if switch_type not in ("notifications", "tweets"):
        raise HTTPException(status_code=400, detail="Gecersiz switch_type. 'notifications' veya 'tweets' olmali.")

    from app.models.app_setting import AppSetting
    from app.services.twitter_service import (
        clear_settings_cache, is_notifications_killed, is_tweets_killed,
    )

    key = "NOTIFICATIONS_KILL_SWITCH" if switch_type == "notifications" else "TWEETS_KILL_SWITCH"
    current = is_notifications_killed() if switch_type == "notifications" else is_tweets_killed()
    new_val = "false" if current else "true"

    result = await db.execute(select(AppSetting).where(AppSetting.key == key))
    setting = result.scalar_one_or_none()
    if setting:
        setting.value = new_val
    else:
        db.add(AppSetting(key=key, value=new_val))
    await db.commit()
    clear_settings_cache()

    label = "Bildirimler" if switch_type == "notifications" else "Tweetler"
    status_text = "DURDURULDU" if new_val == "true" else "AKTİF"
    logger.info("[APP-ADMIN] %s -> %s", key, new_val)

    # Telegram admin bildirimi
    try:
        from app.services.admin_telegram import send_admin_message
        await send_admin_message(
            f"{'🔴' if new_val == 'true' else '🟢'} {label} {status_text} — uygulama admin'inden degistirildi"
        )
    except Exception:
        pass

    return {
        "status": "ok",
        "switch_type": switch_type,
        "killed": new_val == "true",
        "message": f"{label} {status_text}",
    }


# -------------------------------------------------------
# Gemini Test — AI Debug
# -------------------------------------------------------

@app.post("/api/v1/admin/test-gemini")
@limiter.limit("5/minute")
async def test_gemini(request: Request, payload: dict = Body(...)):
    """Gemini API bağlantı testi — KAP analyzer ile aynı ayarları kullanır."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=401, detail="Gecersiz admin sifresi")

    settings = get_settings()
    gemini_key = settings.GEMINI_API_KEY if settings.GEMINI_API_KEY else None

    if not gemini_key:
        return {"status": "error", "message": "GEMINI_API_KEY env variable yok veya bos", "key_exists": False}

    import httpx
    url = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
    test_payload = {
        "model": "gemini-2.5-flash",
        "messages": [{"role": "user", "content": "Say 'test ok' in JSON: {\"result\": \"test ok\"}"}],
        "max_tokens": 2048,  # Gemini 2.5 thinking tokens eat into max_tokens
        "temperature": 0,
    }

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                url,
                headers={"Authorization": f"Bearer {gemini_key}", "Content-Type": "application/json"},
                json=test_payload,
            )
            if resp.status_code == 200:
                data = resp.json()
                try:
                    ai_text = data["choices"][0]["message"]["content"].strip()
                except (KeyError, IndexError, TypeError) as parse_err:
                    return {
                        "status": "error",
                        "key_exists": True,
                        "key_prefix": gemini_key[:4] + "***",
                        "parse_error": str(parse_err),
                        "raw_response": str(data)[:500],
                    }
                return {
                    "status": "ok",
                    "key_exists": True,
                    "key_prefix": gemini_key[:4] + "***",
                    "model": "gemini-2.5-flash",
                    "response": ai_text[:200],
                }
            else:
                return {
                    "status": "error",
                    "key_exists": True,
                    "key_prefix": gemini_key[:4] + "***",
                    "http_status": resp.status_code,
                    "error": resp.text[:300],
                }
    except Exception as e:
        return {
            "status": "error",
            "key_exists": True,
            "key_prefix": gemini_key[:4] + "***",
            "exception": str(e),
        }


# -------------------------------------------------------
# Health Check
# -------------------------------------------------------

@app.get("/health")
async def health_check():
    resp = {
        "status": "ok",
        "service": "bist-finans-backend",
        "version": "2.0.0",
        "deploy_marker": "type-conv-route-2026-05-12",
    }
    # Dahili sistem bilgilerini sadece development'ta goster
    if not settings.is_production:
        from app.services.notification import is_firebase_initialized
        resp["firebase_initialized"] = is_firebase_initialized()
        resp["telegram_configured"] = bool(settings.TELEGRAM_BOT_TOKEN)
        resp["telegram_reader_configured"] = bool(settings.TELEGRAM_READER_BOT_TOKEN)
    return resp


# -------------------------------------------------------
# HALKA ARZ (IPO) ENDPOINTS
# -------------------------------------------------------

@app.get("/api/v1/ipos", response_model=list[IPOListOut])
async def list_ipos(
    status: Optional[str] = Query(None, description="upcoming, active, completed, postponed, cancelled"),
    year: Optional[int] = Query(None, description="Yil filtresi"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Halka arz listesi — filtreli."""
    from app.services.ipo_service import IPOService
    service = IPOService(db)
    ipos = await service.get_all_ipos(status=status, year=year, limit=limit, offset=offset)
    return ipos


@app.get("/api/v1/ipos/upcoming", response_model=list[IPOListOut])
async def upcoming_ipos(db: AsyncSession = Depends(get_db)):
    """Yaklasan ve aktif halka arzlar."""
    from app.services.ipo_service import IPOService
    service = IPOService(db)
    return await service.get_upcoming_ipos()


@app.get("/api/v1/ipos/sections", response_model=IPOSectionsOut)
async def ipo_sections(db: AsyncSession = Depends(get_db)):
    """Halka arz ana ekrani — 6 bolum.

    1. SPK Onayi Beklenen: spk_applications tablosundan pending olanlar
    2. Yeni Onaylanan: SPK onayli, talep toplama henuz baslamamislar
    3. Dagitim Surecinde: Talep toplama acik (in_distribution)
    4. Islem Gunu Beklenen: Dagitim bitmis, islem tarihi bekleniyor
    5. Isleme Baslayanlar: Borsada islem goren, 25 gun takip
    6. Ilk 25 Takvim Gunu Performansi: 25 gunu gecmis arsivlenmis IPO'lar
    """
    from sqlalchemy import func as sa_func

    # 1. SPK Onayi Beklenen — spk_applications tablosu (tum basvurular)
    spk_result = await db.execute(
        select(SPKApplication)
        .where(SPKApplication.status == "pending")
        .order_by(SPKApplication.application_date.asc())
    )
    spk_pending = list(spk_result.scalars().all())

    # 2. Yeni Onaylanan — newly_approved status
    newly_result = await db.execute(
        select(IPO)
        .where(
            and_(
                IPO.status == "newly_approved",
                IPO.archived == False,
            )
        )
        .order_by(IPO.created_at.desc())
        .limit(20)
    )
    newly_approved = list(newly_result.scalars().all())

    # 3. Dagitim Surecinde — in_distribution status
    dist_result = await db.execute(
        select(IPO)
        .where(
            and_(
                IPO.status == "in_distribution",
                IPO.archived == False,
            )
        )
        .order_by(IPO.subscription_end.asc().nullslast())
        .limit(20)
    )
    in_distribution = list(dist_result.scalars().all())

    # 4. Islem Gunu Beklenen — awaiting_trading status
    awaiting_result = await db.execute(
        select(IPO)
        .where(
            and_(
                IPO.status == "awaiting_trading",
                IPO.archived == False,
            )
        )
        .order_by(IPO.created_at.desc())
        .limit(20)
    )
    awaiting_trading = list(awaiting_result.scalars().all())

    # 5. Isleme Baslayanlar — trading status + 25 takvim gunu icinde
    # 25 takvim gunu siniri: trading_start + 25 gun > bugun
    from datetime import date as date_type, timedelta
    today = date_type.today()
    calendar_cutoff = today - timedelta(days=25)  # 25 takvim gunu (ilk islem gunu sayilmaz)

    trading_result = await db.execute(
        select(IPO)
        .where(
            and_(
                IPO.status == "trading",
                IPO.trading_start != None,
                IPO.trading_start >= calendar_cutoff,  # 25 takvim gunu icinde
            )
        )
        .options(selectinload(IPO.ceiling_tracks), selectinload(IPO.allocations))
        .order_by(IPO.trading_start.desc().nullslast())
        .limit(30)
    )
    trading = list(trading_result.scalars().all())

    # 6. Ilk 25 Takvim Gunu Performansi — 25 takvim gunu gecmis VEYA arsivlenmis
    perf_result = await db.execute(
        select(IPO)
        .where(
            or_(
                IPO.archived == True,
                and_(
                    IPO.status == "trading",
                    IPO.trading_start != None,
                    IPO.trading_start < calendar_cutoff,  # 25 takvim gunu gecmis
                ),
            )
        )
        .options(selectinload(IPO.ceiling_tracks), selectinload(IPO.allocations))
        .order_by(IPO.trading_start.desc().nullslast())
        .limit(200)
    )
    performance_archive = list(perf_result.scalars().all())

    # Arsiv sayisi (toplam)
    archived_count_result = await db.execute(
        select(sa_func.count(IPO.id)).where(
            or_(
                IPO.archived == True,
                and_(
                    IPO.status == "trading",
                    IPO.trading_start != None,
                    IPO.trading_start < calendar_cutoff,
                ),
            )
        )
    )
    archived_count = archived_count_result.scalar() or 0

    return IPOSectionsOut(
        spk_pending=spk_pending,
        newly_approved=newly_approved,
        in_distribution=in_distribution,
        awaiting_trading=awaiting_trading,
        trading=trading,
        performance_archive=performance_archive,
        archived_count=archived_count,
    )


@app.get("/api/v1/ipos/spk-applications", response_model=list[SPKApplicationOut])
async def get_spk_applications(db: AsyncSession = Depends(get_db)):
    """SPK onayi beklenen halka arz basvurulari (Bolum 1).

    spk.gov.tr/istatistikler/basvurular/ilk-halka-arz-basvurusu tablosundan
    scraper tarafindan cekilir.
    """
    result = await db.execute(
        select(SPKApplication)
        .where(SPKApplication.status == "pending")
        .order_by(SPKApplication.created_at.desc())
    )
    return list(result.scalars().all())


########################################################
# BLOG API
########################################################

@app.get("/api/v1/public/blogs", response_model=list[BlogPostOut])
async def get_published_blogs(db: AsyncSession = Depends(get_db)):
    """Yayinlanan blog yazilarini listele (web sitesi icin)."""
    from app.models.blog_post import BlogPost as BlogPostModel
    result = await db.execute(
        select(BlogPostModel)
        .where(BlogPostModel.is_published == True)
        .order_by(BlogPostModel.published_at.desc().nullslast())
    )
    return list(result.scalars().all())


@app.get("/api/v1/public/system-announcements")
async def get_system_announcements(
    db: AsyncSession = Depends(get_db),
    days: int = Query(30, le=90),
    limit: int = Query(10, le=30),
):
    """Sistem bilgilendirme duyurulari (borsa tatili, bakim, genel duyurular).
    Web sitesinde 'Son Guncellemeler' icin."""
    from app.models import PendingTweet
    cutoff = datetime.utcnow() - timedelta(days=days)
    stmt = (
        select(PendingTweet)
        .where(
            PendingTweet.source == "system_info",
            PendingTweet.sent_at >= cutoff,
        )
        .order_by(desc(PendingTweet.sent_at))
        .limit(limit)
    )
    rows = (await db.execute(stmt)).scalars().all()
    return [
        {
            "id": r.id,
            "text": r.text,
            "sent_at": r.sent_at.isoformat() if r.sent_at else None,
            "image_path": r.image_path or None,
        }
        for r in rows
    ]


@app.get("/api/v1/public/blogs/{slug}")
async def get_blog_by_slug(slug: str, db: AsyncSession = Depends(get_db)):
    """Slug ile tek blog yazisi getir."""
    from app.models.blog_post import BlogPost as BlogPostModel
    result = await db.execute(
        select(BlogPostModel)
        .where(BlogPostModel.slug == slug, BlogPostModel.is_published == True)
    )
    blog = result.scalar_one_or_none()
    if not blog:
        raise HTTPException(status_code=404, detail="Blog bulunamadi")
    return blog


@app.post("/api/v1/admin/generate-blog")
async def api_generate_blog(
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """API uzerinden blog yazisi uret."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.blog_post import BlogPost
    from app.services.blog_generator_service import generate_blog_post

    topic = payload.get("topic")
    category = payload.get("category", "borsa_rehberi")

    result_q = await db.execute(select(BlogPost.title))
    existing_titles = [row[0] for row in result_q.all()]

    blog_data = await generate_blog_post(topic=topic, category=category, existing_titles=existing_titles)
    if not blog_data:
        raise HTTPException(status_code=500, detail="Blog uretilemedi")

    now = datetime.now(timezone.utc)
    new_blog = BlogPost(
        slug=blog_data["slug"],
        title=blog_data["title"],
        content=blog_data["content"],
        meta_description=blog_data.get("meta_description"),
        category=blog_data.get("category", category),
        is_published=True,
        published_at=now,
    )
    db.add(new_blog)
    await db.flush()
    await db.commit()

    # Push bildirim
    try:
        import asyncio
        from app.services.broadcast import broadcast_background_task
        asyncio.create_task(broadcast_background_task(
            title=f"📚 {new_blog.title}",
            body="Yeni rehber yazısı yayınlandı. İncelemek için tıklayın!",
            audience="rehber",
            deep_link_target="rehber",
        ))
    except Exception:
        pass

    return {"id": new_blog.id, "title": new_blog.title, "slug": new_blog.slug}


@app.post("/api/v1/admin/update-bulletin-text")
async def api_update_bulletin_text(
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Bulten metnini guncelle."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    bid = payload.get("id")
    new_text = payload.get("text")
    if not bid or not new_text:
        raise HTTPException(status_code=400, detail="id ve text gerekli")
    table = payload.get("table", "pending_tweets")
    if table not in ("pending_tweets", "spk_applications"):
        raise HTTPException(status_code=400, detail="Gecersiz tablo")
    await db.execute(sa_text(f"UPDATE {table} SET {payload.get('field', 'text')} = :t WHERE id = :i"), {"t": new_text, "i": bid})
    await db.commit()
    return {"ok": True}


@app.post("/api/v1/admin/cleanup-spk-names")
async def api_cleanup_spk_names(
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """SPK sirket adlarindan * ve ^ isaretlerini kaldir."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    result = await db.execute(sa_text(
        "UPDATE spk_applications SET company_name = TRIM(LEADING '* ' FROM TRIM(LEADING '^ ' FROM TRIM(LEADING '*' FROM TRIM(LEADING '^' FROM company_name)))) WHERE company_name LIKE '*%' OR company_name LIKE '^%'"
    ))
    await db.commit()
    return {"cleaned": result.rowcount}


@app.post("/api/v1/admin/generate-spk-descriptions")
async def api_generate_spk_descriptions(
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """API uzerinden SPK sirket aciklamalarini toplu uret."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    import asyncio
    from app.models.spk_application import SPKApplication as SPKApp
    from app.services.company_description_service import generate_company_description

    from sqlalchemy import func as sa_func

    # Aciklamasi olmayan VEYA 100 karakterden kisa (yarim kalmis) olanlari bul
    result = await db.execute(
        select(SPKApp).where(
            SPKApp.status == "pending",
        ).where(
            (SPKApp.company_description == None) |
            (SPKApp.company_description == "") |
            (sa_func.length(SPKApp.company_description) < 100)
        )
    )
    apps = list(result.scalars().all())

    if not apps:
        return {"message": "Tum sirketlerin aciklamasi zaten var", "generated": 0}

    # Batch limit — Render 300sn timeout'una takilmamak icin
    batch_size = int(payload.get("limit", 20))
    apps = apps[:batch_size]

    generated = 0
    failed = 0
    for app_item in apps:
        try:
            desc = await generate_company_description(app_item.company_name)
            if desc:
                app_item.company_description = desc
                generated += 1
                await db.flush()
            else:
                failed += 1
        except Exception as e:
            logger.error(f"Desc failed for {app_item.company_name}: {e}")
            failed += 1
        await asyncio.sleep(0.5)

    await db.commit()
    return {"message": f"{generated} aciklama uretildi, {failed} basarisiz", "generated": generated, "failed": failed}


########################################################
# ARSIV
########################################################

@app.get("/api/v1/ipos/archived", response_model=list[IPOListOut])
async def get_archived_ipos(
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Arsivlenmis halka arzlar — 25 gunu gecmis olanlar."""
    result = await db.execute(
        select(IPO)
        .where(IPO.archived == True)
        .order_by(IPO.trading_start.desc().nullslast())
        .offset(offset)
        .limit(limit)
    )
    return list(result.scalars().all())


@app.get("/api/v1/ipos/{ipo_id}", response_model=IPODetailOut)
async def get_ipo_detail(ipo_id: int, db: AsyncSession = Depends(get_db)):
    """Halka arz detay — tum bilgileri icerir."""
    import json as _json_detail
    result = await db.execute(
        select(IPO)
        .options(
            selectinload(IPO.allocations),
            selectinload(IPO.ceiling_tracks),
            selectinload(IPO.brokers),
        )
        .where(IPO.id == ipo_id)
    )
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")

    # 0 bulgu prospectus_analysis → null olarak dön (frontend crash önlenir)
    if ipo.prospectus_analysis:
        try:
            _pa = _json_detail.loads(ipo.prospectus_analysis)
            _p = _pa.get("positives", [])
            _n = _pa.get("negatives", [])
            if len(_p) == 0 and len(_n) == 0:
                ipo.prospectus_analysis = None
        except Exception:
            pass

    return ipo


@app.get("/api/v1/ipos/{ipo_id}/prospectus-analysis")
async def get_prospectus_analysis(ipo_id: int, db: AsyncSession = Depends(get_db)):
    """İzahname AI analizi — JSON veri + görsel URL.

    Uygulama içinde tam sayfa görsel gösterimi için kullanılır.
    Görsel: /static/prospectus/{ipo_id}.png (otomatik üretilir)
    """
    import json as _json
    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")

    if not ipo.prospectus_analysis:
        raise HTTPException(status_code=404, detail="İzahname analizi henüz hazır değil")

    # JSON'u parse et
    try:
        analysis_data = _json.loads(ipo.prospectus_analysis)
    except Exception:
        analysis_data = {}

    # Görsel URL — on-the-fly generate endpoint
    img_url = f"/api/v1/ipos/{ipo_id}/prospectus-image"

    return {
        "ipo_id": ipo_id,
        "company_name": ipo.company_name,
        "ticker": ipo.ticker,
        "ipo_price": str(ipo.ipo_price) if ipo.ipo_price else None,
        "analysis": analysis_data,
        "image_url": img_url,
        "analyzed_at": ipo.prospectus_analyzed_at.isoformat() if ipo.prospectus_analyzed_at else None,
        "prospectus_url": ipo.prospectus_url,
    }


@app.get("/api/v1/ipos/{ipo_id}/prospectus-image")
async def get_prospectus_image(ipo_id: int, db: AsyncSession = Depends(get_db)):
    """İzahname analiz görselini on-the-fly üretip PNG olarak döner.

    Render her deploy'da ephemeral disk'i sıfırladığı için
    statik dosya yerine her istekte generate edilir.
    Dosya cache'i varsa ondan, yoksa yeni üretir.
    """
    import json as _json
    from fastapi.responses import FileResponse

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")

    if not ipo.prospectus_analysis:
        raise HTTPException(status_code=404, detail="İzahname analizi yok")

    # Cache: dosya varsa doğrudan dön
    import os
    cache_path = os.path.join(
        os.path.dirname(__file__), "static", "prospectus", f"prospectus_{ipo_id}.png"
    )
    if os.path.exists(cache_path):
        return FileResponse(cache_path, media_type="image/png")

    # JSON parse
    try:
        analysis_data = _json.loads(ipo.prospectus_analysis)
    except Exception:
        raise HTTPException(status_code=500, detail="Analiz verisi okunamadı")

    # 0 bulgu kontrolü
    if not analysis_data.get("positives") and not analysis_data.get("negatives"):
        raise HTTPException(status_code=404, detail="Analiz bulgusu yok")

    # Görsel üret
    try:
        import asyncio
        from app.services.prospectus_image import generate_prospectus_analysis_image

        ipo_price = str(ipo.ipo_price) if ipo.ipo_price else None
        img_path = await asyncio.get_running_loop().run_in_executor(
            None,
            generate_prospectus_analysis_image,
            ipo.company_name,
            ipo_price,
            analysis_data,
            ipo_id,
            0,  # pages_analyzed — DB'de tutmuyoruz, 0 olursa footer'da gösterilmez
        )
        if img_path and os.path.exists(img_path):
            return FileResponse(img_path, media_type="image/png")
        else:
            raise HTTPException(status_code=500, detail="Görsel üretilemedi")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Prospectus image generation error: %s", e)
        raise HTTPException(status_code=500, detail="Görsel üretme hatası")


# -------------------------------------------------------
# IPO POLL (Katılım Anketi + Tavan Tahmini) — 2 Fazlı
# -------------------------------------------------------
# Faz belirleme IPO status'a gore:
#   - hype   (Faz 1): status in ('newly_approved', 'in_distribution')
#   - ceiling (Faz 2): status in ('awaiting_trading', 'trading') ve first trading
#     gununden 1 gunden az gecmisse aktif.
# Archived IPO'larda anket kapali.


def _determine_poll_phase(ipo: IPO) -> str | None:
    """IPO status'una gore aktif poll fazini dondurur.

    Returns:
        'hype'    — Faz 1 (talep toplama sureci)
        'ceiling' — Faz 2 (dagitim sonrasi + ilk islem gunu)
        None      — Anket kapali
    """
    status = (ipo.status or "").strip()

    # ISLEM BASLADI veya ARSIVLENDI -> anket KAPALI (her durumda).
    # ceiling_poll_notified_at set olsa bile artik gercek sonuc gorulmeye
    # basladigi icin tahmin anketi anlamsiz. (Onceki bug: trading basladiktan
    # sonra bile poll acik gorunuyordu — EKDMR ornegi.)
    if status in ("trading", "archived"):
        return None

    # GUVENLI YOL: ceiling_poll_notified_at set edildiyse (17:00 push atildi)
    # zaten tavan fazina gecmis demektir. Bu en kesin sinyal.
    if getattr(ipo, "ceiling_poll_notified_at", None):
        return "ceiling"

    # HYPE fazi: SPK onayi bekleyen veya dagitim surecindeyken "Katilacak misin?" anketi
    # ONEMLI: Dagitim surecinde (in_distribution) olsa bile subscription_end gunu
    # KAPANIS SAATI gectiyse, artik 'sure doldu' anlamina gelir -> ceiling fazina gec.
    # Kapanis saati subscription_hours'tan parse edilir (orn: "09:00-17:00" -> 17:00).
    # Eger subscription_hours yoksa default 17:00 kullanilir.
    if status in ("newly_approved", "in_distribution"):
        from datetime import date as _date, datetime as _dt, timezone as _tz, timedelta as _td
        if status == "in_distribution" and ipo.subscription_end:
            try:
                end = ipo.subscription_end
                end_date = end.date() if hasattr(end, 'date') else end
                _today = _date.today()
                if _today > end_date:
                    return "ceiling"
                # subscription_end GUNU + kapanis saati gectiyse -> ceiling
                if _today == end_date:
                    # subscription_hours parse et: "09:00-17:00" -> hour=17, minute=0
                    close_hour, close_minute = 17, 0
                    sh = (ipo.subscription_hours or "").strip()
                    if sh and "-" in sh:
                        try:
                            close_part = sh.split("-")[1].strip()
                            if ":" in close_part:
                                _h, _m = close_part.split(":", 1)
                                close_hour = int(_h.strip())
                                close_minute = int(_m.strip()[:2])
                        except (ValueError, IndexError):
                            pass
                    _now_tr = _dt.now(_tz(_td(hours=3)))
                    if (_now_tr.hour, _now_tr.minute) >= (close_hour, close_minute):
                        return "ceiling"
            except Exception:
                pass
        return "hype"
    # CEILING fazi: awaiting_trading (talep toplama bitti, islem gunu bekleniyor)
    # Trading basladigi an anket ARSIVLENIR — gercek sonuc gorulmeye baslayinca tahmin kapanir.
    if status == "awaiting_trading":
        return "ceiling"
    return None


def _get_client_ip(request: Request) -> str:
    """X-Forwarded-For header'indan veya client.host'tan IP adresi al."""
    xff = request.headers.get("x-forwarded-for") or ""
    if xff:
        return xff.split(",")[0].strip()
    try:
        return request.client.host if request.client else "unknown"
    except Exception:
        return "unknown"


@app.get("/api/v1/ipos/{ipo_id}/poll-phase")
@limiter.limit("60/minute")
async def get_poll_phase(ipo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    """IPO icin aktif poll fazini dondurur. Anket kapali ise phase=null."""
    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")
    return {
        "ipo_id": ipo_id,
        "phase": _determine_poll_phase(ipo),
        "status": ipo.status,
    }


@app.post("/api/v1/ipos/{ipo_id}/poll-vote")
@limiter.limit("30/minute")
async def submit_poll_vote(
    ipo_id: int,
    payload: dict,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Oy kaydet.

    Payload:
      {
        "phase": "hype" | "ceiling",
        "choice": "participate" | "skip"   (hype icin — sadece evet/hayir)
                  "<int>"                    (ceiling icin, 1-30)
        "device_id": "..."  (mobil ise, opsiyonel ama tek oy kurali icin onerilen)
      }

    Tek oy kurali:
      - device_id varsa (mobil)  → (ipo_id, phase, device_id) unique
      - device_id yoksa (web)    → (ipo_id, phase, ip_address) unique
    """
    from app.models.ipo_poll_vote import IPOPollVote

    phase = str(payload.get("phase") or "").strip()
    choice = str(payload.get("choice") or "").strip()
    device_id = (payload.get("device_id") or "").strip() or None

    if phase not in ("hype", "ceiling"):
        raise HTTPException(status_code=400, detail="Gecersiz faz")

    # Choice validasyonu
    if phase == "hype":
        # Sadece evet/hayir. 'undecided' eski surumden gelmis olabilir → reddet.
        if choice not in ("participate", "skip"):
            raise HTTPException(status_code=400, detail="Gecersiz secim (hype)")
    else:  # ceiling
        try:
            n = int(choice)
            # UI ile tutarli: 1-25 (mobil chip scroll + web slider)
            if n < 1 or n > 25:
                raise ValueError()
            choice = str(n)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Tavan sayisi 1-25 arasi olmali")

    # IPO ve faz kontrolu
    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")

    active_phase = _determine_poll_phase(ipo)
    if active_phase != phase:
        raise HTTPException(
            status_code=400,
            detail=f"Bu faz su anda aktif degil (aktif: {active_phase})",
        )

    ip_address = None if device_id else _get_client_ip(request)

    # Duplicate kontrolu — kullanici zaten oy verdi mi?
    from sqlalchemy import and_ as _and
    if device_id:
        dup_stmt = select(IPOPollVote).where(_and(
            IPOPollVote.ipo_id == ipo_id,
            IPOPollVote.phase == phase,
            IPOPollVote.device_id == device_id,
        ))
    else:
        dup_stmt = select(IPOPollVote).where(_and(
            IPOPollVote.ipo_id == ipo_id,
            IPOPollVote.phase == phase,
            IPOPollVote.ip_address == ip_address,
        ))
    dup_result = await db.execute(dup_stmt)
    if dup_result.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Bu ankete zaten oy kullandiniz")

    # Kaydet
    vote = IPOPollVote(
        ipo_id=ipo_id,
        phase=phase,
        choice=choice,
        device_id=device_id,
        ip_address=ip_address,
    )
    db.add(vote)
    try:
        await db.commit()
    except Exception as _e:
        await db.rollback()
        # Unique constraint violation — race condition ile duplicate
        raise HTTPException(status_code=409, detail="Bu ankete zaten oy kullandiniz")

    return {"status": "ok", "phase": phase, "choice": choice}


@app.get("/api/v1/ipos/{ipo_id}/poll-stats")
@limiter.limit("60/minute")
async def get_poll_stats(ipo_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    """Anket istatistikleri.

    Response:
      {
        "ipo_id": 1,
        "active_phase": "hype" | "ceiling" | null,
        "hype": {
          "total": 10,
          "participate": 6, "participate_pct": 60.0,
          "skip": 4,        "skip_pct": 40.0
        },
        "ceiling": {
          "total": 5,
          "average": 7.4,     # ortalama tavan tahmini
          "distribution": {"5": 2, "8": 2, "10": 1}
        }
      }
    """
    from app.models.ipo_poll_vote import IPOPollVote
    from sqlalchemy import func as _f

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")

    # Hype istatistikleri — sadece evet/hayir. Eski 'undecided' kayitlari
    # gelirse istatistige dahil edilmez (geriye donuk uyumluluk).
    hype_stmt = select(IPOPollVote.choice, _f.count().label("n")).where(
        IPOPollVote.ipo_id == ipo_id, IPOPollVote.phase == "hype",
    ).group_by(IPOPollVote.choice)
    hype_rows = (await db.execute(hype_stmt)).all()
    hype_counts = {"participate": 0, "skip": 0}
    for choice, n in hype_rows:
        if choice in hype_counts:
            hype_counts[choice] = n
    hype_total = sum(hype_counts.values())

    def _pct(n: int, total: int) -> float:
        return round(100.0 * n / total, 1) if total else 0.0

    # Ceiling istatistikleri
    ceiling_stmt = select(IPOPollVote.choice).where(
        IPOPollVote.ipo_id == ipo_id, IPOPollVote.phase == "ceiling",
    )
    ceiling_rows = (await db.execute(ceiling_stmt)).all()
    ceiling_values = []
    # Distribution'i 1..25 ile init et — frontend bos olsa da 25 bar render edebilsin.
    distribution: dict[str, int] = {str(i): 0 for i in range(1, 26)}
    for (choice,) in ceiling_rows:
        try:
            v = int(choice)
            if 1 <= v <= 25:
                ceiling_values.append(v)
                distribution[str(v)] = distribution.get(str(v), 0) + 1
        except (ValueError, TypeError):
            continue
    ceiling_avg = round(sum(ceiling_values) / len(ceiling_values), 1) if ceiling_values else None

    return {
        "ipo_id": ipo_id,
        "active_phase": _determine_poll_phase(ipo),
        "hype": {
            "total": hype_total,
            "participate": hype_counts["participate"],
            "participate_pct": _pct(hype_counts["participate"], hype_total),
            "skip": hype_counts["skip"],
            "skip_pct": _pct(hype_counts["skip"], hype_total),
        },
        "ceiling": {
            "total": len(ceiling_values),
            "average": ceiling_avg,
            "distribution": distribution,
        },
    }


@app.get("/api/v1/ipos/{ipo_id}/poll-my-vote")
@limiter.limit("60/minute")
async def get_my_poll_vote(
    ipo_id: int,
    request: Request,
    device_id: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin bu IPO'ya verdigi oyu dondurur (her iki faz icin)."""
    from app.models.ipo_poll_vote import IPOPollVote
    from sqlalchemy import and_ as _and

    ip_address = None if device_id else _get_client_ip(request)

    if device_id:
        stmt = select(IPOPollVote).where(_and(
            IPOPollVote.ipo_id == ipo_id,
            IPOPollVote.device_id == device_id,
        ))
    else:
        stmt = select(IPOPollVote).where(_and(
            IPOPollVote.ipo_id == ipo_id,
            IPOPollVote.ip_address == ip_address,
        ))

    rows = (await db.execute(stmt)).scalars().all()
    result = {"hype": None, "ceiling": None}
    for v in rows:
        if v.phase in result:
            result[v.phase] = v.choice
    return {"ipo_id": ipo_id, **result}


# -------------------------------------------------------
# TELEGRAM HABER ENDPOINTS (YENi)
# -------------------------------------------------------

@app.get("/api/v1/telegram-news", response_model=list[TelegramNewsOut])
async def list_telegram_news(
    ticker: Optional[str] = Query(None, description="Hisse kodu filtresi"),
    message_type: Optional[str] = Query(None, description="seans_ici_pozitif, borsa_kapali, seans_disi_acilis"),
    sentiment: Optional[str] = Query(None, description="positive, negative, neutral"),
    days: int = Query(365, ge=1, le=365, description="Son kac gun (varsayilan: 365 — pratik olarak sinir yok)"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    device_id: Optional[str] = Query(None, description="Abonelik kontrolu icin device_id"),
    package: Optional[str] = Query(None, description="Frontend'den gelen aktif paket (ana_yildiz)"),
    apply_user_filters: bool = Query(True, description="User'in DB'deki tercih filtrelerini uygula (market+seans+skor)"),
    db: AsyncSession = Depends(get_db),
):
    """Telegram kanalindan gelen AI haberler.

    - Abone DEGiL: BIST 50 hisselerinin son 30 haberi (ucretsiz tanitim)
    - Abone (ana_yildiz): Ana + Yildiz Pazar — tum hisselerin son 50 haberi
    """
    from app.services.news_service import get_bist50_tickers_sync
    BIST50_TICKERS = get_bist50_tickers_sync()

    has_paid_sub = False
    active_package = None

    if device_id:
        user_result = await db.execute(
            select(User).where(User.device_id == device_id)
        )
        user = user_result.scalar_one_or_none()
        if user:
            # is_active VEYA expires_at hâlâ gelecekte → VIP aktif say
            # (webhook gecikmesi / hatası durumunda kullanıcı mağdur olmasın)
            sub_result = await db.execute(
                select(UserSubscription).where(
                    and_(
                        UserSubscription.user_id == user.id,
                        UserSubscription.package == "ana_yildiz",
                        or_(
                            UserSubscription.is_active == True,
                            UserSubscription.expires_at > datetime.utcnow(),
                        ),
                    )
                )
            )
            sub = sub_result.scalar_one_or_none()
            if sub:
                has_paid_sub = True
                active_package = sub.package

            # FALLBACK: DB'de henüz kayıt yoksa ama frontend ana_yildiz gönderiyorsa
            # (sync henüz tamamlanmamış — race condition)
            # Frontend RC entitlement'ı doğruladıktan sonra package gönderir
            if not has_paid_sub and package == "ana_yildiz":
                has_paid_sub = True
                active_package = "ana_yildiz"
                # Arka planda sync'i tetikle — DB'yi güncelle
                if user:
                    try:
                        existing_sub = await db.execute(
                            select(UserSubscription).where(UserSubscription.user_id == user.id)
                        )
                        s = existing_sub.scalar_one_or_none()
                        if s:
                            s.package = "ana_yildiz"
                            s.is_active = True
                        else:
                            db.add(UserSubscription(user_id=user.id, package="ana_yildiz", is_active=True))
                        await db.commit()
                    except Exception:
                        pass  # Sessizce devam — haber gösterilsin

    # Tarih filtresi
    since = datetime.utcnow() - timedelta(days=days)

    if has_paid_sub:
        # Ucretli abone (ana_yildiz): tum hisselerin haberleri (max 50, sayfa basi 30)
        # seans_disi_acilis = acilis gap bilgisi, haber degil — listede gosterme
        query = (
            select(TelegramNews)
            .where(
                TelegramNews.created_at >= since,
                TelegramNews.message_type != "seans_disi_acilis",
            )
            .order_by(desc(TelegramNews.created_at))
        )

        if ticker:
            query = query.where(TelegramNews.ticker == ticker.upper())
        if message_type:
            query = query.where(TelegramNews.message_type == message_type)
        if sentiment:
            query = query.where(TelegramNews.sentiment == sentiment)

        # User tercih filtreleri — DB'deki notify_market_filter + notify_seans_filter
        # Push bildirimlerinde de ayni filtre uygulanir (notify_kap_news'da).
        if apply_user_filters and device_id and user and not message_type and not ticker:
            _mkt = (getattr(user, "notify_market_filter", "all") or "all").lower()
            _seans = (getattr(user, "notify_seans_filter", "all") or "all").lower()

            # Seans filtresi
            if _seans == "seans_ici":
                query = query.where(TelegramNews.message_type == "seans_ici_pozitif")
            elif _seans == "seans_disi":
                query = query.where(TelegramNews.message_type.in_(["borsa_kapali", "seans_disi_acilis"]))

            # Pazar filtresi — stock_markets tablosundan ticker'lari cek
            if _mkt != "all":
                from app.models.stock_market import StockMarket
                if _mkt == "ana":
                    allowed = ["ana_pazar"]
                elif _mkt == "yildiz":
                    allowed = ["yildiz_pazar"]
                elif _mkt == "ana_yildiz":
                    allowed = ["ana_pazar", "yildiz_pazar"]
                else:
                    allowed = []
                if allowed:
                    market_subq = select(StockMarket.ticker).where(
                        StockMarket.market_segment.in_(allowed)
                    )
                    query = query.where(TelegramNews.ticker.in_(market_subq))

        query = query.limit(min(limit, 50)).offset(offset)
    else:
        # Ucretsiz: BIST 50 hisselerinin son 20 haberi
        # seans_disi_acilis = acilis gap bilgisi, haber degil — listede gosterme
        query = (
            select(TelegramNews)
            .where(
                and_(
                    TelegramNews.created_at >= since,
                    TelegramNews.ticker.in_(BIST50_TICKERS),
                    TelegramNews.message_type != "seans_disi_acilis",
                )
            )
            .order_by(desc(TelegramNews.created_at))
        )

        if ticker:
            query = query.where(TelegramNews.ticker == ticker.upper())
        if message_type:
            query = query.where(TelegramNews.message_type == message_type)
        if sentiment:
            query = query.where(TelegramNews.sentiment == sentiment)

        query = query.limit(min(limit, 30)).offset(offset)

    result = await db.execute(query)
    return list(result.scalars().all())


@app.get("/api/v1/telegram-news/locked-teasers")
@limiter.limit("60/minute")
async def get_locked_teasers(
    request: Request,
    limit: int = Query(30, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """FREE kullanicinin GOREMEDIGI PRO-only haberlerin teaser'i.

    Sadece ticker + kisa baslik (ilk 60 karakter) + tarih doner.
    AI puani ve body gizli — frontend'de kilitli PRO kart gosterir.

    Filtre:
      - Sadece BIST50 DISINDAKI hisseler (PRO paket icerigi)
      - sentiment='positive' olanlar (pozitif haber akisindayiz)
      - Son 7 gun
    """
    from app.services.news_service import get_bist50_tickers_sync
    BIST50_TICKERS = get_bist50_tickers_sync()

    since = datetime.utcnow() - timedelta(days=7)

    stmt = (
        select(TelegramNews.ticker, TelegramNews.parsed_title, TelegramNews.message_date, TelegramNews.created_at)
        .where(
            and_(
                TelegramNews.created_at >= since,
                TelegramNews.ticker.isnot(None),
                TelegramNews.parsed_title.isnot(None),
                TelegramNews.message_type != "seans_disi_acilis",
                TelegramNews.sentiment == "positive",
                ~TelegramNews.ticker.in_(BIST50_TICKERS),  # BIST50 HARICI
            )
        )
        .order_by(desc(TelegramNews.created_at))
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.all()

    teasers = []
    for r in rows:
        title = (r.parsed_title or "").strip()
        # Kisalt (teaser) — ilk 60 karakter
        if len(title) > 60:
            title = title[:57].rstrip() + "..."
        if not title:
            continue
        teasers.append({
            "ticker": r.ticker,
            "keyword": title,
            "date": (r.message_date or r.created_at).isoformat() if (r.message_date or r.created_at) else None,
        })
    return teasers


# -------------------------------------------------------
# TELEGRAM DEBUG / TEST ENDPOINT
# -------------------------------------------------------

@app.post("/api/v1/telegram-news/test-send")
async def test_send_telegram_news(
    request: Request,
    ticker: str = Query("TEST", description="Hisse kodu"),
    sentiment: str = Query("positive", description="positive veya negative"),
    db: AsyncSession = Depends(get_db),
):
    """Test icin Telegram kanalina mesaj gonder + DB'ye kaydet.

    Admin sifresi gerektirir — spam onleme.
    """
    admin_pw = request.headers.get("X-Admin-Password", "")
    if not _verify_admin_password(admin_pw):
        raise HTTPException(status_code=401, detail="Admin sifresi gerekli (X-Admin-Password header)")
    from app.services.telegram_sender import send_and_save_kap_news

    success = await send_and_save_kap_news(
        db=db,
        ticker=ticker.upper(),
        sentiment=sentiment,
        news_type="seans_ici",
        matched_keyword="test - pipeline dogrulama",
        kap_url=None,
        kap_id=str(int(datetime.utcnow().timestamp())),
        news_title=f"{ticker.upper()} Test Haberi",
        raw_text=f"Bu bir test mesajidir. Sembol: {ticker.upper()}",
    )
    await db.commit()

    return {
        "success": success,
        "message": f"Telegram + DB kaydedildi: {ticker.upper()}" if success else "Hata olustu",
    }


# -------------------------------------------------------
# KAP HABER ENDPOINTS
# -------------------------------------------------------

@app.get("/api/v1/news", response_model=list[KapNewsOut])
async def list_news(
    ticker: Optional[str] = Query(None, description="Hisse kodu filtresi"),
    sentiment: Optional[str] = Query(None, description="positive, negative"),
    news_type: Optional[str] = Query(None, description="seans_ici, seans_disi"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """KAP haber listesi — filtrelenmis."""
    query = select(KapNews).order_by(desc(KapNews.created_at))

    if ticker:
        query = query.where(KapNews.ticker == ticker.upper())
    if sentiment:
        query = query.where(KapNews.sentiment == sentiment)
    if news_type:
        query = query.where(KapNews.news_type == news_type)

    query = query.limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


@app.get("/api/v1/news/latest", response_model=list[KapNewsOut])
async def latest_news(
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """Son KAP haberleri — kronolojik."""
    result = await db.execute(
        select(KapNews)
        .order_by(desc(KapNews.created_at))
        .limit(limit)
    )
    return list(result.scalars().all())


# -------------------------------------------------------
# BILDIRIM TIER ENDPOINTS (YENi)
# -------------------------------------------------------

@app.get("/api/v1/notification-tiers", response_model=list[NotificationTierOut])
async def list_notification_tiers():
    """Hisse bazli bildirim tipi fiyat listesi."""
    return [
        NotificationTierOut(
            type=tier_key,
            price_tl=tier_info["price_tl"],
            label=tier_info["label"],
            description=tier_info["description"],
        )
        for tier_key, tier_info in NOTIFICATION_TIER_PRICES.items()
    ]


@app.get("/api/v1/news-tiers", response_model=list[NewsTierOut])
async def list_news_tiers():
    """Haber abonelik paket fiyat listesi."""
    return [
        NewsTierOut(
            package=pkg_key,
            price_tl_monthly=pkg_info["price_tl_monthly"],
            annual_months=pkg_info["annual_months"],
            annual_price_tl=pkg_info["price_tl_monthly"] * pkg_info["annual_months"],
            label=pkg_info["label"],
            description=pkg_info["description"],
        )
        for pkg_key, pkg_info in NEWS_TIER_PRICES.items()
    ]


# -------------------------------------------------------
# HISSE BILDIRIM ABONELIK ENDPOINTS (YENi)
# -------------------------------------------------------

@app.post("/api/v1/users/{device_id}/stock-notifications", response_model=StockNotificationOut)
async def create_stock_notification(
    device_id: str,
    data: StockNotificationCreate,
    db: AsyncSession = Depends(get_db),
):
    """Hisse bazli bildirim aboneligi satin al."""
    # Row-level lock — ayni kullanicidan gelen esanli isteklerde race condition engellenir
    user_result = await db.execute(
        select(User).where(User.device_id == device_id).with_for_update()
    )
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    # Yillik paket
    if data.is_annual_bundle:
        # KORUMA: Bundle oluşturmak için geçerli store/product_id veya wallet gerekli
        # Frontend'den gelen "boş" bundle oluşturma isteklerini engelle
        # (purchase transfer veya stale cache sonucu oluşan kısır döngü önlenir)
        valid_bundle_stores = {"play_store", "app_store", "wallet"}
        if not data.store or data.store not in valid_bundle_stores:
            raise HTTPException(status_code=400, detail="Bundle olusturmak icin gecerli store gerekli")

        # Store play_store/app_store ise product_id zorunlu (webhook/sync zaten gönderiyor)
        if data.store in ("play_store", "app_store") and not data.product_id:
            raise HTTPException(status_code=400, detail="Bundle olusturmak icin product_id gerekli")

        existing = await db.execute(
            select(StockNotificationSubscription).where(
                and_(
                    StockNotificationSubscription.user_id == user.id,
                    StockNotificationSubscription.is_annual_bundle == True,
                    StockNotificationSubscription.is_active == True,
                )
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Zaten aktif yillik paketiniz bulunuyor")

        _now = datetime.now(timezone.utc)
        # Paket turu: quarterly (90 gun) veya annual (365 gun)
        # Frontend product_id'den anliyoruz: "3aylik" → 90, "yillik" → 365
        bundle_days = 365 if data.product_id and "yillik" in (data.product_id or "") else 90
        sub = StockNotificationSubscription(
            user_id=user.id,
            ipo_id=None,
            notification_type="all",
            is_annual_bundle=True,
            price_paid_tl=ANNUAL_BUNDLE_PRICE,
            is_active=True,
            store=data.store or "play_store",
            product_id=data.product_id,
            purchased_at=_now,
            expires_at=_now + timedelta(days=bundle_days),
        )
        db.add(sub)
        await db.commit()
        await db.refresh(sub)
        logger.info(
            "Bundle olusturuldu: user_id=%s, days=%d, expires=%s",
            user.id, bundle_days, sub.expires_at.isoformat(),
        )
        return sub

    # Tek hisse bildirim
    if data.notification_type not in NOTIFICATION_TIER_PRICES:
        raise HTTPException(
            status_code=400,
            detail=f"Gecersiz bildirim tipi: {data.notification_type}"
        )

    if not data.ipo_id:
        raise HTTPException(status_code=400, detail="Tek hisse bildirimi icin ipo_id zorunlu")

    ipo_result = await db.execute(select(IPO).where(IPO.id == data.ipo_id))
    ipo = ipo_result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")

    existing = await db.execute(
        select(StockNotificationSubscription).where(
            and_(
                StockNotificationSubscription.user_id == user.id,
                StockNotificationSubscription.ipo_id == data.ipo_id,
                StockNotificationSubscription.notification_type == data.notification_type,
                StockNotificationSubscription.is_active == True,
            )
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=400,
            detail=f"Bu hisse icin {data.notification_type} bildirimi zaten aktif"
        )

    tier_info = NOTIFICATION_TIER_PRICES[data.notification_type]

    sub = StockNotificationSubscription(
        user_id=user.id,
        ipo_id=data.ipo_id,
        notification_type=data.notification_type,
        is_annual_bundle=False,
        price_paid_tl=tier_info["price_tl"],
        is_active=True,
    )
    db.add(sub)
    await db.commit()
    await db.refresh(sub)
    return sub


@app.get("/api/v1/users/{device_id}/stock-notifications", response_model=list[StockNotificationOut])
async def list_stock_notifications(
    device_id: str,
    active_only: bool = Query(True),
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin hisse bildirim aboneliklerini listeler."""
    user_result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    query = select(StockNotificationSubscription).where(
        StockNotificationSubscription.user_id == user.id
    )
    if active_only:
        query = query.where(StockNotificationSubscription.is_active == True)

    query = query.order_by(desc(StockNotificationSubscription.purchased_at))
    result = await db.execute(query)
    return list(result.scalars().all())


@app.patch("/api/v1/users/{device_id}/stock-notifications/deactivate-all")
async def deactivate_all_stock_notifications(
    device_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin TUM aktif hisse bildirim aboneliklerini iptal et.

    3 aylik veya yillik paket alindiginda, mevcut tekil abonelikler
    otomatik olarak deaktif edilir.
    """
    user_result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    # Sadece tekil abonelikleri iptal et — bundle (3 aylik/yillik) paketleri KORUYORUZ
    # Bundle'lar ayri bir satin alma, deactivate-all sadece tekil hisse bazli abonelikleri etkiler
    result = await db.execute(
        update(StockNotificationSubscription)
        .where(
            and_(
                StockNotificationSubscription.user_id == user.id,
                StockNotificationSubscription.is_active == True,
                StockNotificationSubscription.is_annual_bundle == False,
            )
        )
        .values(is_active=False)
    )
    count = result.rowcount
    await db.commit()
    return {"message": f"{count} tekil abonelik iptal edildi (bundle paketler korundu)", "deactivated_count": count}


@app.post("/api/v1/users/{device_id}/stock-notifications/sync")
async def sync_stock_notifications(
    device_id: str,
    data: StockNotificationSyncRequest,
    db: AsyncSession = Depends(get_db),
):
    """App'teki lokal abonelikleri backend ile senkronize et.

    App baslatildiginda veya satin alma sonrasi cagirilir.
    Backend'de olmayan lokal abonelikleri olusturur.
    Zaten varsa atlar (duplicate engelleme).
    """
    user_result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    created_count = 0
    skipped_count = 0
    _now = datetime.now(timezone.utc)

    for item in data.subscriptions:
        # Bundle kontrolu
        if item.is_annual_bundle:
            existing = await db.execute(
                select(StockNotificationSubscription).where(
                    and_(
                        StockNotificationSubscription.user_id == user.id,
                        StockNotificationSubscription.is_annual_bundle == True,
                        StockNotificationSubscription.is_active == True,
                    )
                )
            )
            if existing.scalar_one_or_none():
                skipped_count += 1
                continue

            # expires_at parse
            exp = None
            if item.expires_at:
                try:
                    exp = datetime.fromisoformat(item.expires_at.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    exp = _now + timedelta(days=90)
            else:
                exp = _now + timedelta(days=90)

            sub = StockNotificationSubscription(
                user_id=user.id,
                ipo_id=None,
                notification_type="all",
                is_annual_bundle=True,
                price_paid_tl=0,
                is_active=True,
                purchased_at=_now,
                expires_at=exp,
            )
            db.add(sub)
            created_count += 1
            continue

        # Tekil abonelik kontrolu
        if not item.ipo_id:
            skipped_count += 1
            continue

        existing = await db.execute(
            select(StockNotificationSubscription).where(
                and_(
                    StockNotificationSubscription.user_id == user.id,
                    StockNotificationSubscription.ipo_id == item.ipo_id,
                    StockNotificationSubscription.notification_type == item.notification_type,
                    StockNotificationSubscription.is_active == True,
                )
            )
        )
        if existing.scalar_one_or_none():
            skipped_count += 1
            continue

        sub = StockNotificationSubscription(
            user_id=user.id,
            ipo_id=item.ipo_id,
            notification_type=item.notification_type,
            is_annual_bundle=False,
            price_paid_tl=0,
            is_active=True,
            purchased_at=_now,
        )
        db.add(sub)
        created_count += 1

    if created_count > 0:
        await db.flush()

    logger.info(
        "Stock notification sync: user_id=%s, created=%d, skipped=%d",
        user.id, created_count, skipped_count,
    )
    return {
        "status": "ok",
        "created": created_count,
        "skipped": skipped_count,
    }


@app.patch("/api/v1/users/{device_id}/stock-notifications/{sub_id}/mute")
async def toggle_mute_stock_notification(
    device_id: str,
    sub_id: int,
    notification_type: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Hisse bildirim aboneligini sessize al / sesi ac (toggle).

    Tekli abonelik: muted=True/False toggle (eskisi gibi).
    Bundle abonelik (notification_type='all'):
      - notification_type query param verilirse → tip bazli mute (muted_types JSON)
      - verilmezse → tum tipleri mute (eski davranis)
    """
    import json

    user_result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    sub_result = await db.execute(
        select(StockNotificationSubscription).where(
            StockNotificationSubscription.id == sub_id,
            StockNotificationSubscription.user_id == user.id,
        )
    )
    sub = sub_result.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="Abonelik bulunamadi")

    # Bundle abonelikte tip bazli mute
    if (sub.is_annual_bundle or sub.notification_type == "all") and notification_type:
        current_muted = json.loads(sub.muted_types) if sub.muted_types else []
        if notification_type in current_muted:
            current_muted.remove(notification_type)
            is_muted = False
        else:
            current_muted.append(notification_type)
            is_muted = True
        sub.muted_types = json.dumps(current_muted) if current_muted else None
        # Master mute: hepsi mute ise muted=True, biri bile aciksa muted=False
        all_types = ["tavan_bozulma", "taban_acilma", "gunluk_acilis_kapanis", "yuzde_dusus"]
        sub.muted = len(current_muted) >= len(all_types)
        await db.commit()
        await db.refresh(sub)
        return {
            "status": "ok",
            "subscription_id": sub.id,
            "muted": is_muted,
            "muted_types": current_muted,
            "notification_type": notification_type,
        }

    # Tekli abonelik: basit toggle
    sub.muted = not sub.muted
    await db.commit()
    await db.refresh(sub)

    return {
        "status": "ok",
        "subscription_id": sub.id,
        "muted": sub.muted,
        "message": "Bildirim sessize alindi" if sub.muted else "Bildirim aktif edildi",
    }


# -------------------------------------------------------
# KULLANICI ENDPOINTS
# -------------------------------------------------------

@app.post("/api/v1/users/register", response_model=UserOut)
@limiter.limit("10/minute")
async def register_device(request: Request, data: UserRegister, db: AsyncSession = Depends(get_db)):
    """Cihaz kayit — ilk acilista cagrilir.

    Hesap kurtarma: Eger device_id bulunamazsa ama persistent_id eslesen bir
    kullanici varsa, eski hesabi geri yukler (device_id gunceller).
    Bu sayede uygulama silinip tekrar yuklendiginde cuzdan bakiyesi,
    abonelikler ve tum veriler korunur.
    """
    # Bos string token'lari None'a cevir (AuthContext bos token ile register yapar)
    clean_fcm = data.fcm_token.strip() if data.fcm_token else None
    clean_fcm = clean_fcm or None  # "" -> None
    clean_expo = data.expo_push_token.strip() if data.expo_push_token else None
    clean_expo = clean_expo or None
    clean_persistent = data.persistent_id.strip() if data.persistent_id else None
    clean_persistent = clean_persistent or None

    is_new_install = False  # Telegram admin bildirimi icin
    is_recovery = False

    result = await db.execute(
        select(User).where(User.device_id == data.device_id)
    )
    user = result.scalar_one_or_none()

    if user:
        # Mevcut kullanici — token ve persistent_id guncelle
        if clean_fcm:
            user.fcm_token = clean_fcm
        if clean_expo:
            user.expo_push_token = clean_expo
        if clean_persistent and not user.persistent_id:
            user.persistent_id = clean_persistent
        user.platform = data.platform
        user.app_version = data.app_version
    elif clean_persistent:
        # device_id bulunamadi — persistent_id ile hesap kurtarma dene
        recovery_result = await db.execute(
            select(User).where(
                and_(
                    User.persistent_id == clean_persistent,
                    User.deleted == False,
                )
            )
        )
        recovered_user = recovery_result.scalar_one_or_none()

        if recovered_user:
            # Eski hesap bulundu — device_id'yi guncelle (hesap kurtarma)
            recovered_user.device_id = data.device_id
            if clean_fcm:
                recovered_user.fcm_token = clean_fcm
            if clean_expo:
                recovered_user.expo_push_token = clean_expo
            recovered_user.platform = data.platform
            recovered_user.app_version = data.app_version
            user = recovered_user
            is_recovery = True
            logger.info(
                "Hesap kurtarma basarili: persistent_id=%s, eski_device=%s → yeni_device=%s, user_id=%d",
                clean_persistent, "?", data.device_id, user.id,
            )
        else:
            # persistent_id ile de bulunamadi — yeni kullanici olustur
            user = User(
                device_id=data.device_id,
                persistent_id=clean_persistent,
                fcm_token=clean_fcm,
                expo_push_token=clean_expo,
                platform=data.platform,
                app_version=data.app_version,
            )
            db.add(user)
            is_new_install = True

            await db.flush()
            subscription = UserSubscription(
                user_id=user.id,
                package="free",
                is_active=True,
            )
            db.add(subscription)
    else:
        # persistent_id yok, device_id bulunamadi — yeni kullanici
        user = User(
            device_id=data.device_id,
            fcm_token=clean_fcm,
            expo_push_token=clean_expo,
            platform=data.platform,
            app_version=data.app_version,
        )
        db.add(user)
        is_new_install = True

        await db.flush()
        subscription = UserSubscription(
            user_id=user.id,
            package="free",
            is_active=True,
        )
        db.add(subscription)

    await db.flush()

    # ─── Telegram admin bildirimi: yeni kurulum / hesap kurtarma ───
    if is_new_install or is_recovery:
        try:
            from app.services.admin_telegram import notify_new_install
            await notify_new_install(
                user_id=user.id,
                device_id=user.device_id or "?",
                platform=data.platform or "?",
                app_version=data.app_version or "?",
                is_recovery=is_recovery,
            )
        except Exception as _e:
            logger.warning("Yeni install Telegram bildirim hatasi: %s", _e)

    return user


@app.get("/api/v1/users/{device_id}", response_model=UserOut)
async def get_user(
    device_id: str = Depends(_validate_device_id_param),
    db: AsyncSession = Depends(get_db),
):
    """Kullanici profilini getirir — bildirim tercihleri dahil."""
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")
    return user


@app.put("/api/v1/users/{device_id}", response_model=UserOut)
async def update_user(
    device_id: str = Depends(_validate_device_id_param),
    data: UserUpdate = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Kullanici bilgilerini ve bildirim tercihlerini guncelle."""
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    update_data = data.model_dump(exclude_unset=True)

    # Bos string token'lari temizle — bos string gecerli token degildir
    if "fcm_token" in update_data:
        val = (update_data["fcm_token"] or "").strip()
        if not val:
            del update_data["fcm_token"]  # Bos token ile mevcut tokeni ezme
        else:
            update_data["fcm_token"] = val
    if "expo_push_token" in update_data:
        val = (update_data["expo_push_token"] or "").strip()
        if not val:
            del update_data["expo_push_token"]
        else:
            update_data["expo_push_token"] = val

    # Hesap silme talebi — soft-delete: bildirimleri kapat, tokenlari temizle
    if update_data.get("deleted") is True:
        user.deleted = True
        user.deleted_at = datetime.now(timezone.utc)
        user.notifications_enabled = False
        user.fcm_token = None
        user.expo_push_token = None
        await db.flush()
        return user

    for key, value in update_data.items():
        if hasattr(user, key):
            setattr(user, key, value)

    # NOT: Artık FCM token güncellemesinde notifications_enabled otomatik açılmıyor.
    # Kullanıcı bildirimleri kapattıysa, token yenilemesi bunu sıfırlamamalı.
    # Kullanıcı bildirimleri açmak isterse bunu bildirim ayarlarından yapar.

    await db.flush()
    return user


@app.delete("/api/v1/users/{device_id}")
async def delete_user_account(
    device_id: str = Depends(_validate_device_id_param),
    db: AsyncSession = Depends(get_db),
):
    """Kullanici hesabini ve tum verilerini kalici olarak siler.

    Silinen veriler:
      - Kullanici profili ve bildirim tercihleri
      - Abonelik bilgileri (UserSubscription)
      - IPO bildirim tercihleri (UserIPOAlert)
      - Tavan takip abonelikleri (CeilingTrackSubscription)
      - Hisse bildirim abonelikleri (StockNotificationSubscription)

    Bu islem geri alinamaz.
    Abonelikler App Store / Google Play uzerinden ayrica iptal edilmelidir.
    """
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    # ÖNEMLİ: Ücretli abonelik ve bildirim kayıtları SİLİNMEZ — kullanıcı para ödemiş.
    # "Geri Yükle" ile tekrar aktif edilebilir olmalı.
    # Sadece kişisel tercihler ve loglar silinir.

    # device_id ile bagli tablolar (FK degil, cascade calismaz)
    from app.models.notification_log import NotificationLog
    from app.models.user_watchlist import UserWatchlist
    from app.models.user import FeatureInterest

    await db.execute(
        delete(NotificationLog).where(NotificationLog.device_id == device_id)
    )
    await db.execute(
        delete(UserWatchlist).where(UserWatchlist.device_id == device_id)
    )
    await db.execute(
        delete(FeatureInterest).where(FeatureInterest.device_id == device_id)
    )

    # IPO alert tercihlerini sil (kullanıcı tercihi, ücretli değil)
    from app.models import UserIPOAlert
    await db.execute(
        delete(UserIPOAlert).where(UserIPOAlert.user_id == user.id)
    )

    # Kullanıcı profilini sıfırla (silme yerine — abonelik FK bağlı)
    user.push_token = None
    user.reminder_30min = True
    user.reminder_1h = True
    user.reminder_2h = False
    user.reminder_4h = False

    await db.commit()

    return {
        "status": "ok",
        "message": "Kisisel verileriniz silindi. Ucretli abonelikleriniz korundu — Geri Yukle ile erisebilirsiniz.",
    }


@app.put("/api/v1/users/{device_id}/reminder-settings", response_model=UserOut)
async def update_reminder_settings(
    device_id: str,
    data: ReminderSettingsUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Hatirlatma zamani ayarlarini guncelle (30dk / 1h / 2h / 4h)."""
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    user.reminder_30min = data.reminder_30min
    user.reminder_1h = data.reminder_1h
    user.reminder_2h = data.reminder_2h
    user.reminder_4h = data.reminder_4h

    await db.flush()
    return user


@app.get("/api/v1/users/{device_id}/subscription", response_model=SubscriptionInfo)
async def get_subscription(device_id: str, db: AsyncSession = Depends(get_db)):
    """Kullanicinin abonelik bilgisini getirir."""
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    result = await db.execute(
        select(UserSubscription).where(UserSubscription.user_id == user.id)
    )
    sub = result.scalar_one_or_none()

    if not sub:
        return SubscriptionInfo(package="free", is_active=True)

    return SubscriptionInfo(
        package=sub.package,
        is_active=sub.is_active,
        expires_at=sub.expires_at,
    )


# -------------------------------------------------------
# ADMIN: Subscription Elle Aktiflestirilmesi
# -------------------------------------------------------

@app.post("/api/v1/admin/activate-news-subscription/{device_id}")
async def admin_activate_news_subscription(
    device_id: str,
    days: int = Query(30, description="Kac gun aktif olacak"),
    password: str = Query(..., description="Admin sifresi"),
    db: AsyncSession = Depends(get_db),
):
    """Admin: Kullanicinin haber paketini elle aktiflestirir."""
    if password != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    _now = datetime.now(timezone.utc)
    sub_result = await db.execute(
        select(UserSubscription).where(UserSubscription.user_id == user.id)
    )
    existing_sub = sub_result.scalar_one_or_none()
    if existing_sub:
        existing_sub.package = "ana_yildiz"
        existing_sub.is_active = True
        existing_sub.store = "wallet"
        existing_sub.product_id = "admin_manual_activation"
        existing_sub.started_at = _now
        existing_sub.expires_at = _now + timedelta(days=days)
    else:
        new_sub = UserSubscription(
            user_id=user.id,
            package="ana_yildiz",
            is_active=True,
            store="wallet",
            product_id="admin_manual_activation",
            revenue_cat_id=None,
            started_at=_now,
            expires_at=_now + timedelta(days=days),
        )
        db.add(new_sub)

    await db.flush()
    logger.info("Admin: Haber paketi elle aktif edildi: user_id=%s, %s gun", user.id, days)
    return {"status": "ok", "user_id": user.id, "package": "ana_yildiz", "days": days}


@app.post("/api/v1/admin/deactivate-stock-notification/{sub_id}")
async def admin_deactivate_stock_notification(
    sub_id: int,
    password: str = Query(..., description="Admin sifresi"),
    db: AsyncSession = Depends(get_db),
):
    """Admin: Belirli bir hisse bildirim aboneligini deaktif et."""
    if password != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    result = await db.execute(
        select(StockNotificationSubscription).where(StockNotificationSubscription.id == sub_id)
    )
    sub = result.scalar_one_or_none()
    if not sub:
        raise HTTPException(status_code=404, detail="Abonelik bulunamadi")

    sub.is_active = False
    await db.flush()
    return {"status": "ok", "id": sub_id, "was_bundle": sub.is_annual_bundle}


@app.post("/api/v1/admin/test-notification/{device_id}")
async def admin_test_notification(
    device_id: str,
    password: str = Query(..., description="Admin sifresi"),
    db: AsyncSession = Depends(get_db),
):
    """Admin: Kullaniciya test bildirimi gonder — FCM token durumunu da gosterir."""
    if password != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    token_info = {
        "user_id": user.id,
        "device_id": user.device_id,
        "fcm_token": user.fcm_token[:40] + "..." if user.fcm_token and len(user.fcm_token) > 40 else user.fcm_token,
        "expo_push_token": user.expo_push_token[:40] + "..." if user.expo_push_token and len(user.expo_push_token) > 40 else user.expo_push_token,
        "notifications_enabled": user.notifications_enabled,
    }

    # Token yoksa bildirim gonderemeyiz
    fcm = (user.fcm_token or "").strip()
    expo = (user.expo_push_token or "").strip()
    if not fcm and not expo:
        return {
            "status": "error",
            "message": "FCM token ve Expo token bos — bildirim gonderilemez. Kullanicinin uygulamayi acip bildirim iznini vermesi gerekli.",
            "token_info": token_info,
        }

    # _send_to_user kullan — FCM varsa Firebase, Expo varsa Expo Push API
    from app.services.notification import NotificationService
    notif_service = NotificationService(db)

    try:
        success = await notif_service._send_to_user(
            user=user,
            title="Test Bildirimi",
            body="Push bildirim sistemi test ediliyor!",
            data={"type": "test", "ticker": "TEST"},
            channel_id="kap_news_v2",
            delay=False,
        )

        # method tespiti: FCM basarisiz olup Expo fallback yaptiysa farkli goster
        fcm_error = getattr(notif_service, '_last_send_error', None)
        if fcm and success and fcm_error:
            method = "Expo (FCM fallback)"
        elif fcm and success:
            method = "FCM"
        elif not fcm and success:
            method = "Expo"
        else:
            method = "FCM+Expo" if fcm and expo else ("FCM" if fcm else "Expo")

        if success:
            return {
                "status": "ok",
                "message": f"{method} test bildirimi basarili!",
                "token_info": token_info,
                "method": method,
            }
        else:
            return {
                "status": "error",
                "message": f"Test bildirimi basarisiz — tum tokenlar gecersiz",
                "token_info": token_info,
                "method": method,
                "last_error": fcm_error,
            }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "message": f"Test bildirim HATASI: {str(e)}",
            "error_type": type(e).__name__,
            "token_info": token_info,
            **({"traceback": traceback.format_exc()[-800:]} if not settings.is_production else {}),
        }


@app.post("/api/v1/admin/test-kap-notification")
@limiter.limit("5/minute")
async def admin_test_kap_notification(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: KAP haber bildirimi simulasyonu — notify_kap_news cagir."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ticker = payload.get("ticker", "ARSAN")
    news_type = payload.get("news_type", "seans_ici")  # seans_ici | seans_disi | seans_disi_acilis
    matched_keyword = payload.get("matched_keyword", "Kredi Sozlesmesi")
    pct_change = payload.get("pct_change", "%-2.38")

    from app.services.notification import NotificationService
    notif_service = NotificationService(db)

    sent = await notif_service.notify_kap_news(
        ticker=ticker,
        price=None,
        kap_id="TEST_" + ticker,
        matched_keyword=matched_keyword,
        sentiment="positive",
        news_type=news_type,
        pct_change=pct_change,
    )

    return {
        "status": "ok",
        "message": f"KAP bildirim testi: {ticker} ({news_type}) — {sent} kullaniciya gonderildi",
        "sent": sent,
        "ticker": ticker,
        "news_type": news_type,
    }


@app.post("/api/v1/admin/patch-ipo-fields")
@limiter.limit("10/minute")
async def admin_patch_ipo_fields(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: IPO alanlarini manuel guncelle (test/hot-fix).

    Payload: {admin_password, ipo_id, fields:{close_price:..., percent_change:..., ...}}
    Sadece sunlar guncellenir: close_price, percent_change, ceiling_broken,
    trading_day_count, archived, status, distribution_completed.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ipo_id = payload.get("ipo_id")
    fields = payload.get("fields") or {}
    ALLOWED = {"close_price", "percent_change", "ceiling_broken",
               "trading_day_count", "archived", "status", "distribution_completed",
               "katilim_endeksi", "market_segment", "public_float_pct"}

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    updated = {}
    for k, v in fields.items():
        if k in ALLOWED:
            setattr(ipo, k, v)
            updated[k] = v
    await db.commit()

    return {"status": "ok", "ipo_id": ipo_id, "ticker": ipo.ticker, "updated": updated}


@app.post("/api/v1/admin/set-ipo-status")
@limiter.limit("10/minute")
async def admin_set_ipo_status(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: IPO status degistir (test/demo amacli).

    Payload: {admin_password, ipo_id, status}

    Gecerli status'lar: spk_pending, newly_approved, in_distribution,
    awaiting_trading, trading, completed, archived
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ipo_id = payload.get("ipo_id")
    new_status = str(payload.get("status") or "").strip()
    valid_statuses = {"spk_pending", "newly_approved", "in_distribution",
                      "awaiting_trading", "trading", "completed", "archived"}
    if new_status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Gecersiz status. Gecerli: {valid_statuses}")

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    old_status = ipo.status
    ipo.status = new_status
    await db.commit()

    return {
        "status": "ok",
        "ipo_id": ipo_id,
        "ticker": ipo.ticker,
        "old_status": old_status,
        "new_status": new_status,
    }


@app.post("/api/v1/admin/trigger-ipo-hype-6h-reminder")
@limiter.limit("5/minute")
async def admin_trigger_ipo_hype_6h(
    request: Request,
    ipo_id: int = Query(..., description="Hangi IPO icin push"),
    force: bool = Query(False, description="hype_6h_notified_at dolu olsa bile zorla"),
    db: AsyncSession = Depends(get_db),
):
    """IPO icin anket bitimine 6 saat kala hatirlatma push'unu manuel tetikle.
    Oy vermemis tum kullanicilara kisisel push.
    """
    from app.models.ipo import IPO
    from app.models.ipo_poll_vote import IPOPollVote
    from app.models.user import User
    from app.services.notification import NotificationService
    from sqlalchemy import select as _sel, and_, distinct
    import asyncio as _aio

    ipo = (await db.execute(_sel(IPO).where(IPO.id == ipo_id))).scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")
    if ipo.hype_6h_notified_at and not force:
        return {"status": "skipped", "reason": "zaten gonderilmis (force=true ile zorla)"}
    if force:
        ipo.hype_6h_notified_at = None
        await db.commit()

    voted_subq = _sel(distinct(IPOPollVote.device_id)).where(
        and_(IPOPollVote.ipo_id == ipo.id, IPOPollVote.phase == "hype"),
    )
    target_users = (await db.execute(
        _sel(User).where(
            and_(
                User.notifications_enabled == True,  # noqa: E712
                User.deleted == False,  # noqa: E712
                User.device_id.notin_(voted_subq),
            )
        )
    )).scalars().all()

    company = (ipo.ticker or ipo.company_name or "Halka Arz")[:30]
    title = f"⏰ Anket Bitimine Son Saatler — {company}"
    body = (
        f"{company} halka arzı için anket bitiyor. "
        "Sizin oyunuz da BorsaCebimde topluluğunun sesi olsun!"
    )

    async def _run():
        async with async_session() as bg_db:
            notif = NotificationService(bg_db)
            sent, failed = 0, 0
            for u in target_users[:5000]:
                try:
                    ok = await notif._send_to_user(
                        user=u, title=title, body=body,
                        data={
                            "type": "ipo_poll_reminder",
                            "screen": "halka-arz-detay",
                            "ipo_id": str(ipo.id),
                            "scroll_to": "poll",
                            "poll_phase": "hype",
                        },
                        channel_id="default_v2",
                        category="other",
                    )
                    if ok: sent += 1
                    else: failed += 1
                    await _aio.sleep(0.5)
                except Exception:
                    failed += 1
            ipo_row = (await bg_db.execute(_sel(IPO).where(IPO.id == ipo_id))).scalar_one_or_none()
            if ipo_row:
                ipo_row.hype_6h_notified_at = datetime.now(timezone.utc)
                await bg_db.commit()
            try:
                from app.services.admin_telegram import notify_push_sent
                await notify_push_sent(
                    notification_type=f"IPO 6h Hatirlatma (manuel): {company}",
                    title=title, sent_count=sent, failed_count=failed,
                    detail=f"Toplam hedef: {len(target_users)}",
                )
            except Exception:
                pass

    from app.database import async_session
    _bg_task = _aio.create_task(_run())
    _bg_set = globals().setdefault("_admin_bg_tasks", set())
    _bg_set.add(_bg_task)
    _bg_task.add_done_callback(_bg_set.discard)

    return {
        "status": "sent_async",
        "ipo_id": ipo.id,
        "company": company,
        "target_count": len(target_users),
        "message": "Push arka planda gonderiliyor",
    }


@app.post("/api/v1/admin/trigger-spk-bulten-push")
@limiter.limit("5/minute")
async def admin_trigger_spk_bulten_push(
    request: Request,
    bulletin_no: str = Query(..., description="Bulten numarasi, orn: 2026/29"),
    summary: str = Query("", description="Ozet (opsiyonel, AI analiz konu basliklari)"),
    db: AsyncSession = Depends(get_db),
):
    """SPK bulteni push bildirimini manuel tetikle.

    Scheduler/scraper push atmayi kacirdiysa (NameError, cooldown vs.)
    admin elle calistirir.
    """
    from app.services.notification import NotificationService
    notif = NotificationService(db)
    n = await notif.notify_spk_bulletin(bulletin_no, summary)
    return {"status": "sent", "bulletin_no": bulletin_no, "recipients": n}


@app.post("/api/v1/admin/trigger-ceiling-poll-push")
@limiter.limit("5/minute")
async def admin_trigger_ceiling_poll_push(
    request: Request,
    ipo_id: int = Query(..., description="Hangi IPO icin push atilacak"),
    db: AsyncSession = Depends(get_db),
):
    """Belirli IPO icin tavan anketi push'unu manuel tetikle.

    Scheduler 17:00 TR cron'u kacirdiysa veya yeni bir IPO icin acil push
    gerekirse kullanilir. ceiling_poll_notified_at zaten dolu ise atlanir.
    """
    from app.models.ipo import IPO
    from app.models.ipo_poll_vote import IPOPollVote
    from app.services.broadcast import broadcast_background_task
    from sqlalchemy import select as _sel, and_, func as _func

    ipo = (await db.execute(_sel(IPO).where(IPO.id == ipo_id))).scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    # force=true ise ceiling_poll_notified_at flag'ini ignore et + sifirla
    _force = (request.query_params.get("force") or "").lower() in ("1", "true", "yes")
    if ipo.ceiling_poll_notified_at and not _force:
        return {
            "status": "skipped",
            "reason": "ceiling_poll_notified_at zaten dolu (force=true ile zorlayabilirsin)",
            "notified_at": ipo.ceiling_poll_notified_at.isoformat(),
        }
    if _force:
        ipo.ceiling_poll_notified_at = None
        await db.commit()

    company = (ipo.ticker or ipo.company_name or "Halka Arz")[:50]
    # Hype anketi sonuclarini hesapla
    vote_q = _sel(
        IPOPollVote.choice, _func.count(IPOPollVote.id).label("cnt"),
    ).where(
        and_(IPOPollVote.ipo_id == ipo.id, IPOPollVote.phase == "hype")
    ).group_by(IPOPollVote.choice)
    vote_result = await db.execute(vote_q)
    counts = {row.choice: row.cnt for row in vote_result.all()}
    participate = counts.get("participate", 0)
    undecided = counts.get("undecided", 0)
    skip = counts.get("skip", 0)
    total = participate + undecided + skip
    pct_join = (participate / total * 100) if total else 0

    if total > 0:
        summary = f"{total} oy verildi: %{pct_join:.0f} katiliyor ({participate} kisi)"
    else:
        summary = "Anket sonucu henuz olusmadi"

    title = f"\U0001F514 {company} Halka Arzi Bitti — Tavan Anketi Acildi"
    body = f"{summary}. Simdi tavan beklenti anketimiz acildi, oy ver ve sonuclari gor."

    # Fire-and-forget — broadcast 500+ kullaniciya gondrim 15+ dk surer, HTTP timeout olur.
    # create_task ile arka planda gonder, endpoint hemen donsun.
    import asyncio as _asyncio
    _bg = _asyncio.create_task(broadcast_background_task(
        title=title, body=body, audience="all",
        deep_link_target="halka-arz-detay",
        extra_data={
            "screen": "halka-arz-detay",
            "ipo_id": str(ipo.id),
            "scroll_to": "poll",
            "poll_phase": "ceiling",
        },
    ))
    # GC korumasi — task'i tut
    _admin_bg_tasks = globals().setdefault("_admin_bg_tasks", set())
    _admin_bg_tasks.add(_bg)
    _bg.add_done_callback(_admin_bg_tasks.discard)

    ipo.ceiling_poll_notified_at = datetime.now(timezone.utc)
    await db.commit()
    return {
        "status": "sent_async", "ipo_id": ipo.id, "company": company,
        "total_votes": total, "pct_join": pct_join,
        "message": "Push arka planda gonderiliyor (~15dk).",
    }


@app.post("/api/v1/admin/seed-ipo-poll")
@limiter.limit("10/minute")
async def admin_seed_ipo_poll(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: IPO anketine sanal oy seed et (test/demo icin).

    Payload:
      {
        "admin_password": "...",
        "ipo_id": 46,
        "hype": {"participate": 53, "undecided": 28, "skip": 40},
        "ceiling": {"1": 15, "2": 35, "3": 60, "4": 45, "5": 30, "6": 3, "7": 1},
        "reset": false  // true ise IPO'daki tum poll oylarini once siler
      }

    Her oy unique device_id ile insert edilir: 'seed_{ipo}_{phase}_{i}'.
    Toplam oy, hype/ceiling counts toplamidir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.ipo_poll_vote import IPOPollVote
    from sqlalchemy import delete as _del

    ipo_id = payload.get("ipo_id")
    if not isinstance(ipo_id, int):
        raise HTTPException(status_code=400, detail="ipo_id gerekli (int)")

    # IPO var mi?
    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    # Reset — mevcut poll oylarini sil
    if payload.get("reset"):
        await db.execute(_del(IPOPollVote).where(IPOPollVote.ipo_id == ipo_id))

    hype = payload.get("hype") or {}
    ceiling = payload.get("ceiling") or {}

    inserted_hype = 0
    inserted_ceiling = 0

    # Hype oyları — (participate|undecided|skip): count
    seq = 0
    for choice in ("participate", "undecided", "skip"):
        count = int(hype.get(choice, 0) or 0)
        for _ in range(count):
            seq += 1
            db.add(IPOPollVote(
                ipo_id=ipo_id,
                phase="hype",
                choice=choice,
                device_id=f"seed_{ipo_id}_hype_{seq}",
                ip_address=None,
            ))
            inserted_hype += 1

    # Ceiling oyları — {"1": 15, "2": 35, ...}
    seq = 0
    for choice_str, count in ceiling.items():
        try:
            n = int(choice_str)
            if n < 1 or n > 30:
                continue
        except (ValueError, TypeError):
            continue
        count = int(count or 0)
        for _ in range(count):
            seq += 1
            db.add(IPOPollVote(
                ipo_id=ipo_id,
                phase="ceiling",
                choice=str(n),
                device_id=f"seed_{ipo_id}_ceiling_{seq}",
                ip_address=None,
            ))
            inserted_ceiling += 1

    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=f"Insert hatasi: {e}")

    return {
        "status": "ok",
        "ipo_id": ipo_id,
        "ticker": ipo.ticker,
        "inserted_hype": inserted_hype,
        "inserted_ceiling": inserted_ceiling,
        "total": inserted_hype + inserted_ceiling,
    }


@app.post("/api/v1/admin/backfill-ai-scores")
@limiter.limit("3/minute")
async def admin_backfill_ai_scores(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: Mevcut telegram_news kayitlarini AI ile puanla (bir seferlik backfill).

    force=true: Zaten puanlanmis kayitlari da yeniden puanla (V2 upgrade icin).
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    limit = min(payload.get("limit", 35), 50)
    force = payload.get("force", False)

    from app.models.telegram_news import TelegramNews
    from app.services.ai_news_scorer import analyze_news

    # Debug: toplam kayit sayisini kontrol et
    from sqlalchemy import func
    total_count_q = await db.execute(
        select(func.count()).select_from(TelegramNews)
    )
    total_in_db = total_count_q.scalar() or 0

    type_count_q = await db.execute(
        select(func.count()).select_from(TelegramNews).where(
            TelegramNews.message_type.in_(["seans_ici_pozitif", "borsa_kapali"]),
        )
    )
    type_count = type_count_q.scalar() or 0

    # Query: seans_ici/borsa_kapali + ticker var
    query = select(TelegramNews).where(
        TelegramNews.message_type.in_(["seans_ici_pozitif", "borsa_kapali"]),
        TelegramNews.ticker.isnot(None),
    )
    # force=false: sadece ai_score NULL olanlari isle
    if not force:
        query = query.where(TelegramNews.ai_score.is_(None))

    result = await db.execute(
        query.order_by(TelegramNews.created_at.desc()).limit(limit)
    )
    records = list(result.scalars().all())

    scored = 0
    failed = 0
    details = []

    for record in records:
        try:
            ai_result = await analyze_news(
                record.ticker, record.raw_text,
                matriks_id=record.kap_notification_id,
            )
            s = ai_result.get("score")
            sm = ai_result.get("summary")
            kurl = ai_result.get("kap_url")

            # KAP URL yoksa TradingView + Matriks ID ile olustur
            if not kurl and record.kap_notification_id:
                kurl = f"https://tr.tradingview.com/news/matriks:{record.kap_notification_id}:0/"

            if s is not None:
                record.ai_score = s
                record.ai_summary = sm
                record.kap_url = kurl
                scored += 1
                details.append({
                    "ticker": record.ticker, "score": s,
                    "summary": (sm or "")[:80], "kap_url": kurl,
                })
            else:
                failed += 1
                # kap_url yine kaydedilsin (skor olmasa bile link olsun)
                if kurl:
                    record.kap_url = kurl
                details.append({"ticker": record.ticker, "score": None, "error": "AI skoru uretilmedi"})
        except Exception as e:
            failed += 1
            details.append({"ticker": record.ticker, "score": None, "error": str(e)[:100]})

    await db.commit()

    return {
        "status": "ok",
        "total": len(records),
        "scored": scored,
        "failed": failed,
        "force": force,
        "debug_total_in_db": total_in_db,
        "debug_type_match": type_count,
        "details": details,
    }


@app.post("/api/v1/admin/test-ai-scorer")
@limiter.limit("5/minute")
async def admin_test_ai_scorer(
    request: Request,
    payload: dict,
):
    """Admin: AI scorer debug — API key ve tek mesaj testi."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.services.ai_news_scorer import _get_api_key, _ABACUS_URL, _AI_MODEL, analyze_news, _SCORER_VERSION

    api_key = _get_api_key()
    key_preview = (api_key[:8] + "...") if api_key else "EMPTY"

    # Test mesaji
    test_text = payload.get("text", "BIMAS bedelsiz sermaye artirimi karari aldi. Yonetim kurulu 1:1 oraninda bedelsiz sermaye artirimi karari almistir.")
    test_ticker = payload.get("ticker", "BIMAS")

    test_matriks_id = payload.get("matriks_id", None)

    test_result = None
    test_error = None
    try:
        # analyze_news V3: TradingView icerik + AI puanlama
        result = await analyze_news(test_ticker, test_text, matriks_id=test_matriks_id)
        test_result = result
    except Exception as e:
        test_error = str(e)

    return {
        "api_key_preview": key_preview,
        "abacus_url": _ABACUS_URL,
        "ai_model": _AI_MODEL,
        "scorer_version": _SCORER_VERSION,
        "test_ticker": test_ticker,
        "test_result": test_result,
        "test_error": test_error,
    }


@app.post("/api/v1/admin/spk-bulletin-status")
@limiter.limit("10/minute")
async def admin_spk_bulletin_status(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: SPK bulten durumunu goster ve manuel isle."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.scraper_state import ScraperState
    result = await db.execute(
        select(ScraperState).where(ScraperState.key == "spk_last_bulletin_no")
    )
    state = result.scalar_one_or_none()
    last_no = state.value if state else None

    return {
        "last_bulletin_no": last_no,
        "updated_at": str(state.updated_at) if state and state.updated_at else None,
    }


@app.post("/api/v1/admin/check-kap-indexes")
@limiter.limit("10/minute")
async def admin_check_kap_indexes(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: kap_all_disclosures index'lerini listeler (composite migration kontrolu)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from sqlalchemy import text as _sa_text
    result = await db.execute(_sa_text(
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE tablename = 'kap_all_disclosures'"
    ))
    indexes = [{"name": r[0], "definition": r[1]} for r in result.fetchall()]
    return {"indexes": indexes}


async def _get_app_setting(db: AsyncSession, key: str, fallback: str) -> str:
    """app_settings tablosundan key oku. Yoksa fallback (env) don."""
    try:
        from app.models.app_setting import AppSetting
        result = await db.execute(select(AppSetting).where(AppSetting.key == key))
        row = result.scalar_one_or_none()
        if row and row.value:
            return row.value
    except Exception:
        pass
    return fallback


async def _set_app_setting(db: AsyncSession, key: str, value: str) -> None:
    """app_settings tablosuna yaz (upsert)."""
    from app.models.app_setting import AppSetting
    result = await db.execute(select(AppSetting).where(AppSetting.key == key))
    row = result.scalar_one_or_none()
    if row:
        row.value = value
    else:
        db.add(AppSetting(key=key, value=value))


@app.get("/api/v1/app-version")
@limiter.limit("60/minute")
async def get_app_version(request: Request, db: AsyncSession = Depends(get_db)):
    """Uygulamanın güncel sürüm bilgisi — frontend her açılışta sorgular.

    Oncelik sirasi:
      1) app_settings DB tablosu (admin panel ya da auto-sync ile yazilir)
      2) Render env var fallback

    Frontend cevabı kendi versiyonu ile karşılaştırır:
      - current < min_required → ZORUNLU update modal (kapatılamaz)
      - current < latest → ÖNERİLEN update modal (kapatılabilir, cooldown'lu)
      - current >= latest → hiçbir şey
    """
    s = get_settings()
    return {
        "ios": {
            "latest": await _get_app_setting(db, "ios_latest_version", s.IOS_LATEST_VERSION),
            "min_required": await _get_app_setting(db, "ios_min_required_version", s.IOS_MIN_REQUIRED_VERSION),
            "store_url": "https://apps.apple.com/app/id6760570446",
        },
        "android": {
            "latest": await _get_app_setting(db, "android_latest_version", s.ANDROID_LATEST_VERSION),
            "min_required": await _get_app_setting(db, "android_min_required_version", s.ANDROID_MIN_REQUIRED_VERSION),
            "store_url": "https://play.google.com/store/apps/details?id=com.bistfinans.app",
        },
        "release_notes": await _get_app_setting(db, "app_release_notes", s.APP_RELEASE_NOTES),
    }


@app.post("/api/v1/admin/set-app-version")
@limiter.limit("30/minute")
async def admin_set_app_version(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: app_settings DB'ye sürüm bilgilerini yaz.

    Restart gerekmez — anlik aktif olur. Render env'i bypass eder.

    body (tum alanlar opsiyonel — sadece verilen alanlar guncellenir):
      admin_password: zorunlu
      ios_latest_version: "3.0.0"
      ios_min_required_version: "2.9.5"
      android_latest_version: "3.0.0"
      android_min_required_version: "2.9.5"
      app_release_notes: "..."
      source: "admin_panel" | "auto_sync" | "manual"  (telemetri)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    updates = {}
    for key in (
        "ios_latest_version", "ios_min_required_version",
        "android_latest_version", "android_min_required_version",
        "app_release_notes",
    ):
        val = payload.get(key)
        if val is not None and str(val).strip():
            updates[key] = str(val).strip()
            await _set_app_setting(db, key, str(val).strip())

    if not updates:
        return {"success": False, "message": "Guncellenecek alan yok"}

    await db.commit()

    return {
        "success": True,
        "updated": updates,
        "source": payload.get("source", "manual"),
        "message": f"{len(updates)} ayar guncellendi (anlik aktif)",
    }


@app.post("/api/v1/admin/reprocess-kap-news")
@limiter.limit("5/minute")
async def admin_reprocess_kap_news(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: Belirli bir KAP haberi icin AI puanlama tekrar calistirip
    kap_all_disclosures'a yaz.

    Kullanim: AI provider'lar fail oldugu icin score=None ile kalmis veya
    DB'ye hic yazilmamis bir haberi yeniden puanlamak icin.

    body:
      admin_password: zorunlu
      ticker: zorunlu (orn: "GOKNR")
      matriks_id: zorunlu (Matriks/TradingView haber ID, orn: "6454002")
      title: opsiyonel (varsa kullanilir, yoksa AI ozet baslik olarak girer)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ticker = (payload.get("ticker") or "").strip().upper()
    matriks_id = str(payload.get("matriks_id") or "").strip()
    explicit_title = (payload.get("title") or "").strip() or None

    if not ticker or not matriks_id:
        raise HTTPException(status_code=400, detail="ticker ve matriks_id zorunlu")

    try:
        from app.services.ai_news_scorer import analyze_news, fetch_tradingview_content
        from app.models.kap_all_disclosure import KapAllDisclosure
        from app.utils.ai_score_label import score_to_label
        from sqlalchemy import select as _sa_select
        from datetime import datetime as _dt, timezone as _tz

        # 1) TradingView'dan icerik cek (AI'a beslemek icin)
        tv_data = await fetch_tradingview_content(matriks_id)
        tv_text = tv_data.get("full_text", "") if tv_data else ""
        kap_url = (tv_data or {}).get("real_kap_url") or f"https://tr.tradingview.com/news/matriks:{matriks_id}:0/"
        tv_title = (tv_data or {}).get("title") or explicit_title or f"KAP Bildirimi - {ticker}"

        if not tv_text or len(tv_text.strip()) < 50:
            return {
                "success": False,
                "ticker": ticker,
                "matriks_id": matriks_id,
                "message": "TradingView icerik bos veya cok kisa, AI'a beslenemez",
            }

        # 2) AI puanla
        ai_result = await analyze_news(
            ticker=ticker,
            raw_text=tv_text,
            matriks_id=matriks_id,
        )
        score = ai_result.get("score")
        summary = ai_result.get("summary")
        url_from_ai = ai_result.get("kap_url") or kap_url

        if score is None:
            return {
                "success": False,
                "ticker": ticker,
                "matriks_id": matriks_id,
                "message": "AI puanlama basarisiz (tum providerlar fail). Birazdan yeniden deneyin.",
            }

        # 3) Sentiment label (yeni 9 kategorili sistem)
        sentiment_label = score_to_label(score) or "Nötr"

        # 4) Mevcut kayit var mi kontrol et — duplicate engelle
        existing_q = await db.execute(
            _sa_select(KapAllDisclosure).where(
                KapAllDisclosure.kap_url == url_from_ai,
                KapAllDisclosure.company_code == ticker,
            ).limit(1)
        )
        existing = existing_q.scalar_one_or_none()

        if existing:
            # Update et — eski None skoru AI ile guncelle
            existing.ai_impact_score = score
            existing.ai_sentiment = sentiment_label
            existing.ai_summary = summary
            existing.ai_analyzed_at = _dt.now(_tz.utc)
            await db.commit()
            return {
                "success": True,
                "action": "updated",
                "ticker": ticker,
                "id": existing.id,
                "score": score,
                "sentiment": sentiment_label,
                "summary_preview": (summary or "")[:200],
            }

        # 5) Yeni insert
        new_rec = KapAllDisclosure(
            company_code=ticker,
            title=tv_title[:500],
            body=summary,
            category="Genel",
            is_bilanco=False,
            kap_url=url_from_ai,
            source="manual_reprocess",
            published_at=_dt.now(_tz.utc),
            ai_sentiment=sentiment_label,
            ai_impact_score=score,
            ai_summary=summary,
            ai_analyzed_at=_dt.now(_tz.utc),
        )
        db.add(new_rec)
        await db.commit()
        await db.refresh(new_rec)

        return {
            "success": True,
            "action": "inserted",
            "ticker": ticker,
            "id": new_rec.id,
            "score": score,
            "sentiment": sentiment_label,
            "summary_preview": (summary or "")[:200],
            "kap_url": url_from_ai,
        }
    except Exception as e:
        logger.exception("Reprocess KAP hatasi: %s", e)
        raise HTTPException(status_code=500, detail=f"Reprocess hatasi: {e}")


@app.post("/api/v1/admin/sync-bist-tedbir")
@limiter.limit("3/minute")
async def admin_sync_bist_tedbir(
    request: Request,
    payload: dict,
):
    """Admin: Borsa Istanbul resmi tedbirli CSV'sini cek + cautious_stocks'a sync.

    Periodik (TR 09:40 / 19:00 / 00:00) calisir; gerekirse manuel tetiklenir.
    Ilk kurulum icin de bu endpoint kullanilir — DB'yi sifirdan dolurmak icin.

    body:
      admin_password: zorunlu
      rebuild: true ise once tum cautious_stocks tablosunu siler, sonra CSV'den
               yeniden dolurur (varsayilan false — sadece upsert + deactivate)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    rebuild = bool(payload.get("rebuild", False))

    if rebuild:
        # Mevcut tum kayitlari sil — sifirdan olustur
        async with async_session() as db:
            await db.execute(text("DELETE FROM cautious_stocks"))
            await db.commit()
            logger.info("Admin REBUILD: cautious_stocks tablosu sifirlandi")

    try:
        from app.scrapers.bist_tedbir_csv_scraper import sync_bist_tedbir
        stats = await sync_bist_tedbir()
        return {
            "success": True,
            "rebuild": rebuild,
            "stats": stats,
            "message": "BIST resmi tedbirli CSV sync tamamlandi",
        }
    except Exception as e:
        logger.exception("BIST tedbir sync hatasi: %s", e)
        raise HTTPException(status_code=500, detail=f"Sync hatasi: {e}")


@app.post("/api/v1/admin/trigger-temettu-refresh")
@limiter.limit("2/minute")
async def admin_trigger_temettu_refresh(
    request: Request,
    payload: dict,
):
    """Admin: temettuhisseleri.com scraper'i manuel tetikle.

    Normalde periodik calismaz (haftalik cron KAPATILDI). KAP'tan anlik
    mirror yapildigi icin sadece eksik veri durumunda manuel calistirilir.

    body: admin_password (zorunlu)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    try:
        from app.scrapers.temettuhisseleri_scraper import scrape_temettuhisseleri
        stats = await scrape_temettuhisseleri()
        return {
            "success": True,
            "stats": stats,
            "message": "temettuhisseleri.com scraper manuel olarak calistirildi",
        }
    except Exception as e:
        logger.exception("Manuel temettu refresh hatasi: %s", e)
        raise HTTPException(status_code=500, detail=f"Refresh hatasi: {e}")


@app.post("/api/v1/admin/cleanup-isyatirim-dividends")
@limiter.limit("3/minute")
async def admin_cleanup_isyatirim_dividends(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: dividend_history tablosunda source='isyatirim' olan kayitlari siler.

    temettühisseleri scraper'i artik primary kaynak. IsYatirim eski/cift kayit
    olusturuyor — temizlik icin.

    body:
      admin_password: zorunlu
      dry_run: true ise sadece sayim yapar
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    dry_run = bool(payload.get("dry_run", False))

    # Once dagilim
    count_q = await db.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE source = 'isyatirim') AS isyatirim,
            COUNT(*) FILTER (WHERE source = 'temettuhisseleri') AS temettuhisseleri,
            COUNT(*) FILTER (WHERE source IS NULL OR source NOT IN ('isyatirim', 'temettuhisseleri')) AS diger,
            COUNT(DISTINCT ticker) FILTER (WHERE source = 'isyatirim') AS isyatirim_ticker_sayisi,
            COUNT(*) AS toplam
        FROM dividend_history
    """))
    row = count_q.fetchone()
    stats = {
        "isyatirim_kayit_sayisi": row[0] or 0,
        "temettuhisseleri_kayit_sayisi": row[1] or 0,
        "diger_kaynak_kayit_sayisi": row[2] or 0,
        "isyatirim_ticker_sayisi": row[3] or 0,
        "toplam_kayit": row[4] or 0,
    }

    if dry_run:
        return {
            "dry_run": True,
            "would_delete": stats["isyatirim_kayit_sayisi"],
            "stats": stats,
        }

    try:
        result = await db.execute(text(
            "DELETE FROM dividend_history WHERE source = 'isyatirim'"
        ))
        await db.commit()
        deleted = getattr(result, "rowcount", None) or stats["isyatirim_kayit_sayisi"]
    except Exception as e:
        await db.rollback()
        logger.exception("IsYatirim dividend cleanup hatasi: %s", e)
        raise HTTPException(status_code=500, detail=f"Cleanup hatasi: {e}")

    return {
        "success": True,
        "deleted": deleted,
        "stats_after": stats,
        "message": "IsYatirim kaynakli dividend_history kayitlari silindi; sadece temettuhisseleri kaldi",
    }


@app.post("/api/v1/admin/relabel-ai-sentiment")
@limiter.limit("3/minute")
async def admin_relabel_ai_sentiment(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: Tum kap_all_disclosures kayitlarinda ai_sentiment alanini
    ai_impact_score'a gore yeni 9 kategorili etiketle gunceller.

    Etiketler: Guclu Olumlu / Cok Olumlu / Olumlu / Hafif Olumlu /
               Notr / Hafif Olumsuz / Olumsuz / Cok Olumsuz / Guclu Olumsuz

    Eski 3 kategorili sistemden (Olumlu/Notr/Olumsuz) yeni sisteme migrate eder.
    AI yeniden tetiklenmez — sadece SQL update.

    body:
      admin_password: zorunlu
      dry_run: true ise sadece sayim yapar (default false)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    dry_run = bool(payload.get("dry_run", False))

    # CASE WHEN ile tek SQL update — Postgresqil-uyumlu, hizli
    sql = """
        UPDATE kap_all_disclosures
        SET ai_sentiment = CASE
            WHEN ai_impact_score IS NULL THEN ai_sentiment
            WHEN ai_impact_score >= 9.0 THEN 'Güçlü Olumlu'
            WHEN ai_impact_score >= 8.0 THEN 'Çok Olumlu'
            WHEN ai_impact_score >= 7.0 THEN 'Olumlu'
            WHEN ai_impact_score >= 6.0 THEN 'Hafif Olumlu'
            WHEN ai_impact_score >= 4.1 THEN 'Nötr'
            WHEN ai_impact_score >= 3.1 THEN 'Hafif Olumsuz'
            WHEN ai_impact_score >= 2.1 THEN 'Olumsuz'
            WHEN ai_impact_score >= 1.1 THEN 'Çok Olumsuz'
            ELSE 'Güçlü Olumsuz'
        END
        WHERE ai_impact_score IS NOT NULL
    """

    # Once tahmin: kac kayit etkilenecek ve dagilim
    count_q = await db.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE ai_impact_score >= 9.0) AS guclu_olumlu,
            COUNT(*) FILTER (WHERE ai_impact_score >= 8.0 AND ai_impact_score < 9.0) AS cok_olumlu,
            COUNT(*) FILTER (WHERE ai_impact_score >= 7.0 AND ai_impact_score < 8.0) AS olumlu,
            COUNT(*) FILTER (WHERE ai_impact_score >= 6.0 AND ai_impact_score < 7.0) AS hafif_olumlu,
            COUNT(*) FILTER (WHERE ai_impact_score >= 4.1 AND ai_impact_score < 6.0) AS notr,
            COUNT(*) FILTER (WHERE ai_impact_score >= 3.1 AND ai_impact_score < 4.1) AS hafif_olumsuz,
            COUNT(*) FILTER (WHERE ai_impact_score >= 2.1 AND ai_impact_score < 3.1) AS olumsuz,
            COUNT(*) FILTER (WHERE ai_impact_score >= 1.1 AND ai_impact_score < 2.1) AS cok_olumsuz,
            COUNT(*) FILTER (WHERE ai_impact_score < 1.1) AS guclu_olumsuz,
            COUNT(*) FILTER (WHERE ai_impact_score IS NOT NULL) AS total
        FROM kap_all_disclosures
    """))
    row = count_q.fetchone()
    distribution = {
        "guclu_olumlu":  row[0] or 0,
        "cok_olumlu":    row[1] or 0,
        "olumlu":        row[2] or 0,
        "hafif_olumlu":  row[3] or 0,
        "notr":          row[4] or 0,
        "hafif_olumsuz": row[5] or 0,
        "olumsuz":       row[6] or 0,
        "cok_olumsuz":   row[7] or 0,
        "guclu_olumsuz": row[8] or 0,
        "total":         row[9] or 0,
    }

    if dry_run:
        return {
            "dry_run": True,
            "would_update": distribution["total"],
            "distribution": distribution,
        }

    try:
        # Once kolon boyutunu buyut — yeni etiketler 13 karaktere kadar cikabiliyor
        # (Guclu Olumsuz = 13 char). Eski VARCHAR(10) yetersiz.
        await db.execute(text(
            "ALTER TABLE kap_all_disclosures "
            "ALTER COLUMN ai_sentiment TYPE VARCHAR(32)"
        ))
        result = await db.execute(text(sql))
        await db.commit()
        rowcount = getattr(result, "rowcount", None) or distribution["total"]
    except Exception as e:
        await db.rollback()
        logger.exception("Relabel SQL hatasi: %s", e)
        raise HTTPException(status_code=500, detail=f"Relabel hatasi: {e}")

    return {
        "success": True,
        "updated": rowcount,
        "distribution": distribution,
        "message": "ai_sentiment 9 kategorili yeni sisteme migrate edildi",
    }


@app.post("/api/v1/admin/backfill-multi-symbol-disclosure")
@limiter.limit("10/minute")
async def admin_backfill_multi_symbol(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: Tek bir kap_url icin eksik ticker'lari geri-doldur.

    Body: {"admin_password": "...", "kap_url": "https://...", "missing_tickers": ["DMRGD"]}
    Mevcut bir kayit (orn: DAGI) kopyalanir, missing_tickers'taki her bir ticker icin
    yeni satir olusturulur.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    kap_url = payload.get("kap_url", "")
    missing = payload.get("missing_tickers", [])
    if not kap_url or not missing:
        return {"status": "error", "message": "kap_url ve missing_tickers gerekli"}

    from app.models.kap_all_disclosure import KapAllDisclosure

    # Mevcut kaydi bul (kopyalanacak template)
    result = await db.execute(
        select(KapAllDisclosure).where(KapAllDisclosure.kap_url == kap_url).limit(1)
    )
    template = result.scalar_one_or_none()
    if not template:
        return {"status": "error", "message": f"kap_url icin mevcut kayit yok: {kap_url}"}

    inserted = []
    skipped = []
    for ticker in missing:
        # Zaten var mi?
        check = await db.execute(
            select(KapAllDisclosure.id).where(
                KapAllDisclosure.kap_url == kap_url,
                KapAllDisclosure.company_code == ticker,
            ).limit(1)
        )
        if check.scalar_one_or_none():
            skipped.append(ticker)
            continue

        new_disc = KapAllDisclosure(
            company_code=ticker,
            title=template.title,
            body=template.body,
            category=template.category,
            is_bilanco=template.is_bilanco,
            kap_url=kap_url,
            source=template.source,
            published_at=template.published_at,
            ai_sentiment=template.ai_sentiment,
            ai_impact_score=template.ai_impact_score,
            ai_summary=template.ai_summary,
            ai_analyzed_at=template.ai_analyzed_at,
        )
        db.add(new_disc)
        inserted.append(ticker)

    try:
        await db.commit()
    except Exception as e:
        await db.rollback()
        return {"status": "error", "message": f"Commit hatasi (constraint olabilir): {str(e)[:300]}"}

    return {
        "status": "ok",
        "kap_url": kap_url,
        "template_id": template.id,
        "inserted": inserted,
        "skipped_already_exists": skipped,
    }


@app.post("/api/v1/admin/replace-ai-report-text")
@limiter.limit("10/minute")
async def admin_replace_ai_report_text(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: IPO ai_report metninde string replace yap.

    Body: {"admin_password": "...", "ipo_id": 47, "find": "lock-up", "replace": "satış yasağı süresi"}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ipo_id = payload.get("ipo_id")
    find = payload.get("find", "")
    replace = payload.get("replace", "")
    if not ipo_id or not find:
        return {"status": "error", "message": "ipo_id ve find gerekli"}

    from app.models.ipo import IPO
    result = await db.execute(select(IPO).where(IPO.id == int(ipo_id)))
    ipo = result.scalar_one_or_none()
    if not ipo or not ipo.ai_report:
        return {"status": "error", "message": "IPO veya ai_report bulunamadi"}

    import re
    # Case-insensitive replace, hem string hem JSON
    if isinstance(ipo.ai_report, str):
        old = ipo.ai_report
        # Case-insensitive replace
        pattern = re.compile(re.escape(find), re.IGNORECASE)
        new = pattern.sub(replace, old)
        count = old.count(find) + old.count(find.lower()) + old.count(find.upper()) + old.count(find.title())
        ipo.ai_report = new
        await db.commit()
        return {
            "status": "ok",
            "ipo_id": ipo.id,
            "company": ipo.company_name,
            "replaced_count": len(re.findall(pattern, old)),
            "old_length": len(old),
            "new_length": len(new),
        }
    return {"status": "error", "message": "ai_report formati beklenmedik"}


@app.post("/api/v1/admin/notify-new-ipo")
@limiter.limit("10/minute")
async def admin_notify_new_ipo(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: Belirli bir IPO için "Yeni Halka Arz" push bildirimini manuel tetikle.

    Body: {"admin_password": "...", "ipo_id": 47}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ipo_id = payload.get("ipo_id")
    if not ipo_id:
        return {"status": "error", "message": "ipo_id gerekli"}

    from app.models.ipo import IPO
    from app.services.notification import NotificationService

    result = await db.execute(select(IPO).where(IPO.id == int(ipo_id)))
    ipo = result.scalar_one_or_none()
    if not ipo:
        return {"status": "error", "message": f"IPO {ipo_id} bulunamadi"}

    notif_service = NotificationService(db)
    sent = await notif_service.notify_new_ipo(ipo)
    await db.commit()

    return {
        "status": "ok",
        "ipo_id": ipo.id,
        "company": ipo.company_name,
        "sent_count": sent,
    }


@app.post("/api/v1/admin/reset-spk-flags")
@limiter.limit("10/minute")
async def admin_reset_spk_flags(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: Son 48 saatteki SPK başvurularının tweet/bildirim flag'lerini sıfırla."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from datetime import timedelta
    cutoff = datetime.now() - timedelta(hours=48)
    result = await db.execute(
        select(SPKApplication).where(
            SPKApplication.status == "pending",
            SPKApplication.created_at >= cutoff,
        )
    )
    reset_count = 0
    for app in result.scalars().all():
        app.notified = False
        app.tweeted = False
        reset_count += 1
    await db.commit()
    return {"status": "ok", "reset_count": reset_count}


@app.post("/api/v1/admin/delete-tweets")
@limiter.limit("10/minute")
async def admin_delete_tweets(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: Tweet'leri ID ile sil."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ids = payload.get("ids", [])
    if not ids:
        return {"status": "error", "message": "ids listesi gerekli"}

    deleted = 0
    for tid in ids:
        result = await db.execute(select(PendingTweet).where(PendingTweet.id == tid))
        tweet = result.scalar_one_or_none()
        if tweet:
            await db.delete(tweet)
            deleted += 1
    await db.commit()
    return {"status": "ok", "deleted": deleted}


@app.post("/api/v1/admin/reparse-telegram-news")
@limiter.limit("5/minute")
async def admin_reparse_telegram_news(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: telegram_news kayitlarinin AI puan/ozetini yeniden uret.

    Buyback (pay geri alimi) ise process_buyback parse + buyback_score_and_summary
    ile deterministik skor verir. Diger durumlarda analyze_news cagrilir.

    Body: {
      'admin_password': '...',
      'tickers': ['GLYHO','ENERY'],   // veya 'ticker': 'GLYHO'
      'only_null': true,              // sadece AI null olanlar (default true)
      'days': 7                       // son N gun
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.telegram_news import TelegramNews
    from app.services.buyback_processor import is_buyback, parse_buyback_today, buyback_score_and_summary
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure

    tickers = payload.get("tickers") or ([payload["ticker"]] if payload.get("ticker") else [])
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]
    only_null = bool(payload.get("only_null", True))
    days = int(payload.get("days", 7))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    q = select(TelegramNews).where(TelegramNews.message_date >= cutoff)
    if tickers:
        q = q.where(TelegramNews.ticker.in_(tickers))
    if only_null:
        q = q.where(TelegramNews.ai_score.is_(None))
    rows = (await db.execute(q.order_by(TelegramNews.id.desc()).limit(50))).scalars().all()

    updated = 0
    skipped = 0
    detail: list[dict] = []
    for row in rows:
        try:
            ticker = (row.ticker or "").upper()
            title = row.parsed_title or ""
            if not row.kap_url:
                skipped += 1
                continue
            # KAP body'sini fetch et
            disc = await fetch_kap_disclosure(row.kap_url)
            body = (disc or {}).get("full_text") or ""
            if not body:
                skipped += 1
                continue
            score = None
            summary = None
            # Buyback ise deterministik
            if is_buyback(title) or "geri al" in title.lower():
                parsed = parse_buyback_today(body)
                if parsed and parsed.get("lot"):
                    lot = parsed["lot"]
                    price_avg = parsed.get("price_avg") or 0
                    if not price_avg and parsed.get("price_low") and parsed.get("price_high"):
                        price_avg = (parsed["price_low"] + parsed["price_high"]) / 2
                    total_tl = lot * price_avg if price_avg else 0
                    parsed["total_tl"] = total_tl
                    score, summary = buyback_score_and_summary(parsed, ticker)
            # Hala None ise AI scorer cagir
            if score is None:
                from app.services.ai_news_scorer import analyze_news
                ai_result = await analyze_news(ticker, body[:5000], matriks_id=None)
                score = ai_result.get("score")
                summary = ai_result.get("summary")
            if score is None:
                skipped += 1
                detail.append({"id": row.id, "ticker": ticker, "skip": "ai_fail"})
                continue
            row.ai_score = score
            row.ai_summary = summary
            updated += 1
            detail.append({
                "id": row.id, "ticker": ticker, "score": score,
                "summary": (summary or "")[:80],
            })
        except Exception as e:
            detail.append({"id": row.id, "ticker": row.ticker, "error": str(e)[:150]})
    await db.commit()
    return {"status": "ok", "fetched": len(rows), "updated": updated, "skipped": skipped, "detail": detail}


@app.post("/api/v1/admin/set-block-trade")
@limiter.limit("10/minute")
async def admin_set_block_trade(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: BlockTrade kaydinin alanlarini manuel duzelt.

    Body: {
      'admin_password': '...',
      'id': 151,
      'broker': 'Tera Yatırım Menkul Değerler A.Ş.',
      'counterparties': 'Kevok Gayrimenkul İnşaat A.Ş.',
      'lot_amount': 32300000,
      'cost_price': 24.0,
      'transaction_type': 'satis',
      'transaction_date': '2026-05-13'
    }
    Sadece verilen alanlar guncellenir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.block_trade import BlockTrade
    _id = int(payload.get("id") or 0)
    if not _id:
        raise HTTPException(status_code=400, detail="id gerekli")
    row = (await db.execute(select(BlockTrade).where(BlockTrade.id == _id))).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Kayit bulunamadi")
    changes = {}
    for fld in ("broker", "counterparties"):
        if fld in payload and payload[fld] is not None:
            setattr(row, fld, str(payload[fld])[:1000 if fld == "counterparties" else 255])
            changes[fld] = getattr(row, fld)
    if "lot_amount" in payload and payload["lot_amount"] is not None:
        row.lot_amount = int(payload["lot_amount"]); changes["lot_amount"] = row.lot_amount
    if "cost_price" in payload and payload["cost_price"] is not None:
        row.cost_price = float(payload["cost_price"]); changes["cost_price"] = row.cost_price
    if "transaction_type" in payload and payload["transaction_type"] in ("alis", "satis"):
        row.transaction_type = payload["transaction_type"]; changes["transaction_type"] = row.transaction_type
    if "transaction_date" in payload and isinstance(payload["transaction_date"], str):
        try:
            row.transaction_date = date.fromisoformat(payload["transaction_date"])
            changes["transaction_date"] = str(row.transaction_date)
        except ValueError:
            pass
    await db.commit()
    return {"status": "ok", "id": row.id, "ticker": row.ticker, "changes": changes}


@app.post("/api/v1/admin/process-kap-as-block-trade")
@limiter.limit("5/minute")
async def admin_process_kap_as_block_trade(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: KAP URL'sini block_trade processor'a goder, ilgili tum ticker'lar
    icin kayit olustur (multi-ticker destek). Tipe donusum gibi cokli hisse
    duyurularinda kullanilir.

    Body: {'admin_password': '...', 'kap_url': 'https://www.kap.org.tr/tr/Bildirim/1606871'}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    kap_url = (payload.get("kap_url") or "").strip()
    if not kap_url:
        raise HTTPException(status_code=400, detail="kap_url gerekli")
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.kap_category_processors import process_block_trade
    import re as _re
    disc = await fetch_kap_disclosure(kap_url)
    if not disc or not disc.get("full_text"):
        return {"status": "error", "message": "KAP fetch basarisiz"}
    body = disc["full_text"]
    # İlgili Şirketler'i bul
    m = _re.search(
        r"(?:İlgili\s*Şirketler?|Hisse\s*Kodu|Sembol|Pay\s*Kodu)\s*[:\|]?\s*([A-Z, ]+?)(?:\n|$)",
        body, _re.IGNORECASE,
    )
    # Once payload'dan elle gelen ticker(lar) varsa onu kullan
    forced_tickers = payload.get("tickers") or ([payload["ticker"]] if payload.get("ticker") else [])
    if forced_tickers:
        tickers = [str(t).strip().upper() for t in forced_tickers if str(t).strip()]
    elif m:
        tickers = [t.strip().upper() for t in m.group(1).split(",") if t.strip()]
    else:
        return {"status": "error", "message": "İlgili Şirketler bulunamadi (ticker/tickers parametresi gerekli)"}
    if not tickers:
        return {"status": "error", "message": "ticker yok"}
    # İlk ticker ile çağır — process_block_trade multi-ticker destekli, hepsini ekler
    # Eger zorla ticker verildiyse related_tickers'a da set edip body'yi onunla zenginlestir
    if forced_tickers and not m:
        # Body'ye "İlgili Şirketler" satiri injekt et (process_block_trade ticker validation'i icin)
        body = f"İlgili Şirketler: {', '.join(tickers)}\n\n{body}"
    result = await process_block_trade(
        db, disclosure_id=0, ticker=tickers[0],
        company_name=None, title="Multi-Ticker Block Trade",
        body=body, kap_url=kap_url, published_at=datetime.now(timezone.utc),
    )
    await db.commit()
    return {"status": "ok", "tickers_found": tickers, "kap_url": kap_url}


@app.post("/api/v1/admin/delete-block-trade")
@limiter.limit("10/minute")
async def admin_delete_block_trade(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: belirli block_trade kaydini sil. Yanlis ticker / yanlis kategorize
    edilmis kayitlari temizlemek icin.

    Body: {'admin_password': '...', 'id': 140}   veya
          {'admin_password': '...', 'ids': [140, 142]}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.block_trade import BlockTrade
    ids = payload.get("ids")
    if not ids and payload.get("id"):
        ids = [int(payload["id"])]
    if not ids:
        raise HTTPException(status_code=400, detail="id veya ids gerekli")
    deleted = []
    for _id in ids:
        row = (await db.execute(select(BlockTrade).where(BlockTrade.id == int(_id)))).scalar_one_or_none()
        if row:
            deleted.append({"id": row.id, "ticker": row.ticker, "kap_url": row.kap_url})
            await db.delete(row)
    await db.commit()
    return {"status": "ok", "deleted": len(deleted), "detail": deleted}


@app.post("/api/v1/admin/reparse-block-trades")
@limiter.limit("3/minute")
async def admin_reparse_block_trades(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: block_trades tablosunda lot_amount VEYA counterparties NULL olan
    kayitlari KAP'tan tekrar fetch edip regex+AI ile re-parse et.

    Body: {
      'admin_password': '...',
      'days': 30 (opsiyonel),
      'limit': 50 (opsiyonel),
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.block_trade import BlockTrade
    from app.services.kap_category_processors import _parse_block_trade_regex, _call_gemini, _BT_PROMPT
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure

    days = int(payload.get("days", 30))
    limit = min(int(payload.get("limit", 50)), 100)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Junk degerleri de yakalanmali — "Listesi" gibi placeholder kelimeler.
    _JUNK_CP_SET = ("listesi", "liste", "yok", "-", "none", "null", "n/a")
    junk_filter = sa_func.lower(BlockTrade.counterparties).in_(_JUNK_CP_SET)
    q = select(BlockTrade).where(
        BlockTrade.created_at >= cutoff,
        or_(
            BlockTrade.lot_amount.is_(None),
            BlockTrade.counterparties.is_(None),
            sa_func.length(BlockTrade.counterparties) < 4,
            junk_filter,
        ),
    ).order_by(BlockTrade.id.desc()).limit(limit)
    rows = (await db.execute(q)).scalars().all()

    updated = 0
    skipped = 0
    detail: list[dict] = []
    for row in rows:
        try:
            if not row.kap_url:
                skipped += 1
                continue
            disc = await fetch_kap_disclosure(row.kap_url)
            if not disc or not disc.get("full_text"):
                skipped += 1
                continue
            body = disc["full_text"]
            # AI parse + regex fallback
            parsed = await _call_gemini(_BT_PROMPT.format(
                ticker=row.ticker, title="", body=body[:3500],
            )) or {}
            regex_parsed = _parse_block_trade_regex(body)
            for k in ("transaction_type", "transaction_date", "broker", "counterparties", "lot_amount", "cost_price"):
                if not parsed.get(k) and regex_parsed.get(k):
                    parsed[k] = regex_parsed[k]

            changed = False
            # counterparties: hem NULL hem de "Listesi" gibi placeholder degerler icin overwrite
            _cp_is_junk = (
                not row.counterparties
                or (row.counterparties or "").strip().lower() in ("listesi", "liste", "yok", "-", "none", "null", "n/a")
                or len((row.counterparties or "").strip()) < 4
            )
            if not row.lot_amount and isinstance(parsed.get("lot_amount"), (int, float)):
                row.lot_amount = int(parsed["lot_amount"])
                changed = True
            if _cp_is_junk and parsed.get("counterparties"):
                row.counterparties = parsed["counterparties"][:1000]
                changed = True
            if not row.broker and parsed.get("broker"):
                row.broker = parsed["broker"][:255]
                changed = True
            if not row.cost_price and isinstance(parsed.get("cost_price"), (int, float)):
                row.cost_price = float(parsed["cost_price"])
                changed = True
            if changed:
                updated += 1
                detail.append({
                    "id": row.id, "ticker": row.ticker,
                    "lot": row.lot_amount, "broker": (row.broker or "")[:50],
                    "counterparties": (row.counterparties or "")[:60],
                })
            else:
                skipped += 1
        except Exception as e:
            detail.append({"id": row.id, "ticker": row.ticker, "error": str(e)[:200]})
    await db.commit()
    return {"status": "ok", "updated": updated, "skipped": skipped, "fetched": len(rows), "detail": detail}


@app.post("/api/v1/admin/set-business-deal-amount")
@limiter.limit("10/minute")
async def admin_set_business_deal_amount(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: Bir BusinessDeal kaydinin tutar/para birimini manuel olarak duzelt.

    Body: {
      'admin_password': '...',
      'deal_id': 61,                 # zorunlu
      'amount_original': 17800000,   # zorunlu (kaynak para biriminde)
      'currency': 'USD',             # TRY|USD|EUR|GBP
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.business_deal import BusinessDeal
    from app.services.business_deal_processor import get_exchange_rate

    deal_id = int(payload.get("deal_id") or 0)
    amount_original = float(payload.get("amount_original") or 0)
    currency = (payload.get("currency") or "TRY").upper()
    if currency not in ("TRY", "USD", "EUR", "GBP"):
        raise HTTPException(status_code=400, detail="Gecersiz currency (TRY|USD|EUR|GBP)")
    if not deal_id or amount_original <= 0:
        raise HTTPException(status_code=400, detail="deal_id + amount_original gerekli")

    row = (await db.execute(select(BusinessDeal).where(BusinessDeal.id == deal_id))).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Deal bulunamadi")

    if currency == "TRY":
        amount_try = amount_original
        rate_used = 1.0
        rate_date = row.deal_date or date.today()
    else:
        rate_used, rate_date = await get_exchange_rate(currency)
        amount_try = (amount_original * rate_used) if rate_used else None

    old_try = row.amount_try
    row.amount_original = amount_original
    row.currency = currency
    row.amount_try = amount_try
    row.exchange_rate_used = rate_used
    row.rate_date = rate_date
    await db.commit()
    return {
        "status": "ok", "id": row.id, "ticker": row.ticker,
        "old_try": old_try, "new_try": amount_try,
        "rate": rate_used, "currency": currency,
    }


@app.post("/api/v1/admin/reparse-business-deals")
@limiter.limit("3/minute")
async def admin_reparse_business_deals(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: business_deals tablosundaki kayitlari (veya belirli ticker'i) yeniden parse et.

    Yeni regex sirasiyla amount yeniden cikarilir, KAP body'si tekrar fetch edilir.

    Body: {
      'admin_password': '...',
      'ticker': 'PSGYO' (opsiyonel — yoksa son N gun hepsi),
      'days': 30 (opsiyonel, default 30)
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.business_deal import BusinessDeal
    from app.services.business_deal_processor import (
        ai_parse_business_deal, get_exchange_rate,
    )
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure as _fetch_kap

    ticker = (payload.get("ticker") or "").strip().upper() or None
    days = int(payload.get("days", 365))  # default 365 — hepsini kontrol et
    offset = int(payload.get("offset", 0))
    limit = min(int(payload.get("limit", 100)), 200)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    q = select(BusinessDeal).where(BusinessDeal.created_at >= cutoff)
    if ticker:
        q = q.where(BusinessDeal.ticker == ticker)
    rows = (await db.execute(
        q.order_by(BusinessDeal.id.desc()).offset(offset).limit(limit)
    )).scalars().all()

    from app.models.kap_all_disclosure import KapAllDisclosure
    updated = 0
    skipped = 0
    unchanged = 0
    detail: list[dict] = []
    for row in rows:
        try:
            body = ""
            # 1) KAP URL'den canli fetch (en taze veri)
            if row.kap_url:
                try:
                    disc = await _fetch_kap(row.kap_url)
                    if disc and disc.get("full_text"):
                        body = disc["full_text"]
                except Exception:
                    pass
            # 2) Fallback: kap_all_disclosures.body (DB'de cached)
            if not body and row.kap_disclosure_id:
                ka = (await db.execute(
                    select(KapAllDisclosure).where(
                        KapAllDisclosure.id == row.kap_disclosure_id
                    )
                )).scalar_one_or_none()
                if ka and ka.body:
                    body = ka.body
            # 3) Fallback: kap_url ile kap_all_disclosures'a bak
            if not body and row.kap_url:
                ka = (await db.execute(
                    select(KapAllDisclosure).where(
                        KapAllDisclosure.kap_url == row.kap_url
                    ).limit(1)
                )).scalar_one_or_none()
                if ka and ka.body:
                    body = ka.body
            if not body:
                skipped += 1
                detail.append({"id": row.id, "ticker": row.ticker, "skip_reason": "body_yok"})
                continue
            parsed = await ai_parse_business_deal(row.ticker, row.title or "", body)
            amount_orig = parsed.get("amount_original")
            cur = parsed.get("currency") or "TRY"
            if not amount_orig:
                skipped += 1
                continue
            if cur == "TRY":
                amount_try = amount_orig
                rate_used = 1.0
                rate_date = row.deal_date or date.today()
            else:
                rate_used, rate_date = await get_exchange_rate(cur)
                amount_try = (amount_orig * rate_used) if rate_used else None
            old_try = row.amount_try or 0
            # Sadece DEGER FARKLI ise update (gereksiz commit'i onle)
            if abs((old_try or 0) - (amount_try or 0)) < 0.01 and row.currency == cur:
                unchanged += 1
                continue
            row.amount_original = amount_orig
            row.currency = cur
            row.amount_try = amount_try
            row.exchange_rate_used = rate_used
            row.rate_date = rate_date
            updated += 1
            detail.append({
                "id": row.id,
                "ticker": row.ticker, "kap_id": row.kap_disclosure_id,
                "old_try": old_try, "new_try": amount_try,
                "orig": amount_orig, "currency": cur,
                "kap_url": row.kap_url,
            })
        except Exception as e:
            detail.append({"id": row.id, "ticker": row.ticker, "error": str(e)[:200]})
    await db.commit()
    return {
        "status": "ok", "updated": updated, "skipped": skipped,
        "unchanged": unchanged, "fetched": len(rows),
        "next_offset": offset + len(rows) if len(rows) == limit else None,
        "detail": detail,
    }


@app.post("/api/v1/admin/cleanup-non-csv-cautious")
@limiter.limit("3/minute")
async def admin_cleanup_non_csv_cautious(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: BIST resmi CSV disindan gelen cautious_stocks kayitlarini sil.

    Sadece kaynak resmi CSV'den olsun (source='bist_csv' veya benzeri).
    'kap_ai_parse', 'halkarz_tedbirli' gibi yanlis kaynaklar temizlenir.

    Body: {'admin_password': '...'}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.cautious_stock import CautiousStock
    rows = (await db.execute(
        select(CautiousStock).where(
            CautiousStock.source.notin_(["bist_csv", "manual_import"]),
        )
    )).scalars().all()

    deleted_by_source: dict[str, int] = {}
    for r in rows:
        src = r.source or "null"
        deleted_by_source[src] = deleted_by_source.get(src, 0) + 1
        await db.delete(r)

    await db.commit()
    return {
        "status": "ok",
        "deleted_total": sum(deleted_by_source.values()),
        "by_source": deleted_by_source,
    }


@app.get("/api/v1/stock-market/{ticker}")
@limiter.limit("30/minute")
async def get_stock_market(
    request: Request,
    ticker: str,
    db: AsyncSession = Depends(get_db),
):
    """Hisse pazar bilgisi — frontend/test icin."""
    from app.models.stock_market import StockMarket
    row = (await db.execute(
        select(StockMarket).where(StockMarket.ticker == ticker.upper())
    )).scalar_one_or_none()
    if not row:
        return {"ticker": ticker.upper(), "found": False}
    return {
        "ticker": row.ticker,
        "company_name": row.company_name,
        "market_segment": row.market_segment,
        "indexes": row.indexes,
        "found": True,
    }


@app.post("/api/v1/admin/sync-bist-markets-now")
@limiter.limit("3/minute")
async def admin_sync_bist_markets_now(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: BIST hisse pazar segmenti CSV'sini SIMDI senkronize et.
    Kaynak: https://borsaistanbul.com/datum/hisse_endeks_ds.csv
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.scrapers.bist_market_segment_scraper import sync_bist_markets
        stats = await sync_bist_markets(db)
        return {"status": "ok", "stats": stats}
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "error": str(e)[:500],
            "type": type(e).__name__,
            "traceback": traceback.format_exc()[:2000],
        }


@app.post("/api/v1/admin/sync-bist-tedbir-now")
@limiter.limit("5/minute")
async def admin_sync_bist_tedbir_now(
    request: Request,
    payload: dict = Body(...),
):
    """Admin: BIST resmi tedbir CSV'sini SIMDI senkronize et (cron beklemeden)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.scrapers.bist_tedbir_csv_scraper import sync_bist_tedbir
    stats = await sync_bist_tedbir()
    return {"status": "ok", "stats": stats}


@app.post("/api/v1/admin/cleanup-spk-bulten-duplicates")
@limiter.limit("3/minute")
async def admin_cleanup_spk_bulten_duplicates(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: SPK bulten analiz tweet'leri pespese duplicate atildiysa temizle.

    Her bulten icin EN ESKI kaydi (id en kucuk) tutar, digerlerini siler.
    PendingTweet text icinde bulten no'su (orn '2026/29') gectigi icin
    text uzerinden grup olusturulur.

    Body: {'admin_password': '...'}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    import re as _re
    # tum spk bulten analiz tweet'lerini cek
    rows = (await db.execute(
        select(PendingTweet).where(
            PendingTweet.source.in_(["tweet_spk_bulletin_analysis", "spk_bulten_analiz_flag"]),
        ).order_by(PendingTweet.id.asc())
    )).scalars().all()

    # Bulten numarasina gore grup yap (text icindeki YYYY/NN paterninden)
    groups: dict[str, list] = {}
    for r in rows:
        m = _re.search(r"(\d{4})/(\d{1,3})", r.text or "")
        if not m:
            continue
        key = f"{m.group(1)}/{m.group(2)}"
        groups.setdefault(key, []).append(r)

    deleted = 0
    kept = 0
    detail = []
    for bno, lst in groups.items():
        if len(lst) <= 1:
            kept += 1
            continue
        # Ilk kaydi tut, gerisini sil
        keep_row = lst[0]
        kept += 1
        for r in lst[1:]:
            await db.delete(r)
            deleted += 1
        detail.append({"bulletin_no": bno, "kept_id": keep_row.id, "deleted_count": len(lst) - 1})

    await db.commit()
    return {"status": "ok", "deleted": deleted, "kept": kept, "detail": detail}


@app.post("/api/v1/admin/cleanup-spk-spam")
@limiter.limit("5/minute")
async def admin_cleanup_spk_spam(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: Eski SPK başvuru spam tweet'lerini DB'den sil (son 6 saat hariç)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    hours = payload.get("keep_hours", 6)
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    result = await db.execute(
        select(PendingTweet).where(
            PendingTweet.source == "tweet_spk_application",
            PendingTweet.sent_at < cutoff,
        )
    )
    deleted = 0
    for tweet in result.scalars().all():
        await db.delete(tweet)
        deleted += 1
    await db.commit()
    return {"status": "ok", "deleted": deleted}


@app.post("/api/v1/admin/retweet-spk-apps")
@limiter.limit("5/minute")
async def admin_retweet_spk_apps(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: Belirli SPK başvurularını sil ve scraper'ı tetikle — yeniden tweet atılır."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    company_names = payload.get("company_names", [])
    if not company_names:
        # Son 48 saatteki tüm kayıtları sil
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=48)
        result = await db.execute(
            select(SPKApplication).where(
                SPKApplication.created_at >= cutoff,
                SPKApplication.status == "pending",
            )
        )
        company_names = [app.company_name for app in result.scalars().all()]

    deleted = 0
    for name in company_names:
        result = await db.execute(
            select(SPKApplication).where(SPKApplication.company_name == name)
        )
        app = result.scalar_one_or_none()
        if app:
            await db.delete(app)
            deleted += 1
    await db.commit()

    # Scraper'ı tetikle — silinen kayıtlar yeniden eklenecek ve tweet atılacak
    try:
        from app.scheduler import scrape_spk
        await scrape_spk()
    except Exception as e:
        return {"status": "partial", "deleted": deleted, "scraper_error": str(e)[:200]}

    return {"status": "ok", "deleted": deleted, "message": f"{deleted} kayıt silindi ve scraper tetiklendi"}


@app.post("/api/v1/admin/trigger-spk-scraper")
@limiter.limit("5/minute")
async def admin_trigger_spk_scraper(request: Request, payload: dict):
    """Admin: SPK onay listesi scraper'ını manuel tetikle."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.scheduler import scrape_spk
        await scrape_spk()
        return {"status": "ok", "message": "SPK onay scraper tamamlandi"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/trigger-spk-check")
@limiter.limit("30/minute")
async def admin_trigger_spk_check(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: SPK bulten monitor'u manuel tetikle — detayli debug."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    debug_mode = payload.get("debug", False)

    if not debug_mode:
        try:
            from app.scrapers.spk_bulletin_scraper import check_spk_bulletins
            await check_spk_bulletins()
            return {"status": "ok", "message": "SPK bulten kontrolu tamamlandi"}
        except Exception as e:
            import traceback
            resp = {"status": "error", "message": str(e)[:500]}
            if not settings.is_production:
                resp["traceback"] = traceback.format_exc()[-1000:]
            return resp

    # Debug mode — adim adim isle ve sonuclari dondur
    try:
        from app.scrapers.spk_bulletin_scraper import (
            SPKBulletinScraper, _get_last_bulletin_no, is_newer,
            bulletin_no_str, extract_text_from_pdf, extract_tables_from_pdf,
            find_ilk_halka_arz_table, parse_bulletin_no,
        )
        from datetime import date as _date

        debug_info = {"steps": []}

        scraper = SPKBulletinScraper()
        try:
            # 1. Son numarayi al — override varsa kullan
            override_no = payload.get("force_last_no")  # orn: "2026/8"
            if override_no:
                last_no = parse_bulletin_no(override_no)
                debug_info["last_bulletin_no"] = override_no + " (override)"
            else:
                last_no = await _get_last_bulletin_no(db)
                debug_info["last_bulletin_no"] = bulletin_no_str(*last_no) if last_no else None
            debug_info["steps"].append(f"1. Son bulten: {debug_info['last_bulletin_no']}")

            # 2. Bulten listesini al — force_last_no'dan yil cikar veya mevcut yil
            if last_no:
                search_year = last_no[0]  # force_last_no'nun yili
            else:
                search_year = _date.today().year
            bulletins = await scraper.fetch_bulletin_list(year=search_year)
            # Mevcut yil farkliysa onu da ekle
            current_year = _date.today().year
            if search_year != current_year:
                current_bulletins = await scraper.fetch_bulletin_list(year=current_year)
                bulletins.extend(current_bulletins)
            debug_info["total_bulletins"] = len(bulletins)
            debug_info["bulletin_list"] = [
                {"no": bulletin_no_str(*b["bulletin_no"]), "url": b["pdf_url"][:80]}
                for b in bulletins[-5:]  # Son 5 bulten
            ]
            debug_info["steps"].append(f"2. {len(bulletins)} bulten listelendi")

            # 3. Yeni bultenleri filtrele
            new_bulletins = [b for b in bulletins if is_newer(b["bulletin_no"], last_no)]
            debug_info["new_bulletins"] = [bulletin_no_str(*b["bulletin_no"]) for b in new_bulletins]
            debug_info["steps"].append(f"3. {len(new_bulletins)} yeni bulten: {debug_info['new_bulletins']}")

            if not new_bulletins:
                debug_info["steps"].append("SONUC: Yeni bulten yok")
                return {"status": "ok", "debug": debug_info}

            # 4. Ilk yeni bulteni isle (debug)
            bulletin = sorted(new_bulletins, key=lambda x: x["bulletin_no"])[0]
            bno = bulletin["bulletin_no"]
            pdf_url = bulletin["pdf_url"]
            debug_info["processing_bulletin"] = bulletin_no_str(*bno)
            debug_info["pdf_url"] = pdf_url

            # PDF indir
            pdf_bytes = await scraper.download_pdf(pdf_url)
            if not pdf_bytes:
                debug_info["steps"].append("4. PDF indirilemedi!")
                return {"status": "error", "debug": debug_info}

            debug_info["pdf_size"] = len(pdf_bytes)
            debug_info["steps"].append(f"4. PDF indirildi: {len(pdf_bytes)} bytes")

            # PDF parse
            full_text = extract_text_from_pdf(pdf_bytes)
            tables = extract_tables_from_pdf(pdf_bytes)
            debug_info["text_length"] = len(full_text)
            debug_info["table_count"] = len(tables)
            debug_info["text_preview"] = full_text[:500] if full_text else "BOS"
            debug_info["steps"].append(f"5. PDF parse: {len(full_text)} char, {len(tables)} tablo")

            # Tablo detayi
            for i, table in enumerate(tables):
                rows = len(table) if table else 0
                header_preview = ""
                if table and len(table) > 0:
                    header_preview = " | ".join(str(c or "")[:20] for c in table[0])
                debug_info[f"table_{i}"] = {
                    "rows": rows,
                    "header": header_preview[:200],
                }

            # Table 1 detayli satirlar (Ilk Halka Arz tablosu olmasi beklenir)
            if len(tables) > 1 and tables[1]:
                debug_info["table_1_rows"] = []
                for ri, row in enumerate(tables[1]):
                    row_str = [str(c or "").replace("\n", "\\n")[:50] for c in row]
                    debug_info["table_1_rows"].append(f"row{ri}: {row_str}")

            # Ilk halka arz tablosu
            ipo_approvals = find_ilk_halka_arz_table(tables, full_text)
            debug_info["ipo_approvals"] = [
                {"company": a["company_name"], "price": str(a.get("sale_price"))}
                for a in ipo_approvals
            ]
            debug_info["steps"].append(f"6. {len(ipo_approvals)} halka arz tespit edildi")

            return {"status": "ok", "debug": debug_info}

        finally:
            await scraper.close()

    except Exception as e:
        import traceback
        resp = {"status": "error", "message": str(e)[:500]}
        if not settings.is_production:
            resp["traceback"] = traceback.format_exc()[-1500:]
        return resp


@app.post("/api/v1/admin/reset-spk-bulletin-no")
@limiter.limit("3/minute")
async def admin_reset_spk_bulletin_no(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: SPK son bulten numarasini geri al (yeniden tara)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    new_no = payload.get("bulletin_no")  # orn: "2026/8"
    if not new_no:
        raise HTTPException(status_code=400, detail="bulletin_no gerekli (orn: 2026/8)")

    from app.models.scraper_state import ScraperState
    result = await db.execute(
        select(ScraperState).where(ScraperState.key == "spk_last_bulletin_no")
    )
    state = result.scalar_one_or_none()
    old_no = state.value if state else None

    if state:
        state.value = new_no
    else:
        state = ScraperState(key="spk_last_bulletin_no", value=new_no)
        db.add(state)

    await db.commit()
    return {
        "status": "ok",
        "message": f"SPK bulten no degistirildi: {old_no} → {new_no}",
        "old": old_no,
        "new": new_no,
    }


@app.post("/api/v1/admin/trigger-bulletin-analysis-tweet")
@limiter.limit("3/minute")
async def admin_trigger_bulletin_analysis_tweet(request: Request, payload: dict):
    """Admin: Belirtilen bültenin AI analiz tweetini manuel tetikle."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    bulletin_no = payload.get("bulletin_no")  # orn: "2026/18"
    if not bulletin_no:
        raise HTTPException(status_code=400, detail="bulletin_no gerekli (orn: 2026/18)")

    try:
        from app.scrapers.spk_bulletin_scraper import SPKBulletinScraper, parse_bulletin_no
        from app.scrapers.spk_bulletin_scraper import extract_text_from_pdf, extract_tables_from_pdf
        from app.scrapers.spk_bulletin_scraper import format_tables_for_analysis

        bno = parse_bulletin_no(bulletin_no)
        if not bno:
            raise HTTPException(status_code=400, detail="Gecersiz bulten no formati")

        scraper = SPKBulletinScraper()
        try:
            bulletins = await scraper.fetch_bulletin_list(year=bno[0])
            target = next((b for b in bulletins if b["bulletin_no"] == bno), None)
            if not target:
                return {"status": "error", "message": f"Bulten {bulletin_no} sayfada bulunamadi"}

            pdf_bytes = await scraper.download_pdf(target["pdf_url"])
            if not pdf_bytes:
                return {"status": "error", "message": "PDF indirilemedi"}

            full_text = extract_text_from_pdf(pdf_bytes)
            tables = extract_tables_from_pdf(pdf_bytes)
            bulletin_text = format_tables_for_analysis(tables, full_text)

            from app.services.twitter_service import tweet_spk_bulletin_analysis
            ok = tweet_spk_bulletin_analysis(bulletin_text, bulletin_no)
            return {"status": "ok" if ok else "error", "tweet_sent": ok}
        finally:
            await scraper.close()
    except HTTPException:
        raise
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


# -------------------------------------------------------
# CUZDAN (WALLET) ENDPOINTS
# -------------------------------------------------------

def _get_wallet_cooldown(user: User) -> int:
    """Kalan cooldown saniyesini hesapla."""
    if not user.last_ad_watched_at:
        return 0
    now = datetime.now(timezone.utc)
    diff = (now - user.last_ad_watched_at).total_seconds()
    remaining = max(0, WALLET_COOLDOWN_SECONDS - int(diff))
    return remaining


def _check_daily_reset(user: User):
    """Gun degismisse reklam sayacini sifirla."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if user.ads_reset_date != today:
        user.daily_ads_watched = 0
        user.last_ad_watched_at = None
        user.ads_reset_date = today


@app.get("/api/v1/users/{device_id}/wallet", response_model=WalletBalanceOut)
@limiter.limit("30/minute")
async def get_wallet(request: Request, device_id: str = Depends(_validate_device_id_param), db: AsyncSession = Depends(get_db)):
    """Kullanicinin cuzdan bakiyesini ve reklam durumunu getirir."""
    result = await db.execute(select(User).where(User.device_id == device_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    _check_daily_reset(user)
    await db.flush()  # Gunluk reset degisikligini DB'ye yaz
    cooldown = _get_wallet_cooldown(user)

    return WalletBalanceOut(
        balance=user.wallet_balance or 0.0,
        daily_ads_watched=user.daily_ads_watched or 0,
        max_daily_ads=WALLET_MAX_DAILY_ADS,
        cooldown_remaining=cooldown,
        can_watch_ad=cooldown == 0 and (user.daily_ads_watched or 0) < WALLET_MAX_DAILY_ADS,
    )


@app.post("/api/v1/users/{device_id}/wallet/daily-checkin", response_model=WalletBalanceOut)
@limiter.limit("10/minute")
async def wallet_daily_checkin(
    request: Request,
    device_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Gunluk giris puani — her gun 1 puan, gunde 1 kere."""
    result = await db.execute(
        select(User).where(User.device_id == device_id).with_for_update()
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    _check_daily_reset(user)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if user.last_daily_checkin == today_str:
        # Bugun zaten giris yapilmis — sadece bakiye don
        cooldown = _get_wallet_cooldown(user)
        return WalletBalanceOut(
            balance=user.wallet_balance or 0.0,
            daily_ads_watched=user.daily_ads_watched or 0,
            max_daily_ads=WALLET_MAX_DAILY_ADS,
            cooldown_remaining=cooldown,
            can_watch_ad=cooldown == 0 and (user.daily_ads_watched or 0) < WALLET_MAX_DAILY_ADS,
        )

    # 1 puan ekle
    user.wallet_balance = (user.wallet_balance or 0.0) + 1.0
    user.last_daily_checkin = today_str

    tx = WalletTransaction(
        user_id=user.id,
        amount=1.0,
        tx_type="daily_checkin",
        description="Gunluk giris puani",
        balance_after=user.wallet_balance,
    )
    db.add(tx)
    await db.flush()

    cooldown = _get_wallet_cooldown(user)
    return WalletBalanceOut(
        balance=user.wallet_balance,
        daily_ads_watched=user.daily_ads_watched or 0,
        max_daily_ads=WALLET_MAX_DAILY_ADS,
        cooldown_remaining=cooldown,
        can_watch_ad=cooldown == 0 and (user.daily_ads_watched or 0) < WALLET_MAX_DAILY_ADS,
    )


@app.post("/api/v1/users/{device_id}/wallet/earn", response_model=WalletBalanceOut)
@limiter.limit("35/minute")
async def wallet_earn(
    request: Request,
    device_id: str = Depends(_validate_device_id_param),
    data: WalletEarnRequest = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Reklam izleme sonrasi puan kazanimi — sunucu tarafinda kontrol."""
    result = await db.execute(
        select(User).where(User.device_id == device_id).with_for_update()
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    _check_daily_reset(user)

    # Cooldown kontrolu
    cooldown = _get_wallet_cooldown(user)
    if cooldown > 0:
        raise HTTPException(
            status_code=429,
            detail=f"Reklam icin {cooldown} saniye beklemeniz gerekiyor."
        )

    # Gunluk limit kontrolu
    if (user.daily_ads_watched or 0) >= WALLET_MAX_DAILY_ADS:
        raise HTTPException(
            status_code=429,
            detail="Gunluk reklam izleme limitine ulastiniz."
        )

    # Puan ekle
    now = datetime.now(timezone.utc)
    user.wallet_balance = (user.wallet_balance or 0.0) + WALLET_REWARD_AMOUNT
    user.daily_ads_watched = (user.daily_ads_watched or 0) + 1
    user.last_ad_watched_at = now
    user.ads_reset_date = now.strftime("%Y-%m-%d")

    # Islem logu
    tx = WalletTransaction(
        user_id=user.id,
        amount=WALLET_REWARD_AMOUNT,
        tx_type="ad_reward",
        description=f"Reklam izleme #{user.daily_ads_watched}",
        balance_after=user.wallet_balance,
    )
    db.add(tx)
    await db.flush()

    # Milestone bildirimi (1, 10, 25, 50)
    try:
        if user.daily_ads_watched in (1, 10, 25, 50):
            from app.services.admin_telegram import notify_ad_milestone
            await notify_ad_milestone(
                user_id=user.id,
                device_id=user.device_id or "?",
                daily_count=user.daily_ads_watched,
                balance=user.wallet_balance or 0.0,
            )
    except Exception as _e:
        logger.warning("Reklam milestone bildirim hatasi: %s", _e)

    return WalletBalanceOut(
        balance=user.wallet_balance,
        daily_ads_watched=user.daily_ads_watched,
        max_daily_ads=WALLET_MAX_DAILY_ADS,
        cooldown_remaining=WALLET_COOLDOWN_SECONDS,
        can_watch_ad=False,  # Az once izledi, cooldown basladi
    )


@app.post("/api/v1/users/{device_id}/wallet/spend", response_model=WalletBalanceOut)
@limiter.limit("20/minute")
async def wallet_spend(
    request: Request,
    device_id: str = Depends(_validate_device_id_param),
    data: WalletSpendRequest = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Puan harcama — paket satin alma oncesi bakiye kontrolu + sunucu tarafindan fiyat dogrulama."""
    # Sunucu tarafinda fiyat tablosu — client manipulasyonuna karsi koruma
    SPEND_PRICES: dict[str, float] = {
        "spend_news": 650.0,
        "spend_ipo": 950.0,
        "spend_notif_reward": 150.0,
        "spend_notif": 150.0,
    }

    # spend_type dogrulama
    if data.spend_type not in SPEND_PRICES:
        raise HTTPException(status_code=400, detail=f"Gecersiz harcama tipi: {data.spend_type}")

    # Sunucu tarafindan belirlenen fiyati kullan — client amount'u yoksay
    server_amount = SPEND_PRICES[data.spend_type]

    result = await db.execute(
        select(User).where(User.device_id == device_id).with_for_update()
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    current_balance = user.wallet_balance or 0.0
    if current_balance < server_amount:
        raise HTTPException(
            status_code=400,
            detail=f"Yetersiz bakiye. Mevcut: {current_balance:.0f}, Gerekli: {server_amount:.0f}"
        )

    # Bakiye dus — sunucu fiyati ile
    user.wallet_balance = current_balance - server_amount

    # Islem logu — sunucu tarafindan dogrulanan fiyat
    tx = WalletTransaction(
        user_id=user.id,
        amount=-server_amount,
        tx_type=data.spend_type,
        description=data.description or f"Harcama: {data.spend_type}",
        balance_after=user.wallet_balance,
    )
    db.add(tx)
    await db.flush()

    # ---- Puan ile Haber Paketi alimi → UserSubscription olustur ----
    if data.spend_type == "spend_news":
        _now = datetime.now(timezone.utc)
        sub_result = await db.execute(
            select(UserSubscription).where(UserSubscription.user_id == user.id)
        )
        existing_sub = sub_result.scalar_one_or_none()
        if existing_sub:
            existing_sub.package = "ana_yildiz"
            existing_sub.is_active = True
            existing_sub.store = "wallet"
            existing_sub.product_id = "wallet_spend_news"
            existing_sub.started_at = _now
            existing_sub.expires_at = _now + timedelta(days=30)
        else:
            new_sub = UserSubscription(
                user_id=user.id,
                package="ana_yildiz",
                is_active=True,
                store="wallet",
                product_id="wallet_spend_news",
                revenue_cat_id=None,
                started_at=_now,
                expires_at=_now + timedelta(days=30),
            )
            db.add(new_sub)
        await db.flush()
        logger.info("Puan ile haber paketi aktif edildi: user_id=%s, 30 gun", user.id)

    # ---- Puan ile Halka Arz Paketi (3 Aylik) alimi → StockNotificationSubscription olustur ----
    if data.spend_type == "spend_ipo":
        _now = datetime.now(timezone.utc)
        # Mevcut aktif yillik/quarterly bundle var mi?
        existing_bundle = await db.execute(
            select(StockNotificationSubscription).where(
                and_(
                    StockNotificationSubscription.user_id == user.id,
                    StockNotificationSubscription.is_annual_bundle == True,
                    StockNotificationSubscription.is_active == True,
                )
            )
        )
        if not existing_bundle.scalar_one_or_none():
            quarterly_sub = StockNotificationSubscription(
                user_id=user.id,
                ipo_id=None,
                notification_type="all",
                is_annual_bundle=True,
                price_paid_tl=0,  # Puan ile alindi
                is_active=True,
                store="wallet",
                product_id="wallet_spend_ipo",
                purchased_at=_now,
                expires_at=_now + timedelta(days=90),  # 3 ay — artik suuresiz degil!
            )
            db.add(quarterly_sub)
            await db.flush()
            logger.info(
                "Puan ile 3 aylik halka arz paketi aktif edildi: user_id=%s, expires=%s",
                user.id, quarterly_sub.expires_at.isoformat(),
            )
        else:
            logger.info("Kullanicinin zaten aktif bundle'i var: user_id=%s", user.id)

    # ---- Puan ile Bildirim Paketi (tek hisse, 4 tip birden) — ATOMIK server-side olustur ----
    if data.spend_type in ("spend_notif", "spend_notif_reward"):
        _now = datetime.now(timezone.utc)

        # ipo_id varsa 4 bildirim tipini atomik olarak olustur
        if data.ipo_id:
            from app.models.ipo import IPO
            ipo_result = await db.execute(select(IPO).where(IPO.id == data.ipo_id))
            ipo_obj = ipo_result.scalar_one_or_none()

            if ipo_obj:
                notif_types = ["tavan_bozulma", "taban_acilma", "gunluk_acilis_kapanis", "yuzde_dusus"]
                created_count = 0

                for ntype in notif_types:
                    # Duplicate kontrolu
                    existing_check = await db.execute(
                        select(StockNotificationSubscription).where(
                            and_(
                                StockNotificationSubscription.user_id == user.id,
                                StockNotificationSubscription.ipo_id == data.ipo_id,
                                StockNotificationSubscription.notification_type == ntype,
                                StockNotificationSubscription.is_active == True,
                            )
                        )
                    )
                    if existing_check.scalar_one_or_none():
                        continue  # Zaten aktif, atla

                    new_notif = StockNotificationSubscription(
                        user_id=user.id,
                        ipo_id=data.ipo_id,
                        notification_type=ntype,
                        is_annual_bundle=False,
                        price_paid_tl=0,  # Puan ile alindi
                        is_active=True,
                        store="wallet",
                        product_id="wallet_spend_notif",
                        purchased_at=_now,
                        expires_at=_now + timedelta(days=90),
                    )
                    db.add(new_notif)
                    created_count += 1

                await db.flush()
                logger.info(
                    "Puan ile 4 bildirim ATOMIK olusturuldu: user_id=%s, ipo_id=%s, created=%d",
                    user.id, data.ipo_id, created_count,
                )
            else:
                logger.warning("spend_notif_reward: IPO bulunamadi, ipo_id=%s", data.ipo_id)
        else:
            # ipo_id yoksa sadece log (eski frontend uyumu)
            logger.info(
                "Puan ile bildirim paketi alindi (ipo_id yok): user_id=%s, desc=%s",
                user.id, data.description,
            )

    _check_daily_reset(user)
    cooldown = _get_wallet_cooldown(user)

    return WalletBalanceOut(
        balance=user.wallet_balance,
        daily_ads_watched=user.daily_ads_watched or 0,
        max_daily_ads=WALLET_MAX_DAILY_ADS,
        cooldown_remaining=cooldown,
        can_watch_ad=cooldown == 0 and (user.daily_ads_watched or 0) < WALLET_MAX_DAILY_ADS,
    )


@app.post("/api/v1/users/{device_id}/wallet/redeem-coupon", response_model=WalletBalanceOut)
@limiter.limit("3/minute")
async def wallet_redeem_coupon(
    request: Request,
    device_id: str,
    data: WalletCouponRequest,
    db: AsyncSession = Depends(get_db),
):
    """Kupon kodu ile puan ekleme — once DB, sonra hardcoded fallback."""
    result = await db.execute(
        select(User).where(User.device_id == device_id).with_for_update()
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    code = data.code.upper().strip()

    # --- 1) Dinamik kupon: DB'den ara ---
    now = datetime.now(timezone.utc)
    db_coupon_result = await db.execute(
        select(Coupon).where(
            Coupon.code == code,
            Coupon.is_active == True,
        ).with_for_update()
    )
    db_coupon = db_coupon_result.scalar_one_or_none()

    amount: float = 0.0
    is_db_coupon = False

    if db_coupon:
        # SKT kontrolu
        if db_coupon.expires_at and db_coupon.expires_at < now:
            raise HTTPException(status_code=400, detail="Bu kuponun suresi dolmus.")
        # Max kullanim kontrolu
        if db_coupon.uses_count >= db_coupon.max_uses:
            raise HTTPException(status_code=400, detail="Bu kupon kullanim limitine ulasmis.")
        amount = db_coupon.amount
        is_db_coupon = True
    elif code in WALLET_COUPONS:
        # --- 2) Hardcoded fallback ---
        amount = WALLET_COUPONS[code]
    else:
        raise HTTPException(status_code=400, detail="Gecersiz kupon kodu.")

    # Daha once kullanildi mi? (per-user duplicate check)
    existing = await db.execute(
        select(WalletTransaction).where(
            and_(
                WalletTransaction.user_id == user.id,
                WalletTransaction.tx_type == "coupon",
                WalletTransaction.description == f"Kupon: {code}",
            )
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Bu kuponu daha once kullandiniz.")

    # Puan ekle
    user.wallet_balance = (user.wallet_balance or 0.0) + amount

    # DB kuponunda kullanim sayisini artir
    if is_db_coupon and db_coupon:
        db_coupon.uses_count += 1

    # Islem logu
    tx = WalletTransaction(
        user_id=user.id,
        amount=amount,
        tx_type="coupon",
        description=f"Kupon: {code}",
        balance_after=user.wallet_balance,
    )
    db.add(tx)
    await db.flush()

    # Telegram bildirim — kupon kullanildiginda admin'e haber ver
    if is_db_coupon and db_coupon:
        remaining = db_coupon.max_uses - db_coupon.uses_count
        try:
            from app.services.admin_telegram import send_admin_message
            if remaining == 0:
                # Son kullanim — kupon tukendi!
                await send_admin_message(
                    f"🎟🔴 <b>Kupon Tükendi!</b>\n"
                    f"Kod: <code>{code}</code>\n"
                    f"Puan: +{int(amount)}\n"
                    f"Kullanan: <code>{device_id}</code>\n"
                    f"Kullanım: {db_coupon.uses_count}/{db_coupon.max_uses} ✅\n"
                    f"Tüm haklar kullanıldı!",
                    silent=False,
                )
            else:
                await send_admin_message(
                    f"🎟 <b>Kupon Kullanıldı!</b>\n"
                    f"Kod: <code>{code}</code>\n"
                    f"Puan: +{int(amount)}\n"
                    f"Kullanan: <code>{device_id}</code>\n"
                    f"Kullanım: {db_coupon.uses_count}/{db_coupon.max_uses}\n"
                    f"Kalan: {remaining}",
                    silent=False,
                )
        except Exception:
            pass  # Telegram hatasi uygulama akisini bozmasin

    _check_daily_reset(user)
    cooldown = _get_wallet_cooldown(user)

    return WalletBalanceOut(
        balance=user.wallet_balance,
        daily_ads_watched=user.daily_ads_watched or 0,
        max_daily_ads=WALLET_MAX_DAILY_ADS,
        cooldown_remaining=cooldown,
        can_watch_ad=cooldown == 0 and (user.daily_ads_watched or 0) < WALLET_MAX_DAILY_ADS,
    )


# -------------------------------------------------------
# IPO BILDIRIM TERCIHI
# -------------------------------------------------------

@app.post("/api/v1/users/{device_id}/ipo-alerts")
async def create_ipo_alert(
    device_id: str,
    data: IPOAlertCreate,
    db: AsyncSession = Depends(get_db),
):
    """Belirli bir halka arz icin bildirim tercihi olusturur."""
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    result = await db.execute(
        select(UserIPOAlert).where(
            UserIPOAlert.user_id == user.id,
            UserIPOAlert.ipo_id == data.ipo_id,
        )
    )
    alert = result.scalar_one_or_none()

    if alert:
        alert.notify_last_day = data.notify_last_day
        alert.notify_result = data.notify_result
        alert.notify_ceiling = data.notify_ceiling
    else:
        alert = UserIPOAlert(
            user_id=user.id,
            ipo_id=data.ipo_id,
            notify_last_day=data.notify_last_day,
            notify_result=data.notify_result,
            notify_ceiling=data.notify_ceiling,
        )
        db.add(alert)

    await db.flush()
    return {"status": "ok", "ipo_id": data.ipo_id}


@app.delete("/api/v1/users/{device_id}/ipo-alerts/{ipo_id}")
async def delete_ipo_alert(
    device_id: str,
    ipo_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Halka arz bildirim tercihini kaldirir."""
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    result = await db.execute(
        select(UserIPOAlert).where(
            UserIPOAlert.user_id == user.id,
            UserIPOAlert.ipo_id == ipo_id,
        )
    )
    alert = result.scalar_one_or_none()
    if alert:
        await db.delete(alert)

    return {"status": "ok"}


# -------------------------------------------------------
# TAVAN TAKIP (Matriks Pipeline)
# -------------------------------------------------------

@app.post("/api/v1/ceiling-track")
@limiter.limit("60/minute")
async def update_ceiling_track(
    request: Request,
    data: CeilingTrackUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Matriks Excel pipeline'indan gelen tavan/taban bilgisini kaydeder."""
    import traceback
    from app.services.ipo_service import IPOService
    from app.services.notification import NotificationService

    ipo_service = IPOService(db)
    notif_service = NotificationService(db)

    ipo = await ipo_service.get_ipo_by_ticker(data.ticker)
    if not ipo:
        raise HTTPException(status_code=404, detail=f"IPO bulunamadi: {data.ticker}")

    # ── 25 GÜN SINIRI KONTROLÜ ──────────────────────────────────────────
    # Arşivlenmiş IPO'lara veri kabul etme
    if ipo.archived:
        logger.warning(
            "ceiling-track REDDEDİLDİ: %s arşivlenmiş (trading_day_count=%s), gelen trading_day=%s",
            data.ticker, ipo.trading_day_count, data.trading_day,
        )
        return {
            "status": "skipped",
            "reason": "archived",
            "ticker": data.ticker,
            "trading_day": data.trading_day,
        }
    # 25. günü aşan veri kabul etme
    if data.trading_day > 25:
        logger.warning(
            "ceiling-track REDDEDİLDİ: %s trading_day=%s > 25 sınırı",
            data.ticker, data.trading_day,
        )
        return {
            "status": "skipped",
            "reason": "trading_day_limit_exceeded",
            "ticker": data.ticker,
            "trading_day": data.trading_day,
            "max_allowed": 25,
        }
    # ────────────────────────────────────────────────────────────────────

    try:
        track = await ipo_service.update_ceiling_track(
            ipo_id=ipo.id,
            trading_day=data.trading_day,
            trade_date=data.trade_date,
            open_price=data.open_price,
            close_price=data.close_price,
            high_price=data.high_price,
            low_price=data.low_price,
            hit_ceiling=data.hit_ceiling,
            hit_floor=data.hit_floor,
            alis_lot=data.alis_lot,
            satis_lot=data.satis_lot,
            pct_change=data.pct_change,
        )
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"ceiling-track update_ceiling_track HATASI ({data.ticker}): {e}\n{tb}")
        resp = {"status": "error", "phase": "update_ceiling_track", "detail": str(e)[:200]}
        if not settings.is_production:
            resp["traceback"] = tb
        return resp

    # Bildirimler artik excel_sync.py → /api/v1/realtime-notification uzerinden gonderiliyor.
    # Bu endpoint sadece veri kaydeder, bildirim gondermez.
    notifications_sent = 0

    try:
        await db.flush()
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"ceiling-track flush HATASI ({data.ticker}): {e}\n{tb}")
        resp = {"status": "error", "phase": "flush", "detail": str(e)[:200]}
        if not settings.is_production:
            resp["traceback"] = tb
        return resp

    return {
        "status": "ok",
        "ticker": data.ticker,
        "trading_day": data.trading_day,
        "notifications_sent": notifications_sent,
    }



@app.post("/api/v1/realtime-notification")
@limiter.limit("20/minute")
async def send_realtime_notification(
    request: Request,
    data: RealtimeNotifRequest,
    db: AsyncSession = Depends(get_db),
):
    """
    Gercek zamanli bildirim gonder — excel_sync.py'den cagirilir.

    5 bildirim tipi:
      - tavan_bozulma:          Tavan acilinca/kitlenince
      - taban_acilma:           Taban acilinca/kitlenince
      - el_degistirme:          E.D.O kumulatif esik asildiginda (%10,%25,%50,%75,%100,%125)
      - gunluk_acilis_kapanis:  Gunluk acilis (09:56) ve kapanis (18:08)
      - yuzde_dusus:            Tek hizmet — %4 ve %7 esik, gunde max 2 bildirim
                                sub_event: "pct4" veya "pct7"

    Bildirim mesajlarinda fiyat bilgisi YOKTUR.
    """
    import json as _json
    import time as _time
    from app.services.ipo_service import IPOService
    from app.services.notification import NotificationService

    if not _verify_admin_password(data.admin_password):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    # ── Warmup koruması: servis yeni başladıysa ilk 5 dk bildirim gönderme ──
    # Excel geç açılınca biriken eski veriler spam yapar
    import time as _time_mod
    _uptime = _time_mod.time() - _APP_START_TIME
    if _uptime < 300:  # 5 dakika
        logging.info(
            "[REALTIME-NOTIF] WARMUP: %s %s atlandı (uptime=%.0fs, 300s bekleniyor)",
            data.ticker, data.notification_type, _uptime,
        )
        return {
            "status": "skipped",
            "reason": "warmup",
            "ticker": data.ticker,
            "uptime_seconds": round(_uptime),
            "notifications_sent": 0,
        }

    valid_types = [
        "tavan_bozulma", "taban_acilma", "el_degistirme",
        "gunluk_acilis_kapanis", "yuzde_dusus",
    ]
    if data.notification_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"Gecersiz notification_type: {data.notification_type}. Gecerli: {valid_types}",
        )

    # IPO bul
    ipo_service = IPOService(db)
    ipo = await ipo_service.get_ipo_by_ticker(data.ticker)
    if not ipo:
        logging.warning(
            "[REALTIME-NOTIF] IPO bulunamadi: %s (tip=%s)",
            data.ticker, data.notification_type,
        )
        raise HTTPException(status_code=404, detail=f"IPO bulunamadi: {data.ticker}")

    # Arşivlenmiş veya takibi kapalı IPO'lara bildirim gönderme
    if ipo.archived or not ipo.ceiling_tracking_active:
        reason = "archived" if ipo.archived else "ceiling_tracking_inactive"
        logging.warning(
            "[REALTIME-NOTIF] SKIP: %s %s — reason=%s (ipo_id=%s, archived=%s, tracking=%s)",
            data.ticker, data.notification_type, reason,
            ipo.id, ipo.archived, ipo.ceiling_tracking_active,
        )
        return {
            "status": "skipped",
            "reason": reason,
            "ticker": data.ticker,
            "notifications_sent": 0,
        }

    notif_service = NotificationService(db)

    notifications_sent = 0
    errors = 0
    skip_reasons = {
        "expired": 0,
        "muted_type": 0,
        "dedup": 0,
        "no_user": 0,
        "no_token": 0,
        "master_off": 0,
        "toggle_off": 0,
        "send_fail": 0,
    }
    notified_user_ids: set = set()
    _now_utc = datetime.now(timezone.utc)
    total_target = 0

    # =========================================================
    # FREE_FOR_ALL modu — tum kullanicilara gonder (EDO %1 gibi)
    # =========================================================
    if data.free_for_all:
        all_users_result = await db.execute(
            select(User).where(
                User.notifications_enabled == True,
            )
        )
        all_users = list(all_users_result.scalars().all())
        # FCM kullanıcılarını önce gönder (gerçek Android/iOS)
        all_users.sort(key=lambda u: (0 if (u.fcm_token or "").strip() else 1))
        total_target = len(all_users)

        logging.info(
            "[REALTIME-NOTIF] FREE_FOR_ALL: %s %s — %d kullanici hedefleniyor",
            data.ticker, data.notification_type, total_target,
        )

        for user in all_users:
            # Kullanici ucretsiz EDO bildirimini kapatmis mi?
            if not getattr(user, "notify_edo_free", True):
                skip_reasons["toggle_off"] += 1
                continue

            fcm = (user.fcm_token or "").strip()
            expo = (user.expo_push_token or "").strip()
            if not fcm and not expo:
                skip_reasons["no_token"] += 1
                continue

            if user.id in notified_user_ids:
                skip_reasons["dedup"] += 1
                continue

            try:
                success = await notif_service._send_to_user(
                    user=user,
                    title=data.title,
                    body=data.body,
                    data={
                        "type": "stock_notification",
                        "notification_type": data.notification_type,
                        "ticker": data.ticker,
                        "ipo_id": str(ipo.id),
                        "screen": "halka-arz-detay",
                    },
                    channel_id="ceiling_alerts_v2",
                    category="ipo",
                )
                if success:
                    notifications_sent += 1
                    notified_user_ids.add(user.id)
                else:
                    errors += 1
                    skip_reasons["send_fail"] += 1
            except Exception as e:
                errors += 1
                skip_reasons["send_fail"] += 1
                logging.warning(f"FREE bildirim gonderilemedi (user={user.id}): {e}")

    else:
        # =========================================================
        # NORMAL mod — sadece abonelere gonder
        # =========================================================
        # Bu IPO + bildirim tipi icin aktif aboneleri bul
        stock_notif_result = await db.execute(
            select(StockNotificationSubscription).where(
                and_(
                    or_(
                        # Bu IPO icin spesifik abone
                        and_(
                            StockNotificationSubscription.ipo_id == ipo.id,
                            StockNotificationSubscription.notification_type == data.notification_type,
                        ),
                        # Paket aboneleri — bundle (3 aylik/yillik) tum halka arzlar icin
                        StockNotificationSubscription.is_annual_bundle == True,
                    ),
                    StockNotificationSubscription.is_active == True,
                    # muted=NULL veya muted=False olanlari al (True olanlari atla)
                    or_(
                        StockNotificationSubscription.muted == False,
                        StockNotificationSubscription.muted.is_(None),
                    ),
                )
            )
        )
        active_subs = list(stock_notif_result.scalars().all())
        total_target = len(active_subs)

        logging.info(
            "[REALTIME-NOTIF] %s %s — %d abone bulundu (ipo_id=%s)",
            data.ticker, data.notification_type, total_target, ipo.id,
        )

        # FCM kullanıcılarını önce gönder (gerçek Android/iOS),
        # Expo-only kullanıcıları sonra (test/dev)
        # Bunun için user'ları önceden çekip sıralıyoruz
        _sub_user_pairs = []
        for _s in active_subs:
            _u_result = await db.execute(select(User).where(User.id == _s.user_id))
            _u = _u_result.scalar_one_or_none()
            _sub_user_pairs.append((_s, _u))
        # FCM token'ı olanlar önce (bool sıralaması: True=0, False=1 → FCM'li önce)
        _sub_user_pairs.sort(key=lambda x: (0 if (x[1] and (x[1].fcm_token or "").strip()) else 1))

        for sub, _prefetched_user in _sub_user_pairs:
            # Suresi dolmus mu?
            if sub.expires_at:
                sub_expires = sub.expires_at
                if sub_expires.tzinfo is None:
                    from datetime import timezone as _tz
                    sub_expires = sub_expires.replace(tzinfo=_tz.utc)
                if sub_expires < _now_utc:
                    sub.is_active = False
                    skip_reasons["expired"] += 1
                    continue

            # Bundle aboneliklerde tip bazli mute kontrolu
            if sub.muted_types:
                try:
                    muted_list = _json.loads(sub.muted_types)
                    if data.notification_type in muted_list:
                        skip_reasons["muted_type"] += 1
                        continue
                except (ValueError, TypeError):
                    pass

            if sub.user_id in notified_user_ids:
                skip_reasons["dedup"] += 1
                continue

            notif_title = data.title
            notif_body = data.body

            user = _prefetched_user
            if not user:
                skip_reasons["no_user"] += 1
                continue
            fcm = (user.fcm_token or "").strip()
            expo = (user.expo_push_token or "").strip()
            if not fcm and not expo:
                skip_reasons["no_token"] += 1
                continue
            if not user.notifications_enabled:
                skip_reasons["master_off"] += 1
                continue

            # Kullanici bazli bildirim tipi toggle kontrolu
            _type_toggle_map = {
                "tavan_bozulma": "notify_ceiling_break",
                "taban_acilma": "notify_taban_break",
                "gunluk_acilis_kapanis": "notify_daily_open_close",
                "yuzde_dusus": "notify_percent_drop",
            }
            _toggle_field = _type_toggle_map.get(data.notification_type)
            if _toggle_field and not getattr(user, _toggle_field, True):
                skip_reasons["toggle_off"] += 1
                continue

            try:
                success = await notif_service._send_to_user(
                    user=user,
                    title=notif_title,
                    body=notif_body,
                    data={
                        "type": "stock_notification",
                        "notification_type": data.notification_type,
                        "ticker": data.ticker,
                        "ipo_id": str(ipo.id),
                        "screen": "halka-arz-detay",
                    },
                    channel_id="ceiling_alerts_v2",
                    category="ipo",
                )
                if success:
                    notifications_sent += 1
                    notified_user_ids.add(sub.user_id)
                else:
                    errors += 1
                    skip_reasons["send_fail"] += 1
                sub.notified_count = (sub.notified_count or 0) + 1
            except Exception as e:
                errors += 1
                skip_reasons["send_fail"] += 1
                logging.warning(f"Bildirim gonderilemedi (user={user.id}): {e}")

    await db.flush()

    # Detayli log — debug icin
    skip_summary = ", ".join(f"{k}={v}" for k, v in skip_reasons.items() if v > 0)
    logging.info(
        "[REALTIME-NOTIF] SONUC: %s %s — sent=%d, errors=%d, subs=%d%s",
        data.ticker, data.notification_type,
        notifications_sent, errors, total_target,
        f" | skip: {skip_summary}" if skip_summary else "",
    )

    # Admin Telegram bildirimi — onemli durumlarda (basarili veya hata)
    if notifications_sent > 0 or errors > 0 or len(active_subs) == 0:
        try:
            from app.services.admin_telegram import send_admin_message
            status_emoji = "✅" if notifications_sent > 0 else ("⚠️" if errors > 0 else "📭")
            await send_admin_message(
                f"{status_emoji} <b>Realtime Bildirim</b>\n"
                f"Ticker: {data.ticker} | Tip: {data.notification_type}\n"
                f"Hedef: {total_target} | Gönderildi: {notifications_sent} | Hata: {errors}\n"
                + (f"Mod: FREE_FOR_ALL\n" if data.free_for_all else "")
                + (f"Skip: {skip_summary}" if skip_summary else "")
            )
        except Exception:
            pass  # Telegram hatasi akisi bozmasin

    return {
        "status": "ok",
        "ticker": data.ticker,
        "notification_type": data.notification_type,
        "active_subscribers": total_target,
        "free_for_all": data.free_for_all,
        "notifications_sent": notifications_sent,
        "errors": errors,
        "notified_users": list(notified_user_ids),
        "skip_reasons": skip_reasons,
    }


# ================================================================
# ADMIN: Bildirim Log Geçmişi (tüm kullanıcılar, son X saat)
# ================================================================
@app.post("/api/v1/admin/notification-logs")
@limiter.limit("10/minute")
async def admin_notification_logs(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Son X saatteki tüm bildirimleri döndürür (admin).

    Body: { "admin_password": "...", "hours": 1, "limit": 200, "ticker": "GENKM" (opsiyonel) }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.models.notification_log import NotificationLog
    from datetime import timedelta

    hours = min(int(payload.get("hours", 1)), 24)
    limit = min(int(payload.get("limit", 200)), 500)
    ticker_filter = payload.get("ticker")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    query = (
        select(NotificationLog)
        .where(NotificationLog.created_at >= cutoff)
        .order_by(NotificationLog.created_at.desc())
    )

    if ticker_filter:
        query = query.where(NotificationLog.title.ilike(f"%{ticker_filter}%"))

    query = query.limit(limit)
    result = await db.execute(query)
    logs = result.scalars().all()

    # Kategorilere göre sayım
    cat_counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    for log_entry in logs:
        cat = log_entry.category or "unknown"
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
        # data_json'dan notification_type çıkar
        if log_entry.data_json:
            try:
                import json as _json
                d = _json.loads(log_entry.data_json)
                nt = d.get("notification_type", "unknown")
                type_counts[nt] = type_counts.get(nt, 0) + 1
            except Exception:
                pass

    return {
        "total": len(logs),
        "hours": hours,
        "category_counts": cat_counts,
        "type_counts": type_counts,
        "logs": [
            {
                "id": l.id,
                "title": l.title,
                "body": l.body,
                "category": l.category,
                "data_json": l.data_json,
                "created_at": l.created_at.isoformat() if l.created_at else None,
            }
            for l in logs
        ],
    }


@app.post("/api/v1/admin/send-telegram")
@limiter.limit("5/minute")
async def admin_send_telegram(request: Request, payload: dict):
    """Excel sync'ten gelen Telegram uyari mesajlarini admin'e iletir."""
    pw = payload.get("admin_password", "")
    if pw != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    text = payload.get("text", "")
    if not text:
        raise HTTPException(status_code=400, detail="text gerekli")

    from app.services.admin_telegram import send_admin_message
    ok = await send_admin_message(text)
    return {"status": "ok" if ok else "failed", "sent": ok}


@app.post("/api/v1/admin/bulk-ceiling-track")
@limiter.limit("10/minute")
async def bulk_ceiling_track(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    Toplu tavan/taban verisi yukler.
    Excel/CSV yerine JSON bulk — admin sifresiyle korunur.

    Body:
    {
      "admin_password": "...",
      "tracks": [
        {
          "ticker": "AKHAN",
          "trading_day": 1,
          "trade_date": "2026-02-03",
          "open_price": "23.65",
          "close_price": "23.65",
          "high_price": "23.65",
          "low_price": "21.50",
          "hit_ceiling": true,
          "hit_floor": false
        },
        ...
      ]
    }
    """
    import os
    from decimal import Decimal as D
    from datetime import date as date_type

    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    tracks_raw = payload.get("tracks", [])
    if not tracks_raw:
        raise HTTPException(status_code=400, detail="tracks listesi bos")

    from app.services.ipo_service import IPOService
    ipo_service = IPOService(db)

    results = []
    errors = []

    for idx, t in enumerate(tracks_raw):
        try:
            ticker = t["ticker"]
            ipo = await ipo_service.get_ipo_by_ticker(ticker)
            if not ipo:
                errors.append({"index": idx, "ticker": ticker, "error": "IPO bulunamadi"})
                continue

            trading_day = int(t["trading_day"])
            trade_date = t["trade_date"]
            if isinstance(trade_date, str):
                parts = trade_date.split("-")
                trade_date = date_type(int(parts[0]), int(parts[1]), int(parts[2]))

            open_price = D(str(t["open_price"])) if t.get("open_price") else None
            close_price = D(str(t["close_price"]))
            high_price = D(str(t["high_price"])) if t.get("high_price") else None
            low_price = D(str(t["low_price"])) if t.get("low_price") else None
            hit_ceiling = bool(t.get("hit_ceiling", False))
            hit_floor = bool(t.get("hit_floor", False))
            alis_lot = int(t["alis_lot"]) if t.get("alis_lot") else None
            satis_lot = int(t["satis_lot"]) if t.get("satis_lot") else None
            pct_change = float(t["pct_change"]) if t.get("pct_change") is not None else None
            gunluk_adet = int(t["gunluk_adet"]) if t.get("gunluk_adet") else None
            senet_sayisi_val = int(t["senet_sayisi"]) if t.get("senet_sayisi") else None

            track = await ipo_service.update_ceiling_track(
                ipo_id=ipo.id,
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
                pct_change=pct_change,
                gunluk_adet=gunluk_adet,
                senet_sayisi=senet_sayisi_val,
            )

            # trading_day_count GUNCELLENMEZ — sadece daily_ceiling_update scheduler yapar.
            # Boylece live_sync gun ici calisirken trading_day_count artmaz ve
            # kapanis modu yanlis gun olusturmaz.

            # ceiling_tracking_active
            if not ipo.ceiling_tracking_active:
                ipo.ceiling_tracking_active = True

            results.append({
                "ticker": ticker,
                "trading_day": trading_day,
                "close_price": str(close_price),
                "hit_ceiling": hit_ceiling,
                "hit_floor": hit_floor,
            })
        except Exception as e:
            errors.append({"index": idx, "ticker": t.get("ticker", "?"), "error": str(e)})

    await db.commit()

    # ── E.D.O Threshold Check — CANLI seans icinde (her sync'te) ──
    # Kapanış job'undan buraya taşındı. Her 15sn'de güncellenen EDO degerini
    # kontrol eder, esik asilmissa bildirim gonderir. edo_notified_thresholds
    # duplicate'leri onler — ayni esik icin sadece 1 kez gider.
    try:
        from app.config import EDO_START_DATE
        import json as _json

        for r in results:
            _ticker = r["ticker"]
            _ipo_q = await db.execute(
                select(IPO).where(IPO.ticker == _ticker)
            )
            _ipo = _ipo_q.scalar_one_or_none()
            if not _ipo:
                continue
            if not (_ipo.trading_start and _ipo.trading_start >= EDO_START_DATE):
                continue
            if not (_ipo.senet_sayisi and _ipo.senet_sayisi > 0 and _ipo.cumulative_volume):
                continue

            edo_pct = (_ipo.cumulative_volume / _ipo.senet_sayisi) * 100
            edo_thresholds = [1, 3, 10, 25, 50, 75, 100, 125]
            notified_raw = _ipo.edo_notified_thresholds or "[]"
            try:
                notified = _json.loads(notified_raw)
            except Exception:
                notified = []

            new_thresholds = []
            for threshold in edo_thresholds:
                if edo_pct >= threshold and threshold not in notified:
                    notified.append(threshold)
                    new_thresholds.append(threshold)

            if new_thresholds:
                _ipo.edo_notified_thresholds = _json.dumps(notified)
                await db.commit()

                # Bildirim gonder
                _edo_suffix = {1: "'i", 3: "'ü", 10: "'u", 25: "'i", 50: "'yi", 75: "'i", 100: "'ü", 125: "'i"}

                # Kac islem gunu?
                _day_count_q = await db.execute(
                    select(func.count(IPOCeilingTrack.id)).where(
                        IPOCeilingTrack.ipo_id == _ipo.id
                    )
                )
                _day_count = _day_count_q.scalar() or 0

                for threshold in new_thresholds:
                    try:
                        import httpx
                        import os
                        if threshold == 1:
                            title = f"{_ticker} E.D.O %1'i Aştı! Senetlerin %1'i el değiştirdi"
                            body = f"Kümülatif E.D.O: %{edo_pct:.2f} — 8 farklı eşik bildirimi için paketi aç!"
                            is_free = True
                        else:
                            edo_msgs = {
                                3: "E.D.O %3'ü Aştı! Senetlerin %3'ü el değiştirdi",
                                10: "E.D.O %10'u Aştı! Senetlerin %10'u el değiştirdi",
                                25: "E.D.O %25'i Aştı! Senetlerin çeyreği el değiştirdi",
                                50: "E.D.O %50'yi Aştı! Senetlerin yarısı el değiştirdi",
                                75: "E.D.O %75'i Aştı! Senetlerin dörtte üçü el değiştirdi",
                                100: "E.D.O %100'ü Aştı! Tüm senetler el değiştirdi",
                                125: "E.D.O %125'i Aştı! Senetler 1.25 kez döndü",
                            }
                            title = f"{_ticker} {edo_msgs.get(threshold, f'El Değiştirme Oranı %{threshold} aşıldı')}"
                            body = f"Kümülatif El Değiştirme Oranı: %{edo_pct:.1f} — {_day_count}. İşlem Günü"
                            is_free = False

                        api_url = os.getenv("API_URL", "https://sz-bist-finans-api.onrender.com")
                        admin_pw = os.getenv("ADMIN_PASSWORD", "")
                        async with httpx.AsyncClient(timeout=30) as client:
                            await client.post(
                                f"{api_url}/api/v1/realtime-notification",
                                json={
                                    "admin_password": admin_pw,
                                    "ticker": _ticker,
                                    "notification_type": "el_degistirme",
                                    "title": title,
                                    "body": body,
                                    "free_for_all": is_free,
                                },
                            )
                        logger.info("E.D.O LIVE: %s — %%%d esik bildirimi gonderildi (EDO: %%.2f)", _ticker, threshold, edo_pct)
                    except Exception as notif_err:
                        logger.error("E.D.O LIVE bildirim hatasi: %s", notif_err)

                    # Tweet — %1, %10, %100
                    if threshold in [1, 10, 100]:
                        try:
                            from app.services.twitter_service import tweet_edo_threshold
                            tweet_edo_threshold(_ipo, threshold, edo_pct, _day_count)
                        except Exception as tw_err:
                            logger.error("E.D.O tweet hatasi: %s", tw_err)
    except Exception as edo_live_err:
        logger.warning("E.D.O live threshold check hatasi: %s", edo_live_err)

    return {
        "status": "ok",
        "loaded": len(results),
        "errors": len(errors),
        "results": results,
        "error_details": errors,
    }


# -------------------------------------------------------
# VIDEO PIPELINE — Gonderilmis tweetleri listele
# -------------------------------------------------------

@app.get("/api/v1/admin/sent-tweets")
@limiter.limit("30/minute")
async def get_sent_tweets(
    request: Request,
    admin_password: str = Query(..., description="Admin sifresi"),
    after_id: int = Query(0, description="Bu ID'den sonraki tweetleri getir"),
    limit: int = Query(20, ge=1, le=100, description="Maks sonuc sayisi"),
    db: AsyncSession = Depends(get_db),
):
    """Video pipeline icin gonderilmis tweetleri listeler.

    Pipeline bu endpoint'i poll ederek yeni tweet olup olmadigini kontrol eder.
    after_id ile sadece belirli bir ID'den sonraki tweetler alinir.
    """
    if not _verify_admin_password(admin_password):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.models.pending_tweet import PendingTweet

    query = (
        select(PendingTweet)
        .where(
            PendingTweet.status == "sent",
            PendingTweet.id > after_id,
        )
        .order_by(PendingTweet.id.asc())
        .limit(limit)
    )

    result = await db.execute(query)
    tweets = list(result.scalars().all())

    return [
        {
            "id": t.id,
            "text": t.text,
            "source": t.source,
            "image_path": t.image_path,
            "twitter_tweet_id": t.twitter_tweet_id,
            "sent_at": t.sent_at.isoformat() if t.sent_at else None,
            "created_at": t.created_at.isoformat() if t.created_at else None,
        }
        for t in tweets
    ]


# -------------------------------------------------------
# T15 — Ogle Arasi Market Snapshot Tweet Trigger
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-snapshot-tweet")
@limiter.limit("5/minute")
async def trigger_snapshot_tweet(
    request: Request,
    payload: dict,
):
    """Admin panelden ogle arasi market snapshot tweet'ini tetikler."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.scheduler import market_snapshot_tweet
    result = await market_snapshot_tweet()
    if result and result.get("error"):
        return {"status": "error", "message": result["error"]}
    return {"status": "ok", "message": result.get("message", "Market snapshot tweet tetiklendi") if result else "Market snapshot tweet tetiklendi"}


# -------------------------------------------------------
# T17 — Gun Sonu Kapanis Tweet Trigger (Admin Manuel)
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-closing-tweet")
@limiter.limit("3/minute")
async def trigger_closing_tweet(
    request: Request,
    payload: dict,
):
    """Admin panelden gun sonu kapanis tweet'ini manuel tetikler.

    daily_ceiling_update() scheduler fonksiyonunu cagirır.
    Duplicate tweet koruması: twitter_service._is_duplicate_tweet() 24 saat penceresi.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.scheduler import daily_ceiling_update
    try:
        await daily_ceiling_update()
        return {"status": "ok", "message": "Kapanis tweet'leri tetiklendi (daily_ceiling_update)"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# -------------------------------------------------------
# BOT TWEET PROXY — Lokal bot buraya POST yapar, Render tweet atar
# -------------------------------------------------------

@app.post("/api/v1/admin/bot-tweet-proxy")
@limiter.limit("10/minute")
async def bot_tweet_proxy(request: Request, payload: dict):
    """Lokal reply-bot'tan gelen tweet'i Twitter API ile atar.

    Payload:
        admin_password: str
        text: str  — tweet metni
        reply_to_tweet_id: str | None  — reply ise hedef tweet ID
        image_base64: str | None  — resim (base64 encoded, opsiyonel)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    text = payload.get("text", "").strip()
    if not text:
        return {"status": "error", "message": "Tweet metni boş"}

    reply_to = payload.get("reply_to_tweet_id")
    image_b64 = payload.get("image_base64")

    try:
        from app.services.twitter_service import _safe_tweet, _safe_reply_tweet, _safe_tweet_with_media
        import tempfile, base64

        # Resim varsa geçici dosyaya yaz
        temp_image_path = None
        if image_b64:
            try:
                img_data = base64.b64decode(image_b64)
                with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
                    f.write(img_data)
                    temp_image_path = f.name
            except Exception as img_err:
                logger.warning("Bot proxy: resim decode hatası: %s", img_err)

        # Reply mi yoksa yeni tweet mi?
        if reply_to:
            ok = _safe_reply_tweet(text, reply_to)
            return {"status": "ok" if ok else "error", "message": "Reply gönderildi" if ok else "Reply gönderilemedi"}
        elif temp_image_path:
            ok = _safe_tweet_with_media(text, temp_image_path, source="bot_proxy", force_send=True)
            # Geçici dosyayı temizle
            try:
                os.unlink(temp_image_path)
            except Exception:
                pass
            return {"status": "ok" if ok else "error", "message": "Tweet (resimli) gönderildi" if ok else "Tweet gönderilemedi"}
        else:
            ok = _safe_tweet(text, source="bot_proxy", force_send=True)
            return {"status": "ok" if ok else "error", "message": "Tweet gönderildi" if ok else "Tweet gönderilemedi"}

    except Exception as e:
        logger.error("Bot tweet proxy hatası: %s", e)
        return {"status": "error", "message": str(e)[:200]}


@app.get("/api/v1/admin/search-mentions")
@limiter.limit("10/minute")
async def search_mentions(
    request: Request,
    admin_password: str = Query(...),
    since_id: str = Query(None, description="Bu ID'den sonraki mention'ları getir"),
    max_results: int = Query(10, ge=10, le=100),
):
    """Bot için @BorsaCebimde mention araması — search/recent API.

    Free tier'da çalışır. since_id ile sadece yeni mention'lar alınır.
    """
    if not _verify_admin_password(admin_password):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.services.twitter_service import search_recent_tweets
    result = search_recent_tweets(
        query="@BorsaCebimde -is:retweet",
        since_id=since_id,
        max_results=max_results,
    )
    return result


@app.post("/api/v1/admin/backfill-market-close-ai")
@limiter.limit("3/minute")
async def admin_backfill_market_close_ai(request: Request, payload: dict = Body(...)):
    """Bugün DB'de reason='' olan tavan/taban kayitlarini AI ile doldur.
    Her ticker için ayrı UPDATE+commit — restart durumunda kalan tickers'tan devam eder.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from app.services.market_close_analyzer import _analyze_reason_with_ai
    from app.models.daily_stock_market_stat import DailyStockMarketStat
    from app.database import async_session
    from sqlalchemy import select as _sel9
    from datetime import datetime as _dt9
    import zoneinfo, asyncio as _asy
    tz_tr = zoneinfo.ZoneInfo("Europe/Istanbul")
    today = _dt9.now(tz_tr).date()

    async with async_session() as db:
        rows = (await db.execute(
            _sel9(DailyStockMarketStat).where(
                DailyStockMarketStat.date == today,
                (DailyStockMarketStat.reason == "") | (DailyStockMarketStat.reason.is_(None))
            )
        )).scalars().all()

    filled = 0
    for r in rows:
        try:
            # close_price BIST lisans nedeniyle modelden kaldirildi — 0.0 olarak gec
            reason = await _analyze_reason_with_ai(
                ticker=r.ticker, is_ceiling=bool(r.is_ceiling),
                price=0.0, pct=float(r.percent_change or 0),
                consec=r.consecutive_ceiling_count if r.is_ceiling else r.consecutive_floor_count,
                monthly=r.monthly_ceiling_count if r.is_ceiling else r.monthly_floor_count,
            )
            if reason:
                async with async_session() as db2:
                    obj = (await db2.execute(_sel9(DailyStockMarketStat).where(DailyStockMarketStat.id == r.id))).scalar_one_or_none()
                    if obj:
                        obj.reason = reason[:100]
                        await db2.commit()
                filled += 1
        except Exception:
            pass
        await _asy.sleep(2)  # Hafif rate limit
    return {"filled": filled, "total": len(rows)}


@app.post("/api/v1/admin/save-market-close-fast")
@limiter.limit("3/minute")
async def admin_save_market_close_fast(request: Request, payload: dict = Body(...)):
    """Tavan/taban hisselerini hizla DB'ye kaydeder + AI analizini arka planda baslatir.
    Her ticker'in AI reason'i ayri commit'lendigi icin restart'lar kaybetmez.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from app.services.market_close_analyzer import scrape_uzmanpara
    from app.models.daily_stock_market_stat import DailyStockMarketStat
    from app.database import async_session
    from datetime import datetime as _dt7, timezone as _tz7, date as _date7, timedelta as _td7
    from sqlalchemy import text as sa_text
    from decimal import Decimal as _Dec
    import zoneinfo
    tz_tr = zoneinfo.ZoneInfo("Europe/Istanbul")
    today = _dt7.now(tz_tr).date()

    ceilings = await scrape_uzmanpara(is_ceiling=True)
    floors = await scrape_uzmanpara(is_ceiling=False)
    if not ceilings and not floors:
        return {"status": "error", "msg": "scrape returned empty"}

    saved = 0
    async with async_session() as db:
        # bugünün kayitlarini sil (force gibi)
        await db.execute(sa_text('DELETE FROM daily_stock_market_stats WHERE "date" = :today'), {"today": today})
        await db.commit()

        # consec hesabi (basitleştirilmiş)
        all_stocks = [(s, True) for s in ceilings] + [(s, False) for s in floors]
        seen = set()
        for stock, is_c in all_stocks:
            try:
                ticker = stock["ticker"]
                if ticker in seen: continue
                seen.add(ticker)
                # past
                past = (await db.execute(sa_text("""
                    SELECT is_ceiling, is_floor, consecutive_ceiling_count, consecutive_floor_count, "date"
                    FROM daily_stock_market_stats WHERE ticker=:t ORDER BY "date" DESC LIMIT 30
                """), {"t": ticker})).fetchall()
                consec = 1
                if past and ((is_c and past[0][0]) or (not is_c and past[0][1])):
                    gap = (today - past[0][4]).days
                    if gap == 1 or (gap <= 3 and today.weekday() == 0) or (gap == 2 and today.weekday() in (0, 1)):
                        consec = (past[0][2] if is_c else past[0][3]) + 1
                monthly = sum(1 for r in past if (r[0] if is_c else r[1]) and (today - r[4]).days <= 30) + 1

                db.add(DailyStockMarketStat(
                    ticker=ticker, date=today,
                    close_price=_Dec(str(stock["price"])),
                    percent_change=_Dec(str(stock["change"])),
                    is_ceiling=is_c, is_floor=not is_c,
                    consecutive_ceiling_count=consec if is_c else 0,
                    monthly_ceiling_count=monthly if is_c else 0,
                    consecutive_floor_count=consec if not is_c else 0,
                    monthly_floor_count=monthly if not is_c else 0,
                    reason=""
                ))
                saved += 1
            except Exception as e:
                continue
        await db.commit()
    return {"saved": saved, "today": today.isoformat()}


@app.post("/api/v1/admin/trigger-market-close-tweet")
@limiter.limit("3/minute")
async def trigger_market_close_tweet(
    request: Request,
    payload: dict,
):
    """Günün Tavan/Taban hisseleri tweetini manuel tetikler.

    Uzmanpara'dan veri çeker, AI analiz yapar, görsel üretir, tweet atar.
    Duplicate koruma: daily_stock_market_stats'te bugün kaydı varsa atlar.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    import asyncio
    from app.services.market_close_analyzer import scrape_and_analyze_market_close
    force = payload.get("force", False)
    analyze_only = payload.get("analyze_only", False)
    asyncio.create_task(scrape_and_analyze_market_close(force=force, analyze_only=analyze_only))
    mode = "Sadece analiz (tweet yok)" if analyze_only else "Analiz + tweet"
    return {"status": "ok", "message": f"{mode} arka planda başlatıldı (force={force}). Sonuç Telegram'dan gelecek."}


@app.post("/api/v1/admin/debug-market-close")
@limiter.limit("5/minute")
async def debug_market_close(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Market close analyzer debug — neden çalışmadığını teşhis eder."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.services.market_close_analyzer import scrape_uzmanpara
    from datetime import date as _d
    from zoneinfo import ZoneInfo
    from sqlalchemy import text as sa_text
    import httpx, re

    debug_info = {}

    # 1. Bugünün tarihi
    tr_tz = ZoneInfo("Europe/Istanbul")
    today_tr = datetime.now(tr_tz).date()
    debug_info["today_tr"] = str(today_tr)
    debug_info["weekday"] = today_tr.weekday()  # 0=Mon, 4=Fri

    # 2. DB'de bugün kaydı var mı?
    try:
        check = await db.execute(
            sa_text('SELECT COUNT(*) FROM daily_stock_market_stats WHERE "date" = :today'),
            {"today": today_tr}
        )
        db_count = check.scalar()
        debug_info["db_today_count"] = db_count

        # Bugünkü kayıtları göster
        if db_count > 0:
            rows = await db.execute(
                sa_text('SELECT ticker, is_ceiling, is_floor, reason FROM daily_stock_market_stats WHERE "date" = :today LIMIT 5'),
                {"today": today_tr}
            )
            debug_info["db_today_sample"] = [{"ticker": r[0], "is_ceiling": r[1], "is_floor": r[2], "reason": r[3]} for r in rows.fetchall()]

        # Son kayıt tarihi
        last = await db.execute(sa_text('SELECT MAX("date") FROM daily_stock_market_stats'))
        debug_info["db_last_date"] = str(last.scalar())

        # Toplam kayıt sayısı
        total = await db.execute(sa_text('SELECT COUNT(*) FROM daily_stock_market_stats'))
        debug_info["db_total_count"] = total.scalar()
    except Exception as e:
        debug_info["db_error"] = str(e)

    # 3. Uzmanpara güncelleme tarihini kontrol
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            res = await client.get(
                "https://uzmanpara.milliyet.com.tr/borsa/en-cok-artanlar/",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            )
            debug_info["uzmanpara_status"] = res.status_code
            m = re.search(r"Son\s+g[üu]ncelleme\s+tarihi[:\s]*(\d{2})\.(\d{2})\.(\d{4})", res.text)
            if m:
                update_date = _d(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                debug_info["uzmanpara_update_date"] = str(update_date)
                debug_info["date_match"] = update_date == today_tr
            else:
                debug_info["uzmanpara_update_date"] = "REGEX_FAIL"
    except Exception as e:
        debug_info["uzmanpara_error"] = str(e)

    # 4. Tavan/taban scrape test
    try:
        ceilings = await scrape_uzmanpara(is_ceiling=True)
        floors = await scrape_uzmanpara(is_ceiling=False)
        debug_info["ceiling_count"] = len(ceilings)
        debug_info["floor_count"] = len(floors)
        if ceilings:
            debug_info["ceiling_sample"] = ceilings[:3]
        if floors:
            debug_info["floor_sample"] = floors[:3]
    except Exception as e:
        debug_info["scrape_error"] = str(e)

    return debug_info


# -------------------------------------------------------
# T18 — Yalanci Gun Duzeltme (fix-ghost-days)
# -------------------------------------------------------

@app.post("/api/v1/admin/fix-ghost-days")
@limiter.limit("3/minute")
async def fix_ghost_days(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Yalanci gun kayitlarini temizle.

    live_sync gün içi calistiysa trading_day_count yanlis artmis olabilir.
    Bu endpoint her aktif IPO icin:
    1. trading_start'tan bugunun gercek gun numarasini hesaplar
    2. Bu numaradan BUYUK ceiling_track kayitlarini siler
    3. ipo.trading_day_count'u gercek gecmis gun sayisina gore duzeltir

    Body: {"admin_password": "..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from datetime import date as date_type, timedelta
    from app.models.ipo import IPO, IPOCeilingTrack

    today = date_type.today()

    def count_business_days(start_date, end_date):
        """Iki tarih arasindaki is gunlerini say (her ikisi dahil)."""
        if isinstance(start_date, str):
            from datetime import datetime
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            from datetime import datetime
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        count = 0
        cur = start_date
        while cur <= end_date:
            if cur.weekday() < 5:
                count += 1
            cur += timedelta(days=1)
        return count

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
        return {"status": "ok", "message": "Aktif IPO yok", "fixed": []}

    fixed = []
    errors = []

    for ipo in active_ipos:
        if not ipo.ticker or not ipo.trading_start:
            continue
        try:
            # Bugunun gercek gun numarasi (trading_start dahil)
            real_today_day = count_business_days(ipo.trading_start, today)

            # Bu numaradan buyuk track'leri sil (yalanci gunler)
            del_result = await db.execute(
                select(IPOCeilingTrack).where(
                    and_(
                        IPOCeilingTrack.ipo_id == ipo.id,
                        IPOCeilingTrack.trading_day > real_today_day,
                    )
                )
            )
            ghost_tracks = del_result.scalars().all()
            ghost_days = [t.trading_day for t in ghost_tracks]
            for gt in ghost_tracks:
                await db.delete(gt)

            # Kalan (gercek) track sayisini say
            real_result = await db.execute(
                select(IPOCeilingTrack).where(
                    IPOCeilingTrack.ipo_id == ipo.id,
                ).order_by(IPOCeilingTrack.trading_day.asc())
            )
            real_tracks = real_result.scalars().all()
            real_count = len(real_tracks)

            # trading_day_count'u gercek kapanmis gun sayisina guncelle
            # (bugunun gunu kapanmadiysa real_today_day-1 olabilir)
            # Gercek track sayisi en guvenilir kaynak
            old_count = ipo.trading_day_count or 0
            ipo.trading_day_count = real_count

            fixed.append({
                "ticker": ipo.ticker,
                "real_today_day": real_today_day,
                "ghost_days_deleted": ghost_days,
                "old_trading_day_count": old_count,
                "new_trading_day_count": real_count,
            })

        except Exception as e:
            errors.append({"ticker": ipo.ticker, "error": str(e)})

    await db.commit()

    return {
        "status": "ok",
        "fixed_count": len(fixed),
        "error_count": len(errors),
        "fixed": fixed,
        "errors": errors,
    }


# -------------------------------------------------------
# T16 — Yeni Halka Arzlar Acilis Bilgileri Tweet Trigger
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-opening-tweet")
@limiter.limit("5/minute")
async def trigger_opening_tweet(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Excel sync sonrasi cagirilir — ilk 10 gun icindeki hisselerin
    acilis bilgilerini tweet + gorsel olarak atar.

    Body: {"admin_password": "..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from datetime import date as date_type
    from app.models.ipo import IPO, IPOCeilingTrack

    today = date_type.today()

    # trading_day <= 10 olan aktif hisseleri bul
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
    for ipo in trading_ipos:
        # Bu hissenin tüm ceiling track verisini al
        tracks_result = await db.execute(
            select(IPOCeilingTrack).where(
                IPOCeilingTrack.ipo_id == ipo.id
            ).order_by(IPOCeilingTrack.trading_day.asc())
        )
        tracks = list(tracks_result.scalars().all())

        if not tracks:
            continue

        # Son trading_day (en son giren veri)
        latest = tracks[-1]
        current_day = latest.trading_day

        # Sadece ilk 10 gun icindekiler
        if current_day > 10:
            continue

        # Bugünün verisini bul (open_price olan)
        today_track = None
        for t in tracks:
            if t.trade_date == today:
                today_track = t
                break

        if not today_track or not today_track.open_price:
            continue

        # Tavan / Taban / Normal sayıları (tüm günlerden)
        ceiling_days = sum(1 for t in tracks if t.hit_ceiling)
        floor_days = sum(1 for t in tracks if t.hit_floor)
        normal_days = len(tracks) - ceiling_days - floor_days

        # Dünkü kapanış — bir önceki günün close_price'i
        prev_close = 0.0
        for t in tracks:
            if t.trade_date < today and t.close_price:
                prev_close = float(t.close_price)  # en son günü alacak (sorted asc)

        # Eğer dünkü kapanış yoksa (1. gün) → HA fiyatını kullan
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
        if prev_close <= 0:
            prev_close = ipo_price

        open_price = float(today_track.open_price)

        # Açılış % = açılış vs HA fiyat
        pct_change = ((open_price - ipo_price) / ipo_price * 100) if ipo_price > 0 else 0

        # Günlük % = açılış vs dünkü kapanış
        daily_pct = ((open_price - prev_close) / prev_close * 100) if prev_close > 0 else 0

        # Alış/satış lot (bugünkü track'den)
        alis_lot = today_track.alis_lot or 0
        satis_lot = today_track.satis_lot or 0

        # Durum (günlük değişime göre — dünkü kapanışa kıyasla)
        # NOT: pct_change (HA fiyatına göre) DEĞİL, daily_pct (dünkü kapanışa göre)
        # Tavan/Taban günlük limit olduğu için daily_pct kullanılmalı
        if daily_pct >= 9.5:
            durum = "tavan"
        elif daily_pct <= -9.5:
            durum = "taban"
        elif daily_pct > 0:
            durum = "alici_kapatti"
        elif daily_pct < 0:
            durum = "satici_kapatti"
        else:
            durum = "not_kapatti"

        # E.D.O (Kumulatif El Degistirme Orani)
        edo_pct = None
        if ipo.senet_sayisi and ipo.senet_sayisi > 0 and ipo.cumulative_volume:
            edo_pct = round((ipo.cumulative_volume / ipo.senet_sayisi) * 100, 2)

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
            "edo_pct": edo_pct,
        })

    if not stocks:
        return {"status": "ok", "message": "Ilk 10 gun icinde hisse yok, tweet atilmadi.", "stocks": []}

    # Tweet at
    from app.services.twitter_service import tweet_opening_summary
    from app.services.admin_telegram import notify_tweet_sent

    tickers_str = ", ".join(s["ticker"] for s in stocks)
    tw_ok = tweet_opening_summary(stocks)
    await notify_tweet_sent("acilis_ozet", tickers_str, tw_ok, f"{len(stocks)} hisse")

    return {
        "status": "ok",
        "tweet_sent": tw_ok,
        "stocks_count": len(stocks),
        "stocks": stocks,
    }


# -------------------------------------------------------
# 25 Gun Performans Tweet Trigger (Manuel — Admin)
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-25day-tweet")
@limiter.limit("3/minute")
async def trigger_25day_tweet(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin panelden belirli bir ticker icin 25 gun performans tweetini tetikler.

    Body: {"admin_password": "...", "ticker": "FRMPL"}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    ticker = (payload.get("ticker") or "").strip().upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker gerekli")

    from app.models.ipo import IPO, IPOCeilingTrack

    result = await db.execute(
        select(IPO).where(IPO.ticker == ticker)
    )
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail=f"{ticker} bulunamadi")

    # Ceiling track verilerini oku
    track_result = await db.execute(
        select(IPOCeilingTrack)
        .where(IPOCeilingTrack.ipo_id == ipo.id)
        .order_by(IPOCeilingTrack.trading_day.asc())
        .limit(25)
    )
    tracks = track_result.scalars().all()

    if not tracks:
        raise HTTPException(status_code=404, detail=f"{ticker} icin ceiling track verisi yok")

    ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
    if ipo_price <= 0:
        raise HTTPException(status_code=400, detail=f"{ticker} ipo_price eksik")

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

    last_close = float(days_data[-1]["close"])
    total_pct = ((last_close - ipo_price) / ipo_price) * 100
    ceiling_d = sum(1 for t in tracks if t.hit_ceiling)
    floor_d = sum(1 for t in tracks if t.hit_floor)
    avg_lot = float(ipo.estimated_lots_per_person) if ipo.estimated_lots_per_person else None

    from app.services.twitter_service import tweet_25_day_performance
    tw_ok = tweet_25_day_performance(
        ipo, last_close, total_pct,
        ceiling_d, floor_d, avg_lot,
        days_data=days_data,
    )

    # Admin Telegram bildirim
    try:
        from app.services.admin_telegram import notify_tweet_sent
        await notify_tweet_sent(
            "25_gun_performans_manuel",
            ticker,
            tw_ok,
            f"Toplam: %{total_pct:+.1f} | Tavan: {ceiling_d} | Taban: {floor_d}",
        )
    except Exception:
        pass

    return {
        "status": "ok",
        "ticker": ticker,
        "tweet_sent": tw_ok,
        "total_pct": round(total_pct, 2),
        "ceiling_days": ceiling_d,
        "floor_days": floor_d,
        "days_count": len(days_data),
    }


# -------------------------------------------------------
# BIST 30 SEED — adsiz.txt'den son 10 BIST 30 mesaji
# -------------------------------------------------------

@app.post("/api/v1/admin/seed-bist30-news")
@limiter.limit("10/minute")
async def seed_bist30_news(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """BIST 30 seed haberlerini yeniden ekler. Eski seed kayitlari silinir."""
    import os

    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    # BIST 30 seed mesajlari — 3 tip bildirim (fiyat yok, sadece GAP)
    seed_data = [
        {
            "telegram_message_id": 7200993,
            "message_type": "seans_ici_pozitif",
            "ticker": "SISE",
            "parsed_title": "⚡ Seans İçi Pozitif Haber Yakalandı - SISE",
            "parsed_body": "Sembol: SISE\nKonu: kaynak kullanımı",
            "sentiment": "positive",
            "kap_notification_id": "6200993",
            "message_date": "2026-02-11T14:05:20",
        },
        {
            "telegram_message_id": 7200349,
            "message_type": "seans_ici_pozitif",
            "ticker": "ASELS",
            "parsed_title": "⚡ Seans İçi Pozitif Haber Yakalandı - ASELS",
            "parsed_body": "Sembol: ASELS\nKonu: savunma sanayi",
            "sentiment": "positive",
            "kap_notification_id": "6200349",
            "message_date": "2026-02-11T11:06:03",
        },
        {
            "telegram_message_id": 7199734,
            "message_type": "seans_disi_acilis",
            "ticker": "FROTO",
            "gap_pct": -0.78,
            "parsed_title": "📊 Seans Dışı Haber Yakalanan Hisse Açılışı - FROTO",
            "parsed_body": "Sembol: FROTO\nGap: %-0.78",
            "sentiment": "positive",
            "kap_notification_id": "6199734",
            "expected_trading_date": "2026-02-11",
            "message_date": "2026-02-11T09:56:00",
        },
        {
            "telegram_message_id": 71997342,
            "message_type": "borsa_kapali",
            "ticker": "FROTO",
            "parsed_title": "🌙 Seans Dışı Pozitif Haber Yakalandı - FROTO",
            "parsed_body": "Sembol: FROTO\nBeklenen İşlem Günü: 2026-02-11",
            "sentiment": "positive",
            "kap_notification_id": "6199734",
            "expected_trading_date": "2026-02-11",
            "message_date": "2026-02-11T07:02:37",
        },
        {
            "telegram_message_id": 7198749,
            "message_type": "seans_ici_pozitif",
            "ticker": "FROTO",
            "parsed_title": "⚡ Seans İçi Pozitif Haber Yakalandı - FROTO",
            "parsed_body": "Sembol: FROTO\nKonu: milyon eur",
            "sentiment": "positive",
            "kap_notification_id": "6198749",
            "message_date": "2026-02-10T16:31:48",
        },
        {
            "telegram_message_id": 7196982,
            "message_type": "seans_disi_acilis",
            "ticker": "FROTO",
            "gap_pct": -1.17,
            "parsed_title": "📊 Seans Dışı Haber Yakalanan Hisse Açılışı - FROTO",
            "parsed_body": "Sembol: FROTO\nGap: %-1.17",
            "sentiment": "positive",
            "kap_notification_id": "6196982",
            "expected_trading_date": "2026-02-10",
            "message_date": "2026-02-10T09:56:00",
        },
        {
            "telegram_message_id": 71969822,
            "message_type": "borsa_kapali",
            "ticker": "FROTO",
            "parsed_title": "🌙 Seans Dışı Pozitif Haber Yakalandı - FROTO",
            "parsed_body": "Sembol: FROTO\nBeklenen İşlem Günü: 2026-02-10",
            "sentiment": "positive",
            "kap_notification_id": "6196982",
            "expected_trading_date": "2026-02-10",
            "message_date": "2026-02-10T08:42:07",
        },
        {
            "telegram_message_id": 7186659,
            "message_type": "seans_ici_pozitif",
            "ticker": "ASELS",
            "parsed_title": "⚡ Seans İçi Pozitif Haber Yakalandı - ASELS",
            "parsed_body": "Sembol: ASELS\nKonu: savunma sanayi, seri üretim",
            "sentiment": "positive",
            "kap_notification_id": "6186659",
            "message_date": "2026-02-05T15:24:25",
        },
        {
            "telegram_message_id": 7182535,
            "message_type": "seans_ici_pozitif",
            "ticker": "ASELS",
            "parsed_title": "⚡ Seans İçi Pozitif Haber Yakalandı - ASELS",
            "parsed_body": "Sembol: ASELS\nKonu: milyon dolar",
            "sentiment": "positive",
            "kap_notification_id": "6182535",
            "message_date": "2026-02-04T10:47:33",
        },
        {
            "telegram_message_id": 7176202,
            "message_type": "seans_disi_acilis",
            "ticker": "ENKAI",
            "gap_pct": -2.50,
            "parsed_title": "📊 Seans Dışı Haber Yakalanan Hisse Açılışı - ENKAI",
            "parsed_body": "Sembol: ENKAI\nGap: %-2.50",
            "sentiment": "positive",
            "kap_notification_id": "6176202",
            "expected_trading_date": "2026-02-02",
            "message_date": "2026-02-02T09:56:00",
        },
        {
            "telegram_message_id": 7175970,
            "message_type": "seans_disi_acilis",
            "ticker": "ARCLK",
            "gap_pct": -0.44,
            "parsed_title": "📊 Seans Dışı Haber Yakalanan Hisse Açılışı - ARCLK",
            "parsed_body": "Sembol: ARCLK\nGap: %-0.44",
            "sentiment": "positive",
            "kap_notification_id": "6175970",
            "expected_trading_date": "2026-02-02",
            "message_date": "2026-02-02T09:56:00",
        },
    ]

    from decimal import Decimal as D

    # Eski seed kayitlarini sil (guncellenmis seed ile degistir)
    seed_msg_ids = [item["telegram_message_id"] for item in seed_data]
    await db.execute(
        TelegramNews.__table__.delete().where(
            TelegramNews.telegram_message_id.in_(seed_msg_ids)
        )
    )
    # Ayrica negatif sentiment kayitlarini da temizle
    await db.execute(
        TelegramNews.__table__.delete().where(
            TelegramNews.sentiment == "negative"
        )
    )

    inserted = 0
    for item in seed_data:

        news = TelegramNews(
            telegram_message_id=item["telegram_message_id"],
            chat_id="seed_bist30",
            message_type=item["message_type"],
            ticker=item["ticker"],
            price_at_time=D(str(item.get("price_at_time", 0))) if item.get("price_at_time") else None,
            raw_text=item.get("parsed_body", ""),
            parsed_title=item["parsed_title"],
            parsed_body=item["parsed_body"],
            sentiment=item["sentiment"],
            kap_notification_id=item.get("kap_notification_id"),
            expected_trading_date=datetime.fromisoformat(item["expected_trading_date"]).date() if item.get("expected_trading_date") else None,
            gap_pct=D(str(item["gap_pct"])) if item.get("gap_pct") is not None else None,
            prev_close_price=D(str(item["prev_close_price"])) if item.get("prev_close_price") else None,
            theoretical_open=D(str(item["theoretical_open"])) if item.get("theoretical_open") else None,
            message_date=datetime.fromisoformat(item["message_date"]),
            created_at=datetime.fromisoformat(item["message_date"]),
        )
        db.add(news)
        inserted += 1

    await db.commit()
    return {"status": "ok", "inserted": inserted, "total_seed": len(seed_data)}


@app.delete("/api/v1/admin/ceiling-track/{ticker}/{trading_day}")
@limiter.limit("10/minute")
async def delete_ceiling_track(
    request: Request,
    ticker: str,
    trading_day: int,
    password: str = Query(..., alias="password"),
    db: AsyncSession = Depends(get_db),
):
    """Belirli bir ceiling track kaydini siler (admin)."""
    if not _verify_admin_password(password):
        raise HTTPException(status_code=403, detail="Gecersiz admin sifresi")

    result = await db.execute(
        select(IPOCeilingTrack)
        .join(IPO, IPO.id == IPOCeilingTrack.ipo_id)
        .where(
            and_(
                IPO.ticker == ticker.upper(),
                IPOCeilingTrack.trading_day == trading_day,
            )
        )
    )
    track = result.scalar_one_or_none()
    if not track:
        raise HTTPException(status_code=404, detail=f"{ticker} gun {trading_day} bulunamadi")

    await db.delete(track)
    await db.flush()  # Silme islemini flush et

    # trading_day_count guncelle — max trading_day'e gore
    ipo_result = await db.execute(select(IPO).where(IPO.ticker == ticker.upper()))
    ipo = ipo_result.scalar_one_or_none()
    if ipo:
        from sqlalchemy import func as sa_func
        max_day_result = await db.execute(
            select(sa_func.max(IPOCeilingTrack.trading_day)).where(IPOCeilingTrack.ipo_id == ipo.id)
        )
        ipo.trading_day_count = max_day_result.scalar() or 0

    await db.commit()
    return {"status": "ok", "deleted": f"{ticker} gun {trading_day}"}


# -------------------------------------------------------
# TAVAN TAKIP PAKETLERI
# -------------------------------------------------------

@app.get("/api/v1/ceiling-tiers", response_model=list[CeilingTierOut])
async def list_ceiling_tiers():
    """Mevcut tavan takip paketlerini listeler."""
    return [
        CeilingTierOut(
            tier=tier_key,
            days=tier_info["days"],
            price_tl=tier_info["price_tl"],
            label=tier_info["label"],
        )
        for tier_key, tier_info in CEILING_TIER_PRICES.items()
    ]


@app.post("/api/v1/users/{device_id}/ceiling-subscriptions", response_model=CeilingSubscriptionOut)
async def create_ceiling_subscription(
    device_id: str,
    data: CeilingSubscriptionCreate,
    db: AsyncSession = Depends(get_db),
):
    """Tavan takip aboneligi satin al."""
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    if data.tier not in CEILING_TIER_PRICES:
        raise HTTPException(status_code=400, detail=f"Gecersiz paket: {data.tier}")

    ipo_result = await db.execute(select(IPO).where(IPO.id == data.ipo_id))
    ipo = ipo_result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")

    existing_result = await db.execute(
        select(CeilingTrackSubscription).where(
            and_(
                CeilingTrackSubscription.user_id == user.id,
                CeilingTrackSubscription.ipo_id == data.ipo_id,
                CeilingTrackSubscription.is_active == True,
            )
        )
    )
    existing = existing_result.scalar_one_or_none()
    if existing:
        existing_days = CEILING_TIER_PRICES[existing.tier]["days"]
        new_days = CEILING_TIER_PRICES[data.tier]["days"]
        if new_days <= existing_days:
            raise HTTPException(status_code=400, detail=f"Bu hisse icin zaten {existing.tier} paketi aktif.")
        existing.is_active = False

    tier_info = CEILING_TIER_PRICES[data.tier]

    subscription = CeilingTrackSubscription(
        user_id=user.id,
        ipo_id=data.ipo_id,
        tier=data.tier,
        tracking_days=tier_info["days"],
        price_paid_tl=tier_info["price_tl"],
        is_active=True,
    )
    db.add(subscription)
    ipo.ceiling_tracking_active = True

    await db.flush()
    return subscription


@app.get("/api/v1/users/{device_id}/ceiling-subscriptions", response_model=list[CeilingSubscriptionOut])
async def list_user_ceiling_subscriptions(
    device_id: str,
    active_only: bool = Query(True),
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin tavan takip aboneliklerini listeler."""
    result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    query = select(CeilingTrackSubscription).where(
        CeilingTrackSubscription.user_id == user.id
    )
    if active_only:
        query = query.where(CeilingTrackSubscription.is_active == True)

    query = query.order_by(desc(CeilingTrackSubscription.purchased_at))
    result = await db.execute(query)
    return list(result.scalars().all())


# -------------------------------------------------------
# REVENUECAT WEBHOOK
# -------------------------------------------------------

@app.post("/api/v1/webhooks/revenuecat")
@limiter.limit("30/minute")
async def revenuecat_webhook(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """RevenueCat webhook — abonelik olaylari."""
    # Webhook imza dogrulama — sahte istekleri engelle
    rc_secret = settings.REVENUECAT_WEBHOOK_SECRET
    if rc_secret:
        auth_header = request.headers.get("Authorization", "")
        expected = f"Bearer {rc_secret}"
        if not hmac.compare_digest(auth_header.encode(), expected.encode()):
            logger.warning("RevenueCat webhook: gecersiz Authorization header")
            raise HTTPException(status_code=401, detail="Unauthorized")
    elif settings.is_production:
        logger.error("RevenueCat webhook: REVENUECAT_WEBHOOK_SECRET ayarlanmamis — production'da reddedildi!")
        raise HTTPException(status_code=503, detail="Webhook dogrulama yapilandirmasi eksik")
    else:
        logger.warning("RevenueCat webhook: REVENUECAT_WEBHOOK_SECRET ayarlanmamis — development'ta dogrulama atlaniyor")

    # Payload sometimes contains {"event": null} (test ping veya bozuk gönderim)
    # `.get("event", {})` key yoksa default döner ama key VAR + value None ise None döner.
    event = payload.get("event") or {}
    if not isinstance(event, dict):
        logger.warning("RevenueCat webhook: 'event' dict degil — atlandi (type=%s)", type(event).__name__)
        return {"status": "skipped", "reason": "invalid_event"}
    event_type = event.get("type") or ""
    app_user_id = event.get("app_user_id") or ""
    product_id = event.get("product_id") or ""

    if not app_user_id:
        logger.warning("RevenueCat webhook: app_user_id bos — atlandi")
        return {"status": "skipped", "reason": "missing_app_user_id"}

    logger.info(f"RevenueCat webhook: {event_type} -- {app_user_id} -- {product_id}")

    result = await db.execute(
        select(User).where(User.device_id == app_user_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        # Fallback: cihaz degismis olabilir — revenue_cat_id ile ara
        sub_result = await db.execute(
            select(UserSubscription).where(UserSubscription.revenue_cat_id == app_user_id)
        )
        sub_with_rc = sub_result.scalar_one_or_none()
        if sub_with_rc:
            user_result = await db.execute(select(User).where(User.id == sub_with_rc.user_id))
            user = user_result.scalar_one_or_none()
        if not user:
            logger.warning(f"RevenueCat webhook: kullanici bulunamadi — app_user_id={app_user_id}")
            return {"status": "skipped", "reason": "user_not_found"}

    # Haber abonelikleri
    news_package_map = {
        "bist_finans_yildiz_monthly": "yildiz_pazar",
        "bist_finans_yildiz_annual": "yildiz_pazar",
        "bist_finans_ana_yildiz_monthly": "ana_yildiz",
        "bist_finans_ana_yildiz_annual": "ana_yildiz",
        # iOS App Store product ID'leri (underscore format)
        "ana_yildiz_aylik": "ana_yildiz",
        "ana_yildiz_yillik": "ana_yildiz",
        # Eski paketler (geriye donuk uyumluluk)
        "bist_finans_bist100_monthly": "yildiz_pazar",
        "bist_finans_bist100_annual": "yildiz_pazar",
        "bist_finans_bist30_monthly": "yildiz_pazar",
        "bist_finans_bist50_monthly": "yildiz_pazar",
        "bist_finans_all_monthly": "ana_yildiz",
    }

    notif_package_map = {
        "bist_finans_notif_tavan": "tavan_bozulma",
        "bist_finans_notif_taban": "taban_acilma",
        "bist_finans_notif_acilis": "gunluk_acilis_kapanis",
        "bist_finans_notif_yuzde_dusus": "yuzde_dusus",
        # iOS App Store tek seferlik bildirim ürünleri (aynı ID, platform farkı yok)
        "notif_tavan_15": "tavan_bozulma",
        "notif_taban_10": "taban_acilma",
        "notif_acilis_5": "gunluk_acilis_kapanis",
        "notif_yuzde_dusus_20": "yuzde_dusus",
        "notif_edo_10": "edo",
        "notif_combo_44": "combo",
        # iOS App Store abonelik ürünleri (underscore format)
        "notif_bundle_3aylik": "quarterly",
        "notif_bundle_yillik": "all",
        # Eski urunler (geriye donuk uyumluluk)
        "bist_finans_notif_yuzde4": "yuzde_dusus",
        "bist_finans_notif_yuzde7": "yuzde_dusus",
        "bist_finans_notif_combo": "combo",
        "bist_finans_notif_quarterly": "quarterly",
        "bist_finans_notif_semiannual": "quarterly",  # Eski 6 aylik → quarterly olarak isle
        "bist_finans_notif_annual": "all",
    }

    ceiling_package_map = {
        "bist_finans_ceiling_5gun": "5_gun",
        "bist_finans_ceiling_10gun": "10_gun",
        "bist_finans_ceiling_15gun": "15_gun",
        "bist_finans_ceiling_20gun": "20_gun",
    }

    if product_id in news_package_map:
        result = await db.execute(
            select(UserSubscription).where(UserSubscription.user_id == user.id)
        )
        sub = result.scalar_one_or_none()
        if not sub:
            sub = UserSubscription(user_id=user.id, package="free")
            db.add(sub)

        if event_type in ["INITIAL_PURCHASE", "RENEWAL", "PRODUCT_CHANGE"]:
            sub.package = news_package_map.get(product_id, "free")
            sub.is_active = True
            sub.product_id = product_id
            sub.revenue_cat_id = app_user_id
            sub.store = event.get("store", "")
            expiration = event.get("expiration_at_ms")
            if expiration:
                sub.expires_at = datetime.fromtimestamp(expiration / 1000)

        elif event_type in ["CANCELLATION", "EXPIRATION"]:
            sub.is_active = False
            sub.package = "free"

    elif product_id in notif_package_map:
        if event_type in ["INITIAL_PURCHASE", "NON_RENEWING_PURCHASE"]:
            notif_type = notif_package_map[product_id]
            is_bundle = product_id in (
                "bist_finans_notif_annual",
                "bist_finans_notif_quarterly",
                "bist_finans_notif_semiannual",  # Eski, geriye donuk uyumluluk
                # iOS App Store product ID'leri
                "notif_bundle_yillik",
                "notif_bundle_3aylik",
            )

            ipo_id = event.get("metadata", {}).get("ipo_id")

            # Fiyat belirleme
            bundle_prices = {
                "bist_finans_notif_annual": ANNUAL_BUNDLE_PRICE,
                "bist_finans_notif_quarterly": QUARTERLY_PRICE,
                "bist_finans_notif_combo": COMBO_PRICE,
                # iOS App Store
                "notif_bundle_yillik": ANNUAL_BUNDLE_PRICE,
                "notif_bundle_3aylik": QUARTERLY_PRICE,
                "notif_combo_44": COMBO_PRICE,
            }
            price = bundle_prices.get(product_id) or NOTIFICATION_TIER_PRICES.get(notif_type, {}).get("price_tl", 0)

            # Bundle ise — mevcut aktif bundle var mi kontrol et (duplicate engelleme)
            if is_bundle:
                existing_bundle = await db.execute(
                    select(StockNotificationSubscription).where(
                        and_(
                            StockNotificationSubscription.user_id == user.id,
                            StockNotificationSubscription.is_annual_bundle == True,
                            StockNotificationSubscription.is_active == True,
                        )
                    )
                )
                existing = existing_bundle.scalar_one_or_none()
                if existing:
                    # Mevcut bundle var — yenile (sure uzat, bilgileri guncelle)
                    _now_rc = datetime.now(timezone.utc)
                    expiration_ms = event.get("expiration_at_ms")
                    if expiration_ms:
                        existing.expires_at = datetime.fromtimestamp(expiration_ms / 1000, tz=timezone.utc)
                    existing.revenue_cat_id = app_user_id
                    existing.store = event.get("store", "")
                    existing.product_id = product_id
                    logger.info(
                        "RC webhook: Mevcut bundle yenilendi user_id=%s, expires=%s",
                        user.id, existing.expires_at,
                    )
                    await db.flush()
                    return {"status": "ok"}

            # expires_at hesapla — RC event'ten veya product_id'den
            _now_rc = datetime.now(timezone.utc)
            expiration_ms = event.get("expiration_at_ms")
            if expiration_ms:
                rc_expires_at = datetime.fromtimestamp(expiration_ms / 1000, tz=timezone.utc)
            elif is_bundle:
                bundle_days = 365 if ("annual" in product_id or "yillik" in product_id) else 90
                rc_expires_at = _now_rc + timedelta(days=bundle_days)
            else:
                rc_expires_at = None  # Tekil abonelik — 25 gun trading day bazli

            sub = StockNotificationSubscription(
                user_id=user.id,
                ipo_id=int(ipo_id) if ipo_id else None,
                notification_type=notif_type,
                is_annual_bundle=is_bundle,
                price_paid_tl=price,
                is_active=True,
                revenue_cat_id=app_user_id,
                store=event.get("store", ""),
                product_id=product_id,
                purchased_at=_now_rc,
                expires_at=rc_expires_at,
            )
            db.add(sub)

        elif event_type in ["CANCELLATION", "EXPIRATION", "BILLING_ISSUE"]:
            # Bildirim aboneliklerini iptal et
            notif_subs_result = await db.execute(
                select(StockNotificationSubscription).where(
                    and_(
                        StockNotificationSubscription.user_id == user.id,
                        StockNotificationSubscription.product_id == product_id,
                        StockNotificationSubscription.is_active == True,
                    )
                )
            )
            notif_subs = notif_subs_result.scalars().all()
            cancelled_count = 0
            for ns in notif_subs:
                ns.is_active = False
                cancelled_count += 1
            if cancelled_count:
                logger.info(
                    "RC webhook: %s — %d bildirim aboneligi iptal edildi, user_id=%s, product=%s",
                    event_type, cancelled_count, user.id, product_id,
                )

    elif product_id in ceiling_package_map:
        if event_type in ["INITIAL_PURCHASE", "NON_RENEWING_PURCHASE"]:
            tier = ceiling_package_map[product_id]
            tier_info = CEILING_TIER_PRICES[tier]
            ipo_id = event.get("metadata", {}).get("ipo_id")

            if ipo_id:
                subscription = CeilingTrackSubscription(
                    user_id=user.id,
                    ipo_id=int(ipo_id),
                    tier=tier,
                    tracking_days=tier_info["days"],
                    price_paid_tl=tier_info["price_tl"],
                    is_active=True,
                    revenue_cat_id=app_user_id,
                    store=event.get("store", ""),
                    product_id=product_id,
                )
                db.add(subscription)

    await db.flush()

    # ─── Admin Telegram bildirimi — paket satın alma / trial / yenileme / iptal ───
    # Her webhook event'i sonrası admin'e özet bildirim gitsin (stale token dışında tek en kritik olay)
    try:
        # Hangi kategori?
        if product_id in news_package_map:
            pkg_name = news_package_map.get(product_id, "free")
        elif product_id in notif_package_map:
            pkg_name = f"bildirim:{notif_package_map.get(product_id, '?')}"
        elif product_id in ceiling_package_map:
            pkg_name = f"tavan:{ceiling_package_map.get(product_id, '?')}"
        else:
            pkg_name = product_id or "?"

        # Trial kontrolü
        period_type = (event.get("period_type") or "").upper()
        is_trial = period_type in ("TRIAL", "INTRO")

        # Fiyat tahmini (RC event'inden veya bundle map'ten)
        price_in_purchased = event.get("price_in_purchased_currency")
        price_tl_val: Optional[float] = None
        if price_in_purchased is not None:
            try:
                price_tl_val = float(price_in_purchased)
            except (TypeError, ValueError):
                price_tl_val = None

        # Sadece anlamli event'lerde gonder (spam engeli)
        # Google Play test purchase'da aylik abonelik 5 dk'da bir yeniliyor — Telegram spam'i engelle.
        # Ayni user icin son 1 saat icinde RENEWAL geldiyse (test ortami) bildirim atma.
        _SKIP_RENEWAL = False
        if event_type == "RENEWAL":
            try:
                if not hasattr(app.state, "_renewal_dedup"):
                    app.state._renewal_dedup = {}  # type: ignore
                cache = app.state._renewal_dedup  # type: ignore
                now_ts = datetime.utcnow().timestamp()
                key = f"{user.id}:{product_id}"
                last_ts = cache.get(key, 0)
                if now_ts - last_ts < 3600:  # 1 saat icinde tekrar geldiyse test = sessiz
                    _SKIP_RENEWAL = True
                cache[key] = now_ts
                # 256 entry'den fazlaysa eski olanlari at (memory cap)
                if len(cache) > 256:
                    for k, v in sorted(cache.items(), key=lambda x: x[1])[:64]:
                        cache.pop(k, None)
            except Exception:
                pass

        if not _SKIP_RENEWAL and event_type in (
            "INITIAL_PURCHASE",
            "RENEWAL",
            "NON_RENEWING_PURCHASE",
            "PRODUCT_CHANGE",
            "CANCELLATION",
            "EXPIRATION",
        ):
            from app.services.admin_telegram import notify_subscription_purchase
            await notify_subscription_purchase(
                event_type=event_type,
                user_id=user.id,
                device_id=user.device_id or "?",
                platform=user.platform or "?",
                product_id=product_id,
                package=pkg_name,
                price_tl=price_tl_val,
                store=event.get("store", ""),
                is_trial=is_trial,
            )
    except Exception as e:
        # Bildirim hatası webhook akışını bozmasın
        logger.warning("RevenueCat telegram bildirim hatasi: %s", e)

    return {"status": "ok"}


# -------------------------------------------------------
# SUBSCRIPTION SYNC — Frontend → Backend senkronizasyon
# RevenueCat webhook çalışmasa bile abonelik kaydını günceller
# -------------------------------------------------------

class SyncSubscriptionRequest(BaseModel):
    """Frontend'den gelen abonelik senkronizasyon verisi."""
    package: str  # ana_yildiz, yildiz_pazar, free
    is_active: bool = True
    store: Optional[str] = None  # play_store, app_store
    product_id: Optional[str] = None
    expiration_date: Optional[str] = None  # ISO 8601 string


@app.post("/api/v1/users/{device_id}/sync-subscription")
@limiter.limit("10/minute")
async def sync_subscription(
    device_id: str,
    request: Request,
    body: SyncSubscriptionRequest,
    db: AsyncSession = Depends(get_db),
):
    """Frontend'den abonelik durumu senkronizasyonu.

    RevenueCat SDK aktif abonelik tespit ettiğinde frontend bu endpoint'i çağırır.
    Webhook çalışmasa bile abonelik kaydı güncellenir.
    Mevcut webhook ve puan sistemi etkilenMEZ — sadece ek güvenlik katmanı.
    """
    result = await db.execute(select(User).where(User.device_id == device_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanıcı bulunamadı")

    package = body.package
    if package not in ("ana_yildiz", "yildiz_pazar", "free"):
        raise HTTPException(status_code=400, detail="Geçersiz paket")

    # UserSubscription bul veya oluştur
    result = await db.execute(
        select(UserSubscription).where(UserSubscription.user_id == user.id)
    )
    sub = result.scalar_one_or_none()

    if not sub:
        sub = UserSubscription(user_id=user.id, package="free")
        db.add(sub)

    # KORUMA: Eğer mevcut abonelik wallet (puan) ile alınmış ve hâlâ aktifse,
    # frontend sync ile üzerine YAZMA — puan aboneliğini bozma!
    if sub.store == "wallet" and sub.is_active and sub.package != "free":
        # Wallet abonelik aktif — sadece webhook veya expiry ile değişir
        # Ama gelen paket de aktifse ve store play_store/app_store ise → güncelle
        # (Kullanıcı hem puanla hem parayla almış olabilir — store aboneliği öncelikli)
        if not (body.is_active and body.store in ("play_store", "app_store")):
            logger.info(
                "Sync skip: wallet sub aktif, frontend sync atlandı — device=%s, pkg=%s",
                device_id, sub.package,
            )
            return {"status": "ok", "package": sub.package, "is_active": sub.is_active}

    # Upgrade guvenlik kontrolu — sahte sync istegini engelle
    KNOWN_NEWS_PRODUCTS = {
        "bist_finans_yildiz_monthly", "bist_finans_yildiz_annual",
        "bist_finans_ana_yildiz_monthly", "bist_finans_ana_yildiz_annual",
        # Google Play product ID'leri (colon format)
        "ana_yildiz:aylik", "ana_yildiz:yillik",
        # iOS App Store product ID'leri (underscore format)
        "ana_yildiz_aylik", "ana_yildiz_yillik",
        # Eski paketler (geriye donuk uyumluluk)
        "bist_finans_bist100_monthly", "bist_finans_bist100_annual",
        "bist_finans_bist30_monthly", "bist_finans_bist50_monthly",
        "bist_finans_all_monthly",
    }
    if body.is_active and package != "free":
        if body.store and body.store not in ("play_store", "app_store"):
            raise HTTPException(status_code=400, detail="Gecerli store gerekli (play_store veya app_store)")
        # product_id opsiyonel — eski client'lar göndermeyebilir
        if body.product_id and body.product_id not in KNOWN_NEWS_PRODUCTS:
            logger.warning("Sync: bilinmeyen product_id=%s, device=%s — kabul ediliyor", body.product_id, device_id)
        if not body.expiration_date:
            raise HTTPException(status_code=400, detail="expiration_date gerekli")
        try:
            exp_check = body.expiration_date.replace("Z", "+00:00")
            exp_dt = datetime.fromisoformat(exp_check)
            if exp_dt < datetime.now(timezone.utc):
                raise HTTPException(status_code=400, detail="Suresi dolmus abonelik senkronize edilemez")
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Gecersiz expiration_date formati")

    # Güncelle
    if body.is_active and package != "free":
        sub.package = package
        sub.is_active = True
        sub.store = body.store or ("play_store" if not sub.store else sub.store)
        sub.revenue_cat_id = device_id
        if body.product_id:
            sub.product_id = body.product_id
        if body.expiration_date:
            try:
                exp_str = body.expiration_date.replace("Z", "+00:00")
                sub.expires_at = datetime.fromisoformat(exp_str)
            except (ValueError, TypeError):
                pass  # Geçersiz tarih — es geç

    await db.flush()

    logger.info(
        "Subscription sync: device=%s, package=%s, active=%s, store=%s",
        device_id, sub.package, sub.is_active, sub.store,
    )

    return {"status": "ok", "package": sub.package, "is_active": sub.is_active}


# -------------------------------------------------------
# TEMETTU (YAKINDA)
# -------------------------------------------------------

@app.get("/api/v1/dividends", response_model=list[DividendOut])
async def list_dividends(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Temettu beklentileri listesi."""
    result = await db.execute(
        select(Dividend)
        .order_by(desc(Dividend.expected_dividend_yield_pct))
        .limit(limit)
        .offset(offset)
    )
    return list(result.scalars().all())


@app.get("/api/v1/dividends/{ticker}", response_model=DividendOut)
async def get_dividend_by_ticker(ticker: str, db: AsyncSession = Depends(get_db)):
    """Belirli bir hissenin temettu beklentisini getirir."""
    result = await db.execute(
        select(Dividend).where(Dividend.ticker == ticker.upper())
    )
    dividend = result.scalar_one_or_none()
    if not dividend:
        raise HTTPException(status_code=404, detail=f"Temettu verisi bulunamadi: {ticker}")
    return dividend


# -------------------------------------------------------
# SEED DATA ENDPOINT (gecici — production'a veri yuklemek icin)
# -------------------------------------------------------

@app.post("/api/v1/admin/seed-ipos")
@limiter.limit("5/minute")
async def seed_ipos(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Local DB'den export edilen IPO verilerini production'a yukler.

    Bu endpoint sadece bos DB'ye ilk veri yuklemesi icin kullanilir.
    Gonderilen JSON:
        { "admin_password": "...", "ipos": [...], "allocations": [...] }
    """
    from app.config import get_settings
    _settings = get_settings()

    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ipos_data = payload.get("ipos", [])
    allocs_data = payload.get("allocations", [])
    created_ipos = 0
    created_allocs = 0

    for ipo_raw in ipos_data:
        # Ticker ile kontrol — zaten varsa atla
        ticker = ipo_raw.get("ticker")
        if ticker:
            existing = await db.execute(
                select(IPO).where(IPO.ticker == ticker)
            )
            if existing.scalar_one_or_none():
                continue

        from datetime import date as _date
        def _parse_date(val):
            if not val:
                return None
            try:
                return _date.fromisoformat(str(val)[:10])
            except Exception:
                return None

        from decimal import Decimal
        def _dec(val):
            if val is None:
                return None
            try:
                return Decimal(str(val))
            except Exception:
                return None

        ipo = IPO(
            company_name=ipo_raw.get("company_name", ""),
            ticker=ticker,
            status=ipo_raw.get("status", "newly_approved"),
            ipo_price=_dec(ipo_raw.get("ipo_price")),
            total_lots=ipo_raw.get("total_lots"),
            offering_size_tl=_dec(ipo_raw.get("offering_size_tl")),
            capital_increase_lots=ipo_raw.get("capital_increase_lots"),
            partner_sale_lots=ipo_raw.get("partner_sale_lots"),
            subscription_start=_parse_date(ipo_raw.get("subscription_start")),
            subscription_end=_parse_date(ipo_raw.get("subscription_end")),
            subscription_hours=ipo_raw.get("subscription_hours"),
            trading_start=_parse_date(ipo_raw.get("trading_start")),
            spk_approval_date=_parse_date(ipo_raw.get("spk_approval_date")),
            expected_trading_date=_parse_date(ipo_raw.get("expected_trading_date")),
            spk_bulletin_no=ipo_raw.get("spk_bulletin_no"),
            distribution_completed=bool(ipo_raw.get("distribution_completed")),
            distribution_method=ipo_raw.get("distribution_method"),
            distribution_description=ipo_raw.get("distribution_description"),
            participation_method=ipo_raw.get("participation_method"),
            participation_description=ipo_raw.get("participation_description"),
            public_float_pct=_dec(ipo_raw.get("public_float_pct")),
            discount_pct=_dec(ipo_raw.get("discount_pct")),
            market_segment=ipo_raw.get("market_segment"),
            lead_broker=ipo_raw.get("lead_broker"),
            estimated_lots_per_person=ipo_raw.get("estimated_lots_per_person"),
            min_application_lot=ipo_raw.get("min_application_lot"),
            company_description=ipo_raw.get("company_description"),
            sector=ipo_raw.get("sector"),
            ceiling_tracking_active=bool(ipo_raw.get("ceiling_tracking_active")),
            first_day_close_price=_dec(ipo_raw.get("first_day_close_price")),
            trading_day_count=ipo_raw.get("trading_day_count", 0),
            total_applicants=ipo_raw.get("total_applicants"),
        )
        db.add(ipo)
        await db.flush()

        # Allocations for this IPO
        old_id = ipo_raw.get("id")
        for alloc_raw in allocs_data:
            if alloc_raw.get("ipo_id") == old_id:
                alloc = IPOAllocation(
                    ipo_id=ipo.id,
                    group_name=alloc_raw.get("group_name", ""),
                    allocation_pct=_dec(alloc_raw.get("allocation_pct")),
                    allocated_lots=alloc_raw.get("allocated_lots"),
                    participant_count=alloc_raw.get("participant_count"),
                    avg_lot_per_person=alloc_raw.get("avg_lot_per_person"),
                )
                db.add(alloc)
                created_allocs += 1

        created_ipos += 1

    await db.flush()
    return {
        "status": "ok",
        "created_ipos": created_ipos,
        "created_allocations": created_allocs,
    }


@app.post("/api/v1/admin/bulk-allocations")
@limiter.limit("10/minute")
async def bulk_allocations(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Mevcut IPO'lara toplu tahsisat verisi ekler.

    JSON:
        {
            "admin_password": "...",
            "items": [
                {
                    "ipo_id": 9,
                    "total_applicants": 431380,
                    "groups": [
                        {"group_name": "bireysel", "allocation_pct": 50, "allocated_lots": 42250000, "participant_count": 431279, "avg_lot_per_person": 98},
                        {"group_name": "kurumsal_yurtici", "allocation_pct": 50, "allocated_lots": 42250000, "participant_count": 101}
                    ]
                }
            ]
        }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from decimal import Decimal as _Dec

    items = payload.get("items", [])
    updated_ipos = 0
    created_allocs = 0

    for item in items:
        ipo_id = item.get("ipo_id")
        if not ipo_id:
            continue

        ipo_result = await db.execute(select(IPO).where(IPO.id == ipo_id))
        ipo = ipo_result.scalar_one_or_none()
        if not ipo:
            continue

        # Mevcut allocation'lari sil (varsa)
        await db.execute(
            delete(IPOAllocation).where(IPOAllocation.ipo_id == ipo_id)
        )

        # Yeni allocation'lari ekle
        for grp in item.get("groups", []):
            alloc = IPOAllocation(
                ipo_id=ipo_id,
                group_name=grp.get("group_name", ""),
                allocation_pct=_Dec(str(grp["allocation_pct"])) if grp.get("allocation_pct") is not None else None,
                allocated_lots=grp.get("allocated_lots"),
                participant_count=grp.get("participant_count"),
                avg_lot_per_person=_Dec(str(grp["avg_lot_per_person"])) if grp.get("avg_lot_per_person") is not None else None,
            )
            db.add(alloc)
            created_allocs += 1

        # IPO guncelle
        ipo.allocation_announced = True
        ipo.total_applicants = item.get("total_applicants")
        updated_ipos += 1

        # Tweet #3: Kesinlesen Dagitim Sonuclari
        try:
            from app.services.twitter_service import tweet_allocation_results
            from app.services.admin_telegram import notify_tweet_sent
            # Allocation listesini dict formatinda gonder
            alloc_dicts = []
            for grp in item.get("groups", []):
                alloc_dicts.append({
                    "group_name": grp.get("group_name", ""),
                    "allocation_pct": grp.get("allocation_pct"),
                    "allocated_lots": grp.get("allocated_lots"),
                    "participant_count": grp.get("participant_count"),
                    "avg_lot_per_person": grp.get("avg_lot_per_person"),
                })
            tw_ok = tweet_allocation_results(ipo, alloc_dicts)
            await notify_tweet_sent("dagitim_sonucu", ipo.ticker or ipo.company_name, tw_ok)
        except Exception:
            pass  # Tweet hatasi sistemi etkilemez

    await db.flush()
    return {
        "status": "ok",
        "updated_ipos": updated_ipos,
        "created_allocations": created_allocs,
    }


# -------------------------------------------------------
# ADMIN: TEST NOTIFICATION ENDPOINT
# -------------------------------------------------------

@app.post("/api/v1/admin/test-notification")
@limiter.limit("10/minute")
async def test_notification(request: Request, payload: dict):
    """Firebase push bildirim test endpoint'i.

    FCM token verilirse gercek push gonderir.
    Token verilmezse sadece Firebase durumunu raporlar.

    Body: {
        "admin_password": "...",
        "fcm_token": "...",           (opsiyonel)
        "title": "Test Bildirimi",    (opsiyonel)
        "body": "Bu bir test..."      (opsiyonel)
    }
    """
    from app.services.notification import is_firebase_initialized, _init_firebase

    settings = get_settings()

    # Admin yetki kontrolu
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    # Firebase'i baslatmayi dene (henuz baslatilmadiysa)
    _init_firebase()

    firebase_ok = is_firebase_initialized()
    fcm_token = payload.get("fcm_token")
    expo_token = payload.get("expo_push_token")

    # Token yoksa sadece durum raporla
    if not fcm_token and not expo_token:
        return {
            "firebase_initialized": firebase_ok,
            "push_sent": False,
            "push_error": None,
            "message": "Token verilmedi — sadece Firebase durumu raporlandi."
                       + (" Firebase AKTIF ✅" if firebase_ok else " Firebase INAKTIF ❌"),
        }

    title = payload.get("title", "🔔 BIST Finans Test")
    body = payload.get("body", "Bu bir test bildirimidir. Push calisiyor! ✅")

    # --- Expo Push Token varsa Expo Push API kullan ---
    if expo_token:
        try:
            import httpx

            push_message = {
                "to": expo_token,
                "sound": "default",
                "title": title,
                "body": body,
                "data": {"type": "test", "timestamp": datetime.now().isoformat()},
            }

            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "https://exp.host/--/api/v2/push/send",
                    json=push_message,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                    timeout=10.0,
                )
                result = resp.json()

            return {
                "firebase_initialized": firebase_ok,
                "push_sent": True,
                "push_error": None,
                "expo_response": result,
                "token_type": "expo",
                "message": "Expo push bildirim gonderildi! ✅",
            }
        except Exception as e:
            return {
                "firebase_initialized": firebase_ok,
                "push_sent": False,
                "push_error": str(e),
                "token_type": "expo",
                "message": f"Expo push hatasi: {e}",
            }

    # --- FCM Token varsa Firebase kullan ---
    if not firebase_ok:
        return {
            "firebase_initialized": False,
            "push_sent": False,
            "push_error": "Firebase baslatilamamis",
            "message": "Firebase init basarisiz — push gonderilemez.",
        }

    try:
        from firebase_admin import messaging

        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
            ),
            data={
                "type": "test",
                "timestamp": datetime.now().isoformat(),
            },
            token=fcm_token,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(
                    sound="default",
                    channel_id="default_v2",
                ),
            ),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(
                        sound="default",
                        badge=1,
                    ),
                ),
            ),
        )

        response = messaging.send(message)
        return {
            "firebase_initialized": True,
            "push_sent": True,
            "push_error": None,
            "fcm_response": response,
            "token_type": "fcm",
            "message": "FCM push bildirim gonderildi! ✅",
        }

    except Exception as e:
        return {
            "firebase_initialized": True,
            "push_sent": False,
            "push_error": str(e),
            "token_type": "fcm",
            "message": f"FCM push hatasi: {e}",
        }


@app.post("/api/v1/admin/users")
@limiter.limit("10/minute")
async def admin_list_users(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: Kayitli kullanicilari ve FCM tokenlarini listeler."""
    settings = get_settings()

    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    limit = payload.get("limit", 50)
    limit = min(max(limit, 1), 200)

    result = await db.execute(
        select(User).order_by(desc(User.created_at)).limit(limit)
    )
    users = result.scalars().all()

    return [
        {
            "id": u.id,
            "device_id": u.device_id,
            "platform": u.platform,
            "fcm_token": u.fcm_token[:30] + "..." if u.fcm_token and len(u.fcm_token) > 30 else u.fcm_token,
            "expo_push_token": getattr(u, "expo_push_token", None),
            "app_version": u.app_version,
            "notifications_enabled": u.notifications_enabled,
            "created_at": str(u.created_at) if u.created_at else None,
        }
        for u in users
    ]


# -------------------------------------------------------
# Admin: Eksik IPO verilerini InfoYatirim'dan doldur
# -------------------------------------------------------

@app.post("/api/v1/admin/fill-missing-ipo-data")
@limiter.limit("5/minute")
async def admin_fill_missing_ipo_data(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """InfoYatirim scraper'i calistirarak eksik IPO verilerini doldurur.

    total_lots, subscription_start/end, total_applicants, trading_start
    alanlari NULL olan IPO'lari InfoYatirim'dan gelen veriyle eslestirir.
    """
    settings = get_settings()
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.scrapers.infoyatirim_scraper import InfoYatirimScraper
    from app.services.ipo_service import IPOService

    # 1. InfoYatirim'dan tum halka arz verilerini cek
    scraper = InfoYatirimScraper()
    try:
        scraped_ipos = await scraper.fetch_all_ipos(max_pages=5)
    finally:
        await scraper.close()

    if not scraped_ipos:
        return {"success": False, "message": "InfoYatirim'dan veri cekilemedi", "updated": 0}

    # 2. Ticker bazli lookup dict olustur
    scraped_by_ticker = {}
    for s in scraped_ipos:
        t = s.get("ticker")
        if t:
            scraped_by_ticker[t.upper()] = s

    # 3. DB'den tum IPO'lari al
    result = await db.execute(select(IPO).order_by(IPO.id))
    all_ipos = result.scalars().all()

    updated_list = []
    ipo_service = IPOService(db)

    for ipo in all_ipos:
        if not ipo.ticker:
            continue

        scraped = scraped_by_ticker.get(ipo.ticker.upper())
        if not scraped:
            continue

        changes = {}

        # Eksik alanlari doldur
        if ipo.total_lots is None and scraped.get("total_lots"):
            changes["total_lots"] = scraped["total_lots"]

        if ipo.subscription_start is None and scraped.get("subscription_start"):
            changes["subscription_start"] = scraped["subscription_start"]

        if ipo.subscription_end is None and scraped.get("subscription_end"):
            changes["subscription_end"] = scraped["subscription_end"]

        if ipo.total_applicants is None and scraped.get("total_applicants"):
            changes["total_applicants"] = scraped["total_applicants"]

        if ipo.trading_start is None and scraped.get("trading_start"):
            changes["trading_start"] = scraped["trading_start"]

        if ipo.distribution_method is None and scraped.get("distribution_method"):
            changes["distribution_method"] = scraped["distribution_method"]

        if ipo.distribution_description is None and scraped.get("distribution_description"):
            changes["distribution_description"] = scraped["distribution_description"]

        if ipo.participation_method is None and scraped.get("participation_method"):
            changes["participation_method"] = scraped["participation_method"]

        if ipo.participation_description is None and scraped.get("participation_description"):
            changes["participation_description"] = scraped["participation_description"]

        if changes:
            for key, value in changes.items():
                setattr(ipo, key, value)
            updated_list.append({
                "ticker": ipo.ticker,
                "fields_updated": list(changes.keys()),
            })

    await db.commit()

    return {
        "success": True,
        "scraped_count": len(scraped_ipos),
        "updated_count": len(updated_list),
        "updated": updated_list,
    }


# -------------------------------------------------------
# Admin: fill-ceiling-data KALDIRILDI — Yahoo Finance bagimliligi cikarildi
# Tum ceiling track verisi artik Excel sync uzerinden geliyor.
# -------------------------------------------------------


# -------------------------------------------------------
# Admin: Yeni IPO Ekle (newly_approved olarak)
# -------------------------------------------------------

@app.post("/api/v1/admin/create-ipo")
@limiter.limit("10/minute")
async def admin_create_ipo(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin panelden yeni halka arz ekler — newly_approved olarak.

    SPK bulteni haricinde yeni IPO eklemenin TEK yolu budur.
    Gonderilen JSON:
        {
            "admin_password": "...",
            "company_name": "Sirket Adi A.S.",
            "ticker": "XXXX",        (opsiyonel)
            "ipo_price": 10.50,       (opsiyonel)
            "spk_bulletin_no": "2026/7"  (opsiyonel)
            ... diger IPO alanlari
        }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.services.ipo_service import IPOService

    ipo_data = {k: v for k, v in payload.items() if k != "admin_password"}
    ipo_data["status"] = "newly_approved"  # Admin her zaman newly_approved ekler

    ipo_service = IPOService(db)
    ipo = await ipo_service.create_or_update_ipo(ipo_data, allow_create=True)

    if not ipo:
        raise HTTPException(status_code=400, detail="IPO olusturulamadi")

    await db.commit()

    return {
        "success": True,
        "ipo_id": ipo.id,
        "ticker": ipo.ticker,
        "company_name": ipo.company_name,
        "status": ipo.status,
    }


# -------------------------------------------------------
# Admin: AI IPO Rapor Üret (Manuel Tetikleme)
# -------------------------------------------------------

@app.post("/api/v1/admin/generate-ai-report/{ipo_id}")
@limiter.limit("10/minute")
async def admin_generate_ai_report(request: Request, ipo_id: int, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin panelden belirli bir IPO icin AI rapor uretimini tetikler.

    force=True gonderilirse mevcut rapor silinir ve yeniden uretilir.
    Rapor background task olarak uretilir (30-60 sn surer).
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from sqlalchemy import select
    from app.models.ipo import IPO

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()

    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    force = payload.get("force", False)

    # force=True ise mevcut raporu sil
    if force and ipo.ai_report:
        ipo.ai_report = None
        ipo.ai_report_generated_at = None
        await db.commit()
        logger.info(f"Admin: AI rapor silindi (force) — {ipo.ticker or ipo.company_name}")

    if ipo.ai_report and not force:
        return {
            "success": True,
            "message": "Bu IPO icin zaten rapor var. force=True ile yeniden uretebilirsiniz.",
            "ipo_id": ipo.id,
            "ticker": ipo.ticker,
        }

    # Senkron rapor uret — await ile bekle (Render'da create_task guvenilir degil)
    from app.services.ai_ipo_analyzer import generate_and_save_ipo_report
    ticker = ipo.ticker or ipo.company_name

    logger.info(f"Admin: AI rapor uretimi baslatiliyor (senkron) — {ticker} (id={ipo_id})")

    try:
        success = await generate_and_save_ipo_report(ipo.id, force=force)
    except Exception as e:
        logger.error(f"Admin: AI rapor uretim hatasi — {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Rapor uretim hatasi: {str(e)[:200]}")

    if not success:
        raise HTTPException(status_code=500, detail=f"{ticker} icin AI rapor uretilemedi. Loglari kontrol edin.")

    return {
        "success": True,
        "message": f"{ticker} icin AI rapor basariyla uretildi.",
        "ipo_id": ipo.id,
        "ticker": ipo.ticker,
    }


# -------------------------------------------------------
# Admin: AI IPO Rapor Güncelle (Düzenle/Kaydet)
# -------------------------------------------------------

@app.put("/api/v1/admin/update-ai-report/{ipo_id}")
@limiter.limit("20/minute")
async def admin_update_ai_report(request: Request, ipo_id: int, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin panelden AI rapor icerigini duzenler ve kaydeder.

    Gonderilen JSON:
        {
            "admin_password": "...",
            "report": { ... guncel rapor JSON ... }
        }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    report_data = payload.get("report")
    if not report_data or not isinstance(report_data, dict):
        raise HTTPException(status_code=400, detail="report alani gerekli (JSON object)")

    from sqlalchemy import select
    from app.models.ipo import IPO

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()

    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    import json

    ipo.ai_report = json.dumps(report_data, ensure_ascii=False)
    await db.commit()

    logger.info(f"Admin: AI rapor guncellendi — {ipo.ticker or ipo.company_name} (id={ipo_id})")

    return {
        "success": True,
        "message": f"{ipo.ticker or ipo.company_name} AI raporu guncellendi.",
        "ipo_id": ipo.id,
    }


# -------------------------------------------------------
# Admin: AI Prompt Yönetimi (Okuma/Güncelleme)
# -------------------------------------------------------

@app.get("/api/v1/admin/ai-prompts")
@limiter.limit("30/minute")
async def admin_list_ai_prompts(request: Request):
    """Tüm AI prompt'larının listesini döndürür."""
    admin_pw = request.query_params.get("admin_password", "")
    if not _verify_admin_password(admin_pw):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    prompts = []
    _PROMPT_REGISTRY = [
        {"key": "kap-news", "label": "KAP Haber Puanlama", "category": "haber", "module": "app.services.ai_news_scorer", "getter": "get_system_prompt", "default_getter": "get_default_system_prompt"},
        {"key": "kap-analyzer", "label": "KAP Scraper Analiz", "category": "haber", "module": "app.services.kap_all_analyzer", "getter": "get_system_prompt", "default_getter": "get_default_system_prompt"},
        {"key": "market-close", "label": "Tavan/Taban Neden", "category": "haber", "module": "app.services.market_close_analyzer", "getter": "get_system_prompt", "default_getter": "get_default_system_prompt"},
        {"key": "morning-report", "label": "Sabah Raporu", "category": "rapor", "module": "app.services.ai_market_report", "getter": "get_morning_prompt", "default_getter": "get_default_morning_prompt"},
        {"key": "evening-report", "label": "Akşam Raporu", "category": "rapor", "module": "app.services.ai_market_report", "getter": "get_evening_prompt", "default_getter": "get_default_evening_prompt"},
        {"key": "ipo-report", "label": "Halka Arz Raporu", "category": "rapor", "module": "app.services.ai_ipo_analyzer", "getter": "get_system_prompt", "default_getter": "get_default_system_prompt"},
        {"key": "prospectus", "label": "İzahname Analiz", "category": "rapor", "module": "app.services.prospectus_analyzer", "getter": "get_system_prompt", "default_getter": "get_default_system_prompt"},
        {"key": "twitter-reply", "label": "Tweet Yanıt", "category": "twitter", "module": "app.services.twitter_reply_service", "getter": "get_reply_prompt", "default_getter": "get_default_reply_prompt"},
        {"key": "twitter-quote", "label": "Alıntı Analiz", "category": "twitter", "module": "app.services.twitter_reply_service", "getter": "get_quote_prompt", "default_getter": "get_default_quote_prompt"},
        {"key": "mentions-reply", "label": "Mention Yanıt", "category": "twitter", "module": "app.services.mentions_reply_service", "getter": "get_system_prompt", "default_getter": "get_default_system_prompt"},
        {"key": "spk-bulletin", "label": "SPK Bülten Analiz", "category": "spk", "module": "app.services.twitter_service", "getter": "get_bulletin_prompt", "default_getter": "get_default_bulletin_prompt"},
        {"key": "spk-app", "label": "SPK Başvuru Araştırma", "category": "spk", "module": "app.services.twitter_service", "getter": "get_spk_app_prompt", "default_getter": "get_default_spk_app_prompt"},
    ]
    import importlib
    for p in _PROMPT_REGISTRY:
        try:
            mod = importlib.import_module(p["module"])
            current = getattr(mod, p["getter"])()
            default = getattr(mod, p["default_getter"])()
            prompts.append({
                "prompt_type": p["key"],
                "label": p["label"],
                "category": p["category"],
                "is_custom": current != default,
                "prompt_length": len(current),
            })
        except Exception:
            prompts.append({"prompt_type": p["key"], "label": p["label"], "category": p["category"], "is_custom": False, "prompt_length": 0})
    return prompts


@app.get("/api/v1/admin/ai-prompt/{prompt_type}")
@limiter.limit("30/minute")
async def admin_get_ai_prompt(request: Request, prompt_type: str):
    """AI system prompt'unu oku. prompt_type: 'ipo-report' veya 'prospectus'."""
    # GET isteğinde password query param olarak gelir
    admin_pw = request.query_params.get("admin_password", "")
    if not _verify_admin_password(admin_pw):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    if prompt_type == "ipo-report":
        from app.services.ai_ipo_analyzer import get_system_prompt, get_default_system_prompt
        return {
            "prompt_type": "ipo-report",
            "current_prompt": get_system_prompt(),
            "is_custom": get_system_prompt() != get_default_system_prompt(),
        }
    elif prompt_type == "prospectus":
        from app.services.prospectus_analyzer import get_system_prompt, get_default_system_prompt
        return {
            "prompt_type": "prospectus",
            "current_prompt": get_system_prompt(),
            "is_custom": get_system_prompt() != get_default_system_prompt(),
        }
    elif prompt_type == "kap-news":
        from app.services.ai_news_scorer import get_system_prompt, get_default_system_prompt
        return {
            "prompt_type": "kap-news",
            "current_prompt": get_system_prompt(),
            "is_custom": get_system_prompt() != get_default_system_prompt(),
        }
    elif prompt_type == "kap-analyzer":
        from app.services.kap_all_analyzer import get_system_prompt, get_default_system_prompt
        return {
            "prompt_type": "kap-analyzer",
            "current_prompt": get_system_prompt(),
            "is_custom": get_system_prompt() != get_default_system_prompt(),
        }
    elif prompt_type == "market-close":
        from app.services.market_close_analyzer import get_system_prompt, get_default_system_prompt
        return {
            "prompt_type": "market-close",
            "current_prompt": get_system_prompt(),
            "is_custom": get_system_prompt() != get_default_system_prompt(),
        }
    elif prompt_type == "morning-report":
        from app.services.ai_market_report import get_morning_prompt, get_default_morning_prompt
        return {
            "prompt_type": "morning-report",
            "current_prompt": get_morning_prompt(),
            "is_custom": get_morning_prompt() != get_default_morning_prompt(),
        }
    elif prompt_type == "evening-report":
        from app.services.ai_market_report import get_evening_prompt, get_default_evening_prompt
        return {
            "prompt_type": "evening-report",
            "current_prompt": get_evening_prompt(),
            "is_custom": get_evening_prompt() != get_default_evening_prompt(),
        }
    elif prompt_type == "twitter-reply":
        from app.services.twitter_reply_service import get_reply_prompt, get_default_reply_prompt
        return {
            "prompt_type": "twitter-reply",
            "current_prompt": get_reply_prompt(),
            "is_custom": get_reply_prompt() != get_default_reply_prompt(),
        }
    elif prompt_type == "twitter-quote":
        from app.services.twitter_reply_service import get_quote_prompt, get_default_quote_prompt
        return {
            "prompt_type": "twitter-quote",
            "current_prompt": get_quote_prompt(),
            "is_custom": get_quote_prompt() != get_default_quote_prompt(),
        }
    elif prompt_type == "mentions-reply":
        from app.services.mentions_reply_service import get_system_prompt, get_default_system_prompt
        return {
            "prompt_type": "mentions-reply",
            "current_prompt": get_system_prompt(),
            "is_custom": get_system_prompt() != get_default_system_prompt(),
        }
    elif prompt_type == "spk-bulletin":
        from app.services.twitter_service import get_bulletin_prompt, get_default_bulletin_prompt
        return {
            "prompt_type": "spk-bulletin",
            "current_prompt": get_bulletin_prompt(),
            "is_custom": get_bulletin_prompt() != get_default_bulletin_prompt(),
        }
    elif prompt_type == "spk-app":
        from app.services.twitter_service import get_spk_app_prompt, get_default_spk_app_prompt
        return {
            "prompt_type": "spk-app",
            "current_prompt": get_spk_app_prompt(),
            "is_custom": get_spk_app_prompt() != get_default_spk_app_prompt(),
        }
    else:
        raise HTTPException(status_code=400, detail="Geçersiz prompt_type.")


@app.put("/api/v1/admin/ai-prompt/{prompt_type}")
@limiter.limit("10/minute")
async def admin_update_ai_prompt(request: Request, prompt_type: str, payload: dict):
    """AI system prompt'unu güncelle veya default'a döndür.

    {
        "admin_password": "...",
        "prompt": "yeni prompt metni..."   // null gönderilirse default'a döner
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    new_prompt = payload.get("prompt")
    # Boş string = default'a dön
    if new_prompt is not None and not new_prompt.strip():
        new_prompt = None

    if prompt_type == "ipo-report":
        from app.services.ai_ipo_analyzer import set_system_prompt, get_system_prompt
        set_system_prompt(new_prompt)
        return {
            "success": True,
            "prompt_type": "ipo-report",
            "message": "AI Rapor promptu güncellendi." if new_prompt else "AI Rapor promptu default'a döndürüldü.",
            "prompt_length": len(get_system_prompt()),
        }
    elif prompt_type == "prospectus":
        from app.services.prospectus_analyzer import set_system_prompt, get_system_prompt
        set_system_prompt(new_prompt)
        return {
            "success": True,
            "prompt_type": "prospectus",
            "message": "İzahname Analiz promptu güncellendi." if new_prompt else "İzahname Analiz promptu default'a döndürüldü.",
            "prompt_length": len(get_system_prompt()),
        }
    elif prompt_type == "kap-news":
        from app.services.ai_news_scorer import set_system_prompt, get_system_prompt
        set_system_prompt(new_prompt)
        return {
            "success": True,
            "prompt_type": "kap-news",
            "message": "KAP Haber promptu güncellendi." if new_prompt else "KAP Haber promptu default'a döndürüldü.",
            "prompt_length": len(get_system_prompt()),
        }
    elif prompt_type == "kap-analyzer":
        from app.services.kap_all_analyzer import set_system_prompt, get_system_prompt
        set_system_prompt(new_prompt)
        return {"success": True, "prompt_type": "kap-analyzer", "message": "KAP Analyzer promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_system_prompt())}
    elif prompt_type == "market-close":
        from app.services.market_close_analyzer import set_system_prompt, get_system_prompt
        set_system_prompt(new_prompt)
        return {"success": True, "prompt_type": "market-close", "message": "Tavan/Taban promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_system_prompt())}
    elif prompt_type == "morning-report":
        from app.services.ai_market_report import set_morning_prompt, get_morning_prompt
        set_morning_prompt(new_prompt)
        return {"success": True, "prompt_type": "morning-report", "message": "Sabah Raporu promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_morning_prompt())}
    elif prompt_type == "evening-report":
        from app.services.ai_market_report import set_evening_prompt, get_evening_prompt
        set_evening_prompt(new_prompt)
        return {"success": True, "prompt_type": "evening-report", "message": "Akşam Raporu promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_evening_prompt())}
    elif prompt_type == "twitter-reply":
        from app.services.twitter_reply_service import set_reply_prompt, get_reply_prompt
        set_reply_prompt(new_prompt)
        return {"success": True, "prompt_type": "twitter-reply", "message": "Tweet Yanıt promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_reply_prompt())}
    elif prompt_type == "twitter-quote":
        from app.services.twitter_reply_service import set_quote_prompt, get_quote_prompt
        set_quote_prompt(new_prompt)
        return {"success": True, "prompt_type": "twitter-quote", "message": "Alıntı Analiz promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_quote_prompt())}
    elif prompt_type == "mentions-reply":
        from app.services.mentions_reply_service import set_system_prompt, get_system_prompt
        set_system_prompt(new_prompt)
        return {"success": True, "prompt_type": "mentions-reply", "message": "Mention Yanıt promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_system_prompt())}
    elif prompt_type == "spk-bulletin":
        from app.services.twitter_service import set_bulletin_prompt, get_bulletin_prompt
        set_bulletin_prompt(new_prompt)
        return {"success": True, "prompt_type": "spk-bulletin", "message": "SPK Bülten promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_bulletin_prompt())}
    elif prompt_type == "spk-app":
        from app.services.twitter_service import set_spk_app_prompt, get_spk_app_prompt
        set_spk_app_prompt(new_prompt)
        return {"success": True, "prompt_type": "spk-app", "message": "SPK Başvuru promptu güncellendi." if new_prompt else "Default'a döndürüldü.", "prompt_length": len(get_spk_app_prompt())}
    else:
        raise HTTPException(status_code=400, detail="Geçersiz prompt_type.")


# -------------------------------------------------------
# Admin: İzahname Analiz Tetikle (Mobil Admin)
# -------------------------------------------------------

@app.post("/api/v1/admin/run-prospectus-analysis/{ipo_id}")
@limiter.limit("10/minute")
async def admin_run_prospectus_analysis_api(request: Request, ipo_id: int, payload: dict, db: AsyncSession = Depends(get_db)):
    """Mobil admin panelden izahname PDF analizini tetikler.

    {
        "admin_password": "...",
        "force": true     // mevcut analizi sıfırla ve yeniden üret
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()

    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    if not ipo.prospectus_url:
        raise HTTPException(status_code=400, detail="Bu IPO için izahname PDF URL'si girilmemiş.")

    force = payload.get("force", False)

    if ipo.prospectus_analysis and not force:
        return {
            "success": True,
            "message": "Bu IPO için zaten izahname analizi var. force=True ile yeniden üretebilirsiniz.",
            "ipo_id": ipo.id,
        }

    # Mevcut analizi sıfırla
    ipo.prospectus_analysis = None
    ipo.prospectus_analyzed_at = None
    ipo.prospectus_tweeted = False
    await db.commit()

    # Senkron izahname analizi — await ile bekle (Render'da create_task guvenilir degil)
    from app.services.prospectus_analyzer import analyze_prospectus
    ticker = ipo.ticker or ipo.company_name

    logger.info(f"Admin: Izahname analizi baslatiliyor (senkron) — {ticker} (id={ipo_id})")

    try:
        success = await analyze_prospectus(ipo_id, ipo.prospectus_url, delay_seconds=0)
    except Exception as e:
        logger.error(f"Admin: Izahname analiz hatasi — {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Izahname analiz hatasi: {str(e)[:200]}")

    return {
        "success": True,
        "message": f"{ticker} icin izahname analizi {'basariyla' if success else 'tamamlandi ama basarisiz'} uretildi.",
        "ipo_id": ipo.id,
        "ticker": ipo.ticker,
        "analysis_success": bool(success),
    }


# -------------------------------------------------------
# Admin: Bulk Archive IPO
# -------------------------------------------------------

@app.post("/api/v1/admin/bulk-archive-ipos")
@limiter.limit("5/minute")
async def admin_bulk_archive_ipos(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Belirtilen IPO ID'lerini arsivler ve trading_day_count gunceller.

    {
        "admin_password": "...",
        "ipo_ids": [6, 27, 26, ...],
        "set_day_count": 26          // opsiyonel, varsayilan: degistirme
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ipo_ids = payload.get("ipo_ids", [])
    set_day_count = payload.get("set_day_count")

    if not ipo_ids:
        return {"success": False, "message": "ipo_ids bos", "archived": 0}

    result = await db.execute(
        select(IPO).where(IPO.id.in_(ipo_ids))
    )
    ipos = result.scalars().all()

    archived_count = 0
    for ipo in ipos:
        ipo.archived = True
        ipo.archived_at = datetime.now(timezone.utc)
        if set_day_count is not None:
            ipo.trading_day_count = set_day_count
        archived_count += 1

    await db.commit()

    return {
        "success": True,
        "archived": archived_count,
        "ipo_ids": [ipo.id for ipo in ipos],
    }


# -------------------------------------------------------
# Admin: IPO Sil
# -------------------------------------------------------

@app.delete("/api/v1/admin/delete-ipo/{ipo_id}")
@limiter.limit("10/minute")
async def admin_delete_ipo(request: Request, ipo_id: int, db: AsyncSession = Depends(get_db)):
    """Admin panelden IPO siler.

    Header'da X-Admin-Password gonderilmeli.
    Iliskili tablolar (allocations, ceiling_tracks, brokers) da silinir.
    """
    admin_password = request.headers.get("X-Admin-Password", "")
    if not _verify_admin_password(admin_password):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    # IPO var mi kontrol et
    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    ticker = ipo.ticker
    company = ipo.company_name

    # Iliskili kayitlari sil
    from sqlalchemy import delete
    await db.execute(delete(IPOAllocation).where(IPOAllocation.ipo_id == ipo_id))
    await db.execute(delete(IPOCeilingTrack).where(IPOCeilingTrack.ipo_id == ipo_id))
    await db.execute(delete(IPOBroker).where(IPOBroker.ipo_id == ipo_id))

    # IPO'yu sil
    await db.delete(ipo)
    await db.commit()

    logger.info(f"Admin: IPO silindi — {ticker or company} (id={ipo_id})")

    return {
        "success": True,
        "deleted_ipo_id": ipo_id,
        "ticker": ticker,
        "company_name": company,
    }


# -------------------------------------------------------
# -------------------------------------------------------
# Admin: SPK Başvuru Tweet + Bildirim Gönder
# -------------------------------------------------------

@app.post("/api/v1/admin/tweet-spk-application")
@limiter.limit("10/minute")
async def admin_tweet_spk_application(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """SPK basvuru sirketleri icin tweet ve/veya bildirim gonder.

    Body:
    {
        "admin_password": "...",
        "company_names": ["Sirket A", "Sirket B"],  // Belirli sirketler
        "send_tweet": true,     // Tweet at (her sirket icin ayri)
        "send_notification": true  // Toplu push bildirim gonder
    }

    company_names verilmezse: tum notified=False + tweeted=False olanlari isler.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.spk_application import SPKApplication
    from app.services.notification import NotificationService
    from app.services.twitter_service import tweet_spk_application

    send_tweet = payload.get("send_tweet", True)
    send_notification = payload.get("send_notification", True)
    requested_names = payload.get("company_names", None)

    # Hedef sirketleri belirle
    if requested_names:
        # Belirli sirketler istenmis
        from sqlalchemy import or_
        conditions = [SPKApplication.company_name.ilike(f"%{n}%") for n in requested_names]
        result = await db.execute(
            select(SPKApplication).where(
                SPKApplication.status == "pending",
                or_(*conditions),
            )
        )
    else:
        # Tum bildirim/tweet gonderilmemis olanlar
        result = await db.execute(
            select(SPKApplication).where(
                SPKApplication.status == "pending",
                (SPKApplication.notified == False) | (SPKApplication.tweeted == False),
            )
        )

    targets = list(result.scalars().all())

    if not targets:
        return {"status": "no_targets", "message": "İşlenecek SPK başvurusu bulunamadı"}

    tweets_sent = 0
    tweets_failed = 0
    notification_sent = 0

    # 1. Toplu bildirim
    if send_notification:
        unnotified = [t for t in targets if not t.notified]
        if unnotified:
            short_names = []
            for t in unnotified:
                parts = t.company_name.split()
                short = " ".join(parts[:3]) if len(parts) > 4 else t.company_name
                short_names.append(short)

            notif_service = NotificationService(db)
            notification_sent = await notif_service.notify_spk_applications(short_names)

            for t in unnotified:
                t.notified = True

    # 2. Her sirket icin tweet
    if send_tweet:
        import time as _time
        untweeted = [t for t in targets if not t.tweeted]
        for i, t in enumerate(untweeted):
            if i > 0:
                _time.sleep(5)
            success = tweet_spk_application(t.company_name)
            if success:
                t.tweeted = True
                tweets_sent += 1
            else:
                tweets_failed += 1

    await db.commit()

    return {
        "status": "ok",
        "targets": len(targets),
        "company_names": [t.company_name for t in targets],
        "tweets_sent": tweets_sent,
        "tweets_failed": tweets_failed,
        "notification_sent_to": notification_sent,
    }


# -------------------------------------------------------
# Admin: AI IPO Raporu Oluştur / Yeniden Oluştur
# -------------------------------------------------------

@app.post("/api/v1/admin/generate-ai-report/{ipo_id}")
@limiter.limit("10/minute")
async def admin_generate_ai_report(request: Request, ipo_id: int, payload: dict, db: AsyncSession = Depends(get_db)):
    """Belirli bir IPO icin AI raporunu olusturur veya yeniden olusturur.

    {"admin_password": "...", "force": true}
    force=true ise mevcut rapor silinir ve yeniden olusturulur.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.ipo import IPO
    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    ticker = ipo.ticker or ipo.company_name
    force = payload.get("force", False)

    if ipo.ai_report and not force:
        return {
            "success": True,
            "message": f"{ticker} zaten AI raporu mevcut. Yeniden oluşturmak için force=true gönderin.",
            "already_exists": True,
        }

    # Senkron rapor uret — await ile bekle (Render'da create_task guvenilir degil)
    from app.services.ai_ipo_analyzer import generate_and_save_ipo_report

    logger.info(f"Admin: AI rapor uretimi baslatiliyor (senkron) — {ticker} (id={ipo_id}, force={force})")

    try:
        success = await generate_and_save_ipo_report(ipo_id, force=force)
    except Exception as e:
        logger.error(f"Admin: AI rapor uretim hatasi — {ticker}: {e}")
        raise HTTPException(status_code=500, detail=f"Rapor uretim hatasi: {str(e)[:200]}")

    if not success:
        raise HTTPException(status_code=500, detail=f"{ticker} icin AI rapor uretilemedi. Loglari kontrol edin.")

    return {
        "success": True,
        "message": f"{ticker} icin AI rapor basariyla uretildi.",
        "ipo_id": ipo_id,
        "ticker": ticker,
        "force": force,
    }


# -------------------------------------------------------
# Admin: BIST 30/50/100 Endeks Yonetimi
# -------------------------------------------------------

@app.post("/api/v1/admin/bist50-update")
@limiter.limit("5/minute")
async def admin_bist50_update(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """BIST 50 listesini infoyatirim.com'dan manuel guncelle.

    {"admin_password": "..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.scrapers.bist_index_scraper import fetch_bist50_tickers
    from app.services.news_service import get_bist50_tickers_sync, save_bist50_to_db

    new_tickers = await fetch_bist50_tickers()
    old_tickers = get_bist50_tickers_sync()

    added = new_tickers - old_tickers
    removed = old_tickers - new_tickers

    await save_bist50_to_db(db, new_tickers)

    return {
        "success": True,
        "total": len(new_tickers),
        "tickers": sorted(new_tickers),
        "added": sorted(added) if added else [],
        "removed": sorted(removed) if removed else [],
    }


@app.post("/api/v1/admin/bist-indices-update")
@limiter.limit("3/minute")
async def admin_bist_indices_update(request: Request, payload: dict):
    """BIST 30/50/100 hepsini guncelle + degisiklik varsa tweet at.

    {"admin_password": "..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.scheduler import update_bist_indices_job
    await update_bist_indices_job()

    return {"success": True, "message": "BIST 30/50/100 guncelleme ve tweet islemi tamamlandi"}


# -------------------------------------------------------
# KULLANICI GERİ BİLDİRİM (Bize Yazın)
# -------------------------------------------------------

class FeedbackRequest(BaseModel):
    name: str
    surname: str
    email: str
    phone: Optional[str] = None
    topic: str  # sorun, talep, gorüş, bilgi
    message: str
    device_id: Optional[str] = None

@app.post("/api/v1/feedback")
@limiter.limit("5/minute")
async def submit_feedback(request: Request, body: FeedbackRequest):
    """Kullanici geri bildirimi alir ve admin Telegram'a iletir."""
    from app.services.admin_telegram import send_admin_message

    topic_labels = {
        "sorun": "🔴 Sorun Bildirimi",
        "talep": "🟡 Özellik Talebi",
        "gorus": "🟢 Görüş / Öneri",
        "bilgi": "🔵 Bilgi Talebi",
    }
    topic_label = topic_labels.get(body.topic, f"📩 {body.topic}")

    lines = [
        f"<b>{topic_label}</b>",
        "",
        f"<b>İsim:</b> {body.name} {body.surname}",
        f"<b>E-posta:</b> {body.email}",
    ]
    if body.phone:
        lines.append(f"<b>Telefon:</b> {body.phone}")
    if body.device_id:
        lines.append(f"<b>Cihaz ID:</b> <code>{body.device_id}</code>")
    lines.append("")
    lines.append(f"<b>Mesaj:</b>\n{body.message}")

    text = "\n".join(lines)
    ok = await send_admin_message(text, parse_mode="HTML", silent=False)

    return {"success": ok, "message": "Mesajınız iletildi." if ok else "Bir hata oluştu, lütfen tekrar deneyin."}


# ---------- ERROR REPORT — uygulama crash hata bildirimi ----------

class ErrorReportRequest(BaseModel):
    device_id: str = "unknown"
    user_note: str = ""
    error_message: str = ""
    error_stack: str = ""
    component_stack: str = ""
    app_version: str = ""
    platform: str = ""
    timestamp: str = ""


@app.post("/api/v1/error-report")
@limiter.limit("5/minute")
async def submit_error_report(
    request: Request,
    body: ErrorReportRequest,
    db: AsyncSession = Depends(get_db),
):
    """Uygulama crash hata raporu — Telegram'a bildirim + 50 puan ödül (48 saat cooldown)."""
    from app.services.admin_telegram import send_admin_message

    # Telegram mesajı oluştur
    lines = [
        "<b>🐛 Uygulama Hata Raporu</b>",
        "",
        f"<b>Platform:</b> {body.platform}",
        f"<b>Versiyon:</b> {body.app_version}",
        f"<b>Cihaz:</b> <code>{body.device_id}</code>",
        f"<b>Zaman:</b> {body.timestamp[:19]}",
    ]
    if body.user_note:
        lines.append(f"\n<b>Kullanıcı Notu:</b>\n{body.user_note[:500]}")
    lines.append(f"\n<b>Hata:</b>\n<code>{body.error_message[:500]}</code>")
    if body.error_stack:
        lines.append(f"\n<b>Stack:</b>\n<code>{body.error_stack[:800]}</code>")

    text = "\n".join(lines)
    ok = await send_admin_message(text, parse_mode="HTML", silent=False)

    # Kullanıcıya 50 puan ödül ver — 48 saat cooldown (suistimal önleme)
    points_awarded = False
    cooldown_active = False
    if body.device_id and body.device_id != "unknown":
        try:
            result = await db.execute(
                select(User).where(User.device_id == body.device_id).with_for_update()
            )
            user = result.scalar_one_or_none()
            if user:
                # Son 48 saatte zaten puan verildi mi kontrol et
                cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
                recent_tx = await db.execute(
                    select(WalletTransaction).where(
                        WalletTransaction.user_id == user.id,
                        WalletTransaction.tx_type == "error_report",
                        WalletTransaction.created_at >= cutoff,
                    ).with_for_update().limit(1)
                )
                already_rewarded = recent_tx.scalar_one_or_none()

                if already_rewarded:
                    cooldown_active = True
                else:
                    user.wallet_balance = (user.wallet_balance or 0.0) + 50.0
                    tx = WalletTransaction(
                        user_id=user.id,
                        amount=50.0,
                        tx_type="error_report",
                        description="Hata raporu teşekkür puanı (50 puan)",
                        balance_after=user.wallet_balance,
                    )
                    db.add(tx)
                    await db.flush()
                    points_awarded = True
        except Exception:
            pass  # Puan veremesek de rapor iletilsin

    return {
        "success": ok,
        "points_awarded": points_awarded,
        "cooldown_active": cooldown_active,
        "message": "Hata raporunuz iletildi, teşekkürler!",
    }


# ---------- REVIEW REWARD — KALDIRILDI (Google Play politika ihlali) ----------
# Mağaza yorumu karşılığında ödül vermek Google Play tarafından yasaklanmıştır.
# Bu endpoint kaldırılmıştır. Eski review_reward tx kayıtları DB'de kalır.


# -------------------------------------------------------
# BIST HISSE LISTESI — autocomplete icin
# -------------------------------------------------------

@app.get("/api/v1/bist-stocks")
@limiter.limit("10/minute")
async def list_bist_stocks(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Tum BIST hisse kodlarini dondurur (DB + BigPara + Halka Arz birlesik).
    Autocomplete ve hisse validasyonu icin kullanilir.
    """
    # 1. DB'den: KAP bildirimlerindeki tum unique hisse kodlari
    result = await db.execute(
        select(KapAllDisclosure.company_code)
        .distinct()
        .order_by(KapAllDisclosure.company_code)
    )
    db_codes = set(result.scalars().all())

    # 2. BigPara'dan: BIST Tum endeksi (12h cache)
    try:
        from app.scrapers.kap_all_scraper import _refresh_bist_symbols
        bigpara_codes = await _refresh_bist_symbols()
    except Exception:
        bigpara_codes = set()

    # 3. Halka arz tablosundan: Yeni halka arz hisseleri (ticker'i olan)
    ipo_codes: set[str] = set()
    try:
        from app.models.ipo import IPO
        ipo_result = await db.execute(
            select(IPO.ticker).where(IPO.ticker.isnot(None), IPO.ticker != "")
        )
        ipo_codes = {t for t in ipo_result.scalars().all() if t and t.strip()}
    except Exception:
        pass

    # Birlesik set
    all_codes = db_codes | bigpara_codes | ipo_codes
    return [{"ticker": s, "company_name": s} for s in sorted(all_codes)]


# -------------------------------------------------------
# TUM KAP BILDIRIMLERI ENDPOINTS
# -------------------------------------------------------

@app.get("/api/v1/kap-all-disclosures", response_model=list[KapAllDisclosureOut])
async def list_kap_all_disclosures(
    ticker: Optional[str] = Query(None, description="Hisse kodu filtresi"),
    hours: Optional[int] = Query(None, ge=1, le=744, description="Son kac saat (1=son 1 saat, 24=son 1 gun)"),
    date: Optional[str] = Query(None, description="Belirli gun (YYYY-MM-DD). Bu gunun TR saatine gore tum haberlerini getirir."),
    min_score: Optional[float] = Query(None, ge=0, le=10, description="Minimum AI etki skoru (pozitif filtre icin 6.0)"),
    max_score: Optional[float] = Query(None, ge=0, le=10, description="Maksimum AI etki skoru (negatif filtre icin 5.0)"),
    category: Optional[str] = Query(None, description="Kategori filtresi (orn: 'Toptan Alım Satım', 'Tip Dönüşüm', 'Pay Alım Satım'). CSV ile birden fazla kategori: 'a,b,c'"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Tum KAP bildirimleri — herkes erisebilir.

    Filtreler:
    - ticker: Hisse kodu (orn: THYAO)
    - hours: Son kac saat (1, 24, 168, 720)
    - date: Belirli gun (YYYY-MM-DD formatinda, TR saatine gore)
    - min_score: Minimum AI etki skoru (>=)
    - max_score: Maksimum AI etki skoru (<)
    - category: Kategori adi (tek veya CSV)
    - limit/offset: Sayfalama
    """
    query = select(KapAllDisclosure).order_by(desc(KapAllDisclosure.created_at))

    if ticker:
        # CSV destegi — birden fazla hisse icin "AAA,BBB,CCC" filtre.
        # Frontend'de "Listemi Uygula" watchlist'teki tum ticker'lari boyle gonderir.
        if "," in ticker:
            tickers_list = [t.strip().upper() for t in ticker.split(",") if t.strip()]
            if tickers_list:
                query = query.where(KapAllDisclosure.company_code.in_(tickers_list))
        else:
            query = query.where(KapAllDisclosure.company_code == ticker.upper())

    if category:
        from sqlalchemy import or_, func as _sqlfunc
        cats = [c.strip() for c in category.split(",") if c.strip()]
        # Bildirim Turleri icin title-bazli pattern fallback (eski kayitlarin
        # category alani None olabilir; backfill yapilmadan calismasi icin).
        _CAT_PATTERNS = {
            "Toptan Alım Satım":  ["toptan satış", "toptan alış", "toptan alim satım", "toptan alım satım", "toptan işlem"],
            "Tip Dönüşüm":        ["borsada işlem gören tipe dönüş", "tipe dönüşüm", "tipe donusum"],
            "Pay Alım Satım":     ["pay alım satım bildirimi", "pay alim satim bildirimi", "pay alım satım", "pay alımı", "pay satışı", "geri alım"],
        }
        cond_list = []
        for c in cats:
            cond_list.append(KapAllDisclosure.category == c)
            for pat in _CAT_PATTERNS.get(c, []):
                cond_list.append(_sqlfunc.lower(KapAllDisclosure.title).like(f"%{pat}%"))
        if cond_list:
            query = query.where(or_(*cond_list))

    # date filtresi hours'tan onceliklidir
    # NOT: published_at kullaniyoruz (gercek KAP yayin zamani),
    # created_at degil (DB insert zamani — backfill durumunda yanilticidir).
    if date:
        try:
            from datetime import date as _date, time as _time
            from zoneinfo import ZoneInfo as _ZoneInfo
            tr_tz = _ZoneInfo("Europe/Istanbul")
            day = _date.fromisoformat(date)
            day_start_tr = datetime.combine(day, _time.min).replace(tzinfo=tr_tz)
            day_end_tr = datetime.combine(day, _time.max).replace(tzinfo=tr_tz)
            query = query.where(
                KapAllDisclosure.published_at >= day_start_tr.astimezone(timezone.utc),
                KapAllDisclosure.published_at <= day_end_tr.astimezone(timezone.utc),
            )
        except ValueError:
            pass  # Gecersiz format, filtre uygulanmaz
    elif hours:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        query = query.where(KapAllDisclosure.published_at >= since)

    if min_score is not None:
        query = query.where(KapAllDisclosure.ai_impact_score.isnot(None))
        query = query.where(KapAllDisclosure.ai_impact_score >= min_score)

    if max_score is not None:
        query = query.where(KapAllDisclosure.ai_impact_score.isnot(None))
        query = query.where(KapAllDisclosure.ai_impact_score < max_score)

    query = query.limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


# ═══════════════════════════════════════════════════════════════════
# Günlük Haber Bülteni — PRO Haber abonelerine özel
# Her sabah 07:00 TR'de kesim alır. Hafta sonu/tatilde push atılmaz.
# Pazartesi özeti: önceki Cuma 07:00 → Pazartesi 07:00 aralığını kapsar.
# Tatil sonrası ilk iş gününde de aynı mantık (en son iş günü 07:00'ten).
#
# SPK bölümü kaldırıldı: Önceki versiyon KAP kategorilerini kullanıyordu,
# bu yanıltıcıydı (şirket KAP açıklaması ≠ SPK bülten kararı).
# Gerçek SPK bülten verisi için /spk-bulten-analiz ayrı sayfası var.
# ═══════════════════════════════════════════════════════════════════

# Positive/negative ai_sentiment etiketleri
_POSITIVE_SENTIMENTS = ("Guclu Olumlu", "Cok Olumlu", "Olumlu", "Hafif Olumlu")
_NEGATIVE_SENTIMENTS = ("Guclu Olumsuz", "Cok Olumsuz", "Olumsuz", "Hafif Olumsuz")


def _shrink_summary(text: str | None, max_chars: int = 350) -> str:
    """ai_summary'yi tek tam cümleye indirir — YARIM BIRAKMAZ.

    Mantık:
      1. İlk tam cümleyi bul (. / ! / ? sonu, rakam-noktası hariç).
         Bu cümle ne kadar uzun olursa olsun (max_chars'a kadar) olduğu gibi dön.
         Yarım kelimede asla kesilmez.
      2. Cümle çok kısa ise (<35 char) 2. cümleyi de ekle.
      3. Hiç nokta yoksa ve metin max_chars'tan kısaysa olduğu gibi dön.
      4. Hiç nokta yok + çok uzun → virgül/bağlaç sınırında kes.
    """
    if not text:
        return ""
    s = " ".join(text.strip().split())

    # 1) Tüm cümle sonlarını bul — kısaltmaları (A.Ş., Ltd., Şti., Tic.) ve
    #    rakam-noktasını ("13.") cümle sonu sayma.
    import re as _re
    # Yaygın Türkçe/İngilizce şirket+akademik kısaltmalar
    _ABBREVS = (
        " A.Ş", " A.Ş.", " a.ş", " a.ş.",
        " AŞ", " Aş",
        " A.O", " A.O.", " A.o.",
        " Ltd", " Ltd.", " ltd", " ltd.",
        " Şti", " Şti.", " şti", " şti.",
        " Tic", " Tic.", " tic", " tic.",
        " San", " San.", " san", " san.",
        " Sti", " Sti.",
        " Inc", " Inc.", " Co", " Co.",
        " Dr", " Dr.", " Prof", " Prof.",
        " Av", " Av.",
        " GYO", " G.Y.O", " GMYO", " GSYO",
        " bkz", " bkz.", " vb", " vb.", " vs", " vs.",
        " no", " no.", " No", " No.",
        " Sn", " Sn.",
    )
    sentence_ends = []
    for m in _re.finditer(r'(?<!\d)[.!?](?=\s|$)', s):
        end = m.end()
        # Noktadan önceki ~10 karaktere bak: kısaltma ile mi bitiyor?
        look_back = s[max(0, end - 10):end]
        is_abbrev = any(look_back.endswith(ab) or look_back.endswith(ab + '.') for ab in _ABBREVS)
        if not is_abbrev:
            sentence_ends.append(end)

    if sentence_ends:
        first_end = sentence_ends[0]
        first = s[:first_end].strip()
        # İlk cümle çok kısaysa (35 char altı), 2. cümleyi de ekle
        if len(first) < 35 and len(sentence_ends) > 1:
            second_end = sentence_ends[1]
            two = s[:second_end].strip()
            if len(two) <= max_chars:
                return two.rstrip(". ")
        # İlk cümle uzun da olsa max_chars'a kadar olduğu gibi dön
        if len(first) <= max_chars:
            return first.rstrip(". ")
        # Aşırı uzun (>max_chars) cümle → virgül sınırında kes
        snippet = first[:max_chars]
        for sep in (", ", "; ", " ve ", " ile ", " ancak ", " fakat "):
            idx = snippet.rfind(sep)
            if idx > 100:
                return snippet[:idx].rstrip(",;: ") + "…"
        last_space = snippet.rfind(" ")
        if last_space > 100:
            return snippet[:last_space].rstrip(",;: ") + "…"
        return snippet.rstrip(",;: ") + "…"

    # 2) Hiç nokta yok ama metin sığıyor
    if len(s) <= max_chars:
        return s.rstrip(". ")

    # 3) Hiç nokta yok + uzun → virgül sınırında
    snippet = s[:max_chars]
    for sep in (", ", "; ", " ve ", " ile ", " ancak ", " fakat ", " ayrıca "):
        idx = snippet.rfind(sep)
        if idx > 100:
            return snippet[:idx].rstrip(",;: ") + "…"
    last_space = snippet.rfind(" ")
    if last_space > 100:
        return snippet[:last_space].rstrip(",;: ") + "…"
    return snippet.rstrip(",;: ") + "…"


@app.get("/api/v1/news/daily-summary")
@limiter.limit("30/minute")
async def get_daily_news_summary(
    request: Request,
    days: int = Query(30, ge=1, le=60),
    device_id: Optional[str] = Query(None, description="Cihaz ID — PRO Haber doğrulaması için"),
    db: AsyncSession = Depends(get_db),
):
    """Günlük haber bülteni — son N günün özetleri.

    Cutoff: 07:00 TR. Bir haberin ait olduğu özet günü:
      - 07:00'ten ÖNCE yayınlandıysa → o gün
      - 07:00'ten SONRA yayınlandıysa → ertesi gün
      - Eğer atanmış gün tatil/hafta sonu ise → sonraki ilk işlem gününe kaydır

    Yani Pazartesi özeti = önceki Cuma 07:00 → Pazartesi 07:00 aralığı.

    2 bölüm: positive, negative (ai_sentiment != Notr).
    SPK Bülteni AYRI sayfada (/spk-bulten-analiz).

    PRO değilse preview modu (son 2 iş günü + max 3 satır).
    """
    from zoneinfo import ZoneInfo as _ZoneInfo
    from datetime import time as _time
    from sqlalchemy import or_ as _or, and_ as _and
    from app.utils.bist_holidays import is_trading_day, previous_trading_day, next_trading_day
    tr_tz = _ZoneInfo("Europe/Istanbul")

    # PRO Haber doğrulaması:
    #  - device_id verildiyse DB'den abonelik kontrol et
    #  - device_id verilmediyse (web preview, native ilk açılış vb.) "is_pro=True"
    #    say. Frontend zaten hasNewsSubscription'a göre kendi paywall'unu gösterir,
    #    backend default-allow olur.
    is_pro = True  # default — device_id yoksa veya kontrol başarısızsa
    if device_id:
        try:
            ures = await db.execute(select(User).where(User.device_id == device_id))
            user = ures.scalar_one_or_none()
            if user:
                subq = await db.execute(
                    select(UserSubscription).where(
                        and_(
                            UserSubscription.user_id == user.id,
                            UserSubscription.is_active == True,  # noqa: E712
                        )
                    )
                )
                sub = subq.scalar_one_or_none()
                # UserSubscription.package field'ı: 'ana_yildiz' | 'yildiz_pazar' | 'free'
                # PRO Haber = ana_yildiz veya yildiz_pazar paketi
                pkg = (getattr(sub, "package", "") or "").lower() if sub else ""
                if not (
                    pkg in ("ana_yildiz", "yildiz_pazar", "yildiz", "haber_ai", "haber")
                    or "yildiz" in pkg
                    or "haber" in pkg
                ):
                    is_pro = False
            # user bulunamadıysa (yeni cihaz) → default-allow (is_pro=True kalır)
        except Exception as e:
            logger.warning("daily-summary pro check failed: %s", e)

    # Window — son N takvim gününü kapsayan published_at aralığı
    now_tr = datetime.now(tr_tz)
    earliest_tr = now_tr - timedelta(days=days + 1)  # +1 gün overlap (07:55 cutoff için)
    earliest_utc = earliest_tr.astimezone(timezone.utc)

    cutoff_time = _time(7, 0)

    def _assign_summary_date(pub_tr: datetime) -> date:
        """Bir haberin ait olduğu özet gününü belirler (tatil-aware)."""
        # 07:55 öncesi → o gün, sonrası → sonraki gün
        if pub_tr.time() < cutoff_time:
            candidate = pub_tr.date()
        else:
            candidate = pub_tr.date() + timedelta(days=1)
        # Tatil/hafta sonu ise sonraki iş gününe kaydır
        if not is_trading_day(candidate):
            candidate = next_trading_day(candidate)
        return candidate

    # Tüm pozitif/negatif KAP haberlerini çek
    q = (
        select(
            KapAllDisclosure.id,
            KapAllDisclosure.company_code,
            KapAllDisclosure.title,
            KapAllDisclosure.category,
            KapAllDisclosure.ai_sentiment,
            KapAllDisclosure.ai_impact_score,
            KapAllDisclosure.ai_summary,
            KapAllDisclosure.kap_url,
            KapAllDisclosure.published_at,
        )
        .where(KapAllDisclosure.published_at.isnot(None))
        .where(KapAllDisclosure.published_at >= earliest_utc)
        .where(KapAllDisclosure.company_code.isnot(None))
        .where(KapAllDisclosure.company_code != "")
        .order_by(desc(KapAllDisclosure.ai_impact_score), desc(KapAllDisclosure.published_at))
    )
    res = await db.execute(q)
    rows = res.all()

    grouped: dict[str, dict] = {}
    seen_ids_per_day: dict[str, set] = {}

    # ── SPK Bülten verisini PendingTweet'ten çek ve ekle ──
    # Her bülten tweet'i: "• TICKER - karar açıklaması" satırlarını içerir.
    # Bültenin yayın tarihi (sent_at) → cutoff mantığıyla ait olduğu özet günü.
    try:
        from app.models.pending_tweet import PendingTweet
        spk_q = (
            select(PendingTweet.id, PendingTweet.text, PendingTweet.sent_at)
            .where(PendingTweet.status == "sent")
            .where(PendingTweet.source.in_([
                "tweet_spk_bulletin_analysis",
                "tweet_spk_pending_visual",
            ]))
            .where(PendingTweet.sent_at.isnot(None))
            .where(PendingTweet.sent_at >= earliest_utc)
            .order_by(desc(PendingTweet.sent_at))
        )
        spk_res = await db.execute(spk_q)
        spk_tweets = spk_res.all()

        import re as _re
        # Section başlıkları (büyük harfle başlayan satırlar)
        SECTION_PATTERNS = (
            "Sermaye Artırım", "Sermaye Azaltım",
            "Halka Arz", "İdari Para Ceza", "İdari Yaptırım",
            "Diğer Önemli", "Diğer Gelişme",
            "Pay Geri Alım", "Temettü", "Kar Payı",
            "Esas Sözleşme", "Yetki Belge",
        )
        # "• #TICKER - açıklama" / "• TICKER - açıklama" / "TICKER - açıklama"
        # # karakteri opsiyonel, satır başında bullet ve/veya boşluk olabilir
        BULLET_RE = _re.compile(r"^\s*[•▪▫◦·*\-]?\s*#?([A-ZÇŞĞÜÖİ]{3,6})\s*[-–—]\s*(.+)$")

        for st in spk_tweets:
            if not st.text:
                continue
            # Tweet'in yayın saatine göre özet günü
            try:
                tw_tr = st.sent_at.astimezone(tr_tz)
                sd_date = _assign_summary_date(tw_tr)
            except Exception:
                continue
            sd = sd_date.isoformat()
            if sd not in grouped:
                grouped[sd] = {"summary_date": sd, "positive": [], "negative": [], "spk_bulten": []}
                seen_ids_per_day[sd] = set()

            current_section = None
            tweet_hhmm = tw_tr.strftime("%H:%M")
            # Satır satır parse
            for line_idx, raw_line in enumerate(st.text.split("\n")):
                line = raw_line.strip()
                if not line:
                    continue
                # Section başlığı tespit (kısa, hisse içermeyen)
                for sec in SECTION_PATTERNS:
                    if sec.lower() in line.lower() and len(line) < 60 and "•" not in line and "-" not in line[:10]:
                        current_section = line.rstrip(":")
                        break
                # Bullet satırı?
                m = BULLET_RE.match(line)
                if not m:
                    continue
                ticker = m.group(1).upper()
                spk_desc = m.group(2).strip()  # 'desc' SQLAlchemy import'unu ezmesin
                # Hash-only id (deduplication için)
                item_id = -(abs(hash(f"spk_{st.id}_{ticker}_{line_idx}")) % (10**9))
                if item_id in seen_ids_per_day[sd]:
                    continue
                seen_ids_per_day[sd].add(item_id)
                grouped[sd]["spk_bulten"].append({
                    "id": item_id,
                    "ticker": ticker,
                    "summary": _shrink_summary(spk_desc, max_chars=180),
                    "sentiment": "SPK",
                    "impact": 7.0,
                    "category": current_section or "SPK Kararı",
                    "kap_url": None,
                    "published_at": st.sent_at.isoformat() if st.sent_at else None,
                    "time": tweet_hhmm,
                })
    except Exception as _spk_err:
        logger.warning("SPK bülten parse hatası: %s", _spk_err)

    # Anlamsız/çok kısa özetleri filtrele — sadece şirket adı içeren,
    # tam cümle olmayan kayıtlar atılır.
    def _is_meaningful(summary: str) -> bool:
        if not summary or len(summary) < 30:
            return False
        s = summary.strip().rstrip('…').rstrip()
        sl = s.lower()
        # Cümle sonu işareti var mı?
        has_sentence = any(s.endswith(p) for p in ('.', '!', '?'))
        # Şirket türü ekleri ile bitenler (Ş, AŞ, Ltd vb) — cümle değil
        ends_with_company = (
            s.endswith('A.Ş') or s.endswith('A.Ş.') or s.endswith('AŞ')
            or s.endswith('Ltd') or s.endswith('Ltd.') or s.endswith('Şti')
            or s.endswith('Şti.') or s.endswith('Holding') or s.endswith('Yatırımlar')
        )
        word_count = len(s.split())
        if ends_with_company and word_count < 8:
            return False
        if word_count < 5:
            return False
        if not has_sentence and len(s) < 60:
            return False
        # İÇERİKSİZ / BOŞ MESAJ FİLTRESİ — somut bilgi içermeyen cümleler.
        # AI bazen "X şirketinde önemli bir gelişme yaşandı" gibi içi boş
        # cümle üretiyor → bunlar yatırımcı için faydasız.
        import re as _re_check
        has_number = bool(_re_check.search(r'\d', s))
        empty_patterns = (
            # "gelişme" varyantları
            "önemli bir gelişme yaşandı", "önemli bir gelişme oldu",
            "önemli bir gelişme yaşan", "önemli gelişme yaşandı",
            "bir gelişme yaşandı", "gelişmeler yaşandı", "gelişme yaşandı",
            "gelişmeler oldu", "gelişme oldu", "gelişme gerçekleşti",
            "yeni bir gelişme",
            # "değişiklik" varyantları
            "değişiklikler yaşandı", "değişiklik yaşandı",
            "değişiklikler oldu", "değişiklik oldu",
            "değişiklikler gerçekleşti", "değişiklik gerçekleşti",
            "yapısı değişikliği yaşandı", "yapısında değişiklik",
            "yapısı değişti",
            # "açıklama / duyuru / bildirim" varyantları
            "açıklama yapıldı", "açıklamada bulundu", "aciklamada bulundu",
            "bildirim yayınlandı", "bildirim yayinlandi",
            "bildirim yapıldı", "bildirim yapildi", "bildirimde bulundu",
            "duyuru yapıldı", "duyuru yapildi", "duyuru yayınlandı",
            "duyuruda bulundu",
            "bilgi paylaşıldı", "bilgi paylasildi", "bilgi verdi",
            # "değerlendirme" varyantları
            "değerlendirme yapıldı", "degerlendirme yapildi",
            # tek başına anlamsız fiiller
            "kararı aldı", "karar verildi", "karar alındı",
            "yayınlandı.", "yayinlandi.",
            # rutin
            "rutin/idari bildirim", "rutin bildirim",
            "teknik bildirim", "idari bildirim",
        )
        # Pattern eşleşirse ve cümlede SAYI yoksa → içeriksiz, at
        for pat in empty_patterns:
            if pat in sl and not has_number:
                return False
        # Ekstra kural: özet < 180 char + nokta ile bitiyor + RAKAM yok
        # → büyük ihtimalle yarım/içeriksiz cümle. At.
        if len(s) < 180 and has_sentence and not has_number:
            # Cümlede ANCAK iş anlamı taşıyan eylem fiili VARSA izin ver
            # (kazandı, imzaladı, satın aldı, açıkladı + somut nesne, vb.)
            meaningful_actions = (
                "kazandı", "imzaladı", "satın aldı", "satti", "sattı",
                "devraldı", "devretti", "tamamladı", "başlattı", "açtı",
                "kurdu", "duyurdu", "kararlaştırdı", "onayladı",
                "yatırım", "ihale", "sözleşme", "anlaşma", "kontrat",
                "yangın", "kaza", "ihraç", "tahvil", "kupon",
                "dava", "ceza", "iflas", "tasfiye",
            )
            if not any(act in sl for act in meaningful_actions):
                return False
        return True

    for r in rows:
        is_pos = r.ai_sentiment in _POSITIVE_SENTIMENTS
        is_neg = r.ai_sentiment in _NEGATIVE_SENTIMENTS
        if not (is_pos or is_neg):
            continue

        pub_tr = r.published_at.astimezone(tr_tz)
        try:
            sd_date = _assign_summary_date(pub_tr)
        except Exception:
            continue
        sd = sd_date.isoformat()

        if sd not in grouped:
            grouped[sd] = {"summary_date": sd, "positive": [], "negative": [], "spk_bulten": []}
            seen_ids_per_day[sd] = set()

        if r.id in seen_ids_per_day[sd]:
            continue

        summary_text = _shrink_summary(r.ai_summary or r.title, max_chars=350)
        if not _is_meaningful(summary_text):
            continue

        seen_ids_per_day[sd].add(r.id)

        # Saat:dakika (TR) — UI'da göstermek için
        published_hhmm = pub_tr.strftime("%H:%M")
        item = {
            "id": r.id,
            "ticker": r.company_code,
            "summary": summary_text,
            "sentiment": r.ai_sentiment,
            "impact": float(r.ai_impact_score) if r.ai_impact_score else 0.0,
            "category": r.category,
            "kap_url": r.kap_url,
            "published_at": r.published_at.isoformat() if r.published_at else None,
            "time": published_hhmm,
        }

        if is_pos:
            grouped[sd]["positive"].append(item)
        else:
            grouped[sd]["negative"].append(item)

    # Son N işlem günü ile sınırla (takvim günü değil, iş günü)
    days_list = sorted(grouped.values(), key=lambda x: x["summary_date"], reverse=True)

    # Bölüm bazlı limit
    MAX_PER_SECTION = 25
    for g in days_list:
        g["positive"] = g["positive"][:MAX_PER_SECTION]
        g["negative"] = g["negative"][:MAX_PER_SECTION]
        g["spk_bulten"] = g["spk_bulten"][:MAX_PER_SECTION]
        g["total"] = len(g["positive"]) + len(g["negative"]) + len(g["spk_bulten"])

    # HENÜZ YAYINLANMAMIŞ ÖZETLERİ ÇIKAR:
    # Bir özet yayınlanmış sayılır = o günün 07:00 TR'i geçilmiş olmalı.
    # Örnek: Şu an Cuma 17:00 → Pazartesi (25 May) özeti henüz yayınlanmadı,
    # listede gösterilmez. Pazartesi 07:00'da yayınlanınca görünür.
    today_cutoff = now_tr.replace(hour=7, minute=0, second=0, microsecond=0)
    latest_published_date = (
        today_cutoff.date() if now_tr >= today_cutoff else today_cutoff.date() - timedelta(days=1)
    )
    days_list = [
        g for g in days_list
        if g["total"] > 0 and date.fromisoformat(g["summary_date"]) <= latest_published_date
    ][:days]

    # PRO değilse preview modu
    if not is_pro:
        days_list = days_list[:2]
        for g in days_list:
            g["positive"] = g["positive"][:3]
            g["negative"] = g["negative"][:3]
            g["spk_bulten"] = (g.get("spk_bulten") or [])[:3]
            g["preview"] = True

    # Son yayinlanma zamani (frontend banner icin)
    last_published_at: str | None = None
    try:
        from sqlalchemy import text as _txt
        rres = await db.execute(_txt(
            "SELECT value FROM app_settings WHERE key = 'daily_news_summary_published_at' LIMIT 1"
        ))
        row = rres.first()
        if row and row[0]:
            last_published_at = row[0]
    except Exception:
        pass

    return {
        "is_pro": is_pro,
        "cutoff_hour": "07:00",
        "last_published_at": last_published_at,
        "days": days_list,
    }


# Lightweight endpoint — sadece "yayinlandi mi" bilgisi (banner icin polling)
@app.get("/api/v1/news/daily-summary/status")
@limiter.limit("60/minute")
async def get_daily_summary_status(request: Request, db: AsyncSession = Depends(get_db)):
    """Banner için hafif endpoint: son yayınlanma zamanı + bugün özet var mı.

    Frontend 2 saat kontrolünü bu endpoint'ten yapar.
    """
    from sqlalchemy import text as _txt
    last_published_at = None
    try:
        rres = await db.execute(_txt(
            "SELECT value FROM app_settings WHERE key = 'daily_news_summary_published_at' LIMIT 1"
        ))
        row = rres.first()
        if row and row[0]:
            last_published_at = row[0]
    except Exception:
        pass
    return {"last_published_at": last_published_at}


# ═══════════════════════════════════════════════════════════════════
# Sermaye Artirimi Takvimi — public, herkese acik (free)
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/v1/capital-increases")
async def list_capital_increases(
    type: Optional[str] = Query(None, description="bedelsiz | bedelli | tahsisli (yoksa hepsi)"),
    status: Optional[str] = Query(None, description="ykk_alindi | spk_onayli | tarih_belli | dagitiliyor | tamamlandi | reddedildi (yoksa hepsi)"),
    year: Optional[int] = Query(None, ge=2024, le=2100, description="Filtrelenecek yil (distribution_date ya da ykk_date yili)"),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Sermaye artirimi takvimi — KAP'tan beslenir, state machine sirasi.

    Sort:
      1. dagitiliyor (bugun bolunenler) en ustte
      2. tarih_belli (gelecek tarih) ASC
      3. spk_onayli (tarih bekleyen)
      4. ykk_alindi (SPK bekleyen)
      5. tamamlandi + reddedildi en altta (tarih DESC)
    """
    from app.models.capital_increase import CapitalIncrease

    query = select(CapitalIncrease)
    if type and type in ("bedelsiz", "bedelli", "tahsisli"):
        query = query.where(CapitalIncrease.type == type)
    if status and status in ("ykk_alindi", "spk_onayli", "tarih_belli", "dagitiliyor", "tamamlandi", "reddedildi"):
        query = query.where(CapitalIncrease.status == status)
    if year:
        # Yil filtresi: distribution_date varsa o, yoksa spk_approval_date, yoksa ykk_date
        from sqlalchemy import or_, extract, and_
        query = query.where(
            or_(
                extract("year", CapitalIncrease.distribution_date) == year,
                and_(CapitalIncrease.distribution_date.is_(None),
                     extract("year", CapitalIncrease.spk_approval_date) == year),
                and_(CapitalIncrease.distribution_date.is_(None),
                     CapitalIncrease.spk_approval_date.is_(None),
                     extract("year", CapitalIncrease.ykk_date) == year),
            )
        )

    # Status sira anahtari
    status_order = {
        "dagitiliyor": 0,
        "tarih_belli": 1,
        "spk_onayli": 2,
        "ykk_alindi": 3,
        "tamamlandi": 4,
        "reddedildi": 5,
    }

    result = await db.execute(query)
    rows = list(result.scalars().all())

    today = date.today()

    def sort_key(r):
        rank = status_order.get(r.status, 99)
        # Tarih onceligi: en yakin gelecek tarih en ustte
        if r.distribution_date:
            days_diff = (r.distribution_date - today).days
            if days_diff < 0:
                # Gecmis — en alta
                date_key = abs(days_diff) + 100000
            else:
                date_key = days_diff
        else:
            date_key = 100000
        return (rank, date_key)

    rows.sort(key=sort_key)
    rows = rows[:limit]

    # Manuel serialize — Pydantic schema yok
    return [
        {
            "id": r.id,
            "ticker": r.ticker,
            "company_name": r.company_name,
            "type": r.type,
            "percentage": r.percentage,
            "amount_tl": r.amount_tl,
            "bedelli_pct": getattr(r, "bedelli_pct", None),
            "bedelsiz_pct": getattr(r, "bedelsiz_pct", None),
            "tahsisli_pct": getattr(r, "tahsisli_pct", None),
            "bolunme_sonrasi_sermaye_tl": getattr(r, "bolunme_sonrasi_sermaye_tl", None),
            "ykk_date": r.ykk_date.isoformat() if r.ykk_date else None,
            "ykk_kap_url": r.ykk_kap_url,
            "spk_approval_date": r.spk_approval_date.isoformat() if r.spk_approval_date else None,
            "spk_approval_kap_url": r.spk_approval_kap_url,
            "distribution_date": r.distribution_date.isoformat() if r.distribution_date else None,
            "distribution_kap_url": r.distribution_kap_url,
            "rejected_at": r.rejected_at.isoformat() if r.rejected_at else None,
            "rejection_kap_url": r.rejection_kap_url,
            "status": r.status,
            "is_today": r.distribution_date == today if r.distribution_date else False,
        }
        for r in rows
    ]


# ═══════════════════════════════════════════════════════════════════
# Temettu Takvimi — public, herkese acik (free)
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/v1/dividend-calendar")
async def list_dividend_calendar(
    filter: Optional[str] = Query(None, description="kesinlesen | bekleyen (yoksa hepsi)"),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Temettu takvimi — KAP'tan beslenir, state machine.

    Frontend filtreleri:
      kesinlesen — odeme tarihi belirlenmis (payment_date IS NOT NULL ve status tarih_belli/odeniyor/tamamlandi)
      bekleyen   — odeme tarihi henuz yok (payment_date IS NULL veya status ykk_alindi/genel_kurul_onayli)

    Sort:
      1. odeniyor (bugun) en ustte
      2. tarih_belli (gelecek) ASC
      3. genel_kurul_onayli
      4. ykk_alindi
      5. tamamlandi + reddedildi en altta
    """
    from app.models.dividend_calendar import DividendCalendar
    from sqlalchemy import or_

    query = select(DividendCalendar)

    today = date.today()

    if filter == "kesinlesen":
        # Tarihi belli — payment_date var VE durum aktif/odendi
        query = query.where(DividendCalendar.payment_date.isnot(None))
        query = query.where(DividendCalendar.status.in_(["tarih_belli", "odeniyor", "tamamlandi"]))
    elif filter == "bekleyen":
        # Tarih beklenıyor — payment_date yok veya status henuz olgunlasmamis
        query = query.where(
            or_(
                DividendCalendar.payment_date.is_(None),
                DividendCalendar.status.in_(["ykk_alindi", "genel_kurul_onayli"]),
            )
        )
        query = query.where(DividendCalendar.status != "reddedildi")

    status_order = {
        "odeniyor": 0,
        "tarih_belli": 1,
        "genel_kurul_onayli": 2,
        "ykk_alindi": 3,
        "tamamlandi": 4,
        "reddedildi": 5,
    }

    result = await db.execute(query)
    rows = list(result.scalars().all())

    def sort_key(r):
        rank = status_order.get(r.status, 99)
        if r.payment_date:
            days_diff = (r.payment_date - today).days
            if days_diff < 0:
                date_key = abs(days_diff) + 100000
            else:
                date_key = days_diff
        else:
            date_key = 100000
        return (rank, date_key)

    rows.sort(key=sort_key)
    rows = rows[:limit]

    return [
        {
            "id": r.id,
            "ticker": r.ticker,
            "company_name": r.company_name,
            "period": r.period,
            "gross_amount_per_share": r.gross_amount_per_share,
            "net_amount_per_share": r.net_amount_per_share,
            "gross_yield_pct": r.gross_yield_pct,
            "net_yield_pct": r.net_yield_pct,
            "total_amount_tl": r.total_amount_tl,
            "ykk_date": r.ykk_date.isoformat() if r.ykk_date else None,
            "ykk_kap_url": r.ykk_kap_url,
            "general_assembly_date": r.general_assembly_date.isoformat() if r.general_assembly_date else None,
            "general_assembly_kap_url": r.general_assembly_kap_url,
            "payment_date": r.payment_date.isoformat() if r.payment_date else None,
            "payment_kap_url": r.payment_kap_url,
            "rejected_at": r.rejected_at.isoformat() if r.rejected_at else None,
            "rejection_kap_url": r.rejection_kap_url,
            "status": r.status,
            "is_today": r.payment_date == today if r.payment_date else False,
        }
        for r in rows
    ]


# ═══════════════════════════════════════════════════════════════════
# Pay Alim Satim — public list + admin import
# ═══════════════════════════════════════════════════════════════════

@app.get("/api/v1/share-transactions")
async def list_share_transactions(
    ticker: Optional[str] = Query(None),
    transaction_type: Optional[str] = Query(None, description="alici | satici"),
    days: int = Query(30, ge=1, le=365, description="Son kac gun"),
    limit: int = Query(50, ge=1, le=200),
    quality_only: bool = Query(True, description="True ise eksik (party_name veya oran yok) kayitlar gizlenir"),
    db: AsyncSession = Depends(get_db),
):
    """Pay alim satim — yapilandirilmis kayitlar (kim ne zaman ne kadar aldı/sattı).

    quality_only=True (default): Sadece **eksiksiz** kayıtları dondurur:
      - party_name NULL/bos/'?' DEGIL
      - oy_hakki_pct VEYA pay_orani_pct VEYA price_low en az BIRI dolu
    Bu sayede uygulamada "?" ve "—" olan dağınık kartlar görünmez.

    quality_only=False: TUM kayitlari doner (admin debugging icin).
    """
    from app.models.share_transaction_detail import ShareTransactionDetail
    from datetime import timedelta as _td
    from sqlalchemy import and_ as _and_, or_ as _or_

    cutoff = date.today() - _td(days=days)
    query = select(ShareTransactionDetail).where(
        ShareTransactionDetail.transaction_date >= cutoff
    )
    if ticker:
        query = query.where(ShareTransactionDetail.ticker == ticker.upper())
    if transaction_type in ("alici", "satici", "alis", "satis"):
        # DB'de iki form da var (eski 'alis'/'satis' + yeni 'alici'/'satici').
        # Hangisi gelirse ikisini de yakala.
        if transaction_type in ("alici", "alis"):
            query = query.where(ShareTransactionDetail.transaction_type.in_(["alici", "alis"]))
        else:
            query = query.where(ShareTransactionDetail.transaction_type.in_(["satici", "satis"]))

    if quality_only:
        # Party adi ZORUNLU
        query = query.where(
            _and_(
                ShareTransactionDetail.party_name.isnot(None),
                ShareTransactionDetail.party_name != "",
                ShareTransactionDetail.party_name != "?",
            )
        )
        # OY HAKKI veya PAY ORANI DOLU OLMALI (price_low tek başına yetmez —
        # kart "OY HAKKI: —, PAY ORANI: —" göstermesin diye)
        query = query.where(
            _or_(
                ShareTransactionDetail.oy_hakki_pct.isnot(None),
                ShareTransactionDetail.pay_orani_pct.isnot(None),
            )
        )

    query = query.order_by(desc(ShareTransactionDetail.transaction_date), desc(ShareTransactionDetail.id))
    query = query.limit(limit)

    result = await db.execute(query)
    rows = list(result.scalars().all())
    return [
        {
            "id": r.id,
            "ticker": r.ticker,
            "company_name": r.company_name,
            "transaction_date": r.transaction_date.isoformat() if r.transaction_date else None,
            "transaction_type": r.transaction_type,
            "party_name": r.party_name,
            "party_role": r.party_role,
            "price_low": r.price_low,
            "price_high": r.price_high,
            "nominal_lot": r.nominal_lot,
            "oy_hakki_pct": r.oy_hakki_pct,
            "oy_hakki_change_pct": r.oy_hakki_change_pct,
            "pay_orani_pct": r.pay_orani_pct,
            "pay_orani_change_pct": r.pay_orani_change_pct,
            "kap_url": r.kap_url,
            "source": r.source,
        }
        for r in rows
    ]


@app.get("/api/v1/share-type-conversions")
async def list_share_type_conversions(
    ticker: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(50, ge=1, le=200),
    quality_only: bool = Query(True, description="True ise eksik kayitlar gizlenir"),
    db: AsyncSession = Depends(get_db),
):
    """Borsada İşlem Gören Tipe Dönüşüm — public list.

    quality_only=True (default): investor_name'i NULL/bos/'?' olan veya
    converted_lot null olan kayıtları gizler. Mobilde "—" görünmesin.
    """
    from app.models.share_type_conversion import ShareTypeConversion
    from datetime import timedelta as _td
    from sqlalchemy import and_ as _and_
    cutoff = date.today() - _td(days=days)
    query = select(ShareTypeConversion).where(ShareTypeConversion.transaction_date >= cutoff)
    if ticker:
        query = query.where(ShareTypeConversion.ticker == ticker.upper())
    if quality_only:
        query = query.where(
            _and_(
                ShareTypeConversion.investor_name.isnot(None),
                ShareTypeConversion.investor_name != "",
                ShareTypeConversion.investor_name != "?",
                ShareTypeConversion.converted_lot.isnot(None),
            )
        )
    query = query.order_by(desc(ShareTypeConversion.transaction_date), desc(ShareTypeConversion.id)).limit(limit)
    rows = (await db.execute(query)).scalars().all()
    return [{
        "id": r.id, "ticker": r.ticker, "company_name": r.company_name,
        "transaction_date": r.transaction_date.isoformat() if r.transaction_date else None,
        "investor_name": r.investor_name, "converted_lot": r.converted_lot,
        "kap_url": r.kap_url, "source": r.source,
    } for r in rows]


@app.get("/api/v1/block-trades")
async def list_block_trades(
    ticker: Optional[str] = Query(None),
    transaction_type: Optional[str] = Query(None, description="alis | satis"),
    days: int = Query(365, ge=1, le=365),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Toptan Alım Satım — public list (default son 1 yil)."""
    from app.models.block_trade import BlockTrade
    from datetime import timedelta as _td
    # Eski client'larin gonderdigi kucuk days degerini de override et — son 50 her zaman gosterilsin
    if days < 180:
        days = 365
    cutoff = date.today() - _td(days=days)
    query = select(BlockTrade).where(BlockTrade.transaction_date >= cutoff)
    if ticker:
        query = query.where(BlockTrade.ticker == ticker.upper())
    if transaction_type in ("alis", "satis"):
        query = query.where(BlockTrade.transaction_type == transaction_type)
    query = query.order_by(desc(BlockTrade.transaction_date), desc(BlockTrade.id)).limit(limit)
    rows = (await db.execute(query)).scalars().all()
    return [{
        "id": r.id, "ticker": r.ticker, "company_name": r.company_name,
        "transaction_date": r.transaction_date.isoformat() if r.transaction_date else None,
        "transaction_type": r.transaction_type, "broker": r.broker,
        "counterparties": r.counterparties, "lot_amount": r.lot_amount,
        "cost_price": r.cost_price, "kap_url": r.kap_url, "source": r.source,
    } for r in rows]


@app.get("/api/v1/cautious-stocks")
async def list_cautious_stocks(
    tag: Optional[str] = Query(None, description="KRD|ACS|BRT|EMR|PEM|VEY|TEK"),
    active_only: bool = Query(True),
    sort: str = Query("start", description="start (başlangıç yeni→eski) | end (bitiş yakın→uzak)"),
    limit: int = Query(200, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Tedbirli Hisseler — public list.

    active_only=True (default) → sadece end_date >= bugün olanlar (is_active flag'ine
    bakmaz, doğrudan tarih kontrolü — cron'a bağımlı değil, her zaman güncel).
    sort='start' → başlangıç tarihi yeni→eski (varsayılan, son gelen üstte)
    sort='end' → ceza bitimi yakın→uzak (yakında bitecekler üstte)

    Filtreler:
      - "01 Oca → 31 Ara" placeholder (yıl başı/sonu) kayıtları filtrelenir.
        Bunlar belirli bir cezadan ziyade kalıcı durum bilgisi içerir.
    """
    from app.models.cautious_stock import CautiousStock
    from sqlalchemy import or_ as _or, and_ as _and, not_ as _not, extract as _extract
    query = select(CautiousStock)
    if active_only:
        # Tarih bazli kontrol — bitiş tarihi bugün veya sonrası
        # end_date null ise (tarih bilinmiyor) yine göster
        today = date.today()
        query = query.where(_or(CautiousStock.end_date >= today, CautiousStock.end_date.is_(None)))
    # Placeholder filtresi — start=Jan 1 + end=Dec 31 olan kalıcı/yıl-genişliğinde kayıtları çıkar
    query = query.where(
        _not(_and(
            _extract("month", CautiousStock.start_date) == 1,
            _extract("day",   CautiousStock.start_date) == 1,
            _extract("month", CautiousStock.end_date)   == 12,
            _extract("day",   CautiousStock.end_date)   == 31,
        ))
    )
    if tag:
        query = query.where(CautiousStock.tags.like(f"%{tag.upper()}%"))
    # Sıralama
    if sort == "end":
        # Ceza bitimi yakın → uzak (asc), null'lar sona
        # Tie-break: start_date desc (aynı bitişte yeni açıklanmış üstte)
        query = query.order_by(
            CautiousStock.end_date.asc().nullslast(),
            CautiousStock.start_date.desc().nullslast(),
            desc(CautiousStock.id),
        )
    else:
        # Yeni Eklenen: açıklama tarihi (start_date) yeniden eskiye
        # Tie-break: id desc (aynı günde birden fazla varsa son giren üstte)
        query = query.order_by(
            CautiousStock.start_date.desc().nullslast(),
            desc(CautiousStock.id),
        )
    query = query.limit(limit)
    rows = (await db.execute(query)).scalars().all()
    return [{
        "id": r.id, "ticker": r.ticker, "company_name": r.company_name,
        "last_price": r.last_price, "pct_change": r.pct_change,
        "start_date": r.start_date.isoformat() if r.start_date else None,
        "end_date": r.end_date.isoformat() if r.end_date else None,
        "tags": r.tags.split(",") if r.tags else [],
        "is_active": r.is_active, "kap_url": r.kap_url, "source": r.source,
    } for r in rows]


@app.get("/api/v1/business-deals")
async def list_business_deals(
    period: str = Query("week", description="week|month|quarter"),
    ticker: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    quality_only: bool = Query(True, description="True ise counterparty veya amount yok kayıtlar gizlenir"),
    db: AsyncSession = Depends(get_db),
):
    """Yeni İş Anlaşmaları — KAP'tan AI parse + TRY çevrim.

    quality_only=True (default): counterparty NULL/bos VE amount_try NULL
    olan kayıtlar gizlenir. Yani en az birinden anlamlı veri olmalı.
    Mobilde '— —' gosteren bos kartlar gorunmez.
    """
    from app.models.business_deal import BusinessDeal
    from datetime import timedelta as _td
    from sqlalchemy import or_ as _or_, and_ as _and_
    days_map = {"week": 7, "month": 30, "quarter": 90}
    days = days_map.get(period, 7)
    cutoff = date.today() - _td(days=days)
    query = select(BusinessDeal).where(BusinessDeal.deal_date >= cutoff)
    if ticker:
        query = query.where(BusinessDeal.ticker == ticker.upper())
    if quality_only:
        # En az birinden bilgi olmalı: counterparty (karşı taraf) VEYA amount_try (tutar)
        query = query.where(
            _or_(
                _and_(
                    BusinessDeal.counterparty.isnot(None),
                    BusinessDeal.counterparty != "",
                ),
                BusinessDeal.amount_try.isnot(None),
            )
        )
    query = query.order_by(desc(BusinessDeal.amount_try), desc(BusinessDeal.deal_date)).limit(limit)
    rows = (await db.execute(query)).scalars().all()
    return [{
        "id": r.id, "ticker": r.ticker, "company_name": r.company_name,
        "title": r.title, "summary": r.summary,
        "amount_original": r.amount_original, "currency": r.currency,
        "amount_try": r.amount_try, "exchange_rate_used": r.exchange_rate_used,
        "deal_date": r.deal_date.isoformat() if r.deal_date else None,
        "counterparty": r.counterparty,
        "kap_url": r.kap_url, "source": r.source,
        # Frontend için kullanıcı dostu etiket (tutar yoksa)
        "amount_label": (
            None if r.amount_try
            else "Tutar belirtilmemiş"
        ),
    } for r in rows]


@app.get("/api/v1/business-deals/leaderboard")
async def business_deals_leaderboard(
    period: str = Query("week", description="week|month|quarter"),
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """En çok iş alanlar — ticker bazında SUM(amount_try) DESC."""
    from app.models.business_deal import BusinessDeal
    from sqlalchemy import func as _f
    from datetime import timedelta as _td
    days_map = {"week": 7, "month": 30, "quarter": 90}
    days = days_map.get(period, 7)
    cutoff = date.today() - _td(days=days)
    stmt = (
        select(
            BusinessDeal.ticker,
            BusinessDeal.company_name,
            _f.sum(BusinessDeal.amount_try).label("total_try"),
            _f.count(BusinessDeal.id).label("deal_count"),
        )
        .where(BusinessDeal.deal_date >= cutoff)
        .where(BusinessDeal.amount_try.isnot(None))
        .group_by(BusinessDeal.ticker, BusinessDeal.company_name)
        .order_by(_f.sum(BusinessDeal.amount_try).desc())
        .limit(limit)
    )
    rows = (await db.execute(stmt)).all()
    return [{
        "ticker": r.ticker, "company_name": r.company_name,
        "total_try": float(r.total_try) if r.total_try else 0,
        "deal_count": r.deal_count,
    } for r in rows]


# ═══════════════════════════════════════════════════════════════════
# Admin import endpoint'leri — text-based bulk import
# ═══════════════════════════════════════════════════════════════════

async def _import_records(records: list[dict], model_cls, dedup_keys: list[str], replace_existing: bool):
    """Generic dedup + insert helper."""
    from app.database import async_session
    from sqlalchemy import select as _sel
    inserted = 0
    skipped = 0
    updated = 0
    errors: list[str] = []
    async with async_session() as db:
        for rec in records:
            try:
                conds = [getattr(model_cls, k) == rec[k] for k in dedup_keys if rec.get(k) is not None]
                stmt = _sel(model_cls)
                for c in conds:
                    stmt = stmt.where(c)
                stmt = stmt.limit(1)
                existing = (await db.execute(stmt)).scalar_one_or_none()
                if existing:
                    if replace_existing:
                        for k, v in rec.items():
                            setattr(existing, k, v)
                        updated += 1
                    else:
                        skipped += 1
                    continue
                db.add(model_cls(**rec, source="manual_import"))
                inserted += 1
            except Exception as inner_e:
                errors.append(f"{rec.get('ticker', '?')}: {inner_e}")
        await db.commit()
    return inserted, updated, skipped, errors


@app.post("/api/v1/admin/import-share-type-conversions")
@limiter.limit("3/minute")
async def admin_import_share_type_conversions(request: Request, payload: dict = Body(...)):
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    raw_text = payload.get("raw_text", "")
    replace_existing = bool(payload.get("replace_existing", False))
    if not raw_text:
        raise HTTPException(status_code=400, detail="raw_text gerekli")
    try:
        from app.services.category_text_parsers import parse_type_conversions
        from app.models.share_type_conversion import ShareTypeConversion
        records = parse_type_conversions(raw_text)
        ins, upd, skp, errs = await _import_records(
            records, ShareTypeConversion,
            dedup_keys=["ticker", "transaction_date", "investor_name"],
            replace_existing=replace_existing,
        )
        return {"status": "ok", "parsed": len(records), "inserted": ins, "updated": upd, "skipped_duplicates": skp, "errors": errs[:10]}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/import-block-trades")
@limiter.limit("3/minute")
async def admin_import_block_trades(request: Request, payload: dict = Body(...)):
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    raw_text = payload.get("raw_text", "")
    replace_existing = bool(payload.get("replace_existing", False))
    if not raw_text:
        raise HTTPException(status_code=400, detail="raw_text gerekli")
    try:
        from app.services.category_text_parsers import parse_block_trades
        from app.models.block_trade import BlockTrade
        records = parse_block_trades(raw_text)
        ins, upd, skp, errs = await _import_records(
            records, BlockTrade,
            dedup_keys=["ticker", "transaction_date", "transaction_type"],
            replace_existing=replace_existing,
        )
        return {"status": "ok", "parsed": len(records), "inserted": ins, "updated": upd, "skipped_duplicates": skp, "errors": errs[:10]}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/import-cautious-stocks")
@limiter.limit("3/minute")
async def admin_import_cautious_stocks(request: Request, payload: dict = Body(...)):
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    raw_text = payload.get("raw_text", "")
    if not raw_text:
        raise HTTPException(status_code=400, detail="raw_text gerekli")
    try:
        from app.services.category_text_parsers import parse_cautious_stocks
        from app.models.cautious_stock import CautiousStock
        from app.database import async_session
        from sqlalchemy import select as _sel
        records = parse_cautious_stocks(raw_text)
        # Tedbirli icin farkli dedup: sadece ticker (yeni cekiste eski kayit silinir)
        # 'Replace all' modu — onceki tedbirli listeyi silip yeniden yukle (en pratik)
        replace_all = bool(payload.get("replace_all", True))
        async with async_session() as db:
            if replace_all:
                from sqlalchemy import delete as _del
                await db.execute(_del(CautiousStock))
            inserted = 0
            for rec in records:
                db.add(CautiousStock(**rec, source="manual_import"))
                inserted += 1
            await db.commit()
        return {"status": "ok", "parsed": len(records), "inserted": inserted, "replaced_all": replace_all}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/scrape-halkarz-capital")
@limiter.limit("3/minute")
async def admin_scrape_halkarz_capital(request: Request, payload: dict = Body(...)):
    """halkarz.com/sermaye-artirimi sayfasını çek + CapitalIncrease tablosuna upsert."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    try:
        from app.scrapers.halkarz_capital_scraper import fetch_halkarz_capital, upsert_capital_increases
        from app.database import async_session

        records = await fetch_halkarz_capital()
        if not records:
            return {"status": "ok", "fetched": 0, "message": "halkarz.com'dan kayit cekilemedi"}

        async with async_session() as db:
            stats = await upsert_capital_increases(db, records)
            await db.commit()

        return {"status": "ok", "fetched": len(records), **stats}
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e)[:500],
                "traceback": traceback.format_exc()[-1000:] if not settings.is_production else None}


@app.post("/api/v1/admin/backfill-calendars")
@limiter.limit("1/minute")
async def admin_backfill_calendars(request: Request, payload: dict = Body(...)):
    """Admin: kap_all_disclosures'taki son N gün kayıtlarını state machine'lere besle.

    Body:
        admin_password: str
        days: int (default 30, max 90)
        targets: list[str] — ['capital','dividend','business'] hangileri (default hepsi)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    days = min(int(payload.get("days", 30)), 90)
    targets = payload.get("targets") or ["capital", "dividend", "business", "pay", "block", "tipe", "cautious"]
    max_records = min(int(payload.get("max_records", 500)), 2000)

    try:
        from datetime import datetime, timezone, timedelta
        from app.database import async_session
        from app.models.kap_all_disclosure import KapAllDisclosure
        from app.services.capital_increase_processor import is_capital_increase, process_kap_disclosure as cap_proc
        from app.services.dividend_calendar_processor import is_dividend, process_kap_disclosure as div_proc
        from app.services.business_deal_processor import is_business_deal, process_kap_disclosure as biz_proc
        from app.services.share_transaction_kap_processor import is_share_transaction, process_kap_disclosure as shtx_proc
        from app.services.kap_category_processors import (
            is_block_trade, process_block_trade,
            is_type_conversion, process_type_conversion,
            is_cautious, process_cautious,
        )

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        stats = {
            "scanned": 0, "capital": 0, "dividend": 0, "business": 0,
            "pay": 0, "block": 0, "tipe": 0, "cautious": 0, "errors": 0,
        }

        async with async_session() as db:
            stmt = (
                select(KapAllDisclosure)
                .where(KapAllDisclosure.created_at >= cutoff)
                .order_by(KapAllDisclosure.created_at.desc())
                .limit(max_records)
            )
            rows = (await db.execute(stmt)).scalars().all()
            for r in rows:
                stats["scanned"] += 1
                title = r.title or ""
                body = r.body or title
                try:
                    if "capital" in targets and is_capital_increase(title):
                        if await cap_proc(db, disclosure_id=r.id, ticker=r.company_code,
                                          company_name=None, title=title, body=body,
                                          kap_url=r.kap_url, published_at=r.published_at):
                            stats["capital"] += 1
                    if "dividend" in targets and is_dividend(title):
                        if await div_proc(db, disclosure_id=r.id, ticker=r.company_code,
                                          company_name=None, title=title, body=body,
                                          kap_url=r.kap_url, published_at=r.published_at):
                            stats["dividend"] += 1
                    if "business" in targets and is_business_deal(title):
                        if await biz_proc(db, disclosure_id=r.id, ticker=r.company_code,
                                          company_name=None, title=title, body=body,
                                          kap_url=r.kap_url, published_at=r.published_at):
                            stats["business"] += 1
                    if "pay" in targets and is_share_transaction(title):
                        if await shtx_proc(db, disclosure_id=r.id, ticker=r.company_code,
                                           company_name=None, title=title, body=body,
                                           kap_url=r.kap_url, published_at=r.published_at):
                            stats["pay"] += 1
                    if "block" in targets and is_block_trade(title):
                        if await process_block_trade(db, disclosure_id=r.id, ticker=r.company_code,
                                                     company_name=None, title=title, body=body,
                                                     kap_url=r.kap_url, published_at=r.published_at):
                            stats["block"] += 1
                    if "tipe" in targets and is_type_conversion(title):
                        if await process_type_conversion(db, disclosure_id=r.id, ticker=r.company_code,
                                                         company_name=None, title=title, body=body,
                                                         kap_url=r.kap_url, published_at=r.published_at):
                            stats["tipe"] += 1
                    if "cautious" in targets and is_cautious(title):
                        if await process_cautious(db, disclosure_id=r.id, ticker=r.company_code,
                                                  company_name=None, title=title, body=body,
                                                  kap_url=r.kap_url, published_at=r.published_at):
                            stats["cautious"] += 1
                except Exception as inner_e:
                    stats["errors"] += 1
                    logger.warning("Backfill hata (id=%d): %s", r.id, inner_e)

            await db.commit()

        return {"status": "ok", **stats, "days": days, "targets": targets}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/create-coupon")
@limiter.limit("10/minute")
async def admin_create_coupon_json(request: Request, payload: dict = Body(...)):
    """Admin: JSON ile kupon kodu üret.

    Body: {
      "admin_password": "...",
      "amount": 1000,        // puan
      "max_uses": 1,         // kullanım limiti
      "expires_days": 7      // bugünden N gün sonra biter (null = süresiz)
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.user import Coupon
    import secrets as _secrets
    import string as _string

    def _gen_code() -> str:
        chars = _string.ascii_uppercase + _string.digits
        return "SZ" + "".join(_secrets.choice(chars) for _ in range(6))

    amount = float(payload.get("amount", 1000))
    max_uses = int(payload.get("max_uses", 1))
    expires_days = payload.get("expires_days", 7)

    expire_dt = None
    if expires_days is not None:
        from datetime import timedelta as _td
        expire_dt = (datetime.now(timezone.utc) + _td(days=int(expires_days))).replace(
            hour=23, minute=59, second=59
        )

    async for db in get_db():
        # Unique kod
        code = None
        for _ in range(10):
            cand = _gen_code()
            existing = await db.execute(select(Coupon).where(Coupon.code == cand))
            if not existing.scalar_one_or_none():
                code = cand
                break
        if not code:
            return {"status": "error", "message": "kod uretilemedi"}

        coupon = Coupon(
            code=code,
            amount=amount,
            max_uses=max_uses,
            uses_count=0,
            expires_at=expire_dt,
            is_active=True,
        )
        db.add(coupon)
        await db.commit()

        return {
            "status": "ok",
            "code": code,
            "amount": amount,
            "max_uses": max_uses,
            "expires_at": expire_dt.isoformat() if expire_dt else None,
        }


@app.post("/api/v1/admin/wipe-daily-stock-prices")
@limiter.limit("3/minute")
async def admin_wipe_daily_stock_prices(request: Request, payload: dict = Body(...)):
    """Admin: daily_stock_market_stats tablosundaki tum close_price ve
    percent_change degerlerini sifirlar. BIST veri lisansi sureci icin.

    Sonrasinda frontend sadece: ticker, seri, son 30G ve AI nedeni gosterir.

    Body: {"admin_password": "..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    # close_price kolonu artik tablodan DROP edildi (startup migration). Sadece percent_change kalir.
    async with async_session() as db:
        res = await db.execute(text(
            "UPDATE daily_stock_market_stats "
            "SET percent_change = 0 "
            "WHERE percent_change IS NOT NULL AND percent_change <> 0"
        ))
        await db.commit()
        affected = res.rowcount if hasattr(res, "rowcount") else None
        cnt_res = await db.execute(text("SELECT COUNT(*) FROM daily_stock_market_stats"))
        total = cnt_res.scalar()
    return {"status": "ok", "rows_updated": affected, "total_rows": total}


@app.post("/api/v1/admin/sync-halkarz-tedbirli")
@limiter.limit("3/minute")
async def admin_sync_halkarz_tedbirli(request: Request, payload: dict = Body(...)):
    """Admin: halkarz.com/tedbirli-hisseler sayfasını scrape et + cautious_stocks'a ekle.

    KAP/Telegram'a düşmeyen VBTS bildirimleri için alternatif kaynak.

    Body: {"admin_password": "..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.scrapers.halkarz_tedbirli_scraper import sync_to_db
    result = await sync_to_db()
    return {"status": "ok", **result}


@app.post("/api/v1/admin/backfill-share-transactions")
@limiter.limit("3/minute")
async def admin_backfill_share_transactions(request: Request, payload: dict = Body(...)):
    """Admin: Son N saatteki 'Pay Alım Satım' KAP bildirimlerini tarayıp
    share_transaction_details tablosuna eksik olanları işle.

    Body: {"admin_password": "...", "hours": 48, "limit": 100}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.models.share_transaction_detail import ShareTransactionDetail
    from app.services.kap_pay_alim_satim_fetcher import upsert_pay_alim_satim_from_kap
    from app.services.share_transaction_kap_processor import is_share_transaction
    from datetime import timedelta as _td

    hours = int(payload.get("hours", 48))
    limit = int(payload.get("limit", 100))
    cutoff = datetime.now(timezone.utc) - _td(hours=hours)

    summary = {"scanned": 0, "matched": 0, "processed": 0, "skipped_existing": 0, "errors": [], "newly_added": []}

    async for db in get_db():
        result = await db.execute(
            select(KapAllDisclosure)
            .where(KapAllDisclosure.title.ilike("%Pay Alım Satım%"))
            .where(KapAllDisclosure.published_at >= cutoff)
            .order_by(desc(KapAllDisclosure.published_at))
            .limit(limit)
        )
        rows = result.scalars().all()
        summary["scanned"] = len(rows)

        for row in rows:
            if not is_share_transaction(row.title or ""):
                continue
            summary["matched"] += 1

            try:
                # Zaten var mı?
                existing = await db.execute(
                    select(ShareTransactionDetail)
                    .where(ShareTransactionDetail.kap_url == row.kap_url)
                    .where(ShareTransactionDetail.ticker == row.company_code)
                    .limit(1)
                )
                if existing.scalar_one_or_none():
                    summary["skipped_existing"] += 1
                    continue

                ok = await upsert_pay_alim_satim_from_kap(
                    db,
                    kap_url=row.kap_url,
                    company_code=row.company_code,
                    title=row.title or "",
                    published_at=row.published_at,
                    disclosure_id=row.id,
                )
                if ok:
                    summary["processed"] += 1
                    if len(summary["newly_added"]) < 30:
                        summary["newly_added"].append({
                            "ticker": row.company_code,
                            "kap_url": row.kap_url,
                            "published_at": row.published_at.isoformat() if row.published_at else None,
                        })
                else:
                    summary["errors"].append({"ticker": row.company_code, "reason": "upsert returned False"})
            except Exception as e:
                summary["errors"].append({"ticker": row.company_code, "reason": str(e)[:200]})

        await db.commit()
        return {"status": "ok", **summary}


@app.post("/api/v1/admin/cleanup-generic-issuer-dividends")
@limiter.limit("3/minute")
async def admin_cleanup_generic_issuer_dividends(request: Request, payload: dict = Body(...)):
    """Admin: dividend_calendar tablosundan generic issuer (ISE/BIST/MKK/BORSA/KAP)
    ticker'lı kayıtları sil. Bunlar bulk duyurudan yanlışlıkla oluşmuş.

    Body: {"admin_password": "...", "dry_run": false}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.dividend_calendar import DividendCalendar
    from sqlalchemy import delete as _sa_delete

    GENERIC = ["ISE", "BIST", "BORSA", "MKK", "KAP", "BORSA İSTANBUL", "BORSA ISTANBUL"]
    dry_run = bool(payload.get("dry_run", False))

    async for db in get_db():
        result = await db.execute(
            select(DividendCalendar).where(DividendCalendar.ticker.in_(GENERIC))
        )
        rows = result.scalars().all()
        sample = [{"id": r.id, "ticker": r.ticker, "status": r.status} for r in rows[:30]]

        if dry_run:
            return {"status": "ok", "dry_run": True, "would_delete": len(rows), "sample": sample}

        if rows:
            await db.execute(
                _sa_delete(DividendCalendar).where(DividendCalendar.ticker.in_(GENERIC))
            )
            await db.commit()
        return {"status": "ok", "deleted": len(rows), "sample": sample}


@app.post("/api/v1/admin/list-bilanco-by-period")
@limiter.limit("10/minute")
async def admin_list_bilanco_by_period(request: Request, payload: dict = Body(...)):
    """Admin: Belirli period kayıtlarını listele.

    Body: {"admin_password": "...", "period": "2025-Q4"}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.company_financial import CompanyFinancial
    period = payload.get("period", "")

    async for db in get_db():
        result = await db.execute(
            select(CompanyFinancial)
            .where(CompanyFinancial.period == period)
            .order_by(desc(CompanyFinancial.id))
            .limit(200)
        )
        rows = result.scalars().all()
        return {
            "period": period,
            "count": len(rows),
            "tickers": [r.ticker for r in rows],
            "sample": [
                {"id": r.id, "ticker": r.ticker, "revenue": float(r.revenue) if r.revenue else None}
                for r in rows[:30]
            ],
        }


@app.post("/api/v1/admin/reparse-bilanco-period-by-period")
@limiter.limit("3/minute")
async def admin_reparse_by_period(request: Request, payload: dict = Body(...)):
    """Admin: Belirli period'lu (örn 2025-Q4) tüm kayıtları reparse et.

    Body: {"admin_password": "...", "period": "2025-Q4", "limit": 200}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.company_financial import CompanyFinancial
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.bilanco_kap_scraper import _detect_period

    period = payload.get("period", "")
    limit = int(payload.get("limit", 200))

    fixed = []
    skipped = []

    async for db in get_db():
        result = await db.execute(
            select(CompanyFinancial)
            .where(CompanyFinancial.period == period)
            .order_by(desc(CompanyFinancial.id))
            .limit(limit)
        )
        rows = result.scalars().all()

        for row in rows:
            try:
                kap_q = await db.execute(
                    select(KapAllDisclosure)
                    .where(KapAllDisclosure.company_code == row.ticker)
                    .where(KapAllDisclosure.is_bilanco == True)
                    .order_by(desc(KapAllDisclosure.published_at))
                    .limit(8)
                )
                kap_rows = kap_q.scalars().all()
                detected = set()
                for kap in kap_rows:
                    if not kap.body or len(kap.body) < 200:
                        continue
                    p = _detect_period(kap.body)
                    if p:
                        detected.add(p)
                if not detected:
                    skipped.append({"ticker": row.ticker, "reason": "period yok"})
                    continue
                latest = sorted(detected, reverse=True)[0]
                if latest != row.period:
                    fixed.append({
                        "ticker": row.ticker,
                        "old": row.period,
                        "new": latest,
                    })
                    row.period = latest
            except Exception as e:
                skipped.append({"ticker": row.ticker, "reason": str(e)[:80]})

        if fixed:
            await db.commit()

        return {
            "status": "ok",
            "scanned": len(rows),
            "fixed_count": len(fixed),
            "fixed": fixed[:30],
            "skipped_count": len(skipped),
            "skipped": skipped[:10],
        }


@app.post("/api/v1/admin/reparse-bilanco-period")
@limiter.limit("3/minute")
async def admin_reparse_bilanco_period(request: Request, payload: dict = Body(...)):
    """Admin: company_financials'taki yanlış period'lu kayıtları KAP body'sinden
    yeniden tespit edip düzeltir.

    Body: {"admin_password": "...", "ticker": "YUNSA", "limit": 50}
    ticker verilmezse son N kayıt.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.company_financial import CompanyFinancial
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.bilanco_kap_scraper import _detect_period

    ticker = payload.get("ticker")
    limit = int(payload.get("limit", 50))

    fixed = []
    skipped = []

    async for db in get_db():
        # Son finans kayıtlarını al
        query = select(CompanyFinancial).order_by(desc(CompanyFinancial.id))
        if ticker:
            query = query.where(CompanyFinancial.ticker == ticker.upper())
        query = query.limit(limit)
        result = await db.execute(query)
        rows = result.scalars().all()

        for row in rows:
            try:
                # Bu ticker için bu period'a denk gelen KAP bildirimini bul
                kap_q = await db.execute(
                    select(KapAllDisclosure)
                    .where(KapAllDisclosure.company_code == row.ticker)
                    .where(KapAllDisclosure.is_bilanco == True)
                    .order_by(desc(KapAllDisclosure.published_at))
                    .limit(5)
                )
                kap_rows = kap_q.scalars().all()
                if not kap_rows:
                    skipped.append({"ticker": row.ticker, "period": row.period, "reason": "KAP yok"})
                    continue

                # Her bir KAP body'sinde period tespit et, en güncel olanı al
                detected_periods = set()
                for kap in kap_rows:
                    body = kap.body or ""
                    if len(body) < 200:
                        continue
                    p = _detect_period(body)
                    if p:
                        detected_periods.add(p)

                if not detected_periods:
                    skipped.append({"ticker": row.ticker, "period": row.period, "reason": "period bulunamadi"})
                    continue

                # En yeni period'u seç
                latest = sorted(detected_periods, reverse=True)[0]
                if latest != row.period:
                    old_period = row.period
                    row.period = latest
                    fixed.append({
                        "ticker": row.ticker,
                        "old_period": old_period,
                        "new_period": latest,
                    })
            except Exception as e:
                skipped.append({"ticker": row.ticker, "period": row.period, "reason": str(e)[:100]})

        if fixed:
            await db.commit()

        return {
            "status": "ok",
            "scanned": len(rows),
            "fixed_count": len(fixed),
            "fixed": fixed[:30],
            "skipped_count": len(skipped),
            "skipped": skipped[:10],
        }


@app.post("/api/v1/admin/backfill-cautious-stocks")
@limiter.limit("3/minute")
async def admin_backfill_cautious_stocks(request: Request, payload: dict = Body(...)):
    """Admin: Geçmiş VBTS / Tedbirli hisse KAP bildirimlerini tarayıp
    cautious_stocks tablosuna işle.

    Body: {"admin_password": "...", "days": 30, "limit": 100}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.models.cautious_stock import CautiousStock
    from app.services.kap_category_processors import is_cautious, process_cautious
    from datetime import timedelta as _td
    from sqlalchemy import or_ as _or_

    days = int(payload.get("days", 30))
    limit = int(payload.get("limit", 100))
    cutoff = datetime.now(timezone.utc) - _td(days=days)

    title_filter = _or_(
        KapAllDisclosure.title.ilike("%volatilite%"),
        KapAllDisclosure.title.ilike("%vbts%"),
        KapAllDisclosure.title.ilike("%tedbir%"),
        KapAllDisclosure.title.ilike("%bistech pay piyasas%"),
        KapAllDisclosure.title.ilike("%brüt takas%"),
        KapAllDisclosure.title.ilike("%brut takas%"),
        KapAllDisclosure.title.ilike("%açığa satış%"),
        KapAllDisclosure.title.ilike("%aciga satis%"),
        KapAllDisclosure.title.ilike("%kredili işlem%"),
    )

    summary = {
        "scanned": 0,
        "processed": 0,
        "added": [],
        "skipped_existing": 0,
        "not_cautious": 0,
    }

    async for db in get_db():
        result = await db.execute(
            select(KapAllDisclosure)
            .where(title_filter)
            .where(KapAllDisclosure.published_at >= cutoff)
            .order_by(desc(KapAllDisclosure.published_at))
            .limit(limit)
        )
        rows = result.scalars().all()
        summary["scanned"] = len(rows)

        for row in rows:
            try:
                # Devre kesici atla
                if "devre kesici" in (row.title or "").lower():
                    continue

                if not is_cautious(row.title or ""):
                    summary["not_cautious"] += 1
                    continue

                # Zaten işlenmiş mi?
                exists = await db.execute(
                    select(CautiousStock)
                    .where(CautiousStock.kap_url == row.kap_url)
                    .where(CautiousStock.ticker == row.company_code)
                    .limit(1)
                )
                if exists.scalar_one_or_none():
                    summary["skipped_existing"] += 1
                    continue

                processed = await process_cautious(
                    db,
                    disclosure_id=row.id,
                    ticker=row.company_code,
                    company_name=row.company_code,  # company_name yoksa ticker
                    title=row.title,
                    body=row.body,
                    kap_url=row.kap_url,
                    published_at=row.published_at,
                )
                if processed:
                    summary["processed"] += 1
                    if len(summary["added"]) < 30:
                        summary["added"].append({
                            "ticker": processed.ticker,
                            "tags": processed.tags or [],
                            "kap_url": row.kap_url,
                        })
            except Exception as e:
                logger.warning("Cautious backfill hata (id=%s): %s", row.id, e)

        await db.commit()
        return {"status": "ok", **summary}


@app.post("/api/v1/admin/seed-ipo-poll-votes")
@limiter.limit("3/minute")
async def admin_seed_ipo_poll_votes(request: Request, payload: dict = Body(...)):
    """Admin: Bir IPO için sahte oy ekle (test/demo için).

    Body (hype): {
      "admin_password": "...", "ipo_id": 47,
      "participate": 11, "skip": 3, "undecided": 2,
      "phase": "hype"
    }
    Body (ceiling): {
      "admin_password": "...", "ipo_id": 47,
      "phase": "ceiling",
      "ceiling_votes": {"8": 3, "5": 1, "11": 2}  // tavan_sayisi: oy_sayisi
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.ipo_poll_vote import IPOPollVote
    import uuid as _uuid

    ipo_id = int(payload.get("ipo_id", 0))
    phase = payload.get("phase", "hype")

    if ipo_id <= 0:
        return {"status": "error", "message": "ipo_id gerekli"}

    added: dict[str, int] = {}

    async for db in get_db():
        if phase == "ceiling":
            ceiling_votes = payload.get("ceiling_votes") or {}
            if not isinstance(ceiling_votes, dict):
                return {"status": "error", "message": "ceiling_votes dict olmali"}
            for ceil_val, count in ceiling_votes.items():
                choice = str(ceil_val).strip()
                try:
                    n = int(count)
                except (TypeError, ValueError):
                    continue
                for _ in range(n):
                    fake_device = f"seed-{_uuid.uuid4().hex[:24]}"
                    vote = IPOPollVote(
                        ipo_id=ipo_id, phase="ceiling", choice=choice,
                        device_id=fake_device, ip_address=None,
                    )
                    db.add(vote)
                    added[choice] = added.get(choice, 0) + 1
        else:
            participate = int(payload.get("participate", 0))
            skip = int(payload.get("skip", 0))
            undecided = int(payload.get("undecided", 0))
            for choice, count in [("participate", participate), ("skip", skip), ("undecided", undecided)]:
                for _ in range(count):
                    fake_device = f"seed-{_uuid.uuid4().hex[:24]}"
                    vote = IPOPollVote(
                        ipo_id=ipo_id, phase=phase, choice=choice,
                        device_id=fake_device, ip_address=None,
                    )
                    db.add(vote)
                    added[choice] = added.get(choice, 0) + 1
        await db.commit()
        return {"status": "ok", "ipo_id": ipo_id, "phase": phase, "added": added}


@app.post("/api/v1/admin/backfill-payment-announcements")
@limiter.limit("3/minute")
async def admin_backfill_payment_announcements(request: Request, payload: dict = Body(...)):
    """Admin: Geçmiş 'BISTECH Pay Piyasası' / 'Hak Kullanımı' KAP duyurularını tarayıp
    içlerindeki temettü ödemelerini DividendCalendar'a uygula.

    DB'deki kap_all_disclosures tablosundan TITLE eşleşen kayıtları al,
    her birinin body'sini parse_dividend_payment_announcement ile tara,
    process_dividend_payment_announcement ile DividendCalendar status'larını güncelle.

    Body: {"admin_password": "...", "days": 90, "limit": 200}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.dividend_calendar_processor import (
        parse_dividend_payment_announcement,
        process_dividend_payment_announcement,
    )
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from datetime import timedelta as _td
    from sqlalchemy import or_ as _or_

    days = int(payload.get("days", 90))
    limit = int(payload.get("limit", 200))
    cutoff = datetime.now(timezone.utc) - _td(days=days)

    title_filter = _or_(
        KapAllDisclosure.title.ilike("%BISTECH Pay Piyasas%"),
        KapAllDisclosure.title.ilike("%bistech pay piyasas%"),
        KapAllDisclosure.title.ilike("%Hak Kullan%"),
        KapAllDisclosure.title.ilike("%hak kullan%"),
        KapAllDisclosure.title.ilike("%Borsa İstanbul A.%"),
    )

    summary = {
        "scanned": 0,
        "with_pattern": 0,
        "tickers_processed": [],
        "by_url": [],
    }

    async for db in get_db():
        result = await db.execute(
            select(KapAllDisclosure)
            .where(title_filter)
            .where(KapAllDisclosure.published_at >= cutoff)
            .order_by(desc(KapAllDisclosure.published_at))
            .limit(limit)
        )
        rows = result.scalars().all()
        summary["scanned"] = len(rows)

        for row in rows:
            try:
                body = row.body or ""
                # Body kısa ise KAP'tan tekrar çek
                if len(body) < 200 and row.kap_url:
                    try:
                        disc = await fetch_kap_disclosure(row.kap_url)
                        if disc and disc.get("full_text"):
                            body = disc["full_text"]
                    except Exception:
                        pass
                if not body:
                    continue

                parsed = parse_dividend_payment_announcement(body)
                if not parsed:
                    continue

                summary["with_pattern"] += 1
                tickers = [p["ticker"] for p in parsed]
                summary["tickers_processed"].extend(tickers)

                # Process et
                _r = await process_dividend_payment_announcement(
                    db,
                    body=body,
                    kap_url=row.kap_url,
                    disclosure_id=row.id,
                    published_at=row.published_at,
                )
                if len(summary["by_url"]) < 30:
                    summary["by_url"].append({
                        "kap_url": row.kap_url,
                        "title": (row.title or "")[:80],
                        "tickers": tickers,
                        "processed": _r,
                    })
            except Exception as e:
                logger.warning("Backfill payment announcement hata (id=%s): %s", row.id, e)

        await db.commit()

        # Tekil ticker listesi
        unique_tickers = sorted(set(summary["tickers_processed"]))
        summary["unique_ticker_count"] = len(unique_tickers)
        summary["unique_tickers"] = unique_tickers[:50]
        summary.pop("tickers_processed", None)

        return {"status": "ok", **summary}


@app.post("/api/v1/admin/relabel-market-close-date")
@limiter.limit("5/minute")
async def admin_relabel_market_close_date(request: Request, payload: dict = Body(...)):
    """Admin: daily_stock_market_stats tablosunda belirli bir 'date' satirlarini
    baska bir tarihe tasi. Manuel tetiklemede 'bugun' yazilmis kayitlari 'dun'e
    cekmek icin kullanilir.

    Body:
      {"admin_password": "...",
       "from_date": "2026-05-12",
       "to_date":   "2026-05-11"}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from_date_str = payload.get("from_date", "")
    to_date_str = payload.get("to_date", "")
    if not from_date_str or not to_date_str:
        return {"status": "error", "message": "from_date ve to_date gerekli (YYYY-MM-DD)"}

    try:
        from_d = date.fromisoformat(from_date_str)
        to_d = date.fromisoformat(to_date_str)
    except Exception as e:
        return {"status": "error", "message": f"Tarih parse hata: {e}"}

    async with async_session() as db:
        # Once hedef tarihte kayit var mi kontrol — duplicate olusursa unique
        # constraint hatasi vermesin diye conflicting kayitlari sil
        await db.execute(text(
            'DELETE FROM daily_stock_market_stats WHERE "date" = :to_d '
            'AND ticker IN (SELECT ticker FROM daily_stock_market_stats WHERE "date" = :from_d)'
        ), {"from_d": from_d, "to_d": to_d})

        res = await db.execute(text(
            'UPDATE daily_stock_market_stats SET "date" = :to_d WHERE "date" = :from_d'
        ), {"from_d": from_d, "to_d": to_d})
        affected = res.rowcount if hasattr(res, "rowcount") else None
        await db.commit()

    return {
        "status": "ok",
        "from_date": from_date_str,
        "to_date": to_date_str,
        "rows_updated": affected,
    }


@app.post("/api/v1/admin/trigger-market-close")
@limiter.limit("2/minute")
async def admin_trigger_market_close(request: Request, payload: dict = Body(...)):
    """Admin: market_close pipeline'ini elle tetikle.
    Uzmanpara + BigPara'dan tavan/taban hisselerini scrape eder, AI ile sebep
    analizi yapar, daily_stock_market_stats tablosuna kaydeder.

    close_price ve percent_change DB'ye 0 yazilir (frontend fiyat gostermez).
    Sadece: ticker / consec / monthly / reason kaydedilir.

    Body:
      {"admin_password": "...", "force": true, "analyze_only": false}
    force=true → bugune ait mevcut kayitlari silip yeniden yapar
    analyze_only=true → sadece scrape+DB kaydet, tweet ATMAZ
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.services.market_close_analyzer import scrape_and_analyze_market_close

    import asyncio as _asyncio
    # Background olarak baslat — endpoint dondukten sonra calismaya devam etsin
    _asyncio.create_task(scrape_and_analyze_market_close(
        force=bool(payload.get("force", False)),
        analyze_only=bool(payload.get("analyze_only", True)),
    ))
    return {
        "status": "started",
        "message": "Market close pipeline arkaplanda baslatildi. ~3-5 dk surer.",
    }


@app.post("/api/v1/admin/test-bilanco-parse")
@limiter.limit("10/minute")
async def admin_test_bilanco_parse(request: Request, payload: dict = Body(...)):
    """Admin DEBUG: bir KAP bildirim URL'sinin bilanco parse sonucunu doner.
    DB'ye YAZMAZ — sadece raporlar.

    Body: {"admin_password": "...", "kap_url": "https://www.kap.org.tr/tr/Bildirim/XXX"}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    kap_url = payload.get("kap_url", "")
    if not kap_url:
        return {"status": "error", "message": "kap_url gerekli"}

    import re as _re
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.bilanco_kap_scraper import _detect_period, parse_kap_finansal_rapor

    try:
        disc = await fetch_kap_disclosure(kap_url)
    except Exception as e:
        return {"status": "error", "message": f"KAP fetch fail: {e}"}

    body = (disc or {}).get("full_text", "") if disc else ""
    if not body:
        return {"status": "error", "message": "body bos"}

    # Tum "Cari Donem" ve "Onceki Donem" header eslesmeleri
    cari_headers = _re.findall(r"Cari\s*D[öo]nem[^|]{0,200}", body)[:10]
    onceki_headers = _re.findall(r"[ÖO]nceki\s*D[öo]nem[^|]{0,200}", body)[:10]

    detected_period = _detect_period(body)
    parsed = parse_kap_finansal_rapor(body)

    return {
        "status": "ok",
        "kap_url": kap_url,
        "body_length": len(body),
        "tables_count": len((disc or {}).get("tables", [])),
        "pdf_links": (disc or {}).get("pdf_links", []),
        "detected_period": detected_period,
        "cari_donem_headers": cari_headers,
        "onceki_donem_headers": onceki_headers,
        "parsed": parsed,
        "body_excerpt": body[:800],
    }


@app.post("/api/v1/admin/audit-categories")
@limiter.limit("2/minute")
async def admin_audit_categories(request: Request, payload: dict = Body(...)):
    """Admin: Son N gunluk KAP bildirimleri uzerinde 8 kategori siniflandiricinin
    is_X() sonuclarini calistir + persist edilen DB tablolariyla cross-check yap.

    Her kategori icin:
      - matched: classifier match sayisi
      - persisted: hedef tablodaki kayit sayisi (cross-check ile)
      - missing_persist: classifier eslemis ama tablo yazimi yok (silent fail)
      - sample_titles: 5 ornek baslik

    Cakisma tespiti:
      - multi_match: birden fazla classifier match eden bildirimler (overlap)
      - unclassified: hiçbir classifier match etmeyen bildirimler

    Duplicate tespiti:
      - tablo bazinda ayni kap_url icin 1'den fazla satir

    Body: {"admin_password": "...", "days": 7}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    days = int(payload.get("days", 7))

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.models.dividend_calendar import DividendCalendar
    from app.models.business_deal import BusinessDeal
    from app.models.share_transaction_detail import ShareTransactionDetail
    from app.models.share_type_conversion import ShareTypeConversion
    from app.models.block_trade import BlockTrade
    from app.models.cautious_stock import CautiousStock
    from app.models.capital_increase import CapitalIncrease
    from app.services.dividend_calendar_processor import is_dividend
    from app.services.business_deal_processor import is_business_deal
    from app.services.share_transaction_kap_processor import is_share_transaction
    from app.services.kap_category_processors import is_block_trade, is_type_conversion, is_cautious
    from app.services.buyback_processor import is_buyback
    from app.services.capital_increase_kap_parser import detect_stage as ci_detect_stage

    since = datetime.now(timezone.utc) - timedelta(days=days)

    async with async_session() as db:
        q = (
            select(KapAllDisclosure)
            .where(KapAllDisclosure.published_at >= since)
            .order_by(desc(KapAllDisclosure.published_at))
        )
        rows = list((await db.execute(q)).scalars().all())

        # Her classifier icin sayac + ornek
        cats: dict[str, dict] = {
            "dividend": {"matched": 0, "samples": [], "kap_urls": set()},
            "business_deal": {"matched": 0, "samples": [], "kap_urls": set()},
            "share_transaction": {"matched": 0, "samples": [], "kap_urls": set()},
            "block_trade": {"matched": 0, "samples": [], "kap_urls": set()},
            "type_conversion": {"matched": 0, "samples": [], "kap_urls": set()},
            "cautious": {"matched": 0, "samples": [], "kap_urls": set()},
            "buyback": {"matched": 0, "samples": [], "kap_urls": set()},
            "capital_increase": {"matched": 0, "samples": [], "kap_urls": set()},
        }
        multi_match: list[dict] = []
        unclassified: list[dict] = []

        for r in rows:
            title = r.title or ""
            body = r.body or ""
            ticker = r.company_code or ""

            matches: list[str] = []
            try:
                if is_dividend(title, body, ticker): matches.append("dividend")
            except Exception: pass
            try:
                if is_business_deal(title): matches.append("business_deal")
            except Exception: pass
            try:
                if is_share_transaction(title, body): matches.append("share_transaction")
            except Exception: pass
            try:
                if is_block_trade(title, body): matches.append("block_trade")
            except Exception: pass
            try:
                if is_type_conversion(title): matches.append("type_conversion")
            except Exception: pass
            try:
                if is_cautious(title): matches.append("cautious")
            except Exception: pass
            try:
                if is_buyback(title): matches.append("buyback")
            except Exception: pass
            try:
                if ci_detect_stage(title, body): matches.append("capital_increase")
            except Exception: pass

            for m in matches:
                cats[m]["matched"] += 1
                if r.kap_url:
                    cats[m]["kap_urls"].add(r.kap_url)
                if len(cats[m]["samples"]) < 5:
                    cats[m]["samples"].append({"ticker": ticker, "title": title[:120], "kap_url": r.kap_url})

            if len(matches) > 1 and len(multi_match) < 30:
                multi_match.append({
                    "ticker": ticker, "title": title[:120], "kap_url": r.kap_url,
                    "matched": matches,
                })
            if len(matches) == 0 and len(unclassified) < 30:
                # Sadece anlamli olanlari logla (boilerplate "Kamuyu Aydinlatma" gibi degil)
                if title and len(title) > 10:
                    unclassified.append({
                        "ticker": ticker, "title": title[:120], "kap_url": r.kap_url,
                    })

        # Cross-check: hedef tablolarda gercekten kayit var mi
        async def count_table(model, kap_url_col_name: str | list[str], category_urls: set) -> dict:
            if not category_urls:
                return {"persisted": 0, "missing": 0, "duplicate_urls": 0}
            cols = [kap_url_col_name] if isinstance(kap_url_col_name, str) else kap_url_col_name
            persisted_urls = set()
            duplicate_count = 0
            for col in cols:
                try:
                    res = await db.execute(text(
                        f"SELECT {col}, COUNT(*) FROM {model.__tablename__} "
                        f"WHERE {col} = ANY(:urls) GROUP BY {col}"
                    ), {"urls": list(category_urls)})
                    for url, cnt in res.fetchall():
                        if url:
                            persisted_urls.add(url)
                            if cnt > 1:
                                duplicate_count += 1
                except Exception:
                    pass
            return {
                "persisted": len(persisted_urls),
                "missing": len(category_urls) - len(persisted_urls),
                "duplicate_urls": duplicate_count,
            }

        # Dividend — multiple kap_url columns
        div_cross = await count_table(DividendCalendar, ["ykk_kap_url", "general_assembly_kap_url", "payment_kap_url", "rejection_kap_url"], cats["dividend"]["kap_urls"])
        bd_cross = await count_table(BusinessDeal, "kap_url", cats["business_deal"]["kap_urls"])
        sht_cross = await count_table(ShareTransactionDetail, "kap_url", cats["share_transaction"]["kap_urls"])
        bt_cross = await count_table(BlockTrade, "kap_url", cats["block_trade"]["kap_urls"])
        tc_cross = await count_table(ShareTypeConversion, "kap_url", cats["type_conversion"]["kap_urls"])
        cs_cross = await count_table(CautiousStock, "kap_url", cats["cautious"]["kap_urls"])
        ci_cross = await count_table(CapitalIncrease, "kap_url", cats["capital_increase"]["kap_urls"])

        # Buyback → share_transaction_details tablosuna yazılır
        bb_cross = await count_table(ShareTransactionDetail, "kap_url", cats["buyback"]["kap_urls"])

    # Set'leri sayıya çevir + cross verilerini ekle
    def _pack(name: str, cross: dict) -> dict:
        c = cats[name]
        success_rate = (cross["persisted"] / c["matched"] * 100) if c["matched"] > 0 else 100.0
        return {
            "matched_disclosures": c["matched"],
            "persisted_rows": cross["persisted"],
            "missing_persist": cross["missing"],
            "duplicate_urls_in_target": cross["duplicate_urls"],
            "success_rate_pct": round(success_rate, 1),
            "samples": c["samples"],
        }

    return {
        "status": "ok",
        "scanned_days": days,
        "total_disclosures": len(rows),
        "categories": {
            "dividend": _pack("dividend", div_cross),
            "business_deal": _pack("business_deal", bd_cross),
            "share_transaction": _pack("share_transaction", sht_cross),
            "block_trade": _pack("block_trade", bt_cross),
            "type_conversion": _pack("type_conversion", tc_cross),
            "cautious": _pack("cautious", cs_cross),
            "buyback": _pack("buyback", bb_cross),
            "capital_increase": _pack("capital_increase", ci_cross),
        },
        "overlap_count": len(multi_match),
        "overlap_samples": multi_match,
        "unclassified_count": len(unclassified),
        "unclassified_samples": unclassified,
    }


@app.post("/api/v1/admin/reparse-empty-block-trades")
@limiter.limit("3/minute")
async def admin_reparse_empty_block_trades(request: Request, payload: dict = Body(...)):
    """Admin: block_trades tablosunda lot/fiyat/broker bos olan kayitlari KAP'tan
    re-fetch + regex parse ile doldurur. AI parse fail oldugu kayitlari kurtarir.

    Body: {"admin_password": "..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.models.block_trade import BlockTrade
        from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
        from app.services.kap_category_processors import _parse_block_trade_regex
        async with async_session() as db:
            q = select(BlockTrade).where(
                and_(
                    BlockTrade.kap_url.isnot(None),
                    or_(
                        BlockTrade.lot_amount.is_(None),
                        BlockTrade.broker.is_(None),
                    ),
                )
            ).limit(50)
            rows = (await db.execute(q)).scalars().all()
            updated = 0
            for r in rows:
                try:
                    disc = await fetch_kap_disclosure(r.kap_url)
                    body = (disc or {}).get("full_text", "") if disc else ""
                    if not body or len(body) < 200:
                        try:
                            from app.scrapers.kap_all_scraper import fetch_kap_page_content
                            page_body = await fetch_kap_page_content(r.kap_url)
                            if page_body and len(page_body) > len(body):
                                body = page_body
                        except Exception:
                            pass
                    if not body:
                        continue
                    rp = _parse_block_trade_regex(body)
                    changed = False
                    if not r.lot_amount and rp.get("lot_amount"):
                        r.lot_amount = rp["lot_amount"]; changed = True
                    if not r.cost_price and rp.get("cost_price"):
                        r.cost_price = rp["cost_price"]; changed = True
                    if not r.broker and rp.get("broker"):
                        r.broker = rp["broker"]; changed = True
                    if not r.counterparties and rp.get("counterparties"):
                        r.counterparties = rp["counterparties"]; changed = True
                    if rp.get("transaction_type") and r.transaction_type != rp["transaction_type"]:
                        r.transaction_type = rp["transaction_type"]; changed = True
                    if changed:
                        updated += 1
                except Exception:
                    continue
            await db.commit()
        return {"status": "ok", "scanned": len(rows), "updated": updated}
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "traceback": traceback.format_exc()[-1500:]}


@app.post("/api/v1/admin/delete-bad-block-trade")
@limiter.limit("5/minute")
async def admin_delete_bad_block_trade(request: Request, payload: dict = Body(...)):
    """Admin: ticker='?' veya tum alanlari null olan bad block_trade kayitlarini sil.
    Body: {"admin_password":"..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.models.block_trade import BlockTrade
        async with async_session() as db:
            res = await db.execute(
                delete(BlockTrade).where(
                    or_(
                        BlockTrade.ticker == "?",
                        BlockTrade.ticker == "",
                        and_(
                            BlockTrade.lot_amount.is_(None),
                            BlockTrade.cost_price.is_(None),
                            BlockTrade.broker.is_(None),
                            BlockTrade.counterparties.is_(None),
                        ),
                    )
                )
            )
            await db.commit()
            return {"status": "ok", "deleted_rows": res.rowcount}
    except Exception as e:
        import traceback
        return {"status": "error", "exception_type": type(e).__name__, "exception_message": str(e), "traceback": traceback.format_exc()[-1500:]}


@app.post("/api/v1/admin/normalize-share-tx-types")
@limiter.limit("3/minute")
async def admin_normalize_share_tx_types(request: Request, payload: dict = Body(...)):
    """Admin: share_transaction_details.transaction_type alanini normalize et.
    'alis' -> 'alici', 'satis' -> 'satici' (eski deger → yeni standart).

    Body: {"admin_password": "..."}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    try:
        from app.models.share_transaction_detail import ShareTransactionDetail
        async with async_session() as db:
            r1 = await db.execute(
                update(ShareTransactionDetail)
                .where(ShareTransactionDetail.transaction_type == "alis")
                .values(transaction_type="alici")
            )
            r2 = await db.execute(
                update(ShareTransactionDetail)
                .where(ShareTransactionDetail.transaction_type == "satis")
                .values(transaction_type="satici")
            )
            await db.commit()
            cnt_q = await db.execute(
                select(ShareTransactionDetail.transaction_type, func.count(ShareTransactionDetail.id))
                .group_by(ShareTransactionDetail.transaction_type)
            )
            counts = {row[0]: row[1] for row in cnt_q.fetchall()}
        return {
            "status": "ok",
            "alis_to_alici_updated": r1.rowcount if r1 else None,
            "satis_to_satici_updated": r2.rowcount if r2 else None,
            "current_counts": counts,
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "exception_type": type(e).__name__,
            "exception_message": str(e),
            "traceback": traceback.format_exc()[-2000:],
        }


@app.post("/api/v1/admin/route-disclosure-to-type-conversion")
@limiter.limit("5/minute")
async def admin_route_to_type_conversion(request: Request, payload: dict = Body(...)):
    """Admin: Bir KAP URL'sini ZORLA type_conversion pipeline'ina sok.

    Body: {"admin_password":"...","kap_url":"https://www.kap.org.tr/tr/Bildirim/XXXX"}

    Telegram poller yakalamamis Tipe Donusum bildirimlerini manuel isleme alir.
    Body kisaysa KAP'tan re-fetch. _parse_tc_table tum satirlari cikartip
    share_type_conversions tablosuna yazar (multi-ticker).
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    kap_url = payload.get("kap_url", "")
    if not kap_url:
        return {"status": "error", "message": "kap_url gerekli"}

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.kap_category_processors import process_type_conversion

    async with async_session() as db:
        row_q = select(KapAllDisclosure).where(KapAllDisclosure.kap_url == kap_url).limit(1)
        row = (await db.execute(row_q)).scalar_one_or_none()
        title = row.title if row else "Borsada İşlem Gören Tipe Dönüşüm Duyurusu"
        body = row.body if row else ""
        ticker = row.company_code if row else ""
        disclosure_id = row.id if row else 0
        published_at = row.published_at if row else datetime.now(timezone.utc)

        # Body kisaysa KAP'tan
        if (not body or len(body) < 500):
            try:
                disc = await fetch_kap_disclosure(kap_url)
                if disc and disc.get("full_text"):
                    body = disc["full_text"]
                    if row and (not row.body or len(row.body) < 500):
                        row.body = body
            except Exception as e:
                logger.warning("KAP fetch hata: %s", e)

        try:
            result = await process_type_conversion(
                db,
                disclosure_id=disclosure_id or 0,
                ticker=ticker or "",
                company_name=None,
                title=title or "Borsada İşlem Gören Tipe Dönüşüm Duyurusu",
                body=body or "",
                kap_url=kap_url,
                published_at=published_at,
            )
            await db.commit()
        except Exception as e:
            logger.exception("type_conversion process hata: %s", e)
            return {"status": "error", "message": str(e), "body_length": len(body or "")}

        inserted_count = len(result) if result else 0
        return {
            "status": "ok",
            "inserted_rows": inserted_count,
            "body_length": len(body or ""),
            "rows_preview": [
                {"ticker": r.ticker, "investor": r.investor_name, "nominal": r.converted_lot}
                for r in (result or [])[:20]
            ] if result else [],
        }


@app.post("/api/v1/admin/route-disclosure-to-block-trade")
@limiter.limit("5/minute")
async def admin_route_to_block_trade(request: Request, payload: dict = Body(...)):
    """Admin: Bir KAP URL'sini ZORLA block_trade pipeline'ina sok.

    Body: {"admin_password":"...","kap_url":"https://www.kap.org.tr/tr/Bildirim/XXXX"}

    Klasifikatör yanlis dedi diye yakalanmamis toptan alim satim bildirimlerini
    elle yonlendirmek icin. Body kisaysa KAP'tan re-fetch eder. process_block_trade
    AI ile lot/fiyat/karsi taraf cikartip block_trades tablosuna yazar.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    kap_url = payload.get("kap_url", "")
    if not kap_url:
        return {"status": "error", "message": "kap_url gerekli"}

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.kap_category_processors import process_block_trade, is_block_trade

    async with async_session() as db:
        # DB'de var mi?
        row_q = select(KapAllDisclosure).where(KapAllDisclosure.kap_url == kap_url).limit(1)
        row = (await db.execute(row_q)).scalar_one_or_none()
        title = row.title if row else "Pay Alim Satim Bildirimi (manual route)"
        body = row.body if row else ""
        ticker = row.company_code if row else ""
        disclosure_id = row.id if row else 0
        published_at = row.published_at if row else datetime.now(timezone.utc)

        # Body kisaysa KAP'tan tam icerik
        if (not body or len(body) < 500):
            try:
                disc = await fetch_kap_disclosure(kap_url)
                if disc and disc.get("full_text"):
                    body = disc["full_text"]
                    if row and (not row.body or len(row.body) < 500):
                        row.body = body
                    if disc.get("title") and (not row or not row.title):
                        title = disc["title"]
            except Exception as e:
                logger.warning("KAP fetch hata: %s", e)

        # is_block_trade kontrolu (debug)
        is_bt = is_block_trade(title, body)

        # ZORLA process — title generic olsa bile body'de toptan varsa yakala
        try:
            from app.services.kap_category_processors import _call_gemini, _BT_PROMPT
            # AI'ya gonder, doner parsed
            parsed = await _call_gemini(_BT_PROMPT.format(
                ticker=ticker or "?", title=title or "", body=(body or "")[:3500]
            )) or {}
        except Exception:
            parsed = {}

        # Direkt insert (process_block_trade is_block_trade() check'i atlamak icin manuel)
        from app.models.block_trade import BlockTrade
        from sqlalchemy import select as _sel
        existing = (await db.execute(
            _sel(BlockTrade).where(BlockTrade.kap_url == kap_url).limit(1)
        )).scalar_one_or_none()
        if existing:
            await db.commit()
            return {
                "status": "exists",
                "message": "BlockTrade kaydi zaten var",
                "id": existing.id,
                "ticker": existing.ticker,
                "is_block_trade_classifier": is_bt,
            }

        tx_type = parsed.get("transaction_type") if parsed.get("transaction_type") in ("alis", "satis") else None
        tx_date = None
        if isinstance(parsed.get("transaction_date"), str):
            try:
                tx_date = date.fromisoformat(parsed["transaction_date"])
            except ValueError:
                pass

        new = BlockTrade(
            ticker=(ticker or parsed.get("ticker") or "?")[:10].upper(),
            transaction_type=tx_type or "satis",
            transaction_date=tx_date or (published_at.date() if published_at else date.today()),
            broker=(parsed.get("broker") or "")[:255] or None,
            counterparties=(parsed.get("counterparties") or "") or None,
            lot_amount=int(parsed.get("lot_amount")) if parsed.get("lot_amount") else None,
            cost_price=float(parsed.get("cost_price")) if parsed.get("cost_price") else None,
            kap_url=kap_url,
            source="manual_route",
        )
        db.add(new)
        await db.commit()
        return {
            "status": "ok",
            "message": "BlockTrade kaydi olusturuldu",
            "id": new.id,
            "ticker": new.ticker,
            "transaction_type": new.transaction_type,
            "lot_amount": new.lot_amount,
            "cost_price": new.cost_price,
            "broker": new.broker,
            "counterparties": new.counterparties,
            "is_block_trade_classifier": is_bt,
            "body_length": len(body or ""),
        }


@app.post("/api/v1/admin/debug-disclosure")
@limiter.limit("10/minute")
async def admin_debug_disclosure(request: Request, payload: dict = Body(...)):
    """Admin: Bir KAP URL'sinin title/body + her processor'un is_X() sonuclarini
    doner — yanlis kategori siniflandirma debug'i icin.

    Body: {"admin_password": "...", "kap_url": "https://www.kap.org.tr/tr/Bildirim/1602036"}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    kap_url = payload.get("kap_url") or ""
    if not kap_url:
        return {"status": "error", "message": "kap_url gerekli"}

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.dividend_calendar_processor import is_dividend
    from app.services.business_deal_processor import is_business_deal
    from app.services.share_transaction_kap_processor import is_share_transaction
    from app.services.kap_category_processors import (
        is_block_trade, is_type_conversion, is_cautious,
    )
    from app.services.buyback_processor import is_buyback

    async with async_session() as db:
        # DB'deki kayit
        q = select(KapAllDisclosure).where(KapAllDisclosure.kap_url == kap_url).limit(1)
        row = (await db.execute(q)).scalar_one_or_none()
        db_title = row.title if row else None
        db_body = row.body if row else None
        db_ticker = row.company_code if row else None

        # Live KAP fetch
        live_disc = None
        try:
            live_disc = await fetch_kap_disclosure(kap_url)
        except Exception as e:
            live_disc = {"error": str(e)}

        live_title = (live_disc or {}).get("title") or db_title or ""
        live_body = (live_disc or {}).get("full_text") or db_body or ""

        # Her classifier'in sonucu (body-aware)
        classifications = {
            "is_dividend": is_dividend(live_title, live_body, db_ticker or ""),
            "is_business_deal": is_business_deal(live_title),
            "is_share_transaction": is_share_transaction(live_title, live_body),
            "is_block_trade": is_block_trade(live_title, live_body),
            "is_type_conversion": is_type_conversion(live_title),
            "is_cautious": is_cautious(live_title),
            "is_buyback": is_buyback(live_title),
        }

        return {
            "status": "ok",
            "kap_url": kap_url,
            "db_record": {
                "found": bool(row),
                "id": row.id if row else None,
                "ticker": db_ticker,
                "title": db_title,
                "body_length": len(db_body or ""),
                "published_at": row.published_at.isoformat() if row and row.published_at else None,
            },
            "live_fetch": {
                "title": live_title[:200],
                "body_length": len(live_body),
                "body_excerpt": live_body[:800],
            },
            "classifications": classifications,
            "categorized_as": [k.replace("is_", "") for k, v in classifications.items() if v] or ["UNKNOWN"],
        }


@app.post("/api/v1/admin/backfill-dividend-bodies")
@limiter.limit("2/minute")
async def admin_backfill_dividend_bodies(request: Request, payload: dict = Body(...)):
    """Admin: Son N saatte gelen tum temettu KAP bildirimleri icin body kisaysa
    KAP'tan re-fetch + dividend state machine'inden gecir.

    Body: {"admin_password": "...", "hours": 72, "limit": 200}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    hours = int(payload.get("hours", 72))
    limit = int(payload.get("limit", 200))

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.dividend_calendar_processor import (
        is_dividend, process_kap_disclosure as div_process,
    )
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure

    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    stats = {"scanned": 0, "matched_dividend": 0, "body_refetched": 0, "processed": 0, "errors": 0, "events": []}

    async with async_session() as db:
        q = (
            select(KapAllDisclosure)
            .where(KapAllDisclosure.published_at >= since)
            .order_by(desc(KapAllDisclosure.published_at))
            .limit(limit)
        )
        rows = list((await db.execute(q)).scalars().all())
        stats["scanned"] = len(rows)

        for d in rows:
            title = d.title or ""
            body = d.body or ""
            title_lo = title.lower()

            # ★ ÖN-FETCH: BISTECH / Borsa İstanbul bulk başlıkları kısa body ile
            # is_dividend'a yakalanmaz. Title eşleşirse body'yi önce zenginleştir.
            needs_prefetch = (
                len(body) < 500 and d.kap_url and (
                    "bistech" in title_lo or "bıstech" in title_lo
                    or "borsa istanbul" in title_lo or "borsa ıstanbul" in title_lo
                    or "hak kullan" in title_lo
                    or "temettu" in title_lo or "temettü" in title_lo
                    or "kar payı" in title_lo or "kar payi" in title_lo
                    or "kâr payı" in title_lo
                )
            )
            if needs_prefetch:
                try:
                    disc = await fetch_kap_disclosure(d.kap_url)
                    if disc and disc.get("full_text"):
                        body = disc["full_text"]
                        d.body = body
                        stats["body_refetched"] += 1
                    elif not body or len(body) < 500:
                        # 2. seviye fallback
                        try:
                            from app.scrapers.kap_all_scraper import fetch_kap_page_content
                            page_body = await fetch_kap_page_content(d.kap_url)
                            if page_body and len(page_body) > len(body):
                                body = page_body
                                d.body = body
                                stats["body_refetched"] += 1
                        except Exception:
                            pass
                except Exception:
                    pass

            if not is_dividend(title, body):
                continue
            stats["matched_dividend"] += 1

            # Eski body re-fetch (is_dividend match olduktan sonra son kontrol)
            if (not body or len(body) < 200) and d.kap_url:
                try:
                    disc = await fetch_kap_disclosure(d.kap_url)
                    if disc and disc.get("full_text"):
                        body = disc["full_text"]
                        d.body = body
                        stats["body_refetched"] += 1
                except Exception:
                    pass

            try:
                res = await div_process(
                    db,
                    disclosure_id=d.id,
                    ticker=d.company_code,
                    company_name=None,
                    title=title,
                    body=body,
                    kap_url=d.kap_url,
                    published_at=d.published_at,
                )
                if res:
                    stats["processed"] += 1
                    if len(stats["events"]) < 30:
                        stats["events"].append({
                            "ticker": d.company_code,
                            "title": title[:80],
                            "event_type": res.event_type if hasattr(res, "event_type") else None,
                            "status": res.status,
                            "period": res.period,
                        })
            except Exception as e:
                stats["errors"] += 1
                logger.warning("Backfill dividend hata (%s): %s", d.company_code, e)

        await db.commit()

    return {"status": "ok", "hours": hours, **stats}


@app.post("/api/v1/admin/reprocess-dividend-disclosure")
@limiter.limit("5/minute")
async def admin_reprocess_dividend_disclosure(request: Request, payload: dict = Body(...)):
    """Admin: Bir KAP bildirimini (ticker veya kap_url) temettu state machine'inden
    yeniden gecir. Body kisa gelmisse KAP'tan re-fetch eder.

    Body: {"admin_password": "...", "ticker": "TEZOL"} VEYA {"kap_url": "..."}
    Optional: {"title_contains": "kar payı dağıtım"} - en yeni eslesen kayit secilir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.dividend_calendar_processor import process_kap_disclosure as div_process
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure

    ticker = (payload.get("ticker") or "").upper().strip() or None
    kap_url = payload.get("kap_url") or None
    title_contains = (payload.get("title_contains") or "kar payı dağıtım").lower()

    async with async_session() as db:
        # Hedef bildirimi bul
        q = select(KapAllDisclosure)
        if kap_url:
            q = q.where(KapAllDisclosure.kap_url == kap_url)
        elif ticker:
            q = q.where(KapAllDisclosure.company_code == ticker).order_by(desc(KapAllDisclosure.published_at)).limit(10)
        else:
            return {"status": "error", "message": "ticker veya kap_url gerekli"}

        res = await db.execute(q)
        rows = list(res.scalars().all())
        # title filter (case-insensitive)
        if ticker and title_contains:
            rows = [r for r in rows if title_contains in (r.title or "").lower()]
        if not rows:
            return {"status": "error", "message": "Eslesen bildirim bulunamadi"}

        disclosure = rows[0]
        body = disclosure.body or ""
        # Body kisa ise KAP'tan re-fetch (2 katmanli fallback)
        if (not body or len(body) < 500) and disclosure.kap_url:
            try:
                disc = await fetch_kap_disclosure(disclosure.kap_url)
                if disc and disc.get("full_text"):
                    body = disc["full_text"]
            except Exception as e:
                logger.warning("KAP disclosure fetch hata (%s): %s", disclosure.company_code, e)
            # Hala kisaysa daha agresif HTML fetcher dene
            if not body or len(body) < 500:
                try:
                    from app.scrapers.kap_all_scraper import fetch_kap_page_content
                    page_body = await fetch_kap_page_content(disclosure.kap_url)
                    if page_body and len(page_body) > len(body or ""):
                        body = page_body
                except Exception as e:
                    logger.warning("KAP page fetch hata (%s): %s", disclosure.company_code, e)
            if body:
                disclosure.body = body
                await db.commit()

        result = await div_process(
            db,
            disclosure_id=disclosure.id,
            ticker=disclosure.company_code,
            company_name=None,
            title=disclosure.title,
            body=body,
            kap_url=disclosure.kap_url,
            published_at=disclosure.published_at,
        )
        await db.commit()

        return {
            "status": "ok",
            "ticker": disclosure.company_code,
            "title": disclosure.title,
            "kap_url": disclosure.kap_url,
            "body_length": len(body),
            "dividend_id": result.id if result else None,
            "event_type": result.event_type if result else None,
            "period": result.period if result else None,
            "gross_amount_per_share": float(result.gross_amount_per_share) if result and result.gross_amount_per_share else None,
            "payment_date": result.payment_date.isoformat() if result and result.payment_date else None,
        }


@app.post("/api/v1/admin/reprocess-payment-announcement")
@limiter.limit("3/minute")
async def admin_reprocess_payment_announcement(request: Request, payload: dict = Body(...)):
    """Admin: Belirli bir KAP URL'sindeki temettü ödeme duyurusunu yeniden işle.

    Body: {"admin_password": "...", "kap_url": "https://www.kap.org.tr/tr/Bildirim/1597336"}
    Body fetch edilir → parse_dividend_payment_announcement → DividendCalendar
    'odeniyor'/'tamamlandi' güncellenir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    kap_url = payload.get("kap_url", "")
    if not kap_url:
        return {"status": "error", "message": "kap_url gerekli"}

    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.dividend_calendar_processor import (
        parse_dividend_payment_announcement,
        process_dividend_payment_announcement,
    )

    disc = await fetch_kap_disclosure(kap_url)
    if not disc:
        return {"status": "error", "message": "KAP fetch fail"}

    body = disc.get("full_text") or disc.get("body") or ""
    if not body:
        return {"status": "error", "message": "body bos"}

    parsed = parse_dividend_payment_announcement(body)

    async for db in get_db():
        result = await process_dividend_payment_announcement(
            db,
            body=body,
            kap_url=kap_url,
            disclosure_id=None,
            published_at=datetime.now(timezone.utc),
        )
        await db.commit()

        return {
            "status": "ok",
            "kap_url": kap_url,
            "body_length": len(body),
            "parsed_count": len(parsed),
            "parsed_sample": parsed[:20],
            "process_result": result,
        }


@app.post("/api/v1/admin/cleanup-fund-dividends")
@limiter.limit("3/minute")
async def admin_cleanup_fund_dividends(request: Request, payload: dict = Body(...)):
    """Admin: dividend_calendar tablosundan fon/ETF/GYO ticker'ları sil.

    Pattern: 6+ karakter, sayı içerir, F/FX/FN/FY ile biter.
    Örnek: ZTM25F, BIST30F, AKBNF gibi yatırım fonları.

    Body: {"admin_password": "...", "dry_run": false}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.dividend_calendar import DividendCalendar
    from app.services.dividend_calendar_processor import is_fund_ticker
    from sqlalchemy import delete as _sa_delete

    dry_run = bool(payload.get("dry_run", False))

    async for db in get_db():
        result = await db.execute(select(DividendCalendar))
        rows = result.scalars().all()
        bad_ids = []
        sample = []
        for r in rows:
            if is_fund_ticker(r.ticker or ""):
                bad_ids.append(r.id)
                if len(sample) < 30:
                    sample.append({
                        "id": r.id,
                        "ticker": r.ticker,
                        "company_name": r.company_name,
                        "status": r.status,
                    })

        if dry_run:
            return {
                "status": "ok",
                "dry_run": True,
                "would_delete": len(bad_ids),
                "sample": sample,
            }

        if bad_ids:
            await db.execute(
                _sa_delete(DividendCalendar).where(DividendCalendar.id.in_(bad_ids))
            )
            await db.commit()

        return {
            "status": "ok",
            "deleted": len(bad_ids),
            "sample": sample,
        }


@app.post("/api/v1/admin/reclassify-dividend-calendar")
@limiter.limit("3/minute")
async def admin_reclassify_dividend_calendar(request: Request, payload: dict = Body(...)):
    """Admin: dividend_calendar kayıtlarını yeniden classify et.

    Mevcut KAP body'sini kullanarak (yeniden fetch yok) status'u günceller:
    - Eğer body'de dağıtmama ifadesi varsa ve status 'reddedildi' DEĞİLse → 'reddedildi'
    - Sayısal alanları NULL'a sıfırla (yanlış AI parse'tan kalanlar)

    Body: {"admin_password": "...", "limit": 200}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.dividend_calendar import DividendCalendar
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.dividend_calendar_processor import classify_event_with_body

    limit = int(payload.get("limit", 200))
    fixed = []

    async for db in get_db():
        # Status reddedildi DEĞİL ama YKK kap_url'i olan kayıtlar
        result = await db.execute(
            select(DividendCalendar)
            .where(DividendCalendar.status != "reddedildi")
            .where(DividendCalendar.ykk_kap_disclosure_id.isnot(None))
            .order_by(desc(DividendCalendar.created_at))
            .limit(limit)
        )
        rows = result.scalars().all()

        for row in rows:
            try:
                # YKK disclosure body'sini al
                disc_id = row.ykk_kap_disclosure_id
                if not disc_id:
                    continue
                d_result = await db.execute(
                    select(KapAllDisclosure).where(KapAllDisclosure.id == disc_id).limit(1)
                )
                disc = d_result.scalar_one_or_none()
                if not disc or not disc.body:
                    continue

                event = classify_event_with_body(disc.title or "", disc.body or "")
                if event == "rejection":
                    # Yanlış sınıflandırılmış → düzelt
                    row.status = "reddedildi"
                    row.rejected_at = datetime.now(timezone.utc)
                    row.rejection_kap_disclosure_id = disc_id
                    row.rejection_kap_url = row.ykk_kap_url
                    # Yanlış AI değerlerini temizle
                    row.gross_amount_per_share = None
                    row.net_amount_per_share = None
                    row.gross_yield_pct = None
                    row.net_yield_pct = None
                    row.total_amount_tl = None
                    fixed.append({
                        "id": row.id,
                        "ticker": row.ticker,
                        "old_status": "ykk_alindi (yanlış)",
                        "new_status": "reddedildi",
                    })
            except Exception as e:
                logger.warning("dividend reclassify hata (%s): %s", row.ticker, e)

        if fixed:
            await db.commit()

        return {
            "status": "ok",
            "scanned": len(rows),
            "fixed_count": len(fixed),
            "fixed": fixed[:30],
        }


@app.post("/api/v1/admin/reparse-incomplete-type-conversions")
@limiter.limit("3/minute")
async def admin_reparse_incomplete_type_conversions(request: Request, payload: dict = Body(...)):
    """Admin: Eksik (investor_name='?' veya converted_lot null) tipe dönüşüm
    kayıtlarını KAP'tan yeniden fetch edip parse ederek günceller. SİLMEZ.

    Body: {"admin_password": "...", "limit": 100}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.share_type_conversion import ShareTypeConversion
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.kap_category_processors import _parse_tc_table
    from sqlalchemy import or_ as _or_

    limit = int(payload.get("limit", 100))
    bad_filter = _or_(
        ShareTypeConversion.investor_name.is_(None),
        ShareTypeConversion.investor_name == "",
        ShareTypeConversion.investor_name == "?",
        ShareTypeConversion.converted_lot.is_(None),
    )

    fixed = []
    failed = []

    async for db in get_db():
        result = await db.execute(
            select(ShareTypeConversion)
            .where(bad_filter)
            .order_by(desc(ShareTypeConversion.id))
            .limit(limit)
        )
        rows = result.scalars().all()

        # Aynı kap_url için bir kez fetch yap (cache)
        url_cache: dict = {}

        for row in rows:
            if not row.kap_url:
                failed.append({"id": row.id, "ticker": row.ticker, "reason": "kap_url yok"})
                continue
            try:
                if row.kap_url not in url_cache:
                    disc = await fetch_kap_disclosure(row.kap_url)
                    body = (disc or {}).get("full_text") or ""
                    url_cache[row.kap_url] = _parse_tc_table(body)
                table_rows = url_cache[row.kap_url]

                # Bu satıra ait ticker'ı tablo satırlarında ara
                match = None
                for d in table_rows:
                    if d.get("ticker", "").upper() == (row.ticker or "").upper():
                        match = d
                        break

                if not match:
                    failed.append({"id": row.id, "ticker": row.ticker, "reason": "tabloda eşleşme yok"})
                    continue

                changed = False
                if (not row.investor_name or row.investor_name in ("", "?")) and match.get("investor_name"):
                    row.investor_name = match["investor_name"][:255]
                    changed = True
                if row.converted_lot is None and match.get("nominal_tl"):
                    row.converted_lot = int(match["nominal_tl"])
                    changed = True
                if (not row.company_name) and match.get("company_name"):
                    row.company_name = match["company_name"][:255]
                    changed = True

                if changed:
                    fixed.append({
                        "id": row.id,
                        "ticker": row.ticker,
                        "investor_name": row.investor_name,
                        "converted_lot": row.converted_lot,
                    })

            except Exception as e:
                failed.append({"id": row.id, "ticker": row.ticker, "reason": str(e)[:200]})

        if fixed:
            await db.commit()

        return {
            "status": "ok",
            "scanned": len(rows),
            "fixed_count": len(fixed),
            "failed_count": len(failed),
            "fixed": fixed[:20],
            "failed": failed[:20],
        }


@app.post("/api/v1/admin/reparse-incomplete-share-transactions")
@limiter.limit("3/minute")
async def admin_reparse_incomplete_share_transactions(request: Request, payload: dict = Body(...)):
    """Admin: Eksik (party_name yok veya tum oran/fiyat null) kayitlari KAP'tan
    yeniden fetch edip parse ederek günceller. SİLMEZ.

    Body: {"admin_password": "...", "limit": 50}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.share_transaction_detail import ShareTransactionDetail
    from app.services.kap_pay_alim_satim_fetcher import fetch_kap_pay_alim_satim
    from sqlalchemy import or_ as _or_, and_ as _and_

    limit = int(payload.get("limit", 50))
    bad_filter = _or_(
        ShareTransactionDetail.party_name.is_(None),
        ShareTransactionDetail.party_name == "",
        ShareTransactionDetail.party_name == "?",
        _and_(
            ShareTransactionDetail.oy_hakki_pct.is_(None),
            ShareTransactionDetail.pay_orani_pct.is_(None),
            ShareTransactionDetail.price_low.is_(None),
            ShareTransactionDetail.nominal_lot.is_(None),
        ),
    )

    fixed = []
    failed = []

    async for db in get_db():
        result = await db.execute(
            select(ShareTransactionDetail)
            .where(bad_filter)
            .order_by(desc(ShareTransactionDetail.id))
            .limit(limit)
        )
        rows = result.scalars().all()

        for row in rows:
            if not row.kap_url:
                failed.append({"id": row.id, "ticker": row.ticker, "reason": "kap_url yok"})
                continue
            try:
                parsed = await fetch_kap_pay_alim_satim(row.kap_url)
                if not parsed:
                    failed.append({"id": row.id, "ticker": row.ticker, "reason": "parse failed"})
                    continue

                changed = False

                # party_name güncelle
                new_party = (
                    parsed.get("party_name")
                    or parsed.get("party_name_header")
                )
                if not new_party:
                    body = parsed.get("body_text") or ""
                    from app.services.kap_pay_alim_satim_fetcher import extract_party_name as _epn
                    new_party = _epn(body)
                if new_party and new_party != row.party_name:
                    row.party_name = new_party
                    changed = True

                # Sayisal alanlar
                for src_key, db_attr in [
                    ("end_pay_oran_pct", "pay_orani_pct"),
                    ("end_oy_hakki_pct", "oy_hakki_pct"),
                ]:
                    v = parsed.get(src_key)
                    if v is not None and getattr(row, db_attr) is None:
                        setattr(row, db_attr, v)
                        changed = True

                # Pay/oy değişimi
                if parsed.get("end_pay_oran_pct") is not None and parsed.get("beginning_pay_oran_pct") is not None:
                    diff = parsed["end_pay_oran_pct"] - parsed["beginning_pay_oran_pct"]
                    if row.pay_orani_change_pct is None:
                        row.pay_orani_change_pct = diff
                        changed = True
                if parsed.get("end_oy_hakki_pct") is not None and parsed.get("beginning_oy_hakki_pct") is not None:
                    diff = parsed["end_oy_hakki_pct"] - parsed["beginning_oy_hakki_pct"]
                    if row.oy_hakki_change_pct is None:
                        row.oy_hakki_change_pct = diff
                        changed = True

                # Fiyat
                if parsed.get("price_low") is not None and row.price_low is None:
                    row.price_low = parsed["price_low"]
                    changed = True
                if parsed.get("price_high") is not None and row.price_high is None:
                    row.price_high = parsed["price_high"]
                    changed = True

                # Nominal lot
                alim = parsed.get("alim_nominal") or 0
                satim = parsed.get("satim_nominal") or 0
                lot = int(abs(alim - satim) if alim and satim else (alim or satim or 0))
                if lot and row.nominal_lot is None:
                    row.nominal_lot = lot
                    changed = True

                if changed:
                    fixed.append({
                        "id": row.id,
                        "ticker": row.ticker,
                        "party_name": row.party_name,
                    })

            except Exception as e:
                failed.append({"id": row.id, "ticker": row.ticker, "reason": str(e)[:200]})

        if fixed:
            await db.commit()

        return {
            "status": "ok",
            "scanned": len(rows),
            "fixed_count": len(fixed),
            "failed_count": len(failed),
            "fixed": fixed[:20],
            "failed": failed[:20],
        }


@app.post("/api/v1/admin/cleanup-incomplete-share-transactions")
@limiter.limit("3/minute")
async def admin_cleanup_incomplete_share_transactions(request: Request, payload: dict = Body(...)):
    """Admin: Eksik (party_name yok veya tum oran/fiyat null) kayitlari sil.

    Body: {"admin_password": "...", "dry_run": true}
    dry_run=true ise sadece sayar, silmez.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.share_transaction_detail import ShareTransactionDetail
    from sqlalchemy import or_ as _or_, and_ as _and_, delete as _sa_delete

    dry_run = bool(payload.get("dry_run", False))

    # Eksik kayit kriteri:
    #  1) party_name NULL/bos/'?' VEYA
    #  2) tum sayisal alanlar null (oy_hakki_pct, pay_orani_pct, price_low, nominal_lot)
    bad_filter = _or_(
        ShareTransactionDetail.party_name.is_(None),
        ShareTransactionDetail.party_name == "",
        ShareTransactionDetail.party_name == "?",
        _and_(
            ShareTransactionDetail.oy_hakki_pct.is_(None),
            ShareTransactionDetail.pay_orani_pct.is_(None),
            ShareTransactionDetail.price_low.is_(None),
            ShareTransactionDetail.nominal_lot.is_(None),
        ),
    )

    # Once say
    count_q = select(ShareTransactionDetail).where(bad_filter)
    async for db in get_db():
        count_result = await db.execute(count_q)
        bad_rows = count_result.scalars().all()
        bad_count = len(bad_rows)

        sample = [
            {
                "id": r.id,
                "ticker": r.ticker,
                "party_name": r.party_name,
                "transaction_date": r.transaction_date.isoformat() if r.transaction_date else None,
            }
            for r in bad_rows[:10]
        ]

        if dry_run:
            return {
                "status": "ok",
                "dry_run": True,
                "would_delete": bad_count,
                "sample": sample,
            }

        # Sil
        del_stmt = _sa_delete(ShareTransactionDetail).where(bad_filter)
        await db.execute(del_stmt)
        await db.commit()

        return {
            "status": "ok",
            "deleted": bad_count,
            "sample": sample,
        }


@app.post("/api/v1/admin/reclassify-share-transactions")
@limiter.limit("3/minute")
async def admin_reclassify_share_transactions(request: Request, payload: dict = Body(...)):
    """Admin: Mevcut share_transaction_details kayitlarini pay/oy degisim isaretine
    gore yeniden siniflandirir.

    Kural: pay_orani_change_pct > 0 -> 'alici', < 0 -> 'satici'.
    Eger pay degisimi yoksa oy_hakki_change_pct'e bakilir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.share_transaction_detail import ShareTransactionDetail
    from app.database import async_session

    fixed = 0
    checked = 0
    async with async_session() as db:
        rows = (await db.execute(select(ShareTransactionDetail))).scalars().all()
        for r in rows:
            checked += 1
            chg = r.pay_orani_change_pct
            if chg is None or chg == 0:
                chg = r.oy_hakki_change_pct
            if chg is None or chg == 0:
                continue
            new_type = "alici" if chg > 0 else "satici"
            if r.transaction_type != new_type:
                r.transaction_type = new_type
                fixed += 1
        await db.commit()
    return {"checked": checked, "fixed": fixed}


@app.post("/api/v1/admin/import-share-transactions")
@limiter.limit("3/minute")
async def admin_import_share_transactions(request: Request, payload: dict = Body(...)):
    """Admin: Ham metni parse edip share_transaction_details tablosuna yazar.

    Body:
        admin_password: str
        raw_text: str — Ucretsizderinlikbot/KAP kopya-yapistir formati
        replace_existing: bool (default False) — True ise ayni anahtarlilari overwrite eder
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    raw_text = payload.get("raw_text", "")
    if not raw_text or not isinstance(raw_text, str):
        raise HTTPException(status_code=400, detail="raw_text gerekli")

    replace_existing = bool(payload.get("replace_existing", False))

    try:
        from app.services.share_transaction_parser import parse_records
        from app.models.share_transaction_detail import ShareTransactionDetail
        from app.database import async_session
        from sqlalchemy import select as _sel

        records = parse_records(raw_text)
        if not records:
            return {"status": "ok", "parsed": 0, "inserted": 0, "skipped": 0, "message": "Metinden kayit cikarilamadi"}

        inserted = 0
        skipped = 0
        updated = 0
        errors: list[str] = []

        async with async_session() as db:
            for rec in records:
                try:
                    stmt = _sel(ShareTransactionDetail).where(
                        ShareTransactionDetail.ticker == rec["ticker"],
                        ShareTransactionDetail.transaction_date == rec["transaction_date"],
                        ShareTransactionDetail.transaction_type == rec["transaction_type"],
                        ShareTransactionDetail.party_name == rec["party_name"],
                    ).limit(1)
                    existing = (await db.execute(stmt)).scalar_one_or_none()

                    if existing:
                        if replace_existing:
                            for k, v in rec.items():
                                setattr(existing, k, v)
                            updated += 1
                        else:
                            skipped += 1
                        continue

                    new_row = ShareTransactionDetail(
                        **rec,
                        source="manual_import",
                    )
                    db.add(new_row)
                    inserted += 1
                except Exception as inner_e:
                    errors.append(f"{rec.get('ticker', '?')}: {inner_e}")

            await db.commit()

        return {
            "status": "ok",
            "parsed": len(records),
            "inserted": inserted,
            "updated": updated,
            "skipped_duplicates": skipped,
            "errors": errors[:10],
        }
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "message": str(e)[:500],
            "traceback": traceback.format_exc()[-1000:] if not settings.is_production else None,
        }


# -------------------------------------------------------
# KULLANICI TAKIP LISTESI (WATCHLIST) ENDPOINTS
# -------------------------------------------------------

@app.get("/api/v1/users/{device_id}/kap-watchlist", response_model=list[WatchlistItemOut])
async def get_user_watchlist(
    device_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin KAP hisse takip listesini getir."""
    result = await db.execute(
        select(UserWatchlist)
        .where(UserWatchlist.device_id == device_id)
        .order_by(UserWatchlist.created_at)
    )
    return list(result.scalars().all())


@app.post("/api/v1/users/{device_id}/kap-watchlist")
async def add_to_watchlist(
    device_id: str,
    body: WatchlistAddRequest,
    db: AsyncSession = Depends(get_db),
):
    """Takip listesine hisse ekle.

    Tier limitleri (2026-05 sonrası — birleşik portföy + watchlist):
    - Free: 5 hisse toplam
    - KAP AI PRO (ana_yildiz): 25 hisse toplam
    - Diamond: sınırsız
    NOT: portföy frontend-only AsyncStorage'da, backend sadece watchlist sayar.
    Birleşik kontrol frontend'de yapılır; backend watchlist için tier limiti uygular.
    """
    ticker = body.ticker.upper().strip()
    if not ticker or len(ticker) < 2 or len(ticker) > 10:
        raise HTTPException(status_code=400, detail="Gecersiz hisse kodu")

    # Kullanici kontrolu
    user_result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    # Tier kontrolü
    is_diamond = False
    is_vip = False
    sub_result = await db.execute(
        select(UserSubscription).where(
            and_(
                UserSubscription.user_id == user.id,
                or_(
                    UserSubscription.is_active == True,
                    UserSubscription.expires_at > datetime.utcnow(),
                ),
            )
        )
    )
    for sub in sub_result.scalars().all():
        pkg = (sub.package or "").lower()
        if "diamond" in pkg or "bilanco_temettu" in pkg:
            is_diamond = True
        if pkg == "ana_yildiz":
            is_vip = True

    # Zaten takip ediliyor mu?
    existing = await db.execute(
        select(UserWatchlist).where(
            UserWatchlist.device_id == device_id,
            UserWatchlist.ticker == ticker,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Bu hisse zaten takip listenizde")

    # Limit kontrolu (Free: 5, VIP: 25, Diamond: sınırsız)
    count_result = await db.execute(
        select(func.count(UserWatchlist.id)).where(
            UserWatchlist.device_id == device_id
        )
    )
    current_count = count_result.scalar() or 0

    if is_diamond:
        pass  # sınırsız
    elif is_vip:
        if current_count >= 25:
            raise HTTPException(
                status_code=403,
                detail="KAP AI PRO kullanıcılar toplam 25 hisse takip edebilir. Sınırsız için Diamond."
            )
    else:
        if current_count >= 5:
            raise HTTPException(
                status_code=403,
                detail="Ücretsiz kullanıcılar toplam 5 hisse takip edebilir. Daha fazlası için PRO (25) veya Diamond (100)."
            )

    pref = body.notification_preference if body.notification_preference in ("both", "positive_only", "negative_only", "all", "positive_negative") else "both"
    item = UserWatchlist(device_id=device_id, ticker=ticker, notification_preference=pref)
    db.add(item)
    await db.flush()

    return {"success": True, "ticker": ticker, "is_vip": is_vip, "is_diamond": is_diamond}


@app.patch("/api/v1/users/{device_id}/kap-watchlist/{ticker}/preference")
async def update_watchlist_preference(
    device_id: str,
    ticker: str,
    body: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Takip listesindeki hissenin bildirim tercihini guncelle.

    body: {"notification_preference": "both" | "positive_only" | "negative_only" | "all" | "positive_negative"}
    """
    ticker = ticker.upper().strip()
    pref = body.get("notification_preference", "both")
    if pref not in ("both", "positive_only", "negative_only", "all", "positive_negative"):
        raise HTTPException(status_code=400, detail="Gecersiz bildirim tercihi")

    result = await db.execute(
        update(UserWatchlist)
        .where(
            UserWatchlist.device_id == device_id,
            UserWatchlist.ticker == ticker,
        )
        .values(notification_preference=pref)
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Hisse takip listenizde bulunamadi")

    return {"success": True, "ticker": ticker, "notification_preference": pref}


@app.delete("/api/v1/users/{device_id}/kap-watchlist/{ticker}")
async def remove_from_watchlist(
    device_id: str,
    ticker: str,
    db: AsyncSession = Depends(get_db),
):
    """Takip listesinden hisse cikar."""
    ticker = ticker.upper().strip()
    result = await db.execute(
        delete(UserWatchlist).where(
            UserWatchlist.device_id == device_id,
            UserWatchlist.ticker == ticker,
        )
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Hisse takip listenizde bulunamadi")

    return {"success": True, "ticker": ticker}


@app.get("/api/v1/admin/test-pdf-fetch")
async def admin_test_pdf_fetch(url: str):
    """Debug: KAP PDF fetch test."""
    import httpx, io
    try:
        import pdfplumber
    except Exception as e:
        return {"error": "pdfplumber import: " + str(e)}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
        "Accept": "application/pdf,*/*",
        "Accept-Language": "tr-TR,tr;q=0.9",
        "Referer": "https://www.kap.org.tr/tr/Bildirim/1600871",
    }
    out: dict = {"url": url}
    try:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=20.0) as client:
            # Warmup
            try:
                w = await client.get("https://www.kap.org.tr/tr/Bildirim/1600871")
                out["warmup_status"] = w.status_code
            except Exception as we:
                out["warmup_error"] = str(we)[:200]
            r = await client.get(url)
            out["status"] = r.status_code
            out["len"] = len(r.content)
            out["ctype"] = r.headers.get("content-type")
            if r.status_code == 200 and "pdf" in (r.headers.get("content-type","").lower()):
                with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                    out["pages"] = len(pdf.pages)
                    out["text_p1"] = (pdf.pages[0].extract_text() or "")[:500]
        return out
    except Exception as e:
        import traceback
        return {"error": str(e)[:300], "trace": traceback.format_exc()[:1000]}


@app.get("/api/v1/admin/raw-cf/{ticker}")
async def admin_raw_cf(ticker: str):
    """Debug: raw SQL ile company_financials sorgula."""
    from sqlalchemy import text as sa_text
    from app.database import async_session
    async with async_session() as db:
        res = await db.execute(sa_text("""
            SELECT period, current_assets, non_current_assets, net_debt, total_debt, source, updated_at
            FROM company_financials
            WHERE ticker = :tk
            ORDER BY period DESC LIMIT 6
        """), {"tk": ticker.upper()})
        rows = res.fetchall()
    return [{"period": r[0], "curr_assets": float(r[1]) if r[1] else None,
             "noncurr": float(r[2]) if r[2] else None,
             "net_debt": float(r[3]) if r[3] else None,
             "total_debt": float(r[4]) if r[4] else None,
             "source": r[5], "updated_at": str(r[6])} for r in rows]


@app.post("/api/v1/admin/refresh-isyatirim-bilanco")
@limiter.limit("2/minute")
async def admin_refresh_isyatirim_bilanco(request: Request, payload: dict = Body(...)):
    """Admin: IsYatirim'den fresh bilanco cekip eksik current_assets/non_current_assets/total_debt
    olan ticker'lari toplu guncelle.

    Body: {
      "admin_password": "...",
      "tickers": ["SASA","NTGAZ"]  # opsiyonel, yoksa eksik veri olan TUM ticker'lar
      "limit": 50  # max ticker sayisi
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    from app.scrapers.isyatirim_scraper import fetch_bilanco
    import asyncio as _asyncio
    import gc as _gc
    tickers = payload.get("tickers")
    limit = int(payload.get("limit") or 30)
    fixed_tickers = []
    skipped = []
    async with async_session() as db:
        if not tickers:
            res = await db.execute(sa_text("""
                SELECT DISTINCT ticker FROM company_financials
                WHERE (current_assets IS NULL OR current_assets = 0
                       OR non_current_assets IS NULL OR non_current_assets = 0)
                ORDER BY ticker
                LIMIT :lim
            """), {"lim": limit})
            tickers = [row[0] for row in res.fetchall()]
        else:
            tickers = [t.upper().strip() for t in tickers if t][:limit]

    # Her ticker icin IsYatirim'den fetch + DIRECT SQL UPDATE
    UPDATE_SQL = sa_text("""
        UPDATE company_financials
        SET current_assets = COALESCE(:ca, current_assets),
            non_current_assets = COALESCE(:nca, non_current_assets),
            total_debt = COALESCE(:td, total_debt),
            net_debt = COALESCE(:nd, net_debt),
            cash_and_equivalents = COALESCE(:cash, cash_and_equivalents),
            gross_profit = COALESCE(:gp, gross_profit),
            operating_profit = COALESCE(:op, operating_profit),
            current_ratio = COALESCE(:cr, current_ratio),
            debt_to_equity = COALESCE(:de, debt_to_equity),
            source = 'isyatirim',
            updated_at = NOW()
        WHERE ticker = :tk AND period = :pd
    """)
    INSERT_SQL = sa_text("""
        INSERT INTO company_financials(ticker, period, period_end_date, revenue, gross_profit,
            operating_profit, net_income, ebitda, total_assets, total_equity, total_debt,
            net_debt, cash_and_equivalents, current_assets, non_current_assets,
            current_ratio, gross_margin_pct, net_margin_pct, roe_pct, debt_to_equity,
            source, scraped_at, updated_at)
        VALUES(:tk, :pd, :ped, :rev, :gp, :op, :ni, :eb, :ta, :te, :td,
               :nd, :cash, :ca, :nca, :cr, :gm, :nm, :roe, :de,
               'isyatirim', NOW(), NOW())
        ON CONFLICT (ticker, period) DO NOTHING
    """)
    for ticker in tickers:
        try:
            periods = await fetch_bilanco(ticker, years=3)
            if not periods:
                skipped.append({"ticker": ticker, "reason": "no_data"})
                continue
            updated = 0
            sample_curr = None
            sample_period = None
            async with async_session() as db2:
                for p in periods:
                    # Once UPDATE dene
                    res = await db2.execute(UPDATE_SQL, {
                        "tk": ticker, "pd": p["period"],
                        "ca": p.get("current_assets"),
                        "nca": p.get("non_current_assets"),
                        "td": p.get("total_debt"),
                        "nd": p.get("net_debt"),
                        "cash": p.get("cash_and_equivalents"),
                        "gp": p.get("gross_profit"),
                        "op": p.get("operating_profit"),
                        "cr": p.get("current_ratio"),
                        "de": p.get("debt_to_equity"),
                    })
                    if res.rowcount == 0:
                        # Yoksa INSERT
                        try:
                            from datetime import datetime as _dt2
                            ped = p.get("period_end_date")
                            ped_obj = _dt2.strptime(ped, "%Y-%m-%d") if ped else None
                            await db2.execute(INSERT_SQL, {
                                "tk": ticker, "pd": p["period"], "ped": ped_obj,
                                "rev": p.get("revenue"), "gp": p.get("gross_profit"),
                                "op": p.get("operating_profit"), "ni": p.get("net_income"),
                                "eb": p.get("ebitda"), "ta": p.get("total_assets"),
                                "te": p.get("total_equity"), "td": p.get("total_debt"),
                                "nd": p.get("net_debt"), "cash": p.get("cash_and_equivalents"),
                                "ca": p.get("current_assets"), "nca": p.get("non_current_assets"),
                                "cr": p.get("current_ratio"), "gm": p.get("gross_margin_pct"),
                                "nm": p.get("net_margin_pct"), "roe": p.get("roe_pct"),
                                "de": p.get("debt_to_equity"),
                            })
                        except Exception:
                            pass
                    else:
                        updated += 1
                    if p.get("current_assets") and not sample_curr:
                        sample_curr = p.get("current_assets")
                        sample_period = p.get("period")
                await db2.commit()
            fixed_tickers.append({
                "ticker": ticker,
                "periods_fetched": len(periods),
                "updated_rows": updated,
                "sample_period": sample_period,
                "sample_curr": sample_curr,
            })
            del periods
            _gc.collect()
            await _asyncio.sleep(2)
        except Exception as e:
            skipped.append({"ticker": ticker, "error": str(e)[:200]})
    return {
        "fixed_count": len(fixed_tickers),
        "skipped_count": len(skipped),
        "fixed": fixed_tickers[:50],
        "skipped": skipped[:30],
    }


@app.post("/api/v1/admin/parse-bilanco-from-url")
@limiter.limit("10/minute")
async def admin_parse_bilanco_from_url(request: Request, payload: dict = Body(...)):
    """Admin: Verilen KAP URL'sinden body'i cek, AI ile parse et,
    company_financials'a kaydet. IsYatirim'a hic dokunmaz.

    Body: {
      "admin_password": "...",
      "ticker": "SDTTR",
      "kap_url": "https://www.kap.org.tr/tr/Bildirim/1600155"
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    ticker = (payload.get("ticker") or "").upper().strip()
    kap_url = (payload.get("kap_url") or "").strip()
    if not ticker or not kap_url:
        raise HTTPException(status_code=400, detail="ticker ve kap_url gerekli")

    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.ai_bilanco_analyzer import parse_bilanco_from_kap, save_parsed_bilanco

    steps: list[str] = []
    try:
        disclosure = await fetch_kap_disclosure(kap_url)
        body = disclosure.get("full_text", "") if disclosure else ""
        steps.append(f"KAP body fetched (RSC): {len(body or '')} chars")
        if not body:
            return {"status": "error", "ticker": ticker, "steps": steps, "msg": "KAP body bos"}

        parsed = await parse_bilanco_from_kap(ticker, body)
        if not parsed:
            return {"status": "error", "ticker": ticker, "steps": steps + ["AI parse: bos sonuc"], "msg": "AI bilanco verisi cikartamadi"}

        steps.append(f"AI parsed: revenue={parsed.get('revenue')}, net_income={parsed.get('net_income')}, period={parsed.get('period')}")

        await save_parsed_bilanco(ticker, parsed)
        steps.append("save_parsed_bilanco OK -> company_financials")

        return {"status": "ok", "ticker": ticker, "steps": steps, "parsed": parsed}
    except Exception as e:
        import traceback
        return {
            "status": "error",
            "ticker": ticker,
            "steps": steps,
            "msg": str(e)[:300],
            "trace": traceback.format_exc()[-500:],
        }


@app.post("/api/v1/admin/backfill-kap-ai")
@limiter.limit("3/minute")
async def admin_backfill_kap_ai(request: Request, payload: dict = Body(...)):
    """ai_analyzed_at IS NULL olan recent KAP'lara AI analizi uygula.
    Body: { admin_password, hours (default=4), limit (default=30) }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from datetime import timedelta as _td3, timezone as _tz3, datetime as _dt3
    from sqlalchemy import select as _sel3, desc as _desc3
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.kap_all_analyzer import analyze_disclosure
    from app.database import async_session as _as3
    hours = int(payload.get("hours") or 4)
    limit = int(payload.get("limit") or 30)
    cutoff = _dt3.now(_tz3.utc) - _td3(hours=hours)
    updated = 0
    skipped = 0
    async with _as3() as db:
        rows = (await db.execute(
            _sel3(KapAllDisclosure)
            .where(KapAllDisclosure.published_at >= cutoff)
            .where(KapAllDisclosure.ai_analyzed_at.is_(None))
            .order_by(_desc3(KapAllDisclosure.published_at))
            .limit(limit)
        )).scalars().all()
        for r in rows:
            try:
                ai = await analyze_disclosure(
                    company_code=r.company_code or "",
                    title=r.title or "",
                    body=r.body or "",
                    is_bilanco=bool(r.is_bilanco),
                )
                if ai:
                    r.ai_sentiment = ai.get("sentiment")
                    r.ai_impact_score = ai.get("impact_score")
                    r.ai_summary = ai.get("summary")
                    r.ai_analyzed_at = _dt3.now(_tz3.utc)
                    updated += 1
            except Exception:
                skipped += 1
        await db.commit()
    return {"updated": updated, "skipped": skipped, "total": len(rows)}


@app.post("/api/v1/admin/inject-kap-bilanco")
@limiter.limit("10/minute")
async def admin_inject_kap_bilanco(request: Request, payload: dict = Body(...)):
    """Eksik kap_all_disclosures bilanço kayitlarini ekle.
    Body: { admin_password, items: [{ticker, kap_url, title, published_at?}] }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="items list bos")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    from datetime import datetime as _dt5, timezone as _tz5
    inserted = 0
    skipped = 0
    async with async_session() as db:
        for it in items:
            ticker = (it.get("ticker") or "").upper().strip()
            kap_url = (it.get("kap_url") or "").strip()
            title = (it.get("title") or "Finansal Durum Tablosu (Bilanço)").strip()
            pub = it.get("published_at") or _dt5.now(_tz5.utc).isoformat()
            if not ticker or not kap_url:
                skipped += 1
                continue
            try:
                # Idempotent — kap_url unique olmali
                check = await db.execute(sa_text("SELECT id FROM kap_all_disclosures WHERE kap_url=:k LIMIT 1"), {"k": kap_url})
                if check.scalar():
                    skipped += 1
                    continue
                await db.execute(sa_text("""
                    INSERT INTO kap_all_disclosures
                      (ticker, company_code, title, kap_url, published_at, is_bilanco, category, source, sentiment, ai_score)
                    VALUES (:t,:t,:ti,:k, CAST(:pub AS TIMESTAMPTZ), TRUE, 'Bilanço/Finansal Rapor', 'admin_inject', 'Nötr', 5.0)
                """), {"t": ticker, "ti": title, "k": kap_url, "pub": pub})
                inserted += 1
            except Exception:
                skipped += 1
        await db.commit()
    return {"inserted": inserted, "skipped": skipped}


@app.post("/api/v1/admin/sync-is-bilanco-flag")
@limiter.limit("3/minute")
async def admin_sync_is_bilanco_flag(request: Request, payload: dict = Body(...)):
    """kap_all_disclosures'taki bilanço title'li kayitlara is_bilanco=True set et.

    Frontend `/bilanco` listesi bu flag'i okuyor. Eski kayitlarda flag eksik kalmis olabilir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    async with async_session() as db:
        result = await db.execute(sa_text("""
            UPDATE kap_all_disclosures
            SET is_bilanco = TRUE
            WHERE is_bilanco = FALSE
              AND (
                LOWER(title) LIKE '%finansal durum tablosu%'
                OR LOWER(title) LIKE '%finansal rapor%'
                OR LOWER(title) LIKE '%bilanço%'
                OR LOWER(title) LIKE '%bilanco%'
                OR LOWER(title) LIKE '%finansal tablo%'
                OR LOWER(title) LIKE '%kar veya zarar%'
                OR LOWER(title) LIKE '%sorumluluk beyan%'
              )
            RETURNING id
        """))
        updated = len(result.fetchall())
        await db.commit()
    return {"updated": updated}


@app.post("/api/v1/admin/batch-bilanco-recent-kap")
@limiter.limit("3/minute")
async def admin_batch_bilanco_recent_kap(request: Request, payload: dict = Body(...)):
    """Son N gunde gelen bilanço KAP'larini tarayip her birini direct URL ile parse + save eder.

    Body: { admin_password, days (default=7), tickers? (filtre opsiyonel) }
    Pipeline'in kap_all_disclosures lookup'una bagli kalmadan, direkt company_code+kap_url uzerinden calisir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from datetime import timedelta as _td2, timezone as _tz3, datetime as _dt3
    from sqlalchemy import select as _sel2, desc as _desc2
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.ai_bilanco_analyzer import parse_bilanco_from_kap, save_parsed_bilanco
    from app.database import async_session as _as2

    days = int(payload.get("days") or 7)
    tickers_filter = payload.get("tickers")
    cutoff = _dt3.now(_tz3.utc) - _td2(days=days)

    BILANCO_TITLE_KEYS = ["finansal durum", "bilanço", "bilanco", "finansal rapor", "finansal tablo", "kar veya zarar tablosu"]

    processed = 0
    saved = 0
    skipped = 0
    errors: list[str] = []
    seen = set()
    import gc as _gc
    max_batch = int(payload.get("max_batch") or 5)  # OOM koruma — 5 ticker batch

    async with _as2() as db:
        q = _sel2(KapAllDisclosure).where(KapAllDisclosure.published_at >= cutoff).order_by(_desc2(KapAllDisclosure.published_at)).limit(200)
        rows = (await db.execute(q)).scalars().all()

    for r in rows:
        title_lo = (r.title or "").lower()
        if not any(k in title_lo for k in BILANCO_TITLE_KEYS):
            continue
        ticker = (r.company_code or "").upper().strip()
        if not ticker:
            continue
        if tickers_filter and ticker not in [t.upper() for t in tickers_filter]:
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        if not r.kap_url:
            continue
        processed += 1
        try:
            disc = await fetch_kap_disclosure(r.kap_url)
            body = disc.get("full_text", "") if disc else ""
            if disc:
                del disc
            if not body:
                skipped += 1
                continue
            parsed = await parse_bilanco_from_kap(ticker, body)
            del body  # 200K serbest birak
            _gc.collect()
            if not parsed or not parsed.get("period"):
                skipped += 1
                continue
            await save_parsed_bilanco(ticker, parsed)
            del parsed
            _gc.collect()
            saved += 1
        except Exception as e:
            errors.append(f"{ticker}: {str(e)[:120]}")
            skipped += 1
        if processed >= max_batch:
            break

    return {"processed": processed, "saved": saved, "skipped": skipped, "errors": errors[:5]}


@app.post("/api/v1/admin/process-bilanco-from-kap")
@limiter.limit("10/minute")
async def admin_process_bilanco_from_kap(request: Request, payload: dict = Body(...)):
    """Admin: KAP'tan yakalanan bilanço bildirimini AI ile parse edip
    company_financials tablosuna kaydeder.

    Body: { "admin_password": "...", "tickers": ["SDTTR","VKFYO"] }

    Akis: process_bilanco_bildirimi(ticker) — KAP body'sini AI parse +
    company_financials upsert. Tek hisse 5-15 saniye surer (AI cagrisi).
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    tickers = payload.get("tickers") or []
    force = bool(payload.get("force", False))  # True ise dedup atla (test için)
    if not isinstance(tickers, list) or not tickers:
        raise HTTPException(status_code=400, detail="tickers list olmali")

    from app.services.bilanco_pipeline import process_bilanco_bildirimi

    results: list[dict] = []
    for raw in tickers:
        ticker = str(raw).strip().upper()
        if not ticker:
            continue
        try:
            await process_bilanco_bildirimi(ticker, kap_title="manuel_admin", force=force)
            results.append({"ticker": ticker, "status": "ok"})
        except Exception as e:
            results.append({"ticker": ticker, "status": "error", "msg": str(e)[:200]})

    return {"processed": len(results), "results": results}


@app.post("/api/v1/admin/process-bistech-vbts")
@limiter.limit("20/minute")
async def admin_process_bistech_vbts(request: Request, payload: dict = Body(...)):
    """Admin: KAP URL'sinden BISTECH VBTS bildirimi parse + cautious_stocks update.

    Body: { "admin_password": "...", "kap_url": "https://www.kap.org.tr/tr/Bildirim/1601348" }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    kap_url = (payload.get("kap_url") or "").strip()
    if not kap_url:
        raise HTTPException(status_code=400, detail="kap_url gerekli")

    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.kap_category_processors import process_cautious_bistech_multi
    from app.database import async_session as _async_session
    from datetime import datetime as _dt, timezone as _tz

    try:
        disc = await fetch_kap_disclosure(kap_url)
        body = disc.get("full_text", "") if disc else ""
        if not body:
            return {"status": "error", "msg": "body bos"}

        from app.services.kap_category_processors import is_bistech_vbts, _TICKER_DOT_E_RE, _CS_PROMPT, _call_gemini
        title = payload.get("title") or "BISTECH Pay Piyasası Alım Satım Sistemi Duyurusu"
        debug = {
            "title": title,
            "body_len": len(body),
            "is_vbts": is_bistech_vbts(title, body),
            "tickers_in_body": sorted(set(_TICKER_DOT_E_RE.findall(body))),
        }
        if debug["is_vbts"] and debug["tickers_in_body"]:
            ai_raw = await _call_gemini(_CS_PROMPT.format(
                ticker=",".join(debug["tickers_in_body"]),
                title=title, body=body[:3000],
            ))
            debug["ai_parsed"] = ai_raw

        async with _async_session() as db:
            results = await process_cautious_bistech_multi(
                db, disclosure_id=0, title=title, body=body,
                kap_url=kap_url, published_at=_dt.now(_tz.utc),
            )
            await db.commit()

        return {
            "status": "ok",
            "kap_url": kap_url,
            "processed_tickers": [r.ticker for r in results],
            "count": len(results),
            "debug": debug,
        }
    except Exception as e:
        import traceback
        return {"status": "error", "msg": str(e)[:300], "trace": traceback.format_exc()[-400:]}


@app.post("/api/v1/admin/inject-kap-disclosure")
@limiter.limit("10/minute")
async def admin_inject_kap_disclosure(request: Request, payload: dict = Body(...)):
    """Admin: Belirli bir KAP bildirimini manuel olarak DB'ye ekle, AI analiz et,
    watchlist + portfoy kullanicilarına push gonder.

    Body: {
      "admin_password": "...",
      "ticker": "SDTTR",
      "title": "Finansal Rapor",
      "kap_url": "https://www.kap.org.tr/tr/Bildirim/1600155",
      "body": null  (otomatik fetch),
      "force_send": true  (DB'de varsa bile push at)
    }

    Tam akis: kap_all_disclosures'a ekle/getir → AI analiz → notify_kap_watchlist
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ticker = (payload.get("ticker") or "").upper().strip()
    title = (payload.get("title") or "Finansal Rapor").strip()
    kap_url = payload.get("kap_url")
    body_text = payload.get("body")
    force_send = bool(payload.get("force_send", False))

    if not ticker or not kap_url:
        raise HTTPException(status_code=400, detail="ticker ve kap_url gerekli")

    from app.database import async_session as _async_session
    from app.scrapers.kap_all_scraper import fetch_kap_page_content
    from app.services.kap_all_analyzer import analyze_disclosure
    from app.services.notification import NotificationService
    from datetime import datetime as _dt, timezone as _tz

    result: dict = {"ticker": ticker, "kap_url": kap_url, "steps": []}

    # 1. Body içeriği fetch
    if not body_text:
        try:
            body_text = await fetch_kap_page_content(kap_url)
            result["steps"].append(f"KAP body fetched ({len(body_text or '')} chars)")
        except Exception as e:
            result["steps"].append(f"KAP body fetch hatasi: {e}")

    async with _async_session() as db:
        # 2. DB'de var mi — UNIQUE constraint kap_url uzerinde, oncelikle url ile ara
        existing_q = await db.execute(
            select(KapAllDisclosure).where(
                KapAllDisclosure.kap_url == kap_url,
            ).limit(1)
        )
        disclosure = existing_q.scalar_one_or_none()
        if not disclosure:
            # URL bulunamadi — fallback: ayni ticker+title (eski kayitlar icin)
            existing_q2 = await db.execute(
                select(KapAllDisclosure).where(
                    KapAllDisclosure.company_code == ticker,
                    KapAllDisclosure.title == title,
                ).limit(1)
            )
            disclosure = existing_q2.scalar_one_or_none()

        # is_bilanco / category — title'a göre otomatik
        title_lower = title.lower()
        is_bilanco_flag = any(k in title_lower for k in [
            "finansal rapor", "bilanço", "bilanco", "finansal tablo",
            "ara dönem finansal", "mali tablo"
        ])
        category_inferred = "Bilanço/Finansal Rapor" if is_bilanco_flag else None

        if disclosure:
            result["steps"].append(f"DB'de mevcut id={disclosure.id}")
            # Mevcut kayitta is_bilanco set edilmemisse simdi yap
            if is_bilanco_flag and not disclosure.is_bilanco:
                disclosure.is_bilanco = True
                if category_inferred and not disclosure.category:
                    disclosure.category = category_inferred
                result["steps"].append("is_bilanco=True olarak guncellendi")
        else:
            disclosure = KapAllDisclosure(
                company_code=ticker,
                title=title,
                body=body_text or "",
                kap_url=kap_url,
                source="manual_admin",
                published_at=_dt.now(_tz.utc),
                is_bilanco=is_bilanco_flag,
                category=category_inferred,
            )
            db.add(disclosure)
            await db.flush()
            result["steps"].append(f"DB'ye eklendi id={disclosure.id} is_bilanco={is_bilanco_flag}")

        # 3. AI analiz (henuz yapilmadiysa)
        if not disclosure.ai_analyzed_at:
            try:
                ai_result = await analyze_disclosure(
                    company_code=disclosure.company_code,
                    title=disclosure.title,
                    body=disclosure.body or "",
                    is_bilanco=bool(disclosure.is_bilanco),
                )
                if ai_result:
                    disclosure.ai_sentiment = ai_result.get("sentiment")
                    disclosure.ai_impact_score = ai_result.get("impact_score")
                    disclosure.ai_summary = ai_result.get("summary")
                    disclosure.ai_analyzed_at = _dt.now(_tz.utc)
                result["steps"].append(
                    f"AI analiz: sentiment={disclosure.ai_sentiment}, score={disclosure.ai_impact_score}"
                )
            except Exception as e:
                result["steps"].append(f"AI hata: {str(e)[:200]}")
        else:
            result["steps"].append(
                f"AI mevcut: sentiment={disclosure.ai_sentiment}, score={disclosure.ai_impact_score}"
            )

        await db.commit()

        # 4. Bildirim
        if force_send or not disclosure.ai_analyzed_at:
            notif = NotificationService(db)
            try:
                sent = await notif.notify_kap_watchlist(disclosure)
                result["steps"].append(f"Push bildirim: {sent} kullaniciya gitti")
            except Exception as e:
                result["steps"].append(f"Push hata: {str(e)[:200]}")

    return result


@app.post("/api/v1/admin/import-temel-analiz")
@limiter.limit("30/minute")
async def admin_import_temel_analiz(request: Request, payload: dict = Body(...)):
    """Yerel Python sync scripti her 2 saatte cagiriyor.

    Body: {
      "admin_password": "...",
      "rows": [
        {"ticker":"TUPRS","sektor":"PETROL","dolasim_lot":250000000,"ozsermaye":85e9,
         "yat_fon_oran":2.5,"emeklilik_fon_oran":1.8,"piyasa_degeri":650e9,
         "defter_degeri":340.5,"fk":12.4,"pddd":1.92,"fd_favok":7.3,"pd_efk":11.2,
         "ihracat_yuzdesi":85.0},
         ...
      ]
    }
    Eksik alanlar atlanir (None). Mevcut ticker upsert.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="rows list olmali")

    from app.models.temel_analiz import TemelAnaliz
    from app.database import async_session as _async_session

    inserted = 0
    updated = 0
    errors: list[str] = []
    # KALDIRILDI (BIST lisans): dolasim_lot, piyasa_degeri, fk
    fields = [
        "sektor", "ozsermaye", "yat_fon_oran", "emeklilik_fon_oran",
        "defter_degeri", "pddd", "fd_favok", "pd_efk",
        "ihracat_yuzdesi",
    ]

    async with _async_session() as db:
        for r in rows:
            t = (r.get("ticker") or "").strip().upper()
            if not t or len(t) > 10:
                continue
            try:
                existing = (await db.execute(
                    select(TemelAnaliz).where(TemelAnaliz.ticker == t)
                )).scalar_one_or_none()
                if existing:
                    for f in fields:
                        v = r.get(f)
                        if v is not None and v != "":
                            setattr(existing, f, v)
                    updated += 1
                else:
                    new_row = TemelAnaliz(ticker=t, **{f: r.get(f) for f in fields if r.get(f) is not None and r.get(f) != ""})
                    db.add(new_row)
                    inserted += 1
            except Exception as e:
                errors.append(f"{t}: {str(e)[:100]}")
        await db.commit()

    return {
        "status": "ok",
        "received": len(rows),
        "inserted": inserted,
        "updated": updated,
        "errors": errors[:10],
    }


@app.put("/api/v1/users/{device_id}/portfolio-tickers")
async def update_portfolio_tickers(
    device_id: str,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Frontend portföyündeki hisse kodlarını backend'e sync eder.

    Body: { "tickers": ["TUPRS", "ASELS", ...] }

    Bu liste KAP haber bildirimleri için kullanılır — kullanıcının
    portföyündeki hisseler için de watchlist gibi push bildirimi gider.
    """
    raw = payload.get("tickers", [])
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="tickers list olmali")
    # Sanitize — sadece A-Z0-9, max 10 chr
    cleaned = []
    for t in raw:
        if isinstance(t, str):
            t = t.strip().upper()
            if 2 <= len(t) <= 10 and t.isalnum():
                cleaned.append(t)
    cleaned = list(dict.fromkeys(cleaned))  # uniq, sıra korunur

    user_result = await db.execute(select(User).where(User.device_id == device_id))
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanıcı bulunamadı")

    user.portfolio_tickers = ",".join(cleaned) if cleaned else None
    await db.flush()
    return {"success": True, "count": len(cleaned), "tickers": cleaned}


@app.post("/api/v1/users/{device_id}/kap-watchlist/trim")
async def trim_watchlist(
    device_id: str,
    db: AsyncSession = Depends(get_db),
):
    """VIP/Diamond → Free geçişinde watchlist'i FREE limiti ile sınırla.

    Tier limitleri (2026-05 sonrası):
      Free: 5 — PRO: 25 — Diamond: 100
    En eski eklenen hisseler korunur, fazlası silinir.
    Diamond/PRO aktifse trim yapılmaz.
    """
    FREE_LIMIT = 5

    # Tier kontrolü — hâlâ aktif aboneliği varsa trim yapma
    user_result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    sub_result = await db.execute(
        select(UserSubscription).where(
            and_(
                UserSubscription.user_id == user.id,
                or_(
                    UserSubscription.is_active == True,
                    UserSubscription.expires_at > datetime.utcnow(),
                ),
            )
        )
    )
    has_paid = False
    for sub in sub_result.scalars().all():
        pkg = (sub.package or "").lower()
        if pkg == "ana_yildiz" or "diamond" in pkg or "bilanco_temettu" in pkg:
            has_paid = True
            break
    if has_paid:
        return {"trimmed": False, "remaining": -1}

    # Mevcut watchlist sayısı
    count_result = await db.execute(
        select(func.count(UserWatchlist.id)).where(
            UserWatchlist.device_id == device_id
        )
    )
    current_count = count_result.scalar() or 0

    if current_count <= FREE_LIMIT:
        return {"trimmed": False, "remaining": current_count}

    # En eski FREE_LIMIT kadarını koru, gerisini sil
    # Korunacak hisselerin ID'lerini al (en eski eklenenler)
    keep_result = await db.execute(
        select(UserWatchlist.id)
        .where(UserWatchlist.device_id == device_id)
        .order_by(UserWatchlist.created_at.asc())
        .limit(FREE_LIMIT)
    )
    keep_ids = [row[0] for row in keep_result.fetchall()]

    # Korunmayacakları sil
    if keep_ids:
        await db.execute(
            delete(UserWatchlist).where(
                UserWatchlist.device_id == device_id,
                UserWatchlist.id.notin_(keep_ids),
            )
        )

    deleted_count = current_count - FREE_LIMIT
    return {"trimmed": True, "deleted": deleted_count, "remaining": FREE_LIMIT}


# ============================================
# BILDIRIM MERKEZI — Notification Log
# ============================================

@app.get("/api/v1/users/{device_id}/notifications")
@limiter.limit("30/minute")
async def get_notification_log(
    request: Request,
    device_id: str,
    category: Optional[str] = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin bildirim gecmisini getir.

    Kayitlar her cumartesi 23:50'de sifirlanir.

    Query params:
    - category: kap_watchlist, kap_news, ipo, system (opsiyonel — filtre)
    - limit: max kayit sayisi (varsayilan 50)
    """
    from app.models.notification_log import NotificationLog

    query = (
        select(NotificationLog)
        .where(
            NotificationLog.device_id == device_id,
        )
    )
    if category and category in ("kap_watchlist", "kap_news", "ipo", "system"):
        query = query.where(NotificationLog.category == category)

    query = query.order_by(NotificationLog.created_at.desc()).limit(min(limit, 100))

    result = await db.execute(query)
    logs = result.scalars().all()

    return [
        {
            "id": log.id,
            "title": log.title,
            "body": log.body,
            "category": log.category,
            "data_json": log.data_json,
            "is_read": log.is_read,
            "created_at": log.created_at.isoformat() if log.created_at else None,
        }
        for log in logs
    ]


@app.patch("/api/v1/users/{device_id}/notifications/{notif_id}/read")
async def mark_notification_read(
    device_id: str,
    notif_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Bildirimi okundu olarak isaretle."""
    from app.models.notification_log import NotificationLog

    result = await db.execute(
        update(NotificationLog)
        .where(
            NotificationLog.id == notif_id,
            NotificationLog.device_id == device_id,
        )
        .values(is_read=True)
    )
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Bildirim bulunamadi")
    return {"success": True}


@app.patch("/api/v1/users/{device_id}/notifications/read-all")
async def mark_all_notifications_read(
    device_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Tum bildirimleri okundu olarak isaretle."""
    from app.models.notification_log import NotificationLog

    await db.execute(
        update(NotificationLog)
        .where(
            NotificationLog.device_id == device_id,
            NotificationLog.is_read == False,
        )
        .values(is_read=True)
    )
    return {"success": True}


# ============================================
# KAP AI SKOR FILTRESI — Kullanici push tercihi
# ============================================

class KapMinScoreUpdate(BaseModel):
    """Frontend'den gelen minimum AI skor tercihi."""
    kap_min_score: float  # 6.0 = tum pozitifler | 7.0 = Olumlu+ | 8.0 = Cok Olumlu+ | 9.0 = Guclu


@app.patch("/api/v1/users/{device_id}/kap-min-score")
@limiter.limit("10/minute")
async def update_kap_min_score(
    request: Request,
    device_id: str,
    payload: KapMinScoreUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin AI Pozitif filtre tercihi (push + feed display).

    Frontend AIScoreFilterModal "Uygula" basinca cagirilir.
    Sadece skor >= kap_min_score olan KAP pozitifler kullaniciya push olarak
    gonderilir. Daha dusuk skorlular feed'de gozukur ama push gelmez.

    Sinirlar: 6.0 - 10.0 arasi.
    """
    score = payload.kap_min_score
    if score < 6.0:
        score = 6.0  # Min: tum pozitifler
    elif score > 10.0:
        score = 10.0
    score = round(score, 1)

    from app.models.user import User as _User
    user = await db.execute(
        select(_User).where(_User.device_id == device_id)
    )
    user_obj = user.scalar_one_or_none()
    if not user_obj:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    user_obj.kap_min_score = score
    await db.commit()
    return {"success": True, "kap_min_score": score}


@app.get("/api/v1/users/{device_id}/kap-min-score")
@limiter.limit("30/minute")
async def get_kap_min_score(
    request: Request,
    device_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin mevcut AI skor filtre tercihini dondur."""
    from app.models.user import User as _User
    user = await db.execute(
        select(_User).where(_User.device_id == device_id)
    )
    user_obj = user.scalar_one_or_none()
    if not user_obj:
        return {"kap_min_score": 6.0}  # Varsayilan
    return {"kap_min_score": float(user_obj.kap_min_score or 6.0)}


# ============================================
# FEATURE INTEREST — Özellik Talep Kaydı
# ============================================

class FeatureInterestRequest(BaseModel):
    device_id: Optional[str] = None
    feature_name: str


@app.post("/api/v1/feature-interest")
@limiter.limit("10/minute")
async def register_feature_interest(
    request: Request,
    body: FeatureInterestRequest,
    db: AsyncSession = Depends(get_db),
):
    """Kullanıcı özellik talep kaydı oluşturur (talep ölçümü)."""
    feature = body.feature_name.strip().lower()
    device = (body.device_id or "").strip()

    if not feature:
        raise HTTPException(status_code=400, detail="feature_name gerekli.")

    # Aynı device_id + feature için duplicate kontrolü
    if device:
        existing = await db.execute(
            select(FeatureInterest).where(
                FeatureInterest.device_id == device,
                FeatureInterest.feature_name == feature,
            )
        )
        if existing.scalar_one_or_none():
            return {"success": True, "message": "Zaten kayıtlısınız."}

    interest = FeatureInterest(
        device_id=device or None,
        feature_name=feature,
    )
    db.add(interest)
    await db.commit()
    return {"success": True, "message": "Talebiniz kaydedildi!"}


@app.post("/api/v1/admin/feature-interest-stats")
@limiter.limit("10/minute")
async def admin_feature_interest_stats(
    request: Request,
    payload: dict,
    db: AsyncSession = Depends(get_db),
):
    """Admin: Özellik talep istatistiklerini döner."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    result = await db.execute(
        select(
            FeatureInterest.feature_name,
            func.count(FeatureInterest.id),
        ).group_by(FeatureInterest.feature_name)
    )
    stats = {row[0]: row[1] for row in result.all()}
    return {"stats": stats, "total": sum(stats.values())}


# -------------------------------------------------------
# Admin: KAP All Scraper Manuel Trigger
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-watchlist-report")
@limiter.limit("5/minute")
async def admin_trigger_watchlist_report(request: Request, payload: dict):
    """Admin: Haftalık watchlist raporu hemen gönder."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.scheduler import weekly_watchlist_report
        await weekly_watchlist_report()
        return {"status": "ok", "message": "Watchlist raporu Telegram'a gönderildi"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/trigger-kap-scrape")
@limiter.limit("5/minute")
async def admin_trigger_kap_scrape(request: Request, payload: dict):
    """Admin: KAP scraper'i manuel tetikle (Uzmanpara + KAP.org.tr AI analiz)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    try:
        from app.scheduler import kap_uzmanpara_quick_job
        await kap_uzmanpara_quick_job()
        return {"status": "ok", "message": "KAP scrape tamamlandi (Uzmanpara)"}
    except Exception as e:
        import traceback
        resp = {"status": "error", "message": str(e)[:500]}
        if not settings.is_production:
            resp["traceback"] = traceback.format_exc()[-1000:]
        return resp


# -------------------------------------------------------
# Admin: Ticker / Trading Date Bildirim + Tweet Trigger
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-ipo-event")
@limiter.limit("5/minute")
async def admin_trigger_ipo_event(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: Belirli bir IPO icin ticker_assigned veya trading_date_detected tweet + bildirim gonder."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ipo_id = payload.get("ipo_id")
    event_type = payload.get("event_type")  # "ticker_assigned" veya "trading_date_detected"

    if not ipo_id or event_type not in ("ticker_assigned", "trading_date_detected", "update_field"):
        raise HTTPException(status_code=400, detail="ipo_id ve event_type (ticker_assigned|trading_date_detected|update_field) gerekli")

    from app.models.ipo import IPO
    result = await db.execute(select(IPO).where(IPO.id == int(ipo_id)))
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="IPO bulunamadi")

    results = {"tweet": False, "notification": 0}

    if event_type == "ticker_assigned":
        try:
            from app.services.twitter_service import tweet_ticker_assigned
            results["tweet"] = tweet_ticker_assigned(ipo)
        except Exception as e:
            results["tweet_error"] = str(e)
        try:
            from app.services.notification import NotificationService
            notif_svc = NotificationService(db)
            results["notification"] = await notif_svc.notify_ticker_assigned(ipo)
        except Exception as e:
            results["notif_error"] = str(e)
    elif event_type == "trading_date_detected":
        try:
            from app.services.twitter_service import tweet_trading_date_detected
            results["tweet"] = tweet_trading_date_detected(ipo)
        except Exception as e:
            results["tweet_error"] = str(e)
        try:
            from app.services.notification import NotificationService
            notif_svc = NotificationService(db)
            results["notification"] = await notif_svc.notify_trading_date_detected(ipo)
        except Exception as e:
            results["notif_error"] = str(e)

    if event_type == "update_field":
        field = payload.get("field")
        value = payload.get("value")
        _ALLOWED = {"prospectus_url", "ticker", "trading_start", "subscription_start", "subscription_end", "ipo_price", "market_segment", "trading_day_count"}
        if field not in _ALLOWED:
            raise HTTPException(status_code=400, detail=f"field {_ALLOWED} icinden olmali")
        setattr(ipo, field, value)
        ipo.updated_at = __import__("datetime").datetime.utcnow()
        await db.commit()
        results["updated"] = {field: value}

    return {"status": "ok", "ipo": ipo.company_name, "ticker": ipo.ticker, "event": event_type, "results": results}


# -------------------------------------------------------
# Admin: Eksik Gün Düzeltme (ceiling_tracks gün numarası kaydırma)
# -------------------------------------------------------

@app.post("/api/v1/admin/fix-missing-day")
@limiter.limit("10/minute")
async def admin_fix_missing_day(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Atlanmis gun numarasini duzelt — missing_day'den sonraki gunleri 1 geri kaydir."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ipo_id = payload.get("ipo_id")
    missing_day = payload.get("missing_day")  # Atlanmis gun numarasi

    if not ipo_id or not missing_day:
        raise HTTPException(status_code=400, detail="ipo_id ve missing_day gerekli")

    from app.models.ipo import IPOCeilingTrack
    # missing_day'den buyuk tum gunleri 1 azalt
    result = await db.execute(
        select(IPOCeilingTrack).where(
            IPOCeilingTrack.ipo_id == int(ipo_id),
            IPOCeilingTrack.trading_day > int(missing_day),
        ).order_by(IPOCeilingTrack.trading_day.desc())
    )
    tracks = result.scalars().all()

    updated = 0
    for track in tracks:
        track.trading_day = track.trading_day - 1
        updated += 1

    # IPO trading_day_count = kayitli max trading_day ile esitle
    from app.models.ipo import IPO
    ipo_result = await db.execute(select(IPO).where(IPO.id == int(ipo_id)))
    ipo = ipo_result.scalar_one_or_none()
    if ipo:
        # Guncellenmis kayitlardaki max trading_day'i bul
        max_result = await db.execute(
            select(IPOCeilingTrack.trading_day)
            .where(IPOCeilingTrack.ipo_id == int(ipo_id))
            .order_by(IPOCeilingTrack.trading_day.desc())
            .limit(1)
        )
        max_day = max_result.scalar_one_or_none() or 0
        ipo.trading_day_count = max_day

    await db.commit()

    return {"status": "ok", "ipo_id": ipo_id, "missing_day": missing_day, "tracks_updated": updated}


# -------------------------------------------------------
# Admin: Resmi Gazete Manuel Trigger
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-resmi-gazete")
@limiter.limit("3/minute")
async def admin_trigger_resmi_gazete(request: Request, payload: dict):
    """Admin: Resmi Gazete scraper'ı manuel tetikle."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    try:
        from app.scrapers.resmi_gazete_scraper import check_resmi_gazete
        await check_resmi_gazete()
        return {"status": "ok", "message": "Resmi Gazete tarama tamamlandi"}
    except Exception as e:
        import traceback
        resp = {"status": "error", "message": str(e)[:500]}
        if not settings.is_production:
            resp["traceback"] = traceback.format_exc()[-1000:]
        return resp


@app.post("/api/v1/admin/test-rg-pdf")
@limiter.limit("3/minute")
async def admin_test_rg_pdf(request: Request, payload: dict):
    """Admin: Belirli bir RG PDF'ini OCR ile oku — diagnostic."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    pdf_url = payload.get("url", "https://www.resmigazete.gov.tr/eskiler/2026/03/20260318-3.pdf")
    diag = {"ocr_available": False, "poppler_available": False, "pymupdf_available": False}

    # Diagnostic: check tools
    try:
        import pytesseract
        ver = pytesseract.get_tesseract_version()
        diag["ocr_available"] = True
        diag["tesseract_version"] = str(ver)
    except Exception as e:
        diag["tesseract_error"] = str(e)[:200]

    try:
        from pdf2image import convert_from_bytes
        diag["poppler_available"] = True
    except Exception as e:
        diag["poppler_error"] = str(e)[:200]

    try:
        import fitz
        diag["pymupdf_available"] = True
        diag["pymupdf_version"] = fitz.version[0] if hasattr(fitz, 'version') else "?"
    except Exception as e:
        diag["pymupdf_error"] = str(e)[:200]

    try:
        from app.scrapers.resmi_gazete_scraper import ResmiGazeteScraper
        scraper = ResmiGazeteScraper()
        try:
            text = await scraper.download_pdf_text(pdf_url)
        finally:
            await scraper.close()

        if text:
            return {
                "status": "ok",
                "url": pdf_url,
                "text_length": len(text),
                "text_preview": text[:1000],
                "diagnostic": diag,
            }
        else:
            return {"status": "empty", "url": pdf_url, "message": "PDF text extraction failed", "diagnostic": diag}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500], "diagnostic": diag}


# -------------------------------------------------------
# Admin: KAP AI Re-Analyze (NULL summary kayitlari)
# -------------------------------------------------------

# -------------------------------------------------------
# Admin: VİOP Seans Bildirim Trigger
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-viop-notification")
@limiter.limit("10/minute")
async def admin_trigger_viop_notification(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Admin: VİOP seans bildirimi gonder.

    Body:
        admin_password: str
        session_type: str  (opening, closing, flash)
        summary: str  (bildirim mesaji)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    session_type = payload.get("session_type", "opening")
    summary = payload.get("summary", "")
    price = payload.get("price", 0)
    change_pct = payload.get("change_pct", 0)
    night_diff = payload.get("night_diff")

    if session_type not in ("opening", "closing", "flash", "progress"):
        raise HTTPException(status_code=400, detail="Gecersiz session_type. Gecerli: opening, closing, flash, progress")

    from app.services.notification import NotificationService
    notif_svc = NotificationService(db)
    sent = await notif_svc.notify_viop_session(session_type, summary, price=price, change_pct=change_pct, night_diff=night_diff)

    # Opsiyonel: tweet_text gelirse VIOP sayfasinda gozuksun diye PendingTweet'e
    # 'sent' kaydi olustur. Tweet ban suresince app feed'i bos kalmasin diye.
    tweet_text = payload.get("tweet_text")
    pending_tweet_id = None
    if tweet_text and isinstance(tweet_text, str) and ("VİOP" in tweet_text or "VIOP" in tweet_text.upper()):
        try:
            from app.models.pending_tweet import PendingTweet
            from datetime import datetime as _dt_now
            pt = PendingTweet(
                text=tweet_text,
                source=f"viop_local_{session_type}",
                status="sent",
                sent_at=_dt_now.utcnow(),
            )
            db.add(pt)
            await db.commit()
            await db.refresh(pt)
            pending_tweet_id = pt.id
        except Exception as _pt_err:
            import logging
            logging.getLogger(__name__).warning("VIOP pending_tweet kaydi hatasi: %s", _pt_err)
            try:
                await db.rollback()
            except Exception:
                pass

    return {"status": "ok", "sent": sent, "session_type": session_type, "pending_tweet_id": pending_tweet_id}


@app.post("/api/v1/admin/delete-orphan-dividends")
@limiter.limit("5/minute")
async def admin_delete_orphan_dividends(request: Request, payload: dict = Body(...)):
    """Admin: dividend_calendar'da kap_url'siz, gross'siz hayalet kayitlari sil.

    Kriter: tum kap_url'lar NULL VE gross_amount_per_share NULL/0 VE status='ykk_alindi'
    Yani: title match olmus ama KAP body cekilemediginden dogrulanmamis.

    Body: { admin_password, tickers: ["SMRVA","TRILC",...] (opsiyonel — yoksa hepsi) }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    tickers = payload.get("tickers")
    try:
      async with async_session() as db:
        if tickers:
            res = await db.execute(sa_text("""
                DELETE FROM dividend_calendar
                WHERE ticker = ANY(:tks)
                  AND ykk_kap_url IS NULL
                  AND general_assembly_kap_url IS NULL
                  AND payment_kap_url IS NULL
                  AND COALESCE(gross_amount_per_share, 0) = 0
                  AND status IN ('ykk_alindi', 'tarih_belli')
                RETURNING id, ticker
            """), {"tks": tickers})
        else:
            res = await db.execute(sa_text("""
                DELETE FROM dividend_calendar
                WHERE ykk_kap_url IS NULL
                  AND general_assembly_kap_url IS NULL
                  AND payment_kap_url IS NULL
                  AND COALESCE(gross_amount_per_share, 0) = 0
                  AND status IN ('ykk_alindi', 'tarih_belli')
                  AND created_at > NOW() - INTERVAL '30 days'
                RETURNING id, ticker
            """))
        deleted = res.fetchall()
        await db.commit()
      return {"deleted_count": len(deleted), "deleted": [{"id": r[0], "ticker": r[1]} for r in deleted[:50]]}
    except Exception as e:
      import traceback
      return {"error": str(e)[:500], "trace": traceback.format_exc()[:1500]}


@app.post("/api/v1/admin/cleanup-fake-dividends")
@limiter.limit("5/minute")
async def admin_cleanup_fake_dividends(request: Request, payload: dict = Body(...)):
    """Admin: dividend_calendar tablosunda bedelsiz/sermaye artırımı KAP'larından
    yanlislikla olusmus kayitlari sil.

    Kriter: kap_url body'sinde 'bedelsiz sermaye artirim' veya 'kaydilesmis pay'
    geciyorsa ve 'pay basina brut temettu' GECMIYORSA → temettu degil.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import select, text as sa_text
    from app.database import async_session
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.utils.tr_text import lower_tr
    deleted = []
    skipped = []
    try:
      async with async_session() as db:
        # Son 30 gun, ai_summary olmayan veya kuskulu kayitlari kontrol et
        rows_q = await db.execute(sa_text("""
            SELECT id, ticker,
                   COALESCE(ykk_kap_url, general_assembly_kap_url, payment_kap_url) AS kap_url,
                   status, gross_amount_per_share
            FROM dividend_calendar
            WHERE COALESCE(ykk_kap_url, general_assembly_kap_url, payment_kap_url) IS NOT NULL
              AND status NOT IN ('tamamlandi', 'odeniyor')
              AND created_at > NOW() - INTERVAL '30 days'
              AND COALESCE(gross_amount_per_share, 0) = 0
            ORDER BY id DESC LIMIT 200
        """))
        rows = rows_q.fetchall()
        for row in rows:
            div_id, ticker, kap_url, status, gross = row[0], row[1], row[2], row[3], row[4]
            try:
                disc = await fetch_kap_disclosure(kap_url)
                body = (disc or {}).get("full_text") or ""
                if not body:
                    skipped.append({"id": div_id, "reason": "no_body"})
                    continue
                b = lower_tr(body)
                is_capital = any(s in b for s in [
                    "bedelsiz sermaye artirim", "bedelsiz sermaye artırım",
                    "kaydilesmis pay senetlerinin artirim", "kaydileşmiş pay senetlerinin artırım",
                    "bonus issue",
                ])
                is_div = any(s in b for s in [
                    "pay basina brut temettu", "pay başına brüt temettü",
                    "gross dividend payment per share",
                    "kar payi dagit", "kar payı dağıt", "kâr payı dağıt",
                    "temettu dagit", "temettü dağıt",
                ])
                if is_capital and not is_div:
                    await db.execute(sa_text("DELETE FROM dividend_calendar WHERE id=:id"), {"id": div_id})
                    deleted.append({"id": div_id, "ticker": ticker, "kap": kap_url})
                else:
                    skipped.append({"id": div_id, "ticker": ticker, "is_div": is_div, "is_capital": is_capital})
            except Exception as e:
                skipped.append({"id": div_id, "error": str(e)[:200]})
        await db.commit()
      return {"deleted_count": len(deleted), "deleted": deleted[:30], "skipped_count": len(skipped)}
    except Exception as e:
      import traceback
      return {"error": str(e)[:500], "trace": traceback.format_exc()[:1500]}


@app.post("/api/v1/admin/dividend-backfill")
@limiter.limit("3/minute")
async def admin_dividend_backfill(request: Request, payload: dict = Body(...)):
    """Admin: kap_all_disclosures'tan Kar Payi/Temettu haberlerini tarayip
    dividend_calendar'a tekrar isle (backfill).

    BORSK bug fix sonrasi 2 haftalik kayip temettu kayitlarini geri getirmek icin.

    Body: { "admin_password": "...", "days": 14 (default) }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.dividend_calendar_processor import (
        is_dividend, process_kap_disclosure as div_process,
    )
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    days = int(payload.get("days", 14))
    cutoff = _dt.now(_tz.utc) - _td(days=days)
    processed = 0
    routed = 0
    errors = []

    async with async_session() as db:
        stmt = select(KapAllDisclosure).where(
            KapAllDisclosure.published_at >= cutoff,
        ).order_by(KapAllDisclosure.published_at.asc())
        rows = (await db.execute(stmt)).scalars().all()

        for row in rows:
            title = row.title or ""
            body = row.ai_summary or row.body or ""
            if not is_dividend(title, body):
                continue
            processed += 1
            try:
                # Body kısa olabilir → KAP'tan tam içerik çek
                body_for_div = body
                if (not body_for_div or len(body_for_div) < 200) and row.kap_url:
                    try:
                        disc = await fetch_kap_disclosure(row.kap_url)
                        if disc and disc.get("full_text"):
                            body_for_div = disc["full_text"]
                    except Exception:
                        pass
                async with db.begin_nested():
                    await div_process(
                        db, disclosure_id=row.id, ticker=row.company_code,
                        company_name=None, title=title, body=body_for_div,
                        kap_url=row.kap_url, published_at=row.published_at,
                    )
                routed += 1
            except Exception as e:
                errors.append(f"{row.company_code}: {str(e)[:100]}")
        try:
            await db.commit()
        except Exception as e:
            errors.append(f"commit: {str(e)[:100]}")

    return {
        "status": "ok",
        "scanned": len(rows),
        "dividend_candidates": processed,
        "successfully_routed": routed,
        "errors": errors[:20],
    }


@app.post("/api/v1/admin/kap-manual-insert")
@limiter.limit("10/minute")
async def admin_kap_manual_insert(request: Request, payload: dict = Body(...)):
    """Admin: bir KAP URL'sinden tek bir kap_all_disclosures kaydi MANUEL OLUSTUR.

    DB'de kayit yoksa KAP.org.tr'den icerik ceker, AI analiz yapip INSERT eder.
    BORSK Kar Payi (id=12557 bug fix) gibi kayip kayitlari geri eklemek icin.

    Body: { "admin_password": "...", "kap_url": "...", "ticker": "BORSK",
            "title": "...", "published_at": "2026-05-26T17:58:36" (opsiyonel) }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.scrapers.kap_all_scraper import fetch_kap_page_content, _infer_category
    from app.services.kap_all_analyzer import analyze_disclosure
    from app.database import async_session
    from datetime import datetime as _dt, timezone as _tz

    kap_url = payload.get("kap_url", "").strip()
    ticker = (payload.get("ticker") or "").strip().upper()
    title = payload.get("title") or ""
    pub_str = payload.get("published_at")
    if not kap_url or not ticker:
        return {"error": "kap_url + ticker zorunlu"}

    pub_dt = None
    if pub_str:
        try:
            pub_dt = _dt.fromisoformat(pub_str.replace("Z", "+00:00"))
        except Exception:
            pub_dt = _dt.now(_tz.utc)
    else:
        pub_dt = _dt.now(_tz.utc)

    async with async_session() as db:
        # Duplicate check
        existing = await db.execute(
            select(KapAllDisclosure).where(
                KapAllDisclosure.kap_url == kap_url,
                KapAllDisclosure.company_code == ticker,
            ).limit(1)
        )
        if existing.scalar_one_or_none():
            return {"status": "already_exists", "kap_url": kap_url, "ticker": ticker}

        body = await fetch_kap_page_content(kap_url)
        if not body or len(body) < 50:
            return {"error": "kap fetch bos"}

        ai = await analyze_disclosure(company_code=ticker, title=title, body=body)
        if not ai:
            return {"error": "ai analyze fail"}

        category = _infer_category(title)
        rec = KapAllDisclosure(
            company_code=ticker,
            title=title or "Bildirim",
            body=ai.get("summary"),
            category=category,
            is_bilanco=category in ("Bilanço/Finansal Rapor", "Faaliyet Raporu"),
            kap_url=kap_url,
            source="manual_insert",
            published_at=pub_dt,
            ai_sentiment=ai.get("sentiment"),
            ai_impact_score=ai.get("impact_score"),
            ai_summary=ai.get("summary"),
            ai_analyzed_at=_dt.now(_tz.utc),
        )
        db.add(rec)
        await db.commit()
        await db.refresh(rec)
        return {
            "status": "ok", "id": rec.id, "ticker": ticker,
            "score": rec.ai_impact_score, "sentiment": rec.ai_sentiment,
            "summary": (rec.ai_summary or "")[:120],
        }


@app.post("/api/v1/admin/kap-force-reanalyze")
@limiter.limit("10/minute")
async def admin_kap_force_reanalyze(request: Request, payload: dict = Body(...)):
    """Admin: spesifik kap_url(s) icin AI ozeti zorla yeniden uret.

    Body: { "admin_password": "...", "kap_urls": ["...", ...] | "kap_url": "..." }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import select
    from app.database import async_session
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.services.kap_all_analyzer import analyze_disclosure
    urls = payload.get("kap_urls") or ([payload.get("kap_url")] if payload.get("kap_url") else [])
    urls = [u for u in urls if u]
    if not urls:
        return {"error": "kap_url(s) gerekli"}
    results = []
    async with async_session() as db:
        for url in urls[:30]:
            stmt = select(KapAllDisclosure).where(KapAllDisclosure.kap_url == url).limit(1)
            row = (await db.execute(stmt)).scalar_one_or_none()
            if not row:
                results.append({"url": url, "status": "not_found"})
                continue
            try:
                ai = await analyze_disclosure(
                    company_code=row.company_code or "",
                    title=row.title or "",
                    body=row.body or row.title or "",
                )
                if ai:
                    row.ai_summary = ai.get("summary")
                    row.ai_sentiment = ai.get("sentiment")
                    row.ai_impact_score = ai.get("impact_score")
                    results.append({"url": url, "ticker": row.company_code, "summary": (ai.get("summary") or "")[:120]})
                else:
                    results.append({"url": url, "status": "ai_failed"})
            except Exception as e:
                results.append({"url": url, "error": str(e)[:200]})
        await db.commit()
    return {"results": results}


@app.post("/api/v1/admin/kap-refetch-reanalyze")
@limiter.limit("5/minute")
async def admin_kap_refetch_reanalyze(request: Request, payload: dict = Body(...)):
    """Admin: Belirli ticker(lar) icin son N saatin KAP kayitlarini
    KAP.org.tr'den SIFIRDAN cekilip AI ile yeniden analiz eder.

    Body: { "admin_password": "...", "tickers": ["UCAYM","ERSU"], "hours": 3 }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    tickers = [t.upper().strip() for t in (payload.get("tickers") or [])]
    hours = int(payload.get("hours", 3))
    if not tickers:
        return {"error": "tickers gerekli"}

    from sqlalchemy import select as _sel
    from app.database import async_session as _as
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.scrapers.kap_all_scraper import fetch_kap_page_content
    from app.services.kap_all_analyzer import analyze_disclosure
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td

    cutoff = _dt.now(_tz.utc) - _td(hours=hours)
    results = []

    async with _as() as db:
        stmt = _sel(KapAllDisclosure).where(
            KapAllDisclosure.company_code.in_(tickers),
            KapAllDisclosure.published_at >= cutoff,
        ).order_by(KapAllDisclosure.published_at.desc()).limit(20)
        rows = (await db.execute(stmt)).scalars().all()

        for row in rows:
            item = {"id": row.id, "ticker": row.company_code, "title": row.title, "kap_url": row.kap_url}
            if not row.kap_url:
                item["status"] = "kap_url_yok"
                results.append(item)
                continue
            try:
                # KAP.org.tr'den taze icerik cek
                fresh_body = await fetch_kap_page_content(row.kap_url)
                if not fresh_body or len(fresh_body) < 50:
                    item["status"] = "kap_fetch_bos"
                    results.append(item)
                    continue
                row.body = fresh_body
                # AI yeniden analiz
                ai = await analyze_disclosure(
                    company_code=row.company_code or "",
                    title=row.title or "",
                    body=fresh_body,
                )
                if ai:
                    row.ai_summary = ai.get("summary")
                    row.ai_sentiment = ai.get("sentiment")
                    row.ai_impact_score = ai.get("impact_score")
                    from datetime import datetime as _dtnow, timezone as _tzutc
                    row.ai_analyzed_at = _dtnow.now(_tzutc.utc)
                    item["status"] = "ok"
                    item["score"] = ai.get("impact_score")
                    item["summary_preview"] = (ai.get("summary") or "")[:120]
                else:
                    item["status"] = "ai_failed"
            except Exception as e:
                item["status"] = f"hata: {str(e)[:150]}"
            results.append(item)

        await db.commit()

    return {"total": len(rows), "results": results}


@app.post("/api/v1/admin/kap-reanalyze")
@limiter.limit("3/minute")
async def admin_kap_reanalyze(request: Request, payload: dict = Body(...)):
    """Admin: ai_summary NULL olan KAP kayitlarini yeniden AI ile analiz et.

    Body:
        admin_password: str
        hours: int (kac saatlik kayitlar, default 48)
        limit: int (max kac kayit, default 50)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    hours = min(int(payload.get("hours", 48)), 168)  # max 7 gun
    max_records = min(int(payload.get("limit", 50)), 100)  # max 100

    try:
        from datetime import datetime, timezone, timedelta
        from sqlalchemy import select, and_
        from app.database import async_session
        from app.models.kap_all_disclosure import KapAllDisclosure
        from app.services.kap_all_analyzer import analyze_disclosure

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        reanalyzed = 0
        failed = 0
        results = []

        async with async_session() as db:
            # ai_summary NULL ve bilanco olmayan, son X saatteki kayitlar
            stmt = (
                select(KapAllDisclosure)
                .where(
                    and_(
                        KapAllDisclosure.ai_summary.is_(None),
                        KapAllDisclosure.is_bilanco == False,
                        KapAllDisclosure.created_at >= cutoff,
                    )
                )
                .order_by(KapAllDisclosure.created_at.desc())
                .limit(max_records)
            )
            rows = (await db.execute(stmt)).scalars().all()

            for record in rows:
                try:
                    ai_result = await analyze_disclosure(
                        company_code=record.company_code,
                        title=record.title,
                        body=record.body or record.title,
                        is_bilanco=record.is_bilanco,
                    )
                    summary = ai_result.get("summary")
                    if summary:
                        record.ai_sentiment = ai_result.get("sentiment", record.ai_sentiment)
                        record.ai_impact_score = ai_result.get("impact_score", record.ai_impact_score)
                        record.ai_summary = summary
                        record.ai_analyzed_at = datetime.now(timezone.utc)
                        reanalyzed += 1
                        results.append({
                            "id": record.id,
                            "company_code": record.company_code,
                            "title": record.title[:60],
                            "sentiment": ai_result.get("sentiment"),
                            "score": ai_result.get("impact_score"),
                            "summary": summary[:100] if summary else None,
                        })
                    else:
                        failed += 1
                except Exception as e:
                    failed += 1
                    logger.warning("KAP re-analyze hatasi (id=%d): %s", record.id, e)

            await db.commit()

        return {
            "status": "ok",
            "total_found": len(rows),
            "reanalyzed": reanalyzed,
            "failed": failed,
            "results": results,
        }
    except Exception as e:
        import traceback
        resp = {"status": "error", "message": str(e)[:500]}
        if not settings.is_production:
            resp["traceback"] = traceback.format_exc()[-1000:]
        return resp


# -------------------------------------------------------
# Admin: KAP Disclosure Manuel AI Skor Guncelleme
# -------------------------------------------------------

@app.post("/api/v1/admin/kap-disclosure-reset")
@limiter.limit("5/minute")
async def admin_kap_disclosure_reset(request: Request, payload: dict = Body(...)):
    """Admin: Belirli KAP kayitlarinin AI skorlarini manuel guncelle (AI kredisi harcamadan).

    Body:
        admin_password: str
        ids: list[int] — guncellenecek kayit ID'leri
        sentiment: str (default "Nötr") — "Olumlu" | "Olumsuz" | "Nötr"
        impact_score: float (default 5.0) — 1.0-10.0
        summary: str | None (default None) — AI ozet (null = temizle)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    ids = payload.get("ids", [])
    if not ids or not isinstance(ids, list):
        raise HTTPException(status_code=400, detail="ids listesi gerekli")
    if len(ids) > 50:
        raise HTTPException(status_code=400, detail="Tek seferde max 50 kayit")

    sentiment = payload.get("sentiment", "Nötr")
    if sentiment not in ("Olumlu", "Olumsuz", "Nötr"):
        sentiment = "Nötr"
    impact_score = float(payload.get("impact_score", 5.0))
    if not (1.0 <= impact_score <= 10.0):
        impact_score = 5.0
    summary = payload.get("summary")  # None = temizle

    try:
        from datetime import datetime, timezone
        from sqlalchemy import select
        from app.database import async_session
        from app.models.kap_all_disclosure import KapAllDisclosure

        updated = []
        async with async_session() as db:
            stmt = select(KapAllDisclosure).where(KapAllDisclosure.id.in_(ids))
            rows = (await db.execute(stmt)).scalars().all()

            for record in rows:
                record.ai_sentiment = sentiment
                record.ai_impact_score = impact_score
                record.ai_summary = summary
                record.ai_analyzed_at = datetime.now(timezone.utc)
                updated.append({
                    "id": record.id,
                    "company_code": record.company_code,
                    "title": record.title[:60] if record.title else "",
                })

            await db.commit()

        return {
            "status": "ok",
            "updated_count": len(updated),
            "sentiment": sentiment,
            "impact_score": impact_score,
            "summary": summary[:100] if summary else None,
            "records": updated,
        }
    except Exception as e:
        import traceback
        resp = {"status": "error", "message": str(e)[:500]}
        if not settings.is_production:
            resp["traceback"] = traceback.format_exc()[-1000:]
        return resp


# ================================================================
# HABER TARAMA ADMIN ENDPOINTLERI
# ================================================================

@app.post("/api/v1/admin/news-scan")
async def admin_trigger_news_scan(body: dict, background_tasks: BackgroundTasks):
    """Manuel haber tarama tetikle — background'da calisir, hemen donmez."""
    settings = get_settings()
    if body.get("admin_password") != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    auto_tweet = body.get("auto_tweet", False)

    async def _run_scan():
        from app.services.news_scanner_service import scan_news, process_important_news
        try:
            important = await scan_news()
            if important:
                await process_important_news(important, auto_tweet=auto_tweet)
        except Exception as e:
            logger.error("Manuel news-scan hatasi: %s", e, exc_info=True)

    background_tasks.add_task(_run_scan)
    return {"status": "queued", "message": "Tarama arka planda baslatildi"}


@app.post("/api/v1/admin/publish-news")
async def admin_publish_news(body: dict, db: AsyncSession = Depends(get_db)):
    """Yerel news_scanner_v2'den çağrılır — tweet attıktan sonra
    PendingTweet tablosuna yazar (mobile app public news-feed için) +
    push bildirim gönderir.

    Body:
        admin_password: str
        tweet_text: str
        cover_url: str (optional)
        headline: str (push için)
        summary: str (push için)
        category: str (optional)
    """
    settings = get_settings()
    if body.get("admin_password") != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    tweet_text = body.get("tweet_text", "")
    if not tweet_text:
        raise HTTPException(status_code=400, detail="tweet_text gerekli")

    # 1. DB'ye kaydet — twitter_service._mark_tweet_sent zaten son 2 dk icinde
    # PendingTweet(source="unknown") yazmis olabilir. Varsa onu guncelle, yoksa yeni ekle.
    from app.models import PendingTweet
    from sqlalchemy import select
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=2)
    existing = (await db.execute(
        select(PendingTweet)
        .where(PendingTweet.text == tweet_text)
        .where(PendingTweet.sent_at >= cutoff)
        .order_by(PendingTweet.id.desc())
        .limit(1)
    )).scalar_one_or_none()

    # Kategori BILGI ise source="system_info" — piyasa haberleri listesinde gorunmez,
    # sadece sistem bildirimi olarak islenir
    _src = "system_info" if (body.get("category") or "").upper() == "BILGI" else "news_scanner"

    if existing:
        existing.source = _src
        if body.get("cover_url"):
            existing.image_path = body.get("cover_url")
        pt = existing
    else:
        pt = PendingTweet(
            text=tweet_text,
            image_path=body.get("cover_url", "") or "",
            source=_src,
            status="sent",
            sent_at=datetime.now(timezone.utc),
        )
        db.add(pt)
    await db.commit()

    # 2. Push bildirim
    category_upper = (body.get("category") or "").upper()
    try:
        from app.services.notification import NotificationService
        svc = NotificationService(db)
        if category_upper == "BILGI":
            # Sistem bilgilendirme — Sistem kategorisinde, metin tamamen govdede,
            # yonlendirme yok (bildirim merkezinde acildiginda expand olur)
            headline = body.get("headline", "Bilgilendirme")
            summary = (body.get("summary") or "")
            data = {
                "type": "system_info",
                # screen yok → Detaylari Incele butonu cikmaz, tiklayinca expand olur
            }
            # Body = baslik + ozet (tam metin)
            full_body = f"{headline}\n\n{summary}" if summary else headline
            await svc._send_filtered(
                "notify_news",
                "📢 Borsa Cebimde Bilgilendirme",
                full_body[:900],
                data,
                f"Bilgilendirme: {headline[:50]}",
                category="system",
            )
        else:
            await svc.notify_market_news(
                body.get("headline", "Önemli gelişme"),
                summary=(body.get("summary") or "")[:800],
            )
        await db.commit()
    except Exception as e:
        logger.error("Publish-news push hatasi: %s", e)

    return {"status": "ok", "pending_tweet_id": pt.id}


@app.post("/api/v1/admin/cleanup-bad-hashtags")
async def admin_cleanup_bad_hashtags(body: dict, db: AsyncSession = Depends(get_db)):
    """Tek bir PendingTweet kaydinda #KOC, #SABANCI gibi yanlis hashtag satirini
    ve orphan 'Ç' / 'C' karakterini temizler.

    Body:
      admin_password: str
      search: str (tweet text icinde arama yapar, ilk eslesen satiri duzeltir)
    """
    settings = get_settings()
    if body.get("admin_password") != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    search = (body.get("search") or "").strip()
    if not search:
        raise HTTPException(status_code=400, detail="search gerekli (ornek: 'KOC HOLDING')")

    import re as _re
    from app.models import PendingTweet
    from sqlalchemy import select

    BAD_TAGS = {
        "KOC", "KOÇ", "SABANCI", "ANADOLU", "DOGUS", "DOĞUŞ", "EGE", "ZORLU",
        "YILDIZ", "CALIK", "ÇALIK", "OYAK", "BORUSAN", "ENKA", "IHLAS",
        "TURKCELL", "TURKIYE", "TÜRKİYE", "TURK", "TÜRK", "ISBANK", "GARANTI",
        "YAPIKREDI", "AKBANK", "HOLDING", "GRUP", "BIST", "BORSA", "KAP",
        "SPK", "TCMB", "Ç", "C",
    }

    row = (await db.execute(
        select(PendingTweet)
        .where(PendingTweet.text.ilike(f"%{search}%"))
        .order_by(PendingTweet.id.desc())
        .limit(1)
    )).scalar_one_or_none()

    if not row:
        return {"status": "not_found", "search": search}

    original = row.text or ""
    lines = original.split("\n")
    cleaned_lines = []
    for line in lines:
        s = line.strip()
        if "#" in s:
            tags = _re.findall(r"#(\S+)", s)
            if tags:
                good = [t for t in tags if t.upper() not in BAD_TAGS and 4 <= len(t) <= 5 and t.isalpha()]
                if good:
                    cleaned_lines.append(" ".join(f"#{t}" for t in good))
                continue
        if s in BAD_TAGS and len(s) <= 2:
            continue
        cleaned_lines.append(line)
    new_text = _re.sub(r"\n{3,}", "\n\n", "\n".join(cleaned_lines)).strip()

    changed = new_text != original
    if changed:
        row.text = new_text
        await db.commit()

    return {
        "status": "ok",
        "id": row.id,
        "changed": changed,
        "before_len": len(original),
        "after_len": len(new_text),
    }


@app.post("/api/v1/admin/news-approve")
async def admin_approve_news(body: dict):
    """Telegram'dan onay bekleyen haberi tweet at."""
    settings = get_settings()
    if body.get("admin_password") != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    index = body.get("index", 0)

    from app.services.news_scanner_service import approve_news
    result = await approve_news(index)
    return result


@app.post("/api/v1/admin/news-reject")
async def admin_reject_news(body: dict):
    """Haberi kuyruktan cikar."""
    settings = get_settings()
    if body.get("admin_password") != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    index = body.get("index", 0)

    from app.services.news_scanner_service import reject_news
    result = await reject_news(index)
    return result


@app.get("/api/v1/admin/news-queue")
async def admin_news_queue(admin_password: str = ""):
    """Bekleyen haber kuyrugunu goster."""
    settings = get_settings()
    if admin_password != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.services.news_scanner_service import get_queue
    return {"queue": get_queue()}


@app.post("/api/v1/admin/news-resume")
async def admin_news_resume(body: dict):
    """/devam — kuyruk duraklatmasini kaldir, tarama devam etsin."""
    settings = get_settings()
    if body.get("admin_password") != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.services.news_scanner_service import resume_queue
    return await resume_queue()


@app.post("/api/v1/admin/news-clear")
async def admin_news_clear(body: dict):
    """/temizle — kuyrugu tamamen sifirla."""
    settings = get_settings()
    if body.get("admin_password") != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")

    from app.services.news_scanner_service import clear_queue
    return await clear_queue()


@app.post("/api/v1/admin/fix-html-entities")
@limiter.limit("5/minute")
async def admin_fix_html_entities(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Veritabanindaki HTML entity'leri duzelt (&amp; -> & vb.)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    import html as _html
    import re as _re
    from app.models.pending_tweet import PendingTweet

    # pending_tweets tablosundaki bozuk kayitlar — genis arama
    from sqlalchemy import or_, text as sa_text
    result = await db.execute(
        select(PendingTweet).where(
            or_(
                PendingTweet.text.contains("&amp;"),
                PendingTweet.text.contains("&lt;"),
                PendingTweet.text.contains("&gt;"),
                PendingTweet.text.contains("&;"),
            )
        )
    )
    tweets = result.scalars().all()
    fixed_count = 0
    for tw in tweets:
        cleaned = _html.unescape(tw.text)
        # &; gibi bozuk kalintilari temizle — context'e gore duzelt
        # "doğu&;da" gibi kelime icindeki &; → apostrof (')
        cleaned = _re.sub(r'(\w)&;(\w)', r"\1'\2", cleaned)
        # Kalan &; → &
        cleaned = cleaned.replace("&;", "&")
        # Tekrar eden && düzelt
        while "&&" in cleaned:
            cleaned = cleaned.replace("&&", "&")
        if cleaned != tw.text:
            tw.text = cleaned
            fixed_count += 1

    await db.commit()
    return {"status": "ok", "fixed_tweets": fixed_count}


@app.post("/api/v1/admin/debug-env")
async def debug_env(body: dict):
    """Cloudinary ve diger env var durumunu kontrol et."""
    pw = body.get("admin_password", "")
    if pw != os.environ.get("ADMIN_PASSWORD", "zenger7245175"):
        raise HTTPException(403, "Yetkisiz")
    cloud = os.environ.get("CLOUDINARY_URL", "")
    return {
        "cloudinary_set": bool(cloud),
        "cloudinary_prefix": cloud[:30] + "..." if len(cloud) > 30 else cloud,
        "gemini_set": bool(os.environ.get("GEMINI_API_KEY", "")),
    }


@app.post("/api/v1/admin/fix-tweet-text/{tweet_id}")
async def fix_tweet_text(tweet_id: int, body: dict, db: AsyncSession = Depends(get_db)):
    """Tweet metnini dogrudan guncelle."""
    pw = body.get("admin_password", "")
    if not _verify_admin_password(pw):
        raise HTTPException(403, "Yetkisiz")

    new_text = body.get("text", "")
    if not new_text:
        raise HTTPException(400, "text alani gerekli")

    from app.models.pending_tweet import PendingTweet
    result = await db.execute(select(PendingTweet).where(PendingTweet.id == tweet_id))
    tweet = result.scalar_one_or_none()
    if not tweet:
        raise HTTPException(404, "Tweet bulunamadi")

    old_len = len(tweet.text)
    tweet.text = new_text
    # image_url de guncelle (opsiyonel)
    if body.get("image_url"):
        tweet.image_path = body["image_url"]
    await db.commit()

    return {"status": "ok", "id": tweet_id, "old_len": old_len, "new_len": len(new_text)}


@app.post("/api/v1/admin/fix-market-reason")
async def fix_market_reason(body: dict, db: AsyncSession = Depends(get_db)):
    """Tavan/taban hisse sebebini (reason) dogrudan guncelle."""
    pw = body.get("admin_password", "")
    if not _verify_admin_password(pw):
        raise HTTPException(403, "Yetkisiz")

    ticker = body.get("ticker", "").upper()
    date_str = body.get("date", "")
    new_reason = body.get("reason", "")
    if not ticker or not date_str or not new_reason:
        raise HTTPException(400, "ticker, date, reason alanlari gerekli")

    from sqlalchemy import text as sa_text
    from datetime import date as date_type
    try:
        dt = date_type.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(400, "date formati: YYYY-MM-DD")
    result = await db.execute(
        sa_text('UPDATE daily_stock_market_stats SET reason = :reason WHERE ticker = :ticker AND "date" = :dt'),
        {"reason": new_reason, "ticker": ticker, "dt": dt}
    )
    await db.commit()
    return {"status": "ok", "ticker": ticker, "date": date_str, "rows_updated": result.rowcount}


@app.post("/api/v1/admin/fix-notification-categories")
async def fix_notification_categories(body: dict, db: AsyncSession = Depends(get_db)):
    """Eski system kategorili SPK/VİOP/Piyasa bildirimlerini other'a cevir."""
    pw = body.get("admin_password", "")
    if not _verify_admin_password(pw):
        raise HTTPException(403, "Yetkisiz")

    from sqlalchemy import text as sa_text
    result = await db.execute(
        sa_text("""
            UPDATE notification_logs SET category = 'other'
            WHERE category = 'system'
            AND (
                title LIKE '%SPK%Bülten%'
                OR title LIKE '%VİOP%'
                OR title LIKE '%VIOP%'
                OR title LIKE '%Önemli Piyasa%'
                OR title LIKE '%Piyasa Gelişmesi%'
            )
        """)
    )
    await db.commit()
    return {"status": "ok", "rows_updated": result.rowcount}


# -------------------------------------------------------
# Kurum Onerileri — Araci Kurum Hedef Fiyat & Tavsiye
# -------------------------------------------------------

@app.get("/api/v1/kurum-onerileri")
@limiter.limit("30/minute")
async def get_kurum_onerileri(
    request: Request,
    period: str = Query("today", description="today, week, month, all"),
    ticker: Optional[str] = Query(None, description="Hisse kodu filtresi"),
    institution: Optional[str] = Query(None, description="Kurum adi filtresi"),
    recommendation: Optional[str] = Query(None, description="Oneri filtresi (Al, Tut, Sat vb.)"),
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Kurum onerilerini listele — tarih, hisse, kurum bazli filtreleme."""
    from app.services.kurum_oneri_service import KurumOneriService
    service = KurumOneriService(db)
    items, total = await service.get_recommendations(
        period=period,
        ticker=ticker,
        institution=institution,
        recommendation=recommendation,
        page=page,
        limit=limit,
    )
    return {
        "items": [
            {
                "id": item.id,
                "ticker": item.ticker,
                "company_name": item.company_name,
                "institution_name": item.institution_name,
                "recommendation": item.recommendation,
                "target_price": float(item.target_price) if item.target_price else None,
                "current_price": float(item.current_price) if item.current_price else None,
                "potential_return": float(item.potential_return) if item.potential_return else None,
                "report_date": item.report_date.isoformat() if item.report_date else None,
                "source_url": item.source_url,
                "created_at": item.created_at.isoformat() if item.created_at else None,
                "ai_comment": item.ai_comment,
                "ai_comment_at": item.ai_comment_at.isoformat() if item.ai_comment_at else None,
            }
            for item in items
        ],
        "total": total,
        "page": page,
        "limit": limit,
    }


@app.post("/api/v1/admin/cleanup-kurum-oneri-indexes")
@limiter.limit("3/minute")
async def admin_cleanup_kurum_oneri_indexes(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Admin: kurum_oneri'den endeks ticker'larini sil (XU100, XBANK vs.)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.kurum_oneri import KurumOneri
    INDEX = ["XU100","XU030","XU050","XBANK","XKURU","XSPOR","XTUMY","XGIDA",
             "XKMYA","XMANA","XKAGT","XMESY","XILTM","XGMYO","XUMAL","XUSIN",
             "XHOLD","XINSA","XELKT","XTEKS","XTAST","XTRZM","XSGRT","XFINK",
             "XHARZ","XYORT","XBLSM","XUTEK","XTRAS","XKOBI","XKURY","XSANT",
             "XYUZO","XMADN","XSAVE"]
    rows = (await db.execute(
        select(KurumOneri).where(
            or_(
                KurumOneri.ticker.in_(INDEX),
                KurumOneri.ticker.like("XU0%"),
                KurumOneri.ticker.like("XU1%"),
            )
        )
    )).scalars().all()
    deleted = 0
    for r in rows:
        await db.delete(r)
        deleted += 1
    await db.commit()
    return {"status": "ok", "deleted": deleted}


@app.post("/api/v1/admin/kurum-oneri-ai-backfill")
async def admin_kurum_oneri_backfill(body: dict, db: AsyncSession = Depends(get_db)):
    """AI yorumu eksik olan kurum onerilerine Claude Sonnet ile yorum ekle.
    Body: { admin_password, limit (opsiyonel, varsayilan 30) }"""
    settings = get_settings()
    if body.get("admin_password") != settings.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from app.services.kurum_oneri_ai import backfill_comments
    limit = int(body.get("limit", 30))
    result = await backfill_comments(db, limit=limit)
    return {"status": "ok", **result}


@app.get("/api/v1/kurum-onerileri/stats")
@limiter.limit("30/minute")
async def get_kurum_onerileri_stats(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Kurum onerileri istatistikleri."""
    from app.services.kurum_oneri_service import KurumOneriService
    service = KurumOneriService(db)
    return await service.get_stats()


@app.get("/api/v1/kurum-onerileri/filters")
@limiter.limit("30/minute")
async def get_kurum_onerileri_filters(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Filtre secenekleri — benzersiz kurum ve hisse listesi."""
    from app.services.kurum_oneri_service import KurumOneriService
    service = KurumOneriService(db)
    institutions = await service.get_institutions()
    tickers = await service.get_tickers()
    return {
        "institutions": institutions,
        "tickers": tickers,
    }


@app.post("/api/v1/admin/trigger-kurum-scrape")
@limiter.limit("5/minute")
async def admin_trigger_kurum_scrape(request: Request, payload: dict = Body(...)):
    """Admin: Kurum onerileri scraper'i manuel tetikle."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.scheduler import scrape_kurum_onerileri
        await scrape_kurum_onerileri()
        return {"status": "ok", "message": "Kurum onerileri scrape tamamlandi"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/trigger-kurum-oneri-tweet")
@limiter.limit("5/minute")
async def admin_trigger_kurum_oneri_tweet(request: Request, payload: dict = Body(...)):
    """Admin: Kurum oneri gunluk tweet'ini manuel tetikle + durum raporu dondur."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from datetime import datetime as _dt, timezone as _tz, timedelta as _td
    from app.models.kurum_oneri import KurumOneri
    from sqlalchemy import select as _sel, and_

    # Durum raporu: bugun kac oneri var?
    now_utc = _dt.now(_tz.utc)
    tr_now = now_utc + _td(hours=3)
    tr_day_start = tr_now.replace(hour=0, minute=0, second=0, microsecond=0)
    utc_day_start = tr_day_start - _td(hours=3)

    async with async_session() as db:
        # Tweet gonderilmemis
        r1 = await db.execute(
            _sel(KurumOneri).where(
                KurumOneri.created_at >= utc_day_start,
                KurumOneri.tweet_sent_at.is_(None),
            ).order_by(KurumOneri.created_at.desc())
        )
        pending = list(r1.scalars().all())

        # Tweet gonderilmis
        r2 = await db.execute(
            _sel(KurumOneri).where(
                KurumOneri.created_at >= utc_day_start,
                KurumOneri.tweet_sent_at.isnot(None),
            )
        )
        sent = list(r2.scalars().all())

    report = {
        "utc_day_start": utc_day_start.isoformat(),
        "pending_count": len(pending),
        "sent_count": len(sent),
        "pending_items": [{"ticker": i.ticker, "inst": i.institution_name, "rec": i.recommendation, "created": str(i.created_at)} for i in pending[:10]],
    }

    if not payload.get("dry_run", False):
        try:
            from app.scheduler import kurum_oneri_daily_tweet_job
            await kurum_oneri_daily_tweet_job()
            report["tweet_triggered"] = True
        except Exception as e:
            report["tweet_error"] = str(e)[:300]
    else:
        report["tweet_triggered"] = False
        report["note"] = "dry_run=true, tweet atilmadi"

    return report


# ═══════════════════════════════════════════════════════════════════
# v3.0.0 — BILANCO / TEMETTU / SIRKET KARTI / IPO ANKETI / TAKVIM
# ═══════════════════════════════════════════════════════════════════
# BILANCO (v3.0.0)
# -------------------------------------------------------

@app.get("/api/v1/bilanco/periods")
async def list_bilanco_periods(db: AsyncSession = Depends(get_db)):
    """Mevcut bilanço dönemlerini sayılarıyla döner — frontend tab seçici için.

    Returns: [{period: "2026-Q1", count: 39, label: "2026/1"}, ...] (en yeniden eskiye)
    """
    result = await db.execute(
        select(CompanyFinancial.period, func.count(CompanyFinancial.ticker))
        .where(CompanyFinancial.period.isnot(None))
        .group_by(CompanyFinancial.period)
        .order_by(desc(CompanyFinancial.period))
        .limit(8)
    )
    rows = result.all()
    items = []
    for period, count in rows:
        # "2026-Q1" -> label "2026/1"
        label = period
        try:
            year, q = period.split("-Q")
            label = f"{year}/{q}"
        except Exception:
            pass
        items.append({"period": period, "count": int(count), "label": label})
    return {"periods": items}


@app.get("/api/v1/bilanco/top")
async def get_top_bilancos(
    period: str = Query(..., description="2026-Q1 formatinda donem"),
    limit: int = Query(50, ge=5, le=100),
    sort: str = Query("recent", description="recent (yayinlanma tarihine gore) | ai (AI puanina gore)"),
    db: AsyncSession = Depends(get_db),
):
    """Donemin bilancolari — sort param ile:
      - recent (default): En son yayinlanan bilancolar (KLGYO, BORSK, ... Fintables tweet feed gibi)
      - ai: En yuksek AI puanli bilancolar (TTRAK gibi yorum kalitesi sirali)
    """
    # 1. Bu donemde bilanco aciklayan hisseler
    fin_result = await db.execute(
        select(CompanyFinancial)
        .where(CompanyFinancial.period == period)
    )
    finals = {f.ticker: f for f in fin_result.scalars().all()}
    if not finals:
        return {"period": period, "count": 0, "items": []}

    tickers = list(finals.keys())

    # Her ticker icin son 5 ceyrekligi de getir (chart icin)
    quarterly_q = await db.execute(
        select(CompanyFinancial)
        .where(CompanyFinancial.ticker.in_(tickers))
        .order_by(CompanyFinancial.ticker, desc(CompanyFinancial.period))
    )
    quarterly_by_ticker: dict[str, list] = {}
    for row in quarterly_q.scalars().all():
        quarterly_by_ticker.setdefault(row.ticker, []).append(row)
    # Her ticker icin max 20 ceyrek = 5 yil (en yeni → en eski) — 5 yillik grafikler icin
    for tk in list(quarterly_by_ticker.keys()):
        quarterly_by_ticker[tk] = quarterly_by_ticker[tk][:20]

    # Resmi BIST sektör adlari (stock_sectors)
    sector_name_map: dict[str, str] = {}
    try:
        from sqlalchemy import text as _sa_text
        sres = await db.execute(
            _sa_text("SELECT ticker, sector_name FROM stock_sectors WHERE ticker = ANY(:tickers)"),
            {"tickers": tickers},
        )
        for row in sres.fetchall():
            if row[1]:
                sector_name_map[row[0]] = row[1]
    except Exception:
        pass

    # 2. Her ticker icin en son KAP Finansal Rapor + ai_impact_score
    kap_result = await db.execute(
        select(KapAllDisclosure)
        .where(KapAllDisclosure.company_code.in_(tickers))
        .where(
            # Eski 'Finansal Rapor' VEYA yeni 'Finansal Durum Tablosu (Bilanço)' baslikli olanlar
            or_(
                KapAllDisclosure.title.ilike('%Finansal Rapor%'),
                KapAllDisclosure.title.ilike('%Finansal Durum Tablosu%'),
                KapAllDisclosure.is_bilanco == True,
            )
        )
        .order_by(KapAllDisclosure.company_code, desc(KapAllDisclosure.published_at))
    )
    kap_by_ticker: dict[str, KapAllDisclosure] = {}
    for k in kap_result.scalars().all():
        if k.company_code not in kap_by_ticker:
            kap_by_ticker[k.company_code] = k

    # earnings_calendar.announced_date — her ticker icin gercek bilanco aciklama tarihi.
    # KapAllDisclosure'da is_bilanco=True olmayan eski kayitlar icin de bu tarihten
    # 'recent' sirasini olusturabiliriz. KLGYO disinda da sirali olsun diye.
    from app.models.earnings_calendar import EarningsCalendar
    ec_result = await db.execute(
        select(EarningsCalendar)
        .where(EarningsCalendar.ticker.in_(tickers))
        .where(EarningsCalendar.period == period)
        .where(EarningsCalendar.announced_date.isnot(None))
    )
    announced_by_ticker: dict[str, date] = {}
    for ec in ec_result.scalars().all():
        announced_by_ticker[ec.ticker] = ec.announced_date

    # 3. Ratio + price ekle
    ratios_result = await db.execute(
        select(FinancialRatio)
        .where(FinancialRatio.ticker.in_(tickers))
        .order_by(FinancialRatio.ticker, desc(FinancialRatio.date))
    )
    ratio_by_ticker: dict[str, FinancialRatio] = {}
    for r in ratios_result.scalars().all():
        if r.ticker not in ratio_by_ticker:
            ratio_by_ticker[r.ticker] = r

    # Son fiyat — DailyStockMarketStat.close_price BIST lisans nedeniyle KALDIRILDI
    # Anlik fiyat icin Yahoo Finance fallback'i tek tek cekiyoruz (bilanco kartlari icin)
    price_by_ticker: dict[str, float] = {}
    if tickers:
        import asyncio as _asyncio
        sem = _asyncio.Semaphore(20)
        async def _fetch_one(t: str):
            async with sem:
                try:
                    p = await _fetch_yahoo_v8(t)
                    if p:
                        price_by_ticker[t] = float(p)
                except Exception:
                    pass
        await _asyncio.gather(*[_fetch_one(t) for t in tickers], return_exceptions=True)

    def _f(v):
        return float(v) if v is not None else None

    # 4. Birlestir + ai score sirala
    # Oncelik: company_financials.ai_score (bilanco-spesifik analiz),
    # yoksa KapAllDisclosure.ai_impact_score (routine pre-filter — genelde 5.0)
    items = []
    for ticker, fin in finals.items():
        kap = kap_by_ticker.get(ticker)
        ratio = ratio_by_ticker.get(ticker)
        cf_ai = getattr(fin, "ai_score", None)
        cf_summary = getattr(fin, "ai_summary", None)
        cf_label = getattr(fin, "ai_label", None)
        cf_analysis = getattr(fin, "ai_analysis", None)
        if cf_ai is not None:
            # Bilanco-spesifik AI analizi mevcut
            ai_score = float(cf_ai)
            ai_summary = (cf_summary[:600] if cf_summary else None)
            ai_analysis = cf_analysis  # tam yapilandirilmis JSON (derin analiz bolumleri)
        else:
            # Henuz bilanco-AI uretilmemis. ai_score'u None birak,
            # ai_summary olarak KAP'in 'rutin/idari bildirim' metnini DONDURME
            # (kullaniciyi yaniltir). Sadece puanlanmamis goster.
            ai_score = None
            ai_summary = None
            ai_analysis = None
        items.append({
            "ticker": ticker,
            "period": fin.period,
            "ai_score": ai_score,
            "ai_label": cf_label,
            "ai_summary": ai_summary,
            "ai_analysis": ai_analysis,
            "ai_sentiment": kap.ai_sentiment if kap else None,
            # Sirayla: announced_date (Fintables backfill) > KAP published_at >
            #          earnings_calendar.announced_date > scraped_at
            "published_at": (
                fin.announced_date.isoformat() if getattr(fin, "announced_date", None) else
                (kap.published_at.isoformat() if kap and kap.published_at else
                 (announced_by_ticker[ticker].isoformat() if ticker in announced_by_ticker else
                  (fin.scraped_at.isoformat() if getattr(fin, "scraped_at", None) else
                   (fin.updated_at.isoformat() if getattr(fin, "updated_at", None) else None))))
            ),
            "fk": _f(ratio.fk) if ratio else None,
            "pddd": _f(ratio.pddd) if ratio else None,
            "fd_favok": _f(ratio.fd_favok) if ratio else None,
            "piyasa_degeri": _f(ratio.piyasa_degeri) if ratio else None,
            "price": price_by_ticker.get(ticker),
            "revenue": _f(fin.revenue),
            "net_income": _f(fin.net_income),
            "ebitda": _f(fin.ebitda) if fin.ebitda is not None else _f(fin.operating_profit),
            # Ceyreklik veri (son 5 ceyrek, en eski->en yeni)
            "quarterly": [
                {
                    "period": q.period,
                    "revenue": _f(q.revenue),
                    "ebitda": _f(q.ebitda) if q.ebitda is not None else _f(q.operating_profit),
                    "net_income": _f(q.net_income),
                    "total_equity": _f(q.total_equity),
                }
                for q in reversed(quarterly_by_ticker.get(ticker, []))
            ],
            "total_assets": _f(fin.total_assets),
            "total_equity": _f(fin.total_equity),
            "net_debt": _f(fin.net_debt),
            "sector_type": fin.sector_type,
            "sector_name": sector_name_map.get(ticker),
        })

    # Sort: 'recent' (varsayilan) — yayinlanma tarihine gore DESC (Fintables feed gibi)
    #       'ai' — ai_score DESC (en iyi puanli once)
    if sort == "ai":
        items.sort(key=lambda x: (x["ai_score"] is None, -(x["ai_score"] or 0)))
    else:
        # recent: published_at en yeni once. None'lari en sona at.
        items.sort(key=lambda x: (x["published_at"] is None, x["published_at"] or ""), reverse=False)
        # NOT: tuple compare ile None'lari sona attik. Simdi published_at icin reverse:
        items_no_none = [i for i in items if i["published_at"] is not None]
        items_none = [i for i in items if i["published_at"] is None]
        items_no_none.sort(key=lambda x: x["published_at"], reverse=True)
        items = items_no_none + items_none

    items = items[:limit]
    return {"period": period, "count": len(items), "items": items, "sort": sort}


@app.get("/api/v1/bilanco/{ticker}")
async def get_bilanco_analysis(ticker: str, db: AsyncSession = Depends(get_db)):
    """Hissenin bilanco verileri (5 yil) + oranlar + DERIN AI analizi + sektör.

    Derin Analiz sekmesi bu endpoint'i kullanir. AI puan/yorum BILANCO-AI'dan
    gelir (company_financials.ai_*), KAP rutin metninden DEGIL.
    """
    t = ticker.upper()

    # Finansal veriler (son 20 donem = ~5 yil)
    fin_result = await db.execute(
        select(CompanyFinancial)
        .where(CompanyFinancial.ticker == t)
        .order_by(desc(CompanyFinancial.period))
        .limit(20)
    )
    financials = list(fin_result.scalars().all())

    if not financials:
        raise HTTPException(status_code=404, detail=f"Bilanco verisi bulunamadi: {ticker}")

    latest = financials[0]

    # Son oranlar
    ratio_result = await db.execute(
        select(FinancialRatio)
        .where(FinancialRatio.ticker == t)
        .order_by(desc(FinancialRatio.date))
        .limit(1)
    )
    ratio = ratio_result.scalar_one_or_none()

    # Resmi BIST sektörü
    sector_name = None
    try:
        from sqlalchemy import text as _sa_text
        sres = await db.execute(
            _sa_text("SELECT sector_name FROM stock_sectors WHERE ticker = :t"), {"t": t}
        )
        row = sres.fetchone()
        if row and row[0]:
            sector_name = row[0]
    except Exception:
        pass

    def _f(v):
        return float(v) if v is not None else None

    # Çeyreklik seri (5 yıl, eskiden yeniye) — Derin Analiz grafikleri
    quarterly = [
        {
            "period": q.period,
            "revenue": _f(q.revenue),
            "ebitda": _f(q.ebitda) if q.ebitda is not None else _f(q.operating_profit),
            "net_income": _f(q.net_income),
            "total_equity": _f(q.total_equity),
            "total_debt": _f(q.total_debt),
            "net_debt": _f(q.net_debt),
        }
        for q in reversed(financials)
    ]

    # DERIN AI analizi — company_financials.ai_* (bilanço-AI, rutin KAP DEĞİL)
    deep = None
    if getattr(latest, "ai_analysis", None):
        try:
            import json as _json
            deep = _json.loads(latest.ai_analysis)
        except Exception:
            deep = None

    return {
        "ticker": t,
        "period": latest.period,
        "sector_name": sector_name,
        "sector_type": latest.sector_type,
        "ai_score": _f(latest.ai_score),
        "ai_label": latest.ai_label,
        "ai_summary": latest.ai_summary,
        "ai_analysis": deep,  # tam yapılandırılmış derin analiz (bölümler)
        "ai_analyzed_at": latest.ai_analyzed_at.isoformat() if latest.ai_analyzed_at else None,
        "quarterly": quarterly,
        "ratios": {
            "fk": _f(ratio.fk) if ratio else None,
            "pddd": _f(ratio.pddd) if ratio else None,
            "fd_favok": _f(ratio.fd_favok) if ratio else None,
        } if ratio else None,
    }


@app.get("/api/v1/bilanco")
async def list_latest_bilancos(
    limit: int = Query(50, ge=1, le=800),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Son aciklanan bilancolar — FIFO, hisse adedi bazli.

    Fintables X tarzı: bilanço açıklandığı an, saat:dakika ile.
    Son 50 bilanço (varsayılan), aşağı kaydırdıkça offset ile eski bilançolar.
    Tarih limiti yok — en eski bilanço da görülebilir.
    """
    # KAP bilanço bildirimleri (is_bilanco=TRUE, en yeniden eskiye)
    # Sadece "Finansal Rapor" titleli olanlar — Sorumluluk Beyani ve Faaliyet Raporu hariç
    # KAP'i en yeniden eskiye sirala, ticker bazli DISTINCT al
    # Aynı hisse için 'Finansal Rapor' + 'Finansal Durum Tablosu' + 'Kar Zarar' gibi
    # birden fazla KAP gelirse, yalnizca EN YENI'yi goster (ticker'a gore tek satir).
    # Sadece is_bilanco=True flag'ine guven — title pattern'a takilma.
    # ENJSA'nin Sorumluluk Beyanı / Faaliyet / Özkaynaklar bildirimlerinin hepsi
    # is_bilanco=True ile yazilir, bu hisseler de listede gorunur.
    from app.models.company_financial import FinancialRatio
    from app.models.temel_analiz import TemelAnaliz
    from sqlalchemy import text as _sa_text
    import datetime as _dtmod

    # ── 1) announced_date olan ticker'lar (en yeni dönemin tarihi) — Fintables feed kaynağı ──
    dated_rows = (await db.execute(_sa_text(
        "SELECT DISTINCT ON (ticker) ticker, announced_date "
        "FROM company_financials WHERE announced_date IS NOT NULL "
        "ORDER BY ticker, period DESC"
    ))).fetchall()
    announced_map: dict[str, object] = {r[0]: r[1] for r in dated_rows}

    # ── 2) KAP is_bilanco (ticker dedupe) — title + published_at fallback için ──
    kap_result = await db.execute(
        select(KapAllDisclosure)
        .where(KapAllDisclosure.is_bilanco == True)
        .order_by(desc(KapAllDisclosure.published_at))
        .limit(3000)
    )
    kap_by_ticker: dict[str, KapAllDisclosure] = {}
    for k in kap_result.scalars().all():
        if k.company_code and k.company_code not in kap_by_ticker:
            kap_by_ticker[k.company_code] = k

    # ── 3) Aday ticker'lar = announced_date olanlar ∪ KAP'lılar; etkin tarihe göre DESC ──
    _MINDT = _dtmod.datetime.min.replace(tzinfo=_dtmod.timezone.utc)
    def _eff_dt(tk):
        ad = announced_map.get(tk)
        if ad is not None:
            return ad if ad.tzinfo else ad.replace(tzinfo=_dtmod.timezone.utc)
        k = kap_by_ticker.get(tk)
        if k and k.published_at:
            return k.published_at if k.published_at.tzinfo else k.published_at.replace(tzinfo=_dtmod.timezone.utc)
        return _MINDT
    candidates = set(announced_map.keys()) | set(kap_by_ticker.keys())
    ordered_tickers = sorted(candidates, key=_eff_dt, reverse=True)
    page_tickers = ordered_tickers[offset:offset + limit]

    def _f(v):
        return float(v) if v is not None else None

    # Tum unique ticker'lar icin financial_ratios + son fiyat'i once topluca cek
    unique_tickers = list(page_tickers)
    ratios_map: dict[str, FinancialRatio] = {}
    temel_map: dict[str, TemelAnaliz] = {}
    if unique_tickers:
        # Her ticker icin en son ratio kaydi
        rr = await db.execute(
            select(FinancialRatio)
            .where(FinancialRatio.ticker.in_(unique_tickers))
            .order_by(FinancialRatio.ticker, desc(FinancialRatio.date))
        )
        for r in rr.scalars().all():
            if r.ticker not in ratios_map:
                ratios_map[r.ticker] = r

        # Yerel Excel sync (TemelAnaliz) — fallback fk/pddd/fd_favok/piyasa
        tr = await db.execute(
            select(TemelAnaliz).where(TemelAnaliz.ticker.in_(unique_tickers))
        )
        for t in tr.scalars().all():
            temel_map[t.ticker] = t

    # Resmi BIST sektör adlari (stock_sectors — hisse_endeks_ds.csv'den)
    sector_name_map: dict[str, str] = {}
    if unique_tickers:
        try:
            from sqlalchemy import text as _sa_text
            sres = await db.execute(
                _sa_text("SELECT ticker, sector_name FROM stock_sectors WHERE ticker = ANY(:tickers)"),
                {"tickers": unique_tickers},
            )
            for row in sres.fetchall():
                if row[1]:
                    sector_name_map[row[0]] = row[1]
        except Exception:
            pass
    # Son fiyat — Yahoo Finance (close_price DailyStockMarketStat'tan BIST lisans nedeniyle kaldirildi)
    prices_map: dict[str, float] = {}

    # Yahoo fallback — tum ticker'lar icin (paralel, concurrency=20)
    missing = list(unique_tickers)
    if missing:
        import asyncio as _asyncio
        sem = _asyncio.Semaphore(20)

        async def _fetch_one(t: str):
            async with sem:
                try:
                    p = await _fetch_yahoo_v8(t)
                    if p:
                        prices_map[t] = float(p)
                except Exception:
                    pass

        await _asyncio.gather(*[_fetch_one(t) for t in missing], return_exceptions=True)

    # Her bilanço bildirimi için finansal verileri birleştir
    items = []
    for ticker in page_tickers:
        kap = kap_by_ticker.get(ticker)  # None olabilir (announced_date var, KAP yok)

        # Son 20 dönem (5 yıl) — Derin Analiz 5 yıllık grafikleri + karşılaştırma için
        fin_result = await db.execute(
            select(CompanyFinancial)
            .where(CompanyFinancial.ticker == ticker)
            .order_by(desc(CompanyFinancial.period))
            .limit(20)
        )
        finals = list(fin_result.scalars().all())
        fin = finals[0] if finals else None

        # ★ İki farklı önceki dönem hesabı (Fintables tarzı):
        # - Gelir tablosu için: aynı çeyrek bir yıl önce (YoY — büyüme trendi)
        # - Bilanço için: önceki yıl sonu (Q4) — yıl başına göre değişim
        prev_income = None  # YoY (gelir tablosu karşılaştırması)
        prev_balance = None  # Önceki yıl sonu (bilanço karşılaştırması)
        if fin and fin.period:
            try:
                year, q = fin.period.split('-')
                cur_year = int(year)
                yoy_period = f"{cur_year - 1}-{q}"          # örn 2025-Q1
                year_end_period = f"{cur_year - 1}-Q4"      # örn 2025-Q4

                for f in finals:
                    if f.period == yoy_period:
                        prev_income = f
                    if f.period == year_end_period:
                        prev_balance = f
            except Exception:
                pass

        # Fallback: prev_income veya prev_balance yoksa son 5. dönemi kullan
        prev_to_use_income = prev_income or (finals[4] if len(finals) >= 5 else (finals[1] if len(finals) >= 2 else None))
        prev_to_use_balance = prev_balance or prev_to_use_income

        # Çeyreklik bars — eskiden yeniye sirala (5 yil = 20 ceyrek, Derin Analiz icin)
        quarterly = list(reversed(finals[:20]))

        ratio = ratios_map.get(ticker)
        temel = temel_map.get(ticker)
        price_now = prices_map.get(ticker)

        # Degerlik fallback: önce financial_ratios (mynet), boşsa temel_analiz (Excel sync)
        def _r(r_val, t_val):
            if r_val is not None and r_val != 0:
                return _f(r_val)
            return _f(t_val) if t_val is not None and t_val != 0 else None

        # F/K ve piyasa_degeri temel'den kaldirildi (BIST lisans) — sadece ratio'dan al
        fk_val = _f(ratio.fk) if ratio and ratio.fk is not None else None
        pddd_val = _r(ratio.pddd if ratio else None, temel.pddd if temel else None)
        fd_favok_val = _r(ratio.fd_favok if ratio else None, temel.fd_favok if temel else None)
        piyasa_val = _f(ratio.piyasa_degeri) if ratio and ratio.piyasa_degeri is not None else None

        items.append({
            "ticker": ticker,
            "title": (kap.title if kap else f"{ticker} Finansal Rapor"),
            # Sıralama/gösterim tarihi: announced_date (Fintables) > KAP published_at
            "published_at": (
                fin.announced_date.isoformat() if fin and getattr(fin, "announced_date", None) else
                (kap.published_at.isoformat() if kap and kap.published_at else None)
            ),
            # ★ AI puan/yorum BILANCO-AI'dan gelir (gelen bilanconun 3-5 cumlelik ozeti).
            # KAP'in rutin/idari bildirim metnini DONDURME (kullaniciyi yaniltir).
            # Henuz analiz edilmediyse None — frontend "analiz bekleniyor" gosterir.
            "ai_score": _f(fin.ai_score) if fin and getattr(fin, "ai_score", None) is not None else None,
            "ai_label": getattr(fin, "ai_label", None) if fin else None,
            "ai_summary": (fin.ai_summary[:600] if fin and getattr(fin, "ai_summary", None) else None),
            "ai_analysis": getattr(fin, "ai_analysis", None) if fin else None,
            "ai_sentiment": kap.ai_sentiment if kap else None,
            "period": fin.period if fin else None,
            # Gelir prev = YoY (aynı çeyrek bir yıl önce), Bilanço prev = önceki yıl sonu (Q4)
            "prev_period": prev_to_use_income.period if prev_to_use_income else None,
            "prev_period_balance": prev_to_use_balance.period if prev_to_use_balance else None,
            # Bilanço açıklama tarihi (Fintables/KAP backfill) — sıralama için
            "announced_date": fin.announced_date.isoformat() if fin and getattr(fin, "announced_date", None) else None,
            # Sektör tipi (frontend farklı template seçer)
            "sector_type": fin.sector_type if fin and hasattr(fin, 'sector_type') else None,
            # Resmi BIST sektör adı (örn "Tekstil, Deri") — stock_sectors'tan
            "sector_name": sector_name_map.get(ticker),
            # Banka spesifik
            "net_interest_income": _f(fin.net_interest_income) if fin and hasattr(fin, 'net_interest_income') else None,
            "net_fees_commissions": _f(fin.net_fees_commissions) if fin and hasattr(fin, 'net_fees_commissions') else None,
            "loans": _f(fin.loans) if fin and hasattr(fin, 'loans') else None,
            "deposits": _f(fin.deposits) if fin and hasattr(fin, 'deposits') else None,
            # Sigorta spesifik
            "gross_premiums": _f(fin.gross_premiums) if fin and hasattr(fin, 'gross_premiums') else None,
            "technical_balance": _f(fin.technical_balance) if fin and hasattr(fin, 'technical_balance') else None,
            # Degerlik (financial_ratios -> temel_analiz fallback)
            "fk": fk_val,
            "pddd": pddd_val,
            "fd_favok": fd_favok_val,
            "piyasa_degeri": piyasa_val,
            "price": price_now,
            # Mevcut donem — gelir tablosu
            "revenue": _f(fin.revenue) if fin else None,
            "gross_profit": _f(fin.gross_profit) if fin else None,
            "operating_profit": _f(fin.operating_profit) if fin else None,
            "ebitda": _f(fin.ebitda) if fin else None,  # parser doğru EBITDA hesapliyor, fallback yok
            "net_income": _f(fin.net_income) if fin else None,
            # Mevcut donem — bilanço (snapshot)
            "current_assets": _f(fin.current_assets) if fin else None,
            "non_current_assets": _f(fin.non_current_assets) if fin else None,
            "total_assets": _f(fin.total_assets) if fin else None,
            "total_equity": _f(fin.total_equity) if fin else None,
            "total_debt": _f(fin.total_debt) if fin else None,  # artik Financial Debt
            "net_debt": _f(fin.net_debt) if fin else None,
            "cash_and_equivalents": _f(fin.cash_and_equivalents) if fin else None,
            "gross_margin_pct": _f(fin.gross_margin_pct) if fin else None,
            "net_margin_pct": _f(fin.net_margin_pct) if fin else None,
            "roe_pct": _f(fin.roe_pct) if fin else None,
            # Önceki dönem — GELİR TABLOSU (YoY karşılaştırma)
            "revenue_prev": _f(prev_to_use_income.revenue) if prev_to_use_income else None,
            "gross_profit_prev": _f(prev_to_use_income.gross_profit) if prev_to_use_income else None,
            "ebitda_prev": _f(prev_to_use_income.ebitda) if prev_to_use_income else None,
            "net_income_prev": _f(prev_to_use_income.net_income) if prev_to_use_income else None,
            # Önceki dönem — BİLANÇO (önceki yıl sonu = Q4 karşılaştırma)
            "current_assets_prev": _f(prev_to_use_balance.current_assets) if prev_to_use_balance else None,
            "non_current_assets_prev": _f(prev_to_use_balance.non_current_assets) if prev_to_use_balance else None,
            "total_assets_prev": _f(prev_to_use_balance.total_assets) if prev_to_use_balance else None,
            "total_equity_prev": _f(prev_to_use_balance.total_equity) if prev_to_use_balance else None,
            "net_debt_prev": _f(prev_to_use_balance.net_debt) if prev_to_use_balance else None,
            # Ceyreklik bars (artık gerçek quarterly veri — parser YTD'den dönüştürdü)
            "quarterly": [
                {
                    "period": q.period,
                    "revenue": _f(q.revenue),
                    "ebitda": _f(q.ebitda),
                    "net_income": _f(q.net_income),
                    "total_equity": _f(q.total_equity),
                }
                for q in quarterly
            ],
        })

    # ── Sıralama: önce announced_date (Fintables backfill), yoksa published_at ──
    # Fintables tarz: en son açıklanan bilanço en üstte (KLGYO, BORSK, OTTO...).
    def _eff_date(it):
        return it.get("announced_date") or it.get("published_at") or ""
    items.sort(key=_eff_date, reverse=True)

    return {
        "count": len(items),
        "offset": offset,
        "items": items,
    }


# -------------------------------------------------------
# TEMETTU SAMPIYONLAR (v3.0.0) — En yuksek verim / en uzun seri
# DIKKAT: /sampiyonlar route'u {ticker} oncesinde tanimlanmali
# -------------------------------------------------------

@app.get("/api/v1/temettu/sampiyonlar")
async def get_temettu_sampiyonlar(
    sort_by: str = Query("yield", regex="^(yield|streak)$"),
    period: str = Query("5y", regex="^(1y|5y|10y)$"),
    limit: int = Query(50, ge=10, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Temettu sampiyonlari — kumulatif verim veya en uzun seri sirali.

    Period:
      - 1y: sadece bu yil verimi
      - 5y: son 5 yil kumulatif verim toplami (temettu.app paritesi)
      - 10y: son 10 yil kumulatif verim toplami

    Sirala 'streak' ise dagilim yapilan yil sayisina gore.
    """
    current_year = datetime.now().year
    period_years = {"1y": 1, "5y": 5, "10y": 10}[period]
    cutoff_year = current_year - period_years + 1  # inclusive

    # Tum dividend_history (son 10 yil — streak hesabi icin)
    stmt = (
        select(DividendHistory)
        .where(DividendHistory.payment_year >= current_year - 10)
        .order_by(DividendHistory.ticker, desc(DividendHistory.payment_year))
    )
    rows = (await db.execute(stmt)).scalars().all()

    # Hisse bazinda topla
    stocks: dict[str, dict] = {}
    for r in rows:
        if not r.ticker:
            continue
        if r.ticker not in stocks:
            stocks[r.ticker] = {
                "ticker": r.ticker,
                "years_set": set(),         # streak icin
                "period_yields": [],         # secili period icindeki verim degerleri
                "latest": None,
            }
        s = stocks[r.ticker]
        s["years_set"].add(r.payment_year)
        if r.payment_year >= cutoff_year and r.dividend_yield_pct is not None:
            s["period_yields"].append(float(r.dividend_yield_pct))
        if not s["latest"] or r.payment_year > s["latest"].payment_year:
            s["latest"] = r

    items = []
    for ticker, s in stocks.items():
        latest = s["latest"]
        if not latest:
            continue
        # Streak hesabi
        years_sorted = sorted(s["years_set"], reverse=True)
        consecutive = 0
        expected = max(years_sorted) if years_sorted else current_year
        for y in years_sorted:
            if y == expected:
                consecutive += 1
                expected -= 1
            else:
                break

        # Kumulatif verim (period icinde)
        cumulative_yield = round(sum(s["period_yields"]), 2)
        # Kac yilda odeme yapilmis (period icinde)
        period_payment_years = len([1 for v in s["period_yields"] if v > 0])

        gross_per_share = float(latest.gross_dividend_per_share or 0)
        payout = float(latest.payout_ratio or 0)

        # aiScore: kumulatif verim + streak bonus
        ai_score = 5.0 + min(cumulative_yield, 50) * 0.08 + min(consecutive, 10) * 0.1
        ai_score = min(ai_score, 10.0)

        items.append({
            "ticker": ticker,
            "company": ticker,
            "yieldPct": cumulative_yield,                     # kumulatif (period bazli)
            "periodPaymentYears": period_payment_years,       # kac yil odeme oldu
            "consecutiveYears": consecutive,
            "payoutPct": round(payout, 1),
            "grossPerShare": round(gross_per_share, 4),
            "aiScore": round(ai_score, 1),
            "latest_year": latest.payment_year,
        })

    # Filtrele: secili period icinde en az bir odeme + verim > 0
    items = [it for it in items if it["periodPaymentYears"] >= 1 and it["yieldPct"] > 0]

    # 5y/10y'da streak >= 2 sarti (sampiyon olmasi icin)
    if period in ("5y", "10y"):
        items = [it for it in items if it["consecutiveYears"] >= 2]

    # Sirala
    if sort_by == "streak":
        items.sort(key=lambda x: (x["consecutiveYears"], x["yieldPct"]), reverse=True)
    else:
        items.sort(key=lambda x: (x["yieldPct"], x["consecutiveYears"]), reverse=True)

    return {"period": period, "count": len(items[:limit]), "items": items[:limit]}


# -------------------------------------------------------
# TEMETTU DETAY (v3.0.0)
# -------------------------------------------------------

@app.get("/api/v1/temettu/{ticker}")
async def get_temettu_detail(ticker: str, db: AsyncSession = Depends(get_db)):
    """Hissenin temettu gecmisi + guncel beklenti + AI analizi."""
    t = ticker.upper()

    # Temettu gecmisi
    hist_result = await db.execute(
        select(DividendHistory)
        .where(DividendHistory.ticker == t)
        .order_by(desc(DividendHistory.payment_year))
    )
    history = list(hist_result.scalars().all())

    # Guncel beklenti
    div_result = await db.execute(
        select(Dividend).where(Dividend.ticker == t)
    )
    current = div_result.scalar_one_or_none()

    if not history and not current:
        raise HTTPException(status_code=404, detail=f"Temettu verisi bulunamadi: {ticker}")

    return {
        "ticker": t,
        "current": {
            "expected_yield_pct": float(current.expected_dividend_yield_pct) if current and current.expected_dividend_yield_pct else None,
            "last_year_yield_pct": float(current.last_year_yield_pct) if current and current.last_year_yield_pct else None,
            "fk": float(current.fk) if current and current.fk else None,
            "pd_dd": float(current.pd_dd) if current and current.pd_dd else None,
        } if current else None,
        "history": [
            {
                "year": h.payment_year,
                "gross_per_share": float(h.gross_dividend_per_share) if h.gross_dividend_per_share else None,
                "net_per_share": float(h.net_dividend_per_share) if h.net_dividend_per_share else None,
                "yield_pct": float(h.dividend_yield_pct) if h.dividend_yield_pct else None,
                "payment_date": h.payment_date,
                "ex_dividend_date": h.ex_dividend_date if hasattr(h, 'ex_dividend_date') else None,
            }
            for h in history
        ],
    }


@app.get("/api/v1/temettu-akisi")
async def get_temettu_akisi(
    filter: str = Query("karar", regex="^(karar|dagitmama|odendi|all)$"),
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(100, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Temettu akisi — dividend_calendar (state machine) + KAP yedek.

    filter:
      - karar: ykk_alindi, genel_kurul_onayli, tarih_belli (dağıtım kararı verildi)
      - dagitmama: reddedildi (genel kurul reddetti)
      - odendi: odeniyor, tamamlandi (ödeme yapıldı/yapılıyor)
      - all: hepsi
    """
    from datetime import datetime, timezone, timedelta as _td
    from app.models.dividend_calendar import DividendCalendar

    cutoff_dt = datetime.now(timezone.utc) - _td(days=days)
    cutoff_d = cutoff_dt.date()

    STATUS_MAP = {
        "karar": ["ykk_alindi", "genel_kurul_onayli", "tarih_belli"],
        "dagitmama": ["reddedildi"],
        "odendi": ["odeniyor", "tamamlandi"],
    }
    statuses = STATUS_MAP.get(filter)

    query = select(DividendCalendar)
    if statuses:
        query = query.where(DividendCalendar.status.in_(statuses))
    # En son aktivite tarihi >= cutoff
    query = query.where(
        or_(
            DividendCalendar.ykk_date >= cutoff_d,
            DividendCalendar.general_assembly_date >= cutoff_d,
            DividendCalendar.payment_date >= cutoff_d,
            DividendCalendar.rejected_at >= cutoff_dt,
            DividendCalendar.created_at >= cutoff_dt,
        )
    ).order_by(desc(DividendCalendar.updated_at), desc(DividendCalendar.created_at)).limit(limit)

    rows = (await db.execute(query)).scalars().all()

    def _type_for_status(st: str) -> str:
        if st in ("odeniyor", "tamamlandi"): return "odendi"
        if st == "reddedildi": return "dagitmama"
        return "karar"

    def _title_for(st: str) -> str:
        return {
            "ykk_alindi": "Kâr Payı Dağıtım Kararı (YKK)",
            "genel_kurul_onayli": "Genel Kurul Temettü Onayı",
            "tarih_belli": "Temettü Ödeme Tarihi Belli",
            "odeniyor": "Temettü Ödemesi Bugün",
            "tamamlandi": "Temettü Ödendi",
            "reddedildi": "Temettü Dağıtmama Kararı",
        }.get(st, "Temettü Bildirimi")

    def _latest_event_iso(r) -> str | None:
        # En son hangi aktivite tarihi
        candidates = [
            (r.payment_date, "payment_date"),
            (r.general_assembly_date, "general_assembly_date"),
            (r.ykk_date, "ykk_date"),
        ]
        candidates = [(d, k) for d, k in candidates if d is not None]
        if r.rejected_at is not None:
            return r.rejected_at.isoformat()
        if not candidates:
            return r.created_at.isoformat() if r.created_at else None
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][0].isoformat()

    def _kap_url_for(r) -> str | None:
        if r.status in ("odeniyor", "tamamlandi") and r.payment_kap_url:
            return r.payment_kap_url
        if r.status == "reddedildi" and r.rejection_kap_url:
            return r.rejection_kap_url
        if r.status == "genel_kurul_onayli" and r.general_assembly_kap_url:
            return r.general_assembly_kap_url
        return r.ykk_kap_url or r.general_assembly_kap_url or r.payment_kap_url

    # En son aktivite KAP disclosure_id'lerini topla → AI verilerini join et
    def _latest_disclosure_id(r) -> int | None:
        if r.status in ("odeniyor", "tamamlandi") and r.payment_kap_disclosure_id:
            return r.payment_kap_disclosure_id
        if r.status == "reddedildi" and r.rejection_kap_disclosure_id:
            return r.rejection_kap_disclosure_id
        if r.status == "genel_kurul_onayli" and r.general_assembly_kap_disclosure_id:
            return r.general_assembly_kap_disclosure_id
        return r.ykk_kap_disclosure_id or r.general_assembly_kap_disclosure_id or r.payment_kap_disclosure_id

    disclosure_ids = [d for d in (_latest_disclosure_id(r) for r in rows) if d is not None]
    ai_map: dict[int, dict] = {}
    if disclosure_ids:
        ai_rows = (await db.execute(
            select(KapAllDisclosure).where(KapAllDisclosure.id.in_(disclosure_ids))
        )).scalars().all()
        for k in ai_rows:
            ai_map[k.id] = {
                "ai_summary": (k.ai_summary or "")[:300] if k.ai_summary else None,
                "ai_sentiment": k.ai_sentiment,
                "ai_impact_score": float(k.ai_impact_score) if k.ai_impact_score is not None else None,
            }

    items = []
    for r in rows:
        did = _latest_disclosure_id(r)
        ai = ai_map.get(did or -1) or {"ai_summary": None, "ai_sentiment": None, "ai_impact_score": None}
        # Gerçek KAP "Özet Bilgi" başlığı varsa onu kullan, yoksa generic label
        actual_title = getattr(r, "source_title", None) or _title_for(r.status or "")
        # payment_type yoksa amount'a göre tahmin et (geri uyumluluk)
        pt = getattr(r, "payment_type", None)
        if not pt and r.status != "reddedildi":
            if r.gross_amount_per_share or r.net_amount_per_share:
                pt = "cash"
        items.append({
            "ticker": r.ticker,
            "company_name": r.company_name,
            "title": actual_title,
            "category_label": _title_for(r.status or ""),  # frontend isterse kategori adı için
            "type": _type_for_status(r.status or ""),
            "status": r.status,
            "published_at": _latest_event_iso(r),
            "kap_url": _kap_url_for(r),
            "period": r.period,
            "gross_amount_per_share": float(r.gross_amount_per_share) if r.gross_amount_per_share else None,
            "net_amount_per_share": float(r.net_amount_per_share) if r.net_amount_per_share else None,
            "gross_yield_pct": float(r.gross_yield_pct) if r.gross_yield_pct else None,
            "net_yield_pct": float(r.net_yield_pct) if r.net_yield_pct else None,
            "total_amount_tl": float(r.total_amount_tl) if r.total_amount_tl else None,
            "payment_type": pt,
            "stock_ratio_text": getattr(r, "stock_ratio_text", None),
            "payment_date": r.payment_date.isoformat() if r.payment_date else None,
            "ai_summary": ai["ai_summary"],
            "ai_sentiment": ai["ai_sentiment"],
            "ai_impact_score": ai["ai_impact_score"],
        })

    # Son bildirim → en eski sıralaması (latest_event_iso DESC)
    # SQL updated_at sıralaması yetersiz çünkü backfill eski kayıtlari yenileyip basa cikarir.
    # Burada gercek olay tarihine gore yeniden sirala.
    items.sort(key=lambda x: x.get("published_at") or x.get("payment_date") or "", reverse=True)

    return {
        "filter": filter,
        "days": days,
        "count": len(items),
        "items": items,
    }


@app.get("/api/v1/temettu-takvim")
async def get_temettu_takvim(
    year: int = Query(2026, ge=2010, le=2030),
    status: str = Query("all", regex="^(all|yaklasan|odendi)$"),
    db: AsyncSession = Depends(get_db),
):
    """Temettu takvimi — temettu.app tarzi yaklasan/odenen temettüler.

    Status mantigi:
      - dividend_calendar.status='tamamlandi' (KAP odeme bildirimi geldi) → odendi
      - Aksi takdirde: payment_date < today → odendi (varsayim, KAP bildirimi
        gelmemis olabilir ama gun gecti)
      - Bugun veya gelecek → yaklasan (KAP bildirimi olmadan ödendi yazma)
    """
    from datetime import date as dt_date
    from app.models.dividend_calendar import DividendCalendar

    today = dt_date.today()

    result = await db.execute(
        select(DividendHistory)
        .where(DividendHistory.payment_year == year)
        .order_by(desc(DividendHistory.payment_date))
    )
    all_items = list(result.scalars().all())

    # KAP'tan ödeme tamamlandi onayi gelen ticker+date kombinasyonlarini cek
    cal_result = await db.execute(
        select(DividendCalendar.ticker, DividendCalendar.payment_date)
        .where(DividendCalendar.status == "tamamlandi", DividendCalendar.payment_date.isnot(None))
    )
    confirmed_paid = {(r[0].upper(), r[1]) for r in cal_result.all()}

    takvim = []
    for h in all_items:
        try:
            payment_dt = None
            if h.payment_date:
                if isinstance(h.payment_date, str):
                    payment_dt = datetime.strptime(h.payment_date, "%Y-%m-%d").date()
                else:
                    payment_dt = h.payment_date

            # KAP onayli odendi mi? (en kesin)
            is_kap_confirmed = payment_dt and (h.ticker.upper(), payment_dt) in confirmed_paid

            if is_kap_confirmed:
                item_status = "odendi"
            elif payment_dt and payment_dt < today:
                # Tarih gecti, KAP bildirimi gelmemis — gun bitti varsayalim
                item_status = "odendi"
            else:
                # Bugun veya gelecek (KAP onayi olmadan 'odendi' yazma)
                item_status = "yaklasan"

            if status != "all" and item_status != status:
                continue

            takvim.append({
                "ticker": h.ticker,
                "year": h.payment_year,
                "gross_per_share": float(h.gross_dividend_per_share) if h.gross_dividend_per_share else None,
                "net_per_share": float(h.net_dividend_per_share) if h.net_dividend_per_share else None,
                "yield_pct": float(h.dividend_yield_pct) if h.dividend_yield_pct else None,
                "payment_date": str(h.payment_date) if h.payment_date else None,
                "ex_dividend_date": str(h.ex_dividend_date) if hasattr(h, 'ex_dividend_date') and h.ex_dividend_date else None,
                "status": item_status,
            })
        except Exception:
            continue

    # Istatistikler
    yaklasan_count = sum(1 for t in takvim if t["status"] == "yaklasan")
    odenen_count = sum(1 for t in takvim if t["status"] == "odendi")

    return {
        "year": year,
        "stats": {
            "toplam": len(takvim),
            "yaklasan": yaklasan_count,
            "odenen": odenen_count,
        },
        "items": takvim,
    }


# -------------------------------------------------------
# ŞİRKET KARTI (v3.0.0) — Fintables tarzı tek endpoint
# -------------------------------------------------------

@app.get("/api/v1/sirket-karti/{ticker}")
async def get_sirket_karti(ticker: str, db: AsyncSession = Depends(get_db)):
    """Şirket kartı — fiyat + bilanço + temettü + AI analiz birleşik endpoint.

    15dk gecikmeli fiyat (DailyStockMarketStat) + son bilanço + temettü beklentisi.
    Tüm BIST hisseleri için çalışır — AAGYO'dan itibaren.
    """
    t = ticker.upper()

    # 1. Son fiyat (DailyStockMarketStat — en son işlem günü)
    price_result = await db.execute(
        select(DailyStockMarketStat)
        .where(DailyStockMarketStat.ticker == t)
        .order_by(desc(DailyStockMarketStat.date))
        .limit(1)
    )
    price_row = price_result.scalar_one_or_none()

    # 2. Son 2 dönem bilanço (karşılaştırma için)
    fin_result = await db.execute(
        select(CompanyFinancial)
        .where(CompanyFinancial.ticker == t)
        .order_by(desc(CompanyFinancial.period))
        .limit(2)
    )
    financials = list(fin_result.scalars().all())

    # 3. Finansal oranlar (F/K, PD/DD)
    ratio_result = await db.execute(
        select(FinancialRatio)
        .where(FinancialRatio.ticker == t)
        .order_by(desc(FinancialRatio.date))
        .limit(1)
    )
    ratio = ratio_result.scalar_one_or_none()

    # 4. Temettü beklentisi
    div_result = await db.execute(
        select(Dividend).where(Dividend.ticker == t)
    )
    dividend = div_result.scalar_one_or_none()

    # 5. Son 5 KAP bildirimi
    kap_result = await db.execute(
        select(KapAllDisclosure)
        .where(KapAllDisclosure.company_code == t)
        .order_by(desc(KapAllDisclosure.published_at))
        .limit(5)
    )
    kap_news = list(kap_result.scalars().all())

    # 6. Çeyreklik veriler (son 8 çeyrek — 2 yıl)
    quarterly_result = await db.execute(
        select(CompanyFinancial)
        .where(CompanyFinancial.ticker == t)
        .order_by(desc(CompanyFinancial.period))
        .limit(8)
    )
    quarterly = list(quarterly_result.scalars().all())

    # 7a. Temel analiz (yerel Excel sync ile beslenen ek veriler)
    from app.models.temel_analiz import TemelAnaliz
    temel_result = await db.execute(
        select(TemelAnaliz).where(TemelAnaliz.ticker == t)
    )
    temel = temel_result.scalar_one_or_none()

    # 7. Fiyat geçmişi — BIST lisans uyumu nedeniyle DailyStockMarketStat'tan
    # close_price kaldirildi. Bos liste donuyoruz; ileride alternatif kaynak
    # (Yahoo gecmis vs.) eklenebilir.
    price_history: list[float] = []

    if not price_row and not financials and not dividend:
        raise HTTPException(status_code=404, detail=f"Şirket bulunamadı: {ticker}")

    # Fiyat — BIST lisans uyumu: close_price ve canli fiyat cekilmiyor.
    # close=None doner; frontend bu durumu ele alir (fiyat yerine — gosterir).
    fallback_price: float | None = None

    latest_fin = financials[0] if financials else None
    prev_fin = financials[1] if len(financials) > 1 else None

    def _pct(curr, prev):
        if curr is None or prev is None or float(prev) == 0:
            return None
        return round((float(curr) - float(prev)) / abs(float(prev)) * 100, 1)

    return {
        "ticker": t,
        "price": {
            # close_price BIST lisansi nedeniyle kaldirildi — Yahoo fallback kullan
            "close": fallback_price,
            "change_pct": float(price_row.percent_change) if price_row else None,
            "date": str(price_row.date) if price_row else (date.today().isoformat() if fallback_price else None),
            "is_ceiling": price_row.is_ceiling if price_row else False,
            "is_floor": price_row.is_floor if price_row else False,
        },
        "price_history": price_history[-30:],  # Son 30 gün
        "ratios": {
            "fk": float(ratio.fk) if ratio and ratio.fk else None,
            "pddd": float(ratio.pddd) if ratio and ratio.pddd else None,
            "fd_favok": float(ratio.fd_favok) if ratio and ratio.fd_favok else None,
            "piyasa_degeri": float(ratio.piyasa_degeri) if ratio and ratio.piyasa_degeri else None,
        } if ratio else None,
        # Temel analiz (yerel Excel sync) — BIST lisans icin dolasim_lot/piyasa_degeri/fk kaldirildi
        "temel_analiz": {
            "sektor": temel.sektor if temel else None,
            "ozsermaye": float(temel.ozsermaye) if temel and temel.ozsermaye else 0,
            "yat_fon_oran": float(temel.yat_fon_oran) if temel and temel.yat_fon_oran else 0,
            "emeklilik_fon_oran": float(temel.emeklilik_fon_oran) if temel and temel.emeklilik_fon_oran else 0,
            "defter_degeri": float(temel.defter_degeri) if temel and temel.defter_degeri else 0,
            "pddd": float(temel.pddd) if temel and temel.pddd else 0,
            "fd_favok": float(temel.fd_favok) if temel and temel.fd_favok else 0,
            "pd_efk": float(temel.pd_efk) if temel and temel.pd_efk else 0,
            "ihracat_yuzdesi": float(temel.ihracat_yuzdesi) if temel and temel.ihracat_yuzdesi else 0,
            "updated_at": temel.updated_at.isoformat() if temel and temel.updated_at else None,
        } if temel else None,
        "financials": {
            "period": latest_fin.period if latest_fin else None,
            "prev_period": prev_fin.period if prev_fin else None,
            "revenue": float(latest_fin.revenue) if latest_fin and latest_fin.revenue else None,
            "revenue_prev": float(prev_fin.revenue) if prev_fin and prev_fin.revenue else None,
            "revenue_change_pct": _pct(latest_fin.revenue if latest_fin else None, prev_fin.revenue if prev_fin else None),
            "net_income": float(latest_fin.net_income) if latest_fin and latest_fin.net_income else None,
            "net_income_prev": float(prev_fin.net_income) if prev_fin and prev_fin.net_income else None,
            "net_income_change_pct": _pct(latest_fin.net_income if latest_fin else None, prev_fin.net_income if prev_fin else None),
            "ebitda": float(latest_fin.ebitda) if latest_fin and latest_fin.ebitda else None,
            "total_assets": float(latest_fin.total_assets) if latest_fin and latest_fin.total_assets else None,
            "total_equity": float(latest_fin.total_equity) if latest_fin and latest_fin.total_equity else None,
            "net_debt": float(latest_fin.net_debt) if latest_fin and latest_fin.net_debt else None,
            "gross_margin_pct": float(latest_fin.gross_margin_pct) if latest_fin and latest_fin.gross_margin_pct else None,
            "net_margin_pct": float(latest_fin.net_margin_pct) if latest_fin and latest_fin.net_margin_pct else None,
            "roe_pct": float(latest_fin.roe_pct) if latest_fin and latest_fin.roe_pct else None,
        } if latest_fin else None,
        "quarterly": [
            {
                "period": q.period,
                "revenue": float(q.revenue) if q.revenue else None,
                "ebitda": float(q.ebitda) if q.ebitda else None,
                "net_income": float(q.net_income) if q.net_income else None,
            }
            for q in reversed(quarterly)
        ],
        "dividend": {
            "expected_yield_pct": float(dividend.expected_dividend_yield_pct) if dividend and dividend.expected_dividend_yield_pct else None,
            "last_year_yield_pct": float(dividend.last_year_yield_pct) if dividend and dividend.last_year_yield_pct else None,
            "fk": float(dividend.fk) if dividend and dividend.fk else None,
        } if dividend else None,
        "kap_news": [
            {
                "title": n.title,
                "category": n.category,
                "published_at": n.published_at.isoformat() if n.published_at else None,
                "ai_sentiment": n.ai_sentiment,
                "ai_summary": n.ai_summary[:150] if n.ai_summary else None,
            }
            for n in kap_news
        ],
    }


@app.get("/api/v1/hisseler")
async def list_hisseler(
    q: str = Query("", min_length=0, max_length=20),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Hisse arama — şirket kartı için ticker listesi.

    Tüm BIST hisseleri aranabilir. q parametresi ile filtre.
    """
    from sqlalchemy import func, distinct

    # DailyStockMarketStat'tan unique ticker'lar
    query = select(distinct(DailyStockMarketStat.ticker))

    if q:
        query = query.where(DailyStockMarketStat.ticker.ilike(f"%{q.upper()}%"))

    query = query.order_by(DailyStockMarketStat.ticker).limit(limit)
    result = await db.execute(query)
    tickers = [row[0] for row in result.all()]

    return {"tickers": tickers, "count": len(tickers)}


# -------------------------------------------------------
# HALKA ARZ ANKETI (v3.0.0)
# -------------------------------------------------------

@app.post("/api/v1/ipos/{ipo_id}/vote")
async def vote_on_ipo(
    ipo_id: int,
    payload: IPOVoteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Halka arza katilim anketi — device_id veya IP ile tek oy."""
    device_id = payload.device_id or ""
    client_ip = request.client.host if request.client else ""

    # Daha once oy verdi mi?
    existing = await db.execute(
        select(IPOVote).where(
            IPOVote.ipo_id == ipo_id,
            (IPOVote.device_id == device_id) if device_id else (IPOVote.ip_address == client_ip),
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Bu ankete zaten oy verdiniz")

    new_vote = IPOVote(
        ipo_id=ipo_id,
        device_id=device_id or None,
        ip_address=client_ip,
        vote=payload.vote,
    )
    db.add(new_vote)
    await db.commit()

    return {"status": "ok", "message": "Oyunuz kaydedildi"}


@app.get("/api/v1/ipos/{ipo_id}/vote-results", response_model=IPOVoteResultOut)
async def get_ipo_vote_results(ipo_id: int, db: AsyncSession = Depends(get_db)):
    """Halka arz oylama sonuclari."""
    from sqlalchemy import func

    result = await db.execute(
        select(
            func.count(IPOVote.id).label("total"),
            func.count(func.nullif(IPOVote.vote != "participate", True)).label("participate"),
            func.count(func.nullif(IPOVote.vote != "skip", True)).label("skip_count"),
        ).where(IPOVote.ipo_id == ipo_id)
    )
    row = result.one()
    total = row.total or 0
    participate = row.participate or 0
    skip = row.skip_count or 0

    return IPOVoteResultOut(
        ipo_id=ipo_id,
        total_votes=total,
        participate_count=participate,
        skip_count=skip,
        participate_pct=round(participate / total * 100, 1) if total > 0 else 0,
        skip_pct=round(skip / total * 100, 1) if total > 0 else 0,
    )


# -------------------------------------------------------
# BILANCO TAKVIMI (v3.0.0) — gcmyatirim scraper'dan
# -------------------------------------------------------

@app.get("/api/v1/bilanco-takvim", response_model=list[EarningsCalendarOut])
async def get_earnings_calendar(
    days_ahead: int = Query(60, ge=1, le=180),
    only_pending: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    """Bilanco takvimi — onumuzdeki N gun beklenen aciklamalar."""
    from datetime import date as dt_date, timedelta as _td
    today = dt_date.today()
    end = today + _td(days=days_ahead)

    query = select(EarningsCalendar).where(
        EarningsCalendar.expected_date >= today - _td(days=7),
        EarningsCalendar.expected_date <= end,
    )
    if only_pending:
        query = query.where(EarningsCalendar.is_announced == False)
    query = query.order_by(EarningsCalendar.expected_date.asc())

    result = await db.execute(query)
    return list(result.scalars().all())


# -------------------------------------------------------
# ADMIN — v3 manuel tetikleyiciler
# -------------------------------------------------------

@app.post("/api/v1/admin/trigger-isyatirim-scrape")
@limiter.limit("3/minute")
async def admin_trigger_isyatirim(request: Request, payload: dict = Body(...)):
    """IsYatirim batch bilanco scrape — tum BIST hisseleri (700+).

    weekly_bilanco_update fonksiyonu kullanir: 11 yil veri, ~3-4 saat surer.
    Background'da calisir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.services.bilanco_pipeline import weekly_bilanco_update
        import asyncio as _asyncio
        _asyncio.create_task(weekly_bilanco_update())
        return {"status": "ok", "message": "Isyatirim batch baslatildi (~3-4 saat)"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/trigger-mynet-ratios")
@limiter.limit("3/minute")
async def admin_trigger_mynet_ratios(request: Request, payload: dict = Body(...)):
    """Mynet finans oranlari batch — F/K, PD/DD, FD/FAVOK, Piyasa Degeri (~5 dk)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.scrapers.mynet_ratios_scraper import scrape_all_ratios
        import asyncio as _asyncio
        _asyncio.create_task(scrape_all_ratios())
        return {"status": "ok", "message": "Mynet oranlari scrape baslatildi (~5 dk)"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/trigger-temettu-scrape")
@limiter.limit("3/minute")
async def admin_trigger_temettu(request: Request, payload: dict = Body(...)):
    """temettuhisseleri.com scraper — temettu zenginlestirme."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.scrapers.temettuhisseleri_scraper import scrape_temettuhisseleri
        import asyncio as _asyncio
        _asyncio.create_task(scrape_temettuhisseleri())
        return {"status": "ok", "message": "Temettu scrape baslatildi (background)"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/cleanup-dividend-duplicates")
@limiter.limit("3/minute")
async def admin_cleanup_dividend_duplicates(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """dividend_history'de ayni ticker+year icin payment_date=NULL VE
    payment_date=BELIRLI olan kayitlar varsa, NULL olani siler.

    Eski scraper bug'i nedeniyle olusan duplicate kayitlari temizler.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from sqlalchemy import text as sa_text
        result = await db.execute(sa_text("""
            DELETE FROM dividend_history
            WHERE id IN (
                SELECT n.id
                FROM dividend_history n
                WHERE n.payment_date IS NULL
                  AND EXISTS (
                    SELECT 1 FROM dividend_history d
                    WHERE d.ticker = n.ticker
                      AND d.payment_year = n.payment_year
                      AND d.payment_date IS NOT NULL
                  )
            )
        """))
        await db.commit()
        return {"status": "ok", "deleted": result.rowcount}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


@app.post("/api/v1/admin/trigger-earnings-calendar")
@limiter.limit("3/minute")
async def admin_trigger_earnings_calendar(request: Request, payload: dict = Body(...)):
    """gcmyatirim bilanco takvimi scraper."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    try:
        from app.scrapers.gcm_earnings_calendar_scraper import scrape_earnings_calendar
        result = await scrape_earnings_calendar()
        return {"status": "ok", **result}
    except Exception as e:
        return {"status": "error", "message": str(e)[:500]}


# -------------------------------------------------------
# PORTFÖY SCREENSHOT PARSE (Claude Vision)
# -------------------------------------------------------

@app.post("/api/v1/portfolio/parse-screenshot")
@limiter.limit("10/minute")
async def parse_portfolio_screenshot_endpoint(request: Request, payload: dict = Body(...)):
    """Banka mobil ekran görüntüsünden hisse listesi çıkar (Claude Vision).

    Request:
      { "image_base64": "...", "media_type": "image/jpeg" }

    Response:
      { "stocks": [{ticker, lots, avgCost}], "confidence": "high|medium|low", "notes": "..." }
    """
    image_b64 = payload.get("image_base64", "")
    media_type = payload.get("media_type", "image/jpeg")

    if not image_b64:
        raise HTTPException(status_code=400, detail="image_base64 gerekli")
    if media_type not in ("image/jpeg", "image/png", "image/webp"):
        raise HTTPException(status_code=400, detail="Desteklenmeyen format (jpeg/png/webp)")
    # Boyut sınırı — yaklaşık 10MB base64 ~7.5MB image
    if len(image_b64) > 14_000_000:
        raise HTTPException(status_code=413, detail="Görüntü çok büyük (max ~10MB)")

    try:
        from app.services.ai_portfolio_screenshot import parse_portfolio_screenshot
        result = await parse_portfolio_screenshot(image_b64, media_type)
        if not result:
            return {"stocks": [], "confidence": "low", "notes": "AI yanıt vermedi"}
        return result
    except Exception as e:
        return {"stocks": [], "confidence": "low", "notes": f"Hata: {str(e)[:200]}"}


# -------------------------------------------------------
# PERSONALIZED FEED (v3.0.0) — kullanicinin hisselerine ozel
# Ana sayfa marquee widget icin: KAP haberler birlestirilmis akis
# -------------------------------------------------------

@app.get("/api/v1/feed/personalized")
async def get_personalized_feed(
    tickers: str = Query("", description="Virgulle ayrilmis hisse kodlari (orn: TUPRS,ASELS)"),
    limit: int = Query(10, ge=1, le=30),
    db: AsyncSession = Depends(get_db),
):
    """Kullanicinin portfoy + watchlist hisselerine ait son haberler.

    Su an sadece KapAllDisclosure tablosundan ceker (KAP haberleri, GK kararlari,
    temettu aciklamalar, bilancolar — kategori alaninda mevcut).
    """
    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not ticker_list:
        return {"items": []}

    result = await db.execute(
        select(KapAllDisclosure)
        .where(KapAllDisclosure.company_code.in_(ticker_list))
        .order_by(KapAllDisclosure.published_at.desc().nullslast())
        .limit(limit)
    )
    items = []
    for d in result.scalars().all():
        items.append({
            "id": f"kap-{d.id}",
            "kind": "kap",
            "ticker": d.company_code,
            "title": d.title,
            "category": d.category,
            "is_bilanco": d.is_bilanco,
            "sentiment": d.ai_sentiment,
            "ai_summary": d.ai_summary,
            "published_at": d.published_at.isoformat() if d.published_at else None,
            "url": d.kap_url,
        })
    return {"items": items}


# ═════════════════════════════════════════════════════════════════════════════
# KAP Disclosure Manuel Tetikleyici (admin)
# Belirli bir KAP URL'sini extractor + uygun processor'a yönlendirir.
# Test/backfill için.
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/api/v1/admin/cleanup-share-tx-duplicates")
@limiter.limit("10/minute")
async def admin_cleanup_share_tx_duplicates(request: Request, payload: dict = Body(...)):
    """Admin: share_transaction_details'te '?'/'Bilinmiyor' party_name'li duplicate kayitlari sil.

    Ayni (ticker, transaction_date) icin saglikli kayit varsa, "?" / "Bilinmiyor" / NULL party'li
    olanlari siler. Saglikli kayit yoksa hicbirini silmez.

    Body: { "admin_password": "..." }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    try:
      async with async_session() as db:
        # Bad kayit: '?' / 'Bilinmiyor' / NULL party_name VEYA nominal_lot=0/NULL
        # Sil kosulu: ya ayni kap_url'da saglikli kayit var, ya da ayni (ticker,date)'te saglikli kayit var
        ids_result = await db.execute(sa_text("""
            SELECT bad.id, bad.ticker, bad.transaction_date, bad.kap_url, bad.party_name, bad.source
            FROM share_transaction_details bad
            WHERE (bad.party_name IS NULL
                   OR bad.party_name IN ('?', 'Bilinmiyor', '')
                   OR COALESCE(bad.nominal_lot, 0) = 0)
              AND (
                  EXISTS (
                      SELECT 1 FROM share_transaction_details g1
                      WHERE g1.kap_url = bad.kap_url
                        AND g1.id <> bad.id
                        AND g1.party_name IS NOT NULL
                        AND g1.party_name NOT IN ('?', 'Bilinmiyor', '')
                        AND COALESCE(g1.nominal_lot, 0) > 0
                  )
                  OR EXISTS (
                      SELECT 1 FROM share_transaction_details g2
                      WHERE g2.ticker = bad.ticker
                        AND g2.transaction_date = bad.transaction_date
                        AND g2.id <> bad.id
                        AND g2.party_name IS NOT NULL
                        AND g2.party_name NOT IN ('?', 'Bilinmiyor', '')
                        AND COALESCE(g2.nominal_lot, 0) > 0
                  )
              )
        """))
        bad_rows = ids_result.fetchall()
        bad_ids = [int(r[0]) for r in bad_rows]
        deleted_count = 0
        for bid in bad_ids:
            await db.execute(
                sa_text("DELETE FROM share_transaction_details WHERE id = :id"),
                {"id": bid},
            )
            deleted_count += 1
        if deleted_count:
            await db.commit()
      return {
        "deleted_count": deleted_count,
        "deleted": [{"id": r[0], "ticker": r[1], "date": str(r[2]), "kap": r[3], "party": r[4], "source": r[5]} for r in bad_rows[:80]],
      }
    except Exception as e:
      import traceback
      logger.error("cleanup-share-tx-duplicates failed: %s\n%s", e, traceback.format_exc())
      return {"error": str(e)[:500], "trace": traceback.format_exc()[:1500]}


@app.post("/api/v1/admin/delete-capital-increases-by-tickers")
@limiter.limit("3/minute")
async def admin_delete_cap_inc_by_tickers(request: Request, payload: dict = Body(...)):
    """Admin: belirli ticker'larin capital_increases kayitlarini sil (advance icin)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    tickers = payload.get("tickers") or []
    if not isinstance(tickers, list) or not tickers:
        raise HTTPException(status_code=400, detail="tickers list bos")
    tickers_up = [str(t).upper().strip() for t in tickers if t]
    from sqlalchemy import text as sa_text
    from app.database import async_session
    async with async_session() as db:
        result = await db.execute(
            sa_text("DELETE FROM capital_increases WHERE ticker = ANY(:tks) RETURNING id"),
            {"tks": tickers_up},
        )
        deleted = len(result.fetchall())
        await db.commit()
    return {"deleted": deleted, "tickers": tickers_up}


@app.post("/api/v1/admin/migrate-capital-increases-schema")
@limiter.limit("3/minute")
async def admin_migrate_capital_increases(request: Request, payload: dict = Body(...)):
    """Admin: capital_increases tablosuna yeni 4 column ekle (idempotent)."""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    stmts = [
        "ALTER TABLE capital_increases ADD COLUMN IF NOT EXISTS bedelli_pct DOUBLE PRECISION",
        "ALTER TABLE capital_increases ADD COLUMN IF NOT EXISTS bedelsiz_pct DOUBLE PRECISION",
        "ALTER TABLE capital_increases ADD COLUMN IF NOT EXISTS tahsisli_pct DOUBLE PRECISION",
        "ALTER TABLE capital_increases ADD COLUMN IF NOT EXISTS bolunme_sonrasi_sermaye_tl DOUBLE PRECISION",
        # ykk_date NULL olabilsin (unique constraint icin sorun olmasin diye)
        "ALTER TABLE capital_increases ALTER COLUMN ykk_date DROP NOT NULL",
        # Eski unique constraint problem olabilir — ticker tek anahtar
        "ALTER TABLE capital_increases DROP CONSTRAINT IF EXISTS uq_cap_inc_ticker_type_ykk",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_cap_inc_ticker_status ON capital_increases (ticker, status) WHERE status IN ('ykk_alindi','spk_onayli','tarih_belli','dagitiliyor')",
    ]
    results = []
    async with async_session() as db:
        for s in stmts:
            try:
                await db.execute(sa_text(s))
                results.append({"sql": s[:60], "ok": True})
            except Exception as e:
                results.append({"sql": s[:60], "ok": False, "err": str(e)[:200]})
        await db.commit()
    return {"applied": results}


@app.post("/api/v1/admin/seed-capital-increases")
@limiter.limit("3/minute")
async def admin_seed_capital_increases(request: Request, payload: dict = Body(...)):
    """Admin: capital_increases bulk seed.

    Body: {
      admin_password,
      status: "ykk_alindi" | ...,
      records: [
        { ticker, company_name?, bolunme_sonrasi_sermaye_tl?, bedelli_pct?, bedelsiz_pct?, tahsisli_pct?, ykk_date?, spk_approval_date?, distribution_date? }
      ]
    }
    Type field, dolu olan oran'a gore otomatik atanir (mixed kayitta primary tip).
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    records = payload.get("records") or []
    status_in = payload.get("status") or "ykk_alindi"
    if status_in not in ("ykk_alindi", "spk_onayli", "tarih_belli", "dagitiliyor", "tamamlandi", "reddedildi"):
        raise HTTPException(status_code=400, detail="status gecersiz")
    if not isinstance(records, list) or not records:
        raise HTTPException(status_code=400, detail="records bos")

    from datetime import date as _date
    def _parse_date(s):
        if not s: return None
        if hasattr(s, "year"): return s
        try:
            parts = str(s).split("-")
            return _date(int(parts[0]), int(parts[1]), int(parts[2]))
        except Exception:
            return None

    inserted = 0
    skipped = 0
    errors: list = []
    async with async_session() as db:
        for r in records:
            ticker = (r.get("ticker") or "").upper().strip()
            if not ticker or len(ticker) > 10:
                skipped += 1
                continue
            bedelli = r.get("bedelli_pct")
            bedelsiz = r.get("bedelsiz_pct")
            tahsisli = r.get("tahsisli_pct")
            # Primary type — en buyuk oran hangisindeyse o
            type_options = [(bedelli or 0, "bedelli"), (bedelsiz or 0, "bedelsiz"), (tahsisli or 0, "tahsisli")]
            type_options.sort(key=lambda x: -x[0])
            primary_type = type_options[0][1] if type_options[0][0] > 0 else "bedelsiz"
            try:
                await db.execute(sa_text("""
                    INSERT INTO capital_increases (
                        ticker, company_name, type, status,
                        bedelli_pct, bedelsiz_pct, tahsisli_pct,
                        bolunme_sonrasi_sermaye_tl,
                        ykk_date, spk_approval_date, distribution_date,
                        created_at, updated_at
                    ) VALUES (
                        :ticker, :company_name, :type, :status,
                        :bedelli, :bedelsiz, :tahsisli,
                        :sermaye,
                        CAST(:ykk_date AS DATE), CAST(:spk_date AS DATE), CAST(:dist_date AS DATE),
                        NOW(), NOW()
                    )
                    ON CONFLICT DO NOTHING
                """), {
                    "ticker": ticker,
                    "company_name": r.get("company_name"),
                    "type": primary_type,
                    "status": status_in,
                    "bedelli": bedelli,
                    "bedelsiz": bedelsiz,
                    "tahsisli": tahsisli,
                    "sermaye": r.get("bolunme_sonrasi_sermaye_tl"),
                    "ykk_date": _parse_date(r.get("ykk_date")),
                    "spk_date": _parse_date(r.get("spk_approval_date")),
                    "dist_date": _parse_date(r.get("distribution_date")),
                })
                inserted += 1
            except Exception as e:
                skipped += 1
                if len(errors) < 3:
                    errors.append({"ticker": ticker, "err": str(e)[:300]})
        await db.commit()
    return {"inserted": inserted, "skipped": skipped, "errors": errors}


@app.post("/api/v1/admin/backfill-dividend-classify")
@limiter.limit("3/minute")
async def admin_backfill_dividend_classify(request: Request, payload: dict = Body(...)):
    """ykk_alindi statusunde olan dividend_calendar kayitlarini yeniden siniflandir.

    Her kayit icin:
      1. KAP body fetch
      2. rejection pattern kontrol -> status='reddedildi'
      3. degilse AI parse -> gross_amount, period, payment_date dolduru
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    limit = int(payload.get("limit") or 30)
    only_ticker = (payload.get("ticker") or "").upper().strip() or None
    from sqlalchemy import select as _sel, text as sa_text
    from app.database import async_session
    from app.models.dividend_calendar import DividendCalendar
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.dividend_calendar_processor import (
        classify_event_with_body, ai_parse_dividend, _PARSE_PROMPT,
    )
    from datetime import datetime as _dt2, timezone as _tz2, date as _date2

    def _to_date(s):
        if not s: return None
        if hasattr(s, "year"): return s
        try:
            parts = str(s).split("-")
            return _date2(int(parts[0]), int(parts[1]), int(parts[2]))
        except Exception: return None

    reclassified_rejected = 0
    parsed_filled = 0
    skipped = 0
    errors = []

    async with async_session() as db:
        q = _sel(DividendCalendar).where(DividendCalendar.status == "ykk_alindi").limit(limit)
        if only_ticker:
            q = _sel(DividendCalendar).where(
                DividendCalendar.ticker == only_ticker,
                DividendCalendar.status.in_(["ykk_alindi", "reddedildi", "genel_kurul_onayli", "tarih_belli", "odeniyor"]),
            ).limit(limit)
        rows = (await db.execute(q)).scalars().all()

        for r in rows:
            kap_url = r.ykk_kap_url or r.general_assembly_kap_url or r.payment_kap_url
            if not kap_url:
                skipped += 1
                continue
            try:
                disc = await fetch_kap_disclosure(kap_url)
                if not disc:
                    skipped += 1
                    continue
                body = disc.get("full_text") or ""
                title = disc.get("title") or ""
                ev = classify_event_with_body(title, body)
                if ev == "rejection":
                    r.status = "reddedildi"
                    r.rejected_at = _dt2.now(_tz2.utc)
                    r.rejection_kap_url = kap_url
                    reclassified_rejected += 1
                    continue
                # ykk veya genel parse - AI ile detaylari doldur
                parsed = await ai_parse_dividend(r.ticker, title, body)
                if parsed:
                    if parsed.get("gross_amount_per_share") and not r.gross_amount_per_share:
                        r.gross_amount_per_share = parsed.get("gross_amount_per_share")
                    if parsed.get("net_amount_per_share") and not r.net_amount_per_share:
                        r.net_amount_per_share = parsed.get("net_amount_per_share")
                    if parsed.get("gross_yield_pct") and not r.gross_yield_pct:
                        r.gross_yield_pct = parsed.get("gross_yield_pct")
                    if parsed.get("net_yield_pct") and not r.net_yield_pct:
                        r.net_yield_pct = parsed.get("net_yield_pct")
                    if parsed.get("total_amount_tl") and not r.total_amount_tl:
                        r.total_amount_tl = parsed.get("total_amount_tl")
                    if parsed.get("period") and not r.period:
                        r.period = parsed.get("period")
                    pd = _to_date(parsed.get("payment_date"))
                    if pd and not r.payment_date:
                        r.payment_date = pd
                    gad = _to_date(parsed.get("general_assembly_date"))
                    if gad and not r.general_assembly_date:
                        r.general_assembly_date = gad
                    parsed_filled += 1
            except Exception as e:
                errors.append({"ticker": r.ticker, "err": str(e)[:200]})
        await db.commit()

    return {
        "reclassified_rejected": reclassified_rejected,
        "parsed_filled": parsed_filled,
        "skipped": skipped,
        "errors": errors[:5],
        "total_processed": len(rows),
    }


@app.post("/api/v1/admin/wipe-capital-increases")
@limiter.limit("3/minute")
async def admin_wipe_capital_increases(request: Request, payload: dict = Body(...)):
    """Admin: capital_increases tablosundaki TUM kayitlari sil (yeniden tasarim icin).

    Body: { "admin_password": "...", "confirm": "WIPE_ALL" }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    if payload.get("confirm") != "WIPE_ALL":
        raise HTTPException(status_code=400, detail="confirm='WIPE_ALL' gerekli")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    async with async_session() as db:
        before = (await db.execute(sa_text("SELECT COUNT(*) FROM capital_increases"))).scalar() or 0
        await db.execute(sa_text("DELETE FROM capital_increases"))
        await db.commit()
        after = (await db.execute(sa_text("SELECT COUNT(*) FROM capital_increases"))).scalar() or 0
    return {"deleted": before, "remaining": after}


@app.post("/api/v1/admin/reparse-share-tx")
@limiter.limit("10/minute")
async def admin_reparse_share_tx(request: Request, payload: dict = Body(...)):
    """Mevcut share_transaction_details kaydini yeniden parse et (party_name fix vb.).

    Body: { admin_password, id }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    tx_id = int(payload.get("id") or 0)
    if not tx_id:
        return {"error": "id required"}
    from sqlalchemy import text as sa_text
    from app.database import async_session
    from app.services.kap_pay_alim_satim_fetcher import fetch_kap_pay_alim_satim
    async with async_session() as db:
        row = (await db.execute(sa_text("SELECT ticker, kap_url FROM share_transaction_details WHERE id=:id"), {"id": tx_id})).first()
        if not row:
            return {"error": "not found"}
        ticker, kap_url = row
        if not kap_url:
            return {"error": "kap_url yok"}
        parsed = await fetch_kap_pay_alim_satim(kap_url)
        if not parsed:
            return {"error": "KAP fetch fail"}
        # Header slug'tan party_name al
        new_party = None
        hdr = parsed.get("party_name_header")
        if hdr:
            import re as _re
            hdr_collapsed = _re.sub(r"[^A-ZÇĞİÖŞÜ]", "", hdr.upper())
            if ticker not in hdr_collapsed[:len(ticker)+3]:
                new_party = hdr
        # Yoksa body parse fallback'lerinden
        if not new_party:
            new_party = parsed.get("party_name")
        if not new_party:
            return {"error": "party_name bulunamadi", "header": hdr}
        await db.execute(sa_text("UPDATE share_transaction_details SET party_name=:pn WHERE id=:id"), {"pn": new_party, "id": tx_id})
        await db.commit()
    return {"id": tx_id, "ticker": ticker, "new_party_name": new_party}


@app.post("/api/v1/admin/backfill-share-tx-direction")
@limiter.limit("3/minute")
async def admin_backfill_share_tx_direction(request: Request, payload: dict = Body(...)):
    """Mevcut share_transaction_details kayitlarinin tx_type'ini oran degisimine gore yeniden hesapla.

    Mantik: pay_orani_change_pct (yoksa oy_hakki_change_pct) > 0 -> alis, < 0 -> satis.
    Net nominal: hem alim hem satim varsa zaten parser'da abs(net) yaziliyor, mevcut nominal_lot dokunulmuyor.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    async with async_session() as db:
        # COALESCE: pay_orani_change_pct yoksa oy_hakki_change_pct kullan
        result_alis = await db.execute(sa_text("""
            UPDATE share_transaction_details
            SET transaction_type = 'alis'
            WHERE COALESCE(pay_orani_change_pct, oy_hakki_change_pct) > 0
              AND transaction_type != 'alis'
            RETURNING id
        """))
        alis_updated = len(result_alis.fetchall())
        result_satis = await db.execute(sa_text("""
            UPDATE share_transaction_details
            SET transaction_type = 'satis'
            WHERE COALESCE(pay_orani_change_pct, oy_hakki_change_pct) < 0
              AND transaction_type != 'satis'
            RETURNING id
        """))
        satis_updated = len(result_satis.fetchall())
        await db.commit()
    return {"updated_to_alis": alis_updated, "updated_to_satis": satis_updated}


@app.post("/api/v1/admin/delete-share-tx")
@limiter.limit("20/minute")
async def admin_delete_share_tx(request: Request, payload: dict = Body(...)):
    """Admin: spesifik id'yi share_transaction_details'ten sil. Body: {admin_password, id}"""
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    tx_id = int(payload.get("id") or 0)
    if not tx_id:
        return {"error": "id required"}
    async with async_session() as db:
        await db.execute(sa_text("DELETE FROM share_transaction_details WHERE id=:id"), {"id": tx_id})
        await db.commit()
    return {"deleted_id": tx_id}


@app.post("/api/v1/admin/backfill-business-deals")
@limiter.limit("3/minute")
async def admin_backfill_business_deals(request: Request, payload: dict = Body(...)):
    """Admin: business_deals tablosundaki amount=NULL kayitlari yeniden parse et, DB'yi guncelle.

    Body: { "admin_password": "...", "limit": 50 }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz")
    from sqlalchemy import text as sa_text
    from app.database import async_session
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.services.business_deal_processor import ai_parse_business_deal
    limit = int(payload.get("limit") or 30)
    fixed = []
    skipped = []
    async with async_session() as db:
        result = await db.execute(sa_text("""
            SELECT id, ticker, kap_url, title FROM business_deals
            WHERE (amount_original IS NULL OR currency IS NULL)
              AND kap_url IS NOT NULL
            ORDER BY id DESC LIMIT :lim
        """), {"lim": limit})
        rows = result.fetchall()
        for row in rows:
            bd_id, ticker, kap_url, title = row[0], row[1], row[2], row[3]
            try:
                disc = await fetch_kap_disclosure(kap_url)
                body = (disc or {}).get("full_text") or ""
                if not body:
                    skipped.append({"id": bd_id, "reason": "no_body"})
                    continue
                parsed = await ai_parse_business_deal(ticker, title or "", body)
                if not parsed.get("amount_original"):
                    skipped.append({"id": bd_id, "ticker": ticker, "reason": "no_amount", "cp": parsed.get("counterparty")})
                    # Counterparty/summary bulunduysa onlari bile yaz
                    if parsed.get("counterparty") or parsed.get("summary"):
                        await db.execute(sa_text("""
                            UPDATE business_deals
                            SET counterparty=COALESCE(:cp, counterparty),
                                summary=COALESCE(:sm, summary)
                            WHERE id=:id
                        """), {"id": bd_id, "cp": parsed.get("counterparty"), "sm": parsed.get("summary")})
                    continue
                # Kur cevirisi
                amt = float(parsed["amount_original"])
                cur = parsed.get("currency") or "TRY"
                amt_try = amt
                if cur != "TRY":
                    try:
                        from app.services.business_deal_processor import get_exchange_rate
                        rate, _ = await get_exchange_rate(cur)
                        if rate:
                            amt_try = amt * rate
                    except Exception:
                        pass
                await db.execute(sa_text("""
                    UPDATE business_deals
                    SET amount_original=:amt, currency=:cur, amount_try=:amt_try,
                        counterparty=COALESCE(:cp, counterparty),
                        summary=COALESCE(:sm, summary)
                    WHERE id=:id
                """), {
                    "id": bd_id, "amt": amt, "cur": cur, "amt_try": amt_try,
                    "cp": parsed.get("counterparty"), "sm": parsed.get("summary"),
                })
                fixed.append({"id": bd_id, "ticker": ticker, "amount": amt, "cur": cur, "cp": parsed.get("counterparty")})
            except Exception as e:
                skipped.append({"id": bd_id, "error": str(e)[:200]})
        await db.commit()
    return {"fixed_count": len(fixed), "skipped_count": len(skipped), "fixed": fixed[:30], "skipped": skipped[:30]}


@app.post("/api/v1/admin/process-kap-disclosure")
@limiter.limit("30/minute")
async def admin_process_kap_disclosure(request: Request, payload: dict = Body(...)):
    """Admin: KAP URL'sini extractor + processors'a gönder.

    Body: {
      "admin_password": "...",
      "kap_url": "https://www.kap.org.tr/tr/Bildirim/1600207",
      "ticker": "ALARK"   # opsiyonel hint
    }

    Çalışan processors (auto-detect):
      - business_deal (regex amount extract)
      - dividend_payment (Pay Başına Brüt Temettü pattern)
      - mkk_capital_realization (bedelsiz gerçekleşme)
      - bilanco (XBRL parse)
      - pay_alim_satim (KAP table)
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    kap_url = (payload.get("kap_url") or "").strip()
    ticker_hint = (payload.get("ticker") or "").upper().strip() or None

    if not kap_url or "kap.org.tr" not in kap_url:
        raise HTTPException(status_code=400, detail="Geçerli kap_url gerekli")

    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.database import async_session as _async_session
    from datetime import datetime as _dt, timezone as _tz

    result: dict = {"kap_url": kap_url, "steps": [], "processors": {}}

    # 1. Extractor — body
    disclosure = await fetch_kap_disclosure(kap_url)
    if not disclosure:
        raise HTTPException(status_code=502, detail="KAP fetch başarısız")
    body = disclosure.get("full_text", "")
    result["body_length"] = len(body)
    result["text_blocks"] = len(disclosure.get("text_blocks", []))
    result["tables"] = len(disclosure.get("tables", []))
    result["pdf_links"] = disclosure.get("pdf_links", [])
    result["body_preview"] = body[:300]

    if not ticker_hint:
        # body tablesinden ilgili şirket çek
        rel_match = re.search(r"İlgili\s+Şirketler[^\[]*\[([^\]]+)\]", body)
        if rel_match:
            tickers = [t.strip().upper() for t in rel_match.group(1).split(",") if t.strip()]
            if tickers:
                ticker_hint = tickers[0]

    # 2. Tüm processors'ı dene
    async with _async_session() as db:
        # 2a. business_deal — regex
        try:
            from app.services.business_deal_processor import regex_extract_business_deal
            bd_result = regex_extract_business_deal(body)
            result["processors"]["business_deal_regex"] = {
                k: v for k, v in bd_result.items() if v is not None
            }
        except Exception as e:
            result["processors"]["business_deal_regex"] = {"error": str(e)}

        # 2b. dividend_payment — regex + DB update
        try:
            from app.services.dividend_calendar_processor import (
                is_dividend_payment_announcement,
                process_dividend_payment_announcement,
                parse_dividend_payment_announcement,
            )
            parsed = parse_dividend_payment_announcement(body)
            if parsed:
                proc_result = await process_dividend_payment_announcement(
                    db, body=body, kap_url=kap_url,
                    disclosure_id=None,
                    published_at=_dt.now(_tz.utc),
                )
                result["processors"]["dividend_payment"] = {
                    "parsed_items": parsed,
                    "result": proc_result,
                }
            else:
                result["processors"]["dividend_payment"] = {"matched": False}
        except Exception as e:
            result["processors"]["dividend_payment"] = {"error": str(e)}

        # 2c. MKK realization — regex + DB update
        try:
            from app.services.capital_increase_processor import (
                is_mkk_capital_realization,
                process_mkk_capital_realization,
                parse_mkk_capital_realization,
            )
            parsed = parse_mkk_capital_realization(body)
            if parsed:
                proc_result = await process_mkk_capital_realization(
                    db, ticker_hint=ticker_hint, body=body,
                    kap_url=kap_url, disclosure_id=None,
                )
                result["processors"]["mkk_realization"] = {
                    "parsed": {
                        "percentage": parsed.get("percentage"),
                        "issuance_type": parsed.get("issuance_type"),
                        "realization_date": str(parsed.get("realization_date")) if parsed.get("realization_date") else None,
                    },
                    "result": proc_result,
                }
            else:
                result["processors"]["mkk_realization"] = {"matched": False}
        except Exception as e:
            result["processors"]["mkk_realization"] = {"error": str(e)}

        # 2d. bilanço — XBRL scrape + DB save
        try:
            from app.services.ai_bilanco_analyzer import parse_bilanco_from_kap, save_parsed_bilanco
            if ticker_hint:
                bil_parsed = await parse_bilanco_from_kap(ticker_hint, body)
                result["processors"]["bilanco"] = bil_parsed or {"parsed": None}
                if bil_parsed and (bil_parsed.get("revenue") or bil_parsed.get("total_assets")):
                    try:
                        await save_parsed_bilanco(ticker_hint, bil_parsed)
                        result["processors"]["bilanco"]["db_saved"] = True
                    except Exception as save_err:
                        result["processors"]["bilanco"]["save_error"] = str(save_err)[:200]
            else:
                result["processors"]["bilanco"] = {"skipped": "no_ticker_hint"}
        except Exception as e:
            result["processors"]["bilanco"] = {"error": str(e)}

        # 2e. pay_alim_satim — fetch + DB save
        try:
            from app.services.kap_pay_alim_satim_fetcher import (
                fetch_kap_pay_alim_satim, upsert_pay_alim_satim_from_kap,
            )
            from datetime import datetime as _dt2, timezone as _tz2
            # Title check — sadece "Pay Alım Satım Bildirimi" KAP'larina pay_alim_satim uygula
            # CCOLA gibi bilanco KAP'larin yanlislikla buraya dusmesini engelle
            title_lo = (disclosure.get("title") or "").lower() if disclosure else ""
            body_lo = (body or "").lower()[:2000]
            # Title bos olabiliyor — body'de "Pay Alım Satım Bildirimi" basligi varsa da kabul et
            is_pay_alim_satim = (
                "pay alım satım" in title_lo or "pay alim satim" in title_lo
                or "pay alım bildirim" in title_lo or "alım satım bildirim" in title_lo
                or "pay alım satım bildirimi" in body_lo or "pay alim satim bildirimi" in body_lo
            )
            # Bilanco/finansal tablo KAP'larini explicit olarak hariç tut
            if "finansal rapor" in title_lo or "finansal tablo" in title_lo or "bilanço" in title_lo:
                is_pay_alim_satim = False
            pay_parsed = await fetch_kap_pay_alim_satim(kap_url) if is_pay_alim_satim else None
            result["processors"]["pay_alim_satim"] = pay_parsed or {"skipped": "title_not_pay_alim_satim", "title": title_lo[:80]}
            # Ticker: fetcher'dan veya payload hint'inden
            tx_ticker = (pay_parsed.get("ticker") if pay_parsed else None) or ticker_hint
            if pay_parsed and tx_ticker:
                try:
                    saved = await upsert_pay_alim_satim_from_kap(
                        db, kap_url=kap_url,
                        company_code=tx_ticker,
                        title="Pay Alım Satım Bildirimi",
                        published_at=_dt2.now(_tz2.utc),
                        disclosure_id=None,
                    )
                    result["processors"]["pay_alim_satim"]["db_saved"] = bool(saved)
                except Exception as save_err:
                    result["processors"]["pay_alim_satim"]["save_error"] = str(save_err)[:200]
        except Exception as e:
            result["processors"]["pay_alim_satim"] = {"error": str(e)}

        await db.commit()

    return result


# ═════════════════════════════════════════════════════════════════════════════
# BACKFILL — Geçmiş KAP kayıtlarını yeni processor'larla tekrar işle
# ═════════════════════════════════════════════════════════════════════════════

@app.post("/api/v1/admin/backfill-kap-processors")
@limiter.limit("3/minute")
async def admin_backfill_kap_processors(request: Request, payload: dict = Body(...)):
    """Admin: Eksik kalan KAP işlemlerini geçmişe dönük doldur.

    Body: {
      "admin_password": "...",
      "categories": ["business_deal", "dividend_payment", "mkk_realization"],
      "days": 90,    # Son N gün
      "limit": 500   # Max kayıt
    }
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    cats = payload.get("categories") or ["business_deal", "dividend_payment", "mkk_realization", "dividend_rejection", "type_conversion", "bilanco_enrich"]
    days = int(payload.get("days") or 90)
    limit_n = int(payload.get("limit") or 500)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    from app.database import async_session as _async_session
    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
    from app.models.kap_all_disclosure import KapAllDisclosure
    from app.models.business_deal import BusinessDeal

    summary = {"categories": cats, "days": days, "limit": limit_n,
               "processed": 0, "updates": {}}

    async with _async_session() as db:
        # ─── 1. business_deal: amount_try IS NULL kayıtları
        if "business_deal" in cats:
            from app.services.business_deal_processor import (
                regex_extract_business_deal, get_exchange_rate,
            )
            stmt = (
                select(BusinessDeal)
                .where(BusinessDeal.amount_try.is_(None))
                .order_by(BusinessDeal.deal_date.desc().nullslast())
                .limit(limit_n)
            )
            rows = (await db.execute(stmt)).scalars().all()
            updated = 0
            for r in rows:
                if not r.kap_url:
                    continue
                try:
                    disc = await fetch_kap_disclosure(r.kap_url)
                    if not disc or not disc.get("full_text"):
                        continue
                    body = disc["full_text"]
                    parsed = regex_extract_business_deal(body)
                    amt = parsed.get("amount_original")
                    cur = parsed.get("currency") or "TRY"
                    if amt is None:
                        continue
                    if cur == "TRY":
                        r.amount_original = amt
                        r.currency = "TRY"
                        r.amount_try = amt
                        r.exchange_rate_used = 1.0
                    else:
                        # Yabanci para — amount_original/currency her durumda yaz
                        # (kur fetch basarisiz olsa bile orjinal tutar kayit olsun)
                        r.amount_original = amt
                        r.currency = cur
                        rate, rdate = await get_exchange_rate(cur)
                        if rate:
                            r.amount_try = amt * rate
                            r.exchange_rate_used = rate
                            r.rate_date = rdate
                    if parsed.get("counterparty") and not r.counterparty:
                        r.counterparty = parsed["counterparty"]
                    updated += 1
                except Exception as e:
                    logger.warning("Backfill business_deal hata (%s): %s", r.ticker, e)
            await db.flush()
            summary["updates"]["business_deal"] = {"scanned": len(rows), "updated": updated}

        # ─── 2. dividend_payment + mkk_realization: kap_all_disclosures üzerinden
        if "dividend_payment" in cats or "mkk_realization" in cats:
            from app.services.dividend_calendar_processor import (
                is_dividend_payment_announcement,
                process_dividend_payment_announcement,
            )
            from app.services.capital_increase_processor import (
                is_mkk_capital_realization,
                process_mkk_capital_realization,
            )

            stmt = (
                select(KapAllDisclosure)
                .where(KapAllDisclosure.published_at >= cutoff)
                .where(or_(
                    KapAllDisclosure.title.ilike("%merkezi kayıt%"),
                    KapAllDisclosure.title.ilike("%merkezi kayit%"),
                    KapAllDisclosure.title.ilike("%MKK%"),
                    KapAllDisclosure.title.ilike("%BISTECH%"),
                    KapAllDisclosure.title.ilike("%Pay Piyasası%"),
                    KapAllDisclosure.title.ilike("%Pay Piyasasi%"),
                    KapAllDisclosure.title.ilike("%hak kullanım%"),
                    KapAllDisclosure.title.ilike("%hak kullanim%"),
                ))
                .order_by(KapAllDisclosure.published_at.desc())
                .limit(limit_n)
            )
            rows = (await db.execute(stmt)).scalars().all()
            div_updated = 0
            mkk_updated = 0
            for d in rows:
                body = d.body or ""
                if (not body or len(body) < 200) and d.kap_url:
                    try:
                        disc = await fetch_kap_disclosure(d.kap_url)
                        if disc and disc.get("full_text"):
                            body = disc["full_text"]
                    except Exception:
                        continue

                if "dividend_payment" in cats:
                    try:
                        if is_dividend_payment_announcement(d.title or "", body):
                            res = await process_dividend_payment_announcement(
                                db, body=body, kap_url=d.kap_url,
                                disclosure_id=d.id, published_at=d.published_at,
                            )
                            if res.get("updated"):
                                div_updated += res["updated"]
                    except Exception as e:
                        logger.warning("Backfill div_payment hata (id=%s): %s", d.id, e)

                if "mkk_realization" in cats:
                    try:
                        if is_mkk_capital_realization(d.title or "", body):
                            res = await process_mkk_capital_realization(
                                db, ticker_hint=d.company_code, body=body,
                                kap_url=d.kap_url, disclosure_id=d.id,
                            )
                            if res.get("matched"):
                                mkk_updated += 1
                    except Exception as e:
                        logger.warning("Backfill mkk hata (id=%s): %s", d.id, e)

            await db.flush()
            if "dividend_payment" in cats:
                summary["updates"]["dividend_payment"] = {"scanned": len(rows), "updated": div_updated}
            if "mkk_realization" in cats:
                summary["updates"]["mkk_realization"] = {"scanned": len(rows), "updated": mkk_updated}

        # ─── 3. dividend_rejection: existing DividendCalendar rows + KAP body re-classify
        if "dividend_rejection" in cats:
            from app.services.dividend_calendar_processor import classify_event_with_body
            from app.models.dividend_calendar import DividendCalendar
            from app.models.kap_all_disclosure import KapAllDisclosure
            from sqlalchemy import select as _sel

            # Tüm DividendCalendar reddedildi olmayan kayıtlar
            dc_rows = (await db.execute(
                _sel(DividendCalendar)
                .where(DividendCalendar.status != "reddedildi")
                .where(DividendCalendar.created_at >= cutoff)
                .limit(limit_n)
            )).scalars().all()

            updated_rej = 0
            for r in dc_rows:
                # KAP disclosure linkini bul
                kid = r.ykk_kap_disclosure_id or r.general_assembly_kap_disclosure_id
                if not kid:
                    continue
                kap = (await db.execute(
                    _sel(KapAllDisclosure).where(KapAllDisclosure.id == kid).limit(1)
                )).scalar_one_or_none()
                if not kap:
                    continue

                body = kap.body or ""
                if (not body or len(body) < 200) and kap.kap_url:
                    try:
                        disc = await fetch_kap_disclosure(kap.kap_url)
                        if disc and disc.get("full_text"):
                            body = disc["full_text"]
                    except Exception:
                        continue

                event = classify_event_with_body(kap.title or "", body)
                if event == "rejection":
                    r.status = "reddedildi"
                    if kid and not r.rejection_kap_disclosure_id:
                        r.rejection_kap_disclosure_id = kid
                    if kap.kap_url and not r.rejection_kap_url:
                        r.rejection_kap_url = kap.kap_url
                    if not r.rejected_at:
                        r.rejected_at = kap.published_at or datetime.now(timezone.utc)
                    updated_rej += 1
            await db.flush()
            summary["updates"]["dividend_rejection"] = {"scanned": len(dc_rows), "updated": updated_rej}

        # ─── 5. type_conversion: KAP body'deki tabloyu yeniden parse et,
        # eski tek-satirli yanlis kayitlari sil + yeni tum satirlari ekle
        if "type_conversion" in cats:
            from app.services.kap_category_processors import (
                is_type_conversion, _parse_tc_table,
            )
            from app.models.share_type_conversion import ShareTypeConversion
            from app.models.kap_all_disclosure import KapAllDisclosure
            from sqlalchemy import select as _sel, delete as _del

            # Tipe Dönüşüm KAP haberleri
            kap_rows = (await db.execute(
                _sel(KapAllDisclosure)
                .where(KapAllDisclosure.published_at >= cutoff)
                .where(or_(
                    KapAllDisclosure.title.ilike("%tipe dönüşüm%"),
                    KapAllDisclosure.title.ilike("%tipe donusum%"),
                    KapAllDisclosure.title.ilike("%borsada işlem gören tipe%"),
                ))
                .limit(limit_n)
            )).scalars().all()

            tc_added = 0
            tc_removed = 0
            tc_debug = []
            import asyncio as _asyncio
            for kap in kap_rows:
                # Tipe donusum tablosu icin daima full RSC body gerekli (>5K char)
                body = ""
                tables_count = 0
                if kap.kap_url:
                    try:
                        disc = await fetch_kap_disclosure(kap.kap_url)
                        if disc and disc.get("full_text"):
                            body = disc["full_text"]
                            tables_count = len(disc.get("tables", []))
                    except Exception as ex:
                        tc_debug.append(f"{kap.kap_url}: fetch error {ex}")
                        continue
                    # KAP rate limit: her fetch arasinda 2 sn bekle
                    await _asyncio.sleep(2)

                rows_data = _parse_tc_table(body)
                if not rows_data:
                    tc_debug.append(f"{kap.kap_url}: tables={tables_count} body={len(body)} parse=0")
                    continue

                # Eski tek-satirli yanlis kayitlari sil
                existing = (await db.execute(
                    _sel(ShareTypeConversion)
                    .where(ShareTypeConversion.kap_url == kap.kap_url)
                )).scalars().all()
                tc_debug.append(f"{kap.kap_url}: parsed={len(rows_data)} existing={len(existing)}")

                if len(existing) < len(rows_data):
                    # Tum eski kayitlari sil
                    if existing:
                        await db.execute(
                            _del(ShareTypeConversion).where(ShareTypeConversion.kap_url == kap.kap_url)
                        )
                        tc_removed += len(existing)

                    # Tum satirlari ekle
                    for d in rows_data:
                        new_row = ShareTypeConversion(
                            ticker=d["ticker"],
                            company_name=d.get("company_name"),
                            transaction_date=kap.published_at.date() if kap.published_at else None,
                            investor_name=d["investor_name"],
                            converted_lot=int(d["nominal_tl"]) if d.get("nominal_tl") else None,
                            kap_url=kap.kap_url,
                            source="kap_table_parse",
                        )
                        db.add(new_row)
                        tc_added += 1
            await db.flush()
            summary["updates"]["type_conversion"] = {
                "scanned": len(kap_rows),
                "removed": tc_removed,
                "added": tc_added,
                "debug": tc_debug[:20],
            }

        # ─── 4. dividend_misclassified: 'Hak Kullanımı' başlıklı KAP'lar için
        # body bedelsiz sermaye artırımıysa ilgili DividendCalendar kaydını sil
        if "dividend_misclassified" in cats:
            from app.services.dividend_calendar_processor import is_dividend as _is_div
            from app.models.dividend_calendar import DividendCalendar
            from app.models.kap_all_disclosure import KapAllDisclosure
            from sqlalchemy import select as _sel, delete as _del

            dc_rows = (await db.execute(
                _sel(DividendCalendar)
                .where(DividendCalendar.created_at >= cutoff)
                .limit(limit_n)
            )).scalars().all()

            removed = 0
            for r in dc_rows:
                kid = r.ykk_kap_disclosure_id or r.general_assembly_kap_disclosure_id or r.payment_kap_disclosure_id
                if not kid:
                    continue
                kap = (await db.execute(
                    _sel(KapAllDisclosure).where(KapAllDisclosure.id == kid).limit(1)
                )).scalar_one_or_none()
                if not kap:
                    continue

                body = kap.body or ""
                if (not body or len(body) < 200) and kap.kap_url:
                    try:
                        disc = await fetch_kap_disclosure(kap.kap_url)
                        if disc and disc.get("full_text"):
                            body = disc["full_text"]
                    except Exception:
                        continue

                # body-aware is_dividend false dönerse → yanlış kayıt, sil
                if not _is_div(kap.title or "", body):
                    await db.execute(
                        _del(DividendCalendar).where(DividendCalendar.id == r.id)
                    )
                    removed += 1
                    logger.info("DividendCalendar yanlis kayit silindi: %s id=%s (KAP id=%s)", r.ticker, r.id, kid)
            await db.flush()
            summary["updates"]["dividend_misclassified"] = {"scanned": len(dc_rows), "removed": removed}

        # ─── 6. bilanco_enrich: company_financials 'da current_assets/non_current_assets/total_debt/cash NULL/0 olanlari KAP'tan parse edip doldur
        if "bilanco_enrich" in cats:
            from app.models.company_financial import CompanyFinancial
            from app.models.kap_all_disclosure import KapAllDisclosure
            from app.services.ai_bilanco_analyzer import parse_bilanco_from_kap, save_parsed_bilanco
            from sqlalchemy import select as _sel, or_ as _or

            # NULL/0 current_assets veya non_current_assets olan kayitlar
            stmt = (
                _sel(CompanyFinancial)
                .where(_or(
                    CompanyFinancial.current_assets.is_(None),
                    CompanyFinancial.current_assets == 0,
                    CompanyFinancial.non_current_assets.is_(None),
                    CompanyFinancial.non_current_assets == 0,
                ))
                .order_by(CompanyFinancial.updated_at.desc().nullslast())
                .limit(limit_n)
            )
            cf_rows = (await db.execute(stmt)).scalars().all()

            be_enriched = 0
            be_skipped = 0
            import asyncio as _asyncio_be
            import gc as _gc
            # ticker bazli KAP listesi cache (her ticker icin son ~10 KAP)
            ticker_kap_cache: dict[str, list] = {}
            for r in cf_rows:
                # KAP rate-limit: her fetch arasinda 2 sn bekle
                await _asyncio_be.sleep(2)
                # Bu ticker icin TUM is_bilanco KAP'larini cache'le (1 sefer)
                if r.ticker not in ticker_kap_cache:
                    kaps = (await db.execute(
                        _sel(KapAllDisclosure)
                        .where(KapAllDisclosure.company_code == r.ticker)
                        .where(KapAllDisclosure.is_bilanco == True)
                        .order_by(KapAllDisclosure.published_at.desc())
                        .limit(15)
                    )).scalars().all()
                    ticker_kap_cache[r.ticker] = list(kaps)

                kaps = ticker_kap_cache.get(r.ticker, [])
                if not kaps:
                    be_skipped += 1
                    continue

                # Her KAP'i sirayla dene — period match olani bul
                enriched_for_this = False
                for kap in kaps:
                    if not kap.kap_url:
                        continue
                    try:
                        disc = await fetch_kap_disclosure(kap.kap_url)
                        if not disc or not disc.get("full_text"):
                            continue
                        body = disc["full_text"]
                        del disc  # memory free
                        parsed = await parse_bilanco_from_kap(r.ticker, body)
                        del body
                        _gc.collect()
                        if not parsed:
                            continue
                        # Period match kontrolu
                        if parsed.get("period") != r.period:
                            # Farkli donem - kayit varsa save_parsed_bilanco
                            # (baska bir cf_row icin enrich olabilir)
                            try:
                                await save_parsed_bilanco(r.ticker, parsed)
                            except Exception:
                                pass
                            continue
                        # Ayni donem - eksik alanlari doldur
                        any_filled = False
                        for field in ["current_assets", "non_current_assets",
                                      "total_debt", "cash_and_equivalents",
                                      "gross_profit", "operating_profit", "net_debt"]:
                            val = parsed.get(field)
                            if val is None or val == 0:
                                continue
                            existing_val = getattr(r, field, None)
                            if existing_val is None or float(existing_val or 0) == 0:
                                setattr(r, field, val)
                                any_filled = True
                        if any_filled:
                            r.updated_at = datetime.now(timezone.utc)
                            be_enriched += 1
                            enriched_for_this = True
                        break  # Period match bulundu, bu cf_row icin yeter
                    except Exception:
                        continue
                if not enriched_for_this:
                    be_skipped += 1

            await db.flush()
            summary["updates"]["bilanco_enrich"] = {
                "scanned": len(cf_rows),
                "enriched": be_enriched,
                "skipped": be_skipped,
            }

        await db.commit()

    return summary


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE HEALTH — MANUEL DÜZELTME ENDPOINT'LERI
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/v1/admin/set-bilanco-field")
@limiter.limit("20/minute")
async def admin_set_bilanco_field(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Bilanço kaydının tek bir alanını manuel düzelt (Fintables ile karşılaştırıp).

    Body: {'admin_password': '...', 'ticker': 'KLGYO', 'period': '2026-Q1',
           'field': 'revenue', 'value': 392050000}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.company_financial import CompanyFinancial
    ticker = (payload.get("ticker") or "").upper()
    period = payload.get("period")
    field = payload.get("field")
    value = payload.get("value")
    allowed = {
        "revenue", "gross_profit", "operating_profit", "net_income", "ebitda",
        "total_assets", "current_assets", "non_current_assets",
        "total_equity", "total_debt", "net_debt", "cash_and_equivalents",
        "net_interest_income", "net_fees_commissions",
        "gross_premiums", "technical_balance",
    }
    if not ticker or not period or field not in allowed:
        raise HTTPException(status_code=400, detail="ticker+period+field zorunlu, field whitelist'te olmali")
    row = (await db.execute(
        select(CompanyFinancial).where(
            CompanyFinancial.ticker == ticker,
            CompanyFinancial.period == period,
        )
    )).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Bilanço kaydı bulunamadı")
    old = getattr(row, field, None)
    setattr(row, field, float(value) if value is not None else None)
    row.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"status": "ok", "ticker": ticker, "period": period, "field": field,
            "old": float(old) if old else None, "new": float(value) if value else None}


@app.post("/api/v1/admin/set-tipe-conversion-lot")
@limiter.limit("20/minute")
async def admin_set_tipe_conversion_lot(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Tipe dönüşüm kaydının converted_lot / investor_name düzeltme.

    Body: {'admin_password': '...', 'id': 12, 'converted_lot': 1500000,
           'investor_name': 'Ali Veli'}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.share_type_conversion import ShareTypeConversion
    _id = int(payload.get("id") or 0)
    row = (await db.execute(select(ShareTypeConversion).where(ShareTypeConversion.id == _id))).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Kayıt bulunamadı")
    changes = {}
    if "converted_lot" in payload and payload["converted_lot"] is not None:
        row.converted_lot = int(payload["converted_lot"])
        changes["converted_lot"] = row.converted_lot
    if "investor_name" in payload and payload["investor_name"]:
        row.investor_name = str(payload["investor_name"])[:255]
        changes["investor_name"] = row.investor_name
    await db.commit()
    return {"status": "ok", "id": row.id, "ticker": row.ticker, "changes": changes}


@app.post("/api/v1/admin/set-dividend-payment-date")
@limiter.limit("20/minute")
async def admin_set_dividend_payment_date(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Temettü kaydının payment_date + brüt/net TL manuel düzelt.

    Body: {'admin_password': '...', 'id': 42, 'payment_date': '2026-07-15',
           'gross_amount_per_share': 1.25, 'net_amount_per_share': 1.0625}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.dividend_calendar import DividendCalendar
    _id = int(payload.get("id") or 0)
    row = (await db.execute(select(DividendCalendar).where(DividendCalendar.id == _id))).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Kayıt bulunamadı")
    changes = {}
    if "payment_date" in payload and payload["payment_date"]:
        try:
            row.payment_date = date.fromisoformat(payload["payment_date"])
            changes["payment_date"] = str(row.payment_date)
            # Status update: bugün/ileri/geçmiş
            today = date.today()
            if row.payment_date > today:
                row.status = "tarih_belli"
            elif row.payment_date == today:
                row.status = "odeniyor"
            else:
                row.status = "tamamlandi"
            changes["status"] = row.status
        except ValueError:
            raise HTTPException(status_code=400, detail="payment_date YYYY-MM-DD formatında olmalı")
    if "gross_amount_per_share" in payload and payload["gross_amount_per_share"] is not None:
        row.gross_amount_per_share = float(payload["gross_amount_per_share"])
        changes["gross_amount_per_share"] = row.gross_amount_per_share
    if "net_amount_per_share" in payload and payload["net_amount_per_share"] is not None:
        row.net_amount_per_share = float(payload["net_amount_per_share"])
        changes["net_amount_per_share"] = row.net_amount_per_share
    row.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"status": "ok", "id": row.id, "ticker": row.ticker, "changes": changes}


@app.post("/api/v1/admin/set-capital-increase-amount")
@limiter.limit("20/minute")
async def admin_set_capital_increase_amount(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Sermaye artırımı kaydının yüzde/tutar düzelt.

    Body: {'admin_password': '...', 'id': 5, 'type': 'bedelsiz',
           'pct': 990.91, 'amount_tl': 1845000000}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.capital_increase import CapitalIncrease
    _id = int(payload.get("id") or 0)
    row = (await db.execute(select(CapitalIncrease).where(CapitalIncrease.id == _id))).scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Kayıt bulunamadı")
    changes = {}
    pct = payload.get("pct")
    if pct is not None:
        t = (payload.get("type") or row.type or "bedelsiz").lower()
        field = f"{t}_pct"
        if hasattr(row, field):
            setattr(row, field, float(pct))
            changes[field] = float(pct)
    amount = payload.get("amount_tl")
    if amount is not None and hasattr(row, "bolunme_sonrasi_sermaye_tl"):
        row.bolunme_sonrasi_sermaye_tl = float(amount)
        changes["bolunme_sonrasi_sermaye_tl"] = float(amount)
    row.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"status": "ok", "id": row.id, "ticker": row.ticker, "changes": changes}


@app.post("/api/v1/admin/cleanup-is-bilanco-flags")
@limiter.limit("3/minute")
async def admin_cleanup_is_bilanco_flags(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Eski kayıtlardaki yanlış is_bilanco=True flag'lerini temizler.

    Sadece "Finansal Durum Tablosu (Bilanço)" başlıklı KAP'lar gerçek bilançodur.
    "Sorumluluk Beyanı", "Faaliyet Raporu", "Nakit Akış Tablosu" gibi yan
    dokümanlar yanlışlıkla is_bilanco=True işaretlenmiş — bunları False yapar.

    HİÇBİR KAP haberi silinmez, sadece flag düzeltilir. Mobil app bilanço
    sayfası (company_financials tablosu) etkilenmez.

    Body: {'admin_password': '...'}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.models.kap_all_disclosure import KapAllDisclosure
    from sqlalchemy import update as _update, func as _f

    # Once kac kayit etkilenecek bul
    count_q = await db.execute(
        select(_f.count(KapAllDisclosure.id))
        .where(KapAllDisclosure.is_bilanco == True)  # noqa: E712
        .where(_f.lower(KapAllDisclosure.title).notlike("%finansal durum tablosu%"))
    )
    affected = count_q.scalar() or 0

    # Update
    upd = (
        _update(KapAllDisclosure)
        .where(KapAllDisclosure.is_bilanco == True)  # noqa: E712
        .where(_f.lower(KapAllDisclosure.title).notlike("%finansal durum tablosu%"))
        .values(is_bilanco=False)
    )
    result = await db.execute(upd)
    await db.commit()

    return {
        "status": "ok",
        "affected_rows": affected,
        "matched": result.rowcount,
        "note": "Sadece 'Finansal Durum Tablosu (Bilanço)' başlıklı KAP'lar is_bilanco=True olarak kaldı",
    }


@app.post("/api/v1/admin/run-bilanco-ai")
@limiter.limit("5/minute")
async def admin_run_bilanco_ai(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """Belirli bir ticker icin AI bilanco analizini elle tetikle.

    Body: {'admin_password': '...', 'ticker': 'TTRAK', 'period': '2026-Q1'}
    Period belirtilmezse en yeni period kullanilir.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.models.company_financial import CompanyFinancial
    from app.services.ai_bilanco_analyzer import analyze_bilanco

    ticker = (payload.get("ticker") or "").upper()
    if not ticker:
        raise HTTPException(status_code=400, detail="ticker zorunlu")

    # Son 20 ceyrek (5 yil)
    recent = (await db.execute(
        select(CompanyFinancial).where(CompanyFinancial.ticker == ticker)
        .order_by(desc(CompanyFinancial.period)).limit(20)
    )).scalars().all()
    if not recent:
        raise HTTPException(status_code=404, detail=f"{ticker} icin bilanco verisi yok")

    periods_data = [
        {
            "period": p.period,
            "sector_type": p.sector_type,
            "revenue": float(p.revenue) if p.revenue else None,
            "gross_profit": float(p.gross_profit) if p.gross_profit else None,
            "operating_profit": float(p.operating_profit) if p.operating_profit else None,
            "net_income": float(p.net_income) if p.net_income else None,
            "ebitda": float(p.ebitda) if p.ebitda else None,
            "total_assets": float(p.total_assets) if p.total_assets else None,
            "total_equity": float(p.total_equity) if p.total_equity else None,
            "total_debt": float(p.total_debt) if p.total_debt else None,
            "net_debt": float(p.net_debt) if p.net_debt else None,
            "net_interest_income": float(p.net_interest_income) if p.net_interest_income else None,
            "gross_premiums": float(p.gross_premiums) if p.gross_premiums else None,
        }
        for p in recent
    ]

    ai_result = await analyze_bilanco(ticker, periods_data)
    if not ai_result:
        raise HTTPException(status_code=502, detail="AI analizi basarisiz")

    # En yeni doneme yaz
    import json as _json
    latest = recent[0]
    latest.ai_score = float(ai_result.get("overall_health_score", 5.0))
    latest.ai_label = str(ai_result.get("overall_health_label", ""))[:32] or None
    latest.ai_summary = str(ai_result.get("summary", ""))[:2000] or None
    latest.ai_analysis = _json.dumps(ai_result, ensure_ascii=False)[:8000]
    latest.ai_analyzed_at = datetime.now(timezone.utc)
    await db.commit()

    return {
        "status": "ok",
        "ticker": ticker,
        "period": latest.period,
        "ai_score": latest.ai_score,
        "ai_label": latest.ai_label,
        "summary_preview": (latest.ai_summary or "")[:200],
        "analysis": ai_result,
    }


@app.post("/api/v1/admin/run-raw-sql")
@limiter.limit("3/minute")
async def admin_run_raw_sql(
    request: Request,
    payload: dict = Body(...),
    db: AsyncSession = Depends(get_db),
):
    """ACIL DURUM: Admin için raw SQL çalıştır.

    Migration kacirilmis vb. durumlarda DB schema'sini elle duzeltmek icin.

    Body: {'admin_password': '...', 'sql': 'ALTER TABLE ... ADD COLUMN ...'}

    UYARI: SELECT * disinda her sey DESTRUCTIVE olabilir. Dikkatli kullan.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    sql = (payload.get("sql") or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="sql zorunlu")
    if len(sql) > 4000:
        raise HTTPException(status_code=400, detail="sql max 4000 char")

    from sqlalchemy import text as _text
    try:
        result = await db.execute(_text(sql))
        rows_affected = result.rowcount if hasattr(result, "rowcount") else -1
        # SELECT ise sonuc dondur
        rows = []
        try:
            rows = [dict(r._mapping) for r in result.fetchall()]
        except Exception:
            pass
        await db.commit()
        return {
            "status": "ok",
            "rows_affected": rows_affected,
            "rows": rows[:50] if rows else [],
            "row_count": len(rows),
        }
    except Exception as e:
        await db.rollback()
        return {"status": "error", "error": str(e)[:500]}


async def _batch_bilanco_ai_worker(period: str, limit: int):
    """Background task — N ticker icin AI bilanco analizini ardarda calistirir."""
    from app.database import async_session as _ases
    from app.models.company_financial import CompanyFinancial
    from app.services.ai_bilanco_analyzer import analyze_bilanco
    import asyncio as _asyncio
    import logging as _lg
    _logger = _lg.getLogger("batch_bilanco_ai")
    try:
        async with _ases() as db:
            candidates = (await db.execute(
                select(CompanyFinancial.ticker)
                .where(CompanyFinancial.period == period)
                .where(CompanyFinancial.ai_score.is_(None))
                .where(CompanyFinancial.total_assets.isnot(None))
                .limit(limit)
            )).scalars().all()
        _logger.info("batch_bilanco_ai: %d aday bulundu, AI uretiliyor", len(candidates))
        ok = 0; fail = 0
        for ticker in candidates:
            try:
                async with _ases() as db:
                    recent = (await db.execute(
                        select(CompanyFinancial).where(CompanyFinancial.ticker == ticker)
                        .order_by(desc(CompanyFinancial.period)).limit(20)  # 5 yil
                    )).scalars().all()
                    if not recent:
                        continue
                    periods_data = [
                        {
                            "period": p.period,
                            "sector_type": p.sector_type,
                            "revenue": float(p.revenue) if p.revenue else None,
                            "gross_profit": float(p.gross_profit) if p.gross_profit else None,
                            "operating_profit": float(p.operating_profit) if p.operating_profit else None,
                            "net_income": float(p.net_income) if p.net_income else None,
                            "ebitda": float(p.ebitda) if p.ebitda else None,
                            "total_assets": float(p.total_assets) if p.total_assets else None,
                            "total_equity": float(p.total_equity) if p.total_equity else None,
                            "total_debt": float(p.total_debt) if p.total_debt else None,
                            "net_debt": float(p.net_debt) if p.net_debt else None,
                            "net_interest_income": float(p.net_interest_income) if p.net_interest_income else None,
                            "gross_premiums": float(p.gross_premiums) if p.gross_premiums else None,
                        }
                        for p in recent
                    ]
                    ai_result = await analyze_bilanco(ticker, periods_data)
                    if ai_result:
                        import json as _json
                        latest = recent[0]
                        latest.ai_score = float(ai_result.get("overall_health_score", 5.0))
                        latest.ai_label = str(ai_result.get("overall_health_label", ""))[:32] or None
                        latest.ai_summary = str(ai_result.get("summary", ""))[:2000] or None
                        latest.ai_analysis = _json.dumps(ai_result, ensure_ascii=False)[:8000]
                        latest.ai_analyzed_at = datetime.now(timezone.utc)
                        await db.commit()
                        ok += 1
                    else:
                        fail += 1
                await _asyncio.sleep(2)
            except Exception as e:
                fail += 1
                _logger.warning("batch_bilanco_ai %s: %s", ticker, e)
        _logger.info("batch_bilanco_ai TAMAM: ok=%d fail=%d", ok, fail)
    except Exception as e:
        _logger.exception("batch_bilanco_ai worker hata: %s", e)


@app.post("/api/v1/admin/batch-bilanco-ai")
@limiter.limit("3/minute")
async def admin_batch_bilanco_ai(
    request: Request,
    background_tasks: BackgroundTasks,
    payload: dict = Body(...),
):
    """Son N bilanço için AI analizini ARKAPLAN'DA toplu üret. Hemen 'queued' donder.

    Body: {'admin_password': '...', 'period': '2026-Q1', 'limit': 50}
    Sonuc Render log'larinda goruntulenir; AI sonuclari /bilanco/top icin
    DB'ye yazilir, sayfa refresh ile gorunur.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    period = payload.get("period") or "2026-Q1"
    limit = min(int(payload.get("limit", 30)), 200)
    background_tasks.add_task(_batch_bilanco_ai_worker, period, limit)
    return {"status": "queued", "period": period, "limit": limit, "note": "Backend arkaplan'da isleniyor — Render log'larini izle veya /bilanco/top'tan sonuc gor"}


async def _overnight_bilanco_ai_worker(sleep_sec: int, max_count: int):
    """700+ sirket icin GECE toplu AI — scheduler'daki paylasimli worker'i cagirir."""
    try:
        from app.scheduler import run_overnight_bilanco_ai
        await run_overnight_bilanco_ai(sleep_sec=sleep_sec, max_count=max_count)
    except Exception as e:
        import logging as _lg
        _lg.getLogger("overnight_bilanco_ai").exception("worker hata: %s", e)


@app.post("/api/v1/admin/backfill-bilanco-dates")
@limiter.limit("5/minute")
async def admin_backfill_bilanco_dates(request: Request, payload: dict = Body(...), db: AsyncSession = Depends(get_db)):
    """Bilanço açıklama tarihlerini elle backfill et (Fintables listesinden).

    Son Bilançolar sıralaması bu tarihe göre yapılır.
    Body: {'admin_password': '...', 'items': [{'ticker':'KLGYO','date':'2026-05-22','period':'2026-Q1'}, ...]}
    period verilirse o döneme, verilmezse ticker'ın EN YENİ dönemine yazılır.
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="items zorunlu")
    from sqlalchemy import text as _t
    from datetime import datetime as _dt, timezone as _tz
    ok = 0; miss = 0
    for it in items:
        ticker = (it.get("ticker") or "").upper().strip()
        date_s = (it.get("date") or "").strip()
        period = (it.get("period") or "").strip()
        if not ticker or not date_s:
            continue
        try:
            dt = _dt.fromisoformat(date_s).replace(tzinfo=_tz.utc)
        except Exception:
            miss += 1; continue
        if period:
            res = await db.execute(_t(
                "UPDATE company_financials SET announced_date = :d WHERE ticker = :t AND period = :p"
            ), {"d": dt, "t": ticker, "p": period})
            if (res.rowcount or 0) == 0:
                # period eşleşmedi → en yeni döneme yaz
                res2 = await db.execute(_t(
                    "UPDATE company_financials SET announced_date = :d WHERE id = "
                    "(SELECT id FROM company_financials WHERE ticker = :t ORDER BY period DESC LIMIT 1)"
                ), {"d": dt, "t": ticker})
                ok += 1 if (res2.rowcount or 0) else 0
                miss += 0 if (res2.rowcount or 0) else 1
            else:
                ok += 1
        else:
            res = await db.execute(_t(
                "UPDATE company_financials SET announced_date = :d WHERE id = "
                "(SELECT id FROM company_financials WHERE ticker = :t ORDER BY period DESC LIMIT 1)"
            ), {"d": dt, "t": ticker})
            ok += 1 if (res.rowcount or 0) else 0
            miss += 0 if (res.rowcount or 0) else 1
    await db.commit()
    return {"status": "ok", "updated": ok, "missed": miss, "total": len(items)}


@app.post("/api/v1/admin/update-stock-sectors")
@limiter.limit("3/minute")
async def admin_update_stock_sectors(request: Request, payload: dict = Body(...)):
    """Resmi BIST CSV'sinden ticker→sektör + endeks üyeliğini ŞIMDI güncelle.

    Body: {'admin_password': '...'}
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    from app.scrapers.bist_sector_scraper import update_stock_sectors
    return await update_stock_sectors()


@app.post("/api/v1/admin/overnight-bilanco-ai")
@limiter.limit("2/minute")
async def admin_overnight_bilanco_ai(
    request: Request,
    background_tasks: BackgroundTasks,
    payload: dict = Body(...),
):
    """GECE TOPLU AI — 700+ sirketin AI'sız en guncel bilancolarini tek tek isler.

    Render 512MB'i tikamadan: her ticker icin taze DB session + aralarda ~28s sleep.
    700 sirket ≈ 5.8 saat. Sadece ai_score NULL olanlar islenir (tekrar tiklanirsa
    sadece yeni gelenler analiz edilir → bos yere maliyet cikmaz).

    Body: {'admin_password': '...', 'sleep_sec': 28, 'max_count': 900}
    Hemen 'queued' doner; ilerleme Render log'larinda ([overnight_bilanco_ai]).
    """
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")
    sleep_sec = max(5, min(int(payload.get("sleep_sec", 28)), 120))
    max_count = min(int(payload.get("max_count", 900)), 1500)
    background_tasks.add_task(_overnight_bilanco_ai_worker, sleep_sec, max_count)
    est_hours = round((max_count * sleep_sec) / 3600.0, 1)
    return {
        "status": "queued",
        "sleep_sec": sleep_sec,
        "max_count": max_count,
        "tahmini_sure_saat": est_hours,
        "note": "Gece batch baslatildi — sadece AI'sız (ai_score NULL) en guncel bilancolar islenir. Ilerleme: Render log [overnight_bilanco_ai]",
    }
