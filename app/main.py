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

from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy import select, delete, update, desc, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database import get_db, init_db
from app.models import (
    IPO, IPOBroker, IPOAllocation, IPOCeilingTrack,
    SPKApplication,
    KapNews, User, UserSubscription, UserIPOAlert,
    CeilingTrackSubscription, CEILING_TIER_PRICES,
    TelegramNews, StockNotificationSubscription,
    NOTIFICATION_TIER_PRICES, NEWS_TIER_PRICES,
    COMBO_PRICE, QUARTERLY_PRICE,
    ANNUAL_BUNDLE_PRICE, COMBINED_ANNUAL_DISCOUNT_PCT,
    WalletTransaction, WALLET_COUPONS,
    WALLET_REWARD_AMOUNT, WALLET_COOLDOWN_SECONDS, WALLET_MAX_DAILY_ADS,
    Dividend, DividendHistory,
)
from app.schemas import (
    IPOListOut, IPODetailOut, IPOSectionsOut,
    SPKApplicationOut,
    KapNewsOut, TelegramNewsOut,
    UserRegister, UserUpdate, UserOut, SubscriptionInfo,
    ReminderSettingsUpdate,
    IPOAlertCreate, CeilingTrackUpdate,
    CeilingTierOut, CeilingSubscriptionCreate, CeilingSubscriptionOut,
    NotificationTierOut, NewsTierOut,
    StockNotificationCreate, StockNotificationOut,
    RealtimeNotifRequest,
    DividendOut,
    WalletBalanceOut, WalletEarnRequest, WalletSpendRequest,
    WalletCouponRequest, WalletTransactionOut,
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


# -------------------------------------------------------
# Uygulama Yasam Dongusu
# -------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Uygulama baslangic ve kapanis islemleri."""
    logger.info("BIST Finans Backend baslatiliyor...")

    # Veritabani tablolarini olustur
    try:
        await init_db()
    except Exception as e:
        logger.error("Veritabani init hatasi: %s", e)

    # Scheduler'i baslat
    try:
        setup_scheduler()
    except Exception as e:
        logger.error("Scheduler baslatilamadi: %s", e)

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

# CORS — production'da spesifik origin, gelistirmede *
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
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
# Admin sifre dogrulama — timing-safe
# -------------------------------------------------------

def _verify_admin_password(provided: str) -> bool:
    """Timing-safe admin sifre dogrulama. Brute force'a karsi hmac kullanir."""
    admin_pw = settings.ADMIN_PASSWORD
    if not admin_pw or not provided:
        return False
    return hmac.compare_digest(provided.encode("utf-8"), admin_pw.encode("utf-8"))


# -------------------------------------------------------
# Health Check
# -------------------------------------------------------

@app.get("/health")
async def health_check():
    from app.services.notification import is_firebase_initialized
    settings = get_settings()
    return {
        "status": "ok",
        "service": "bist-finans-backend",
        "version": "2.0.0",
        "firebase_initialized": is_firebase_initialized(),
        "telegram_configured": bool(settings.TELEGRAM_BOT_TOKEN),
    }


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
        .limit(20)
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
    result = await db.execute(
        select(IPO)
        .options(
            selectinload(IPO.allocations),
            selectinload(IPO.ceiling_tracks),
        )
        .where(IPO.id == ipo_id)
    )
    ipo = result.scalar_one_or_none()
    if not ipo:
        raise HTTPException(status_code=404, detail="Halka arz bulunamadi")
    return ipo


# -------------------------------------------------------
# TELEGRAM HABER ENDPOINTS (YENi)
# -------------------------------------------------------

@app.get("/api/v1/telegram-news", response_model=list[TelegramNewsOut])
async def list_telegram_news(
    ticker: Optional[str] = Query(None, description="Hisse kodu filtresi"),
    message_type: Optional[str] = Query(None, description="seans_ici_pozitif, borsa_kapali, seans_disi_acilis"),
    sentiment: Optional[str] = Query(None, description="positive, negative, neutral"),
    days: int = Query(7, ge=1, le=30, description="Son kac gun"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    device_id: Optional[str] = Query(None, description="Abonelik kontrolu icin device_id"),
    db: AsyncSession = Depends(get_db),
):
    """Telegram kanalindan gelen AI haberler.

    - Abone DEGiL: BIST 30 hisselerinin son 10 haberi (ucretsiz tanitim)
    - Abone (ana_yildiz): Ana + Yildiz Pazar — tum hisselerin son 20 haberi
    """
    from app.services.news_service import BIST30_TICKERS

    has_paid_sub = False
    active_package = None

    if device_id:
        user_result = await db.execute(
            select(User).where(User.device_id == device_id)
        )
        user = user_result.scalar_one_or_none()
        if user:
            sub_result = await db.execute(
                select(UserSubscription).where(
                    and_(
                        UserSubscription.user_id == user.id,
                        UserSubscription.is_active == True,
                        UserSubscription.package == "ana_yildiz",
                    )
                )
            )
            sub = sub_result.scalar_one_or_none()
            if sub:
                has_paid_sub = True
                active_package = sub.package

    # Tarih filtresi
    since = datetime.utcnow() - timedelta(days=days)

    if has_paid_sub:
        # Ucretli abone (ana_yildiz): tum hisselerin haberleri (max 50, sayfa basi 25)
        query = (
            select(TelegramNews)
            .where(TelegramNews.created_at >= since)
            .order_by(desc(TelegramNews.created_at))
        )

        if ticker:
            query = query.where(TelegramNews.ticker == ticker.upper())
        if message_type:
            query = query.where(TelegramNews.message_type == message_type)
        if sentiment:
            query = query.where(TelegramNews.sentiment == sentiment)

        query = query.limit(min(limit, 50)).offset(offset)
    else:
        # Ucretsiz: BIST 30 hisselerinin son 10 haberi
        query = (
            select(TelegramNews)
            .where(
                and_(
                    TelegramNews.created_at >= since,
                    TelegramNews.ticker.in_(BIST30_TICKERS),
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

        query = query.limit(min(limit, 10)).offset(offset)

    result = await db.execute(query)
    return list(result.scalars().all())


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
    user_result = await db.execute(
        select(User).where(User.device_id == device_id)
    )
    user = user_result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    # Yillik paket
    if data.is_annual_bundle:
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

        sub = StockNotificationSubscription(
            user_id=user.id,
            ipo_id=None,
            notification_type="all",
            is_annual_bundle=True,
            price_paid_tl=ANNUAL_BUNDLE_PRICE,
            is_active=True,
        )
        db.add(sub)
        await db.flush()
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
    await db.flush()
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

    result = await db.execute(
        update(StockNotificationSubscription)
        .where(
            and_(
                StockNotificationSubscription.user_id == user.id,
                StockNotificationSubscription.is_active == True,
            )
        )
        .values(is_active=False)
    )
    count = result.rowcount
    await db.commit()
    return {"message": f"{count} abonelik iptal edildi", "deactivated_count": count}


@app.patch("/api/v1/users/{device_id}/stock-notifications/{sub_id}/mute")
async def toggle_mute_stock_notification(
    device_id: str,
    sub_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Hisse bildirim aboneligini sessize al / sesi ac (toggle).

    Kullanici tek tusla bildirimi pasif/aktif yapabilir.
    muted=True ise bildirim gelmez, False ise gelir.
    """
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
    """Cihaz kayit — ilk acilista cagrilir."""
    result = await db.execute(
        select(User).where(User.device_id == data.device_id)
    )
    user = result.scalar_one_or_none()

    if user:
        user.fcm_token = data.fcm_token
        if data.expo_push_token:
            user.expo_push_token = data.expo_push_token
        user.platform = data.platform
        user.app_version = data.app_version
    else:
        user = User(
            device_id=data.device_id,
            fcm_token=data.fcm_token,
            expo_push_token=data.expo_push_token,
            platform=data.platform,
            app_version=data.app_version,
        )
        db.add(user)

        subscription = UserSubscription(
            user_id=user.id if user.id else 0,
            package="free",
            is_active=True,
        )
        await db.flush()
        subscription.user_id = user.id
        db.add(subscription)

    await db.flush()
    return user


@app.get("/api/v1/users/{device_id}", response_model=UserOut)
async def get_user(
    device_id: str,
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
    device_id: str,
    data: UserUpdate,
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

    await db.flush()
    return user


@app.delete("/api/v1/users/{device_id}")
async def delete_user_account(
    device_id: str,
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

    # Abonelik kaydini da sil (cascade ile gitmeyebilir — ayri tablo)
    sub_result = await db.execute(
        select(UserSubscription).where(UserSubscription.user_id == user.id)
    )
    sub = sub_result.scalar_one_or_none()
    if sub:
        await db.delete(sub)

    # Kullaniciyi sil — cascade ile tum iliskili veriler silinir
    await db.delete(user)
    await db.commit()

    return {
        "status": "ok",
        "message": "Hesabiniz ve tum verileriniz kalici olarak silindi.",
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
async def get_wallet(device_id: str, db: AsyncSession = Depends(get_db)):
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


@app.post("/api/v1/users/{device_id}/wallet/earn", response_model=WalletBalanceOut)
@limiter.limit("35/minute")
async def wallet_earn(
    request: Request,
    device_id: str,
    data: WalletEarnRequest,
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
    device_id: str,
    data: WalletSpendRequest,
    db: AsyncSession = Depends(get_db),
):
    """Puan harcama — paket satin alma oncesi bakiye kontrolu."""
    result = await db.execute(
        select(User).where(User.device_id == device_id).with_for_update()
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    if data.amount <= 0:
        raise HTTPException(status_code=400, detail="Miktar 0'dan buyuk olmali")

    current_balance = user.wallet_balance or 0.0
    if current_balance < data.amount:
        raise HTTPException(
            status_code=400,
            detail=f"Yetersiz bakiye. Mevcut: {current_balance:.0f}, Gerekli: {data.amount:.0f}"
        )

    # Bakiye dus
    user.wallet_balance = current_balance - data.amount

    # Islem logu
    tx = WalletTransaction(
        user_id=user.id,
        amount=-data.amount,
        tx_type=data.spend_type,
        description=data.description or f"Harcama: {data.spend_type}",
        balance_after=user.wallet_balance,
    )
    db.add(tx)
    await db.flush()

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
@limiter.limit("10/minute")
async def wallet_redeem_coupon(
    request: Request,
    device_id: str,
    data: WalletCouponRequest,
    db: AsyncSession = Depends(get_db),
):
    """Kupon kodu ile puan ekleme — sunucu tarafinda dogrulama."""
    result = await db.execute(
        select(User).where(User.device_id == device_id).with_for_update()
    )
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    code = data.code.upper().strip()

    # Kupon var mi?
    if code not in WALLET_COUPONS:
        raise HTTPException(status_code=400, detail="Gecersiz kupon kodu.")

    # Daha once kullanildi mi? (SZ_ALGO_DENEM01 haric — sinirsiz test kuponu)
    if code != "SZ_ALGO_DENEM01":
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
    amount = WALLET_COUPONS[code]
    user.wallet_balance = (user.wallet_balance or 0.0) + amount

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
async def update_ceiling_track(
    data: CeilingTrackUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Matriks Excel pipeline'indan gelen tavan/taban bilgisini kaydeder."""
    from app.services.ipo_service import IPOService
    from app.services.notification import NotificationService

    ipo_service = IPOService(db)
    notif_service = NotificationService(db)

    ipo = await ipo_service.get_ipo_by_ticker(data.ticker)
    if not ipo:
        raise HTTPException(status_code=404, detail=f"IPO bulunamadi: {data.ticker}")

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
    )

    # Eski + yeni bildirim abonelerini topla
    subs_result = await db.execute(
        select(CeilingTrackSubscription).where(
            and_(
                CeilingTrackSubscription.ipo_id == ipo.id,
                CeilingTrackSubscription.is_active == True,
                CeilingTrackSubscription.tracking_days >= data.trading_day,
            )
        )
    )
    active_subs = list(subs_result.scalars().all())

    stock_notif_result = await db.execute(
        select(StockNotificationSubscription).where(
            and_(
                or_(
                    and_(
                        StockNotificationSubscription.ipo_id == ipo.id,
                        StockNotificationSubscription.notification_type.in_(["tavan_bozulma", "taban_acilma"]),
                    ),
                    StockNotificationSubscription.is_annual_bundle == True,
                ),
                StockNotificationSubscription.is_active == True,
            )
        )
    )
    stock_notif_subs = list(stock_notif_result.scalars().all())

    notifications_sent = 0
    all_subscribers = active_subs + stock_notif_subs

    for sub in all_subscribers:
        user_result = await db.execute(
            select(User).where(User.id == sub.user_id)
        )
        user = user_result.scalar_one_or_none()
        if not user or not user.fcm_token:
            continue
        # Master switch kontrolu
        if not user.notifications_enabled:
            continue

        if not data.hit_ceiling and not track.notified_ceiling_break:
            await notif_service.send_to_device(
                token=user.fcm_token,
                title=f"{data.ticker} Tavan Cozuldu!",
                body=f"{data.ticker} {data.trading_day}. islem gunu tavan bozuldu. Kapanis: {data.close_price} TL",
                data={"type": "ceiling_break", "ticker": data.ticker, "ipo_id": str(ipo.id)},
            )
            notifications_sent += 1

        if data.hit_floor and not track.notified_floor:
            await notif_service.send_to_device(
                token=user.fcm_token,
                title=f"{data.ticker} Tabana Kitlendi!",
                body=f"{data.ticker} {data.trading_day}. islem gunu tabana kitlendi.",
                data={"type": "floor_lock", "ticker": data.ticker, "ipo_id": str(ipo.id)},
            )
            notifications_sent += 1

        if track.relocked and not track.notified_relock:
            await notif_service.send_to_device(
                token=user.fcm_token,
                title=f"{data.ticker} TAVANA KİTLEDİ",
                body=f"{data.ticker} {data.trading_day}. islem gunu tavana kitledi.",
                data={"type": "relock", "ticker": data.ticker, "ipo_id": str(ipo.id)},
            )
            notifications_sent += 1

        if hasattr(sub, 'notified_count'):
            sub.notified_count += 1

    if not data.hit_ceiling:
        track.notified_ceiling_break = True
    if data.hit_floor:
        track.notified_floor = True
    if track.relocked:
        track.notified_relock = True

    await db.flush()
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
    Gercek zamanli bildirim gonder — halka_arz_sync.py'den cagirilir.

    4 bildirim tipi:
      - tavan_bozulma:          Tavan acilinca/kitlenince
      - taban_acilma:           Taban acilinca/kitlenince
      - gunluk_acilis_kapanis:  Gunluk acilis (09:56) ve kapanis (18:08)
      - yuzde_dusus:            Tek hizmet — %4 ve %7 esik, gunde max 2 bildirim
                                sub_event: "pct4" veya "pct7"

    Bildirim mesajlarinda fiyat bilgisi YOKTUR.
    """
    import os
    from app.services.ipo_service import IPOService
    from app.services.notification import NotificationService

    if not _verify_admin_password(data.admin_password):
        raise HTTPException(status_code=403, detail="Yetkisiz")

    valid_types = [
        "tavan_bozulma", "taban_acilma",
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
        raise HTTPException(status_code=404, detail=f"IPO bulunamadi: {data.ticker}")

    notif_service = NotificationService(db)

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
                    # Paket aboneleri (tum halka arzlar icin)
                    StockNotificationSubscription.is_annual_bundle == True,
                ),
                StockNotificationSubscription.is_active == True,
                StockNotificationSubscription.muted == False,
            )
        )
    )
    active_subs = list(stock_notif_result.scalars().all())

    notifications_sent = 0
    errors = 0

    for sub in active_subs:
        # Paket aboneleri icin: "all" tipinde kayitli, her bildirim tipini alir
        if sub.is_annual_bundle and sub.notification_type != "all":
            pass

        notif_title = data.title
        notif_body = data.body

        user_result = await db.execute(
            select(User).where(User.id == sub.user_id)
        )
        user = user_result.scalar_one_or_none()
        if not user or not user.fcm_token:
            continue
        # Master switch kontrolu — tum bildirimler kapaliysa atla
        if not user.notifications_enabled:
            continue

        try:
            await notif_service.send_to_device(
                token=user.fcm_token,
                title=notif_title,
                body=notif_body,
                data={
                    "type": "stock_notification",
                    "notification_type": data.notification_type,
                    "ticker": data.ticker,
                    "ipo_id": str(ipo.id),
                },
            )
            notifications_sent += 1
            sub.notified_count = (sub.notified_count or 0) + 1
        except Exception as e:
            errors += 1
            logging.warning(f"Bildirim gonderilemedi (user={user.id}): {e}")

    await db.flush()

    return {
        "status": "ok",
        "ticker": data.ticker,
        "notification_type": data.notification_type,
        "active_subscribers": len(active_subs),
        "notifications_sent": notifications_sent,
        "errors": errors,
    }


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
            )

            # trading_day_count guncelle
            if trading_day > (ipo.trading_day_count or 0):
                ipo.trading_day_count = trading_day

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

    return {
        "status": "ok",
        "loaded": len(results),
        "errors": len(errors),
        "results": results,
        "error_details": errors,
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

    event = payload.get("event", {})
    event_type = event.get("type", "")
    app_user_id = event.get("app_user_id", "")
    product_id = event.get("product_id", "")

    logger.info(f"RevenueCat webhook: {event_type} -- {app_user_id} -- {product_id}")

    result = await db.execute(
        select(User).where(User.device_id == app_user_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        return {"status": "user_not_found"}

    # Haber abonelikleri
    news_package_map = {
        "bist_finans_yildiz_monthly": "yildiz_pazar",
        "bist_finans_yildiz_annual": "yildiz_pazar",
        "bist_finans_ana_yildiz_monthly": "ana_yildiz",
        "bist_finans_ana_yildiz_annual": "ana_yildiz",
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
            )

            ipo_id = event.get("metadata", {}).get("ipo_id")

            # Fiyat belirleme
            bundle_prices = {
                "bist_finans_notif_annual": ANNUAL_BUNDLE_PRICE,
                "bist_finans_notif_quarterly": QUARTERLY_PRICE,
                "bist_finans_notif_combo": COMBO_PRICE,
            }
            price = bundle_prices.get(product_id) or NOTIFICATION_TIER_PRICES.get(notif_type, {}).get("price_tl", 0)

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
            )
            db.add(sub)

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
    return {"status": "ok"}


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
            tweet_allocation_results(ipo, alloc_dicts)
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
                    channel_id="bist_finans_channel",
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

    result = await db.execute(
        select(User).order_by(desc(User.created_at)).limit(20)
    )
    users = result.scalars().all()

    return [
        {
            "id": u.id,
            "device_id": u.device_id[:8] + "...",
            "platform": u.platform,
            "fcm_token": u.fcm_token[:30] + "..." if u.fcm_token and len(u.fcm_token) > 30 else u.fcm_token,
            "expo_push_token": getattr(u, "expo_push_token", None),
            "app_version": u.app_version,
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
# Admin: Gunluk kapanis fiyatlarini Yahoo Finance'den cek
# -------------------------------------------------------

@app.post("/api/v1/admin/fill-ceiling-data")
@limiter.limit("5/minute")
async def admin_fill_ceiling_data(request: Request, payload: dict, db: AsyncSession = Depends(get_db)):
    """Yahoo Finance'den gunluk OHLC verisini cekerek ipo_ceiling_tracks tablosunu doldurur.

    status='trading' olan ve ceiling track verisi eksik olan tum IPO'lar icin:
    - trading_start tarihinden itibaren gunluk OHLC verisi cekilir
    - Tavan/taban tespiti yapilir
    - ipo_ceiling_tracks tablosuna yazilir
    - trading_day_count guncellenir
    """
    settings = get_settings()
    if not _verify_admin_password(payload.get("admin_password", "")):
        raise HTTPException(status_code=403, detail="Yetkisiz erisim")

    from app.scrapers.yahoo_finance_scraper import YahooFinanceScraper, detect_ceiling_floor
    from app.services.ipo_service import IPOService

    # Hangi IPO'lari guncelleyecegimizi bul
    # status='trading' ve trading_start dolmus olan IPO'lar
    result = await db.execute(
        select(IPO).where(
            and_(
                IPO.status == "trading",
                IPO.trading_start.isnot(None),
            )
        ).order_by(IPO.trading_start.desc())
    )
    trading_ipos = result.scalars().all()

    if not trading_ipos:
        return {"success": True, "message": "Guncellenecek IPO bulunamadi", "results": []}

    scraper = YahooFinanceScraper()
    ipo_service = IPOService(db)
    results_list = []

    try:
        for ipo in trading_ipos:
            if not ipo.ticker or not ipo.trading_start:
                continue

            # Yahoo Finance'den OHLC verisi cek
            days_data = await scraper.fetch_ohlc_since_trading_start(
                ticker=ipo.ticker,
                trading_start=ipo.trading_start,
                max_days=25,
            )

            if not days_data:
                results_list.append({
                    "ticker": ipo.ticker,
                    "status": "no_data",
                    "days_found": 0,
                })
                continue

            # Her gun icin ceiling track kaydi olustur/guncelle
            days_written = 0
            prev_close = None

            # Eger IPO fiyati varsa, ilk gun icin referans olarak kullan
            if ipo.ipo_price:
                prev_close = ipo.ipo_price

            for day in days_data:
                # Tavan/taban tespit et
                detection = detect_ceiling_floor(
                    close_price=day["close"],
                    prev_close=prev_close,
                    high_price=day.get("high"),
                    low_price=day.get("low"),
                )

                # Ceiling track kaydi olustur
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

                # Durum ve pct_change ayarla
                if track:
                    track.durum = detection["durum"]
                    track.pct_change = detection["pct_change"]

                prev_close = day["close"]
                days_written += 1

            # trading_day_count guncelle
            ipo.trading_day_count = len(days_data)

            # first_day_close_price guncelle
            if days_data and not ipo.first_day_close_price:
                ipo.first_day_close_price = days_data[0]["close"]

            results_list.append({
                "ticker": ipo.ticker,
                "status": "updated",
                "days_found": len(days_data),
                "days_written": days_written,
                "trading_day_count": len(days_data),
            })

    finally:
        await scraper.close()

    await db.commit()

    total_updated = sum(1 for r in results_list if r["status"] == "updated")
    return {
        "success": True,
        "ipos_processed": len(results_list),
        "ipos_updated": total_updated,
        "results": results_list,
    }


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
