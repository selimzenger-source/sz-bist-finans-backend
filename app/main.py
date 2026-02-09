"""BIST Finans Backend — FastAPI Ana Uygulama.

Servisler:
1. Halka Arz Takip (ucretsiz)
2. Tavan/Taban Takip — hisse bazli ucretli paketler (5/10/15/20 gun)
3. Hisse Bazli Bildirim Aboneligi — 5 tip (tavan_bozulma/taban_acilma/gunluk_acilis_kapanis/yuzde4_dusus/yuzde7_dusus)
   + Kombo (45 TL) + 3 Aylik (195 TL) + 6 Aylik (295 TL) + Yillik (395 TL)
4. Yapay Zeka Haber Takibi — Telegram kanal entegrasyonu (bist100/yildiz/ana_yildiz)
5. KAP Haber Bildirimleri
"""

import logging
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select, desc, and_, or_
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
    COMBO_PRICE, QUARTERLY_PRICE, SEMIANNUAL_PRICE,
    ANNUAL_BUNDLE_PRICE, COMBINED_ANNUAL_DISCOUNT_PCT,
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
    DividendOut,
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
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static dosyalar (admin panel logo vb.)
import os
_static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# Admin Panel
app.include_router(admin_router)


# -------------------------------------------------------
# Health Check
# -------------------------------------------------------

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "bist-finans-backend", "version": "2.0.0"}


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
    """Halka arz ana ekrani — 5 bolum + arsiv sayisi.

    1. SPK Onayi Beklenen: spk_applications tablosundan pending olanlar
    2. Yeni Onaylanan: SPK onayli, talep toplama henuz baslamamislar
    3. Dagitim Surecinde: Talep toplama acik (in_distribution)
    4. Islem Gunu Beklenen: Dagitim bitmis, islem tarihi bekleniyor
    5. Isleme Baslayanlar: Borsada islem goren, 25 gun takip
    """
    from sqlalchemy import func as sa_func

    # 1. SPK Onayi Beklenen — spk_applications tablosu
    spk_result = await db.execute(
        select(SPKApplication)
        .where(SPKApplication.status == "pending")
        .order_by(SPKApplication.created_at.desc())
        .limit(50)
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

    # 5. Isleme Baslayanlar — trading status, arsivlenmemis
    trading_result = await db.execute(
        select(IPO)
        .where(
            and_(
                IPO.status == "trading",
                IPO.archived == False,
            )
        )
        .order_by(IPO.trading_start.desc().nullslast())
        .limit(30)
    )
    trading = list(trading_result.scalars().all())

    # Arsiv sayisi
    archived_count_result = await db.execute(
        select(sa_func.count(IPO.id)).where(IPO.archived == True)
    )
    archived_count = archived_count_result.scalar() or 0

    return IPOSectionsOut(
        spk_pending=spk_pending,
        newly_approved=newly_approved,
        in_distribution=in_distribution,
        awaiting_trading=awaiting_trading,
        trading=trading,
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
    message_type: Optional[str] = Query(None, description="seans_ici_pozitif, seans_ici_negatif, borsa_kapali, seans_disi_acilis"),
    sentiment: Optional[str] = Query(None, description="positive, negative, neutral"),
    days: int = Query(7, ge=1, le=30, description="Son kac gun"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    device_id: Optional[str] = Query(None, description="Abonelik kontrolu icin device_id"),
    db: AsyncSession = Depends(get_db),
):
    """Telegram kanalindan gelen AI haberler — sadece abonelere acik."""
    # Abonelik kontrolu
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
                        UserSubscription.package.in_(["bist100", "yildiz_pazar", "ana_yildiz"]),
                    )
                )
            )
            sub = sub_result.scalar_one_or_none()
            if not sub:
                raise HTTPException(
                    status_code=403,
                    detail="AI Haber Takibi icin abonelik gerekli."
                )
        else:
            raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

    # Tarih filtresi
    since = datetime.utcnow() - timedelta(days=days)

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

    query = query.limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


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


# -------------------------------------------------------
# KULLANICI ENDPOINTS
# -------------------------------------------------------

@app.post("/api/v1/users/register", response_model=UserOut)
async def register_device(data: UserRegister, db: AsyncSession = Depends(get_db)):
    """Cihaz kayit — ilk acilista cagrilir."""
    result = await db.execute(
        select(User).where(User.device_id == data.device_id)
    )
    user = result.scalar_one_or_none()

    if user:
        user.fcm_token = data.fcm_token
        user.platform = data.platform
        user.app_version = data.app_version
    else:
        user = User(
            device_id=data.device_id,
            fcm_token=data.fcm_token,
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
    for key, value in update_data.items():
        if hasattr(user, key):
            setattr(user, key, value)

    await db.flush()
    return user


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
                title=f"{data.ticker} Tekrar Tavana Kitlendi!",
                body=f"{data.ticker} {data.trading_day}. islem gunu tekrar tavana kitlendi.",
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
async def revenuecat_webhook(payload: dict, db: AsyncSession = Depends(get_db)):
    """RevenueCat webhook — abonelik olaylari."""
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
        "bist_finans_bist100_monthly": "bist100",
        "bist_finans_bist100_annual": "bist100",
        "bist_finans_yildiz_monthly": "yildiz_pazar",
        "bist_finans_yildiz_annual": "yildiz_pazar",
        "bist_finans_ana_yildiz_monthly": "ana_yildiz",
        "bist_finans_ana_yildiz_annual": "ana_yildiz",
        # Eski paketler
        "bist_finans_bist30_monthly": "bist100",
        "bist_finans_bist50_monthly": "bist100",
        "bist_finans_all_monthly": "ana_yildiz",
    }

    notif_package_map = {
        "bist_finans_notif_tavan": "tavan_bozulma",
        "bist_finans_notif_taban": "taban_acilma",
        "bist_finans_notif_acilis": "gunluk_acilis_kapanis",
        "bist_finans_notif_yuzde4": "yuzde4_dusus",
        "bist_finans_notif_yuzde7": "yuzde7_dusus",
        "bist_finans_notif_combo": "combo",
        "bist_finans_notif_quarterly": "quarterly",
        "bist_finans_notif_semiannual": "semiannual",
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
                "bist_finans_notif_semiannual",
                "bist_finans_notif_quarterly",
            )

            ipo_id = event.get("metadata", {}).get("ipo_id")

            # Fiyat belirleme
            bundle_prices = {
                "bist_finans_notif_annual": ANNUAL_BUNDLE_PRICE,
                "bist_finans_notif_semiannual": SEMIANNUAL_PRICE,
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
