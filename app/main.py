"""BIST Finans Backend — FastAPI Ana Uygulama.

3 Servis:
1. Halka Arz Takip (ucretsiz)
2. AI KAP Haber Bildirimleri (ucretli)
3. Devre Kesici Bildirimi (v2 — ileride)
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import get_settings
from app.database import get_db, init_db
from app.models import (
    IPO, IPOBroker, IPOAllocation, IPOCeilingTrack,
    KapNews, User, UserSubscription, UserIPOAlert,
)
from app.schemas import (
    IPOListOut, IPODetailOut, KapNewsOut,
    UserRegister, UserUpdate, UserOut, SubscriptionInfo,
    IPOAlertCreate, CeilingTrackUpdate,
)
from app.scheduler import setup_scheduler, shutdown_scheduler

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

    # Veritabani tablolarini olustur (production'da Alembic kullan)
    if not settings.is_production:
        await init_db()

    # Scheduler'i baslat
    setup_scheduler()

    yield

    # Kapanis
    shutdown_scheduler()
    logger.info("BIST Finans Backend kapatildi.")


# -------------------------------------------------------
# FastAPI Uygulamasi
# -------------------------------------------------------

app = FastAPI(
    title="BIST Finans API",
    description="Halka Arz Takip + AI KAP Haber Bildirimleri",
    version="1.0.0",
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


# -------------------------------------------------------
# Health Check
# -------------------------------------------------------

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "bist-finans-backend"}


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


@app.get("/api/v1/ipos/{ipo_id}", response_model=IPODetailOut)
async def get_ipo_detail(ipo_id: int, db: AsyncSession = Depends(get_db)):
    """Halka arz detay — halkarz.com formati.

    Tum bilgileri icerir: sirket, fiyat, tarihler, araci kurumlar,
    tahsisat dagilimlari, tavan takip verileri.
    """
    result = await db.execute(
        select(IPO)
        .options(
            selectinload(IPO.brokers),
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
# KULLANICI ENDPOINTS
# -------------------------------------------------------

@app.post("/api/v1/users/register", response_model=UserOut)
async def register_device(data: UserRegister, db: AsyncSession = Depends(get_db)):
    """Cihaz kayit — ilk acilista cagrilir."""
    # Mevcut kayit kontrol
    result = await db.execute(
        select(User).where(User.device_id == data.device_id)
    )
    user = result.scalar_one_or_none()

    if user:
        # Token guncelle
        user.fcm_token = data.fcm_token
        user.platform = data.platform
        user.app_version = data.app_version
    else:
        # Yeni kullanici
        user = User(
            device_id=data.device_id,
            fcm_token=data.fcm_token,
            platform=data.platform,
            app_version=data.app_version,
        )
        db.add(user)

        # Ucretsiz abonelik olustur
        subscription = UserSubscription(
            user_id=user.id if user.id else 0,  # flush sonrasi guncellenecek
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

    # Mevcut alert kontrol
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
    """Matriks Excel pipeline'indan gelen tavan bilgisini kaydeder.

    Eger tavan bozulduysa push bildirim gonderir.
    """
    from app.services.ipo_service import IPOService
    from app.services.notification import NotificationService

    ipo_service = IPOService(db)
    notif_service = NotificationService(db)

    # Ticker ile IPO bul
    ipo = await ipo_service.get_ipo_by_ticker(data.ticker)
    if not ipo:
        raise HTTPException(status_code=404, detail=f"IPO bulunamadi: {data.ticker}")

    # Tavan takip guncelle
    track = await ipo_service.update_ceiling_track(
        ipo_id=ipo.id,
        trading_day=data.trading_day,
        trade_date=data.trade_date,
        close_price=data.close_price,
        hit_ceiling=data.hit_ceiling,
    )

    # Tavan bozulduysa bildirim gonder
    if not data.hit_ceiling and not track.notified:
        await notif_service.notify_ceiling_broken(ipo)
        track.notified = True

    await db.flush()
    return {"status": "ok", "ticker": data.ticker, "hit_ceiling": data.hit_ceiling}


# -------------------------------------------------------
# REVENUECAT WEBHOOK (Abonelik Guncelleme)
# -------------------------------------------------------

@app.post("/api/v1/webhooks/revenuecat")
async def revenuecat_webhook(payload: dict, db: AsyncSession = Depends(get_db)):
    """RevenueCat webhook — abonelik olaylari.

    RevenueCat App Store/Play Store satin almalarini yonetir
    ve webhook ile backend'i bilgilendirir.
    """
    event = payload.get("event", {})
    event_type = event.get("type", "")
    app_user_id = event.get("app_user_id", "")
    product_id = event.get("product_id", "")

    logger.info(f"RevenueCat webhook: {event_type} — {app_user_id}")

    # Kullaniciyi bul
    result = await db.execute(
        select(User).where(User.device_id == app_user_id)
    )
    user = result.scalar_one_or_none()
    if not user:
        return {"status": "user_not_found"}

    # Aboneligi bul veya olustur
    result = await db.execute(
        select(UserSubscription).where(UserSubscription.user_id == user.id)
    )
    sub = result.scalar_one_or_none()

    if not sub:
        sub = UserSubscription(user_id=user.id, package="free")
        db.add(sub)

    # Product ID → paket eslestirmesi
    package_map = {
        "bist_finans_bist30_monthly": "bist30",
        "bist_finans_bist50_monthly": "bist50",
        "bist_finans_bist100_monthly": "bist100",
        "bist_finans_all_monthly": "all",
    }

    if event_type in ["INITIAL_PURCHASE", "RENEWAL", "PRODUCT_CHANGE"]:
        sub.package = package_map.get(product_id, "free")
        sub.is_active = True
        sub.product_id = product_id
        sub.revenue_cat_id = app_user_id
        sub.store = event.get("store", "")
        # Bitis tarihini ayarla
        expiration = event.get("expiration_at_ms")
        if expiration:
            from datetime import datetime
            sub.expires_at = datetime.fromtimestamp(expiration / 1000)

    elif event_type in ["CANCELLATION", "EXPIRATION"]:
        sub.is_active = False
        sub.package = "free"

    await db.flush()
    return {"status": "ok"}
