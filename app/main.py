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

from fastapi import FastAPI, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel
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
    WalletTransaction, Coupon, WALLET_COUPONS,
    WALLET_REWARD_AMOUNT, WALLET_COOLDOWN_SECONDS, WALLET_MAX_DAILY_ADS,
    Dividend, DividendHistory,
    KapAllDisclosure, UserWatchlist,
    FeatureInterest,
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
    StockNotificationCreate, StockNotificationOut, StockNotificationSyncRequest,
    RealtimeNotifRequest,
    DividendOut,
    WalletBalanceOut, WalletEarnRequest, WalletSpendRequest,
    WalletCouponRequest, WalletTransactionOut,
    KapAllDisclosureOut, WatchlistItemOut, WatchlistAddRequest,
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

            # Mevcut duplicate'lari temizle (en eski kaydi tut)
            await db.execute(sa_text("""
                DELETE FROM kap_all_disclosures
                WHERE id NOT IN (
                    SELECT MIN(id) FROM kap_all_disclosures
                    GROUP BY company_code, title
                )
            """))
            await db.commit()
            # Unique index olustur
            await db.execute(sa_text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_kap_company_title "
                "ON kap_all_disclosures (company_code, title)"
            ))
            await db.commit()
            logger.info("KAP unique constraint OK")
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
    db: AsyncSession = Depends(get_db),
):
    """Telegram kanalindan gelen AI haberler.

    - Abone DEGiL: BIST 50 hisselerinin son 20 haberi (ucretsiz tanitim)
    - Abone (ana_yildiz): Ana + Yildiz Pazar — tum hisselerin son 30 haberi
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

        query = query.limit(min(limit, 20)).offset(offset)

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
    # Row-level lock — ayni kullanicidan gelen esanli isteklerde race condition engellenir
    user_result = await db.execute(
        select(User).where(User.device_id == device_id).with_for_update()
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

        await db.flush()
        subscription = UserSubscription(
            user_id=user.id,
            package="free",
            is_active=True,
        )
        db.add(subscription)

    await db.flush()
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

    # Guvenlik agi: FCM token guncellendiyse ve gecerliyse,
    # notifications_enabled acik degilse otomatik ac
    # (kullanici izin verdiyse token gelir — bildirimleri de acik olmali)
    new_fcm = update_data.get("fcm_token")
    if new_fcm and "notifications_enabled" not in update_data:
        if not user.notifications_enabled:
            user.notifications_enabled = True

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

    # Abonelik kaydini da sil (cascade ile gitmeyebilir — ayri tablo)
    sub_result = await db.execute(
        select(UserSubscription).where(UserSubscription.user_id == user.id)
    )
    sub = sub_result.scalar_one_or_none()
    if sub:
        await db.delete(sub)

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
                    f"Kullanan: {device_id[:12]}…\n"
                    f"Kullanım: {db_coupon.uses_count}/{db_coupon.max_uses} ✅\n"
                    f"Tüm haklar kullanıldı!",
                    silent=False,
                )
            else:
                await send_admin_message(
                    f"🎟 <b>Kupon Kullanıldı!</b>\n"
                    f"Kod: <code>{code}</code>\n"
                    f"Puan: +{int(amount)}\n"
                    f"Kullanan: {device_id[:12]}…\n"
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
    from app.services.ipo_service import IPOService
    from app.services.notification import NotificationService

    if not _verify_admin_password(data.admin_password):
        raise HTTPException(status_code=403, detail="Yetkisiz")

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
                        "screen": "bildirim-merkezi",
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
                        "screen": "bildirim-merkezi",
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
            raise HTTPException(status_code=404, detail="Kullanici bulunamadi")

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
                bundle_days = 365 if "annual" in product_id else 90
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
        # Eski paketler (geriye donuk uyumluluk)
        "bist_finans_bist100_monthly", "bist_finans_bist100_annual",
        "bist_finans_bist30_monthly", "bist_finans_bist50_monthly",
        "bist_finans_all_monthly",
    }
    if body.is_active and package != "free":
        if body.store not in ("play_store", "app_store"):
            raise HTTPException(status_code=400, detail="Gecerli store gerekli (play_store veya app_store)")
        if not body.product_id or body.product_id not in KNOWN_NEWS_PRODUCTS:
            raise HTTPException(status_code=400, detail="Gecersiz veya eksik product_id")
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
# Admin: BIST 50 Endeks Yonetimi
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
        lines.append(f"<b>Cihaz ID:</b> <code>{body.device_id[:16]}</code>")
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
        f"<b>Cihaz:</b> <code>{body.device_id[:16]}</code>",
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
    min_score: Optional[float] = Query(None, ge=0, le=10, description="Minimum AI etki skoru (pozitif filtre icin 6.0)"),
    max_score: Optional[float] = Query(None, ge=0, le=10, description="Maksimum AI etki skoru (negatif filtre icin 5.0)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Tum KAP bildirimleri — herkes erisebilir.

    Filtreler:
    - ticker: Hisse kodu (orn: THYAO)
    - hours: Son kac saat (1, 24, 168, 720)
    - min_score: Minimum AI etki skoru (>=)
    - max_score: Maksimum AI etki skoru (<)
    - limit/offset: Sayfalama
    """
    query = select(KapAllDisclosure).order_by(desc(KapAllDisclosure.created_at))

    if ticker:
        query = query.where(KapAllDisclosure.company_code == ticker.upper())

    if hours:
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        query = query.where(KapAllDisclosure.created_at >= since)

    if min_score is not None:
        query = query.where(KapAllDisclosure.ai_impact_score.isnot(None))
        query = query.where(KapAllDisclosure.ai_impact_score >= min_score)

    if max_score is not None:
        query = query.where(KapAllDisclosure.ai_impact_score.isnot(None))
        query = query.where(KapAllDisclosure.ai_impact_score < max_score)

    query = query.limit(limit).offset(offset)
    result = await db.execute(query)
    return list(result.scalars().all())


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

    Free: max 3 hisse. VIP (ana_yildiz): sinirsiz.
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

    # Zaten takip ediliyor mu?
    existing = await db.execute(
        select(UserWatchlist).where(
            UserWatchlist.device_id == device_id,
            UserWatchlist.ticker == ticker,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Bu hisse zaten takip listenizde")

    # VIP kontrolu — ana_yildiz abonesi mi? (is_active VEYA expires_at gelecekte)
    is_vip = False
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
    if sub_result.scalar_one_or_none():
        is_vip = True

    # Limit kontrolu (Free: 5, VIP: 25)
    count_result = await db.execute(
        select(func.count(UserWatchlist.id)).where(
            UserWatchlist.device_id == device_id
        )
    )
    current_count = count_result.scalar() or 0

    if is_vip:
        if current_count >= 25:
            raise HTTPException(
                status_code=403,
                detail="VIP kullanicilar en fazla 25 hisse takip edebilir."
            )
    else:
        if current_count >= 5:
            raise HTTPException(
                status_code=403,
                detail="Ucretsiz kullanicilar en fazla 5 hisse takip edebilir. VIP'e yukselin!"
            )

    pref = body.notification_preference if body.notification_preference in ("both", "positive_only", "negative_only", "all", "positive_negative") else "both"
    item = UserWatchlist(device_id=device_id, ticker=ticker, notification_preference=pref)
    db.add(item)
    await db.flush()

    return {"success": True, "ticker": ticker, "is_vip": is_vip}


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


@app.post("/api/v1/users/{device_id}/kap-watchlist/trim")
async def trim_watchlist(
    device_id: str,
    db: AsyncSession = Depends(get_db),
):
    """VIP → Free geçişinde watchlist'i FREE limiti (5) ile sınırla.

    En eski eklenen hisseler korunur, fazlası silinir.
    """
    FREE_LIMIT = 5

    # VIP kontrolü — hâlâ VIP ise trim yapma
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
                UserSubscription.package == "ana_yildiz",
                or_(
                    UserSubscription.is_active == True,
                    UserSubscription.expires_at > datetime.utcnow(),
                ),
            )
        )
    )
    if sub_result.scalar_one_or_none():
        # Hâlâ VIP — trim gerekmiyor
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
            "data": log.data_json,
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

    if not ipo_id or event_type not in ("ticker_assigned", "trading_date_detected"):
        raise HTTPException(status_code=400, detail="ipo_id ve event_type (ticker_assigned|trading_date_detected) gerekli")

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

    return {"status": "ok", "ipo": ipo.company_name, "ticker": ipo.ticker, "event": event_type, "results": results}


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
        sentiment: str (default "Notr") — "Olumlu" | "Olumsuz" | "Notr"
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

    sentiment = payload.get("sentiment", "Notr")
    if sentiment not in ("Olumlu", "Olumsuz", "Notr"):
        sentiment = "Notr"
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
