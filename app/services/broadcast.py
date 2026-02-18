"""Admin broadcast bildirim servisi — toplu duyuru gonderimi.

Mevcut notification.py'ye dokunmadan ayri bir modul.
Sadece okuma sorgusu + send_to_device() cagrisi yapar.
Kullanici tablosuna hicbir yazma islemi yapmaz.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, and_, func, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User, UserSubscription, StockNotificationSubscription

logger = logging.getLogger(__name__)

# -------------------------------------------------------
# Rate limit — in-memory cooldown (Render restart temizler)
# -------------------------------------------------------
_last_broadcast_time: Optional[datetime] = None
BROADCAST_COOLDOWN_SECONDS = 300  # 5 dakika


def can_broadcast() -> tuple[bool, int]:
    """Rate limit kontrolu.

    Returns:
        (True, 0) gonderim yapilabilir
        (False, kalan_saniye) cooldown aktif
    """
    global _last_broadcast_time
    if _last_broadcast_time is None:
        return True, 0
    elapsed = (datetime.now(timezone.utc) - _last_broadcast_time).total_seconds()
    if elapsed >= BROADCAST_COOLDOWN_SECONDS:
        return True, 0
    return False, int(BROADCAST_COOLDOWN_SECONDS - elapsed)


def mark_broadcast_sent():
    """Broadcast gonderildi — cooldown baslasin."""
    global _last_broadcast_time
    _last_broadcast_time = datetime.now(timezone.utc)


# -------------------------------------------------------
# Hedef kitle sorgulari
# -------------------------------------------------------

def _base_filter():
    """Tum broadcast sorgularinda ortak filtre.

    Sadece FCM token'i olan kullanicilari dahil et.
    Expo Push Token tek basina bildirim gondermek icin yeterli degil
    — Firebase (FCM) uzerinden gonderim yapiyoruz.
    """
    return and_(
        User.notifications_enabled == True,
        User.deleted == False,
        User.fcm_token.isnot(None),
        User.fcm_token != "",
    )


def _paid_user_ids_subquery():
    """Ucretli kullanicilarin ID'lerini donduren union subquery.

    Ucretli = aktif KAP aboneligi (ana_yildiz) VEYA aktif yillik hisse paketi
    """
    sub_ids = select(UserSubscription.user_id).where(
        and_(
            UserSubscription.is_active == True,
            UserSubscription.package == "ana_yildiz",
        )
    )
    stock_ids = select(StockNotificationSubscription.user_id).where(
        and_(
            StockNotificationSubscription.is_active == True,
            StockNotificationSubscription.is_annual_bundle == True,
        )
    )
    return union_all(sub_ids, stock_ids).subquery()


async def count_recipients(db: AsyncSession, audience: str) -> int:
    """Hedef kitleye gore alici sayisini hesapla (onizleme icin).

    audience: "all" | "paid" | "free"
    """
    base = _base_filter()

    if audience == "all":
        result = await db.execute(
            select(func.count(User.id)).where(base)
        )
    elif audience == "paid":
        paid_sub = _paid_user_ids_subquery()
        result = await db.execute(
            select(func.count(User.id)).where(
                and_(base, User.id.in_(select(paid_sub.c.user_id)))
            )
        )
    elif audience == "free":
        paid_sub = _paid_user_ids_subquery()
        result = await db.execute(
            select(func.count(User.id)).where(
                and_(base, User.id.notin_(select(paid_sub.c.user_id)))
            )
        )
    else:
        return 0

    return result.scalar() or 0


async def _get_target_users(db: AsyncSession, audience: str) -> list:
    """Hedef kullanicilari sorgula."""
    base = _base_filter()

    if audience == "all":
        result = await db.execute(select(User).where(base))
    elif audience == "paid":
        paid_sub = _paid_user_ids_subquery()
        result = await db.execute(
            select(User).where(
                and_(base, User.id.in_(select(paid_sub.c.user_id)))
            )
        )
    elif audience == "free":
        paid_sub = _paid_user_ids_subquery()
        result = await db.execute(
            select(User).where(
                and_(base, User.id.notin_(select(paid_sub.c.user_id)))
            )
        )
    else:
        return []

    return list(result.scalars().all())


# -------------------------------------------------------
# Arka plan broadcast gorevi
# -------------------------------------------------------

async def broadcast_background_task(
    title: str,
    body: str,
    audience: str,
    deep_link_target: str,
):
    """Broadcast gonderim gorevi — asyncio.create_task() ile cagrilir.

    Kendi async_session acar (HTTP request session'i kapanmis olur).
    """
    from app.database import async_session

    sent = 0
    failed = 0
    total = 0

    try:
        async with async_session() as db:
            # Hedef kullanicilari sorgula
            users = await _get_target_users(db, audience)
            total = len(users)

            if total == 0:
                logger.info("Broadcast: Hedef kitle bos — hicbir bildirim gonderilmedi")
                await _send_telegram_report(title, audience, deep_link_target, 0, 0, 0)
                return

            # NotificationService olustur
            from app.services.notification import NotificationService
            notif_service = NotificationService(db)

            # Data payload
            data = {
                "type": "announcement",
                "target": deep_link_target,
            }

            logger.info(
                "Broadcast baslatiliyor: '%s' → %d kullanici (%s)",
                title, total, audience,
            )

            for user in users:
                try:
                    token = (user.fcm_token or "").strip()
                    if not token:
                        logger.warning(
                            "Broadcast: User %d FCM token bos — atlaniyor", user.id
                        )
                        failed += 1
                        continue

                    success = await notif_service.send_to_device(
                        fcm_token=token,
                        title=title,
                        body=body,
                        data=data,
                        delay=True,  # 2sn throttle — Firebase rate limit koruması
                        channel_id="default_v2",
                    )
                    if success:
                        sent += 1
                        logger.info(
                            "Broadcast: User %d OK (token: %s...)",
                            user.id, token[:20],
                        )
                    else:
                        failed += 1
                        logger.warning(
                            "Broadcast: User %d FAILED (token: %s...)",
                            user.id, token[:20],
                        )
                except Exception as e:
                    logger.warning(
                        "Broadcast: User %d exception: %s", user.id, e
                    )
                    failed += 1

        logger.info(
            "Broadcast tamamlandi: '%s' — %d/%d basarili, %d basarisiz",
            title, sent, total, failed,
        )

    except Exception as e:
        logger.error("Broadcast background task hatasi: %s", e)

    # Telegram rapor
    await _send_telegram_report(title, audience, deep_link_target, total, sent, failed)


async def _send_telegram_report(
    title: str,
    audience: str,
    deep_link_target: str,
    total: int,
    sent: int,
    failed: int,
):
    """Broadcast sonucunu admin Telegram'a raporla."""
    audience_labels = {"all": "Tum Kullanicilar", "paid": "Ucretli Aboneler", "free": "Ucretsiz Kullanicilar"}
    target_labels = {
        "none": "Yok",
        "halka-arz": "Halka Arz",
        "ai-haberler": "KAP Haberler",
        "ayarlar": "Ayarlar",
    }

    try:
        from app.services.admin_telegram import send_admin_message
        await send_admin_message(
            f"📢 <b>Broadcast Gonderildi</b>\n\n"
            f"<b>Baslik:</b> {title}\n"
            f"<b>Hedef:</b> {audience_labels.get(audience, audience)}\n"
            f"<b>Yonlendirme:</b> {target_labels.get(deep_link_target, deep_link_target)}\n"
            f"<b>Toplam hedef:</b> {total}\n"
            f"<b>Basarili:</b> {sent}\n"
            f"<b>Basarisiz:</b> {failed}"
        )
    except Exception as e:
        logger.warning("Broadcast Telegram rapor hatasi: %s", e)
