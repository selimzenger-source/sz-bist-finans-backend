"""Admin broadcast bildirim servisi — toplu duyuru gonderimi.

Mevcut notification.py'ye dokunmadan ayri bir modul.
Sadece okuma sorgusu + send_to_device() cagrisi yapar.
Kullanici tablosuna hicbir yazma islemi yapmaz.
"""

import asyncio
import functools
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
    error_details: list[str] = []  # Hata detaylari — Telegram raporuna eklenir

    try:
        async with async_session() as db:
            # Hedef kullanicilari sorgula
            users = await _get_target_users(db, audience)
            total = len(users)

            if total == 0:
                logger.info("Broadcast: Hedef kitle bos — hicbir bildirim gonderilmedi")
                await _send_telegram_report(title, audience, deep_link_target, 0, 0, 0, [])
                return

            # Firebase dogrudan kullan — NotificationService yerine
            # (NotificationService._clear_stale_token session sorunlari onlemek icin)
            from app.services.notification import _init_firebase, is_firebase_initialized
            _init_firebase()

            if not is_firebase_initialized():
                logger.error("Broadcast: Firebase baslatılamadı — iptal")
                await _send_telegram_report(
                    title, audience, deep_link_target, total, 0, total,
                    ["Firebase baslatılamadı"],
                )
                return

            from firebase_admin import messaging

            # Data payload — tum value'lar STRING olmali
            safe_data = {
                "type": "announcement",
                "target": str(deep_link_target),
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
                        error_details.append(f"User {user.id}: token bos")
                        continue

                    # Token debug — ilk 30 karakteri logla
                    logger.info(
                        "Broadcast: User %d gonderiliyor (token: %s...%s, len=%d)",
                        user.id, token[:15], token[-5:], len(token),
                    )

                    message = messaging.Message(
                        notification=messaging.Notification(
                            title=title,
                            body=body,
                        ),
                        data=safe_data,
                        token=token,
                        android=messaging.AndroidConfig(
                            priority="high",
                            notification=messaging.AndroidNotification(
                                sound="default",
                                channel_id="default_v2",
                                default_vibrate_timings=True,
                                notification_priority="PRIORITY_MAX",
                                visibility="PUBLIC",
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

                    # messaging.send() senkron blocking I/O — thread pool'da calistir
                    # (async event loop'u bloke etmesin)
                    loop = asyncio.get_event_loop()
                    response = await loop.run_in_executor(
                        None, functools.partial(messaging.send, message)
                    )
                    sent += 1
                    logger.info(
                        "Broadcast: User %d OK — %s",
                        user.id, response,
                    )

                    # 2sn throttle — Firebase rate limit koruması
                    await asyncio.sleep(2)

                except Exception as e:
                    error_name = type(e).__name__
                    error_msg = str(e)[:120]
                    failed += 1
                    error_details.append(
                        f"User {user.id} ({error_name}): {error_msg}"
                    )
                    logger.warning(
                        "Broadcast: User %d FAILED (%s): %s (token: %s...)",
                        user.id, error_name, error_msg, token[:20],
                    )

        logger.info(
            "Broadcast tamamlandi: '%s' — %d/%d basarili, %d basarisiz",
            title, sent, total, failed,
        )

    except Exception as e:
        logger.error("Broadcast background task hatasi: %s", e)
        error_details.append(f"GENEL HATA: {type(e).__name__}: {e}")

    # Telegram rapor — hata detaylari ile
    await _send_telegram_report(title, audience, deep_link_target, total, sent, failed, error_details)


async def _send_telegram_report(
    title: str,
    audience: str,
    deep_link_target: str,
    total: int,
    sent: int,
    failed: int,
    error_details: list[str] | None = None,
):
    """Broadcast sonucunu admin Telegram'a raporla (hata detaylari dahil)."""
    audience_labels = {"all": "Tum Kullanicilar", "paid": "Ucretli Aboneler", "free": "Ucretsiz Kullanicilar"}
    target_labels = {
        "none": "Yok",
        "halka-arz": "Halka Arz",
        "ai-haberler": "KAP Haberler",
        "ayarlar": "Ayarlar",
    }

    msg = (
        f"📢 <b>Broadcast Gonderildi</b>\n\n"
        f"<b>Baslik:</b> {title}\n"
        f"<b>Hedef:</b> {audience_labels.get(audience, audience)}\n"
        f"<b>Yonlendirme:</b> {target_labels.get(deep_link_target, deep_link_target)}\n"
        f"<b>Toplam hedef:</b> {total}\n"
        f"<b>Basarili:</b> {sent}\n"
        f"<b>Basarisiz:</b> {failed}"
    )

    # Hata detaylari varsa ekle (ilk 5 hatayi goster — Telegram mesaj limiti)
    if error_details:
        shown = error_details[:5]
        msg += "\n\n<b>Hata Detaylari:</b>\n"
        for err in shown:
            msg += f"• <code>{err[:150]}</code>\n"
        if len(error_details) > 5:
            msg += f"... ve {len(error_details) - 5} hata daha"

    try:
        from app.services.admin_telegram import send_admin_message
        await send_admin_message(msg)
    except Exception as e:
        logger.warning("Broadcast Telegram rapor hatasi: %s", e)
