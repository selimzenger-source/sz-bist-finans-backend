"""Firebase Cloud Messaging (FCM) + Expo Push bildirim servisi.

Kullanicilara halka arz ve KAP haber bildirimlerini gonderir.
FCM token varsa Firebase, ExponentPushToken varsa Expo Push API kullanilir.
Ust uste seri bildirim onlemek icin her bildirim arasi 5 saniye beklenir.
"""

import asyncio
import json
import logging
import time
from typing import Optional

from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

# Bildirimler arasi bekleme suresi (saniye) — seri bildirim onleme
NOTIFICATION_DELAY_SECONDS = 2

logger = logging.getLogger(__name__)

# ─── Watchlist Bildirim Spam Korumasi ───
# Ayni ticker icin ayni kullaniciya belirli sure icinde tekrar bildirim gondermeyi engeller.
# Key: "device_id:ticker" → Value: son gonderim zamani (epoch)
_watchlist_notif_cache: dict[str, float] = {}
_WATCHLIST_NOTIF_COOLDOWN = 300  # 5 dakika — ayni ticker icin minimum bekleme suresi
_WATCHLIST_CACHE_CLEANUP_INTERVAL = 3600  # 1 saat — cache temizleme araligi
_watchlist_cache_last_cleanup: float = 0

# ─── FCM Token Dedup (Çift Bildirim Önleme) ───
# Aynı FCM token'a aynı başlıkla kısa süre içinde tekrar push göndermeyi engeller.
# Sorun: Aynı cihazın birden fazla user kaydı olabilir (reinstall, veri silme vb.)
# Key: "fcm_token:title_hash" → Value: son gönderim zamanı (epoch)
_fcm_dedup_cache: dict[str, float] = {}
_FCM_DEDUP_SECONDS = 60  # 60 saniye — aynı token+başlık için minimum bekleme


def _cleanup_watchlist_cache():
    """Eski cache girdilerini temizle (memory leak onleme)."""
    global _watchlist_cache_last_cleanup
    now = time.time()
    if now - _watchlist_cache_last_cleanup < _WATCHLIST_CACHE_CLEANUP_INTERVAL:
        return
    _watchlist_cache_last_cleanup = now
    cutoff = now - _WATCHLIST_NOTIF_COOLDOWN * 2
    expired_keys = [k for k, v in _watchlist_notif_cache.items() if v < cutoff]
    for k in expired_keys:
        del _watchlist_notif_cache[k]
    if expired_keys:
        logger.debug("Watchlist notif cache temizlendi: %d eski girdi silindi", len(expired_keys))

# Firebase Admin SDK — lazy init
_firebase_initialized = False


def _init_firebase():
    """Firebase Admin SDK'yi baslatir (tek seferlik).

    GOOGLE_APPLICATION_CREDENTIALS degerini su sekilde yorumlar:
    - JSON string ise → parse edip dict olarak kullanir (Render icin)
    - Dosya yolu ise → dosyadan okur (lokal gelistirme icin)
    """
    global _firebase_initialized
    if _firebase_initialized:
        return

    try:
        import firebase_admin
        from firebase_admin import credentials

        from app.config import get_settings
        settings = get_settings()

        cred_value = settings.GOOGLE_APPLICATION_CREDENTIALS

        # JSON string mi yoksa dosya yolu mu?
        if cred_value.strip().startswith("{"):
            # Render'da env var olarak JSON string gelir
            cred_dict = json.loads(cred_value)
            cred = credentials.Certificate(cred_dict)
            logger.info("Firebase credentials JSON string'den yuklendi")
        else:
            # Lokal gelistirmede dosya yolu kullanilir
            cred = credentials.Certificate(cred_value)
            logger.info("Firebase credentials dosyadan yuklendi")

        firebase_admin.initialize_app(cred)
        _firebase_initialized = True
        logger.info("Firebase Admin SDK baslatildi")
    except Exception as e:
        logger.error(f"Firebase baslatma hatasi: {e}")
        # Firebase init hatasi kritik — admin'e bildir
        try:
            import asyncio
            from app.services.admin_telegram import notify_scraper_error
            # sync context'te async cagirmak icin
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(
                    notify_scraper_error("Firebase Init", str(e))
                )
        except Exception:
            pass


def is_firebase_initialized() -> bool:
    """Firebase Admin SDK'nin baslatilip baslatilmadigini dondurur."""
    return _firebase_initialized


class NotificationService:
    """FCM push bildirim gonderici."""

    def __init__(self, db: AsyncSession):
        self.db = db
        _init_firebase()

    async def send_to_device(
        self,
        fcm_token: str,
        title: str,
        body: str,
        data: Optional[dict] = None,
        delay: bool = True,
        channel_id: str = "default_v2",
    ) -> bool:
        """Tek bir cihaza push bildirim gonderir.

        delay=True ise bildirim gonderildikten sonra NOTIFICATION_DELAY_SECONDS
        kadar bekler — ust uste seri bildirim onleme.
        """
        if not _firebase_initialized:
            logger.info(f"[DRY-RUN] Push → device: {title} | {body}")
            if delay:
                await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)
            return True

        try:
            from firebase_admin import messaging

            # Data payload — tum value'lar STRING olmali (Firebase zorunlulugu)
            safe_data = {}
            for k, v in (data or {}).items():
                safe_data[k] = str(v) if v is not None else ""

            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body,
                ),
                data=safe_data,
                token=fcm_token,
                android=messaging.AndroidConfig(
                    priority="high",
                    notification=messaging.AndroidNotification(
                        sound="default",
                        channel_id=channel_id,
                        default_vibrate_timings=True,
                        visibility="public",
                        icon="notification_icon",
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
            logger.info(f"Push bildirim gonderildi: {response}")

            # Seri bildirim onleme — sonraki bildirimden once bekle
            if delay:
                await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)

            return True

        except Exception as e:
            error_name = type(e).__name__
            logger.error(f"Push bildirim hatasi ({error_name}): {e}")
            # Hata detayini attribute olarak sakla — test endpoint'ten okunabilir
            self._last_send_error = str(e)

            # Gecersiz/stale token → DB'den temizle
            # UnregisteredError: Cihaz artik kayitli degil
            # InvalidArgumentError: Token formati gecersiz
            # SenderIdMismatchError: Token farkli bir Firebase projesine ait
            # NotFoundError: Token bulunamadi (FCM'den kaldirilmis)
            if error_name in ("UnregisteredError", "InvalidArgumentError", "SenderIdMismatchError", "NotFoundError"):
                await self._clear_stale_token(fcm_token)

            return False

    async def send_to_expo_device(
        self,
        expo_token: str,
        title: str,
        body: str,
        data: Optional[dict] = None,
        delay: bool = True,
    ) -> bool:
        """Expo Push Token'li cihaza bildirim gonderir (Expo Push API v2).

        FCM token yerine ExponentPushToken[...] olan kullanicilar icin.
        """
        try:
            import httpx

            safe_data = {}
            for k, v in (data or {}).items():
                safe_data[k] = str(v) if v is not None else ""

            payload = {
                "to": expo_token,
                "title": title,
                "body": body,
                "data": safe_data,
                "sound": "default",
                "priority": "high",
                "channelId": "default_v2",
            }

            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    "https://exp.host/--/api/v2/push/send",
                    json=payload,
                    headers={
                        "Accept": "application/json",
                        "Accept-Encoding": "gzip, deflate",
                        "Content-Type": "application/json",
                    },
                )
                result = resp.json()

            # Expo API sonucu: {"data": {"status": "ok"}} veya {"data": {"status": "error"}}
            status = result.get("data", {}).get("status", "error")
            if status == "ok":
                logger.info(f"Expo push gonderildi: {expo_token[:30]}...")
                if delay:
                    await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)
                return True
            else:
                err = result.get("data", {}).get("message", "unknown")
                logger.warning(f"Expo push hatasi ({expo_token[:20]}...): {err}")
                # DeviceNotRegistered → token gecersiz, DB'den temizle
                if "DeviceNotRegistered" in str(err) or "InvalidCredentials" in str(err):
                    await self._clear_expo_token(expo_token)
                return False

        except Exception as e:
            logger.error(f"Expo push exception: {e}")
            return False

    async def _clear_expo_token(self, expo_token: str):
        """Gecersiz Expo push token'i DB'den temizler."""
        try:
            from app.models.user import User
            result = await self.db.execute(
                select(User).where(User.expo_push_token == expo_token)
            )
            user = result.scalar_one_or_none()
            if user:
                logger.warning(f"Stale Expo token temizleniyor — user_id={user.id}")
                user.expo_push_token = None
                await self.db.commit()
        except Exception as e:
            logger.error(f"Expo token temizleme hatasi: {e}")

    async def _send_to_user(
        self,
        user,
        title: str,
        body: str,
        data: Optional[dict] = None,
        channel_id: str = "default_v2",
        delay: bool = True,
        category: str = "system",
    ) -> bool:
        """FCM veya Expo token ile kullaniciya bildirim gonderir.

        FCM token varsa Firebase, basarisiz olursa Expo fallback.
        ExponentPushToken varsa Expo Push API kullanilir.
        Basarili gonderimde NotificationLog tablosuna kayit eklenir.
        """
        # KILL SWITCH — admin panelden tüm bildirimler durduruldu
        try:
            from app.services.twitter_service import is_notifications_killed
            if is_notifications_killed():
                logger.warning("[NOTIF KILL SWITCH] Bildirim durduruldu: %s → user_id=%s", title[:40], getattr(user, 'id', '?'))
                return False
        except Exception:
            pass  # import hatasi olursa bildirimi gonder, kill switch'i atlama

        fcm = (user.fcm_token or "").strip()
        expo = (user.expo_push_token or "").strip()

        # ─── FCM Token Dedup — aynı token+başlık çift gönderim önleme ───
        # Aynı cihazın birden fazla user kaydı olabilir (reinstall/veri silme)
        # Bu durumda aynı FCM token'a aynı bildirimi tekrar gönderme
        token_for_dedup = fcm or expo
        if token_for_dedup:
            dedup_key = f"{token_for_dedup}:{hash(title)}"
            now = time.time()
            last_sent = _fcm_dedup_cache.get(dedup_key, 0)
            if now - last_sent < _FCM_DEDUP_SECONDS:
                logger.info(
                    "[FCM DEDUP] Çift bildirim engellendi: %s → user_id=%s (%.0fs önce gönderildi)",
                    title[:40], getattr(user, 'id', '?'), now - last_sent,
                )
                return False
            _fcm_dedup_cache[dedup_key] = now
            # Eski girdileri temizle (memory leak önleme)
            if len(_fcm_dedup_cache) > 5000:
                cutoff = now - _FCM_DEDUP_SECONDS * 2
                expired = [k for k, v in _fcm_dedup_cache.items() if v < cutoff]
                for k in expired:
                    del _fcm_dedup_cache[k]

        success = False
        if fcm:
            result = await self.send_to_device(
                fcm_token=fcm,
                title=title,
                body=body,
                data=data,
                channel_id=channel_id,
                delay=delay,
            )
            if result:
                success = True
            else:
                # FCM basarisiz — Expo fallback dene
                logger.info(f"FCM basarisiz, Expo fallback deneniyor: user_id={user.id}")
                if expo and expo.startswith("ExponentPushToken"):
                    success = await self.send_to_expo_device(
                        expo_token=expo,
                        title=title,
                        body=body,
                        data=data,
                        delay=delay,
                    )
        elif expo and expo.startswith("ExponentPushToken"):
            success = await self.send_to_expo_device(
                expo_token=expo,
                title=title,
                body=body,
                data=data,
                delay=delay,
            )
        else:
            logger.warning(f"Kullanicinin gecerli tokeni yok: user_id={user.id}")
            return False

        # Basarili gonderimde Bildirim Merkezi'ne kaydet
        if success:
            try:
                await self._log_notification(
                    device_id=user.device_id,
                    title=title,
                    body=body,
                    category=category,
                    data=data,
                )
            except Exception as e:
                logger.warning("NotificationLog kayit hatasi: %s", e)

        return success

    async def _log_notification(
        self,
        device_id: str,
        title: str,
        body: str,
        category: str = "system",
        data: Optional[dict] = None,
    ):
        """Gonderilen bildirimi notification_logs tablosuna kaydeder."""
        try:
            from app.models.notification_log import NotificationLog
            log = NotificationLog(
                device_id=device_id,
                title=title,
                body=body,
                category=category,
                data_json=json.dumps(data, ensure_ascii=False) if data else None,
            )
            self.db.add(log)
            await self.db.commit()
        except Exception as e:
            logger.debug("NotificationLog insert hatasi: %s", e)

    async def _clear_stale_token(self, fcm_token: str):
        """UnregisteredError alan FCM token'i DB'den temizler.

        Token gecersiz/stale olunca Firebase hata veriyor.
        Kullanici uygulamayi tekrar actiginda yeni token alinir.
        """
        try:
            from app.models.user import User

            result = await self.db.execute(
                select(User).where(User.fcm_token == fcm_token)
            )
            user = result.scalar_one_or_none()
            if user:
                logger.warning(
                    f"Stale FCM token temizleniyor — user_id={user.id}, "
                    f"device_id={user.device_id[:8]}..."
                )
                user.fcm_token = None
                await self.db.commit()

                # Telegram admin bildirimi
                try:
                    from app.services.admin_telegram import notify_stale_token_cleaned
                    await notify_stale_token_cleaned(user.id, user.device_id)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Stale token temizleme hatasi: {e}")

    async def send_to_topic(
        self,
        topic: str,
        title: str,
        body: str,
        data: Optional[dict] = None,
        delay: bool = True,
    ) -> bool:
        """Bir konuya (topic) abone olan tum cihazlara bildirim gonderir.

        delay=True ise bildirim gonderildikten sonra NOTIFICATION_DELAY_SECONDS
        kadar bekler — ust uste seri bildirim onleme.
        """
        # KILL SWITCH — admin panelden tüm bildirimler durduruldu
        try:
            from app.services.twitter_service import is_notifications_killed
            if is_notifications_killed():
                logger.warning("[NOTIF KILL SWITCH] Topic bildirim durduruldu: %s → topic=%s", title[:40], topic)
                return False
        except Exception:
            pass

        if not _firebase_initialized:
            logger.info(f"[DRY-RUN] Push → topic/{topic}: {title} | {body}")
            if delay:
                await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)
            return True

        try:
            from firebase_admin import messaging

            # Data payload — tum value'lar STRING olmali (Firebase zorunlulugu)
            safe_data = {}
            for k, v in (data or {}).items():
                safe_data[k] = str(v) if v is not None else ""

            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body,
                ),
                data=safe_data,
                topic=topic,
                android=messaging.AndroidConfig(
                    priority="high",
                    notification=messaging.AndroidNotification(
                        sound="default",
                        channel_id="default_v2",
                        icon="notification_icon",
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
            logger.info(f"Topic bildirim gonderildi ({topic}): {response}")

            # Seri bildirim onleme — sonraki bildirimden once bekle
            if delay:
                await asyncio.sleep(NOTIFICATION_DELAY_SECONDS)

            return True

        except Exception as e:
            logger.error(f"Topic bildirim hatasi ({topic}): {e}")
            return False

    # -------------------------------------------------------
    # Yardimci: Kullanici tercihine gore filtreleyip gonder
    # -------------------------------------------------------

    async def _send_filtered(
        self,
        preference_field: str,
        title: str,
        body: str,
        data: dict,
        log_label: str,
        channel_id: str = "default_v2",
        category: str = "system",
    ) -> int:
        """Belirli bildirim tercihini kontrol ederek sadece aktif kullanicilara gonderir.

        - notifications_enabled = True (master switch)
        - preference_field = True (ilgili bildirim tercihi)
        - Push token mevcut
        """
        from app.models.user import User

        pref_col = getattr(User, preference_field, None)
        if pref_col is None:
            logger.error("Gecersiz preference_field: %s", preference_field)
            return 0

        users_result = await self.db.execute(
            select(User).where(
                and_(
                    User.notifications_enabled == True,
                    User.deleted == False,
                    pref_col == True,
                    or_(
                        and_(User.fcm_token.isnot(None), User.fcm_token != ""),
                        and_(User.expo_push_token.isnot(None), User.expo_push_token != ""),
                    ),
                )
            )
        )
        users = list(users_result.scalars().all())

        sent_count = 0
        failed_count = 0
        for user in users:
            try:
                success = await self._send_to_user(
                    user=user,
                    title=title,
                    body=body,
                    data=data,
                    channel_id=channel_id,
                    category=category,
                )
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning("_send_filtered bildirim hatasi (user=%s): %s", user.id, e)

        logger.info(
            "%s — %d kullaniciya gonderildi, %d basarisiz (filtre: %s)",
            log_label, sent_count, failed_count, preference_field,
        )

        # Telegram admin raporu
        try:
            from app.services.admin_telegram import notify_push_sent
            await notify_push_sent(
                notification_type=log_label,
                title=title,
                sent_count=sent_count,
                failed_count=failed_count,
            )
        except Exception:
            pass  # Telegram hatasi bildirim akisini bozmasin

        return sent_count

    # -------------------------------------------------------
    # Halka Arz Bildirimleri
    # -------------------------------------------------------

    async def notify_new_ipo(self, ipo) -> int:
        """Yeni halka arz bildirimi — notify_new_ipo = True olanlara."""
        title = "🆕 Yeni Halka Arz"
        body = f"{ipo.company_name}"
        if ipo.ticker:
            body += f" ({ipo.ticker})"
        if ipo.ipo_price:
            body += f" — {ipo.ipo_price} TL"

        data = {
            "type": "new_ipo",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
            "screen": "halka-arz-detay",
        }

        return await self._send_filtered(
            "notify_new_ipo", title, body, data,
            f"Yeni halka arz: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    async def notify_ipo_subscription_start(self, ipo) -> int:
        """Basvuru baslangici bildirimi — notify_ipo_start = True olanlara."""
        title = "📋 Başvuru Başladı"
        body = f"{ipo.ticker or ipo.company_name} halka arz başvurusu başladı!"
        if ipo.subscription_end:
            body += f" Son gün: {ipo.subscription_end.strftime('%d.%m.%Y')}"

        data = {
            "type": "ipo_start",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
            "screen": "halka-arz-detay",
        }

        return await self._send_filtered(
            "notify_ipo_start", title, body, data,
            f"Basvuru basladi: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    async def notify_ai_report_ready(self, ipo, overall_score: float) -> int:
        """AI analiz raporu hazir bildirimi — notify_ipo_start = True olanlara."""
        ticker = ipo.ticker or ipo.company_name
        title = f"🤖 {ticker} AI Analiz Raporu Hazır"
        body = (
            f"{ticker} için AI analiz raporu hazırlandı. "
            f"Skor: {overall_score:.1f}/10 — "
            f"Halka arz şirket kartından bakabilirsiniz."
        )

        data = {
            "type": "ai_report_ready",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
            "screen": "halka-arz-detay",
            "score": str(overall_score),
        }

        return await self._send_filtered(
            "notify_ipo_start", title, body, data,
            f"AI rapor hazir: {ticker} (skor={overall_score:.1f})",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    async def notify_ipo_last_day(self, ipo) -> int:
        """Son gun uyarisi — notify_ipo_last_day = True olanlara."""
        title = "⏰ Son Gün Uyarısı"
        body = f"{ipo.ticker or ipo.company_name} halka arz başvurusu BUGÜN son gün!"

        data = {
            "type": "ipo_last_day",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
            "screen": "halka-arz-detay",
        }

        return await self._send_filtered(
            "notify_ipo_last_day", title, body, data,
            f"Son gun uyarisi: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    @staticmethod
    def _format_lot_range(lot_val: float) -> str:
        """Lot'u aralık formatına çevir: 8.5 → '8-9', 3.0 → '3', 12.7 → '12-13'"""
        if lot_val == int(lot_val):
            return str(int(lot_val))
        low = int(lot_val)  # floor
        high = low + 1
        return f"{low}-{high}"

    async def notify_allocation_result(self, ipo, total_applicants: int = 0) -> int:
        """Dagitim sonucu bildirimi — notify_ipo_result = True olanlara.

        Bildirim icerigi:
        - Baslik: Dagitim Sonuclari + kisi basi
        - Govde: Ticker, toplam basvuran, bireysel kisi, dagitilan lot, kisi basi TL
        """
        ticker = ipo.ticker or ipo.company_name

        # Kisi basi hesapla (basliga eklemek icin)
        bireysel_kisi = getattr(ipo, "result_bireysel_kisi", None)
        bireysel_lot = getattr(ipo, "result_bireysel_lot", None)
        arz_fiyati = getattr(ipo, "price", None)
        lot_buyuklugu = getattr(ipo, "lot_size", None) or 100  # default 100 adet

        kisi_basi_lot = None
        kisi_basi_tl = None
        if bireysel_kisi and bireysel_lot and bireysel_kisi > 0:
            kisi_basi_lot = bireysel_lot / bireysel_kisi
            if arz_fiyati:
                kisi_basi_tl = kisi_basi_lot * int(lot_buyuklugu) * float(arz_fiyati)

        # Baslik — kisi basi lot bilgisi varsa ekle (aralık formatı: 8-9 lot)
        if kisi_basi_lot is not None:
            lot_str = self._format_lot_range(kisi_basi_lot)
            title = f"📊 {ticker} Dağıtım: Kişi Başı ~{lot_str} Lot"
        else:
            title = f"📊 {ticker} Dağıtım Sonuçları"

        parts = [f"{ticker} dağıtım sonuçları açıklandı!"]

        # Toplam basvuran
        t_applicants = total_applicants or getattr(ipo, "total_applicants", None)
        if t_applicants:
            parts.append(f"Toplam başvuru: {int(t_applicants):,} kişi")

        # Bireysel kisi ve lot
        if bireysel_kisi:
            parts.append(f"Yurt içi bireysel: {int(bireysel_kisi):,} kişi")
        if bireysel_lot:
            parts.append(f"Dağıtılan lot: {int(bireysel_lot):,}")

        # Kisi basi lot ve TL (aralık formatı: 8-9 lot)
        if kisi_basi_lot is not None:
            lot_range = self._format_lot_range(kisi_basi_lot)
            kisi_basi_str = f"Kişi başı: ~{lot_range} lot"
            if kisi_basi_tl is not None:
                kisi_basi_str += f" (~{kisi_basi_tl:,.0f} TL)"
            parts.append(kisi_basi_str)

        body = "\n".join(parts)

        data = {
            "type": "ipo_result",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
            "screen": "halka-arz-detay",
        }

        return await self._send_filtered(
            "notify_ipo_result", title, body, data,
            f"Dagitim sonucu: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    async def notify_first_trading_day(self, ipo) -> int:
        """Ilk islem gunu bildirimi — notify_first_trading_day = True olanlara."""
        title = "🔔 Bugün İşlem Görmeye Başlıyor"
        body = f"{ipo.ticker or ipo.company_name} bugün borsada işlem görmeye başlıyor!"
        if ipo.ipo_price:
            body += f" (Halka arz fiyatı: {ipo.ipo_price} TL)"

        data = {
            "type": "first_trading_day",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
            "screen": "halka-arz-detay",
        }

        return await self._send_filtered(
            "notify_first_trading_day", title, body, data,
            f"Ilk islem gunu: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    async def notify_trading_date_detected(self, ipo) -> int:
        """Ilk islem tarihi tespit bildirimi — notify_first_trading_day = True olanlara."""
        pazar_map = {
            "yildiz_pazar": "Yıldız Pazar",
            "ana_pazar": "Ana Pazar",
            "alt_pazar": "Alt Pazar",
        }
        pazar = pazar_map.get(ipo.market_segment or "", "")
        pazar_text = f" ({pazar})" if pazar else ""

        title = "📊 İşlem Tarihi Belli Oldu"
        body = f"{ipo.ticker or ipo.company_name}{pazar_text} borsada işlem görmeye başlıyor!"
        if ipo.trading_start:
            body += f" İlk işlem: {ipo.trading_start.strftime('%d.%m.%Y')}"

        data = {
            "type": "trading_date_detected",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
            "screen": "halka-arz-detay",
        }

        return await self._send_filtered(
            "notify_first_trading_day",   # Aynı filtre: notify_first_trading_day = True
            title, body, data,
            f"Islem tarihi tespit: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    async def notify_ticker_assigned(self, ipo) -> int:
        """Halka arz kodu + talep tarihi belli oldu bildirimi — notify_first_trading_day = True olanlara."""
        ticker = ipo.ticker or ""

        _AYLAR = ["Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran",
                  "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"]

        has_dates = bool(ipo.subscription_start)
        if has_dates:
            title = "💹 Halka Arz Kodu ve Talep Tarihi Belli Oldu"
        else:
            title = "💹 Halka Arz Kodu Belli Oldu"

        body = f"{ipo.company_name} borsa kodu: {ticker}"
        if ipo.subscription_start and ipo.subscription_end:
            s = ipo.subscription_start
            e = ipo.subscription_end
            if s.month == e.month:
                body += f" | {s.day}-{e.day} {_AYLAR[s.month - 1]} talep toplanacak"
            else:
                body += f" | {s.day} {_AYLAR[s.month - 1]} - {e.day} {_AYLAR[e.month - 1]} talep toplanacak"
        if ipo.trading_start:
            body += f" | İlk işlem: {ipo.trading_start.strftime('%d.%m.%Y')}"

        data = {
            "type": "ticker_assigned",
            "ipo_id": str(ipo.id),
            "ticker": ticker,
            "screen": "halka-arz-detay",
        }

        return await self._send_filtered(
            "notify_first_trading_day",   # Ayni filtre: halka arz haberleri isteyenler
            title, body, data,
            f"Ticker tespit: {ipo.company_name} → {ticker}",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    # NOTE: notify_ceiling_broken() kaldirildi — kullanilmiyordu.
    # Tavan/taban/dusus bildirimleri artik /api/v1/realtime-notification
    # endpoint'i uzerinden StockNotificationSubscription bazli gonderiliyor.
    # Kullanici toggle kontrolu: main.py _type_toggle_map ile yapiliyor.

    # -------------------------------------------------------
    # SPK Basvuru Bildirimleri
    # -------------------------------------------------------

    async def notify_spk_applications(self, company_names: list[str]) -> int:
        """SPK'ya halka arz onay basvurusu yapan yeni sirketleri bildirir.

        Toplu bildirim — ayni scrape dongusunde tespit edilen tum sirketler
        tek bir push bildirimde gonderilir.

        Filtre: notify_new_ipo (halka arz haberleri isteyen kullanicilar).
        ⚠️ Title'da "BAŞVURU" kelimesi on planda — yeni halka arz algisi yaratmamali.
        """
        if not company_names:
            return 0

        title = "📝 SPK Halka Arz Başvurusu"

        # Body olustur — sirket sayisina gore
        count = len(company_names)
        if count == 1:
            body = f"{company_names[0]}, SPK'ya halka arz onay başvurusunda bulundu"
        elif count == 2:
            body = f"{company_names[0]} ve {company_names[1]}, SPK'ya halka arz onay başvurusunda bulundu"
        elif count <= 4:
            joined = ", ".join(company_names[:-1]) + f" ve {company_names[-1]}"
            body = f"{joined}, SPK'ya halka arz onay başvurusunda bulundu"
        else:
            body = f"{count} yeni şirket SPK'ya halka arz onay başvurusunda bulundu"

        data = {
            "type": "spk_application",
            "count": str(count),
            "screen": "halka-arz",
        }

        return await self._send_filtered(
            "notify_new_ipo", title, body, data,
            f"SPK basvuru bildirimi: {count} sirket",
            channel_id="ipo_alerts_v2",
            category="ipo",
        )

    # -------------------------------------------------------
    # KAP Haber Bildirimleri
    # -------------------------------------------------------

    async def notify_kap_news(
        self,
        ticker: str,
        price: Optional[float],
        kap_id: str,
        matched_keyword: str,
        sentiment: str,
        news_type: str,
        pct_change: Optional[str] = None,
        gap_pct: Optional[str] = None,
        ai_score: Optional[float] = None,
        ai_summary: Optional[str] = None,
        prev_close: Optional[str] = None,
        theoretical_open: Optional[str] = None,
    ) -> int:
        """KAP haber bildirimini gonder (sadece pozitif).

        Ucretli aboneler (ana_yildiz): Per-user bildirim — notify_kap_all kontrollu
        Ucretsiz BIST 50: Per-user push (_send_bist50_free) — ucretli aboneler haric (dedup)

        3 Tip Bildirim:
        - Seans Ici Pozitif Haber Yakalandi
        - Seans Disi Pozitif Haber Yakalandi
        - Seans Disi Haber Yakalanan Hisse Acilisi (GAP bilgisi ile)
        """
        # AI puani varsa basliga ekle
        score_tag = ""
        if ai_score is not None:
            score_tag = f" (AI: {ai_score:.1f}/10)"

        if news_type == "seans_ici":
            title = f"⚡ Seans İçi Pozitif Haber Yakalandı - {ticker}{score_tag}"
        elif news_type == "seans_disi_acilis":
            # Gap yuzdesini title'a ekle — kullanici bildirimde hemen gorsun
            gap_str = f" ({gap_pct})" if gap_pct else ""
            title = f"📊 {ticker} Açılış{gap_str}{score_tag}"
        else:
            title = f"🌙 Seans Dışı Pozitif Haber Yakalandı - {ticker}{score_tag}"

        # Virgulden onceki ilk kelimeyi al (cok kelimeli keyword'leri kirp)
        clean_kw = matched_keyword.split(",")[0].strip() if matched_keyword else matched_keyword

        if news_type == "seans_disi_acilis":
            # Acilis bildirimi: fiyat ve gap bilgisi on planda
            lines = []
            if theoretical_open:
                lines.append(f"Teorik Açılış: {theoretical_open} TL")
            if prev_close:
                lines.append(f"Önceki Kapanış: {prev_close} TL")
            if gap_pct:
                lines.append(f"Açılış Gap: {gap_pct}")
            lines.append(f"Yakalanan Kelime: {clean_kw}")
            body = "\n".join(lines)
        else:
            # Fiyat bilgisi gonderilmez (veri ihlali)
            body = f"Sembol: {ticker}\nYakalanan Kelime: {clean_kw}"
            # Seans ici yuzdesel degisim varsa ekle
            if news_type == "seans_ici" and pct_change:
                body += f"\nDeğişim: {pct_change}"
        # AI ozeti varsa ekle — bildirim merkezinde genisletince gorulecek
        if ai_summary and ai_summary.strip():
            body += f"\n\n📝 AI Analiz:\n{ai_summary.strip()}"

        data = {
            "type": "kap_news",
            "ticker": ticker,
            "kap_id": kap_id,
            "sentiment": sentiment,
            "matched_keyword": matched_keyword,
            "screen": "ai-kap",
        }
        if ai_score is not None:
            data["ai_score"] = str(ai_score)

        from app.services.news_service import get_bist50_tickers_sync
        BIST50_TICKERS = get_bist50_tickers_sync()

        sent = 0
        ticker_upper = ticker.upper()

        # 1. Ucretli abonelere PER-USER bildirim (notify_kap_all == True olanlara)
        sent += await self._send_paid_kap_news(title, body, data, ticker_upper)

        # 2. BIST 50 ucretsiz per-user bildirim (ucretli aboneler HARIC — dedup)
        if ticker_upper in BIST50_TICKERS:
            sent += await self._send_bist50_free(title, body, data, ticker_upper)

            # Tweet: poller'da zaten tweet_bist30_news cagriliyor, burada TEKRAR atma (dedup)

        return sent

    async def _send_paid_kap_news(
        self,
        title: str,
        body: str,
        data: dict,
        ticker: str,
    ) -> int:
        """Ucretli abonelere KAP haber bildirimi gonder.

        SADECE ana_yildiz KAP haber aboneleri (UserSubscription) — notify_kap_all kontrollu.
        Halka Arz bundle (StockNotificationSubscription) sahipleri KAP haberi ALMAZ,
        onlar sadece halka arz bildirimlerini (tavan, taban, acilis/kapanis, dusus) alir.
        BIST 50 kapsamindaki haberler ise _send_bist50_free ile ucretsiz gonderilir.
        """
        from app.models.user import User, UserSubscription

        # ana_yildiz KAP haber aboneleri — notify_kap_all == True zorunlu
        kap_sub_result = await self.db.execute(
            select(User)
            .join(UserSubscription, UserSubscription.user_id == User.id)
            .where(
                and_(
                    UserSubscription.is_active == True,
                    UserSubscription.package == "ana_yildiz",
                    User.notifications_enabled == True,
                    User.deleted == False,
                    User.notify_kap_all == True,
                    or_(
                        and_(User.fcm_token.isnot(None), User.fcm_token != ""),
                        and_(User.expo_push_token.isnot(None), User.expo_push_token != ""),
                    ),
                )
            )
        )
        kap_users = list(kap_sub_result.scalars().all())

        sent_count = 0
        failed_count = 0
        for user in kap_users:
            try:
                success = await self._send_to_user(
                    user=user,
                    title=title,
                    body=body,
                    data=data,
                    channel_id="kap_news_v2",
                    category="kap_news",
                )
                if success:
                    sent_count += 1
                    # Watchlist dedup: VIP kullaniciya bu ticker icin
                    # ayrica watchlist bildirimi gitmesini engelle
                    cache_key = f"{user.device_id}:{ticker}"
                    _watchlist_notif_cache[cache_key] = time.time()
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning("Ucretli KAP bildirim hatasi (user=%s): %s", user.id, e)

        logger.info(
            "Ucretli KAP bildirim: %s — %d kullaniciya gonderildi (kap_abone=%d)",
            ticker, sent_count, len(kap_users),
        )

        # Telegram admin raporu
        try:
            from app.services.admin_telegram import notify_push_sent
            await notify_push_sent(
                notification_type=f"KAP Ucretli: {ticker}",
                title=title,
                sent_count=sent_count,
                failed_count=failed_count,
                detail=f"KAP abone: {len(kap_users)}",
            )
        except Exception:
            pass

        return sent_count

    async def _send_bist50_free(
        self,
        title: str,
        body: str,
        data: dict,
        ticker: str,
    ) -> int:
        """BIST 50 ucretsiz bildirim — ana_yildiz aboneligi OLMAYAN kullanicilara.

        Dedup: _send_paid_kap_news ile zaten bildirim alan kullanicilar haric tutulur:
        - UserSubscription aktif ana_yildiz olanlar (zaten TUM haberleri aliyor)

        NOT: Halka Arz bundle sahipleri (StockNotificationSubscription) HARIC TUTULMAZ.
        Onlar KAP haber abonesi degil, sadece halka arz bildirimi aliyor.
        BIST 50 haberleri ucretsiz — herkes gibi onlar da alabilir.
        """
        from app.models.user import User, UserSubscription

        # Ucretli KAP abonelerin user_id'leri (haric tutulacak — zaten tum haberleri aliyor)
        paid_kap_ids = (
            select(UserSubscription.user_id)
            .where(
                and_(
                    UserSubscription.is_active == True,
                    UserSubscription.package == "ana_yildiz",
                )
            )
        )

        users_result = await self.db.execute(
            select(User).where(
                and_(
                    User.notifications_enabled == True,
                    User.deleted == False,
                    User.notify_kap_bist30 == True,
                    or_(
                        and_(User.fcm_token.isnot(None), User.fcm_token != ""),
                        and_(User.expo_push_token.isnot(None), User.expo_push_token != ""),
                    ),
                    User.id.notin_(paid_kap_ids),
                )
            )
        )
        users = list(users_result.scalars().all())

        sent_count = 0
        failed_count = 0
        for user in users:
            try:
                success = await self._send_to_user(
                    user=user,
                    title=title,
                    body=body,
                    data=data,
                    channel_id="kap_news_v2",
                    category="kap_news",
                )
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning("BIST50 free bildirim hatasi (user=%s): %s", user.id, e)

        logger.info(
            "BIST50 free bildirim: %s — %d ucretsiz kullaniciya gonderildi",
            ticker, sent_count,
        )

        # Telegram admin raporu
        try:
            from app.services.admin_telegram import notify_push_sent
            await notify_push_sent(
                notification_type=f"KAP BIST50 Free: {ticker}",
                title=title,
                sent_count=sent_count,
                failed_count=failed_count,
            )
        except Exception:
            pass

        return sent_count

    async def notify_kap_watchlist(
        self,
        disclosure,
    ) -> int:
        """Takip listesindeki kullanicilara KAP bildirimi gonderir.

        Kullanicinin takip etigi hisse icin yeni KAP bildirimi geldiginde
        push bildirim gonderilir.

        Kosullar:
        - user_watchlist'te ticker eslesen kullanicilar
        - notify_kap_watchlist=True
        - notifications_enabled=True
        - deleted=False
        """
        from app.models.user import User
        from app.models.user_watchlist import UserWatchlist

        ticker = disclosure.company_code
        sentiment = disclosure.ai_sentiment or ""
        score = disclosure.ai_impact_score

        # Sentiment emoji + etiket
        if sentiment == "Olumlu":
            emoji = "📈"
            sentiment_tag = "Pozitif"
        elif sentiment == "Olumsuz":
            emoji = "📉"
            sentiment_tag = "Negatif"
        else:
            emoji = "📋"
            sentiment_tag = "Nötr"

        title = f"{emoji} {ticker} — {sentiment_tag} Haber"

        # Bildirim govdesi — AI ozeti varsa ekle
        body = (disclosure.title or "")[:200]
        if disclosure.ai_summary and disclosure.ai_summary.strip():
            body += f"\n\n📝 AI Analiz:\n{disclosure.ai_summary.strip()}"

        data = {
            "type": "kap_watchlist",
            "ticker": ticker,
            "disclosure_id": str(disclosure.id),
            "sentiment": sentiment_tag,
            "screen": "ai-kap",
        }

        # Takip eden kullanicilari bul — tercihleriyle birlikte
        watchlist_result = await self.db.execute(
            select(UserWatchlist.device_id, UserWatchlist.notification_preference).where(
                UserWatchlist.ticker == ticker
            )
        )
        watchlist_rows = watchlist_result.all()

        if not watchlist_rows:
            return 0

        # Sentiment'e gore filtrele + spam korumasi
        _cleanup_watchlist_cache()
        now = time.time()
        filtered_device_ids = []
        skipped_spam = 0

        for device_id, pref in watchlist_rows:
            # Spam korumasi — ayni ticker icin son 5 dk icinde bildirim gittiyse atla
            cache_key = f"{device_id}:{ticker}"
            last_sent = _watchlist_notif_cache.get(cache_key, 0)
            if now - last_sent < _WATCHLIST_NOTIF_COOLDOWN:
                skipped_spam += 1
                continue

            # Bildirim tercihi filtresi
            if pref == "all" or pref == "both" or not pref:
                # Tum haberler (Olumlu + Notr + Olumsuz)
                filtered_device_ids.append(device_id)
            elif pref == "positive_negative":
                # Hem pozitif hem negatif — notr haric
                if sentiment in ("Olumlu", "Olumsuz"):
                    filtered_device_ids.append(device_id)
            elif pref == "positive_only" and sentiment == "Olumlu":
                filtered_device_ids.append(device_id)
            elif pref == "negative_only" and sentiment == "Olumsuz":
                filtered_device_ids.append(device_id)
            # "Notr" → sadece "all"/"both" tercih edenler alir (yukarida yakalandi)

        if skipped_spam > 0:
            logger.info("KAP Watchlist spam korumasi: %s — %d kullanici 5dk cooldown icinde, atlandilar", ticker, skipped_spam)

        if not filtered_device_ids:
            return 0

        # Bildirimleri acik olan kullanicilari getir
        users_result = await self.db.execute(
            select(User).where(
                and_(
                    User.device_id.in_(filtered_device_ids),
                    User.notifications_enabled == True,
                    User.notify_kap_watchlist == True,
                    or_(User.deleted == False, User.deleted.is_(None)),
                )
            )
        )
        users = list(users_result.scalars().all())

        sent_count = 0
        for user in users:
            try:
                success = await self._send_to_user(
                    user=user,
                    title=title,
                    body=body,
                    data=data,
                    channel_id="kap_news_v2",
                    category="kap_watchlist",
                )
                if success:
                    sent_count += 1
                    # Spam cache guncelle — basarili gonderimde cooldown baslat
                    cache_key = f"{user.device_id}:{ticker}"
                    _watchlist_notif_cache[cache_key] = time.time()
            except Exception as e:
                logger.warning("Watchlist bildirim hatasi (user=%s): %s", user.id, e)

        if sent_count > 0:
            logger.info(
                "KAP Watchlist bildirim: %s — %d kullaniciya gonderildi",
                ticker, sent_count,
            )

        return sent_count

    # -------------------------------------------------------
    # Gunluk Takip (Daily Tracking) Bildirimleri
    # -------------------------------------------------------

    async def notify_daily_tracking(
        self,
        ipo,
        current_day: int,
        daily_pct: float,
        durum: str,
    ) -> int:
        """Gunluk takip bildirimi — IPO icin bildirim abonesi olan kullanicilara.

        StockNotificationSubscription uzerinden ipo_id eslesen,
        aktif aboneligi olan kullanicilara push bildirim gonderir.

        Args:
            ipo: IPO modeli (id, ticker, ipo_price vb.)
            current_day: Kacinci islem gunu (1-25)
            daily_pct: Gunluk yuzdesel degisim
            durum: Son durum (tavan, taban, not_kapatti vb.)
        """
        from app.models.user import User, StockNotificationSubscription

        ticker = ipo.ticker or ""
        if not ticker:
            return 0

        # Durum etiketi
        durum_labels = {
            "tavan": "Tavan",
            "taban": "Taban",
            "not_kapatti": "Normal",
        }
        durum_label = durum_labels.get(durum, durum)

        title = f"📊 {ticker} Günlük Takip"
        body = f"Gün: {current_day}/25 | %{daily_pct:+.2f} | {durum_label}"

        data = {
            "type": "daily_tracking",
            "ipo_id": str(ipo.id),
            "ticker": ticker,
            "day": str(current_day),
            "screen": "halka-arz-detay",
        }

        # StockNotificationSubscription uzerinden bu IPO'yu takip eden kullanicilari bul
        sub_result = await self.db.execute(
            select(User)
            .join(
                StockNotificationSubscription,
                StockNotificationSubscription.user_id == User.id,
            )
            .where(
                and_(
                    StockNotificationSubscription.ipo_id == ipo.id,
                    StockNotificationSubscription.is_active == True,
                    or_(
                        StockNotificationSubscription.muted == False,
                        StockNotificationSubscription.muted.is_(None),
                    ),
                    User.notifications_enabled == True,
                    User.deleted == False,
                    or_(
                        and_(User.fcm_token.isnot(None), User.fcm_token != ""),
                        and_(User.expo_push_token.isnot(None), User.expo_push_token != ""),
                    ),
                )
            )
        )
        # Dedup — ayni kullanici birden fazla bildirim tipi almis olabilir
        users = list({u.id: u for u in sub_result.scalars().all()}.values())

        if not users:
            logger.info("Gunluk takip: %s — bildirim abonesi yok", ticker)
            return 0

        sent_count = 0
        failed_count = 0
        for user in users:
            try:
                success = await self._send_to_user(
                    user=user,
                    title=title,
                    body=body,
                    data=data,
                    channel_id="ipo_alerts_v2",
                    category="ipo",
                )
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning("Gunluk takip bildirim hatasi (user=%s): %s", user.id, e)

        logger.info(
            "Gunluk takip bildirim: %s — %d kullaniciya gonderildi, %d basarisiz",
            ticker, sent_count, failed_count,
        )

        # Telegram admin raporu
        try:
            from app.services.admin_telegram import notify_push_sent
            await notify_push_sent(
                notification_type=f"gunluk_takip: {ticker}",
                title=title,
                sent_count=sent_count,
                failed_count=failed_count,
                detail=f"Gün: {current_day}/25 | Abone: {len(users)}",
            )
        except Exception:
            pass

        return sent_count

    # -------------------------------------------------------
    # Genel Piyasa Bildirimleri (Tavan/Taban, Haberler, SPK, VİOP)
    # Spam korumasi: Her bildirim tipi icin cooldown suresi
    # -------------------------------------------------------

    # Tip bazli son gonderim zamani — duplicate/spam onleme
    _general_notif_cooldowns: dict[str, float] = {}

    def _check_cooldown(self, notif_key: str, cooldown_seconds: int) -> bool:
        """Spam korumasi — ayni tip bildirim cooldown suresi icinde tekrar gonderilmez.

        Returns True if OK to send, False if in cooldown.
        """
        now = time.time()
        last_sent = self._general_notif_cooldowns.get(notif_key, 0)
        if now - last_sent < cooldown_seconds:
            logger.info("Spam koruma: %s icin %ds cooldown aktif, atlaniyor", notif_key, cooldown_seconds)
            return False
        self._general_notif_cooldowns[notif_key] = now
        return True

    async def notify_tavan_taban(self, ceiling_count: int, floor_count: int, date_label: str) -> int:
        """Gunluk tavan/taban listesi hazir bildirimi — gunde 1 kez.

        Spam koruma: 6 saat cooldown (ayni gun icinde tekrar gonderilmez)
        """
        if not self._check_cooldown("tavan_taban", 21600):  # 6 saat
            return 0

        title = "Günün Tavan/Taban Hisseleri"
        body = f"{date_label} kapanış: {ceiling_count} tavan, {floor_count} taban hisse tespit edildi."

        data = {
            "type": "tavan_taban",
            "screen": "tavan-taban-gunluk",
        }

        return await self._send_filtered(
            "notify_daily_open_close", title, body, data,
            f"Tavan/Taban: {ceiling_count}T/{floor_count}Tb",
        )

    async def notify_market_news(self, headline: str, summary: str = "") -> int:
        """Onemli piyasa haberi bildirimi.

        Spam koruma: 10 dakika cooldown (arka arkaya haber onaylanirsa)
        """
        if not self._check_cooldown("market_news", 600):  # 10 dk
            return 0

        title = "Önemli Piyasa Gelişmesi"
        # Headline + AI ozet varsa ekle
        if summary:
            body = f"{headline[:100]}\n{summary[:300]}"
        else:
            body = headline[:200]

        data = {
            "type": "market_news",
            "screen": "haberler-genel",
        }

        return await self._send_filtered(
            "notifications_enabled", title, body, data,
            f"Piyasa haberi: {headline[:50]}",
            category="system",
        )

    async def notify_spk_bulletin(self, bulletin_no: str, summary: str = "") -> int:
        """Yeni SPK bulteni tespit edildi bildirimi.

        Spam koruma: 1 saat cooldown
        """
        if not self._check_cooldown("spk_bulletin", 3600):  # 1 saat
            return 0

        title = "Yeni SPK Bülteni Yayınlandı"
        if summary:
            body = f"SPK Haftalık Bülten {bulletin_no}\n{summary[:300]}"
        else:
            body = f"SPK Haftalık Bülten {bulletin_no} — Halka arz kararları ve düzenleyici gelişmeler için inceleyin."

        data = {
            "type": "spk_bulletin",
            "bulletin_no": bulletin_no,
            "screen": "spk-bulten-analiz",
        }

        return await self._send_filtered(
            "notify_new_ipo", title, body, data,
            f"SPK Bülten: {bulletin_no}",
        )

    async def notify_viop_session(self, session_type: str, summary: str = "") -> int:
        """VİOP seans bildirimi — acilis, kapanis veya flash.

        Seans türü saate göre belirlenir:
        - 06:00-18:00 arası → Gündüz Seansı
        - 18:00-06:00 arası → Akşam Seansı

        Spam koruma: acilis/kapanis 4 saat, flash 30 dakika cooldown
        """
        cooldown = 1800 if session_type == "flash" else 14400  # flash: 30dk, diger: 4 saat
        if not self._check_cooldown(f"viop_{session_type}", cooldown):
            return 0

        # Saat bazlı seans türü belirleme (TR saati UTC+3)
        from datetime import datetime, timezone, timedelta
        tr_now = datetime.now(timezone(timedelta(hours=3)))
        hour = tr_now.hour
        is_evening = hour >= 18 or hour < 6
        seans_adi = "Akşam Seansı" if is_evening else "Gündüz Seansı"

        type_labels = {
            "opening": (f"VİOP {seans_adi} Açıldı", f"Vadeli işlem piyasası {seans_adi.lower()} başladı."),
            "closing": (f"VİOP {seans_adi} Kapandı", f"{seans_adi} sona erdi, veriler güncellendi."),
            "flash": ("VİOP Flaş Haber", summary or "Önemli VİOP hareketi tespit edildi."),
        }
        title, default_body = type_labels.get(session_type, ("VİOP Güncelleme", "VİOP güncelleme"))
        body = summary or default_body

        data = {
            "type": "viop_session",
            "session_type": session_type,
            "screen": "viop-gece-seansi",
        }

        return await self._send_filtered(
            "notifications_enabled", title, body, data,
            f"VİOP {session_type}",
            category="system",
        )
