"""Firebase Cloud Messaging (FCM) + Expo Push bildirim servisi.

Kullanicilara halka arz ve KAP haber bildirimlerini gonderir.
FCM token varsa Firebase, ExponentPushToken varsa Expo Push API kullanilir.
Ust uste seri bildirim onlemek icin her bildirim arasi 5 saniye beklenir.
"""

import asyncio
import json
import logging
from typing import Optional

from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

# Bildirimler arasi bekleme suresi (saniye) — seri bildirim onleme
NOTIFICATION_DELAY_SECONDS = 2

logger = logging.getLogger(__name__)

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
                        notification_priority="PRIORITY_MAX",
                        visibility="PUBLIC",
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
            if error_name in ("UnregisteredError", "InvalidArgumentError", "SenderIdMismatchError"):
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
                await self.db.flush()
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
    ) -> bool:
        """FCM veya Expo token ile kullaniciya bildirim gonderir.

        FCM token varsa Firebase, ExponentPushToken varsa Expo Push API kullanilir.
        """
        fcm = (user.fcm_token or "").strip()
        expo = (user.expo_push_token or "").strip()

        if fcm:
            return await self.send_to_device(
                fcm_token=fcm,
                title=title,
                body=body,
                data=data,
                channel_id=channel_id,
                delay=delay,
            )
        elif expo and expo.startswith("ExponentPushToken"):
            return await self.send_to_expo_device(
                expo_token=expo,
                title=title,
                body=body,
                data=data,
                delay=delay,
            )
        else:
            logger.warning(f"Kullanicinin gecerli tokeni yok: user_id={user.id}")
            return False

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
                await self.db.flush()

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
        }

        return await self._send_filtered(
            "notify_new_ipo", title, body, data,
            f"Yeni halka arz: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
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
        }

        return await self._send_filtered(
            "notify_ipo_start", title, body, data,
            f"Basvuru basladi: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
        )

    async def notify_ipo_last_day(self, ipo) -> int:
        """Son gun uyarisi — notify_ipo_last_day = True olanlara."""
        title = "⏰ Son Gün Uyarısı"
        body = f"{ipo.ticker or ipo.company_name} halka arz başvurusu YARIN son gün!"

        data = {
            "type": "ipo_last_day",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
        }

        return await self._send_filtered(
            "notify_ipo_last_day", title, body, data,
            f"Son gun uyarisi: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
        )

    async def notify_allocation_result(self, ipo, total_applicants: int = 0) -> int:
        """Dagitim sonucu bildirimi — notify_ipo_result = True olanlara.

        Bildirim icerigi:
        - Baslik: Dagitim Sonuclari
        - Govde: Ticker, toplam basvuran, bireysel kisi, dagitilan lot
        """
        title = "📊 Dağıtım Sonuçları"

        ticker = ipo.ticker or ipo.company_name
        parts = [f"{ticker} dağıtım sonuçları açıklandı!"]

        # Toplam basvuran
        t_applicants = total_applicants or getattr(ipo, "total_applicants", None)
        if t_applicants:
            parts.append(f"Toplam başvuru: {int(t_applicants):,} kişi")

        # Bireysel kisi ve lot
        bireysel_kisi = getattr(ipo, "result_bireysel_kisi", None)
        bireysel_lot = getattr(ipo, "result_bireysel_lot", None)
        if bireysel_kisi:
            parts.append(f"Yurt içi bireysel: {int(bireysel_kisi):,} kişi")
        if bireysel_lot:
            parts.append(f"Dağıtılan lot: {int(bireysel_lot):,}")

        # Kisi basi lot
        if bireysel_kisi and bireysel_lot and bireysel_kisi > 0:
            avg = bireysel_lot / bireysel_kisi
            parts.append(f"Kişi başı: ~{avg:.0f} lot")

        body = "\n".join(parts)

        data = {
            "type": "ipo_result",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
        }

        return await self._send_filtered(
            "notify_ipo_result", title, body, data,
            f"Dagitim sonucu: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
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
        }

        return await self._send_filtered(
            "notify_first_trading_day", title, body, data,
            f"Ilk islem gunu: {ipo.ticker or ipo.company_name}",
            channel_id="ipo_alerts_v2",
        )

    async def notify_ceiling_broken(self, ipo) -> int:
        """Tavan bozuldu bildirimi — notify_ceiling_break = True olanlara."""
        title = "🔓 Tavan Çözüldü"
        body = f"{ipo.ticker} tavan çözüldü!"

        data = {
            "type": "ceiling_broken",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
        }

        return await self._send_filtered(
            "notify_ceiling_break", title, body, data,
            f"Tavan bozuldu: {ipo.ticker}",
            channel_id="ceiling_alerts_v2",
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
    ) -> int:
        """KAP haber bildirimini gonder (sadece pozitif).

        Ucretli aboneler (ana_yildiz): Per-user bildirim — notify_kap_all kontrollu
        Ucretsiz BIST 50: Per-user push (_send_bist50_free) — ucretli aboneler haric (dedup)

        3 Tip Bildirim:
        - Seans Ici Pozitif Haber Yakalandi
        - Seans Disi Pozitif Haber Yakalandi
        - Seans Disi Haber Yakalanan Hisse Acilisi (GAP bilgisi ile)
        """
        if news_type == "seans_ici":
            title = f"⚡ Seans İçi Pozitif Haber Yakalandı - {ticker}"
        elif news_type == "seans_disi_acilis":
            title = f"📊 Seans Dışı Yakalanan Hisse Açılışı - {ticker}"
        else:
            title = f"🌙 Seans Dışı Pozitif Haber Yakalandı - {ticker}"

        # Fiyat bilgisi gonderilmez (veri ihlali)
        body = f"Sembol: {ticker}\n{matched_keyword}"
        # Seans ici yuzdesel degisim varsa ekle
        if news_type == "seans_ici" and pct_change:
            body += f"\nDeğişim: {pct_change}"

        data = {
            "type": "kap_news",
            "ticker": ticker,
            "kap_id": kap_id,
            "sentiment": sentiment,
            "matched_keyword": matched_keyword,
        }

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

        2 kaynak:
        1. ana_yildiz KAP haber aboneleri (UserSubscription) — notify_kap_all kontrollu
        2. 3 aylik / yillik hisse bildirim paketi sahipleri (StockNotificationSubscription)
           — notify_kap_all filtresi YOK (hisse paketi aldiysa KAP haberi alir)
        """
        from app.models.user import User, UserSubscription, StockNotificationSubscription

        # 1) ana_yildiz KAP haber aboneleri — notify_kap_all == True zorunlu
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

        # 2) 3 aylik / yillik hisse bildirim paketi sahipleri
        # NOT: notify_kap_all filtresi yok — hisse paketi alan kisi KAP haberini alir
        stock_bundle_result = await self.db.execute(
            select(User)
            .join(
                StockNotificationSubscription,
                StockNotificationSubscription.user_id == User.id,
            )
            .where(
                and_(
                    StockNotificationSubscription.is_active == True,
                    StockNotificationSubscription.is_annual_bundle == True,
                    User.notifications_enabled == True,
                    User.deleted == False,
                    or_(
                        and_(User.fcm_token.isnot(None), User.fcm_token != ""),
                        and_(User.expo_push_token.isnot(None), User.expo_push_token != ""),
                    ),
                )
            )
        )
        stock_users = list(stock_bundle_result.scalars().all())

        # Dedup — ayni kullaniciya 2 kere gonderme
        seen_ids: set[int] = set()
        all_users: list = []
        for u in kap_users + stock_users:
            if u.id not in seen_ids:
                seen_ids.add(u.id)
                all_users.append(u)

        sent_count = 0
        failed_count = 0
        for user in all_users:
            try:
                success = await self._send_to_user(
                    user=user,
                    title=title,
                    body=body,
                    data=data,
                    channel_id="kap_news_v2",
                )
                if success:
                    sent_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                logger.warning("Ucretli KAP bildirim hatasi (user=%s): %s", user.id, e)

        logger.info(
            "Ucretli KAP bildirim: %s — %d kullaniciya gonderildi (kap=%d, stock_bundle=%d)",
            ticker, sent_count, len(kap_users), len(stock_users),
        )

        # Telegram admin raporu
        try:
            from app.services.admin_telegram import notify_push_sent
            await notify_push_sent(
                notification_type=f"KAP Ucretli: {ticker}",
                title=title,
                sent_count=sent_count,
                failed_count=failed_count,
                detail=f"KAP abone: {len(kap_users)}, Bundle: {len(stock_users)}",
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
        """BIST 50 ucretsiz bildirim — ucretli aboneligi OLMAYAN kullanicilara.

        Dedup: _send_paid_kap_news ile zaten bildirim alan kullanicilar haric tutulur:
        - UserSubscription aktif ana_yildiz olanlar
        - StockNotificationSubscription aktif bundle olanlar
        """
        from app.models.user import User, UserSubscription, StockNotificationSubscription

        # Ucretli abonelerin user_id'leri (haric tutulacak)
        paid_kap_ids = (
            select(UserSubscription.user_id)
            .where(
                and_(
                    UserSubscription.is_active == True,
                    UserSubscription.package == "ana_yildiz",
                )
            )
        )
        paid_bundle_ids = (
            select(StockNotificationSubscription.user_id)
            .where(
                and_(
                    StockNotificationSubscription.is_active == True,
                    StockNotificationSubscription.is_annual_bundle == True,
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
                    User.id.notin_(paid_bundle_ids),
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
