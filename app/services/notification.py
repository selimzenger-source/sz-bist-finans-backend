"""Firebase Cloud Messaging (FCM) push bildirim servisi.

Kullanicilara halka arz ve KAP haber bildirimlerini gonderir.
"""

import json
import logging
from typing import Optional

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

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
    ) -> bool:
        """Tek bir cihaza push bildirim gonderir."""
        if not _firebase_initialized:
            logger.info(f"[DRY-RUN] Push → device: {title} | {body}")
            return True

        try:
            from firebase_admin import messaging

            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body,
                ),
                data=data or {},
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
            logger.info(f"Push bildirim gonderildi: {response}")
            return True

        except Exception as e:
            logger.error(f"Push bildirim hatasi: {e}")
            return False

    async def send_to_topic(
        self,
        topic: str,
        title: str,
        body: str,
        data: Optional[dict] = None,
    ) -> bool:
        """Bir konuya (topic) abone olan tum cihazlara bildirim gonderir."""
        if not _firebase_initialized:
            logger.info(f"[DRY-RUN] Push → topic/{topic}: {title} | {body}")
            return True

        try:
            from firebase_admin import messaging

            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body,
                ),
                data=data or {},
                topic=topic,
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
            logger.info(f"Topic bildirim gonderildi ({topic}): {response}")
            return True

        except Exception as e:
            logger.error(f"Topic bildirim hatasi ({topic}): {e}")
            return False

    # -------------------------------------------------------
    # Halka Arz Bildirimleri
    # -------------------------------------------------------

    async def notify_new_ipo(self, ipo) -> int:
        """Yeni halka arz duyuruldugunda tum kullanicilara bildirim gonder."""
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

        await self.send_to_topic("ipo_all", title, body, data)
        return 1

    async def notify_ipo_subscription_start(self, ipo) -> int:
        """Halka arz basvuru baslangicinda bildirim gonder."""
        title = "📋 Başvuru Başladı"
        body = f"{ipo.ticker or ipo.company_name} halka arz başvurusu başladı!"
        if ipo.subscription_end:
            body += f" Son gün: {ipo.subscription_end.strftime('%d.%m.%Y')}"

        data = {
            "type": "ipo_start",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
        }

        await self.send_to_topic("ipo_all", title, body, data)
        return 1

    async def notify_ipo_last_day(self, ipo) -> int:
        """Halka arz son gun uyarisi."""
        title = "⏰ Son Gün Uyarısı"
        body = f"{ipo.ticker or ipo.company_name} halka arz başvurusu YARIN son gün!"

        data = {
            "type": "ipo_last_day",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
        }

        await self.send_to_topic("ipo_all", title, body, data)
        # Ozel alert kurmus kullanicilara da gonder
        await self.send_to_topic(f"ipo_{ipo.id}", title, body, data)
        return 1

    async def notify_allocation_result(self, ipo, total_applicants: int = 0) -> int:
        """Tahsisat sonucu aciklandi bildirimi."""
        title = "📊 Tahsisat Sonuçları"
        body = f"{ipo.ticker or ipo.company_name} tahsisat sonuçları açıklandı!"
        if total_applicants:
            body += f" ({total_applicants:,} başvuru)"

        data = {
            "type": "ipo_result",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
        }

        await self.send_to_topic("ipo_all", title, body, data)
        await self.send_to_topic(f"ipo_{ipo.id}", title, body, data)
        return 1

    async def notify_ceiling_broken(self, ipo) -> int:
        """Tavan bozuldu bildirimi."""
        title = "🔓 Tavan Çözüldü"
        body = f"{ipo.ticker} tavan çözüldü!"

        data = {
            "type": "ceiling_broken",
            "ipo_id": str(ipo.id),
            "ticker": ipo.ticker or "",
        }

        await self.send_to_topic("ipo_all", title, body, data)
        await self.send_to_topic(f"ipo_{ipo.id}", title, body, data)
        return 1

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
    ) -> int:
        """KAP haber bildirimini ilgili paketlere gonder."""
        sentiment_label = "POZİTİF" if sentiment == "positive" else "NEGATİF"
        session_label = "SEANS İÇİ" if news_type == "seans_ici" else "SEANS DIŞI"

        title = f"{session_label} {sentiment_label} HABER"
        body = f"{ticker}"
        if price:
            body += f" | {price:.2f} TL"
        body += f"\n{matched_keyword}"

        data = {
            "type": "kap_news",
            "ticker": ticker,
            "kap_id": kap_id,
            "sentiment": sentiment,
            "matched_keyword": matched_keyword,
        }

        # Her abonelik paketine uygun topic'e gonder
        from app.services.news_service import (
            BIST30_TICKERS, BIST50_TICKERS, BIST100_TICKERS
        )

        sent = 0
        ticker_upper = ticker.upper()

        # Tum hisseler paketi — her zaman
        await self.send_to_topic("news_all", title, body, data)
        sent += 1

        # BIST 100
        if ticker_upper in BIST100_TICKERS:
            await self.send_to_topic("news_bist100", title, body, data)
            sent += 1

        # BIST 50
        if ticker_upper in BIST50_TICKERS:
            await self.send_to_topic("news_bist50", title, body, data)
            sent += 1

        # BIST 30
        if ticker_upper in BIST30_TICKERS:
            await self.send_to_topic("news_bist30", title, body, data)
            sent += 1

        return sent
