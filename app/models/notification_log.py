"""Bildirim Merkezi — Gonderilen bildirimlerin kaydi.

Her push bildirim gonderildiginde bu tabloya da kaydedilir.
Kullanicilar "Bildirim Merkezi" sayfasindan son 24 saatlik
bildirimlerini gorebilir.

Kategoriler:
- kap_watchlist: Favori hisse KAP bildirimi
- kap_news: AI Pozitif Haber bildirimi (BIST30/50 + ucretli)
- ipo: Halka arz bildirimi (onay, dagitim, son gun, sonuc, islem)
- system: Sistem bildirimi

Her gece 02:00'de 24 saatten eski kayitlar temizlenir.
"""

from datetime import datetime
from sqlalchemy import String, Text, Boolean, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class NotificationLog(Base):
    """Gonderilen bildirim kaydi."""

    __tablename__ = "notification_logs"

    id: Mapped[int] = mapped_column(primary_key=True)

    device_id: Mapped[str] = mapped_column(
        String(100), nullable=False,
        comment="Kullanici cihaz ID"
    )
    title: Mapped[str] = mapped_column(
        String(500), nullable=False,
        comment="Bildirim basligi"
    )
    body: Mapped[str | None] = mapped_column(
        Text,
        comment="Bildirim govdesi"
    )
    category: Mapped[str] = mapped_column(
        String(30), nullable=False, default="system",
        comment="Kategori: kap_watchlist, kap_news, ipo, system"
    )
    data_json: Mapped[str | None] = mapped_column(
        Text,
        comment="Bildirim data payload (JSON string)"
    )
    is_read: Mapped[bool] = mapped_column(
        Boolean, default=False,
        comment="Okundu mu"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_notiflog_device_created", "device_id", "created_at"),
        Index("idx_notiflog_created", "created_at"),
    )
