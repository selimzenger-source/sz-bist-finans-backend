"""Kullanici, abonelik ve bildirim tercihleri modelleri."""

from datetime import datetime
from sqlalchemy import String, Text, Boolean, Integer, DateTime, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.database import Base


class User(Base):
    """Mobil uygulama kullanicisi — cihaz bazli kayit."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    device_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="Benzersiz cihaz ID")
    fcm_token: Mapped[str | None] = mapped_column(Text, comment="Firebase push token")
    platform: Mapped[str | None] = mapped_column(String(10), comment="ios, android")
    app_version: Mapped[str | None] = mapped_column(String(20), comment="Uygulama versiyonu")

    # Bildirim tercihleri
    notify_new_ipo: Mapped[bool] = mapped_column(Boolean, default=True, comment="Yeni halka arz bildirimi")
    notify_ipo_start: Mapped[bool] = mapped_column(Boolean, default=True, comment="Basvuru basladi bildirimi")
    notify_ipo_last_day: Mapped[bool] = mapped_column(Boolean, default=True, comment="Son gun uyarisi")
    notify_ipo_result: Mapped[bool] = mapped_column(Boolean, default=True, comment="Tahsisat sonucu")
    notify_ceiling_break: Mapped[bool] = mapped_column(Boolean, default=True, comment="Tavan bozuldu bildirimi")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Iliskiler
    subscription: Mapped["UserSubscription | None"] = relationship(back_populates="user", uselist=False)
    ipo_alerts: Mapped[list["UserIPOAlert"]] = relationship(back_populates="user", cascade="all, delete-orphan")


class UserSubscription(Base):
    """Kullanici abonelik bilgisi — RevenueCat ile senkronize."""

    __tablename__ = "user_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), unique=True)

    # Paket bilgisi
    package: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="free, bist30, bist50, bist100, all"
    )

    # RevenueCat
    revenue_cat_id: Mapped[str | None] = mapped_column(String(255), comment="RevenueCat subscriber ID")
    store: Mapped[str | None] = mapped_column(String(20), comment="app_store, play_store")
    product_id: Mapped[str | None] = mapped_column(String(100), comment="Store product ID")

    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped["User"] = relationship(back_populates="subscription")


class UserIPOAlert(Base):
    """Kullanicinin takip ettigi halka arzlar — ozel bildirim tercihi."""

    __tablename__ = "user_ipo_alerts"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    ipo_id: Mapped[int] = mapped_column(ForeignKey("ipos.id", ondelete="CASCADE"))

    notify_last_day: Mapped[bool] = mapped_column(Boolean, default=True)
    notify_result: Mapped[bool] = mapped_column(Boolean, default=True)
    notify_ceiling: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped["User"] = relationship(back_populates="ipo_alerts")

    __table_args__ = (
        Index("idx_user_ipo_alert", "user_id", "ipo_id", unique=True),
    )
