"""Kullanici, abonelik ve bildirim tercihleri modelleri."""

from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Text, Boolean, Integer, DateTime, ForeignKey, Index, Numeric
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

    # Hatirlatma zamanlari (son gun icin)
    reminder_30min: Mapped[bool] = mapped_column(Boolean, default=False, comment="Son gune 30 dk kala")
    reminder_1h: Mapped[bool] = mapped_column(Boolean, default=True, comment="Son gune 1 saat kala")
    reminder_2h: Mapped[bool] = mapped_column(Boolean, default=False, comment="Son gune 2 saat kala")
    reminder_4h: Mapped[bool] = mapped_column(Boolean, default=False, comment="Son gune 4 saat kala")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Iliskiler
    subscription: Mapped["UserSubscription | None"] = relationship(back_populates="user", uselist=False)
    ipo_alerts: Mapped[list["UserIPOAlert"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    ceiling_subscriptions: Mapped[list["CeilingTrackSubscription"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )
    stock_notifications: Mapped[list["StockNotificationSubscription"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class UserSubscription(Base):
    """Kullanici abonelik bilgisi — RevenueCat ile senkronize.

    Bu KAP haber aboneligi icin (bist30/50/100/all paketleri).
    """

    __tablename__ = "user_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), unique=True)

    # Paket bilgisi
    package: Mapped[str] = mapped_column(
        String(20), nullable=False,
        comment="free, bist100, yildiz_pazar, ana_yildiz"
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


# -------------------------------------------------------
# Hisse Bildirim Aboneligi — hisse bazli ucretli paket
# -------------------------------------------------------

# Eski fiyatlama (geriye donuk uyumluluk icin)
CEILING_TIER_PRICES = {
    "5_gun":  {"days": 5,  "price_tl": Decimal("20.00"), "label": "Ilk 5 islem gunu"},
    "10_gun": {"days": 10, "price_tl": Decimal("50.00"), "label": "Ilk 10 islem gunu"},
    "15_gun": {"days": 15, "price_tl": Decimal("60.00"), "label": "Ilk 15 islem gunu"},
    "20_gun": {"days": 20, "price_tl": Decimal("75.00"), "label": "Ilk 20 islem gunu"},
}

# YENİ: Bildirim tipi bazli fiyatlama (hisse basina, 25 gun)
NOTIFICATION_TIER_PRICES = {
    "tavan_bozulma": {
        "price_tl": Decimal("10.00"),
        "label": "Tavan Acilinca / Kitlenince Bildirimi",
        "description": "Tavan acildiginda ve tekrar kitlendiginde anlik bildirim",
    },
    "taban_acilma": {
        "price_tl": Decimal("10.00"),
        "label": "Taban Acilinca Bildirim",
        "description": "Taban acildiginda anlik bildirim",
    },
    "gunluk_acilis_kapanis": {
        "price_tl": Decimal("5.00"),
        "label": "Gunluk Acilis Kapanis Bilgisi",
        "description": "Her gun acilis ve kapanis analizini bildirim olarak al",
    },
    "yuzde4_dusus": {
        "price_tl": Decimal("15.00"),
        "label": "En Yukseginden %4 Dusunce Bildirim",
        "description": "Hisse en yukseginden %4 dustugunde anlik bildirim",
    },
    "yuzde7_dusus": {
        "price_tl": Decimal("15.00"),
        "label": "En Yukseginden %7 Dusunce Bildirim",
        "description": "Hisse en yukseginden %7 dustugunde anlik bildirim",
    },
}
COMBO_PRICE = Decimal("45.00")  # Hepsi secilince ~~55~~ 45 TL
QUARTERLY_PRICE = Decimal("195.00")  # 3 Aylik (istegi 3 bildirim)
SEMIANNUAL_PRICE = Decimal("295.00")  # Tum Halka Arz 6 Aylik (istegi 3 bildirim)
ANNUAL_BUNDLE_PRICE = Decimal("395.00")  # Tum Halka Arz Yillik (istegi 3 bildirim)

# YENİ: Haber abonelik fiyatlari
NEWS_TIER_PRICES = {
    "bist100": {
        "price_tl_monthly": Decimal("45.00"),
        "annual_months": 10,
        "label": "BIST 100 Hisseleri",
        "description": "BIST 100 endeksindeki tum hisselerin haber takibi",
    },
    "yildiz_pazar": {
        "price_tl_monthly": Decimal("65.00"),
        "annual_months": 9,
        "label": "BIST Yildiz Pazar Hisseleri",
        "description": "Yildiz Pazar'daki tum hisselerin haber takibi",
    },
    "ana_yildiz": {
        "price_tl_monthly": Decimal("95.00"),
        "annual_months": 8,
        "label": "Ana Pazar + Yildiz Pazar Hisseleri",
        "description": "Ana ve Yildiz Pazar'daki tum hisselerin haber takibi",
    },
}
COMBINED_ANNUAL_DISCOUNT_PCT = 20  # Halka Arz + Ana+Yildiz kombine indirim


class CeilingTrackSubscription(Base):
    """Tavan takip aboneligi — her halka arz hissesi icin ayri satin alinir.

    Paketler:
      - 5_gun:  Ilk 5 islem gunu tavan/taban takibi — 20 TL
      - 10_gun: Ilk 10 islem gunu tavan/taban takibi — 50 TL
      - 15_gun: Ilk 15 islem gunu tavan/taban takibi — 60 TL
      - 20_gun: Ilk 20 islem gunu tavan/taban takibi — 75 TL

    Her halka arz olan hisse icin gecerli. Kullanici hisse bazli satin alir.
    """

    __tablename__ = "ceiling_track_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    ipo_id: Mapped[int] = mapped_column(ForeignKey("ipos.id", ondelete="CASCADE"))

    # Paket bilgisi
    tier: Mapped[str] = mapped_column(
        String(10), nullable=False,
        comment="5_gun, 10_gun, 15_gun, 20_gun"
    )
    tracking_days: Mapped[int] = mapped_column(Integer, comment="Kac gun takip edilecek (5/10/15/20)")
    price_paid_tl: Mapped[Decimal] = mapped_column(Numeric(8, 2), comment="Odenen fiyat (TL)")

    # RevenueCat / store bilgisi
    revenue_cat_id: Mapped[str | None] = mapped_column(String(255))
    store: Mapped[str | None] = mapped_column(String(20), comment="app_store, play_store")
    product_id: Mapped[str | None] = mapped_column(String(100), comment="Store product ID")

    # Durum
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    purchased_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), comment="Takip bitis tarihi")

    # Bildirim durumu
    notified_count: Mapped[int] = mapped_column(Integer, default=0, comment="Gonderilen bildirim sayisi")

    # Iliskiler
    user: Mapped["User"] = relationship(back_populates="ceiling_subscriptions")

    __table_args__ = (
        Index("idx_ceiling_sub_user_ipo", "user_id", "ipo_id"),
        Index("idx_ceiling_sub_active", "is_active"),
    )


class StockNotificationSubscription(Base):
    """Hisse bazli bildirim aboneligi — 25 gun takip.

    Her halka arz hissesi icin 5 bildirim tipi ayri ayri satin alinabilir:
      - tavan_bozulma:         10 TL (tavan acilinca / kitlenince bildirimi)
      - taban_acilma:          10 TL (taban acilinca bildirim)
      - gunluk_acilis_kapanis:  5 TL (gunluk acilis ve kapanis bilgisi)
      - yuzde4_dusus:          15 TL (en yukseginden %4 dusunce bildirim)
      - yuzde7_dusus:          15 TL (en yukseginden %7 dusunce bildirim)
      - Hepsi secilince:  ~~55~~ 45 TL

    Paketler:
      - 3 Aylik:               195 TL (istegi 3 bildirim)
      - Tum Halka Arz 6 Aylik: 295 TL (istegi 3 bildirim)
      - Tum Halka Arz Yillik:  395 TL (istegi 3 bildirim)
    """

    __tablename__ = "stock_notification_subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    ipo_id: Mapped[int | None] = mapped_column(
        ForeignKey("ipos.id", ondelete="CASCADE"),
        nullable=True,
        comment="NULL ise yillik paket (tum hisseler)"
    )

    # Bildirim tipi
    notification_type: Mapped[str] = mapped_column(
        String(30), nullable=False,
        comment="tavan_bozulma, taban_acilma, gunluk_acilis_kapanis, yuzde4_dusus, yuzde7_dusus"
    )
    is_annual_bundle: Mapped[bool] = mapped_column(
        Boolean, default=False,
        comment="Paket mi? (quarterly/semiannual/annual)"
    )

    # Odeme
    price_paid_tl: Mapped[Decimal] = mapped_column(Numeric(8, 2), comment="Odenen fiyat (TL)")
    revenue_cat_id: Mapped[str | None] = mapped_column(String(255))
    store: Mapped[str | None] = mapped_column(String(20), comment="app_store, play_store")
    product_id: Mapped[str | None] = mapped_column(String(100), comment="Store product ID")

    # Durum
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    purchased_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Bildirim sayaci
    notified_count: Mapped[int] = mapped_column(Integer, default=0)

    user: Mapped["User"] = relationship(back_populates="stock_notifications")

    __table_args__ = (
        Index("idx_stock_notif_user_ipo", "user_id", "ipo_id"),
        Index("idx_stock_notif_active", "is_active"),
        Index("idx_stock_notif_type", "notification_type"),
    )
