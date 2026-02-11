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
    expo_push_token: Mapped[str | None] = mapped_column(String(255), comment="Expo Push Token (ExponentPushToken[...])")
    platform: Mapped[str | None] = mapped_column(String(10), comment="ios, android")
    app_version: Mapped[str | None] = mapped_column(String(20), comment="Uygulama versiyonu")

    # Master bildirim switch — False ise HICBIR bildirim gitmez
    notifications_enabled: Mapped[bool] = mapped_column(Boolean, default=True, comment="Tum bildirimleri ac/kapat")

    # Bildirim tercihleri
    notify_new_ipo: Mapped[bool] = mapped_column(Boolean, default=True, comment="Yeni halka arz bildirimi")
    notify_ipo_start: Mapped[bool] = mapped_column(Boolean, default=True, comment="Basvuru basladi bildirimi")
    notify_ipo_last_day: Mapped[bool] = mapped_column(Boolean, default=True, comment="Son gun uyarisi")
    notify_ipo_result: Mapped[bool] = mapped_column(Boolean, default=True, comment="Tahsisat sonucu")
    notify_ceiling_break: Mapped[bool] = mapped_column(Boolean, default=True, comment="Tavan bozuldu bildirimi")
    notify_first_trading_day: Mapped[bool] = mapped_column(Boolean, default=True, comment="Ilk islem gunu bildirimi (ucretsiz)")
    notify_kap_bist30: Mapped[bool] = mapped_column(Boolean, default=True, comment="BIST 30 KAP ucretsiz bildirim")
    notify_kap_all: Mapped[bool] = mapped_column(Boolean, default=True, comment="Tum KAP haber bildirimi (ucretli aboneler)")

    # Halka Arz ucretli bildirim tercihleri
    notify_taban_break: Mapped[bool] = mapped_column(Boolean, default=True, comment="Taban acilinca bildirimi")
    notify_daily_open_close: Mapped[bool] = mapped_column(Boolean, default=True, comment="Gunluk acilis kapanis bildirimi")
    notify_percent_drop: Mapped[bool] = mapped_column(Boolean, default=True, comment="Yuzde dusus bildirimi")

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

    Bu KAP haber aboneligi icin (yildiz_pazar / ana_yildiz paketleri).
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

# YENİ v2: Bildirim tipi bazli fiyatlama (hisse basina, 25 gun)
# 4 bildirim tipi — yuzde_dusus tek hizmet: %4 ve %7 esik, gunde max 2 bildirim
NOTIFICATION_TIER_PRICES = {
    "tavan_bozulma": {
        "price_tl": Decimal("15.00"),
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
    "yuzde_dusus": {
        "price_tl": Decimal("20.00"),
        "label": "En Yukseginden % Dusus Bildirimi",
        "description": "%4 ve %7 esik bildirimi — gunde max 2 (once %4, sonra %7)",
    },
}
COMBO_PRICE = Decimal("44.00")  # 50 TL → %11 indirim → ~44 TL
QUARTERLY_PRICE = Decimal("90.00")  # 3 Aylik
ANNUAL_BUNDLE_PRICE = Decimal("245.00")  # Yillik

# YENİ: Haber abonelik fiyatlari — tek paket: Ana+Yildiz
NEWS_TIER_PRICES = {
    "ana_yildiz": {
        "price_tl_monthly": Decimal("75.00"),
        "price_tl_annual": Decimal("675.00"),  # 9 aylik fiyat (3 ay tasarruf)
        "annual_months": 9,
        "label": "Ana Pazar + Yildiz Pazar Hisseleri (~350 hisse)",
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

    4 bildirim tipi (hisse basina, 25 gun):
      - tavan_bozulma:         15 TL (tavan acilinca / kitlenince bildirimi)
      - taban_acilma:          10 TL (taban acilinca bildirim)
      - gunluk_acilis_kapanis:  5 TL (gunluk acilis ve kapanis bilgisi)
      - yuzde4_dusus VEYA yuzde7_dusus: 20 TL (kullanici birini secer)
      - Hepsi secilince:  ~~50~~ 44 TL (%11 indirim)

    Paketler (Tum Halka Arz Canli Anlik Paketi):
      - 3 Aylik:                90 TL
      - Yillik:                245 TL
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
        comment="tavan_bozulma, taban_acilma, gunluk_acilis_kapanis, yuzde_dusus"
    )
    is_annual_bundle: Mapped[bool] = mapped_column(
        Boolean, default=False,
        comment="Paket mi? (quarterly/semiannual/annual)"
    )

    # Yuzde dusus icin kullanicinin sectigi oran (%1-%9)
    custom_percentage: Mapped[int | None] = mapped_column(
        Integer, nullable=True,
        comment="Yuzde dusus icin kullanicinin sectigi oran (1-9)"
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

    # Kullanici bildirimi pasif yapabilir (mute)
    muted: Mapped[bool] = mapped_column(
        Boolean, default=False,
        comment="Kullanici tek tusla sessiz moda alabilir"
    )

    user: Mapped["User"] = relationship(back_populates="stock_notifications")

    __table_args__ = (
        Index("idx_stock_notif_user_ipo", "user_id", "ipo_id"),
        Index("idx_stock_notif_active", "is_active"),
        Index("idx_stock_notif_type", "notification_type"),
    )
