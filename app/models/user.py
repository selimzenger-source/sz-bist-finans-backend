"""Kullanici, abonelik, bildirim tercihleri ve cuzdan modelleri."""

from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Text, Boolean, Integer, DateTime, ForeignKey, Index, Numeric, Float
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.database import Base


class User(Base):
    """Mobil uygulama kullanicisi — cihaz bazli kayit."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    device_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="Benzersiz cihaz ID")
    persistent_id: Mapped[str | None] = mapped_column(String(255), nullable=True, unique=True, comment="Kalici cihaz ID (Android ID / iOS Keychain). Silip yukleme sonrasi hesap kurtarma icin.")
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
    notify_edo_free: Mapped[bool] = mapped_column(Boolean, default=True, comment="Ucretsiz EDO %1 bildirimi")
    notify_kap_watchlist: Mapped[bool] = mapped_column(Boolean, default=True, comment="KAP takip listesi bildirimleri")

    # Hatirlatma zamanlari (son gun icin)
    reminder_30min: Mapped[bool] = mapped_column(Boolean, default=True, comment="Son gune 30 dk kala")
    reminder_1h: Mapped[bool] = mapped_column(Boolean, default=False, comment="Son gune 1 saat kala")
    reminder_2h: Mapped[bool] = mapped_column(Boolean, default=False, comment="Son gune 2 saat kala")
    reminder_4h: Mapped[bool] = mapped_column(Boolean, default=False, comment="Son gune 4 saat kala")

    # --- Cuzdan (Wallet) ---
    wallet_balance: Mapped[float] = mapped_column(Float, default=0.0, comment="Puan bakiyesi")
    daily_ads_watched: Mapped[int] = mapped_column(Integer, default=0, comment="Bugun izlenen reklam sayisi")
    last_ad_watched_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, comment="Son reklam izleme zamani")
    ads_reset_date: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="Gunluk reklam sayaci tarihi (YYYY-MM-DD)")
    last_daily_checkin: Mapped[str | None] = mapped_column(String(20), nullable=True, comment="Son gunluk giris puan tarihi (YYYY-MM-DD)")

    # Hesap silme (soft-delete — Google Play zorunlulugu)
    deleted: Mapped[bool] = mapped_column(Boolean, default=False, comment="Kullanici hesabini sildi")
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, comment="Silme talep tarihi")

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
    wallet_transactions: Mapped[list["WalletTransaction"]] = relationship(
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
    "5_gun":  {"days": 5,  "price_tl": Decimal("20.00"), "label": "İlk 5 işlem günü"},
    "10_gun": {"days": 10, "price_tl": Decimal("50.00"), "label": "İlk 10 işlem günü"},
    "15_gun": {"days": 15, "price_tl": Decimal("60.00"), "label": "İlk 15 işlem günü"},
    "20_gun": {"days": 20, "price_tl": Decimal("75.00"), "label": "İlk 20 işlem günü"},
}

# YENİ v2: Bildirim tipi bazli fiyatlama (hisse basina, 25 gun)
# 5 bildirim tipi — v20: el_degistirme eklendi
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
    "el_degistirme": {
        "price_tl": Decimal("10.00"),
        "label": "El Degistirme Orani (E.D.O)",
        "description": "Kumulatif %10, %25, %50, %75, %100, %125 esiklerinde bildirim",
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
COMBO_PRICE = Decimal("44.00")  # 60 TL → %27 indirim → 44 TL (60 TL yerine 44 TL)
QUARTERLY_PRICE = Decimal("90.00")  # 3 Aylik
ANNUAL_BUNDLE_PRICE = Decimal("245.00")  # Yillik

# YENİ: Haber abonelik fiyatlari — tek paket: Ana+Yıldız
NEWS_TIER_PRICES = {
    "ana_yildiz": {
        "price_tl_monthly": Decimal("75.00"),
        "price_tl_annual": Decimal("675.00"),  # 9 aylik fiyat (3 ay tasarruf)
        "annual_months": 9,
        "label": "Ana Pazar + Yıldız Pazar Hisseleri (~350 hisse)",
        "description": "Ana ve Yıldız Pazar'daki tüm hisselerin haber takibi",
    },
}
COMBINED_ANNUAL_DISCOUNT_PCT = 20  # Halka Arz + Ana+Yıldız kombine indirim


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
    ipo_id: Mapped[int | None] = mapped_column(
        ForeignKey("ipos.id", ondelete="SET NULL"),
        nullable=True,
        comment="IPO silinirse abonelik korunur"
    )

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
        ForeignKey("ipos.id", ondelete="SET NULL"),
        nullable=True,
        comment="NULL ise yillik paket (tum hisseler). IPO silinirse abonelik korunur."
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
    # Bundle aboneliklerde tip bazli mute (JSON array: ["tavan_bozulma","taban_acilma"])
    muted_types: Mapped[str | None] = mapped_column(
        Text, nullable=True, default=None,
        comment="Bundle icin tip bazli mute — JSON array"
    )

    user: Mapped["User"] = relationship(back_populates="stock_notifications")

    __table_args__ = (
        Index("idx_stock_notif_user_ipo", "user_id", "ipo_id"),
        Index("idx_stock_notif_active", "is_active"),
        Index("idx_stock_notif_type", "notification_type"),
    )


# -------------------------------------------------------
# Cuzdan Islem Gecmisi — her puan hareketi loglanir
# -------------------------------------------------------

class WalletTransaction(Base):
    """Cuzdan islem gecmisi — reklam kazanimi, kupon, harcama."""

    __tablename__ = "wallet_transactions"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))

    amount: Mapped[float] = mapped_column(Float, comment="Islem miktari (pozitif=kazanc, negatif=harcama)")
    tx_type: Mapped[str] = mapped_column(
        String(30), nullable=False,
        comment="ad_reward, coupon, spend_news, spend_ipo, spend_notif"
    )
    description: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Islem aciklamasi")
    balance_after: Mapped[float] = mapped_column(Float, comment="Islem sonrasi bakiye")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped["User"] = relationship(back_populates="wallet_transactions")

    __table_args__ = (
        Index("idx_wallet_tx_user", "user_id"),
        Index("idx_wallet_tx_type", "tx_type"),
    )


# -------------------------------------------------------
# Dinamik Kupon Modeli — admin panelden yonetilebilir
# -------------------------------------------------------

class Coupon(Base):
    """Kampanya kupon kodlari — admin panelden olusturulur, DB'de saklanir."""

    __tablename__ = "coupons"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False, index=True)
    amount: Mapped[float] = mapped_column(Float, nullable=False, comment="Puan miktari")
    max_uses: Mapped[int] = mapped_column(Integer, default=1, comment="Maks kullanim sayisi")
    uses_count: Mapped[int] = mapped_column(Integer, default=0, comment="Mevcut kullanim sayisi")
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="Son kullanma tarihi (None=sinirsiz)"
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, comment="Aktif mi")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# -------------------------------------------------------
# Eski Kupon Kodlari — geriye uyumluluk (fallback)
# -------------------------------------------------------

WALLET_COUPONS: dict[str, float] = {
    "SZ250X": 250.0,
    "SZ500C": 500.0,
    "SZ750D": 750.0,
    "SZ1000K": 1000.0,
    "SZ1250H": 1250.0,
    "SZ1500M": 1500.0,
    "SZ1750L": 1750.0,
    "SZ2000Y": 2000.0,
    "SZ2500Z": 2500.0,
}

# Reklam ayarlari
WALLET_REWARD_AMOUNT = 3.0    # Reklam basina kazanilan puan
WALLET_COOLDOWN_SECONDS = 350  # 5 dakika 50 saniye bekleme
WALLET_MAX_DAILY_ADS = 15     # Gunluk max reklam


# -------------------------------------------------------
# X (Twitter) Otomatik Reply Sistemi
# -------------------------------------------------------

class ReplyTarget(Base):
    """Takip edilen X (Twitter) hesabi — otomatik reply icin."""

    __tablename__ = "reply_targets"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(
        String(50), unique=True, nullable=False, index=True,
        comment="X kullanici adi (@ olmadan)",
    )
    twitter_user_id: Mapped[str | None] = mapped_column(
        String(30), nullable=True,
        comment="Twitter API user ID (cache)",
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, comment="Aktif mi")
    last_seen_tweet_id: Mapped[str | None] = mapped_column(
        String(30), nullable=True,
        comment="Son gorulmus tweet ID — bundan onceki tweetlere reply atilmaz",
    )
    last_reply_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, default=None,
        comment="Son reply zamani — 1 saat cooldown icin",
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class AutoReply(Base):
    """Otomatik atilan reply logu."""

    __tablename__ = "auto_replies"

    id: Mapped[int] = mapped_column(primary_key=True)
    target_tweet_id: Mapped[str] = mapped_column(
        String(30), unique=True, nullable=False, index=True,
        comment="Yanit verilen tweet ID",
    )
    target_username: Mapped[str] = mapped_column(String(50), nullable=False, comment="Tweet sahibi")
    target_text: Mapped[str] = mapped_column(Text, nullable=False, comment="Orijinal tweet metni")
    reply_text: Mapped[str] = mapped_column(Text, nullable=False, comment="Gonderilen reply")
    reply_tweet_id: Mapped[str | None] = mapped_column(
        String(30), nullable=True, comment="X'teki reply tweet ID",
    )
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="replied",
        comment="replied / failed / skipped / unsafe",
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FeatureInterest(Base):
    """Kullanıcı özellik talep kaydı — talep ölçümü için."""

    __tablename__ = "feature_interests"

    id: Mapped[int] = mapped_column(primary_key=True)
    device_id: Mapped[str | None] = mapped_column(String(255), nullable=True, comment="Cihaz ID")
    feature_name: Mapped[str] = mapped_column(
        String(100), nullable=False, index=True,
        comment="Özellik adı (bilanco_analizi vb.)",
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


# Baslangic reply hedefleri (DB seed — sistem ilk acilista ekler)
DEFAULT_REPLY_TARGETS = [
    "mehmetmesci", "arifcoskun05", "taaardu", "BORSAIZINDE",
    "PiyasaTurkiye", "suatyildiz", "MertBasaran_inv", "BoraOzkent",
    "mervedemirel___", "TanerGenek", "ademayan66", "borsaninizinden", "kursadbucak",
]
