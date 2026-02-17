"""Pydantic sema (schema) tanimlari — API request/response modelleri."""

from datetime import date, datetime
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, ConfigDict


# -------------------------------------------------------
# IPO Schemalari
# -------------------------------------------------------

class IPOBrokerOut(BaseModel):
    id: int
    broker_name: str
    broker_type: Optional[str] = None
    is_rejected: bool = False
    application_url: Optional[str] = None
    phone: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class IPOAllocationOut(BaseModel):
    id: int
    group_name: str
    allocation_pct: Optional[Decimal] = None
    allocated_lots: Optional[int] = None
    participant_count: Optional[int] = None
    avg_lot_per_person: Optional[Decimal] = None

    model_config = ConfigDict(from_attributes=True)


class IPOCeilingTrackOut(BaseModel):
    id: int
    trading_day: int
    trade_date: date
    open_price: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    high_price: Optional[Decimal] = None
    low_price: Optional[Decimal] = None
    hit_ceiling: bool = False
    hit_floor: bool = False
    ceiling_broken_at: Optional[datetime] = None
    floor_hit_at: Optional[datetime] = None
    relocked: bool = False
    relocked_at: Optional[datetime] = None
    # v3: 5 durum + kumulatif % fark
    durum: str = "alici_kapatti"
    pct_change: Optional[Decimal] = None

    model_config = ConfigDict(from_attributes=True)


class IPOListOut(BaseModel):
    """Halka arz listesi — ozet bilgi."""
    id: int
    company_name: str
    ticker: Optional[str] = None
    logo_url: Optional[str] = None
    status: str
    ipo_price: Optional[Decimal] = None
    total_lots: Optional[int] = None
    offering_size_tl: Optional[Decimal] = None
    subscription_start: Optional[date] = None
    subscription_end: Optional[date] = None
    subscription_hours: Optional[str] = None
    trading_start: Optional[date] = None
    distribution_method: Optional[str] = None
    participation_method: Optional[str] = None
    market_segment: Optional[str] = None
    public_float_pct: Optional[Decimal] = None
    discount_pct: Optional[Decimal] = None
    ceiling_broken: bool = False
    total_applicants: Optional[int] = None
    estimated_lots_per_person: Optional[int] = None
    # v3 — 5 bolumlu yapi alanlari
    spk_approval_date: Optional[date] = None
    spk_bulletin_no: Optional[str] = None
    distribution_completed: bool = False
    expected_trading_date: Optional[date] = None
    # Arsiv & takip alanlari
    archived: bool = False
    trading_day_count: int = 0
    high_from_start: Optional[Decimal] = None

    model_config = ConfigDict(from_attributes=True)


class IPOTradingOut(IPOListOut):
    """Isleme baslayan IPO — ceiling_tracks + allocations ile birlikte.

    Sadece sections endpoint'in trading ve performance_archive bolumlerinde kullanilir.
    """
    ceiling_tracks: list[IPOCeilingTrackOut] = []
    allocations: list[IPOAllocationOut] = []
    allocation_announced: bool = False


class IPODetailOut(BaseModel):
    """Halka arz detay — tum bilgiler (halkarz.com formati)."""
    id: int
    company_name: str
    ticker: Optional[str] = None
    logo_url: Optional[str] = None
    status: str

    # Fiyat & Buyukluk
    ipo_price: Optional[Decimal] = None
    total_lots: Optional[int] = None
    offering_size_tl: Optional[Decimal] = None
    capital_increase_lots: Optional[int] = None
    partner_sale_lots: Optional[int] = None

    # Tarihler
    subscription_start: Optional[date] = None
    subscription_end: Optional[date] = None
    subscription_hours: Optional[str] = None
    trading_start: Optional[date] = None
    spk_approval_date: Optional[date] = None
    expected_trading_date: Optional[date] = None

    # SPK Referans
    spk_bulletin_no: Optional[str] = None

    # Dagitim & Katilim
    distribution_method: Optional[str] = None
    participation_method: Optional[str] = None
    distribution_completed: bool = False
    public_float_pct: Optional[Decimal] = None
    discount_pct: Optional[Decimal] = None

    # Pazar
    market_segment: Optional[str] = None

    # Tahmini Lot (500K katilimci varsayimi)
    estimated_lots_per_person: Optional[int] = None

    # Ek Bilgiler
    lock_up_period_days: Optional[int] = None
    price_stability_days: Optional[int] = None
    min_application_lot: Optional[int] = None

    # Sirket
    company_description: Optional[str] = None
    sector: Optional[str] = None
    fund_usage: Optional[str] = None
    revenue_current_year: Optional[Decimal] = None
    revenue_previous_year: Optional[Decimal] = None
    gross_profit: Optional[Decimal] = None

    # Linkler
    kap_notification_url: Optional[str] = None
    prospectus_url: Optional[str] = None
    spk_bulletin_url: Optional[str] = None

    # Tahsisat
    allocation_announced: bool = False
    total_applicants: Optional[int] = None

    # Tavan Takip
    ceiling_tracking_active: bool = False
    first_day_close_price: Optional[Decimal] = None
    ceiling_broken: bool = False
    ceiling_broken_at: Optional[datetime] = None

    # Arsiv & Takip
    archived: bool = False
    trading_day_count: int = 0
    high_from_start: Optional[Decimal] = None

    # Zaman
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Iliskiler
    allocations: list[IPOAllocationOut] = []
    ceiling_tracks: list[IPOCeilingTrackOut] = []

    model_config = ConfigDict(from_attributes=True)


# -------------------------------------------------------
# KAP Haber Schemalari
# -------------------------------------------------------

class KapNewsOut(BaseModel):
    id: int
    ticker: str
    price_at_time: Optional[Decimal] = None
    kap_notification_id: Optional[str] = None
    news_title: Optional[str] = None
    news_detail: Optional[str] = None
    matched_keyword: Optional[str] = None
    news_type: str
    sentiment: str
    kap_url: Optional[str] = None
    published_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# -------------------------------------------------------
# Telegram Haber Schemalari (YENi)
# -------------------------------------------------------

class TelegramNewsOut(BaseModel):
    """Telegram kanalindan gelen haber."""
    id: int
    telegram_message_id: int
    message_type: str  # seans_ici_pozitif, seans_ici_negatif, borsa_kapali, seans_disi_acilis
    ticker: Optional[str] = None
    price_at_time: Optional[Decimal] = None
    parsed_title: Optional[str] = None
    parsed_body: Optional[str] = None
    sentiment: str
    kap_notification_id: Optional[str] = None
    expected_trading_date: Optional[date] = None
    gap_pct: Optional[Decimal] = None
    prev_close_price: Optional[Decimal] = None
    theoretical_open: Optional[Decimal] = None
    message_date: Optional[datetime] = None
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# -------------------------------------------------------
# SPK Basvuru Schema
# -------------------------------------------------------

class SPKApplicationOut(BaseModel):
    """SPK onayi beklenen halka arz basvurusu."""
    id: int
    company_name: str
    existing_capital: Optional[Decimal] = None
    new_capital: Optional[Decimal] = None
    capital_increase_paid: Optional[Decimal] = None
    capital_increase_free: Optional[Decimal] = None
    existing_share_sale: Optional[Decimal] = None
    additional_share_sale: Optional[Decimal] = None
    sale_price: Optional[Decimal] = None
    application_date: Optional[datetime] = None
    notes: Optional[str] = None
    status: str = "pending"
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# -------------------------------------------------------
# IPO Bolumler Schema (v3.0 — 5 bolumlu endpoint)
# -------------------------------------------------------

class IPOSectionsOut(BaseModel):
    """Halka arz ana ekrani — 6 bolum.

    1. spk_pending:         SPK onayi beklenen basvurular
    2. newly_approved:      Yeni onaylanan (SPK bulteninden)
    3. in_distribution:     Dagitim surecinde (talep toplama acik)
    4. awaiting_trading:    Islem gunu ilani beklenen
    5. trading:             Isleme baslayanlar (25 gun takip)
    6. performance_archive: 25 gunu gecmis — Ilk 25 Takvim Gunu Performansi
    """
    spk_pending: list[SPKApplicationOut] = []
    newly_approved: list[IPOListOut] = []
    in_distribution: list[IPOListOut] = []
    awaiting_trading: list[IPOListOut] = []
    trading: list[IPOTradingOut] = []
    performance_archive: list[IPOTradingOut] = []
    archived_count: int = 0


# -------------------------------------------------------
# Kullanici Schemalari
# -------------------------------------------------------

class UserRegister(BaseModel):
    """Cihaz kayit istegi."""
    device_id: str
    fcm_token: str
    expo_push_token: Optional[str] = None
    platform: str  # ios, android
    app_version: Optional[str] = None


class UserUpdate(BaseModel):
    """Kullanici bilgi guncelleme."""
    fcm_token: Optional[str] = None
    expo_push_token: Optional[str] = None
    app_version: Optional[str] = None
    notifications_enabled: Optional[bool] = None
    notify_new_ipo: Optional[bool] = None
    notify_ipo_start: Optional[bool] = None
    notify_ipo_last_day: Optional[bool] = None
    notify_ipo_result: Optional[bool] = None
    notify_ceiling_break: Optional[bool] = None
    notify_first_trading_day: Optional[bool] = None
    notify_kap_bist30: Optional[bool] = None
    notify_kap_all: Optional[bool] = None
    # Halka Arz ucretli bildirim tercihleri
    notify_taban_break: Optional[bool] = None
    notify_daily_open_close: Optional[bool] = None
    notify_percent_drop: Optional[bool] = None
    # Hatirlatma zamanları
    reminder_30min: Optional[bool] = None
    reminder_1h: Optional[bool] = None
    reminder_2h: Optional[bool] = None
    reminder_4h: Optional[bool] = None
    # Hesap silme
    deleted: Optional[bool] = None


class ReminderSettingsUpdate(BaseModel):
    """Hatirlatma zamani ayarlari."""
    reminder_30min: bool = False
    reminder_1h: bool = True
    reminder_2h: bool = False
    reminder_4h: bool = False


class UserOut(BaseModel):
    id: int
    device_id: str
    platform: Optional[str] = None
    notifications_enabled: bool = True
    notify_new_ipo: bool = True
    notify_ipo_start: bool = True
    notify_ipo_last_day: bool = True
    notify_ipo_result: bool = True
    notify_ceiling_break: bool = True
    notify_first_trading_day: bool = True
    notify_kap_bist30: bool = True
    notify_kap_all: bool = True
    notify_taban_break: bool = True
    notify_daily_open_close: bool = True
    notify_percent_drop: bool = True
    reminder_30min: bool = False
    reminder_1h: bool = True
    reminder_2h: bool = False
    reminder_4h: bool = False
    deleted: bool = False
    subscription_package: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class SubscriptionInfo(BaseModel):
    package: str  # free, bist100, yildiz_pazar, ana_yildiz
    is_active: bool = False
    expires_at: Optional[datetime] = None


class IPOAlertCreate(BaseModel):
    """Belirli bir halka arz icin bildirim tercihi."""
    ipo_id: int
    notify_last_day: bool = True
    notify_result: bool = True
    notify_ceiling: bool = True


# -------------------------------------------------------
# Tavan Takip (Matriks Pipeline)
# -------------------------------------------------------

class CeilingTrackUpdate(BaseModel):
    """Matriks Excel pipeline'indan gelen tavan/taban bilgisi."""
    ticker: str
    trading_day: int
    trade_date: date
    open_price: Optional[Decimal] = None
    close_price: Decimal
    high_price: Optional[Decimal] = None
    low_price: Optional[Decimal] = None
    hit_ceiling: bool
    hit_floor: bool = False
    alis_lot: Optional[int] = None    # 1. kademe alis lotu
    satis_lot: Optional[int] = None   # 1. kademe satis lotu


# -------------------------------------------------------
# Tavan Takip Abonelik Schemalari
# -------------------------------------------------------

class CeilingTierOut(BaseModel):
    """Tavan takip paket bilgisi."""
    tier: str           # 5_gun, 10_gun, 15_gun, 20_gun
    days: int           # 5, 10, 15, 20
    price_tl: Decimal   # 20, 50, 60, 75
    label: str          # "Ilk 5 islem gunu"


class CeilingSubscriptionCreate(BaseModel):
    """Tavan takip aboneligi olusturma istegi."""
    ipo_id: int
    tier: str  # 5_gun, 10_gun, 15_gun, 20_gun


class CeilingSubscriptionOut(BaseModel):
    """Tavan takip abonelik bilgisi."""
    id: int
    ipo_id: int
    tier: str
    tracking_days: int
    price_paid_tl: Decimal
    is_active: bool = True
    purchased_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    notified_count: int = 0

    model_config = ConfigDict(from_attributes=True)


# -------------------------------------------------------
# Hisse Bildirim Abonelik Schemalari (YENi)
# -------------------------------------------------------

class NotificationTierOut(BaseModel):
    """Bildirim tipi fiyat bilgisi."""
    type: str           # tavan_bozulma, gunluk_acilis_kapanis, yuzde4_dusus
    price_tl: Decimal
    label: str
    description: str


class NewsTierOut(BaseModel):
    """Haber abonelik paket bilgisi."""
    package: str             # bist100, yildiz_pazar, ana_yildiz
    price_tl_monthly: Decimal
    annual_months: int       # Yillik alindiginda kac ay odenir
    annual_price_tl: Decimal  # Hesaplanmis yillik fiyat
    label: str
    description: str


class RealtimeNotifRequest(BaseModel):
    """Gercek zamanli bildirim gonderimi (halka_arz_sync.py'den gelir)."""
    admin_password: str
    ticker: str
    notification_type: str  # tavan_bozulma, taban_acilma, gunluk_acilis_kapanis, yuzde_dusus
    title: str
    body: str
    sub_event: Optional[str] = None  # yuzde_dusus icin: "pct4" veya "pct7"


class StockNotificationCreate(BaseModel):
    """Hisse bazli bildirim aboneligi olusturma."""
    ipo_id: Optional[int] = None  # None ise yillik paket
    notification_type: str  # tavan_bozulma, taban_acilma, gunluk_acilis_kapanis, yuzde_dusus
    is_annual_bundle: bool = False


class StockNotificationOut(BaseModel):
    """Hisse bildirim abonelik bilgisi."""
    id: int
    ipo_id: Optional[int] = None
    notification_type: str
    is_annual_bundle: bool = False
    custom_percentage: Optional[int] = None
    price_paid_tl: Decimal
    is_active: bool = True
    purchased_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    notified_count: int = 0
    muted: bool = False

    model_config = ConfigDict(from_attributes=True)


# -------------------------------------------------------
# Temettu Schemalari (yakinda)
# -------------------------------------------------------

class DividendOut(BaseModel):
    """Temettu beklentisi bilgisi."""
    id: int
    ticker: str
    company_name: Optional[str] = None
    expected_dividend_yield_pct: Optional[Decimal] = None
    expected_year: Optional[int] = None
    last_year_yield_pct: Optional[Decimal] = None
    avg_2y_yield_pct: Optional[Decimal] = None
    avg_3y_yield_pct: Optional[Decimal] = None
    pd_dd: Optional[Decimal] = None
    fk: Optional[Decimal] = None
    ytd_return_pct: Optional[Decimal] = None
    yearly_return_pct: Optional[Decimal] = None
    scraped_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


# -------------------------------------------------------
# Cuzdan (Wallet) Schemalari
# -------------------------------------------------------

class WalletBalanceOut(BaseModel):
    """Kullanici cuzdan bakiyesi."""
    balance: float
    daily_ads_watched: int
    max_daily_ads: int
    cooldown_remaining: int  # Saniye cinsinden kalan bekleme
    can_watch_ad: bool


class WalletEarnRequest(BaseModel):
    """Reklam izleme sonrasi puan kazanimi."""
    reward_type: str = "ad_reward"  # Ileride farkli kazanc tipleri


class WalletSpendRequest(BaseModel):
    """Puan harcama istegi."""
    amount: float
    spend_type: str  # spend_news, spend_ipo, spend_notif
    description: Optional[str] = None


class WalletCouponRequest(BaseModel):
    """Kupon kullanim istegi."""
    code: str


class WalletTransactionOut(BaseModel):
    """Cuzdan islem kaydi."""
    id: int
    amount: float
    tx_type: str
    description: Optional[str] = None
    balance_after: float
    created_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)
