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
    close_price: Optional[Decimal] = None
    hit_ceiling: bool = True
    ceiling_broken_at: Optional[datetime] = None
    relocked: bool = False

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
    trading_start: Optional[date] = None
    distribution_method: Optional[str] = None
    market_segment: Optional[str] = None
    lead_broker: Optional[str] = None
    public_float_pct: Optional[Decimal] = None
    discount_pct: Optional[Decimal] = None
    ceiling_broken: bool = False

    model_config = ConfigDict(from_attributes=True)


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

    # Dagitim
    distribution_method: Optional[str] = None
    public_float_pct: Optional[Decimal] = None
    discount_pct: Optional[Decimal] = None

    # Pazar & Araci
    market_segment: Optional[str] = None
    lead_broker: Optional[str] = None

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

    # Zaman
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Iliskiler
    brokers: list[IPOBrokerOut] = []
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
# Kullanici Schemalari
# -------------------------------------------------------

class UserRegister(BaseModel):
    """Cihaz kayit istegi."""
    device_id: str
    fcm_token: str
    platform: str  # ios, android
    app_version: Optional[str] = None


class UserUpdate(BaseModel):
    """Kullanici bilgi guncelleme."""
    fcm_token: Optional[str] = None
    app_version: Optional[str] = None
    notify_new_ipo: Optional[bool] = None
    notify_ipo_start: Optional[bool] = None
    notify_ipo_last_day: Optional[bool] = None
    notify_ipo_result: Optional[bool] = None
    notify_ceiling_break: Optional[bool] = None


class UserOut(BaseModel):
    id: int
    device_id: str
    platform: Optional[str] = None
    notify_new_ipo: bool = True
    notify_ipo_start: bool = True
    notify_ipo_last_day: bool = True
    notify_ipo_result: bool = True
    notify_ceiling_break: bool = True
    subscription_package: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class SubscriptionInfo(BaseModel):
    package: str  # free, bist30, bist50, bist100, all
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
    """Matriks Excel pipeline'indan gelen tavan bilgisi."""
    ticker: str
    trading_day: int
    trade_date: date
    close_price: Decimal
    hit_ceiling: bool
