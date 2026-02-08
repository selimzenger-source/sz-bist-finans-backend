"""Halka Arz (IPO) veritabani modelleri.

Referans: halkarz.com detay sayfasi yapisina uygun.
Veri kaynaklari: KAP + SPK resmi bildirimleri.
"""

from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import (
    String, Text, Integer, BigInteger, Boolean, Date, DateTime,
    Numeric, ForeignKey, Index
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from app.database import Base


class IPO(Base):
    """Halka arz ana tablosu — bir sirketin halka arz bilgilerini icerir."""

    __tablename__ = "ipos"

    id: Mapped[int] = mapped_column(primary_key=True)

    # --- Temel Bilgiler ---
    company_name: Mapped[str] = mapped_column(Text, nullable=False, comment="Sirket adi")
    ticker: Mapped[str | None] = mapped_column(String(10), comment="Borsa kodu, orn: ATATR")
    logo_url: Mapped[str | None] = mapped_column(Text, comment="Sirket logo URL")

    # --- Durum ---
    status: Mapped[str] = mapped_column(
        String(20), default="upcoming",
        comment="upcoming, active, completed, postponed, cancelled"
    )

    # --- Fiyat & Buyukluk ---
    ipo_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), comment="Halka arz fiyati (TL)")
    total_lots: Mapped[int | None] = mapped_column(BigInteger, comment="Toplam pay miktari (lot)")
    offering_size_tl: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Arz buyuklugu (TL)")

    # --- Halka Arz Sekli ---
    capital_increase_lots: Mapped[int | None] = mapped_column(BigInteger, comment="Sermaye artirimi (lot)")
    partner_sale_lots: Mapped[int | None] = mapped_column(BigInteger, comment="Ortak satisi (lot)")

    # --- Tarihler ---
    subscription_start: Mapped[date | None] = mapped_column(Date, comment="Basvuru baslangic tarihi")
    subscription_end: Mapped[date | None] = mapped_column(Date, comment="Basvuru bitis tarihi")
    subscription_hours: Mapped[str | None] = mapped_column(String(30), comment="Basvuru saatleri, orn: 09:00-17:00")
    trading_start: Mapped[date | None] = mapped_column(Date, comment="Borsada islem baslangic tarihi")
    spk_approval_date: Mapped[date | None] = mapped_column(Date, comment="SPK onay tarihi")

    # --- Dagitim ---
    distribution_method: Mapped[str | None] = mapped_column(
        String(50), comment="Dagitim yontemi: esit, oransal, karma"
    )
    public_float_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), comment="Halka aciklik orani (%)")
    discount_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), comment="Iskonto orani (%)")

    # --- Pazar & Araci ---
    market_segment: Mapped[str | None] = mapped_column(
        String(30), comment="yildiz_pazar, ana_pazar, alt_pazar"
    )
    lead_broker: Mapped[str | None] = mapped_column(Text, comment="Konsorsiyum lideri araci kurum")

    # --- Ek Bilgiler ---
    lock_up_period_days: Mapped[int | None] = mapped_column(Integer, comment="Satmama taahhut suresi (gun)")
    price_stability_days: Mapped[int | None] = mapped_column(Integer, comment="Fiyat istikrari suresi (gun)")
    min_application_lot: Mapped[int | None] = mapped_column(Integer, default=1, comment="Minimum basvuru lotu")

    # --- Sirket Hakkinda ---
    company_description: Mapped[str | None] = mapped_column(Text, comment="Sirket tanitim metni")
    sector: Mapped[str | None] = mapped_column(String(100), comment="Sektor")
    fund_usage: Mapped[str | None] = mapped_column(Text, comment="Fon kullanim hedefleri (JSON)")

    # --- Mali Veriler ---
    revenue_current_year: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Guncel yil hasilat")
    revenue_previous_year: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Onceki yil hasilat")
    gross_profit: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Brut kar")

    # --- KAP / SPK Referans ---
    kap_notification_url: Mapped[str | None] = mapped_column(Text, comment="KAP bildirim linki")
    prospectus_url: Mapped[str | None] = mapped_column(Text, comment="Izahname PDF linki")
    spk_bulletin_url: Mapped[str | None] = mapped_column(Text, comment="SPK bulteni linki")

    # --- Tahsisat Sonuclari (completed sonrasi) ---
    allocation_announced: Mapped[bool] = mapped_column(Boolean, default=False)
    total_applicants: Mapped[int | None] = mapped_column(Integer, comment="Toplam basvuran sayisi")

    # --- Tavan Takip ---
    ceiling_tracking_active: Mapped[bool] = mapped_column(Boolean, default=False)
    first_day_close_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    ceiling_broken: Mapped[bool] = mapped_column(Boolean, default=False)
    ceiling_broken_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # --- Zaman damgalari ---
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # --- Iliskiler ---
    brokers: Mapped[list["IPOBroker"]] = relationship(back_populates="ipo", cascade="all, delete-orphan")
    allocations: Mapped[list["IPOAllocation"]] = relationship(back_populates="ipo", cascade="all, delete-orphan")
    ceiling_tracks: Mapped[list["IPOCeilingTrack"]] = relationship(back_populates="ipo", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_ipos_status", "status"),
        Index("idx_ipos_ticker", "ticker"),
        Index("idx_ipos_subscription_end", "subscription_end"),
    )


class IPOBroker(Base):
    """Halka arza basvuru yapilabilecek banka/araci kurumlar."""

    __tablename__ = "ipo_brokers"

    id: Mapped[int] = mapped_column(primary_key=True)
    ipo_id: Mapped[int] = mapped_column(ForeignKey("ipos.id", ondelete="CASCADE"))
    broker_name: Mapped[str] = mapped_column(Text, nullable=False)
    broker_type: Mapped[str | None] = mapped_column(
        String(20), comment="banka, araci_kurum"
    )
    application_url: Mapped[str | None] = mapped_column(Text)
    phone: Mapped[str | None] = mapped_column(String(30))

    ipo: Mapped["IPO"] = relationship(back_populates="brokers")


class IPOAllocation(Base):
    """Tahsisat dagilimi — bireysel, yuksek basvurulu, kurumsal yurt ici/disi."""

    __tablename__ = "ipo_allocations"

    id: Mapped[int] = mapped_column(primary_key=True)
    ipo_id: Mapped[int] = mapped_column(ForeignKey("ipos.id", ondelete="CASCADE"))
    group_name: Mapped[str] = mapped_column(
        String(50), nullable=False,
        comment="bireysel, yuksek_basvurulu, kurumsal_yurtici, kurumsal_yurtdisi"
    )
    allocation_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), comment="Tahsisat orani (%)")
    allocated_lots: Mapped[int | None] = mapped_column(BigInteger)
    participant_count: Mapped[int | None] = mapped_column(Integer, comment="Basvuran sayisi (sonuc)")
    avg_lot_per_person: Mapped[Decimal | None] = mapped_column(Numeric(10, 2), comment="Kisi basina ort lot (sonuc)")

    ipo: Mapped["IPO"] = relationship(back_populates="allocations")


class IPOCeilingTrack(Base):
    """Halka arz sonrasi tavan takibi — ilk 14 islem gunu."""

    __tablename__ = "ipo_ceiling_tracks"

    id: Mapped[int] = mapped_column(primary_key=True)
    ipo_id: Mapped[int] = mapped_column(ForeignKey("ipos.id", ondelete="CASCADE"))
    trading_day: Mapped[int] = mapped_column(Integer, comment="1-14 arasi islem gunu")
    trade_date: Mapped[date] = mapped_column(Date)
    close_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    hit_ceiling: Mapped[bool] = mapped_column(Boolean, default=True)
    ceiling_broken_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    relocked: Mapped[bool] = mapped_column(Boolean, default=False, comment="Tekrar kilitlendi mi")
    notified: Mapped[bool] = mapped_column(Boolean, default=False)

    ipo: Mapped["IPO"] = relationship(back_populates="ceiling_tracks")

    __table_args__ = (
        Index("idx_ceiling_ipo_day", "ipo_id", "trading_day", unique=True),
    )
