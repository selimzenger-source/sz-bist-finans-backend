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
        String(30), default="newly_approved",
        comment="newly_approved, in_distribution, awaiting_trading, trading, archived (eski: upcoming, active, completed)"
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
    expected_trading_date: Mapped[date | None] = mapped_column(Date, comment="Beklenen islem baslangic tarihi")

    # --- SPK Referans ---
    spk_bulletin_no: Mapped[str | None] = mapped_column(String(50), comment="SPK bulteni numarasi, orn: 2026/5")

    # --- Dagitim Durumu ---
    distribution_completed: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Dagitim tamamlandi mi"
    )

    # --- Dagitim & Katilim ---
    distribution_method: Mapped[str | None] = mapped_column(
        String(50), comment="Dagitim yontemi kodu: esit, bireysele_esit, tamami_esit, oransal, karma"
    )
    distribution_description: Mapped[str | None] = mapped_column(
        Text, comment="Dagitim yontemi aciklama: Kucuk yatirimciya anlasilir dilde aciklama"
    )
    participation_method: Mapped[str | None] = mapped_column(
        String(30), comment="Katilim yontemi: talep_toplama, borsada_satis"
    )
    participation_description: Mapped[str | None] = mapped_column(
        Text, comment="Katilim yontemi aciklama: Kullaniciya anlasilir dilde nasil basvurulur"
    )
    public_float_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), comment="Halka aciklik orani (%)")
    discount_pct: Mapped[Decimal | None] = mapped_column(Numeric(5, 2), comment="Iskonto orani (%)")

    # --- Pazar & Araci ---
    market_segment: Mapped[str | None] = mapped_column(
        String(30), comment="yildiz_pazar, ana_pazar, alt_pazar"
    )
    lead_broker: Mapped[str | None] = mapped_column(Text, comment="Konsorsiyum lideri araci kurum")

    # --- Tahmini Lot ---
    estimated_lots_per_person: Mapped[int | None] = mapped_column(
        Integer, comment="Tahmini kisi basi lot (500K katilimci varsayimi, Gedik kaynakli)"
    )

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

    # --- Arsiv (25 is gunu sonrasi) ---
    archived: Mapped[bool] = mapped_column(Boolean, default=False, comment="25 is gunu gecti mi")
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), comment="Arsiv tarihi")
    trading_day_count: Mapped[int] = mapped_column(Integer, default=0, comment="Mevcut islem gunu sayaci")
    high_from_start: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), comment="Islem basindan bu yana en yuksek fiyat (%4 dusus hesabi icin)"
    )

    # --- Admin Koruma ---
    manual_fields: Mapped[str | None] = mapped_column(
        Text, comment="Admin tarafindan elle duzenlenen alanlar (JSON list). Bot bu alanlari ezmez."
    )

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
        String(20), comment="banka, araci_kurum, konsorsiyum"
    )
    is_rejected: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="True = basvurulamaz (ustu cizili)"
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
    """Halka arz sonrasi tavan/taban takibi — ilk 20 islem gunu.

    Matriks Excel pipeline'indan gelen veriyle guncellenir.
    Tavan bozuldu, tekrar tavana kitlendi, tabana kitlendi bildirimleri gonderir.
    """

    __tablename__ = "ipo_ceiling_tracks"

    id: Mapped[int] = mapped_column(primary_key=True)
    ipo_id: Mapped[int] = mapped_column(ForeignKey("ipos.id", ondelete="CASCADE"))
    trading_day: Mapped[int] = mapped_column(Integer, comment="1-20 arasi islem gunu")
    trade_date: Mapped[date] = mapped_column(Date)
    open_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), comment="Acilis fiyati")
    close_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), comment="Kapanis fiyati")
    high_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), comment="Gun ici en yuksek")
    low_price: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), comment="Gun ici en dusuk")
    hit_ceiling: Mapped[bool] = mapped_column(Boolean, default=False, comment="Tavan yaptı mi")
    hit_floor: Mapped[bool] = mapped_column(Boolean, default=False, comment="Taban yapti mi")
    ceiling_broken_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), comment="Tavan bozulma zamani")
    floor_hit_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), comment="Tabana kitlenme zamani")
    relocked: Mapped[bool] = mapped_column(Boolean, default=False, comment="Tekrar tavana kitledi mi")
    relocked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    notified_ceiling_break: Mapped[bool] = mapped_column(Boolean, default=False)
    notified_relock: Mapped[bool] = mapped_column(Boolean, default=False)
    notified_floor: Mapped[bool] = mapped_column(Boolean, default=False)

    # v4: 5 durum + gunluk % degisim
    durum: Mapped[str] = mapped_column(
        String(20), default="alici_kapatti",
        comment="tavan, alici_kapatti, not_kapatti, satici_kapatti, taban",
    )
    pct_change: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 2), nullable=True,
        comment="Gunluk % degisim (onceki gun kapanisina gore)",
    )

    ipo: Mapped["IPO"] = relationship(back_populates="ceiling_tracks")

    __table_args__ = (
        Index("idx_ceiling_ipo_day", "ipo_id", "trading_day", unique=True),
    )
