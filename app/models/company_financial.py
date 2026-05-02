"""Sirket finansal verileri, oranlar, halka arz oylama ve AI asistan kullanim modelleri.

v3.0.0 — Bilanco analizi, finansal oranlar, IPO anket ve AI asistan.
"""

from datetime import datetime
from decimal import Decimal
from sqlalchemy import (
    String, Text, Integer, DateTime,
    Numeric, Index, UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class CompanyFinancial(Base):
    """Ceyreklik/yillik bilanco verileri.

    Kaynak: IS Yatirim / FINNET
    Periyot formati: "2024-Q3" (ceyrek) veya "2024-FY" (yillik)
    """

    __tablename__ = "company_financials"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Hisse & donem
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, comment="Hisse kodu, orn: TUPRS")
    period: Mapped[str] = mapped_column(String(10), nullable=False, comment="Donem, orn: 2024-Q3, 2024-FY")
    period_end_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), comment="Donem bitis tarihi")

    # Gelir tablosu
    revenue: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Ciro (TL)")
    gross_profit: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Brut Kar (TL)")
    operating_profit: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Faaliyet Kari (TL)")
    net_income: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Net Kar (TL)")
    ebitda: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="FAVOK (TL)")

    # Bilanco
    total_assets: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Toplam Aktif (TL)")
    total_equity: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Ozkaynaklar (TL)")
    total_debt: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Toplam Borc (TL)")
    net_debt: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Net Borc (TL)")
    cash_and_equivalents: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Nakit ve Benzerleri (TL)")

    # Oranlar
    current_ratio: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="Cari Oran")
    gross_margin_pct: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="Brut Kar Marji (%)")
    net_margin_pct: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="Net Kar Marji (%)")
    roe_pct: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="Ozkaynak Karliligi (%)")
    debt_to_equity: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="Borc/Ozkaynak")

    # Kaynak & zaman
    source: Mapped[str | None] = mapped_column(String(50), default="isyatirim", comment="Veri kaynagi")
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="Verinin cekildigi tarih"
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint("ticker", "period", name="uq_company_financial_ticker_period"),
        Index("idx_cf_ticker", "ticker"),
        Index("idx_cf_period", "period"),
    )


class FinancialRatio(Base):
    """Gunluk finansal oranlar (F/K, PD/DD, FD/FAVOK vb).

    Kaynak: IS Yatirim
    """

    __tablename__ = "financial_ratios"

    id: Mapped[int] = mapped_column(primary_key=True)

    ticker: Mapped[str] = mapped_column(String(10), nullable=False, comment="Hisse kodu")

    # Degerlik carpanlari
    fk: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="F/K (P/E)")
    pddd: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="PD/DD (P/B)")
    fd_favok: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="FD/FAVOK (EV/EBITDA)")
    piyasa_degeri: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), comment="Piyasa Degeri (TL)")

    # Sektor
    sector: Mapped[str | None] = mapped_column(String(100), comment="Sektor adi")
    sector_avg_fk: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="Sektor ortalama F/K")
    sector_avg_pddd: Mapped[Decimal | None] = mapped_column(Numeric(8, 2), comment="Sektor ortalama PD/DD")

    # Tarih & kaynak
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, comment="Veri tarihi")
    source: Mapped[str | None] = mapped_column(String(50), default="isyatirim", comment="Veri kaynagi")
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="Verinin cekildigi tarih"
    )

    __table_args__ = (
        UniqueConstraint("ticker", "date", name="uq_financial_ratio_ticker_date"),
        Index("idx_fr_ticker", "ticker"),
        Index("idx_fr_date", "date"),
        Index("idx_fr_sector", "sector"),
    )


class IPOVote(Base):
    """Halka arz katilim anketi.

    Kullanicilar bir halka arza katilip katilmayacaklarini oylar.
    App'te device_id, web'de ip_address uzerinden tekil oy kontrolu yapilir.
    """

    __tablename__ = "ipo_votes"

    id: Mapped[int] = mapped_column(primary_key=True)

    ipo_id: Mapped[int] = mapped_column(Integer, nullable=False, comment="Halka arz ID")
    device_id: Mapped[str | None] = mapped_column(String(100), comment="Uygulama oylari icin cihaz ID")
    ip_address: Mapped[str | None] = mapped_column(String(45), comment="Web oylari icin IP adresi")
    vote: Mapped[str] = mapped_column(String(20), nullable=False, comment="participate veya skip")
    source: Mapped[str | None] = mapped_column(String(10), default="app", comment="app veya web")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("ipo_id", "device_id", name="uq_ipo_vote_device"),
        UniqueConstraint("ipo_id", "ip_address", "source", name="uq_ipo_vote_ip_source"),
        Index("idx_ipovote_ipo_id", "ipo_id"),
    )


class AIAssistantUsage(Base):
    """AI asistan aylik kullanim limiti.

    Her cihaz icin aylik soru sorma sayisi takip edilir.
    """

    __tablename__ = "ai_assistant_usage"

    id: Mapped[int] = mapped_column(primary_key=True)

    device_id: Mapped[str] = mapped_column(String(100), nullable=False, comment="Cihaz ID")
    month: Mapped[str] = mapped_column(String(7), nullable=False, comment="Ay, orn: 2026-04")
    usage_count: Mapped[int] = mapped_column(Integer, default=0, comment="Aylik kullanim sayisi")
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), comment="Son kullanim zamani")

    __table_args__ = (
        UniqueConstraint("device_id", "month", name="uq_ai_usage_device_month"),
        Index("idx_ai_usage_device_id", "device_id"),
    )
