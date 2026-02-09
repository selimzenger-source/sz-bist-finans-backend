"""Temettu (Dividend) veritabani modelleri.

Kaynak: BorsaDirekt / Osmanli Yatirim temettu beklentileri sayfasi.
Son 3 yil icerisinde minimum 2 yil temettu odeyen BIST Tum hisse senetleri.
"""

from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import (
    String, Text, Integer, Boolean, Date, DateTime,
    Numeric, ForeignKey, Index
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class Dividend(Base):
    """Temettu beklentileri ve gecmis temettu verileri.

    BorsaDirekt'ten cekilen veriler:
    - Hisse kodu (ticker)
    - 2027 Temettu Beklentisi (%)
    - Son Yil Temettu Verimi (%)
    - 2 Yillik Ortalama Temettu Verimi (%)
    - 3 Yillik Ortalama Temettu Verimi (%)
    - PD/DD (Piyasa Degeri / Defter Degeri)
    - F/K (Fiyat / Kazanc)
    - YBG Getiri (%) — Yilbasi getiri
    - Yillik Getiri (%)
    """

    __tablename__ = "dividends"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Hisse bilgisi
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, comment="Hisse kodu, orn: TUPRS")
    company_name: Mapped[str | None] = mapped_column(Text, comment="Sirket adi")

    # Temettu beklentisi
    expected_dividend_yield_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="Gelecek yil temettu beklentisi (%)"
    )
    expected_year: Mapped[int | None] = mapped_column(Integer, comment="Beklenti yili, orn: 2027")

    # Gecmis temettu verimleri
    last_year_yield_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="Son yil temettu verimi (%)"
    )
    avg_2y_yield_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="2 yillik ortalama temettu verimi (%)"
    )
    avg_3y_yield_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="3 yillik ortalama temettu verimi (%)"
    )

    # Degerlik metrikleri
    pd_dd: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="PD/DD — Piyasa Degeri / Defter Degeri"
    )
    fk: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="F/K — Fiyat / Kazanc"
    )

    # Getiri bilgileri
    ytd_return_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="Yilbasi getiri (%)"
    )
    yearly_return_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="Yillik getiri (%)"
    )

    # Kaynak
    source: Mapped[str | None] = mapped_column(
        String(50), default="borsadirekt", comment="Veri kaynagi"
    )

    # Zaman damgalari
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="Verinin cekildigi tarih"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_dividend_ticker", "ticker"),
        Index("idx_dividend_expected_yield", "expected_dividend_yield_pct"),
    )


class DividendHistory(Base):
    """Gecmis temettu odemeleri takvimi.

    Her hisse icin yil bazli temettu odemesi gecmisi.
    Ileride daha detayli veri eklenebilir (hisse basi temettu, odeme tarihi vs).
    """

    __tablename__ = "dividend_history"

    id: Mapped[int] = mapped_column(primary_key=True)

    ticker: Mapped[str] = mapped_column(String(10), nullable=False, comment="Hisse kodu")
    payment_year: Mapped[int] = mapped_column(Integer, comment="Temettu odeme yili")

    # Temettu detaylari
    gross_dividend_per_share: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), comment="Brut hisse basi temettu (TL)"
    )
    net_dividend_per_share: Mapped[Decimal | None] = mapped_column(
        Numeric(10, 4), comment="Net hisse basi temettu (TL)"
    )
    dividend_yield_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(8, 2), comment="Temettu verimi (%)"
    )

    # Tarihler
    ex_dividend_date: Mapped[date | None] = mapped_column(
        Date, comment="Temettu hakedis tarihi (ex-date)"
    )
    payment_date: Mapped[date | None] = mapped_column(
        Date, comment="Temettu odeme tarihi"
    )
    record_date: Mapped[date | None] = mapped_column(
        Date, comment="Kayit tarihi (record date)"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_divhist_ticker_year", "ticker", "payment_year"),
    )
