"""Temel Analiz — manuel Excel ile beslenen ek finansal veriler.

Yerelde Python scripti ile Excel okunur, /admin/import-temel-analiz endpoint'i ile
Render'a 2 saatte bir gonderilir.
"""

from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Numeric, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class TemelAnaliz(Base):
    __tablename__ = "temel_analiz"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, unique=True, comment="Hisse kodu")
    sektor: Mapped[str | None] = mapped_column(String(100), comment="Sektor")
    dolasim_lot: Mapped[Decimal | None] = mapped_column(Numeric(18, 0), comment="Dolasimdaki senet sayisi (lot)")
    ozsermaye: Mapped[Decimal | None] = mapped_column(Numeric(20, 2), comment="Ozsermaye (TL)")
    yat_fon_oran: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), comment="Yatirim fonu oran (%)")
    emeklilik_fon_oran: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), comment="Emeklilik fonu oran (%)")
    piyasa_degeri: Mapped[Decimal | None] = mapped_column(Numeric(20, 2), comment="Piyasa degeri (TL)")
    defter_degeri: Mapped[Decimal | None] = mapped_column(Numeric(15, 4), comment="Defter degeri")
    fk: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), comment="F/K (P/E)")
    pddd: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), comment="PD/DD (P/B)")
    fd_favok: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), comment="FD/FAVOK (EV/EBITDA)")
    pd_efk: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), comment="PD/EFK")
    ihracat_yuzdesi: Mapped[Decimal | None] = mapped_column(Numeric(6, 2), comment="Ihracat yuzdesi (%)")

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    source: Mapped[str | None] = mapped_column(String(30), default="excel_sync", comment="Kaynak")

    __table_args__ = (Index("idx_temel_ticker", "ticker"),)
