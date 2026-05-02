"""Borsada İşlem Gören Tipe Dönüşüm — Yapilandirilmis kayit.

Format (KAP/Ucretsizderinlikbot):
  TICKER, COMPANY, DATE, INVESTOR, CONVERTED_LOT
"""

from datetime import date, datetime
from sqlalchemy import String, Integer, Date, DateTime, Index, Text, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class ShareTypeConversion(Base):
    __tablename__ = "share_type_conversions"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    company_name: Mapped[str | None] = mapped_column(String(255))
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    investor_name: Mapped[str] = mapped_column(String(255), nullable=False)
    converted_lot: Mapped[int | None] = mapped_column(BigInteger)
    kap_url: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="kap_ai_parse")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_stc_ticker_date", "ticker", "transaction_date"),
    )
