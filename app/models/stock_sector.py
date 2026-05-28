"""Hisse sektör & endeks üyeliği — resmi BIST CSV'sinden beslenir.

Kaynak: https://borsaistanbul.com/datum/hisse_endeks_ds.csv
Günlük cron ile güncellenir.
"""

from datetime import datetime
from sqlalchemy import String, Text, Boolean, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class StockSector(Base):
    """Ticker → sektör adı + endeks üyelikleri (resmi BIST verisi)."""

    __tablename__ = "stock_sectors"

    ticker: Mapped[str] = mapped_column(String(12), primary_key=True, comment="Hisse kodu (örn: YUNSA)")
    company_name: Mapped[str | None] = mapped_column(String(120), comment="Şirket adı (bülten)")
    sector_name: Mapped[str | None] = mapped_column(String(80), comment="Detaylı sektör (örn: Tekstil, Deri)")
    sector_index: Mapped[str | None] = mapped_column(String(10), comment="Sektör endeks kodu (örn: XTEKS)")
    indices: Mapped[str | None] = mapped_column(Text, comment="Üye olduğu tüm endeks kodları (virgüllü)")
    in_bist30: Mapped[bool] = mapped_column(Boolean, default=False)
    in_bist50: Mapped[bool] = mapped_column(Boolean, default=False)
    in_bist100: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_stock_sectors_sector", "sector_name"),
    )
