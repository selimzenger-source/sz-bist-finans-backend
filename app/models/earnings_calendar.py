"""Bilanco takvimi — hangi hisse hangi tarihte bilanco aciklayacak.

Kaynak: https://www.gcmyatirim.com.tr/arastirma-analiz/yurt-ici-bilanco-takvimi
"""

from datetime import date, datetime
from sqlalchemy import (
    String, Boolean, Date, DateTime, Index, UniqueConstraint
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class EarningsCalendar(Base):
    """Bilanco aciklama takvimi — beklenen ve aciklanan tarihler.

    Her (ticker, period) icin tek satir. Bilanco aciklandiginda
    is_announced=TRUE ve announced_date set edilir.
    """

    __tablename__ = "earnings_calendar"

    id: Mapped[int] = mapped_column(primary_key=True)

    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    company_name: Mapped[str | None] = mapped_column(String(200))
    period: Mapped[str] = mapped_column(String(10), nullable=False, comment="2025-Q4, 2026-Q1 ...")

    expected_date: Mapped[date | None] = mapped_column(Date, comment="GCM Yatirim'dan beklenen aciklama tarihi")
    announced_date: Mapped[date | None] = mapped_column(Date, comment="Gercek aciklama tarihi (KAP)")
    is_announced: Mapped[bool] = mapped_column(Boolean, default=False)

    source: Mapped[str | None] = mapped_column(String(50), default="gcmyatirim")
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint("ticker", "period", name="uq_earnings_calendar_ticker_period"),
        Index("idx_ec_ticker", "ticker"),
        Index("idx_ec_expected", "expected_date"),
        Index("idx_ec_period", "period"),
    )
