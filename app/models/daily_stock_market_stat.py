"""Daily Stock Market Stats (Tavan/Taban) tablosu."""

from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import String, Integer, Boolean, Date, DateTime, Numeric, Text, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class DailyStockMarketStat(Base):
    """Gunun Tavan/Taban hisse istatistikleri ve AI nedenleri."""

    __tablename__ = "daily_stock_market_stats"

    id: Mapped[int] = mapped_column(primary_key=True)

    ticker: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="Hisse kodu, orn: EGEGY"
    )
    date: Mapped[date] = mapped_column(
        Date, nullable=False, comment="Islem gunu tarihi"
    )
    close_price: Mapped[Decimal] = mapped_column(
        Numeric(12, 2), nullable=False, comment="Gun sonu kapanis fiyati"
    )

    is_ceiling: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Tavan (>= 9.75%) kapatti mi?"
    )
    is_floor: Mapped[bool] = mapped_column(
        Boolean, default=False, comment="Taban (<= -9.75%) kapatti mi?"
    )

    consecutive_ceiling_count: Mapped[int] = mapped_column(
        Integer, default=0, comment="Pes pese kac gundur tavan?"
    )
    monthly_ceiling_count: Mapped[int] = mapped_column(
        Integer, default=0, comment="Son 1 ayda (30 gun) kacinci tavani?"
    )
    
    consecutive_floor_count: Mapped[int] = mapped_column(
        Integer, default=0, comment="Pes pese kac gundur taban?"
    )
    monthly_floor_count: Mapped[int] = mapped_column(
        Integer, default=0, comment="Son 1 ayda (30 gun) kacinci tabani?"
    )

    reason: Mapped[str | None] = mapped_column(
        Text, comment="AI (Sonnet/Abacus) tarafindan uretilen kisa yukselis/dusus sebebi"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_daily_market_stat_date", "date"),
        Index("idx_daily_market_stat_ticker", "ticker"),
        Index("idx_daily_mk_stat_ticker_date", "ticker", "date", unique=True),
    )
