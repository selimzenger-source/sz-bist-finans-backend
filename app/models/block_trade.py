"""Toptan Alım Satım — Yapilandirilmis kayit.

Format:
  TICKER, COMPANY, DATE, ISLEM_TIPI (Alış/Satış), ARACI_KURUM,
  ALICILAR/SATICILAR (uzun liste), LOT_MIKTARI, MALIYET_FIYATI
"""

from datetime import date, datetime
from sqlalchemy import String, Float, Date, DateTime, Index, Text, BigInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class BlockTrade(Base):
    __tablename__ = "block_trades"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    company_name: Mapped[str | None] = mapped_column(String(255))
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    transaction_type: Mapped[str] = mapped_column(
        String(10), nullable=False,
        comment="alis | satis"
    )
    broker: Mapped[str | None] = mapped_column(
        String(255),
        comment="Aracı Kurum"
    )
    counterparties: Mapped[str | None] = mapped_column(
        Text,
        comment="Alıcılar veya Satıcılar listesi (virgülle ayrılmış)"
    )
    lot_amount: Mapped[int | None] = mapped_column(BigInteger)
    cost_price: Mapped[float | None] = mapped_column(Float)
    kap_url: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="kap_ai_parse")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_bt_ticker_date", "ticker", "transaction_date"),
        UniqueConstraint("kap_url", "ticker", name="uq_block_trade_kap_ticker"),
    )
