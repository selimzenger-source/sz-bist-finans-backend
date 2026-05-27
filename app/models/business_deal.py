"""Yeni İş Anlaşması / İş İlişkisi — KAP'tan AI parse + TRY çevrim.

KAP başlıkları:
  - Sözleşme imzalanması
  - İş anlaşması / İhale sonucu
  - Önemli nitelikteki işlem
  - Yeni müşteri kazanımı

AI parser body'den tutar + para birimi çıkarır.
TCMB kuru ile TRY'a çevrilir.
"""

from datetime import date, datetime
from sqlalchemy import String, Float, Date, DateTime, Index, Text, BigInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class BusinessDeal(Base):
    __tablename__ = "business_deals"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    company_name: Mapped[str | None] = mapped_column(String(255))

    title: Mapped[str | None] = mapped_column(String(500))
    summary: Mapped[str | None] = mapped_column(Text, comment="AI özet")

    # Tutar bilgisi
    amount_original: Mapped[float | None] = mapped_column(Float)
    currency: Mapped[str | None] = mapped_column(
        String(5),
        comment="TRY|USD|EUR|GBP"
    )
    amount_try: Mapped[float | None] = mapped_column(
        Float, index=True,
        comment="TRY'a çevrilmiş tutar (TCMB kuru ile)"
    )
    exchange_rate_used: Mapped[float | None] = mapped_column(Float)
    rate_date: Mapped[date | None] = mapped_column(Date)

    deal_date: Mapped[date | None] = mapped_column(Date, index=True)
    counterparty: Mapped[str | None] = mapped_column(
        String(500),
        comment="Müşteri/karşı taraf (varsa)"
    )

    kap_disclosure_id: Mapped[int | None] = mapped_column(BigInteger)
    kap_url: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="kap_ai_parse")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_bd_ticker_date", "ticker", "deal_date"),
        Index("idx_bd_amount_try", "amount_try"),
        UniqueConstraint("kap_url", "ticker", name="uq_business_deal_kap_ticker"),
    )
