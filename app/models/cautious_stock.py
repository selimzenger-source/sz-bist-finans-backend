"""Tedbirli Hisseler — Yapilandirilmis kayit.

Format:
  TICKER, COMPANY, PRICE, CHANGE%, START_DATE → END_DATE, [TAGS]

Tagler:
  KRD = Kredili
  ACS = Acigayan Satis
  BRT = Brut Takas
  EMR = Emir Iptali
  PEM = Piyasa Emri
  VEY = Veri Yayini
  TEK = Tek Fiyat
"""

from datetime import date, datetime
from sqlalchemy import String, Float, Date, DateTime, Index, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class CautiousStock(Base):
    __tablename__ = "cautious_stocks"

    # DUPLICATE ÖNLEME: ayni hisse + ayni tedbir donemi (start,end) tek satir olmali.
    # (NULL'lar Postgres'te distinct sayilir; bist_csv kayitlarinda tarihler dolu.)
    __table_args__ = (
        Index("ux_cautious_ticker_period", "ticker", "start_date", "end_date", unique=True),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    company_name: Mapped[str | None] = mapped_column(String(255))
    last_price: Mapped[float | None] = mapped_column(Float)
    pct_change: Mapped[float | None] = mapped_column(Float)
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date, index=True)
    # Etiketler — virgül ayrımlı
    tags: Mapped[str | None] = mapped_column(
        String(100),
        comment="Virgülle ayrılmış: KRD,ACS,BRT,EMR,PEM,VEY,TEK"
    )
    is_active: Mapped[bool] = mapped_column(
        default=True, index=True,
        comment="end_date >= bugün ise true"
    )
    kap_url: Mapped[str | None] = mapped_column(
        Text,
        comment="KAP bildirim linki (varsa)"
    )
    source: Mapped[str] = mapped_column(String(20), nullable=False, default="manual_import")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
