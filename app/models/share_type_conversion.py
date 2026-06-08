"""Borsada İşlem Gören Tipe Dönüşüm — Yapilandirilmis kayit.

Format (KAP/Ucretsizderinlikbot):
  TICKER, COMPANY, DATE, INVESTOR, CONVERTED_LOT
"""

from datetime import date, datetime
from sqlalchemy import String, Integer, Date, DateTime, Index, Text, BigInteger, UniqueConstraint, Float, Numeric
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

    # ── Kayıt tipi: 'gerceklesen' (borsada işlem görür hale geldi, kişi+lot) |
    #    'basvuru' (SPK'ya pay satış bilgi formu başvurusu — henüz olmadı, gelecek arz) ──
    record_type: Mapped[str] = mapped_column(String(20), nullable=False, default="gerceklesen", index=True)
    ratio_pct: Mapped[float | None] = mapped_column(Float)        # çıkarılmış sermayeye oran (%)
    nominal_tl: Mapped[float | None] = mapped_column(Numeric(20, 2))  # nominal tutar (TL)
    ai_summary: Mapped[str | None] = mapped_column(Text)          # başvuru için AI yorumu

    __table_args__ = (
        Index("idx_stc_ticker_date", "ticker", "transaction_date"),
        Index("idx_stc_record_type", "record_type", "transaction_date"),
        UniqueConstraint("kap_url", "ticker", "investor_name", name="uq_share_type_conv_kap_ticker_investor"),
    )
