"""Kurum Onerileri — Araci kurum hedef fiyat ve hisse tavsiyeleri.

hedeffiyat.com.tr'den scrape edilen veriler.
Frontend "Kurum Onerileri" sayfasinda gosterilir.
"""

from datetime import datetime, date
from decimal import Decimal
from sqlalchemy import String, Text, Numeric, Date, DateTime, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class KurumOneri(Base):
    """Araci kurum hedef fiyat onerileri."""

    __tablename__ = "kurum_onerileri"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Hisse bilgileri
    ticker: Mapped[str] = mapped_column(
        String(10), nullable=False,
        comment="BIST kodu, orn: THYAO"
    )
    company_name: Mapped[str | None] = mapped_column(
        Text,
        comment="Sirket adi, orn: Turk Hava Yollari A.O."
    )

    # Kurum & oneri bilgileri
    institution_name: Mapped[str] = mapped_column(
        Text, nullable=False,
        comment="Araci kurum adi, orn: Is Yatirim, Tera Yatirim"
    )
    recommendation: Mapped[str | None] = mapped_column(
        String(30),
        comment="Oneri: Al, Tut, Sat, Endeks Ustu Getiri, Notr vb."
    )
    target_price: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2),
        comment="Hedef fiyat (TL)"
    )

    # Tarih
    report_date: Mapped[date] = mapped_column(
        Date, nullable=False,
        comment="Rapor/oneri tarihi"
    )

    # Kaynak
    source_url: Mapped[str | None] = mapped_column(
        Text,
        comment="Rapor kaynak linki (varsa)"
    )

    # Meta
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_kurum_oneri_ticker", "ticker"),
        Index("idx_kurum_oneri_date", "report_date"),
        Index("idx_kurum_oneri_institution", "institution_name"),
        Index("idx_kurum_oneri_created", "created_at"),
        UniqueConstraint(
            "ticker", "institution_name", "report_date",
            name="uq_kurum_oneri_ticker_kurum_tarih"
        ),
    )

    def __repr__(self) -> str:
        return f"<KurumOneri {self.ticker} {self.institution_name} {self.report_date}>"
