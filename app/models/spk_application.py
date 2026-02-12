"""SPK Basvuru Modeli — SPK onayi beklenen halka arz basvurulari.

Kaynak: https://spk.gov.tr/istatistikler/basvurular/ilk-halka-arz-basvurusu
Frontend Bolum 1: "SPK Onayi Beklenen"

Scraper tarafindan periyodik olarak guncellenir.
"""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import String, Text, Numeric, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class SPKApplication(Base):
    """SPK onayi beklenen halka arz basvurulari.

    spk.gov.tr/istatistikler/basvurular/ilk-halka-arz-basvurusu tablosundan cekilir.
    """

    __tablename__ = "spk_applications"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Ortaklik bilgileri
    company_name: Mapped[str] = mapped_column(Text, nullable=False, comment="Ortaklik adi")

    # Sermaye bilgileri (TL)
    existing_capital: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2), comment="Mevcut Sermaye (TL)"
    )
    new_capital: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2), comment="Yeni Sermaye (TL)"
    )

    # Sermaye artirimi
    capital_increase_paid: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2), comment="Sermaye Artirimi - Bedelli (TL)"
    )
    capital_increase_free: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2), comment="Sermaye Artirimi - Bedelsiz (TL)"
    )

    # Pay satisi
    existing_share_sale: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2), comment="Mevcut Ortak Pay Satisi (TL)"
    )
    additional_share_sale: Mapped[Decimal | None] = mapped_column(
        Numeric(18, 2), comment="Ek Pay Satisi (TL)"
    )

    # Satis fiyati
    sale_price: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), comment="Satis fiyati (TL)"
    )

    # Basvuru tarihi
    application_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), comment="SPK basvuru tarihi"
    )

    # Dipnotlar
    notes: Mapped[str | None] = mapped_column(Text, comment="Dipnotlar / aciklamalar")

    # Durum
    status: Mapped[str] = mapped_column(
        String(20), default="pending",
        comment="pending: onay bekliyor, approved: onaylandi, rejected: reddedildi, deleted: admin sildi (tekrar eklenmez)"
    )

    # Zaman damgasi
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_spk_app_status", "status"),
        Index("idx_spk_app_company", "company_name"),
    )
