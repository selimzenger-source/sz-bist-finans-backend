"""Temettü Takvimi — KAP-driven state machine.

Akis (state machine):
  YKK alindi (yonetim kurulu temettu karari KAP'ta yayinlandi)
    → genel_kurul_onayli (genel kurul kabul etti)
      → tarih_belli (dagitim/odeme tarihi ilan edildi)
        → odeniyor (bugun temettu odeniyor)
          → tamamlandi (gecmis tarih)
    → reddedildi (genel kurul red veya iptal)

Frontend (3 sekme — net/brut/yaklasan):
- Net: net temettu odeyenler
- Brüt: brut temettu odeyenler
- Yaklaşan: tarihi yaklasanlar (odeme tarihi >= bugun)
"""

from datetime import date, datetime
from sqlalchemy import String, Float, Date, DateTime, Index, UniqueConstraint, BigInteger, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class DividendCalendar(Base):
    """Temettu takvimi — KAP'tan beslenen state machine."""

    __tablename__ = "dividend_calendar"

    id: Mapped[int] = mapped_column(primary_key=True)

    # ─── Hisse bilgisi ───
    ticker: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="Hisse kodu"
    )
    company_name: Mapped[str | None] = mapped_column(
        String(255), comment="Sirket tam adi"
    )

    # ─── Yapilandirilmis veriler (KAP body'sinden AI parse) ───
    period: Mapped[str | None] = mapped_column(
        String(20),
        comment="Donem: 2025-Q1 / 2025-Q2 / 2025-Q3 / 2025-Q4 / 2025"
    )
    gross_amount_per_share: Mapped[float | None] = mapped_column(
        Float, comment="Hisse basi brut temettu (TL)"
    )
    net_amount_per_share: Mapped[float | None] = mapped_column(
        Float, comment="Hisse basi net temettu (TL)"
    )
    gross_yield_pct: Mapped[float | None] = mapped_column(
        Float, comment="Brut temettu verim yuzdesi"
    )
    net_yield_pct: Mapped[float | None] = mapped_column(
        Float, comment="Net temettu verim yuzdesi"
    )
    total_amount_tl: Mapped[float | None] = mapped_column(
        Float, comment="Toplam dagitilacak temettu tutari (TL)"
    )

    # ─── Dağıtım Türü (v56) ───
    # cash | stock | cash_and_stock | none (none = dağıtmama kararı veya henüz belirsiz)
    payment_type: Mapped[str | None] = mapped_column(
        String(20), comment="Dağıtım tipi: cash | stock | cash_and_stock | none"
    )
    # Bedelsiz pay oranı serbest metin — örn. \"1 lota 2 lot\" veya \"%200\"
    stock_ratio_text: Mapped[str | None] = mapped_column(
        String(80), comment="Bedelsiz pay dağıtım oranı (serbest metin)"
    )
    # Orijinal KAP \"Özet Bilgi\" başlığı (her zaman bizim generic label'imizden daha
    # spesifik — örn. \"Kar Payı Dağıtımına İlişkin Genel Kurul Kararı\")
    source_title: Mapped[str | None] = mapped_column(
        String(255), comment="Orijinal KAP Özet Bilgi başlığı"
    )

    # ─── State machine kilometre taslari ───
    # 1. YKK (Yonetim Kurulu Karari)
    ykk_date: Mapped[date | None] = mapped_column(Date)
    ykk_kap_disclosure_id: Mapped[int | None] = mapped_column(BigInteger)
    ykk_kap_url: Mapped[str | None] = mapped_column(Text)

    # 2. Genel Kurul Onayi
    general_assembly_date: Mapped[date | None] = mapped_column(Date)
    general_assembly_kap_disclosure_id: Mapped[int | None] = mapped_column(BigInteger)
    general_assembly_kap_url: Mapped[str | None] = mapped_column(Text)

    # 3. Odeme tarihi (dagitim baslangici)
    payment_date: Mapped[date | None] = mapped_column(
        Date, comment="Hak kullanim/odeme tarihi"
    )
    payment_kap_disclosure_id: Mapped[int | None] = mapped_column(BigInteger)
    payment_kap_url: Mapped[str | None] = mapped_column(Text)

    # 4. Reddedildi (terminal)
    rejected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rejection_kap_disclosure_id: Mapped[int | None] = mapped_column(BigInteger)
    rejection_kap_url: Mapped[str | None] = mapped_column(Text)

    # ─── Status (turetilmis ama hizli filtre icin saklanir) ───
    status: Mapped[str] = mapped_column(
        String(24), nullable=False, default="ykk_alindi", index=True,
        comment=(
            "ykk_alindi | genel_kurul_onayli | tarih_belli | "
            "odeniyor | tamamlandi | reddedildi"
        )
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        UniqueConstraint("ticker", "period", "ykk_date", name="uq_div_cal_ticker_period_ykk"),
        Index("idx_div_cal_ticker", "ticker"),
        Index("idx_div_cal_status", "status"),
        Index("idx_div_cal_payment_date", "payment_date"),
    )
