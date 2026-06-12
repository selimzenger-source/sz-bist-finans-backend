"""Sermaye Artırımı Takvimi — KAP-driven state machine.

Akis (state machine):
  YKK alindi (yonetim kurulu karari KAP'ta yayinlandi)
    → spk_onayli (SPK onay verdi → tarih beklenir)
      → tarih_belli (dagitim tarihi ilan edildi)
        → dagitiliyor (bugun bolunuyor)
          → tamamlandi (tarih gectı)
    → reddedildi (SPK red → tarih beklemez)

Frontend'de 3 sekme: Bedelsiz | Bedelli | Tahsisli — bu `type` alanindan dagitilir.
"""

from datetime import date, datetime
from sqlalchemy import String, Text, Float, Date, DateTime, Index, UniqueConstraint, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class CapitalIncrease(Base):
    """Sermaye artirimi state machine — bedelsiz / bedelli / tahsisli."""

    __tablename__ = "capital_increases"

    id: Mapped[int] = mapped_column(primary_key=True)

    # ─── Hisse bilgisi ───
    ticker: Mapped[str] = mapped_column(
        String(10), nullable=False, comment="Hisse kodu (orn: ENTRA)"
    )
    company_name: Mapped[str | None] = mapped_column(
        String(255), comment="Sirket tam adi"
    )

    # ─── Tip: bedelsiz | bedelli | tahsisli ───
    type: Mapped[str] = mapped_column(
        String(20), nullable=False, default="bedelsiz",
        comment="bedelsiz | bedelli | tahsisli"
    )

    # ─── Yapilandirilmis veriler (KAP body'sinden AI ile parse) ───
    percentage: Mapped[float | None] = mapped_column(
        Float, comment="(deprecated — yerine 4 ayri pct alani)"
    )
    amount_tl: Mapped[float | None] = mapped_column(
        Float, comment="(deprecated — yerine bolunme_sonrasi_sermaye_tl)"
    )

    # ─── 3 oran tipi (ayni ticker'da bir veya birkaci dolu olabilir) ───
    bedelli_pct: Mapped[float | None] = mapped_column(
        Float, comment="Bedelli oran %"
    )
    bedelsiz_pct: Mapped[float | None] = mapped_column(
        Float, comment="Bedelsiz oran % (ic kaynak)"
    )
    tahsisli_pct: Mapped[float | None] = mapped_column(
        Float, comment="Tahsisli oran %"
    )
    bolunme_sonrasi_sermaye_tl: Mapped[float | None] = mapped_column(
        Float, comment="Bolunme sonrasi toplam sermaye (TL)"
    )

    # ─── State machine kilometre taslari ───
    # 1. YKK (Yonetim Kurulu Karari)
    ykk_date: Mapped[date | None] = mapped_column(
        Date, comment="YKK tarihi"
    )
    ykk_kap_disclosure_id: Mapped[int | None] = mapped_column(
        BigInteger, comment="kap_all_disclosures.id referansi"
    )
    ykk_kap_url: Mapped[str | None] = mapped_column(Text)

    # 2. SPK Onay
    spk_approval_date: Mapped[date | None] = mapped_column(
        Date, comment="SPK onay tarihi"
    )
    spk_approval_kap_disclosure_id: Mapped[int | None] = mapped_column(BigInteger)
    spk_approval_kap_url: Mapped[str | None] = mapped_column(Text)

    # 3. Dagitim Tarihi (Tarih)
    distribution_date: Mapped[date | None] = mapped_column(
        Date, comment="Pay dagitim/bolunme tarihi (ekran: Tarih)"
    )
    distribution_kap_disclosure_id: Mapped[int | None] = mapped_column(BigInteger)
    distribution_kap_url: Mapped[str | None] = mapped_column(Text)

    # 4. Reddedildi (terminal state)
    rejected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rejection_kap_disclosure_id: Mapped[int | None] = mapped_column(BigInteger)
    rejection_kap_url: Mapped[str | None] = mapped_column(Text)

    # ─── Status (turetilmis ama hizli filtre icin saklanir) ───
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="ykk_alindi", index=True,
        comment=(
            "ykk_alindi | spk_onayli | tarih_belli | dagitiliyor | "
            "tamamlandi | reddedildi"
        )
    )

    # ─── Meta ───
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    # Halkarz'da en son ne zaman LISTELENDIGI. Her scrape'te aktif kayitlar
    # damgalanir; bu damga eskirse/yoksa kayit halkarz'dan dusmus (tamamlandi/
    # iptal/red) demektir -> bekleyen listeden cikarilir.
    last_seen_on_source: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        # Ayni hisse + tip + YKK tarihi -> tek kayit (ayni surec)
        UniqueConstraint("ticker", "type", "ykk_date", name="uq_cap_inc_ticker_type_ykk"),
        Index("idx_cap_inc_ticker", "ticker"),
        Index("idx_cap_inc_type", "type"),
        Index("idx_cap_inc_status", "status"),
        Index("idx_cap_inc_distribution_date", "distribution_date"),
    )
