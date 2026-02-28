"""Tum KAP Bildirimleri — BigPara/Bloomberg HT'den scrape edilen haberler.

Her bildirme Abacus AI (Claude Sonnet) ile sentiment analizi yapilir.
Frontend "Tum KAP Haberleri" tab'inda gosterilir.
"""

from datetime import datetime
from sqlalchemy import String, Text, Float, Boolean, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class KapAllDisclosure(Base):
    """Tum KAP bildirimleri (AI analiz dahil)."""

    __tablename__ = "kap_all_disclosures"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Hisse & bildirim bilgileri
    company_code: Mapped[str] = mapped_column(
        String(10), nullable=False,
        comment="Hisse kodu, orn: THYAO"
    )
    title: Mapped[str] = mapped_column(
        Text, nullable=False,
        comment="Bildirim basligi"
    )
    body: Mapped[str | None] = mapped_column(
        Text,
        comment="Tam bildirim metni (scrape edilen)"
    )
    category: Mapped[str | None] = mapped_column(
        String(100),
        comment="Kategori: Bilanco, Temettu, Ozel Durum, vb."
    )
    is_bilanco: Mapped[bool] = mapped_column(
        Boolean, default=False,
        comment="Bilanco/Finansal Rapor bildirimi mi (blur icin)"
    )
    kap_url: Mapped[str | None] = mapped_column(
        Text,
        comment="KAP bildirim linki (kap.org.tr/tr/Bildirim/XXX)"
    )
    source: Mapped[str | None] = mapped_column(
        String(20),
        comment="Kaynak: bigpara, bloomberght"
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        comment="Orijinal yayin zamani"
    )

    # AI analiz alanlari
    ai_sentiment: Mapped[str | None] = mapped_column(
        String(10),
        comment="Olumlu, Olumsuz, Notr"
    )
    ai_impact_score: Mapped[float | None] = mapped_column(
        Float,
        comment="AI etki puani (0.0-10.0)"
    )
    ai_summary: Mapped[str | None] = mapped_column(
        Text,
        comment="AI tarafindan uretilen Turkce ozet"
    )
    ai_analyzed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        comment="AI analiz zamani"
    )

    # Meta
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_kap_all_published", "published_at"),
        Index("idx_kap_all_company", "company_code"),
        Index("idx_kap_all_sentiment", "ai_sentiment"),
        Index("idx_kap_all_created", "created_at"),
    )
