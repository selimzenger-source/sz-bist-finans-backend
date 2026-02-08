"""KAP Haber modeli — AI tabanli keyword eslesmesi ile filtrelenmis haberler."""

from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Text, Integer, DateTime, Numeric, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class KapNews(Base):
    """KAP'tan gelen ve keyword eslesmesi ile filtrelenmis haberler."""

    __tablename__ = "kap_news"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, comment="Hisse kodu")
    kap_notification_id: Mapped[str | None] = mapped_column(String(20), comment="KAP bildirim ID")

    # Fiyat bilgisi
    price_at_time: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), comment="Haber anindaki fiyat"
    )

    # Haber detaylari
    news_title: Mapped[str | None] = mapped_column(Text, comment="KAP bildirim basligi")
    news_detail: Mapped[str | None] = mapped_column(Text, comment="Iliskilendirilen haber detayi")
    matched_keyword: Mapped[str | None] = mapped_column(Text, comment="Eslesen anahtar kelime")
    news_type: Mapped[str] = mapped_column(
        String(20), default="seans_ici",
        comment="seans_ici, seans_disi"
    )
    sentiment: Mapped[str] = mapped_column(
        String(10), default="positive",
        comment="positive, negative, neutral"
    )

    # KAP orijinal veri
    raw_text: Mapped[str | None] = mapped_column(Text, comment="KAP orijinal bildirim metni")
    kap_url: Mapped[str | None] = mapped_column(Text, comment="KAP bildirim linki")
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), comment="KAP yayinlanma zamani"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_news_ticker", "ticker"),
        Index("idx_news_created", "created_at"),
        Index("idx_news_sentiment", "sentiment"),
        Index("idx_news_type", "news_type"),
    )
