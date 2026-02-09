"""Telegram kanal mesajlari — AI Haber Takibi icin.

Telegram Bot API uzerinden cekilen mesajlar burada depolanir.
3 mesaj tipi: seans_ici_pozitif, seans_ici_negatif, borsa_kapali, seans_disi_acilis.
"""

from datetime import date, datetime
from decimal import Decimal
from sqlalchemy import String, Text, Integer, BigInteger, Boolean, Date, DateTime, Numeric, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class TelegramNews(Base):
    """Telegram kanalından gelen haber mesajları."""

    __tablename__ = "telegram_news"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Telegram bilgileri
    telegram_message_id: Mapped[int] = mapped_column(
        BigInteger, unique=True, nullable=False,
        comment="Telegram mesaj ID (tekrari engellemek icin)"
    )
    chat_id: Mapped[str] = mapped_column(
        String(50), nullable=False,
        comment="Telegram chat/kanal ID"
    )

    # Mesaj tipi
    message_type: Mapped[str] = mapped_column(
        String(30), nullable=False,
        comment="seans_ici_pozitif, seans_ici_negatif, borsa_kapali, seans_disi_acilis"
    )

    # Hisse bilgisi
    ticker: Mapped[str | None] = mapped_column(String(10), comment="Hisse kodu, orn: DAPGM")
    price_at_time: Mapped[Decimal | None] = mapped_column(Numeric(12, 2), comment="Anlik fiyat")

    # Mesaj icerigi
    raw_text: Mapped[str] = mapped_column(Text, nullable=False, comment="Ham mesaj metni")
    parsed_title: Mapped[str | None] = mapped_column(Text, comment="Parse edilmis baslik")
    parsed_body: Mapped[str | None] = mapped_column(Text, comment="Parse edilmis icerik")

    # Sentiment
    sentiment: Mapped[str | None] = mapped_column(
        String(10), comment="positive, negative, neutral"
    )

    # Tip bazli ek bilgiler
    kap_notification_id: Mapped[str | None] = mapped_column(
        String(20), comment="KAP haber ID"
    )
    expected_trading_date: Mapped[date | None] = mapped_column(
        Date, comment="Beklenen islem gunu (borsa_kapali tipi icin)"
    )
    gap_pct: Mapped[Decimal | None] = mapped_column(
        Numeric(6, 2), comment="Acilis gap yuzde (seans_disi_acilis tipi icin)"
    )
    prev_close_price: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), comment="Onceki kapanis fiyati"
    )
    theoretical_open: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 2), comment="Teorik acilis fiyati"
    )

    # Zaman
    message_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), comment="Telegram mesaj zamani"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_telegram_news_type", "message_type"),
        Index("idx_telegram_news_ticker", "ticker"),
        Index("idx_telegram_news_created", "created_at"),
        Index("idx_telegram_news_msg_id", "telegram_message_id", unique=True),
    )
