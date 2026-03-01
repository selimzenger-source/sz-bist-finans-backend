"""Kullanici KAP Hisse Takip Listesi.

Kullanicilar hisse kodu ekleyerek yeni KAP bildirimlerinde
push bildirim alabilir.
- Free: max 3 hisse
- VIP (ana_yildiz abonesi): sinirsiz
"""

from datetime import datetime
from sqlalchemy import String, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class UserWatchlist(Base):
    """Kullanici KAP hisse takip listesi."""

    __tablename__ = "user_watchlist"

    id: Mapped[int] = mapped_column(primary_key=True)

    device_id: Mapped[str] = mapped_column(
        String(100), nullable=False,
        comment="Kullanici cihaz ID (users.device_id FK)"
    )
    ticker: Mapped[str] = mapped_column(
        String(10), nullable=False,
        comment="Takip edilen hisse kodu, orn: THYAO"
    )
    notification_preference: Mapped[str] = mapped_column(
        String(20), default="both", nullable=False,
        comment="Bildirim tercihi: both, positive_only, negative_only"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_watchlist_device", "device_id"),
        Index("idx_watchlist_ticker", "ticker"),
        Index("idx_watchlist_unique", "device_id", "ticker", unique=True),
    )
