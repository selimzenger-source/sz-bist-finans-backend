"""
IPO Poll Vote — Halka Arz Anket Oyları

İki fazlı anket:
  - phase='hype'    → Faz 1: Katılım anketi (choice: participate|undecided|skip)
  - phase='ceiling' → Faz 2: Tavan sayısı tahmini (choice: '<int>' string olarak)

Tek oy kuralı (her kullanıcı her anket için 1 oy):
  - Mobil: device_id + ipo_id + phase UNIQUE
  - Web:   ip_address + ipo_id + phase UNIQUE

Not: device_id VEYA ip_address en az biri dolu olmalı.
"""

from datetime import datetime, timezone
from sqlalchemy import String, Integer, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class IPOPollVote(Base):
    __tablename__ = "ipo_poll_votes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ipo_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("ipos.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Faz — 'hype' veya 'ceiling'
    phase: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    # Oy değeri:
    #   hype    → 'participate' | 'undecided' | 'skip'
    #   ceiling → '1'..'30' (tavan sayısı, string olarak)
    choice: Mapped[str] = mapped_column(String(32), nullable=False)

    # Kullanıcı tanımlayıcı — mobil için device_id, web için ip_address
    device_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    ip_address: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        # device_id bazlı tekillik (mobil)
        UniqueConstraint("ipo_id", "phase", "device_id", name="uq_ipo_poll_device"),
        # ip_address bazlı tekillik (web)
        UniqueConstraint("ipo_id", "phase", "ip_address", name="uq_ipo_poll_ip"),
        Index("idx_ipo_poll_phase_ipo", "ipo_id", "phase"),
    )
