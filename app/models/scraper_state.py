"""Scraper state modeli — scraper'larin durumunu veritabaninda saklar.

Ornek kullanim:
- Son islenmis SPK bulten URL'leri
- Son scrape zamanlari
- Herhangi bir key-value cift
"""

from datetime import datetime

from sqlalchemy import Column, Integer, String, Text, DateTime

from app.database import Base


class ScraperState(Base):
    """Scraper durum bilgisi — key/value store."""

    __tablename__ = "scraper_state"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(255), unique=True, nullable=False, index=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<ScraperState(key={self.key}, value={self.value[:50] if self.value else None})>"
