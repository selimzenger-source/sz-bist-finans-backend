"""AppSetting modeli — key/value ayarlar tablosu.

Admin panelden düzenlenebilir sabitler (APP_LINK, SLOGAN, DISCLAIMER vb.)
twitter_service.py ve diğer servisler bu tablodan okur.
"""

from datetime import datetime
from sqlalchemy import String, Text, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class AppSetting(Base):
    """Uygulama geneli key-value ayarlar tablosu."""

    __tablename__ = "app_settings"

    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(
        String(100), unique=True, nullable=False, index=True,
        comment="Ayar anahtarı: APP_LINK, SLOGAN, DISCLAIMER, vb."
    )
    value: Mapped[str] = mapped_column(
        Text, nullable=False, default="",
        comment="Ayar değeri"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
