"""Uygulama konfigurasyonu."""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Uygulama ayarlari — .env dosyasindan veya ortam degiskenlerinden okunur."""

    # Veritabani — local: SQLite, production: PostgreSQL
    DATABASE_URL: str = "sqlite+aiosqlite:///./bist_finans.db"

    # App
    APP_ENV: str = "development"
    SECRET_KEY: str = ""
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:8081"
    PORT: int = 8001

    # Firebase
    GOOGLE_APPLICATION_CREDENTIALS: str = "firebase-service-account.json"

    # Telegram Bot
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = "-1002704950091"

    # Admin paneli
    ADMIN_PASSWORD: str = ""

    # Admin Telegram bot — hata/durum bildirimleri icin
    ADMIN_TELEGRAM_BOT_TOKEN: str = ""
    ADMIN_TELEGRAM_CHAT_ID: str = ""

    # Scraping intervals (saniye)
    KAP_SCRAPE_INTERVAL_SECONDS: int = 1800   # 30 dakika — halka arz
    NEWS_SCRAPE_INTERVAL_SECONDS: int = 30     # 30 saniye — KAP haberler

    @property
    def cors_origins_list(self) -> list[str]:
        origins = [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]
        if "*" in origins:
            return ["*"]
        return origins

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"

    @property
    def database_url_async(self) -> str:
        """Render PostgreSQL URL'sini asyncpg formatina cevirir.

        Render DATABASE_URL'si postgresql:// ile baslar,
        SQLAlchemy async icin postgresql+asyncpg:// gerekir.
        """
        url = self.DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://") and "+asyncpg" not in url:
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()
