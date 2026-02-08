"""Uygulama konfigurasyonu."""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Uygulama ayarlari — .env dosyasindan veya ortam degiskenlerinden okunur."""

    # PostgreSQL
    DATABASE_URL: str = "postgresql+asyncpg://localhost:5432/bist_finans"

    # App
    APP_ENV: str = "development"
    SECRET_KEY: str = "dev-secret-key"
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:8081"

    # Firebase
    GOOGLE_APPLICATION_CREDENTIALS: str = "firebase-service-account.json"

    # Scraping intervals (saniye)
    KAP_SCRAPE_INTERVAL_SECONDS: int = 1800   # 30 dakika — halka arz
    NEWS_SCRAPE_INTERVAL_SECONDS: int = 30     # 30 saniye — KAP haberler

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.CORS_ORIGINS.split(",") if o.strip()]

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()
