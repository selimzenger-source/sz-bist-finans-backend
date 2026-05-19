"""Uygulama konfigurasyonu."""

from datetime import date
from pydantic_settings import BaseSettings
from functools import lru_cache

# ── E.D.O (El Degistirme Orani) Sabitleri ──────────────────
# Bu tarihten itibaren isleme baslamis IPO'lar icin EDO hesaplanir.
# Tek yerde tanimlanir, tum backend bu sabiti kullanir.
EDO_START_DATE = date(2026, 3, 10)


class Settings(BaseSettings):
    """Uygulama ayarlari — .env dosyasindan veya ortam degiskenlerinden okunur."""

    # Veritabani — local: SQLite, production: PostgreSQL
    DATABASE_URL: str = "sqlite+aiosqlite:///./bist_finans.db"

    # App
    APP_ENV: str = "development"
    SECRET_KEY: str = ""
    # Production hostnames default'a eklendi — Render env tarafindan override edilebilir.
    CORS_ORIGINS: str = (
        "http://localhost:3000,http://localhost:8081,http://localhost:3001,"
        "https://borsacebimde.com,https://www.borsacebimde.com"
    )
    PORT: int = 8001

    # Firebase
    GOOGLE_APPLICATION_CREDENTIALS: str = "firebase-service-account.json"

    # Telegram Bot — sistem raporlari (eski notify bot)
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = "-1002704950091"

    # Telegram News Bot — haber gonderimleri (@sz_reply_notify_bot)
    TELEGRAM_NEWS_BOT_TOKEN: str = ""
    TELEGRAM_NEWS_CHAT_ID: str = ""

    # Telegram Okuyucu Bot — kanal mesajlarini okur (poller)
    # Sender bot kendi mesajlarini getUpdates'te goremez,
    # bu yuzden ayri bir okuyucu bot gerekli.
    # Bos ise TELEGRAM_BOT_TOKEN fallback olarak kullanilir.
    TELEGRAM_READER_BOT_TOKEN: str = ""

    # Admin paneli
    ADMIN_PASSWORD: str = ""

    # Admin Telegram bot — hata/durum bildirimleri icin
    ADMIN_TELEGRAM_BOT_TOKEN: str = ""
    ADMIN_TELEGRAM_CHAT_ID: str = ""

    # KAP pozitif bildirimleri icin ayri kanal (opsiyonel).
    # Set degilse ADMIN_TELEGRAM_* fallback olarak kullanilir.
    # Tipik kullanim: OPS hatalari grupta, KAP pozitif mesajlari ozel DM'de.
    KAP_POSITIVE_BOT_TOKEN: str = ""
    KAP_POSITIVE_CHAT_ID: str = ""

    # Uygulama sürüm yönetimi — frontend her açılışta sorar
    # Bu altındaki sürümler force update modal görür (kapatılamaz)
    IOS_MIN_REQUIRED_VERSION: str = "2.9.5"
    IOS_LATEST_VERSION: str = "3.0.0"
    ANDROID_MIN_REQUIRED_VERSION: str = "2.9.5"
    ANDROID_LATEST_VERSION: str = "3.0.0"
    APP_RELEASE_NOTES: str = "9 kategorili AI puan sistemi · Kişisel bildirim filtresi · Resmi tedbirli hisse listesi · Hata düzeltmeleri"

    # RevenueCat webhook dogrulama
    REVENUECAT_WEBHOOK_SECRET: str = ""

    # X (Twitter) API — @SZAlgoFinans otomatik tweet
    X_API_KEY: str = ""
    X_API_SECRET: str = ""
    X_ACCESS_TOKEN: str = ""
    X_ACCESS_TOKEN_SECRET: str = ""
    X_USER_ID: str = ""  # Kendi hesap user ID (begeni icin — bossa /users/me'den cekilir)

    # Tweet onay modu — False iken tweetler kuyruğa girer, admin onaylar
    # True yapilinca otomatik atilir (sistem oturunca)
    TWITTER_AUTO_SEND: bool = False

    # Abacus AI (RouteLLM) — scraper veri dogrulama + haber puanlama
    ABACUS_API_KEY: str = ""

    # Google Gemini API (yedek) — Abacus kredi bitince otomatik devreye girer
    # Ücretsiz: https://aistudio.google.com → Get API Key
    GEMINI_API_KEY: str = ""

    # OpenAI API — tavan/taban AI analizi (birincil)
    OPENAI_API_KEY: str = ""

    # Anthropic Claude API (direkt) — izahname + AI rapor analizi
    # https://console.anthropic.com → API Keys
    ANTHROPIC_API_KEY: str = ""

    # Tavily API — web arama (tavan/taban sebep analizi)
    TAVILY_API_KEY: str = ""

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
