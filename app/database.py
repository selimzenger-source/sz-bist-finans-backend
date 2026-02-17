"""Veritabani baglantisi — SQLAlchemy async engine.

Local: SQLite (aiosqlite) — kurulum gerektirmez
Production: PostgreSQL (asyncpg)
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from app.config import get_settings

settings = get_settings()

# Async uyumlu URL (postgres:// → postgresql+asyncpg://)
db_url = settings.database_url_async
is_sqlite = db_url.startswith("sqlite")

engine_kwargs = {
    "echo": not settings.is_production,
}

if not is_sqlite:
    engine_kwargs["pool_size"] = 5
    engine_kwargs["max_overflow"] = 10
    engine_kwargs["pool_pre_ping"] = True  # Baglanti kopmasini onle
    engine_kwargs["pool_recycle"] = 300    # 5 dk'da bir recycle

engine = create_async_engine(db_url, **engine_kwargs)

async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    """SQLAlchemy ORM base class."""
    pass


async def get_db() -> AsyncSession:
    """FastAPI dependency — veritabani oturumu saglayici."""
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db():
    """Tablo olusturma + migration (yeni kolon ekleme)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # v2 migration: durum + pct_change kolonlari
        try:
            await conn.execute(
                text("ALTER TABLE ipo_ceiling_tracks ADD COLUMN IF NOT EXISTS durum VARCHAR(20) DEFAULT 'aktif'")
            )
            await conn.execute(
                text("ALTER TABLE ipo_ceiling_tracks ADD COLUMN IF NOT EXISTS pct_change NUMERIC(10,2)")
            )
        except Exception:
            pass  # Zaten varsa hata vermez (IF NOT EXISTS)

        # v3 migration: stock_notification_subscriptions.muted kolonu
        try:
            await conn.execute(
                text("ALTER TABLE stock_notification_subscriptions ADD COLUMN IF NOT EXISTS muted BOOLEAN DEFAULT FALSE")
            )
        except Exception:
            pass

        # v4 migration: custom_percentage kolonu + yuzde4/yuzde7 → yuzde_dusus birlestirme
        try:
            await conn.execute(
                text("ALTER TABLE stock_notification_subscriptions ADD COLUMN IF NOT EXISTS custom_percentage INTEGER")
            )
            # yuzde4_dusus → yuzde_dusus (tek hizmet)
            await conn.execute(
                text("""
                    UPDATE stock_notification_subscriptions
                    SET notification_type = 'yuzde_dusus'
                    WHERE notification_type IN ('yuzde4_dusus', 'yuzde7_dusus')
                """)
            )
        except Exception:
            pass

        # v5 migration: users.expo_push_token kolonu
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS expo_push_token VARCHAR(255)")
            )
        except Exception:
            pass

        # v6 migration: users.notify_first_trading_day kolonu (ilk islem gunu bildirimi)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_first_trading_day BOOLEAN DEFAULT TRUE")
            )
        except Exception:
            pass

        # v7 migration: users.notifications_enabled (master bildirim switch)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notifications_enabled BOOLEAN DEFAULT TRUE")
            )
        except Exception:
            pass

        # v8 migration: users.notify_kap_bist30 (BIST 30 KAP ucretsiz bildirim)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_kap_bist30 BOOLEAN DEFAULT TRUE")
            )
        except Exception:
            pass

        # v9 migration: users.notify_kap_all (ucretli aboneler icin tum KAP bildirimi)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_kap_all BOOLEAN DEFAULT TRUE")
            )
        except Exception:
            pass

        # v10 migration: Halka Arz ucretli bildirim tercihleri
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_taban_break BOOLEAN DEFAULT TRUE")
            )
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_daily_open_close BOOLEAN DEFAULT TRUE")
            )
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_percent_drop BOOLEAN DEFAULT TRUE")
            )
        except Exception:
            pass

        # v12 migration: users.deleted + deleted_at (Google Play hesap silme zorunlulugu)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS deleted BOOLEAN DEFAULT FALSE")
            )
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ")
            )
        except Exception:
            pass

        # v13 migration: ipos.company_name'deki \n karakterlerini temizle
        # SPK bultenden gelen sirket isimlerinde \n olabiliyor (tweet'lerde bozuk gorunuyor)
        try:
            await conn.execute(
                text("UPDATE ipos SET company_name = REPLACE(REPLACE(company_name, E'\\n', ' '), E'\\r', ' ') WHERE company_name LIKE E'%\\n%' OR company_name LIKE E'%\\r%'")
            )
        except Exception:
            pass

        # v14 migration: ipos.manual_fields (admin koruma — scraper bu alanlari ezmez)
        try:
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS manual_fields TEXT")
            )
        except Exception:
            pass

        # v15 migration: ipo_brokers.is_rejected (basvurulamaz broker tespiti)
        try:
            await conn.execute(
                text("ALTER TABLE ipo_brokers ADD COLUMN IF NOT EXISTS is_rejected BOOLEAN DEFAULT FALSE")
            )
        except Exception:
            pass

        # v16 migration: ipos.intro_tweeted (sirket tanitim tweeti atildi mi — duplicate koruma)
        try:
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS intro_tweeted BOOLEAN DEFAULT FALSE")
            )
        except Exception:
            pass

        # v17 migration: users cuzdan alanlari (sunucu tarafli puan sistemi)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS wallet_balance FLOAT DEFAULT 0.0")
            )
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS daily_ads_watched INTEGER DEFAULT 0")
            )
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_ad_watched_at TIMESTAMPTZ")
            )
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS ads_reset_date VARCHAR(20)")
            )
        except Exception:
            pass

        # v18 migration: ipos.result_bireysel_kisi + result_bireysel_lot (dagitim sonuclari)
        try:
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS result_bireysel_kisi INTEGER")
            )
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS result_bireysel_lot BIGINT")
            )
        except Exception:
            pass

        # v19 migration: ipo_ceiling_tracks.alis_lot + satis_lot (1. kademe lot verileri — ogle arasi tweet)
        try:
            await conn.execute(
                text("ALTER TABLE ipo_ceiling_tracks ADD COLUMN IF NOT EXISTS alis_lot INTEGER")
            )
            await conn.execute(
                text("ALTER TABLE ipo_ceiling_tracks ADD COLUMN IF NOT EXISTS satis_lot INTEGER")
            )
        except Exception:
            pass

        # v11 migration: telegram_news.message_date saat +3 hatasini duzelt
        # Eski kayitlar TZ_TR ile kaydedilmisti, UTC olmasi lazimdi.
        # Sadece 1 kez calisir: tz_fix_applied kolonu yoksa calistir, sonra kolonu ekle.
        try:
            # Marker kolon var mi kontrol et
            check = await conn.execute(
                text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'telegram_news' AND column_name = 'tz_fix_applied'
                """)
            )
            if not check.fetchone():
                # Tum kayitlarda 3 saat geri al (UTC+3 → UTC)
                await conn.execute(
                    text("UPDATE telegram_news SET message_date = message_date - INTERVAL '3 hours' WHERE message_date IS NOT NULL")
                )
                # Marker kolon ekle — tekrar calismasini engeller
                await conn.execute(
                    text("ALTER TABLE telegram_news ADD COLUMN IF NOT EXISTS tz_fix_applied BOOLEAN DEFAULT TRUE")
                )
        except Exception:
            pass
