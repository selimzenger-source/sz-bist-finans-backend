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
