"""Veritabani baglantisi — SQLAlchemy async engine.

Local: SQLite (aiosqlite) — kurulum gerektirmez
Production: PostgreSQL (asyncpg)
"""

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
    """Tablo olusturma (gelistirme ortami icin)."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
