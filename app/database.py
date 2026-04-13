"""Veritabani baglantisi — SQLAlchemy async engine.

Local: SQLite (aiosqlite) — kurulum gerektirmez
Production: PostgreSQL (asyncpg)
"""

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Async uyumlu URL (postgres:// → postgresql+asyncpg://)
db_url = settings.database_url_async
is_sqlite = db_url.startswith("sqlite")

engine_kwargs = {
    "echo": not settings.is_production,
}

if not is_sqlite:
    engine_kwargs["pool_size"] = 10
    engine_kwargs["max_overflow"] = 20
    engine_kwargs["pool_pre_ping"] = True  # Baglanti kopmasini onle
    engine_kwargs["pool_recycle"] = 300    # 5 dk'da bir recycle
    engine_kwargs["pool_timeout"] = 60     # Baglanti bekleme suresi (default 30 → 60)

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
        # Güvenlik: hiçbir migration lock bekleyerek hang etmesin
        try:
            await conn.execute(text("SET lock_timeout = '5s'"))
            await conn.execute(text("SET statement_timeout = '30s'"))
        except Exception:
            pass  # SQLite'da bu komutlar yoktur

        # Zombie bağlantıları öldür — önceki deploy'dan kalan idle transaction'lar
        try:
            await conn.execute(text("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND pid <> pg_backend_pid()
                  AND state IN ('idle in transaction', 'idle in transaction (aborted)')
            """))
            logger.info("Zombie bağlantılar temizlendi")
        except Exception:
            pass  # SQLite'da veya yetki yoksa sessizce atla

        try:
            await conn.run_sync(Base.metadata.create_all)
        except Exception as e:
            logger.warning("create_all hatası (devam ediyor): %s", e)

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

        # v20 migration: ipos.katilim_endeksi (katilim endeksine uygunluk)
        try:
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS katilim_endeksi VARCHAR(20)")
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

        # v21 migration: stock_notification_subscriptions.muted_types (bundle tip bazli mute)
        try:
            await conn.execute(
                text("ALTER TABLE stock_notification_subscriptions ADD COLUMN IF NOT EXISTS muted_types TEXT")
            )
        except Exception:
            pass

        # v22 migration: users.last_daily_checkin (gunluk giris puani)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_daily_checkin VARCHAR(20)")
            )
        except Exception:
            pass

        # v23 migration: telegram_news AI puanlama alanlari
        try:
            await conn.execute(
                text("ALTER TABLE telegram_news ADD COLUMN IF NOT EXISTS ai_score INTEGER")
            )
            await conn.execute(
                text("ALTER TABLE telegram_news ADD COLUMN IF NOT EXISTS ai_summary TEXT")
            )
        except Exception:
            pass

        # v24 migration: telegram_news.kap_url (KAP bildirim linki)
        try:
            await conn.execute(
                text("ALTER TABLE telegram_news ADD COLUMN IF NOT EXISTS kap_url TEXT")
            )
        except Exception:
            pass

        # v25 migration: ai_score INTEGER → FLOAT (V4 ondalik puanlama: 8.7, 6.3 gibi)
        try:
            await conn.execute(
                text("ALTER TABLE telegram_news ALTER COLUMN ai_score TYPE FLOAT USING ai_score::float")
            )
        except Exception:
            pass

        # v26 migration: reply_targets.last_seen_tweet_id (eski tweetlere reply engeli)
        try:
            await conn.execute(
                text("ALTER TABLE reply_targets ADD COLUMN IF NOT EXISTS last_seen_tweet_id VARCHAR(30)")
            )
        except Exception:
            pass

        # v27 migration: KALDIRILDI — her deploy'da auto_replies silip last_seen_tweet_id
        # sıfırlıyordu, sistem sürekli resetleniyordu. Artık çalışmaz.

        # v28 migration: KALDIRILDI — her deploy'da seans_disi_acilis siliyordu.
        # Poller zaten bu kayitlari kaydetmiyor, migration gereksiz.

        # v30 migration: reply_targets.last_reply_at (hesap bazli saatlik rate limit)
        try:
            await conn.execute(
                text("ALTER TABLE reply_targets ADD COLUMN IF NOT EXISTS last_reply_at TIMESTAMPTZ")
            )
        except Exception:
            pass

        # v29 migration: KALDIRILDI — her deploy'da ai_score < 6 siliyordu.
        # Poller zaten bu kayitlari kaydetmiyor, migration gereksiz.

        # v31 migration: IPO AI rapor alanlari
        try:
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS ai_report TEXT")
            )
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS ai_report_generated_at TIMESTAMPTZ")
            )
        except Exception:
            pass

        # v32 migration: FK ondelete CASCADE → SET NULL (IPO silinince abonelikler korunsun)
        # stock_notification_subscriptions.ipo_id: CASCADE → SET NULL
        # ceiling_track_subscriptions.ipo_id: CASCADE → SET NULL
        try:
            # stock_notification_subscriptions
            await conn.execute(text("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'stock_notification_subscriptions'
                        AND constraint_type = 'FOREIGN KEY'
                        AND constraint_name LIKE '%ipo_id%'
                    ) THEN
                        ALTER TABLE stock_notification_subscriptions
                            DROP CONSTRAINT IF EXISTS stock_notification_subscriptions_ipo_id_fkey;
                        ALTER TABLE stock_notification_subscriptions
                            ADD CONSTRAINT stock_notification_subscriptions_ipo_id_fkey
                            FOREIGN KEY (ipo_id) REFERENCES ipos(id) ON DELETE SET NULL;
                    END IF;
                END $$;
            """))
            # ceiling_track_subscriptions
            await conn.execute(text("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.table_constraints
                        WHERE table_name = 'ceiling_track_subscriptions'
                        AND constraint_type = 'FOREIGN KEY'
                        AND constraint_name LIKE '%ipo_id%'
                    ) THEN
                        ALTER TABLE ceiling_track_subscriptions
                            DROP CONSTRAINT IF EXISTS ceiling_track_subscriptions_ipo_id_fkey;
                        ALTER TABLE ceiling_track_subscriptions
                            ADD CONSTRAINT ceiling_track_subscriptions_ipo_id_fkey
                            FOREIGN KEY (ipo_id) REFERENCES ipos(id) ON DELETE SET NULL;
                    END IF;
                END $$;
            """))
        except Exception:
            pass

        # v33 migration: users.persistent_id (kalici cihaz ID — hesap kurtarma)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS persistent_id VARCHAR(255)")
            )
            # Unique constraint (aynı cihazdan birden fazla hesap olmasın)
            await conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes
                        WHERE tablename = 'users' AND indexname = 'idx_users_persistent_id'
                    ) THEN
                        CREATE UNIQUE INDEX idx_users_persistent_id ON users(persistent_id) WHERE persistent_id IS NOT NULL;
                    END IF;
                END $$;
            """))
        except Exception:
            pass

        # v35 migration: ipos.distribution_tweeted (deploy-safe dagitim tweet dedup)
        try:
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS distribution_tweeted BOOLEAN DEFAULT FALSE")
            )
        except Exception:
            pass

        # v34 migration: ipos izahname analiz alanlari (AI prospectus analysis)
        try:
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS prospectus_analysis TEXT")
            )
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS prospectus_analyzed_at TIMESTAMPTZ")
            )
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS prospectus_tweeted BOOLEAN DEFAULT FALSE")
            )
        except Exception:
            pass

        # v36 migration: KALDIRILDI — prospectus_image_base64 ORM'den çıkarıldı

        # v37 migration: DEVRE DIŞI — izahname analizleri admin panelden temizlenecek
        # Üretim ortamında sorun çıkardı, güvenli şekilde elle yapılacak.

        # v38 migration: spk_applications bildirim/tweet takip alanlari
        try:
            await conn.execute(
                text("ALTER TABLE spk_applications ADD COLUMN IF NOT EXISTS notified BOOLEAN DEFAULT FALSE")
            )
            await conn.execute(
                text("ALTER TABLE spk_applications ADD COLUMN IF NOT EXISTS tweeted BOOLEAN DEFAULT FALSE")
            )
        except Exception:
            pass

        # v39 migration: users.notify_kap_watchlist (Takip Listesi KAP bildirimi)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_kap_watchlist BOOLEAN DEFAULT TRUE")
            )
        except Exception:
            pass

        # v40 migration: kap_all_disclosures + user_watchlist tablolari
        try:
            await conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_indexes
                        WHERE tablename = 'kap_all_disclosures' AND indexname = 'idx_kap_all_dedup'
                    ) THEN
                        CREATE UNIQUE INDEX idx_kap_all_dedup
                        ON kap_all_disclosures(company_code, title, published_at);
                    END IF;
                END $$;
            """))
        except Exception:
            pass

        # v41 migration: Faaliyet Raporu kategorili kayitlari is_bilanco = True yap
        try:
            await conn.execute(text("""
                UPDATE kap_all_disclosures
                SET is_bilanco = TRUE
                WHERE category = 'Faaliyet Raporu' AND is_bilanco = FALSE
            """))
        except Exception:
            pass

        # v42 migration: Hatali KAP linkleri olan kayitlari sil (bildirim no < 1000000)
        # Eski scrape'lerden kalan yanlis BigPara ID'leri — yeniden scrape ile dogru linkler gelecek
        try:
            await conn.execute(text("""
                DELETE FROM kap_all_disclosures
                WHERE kap_url ~ '/Bildirim/[0-9]+'
                  AND CAST(substring(kap_url FROM '/Bildirim/([0-9]+)') AS BIGINT) < 1000000
            """))
        except Exception:
            pass

        # v43 migration: notification_logs tablosu (Bildirim Merkezi)
        try:
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS notification_logs (
                    id SERIAL PRIMARY KEY,
                    device_id VARCHAR(100) NOT NULL,
                    title VARCHAR(500) NOT NULL,
                    body TEXT,
                    category VARCHAR(30) NOT NULL DEFAULT 'system',
                    data_json TEXT,
                    is_read BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_notiflog_device_created
                ON notification_logs(device_id, created_at)
            """))
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_notiflog_created
                ON notification_logs(created_at)
            """))
        except Exception:
            pass

        # v44 migration: pending_tweets.thread_data (Thread tweet desteği — Ayın Halka Arzı)
        try:
            await conn.execute(
                text("ALTER TABLE pending_tweets ADD COLUMN IF NOT EXISTS thread_data TEXT")
            )
        except Exception:
            pass

        # v45 migration: pending_tweets.twitter_tweet_id (Video pipeline resim çekimi için)
        try:
            await conn.execute(
                text("ALTER TABLE pending_tweets ADD COLUMN IF NOT EXISTS twitter_tweet_id VARCHAR(50)")
            )
        except Exception:
            pass

        # v46 migration: E.D.O (El Degistirme Orani) kolonlari
        try:
            # IPO tablosu — senet_sayisi, cumulative_volume, edo_notified_thresholds
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS senet_sayisi BIGINT")
            )
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS cumulative_volume BIGINT DEFAULT 0")
            )
            await conn.execute(
                text("ALTER TABLE ipos ADD COLUMN IF NOT EXISTS edo_notified_thresholds TEXT")
            )
            # Ceiling track tablosu — gunluk_adet, senet_sayisi, cumulative_edo_pct
            await conn.execute(
                text("ALTER TABLE ipo_ceiling_tracks ADD COLUMN IF NOT EXISTS gunluk_adet BIGINT")
            )
            await conn.execute(
                text("ALTER TABLE ipo_ceiling_tracks ADD COLUMN IF NOT EXISTS senet_sayisi BIGINT")
            )
            await conn.execute(
                text("ALTER TABLE ipo_ceiling_tracks ADD COLUMN IF NOT EXISTS cumulative_edo_pct NUMERIC(10,2)")
            )
        except Exception:
            pass

        # v47 migration: notify_edo_free kolonu (ucretsiz EDO %1 bildirimi tercihi)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_edo_free BOOLEAN DEFAULT TRUE")
            )
        except Exception:
            pass

        # v48 migration: notify_kurum_onerileri kolonu (kurum onerileri bildirim tercihi)
        try:
            await conn.execute(
                text("ALTER TABLE users ADD COLUMN IF NOT EXISTS notify_kurum_onerileri BOOLEAN DEFAULT TRUE")
            )
        except Exception:
            pass

        # Timeout'ları resetle — normal çalışma için
        try:
            await conn.execute(text("SET lock_timeout = '0'"))
            await conn.execute(text("SET statement_timeout = '0'"))
        except Exception:
            pass
