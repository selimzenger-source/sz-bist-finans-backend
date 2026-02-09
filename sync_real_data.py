"""Gercek halka arz verisini cek ve DB'ye kaydet.

Kaynaklar:
1. SPK — bekleyen basvurular (130+ sirket)
2. InfoYatirim — aktif takvim (fiyat, lot, tarih, durum)
"""

import asyncio
import warnings
import logging

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

from app.database import async_session, init_db, engine
from app.models.ipo import IPO, IPOBroker, IPOAllocation
from sqlalchemy import select, text


async def sync():
    # Tablolari yeniden olustur (schema degisti)
    from app.database import Base
    import app.models  # noqa — modelleri yukle

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    print("Tablolar yeniden olusturuldu")

    # --- 1. InfoYatirim'dan aktif takvimi cek ---
    from app.scrapers.infoyatirim_scraper import InfoYatirimScraper

    scraper = InfoYatirimScraper()
    try:
        all_ipos = await scraper.fetch_all_ipos(max_pages=3)
        print(f"\nInfoYatirim: {len(all_ipos)} halka arz cekildi")
    finally:
        await scraper.close()

    # --- 2. SPK'dan bekleyen basvurulari cek ---
    from app.scrapers.spk_scraper import SPKScraper

    spk_scraper = SPKScraper()
    try:
        spk_apps = await spk_scraper.fetch_ipo_applications()
        print(f"SPK: {len(spk_apps)} basvuru cekildi")
    finally:
        await spk_scraper.close()

    # --- 3. DB'ye kaydet ---
    async with async_session() as db:
        saved = 0

        # InfoYatirim verisini kaydet (detayli veri)
        for ipo_data in all_ipos:
            ipo = IPO(
                company_name=ipo_data["company_name"],
                ticker=ipo_data.get("ticker"),
                status=ipo_data["status"],
                ipo_price=ipo_data.get("ipo_price"),
                total_lots=ipo_data.get("total_lots"),
                total_applicants=ipo_data.get("total_applicants"),
                subscription_start=ipo_data.get("subscription_start"),
                subscription_end=ipo_data.get("subscription_end"),
                subscription_hours=ipo_data.get("subscription_dates_raw"),
                trading_start=ipo_data.get("trading_start"),
                distribution_method=ipo_data.get("distribution_method"),
                kap_notification_url=ipo_data.get("detail_url"),
            )
            db.add(ipo)
            saved += 1

        await db.commit()
        print(f"\nDB'ye {saved} InfoYatirim halka arz kaydedildi")

        # SPK basvurularindan henuz InfoYatirim'da olmayanlari ekle
        spk_added = 0
        for app in spk_apps:
            # Ayni isimde bir kayit var mi kontrol et
            result = await db.execute(
                select(IPO).where(
                    IPO.company_name.ilike(f"%{app['company_name'][:30]}%")
                )
            )
            existing = result.scalar_one_or_none()

            if not existing:
                ipo = IPO(
                    company_name=app["company_name"],
                    status="upcoming",
                    spk_approval_date=app.get("application_date"),
                )
                db.add(ipo)
                spk_added += 1

        await db.commit()
        print(f"DB'ye {spk_added} SPK-only basvuru eklendi")

        # --- Ozet ---
        result = await db.execute(select(IPO))
        total = len(result.scalars().all())

        result = await db.execute(select(IPO).where(IPO.status == "active"))
        active = len(result.scalars().all())

        result = await db.execute(select(IPO).where(IPO.status == "completed"))
        completed = len(result.scalars().all())

        result = await db.execute(select(IPO).where(IPO.status == "upcoming"))
        upcoming = len(result.scalars().all())

        print(f"\n=== DB Ozet ===")
        print(f"Toplam: {total}")
        print(f"  Aktif: {active}")
        print(f"  Tamamlanan: {completed}")
        print(f"  Bekleyen: {upcoming}")


if __name__ == "__main__":
    asyncio.run(sync())
