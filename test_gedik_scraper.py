"""Gedik Yatirim scraper testi — liste + detay + lot tahmini."""

import asyncio
import sys
import logging

sys.path.insert(0, ".")
logging.basicConfig(level=logging.INFO)


async def main():
    from app.scrapers.gedik_scraper import GedikScraper

    scraper = GedikScraper()
    try:
        print()
        print("=" * 60)
        print("  GEDIK YATIRIM SCRAPER TEST")
        print("=" * 60)

        # 1. Liste sayfasi
        print("\n--- 1. Liste Sayfasi ---")
        ipos = await scraper.fetch_ipo_list()
        print(f"   Toplam: {len(ipos)} halka arz")

        for i, ipo in enumerate(ipos):
            ticker = ipo.get("ticker", "?")
            company = (ipo.get("company_name") or "?")[:40]
            price = ipo.get("ipo_price", "-")
            status = ipo.get("status", "-")
            dates = ipo.get("dates_raw", "-")
            print(f"   [{i+1}] {ticker} | {company}")
            print(f"       Fiyat: {price} TL | Durum: {status} | Tarih: {dates}")

        # 2. Detay sayfasi (ilk aktif IPO icin)
        print("\n--- 2. Detay Sayfasi ---")
        for ipo in ipos[:3]:
            url = ipo.get("detail_url")
            if not url:
                continue

            ticker = ipo.get("ticker", "?")
            print(f"\n   >>> {ticker} detay: {url}")
            detail = await scraper.fetch_ipo_detail(url)

            if detail:
                print(f"   Ticker: {detail.get('ticker', '-')}")
                print(f"   Sirket: {str(detail.get('company_name', '-'))[:50]}")
                print(f"   Fiyat: {detail.get('ipo_price', '-')} TL")
                print(f"   Dagitim: {detail.get('distribution_method', '-')}")
                print(f"   Pazar: {detail.get('market_segment', '-')}")
                print(f"   Toplam Lot: {detail.get('total_lots', '-')}")
                print(f"   Halka Aciklik: {detail.get('public_float_pct', '-')}%")
                print(f"   Fiyat Istikrar: {detail.get('price_stability_days', '-')} gun")

                # LOT TAHMINI
                est_lot = detail.get("estimated_lots_per_person")
                print(f"   >>> TAHMINI LOT (350K kisi): {est_lot}")

                lot_estimates = detail.get("lot_estimates", {})
                if lot_estimates:
                    print(f"   Tum lot tahminleri:")
                    for threshold, lot in sorted(lot_estimates.items()):
                        print(f"       {threshold:>10,} kisi -> {lot} lot")

                # Tahsisat
                allocations = detail.get("allocations", [])
                if allocations:
                    print(f"   Tahsisat gruplari:")
                    for a in allocations:
                        print(f"       {a['group_name']}: %{a['allocation_pct']}")
            else:
                print(f"   HATA: Detay alinamadi!")

        print()
        print("=" * 60)
        print(f"  GEDIK TEST TAMAMLANDI — {len(ipos)} halka arz bulundu")
        print("=" * 60)

    except Exception as e:
        print(f"HATA: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await scraper.close()


if __name__ == "__main__":
    asyncio.run(main())
