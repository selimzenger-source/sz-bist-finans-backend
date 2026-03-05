import asyncio
import shutil
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# Pydantic ve app.config ayarlari icin path ekle
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.market_close_analyzer import scrape_uzmanpara, scrape_uzmanpara_supplementary, _analyze_reason_with_ai
from app.services.chart_image_generator import generate_ceiling_floor_images

class MockStat:
    def __init__(self, ticker, price, change, is_ceiling, reason):
        self.ticker = ticker
        self.close_price = price
        self.percent_change = change
        self.is_ceiling = is_ceiling
        self.is_floor = not is_ceiling
        self.consecutive_ceiling_count = 1
        self.consecutive_floor_count = 1
        self.monthly_ceiling_count = 1
        self.monthly_floor_count = 1
        self.reason = reason

async def main():
    print("Veriler cekiliyor...")
    ceilings = await scrape_uzmanpara(is_ceiling=True)
    floors = await scrape_uzmanpara(is_ceiling=False)
    
    print(f"Bulunan Tavan: {len(ceilings)}, Taban: {len(floors)}")
    
    c_stats = []
    print("\nTavan hisseleri analiz ediliyor...")
    for c in ceilings:
        print(f" - {c['ticker']} analizi...")
        reason = await _analyze_reason_with_ai(
            ticker=c['ticker'], 
            is_ceiling=True,
            price=c['price'],
            pct=c['change']
        )
        c_stats.append(MockStat(c['ticker'], c['price'], c['change'], True, reason))
        
    f_stats = []
    print("\nTaban hisseleri analiz ediliyor...")
    for f in floors:
        print(f" - {f['ticker']} analizi...")
        reason = await _analyze_reason_with_ai(
            ticker=f['ticker'], 
            is_ceiling=False,
            price=f['price'],
            pct=f['change']
        )
        f_stats.append(MockStat(f['ticker'], f['price'], f['change'], False, reason))

    # Ek hisseler — liste <5 ise
    c_supp = []
    f_supp = []
    if len(c_stats) < 5:
        print(f"\nTavan <5, ek yükselen hisseler çekiliyor...")
        c_tickers = [s.ticker for s in c_stats]
        supp_data = await scrape_uzmanpara_supplementary(True, c_tickers, 8 - len(c_stats))
        for s in supp_data:
            c_supp.append(MockStat(s['ticker'], s['price'], s['change'], True, ""))
    if len(f_stats) < 5:
        print(f"\nTaban <5, ek düşen hisseler çekiliyor...")
        f_tickers = [s.ticker for s in f_stats]
        supp_data = await scrape_uzmanpara_supplementary(False, f_tickers, 8 - len(f_stats))
        for s in supp_data:
            f_supp.append(MockStat(s['ticker'], s['price'], s['change'], False, ""))

    print("\nGorseller uretiliyor...")
    if c_stats:
        paths = generate_ceiling_floor_images(c_stats, True, c_supp if c_supp else None)
        for i, p in enumerate(paths):
            suffix = f"_Sayfa{i+1}" if len(paths) > 1 else ""
            dest = f"C:\\Users\\PC\\Desktop\\Guncel_Tavan_Ornegi{suffix}.png"
            shutil.copy(p, dest)
            print(f"✅ Tavan gorseli {i+1}/{len(paths)}: {dest}")
            
    if f_stats:
        paths = generate_ceiling_floor_images(f_stats, False, f_supp if f_supp else None)
        for i, p in enumerate(paths):
            suffix = f"_Sayfa{i+1}" if len(paths) > 1 else ""
            dest = f"C:\\Users\\PC\\Desktop\\Guncel_Taban_Ornegi{suffix}.png"
            shutil.copy(p, dest)
            print(f"✅ Taban gorseli {i+1}/{len(paths)}: {dest}")

if __name__ == "__main__":
    asyncio.run(main())
