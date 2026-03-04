import asyncio
import shutil
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# Pydantic ve app.config ayarlari icin path ekle
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.market_close_analyzer import scrape_uzmanpara, _analyze_reason_with_ai
from app.services.chart_image_generator import generate_ceiling_floor_images

class MockStat:
    def __init__(self, ticker, price, is_ceiling, reason):
        self.ticker = ticker
        self.close_price = price
        self.is_ceiling = is_ceiling
        self.is_floor = not is_ceiling
        self.consecutive_ceiling_count = 1
        self.consecutive_floor_count = 1
        self.reason = reason

async def main():
    print("Veriler cekiliyor...")
    ceilings = await scrape_uzmanpara(is_ceiling=True)
    floors = await scrape_uzmanpara(is_ceiling=False)
    
    print(f"Bulunan Tavan: {len(ceilings)}, Taban: {len(floors)}")
    
    limit = 10
    
    c_stats = []
    print("\nTavan hisseleri analiz ediliyor...")
    for c in ceilings[:limit]:
        print(f" - {c['ticker']} analizi...")
        reason = await _analyze_reason_with_ai(c['ticker'], True)
        if hasattr(reason, "replace"):
            reason = reason.replace("\n", " ")
        c_stats.append(MockStat(c['ticker'], c['price'], True, reason))
        
    f_stats = []
    print("\nTaban hisseleri analiz ediliyor...")
    for f in floors[:limit]:
        print(f" - {f['ticker']} analizi...")
        reason = await _analyze_reason_with_ai(f['ticker'], False)
        if hasattr(reason, "replace"):
            reason = reason.replace("\n", " ")
        f_stats.append(MockStat(f['ticker'], f['price'], False, reason))

    print("\nGorseller uretiliyor...")
    if c_stats:
        paths = generate_ceiling_floor_images(c_stats, True)
        for i, p in enumerate(paths):
            dest = f"C:\\Users\\PC\\Desktop\\Ornek_Tavan_{i+1}.png"
            shutil.copy(p, dest)
            print("Tavan gorseli masaustune kaydedildi:", dest)
            
    if f_stats:
        paths = generate_ceiling_floor_images(f_stats, False)
        for i, p in enumerate(paths):
            dest = f"C:\\Users\\PC\\Desktop\\Ornek_Taban_{i+1}.png"
            shutil.copy(p, dest)
            print("Taban gorseli masaustune kaydedildi:", dest)

if __name__ == "__main__":
    asyncio.run(main())
