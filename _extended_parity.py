"""Genişletilmiş parite testi — 2025-Q4 ve 2025-Q3 dönem testi için
KAP ID brute force + parser sonucu DB ile karşılaştır."""
import asyncio, sys, io, os, re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("SECRET_KEY", "x")
os.environ.setdefault("ADMIN_PASSWORD", "x")
sys.path.insert(0, r"C:\Users\PC\Desktop\sz-bist-finans-backend")

import httpx
from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
from app.services.bilanco_kap_scraper import parse_kap_finansal_rapor

API = "https://sz-bist-finans-api.onrender.com"
TICKERS = ["KLGYO","EREGL","TOASO","FROTO","BIMAS",
           "AKBNK","GARAN","ISCTR","YKBNK","HALKB",
           "ANSGR","AKGRT","AGESA","EKGYO","SAHOL","KCHOL"]

# Bilinen 2026-Q1 etrafı: 1598000-1601000
# 2025-Q4 (Mart 2026 başı): 1550000-1590000
# 2025-Q3 (Kasım 2025): 1450000-1480000
SCAN_RANGES = [
    ("2026-Q1", range(1597000, 1612000, 1)),   # zaten testten geçti
    ("2025-Q4", range(1545000, 1595000, 1)),   # Mart 2026 başı
    ("2025-Q3", range(1430000, 1490000, 1)),
]

FIELD_LIST = ["revenue","gross_profit","operating_profit","net_income","ebitda",
              "total_assets","current_assets","non_current_assets","total_equity",
              "total_debt","net_debt","cash_and_equivalents",
              "net_interest_income","net_fees_commissions","loans","deposits",
              "gross_premiums","technical_balance"]

async def fetch_db_bilanco(ticker, cli):
    r = await cli.get(f"{API}/api/v1/bilanco/{ticker}", timeout=30)
    if r.status_code != 200: return {}
    return r.json()

def pct_diff(a, b):
    try: a = float(a); b = float(b)
    except: return None
    if a == 0 and b == 0: return 0.0
    if a == 0 or b == 0: return 100.0
    return abs(a-b)/max(abs(a),abs(b))*100

async def fetch_kap_quick(kap_id, cli):
    """Hızlı fetch — sadece HTML çek + ticker + dönem çıkar"""
    url = f"https://www.kap.org.tr/tr/Bildirim/{kap_id}"
    try:
        r = await cli.get(url, timeout=10,
                          headers={"User-Agent":"Mozilla/5.0","Accept":"text/html"})
        if r.status_code != 200: return None
        html = r.text
        # Bildirim tipini kontrol
        if "Finansal Rapor" not in html and "Konsolide Finansal" not in html:
            return None
        # Ticker çıkar
        m = re.search(r'"stockCode"\s*:\s*"([A-Z0-9,]+)"', html)
        if not m: return None
        codes = m.group(1).split(",")
        # Tarih çıkar
        d = re.search(r'(\d{2})\.(\d{2})\.(20\d{2})', html)
        return {"codes": codes, "date": d.group(0) if d else None, "kap_id": kap_id, "html_len": len(html)}
    except Exception:
        return None

async def find_by_ticker(target_ticker, period_label, scan_range, cli, max_per_ticker=2):
    """Belirli ticker için belirli period etrafında bildirim ara."""
    found = []
    # Step size: önce 50, sonra hassas ara
    for kid in scan_range:
        if len(found) >= max_per_ticker: break
        meta = await fetch_kap_quick(kid, cli)
        if meta and target_ticker in meta["codes"]:
            print(f"  FOUND {target_ticker} -> KAP/{kid} date={meta['date']}")
            found.append(kid)
    return found

async def main():
    # ARGS ile çalıştır: python _extended_parity.py SCAN_FAST  veya  TEST_KNOWN
    mode = sys.argv[1] if len(sys.argv)>1 else "TEST_KNOWN"

    async with httpx.AsyncClient() as cli:
        if mode == "TEST_KNOWN":
            # Bilinen ID'leri test et (manuel girdi)
            # 2026-Q1 testleri zaten yapıldı. 2025-Q4 ve önceleri için aday ID listesi:
            KNOWN_IDS = {
                # ticker: [list of KAP ID'leri]
                "KLGYO": [1556728, 1556729, 1556730],  # tahmini Mart 2026 başı
                "EREGL": [1556000, 1556001],
            }
            # Brute force yerine: bilinen test ID'leri
            tests = [
                # Sadece bulduğumuz çalışan ID'ler
            ]
            print("TEST_KNOWN modunda. Aday ID test:")
            for kid in range(1556000, 1560000, 100):
                meta = await fetch_kap_quick(kid, cli)
                if meta:
                    print(f"  {kid} -> codes={meta['codes'][:3]} date={meta['date']}")
                await asyncio.sleep(0.1)

        elif mode == "SCAN":
            # KLGYO için 2025-Q4 ara
            print("KLGYO 2025-Q4 (Mart 2026 başı) için tarama...")
            ids = await find_by_ticker("KLGYO", "2025-Q4", range(1556000, 1565000), cli)
            print("Sonuç:", ids)

asyncio.run(main())
