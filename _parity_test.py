"""Bilanço KAP Parser Parite Testi
Parser çıktısı (XBRL regex) ile DB'deki xlsx kaynaklı veriyi karşılaştır.
"""
from __future__ import annotations
import asyncio
import os
import sys
import json
import io
import re
from typing import Optional

# Force UTF-8 stdout
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add backend path
sys.path.insert(0, r"C:\Users\PC\Desktop\sz-bist-finans-backend")

# Bypass pydantic settings — set dummy env vars
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("SECRET_KEY", "x")
os.environ.setdefault("ADMIN_PASSWORD", "x")
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("ALLOWED_ORIGINS", "*")

import httpx
from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
from app.services.bilanco_kap_scraper import parse_kap_finansal_rapor

API = "https://sz-bist-finans-api.onrender.com"

# Bilinen / aday KAP bildirim ID'leri (manuel olarak bilinenler + yakın aralıkta tahmin).
# Format: (ticker, kap_id_or_url, beklenen_donem)
TEST_CASES = [
    ("KLGYO", "1610800", "2026-Q1"),
    # Aşağıdakileri tarayarak bulacağız (None ise skip)
]

# KAP bildirim ID aralığı tarayıp her hisse için en son bilanço bul
SCAN_RANGE_START = 1600000
SCAN_RANGE_END   = 1612000  # ~12K aralık
SCAN_STEP        = 1

TICKERS = ["KLGYO","EREGL","TOASO","FROTO","BIMAS",
           "AKBNK","GARAN","ISCTR","YKBNK","HALKB",
           "ANSGR","AKGRT","AGESA","EKGYO","SAHOL","KCHOL"]

FIELD_LIST = [
    "revenue","gross_profit","operating_profit","net_income","ebitda",
    "total_assets","current_assets","non_current_assets","total_equity",
    "total_debt","net_debt","cash_and_equivalents",
    "net_interest_income","net_fees_commissions","loans","deposits",
    "gross_premiums","technical_balance",
]

async def find_kap_urls_via_db(tickers: list[str]) -> dict[str, list[dict]]:
    """Backend DB'den her ticker için kap.org.tr URL'si olan bilanço bildirimleri."""
    out: dict[str, list[dict]] = {t: [] for t in tickers}
    async with httpx.AsyncClient(timeout=30) as cli:
        # Bilanço/Finansal Rapor kategorisi, son 720 saat
        for page_offset in (0, 500, 1000):
            r = await cli.get(f"{API}/api/v1/kap-all-disclosures",
                              params={"limit":500,"offset":page_offset,
                                      "category":"Bilanço/Finansal Rapor",
                                      "hours":720})
            if r.status_code != 200: break
            data = r.json()
            if not data: break
            for x in data:
                url = (x.get("url") or x.get("kap_url") or "")
                t = (x.get("company_code") or "").upper()
                if t not in out: continue
                if "kap.org.tr/tr/Bildirim/" not in url: continue
                title = (x.get("title") or "").lower()
                if not any(k in title for k in ["finansal rapor","finansal durum","bilanço","bilanco","kar veya zarar"]):
                    continue
                out[t].append({"url": url, "title": x.get("title"), "pub": x.get("published_at","")[:10]})
    return out

async def fetch_db_bilanco(ticker: str) -> dict:
    async with httpx.AsyncClient(timeout=30) as cli:
        r = await cli.get(f"{API}/api/v1/bilanco/{ticker}")
        if r.status_code != 200:
            return {}
        return r.json()

def pct_diff(a, b) -> Optional[float]:
    try:
        a = float(a); b = float(b)
    except (TypeError, ValueError):
        return None
    if a == 0 and b == 0:
        return 0.0
    if a == 0 or b == 0:
        return 100.0
    return abs(a - b) / max(abs(a), abs(b)) * 100

def classify(diff: Optional[float], db_val, p_val) -> str:
    if db_val is None and p_val is None: return "BOTH_NULL"
    if db_val is None: return "DB_NULL_PARSER_HAS"
    if p_val is None:  return "PARSER_NULL_DB_HAS"
    if diff is None:   return "ERR"
    # Sign flip
    try:
        if float(db_val) * float(p_val) < 0 and abs(float(db_val)) > 1 and abs(float(p_val)) > 1:
            return "SIGN_FLIP"
    except Exception: pass
    if diff < 1: return "OK"
    if diff < 10: return "MINOR"
    if diff < 50: return "MAJOR"
    return "MASSIVE"

async def run_test_case(ticker: str, kap_url: str, db_period_data: dict) -> dict:
    """Tek bir bildirim için parser çıktısı + DB karşılaştırma."""
    result = {"ticker": ticker, "url": kap_url}
    try:
        disc = await fetch_kap_disclosure(kap_url)
        if not disc:
            return {**result, "error": "fetch_failed"}
        body = disc.get("full_text") or ""
        if len(body) < 1000:
            return {**result, "error": f"body_too_short ({len(body)})"}

        parsed = parse_kap_finansal_rapor(body)
        period = parsed.get("period")
        result["period"] = period
        result["sector"] = parsed.get("sector_type")
        result["confidence"] = parsed.get("confidence")
        result["body_len"] = len(body)

        # DB karşılığını bul
        db_row = None
        if db_period_data and period:
            for r in db_period_data.get("financials", []):
                if r.get("period") == period:
                    db_row = r
                    break
        if not db_row:
            return {**result, "warn": f"db_no_period_{period}"}

        rows = []
        for f in FIELD_LIST:
            db_v = db_row.get(f)
            try: db_v = float(db_v) if db_v is not None else None
            except (TypeError, ValueError): db_v = None
            p_v = parsed.get(f)
            try: p_v = float(p_v) if p_v is not None else None
            except (TypeError, ValueError): p_v = None
            d = pct_diff(db_v, p_v) if (db_v is not None and p_v is not None) else None
            cls = classify(d, db_v, p_v)
            rows.append({"field": f, "db": db_v, "parser": p_v, "diff_pct": d, "status": cls})
        result["fields"] = rows
        return result
    except Exception as e:
        return {**result, "error": f"exc:{type(e).__name__}:{str(e)[:100]}"}

def fmt_v(v):
    if v is None: return "—"
    try:
        v = float(v)
    except: return str(v)
    if abs(v) > 1e9: return f"{v/1e9:.2f}B"
    if abs(v) > 1e6: return f"{v/1e6:.2f}M"
    if abs(v) > 1e3: return f"{v/1e3:.2f}K"
    return f"{v:.2f}"

async def main():
    print("=" * 100)
    print("PARITE TESTI — KAP XBRL Parser vs DB (xlsx)")
    print("=" * 100)

    # 1. DB'den KAP URL'leri bul
    print("\n[1/3] DB'den KAP URL'leri taraniyor...")
    urls_per_ticker = await find_kap_urls_via_db(TICKERS)
    for t, urls in urls_per_ticker.items():
        print(f"  {t}: {len(urls)} kap.org.tr URL")

    # 2. Her ticker için DB'den bilanço çek
    print("\n[2/3] DB'den bilanço verileri cekiliyor...")
    db_data = {}
    for t in TICKERS:
        db_data[t] = await fetch_db_bilanco(t)
        n = len(db_data[t].get("financials", []))
        print(f"  {t}: {n} donem DB'de")

    # 3. Her bildirim için parser test
    print("\n[3/3] Test caseleri calistiriliyor...\n")

    all_results = []
    # KLGYO için bilinen 1610800 ekstra test
    all_results.append(await run_test_case("KLGYO", "https://www.kap.org.tr/tr/Bildirim/1610800", db_data.get("KLGYO", {})))

    for t in TICKERS:
        urls = urls_per_ticker.get(t, [])
        # En fazla 2 bildirim test et per ticker
        for u in urls[:2]:
            r = await run_test_case(t, u["url"], db_data.get(t, {}))
            all_results.append(r)
            await asyncio.sleep(0.5)  # rate limit

    # ─────────────────────────────────────────────────────────
    # RAPOR
    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("DETAYLI RAPOR")
    print("=" * 100)

    summary_by_status = {"OK":0,"MINOR":0,"MAJOR":0,"MASSIVE":0,"SIGN_FLIP":0,
                         "DB_NULL_PARSER_HAS":0,"PARSER_NULL_DB_HAS":0,"BOTH_NULL":0,"ERR":0}
    sector_stats = {}

    for r in all_results:
        print(f"\n─── {r.get('ticker')} | {r.get('url')}")
        if r.get("error"):
            print(f"   HATA: {r['error']}")
            continue
        if r.get("warn"):
            print(f"   UYARI: {r['warn']} (parser period={r.get('period')})")
            continue
        print(f"   period={r['period']}  sector={r.get('sector')}  conf={r.get('confidence')}  body_len={r.get('body_len')}")
        sec = r.get("sector") or "?"
        sector_stats.setdefault(sec, {"OK":0,"BAD":0,"NULL":0})

        for f in r.get("fields", []):
            status = f["status"]
            summary_by_status[status] = summary_by_status.get(status, 0) + 1
            mark = {"OK":"OK","MINOR":"~","MAJOR":"!","MASSIVE":"XX","SIGN_FLIP":"+/-",
                    "DB_NULL_PARSER_HAS":"PA","PARSER_NULL_DB_HAS":"PN","BOTH_NULL":"--","ERR":"?"}[status]
            if status in ("OK","MINOR"): sector_stats[sec]["OK"] += 1
            elif status in ("PARSER_NULL_DB_HAS","DB_NULL_PARSER_HAS","BOTH_NULL"): sector_stats[sec]["NULL"] += 1
            else: sector_stats[sec]["BAD"] += 1

            df = f"{f['diff_pct']:.1f}%" if f['diff_pct'] is not None else "—"
            print(f"     [{mark:>3}] {f['field']:<25} db={fmt_v(f['db']):>10}  parser={fmt_v(f['parser']):>10}  diff={df}")

    # ─────────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("OZET")
    print("=" * 100)
    total = sum(summary_by_status.values())
    print(f"\nToplam alan karsilastirmasi: {total}")
    for k, v in summary_by_status.items():
        pct = (v/total*100) if total else 0
        print(f"  {k:<25} {v:>4}  ({pct:.1f}%)")

    print("\nSektor bazinda:")
    for sec, st in sector_stats.items():
        tot = sum(st.values())
        if tot == 0: continue
        ok_pct = st["OK"]/tot*100
        bad_pct = st["BAD"]/tot*100
        print(f"  {sec:<12} OK={st['OK']}/{tot} ({ok_pct:.1f}%)  BAD={st['BAD']} ({bad_pct:.1f}%)  NULL={st['NULL']}")

if __name__ == "__main__":
    asyncio.run(main())
