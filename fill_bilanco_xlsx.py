# -*- coding: utf-8 -*-
"""
Tek-komut bilanço xlsx fill — KAP parse başarısız/eksik kalan hisseler için.

Kullanım:
    python fill_bilanco_xlsx.py TICKER [TICKER2 ...]   # D:\\bilanco\\TICKER.xlsx
    python fill_bilanco_xlsx.py TICKER --overwrite      # mevcut değeri de ez (varsayılan: sadece NULL)

Ne yapar:
  1) D:\\bilanco\\TICKER.xlsx parse (parser sektör-bağımsız, label-bazlı)
  2) DB'deki bilinen dönemle çapraz kontrol (kaynak güvenilirlik)
  3) Sanity (assets>0, assets>=equity)
  4) company_financials'a SADECE eksik (NULL) alanları yaz — mevcut doğru veriyi BOZMA
  5) AI'yı yeniden üret (/admin/run-bilanco-ai)
"""
import sys, re, os, requests
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import psycopg2
from dotenv import load_dotenv
from app.services.bilanco_xlsx_parser import parse_bilanco_xlsx

load_dotenv()
_API = "https://sz-bist-finans-api.onrender.com/api/v1/admin/run-bilanco-ai"
_PW = "zenger7245175"
_FIELDS = ["total_assets", "total_equity", "current_assets", "non_current_assets",
           "cash_and_equivalents", "revenue", "gross_profit", "operating_profit", "net_income"]


def _db():
    url = None
    for l in open(".env", encoding="utf-8"):
        m = re.search(r"(postgresql://[^\s\"']+)", l)
        if m:
            url = m.group(1); break
    c = psycopg2.connect(url); c.set_client_encoding("UTF8")
    return c


def fill(ticker: str, overwrite=False, run_ai=True):
    path = f"D:/bilanco/{ticker}.xlsx"
    if not os.path.exists(path):
        print(f"❌ {ticker}: dosya yok ({path})"); return
    parsed = parse_bilanco_xlsx(path)
    if not parsed or not parsed.get("period"):
        print(f"❌ {ticker}: parse boş"); return
    period = parsed["period"]
    ta, te = parsed.get("total_assets"), parsed.get("total_equity")
    # SANITY
    if ta and ta <= 0:
        print(f"❌ {ticker} {period}: assets<=0 SANITY FAIL"); return
    if ta and te and ta < abs(te):
        print(f"❌ {ticker} {period}: assets<equity SANITY FAIL"); return

    c = _db(); cur = c.cursor()
    # ÇAPRAZ KONTROL: DB'de total_assets dolu başka dönem var mı, xlsx'le tutuyor mu?
    cur.execute("SELECT period,total_assets FROM company_financials WHERE ticker=%s AND total_assets IS NOT NULL AND period<>%s ORDER BY period DESC LIMIT 1", (ticker, period))
    base = cur.fetchone()
    if base:
        bp = parse_bilanco_xlsx(path, target_period=base[0])
        if bp and bp.get("total_assets"):
            diff = abs(float(base[1]) - bp["total_assets"]) / float(base[1]) * 100
            flag = "OK" if diff < 1 else "!!FARK"
            print(f"   çapraz {base[0]}: DB={float(base[1]):,.0f} xlsx={bp['total_assets']:,.0f} fark=%{diff:.3f} {flag}")
            if diff >= 1:
                print(f"❌ {ticker}: çapraz kontrol UYUŞMADI — güvenli, yazılmadı"); cur.close(); c.close(); return

    # Mevcut satır
    cur.execute("SELECT id,"+",".join(_FIELDS)+" FROM company_financials WHERE ticker=%s AND period=%s", (ticker, period))
    row = cur.fetchone()
    if not row:
        print(f"⚠️ {ticker} {period}: DB'de kayıt yok — yeni satır oluşturmuyorum (pipeline işi). Atlandı."); cur.close(); c.close(); return
    cur_vals = dict(zip(_FIELDS, row[1:]))
    sets, args, filled = [], [], []
    for f in _FIELDS:
        v = parsed.get(f)
        if v is None:
            continue
        if overwrite or cur_vals.get(f) is None:
            sets.append(f"{f}=%s"); args.append(v); filled.append(f)
    if not sets:
        print(f"✓ {ticker} {period}: doldurulacak eksik yok (zaten dolu)"); cur.close(); c.close(); return
    args += [ticker, period]
    cur.execute(f"UPDATE company_financials SET {', '.join(sets)} WHERE ticker=%s AND period=%s", args)
    c.commit()
    print(f"✅ {ticker} {period}: yazıldı → {', '.join(filled)}")
    cur.close(); c.close()

    if run_ai:
        try:
            r = requests.post(_API, json={"admin_password": _PW, "ticker": ticker}, timeout=90)
            d = r.json()
            print(f"   🤖 AI: score={d.get('ai_score')} label={d.get('ai_label')}")
        except Exception as e:
            print(f"   ⚠️ AI tetikleme hata: {e}")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    overwrite = "--overwrite" in sys.argv
    no_ai = "--no-ai" in sys.argv
    if not args:
        print("Kullanım: python fill_bilanco_xlsx.py TICKER [TICKER2 ...] [--overwrite] [--no-ai]"); sys.exit(1)
    for t in args:
        fill(t.upper(), overwrite=overwrite, run_ai=not no_ai)
