"""KAP'tan her ticker için son finansal rapor bildirim ID'lerini bulur.
Member sayfasındaki RSC chunk'lardan çıkarır."""
import asyncio, re, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
import httpx

TICKERS = ["KLGYO","EREGL","TOASO","FROTO","BIMAS",
           "AKBNK","GARAN","ISCTR","YKBNK","HALKB",
           "ANSGR","AKGRT","AGESA","EKGYO","SAHOL","KCHOL",
           "ISMEN","VKING"]

HEADERS = {"User-Agent":"Mozilla/5.0","Accept-Language":"tr-TR,tr;q=0.9"}

async def fetch_member_disclosures(ticker: str, cli: httpx.AsyncClient):
    # KAP üye sayfası
    url = f"https://www.kap.org.tr/tr/sirket-bilgileri/ozet/{ticker}"
    try:
        r = await cli.get(url, headers=HEADERS, timeout=20, follow_redirects=True)
    except Exception as e:
        return ticker, f"ERR: {e}"
    if r.status_code != 200:
        return ticker, f"HTTP {r.status_code}"
    html = r.text
    # Bildirim/{id} pattern
    ids = re.findall(r"/tr/Bildirim/(\d+)", html)
    # Finansal Rapor / bilanço başlığı olanları bul
    # RSC chunk'larında bildirim metadata var
    # Mesela: "title":"...","disclosureClass":"FR","kapDisclosureNumber":"1610800"
    # disclosureClass=FR (Finansal Rapor) olanları al
    fr_matches = re.findall(
        r'\{[^{}]*?"disclosureClass"\s*:\s*"FR"[^{}]*?"kapDisclosureNumber"\s*:\s*"(\d+)"[^{}]*?\}',
        html
    )
    # Alternate
    fr_matches2 = re.findall(
        r'"kapDisclosureNumber"\s*:\s*"(\d+)"[^{}]*?"disclosureClass"\s*:\s*"FR"',
        html
    )
    found_fr = sorted(set(fr_matches + fr_matches2), reverse=True)
    return ticker, {"all_count": len(set(ids)), "fr_ids": found_fr[:5], "html_len": len(html)}

async def main():
    async with httpx.AsyncClient() as cli:
        for t in TICKERS:
            res = await fetch_member_disclosures(t, cli)
            print(res)
            await asyncio.sleep(0.5)

asyncio.run(main())
