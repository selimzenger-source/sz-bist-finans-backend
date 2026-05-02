"""
IsYatirim Bilanco & Temettu Scraper
====================================
Kaynak: isyatirim.com.tr undocumented JSON API
Referans: https://github.com/urazakgul/isyatirimhisse
           https://github.com/saidsurucu/borsapy

API Notlari:
- MaliTablo endpoint'i GET ile calisir, max 4 period per call
- Bilanco icin financialGroup: XI_29 (sanayi), UFRS (banka/finans)
- Temettu icin GetSermayeArttirimlari POST endpoint'i (session cookie gerekli)
- Rate limit: Belgelenmemis, 1-2 sn arasi bekleme onerilen
- 700+ hisse x 5 yil = ~875 istek (bilanco), ~22 dk @ 1.5sn/istek

Veri Saklama Stratejisi:
- 2015'ten itibaren tum verileri sakla, SILME — surekli biriktirilir
- Haftalik batch job tum hisseleri tarar (Pazar gecesi)
- Yeni bilanco bildirimi (KAP is_bilanco=TRUE) → aninda o ticker icin guncelle
- Yeni halka arzlar icin: trading basladiginda otomatik eklenir
"""

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ─── URL'ler ──────────────────────────────────────────────────────────────────
_BASE_URL = "https://www.isyatirim.com.tr"
_MALI_TABLO_URL = (
    f"{_BASE_URL}/_layouts/15/IsYatirim.Website/Common/Data.aspx/MaliTablo"
)
_TEMETTU_URL = (
    f"{_BASE_URL}/_layouts/15/IsYatirim.Website/StockInfo/"
    "CompanyInfoAjax.aspx/GetSermayeArttirimlari"
)
_FINTABLES_TEMETTU_URL = "https://fintables.com/sirketler/{ticker}/sermaye-artirimlari-temettuler"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/default.aspx",
}

_TIMEOUT = 30
_RATE_LIMIT_DELAY = 1.5  # saniye — istekler arasi bekleme

# ─── itemCode → field mapping ─────────────────────────────────────────────────
# IsYatirim MaliTablo response'unda her satir bir itemCode ile gelir.
# Bu mapping, onemli finansal kalemleri bizim DB field'larina esler.

_BILANCO_ITEM_MAP = {
    # Sanayi (XI_29)
    "1A": "current_assets",          # Donen Varliklar
    "1AK": "non_current_assets",     # Duran Varliklar
    "1BL": "total_assets",           # TOPLAM VARLIKLAR
    "2A": "short_term_liabilities",  # Kisa Vadeli Yukumlulukler
    "2B": "long_term_liabilities",   # Uzun Vadeli Yukumlulukler
    "2N": "total_equity",            # Ozkaynaklar (sanayi)
    "1AB": "cash_and_equivalents",   # Nakit ve Nakit Benzerleri

    # Banka (UFRS_K) — farkli kodlar
    "2O": "total_equity",            # XVI. OZKAYNAKLAR (banka)
    "2Z": "total_assets",            # PASIF TOPLAMI (banka, = aktif toplami)
    "3Z": "net_income",              # XXIII. NET DONEM KARI/ZARARI (banka)
    "3ZA": "net_income_parent",      # 23.1 Grubun Kari/Zarari
}

_GELIR_ITEM_MAP = {
    # Gelir Tablosu
    "3C": "revenue",                 # Satis Gelirleri (Hasilat)
    "3D": "gross_profit",            # BRUT KAR
    "3DF": "operating_profit",       # FAALIYET KARI
    "3I": "profit_before_tax",       # Vergi Oncesi Kar
    "3L": "net_income",              # DONEM NET KARI
}


def _normalize_tr(s: str) -> str:
    """Turkce karakterleri ASCII'ye cevir + uppercase. Match icin."""
    if not s:
        return ""
    return (s.upper()
            .replace("İ", "I").replace("I", "I")  # i kucuk -> I buyuk
            .replace("Ş", "S").replace("Ğ", "G")
            .replace("Ç", "C").replace("Ö", "O").replace("Ü", "U"))


def _match_field_by_desc(desc: str, item_code: str = "") -> str | None:
    """Universal description-based field matcher — TUM sektorler.

    Sanayi (XI_29), Banka (UFRS_K), Sigorta, Holding, GMYO, Faktoring, Leasing,
    Yatirim Ortakligi — hepsi farkli itemCode kullanir ama Turkce description'lari
    benzer. Description-based match TUM'unu kapsar.

    Oncelik: itemCode'da kesin match -> description'da kesin match -> fallback.
    """
    if not desc:
        return None
    d = _normalize_tr(desc).strip()

    # ═══════════ NET KAR ═══════════
    # Sanayi: "DÖNEM NET KÂRI"
    # Banka:  "DÖNEM KARI/ZARARI" / "DÖNEM NET KARI"
    # Sigorta: "DÖNEM KARI/ZARARI"
    # GMYO/Holding: "ANA ORTAKLIK PAYLARI" (konsolide net kar payi)
    if any(k in d for k in [
        "DONEM NET KARI", "DONEM NET KAR/", "NET DONEM KARI",
        "DONEM KARI/ZARARI", "NET DONEM KAR", "DONEM NET KAR",
    ]) and "DAGITIM" not in d and "KARPAYI" not in d.replace(" ",""):
        return "net_income"
    # Holdingler: "Ana Ortaklik Paylari" net kar bolumu
    if "ANA ORTAKLIK" in d and ("PAYLARI" in d or "PAY" in d) and ("KAR" in d or "ZARAR" in d):
        return "net_income"

    # ═══════════ HASILAT / GELIR ═══════════
    # Sanayi: "Hasılat", "Satış Gelirleri"
    # Banka: "Faiz Gelirleri" / "Toplam Faiz Geliri"
    # Sigorta: "Brüt Yazılan Primler" / "Teknik Gelirler"
    # Holding/Faktoring: "Esas Faaliyet Gelirleri"
    if d == "HASILAT" or "SATIS GELIRLERI" in d or d == "GELIRLER":
        return "revenue"
    if "FAIZ GELIRLERI" in d and "NET" not in d.split("FAIZ")[0]:  # "Faiz Gelirleri" ama "Net Faiz" değil
        return "revenue"
    if "BRUT YAZILAN PRIM" in d or "TEKNIK GELIRLER" in d:
        return "revenue"
    if "ESAS FAALIYET GELIRLERI" in d:
        return "revenue"

    # ═══════════ BRUT KAR ═══════════
    if d == "BRUT KAR" or d == "BRUT KAR (ZARAR)" or "BRUT KAR/" in d:
        return "gross_profit"

    # ═══════════ FAALIYET KARI ═══════════
    if "FAALIYET KARI" in d and "ESAS" not in d and "DEVAM" not in d:
        return "operating_profit"

    # ═══════════ FAVOK / EBITDA ═══════════
    if "FAVOK" in d or "EBITDA" in d:
        return "ebitda"

    # ═══════════ TOPLAM AKTIF / VARLIKLAR ═══════════
    # Sanayi/holding/GMYO: "TOPLAM VARLIKLAR"
    # Banka: "VI. AKTIF TOPLAMI" / "TOPLAM AKTIFLER"
    # Sigorta: "AKTIF TOPLAMI" / "TOPLAM AKTIFLER"
    if any(k in d for k in [
        "TOPLAM AKTIFLER", "TOPLAM AKTIF", "TOPLAM VARLIKLAR",
        "AKTIF TOPLAMI", "AKTIFLER TOPLAMI",
    ]):
        return "total_assets"

    # ═══════════ OZKAYNAK ═══════════
    # Sanayi: "Toplam Özkaynaklar"
    # Banka: "XVI. ÖZKAYNAKLAR" (Roman numeral prefix, Standalone)
    # Sigorta/Holding: "Özkaynaklar Toplamı"
    if "AZINLIK" not in d and "KONTROL GUCU OLMAYAN" not in d and "PAY" not in d.split("OZKAYNAK")[0] if "OZKAYNAK" in d else True:
        # Toplam/Genel kelimesiyle eslesim
        if any(k in d for k in [
            "OZKAYNAKLAR TOPLAMI", "OZKAYNAK GENEL TOPLAM", "OZKAYNAKLAR GENEL TOPLAM",
            "TOPLAM OZKAYNAK", "OZSERMAYE TOPLAM", "OZ SERMAYE TOPLAM",
            "OZKAYNAK TOPLAM",
        ]):
            return "total_equity"
        # Banka: "XVI. ÖZKAYNAKLAR" — Roman numeral + ÖZKAYNAKLAR yalnız
        # (yontemine, payi, sermaye ile eslesmemeli)
        import re as _re
        if _re.match(r"^[IVXL]+\.\s*OZKAYNAKLAR\s*$", d) or d == "OZKAYNAKLAR":
            return "total_equity"

    # ═══════════ DONEN VARLIKLAR (sanayi) ═══════════
    if d == "DONEN VARLIKLAR" or "TOPLAM DONEN VARLIK" in d:
        return "current_assets"
    if d == "DURAN VARLIKLAR" or "TOPLAM DURAN VARLIK" in d:
        return "non_current_assets"

    # ═══════════ TOPLAM YUKUMLULUKLER / BORC ═══════════
    if any(k in d for k in ["TOPLAM YUKUMLULUK", "TOPLAM PASIF", "TOPLAM BORC", "TOPLAM KAYNAKLAR"]):
        if "OZKAYNAK" not in d:  # "Toplam Yükümlülükler ve Özkaynaklar" hariç
            return "total_debt"

    # ═══════════ NAKIT ═══════════
    if "NAKIT VE NAKIT BENZER" in d or "NAKIT VE MERKEZ" in d:
        return "cash_and_equivalents"
    if d.startswith("I. NAKIT"):
        return "cash_and_equivalents"

    # ═══════════ KISA / UZUN VADELI YUKUMLULUK ═══════════
    if "KISA VADELI YUKUMLULUK" in d:
        return "short_term_liabilities"
    if "UZUN VADELI YUKUMLULUK" in d:
        return "long_term_liabilities"

    return None


# Geriye uyumluluk — eski cagrilar
def _match_bank_field(desc: str) -> str | None:
    return _match_field_by_desc(desc)

# ─── Her ticker icin dogru financialGroup cache ───────────────────────────────
_ticker_financial_group: dict[str, str] = {}

# Banka / finans sektoru tickerlari (UFRS kullanan)
_BANK_TICKERS = {
    "AKBNK", "GARAN", "ISCTR", "YKBNK", "HALKB", "VAKBN", "QNBFB",
    "TSKB", "SKBNK", "ALBRK", "DENIZ", "ICBCT", "KLNMA", "TURKF",
    "SAHOL", "KOZAL", "AGHOL",  # Holdingler de UFRS olabilir
}


def _quarter_from_month(month: int) -> str:
    """3 → Q1, 6 → Q2, 9 → Q3, 12 → Q4"""
    return f"Q{month // 3}"


def _period_end_date(year: int, month: int) -> str:
    """Year + month → YYYY-MM-DD (donem sonu)"""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-{last_day:02d}"


def _safe_float(val: Any) -> float | None:
    """None / 0 / bos → None, diger → float"""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f != 0 else None
    except (ValueError, TypeError):
        return None


def _calc_ratio(numerator: float | None, denominator: float | None) -> float | None:
    """Guvenli bolme — None veya 0 boleni icin None doner."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator, 4)


# ═══════════════════════════════════════════════════════════════════════════════
#  BILANCO (MALI TABLO) SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════


async def fetch_bilanco(
    ticker: str, years: int = 11, client: httpx.AsyncClient | None = None
) -> list[dict]:
    """
    Belirtilen hisse icin ceyreklik bilanco + gelir tablosu verilerini ceker.
    IsYatirim MaliTablo GET endpoint'i kullanilir — max 4 period per call.

    Args:
        ticker: Hisse kodu (or. "THYAO")
        years: Kac yillik veri cekilecek (varsayilan: 11 → 2015'ten itibaren)
        client: Var olan httpx client (batch icin paylasilir)

    Returns:
        list[dict] — Her eleman bir ceyrek donem verisi, yeniden eskiye sirali.
    """
    now = datetime.now()
    current_year = now.year

    # financialGroup belirle: banka/finans → UFRS, diger → XI_29
    if ticker in _ticker_financial_group:
        fin_group = _ticker_financial_group[ticker]
    elif ticker.upper() in _BANK_TICKERS:
        fin_group = "UFRS"
    else:
        fin_group = "XI_29"

    # Her yil icin 4 ceyrek (Q1=3, Q2=6, Q3=9, Q4=12) cekilecek
    all_periods: list[dict] = []
    own_client = client is None

    try:
        if own_client:
            client = httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS)

        for year_offset in range(years):
            year = current_year - year_offset
            params = {
                "companyCode": ticker,
                "exchange": "TRY",
                "financialGroup": fin_group,
                "year1": str(year),
                "period1": "12",
                "year2": str(year),
                "period2": "9",
                "year3": str(year),
                "period3": "6",
                "year4": str(year),
                "period4": "3",
            }

            resp = await client.get(_MALI_TABLO_URL, params=params)

            if resp.status_code != 200:
                logger.warning(
                    "IsYatirim MaliTablo %s/%d HTTP %d", ticker, year, resp.status_code
                )
                continue

            data = resp.json()

            # Bos response kontrolu — yanlis financialGroup olabilir
            values = data.get("value", [])
            if not values and fin_group == "XI_29":
                # UFRS ile tekrar dene (ardindan UFRS_K — banka)
                for fb_group in ("UFRS", "UFRS_K"):
                    params["financialGroup"] = fb_group
                    resp2 = await client.get(_MALI_TABLO_URL, params=params)
                    if resp2.status_code == 200:
                        data2 = resp2.json()
                        values = data2.get("value", [])
                        if values:
                            fin_group = fb_group
                            _ticker_financial_group[ticker] = fb_group
                            logger.info("%s icin %s kullaniliyor", ticker, fb_group)
                            break

            if not values:
                continue

            # Response'u parse et — her value satirinda itemCode + value1..4
            # value1 = period1 (Q4), value2 = period2 (Q3), ...
            period_data: dict[int, dict] = {}  # period_idx → {field: val}
            for row in values:
                item_code = row.get("itemCode", "")

                # Description-based match ONCELIKLI — TUM sektorler icin guvenli
                # (itemCode farkli sektorlerde farkli anlam tasiyor:
                #  banka 1A=Nakit, sanayi 1A=Donen Varlik)
                desc_raw = row.get("itemDescTr") or ""
                field = _match_field_by_desc(desc_raw, item_code)

                # Description match basarisizsa itemCode mapping (sanayi standardi)
                if not field:
                    field = _BILANCO_ITEM_MAP.get(item_code) or _GELIR_ITEM_MAP.get(item_code)

                # FAVOK son cara
                if not field:
                    desc_upper = desc_raw.upper()
                    if "FAVÖK" in desc_upper or "EBITDA" in desc_upper:
                        field = "ebitda"

                if not field:
                    continue

                for idx in range(4):
                    val = _safe_float(row.get(f"value{idx + 1}"))
                    if val is not None:
                        if idx not in period_data:
                            period_data[idx] = {}
                        period_data[idx][field] = val

            # Period'lari yapila
            period_months = [12, 9, 6, 3]  # value1→Q4, value2→Q3, ...
            for idx, month in enumerate(period_months):
                if idx not in period_data:
                    continue

                pd = period_data[idx]
                period_str = f"{year}-{_quarter_from_month(month)}"

                # Turetilmis alanlar
                total_debt = None
                stl = pd.get("short_term_liabilities")
                ltl = pd.get("long_term_liabilities")
                if stl is not None or ltl is not None:
                    total_debt = (stl or 0) + (ltl or 0)

                net_debt = None
                cash = pd.get("cash_and_equivalents")
                if total_debt is not None and cash is not None:
                    net_debt = total_debt - cash

                revenue = pd.get("revenue")
                gross_profit = pd.get("gross_profit")
                net_income = pd.get("net_income")
                total_equity = pd.get("total_equity")
                current_assets = pd.get("current_assets")

                result = {
                    "ticker": ticker,
                    "period": period_str,
                    "period_end_date": _period_end_date(year, month),
                    "revenue": revenue,
                    "gross_profit": gross_profit,
                    "operating_profit": pd.get("operating_profit"),
                    "net_income": net_income,
                    "ebitda": pd.get("ebitda"),
                    "total_assets": pd.get("total_assets"),
                    "total_equity": total_equity,
                    "total_debt": total_debt,
                    "net_debt": net_debt,
                    "cash_and_equivalents": cash,
                    # Oranlar
                    "current_ratio": _calc_ratio(current_assets, stl),
                    "gross_margin_pct": (
                        round(gross_profit / revenue * 100, 2)
                        if revenue and gross_profit else None
                    ),
                    "net_margin_pct": (
                        round(net_income / revenue * 100, 2)
                        if revenue and net_income else None
                    ),
                    "roe_pct": (
                        round(net_income / total_equity * 100, 2)
                        if total_equity and net_income else None
                    ),
                    "debt_to_equity": _calc_ratio(total_debt, total_equity),
                    "source": "isyatirim",
                    "financial_group": fin_group,
                }
                all_periods.append(result)

            # Rate limiting
            await asyncio.sleep(_RATE_LIMIT_DELAY)

        # financialGroup cache'le
        if fin_group != _ticker_financial_group.get(ticker):
            _ticker_financial_group[ticker] = fin_group

    except httpx.TimeoutException:
        logger.warning("IsYatirim bilanco timeout: %s", ticker)
    except Exception as e:
        logger.exception("IsYatirim bilanco hatasi %s: %s", ticker, e)
    finally:
        if own_client and client:
            await client.aclose()

    # Yeniden eskiye sirala
    all_periods.sort(key=lambda x: x["period"], reverse=True)
    logger.info("IsYatirim bilanco: %s — %d donem cekildi", ticker, len(all_periods))
    return all_periods


# ═══════════════════════════════════════════════════════════════════════════════
#  TEMETTU (TEMETTÜ) SCRAPER — Fintables (basit, cookie gerektirmez)
# ═══════════════════════════════════════════════════════════════════════════════


async def fetch_temettu_gecmisi(
    ticker: str, client: httpx.AsyncClient | None = None
) -> list[dict]:
    """
    Belirtilen hisse icin temettu gecmisini IsYatirim'den ceker.

    Strateji: IsYatirim Temettü sayfasını HTML parse et.
    Fintables 403 doner (Cloudflare), IsYatirim POST endpoint session cookie ister.
    En guvenilir yol: IsYatirim hisse detay sayfasindan temettu bilgisi cekmek.

    Alternatif: Mevcut DB'deki dividends + dividend_history tablolarini kullan
    (zaten dolu, SPK/KAP'tan besleniyorlar).

    Returns:
        list[dict] — Her eleman bir temettu odeme kaydi (yeniden eskiye)
    """
    own_client = client is None
    results = []

    try:
        if own_client:
            client = httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS)

        # IsYatirim hisse detay sayfasindan temettu verisi cek
        detail_url = f"{_BASE_URL}/tr-tr/analiz/hisse/Sayfalar/sirket-karti.aspx?hession={ticker}"
        resp = await client.get(detail_url)

        if resp.status_code != 200:
            logger.warning("IsYatirim temettu %s HTTP %d", ticker, resp.status_code)
            return []

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")

        # "Temettü" veya "Nakit Temettü" basligi olan tabloyu bul
        tables = soup.find_all("table")
        for table in tables:
            headers_text = " ".join(
                th.get_text(strip=True).lower() for th in table.find_all("th")
            )
            if "temettü" not in headers_text and "nakit" not in headers_text:
                continue

            header_cells = [th.get_text(strip=True) for th in table.find_all("th")]

            rows = table.find_all("tr")[1:]
            for row in rows:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if len(cells) < 2:
                    continue

                try:
                    entry: dict[str, Any] = {"ticker": ticker}

                    # İlk kolon genellikle yil veya tarih
                    date_str = cells[0]
                    if date_str:
                        # Yil formati: "2024" veya "2024-05-15"
                        if len(date_str) == 4 and date_str.isdigit():
                            entry["payment_year"] = int(date_str)
                        else:
                            for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
                                try:
                                    dt = datetime.strptime(date_str, fmt)
                                    entry["payment_date"] = dt.strftime("%Y-%m-%d")
                                    entry["payment_year"] = dt.year
                                    break
                                except ValueError:
                                    continue

                    def _parse_num(s: str) -> float | None:
                        s = s.replace(",", ".").replace("%", "").replace("TL", "").strip()
                        s = s.replace("\xa0", "").replace(" ", "")
                        if not s or s == "-" or s == "N/A":
                            return None
                        try:
                            return float(s)
                        except ValueError:
                            return None

                    if len(cells) > 1:
                        entry["gross_dividend_per_share"] = _parse_num(cells[1])
                    if len(cells) > 2:
                        entry["net_dividend_per_share"] = _parse_num(cells[2])
                    if len(cells) > 3:
                        entry["dividend_yield_pct"] = _parse_num(cells[3])

                    if entry.get("payment_year"):
                        results.append(entry)
                except Exception:
                    continue

            break

    except httpx.TimeoutException:
        logger.warning("IsYatirim temettu timeout: %s", ticker)
    except Exception as e:
        logger.exception("IsYatirim temettu hatasi %s: %s", ticker, e)
    finally:
        if own_client and client:
            await client.aclose()

    logger.info("IsYatirim temettu: %s — %d kayit cekildi", ticker, len(results))
    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  BATCH ISLEMLER (700+ hisse icin)
# ═══════════════════════════════════════════════════════════════════════════════


async def fetch_bilanco_batch(
    tickers: list[str],
    years: int = 5,
    on_progress: Any = None,
) -> dict[str, list[dict]]:
    """
    Birden fazla hisse icin bilanco verisi ceker (batch).
    Rate limiting ile sirali isler — ~1.5sn/istek x 5 yil = ~7.5sn/hisse.

    700 hisse x 5 call/hisse = 3500 istek, ~87 dakika.

    Args:
        tickers: Hisse kodlari listesi
        years: Kac yillik veri
        on_progress: Optional callback(ticker, index, total) — ilerleme bildirimi

    Returns:
        dict — {ticker: [period_data, ...]}
    """
    results: dict[str, list[dict]] = {}
    total = len(tickers)
    success = 0
    fail = 0

    async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS) as client:
        for i, ticker in enumerate(tickers):
            try:
                data = await fetch_bilanco(ticker, years, client=client)
                if data:
                    results[ticker] = data
                    success += 1
                else:
                    fail += 1
            except Exception as e:
                logger.warning("Batch bilanco hatasi %s: %s", ticker, e)
                fail += 1

            if on_progress:
                try:
                    on_progress(ticker, i + 1, total)
                except Exception:
                    pass

            # Her 50 hissede durum logu
            if (i + 1) % 50 == 0:
                logger.info(
                    "Bilanco batch ilerleme: %d/%d (%d basarili, %d basarisiz)",
                    i + 1, total, success, fail,
                )

    logger.info(
        "Bilanco batch tamamlandi: %d/%d basarili", success, total,
    )
    return results


async def fetch_temettu_batch(
    tickers: list[str],
    on_progress: Any = None,
) -> dict[str, list[dict]]:
    """
    Birden fazla hisse icin temettu verisi ceker (batch).
    Rate limiting ile sirali — ~1.5sn/hisse.

    700 hisse = ~17 dakika.
    """
    results: dict[str, list[dict]] = {}
    total = len(tickers)
    success = 0

    async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS) as client:
        for i, ticker in enumerate(tickers):
            try:
                data = await fetch_temettu_gecmisi(ticker, client=client)
                if data:
                    results[ticker] = data
                    success += 1
            except Exception as e:
                logger.warning("Batch temettu hatasi %s: %s", ticker, e)

            if on_progress:
                try:
                    on_progress(ticker, i + 1, total)
                except Exception:
                    pass

            await asyncio.sleep(_RATE_LIMIT_DELAY)

            if (i + 1) % 50 == 0:
                logger.info("Temettu batch ilerleme: %d/%d", i + 1, total)

    logger.info("Temettu batch tamamlandi: %d/%d basarili", success, total)
    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  BILANCO TETIKLEME — KAP bildirimi gelince aninda o ticker'i guncelle
# ═══════════════════════════════════════════════════════════════════════════════


async def on_bilanco_bildirimi(ticker: str) -> dict | None:
    """
    KAP'ta bilanco bildirimi yakalandi (is_bilanco=TRUE).
    Bu fonksiyon:
    1. IsYatirim'den o ticker'in son bilanco verisini ceker
    2. DB'ye kaydeder (ayri bir fonksiyon)
    3. AI analize gonderilecek veriyi doner

    Scheduler tarafindan cagrilir — kap_all_scraper yeni is_bilanco=TRUE
    kayit kaydettiginde bu tetiklenir.

    Returns:
        dict | None — Son donem bilanco verisi (AI analiz icin)
    """
    logger.info("Bilanco bildirimi tetiklendi: %s", ticker)

    try:
        # Son 1 yilin verisini cek (karsilastirma icin yeterli)
        periods = await fetch_bilanco(ticker, years=2)
        if not periods:
            logger.warning("Bilanco tetikleme: %s icin veri cekilemedi", ticker)
            return None

        logger.info(
            "Bilanco tetikleme basarili: %s — %d donem cekildi",
            ticker, len(periods),
        )
        return {
            "ticker": ticker,
            "periods": periods,
            "latest_period": periods[0] if periods else None,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        logger.exception("Bilanco tetikleme hatasi %s: %s", ticker, e)
        return None


# ═══════════════════════════════════════════════════════════════════════════════
#  YARDIMCI — Tum BIST tickerlarini getir
# ═══════════════════════════════════════════════════════════════════════════════


async def fetch_all_bist_tickers() -> list[str]:
    """
    Tum BIST hisse kodlarini getirir.
    Mevcut BigPara scraper'dan veya DB'den alinabilir.
    Fallback: isyatirim hisse listesi sayfasi.
    """
    # Bu fonksiyon mevcut _refresh_bist_symbols (kap_all_scraper.py) ile
    # ayni kaynak kullanilabilir. Oradan import edilecek.
    try:
        from app.scrapers.kap_all_scraper import _refresh_bist_symbols
        symbols = await _refresh_bist_symbols()
        return sorted(symbols)
    except Exception as e:
        logger.exception("BIST ticker listesi alinamadi: %s", e)
        return []
