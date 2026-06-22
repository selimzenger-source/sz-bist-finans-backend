"""temettuhisseleri.com scraper.

Veri kaynagi: https://temettuhisseleri.com/backend/gethisseanaliz.php
Tum BIST hisselerinin temettu odeme gecmisi, payout ratio, forecast ve sektor bilgisini ceker.

Cikti tablolari:
  - dividends (mevcut beklenti — overwrite/upsert)
  - dividend_history (her odeme satiri — upsert)
"""

import asyncio
import logging
from datetime import date, datetime
from decimal import Decimal

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.database import async_session
from app.models.dividend import Dividend, DividendHistory

logger = logging.getLogger(__name__)

BASE_URL = "https://temettuhisseleri.com"
STOCKS_URL = f"{BASE_URL}/backend/getstocks.php"
ANALYSIS_URL = f"{BASE_URL}/backend/gethisseanaliz.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://temettuhisseleri.com/hisseanaliz/",
    "X-Requested-With": "XMLHttpRequest",
}


async def fetch_stocks_list(client: httpx.AsyncClient) -> list[dict]:
    """Tum BIST hisselerinin temettuhisseleri.com'daki listesini cek."""
    resp = await client.post(STOCKS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


async def fetch_stock_analysis(client: httpx.AsyncClient, ticker: str) -> dict | None:
    """Tek hisse icin tum analiz verilerini cek."""
    try:
        resp = await client.post(
            ANALYSIS_URL,
            data={"ticker": ticker},
            headers=HEADERS,
            timeout=20,
        )
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception as e:
        logger.warning("temettuhisseleri analiz hatasi (%s): %s", ticker, e)
        return None


def _safe_decimal(val) -> Decimal | None:
    """String/numeric -> Decimal donusumu. Bos/None ise None."""
    if val is None or val == "" or val == "0.00":
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None


def _parse_payment_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(int(year), int(month), int(day))
    except Exception:
        return None


async def _ticker_no_withholding(session, ticker: str) -> bool:
    """GYO / GSYO hisselerinde temettü STOPAJI YOK (net = brüt).

    Sektör tablosundan tespit eder ('Gayrimenkul Y.O.' / 'Girişim Sermayesi Y.O.').
    Sektör bulunamazsa ticker suffix'i (GYO) ile yedek tahmin.
    """
    try:
        from sqlalchemy import text as _t
        r = await session.execute(
            _t("SELECT sector_name FROM stock_sectors WHERE ticker = :tk LIMIT 1"),
            {"tk": (ticker or "").upper()},
        )
        s = (r.scalar() or "").lower()
        if s:
            return ("gayrimenkul" in s) or ("girişim sermayesi" in s) or ("girisim sermayesi" in s)
    except Exception:
        pass
    return (ticker or "").upper().endswith("GYO")


async def upsert_dividend_history(session, ticker: str, payments: list[dict], company_name: str | None = None) -> int:
    """Her odeme kaydini dividend_history'ye upsert et.

    Anahtar: (ticker, payment_year, payment_date) — ayni yilda 2 odeme olabilir,
    bu yuzden tarih de anahtar parcasi.
    """
    # Turkiye stopaj orani: 2024 sonrasi BIST hisseleri icin %15 (eski %10).
    # Net = Brut * 0.85
    # ★ GYO / GSYO İSTİSNASI (kullanıcı/Twitter bildirimi): Gayrimenkul Yatırım
    # Ortaklığı ve Girişim Sermayesi YO temettülerinde STOPAJ YOKTUR → Net = Brüt.
    # Eskiden hepsine %15 uygulanıyordu (AAGYO 0,43 brüt → yanlış 0,36 net).
    # Sektör bazlı tespit (ticker suffix güvenilmez — BASGZ de GYO).
    _no_stopaj = await _ticker_no_withholding(session, ticker)
    WITHHOLDING_RATE = Decimal("1.0") if _no_stopaj else Decimal("0.85")

    inserted = 0
    for p in payments:
        try:
            year = int(p.get("year") or 0)
            if not year:
                continue
            payment_dt = _parse_payment_date(year, p.get("month") or 1, p.get("day") or 1)
            yield_pct = _safe_decimal(p.get("amount"))  # "amount" verim yuzdesi
            per_share = _safe_decimal(p.get("perstock"))  # TL/hisse brut
            payout = _safe_decimal(p.get("payoutratio"))
            # Net hesabi (stopaj sonrasi)
            net_per_share = (per_share * WITHHOLDING_RATE).quantize(Decimal("0.0001")) if per_share is not None else None

            # Mevcut kayit var mi? Once tam eslesen (year+date) ara, yoksa
            # year+null aramasi yap (eski kayitlari guncellemek icin).
            row = None
            if payment_dt is not None:
                exact = await session.execute(
                    select(DividendHistory).where(
                        DividendHistory.ticker == ticker,
                        DividendHistory.payment_year == year,
                        DividendHistory.payment_date == payment_dt,
                    )
                )
                row = exact.scalars().first()
                if not row:
                    # Tarih null olan eski kaydi bul (ayni yil) — eski scrape'lerden kalmis olabilir
                    legacy = await session.execute(
                        select(DividendHistory).where(
                            DividendHistory.ticker == ticker,
                            DividendHistory.payment_year == year,
                            DividendHistory.payment_date.is_(None),
                        )
                    )
                    row = legacy.scalars().first()
            else:
                null_match = await session.execute(
                    select(DividendHistory).where(
                        DividendHistory.ticker == ticker,
                        DividendHistory.payment_year == year,
                        DividendHistory.payment_date.is_(None),
                    )
                )
                row = null_match.scalars().first()

            if row:
                # ALANLARI SET ET (None gelirse mevcut deger korunur)
                if yield_pct is not None:
                    row.dividend_yield_pct = yield_pct
                if per_share is not None:
                    row.gross_dividend_per_share = per_share
                if net_per_share is not None:
                    row.net_dividend_per_share = net_per_share
                if payout is not None:
                    row.payout_ratio = payout
                if payment_dt is not None and row.payment_date != payment_dt:
                    row.payment_date = payment_dt  # KRITIK: tarihi her zaman guncelle
                row.source = "temettuhisseleri"
            else:
                new_row = DividendHistory(
                    ticker=ticker,
                    payment_year=year,
                    gross_dividend_per_share=per_share,
                    net_dividend_per_share=net_per_share,
                    dividend_yield_pct=yield_pct,
                    payout_ratio=payout,
                    payment_date=payment_dt,
                    source="temettuhisseleri",
                )
                session.add(new_row)
            inserted += 1
        except Exception as e:
            logger.debug("upsert_dividend_history satir hatasi: %s", e)
            continue

    # ★ BAYAT KAYIT TEMİZLİĞİ (AVPGY bug'ı): kaynak bir ödemenin tarihini REVİZE
    # edince (örn. AVPGY Haz 24 → Haz 5) eski tarih için ayrı kayıt açılıyor ama
    # silinmiyordu → bayat tarih "önümüzdeki hafta ödeyecek"te kalıyordu. Bu scrape'te
    # GELEN tarihler doğrudur; aynı (ticker, yıl) için temettuhisseleri kaynaklı olup
    # bu turda GELMEYEN kayıtlar bayattır → silinir. (KAP/diğer kaynaklara dokunma.)
    try:
        from collections import defaultdict
        fresh_by_year: dict[int, set] = defaultdict(set)
        for p in payments:
            try:
                _y = int(p.get("year") or 0)
                _d = _parse_payment_date(_y, p.get("month") or 1, p.get("day") or 1)
                if _y and _d is not None:
                    fresh_by_year[_y].add(_d)
            except Exception:
                continue
        for _y, _dates in fresh_by_year.items():
            if not _dates:
                continue
            stale = await session.execute(
                select(DividendHistory).where(
                    DividendHistory.ticker == ticker,
                    DividendHistory.payment_year == _y,
                    DividendHistory.source == "temettuhisseleri",
                    DividendHistory.payment_date.isnot(None),
                    DividendHistory.payment_date.notin_(list(_dates)),
                )
            )
            for _srow in stale.scalars().all():
                logger.info("Bayat temettü kaydı silindi: %s %s (yeni tarihler: %s)",
                            ticker, _srow.payment_date, sorted(_dates))
                await session.delete(_srow)
    except Exception as _ce:
        logger.debug("Bayat temettü temizlik hatası (%s): %s", ticker, _ce)

    return inserted


async def upsert_dividend_summary(
    session, ticker: str, name: str | None,
    forecast_pct: float | None, fk: float | None, pddd: float | None,
):
    """dividends tablosuna ozet bilgi yaz/guncelle."""
    existing = await session.execute(select(Dividend).where(Dividend.ticker == ticker))
    row = existing.scalar_one_or_none()

    if row:
        if name and not row.company_name:
            row.company_name = name
        if forecast_pct is not None:
            row.expected_dividend_yield_pct = Decimal(str(round(forecast_pct, 2)))
            row.expected_year = datetime.now().year
        if fk is not None:
            row.fk = Decimal(str(round(fk, 2)))
        if pddd is not None:
            row.pd_dd = Decimal(str(round(pddd, 2)))
        row.source = "temettuhisseleri"
    else:
        session.add(Dividend(
            ticker=ticker,
            company_name=name,
            expected_dividend_yield_pct=Decimal(str(round(forecast_pct, 2))) if forecast_pct is not None else None,
            expected_year=datetime.now().year if forecast_pct is not None else None,
            fk=Decimal(str(round(fk, 2))) if fk is not None else None,
            pd_dd=Decimal(str(round(pddd, 2))) if pddd is not None else None,
            source="temettuhisseleri",
        ))


async def scrape_temettuhisseleri(limit: int | None = None) -> dict:
    """Ana scraper — tum BIST hisselerini temettuhisseleri.com'dan ceker.

    Args:
        limit: Test icin ilk N hisse (None = hepsi).

    Returns:
        {"stocks_total": int, "processed": int, "history_rows": int, "errors": int}
    """
    stats = {"stocks_total": 0, "processed": 0, "history_rows": 0, "errors": 0}

    # Hisse evreni: BIST resmi CSV listesi (stock_markets tablosu) — her zaman guncel,
    # sabit degil. hisse_endeks_ds.csv'den bist_market_segment_scraper ile beslenir.
    # Boylece yeni kote olan / cikan hisseler otomatik kapsanir.
    stocks: list[dict] = []
    try:
        from app.models.stock_market import StockMarket
        async with async_session() as s:
            rows = (await s.execute(select(StockMarket.ticker, StockMarket.company_name))).all()
        stocks = [{"ticker": r[0], "name": r[1] or ""} for r in rows if r[0]]
        logger.info("temettu: BIST CSV listesinden %d hisse alindi", len(stocks))
    except Exception as e:
        logger.warning("temettu: StockMarket listesi alinamadi (%s), temettuhisseleri listesine dusuluyor", e)

    async with httpx.AsyncClient(http2=False, headers=HEADERS) as client:
        # Fallback: StockMarket bossa temettuhisseleri'nin kendi listesi
        if not stocks:
            try:
                stocks = await fetch_stocks_list(client)
            except Exception as e:
                logger.error("temettuhisseleri stocks listesi alinamadi: %s", e)
                return {**stats, "error": str(e)[:200]}

        stats["stocks_total"] = len(stocks)
        if limit:
            stocks = stocks[:limit]

        logger.info("temettuhisseleri: %d hisse islenecek", len(stocks))

        for i, stock in enumerate(stocks, 1):
            ticker = stock.get("ticker", "").upper().strip()
            name = stock.get("name", "").strip()
            if not ticker:
                continue

            data = await fetch_stock_analysis(client, ticker)
            if not data:
                stats["errors"] += 1
                await asyncio.sleep(0.5)
                continue

            try:
                async with async_session() as session:
                    payments = data.get("dividendslist") or []
                    rows = await upsert_dividend_history(session, ticker, payments, name)
                    stats["history_rows"] += rows

                    forecast = (data.get("dividendforecast") or {}).get("forecast")
                    bookvalue = data.get("bookvalue") or {}
                    earning = data.get("earning") or []
                    fk_val = None  # Bu site F/K vermiyor direkt — None birak
                    pddd_val = None
                    try:
                        if isinstance(bookvalue, dict) and "ratio" in bookvalue:
                            pddd_val = float(bookvalue["ratio"])
                    except Exception:
                        pass

                    await upsert_dividend_summary(session, ticker, name, forecast, fk_val, pddd_val)
                    await session.commit()
                    stats["processed"] += 1
            except Exception as e:
                logger.warning("temettuhisseleri DB hata (%s): %s", ticker, e)
                stats["errors"] += 1

            # Rate limit — site cidden istek limiti yok ama nazik ol
            if i % 50 == 0:
                logger.info("temettuhisseleri: %d/%d islendi", i, len(stocks))
                await asyncio.sleep(1.0)
            else:
                await asyncio.sleep(0.2)

    logger.info("temettuhisseleri tamamlandi: %s", stats)
    return stats


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    n = int(sys.argv[1]) if len(sys.argv) > 1 else None
    asyncio.run(scrape_temettuhisseleri(limit=n))
