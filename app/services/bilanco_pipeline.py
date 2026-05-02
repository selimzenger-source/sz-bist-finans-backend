"""
Bilanco Pipeline Servisi
========================
KAP'ta bilanco bildirimi geldiginde (is_bilanco=TRUE) otomatik tetiklenen akis:

1. KAP scraper yeni is_bilanco=TRUE kayit buldu
2. Bu pipeline tetiklenir → IsYatirim'den detayli bilanco verisi cekilir
3. DB'ye kaydedilir (company_financials tablosu)
4. AI analiz yapilir (ai_bilanco_analyzer.py)
5. Tweet atilir + uygulama bildirimi gonderilir
6. Bilanco sezonu yogunlugu icin queue + rate limit

Ayrica:
- Haftalik batch job: Tum 700+ hisse icin bilanco guncelleme
- Gunluk: Temettu verisi guncelleme
"""

import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Bilanco sezonu yogunluk kontrolu
_bilanco_queue: asyncio.Queue | None = None
_queue_worker_running = False
_MAX_CONCURRENT_ANALYSES = 3  # Ayni anda max AI analiz sayisi


async def _ensure_queue():
    """Lazy queue initialization."""
    global _bilanco_queue
    if _bilanco_queue is None:
        _bilanco_queue = asyncio.Queue(maxsize=500)


# ═══════════════════════════════════════════════════════════════════════════════
#  ANA PIPELINE — Tek bir ticker icin bilanco guncelle + AI analiz + tweet
# ═══════════════════════════════════════════════════════════════════════════════


async def process_bilanco_bildirimi(ticker: str, kap_title: str = ""):
    """
    Tek bir hisse icin bilanco pipeline'i calistirir.
    KAP scraper'dan tetiklenir.

    Args:
        ticker: Hisse kodu
        kap_title: KAP bildirim basligi (tweet icin)
    """
    logger.info("📊 Bilanco pipeline baslatildi: %s — %s", ticker, kap_title)

    try:
        # 0. KAP bildirim iceriginden ANINDA rakamlari parse et (AI ile)
        #    IsYatirim'e veri 1-2 gun sonra duser — biz aninda yakaliyoruz
        kap_parsed = None
        try:
            from app.services.ai_bilanco_analyzer import parse_bilanco_from_kap, save_parsed_bilanco
            # KAP bildirimdeki body'yi al
            from app.database import async_session
            from app.models.kap_all_disclosure import KapAllDisclosure
            from sqlalchemy import select, desc

            async with async_session() as db:
                kap_result = await db.execute(
                    select(KapAllDisclosure)
                    .where(KapAllDisclosure.company_code == ticker, KapAllDisclosure.is_bilanco == True)
                    .order_by(desc(KapAllDisclosure.published_at))
                    .limit(1)
                )
                kap_news = kap_result.scalar_one_or_none()

            if kap_news and kap_news.body:
                kap_parsed = await parse_bilanco_from_kap(ticker, kap_news.body)
                if kap_parsed:
                    await save_parsed_bilanco(ticker, kap_parsed)
                    logger.info("📊 KAP aninda parse OK: %s — Ciro: %s", ticker, kap_parsed.get("revenue"))
        except Exception as kap_err:
            logger.warning("KAP aninda parse hatasi %s: %s", ticker, kap_err)

        # 1. IsYatirim'den bilanco verisi cek (1-2 gun gecikme olabilir)
        from app.scrapers.isyatirim_scraper import on_bilanco_bildirimi
        bilanco_data = await on_bilanco_bildirimi(ticker)

        if not bilanco_data or not bilanco_data.get("periods"):
            if kap_parsed:
                # IsYatirim'de henuz yok ama KAP parse'dan rakamlar var
                logger.info("Bilanco pipeline: %s — IsYatirim bos, KAP parse kullaniliyor", ticker)
                bilanco_data = {"periods": [kap_parsed], "ticker": ticker}
            else:
                logger.warning("Bilanco pipeline: %s icin veri cekilemedi", ticker)
                return

        # 2. DB'ye kaydet (IsYatirim verisi gelirse KAP parse uzerine yazar)
        saved = await _save_bilanco_to_db(ticker, bilanco_data["periods"])
        if not saved:
            logger.warning("Bilanco pipeline: %s DB kaydi basarisiz", ticker)
            return

        # 3. AI analiz
        ai_result = await _run_ai_analysis(ticker, bilanco_data["periods"])

        # 4. Tweet at (opsiyonel — AI sonucu varsa)
        if ai_result:
            await _tweet_bilanco_analysis(ticker, kap_title, ai_result)

        # 5. Bildirim gonder (premium kullanicilara)
        if ai_result:
            await _send_bilanco_notification(ticker, ai_result)

        logger.info("✅ Bilanco pipeline tamamlandi: %s", ticker)

    except Exception as e:
        logger.exception("Bilanco pipeline hatasi %s: %s", ticker, e)


# ═══════════════════════════════════════════════════════════════════════════════
#  QUEUE WORKER — Bilanco sezonu yogunlugu icin sirayla isle
# ═══════════════════════════════════════════════════════════════════════════════


async def enqueue_bilanco(ticker: str, kap_title: str = ""):
    """Bilanco islemini queue'ya ekler. Queue worker isleyecek."""
    await _ensure_queue()
    await _bilanco_queue.put((ticker, kap_title))
    logger.info("Bilanco queue'ya eklendi: %s (queue size: %d)", ticker, _bilanco_queue.qsize())


async def start_bilanco_queue_worker():
    """
    Queue worker — queue'daki bilanco islemlerini sirayla isler.
    Scheduler baslangicinda bir kez cagrilir.
    Bilanco sezonu (Mart-Nisan) yogunlugu icin tasarlanmistir.
    """
    global _queue_worker_running
    if _queue_worker_running:
        return

    _queue_worker_running = True
    await _ensure_queue()
    logger.info("Bilanco queue worker baslatildi")

    while _queue_worker_running:
        try:
            ticker, kap_title = await asyncio.wait_for(
                _bilanco_queue.get(), timeout=60
            )
            await process_bilanco_bildirimi(ticker, kap_title)
            _bilanco_queue.task_done()

            # Istekler arasi 5 sn bekleme (IsYatirim rate limit)
            await asyncio.sleep(5)

        except asyncio.TimeoutError:
            continue  # Queue bos, bekle
        except Exception as e:
            logger.exception("Bilanco queue worker hatasi: %s", e)
            await asyncio.sleep(10)


# ═══════════════════════════════════════════════════════════════════════════════
#  HAFTALIK BATCH JOB — Tum hisselerin bilancosunu guncelle
# ═══════════════════════════════════════════════════════════════════════════════


async def weekly_bilanco_update():
    """
    Haftalik batch: Tum BIST hisseleri icin 2015'ten itibaren bilanco verisi gunceller.
    Pazar gecesi calistirilmasi onerilir.
    Tahmini sure: ~3-4 saat (700+ hisse x 11 yil x 1.5sn) — ilk calisma uzun, sonrakiler sadece yeni donem
    """
    logger.info("📊 Haftalik bilanco batch baslatildi")
    start_time = datetime.now(timezone.utc)

    try:
        from app.scrapers.isyatirim_scraper import fetch_all_bist_tickers, fetch_bilanco

        tickers = await fetch_all_bist_tickers()
        if not tickers:
            logger.error("Haftalik bilanco: Ticker listesi alinamadi")
            return

        logger.info("Haftalik bilanco: %d hisse islenecek", len(tickers))

        success = 0
        fail = 0

        async with __import__("httpx").AsyncClient(
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0",
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/default.aspx",
            },
        ) as client:
            for i, ticker in enumerate(tickers):
                try:
                    periods = await fetch_bilanco(ticker, years=11, client=client)
                    if periods:
                        await _save_bilanco_to_db(ticker, periods)
                        success += 1
                    else:
                        fail += 1
                except Exception as e:
                    logger.warning("Haftalik bilanco %s hatasi: %s", ticker, e)
                    fail += 1

                if (i + 1) % 50 == 0:
                    elapsed = (datetime.now(timezone.utc) - start_time).seconds
                    logger.info(
                        "Haftalik bilanco: %d/%d tamamlandi (%d basarili, %d basarisiz) — %d sn",
                        i + 1, len(tickers), success, fail, elapsed,
                    )

        elapsed = (datetime.now(timezone.utc) - start_time).seconds
        logger.info(
            "✅ Haftalik bilanco batch tamamlandi: %d/%d basarili — %d sn",
            success, len(tickers), elapsed,
        )

    except Exception as e:
        logger.exception("Haftalik bilanco batch hatasi: %s", e)


async def daily_temettu_update():
    """
    Gunluk batch: Tum BIST hisseleri icin temettu verisi gunceller.
    Tahmini sure: ~17 dakika (700 hisse x 1.5sn)
    """
    logger.info("💰 Gunluk temettu batch baslatildi")

    try:
        from app.scrapers.isyatirim_scraper import fetch_all_bist_tickers, fetch_temettu_gecmisi

        tickers = await fetch_all_bist_tickers()
        success = 0

        async with __import__("httpx").AsyncClient(
            timeout=30, headers={"User-Agent": "Mozilla/5.0 Chrome/131.0.0.0"}
        ) as client:
            for i, ticker in enumerate(tickers):
                try:
                    data = await fetch_temettu_gecmisi(ticker, client=client)
                    if data:
                        await _save_temettu_to_db(ticker, data)
                        success += 1
                except Exception:
                    pass
                await asyncio.sleep(1.5)

                if (i + 1) % 100 == 0:
                    logger.info("Gunluk temettu: %d/%d", i + 1, len(tickers))

        logger.info("✅ Gunluk temettu batch: %d/%d basarili", success, len(tickers))

    except Exception as e:
        logger.exception("Gunluk temettu batch hatasi: %s", e)


# ═══════════════════════════════════════════════════════════════════════════════
#  YARDIMCI FONKSIYONLAR
# ═══════════════════════════════════════════════════════════════════════════════


async def _save_bilanco_to_db(ticker: str, periods: list[dict]) -> bool:
    """Bilanco verilerini company_financials tablosuna kaydet (UPSERT)."""
    try:
        from app.database import async_session
        from app.models.company_financial import CompanyFinancial
        from sqlalchemy import select
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        async with async_session() as db:
            for p in periods:
                # Upsert — ayni ticker+period varsa guncelle
                stmt = select(CompanyFinancial).where(
                    CompanyFinancial.ticker == ticker,
                    CompanyFinancial.period == p["period"],
                )
                existing = (await db.execute(stmt)).scalar_one_or_none()

                if existing:
                    # Guncelle
                    for field in [
                        "revenue", "gross_profit", "operating_profit", "net_income",
                        "ebitda", "total_assets", "total_equity", "total_debt",
                        "net_debt", "cash_and_equivalents", "current_ratio",
                        "gross_margin_pct", "net_margin_pct", "roe_pct",
                        "debt_to_equity",
                    ]:
                        val = p.get(field)
                        if val is not None:
                            setattr(existing, field, val)
                    existing.updated_at = datetime.now(timezone.utc)
                else:
                    # Yeni kayit
                    new_record = CompanyFinancial(
                        ticker=ticker,
                        period=p["period"],
                        period_end_date=datetime.strptime(p["period_end_date"], "%Y-%m-%d") if p.get("period_end_date") else None,
                        revenue=p.get("revenue"),
                        gross_profit=p.get("gross_profit"),
                        operating_profit=p.get("operating_profit"),
                        net_income=p.get("net_income"),
                        ebitda=p.get("ebitda"),
                        total_assets=p.get("total_assets"),
                        total_equity=p.get("total_equity"),
                        total_debt=p.get("total_debt"),
                        net_debt=p.get("net_debt"),
                        cash_and_equivalents=p.get("cash_and_equivalents"),
                        current_ratio=p.get("current_ratio"),
                        gross_margin_pct=p.get("gross_margin_pct"),
                        net_margin_pct=p.get("net_margin_pct"),
                        roe_pct=p.get("roe_pct"),
                        debt_to_equity=p.get("debt_to_equity"),
                        source="isyatirim",
                    )
                    db.add(new_record)

            await db.commit()
            logger.info("DB kayit: %s — %d donem", ticker, len(periods))
            return True

    except Exception as e:
        logger.exception("DB bilanco kayit hatasi %s: %s", ticker, e)
        return False


async def _save_temettu_to_db(ticker: str, data: list[dict]) -> bool:
    """Temettu verilerini dividend_history tablosuna kaydet (UPSERT)."""
    try:
        from app.database import async_session
        from app.models.dividend import DividendHistory
        from sqlalchemy import select

        async with async_session() as db:
            for d in data:
                year = d.get("payment_year")
                if not year:
                    continue

                stmt = select(DividendHistory).where(
                    DividendHistory.ticker == ticker,
                    DividendHistory.payment_year == year,
                )
                existing = (await db.execute(stmt)).scalar_one_or_none()

                if existing:
                    if d.get("gross_dividend_per_share") is not None:
                        existing.gross_dividend_per_share = d["gross_dividend_per_share"]
                    if d.get("net_dividend_per_share") is not None:
                        existing.net_dividend_per_share = d.get("net_dividend_per_share")
                    if d.get("payment_date"):
                        existing.payment_date = d["payment_date"]
                else:
                    new_record = DividendHistory(
                        ticker=ticker,
                        payment_year=year,
                        gross_dividend_per_share=d.get("gross_dividend_per_share"),
                        payment_date=d.get("payment_date"),
                    )
                    db.add(new_record)

            await db.commit()
            return True

    except Exception as e:
        logger.exception("DB temettu kayit hatasi %s: %s", ticker, e)
        return False


async def _run_ai_analysis(ticker: str, periods: list[dict]) -> dict | None:
    """Bilanco verilerini AI ile analiz et."""
    try:
        from app.services.ai_bilanco_analyzer import analyze_bilanco
        return await analyze_bilanco(ticker, periods)
    except Exception as e:
        logger.exception("AI bilanco analiz hatasi %s: %s", ticker, e)
        return None


async def _tweet_bilanco_analysis(ticker: str, kap_title: str, ai_result: dict):
    """Bilanco AI analizini Fintables X tarzinda Twitter'a tweet at."""
    try:
        summary = ai_result.get("summary", "")
        health_label = ai_result.get("overall_health_label", "")
        health_score = ai_result.get("overall_health_score", "")
        revenue_change = ai_result.get("revenue_change_pct", "")
        net_income_change = ai_result.get("net_income_change_pct", "")
        period = ai_result.get("period", "")

        # Fintables tarzi compact tweet
        lines = [f"📊 #{ticker} Bilanço Analizi — {period}"]

        if health_label and health_score:
            score_emoji = "🟢" if float(health_score) >= 7 else "🟡" if float(health_score) >= 5 else "🔴"
            lines.append(f"{score_emoji} Sağlık: {health_label} ({health_score}/10)")

        if revenue_change:
            rev_emoji = "📈" if not str(revenue_change).startswith("-") else "📉"
            lines.append(f"{rev_emoji} Satışlar: {revenue_change}")
        if net_income_change:
            ni_emoji = "📈" if not str(net_income_change).startswith("-") else "📉"
            lines.append(f"{ni_emoji} Net Kâr: {net_income_change}")

        if summary:
            lines.append(f"\n💡 {summary[:180]}")

        lines.append(f"\n#BIST #{ticker} #Bilanço")
        lines.append("🤖 AI analiz — yatırım tavsiyesi değildir.")

        tweet_text = "\n".join(lines)

        # Gercek tweet gonderimi
        from app.services.twitter_service import _safe_tweet
        result = _safe_tweet(tweet_text, source="bilanco_ai")

        if result:
            logger.info("✅ Bilanco tweet gonderildi: %s", ticker)
        else:
            logger.warning("Bilanco tweet gonderilemedi: %s", ticker)

    except Exception as e:
        logger.warning("Tweet hatasi %s: %s", ticker, e)


async def _send_bilanco_notification(ticker: str, ai_result: dict):
    """Bilanco+Temettu veya Diamond abonelerine push bildirim gonder."""
    try:
        from app.services.notification import send_topic_notification

        health_label = ai_result.get("overall_health_label", "")
        health_score = ai_result.get("overall_health_score", "")
        period = ai_result.get("period", "")

        title = f"📊 {ticker} Bilanço Açıklandı"
        body = f"{period} — {health_label} ({health_score}/10)"

        # Bilanco+Temettu ve Diamond abonelerine bildirim
        # Topic: bilanco_ai (bu topic'e abone olan cihazlara gider)
        await send_topic_notification(
            topic="bilanco_ai",
            title=title,
            body=body,
            data={
                "type": "bilanco",
                "ticker": ticker,
                "screen": f"/bilanco-analizi?ticker={ticker}",
            },
        )
        logger.info("✅ Bilanco bildirimi gonderildi: %s", ticker)

    except Exception as e:
        logger.warning("Bildirim hatasi %s: %s", ticker, e)
