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

★★★ BILANCO PIPELINE GEÇİCİ KAPALI ★★★
Mobil uygulamada Bilanço tab'ı şu an gizli (yasal hazırlık dönemi).
KAP bildirimi geldiğinde pipeline tetiklenmesin, AI analizi üretilmesin,
DB'ye boş yazma yapılmasın diye BILANCO_PIPELINE_ENABLED=False.
İleride aktif etmek için bu flag'i True yap.
"""

import asyncio
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# v3.2 Bilanço pipeline — AÇIK (kontrollu).
# 27.05.2026 parity testi: KLGYO/EREGL/AKBNK/ANSGR Q1 2026 — 35/37 alan %1 alti fark,
# 0 isaret hatasi, 0 olcek hatasi (10x/100x/1000x YOK). Q1 icin parser DOGRU.
#
# Eklenen safeguard'lar:
# 1) YTD->Q donusumu (ai_bilanco_analyzer._convert_ytd_to_net_quarter)
#    — Q2/Q3/Q4 gelir tablosu kalemleri onceki YTD'lerden cikarilir.
# 2) Sigorta technical_balance: brut toplam yerine NET teknik bolum dengesi
#    (kap-fr_TechnicalBalance veya NonlifeTechnicalSectionBalance+LifeTechnicalSectionBalance).
# 3) save_parsed_bilanco senaryo 1 (IsYatirim mevcut): yalniz NULL/0 alanlari enrich eder,
#    mevcut dogru veriyi EZMEZ. Yani xlsx import veya IsYatirim verisi guvendedir.
BILANCO_PIPELINE_ENABLED = True

# Yalnizca bu sektorlerden gelen bilancolar pipeline'a girer.
# Insurance: technical_balance fix sonrasi guvenli oldu — kapsama dahil.
# Bank: Parser xlsx'ten daha iyi cikariyor (deposits/loans/NII) — kapsama dahil.
# Industrial: Default, en cok test edildi.
BILANCO_ALLOWED_SECTORS = ("industrial", "bank", "insurance")

# Tweet kill-switch — defence-in-depth, ek koruma.
BILANCO_TWEET_ENABLED = False

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


async def process_bilanco_bildirimi(ticker: str, kap_title: str = "", force: bool = False):
    """
    Tek bir hisse icin bilanco pipeline'i calistirir.
    KAP scraper'dan tetiklenir.

    Args:
        ticker: Hisse kodu
        kap_title: KAP bildirim basligi (tweet icin)
        force: True ise idempotent dedup atla (admin test icin)
    """
    if not BILANCO_PIPELINE_ENABLED:
        logger.info(
            "📊 Bilanco pipeline ATLANDI (kapatildi): %s — %s — flag BILANCO_PIPELINE_ENABLED=False",
            ticker, kap_title,
        )
        return

    # ★ IDEMPOTENT DEDUP — son saat icinde aynı hisse için pipeline calıştırıldıysa skip
    # Sebep: KAP "Finansal Durum Tablosu" + "Sorumluluk Beyanı" + "Faaliyet Raporu"
    # 3 ayrı KAP olarak gelir; üçünde de is_bilanco=True flag var. Üçü ardı arda
    # pipeline'ı tetikler — sadece ilki işlensin yeterli.
    if not force:
        try:
            from datetime import timedelta as _td2
            from app.database import async_session as _as
            from app.models.company_financial import CompanyFinancial as _CF
            from sqlalchemy import select as _sel, desc as _desc
            async with _as() as _db:
                _r = await _db.execute(
                    _sel(_CF).where(_CF.ticker == ticker)
                    .order_by(_desc(_CF.scraped_at)).limit(1)
                )
                _last = _r.scalar_one_or_none()
                if _last and _last.scraped_at:
                    age = datetime.now(timezone.utc) - _last.scraped_at
                    if age < _td2(hours=1):
                        logger.info(
                            "📊 Bilanco pipeline SKIP (dedup): %s — son işleme %d dakika önce",
                            ticker, int(age.total_seconds() / 60),
                        )
                        return
        except Exception as _e:
            logger.debug("Dedup kontrolu hata (%s): %s — devam", ticker, _e)

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
                # ENJSA gibi tickerlarda Sorumluluk + Faaliyet + Özkaynaklar gelir,
                # ama gerçek "Finansal Durum Tablosu (Bilanço)" KAP'ı kayıp olabilir.
                # Bu yüzden son 7 gündeki TÜM is_bilanco=True KAP'lara bakıp
                # XBRL içereni bulana kadar dene.
                from datetime import timedelta as _td
                cutoff = datetime.now(timezone.utc) - _td(days=7)
                kap_result = await db.execute(
                    select(KapAllDisclosure)
                    .where(KapAllDisclosure.company_code == ticker)
                    .where(KapAllDisclosure.is_bilanco == True)
                    .where(KapAllDisclosure.published_at >= cutoff)
                    .order_by(desc(KapAllDisclosure.published_at))
                    .limit(10)
                )
                kap_news_list = list(kap_result.scalars().all())

            from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
            import gc
            # Birden fazla KAP body'sini MERGE et — bilanco bildirimi 3 ayri parcaya bolunmus
            # olabilir (Bilanco / Kar-Zarar / Nakit Akis). Hepsini gez, ayni period icin
            # NULL olmayan alanlari biriktir.
            #
            # KRITIK FIX: Onceki sistem published_at desc sirayla ilk gelen parsed'i
            # MASTER yapiyordu — ama Q4 duzeltme bildirimi Q1'den sonra gelmis ise
            # baz Q4 oluyor, Q1 atiliyordu (period mismatch). Cozum: ONCE TUM body'leri
            # parse et, sonra **en yeni period'a** sahip olanlari merge et.
            parsed_list: list[tuple[str, dict, str]] = []  # (period, parsed, title)
            for kap_news in kap_news_list:
                body_full = kap_news.body or ""
                if (not body_full or len(body_full) < 500) and kap_news.kap_url:
                    try:
                        disclosure = await fetch_kap_disclosure(kap_news.kap_url)
                        if disclosure and disclosure.get("full_text"):
                            body_full = disclosure["full_text"]
                        del disclosure
                    except Exception as fe:
                        logger.debug("KAP body fetch hata %s: %s", ticker, fe)
                        continue

                if not body_full:
                    continue

                parsed = await parse_bilanco_from_kap(ticker, body_full)
                del body_full
                if not parsed:
                    continue
                if not (parsed.get("total_assets") or parsed.get("revenue") or
                        parsed.get("net_income") or parsed.get("total_equity")):
                    del parsed
                    continue

                period = parsed.get("period") or ""
                parsed_list.append((period, parsed, kap_news.title[:40]))

            # En yeni period'a sahip parsed'leri filtrele (ornek: tum 2026-Q1'ler)
            merged: dict | None = None
            tried_titles: list[str] = []
            if parsed_list:
                # Period string'i lexicographic siralanir: "2026-Q1" > "2025-Q4"
                parsed_list.sort(key=lambda x: x[0], reverse=True)
                target_period = parsed_list[0][0]
                logger.info("📊 KAP merge: %s — target period = %s (toplam %d KAP)",
                            ticker, target_period, len(parsed_list))
                for period, parsed, title in parsed_list:
                    if period != target_period:
                        continue  # eski period bildirimi (Q4 duzeltme vs.) atla
                    tried_titles.append(title)
                    if merged is None:
                        merged = dict(parsed)
                    else:
                        for k, v in parsed.items():
                            if v is None:
                                continue
                            if merged.get(k) is None:
                                merged[k] = v
                    if all(merged.get(k) is not None for k in (
                        "revenue", "net_income", "total_assets", "total_equity"
                    )):
                        break

            best_parsed = merged
            if best_parsed:
                logger.info("📊 KAP XBRL merged: %s — %d KAP'tan birlesti (%s)",
                            ticker, len(tried_titles), ", ".join(tried_titles))
                gc.collect()

            if best_parsed:
                kap_parsed = best_parsed
                # kap_news_list (10 obje) artik gereksiz — save oncesi bosalt
                del kap_news_list
                gc.collect()
                await save_parsed_bilanco(ticker, kap_parsed)
                logger.info("📊 KAP aninda parse OK: %s — Ciro: %s, Varlik: %s",
                            ticker, kap_parsed.get("revenue"), kap_parsed.get("total_assets"))
            else:
                logger.warning("📊 KAP XBRL bulunamadi (%d KAP denendi): %s", len(kap_news_list), ticker)
                # FALLBACK: telegram_news tablosundan ticker'in son matriks_id'lerini al
                # ve TradingView uzerinden direkt KAP URL'leri bul. Telegram poller atlamis
                # KAP'lara erisim icin (ENJSA/KAREL gibi multi-burst KAP'larda kayip mesajlar).
                try:
                    from app.models.telegram_news import TelegramNews
                    from sqlalchemy import select as _sel, desc as _desc
                    async with async_session() as db2:
                        # Ticker'in son 24 saat icindeki tum telegram mesajlarini al (en yeniden eskiye)
                        cutoff_tg = datetime.now(timezone.utc) - _td(hours=24)
                        tg_result = await db2.execute(
                            _sel(TelegramNews)
                            .where(TelegramNews.ticker == ticker)
                            .where(TelegramNews.created_at >= cutoff_tg)
                            .order_by(_desc(TelegramNews.created_at))
                            .limit(20)
                        )
                        tg_msgs = list(tg_result.scalars().all())

                    # Her telegram mesaji icin matriks_id varsa → TradingView'dan KAP URL ara
                    from app.services.ai_news_scorer import fetch_tradingview_content
                    for msg in tg_msgs:
                        if not msg.matriks_id:
                            continue
                        try:
                            tv = await fetch_tradingview_content(msg.matriks_id)
                            tv_kap_url = tv.get("real_kap_url") if tv else None
                            if not tv_kap_url:
                                continue
                            # Bu KAP URL'sini fetch + parse et
                            disc = await fetch_kap_disclosure(tv_kap_url)
                            if not disc or not disc.get("full_text"):
                                continue
                            parsed = await parse_bilanco_from_kap(ticker, disc["full_text"])
                            if parsed and (parsed.get("total_assets") or parsed.get("revenue")):
                                await save_parsed_bilanco(ticker, parsed)
                                logger.info("📊 TradingView fallback OK: %s — matriks=%s → %s (Ciro: %s)",
                                            ticker, msg.matriks_id, tv_kap_url, parsed.get("revenue"))
                                kap_parsed = parsed
                                break
                        except Exception as tv_err:
                            logger.debug("TradingView fallback hata %s/%s: %s", ticker, msg.matriks_id, tv_err)
                            continue
                except Exception as fb_err:
                    logger.warning("KAP fallback hatasi (%s): %s", ticker, fb_err)
        except Exception as kap_err:
            logger.warning("KAP aninda parse hatasi %s: %s", ticker, kap_err)

        # 1. SADECE KAP body parse — IsYatirim KAPATILDI (sadece DB seed icindi)
        # KAP'tan Cari Donem tarihinden quarter cikar, o donemi DB'ye yaz.
        if not kap_parsed:
            logger.warning("Bilanco pipeline: %s — KAP parse fail, atlanacak", ticker)
            return

        # save_parsed_bilanco() yukarda zaten cagriildi (line 118), o yuzden DB'de var.
        # AI analiz icin DB'den son 8 cevreklik veriyi okuyup karsılastır.
        from app.models.company_financial import CompanyFinancial
        from sqlalchemy import select as _sel_cf, desc as _desc_cf
        async with async_session() as db:
            recent_q = (await db.execute(
                _sel_cf(CompanyFinancial).where(CompanyFinancial.ticker == ticker)
                .order_by(_desc_cf(CompanyFinancial.period)).limit(8)
            )).scalars().all()
        bilanco_data = {"ticker": ticker, "periods": [
            {
                "period": p.period,
                "period_end_date": p.period_end_date.isoformat() if p.period_end_date else None,
                "revenue": float(p.revenue) if p.revenue else None,
                "gross_profit": float(p.gross_profit) if p.gross_profit else None,
                "operating_profit": float(p.operating_profit) if p.operating_profit else None,
                "net_income": float(p.net_income) if p.net_income else None,
                "ebitda": float(p.ebitda) if p.ebitda else None,
                "total_assets": float(p.total_assets) if p.total_assets else None,
                "current_assets": float(p.current_assets) if p.current_assets else None,
                "non_current_assets": float(p.non_current_assets) if p.non_current_assets else None,
                "total_equity": float(p.total_equity) if p.total_equity else None,
                "total_debt": float(p.total_debt) if p.total_debt else None,
                "net_debt": float(p.net_debt) if p.net_debt else None,
                "cash_and_equivalents": float(p.cash_and_equivalents) if p.cash_and_equivalents else None,
            }
            for p in recent_q
        ]}

        # 2. ★ AI TOKEN KALKANI — eksik parse'a AI harcama
        # En yeni dönem bilanço-tamlık kontrolü: total_assets + total_equity + (gelir kalemi).
        # Eksikse → AI ÇALIŞTIRMA (token israfı + bilançosuz yanlış puan önlenir),
        # admin'e "manuel fill gerekli" uyarısı at. xlsx/görsel ile sonra düzeltilir.
        _latest = recent_q[0] if recent_q else None
        _income_ok = bool(_latest and (_latest.net_income is not None or _latest.revenue))
        _complete = bool(_latest and _latest.total_assets and _latest.total_equity and _income_ok)
        if not _complete:
            _miss = []
            if _latest:
                if not _latest.total_assets: _miss.append("total_assets")
                if not _latest.total_equity: _miss.append("total_equity")
                if not _income_ok: _miss.append("revenue/net_income")
            logger.warning(
                "📊 AI ATLANDI (eksik veri, token korundu): %s %s — eksik=%s",
                ticker, _latest.period if _latest else "?", ",".join(_miss) or "veri yok",
            )
            try:
                from app.services.admin_telegram import send_admin_message
                await send_admin_message(
                    "⚠️ <b>Bilanço AI atlandı — eksik veri</b>\n"
                    f"Hisse: <b>{ticker}</b> · Dönem: {_latest.period if _latest else '?'}\n"
                    f"Eksik alan: <b>{', '.join(_miss) or 'veri yok'}</b>\n"
                    "AI harcanmadı. xlsx/görsel ile manuel doldurup yeniden üret.",
                    silent=True,
                )
            except Exception:
                pass
            return  # AI yok — eksik parse

        # AI analiz — DB'deki son 8 ceyrek karsilastirilarak (sadece TAM veride)
        ai_result = await _run_ai_analysis(ticker, bilanco_data["periods"])

        # 3. AI sonucunu DB'ye kaydet (en yeni donem icin)
        if ai_result and recent_q:
            try:
                latest = recent_q[0]
                from sqlalchemy import update as _upd
                score = ai_result.get("overall_health_score")
                label = ai_result.get("overall_health_label")
                summary = ai_result.get("summary") or ai_result.get("ai_summary")
                async with async_session() as _db_save:
                    await _db_save.execute(
                        _upd(CompanyFinancial)
                        .where(CompanyFinancial.id == latest.id)
                        .values(
                            ai_score=float(score) if score is not None else None,
                            ai_label=str(label)[:32] if label else None,
                            ai_summary=str(summary)[:2000] if summary else None,
                            ai_analyzed_at=datetime.now(timezone.utc),
                        )
                    )
                    await _db_save.commit()
                logger.info("Bilanco AI DB kayit: %s %s score=%s", ticker, latest.period, score)
            except Exception as _e:
                logger.warning("Bilanco AI DB kayit hata (%s): %s", ticker, _e)

        # 4. Tweet at (opsiyonel — AI sonucu varsa)
        if ai_result:
            await _tweet_bilanco_analysis(ticker, kap_title, ai_result)

        # 5. Bildirim gonder (premium / topic aboneleri)
        if ai_result:
            await _send_bilanco_notification(ticker, ai_result)

        # 5b. FAVORI/PORTFOY listesindekilere "Bilanco tablosu ozetlendi" bildirimi
        #     -> tiklayinca o hissenin bilanco kartina/ozetine yonlendirir.
        if ai_result:
            try:
                from app.services.notification import NotificationService
                from app.database import async_session as _asess_notif
                async with _asess_notif() as _ns:
                    await NotificationService(_ns).notify_bilanco_summarized(
                        ticker, str(ai_result.get("period", ""))
                    )
                    await _ns.commit()
            except Exception as _ne:
                logger.warning("Favori bilanco ozetlendi bildirim hata (%s): %s", ticker, _ne)

        logger.info("✅ Bilanco pipeline tamamlandi: %s", ticker)

    except Exception as e:
        logger.exception("Bilanco pipeline hatasi %s: %s", ticker, e)
        try:
            from app.services.admin_telegram import notify_scraper_error
            await notify_scraper_error(f"Bilanço Pipeline ({ticker})", str(e))
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
#  QUEUE WORKER — Bilanco sezonu yogunlugu icin sirayla isle
# ═══════════════════════════════════════════════════════════════════════════════


async def enqueue_bilanco(ticker: str, kap_title: str = ""):
    """Bilanco islemini queue'ya ekler. Queue worker isleyecek."""
    if not BILANCO_PIPELINE_ENABLED:
        logger.debug("Bilanco enqueue atlandi (pipeline kapali): %s", ticker)
        return
    await _ensure_queue()
    await _bilanco_queue.put((ticker, kap_title))
    logger.info("Bilanco queue'ya eklendi: %s (queue size: %d)", ticker, _bilanco_queue.qsize())


async def start_bilanco_queue_worker():
    """
    Queue worker — queue'daki bilanco islemlerini sirayla isler.
    Scheduler baslangicinda bir kez cagrilir.
    Bilanco sezonu (Mart-Nisan) yogunlugu icin tasarlanmistir.
    """
    if not BILANCO_PIPELINE_ENABLED:
        logger.info("Bilanco queue worker BASLATILMADI — flag kapali")
        return
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

            # Render sitesini kasmamak için kuyruk büyüklüğüne göre dinamik bekleme:
            #   queue >= 20 → 180 sn (3 dk) aralık (yoğun bilanço sezonu)
            #   queue >= 10 → 120 sn (2 dk)
            #   queue >= 5  → 60 sn  (1 dk)
            #   queue >= 1  → 15 sn
            #   queue 0     → 5 sn (rutin)
            qsize = _bilanco_queue.qsize()

            await process_bilanco_bildirimi(ticker, kap_title)
            _bilanco_queue.task_done()

            if qsize >= 20:
                delay = 180
            elif qsize >= 10:
                delay = 120
            elif qsize >= 5:
                delay = 60
            elif qsize >= 1:
                delay = 15
            else:
                delay = 5
            logger.info("Bilanco queue: kalan=%d, sonraki bilanco icin %ds bekleme", qsize, delay)
            await asyncio.sleep(delay)

        except asyncio.TimeoutError:
            continue  # Queue bos, bekle
        except Exception as e:
            logger.exception("Bilanco queue worker hatasi: %s", e)
            try:
                from app.services.admin_telegram import notify_scraper_error
                await notify_scraper_error("Bilanço Queue Worker", str(e))
            except Exception:
                pass
            await asyncio.sleep(30)


# ═══════════════════════════════════════════════════════════════════════════════
#  HAFTALIK BATCH JOB — Tum hisselerin bilancosunu guncelle
# ═══════════════════════════════════════════════════════════════════════════════


async def weekly_bilanco_update():
    """
    Haftalik batch: Tum BIST hisseleri icin 2015'ten itibaren bilanco verisi gunceller.
    Pazar gecesi calistirilmasi onerilir.
    Tahmini sure: ~3-4 saat (700+ hisse x 11 yil x 1.5sn) — ilk calisma uzun, sonrakiler sadece yeni donem
    """
    # DEVRE DISI — Haftalik IsYatirim bilanco batch'i kaldirildi (BIST lisans).
    # Bilanco verisi artik SADECE iki kaynaktan gelir:
    #   1) KAP bildirim aninda parse (bilanco_kap_scraper + ai_bilanco_analyzer)
    #   2) DB'de mevcut company_financials gecmisi
    logger.info("📊 Haftalik bilanco batch DEVRE DISI (BIST lisans) — atlandi")
    return


async def daily_temettu_update():
    """DEVRE DISI — Gunluk IsYatirim temettu batch'i kaldirildi (BIST lisans).
    Temettu verisi sadece KAP bildirimlerinden ve mevcut dividend_history'den gelir.
    """
    logger.info("💰 Gunluk temettu batch DEVRE DISI (BIST lisans) — atlandi")
    return


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
                        "current_assets", "non_current_assets",
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
                        current_assets=p.get("current_assets"),
                        non_current_assets=p.get("non_current_assets"),
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
    # ★ HARD KILL-SWITCH — bilanco tweetleri kapatildi (yasal donem)
    if not BILANCO_TWEET_ENABLED:
        logger.info(
            "🚫 Bilanco TWEET atlandi (BILANCO_TWEET_ENABLED=False): %s", ticker,
        )
        return
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
