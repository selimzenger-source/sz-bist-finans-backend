import asyncio
import logging
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal
import json

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select, desc, func

from app.database import async_session
from app.models.daily_stock_market_stat import DailyStockMarketStat
from app.models.kap_all_disclosure import KapAllDisclosure
from app.models.ipo import IPO
from app.config import get_settings

# Gemini for summarization
import google.generativeai as genai

logger = logging.getLogger(__name__)

_TR_TZ = ZoneInfo("Europe/Istanbul")

async def scrape_uzmanpara(is_ceiling: bool) -> list[dict]:
    """Uzmanpara'dan tavan/taban hisseleri ceker."""
    url = "https://uzmanpara.milliyet.com.tr/borsa/en-cok-artanlar/" if is_ceiling else "https://uzmanpara.milliyet.com.tr/borsa/en-cok-azalanlar/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html"
    }
    results = []
    
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, headers=headers)
            if res.status_code != 200:
                logger.error(f"Scrape URL error: {url} -> {res.status_code}")
                return []
                
            soup = BeautifulSoup(res.text, "html.parser")
            table_id = "tbl_artanlar" if is_ceiling else "tbl_azalanlar"
            table = soup.find("table", {"id": table_id})
            if not table:
                table = soup.select_one("table")
                
            if not table:
                return []
                
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 4:
                    ticker = cols[0].text.strip()
                    price_str = cols[1].text.strip().replace(".", "").replace(",", ".")
                    change_str = cols[3].text.strip().replace(".", "").replace(",", ".")
                    
                    try:
                        price = float(price_str)
                        change = float(change_str)
                        
                        if is_ceiling and change >= 9.75:
                            results.append({"ticker": ticker, "price": price, "change": change})
                        elif not is_ceiling and change <= -9.75:
                            results.append({"ticker": ticker, "price": price, "change": change})
                    except ValueError:
                        continue
    except Exception as e:
        logger.error(f"Uzmanpara scrape hatasi: {e}")
    
    return results


async def scrape_uzmanpara_supplementary(is_ceiling: bool, exclude_tickers: list[str] = None, limit: int = 8) -> list[dict]:
    """Tavan/taban olmayan en çok artan/azalan hisseleri getirir (ek liste için)."""
    url = "https://uzmanpara.milliyet.com.tr/borsa/en-cok-artanlar/" if is_ceiling else "https://uzmanpara.milliyet.com.tr/borsa/en-cok-azalanlar/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html"
    }
    exclude = set(exclude_tickers or [])
    results = []
    
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(url, headers=headers)
            if res.status_code != 200:
                return []
            soup = BeautifulSoup(res.text, "html.parser")
            table_id = "tbl_artanlar" if is_ceiling else "tbl_azalanlar"
            table = soup.find("table", {"id": table_id})
            if not table:
                table = soup.select_one("table")
            if not table:
                return []
            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) >= 4:
                    ticker = cols[0].text.strip()
                    if ticker in exclude:
                        continue
                    price_str = cols[1].text.strip().replace(".", "").replace(",", ".")
                    change_str = cols[3].text.strip().replace(".", "").replace(",", ".")
                    try:
                        price = float(price_str)
                        change = float(change_str)
                        # Tavan/taban olmayan ama güçlü hareketle olanlar
                        if is_ceiling and 0 < change < 9.75:
                            results.append({"ticker": ticker, "price": price, "change": change})
                        elif not is_ceiling and -9.75 < change < 0:
                            results.append({"ticker": ticker, "price": price, "change": change})
                    except ValueError:
                        continue
                if len(results) >= limit:
                    break
    except Exception as e:
        logger.error(f"Supplementary scrape hatasi: {e}")
    return results[:limit]

async def _analyze_reason_with_ai(ticker: str, is_ceiling: bool, price: float = None, pct: float = None, consec: int = 1, monthly: int = 1) -> str:
    """Internal KAP + Tavily + Context ile Coklu-AI Fallback ile analiz yapar.
    Sira: OpenAI (GPT-4o) -> Abacus (Sonnet) -> Gemini 2.5 Pro
    """
    settings = get_settings()
    tavily_key = "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"
    
    # 1. Dahili KAP + Tavily Context Hazirla
    internal_news = []
    try:
        async with async_session() as session:
            since = datetime.now(timezone.utc) - timedelta(hours=48)
            stmt = select(KapAllDisclosure).where(
                KapAllDisclosure.company_code == ticker,
                KapAllDisclosure.created_at >= since
            ).order_by(desc(KapAllDisclosure.created_at)).limit(5)
            res = await session.execute(stmt)
            news_items = res.scalars().all()
            for n in news_items:
                t_str = f"KAP: {n.title}"
                if n.ai_summary: t_str += f" ({n.ai_summary})"
                internal_news.append(t_str)
    except Exception as e:
        logger.warning(f"Internal KAP news error for {ticker}: {e}")

    query_action = "neden yükseldi tavan" if is_ceiling else "neden düştü taban"
    query = f"{ticker} hisse {query_action}"
    external_search = ""
    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(
                "https://api.tavily.com/search",
                json={"api_key": tavily_key, "query": query, "search_depth": "basic", "max_results": 2}
            )
            if res.status_code == 200:
                data = res.json()
                results = data.get("results", [])
                external_search = "\n".join([r.get("content", "") for r in results])
    except Exception as e:
        logger.warning(f"Tavily search error for {ticker}: {e}")

    # 2. IPO kontrolu — gercekten yeni halka arz mi? Detayli bilgi cek.
    is_recent_ipo = False
    ipo_info = ""
    try:
        async with async_session() as session:
            cutoff = date.today() - timedelta(days=60)
            stmt = select(IPO).where(
                IPO.ticker == ticker,
                IPO.trading_start != None,
                IPO.trading_start >= cutoff
            ).limit(1)
            res = await session.execute(stmt)
            ipo = res.scalar_one_or_none()
            if ipo:
                is_recent_ipo = True
                days_since = (date.today() - ipo.trading_start).days
                parts = [f"\nÖNEMLİ HALKA ARZ BİLGİSİ:"]
                parts.append(f"- Şirket: {ipo.company_name}")
                if ipo.sector: parts.append(f"- Sektör: {ipo.sector}")
                if ipo.ipo_price: parts.append(f"- Halka arz fiyatı: {ipo.ipo_price} TL (Şu anki: {f_price if price else '?'} TL)")
                parts.append(f"- İşlem görmeye başlayalı {days_since} gün oldu")
                if ipo.market_segment:
                    seg_map = {"yildiz_pazar": "Yıldız Pazar", "ana_pazar": "Ana Pazar", "alt_pazar": "Alt Pazar"}
                    parts.append(f"- Pazar: {seg_map.get(ipo.market_segment, ipo.market_segment)}")
                if ipo.total_applicants:
                    parts.append(f"- Toplam başvuran: {ipo.total_applicants:,} kişi")
                ipo_info = "\n".join(parts)
    except Exception as e:
        logger.warning(f"IPO check error for {ticker}: {e}")

    # 3. Fiyat geçmişi — DB veya BigPara fallback
    price_history = ""
    trend_statement = ""  # Programatik hesaplanmış trend
    try:
        prices = []
        # Önce DB dene
        async with async_session() as session:
            since = date.today() - timedelta(days=15)
            stmt = select(DailyStockMarketStat).where(
                DailyStockMarketStat.ticker == ticker,
                DailyStockMarketStat.date >= since
            ).order_by(DailyStockMarketStat.date.desc()).limit(10)
            res = await session.execute(stmt)
            history = res.scalars().all()
            if history and len(history) >= 3:
                for h in reversed(history):
                    prices.append(float(h.close_price))
        
        # DB'de yoksa web'den çek (1. uzmanpara, 2. bigpara)
        if len(prices) < 3:
            for scrape_url in [
                f"https://uzmanpara.milliyet.com.tr/borsa/hisse-detay/{ticker}/",
                f"https://bigpara.hurriyet.com.tr/borsa/hisse-fiyatlari/{ticker.lower()}/"
            ]:
                try:
                    async with httpx.AsyncClient(timeout=8) as client:
                        res = await client.get(scrape_url, headers={"User-Agent": "Mozilla/5.0"})
                        if res.status_code == 200:
                            soup = BeautifulSoup(res.text, "html.parser")
                            for table in soup.find_all("table"):
                                for row in table.find_all("tr")[1:11]:
                                    cols = row.find_all("td")
                                    if len(cols) >= 4:
                                        for col in cols:
                                            try:
                                                txt = col.text.strip().replace(".", "").replace(",", ".")
                                                val = float(txt)
                                                if 0.5 < val < 100000:  # Makul fiyat
                                                    prices.append(val)
                                                    break
                                            except: pass
                            if len(prices) >= 3:
                                break
                except Exception as e:
                    logger.warning(f"Scrape error ({scrape_url}) for {ticker}: {e}")
        
        # Programatik trend hesapla
        if len(prices) >= 3:
            oldest_price = prices[0]
            newest_price = prices[-1]
            if oldest_price > 0:
                pct_change_period = ((newest_price - oldest_price) / oldest_price) * 100
                
                if pct_change_period <= -25:
                    # Gerçekten derin düşüş var — bu veriyle söyle
                    trend_statement = f"\nTREND VERİSİ: Son {len(prices)} günde %{pct_change_period:.0f} düşüş. Bu GERÇEK derin satış."
                    price_history = trend_statement
                elif pct_change_period >= 25:
                    trend_statement = f"\nTREND VERİSİ: Son {len(prices)} günde %{pct_change_period:+.0f} yükseliş. Tarihi tepe bölgesi."
                    price_history = trend_statement
                else:
                    trend_statement = f"\nTREND VERİSİ: Son {len(prices)} günde %{pct_change_period:+.1f} değişim. Normal seyir."
                    price_history = trend_statement
    except Exception as e:
        logger.warning(f"Price history error for {ticker}: {e}")

    combined_context = "\n".join(internal_news) + "\n" + external_search
    has_news = bool(internal_news) or len(external_search.strip()) > 20
    f_pct = f"{pct:+.2f}" if pct is not None else ("+9.95" if is_ceiling else "-9.95")
    f_price = f"{price:.2f}" if price is not None else "0.00"
    
    ipo_rule = ""
    if is_recent_ipo:
        days_since = 0
        try:
            for line in ipo_info.split("\n"):
                if "başlayalı" in line:
                    import re
                    m = re.search(r"(\d+) gün", line)
                    if m: days_since = int(m.group(1))
        except: pass
        
        if days_since <= 10:
            ipo_rule = "\n- Bu hisse YENİ HALKA ARZ ve henüz ilk 10 işlem gününde. 'Halka arz sonrası yoğun talep' yaz."
        else:
            ipo_rule = ("\n- Bu hisse halka arz oldu ama ilk 10 günü geçti. Artık basit 'halka arz' açıklaması yetmez."
                       "\n  Somut bir sebep bul veya boş bırak.")
    else:
        ipo_rule = "\n- Bu hisse ESKİ bir şirket. ASLA 'halka arz' deme."
    
    # Trend kuralı — AI ASLA trend yorumu yapmasın
    trend_rule = "\n2. ASLA 'derin satış', 'tepki alışı', 'kâr satışı', 'sert yükseliş' gibi trend yorumları YAZMA. Sadece somut haber/veri bazlı sebepler."
    
    prompt = (f"Hisse: {ticker}, Fiyat: {f_price} TL, Değişim: %{f_pct}\n"
              f"Son KAP/Haberler:\n{combined_context}{ipo_info}{price_history}\n\n"
              f"GÖREV: Bu hissenin bugün neden {'tavana' if is_ceiling else 'tabana'} ulaştığını açıkla. MAX 5-6 kelime.\n\n"
              "KURALLAR:\n"
              "1. Somut bir sebep varsa yaz: bilanço, ihale, sözleşme, temettü, geri alım, bedelsiz sermaye artırımı vs.\n"
              f"{ipo_rule}"
              f"{trend_rule}\n"
              "3. ASLA 'tavan serisi devam', 'X. gün tavan serisi', 'taban serisi devam' YAZMA. Bu bilgi zaten tabloda var.\n"
              "4. Somut bir sebep BULAMIYORSAN, boş string dön: ''\n"
              "5. ASLA uydurma. ASLA 'trend direnci kırıldı', 'momentum', 'alıcı baskısı', 'piyasa beklentisi' gibi genel laflar yazma.\n"
              "6. Sadece somut, doğrulanabilir sebepler yaz veya boş bırak.")

    # FALLBACK SİSTEMİ
    # ── 1. OPENAI (GPT-4o) ──
    if settings.OPENAI_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                res = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {settings.OPENAI_API_KEY}"},
                    json={"model": "gpt-4o", "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
                )
                if res.status_code == 200:
                    text = res.json()["choices"][0]["message"]["content"].strip().replace('"', '').replace("'", "")
                    # Generic filtre
                    bad = ["momentum", "alıcı baskısı", "satıcı baskısı", "trend direnci", "hacimli kırılım", "piyasa beklentisi", "yatırımcı talebi", "teknik trend", "fiyatlama", "tavan serisi", "taban serisi", "serisi devam", "derin satış", "tepki alışı", "kâr satışı", "sert yükseliş", "kar satışı", "tepki yükselişi"]
                    if any(x in text.lower() for x in bad):
                        return ""
                    logger.info(f"OpenAI result for {ticker}: {text}")
                    return text
        except Exception as e:
            logger.warning(f"OpenAI error for {ticker}: {e}")

    # ── 2. ABACUS (Sonnet) ──
    if settings.ABACUS_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.post(
                    "https://routellm.abacus.ai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {settings.ABACUS_API_KEY}"},
                    json={"model": "claude-sonnet-4-6", "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
                )
                if res.status_code == 200:
                    text = res.json()["choices"][0]["message"]["content"].strip().replace('"', '').replace("'", "")
                    bad = ["momentum", "alıcı baskısı", "satıcı baskısı", "trend direnci", "hacimli kırılım", "piyasa beklentisi", "yatırımcı talebi", "teknik trend", "fiyatlama", "tavan serisi", "taban serisi", "serisi devam", "derin satış", "tepki alışı", "kâr satışı", "sert yükseliş", "kar satışı", "tepki yükselişi"]
                    if any(x in text.lower() for x in bad):
                        return ""
                    logger.info(f"Abacus result for {ticker}: {text}")
                    return text
        except Exception as e:
            logger.warning(f"Abacus error for {ticker}: {e}")

    # ── 3. GEMINI 2.5 PRO ──
    if settings.GEMINI_API_KEY:
        try:
            genai.configure(api_key=settings.GEMINI_API_KEY)
            model = genai.GenerativeModel("gemini-2.5-pro")
            res = model.generate_content(prompt)
            text = res.text.strip().replace('"', '').replace("'", "")
            bad = ["momentum", "alıcı baskısı", "satıcı baskısı", "trend direnci", "hacimli kırılım", "piyasa beklentisi", "yatırımcı talebi", "teknik trend", "fiyatlama", "tavan serisi", "taban serisi", "serisi devam", "derin satış", "tepki alışı", "kâr satışı", "sert yükseliş", "kar satışı", "tepki yükselişi"]
            if any(x in text.lower() for x in bad):
                return ""
            logger.info(f"Gemini result for {ticker}: {text}")
            return text
        except Exception as e:
            logger.warning(f"Gemini error for {ticker}: {e}")

    # ── 4. PROGRAMATIK TREND FALLBACK ──
    # Tüm AI provider'lar boş döndüyse ve gerçek trend verisi varsa
    if "derin satış" in trend_statement.lower():
        if is_ceiling:
            return "Derin satış sonrası tepki alışı"
        else:
            return "Sert yükseliş sonrası kâr satışı"
    elif "tepe bölgesi" in trend_statement.lower():
        if not is_ceiling:
            return "Sert yükseliş sonrası kâr satışı"
    
    return ""

async def scrape_and_analyze_market_close():
    """18:35'te calisip en cok artan/azalanlari bulur ve AI ile analiz edip SQL'e kaydeder."""
    logger.info("Market close analysis started...")
    
    ceilings = await scrape_uzmanpara(is_ceiling=True)
    floors = await scrape_uzmanpara(is_ceiling=False)
    
    logger.info(f"Market Close: {len(ceilings)} ceilings, {len(floors)} floors found.")
    
    # Eger hic sonuc yoksa (haftasonu, tatil) atla
    if not ceilings and not floors:
        logger.info("No ceiling or floor stocks found. It might be a weekend or holiday.")
        return
        
    today = datetime.now(_TR_TZ).date()
    
    async with async_session() as session:
        # Piyasalarin o gun acik olup olmadigini gormek icin son kayitla karsilastir
        # Herhangi bir tavan/tabanın fiyatı değişmiş mi diye bakabiliriz ama daha garantisi
        # Veri tabanındaki 'today' kaydı var mı diye bakmak (mükerrer çalışmayı da önler).
        check_stmt = select(DailyStockMarketStat).where(DailyStockMarketStat.date == today).limit(1)
        existing = (await session.execute(check_stmt)).scalar()
        if existing:
            logger.info(f"{today} için zaten analiz yapılmış. Atlanıyor.")
            return

        # Tatil kontrolü (Opsiyonel: Eğer Uzmanpara verisi dünküyle tam aynıysa tatildir)
        # Ama Genelde tavan/taban listesi boşsa veya bugün işlem yoksa scrape zaten boş döner.
        
        # FIFO Mantığı: 30 günden eski kayıtları temizle (isteğe göre)
        cleanup_date = today - timedelta(days=32)
        from sqlalchemy import delete
        await session.execute(delete(DailyStockMarketStat).where(DailyStockMarketStat.date < cleanup_date))

        # Tavanlari isleyelim
        for stock in ceilings:
            ticker = stock["ticker"]
            price_val = Decimal(str(stock["price"]))
            pct_val = Decimal(str(stock["change"]))
            
            # Gecmis 30 gun icinde rekorlari var mi? Statlari hesapla.
            stmt = select(DailyStockMarketStat).where(DailyStockMarketStat.ticker == ticker).order_by(desc(DailyStockMarketStat.date))
            res = await session.execute(stmt)
            past_records = res.scalars().all()
            
            consec_ceil = 1
            if past_records and past_records[0].is_ceiling:
                consec_ceil = past_records[0].consecutive_ceiling_count + 1
            
            monthly_ceil = sum(1 for r in past_records if r.is_ceiling and (today - r.date).days <= 30) + 1
            
            # AI Analizi artik daha fazla veri ile yapiliyor
            reason = await _analyze_reason_with_ai(
                ticker=ticker, 
                is_ceiling=True, 
                price=float(price_val), 
                pct=float(pct_val),
                consec=consec_ceil,
                monthly=monthly_ceil
            )
            
            new_stat = DailyStockMarketStat(
                ticker=ticker,
                date=today,
                close_price=price_val,
                percent_change=pct_val,
                is_ceiling=True,
                consecutive_ceiling_count=consec_ceil,
                monthly_ceiling_count=monthly_ceil,
                reason=reason[:100]
            )
            session.add(new_stat)
            
        # Tabanlari isleyelim
        for stock in floors:
            ticker = stock["ticker"]
            price_val = Decimal(str(stock["price"]))
            pct_val = Decimal(str(stock["change"]))
            
            stmt = select(DailyStockMarketStat).where(DailyStockMarketStat.ticker == ticker).order_by(desc(DailyStockMarketStat.date))
            res = await session.execute(stmt)
            past_records = res.scalars().all()
            
            consec_flr = 1
            if past_records and past_records[0].is_floor:
                consec_flr = past_records[0].consecutive_floor_count + 1

            monthly_flr = sum(1 for r in past_records if r.is_floor and (today - r.date).days <= 30) + 1
            
            reason = await _analyze_reason_with_ai(
                ticker=ticker, 
                is_ceiling=False, 
                price=float(price_val), 
                pct=float(pct_val),
                consec=consec_flr,
                monthly=monthly_flr
            )
            
            new_stat = DailyStockMarketStat(
                ticker=ticker,
                date=today,
                close_price=price_val,
                percent_change=pct_val,
                is_floor=True,
                consecutive_floor_count=consec_flr,
                monthly_floor_count=monthly_flr,
                reason=reason[:100]
            )
            session.add(new_stat)
            
        await session.commit()
        
        # Oku (yeni olanlari listele)
        t_stmt = select(DailyStockMarketStat).where(DailyStockMarketStat.date == today, DailyStockMarketStat.is_ceiling == True).order_by(desc(DailyStockMarketStat.consecutive_ceiling_count))
        t_res = await session.execute(t_stmt)
        c_stats = t_res.scalars().all()
        
        f_stmt = select(DailyStockMarketStat).where(DailyStockMarketStat.date == today, DailyStockMarketStat.is_floor == True).order_by(desc(DailyStockMarketStat.consecutive_floor_count))
        f_res = await session.execute(f_stmt)
        fl_stats = f_res.scalars().all()

    # ==========================
    # GÖRSEL ÜRETİMİ VE TWITTER
    # ==========================
    from app.services.chart_image_generator import generate_ceiling_floor_images
    from app.services.twitter_service import _safe_tweet

    if c_stats:
        tavan_images = generate_ceiling_floor_images(c_stats, is_ceiling=True)
        tickers_str = " ".join([f"#{s.ticker}" for s in c_stats])
        if len(tickers_str) > 150:
            tickers_str = tickers_str[:150] + "..." # Limit hashtags if too many
            
        base_t_text = f"🚨 Günün TAVAN Yapan Hisseleri ve Sebepleri\n\n🎯 Hangi şirketler neden zirveyi gördü? Yapay zeka yatırımcı özetleri görsellerde!\n\n{tickers_str}"
        
        for idx, path in enumerate(tavan_images):
            page_info = f" (Sayfa {idx+1}/{len(tavan_images)})" if len(tavan_images) > 1 else ""
            tweet_text = f"{base_t_text}{page_info}"
            _safe_tweet(text=tweet_text, image_path=path, source="market_close_analyzer")
            
    if fl_stats:
        taban_images = generate_ceiling_floor_images(fl_stats, is_ceiling=False)
        tickers_str = " ".join([f"#{s.ticker}" for s in fl_stats])
        if len(tickers_str) > 150:
            tickers_str = tickers_str[:150] + "..."
            
        base_f_text = f"📉 Günün TABAN Yapan Hisseleri ve Sebepleri\n\n📌 Şirketler neden kan kaybetti? Yapay zeka analizleri görsellerde!\n\n{tickers_str}"
        
        for idx, path in enumerate(taban_images):
            page_info = f" (Sayfa {idx+1}/{len(taban_images)})" if len(taban_images) > 1 else ""
            tweet_text = f"{base_f_text}{page_info}"
            _safe_tweet(text=tweet_text, image_path=path, source="market_close_analyzer")
    
    logger.info("Market close analysis & Twitter generation completed.")

