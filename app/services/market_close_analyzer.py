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

async def _analyze_reason_with_ai(ticker: str, is_ceiling: bool) -> str:
    """Tavily ile haberi bulur, Gemini ile ozetler."""
    tavily_key = "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"
    settings = get_settings()
    
    query_action = "neden yükseldi tavan oldu" if is_ceiling else "neden düştü taban oldu"
    query = f"Borsa İstanbul {ticker} hissesi bugün {query_action} site:borsaningundemi.com OR KAP haber"
    
    search_context = ""
    try:
        async with httpx.AsyncClient() as client:
            res = await client.post(
                "https://api.tavily.com/search",
                json={"api_key": tavily_key, "query": query, "search_depth": "basic", "max_results": 2}
            )
            if res.status_code == 200:
                data = res.json()
                results = data.get("results", [])
                search_context = "\n".join([r.get("content", "") for r in results])
    except Exception as e:
        logger.warning(f"Tavily search error for {ticker}: {e}")

    if not search_context:
        return "Pozitif piyasa algısı ve alıcı baskısı." if is_ceiling else "Negatif piyasa algısı ve satıcı baskısı."

    # Gemini ile özetle
    try:
        genai.configure(api_key=settings.GEMINI_API_KEY)
        model = genai.GenerativeModel("gemini-2.5-pro")
        prompt = (f"Bir hisse senedi ({ticker}) bugün {'tavan' if is_ceiling else 'taban'} yaptı.\n"
                  f"İnternetten gelen güncel haber içerikleri şunlar:\n{search_context}\n\n"
                  "Bu metinlerden çıkararak hissenin neden bu hareketi yaptığını "
                  "TEK BİR CÜMLE halinde ve MAKSİMUM 8 KELİME ile açıkla. (Örn: Şirket yeni iş sözleşmesi imzaladığını duyurdu.)")
        
        response = model.generate_content(prompt)
        text = response.text.strip().replace('"', '').replace('\n', '')
        return text if len(text) > 0 else ("Pozitif piyasa algısı." if is_ceiling else "Kâr satışı ve negatif beklenti.")
    except Exception as e:
        logger.error(f"Gemini error for {ticker}: {e}")
        return "Pozitif beklenti satın alındı." if is_ceiling else "Hisseye yönelik satış baskısı arttı."

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
        # Piyasalarin o gun acik olup olmadigini gormek icin dunu kontrol edelim mi?
        # En temeli: Eger tablo bossa tatildir dedik (uzmanpara listesi guncellenmiyorsa).
        # Tavanlari isleyelim
        for stock in ceilings:
            ticker = stock["ticker"]
            price = Decimal(str(stock["price"]))
            reason = await _analyze_reason_with_ai(ticker, is_ceiling=True)
            
            # Gecmis 30 gun icinde rekorlari var mi? Statlari hesapla.
            stmt = select(DailyStockMarketStat).where(DailyStockMarketStat.ticker == ticker).order_by(desc(DailyStockMarketStat.date))
            res = await session.execute(stmt)
            past_records = res.scalars().all()
            
            consec_ceil = 1
            for r in past_records:
                if r.is_ceiling: consec_ceil += 1
                else: break
            monthly_ceil = sum(1 for r in past_records if r.is_ceiling and (today - r.date).days <= 30) + 1
            
            new_stat = DailyStockMarketStat(
                ticker=ticker,
                date=today,
                close_price=price,
                is_ceiling=True,
                consecutive_ceiling_count=consec_ceil,
                monthly_ceiling_count=monthly_ceil,
                reason=reason[:100]  # sinirlama
            )
            session.add(new_stat)
            
        # Tabanlari isleyelim
        for stock in floors:
            ticker = stock["ticker"]
            price = Decimal(str(stock["price"]))
            reason = await _analyze_reason_with_ai(ticker, is_ceiling=False)
            
            stmt = select(DailyStockMarketStat).where(DailyStockMarketStat.ticker == ticker).order_by(desc(DailyStockMarketStat.date))
            res = await session.execute(stmt)
            past_records = res.scalars().all()
            
            consec_flr = 1
            for r in past_records:
                if r.is_floor: consec_flr += 1
                else: break
            monthly_flr = sum(1 for r in past_records if r.is_floor and (today - r.date).days <= 30) + 1
            
            new_stat = DailyStockMarketStat(
                ticker=ticker,
                date=today,
                close_price=price,
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

