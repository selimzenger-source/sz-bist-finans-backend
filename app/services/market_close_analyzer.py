import asyncio
import logging
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from decimal import Decimal
import json

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select, desc, func, text

from app.database import async_session
from app.models.daily_stock_market_stat import DailyStockMarketStat
from app.models.kap_all_disclosure import KapAllDisclosure
from app.models.ipo import IPO
from app.config import get_settings

# Gemini SDK kaldırıldı — diğer servisler REST API kullanıyor, bu dosyada artık gerek yok

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
    Sira: OpenAI (GPT-4o) -> Anthropic (Claude 3.5 Sonnet) -> Abacus (Sonnet) -> Gemini 2.5 Pro
    """
    settings = get_settings()
    tavily_key = settings.TAVILY_API_KEY
    f_price = float(price) if price else 0
    
    # 1. Dahili KAP + Tavily Context Hazirla
    internal_news = []
    try:
        async with async_session() as session:
            since = datetime.now(timezone.utc) - timedelta(days=15)
            stmt = select(KapAllDisclosure).where(
                KapAllDisclosure.company_code == ticker,
                KapAllDisclosure.created_at >= since
            ).order_by(desc(KapAllDisclosure.created_at)).limit(10)
            res = await session.execute(stmt)
            news_items = res.scalars().all()
            for n in news_items:
                t_str = f"KAP: {n.title}"
                if n.ai_summary: t_str += f" ({n.ai_summary})"
                internal_news.append(t_str)
    except Exception as e:
        logger.warning(f"Internal KAP news error for {ticker}: {e}")

    query_action = "neden yükseldi tavan" if is_ceiling else "neden düştü taban"
    query = f"{ticker} hisse {query_action} son haberler"
    external_search = ""
    if tavily_key:
        try:
            async with httpx.AsyncClient() as client:
                res = await client.post(
                    "https://api.tavily.com/search",
                    json={"api_key": tavily_key, "query": query, "search_depth": "advanced", "max_results": 3}
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
                if ipo.ipo_price: parts.append(f"- Halka arz fiyatı: {ipo.ipo_price} TL (Şu anki: {f_price:.2f} TL)")
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
        # Önce DB dene — raw SQL ile "date" quoting
        async with async_session() as session:
            since = date.today() - timedelta(days=15)
            raw = text("""
                SELECT close_price FROM daily_stock_market_stats
                WHERE ticker = :ticker AND "date" >= :since
                ORDER BY "date" DESC LIMIT 10
            """)
            res = await session.execute(raw, {"ticker": ticker, "since": since})
            rows = res.fetchall()
            if rows and len(rows) >= 3:
                for row in reversed(rows):
                    prices.append(float(row[0]))
        
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
    trend_rule = "\n- ASLA 'derin satış', 'tepki alışı', 'kâr satışı', 'sert yükseliş' gibi trend yorumları YAZMA. Sadece somut haber/veri bazlı sebepler."

    # Context string oluştur (KAP haberleri + web arama + IPO bilgisi)
    context_parts = []
    if has_news:
        context_parts.append(f"SON HABERLER:\n{combined_context}")
    if ipo_info:
        context_parts.append(ipo_info)
    context_str = "\n".join(context_parts) if context_parts else "- Belirgin haber veya veri bulunamadı."

    prompt = f"""#{ticker} bugün {"tavan" if is_ceiling else "taban"} yaptı. Sebebini SADECE 4-6 kelime ile yaz.

VERİLER:
{context_str}
{ipo_rule}

KURALLAR:
1. Verilerde somut haber varsa (bilanço, sermaye artırımı, halka arz, ihale, sözleşme) SADECE onu yaz.
2. Somut haber yoksa SADECE "EMPTY" yaz. Uydurma, jenerik yorum YASAK.
3. Kısa ol: "Güçlü 3. çeyrek bilançosu açıklandı." gibi.
4. YASAK ifadeler: düşük işlem hacmi, yatay seyir, konsolide, volatilite, sessiz, istikrarlı, sınırlı, rutin, potansiyel, kurumsal kalite, güven pekiştir.
"""

    # Ortak filtre — jenerik/dolgu yanıtları yakala
    bad = ["momentum", "alıcı baskısı", "satıcı baskısı", "trend direnci", "hacimli kırılım",
           "piyasa beklentisi", "yatırımcı talebi", "teknik trend", "fiyatlama", "tavan serisi",
           "taban serisi", "serisi devam", "derin satış", "tepki alışı", "kâr satışı",
           "sert yükseliş", "kar satışı", "tepki yükselişi", "düşük işlem hacmi",
           "yatay seyir", "konsolide", "volatilite", "sessiz yükseliş", "istikrarlı seyir",
           "kurumsal kalite", "sınırlı hareket", "rutin işlem", "sessiz seans",
           "potansiyeli ile", "sektörü potansiyeli", "güvenini pekiştir", "seyir izliyor",
           "seyirde", "katalizör eksikliği"]

    def _clean_ai_text(raw: str) -> str:
        """AI yanıtını temizle — EMPTY veya jenerik ise boş dön."""
        t = raw.strip().replace('"', '').replace("'", "")
        if not t or t.upper() == "EMPTY" or len(t) < 5:
            return ""
        if any(x in t.lower() for x in bad):
            return ""
        return t

    # FALLBACK SİSTEMİ
    # ── 1. ANTHROPIC (Claude — birincil) ──
    if settings.ANTHROPIC_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=25) as client:
                res = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": settings.ANTHROPIC_API_KEY,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json"
                    },
                    json={
                        "model": "claude-sonnet-4-20250514",
                        "max_tokens": 120,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.2
                    }
                )
                if res.status_code == 200:
                    text = _clean_ai_text(res.json()["content"][0]["text"])
                    if text:
                        logger.info(f"Anthropic result for {ticker}: {text}")
                        return text
                    else:
                        logger.info(f"Anthropic empty/filtered for {ticker}")
                else:
                    logger.warning(f"Anthropic HTTP {res.status_code} for {ticker}: {res.text[:150]}")
        except Exception as e:
            logger.warning(f"Anthropic error for {ticker}: {e}")

    # ── 2. OPENAI (GPT-4o) ──
    if settings.OPENAI_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                res = await client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {settings.OPENAI_API_KEY}"},
                    json={"model": "gpt-4o", "max_tokens": 120, "messages": [{"role": "system", "content": "Sen kısa finansal yorum asistanısın. SADECE 4-6 kelime yaz. Uzun cümleler YASAK."}, {"role": "user", "content": prompt}], "temperature": 0.2}
                )
                if res.status_code == 200:
                    text = _clean_ai_text(res.json()["choices"][0]["message"]["content"])
                    if text:
                        logger.info(f"OpenAI result for {ticker}: {text}")
                        return text
                    else:
                        logger.info(f"OpenAI empty/filtered for {ticker}")
                else:
                    logger.warning(f"OpenAI HTTP {res.status_code} for {ticker}: {res.text[:150]}")
        except Exception as e:
            logger.warning(f"OpenAI error for {ticker}: {e}")

    # ── 3. ABACUS (Sonnet) ──
    if settings.ABACUS_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.post(
                    "https://routellm.abacus.ai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {settings.ABACUS_API_KEY}"},
                    json={"model": "claude-sonnet-4-6", "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
                )
                if res.status_code == 200:
                    text = _clean_ai_text(res.json()["choices"][0]["message"]["content"])
                    if text:
                        logger.info(f"Abacus result for {ticker}: {text}")
                        return text
                    else:
                        logger.info(f"Abacus empty/filtered for {ticker}")
                else:
                    logger.warning(f"Abacus HTTP {res.status_code} for {ticker}: {res.text[:100]}")
        except Exception as e:
            logger.warning(f"Abacus error for {ticker}: {e}")

    # ── 4. GEMINI REST API ──
    if settings.GEMINI_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                res = await client.post(
                    "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
                    headers={
                        "Authorization": f"Bearer {settings.GEMINI_API_KEY}",
                        "Content-Type": "application/json"
                    },
                    json={"model": "gemini-2.5-pro", "messages": [{"role": "user", "content": prompt}], "temperature": 0.2}
                )
                if res.status_code == 200:
                    text = _clean_ai_text(res.json()["choices"][0]["message"]["content"])
                    if text:
                        logger.info(f"Gemini result for {ticker}: {text}")
                        return text
                    else:
                        logger.info(f"Gemini empty/filtered for {ticker}")
                else:
                    logger.warning(f"Gemini HTTP {res.status_code} for {ticker}: {res.text[:100]}")
        except Exception as e:
            logger.warning(f"Gemini error for {ticker}: {e}")

    return ""

async def _save_market_close_data(session, today, ceilings, floors):
    """Tavan/taban verilerini AI analiz ile DB'ye kaydeder (helper)."""
    # FIFO: 32 günden eski kayıtları temizle
    cleanup_date = today - timedelta(days=32)
    await session.execute(
        text('DELETE FROM daily_stock_market_stats WHERE "date" < :cutoff'),
        {"cutoff": cleanup_date}
    )

    saved_count = 0
    ai_ok_count = 0

    # Tavanları işle
    for i, stock in enumerate(ceilings):
        try:
            ticker = stock["ticker"]
            price_val = Decimal(str(stock["price"]))
            pct_val = Decimal(str(stock["change"]))

            past_res = await session.execute(
                text("""SELECT is_ceiling, is_floor, consecutive_ceiling_count,
                        consecutive_floor_count, "date"
                        FROM daily_stock_market_stats
                        WHERE ticker = :ticker ORDER BY "date" DESC"""),
                {"ticker": ticker}
            )
            past_records = past_res.fetchall()

            consec_ceil = 1
            if past_records and past_records[0][0]:
                consec_ceil = past_records[0][2] + 1

            monthly_ceil = sum(1 for r in past_records if r[0] and (today - r[4]).days <= 30) + 1

            reason = await _analyze_reason_with_ai(
                ticker=ticker,
                is_ceiling=True,
                price=float(price_val),
                pct=float(pct_val),
                consec=consec_ceil,
                monthly=monthly_ceil
            )
            if reason:
                ai_ok_count += 1

            logger.info(f"[TAVAN {i+1}/{len(ceilings)}] {ticker}: reason={'✅' if reason else '❌'} | {reason[:50] if reason else 'boş'}")

            new_stat = DailyStockMarketStat(
                ticker=ticker,
                date=today,
                close_price=price_val,
                percent_change=pct_val,
                is_ceiling=True,
                consecutive_ceiling_count=consec_ceil,
                monthly_ceiling_count=monthly_ceil,
                reason=(reason or "")[:100]
            )
            session.add(new_stat)
            saved_count += 1
        except Exception as e:
            logger.error(f"[TAVAN] {stock.get('ticker', '?')} kayıt hatası: {e}")

    # Tabanları işle
    for i, stock in enumerate(floors):
        try:
            ticker = stock["ticker"]
            price_val = Decimal(str(stock["price"]))
            pct_val = Decimal(str(stock["change"]))

            past_res = await session.execute(
                text("""SELECT is_ceiling, is_floor, consecutive_ceiling_count,
                        consecutive_floor_count, "date"
                        FROM daily_stock_market_stats
                        WHERE ticker = :ticker ORDER BY "date" DESC"""),
                {"ticker": ticker}
            )
            past_records = past_res.fetchall()

            consec_flr = 1
            if past_records and past_records[0][1]:
                consec_flr = past_records[0][3] + 1

            monthly_flr = sum(1 for r in past_records if r[1] and (today - r[4]).days <= 30) + 1

            reason = await _analyze_reason_with_ai(
                ticker=ticker,
                is_ceiling=False,
                price=float(price_val),
                pct=float(pct_val),
                consec=consec_flr,
                monthly=monthly_flr
            )
            if reason:
                ai_ok_count += 1

            logger.info(f"[TABAN {i+1}/{len(floors)}] {ticker}: reason={'✅' if reason else '❌'} | {reason[:50] if reason else 'boş'}")

            new_stat = DailyStockMarketStat(
                ticker=ticker,
                date=today,
                close_price=price_val,
                percent_change=pct_val,
                is_floor=True,
                consecutive_floor_count=consec_flr,
                monthly_floor_count=monthly_flr,
                reason=(reason or "")[:100]
            )
            session.add(new_stat)
            saved_count += 1
        except Exception as e:
            logger.error(f"[TABAN] {stock.get('ticker', '?')} kayıt hatası: {e}")

    await session.commit()
    logger.info(f"DB'ye kaydedildi: {saved_count}/{len(ceilings)+len(floors)} kayıt, AI analiz: {ai_ok_count} başarılı.")


async def scrape_and_analyze_market_close(force: bool = False):
    """18:35'te calisip en cok artan/azalanlari bulur ve AI ile analiz edip SQL'e kaydeder.
    Eksik veri veya hata durumunda 1 dk arayla 3 kez daha dener (toplam 4 deneme).
    force=True: Mevcut kayıtları silip yeniden analiz + tweet yapar.
    """
    for attempt in range(4):
        try:
            logger.info(f"Market close analysis attempt {attempt + 1}/4 started...")
            
            # 1. Verileri cek
            ceilings = await scrape_uzmanpara(is_ceiling=True)
            floors = await scrape_uzmanpara(is_ceiling=False)
            
            # 2. Uzmanpara güncelleme tarihini kontrol et — hafta sonu/tatil tespiti
            today = datetime.now(_TR_TZ).date()
            market_is_open = False
            try:
                async with httpx.AsyncClient(timeout=8) as client:
                    res = await client.get("https://uzmanpara.milliyet.com.tr/borsa/en-cok-artanlar/",
                                           headers={"User-Agent": "Mozilla/5.0"})
                    if res.status_code == 200:
                        import re
                        # "Son güncelleme tarihi: 04.03.2026" formatı
                        m = re.search(r"Son\s+g[üu]ncelleme\s+tarihi[:\s]*(\d{2})\.(\d{2})\.(\d{4})", res.text)
                        if m:
                            update_date = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
                            if update_date != today:
                                logger.info(f"Uzmanpara son güncelleme: {update_date}, bugün: {today}. Piyasa kapalı, atlanıyor.")
                                return # Graceful exit: Hafta sonu/tatil
                            market_is_open = True
                            logger.info(f"Uzmanpara güncelleme tarihi bugün ({update_date}) — piyasa açık ✅")
            except Exception as e:
                logger.warning(f"Güncelleme tarihi kontrol hatası (Deneme {attempt + 1}): {e}")

            # Eger piyasa aciksa ama veri gelmemisse veya hata olmussa retry et
            if market_is_open and not ceilings and not floors:
                raise ValueError("Piyasa açık görünüyor ama tavan/taban verisi alınamadı.")

            if not ceilings and not floors:
                logger.info("Hiçbir tavan/taban hissesi bulunamadı. Hafta sonu veya tatil olabilir.")
                return # Graceful exit

            async with async_session() as session:
                # Bugün zaten kaydedilmiş mi?
                check_res = await session.execute(
                    text('SELECT COUNT(*) FROM daily_stock_market_stats WHERE "date" = :today'),
                    {"today": today}
                )
                existing_count = check_res.scalar() or 0

                if existing_count > 0 and force:
                    await session.execute(
                        text('DELETE FROM daily_stock_market_stats WHERE "date" = :today'),
                        {"today": today}
                    )
                    await session.commit()
                    logger.info(f"Force mode: {existing_count} kayıt silindi, yeniden analiz yapılacak.")
                    existing_count = 0

                if existing_count > 0:
                    logger.info(f"{today} verisi zaten DB'de ({existing_count} kayıt). Tweet aşamasına geçiliyor...")
                else:
                    await _save_market_close_data(session, today, ceilings, floors)
                
                # Bugünkü verileri oku — raw SQL
                t_res = await session.execute(
                    text("""SELECT * FROM daily_stock_market_stats 
                            WHERE "date" = :today AND is_ceiling = true
                            ORDER BY consecutive_ceiling_count DESC"""),
                    {"today": today}
                )
                c_stats_raw = t_res.fetchall()
                
                f_res = await session.execute(
                    text("""SELECT * FROM daily_stock_market_stats 
                            WHERE "date" = :today AND is_floor = true
                            ORDER BY consecutive_floor_count DESC"""),
                    {"today": today}
                )
                fl_stats_raw = f_res.fetchall()
                
                # ORM objelere dönüştür (image generator uyumu için)
                # DB sütun sırası: id(0), ticker(1), date(2), close_price(3), percent_change(4),
                #   is_ceiling(5), is_floor(6), consec_ceil(7), monthly_ceil(8),
                #   consec_floor(9), monthly_floor(10), reason(11), created_at(12)
                c_stats = []
                for r in c_stats_raw:
                    s = DailyStockMarketStat(
                        ticker=r[1], date=r[2], close_price=r[3], percent_change=r[4],
                        is_ceiling=r[5], is_floor=r[6], consecutive_ceiling_count=r[7],
                        monthly_ceiling_count=r[8], consecutive_floor_count=r[9],
                        monthly_floor_count=r[10], reason=r[11]
                    )
                    c_stats.append(s)

                fl_stats = []
                for r in fl_stats_raw:
                    s = DailyStockMarketStat(
                        ticker=r[1], date=r[2], close_price=r[3], percent_change=r[4],
                        is_ceiling=r[5], is_floor=r[6], consecutive_ceiling_count=r[7],
                        monthly_ceiling_count=r[8], consecutive_floor_count=r[9],
                        monthly_floor_count=r[10], reason=r[11]
                    )
                    fl_stats.append(s)

            # GÖRSEL ÜRETİMİ VE TWITTER
            tweet_ok = True
            tweet_error_msg = ""
            from app.services.chart_image_generator import generate_ceiling_floor_images
            from app.services.twitter_service import _safe_tweet_with_multi_media

            # ── TAVAN TWEET ──
            if c_stats:
                try:
                    tavan_images = generate_ceiling_floor_images(c_stats, is_ceiling=True)
                    tickers_str = " ".join([f"#{s.ticker}" for s in c_stats])
                    tweet_text = f"🚨 Günün TAVAN Yapan Hisseleri ve Sebepleri\n\n🎯 Hangi şirketler neden zirveyi gördü? Yapay zeka yatırımcı özetleri görsellerde!\n\n{tickers_str}"
                    _safe_tweet_with_multi_media(text=tweet_text, image_paths=tavan_images, source="market_close_analyzer")
                    logger.info(f"✅ TAVAN tweet gönderildi ({len(c_stats)} hisse)")
                except Exception as e:
                    tweet_ok = False
                    tweet_error_msg += f"Tavan tweet hata: {e} | "
                    logger.error(f"TAVAN tweet hatası: {e}")

            # Tavan ve taban tweetleri arası 60 saniye bekle
            if c_stats and fl_stats:
                logger.info("Tavan tweeti atıldı, taban tweeti için 60s bekleniyor...")
                await asyncio.sleep(60)

            # ── TABAN TWEET ──
            if fl_stats:
                try:
                    taban_images = generate_ceiling_floor_images(fl_stats, is_ceiling=False)
                    tickers_str = " ".join([f"#{s.ticker}" for s in fl_stats])
                    tweet_text = f"📉 Günün TABAN Yapan Hisseleri ve Sebepleri\n\n📌 Şirketler neden kan kaybetti? Yapay zeka analizleri görsellerde!\n\n{tickers_str}"
                    _safe_tweet_with_multi_media(text=tweet_text, image_paths=taban_images, source="market_close_analyzer")
                    logger.info(f"✅ TABAN tweet gönderildi ({len(fl_stats)} hisse)")
                except Exception as e:
                    tweet_ok = False
                    tweet_error_msg += f"Taban tweet hata: {e}"
                    logger.error(f"TABAN tweet hatası: {e}")

            # Admin Telegram — her durumda gönder
            try:
                from app.services.admin_telegram import send_admin_message
                c_count = len(c_stats) if c_stats else 0
                f_count = len(fl_stats) if fl_stats else 0
                if tweet_ok:
                    await send_admin_message(
                        f"✅ Tavan/Taban Tweet OK\n"
                        f"Tavan: {c_count} hisse | Taban: {f_count} hisse\n"
                        f"Deneme: {attempt + 1}/4"
                    )
                else:
                    await send_admin_message(
                        f"⚠️ Tavan/Taban Veri OK ama Tweet HATALI!\n"
                        f"Tavan: {c_count} | Taban: {f_count}\n"
                        f"Hata: {tweet_error_msg}\n"
                        f"Manuel: POST trigger-market-close-tweet force:true"
                    )
            except Exception:
                pass

            logger.info("Market close analysis completed (tweet_ok=%s).", tweet_ok)
            return # DB kaydedildi, tweet denendi

        except Exception as e:
            if attempt < 3:
                logger.warning(f"Market close analysis attempt {attempt + 1} failed: {e}. Retrying in 60s...")
                await asyncio.sleep(60)
            else:
                logger.error(f"All 4 attempts for market close analysis failed. Final error: {e}")
                # Admin Telegram — 4 deneme de başarısız
                try:
                    from app.services.admin_telegram import send_admin_message
                    await send_admin_message(
                        f"❌ Tavan/Taban Tweet BAŞARISIZ!\n"
                        f"4 deneme de hata aldı.\n"
                        f"Son hata: {str(e)[:200]}"
                    )
                except Exception:
                    pass


