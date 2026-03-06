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

# ═══════════════════════════════════════════════════════════════════
# System Prompt Yönetimi
# ═══════════════════════════════════════════════════════════════════

_DEFAULT_SYSTEM_PROMPT = (
    "Sen Borsa İstanbul (BIST) verileri, şirket haber akışları (KAP) ve piyasa analizi "
    "konusunda uzman, son derece titiz ve araştırmacı Kıdemli bir Finansal Analistsin. "
    "En büyük kuralın 'Sıfır Halüsinasyon' ve 'Kesin Doğruluk'tur. "
    "Verileri incelerken bir dedektif gibi şüpheci yaklaşır, her hisse kodunu, şirket "
    "unvanını ve haberin güncel geçerliliğini iki kez kontrol edersin. "
    "Asla ezberden konuşmaz veya tahminde bulunmazsın; sadece teyit edilmiş, net ve "
    "güncel gerçekleri raporlarsın. Çıktın SADECE 4-6 kelimeli tek bir Türkçe cümle "
    "ya da 'EMPTY' olacak — başka hiçbir şey yazma."
)

_custom_system_prompt: str | None = None


def get_system_prompt() -> str:
    return _custom_system_prompt if _custom_system_prompt is not None else _DEFAULT_SYSTEM_PROMPT


def set_system_prompt(new_prompt: str | None) -> None:
    global _custom_system_prompt
    _custom_system_prompt = new_prompt


def get_default_system_prompt() -> str:
    return _DEFAULT_SYSTEM_PROMPT


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
            since = datetime.now(timezone.utc) - timedelta(days=14)
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
    bilanco_search = ""
    if tavily_key:
        try:
            async with httpx.AsyncClient() as client:
                # 1a. Genel haber araması
                res = await client.post(
                    "https://api.tavily.com/search",
                    json={"api_key": tavily_key, "query": query, "search_depth": "advanced", "max_results": 3, "days": 14}
                )
                if res.status_code == 200:
                    data = res.json()
                    results = data.get("results", [])
                    external_search = "\n".join([r.get("content", "") for r in results])

                # 1b. Bilanço / finansal sonuç araması
                current_year = date.today().year
                bilanco_query = f"{ticker} bilanço finansal sonuçlar kâr gelir {current_year}"
                res2 = await client.post(
                    "https://api.tavily.com/search",
                    json={"api_key": tavily_key, "query": bilanco_query, "search_depth": "basic", "max_results": 2, "days": 14}
                )
                if res2.status_code == 200:
                    data2 = res2.json()
                    results2 = data2.get("results", [])
                    bilanco_search = "\n".join([r.get("content", "") for r in results2])
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

    # 3. Fiyat geçmişi — DB → IsYatirim → uzmanpara → bigpara fallback
    price_history = ""
    trend_statement = ""
    programmatic_reason = ""  # Kâr satışı / tepki alışı — AI boş kalırsa kullanılır
    try:
        prices = []
        # 3a. Önce DB dene — raw SQL ile "date" quoting
        async with async_session() as session:
            since = date.today() - timedelta(days=20)
            raw = text("""
                SELECT close_price FROM daily_stock_market_stats
                WHERE ticker = :ticker AND "date" >= :since
                ORDER BY "date" DESC LIMIT 15
            """)
            res = await session.execute(raw, {"ticker": ticker, "since": since})
            rows = res.fetchall()
            if rows and len(rows) >= 3:
                for row in reversed(rows):
                    prices.append(float(row[0]))

        # 3b. DB yetersizse IsYatirim dene
        if len(prices) < 5:
            try:
                from_dt = (date.today() - timedelta(days=22)).strftime("%d.%m.%Y")
                to_dt = date.today().strftime("%d.%m.%Y")
                isy_url = (
                    "https://www.isyatirim.com.tr/_layouts/15/IsYatirim.Website/Common/"
                    f"Data.aspx/HisseSenetleriFiyatBilgileri"
                    f"?hisse={ticker}&startdate={from_dt}&enddate={to_dt}&exportFlag=1"
                )
                async with httpx.AsyncClient(timeout=10) as client:
                    resp = await client.get(isy_url, headers={"User-Agent": "Mozilla/5.0"})
                    if resp.status_code == 200:
                        isy_prices = []
                        soup = BeautifulSoup(resp.text, "html.parser")
                        for tbl in soup.find_all("table"):
                            for row in tbl.find_all("tr")[1:20]:
                                cols = row.find_all("td")
                                for col in cols:
                                    try:
                                        txt = col.text.strip().replace(".", "").replace(",", ".")
                                        val = float(txt)
                                        if 0.5 < val < 500000:
                                            isy_prices.append(val)
                                            break
                                    except:
                                        pass
                        if len(isy_prices) >= 5:
                            prices = isy_prices[:15]
                            logger.info(f"IsYatirim fiyat: {ticker} — {len(prices)} gün")
            except Exception as ie:
                logger.warning(f"IsYatirim scrape error for {ticker}: {ie}")

        # 3c. Hâlâ yetersizse uzmanpara / bigpara
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
                                                if 0.5 < val < 100000:
                                                    prices.append(val)
                                                    break
                                            except:
                                                pass
                            if len(prices) >= 3:
                                break
                except Exception as e:
                    logger.warning(f"Scrape error ({scrape_url}) for {ticker}: {e}")

        # 3d. Programatik kâr satışı / tepki alışı tespiti
        # Sadece AI boş kalırsa devreye girer — prompt verisi her zaman öncelikli
        if len(prices) >= 5:
            # prices[-1] = bugün (taban/tavan), prices[-2] = dün, prices[0] = en eski
            ref_end = prices[-2]   # Dünkü kapanış (bugünün etkisi hariç)
            ref_start = prices[0]  # ~15 gün önceki kapanış
            prev_day_chg = (
                ((prices[-2] - prices[-3]) / prices[-3]) * 100
                if len(prices) >= 3 and prices[-3] > 0 else 0
            )
            if ref_start > 0:
                gain_15d = ((ref_end - ref_start) / ref_start) * 100
                # TABAN senaryosu: 15 günde %30+ yükseliş, dün %5+ artış, bugün taban → kâr satışı
                if not is_ceiling and gain_15d >= 30 and prev_day_chg >= 5:
                    programmatic_reason = "Sert yükseliş sonrası kâr satışı."
                    logger.info(
                        f"[PROG] {ticker} kâr satışı tespit: 15g={gain_15d:.0f}%, dün={prev_day_chg:.1f}%"
                    )
                # TAVAN senaryosu: 15 günde %30+ düşüş, dün %5+ düşüş, bugün tavan → tepki alışı
                elif is_ceiling and gain_15d <= -30 and prev_day_chg <= -5:
                    programmatic_reason = "Derin düşüş sonrası tepki alışı."
                    logger.info(
                        f"[PROG] {ticker} tepki alışı tespit: 15g={gain_15d:.0f}%, dün={prev_day_chg:.1f}%"
                    )

        # 3e. Genel trend özeti (AI'a context olarak verilir)
        if len(prices) >= 3:
            oldest_price = prices[0]
            newest_price = prices[-1]
            if oldest_price > 0:
                pct_change_period = ((newest_price - oldest_price) / oldest_price) * 100
                if pct_change_period <= -25:
                    trend_statement = f"\nTREND VERİSİ: Son {len(prices)} günde %{pct_change_period:.0f} düşüş."
                    price_history = trend_statement
                elif pct_change_period >= 25:
                    trend_statement = f"\nTREND VERİSİ: Son {len(prices)} günde %{pct_change_period:+.0f} yükseliş."
                    price_history = trend_statement
                else:
                    trend_statement = f"\nTREND VERİSİ: Son {len(prices)} günde %{pct_change_period:+.1f} değişim."
                    price_history = trend_statement
    except Exception as e:
        logger.warning(f"Price history error for {ticker}: {e}")

    combined_context = "\n".join(internal_news) + "\n" + external_search
    if bilanco_search and len(bilanco_search.strip()) > 20:
        combined_context += "\n\nBİLANÇO / FİNANSAL SONUÇLAR:\n" + bilanco_search
    has_news = bool(internal_news) or len(external_search.strip()) > 20 or len(bilanco_search.strip()) > 20
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
            if is_ceiling:
                ipo_rule = "\n- Bu hisse YENİ HALKA ARZ ve henüz ilk 10 işlem gününde. 'Halka arz sonrası yoğun talep.' yaz."
            else:
                ipo_rule = "\n- Bu hisse YENİ HALKA ARZ ve henüz ilk 10 işlem gününde. 'Halka arz sonrası kâr satışı.' yaz."
        elif days_since <= 30:
            if is_ceiling:
                ipo_rule = "\n- Bu hisse halka arz olalı {0} gün oldu. Tavan serisi devam ediyorsa 'Halka arz sonrası talep devam ediyor.' yaz, somut haber varsa onu tercih et.".format(days_since)
            else:
                ipo_rule = "\n- Bu hisse halka arz olalı {0} gün oldu. Düşüyorsa 'Tavan serisi sonrası kâr realizasyonu.' yaz, somut haber varsa onu tercih et.".format(days_since)
        else:
            ipo_rule = ("\n- Bu hisse halka arz oldu ama ilk 30 günü geçti. Artık basit 'halka arz' açıklaması yetmez."
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

    hareket = "TAVAN (+%9.95 civarı yükseliş)" if is_ceiling else "TABAN (-%9.95 civarı düşüş)"
    prompt = f"""Sen Türkiye borsası (BIST) uzmanısın. #{ticker} hissesi bugün {hareket} yaptı.
Görevin: Bu fiyat hareketinin GERÇEK sebebini bulmak ve SADECE 4-6 kelime ile yazmak.

━━━ ADIM 1 — VERİLERİ DİKKATLİCE İNCELE ━━━
{context_str}
{ipo_rule}

━━━ ADIM 2 — SIRAYLA KONTROL ET ━━━
A) BİLANÇO / FİNANSAL SONUÇ: Şirket son 7 günde finansal tablo, kâr/zarar, gelir açıklaması yaptı mı?
   → Evet ise: "Güçlü yıllık bilanço açıklandı." / "Beklenti altı bilanço açıklandı." gibi yaz. Spesifik rakam YAZMA.
   → BİLANÇO DÖNEMİ KURALI: Haberde bilanço dönemi belirtilmediyse mevcut takvime göre belirle:
     Ocak-Mart arası açıklanan → "yıllık" (12 aylık/4Ç), Nisan-Mayıs → "3 aylık" (1Ç),
     Temmuz-Ağustos → "6 aylık" (yarıyıl), Ekim-Kasım → "9 aylık" (3Ç).
     Bugün {date.today().strftime("%d %B")} — buna göre doğru dönemi yaz. ASLA tahmin etme.

B) SERMAYE HAREKETLERİ: Bedelsiz/bedelli sermaye artırımı, temettü, hisse geri alımı var mı?
   → İPTAL EDİLMİŞ veya GERİ ÇEKİLMİŞ kararları ASLA yazma.

C) KURUMSAL OLAY: İhale kazanma/kaybetme, önemli sözleşme, ortaklık, proje ihalesi, lisans var mı?

D) HUKUKİ/YÖNETİM: Tutukluluk kararı, beraat, mahkeme kararı, yönetim değişikliği var mı?

E) HEDEFLİ FİYAT / ANALIST RAPORU: Aracı kurum raporu var mı?
   → Spesifik TL rakamı ASLA yazma. "Yüksek hedef fiyat raporu." / "Düşük hedef fiyat raporu." yaz.

F) HALKA ARZ: Yeni halka arz mı? (IPO bilgisi kontrol et)

G) SEKTÖR/MAKRO: Sektörü doğrudan etkileyen düzenleme, kota, yasal karar var mı?

━━━ ADIM 3 — TICKER DOĞRULA ━━━
Bulduğun haberin #{ticker} ŞİRKETİNE ait olduğundan %100 emin ol.
Aynı/benzer isimdeki BAŞKA bir şirketin haberi mi? → O zaman EMPTY yaz.
Haber 14 günden eski mi? → EMPTY yaz.
Karar iptal mi edilmiş? → EMPTY yaz.

━━━ ADIM 4 — ÇIKTI ━━━
Yukarıda A-G'den birinde somut bulgu varsa → 4-6 kelime ile Türkçe yaz.
Somut bulgu yoksa → sadece "EMPTY" yaz.

❌ YASAK: trend yorumu, "momentum", "alıcı/satıcı baskısı", "hacimli", "volatilite",
   "piyasa beklentisi", "yatırımcı talebi", "konsolide", "istikrarlı", "potansiyel",
   rakam içeren hedef fiyat, rakam içeren kâr/zarar tutarı.
✅ İSTENEN FORMAT: "Bedelsiz sermaye artırımı kararı alındı." / "Güçlü 3Ç bilançosu açıklandı." /
   "Yüksek hedef fiyat raporu yayınlandı." / "Önemli ihale sözleşmesi imzalandı."
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
        """AI yanıtını temizle — EMPTY veya jenerik ise boş dön, hedef fiyat rakamlarını sil."""
        import re
        t = raw.strip().replace('"', '').replace("'", "")
        if not t or t.upper() == "EMPTY" or len(t) < 5:
            return ""
        if any(x in t.lower() for x in bad):
            return ""
        # Hedef fiyat rakamlarını temizle — "68,36 TL" → kaldır (yatırım tavsiyesi riski)
        t = re.sub(r'\d+[.,]\d+\s*TL', '', t).strip()
        t = re.sub(r'hedef\s*(?:fiyat[ıi]?\s*)?\d+[.,]?\d*', 'hedef fiyat', t, flags=re.IGNORECASE).strip()
        # Çift boşlukları temizle
        t = re.sub(r'\s{2,}', ' ', t).strip()
        # Sonundaki noktalama düzelt
        if t and t[-1] not in '.!':
            t += '.'
        if len(t) < 5:
            return ""
        return t

    # Tüm modeller için ortak sistem kişiliği (Sıfır Halüsinasyon prensibi)
    _SYSTEM_PERSONA = get_system_prompt()

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
                        "max_tokens": 150,
                        "system": _SYSTEM_PERSONA,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.1
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
                    json={
                        "model": "gpt-4o",
                        "max_tokens": 150,
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PERSONA},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.1
                    }
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
                    json={
                        "model": "claude-sonnet-4-6",
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PERSONA},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.1
                    }
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
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "gemini-2.5-pro",
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PERSONA},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.1
                    }
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

    # Tüm AI modelleri boş / başarısız — programatik fallback
    if programmatic_reason:
        logger.info(f"[PROG FALLBACK] {ticker}: {programmatic_reason}")
        return programmatic_reason
    return ""

async def _save_market_close_data(session, today, ceilings, floors):
    """3 fazlı: 1) DB'den geçmiş veri çek  2) AI paralel analiz  3) DB'ye kaydet"""
    # FIFO: 32 günden eski kayıtları temizle
    cleanup_date = today - timedelta(days=32)
    await session.execute(
        text('DELETE FROM daily_stock_market_stats WHERE "date" < :cutoff'),
        {"cutoff": cleanup_date}
    )

    # ── FAZ 1: DB'den geçmiş verileri çek (hızlı) ──
    # Yardımcı: önceki kaydın ardışık olup olmadığını kontrol et
    def _is_consecutive_day(prev_date, today_date):
        """Önceki kayıt 'dünkü işlem gününden' mi? Hafta sonu/tatil toleransı ile."""
        gap = (today_date - prev_date).days
        if gap == 1:
            return True  # Düz ardışık (Pzt→Sal, Sal→Çar, ...)
        if gap <= 3 and today_date.weekday() == 0:
            return True  # Cuma→Pazartesi (hafta sonu köprüsü)
        if gap == 2 and today_date.weekday() in (0, 1):
            return True  # Tatil köprüsü (Perş→Pzt veya Cum→Sal)
        return False

    prepared = []
    for stock in ceilings:
        try:
            ticker = stock["ticker"]
            past_res = await session.execute(
                text("""SELECT is_ceiling, is_floor, consecutive_ceiling_count,
                        consecutive_floor_count, "date"
                        FROM daily_stock_market_stats
                        WHERE ticker = :ticker ORDER BY "date" DESC"""),
                {"ticker": ticker}
            )
            past = past_res.fetchall()
            # Seri kontrolü: önceki kayıt TAVAN + ARDIŞIK GÜN olmalı
            if past and past[0][0] and _is_consecutive_day(past[0][4], today):
                consec = past[0][2] + 1
            else:
                consec = 1
            monthly = sum(1 for r in past if r[0] and (today - r[4]).days <= 30) + 1
            prepared.append({"ticker": ticker, "price": Decimal(str(stock["price"])),
                           "pct": Decimal(str(stock["change"])), "is_ceiling": True,
                           "consec": consec, "monthly": monthly})
        except Exception as e:
            logger.error(f"[PREP] {stock.get('ticker','?')} hata: {e}")

    for stock in floors:
        try:
            ticker = stock["ticker"]
            past_res = await session.execute(
                text("""SELECT is_ceiling, is_floor, consecutive_ceiling_count,
                        consecutive_floor_count, "date"
                        FROM daily_stock_market_stats
                        WHERE ticker = :ticker ORDER BY "date" DESC"""),
                {"ticker": ticker}
            )
            past = past_res.fetchall()
            # Seri kontrolü: önceki kayıt TABAN + ARDIŞIK GÜN olmalı
            if past and past[0][1] and _is_consecutive_day(past[0][4], today):
                consec = past[0][3] + 1
            else:
                consec = 1
            monthly = sum(1 for r in past if r[1] and (today - r[4]).days <= 30) + 1
            prepared.append({"ticker": ticker, "price": Decimal(str(stock["price"])),
                           "pct": Decimal(str(stock["change"])), "is_ceiling": False,
                           "consec": consec, "monthly": monthly})
        except Exception as e:
            logger.error(f"[PREP] {stock.get('ticker','?')} hata: {e}")

    logger.info(f"Faz1 OK: {len(prepared)} hisse. AI paralel analiz başlıyor...")

    # ── FAZ 2: AI analiz — 5 paralel, DB bağımsız ──
    sem = asyncio.Semaphore(5)
    async def _ai(s):
        async with sem:
            try:
                return await _analyze_reason_with_ai(
                    ticker=s["ticker"], is_ceiling=s["is_ceiling"],
                    price=float(s["price"]), pct=float(s["pct"]),
                    consec=s["consec"], monthly=s["monthly"])
            except Exception as e:
                logger.error(f"AI {s['ticker']}: {e}")
                return ""

    reasons = await asyncio.gather(*[_ai(s) for s in prepared])
    ai_ok = sum(1 for r in reasons if r)
    logger.info(f"Faz2 OK: {ai_ok}/{len(prepared)} AI başarılı.")

    # ── FAZ 3: DB'ye kaydet (hızlı) ──
    saved = 0
    for s, reason in zip(prepared, reasons):
        try:
            lbl = "TAVAN" if s["is_ceiling"] else "TABAN"
            logger.info(f"[{lbl}] {s['ticker']}: {'✅' if reason else '❌'} | {reason[:50] if reason else 'boş'}")
            session.add(DailyStockMarketStat(
                ticker=s["ticker"], date=today,
                close_price=s["price"], percent_change=s["pct"],
                is_ceiling=s["is_ceiling"], is_floor=not s["is_ceiling"],
                consecutive_ceiling_count=s["consec"] if s["is_ceiling"] else 0,
                monthly_ceiling_count=s["monthly"] if s["is_ceiling"] else 0,
                consecutive_floor_count=s["consec"] if not s["is_ceiling"] else 0,
                monthly_floor_count=s["monthly"] if not s["is_ceiling"] else 0,
                reason=(reason or "")[:100]
            ))
            saved += 1
        except Exception as e:
            logger.error(f"[KAYIT] {s['ticker']}: {e}")

    await session.commit()
    logger.info(f"Faz3 OK: {saved}/{len(prepared)} kayıt, AI: {ai_ok} başarılı.")


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
                
                # Bugünkü verileri oku — ORM query (sütun sırası sorunu yok)
                from sqlalchemy import select
                c_stats = (await session.execute(
                    select(DailyStockMarketStat).where(
                        DailyStockMarketStat.date == today,
                        DailyStockMarketStat.is_ceiling == True
                    ).order_by(DailyStockMarketStat.consecutive_ceiling_count.desc())
                )).scalars().all()

                fl_stats = (await session.execute(
                    select(DailyStockMarketStat).where(
                        DailyStockMarketStat.date == today,
                        DailyStockMarketStat.is_floor == True
                    ).order_by(DailyStockMarketStat.consecutive_floor_count.desc())
                )).scalars().all()

            # GÖRSEL ÜRETİMİ VE TWITTER
            tweet_ok = True
            tweet_error_msg = ""
            from app.services.chart_image_generator import generate_ceiling_floor_images
            import app.services.twitter_service as _tw_svc

            # Disclaimer flood tweet — her ana tweetin reply'ı olarak gönderilir
            ai_disclaimer = (
                "📌 Dipnot / Uyarı: Görsellerdeki veriler, haber akışlarını tarayan "
                "özel eğitimli yapay zeka modelleri tarafından oluşturulmuştur. Modelimiz "
                "yüksek doğrulukla çalışsa da nadiren güncel olmayan haberleri veya isim/kod "
                "benzerliklerini rapora yansıtabilir. Lütfen işlem yapmadan önce mutlaka "
                "kendi araştırmanızı yapın ve teyit edin. Bu veriler yatırım tavsiyesi "
                "(YTD) niteliği taşımaz."
            )

            # ── TAVAN TWEET (0 hisse dahil) ──
            try:
                # Ek hisseler — tavan ≤6 ise en çok artanları ekle
                tavan_supp = []
                if len(c_stats) <= 6:
                    try:
                        tavan_supp_raw = await scrape_uzmanpara_supplementary(
                            is_ceiling=True,
                            exclude_tickers=[s.ticker for s in c_stats]
                        )
                        # SimpleNamespace'e çevir (generate_ceiling_floor_images uyumu)
                        from types import SimpleNamespace
                        tavan_supp = [SimpleNamespace(ticker=s["ticker"], close_price=s["price"], percent_change=s["change"]) for s in tavan_supp_raw]
                    except Exception as e:
                        logger.warning(f"Tavan supplementary hata: {e}")
                tavan_images = generate_ceiling_floor_images(c_stats, is_ceiling=True, supplementary=tavan_supp)
                if c_stats:
                    tickers_str = " ".join([f"#{s.ticker}" for s in c_stats])
                    tweet_text = (
                        f"📈 Günün TAVAN Yapan Hisseleri ve Sebepleri!\n\n"
                        f"Hangi şirketler neden uçuşa geçti? Yapay zeka modelimizin "
                        f"derlediği haber analizleri görsellerde! 🚀👇\n\n"
                        f"{tickers_str}"
                    )
                else:
                    tweet_text = (
                        f"📈 Bugün TAVAN yapan hisse yok!\n\n"
                        f"En çok yükselen hisseler görselde! 📊👇"
                    )
                _tw_svc._safe_tweet_with_multi_media(
                    text=tweet_text, image_paths=tavan_images,
                    source="market_close_analyzer"
                )
                logger.info(f"✅ TAVAN tweet gönderildi ({len(c_stats)} hisse)")
                # Disclaimer flood reply — 5 saniye bekle sonra at
                await asyncio.sleep(5)
                tavan_tweet_id = _tw_svc._last_tweet_id
                if tavan_tweet_id and tavan_tweet_id != "?":
                    _tw_svc._safe_reply_tweet(ai_disclaimer, tavan_tweet_id)
                    logger.info(f"✅ TAVAN disclaimer reply gönderildi (reply_to={tavan_tweet_id})")
                else:
                    logger.warning("TAVAN disclaimer reply: tweet ID alınamadı, atlanıyor")
            except Exception as e:
                tweet_ok = False
                tweet_error_msg += f"Tavan tweet hata: {e} | "
                logger.error(f"TAVAN tweet hatası: {e}")

            # Tavan ve taban tweetleri arası 60 saniye bekle
            logger.info("Tavan tweeti atıldı, taban tweeti için 60s bekleniyor...")
            await asyncio.sleep(60)

            # ── TABAN TWEET (0 hisse dahil) ──
            try:
                # Ek hisseler — taban ≤6 ise en çok düşenleri ekle
                taban_supp = []
                if len(fl_stats) <= 6:
                    try:
                        taban_supp_raw = await scrape_uzmanpara_supplementary(
                            is_ceiling=False,
                            exclude_tickers=[s.ticker for s in fl_stats]
                        )
                        from types import SimpleNamespace
                        taban_supp = [SimpleNamespace(ticker=s["ticker"], close_price=s["price"], percent_change=s["change"]) for s in taban_supp_raw]
                    except Exception as e:
                        logger.warning(f"Taban supplementary hata: {e}")
                taban_images = generate_ceiling_floor_images(fl_stats, is_ceiling=False, supplementary=taban_supp)
                if fl_stats:
                    tickers_str = " ".join([f"#{s.ticker}" for s in fl_stats])
                    tweet_text = (
                        f"📉 Günün TABAN Yapan Hisseleri ve Sebepleri!\n\n"
                        f"Şirketler neden kan kaybetti? Yapay zeka modelimizin "
                        f"derlediği haber analizleri görsellerde! 📊👇\n\n"
                        f"{tickers_str}"
                    )
                else:
                    tweet_text = (
                        f"📉 Bugün TABAN yapan hisse yok!\n\n"
                        f"En çok düşen hisseler görselde! 📊👇"
                    )
                _tw_svc._safe_tweet_with_multi_media(
                    text=tweet_text, image_paths=taban_images,
                    source="market_close_analyzer"
                )
                logger.info(f"✅ TABAN tweet gönderildi ({len(fl_stats)} hisse)")
                # Disclaimer flood reply — 5 saniye bekle sonra at
                await asyncio.sleep(5)
                taban_tweet_id = _tw_svc._last_tweet_id
                if taban_tweet_id and taban_tweet_id != "?":
                    _tw_svc._safe_reply_tweet(ai_disclaimer, taban_tweet_id)
                    logger.info(f"✅ TABAN disclaimer reply gönderildi (reply_to={taban_tweet_id})")
                else:
                    logger.warning("TABAN disclaimer reply: tweet ID alınamadı, atlanıyor")
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


