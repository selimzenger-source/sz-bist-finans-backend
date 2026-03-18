"""Resmi Gazete Scraper — Borsa etkisi olabilecek kararlari yakalar.

Her gun sabah 06:00, 12:00, 18:00 (TR saati) kontrol eder.
Fihrist sayfasindan basliklari parse eder, ilgili PDF'leri indirir,
AI ile BIST sirketlerine/sektorlerine etkisini analiz eder.
Kuvvetli etki tespit edilirse tweet atar.

Kaynak: https://www.resmigazete.gov.tr/fihrist?tarih=YYYY-MM-DD
"""

import io
import re
import json
import asyncio
import logging
from datetime import date, datetime, timedelta

import httpx
import pdfplumber
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

RG_BASE = "https://www.resmigazete.gov.tr"
RG_FIHRIST_URL = f"{RG_BASE}/fihrist"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Referer": "https://www.resmigazete.gov.tr/",
}

# DB state key
SCRAPER_STATE_KEY = "rg_last_processed_date"

# Race condition korumasi
_rg_check_lock = asyncio.Lock()

# İlgili bölüm başlıkları (küçük harf)
_RELEVANT_SECTIONS = [
    "cumhurbaşkanı karar",
    "cumhurbaskani karar",
    "yönetmelik",
    "yonetmelik",
    "tebliğ",
    "teblig",
    "kurul karar",
    "genelge",
    "kanun",
    "cumhurbaşkanlığı karar",
    "cumhurbaskanligi karar",
    "karar",
]

# Borsa ile alakasız konular (filtrelemek için)
_IRRELEVANT_KEYWORDS = [
    "üniversite", "universite",
    "eğitim-öğretim", "egitim-ogretim",
    "sınav yönetmeliği", "sinav yonetmeligi",
    "yargıtay", "yargitay",
    "anayasa mahkemesi",
    "danıştay", "danistay",
    "artırma, eksiltme ve ihale",
    "yargı ilân", "yargi ilan",
    "çeşitli ilân", "cesitli ilan",
    "döviz kur", "doviz kur",
]


class ResmiGazeteScraper:
    """Resmi Gazete fihrist sayfasini parse eder."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30,
            headers=HEADERS,
            follow_redirects=True,
            verify=False,
        )

    async def close(self):
        await self.client.aclose()

    async def fetch_fihrist(self, tarih: date) -> list[dict]:
        """Belirli bir tarihin fihrist sayfasini cekyp baslik+link listesi dondurur.

        Returns:
            [{"title": str, "url": str, "section": str, "is_pdf": bool}, ...]
        """
        tarih_str = tarih.strftime("%Y-%m-%d")
        url = f"{RG_FIHRIST_URL}?tarih={tarih_str}"

        try:
            resp = await self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.error("Resmi Gazete fihrist cekilemedi (%s): %s", tarih_str, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        items = []
        current_section = ""

        # Fihrist sayfasında h2/h3 section başlıkları + a link'leri var
        for tag in soup.find_all(["h2", "h3", "h4", "a"]):
            if tag.name in ("h2", "h3", "h4"):
                current_section = tag.get_text(strip=True)
                continue

            href = tag.get("href", "")
            text = tag.get_text(strip=True)
            if not text or not href:
                continue

            # Sadece resmigazete linkleri
            if href.startswith("/"):
                href = f"{RG_BASE}{href}"
            elif not href.startswith("http"):
                continue

            # Resmi Gazete eskiler/dosya linkleri
            if "resmigazete.gov.tr" not in href:
                continue

            is_pdf = href.lower().endswith(".pdf")
            items.append({
                "title": text,
                "url": href,
                "section": current_section,
                "is_pdf": is_pdf,
            })

        logger.info("Resmi Gazete fihrist (%s): %d madde bulundu", tarih_str, len(items))
        return items

    def filter_relevant_items(self, items: list[dict]) -> list[dict]:
        """Borsa/ekonomi ile ilgili olabilecek maddeleri filtrele.

        İlk filtre: Bölüm başlığına göre (üniversite yönetmeliği vs. atla)
        """
        relevant = []
        for item in items:
            title_lower = item["title"].lower()
            section_lower = item["section"].lower()
            combined = f"{section_lower} {title_lower}"

            # İlgisiz konuları atla
            if any(kw in combined for kw in _IRRELEVANT_KEYWORDS):
                continue

            # İlgili bölüm başlığı var mı
            is_relevant_section = any(kw in section_lower for kw in _RELEVANT_SECTIONS)

            # Başlıkta ekonomik anahtar kelimeler
            economic_keywords = [
                "banka", "sermaye", "vergi", "teşvik", "tesvik",
                "gümrük", "gumruk", "ithalat", "ihracat",
                "enerji", "maden", "petrol", "doğalgaz", "dogalgaz",
                "sigorta", "borsa", "finans",
                "holding", "gayrimenkul",
                "ihalenin", "özelleştirme", "ozellestirme",
                "bddk", "spk", "tcmb", "epdk", "rekabet",
                "kur", "faiz", "enflasyon",
                "bakan", "müsteşar", "müdür", "başkan",
                "atama", "görevden", "gorevden",
                "katılım", "katilim",
                "lisans", "ruhsat",
            ]
            has_economic_keyword = any(kw in combined for kw in economic_keywords)

            if is_relevant_section or has_economic_keyword:
                relevant.append(item)

        logger.info("Resmi Gazete filtre: %d/%d madde ilgili", len(relevant), len(items))
        return relevant

    async def download_pdf_text(self, url: str) -> str | None:
        """PDF indir ve text olarak dondur.

        Yontem 1: pdfplumber (metin katmanli PDF'ler)
        Yontem 2: PyMuPDF/fitz (daha iyi text extraction)
        Yontem 3: pdf2image + pytesseract OCR (taranmis PDF'ler)
        """
        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                logger.warning("PDF indirilemedi (%d): %s", resp.status_code, url)
                return None

            content_type = resp.headers.get("content-type", "")
            # Resmi Gazete bazen text/html dondurur (redirect/hata)
            if "text/html" in content_type:
                logger.warning("PDF yerine HTML geldi: %s", url)
                return None

            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning("PDF cok kucuk (%d byte): %s", len(pdf_bytes), url)
                return None

            result = None

            # Yontem 1: pdfplumber
            try:
                with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                    texts = []
                    for page in pdf.pages[:10]:
                        page_text = page.extract_text()
                        if page_text:
                            texts.append(page_text)
                    if texts:
                        result = "\n\n".join(texts)
            except Exception as e:
                logger.debug("pdfplumber basarisiz: %s", e)

            # Yontem 2: PyMuPDF (fitz) — daha iyi text extraction
            if not result or len(result.strip()) < 50:
                try:
                    import fitz  # PyMuPDF
                    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
                    texts = []
                    for page_num in range(min(len(doc), 10)):
                        page_text = doc[page_num].get_text()
                        if page_text and page_text.strip():
                            texts.append(page_text.strip())
                    doc.close()
                    if texts:
                        result = "\n\n".join(texts)
                        logger.info("PyMuPDF basarili: %d karakter", len(result))
                except ImportError:
                    logger.debug("PyMuPDF (fitz) yuklu degil, atlandi")
                except Exception as e:
                    logger.debug("PyMuPDF basarisiz: %s", e)

            # Yontem 3: OCR (pdf2image + pytesseract)
            if not result or len(result.strip()) < 50:
                try:
                    from pdf2image import convert_from_bytes
                    import pytesseract

                    images = convert_from_bytes(pdf_bytes, first_page=1, last_page=3, dpi=200)
                    ocr_texts = []
                    for img in images:
                        ocr_text = pytesseract.image_to_string(img, lang="tur")
                        if ocr_text and ocr_text.strip():
                            ocr_texts.append(ocr_text.strip())
                    if ocr_texts:
                        result = "\n\n".join(ocr_texts)
                        logger.info("OCR basarili: %d karakter", len(result))
                except ImportError:
                    logger.debug("pdf2image/pytesseract yuklu degil, OCR atlandi")
                except Exception as e:
                    logger.warning("OCR hatasi: %s", e)

            if result and len(result.strip()) > 20:
                logger.info("PDF metin OK (%d kar): %s → %s...",
                            len(result), url.split("/")[-1], result[:200])
            else:
                logger.warning("PDF metin BOŞ/yetersiz: %s", url)
                result = None

            return result

        except Exception as e:
            logger.warning("PDF parse hatasi (%s): %s", url, e)
            return None

    async def download_htm_text(self, url: str) -> str | None:
        """HTM sayfayı indirip text olarak döndür."""
        try:
            resp = await self.client.get(url)
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")
            # Ana icerik alani
            content = soup.find("div", class_="content") or soup.find("body")
            if content:
                return content.get_text(separator="\n", strip=True)
            return soup.get_text(separator="\n", strip=True)
        except Exception as e:
            logger.warning("HTM parse hatasi (%s): %s", url, e)
            return None


async def check_resmi_gazete():
    """Ana kontrol fonksiyonu — scheduler tarafindan cagrilir."""
    async with _rg_check_lock:
        try:
            await _check_resmi_gazete_inner()
        except Exception as e:
            logger.error("Resmi Gazete kontrol hatasi: %s", e, exc_info=True)


async def _check_resmi_gazete_inner():
    """Resmi Gazete'yi kontrol et, yeni ilgili karar varsa tweet at."""
    from app.database import async_session
    from app.models.scraper_state import ScraperState
    from sqlalchemy import select

    # Render UTC'de calisiyor, Turkiye saatine cevir
    from datetime import timezone, timedelta
    tr_tz = timezone(timedelta(hours=3))
    now_tr = datetime.now(tr_tz)
    today = now_tr.date()
    today_str = today.isoformat()

    # Hafta sonu kontrolu — RG hafta sonu nadiren cikar, atla
    if now_tr.weekday() >= 5:  # 5=Cumartesi, 6=Pazar
        logger.debug("Resmi Gazete: Hafta sonu, atlanıyor")
        return

    # Son islenmis tarihi DB'den oku
    async with async_session() as session:
        result = await session.execute(
            select(ScraperState).where(ScraperState.key == SCRAPER_STATE_KEY)
        )
        state = result.scalar_one_or_none()

        last_date_str = state.value if state else None

    # Gunluk tweet limiti dolmussa erken cik (gereksiz PDF indirme + AI maliyetini onle)
    already_tweeted_early = await _get_tweeted_urls(today_str)
    RG_DAILY_TWEET_LIMIT = 3
    if len(already_tweeted_early) >= RG_DAILY_TWEET_LIMIT:
        logger.info("Resmi Gazete: Gunluk tweet limiti zaten doldu (%d/%d), tarama atlanıyor",
                    len(already_tweeted_early), RG_DAILY_TWEET_LIMIT)
        return
    scraper = ResmiGazeteScraper()
    try:
        items = await scraper.fetch_fihrist(today)
        if not items:
            logger.info("Resmi Gazete: Bugun (%s) henuz icerik yok", today_str)
            return

        relevant = scraper.filter_relevant_items(items)
        if not relevant:
            logger.info("Resmi Gazete: Bugun (%s) borsa ile ilgili karar yok (%d karar tarandi)", today_str, len(items))
            await _save_state(today_str)
            return

        logger.info("Resmi Gazete: %d ilgili karar bulundu, AI analiz basliyor...", len(relevant))

        # Telegram — tarama durumu (ilk gunlerde kontrol icin)
        try:
            from app.services.admin_telegram import send_admin_message
            await send_admin_message(
                f"🔍 <b>RG Tarama</b> ({today_str})\n"
                f"Toplam: {len(items)} karar\n"
                f"Filtre sonrası: {len(relevant)} ilgili karar\n"
                f"AI analiz başlıyor...",
                silent=True,
            )
        except Exception:
            pass

        # İçerikleri indir (max 5 PDF/HTM)
        contents = []
        for item in relevant[:8]:
            text = None
            if item["is_pdf"]:
                text = await scraper.download_pdf_text(item["url"])
            else:
                text = await scraper.download_htm_text(item["url"])

            if text:
                logger.info("İçerik alındı [%s]: %d kar → %s...",
                            item["title"][:40], len(text), text[:150])
                contents.append({
                    "title": item["title"],
                    "section": item["section"],
                    "url": item["url"],
                    "text": text[:3000],  # max 3000 char per item
                })
            else:
                logger.warning("İçerik alınamadı: %s → sadece başlık ile devam", item["title"][:60])
                # text alinamamissa bile basligi ekle
                contents.append({
                    "title": item["title"],
                    "section": item["section"],
                    "url": item["url"],
                    "text": item["title"],
                })

        # Daha once tweetlenenleri filtrele
        already_tweeted = await _get_tweeted_urls(today_str)
        new_contents = [c for c in contents if c["url"] not in already_tweeted]

        if not new_contents:
            logger.info("Resmi Gazete: Tum ilgili kararlar zaten tweetlendi")
            return

        # Günlük tweet limiti — spam önleme (max 3 RG tweeti/gün)
        RG_DAILY_TWEET_LIMIT = 3
        tweeted_today = len(already_tweeted)
        if tweeted_today >= RG_DAILY_TWEET_LIMIT:
            logger.info("Resmi Gazete: Gunluk tweet limiti doldu (%d/%d)",
                        tweeted_today, RG_DAILY_TWEET_LIMIT)
            return

        # AI analiz — kuvvetli borsa etkisi olan kararlari bul
        analysis = await _ai_analyze_gazette(new_contents, today)
        if not analysis:
            logger.info("Resmi Gazete: AI analize gore borsa etkisi yok")
            await _save_state(today_str)
            return

        # Max kaç tweet atabiliriz
        remaining_slots = RG_DAILY_TWEET_LIMIT - tweeted_today
        if len(analysis) > remaining_slots:
            logger.info("Resmi Gazete: %d karar bulundu ama %d slot kaldi, en onemliler alinacak",
                        len(analysis), remaining_slots)
            analysis = analysis[:remaining_slots]

        # Telegram bildirim — AI ne buldu, kontrol edelim
        from app.services.admin_telegram import send_admin_message
        for dec in analysis:
            tickers_str = ", ".join(f"#{t}" for t in dec.get("tickers", [])) or "ticker yok"
            sentiment = dec.get("sentiment", "?")
            sentiment_emoji = {"pozitif": "🟢", "negatif": "🔴", "nötr": "⚪"}.get(sentiment, "❓")
            tg_text = (
                f"📰 <b>Resmi Gazete Karar Yakalandı!</b>\n\n"
                f"<b>{dec.get('title', '?')}</b>\n\n"
                f"{dec.get('summary', '')}\n\n"
                f"{sentiment_emoji} Etki: {dec.get('impact', '?')}\n"
                f"🏷️ Ticker: {tickers_str}\n"
                f"🔗 {dec.get('source_url', '')}"
            )
            try:
                await send_admin_message(tg_text, silent=False)
            except Exception:
                pass

        # Tweet at
        from app.services.twitter_service import tweet_resmi_gazete_decision
        for decision in analysis:
            success = tweet_resmi_gazete_decision(
                decision=decision,
                gazette_date=today,
            )
            if success:
                await _mark_as_tweeted(today_str, decision.get("source_url", ""))
                logger.info("Resmi Gazete tweet OK: %s", decision.get("title", "")[:60])
                # Telegram — tweet başarılı
                try:
                    await send_admin_message(
                        f"✅ <b>RG Tweet Atıldı</b>\n{decision.get('title', '')[:80]}",
                        silent=True,
                    )
                except Exception:
                    pass
            else:
                logger.warning("Resmi Gazete tweet FAIL: %s", decision.get("title", "")[:60])
                try:
                    await send_admin_message(
                        f"❌ <b>RG Tweet BAŞARISIZ</b>\n{decision.get('title', '')[:80]}",
                        silent=False,
                    )
                except Exception:
                    pass

        await _save_state(today_str)

    finally:
        await scraper.close()


async def _ai_analyze_gazette(contents: list[dict], gazette_date: date) -> list[dict] | None:
    """AI ile Resmi Gazete iceriklerini analiz et.

    Returns:
        Kuvvetli borsa etkisi olan kararlar listesi:
        [{"title": str, "summary": str, "impact": str, "tickers": [str], "sentiment": str, "source_url": str}, ...]
        veya None (etki yok)
    """
    from app.services.twitter_service import _get_bist_ticker_cache

    # Ticker listesi
    ticker_lines = _get_bist_ticker_cache()
    ticker_text = "\n".join(ticker_lines[:700]) if ticker_lines else "(ticker listesi alinamadi)"

    # İçerikleri birleştir
    gazette_text = ""
    for i, c in enumerate(contents, 1):
        gazette_text += f"\n\n--- KARAR {i}: [{c['section']}] {c['title']} ---\n"
        gazette_text += c["text"]
        gazette_text += f"\nKaynak URL: {c['url']}\n"

    system_prompt = """Sen bir Türk borsa analisti ve finans editörüsün.
Sana bugünkü Resmi Gazete'den çıkarılmış kararlar verilecek.

GÖREV: Bu kararlardan SADECE borsa/BIST şirketlerine DOĞRUDAN ve KUVVETLİ etkisi olanları bul.

KUVVETLİ ETKİ ÖRNEKLERİ:
- Bir BIST şirketinin doğrudan adı geçen kararlar (kuruluş, birleşme, tasfiye, ceza)
- BIST şirketinin ortağı/ana şirketine yönelik kararlar
- Sektörel düzenlemeler (bankacılık sermaye yeterlilik oranı değişikliği → banka hisseleri)
- Vergi/teşvik değişiklikleri (ÖTV, KDV, yatırım teşviki → ilgili sektör)
- Önemli atama/görevden almalar (TCMB başkanı, BDDK başkanı, ekonomi bakanı)
- İthalat/ihracat kısıtlamaları veya serbestleştirmeleri
- Enerji, maden, telekom, ilaç sektörüne yönelik düzenlemeler

HOLDİNG / İŞTİRAK / GRUP BAĞLANTILARI — ÇOK ÖNEMLİ:
Kararda geçen şirket doğrudan BIST'te olmasa bile, holding/grup bağlantısıyla BIST hissesini etkiler.
Örnek bağlantılar:
- Fuzul Holding, Fuzul Yapı, Fuzul GYO → FZLGY (Fuzul GYO)
- Koç Holding grubu → KCHOL, ARCLK, FROTO, TOASO, TUPRS, YKBNK, AYGAZ, OTKAR
- Sabancı Holding grubu → SAHOL, AKBNK, KRDMD, CIMSA, ENKAI
- Zorlu Holding → ZOREN, VESBE, VESTL
- Anadolu Grubu → AGHOL, AEFES, ANACM, MIGRS
- Doğuş Holding → DOHOL, GARAN, DOAS
- Yıldız Holding → ULKER, BIZIM, GODMC
- Cengiz Holding → ilgili enerji/inşaat şirketleri
- Kalyon Holding → ilgili enerji/inşaat şirketleri
- Limak Holding → ilgili enerji/havalimanı şirketleri
- Eczacıbaşı → ECILC, ECZYT
- Alarko → ALARK, ALCAR, ALCTL
- Oyak → OYAKC, EREGL, ISDMR
- Turkcell → TCELL, Superonline
- Türk Telekom → TTKOM, TTRAK değil
- THY → THYAO, TGS, PGSUS (rakip etki)
- İş Bankası grubu → ISCTR, ISMEN, ISYAT, SISE, TSKB
- Garanti BBVA → GARAN
- QNB Finansbank → QNBFB (halka açık değil ama sektör etkisi)
Kararda "Fuzul" geçiyorsa → FZLGY ticker'ını kullan.
Kararda "Koç" geçiyorsa → KCHOL + ilgili grup şirketlerini kullan.
Bu bağlantıları KENDİN KUR — sadece ticker listesine bakma, holding yapılarını biliyorsun.

ZAYIF ETKİ (ATLA):
- Üniversite yönetmelikleri
- Mahkeme kararları (ticari davalar hariç)
- Belediye ilanları
- Döviz kuru tabloları
- Genel bütçe teknik değişiklikleri
- Kişisel atamalar (şube müdürü vs.)
- Spor federasyonu kararları
- Askeri atamalar
- İl özel idare kararları

FORMAT: JSON dizisi döndür. Eğer kuvvetli etki yoksa boş dizi [] döndür.
[
  {
    "title": "Karar başlığı (kısa, 80 karakter max)",
    "summary": "2-3 cümle açıklama. Ne kararı, kimi etkiler, nasıl etkiler. Holding/grup bağlantısını açıkla.",
    "impact": "Borsa etkisi açıklaması (1 cümle)",
    "tickers": ["FZLGY", "KCHOL"],
    "sentiment": "pozitif" veya "negatif" veya "nötr",
    "source_url": "Kararın PDF/HTM URL'si"
  }
]

ÖNEMLİ KURALLAR:
- SADECE gerçekten kuvvetli etkisi olan kararları dahil et
- Ticker eşleştirmesinde verilen BIST ticker listesini + holding bilgini kullan
- Holding/iştirak bağlantısını MUTLAKA summary'de açıkla (örn: "Fuzul Holding'in iştiraki FZLGY etkilenebilir")
- Eğer bir şirketin ticker'ını bulamıyorsan yine de dahil et (tickers boş bırak)
- Sektörel etkide birden fazla ticker olabilir
- ASLA karar uydurmayın — sadece verilen içerikten çıkar
- JSON dışında hiçbir şey yazma"""

    user_message = f"""Bugünkü Resmi Gazete içeriği:

{gazette_text}

--- TÜM BIST TICKER EŞLEŞMELERİ ---
{ticker_text}

Yukarıdaki kararları analiz et. Borsa etkisi olan kararları JSON formatında döndür."""

    # AI call — Abacus → Claude → Gemini fallback
    result_json = None

    try:
        from app.config import get_settings
        settings = get_settings()

        # 1. Abacus AI
        abacus_key = settings.ABACUS_API_KEY
        if abacus_key:
            try:
                resp = httpx.post(
                    "https://api.abacus.ai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {abacus_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "claude-3-5-sonnet-20241022",
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_message},
                        ],
                        "temperature": 0.2,
                        "max_tokens": 2000,
                    },
                    timeout=60,
                )
                if resp.status_code == 200:
                    content = resp.json()["choices"][0]["message"]["content"]
                    result_json = _extract_json_from_response(content)
                    logger.info("Resmi Gazete AI: Abacus basarili")
            except Exception as e:
                logger.warning("Resmi Gazete AI Abacus hatasi: %s", e)

        # 2. Claude fallback
        if result_json is None:
            anthropic_key = getattr(settings, "ANTHROPIC_API_KEY", None)
            if anthropic_key:
                try:
                    resp = httpx.post(
                        "https://api.anthropic.com/v1/messages",
                        headers={
                            "x-api-key": anthropic_key,
                            "anthropic-version": "2023-06-01",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": "claude-sonnet-4-20250514",
                            "max_tokens": 2000,
                            "temperature": 0.2,
                            "system": system_prompt,
                            "messages": [{"role": "user", "content": user_message}],
                        },
                        timeout=60,
                    )
                    if resp.status_code == 200:
                        content = resp.json()["content"][0]["text"]
                        result_json = _extract_json_from_response(content)
                        logger.info("Resmi Gazete AI: Claude basarili")
                except Exception as e:
                    logger.warning("Resmi Gazete AI Claude hatasi: %s", e)

        # 3. Gemini fallback
        if result_json is None:
            gemini_key = settings.GEMINI_API_KEY if settings.GEMINI_API_KEY else None
            if gemini_key:
                try:
                    resp = httpx.post(
                        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={gemini_key}",
                        headers={"Content-Type": "application/json"},
                        json={
                            "contents": [{"parts": [{"text": f"{system_prompt}\n\n{user_message}"}]}],
                            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2000},
                        },
                        timeout=60,
                    )
                    if resp.status_code == 200:
                        content = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
                        result_json = _extract_json_from_response(content)
                        logger.info("Resmi Gazete AI: Gemini basarili")
                except Exception as e:
                    logger.warning("Resmi Gazete AI Gemini hatasi: %s", e)

    except Exception as e:
        logger.error("Resmi Gazete AI genel hata: %s", e)

    if not result_json or not isinstance(result_json, list):
        return None

    # Boş dizi = etki yok
    if len(result_json) == 0:
        return None

    return result_json


def _extract_json_from_response(text: str) -> list | None:
    """AI cevabından JSON dizisini çıkar."""
    text = text.strip()

    # ```json ... ``` bloğu var mı?
    match = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass

    # Direkt JSON dizi
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    return None


async def _save_state(date_str: str):
    """Son islenmis tarihi DB'ye kaydet."""
    from app.database import async_session
    from app.models.scraper_state import ScraperState
    from sqlalchemy import select

    async with async_session() as session:
        result = await session.execute(
            select(ScraperState).where(ScraperState.key == SCRAPER_STATE_KEY)
        )
        state = result.scalar_one_or_none()
        if state:
            state.value = date_str
            state.updated_at = datetime.utcnow()
        else:
            session.add(ScraperState(key=SCRAPER_STATE_KEY, value=date_str))
        await session.commit()


async def _get_tweeted_urls(date_str: str) -> set[str]:
    """Bugün zaten tweetlenmiş URL'leri döndür."""
    from app.database import async_session
    from app.models.scraper_state import ScraperState
    from sqlalchemy import select

    key = f"rg_tweeted_{date_str}"
    async with async_session() as session:
        result = await session.execute(
            select(ScraperState).where(ScraperState.key == key)
        )
        state = result.scalar_one_or_none()
        if state and state.value:
            try:
                return set(json.loads(state.value))
            except json.JSONDecodeError:
                pass
    return set()


async def _mark_as_tweeted(date_str: str, url: str):
    """URL'yi tweetlenmiş olarak kaydet."""
    from app.database import async_session
    from app.models.scraper_state import ScraperState
    from sqlalchemy import select

    key = f"rg_tweeted_{date_str}"
    async with async_session() as session:
        result = await session.execute(
            select(ScraperState).where(ScraperState.key == key)
        )
        state = result.scalar_one_or_none()
        urls = set()
        if state and state.value:
            try:
                urls = set(json.loads(state.value))
            except json.JSONDecodeError:
                pass

        urls.add(url)

        if state:
            state.value = json.dumps(list(urls))
            state.updated_at = datetime.utcnow()
        else:
            session.add(ScraperState(key=key, value=json.dumps(list(urls))))
        await session.commit()
