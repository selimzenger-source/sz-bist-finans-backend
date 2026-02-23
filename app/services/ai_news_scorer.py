"""Abacus AI (RouteLLM) — KAP Haber Puanlama & Yorum Servisi V2.

Akis:
1. Telegram'dan hisse kodu (ticker) gelir
2. KAP'tan son bildirimleri cek → ticker ile esleseni bul
3. Eslesen KAP bildiriminin tam metnini al
4. Abacus AI (gpt-4o) ile 1-10 puan + 2 cumle Turkce ozet uret
5. Sonuc: {"score": int, "summary": str, "kap_url": str|None}

KAP Eslestirme Stratejisi:
- Oncelik 1: KAP API → bugunun bildirimlerinden ticker ile filtrele
- Oncelik 2: KAP detay sayfasi HTML scrape
- Fallback: Telegram ham metni ile AI analiz (KAP bulunamazsa)

Hata Toleransi:
- KAP erisimi basarisiz → Telegram metniyle devam
- AI basarisiz → score=None, summary=None don
- Hicbir hata akisi durdurmaz
"""

import json
import logging
from datetime import date

import httpx

logger = logging.getLogger(__name__)

# Abacus AI RouteLLM endpoint (OpenAI uyumlu)
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"

# AI model — gpt-4o guclu analiz icin (gpt-4o-mini yetersizdi)
_AI_MODEL = "gpt-4o"

# Timeouts
_KAP_TIMEOUT = 12  # KAP icin
_AI_TIMEOUT = 20   # AI icin (gpt-4o daha yavas)

# KAP base
KAP_BASE = "https://www.kap.org.tr"
KAP_API_BASE = f"{KAP_BASE}/tr/api"

KAP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "tr-TR,tr;q=0.9",
    "Referer": f"{KAP_BASE}/tr/bildirim-sorgu",
    "X-Requested-With": "XMLHttpRequest",
}


def _get_api_key() -> str | None:
    """Config'den Abacus API key'i al."""
    try:
        from app.config import get_settings
        key = get_settings().ABACUS_API_KEY
        return key if key else None
    except Exception:
        return None


# -------------------------------------------------------
# ADIM 1: KAP'tan Ticker ile Bildirim Bul
# -------------------------------------------------------

async def find_kap_disclosure_by_ticker(ticker: str) -> dict | None:
    """KAP'tan ticker'a gore son bildirimi bul.

    Strateji:
    1. KAP API (memberDisclosureQuery) → bugunun tum bildirimlerini cek,
       ticker ile filtrele
    2. Eslesen bildirimin detay sayfasini cek (full_text)
    3. KAP URL'yi olustur

    Args:
        ticker: Hisse kodu (orn: "ENDAE", "BIMAS")

    Returns:
        {
            "full_text": str,        # KAP bildirim tam metni
            "kap_url": str,          # KAP bildirim linki
            "kap_id": str,           # Gercek KAP bildirim ID
            "subject": str,          # Bildirim basligi
        }
        Bulunamazsa None doner.
    """
    if not ticker:
        return None

    try:
        async with httpx.AsyncClient(timeout=_KAP_TIMEOUT, headers=KAP_HEADERS, follow_redirects=True) as client:
            # --- KAP API: Bugunun tum bildirimlerini cek ---
            matched_disclosure = await _search_kap_api(client, ticker)

            if not matched_disclosure:
                logger.info("KAP API'de %s icin bildirim bulunamadi", ticker)
                return None

            kap_id = matched_disclosure.get("kap_id")
            if not kap_id:
                return None

            # --- KAP detay sayfasindan tam metni cek ---
            full_text = await _fetch_kap_detail_text(client, kap_id)
            kap_url = f"{KAP_BASE}/tr/Bildirim/{kap_id}"

            if full_text:
                logger.info(
                    "KAP eslestirme basarili: %s → KAP#%s (%d karakter)",
                    ticker, kap_id, len(full_text),
                )
                return {
                    "full_text": full_text,
                    "kap_url": kap_url,
                    "kap_id": kap_id,
                    "subject": matched_disclosure.get("subject", ""),
                }
            else:
                # Detay alinamazsa en azindan URL ve subject dondur
                logger.warning("KAP detay alinamadi (%s), sadece URL donuluyor", kap_id)
                return {
                    "full_text": matched_disclosure.get("subject", ""),
                    "kap_url": kap_url,
                    "kap_id": kap_id,
                    "subject": matched_disclosure.get("subject", ""),
                }

    except Exception as e:
        logger.warning("KAP ticker arama hatasi (%s): %s", ticker, e)
        return None


async def _search_kap_api(client: httpx.AsyncClient, ticker: str) -> dict | None:
    """KAP API ile bugunun bildirimlerinden ticker eslestir.

    KAP memberDisclosureQuery POST endpoint'i ile bugunun
    tum bildirimlerini cekip ticker ile filtreliyoruz.
    """
    try:
        today_str = date.today().strftime("%Y-%m-%d")
        resp = await client.post(
            f"{KAP_API_BASE}/memberDisclosureQuery",
            json={
                "fromDate": today_str,
                "toDate": today_str,
            },
        )

        if resp.status_code != 200:
            logger.warning("KAP API status: %s", resp.status_code)
            return None

        data = resp.json()
        disclosures = data if isinstance(data, list) else data.get("data", [])

        # Ticker ile eslestir — en son (en guncel) olanı al
        ticker_upper = ticker.upper()
        for d in disclosures:
            stock_code = (d.get("stockCode", "") or d.get("memberCode", "") or "").upper()
            if stock_code == ticker_upper:
                kap_id = str(d.get("disclosureIndex", d.get("id", "")))
                subject = d.get("disclosureTitle", d.get("subject", ""))
                return {
                    "kap_id": kap_id,
                    "subject": subject,
                    "company_name": d.get("companyName", d.get("memberName", "")),
                }

        return None

    except Exception as e:
        logger.warning("KAP API arama hatasi: %s", e)
        return None


async def _fetch_kap_detail_text(client: httpx.AsyncClient, kap_id: str) -> str | None:
    """KAP bildirim detay sayfasindan tam metni cek (HTML scrape)."""
    try:
        url = f"{KAP_BASE}/tr/Bildirim/{kap_id}"
        resp = await client.get(url)
        if resp.status_code != 200:
            return None

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "lxml")

        # Bildirim metin icerigini al
        content_div = soup.select_one(".disclosure-content, .sub-content, #divContent, [class*='disclosure']")
        if content_div:
            return content_div.get_text(separator="\n", strip=True)

        # Fallback: body icindeki ana metni al
        main = soup.select_one("main, article, .content")
        if main:
            return main.get_text(separator="\n", strip=True)[:5000]

        return None

    except Exception as e:
        logger.warning("KAP detay cekme hatasi (%s): %s", kap_id, e)
        return None


# -------------------------------------------------------
# ADIM 2: AI Puanlama (Abacus RouteLLM — gpt-4o)
# -------------------------------------------------------

async def score_news(
    ticker: str,
    raw_text: str,
    kap_content: str | None = None,
    kap_url: str | None = None,
) -> dict:
    """Haberi AI ile puanla ve yorumla.

    Args:
        ticker: Hisse kodu (orn: "ENDAE")
        raw_text: Telegram mesajinin ham metni
        kap_content: KAP bildirim tam metni (varsa)
        kap_url: KAP bildirim linki (varsa)

    Returns:
        {"score": int|None, "summary": str|None, "kap_url": str|None}
        Hata durumunda score+summary None olur — akis kirilmaz.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.debug("AI News Scorer: ABACUS_API_KEY bos, devre disi")
        return {"score": None, "summary": None, "kap_url": kap_url}

    # KAP icerigi varsa birincil kaynak, yoksa Telegram metni
    has_kap = bool(kap_content and len(kap_content.strip()) > 50)
    content = kap_content if has_kap else raw_text
    content = content[:4000] if content else ""  # gpt-4o daha uzun metin isleyebilir

    if not content.strip():
        return {"score": None, "summary": None, "kap_url": kap_url}

    # Kaynak bilgisini prompt'a ekle
    source_info = "KAP Bildirim Tam Metni" if has_kap else "Telegram Kanal Ozeti (KAP erisilemedi)"

    prompt = f"""Borsa Istanbul (BIST) KAP bildirimi analizi.

Hisse: {ticker}
Kaynak: {source_info}

--- ICERIK ---
{content}
--- ICERIK SONU ---

ANALIZ KURALLARI:
1. Bu haberin HISSE FIYATINA muhtemel etkisini 1-10 arasi puanla:
   1-2: Ciddi olumsuz (zarar, ceza, iflas, sermaye erimesi)
   3-4: Olumsuz veya belirsiz (borcluluk artisi, dava, risk)
   5: Notr — rutin bildirim, etki belirsiz
   6-7: Hafif olumlu (yeni sozlesme, is birligi, buyume sinyali)
   8-9: Guclu olumlu (rekor kar, bedelsiz, temettuu, buyuk ihale)
   10: Cok guclu olumlu (sektoru degistirecek haber)

2. DIKKAT: Sadece "pozitif haber" etiketi tasimasi YETMEZ.
   - Rutin bildirimler (SPK onay, genel kurul) → 5
   - Personel alimi gibi standart isler → 5-6
   - Gercek impact analizi yap: haberin sirket degerine etkisini dusun

3. Haberi yatirimci bakis acisiyla Turkce 2 cumle ile ozetle.

SADECE asagidaki JSON formatinda yanit ver:
{{"score": 7, "summary": "Iki cumlelik Turkce ozet."}}"""

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _ABACUS_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _AI_MODEL,
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "Sen deneyimli bir Borsa Istanbul yatirimci analistisin. "
                                "KAP bildirimlerini objektif analiz edersin. "
                                "Her habere yuksek puan vermezsin — gercek etkiyi degerlendirirsin. "
                                "Sadece JSON formatinda yanit ver."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0,
                    "max_tokens": 400,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        # OpenAI format: choices[0].message.content
        text = data["choices"][0]["message"]["content"].strip()

        # JSON blogu temizle
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        result = json.loads(text)
        score = result.get("score")
        summary = result.get("summary")

        # Score validation: 1-10 arasinda olmali
        if isinstance(score, (int, float)) and 1 <= score <= 10:
            score = int(score)
        else:
            logger.warning("AI News Scorer: Gecersiz skor=%s (%s)", score, ticker)
            score = None

        # Summary validation
        if not isinstance(summary, str) or not summary.strip():
            summary = None

        logger.info(
            "AI News Scorer: %s — skor=%s, kaynak=%s, ozet=%s",
            ticker, score,
            "KAP" if has_kap else "Telegram",
            (summary[:60] + "...") if summary and len(summary) > 60 else summary,
        )

        return {"score": score, "summary": summary, "kap_url": kap_url}

    except Exception as e:
        logger.warning("AI News Scorer: Abacus hatasi (%s) — %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url}


# -------------------------------------------------------
# MASTER FONKSIYON: KAP Bul + AI Puanla (Tek Cagri)
# -------------------------------------------------------

async def analyze_news(ticker: str, raw_text: str, max_retries: int = 3) -> dict:
    """Tam AI analiz pipeline'i: KAP ara → icerik cek → AI puanla.

    KAP'a ilk seferde ulasilamazsa 5 saniye aralikla 2 kez daha dener.
    telegram_poller.py bu fonksiyonu cagirir.

    Args:
        ticker: Hisse kodu
        raw_text: Telegram ham mesaj metni
        max_retries: KAP arama deneme sayisi (varsayilan: 3)

    Returns:
        {
            "score": int | None,
            "summary": str | None,
            "kap_url": str | None,
        }
    """
    import asyncio

    kap_content = None
    kap_url = None

    # Adim 1: KAP'tan bildirimi bul (retry ile — 5 sn aralikla)
    for attempt in range(1, max_retries + 1):
        try:
            kap_result = await find_kap_disclosure_by_ticker(ticker)
            if kap_result and kap_result.get("full_text"):
                kap_content = kap_result.get("full_text")
                kap_url = kap_result.get("kap_url")
                logger.info(
                    "KAP eslestirme basarili (deneme %d/%d): %s → %s",
                    attempt, max_retries, ticker, kap_url,
                )
                break  # Basarili — donguden cik
            else:
                if attempt < max_retries:
                    logger.info(
                        "KAP bulunamadi (%s), %d sn sonra tekrar deneniyor (%d/%d)...",
                        ticker, 5, attempt, max_retries,
                    )
                    await asyncio.sleep(5)  # 5 saniye bekle, tekrar dene
                else:
                    logger.info(
                        "KAP %d denemede bulunamadi (%s), Telegram metniyle devam",
                        max_retries, ticker,
                    )
        except Exception as e:
            logger.warning("KAP arama hatasi (deneme %d): %s — %s", attempt, ticker, e)
            if attempt < max_retries:
                await asyncio.sleep(5)

    # Adim 2: AI puanlama (KAP icerigi veya Telegram metni ile)
    try:
        result = await score_news(ticker, raw_text, kap_content, kap_url)
        return result
    except Exception as e:
        logger.warning("AI puanlama hatasi (%s): %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url}
