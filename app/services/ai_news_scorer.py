"""Abacus AI (RouteLLM) — KAP Haber Puanlama & Yorum Servisi V3.

Akis:
1. Telegram'dan Matriks HaberId (kap_notification_id) gelir
2. TradingView'dan haber icerigini cek (matriks:{id}:0/ URL)
3. Abacus AI (gpt-4o) ile 1-10 puan + 2 cumle Turkce ozet uret
4. Sonuc: {"score": int, "summary": str, "kap_url": str|None}

Icerik Kaynagi (Oncelik sirasi):
- Oncelik 1: TradingView haber sayfasi (matriks ID ile)
- Fallback: Telegram ham metni (TradingView basarisizsa)

Hata Toleransi:
- TradingView erisimi basarisiz → Telegram metniyle devam
- AI basarisiz → score=None, summary=None don
- Hicbir hata akisi durdurmaz
"""

import json
import logging

import httpx

logger = logging.getLogger(__name__)

# Abacus AI RouteLLM endpoint (OpenAI uyumlu)
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"

# Versiyon — deploy dogrulama icin
_SCORER_VERSION = "v3-tradingview"

# AI model — gpt-4o guclu analiz icin
_AI_MODEL = "gpt-4o"

# Timeouts
_TV_TIMEOUT = 15   # TradingView icin
_AI_TIMEOUT = 20   # AI icin (gpt-4o daha yavas)

# TradingView base URL
TV_NEWS_BASE = "https://tr.tradingview.com/news"

# Browser benzeri headers (TradingView icin)
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
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
# ADIM 1: TradingView'dan Icerik Cek (Matriks ID ile)
# -------------------------------------------------------

async def fetch_tradingview_content(matriks_id: str) -> dict | None:
    """TradingView haber sayfasindan icerik cek.

    URL format: https://tr.tradingview.com/news/matriks:{id}:0/

    Args:
        matriks_id: Matriks Haber ID'si (orn: "6225961")

    Returns:
        {
            "full_text": str,   # Haber tam metni
            "tv_url": str,      # TradingView linki
            "title": str,       # Haber basligi
        }
        Basarisizsa None doner.
    """
    if not matriks_id:
        return None

    tv_url = f"{TV_NEWS_BASE}/matriks:{matriks_id}:0/"

    try:
        async with httpx.AsyncClient(
            timeout=_TV_TIMEOUT,
            headers=_HEADERS,
            follow_redirects=True,
        ) as client:
            resp = await client.get(tv_url)

            if resp.status_code != 200:
                logger.warning(
                    "TradingView %s status: %s",
                    matriks_id, resp.status_code,
                )
                return None

            # HTML'den metin cikart
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")

            # Baslik
            title = ""
            title_el = soup.select_one("h1, .title, [class*='title']")
            if title_el:
                title = title_el.get_text(strip=True)

            # Icerik — TradingView haber sayfasi yapisi
            full_text = ""

            # Ana icerik bolumu
            content_el = (
                soup.select_one("article")
                or soup.select_one("[class*='body']")
                or soup.select_one("[class*='content']")
                or soup.select_one("main")
            )

            if content_el:
                # Script ve style etiketlerini kaldir
                for tag in content_el.find_all(["script", "style", "nav", "footer"]):
                    tag.decompose()
                full_text = content_el.get_text(separator="\n", strip=True)

            # Fallback: tum body'den cek
            if not full_text or len(full_text) < 30:
                body = soup.find("body")
                if body:
                    for tag in body.find_all(["script", "style", "nav", "footer", "header"]):
                        tag.decompose()
                    full_text = body.get_text(separator="\n", strip=True)

            # Cok kisa icerik = basarisiz
            if not full_text or len(full_text) < 30:
                logger.warning("TradingView icerik cok kisa (%s): %d karakter", matriks_id, len(full_text or ""))
                return None

            # 5000 karakterle sinirla
            full_text = full_text[:5000]

            logger.info(
                "TradingView icerik basarili: matriks:%s (%d karakter)",
                matriks_id, len(full_text),
            )

            return {
                "full_text": full_text,
                "tv_url": tv_url,
                "title": title,
            }

    except Exception as e:
        logger.warning("TradingView icerik hatasi (matriks:%s): %s", matriks_id, e)
        return None


# -------------------------------------------------------
# ADIM 2: AI Puanlama (Abacus RouteLLM — gpt-4o)
# -------------------------------------------------------

async def score_news(
    ticker: str,
    raw_text: str,
    tv_content: str | None = None,
    kap_url: str | None = None,
) -> dict:
    """Haberi AI ile puanla ve yorumla.

    Args:
        ticker: Hisse kodu (orn: "ENDAE")
        raw_text: Telegram mesajinin ham metni
        tv_content: TradingView'dan cekilmis bildirim tam metni (varsa)
        kap_url: TradingView/KAP linki (varsa)

    Returns:
        {"score": int|None, "summary": str|None, "kap_url": str|None}
        Hata durumunda score+summary None olur — akis kirilmaz.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.debug("AI News Scorer: ABACUS_API_KEY bos, devre disi")
        return {"score": None, "summary": None, "kap_url": kap_url}

    # TradingView icerigi varsa birincil kaynak, yoksa Telegram metni
    has_tv = bool(tv_content and len(tv_content.strip()) > 50)
    content = tv_content if has_tv else raw_text
    content = content[:4000] if content else ""  # gpt-4o uzun metin isleyebilir

    if not content.strip():
        return {"score": None, "summary": None, "kap_url": kap_url}

    # Kaynak bilgisini prompt'a ekle
    source_info = "KAP Bildirim Tam Metni (TradingView)" if has_tv else "Telegram Kanal Ozeti (detay erisilemedi)"

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
            "TradingView" if has_tv else "Telegram",
            (summary[:60] + "...") if summary and len(summary) > 60 else summary,
        )

        return {"score": score, "summary": summary, "kap_url": kap_url}

    except Exception as e:
        logger.warning("AI News Scorer: Abacus hatasi (%s) — %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url}


# -------------------------------------------------------
# MASTER FONKSIYON: TradingView Icerik + AI Puanla
# -------------------------------------------------------

async def analyze_news(
    ticker: str,
    raw_text: str,
    matriks_id: str | None = None,
) -> dict:
    """Tam AI analiz pipeline'i: TradingView icerik cek → AI puanla.

    Matriks ID varsa TradingView'dan tam haber metni cekilir.
    Yoksa veya basarisizsa Telegram ham metniyle AI puanlama yapilir.

    Args:
        ticker: Hisse kodu
        raw_text: Telegram ham mesaj metni
        matriks_id: Telegram mesajindaki kap_notification_id (Matriks HaberId)

    Returns:
        {
            "score": int | None,
            "summary": str | None,
            "kap_url": str | None,
        }
    """
    tv_content = None
    kap_url = None

    # Adim 1: TradingView'dan icerik cek (Matriks ID varsa)
    if matriks_id:
        kap_url = f"{TV_NEWS_BASE}/matriks:{matriks_id}:0/"

        try:
            tv_result = await fetch_tradingview_content(matriks_id)
            if tv_result and tv_result.get("full_text"):
                tv_content = tv_result["full_text"]
                logger.info(
                    "TradingView eslestirme basarili: %s → matriks:%s (%d karakter)",
                    ticker, matriks_id, len(tv_content),
                )
            else:
                logger.info(
                    "TradingView icerik alinamadi (%s), Telegram metniyle devam",
                    ticker,
                )
        except Exception as e:
            logger.warning("TradingView hatasi (%s): %s", ticker, e)

    # Adim 2: AI puanlama (TradingView icerigi veya Telegram metni ile)
    try:
        result = await score_news(ticker, raw_text, tv_content, kap_url)
        return result
    except Exception as e:
        logger.warning("AI puanlama hatasi (%s): %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url}
