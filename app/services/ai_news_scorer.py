"""Abacus AI (RouteLLM) — KAP Haber Puanlama & Yorum Servisi V4.

Akis:
1. Telegram'dan Matriks HaberId (kap_notification_id) gelir
2. TradingView'dan haber icerigini cek (matriks:{id}:0/ URL)
3. Abacus AI (gpt-4o) ile 1.0-10.0 ondalik puan + 3 cumle Turkce ozet uret
4. Sonuc: {"score": float, "summary": str, "kap_url": str|None}

V4 Degisiklikler:
- Ondalik skor (8.7, 6.3 gibi) — daha hassas ayirim
- Gelismis prompt — detayli analiz kurallari
- %100 artis vs %32 artis farkli puan alir

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
_SCORER_VERSION = "v4-decimal"

# AI model — gpt-4o guclu analiz icin
_AI_MODEL = "gpt-4o"

# Timeouts
_TV_TIMEOUT = 15   # TradingView icin
_AI_TIMEOUT = 25   # AI icin (detayli analiz daha uzun surebilir)

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

            # --- Gercek KAP bildirim linkini cikart (cok katmanli arama) ---
            import re as _re
            real_kap_url = None

            # Katman 1: <a> tag'lerinde kap.org.tr linki ara
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "kap.org.tr" in href and "/Bildirim/" in href:
                    real_kap_url = href
                    break

            # Katman 2: Icerik metninden regex ile kap linkini bul
            # /tr/ ve /en/ opsiyonel — bazen kap.org.tr/Bildirim/123 formati olabilir
            _KAP_REGEX = r'https?://(?:www\.)?kap\.org\.tr/(?:(?:tr|en)/)?Bildirim/(\d+)'
            if not real_kap_url:
                kap_match = _re.search(_KAP_REGEX, resp.text)
                if kap_match:
                    # Normalize: her zaman /tr/ ile dondur
                    real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_match.group(1)}"

            # Katman 3: JSON-LD / <script> tag'lerinde kap.org.tr linkini ara
            if not real_kap_url:
                for script_tag in soup.find_all("script"):
                    script_text = script_tag.string or ""
                    if "kap.org.tr" in script_text:
                        kap_match = _re.search(_KAP_REGEX, script_text)
                        if kap_match:
                            real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_match.group(1)}"
                            break

            # Katman 4: Meta tag'lerden KAP linki ara (og:url, canonical, og:see_also)
            if not real_kap_url:
                for meta_tag in soup.find_all("meta"):
                    content = meta_tag.get("content", "")
                    if "kap.org.tr" in content and "Bildirim" in content:
                        kap_match = _re.search(_KAP_REGEX, content)
                        if kap_match:
                            real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_match.group(1)}"
                            break

            # Katman 5: Tam HTML'de genis regex (encoded URL'ler, parcali URL'ler dahil)
            if not real_kap_url:
                kap_match = _re.search(
                    r'kap\.org\.tr[^"\'<>\s]*?Bildirim/(\d+)',
                    resp.text,
                )
                if kap_match:
                    real_kap_url = f"https://www.kap.org.tr/tr/Bildirim/{kap_match.group(1)}"

            if real_kap_url:
                # /en/ → /tr/ normalize (TradingView bazen Ingilizce KAP linki veriyor)
                real_kap_url = real_kap_url.replace("/en/Bildirim/", "/tr/Bildirim/")
                real_kap_url = real_kap_url.replace("/en/bildirim/", "/tr/Bildirim/")
                logger.info(
                    "KAP bildirim linki bulundu: matriks:%s → %s",
                    matriks_id, real_kap_url,
                )
            else:
                logger.warning(
                    "KAP bildirim linki bulunamadi: matriks:%s — TradingView fallback kullanilacak",
                    matriks_id,
                )

            logger.info(
                "TradingView icerik basarili: matriks:%s (%d karakter)",
                matriks_id, len(full_text),
            )

            return {
                "full_text": full_text,
                "tv_url": tv_url,
                "title": title,
                "real_kap_url": real_kap_url,
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
        {"score": float|None, "summary": str|None, "kap_url": str|None}
        Hata durumunda score+summary None olur — akis kirilmaz.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.error("AI News Scorer: ABACUS_API_KEY bos — AI puanlama devre disi! (%s)", ticker)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}

    # TradingView icerigi varsa birincil kaynak, yoksa Telegram metni
    has_tv = bool(tv_content and len(tv_content.strip()) > 50)
    content = tv_content if has_tv else raw_text
    content = content[:4000] if content else ""  # gpt-4o uzun metin isleyebilir

    if not content.strip():
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}

    # Kaynak bilgisini prompt'a ekle
    source_info = "KAP Bildirim Tam Metni (TradingView)" if has_tv else "Telegram Kanal Ozeti (detay erisilemedi)"

    prompt = f"""Borsa Istanbul (BIST) KAP bildirimi analizi.

Hisse: {ticker}
Kaynak: {source_info}

--- ICERIK BASLANGIC ---
{content}
--- ICERIK BITIS ---

PUANLAMA SISTEMI (1.0 — 10.0 arasi, 0.1 hassasiyetle):

OLUMSUZ BOLGESI (1.0 — 4.9):
  1.0-2.0: Ciddi olumsuz — iflas, agir ceza, sermaye erimesi, buyuk zarar
  2.1-3.5: Olumsuz — net donem zarari, yuksek borcluluk, dava/ceza riski
  3.6-4.9: Hafif olumsuz veya belirsiz — kredi kullanimi, kucuk zarar, belirsiz sonuc

NOTR BOLGESI (5.0 — 5.9):
  5.0-5.4: Tam notr — rutin bildirim, SPK onay, genel kurul, yonetim kadrosu degisikligi
  5.5-5.9: Notr+ — detayi belli olmayan bildirim, personel alimi, kurumsal uyum raporu

OLUMLU BOLGESI (6.0 — 10.0):
  6.0-6.4: Hafif olumlu — kucuk ihale kazanimi (<50M TL), kucuk sozlesme, standart dis ticaret
  6.5-6.9: Olumlu — orta olcekli sozlesme (50-200M TL), yeni is birligi, ihracat anlasmasi
  7.0-7.4: Iyi — buyuk sozlesme (200M-1 milyar TL), guclu ihracat artisi, onemli ihale
  7.5-7.9: Cok iyi — %20-40 arasi kar artisi, buyuk ihale (>1 milyar TL), onemli ortaklik
  8.0-8.4: Guclu olumlu — %40-70 kar artisi, buyuk bedelsiz (<=%50), yuksek temettu
  8.5-8.9: Cok guclu — %70-100 kar artisi, buyuk bedelsiz (%50-100), sektor liderligi
  9.0-9.4: Olaganustu — %100+ kar artisi, %100+ bedelsiz sermaye artirimi, mega ihale
  9.5-10.0: Tarihsel — sektoru degistirecek haber, devasa birlesme, rekor kar + bedelsiz birlikte

KRITIK KURALLAR:
- Haberin GERCEK BUYUKLUGUNU deger — rakamsal verileri kullan
- %100 bedelsiz ≠ %30 kar artisi → FARKLI puanlar verilmeli
- Ihale tutari onemli: 10M TL = 6.2, 200M TL = 7.0, 1 milyar TL = 7.8
- Bir sirketin buyuklugune gore degerlendir (BIMAS icin 600M TL sermaye artirimi ≠ kucuk sirket icin)
- Sadece "pozitif" etiketi tasimasi YETMEZ — gercek etkiyi ol
- Kaynak "Telegram Ozeti" ise ve detay yoksa: 5.0-5.5 arasi ver (belirsiz)

Haberi yatirimci bakis acisiyla Turkce en fazla 3 cumle ile ozetle.
Onemli rakamlari ozete dahil et (tutar, oran, yuzde).

HASHTAG KURALLARI:
- Haberin konusuyla ilgili 2-3 adet Twitter hashtag uret (# isareti OLMADAN)
- Sirket ticker'i ({ticker}) zaten ekleniyor, onu TEKRAR verme
- Sektor, konu ve iliskili kavramlardan sec: gayrimenkul, enerji, teknoloji, insaat, gida, saglik, otomotiv, ihracat, ithalat, temettü, bedelsiz, sermayeartirimi, karaciklama, ihale, sozlesme, ortaklik, satis, alim, uretim, yatirim vb.
- Turkce ve kucuk harf yaz (ornek: gayrimenkul, temettü, ihale)
- Sadece haberin icerigiyle GERCEKTEN ilgili hashtagler sec, genel/alakasiz olmasin

SADECE asagidaki JSON formatinda yanit ver (score ONDALIKLI olmali):
{{"score": 7.3, "summary": "Uc cumleye kadar Turkce ozet.", "hashtags": ["sektor", "konu"]}}"""

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
                                "Sen 15+ yillik deneyime sahip bir Borsa Istanbul kurumsal yatirimci analistisin. "
                                "KAP bildirimlerini son derece detayli ve objektif analiz edersin. "
                                "Haberdeki rakamlari, oranlari ve buyuklukleri dikkatlice degerlendirirsin. "
                                "Puanlaman cok hassas olmali — 0.1 hassasiyetle score ver. "
                                "Benzer gorunen ama farkli buyuklukte haberler FARKLI puan almali. "
                                "Sadece JSON formatinda yanit ver."
                            ),
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,  # Biraz yaraticilik — daha dogal puanlama
                    "max_tokens": 500,
                },
            )
            if resp.status_code != 200:
                logger.error(
                    "AI News Scorer: Abacus API HTTP %s (%s) — %s",
                    resp.status_code, ticker, resp.text[:200],
                )
                return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}
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
        hashtags = result.get("hashtags", [])

        # Score validation: 1.0-10.0 arasinda olmali (ondalik)
        if isinstance(score, (int, float)) and 1.0 <= score <= 10.0:
            score = round(float(score), 1)  # 1 ondalik basamak
        else:
            logger.warning("AI News Scorer: Gecersiz skor=%s (%s)", score, ticker)
            score = None

        # Summary validation
        if not isinstance(summary, str) or not summary.strip():
            summary = None

        # Hashtags validation — max 3, her biri string, # isareti temizle
        if isinstance(hashtags, list):
            clean_tags = []
            for tag in hashtags[:3]:
                if isinstance(tag, str) and tag.strip():
                    clean = tag.strip().lstrip("#").replace(" ", "")
                    if clean and clean.upper() != ticker.upper():
                        clean_tags.append(clean)
            hashtags = clean_tags
        else:
            hashtags = []

        logger.info(
            "AI News Scorer: %s — skor=%s, kaynak=%s, hashtags=%s, ozet=%s",
            ticker, score,
            "TradingView" if has_tv else "Telegram",
            hashtags,
            (summary[:60] + "...") if summary and len(summary) > 60 else summary,
        )

        return {"score": score, "summary": summary, "kap_url": kap_url, "hashtags": hashtags}

    except httpx.TimeoutException:
        logger.error("AI News Scorer: Abacus API zaman asimi (%s) — %s sn", ticker, _AI_TIMEOUT)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}
    except json.JSONDecodeError as e:
        logger.error("AI News Scorer: JSON parse hatasi (%s) — %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}
    except Exception as e:
        logger.error("AI News Scorer: Beklenmeyen hata (%s) — %s", ticker, e)
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}


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
            "score": float | None,
            "summary": str | None,
            "kap_url": str | None,
            "hashtags": list[str],
        }
    """
    tv_content = None
    kap_url = None

    # Adim 1: TradingView'dan icerik cek (Matriks ID varsa)
    if matriks_id:
        # Fallback olarak TradingView linki (gercek KAP linki bulunursa degisir)
        kap_url = f"https://tr.tradingview.com/news/matriks:{matriks_id}:0/"

        try:
            tv_result = await fetch_tradingview_content(matriks_id)
            if tv_result and tv_result.get("full_text"):
                tv_content = tv_result["full_text"]
                # Gercek KAP bildirim linkini kullan (TradingView'dan cikarildi)
                if tv_result.get("real_kap_url"):
                    kap_url = tv_result["real_kap_url"]
                    logger.info(
                        "Gercek KAP linki kullaniliyor: %s → %s",
                        ticker, kap_url,
                    )
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
        return {"score": None, "summary": None, "kap_url": kap_url, "hashtags": []}
