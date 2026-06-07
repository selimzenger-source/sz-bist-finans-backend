"""Blog → X (Twitter) paylaşım servisi.

Admin panelden bir blog YAYINLANDIĞINDA çağrılır:
  1. Blog içeriğini Gemini ile 4-5 tweet'lik Türkçe THREAD'e çevirir
     (emoji + boş satırlar + ilgili hisse/genel hashtag'ler).
  2. Konuya uygun marka kartı (PNG) üretir → 1. tweete eklenir.
  3. Thread'i 40 saniye aralıklarla zincirleme atar (premium hesap).

Engelleme yapmaz: tweet kill switch'e saygı gösterir, hata olursa loglar.
"""
from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time

import httpx

logger = logging.getLogger(__name__)

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"

_THREAD_GAP_SECONDS = 40   # tweet'ler arası bekleme (kullanıcı isteği)
_MAX_TWEETS = 5
_GENEL_HASHTAGS = "#borsaistanbul #BIST #hisse #yatırım"


def _gemini_key() -> str | None:
    try:
        from app.config import get_settings
        return get_settings().GEMINI_API_KEY or None
    except Exception:
        return os.getenv("GEMINI_API_KEY") or None


def _html_to_text(html: str) -> str:
    if not html:
        return ""
    try:
        import html as _h
        t = _h.unescape(html)
    except Exception:
        t = html
    t = re.sub(r"(?i)</(p|h[1-6]|li|div|br)>", "\n", t)
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n\s*\n+", "\n\n", t)
    return t.strip()


# Ticker'a benzeyen ama hisse OLMAYAN kısaltmalar (blog metninde sık geçer)
_TICKER_STOP = {
    "SPK", "KAP", "BIST", "VIOP", "ABD", "KDV", "IPO", "ETF", "BES", "GYO",
    "MKK", "BDDK", "TCMB", "KGF", "REIT", "FON", "NET", "USD", "EUR", "GBP",
    "TRY", "ESG", "ORANI", "PAY", "KAR", "HALK", "SAN", "TIC",
}


def _find_tickers(text: str) -> list[str]:
    """İçerikte örnek olarak geçen BIST hisse kodlarını bul (#THYAO gibi hashtag için).

    Sadece metinde ZATEN BÜYÜK HARFLE geçen kodlar sayılır (THYAO gibi gerçek hisse
    referansları büyük yazılır; "oranı" gibi normal kelimeler küçük → elenir).
    """
    try:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(base, "data", "ticker_names.json"), encoding="utf-8") as f:
            tickers = set(json.load(f).keys())
    except Exception:
        return []
    found = []
    # text BÜYÜTÜLMEZ — orijinal metindeki büyük-harf kodlar
    for tk in re.findall(r"\b[A-ZÇĞİÖŞÜ]{3,6}\b", text):
        if tk in tickers and tk not in _TICKER_STOP and tk not in found:
            found.append(tk)
        if len(found) >= 4:
            break
    return found


_THREAD_PROMPT = """Sen bir finans içerik editörüsün. Aşağıdaki blog yazısını Türkçe, {n} tweet'lik
bir X (Twitter) THREAD'ine dönüştür. Hesap premium (uzun tweet serbest) ama her tweet AKICI ve
~250-450 karakter olsun.

KURALLAR:
- Her tweet bir alt-konu/paragraf. Bilgilendirici, akıcı, ÖLÇÜLÜ emoji (her tweette 1-3).
- Tweet İÇİNDE cümleler arasında BOŞ SATIR bırak (okunabilirlik için \\n\\n).
- 1. tweet dikkat çekici giriş olsun ve 🧵 ile thread olduğunu belli et.
- Metinde geçen BIST hisse kodlarını ilgili tweette #KOD biçiminde hashtag yap (örn #THYAO).
- SON tweette: kısa kapanış + şu hashtag'ler: {hashtags} + "⚠️ Yatırım tavsiyesi değildir."
- Abartısız, profesyonel, doğru bilgi. Uydurma rakam ekleme.
- ÇIKTI: SADECE JSON dizi → ["tweet1","tweet2",...]. Başka hiçbir şey yazma.

BAŞLIK: {title}

İÇERİK:
{content}
"""


def generate_blog_thread(title: str, content_text: str, slug: str | None = None) -> list[str]:
    """Gemini ile blog'u 4-5 tweet thread'ine çevirir. Hata olursa paragraf-bölme fallback."""
    tickers = _find_tickers(f"{title}\n{content_text}")
    tag_line = " ".join(f"#{t}" for t in tickers)
    hashtags = (tag_line + " " + _GENEL_HASHTAGS).strip()

    key = _gemini_key()
    if key:
        try:
            prompt = _THREAD_PROMPT.format(
                n="4-5", title=title, content=content_text[:6000], hashtags=hashtags,
            )
            resp = httpx.post(
                _GEMINI_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Sadece geçerli JSON dizi döndür."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.6, "max_tokens": 6000,
                },
                timeout=90.0,
            )
            if resp.status_code == 200:
                txt = resp.json()["choices"][0]["message"]["content"].strip()
                txt = re.sub(r"^```(?:json)?\s*|\s*```$", "", txt).strip()
                arr = json.loads(txt[txt.find("["): txt.rfind("]") + 1])
                tweets = [str(t).strip() for t in arr if str(t).strip()][:_MAX_TWEETS]
                if len(tweets) >= 2:
                    # Son tweette hashtag/uyarı yoksa ekle (garanti)
                    if "#" not in tweets[-1]:
                        tweets[-1] = tweets[-1].rstrip() + "\n\n" + hashtags
                    if "tavsiye" not in tweets[-1].lower():
                        tweets[-1] = tweets[-1].rstrip() + "\n⚠️ Yatırım tavsiyesi değildir."
                    return tweets
        except Exception as e:
            logger.warning("Blog thread Gemini hata: %s", e)

    # Fallback — paragrafları böl
    paras = [p.strip() for p in content_text.split("\n\n") if len(p.strip()) > 40]
    tweets = [f"🧵 {title}"]
    for p in paras[:3]:
        tweets.append(p[:420])
    tweets.append(f"Detaylı rehber: borsacebimde.com\n\n{hashtags}\n⚠️ Yatırım tavsiyesi değildir.")
    return tweets[:_MAX_TWEETS]


def generate_blog_card(title: str, category: str | None = None) -> str | None:
    """Konuya uygun sade marka kartı (1080x1080 PNG) üretir — 1. tweete eklenir."""
    try:
        from PIL import Image, ImageDraw
        from app.services.chart_image_generator import (
            _load_font, _draw_bg_watermark, draw_brand_footer,
            BG_COLOR, GOLD, WHITE, GRAY,
        )
        W = H = 1080
        img = Image.new("RGB", (W, H), BG_COLOR)
        d = ImageDraw.Draw(img)
        _draw_bg_watermark(img, W, H)
        # Üst altın bar + etiket
        d.rectangle([(0, 0), (W, 8)], fill=GOLD)
        f_label = _load_font(30, bold=True)
        d.text((64, 60), "📚 BORSA CEBİMDE · REHBER", font=f_label, fill=GOLD)
        # Kategori rozeti
        cat_map = {
            "halka_arz": "Halka Arz", "kap": "KAP", "tavan_taban": "Tavan/Taban",
            "viop": "VİOP", "spk": "SPK", "borsa_rehberi": "Borsa Rehberi",
            "teknoloji": "Teknoloji", "temel_analiz": "Temel Analiz",
        }
        cat_txt = cat_map.get((category or "").lower(), "Borsa Rehberi")
        f_cat = _load_font(26, bold=False)
        d.text((64, 110), cat_txt, font=f_cat, fill=GRAY)
        # Başlık — büyük, sar
        f_title = _load_font(58, bold=True)
        words = title.split()
        lines, cur = [], ""
        avail = W - 128
        for w in words:
            cand = (cur + " " + w).strip()
            if d.textlength(cand, font=f_title) <= avail:
                cur = cand
            else:
                if cur:
                    lines.append(cur)
                cur = w
        if cur:
            lines.append(cur)
        lines = lines[:7]
        y = 320
        for ln in lines:
            d.text((64, y), ln, font=f_title, fill=WHITE)
            y += 78
        draw_brand_footer(d, img, W, H, source="borsacebimde.com", foot_h=90)
        fd, path = tempfile.mkstemp(suffix=".png", prefix="blog_card_")
        os.close(fd)
        img.save(path, "PNG", optimize=True)
        return path
    except Exception as e:
        logger.warning("Blog kart görseli üretilemedi: %s", e)
        return None


def post_blog_thread_sync(title: str, content_html: str, slug: str | None = None,
                          category: str | None = None) -> int:
    """SENKRON: thread üret + kart üret + 40sn aralıklarla zincir at. Gönderilen tweet sayısını döner."""
    from app.services.twitter_service import _safe_tweet_with_multi_media, is_tweets_killed
    if is_tweets_killed():
        logger.warning("[BLOG-X] Tweet kill switch açık — blog thread atlanmadı")
        return 0

    content_text = _html_to_text(content_html)
    tweets = generate_blog_thread(title, content_text, slug)
    if not tweets:
        return 0
    card = generate_blog_card(title, category)

    sent = 0
    parent_id = None
    card_path = None
    try:
        for i, tw in enumerate(tweets):
            imgs = [card] if (i == 0 and card) else []
            res = _safe_tweet_with_multi_media(
                tw, imgs, source="blog_thread",
                force_send=True, in_reply_to=parent_id, return_id=True,
            )
            if not res:
                logger.warning("[BLOG-X] %d. tweet gönderilemedi, thread durdu", i + 1)
                break
            parent_id = str(res)
            sent += 1
            logger.info("[BLOG-X] %d/%d tweet atıldı (id=%s)", i + 1, len(tweets), parent_id)
            if i < len(tweets) - 1:
                time.sleep(_THREAD_GAP_SECONDS)
    finally:
        if card and os.path.exists(card):
            try:
                os.remove(card)
            except OSError:
                pass
    logger.info("[BLOG-X] Blog thread tamamlandı: %d/%d tweet (slug=%s)", sent, len(tweets), slug)
    return sent


async def post_blog_to_twitter(title: str, content_html: str, slug: str | None = None,
                               category: str | None = None) -> None:
    """Async sarmalayıcı — bloglama thread'ini ayrı thread'de (event loop'u bloklamadan) çalıştırır."""
    import asyncio
    try:
        await asyncio.to_thread(post_blog_thread_sync, title, content_html, slug, category)
    except Exception as e:
        logger.warning("[BLOG-X] Blog thread görevi hata: %s", e)
