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
_GENEL_HASHTAGS = "#borsa #BIST"  # X kuralı: max 2 hashtag (3+ = spam, erişim ölür)


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
- Her tweet bir alt-konu. Bilgilendirici, akıcı, ÖLÇÜLÜ emoji (her tweette 1-3).
- ★ BİÇİM (ÇOK ÖNEMLİ): HER tweette cümleleri ALT ALTA yaz; HER cümleden sonra BİR BOŞ
  SATIR (\\n\\n) bırak. Cümleleri ASLA yan yana bitişik yazma. Şu biçimde olmalı:
  "İlk cümle burada. 🚀\\n\\nİkinci cümle ayrı satırda.\\n\\nÜçüncü cümle de ayrı 👇"
  (Yani tek paragraf bloğu DEĞİL — her cümle kendi satırında, aralarında boşluk.)
- 1. tweet dikkat çekici giriş olsun ve 🧵 ile thread olduğunu belli et.
- Metinde geçen BIST hisse kodlarını ilgili tweette #KOD biçiminde hashtag yap (örn #THYAO).
- SON tweette: kısa kapanış + şu hashtag'ler: {hashtags} + "⚠️ Yatırım tavsiyesi değildir."
- Abartısız, profesyonel, doğru bilgi. Uydurma rakam ekleme.
- ÇIKTI: SADECE JSON dizi → ["tweet1","tweet2",...]. Başka hiçbir şey yazma.

BAŞLIK: {title}

İÇERİK:
{content}
"""


def _anthropic_key() -> str | None:
    try:
        from app.config import get_settings
        return getattr(get_settings(), "ANTHROPIC_API_KEY", "") or None
    except Exception:
        return None


def _finalize_thread(tweets: list[str], hashtags: str) -> list[str]:
    """Son tweette hashtag + yatırım uyarısı garanti et."""
    tweets = [str(t).strip() for t in tweets if str(t).strip()][:_MAX_TWEETS]
    if not tweets:
        return tweets
    if "#" not in tweets[-1]:
        tweets[-1] = tweets[-1].rstrip() + "\n\n" + hashtags
    if "tavsiye" not in tweets[-1].lower():
        tweets[-1] = tweets[-1].rstrip() + "\n\n⚠️ Yatırım tavsiyesi değildir."
    return tweets


def _parse_thread_json(txt: str) -> list[str]:
    txt = re.sub(r"^```(?:json)?\s*|\s*```$", "", (txt or "").strip()).strip()
    i, j = txt.find("["), txt.rfind("]")
    if i < 0 or j < 0:
        return []
    arr = json.loads(txt[i:j + 1])
    return [str(t).strip() for t in arr if str(t).strip()]


def _thread_naive_fallback(title: str, content_text: str, hashtags: str) -> list[str]:
    """Cümle-GÜVENLİ kaba fallback — kelime/cümle ORTASINDAN ASLA kesmez.

    Cümlelere ayırır, ~260 karaktere kadar gruplar; her grup içindeki cümleleri
    alt alta + boş satırla dizer (şık görünüm). İlk tweet 🧵 girişli.
    """
    sentences = [s.strip() for s in re.split(r'(?<=[.!?])\s+', content_text) if len(s.strip()) > 15]
    bodies: list[list[str]] = []
    cur: list[str] = []
    cur_len = 0
    for s in sentences:
        if len(s) > 270:  # tek cümle çok uzunsa kelime sınırında kırp (orta-kelime DEĞİL)
            s = s[:270].rsplit(" ", 1)[0] + "…"
        if cur_len + len(s) > 250 and cur:
            bodies.append(cur); cur = []; cur_len = 0
        cur.append(s); cur_len += len(s) + 2
        if len(bodies) >= _MAX_TWEETS - 1:
            break
    if cur and len(bodies) < _MAX_TWEETS - 1:
        bodies.append(cur)
    tweets = [f"🧵 {title}\n\nDetaylar aşağıda 👇"]
    for grp in bodies[:_MAX_TWEETS - 2]:
        tweets.append("\n\n".join(grp))   # cümleler alt alta + boş satır
    tweets.append(f"📲 Detaylı rehber: borsacebimde.com\n\n{hashtags}")
    return tweets[:_MAX_TWEETS]


def generate_blog_thread(title: str, content_text: str, slug: str | None = None) -> list[str]:
    """Blog'u 4-5 tweet'lik AKICI thread'e çevirir.

    Sıra: Claude Sonnet (birincil, en kaliteli) → Gemini → cümle-güvenli fallback.
    Format: her tweette cümleler alt alta + boş satır, emoji, max 2 genel hashtag.
    """
    tickers = _find_tickers(f"{title}\n{content_text}")
    tag_line = " ".join(f"#{t}" for t in tickers[:1])  # en fazla 1 ticker hashtag
    hashtags = (tag_line + " " + _GENEL_HASHTAGS).strip()
    prompt = _THREAD_PROMPT.format(
        n="4-5", title=title, content=content_text[:6000], hashtags=hashtags,
    )

    # 1) Claude Sonnet — Ayın Halka Arzı ile aynı kalite
    akey = _anthropic_key()
    if akey:
        try:
            resp = httpx.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": akey, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 4000,
                    "system": "Sadece geçerli JSON dizi döndür, başka hiçbir şey yazma.",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.5,
                },
                timeout=90.0,
            )
            if resp.status_code == 200:
                blocks = resp.json().get("content", [])
                txt = next((b.get("text", "") for b in blocks if b.get("type") == "text"), "")
                tweets = _parse_thread_json(txt)
                if len(tweets) >= 2:
                    logger.info("Blog thread: Claude Sonnet (%d tweet)", len(tweets))
                    return _finalize_thread(tweets, hashtags)
            else:
                logger.warning("Blog thread Claude HTTP %d", resp.status_code)
        except Exception as e:
            logger.warning("Blog thread Claude hata: %s", e)

    # 2) Gemini fallback
    key = _gemini_key()
    if key:
        try:
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
                tweets = _parse_thread_json(resp.json()["choices"][0]["message"]["content"])
                if len(tweets) >= 2:
                    logger.info("Blog thread: Gemini (%d tweet)", len(tweets))
                    return _finalize_thread(tweets, hashtags)
        except Exception as e:
            logger.warning("Blog thread Gemini hata: %s", e)

    # 3) Cümle-güvenli fallback (kelime ortasından KESMEZ)
    logger.warning("Blog thread: AI başarısız, cümle-güvenli fallback")
    return _finalize_thread(_thread_naive_fallback(title, content_text, hashtags), hashtags)


def generate_blog_card(title: str, category: str | None = None) -> str | None:
    """Konuya uygun sade marka kartı (1080x1080 PNG) üretir — 1. tweete eklenir."""
    try:
        from PIL import Image, ImageDraw
        from app.services.chart_image_generator import (
            _load_font, _draw_bg_watermark, draw_brand_footer,
            BG_COLOR, GOLD, WHITE, GRAY,
        )
        # YATAY 16:9 — boşluk az, Twitter kart oranıyla uyumlu
        W, H = 1200, 675
        PAD = 64
        FOOT_H = 84
        img = Image.new("RGB", (W, H), BG_COLOR)
        d = ImageDraw.Draw(img)
        _draw_bg_watermark(img, W, H)
        # Üst altın bar + etiket
        d.rectangle([(0, 0), (W, 7)], fill=GOLD)
        f_label = _load_font(28, bold=True)
        d.text((PAD, 40), "📚 BORSA CEBİMDE · REHBER", font=f_label, fill=GOLD)
        cat_map = {
            "halka_arz": "Halka Arz", "kap": "KAP", "tavan_taban": "Tavan/Taban",
            "viop": "VİOP", "spk": "SPK", "borsa_rehberi": "Borsa Rehberi",
            "teknoloji": "Teknoloji", "temel_analiz": "Temel Analiz",
        }
        cat_txt = cat_map.get((category or "").lower(), "Borsa Rehberi")
        f_cat = _load_font(24, bold=False)
        d.text((PAD, 84), cat_txt, font=f_cat, fill=GRAY)

        # Başlık — uzunluğa göre font seç, sar, header ile footer arasına ORTALA
        top = 150               # başlık alanı başı (etiketlerin altı)
        bot = H - FOOT_H - 24   # footer üstü
        avail_w = W - 2 * PAD

        def _wrap(font):
            out, cur = [], ""
            for w in title.split():
                cand = (cur + " " + w).strip()
                if d.textlength(cand, font=font) <= avail_w:
                    cur = cand
                else:
                    if cur:
                        out.append(cur)
                    cur = w
            if cur:
                out.append(cur)
            return out

        # Boşluğu dolduracak en büyük fontu seç (alana sığana kadar küçült)
        f_title = None
        lines = []
        for sz, lh in ((64, 80), (56, 72), (50, 64), (44, 58)):
            f = _load_font(sz, bold=True)
            ls = _wrap(f)
            if len(ls) * lh <= (bot - top):
                f_title, lines, line_h = f, ls, lh
                break
        if f_title is None:
            f_title = _load_font(44, bold=True)
            lines = _wrap(f_title)[:6]
            line_h = 58

        block_h = len(lines) * line_h
        y = top + max(0, ((bot - top) - block_h) // 2)  # dikey ortala
        for ln in lines:
            d.text((PAD, y), ln, font=f_title, fill=WHITE)
            y += line_h
        draw_brand_footer(d, img, W, H, source="borsacebimde.com", foot_h=FOOT_H)
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
