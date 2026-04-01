"""Haber kapak resmi olusturucu — Gemini Imagen + Pillow overlay.

1. Gemini AI ile konu ile alakali arka plan gorseli uretir
2. Pillow ile baslik, kategori, branding yazilari overlay eder
Sonuc: Profesyonel, konu ile alakali, markalı kapak resmi.
"""

import base64
import io
import logging
import os
import tempfile

import httpx
from PIL import Image, ImageDraw, ImageFont, ImageFilter

from app.config import get_settings

logger = logging.getLogger(__name__)

W, H = 1200, 675

# ── Renkler ────────────────────────────────────────────
CATEGORY_COLORS = {
    "HALKA_ARZ": (21, 101, 192),
    "TURKIYE_GUNDEM": (198, 40, 40),
    "SIRKET_HABERI": (46, 125, 50),
    "PIYASA": (106, 27, 154),
    "GLOBAL": (183, 28, 28),
    "SEKTOR": (0, 150, 136),
}

BANNER_LABELS = {
    "SON_DAKIKA": "SON DAKIKA",
    "HALKA_ARZ": "HALKA ARZ",
    "SIRKET_HABERI": "SIRKET HABERI",
    "SEKTOR": "SEKTOR",
    "GLOBAL": "GLOBAL GUNDEM",
    "PIYASA": "PIYASA",
    "TURKIYE_GUNDEM": "TURKIYE GUNDEMI",
}

# ── Font Yollari ──────────────────────────────────────
_BOLD_FONTS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
]
_REGULAR_FONTS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "C:/Windows/Fonts/arial.ttf",
]

# Kategori bazli tema (Gemini prompt)
_CATEGORY_THEMES = {
    "HALKA_ARZ": "IPO stock market launch, celebration bells, Turkish stock exchange BIST",
    "TURKIYE_GUNDEM": "Turkish economy, Central Bank TCMB, government policy",
    "SIRKET_HABERI": "corporate business deal, company earnings, corporate building",
    "PIYASA": "stock market trading, candlestick charts, market data screens",
    "GLOBAL": "world economy, global markets, world map with glowing connections",
    "SEKTOR": "industry sector, manufacturing, technology infrastructure",
}


def _load_font(paths: list[str], size: int) -> ImageFont.FreeTypeFont:
    for p in paths:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _wrap_text(text: str, font: ImageFont.FreeTypeFont, max_w: int) -> list[str]:
    words = text.split()
    lines, current = [], ""
    for word in words:
        test = f"{current} {word}".strip()
        if font.getbbox(test)[2] > max_w and current:
            lines.append(current)
            current = word
        else:
            current = test
    if current:
        lines.append(current)
    return lines


async def _generate_gemini_background(headline: str, category: str) -> bytes | None:
    """Gemini Imagen ile arka plan gorseli uret."""
    settings = get_settings()
    api_key = settings.GEMINI_API_KEY
    if not api_key:
        return None

    theme = _CATEGORY_THEMES.get(category, "financial news, stock market")

    prompt = (
        f"Create a cinematic, professional background image for a financial news cover. "
        f"Theme: {theme}. "
        f"News topic: '{headline}'. "
        f"Style: Dark, moody, premium fintech aesthetic. "
        f"Dark navy/deep blue gradient background (#0D1B2A to #1B2838). "
        f"Abstract geometric shapes, subtle glowing grid lines, bokeh light effects. "
        f"Professional color accents: electric blue (#2979FF), emerald (#00E676), gold (#FFD600). "
        f"Financial iconography: charts, data flows, digital patterns. "
        f"The image must work as a BACKGROUND — leave space for text overlay. "
        f"Keep the center-left area relatively clean/dark for text readability. "
        f"Aspect ratio 16:9. No text. No watermark. No human faces."
    )

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key={api_key}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "responseModalities": ["TEXT", "IMAGE"],
                        "responseMimeType": "text/plain",
                    },
                },
            )

        if resp.status_code == 200:
            data = resp.json()
            for candidate in data.get("candidates", []):
                for part in candidate.get("content", {}).get("parts", []):
                    if "inlineData" in part:
                        img_bytes = base64.b64decode(part["inlineData"]["data"])
                        logger.info("Gemini arka plan uretildi (%d bytes)", len(img_bytes))
                        return img_bytes

        logger.warning("Gemini gorsel uretemedi (status=%s)", resp.status_code)
        return None
    except Exception as e:
        logger.warning("Gemini arka plan hatasi: %s", e)
        return None


def _create_fallback_background() -> Image.Image:
    """Gemini basarisizsa koyu gradient arka plan."""
    img = Image.new("RGB", (W, H), (13, 27, 42))
    draw = ImageDraw.Draw(img)
    for y in range(H):
        ratio = y / H
        r = int(13 + (8 - 13) * ratio)
        g = int(27 + (16 - 27) * ratio)
        b = int(42 + (28 - 42) * ratio)
        draw.line([(0, y), (W, y)], fill=(r, g, b))
    return img


def _overlay_text(
    img: Image.Image,
    headline: str,
    category: str,
    source: str,
    banner: str | None,
) -> Image.Image:
    """Arka plan gorseli uzerine baslik, kategori ve branding ekle."""
    # Koyu overlay — metin okunabilirligi icin
    overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    ov_draw = ImageDraw.Draw(overlay)

    # Alt yari karanlik gradient overlay
    for y in range(H):
        alpha = int(min(200, max(0, (y - H * 0.2) / (H * 0.8) * 200)))
        ov_draw.line([(0, y), (W, y)], fill=(0, 0, 0, alpha))

    # Ust banner bar (tam opak)
    cat_color = CATEGORY_COLORS.get(category, (106, 27, 154))
    ov_draw.rectangle([(0, 0), (W, 70)], fill=(*cat_color, 230))

    # Banner metni
    banner_key = banner or category
    banner_text = BANNER_LABELS.get(banner_key, "HABER")
    banner_font = _load_font(_BOLD_FONTS, 36)
    bbox = banner_font.getbbox(banner_text)
    bw = bbox[2] - bbox[0]
    ov_draw.text(((W - bw) // 2, 14), banner_text, fill=(255, 255, 255, 255), font=banner_font)

    # Alt bilgi kutusu (koyu bar)
    ov_draw.rectangle([(0, H - 70), (W, H)], fill=(0, 0, 0, 200))

    img_rgba = img.convert("RGBA")
    img_rgba = Image.alpha_composite(img_rgba, overlay)
    img = img_rgba.convert("RGB")
    draw = ImageDraw.Draw(img)

    # ── Ana baslik ──
    headline = headline.upper().strip()
    text_len = len(headline)
    if text_len <= 40:
        font_size, line_h = 54, 68
    elif text_len <= 80:
        font_size, line_h = 44, 58
    elif text_len <= 120:
        font_size, line_h = 38, 50
    else:
        font_size, line_h = 32, 44

    h_font = _load_font(_BOLD_FONTS, font_size)
    lines = _wrap_text(headline, h_font, W - 120)
    total_h = len(lines) * line_h
    start_y = max(100, 100 + (420 - total_h) // 2)

    # Golge + beyaz metin
    for i, line in enumerate(lines):
        y = start_y + i * line_h
        # Golge
        draw.text((62, y + 2), line, fill=(0, 0, 0), font=h_font)
        # Ana metin
        draw.text((60, y), line, fill=(255, 255, 255), font=h_font)

    # ── Accent cizgi ──
    accent_y = start_y + len(lines) * line_h + 12
    if accent_y < H - 90:
        draw.rectangle([(60, accent_y), (260, accent_y + 4)], fill=cat_color)

    # ── Footer: Kaynak + Branding ──
    footer_y = H - 50
    if source:
        src_font = _load_font(_REGULAR_FONTS, 20)
        draw.text((30, footer_y), f"Kaynak: {source}", fill=(180, 195, 210), font=src_font)

    brand_font = _load_font(_BOLD_FONTS, 18)
    brand = "Borsa Cebimde | borsacebimde.app"
    bb = brand_font.getbbox(brand)
    draw.text((W - (bb[2] - bb[0]) - 30, footer_y), brand, fill=(140, 210, 160), font=brand_font)

    return img


async def generate_news_cover(
    headline: str,
    category: str = "PIYASA",
    source: str = "",
    banner: str | None = None,
) -> str | None:
    """Haber kapak resmi olustur: Gemini arka plan + Pillow text overlay.

    Returns:
        Gecici PNG dosya yolu veya None
    """
    try:
        # 1. Gemini ile arka plan uret
        bg_bytes = await _generate_gemini_background(headline, category)

        if bg_bytes:
            bg_img = Image.open(io.BytesIO(bg_bytes)).convert("RGB")
            bg_img = bg_img.resize((W, H), Image.LANCZOS)
        else:
            bg_img = _create_fallback_background()

        # 2. Pillow ile text overlay
        final = _overlay_text(bg_img, headline, category, source, banner)

        # 3. Kaydet
        fd, path = tempfile.mkstemp(suffix=".png", prefix="news_cover_")
        os.close(fd)
        final.save(path, "PNG", optimize=True)
        logger.info("Kapak resmi tamamlandi: %s (%s)", path, category)
        return path

    except Exception as e:
        logger.error("Kapak resmi olusturulamadi: %s", e)
        return None
