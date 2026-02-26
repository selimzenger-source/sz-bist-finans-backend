"""İzahname Analiz Görseli Üretici.

Tasarım: SZZZ.png stilinde koyu arka planlı, yeşil/kırmızı bölümlü PNG.
Layout:
  ┌─────────────────────────────────────────────────────────┐
  │  🏦 SZ Algo Finans    [ŞİRKET ADI]   [İZAHNAME ANALİZİ]│  ← Header
  ├─────────────────────────────────────────────────────────┤
  │  ✅ OLUMLU DİPNOTLAR                                     │  ← Yeşil başlık
  │  ●  Madde 1                                              │
  │  ●  Madde 2                                              │
  │  ...                                                     │
  ├─────────────────────────────────────────────────────────┤
  │  ⚠️ OLUMSUZ DİPNOTLAR                                    │  ← Kırmızı başlık
  │  ●  Madde 1                                              │
  │  ●  Madde 2                                              │
  │  ...                                                     │
  ├─────────────────────────────────────────────────────────┤
  │  📌 ÖZET: ...                                            │  ← Mavi bant
  ├─────────────────────────────────────────────────────────┤
  │  szalgo.net.tr          Yatırım tavsiyesi değildir       │  ← Footer
  └─────────────────────────────────────────────────────────┘

Her IPO için ayrı PNG üretilir ve /static/prospectus/{ipo_id}.png olarak saklanır.
"""

import logging
import os
import textwrap
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger(__name__)

# ─── Renkler ──────────────────────────────────────────────────────
BG_COLOR        = (14, 14, 26)           # #0e0e1a çok koyu
HEADER_BG       = (20, 20, 40)           # #141428
CARD_BG_GREEN   = (18, 40, 28)           # #12281c koyu yeşil
CARD_BG_RED     = (40, 18, 20)           # #281214 koyu kırmızı
CARD_BG_BLUE    = (18, 28, 48)           # özet bölümü
TOP_STRIPE      = (34, 197, 94)          # #22c55e yeşil şerit
GREEN           = (34, 197, 94)
RED             = (239, 68, 68)
GOLD            = (250, 204, 21)
ORANGE          = (251, 146, 60)
WHITE           = (255, 255, 255)
GRAY            = (156, 163, 175)
LIGHT_GRAY      = (110, 120, 140)
DIVIDER         = (50, 50, 80)
BLUE_ACC        = (99, 102, 241)         # #6366f1 indigo
CYAN            = (34, 211, 238)
YELLOW_SOFT     = (254, 240, 138)        # uyarı sarısı

# ─── Font Yolları ─────────────────────────────────────────────────
_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "C:/Windows/Fonts/consola.ttf",
    "C:/Windows/Fonts/arial.ttf",
]
_BOLD_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "C:/Windows/Fonts/consolab.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
]

_IMG_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "static", "img"
)

# İzahname görselleri kalıcı klasörü
_PROSPECTUS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "static", "prospectus"
)


def _ensure_dirs():
    os.makedirs(_PROSPECTUS_DIR, exist_ok=True)


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    paths = _BOLD_FONT_PATHS if bold else _FONT_PATHS
    for path in paths:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _draw_bg_watermark(img: Image.Image, width: int, height: int):
    """Diagonal szalgo.net.tr watermark."""
    try:
        wm_font = _load_font(28, bold=False)
        overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        wm_draw = ImageDraw.Draw(overlay)
        for yy in range(-height, height * 2, 300):
            for xx in range(-width, width * 2, 550):
                wm_draw.text((xx, yy), "szalgo.net.tr", fill=(255, 255, 255, 14), font=wm_font)
        overlay = overlay.rotate(-30, resample=Image.BICUBIC, expand=False)
        img_rgba = img.convert("RGBA")
        img_rgba = Image.alpha_composite(img_rgba, overlay)
        img.paste(img_rgba.convert("RGB"))
    except Exception:
        pass


def _wrap_text(text: str, font: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    """Metni max_width piksel genişliğinde satırlara böler."""
    words = text.split()
    lines = []
    current = ""
    for word in words:
        test = (current + " " + word).strip()
        bbox = font.getbbox(test)
        if bbox[2] - bbox[0] <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines or [text[:80]]


def _draw_section(
    draw: ImageDraw.ImageDraw,
    img: Image.Image,
    y: int,
    width: int,
    padding: int,
    title: str,
    title_color: tuple,
    bg_color: tuple,
    items: list[str],
    bullet_color: tuple,
    section_h: int,
    font_title: ImageFont.FreeTypeFont,
    font_item: ImageFont.FreeTypeFont,
    accent_color: tuple,
) -> int:
    """Bir bölümü çizer, yeni y pozisyonunu döner."""

    # Bölüm arka planı
    draw.rectangle(
        [(0, y), (width, y + section_h)],
        fill=bg_color,
    )

    # Sol aksan çizgisi
    draw.rectangle(
        [(0, y), (5, y + section_h)],
        fill=accent_color,
    )

    # Başlık
    title_y = y + 14
    draw.text((padding, title_y), title, fill=title_color, font=font_title)
    item_y = title_y + 46

    # Maddeler
    item_max_w = width - padding * 2 - 30
    for item in items:
        # Bullet nokta
        draw.ellipse(
            [(padding, item_y + 8), (padding + 8, item_y + 16)],
            fill=bullet_color,
        )
        # Metin (wrap)
        lines = _wrap_text(item, font_item, item_max_w)
        for li, line in enumerate(lines):
            draw.text(
                (padding + 18, item_y + li * 26),
                line, fill=WHITE, font=font_item,
            )
        item_y += max(len(lines) * 26 + 8, 36)

    return y + section_h


def generate_prospectus_analysis_image(
    company_name: str,
    ipo_price: Optional[str],
    analysis: dict,
    ipo_id: int,
    pages_analyzed: int = 0,
) -> Optional[str]:
    """İzahname analizi için PNG görsel üretir.

    Args:
        company_name: Şirket adı
        ipo_price: Halka arz fiyatı (string veya None)
        analysis: {positives: [], negatives: [], summary: str, risk_level: str}
        ipo_id: IPO DB ID (dosya adı için)

    Returns:
        Kaydedilen PNG dosyasının absolute yolu veya None
    """
    try:
        _ensure_dirs()

        positives   = analysis.get("positives", [])[:7]
        negatives   = analysis.get("negatives", [])[:7]
        summary     = analysis.get("summary", "")[:300]
        risk_level  = analysis.get("risk_level", "orta")
        key_risk    = analysis.get("key_risk", "")

        # Risk rengi
        risk_colors = {
            "düşük":      (34, 197, 94),
            "orta":       (250, 204, 21),
            "yüksek":     (251, 146, 60),
            "çok yüksek": (239, 68, 68),
        }
        risk_color = risk_colors.get(risk_level, ORANGE)

        # ─── Fontlar ──────────────────────────────────────────
        f_header_brand  = _load_font(36, bold=True)
        f_header_sm     = _load_font(22)
        f_badge         = _load_font(22, bold=True)
        f_section_title = _load_font(28, bold=True)
        f_item          = _load_font(22)
        f_summary       = _load_font(22)
        f_footer        = _load_font(24, bold=True)
        f_footer_sm     = _load_font(20)

        # ─── Boyutlar ─────────────────────────────────────────
        width       = 1200
        padding     = 48
        inner_w     = width - padding * 2

        top_stripe_h = 6
        header_h     = 130

        # Bölüm yüksekliği hesapla (madde sayısına göre)
        def _calc_section_h(items: list, base: int = 60) -> int:
            total = base  # başlık
            for item in items:
                lines = max(1, len(item) // 58 + 1)
                total += lines * 26 + 8 + 10
            return total + 20

        pos_section_h = _calc_section_h(positives, base=60)
        neg_section_h = _calc_section_h(negatives, base=60)

        # Özet bant yüksekliği
        summary_lines = max(1, len(summary) // 90 + 1)
        summary_h     = 20 + summary_lines * 28 + 30
        if key_risk:
            summary_h += 40

        footer_h     = 80
        gap          = 12

        total_h = (
            top_stripe_h + header_h + gap
            + pos_section_h + gap
            + neg_section_h + gap
            + summary_h + gap
            + footer_h
        )

        # ─── Canvas ───────────────────────────────────────────
        img  = Image.new("RGB", (width, total_h), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # ─── Üst renkli şerit ─────────────────────────────────
        draw.rectangle([(0, 0), (width, top_stripe_h)], fill=TOP_STRIPE)

        # ─── Header ───────────────────────────────────────────
        y = top_stripe_h
        draw.rectangle([(0, y), (width, y + header_h)], fill=HEADER_BG)

        # Logo
        logo_path = os.path.join(_IMG_DIR, "logo.jpg")
        logo_x = padding
        logo_size = 70
        try:
            if os.path.exists(logo_path):
                logo_raw = Image.open(logo_path).convert("RGBA")
                logo_r   = logo_raw.resize((logo_size, logo_size), Image.LANCZOS)
                tmp      = Image.new("RGBA", img.size, (0, 0, 0, 0))
                tmp.paste(logo_r, (logo_x, y + (header_h - logo_size) // 2))
                img_rgba = img.convert("RGBA")
                img_rgba = Image.alpha_composite(img_rgba, tmp)
                img.paste(img_rgba.convert("RGB"))
                draw = ImageDraw.Draw(img)  # Draw nesnesini yenile
                logo_x += logo_size + 14
        except Exception:
            pass

        # Brand text
        brand_y = y + 22
        draw.text((logo_x, brand_y), "SZ Algo Finans", fill=WHITE, font=f_header_brand)
        draw.text((logo_x, brand_y + 44), "İzahname Analizi", fill=GRAY, font=f_header_sm)

        # Şirket adı — orta
        company_short = company_name if len(company_name) <= 42 else company_name[:40] + "…"
        cb = f_badge.getbbox(company_short)
        cw = cb[2] - cb[0] + 28
        ch = 34
        cx = (width - cw) // 2
        cy = y + (header_h - ch) // 2
        draw.rounded_rectangle(
            [(cx, cy), (cx + cw, cy + ch)],
            radius=6, fill=(35, 35, 60),
        )
        draw.text((cx + 14, cy + 7), company_short, fill=WHITE, font=f_badge)

        # Fiyat badge — sağ (Risk badge kaldırıldı, sadece HA fiyatı)
        if ipo_price:
            price_text = f"HA: {ipo_price} TL"
            pb = f_badge.getbbox(price_text)
            pw = pb[2] - pb[0] + 24
            px = width - padding - pw
            py = y + (header_h - 32) // 2   # Dikey ortalanmış
            draw.rounded_rectangle(
                [(px, py), (px + pw, py + 32)],
                radius=6, fill=(35, 50, 35),
            )
            draw.text((px + 12, py + 7), price_text, fill=GREEN, font=f_badge)

        # Alt divider
        draw.line(
            [(0, y + header_h - 1), (width, y + header_h - 1)],
            fill=DIVIDER, width=1,
        )

        # ─── Olumlu Bölüm ─────────────────────────────────────
        y += header_h + gap
        y = _draw_section(
            draw=draw, img=img, y=y,
            width=width, padding=padding,
            title="✅  OLUMLU DİPNOTLAR",
            title_color=GREEN,
            bg_color=CARD_BG_GREEN,
            items=positives,
            bullet_color=GREEN,
            section_h=pos_section_h,
            font_title=f_section_title,
            font_item=f_item,
            accent_color=GREEN,
        )

        # ─── Olumsuz Bölüm ────────────────────────────────────
        y += gap
        y = _draw_section(
            draw=draw, img=img, y=y,
            width=width, padding=padding,
            title="⚠️  OLUMSUZ DİPNOTLAR",
            title_color=(255, 100, 100),
            bg_color=CARD_BG_RED,
            items=negatives,
            bullet_color=RED,
            section_h=neg_section_h,
            font_title=f_section_title,
            font_item=f_item,
            accent_color=RED,
        )

        # ─── Özet Bant ────────────────────────────────────────
        y += gap
        draw.rectangle(
            [(0, y), (width, y + summary_h)],
            fill=CARD_BG_BLUE,
        )
        draw.rectangle([(0, y), (5, y + summary_h)], fill=CYAN)

        sum_y = y + 16
        draw.text((padding, sum_y), "📌  ÖZET", fill=CYAN, font=_load_font(24, bold=True))
        sum_y += 36

        sum_lines = _wrap_text(summary, f_summary, inner_w - 20)
        for line in sum_lines:
            draw.text((padding, sum_y), line, fill=LIGHT_GRAY, font=f_summary)
            sum_y += 28

        if key_risk:
            sum_y += 4
            draw.text((padding, sum_y), f"🔑 Kritik Risk: {key_risk[:100]}",
                      fill=YELLOW_SOFT, font=_load_font(20))

        y += summary_h

        # ─── Footer ───────────────────────────────────────────
        y += gap
        draw.rectangle([(0, y), (width, y + footer_h)], fill=HEADER_BG)
        draw.line([(0, y), (width, y)], fill=TOP_STRIPE, width=2)

        # Logo
        footer_logo_x = padding
        try:
            if os.path.exists(logo_path):
                fl = Image.open(logo_path).convert("RGBA").resize((30, 30), Image.LANCZOS)
                tmp3 = Image.new("RGBA", img.size, (0, 0, 0, 0))
                tmp3.paste(fl, (footer_logo_x, y + 20))
                img_rgba3 = img.convert("RGBA")
                img_rgba3 = Image.alpha_composite(img_rgba3, tmp3)
                img.paste(img_rgba3.convert("RGB"))
                draw = ImageDraw.Draw(img)
                footer_logo_x += 36
        except Exception:
            pass

        draw.text((footer_logo_x, y + 14), "szalgo.net.tr", fill=ORANGE, font=f_footer)

        # Sayfa sayısı + dipnot sayısı — orta
        dipnot_count = len(positives) + len(negatives)
        if pages_analyzed > 0:
            info_text = f"📄 {pages_analyzed} sayfa analiz edildi  •  {dipnot_count} dipnot yakalandı"
        else:
            info_text = f"📌 {dipnot_count} dipnot yakalandı"
        ib = f_footer_sm.getbbox(info_text)
        iw = ib[2] - ib[0]
        draw.text(
            ((width - iw) // 2, y + 18),
            info_text, fill=CYAN, font=f_footer_sm,
        )

        disc = "Yatırım tavsiyesi değildir"
        db   = f_footer_sm.getbbox(disc)
        dw   = db[2] - db[0]
        draw.text(
            (width - padding - dw, y + 14),
            disc, fill=GRAY, font=f_footer_sm,
        )

        # ─── Watermark ────────────────────────────────────────
        _draw_bg_watermark(img, width, total_h)

        # ─── Kaydet ───────────────────────────────────────────
        filename = f"prospectus_{ipo_id}.png"
        filepath = os.path.join(_PROSPECTUS_DIR, filename)
        img.save(filepath, "PNG", optimize=True)

        file_size = os.path.getsize(filepath)
        logger.info(
            "İzahname görseli üretildi: %s (%d KB, %dx%d px)",
            filepath, file_size // 1024, width, total_h,
        )
        return filepath

    except Exception as e:
        logger.error("generate_prospectus_analysis_image hatası: %s", e, exc_info=True)
        return None
