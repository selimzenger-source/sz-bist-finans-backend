"""Halka arz performans gorsel olusturucu.

Pillow ile koyu arka planli, renkli satirli PNG tablo olusturur.
Tweet'e resim olarak eklenir.
- 25 gunluk karne gorseli (generate_25day_image)
- Gunluk takip gorseli (generate_daily_tracking_image) — 6+ gun
"""

import logging
import os
import tempfile
from datetime import datetime
from decimal import Decimal
from typing import Optional

from PIL import Image, ImageDraw, ImageFont, ImageFilter

logger = logging.getLogger(__name__)

# ── Renkler ────────────────────────────────────────────────
BG_COLOR = (18, 18, 32)          # #121220 koyu arka plan
HEADER_BG = (26, 26, 46)         # #1a1a2e header
ROW_EVEN = (22, 22, 38)          # #161626
ROW_ODD = (26, 26, 46)           # #1a1a2e
GREEN = (34, 197, 94)            # #22c55e
RED = (239, 68, 68)              # #ef4444
WHITE = (255, 255, 255)
GRAY = (156, 163, 175)           # #9ca3af
GOLD = (250, 204, 21)            # #facc15
DIVIDER = (55, 55, 80)           # #373750
ORANGE = (251, 146, 60)          # #fb923c

# ── Font ───────────────────────────────────────────────────
# Render (Linux) DejaVu fontlari mevcut
_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    # Windows fallback
    "C:/Windows/Fonts/consola.ttf",
    "C:/Windows/Fonts/consolab.ttf",
]

_BOLD_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "C:/Windows/Fonts/consolab.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
]


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Font yukle, bulamazsa default kullan."""
    paths = _BOLD_FONT_PATHS if bold else _FONT_PATHS
    for path in paths:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _draw_bg_watermark(img: Image.Image, width: int, height: int):
    """Gorsel uzerine silik capraz 'szalgo.net.tr' watermark basar."""
    try:
        wm_font = _load_font(30, bold=False)
        wm_text = "szalgo.net.tr"
        # Seffaf katman olustur
        overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        wm_draw = ImageDraw.Draw(overlay)

        # Kucuk font, genis aralik — silik ve profesyonel
        spacing_x = 600
        spacing_y = 320
        for yy in range(-height, height * 2, spacing_y):
            for xx in range(-width, width * 2, spacing_x):
                wm_draw.text((xx, yy), wm_text, fill=(255, 255, 255, 16), font=wm_font)

        # Katmani -30 derece dondur
        overlay = overlay.rotate(-30, resample=Image.BICUBIC, expand=False)
        # Ortala kirp (rotate expand=False ile ayni boyut kalir)

        # Ana gorseli RGBA'ya cevir, overlay'i birlestur, sonra RGB'ye geri don
        img_rgba = img.convert("RGBA")
        img_rgba = Image.alpha_composite(img_rgba, overlay)
        # Sonucu orijinal img'ye geri yaz
        img.paste(img_rgba.convert("RGB"))
    except Exception as e:
        logger.warning("Watermark eklenemedi: %s", e)


def generate_25day_image(
    ipo,
    days_data: list,
    ceiling_days: int,
    floor_days: int,
    avg_lot: Optional[float] = None,
) -> Optional[str]:
    """25 gunluk karne gorseli olusturur.

    Args:
        ipo: IPO model objesi (ticker, company_name, ipo_price)
        days_data: 25 gunluk veri listesi [{trading_day, close, open, high, low, ...}]
        ceiling_days: Tavan kapanan gun sayisi
        floor_days: Taban kapanan gun sayisi
        avg_lot: Kisi basi ortalama lot (opsiyonel)

    Returns:
        Olusturulan PNG dosyasinin yolu veya None
    """
    try:
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
        ticker = ipo.ticker or "BILINMIYOR"
        if not days_data or ipo_price <= 0:
            logger.warning("generate_25day_image: veri eksik (days_data=%s, ipo_price=%s)",
                           len(days_data) if days_data else 0, ipo_price)
            return None

        last_close = float(days_data[-1]["close"])
        total_pct = ((last_close - ipo_price) / ipo_price) * 100
        normal_days = len(days_data) - ceiling_days - floor_days

        # Lot bazli kar/zarar
        lot_count = int(avg_lot) if avg_lot else 0
        lot_profit = 0.0
        if lot_count > 0:
            lot_profit = (last_close - ipo_price) * lot_count  # lot = adet

        # ── Banner tema gorseli yukle ─────────────────
        banner_h = 0
        banner_img = None
        _IMG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
        banner_path = os.path.join(_IMG_DIR, "25_gun_performans_banner.png")
        if os.path.exists(banner_path):
            try:
                banner_img = Image.open(banner_path).convert("RGB")
                # Banner'i tablonun genisligine kucult, oran koru
                banner_ratio = banner_img.width / banner_img.height
                banner_h = int(1200 / banner_ratio)
                if banner_h > 500:
                    banner_h = 500  # max 500px
                banner_img = banner_img.resize((1200, banner_h), Image.LANCZOS)
            except Exception as be:
                logger.warning("Banner yuklenemedi: %s", be)
                banner_img = None
                banner_h = 0

        # ── Boyut hesapla ──────────────────────────────
        width = 1200
        header_h = 280       # ust bilgi alani (kisaltildi)
        row_h = 44            # her satir yuksekligi
        col_header_h = 50     # sutun baslik satiri
        footer_h = 70         # alt — sadece szalgo.net.tr
        padding = 40
        num_rows = len(days_data)
        table_h = col_header_h + (num_rows * row_h)
        total_h = banner_h + header_h + table_h + footer_h

        img = Image.new("RGB", (width, total_h), BG_COLOR)

        # Banner'i uste yapistir
        if banner_img:
            img.paste(banner_img, (0, 0))

        # ── ARKA PLAN WATERMARK (silik çapraz szalgo.net.tr) ──
        _draw_bg_watermark(img, width, total_h)

        draw = ImageDraw.Draw(img)

        # Fontlar
        font_title = _load_font(38, bold=True)
        font_subtitle = _load_font(28)
        font_big = _load_font(34, bold=True)
        font_row = _load_font(26)
        font_row_bold = _load_font(26, bold=True)
        font_col_header = _load_font(24, bold=True)
        font_footer = _load_font(28, bold=True)
        font_footer_sm = _load_font(24)
        font_watermark = _load_font(22)

        y = banner_h + padding

        # ── HEADER ─────────────────────────────────────
        # Baslik
        title = f"{ticker} — 25 Günü Bitirdi"
        draw.text((padding, y), title, fill=WHITE, font=font_title)
        y += 48

        # Sirket adi
        company = ipo.company_name or ""
        if company and company != ticker:
            draw.text((padding, y), company, fill=GRAY, font=font_subtitle)
            y += 36

        # Halka arz fiyati
        draw.text((padding, y), f"Halka Arz Fiyatı: {ipo_price:.2f} TL",
                  fill=GRAY, font=font_subtitle)
        y += 40

        # Kisi basi lot
        if lot_count > 0:
            draw.text((padding, y), f"Kişi Başı Ort Lot: {lot_count}",
                      fill=GRAY, font=font_subtitle)
            y += 40

            # Kar/zarar
            profit_color = GREEN if lot_profit >= 0 else RED
            if lot_profit >= 0:
                profit_text = f"25. Gün Karnesi: +{lot_profit:,.0f} TL (%{total_pct:+.1f})"
            else:
                profit_text = f"25. Gün Karnesi: {lot_profit:,.0f} TL (%{total_pct:+.1f})"
            draw.text((padding, y), profit_text, fill=profit_color, font=font_big)
            y += 38
        else:
            # Lot bilgisi yoksa sadece toplam yuzde
            pct_color = GREEN if total_pct >= 0 else RED
            draw.text((padding, y), f"25. Gün Toplam: %{total_pct:+.1f}",
                      fill=pct_color, font=font_big)
            y += 50

        # ── DIVIDER ────────────────────────────────────
        y = banner_h + header_h - 10
        draw.line([(padding, y), (width - padding, y)], fill=DIVIDER, width=2)
        y += 15

        # ── SUTUN BASLIKLARI ───────────────────────────
        col_x = [padding, 140, 370, 580, 810]  # Gün, Kapanış, Günlük%, Küm%, Durum
        col_labels = ["Gün", "Kapanış", "Günlük %", "Kümülatif %", "Durum"]

        for i, label in enumerate(col_labels):
            draw.text((col_x[i], y), label, fill=GOLD, font=font_col_header)
        y += col_header_h

        # ── TABLO SATIRLARI ───────────────────────────
        for idx, d in enumerate(days_data):
            day_num = d["trading_day"]
            day_close = float(d["close"])
            cum_pct = ((day_close - ipo_price) / ipo_price) * 100

            # Gunluk degisim hesapla
            if idx == 0:
                daily_pct = ((day_close - ipo_price) / ipo_price) * 100
            else:
                prev_close = float(days_data[idx - 1]["close"])
                if prev_close > 0:
                    daily_pct = ((day_close - prev_close) / prev_close) * 100
                else:
                    daily_pct = 0

            # Satir arka plani
            row_y = y + (idx * row_h)
            row_bg = ROW_EVEN if idx % 2 == 0 else ROW_ODD
            draw.rectangle(
                [(0, row_y), (width, row_y + row_h)],
                fill=row_bg,
            )

            # Renk secimi
            daily_color = GREEN if daily_pct >= 0 else RED
            cum_color = GREEN if cum_pct >= 0 else RED

            text_y = row_y + 8

            # Gun numarasi
            draw.text((col_x[0] + 15, text_y), f"{day_num}", fill=WHITE, font=font_row)

            # Kapanis fiyati
            draw.text((col_x[1], text_y), f"{day_close:.2f} TL", fill=WHITE, font=font_row)

            # Gunluk %
            daily_str = f"%{daily_pct:+.1f}"
            draw.text((col_x[2], text_y), daily_str, fill=daily_color, font=font_row)

            # Kumulatif %
            cum_str = f"%{cum_pct:+.1f}"
            draw.text((col_x[3], text_y), cum_str, fill=cum_color, font=font_row_bold)

            # Durum (TAVAN, ALICILI, SATICILI, TABAN)
            durum_raw = d.get("durum", "")
            durum_label_map = {
                "tavan": "TAVAN",
                "alici_kapatti": "ALICILI",
                "satici_kapatti": "SATICILI",
                "taban": "TABAN",
                "not_kapatti": "NORMAL",
            }
            durum_color_map = {
                "tavan": GREEN,
                "alici_kapatti": GREEN,
                "satici_kapatti": RED,
                "taban": RED,
                "not_kapatti": ORANGE,
            }
            durum_label = durum_label_map.get(durum_raw, "")
            durum_color = durum_color_map.get(durum_raw, GRAY)
            if durum_label:
                draw.text((col_x[4], text_y), durum_label, fill=durum_color, font=font_row_bold)

        # ── FOOTER (sadece szalgo.net.tr) ─────────────
        footer_y = banner_h + header_h + table_h + 15

        draw.line([(padding, footer_y - 5), (width - padding, footer_y - 5)],
                  fill=DIVIDER, width=2)

        draw.text((padding, footer_y + 10), "szalgo.net.tr", fill=ORANGE, font=font_watermark)

        # ── KAYDET ─────────────────────────────────────
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{ticker}_25day_{ts}.png"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)

        file_size = os.path.getsize(filepath)
        logger.info("25 gun karne gorseli olusturuldu: %s (%d KB)", filepath, file_size // 1024)
        return filepath

    except Exception as e:
        logger.error("generate_25day_image hatasi: %s", e, exc_info=True)
        return None


# ================================================================
# GUNLUK TAKIP GORSELI (6-24. gun)
# ================================================================

_IMG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")


def generate_daily_tracking_image(
    ipo,
    days_data: list,
    ceiling_days: int,
    floor_days: int,
    trading_day: int,
) -> Optional[str]:
    """Gunluk takip gorseli olusturur (6-24. gun arasi).

    Args:
        ipo: IPO model objesi (ticker, company_name, ipo_price)
        days_data: Gun verisi listesi [{trading_day, close, durum, ...}]
        ceiling_days: Tavan kapanan gun sayisi
        floor_days: Taban kapanan gun sayisi
        trading_day: Mevcut islem gunu (6-24)

    Returns:
        Olusturulan PNG dosyasinin yolu veya None
    """
    try:
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0
        ticker = ipo.ticker or "BILINMIYOR"
        if not days_data or ipo_price <= 0:
            logger.warning("generate_daily_tracking_image: veri eksik (days_data=%s, ipo_price=%s)",
                           len(days_data) if days_data else 0, ipo_price)
            return None

        last_close = float(days_data[-1]["close"])
        total_pct = ((last_close - ipo_price) / ipo_price) * 100
        normal_days = len(days_data) - ceiling_days - floor_days

        # ── Banner yukle ─────────────────────────────────
        banner_h = 0
        banner_img = None
        banner_path = os.path.join(_IMG_DIR, "gunluk_takip_gorsel_banner.png")
        if os.path.exists(banner_path):
            try:
                banner_img = Image.open(banner_path).convert("RGB")
                banner_ratio = banner_img.width / banner_img.height
                banner_h = int(1200 / banner_ratio)
                if banner_h > 430:
                    banner_h = 430
                banner_img = banner_img.resize((1200, banner_h), Image.LANCZOS)
            except Exception as be:
                logger.warning("Gunluk takip banner yuklenemedi: %s", be)
                banner_img = None
                banner_h = 0

        # ── Boyut hesapla ────────────────────────────────
        width = 1200
        header_h = 230       # lot/kar yok, biraz bosluk
        row_h = 44
        col_header_h = 50
        footer_h = 70
        padding = 40
        num_rows = len(days_data)
        table_h = col_header_h + (num_rows * row_h)
        total_h = banner_h + header_h + table_h + footer_h

        img = Image.new("RGB", (width, total_h), BG_COLOR)

        # Banner'i uste yapistir
        if banner_img:
            img.paste(banner_img, (0, 0))

        # Watermark
        _draw_bg_watermark(img, width, total_h)

        draw = ImageDraw.Draw(img)

        # Fontlar
        font_title = _load_font(38, bold=True)
        font_subtitle = _load_font(28)
        font_row = _load_font(26)
        font_row_bold = _load_font(26, bold=True)
        font_col_header = _load_font(24, bold=True)
        font_footer = _load_font(28, bold=True)
        font_footer_sm = _load_font(24)
        font_watermark = _load_font(22)

        y = banner_h + padding

        # ── HEADER ───────────────────────────────────────
        title = f"{ticker} — {trading_day}/25 Gün Sonu"
        draw.text((padding, y), title, fill=WHITE, font=font_title)
        y += 48

        # Sirket adi
        company = ipo.company_name or ""
        if company and company != ticker:
            draw.text((padding, y), company, fill=GRAY, font=font_subtitle)
            y += 36

        # Halka arz fiyati
        draw.text((padding, y), f"Halka Arz Fiyatı: {ipo_price:.2f} TL",
                  fill=GRAY, font=font_subtitle)
        y += 40

        # Kumulatif toplam
        pct_color = GREEN if total_pct >= 0 else RED
        draw.text((padding, y), f"Kümülatif: %{total_pct:+.1f}",
                  fill=pct_color, font=_load_font(34, bold=True))
        y += 50

        # ── DIVIDER ──────────────────────────────────────
        y = banner_h + header_h - 10
        draw.line([(padding, y), (width - padding, y)], fill=DIVIDER, width=2)
        y += 15

        # ── SUTUN BASLIKLARI ─────────────────────────────
        col_x = [padding, 140, 370, 580, 810]
        col_labels = ["Gün", "Kapanış", "Günlük %", "Kümülatif %", "Durum"]

        for i, label in enumerate(col_labels):
            draw.text((col_x[i], y), label, fill=GOLD, font=font_col_header)
        y += col_header_h

        # ── TABLO SATIRLARI ──────────────────────────────
        for idx, d in enumerate(days_data):
            day_num = d["trading_day"]
            day_close = float(d["close"])
            cum_pct = ((day_close - ipo_price) / ipo_price) * 100

            if idx == 0:
                daily_pct = ((day_close - ipo_price) / ipo_price) * 100
            else:
                prev_close = float(days_data[idx - 1]["close"])
                if prev_close > 0:
                    daily_pct = ((day_close - prev_close) / prev_close) * 100
                else:
                    daily_pct = 0

            row_y = y + (idx * row_h)
            row_bg = ROW_EVEN if idx % 2 == 0 else ROW_ODD
            draw.rectangle([(0, row_y), (width, row_y + row_h)], fill=row_bg)

            daily_color = GREEN if daily_pct >= 0 else RED
            cum_color = GREEN if cum_pct >= 0 else RED

            text_y = row_y + 8
            draw.text((col_x[0] + 15, text_y), f"{day_num}", fill=WHITE, font=font_row)
            draw.text((col_x[1], text_y), f"{day_close:.2f} TL", fill=WHITE, font=font_row)
            draw.text((col_x[2], text_y), f"%{daily_pct:+.1f}", fill=daily_color, font=font_row)
            draw.text((col_x[3], text_y), f"%{cum_pct:+.1f}", fill=cum_color, font=font_row_bold)

            # Durum
            durum_raw = d.get("durum", "")
            durum_label_map = {
                "tavan": "TAVAN", "alici_kapatti": "ALICILI",
                "satici_kapatti": "SATICILI", "taban": "TABAN", "not_kapatti": "NORMAL",
            }
            durum_color_map = {
                "tavan": GREEN, "alici_kapatti": GREEN,
                "satici_kapatti": RED, "taban": RED, "not_kapatti": ORANGE,
            }
            durum_label = durum_label_map.get(durum_raw, "")
            durum_color = durum_color_map.get(durum_raw, GRAY)
            if durum_label:
                draw.text((col_x[4], text_y), durum_label, fill=durum_color, font=font_row_bold)

        # ── FOOTER (sadece szalgo.net.tr) ────────────────
        footer_y = banner_h + header_h + table_h + 15

        draw.line([(padding, footer_y - 5), (width - padding, footer_y - 5)],
                  fill=DIVIDER, width=2)

        draw.text((padding, footer_y + 10), "szalgo.net.tr", fill=ORANGE, font=font_watermark)

        # ── KAYDET ───────────────────────────────────────
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{ticker}_{trading_day}day_{ts}.png"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)

        file_size = os.path.getsize(filepath)
        logger.info("Gunluk takip gorseli olusturuldu: %s (%d KB) — %d. gun",
                     filepath, file_size // 1024, trading_day)
        return filepath

    except Exception as e:
        logger.error("generate_daily_tracking_image hatasi: %s", e, exc_info=True)
        return None


def _overlay_day_number(img: Image.Image, day_num: int, banner_h: int):
    """Banner uzerindeki 'X/25' badge alanina gercek gun numarasini yazar."""
    if banner_h <= 0:
        return
    try:
        draw = ImageDraw.Draw(img)
        font = _load_font(32, bold=True)
        text = f"{day_num}/25"
        # Banner'daki badge konumu — sagda, ustte (banner 1200x~427)
        # Gemini banner'daki "GUN X/25" badge'i ~sag ust kosede
        badge_x = 600  # ~orta
        badge_y = 18    # ustten
        # Arka plan kutusu
        bbox = font.getbbox(text)
        tw = bbox[2] - bbox[0]
        th = bbox[3] - bbox[1]
        box_x = badge_x - 8
        box_y = badge_y - 4
        draw.rounded_rectangle(
            [(box_x, box_y), (box_x + tw + 16, box_y + th + 8)],
            radius=8,
            fill=(40, 50, 60, 220),
        )
        draw.text((badge_x, badge_y), text, fill=WHITE, font=font)
    except Exception as e:
        logger.warning("Gun numarasi overlay eklenemedi: %s", e)
