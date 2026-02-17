"""25 gunluk karne gorseli olusturucu.

Pillow ile koyu arka planli, renkli satirli PNG tablo olusturur.
Tweet'e resim olarak eklenir.
"""

import logging
import os
import tempfile
from datetime import datetime
from decimal import Decimal
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

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
            lot_profit = (last_close - ipo_price) * 100 * lot_count  # 1 lot = 100 hisse

        # ── Boyut hesapla ──────────────────────────────
        width = 1200
        header_h = 280       # ust bilgi alani
        row_h = 44            # her satir yuksekligi
        col_header_h = 50     # sutun baslik satiri
        footer_h = 180        # alt ozet alani
        padding = 40
        num_rows = len(days_data)
        table_h = col_header_h + (num_rows * row_h)
        total_h = header_h + table_h + footer_h

        img = Image.new("RGB", (width, total_h), BG_COLOR)
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

        y = padding

        # ── HEADER ─────────────────────────────────────
        # Baslik
        title = f"{ticker} — 25 Günü Bitirdi"
        draw.text((padding, y), title, fill=WHITE, font=font_title)
        y += 52

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
            y += 50
        else:
            # Lot bilgisi yoksa sadece toplam yuzde
            pct_color = GREEN if total_pct >= 0 else RED
            draw.text((padding, y), f"25. Gün Toplam: %{total_pct:+.1f}",
                      fill=pct_color, font=font_big)
            y += 50

        # ── DIVIDER ────────────────────────────────────
        y = header_h - 10
        draw.line([(padding, y), (width - padding, y)], fill=DIVIDER, width=2)
        y += 15

        # ── SUTUN BASLIKLARI ───────────────────────────
        col_x = [padding, 140, 380, 600, 860]  # Gün, Kapanış, Günlük%, Küm%, Durum (opsiyonel)
        col_labels = ["Gün", "Kapanış", "Günlük %", "Kümülatif %"]

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

        # ── FOOTER ─────────────────────────────────────
        footer_y = header_h + table_h + 15

        # Divider
        draw.line([(padding, footer_y - 5), (width - padding, footer_y - 5)],
                  fill=DIVIDER, width=2)

        # Toplam
        total_color = GREEN if total_pct >= 0 else RED
        draw.text((padding, footer_y + 5),
                  f"Toplam: %{total_pct:+.1f}", fill=total_color, font=font_footer)

        # Tavan / Taban / Normal
        summary_y = footer_y + 45
        summary_text = f"Tavan: {ceiling_days}  |  Taban: {floor_days}  |  Normal: {normal_days}"
        draw.text((padding, summary_y), summary_text, fill=GRAY, font=font_footer_sm)

        # Watermark
        wm_y = summary_y + 50
        draw.text((padding, wm_y), "szalgo.net.tr", fill=ORANGE, font=font_watermark)

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
