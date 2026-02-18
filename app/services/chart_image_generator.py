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
        row_h = 44            # her satir yuksekligi
        col_header_h = 50     # sutun baslik satiri
        footer_h = 120        # alt — toplam + tavan/taban/normal + szalgo
        padding = 40
        num_rows = len(days_data)

        # Header yuksekligini icerigi hesaplayarak belirle
        # Baslik(48) + sirket(36) + fiyat(40) + lot/toplam(~50) + bosluk(30)
        if lot_count > 0:
            header_h = 48 + 36 + 40 + 40 + 38 + 30  # lot + kar bilgisi
        else:
            header_h = 48 + 36 + 40 + 50 + 30        # sadece toplam yuzde

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
                "not_kapatti": "Normal İşlem",
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

        # ── FOOTER ────────────────────────────────────
        footer_y = banner_h + header_h + table_h + 15

        draw.line([(padding, footer_y - 5), (width - padding, footer_y - 5)],
                  fill=DIVIDER, width=2)

        # Toplam yuzde
        pct_color = GREEN if total_pct >= 0 else RED
        draw.text((padding, footer_y + 5), f"Toplam: %{total_pct:+.1f}",
                  fill=pct_color, font=font_footer)

        # Tavan / Taban / Normal İşlem
        draw.text((padding, footer_y + 38),
                  f"Tavan: {ceiling_days}  |  Taban: {floor_days}  |  Normal İşlem: {normal_days}",
                  fill=GRAY, font=font_footer_sm)

        # szalgo.net.tr
        draw.text((padding, footer_y + 70), "szalgo.net.tr", fill=ORANGE, font=font_watermark)

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


# ═══════════════════════════════════════════════════════════════
# OGLE ARASI MARKET SNAPSHOT (14:00 — tum islem goren hisseler)
# ═══════════════════════════════════════════════════════════════

def _format_lot(lot_val) -> str:
    """Lot sayisini binlik ayiracli formata cevirir. 0 veya None → '—'."""
    if not lot_val or lot_val == 0:
        return "—"
    return f"{int(lot_val):,}".replace(",", ".")


def generate_market_snapshot_image(snapshot_data: list) -> Optional[str]:
    """Ogle arasi market snapshot gorseli olusturur — kart (card) bazli layout.

    Args:
        snapshot_data: Her hisse icin dict listesi:
            [
                {
                    "ticker": "AKHAN",
                    "trading_day": 3,
                    "close_price": 23.65,
                    "pct_change": 10.0,     # gunluk %
                    "cum_pct": 45.2,        # kumulatif % (HA fiyatindan)
                    "durum": "tavan",        # tavan/taban/alici_kapatti/satici_kapatti/not_kapatti
                    "alis_lot": 1245000,
                    "satis_lot": 0,
                    "ipo_price": 21.50,
                }
            ]

    Returns:
        PNG dosya yolu (tempdir) veya None
    """
    try:
        if not snapshot_data:
            return None

        width = 1200
        padding = 40

        # Fontlar
        font_title = _load_font(36, bold=True)
        font_subtitle = _load_font(24, bold=False)
        font_ticker = _load_font(32, bold=True)
        font_data = _load_font(24, bold=False)
        font_data_bold = _load_font(24, bold=True)
        font_lot = _load_font(20, bold=False)
        font_cum_val = _load_font(16, bold=True)   # G. Toplam yuzde degeri (kucuk)
        font_footer = _load_font(22, bold=False)
        font_footer_bold = _load_font(24, bold=True)

        # Banner yukleme
        _IMG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
        banner_path = os.path.join(_IMG_DIR, "ogle_arasi_banner.png")
        banner_img = None
        banner_h = 0

        if os.path.exists(banner_path):
            try:
                raw_banner = Image.open(banner_path).convert("RGB")
                # Banner'i 1200 genislige scale et (orantili)
                scale = width / raw_banner.width
                new_h = int(raw_banner.height * scale)
                banner_img = raw_banner.resize((width, new_h), Image.LANCZOS)
                banner_h = new_h
            except Exception:
                banner_img = None

        # Layout hesaplamalari
        header_h = 90      # Baslik (42pt GOLD) + tarih
        card_h = 90         # Her kart yuksekligi
        card_gap = 4        # Kartlar arasi bosluk
        footer_h = 45       # Footer (sadece szalgo.net.tr)
        accent_w = 5        # Sol kenar renk cizgisi genisligi

        num_cards = len(snapshot_data)
        cards_total_h = num_cards * card_h + (num_cards - 1) * card_gap
        total_h = banner_h + header_h + cards_total_h + footer_h + 20  # 20 = alt bosluk

        # Gorsel olustur
        img = Image.new("RGB", (width, total_h), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # Banner paste
        y = 0
        if banner_img:
            img.paste(banner_img, (0, 0))
            y = banner_h

        # ── Header ──────────────────────────────────────────
        draw.rectangle([(0, y), (width, y + header_h)], fill=HEADER_BG)
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
        # Baslik — GOLD, buyuk, ozel
        font_brand = _load_font(42, bold=True)
        draw.text((padding, y + 8), "HALKA ARZ HİSSELERİ", fill=GOLD, font=font_brand)
        draw.text((padding, y + 54), f"{now_str}  |  {num_cards} Hisse İşlemde", fill=GRAY, font=font_subtitle)

        # Tavan/taban/normal sayaci — sag tarafa
        tavan_c = sum(1 for s in snapshot_data if s.get("durum") == "tavan")
        taban_c = sum(1 for s in snapshot_data if s.get("durum") == "taban")
        normal_c = num_cards - tavan_c - taban_c
        summary = f"Tavan: {tavan_c}  Taban: {taban_c}  Normal İşlem: {normal_c}"
        bbox = font_subtitle.getbbox(summary)
        sw = bbox[2] - bbox[0]
        draw.text((width - padding - sw, y + 56), summary, fill=GOLD, font=font_subtitle)

        y += header_h

        # Divider
        draw.line([(padding, y), (width - padding, y)], fill=DIVIDER, width=2)
        y += 4

        # ── Kartlar ────────────────────────────────────────
        for idx, stock in enumerate(snapshot_data):
            card_y = y + idx * (card_h + card_gap)

            # Arka plan (alternating)
            card_bg = ROW_EVEN if idx % 2 == 0 else ROW_ODD
            draw.rectangle([(0, card_y), (width, card_y + card_h)], fill=card_bg)

            # Sol kenar renk accent (pozitif=GREEN, negatif=RED)
            pct = float(stock.get("pct_change", 0))
            accent_color = GREEN if pct >= 0 else RED
            draw.rectangle([(0, card_y), (accent_w, card_y + card_h)], fill=accent_color)

            # ─ Satir 1: Ticker | Gun | Fiyat | Gunluk% | Durum ─
            row1_y = card_y + 10

            # Ticker (sol, buyuk bold)
            draw.text((padding, row1_y), stock["ticker"], fill=WHITE, font=font_ticker)

            # X/25 Gun (ticker'dan sonra)
            day_text = f"{stock['trading_day']}/25"
            draw.text((padding + 180, row1_y + 6), day_text, fill=GRAY, font=font_data)

            # Son Fiyat (orta)
            price_text = f"{float(stock['close_price']):.2f} TL"
            draw.text((420, row1_y), price_text, fill=WHITE, font=font_data_bold)

            # Gunluk % (orta-sag)
            pct_color = GREEN if pct >= 0 else RED
            pct_text = f"%{pct:+.2f}"
            draw.text((620, row1_y), pct_text, fill=pct_color, font=font_data_bold)

            # Kumulatif % — "3 G. Toplam: ▲%+42.5" seklinde
            cum_pct = float(stock.get("cum_pct", 0))
            t_day = stock.get("trading_day", 0)
            # Label kismi duz GRAY
            cum_label = f"{t_day} G. Toplam: "
            draw.text((750, row1_y + 6), cum_label, fill=GRAY, font=font_lot)
            # Yuzde kismi renkli (kucuk font, oksuz)
            label_w = font_lot.getbbox(cum_label)[2] - font_lot.getbbox(cum_label)[0]
            cum_color = GREEN if cum_pct >= 0 else RED
            cum_val = f"%{cum_pct:+.1f}"
            draw.text((750 + label_w, row1_y + 8), cum_val, fill=cum_color, font=font_cum_val)

            # Durum badge (sag kenar)
            durum = stock.get("durum", "")
            durum_labels = {
                "tavan": "TAVAN",
                "taban": "TABAN",
                "alici_kapatti": "ALICILI",
                "satici_kapatti": "SATICILI",
                "not_kapatti": "Normal İşlem",
            }
            durum_colors = {
                "tavan": GREEN,
                "taban": RED,
                "alici_kapatti": GREEN,
                "satici_kapatti": RED,
                "not_kapatti": ORANGE,
            }
            d_label = durum_labels.get(durum, durum.upper() if durum else "—")
            d_color = durum_colors.get(durum, GRAY)

            # Badge arka plan kutusu
            font_badge = font_lot if len(d_label) > 8 else font_data_bold  # uzun etiketler icin kucuk font
            d_bbox = font_badge.getbbox(d_label)
            d_w = d_bbox[2] - d_bbox[0]
            badge_x = width - padding - d_w - 16
            badge_y = row1_y - 2
            badge_h = 28 if len(d_label) > 8 else 32
            draw.rectangle(
                [(badge_x, badge_y), (badge_x + d_w + 16, badge_y + badge_h)],
                fill=(d_color[0], d_color[1], d_color[2]),
            )
            # Badge metni (koyu arka plan uzerine siyah)
            draw.text((badge_x + 8, badge_y + 3), d_label, fill=(0, 0, 0), font=font_badge)

            # ─ Satir 2: Lot bilgileri (durum'a gore degisir) ─
            row2_y = card_y + 52

            alis_lot = stock.get("alis_lot")
            satis_lot = stock.get("satis_lot")

            if durum == "tavan":
                # Tavandaysa sadece alis lot goster
                lot_text = f"Tavanda Bekleyen Alış: {_format_lot(alis_lot)}"
                lot_color = GREEN
            elif durum == "taban":
                # Tabandaysa sadece satis lot goster
                lot_text = f"Tabanda Bekleyen Satış: {_format_lot(satis_lot)}"
                lot_color = RED
            else:
                # Normal — her ikisini goster
                lot_text = f"Alış Lot: {_format_lot(alis_lot)}  |  Satış Lot: {_format_lot(satis_lot)}"
                lot_color = GRAY

            draw.text((padding + 180, row2_y), lot_text, fill=lot_color, font=font_lot)

            # HA fiyati (sag alt)
            ipo_price = stock.get("ipo_price")
            if ipo_price:
                ha_text = f"HA: {float(ipo_price):.2f} TL"
                ha_bbox = font_lot.getbbox(ha_text)
                ha_w = ha_bbox[2] - ha_bbox[0]
                draw.text((width - padding - ha_w, row2_y), ha_text, fill=GRAY, font=font_lot)

        # ── Footer ──────────────────────────────────────────
        footer_y = y + cards_total_h + 10
        draw.line([(padding, footer_y), (width - padding, footer_y)], fill=DIVIDER, width=2)
        footer_y += 8
        draw.rectangle([(0, footer_y), (width, footer_y + footer_h)], fill=HEADER_BG)

        # szalgo.net.tr (orta)
        footer_text = "szalgo.net.tr"
        ft_bbox = font_footer_bold.getbbox(footer_text)
        ft_w = ft_bbox[2] - ft_bbox[0]
        draw.text(((width - ft_w) // 2, footer_y + 8), footer_text, fill=ORANGE, font=font_footer_bold)

        # Watermark
        _draw_bg_watermark(img, width, total_h)

        # Kaydet
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"market_snapshot_{ts}.png"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)
        logger.info("Market snapshot gorsel olusturuldu: %s (%d hisse)", filepath, num_cards)
        return filepath

    except Exception as e:
        logger.error("Market snapshot gorsel hatasi: %s", e)
        return None


# ══════════════════════════════════════════════════════════════
# T16 — Yeni Halka Arzlar Ilk 5 Gun Acilis Bilgileri (09:57)
# ══════════════════════════════════════════════════════════════

# Ek renkler
LIGHT_BLUE = (96, 165, 250)    # #60a5fa
DARK_CARD_BG = (20, 20, 40)   # koyu kart ici


def generate_opening_summary_image(stocks: list) -> Optional[str]:
    """Ilk 5 gun icindeki hisselerin acilis bilgilerini sutunlu yatay gorsel olarak olusturur.

    Args:
        stocks: Her hisse icin dict:
            [
                {
                    "ticker": "ASELS",
                    "company_name": "Aselsan A.Ş.",
                    "trading_day": 3,
                    "ipo_price": 38.00,
                    "open_price": 42.50,
                    "pct_change": +11.8,         # acilis vs HA fiyat %
                    "durum": "tavan",             # bugunun acilis durumu
                    "ceiling_days": 2,
                    "floor_days": 0,
                    "normal_days": 1,
                }
            ]

    Returns:
        PNG dosya yolu veya None
    """
    try:
        if not stocks:
            return None

        num_cols = len(stocks)
        if num_cols > 8:
            stocks = stocks[:8]  # max 8 sutun
            num_cols = 8

        # ── Boyutlar ────────────────────────────
        col_width = 200
        min_width = 1200
        width = max(min_width, num_cols * col_width + 80)
        padding = 40

        header_h = 90
        col_header_h = 50
        data_h = 220
        footer_h = 50
        total_h = header_h + col_header_h + data_h + footer_h + 20

        # ── Fontlar ─────────────────────────────
        font_title = _load_font(22, bold=True)
        font_sub = _load_font(14)
        font_ticker = _load_font(20, bold=True)
        font_day = _load_font(13)
        font_data = _load_font(15, bold=True)
        font_label = _load_font(11)
        font_small = _load_font(12)
        font_footer = _load_font(14, bold=True)

        # ── Canvas ──────────────────────────────
        img = Image.new("RGB", (width, total_h), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # ── Header ──────────────────────────────
        draw.rectangle([(0, 0), (width, header_h)], fill=HEADER_BG)

        title_text = "📊  YENİ HALKA ARZLAR — AÇILIŞ BİLGİLERİ"
        draw.text((padding, 18), title_text, fill=GOLD, font=font_title)

        date_text = datetime.now().strftime("%d.%m.%Y")
        draw.text((padding, 52), date_text, fill=GRAY, font=font_sub)

        count_text = f"{num_cols} Hisse  |  İlk 5 İşlem Günü"
        ct_bbox = font_sub.getbbox(count_text)
        ct_w = ct_bbox[2] - ct_bbox[0]
        draw.text((width - padding - ct_w, 52), count_text, fill=GRAY, font=font_sub)

        # ── Sütun hesaplama ─────────────────────
        usable_w = width - 2 * padding
        actual_col_w = usable_w // num_cols
        col_start_x = padding

        # ── Sütun Başlıkları ────────────────────
        sh_y = header_h + 5
        for i, stock in enumerate(stocks):
            cx = col_start_x + i * actual_col_w
            mid_x = cx + actual_col_w // 2

            # Ticker
            ticker_text = f"#{stock['ticker']}"
            tb = font_ticker.getbbox(ticker_text)
            tw = tb[2] - tb[0]
            draw.text((mid_x - tw // 2, sh_y + 5), ticker_text, fill=WHITE, font=font_ticker)

            # Gün
            day_text = f"{stock['trading_day']}. Gün"
            db = font_day.getbbox(day_text)
            dw = db[2] - db[0]
            draw.text((mid_x - dw // 2, sh_y + 30), day_text, fill=GRAY, font=font_day)

            # Sütun ayırıcı (son hariç)
            if i < num_cols - 1:
                sep_x = cx + actual_col_w
                draw.line([(sep_x, sh_y), (sep_x, sh_y + col_header_h + data_h)],
                          fill=DIVIDER, width=1)

        # Yatay ayırıcı
        divider_y = sh_y + col_header_h
        draw.line([(padding, divider_y), (width - padding, divider_y)],
                  fill=DIVIDER, width=2)

        # ── Veri Satırları ──────────────────────
        data_y = divider_y + 10

        for i, stock in enumerate(stocks):
            cx = col_start_x + i * actual_col_w
            mid_x = cx + actual_col_w // 2

            pct = float(stock.get("pct_change", 0))
            durum = stock.get("durum", "")

            # HA Fiyat
            ipo_price = float(stock.get("ipo_price", 0))
            _draw_centered(draw, mid_x, data_y, "HA Fiyat", font_label, GRAY)
            _draw_centered(draw, mid_x, data_y + 14, f"{ipo_price:.2f} TL",
                           font_data, LIGHT_BLUE)

            # Açılış Fiyat
            open_price = float(stock.get("open_price", 0))
            _draw_centered(draw, mid_x, data_y + 42, "Açılış", font_label, GRAY)
            price_color = GREEN if pct >= 0 else RED
            _draw_centered(draw, mid_x, data_y + 56, f"{open_price:.2f} TL",
                           font_data, price_color)

            # Yüzde Değişim
            pct_text = f"%{pct:+.1f}"
            _draw_centered(draw, mid_x, data_y + 84, pct_text, font_data, price_color)

            # Durum etiketi
            durum_text, durum_color = _durum_label(durum)
            _draw_centered(draw, mid_x, data_y + 108, durum_text, font_small, durum_color)

            # Ayırıcı çizgi
            sep_y2 = data_y + 130
            draw.line([(cx + 10, sep_y2), (cx + actual_col_w - 10, sep_y2)],
                      fill=DIVIDER, width=1)

            # Tavan / Taban / Normal (önceki günlerden)
            ceiling_d = stock.get("ceiling_days", 0)
            floor_d = stock.get("floor_days", 0)
            normal_d = stock.get("normal_days", 0)

            stats_y = sep_y2 + 8
            _draw_centered(draw, mid_x, stats_y,
                           f"T:{ceiling_d}  Tb:{floor_d}  N:{normal_d}",
                           font_small, GRAY)

            # Küçük açıklama
            _draw_centered(draw, mid_x, stats_y + 18,
                           "Tavan|Taban|Normal",
                           font_label, (80, 80, 100))

        # ── Footer ──────────────────────────────
        footer_y = total_h - footer_h
        draw.line([(padding, footer_y - 5), (width - padding, footer_y - 5)],
                  fill=DIVIDER, width=2)
        draw.rectangle([(0, footer_y), (width, total_h)], fill=HEADER_BG)

        # Disclaimer sol
        disc_text = "⚠ YZ destekli bildirimdir"
        draw.text((padding, footer_y + 15), disc_text, fill=GRAY, font=font_small)

        # szalgo.net.tr orta
        site_text = "szalgo.net.tr"
        sb = font_footer.getbbox(site_text)
        sw = sb[2] - sb[0]
        draw.text(((width - sw) // 2, footer_y + 13), site_text,
                  fill=ORANGE, font=font_footer)

        # Watermark
        _draw_bg_watermark(img, width, total_h)

        # Kaydet
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"opening_summary_{ts}.png"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)
        logger.info("Opening summary gorsel olusturuldu: %s (%d hisse)", filepath, num_cols)
        return filepath

    except Exception as e:
        logger.error("Opening summary gorsel hatasi: %s", e)
        return None


def _draw_centered(draw, center_x: int, y: int, text: str,
                   font, color: tuple):
    """Metni verilen center_x'e göre ortalar."""
    bbox = font.getbbox(text)
    tw = bbox[2] - bbox[0]
    draw.text((center_x - tw // 2, y), text, fill=color, font=font)


def _durum_label(durum: str) -> tuple:
    """Durum kodundan okunabilir etiket + renk doner."""
    mapping = {
        "tavan": ("🟢 TAVAN", GREEN),
        "alici_kapatti": ("🟢 ALICI", GREEN),
        "not_kapatti": ("🟡 NÖTR", GOLD),
        "satici_kapatti": ("🔴 SATICI", RED),
        "taban": ("🔴 TABAN", RED),
    }
    return mapping.get(durum, ("—", GRAY))
