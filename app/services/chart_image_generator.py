"""Halka arz performans gorsel olusturucu.

Pillow ile koyu arka planli, renkli satirli PNG tablo olusturur.
Tweet'e resim olarak eklenir.
- 25 gunluk karne gorseli (generate_25day_image)
- Gunluk takip gorseli (generate_daily_tracking_image) — 6+ gun
"""

import gc
import logging
import os
import tempfile
from datetime import datetime, timezone, timedelta
from decimal import Decimal

# Turkiye saat dilimi (UTC+3)
_TR_TZ = timezone(timedelta(hours=3))
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

# Durum renkleri — tavan/taban belirgin, alicili/saticili silik
TAVAN_GREEN = (0, 230, 64)       # #00e640 parlak / koyu yesil — tavan
MUTED_GREEN = (74, 140, 96)      # #4a8c60 silik/soluk yesil — alicili
MUTED_RED = (160, 80, 80)        # #a05050 silik/soluk kirmizi — saticili
TABAN_RED = (255, 40, 40)        # #ff2828 parlak / koyu kirmizi — taban

# ── Font ───────────────────────────────────────────────────
# Render (Linux) DejaVu fontlari mevcut
_FONT_PATHS = [
    # Sans-serif öncelikli (Türkçe karakter desteği + okunabilirlik)
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
    # Windows fallback
    "C:/Windows/Fonts/segoeui.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "C:/Windows/Fonts/consola.ttf",
]

_BOLD_FONT_PATHS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf",
    "C:/Windows/Fonts/segoeuib.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
    "C:/Windows/Fonts/consolab.ttf",
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
        # Lot: aralık formatı (8.5 → "8-9"), kar hesabı floor değerle
        _lot_val = float(avg_lot) if avg_lot else 0
        lot_count = int(_lot_val) if _lot_val > 0 else 0
        if _lot_val > 0 and _lot_val == int(_lot_val):
            lot_display = str(int(_lot_val))
        elif _lot_val > 0:
            lot_display = f"{int(_lot_val)}-{int(_lot_val)+1}"
        else:
            lot_display = ""
        lot_profit = 0.0
        if lot_count > 0:
            lot_profit = (last_close - ipo_price) * lot_count  # lot = adet (floor)

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
            header_h = 48 + 36 + 40 + 40 + 55 + 40  # lot + kar bilgisi (259px)
        else:
            header_h = 48 + 36 + 40 + 55 + 40        # sadece toplam yuzde (219px)

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
            draw.text((padding, y), f"Kişi Başı Ort Lot: {lot_display}",
                      fill=GRAY, font=font_subtitle)
            y += 40

            # Kar/zarar
            profit_color = GREEN if lot_profit >= 0 else RED
            if lot_profit >= 0:
                profit_text = f"25. Gün Karnesi: +{lot_profit:,.0f} TL (%{total_pct:+.1f})"
            else:
                profit_text = f"25. Gün Karnesi: {lot_profit:,.0f} TL (%{total_pct:+.1f})"
            draw.text((padding, y), profit_text, fill=profit_color, font=font_big)
            y += 55
        else:
            # Lot bilgisi yoksa sadece toplam yuzde
            pct_color = GREEN if total_pct >= 0 else RED
            draw.text((padding, y), f"25. Gün Toplam: %{total_pct:+.1f}",
                      fill=pct_color, font=font_big)
            y += 55

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
                "tavan": TAVAN_GREEN,
                "alici_kapatti": MUTED_GREEN,
                "satici_kapatti": MUTED_RED,
                "taban": TABAN_RED,
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
        del img, draw; gc.collect()

        file_size = os.path.getsize(filepath)
        logger.info("25 gun karne gorseli olusturuldu: %s (%d KB)", filepath, file_size // 1024)
        return filepath

    except Exception as e:
        logger.error("generate_25day_image hatasi: %s", e, exc_info=True)
        return None


# ================================================================
# TAVAN / TABAN YAPAY ZEKA GORSELI
# ================================================================
import textwrap

def generate_ceiling_floor_images(stats: list, is_ceiling: bool, supplementary: list = None) -> list[str]:
    """Tavan veya taban yapan hisseler icin listeyi gorsele aktarir (sayfali).
    
    Args:
        stats: Ana tavan/taban hisse listesi
        is_ceiling: Tavan mi taban mi
        supplementary: Ek hisseler (tavan/taban olmayan en çok artanlar/azalanlar) — liste <5 ise kullanılır
    """
    # None → boş liste güvenliği
    if stats is None:
        stats = []
    # Aşırı yüksek fiyatlı hisseleri filtrele (lot bölünmesi yapmamış, tabloda anlamsız)
    _MAX_PRICE = 25000
    stats = [s for s in stats if getattr(s, 'close_price', 0) < _MAX_PRICE]
    if supplementary:
        supplementary = [s for s in supplementary if getattr(s, 'close_price', 0) < _MAX_PRICE]
    # 0 hisse + supplementary yoksa boş dön; supplementary varsa devam et
    if not stats and not supplementary:
        return []

    banner_name = "tavan_banner.png" if is_ceiling else "taban_banner.png"
    banner_path = os.path.join(_IMG_DIR, banner_name)
    banner_img = None
    banner_h = 0
    width = 1200

    if os.path.exists(banner_path):
        try:
            raw_banner = Image.open(banner_path).convert("RGB")
            scale = width / raw_banner.width
            banner_h = int(raw_banner.height * scale)
            banner_img = raw_banner.resize((width, banner_h), Image.LANCZOS)
        except Exception as e:
            logger.warning(f"Banner error: {e}")

    # Ek hisseleri: ana liste ≤6 ise ekle (en az 2 ek hisse), 0 ise 8 ek hisse
    show_supplementary = bool(supplementary) and len(stats) <= 6
    supp_to_show = []
    if show_supplementary:
        need = max(8 - len(stats), 2)
        supp_to_show = supplementary[:need]

    max_per_page = 15
    import math

    # 0 hisse durumu — boş sayfa + supplementary
    if len(stats) == 0:
        pages = [[]]
        max_rows_in_any_page = 0
    elif len(stats) <= max_per_page:
        pages = [stats]
        max_rows_in_any_page = len(stats)
    else:
        num_pages = math.ceil(len(stats) / max_per_page)
        per_page = math.ceil(len(stats) / num_pages)
        pages = [stats[i:i + per_page] for i in range(0, len(stats), per_page)]
        max_rows_in_any_page = max(len(p) for p in pages)
    image_paths = []

    font_title = _load_font(34, bold=True)
    font_row = _load_font(24)
    font_change = _load_font(22)              # Değişim sütunu — biraz küçük, çakışma önlenir
    font_symbol = _load_font(24, bold=True)   # Ticker — küçültüldü, fiyatla çakışmasın
    font_reason = _load_font(15, bold=False)
    font_col = _load_font(22, bold=True)
    font_seri_bold = _load_font(20, bold=True)
    font_footer_brand = _load_font(16, bold=True)
    font_footer_disclaimer = _load_font(14, bold=False)
    # Ek hisseler icin %25 daha kucuk fontlar
    font_supp_symbol = _load_font(24, bold=True)
    font_supp_row = _load_font(21)
    font_supp_title = _load_font(28, bold=True)

    CYAN = (34, 211, 238)

    for page_idx, page_stats in enumerate(pages):
        row_h = 106
        supp_row_h = 68  # Ek hisseler icin daha kucuk satir
        header_h = 105  # title(50) + divider(15) + col_headers(40)
        # Tüm sayfalar aynı yükseklik — en kalabalık sayfanın satır sayısını kullan
        table_h = max_rows_in_any_page * row_h
        # 0 hisse → "hisse yok" mesajı alanı (80px)
        if max_rows_in_any_page == 0:
            table_h = 80
        padding = 40
        footer_h = 80  # 2 satir footer icin daha yuksek

        # Ek hisseler icin ek alan (sadece son sayfada)
        supp_section_h = 0
        is_last_page = page_idx == len(pages) - 1
        if show_supplementary and is_last_page and supp_to_show:
            import math
            # 0 hisse → 4 kolon, normal → 3 kolon
            _supp_cols = 4 if max_rows_in_any_page == 0 else 3
            supp_grid_rows = math.ceil(len(supp_to_show) / _supp_cols)
            supp_section_h = 15 + 65 + supp_grid_rows * supp_row_h + 20  # gap + baslik + grid + alt bosluk
        
        total_h = banner_h + padding + header_h + table_h + supp_section_h + footer_h

        img = Image.new("RGB", (width, total_h), BG_COLOR)
        draw = ImageDraw.Draw(img)

        if banner_img:
            img.paste(banner_img, (0, 0))

        _draw_bg_watermark(img, width, total_h)

        y = banner_h + padding

        page_text = f" (Sayfa {page_idx+1}/{len(pages)})" if len(pages) > 1 else ""
        title = f"Günün {'Tavan' if is_ceiling else 'Taban'} Yapan Hisseleri{page_text}"
        draw.text((padding, y), title, fill=GOLD, font=font_title)
        y += 50

        draw.line([(padding, y), (width - padding, y)], fill=DIVIDER, width=2)
        y += 15

        # 0: Hisse, 1: Fiyat, 2: Değişim, 3: Seri, 4: 30 Gün, 5: Not
        # Sütunlar arası boşluk artırıldı — her kolon rahat, çakışma yok
        col_x = [padding, 175, 320, 440, 550, 680]
        draw.text((col_x[0], y), "Hisse", fill=GRAY, font=font_col)
        draw.text((col_x[1], y), "Fiyat", fill=GRAY, font=font_col)
        draw.text((col_x[2], y), "Değişim", fill=GRAY, font=font_col)
        draw.text((col_x[3], y), "Seri", fill=GRAY, font=font_col)
        draw.text((col_x[4], y), "Son 30G", fill=GRAY, font=font_col)
        neden_header = "Neden Yükseldi (AI)" if is_ceiling else "Neden Düştü (AI)"
        draw.text((col_x[5], y), neden_header, fill=CYAN, font=font_col)
        y += 40

        # 0 hisse durumu — bilgi mesajı göster
        if len(page_stats) == 0:
            no_stock_msg = f"Bugün {'tavan' if is_ceiling else 'taban'} yapan hisse yok."
            draw.text((padding + 20, y + 30), no_stock_msg, fill=GRAY, font=font_title)

        for idx, stat in enumerate(page_stats):
            row_y = y + (idx * row_h)
            row_bg = ROW_EVEN if idx % 2 == 0 else ROW_ODD
            draw.rectangle([(0, row_y), (width, row_y + row_h)], fill=row_bg)

            text_y = row_y + 33

            # Ticker
            draw.text((col_x[0], text_y), stat.ticker, fill=WHITE, font=font_symbol)

            # Price
            color = GREEN if is_ceiling else RED
            draw.text((col_x[1], text_y), f"{stat.close_price:.2f} ₺", fill=color, font=font_row)

            # Percent Change (%) — font_change (24px) ile çakışma önlenir
            pct = getattr(stat, "percent_change", 10.0) # Fallback to 10.0
            draw.text((col_x[2], text_y), f"% {pct:+.2f}", fill=color, font=font_change)

            # Seri — gold+bold if ≥2
            consec = stat.consecutive_ceiling_count if is_ceiling else stat.consecutive_floor_count
            seri_yazi = f"{consec}. Gün"
            seri_color = GOLD if consec >= 2 else WHITE
            seri_font = font_seri_bold if consec >= 2 else font_reason
            draw.text((col_x[3], text_y), seri_yazi, fill=seri_color, font=seri_font)

            # Son 1 Ay — gold+bold if ≥2
            m_count = stat.monthly_ceiling_count if is_ceiling else stat.monthly_floor_count
            m_yazi = f"{m_count} Kez"
            m_color = GOLD if m_count >= 2 else WHITE
            m_font = font_seri_bold if m_count >= 2 else font_reason
            draw.text((col_x[4], text_y), m_yazi, fill=m_color, font=m_font)

            # Neden (Multi-line) — 16px font, ~9.6px/char, 445px alan → max 38 char/satır
            reason_text = stat.reason if stat.reason else ""
            if reason_text:
                wrapped = textwrap.wrap(reason_text, width=38)
                if len(wrapped) > 2:
                    r_y = text_y - 16   # 3 satır: row_y+17, +39, +61
                elif len(wrapped) > 1:
                    r_y = text_y - 8    # 2 satır: row_y+25, +47
                else:
                    r_y = text_y        # 1 satır: row_y+33
                for line in wrapped[:3]:  # max 3 satır göster
                    draw.text((col_x[5], r_y), line, fill=GRAY, font=font_reason)
                    r_y += 22

        # === EK HİSSELER BÖLÜMÜ ===
        if show_supplementary and is_last_page and supp_to_show:
            # 0 hisse → mesaj yüksekliği (row_h yerine 80px), normal → satır sayısı * row_h
            main_area_h = 80 if len(page_stats) == 0 else len(page_stats) * row_h
            supp_start_y = y + main_area_h + 15
            supp_title = f"Diğer {'Yükselen' if is_ceiling else 'Düşen'} Hisseler"
            draw.line([(padding, supp_start_y), (width - padding, supp_start_y)], fill=DIVIDER, width=1)
            supp_start_y += 8
            draw.text((padding, supp_start_y), supp_title, fill=GRAY, font=font_supp_title)
            supp_start_y += 40

            # 0 hisse → 4 kolon (2x4=8), normal → 3 kolon
            cols_per_row = 4 if len(page_stats) == 0 else 3
            col_width = (width - 2 * padding) // cols_per_row
            for s_idx, s_stat in enumerate(supp_to_show):
                grid_row = s_idx // cols_per_row
                grid_col = s_idx % cols_per_row
                s_row_y = supp_start_y + (grid_row * supp_row_h)
                s_x_base = padding + grid_col * col_width
                
                # Alternate row bg
                if grid_col == 0:
                    s_row_bg = ROW_EVEN if grid_row % 2 == 0 else ROW_ODD
                    draw.rectangle([(0, s_row_y), (width, s_row_y + supp_row_h)], fill=s_row_bg)
                
                s_text_y = s_row_y + 18
                s_color = GREEN if is_ceiling else RED
                # Ticker
                draw.text((s_x_base, s_text_y), s_stat.ticker, fill=(180, 180, 180), font=font_supp_symbol)
                # Price + %
                s_pct = getattr(s_stat, "percent_change", 0.0)
                info_text = f"{s_stat.close_price:.2f}₺ ({s_pct:+.1f}%)"
                draw.text((s_x_base + 110, s_text_y), info_text, fill=s_color, font=font_footer_disclaimer)

        # === FOOTER ===
        footer_y = total_h - footer_h
        draw.line([(padding, footer_y), (width - padding, footer_y)], fill=DIVIDER, width=2)
        footer_y += 12
        draw.text((padding, footer_y), "szalgo.net.tr", fill=ORANGE, font=_load_font(18, bold=True))
        line1 = "SZ Algo Özel Eğitimli Modeller Tarafından Üretilmiştir."
        line2 = "Yatırım yaparken mutlaka kendi araştırmanızı yapınız."
        draw.text((padding + 170, footer_y), line1, fill=GRAY, font=_load_font(16, bold=False))
        draw.text((padding + 170, footer_y + 22), line2, fill=(120, 120, 120), font=_load_font(14, bold=False))

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{'tavan' if is_ceiling else 'taban'}_sf{page_idx+1}_{ts}.png"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)
        image_paths.append(filepath)

    return image_paths


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

        # ── E.D.O kolonu var mi kontrol (MCARD+) ────────
        has_edo = any(d.get("cumulative_edo_pct") is not None for d in days_data)

        # ── SUTUN BASLIKLARI ─────────────────────────────
        if has_edo:
            col_x = [padding, 130, 310, 490, 670, 890]
            col_labels = ["Gün", "Kapanış", "Günlük %", "Kümülatif %", "Durum", "Küm. E.D.O"]
        else:
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
                "tavan": TAVAN_GREEN, "alici_kapatti": MUTED_GREEN,
                "satici_kapatti": MUTED_RED, "taban": TABAN_RED, "not_kapatti": ORANGE,
            }
            durum_label = durum_label_map.get(durum_raw, "")
            durum_color = durum_color_map.get(durum_raw, GRAY)
            if durum_label:
                draw.text((col_x[4], text_y), durum_label, fill=durum_color, font=font_row_bold)

            # E.D.O kolonu (MCARD+)
            if has_edo:
                edo_val = d.get("cumulative_edo_pct")
                if edo_val is not None:
                    edo_text = f"%{edo_val:.2f}"
                    draw.text((col_x[5], text_y), edo_text, fill=GOLD, font=font_row)
                else:
                    draw.text((col_x[5], text_y), "—", fill=GRAY, font=font_row)

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
        del img, draw; gc.collect()

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


def _pct_to_color(pct: float) -> tuple:
    """Yuzde degerine gore gradyan renk doner.

    Tavan/taban parlak, orta degerler silik, nötr turuncu.
    Hem yesil hem kirmizi tarafta 4 kademe var.
    """
    if pct >= 9.5:
        return (0, 230, 64)       # tavan — cok parlak yesil
    elif pct >= 5.0:
        return (34, 197, 94)      # belirgin yesil
    elif pct >= 2.0:
        return (74, 140, 96)      # silik yesil
    elif pct > 0:
        return (60, 110, 75)      # cok silik yesil (hafif alicili)
    elif pct == 0.0:
        return (251, 146, 60)     # notr — turuncu
    elif pct > -2.0:
        return (110, 70, 70)      # cok silik kirmizi (hafif saticili)
    elif pct > -5.0:
        return (160, 80, 80)      # silik kirmizi
    elif pct > -9.5:
        return (239, 68, 68)      # belirgin kirmizi
    else:
        return (255, 40, 40)      # taban — cok parlak kirmizi


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
        font_badge_sm = _load_font(18, bold=False)  # Uzun badge etiketleri icin (SATICILI vs.)
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
        now_str = datetime.now(_TR_TZ).strftime("%d.%m.%Y %H:%M")
        # Baslik — GOLD, buyuk, ozel
        font_brand = _load_font(42, bold=True)
        draw.text((padding, y + 8), "HALKA ARZ HİSSELERİ", fill=GOLD, font=font_brand)
        draw.text((padding, y + 54), f"{now_str}  |  {num_cards} Hisse İşlemde", fill=GRAY, font=font_subtitle)

        # Tavan/taban/normal sayaci — sag tarafa
        tavan_c = sum(1 for s in snapshot_data if s.get("durum") == "tavan")
        taban_c = sum(1 for s in snapshot_data if s.get("durum") == "taban")
        normal_c = num_cards - tavan_c - taban_c
        summary = f"Tavan: {tavan_c}  |  Taban: {taban_c}  |  Normal: {normal_c}"
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

            # Sol kenar renk accent (gunluk % degerine gore gradyan)
            cum_pct = float(stock.get("cum_pct", 0))
            daily_fark_for_accent = float(stock.get("pct_change", 0))
            durum = stock.get("durum", "")
            accent_color = _pct_to_color(daily_fark_for_accent)
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

            # Kumulatif % (orta-sag) — HA fiyatindan bugune toplam degisim (gradyan renk)
            cum_color = _pct_to_color(cum_pct)
            cum_text = f"%{cum_pct:+.2f}"
            draw.text((620, row1_y), cum_text, fill=cum_color, font=font_data_bold)

            # Gunluk Fark — dunku kapanisa gore bugunun degisimi (gradyan renk)
            daily_fark = float(stock.get("pct_change", 0))
            # Label kismi duz GRAY
            fark_label = "Günlük Fark: "
            draw.text((750, row1_y + 6), fark_label, fill=GRAY, font=font_lot)
            # Yuzde kismi renkli (gradyan)
            label_w = font_lot.getbbox(fark_label)[2] - font_lot.getbbox(fark_label)[0]
            fark_color = _pct_to_color(daily_fark)
            fark_val = f"%{daily_fark:+.2f}"
            draw.text((750 + label_w, row1_y + 8), fark_val, fill=fark_color, font=font_cum_val)

            # Durum badge (sag kenar) — renk gunluk % bazli gradyan
            durum = stock.get("durum", "")
            durum_labels = {
                "tavan": "TAVAN",
                "taban": "TABAN",
                "alici_kapatti": "ALICILI",
                "satici_kapatti": "SATICILI",
                "not_kapatti": "NORMAL",
                "ilk_gun": "İLK GÜN",
            }
            d_label = durum_labels.get(durum, durum.upper() if durum else "—")
            d_color = _pct_to_color(daily_fark)  # gunluk % bazli gradyan renk

            # Badge arka plan kutusu — uzunluga gore font secimi
            if len(d_label) > 8:
                font_badge = font_badge_sm   # SATICILI vb. icin 18pt
            elif len(d_label) > 6:
                font_badge = font_lot        # 20pt
            else:
                font_badge = font_data_bold  # 24pt
            d_bbox = font_badge.getbbox(d_label)
            d_w = d_bbox[2] - d_bbox[0]
            badge_x = width - padding - d_w - 16
            badge_y = row1_y - 2
            badge_h = 30
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
                lot_text = f"Alış Lot: {_format_lot(alis_lot)}"
                lot_color = _pct_to_color(daily_fark)
                draw.text((padding + 180, row2_y), lot_text, fill=lot_color, font=font_lot)
            elif durum == "taban":
                lot_text = f"Satış Lot: {_format_lot(satis_lot)}"
                lot_color = _pct_to_color(daily_fark)
                draw.text((padding + 180, row2_y), lot_text, fill=lot_color, font=font_lot)

            # Küm. E.D.O (lot text'ten sonra, orta alan)
            edo_pct = stock.get("edo_pct")
            if edo_pct is not None:
                edo_label = "Küm. E.D.O: "
                edo_val = f"%{edo_pct:.2f}"
                # Lot text yoksa padding+180, varsa lot'un yanina koy
                edo_x = padding + 180
                if durum == "tavan":
                    lot_t = f"Alış Lot: {_format_lot(alis_lot)}"
                    edo_x = padding + 180 + font_lot.getbbox(lot_t)[2] - font_lot.getbbox(lot_t)[0] + 20
                elif durum == "taban":
                    lot_t = f"Satış Lot: {_format_lot(satis_lot)}"
                    edo_x = padding + 180 + font_lot.getbbox(lot_t)[2] - font_lot.getbbox(lot_t)[0] + 20
                draw.text((edo_x, row2_y + 2), edo_label, fill=GRAY, font=font_badge_sm)
                lbl_w = font_badge_sm.getbbox(edo_label)[2] - font_badge_sm.getbbox(edo_label)[0]
                draw.text((edo_x + lbl_w, row2_y + 2), edo_val, fill=GOLD, font=font_badge_sm)

            # Halka Arz fiyati (sag alt)
            ipo_price = stock.get("ipo_price")
            if ipo_price:
                ha_text = f"Halka Arz: {float(ipo_price):.2f} TL"
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
        del img, draw; gc.collect()
        logger.info("Market snapshot gorsel olusturuldu: %s (%d hisse)", filepath, num_cards)
        return filepath

    except Exception as e:
        logger.error("Market snapshot gorsel hatasi: %s", e)
        return None


# ══════════════════════════════════════════════════════════════
# T16 — Yeni Halka Arzlar Ilk 10 Gun Acilis Bilgileri (09:57)
# Grid layout: 1-3 = 1 satir, 4-6 = 2 satir (3'lu), banner bg
# ══════════════════════════════════════════════════════════════

# Ek renkler
LIGHT_BLUE = (96, 165, 250)    # #60a5fa
DARK_CARD_BG = (30, 30, 56)    # #1e1e38 — belirgin kart ici (BG'den ayrisir)
CARD_BORDER = (65, 65, 105)    # #41416b — belirgin kart kenarligi
CARD_SHADOW = (8, 8, 18)       # #080812 — drop shadow rengi
CARD_HIGHLIGHT = (45, 45, 80)  # #2d2d50 — ust kenar ince parlama
CYAN = (34, 211, 238)          # #22d3ee


def _draw_centered(draw, center_x: int, y: int, text: str,
                   font, color: tuple):
    """Metni verilen center_x'e gore ortalar."""
    bbox = font.getbbox(text)
    tw = bbox[2] - bbox[0]
    draw.text((center_x - tw // 2, y), text, fill=color, font=font)


def _durum_label(durum: str) -> tuple:
    """Durum kodundan okunabilir etiket + renk doner."""
    mapping = {
        "tavan": ("TAVAN", TAVAN_GREEN),
        "alici_kapatti": ("ALICILI", MUTED_GREEN),
        "not_kapatti": ("NORMAL", GOLD),
        "satici_kapatti": ("SATICILI", MUTED_RED),
        "taban": ("TABAN", TABAN_RED),
    }
    return mapping.get(durum, ("—", GRAY))


def _draw_rounded_rect(draw, xy, radius, fill, outline=None, outline_width=1):
    """Koseleri yuvarlatilmis dikdortgen cizer."""
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle([(x0, y0), (x1, y1)], radius=radius,
                           fill=fill, outline=outline, width=outline_width)


def _draw_card_premium(draw, xy, radius, fill, outline, shadow_color=None):
    """Premium kart cizer — drop shadow + highlight + kalin border."""
    x0, y0, x1, y1 = xy
    # 1) Drop shadow (4px offset, biraz buyuk)
    if shadow_color:
        _draw_rounded_rect(draw, (x0 + 4, y0 + 4, x1 + 4, y1 + 4),
                           radius=radius, fill=shadow_color)
    # 2) Ana kart gövdesi
    _draw_rounded_rect(draw, (x0, y0, x1, y1),
                       radius=radius, fill=fill, outline=outline,
                       outline_width=2)
    # 3) Üst iç kenar highlight (1px ince parlak çizgi)
    draw.rounded_rectangle([(x0 + 2, y0 + 2), (x1 - 2, y0 + 6)],
                           radius=4, fill=CARD_HIGHLIGHT)


def generate_opening_summary_image(stocks: list) -> Optional[str]:
    """Ilk 10 gun icindeki hisselerin acilis bilgilerini GRID layout ile olusturur.

    Layout:
        1-3 hisse = 1 satir (yan yana)
        4-6 hisse = 2 satir (ust 3 + alt 1-3)
        Banner arka plan + kart bazli tasarim

    Args:
        stocks: Her hisse icin dict:
            [
                {
                    "ticker": "AKHAN",
                    "company_name": "...",
                    "trading_day": 3,
                    "ipo_price": 21.50,
                    "open_price": 23.65,
                    "prev_close": 21.50,         # dunku kapanis
                    "pct_change": +10.0,          # acilis vs dunku kapanis %
                    "daily_pct": +10.0,           # gunluk % degisim
                    "durum": "tavan",
                    "ceiling_days": 2,
                    "floor_days": 0,
                    "normal_days": 1,
                    "alis_lot": 1245000,          # tavanda/alista bekleyen
                    "satis_lot": 0,               # tabanda/satista bekleyen
                }
            ]

    Returns:
        PNG dosya yolu veya None
    """
    try:
        if not stocks:
            return None

        num_stocks = len(stocks)
        if num_stocks > 6:
            stocks = stocks[:6]
            num_stocks = 6

        # ── Grid hesaplama ────────────────────────
        # 1-3 hisse = 1 satir, 4-6 = 2 satir
        if num_stocks <= 3:
            cols = num_stocks
            rows = 1
        else:
            cols = 3
            rows = 2

        # ── Boyutlar ─────────────────────────────
        width = 1200
        padding = 40
        card_gap = 20           # kartlar arasi bosluk
        card_w = (width - 2 * padding - (cols - 1) * card_gap) // cols
        card_h = 380            # her kart yuksekligi (EDO + lot icin alan)
        card_radius = 18        # daha yumusak koseler

        # Banner
        banner_h = 0
        banner_img = None
        _img_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
        banner_path = os.path.join(_img_dir, "acilis_analizi_banner.png")
        if not os.path.exists(banner_path):
            banner_path = os.path.join(_img_dir, "acilis_raporu_banner.png")
        if not os.path.exists(banner_path):
            banner_path = os.path.join(_img_dir, "acilis_bilgileri_banner.jpg")

        if os.path.exists(banner_path):
            try:
                raw = Image.open(banner_path).convert("RGB")
                # Banner'i genislige scale et
                scale = width / raw.width
                banner_h = int(raw.height * scale)
                if banner_h > 380:
                    banner_h = 380
                banner_img = raw.resize((width, banner_h), Image.LANCZOS)
            except Exception as be:
                logger.warning("T16 Banner yuklenemedi: %s", be)
                banner_img = None
                banner_h = 0

        # Header (baslik alani — banner'in altinda)
        header_h = 70
        # Grid alani
        grid_h = rows * card_h + (rows - 1) * card_gap
        # Footer
        footer_h = 45

        total_h = banner_h + header_h + grid_h + footer_h + padding * 2

        # ── Canvas olustur ────────────────────────
        img = Image.new("RGB", (width, total_h), BG_COLOR)

        # Banner paste
        if banner_img:
            img.paste(banner_img, (0, 0))

        draw = ImageDraw.Draw(img)

        # ── Fontlar ───────────────────────────────
        font_header = _load_font(30, bold=True)
        font_header_sm = _load_font(18)
        font_ticker = _load_font(26, bold=True)
        font_day = _load_font(16)
        font_label = _load_font(14)
        font_value = _load_font(18, bold=True)
        font_value_lg = _load_font(22, bold=True)
        font_lot = _load_font(13)
        font_badge = _load_font(15, bold=True)
        font_small = _load_font(12)
        font_footer = _load_font(16, bold=True)
        font_footer_sm = _load_font(13)

        # ── Header ────────────────────────────────
        hdr_y = banner_h
        draw.rectangle([(0, hdr_y), (width, hdr_y + header_h)], fill=(18, 18, 36))

        title_text = "YENİ HALKA ARZLAR — AÇILIŞ BİLGİLERİ"
        draw.text((padding, hdr_y + 10), title_text, fill=GOLD, font=font_header)

        date_text = datetime.now(_TR_TZ).strftime("%d.%m.%Y")
        info_text = f"{date_text}  |  {num_stocks} Hisse  |  İlk 10 İşlem Günü"
        draw.text((padding, hdr_y + 44), info_text, fill=GRAY, font=font_header_sm)

        # Header alt cizgi — ince gold
        draw.line([(padding, hdr_y + header_h - 1),
                   (width - padding, hdr_y + header_h - 1)],
                  fill=GOLD, width=2)

        # ── Grid Kartlari ─────────────────────────
        grid_start_y = banner_h + header_h + padding

        for idx, stock in enumerate(stocks):
            row = idx // cols
            col = idx % cols

            # Kart pozisyonu
            cx = padding + col * (card_w + card_gap)
            cy = grid_start_y + row * (card_h + card_gap)

            pct = float(stock.get("pct_change", 0))
            daily_pct = float(stock.get("daily_pct", pct))
            durum = stock.get("durum", "")
            durum_text, durum_color = _durum_label(durum)
            ipo_price = float(stock.get("ipo_price", 0))
            open_price = float(stock.get("open_price", 0))
            prev_close = float(stock.get("prev_close", ipo_price))
            alis_lot = stock.get("alis_lot")
            satis_lot = stock.get("satis_lot")

            # ─ Premium kart (shadow + highlight + border) ─
            _draw_card_premium(draw, (cx, cy, cx + card_w, cy + card_h),
                               radius=card_radius, fill=DARK_CARD_BG,
                               outline=CARD_BORDER, shadow_color=CARD_SHADOW)

            # ─ Ust kenar renk cizgisi (durum'a gore — daha kalin) ─
            draw.rounded_rectangle(
                [(cx + 2, cy + 2), (cx + card_w - 2, cy + 7)],
                radius=6, fill=durum_color,
            )

            mid_x = cx + card_w // 2
            inner_pad = 14  # kart ici kenar boslugu
            content_x = cx + inner_pad

            # ─ Ticker + Gun ─
            y = cy + 16
            _draw_centered(draw, mid_x, y, f"#{stock['ticker']}", font_ticker, WHITE)
            y += 32
            _draw_centered(draw, mid_x, y, f"{stock['trading_day']}. İşlem Günü",
                           font_day, GRAY)
            y += 26

            # ─ Durum badge ─
            d_bbox = font_badge.getbbox(durum_text)
            d_w = d_bbox[2] - d_bbox[0]
            badge_x = mid_x - d_w // 2 - 10
            badge_w = d_w + 20
            badge_h_px = 24
            _draw_rounded_rect(draw,
                               (badge_x, y, badge_x + badge_w, y + badge_h_px),
                               radius=6, fill=durum_color)
            _draw_centered(draw, mid_x, y + 3, durum_text, font_badge, (0, 0, 0))
            y += badge_h_px + 14

            # ─ Acilis Fiyati (buyuk) ─
            # Renk: günlük değişime göre (daily_pct), HA fiyatına göre (pct) DEĞİL
            price_color = GREEN if daily_pct >= 0 else RED
            _draw_centered(draw, mid_x, y, "Açılış Fiyatı", font_label, GRAY)
            y += 18
            _draw_centered(draw, mid_x, y, f"{open_price:.2f} TL",
                           font_value_lg, price_color)
            y += 30

            # ─ Acilis % Fark (buyuk, renkli) ─
            _draw_centered(draw, mid_x, y, "Açılış % Fark", font_label, GRAY)
            y += 18
            pct_text = f"%{daily_pct:+.1f}"
            _draw_centered(draw, mid_x, y, pct_text, font_value_lg, price_color)
            y += 32

            # ─ Lot bilgileri (tavan/taban'a özel) ─
            _alis = alis_lot or 0
            _satis = satis_lot or 0

            if durum == "tavan":
                _draw_centered(draw, mid_x, y, "Tavana Alış Bekleyen", font_small, (100, 100, 120))
                y += 16
                _draw_centered(draw, mid_x, y, f"{_format_lot(_alis)} lot",
                               font_value, TAVAN_GREEN)
                y += 28
            elif durum == "taban":
                _draw_centered(draw, mid_x, y, "Tabana Satış Bekleyen", font_small, (100, 100, 120))
                y += 16
                _draw_centered(draw, mid_x, y, f"{_format_lot(_satis)} lot",
                               font_value, TABAN_RED)
                y += 28
            # Normal durumda lot gösterme

            # ─ Kümülatif E.D.O (senet_sayisi olan IPO'lar) ─
            edo_pct = stock.get("edo_pct")
            if edo_pct is not None:
                # İnce ayırıcı çizgi
                line_pad = 20
                draw.line(
                    [(cx + line_pad, cy + card_h - 50),
                     (cx + card_w - line_pad, cy + card_h - 50)],
                    fill=CARD_BORDER, width=1,
                )
                edo_y = cy + card_h - 42
                _draw_centered(draw, mid_x, edo_y, "Küm. E.D.O", font_small, GRAY)
                _draw_centered(draw, mid_x, edo_y + 16, f"%{edo_pct:.2f}",
                               font_value, GOLD)

        # ── Footer ────────────────────────────────
        footer_y = total_h - footer_h
        draw.rectangle([(0, footer_y), (width, total_h)], fill=HEADER_BG)
        draw.line([(padding, footer_y), (width - padding, footer_y)],
                  fill=DIVIDER, width=2)

        # Logo (sol — 34x34 boyutunda)
        logo_path = os.path.join(_img_dir, "logo.png")
        if not os.path.exists(logo_path):
            logo_path = os.path.join(_img_dir, "logo.jpg")  # fallback
        logo_x = padding
        try:
            if os.path.exists(logo_path):
                logo_raw = Image.open(logo_path).convert("RGBA")
                logo_size = 34
                logo_resized = logo_raw.resize((logo_size, logo_size), Image.LANCZOS)
                logo_y_pos = footer_y + (footer_h - logo_size) // 2
                # RGBA paste (seffaflik destegi)
                img.paste(logo_resized, (logo_x, logo_y_pos), logo_resized)
                logo_x += logo_size + 8
        except Exception:
            pass

        # szalgo.net.tr (logo'nun yaninda)
        site_text = "szalgo.net.tr"
        draw.text((logo_x, footer_y + 12), site_text,
                  fill=ORANGE, font=font_footer)

        # Sag taraf — BIST Haber & Arz
        right_text = "BIST Haber & Arz"
        rb = font_footer_sm.getbbox(right_text)
        rw = rb[2] - rb[0]
        draw.text((width - padding - rw, footer_y + 14), right_text,
                  fill=GRAY, font=font_footer_sm)

        # Watermark
        _draw_bg_watermark(img, width, total_h)

        # ── Kaydet ────────────────────────────────
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"opening_summary_{ts}.png"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)
        del img, draw; gc.collect()
        logger.info("Opening summary gorsel olusturuldu: %s (%d hisse, %dx%d)",
                     filepath, num_stocks, width, total_h)
        return filepath

    except Exception as e:
        logger.error("Opening summary gorsel hatasi: %s", e, exc_info=True)
        return None


# ================================================================
# SPK YENİ HALKA ARZ ONAYI GÖRSELİ
# ================================================================

def _fmt_capital(val) -> str:
    """Sermaye sayısını okunabilir formata çevirir: 141000000 → 141 mn TL"""
    try:
        n = float(val)
        if n >= 1_000_000_000:
            return f"{n / 1_000_000_000:.2f} mr TL"
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f} mn TL"
        return f"{n:,.0f} TL"
    except Exception:
        return str(val)


def generate_spk_onay_image(approvals: list, bulletin_no: str) -> Optional[str]:
    """SPK halka arz onayları için özel görsel oluşturur.

    Args:
        approvals: [{"company_name": str, "existing_capital": Decimal,
                     "new_capital": Decimal, "sale_price": Decimal|None}, ...]
        bulletin_no: "2026/10" gibi bülten numarası

    Returns:
        Oluşturulan PNG dosyasının yolu veya None
    """
    try:
        if not approvals:
            return None

        _img_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
        width = 1200
        padding = 48
        count = len(approvals)

        # ── Renkler ─────────────────────────────────────
        BG          = (14, 14, 26)        # #0e0e1a çok koyu
        CARD_BG     = (22, 22, 42)        # #16162a kart arkaplanı
        CARD_BG2    = (28, 28, 52)        # #1c1c34 alternatif satır
        HEADER_BG_C = (20, 20, 40)        # üst bar
        TOP_STRIPE  = (34, 197, 94)       # #22c55e yeşil üst şerit
        ACCENT_GRN  = (34, 197, 94)
        GOLD_C      = (250, 204, 21)
        ORANGE_C    = (251, 146, 60)
        WHITE_C     = (255, 255, 255)
        GRAY_C      = (156, 163, 175)
        DIVIDER_C   = (50, 50, 80)
        BLUE_ACC    = (99, 102, 241)      # #6366f1 indigo — vurgu

        # ── Fontlar ─────────────────────────────────────
        f_title      = _load_font(46, bold=True)
        f_subtitle   = _load_font(28, bold=False)
        f_company    = _load_font(34, bold=True)
        f_detail     = _load_font(26, bold=False)
        f_detail_b   = _load_font(26, bold=True)
        f_footer     = _load_font(28, bold=True)
        f_footer_sm  = _load_font(22, bold=False)
        f_badge      = _load_font(22, bold=True)

        # ── Boyut hesapla ────────────────────────────────
        top_stripe_h = 8      # üst yeşil çizgi
        header_h     = 160    # SZ Algo branding
        title_h      = 120    # başlık bölümü
        card_h       = 175    # her IPO kartı
        gap          = 16     # kartlar arası boşluk
        footer_h     = 80
        cards_total  = count * card_h + (count - 1) * gap + padding
        total_h      = top_stripe_h + header_h + title_h + cards_total + footer_h

        img  = Image.new("RGB", (width, total_h), BG)
        draw = ImageDraw.Draw(img)

        # ── Üst yeşil şerit ─────────────────────────────
        draw.rectangle([(0, 0), (width, top_stripe_h)], fill=TOP_STRIPE)

        # ── Header bölümü (Logo + SZ Algo Finans) ───────
        y = top_stripe_h
        draw.rectangle([(0, y), (width, y + header_h)], fill=HEADER_BG_C)

        # Logo — logo.png (megafon+candlestick), fallback logo.jpg
        logo_path = os.path.join(_img_dir, "logo.png")
        if not os.path.exists(logo_path):
            logo_path = os.path.join(_img_dir, "logo.jpg")
        logo_x = padding
        logo_size_h = 80  # Header'da büyük logo
        logo_y = y + (header_h - logo_size_h) // 2
        try:
            if os.path.exists(logo_path):
                logo_raw = Image.open(logo_path).convert("RGBA")
                logo_r = logo_raw.resize((logo_size_h, logo_size_h), Image.LANCZOS)
                # RGBA destekli yapıştırma (transparan arka plan için)
                tmp = Image.new("RGBA", img.size, (0, 0, 0, 0))
                tmp.paste(logo_r, (logo_x, logo_y))
                img_rgba = img.convert("RGBA")
                img_rgba = Image.alpha_composite(img_rgba, tmp)
                img.paste(img_rgba.convert("RGB"))
                logo_x += logo_size_h + 14
        except Exception:
            pass

        # "SZ Algo Finans" + "Halka Arz İzleme"
        draw.text((logo_x, y + 28), "SZ Algo Finans",
                  fill=WHITE_C, font=_load_font(36, bold=True))
        draw.text((logo_x, y + 76), "Halka Arz Takip & Bildirim",
                  fill=GRAY_C, font=f_subtitle)

        # Sağ taraf — bülten numarası badge
        badge_text = f"SPK Bülteni {bulletin_no}"
        bb = f_badge.getbbox(badge_text)
        bw = bb[2] - bb[0] + 28
        bh = 38
        bx = width - padding - bw
        by = y + (header_h - bh) // 2
        draw.rounded_rectangle([(bx, by), (bx + bw, by + bh)],
                                radius=8, fill=BLUE_ACC)
        draw.text((bx + 14, by + 8), badge_text, fill=WHITE_C, font=f_badge)

        # Alt divider
        draw.line([(0, y + header_h - 1), (width, y + header_h - 1)],
                  fill=DIVIDER_C, width=1)

        # ── Başlık bölümü ────────────────────────────────
        y += header_h
        draw.rectangle([(0, y), (width, y + title_h)], fill=BG)

        # Yeşil tik + büyük başlık
        title_icon = "✅"
        title_num  = f" {count} Yeni" if count > 1 else " Yeni"
        title_rest = " Halka Arz Onayı"
        draw.text((padding, y + 22), title_icon + title_num,
                  fill=ACCENT_GRN, font=f_title)
        # "Halka Arz Onayı" kısmını beyaz yaz
        icon_bb = f_title.getbbox(title_icon + title_num)
        icon_w  = icon_bb[2] - icon_bb[0]
        draw.text((padding + icon_w, y + 22), title_rest,
                  fill=WHITE_C, font=f_title)

        # Alt satır: "SPK tarafından onaylandı"
        sub_text = f"SPK tarafından onaylandı — {bulletin_no} Bülteni"
        draw.text((padding, y + 78), sub_text, fill=GRAY_C, font=f_subtitle)

        # Divider
        draw.line([(padding, y + title_h - 4), (width - padding, y + title_h - 4)],
                  fill=DIVIDER_C, width=1)

        # ── IPO Kartları ─────────────────────────────────
        y += title_h + gap // 2
        for i, appr in enumerate(approvals):
            card_y = y + i * (card_h + gap)
            card_bg = CARD_BG if i % 2 == 0 else CARD_BG2

            # Kart arkaplanı (yuvarlak köşe)
            draw.rounded_rectangle(
                [(padding, card_y), (width - padding, card_y + card_h)],
                radius=12, fill=card_bg,
            )

            # Sol yeşil aksan çizgisi
            draw.rounded_rectangle(
                [(padding, card_y), (padding + 6, card_y + card_h)],
                radius=4, fill=ACCENT_GRN,
            )

            # Numara badge (sağ üst)
            num_text = f"#{i + 1}"
            nb = f_badge.getbbox(num_text)
            nw = nb[2] - nb[0] + 20
            nx = width - padding - nw - 10
            ny = card_y + 12
            draw.rounded_rectangle([(nx, ny), (nx + nw, ny + 28)],
                                    radius=6, fill=(40, 40, 70))
            draw.text((nx + 10, ny + 4), num_text, fill=GRAY_C, font=f_badge)

            cx = padding + 24   # sol iç padding
            cy = card_y + 18

            # Şirket adı
            company_name = appr.get("company_name", "Bilinmiyor")
            # Çok uzunsa kes
            if len(company_name) > 52:
                company_name = company_name[:50] + "…"
            draw.text((cx, cy), company_name, fill=WHITE_C, font=f_company)
            cy += 44

            # Sermaye bilgisi: Mevcut → Yeni
            exist_cap = appr.get("existing_capital")
            new_cap   = appr.get("new_capital")
            if exist_cap and new_cap:
                cap_text = f"🏦  {_fmt_capital(exist_cap)}  →  {_fmt_capital(new_cap)}"
                draw.text((cx, cy), cap_text, fill=GRAY_C, font=f_detail)
                cy += 36
            elif new_cap:
                cap_text = f"🏦  Yeni Sermaye: {_fmt_capital(new_cap)}"
                draw.text((cx, cy), cap_text, fill=GRAY_C, font=f_detail)
                cy += 36

            # Halka arz fiyatı
            price = appr.get("sale_price")
            if price:
                try:
                    price_f = float(price)
                    price_str = f"{price_f:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    draw.text((cx, cy), f"💰  Halka Arz Fiyatı: ",
                              fill=GRAY_C, font=f_detail)
                    px_bb = f_detail.getbbox("💰  Halka Arz Fiyatı: ")
                    px_w  = px_bb[2] - px_bb[0]
                    draw.text((cx + px_w, cy), f"{price_str} TL",
                              fill=GOLD_C, font=f_detail_b)
                except Exception:
                    pass
            else:
                draw.text((cx, cy), "💰  Halka Arz Fiyatı: Belirlenmedi",
                          fill=GRAY_C, font=f_detail)

        # ── Footer ───────────────────────────────────────
        footer_y = total_h - footer_h
        draw.rectangle([(0, footer_y), (width, total_h)], fill=HEADER_BG_C)
        draw.line([(0, footer_y), (width, footer_y)], fill=TOP_STRIPE, width=2)

        # Footer logo (küçük — transparan paste)
        logo_xx = padding
        try:
            if os.path.exists(logo_path):
                logo_r2 = Image.open(logo_path).convert("RGBA").resize((32, 32), Image.LANCZOS)
                tmp2 = Image.new("RGBA", img.size, (0, 0, 0, 0))
                tmp2.paste(logo_r2, (logo_xx, footer_y + 22))
                img_rgba2 = img.convert("RGBA")
                img_rgba2 = Image.alpha_composite(img_rgba2, tmp2)
                img.paste(img_rgba2.convert("RGB"))
                logo_xx += 40
        except Exception:
            pass
        draw.text((logo_xx, footer_y + 24), "szalgo.net.tr",
                  fill=ORANGE_C, font=f_footer)

        # Sağ: disclaimer
        disc = "Yatırım tavsiyesi değildir"
        db   = f_footer_sm.getbbox(disc)
        dw   = db[2] - db[0]
        draw.text((width - padding - dw, footer_y + 28),
                  disc, fill=GRAY_C, font=f_footer_sm)

        # Watermark
        _draw_bg_watermark(img, width, total_h)

        # ── Kaydet ───────────────────────────────────────
        ts = datetime.now(_TR_TZ).strftime("%Y%m%d_%H%M%S")
        filename  = f"spk_onay_{ts}.png"
        filepath  = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)
        del img, draw; gc.collect()
        logger.info("SPK onay gorseli olusturuldu: %s (%d onay)", filepath, count)
        return filepath

    except Exception as e:
        logger.error("generate_spk_onay_image hatasi: %s", e, exc_info=True)
        return None


# ════════════════════════════════════════════════════════════════
# AI MARKET REPORT → PNG GÖRSEL
# ════════════════════════════════════════════════════════════════

# Bölüm renk haritası — emoji header → (accent_color, clean_title)
_SECTION_MAP = [
    # (emoji_pattern, accent_color, display_title)
    ("🇹🇷", RED,    "BIST 100"),
    ("🔴", RED,     "BIST 100"),
    ("🇺🇸", (34, 211, 238),  "ABD Piyasalari"),
    ("💰", GOLD,   "Dolar & Altin"),
    ("💵", GOLD,   "Dolar & Altin"),
    ("📰", ORANGE, "Gunun Onemli Gelismeleri"),
    ("📈", GREEN,  "Halka Arz Takibi"),
    ("🏦", GREEN,  "Halka Arz Takibi"),
    ("📅", GRAY,   "Ekonomik Takvim"),
    ("⏰", GRAY,   "Ekonomik Takvim"),
    ("📌", RED,    "Kritik Noktalar"),
    ("🚀", RED,    "Kritik Noktalar"),
    ("🔮", ORANGE, "Yarin Icin Beklentiler"),
]

# **bold** metin section header'ları (emoji olmadan) — fallback matching
_BOLD_SECTION_MAP = [
    # (keyword_pattern, accent_color, display_title)
    ("bist 100",             RED,            "BIST 100 (XU100)"),
    ("bist100",              RED,            "BIST 100 (XU100)"),
    ("xu100",                RED,            "BIST 100 (XU100)"),
    ("abd piyasa",           (34, 211, 238), "ABD Piyasalari"),
    ("dolar",                GOLD,           "Dolar & Altin"),
    ("altin",                GOLD,           "Dolar & Altin"),
    ("onemli gelis",         ORANGE,         "Gunun Onemli Gelismeleri"),
    ("önemli gelis",         ORANGE,         "Gunun Onemli Gelismeleri"),
    ("önemli gelişme",       ORANGE,         "Günün Önemli Gelişmeleri"),
    ("gunun gelisme",        ORANGE,         "Gunun Onemli Gelismeleri"),
    ("günün gelişme",        ORANGE,         "Günün Önemli Gelişmeleri"),
    ("halka arz",            GREEN,          "Halka Arz Takibi"),
    ("ekonomik takvim",      GRAY,           "Ekonomik Takvim"),
    ("kritik nokta",         RED,            "Kritik Noktalar"),
    ("bugünün kritik",       RED,            "Bugünün Kritik Noktaları"),
    ("bugunun kritik",       RED,            "Bugunun Kritik Noktalari"),
    ("yarin icin",           ORANGE,         "Yarin Icin Beklentiler"),
    ("yarın için",           ORANGE,         "Yarın İçin Beklentiler"),
]

# Sondaki bölümler (footer olarak render edilecek)
_FOOTER_MARKERS = ["💬", "❓", "🗣", "⚠️", "⚠", "📲", "#BIST",
                    "Sizce", "sizce", "Yatirim tavsiyesi", "Yatırım tavsiyesi",
                    "yatirim tavsiyesi", "yatırım tavsiyesi"]

CYAN = (34, 211, 238)


def _parse_report_sections(report_text: str) -> dict:
    """AI rapor metnini bölümlere ayırır.

    Returns:
        {
            "hook": str,           # İlk 1-2 satır (⚡ hook)
            "sections": [          # Ana bölümler
                {"color": (r,g,b), "title": str, "lines": [str]},
                ...
            ],
            "footer_lines": [str], # Soru + disclaimer + link + hashtags
        }
    """
    import re

    lines = report_text.strip().split("\n")

    # ── Hook: İlk anlamlı satırlar (emoji veya metin, bölüm header'ı olmayan) ──
    hook_lines = []
    body_start = 0

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            if hook_lines:
                body_start = i + 1
                break
            continue

        # Bu satır bir section header mı? (emoji veya **bold** kontrol)
        is_section = False
        for emoji, _, _ in _SECTION_MAP:
            if stripped.startswith(emoji):
                is_section = True
                break
        # **bold** header kontrolü
        if not is_section and stripped.startswith("**") and stripped.endswith("**"):
            inner = stripped.strip("* ").lower()
            for keyword, _, _ in _BOLD_SECTION_MAP:
                if keyword in inner:
                    is_section = True
                    break

        if is_section and hook_lines:
            body_start = i
            break

        # Hook satırı — ⚡, 🔥, 🚨 gibi emojileri temizle
        clean = re.sub(r'^[⚡🔥🚨📊🔴📈💥🔑🏦]\s*', '', stripped).strip()
        if clean:
            hook_lines.append(clean)

        if len(hook_lines) >= 2:
            body_start = i + 1
            break

    # ── Bölümleri parse et ──
    sections = []
    current_section = None
    footer_lines = []
    in_footer = False

    for i in range(body_start, len(lines)):
        stripped = lines[i].strip()
        if not stripped:
            continue

        # Footer marker kontrolü
        if not in_footer:
            for marker in _FOOTER_MARKERS:
                if stripped.startswith(marker):
                    in_footer = True
                    break

        if in_footer:
            # Emoji'leri temizle, plain text al
            clean = re.sub(r'^[⚠️📲💬❓🗣🗨]\s*', '', stripped).strip()
            if clean and not clean.startswith("#BIST") and not clean.startswith("#borsa"):
                footer_lines.append(clean)
            continue

        # Section header kontrolü — emoji prefix
        matched_section = None
        for emoji, color, title in _SECTION_MAP:
            if stripped.startswith(emoji):
                raw_title = stripped
                raw_title = re.sub(r'[\U0001F1E0-\U0001F1FF\U0001F300-\U0001F9FF\u2600-\u26FF\u2700-\u27BF]', '', raw_title).strip()
                raw_title = raw_title.replace("**", "").strip()
                if not raw_title:
                    raw_title = title
                matched_section = {"color": color, "title": raw_title, "lines": []}
                break

        # Fallback: **bold text** header kontrolü (emoji yoksa)
        if not matched_section and stripped.startswith("**") and stripped.endswith("**"):
            inner = stripped.strip("* ").lower()
            for keyword, color, title in _BOLD_SECTION_MAP:
                if keyword in inner:
                    display_title = stripped.strip("* ").strip()
                    matched_section = {"color": color, "title": display_title, "lines": []}
                    break

        if matched_section:
            if current_section and current_section["lines"]:
                sections.append(current_section)
            current_section = matched_section
        elif current_section is not None:
            # Body satırı — temizle
            clean = stripped
            clean = clean.replace("**", "")  # Bold marker kaldır
            # Emoji'leri kaldır (text fontlar desteklemiyor)
            clean = re.sub(r'[\U0001F300-\U0001F9FF\U0001F1E0-\U0001F1FF\u2600-\u26FF\u2700-\u27BF\u200d]', '', clean).strip()
            # Hashtag (#) işaretlerini kaldır — resimde hashtag olmayacak
            clean = re.sub(r'#([A-Za-z])', r'\1', clean)
            if clean:
                current_section["lines"].append(clean)

    if current_section and current_section["lines"]:
        sections.append(current_section)

    return {
        "hook": "\n".join(hook_lines) if hook_lines else "",
        "sections": sections,
        "footer_lines": footer_lines,
    }


def _wrap_text(text: str, font, max_width: int, draw: ImageDraw.Draw) -> list[str]:
    """Metni verilen genişliğe göre satırlara böler (word-wrap)."""
    words = text.split()
    if not words:
        return []

    lines_out = []
    current_line = words[0]

    for word in words[1:]:
        test = current_line + " " + word
        bbox = draw.textbbox((0, 0), test, font=font)
        w = bbox[2] - bbox[0]
        if w <= max_width:
            current_line = test
        else:
            lines_out.append(current_line)
            current_line = word

    lines_out.append(current_line)
    return lines_out


def generate_report_image(report_text: str, report_type: str = "morning") -> Optional[str]:
    """AI piyasa rapor metnini şık bir PNG görsele çevirir.

    Args:
        report_text: AI'ın ürettiği rapor metni (2000+ karakter)
        report_type: "morning" veya "evening"

    Returns:
        PNG dosya yolu veya None
    """
    try:
        parsed = _parse_report_sections(report_text)

        # ── Taşma koruması: bölüm ve satır sınırları ──
        if len(parsed["sections"]) > 8:
            parsed["sections"] = parsed["sections"][:8]
        for sec in parsed["sections"]:
            if len(sec["lines"]) > 6:
                sec["lines"] = sec["lines"][:6]
        if len(parsed["footer_lines"]) > 3:
            parsed["footer_lines"] = parsed["footer_lines"][:3]

        # ── Fontlar ──
        font_hook = _load_font(28, bold=True)
        font_title = _load_font(22, bold=True)
        font_body = _load_font(18, bold=False)
        font_body_bold = _load_font(18, bold=True)
        font_footer = _load_font(16, bold=False)
        font_footer_bold = _load_font(16, bold=True)

        WIDTH = 1200
        PAD = 50           # Sol/sağ padding
        CONTENT_W = WIDTH - PAD * 2  # 1100px içerik alanı
        SECTION_GAP = 20   # Bölümler arası boşluk
        LINE_H = 26        # Satır yüksekliği (body)
        ACCENT_W = 5       # Sol accent çubuk genişliği
        ACCENT_PAD = 16    # Accent çubuk → metin arası

        # ── Taşma korumaları ──
        MAX_BODY_LINES = 8       # Bölüm başına max satır (wrap sonrası)
        MAX_IMAGE_H = 2600       # Görsel max yükseklik (px) — çok uzun olmasın
        MAX_SECTIONS = 8         # Max bölüm sayısı

        # ── Banner yüksekliği hesapla ──
        _img_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
        if report_type == "evening":
            banner_path = os.path.join(_img_dir, "kapanis_raporu_banner.png")
        else:
            banner_path = os.path.join(_img_dir, "acilis_analizi_banner.png")

        BANNER_H = 200
        banner_img = None
        if os.path.exists(banner_path):
            try:
                banner_img = Image.open(banner_path)
                # 1200px genişliğe sığdır, oran koru
                ratio = WIDTH / banner_img.width
                new_h = int(banner_img.height * ratio)
                # Max 200px yükseklik — üstten kes gerekirse
                if new_h > BANNER_H:
                    banner_img = banner_img.resize((WIDTH, new_h), Image.LANCZOS)
                    # Üstten kırp (başlık genelde üstte)
                    banner_img = banner_img.crop((0, 0, WIDTH, BANNER_H))
                else:
                    BANNER_H = new_h
                    banner_img = banner_img.resize((WIDTH, BANNER_H), Image.LANCZOS)
            except Exception as e:
                logger.warning("Banner yuklenemedi: %s", e)
                banner_img = None
                BANNER_H = 0
        else:
            BANNER_H = 0

        # ── Yükseklik hesapla (pre-render) ──
        # Geçici draw nesnesi oluştur
        tmp_img = Image.new("RGB", (WIDTH, 100))
        tmp_draw = ImageDraw.Draw(tmp_img)

        y_calc = BANNER_H + 10  # Banner sonrası

        # Hook yüksekliği
        hook_lines_wrapped = []
        if parsed["hook"]:
            for hl in parsed["hook"].split("\n"):
                hook_lines_wrapped.extend(_wrap_text(hl, font_hook, CONTENT_W, tmp_draw))
            y_calc += len(hook_lines_wrapped) * 36 + 20  # 36px satır yüksekliği

        # Bölüm yükseklikleri
        for sec in parsed["sections"]:
            y_calc += 8  # Divider üstü boşluk
            y_calc += 2  # Divider çizgi
            y_calc += 12 # Divider altı boşluk
            y_calc += 30  # Başlık
            y_calc += 8   # Başlık-body arası

            body_x = PAD + ACCENT_W + ACCENT_PAD
            body_w = CONTENT_W - ACCENT_W - ACCENT_PAD

            sec_line_count = 0
            for line in sec["lines"]:
                if sec_line_count >= MAX_BODY_LINES:
                    break
                if line.startswith("•") or line.startswith("-"):
                    indent = 20
                    wrapped = _wrap_text(line, font_body, body_w - indent, tmp_draw)
                else:
                    wrapped = _wrap_text(line, font_body, body_w, tmp_draw)
                # Tek bir body line max 4 wrap satırı (çok uzun paragrafları kes)
                if len(wrapped) > 4:
                    wrapped = wrapped[:4]
                    wrapped[-1] = wrapped[-1][:len(wrapped[-1])-3] + "..."
                y_calc += len(wrapped) * LINE_H
                sec_line_count += len(wrapped)

            y_calc += SECTION_GAP

        # Footer yüksekliği
        y_calc += 16  # Divider öncesi
        y_calc += 2   # Divider
        y_calc += 16  # Divider sonrası
        y_calc += len(parsed["footer_lines"]) * 24 + 16

        total_h = y_calc + 20  # Alt padding

        # ── Taşma koruması: max yükseklik ──
        if total_h > MAX_IMAGE_H:
            logger.warning("Rapor gorseli cok uzun: %dpx, %dpx'e kisilacak", total_h, MAX_IMAGE_H)
            total_h = MAX_IMAGE_H

        del tmp_img, tmp_draw

        # ── Asıl görseli oluştur ──
        img = Image.new("RGB", (WIDTH, total_h), BG_COLOR)
        draw = ImageDraw.Draw(img)

        y = 0

        # ── Banner ──
        if banner_img:
            img.paste(banner_img.convert("RGB"), (0, 0))
            y = BANNER_H
            # Banner altına ince gradient çizgi
            for i in range(4):
                alpha = 80 - i * 20
                c = min(255, DIVIDER[0] + alpha), min(255, DIVIDER[1] + alpha), min(255, DIVIDER[2] + alpha)
                draw.line([(0, y + i), (WIDTH, y + i)], fill=c)
            y += 6
        else:
            y = 10

        # ── Hook başlık ──
        if hook_lines_wrapped:
            y += 8
            for hl in hook_lines_wrapped:
                draw.text((PAD, y), hl, fill=GOLD, font=font_hook)
                y += 36
            y += 12

        # ── Bölümler ──
        for sec in parsed["sections"]:
            # Taşma koruması: yeterli alan kalmadıysa dur
            if y + 80 > total_h:
                break

            # Divider
            y += 8
            draw.line([(PAD, y), (WIDTH - PAD, y)], fill=DIVIDER, width=2)
            y += 14

            # Accent çubuk (tam bölüm yüksekliği boyunca çizilecek)
            accent_top = y

            # Başlık
            draw.text((PAD + ACCENT_W + ACCENT_PAD, y), sec["title"], fill=WHITE, font=font_title)
            y += 30 + 8

            # Body lines
            body_x = PAD + ACCENT_W + ACCENT_PAD
            body_w = CONTENT_W - ACCENT_W - ACCENT_PAD

            sec_line_count = 0
            for line in sec["lines"]:
                if sec_line_count >= MAX_BODY_LINES:
                    break
                is_bullet = line.startswith("•") or line.startswith("-")
                indent = 20 if is_bullet else 0
                available_w = body_w - indent

                wrapped = _wrap_text(line, font_body, available_w, draw)
                # Tek body line max 4 wrap satırı
                if len(wrapped) > 4:
                    wrapped = wrapped[:4]
                    wrapped[-1] = wrapped[-1][:max(0, len(wrapped[-1])-3)] + "..."
                for j, wl in enumerate(wrapped):
                    if y + LINE_H > total_h - 100:  # Footer'a yer bırak
                        break
                    line_color = GRAY
                    used_font = font_body

                    # Bullet ilk satırı — bullet karakteri beyaz
                    if j == 0 and is_bullet:
                        bullet_char = line[0]
                        draw.text((body_x + 4, y), bullet_char, fill=WHITE, font=font_body_bold)
                        rest = wl[2:] if len(wl) > 2 else wl
                        draw.text((body_x + indent, y), rest, fill=GRAY, font=font_body)
                    else:
                        draw.text((body_x + indent, y), wl, fill=line_color, font=used_font)
                    y += LINE_H
                    sec_line_count += 1

            accent_bottom = y

            # Sol accent çubuk çiz (bölüm başından sonuna)
            draw.rectangle(
                [(PAD, accent_top), (PAD + ACCENT_W, accent_bottom)],
                fill=sec["color"],
            )

            y += SECTION_GAP

        # ── Footer ──
        y += 8
        draw.line([(PAD, y), (WIDTH - PAD, y)], fill=DIVIDER, width=2)
        y += 16

        for fl in parsed["footer_lines"]:
            if "yatirim tavsiyesi" in fl.lower() or "yatırım tavsiyesi" in fl.lower():
                draw.text((PAD, y), fl, fill=GRAY, font=font_footer)
            elif "szalgo" in fl.lower():
                draw.text((PAD, y), fl, fill=GOLD, font=font_footer_bold)
            else:
                # Soru satırı
                draw.text((PAD, y), fl, fill=GOLD, font=font_footer)
            y += 24

        # ── Watermark ──
        _draw_bg_watermark(img, WIDTH, total_h)

        # ── Kaydet ──
        ts = datetime.now(_TR_TZ).strftime("%Y%m%d_%H%M%S")
        rtype = "acilis" if report_type == "morning" else "kapanis"
        filename = f"rapor_{rtype}_{ts}.png"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)
        del img, draw
        gc.collect()
        logger.info("Rapor gorseli olusturuldu: %s (%.1f KB)", filepath, os.path.getsize(filepath) / 1024)
        return filepath

    except Exception as e:
        logger.error("generate_report_image hatasi: %s", e, exc_info=True)
        return None


# ═════════════════════════════════════════════════════════════════
# SPK BÜLTEN ANALİZ GÖRSELİ (kapanış raporu tarzı dinamik PNG)
# ═════════════════════════════════════════════════════════════════

# SPK bülten bölüm renkleri
_SPK_SECTION_COLORS = {
    "halka arz": GREEN,
    "sermaye art": GOLD,
    "idari para": RED,
    "önemli gelişme": ORANGE,
    "diğer": ORANGE,
    "pay alım": ORANGE,
    "default": GRAY,
}


def _get_spk_section_color(title: str) -> tuple:
    """Bölüm başlığına göre accent rengi döndürür."""
    t = title.lower()
    for key, color in _SPK_SECTION_COLORS.items():
        if key in t:
            return color
    return _SPK_SECTION_COLORS["default"]


def _parse_spk_bulletin_sections(ai_text: str) -> dict:
    """AI ürettiği SPK bülten metnini bölümlere ayırır.

    Returns:
        {
            "bulletin_title": str,
            "sections": [{"title": str, "color": (r,g,b), "lines": [str]}, ...],
            "footer_note": str,
        }
    """
    import re

    lines = ai_text.strip().split("\n")
    sections = []
    current_section = None
    bulletin_title = ""
    footer_note = ""

    # Emoji başlık pattern'leri
    _SECTION_EMOJIS = ["🚀", "💰", "💵", "📊", "📈", "⚖️", "🏛", "🔔", "📋",
                       "🏢", "🔍", "⚠️", "🎯", "📌", "🔴", "🟢", "💼", "🏗"]

    # Bilinen bölüm başlıkları (düz metin eşleşme — emoji/bold olmadan da yakala)
    _KNOWN_HEADERS = [
        "halka arz onay", "halka arz", "sermaye art", "bedelli sermaye",
        "bedelsiz sermaye", "idari para ceza", "para cezalar",
        "diger onemli", "diğer önemli", "önemli gelişme", "onemli gelisme",
        "pay alim teklif", "pay alım teklif", "site yasak",
        "piyasa tedbir",
    ]

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Footer/not satırı
        s_lower = stripped.lower().replace("İ", "i").replace("ı", "i")
        if s_lower.startswith("not:") or s_lower.startswith("bu bulten") or s_lower.startswith("bu bülten"):
            footer_note = stripped.replace("**", "")
            footer_note = re.sub(r'[\U0001F300-\U0001F9FF\u2600-\u26FF\u2700-\u27BF\u200d]', '', footer_note).strip()
            continue

        # Bülten başlığı (SPK Bülteni 2026/16 Analizi gibi)
        if ("spk" in s_lower or "bülten" in stripped.lower()) and ("bulten" in s_lower or "bülten" in stripped.lower()) and not bulletin_title:
            bulletin_title = re.sub(r'[\U0001F300-\U0001F9FF\u2600-\u26FF\u2700-\u27BF]', '', stripped)
            bulletin_title = bulletin_title.replace("**", "").strip()
            continue

        # Section header kontrolü — emoji ile başlayan
        is_header = False
        for em in _SECTION_EMOJIS:
            if stripped.startswith(em):
                is_header = True
                break

        # **Bold** header kontrolü
        if not is_header and stripped.startswith("**") and "**" in stripped[2:]:
            inner = stripped.replace("**", "").strip()
            if len(inner) < 60 and not inner.startswith("•") and not inner.startswith("-") and not inner.startswith("#"):
                is_header = True

        # Düz metin header kontrolü — bilinen başlıkları yakala
        if not is_header and not stripped.startswith("•") and not stripped.startswith("-"):
            clean_lower = stripped.replace("**", "").lower()
            for kh in _KNOWN_HEADERS:
                if kh in clean_lower and len(stripped) < 60:
                    is_header = True
                    break

        if is_header:
            # Önceki section'ı kaydet
            if current_section and current_section["lines"]:
                sections.append(current_section)

            # Başlık temizle
            title = stripped
            title = re.sub(r'[\U0001F300-\U0001F9FF\U0001F1E0-\U0001F1FF\u2600-\u26FF\u2700-\u27BF\u200d]', '', title)
            title = title.replace("**", "").strip()
            if not title:
                title = "Diğer Gelişmeler"

            current_section = {
                "title": title,
                "color": _get_spk_section_color(title),
                "lines": [],
            }
        elif current_section is not None:
            # Body satırı — temizle
            clean = stripped.replace("**", "")
            clean = re.sub(r'[\U0001F300-\U0001F9FF\U0001F1E0-\U0001F1FF\u2600-\u26FF\u2700-\u27BF\u200d]', '', clean).strip()
            # Resimde # işareti olmasın — hashtag sadece tweet metninde
            clean = re.sub(r'#([A-Za-z])', r'\1', clean)
            if clean:
                current_section["lines"].append(clean)

    if current_section and current_section["lines"]:
        sections.append(current_section)

    return {
        "bulletin_title": bulletin_title,
        "sections": sections,
        "footer_note": footer_note,
    }


def generate_spk_bulletin_image(ai_text: str, bulletin_no: str) -> Optional[str]:
    """SPK bülten analizini kapanış raporu tarzı PNG'ye çevirir.

    Args:
        ai_text: AI'ın ürettiği analiz metni
        bulletin_no: Bülten numarası (orn: "2026/16")

    Returns:
        PNG dosya yolu veya None
    """
    try:
        parsed = _parse_spk_bulletin_sections(ai_text)

        if not parsed["sections"]:
            logger.warning("SPK bulten gorsel: section bulunamadi")
            return None

        # Max 8 section, section başına max 6 satır
        if len(parsed["sections"]) > 8:
            parsed["sections"] = parsed["sections"][:8]
        for sec in parsed["sections"]:
            if len(sec["lines"]) > 6:
                sec["lines"] = sec["lines"][:6]

        # ── Fontlar ──
        font_header = _load_font(36, bold=True)
        font_subheader = _load_font(20, bold=False)
        font_title = _load_font(22, bold=True)
        font_body = _load_font(17, bold=False)
        font_footer = _load_font(15, bold=False)
        font_footer_bold = _load_font(15, bold=True)

        WIDTH = 1200
        PAD = 50
        CONTENT_W = WIDTH - PAD * 2
        SECTION_GAP = 18
        LINE_H = 24
        ACCENT_W = 5
        ACCENT_PAD = 14

        MAX_BODY_LINES = 8
        MAX_IMAGE_H = 2400

        # ── Banner görseli yükle ──
        _img_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
        _banner_path = os.path.join(_img_dir, "spk_bulten_banner.png")
        banner_img = None
        HEADER_H = 120  # fallback

        if os.path.exists(_banner_path):
            try:
                banner_img = Image.open(_banner_path)
                # 1200px genişliğe sığdır
                ratio = WIDTH / banner_img.width
                new_h = int(banner_img.height * ratio)
                banner_img = banner_img.resize((WIDTH, new_h), Image.LANCZOS)
                HEADER_H = new_h
            except Exception as _be:
                logger.warning("SPK banner yuklenemedi: %s", _be)
                banner_img = None

        # ── Yükseklik hesapla ──
        tmp_img = Image.new("RGB", (WIDTH, 100))
        tmp_draw = ImageDraw.Draw(tmp_img)

        y_calc = HEADER_H + 20  # Header sonrası

        for sec in parsed["sections"]:
            y_calc += 8 + 2 + 12  # divider
            y_calc += 28 + 8      # başlık

            body_w = CONTENT_W - ACCENT_W - ACCENT_PAD
            sec_lines = 0
            for line in sec["lines"]:
                if sec_lines >= MAX_BODY_LINES:
                    break
                indent = 20 if (line.startswith("•") or line.startswith("-")) else 0
                wrapped = _wrap_text(line, font_body, body_w - indent, tmp_draw)
                if len(wrapped) > 3:
                    wrapped = wrapped[:3]
                y_calc += len(wrapped) * LINE_H
                sec_lines += len(wrapped)
            y_calc += SECTION_GAP

        # Footer
        y_calc += 16 + 2 + 16  # divider
        if parsed["footer_note"]:
            footer_wrapped = _wrap_text(parsed["footer_note"], font_footer, CONTENT_W, tmp_draw)
            y_calc += len(footer_wrapped) * 20
        y_calc += 44  # disclaimer + branding

        total_h = min(y_calc + 20, MAX_IMAGE_H)
        del tmp_img, tmp_draw

        # ── Görseli oluştur ──
        img = Image.new("RGB", (WIDTH, total_h), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # ── Header ──
        from datetime import datetime as _dt

        if banner_img:
            # Banner görseli yapıştır
            img.paste(banner_img.convert("RGB"), (0, 0))
            # Bülten numarasını banner üstüne yaz ("Sermaye Piyasası Kurulu" yanına)
            font_bno = _load_font(18, bold=False)
            _AYLAR = ["Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran",
                      "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"]
            _now = _dt.now()
            _tarih = f"{_now.day} {_AYLAR[_now.month - 1]} {_now.year}"
            bno_text = f"Bülten {bulletin_no}  •  {_tarih}"
            # Sermaye Piyasası Kurulu yazısının yanına/altına — banner'ın sol alt köşesi
            draw.text((PAD, HEADER_H - 35), bno_text, fill=GOLD, font=font_bno)
        else:
            # Fallback: gradient header
            for row_y in range(HEADER_H):
                ratio = row_y / HEADER_H
                r = int(18 + (30 - 18) * ratio)
                g = int(18 + (40 - 18) * ratio)
                b = int(32 + (70 - 32) * ratio)
                draw.line([(0, row_y), (WIDTH, row_y)], fill=(r, g, b))
            draw.line([(0, HEADER_H - 2), (WIDTH, HEADER_H - 2)], fill=GOLD, width=2)
            draw.text((PAD, 25), "SPK BÜLTENİ ANALİZİ", fill=WHITE, font=font_header)
            _AYLAR = ["Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran",
                      "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"]
            _now = _dt.now()
            _tarih = f"{_now.day} {_AYLAR[_now.month - 1]} {_now.year}"
            subtitle = f"Bülten {bulletin_no}  •  {_tarih}  •  BorsaCebimde"
            draw.text((PAD, 72), subtitle, fill=GRAY, font=font_subheader)

        y = HEADER_H + 16

        # ── Bölümler ──
        for sec in parsed["sections"]:
            if y + 60 > total_h:
                break

            # Divider
            y += 8
            draw.line([(PAD, y), (WIDTH - PAD, y)], fill=DIVIDER, width=1)
            y += 12

            # Accent çubuk başlangıcı
            accent_top = y

            # Başlık
            draw.text((PAD + ACCENT_W + ACCENT_PAD, y), sec["title"], fill=WHITE, font=font_title)
            y += 28 + 6

            # Body
            body_x = PAD + ACCENT_W + ACCENT_PAD
            body_w = CONTENT_W - ACCENT_W - ACCENT_PAD
            sec_lines = 0

            for line in sec["lines"]:
                if sec_lines >= MAX_BODY_LINES:
                    break
                is_bullet = line.startswith("•") or line.startswith("-")
                indent = 20 if is_bullet else 0

                wrapped = _wrap_text(line, font_body, body_w - indent, draw)
                if len(wrapped) > 3:
                    wrapped = wrapped[:3]
                    wrapped[-1] = wrapped[-1][:max(0, len(wrapped[-1]) - 3)] + "..."

                for j, wl in enumerate(wrapped):
                    if y + LINE_H > total_h - 80:
                        break
                    if j == 0 and is_bullet:
                        draw.text((body_x + 4, y), line[0], fill=WHITE, font=font_body)
                        rest = wl[2:] if len(wl) > 2 else wl
                        draw.text((body_x + indent, y), rest, fill=GRAY, font=font_body)
                    else:
                        draw.text((body_x + indent, y), wl, fill=GRAY, font=font_body)
                    y += LINE_H
                    sec_lines += 1

            accent_bottom = y

            # Sol accent çubuk
            draw.rectangle(
                [(PAD, accent_top), (PAD + ACCENT_W, accent_bottom)],
                fill=sec["color"],
            )
            y += SECTION_GAP

        # ── Footer ──
        y += 8
        draw.line([(PAD, y), (WIDTH - PAD, y)], fill=DIVIDER, width=1)
        y += 14

        # Footer note (varsa)
        if parsed["footer_note"]:
            footer_wrapped = _wrap_text(parsed["footer_note"], font_footer, CONTENT_W, draw)
            for fl in footer_wrapped:
                draw.text((PAD, y), fl, fill=GRAY, font=font_footer)
                y += 20
            y += 6

        draw.text((PAD, y), "Yatırım tavsiyesi değildir.", fill=GRAY, font=font_footer)
        y += 20
        draw.text((PAD, y), "BorsaCebimde", fill=GOLD, font=font_footer_bold)

        # ── Watermark ──
        _draw_bg_watermark(img, WIDTH, total_h)

        # ── Kaydet ──
        ts = _dt.now().strftime("%Y%m%d_%H%M%S")
        filename = f"spk_bulten_{bulletin_no.replace('/', '_')}_{ts}.png"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        img.save(filepath, "PNG", optimize=True)
        del img, draw
        gc.collect()
        logger.info("SPK bulten gorseli olusturuldu: %s (%.1f KB)", filepath, os.path.getsize(filepath) / 1024)
        return filepath

    except Exception as e:
        logger.error("generate_spk_bulletin_image hatasi: %s", e, exc_info=True)
        try:
            from app.services.admin_telegram import send_admin_message
            import asyncio
            asyncio.get_event_loop().run_until_complete(
                send_admin_message(f"⚠️ SPK Bülten Görsel Hatası:\n{str(e)[:300]}")
            )
        except Exception:
            pass
        return None
