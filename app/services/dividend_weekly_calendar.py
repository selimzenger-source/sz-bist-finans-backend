"""Haftalık Temettü Takvimi — görsel + tweet.

Her Pazar 18:00 TR: önümüzdeki haftanın (Pzt–Cuma, yalnızca BIST işlem günleri)
temettü ödemelerini temettühisseleri.com verisinden (dividend_history.payment_date
+ gross_dividend_per_share) derler, marka konseptinde görsel üretir ve tweet atar.

Koşullar:
  - Tweet ATILIR yalnızca o hafta temettü ödeyecek hisse sayısı >= 3 ise.
  - BIST tatili olan günler takvimde GÖSTERİLMEZ (işlem günü değil).
  - İşlem günü olup ödeme olmayan günler "ödeme yok" olarak BOŞ gösterilir.
  - Tweet öncesi temettühisseleri.com verisi tazelenir (en güncel veriyle çalış).
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import text as sa_text

from app.utils.bist_holidays import is_trading_day

logger = logging.getLogger(__name__)

_TR_TZ = timezone(timedelta(hours=3))

_TR_MONTHS = {
    1: "Ocak", 2: "Şubat", 3: "Mart", 4: "Nisan", 5: "Mayıs", 6: "Haziran",
    7: "Temmuz", 8: "Ağustos", 9: "Eylül", 10: "Ekim", 11: "Kasım", 12: "Aralık",
}
_TR_DAYS = {
    0: "Pazartesi", 1: "Salı", 2: "Çarşamba", 3: "Perşembe",
    4: "Cuma", 5: "Cumartesi", 6: "Pazar",
}

# ── Minimum hisse koşulu ──
MIN_STOCKS_FOR_TWEET = 3


# ════════════════════════════════════════════════════════════════════════════
#  TARİH / VERİ
# ════════════════════════════════════════════════════════════════════════════

def _today_tr() -> date:
    return datetime.now(_TR_TZ).date()


def next_week_range(today: date | None = None) -> tuple[date, date]:
    """Bir sonraki haftanın Pazartesi–Cuma aralığı.

    Pazar akşamı çalıştığında 'önümüzdeki hafta' = ertesi gün başlayan hafta.
    """
    d = today or _today_tr()
    # Bu haftanın pazartesisi
    this_monday = d - timedelta(days=d.weekday())
    next_monday = this_monday + timedelta(days=7)
    next_friday = next_monday + timedelta(days=4)
    return next_monday, next_friday


def this_week_range(today: date | None = None) -> tuple[date, date]:
    """İçinde bulunulan haftanın Pazartesi–Cuma aralığı (geçen/biten hafta).

    Pazar akşamı çalıştığında 'bu hafta' = az önce biten Pzt–Cuma.
    """
    d = today or _today_tr()
    this_monday = d - timedelta(days=d.weekday())
    this_friday = this_monday + timedelta(days=4)
    return this_monday, this_friday


async def get_week_dividends(start: date, end: date) -> list[dict]:
    """Verilen hafta için işlem günü bazında temettü ödemelerini döndürür.

    Döner: [{"date": date, "is_trading": True, "items": [{"ticker","gross"}...]}, ...]
    Sadece BIST işlem günleri (tatil/hafta sonu hariç) listelenir; ödeme olmayan
    işlem günleri boş items ile yer alır.
    """
    from app.database import async_session

    rows_by_date: dict[date, list[dict]] = {}
    async with async_session() as session:
        res = await session.execute(
            sa_text(
                """
                SELECT payment_date, UPPER(ticker) AS ticker,
                       MAX(gross_dividend_per_share) AS gross
                FROM dividend_history
                WHERE payment_date BETWEEN :s AND :e
                  AND gross_dividend_per_share IS NOT NULL
                  AND gross_dividend_per_share > 0
                GROUP BY payment_date, UPPER(ticker)
                ORDER BY payment_date, ticker
                """
            ),
            {"s": start, "e": end},
        )
        for pd, ticker, gross in res.all():
            if pd is None:
                continue
            rows_by_date.setdefault(pd, []).append(
                {"ticker": ticker, "gross": Decimal(str(gross))}
            )

    out: list[dict] = []
    cur = start
    while cur <= end:
        if is_trading_day(cur):
            items = sorted(rows_by_date.get(cur, []), key=lambda x: x["ticker"])
            out.append({"date": cur, "is_trading": True, "items": items})
        cur += timedelta(days=1)
    return out


def count_week_stocks(week: list[dict]) -> int:
    """Hafta boyunca temettü ödeyecek benzersiz hisse sayısı."""
    s: set[str] = set()
    for day in week:
        for it in day["items"]:
            s.add(it["ticker"])
    return len(s)


def _balanced_chunks(items: list, max_per: int = 6) -> list[list]:
    """Listeyi her biri <= max_per olacak şekilde DENGELİ parçalara böler.
    10 → [5,5] · 7 → [4,3] · 11 → [6,5] · 13 → [5,4,4] · 6 → [6]."""
    n = len(items)
    if n == 0:
        return []
    k = -(-n // max_per)  # ceil(n / max_per)
    base, rem = divmod(n, k)
    out, idx = [], 0
    for i in range(k):
        size = base + (1 if i < rem else 0)
        out.append(items[idx:idx + size])
        idx += size
    return out


_CURRENCY_CACHE: dict[str, str] = {}


def _currency_token() -> str:
    """Para birimi simgesi. Render/Windows fontu ₺ (U+20BA) destekliyorsa onu,
    desteklemiyorsa 'TL' döndürür (tofu/kutu kareyi önler)."""
    if "tok" in _CURRENCY_CACHE:
        return _CURRENCY_CACHE["tok"]
    tok = "TL"
    try:
        from app.services.chart_image_generator import _load_font
        f = _load_font(24, bold=True)
        path = getattr(f, "path", None)
        if path:
            from fontTools.ttLib import TTFont
            tt = TTFont(path)
            if any(0x20BA in t.cmap for t in tt["cmap"].tables):
                tok = "₺"
    except Exception:
        tok = "TL"
    _CURRENCY_CACHE["tok"] = tok
    return tok


def _fmt_money(v) -> str:
    """1.5984 -> '1,60 ₺' (Türkçe ondalık virgül + para simgesi)."""
    try:
        num = f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        num = str(v)
    return f"{num} {_currency_token()}"


def _fmt_tl(v: Decimal) -> str:
    """0.0544 -> '0,05 TL' (Türkçe ondalık virgülü, 2 hane)."""
    try:
        return f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + " TL"
    except Exception:
        return f"{v} TL"


def week_label(start: date, end: date) -> str:
    if start.month == end.month:
        return f"{start.day} - {end.day} {_TR_MONTHS[start.month]} {start.year}"
    return f"{start.day} {_TR_MONTHS[start.month]} - {end.day} {_TR_MONTHS[end.month]} {end.year}"


# ════════════════════════════════════════════════════════════════════════════
#  GÖRSEL (marka konsepti — koyu zemin + altın vurgu + logo + watermark)
# ════════════════════════════════════════════════════════════════════════════

def generate_weekly_calendar_image(week: list[dict], label: str) -> str | None:
    """Haftalık temettü takvimi görselini üretir, PNG dosya yolu döner."""
    try:
        from PIL import Image, ImageDraw
        from app.services.chart_image_generator import (
            _load_font, _draw_bg_watermark, draw_brand_footer,
            BG_COLOR, HEADER_BG, WHITE, GRAY, GOLD, GREEN, DIVIDER, ORANGE,
        )

        W = 1080
        PAD = 48
        header_h = 196
        # Gün kartı yükseklikleri
        day_head_h = 64
        row_h = 56
        day_gap = 22
        empty_h = 50

        # Yükseklik hesabı
        body_h = 0
        for day in week:
            n = len(day["items"])
            body_h += day_head_h + (row_h * n if n else empty_h) + day_gap
        footer_h = 96
        H = header_h + body_h + footer_h + PAD

        img = Image.new("RGB", (W, H), BG_COLOR)
        d = ImageDraw.Draw(img)

        f_title = _load_font(52, bold=True)
        f_sub = _load_font(30, bold=False)
        f_day = _load_font(30, bold=True)
        f_dow = _load_font(24, bold=False)
        f_tk = _load_font(34, bold=True)
        f_amt = _load_font(34, bold=True)
        f_empty = _load_font(26, bold=False)
        f_foot = _load_font(24, bold=False)

        # ── Header ──
        d.rectangle([(0, 0), (W, header_h)], fill=HEADER_BG)
        d.rectangle([(0, header_h - 5), (W, header_h)], fill=GOLD)

        # Logo (sol üst)
        _IMG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
        logo_x = PAD
        try:
            for ln in ("logo.png", "logo.jpg"):
                lp = os.path.join(_IMG_DIR, ln)
                if os.path.exists(lp):
                    logo = Image.open(lp).convert("RGBA").resize((96, 96), Image.LANCZOS)
                    img.paste(logo, (PAD, 30), logo)
                    logo_x = PAD + 96 + 24
                    break
        except Exception:
            logo_x = PAD

        d.text((logo_x, 44), "HAFTALIK TEMETTÜ TAKVİMİ", font=f_title, fill=WHITE)
        d.text((logo_x, 112), label, font=f_sub, fill=GOLD)

        # ── Gövde ──
        y = header_h + 18
        for day in week:
            dd = day["date"]
            dow = _TR_DAYS[dd.weekday()]
            date_str = f"{dd.day} {_TR_MONTHS[dd.month]}"

            # Gün başlık çubuğu
            d.rectangle([(PAD, y), (W - PAD, y + day_head_h)], fill=HEADER_BG)
            d.rectangle([(PAD, y), (PAD + 8, y + day_head_h)], fill=GOLD)
            d.text((PAD + 28, y + 16), date_str, font=f_day, fill=WHITE)
            # gün adı sağa
            dow_w = d.textlength(dow, font=f_dow)
            d.text((W - PAD - dow_w - 24, y + 20), dow, font=f_dow, fill=GRAY)
            y += day_head_h

            if not day["items"]:
                d.text((PAD + 28, y + 12), "Temettü ödemesi yok", font=f_empty, fill=GRAY)
                y += empty_h
            else:
                for i, it in enumerate(day["items"]):
                    row_bg = (22, 22, 38) if i % 2 == 0 else (26, 26, 46)
                    d.rectangle([(PAD, y), (W - PAD, y + row_h)], fill=row_bg)
                    d.text((PAD + 28, y + 12), f"#{it['ticker']}", font=f_tk, fill=GREEN)
                    amt = _fmt_tl(it["gross"])
                    amt_w = d.textlength(amt, font=f_amt)
                    d.text((W - PAD - amt_w - 24, y + 12), amt, font=f_amt, fill=GOLD)
                    y += row_h
            y += day_gap

        # ── Footer (EK-1 ortak marka şeridi) ──
        _draw_bg_watermark(img, W, H)
        draw_brand_footer(d, img, W, H, source="Veri: temettühisseleri.com")

        out_path = os.path.join(
            tempfile.gettempdir(),
            f"temettu_takvim_{datetime.now(_TR_TZ).strftime('%Y%m%d')}.png",
        )
        img.save(out_path, "PNG", optimize=True)
        logger.info("Haftalık temettü takvimi görseli üretildi: %s", out_path)
        return out_path
    except Exception as e:
        logger.exception("Haftalık temettü takvimi görsel hatası: %s", e)
        return None


# ════════════════════════════════════════════════════════════════════════════
#  GRAFİKLİ KART VERSİYONU — her hisse: net/brüt/tarih + yıllık mini bar grafik
# ════════════════════════════════════════════════════════════════════════════

async def get_week_dividend_cards(start: date, end: date) -> list[dict]:
    """Hafta içi temettü ödeyecek hisseler — net/brüt/tarih + yıllık geçmiş (mini chart).

    Döner: [{"ticker","date","gross","net","history":[(year, gross_float)...]}...]
    """
    from app.database import async_session
    cards: list[dict] = []
    async with async_session() as session:
        # TICKER bazında TEK kart (aynı hafta birden fazla ödeme tarihi olsa bile
        # duplicate kart üretme — en erken ödeme tarihi + en yüksek pay gösterilir).
        res = await session.execute(sa_text(
            """
            SELECT UPPER(ticker) AS ticker,
                   MIN(payment_date) AS pd,
                   MAX(gross_dividend_per_share) AS g,
                   MAX(net_dividend_per_share) AS n,
                   MAX(dividend_yield_pct) AS y
            FROM dividend_history
            WHERE payment_date BETWEEN :s AND :e AND gross_dividend_per_share > 0
            GROUP BY UPPER(ticker)
            ORDER BY MIN(payment_date), UPPER(ticker)
            """
        ), {"s": start, "e": end})
        base = [(tk, pd, g, n, y) for tk, pd, g, n, y in res.all() if pd and tk]
        for tk, pd, g, n, y in base:
            hres = await session.execute(sa_text(
                """
                SELECT payment_year, MAX(gross_dividend_per_share) AS g
                FROM dividend_history
                WHERE UPPER(ticker) = :tk AND gross_dividend_per_share > 0
                  AND payment_year IS NOT NULL
                GROUP BY payment_year ORDER BY payment_year
                """
            ), {"tk": tk})
            hist = [(int(y), float(gg)) for y, gg in hres.all() if gg is not None]
            cards.append({
                "ticker": tk, "date": pd,
                "gross": float(g), "net": float(n) if n is not None else None,
                "verim": float(y) if y is not None else None,
                "history": hist,
            })
    return cards


def generate_weekly_calendar_cards_image(sections: list[dict], label: str,
                                         suffix: str = "") -> str | None:
    """Bölümlü grafikli temettü görseli.

    sections: [{"title": str, "color": (r,g,b)|None, "cards": [card...]}]
      card: {"ticker","date","gross","net","verim","history":[(yıl,brüt)...]}
    Her kart: #kod, ödeme tarihi, brüt/net pay, verim%, yıllık brüt mini bar grafik.
    suffix: çıktı dosya adına eklenir (aynı gün 2 görsel üretirken çakışmayı önler).
    """
    sections = [s for s in (sections or []) if s.get("cards")]
    if not sections:
        return None
    try:
        from PIL import Image, ImageDraw
        from app.services.chart_image_generator import (
            _load_font, _draw_bg_watermark, draw_brand_footer,
            BG_COLOR, HEADER_BG, WHITE, GRAY, GOLD, GREEN, DIVIDER,
        )
        W, PAD, GAP, COLS = 1080, 44, 24, 2
        col_w = (W - 2 * PAD - GAP * (COLS - 1)) // COLS
        card_h = 396
        sec_head_h, sec_gap = 56, 18
        header_h, footer_h = 196, 96

        body_h = 0
        for s in sections:
            rws = (len(s["cards"]) + COLS - 1) // COLS
            body_h += sec_head_h + rws * (card_h + GAP) + sec_gap
        H = header_h + 18 + body_h + footer_h

        img = Image.new("RGB", (W, H), BG_COLOR)
        d = ImageDraw.Draw(img)
        f_title = _load_font(50, bold=True)
        f_sub = _load_font(30, bold=False)
        f_sec = _load_font(30, bold=True)
        f_tk = _load_font(36, bold=True)
        f_date = _load_font(26, bold=True)
        f_amt = _load_font(27, bold=True)
        f_amt2 = _load_font(24, bold=False)
        f_bar = _load_font(17, bold=True)
        f_yr = _load_font(16, bold=False)
        f_cap = _load_font(18, bold=False)

        # Header
        d.rectangle([(0, 0), (W, header_h)], fill=HEADER_BG)
        d.rectangle([(0, header_h - 5), (W, header_h)], fill=GOLD)
        _IMG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
        logo_x = PAD
        try:
            for ln in ("logo.png", "logo.jpg"):
                lp = os.path.join(_IMG_DIR, ln)
                if os.path.exists(lp):
                    logo = Image.open(lp).convert("RGBA").resize((92, 92), Image.LANCZOS)
                    img.paste(logo, (PAD, 32), logo)
                    logo_x = PAD + 92 + 22
                    break
        except Exception:
            pass
        d.text((logo_x, 46), "HAFTALIK TEMETTÜ TAKVİMİ", font=f_title, fill=WHITE)
        d.text((logo_x, 112), label, font=f_sub, fill=GOLD)

        _tl = _fmt_money  # ₺ simgesi (font destekliyorsa) + Türkçe ondalık

        def _draw_card(x, y, c):
            d.rounded_rectangle([(x, y), (x + col_w, y + card_h)], radius=16,
                                fill=(24, 24, 40), outline=DIVIDER, width=1)
            d.text((x + 22, y + 18), f"#{c['ticker']}", font=f_tk, fill=GREEN)
            ds = f"{c['date'].day} {_TR_MONTHS[c['date'].month]}"
            dw = d.textlength(ds, font=f_date)
            d.text((x + col_w - 22 - dw, y + 26), ds, font=f_date, fill=GOLD)
            d.text((x + 22, y + 66), f"Brüt/pay: {_tl(c['gross'])}", font=f_amt, fill=WHITE)
            if c.get("net") is not None:
                d.text((x + 22, y + 98), f"Net/pay:  {_tl(c['net'])}", font=f_amt2, fill=GRAY)
            if c.get("verim") is not None:
                vtxt = f"Verim: %{c['verim']:.2f}".replace(".", ",")
                d.text((x + 22, y + 126), vtxt, font=f_amt2, fill=GOLD)
            # Mini bar grafik — SABİT 6 yıllık eksen (örn 2021-2026).
            # Veri olmayan yıllar BOŞ (0) gösterilir → tek/iki barlık çorak görünüm yok.
            cur_year = datetime.now(_TR_TZ).year
            years_axis = list(range(cur_year - 5, cur_year + 1))  # 6 yıl
            hmap = {int(yy): float(gg) for yy, gg in (c.get("history") or [])}
            hist = [(yy, hmap.get(yy, 0.0)) for yy in years_axis]
            cap_y = y + 168
            d.text((x + 22, cap_y),
                   f"Yıllık brüt temettü · {years_axis[0]}-{years_axis[-1]} ({_currency_token()}/pay)",
                   font=f_cap, fill=GRAY)
            ch_x0, ch_y0 = x + 22, cap_y + 30
            ch_w, ch_h = col_w - 44, 152
            # Üstte değer etiketi için pay, altta yıl etiketi için pay bırakılır →
            # barlar bu pay kadar küçülür, yazılar barlarla çakışmaz.
            TOP_PAD, BOT_PAD = 26, 26
            base_y = ch_y0 + ch_h - BOT_PAD          # bar tabanı (yıl yazısı bunun altında)
            bar_area = ch_h - TOP_PAD - BOT_PAD       # barların kullanabileceği yükseklik
            mx = max((g for _, g in hist), default=0.0) or 1.0
            nb = len(hist)
            bw = (ch_w - (nb - 1) * 10) / nb
            for i, (yr, g) in enumerate(hist):
                bx = ch_x0 + i * (bw + 10)
                last = (i == nb - 1)
                if g > 0:
                    bh = max((g / mx) * bar_area, 3)
                    by = base_y - bh
                    d.rectangle([(bx, by), (bx + bw, base_y)],
                                fill=GREEN if last else (40, 90, 60))
                    vs = f"{g:.2f}".rstrip("0").rstrip(".").replace(".", ",")
                    vw = d.textlength(vs, font=f_bar)
                    d.text((bx + bw / 2 - vw / 2, by - 21), vs, font=f_bar,
                           fill=GOLD if last else GRAY)
                else:
                    # boş yıl — ince taban çizgisi (0)
                    d.line([(bx, base_y), (bx + bw, base_y)], fill=DIVIDER, width=2)
                ys = "'" + str(yr)[2:]
                yw = d.textlength(ys, font=f_yr)
                d.text((bx + bw / 2 - yw / 2, base_y + 5), ys, font=f_yr, fill=GRAY)

        y = header_h + 18
        for s in sections:
            color = s.get("color") or GOLD
            d.rectangle([(PAD, y), (W - PAD, y + sec_head_h)], fill=HEADER_BG)
            d.rectangle([(PAD, y), (PAD + 8, y + sec_head_h)], fill=color)
            d.text((PAD + 26, y + 14), s["title"], font=f_sec, fill=color)
            y += sec_head_h
            cards = s["cards"]
            for idx, c in enumerate(cards):
                col = idx % COLS
                r = idx // COLS
                _draw_card(PAD + col * (col_w + GAP), y + r * (card_h + GAP), c)
            rws = (len(cards) + COLS - 1) // COLS
            y += rws * (card_h + GAP) + sec_gap

        _draw_bg_watermark(img, W, H)
        draw_brand_footer(d, img, W, H, center=True)  # logo + marka ortalı, kaynak yok
        out_path = os.path.join(
            tempfile.gettempdir(),
            f"temettu_takvim_kart_{datetime.now(_TR_TZ).strftime('%Y%m%d')}{('_' + suffix) if suffix else ''}.png",
        )
        img.save(out_path, "PNG", optimize=True)
        logger.info("Haftalık temettü kart görseli üretildi: %s (%d bölüm)", out_path, len(sections))
        return out_path
    except Exception as e:
        logger.exception("Haftalık temettü kart görsel hatası: %s", e)
        return None


# ════════════════════════════════════════════════════════════════════════════
#  TWEET METNİ
# ════════════════════════════════════════════════════════════════════════════

def build_tweet_text(week: list[dict], label: str, total: int) -> str:
    """Kısa tweet metni — detay görselde, metin başlık + hashtag."""
    # Hashtag için hisse kodları (max 10)
    tickers: list[str] = []
    for day in week:
        for it in day["items"]:
            if it["ticker"] not in tickers:
                tickers.append(it["ticker"])
    ticker_tags = " ".join(f"#{t}" for t in tickers[:10])

    lines = [
        "📅 Haftalık Temettü Takvimi",
        "",
        "🟢 Bu hafta temettü ödeyen şirketler",
        "🟡 Önümüzdeki hafta ödeyecek şirketler",
        "",
        "Brüt/net pay, ödeme tarihi, verim ve yıllık temettü grafikleri görsellerde 👇",
        "",
        f"#temettü #BIST100 #borsa #hisse #yatırım {ticker_tags}".strip(),
    ]
    return "\n".join(lines)


def _net_num(c: dict) -> str | None:
    if c.get("net") is None:
        return None
    try:
        return f"{float(c['net']):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return None


def build_tweet_text_cards(paid_cards: list[dict], upcoming_cards: list[dict],
                           p_label: str, u_label: str) -> str:
    """Detaylı tweet metni — her hisse ayrı satırda Brüt/Net pay + temiz boşluklar.

    Örn:  #AYES · Brüt 0,30 ₺ · Net 0,26 ₺
    """
    sym = _currency_token()

    def _line(c: dict) -> str:
        g = _fmt_money(c["gross"])  # "0,30 ₺"
        n = _net_num(c)
        if n is not None:
            return f"#{c['ticker']} · Brüt {g} · Net {n} {sym}"
        return f"#{c['ticker']} · Brüt {g}"

    lines: list[str] = ["📅 Haftalık Temettü Takvimi", ""]
    if paid_cards:
        lines.append(f"🟢 Bu hafta ödeyenler · {p_label}")
        lines.append("")
        lines += [_line(c) for c in paid_cards]
        lines.append("")
    if upcoming_cards:
        lines.append(f"🟡 Önümüzdeki hafta ödeyecekler · {u_label}")
        lines.append("")
        lines += [_line(c) for c in upcoming_cards]
        lines.append("")
    lines.append("📊 Tarih, verim ve yıllık temettü grafikleri görsellerde 👇")
    lines.append("")
    lines.append("#temettü #BIST100 #borsa #hisse #yatırım")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
#  ORKESTRASYON (Pazar 18:00 job)
# ════════════════════════════════════════════════════════════════════════════

async def _refresh_with_healthcheck() -> tuple[bool, str, dict]:
    """temettühisseleri.com scrape'i çalıştır + sağlık değerlendir.

    Döner: (ok, reason, stats)
      ok=False → HARD FAIL (tweet iptal edilmeli): exception | error | processed==0
      ok=True ama reason='degraded' → veri var ama yüksek hata oranı (uyarı atılır,
               tweet devam eder).
    """
    try:
        from app.scrapers.temettuhisseleri_scraper import scrape_temettuhisseleri
        stats = await scrape_temettuhisseleri()
    except Exception as e:
        logger.error("Haftalık takvim: temettü scrape EXCEPTION: %s", e)
        return False, "exception", {"error": str(e)[:300]}

    logger.info("Haftalık takvim öncesi temettü refresh: %s", stats)

    if not isinstance(stats, dict):
        return False, "bad_stats", {"raw": str(stats)[:200]}
    if stats.get("error"):
        return False, "scrape_error", stats

    processed = int(stats.get("processed") or 0)
    errors = int(stats.get("errors") or 0)
    total = int(stats.get("stocks_total") or 0)

    # Hard fail: hiç hisse işlenememiş
    if processed == 0:
        return False, "processed_zero", stats

    # Degraded: hata oranı %40+ (veri var ama güvenilirlik düşük)
    denom = max(total, processed + errors, 1)
    if errors > 0 and (errors / denom) >= 0.40:
        await _alert_scrape_problem("degraded", stats, hard=False)
        return True, "degraded", stats

    return True, "ok", stats


async def _alert_scrape_problem(
    reason: str, stats: dict, *, hard: bool, context: str = "weekly_tweet"
) -> None:
    """Scrape sorununu admin'e Telegram ile bildir.

    context: 'weekly_tweet' (Pazar takvim akışı) veya 'refresh_2h' (düzenli tarama).
    """
    try:
        from app.services.admin_telegram import send_admin_message
        head = "🔴 <b>Temettü Scrape HATASI</b>" if hard else "🟠 <b>Temettü Scrape Uyarısı</b>"
        where = "Pazar haftalık takvim" if context == "weekly_tweet" else "2 saatlik tarama"
        body = (
            f"{head}\n"
            f"Kaynak: temettühisseleri.com ({where})\n"
            f"Durum: <b>{reason}</b>\n"
            f"İşlenen: {stats.get('processed', '?')} · "
            f"Hata: {stats.get('errors', '?')} · "
            f"Toplam: {stats.get('stocks_total', '?')}\n"
        )
        if stats.get("error"):
            body += f"Detay: <code>{str(stats.get('error'))[:200]}</code>\n"
        if hard and context == "weekly_tweet":
            body += "→ Haftalık temettü takvimi tweet'i <b>İPTAL</b> edildi (eksik veri riski)."
        elif hard:
            body += "→ Veri çekilemedi, kontrol et."
        else:
            body += "→ Tarama tamamlandı ama veri güvenilirliği düşük olabilir, kontrol et."
        await send_admin_message(body)
        logger.info("Temettü scrape sorun uyarısı gönderildi (reason=%s, hard=%s)", reason, hard)
    except Exception as e:
        logger.error("Temettü scrape uyarısı gönderilemedi: %s", e)


async def run_weekly_dividend_calendar(*, force: bool = False, dry_run: bool = False,
                                       skip_refresh: bool = False,
                                       custom_text: str | None = None) -> dict:
    """Haftalık temettü takvimi akışı.

    Args:
        force: hisse sayısı < MIN olsa bile devam et (test).
        dry_run: tweet atma, sadece görsel üret + sonucu döndür.
        skip_refresh: scrape healthcheck'i atla (admin manuel gönderim — mevcut
                      DB verisiyle hızlı/garantili çalışır).
        custom_text: verilirse otomatik metin yerine bu metin kullanılır
                     (admin panelde düzenlenebilir tweet).
    """
    start, end = next_week_range()
    label = week_label(start, end)
    p_start, p_end = this_week_range()  # az önce biten hafta (ödeyenler)

    # 1) Veriyi tazele (en güncel temettühisseleri verisi) + SAĞLIK KONTROLÜ
    # ÖNEMLİ: Render datacenter IP'si temettühisseleri.com tarafından zaman zaman
    # ENGELLENİYOR (tüm hisseler fetch hatası → processed=0). Bu durumda SİTE/VERİ
    # bozuk DEĞİL — DB'de zaten geçerli veri var. O yüzden scrape başarısızsa tweet'i
    # İPTAL ETMEK YERİNE mevcut DB verisiyle DEVAM ET, sadece admin'e SOFT uyarı düş.
    # (Tamamen boş/yetersiz veriyi aşağıdaki MIN_STOCKS eşiği zaten yakalar.)
    data_stale = False
    if not dry_run and not skip_refresh:
        scrape_ok, scrape_reason, stats = await _refresh_with_healthcheck()
        if not scrape_ok:
            data_stale = True
            await _alert_scrape_problem(scrape_reason, stats, hard=False)
            logger.warning("Temettü scrape başarısız (%s) — mevcut DB verisiyle devam",
                           scrape_reason)

    # 2) Hafta verisini derle
    week = await get_week_dividends(start, end)
    total = count_week_stocks(week)
    logger.info("Haftalık temettü takvimi (%s): %d hisse (stale=%s)", label, total, data_stale)

    if total < MIN_STOCKS_FOR_TWEET and not force:
        return {
            "sent": False, "reason": "below_threshold",
            "total": total, "min": MIN_STOCKS_FOR_TWEET, "label": label,
        }

    # 3) Görsel — GRAFİKLİ kart versiyonu, 2 AYRI GÖRSEL:
    #    (1) Bu hafta temettü ÖDEYENLER  (az önce biten hafta)
    #    (2) Önümüzdeki hafta ÖDEYECEKLER
    # Her kart: #kod, tarih, brüt/net pay, verim%, yıllık brüt mini bar grafik.
    # Kart üretilemezse eski düz takvim görseline düş (fallback).
    # KURAL: her görselde MAX 6 kart. 6'dan fazlaysa DENGELİ böl (10→5+5, 7→4+3).
    # Twitter limiti 4 görsel → ödeyenler+ödeyecekler parçaları toplam 4'ü geçmez.
    image_paths: list[str] = []
    paid_cards: list[dict] = []
    upcoming_cards: list[dict] = []
    try:
        from app.services.chart_image_generator import GREEN, GOLD
        paid_cards = await get_week_dividend_cards(p_start, p_end)
        upcoming_cards = await get_week_dividend_cards(start, end)

        # (kategori, başlık_prefix, renk, suffix_prefix, kart_listesi)
        groups = [
            ("ÖDEYENLER", "TEMETTÜ ÖDEYENLER", GREEN, "odeyenler",
             paid_cards, f"{week_label(p_start, p_end)}", "Bu hafta ödeyenler"),
            ("ODEYECEKLER", "ÖNÜMÜZDEKİ HAFTA ÖDEYECEKLER", GOLD, "odeyecekler",
             upcoming_cards, f"{week_label(start, end)}", "Önümüzdeki hafta"),
        ]
        # Önce her kategoriyi parçala, sonra 4 görsel limitine göre kırp
        planned: list[tuple] = []  # (head_prefix, color, suffix, chunk, sub_label, parts, idx)
        for _key, head_prefix, color, sfx, cards, rng, sub in groups:
            if not cards:
                continue
            chunks = _balanced_chunks(cards, max_per=6)
            for i, ch in enumerate(chunks):
                planned.append((head_prefix, color, sfx, ch, sub, rng, len(chunks), i))

        # Twitter 4 görsel limiti — fazlası varsa son parçaları at (log)
        if len(planned) > 4:
            logger.warning("Temettü görseli 4 limiti aşıldı (%d), kırpıldı", len(planned))
            planned = planned[:4]

        for head_prefix, color, sfx, ch, sub, rng, parts, i in planned:
            part_tag = f"  ({i + 1}/{parts})" if parts > 1 else ""
            title = f"{head_prefix}  ·  {rng}{part_tag}"
            sub_label = f"{sub}  ·  {rng}{part_tag}"
            img = generate_weekly_calendar_cards_image(
                [{"title": title, "color": color, "cards": ch}],
                sub_label, suffix=f"{sfx}_{i}",
            )
            if img:
                image_paths.append(img)
    except Exception as e:
        logger.warning("Temettü kart görseli hatası, düz takvime düşülüyor: %s", e)
    if not image_paths:
        fb = generate_weekly_calendar_image(week, label)
        if fb:
            image_paths.append(fb)
    if not image_paths:
        return {"sent": False, "reason": "image_failed", "total": total, "label": label}

    # Metin: admin custom_text varsa onu kullan, yoksa kart bazlı detaylı metin
    if custom_text and custom_text.strip():
        text = custom_text.strip()
    elif paid_cards or upcoming_cards:
        text = build_tweet_text_cards(
            paid_cards, upcoming_cards,
            week_label(p_start, p_end), week_label(start, end),
        )
    else:
        text = build_tweet_text(week, label, total)

    if dry_run:
        return {"sent": False, "reason": "dry_run", "total": total,
                "label": label, "image_paths": image_paths, "text": text}

    # 4) Tweet (2 görsel + metin, tek tweet)
    try:
        from app.services.twitter_service import _safe_tweet_with_multi_media
        ok = _safe_tweet_with_multi_media(
            text, image_paths[:4], source="dividend_weekly_calendar"
        )
    except Exception as e:
        logger.exception("Haftalık temettü takvimi tweet hatası: %s", e)
        ok = False

    return {"sent": bool(ok), "total": total, "label": label,
            "image_paths": image_paths, "text": text, "data_stale": data_stale}
