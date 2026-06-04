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
        label,
        "",
        f"Önümüzdeki hafta {total} hissede temettü ödemesi var 👇",
        "Detaylar görselde.",
        "",
        f"#temettü #BIST100 #borsa #hisse #yatırım {ticker_tags}".strip(),
    ]
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


async def run_weekly_dividend_calendar(*, force: bool = False, dry_run: bool = False) -> dict:
    """Haftalık temettü takvimi akışı.

    Args:
        force: hisse sayısı < MIN olsa bile devam et (test).
        dry_run: tweet atma, sadece görsel üret + sonucu döndür.
    """
    start, end = next_week_range()
    label = week_label(start, end)

    # 1) Veriyi tazele (en güncel temettühisseleri verisi) + SAĞLIK KONTROLÜ
    # Scrape başarısız/bozuksa SESSİZCE eksik takvim atmak yerine ADMIN'E TELEGRAM
    # uyarısı gönderilir. HARD FAIL (exception / error / processed=0) → tweet İPTAL.
    if not dry_run:
        scrape_ok, scrape_reason, stats = await _refresh_with_healthcheck()
        if not scrape_ok:
            await _alert_scrape_problem(scrape_reason, stats, hard=True)
            return {
                "sent": False, "reason": f"scrape_failed:{scrape_reason}",
                "stats": stats, "label": label,
            }

    # 2) Hafta verisini derle
    week = await get_week_dividends(start, end)
    total = count_week_stocks(week)
    logger.info("Haftalık temettü takvimi (%s): %d hisse", label, total)

    if total < MIN_STOCKS_FOR_TWEET and not force:
        return {
            "sent": False, "reason": "below_threshold",
            "total": total, "min": MIN_STOCKS_FOR_TWEET, "label": label,
        }

    # 3) Görsel
    image_path = generate_weekly_calendar_image(week, label)
    if not image_path:
        return {"sent": False, "reason": "image_failed", "total": total, "label": label}

    text = build_tweet_text(week, label, total)

    if dry_run:
        return {"sent": False, "reason": "dry_run", "total": total,
                "label": label, "image_path": image_path, "text": text}

    # 4) Tweet (görsel + metin)
    try:
        from app.services.twitter_service import _safe_tweet_with_media
        ok = _safe_tweet_with_media(text, image_path, source="dividend_weekly_calendar")
    except Exception as e:
        logger.exception("Haftalık temettü takvimi tweet hatası: %s", e)
        ok = False

    return {"sent": bool(ok), "total": total, "label": label,
            "image_path": image_path, "text": text}
