"""Haftalık 'SPK Onayı Bekleyen Sermaye Artırımı Talepleri' grafikli tweet.

Çarşamba 08:00 TR — YKK kararı alınmış ama henüz SPK onayı çıkmamış (status=ykk_alindi,
spk_approval_date IS NULL) bedelli/bedelsiz/tahsisli artırım taleplerini grafikli
kartlarla paylaşır (temettü haftalık takvimi tarzı).
"""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text as sa_text

logger = logging.getLogger(__name__)

_TR_TZ = ZoneInfo("Europe/Istanbul")
_TR_MONTHS = {1: "Oca", 2: "Şub", 3: "Mar", 4: "Nis", 5: "May", 6: "Haz",
              7: "Tem", 8: "Ağu", 9: "Eyl", 10: "Eki", 11: "Kas", 12: "Ara"}

# Tip → (etiket, renk). Renkler chart_image_generator paletinden bağımsız sabit.
_TYPE_META = {
    "bedelli":  ("BEDELLİ",  (255, 138, 101)),   # turuncu
    "bedelsiz": ("BEDELSİZ", (102, 187, 106)),   # yeşil
    "tahsisli": ("TAHSİSLİ", (100, 181, 246)),   # mavi
}

MIN_FOR_TWEET = 3


def _today_tr() -> date:
    return datetime.now(_TR_TZ).date()


async def get_pending_spk_cards() -> dict:
    """SPK onayı bekleyen artırım taleplerini tip bazında döndürür.

    Döner: {"bedelli":[card...], "bedelsiz":[...], "tahsisli":[...]}
      card: {"ticker","pct","ykk_date","company"}
    Kalite filtresi: oran dolu (≤%1000), YKK tarihi dolu, son 120 gün.
    """
    from app.database import async_session
    out = {"bedelli": [], "bedelsiz": [], "tahsisli": []}
    async with async_session() as s:
        # ★ Liste CANLI HALKARZ'a sabit (12.06.2026): last_seen_on_source son
        #   3 gunde damgalanmis = halkarz hala listeliyor. Hayalet/dusmus
        #   kayitlar (KAP'tan gelip onay almamis veya tamamlanmis) otomatik haric.
        # ★ pct tavani 1000 -> 10000: VKGYO %2753 gibi GERCEK yuksek bedelsizler
        #   girsin; sadece bariz parse hatasi (amount kolonu pct sanilmis,
        #   orn %132755) dislanir.
        res = await s.execute(sa_text(
            """
            SELECT UPPER(ticker) AS ticker, type, company_name, ykk_date,
                   COALESCE(bedelli_pct, bedelsiz_pct, tahsisli_pct) AS pct
            FROM capital_increases
            WHERE status = 'ykk_alindi' AND spk_approval_date IS NULL
              -- ★ YKK tarih penceresi KALDIRILDI (24.06.2026): tek doğru ölçüt
              --   "halkarz HÂLÂ listeliyor mu" = last_seen son 3 gün. YKK ne kadar
              --   eski olursa olsun halkarz listeliyorsa SPK onayı hâlâ bekleniyor
              --   demektir; göstermeliyiz (REEDR/MERCN/ALKLC/RYSAS/PASEU gibi 1 yıldan
              --   eski ama halkarz'da duran kayıtları 365 gün filtresi düşürüyordu →
              --   "tam liste halkarz'daki gibi" kuralı). YKK tarihi yine de dolu olmalı
              --   (kartta gösteriliyor).
              AND ykk_date IS NOT NULL
              AND last_seen_on_source IS NOT NULL
              AND last_seen_on_source >= (NOW() - INTERVAL '3 days')
              AND COALESCE(bedelli_pct, bedelsiz_pct, tahsisli_pct) IS NOT NULL
              AND COALESCE(bedelli_pct, bedelsiz_pct, tahsisli_pct) > 0
              AND COALESCE(bedelli_pct, bedelsiz_pct, tahsisli_pct) <= 10000
            ORDER BY ykk_date DESC, ticker
            """
        ))
        for tk, typ, comp, ykk, pct in res.all():
            if typ not in out or not tk:
                continue
            out[typ].append({
                "ticker": tk, "pct": float(pct) if pct is not None else None,
                "ykk_date": ykk, "company": comp,
            })
    return out


def _fmt_pct(v) -> str:
    if v is None:
        return "—"
    # Her zaman virgülden sonra 2 hane (kullanıcı kuralı): %900 -> %900,00
    return "%" + f"{float(v):.2f}".replace(".", ",")


def generate_pending_spk_image(sections: list[dict], label: str, suffix: str = "") -> str | None:
    """Bölümlü grafikli görsel. sections: [{"title","color","cards":[...]}]."""
    sections = [s for s in (sections or []) if s.get("cards")]
    if not sections:
        return None
    try:
        from PIL import Image, ImageDraw
        from app.services.chart_image_generator import (
            _load_font, _draw_bg_watermark, draw_brand_footer,
            BG_COLOR, HEADER_BG, WHITE, GRAY, GOLD, DIVIDER,
        )
        W, PAD, GAP, COLS = 1080, 44, 24, 2
        col_w = (W - 2 * PAD - GAP * (COLS - 1)) // COLS
        card_h = 150
        sec_head_h, sec_gap = 56, 18
        header_h, footer_h = 196, 96

        body_h = 0
        for s in sections:
            rws = (len(s["cards"]) + COLS - 1) // COLS
            body_h += sec_head_h + rws * (card_h + GAP) + sec_gap
        H = header_h + 18 + body_h + footer_h

        img = Image.new("RGB", (W, H), BG_COLOR)
        d = ImageDraw.Draw(img)
        f_title = _load_font(46, bold=True)
        f_sub = _load_font(28, bold=False)
        f_sec = _load_font(30, bold=True)
        f_tk = _load_font(34, bold=True)
        f_pct = _load_font(40, bold=True)
        f_lbl = _load_font(20, bold=False)
        f_date = _load_font(22, bold=True)

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
        d.text((logo_x, 40), "SPK ONAYI BEKLEYEN", font=f_title, fill=WHITE)
        d.text((logo_x, 96), "SERMAYE ARTIRIMI TALEPLERİ", font=f_title, fill=GOLD)
        d.text((logo_x, 150), label, font=f_sub, fill=GRAY)

        def _draw_card(x, y, c, color):
            d.rounded_rectangle([(x, y), (x + col_w, y + card_h)], radius=16,
                                fill=(24, 24, 40), outline=DIVIDER, width=1)
            d.rectangle([(x, y), (x + 8, y + card_h)], fill=color)
            d.text((x + 24, y + 18), f"#{c['ticker']}", font=f_tk, fill=color)
            # oran (büyük, sağ)
            ptxt = _fmt_pct(c.get("pct"))
            pw = d.textlength(ptxt, font=f_pct)
            d.text((x + col_w - 22 - pw, y + 16), ptxt, font=f_pct, fill=WHITE)
            d.text((x + 24, y + 64), "Artırım oranı", font=f_lbl, fill=GRAY)
            # YKK tarihi
            yk = c.get("ykk_date")
            if yk:
                ds = f"YKK: {yk.day} {_TR_MONTHS.get(yk.month, '')} {yk.year}"
                d.text((x + 24, y + 100), ds, font=f_date, fill=GRAY)
            _tag = "SPK onayı bekliyor"
            d.text((x + col_w - 22 - d.textlength(_tag, font=f_lbl), y + 102),
                   _tag, font=f_lbl, fill=GOLD)

        y = header_h + 18
        for s in sections:
            color = s.get("color") or GOLD
            d.rectangle([(PAD, y), (W - PAD, y + sec_head_h)], fill=HEADER_BG)
            d.rectangle([(PAD, y), (PAD + 8, y + sec_head_h)], fill=color)
            d.text((PAD + 26, y + 14), s["title"], font=f_sec, fill=color)
            y += sec_head_h
            cards = s["cards"]
            for idx, c in enumerate(cards):
                col, r = idx % COLS, idx // COLS
                _draw_card(PAD + col * (col_w + GAP), y + r * (card_h + GAP), c, color)
            rws = (len(cards) + COLS - 1) // COLS
            y += rws * (card_h + GAP) + sec_gap

        _draw_bg_watermark(img, W, H)
        draw_brand_footer(d, img, W, H, center=True)
        out_path = os.path.join(
            tempfile.gettempdir(),
            f"spk_bekleyen_{datetime.now(_TR_TZ).strftime('%Y%m%d')}{('_' + suffix) if suffix else ''}.png",
        )
        img.save(out_path, "PNG", optimize=True)
        logger.info("SPK bekleyen görseli üretildi: %s", out_path)
        return out_path
    except Exception as e:
        logger.exception("SPK bekleyen görsel hatası: %s", e)
        return None


def build_tweet_text(data: dict) -> str:
    # SADECE BEDELSİZ (kullanıcı kuralı) — bedelli/tahsisli tweet'e girmez.
    nz = len(data.get("bedelsiz", []))
    lines = [
        "📋 SPK Onayı Bekleyen Bedelsiz Sermaye Artırımları",
        "",
        "Yönetim kurulu kararı alınmış, SPK onayı bekleyen bedelsiz artırımlar 👇",
        "",
        f"🟢 {nz} hisse",
        "",
        "Oran + YKK tarihi görselde.",
        "",
        "#bedelsiz #sermayeartırımı",
    ]
    return "\n".join(lines)


async def run_weekly_capital_spk(*, force: bool = False, dry_run: bool = False) -> dict:
    """Çarşamba 08:00 — SPK onayı bekleyen BEDELSİZ artırım talepleri grafikli tweet."""
    data = await get_pending_spk_cards()
    # Sadece bedelsiz say (bedelli/tahsisli tweet'e girmez)
    total = len(data.get("bedelsiz", []))
    label = f"{_today_tr().day} {_TR_MONTHS[_today_tr().month]} {_today_tr().year} itibarıyla"
    logger.info("SPK bekleyen BEDELSİZ artırım: %d talep (bedelli=%d görmezden geliniyor)",
                total, len(data.get("bedelli", [])))
    if total < MIN_FOR_TWEET and not force:
        return {"sent": False, "reason": "below_threshold", "total": total}

    # Görseller: bedelli + bedelsiz (+tahsisli) ayrı. Görsel DİNAMİK yüksekliktedir —
    # tüm kartlar tek görselde gösterilir (eskiden cards[:8] ile 8'den fazlası kırpılıyordu:
    # "12 hisse" yazıp 8 göstermesi bug'ı). Twitter tek tweette max 4 görsel kabul ettiği
    # için, bir tip 14'ten fazlaysa 14'erli sayfalara bölünür (toplam 4 görsel sınırı korunur).
    # ★ SADECE BEDELSİZ (kullanıcı kuralı): bedelli/tahsisli tweet'e GİRMEZ.
    # Bedelli artırım yatırımcıdan para ister (seyreltme riski) — bedelsiz ise
    # iç kaynaktan dağıtım, retail için pozitif algı. Sadece bedelsiz paylaşılır.
    image_paths: list[str] = []
    order = [("bedelsiz", "BEDELSİZ SERMAYE ARTIRIMI TALEPLERİ")]
    _PER_IMG = 14  # tek görselde max kart (aşırı uzun görsel olmasın)
    for typ, title in order:
        cards = data.get(typ) or []
        if not cards:
            continue
        color = _TYPE_META[typ][1]
        npages = (len(cards) + _PER_IMG - 1) // _PER_IMG
        for p in range(npages):
            chunk = cards[p * _PER_IMG:(p + 1) * _PER_IMG]
            _pg = f" ({p + 1}/{npages})" if npages > 1 else ""
            img = generate_pending_spk_image(
                [{"title": f"{title}  ·  {len(cards)} hisse{_pg}", "color": color, "cards": chunk}],
                label, suffix=f"{typ}{p + 1}" if npages > 1 else typ,
            )
            if img:
                image_paths.append(img)
    if not image_paths:
        return {"sent": False, "reason": "image_failed", "total": total}

    text = build_tweet_text(data)
    if dry_run:
        return {"sent": False, "reason": "dry_run", "total": total,
                "image_paths": image_paths, "text": text}

    try:
        from app.services.twitter_service import _safe_tweet_with_multi_media
        ok = _safe_tweet_with_multi_media(text, image_paths[:4], source="capital_spk_weekly")
    except Exception as e:
        logger.exception("SPK bekleyen tweet hatası: %s", e)
        ok = False
    return {"sent": bool(ok), "total": total, "image_paths": image_paths, "text": text}
