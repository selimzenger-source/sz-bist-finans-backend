"""Geride Bırakılan Haftanın Önemli KAP Gelişmeleri — haftalık özet.

Cumartesi 15:30 TR: geçen haftanın (Pzt–Cuma) günlük AI haber bülteninde biriken
OLUMLU / OLUMSUZ KAP gelişmeleri + (varsa) SPK bülteni admin panele dizilir.
Admin panelde seçilir → marka konseptinde kareye-yakın görsel + hashtag tweet.

Veri kaynağı: /api/v1/news/daily-summary ile AYNI havuz:
  - olumlu/olumsuz: kap_all_disclosures (ai_sentiment, ai_impact_score, ai_summary)
  - spk: SPK bülteni analiz tweet'leri (pending_tweets) → "#TICKER - açıklama"

Akış: Saturday job sadece HAZIRLAR + Telegram bildirir. Yayın admin onayıyla.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text as sa_text

logger = logging.getLogger(__name__)

_TR_TZ = timezone(timedelta(hours=3))

_TR_MONTHS = {
    1: "Ocak", 2: "Şubat", 3: "Mart", 4: "Nisan", 5: "Mayıs", 6: "Haziran",
    7: "Temmuz", 8: "Ağustos", 9: "Eylül", 10: "Ekim", 11: "Kasım", 12: "Aralık",
}

# DİKKAT: DB Türkçe karakterle saklar ('Çok Olumlu', 'Güçlü Olumlu'). ASCII yazımlar
# ('Cok Olumlu') EŞLEŞMEZ → en güçlü haberler sessizce elenir. Her iki form da listede.
_POSITIVE_SENTIMENTS = (
    "Güçlü Olumlu", "Çok Olumlu", "Olumlu", "Hafif Olumlu",
    "Guclu Olumlu", "Cok Olumlu",  # ASCII fallback
)
_NEGATIVE_SENTIMENTS = (
    "Güçlü Olumsuz", "Çok Olumsuz", "Olumsuz", "Hafif Olumsuz",
    "Guclu Olumsuz", "Cok Olumsuz",  # ASCII fallback
)

# Görsel kareye-yakın kalsın diye toplam öğe tavanı (admin daha azını seçebilir)
MAX_TOTAL_ITEMS = 40  # ihtiyaca göre max 5 kareye kadar dağıtılır
# Görselde özet dinamik satıra sarılır (yarım kesilmez) — bu uzunluğa kadar tam cümle
IMG_SUMMARY_CHARS = 300


# ════════════════════════════════════════════════════════════════════════════
#  TARİH
# ════════════════════════════════════════════════════════════════════════════

def _today_tr() -> date:
    return datetime.now(_TR_TZ).date()


def last_week_range(today: date | None = None) -> tuple[date, date]:
    """Geride bırakılan haftanın Pazartesi–Cuma aralığı.

    Cumartesi çalıştığında 'geçen hafta' = o anki haftanın Pzt–Cuma'sı (yeni biten).
    """
    d = today or _today_tr()
    monday = d - timedelta(days=d.weekday())  # bu haftanın pazartesisi
    friday = monday + timedelta(days=4)
    return monday, friday


def week_label(start: date, end: date) -> str:
    if start.month == end.month:
        return f"{start.day} - {end.day} {_TR_MONTHS[start.month]} {start.year}"
    return f"{start.day} {_TR_MONTHS[start.month]} - {end.day} {_TR_MONTHS[end.month]} {end.year}"


# ════════════════════════════════════════════════════════════════════════════
#  VERİ
# ════════════════════════════════════════════════════════════════════════════

# Sonundaki nokta CÜMLE SONU OLMAYAN kısaltmalar (küçük harf, noktasız son segment).
# "A.Ş.", "Ş.", "A.O." gibi noktalı kısaltmalarda son segment tek harf olduğu için
# zaten tek-harf kuralıyla yakalanır; buraya çok-harfli kısaltmalar girer.
_ABBREV = {
    "vb", "vs", "vd", "bkz", "örn", "orn", "no", "nr", "sn", "dr", "av", "prof",
    "doç", "doc", "san", "tic", "ltd", "şti", "sti", "max", "min", "yön", "müd",
    "gen", "mah", "cad", "sok", "apt", "bld", "tl", "usd", "eur",
}


def _is_sentence_end(s: str, idx: int) -> bool:
    """s[idx] bir '.', '!' veya '?' — gerçek cümle sonu mu?"""
    ch = s[idx]
    if ch in "!?":
        return True
    # Ondalık sayı: 70.81 → iki yanı rakam ise cümle sonu değil
    if 0 < idx < len(s) - 1 and s[idx - 1].isdigit() and s[idx + 1].isdigit():
        return False
    # Noktadan önceki "kelimeyi" al (harf/nokta zinciri)
    j = idx - 1
    while j >= 0 and (s[j].isalpha() or s[j] == "."):
        j -= 1
    word = s[j + 1:idx].lower().strip(".")
    # Noktalı kısaltma (A.Ş., T.A.Ş., B.V., T.C., A.O. ...) → kelime içi nokta varsa
    # bu bir kısaltmadır, cümle sonu DEĞİL (uzunluktan bağımsız).
    if "." in word:
        return False
    # Tek harf inisyali (A., Ş., T. ...) → cümle sonu değil
    if len(word) <= 1:
        return False
    # Bilinen kısaltma → cümle sonu değil
    if word in _ABBREV:
        return False
    return True


def _first_sentence(s: str, min_len: int = 40) -> str:
    """Kısaltma/ondalık tuzaklarını atlayarak ilk GERÇEK cümleyi döndürür.
    min_len'den kısa biten 'cümleyi' atlar (devamına bakar) — yarım kesilmeyi önler.
    """
    for m in re.finditer(r"[.!?]", s):
        idx = m.start()
        if not _is_sentence_end(s, idx):
            continue
        # Sonu kapanış tırnağı/parantez ise onu da dahil et
        end = idx + 1
        while end < len(s) and s[end] in '"”»)]':
            end += 1
        cand = s[:end].strip()
        if len(cand) >= min_len:
            return cand
    return s


def _shrink(text: str, max_chars: int, ticker: str | None = None) -> str:
    """Özeti tek satıra indir — ilk cümle veya kırp. Baştaki ticker tekrarını sil."""
    if not text:
        return ""
    s = " ".join(text.split())
    # Baştaki "TICKER" / "TICKER," / "TICKER -" tekrarını kaldır (görselde #TICKER zaten var)
    if ticker:
        s = re.sub(rf"^#?{re.escape(ticker)}\b[\s,:\-–—]*", "", s, flags=re.IGNORECASE).lstrip()
    # İlk gerçek cümle (kısaltma/ondalık tuzaklarını atlar)
    first = _first_sentence(s)
    if first and len(first) <= max_chars + 20:
        s = first
    if len(s) > max_chars:
        s = s[:max_chars].rsplit(" ", 1)[0].rstrip(",;:") + "…"
    return s.strip()


async def get_week_extras(start: date, end: date) -> dict:
    """Bu haftanın tedbir gelen / tedbiri biten / temettü dağıtan hisseleri.

    Kompakt görsel bloğu için — hepsi OTOMATİK gösterilir (seçim yok).
    Döner: {"tedbir_added":[tk...], "tedbir_ended":[tk...], "dividends":[(tk,gross)...]}
    """
    from app.database import async_session
    out = {"tedbir_added": [], "tedbir_ended": [], "dividends": []}
    try:
        async with async_session() as s:
            r1 = await s.execute(sa_text(
                "SELECT DISTINCT UPPER(ticker) FROM cautious_stocks "
                "WHERE start_date BETWEEN :s AND :e ORDER BY 1"), {"s": start, "e": end})
            out["tedbir_added"] = [x[0] for x in r1.all() if x[0]]
            r2 = await s.execute(sa_text(
                "SELECT DISTINCT UPPER(ticker) FROM cautious_stocks "
                "WHERE end_date BETWEEN :s AND :e ORDER BY 1"), {"s": start, "e": end})
            out["tedbir_ended"] = [x[0] for x in r2.all() if x[0]]
            r3 = await s.execute(sa_text(
                "SELECT UPPER(ticker), MAX(gross_dividend_per_share) FROM dividend_history "
                "WHERE payment_date BETWEEN :s AND :e AND gross_dividend_per_share > 0 "
                "GROUP BY UPPER(ticker) ORDER BY 1"), {"s": start, "e": end})
            out["dividends"] = [(x[0], float(x[1])) for x in r3.all() if x[0] and x[1] is not None]
    except Exception as e:
        logger.warning("Haftalık extras (tedbir/temettü) derleme hatası: %s", e)
    return out


async def get_week_kap_news(start: date, end: date) -> dict:
    """Hafta için olumlu/olumsuz/spk öğelerini derler.

    Döner: {
      "positive": [{"id","ticker","summary","impact","sentiment"}...],
      "negative": [...],
      "spk": [{"ticker","summary"}...],
    }
    Olumlu/olumsuz ticker bazında tekilleştirilir (en yüksek impact tutulur),
    impact'e göre azalan sıralanır.
    """
    from app.database import async_session

    start_dt = datetime.combine(start, datetime.min.time(), tzinfo=_TR_TZ)
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=_TR_TZ)
    start_utc = start_dt.astimezone(timezone.utc)
    end_utc = end_dt.astimezone(timezone.utc)

    pos_by_ticker: dict[str, dict] = {}
    neg_by_ticker: dict[str, dict] = {}
    spk_items: list[dict] = []

    async with async_session() as session:
        res = await session.execute(
            sa_text(
                """
                SELECT id, company_code, title, ai_sentiment, ai_impact_score, ai_summary
                FROM kap_all_disclosures
                WHERE published_at >= :s AND published_at < :e
                  AND company_code IS NOT NULL AND company_code <> ''
                  AND ai_sentiment IS NOT NULL
                ORDER BY ai_impact_score DESC NULLS LAST, published_at DESC
                """
            ),
            {"s": start_utc, "e": end_utc},
        )
        for row in res.all():
            rid, ticker, title, sentiment, impact, summary = row
            ticker = (ticker or "").upper().strip()
            if not ticker:
                continue
            is_pos = sentiment in _POSITIVE_SENTIMENTS
            is_neg = sentiment in _NEGATIVE_SENTIMENTS
            if not (is_pos or is_neg):
                continue
            short = _shrink(summary or title or "", IMG_SUMMARY_CHARS, ticker=ticker)
            if not short:
                continue
            item = {
                "id": int(rid), "ticker": ticker, "summary": short,
                "impact": float(impact) if impact is not None else 0.0,
                "sentiment": sentiment,
            }
            bucket = pos_by_ticker if is_pos else neg_by_ticker
            # Ticker bazında en yüksek impact'i tut
            if ticker not in bucket or item["impact"] > bucket[ticker]["impact"]:
                bucket[ticker] = item

        # ── SPK bülteni — analiz tweet'lerinden bullet satırları ──
        try:
            spk_res = await session.execute(
                sa_text(
                    """
                    SELECT text FROM pending_tweets
                    WHERE status = 'sent'
                      AND source IN ('tweet_spk_bulletin_analysis','tweet_spk_pending_visual')
                      AND sent_at IS NOT NULL
                      AND sent_at >= :s AND sent_at < :e
                    ORDER BY sent_at DESC
                    """
                ),
                {"s": start_utc, "e": end_utc},
            )
            bullet_re = re.compile(r"^\s*[•▪▫◦·*\-]?\s*#?([A-ZÇŞĞÜÖİ]{3,6})\s*[-–—]\s*(.+)$")
            seen_spk: set[str] = set()
            _gen_counter = 0
            for (txt,) in spk_res.all():
                if not txt:
                    continue
                for raw in txt.split("\n"):
                    line = raw.strip()
                    if not line:
                        continue
                    m = bullet_re.match(line)
                    if m:
                        tk = m.group(1).upper()
                        desc = _shrink(m.group(2), IMG_SUMMARY_CHARS)
                        key = f"{tk}:{desc[:20]}"
                        if key in seen_spk or not desc:
                            continue
                        seen_spk.add(key)
                        spk_items.append({"ticker": tk, "summary": desc})
                        continue
                    # ── Piyasa-geneli (hissesiz) SPK karari: "• <metin>" ──
                    # SPK bulteni cogunlukla hisse koduyla DEGIL genel kararlarla gelir
                    # (orn "• Borsa Istanbul'da ... pay orani hesaplama yontemi degisti").
                    # bullet_re bunlari atliyordu -> 0 SPK. "Karar N" olarak ekle.
                    if line[:1] in ("•", "▪", "▫", "◦", "·", "*", "-"):
                        _gen = line.lstrip("•▪▫◦·*– -").strip()
                        if len(_gen) >= 40:
                            # "#TICKER ..." ile basliyorsa hisse kodunu etiket yap;
                            # yoksa piyasa-geneli karar -> "Karar N"
                            _tm = re.match(r"^#([A-ZÇŞĞÜÖİ]{3,6})\b", _gen)
                            if _tm:
                                _lbl = _tm.group(1).upper()
                                key = f"{_lbl}:{_gen[:25]}"
                            else:
                                _gen_counter += 1
                                _lbl = f"Karar {_gen_counter}"
                                key = f"GEN:{_gen[:25]}"
                            if key in seen_spk:
                                continue
                            seen_spk.add(key)
                            spk_items.append({
                                "ticker": _lbl,
                                "summary": _shrink(_gen, IMG_SUMMARY_CHARS),
                            })
        except Exception as e:
            logger.warning("Haftalık SPK derleme hatası: %s", e)

    positive = sorted(pos_by_ticker.values(), key=lambda x: x["impact"], reverse=True)
    negative = sorted(neg_by_ticker.values(), key=lambda x: x["impact"], reverse=True)
    return {"positive": positive, "negative": negative, "spk": spk_items}


# ════════════════════════════════════════════════════════════════════════════
#  GÖRSEL (kare çerçeveler, marka konsepti, EK-1 footer)
# ────────────────────────────────────────────────────────────────────────────
#  Seçilenler tek kareye sığıyorsa → 1 kare. Sığmıyorsa → öğeleri/başlıkları
#  YARIM KESMEDEN 2 ayrı kare (Twitter tek tweet'te 2 görsel). Sayfalama satır
#  (öğe) ve bölüm-başlığı sınırlarında yapılır; başlık tek başına sayfa sonunda
#  kalmaz, bölüm bölünürse 2. karede "(devam)" ile tekrar yazılır.
# ════════════════════════════════════════════════════════════════════════════

_IMG_W = 1080
_IMG_PAD = 44
_SEC_HEAD_H = 56
_ITEM_H = 84  # (eski sabit — dinamik yükseklik için fallback)
_SEC_GAP = 14
_FOOTER_H = 70
_HEADER_MAIN_H = 188
_HEADER_CONT_H = 120
_SQUARE_MAX = 1120  # tek kare bu yüksekliği aşarsa böl
# Dinamik öğe yüksekliği — özet kaç satırsa o kadar (YARIM KESME YOK)
_LINE_H = 30          # özet satır yüksekliği (font 22)
_ITEM_PAD_TOP = 11
_ITEM_PAD_BOT = 13
_SUM_FONT_SZ = 22
_TK_FONT_SZ = 28


def _wrap_item(d, it: dict) -> None:
    """Bir öğenin özetini tam satırlara sarar (kesmeden) ve yüksekliğini hesaplar.
    it['_lines'], it['_sx'], it['_h'] doldurulur. d: ölçüm için ImageDraw.
    """
    from app.services.chart_image_generator import _load_font
    f_tk = _load_font(_TK_FONT_SZ, bold=True)
    f_sum = _load_font(_SUM_FONT_SZ, bold=False)
    W, PAD = _IMG_W, _IMG_PAD
    tk = f"#{it.get('ticker','')}"
    tkw = d.textlength(tk, font=f_tk)
    sx = PAD + 26 + int(tkw) + 16
    avail1 = (W - PAD - 20) - sx           # 1. satır (ticker'dan sonra)
    avail2 = (W - PAD - 20) - (PAD + 26)   # diğer satırlar (tam genişlik)
    words = (it.get("summary", "") or "").split()
    lines: list[str] = []
    cur, avail, i = "", avail1, 0
    while i < len(words):
        cand = (cur + " " + words[i]).strip()
        if d.textlength(cand, font=f_sum) <= avail:
            cur = cand; i += 1
        elif cur:
            lines.append(cur); cur = ""; avail = avail2
        else:  # tek kelime satırdan uzun — zorla ekle
            lines.append(words[i]); i += 1; avail = avail2
    if cur:
        lines.append(cur)
    if not lines:
        lines = [""]
    it["_lines"] = lines
    it["_sx"] = sx
    it["_h"] = _ITEM_PAD_TOP + len(lines) * _LINE_H + _ITEM_PAD_BOT


def _measure_sections(sections) -> None:
    """Tüm öğelerin satırlarını/yüksekliklerini önceden hesapla (dinamik layout)."""
    from PIL import Image, ImageDraw
    _mimg = Image.new("RGB", (_IMG_W, 10))
    _md = ImageDraw.Draw(_mimg)
    for s in sections:
        for it in s["items"]:
            _wrap_item(_md, it)


# ── KOMPAKT EXTRAS (tedbir gelen/biten + temettü dağıtan) — ilk sayfada 2 sütun ──
_EX_CAT_H = 30   # kategori başlık yüksekliği
_EX_ROW_H = 26   # chip satır yüksekliği


def _extras_cats(extras: dict):
    """(başlık, [chip metni...], renk) listesi — boş olanlar atlanır."""
    if not extras:
        return []
    def _tl(v):
        return f"{v:.2f}".replace(".", ",") + " TL"
    cats = [
        ("⛔ BU HAFTA TEDBİR GELEN", [f"#{t}" for t in extras.get("tedbir_added", [])], (255, 112, 67)),
        ("🔓 TEDBİRİ BİTEN", [f"#{t}" for t in extras.get("tedbir_ended", [])], (38, 198, 218)),
        ("💰 TEMETTÜ DAĞITAN", [f"#{t} {_tl(g)}" for t, g in extras.get("dividends", [])], (0, 200, 83)),
    ]
    return [c for c in cats if c[1]]


def _extras_height(extras: dict) -> int:
    cats = _extras_cats(extras)
    if not cats:
        return 0
    h = 10
    for _, items, _c in cats:
        rows = (len(items) + 1) // 2
        h += _EX_CAT_H + rows * _EX_ROW_H + 10
    return h + 6


def _render_extras(d, extras: dict, y0: int) -> int:
    """Kompakt 2 sütunlu extras bloğunu çizer; bittiği y'yi döner."""
    from app.services.chart_image_generator import _load_font, WHITE
    f_cat = _load_font(24, bold=True)
    f_chip = _load_font(21, bold=False)
    W, PAD = _IMG_W, _IMG_PAD
    col_w = (W - 2 * PAD) // 2
    y = y0 + 6
    for title, items, color in _extras_cats(extras):
        d.text((PAD, y), f"{title}  ({len(items)})", font=f_cat, fill=color)
        y += _EX_CAT_H
        rows = (len(items) + 1) // 2
        for r in range(rows):
            d.text((PAD + 6, y), items[r], font=f_chip, fill=WHITE)
            ri = r + rows
            if ri < len(items):
                d.text((PAD + col_w + 6, y), items[ri], font=f_chip, fill=WHITE)
            y += _EX_ROW_H
        y += 10
    return y + 6


def _wk_sections(positive, negative, spk):
    from app.services.chart_image_generator import GREEN, RED, GOLD
    secs = []
    if positive:
        secs.append({"title": "OLUMLU GELİŞMELER", "items": positive, "color": GREEN})
    if negative:
        secs.append({"title": "OLUMSUZ GELİŞMELER", "items": negative, "color": RED})
    if spk:
        secs.append({"title": "SPK BÜLTENİ", "items": spk, "color": GOLD})
    return secs


def _paginate(sections, extras_h: int = 0) -> list[list]:
    """Bölümleri/öğeleri sayfalara böler — öğe/başlık YARIM KESİLMEZ.

    extras_h: ilk sayfada üstte yer kaplayan kompakt extras bloğu (tedbir/temettü).

    Tek sayfaya sığarsa (≤ _SQUARE_MAX) → 1 sayfa. Aksi halde sayfalar
    DENGELİ doldurulur (boş kare kalmasın) ama cap aşılmaz; bölüm bölünürse
    yeni sayfada "(devam)" başlığı eklenir.
    """
    def cap(pi):
        head = (_HEADER_MAIN_H + extras_h) if pi == 0 else _HEADER_CONT_H
        return _SQUARE_MAX - head - _FOOTER_H - 14

    def _ih(it):
        return it.get("_h") or _ITEM_H

    base = sum(_SEC_HEAD_H + sum(_ih(it) for it in s["items"]) + _SEC_GAP for s in sections)
    single_h = _HEADER_MAIN_H + extras_h + base + _FOOTER_H + 14
    if single_h <= _SQUARE_MAX:
        ops = []
        for s in sections:
            ops.append(("header", s, False))
            for i, it in enumerate(s["items"]):
                ops.append(("item", s, it, i))
        return [ops]

    # Gerekli sayfa sayısı (greedy) → dengeli hedef yükseklik
    def greedy_count():
        pages, cur = 1, 0
        for s in sections:
            if cur > 0 and cur + _SEC_HEAD_H + (s["items"][0]["_h"] if s["items"] else 0) > cap(pages - 1):
                pages += 1; cur = 0
            cur += _SEC_HEAD_H
            for it in s["items"]:
                if cur + _ih(it) > cap(pages - 1):
                    pages += 1; cur = _SEC_HEAD_H
                cur += _ih(it)
            cur += _SEC_GAP
        return pages

    N = max(2, min(5, greedy_count()))  # ihtiyaca göre max 5 kare
    target = base / N  # sayfa başına dengeli hedef

    pages: list[list] = [[]]
    cur_h = 0
    for s in sections:
        pi = len(pages) - 1
        first_ih = s["items"][0]["_h"] if s["items"] else 0
        if cur_h > 0 and (cur_h + _SEC_HEAD_H + first_ih > cap(pi)
                          or (cur_h >= target and len(pages) < N)):
            pages.append([]); cur_h = 0; pi = len(pages) - 1
        pages[pi].append(("header", s, False))
        cur_h += _SEC_HEAD_H
        for i, it in enumerate(s["items"]):
            pi = len(pages) - 1
            if cur_h + _ih(it) > cap(pi) or (cur_h >= target and len(pages) < N):
                pages.append([]); cur_h = 0; pi = len(pages) - 1
                pages[pi].append(("header", s, True))
                cur_h += _SEC_HEAD_H
            pages[pi].append(("item", s, it, i))
            cur_h += _ih(it)
        cur_h += _SEC_GAP
    return pages


def _page_height(ops, page_idx, extras_h: int = 0) -> int:
    head_h = (_HEADER_MAIN_H + extras_h) if page_idx == 0 else _HEADER_CONT_H
    content_h = sum(_SEC_HEAD_H if o[0] == "header" else (o[2].get("_h") or _ITEM_H) for o in ops)
    return head_h + content_h + _SEC_GAP + _FOOTER_H + 14


def _render_page(ops, page_idx, total_pages, label, height: int | None = None, extras: dict | None = None) -> str:
    from PIL import Image, ImageDraw
    from app.services.chart_image_generator import (
        _load_font, _draw_bg_watermark, draw_brand_footer,
        BG_COLOR, HEADER_BG, WHITE, GRAY, GOLD,
    )

    W, PAD = _IMG_W, _IMG_PAD
    is_first = page_idx == 0
    head_h = _HEADER_MAIN_H if is_first else _HEADER_CONT_H
    _ex_h = _extras_height(extras) if (is_first and extras) else 0
    H = height or _page_height(ops, page_idx, _ex_h)

    img = Image.new("RGB", (W, H), BG_COLOR)
    d = ImageDraw.Draw(img)
    f_t1 = _load_font(40, bold=True)
    f_sub = _load_font(28, bold=False)
    f_ch = _load_font(30, bold=True)
    f_sec = _load_font(28, bold=True)
    f_tk = _load_font(28, bold=True)
    f_sum = _load_font(22, bold=False)  # biraz daha ufak — tam cümle sığsın

    _IMG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")

    # ── Header ──
    d.rectangle([(0, 0), (W, head_h)], fill=HEADER_BG)
    d.rectangle([(0, head_h - 5), (W, head_h)], fill=GOLD)
    tx = PAD
    logo_sz = 84 if is_first else 56
    try:
        for ln in ("logo.png", "logo.jpg"):
            lp = os.path.join(_IMG_DIR, ln)
            if os.path.exists(lp):
                logo = Image.open(lp).convert("RGBA").resize((logo_sz, logo_sz), Image.LANCZOS)
                img.paste(logo, (PAD, 26 if is_first else 18), logo)
                tx = PAD + logo_sz + 22
                break
    except Exception:
        tx = PAD
    if is_first:
        d.text((tx, 34), "GERİDE BIRAKILAN HAFTANIN", font=f_t1, fill=WHITE)
        d.text((tx, 80), "ÖNEMLİ KAP GELİŞMELERİ", font=f_t1, fill=GOLD)
        d.text((tx, 134), label, font=f_sub, fill=GRAY)
    else:
        d.text((tx, 24), "ÖNEMLİ KAP GELİŞMELERİ", font=f_t1, fill=GOLD)
        d.text((tx, 70), label, font=f_sub, fill=GRAY)
    if total_pages > 1:
        pg = f"{page_idx+1}/{total_pages}"
        pw = d.textlength(pg, font=f_ch)
        d.text((W - PAD - pw, (head_h - 30) // 2), pg, font=f_ch, fill=GRAY)

    # ── Kompakt extras (sadece 1. sayfa) — tedbir gelen/biten + temettü dağıtan ──
    if is_first and extras and _ex_h:
        y = _render_extras(d, extras, head_h + 8)
    else:
        y = head_h + 14
    for op in ops:
        if op[0] == "header":
            sec = op[1]
            is_cont = op[2]
            title = f"{sec['title']}  ({len(sec['items'])})" + ("  · devam" if is_cont else "")
            color = sec["color"]
            d.rectangle([(PAD, y), (W - PAD, y + _SEC_HEAD_H)], fill=HEADER_BG)
            d.rectangle([(PAD, y), (PAD + 8, y + _SEC_HEAD_H)], fill=color)
            d.text((PAD + 26, y + 13), title, font=f_sec, fill=color)
            y += _SEC_HEAD_H
        else:
            sec = op[1]; it = op[2]; idx = op[3]
            color = sec["color"]
            # Önceden hesaplanmış satırlar/yükseklik (dinamik — yarım kesme YOK)
            lines = it.get("_lines")
            if lines is None:
                _wrap_item(d, it)
                lines = it["_lines"]
            ih = it.get("_h") or _ITEM_H
            sx = it.get("_sx") or (PAD + 26)
            row_bg = (22, 22, 38) if idx % 2 == 0 else (26, 26, 46)
            d.rectangle([(PAD, y), (W - PAD, y + ih)], fill=row_bg)
            tk = f"#{it['ticker']}"
            d.text((PAD + 26, y + _ITEM_PAD_TOP), tk, font=f_tk, fill=color)
            ly = y + _ITEM_PAD_TOP
            for li, ln in enumerate(lines):
                lx = sx if li == 0 else (PAD + 26)
                d.text((lx, ly), ln, font=f_sum, fill=WHITE)
                ly += _LINE_H
            y += ih

    _draw_bg_watermark(img, W, H)
    draw_brand_footer(d, img, W, H, source="Kaynak: KAP")

    suffix = "" if total_pages == 1 else f"_{page_idx+1}"
    out_path = os.path.join(
        tempfile.gettempdir(),
        f"haftalik_kap_{datetime.now(_TR_TZ).strftime('%Y%m%d')}{suffix}.png",
    )
    img.save(out_path, "PNG", optimize=True)
    return out_path


def generate_weekly_kap_images(positive: list, negative: list, spk: list, label: str, extras: dict | None = None) -> list[str]:
    """Haftalık KAP görsel(ler)i — ihtiyaca göre max 5 kare PNG yolu listesi.

    extras: 1. sayfada üstte kompakt 2 sütun gösterilen tedbir/temettü bloğu
    (OTOMATİK — seçim gerektirmez).
    """
    try:
        sections = _wk_sections(positive, negative, spk)
        if not sections:
            return []
        _measure_sections(sections)  # dinamik satır/yükseklik (tam cümle)
        _ex_h = _extras_height(extras) if extras else 0
        pages = _paginate(sections, _ex_h)
        if len(pages) > 5:
            pages = pages[:5]  # max 5 kare (Twitter thread: 4 + 1 yanıt)
        total = len(pages)
        # Çok sayfada kareleri EŞİT yükseklikte tut (en dolu sayfaya göre)
        shared_h = None
        if total > 1:
            shared_h = min(_SQUARE_MAX, max(
                _page_height(ops, i, _ex_h if i == 0 else 0) for i, ops in enumerate(pages)))
        out = [_render_page(ops, i, total, label, height=shared_h,
                            extras=(extras if i == 0 else None)) for i, ops in enumerate(pages)]
        out = [p for p in out if p]
        logger.info("Haftalık KAP görsel(ler)i üretildi: %d kare", len(out))
        return out
    except Exception as e:
        logger.exception("Haftalık KAP görsel hatası: %s", e)
        return []


def item_key(kind: str, item: dict) -> str:
    """Seçim için stabil anahtar. kind: 'positive'|'negative'|'spk'."""
    if kind in ("positive", "negative"):
        return f"{kind[0]}{item.get('id')}"
    return "s" + item.get("ticker", "") + "|" + (item.get("summary", "")[:24])


def default_selection(data: dict, max_total: int = MAX_TOTAL_ITEMS) -> set[str]:
    """Varsayılan ön-seçim — olumlu + olumsuz + SPK DENGELİ temsil edilir.

    Eski sürüm tüm olumluları önce aldığı için (88 olumlu) olumsuz/SPK hiç
    seçilmiyordu. Artık SPK'nın tamamı (makul sınırda) + kalan bütçe olumlu/
    olumsuz arasında (~%40 olumsuza) bölünür; max_total'a kadar.
    """
    pos = [item_key("positive", it) for it in data.get("positive", [])]
    neg = [item_key("negative", it) for it in data.get("negative", [])]
    spk = [item_key("spk", it) for it in data.get("spk", [])]

    sel: list[str] = []
    sel += spk[:min(len(spk), max(1, max_total // 3))]  # SPK (varsa) — max ~1/3
    rem = max_total - len(sel)
    neg_quota = min(len(neg), max(0, int(rem * 0.4)))
    pos_quota = rem - neg_quota
    sel += pos[:pos_quota]
    sel += neg[:neg_quota]
    # Boşluk kaldıysa kalanlarla doldur (sıra: olumlu, olumsuz, spk)
    if len(sel) < max_total:
        chosen = set(sel)
        for k in (pos + neg + spk):
            if k not in chosen:
                sel.append(k); chosen.add(k)
                if len(sel) >= max_total:
                    break
    return set(sel)


def filter_selected(data: dict, selected: set[str]) -> tuple[list, list, list]:
    """Seçili anahtarlara göre (positive, negative, spk) listelerini süz (sıra korunur)."""
    pos = [it for it in data.get("positive", []) if item_key("positive", it) in selected]
    neg = [it for it in data.get("negative", []) if item_key("negative", it) in selected]
    spk = [it for it in data.get("spk", []) if item_key("spk", it) in selected]
    return pos, neg, spk


def build_tweet_text(positive: list, negative: list, spk: list, label: str) -> str:
    """Kısa tweet metni — detay görselde, metin başlık + sayı + hashtag."""
    tickers: list[str] = []
    for grp in (positive, negative, spk):
        for it in grp:
            t = it.get("ticker")
            # "Karar N" gibi sahte ticker'lar hashtag olmaz
            if t and t not in tickers and not t.lower().startswith("karar"):
                tickers.append(t)
    # Seçilen tüm hisselerin hashtag'i (kullanıcı isteği)
    ticker_tags = " ".join(f"#{t}" for t in tickers)

    parts = []
    if positive:
        parts.append(f"{len(positive)} olumlu")
    if negative:
        parts.append(f"{len(negative)} olumsuz")
    if spk:
        parts.append(f"{len(spk)} SPK")
    ozet = " · ".join(parts) if parts else "—"

    # Öne çıkan öğeleri kısa satır olarak ekle (AI'in en önemli bulduklari).
    # Listeler zaten impact'e göre sıralı → ilk öğeler en önemlisi.
    def _short(it, maxc: int = 70) -> str:
        tk = (it.get("ticker") or "").strip()
        s = _shrink(it.get("summary", "") or "", maxc, ticker=tk)
        tag = tk if tk.lower().startswith("karar") else f"#{tk}"
        return f"• {tag} {s}".rstrip()

    lines = [
        "📰 Geride Bırakılan Haftanın Önemli KAP Gelişmeleri",
        label,
        "",
        f"Bu hafta: {ozet}",
    ]
    if positive:
        lines.append("")
        lines.append("🟢 Öne çıkan olumlu:")
        lines += [_short(it) for it in positive[:3]]
    if negative:
        lines.append("")
        lines.append("🔴 Öne çıkan olumsuz:")
        lines += [_short(it) for it in negative[:3]]
    if spk:
        lines.append("")
        lines.append("📋 SPK bülteninden:")
        lines += [_short(it) for it in spk[:2]]
    lines.append("")
    lines.append("Tüm gelişmeler görselde 👇")
    lines.append("")
    lines.append(f"#KAP #BIST100 #borsa #hisse #yatırım {ticker_tags}".strip())
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════════════════
#  ORKESTRASYON
# ════════════════════════════════════════════════════════════════════════════

async def send_weekly_kap(start: date, end: date, selected_keys: set[str], *, dry_run: bool = False) -> dict:
    """Seçili haberlerden görsel üret + (dry_run değilse) tweet at."""
    label = week_label(start, end)
    data = await get_week_kap_news(start, end)
    pos, neg, spk = filter_selected(data, selected_keys)
    total = len(pos) + len(neg) + len(spk)
    if total == 0:
        return {"sent": False, "reason": "no_selection", "label": label}

    extras = await get_week_extras(start, end)  # tedbir gelen/biten + temettü (otomatik)
    images = generate_weekly_kap_images(pos, neg, spk, label, extras=extras)
    if not images:
        return {"sent": False, "reason": "image_failed", "label": label}
    text = build_tweet_text(pos, neg, spk, label)

    if dry_run:
        return {"sent": False, "reason": "dry_run", "label": label,
                "images": images, "text": text, "total": total, "frames": len(images)}

    try:
        from app.services.twitter_service import _safe_tweet_with_multi_media
        if len(images) <= 4:
            ok = bool(_safe_tweet_with_multi_media(text, images, source="weekly_kap_summary"))
        else:
            # Twitter tek tweette max 4 görsel → thread: ilk 4 + kalanlar yanıt olarak
            tid = _safe_tweet_with_multi_media(text, images[:4], source="weekly_kap_summary", return_id=True)
            ok = bool(tid)
            if tid:
                rest = images[4:]
                _safe_tweet_with_multi_media(
                    "📰 Haftalık KAP özeti — devamı 👇\n#KAP #BIST100 #borsa",
                    rest, source="weekly_kap_summary", in_reply_to=tid, force_send=True,
                )
    except Exception as e:
        logger.exception("Haftalık KAP tweet hatası: %s", e)
        ok = False
    return {"sent": bool(ok), "label": label, "total": total,
            "images": images, "frames": len(images), "text": text}


async def prepare_and_notify() -> dict:
    """Cumartesi 15:30 görevi — haftayı derle + admin'e 'hazır' Telegram bildirimi.

    Yayın YAPMAZ; admin panelde seçip gönderir. Bildirim, panel linkini içerir.
    """
    start, end = last_week_range()
    label = week_label(start, end)
    data = await get_week_kap_news(start, end)
    np, nn, ns = len(data["positive"]), len(data["negative"]), len(data["spk"])
    total = np + nn + ns

    try:
        from app.services.admin_telegram import send_admin_message
        if total == 0:
            await send_admin_message(
                f"📰 <b>Haftalık KAP Özeti</b> ({label})\n"
                "Bu hafta öne çıkan olumlu/olumsuz/SPK gelişmesi bulunamadı — özet atlanabilir."
            )
        else:
            await send_admin_message(
                f"📰 <b>Haftalık KAP Özeti HAZIR</b> ({label})\n"
                f"🟢 {np} olumlu · 🔴 {nn} olumsuz · 📋 {ns} SPK\n"
                "→ Admin panel → <b>Haftalık KAP</b> sayfasından seç ve gönder."
            )
    except Exception as e:
        logger.warning("Haftalık KAP hazır bildirimi hatası: %s", e)

    return {"label": label, "positive": np, "negative": nn, "spk": ns, "total": total}
