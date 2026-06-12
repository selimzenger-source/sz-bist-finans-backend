"""Telegram Kanal Mesaj Poller — AI Haber Takibi.

Telegram Bot API uzerinden belirli kanaldan mesajlari ceker,
3 farkli mesaj formatini parse eder ve veritabanina kaydeder.
Yeni haber geldiginde push bildirim gonderir.

Mesaj Tipleri (sadece pozitif):
1. seans_ici_pozitif — Seans Ici Pozitif Haber Yakalandi
2. borsa_kapali — Seans Disi Pozitif Haber Yakalandi
3. seans_disi_acilis — Seans Disi Haber Yakalanan Hisse Acilisi (GAP)

NOT: Negatif haber yok. Fiyat bilgisi kaydedilmez (veri ihlali).

Konfigürasyon:
    TELEGRAM_BOT_TOKEN: Bot token (env var)
    TELEGRAM_CHAT_ID:   Kanal chat ID (env var)
"""

import re
import asyncio
import logging
from datetime import datetime, date, timezone, timedelta
from decimal import Decimal

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import async_session
from app.models.telegram_news import TelegramNews

# Turkiye saat dilimi (UTC+3)
TZ_TR = timezone(timedelta(hours=3))

logger = logging.getLogger(__name__)


async def _router_err(category: str, err, ticker: str = ""):
    """KAP router kategori-parse hatasini hem logla hem Telegram'a bildir.
    notify_scraper_error 10dk/3-mesaj dedup'li -> spam yok."""
    logger.warning("Router→%s hata (%s): %s", category, ticker or "?", err)
    try:
        from app.services.admin_telegram import notify_scraper_error
        await notify_scraper_error(f"KAP Router: {category} ({ticker or '?'})", str(err)[:300])
    except Exception:
        pass


async def _sync_dividend_feed_score(session, disclosure_id: int, div_row, body: str | None = None) -> None:
    """Kâr payı kararının feed (kap_all_disclosures) skorunu OTORİTER dividend
    sınıflandırmasına + AŞAMA (YKK/GK) ayrımına göre düzeltir.

    SİMETRİK KURAL (kullanıcı):
      • YKK dağıtım önerisi  (ilk/asıl karar) → POZİTİF (7.0)
      • YKK dağıtmama önerisi (ilk/asıl karar) → NEGATİF (3.7)
      • GK dağıtım onayı  (teyit, yeni etki yok) → NÖTR 5.5 (yeşilimsi koku)
      • GK dağıtmama onayı (teyit, yeni etki yok) → NÖTR 4.5 (kırmızımsı koku)
      • Ödeme/uygulama → NÖTR 5.0

    Aşama (YKK mı GK mı) body'den `is_genel_kurul_decision` ile belirlenir. Çünkü
    div_row.status hem YKK-dağıtmama hem GK-dağıtmama onayını 'reddedildi' yapar —
    body olmadan ayrılamaz.

    OVERRIDE POLİTİKASI:
      • GK onayı / ödeme → DEFINİTİF nötr olduklarından HER ZAMAN override edilir
        (gerçek AI skoru "dağıtmama=negatif" verse bile, teyit haberi nötr olmalı).
      • YKK (pozitif/negatif) → gerçek AI skoru DOĞRU yöndedir; yalnızca AI içerik
        çekemeyip GARANTİ-GENERIC nötr (5.0) verdiğinde düzeltilir.
    """
    if not disclosure_id or div_row is None:
        return
    from app.models.kap_all_disclosure import KapAllDisclosure

    disc = await session.get(KapAllDisclosure, disclosure_id)
    if disc is None:
        return
    cur = float(disc.ai_impact_score) if disc.ai_impact_score is not None else None
    summ = disc.ai_summary or ""
    is_generic_neutral = (
        cur is not None and 4.8 <= cur <= 5.2 and (
            not summ
            or "etki beklenmemektedir" in summ
            or "rutin/idari bildirim" in summ
            or "— bildirim." in summ
        )
    )

    status = (getattr(div_row, "status", None) or "")
    ticker = getattr(div_row, "ticker", "") or ""

    # AŞAMA: bu bildirim GENEL KURUL onayı mı (teyit) yoksa YKK önerisi mi (ilk karar)?
    is_gk = False
    try:
        from app.services.dividend_calendar_processor import is_genel_kurul_decision
        is_gk = is_genel_kurul_decision(body or "")
    except Exception:
        is_gk = False
    if status == "genel_kurul_onayli":
        is_gk = True

    # Gerçek brüt/net tutarı (varsa) — özette göster
    def _tl(v) -> str:
        try:
            return f"{float(v):.4f}".rstrip("0").rstrip(".").replace(".", ",")
        except Exception:
            return ""
    _g = getattr(div_row, "gross_amount_per_share", None)
    _n = getattr(div_row, "net_amount_per_share", None)
    _parts = []
    if _g:
        _parts.append(f"brüt {_tl(_g)} TL")
    if _n:
        _parts.append(f"net {_tl(_n)} TL")
    amt = (" Pay başına " + " / ".join(_parts) + ".") if _parts else ""
    _pt = (getattr(div_row, "payment_type", None) or "")
    _pay_kind = "nakit" if _pt == "cash" else ("nakit + pay" if _pt == "cash_and_stock" else ("bedelsiz pay" if _pt == "stock" else ""))

    if status == "reddedildi" and is_gk:
        # GK DAĞITMAMA ONAYI → teyit, yeni etki yok → NÖTR 4.5 (HER ZAMAN)
        disc.ai_impact_score = 4.5
        disc.ai_sentiment = "Nötr"
        disc.ai_summary = (
            f"{ticker} — Kâr payı dağıtmama kararı genel kurulda onaylandı. Yönetim "
            "kurulunun ilk kararı önceden ilan edildiği için bu onay yeni bir sürpriz ya "
            "da ek fiyat etkisi taşımaz; teyit niteliğindedir."
        )
    elif status == "reddedildi":
        # YKK DAĞITMAMA ÖNERİSİ → ilk/asıl karar → NEGATİF.
        # Gerçek AI negatif skoru DOĞRU; sadece garanti-generic nötr ise düzelt.
        if not is_generic_neutral:
            return
        disc.ai_impact_score = 3.7
        disc.ai_sentiment = "Hafif Olumsuz"
        disc.ai_summary = (
            f"{ticker} — Yönetim kurulu, ilgili dönem için kâr payı (temettü) "
            "dağıtmama yönünde öneri/karar aldı. İlk ve asıl karar olduğundan, "
            "hissedarlara nakit getiri sağlamaması nedeniyle olumsuz değerlendirilir."
        )
    elif status == "genel_kurul_onayli":
        # GK DAĞITIM ONAYI → teyit → NÖTR 5.5 (HER ZAMAN)
        disc.ai_impact_score = 5.5
        disc.ai_sentiment = "Nötr"
        disc.ai_summary = (
            f"{ticker} — Kâr payı dağıtımı genel kurulda onaylandı.{amt} "
            "İlk karar (yönetim kurulu) önceden ilan edildiği için bu onay yeni bir "
            "sürpriz ya da fiyat etkisi taşımaz; teyit niteliğindedir."
        )
    elif status in ("tarih_belli", "odeniyor", "tamamlandi"):
        # ÖDEME/UYGULAMA → NÖTR 5.0 (HER ZAMAN)
        disc.ai_impact_score = 5.0
        disc.ai_sentiment = "Nötr"
        disc.ai_summary = (
            f"{ticker} — Kâr payı ödeme/uygulama aşaması.{amt} "
            "İlk karar önceden ilan edildiği için yeni bir fiyat etkisi taşımaz."
        )
    else:
        # ykk_alindi — ilk YKK kâr payı DAĞITIM kararı (asıl fiyat etkisi burada) → POZİTİF.
        # Sadece garanti-generic nötr ise düzelt (gerçek AI skoru zaten doğru yönde).
        if not is_generic_neutral:
            return
        has_real_parse = bool(
            getattr(div_row, "period", None) or _g or _n or _pt
            or getattr(div_row, "stock_ratio_text", None)
        )
        if not has_real_parse:
            return
        disc.ai_impact_score = 7.0
        disc.ai_sentiment = "Olumlu"
        _kind_txt = f" ({_pay_kind})" if _pay_kind else ""
        disc.ai_summary = (
            f"{ticker} — Yönetim kurulu kâr payı (temettü) dağıtım kararı aldı{_kind_txt}."
            f"{amt} Hissedarlara getiri sağlayan olumlu bir gelişmedir; ödeme tarihi "
            "onay sürecinde netleşir."
        )
    logger.info(
        "Dividend feed skor senkron (%s): %s -> %.1f (status=%s, gk=%s)",
        ticker, cur, disc.ai_impact_score, status, is_gk,
    )
    # Override'ı çağırana DÖNDÜR — poller bu değeri tweet/push kararı için
    # ai_score/ai_summary değişkenlerine yansıtsın (GK onayı nötr → tweetlenmez).
    return {
        "score": disc.ai_impact_score,
        "summary": disc.ai_summary,
        "sentiment": disc.ai_sentiment,
    }


# -------------------------------------------------------------------
# Telegram API
# -------------------------------------------------------------------

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"
_last_update_id: int | None = None
_poll_lock = asyncio.Lock()  # Eszamanli getUpdates cagrilarini engelle
_consecutive_errors = 0  # Ust uste hata sayaci — spam onleme
_last_heartbeat: float = 0.0  # Periyodik status log zamani

# Push bildirim debounce — ayni ticker icin 2 dk icinde 2. positif KAP gelirse
# push gonderilmez ve kap_all_disclosures'a yazilmaz (kullanici spam yememesi icin).
# Format: { "TICKER": unix_timestamp_son_push }
_last_kap_push_per_ticker: dict[str, float] = {}
_KAP_PUSH_COOLDOWN_SEC = 120  # 2 dakika


async def fetch_telegram_updates(bot_token: str, offset: int | None = None) -> list[dict]:
    """Telegram getUpdates API'sini cagir.

    timeout=0: Aninda cevap al (long polling kullanma).
    Bu sayede 10 sn aralikla cagrildiginda cakisma olmaz.
    409 Conflict: Baska bir process ayni token'i kullaniyor — webhook kaldir + tekrar dene.
    """
    url = f"{TELEGRAM_API_BASE.format(token=bot_token)}/getUpdates"
    params = {"timeout": 0, "limit": 100}  # timeout=0: long polling yok, aninda yanit
    if offset is not None:
        params["offset"] = offset

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)

        # 409 Conflict — webhook ayarli olabilir, kaldirmaya calis
        if resp.status_code == 409:
            logger.warning("Telegram 409 Conflict — webhook kaldiriliyor...")
            try:
                delete_url = f"{TELEGRAM_API_BASE.format(token=bot_token)}/deleteWebhook"
                await client.post(delete_url)
                logger.info("Telegram webhook kaldirildi, tekrar deneniyor...")
                # Tekrar dene
                resp = await client.get(url, params=params)
            except Exception as e:
                logger.error("Webhook kaldirma hatasi: %s", e)
                return []

        resp.raise_for_status()
        data = resp.json()

    if not data.get("ok"):
        logger.warning("Telegram API hatasi: %s", data)
        return []

    return data.get("result", [])


# -------------------------------------------------------------------
# Mesaj Parse Fonksiyonlari
# -------------------------------------------------------------------

# ═══════════════════════════════════════════════════════════════════
# Ticker Validation — XU100/Endeks ve BIST'te olmayan ticker'lari ele
# ═══════════════════════════════════════════════════════════════════

# BIST endeksleri — bunlar hisse degil, atlanmali
_INDEX_TICKERS = {
    "XU100", "XU030", "XU050", "XBANK", "XKURU", "XSPOR", "XTUMY",
    "XGIDA", "XKMYA", "XMANA", "XKAGT", "XMESY", "XILTM", "XGMYO",
    "XUMAL", "XUSIN", "XHOLD", "XINSA", "XELKT", "XTEKS", "XTAST",
    "XTRZM", "XSGRT", "XFINK", "XHARZ", "XYORT", "XBLSM", "XUTEK",
    "XTRAS", "XKOBI", "XKURY", "XSANT", "XYUZO", "XUSIN", "XILMN",
    "XMADN", "XKMYA", "XSAVE",
}


def _is_valid_bist_ticker(ticker: str) -> bool:
    """Ticker BIST'te islem goren bir hisse mi?

    DEGISIM (2026-05-04): BigPara whitelist kontrolu KALDIRILDI. Cunku:
    - Yeni listelenen hisseler (RUBNS, IZENR vb.) cache'de olmayabiliyor
    - BigPara cache'i 2 gun, yeni hisseyi 2 gune kadar yakalamaz
    - KAP'in kendisi gecerli ticker listesi sagliyor (Sembol: alani)
    - Cok-sembollu bildirimlerde (BISTECH/MKK) gecerli sembolleri filtrelemek
      veri kaybina yol aciyordu (PGSUS gibi major hisseler bile filtreleniyordu)

    Yalnizca endeks ticker'larini reddet (XU100, XU030, XBANK vs.).
    """
    if not ticker:
        return False
    tk = ticker.upper().strip()

    # Endeksler — bunlar hisse degil
    if tk in _INDEX_TICKERS or tk.startswith("XU0") or tk.startswith("XU1"):
        return False

    return True


# ── HISSE WHITELIST (fon/endeks/forex eleme) ──────────────────────────────
# OPTGYF, Z30EAF, ZGOLDF gibi yatirim FONLARI ve BTCTRY/USDJPY/COPPER gibi forex/
# emtia kodlari KAP "pozitif haber" gibi yakalanip AI'a gidiyor, token harciyor ve
# AI Pozitif + favori bildirimine dusuyordu. Whitelist = company_financials (bilanco
# veren gercek sirketler) ∪ stock_markets (pazar listesi). IZENR/RUBNS gibi YENI ve
# EKIZ/QNBTR gibi kucuk hisseler dahil; fonlar/forex haric. Saatlik cache.
_EQUITY_TICKERS: set[str] = set()
_EQUITY_TICKERS_TS: float = 0.0
_EQUITY_TTL = 3600.0  # 1 saat


async def _get_equity_tickers(session) -> set[str]:
    """Gercek BIST hisse evreni (company_financials ∪ stock_markets), cache'li."""
    global _EQUITY_TICKERS, _EQUITY_TICKERS_TS
    import time as _t
    now = _t.time()
    if _EQUITY_TICKERS and (now - _EQUITY_TICKERS_TS) < _EQUITY_TTL:
        return _EQUITY_TICKERS
    try:
        from sqlalchemy import text as _sqltxt
        res = await session.execute(_sqltxt(
            "SELECT ticker FROM company_financials "
            "UNION SELECT ticker FROM stock_markets"
        ))
        s = {(r[0] or "").upper().strip() for r in res.fetchall() if r[0]}
        # Sanity: beklenen ~600. Cok az donduyse yukleme bozuk — eski seti koru
        # (yanlislikla tum hisseleri elemeyi onler).
        if len(s) >= 400:
            _EQUITY_TICKERS = s
            _EQUITY_TICKERS_TS = now
    except Exception as _eq_err:
        logger.warning("Equity whitelist yuklenemedi: %s", _eq_err)
    return _EQUITY_TICKERS


# Forex / emtia / kripto kodlari (hisse degil)
_FOREX_COMMODITY = {
    "BTCTRY", "BTCUSD", "ETHTRY", "ETHUSD", "USDTRY", "EURTRY", "GBPTRY",
    "USDJPY", "EURUSD", "GBPUSD", "XAUUSD", "XAGUSD",
    "COPPER", "BRENT", "GOLD", "SILVER",
}


def _looks_like_fund_or_index(ticker: str) -> bool:
    """Pattern bazli FON / FOREX / EMTIA tespiti — whitelist'e EK guvenlik katmani.

    Whitelist (company_financials ∪ stock_markets) birincil filtre; bu fonksiyon
    whitelist yuklenememesi durumunda fallback + her durumda ekstra koruma.
    Gercek BIST hisse kodlari: max 5 harf, RAKAM icermez. Buna gore:
      - Rakam iceren  -> fon/varant (Z30EAF, OPT25F, ZPX30F ...)
      - Forex/emtia set -> (BTCTRY, USDJPY, COPPER ...)
      - 6+ harf + 'F' ile biten -> yatirim fonu (OPTGYF, ZGOLDF, ZTLRFF, APBDLF ...)
    Gercek hisseler (THYAO, EKIZ, ISKUR) bu kaliplara TAKILMAZ.
    """
    tk = (ticker or "").upper().strip()
    if not tk:
        return True
    if tk in _FOREX_COMMODITY:
        return True
    if any(ch.isdigit() for ch in tk):
        return True
    if len(tk) >= 6 and tk.endswith("F"):
        return True
    return False


def detect_message_type(text: str) -> str | None:
    """Mesaj metninden tipini tespit et. Sadece pozitif haberler gecerli.

    Telegram bot mesaj formatlari:
    1. "SEANS İÇİ POZİTİF HABER"          → seans_ici_pozitif
    2. "🔒 BORSA KAPALI - Haber Kaydedildi" → borsa_kapali
    3. "ℹ️ Seans Dışı Haber Kaydedildi"     → borsa_kapali
    4. "📊 Seans Dışı Haber - AÇILIŞ BİLGİLERİ" → seans_disi_acilis
    """
    text_upper = text.upper()

    # Seans ici pozitif haber (negatif yok)
    if "SEANS İÇİ" in text_upper or "SEANS ICI" in text_upper:
        if "POZİTİF" in text_upper or "POZITIF" in text_upper:
            return "seans_ici_pozitif"
        # Negatif mesajlar atlanir
        return None

    # Acilis bilgileri (bu kontrolu borsa_kapali'dan ONCE yap —
    # cunku acilis mesaji da "Seans Disi" iceriyor)
    if "AÇILIŞ BİLGİLERİ" in text_upper or "ACILIS BILGILERI" in text_upper:
        return "seans_disi_acilis"

    # Borsa kapali = Seans disi pozitif haber
    # "🔒 BORSA KAPALI" veya "ℹ️ Seans Dışı Haber Kaydedildi"
    if "BORSA KAPALI" in text_upper:
        return "borsa_kapali"
    if "SEANS DIŞI HABER" in text_upper or "SEANS DISI HABER" in text_upper:
        return "borsa_kapali"

    return None


def parse_ticker(text: str) -> str | None:
    """Mesajdan ILK hisse kodunu cikart. 'Sembol: XXXXX' formatinda arar.

    Cok-sembollu mesajlarda sadece ilk ticker'i doner. Tum semboller icin
    parse_tickers() kullanin.
    """
    tickers = parse_tickers(text)
    return tickers[0] if tickers else None


def parse_tickers(text: str) -> list[str]:
    """Mesajdan TUM hisse kodlarini cikart. 'Sembol: XX,YY,ZZ' formatinda arar.

    KAP bazi bildirimleri (BISTECH, MKK, KAP genel duyurulari) birden fazla
    sembolu virgulle ayirarak yayinlar. Bu fonksiyon hepsini parse eder.

    Ornek girdiler:
        "Sembol: ALARK,EGGUB,KFEIN"  → ["ALARK", "EGGUB", "KFEIN"]
        "Sembol: THYAO"               → ["THYAO"]
        "Sembol: AAA, BBB , CCC"     → ["AAA", "BBB", "CCC"]
    """
    # Sembol satirini bul — sonraki satira kadar (\n ile durur)
    patterns = [
        r"Sembol:\s*([A-Z0-9 ,]+?)(?:\n|$)",
        r"Semb[oö]l:\s*([A-Z0-9 ,]+?)(?:\n|$)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            raw = match.group(1).strip()
            # Virgulle ayrilmis sembolleri parcala, bosluk/duplicate temizle
            tickers: list[str] = []
            for part in raw.split(","):
                tk = part.strip().upper()
                # Sadece harf+rakam (3-10 karakter), gecerli ticker formati
                if tk and re.fullmatch(r"[A-Z][A-Z0-9]{2,9}", tk) and tk not in tickers:
                    tickers.append(tk)
            if tickers:
                return tickers
    return []


def parse_price(text: str, label: str = "Fiyat") -> Decimal | None:
    """Mesajdan fiyat bilgisini cikart."""
    patterns = [
        rf"{label}[:\s]*?([\d]+[.,][\d]+)",
        rf"Anlık\s*{label}[:\s]*?([\d]+[.,][\d]+)",
        rf"Son\s*{label}[:\s]*?([\d]+[.,][\d]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            price_str = match.group(1).replace(",", ".")
            try:
                return Decimal(price_str)
            except Exception:
                continue
    return None


def parse_kap_id(text: str) -> str | None:
    """HaberId / NewsId alanini cikart.

    Eski Matriks botu: 'HaberId: 6418080'
    Yeni kap_tumhaberler_bot: 'NewsId: 6418080'
    Ikisi de KAP'tan gelen ayni ID — farkli prefix ile yaziliyor.
    """
    # Once HaberId, sonra NewsId dene
    for pattern in (r"HaberId[:\s]*(\d+)", r"NewsId[:\s]*(\d+)"):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def parse_news_title(text: str) -> str | None:
    """Yeni bot formatindaki 'Baslik:' satirindan haberin asil basligini cikart.

    Format:
        Baslik: Ozel Durum Aciklamasi (Genel)
        Baslik: Kar Payi Dagitim Islemlerine Iliskin Bildirim

    Eski Matriks formatinda 'Baslik:' satiri yoktur, None doner.
    """
    # "Başlık:" veya "Baslik:" sonrasi satirin sonuna kadar
    match = re.search(r"Ba[şs]l[ıi]k[:\s]+(.+?)(?:\n|$)", text, re.IGNORECASE)
    if match:
        title = match.group(1).strip()
        # Anlamsiz / cok kisa basliklar
        if title and len(title) >= 3 and title.upper() not in ("BULUNAMADI", "YOK", "?", "-"):
            return title
    return None


def parse_expected_trading_date(text: str) -> date | None:
    """Beklenen islem gununu cikart: 'Beklenen İşlem Günü: 2026-02-09 (Pazartesi)'."""
    match = re.search(r"Beklenen\s+[İI]şlem\s+G[üu]n[üu]:\s*(\d{4}-\d{2}-\d{2})", text, re.IGNORECASE)
    if match:
        try:
            return date.fromisoformat(match.group(1))
        except ValueError:
            pass
    return None


def parse_gap_pct(text: str) -> Decimal | None:
    """Acilis gap yuzdesini cikart: 'Açılış Gap: %0.50'."""
    match = re.search(r"[Aa]çılış\s+Gap[:\s]*%?([-]?[\d]+[.,][\d]+)", text, re.IGNORECASE)
    if match:
        try:
            return Decimal(match.group(1).replace(",", "."))
        except Exception:
            pass
    return None


def parse_pct_change(text: str) -> str | None:
    """Anlik yuzdesel degisimi cikart: 'Anlık: +%3,56' veya 'Anlık: -%1.20'.

    Fiyat degil, sadece yuzdesel degisim bilgisi (str olarak).
    Ornek: '+%3,56', '-%1.20', '+%0.45'
    """
    # Anlık: +%3,56  /  Anlık: -%1.20  /  Anlık:+%0,45
    match = re.search(
        r"Anl[ıi]k\s*:\s*([+-]?\s*%?\s*[\d]+[.,][\d]+)",
        text, re.IGNORECASE
    )
    if match:
        raw = match.group(1).strip()
        # Normalize: bosluk temizle, % isareti ekle
        raw = raw.replace(" ", "")
        if "%" not in raw:
            # +3,56 → +%3,56
            if raw.startswith("+") or raw.startswith("-"):
                raw = raw[0] + "%" + raw[1:]
            else:
                raw = "%" + raw
        return raw
    return None


def parse_prev_close(text: str) -> Decimal | None:
    """Onceki kapanis fiyatini cikart."""
    match = re.search(r"[Öö]nceki\s+Kapanış[:\s]*([\d]+[.,][\d]+)", text, re.IGNORECASE)
    if match:
        try:
            return Decimal(match.group(1).replace(",", "."))
        except Exception:
            pass
    return None


def parse_theoretical_open(text: str) -> Decimal | None:
    """Teorik acilis fiyatini cikart."""
    match = re.search(r"Teorik\s+[Aa]çılış[:\s]*([\d]+[.,][\d]+)", text, re.IGNORECASE)
    if match:
        try:
            return Decimal(match.group(1).replace(",", "."))
        except Exception:
            pass
    return None


def parse_sentiment(message_type: str) -> str:
    """Mesaj tipinden sentiment belirle. Tum haberler pozitif."""
    return "positive"


def build_parsed_title(message_type: str, ticker: str | None) -> str:
    """Mesaj tipi ve ticker'dan baslik olustur."""
    ticker_str = ticker or "???"
    type_labels = {
        "seans_ici_pozitif": f"⚡ Seans İçi Pozitif Haber Yakalandı - {ticker_str}",
        "borsa_kapali": f"🌙 Seans Dışı Pozitif Haber Yakalandı - {ticker_str}",
        "seans_disi_acilis": f"📊 Seans Dışı Haber Yakalanan Hisse Açılışı - {ticker_str}",
    }
    return type_labels.get(message_type, f"Haber — {ticker_str}")


# Bilanco-paketi bildirim basliklari — bir bilanco aciklamasinda KAP'a 5+ ayri
# tablo/rapor dusebiliyor (Finansal Durum, Kar/Zarar, Ozkaynaklar, Nakit Akis,
# Faaliyet Raporu, Sorumluluk Beyani). Bunlar favori listesine TEK TEK push
# GONDERILMEZ (spam); yerine tek "bilanco aciklandi" bildirimi gider.
_BILANCO_PKG_KW = (
    "finansal durum tablosu", "(bilanço)", "(bilanco)",
    "kar veya zarar", "kâr veya zarar", "kar/zarar", "kâr/zarar",
    "özkaynaklar değişim", "ozkaynaklar degisim", "özkaynak değişim",
    "nakit akış tablosu", "nakit akis tablosu",
    "diğer kapsamlı gelir", "diger kapsamli gelir",
    "finansal rapor", "finansal tablo ve dipnot", "finansal tablolar ve dipnot",
    "faaliyet raporu", "sorumluluk beyanı", "sorumluluk beyani",
)


def _is_bilanco_package_title(title: str) -> bool:
    """Baslik bir bilanco-paketi bildirimi mi? (favori push'tan haric tutulur)"""
    t = (title or "").lower()
    return any(k in t for k in _BILANCO_PKG_KW)


# Bir sirket ayni donem icin hem KONSOLIDE hem SOLO (Konsolide Olmayan) bilanco
# filing'i yapabilir (orn AVGYO). Fintables KONSOLIDE'yi gosterir. SOLO, konsolide
# zaten kaydedildiyse ONU EZMEMELI. {ticker:period -> timestamp} (konsolide kaydedildi).
_CONSOL_BILANCO_CACHE: dict[str, float] = {}


# -------------------------------------------------------------------
# Router — 6 ozel takvime dagit
# -------------------------------------------------------------------

async def _route_to_calendars(
    session,
    *,
    disclosure_id: int,
    ticker: str,
    company_name: str | None,
    title: str,
    body: str | None,
    kap_url: str | None,
    published_at,
) -> None:
    """Yeni KAP bildirimini sermaye artirimi + temettu state machine'lerine yonlendir.

    Title pattern check yaparak hizlica filtreler. Sadece ilgili olanlar AI parse edilir.
    Toptan/Tip/Pay zaten kap_all_disclosures.category alaninda — ayri tablo gerekmez.
    Tedbirli ayri kaynaktan beslenir.
    """
    from app.services.capital_increase_kap_parser import detect_stage as _ci_detect_stage
    from app.services.capital_increase_kap_processor import (
        process_kap_for_capital_increase as cap_process_new,
    )
    from app.services.dividend_calendar_processor import (
        is_dividend, process_kap_disclosure as div_process,
    )
    from app.services.business_deal_processor import (
        is_business_deal, process_kap_disclosure as deal_process,
    )
    from app.services.share_transaction_kap_processor import (
        is_share_transaction, process_kap_disclosure as shtx_process,
        is_mkk_share_disclosure, process_mkk_share_batch,
    )
    from app.services.kap_category_processors import (
        is_block_trade, process_block_trade,
        is_type_conversion, process_type_conversion,
        is_cautious, process_cautious,
    )

    # Dividend GK-nötr override'ı — poller'a geri döndürülür (tweet/push düzeltmesi için)
    _feed_override = None

    # Capital increase — yeni 3-pct schema processor (state machine)
    if _ci_detect_stage(title or "", body or ""):
        try:
            # SAVEPOINT: capital_increase hatasi outer transaction'i bozmasin
            async with session.begin_nested():
                res = await cap_process_new(
                    session, ticker=ticker or "", kap_url=kap_url or "",
                    title=title or "", body=body or "",
                )
                if res:
                    logger.info("Router→cap_inc %s: %s", ticker, res)
        except Exception as e:
            await _router_err("capital_increase", e, ticker)

    # is_dividend body-aware: 'Hak Kullanımı' generic - bedelsiz sermaye artırımıysa skip
    # Title tek başına is_dividend true diyorsa direkt kabul et — body re-fetch ile dolar
    if is_dividend(title or "", body or ""):
        try:
            # BUG FIX: Telegram'dan gelen body genelde kısa (title+snippet).
            # Dividend state machine `< 50 karakter` ise skip ediyor → tüm
            # dağıtım kararı / dağıtmama / ödeme bildirimleri kaçıyordu.
            # Buyback router'ındaki gibi KAP'tan tam body çekiyoruz.
            body_for_div = body or ""
            # BISTECH/bulk: AI özeti tek ticker'a indirgenmiş olabilir (363 char > 200 eşiği)
            # -> ham metinde "Pay Başına Brüt Temettü" yoksa MUTLAKA ham body çek
            # (yoksa multi-ticker tutarlar kaybolur, hepsine ilk ticker'ın değeri yazılır).
            _tl = (title or "").lower()
            _is_bistech_bulk = ("bistech pay piyasas" in _tl or "bıstech pay piyasas" in _tl
                                or "borsa istanbul a.ş." in _tl or "borsa istanbul a.s." in _tl)
            _needs_raw = (
                not body_for_div or len(body_for_div) < 200
                or (_is_bistech_bulk and "pay başına brüt temettü" not in body_for_div.lower()
                    and "pay basina brut temettu" not in body_for_div.lower())
            )
            if _needs_raw and kap_url:
                try:
                    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
                    disc = await fetch_kap_disclosure(kap_url)
                    if disc and disc.get("full_text"):
                        body_for_div = disc["full_text"]
                        logger.info("Dividend body KAP re-fetch (%s): %d kar (bistech=%s)", ticker, len(body_for_div), _is_bistech_bulk)
                except Exception as _fe:
                    logger.debug("Dividend body re-fetch hata (%s): %s", ticker, _fe)

            # ★ SAVEPOINT: divident processor hatasi outer transaction'i bozmasin
            # (BORSK bug: dividend_calendar.payment_type kolonu eksikti, hata
            # kap_all_disclosures kaydını kaybettiriyordu)
            div_row = None
            async with session.begin_nested():
                div_row = await div_process(
                    session, disclosure_id=disclosure_id, ticker=ticker,
                    company_name=company_name, title=title, body=body_for_div,
                    kap_url=kap_url, published_at=published_at,
                )
            # ── FEED SKORU SENKRONU ──
            # AI içerik çekemeyip garanti-generic nötr verdiyse, dividend processor'ın
            # OTORİTER sınıflandırmasına göre feed skorunu düzelt (kâr payı dağıtım
            # kararı yanlışlıkla "nötr/etki yok" görünmesin — BVSAN vakası).
            try:
                _feed_override = await _sync_dividend_feed_score(session, disclosure_id, div_row, body_for_div)
            except Exception as _se:
                logger.debug("Dividend feed skor senkron hata (%s): %s", ticker, _se)
        except Exception as e:
            await _router_err("dividend", e, ticker)

    if is_business_deal(title):
        try:
            # SAVEPOINT: business_deal hatasi outer transaction'i bozmasin
            async with session.begin_nested():
                await deal_process(
                    session, disclosure_id=disclosure_id, ticker=ticker,
                    company_name=company_name, title=title, body=body,
                    kap_url=kap_url, published_at=published_at,
                )
        except Exception as e:
            await _router_err("business_deal", e, ticker)

    # Pay Geri Alımı (buyback) — share_transaction'dan ÖNCE check.
    # Buyback ile share_transaction patterns'ı çakışıyor ("geri alın").
    # is_buyback True ise share_transaction'a HİÇ gitmesin (duplicate önlenir).
    is_bb = False
    bb_parsed = None  # buyback parse sonucu — AI fail durumunda fallback skor icin
    try:
        from app.services.buyback_processor import is_buyback, process_buyback
        if is_buyback(title):
            is_bb = True
            body_for_bb = body or ""
            if (not body_for_bb or len(body_for_bb) < 200) and kap_url:
                try:
                    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
                    disc = await fetch_kap_disclosure(kap_url)
                    if disc and disc.get("full_text"):
                        body_for_bb = disc["full_text"]
                except Exception:
                    pass
            bb_parsed = await process_buyback(
                session, ticker=ticker, body=body_for_bb,
                kap_url=kap_url, disclosure_id=disclosure_id,
                published_at=published_at,
            )
    except Exception as e:
        await _router_err("buyback", e, ticker)
    # is_bb + bb_parsed bilgisini sonradan AI fail durumunda kullanmak icin sakla
    if is_bb and bb_parsed:
        # _route_to_calendars'in cagiren fonksiyona dondurmesi gerekiyor — burada
        # session attribute olarak set edip telegram_poller ana akisinda okuyacagiz
        try:
            session.info["last_buyback_parsed"] = bb_parsed
            session.info["last_buyback_ticker"] = ticker.upper()
        except Exception:
            pass

    # TİPE DÖNÜŞÜM BAŞVURUSU (Pay Satış Bilgi Formu) — gerçek içerik EKTE olabilir
    # (generic "Özel Durum Açıklaması (Genel)" kapağı). Ortağın imtiyazlı payını
    # borsada işlem gören niteliğe dönüştürme/satışa konu etme BAŞVURUSU = gelecek arz.
    # share_type_conversions'a record_type='basvuru' yazılır (Başvurular sekmesi).
    try:
        from app.services.share_conversion_application_processor import (
            is_conversion_application as _is_conv_app, process_conversion_application as _proc_conv_app,
        )
        _conv_body = body or ""
        if not _is_conv_app(title, _conv_body) and kap_url:
            _tl = (title or "").lower()
            # Generic kapak / kısa body → EK'i çek, başvuru mu bak (gereksiz indirme yok)
            if ("özel durum" in _tl or "genel" in _tl or "bilgi form" in _tl or len(_conv_body) < 400):
                try:
                    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
                    _disc = await fetch_kap_disclosure(kap_url)
                    if _disc and _disc.get("full_text"):
                        _conv_body = _disc["full_text"]
                except Exception:
                    pass
        if _is_conv_app(title, _conv_body):
            await _proc_conv_app(
                session, ticker=ticker, company_name=company_name,
                title=title, body=_conv_body, kap_url=kap_url, published_at=published_at,
            )
    except Exception as e:
        await _router_err("conversion_application", e, ticker)

    # ÖNCE block_trade kontrol — toptan/borsa dışı pay devri bildirimleri
    # bazen başlıkta sadece "Pay Alım Satım Bildirimi" der ama body'de
    # "toptan alış satış" geçer. Bu durumda share_transaction'a değil
    # block_trade'e route etmek lazım.
    is_bt = is_block_trade(title or "", body or "") and not is_bb
    if is_bt:
        try:
            # KRİTİK: body genelde AI özeti (lot/fiyat/taraf YOK) — yapısal parse için
            # HAM KAP metni şart. Özet ise (kısa veya "lot/adet" geçmiyorsa) ham çek.
            body_for_bt = body or ""
            _bt_lo = body_for_bt.lower()
            if kap_url and (len(body_for_bt) < 250
                            or not any(k in _bt_lo for k in ("lot", "adet", "tl", "pay", "nominal"))
                            or "yapılan teknik bir duyuru" in _bt_lo  # AI özet imzası
                            or "fiyat etkisi beklenm" in _bt_lo):
                try:
                    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
                    disc = await fetch_kap_disclosure(kap_url)
                    if disc and disc.get("full_text"):
                        body_for_bt = disc["full_text"]
                        logger.info("block_trade ham body re-fetch (%s): %d kar", ticker, len(body_for_bt))
                except Exception:
                    pass
            await process_block_trade(
                session, disclosure_id=disclosure_id, ticker=ticker,
                company_name=company_name, title=title, body=body_for_bt,
                kap_url=kap_url, published_at=published_at,
            )
        except Exception as e:
            await _router_err("block_trade", e, ticker)

    # ── MKK "Özel Durumlar Tebliği 12-(4)" TOPLU pay sahipliği bildirimi ──
    # Tek disclosure'da çok şirket; detay ekte tablo. Tekil pay alım satım
    # akışından AYRI işlenir (multi-ticker + dedup). Haber tipi STANDART kalır
    # (feed skoru override edilmez). Bazı işlemler SADECE burada → kaçırmamak için.
    is_mkk = is_mkk_share_disclosure(title or "", body or "")
    if is_mkk:
        try:
            async with session.begin_nested():
                _n = await process_mkk_share_batch(
                    session, disclosure_id=disclosure_id, title=title or "",
                    body=body, kap_url=kap_url, published_at=published_at,
                )
                if _n:
                    logger.info("Router→MKK 12(4) %s: %d yeni pay alım satım", ticker, _n)
        except Exception as e:
            await _router_err("mkk_share_batch", e, ticker)

    # share_transaction sadece block_trade VE buyback VE MKK DEĞİL ise çalışır
    # ("geri alın" share_transaction VE buyback pattern'ında ortak → çakışma)
    if not is_bt and not is_bb and not is_mkk and is_share_transaction(title, body or ""):
        # Multi-symbol bulk duyurularda ardışık fetch KAP rate limit'e takılır.
        # Her fetch öncesi 1.5sn bekle (KAP standart rate limit toleransı).
        try:
            import asyncio as _asyncio
            await _asyncio.sleep(1.5)
        except Exception:
            pass

        # ÖNCE: KAP URL'den structured table fetch (deterministik, daha güvenilir)
        kap_fetch_ok = False
        kap_fetch_error: str | None = None
        if kap_url:
            try:
                from app.services.kap_pay_alim_satim_fetcher import upsert_pay_alim_satim_from_kap
                kap_fetch_ok = await upsert_pay_alim_satim_from_kap(
                    session, kap_url=kap_url, company_code=ticker,
                    title=title, published_at=published_at, disclosure_id=disclosure_id,
                )
                if not kap_fetch_ok:
                    kap_fetch_error = "upsert_returned_false"
            except Exception as e:
                kap_fetch_error = f"exception:{type(e).__name__}:{str(e)[:120]}"
                await _router_err("kap_pay_fetch", e, ticker)

        # KAP fetcher fail VEYA exception olursa AI parser fallback
        if not kap_fetch_ok:
            logger.info(
                "Pay alım satım AI fallback (%s): kap_fetch=%s",
                ticker, kap_fetch_error or "unknown",
            )
            try:
                # SAVEPOINT: share_transaction hatasi outer transaction'i bozmasin
                async with session.begin_nested():
                    await shtx_process(
                        session, disclosure_id=disclosure_id, ticker=ticker,
                        company_name=company_name, title=title, body=body,
                        kap_url=kap_url, published_at=published_at,
                    )
            except Exception as e:
                logger.exception("Router→share_transaction AI fallback hata (%s): %s", ticker, e)

    if is_type_conversion(title):
        try:
            # SAVEPOINT: type_conversion hatasi outer transaction'i bozmasin
            async with session.begin_nested():
                await process_type_conversion(
                    session, disclosure_id=disclosure_id, ticker=ticker,
                    company_name=company_name, title=title, body=body,
                    kap_url=kap_url, published_at=published_at,
                )
        except Exception as e:
            await _router_err("type_conversion", e, ticker)

    if is_cautious(title):
        try:
            # Body kisaysa KAP'tan tam icerigi cek — AI parse tag'leri yakalasin
            body_for_cs = body or ""
            if (not body_for_cs or len(body_for_cs) < 200) and kap_url:
                try:
                    from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
                    disc = await fetch_kap_disclosure(kap_url)
                    if disc and disc.get("full_text"):
                        body_for_cs = disc["full_text"]
                except Exception:
                    pass
            # SAVEPOINT: cautious hatasi outer transaction'i bozmasin
            async with session.begin_nested():
                await process_cautious(
                    session, disclosure_id=disclosure_id, ticker=ticker,
                    company_name=company_name, title=title, body=body_for_cs,
                    kap_url=kap_url, published_at=published_at,
                )
        except Exception as e:
            await _router_err("cautious", e, ticker)

    # BISTECH VBTS multi-ticker — body'den tum ticker'lari cikart
    try:
        from app.services.kap_category_processors import is_bistech_vbts, process_cautious_bistech_multi
        body_for_vbts = body or ""
        if (not body_for_vbts or len(body_for_vbts) < 200) and kap_url and is_bistech_vbts(title, body_for_vbts):
            from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
            disc = await fetch_kap_disclosure(kap_url)
            if disc and disc.get("full_text"):
                body_for_vbts = disc["full_text"]
        if is_bistech_vbts(title, body_for_vbts):
            # SAVEPOINT: BISTECH VBTS hatasi outer transaction'i bozmasin
            async with session.begin_nested():
                await process_cautious_bistech_multi(
                    session, disclosure_id=disclosure_id, title=title,
                    body=body_for_vbts, kap_url=kap_url, published_at=published_at,
                )
    except Exception as e:
        await _router_err("BISTECH_VBTS", e)

    # ════════════════════════════════════════════════════════════════
    # MKK/BIST DUYURULARI — title generic, body'ye bakmak gerek
    # 1. Temettü ödeme duyurusu (BIST sistemine düştü) → DividendCalendar 'tamamlandi'
    # 2. Bedelsiz/Sermaye artırım gerçekleşme → CapitalIncrease 'tamamlandi'
    # ════════════════════════════════════════════════════════════════
    title_lo = (title or "").lower()
    needs_body_check = any(k in title_lo for k in [
        "merkezi kayıt", "merkezi kayit", "mkk",
        "bistech", "pay piyasası", "pay piyasasi",
        "alım satım sistemi", "alim satim sistemi",
        "duyurusu", "kamuyu aydınlatma platformu",
    ])

    if needs_body_check:
        # Body yoksa KAP'tan çek
        body_for_check = body or ""
        if (not body_for_check or len(body_for_check) < 200) and kap_url:
            try:
                from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
                disclosure = await fetch_kap_disclosure(kap_url)
                if disclosure and disclosure.get("full_text"):
                    body_for_check = disclosure["full_text"]
            except Exception as e:
                logger.debug("Router→body fetch hata (%s): %s", ticker, e)

        # 1. Temettü ödeme
        try:
            from app.services.dividend_calendar_processor import (
                is_dividend_payment_announcement,
                process_dividend_payment_announcement,
            )
            if is_dividend_payment_announcement(title, body_for_check):
                result = await process_dividend_payment_announcement(
                    session, body=body_for_check, kap_url=kap_url,
                    disclosure_id=disclosure_id, published_at=published_at,
                )
                if result.get("updated"):
                    logger.info("Router→DividendPayment: %s ticker güncellendi", result["updated"])
        except Exception as e:
            await _router_err("DividendPayment", e)

        # 2. MKK gerçekleşme
        try:
            from app.services.capital_increase_processor import (
                is_mkk_capital_realization,
                process_mkk_capital_realization,
            )
            if is_mkk_capital_realization(title, body_for_check):
                result = await process_mkk_capital_realization(
                    session, ticker_hint=ticker, body=body_for_check,
                    kap_url=kap_url, disclosure_id=disclosure_id,
                )
                if result.get("matched"):
                    logger.info("Router→MKK Realization: %s tamamlandi", result.get("ticker"))
        except Exception as e:
            await _router_err("MKK_Realization", e)

    # v3.1 — Bilanco pipeline TETIKLEME SADECE "Finansal Durum Tablosu (Bilanço)" basliginda.
    # 11 ek mali tablo basligi (Kar veya Zarar Tablosu, Nakit Akis, Sorumluluk Beyani, Faaliyet
    # Raporu, Ozkaynaklar Degisim, vb.) RUTINDIR ve "Tum KAP" akisina notr olarak basilir,
    # bilanco pipeline tetiklemez. Sadece "Finansal Durum Tablosu (Bilanço)" ana bilanco
    # kalemidir — bu geldiginde XBRL parse + bilanco analizi + frontend "Yakinda" yonlendirme.
    title_lower = (title or "").lower().strip()
    is_bilanco_kap = (
        "finansal durum tablosu" in title_lower
        and "bilan" in title_lower  # "(Bilanço)" suffix kontrolu — extra emniyet
    )
    if is_bilanco_kap:
        # ANINDA direkt parse — kap_url'den XBRL cek, period+rakamlar dogru kaydet.
        # Queue/kap_all_disclosures'a is_bilanco=True flag bagimliligi yok.
        if kap_url:
            try:
                from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
                from app.services.bilanco_kap_scraper import parse_kap_finansal_rapor
                from app.services.ai_bilanco_analyzer import save_parsed_bilanco
                from app.services.bilanco_pipeline import BILANCO_ALLOWED_SECTORS
                disc = await fetch_kap_disclosure(kap_url)
                body_xbrl = disc.get("full_text", "") if disc else ""
                if body_xbrl:
                    parsed = parse_kap_finansal_rapor(body_xbrl, ticker)
                    # Safeguard: sektor whitelist + confidence kontrol
                    sec = parsed.get("sector_type") if parsed else None
                    conf = parsed.get("confidence") if parsed else None
                    # KONSOLIDE TERCIHI: "Finansal Tablo Niteliği" Konsolide mi, Konsolide
                    # Olmayan (solo) mu? Konsolide zaten kaydedildiyse SOLO onu ezmesin
                    # (her iki gelis sirasinda da konsolide kazanir).
                    import re as _re_k, time as _time_k
                    _mn = _re_k.search(r"Finansal Tablo Niteli[ğg]i\s+(Konsolide Olmayan|Konsolide)", body_xbrl)
                    _is_solo = bool(_mn and "Olmayan" in _mn.group(1))
                    _pkey = f"{ticker}:{parsed.get('period')}" if parsed and parsed.get("period") else None
                    _kons_recent = bool(_pkey and (_time_k.time() - _CONSOL_BILANCO_CACHE.get(_pkey, 0) < 1800))
                    if (
                        parsed and parsed.get("period")
                        and (parsed.get("total_assets") or parsed.get("revenue"))
                        and sec in BILANCO_ALLOWED_SECTORS
                        and conf in ("high", "medium")
                    ):
                        if _is_solo and _kons_recent:
                            logger.info(
                                "Router→bilanco: %s %s SOLO atlandi (konsolide zaten kayitli)",
                                ticker, parsed.get("period"),
                            )
                        else:
                            await save_parsed_bilanco(ticker, parsed)
                            if not _is_solo and _pkey:
                                _CONSOL_BILANCO_CACHE[_pkey] = _time_k.time()
                            logger.info(
                                "Router→bilanco DIREKT save: %s %s sec=%s conf=%s konsolide=%s",
                                ticker, parsed.get("period"), sec, conf, (not _is_solo),
                            )
                    elif parsed:
                        logger.warning(
                            "Router→bilanco SKIP: %s sektor=%s conf=%s (whitelist/confidence dısı)",
                            ticker, sec, conf,
                        )
                        # ADMIN UYARI: bilanco otomatik kaydedilemedi — manuel kontrol gerek
                        try:
                            from app.services.admin_telegram import send_admin_message
                            await send_admin_message(
                                f"⚠️ <b>Bilanço otomatik işlenemedi</b>\n"
                                f"Hisse: <b>{ticker}</b>\n"
                                f"Dönem: {parsed.get('period') or '?'}\n"
                                f"Sektör: {sec or '?'} · Confidence: {conf or '?'}\n"
                                f"Sebep: whitelist/confidence dışı — <b>manuel scrape gerekebilir</b>.\n"
                                f"KAP: {kap_url}",
                                silent=True,
                            )
                        except Exception as _ae:
                            logger.debug("admin bilanco uyari hata: %s", _ae)
            except Exception as e:
                await _router_err("bilanco_direkt", e, ticker)
        # Yedek: queue worker da calissin (full pipeline tweet/notification icin)
        try:
            from app.services.bilanco_pipeline import enqueue_bilanco
            await enqueue_bilanco(ticker, title or "")
        except Exception as e:
            await _router_err("bilanco_queue", e, ticker)

    return _feed_override


# -------------------------------------------------------------------
# Ana Poller Fonksiyonu
# -------------------------------------------------------------------

async def poll_telegram_messages(bot_token: str, chat_id: str) -> int:
    """Telegram kanalından yeni mesajları çek, parse et, DB'ye kaydet.

    Returns:
        İşlenen yeni mesaj sayısı.
    """
    global _last_update_id

    try:
        # Restart sonrasi offset None ise, offset gondermeden cagir
        # → Telegram API tum pending update'leri verir.
        # Duplicate kontrolu DB'de telegram_message_id ile yapilir.
        req_offset = _last_update_id  # None ise offset parametresi gonderilmez
        updates = await fetch_telegram_updates(bot_token, offset=req_offset)
    except Exception as e:
        logger.error("Telegram API baglanamadi: %s", e)
        return 0

    # Periyodik heartbeat log — 5 dakikada bir (bos olsa bile)
    import time
    global _last_heartbeat
    now = time.time()
    if now - _last_heartbeat > 300:
        logger.info(
            "Telegram poller heartbeat: offset=%s, pending_updates=%d, last_update_id=%s",
            req_offset, len(updates), _last_update_id,
        )
        _last_heartbeat = now

    if not updates:
        return 0

    logger.info("Telegram: %d update geldi (offset=%s)", len(updates), req_offset)

    new_count = 0
    skipped_chat = 0
    skipped_notext = 0
    skipped_unknown_type = 0
    skipped_duplicate = 0

    async with async_session() as session:
        for update in updates:
            update_id = update.get("update_id", 0)

            # Offset guncelle (bir sonraki sorgu icin)
            _last_update_id = update_id + 1

            # Channel post veya message
            message = update.get("channel_post") or update.get("message")
            if not message:
                continue

            msg_chat_id = str(message.get("chat", {}).get("id", ""))
            if msg_chat_id != chat_id:
                skipped_chat += 1
                continue

            text = message.get("text", "")
            if not text:
                skipped_notext += 1
                continue

            telegram_message_id = message.get("message_id")
            if not telegram_message_id:
                skipped_notext += 1
                continue

            # Daha once kaydedilmis mi kontrol et
            existing = await session.execute(
                select(TelegramNews).where(
                    TelegramNews.telegram_message_id == telegram_message_id
                )
            )
            if existing.scalar_one_or_none():
                skipped_duplicate += 1
                continue

            # Mesaj tipini tespit et
            message_type = detect_message_type(text)
            if not message_type:
                skipped_unknown_type += 1
                logger.info(
                    "Telegram: bilinmeyen mesaj tipi, atlandi (msg_id=%s): %.120s",
                    telegram_message_id, text.replace("\n", " "),
                )
                continue

            # Parse — TUM tickerlari al (cok-sembollu KAP duyurulari icin)
            all_tickers = parse_tickers(text)
            # Endeks / BIST'te olmayan ticker'lari ele (XU100, XU030, vs.)
            all_tickers = [t for t in all_tickers if _is_valid_bist_ticker(t)]

            # ── FON / FOREX / EMTIA ELEME (2 katman) ───────────────────────
            # Sadece GERCEK hisseler islensin. OPTGYF, Z30EAF gibi fonlar +
            # BTCTRY/USDJPY gibi forex kodlari AI'a GITMEZ (token israfi) ve
            # bildirim/feed'e dusmez.
            #   1) Pattern (her durumda): rakam iceren / forex / 6+harf+F -> fon
            #   2) Whitelist (yukluyse): company_financials ∪ stock_markets
            # Whitelist bos ise (yukleme bozuk) -> pattern tek basina fallback.
            _equity_wl = await _get_equity_tickers(session)
            _pre_eq = list(all_tickers)

            def _is_real_equity(_t: str) -> bool:
                # Whitelist BIRINCIL ve KESIN: icindeyse gercek hissedir, pattern'e
                # BAKMA (A1CAP, A1YEN gibi rakamli ama gercek hisseler korunur).
                if _equity_wl:
                    return _t.upper() in _equity_wl
                # Whitelist yuklenemedi -> pattern fallback (fon/forex ele)
                return not _looks_like_fund_or_index(_t)

            all_tickers = [t for t in all_tickers if _is_real_equity(t)]
            if _pre_eq and not all_tickers:
                logger.info(
                    "Telegram: hisse olmayan kod atlandi (fon/endeks/forex) — %s (msg_id=%s)",
                    ",".join(_pre_eq), telegram_message_id,
                )

            # Birincil ticker — AI/router/push icin kullanilir
            ticker = all_tickers[0] if all_tickers else None

            if not ticker:
                # Sembol parse edilemedi veya hicbiri gecerli BIST hissesi degil
                logger.info(
                    "Telegram: Gecerli ticker bulunamadi (msg_id=%s) — atlandi",
                    telegram_message_id,
                )
                continue

            if len(all_tickers) > 1:
                logger.info(
                    "Telegram: Cok-sembollu bildirim (msg_id=%s) — %d ticker: %s",
                    telegram_message_id, len(all_tickers), ",".join(all_tickers),
                )

            # Fiyat bilgisi KAYDEDILMEZ (veri ihlali)
            kap_id = parse_kap_id(text)
            expected_date = parse_expected_trading_date(text)
            gap = parse_gap_pct(text)
            prev_close = parse_prev_close(text)
            theo_open = parse_theoretical_open(text)
            pct_change = parse_pct_change(text)  # Seans ici yuzdesel degisim
            sentiment = parse_sentiment(message_type)
            title = build_parsed_title(message_type, ticker)

            # Mesaj tarihini al — UTC olarak kaydet (PostgreSQL timezone=True icin)
            # Telegram API unix timestamp UTC olarak verir
            msg_date_unix = message.get("date")
            msg_date = (
                datetime.fromtimestamp(msg_date_unix, tz=timezone.utc)
                if msg_date_unix else None
            )

            # Matched keyword/baslik — frontend listede gosterilir.
            # Oncelik:
            #   1) Yeni bot formati: "Baslik: <KAP bildirim basligi>" (en iyi — anlamli)
            #   2) Eski Matriks formati: "Iliskilendirilen Haber Detayi: <keyword>"
            #   3) Fallback: ticker
            matched_kw = ""

            # 1) Yeni format — "Baslik:" satiri
            news_title = parse_news_title(text)
            if news_title:
                matched_kw = news_title

                # ── Pre-filter: Rutin/idari formalite mi? ──
                # Sorumluluk Beyani, Faaliyet Raporu, Genel Kurul, Sirket Genel Bilgi Formu vb.
                # Bu basliklar AI'a GONDERILMEZ (token tasarrufu).
                # AMA: Tum KAP Haber sekmesinde gorunmesi icin kap_all_disclosures'a
                # Notr/5.0 olarak DOGRUDAN yazilir. AI Pozitif Haber sekmesine dusmez
                # (cunku score 5.0 < 6.0 esik).
                _is_routine = False
                try:
                    from app.services.kap_all_analyzer import _is_routine_admin_disclosure
                    _is_routine = _is_routine_admin_disclosure(news_title, "")
                except Exception as e:
                    logger.warning("Telegram pre-filter hatasi: %s", e)

                if _is_routine:
                    logger.info(
                        "Telegram: Rutin bildirim — AI atlandi, Notr olarak kaydedilecek (msg_id=%s, ticker=%s, baslik='%s')",
                        telegram_message_id, ticker, news_title[:50],
                    )
                    # Bu flag asagida ai_news_scorer cagrisini bypass eder
                    # ve dogrudan kap_all_disclosures'a Notr/5.0 yazimini tetikler

            # 2) Eski format — "Iliskilendirilen Haber Detayi"
            if not matched_kw and kap_id:
                detail_match = re.search(
                    r"[İI]li[sş]kilendirilen\s+Haber\s+Detay[ıiİ]:\s*\n?(.+)",
                    text, re.IGNORECASE
                )
                if detail_match:
                    raw_kw = detail_match.group(1).strip()
                    # "Haber Detayi Bulunamadi" gibi anlamsiz degerler atlanir
                    if raw_kw and "BULUNAMADI" not in raw_kw.upper():
                        matched_kw = raw_kw

            # 3) Fallback — ticker
            if not matched_kw:
                matched_kw = ticker or ""

            # Parsed body — fiyat bilgisi olmadan temiz format
            # Sembol satiri, keyword, % degisim/gap/tarih bilgisi
            body_parts = [f"Sembol: {ticker or '???'}"]
            if matched_kw and matched_kw != ticker:
                body_parts.append(matched_kw)
            if message_type == "seans_ici_pozitif" and pct_change:
                body_parts.append(f"G.Fark: {pct_change}")
            elif message_type == "seans_disi_acilis" and gap is not None:
                body_parts.append(f"Gap: %{gap}")
            elif message_type == "borsa_kapali" and expected_date:
                body_parts.append(f"Beklenen İşlem Günü: {expected_date.isoformat()}")
            parsed_body = "\n".join(body_parts)

            # --- AI Puanlama V3 (TradingView icerik + Abacus AI gpt-4o) ---
            # Sadece seans_ici ve borsa_kapali icin AI yorum uret
            # seans_disi_acilis sadece gap verisi — AI yoruma gerek yok
            ai_score = None
            ai_summary = None
            ai_hashtags = []
            kap_url = None

            # Rutin formaliteler (Sorumluluk Beyani, Faaliyet Raporu, Genel Kurul vb)
            # → AI'a GONDERILMEZ, dogrudan Notr/5.0 atanir. Token tasarrufu.
            # Yine de kap_all_disclosures'a Notr olarak yazilir → Tum KAP Haber'de gorunur.
            # Ancak GERCEK KAP URL'sini cekmek icin TradingView'dan ufak bir fetch yapilir
            # (icerik AI'a gitmez, sadece KAP linki parse edilir).
            if _is_routine:
                ai_score = 5.0
                ai_summary = f"{news_title.strip()} — rutin/idari bildirim. Hisse fiyatina dogrudan etkisi beklenmemektedir."
                logger.info("Rutin bildirim: AI atlandi, Notr/5.0 atanan: %s — %s", ticker, news_title[:50])

                # KAP URL'sini TradingView'dan cek (sadece URL, icerik yok)
                if kap_id:
                    try:
                        from app.services.ai_news_scorer import fetch_tradingview_content
                        tv_data = await fetch_tradingview_content(kap_id)
                        if tv_data and tv_data.get("real_kap_url"):
                            kap_url = tv_data["real_kap_url"]
                    except Exception as _kap_url_err:
                        logger.debug("Rutin bildirim KAP URL fetch hatasi: %s", _kap_url_err)
            elif ticker and message_type in ("seans_ici_pozitif", "borsa_kapali"):
                try:
                    from app.services.ai_news_scorer import analyze_news
                    ai_result = await analyze_news(
                        ticker, text, matriks_id=kap_id,
                    )
                    ai_score = ai_result.get("score")
                    ai_summary = ai_result.get("summary")
                    ai_hashtags = ai_result.get("hashtags", [])
                    kap_url = ai_result.get("kap_url")
                    if ai_score:
                        logger.info(
                            "AI puanlama: %s — skor=%s, kap=%s",
                            ticker, ai_score, kap_url or "yok",
                        )
                except Exception as ai_err:
                    logger.warning("AI puanlama hatasi (%s): %s", ticker, ai_err)

                # ★ BUYBACK SKORU — DAİMA deterministik (GÜNLÜK tutar bazlı).
                # KRİTİK: AI, KAP metnindeki PROGRAM TOPLAMI'nı (ör. GOKNR 42.9M)
                # o günkü alımmış gibi okuyup skoru şişiriyordu → bildirim "6.2 Hafif
                # Olumlu" gidiyor ama app feed Nötr 5.0 gösteriyordu (tutarsızlık).
                # Buyback'lerde o GÜNKÜ işlem tutarı KAP formundan kesin parse edilir;
                # AI'a güvenmeyip her zaman günlük-tutar eşiğine göre skor + özet ver.
                # Böylece bildirim ile app feed AYNI olur; rutin/küçük alımlar Nötr kalır.
                try:
                    from app.services.buyback_processor import (
                        is_buyback as _isbb, is_buyback_program as _isbbprog,
                        buyback_score_and_summary,
                    )
                    _bb_parsed = (session.info or {}).get("last_buyback_parsed") if hasattr(session, 'info') else None
                    _bb_ticker = (session.info or {}).get("last_buyback_ticker") if hasattr(session, 'info') else None
                    if _bb_parsed and _bb_ticker and _bb_ticker == (ticker or "").upper():
                        # GÜNLÜK İŞLEM (lot/fiyat parse edildi) → deterministik Nötr-civari skor.
                        fb_score, fb_summary = buyback_score_and_summary(_bb_parsed, ticker)
                        logger.info(
                            "Buyback deterministik skor: %s — gunluk total_tl=%s, AI=%s -> skor=%.1f (override)",
                            ticker, _bb_parsed.get("total_tl"), ai_score, fb_score,
                        )
                        ai_score = fb_score
                        ai_summary = fb_summary
                        ai_hashtags = ai_hashtags or ["paygerialim"]
                        session.info.pop("last_buyback_parsed", None)
                        session.info.pop("last_buyback_ticker", None)
                    elif _isbb(news_title or title or ""):
                        if _isbbprog(news_title or title or "", text or ""):
                            # YENİ GERİ ALIM PROGRAMI duyurusu (karar) → AI skoruna DOKUNMA
                            # (program başlatılması gerçek pozitif: şirket güveni sinyali).
                            logger.info("Buyback PROGRAM duyurusu (%s): AI skor %s korunuyor (pozitif olabilir)", ticker, ai_score)
                        elif ai_score is None or ai_score > 5.5:
                            # Günlük işlem ama tutar parse edilemedi → AI'ın şişmiş skoruna
                            # güvenme, Nötr'e çek (pozitif push gitmesin).
                            logger.info("Buyback gunluk parse fail (%s): AI skor %s -> 5.5 Nötr (cap)", ticker, ai_score)
                            ai_score = 5.5
                            ai_summary = ai_summary or (
                                f"{ticker} pay geri alım programı kapsamında günlük işlem bildirimi. "
                                "Rutin nitelikte; tek başına büyük fiyat etkisi beklenmez."
                            )
                except Exception as _bb_fb_err:
                    logger.warning("Buyback skor override hata (%s): %s", ticker, _bb_fb_err)

            # ── GARANTİ SKOR — hiçbir öğe puansız (bare kart) kalmasın ──
            # Pay alım-satım / seans-dışı gibi AI dalına girmeyen tipler ai_score=None
            # kalıp feed'de puansız "KAP Bildirimi" kartı oluyordu. Kural: puan yoksa
            # da Nötr 5.0 ver. Pay alım-satımda yön (alım/satım) belirlenebiliyorsa ona göre.
            if ai_score is None:
                _full = f"{news_title or ''} {text or ''}".lower()
                _is_pas = any(k in _full for k in (
                    "pay alım", "pay alim", "alım-satım", "alim-satim",
                    "pay alış", "pay alis", "pay satış", "pay satis", "pay alim satim",
                ))
                if _is_pas and ticker:
                    # share_transaction_details'tan pay oranı değişimine bak (alım=+, satım=-)
                    _dir = None
                    _r = None
                    try:
                        from app.models.share_transaction_detail import ShareTransactionDetail as _STD
                        from sqlalchemy import select as _sel2, desc as _desc2
                        _r = (await session.execute(
                            _sel2(_STD).where(_STD.ticker == (ticker or "").upper())
                            .order_by(_desc2(_STD.created_at)).limit(1)
                        )).scalar_one_or_none()
                        if _r is not None and getattr(_r, "pay_orani_change_pct", None) is not None:
                            _dir = float(_r.pay_orani_change_pct)
                    except Exception:
                        _dir = None
                    # Esikler (sermaye orani degisimi):
                    # |%0.3| alti -> Notr (mikro/insider rutin)
                    # %0.3-1     -> hafif sinyal (6.3 / 4.0)
                    # %1-3       -> belirgin (6.8 / 3.5)
                    # %3-5       -> guclu (7.3 / 2.8)
                    # >%5        -> cok guclu (7.8 / 2.3)
                    # TAM CÜMLE özet: taraf + oran + yön + bağlamlı yorum (kuru fragman değil)
                    def _pf(x):  # %1,23 formatı
                        return f"%{abs(float(x)):.2f}".replace(".", ",")
                    _party = (getattr(_r, "party_name", None) or "").strip() if _r is not None else ""
                    if not _party or _party in ("?", "Bilinmiyor"):
                        _party = "Önemli bir pay sahibi"
                    _paynow = getattr(_r, "pay_orani_pct", None) if _r is not None else None
                    _now_s = (f"; toplam payı {_pf(_paynow)} seviyesine geldi"
                              if isinstance(_paynow, (int, float)) else "")
                    # ── ARASE fix (11.06.2026): YON CELISKISI — AI ozet varsa o KAZANIR ──
                    # DB'deki son share_transaction satiri KARSI TARAFIN kaydi olabilir:
                    # aile ici pay devrinde satan %dusus yazar ama alanin satiri (+%)
                    # daha yeni oldugu icin yon "alim" cikiyordu → patron hisse satarken
                    # 6.8 'Hafif Olumlu' + negatif AI ozeti birlikte yayinlandi.
                    if ai_summary and _dir is not None:
                        _sum_l = ai_summary.lower()
                        _neg_cue = any(k in _sum_l for k in (
                            "geril", "azal", "düşüş", "dusus", "düşmüş", "dusmus",
                            "satış baskısı", "satis baskisi", "arz baskısı", "arz baskisi",
                            "olumsuz sinyal", "olumsuz bir sinyal", "güven kaybı", "guven kaybi",
                        ))
                        _pos_cue = any(k in _sum_l for k in (
                            "artır", "artir", "yükselt", "yukselt", "yükselmiş", "yukselmis",
                            "güven sinyali", "guven sinyali", "olumlu sinyal",
                        ))
                        if _neg_cue and not _pos_cue and _dir > 0:
                            logger.info(
                                "Garanti skor YON DUZELTME (%s): DB satiri 'alim' diyor ama "
                                "AI ozet acikca satis/azalis anlatiyor → SATIS yonu esas alindi",
                                ticker,
                            )
                            _dir = -abs(_dir)
                        elif _pos_cue and not _neg_cue and _dir < 0:
                            _dir = abs(_dir)
                        # Buyukluk: ozette "%X'dan %Y'e" varsa gercek degisim oradan
                        try:
                            _pcts = [float(p.replace(",", ".")) for p in re.findall(r"%\s*([\d]+[.,]?[\d]*)", ai_summary)[:2]]
                            if len(_pcts) == 2 and abs(_pcts[0] - _pcts[1]) > 0:
                                _dir = (abs(_pcts[0] - _pcts[1])) * (1 if _dir > 0 else -1)
                        except (ValueError, TypeError):
                            pass

                    if _dir is not None:
                        _abs = abs(_dir)
                        _alim = _dir > 0
                        _fiil = "artırdı" if _alim else "azalttı"
                        if _abs < 0.3:
                            ai_score = 5.0
                            ai_summary = ai_summary or (
                                f"{_party}, {ticker} sermayesindeki payını {_pf(_abs)} oranında {_fiil}{_now_s}. "
                                "Çok küçük ölçekli, mikro nitelikte bir işlem; fiyata doğrudan etki beklenmez.")
                        elif _abs < 1.0:
                            ai_score = 6.3 if _alim else 4.0
                            ai_summary = ai_summary or (
                                f"{_party}, {ticker} sermayesindeki payını {_pf(_abs)} oranında {_fiil}{_now_s}. "
                                + ("İçeriden hafif alım; sınırlı da olsa güven sinyali olarak okunabilir."
                                   if _alim else "Hafif satış; küçük çaplı pozisyon azaltımı, etkisi sınırlı."))
                        elif _abs < 3.0:
                            ai_score = 6.8 if _alim else 3.5
                            ai_summary = ai_summary or (
                                f"{_party}, {ticker} sermayesindeki payını {_pf(_abs)} oranında {_fiil}{_now_s}. "
                                + ("İçeriden belirgin alım — yönetimin/ortağın hisseye güveni açısından olumlu sinyal."
                                   if _alim else "Belirgin satış — pozisyon azaltımı; yatırımcı açısından temkinli bir sinyal."))
                        elif _abs < 5.0:
                            ai_score = 7.3 if _alim else 2.8
                            ai_summary = ai_summary or (
                                f"{_party}, {ticker} sermayesindeki payını {_pf(_abs)} oranında {_fiil}{_now_s}. "
                                + ("Güçlü içeriden alım — kayda değer bir güven göstergesi, fiyat destekleyici olabilir."
                                   if _alim else "Güçlü satış — kayda değer bir pozisyon azaltımı; arz baskısı yaratabilir, olumsuz sinyal."))
                        else:
                            ai_score = 7.8 if _alim else 2.3
                            ai_summary = ai_summary or (
                                f"{_party}, {ticker} sermayesindeki payını {_pf(_abs)} gibi yüksek bir oranda {_fiil}{_now_s}. "
                                + ("Çok güçlü içeriden alım — büyük ölçekli güven sinyali, fiyat üzerinde olumlu etki potansiyeli."
                                   if _alim else "Çok güçlü satış — büyük ölçekli çıkış; ciddi arz baskısı ve güven kaybı sinyali, olumsuz."))
                    else:
                        ai_score = 5.0
                        ai_summary = ai_summary or (
                            f"{_party}, {ticker} paylarında alım-satım işlemi gerçekleştirdi. "
                            "İşlem ölçeği netleşmediğinden fiyata doğrudan etki beklenmez.")
                else:
                    ai_score = 5.0
                    ai_summary = ai_summary or f"{(news_title or '').strip()} — bildirim. Fiyata doğrudan etki beklenmemektedir."

                # AILE ICI DEVIR yumusatmasi: paylar piyasaya satilmiyor, gercek
                # arz baskisi yok → satis yonlu skor en fazla hafif-temkinli (4.2)
                _aile_blob = f"{ai_summary or ''} {text or ''}".lower()
                if ai_score is not None and ai_score < 4.2 and any(
                    k in _aile_blob for k in ("aile üyeleri", "aile uyeleri", "aile içi", "aile ici", "aile bireyleri")
                ):
                    logger.info(
                        "Garanti skor AILE-ICI yumusatma (%s): %.1f -> 4.2 (piyasaya satis yok)",
                        ticker, float(ai_score),
                    )
                    ai_score = 4.2
                logger.info("Garanti skor uygulandı (%s): ai_score=%s", ticker, ai_score)

            # ── SON GUARDRAIL: skor-özet tutarlılık (pozitif özet → Nötr skor paradoksu) ──
            # analyze_news içindeki guardrail BAZI path'lerde atlanabiliyor:
            #   - analyze_news skor=None döndürüp garanti-skor 5.0 atadığında
            #   - skor düşük dönüp guardrail çağrılmadığında
            # Bu SON pass her durumda (özet varsa) çalışır → özet "olumlu/pozitif"
            # diyorsa skor en az 6.2'ye, "olumsuz/negatif" diyorsa en fazla 3.8'e çekilir.
            # FORTE örneği: özet "pozitif sinyal, olumlu yansıma" diyordu ama skor 5.0
            # kalmıştı (Yeni İş İlişkisi). Hangi path'ten gelirse gelsin artık tutarlı.
            # BUYBACK MUAFIYETI: pay geri alımı skoru o günkü tutara göre deterministik
            # ve OTORİTERDİR. Bu guardrail "şirket güveni" gibi pozitif kelimeler görünce
            # skoru 6.2'ye çekip Nötr buyback'i yanlışlıkla "pozitif"e çeviriyordu →
            # rutin geri alımda pozitif push gidiyordu. Buyback'lerde guardrail ATLANIR.
            _is_bb_now = False
            try:
                from app.services.buyback_processor import is_buyback as _is_bb_fn
                _is_bb_now = _is_bb_fn(news_title or title or "")
            except Exception:
                _is_bb_now = False
            if (not _is_bb_now) and ai_summary and ai_score is not None and message_type in ("seans_ici_pozitif", "borsa_kapali"):
                try:
                    from app.services.ai_news_scorer import _validate_score_against_content
                    # Baslik da icerige eklenir: "Yeni Is Iliskisi" gibi olay tipi
                    # basligi icerikte gecmese bile floor/kategori kurallari tetiklensin
                    # (CWENE: baslik "Yeni Is Iliskisi" -> asla notr olmamali).
                    _vc_text = ((news_title or "") + "\n" + (text or "")).strip()
                    _adj = _validate_score_against_content(
                        float(ai_score), _vc_text, ticker or "", ai_summary=ai_summary
                    )
                    if _adj is not None and abs(float(_adj) - float(ai_score)) >= 0.1:
                        logger.info(
                            "Son guardrail (%s): skor %.1f -> %.1f (özet framing tutarlılık)",
                            ticker, float(ai_score), float(_adj),
                        )
                        ai_score = round(float(_adj), 1)
                except Exception as _gerr:
                    logger.warning("Son guardrail hatası (%s): %s", ticker, _gerr)

            # KAP URL yoksa — once KAP'in KENDI sorgusundan baslik eslestir (TV'ye bagimsiz).
            # Rutin bilanco bildirimleri (Faaliyet Raporu, Sorumluluk Beyani, Ozkaynaklar
            # Degisim) TradingView'da KAP linki icermez; KAP kaynak oldugu icin burada
            # gercek Bildirim url'si bulunur. Boylece bilanco bildirimleri de diger
            # haberler gibi ORJINAL KAP linki gosterir (TV linki degil).
            # ★ FIX (HALKB 6511839): eski kosul `if not kap_url` idi — ama analyze_news
            # HER ZAMAN en azindan TradingView linki dondurur (TV'de haber olmasa,
            # 404 olsa bile). Bu yuzden baslik eslestirme HIC calismiyordu ve gercek
            # kap.org.tr linki bulunamayinca haber atiliyordu. Yeni kosul: gercek
            # KAP linki YOKSA (bos veya TV linki) baslik eslestirmeyi DENE.
            if ticker and news_title and "kap.org.tr" not in (kap_url or ""):
                try:
                    from app.services.ai_news_scorer import resolve_kap_url_by_title
                    _rk = await resolve_kap_url_by_title(ticker, news_title)
                    if _rk:
                        kap_url = _rk
                        logger.info("KAP url baslik eslestirme: %s — %s (%s)", ticker, kap_url, news_title[:40])
                except Exception as _rk_err:
                    logger.debug("KAP url baslik eslestirme hatasi (%s): %s", ticker, _rk_err)

            # ★ KAP URL kontrolu — ESKI DAVRANIS: gercek kap.org.tr linki yoksa haber
            # tamamen ATILIYORDU (TradingView gec indeksleyince haber sonsuza dek
            # kayboluyordu — HALKB 6511839 vakasi, 11.06.2026). YENI DAVRANIS:
            # Ticker zaten ustte BIST equity whitelist'inden gecti (fon/forex/yabanci
            # AMUNDI tipi bildirimler oraya takilir) → gercek hissenin haberi KESINLIKLE
            # kaydedilir. KAP linki yoksa kap_url=None yazilir; "KAP URL Zenginlestirici"
            # job'i (15 dk'da bir) linki sonradan bulup doldurur.
            _has_real_kap_url = bool(kap_url) and "kap.org.tr" in (kap_url or "")
            if not _has_real_kap_url:
                kap_url = None  # TV 404 linki yazma — zenginlestirici sonra doldurur
                logger.warning(
                    "KAP url henuz yok — haber YINE DE kaydediliyor (url sonradan "
                    "doldurulacak): ticker=%s, baslik='%s', matriks_id=%s",
                    ticker, (news_title or '')[:60], kap_id,
                )

            # ──────────────────────────────────────────────────────────────────
            # YENI: kap_all_disclosures'a yaz — Tum KAP Haber sekmesi icin
            # ──────────────────────────────────────────────────────────────────
            # Telegram bot artik primary kaynak. Tum sentiment'leri (Olumlu/Notr/
            # Olumsuz) kap_all_disclosures'a yazar — boylece Tum KAP Haber sekmesi
            # buradan beslenir. AI Pozitif Haber sekmesi telegram_news'den okumaya
            # devam eder (sadece score >= 6).
            #
            # Admin panelden "kap_primary_source" ayariyla kontrol edilir:
            #   - "telegram"  → buraya yazilir (default)
            #   - "uzmanpara" → yazilmaz (Uzmanpara/BigPara primary)
            #   - "both"      → yazilir (Uzmanpara da paralel calisir)
            try:
                from app.services.kap_source_setting import get_kap_source, is_telegram_active
                kap_source = await get_kap_source()
            except Exception:
                kap_source = "telegram"  # Default — guvenli yol

            # ── DEBOUNCE: ayni ticker icin 2 dk icinde 2. pozitif KAP gelirse skip ──
            # Push spam ve KAP feed'inde duplicate olmamasi icin. BIST 50 ve BIST Tum
            # icin de gecerli (channel ayrimi yok, ticker bazli).
            import time as _time_mod
            _now_ts = _time_mod.time()
            # Çok-ticker skor birleştirme/pozitif-liste için (gate atlansa bile tanımlı kalsın)
            _pt_objs: dict = {}
            _is_positive_dup = False
            if ticker and ai_score is not None and ai_score >= 6 and message_type in ("seans_ici_pozitif", "borsa_kapali"):
                _last_push = _last_kap_push_per_ticker.get(ticker.upper())
                if _last_push and (_now_ts - _last_push) < _KAP_PUSH_COOLDOWN_SEC:
                    _is_positive_dup = True
                    logger.info(
                        "KAP push debounce: %s — son push %.0fs once, %ds cooldown — atlandi",
                        ticker, _now_ts - _last_push, _KAP_PUSH_COOLDOWN_SEC,
                    )

            if (
                ticker
                and ai_score is not None
                and message_type in ("seans_ici_pozitif", "borsa_kapali")
                and is_telegram_active(kap_source)
                and not _is_positive_dup
            ):
                try:
                    from app.models.kap_all_disclosure import KapAllDisclosure
                    from app.scrapers.kap_all_scraper import _infer_category
                    from sqlalchemy import select as _sa_select

                    # Sentiment çıkarımı (ai_news_scorer score'dan):
                    # Yeni 9 kategorili etiket sistemi
                    try:
                        from app.utils.ai_score_label import score_to_label
                        ka_sentiment = score_to_label(ai_score) or "Nötr"
                    except Exception:
                        # Fallback: eski 3'lu sistem
                        if ai_score >= 6.0:
                            ka_sentiment = "Olumlu"
                        elif ai_score < 4.5:
                            ka_sentiment = "Olumsuz"
                        else:
                            ka_sentiment = "Nötr"

                    # Title — yeni bot "Baslik:" satirindan, fallback matched_kw
                    ka_title = (news_title or matched_kw or title or "")[:500]

                    # Kategori + is_bilanco — frontend "Bilanco AI Analizi - COK YAKINDA"
                    # badge'ini bu flag'e gore gosterir. KAP scraper ile ayni mantik.
                    ka_category = _infer_category(ka_title)
                    # v3.1: is_bilanco SADECE "Finansal Durum Tablosu (Bilanço)" icin True
                    # Diger mali tablo bildirimleri RUTIN -> "Mali Tablo Eki" kategorisinde, notr.
                    ka_is_bilanco = ka_category == "Bilanço/Finansal Rapor"

                    # ── DUPLICATE KONTROLU: (kap_url + company_code + TITLE) uzerinden ──
                    # Cok-sembollu bildirimlerde ayni kap_url N farkli ticker icin yazilir
                    # (ticker kontrole dahil). AYRICA: bir sirketin TUM finansal raporu
                    # KAP'ta TEK bildirim numarasi altinda toplanir (orn 1611638) ama matriks
                    # her tabloyu (Finansal Durum Tablosu / Kar-Zarar / Nakit Akis / Ozkaynaklar)
                    # AYRI mesaj olarak yollar — hepsi AYNI kap_url'e cozumlenir. Eski dedup
                    # (kap_url+ticker) bunlari "duplicate" sanip ilkini yazip kalanini atiyordu;
                    # "Finansal Durum Tablosu (Bilanço)" son sirada gelirse DUSUYOR -> is_bilanco
                    # hic yazilmiyor -> bilanco pipeline HIC tetiklenmiyordu (BRMEN/MEPET bug'i).
                    # TITLE'i da anahtara ekleyerek her farkli tablo ayri yazilir, bilanco garanti.
                    # ── PER-TICKER ANALİZ (çok-tickerlı / zıt-yönlü bildirimler) ──
                    # Tek bildirim birden çok hisseyi ZIT yönde etkileyebilir:
                    # endeks değişikliğinde biri DAHIL (pozitif), diğeri ÇIKARILDI
                    # (negatif); pay devrinde alan/satan farklı yönde. Eskiden primary
                    # ticker'ın tek analizi HER tickera aynen yazılıyordu (FADE pozitif
                    # özeti TRILC'e de basıldı). Artık 2-4 tickerlı bildirimlerde her
                    # ticker AYRI ve o hisseye SABİTLENMİŞ analiz edilir.
                    _multi_opposed = (2 <= len(all_tickers) <= 4) and bool(text)

                    # ── AFFILIATE GUARD (tek-konulu kategoriler) ──
                    # "Borsada İşlem Gören Tipe Dönüşüm" gibi bildirimlerde KAP "İlgili
                    # Şirketler" alanı holding grubundaki tüm şirketleri listeler (DARDL,
                    # ENDAE, MAGEN) ama dönüşüm SADECE asıl şirketi (DARDL) ilgilendirir.
                    # parse_tickers hepsini çekip her birine kart açıyordu (DARDL metni
                    # ENDAE/MAGEN'e de basılıyordu). Kural: tek-konulu kategoride yalnızca
                    # AI özetinde adı geçen (asıl konu) ticker kart alır. Özette hiçbir
                    # ticker geçmiyorsa güvenli taraf: hepsini bırak (drop etme).
                    _single_subject_cat = False
                    try:
                        from app.services.kap_category_processors import is_type_conversion as _is_tc
                        _single_subject_cat = _is_tc(ka_title)
                    except Exception:
                        _single_subject_cat = False
                    _summary_uc = (ai_summary or "").upper()
                    _subjects_in_summary = [t for t in all_tickers if t and t.upper() in _summary_uc]

                    # Yazılan kap_disc objeleri (çok-ticker skor birleştirme + per-ticker
                    # chatbox/push için loop sonrası kullanılır)
                    _pt_objs: dict = {}

                    for _tk in all_tickers:
                        # Affiliate drop: tek-konulu kategori + özette asıl konu belli +
                        # bu ticker özette yoksa → kart açma.
                        if (
                            _single_subject_cat
                            and _subjects_in_summary
                            and (_tk or "").upper() not in _summary_uc
                        ):
                            logger.info(
                                "Affiliate atlandı (tek-konulu kategori): %s — asıl konu=%s",
                                _tk, ",".join(_subjects_in_summary),
                            )
                            continue
                        # Varsayılan: primary'nin paylaşılan değerleri
                        _tk_score = ai_score
                        _tk_summary = ai_summary
                        _tk_sentiment = ka_sentiment
                        # ── ÇOK-ŞİRKETLİ LİSTE DUYURULARI (SPK işlem yasağı vb.) ──
                        # "SPK İşlem Yasağı Nedeniyle Pay Duyurusu" tek bildirimde 10+
                        # şirketi listeler; her şirketin dönüştürülen nominal tutarı EK
                        # tablodadır ve birbirinden ÇOK farklıdır (bazısında 1 lot bile yok).
                        # Primary analiz (örn DERHL'in 1.5M TL'si) diğer 13 şirkete aynen
                        # kopyalanıyordu. Kural: bu kategoride her ticker'a RAKAMSIZ,
                        # şirkete-özel NÖTR şablon yazılır — yanlış şirket verisi yazılmaz.
                        _title_low_pt = (ka_title or "").lower().replace("̇", "")
                        if ("işlem yasağı" in _title_low_pt or "islem yasag" in _title_low_pt) and len(all_tickers) >= 2:
                            _tk_score = 5.0
                            _tk_sentiment = "Nötr"
                            _tk_summary = (
                                f"SPK kararı doğrultusunda, işlem yasağı getirilen yatırımcılara ait {_tk} "
                                "payları MKK tarafından borsada işlem görmeyen statüye dönüştürüldü. Bu, şirket "
                                "faaliyetleriyle ilgili olmayan teknik/idari bir işlemdir; dönüştürülen tutar "
                                "şirketten şirkete farklılık gösterir (şirkete özel tutar KAP ekindedir). "
                                "Fiyat üzerinde doğrudan etki beklenmez."
                            )
                        elif _multi_opposed:
                            try:
                                from app.services.ai_news_scorer import analyze_news as _an_pt
                                _anchor = (
                                    f"[ÖNEMLİ — ÖZNE KONTROLÜ: Bu bildirimi YALNIZCA {_tk} hissesi "
                                    f"açısından değerlendir.\n"
                                    f"ÖNCE belirle: Haberdeki eylemin/işlemin ASIL ÖZNESİ "
                                    f"(yapan/kazanan/satan/açıklayan şirket) {_tk} mi, yoksa {_tk} haberde "
                                    f"sadece KARŞI TARAF / işlemi DÜZENLEYEN / müşteri / bağlam olarak mı geçiyor?\n"
                                    f"• Eğer asıl özne {_tk} DEĞİLSE (örn: ihaleyi {_tk} DÜZENLEDİ ama KAZANAN "
                                    f"başka şirket; ya da satışı {_tk} açtı ama ALAN başka şirket): bu haber {_tk} "
                                    f"İÇİN doğrudan gelişme DEĞİLDİR → score=5.0 NÖTR ver; özette 'Bu bildirim başka "
                                    f"bir şirketin işlemine ilişkindir; {_tk} yalnızca karşı taraf/düzenleyen olarak "
                                    f"geçmektedir, {_tk} için doğrudan etki taşımaz' de. Başka şirketin yaptığı işlemi "
                                    f"{_tk} yapmış gibi YAZMA; 'şirketimiz' ifadesini {_tk} sanma.\n"
                                    f"• Asıl özne gerçekten {_tk} ise normal değerlendir.\n"
                                    f"Puan ve özet {_tk} için olmalı.]\n{text}"
                                )
                                _rr = await _an_pt(_tk, _anchor, matriks_id=kap_id)
                                if _rr and _rr.get("score") is not None:
                                    _tk_score = _rr["score"]
                                    if _rr.get("summary"):
                                        _tk_summary = _rr["summary"]
                                    try:
                                        from app.utils.ai_score_label import score_to_label as _s2l_pt
                                        _tk_sentiment = _s2l_pt(_tk_score) or ka_sentiment
                                    except Exception:
                                        pass
                                    logger.info(
                                        "Per-ticker analiz: %s skor=%.1f (primary=%s skor=%s)",
                                        _tk, _tk_score, ticker, ai_score,
                                    )
                            except Exception as _pte:
                                logger.debug("Per-ticker analiz hata (%s): %s", _tk, _pte)

                        is_duplicate = False
                        if kap_url:
                            existing_check = await session.execute(
                                _sa_select(KapAllDisclosure.id).where(
                                    KapAllDisclosure.kap_url == kap_url,
                                    KapAllDisclosure.company_code == _tk,
                                    KapAllDisclosure.title == ka_title,
                                ).limit(1)
                            )
                            is_duplicate = existing_check.scalar_one_or_none() is not None

                        if is_duplicate:
                            logger.debug(
                                "kap_all_disclosures duplicate atlandi: %s — %s",
                                _tk, ka_title[:50],
                            )
                            continue

                        # ── SKOR-OZET TUTARLILIK (feed icin de uygula) ──────
                        # Notification guardrail bazi message_type'larda atlaniyor;
                        # kap_all feed skoru ham AI skoruyla yazilip ozetle celisiyordu
                        # (KAYSE: ozet "hafif olumsuz" ama skor 5.0 Notr feed'de). Her
                        # durumda dogrula — content+ozet birlikte verilir.
                        try:
                            from app.services.ai_news_scorer import _validate_score_against_content as _vsc_feed
                            if _tk_score is not None and _tk_summary:
                                _vc_in = ((text or "") + "\n" + (_tk_summary or "")).strip()
                                _tk_adj = _vsc_feed(float(_tk_score), _vc_in, _tk or "", ai_summary=_tk_summary)
                                if _tk_adj is not None and abs(float(_tk_adj) - float(_tk_score)) >= 0.1:
                                    try:
                                        from app.utils.ai_score_label import score_to_label as _s2l_feed
                                        _tk_sentiment = _s2l_feed(float(_tk_adj)) or _tk_sentiment
                                    except Exception:
                                        pass
                                    logger.info(
                                        "kap_all skor-ozet tutarlilik (%s): %.1f -> %.1f",
                                        _tk, float(_tk_score), float(_tk_adj),
                                    )
                                    _tk_score = round(float(_tk_adj), 1)
                        except Exception as _vfe:
                            logger.debug("kap_all tutarlilik hata (%s): %s", _tk, _vfe)

                        kap_disc = KapAllDisclosure(
                            company_code=_tk,
                            title=ka_title,
                            body=_tk_summary,
                            category=ka_category,
                            is_bilanco=ka_is_bilanco,
                            kap_url=kap_url,
                            source="telegram",
                            published_at=msg_date,
                            ai_sentiment=_tk_sentiment,
                            ai_impact_score=_tk_score,
                            ai_summary=_tk_summary,
                            ai_analyzed_at=datetime.now(timezone.utc),
                        )
                        # Savepoint — beklenmedik hata olursa session korunur
                        try:
                            async with session.begin_nested():
                                session.add(kap_disc)
                                await session.flush()
                            logger.info(
                                "Telegram → kap_all_disclosures yazildi: %s — '%s' (%s, skor=%s)",
                                _tk, ka_title[:50], _tk_sentiment, _tk_score,
                            )
                            _pt_objs[_tk] = kap_disc

                            # ── FAVORI/WATCHLIST BILDIRIMI + notify-bot raporu ──
                            # Bu hisseyi favorisine/portfoyune ekleyen kullanicilara
                            # (notify_kap_watchlist pref + pozitif/negatif/tum filtresine gore)
                            # push gonderir VE notify-bot'a "👥 Watchlist'te N · ✅ Gonderildi N kisi"
                            # raporu atar. Notr dahil — filtreleme fonksiyon icinde, kullanici
                            # tercihine gore yapilir. ESKIDEN uzmanpara scraper
                            # (_process_kap_disclosures) yapiyordu; KAP kaynagi Telegram poller'a
                            # tasininca baglanti koptu (favori bildirimleri + rapor atmaz olmustu) — geri baglandi.
                            # Bilanco-paketi bildirimleri (Finansal Durum, Kar/Zarar,
                            # Ozkaynaklar, Nakit Akis, Faaliyet Raporu, Sorumluluk Beyani)
                            # favoriye TEK TEK push GONDERILMEZ — 5 ayri spam yerine, asagida
                            # (ka_is_bilanco geldiginde) TEK "bilanco aciklandi" bildirimi gider.
                            # Bunlar yine kap_all_disclosures'a (Tum KAP) 5.0 Notr + KAP linki
                            # ile yaziliyor (yukarida); sadece favori PUSH'tan haric tutulur.
                            if not _is_bilanco_package_title(ka_title):
                                try:
                                    from app.services.notification import NotificationService as _WLNS
                                    async with session.begin_nested():
                                        _wl_sent = await _WLNS(db=session).notify_kap_watchlist(kap_disc)
                                    if _wl_sent:
                                        logger.info(
                                            "Favori/watchlist bildirimi gonderildi: %s — %d kullaniciya",
                                            _tk, _wl_sent,
                                        )
                                except Exception as _wl_err:
                                    logger.warning("Favori/watchlist bildirim hatasi (%s): %s", _tk, _wl_err)

                            # ── ROUTER: 6 ozel takvime dagit ──
                            _route_override = None
                            try:
                                async with session.begin_nested():
                                    _route_override = await _route_to_calendars(
                                        session,
                                        disclosure_id=kap_disc.id,
                                        ticker=_tk,
                                        company_name=None,
                                        title=ka_title,
                                        # BUG-3 fix: bu ticker'a ait özet (_tk_summary) —
                                        # primary'nin özeti değil; multi-opposed'da yanlış
                                        # ticker özeti ile takvim sınıflandırması yapılıyordu
                                        body=_tk_summary or ai_summary,
                                        kap_url=kap_url,
                                        published_at=msg_date,
                                    )
                            except Exception as _route_err:
                                logger.warning(
                                    "KAP router hata (%s): %s", _tk, _route_err,
                                )
                            # PRIMARY ticker için dividend GK-nötr override'ını tweet/push
                            # KARARINA yansıt: GK onayı (dağıtım 5.5 / dağıtmama 4.5) ve ödeme
                            # nötr olduğundan should_notify (>=6) ve negatif-tweet (<4.1)
                            # eşiklerine TAKILMAZ → tweetlenmez/push edilmez (simetrik kural).
                            if (
                                _tk == ticker
                                and _route_override
                                and _route_override.get("score") is not None
                            ):
                                if ai_score != _route_override["score"]:
                                    logger.info(
                                        "Dividend override → tweet/push skoru: %s %s -> %s",
                                        ticker, ai_score, _route_override["score"],
                                    )
                                ai_score = _route_override["score"]
                                if _route_override.get("summary"):
                                    ai_summary = _route_override["summary"]

                            # ── BILANCO PIPELINE ──
                            if ka_is_bilanco:
                                # ── TEK "BILANCO ACIKLANDI" FAVORI BILDIRIMI ──
                                # Bilanco-paketi 5 bildirimi favoriye tek tek gitmez; bunun
                                # yerine burada (Finansal Durum Tablosu = is_bilanco, TEK SEFER)
                                # tek konsolide bildirim gider. Tiklayinca Bilanco sekmesine
                                # (Son Bilancolar — bu hissenin karti) yonlendirir.
                                try:
                                    from app.services.notification import NotificationService as _BNS
                                    async with session.begin_nested():
                                        _bn = await _BNS(db=session).notify_bilanco_announced(_tk)
                                    if _bn:
                                        logger.info(
                                            "Bilanco aciklandi favori bildirimi: %s — %d kullaniciya", _tk, _bn,
                                        )
                                except Exception as _bn_err:
                                    logger.warning("Bilanco aciklandi bildirim hatasi (%s): %s", _tk, _bn_err)

                                try:
                                    # ★ KUYRUK fix (11.06.2026): eskiden create_task ile
                                    # DOGRUDAN paralel baslatiliyordu — 5-10 bilanco pespese
                                    # gelince HEPSI AYNI ANDA calisip sistemi kasiyordu ve
                                    # queue worker (sirali + dinamik bekleme) hic
                                    # kullanilmiyordu. Artik kuyruga eklenir; worker SIRAYLA
                                    # isler, hicbiri atlanmaz, zamana yayilir.
                                    from app.services.bilanco_pipeline import enqueue_bilanco
                                    await enqueue_bilanco(_tk, kap_title=ka_title)
                                    logger.info(
                                        "Bilanco kuyruga eklendi (sirali islenecek): %s — '%s'",
                                        _tk, ka_title[:50],
                                    )
                                except Exception as _bil_err:
                                    logger.warning(
                                        "Bilanco pipeline tetikleme hata (%s): %s", _tk, _bil_err,
                                    )

                            # ── ADMIN TELEGRAM GRUBU: POZITIF KAP BILDIRIMI ──
                            # ★ KOK FIX (EKDMR, 12.06.2026): kanal mesaji DB'ye
                            # YAZILAN skoru/ozeti kullanir (_pt_objs[_tk] = kap_disc),
                            # poller-seviye ai_score'u DEGIL. Eskiden ai_score 'Son
                            # guardrail' ile 6.2'ye cikmis olabiliyordu ama DB per-
                            # ticker re-validasyonla 5.0 yaziyordu -> app Notr,
                            # kanal Pozitif celiskisi. Artik TEK KAYNAK: DB skoru.
                            # "Notrse Notr" — DB 5.0 ise kanala POZITIF gitmez.
                            _pt_db = _pt_objs.get(_tk)
                            _ch_score = (
                                float(_pt_db.ai_impact_score)
                                if (_pt_db is not None and _pt_db.ai_impact_score is not None)
                                else ai_score
                            )
                            _ch_summary = (
                                _pt_db.ai_summary
                                if (_pt_db is not None and _pt_db.ai_summary)
                                else ai_summary
                            )
                            if _ch_score is not None and _ch_score >= 6.0:
                                try:
                                    from app.services.admin_telegram import send_kap_positive_to_admin_group
                                    import asyncio as _asyncio2
                                    _asyncio2.create_task(
                                        send_kap_positive_to_admin_group(
                                            ticker=_tk,
                                            ai_score=_ch_score,
                                            ai_summary=_ch_summary,
                                            kap_url=kap_url,
                                            message_type=message_type,
                                        )
                                    )
                                except Exception as _adm_err:
                                    logger.warning(
                                        "Admin grup pozitif gonderim hata (%s): %s", _tk, _adm_err,
                                    )

                            # ── ADMIN: NEGATIF KAP BILDIRIMI (havuza dustu) ──
                            # Negatiflerden de haberin olmasi icin (pozitiflerle AYNI
                            # kanal). DB skoru <= 4.0 = havuzdaki negatif kart.
                            # KOK FIX: pozitif ile ayni — DB'ye yazilan skor/ozet kullanilir.
                            if _ch_score is not None and _ch_score <= 4.0:
                                try:
                                    from app.services.admin_telegram import send_kap_negative_to_admin_group
                                    import asyncio as _asyncio3
                                    _asyncio3.create_task(
                                        send_kap_negative_to_admin_group(
                                            ticker=_tk,
                                            title=ka_title,
                                            ai_score=_ch_score,
                                            ai_summary=_ch_summary,
                                            kap_url=kap_url,
                                            message_type=message_type,
                                        )
                                    )
                                except Exception as _admn_err:
                                    logger.warning(
                                        "Admin grup negatif gonderim hata (%s): %s", _tk, _admn_err,
                                    )
                        except Exception as _flush_err:
                            logger.warning(
                                "kap_all_disclosures yazma hatasi: %s — %s",
                                _tk, _flush_err,
                            )
                except Exception as _ka_err:
                    logger.warning(
                        "Telegram → kap_all_disclosures yazma hatasi (%s): %s",
                        ticker, _ka_err,
                    )

            # AI skoru kontrolu — 6 altindaki haberler telegram_news'e kaydedilmez,
            # bildirim gitmez, tweet atilmaz. AI Pozitif Haber sekmesinde gorünmez.
            # NOT: kap_all_disclosures'a yukarida ZATEN yazildi — Tum KAP Haber
            # sekmesinde gorünur (sentiment ne olursa olsun).
            # ── ÇOK-TICKER: aynı yön → AYNI skor + pozitif liste + temsili ticker ──
            # Per-ticker analiz LLM gürültüsüyle aynı yöndeki sembollere hafif farklı
            # skor verebiliyordu (BSOKE 7.1 / BTCIM 6.8). Hepsi AYNI yöndeyse (hepsi
            # pozitif / hepsi negatif / hepsi nötr) tek ortak skora eşitlenir. ZIT
            # yönlüyse (FADE+ / TRILC−) dokunulmaz. Ayrıca push/chatbox kapısı (should_notify)
            # eski tek 'ai_score' yerine pozitif sembollerin varlığına bakar (BSOKE/BTCIM
            # vakası: Tüm Haber'de 7.1 ama chatbox/push'a gitmiyordu).
            _pos_tickers = []
            if _pt_objs:
                _sc = {tk: float(d.ai_impact_score) for tk, d in _pt_objs.items()
                       if d.ai_impact_score is not None}
                if len(_sc) > 1:
                    _all_pos = all(s >= 6.0 for s in _sc.values())
                    _all_neg = all(s < 4.1 for s in _sc.values())
                    _all_neu = all(4.1 <= s < 6.0 for s in _sc.values())
                    # B5 fix: ayni-yon birlestirme yalniz skorlar YAKINSA (fark
                    # <= 0.5, LLM gurultusu) yapilir. Fark buyukse her ticker
                    # KENDI skorunu korur — haksiz 8.5'in hafif 6.1'e (veya
                    # hafif 4.0'in en kotunun 2.5'ine) kopyalanmasi onlenir.
                    _spread = max(_sc.values()) - min(_sc.values())
                    if (_all_pos or _all_neg or _all_neu) and _spread <= 0.5:
                        _rep = (max(_sc.values()) if _all_pos
                                else min(_sc.values()) if _all_neg
                                else round(sum(_sc.values()) / len(_sc), 1))
                        for _d in _pt_objs.values():
                            _d.ai_impact_score = _rep
                            try:
                                from app.utils.ai_score_label import score_to_label as _s2l_u
                                _d.ai_sentiment = _s2l_u(_rep) or _d.ai_sentiment
                            except Exception:
                                pass
                        logger.info("Multi-ticker skor birleştirildi (aynı yön): %s -> %.1f",
                                    list(_sc.items()), _rep)
                    elif _all_pos or _all_neg or _all_neu:
                        logger.info(
                            "Multi-ticker skorlar KORUNDU (ayni yon ama fark %.1f > 0.5): %s",
                            _spread, list(_sc.items()),
                        )
                _pos_tickers = [tk for tk, d in _pt_objs.items()
                                if d.ai_impact_score is not None and float(d.ai_impact_score) >= 6.0]
                if _pos_tickers:
                    # Temsili pozitif ticker → telegram_news(chatbox) + genel push bunu kullanır
                    _rep_tk = max(_pos_tickers, key=lambda k: float(_pt_objs[k].ai_impact_score))
                    ticker = _rep_tk
                    ai_score = float(_pt_objs[_rep_tk].ai_impact_score)
                    if _pt_objs[_rep_tk].ai_summary:
                        ai_summary = _pt_objs[_rep_tk].ai_summary
                else:
                    # ★ TEK-KAYNAK SENKRONU (12.06.2026, denetim BUG-2/BUG-4):
                    # Pozitif temsilci YOKSA da poller-seviye ai_score/ai_summary
                    # DB'ye yazilan degere esitlenir. Eskiden 'Son guardrail'
                    # ai_score'u 6.2'ye kaldirmis ama DB re-validasyonla 5.0
                    # yazmis olabiliyordu; negatif-tweet kapisi ve loglar stale
                    # degeri kullaniyordu. Bundan sonraki TUM tuketiciler
                    # (push karari, negatif tweet, log) DB skoruyla calisir.
                    _db_pri = _pt_objs.get((ticker or "").upper()) or next(iter(_pt_objs.values()))
                    if _db_pri.ai_impact_score is not None:
                        if ai_score is not None and abs(float(_db_pri.ai_impact_score) - float(ai_score)) >= 0.1:
                            logger.info(
                                "Tek-kaynak senkron (%s): poller skoru %.1f -> DB %.1f",
                                ticker, float(ai_score), float(_db_pri.ai_impact_score),
                            )
                        ai_score = float(_db_pri.ai_impact_score)
                        if _db_pri.ai_summary:
                            ai_summary = _db_pri.ai_summary

            # ai_score None = AI basarisiz → guvenli yol: kaydet + bildir
            # ★ TUTARLILIK: kap_all feed per-ticker skorlari yazildiysa (_pt_objs),
            # bildirim karari SADECE feed skorlarina (_pos_tickers) bakar. Eski 'ai_score'
            # notification-guardrail ile 6.2'ye cikabiliyor ama feed re-validasyonu 5.0
            # Notr yazabiliyordu → bildirim pozitif gidip DB/app 5.0 kaliyordu (FRIGO).
            # _pt_objs varsa stale ai_score'a GUVENME; yoksa (feed yazilmadi) ai_score fallback.
            if _pt_objs:
                should_notify = (ai_score is None) or bool(_pos_tickers)
            else:
                should_notify = (ai_score is None) or (ai_score is not None and ai_score >= 6)

            if not should_notify:
                logger.info(
                    "AI skoru dusuk (%s < 6), telegram_news + push + tweet atlandi: %s — %s",
                    ai_score, ticker, title,
                )
                # NOT: Admin Telegram'a "AI olumlu bulmadi" bildirimi gonderilmiyor.
                # Yeni sistemde tum KAP haberleri Telegram'dan geldigi icin her bildirimde
                # admin'e mesaj atmak spam yaratiyordu. Sadece log'a yazilir, izlemek icin
                # Render logs kullanilir.
            elif message_type == "seans_disi_acilis":
                # seans_disi_acilis = acilis gap bilgisi, haber degil → DB'ye kaydetme
                logger.info(
                    "seans_disi_acilis DB'ye kaydedilmedi (haber degil): %s — %s",
                    ticker, title,
                )
            else:
                # AI skor >= 6 veya None → DB'ye kaydet
                news = TelegramNews(
                    telegram_message_id=telegram_message_id,
                    chat_id=msg_chat_id,
                    message_type=message_type,
                    ticker=ticker,
                    price_at_time=None,
                    raw_text=text,
                    parsed_title=title,
                    parsed_body=parsed_body,
                    sentiment=sentiment,
                    kap_notification_id=kap_id,
                    expected_trading_date=expected_date,
                    gap_pct=gap,
                    prev_close_price=prev_close,
                    theoretical_open=theo_open,
                    message_date=msg_date,
                    ai_score=ai_score,
                    ai_summary=ai_summary,
                    kap_url=kap_url,
                )
                session.add(news)
                new_count += 1

            # ──────────────────────────────────────────────────────────────────
            # PER-MESSAGE COMMIT — duplicate push'i onler
            # Push/tweet harici side-effect'ten ONCE commit ediyoruz. Boylece
            # commit sonrasi push basarisiz olsa bile, telegram_message_id
            # zaten DB'de duplicate olarak isaretli — sonraki poll'da tekrar
            # islenmez, "ayni hisseye 3 kez bildirim" sorunu cozulur.
            # ──────────────────────────────────────────────────────────────────
            try:
                await session.commit()
            except Exception as _commit_err:
                logger.error(
                    "Per-message commit hatasi (msg_id=%s, ticker=%s): %s",
                    telegram_message_id, ticker, _commit_err,
                )
                # Commit basarisizsa rollback edip bu mesaji atla — push da gonderme
                try:
                    await session.rollback()
                except Exception:
                    pass
                continue

            # Push bildirim — sadece should_notify True ise
            if not should_notify:
                pass  # Yukarida loglandi
            elif _is_positive_dup:
                logger.info(
                    "Push debounce: %s — 2dk icinde tekrar pozitif KAP, push atilmadi",
                    ticker,
                )
            else:
                # Default ata — try block icinde hata olsa bile NameError olmasin
                news_type = "seans_disi"
                try:
                    from app.services.notification import NotificationService
                    notif = NotificationService(db=session)

                    # 3 Tip: seans_ici, seans_disi, seans_disi_acilis
                    if message_type == "seans_ici_pozitif":
                        news_type = "seans_ici"
                    elif message_type == "seans_disi_acilis":
                        news_type = "seans_disi_acilis"
                    else:
                        news_type = "seans_disi"
                    # Cooldown timestamp'ini kaydet (sadece pozitif seans_ici/disi icin)
                    if ticker and message_type in ("seans_ici_pozitif", "borsa_kapali"):
                        _last_kap_push_per_ticker[ticker.upper()] = _now_ts
                    await notif.notify_kap_news(
                        ticker=ticker or "",
                        price=None,
                        kap_id=kap_id or "",
                        matched_keyword=matched_kw,
                        sentiment="positive",
                        news_type=news_type,
                        pct_change=pct_change if message_type == "seans_ici_pozitif" else None,
                        gap_pct=f"%{gap}" if (message_type == "seans_disi_acilis" and gap is not None) else None,
                        ai_score=ai_score,
                        ai_summary=ai_summary,
                        prev_close=str(prev_close) if (message_type == "seans_disi_acilis" and prev_close is not None) else None,
                        theoretical_open=str(theo_open) if (message_type == "seans_disi_acilis" and theo_open is not None) else None,
                        kap_url=kap_url,
                    )
                    logger.info("Push bildirim gonderildi: %s — skor=%s — %s", ticker, ai_score, title)
                except Exception as notif_err:
                    logger.error("Push bildirim hatasi: %s", notif_err)

            logger.info(
                "Telegram haber kaydedildi: [%s] %s — %s",
                message_type, ticker or "???", title,
            )

            # OTOMATİK KAP TWEET AÇIK MI? (admin Haber Havuzu toggle)
            # Kapalıysa otomatik tweet atılmaz — admin manuel seçip gönderir.
            # Manuel tweet (is_manual=True) bu ayardan ETKİLENMEZ.
            try:
                from app.services.twitter_service import is_auto_kap_tweet_enabled
                _auto_tweet_on = is_auto_kap_tweet_enabled()
            except Exception:
                # FAIL-SAFE: ayar okunamazsa otomatik tweet ATMA (eskiden True idi →
                # istenmeyen tweet atiyordu). Kullanici manuel gonderebilir.
                _auto_tweet_on = False

            # ----------------------------------------------------------------
            # TWITTER ENTEGRASYONU (Tum haberler — her 5 haberden 1'i)
            # AI skoru dusukse tweet de atilmaz (notr/olumsuz haber)
            # ----------------------------------------------------------------
            if _auto_tweet_on and should_notify and message_type != "seans_disi_acilis":  # seans_disi_acilis = sadece acilis gap, tweet atilmaz
                try:
                    from app.services.twitter_service import tweet_kap_news, _kap_tweet_counter

                    # Restart sonrasi sayaci DB'den yukle (bir kerelik)
                    if _kap_tweet_counter["total"] == 0:
                        try:
                            from app.models.pending_tweet import PendingTweet
                            from sqlalchemy import func as sqlfunc
                            from datetime import time as _time_type
                            _today_start = datetime.combine(date.today(), _time_type.min)
                            _db_count_result = await session.execute(
                                select(sqlfunc.count(PendingTweet.id)).where(
                                    PendingTweet.source == "kap_haber",
                                    PendingTweet.created_at >= _today_start,
                                )
                            )
                            _db_count = _db_count_result.scalar() or 0
                            if _db_count > 0:
                                # DB'de bugun kac KAP tweet atilmis → sayaci oradan devam ettir
                                # Her tweet = 5 haber (1., 6., 11. ...) → toplam haber ≈ tweet * 5
                                _kap_tweet_counter["total"] = _db_count * 5
                                logger.info(
                                    "[TWEET-FLOW] Sayac DB'den yuklendi: bugun %d KAP tweet atilmis → sayac=%d",
                                    _db_count, _kap_tweet_counter["total"],
                                )
                        except Exception as _cnt_err:
                            logger.warning("[TWEET-FLOW] Sayac DB yuklemesi basarisiz: %s", _cnt_err)

                    # Tweet politikasi: seans ici/disi × POZITIF orani admin'den ayarli
                    # (1-5; 1=hepsi). Kategori bazli sayac + (sayac-1) % N == 0 kurali.
                    from app.services.twitter_service import get_tweet_rates, _kap_tweet_counters
                    _rates = await get_tweet_rates(session)
                    _pcat = "ici_poz" if message_type == "seans_ici_pozitif" else "disi_poz"
                    _pN = _rates.get(_pcat, 3 if _pcat == "ici_poz" else 4)
                    _kap_tweet_counters[_pcat]["total"] += 1
                    _counter_val = _kap_tweet_counters[_pcat]["total"]

                    if (_counter_val - 1) % _pN == 0:
                        tweet_kw = matched_kw
                        if not tweet_kw or "BULUNAMADI" in tweet_kw.upper() or tweet_kw == ticker:
                            tweet_kw = "Yeni KAP Bildirimi"

                        logger.info(
                            "[TWEET-FLOW] KAP tweet baslatiliyor (%d. haber, tweet atilacak): %s | kw=%s | ai=%s | url=%s",
                            _counter_val, ticker, tweet_kw, ai_score, kap_url,
                        )

                        tw_success = tweet_kap_news(
                            ticker,
                            tweet_kw,
                            "positive",
                            ai_score=ai_score,
                            ai_summary=ai_summary,
                            kap_url=kap_url,
                            ai_hashtags=ai_hashtags,
                        )
                        logger.info(
                            "[TWEET-FLOW] KAP tweet sonuc: %s (basarili=%s, ai_score=%s)",
                            ticker, tw_success, ai_score,
                        )

                        from app.services.admin_telegram import notify_tweet_sent
                        await notify_tweet_sent(
                            "kap_haber", ticker, tw_success,
                            f"Anahtar: {tweet_kw} | AI: {ai_score}/10 | Sayac: {_counter_val}" if ai_score is None else f"Anahtar: {tweet_kw} | AI: {ai_score:.1f}/10 | Sayac: {_counter_val}",
                        )
                    else:
                        logger.info(
                            "[TWEET-FLOW] Sayac %d (skor=%.1f), tweet atlandi (%s: her %d'te 1): %s",
                            _counter_val, ai_score or 0, _pcat, _pN, ticker,
                        )

                except Exception as tw_err:
                    logger.error("[TWEET-FLOW] Twitter tweet hatasi (poller devam eder): %s", tw_err, exc_info=True)

            # ----------------------------------------------------------------
            # NEGATIF HABER TWEET — 2 ayri akis:
            #   - Guclu Olumsuz (0-1) + Cok Olumsuz (1.1-2.0): HER birini tweet
            #     (atlanmaz, sayac yok). Skor < 2.1
            #   - Olumsuz (2.1-3.0) + Hafif Olumsuz (3.1-4.0): her 2 haberden 1 tweet
            # Push atilmaz, sadece Twitter'da kirmizi banner.
            # ----------------------------------------------------------------
            _should_negative_tweet = False
            _negative_category = ""  # log icin
            if (
                _auto_tweet_on
                and not should_notify
                and message_type != "seans_disi_acilis"
                and ai_score is not None
                and ticker
            ):
                if ai_score < 4.1:
                    # Tum olumsuz haberler (Guclu/Cok/Olumsuz/Hafif).
                    # Seans ici/disi × NEGATIF orani admin'den ayarli (1-5; 1=hepsi).
                    try:
                        from app.services.twitter_service import get_tweet_rates, _kap_tweet_counters
                        _nrates = await get_tweet_rates(session)
                        _ncat = "ici_neg" if message_type == "seans_ici_pozitif" else "disi_neg"
                        _nN = _nrates.get(_ncat, 2 if _ncat == "ici_neg" else 3)
                        _kap_tweet_counters[_ncat]["total"] += 1
                        _neg_counter_val = _kap_tweet_counters[_ncat]["total"]
                        if (_neg_counter_val - 1) % _nN == 0:
                            _should_negative_tweet = True
                            if ai_score < 1.1:
                                _category_label = "guclu_olumsuz"
                            elif ai_score < 2.1:
                                _category_label = "cok_olumsuz"
                            elif ai_score < 3.1:
                                _category_label = "olumsuz"
                            else:
                                _category_label = "hafif_olumsuz"
                            _negative_category = f"{_category_label} (sayac={_neg_counter_val})"
                        else:
                            logger.info(
                                "[TWEET-FLOW-NEG] Olumsuz sayac %d, tweet atlandi (%s: her %d'te 1): %s",
                                _neg_counter_val, _ncat, _nN, ticker,
                            )
                    except Exception as _cnt_e:
                        logger.warning("[TWEET-FLOW-NEG] sayac hata: %s", _cnt_e)

            if _should_negative_tweet:
                try:
                    from app.services.twitter_service import tweet_kap_news
                    tweet_kw_neg = matched_kw
                    if not tweet_kw_neg or "BULUNAMADI" in tweet_kw_neg.upper() or tweet_kw_neg == ticker:
                        tweet_kw_neg = "Yeni KAP Bildirimi"
                    logger.info(
                        "[TWEET-FLOW-NEG] Negatif KAP tweet baslatiliyor [%s]: %s | skor=%.1f | kw=%s",
                        _negative_category, ticker, ai_score, tweet_kw_neg,
                    )
                    tw_neg_success = tweet_kap_news(
                        ticker, tweet_kw_neg, "negative",
                        ai_score=ai_score,
                        ai_summary=ai_summary,
                        kap_url=kap_url,
                        ai_hashtags=ai_hashtags,
                    )
                    logger.info(
                        "[TWEET-FLOW-NEG] Negatif KAP tweet sonuc: %s (basarili=%s, skor=%.1f)",
                        ticker, tw_neg_success, ai_score,
                    )
                    from app.services.admin_telegram import notify_tweet_sent
                    await notify_tweet_sent(
                        "kap_haber_negatif", ticker, tw_neg_success,
                        f"Anahtar: {tweet_kw_neg} | AI: {ai_score:.1f}/10 [{_negative_category}]",
                    )
                except Exception as tw_neg_err:
                    logger.error(
                        "[TWEET-FLOW-NEG] Negatif tweet hatasi: %s", tw_neg_err, exc_info=True,
                    )

        # Commit her zaman yapilir — score < 6 mesajlari telegram_news'e yazilmaz
        # ama kap_all_disclosures'a yazilir. Eger sadece new_count > 0 kontrolu
        # yaparsak, kap_all_disclosures INSERT'leri commit edilmez ve rollback olur.
        await session.commit()
        if new_count > 0:
            logger.info(
                "Telegram: %d yeni mesaj kaydedildi (DB commit basarili)",
                new_count,
            )
        else:
            logger.debug(
                "Telegram: yeni mesaj yok (chat_skip=%d, notext=%d, dup=%d, unknown_type=%d)",
                skipped_chat, skipped_notext, skipped_duplicate, skipped_unknown_type,
            )

    return new_count


# -------------------------------------------------------------------
# Scheduler Entrypoint
# -------------------------------------------------------------------

async def poll_telegram():
    """Scheduler tarafindan cagirilir.

    Bot token ve chat ID'yi config/env'den alir.
    asyncio.Lock ile eszamanli cagrilari engeller — 409 Conflict onlenir.
    """
    # Lock ile koruma: Eger onceki poll hala suruyorsa atlaniyor
    if _poll_lock.locked():
        logger.debug("Telegram poll zaten calisiyor, atlaniyor")
        return

    async with _poll_lock:
        from app.config import get_settings
        settings = get_settings()

        # Okuyucu bot token: sender bot kendi mesajlarini getUpdates'te goremez,
        # ayri bir reader bot gerekli. Yoksa fallback olarak sender token kullanilir.
        bot_token = settings.TELEGRAM_READER_BOT_TOKEN or settings.TELEGRAM_BOT_TOKEN
        chat_id = settings.TELEGRAM_CHAT_ID

        if not bot_token:
            logger.warning("TELEGRAM_READER_BOT_TOKEN ve TELEGRAM_BOT_TOKEN ayarlanmamis, poller atlaniyor")
            return

        try:
            global _consecutive_errors
            count = await poll_telegram_messages(bot_token, chat_id)
            if count > 0:
                logger.info("Telegram: %d yeni mesaj islendi", count)
            # Basarili — hata sayaci sifirla
            if _consecutive_errors > 0:
                _consecutive_errors = 0
        except Exception as e:
            logger.error("Telegram poller hatasi: %s", e)
            _consecutive_errors += 1
            # Spam onleme: sadece ilk hata ve her 30 hatada bir bildir
            # (10sn aralikla = ~5 dakikada bir bildirim)
            if _consecutive_errors == 1 or _consecutive_errors % 30 == 0:
                try:
                    from app.services.admin_telegram import notify_scraper_error
                    error_str = str(e)
                    label = "Telegram Poller"
                    if "409" in error_str or "Conflict" in error_str:
                        label = "Telegram Poller (409 Conflict)"
                    if _consecutive_errors > 1:
                        label += f" — {_consecutive_errors}. üst üste hata"
                    await notify_scraper_error(label, error_str)
                except Exception:
                    pass
