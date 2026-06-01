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
    )
    from app.services.kap_category_processors import (
        is_block_trade, process_block_trade,
        is_type_conversion, process_type_conversion,
        is_cautious, process_cautious,
    )

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
            async with session.begin_nested():
                await div_process(
                    session, disclosure_id=disclosure_id, ticker=ticker,
                    company_name=company_name, title=title, body=body_for_div,
                    kap_url=kap_url, published_at=published_at,
                )
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

    # share_transaction sadece block_trade VE buyback DEĞİL ise çalışır
    # ("geri alın" share_transaction VE buyback pattern'ında ortak → çakışma)
    if not is_bt and not is_bb and is_share_transaction(title, body or ""):
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
                    parsed = parse_kap_finansal_rapor(body_xbrl)
                    # Safeguard: sektor whitelist + confidence kontrol
                    sec = parsed.get("sector_type") if parsed else None
                    conf = parsed.get("confidence") if parsed else None
                    if (
                        parsed and parsed.get("period")
                        and (parsed.get("total_assets") or parsed.get("revenue"))
                        and sec in BILANCO_ALLOWED_SECTORS
                        and conf in ("high", "medium")
                    ):
                        await save_parsed_bilanco(ticker, parsed)
                        logger.info(
                            "Router→bilanco DIREKT save: %s %s sec=%s conf=%s",
                            ticker, parsed.get("period"), sec, conf,
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

                # ★ BUYBACK FALLBACK: AI score None/dusukse buyback parsed
                # data'sindan deterministik skor + standart ozet uret. Buyback'ler
                # KAP form yapisindan tutar cikarilabilir oldugu icin AI'a guvenmek
                # zorunda degiliz — TL tutarina gore esik bazli skor daha guvenli.
                try:
                    _bb_parsed = (session.info or {}).get("last_buyback_parsed") if hasattr(session, 'info') else None
                    _bb_ticker = (session.info or {}).get("last_buyback_ticker") if hasattr(session, 'info') else None
                    if _bb_parsed and _bb_ticker and _bb_ticker == (ticker or "").upper():
                        if (ai_score is None) or (ai_summary is None) or (ai_score < 5.0):
                            from app.services.buyback_processor import buyback_score_and_summary
                            fb_score, fb_summary = buyback_score_and_summary(_bb_parsed, ticker)
                            ai_score = fb_score
                            ai_summary = fb_summary
                            ai_hashtags = ai_hashtags or ["paygerialim"]
                            logger.info(
                                "Buyback fallback skor: %s — total_tl=%s -> skor=%.1f",
                                ticker, _bb_parsed.get("total_tl"), fb_score,
                            )
                        # tek seferlik kullan, temizle
                        session.info.pop("last_buyback_parsed", None)
                        session.info.pop("last_buyback_ticker", None)
                except Exception as _bb_fb_err:
                    logger.warning("Buyback fallback hata (%s): %s", ticker, _bb_fb_err)

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
                    if _dir is not None and _dir >= 0.5:
                        ai_score = 6.5; _pl = "içeriden/ortak pay ALIM işlemi — güven sinyali"
                    elif _dir is not None and _dir <= -0.5:
                        ai_score = 3.8; _pl = "pay SATIM işlemi — pozisyon azaltımı"
                    else:
                        ai_score = 5.0; _pl = "pay alım-satım bildirimi (sınırlı etki)"
                    ai_summary = ai_summary or f"{ticker} — {_pl}."
                else:
                    ai_score = 5.0
                    ai_summary = ai_summary or f"{(news_title or '').strip()} — bildirim. Fiyata doğrudan etki beklenmemektedir."
                logger.info("Garanti skor uygulandı (%s): ai_score=%s", ticker, ai_score)

            # ── SON GUARDRAIL: skor-özet tutarlılık (pozitif özet → Nötr skor paradoksu) ──
            # analyze_news içindeki guardrail BAZI path'lerde atlanabiliyor:
            #   - analyze_news skor=None döndürüp garanti-skor 5.0 atadığında
            #   - skor düşük dönüp guardrail çağrılmadığında
            # Bu SON pass her durumda (özet varsa) çalışır → özet "olumlu/pozitif"
            # diyorsa skor en az 6.2'ye, "olumsuz/negatif" diyorsa en fazla 3.8'e çekilir.
            # FORTE örneği: özet "pozitif sinyal, olumlu yansıma" diyordu ama skor 5.0
            # kalmıştı (Yeni İş İlişkisi). Hangi path'ten gelirse gelsin artık tutarlı.
            if ai_summary and ai_score is not None and message_type in ("seans_ici_pozitif", "borsa_kapali"):
                try:
                    from app.services.ai_news_scorer import _validate_score_against_content
                    _adj = _validate_score_against_content(
                        float(ai_score), text or "", ticker or "", ai_summary=ai_summary
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
            if not kap_url and ticker and news_title:
                try:
                    from app.services.ai_news_scorer import resolve_kap_url_by_title
                    _rk = await resolve_kap_url_by_title(ticker, news_title)
                    if _rk:
                        kap_url = _rk
                        logger.info("KAP url baslik eslestirme: %s — %s (%s)", ticker, kap_url, news_title[:40])
                except Exception as _rk_err:
                    logger.debug("KAP url baslik eslestirme hatasi (%s): %s", ticker, _rk_err)

            # Hala yoksa son care: TradingView + Matriks ID
            if not kap_url and kap_id:
                kap_url = f"https://tr.tradingview.com/news/matriks:{kap_id}:0/"

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
                    for _tk in all_tickers:
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

                        kap_disc = KapAllDisclosure(
                            company_code=_tk,
                            title=ka_title,
                            body=ai_summary,
                            category=ka_category,
                            is_bilanco=ka_is_bilanco,
                            kap_url=kap_url,
                            source="telegram",
                            published_at=msg_date,
                            ai_sentiment=ka_sentiment,
                            ai_impact_score=ai_score,
                            ai_summary=ai_summary,
                            ai_analyzed_at=datetime.now(timezone.utc),
                        )
                        # Savepoint — beklenmedik hata olursa session korunur
                        try:
                            async with session.begin_nested():
                                session.add(kap_disc)
                                await session.flush()
                            logger.info(
                                "Telegram → kap_all_disclosures yazildi: %s — '%s' (%s, skor=%s)",
                                _tk, ka_title[:50], ka_sentiment, ai_score,
                            )

                            # ── FAVORI/WATCHLIST BILDIRIMI + notify-bot raporu ──
                            # Bu hisseyi favorisine/portfoyune ekleyen kullanicilara
                            # (notify_kap_watchlist pref + pozitif/negatif/tum filtresine gore)
                            # push gonderir VE notify-bot'a "👥 Watchlist'te N · ✅ Gonderildi N kisi"
                            # raporu atar. Notr dahil — filtreleme fonksiyon icinde, kullanici
                            # tercihine gore yapilir. ESKIDEN uzmanpara scraper
                            # (_process_kap_disclosures) yapiyordu; KAP kaynagi Telegram poller'a
                            # tasininca baglanti koptu (favori bildirimleri + rapor atmaz olmustu) — geri baglandi.
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
                            try:
                                async with session.begin_nested():
                                    await _route_to_calendars(
                                        session,
                                        disclosure_id=kap_disc.id,
                                        ticker=_tk,
                                        company_name=None,
                                        title=ka_title,
                                        body=ai_summary,
                                        kap_url=kap_url,
                                        published_at=msg_date,
                                    )
                            except Exception as _route_err:
                                logger.warning(
                                    "KAP router hata (%s): %s", _tk, _route_err,
                                )

                            # ── BILANCO PIPELINE ──
                            if ka_is_bilanco:
                                try:
                                    from app.services.bilanco_pipeline import process_bilanco_bildirimi
                                    import asyncio as _asyncio
                                    _asyncio.create_task(
                                        process_bilanco_bildirimi(_tk, kap_title=ka_title)
                                    )
                                    logger.info(
                                        "Bilanco pipeline tetiklendi: %s — '%s'",
                                        _tk, ka_title[:50],
                                    )
                                except Exception as _bil_err:
                                    logger.warning(
                                        "Bilanco pipeline tetikleme hata (%s): %s", _tk, _bil_err,
                                    )

                            # ── ADMIN TELEGRAM GRUBU: POZITIF KAP BILDIRIMI ──
                            # Sadece ai_score >= 6.0 olan haberler (Hafif Olumlu ve uzeri)
                            # admin grubuna temiz formatla atilir.
                            if ai_score is not None and ai_score >= 6.0:
                                try:
                                    from app.services.admin_telegram import send_kap_positive_to_admin_group
                                    import asyncio as _asyncio2
                                    _asyncio2.create_task(
                                        send_kap_positive_to_admin_group(
                                            ticker=_tk,
                                            ai_score=ai_score,
                                            ai_summary=ai_summary,
                                            kap_url=kap_url,
                                            message_type=message_type,
                                        )
                                    )
                                except Exception as _adm_err:
                                    logger.warning(
                                        "Admin grup pozitif gonderim hata (%s): %s", _tk, _adm_err,
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
            # ai_score None = AI basarisiz → guvenli yol: kaydet + bildir
            should_notify = (ai_score is None) or (ai_score >= 6)

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

            # ----------------------------------------------------------------
            # TWITTER ENTEGRASYONU (Tum haberler — her 5 haberden 1'i)
            # AI skoru dusukse tweet de atilmaz (notr/olumsuz haber)
            # ----------------------------------------------------------------
            if should_notify and message_type != "seans_disi_acilis":  # seans_disi_acilis = sadece acilis gap, tweet atilmaz
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

                    # Tweet politikasi: TUM olumlu haberler 5'te 1 atilir (spam koruma).
                    # Onemli haberleri admin /news-pool sayfasindan manuel tweetler.
                    _kap_tweet_counter["total"] += 1
                    _counter_val = _kap_tweet_counter["total"]

                    if _counter_val % 5 == 1:
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
                            "[TWEET-FLOW] Sayac %d (skor=%.1f), tweet atlandi (her 5'te 1): %s",
                            _counter_val, ai_score or 0, ticker,
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
                not should_notify
                and message_type != "seans_disi_acilis"
                and ai_score is not None
                and ticker
            ):
                if ai_score < 4.1:
                    # Tum olumsuz haberler (Guclu/Cok/Olumsuz/Hafif) -> sayac 2'de 1
                    # Kullanici karari: yuksek skor istisnasi YOK.
                    try:
                        from app.services.twitter_service import _kap_negative_tweet_counter
                        # Restart sonrasi sayaci DB'den yukle
                        if _kap_negative_tweet_counter["total"] == 0:
                            try:
                                from app.models.pending_tweet import PendingTweet
                                from sqlalchemy import func as sqlfunc
                                from datetime import time as _time_type
                                _today_start = datetime.combine(date.today(), _time_type.min)
                                _db_neg_count_result = await session.execute(
                                    select(sqlfunc.count(PendingTweet.id)).where(
                                        PendingTweet.source == "kap_haber_negatif",
                                        PendingTweet.created_at >= _today_start,
                                    )
                                )
                                _db_neg_count = _db_neg_count_result.scalar() or 0
                                if _db_neg_count > 0:
                                    _kap_negative_tweet_counter["total"] = _db_neg_count * 3
                            except Exception:
                                pass
                        _kap_negative_tweet_counter["total"] += 1
                        _neg_counter_val = _kap_negative_tweet_counter["total"]
                        if _neg_counter_val % 3 == 1:
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
                                "[TWEET-FLOW-NEG] Olumsuz sayac %d, tweet atlandi (her 3'te 1): %s",
                                _neg_counter_val, ticker,
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
