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

# -------------------------------------------------------------------
# Telegram API
# -------------------------------------------------------------------

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"
_last_update_id: int | None = None
_poll_lock = asyncio.Lock()  # Eszamanli getUpdates cagrilarini engelle
_consecutive_errors = 0  # Ust uste hata sayaci — spam onleme


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
    """Mesajdan hisse kodunu cikart. 'Sembol: XXXXX' formatinda arar."""
    patterns = [
        r"Sembol:\s*([A-Z]{3,10})",
        r"Semb[oö]l:\s*([A-Z]{3,10})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).upper()
    return None


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
    """HaberId alanini cikart."""
    match = re.search(r"HaberId[:\s]*(\d+)", text, re.IGNORECASE)
    if match:
        return match.group(1)
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
# Ana Poller Fonksiyonu
# -------------------------------------------------------------------

async def poll_telegram_messages(bot_token: str, chat_id: str) -> int:
    """Telegram kanalından yeni mesajları çek, parse et, DB'ye kaydet.

    Returns:
        İşlenen yeni mesaj sayısı.
    """
    global _last_update_id

    try:
        # Eger offset henuz yoksa (ilk baslangic), son 20 mesaji iste (-20).
        # Bu sayede restart/deploy sirasinda kacirilan mesajlar yakalanir (catch-up).
        req_offset = _last_update_id if _last_update_id is not None else -20
        updates = await fetch_telegram_updates(bot_token, offset=req_offset)
    except Exception as e:
        logger.error("Telegram API baglanamadi: %s", e)
        return 0

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

            # Parse
            ticker = parse_ticker(text)
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

            # Matched keyword'u raw text'ten cikar (parsed_body icin de lazim)
            matched_kw = ""
            if kap_id:
                detail_match = re.search(
                    r"[İI]li[sş]kilendirilen\s+Haber\s+Detay[ıiİ]:\s*\n?(.+)",
                    text, re.IGNORECASE
                )
                if detail_match:
                    matched_kw = detail_match.group(1).strip()
            if not matched_kw:
                matched_kw = ticker or ""

            # Parsed body — fiyat bilgisi olmadan temiz format
            parsed_body = f"Sembol: {ticker or '???'}"
            if matched_kw and matched_kw != ticker:
                parsed_body += f"\n{matched_kw}"
            if message_type == "seans_ici_pozitif" and pct_change:
                parsed_body += f"\nDeğişim: {pct_change}"
            elif message_type == "seans_disi_acilis" and gap is not None:
                parsed_body += f"\nGap: %{gap}"
            elif message_type == "borsa_kapali" and expected_date:
                parsed_body += f"\nBeklenen İşlem Günü: {expected_date.isoformat()}"

            # DB'ye kaydet — fiyat yok
            news = TelegramNews(
                telegram_message_id=telegram_message_id,
                chat_id=msg_chat_id,
                message_type=message_type,
                ticker=ticker,
                price_at_time=None,  # Fiyat kaydedilmez
                raw_text=text,
                parsed_title=title,
                parsed_body=parsed_body,
                sentiment=sentiment,
                kap_notification_id=kap_id,
                expected_trading_date=expected_date,
                gap_pct=gap,
                prev_close_price=None,  # Fiyat kaydedilmez
                theoretical_open=None,  # Fiyat kaydedilmez
                message_date=msg_date,
            )
            session.add(news)
            new_count += 1

            # Hemen push bildirim gonder (matched_kw yukarida parse edildi)
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
                await notif.notify_kap_news(
                    ticker=ticker or "",
                    price=None,
                    kap_id=kap_id or "",
                    matched_keyword=matched_kw,
                    sentiment="positive",
                    news_type=news_type,
                    pct_change=pct_change if message_type == "seans_ici_pozitif" else None,
                )
                logger.info("Push bildirim gonderildi: %s — %s", ticker, title)
            except Exception as notif_err:
                logger.error("Push bildirim hatasi: %s", notif_err)

            logger.info(
                "Telegram haber kaydedildi: [%s] %s — %s",
                message_type, ticker or "???", title,
            )

            # ----------------------------------------------------------------
            # TWITTER ENTEGRASYONU (Sadece BIST 30)
            # ----------------------------------------------------------------
            try:
                # BIST 30 kontrolu icin import — lazy import (dongu icinde ama performans sorunu olmaz)
                from app.services.news_service import BIST30_TICKERS
                from app.services.twitter_service import tweet_bist30_news

                if ticker and ticker.upper() in BIST30_TICKERS:
                    # Tweet metni icin keyword temizligi
                    tweet_kw = matched_kw
                    if not tweet_kw or "BULUNAMADI" in tweet_kw.upper() or tweet_kw == ticker:
                        tweet_kw = "Yeni KAP Bildirimi"
                    
                    # Sentiment hep positive kabul ediliyor bu poller'da
                    # Arka planda tweet at (await etme, poller'i bloke etmesin)
                    # Ancak tweet_bist30_news senkron bir fonksiyon (httpx sync kullaniyor _safe_tweet icinde)
                    # Bu yuzden direkt cagiriyoruz, _safe_tweet zaten exception yutar.
                    tweet_bist30_news(ticker, tweet_kw, "positive")
                    logger.info("Twitter BIST30 tweet atildi: %s", ticker)

            except Exception as tw_err:
                logger.error("Twitter tweet hatasi (poller devam eder): %s", tw_err)

        if new_count > 0:
            await session.commit()
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

        bot_token = settings.TELEGRAM_BOT_TOKEN
        chat_id = settings.TELEGRAM_CHAT_ID

        if not bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN ayarlanmamis, poller atlaniyor")
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
