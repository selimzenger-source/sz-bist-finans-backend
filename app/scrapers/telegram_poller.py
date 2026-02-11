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


async def fetch_telegram_updates(bot_token: str, offset: int | None = None) -> list[dict]:
    """Telegram getUpdates API'sini cagir."""
    url = f"{TELEGRAM_API_BASE.format(token=bot_token)}/getUpdates"
    params = {"timeout": 5, "limit": 100}
    if offset is not None:
        params["offset"] = offset

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(url, params=params)
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
    """Mesaj metninden tipini tespit et. Sadece pozitif haberler gecerli."""
    text_upper = text.upper()

    # Seans ici pozitif haber (negatif yok)
    if "SEANS İÇİ" in text_upper or "SEANS ICI" in text_upper:
        if "POZİTİF" in text_upper or "POZITIF" in text_upper:
            return "seans_ici_pozitif"
        # Negatif mesajlar atlanir
        return None

    # Borsa kapali = Seans disi pozitif haber
    if "BORSA KAPALI" in text_upper:
        return "borsa_kapali"

    # Acilis bilgileri
    if "AÇILIŞ BİLGİLERİ" in text_upper or "ACILIS BILGILERI" in text_upper:
        return "seans_disi_acilis"

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
        updates = await fetch_telegram_updates(bot_token, offset=_last_update_id)
    except Exception as e:
        logger.error("Telegram API baglanamadi: %s", e)
        return 0

    if not updates:
        return 0

    new_count = 0

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
                continue

            text = message.get("text", "")
            if not text:
                continue

            telegram_message_id = message.get("message_id")
            if not telegram_message_id:
                continue

            # Daha once kaydedilmis mi kontrol et
            existing = await session.execute(
                select(TelegramNews).where(
                    TelegramNews.telegram_message_id == telegram_message_id
                )
            )
            if existing.scalar_one_or_none():
                continue

            # Mesaj tipini tespit et
            message_type = detect_message_type(text)
            if not message_type:
                logger.debug("Bilinmeyen mesaj tipi, atlandi: %s", text[:80])
                continue

            # Parse
            ticker = parse_ticker(text)
            # Fiyat bilgisi KAYDEDILMEZ (veri ihlali)
            kap_id = parse_kap_id(text)
            expected_date = parse_expected_trading_date(text)
            gap = parse_gap_pct(text)
            prev_close = parse_prev_close(text)
            theo_open = parse_theoretical_open(text)
            sentiment = parse_sentiment(message_type)
            title = build_parsed_title(message_type, ticker)

            # Mesaj tarihini al — Turkiye saat diliminde (UTC+3)
            msg_date_unix = message.get("date")
            msg_date = (
                datetime.fromtimestamp(msg_date_unix, tz=TZ_TR).replace(tzinfo=None)
                if msg_date_unix else None
            )

            # Parsed body — fiyat bilgisi olmadan temiz format
            parsed_body = f"Sembol: {ticker or '???'}"
            if message_type == "seans_disi_acilis" and gap is not None:
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

            # Hemen push bildirim gonder
            try:
                from app.services.notification import NotificationService
                notif = NotificationService()
                price_val = parse_price(text, "Fiyat")  # Gecici: sadece keyword icin parse
                # Matched keyword'u raw text'ten cikar
                matched_kw = ""
                if kap_id:
                    # "Iliskilendirilen Haber Detayi:" sonrasini al
                    detail_match = re.search(r"[İI]lişkilendirilen\s+Haber\s+Detay[ıi]:\s*\n(.+)", text, re.IGNORECASE)
                    if detail_match:
                        matched_kw = detail_match.group(1).strip()
                if not matched_kw:
                    matched_kw = ticker or ""

                news_type = "seans_ici" if message_type == "seans_ici_pozitif" else "seans_disi"
                await notif.notify_kap_news(
                    ticker=ticker or "",
                    price=None,
                    kap_id=kap_id or "",
                    matched_keyword=matched_kw,
                    sentiment="positive",
                    news_type=news_type,
                )
                logger.info("Push bildirim gonderildi: %s — %s", ticker, title)
            except Exception as notif_err:
                logger.error("Push bildirim hatasi: %s", notif_err)

            logger.info(
                "Telegram haber kaydedildi: [%s] %s — %s",
                message_type, ticker or "???", title,
            )

        if new_count > 0:
            await session.commit()

    return new_count


# -------------------------------------------------------------------
# Scheduler Entrypoint
# -------------------------------------------------------------------

async def poll_telegram():
    """Scheduler tarafindan cagirilir.

    Bot token ve chat ID'yi config/env'den alir.
    """
    from app.config import get_settings
    settings = get_settings()

    bot_token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID

    if not bot_token:
        logger.warning("TELEGRAM_BOT_TOKEN ayarlanmamis, poller atlaniyor")
        return

    try:
        count = await poll_telegram_messages(bot_token, chat_id)
        if count > 0:
            logger.info("Telegram: %d yeni mesaj islendi", count)
    except Exception as e:
        logger.error("Telegram poller hatasi: %s", e)
