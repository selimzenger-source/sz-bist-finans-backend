"""Telegram Kanal Mesaj Gonderici + DB Kaydedici.

KAP scraper haber buldugunda:
1. Telegram kanalina formatlı mesaj gonderir
2. telegram_news tablosuna kaydeder (uygulama gosterebilsin diye)

Boylece:
- Telegram kanalindaki insanlar haberi gorur
- Uygulama DB'den okur (FIFO son 20)
- Push notification ayrica Firebase ile gider
"""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.telegram_news import TelegramNews

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"


def _get_telegram_config():
    """Config'den Telegram ayarlarini al."""
    from app.config import get_settings
    settings = get_settings()
    return settings.TELEGRAM_BOT_TOKEN, settings.TELEGRAM_CHAT_ID


def _format_telegram_message(
    ticker: str,
    sentiment: str,
    news_type: str,
    matched_keyword: str,
    kap_url: Optional[str] = None,
    kap_id: Optional[str] = None,
    news_title: Optional[str] = None,
) -> str:
    """KAP haberini Telegram mesaj formatina donusturur.

    telegram_poller.py'nin parse edebilecegi formatta olusturur:
    - 'SEANS ICI POZITIF HABER' veya 'SEANS ICI NEGATIF HABER' veya 'BORSA KAPALI'
    - Sembol: XXXXX
    - HaberId: NNNNN
    """
    # Mesaj tipi baslik
    if news_type == "seans_ici" and sentiment == "positive":
        header = "SEANS ICI POZITIF HABER"
        emoji = "\u26a1"
    elif news_type == "seans_ici" and sentiment == "negative":
        header = "SEANS ICI NEGATIF HABER"
        emoji = "\u26a0\ufe0f"
    elif news_type == "seans_disi":
        header = "BORSA KAPALI - Haber Kaydedildi"
        emoji = "\U0001f512"
    else:
        header = "SEANS ICI POZITIF HABER"
        emoji = "\u26a1"

    lines = [
        f"{emoji} {header} {emoji}",
        "",
        f"Sembol: {ticker}",
    ]

    if news_title:
        lines.append(f"Konu: {news_title}")

    lines.append(f"Anahtar Kelime: {matched_keyword}")

    if kap_id:
        lines.append(f"HaberId: {kap_id}")

    if kap_url:
        lines.append(f"KAP: {kap_url}")

    return "\n".join(lines)


def _determine_message_type(news_type: str, sentiment: str) -> str:
    """news_type + sentiment'ten telegram_news message_type'a donusturur."""
    if news_type == "seans_ici":
        if sentiment == "negative":
            return "seans_ici_negatif"
        return "seans_ici_pozitif"
    elif news_type == "seans_disi":
        return "borsa_kapali"
    return "seans_ici_pozitif"


def _build_parsed_title(message_type: str, ticker: str) -> str:
    """Mesaj tipi ve ticker'dan baslik olustur."""
    type_labels = {
        "seans_ici_pozitif": f"\U0001f4c8 Seans Ici Pozitif - {ticker}",
        "seans_ici_negatif": f"\U0001f4c9 Seans Ici Negatif - {ticker}",
        "borsa_kapali": f"\U0001f512 Borsa Kapali - {ticker}",
        "seans_disi_acilis": f"\U0001f4ca Acilis Bilgileri - {ticker}",
    }
    return type_labels.get(message_type, f"Haber - {ticker}")


async def send_to_telegram(
    ticker: str,
    sentiment: str,
    news_type: str,
    matched_keyword: str,
    kap_url: Optional[str] = None,
    kap_id: Optional[str] = None,
    news_title: Optional[str] = None,
) -> Optional[int]:
    """Telegram kanalina mesaj gonderir.

    Returns:
        Gonderilen mesajin message_id'si, basarisizsa None.
    """
    text = _format_telegram_message(
        ticker=ticker,
        sentiment=sentiment,
        news_type=news_type,
        matched_keyword=matched_keyword,
        kap_url=kap_url,
        kap_id=kap_id,
        news_title=news_title,
    )

    bot_token, chat_id = _get_telegram_config()
    if not bot_token:
        logger.warning("TELEGRAM_BOT_TOKEN ayarlanmamis, mesaj gonderilemedi")
        return None

    try:
        url = f"{TELEGRAM_API_BASE.format(token=bot_token)}/sendMessage"
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(url, data={
                "chat_id": chat_id,
                "text": text,
            })
            resp.raise_for_status()
            data = resp.json()

        if data.get("ok"):
            msg_id = data["result"]["message_id"]
            logger.info(f"Telegram mesaj gonderildi: {ticker} (msg_id={msg_id})")
            return msg_id
        else:
            logger.warning(f"Telegram mesaj gonderilemedi: {data}")
            return None

    except Exception as e:
        logger.error(f"Telegram mesaj gonderme hatasi: {e}")
        return None


async def save_to_telegram_news(
    db: AsyncSession,
    telegram_message_id: int,
    ticker: str,
    sentiment: str,
    news_type: str,
    matched_keyword: str,
    kap_id: Optional[str] = None,
    news_title: Optional[str] = None,
    raw_text: Optional[str] = None,
) -> TelegramNews:
    """Gonderilen Telegram mesajini telegram_news tablosuna kaydeder.

    Boylece uygulama /api/v1/telegram-news endpoint'inden okuyabilir.
    """
    message_type = _determine_message_type(news_type, sentiment)
    parsed_title = _build_parsed_title(message_type, ticker)

    body_text = raw_text or f"{matched_keyword}"
    if news_title:
        body_text = f"{news_title}\n{body_text}"

    _, chat_id = _get_telegram_config()
    news = TelegramNews(
        telegram_message_id=telegram_message_id,
        chat_id=chat_id or "-1002704950091",
        message_type=message_type,
        ticker=ticker,
        price_at_time=None,
        raw_text=raw_text or f"Sembol: {ticker}\nKonu: {news_title or matched_keyword}",
        parsed_title=parsed_title,
        parsed_body=body_text,
        sentiment=sentiment if sentiment in ("positive", "negative") else "neutral",
        kap_notification_id=kap_id,
        expected_trading_date=None,
        gap_pct=None,
        prev_close_price=None,
        theoretical_open=None,
        message_date=datetime.utcnow(),
    )

    db.add(news)
    logger.info(f"telegram_news kaydedildi: {ticker} (msg_id={telegram_message_id})")
    return news


async def send_and_save_kap_news(
    db: AsyncSession,
    ticker: str,
    sentiment: str,
    news_type: str,
    matched_keyword: str,
    kap_url: Optional[str] = None,
    kap_id: Optional[str] = None,
    news_title: Optional[str] = None,
    raw_text: Optional[str] = None,
) -> bool:
    """KAP haberini hem Telegram'a gonder, hem DB'ye kaydet.

    Bu tek fonksiyon scheduler'dan cagirilir.
    Returns True if successful.
    """
    # 1. Telegram kanalina gonder
    msg_id = await send_to_telegram(
        ticker=ticker,
        sentiment=sentiment,
        news_type=news_type,
        matched_keyword=matched_keyword,
        kap_url=kap_url,
        kap_id=kap_id,
        news_title=news_title,
    )

    if msg_id is None:
        # Telegram gonderilemedi — yine de DB'ye kaydet (fake msg_id ile)
        import time
        msg_id = int(time.time() * 1000)  # timestamp-based unique id
        logger.warning(f"Telegram gonderilemedi, DB'ye fake msg_id ile kaydediliyor: {msg_id}")

    # 2. DB'ye kaydet
    try:
        await save_to_telegram_news(
            db=db,
            telegram_message_id=msg_id,
            ticker=ticker,
            sentiment=sentiment,
            news_type=news_type,
            matched_keyword=matched_keyword,
            kap_id=kap_id,
            news_title=news_title,
            raw_text=raw_text,
        )
        return True
    except Exception as e:
        logger.error(f"telegram_news DB kayit hatasi: {e}")
        return False
