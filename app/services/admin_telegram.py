"""Admin Telegram bildirim servisi.

Kritik hata ve durum bilgilerini admin'e gonderir.
Spam yapmaz — sadece onemli olaylar:
- IPO durum gecisleri (spk_pending → newly_approved → ... → trading)
- Scraper/bildirim hatalari
- Ceiling update sonuclari
- Firebase init basarisiz
"""

import logging
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


def _get_admin_config() -> tuple[str, str]:
    """Admin bot token ve chat ID'yi config'den al."""
    settings = get_settings()
    return (
        getattr(settings, "ADMIN_TELEGRAM_BOT_TOKEN", ""),
        getattr(settings, "ADMIN_TELEGRAM_CHAT_ID", ""),
    )


async def send_admin_message(
    text: str,
    parse_mode: str = "HTML",
    silent: bool = False,
) -> bool:
    """Admin'e Telegram mesaji gonder.

    Args:
        text: Mesaj metni (HTML veya plain text)
        parse_mode: "HTML" veya "Markdown"
        silent: True ise sessiz bildirim (telefon titremez)

    Returns:
        Basarili ise True, hata varsa False
    """
    bot_token, chat_id = _get_admin_config()

    if not bot_token or not chat_id:
        logger.debug("Admin Telegram yapilandirilmamis, mesaj atlaniyor")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
                "disable_notification": silent,
            })
            if resp.status_code != 200:
                logger.warning(
                    "Admin Telegram mesaj gonderilemedi: %s — %s",
                    resp.status_code, resp.text[:200],
                )
                return False
            return True
    except Exception as e:
        # Admin mesaj hatasi uygulama akisini bozmasin
        logger.error("Admin Telegram mesaj hatasi: %s", e)
        return False


async def notify_ipo_status_change(
    ticker: Optional[str],
    company_name: str,
    old_status: str,
    new_status: str,
    extra: str = "",
):
    """IPO durum gecisi bildirimi."""
    status_emojis = {
        "spk_pending": "📝",
        "newly_approved": "🆕",
        "in_distribution": "📋",
        "awaiting_trading": "⏳",
        "trading": "🔔",
    }
    emoji = status_emojis.get(new_status, "📌")
    name = ticker or company_name

    status_labels = {
        "spk_pending": "SPK Onay Bekleniyor",
        "newly_approved": "Yeni Onaylandi",
        "in_distribution": "Dagitim Surecinde",
        "awaiting_trading": "Islem Gunu Bekleniyor",
        "trading": "Isleme Basladi",
    }
    old_label = status_labels.get(old_status, old_status)
    new_label = status_labels.get(new_status, new_status)

    text = f"{emoji} <b>{name}</b>\n{old_label} → {new_label}"
    if extra:
        text += f"\n{extra}"

    await send_admin_message(text)


async def notify_scraper_error(scraper_name: str, error: str):
    """Scraper hatasi bildirimi."""
    await send_admin_message(
        f"⚠️ <b>Scraper Hatasi</b>\n"
        f"Kaynak: {scraper_name}\n"
        f"Hata: {error[:500]}",
        silent=True,
    )


async def notify_ceiling_update_result(
    total: int,
    success: int,
    failed: int,
    failed_tickers: list[str] | None = None,
):
    """Tavan takip guncelleme sonucu."""
    text = (
        f"📊 <b>Tavan Takip Guncellendi</b>\n"
        f"Toplam: {total} IPO\n"
        f"Basarili: {success}\n"
        f"Basarisiz: {failed}"
    )
    if failed_tickers:
        text += f"\nBasarisiz: {', '.join(failed_tickers)}"

    await send_admin_message(text, silent=(failed == 0))


async def notify_spk_approval(company_name: str, approval_type: str):
    """Yeni SPK onayi bildirimi."""
    await send_admin_message(
        f"🆕 <b>Yeni SPK Onayi!</b>\n"
        f"{company_name}\n"
        f"Onay tipi: {approval_type}"
    )


async def notify_tweet_sent(tweet_type: str, ticker: str, success: bool, detail: str = ""):
    """Tweet atildiginda admin Telegram'a bildirim gonder.

    Args:
        tweet_type: Tweet tipi (gunluk_takip, 25_gun, dagitim, spk_onayi vs.)
        ticker: Hisse kodu
        success: Tweet basarili mi
        detail: Ek bilgi (gun sayisi, hata mesaji vs.)
    """
    if success:
        emoji = "✅"
        status = "basarili"
    else:
        emoji = "❌"
        status = "BASARISIZ"

    text = (
        f"{emoji} <b>Tweet {status}</b>\n"
        f"Tip: {tweet_type}\n"
        f"Ticker: #{ticker}"
    )
    if detail:
        text += f"\n{detail}"

    await send_admin_message(text, silent=success)
