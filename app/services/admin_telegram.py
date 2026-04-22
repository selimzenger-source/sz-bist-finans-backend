"""Admin Telegram bildirim servisi.

Kritik hata ve durum bilgilerini admin'e gonderir.
Spam yapmaz — sadece onemli olaylar:
- IPO durum gecisleri (spk_pending → newly_approved → ... → trading)
- Scraper/bildirim hatalari
- Ceiling update sonuclari
- Firebase init basarisiz

Anti-spam: Ayni hata mesaji icin max 3 bildirim gonderir (10 dk cooldown).
"""

import logging
import time
from collections import defaultdict
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

# ─── Anti-spam: Hata bildirim tekrar sınırlaması ───
# Aynı hata mesajı (ilk 80 karakter) için max 3 bildirim, 10dk cooldown
_ERROR_NOTIFY_MAX = 3        # Aynı hatadan max bu kadar mesaj
_ERROR_COOLDOWN_SEC = 600    # 10 dakika cooldown
_error_counts: dict[str, int] = defaultdict(int)       # hata_key → gönderim sayısı
_error_first_seen: dict[str, float] = {}                # hata_key → ilk gönderim zamanı


def _should_send_error(error_text: str) -> bool:
    """Hata bildiriminin gönderilip gönderilmeyeceğini kontrol eder.

    Aynı hata (ilk 80 karakter) 10 dakika içinde max 3 kez gönderilir.
    10 dakika geçince sayaç sıfırlanır.
    """
    key = error_text[:80].strip().lower()
    now = time.monotonic()

    first_seen = _error_first_seen.get(key)

    # Cooldown süresi dolmuşsa sıfırla
    if first_seen and (now - first_seen) > _ERROR_COOLDOWN_SEC:
        _error_counts[key] = 0
        _error_first_seen[key] = now

    count = _error_counts[key]

    if count >= _ERROR_NOTIFY_MAX:
        return False  # Limit dolmuş, spam yapma

    # İlk kez görülüyorsa zaman kaydet
    if key not in _error_first_seen:
        _error_first_seen[key] = now

    _error_counts[key] = count + 1

    # Son mesajda uyarı ekle
    if count + 1 == _ERROR_NOTIFY_MAX:
        logger.info("Telegram anti-spam: '%s...' hatası %d. kez gönderildi — sonraki mesajlar susturulacak", key[:40], _ERROR_NOTIFY_MAX)

    return True


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
        "newly_approved": "Yeni Onaylandı",
        "in_distribution": "Dağıtım Sürecinde",
        "awaiting_trading": "İşlem Günü Bekleniyor",
        "trading": "İşleme Başladı",
    }
    old_label = status_labels.get(old_status, old_status)
    new_label = status_labels.get(new_status, new_status)

    text = f"{emoji} <b>{name}</b>\n{old_label} → {new_label}"
    if extra:
        text += f"\n{extra}"

    await send_admin_message(text)


async def notify_scraper_error(scraper_name: str, error: str):
    """Scraper hatasi bildirimi (anti-spam: aynı hata max 3 mesaj / 10dk)."""
    spam_key = f"{scraper_name}:{error}"
    if not _should_send_error(spam_key):
        return  # Aynı hatadan çok fazla mesaj gönderildi, sustur

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


# -------------------------------------------------------
# Push Bildirim Monitoring
# -------------------------------------------------------

async def notify_push_sent(
    notification_type: str,
    title: str,
    sent_count: int,
    failed_count: int = 0,
    stale_tokens: int = 0,
    detail: str = "",
):
    """Push bildirim gonderim sonucunu raporla.

    Her toplu bildirim gonderiminde (KAP, halka arz, tavan vs.)
    kaca gitti, kaci basarisiz, stale token sayisi.
    """
    if sent_count == 0 and failed_count == 0:
        return  # Hicbir sey gonderilmediyse spam yapma

    emoji = "📲" if failed_count == 0 else "⚠️"
    text = (
        f"{emoji} <b>Push Bildirim</b>\n"
        f"Tip: {notification_type}\n"
        f"Baslik: {title[:60]}\n"
        f"Gonderilen: {sent_count}"
    )
    if failed_count > 0:
        text += f"\nBasarisiz: {failed_count}"
    if stale_tokens > 0:
        text += f"\n🗑 Stale token temizlendi: {stale_tokens}"
    if detail:
        text += f"\n{detail}"

    await send_admin_message(text, silent=(failed_count == 0))


async def notify_push_error(
    notification_type: str,
    error: str,
    user_id: int | None = None,
):
    """Push bildirim hatasi — aninda uyari (anti-spam: aynı hata max 3 mesaj / 10dk)."""
    spam_key = f"push:{notification_type}:{error}"
    if not _should_send_error(spam_key):
        return  # Spam engeli

    text = f"❌ <b>Push Hata</b>\n" f"Tip: {notification_type}\n" f"Hata: {error[:300]}"
    if user_id:
        text += f"\nUser ID: {user_id}"

    await send_admin_message(text)


# ─── Stale token batcher — tek tek bildirim yerine 60sn'de bir özet ───
import asyncio

_stale_token_buffer: list[tuple[int, str]] = []   # (user_id, device_id) listesi
_stale_token_flush_task: Optional[asyncio.Task] = None
_STALE_FLUSH_INTERVAL_SEC = 60                     # 60sn'de bir özet at
_STALE_FLUSH_THRESHOLD = 20                        # 20 birikince hemen at


async def _flush_stale_tokens():
    """Buffer'ı boşalt, özet mesaj at."""
    global _stale_token_buffer
    if not _stale_token_buffer:
        return
    batch = _stale_token_buffer[:]
    _stale_token_buffer = []

    count = len(batch)
    # İlk 10 user'ı detayda göster, gerisi sayı olarak
    preview_lines = [f"• User {uid} ({did[:8]}...)" for uid, did in batch[:10]]
    preview = "\n".join(preview_lines)
    extra = f"\n... ve {count - 10} kullanıcı daha" if count > 10 else ""

    await send_admin_message(
        f"🗑 <b>Stale Token Temizlendi</b> (toplam {count})\n{preview}{extra}",
        silent=True,
    )


async def _stale_flush_loop():
    """Periyodik flush task."""
    while True:
        try:
            await asyncio.sleep(_STALE_FLUSH_INTERVAL_SEC)
            await _flush_stale_tokens()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Stale token flush hatasi: %s", e)


async def notify_stale_token_cleaned(user_id: int, device_id: str):
    """Stale FCM token temizlendi — buffer'a ekle, periyodik özet at (spam engeli)."""
    global _stale_token_flush_task
    _stale_token_buffer.append((user_id, device_id))

    # Flush task başlatılmadıysa başlat
    if _stale_token_flush_task is None or _stale_token_flush_task.done():
        try:
            _stale_token_flush_task = asyncio.create_task(_stale_flush_loop())
        except RuntimeError:
            # Event loop yoksa (muhtemelen import sırasında) sessizce geç
            pass

    # Çok birikti — hemen flush et
    if len(_stale_token_buffer) >= _STALE_FLUSH_THRESHOLD:
        await _flush_stale_tokens()


async def notify_subscription_purchase(
    event_type: str,
    user_id: int,
    device_id: str,
    platform: str,
    product_id: str,
    package: str,
    price_tl: Optional[float] = None,
    store: str = "",
    is_trial: bool = False,
):
    """Paket satın alma / trial / yenileme bildirimi — en kritik olay.

    event_type: INITIAL_PURCHASE, RENEWAL, NON_RENEWING_PURCHASE, PRODUCT_CHANGE,
                CANCELLATION, EXPIRATION
    """
    event_emoji = {
        "INITIAL_PURCHASE": "💰",
        "RENEWAL": "🔁",
        "NON_RENEWING_PURCHASE": "💰",
        "PRODUCT_CHANGE": "🔀",
        "CANCELLATION": "❌",
        "EXPIRATION": "⌛",
    }
    emoji = event_emoji.get(event_type, "💳")

    event_labels = {
        "INITIAL_PURCHASE": "Yeni Satın Alma" if not is_trial else "🎁 Free Trial Başladı",
        "RENEWAL": "Yenileme",
        "NON_RENEWING_PURCHASE": "Tek Seferlik Satın Alma",
        "PRODUCT_CHANGE": "Paket Değişimi",
        "CANCELLATION": "İptal",
        "EXPIRATION": "Süresi Doldu",
    }
    label = event_labels.get(event_type, event_type)

    platform_emoji = {"ios": "🍎", "android": "🤖"}.get((platform or "").lower(), "📱")

    price_line = f"\nFiyat: <b>{price_tl:.2f} ₺</b>" if price_tl else ""

    text = (
        f"{emoji} <b>{label}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"User ID: <code>{user_id}</code>\n"
        f"Cihaz: {platform_emoji} {platform or '?'} — <code>{device_id[:12]}...</code>\n"
        f"Ürün: <code>{product_id}</code>\n"
        f"Paket: <b>{package}</b>"
        f"{price_line}"
    )
    if store:
        text += f"\nMağaza: {store}"

    await send_admin_message(text, silent=(event_type in ("RENEWAL",)))


async def notify_push_health_report(
    total_users: int,
    with_fcm_token: int,
    with_expo_token: int,
    notifications_enabled: int,
    stale_cleaned_today: int = 0,
):
    """Saatlik push bildirim saglik raporu."""
    no_token = total_users - with_fcm_token
    text = (
        f"📊 <b>Push Saglik Raporu</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"Toplam kullanici: {total_users}\n"
        f"FCM token var: {with_fcm_token} ✅\n"
        f"Token yok: {no_token} ⚪\n"
        f"Bildirim acik: {notifications_enabled}\n"
        f"Expo token var: {with_expo_token}"
    )
    if stale_cleaned_today > 0:
        text += f"\n🗑 Bugun temizlenen stale: {stale_cleaned_today}"

    await send_admin_message(text, silent=True)
