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
_last_heartbeat: float = 0.0  # Periyodik status log zamani


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
                    raw_kw = detail_match.group(1).strip()
                    # "Haber Detayi Bulunamadi" gibi anlamsiz degerler atlanir
                    if raw_kw and "BULUNAMADI" not in raw_kw.upper():
                        matched_kw = raw_kw
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
            if ticker and message_type in ("seans_ici_pozitif", "borsa_kapali"):
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

            # KAP URL yoksa TradingView + Matriks ID ile olustur
            if not kap_url and kap_id:
                kap_url = f"https://tr.tradingview.com/news/matriks:{kap_id}:0/"

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
                ai_score=ai_score,
                ai_summary=ai_summary,
                kap_url=kap_url,
            )
            session.add(news)
            new_count += 1

            # Hemen push bildirim gonder (matched_kw yukarida parse edildi)
            # AI skoru notr veya olumsuz ise bildirim gonderme (gereksiz bildirim engelle)
            # ai_score None = AI basarisiz → guvenlisyeni gonder, ai_score >= 6 = pozitif
            should_notify = (ai_score is None) or (ai_score >= 6)
            if not should_notify:
                logger.info(
                    "AI skoru dusuk (%s), bildirim atlanıyor: %s — %s",
                    ai_score, ticker, title,
                )
            else:
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
                    logger.info("Push bildirim gonderildi: %s — skor=%s — %s", ticker, ai_score, title)
                except Exception as notif_err:
                    logger.error("Push bildirim hatasi: %s", notif_err)

            logger.info(
                "Telegram haber kaydedildi: [%s] %s — %s",
                message_type, ticker or "???", title,
            )

            # ----------------------------------------------------------------
            # TWITTER ENTEGRASYONU (Sadece BIST 50)
            # AI skoru dusukse tweet de atilmaz (notr/olumsuz haber)
            # ----------------------------------------------------------------
            if should_notify and message_type != "seans_disi_acilis":  # seans_disi_acilis = sadece acilis gap, tweet atilmaz
                try:
                    from app.services.news_service import get_bist50_tickers_sync
                    from app.services.twitter_service import tweet_bist30_news

                    bist50 = get_bist50_tickers_sync()
                    if ticker and ticker.upper() in bist50:
                        tweet_kw = matched_kw
                        if not tweet_kw or "BULUNAMADI" in tweet_kw.upper() or tweet_kw == ticker:
                            tweet_kw = "Yeni KAP Bildirimi"

                        logger.info(
                            "[TWEET-FLOW] KAP tweet baslatiliyor: %s | kw=%s | ai=%s | url=%s",
                            ticker, tweet_kw, ai_score, kap_url,
                        )

                        tw_success = tweet_bist30_news(
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
                            "bist50_kap_haber", ticker, tw_success,
                            f"Anahtar: {tweet_kw} | AI: {ai_score}/10" if ai_score is None else f"Anahtar: {tweet_kw} | AI: {ai_score:.1f}/10",
                        )
                    else:
                        logger.info(
                            "[TWEET-FLOW] BIST50 disinda, tweet atilmadi: %s (BIST50=%d hisse)",
                            ticker, len(bist50),
                        )

                except Exception as tw_err:
                    logger.error("[TWEET-FLOW] Twitter tweet hatasi (poller devam eder): %s", tw_err, exc_info=True)

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
