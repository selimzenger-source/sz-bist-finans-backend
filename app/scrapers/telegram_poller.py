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

    1. Endeks ticker'larini reddet (XU100, XU030, XBANK...)
    2. BigPara'nin BIST hisse listesinde var mi kontrol et
    3. Listede yoksa ATLA — KAP bazi BIST'te islem gormeyen sirketleri de
       icerir (bagli ortakliklar, halka acik olmayanlar). Bunlari islemiyoruz.
    """
    if not ticker:
        return False
    tk = ticker.upper().strip()

    # Endeksler
    if tk in _INDEX_TICKERS or tk.startswith("XU0") or tk.startswith("XU1"):
        return False

    # BigPara whitelist
    try:
        from app.services.twitter_service import _get_bist_ticker_cache
        ticker_lines = _get_bist_ticker_cache()
        if not ticker_lines:
            # Cache bos donerse riske girme — geciyor say (false negative onle)
            return True
        # Format: "Sirket Adi → #TICKER"
        valid_set = set()
        for line in ticker_lines:
            m = re.search(r"#([A-Z0-9]+)\s*$", line)
            if m:
                valid_set.add(m.group(1).upper())
        if not valid_set:
            return True  # parse hatasi — riske girme
        return tk in valid_set
    except Exception:
        return True  # hata durumunda blokla degil, gecsin


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

            # ── Ticker Validation: Endeks / BIST'te olmayan ticker'lari atla ──
            # XU100, XU030 (endeksler) + GATEG gibi BIST listesinde olmayanlar.
            if ticker and not _is_valid_bist_ticker(ticker):
                logger.info(
                    "Telegram: Ticker BIST'te yok veya endeks (msg_id=%s, ticker=%s) — atlandi",
                    telegram_message_id, ticker,
                )
                continue

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

            # KAP URL yoksa TradingView + Matriks ID ile olustur
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

            if (
                ticker
                and ai_score is not None
                and message_type in ("seans_ici_pozitif", "borsa_kapali")
                and is_telegram_active(kap_source)
            ):
                try:
                    from app.models.kap_all_disclosure import KapAllDisclosure
                    from app.scrapers.kap_all_scraper import _infer_category
                    from sqlalchemy import select as _sa_select

                    # Sentiment çıkarımı (ai_news_scorer score'dan):
                    if ai_score >= 6.0:
                        ka_sentiment = "Olumlu"
                    elif ai_score < 4.5:
                        ka_sentiment = "Olumsuz"
                    else:
                        ka_sentiment = "Notr"

                    # Title — yeni bot "Baslik:" satirindan, fallback matched_kw
                    ka_title = (news_title or matched_kw or title or "")[:500]

                    # Kategori + is_bilanco — frontend "Bilanco AI Analizi - COK YAKINDA"
                    # badge'ini bu flag'e gore gosterir. KAP scraper ile ayni mantik.
                    ka_category = _infer_category(ka_title)
                    ka_is_bilanco = ka_category in ("Bilanço/Finansal Rapor", "Faaliyet Raporu")

                    # ── BASIT DUPLICATE KONTROLU: kap_url uzerinden ──
                    # Her KAP bildiriminin unique URL'si var (Bildirim/XXXXX).
                    # Ayni URL DB'de yoksa yaz, varsa atla. Bu kadar.
                    is_duplicate = False
                    if kap_url:
                        existing_check = await session.execute(
                            _sa_select(KapAllDisclosure.id).where(
                                KapAllDisclosure.kap_url == kap_url,
                            ).limit(1)
                        )
                        is_duplicate = existing_check.scalar_one_or_none() is not None

                    if is_duplicate:
                        logger.debug(
                            "kap_all_disclosures duplicate atlandi: %s — %s",
                            ticker, ka_title[:50],
                        )
                    else:
                        kap_disc = KapAllDisclosure(
                            company_code=ticker,
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
                                ticker, ka_title[:50], ka_sentiment, ai_score,
                            )
                        except Exception as _flush_err:
                            logger.warning(
                                "kap_all_disclosures yazma hatasi: %s — %s",
                                ticker, _flush_err,
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

            # Push bildirim — sadece should_notify True ise
            if not should_notify:
                pass  # Yukarida loglandi
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
                        gap_pct=f"%{gap}" if (message_type == "seans_disi_acilis" and gap is not None) else None,
                        ai_score=ai_score,
                        ai_summary=ai_summary,
                        prev_close=str(prev_close) if (message_type == "seans_disi_acilis" and prev_close is not None) else None,
                        theoretical_open=str(theo_open) if (message_type == "seans_disi_acilis" and theo_open is not None) else None,
                    )
                    logger.info("Push bildirim gonderildi: %s — skor=%s — %s", ticker, ai_score, title)
                except Exception as notif_err:
                    logger.error("Push bildirim hatasi: %s", notif_err)

            logger.info(
                "Telegram haber kaydedildi: [%s] %s — %s",
                message_type, ticker or "???", title,
            )

            # ----------------------------------------------------------------
            # TWITTER ENTEGRASYONU (Tum haberler — her 3 haberden 1'i)
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
                                # Her tweet = 3 haber (1., 4., 7. ...) → toplam haber = tweet * 3 - 2
                                # Ama basit olsun: tweet * 3 kadar sayalim ki bir sonraki cycle'da dogru calıssin
                                _kap_tweet_counter["total"] = _db_count * 3
                                logger.info(
                                    "[TWEET-FLOW] Sayac DB'den yuklendi: bugun %d KAP tweet atilmis → sayac=%d",
                                    _db_count, _kap_tweet_counter["total"],
                                )
                        except Exception as _cnt_err:
                            logger.warning("[TWEET-FLOW] Sayac DB yuklemesi basarisiz: %s", _cnt_err)

                    # Her 3 haberden 1'ini tweetle
                    _kap_tweet_counter["total"] += 1
                    _counter_val = _kap_tweet_counter["total"]

                    if _counter_val % 3 == 1:  # 1., 4., 7., 10. ... haber tweet atilir
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
                            "[TWEET-FLOW] Sayac %d, tweet atilmadi (her 3'te 1): %s",
                            _counter_val, ticker,
                        )

                except Exception as tw_err:
                    logger.error("[TWEET-FLOW] Twitter tweet hatasi (poller devam eder): %s", tw_err, exc_info=True)

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
