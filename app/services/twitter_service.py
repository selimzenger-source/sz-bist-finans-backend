"""X (Twitter) Otomatik Tweet Servisi — @SZAlgoFinans

11 farkli tweet tipi ile halka arz ve KAP haberlerini X'e otomatik atar.
Mevcut sistemi ASLA bozmamalı — tum cagrılar try/except ile korunur.

Tweet Tipleri:
1.  Yeni Halka Arz (SPK onayi)
2.  Dagitima Cikis (in_distribution)
3.  Tahmini Lot Sayisi
4.  Son 4 Saat Hatirlatma
5.  Son 30 Dakika Hatirlatma
6.  Ilk Islem Gunu (09:00 gong)
7.  Acilis Fiyati (09:56 sadece ilk islem gunu)
8.  Gunluk Takip (18:20 her islem gunu)
9.  25 Gün Performans Ozeti (25. gunde bir kez)
10. Yillik Halka Arz Ozeti (her ayin 1'i 20:00, ocak haric)
11. BIST 30 KAP Haberi (aninda)
"""

import logging
import time
import hashlib
import hmac
import base64
import urllib.parse
import uuid
import json
from datetime import datetime, date
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Twitter API v2 endpoint
_TWITTER_TWEET_URL = "https://api.twitter.com/2/tweets"

# Credentials cache — lazy init
_credentials = None
_init_attempted = False


def _load_credentials() -> Optional[dict]:
    """Twitter API anahtarlarini yukler (tek seferlik)."""
    global _credentials, _init_attempted
    if _init_attempted:
        return _credentials
    _init_attempted = True

    try:
        from app.config import get_settings
        settings = get_settings()

        api_key = settings.X_API_KEY
        api_secret = settings.X_API_SECRET
        access_token = settings.X_ACCESS_TOKEN
        access_token_secret = settings.X_ACCESS_TOKEN_SECRET

        if not all([api_key, api_secret, access_token, access_token_secret]):
            logger.warning("Twitter API anahtarlari eksik — tweet atma devre disi")
            return None

        _credentials = {
            "api_key": api_key,
            "api_secret": api_secret,
            "access_token": access_token,
            "access_token_secret": access_token_secret,
        }
        logger.info("Twitter credentials yuklendi (@SZAlgoFinans)")
        return _credentials

    except Exception as e:
        logger.error(f"Twitter credentials yuklenemedi: {e}")
        return None


def _generate_oauth_signature(
    method: str,
    url: str,
    oauth_params: dict,
    consumer_secret: str,
    token_secret: str,
) -> str:
    """OAuth 1.0a HMAC-SHA1 imza uretir."""
    # Parameter string olustur (sirali)
    sorted_params = sorted(oauth_params.items())
    param_string = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted_params
    )

    # Base string
    base_string = (
        f"{method.upper()}&"
        f"{urllib.parse.quote(url, safe='')}&"
        f"{urllib.parse.quote(param_string, safe='')}"
    )

    # Signing key
    signing_key = (
        f"{urllib.parse.quote(consumer_secret, safe='')}&"
        f"{urllib.parse.quote(token_secret, safe='')}"
    )

    # HMAC-SHA1
    hashed = hmac.new(
        signing_key.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha1,
    )
    return base64.b64encode(hashed.digest()).decode("utf-8")


def _build_oauth_header(creds: dict) -> str:
    """OAuth 1.0a Authorization header olusturur."""
    oauth_params = {
        "oauth_consumer_key": creds["api_key"],
        "oauth_nonce": uuid.uuid4().hex,
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": creds["access_token"],
        "oauth_version": "1.0",
    }

    # Imza olustur
    signature = _generate_oauth_signature(
        method="POST",
        url=_TWITTER_TWEET_URL,
        oauth_params=oauth_params,
        consumer_secret=creds["api_secret"],
        token_secret=creds["access_token_secret"],
    )
    oauth_params["oauth_signature"] = signature

    # Header string
    header_parts = ", ".join(
        f'{urllib.parse.quote(k, safe="")}="{urllib.parse.quote(v, safe="")}"'
        for k, v in sorted(oauth_params.items())
    )
    return f"OAuth {header_parts}"


def _safe_tweet(text: str) -> bool:
    """Tweet atar — ASLA hata firlatmaz, sadece log'a yazar.

    httpx + OAuth 1.0a HMAC-SHA1 ile Twitter API v2 kullanir.
    tweepy gerektirmez — Python 3.13 uyumlu.

    Returns:
        True: tweet basarili
        False: tweet basarisiz (ama sistem etkilenmez)
    """
    try:
        creds = _load_credentials()
        if not creds:
            logger.info(f"[TWITTER-DRY-RUN] {text[:80]}...")
            return False

        # Twitter karakter limiti: 280
        if len(text) > 280:
            text = text[:277] + "..."

        auth_header = _build_oauth_header(creds)

        response = httpx.post(
            _TWITTER_TWEET_URL,
            json={"text": text},
            headers={
                "Authorization": auth_header,
                "Content-Type": "application/json",
            },
            timeout=15.0,
        )

        if response.status_code in (200, 201):
            tweet_id = response.json().get("data", {}).get("id", "?")
            logger.info(f"Tweet basarili (id={tweet_id}): {text[:60]}...")
            return True
        else:
            logger.error(
                "Tweet API hatasi (status=%d): %s",
                response.status_code, response.text[:200],
            )
            return False

    except Exception as e:
        logger.error(f"Tweet hatasi (sistem etkilenmez): {e}")
        return False


# ================================================================
# APP LINK — store'a yuklenince gercek link ile degistirilecek
# ================================================================
APP_LINK = "szalgo.net.tr"

# Standart footer — slogan + yasal uyari
SLOGAN = "\U0001F514 İlk bilen siz olun!"
DISCLAIMER = "\u26A0\uFE0F Yapay zek\u00e2 destekli otomatik bildirimdir, yat\u0131r\u0131m tavsiyesi i\u00e7ermez."
DISCLAIMER_SHORT = "\u26A0\uFE0F YZ destekli bildirimdir, yat\u0131r\u0131m tavsiyesi i\u00e7ermez."


# ================================================================
# 1. YENI HALKA ARZ (SPK Onayi)
# ================================================================
def tweet_new_ipo(ipo) -> bool:
    """SPK'dan yeni halka arz onayi geldiginde tweet atar."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        price_text = ""
        if ipo.ipo_price:
            price_text = f"\n\U0001F4B0 Halka arz fiyatı: {ipo.ipo_price} TL"

        text = (
            f"\U0001F6A8 SPK Bülteni Yayımlandı!\n\n"
            f"{ipo.company_name}{ticker_text} için halka arz başvurusu SPK tarafından onaylandı."
            f"{price_text}\n\n"
            f"📲 Bilgiler geldikçe bildirim göndereceğiz.\n"
            f"Detaylar için: {APP_LINK}\n\n"
            f"#HalkaArz #BIST #Borsa"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_new_ipo hatasi: {e}")
        return False


# ================================================================
# 2. DAGITIMA CIKIS
# ================================================================
def tweet_distribution_start(ipo) -> bool:
    """Dagitim sureci basladiginda tweet atar."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        end_date = ""
        if ipo.subscription_end:
            end_date = f"\n\U0001F4C5 Son başvuru: {ipo.subscription_end.strftime('%d.%m.%Y')}"
        price_text = f"\n\U0001F4B0 Fiyatı: {ipo.ipo_price} TL" if ipo.ipo_price else ""

        text = (
            f"\U0001F4CB Halka Arz Başvuruları Başladı!\n\n"
            f"{ipo.company_name}{ticker_text} için talep toplama süreci başlamıştır."
            f"{price_text}{end_date}\n\n"
            f"📲 Detaylar ve anlık bildirimler için:\n"
            f"{APP_LINK}\n\n"
            f"#HalkaArz #BIST #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_distribution_start hatasi: {e}")
        return False


# ================================================================
# 3. TAHMINI LOT SAYISI
# ================================================================
def tweet_estimated_lots(ipo) -> bool:
    """Tahmini lot sayisi belli oldugunda tweet atar."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        lots = ipo.estimated_lots_per_person or "?"

        text = (
            f"\U0001F4CA Tahmini Dağıtım Bilgisi\n\n"
            f"{ipo.company_name}{ticker_text}\n"
            f"\u2022 Tahmini dağıtım: ~{lots} lot/kişi\n\n"
            f"\u26A0\uFE0F Yurt içi bireysel yatırımcıya dağıtılan"
            f" ortalama lot baz alınmıştır.\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_estimated_lots hatasi: {e}")
        return False


# ================================================================
# 4. SON 4 SAAT HATIRLATMA
# ================================================================
def tweet_last_4_hours(ipo) -> bool:
    """Son 4 saat kala hatirlatma tweeti."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        text = (
            f"\u23F0 Son 4 Saat!\n\n"
            f"{ipo.company_name}{ticker_text} halka arz başvurusu için"
            f" kapanışa son 4 saat kaldı!\n\n"
            f"Başvurunuzu yapmayı unutmayın.\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #SonGün #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_last_4_hours hatasi: {e}")
        return False


# ================================================================
# 5. SON 30 DAKIKA HATIRLATMA
# ================================================================
def tweet_last_30_min(ipo) -> bool:
    """Son 30 dakika kala hatirlatma tweeti."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        text = (
            f"\U0001F6A8 Son 30 Dakika!\n\n"
            f"{ipo.company_name}{ticker_text} halka arz başvurusu kapanmak üzere!\n\n"
            f"Başvuru yapmak isteyenler acele etsin.\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #SonDakika #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_last_30_min hatasi: {e}")
        return False


# ================================================================
# 6. ILK ISLEM GUNU (09:00 — Gong caliyor!)
# ================================================================
def tweet_first_trading_day(ipo) -> bool:
    """Ilk islem gunu sabahi gong tweeti."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        price_text = ""
        if ipo.ipo_price:
            price_text = f"\n\U0001F4B0 Halka arz fiyatı: {ipo.ipo_price} TL"

        text = (
            f"\U0001F514 Gong Çalıyor!\n\n"
            f"{ipo.company_name}{ticker_text} bugün borsada işleme başlıyor!"
            f"{price_text}\n\n"
            f"25 günlük tavan/taban takibini uygulamamızdan yapabilirsiniz.\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #BIST #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_first_trading_day hatasi: {e}")
        return False


# ================================================================
# 7. ACILIS FIYATI (09:56 — sadece ilk islem gunu)
# ================================================================
def tweet_opening_price(ipo, open_price: float, pct_change: float) -> bool:
    """Ilk islem gunu acilis fiyati tweeti (09:56)."""
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0

        # Durum belirle
        if pct_change >= 9.5:
            durum = "\U0001F7E2 Tavandan açıldı!"
        elif pct_change > 0:
            durum = f"\U0001F7E2 %{pct_change:+.2f} yükselişle açıldı"
        elif pct_change == 0:
            durum = f"\U0001F7E1 Halka arz fiyatından açıldı"
        else:
            durum = f"\U0001F534 %{pct_change:+.2f} düşüşle açıldı"

        text = (
            f"\U0001F4C8 Açılış Fiyatı Belli Oldu!\n\n"
            f"{ipo.company_name}{ticker_text}\n\n"
            f"\u2022 Halka arz fiyatı: {ipo_price:.2f} TL\n"
            f"\u2022 Açılış fiyatı: {open_price:.2f} TL\n"
            f"\u2022 {durum}\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_opening_price hatasi: {e}")
        return False


# ================================================================
# 8. GUNLUK TAKIP (18:20 — her islem gunu)
# ================================================================
def tweet_daily_tracking(ipo, trading_day: int, close_price: float,
                         pct_change: float, durum: str,
                         days_data: list = None) -> bool:
    """Her islem gunu 18:20'de kumulatif tablo seklinde gunluk takip tweeti."""
    try:
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0

        # Durum emoji
        durum_map = {
            "tavan": "\U0001F7E2 Tavan",
            "alici_kapatti": "\U0001F7E2 Alıcı kapattı",
            "not_kapatti": "\U0001F7E1 Not kapattı",
            "satici_kapatti": "\U0001F534 Satıcı kapattı",
            "taban": "\U0001F534 Taban",
        }
        durum_text = durum_map.get(durum, durum)

        # Kumulatif performans tablosu — her gun halka arz fiyatina gore toplam getiri
        table_lines = []
        if days_data and ipo_price > 0:
            for d in days_data:
                day_num = d["trading_day"]
                day_close = float(d["close"])
                cum_pct = ((day_close - ipo_price) / ipo_price) * 100
                emoji = "\U0001F7E2" if cum_pct >= 0 else "\U0001F534"
                if day_num == trading_day:
                    table_lines.append(f"{day_num}. {emoji} %{cum_pct:+.1f} \u25C0")
                else:
                    table_lines.append(f"{day_num}. {emoji} %{cum_pct:+.1f}")
        else:
            # days_data yoksa eski formatta tek satir yaz
            if ipo_price > 0:
                cum_change = ((close_price - ipo_price) / ipo_price) * 100
            else:
                cum_change = 0
            table_lines.append(f"{trading_day}. %{cum_change:+.1f}")

        table_text = "\n".join(table_lines)

        # Gunluk degisim emoji
        daily_emoji = "\U0001F7E2" if pct_change >= 0 else "\U0001F534"

        # Header + Kumulatif Toplam basligi + footer bugun detay
        header = (
            f"\U0001F4CA #{ipo.ticker or ipo.company_name} \u2014 {trading_day}. Gün Sonu\n\n"
            f"Kümülatif Toplam:\n"
        )
        footer = (
            f"\n\n{daily_emoji} Kapanış: {close_price:.2f} TL | %{pct_change:+.2f} | {durum_text}\n\n"
            f"\U0001F4F2 Detaylar için: {APP_LINK}\n"
            f"#HalkaArz #{ipo.ticker or 'Borsa'}"
        )

        text = header + table_text + footer

        # Twitter 280 karakter limiti kontrolu
        if len(text) > 280:
            # Cok uzunsa ilk 2 + ・・・ + son 8 gunu goster
            if days_data and len(days_data) > 10:
                first_lines = table_lines[:2]
                last_lines = table_lines[-8:]
                table_text = "\n".join(first_lines) + "\n\u30FB\u30FB\u30FB\n" + "\n".join(last_lines)
                text = header + table_text + footer

        # Hala 280'i asiyorsa — noktalarla son 6 gun
        if len(text) > 280:
            last_6 = table_lines[-6:]
            table_text = "\u30FB\u30FB\u30FB\n" + "\n".join(last_6)
            text = header + table_text + footer

        # Son kurtarma — app linkini kaldir
        if len(text) > 280:
            footer = (
                f"\n\n{daily_emoji} Kapanış: {close_price:.2f} TL | %{pct_change:+.2f} | {durum_text}\n\n"
                f"#HalkaArz #{ipo.ticker or 'Borsa'}"
            )
            text = header + table_text + footer

        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_daily_tracking hatasi: {e}")
        return False


# ================================================================
# 9. 25 GUN PERFORMANS OZETI (25. gun tamamlandiginda bir kez)
# ================================================================
def tweet_25_day_performance(
    ipo,
    close_price_25: float,
    total_pct: float,
    ceiling_days: int,
    floor_days: int,
    avg_lot: Optional[float] = None,
    days_data: list = None,
) -> bool:
    """25 islem gunu tamamlandiginda kumulatif tablo + performans ozeti tweeti."""
    try:
        ipo_price = float(ipo.ipo_price) if ipo.ipo_price else 0

        # Performans emoji
        if total_pct >= 50:
            perf_emoji = "\U0001F525"  # ates
        elif total_pct >= 0:
            perf_emoji = "\U0001F7E2"
        else:
            perf_emoji = "\U0001F534"

        # Lot kazanc hesabi
        lot_text = ""
        if avg_lot and ipo_price > 0:
            lot_count = int(avg_lot)
            profit_per_lot = (close_price_25 - ipo_price) * 100  # 1 lot = 100 hisse
            total_profit = profit_per_lot * lot_count
            if total_profit >= 0:
                lot_text = f"\n{lot_count} lot kazanç: +{total_profit:,.0f} TL"
            else:
                lot_text = f"\n{lot_count} lot zarar: {total_profit:,.0f} TL"

        # Kumulatif performans tablosu
        table_lines = []
        if days_data and ipo_price > 0:
            for d in days_data:
                day_num = d["trading_day"]
                day_close = float(d["close"])
                cum_pct = ((day_close - ipo_price) / ipo_price) * 100
                emoji = "\U0001F7E2" if cum_pct >= 0 else "\U0001F534"
                table_lines.append(f"{day_num}. {emoji} %{cum_pct:+.1f}")

        header = (
            f"{perf_emoji} #{ipo.ticker or ipo.company_name} \u2014 25 Gün Performans\n\n"
            f"Kümülatif Toplam:\n"
        )
        footer = (
            f"\n\n{perf_emoji} Toplam: %{total_pct:+.2f} | "
            f"Tavan: {ceiling_days} | Taban: {floor_days}"
            f"{lot_text}\n\n"
            f"\U0001F4F2 {APP_LINK}\n"
            f"#HalkaArz #{ipo.ticker or 'Borsa'}"
        )

        if table_lines:
            # Ilk 2 + ・・・ + son 8 (25 gun sigmiyor)
            first_lines = table_lines[:2]
            last_lines = table_lines[-8:]
            table_text = "\n".join(first_lines) + "\n\u30FB\u30FB\u30FB\n" + "\n".join(last_lines)
            text = header + table_text + footer

            # Hala sigmazsa son 6
            if len(text) > 280:
                last_6 = table_lines[-6:]
                table_text = "\u30FB\u30FB\u30FB\n" + "\n".join(last_6)
                text = header + table_text + footer

            # Son kurtarma — app linkini kaldir
            if len(text) > 280:
                footer = (
                    f"\n\n{perf_emoji} Toplam: %{total_pct:+.2f} | "
                    f"Tavan: {ceiling_days} | Taban: {floor_days}"
                    f"{lot_text}\n\n"
                    f"#HalkaArz #{ipo.ticker or 'Borsa'}"
                )
                text = header + table_text + footer
        else:
            # days_data yoksa eski ozet formati
            text = (
                f"{perf_emoji} #{ipo.ticker or ipo.company_name} \u2014 25 Gün Performans\n\n"
                f"\u2022 Halka arz: {ipo_price:.2f} TL\n"
                f"\u2022 25. gun: {close_price_25:.2f} TL\n"
                f"\u2022 Toplam: %{total_pct:+.2f}\n"
                f"\u2022 Tavan: {ceiling_days} | Taban: {floor_days}"
                f"{lot_text}\n\n"
                f"\U0001F4F2 {APP_LINK}\n"
                f"#HalkaArz #{ipo.ticker or 'Borsa'}"
            )

        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_25_day_performance hatasi: {e}")
        return False


# ================================================================
# 10. AY SONU HALKA ARZ RAPORU (Her ayin son gunu gece yarisi)
# ================================================================
def tweet_yearly_summary(
    year: int,
    month_name: str,
    total_ipos: int,
    avg_return_pct: float,
    best_ticker: str,
    best_return_pct: float,
    worst_ticker: str,
    worst_return_pct: float,
    total_completed: int,
    positive_count: int,
) -> bool:
    """Ay sonu halka arz raporu tweeti — her ayin son gunu gece yarisi."""
    try:
        # Performans emoji
        if avg_return_pct >= 10:
            perf_emoji = "\U0001F525"
        elif avg_return_pct >= 0:
            perf_emoji = "\U0001F7E2"
        else:
            perf_emoji = "\U0001F534"

        text = (
            f"\U0001F4CA {year} Halka Arz \u2014 {month_name} Sonu Raporu\n\n"
            f"\u2022 Toplam halka arz: {total_ipos}\n"
            f"\u2022 25 günü doldu: {total_completed}\n"
            f"\u2022 Kar/zarar: {positive_count}/{total_completed}\n"
            f"\u2022 Ort. getiri: {perf_emoji} %{avg_return_pct:+.1f}\n"
            f"\u2022 En iyi: #{best_ticker} (%{best_return_pct:+.1f})\n"
            f"\u2022 En kötü: #{worst_ticker} (%{worst_return_pct:+.1f})\n\n"
            f"\u26A0\uFE0F İlk 25 işlem günü baz alınmıştır.\n\n"
            f"\U0001F4F2 {APP_LINK}\n"
            f"#HalkaArz #BIST #AySonuRaporu"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_yearly_summary hatasi: {e}")
        return False


# ================================================================
# 11. BIST 30 KAP HABERI
# ================================================================
def tweet_bist30_news(ticker: str, matched_keyword: str, sentiment: str) -> bool:
    """BIST 30 hissesi icin KAP haberi tweeti."""
    try:
        if sentiment == "positive":
            emoji = "\U0001F7E2"
        else:
            emoji = "\U0001F534"

        text = (
            f"{emoji} #{ticker} \u2014 KAP Bildirimi\n\n"
            f"\u2022 {matched_keyword}\n\n"
            f"\u26A0\uFE0F KAP haberleri şu an BİST 30 ile sınırlıdır.\n"
            f"350+ hisse için anlık bildirimler yakında!\n\n"
            f"\U0001F4F2 {APP_LINK}\n\n"
            f"#BIST30 #{ticker} #KAP #Borsa"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_bist30_news hatasi: {e}")
        return False


# ================================================================
# YARDIMCI FONKSIYONLAR
# ================================================================

def _get_turkish_month(month: int) -> str:
    """Ay numarasini Turkce ay adina cevirir."""
    months = {
        1: "Ocak", 2: "Şubat", 3: "Mart", 4: "Nisan",
        5: "Mayıs", 6: "Haziran", 7: "Temmuz", 8: "Ağustos",
        9: "Eylül", 10: "Ekim", 11: "Kasım", 12: "Aralık",
    }
    return months.get(month, "")


# ================================================================
# Telegram Mesaj Sablonu — SPK Onayi (admin_telegram'dan cagrilir)
# ================================================================
def format_spk_approval_telegram(company_name: str, bulletin_no: str, price: str = "") -> str:
    """SPK onayi icin Telegram mesaj sablonu."""
    price_line = f"\n💰 Halka arz fiyatı: {price} TL" if price else ""
    return (
        f"🚨 <b>SPK Bülteni Yayımlandı!</b>\n\n"
        f"<b>{company_name}</b> için halka arz başvurusu SPK tarafından onaylandı."
        f"{price_line}\n\n"
        f"📋 Bülten No: {bulletin_no}\n\n"
        f"📲 Bilgiler geldikçe bildirim göndereceğiz.\n"
        f"Detaylar için: {APP_LINK}"
    )
