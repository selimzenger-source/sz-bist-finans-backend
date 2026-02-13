"""X (Twitter) Otomatik Tweet Servisi — @SZAlgoFinans

14 farkli tweet tipi ile halka arz ve KAP haberlerini X'e otomatik atar.
Mevcut sistemi ASLA bozmamalı — tum cagrılar try/except ile korunur.

Tweet Tipleri:
1.  Yeni Halka Arz (SPK onayi)
2.  Dagitima Cikis (in_distribution)
3.  Kesinlesen Dagitim Sonuclari
4.  Son 4 Saat Hatirlatma
5.  Son 30 Dakika Hatirlatma
6.  Ilk Islem Gunu (09:00 gong)
7.  Acilis Fiyati (09:56 sadece ilk islem gunu)
8.  Gunluk Takip (18:20 her islem gunu)
9.  25 Gün Performans Ozeti (25. gunde bir kez)
10. Yillik Halka Arz Ozeti (her ayin 1'i 20:00, ocak haric)
11. BIST 30 KAP Haberi (aninda)
12. Son Gun Sabah Tweeti (07:30 — hafif uyari tonu)
13. Sirket Tanitim Tweeti (ertesi gun 20:00 — izahname sonrasi)
14. SPK Bekleyenler Gorselli Tweet (her ayin 1'i — gorsel ile)
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

import os
import httpx

logger = logging.getLogger(__name__)

# Twitter API v2 endpoint
_TWITTER_TWEET_URL = "https://api.twitter.com/2/tweets"

# Banner gorsel yollari — static/img/ altinda
_IMG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "img")
BANNER_SPK_BEKLEYENLER = os.path.join(_IMG_DIR, "spk_bekleyenler_banner.png")
BANNER_SON_BASVURU_GUNU = os.path.join(_IMG_DIR, "son_basvuru_gunu_banner.png")
BANNER_SON_4_SAAT = os.path.join(_IMG_DIR, "son_4_saat_banner.png")
BANNER_HALKA_ARZ_HAKKINDA = os.path.join(_IMG_DIR, "halka_arz_hakkinda_banner.png")

# Credentials cache — lazy init
_credentials = None
_init_attempted = False

# Duplicate tweet korumasi — ayni tweeti tekrar atmamak icin
# Key: tweet text hash, Value: timestamp (unix)
# 24 saat icinde ayni tweet atilmaz
_tweet_sent_cache: dict[str, float] = {}
_TWEET_DEDUP_HOURS = 24


def _is_duplicate_tweet(text: str) -> bool:
    """Ayni tweet son 24 saat icinde atildiysa True doner."""
    import time as _time
    text_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
    now = _time.time()

    # Eski kayitlari temizle (24 saatten eski)
    expired = [k for k, v in _tweet_sent_cache.items() if now - v > _TWEET_DEDUP_HOURS * 3600]
    for k in expired:
        del _tweet_sent_cache[k]

    if text_hash in _tweet_sent_cache:
        age_min = (now - _tweet_sent_cache[text_hash]) / 60
        logger.warning(f"DUPLICATE tweet engellendi (%.0f dk once atildi): {text[:60]}...", age_min)
        return True

    return False


def _mark_tweet_sent(text: str):
    """Basarili tweet'i cache'e kaydet."""
    import time as _time
    text_hash = hashlib.md5(text.encode("utf-8")).hexdigest()
    _tweet_sent_cache[text_hash] = _time.time()


def _validate_ipo_for_tweet(ipo, required_fields: list[str], tweet_type: str) -> bool:
    """IPO verisinin tweet icin yeterli olup olmadigini kontrol eder.

    Eksik veri varsa tweet ATILMAZ, Telegram'a raporlanir.

    Args:
        ipo: IPO model instance
        required_fields: Zorunlu alan listesi (orn: ["company_name", "ticker"])
        tweet_type: Tweet tipi adi (orn: "Son Gun Sabah")

    Returns:
        True: veri yeterli, tweet atilabilir
        False: veri eksik, tweet atilmaz
    """
    missing = []
    for field in required_fields:
        val = getattr(ipo, field, None)
        if val is None or (isinstance(val, str) and not val.strip()):
            missing.append(field)

    if missing:
        ipo_name = getattr(ipo, "company_name", "?") or "?"
        msg = (
            f"Tweet ATILMADI — eksik veri!\n\n"
            f"Tweet tipi: {tweet_type}\n"
            f"IPO: {ipo_name}\n"
            f"Eksik alanlar: {', '.join(missing)}"
        )
        logger.warning(msg)
        _notify_tweet_failure(f"[{tweet_type}] {ipo_name}", f"Eksik veri: {', '.join(missing)}")
        return False

    return True


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


def _notify_tweet_failure(text: str, error_detail: str):
    """Tweet basarisiz olunca Telegram'a bildirim gonder."""
    try:
        from app.services.admin_telegram import notify_scraper_error
        import asyncio
        msg = f"Tweet atilamadi!\n\nTweet: {text[:100]}...\n\nHata: {error_detail[:200]}"
        # sync context'te async fonksiyon cagirmak icin
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(notify_scraper_error("Twitter Tweet Hatası", msg))
        except RuntimeError:
            # Event loop yoksa yeni olustur
            asyncio.run(notify_scraper_error("Twitter Tweet Hatası", msg))
    except Exception:
        pass  # Telegram bildirimi de basarisiz olursa sessizce gec


def _safe_tweet(text: str) -> bool:
    """Tweet atar — ASLA hata firlatmaz, sadece log'a yazar.
    Basarisiz olursa Telegram'a bildirim gonderir.

    httpx + OAuth 1.0a HMAC-SHA1 ile Twitter API v2 kullanir.
    tweepy gerektirmez — Python 3.13 uyumlu.

    Returns:
        True: tweet basarili
        False: tweet basarisiz (ama sistem etkilenmez)
    """
    try:
        # Duplicate kontrolu — ayni tweeti 24 saat icinde tekrar atma
        if _is_duplicate_tweet(text):
            return False

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
            _mark_tweet_sent(text)
            return True
        else:
            error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
            logger.error("Tweet API hatasi: %s", error_msg)
            _notify_tweet_failure(text, error_msg)
            return False

    except Exception as e:
        logger.error(f"Tweet hatasi (sistem etkilenmez): {e}")
        _notify_tweet_failure(text, str(e))
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
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Yeni Halka Arz"):
            return False
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
    """Dağıtım süreci başladığında tweet atar. Tahmini lot varsa ekler."""
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Dağıtıma Çıkış"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""
        end_date = ""
        if ipo.subscription_end:
            end_date = f"\n\U0001F4C5 Son başvuru: {ipo.subscription_end.strftime('%d.%m.%Y')}"
        price_text = f"\n\U0001F4B0 Fiyatı: {ipo.ipo_price} TL" if ipo.ipo_price else ""

        # Tahmini lot bilgisi varsa ekle (parantez içinde tahminidir notu)
        lot_text = ""
        if ipo.estimated_lots_per_person:
            lot_text = f"\n\U0001F4CA Tahmini dağıtım: ~{ipo.estimated_lots_per_person} lot/kişi (tahminidir)"

        text = (
            f"\U0001F4CB Halka Arz Başvuruları Başladı!\n\n"
            f"{ipo.company_name}{ticker_text} için talep toplama süreci başlamıştır."
            f"{price_text}{end_date}{lot_text}\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #BIST #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_distribution_start hatası: {e}")
        return False


# ================================================================
# 3. KESİNLEŞEN DAĞITIM SONUÇLARI
# ================================================================
def tweet_allocation_results(ipo, allocations: list = None) -> bool:
    """Kesinleşen dağıtım sonuçları tweet atar.

    allocations: IPOAllocation listesi veya dict listesi
      Her biri: group_name, allocation_pct, allocated_lots, participant_count, avg_lot_per_person
    """
    try:
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Tahsisat tablosu — kurumsal yurt içi/dışı + bireysel
        table_lines = []
        bireysel_avg_lot = None
        total_applicants = getattr(ipo, "total_applicants", None)

        if allocations:
            for a in allocations:
                # dict veya ORM objesi destekle
                if isinstance(a, dict):
                    grp = a.get("group_name", "")
                    pct = a.get("allocation_pct")
                    lots = a.get("allocated_lots")
                    participants = a.get("participant_count")
                    avg_lot = a.get("avg_lot_per_person")
                else:
                    grp = a.group_name
                    pct = a.allocation_pct
                    lots = a.allocated_lots
                    participants = a.participant_count
                    avg_lot = a.avg_lot_per_person

                # Grup adi Turkce
                grp_labels = {
                    "bireysel": "Bireysel",
                    "yuksek_basvurulu": "Yüksek Başvurulu",
                    "kurumsal_yurtici": "Kurumsal Yurt İçi",
                    "kurumsal_yurtdisi": "Kurumsal Yurt Dışı",
                }
                label = grp_labels.get(grp, grp)

                line = f"• {label}: %{float(pct):.0f}" if pct else f"• {label}"
                table_lines.append(line)

                # Bireysel yatırımcıya düşen ort lot
                if grp == "bireysel" and avg_lot:
                    bireysel_avg_lot = avg_lot

        table_text = "\n".join(table_lines) if table_lines else ""

        # Bireysel yatırımcı sonucu
        bireysel_text = ""
        if bireysel_avg_lot:
            bireysel_text = f"\n\n👤 Bireysel yatırımcıya düşen: ~{int(float(bireysel_avg_lot))} lot/kişi"

        # Toplam başvuran
        applicant_text = ""
        if total_applicants:
            applicant_text = f"\n📊 Toplam başvuran: {int(total_applicants):,} kişi"

        text = (
            f"✅ Kesinleşen Dağıtım Sonuçları\n\n"
            f"{ipo.company_name}{ticker_text}\n\n"
            f"{table_text}"
            f"{bireysel_text}"
            f"{applicant_text}\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #{ipo.ticker or 'Borsa'}"
        )

        # 280 karakter limiti — gerekirse app linkini kaldır
        if len(text) > 280:
            text = (
                f"✅ Kesinleşen Dağıtım Sonuçları\n\n"
                f"{ipo.company_name}{ticker_text}\n\n"
                f"{table_text}"
                f"{bireysel_text}"
                f"{applicant_text}\n\n"
                f"#HalkaArz #{ipo.ticker or 'Borsa'}"
            )

        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_allocation_results hatası: {e}")
        return False


# ================================================================
# 4. SON 4 SAAT HATIRLATMA
# ================================================================
def tweet_last_4_hours(ipo) -> bool:
    """Son 4 saat kala hatirlatma tweeti."""
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Son 4 Saat"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        text = (
            f"\u23F0 Son 4 Saat!\n\n"
            f"{ipo.company_name}{ticker_text} halka arz başvurusu için"
            f" kapanışa son 4 saat kaldı!\n\n"
            f"Başvurunuzu yapmayı unutmayın.\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #SonGün #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet_with_media(text, BANNER_SON_4_SAAT)
    except Exception as e:
        logger.error(f"tweet_last_4_hours hatasi: {e}")
        return False


# ================================================================
# 5. SON 30 DAKIKA HATIRLATMA
# ================================================================
def tweet_last_30_min(ipo) -> bool:
    """Son 30 dakika kala hatirlatma tweeti."""
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Son 30 Dakika"):
            return False
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
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "İlk İşlem Günü"):
            return False
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
# 12. SON GUN SABAH TWEETI (05:00 — hafif uyari tonu)
# ================================================================
def tweet_last_day_morning(ipo) -> bool:
    """Son gun sabahi 05:00'da hafif uyari tonunda tweet.

    Kirmizi degil, turuncu/sari uyari tonu — bilgilendirici.
    Son 30 dk kala hatirlatma atilacagini da belirtir.
    """
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Son Gün Sabah"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Bitis saatini belirle
        end_hour = "17:00"
        if ipo.subscription_hours:
            parts = str(ipo.subscription_hours).split("-")
            if len(parts) >= 2:
                end_hour = parts[-1].strip()

        price_text = f"\n💰 Fiyat: {ipo.ipo_price} TL" if ipo.ipo_price else ""

        text = (
            f"📢 Son Başvuru Günü!\n\n"
            f"{ipo.company_name}{ticker_text} için halka arz başvuruları"
            f" bugün saat {end_hour}'a kadar devam ediyor."
            f"{price_text}\n\n"
            f"⏰ Son anlara kadar hatırlatma yapacağız.\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #{ipo.ticker or 'Borsa'}"
        )
        return _safe_tweet_with_media(text, BANNER_SON_BASVURU_GUNU)
    except Exception as e:
        logger.error(f"tweet_last_day_morning hatasi: {e}")
        return False


# ================================================================
# 13. SIRKET TANITIM TWEETI (ertesi gun 20:00 — izahname sonrasi)
# ================================================================
def tweet_company_intro(ipo) -> bool:
    """Taslak izahname aciklandiktan sonra ertesi gun 20:00'de
    sirket tanitim tweeti — samimi, bilgilendirici ton.

    IPO.company_description'dan ilk paragrafı alir.
    Cok uzunsa son cumleyi kirpar (cumle bazli truncation).
    """
    try:
        if not _validate_ipo_for_tweet(ipo, ["company_name"], "Şirket Tanıtım"):
            return False
        ticker_text = f" (#{ipo.ticker})" if ipo.ticker else ""

        # Sektor bilgisi
        sector_text = ""
        if ipo.sector:
            sector_text = f"\n🏭 Sektör: {ipo.sector}"

        # Sirket aciklamasi — ilk paragraf, cumle bazli kisaltma
        desc_text = ""
        if ipo.company_description:
            full_desc = str(ipo.company_description).strip()
            # Ilk paragrafi al (ilk bos satira kadar)
            first_para = full_desc.split("\n")[0].strip()
            if not first_para:
                first_para = full_desc[:200]

            # Max karakter limiti (tweet icinde ~130 char yer var)
            max_desc_len = 130
            if len(first_para) > max_desc_len:
                # Cumle bazli kirpma — son tam cumleyi bul
                sentences = first_para.replace(".", ".|").replace("!", "!|").replace("?", "?|").split("|")
                trimmed = ""
                for s in sentences:
                    s = s.strip()
                    if not s:
                        continue
                    if len(trimmed) + len(s) + 1 <= max_desc_len:
                        trimmed = f"{trimmed} {s}".strip() if trimmed else s
                    else:
                        break
                first_para = trimmed if trimmed else first_para[:max_desc_len - 3] + "..."

            desc_text = f"\n\n{first_para}"

        price_text = ""
        if ipo.ipo_price:
            price_text = f"\n💰 Halka arz fiyatı: {ipo.ipo_price} TL"

        text = (
            f"📋 Halka Arz Hakkında\n\n"
            f"{ipo.company_name}{ticker_text}"
            f"{sector_text}{price_text}"
            f"{desc_text}\n\n"
            f"📲 Detaylar: {APP_LINK}\n\n"
            f"#HalkaArz #{ipo.ticker or 'Borsa'}"
        )

        # 280 karakter limiti — gerekirse desc kisalt
        if len(text) > 280 and desc_text:
            # Daha da kisalt — sadece ilk cumle
            if ipo.company_description:
                full = str(ipo.company_description).strip().split("\n")[0]
                first_sentence = full.split(".")[0].strip()
                if first_sentence and len(first_sentence) < 120:
                    desc_text = f"\n\n{first_sentence}."
                else:
                    desc_text = ""
            text = (
                f"📋 Halka Arz Hakkında\n\n"
                f"{ipo.company_name}{ticker_text}"
                f"{sector_text}{price_text}"
                f"{desc_text}\n\n"
                f"📲 {APP_LINK}\n\n"
                f"#HalkaArz #{ipo.ticker or 'Borsa'}"
            )

        # Hala sigmazsa desc kaldir
        if len(text) > 280:
            text = (
                f"📋 Halka Arz Hakkında\n\n"
                f"{ipo.company_name}{ticker_text}"
                f"{sector_text}{price_text}\n\n"
                f"📲 {APP_LINK}\n\n"
                f"#HalkaArz #{ipo.ticker or 'Borsa'}"
            )

        return _safe_tweet_with_media(text, BANNER_HALKA_ARZ_HAKKINDA)
    except Exception as e:
        logger.error(f"tweet_company_intro hatasi: {e}")
        return False


# ================================================================
# 14. SPK BEKLEYENLER GORSELLI TWEET (her ayin 1'i)
# ================================================================
def tweet_spk_pending_with_image(pending_count: int, image_path: str = None) -> bool:
    """Her ayin 1'inde SPK onayi bekleyenler gorselli tweet.

    image_path: Yerel gorsel dosya yolu (opsiyonel).
    Gorsel varsa media upload yapilir, yoksa sadece metin atilir.
    """
    try:
        text = (
            f"📊 SPK Onay Bekleyenler\n\n"
            f"Şu an {pending_count} şirket SPK onayı beklemektedir.\n\n"
            f"Güncel listeyi uygulamamızdan takip edebilirsiniz.\n\n"
            f"📲 {APP_LINK}\n\n"
            f"#HalkaArz #SPK #BIST #Borsa"
        )

        if image_path:
            return _safe_tweet_with_media(text, image_path)
        return _safe_tweet(text)
    except Exception as e:
        logger.error(f"tweet_spk_pending_with_image hatasi: {e}")
        return False


def _safe_tweet_with_media(text: str, image_path: str) -> bool:
    """Gorsel + metin tweeti atar.

    1. Twitter v1.1 media/upload ile gorseli yukle → media_id al
    2. Twitter v2 tweets ile tweet at (media_ids ekleyerek)
    """
    try:
        # Duplicate kontrolu — ayni tweeti 24 saat icinde tekrar atma
        if _is_duplicate_tweet(text):
            return False

        creds = _load_credentials()
        if not creds:
            logger.info(f"[TWITTER-DRY-RUN-MEDIA] {text[:60]}... (image={image_path})")
            return False

        if not os.path.exists(image_path):
            logger.warning(f"Gorsel bulunamadi: {image_path}, sadece metin atiliyor")
            return _safe_tweet(text)

        # 1. Media Upload (v1.1 — multipart/form-data)
        upload_url = "https://upload.twitter.com/1.1/media/upload.json"

        # OAuth header — upload icin ozel
        oauth_params = {
            "oauth_consumer_key": creds["api_key"],
            "oauth_nonce": uuid.uuid4().hex,
            "oauth_signature_method": "HMAC-SHA1",
            "oauth_timestamp": str(int(time.time())),
            "oauth_token": creds["access_token"],
            "oauth_version": "1.0",
        }
        signature = _generate_oauth_signature(
            "POST", upload_url, oauth_params,
            creds["api_secret"], creds["access_token_secret"],
        )
        oauth_params["oauth_signature"] = signature
        header_parts = ", ".join(
            f'{k}="{urllib.parse.quote(v, safe="")}"'
            for k, v in sorted(oauth_params.items())
        )
        auth_header = f"OAuth {header_parts}"

        with open(image_path, "rb") as f:
            files = {"media": ("image.png", f, "image/png")}
            upload_resp = httpx.post(
                upload_url,
                files=files,
                headers={"Authorization": auth_header},
                timeout=30.0,
            )

        if upload_resp.status_code not in (200, 201):
            logger.error(f"Media upload hatasi ({upload_resp.status_code}): {upload_resp.text[:200]}")
            return _safe_tweet(text)  # Gorsel basarisiz → sadece metin at

        media_id = upload_resp.json().get("media_id_string")
        if not media_id:
            logger.error("Media upload: media_id alinamadi")
            return _safe_tweet(text)

        logger.info(f"Media upload basarili: media_id={media_id}")

        # 2. Tweet at — media_ids ile
        if len(text) > 280:
            text = text[:277] + "..."

        tweet_auth = _build_oauth_header(creds)
        tweet_resp = httpx.post(
            _TWITTER_TWEET_URL,
            json={
                "text": text,
                "media": {"media_ids": [media_id]},
            },
            headers={
                "Authorization": tweet_auth,
                "Content-Type": "application/json",
            },
            timeout=15.0,
        )

        if tweet_resp.status_code in (200, 201):
            tweet_id = tweet_resp.json().get("data", {}).get("id", "?")
            logger.info(f"Gorselli tweet basarili (id={tweet_id}): {text[:60]}...")
            _mark_tweet_sent(text)
            return True
        else:
            error_msg = f"HTTP {tweet_resp.status_code}: {tweet_resp.text[:200]}"
            logger.error(f"Gorselli tweet hatasi: {error_msg}")
            _notify_tweet_failure(text, f"[MEDIA] {error_msg}")
            return False

    except Exception as e:
        logger.error(f"Gorselli tweet hatasi (sistem etkilenmez): {e}")
        _notify_tweet_failure(text, f"[MEDIA] {str(e)}")
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
