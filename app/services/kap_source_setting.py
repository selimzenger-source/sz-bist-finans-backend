"""KAP veri kaynagi yonetimi.

Telegram bot artik primary kaynak. Uzmanpara/BigPara/Bloomberg HT scraper'lari
yedek olarak duruyor — admin panelden toggle edilebilir.

Kaynak modlari:
  - "telegram"  → Sadece Telegram bot (default, yeni primary)
                   Uzmanpara/BigPara/Bloomberg scraper'lari devre disi
  - "uzmanpara" → Sadece Uzmanpara/BigPara/Bloomberg HT (eski sistem, fallback)
                   Telegram bot kap_all_disclosures'a yazmaz
  - "both"      → Ikisi de yazar — duplicate'lar unique constraint ile elenir
                   (Felaket durumu, geciс modu)

Frontend hicbir degisiklik yapmaz, ayni endpoint'lerden okur.
"""

import logging
from sqlalchemy import select

from app.database import async_session
from app.models.app_setting import AppSetting

logger = logging.getLogger(__name__)

DEFAULT_SOURCE = "telegram"
VALID_SOURCES = {"telegram", "uzmanpara", "both"}
SETTING_KEY = "kap_primary_source"

# In-memory cache — DB sorgusu maliyetini onlemek icin (60 sn TTL)
_cache: dict = {"value": None, "at": 0.0}
_CACHE_TTL = 60.0


async def get_kap_source() -> str:
    """Aktif KAP kaynagini dondurur. 60sn cache.

    Hata durumunda default (telegram) doner.
    """
    import time
    now = time.time()
    if _cache["value"] and (now - _cache["at"]) < _CACHE_TTL:
        return _cache["value"]

    try:
        async with async_session() as db:
            result = await db.execute(
                select(AppSetting).where(AppSetting.key == SETTING_KEY)
            )
            setting = result.scalar_one_or_none()
            if setting and setting.value in VALID_SOURCES:
                _cache["value"] = setting.value
                _cache["at"] = now
                return setting.value
    except Exception as e:
        logger.warning("KAP source ayari okunamadi: %s — default '%s' kullaniliyor", e, DEFAULT_SOURCE)

    _cache["value"] = DEFAULT_SOURCE
    _cache["at"] = now
    return DEFAULT_SOURCE


async def set_kap_source(value: str) -> bool:
    """KAP kaynagini degistirir. Gecersiz deger ise False doner."""
    if value not in VALID_SOURCES:
        return False
    try:
        async with async_session() as db:
            result = await db.execute(
                select(AppSetting).where(AppSetting.key == SETTING_KEY)
            )
            setting = result.scalar_one_or_none()
            if setting:
                setting.value = value
            else:
                db.add(AppSetting(key=SETTING_KEY, value=value))
            await db.commit()
        # Cache invalidate
        _cache["value"] = value
        import time
        _cache["at"] = time.time()
        logger.info("KAP source degistirildi: %s", value)
        return True
    except Exception as e:
        logger.error("KAP source set hatasi: %s", e)
        return False


def is_telegram_active(source: str) -> bool:
    """Telegram bot kap_all_disclosures'a yazmali mi?"""
    return source in ("telegram", "both")


def is_uzmanpara_active(source: str) -> bool:
    """Uzmanpara/BigPara/Bloomberg HT scraper calismali mi?"""
    return source in ("uzmanpara", "both")
