"""Borsa İstanbul resmi tatil günleri.

Kaynak: https://www.borsaistanbul.com/resmi-tatil-gunleri

Sadece TAM gün kapalı olunan günler listelenir.
Yarım gün (arefe) günlerinde seans olduğundan haber analizi yapılır.

Pazartesi günkü haber bülteni:
  - Önceki Cuma 07:55 → Pazartesi 07:55 aralığını kapsar (3 gün).
  - Hafta içi normal günlerde: 24 saatlik kesim.
  - Tatilden sonraki ilk iş gününde: son iş gününün 07:55'inden itibaren.
"""

from datetime import date, datetime, time, timedelta

try:
    from zoneinfo import ZoneInfo
    _TR_TZ = ZoneInfo("Europe/Istanbul")
except Exception:  # pragma: no cover
    _TR_TZ = None

# BIST pay piyasası AÇILIŞ SEANSI başlangıcı (09:40) — tedbir bu anda kalkar.
# (Açılış seansı emir toplama 09:40'ta başlar; sürekli işlem 10:00.)
SESSION_OPEN_HOUR = 9
SESSION_OPEN_MINUTE = 40


# 2026 — Borsa İstanbul'un kapalı olduğu TAM gün tarihleri
_BIST_HOLIDAYS_2026: set[date] = {
    date(2026, 1, 1),    # Yılbaşı
    date(2026, 3, 20),   # Ramazan Bayramı 1. gün
    date(2026, 3, 21),   # Ramazan Bayramı 2. gün
    date(2026, 3, 22),   # Ramazan Bayramı 3. gün
    date(2026, 4, 23),   # 23 Nisan
    date(2026, 5, 1),    # 1 Mayıs
    date(2026, 5, 19),   # 19 Mayıs
    date(2026, 5, 27),   # Kurban Bayramı 1. gün
    date(2026, 5, 28),   # Kurban Bayramı 2. gün
    date(2026, 5, 29),   # Kurban Bayramı 3. gün
    date(2026, 5, 30),   # Kurban Bayramı 4. gün
    date(2026, 7, 15),   # Demokrasi
    date(2026, 8, 30),   # Zafer
    date(2026, 10, 29),  # Cumhuriyet
}

# 2027 — tahmini (hicri takvim kayışı olabilir, doğrulanmalı)
_BIST_HOLIDAYS_2027: set[date] = {
    date(2027, 1, 1),    # Yılbaşı
    date(2027, 4, 23),   # 23 Nisan
    date(2027, 5, 1),    # 1 Mayıs
    date(2027, 5, 19),   # 19 Mayıs
    date(2027, 7, 15),   # Demokrasi
    date(2027, 8, 30),   # Zafer
    date(2027, 10, 29),  # Cumhuriyet
}

_ALL_HOLIDAYS: set[date] = _BIST_HOLIDAYS_2026 | _BIST_HOLIDAYS_2027


def is_bist_holiday(d: date) -> bool:
    """Borsa o gun tam kapali mi? Hafta sonu DAHIL edilmez."""
    return d in _ALL_HOLIDAYS


def is_trading_day(d: date) -> bool:
    """Borsa o gun acik mi? (Hafta ici + tatil degil)"""
    if d.weekday() >= 5:  # 5=Cumartesi, 6=Pazar
        return False
    return not is_bist_holiday(d)


def previous_trading_day(d: date) -> date:
    """d'den önceki son işlem gününü döner.

    Örnek: Pazartesi -> önceki Cuma (veya tatil varsa daha öncesi)
    """
    prev = d - timedelta(days=1)
    while not is_trading_day(prev):
        prev = prev - timedelta(days=1)
    return prev


def next_trading_day(d: date) -> date:
    """d'den sonraki ilk işlem gününü döner."""
    nxt = d + timedelta(days=1)
    while not is_trading_day(nxt):
        nxt = nxt + timedelta(days=1)
    return nxt


# ════════════════════════════════════════════════════════════════════════════
#  TEDBİR (VBTS) KALKIŞ MANTIĞI
#  end_date = SON yasaklı seans. Engel, bir sonraki İŞLEM GÜNÜ seans açılışında
#  (~10:00) kalkar. Hafta sonu/resmi tatil atlanır (Cuma biten → Pazartesi kalkar).
# ════════════════════════════════════════════════════════════════════════════

def tedbir_lift_date(end_date: date | None) -> date | None:
    """Tedbirin KALKACAĞI gün = end_date'ten sonraki ilk işlem günü."""
    if end_date is None:
        return None
    return next_trading_day(end_date)


def _now_tr() -> datetime:
    return datetime.now(_TR_TZ) if _TR_TZ else datetime.now()


def tedbir_lift_datetime(end_date: date | None) -> datetime | None:
    """Tedbirin kalkacağı tam an: lift günü saat 10:00 (TR)."""
    lift = tedbir_lift_date(end_date)
    if lift is None:
        return None
    dt = datetime.combine(lift, time(SESSION_OPEN_HOUR, SESSION_OPEN_MINUTE))
    return dt.replace(tzinfo=_TR_TZ) if _TR_TZ else dt


def cautious_status(end_date: date | None, is_active: bool,
                    now: datetime | None = None) -> dict:
    """Tedbir kaydının ANLIK durumu.

    Döner:
      {
        "status": "active" | "ended",
        "lift_date": date|None,          # kalkış günü
        "ends_this_session": bool,       # bugün seans açılışında kalkacak (BU SEANS)
        "days_to_lift": int|None,        # kalkışa kalan TAKVİM günü (0=bugün)
      }
    Mantık:
      - now >= lift_datetime → ended (engel kalktı)
      - now < lift_datetime ve is_active=False → ended (erken iptal — gizli)
      - now < lift_datetime ve is_active=True → active
    """
    if now is None:
        now = _now_tr()
    lift = tedbir_lift_date(end_date)
    lift_dt = tedbir_lift_datetime(end_date)

    if lift_dt is None:
        # end_date yok → süresiz; sadece is_active belirler
        return {"status": "active" if is_active else "ended",
                "lift_date": None, "lift_at": None,
                "ends_this_session": False, "days_to_lift": None, "mins_to_lift": None}

    if now >= lift_dt:
        status = "ended"
    elif not is_active:
        status = "ended"   # erken iptal
    else:
        status = "active"

    ends_this_session = (status == "active" and now.date() == lift)
    days_to_lift = (lift - now.date()).days
    mins_to_lift = int((lift_dt - now).total_seconds() // 60) if status == "active" else None
    return {"status": status, "lift_date": lift, "lift_at": lift_dt,
            "ends_this_session": ends_this_session, "days_to_lift": days_to_lift,
            "mins_to_lift": mins_to_lift}
