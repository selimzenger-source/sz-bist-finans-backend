"""Borsa İstanbul resmi tatil günleri.

Kaynak: https://www.borsaistanbul.com/resmi-tatil-gunleri

Sadece TAM gün kapalı olunan günler listelenir.
Yarım gün (arefe) günlerinde seans olduğundan haber analizi yapılır.

Pazartesi günkü haber bülteni:
  - Önceki Cuma 07:55 → Pazartesi 07:55 aralığını kapsar (3 gün).
  - Hafta içi normal günlerde: 24 saatlik kesim.
  - Tatilden sonraki ilk iş gününde: son iş gününün 07:55'inden itibaren.
"""

from datetime import date, timedelta


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
