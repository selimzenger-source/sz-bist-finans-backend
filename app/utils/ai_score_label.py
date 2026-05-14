"""AI skor (0-10) -> 9 kategorili etiket donusumu.

Frontend `utils/aiScoreLabel.ts` ile ayni mantik. Backend tarafinda hem yeni
kayit yazarken (kap_all_analyzer) hem de eski kayitlarda backfill icin
kullanilir.

Kategoriler:
  9.0 - 10.0  -> Guclu Olumlu
  8.0 - 8.9   -> Cok Olumlu
  7.0 - 7.9   -> Olumlu
  6.0 - 6.9   -> Hafif Olumlu
  4.1 - 5.9   -> Notr
  3.1 - 4.0   -> Hafif Olumsuz
  2.1 - 3.0   -> Olumsuz
  1.1 - 2.0   -> Cok Olumsuz
  0.0 - 1.0   -> Guclu Olumsuz
"""

from typing import Optional


def score_to_label(score: Optional[float]) -> Optional[str]:
    """0-10 araligindaki AI skorunu 9 kategorili etikete donusturur.

    None / NaN icin None doner.
    """
    if score is None:
        return None
    try:
        s = float(score)
    except (TypeError, ValueError):
        return None
    if s != s:  # NaN check
        return None

    if s >= 9.0:
        return "Güçlü Olumlu"
    if s >= 8.0:
        return "Çok Olumlu"
    if s >= 7.0:
        return "Olumlu"
    if s >= 6.0:
        return "Hafif Olumlu"
    if s >= 4.1:
        return "Nötr"
    if s >= 3.1:
        return "Hafif Olumsuz"
    if s >= 2.1:
        return "Olumsuz"
    if s >= 1.1:
        return "Çok Olumsuz"
    return "Güçlü Olumsuz"


def score_to_group(score: Optional[float]) -> Optional[str]:
    """Skoru 3 ana gruba ayirir (eski API ile uyum icin):
      >= 6.0  -> "Olumlu"
      4.1-5.9 -> "Nötr"
      <= 4.0  -> "Olumsuz"
    """
    if score is None:
        return None
    try:
        s = float(score)
    except (TypeError, ValueError):
        return None
    if s >= 6.0:
        return "Olumlu"
    if s >= 4.1:
        return "Nötr"
    return "Olumsuz"
