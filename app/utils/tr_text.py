"""Türkçe-aware text utilities.

Python'un default .lower() metodu Türkçe için BOZUK:
  "İ".lower() == "i̇" (U+0069 + U+0307 dotted i, 2 karakter)
  "I".lower() == "i" (yanlış — Türkçe'de I.lower() == "ı" olmalı)

Bu pattern matching'i bozar:
  "Yeni İş İlişkisi".lower() == "yeni i̇ş i̇lişkisi"
  "yeni iş ilişkisi" in "yeni i̇ş i̇lişkisi" → False ❌

lower_tr() bunu düzeltir.
"""


def lower_tr(s: str | None) -> str:
    """Türkçe-aware lowercase. Pattern matching için kullan."""
    if not s:
        return ""
    # İ → i (Latin Capital I with Dot Above → normal i)
    # I → ı (ASCII I → Türkçe küçük dotsız ı)
    return s.replace("İ", "i").replace("I", "ı").lower()
