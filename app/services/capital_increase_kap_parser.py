"""KAP bildirimlerinden Sermaye Artirimi state machine parser.

4 KAP tipi taninir:

  STAGE 1 (application) — SPK Basvurusu
    Title/Ozet: "Bedelsiz/Bedelli/Tahsisli Sermaye Artirimina Iliskin SPK Basvuru"
    -> ykk_alindi (yeni kayit)

  STAGE 2 (spk_approval) — SPK Bulteninden gelir, ayri scraper
  STAGE 2B (spk_rejection) — SPK Bulteninden gelir, ayri scraper

  STAGE 3 (distribution_date) — Dagitim Tarihi Ilani
    Title/Ozet: "Bedelsiz Pay Alma Hakki Kullanim Baslangic Tarihi"
              veya "Rucan Hakki Kullanim Baslangic Tarihi"
    -> tarih_belli (UPDATE distribution_date)

  STAGE 4 (split_completed) — Bolunme Gunu Bildirimi
    Body: "TICKER.E %X.XX Ic Kaynaklardan Bedelsiz, bolunme sonrasi teorik fiyat..."
              veya "TICKER.E %X.XX Bedelli, ..."
    -> tamamlandi (UPDATE status, distribution_date=today)
    SKIP: "Temettu" iceriyorsa (bu ayri kategori)

Ornek KAP URL'leri:
  Stage 1 Bedelsiz: 1597495
  Stage 1 Bedelli:  1562229
  Stage 3:          1598621
  Stage 4:          1600858
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any, Optional

from app.utils.tr_text import lower_tr


# ═══════════════════════════════════════════════════════════════════
# Stage Detection
# ═══════════════════════════════════════════════════════════════════

# Stage 1 — SPK Basvurusu
_STAGE1_PATTERNS = [
    "bedelsiz sermaye artırımına ilişkin spk başvuru",
    "bedelsiz sermaye artirimi̇na i̇li̇şki̇n spk başvuru",
    "bedelli sermaye artırımı işlemine ilişkin spk başvuru",
    "bedelli sermaye artırımına ilişkin spk başvuru",
    "tahsisli sermaye artırımına ilişkin spk başvuru",
    "tahsisli sermaye artırımı işlemine ilişkin spk başvuru",
    # Fallback — title bos ise body'de "SPK Basvuru" + "Sermaye Artirim"
    "sermaye artırımı - azaltımı işlemlerine ilişkin bildirim",
]

# Stage 3 — Dagitim Tarihi
_STAGE3_PATTERNS = [
    "bedelsiz pay alma hakkı kullanım başlangıç tarihi",
    "rüçhan hakkı kullanım başlangıç tarihi",
    "bedelli pay alma hakkı kullanım başlangıç tarihi",
]

# Stage 4 — Bolunme Gunu (BISTECH Pay Piyasasi Al-Sat Sistemi Duyurusu)
# Pattern: TICKER.E %X.XX <type>, bolunme sonrasi teorik fiyat: Y TL
_STAGE4_REGEX = re.compile(
    r"([A-Z][A-Z0-9]{2,5})\.E\s+%([\d.,]+)\s+(.*?)bölünme sonrası teorik fiyat[:\s]+([\d.,]+)",
    re.IGNORECASE | re.DOTALL,
)
# Temettu icermemeli
_TEMETTU_MARKERS = ["temettü", "kar payı dağıtım", "nakit kar payı"]


def detect_stage(title: str, body: str) -> Optional[str]:
    """Bir KAP bildiriminin hangi stage'e ait oldugunu doner.

    Returns:
        'application' | 'distribution_date' | 'split_completed' | None
    """
    title_lo = lower_tr(title or "")
    body_lo = lower_tr(body or "")[:3000]

    # Stage 4 kontrol — body'de TICKER.E % ... bolunme sonrasi
    if "bölünme sonrası teorik fiyat" in body_lo:
        # Temettu degilse kabul et
        if not any(m in body_lo for m in _TEMETTU_MARKERS):
            if _STAGE4_REGEX.search(body):
                return "split_completed"

    # Stage 3 kontrol — title/body'de "Kullanım Başlangıç Tarihi"
    if any(p in title_lo or p in body_lo for p in _STAGE3_PATTERNS):
        return "distribution_date"

    # Stage 1 kontrol — title/body'de "SPK Başvuru"
    if any(p in title_lo or p in body_lo for p in _STAGE1_PATTERNS):
        return "application"

    return None


# ═══════════════════════════════════════════════════════════════════
# Yardimci parser'lar
# ═══════════════════════════════════════════════════════════════════

def _parse_tr_number(s: str) -> Optional[float]:
    """1.845.000.000,00 -> 1845000000.0; 638,33834 -> 638.33834; 95.000 -> 95000

    KRITIK: Tek nokta sonrasi 3 hane ise binlik ayrac say. Onceki versiyon
    "95.000" -> 95.0 yapiyordu (1000x kucuk).
    """
    if not s:
        return None
    s = s.strip().replace(" ", "")
    if "," in s:
        int_part, dec_part = s.rsplit(",", 1)
        int_part = int_part.replace(".", "")
        try:
            return float(f"{int_part}.{dec_part}")
        except ValueError:
            return None
    if "." in s:
        parts = s.split(".")
        if len(parts) >= 2 and all(len(p) == 3 and p.isdigit() for p in parts[1:]):
            try:
                return float(s.replace(".", ""))
            except ValueError:
                return None
        try:
            return float(s)
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_tr_date(s: str) -> Optional[date]:
    """DD.MM.YYYY veya DD/MM/YYYY -> date"""
    if not s:
        return None
    m = re.search(r"([0-3]?\d)[./]([01]?\d)[./](20\d{2})", s)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════
# Stage 1 — SPK Basvuru parser
# ═══════════════════════════════════════════════════════════════════

def parse_application(title: str, body: str) -> dict[str, Any]:
    """Stage 1 (SPK basvuru) bildiriminden veri cikar.

    Returns:
      {
        "type": "bedelsiz" | "bedelli" | "tahsisli",
        "ykk_date": date | None,
        "mevcut_sermaye_tl": float | None,    # eski sermaye
        "ulasilacak_sermaye_tl": float | None,  # bolunme sonrasi (capital_increases.bolunme_sonrasi_sermaye_tl)
        "bedelli_pct": float | None,
        "bedelsiz_pct": float | None,
        "tahsisli_pct": float | None,
      }
    """
    out: dict[str, Any] = {
        "type": None,
        "ykk_date": None,
        "mevcut_sermaye_tl": None,
        "ulasilacak_sermaye_tl": None,
        "bedelli_pct": None,
        "bedelsiz_pct": None,
        "tahsisli_pct": None,
    }
    if not body:
        return out

    title_lo = lower_tr(title or "")
    body_lo = lower_tr(body)
    combined_lo = title_lo + " " + body_lo[:2000]

    # 1) Tip belirle
    if "tahsisli" in combined_lo:
        out["type"] = "tahsisli"
    elif "bedelli" in combined_lo or "rüçhan" in combined_lo:
        out["type"] = "bedelli"
    elif "bedelsiz" in combined_lo:
        out["type"] = "bedelsiz"

    # 2) YKK tarihi — "Yönetim Kurulu Karar Tarihi 18.02.2026"
    ykk_m = re.search(
        r"yönetim\s+kurulu\s+kara[rl]\s*tarihi[\s|:]*([0-3]?\d[./][01]?\d[./]20\d{2})",
        body, re.IGNORECASE,
    )
    if ykk_m:
        out["ykk_date"] = _parse_tr_date(ykk_m.group(1))

    # 3) Mevcut Sermaye — "Mevcut Sermaye (TL) 115.123.372,35"
    mevcut_m = re.search(
        r"mevcut\s+sermaye\s*(?:\(tl\))?\s*[\s|:]*([0-9.,]{4,30})",
        body, re.IGNORECASE,
    )
    if mevcut_m:
        out["mevcut_sermaye_tl"] = _parse_tr_number(mevcut_m.group(1))

    # 4) Ulasilacak Sermaye
    ulas_m = re.search(
        r"ulaşılacak\s+sermaye\s*(?:\(tl\))?\s*[\s|:]*([0-9.,]{4,30})",
        body, re.IGNORECASE,
    )
    if ulas_m:
        out["ulasilacak_sermaye_tl"] = _parse_tr_number(ulas_m.group(1))

    # 5) Oranlar — Bedelsiz icin "İç Kaynaklardan Bedelsiz Pay Alma Oranı (%) ... 638,33834"
    if out["type"] == "bedelsiz":
        # Toplam Bedelsiz Pay Alma Oranı (öncelik) — yoksa İç Kaynaklardan
        for pat in [
            r"toplam\s+bedelsiz\s+pay\s+alma\s+oran[ıi]\s*(?:\(%\))?\s*[\s|:]*([0-9.,]{1,15})",
            r"iç\s+kaynaklardan\s+bedelsiz\s+pay\s+alma\s+oran[ıi]\s*(?:\(%\))?\s*[\s|:]*([0-9.,]{1,15})",
        ]:
            m = re.search(pat, body, re.IGNORECASE)
            if m:
                v = _parse_tr_number(m.group(1))
                if v and 0 < v < 10000:
                    out["bedelsiz_pct"] = v
                    break
        # Mevcut + Ulasilacak'tan da hesapla, regex bulamadi ise
        if out["bedelsiz_pct"] is None and out["mevcut_sermaye_tl"] and out["ulasilacak_sermaye_tl"]:
            try:
                pct = ((out["ulasilacak_sermaye_tl"] - out["mevcut_sermaye_tl"]) / out["mevcut_sermaye_tl"]) * 100
                if 0 < pct < 10000:
                    out["bedelsiz_pct"] = round(pct, 2)
            except Exception:
                pass

    elif out["type"] == "bedelli":
        # "Rüçhan Hakkı Kullanım Oranı (%) 100,000"
        m = re.search(
            r"rüçhan\s+hakkı\s+kullanım\s+oran[ıi]\s*(?:\(%\))?\s*[\s|:]*([0-9.,]{1,15})",
            body, re.IGNORECASE,
        )
        if m:
            v = _parse_tr_number(m.group(1))
            if v and 0 < v < 10000:
                out["bedelli_pct"] = v
        if out["bedelli_pct"] is None and out["mevcut_sermaye_tl"] and out["ulasilacak_sermaye_tl"]:
            try:
                pct = ((out["ulasilacak_sermaye_tl"] - out["mevcut_sermaye_tl"]) / out["mevcut_sermaye_tl"]) * 100
                if 0 < pct < 10000:
                    out["bedelli_pct"] = round(pct, 2)
            except Exception:
                pass

    elif out["type"] == "tahsisli":
        # "Tahsisli Pay Alma Oranı (%) X" — pattern yakalamak zor, fallback hesapla
        if out["mevcut_sermaye_tl"] and out["ulasilacak_sermaye_tl"]:
            try:
                pct = ((out["ulasilacak_sermaye_tl"] - out["mevcut_sermaye_tl"]) / out["mevcut_sermaye_tl"]) * 100
                if 0 < pct < 10000:
                    out["tahsisli_pct"] = round(pct, 2)
            except Exception:
                pass

    return out


# ═══════════════════════════════════════════════════════════════════
# Stage 3 — Dagitim Tarihi parser
# ═══════════════════════════════════════════════════════════════════

def parse_distribution_date(body: str) -> Optional[date]:
    """Stage 3 bildiriminden dagitim tarihini cek.

    Pattern:
      "Bedelsiz Pay Alma Hakkı Kullanım Başlangıç Tarihi 05.05.2026"
      "Rüçhan Hakkı Kullanım Başlangıç Tarihi 05.05.2026"
    """
    if not body:
        return None

    for pat in [
        r"bedelsiz\s+pay\s+alma\s+hakkı\s+kullanım\s+başlangıç\s+tarihi\s*[\s|:]*([0-3]?\d[./][01]?\d[./]20\d{2})",
        r"rüçhan\s+hakkı\s+kullanım\s+başlangıç\s+tarihi\s*[\s|:]*([0-3]?\d[./][01]?\d[./]20\d{2})",
        r"bedelli\s+pay\s+alma\s+hakkı\s+kullanım\s+başlangıç\s+tarihi\s*[\s|:]*([0-3]?\d[./][01]?\d[./]20\d{2})",
        # Generic fallback
        r"kullanım\s+başlangıç\s+tarihi\s*[\s|:]*([0-3]?\d[./][01]?\d[./]20\d{2})",
    ]:
        m = re.search(pat, body, re.IGNORECASE)
        if m:
            d = _parse_tr_date(m.group(1))
            if d:
                return d
    return None


# ═══════════════════════════════════════════════════════════════════
# Stage 4 — Bolunme Gunu (Tamamlandi) parser
# ═══════════════════════════════════════════════════════════════════

def parse_split_completed(body: str) -> Optional[dict[str, Any]]:
    """Stage 4 bildiriminden bolunme bilgilerini cek.

    Pattern: "TICKER.E %X.XX İç Kaynaklardan Bedelsiz, bölünme sonrası teorik fiyat: Y TL"

    Returns:
      {
        "ticker": str,
        "percentage": float,
        "type": "bedelsiz" | "bedelli",
        "theoretical_price": float | None,
      }
      veya None (Temettu ise/match yoksa)
    """
    if not body:
        return None

    body_lo = lower_tr(body[:3000])

    # Temettu filter — bu ayri kategori, sermaye artirimi degil
    if any(m in body_lo for m in _TEMETTU_MARKERS):
        return None

    m = _STAGE4_REGEX.search(body)
    if not m:
        return None

    ticker = m.group(1).upper()
    pct = _parse_tr_number(m.group(2))
    middle_text = m.group(3).lower()
    price = _parse_tr_number(m.group(4))

    # Tip — middle_text'te "ic kaynaklardan bedelsiz" veya "bedelli" gec
    if "bedelsiz" in middle_text or "ic kaynaklardan" in lower_tr(middle_text):
        tip = "bedelsiz"
    elif "bedelli" in middle_text or "rüçhan" in middle_text:
        tip = "bedelli"
    else:
        tip = "bedelsiz"  # default

    if pct is None or pct <= 0 or pct > 10000:
        return None

    return {
        "ticker": ticker,
        "percentage": pct,
        "type": tip,
        "theoretical_price": price,
    }
