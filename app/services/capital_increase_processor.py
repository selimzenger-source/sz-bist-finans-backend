"""Sermaye Artırımı Processor — KAP bildirimlerinden state machine.

Akis:
1. Yeni KAP bildirimi geldiginde process_kap_disclosure(disclosure) cagrilir
2. Title pattern check ile sermaye artirimi olup olmadigi belirlenir
3. Etkinlik tipi siniflandirilir (YKK / SPK Onay / SPK Red / Tarih Ilani)
4. Gemini 2.5 Flash ile body'den yapilandirilmis veri (yuzde, tutar, tarih, type) cikarilir
5. capital_increases tablosunda state machine uyari guncellenir

Frontend (3 sekme):
- bedelsiz: ucretsiz sermaye artirimi
- bedelli:  rights issue (mevcut paydaslarin satin alma hakki)
- tahsisli: belirli yatirimcilara tahsis (private placement)

Status akisi:
  ykk_alindi -> spk_onayli -> tarih_belli -> dagitiliyor -> tamamlandi
              \-> reddedildi (terminal)
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone
from typing import Any, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.capital_increase import CapitalIncrease
from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════
# DUPLICATE MERGE — güvenlik ağı (birden çok işleyici/scraper aynı
# sermaye artırımı için ayrı satır açabiliyor: YKK stub + SPK onaylı vb).
# Aynı (ticker, type) için AÇIK (terminal olmayan) kayıtları tek satırda
# birleştirir: en İLERİ aşamayı tutar, eksik tarih/oran/url'leri katlar,
# diğerlerini siler. Günlük job + her KAP işleme sonrası çağrılır.
# ═══════════════════════════════════════════════════════════════════

_STATUS_RANK = {
    "tamamlandi": 5, "dagitiliyor": 4, "tarih_belli": 3,
    "spk_onayli": 2, "ykk_alindi": 1,
}
_MERGE_DATE_COLS = ("ykk_date", "spk_approval_date", "distribution_date")
_MERGE_FOLD_COLS = (
    "percentage", "amount_tl", "bedelli_pct", "bedelsiz_pct", "tahsisli_pct",
    "bolunme_sonrasi_sermaye_tl", "company_name",
    "ykk_date", "ykk_kap_disclosure_id", "ykk_kap_url",
    "spk_approval_date", "spk_approval_kap_disclosure_id", "spk_approval_kap_url",
    "distribution_date", "distribution_kap_disclosure_id", "distribution_kap_url",
)


async def merge_duplicate_capital_increases(db: AsyncSession) -> int:
    """Aynı (ticker, type) açık sermaye artırımı kayıtlarını tek satırda birleştir.

    Döner: silinen (merge edilen) satır sayısı.
    """
    from collections import defaultdict
    stmt = select(CapitalIncrease).where(
        CapitalIncrease.status.notin_(["tamamlandi", "reddedildi"])
    )
    rows = (await db.execute(stmt)).scalars().all()

    # SANITIZE: saçma yüzdeleri temizle (>%1000 = parse hatası — nominal tutar
    # yüzde sanılmış: 202380%, 16666% gibi). Yanlış veri göstermektense boş bırak.
    for r in rows:
        for f in ("bedelli_pct", "bedelsiz_pct", "tahsisli_pct"):
            v = getattr(r, f, None)
            if v is not None and v > 1000:
                setattr(r, f, None)

    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        groups[(r.ticker, r.type)].append(r)

    deleted = 0
    for _key, grp in groups.items():
        if len(grp) < 2:
            continue

        def _score(r):
            ndates = sum(1 for c in _MERGE_DATE_COLS if getattr(r, c) is not None)
            return (_STATUS_RANK.get(r.status, 0), ndates, r.id or 0)

        grp.sort(key=_score, reverse=True)
        primary, others = grp[0], grp[1:]
        # Eksik alanları diğerlerinden katla (primary boşsa doldur)
        for o in others:
            for f in _MERGE_FOLD_COLS:
                pv = getattr(primary, f, None)
                ov = getattr(o, f, None)
                if (pv is None or pv == "") and ov not in (None, ""):
                    setattr(primary, f, ov)
        for o in others:
            await db.delete(o)
            deleted += 1
        logger.info("Capital merge: %s/%s — tut #%s (%s), sil %s",
                    primary.ticker, primary.type, primary.id, primary.status,
                    [o.id for o in others])
    if deleted:
        await db.flush()
    return deleted


# ═══════════════════════════════════════════════════════════════════
# Fast title-pattern siniflandirici (AI cagirilmadan once filtre)
# ═══════════════════════════════════════════════════════════════════

# Sermaye artirimi ile ilgili KAP basliklari
_TITLE_PATTERNS_CAPITAL_INCREASE = [
    "sermaye artırımı",
    "sermaye artirimi",
    "sermaye azaltımı",
    "bedelsiz sermaye",
    "bedelli sermaye",
    "tahsisli sermaye",
    "rüçhan hakkı",
    "ruchan hakki",
    "kayıtlı sermaye tavanı",
    "ödenmiş sermaye",
    "odenmis sermaye",
]

# Etkinlik tipini belirleyen kaliplar
_PATTERN_YKK = [
    "yönetim kurulu kararı", "yonetim kurulu karari",
    "yönetim kurulunun", "yönetim kurulu kararının",
]
_PATTERN_SPK_APPROVAL = [
    "spk onay", "spk'nın onay", "spk tarafından onay",
    "sermaye piyasası kurulu onay", "sermaye piyasasi kurulu onay",
    "kabul edilmiştir", "kabul edilmis",
    "izahname onay", "ihraç belgesi onay",
]
_PATTERN_SPK_REJECTION = [
    "reddedilmiştir", "reddedildigi", "reddedildi",
    "olumsuz görüş", "olumsuz gorus",
    "iade edil", "uygun bulunmamış",
]
_PATTERN_DISTRIBUTION = [
    "pay dağıtım tarihi", "pay dagitim tarihi",
    "bedelsiz pay alma", "bedelli pay alma",
    "rüçhan hakkı kullanım", "ruchan hakki kullanim",
    "kayda alınmış", "kayda alinmis",
    "kullanım dönemi", "kullanim donemi",
]


def is_capital_increase(title: str) -> bool:
    """Hizli title-pattern check — sermaye artirimi ile ilgili mi?"""
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _TITLE_PATTERNS_CAPITAL_INCREASE)


def classify_event(title: str) -> str:
    """Etkinlik tipini siniflandirir.

    Returns:
        'ykk'           — Yonetim Kurulu Karari
        'spk_approval'  — SPK onayladi
        'spk_rejection' — SPK reddetti
        'distribution'  — Dagitim/kullanim tarihi ilan edildi
        'unknown'       — Sermaye artirimi ile ilgili ama spesifik olay belirsiz
    """
    if not title:
        return "unknown"
    t = lower_tr(title)

    # Oncelik: red > onay > tarih > YKK
    if any(p in t for p in _PATTERN_SPK_REJECTION):
        return "spk_rejection"
    if any(p in t for p in _PATTERN_SPK_APPROVAL):
        return "spk_approval"
    if any(p in t for p in _PATTERN_DISTRIBUTION):
        return "distribution"
    if any(p in t for p in _PATTERN_YKK):
        return "ykk"
    return "unknown"


# ═══════════════════════════════════════════════════════════════════
# Type tahmini — bedelsiz / bedelli / tahsisli
# ═══════════════════════════════════════════════════════════════════

def infer_type_from_text(text: str) -> str:
    """Metinden sermaye artirimi tipini tahmin eder."""
    if not text:
        return "bedelsiz"
    t = lower_tr(text)
    if "tahsisli" in t:
        return "tahsisli"
    if "bedelli" in t or "rüçhan" in t or "ruchan" in t:
        return "bedelli"
    return "bedelsiz"  # default


# ═══════════════════════════════════════════════════════════════════
# Regex bazli yedek extraction (AI calismazsa)
# ═══════════════════════════════════════════════════════════════════

_PCT_REGEX = re.compile(
    r"(?:%\s*|yüzde\s*)([0-9]{1,4}(?:[.,][0-9]{1,4})?)",
    re.IGNORECASE,
)
_AMOUNT_REGEX = re.compile(
    r"([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]+)?)\s*(?:tl|türk lirası|turk lirasi)",
    re.IGNORECASE,
)
_DATE_REGEX = re.compile(
    r"([0-3]?[0-9])[./]([0-1]?[0-9])[./](20[0-9]{2})"
)


def _parse_tr_number(s: str) -> Optional[float]:
    """Turkce sayi formati: 1.845.000.000,00 -> 1845000000.0"""
    if not s:
        return None
    s = s.strip()
    # Eger virgul varsa: nokta=binlik, virgul=ondalik
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # Sadece nokta varsa: birden fazla varsa binlik, tek varsa ondalik olabilir
        if s.count(".") > 1:
            s = s.replace(".", "")
    try:
        return float(s)
    except ValueError:
        return None


def _parse_tr_date(s: str) -> Optional[date]:
    """DD.MM.YYYY veya DD/MM/YYYY -> date"""
    m = _DATE_REGEX.search(s or "")
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    except (ValueError, TypeError):
        return None


def regex_extract(body: str) -> dict[str, Any]:
    """AI calismazsa minimum bilgiyi regex ile cek."""
    out: dict[str, Any] = {}
    if not body:
        return out

    pct_m = _PCT_REGEX.search(body)
    if pct_m:
        val = _parse_tr_number(pct_m.group(1))
        if val is not None and 0 < val < 10000:
            out["percentage"] = val

    amt_m = _AMOUNT_REGEX.search(body)
    if amt_m:
        val = _parse_tr_number(amt_m.group(1))
        if val is not None and val > 1000:  # 1000 TL altindakiler ihtimal disi
            out["amount_tl"] = val

    return out


# ═══════════════════════════════════════════════════════════════════
# Gemini AI parser
# ═══════════════════════════════════════════════════════════════════

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_AI_TIMEOUT = 30


def _get_gemini_key() -> Optional[str]:
    try:
        from app.config import get_settings
        s = get_settings()
        return s.GEMINI_API_KEY if s.GEMINI_API_KEY else None
    except Exception:
        return None


_PARSE_PROMPT = """Asagidaki KAP sermaye artirimi bildirimini analiz et ve yapilandirilmis JSON dondur.

KAP BILDIRIMI:
Hisse: {ticker}
Baslik: {title}
Icerik:
{body}

Donen JSON sablonu (eksik alan icin null kullan):
{{
  "type": "bedelsiz" | "bedelli" | "tahsisli",
  "percentage": <sermaye artis yuzdesi, ornek 990.99>,
  "amount_tl": <yeni eklenen sermaye tutari TL, ornek 545000000>,
  "ykk_date": "YYYY-MM-DD" (yonetim kurulu kararı tarihi),
  "spk_approval_date": "YYYY-MM-DD" (SPK onay tarihi),
  "distribution_date": "YYYY-MM-DD" (pay dagitim/bolunme tarihi)
}}

KURALLAR:
- Sadece JSON dondur, baska metin yazma.
- Tarihler bildirim icindeki ifade edilen tarihler (rapor tarihi degil).
- Yuzde: yeni cikarilan paylarin mevcut sermayeye orani. ornegin "100 TL'lik sermayeden 990 TL'ye cikis" = 990 (yeni/eski). Genelde basliktaki "%X" rakamidir.
- Tutar: yeni eklenen sermaye TL cinsinden (eski + yeni - eski = yeni eklenen).
- Eger bilgi yoksa null kullan, tahmin etme.
- Type:
  * bedelsiz = ucretsiz dagitim (mevcut paydaslara)
  * bedelli  = paydaslarin satin alma hakki (rüçhan)
  * tahsisli = belirli yatirimcilara tahsis (private placement)
"""


async def ai_parse_capital_increase(
    ticker: str,
    title: str,
    body: str,
) -> dict[str, Any]:
    """Gemini ile KAP body'sinden yapilandirilmis veri cikar.

    Returns:
        {
          "type": str,
          "percentage": float | None,
          "amount_tl": float | None,
          "ykk_date": date | None,
          "spk_approval_date": date | None,
          "distribution_date": date | None,
        }
    """
    # 1. Default + regex fallback
    out: dict[str, Any] = {
        "type": infer_type_from_text(f"{title}\n{body[:500] if body else ''}"),
        "percentage": None,
        "amount_tl": None,
        "ykk_date": None,
        "spk_approval_date": None,
        "distribution_date": None,
    }
    if body:
        out.update(regex_extract(body))

    # 2. Gemini ile ozellikle tarih + yuzde + tutar
    gemini_key = _get_gemini_key()
    if not gemini_key or not body:
        return out

    prompt = _PARSE_PROMPT.format(
        ticker=ticker,
        title=title or "",
        body=(body or "")[:4000],  # Gemini'ye max 4K karakter
    )

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _GEMINI_URL,
                headers={
                    "Authorization": f"Bearer {gemini_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Sen finansal verileri yapilandirilmis JSON'a ceviren bir analizcisin. SADECE JSON dondur."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 1024,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                if content:
                    parsed = _parse_ai_json(content.strip())
                    if parsed:
                        # AI sonuclari ile guncelle (regex'i ezecek sekilde)
                        if parsed.get("type") in ("bedelsiz", "bedelli", "tahsisli"):
                            out["type"] = parsed["type"]
                        for k in ("percentage", "amount_tl"):
                            v = parsed.get(k)
                            if isinstance(v, (int, float)) and v > 0:
                                out[k] = float(v)
                        for k in ("ykk_date", "spk_approval_date", "distribution_date"):
                            d = parsed.get(k)
                            if isinstance(d, str):
                                try:
                                    out[k] = date.fromisoformat(d)
                                except ValueError:
                                    pass
            else:
                logger.warning("Capital AI: HTTP %s — %s", resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("Capital AI hata: %s", e)

    return out


def _parse_ai_json(text: str) -> Optional[dict[str, Any]]:
    """AI yanitindan JSON cikar (markdown fence'ler vs.)"""
    if not text:
        return None
    # Markdown fence kaldir
    if "```" in text:
        text = re.sub(r"```(?:json)?\s*", "", text)
        text = text.replace("```", "")
    # Ilk { ile son } arasi
    s = text.find("{")
    e = text.rfind("}")
    if s < 0 or e < 0 or e < s:
        return None
    try:
        return json.loads(text[s:e + 1])
    except json.JSONDecodeError:
        return None


# ═══════════════════════════════════════════════════════════════════
# State machine — disclosure islemi
# ═══════════════════════════════════════════════════════════════════

async def process_kap_disclosure(
    db: AsyncSession,
    *,
    disclosure_id: int,
    ticker: str,
    company_name: Optional[str],
    title: str,
    body: Optional[str],
    kap_url: Optional[str],
    published_at: Optional[datetime],
) -> Optional[CapitalIncrease]:
    """KAP bildirimini sermaye artirimi state machine'e gonderir.

    Sermaye artirimi degilse None doner.
    Yeni kayit olusturulursa veya mevcut kayit guncellenirse CapitalIncrease doner.
    """
    if not is_capital_increase(title):
        return None

    event_type = classify_event(title)
    if event_type == "unknown":
        # Sermaye artirimi ile ilgili ama spesifik etkinlik yok — yine de YKK olarak kayda al
        # (sonraki KAP'lar daha net bilgi getirebilir)
        event_type = "ykk"

    # Body + title'dan yapilandirilmis veri
    parsed = await ai_parse_capital_increase(ticker, title, body or "")

    sa_type = parsed.get("type") or "bedelsiz"
    pct = parsed.get("percentage")
    amt = parsed.get("amount_tl")
    ykk_dt = parsed.get("ykk_date")
    spk_dt = parsed.get("spk_approval_date")
    dist_dt = parsed.get("distribution_date")

    # Eger ykk_date AI'dan gelmedi ama event YKK ise published_at'ı kullan
    if event_type == "ykk" and not ykk_dt and published_at:
        ykk_dt = published_at.date()

    # Mevcut kayit ara (ayni ticker + tip + bir aylik pencere)
    existing: Optional[CapitalIncrease] = None
    stmt = (
        select(CapitalIncrease)
        .where(CapitalIncrease.ticker == ticker)
        .where(CapitalIncrease.type == sa_type)
        .where(CapitalIncrease.status.notin_(["tamamlandi", "reddedildi"]))
        .order_by(CapitalIncrease.created_at.desc())
        .limit(1)
    )
    res = await db.execute(stmt)
    existing = res.scalar_one_or_none()

    today = date.today()

    if event_type == "ykk":
        if not existing:
            # Yeni kayit
            new_row = CapitalIncrease(
                ticker=ticker,
                company_name=company_name,
                type=sa_type,
                percentage=pct,
                amount_tl=amt,
                ykk_date=ykk_dt,
                ykk_kap_disclosure_id=disclosure_id,
                ykk_kap_url=kap_url,
                status="ykk_alindi",
            )
            db.add(new_row)
            await db.flush()
            logger.info("Capital: yeni YKK kaydi (%s, %s, %%%s)", ticker, sa_type, pct)
            return new_row
        # Mevcudu guncelle
        if not existing.ykk_date and ykk_dt:
            existing.ykk_date = ykk_dt
            existing.ykk_kap_disclosure_id = disclosure_id
            existing.ykk_kap_url = kap_url
        if pct and not existing.percentage:
            existing.percentage = pct
        if amt and not existing.amount_tl:
            existing.amount_tl = amt
        return existing

    if event_type == "spk_approval":
        if not existing:
            # SPK onay var ama YKK kaydi yok — yine de olustur (geriye donuk durumda)
            existing = CapitalIncrease(
                ticker=ticker,
                company_name=company_name,
                type=sa_type,
                percentage=pct,
                amount_tl=amt,
                status="ykk_alindi",
            )
            db.add(existing)
            await db.flush()
        existing.spk_approval_date = spk_dt or (published_at.date() if published_at else None)
        existing.spk_approval_kap_disclosure_id = disclosure_id
        existing.spk_approval_kap_url = kap_url
        if existing.status in ("ykk_alindi",):
            existing.status = "spk_onayli"
        if pct and not existing.percentage:
            existing.percentage = pct
        if amt and not existing.amount_tl:
            existing.amount_tl = amt
        if dist_dt and not existing.distribution_date:
            existing.distribution_date = dist_dt
            existing.status = "tarih_belli"
        logger.info("Capital: SPK onay (%s, %s, dist=%s)", ticker, sa_type, dist_dt)
        return existing

    if event_type == "spk_rejection":
        if not existing:
            existing = CapitalIncrease(
                ticker=ticker,
                company_name=company_name,
                type=sa_type,
                status="reddedildi",
            )
            db.add(existing)
            await db.flush()
        existing.status = "reddedildi"
        existing.rejected_at = datetime.now(timezone.utc)
        existing.rejection_kap_disclosure_id = disclosure_id
        existing.rejection_kap_url = kap_url
        logger.info("Capital: SPK red (%s, %s)", ticker, sa_type)
        return existing

    if event_type == "distribution":
        if not existing:
            existing = CapitalIncrease(
                ticker=ticker,
                company_name=company_name,
                type=sa_type,
                percentage=pct,
                amount_tl=amt,
                status="ykk_alindi",
            )
            db.add(existing)
            await db.flush()
        if dist_dt:
            existing.distribution_date = dist_dt
            existing.distribution_kap_disclosure_id = disclosure_id
            existing.distribution_kap_url = kap_url
            if dist_dt > today:
                existing.status = "tarih_belli"
            elif dist_dt == today:
                existing.status = "dagitiliyor"
            else:
                existing.status = "tamamlandi"
        logger.info("Capital: dagitim tarihi (%s, %s, %s)", ticker, sa_type, dist_dt)
        return existing

    return existing


async def update_distribution_statuses(db: AsyncSession) -> int:
    """Gunluk gorev — distribution_date == bugun olanları 'dagitiliyor', gecmis olanlari 'tamamlandi' yap.

    Returns: guncellenen kayit sayisi
    """
    today = date.today()
    updated = 0

    # tarih_belli durumda olup tarihi gelenler -> dagitiliyor
    stmt = (
        select(CapitalIncrease)
        .where(CapitalIncrease.status == "tarih_belli")
        .where(CapitalIncrease.distribution_date.isnot(None))
    )
    res = await db.execute(stmt)
    for row in res.scalars().all():
        if row.distribution_date == today:
            row.status = "dagitiliyor"
            updated += 1
        elif row.distribution_date < today:
            row.status = "tamamlandi"
            updated += 1

    # dagitiliyor durumunda olup tarihi gecenler -> tamamlandi
    stmt2 = (
        select(CapitalIncrease)
        .where(CapitalIncrease.status == "dagitiliyor")
        .where(CapitalIncrease.distribution_date.isnot(None))
    )
    res2 = await db.execute(stmt2)
    for row in res2.scalars().all():
        if row.distribution_date < today:
            row.status = "tamamlandi"
            updated += 1

    if updated:
        await db.flush()
    return updated


# ═══════════════════════════════════════════════════════════════════
# MKK Bedelsiz/Sermaye Artırım GERÇEKLEŞME duyurusu (regex, AI YOK)
# ═══════════════════════════════════════════════════════════════════
#
# Örnek: https://www.kap.org.tr/tr/Bildirim/1600267
# Title : "Merkezi Kayıt Kuruluşu A.Ş. Duyurusu"
# Body  : "SÜMER VARLIK YÖNETİM A.Ş. 'nin 29.04.2026 tarihinde başlayan
#          %408,47457 oranındaki bedelsiz sermaye artırım işleminde,
#          kaydileşmiş pay senetlerinin artırım karşılıkları ilgili üyelerin
#          müşteri alt hesaplarına 04.05.2026 tarihinde alacak kaydedilmiştir."
#
# Bu bildirim → ilgili capital_increase kaydını 'tamamlandi' yap.

_MKK_REALIZATION_RE = re.compile(
    r"%\s*([\d.,]+)\s*oran(?:ı|i)ndaki\s+(bedelsiz|bedelli|tahsisli)\s+sermaye\s+art(?:ı|i)r(?:ı|i)m"
    r".{0,400}?"  # arada metin
    r"(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4})\s+tarihinde\s+alacak\s+kaydedil",
    re.IGNORECASE | re.DOTALL,
)


def parse_mkk_capital_realization(body: str) -> Optional[dict[str, Any]]:
    """MKK bedelsiz gerçekleşme duyurusunu regex ile parse et."""
    if not body:
        return None
    m = _MKK_REALIZATION_RE.search(body)
    if not m:
        return None

    raw_pct = m.group(1).replace(".", "").replace(",", ".")
    try:
        pct = float(raw_pct)
    except ValueError:
        pct = None

    issuance_type = m.group(2).lower()
    realization_date_str = m.group(3)
    real_date = None
    for fmt in ("%d.%m.%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            real_date = datetime.strptime(realization_date_str, fmt).date()
            break
        except ValueError:
            continue

    return {
        "percentage": pct,
        "issuance_type": issuance_type,
        "realization_date": real_date,
    }


def is_mkk_capital_realization(title: str, body: str) -> bool:
    """MKK gerçekleşme duyurusu mu?"""
    if not body:
        return False
    return bool(_MKK_REALIZATION_RE.search(body))


async def process_mkk_capital_realization(
    db: AsyncSession,
    *,
    ticker_hint: Optional[str],
    body: str,
    kap_url: Optional[str],
    disclosure_id: Optional[int],
) -> dict[str, Any]:
    """MKK gerçekleşme bildirimini işle: ilgili capital_increase kaydı 'tamamlandi'.

    ticker_hint: KAP başlığındaki ticker (varsa)
    """
    parsed = parse_mkk_capital_realization(body or "")
    if not parsed:
        return {"matched": False, "reason": "regex_no_match"}

    # Ticker bul: hint > body'den çıkar
    target_ticker = (ticker_hint or "").upper().strip()
    if not target_ticker:
        # Body'de İlgili Şirketler [SMRVA] gibi
        rel_match = re.search(r"İlgili\s+Şirketler[^\[]*\[([^\]]+)\]", body)
        if rel_match:
            tickers = [t.strip() for t in rel_match.group(1).split(",") if t.strip()]
            if tickers:
                target_ticker = tickers[0].upper()

    if not target_ticker:
        return {"matched": False, "reason": "no_ticker"}

    pct = parsed.get("percentage")
    real_date = parsed.get("realization_date")
    issuance_type = parsed.get("issuance_type")

    # En yakın eşleşen capital_increase kaydını bul (ticker + tip + tarih_belli/dagitiliyor)
    stmt = (
        select(CapitalIncrease)
        .where(CapitalIncrease.ticker == target_ticker)
        .where(CapitalIncrease.status.in_(["tarih_belli", "dagitiliyor", "spk_onayli", "ykk_alindi"]))
        .order_by(CapitalIncrease.distribution_date.desc().nullslast())
        .limit(5)
    )
    rows = (await db.execute(stmt)).scalars().all()

    target = None
    if rows and pct is not None:
        # Yüzde match (±%2)
        for r in rows:
            if r.percentage and abs(r.percentage - pct) < 2.0:
                target = r
                break
    if target is None and rows:
        target = rows[0]

    if target is None:
        return {"matched": False, "reason": "no_capital_increase_row", "ticker": target_ticker}

    target.status = "tamamlandi"
    if real_date and (not target.distribution_date or target.distribution_date != real_date):
        target.distribution_date = real_date
    if kap_url:
        # Distribution KAP url alanı yoksa sadece logla
        pass
    await db.flush()
    logger.info("MKK Realization: %s tamamlandi (%%%s, %s)", target_ticker, pct, real_date)
    return {
        "matched": True,
        "ticker": target_ticker,
        "percentage": pct,
        "realization_date": str(real_date) if real_date else None,
        "issuance_type": issuance_type,
        "capital_increase_id": target.id,
    }
