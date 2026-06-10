"""Pay Alım Satım — KAP body AI parse → ShareTransactionDetail."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.share_transaction_detail import ShareTransactionDetail
from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)

_TITLE_PATTERNS = [
    # KAP gerçek başlıkları (production DB analizinden)
    "pay alım satım bildirimi", "pay alim satim bildirimi",       # 41 kayit son 30 gun
    "pay alım satım", "pay alim satim",
    # NOT: "geri alın" / "payların geri alın" KALDIRILDI — bunlar `buyback_processor`
    # tarafından işleniyor ve şirketin kendi paylarını geri alımı (farklı kategori).
    # Önceki çakışma duplicate kayıt yaratıyordu (hem buybacks hem
    # share_transaction_details tablosuna yazılıyordu).
    "pay alımı", "pay alimi",
    "pay satışı", "pay satisi",
    "önemli paydaş", "onemli paydas",
]

# Body içinde aranacak pay alım satım sinyalleri
# Multi-symbol bulk duyurularda title generic olabilir ("Kamuyu Aydınlatma")
# ama body'de Pay Alım Satım kalıbı geçer.
_BODY_PATTERNS = [
    "pay alım satım bildirimi", "pay alim satim bildirimi",
    "alım nominal", "satım nominal", "alim nominal", "satim nominal",
    "günü içinde",  # KAP standart pay alım satım açıklama kalıbı
    "fiyat aralığından", "fiyat aralıgindan",
    "pay başına ortalama fiyat", "pay basina ortalama fiyat",
    "oy hakkı oranı", "oy hakki orani",
    "pay oranı", "pay orani",
]


def is_share_transaction(title: str, body: str = "") -> bool:
    """Pay Alım Satım Bildirimi mi?

    Title'da kalıp varsa direkt True. Title generic ise ("Kamuyu Aydınlatma
    Platformu Duyurusu") body'de pay alım satım kalıbı arar — multi-symbol
    bulk duyurular için.
    """
    if title:
        t = lower_tr(title)
        if any(p in t for p in _TITLE_PATTERNS):
            return True
    # Title yetersiz — body'de ara
    if body:
        b = lower_tr(body)
        # En az 2 farklı body sinyali olmalı (yanlış pozitif önle)
        matches = sum(1 for p in _BODY_PATTERNS if p in b)
        if matches >= 2:
            return True
    return False


_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_AI_TIMEOUT = 30


def _get_gemini_key() -> Optional[str]:
    try:
        from app.config import get_settings
        return get_settings().GEMINI_API_KEY or None
    except Exception:
        return None


_PROMPT = """Asagidaki KAP pay alim satim bildirimini analiz et ve YAPILANDIRILMIS JSON dondur.

KAP BILDIRIMI:
Hisse: {ticker}
Baslik: {title}
Icerik:
{body}

Donen JSON sablonu (eksikler null):
{{
  "transaction_type": "alici" | "satici",
  "transaction_date": "YYYY-MM-DD",
  "party_name": "Alan/satan kisi veya sirket",
  "party_role": "Görev (ornegin Yonetim Kurulu Baskani veya Vice President)",
  "price_low": <sayi>,
  "price_high": <sayi> (aralik ust degeri),
  "nominal_lot": <int>,
  "oy_hakki_pct": <yuzde sayi>,
  "oy_hakki_change_pct": <degisim yuzde, +/->,
  "pay_orani_pct": <yuzde>,
  "pay_orani_change_pct": <degisim>
}}

KURALLAR:
- SADECE JSON dondur.
- Bilinmeyenler null. Tahmin etme.
- transaction_type: pay alanlar icin "alici", satanlar icin "satici".
"""


async def ai_parse(ticker: str, title: str, body: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    key = _get_gemini_key()
    if not key or not body:
        return out
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as c:
            r = await c.post(
                _GEMINI_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Yapilandirilmis JSON dondur. SADECE JSON."},
                        {"role": "user", "content": _PROMPT.format(ticker=ticker, title=title or "", body=(body or "")[:3500])},
                    ],
                    "temperature": 0.1, "max_tokens": 1024,
                },
            )
            if r.status_code == 200:
                txt = r.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                p = _parse_json(txt)
                if p:
                    if p.get("transaction_type") in ("alici", "satici"):
                        out["transaction_type"] = p["transaction_type"]
                    if isinstance(p.get("transaction_date"), str):
                        try:
                            out["transaction_date"] = date.fromisoformat(p["transaction_date"])
                        except ValueError:
                            pass
                    for k in ("party_name", "party_role"):
                        v = p.get(k)
                        if isinstance(v, str) and v.strip():
                            out[k] = v.strip()[:255]
                    for k in ("price_low", "price_high", "oy_hakki_pct", "oy_hakki_change_pct", "pay_orani_pct", "pay_orani_change_pct"):
                        v = p.get(k)
                        if isinstance(v, (int, float)):
                            out[k] = float(v)
                    nl = p.get("nominal_lot")
                    if isinstance(nl, (int, float)):
                        out["nominal_lot"] = int(nl)
    except Exception as e:
        logger.warning("ShareTx AI hata: %s", e)
    return out


def _parse_json(text: str) -> Optional[dict]:
    if not text:
        return None
    if "```" in text:
        text = re.sub(r"```(?:json)?\s*", "", text).replace("```", "")
    s, e = text.find("{"), text.rfind("}")
    if s < 0 or e < 0:
        return None
    try:
        return json.loads(text[s:e + 1])
    except json.JSONDecodeError:
        return None


async def _fetch_attachment_text(kap_url: Optional[str]) -> str:
    """Detayı EKTE olan 'Pay Alım Satım Bildirimi'nde ek PDF'i indirip metin çıkarır.

    KAP body'si çoğu zaman sadece kapak notudur ("...açıklama ekte yer almaktadır");
    asıl işlem (alış/satış, nominal, fiyat, oran) ekteki PDF'dedir. Bu fonksiyon:
      - pdf_links'i (kap_disclosure_extractor zaten yakalıyor) indirir,
      - KAP ek dosyaları Java-serialization wrapper içinde gelebilir → %PDF offset'iyle atlar,
      - indirme /tr/api + Referer gerektirir,
      - pdfplumber ile metin çıkarır.
    """
    if not kap_url:
        return ""
    try:
        import io
        import httpx
        import pdfplumber
        from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
        d = await fetch_kap_disclosure(kap_url)
        links = (d or {}).get("pdf_links") or []
        if not links:
            return ""
        hdr = {"User-Agent": "Mozilla/5.0", "Referer": kap_url}
        out: list[str] = []
        async with httpx.AsyncClient(timeout=30, headers=hdr, follow_redirects=True) as c:
            for url in links:
                u = url.replace("://www.kap.org.tr/api/", "://www.kap.org.tr/tr/api/")
                try:
                    r = await c.get(u)
                    b = r.content or b""
                    pi = b.find(b"%PDF")
                    if pi < 0:
                        continue
                    with pdfplumber.open(io.BytesIO(b[pi:])) as pdf:
                        t = "\n".join((p.extract_text() or "") for p in pdf.pages)
                    if t and len(t.strip()) > 50:
                        out.append(t)
                except Exception:
                    continue
        return "\n".join(out)
    except Exception as e:
        logger.warning("ShareTx ek PDF metin hata (%s): %s", kap_url, e)
        return ""


# ════════════════════════════════════════════════════════════════════════════
#  MKK "Özel Durumlar Tebliği 12-(4)" TOPLU pay sahipliği bildirimi
# ────────────────────────────────────────────────────────────────────────────
#  Bu bildirim TEK disclosure içinde BİRDEN FAZLA şirketin (issuer) pay
#  sahipliği değişimini tablo halinde verir. Detay sadece ek PDF'tedir.
#  Tablo: Ortak (taraf) | İhraççı Şirket (=ticker) | T-1 nominal | T nominal | %.
#  Yön nominal değişiminden çıkar (T > T-1 → alıcı, küçük → satıcı).
#
#  Neden ayrı dal:
#   - Tekil "Pay Alım Satım Bildirimi" akışı tek ticker varsayar.
#   - Buradaki bazı işlemler SADECE bu toplu bildirimde geçer (tekil bildirim
#     yapılmamış) → atlanırsa KAÇIRILIR.
#   - Bazıları tekil bildirimle de gelir → DUPLICATE olmamalı (dedup şart).
#   - Haber tipi STANDART kalır: feed skoru override edilmez, sadece Pay Alım
#     Satım listesine yapısal kayıt düşülür.
# ════════════════════════════════════════════════════════════════════════════

_MKK_124_SIGNALS = (
    "12-(4)", "12 - (4)", "12-(4).", "ozel durumlar tebligi", "özel durumlar tebliği",
)


def is_mkk_share_disclosure(title: str, body: str = "") -> bool:
    """MKK Özel Durumlar Tebliği 12-(4) toplu pay sahipliği bildirimi mi?

    Başlık/özet: "...Özel Durumlar Tebliği'nin 12-(4). maddesi gereğince yapılan
    açıklama". MKK tarafından yayınlanır, detay ekte tablo olarak gelir.
    """
    blob = lower_tr((title or "") + " " + (body or ""))
    has_124 = ("12-(4)" in blob) or ("12 - (4)" in blob) or ("12-(4)." in blob)
    has_teblig = "ozel durumlar tebligi" in blob or "özel durumlar tebliği" in blob
    return has_124 and has_teblig


def _extract_related_tickers(text: str) -> list[str]:
    """Body/PDF metninden 'İlgili Şirketler [CELHA, GUNDG, ...]' ticker listesini çıkar."""
    if not text:
        return []
    out: list[str] = []
    for m in re.finditer(r"\[([A-Z0-9]{2,6}(?:\s*,\s*[A-Z0-9]{2,6})*)\]", text):
        for tk in m.group(1).split(","):
            tk = tk.strip().upper()
            if 2 <= len(tk) <= 6 and tk.isalnum() and tk not in out:
                out.append(tk)
    return out


_MKK_PROMPT = """Asagida bir MKK 'Ozel Durumlar Tebligi 12-(4)' pay sahipligi degisim tablosunun metni var.
Her satir: Ortagin Adi/Unvani (taraf) | Ihracci Sirket (issuer) | T-1 gunu nominal (TL) | T gunu nominal (TL) | T-1 pay % | T pay %.

Ilgili ticker listesi (issuer sirket adini buna eslestir): {tickers}

Her ISLEM icin (T-1 nominal != T nominal olanlar) JSON satiri uret. Nominal DEGISMEYEN
(sadece sermaye artisi/sulanma kaynakli pay% degisimi) satirlari ATLA.

Donen JSON: {{"transactions": [
  {{"ticker": "<issuer ticker, SADECE listeden>", "party_name": "<ortak/taraf adi>",
    "transaction_type": "alici" | "satici",
    "transaction_date": "YYYY-MM-DD",
    "t1_nominal": <sayi>, "t_nominal": <sayi>,
    "nominal_lot": <abs(t_nominal - t1_nominal) tamsayi>,
    "pay_orani_pct": <T gunu pay %>}}
]}}

KURALLAR:
- transaction_type: T nominal > T-1 nominal ise "alici", kucukse "satici".
- issuer ticker'i SADECE verilen listeden sec. Eslestiremezsen o satiri ATLA.
- Taraf (ortak) adini ticker'a karistirma; ticker = IHRACCI sirket.
- Nominal ayni ise atla. SADECE JSON dondur.

TABLO METNI:
{body}
"""


async def _ai_parse_mkk_table(text: str, tickers: list[str]) -> list[dict[str, Any]]:
    """MKK 12(4) tablosunu AI ile satir satir yapilandir."""
    key = _get_gemini_key()
    if not key or not text:
        return []
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT + 15) as c:
            r = await c.post(
                _GEMINI_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Yapilandirilmis JSON dondur. SADECE JSON."},
                        {"role": "user", "content": _MKK_PROMPT.format(tickers=tickers, body=(text or "")[:6500])},
                    ],
                    "temperature": 0.1, "max_tokens": 8192, "reasoning_effort": "none",
                },
            )
            if r.status_code != 200:
                logger.warning("MKK 12(4) AI status %s", r.status_code)
                return []
            txt = r.json().get("choices", [{}])[0].get("message", {}).get("content", "")
            p = _parse_json(txt)
            if not p:
                return []
            rows = p.get("transactions") if isinstance(p, dict) else None
            return rows if isinstance(rows, list) else []
    except Exception as e:
        logger.warning("MKK 12(4) AI parse hata: %s", e)
        return []


def _norm_party(s: Optional[str]) -> str:
    """Taraf adini dedup icin normalize: harf/rakam disini at, kucult, ilk 14 kar."""
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", lower_tr(s))[:14]


async def process_mkk_share_batch(
    db: AsyncSession, *, disclosure_id: int, title: str, body: Optional[str],
    kap_url: Optional[str], published_at: Optional[datetime],
) -> int:
    """MKK 12(4) toplu bildirimini parse et → Pay Alım Satım'a dedup'li kaydet.

    Döner: eklenen yeni kayıt sayısı.
    """
    from datetime import timedelta

    # ── Bu disclosure daha önce işlendi mi? (tüm satırlar aynı kap_disclosure_id) ──
    if disclosure_id:
        seen = await db.execute(
            select(ShareTransactionDetail.id)
            .where(ShareTransactionDetail.kap_disclosure_id == disclosure_id)
            .where(ShareTransactionDetail.source == "kap_mkk_12_4")
            .limit(1)
        )
        if seen.scalar_one_or_none():
            logger.info("MKK 12(4) zaten işlenmiş (disclosure=%s), skip", disclosure_id)
            return 0

    # ── Tablo ekte: ek PDF metnini çek ──
    text = await _fetch_attachment_text(kap_url)
    if not text or len(text) < 100:
        # Bazı durumlarda body'nin kendisi tabloyu içerebilir
        text = (body or "") + "\n" + (text or "")
    if len(text.strip()) < 100:
        logger.info("MKK 12(4): tablo metni yok (%s)", kap_url)
        return 0

    tickers = _extract_related_tickers(text) or _extract_related_tickers(body or "")
    rows = await _ai_parse_mkk_table(text, tickers)
    if not rows:
        logger.info("MKK 12(4): AI satır çıkaramadı (%s)", kap_url)
        return 0

    added = 0
    for row in rows:
        try:
            tk = (row.get("ticker") or "").strip().upper()
            if not tk or (tickers and tk not in tickers):
                continue
            ttype = row.get("transaction_type")
            if ttype not in ("alici", "satici"):
                continue
            # transaction_date
            tx_date = None
            ds = row.get("transaction_date")
            if isinstance(ds, str):
                try:
                    tx_date = date.fromisoformat(ds)
                except ValueError:
                    tx_date = None
            if tx_date is None:
                tx_date = published_at.date() if published_at else date.today()
            party = (row.get("party_name") or "?")[:255]
            nom = row.get("nominal_lot")
            nom = int(nom) if isinstance(nom, (int, float)) else None
            # nominal değişimi yoksa (işlem yok) atla
            if nom is not None and nom == 0:
                continue
            pay_pct = row.get("pay_orani_pct")
            pay_pct = float(pay_pct) if isinstance(pay_pct, (int, float)) else None

            # ── DEDUP ── aynı ticker + aynı taraf-prefix + tarih(±3g) varsa atla
            lo = tx_date - timedelta(days=3)
            hi = tx_date + timedelta(days=3)
            existing = await db.execute(
                select(ShareTransactionDetail)
                .where(ShareTransactionDetail.ticker == tk)
                .where(ShareTransactionDetail.transaction_date >= lo)
                .where(ShareTransactionDetail.transaction_date <= hi)
            )
            dup = False
            np = _norm_party(party)
            for ex in existing.scalars():
                if np and _norm_party(ex.party_name) and (
                    np[:8] == _norm_party(ex.party_name)[:8]
                ):
                    dup = True
                    break
            if dup:
                logger.info("MKK 12(4) dedup skip: %s / %s / %s", tk, party[:24], tx_date)
                continue

            db.add(ShareTransactionDetail(
                ticker=tk,
                company_name=None,
                transaction_date=tx_date,
                transaction_type=ttype,
                party_name=party,
                party_role=None,
                price_low=None, price_high=None,
                nominal_lot=nom,
                oy_hakki_pct=None, oy_hakki_change_pct=None,
                pay_orani_pct=pay_pct, pay_orani_change_pct=None,
                kap_disclosure_id=disclosure_id,
                kap_url=kap_url,
                source="kap_mkk_12_4",
                raw_excerpt=f"MKK 12(4) toplu bildirim · {ttype} · {nom} TL nominal",
            ))
            added += 1
            logger.info("MKK 12(4) yeni: %s / %s / %s / %s TL", tk, ttype, party[:24], nom)
        except Exception as _re:
            logger.debug("MKK 12(4) satır hata: %s", _re)
            continue

    if added:
        await db.flush()
    logger.info("MKK 12(4) tamam (%s): %d yeni, %d satır", kap_url, added, len(rows))
    return added


async def process_kap_disclosure(
    db: AsyncSession, *, disclosure_id: int, ticker: str, company_name: Optional[str],
    title: str, body: Optional[str], kap_url: Optional[str], published_at: Optional[datetime],
) -> Optional[ShareTransactionDetail]:
    if not is_share_transaction(title):
        return None

    # Mevcut KAP id ile kayit varsa skip
    if disclosure_id:
        stmt = select(ShareTransactionDetail).where(ShareTransactionDetail.kap_disclosure_id == disclosure_id).limit(1)
        if (await db.execute(stmt)).scalar_one_or_none():
            return None

    # ── DETAY EKTE Mİ? ── KAP body kapak notuysa (işlem detayı ekteki PDF'de) ek PDF'i
    # indirip metnini kullan. Aksi halde alış/satış/nominal/fiyat çıkarılamaz (KGYO/Orhun
    # Kartal vakası: body "açıklama ekte yer almaktadır" der, gerçek satış ekte).
    eff_body = body or ""
    _bl = lower_tr(eff_body)
    _refers_ek = ("ekte yer al" in _bl or "ekte yer aldı" in _bl or "ekte yer aldigi" in _bl)
    _has_detail = any(k in _bl for k in (
        "nominal", "işlem fiyat", "islem fiyat", "satış işlem", "satis islem",
        "alış işlem", "alis islem", "pay oran", "oy hakk",
    ))
    if _refers_ek and not _has_detail and kap_url:
        _ek = await _fetch_attachment_text(kap_url)
        if _ek and len(_ek) > len(eff_body):
            logger.info("ShareTx: detay ekteydi, ek PDF metni çekildi (%s): %d kar", ticker, len(_ek))
            eff_body = _ek

    parsed = await ai_parse(ticker, title, eff_body)

    # transaction_type: AI > body keyword > heuristik (pay_orani_change_pct işareti)
    transaction_type = parsed.get("transaction_type")
    if transaction_type not in ("alici", "satici"):
        # Ek PDF/body içinde işlem yönü ifadesi (SPK formu: "satış işlemi gerçekleştirilmiştir")
        bl = lower_tr(eff_body)
        if "satış işlem" in bl or "satis islem" in bl or "satışı hk" in bl or "elden çıkar" in bl or "elden cikar" in bl:
            transaction_type = "satici"
        elif "alış işlem" in bl or "alis islem" in bl or "alımı hk" in bl or "edinim" in bl or "iktisap" in bl:
            transaction_type = "alici"
        elif "alıcı" in bl or "alici" in bl or "alimi" in bl or "alımı" in bl:
            transaction_type = "alici"
        elif "satıcı" in bl or "satici" in bl or "satışı" in bl or "satisi" in bl:
            transaction_type = "satici"
    if transaction_type not in ("alici", "satici"):
        # Pay/oy oranı artıyorsa alıcı, azalıyorsa satıcı
        pay_chg = parsed.get("pay_orani_change_pct")
        oy_chg = parsed.get("oy_hakki_change_pct")
        chg = pay_chg if isinstance(pay_chg, (int, float)) and pay_chg != 0 else (oy_chg if isinstance(oy_chg, (int, float)) else None)
        if isinstance(chg, (int, float)):
            transaction_type = "alici" if chg > 0 else "satici"
        else:
            transaction_type = "satici"  # son çare

    tx_date = parsed.get("transaction_date") or (published_at.date() if published_at else date.today())
    party_name = parsed.get("party_name") or "?"

    new_row = ShareTransactionDetail(
        ticker=ticker,
        company_name=company_name,
        transaction_date=tx_date,
        transaction_type=transaction_type,
        party_name=party_name,
        party_role=parsed.get("party_role"),
        price_low=parsed.get("price_low"),
        price_high=parsed.get("price_high"),
        nominal_lot=parsed.get("nominal_lot"),
        oy_hakki_pct=parsed.get("oy_hakki_pct"),
        oy_hakki_change_pct=parsed.get("oy_hakki_change_pct"),
        pay_orani_pct=parsed.get("pay_orani_pct"),
        pay_orani_change_pct=parsed.get("pay_orani_change_pct"),
        kap_disclosure_id=disclosure_id,
        kap_url=kap_url,
        source="kap_ai_parse",
    )
    db.add(new_row)
    await db.flush()
    logger.info("ShareTx: yeni (%s, %s, %s)", ticker, transaction_type, party_name[:30])

    # ── FEED SKORU: içeriden alım/satım → DETERMİNİSTİK skor senkronu ──
    # İKİ BUG FIX (TABGD %6 satış 6.8 Hafif Olumlu görünüyordu):
    # 1) Eski kod `transaction_type == "satici"` arıyordu ama kayıtlar "satis"/
    #    "alis" — satış else-dalına düşüp ALIM sayılıyordu.
    # 2) Sadece 4.8-5.2 (nötr) bandı override ediliyordu — AI satışa 6.8 verdiyse
    #    dokunulmuyordu. Artık parse yönü + oran değişimi belliyse skor HER ZAMAN
    #    telegram_poller ile aynı deterministik banda çekilir; AI özeti yön ile
    #    tutarlıysa korunur (rakamlı/iyi AI özetini silmeyiz), skor/etiket düzelir.
    if disclosure_id:
        try:
            from app.models.kap_all_disclosure import KapAllDisclosure
            from app.utils.ai_score_label import score_to_label as _s2l
            disc = await db.get(KapAllDisclosure, disclosure_id)
            _cur = float(disc.ai_impact_score) if (disc and disc.ai_impact_score is not None) else None
            _is_sell = (transaction_type or "").lower().startswith("sat")  # satis/satici/satım
            _chg = parsed.get("pay_orani_change_pct")
            if disc is not None:
                # Hedef skor: oran değişimi varsa poller bantları; yoksa yön bazlı default
                if _chg is not None:
                    _a = abs(float(_chg))
                    if _a < 0.3:
                        _target = 5.0
                    elif _a < 1.0:
                        _target = 4.0 if _is_sell else 6.3
                    elif _a < 3.0:
                        _target = 3.5 if _is_sell else 6.8
                    elif _a < 5.0:
                        _target = 2.8 if _is_sell else 7.3
                    else:
                        _target = 2.3 if _is_sell else 7.8
                else:
                    _target = 4.0 if _is_sell else 6.3
                # Skor yön ile çelişiyorsa veya nötrde kalmışsa hedefe çek.
                # (Yön DOĞRU ve makul banttaysa AI skoruna dokunma.)
                _conflict = (
                    _cur is None
                    or (4.8 <= _cur <= 5.2 and _target != 5.0)
                    or (_is_sell and _cur > 5.2 and _target < 5.0)
                    or ((not _is_sell) and _cur < 4.8 and _target > 5.0)
                )
                if _conflict and abs((_cur or 5.0) - _target) >= 0.1:
                    _old = _cur
                    disc.ai_impact_score = _target
                    disc.ai_sentiment = _s2l(_target) or disc.ai_sentiment
                    logger.info(
                        "ShareTx feed skor senkron (%s): %s -> %.1f (%s, chg=%s)",
                        ticker, _old, _target, transaction_type, _chg,
                    )
                # Özet boş/nötr-kapaksa tam cümleli deterministik özet yaz
                if not (disc.ai_summary or "").strip() or "ekte yer al" in (disc.ai_summary or "").lower():
                    _who = party_name if party_name and party_name != "?" else "Önemli bir pay sahibi"
                    _role = parsed.get("party_role")
                    _who_full = f"{_who}" + (f" ({_role})" if _role else "")
                    _oran = parsed.get("pay_orani_pct")
                    _chg_s = (f"%{abs(float(_chg)):.2f}".replace(".", ",") + " oranında ") if _chg is not None else ""
                    _now_s = (f"; toplam payı %{_oran}".replace(".", ",") + " seviyesine geldi") if _oran is not None else ""
                    if _is_sell:
                        disc.ai_summary = (
                            f"{_who_full}, {ticker} sermayesindeki payını {_chg_s}azalttı{_now_s}. "
                            "İçeriden satış, yatırımcı açısından temkinli bir sinyaldir; ölçeğine göre arz baskısı yaratabilir."
                        )
                    else:
                        disc.ai_summary = (
                            f"{_who_full}, {ticker} sermayesindeki payını {_chg_s}artırdı{_now_s}. "
                            "İçeriden alım, şirkete güven sinyali olarak olumlu değerlendirilir."
                        )
        except Exception as _fe:
            logger.debug("ShareTx feed skor senkron hata (%s): %s", ticker, _fe)

    return new_row
