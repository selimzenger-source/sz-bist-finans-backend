"""İş Anlaşması Processor — KAP'tan AI parse + TRY çevrim (TCMB).

Title patterns + Gemini ile body'den tutar + para birimi çıkarır.
TCMB güncel kur ile TRY'a çevirir.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any, Optional

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.business_deal import BusinessDeal
from app.utils.tr_text import lower_tr

logger = logging.getLogger(__name__)


# ─── Title patterns — iş anlaşması ile ilgili KAP başlıkları ───
_TITLE_PATTERNS = [
    # KAP'ta gerçek başlıklar (production DB analizinden)
    "yeni iş ilişkisi", "yeni is iliskisi",          # 21 kayit son 30 gun
    "ihale süreci", "ihale sonucu", "ihale sonuçland",  # 5+
    "ihale alındı", "ihale alınmış",
    "sözleşme imzalan", "sozlesme imzaland",
    "iş anlaşması", "is anlasmasi",
    "yeni müşteri", "yeni musteri",
    "önemli nitelikteki işlem", "onemli nitelikteki",
    "işbirliği", "isbirligi",
    "tedarik anlaşması", "tedarik sözleşmesi",
    "satış sözleşmesi", "satis sozlesmesi",
]


def is_business_deal(title: str) -> bool:
    if not title:
        return False
    t = lower_tr(title)
    return any(p in t for p in _TITLE_PATTERNS)


# ─── Gemini AI ───
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_AI_TIMEOUT = 30


def _get_gemini_key() -> Optional[str]:
    try:
        from app.config import get_settings
        return get_settings().GEMINI_API_KEY or None
    except Exception:
        return None


_PARSE_PROMPT = """Asagidaki KAP is anlasmasi/sozlesme bildirimini analiz et ve yapilandirilmis JSON dondur.

KAP BILDIRIMI:
Hisse: {ticker}
Baslik: {title}
Icerik:
{body}

Donen JSON sablonu (bilgi yoksa null):
{{
  "amount_original": <sozlesme tutari sayi>,
  "currency": "TRY" | "USD" | "EUR" | "GBP",
  "deal_date": "YYYY-MM-DD",
  "counterparty": "Karsi taraf (musteri/satici) adi",
  "summary": "Kisa Turkce ozet (max 150 char)"
}}

KURALLAR:
- SADECE JSON dondur.
- Tutar yoksa null. KDV hariç tutar tercih edilir.
- Para birimi: TL/Lira/₺ -> TRY, dolar/USD -> USD, vs.
- Tarih bildirimde gecen sozlesme tarihi (rapor tarihi degil).
- Bilinmeyenler null.
"""


_CURRENCY_GROUP = (
    r"(TL|TRY|USD|EUR|GBP|\$|€|£|"
    r"ABD\s*Dolar[ıi]?|Amerikan\s*Dolar[ıi]?|"
    r"T[üu]rk\s*Liras[ıi]|Lira|"
    r"Avro|Euro|Sterlin|Pound)"
)

_AMOUNT_RE = re.compile(
    r"(?:"
    r"İhale\s*bedeli|"
    r"sözleşme\s*bedeli|sözleşme\s*tutarı|sözleşme\s*değeri|"
    r"toplam\s*bedel|toplam\s*tutar|"
    r"işin\s*bedeli|işin\s*tutarı|"
    r"alım\s*bedeli|satım\s*bedeli|"
    r"yatırım\s*tutarı|yatırım\s*bedeli|yatırım\s*maliyeti|yatırım\s*değeri|"
    r"proje\s*bedeli|proje\s*değeri|proje\s*tutarı|"
    r"anlaşma\s*bedeli|anlaşma\s*tutarı|anlaşma\s*değeri|"
    r"iş\s*anlaşması\s*bedeli|"
    r"sipariş\s*bedeli|sipariş\s*tutarı|"
    r"hizmet\s*bedeli|hizmet\s*tutarı|"
    r"satış\s*tutarı|satış\s*bedeli|"
    r"tutarı|bedeli"
    r")"
    r"[\s:]*"
    r"(?:KDV\s*hariç|KDV\s*dahil|net|brüt)?[\s:]*"
    r"([\d]{1,3}(?:[.\s]\d{3})*(?:,\d+)?|[\d]+(?:,\d+)?)"
    r"\s*"
    + _CURRENCY_GROUP,
    re.IGNORECASE,
)

# Geniş fallback: "12.100.000 TL" / "76.650 ABD Doları" / "5,5 milyon Euro"
_AMOUNT_FALLBACK_RE = re.compile(
    r"([\d]{1,3}(?:[.\s]\d{3})+(?:,\d+)?|[\d]+(?:,\d+)?)\s*"
    + _CURRENCY_GROUP,
    re.IGNORECASE,
)

# Action-coupled pattern: "X TL/USD/Doları satış/sözleşme/ihracat/anlaşma"
_AMOUNT_ACTION_RE = re.compile(
    r"([\d]{1,3}(?:[.\s]\d{3})*(?:,\d+)?|[\d]+(?:[.,]\d+)?)\s*"
    + _CURRENCY_GROUP +
    r"\s*(?:tutar|değer|bedel|sat[ıi][şs]|al[ıi][mn]|ihrac|s[oö]zle[şs]me|anla[şs]ma|i[şs]lem|ihale|gelir)",
    re.IGNORECASE,
)

# Çarpan kelimeleri ("milyon", "milyar")
_MULTIPLIER_RE = re.compile(
    r"([\d]+(?:[.,]\d+)?)\s*(milyon|milyar|trilyon|bin)\s*"
    + _CURRENCY_GROUP,
    re.IGNORECASE,
)


def _normalize_currency(raw: str) -> str:
    if not raw:
        return "TRY"
    r = raw.upper().replace(" ", "").replace(".", "")
    if r in ("TL", "TRY", "TÜRKLIRASI", "TÜRKLİRASI", "TURKLIRASI", "₺") or "LIRA" in r or "LİRA" in r:
        return "TRY"
    if r in ("USD", "$", "AMERIKANDOLARI") or "DOLAR" in r:
        return "USD"
    if r in ("EUR", "€", "AVRO", "EURO"):
        return "EUR"
    if r in ("GBP", "£") or "STERLİN" in r or "STERLIN" in r:
        return "GBP"
    return "TRY"


def _parse_tr_number(raw: str, is_multiplier_base: bool = False) -> Optional[float]:
    """Türkçe sayı parser.

    Format ayrımı (KRİTİK — eski versiyon "18.8" → 188 bug'i için düzeltildi):
    - "12.100.000,50" → 12100000.50 (Türkçe full format, virgül = ondalık)
    - "12.100.000"    → 12100000    (3'lü grup → binlik ayraç)
    - "18.8"          → 18.8        (3 haneden farklı → ondalık)
    - "0.95"          → 0.95        (ondalık)
    - "3,5"           → 3.5         (virgül = ondalık)

    is_multiplier_base=True (çarpanlı "X milyon/milyar" tabanı):
    - "20.528 milyon" → 20.528 (ondalık) → 20.5 milyon. TEK noktalı sayı her
      zaman ondalıktır; "20528 milyon" yazımı "20.528 milyon" olmaz.
    - Önceki bug: "20.528 milyon EUR" → 20528×milyon = 20.528 MİLYAR EUR →
      1097.9 milyar TL (gerçek ~20.5M EUR ≈ 1.1 milyar TL). YEOTK kaydı.

    Önceki bug: "18.8" → noktayı binlik sayıp `188.0` döndürüyordu.
    Sonuç: ASELS 188 milyar TL (gerçek 18.8 milyar), LINK 402 mn (gerçek 40 mn),
    ONCSM 366 mn (gerçek 36.6 mn) kayıtlarında 10× hata oluştu.
    """
    if not raw:
        return None
    s = raw.strip().replace(" ", "")
    # 12.100.000,50 → 12100000.50 (Türkçe full)
    if "," in s:
        int_part, dec_part = s.rsplit(",", 1)
        int_part = int_part.replace(".", "")
        try:
            return float(f"{int_part}.{dec_part}")
        except ValueError:
            return None
    # Sadece nokta var
    if "." in s:
        parts = s.split(".")
        # Çarpan tabanı ("X milyon/milyar") + TEK nokta → her zaman ondalık.
        # "20.528 milyon" = 20.528 milyon (20.5M), binlik (20528) DEĞİL.
        if is_multiplier_base and len(parts) == 2:
            try:
                return float(s)
            except ValueError:
                return None
        # Tüm "nokta sonrası" grupları tam 3 hane ise → binlik ayraç ("12.100.000")
        if len(parts) >= 2 and all(len(p) == 3 and p.isdigit() for p in parts[1:]):
            try:
                return float(s.replace(".", ""))
            except ValueError:
                return None
        # Aksi takdirde nokta = ondalık ayraç ("18.8", "0.95", "3.14")
        try:
            return float(s)
        except ValueError:
            return None
    # Hiç ayraç yok — düz sayı
    try:
        return float(s)
    except ValueError:
        return None


def regex_extract_business_deal(body: str) -> dict[str, Any]:
    """AI ÇAĞIRMADAN body'den miktar+para birimi+karşı taraf çıkar.

    KAP iş anlaşması bildirimleri ÇOĞUNLUKLA "İhale bedeli KDV hariç X TL'dir" gibi
    sabit kalıplar içerir. Bu fonksiyon onları regex ile yakalar.
    """
    out: dict[str, Any] = {
        "amount_original": None, "currency": None,
        "counterparty": None, "summary": None, "deal_date": None,
    }
    if not body:
        return out

    # ★ SIRALAMA ONEMLI: Yapilandirilmis ("ihale bedeli: ... TL") EN ONCE.
    # Multiplier ("X milyar TL") en son — cunku body'de gecen acklama metni
    # ("yaklasik 177 milyar TL'lik proje") yanlis yakalanabilir.
    amount = None
    currency = None

    # 1) Yapılandırılmış: "ihale bedeli ... 12.100.000 TL" — EN GUVENILIR
    m = _AMOUNT_RE.search(body)
    if m:
        a = _parse_tr_number(m.group(1))
        if a:
            amount, currency = a, _normalize_currency(m.group(2))

    # 2) Action-coupled: "76.650 ABD Doları satışı/ihracatı"
    if amount is None:
        m = _AMOUNT_ACTION_RE.search(body)
        if m:
            a = _parse_tr_number(m.group(1))
            if a:
                amount, currency = a, _normalize_currency(m.group(2))

    # 3) Çarpanlı pattern: "5,5 milyon Euro" — orta guvenirlik
    if amount is None:
        m = _MULTIPLIER_RE.search(body)
        if m:
            base = _parse_tr_number(m.group(1), is_multiplier_base=True)
            mult_word = m.group(2).lower()
            mult = {"bin": 1_000, "milyon": 1_000_000, "milyar": 1_000_000_000, "trilyon": 1_000_000_000_000}.get(mult_word, 1)
            if base:
                amount = base * mult
                currency = _normalize_currency(m.group(3))

    # 4) En geniş fallback: "X TL/Doları" cümle içinde — son care
    if amount is None:
        m = _AMOUNT_FALLBACK_RE.search(body)
        if m:
            a = _parse_tr_number(m.group(1))
            if a:
                amount, currency = a, _normalize_currency(m.group(2))

    if amount and amount >= 1000:  # 1000 birim altı şüpheli
        out["amount_original"] = amount
        out["currency"] = currency

    # Karşı taraf
    # 1) KAP form template: "Müşterinin/Tedarikçinin Adı Soyadı/Ticaret Ünvanı  X"
    #    Value satir sonunda VEYA bir sonraki satirda olabilir (cok satirli tablo)
    cp_clean = None
    cp = re.search(
        r"(?:Müşterinin|Tedarikçinin|Karşı\s*Taraf|Alıcının|Satıcının|İlgili\s*Tarafın)[^\n:]*?(?:Ünvan[ıi]|Adı|Taraf)[^\n:]*?[:\s]+([^\n\r]{3,200}?)(?:\n|Varsa|İş İlişkisinin|Bağlantılı|$)",
        body, re.IGNORECASE,
    )
    if cp:
        cp_clean = cp.group(1).strip().rstrip(",.").strip()
    # 1b) "Yurtdışı Müşteri" ise gercek ulke: a) "Hangi Ülke" alani b) Baslik/Ozet'te "-X Pazari/Ülkesi"
    if cp_clean and re.search(r"yurtd[ıi]ş[ıi]\s*(?:müşteri|tedarikçi)", cp_clean, re.IGNORECASE):
        country = None
        country_m = re.search(
            r"(?:Yurtd[ıi]ş[ıi].*?Hangi\s*Ülke|Hangi\s*Ülke|Ülke(?:si)?)[^\n:]*?[:\s]+([A-ZÇĞİÖŞÜA-Za-zÇĞİÖŞÜçğıöşü][^\n\r]{2,60})",
            body, re.IGNORECASE,
        )
        if country_m:
            cand = country_m.group(1).strip().rstrip(",.")
            if cand and cand.lower() not in ("evet", "hayır", "hayir", "-", "yok"):
                country = cand
        if not country:
            # Baslik/ilk satir: "Yeni İş İlişkisi -Japonya Pazarı"
            title_m = re.search(r"-\s*([A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü]{2,30})\s*(?:Pazar[ıi]|Ülkesi|Müşteris[ıi])", body[:500])
            if title_m:
                country = title_m.group(1).strip()
        if country:
            cp_clean = country
    # 2) Body'den: "Şirketimiz ile X arasında" / "X ile yapılan/imzalanan"
    if not cp_clean or len(cp_clean) < 4:
        m = re.search(r"[Şş]irketimiz\s+ile\s+([A-ZÇĞİÖŞÜ][^\s,]{2,80}?(?:\s+[A-ZÇĞİÖŞÜ][^\s,]{2,40}){0,5})\s+aras[ıi]nda", body)
        if m:
            cp_clean = m.group(1).strip()
    if not cp_clean:
        m = re.search(r"([A-ZÇĞİÖŞÜ][A-ZÇĞİÖŞÜ\s]{2,40})\s+(?:ile|firmasi|firması|şirketi|sirketi)\s+(?:imzalan|yapılan|yapilan|anla[şs]ma)", body)
        if m:
            cp_clean = m.group(1).strip()
    # 3) Ülke ihracat: "JAPONYA ülkesine yapacağı"
    if not cp_clean:
        m = re.search(r'"?\s*([A-ZÇĞİÖŞÜ]{3,40})\s*"?\s*[üu]lkesine', body)
        if m:
            cp_clean = m.group(1).strip()
    # 4) Generic büyük harfli ifade: "ile X imzalandı/yapıldı"
    if not cp_clean:
        m = re.search(r"ile\s+([A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü\s\.\-&]{2,80}?)\s+(?:aras[ıi]nda|firmas[ıi]|şirketi|sirketi|ile)", body)
        if m:
            cp_clean = m.group(1).strip()
    # 5) Özet kalıbı: "TICKER, X A.Ş./Ltd ile ..." (CWENE summary gibi)
    if not cp_clean:
        m = re.search(
            r"(?:^|[,\.]\s)([A-ZÇĞİÖŞÜ][A-Za-zÇĞİÖŞÜçğıöşü\s\.\-&]{3,80}?(?:A\.\s*Ş\.?|AŞ\.?|Ltd\.?|Holding|Şti\.?))\s+ile\b",
            body,
        )
        if m:
            cp_clean = m.group(1).strip().rstrip(",.")
    if cp_clean and 3 < len(cp_clean) < 250:
        out["counterparty"] = cp_clean

    # Özet — ilk anlamlı paragraf
    first_para = body.strip().split("\n")[0][:300]
    if len(first_para) > 30:
        out["summary"] = first_para

    return out


async def ai_parse_business_deal(ticker: str, title: str, body: str) -> dict[str, Any]:
    """KAP body'sinden iş anlaşması yapılandırılmış veri çıkar.

    Önce regex (deterministik). AI sadece eksik kalan alanlar için fallback.
    """
    # ÖNCELİK 1: Regex (deterministik, ~%95 standart kalipi yakalar)
    out = regex_extract_business_deal(body or "")
    # Tum alanlar dolu ise AI'a gerek yok
    if out.get("amount_original") and out.get("currency") and out.get("counterparty") and out.get("summary"):
        logger.info("BusinessDeal regex tam parse: %s %s cp=%s",
                    out["amount_original"], out["currency"], (out.get("counterparty") or "")[:30])
        return out

    # ÖNCELİK 2: AI fallback — eksik alanlari tamamlar (regex ne bulduysa korunur)
    gemini_key = _get_gemini_key()
    if not gemini_key or not body:
        return out
    prompt = _PARSE_PROMPT.format(ticker=ticker, title=title or "", body=(body or "")[:30000])
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _GEMINI_URL,
                headers={"Authorization": f"Bearer {gemini_key}", "Content-Type": "application/json"},
                json={
                    "model": _GEMINI_MODEL,
                    "messages": [
                        {"role": "system", "content": "Sen finansal verileri yapilandirilmis JSON'a ceviren bir analizcisin. SADECE JSON dondur."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.1, "max_tokens": 1024,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                parsed = _parse_ai_json(content.strip()) if content else None
                if parsed:
                    # AI sonucu sadece regex bulamadigi alanlara yaz (regex'i ezmez)
                    if not out.get("amount_original") and isinstance(parsed.get("amount_original"), (int, float)):
                        out["amount_original"] = float(parsed["amount_original"])
                    if not out.get("currency"):
                        cur = parsed.get("currency")
                        if isinstance(cur, str) and cur.upper() in ("TRY", "USD", "EUR", "GBP"):
                            out["currency"] = cur.upper()
                    if not out.get("deal_date") and isinstance(parsed.get("deal_date"), str):
                        try:
                            out["deal_date"] = date.fromisoformat(parsed["deal_date"])
                        except ValueError:
                            pass
                    if not out.get("counterparty"):
                        cp = parsed.get("counterparty")
                        if isinstance(cp, str):
                            out["counterparty"] = cp[:500]
                    if not out.get("summary"):
                        s = parsed.get("summary")
                        if isinstance(s, str):
                            out["summary"] = s[:300]
    except Exception as e:
        logger.warning("BusinessDeal AI hata: %s", e)
    return out


def _parse_ai_json(text: str) -> Optional[dict[str, Any]]:
    if not text:
        return None
    if "```" in text:
        text = re.sub(r"```(?:json)?\s*", "", text)
        text = text.replace("```", "")
    s = text.find("{")
    e = text.rfind("}")
    if s < 0 or e < 0 or e < s:
        return None
    try:
        return json.loads(text[s:e + 1])
    except json.JSONDecodeError:
        return None


# ─── Anlik kur servisi (ucretsiz, auth'suz) ───
# Geçmiş işlem için bile bugünkü kur kullanılır (kullanıcı tercihi).
# Birincil: exchangerate.host (ECB tabanlı). Yedek: Frankfurter (ECB).
_RATES_CACHE: dict[str, tuple[float, datetime]] = {}
_RATES_TTL = 6 * 3600  # 6 saat


async def _fetch_from_exchangerate_host(currency: str) -> Optional[float]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.exchangerate.host/latest",
                params={"base": currency, "symbols": "TRY"},
            )
            if r.status_code == 200:
                data = r.json()
                rate = data.get("rates", {}).get("TRY")
                if isinstance(rate, (int, float)) and rate > 0:
                    return float(rate)
    except Exception as e:
        logger.debug("exchangerate.host fail (%s): %s", currency, e)
    return None


async def _fetch_from_frankfurter(currency: str) -> Optional[float]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.frankfurter.app/latest",
                params={"from": currency, "to": "TRY"},
            )
            if r.status_code == 200:
                data = r.json()
                rate = data.get("rates", {}).get("TRY")
                if isinstance(rate, (int, float)) and rate > 0:
                    return float(rate)
    except Exception as e:
        logger.debug("frankfurter fail (%s): %s", currency, e)
    return None


async def _fetch_from_open_er_api(currency: str) -> Optional[float]:
    """open.er-api.com — ücretsiz, auth'suz, 1500 req/ay limit."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"https://open.er-api.com/v6/latest/{currency}")
            if r.status_code == 200:
                data = r.json()
                rate = data.get("rates", {}).get("TRY")
                if isinstance(rate, (int, float)) and rate > 0:
                    return float(rate)
    except Exception as e:
        logger.debug("open.er-api fail (%s): %s", currency, e)
    return None


async def get_exchange_rate(currency: str) -> tuple[Optional[float], Optional[date]]:
    """Anlık kur (TRY karşılığı) — cache'li.

    Kullanılan API'ler:
        1. exchangerate.host (birincil, ücretsiz, auth'suz)
        2. Frankfurter (yedek, ECB)

    Returns: (rate, fetch_date) veya (None, None)
    """
    if not currency or currency == "TRY":
        return (1.0, date.today())
    now = datetime.now()
    if currency in _RATES_CACHE:
        rate, ts = _RATES_CACHE[currency]
        if (now - ts).total_seconds() < _RATES_TTL:
            return (rate, ts.date())

    # Önce exchangerate.host
    rate = await _fetch_from_exchangerate_host(currency)
    # Yedek 1: Frankfurter
    if not rate:
        rate = await _fetch_from_frankfurter(currency)
    # Yedek 2: open.er-api.com
    if not rate:
        rate = await _fetch_from_open_er_api(currency)
    # Son cihat — TCMB statik fallback (yaklaşık değerler)
    if not rate:
        STATIC_FALLBACK = {"USD": 38.0, "EUR": 41.0, "GBP": 48.0, "CHF": 43.0}
        rate = STATIC_FALLBACK.get(currency)
        if rate:
            logger.warning("Statik kur fallback kullaniliyor: %s = %s TRY", currency, rate)

    if rate:
        _RATES_CACHE[currency] = (rate, now)
        return (rate, now.date())

    logger.warning("Kur bulunamadi (her iki kaynak da basarisiz): %s", currency)
    return (None, None)


# Geriye dönük uyumluluk için eski isim
get_tcmb_rate = get_exchange_rate


# ─── State machine ───
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
) -> Optional[BusinessDeal]:
    """KAP bildirimini iş anlaşması state machine'e gonder.

    Iş anlaşması değilse None döner.
    """
    if not is_business_deal(title):
        return None

    # Mevcut kayit — varsa amount_try doluysa skip, bossa re-parse + UPDATE
    existing = None
    if disclosure_id:
        stmt = select(BusinessDeal).where(BusinessDeal.kap_disclosure_id == disclosure_id).limit(1)
        existing = (await db.execute(stmt)).scalar_one_or_none()
        if existing and existing.amount_try is not None:
            return existing  # Tutar dolu — atla

    # Body bossa KAP URL'den canli cek (yeni RSC-aware extractor)
    if (not body or len(body) < 200) and kap_url:
        try:
            from app.scrapers.kap_disclosure_extractor import fetch_kap_disclosure
            disclosure = await fetch_kap_disclosure(kap_url)
            if disclosure and disclosure.get("full_text") and len(disclosure["full_text"]) > 100:
                body = disclosure["full_text"]
                logger.info("BusinessDeal body fetched (RSC): %s — %d char", ticker, len(body))
        except Exception as fe:
            logger.warning("BusinessDeal body fetch hata (%s): %s", ticker, fe)

    parsed = await ai_parse_business_deal(ticker, title, body or "")

    deal_date = parsed.get("deal_date") or (published_at.date() if published_at else date.today())
    currency = parsed.get("currency") or "TRY"
    amount_original = parsed.get("amount_original")

    amount_try = None
    rate_used = None
    rate_date = None
    if amount_original and currency:
        if currency == "TRY":
            amount_try = amount_original
            rate_used = 1.0
            rate_date = deal_date
        else:
            rate_used, rate_date = await get_exchange_rate(currency)
            if rate_used:
                amount_try = amount_original * rate_used

    # ★ SANITY CHECK: 1 milyar TL ustu tutarli dealler nadirdir.
    # Ozellikle currency=TRY + amount > 1B siklikla parser hatasi olur
    # (SAYAS 5.84 milyar bug). Body'de EUR/USD geciyor mu kontrol et;
    # geciyorsa flag at admin telegram'a bildir.
    if amount_try and amount_try > 1_000_000_000:
        try:
            body_check = (body or "")[:5000].upper()
            has_fx = any(k in body_check for k in ("EUR", "€", "USD", "DOLAR", "GBP", "£"))
            if currency == "TRY" and has_fx:
                logger.warning(
                    "BusinessDeal SHUPHELI: %s amount_try=%.0f TRY ama body'de EUR/USD geciyor — parser hatasi olabilir",
                    ticker, amount_try,
                )
                try:
                    from app.services.admin_telegram import send_admin_message
                    await send_admin_message(
                        f"⚠️ <b>BusinessDeal Şüpheli Tutar</b>\n"
                        f"#{ticker} — {amount_try:,.0f} TL ({currency} {amount_original:,.2f})\n"
                        f"Body'de EUR/USD geçiyor ama parser TRY okudu. Manuel kontrol et:\n"
                        f"{kap_url or '-'}",
                        silent=True,
                    )
                except Exception:
                    pass
            elif amount_try > 50_000_000_000:  # 50 milyar TL ustu — kesin supheli
                logger.warning(
                    "BusinessDeal COK YUKSEK: %s amount_try=%.0f TRY (%s %s) — manuel dogrula",
                    ticker, amount_try, amount_original, currency,
                )
                try:
                    from app.services.admin_telegram import send_admin_message
                    await send_admin_message(
                        f"⚠️ <b>BusinessDeal Aşırı Yüksek Tutar</b>\n"
                        f"#{ticker} — {amount_try:,.0f} TL ({currency} {amount_original:,.2f})\n"
                        f"50 milyar TL üstü — büyük olasılıkla parser hatası.\n"
                        f"{kap_url or '-'}",
                        silent=False,
                    )
                except Exception:
                    pass
        except Exception:
            pass

    # Mevcut kaydi UPDATE et — yeni AI parse sonucuyla
    if existing:
        if amount_original is not None:
            existing.amount_original = amount_original
            existing.currency = currency
            existing.amount_try = amount_try
            existing.exchange_rate_used = rate_used
            existing.rate_date = rate_date
        if parsed.get("counterparty") and not existing.counterparty:
            existing.counterparty = parsed["counterparty"]
        if parsed.get("summary") and not existing.summary:
            existing.summary = parsed["summary"]
        await db.flush()
        logger.info("BusinessDeal: UPDATE (%s, %s %s)", ticker, amount_original, currency)
        return existing

    new_row = BusinessDeal(
        ticker=ticker,
        company_name=company_name,
        title=(title or "")[:500],
        summary=parsed.get("summary"),
        amount_original=amount_original,
        currency=currency,
        amount_try=amount_try,
        exchange_rate_used=rate_used,
        rate_date=rate_date,
        deal_date=deal_date,
        counterparty=parsed.get("counterparty"),
        kap_disclosure_id=disclosure_id,
        kap_url=kap_url,
        source="kap_ai_parse",
    )
    db.add(new_row)
    await db.flush()
    logger.info("BusinessDeal: yeni (%s, %s %s = %s TRY)", ticker, amount_original, currency, amount_try)
    return new_row
