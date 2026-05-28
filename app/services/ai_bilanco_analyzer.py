"""
Bilanço AI Analiz Servisi
Hisse senedi bilanço verilerini Claude AI ile analiz eder.
"""

import httpx
import json
import logging
from decimal import Decimal
from typing import Optional
from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_AI_MODEL = "claude-sonnet-4-6"
_CLAUDE_MODEL = "claude-sonnet-4-20250514"
_AI_TIMEOUT = 120


_SYSTEM_PROMPT = """Sen kıdemli bir BIST (Borsa İstanbul) finansal analistisin. Bir CFA gibi,
şirketin 5 YILLIK finansal gelişimini derinlemesine okur, rakamların ARDINDAKİ hikâyeyi
çıkarırsın. Görevin yüzeysel bir puan vermek DEĞİL — şirketin gerçek finansal sağlığını,
trendini ve risklerini titizce analiz etmek.

═══════════════════ ANALİZ YÖNTEMİ ═══════════════════
1) TREND OKU (en önemli kısım): Tek çeyreğe değil, ZAMAN SERİSİNE bak.
   - Ciro/satış 5 yılda büyüyor mu, enflasyonun üzerinde mi? (TR enflasyonu yüksek —
     nominal büyüme ≠ reel büyüme. Yıllık %40-50 altı nominal büyüme aslında DARALMA olabilir.)
   - Net kâr istikrarlı mı, dalgalı mı, tek seferlik kalemlerle mi şiştî?
   - FAVÖK (esas faaliyet kârlılığı) marjı korunuyor mu, eriyor mu?
   - Özkaynak büyüyor mu (kâr birikimi/sermaye artışı)?
   - Çeyrekten çeyreğe momentum: son 2-3 çeyrek hızlanıyor mu yavaşlıyor mu?

2) KÂRLILIK KALİTESİ:
   - Brüt marj → faaliyet marjı → net marj zinciri. Nerede kâr eriyor?
   - ROE (özkaynak kârlılığı) sektör için iyi mi? (TR'de %20+ iyi, enflasyon nedeniyle.)
   - Net kâr esas faaliyetten mi yoksa finansman/kur/tek-seferlik gelirden mi geliyor?

3) BİLANÇO SAĞLIĞI & BORÇ:
   - Net Borç/FAVÖK kaç? (>4 yüksek risk, <2 sağlıklı, negatif=net nakit pozisyonu çok iyi)
   - Borç/Özkaynak oranı. Kısa vadeli borç baskısı.
   - Cari oran (dönen varlık/kısa borç) likidite.

4) SEKTÖRE GÖRE BAK: Banka/sigorta/faktoring/aracı kurum farklı okunur.
   - Banka: net faiz geliri, kredi/mevduat büyümesi öne çıkar (ciro/FAVÖK anlamsız).
   - Sigorta: brüt prim üretimi, teknik denge.
   - Sanayi/ticaret: ciro, FAVÖK marjı, net borç.

═══════════════════ PUANLAMA (1-10) ═══════════════════
Puanı analizden TÜRET, gelişigüzel verme. Rehber:
  9-10: Çok güçlü — istikrarlı reel büyüme + yüksek ROE + düşük borç + güçlü nakit.
  7-8 : İyi — sağlıklı büyüme/kârlılık, yönetilebilir borç, küçük zayıflıklar.
  5-6 : Orta — karışık tablo; bazı kalemler iyi, bazıları zayıf/dalgalı.
  3-4 : Zayıf — daralan ciro/kâr VEYA yüksek borç VEYA marj erimesi.
  1-2 : Riskli — zarar, eriyen özkaynak, sürdürülemez borç, ciddi bozulma.
Label: 9-10 "Çok Güçlü", 7-8 "Güçlü", 6 "İyi", 5 "Orta", 3-4 "Zayıf", 1-2 "Riskli".
Ondalık kullan (örn 7.5). Tüm şirketlere 5-6 verme — gerçek farkları yansıt.

═══════════════════ KURALLAR ═══════════════════
- Türkçe, akıcı ve PROFESYONEL ama anlaşılır. Teknik terimi ilk geçtiğinde parantezle açıkla.
- RAKAM KULLAN: "ciro %X büyüdü", "net borç/FAVÖK Y'ye düştü" gibi somut ifadeler.
- ASLA yatırım tavsiyesi verme (al/sat/tut deme). Sadece finansal durumu değerlendir.
- Veride olmayanı UYDURMA. Eksikse "veri yetersiz" de.
- Hedef fiyat, getiri beklentisi YAZMA.

═══════════════════ ÇIKTI (sadece geçerli JSON) ═══════════════════
{
    "overall_health_score": 7.5,
    "overall_health_label": "Güçlü",
    "five_year_growth": "5 yıllık ciro/kâr/özkaynak gelişimi — somut yüzdelerle trend hikâyesi (3-5 cümle).",
    "revenue_trend": "Satış/ciro trendi, reel büyüme yorumu, son çeyrek momentumu (2-4 cümle).",
    "profitability_analysis": "Brüt→faaliyet→net marj zinciri, ROE, kâr kalitesi (2-4 cümle).",
    "debt_analysis": "Net borç/FAVÖK, borç/özkaynak, likidite ve risk (2-4 cümle).",
    "balance_sheet_quality": "Özkaynak gelişimi, varlık yapısı, nakit pozisyonu (2-3 cümle).",
    "key_strengths": ["Somut güçlü yön 1", "Somut güçlü yön 2", "Somut güçlü yön 3"],
    "key_risks": ["Somut risk 1", "Somut risk 2"],
    "summary": "Yatırımcıya 3-4 cümlelik net genel değerlendirme — en kritik 2-3 bulguyu vurgula.",
    "disclaimer": "Bu analiz yatırım tavsiyesi değildir. Yatırım kararlarınızı kendi araştırmanıza dayandırın."
}
"""


def _fmt_tl(v) -> str:
    """Büyük TL rakamlarını okunur kısalt: 1.2 milyar / 340 milyon / 12.5 bin."""
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    a = abs(v)
    if a >= 1_000_000_000:
        return f"{v/1_000_000_000:,.2f} milyar TL"
    if a >= 1_000_000:
        return f"{v/1_000_000:,.1f} milyon TL"
    if a >= 1_000:
        return f"{v/1_000:,.0f} bin TL"
    return f"{v:,.0f} TL"


def _yoy(curr, prev) -> str:
    """Yıldan yıla % değişim metni."""
    try:
        curr = float(curr); prev = float(prev)
        if prev == 0:
            return ""
        pct = (curr - prev) / abs(prev) * 100
        sign = "+" if pct >= 0 else ""
        return f" ({sign}{pct:.0f}% y/y)"
    except (TypeError, ValueError):
        return ""


def _annual_aggregates(financials: list[dict]) -> list[dict]:
    """Çeyreklik veriden YILLIK toplam/snapshot üret (5 yıllık trend için).

    Gelir tablosu kalemleri (revenue/net_income/ebitda) → yıl içi NET çeyreklerin TOPLAMI.
    Bilanço kalemleri (equity/assets/net_debt) → yılın EN SON çeyreğindeki değer (snapshot).
    financials NET çeyreklik (YTD değil) varsayılır.
    """
    by_year: dict[str, dict] = {}
    # En eskiden yeniye sırala ki "son çeyrek snapshot" doğru olsun
    ordered = sorted(financials, key=lambda f: f.get("period", ""))
    for f in ordered:
        period = f.get("period", "")
        year = period.split("-")[0] if "-" in period else period[:4]
        if not year.isdigit():
            continue
        slot = by_year.setdefault(year, {
            "year": year, "revenue": 0.0, "net_income": 0.0, "ebitda": 0.0,
            "_rev_n": 0, "equity": None, "total_assets": None, "net_debt": None,
            "net_interest_income": 0.0, "gross_premiums": 0.0,
        })
        for fld in ("revenue", "net_income", "ebitda", "net_interest_income", "gross_premiums"):
            if f.get(fld) is not None:
                slot[fld] += float(f[fld])
        if f.get("revenue") is not None:
            slot["_rev_n"] += 1
        # Bilanço snapshot — en son işlenen (en yeni çeyrek) kazanır
        for snap in ("equity", "total_assets", "net_debt"):
            src = "total_equity" if snap == "equity" else snap
            if f.get(src) is not None:
                slot[snap] = float(f[src])
    return [by_year[y] for y in sorted(by_year.keys())]


def _build_bilanco_context(ticker: str, financials: list[dict], ratios: dict | None = None) -> str:
    """Bilanço verilerinden AI için context oluşturur — 5 yıllık + trend odaklı."""
    sec = (ratios or {}).get("sector") or (financials[0].get("sector_type") if financials else None)
    lines = [f"## {ticker} — Finansal Veri Seti", f"Sektör tipi: {sec or 'bilinmiyor'}\n"]

    # ── YILLIK ÖZET TABLOSU (5 yıllık trend — en kritik kısım) ──
    annuals = _annual_aggregates(financials)
    if annuals:
        lines.append("### 📊 Yıllık Gelişim (trend analizi için — en önemli)")
        prev = None
        for a in annuals[-5:]:  # son 5 yıl
            parts = [f"**{a['year']}**:"]
            if a["revenue"]:
                parts.append(f"Ciro {_fmt_tl(a['revenue'])}{_yoy(a['revenue'], prev['revenue']) if prev else ''}")
            if a["net_income"]:
                parts.append(f"Net Kâr {_fmt_tl(a['net_income'])}{_yoy(a['net_income'], prev['net_income']) if prev else ''}")
            if a["ebitda"]:
                parts.append(f"FAVÖK {_fmt_tl(a['ebitda'])}{_yoy(a['ebitda'], prev['ebitda']) if prev else ''}")
            if a["equity"] is not None:
                parts.append(f"Özkaynak {_fmt_tl(a['equity'])}{_yoy(a['equity'], prev['equity']) if (prev and prev.get('equity')) else ''}")
            if a["net_debt"] is not None:
                parts.append(f"Net Borç {_fmt_tl(a['net_debt'])}")
            if a["net_interest_income"]:
                parts.append(f"Net Faiz Geliri {_fmt_tl(a['net_interest_income'])}")
            if a["gross_premiums"]:
                parts.append(f"Brüt Prim {_fmt_tl(a['gross_premiums'])}")
            lines.append("- " + parts[0] + " " + " · ".join(parts[1:]))
            prev = a
        lines.append("")

    if financials:
        lines.append("### 📈 Çeyreklik Veriler (son dönemden eskiye, momentum için)")
        for f in financials[:12]:  # Son 12 çeyrek (3 yıl) — çeyreklik momentum
            row = [f"**{f.get('period', '?')}**:"]
            if f.get("revenue") is not None:
                row.append(f"Ciro {_fmt_tl(f['revenue'])}")
            if f.get("net_income") is not None:
                row.append(f"Net Kâr {_fmt_tl(f['net_income'])}")
            if f.get("ebitda") is not None:
                row.append(f"FAVÖK {_fmt_tl(f['ebitda'])}")
            if f.get("total_equity") is not None:
                row.append(f"Özkaynak {_fmt_tl(f['total_equity'])}")
            if f.get("net_debt") is not None:
                row.append(f"Net Borç {_fmt_tl(f['net_debt'])}")
            if f.get("net_interest_income") is not None:
                row.append(f"Net Faiz Geliri {_fmt_tl(f['net_interest_income'])}")
            if f.get("gross_premiums") is not None:
                row.append(f"Brüt Prim {_fmt_tl(f['gross_premiums'])}")
            lines.append("- " + row[0] + " " + " · ".join(row[1:]))
        lines.append("")

    if ratios:
        lines.append("\n### Güncel Değerleme Çarpanları")
        if ratios.get("fk"):
            lines.append(f"- F/K: {ratios['fk']:.2f}")
        if ratios.get("pddd"):
            lines.append(f"- PD/DD: {ratios['pddd']:.2f}")
        if ratios.get("fd_favok"):
            lines.append(f"- FD/FAVÖK: {ratios['fd_favok']:.2f}")
        if ratios.get("piyasa_degeri"):
            lines.append(f"- Piyasa Değeri: {ratios['piyasa_degeri']:,.0f} TL")
        if ratios.get("sector"):
            lines.append(f"- Sektör: {ratios['sector']}")
        if ratios.get("sector_avg_fk"):
            lines.append(f"- Sektör Ort. F/K: {ratios['sector_avg_fk']:.2f}")
        if ratios.get("sector_avg_pddd"):
            lines.append(f"- Sektör Ort. PD/DD: {ratios['sector_avg_pddd']:.2f}")

    return "\n".join(lines)


async def _call_ai_abacus(system_prompt: str, user_message: str) -> str | None:
    """Abacus RouteLLM üzerinden Claude çağrısı."""
    abacus_key = settings.ABACUS_API_KEY
    if not abacus_key:
        logger.warning("ABACUS_API_KEY tanımlı değil")
        return None
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _ABACUS_URL,
                headers={"Authorization": f"Bearer {abacus_key}", "Content-Type": "application/json"},
                json={
                    "model": _AI_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    "temperature": 0.12,
                    "max_tokens": 4000,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            logger.warning("Abacus bilanço AI %d döndü", resp.status_code)
    except Exception as e:
        logger.exception("Abacus bilanço AI hatası: %s", e)
    return None


async def _call_ai_anthropic(system_prompt: str, user_message: str) -> str | None:
    """Doğrudan Anthropic API fallback."""
    api_key = settings.ANTHROPIC_API_KEY
    if not api_key:
        return None
    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _ANTHROPIC_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _CLAUDE_MODEL,
                    "max_tokens": 4000,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_message}],
                    "temperature": 0.12,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                return data["content"][0]["text"]
            logger.warning("Anthropic bilanço AI %d döndü", resp.status_code)
    except Exception as e:
        logger.exception("Anthropic bilanço AI hatası: %s", e)
    return None


async def analyze_bilanco(ticker: str, financials: list[dict], ratios: dict | None = None) -> dict | None:
    """
    Bilanço verilerini AI ile analiz eder.

    Args:
        ticker: Hisse kodu
        financials: Çeyreklik bilanço verileri listesi
        ratios: Güncel F/K, PD/DD gibi oranlar

    Returns:
        dict — AI analiz sonucu (JSON) veya None
    """
    if not financials:
        logger.warning("Bilanço analizi için veri yok: %s", ticker)
        return None

    context = _build_bilanco_context(ticker, financials, ratios)
    user_message = f"Aşağıdaki {ticker} hissesinin finansal verilerini analiz et:\n\n{context}"

    # Önce Abacus, sonra Anthropic fallback
    content = await _call_ai_abacus(_SYSTEM_PROMPT, user_message)
    if not content:
        logger.info("Abacus başarısız, Anthropic deneniyor: %s", ticker)
        content = await _call_ai_anthropic(_SYSTEM_PROMPT, user_message)

    if not content:
        logger.error("Bilanço AI analizi başarısız: %s", ticker)
        return None

    # JSON parse
    try:
        # Claude bazen ```json ... ``` wrapper ile döner
        clean = content.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1]
            clean = clean.rsplit("```", 1)[0]
        return json.loads(clean)
    except json.JSONDecodeError:
        logger.warning("Bilanço AI JSON parse hatası %s, raw content döndürülüyor", ticker)
        return {"summary": content, "disclaimer": "Bu analiz yatırım tavsiyesi değildir."}


# ═══════════════════════════════════════════════════════════════════════════════
#  KAP BİLDİRİMİNDEN BİLANÇO RAKAMLARINI PARSE ET
# ═══════════════════════════════════════════════════════════════════════════════

_PARSE_SYSTEM_PROMPT = """Sen KAP Finansal Rapor metinlerinden bilanço ve gelir tablosu rakamlarını çıkaran bir uzmansın.

KAP Finansal Rapor formatı:
- "Cari Dönem 31.03.2026" (SOL kolon — bu güncel veriler, BUNU AL)
- "Önceki Dönem 31.12.2025" veya "01.01.2025-31.03.2025" (SAĞ kolon — KULLANMA)
- XBRL etiketleri: ifrs-full_..., kap-fr_...
- Her satırda: Etiket | Türkçe açıklama | Dipnot | Cari Dönem | Önceki Dönem

ÖNEMLİ EŞLEŞMELER (Cari Dönem rakamını al):
- "Hasılat" / "Revenue" / ifrs-full_Revenue → revenue
- "BRÜT KAR (ZARAR)" / ifrs-full_GrossProfit → gross_profit
- "ESAS FAALİYET KARI (ZARARI)" / ifrs-full_ProfitLossFromOperatingActivities → operating_profit
- "DÖNEM KARI (ZARARI)" / ifrs-full_ProfitLoss → net_income
  (Eğer "Ana Ortaklık Payları" satırı varsa onu kullan)
- "TOPLAM VARLIKLAR" / ifrs-full_Assets → total_assets
- "TOPLAM ÖZKAYNAKLAR" / ifrs-full_Equity → total_equity
- "TOPLAM YÜKÜMLÜLÜKLER" / ifrs-full_Liabilities → total_debt
- "Nakit ve Nakit Benzerleri" / ifrs-full_CashAndCashEquivalents → cash_and_equivalents
- net_debt = total_debt - cash_and_equivalents (hesapla)
- "FAVÖK" varsa al, yoksa operating_profit + amortisman düzeltmesi (eğer "Amortisman ve İtfa Gideri" varsa ekle)

DÖNEM TESPİTİ:
- "01.01.2026 - 31.03.2026" → "2026-Q1"
- "01.01.2026 - 30.06.2026" → "2026-Q2"
- "01.01.2026 - 30.09.2026" → "2026-Q3"
- "01.01.2026 - 31.12.2026" → "2026-Q4"
- Sadece bilanço (durum tablosu) için: 31.03 → Q1, 30.06 → Q2 vb.

KURALLAR:
- Rakamları nokta/virgül ayraçlardan temizle: "506.840.805" → 506840805
- Negatif rakamlar parantezli/eksili olabilir: "-77.861.972" → -77861972
- Rakam bulunamazsa null. Tahmin etme.
- Sadece Cari Dönem (sol kolon) — Önceki Dönem KULLANMA
- TL cinsinden olduğu varsayılır

ÇIKTI (sadece JSON):
{
    "period": "2026-Q1",
    "revenue": 506840805,
    "gross_profit": 202479726,
    "operating_profit": 129117739,
    "net_income": 13935214,
    "ebitda": null,
    "total_assets": 4529098206,
    "total_equity": 2696607818,
    "total_debt": 1832490388,
    "net_debt": null,
    "cash_and_equivalents": 160349649,
    "confidence": "high"
}

Bilanço/Finansal Rapor DEĞİLSE: {"error": "not_bilanco"}
"""


async def parse_bilanco_from_kap(ticker: str, kap_content: str) -> dict | None:
    """
    KAP Finansal Rapor body'sinden bilanço/gelir tablosu rakamlarını cıkarır.

    Yöntem: AI YOK — XBRL etiketleri uzerinden regex scraper.
    Hızlı, deterministik, ücretsiz.

    Args:
        ticker: Hisse kodu
        kap_content: KAP bildirim metin içeriği (body)

    Returns:
        dict — Parse edilmis bilanço rakamlari veya None
    """
    if not kap_content or len(kap_content) < 50:
        logger.warning("KAP parse: %s — içerik çok kısa", ticker)
        return None

    from app.services.bilanco_kap_scraper import parse_kap_finansal_rapor
    result = parse_kap_finansal_rapor(kap_content)

    # Period yoksa veya en kritik alanlardan hicbiri yoksa null don
    if not result.get("period") and not result.get("total_assets") and not result.get("revenue"):
        logger.warning("KAP scrape: %s — XBRL etiketleri bulunamadi", ticker)
        return None

    result["ticker"] = ticker
    result["needs_verification"] = result.get("confidence") != "high"

    logger.info(
        "KAP bilanco scrape: %s %s — Ciro: %s, Net Kar: %s, Top.Varlik: %s (guven: %s)",
        ticker, result.get("period", "?"),
        result.get("revenue"), result.get("net_income"),
        result.get("total_assets"), result.get("confidence"),
    )
    return result


# Gelir tablosu alanlari — YTD verilir, Net Q icin onceki YTD'den cikarma gerekir
_INCOME_STATEMENT_FIELDS = (
    "revenue", "gross_profit", "operating_profit", "net_income", "ebitda",
    # Banka sektoru
    "net_interest_income", "net_fees_commissions", "operating_revenue",
    # Sigorta sektoru
    "gross_premiums", "technical_balance",
)
# Bilanco alanlari — anlik (point-in-time), donusum GEREKMEZ
_BALANCE_SHEET_FIELDS = (
    "total_assets", "current_assets", "non_current_assets",
    "total_equity", "total_debt", "net_debt", "cash_and_equivalents",
    "loans", "deposits",
)


def _prev_period_in_same_year(period: str) -> Optional[str]:
    """2026-Q2 -> 2026-Q1, 2026-Q3 -> 2026-Q2, 2026-Q4 -> 2026-Q3.
    Q1 icin None (YTD = Q1, donusum gerekmez)."""
    try:
        y, q = period.split("-Q")
        qi = int(q)
        if qi <= 1:
            return None
        return f"{y}-Q{qi - 1}"
    except (ValueError, AttributeError):
        return None


async def _convert_ytd_to_net_quarter(
    db,
    ticker: str,
    period: str,
    parsed: dict,
) -> dict:
    """KAP XBRL'inden gelen gelir tablosu RAKAMLARI YTD'dir (yil basindan beri).
    Net ceyrek icin onceki donemin YTD'sini cikartmak gerekir.

    Q1: 3 aylik YTD = Q1 (donusum yok)
    Q2: 6 aylik YTD - Q1 = Q2 net
    Q3: 9 aylik YTD - H1 YTD = Q3 net
    Q4: 12 aylik YTD - 9M YTD = Q4 net

    DB'de onceki donem yoksa: gelir alanlarini None yap (yanlis YTD yazmaktan iyidir).
    """
    if not period or "-Q" not in period:
        return parsed
    prev_period = _prev_period_in_same_year(period)
    if prev_period is None:
        # Q1 — YTD zaten net Q1, donusum yok
        return parsed

    from app.models.company_financial import CompanyFinancial
    from sqlalchemy import select

    # Yil basindan beri kumulatif: Q2'de prev Q1'in NET'i = Q1 YTD
    # Q3'te prev Q1+Q2 net toplami = H1 YTD lazim
    # Q4'te Q1+Q2+Q3 net toplami = 9M YTD lazim
    qi = int(period.split("-Q")[1])
    year = period.split("-Q")[0]
    cumulative: dict[str, float] = {f: 0.0 for f in _INCOME_STATEMENT_FIELDS}
    found_any = False
    for prev_q in range(1, qi):
        pp = f"{year}-Q{prev_q}"
        row = (await db.execute(
            select(CompanyFinancial).where(
                CompanyFinancial.ticker == ticker,
                CompanyFinancial.period == pp,
            )
        )).scalar_one_or_none()
        if row is None:
            # Onceki donem eksik — kumulatif hesabi yapamayiz, guvenli skip
            logger.warning(
                "YTD->Q donusum: %s %s icin onceki donem %s eksik, gelir alanlari atlanacak",
                ticker, period, pp,
            )
            for f in _INCOME_STATEMENT_FIELDS:
                if parsed.get(f) is not None:
                    parsed[f] = None  # Yanlis YTD yazmaktansa NULL biraktig
            return parsed
        found_any = True
        for f in _INCOME_STATEMENT_FIELDS:
            v = getattr(row, f, None)
            if v is not None:
                cumulative[f] += float(v)

    if not found_any:
        # Q2+ ama hicbir prev veri yok — gelir alanlari skip
        for f in _INCOME_STATEMENT_FIELDS:
            if parsed.get(f) is not None:
                parsed[f] = None
        return parsed

    # Net Q = YTD - kumulatif onceki Q'lar
    for f in _INCOME_STATEMENT_FIELDS:
        ytd_val = parsed.get(f)
        if ytd_val is not None:
            parsed[f] = float(ytd_val) - cumulative[f]

    logger.info(
        "YTD->Q donusum: %s %s — gelir alanlari %s onceki Q toplamlardan cikartildi",
        ticker, period, ", ".join(f for f in _INCOME_STATEMENT_FIELDS if parsed.get(f) is not None),
    )
    return parsed


async def _alert_bilanco_issue(ticker: str, parsed: dict, kap_url: Optional[str] = None):
    """Bilanco parse eksiklikleri icin Telegram alert (anti-spam)."""
    try:
        missing = []
        sec = parsed.get("sector_type")
        conf = parsed.get("confidence")
        # Sanayi/sigorta icin revenue NULL kritik
        if sec in ("industrial", "insurance") and parsed.get("revenue") is None:
            missing.append("revenue")
        if sec == "bank" and parsed.get("net_interest_income") is None:
            missing.append("net_interest_income")
        if parsed.get("total_assets") is None:
            missing.append("total_assets")
        if conf == "low":
            missing.append("low_confidence")
        if missing:
            from app.services.admin_telegram import notify_kap_parse_issue
            await notify_kap_parse_issue(
                "bilanco", ticker, kap_url, missing,
                detail=f"sector={sec} period={parsed.get('period')} confidence={conf}",
            )
    except Exception:
        pass


async def save_parsed_bilanco(ticker: str, parsed: dict) -> bool:
    """
    AI ile parse edilen bilanço rakamlarını DB'ye kaydeder.

    needs_verification=True ile kaydedilir — IsYatirim'den kesin veri
    geldiğinde üzerine yazılır.

    KRITIK: KAP XBRL gelir tablosunu YTD verir. Q2-Q4 icin onceki dönemler
    cikartilarak NET CEYREK degerine donusturulur. Aksi halde Q2'de revenue
    Q1+Q2 toplami görünür ve AI "%200 büyüme" yorumu yapar (kullanicinin
    "0 fazla" sikayetinin kaynagi).
    """
    try:
        from app.database import async_session
        from app.models.company_financial import CompanyFinancial
        from sqlalchemy import select

        period = parsed.get("period")
        if not period:
            return False

        # Eksik kritik alan varsa admin'e bildir
        await _alert_bilanco_issue(ticker, parsed)

        # YTD -> Net Ceyrek donusumu (Q2/Q3/Q4 icin)
        async with async_session() as _conv_db:
            parsed = await _convert_ytd_to_net_quarter(_conv_db, ticker, period, parsed)

        async with async_session() as db:
            # Var mı kontrol et
            stmt = select(CompanyFinancial).where(
                CompanyFinancial.ticker == ticker,
                CompanyFinancial.period == period,
            )
            existing = (await db.execute(stmt)).scalar_one_or_none()

            if existing and existing.source == "isyatirim":
                # IsYatirim verisi var ama current_assets/non_current_assets gibi
                # alanlari 0 olarak doluyor (None degil). KAP XBRL'de gercek deger var.
                # NULL VEYA 0 olanlari KAP'tan ENRICH et.
                from datetime import datetime, timezone
                enriched = False
                for field in ["current_assets", "non_current_assets",
                              "total_debt", "cash_and_equivalents",
                              "gross_profit", "operating_profit"]:
                    val = parsed.get(field)
                    if val is None or val == 0:
                        continue
                    existing_val = getattr(existing, field, None)
                    # NULL veya 0 veya cok kucuk (anlamsiz) ise enrich et
                    if existing_val is None or float(existing_val or 0) == 0:
                        setattr(existing, field, val)
                        enriched = True
                if enriched:
                    existing.updated_at = datetime.now(timezone.utc)
                    await db.commit()
                    logger.info("KAP parse ENRICH: %s %s — IsYatirim'de NULL/0 alanlar dolduruldu", ticker, period)
                else:
                    logger.info("KAP parse: %s %s — IsYatirim mevcut, enrich gerekmedi", ticker, period)
                return enriched

            from datetime import datetime, timezone

            if existing:
                # AI parse verisini güncelle (henüz IsYatirim gelmemişse)
                for field in ["revenue", "gross_profit", "operating_profit", "net_income",
                              "ebitda", "total_assets", "current_assets", "non_current_assets",
                              "total_equity", "total_debt",
                              "net_debt", "cash_and_equivalents"]:
                    val = parsed.get(field)
                    if val is not None:
                        setattr(existing, field, val)
                existing.source = "kap_ai_parse"
                existing.updated_at = datetime.now(timezone.utc)
            else:
                new_record = CompanyFinancial(
                    ticker=ticker,
                    period=period,
                    revenue=parsed.get("revenue"),
                    gross_profit=parsed.get("gross_profit"),
                    operating_profit=parsed.get("operating_profit"),
                    net_income=parsed.get("net_income"),
                    ebitda=parsed.get("ebitda"),
                    total_assets=parsed.get("total_assets"),
                    current_assets=parsed.get("current_assets"),
                    non_current_assets=parsed.get("non_current_assets"),
                    total_equity=parsed.get("total_equity"),
                    total_debt=parsed.get("total_debt"),
                    net_debt=parsed.get("net_debt"),
                    cash_and_equivalents=parsed.get("cash_and_equivalents"),
                    source="kap_ai_parse",
                )
                db.add(new_record)

            await db.commit()
            logger.info("KAP parse DB kayıt: %s %s", ticker, period)
            return True

    except Exception as e:
        logger.exception("KAP parse DB hatası %s: %s", ticker, e)
        return False
