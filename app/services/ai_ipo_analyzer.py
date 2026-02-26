"""AI Halka Arz Degerlendirme Raporu Servisi.

IPO dagitima girdiginde (in_distribution) otomatik olarak AI analiz raporu uretir.
Rapor, kucuk yatirimcilar icin profesyonel analist kalitesinde yazilir.

Versiyon 2: Gecmis tahsisat verileri, senaryo tablosu, izahname analizi entegre.
Model: claude-sonnet-4-5 (Abacus RouteLLM uzerinden)
"""

import gc
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────
# Sabitler
# ────────────────────────────────────────────

_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_AI_MODEL = "claude-sonnet-4-5"
_AI_TIMEOUT = 150  # Daha fazla kontekst → daha uzun yanit suresi

_SYSTEM_PROMPT = """Sen Turkiye'nin en deneyimli halka arz analistlerinden birisin. SZ Algo Trade platformu icin profesyonel halka arz degerlendirme raporlari yaziyorsun. Hedef kitlen: kucuk bireysel yatirimci.

YAZIM KURALLARI:
- Profesyonel ama anlasilir. Finans jargonunu ACIKLAYARAK kullan.
- Veriye dayali, somut, NET ifadeler. Muglak cumle kurma.
- Hem firsatlari hem riskleri dengeli degerlendir — tarafsiz ol.
- Sektore ozgu derinlikli analiz yap.
- Turkce hakim: akici, etkileyici, profesyonel.
- DEVRIK CUMLE YASAK — ozne + nesne + yuklem sirasi koru.
- Emoji kullanma.
- Dolgu paragraf yazma. Her cumle yeni bilgi tasimali.

KAYNAK YASAGI (MUTLAK):
- HICBIR kaynak, referans, link veya web sitesi ADI yazma.
- "Gedik Yatirim", "gedik.com", "halkarztakip.com", "KAP", "isyatirim.com.tr" gibi kaynak ASLA belirtme.
- "X kaynagina gore", "Y sitesinde", "Z'ye gore" gibi ifadeler YASAK.
- Veriyi dogrudan yaz, kaynagini soyleme. Ornek: "Tahmini katilimci sayisi 200.000-300.000 araliginda" DE, "Gedik verisine gore..." DEME.
- Bu kurallar tum alanlar icin gecerli: analysis, how_to_participate, lot_estimate_explanation, sector_comparison, recommendation.
- Kural ihlali durumunda rapor REDDEDILIR.

RAPOR FORMATI (JSON):
{
  "overall_score": <1.0 ile 10.0 arasi GELECEK POTANSIYELI puani — ondalikli>,
  "risk_level": "<dusuk | orta | yuksek | cok_yuksek>",
  "analysis": "<4-6 paragraflik detayli genel degerlendirme — risk detaylarini da ICINDE degerlendir>",
  "how_to_participate": "<Basvuru rehberi — hangi araci kurum, adim adim>",
  "lot_estimate_explanation": "<Lot tahmini aciklamasi — gecmis verilere dayanarak>",
  "scenario_table": [
    {"participants": "100.000", "estimated_lot": <sayi>},
    {"participants": "200.000", "estimated_lot": <sayi>},
    {"participants": "300.000", "estimated_lot": <sayi>},
    {"participants": "500.000", "estimated_lot": <sayi>}
  ],
  "sector_comparison": "<Sirketin sektordeki konumu ve kiyaslama>",
  "recommendation": "<Sonuc paragrafi>"
}

ONEMLI PUANLAMA NOTU:
- overall_score GELECEK POTANSIYELI puanidir — sirketin ileriye donuk buyume, karlilık ve yatirimci getirisi potansiyelini olcer.
- risk_level ayri bir alan olarak kalir ama puan belirlerken asil odak POTANSIYEL olmalidir.
- Riskleri analysis icerisinde detayli isle, ancak dusuk puan "cok riskli" degil "sinirli potansiyel" anlamina gelir.
- Ton olarak: olumlu ve yapici ol, ama gercekci kal. Abartma, sise de.

ALAN DETAYLARI:

1. analysis (en az 250 kelime):
   - Sirketin guclu yanlari, zayif yanlari
   - Halka arz fiyatlandirmasi makul mu? (arz buyuklugu, iskonto, halka aciklik orani)
   - Mali tablolar: hasilat buyumesi, kar marji, borc durumu
   - Eger izahname analiz verileri sunulmussa, olumlu ve olumsuz bulgulari raporuna entegre et
   - Piyasa kosullari ve zamanlama degerlendirmesi
   - Fon kullanim hedefleri makul mu?
   - RISK DETAYLARINI BU BOLUMDE AC: sektor riskleri, sirket riskleri, makro riskler

2. how_to_participate (en az 100 kelime):
   - Dagitim yontemi: esit/oransal/karma ne demek, yatirimci icin ne anlama gelir
   - Hangi araci kurum/bankadan basvurulur
   - Minimum lot, basvuru saatleri
   - Talep toplama mi, borsada satis mi — fark ne
   - Lock-up suresi varsa etkisi

3. lot_estimate_explanation (en az 120 kelime):
   - Toplam lot ve dagitim yontemine gore tahmini kisi basi lot
   - Eger gecmis halka arz tahsisat verileri verilmisse: benzer dagitim yontemli halka arzlardaki katilimci sayisi ve lot dagitimini baz al
   - Senaryo tablosu verilmisse: "100K kisi katilirsa X lot, 500K kisi katilirsa Y lot" seklinde ACIKLA
   - Oransal dagitimsa mekanizmayi acikla

4. scenario_table (JSON dizisi):
   - Sana verilen senaryo tablosundaki degerleri AYNEN kullan
   - Eger senaryo tablosu verilmemisse, toplam lot sayisina gore kendin hesapla:
     participants (formatli string: "100.000"), estimated_lot (tam sayi)
   - 4 satir: 100K, 200K, 300K, 500K katilimci
   - Hesaplama: bireysel_tahsisat_lotu / katilimci_sayisi (esit dagitimda)
   - Oransal dagitimda: "Oransal dagitimda lot tahmini yapilamaz" yazilip bos dizi [] dondurulebilir

5. sector_comparison (en az 100 kelime):
   - Sirketin sektordeki konumu
   - Varsa F/K, PD/DD kiyaslamasi (halka arz fiyatindan hesaplanabiliyorsa)
   - Sektorun genel durumu ve buyume potansiyeli
   - Rakiplerle kiyaslama (halka acik rakipler varsa)

6. recommendation (en az 100 kelime):
   - Net sonuc: katilmaya deger mi?
   - Hangi stratejiler izlenebilir (kisa/orta/uzun vade)
   - Dikkat edilmesi gerekenler
   - SON CUMLE MUTLAKA: "Bu degerlendirme yatirim tavsiyesi niteliginde degildir."

PUANLAMA REHBERI — GELECEK POTANSIYELI:
Bu puan sirketin GELECEK POTANSIYELI'ni olcer. Riskleri analysis icerisinde ayrintili degerlendir ama puan tamamen potansiyele odaklansin.
- 1.0-2.5: Sinirli potansiyel — buyume perspektifi zayif, temel gostergeler yetersiz
- 2.6-4.0: Orta potansiyel — bazi olumlu yanlar var ama genel goruntu karisik
- 4.1-5.5: Iyi potansiyel — fiyatlama makul, sektor ortalamasi uzerinde beklenti
- 5.6-7.0: Yuksek potansiyel — guclu temel gostergeler, cazip fiyatlama, buyume alani genis
- 7.1-8.5: Cok yuksek potansiyel — istisnai buyume hikayesi, her sey olumlu
- 8.6-10.0: Olagan ustu potansiyel — COK nadiren verilmeli, piyasada ender firsat

KRITIK:
- SADECE JSON formatinda cevap ver. Baska hicbir sey yazma.
- JSON disinda hicbir metin, aciklama, markdown isareti ekleme.
- Tum string degerler Turkce olmali.
- scenario_table icerisindeki participants alanı formatli string olmali (orn: "100.000").
- Verisi olmayan konularda spekülasyon yapma, "veri bulunamadi" de."""


# ────────────────────────────────────────────
# Kontekst Olusturma Fonksiyonlari
# ────────────────────────────────────────────

async def _build_historical_allocation_context(session, ipo) -> str:
    """Gecmis halka arzlarin tahsisat verilerinden lot tahmini konteksti olusturur."""
    from app.models.ipo import IPO, IPOAllocation
    from sqlalchemy import select, and_

    try:
        result = await session.execute(
            select(IPO, IPOAllocation)
            .join(IPOAllocation)
            .where(and_(
                IPOAllocation.group_name == "bireysel",
                IPOAllocation.participant_count.isnot(None),
                IPOAllocation.avg_lot_per_person.isnot(None),
                IPO.id != ipo.id,
                IPO.status.in_(["trading", "awaiting_trading", "archived"]),
            ))
            .order_by(IPO.trading_start.desc().nullslast())
            .limit(15)
        )
        rows = result.all()
    except Exception as e:
        logger.warning("Gecmis tahsisat verisi cekilemedi: %s", e)
        return ""

    if not rows:
        return ""

    method_labels = {
        "esit": "Esit",
        "bireysele_esit": "Bireysel Esit",
        "tamami_esit": "Tum Esit",
        "oransal": "Oransal",
        "karma": "Karma",
    }

    lines = [
        "",
        "--- GECMIS HALKA ARZ TAHSISAT VERILERI (Referans) ---",
        "Asagidaki veriler son halka arzlarin GERCEKLESEN bireysel yatirimci sonuclaridir:",
    ]

    for past_ipo, alloc in rows:
        method = method_labels.get(past_ipo.distribution_method or "", past_ipo.distribution_method or "?")
        lines.append(
            f"  {past_ipo.company_name} ({past_ipo.ticker or '?'}): "
            f"{alloc.participant_count:,} kisi basvurdu, "
            f"kisi basi {alloc.avg_lot_per_person} lot dagitildi, "
            f"dagitim: {method}"
        )

    # Istatistikler
    participants = [r[1].participant_count for r in rows]
    lots = [float(r[1].avg_lot_per_person) for r in rows]
    avg_p = sum(participants) / len(participants)
    avg_l = sum(lots) / len(lots)
    min_l = min(lots)
    max_l = max(lots)

    lines.append(f"\nSon {len(rows)} halka arz ortalamasi: {avg_p:,.0f} basvuru, kisi basi {avg_l:.1f} lot")
    lines.append(f"Aralik: kisi basi {min_l:.0f} - {max_l:.0f} lot")

    # Benzer dagitim yontemli olanlari filtrele
    if ipo.distribution_method:
        similar = [
            (p, a) for p, a in rows
            if p.distribution_method == ipo.distribution_method
        ]
        if similar:
            sim_avg = sum(float(a.avg_lot_per_person) for _, a in similar) / len(similar)
            lines.append(
                f"Ayni dagitim yontemli ({method_labels.get(ipo.distribution_method, ipo.distribution_method)}) "
                f"ortalama: kisi basi {sim_avg:.1f} lot ({len(similar)} halka arz)"
            )

    return "\n".join(lines)


def _build_lot_scenario_table(ipo) -> str:
    """Lot senaryo tablosu olusturur — AI'a scenario_table uretmesi icin veri saglar."""
    if not ipo.total_lots:
        return ""

    # Bireysel yatirimci tahsisat oranini belirle
    bireysel_pct = 0.40  # varsayilan %40
    if hasattr(ipo, 'allocations') and ipo.allocations:
        for alloc in ipo.allocations:
            if alloc.group_name == "bireysel" and alloc.allocation_pct:
                bireysel_pct = float(alloc.allocation_pct) / 100
                break

    bireysel_lots = int(ipo.total_lots * bireysel_pct)

    lines = [
        "",
        "--- LOT SENARYO TABLOSU ---",
        f"Toplam lot: {ipo.total_lots:,} | Bireysel tahsisat (%{bireysel_pct*100:.0f}): {bireysel_lots:,} lot",
        "",
        "Katilimci Sayisi | Tahmini Kisi Basi Lot",
    ]

    for threshold in [100_000, 200_000, 300_000, 500_000]:
        if ipo.distribution_method == "oransal":
            lines.append(f"  {threshold:,} kisi → Oransal dagitim (yatirim tutarina gore degisir)")
        else:
            est = max(1, bireysel_lots // threshold)
            lines.append(f"  {threshold:,} kisi → {est} lot")

    if ipo.estimated_lots_per_person:
        lines.append(f"\n500.000 katilimci varsayiminda tahmini kisi basi: {ipo.estimated_lots_per_person} lot")

    return "\n".join(lines)


def _build_prospectus_context(ipo) -> str:
    """Izahname AI analiz sonuclarini kontekst olarak ekler."""
    if not ipo.prospectus_analysis:
        return ""

    try:
        pa = json.loads(ipo.prospectus_analysis)
    except (json.JSONDecodeError, TypeError):
        return ""

    lines = [
        "",
        "--- IZAHNAME AI ANALIZ SONUCLARI ---",
        "Asagidaki bulgular izahname PDF'inden otomatik cikarilmistir:",
        f"Risk Seviyesi: {pa.get('risk_level', 'bilinmiyor')}",
    ]

    if pa.get("key_risk"):
        lines.append(f"Kritik Risk: {pa['key_risk']}")

    positives = pa.get("positives", [])
    if positives:
        lines.append("\nOlumlu Bulgular:")
        for p in positives[:6]:
            lines.append(f"  + {p}")

    negatives = pa.get("negatives", [])
    if negatives:
        lines.append("\nOlumsuz Bulgular:")
        for n in negatives[:6]:
            lines.append(f"  - {n}")

    if pa.get("summary"):
        lines.append(f"\nIzahname Ozeti: {pa['summary']}")

    return "\n".join(lines)


def _build_ipo_context(
    ipo,
    historical_context: str = "",
    scenario_table: str = "",
    prospectus_context: str = "",
) -> str:
    """IPO verisini AI icin okunabilir formata cevirir."""
    lines = []
    lines.append(f"SIRKET: {ipo.company_name}")
    if ipo.ticker:
        lines.append(f"BORSA KODU: {ipo.ticker}")
    if ipo.sector:
        lines.append(f"SEKTOR: {ipo.sector}")

    lines.append("")
    lines.append("--- HALKA ARZ BILGILERI ---")

    if ipo.ipo_price:
        lines.append(f"Halka Arz Fiyati: {ipo.ipo_price} TL")
    if ipo.total_lots:
        lines.append(f"Toplam Lot Sayisi: {ipo.total_lots:,}")
    if ipo.offering_size_tl:
        lines.append(f"Arz Buyuklugu: {ipo.offering_size_tl:,.0f} TL")
    if ipo.capital_increase_lots:
        lines.append(f"Sermaye Artirimi: {ipo.capital_increase_lots:,} lot")
    if ipo.partner_sale_lots:
        lines.append(f"Ortak Satisi: {ipo.partner_sale_lots:,} lot")
    if ipo.public_float_pct:
        lines.append(f"Halka Aciklik Orani: %{ipo.public_float_pct}")
    if ipo.discount_pct:
        lines.append(f"Iskonto Orani: %{ipo.discount_pct}")

    lines.append("")
    lines.append("--- DAGITIM & KATILIM ---")

    if ipo.distribution_method:
        method_labels = {
            "esit": "Esit Dagitim",
            "bireysele_esit": "Bireysel Yatirimciya Esit",
            "tamami_esit": "Tamami Esit",
            "oransal": "Oransal Dagitim",
            "karma": "Karma Dagitim",
        }
        lines.append(f"Dagitim Yontemi: {method_labels.get(ipo.distribution_method, ipo.distribution_method)}")
    if ipo.distribution_description:
        lines.append(f"Dagitim Aciklamasi: {ipo.distribution_description}")
    if ipo.participation_method:
        method_labels = {
            "talep_toplama": "Talep Toplama",
            "borsada_satis": "Borsada Satis",
        }
        lines.append(f"Katilim Yontemi: {method_labels.get(ipo.participation_method, ipo.participation_method)}")
    if ipo.participation_description:
        lines.append(f"Katilim Aciklamasi: {ipo.participation_description}")
    if ipo.estimated_lots_per_person:
        lines.append(f"Tahmini Kisi Basi Lot: {ipo.estimated_lots_per_person}")
    if ipo.min_application_lot:
        lines.append(f"Minimum Basvuru: {ipo.min_application_lot} lot")

    lines.append("")
    lines.append("--- TARIHLER ---")

    if ipo.subscription_start:
        lines.append(f"Basvuru Baslangic: {ipo.subscription_start}")
    if ipo.subscription_end:
        lines.append(f"Basvuru Bitis: {ipo.subscription_end}")
    if ipo.subscription_hours:
        lines.append(f"Basvuru Saatleri: {ipo.subscription_hours}")
    if ipo.expected_trading_date:
        lines.append(f"Beklenen Islem Tarihi: {ipo.expected_trading_date}")
    if ipo.trading_start:
        lines.append(f"Islem Baslangic: {ipo.trading_start}")
    if ipo.spk_approval_date:
        lines.append(f"SPK Onay Tarihi: {ipo.spk_approval_date}")

    lines.append("")
    lines.append("--- PAZAR & ARACI ---")

    if ipo.market_segment:
        segment_labels = {
            "yildiz_pazar": "Yildiz Pazar",
            "ana_pazar": "Ana Pazar",
            "alt_pazar": "Alt Pazar",
        }
        lines.append(f"Pazar: {segment_labels.get(ipo.market_segment, ipo.market_segment)}")
    if ipo.lead_broker:
        lines.append(f"Konsorsiyum Lideri: {ipo.lead_broker}")
    if ipo.katilim_endeksi:
        lines.append(f"Katilim Endeksi: {'Uygun' if ipo.katilim_endeksi == 'uygun' else 'Uygun Degil'}")

    lines.append("")
    lines.append("--- EK BILGILER ---")

    if ipo.lock_up_period_days:
        lines.append(f"Lock-up Suresi: {ipo.lock_up_period_days} gun")
    if ipo.price_stability_days:
        lines.append(f"Fiyat Istikrari: {ipo.price_stability_days} gun")

    lines.append("")
    lines.append("--- SIRKET HAKKINDA ---")

    if ipo.company_description:
        lines.append(f"Tanitim: {ipo.company_description}")
    if ipo.fund_usage:
        lines.append(f"Fon Kullanim Hedefleri: {ipo.fund_usage}")

    lines.append("")
    lines.append("--- MALI VERILER ---")

    if ipo.revenue_current_year:
        lines.append(f"Guncel Yil Hasilat: {ipo.revenue_current_year:,.0f} TL")
    if ipo.revenue_previous_year:
        lines.append(f"Onceki Yil Hasilat: {ipo.revenue_previous_year:,.0f} TL")
    if ipo.gross_profit:
        lines.append(f"Brut Kar: {ipo.gross_profit:,.0f} TL")

    # Hasilat buyume orani hesapla
    if ipo.revenue_current_year and ipo.revenue_previous_year and ipo.revenue_previous_year > 0:
        growth = float((ipo.revenue_current_year - ipo.revenue_previous_year) / ipo.revenue_previous_year * 100)
        lines.append(f"Hasilat Buyume: %{growth:.1f}")

    # ── Yeni bolumler ──
    if scenario_table:
        lines.append(scenario_table)

    if historical_context:
        lines.append(historical_context)

    if prospectus_context:
        lines.append(prospectus_context)

    return "\n".join(lines)


# ────────────────────────────────────────────
# AI Rapor Uretimi
# ────────────────────────────────────────────

async def generate_ipo_report(
    ipo,
    historical_context: str = "",
    scenario_table: str = "",
    prospectus_context: str = "",
) -> dict | None:
    """AI ile profesyonel halka arz degerlendirme raporu uretir.

    Args:
        ipo: IPO model instance (tum verileri icermeli)
        historical_context: Gecmis halka arz tahsisat verileri
        scenario_table: Lot senaryo tablosu
        prospectus_context: Izahname analiz sonuclari

    Returns:
        JSON dict veya None (basarisiz)
    """
    api_key = get_settings().ABACUS_API_KEY
    if not api_key:
        logger.error("Abacus API key yok — IPO rapor uretilemedi")
        return None

    context = _build_ipo_context(
        ipo,
        historical_context=historical_context,
        scenario_table=scenario_table,
        prospectus_context=prospectus_context,
    )
    user_message = f"Asagidaki halka arz icin detayli degerlendirme raporu yaz:\n\n{context}"

    try:
        async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
            resp = await client.post(
                _ABACUS_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _AI_MODEL,
                    "messages": [
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": user_message},
                    ],
                    "temperature": 0.15,
                    "max_tokens": 5000,
                },
            )

            if resp.status_code != 200:
                logger.error(
                    "AI IPO rapor hatasi: HTTP %d — %s",
                    resp.status_code,
                    resp.text[:300],
                )
                return None

            data = resp.json()
            content = (
                data.get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
                .strip()
            )

            if not content:
                logger.error("AI bos IPO raporu dondu")
                return None

            # JSON parse — bazen markdown ```json ... ``` ile sarar
            if content.startswith("```"):
                content = content.split("\n", 1)[-1]
                if content.endswith("```"):
                    content = content[:-3].strip()

            report = json.loads(content)

            # Zorunlu alanlar kontrolu
            required_keys = [
                "overall_score", "risk_level", "analysis",
                "how_to_participate", "lot_estimate_explanation",
                "sector_comparison", "recommendation",
            ]
            missing = [k for k in required_keys if k not in report]
            if missing:
                logger.error("AI IPO raporu eksik alanlar: %s", missing)
                return None

            # scenario_table yoksa bos liste ata (frontend guvenli)
            if "scenario_table" not in report:
                report["scenario_table"] = []

            # Skor dogrulama
            score = float(report["overall_score"])
            if not (1.0 <= score <= 10.0):
                logger.warning("AI IPO skor sinir disi: %.1f — clamp ediliyor", score)
                report["overall_score"] = max(1.0, min(10.0, score))

            logger.info(
                "AI IPO raporu uretildi: %s — skor=%.1f, risk=%s, senaryo=%d satir, %d karakter",
                ipo.ticker or ipo.company_name,
                report["overall_score"],
                report["risk_level"],
                len(report.get("scenario_table", [])),
                len(content),
            )
            return report

    except json.JSONDecodeError as e:
        logger.error("AI IPO raporu JSON parse hatasi: %s — content: %s", e, content[:200])
        return None
    except httpx.TimeoutException as e:
        logger.error("AI IPO rapor TIMEOUT hatasi (%d sn): %s — %s", _AI_TIMEOUT, ipo.ticker or ipo.company_name, type(e).__name__)
        return None
    except Exception as e:
        logger.error("AI IPO rapor uretme hatasi: %s — type=%s — ipo=%s", e, type(e).__name__, ipo.ticker or ipo.company_name)
        return None


async def generate_and_save_ipo_report(ipo_id: int) -> bool:
    """IPO raporu uret ve veritabanina kaydet.

    Bu fonksiyon background task olarak calistirilir.
    ipo_service.py'den status degisikligi sonrasi cagirilir.

    Args:
        ipo_id: IPO veritabani ID'si

    Returns:
        True basarili, False basarisiz
    """
    try:
        from app.database import async_session
        from app.models.ipo import IPO
        from sqlalchemy import select
        from sqlalchemy.orm import selectinload

        async with async_session() as session:
            result = await session.execute(
                select(IPO)
                .options(selectinload(IPO.allocations))
                .where(IPO.id == ipo_id)
            )
            ipo = result.scalar_one_or_none()

            if not ipo:
                logger.error("IPO bulunamadi: id=%d", ipo_id)
                return False

            if ipo.ai_report:
                logger.info("IPO zaten rapor var: %s — atlaniyor", ipo.ticker or ipo.company_name)
                return True

            # ── Ek kontekstleri topla ──
            historical_ctx = await _build_historical_allocation_context(session, ipo)
            scenario_ctx = _build_lot_scenario_table(ipo)
            prospectus_ctx = _build_prospectus_context(ipo)

            logger.info(
                "IPO rapor kontekst: %s — historical=%d, scenario=%d, prospectus=%d karakter",
                ipo.ticker or ipo.company_name,
                len(historical_ctx),
                len(scenario_ctx),
                len(prospectus_ctx),
            )

            report = await generate_ipo_report(
                ipo,
                historical_context=historical_ctx,
                scenario_table=scenario_ctx,
                prospectus_context=prospectus_ctx,
            )

            if report is None:
                logger.error("IPO rapor uretilemedi: %s", ipo.ticker or ipo.company_name)
                return False

            ipo.ai_report = json.dumps(report, ensure_ascii=False)
            ipo.ai_report_generated_at = datetime.now(timezone.utc)

            await session.commit()
            logger.info(
                "IPO AI raporu kaydedildi: %s (id=%d) — skor=%.1f",
                ipo.ticker or ipo.company_name,
                ipo_id,
                report["overall_score"],
            )

            # Admin Telegram bildirimi
            try:
                from app.services.admin_telegram import send_admin_notification
                scenario_count = len(report.get("scenario_table", []))
                await send_admin_notification(
                    f"\U0001f916 AI IPO Raporu Uretildi\n\n"
                    f"Sirket: {ipo.company_name}\n"
                    f"Kod: {ipo.ticker or '\u2014'}\n"
                    f"Skor: {report['overall_score']}/10\n"
                    f"Risk: {report['risk_level']}\n"
                    f"Senaryo: {scenario_count} satir\n"
                    f"Izahname: {'Entegre' if prospectus_ctx else 'Yok'}\n"
                    f"Gecmis Veri: {'Var' if historical_ctx else 'Yok'}\n"
                    f"Karakter: {len(ipo.ai_report)}"
                )
            except Exception:
                pass

            return True

    except Exception as e:
        logger.error("IPO rapor kaydetme hatasi (id=%d): %s", ipo_id, e)
        return False
    finally:
        gc.collect()  # Bellek tasarrufu — Render 512MB limiti
