"""AI Halka Arz Degerlendirme Raporu Servisi.

IPO dagitima girdiginde (in_distribution) otomatik olarak AI analiz raporu uretir.
Rapor, kucuk yatirimcilar icin profesyonel analist kalitesinde yazilir.

Model: claude-sonnet-4-5 (Abacus RouteLLM uzerinden) — en ust seviye analiz
"""

import json
import logging
from datetime import datetime, timezone

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

# ────────────────────────────────────────────
# Sabitler
# ────────────────────────────────────────────

_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_AI_MODEL = "claude-sonnet-4-5"
_AI_TIMEOUT = 60  # Detayli analiz uzun surebilir

_SYSTEM_PROMPT = """Sen Turkiye'nin en deneyimli halka arz analistlerinden birisin. SZ Algo Trade platformu icin profesyonel halka arz degerlendirme raporlari yaziyorsun.

YAZIM TARZI:
- Profesyonel ama anlasilir. Kucuk yatirimci da buyuk yatirimci da rahatca okumali.
- Veriye dayali, somut, net ifadeler. Muglak cumle kurma.
- Hem firsat hem riskleri dengeli degerlendir — tarafsiz ol.
- Sektore ozgu derinlikli analiz yap, yuzeysel olma.
- Turkce dil hakimiyetin muhtesemel olmali. Akici, etkileyici, profesyonel.
- Emoji kullanma — ciddi bir analiz raporu bu.

RAPOR FORMATI (JSON):
{
  "overall_score": <1.0 ile 10.0 arasi puan — ondalikli>,
  "risk_level": "<dusuk | orta | yuksek | cok_yuksek>",
  "analysis": "<3-5 paragraflik detayli genel degerlendirme — sirketin guclu ve zayif yanlari, halka arz fiyatlandirmasi, piyasa kosullari, sektordeki konumu>",
  "how_to_participate": "<Basvuru nasil yapilir — hangi araci kurum, internet bankaciligi, mobil uygulama uzerinden adim adim aciklama. Dagitim yontemi, min lot, basvuru saatleri dahil>",
  "lot_estimate_explanation": "<Tahmini kisi basi lot, neden bu rakam, gecmis benzer halka arzlarla karsilastirma. Oransal dagitimsa bunu acikla>",
  "sector_comparison": "<Sirketin sektordeki konumu, rakipleri, F/K, PD/DD bazli kiyaslama — yoksa sektorun genel durumu>",
  "recommendation": "<Sonuc paragrafi — katilmaya deger mi, hangi stratejiler izlenebilir, dikkat edilmesi gerekenler. 'Yatirim tavsiyesi degildir' notu ile bitir>"
}

PUANLAMA KRITERLERI:
- 1-3: Cok riskli, katilmamak daha mantikli
- 4-5: Orta risk, dikkatli olunmali
- 6-7: Makul, sektore gore fiyatlandirma uygun
- 8-9: Guzel firsat, temel gostergeler olumlu
- 10: Istisnai — nadiren verilmeli

ONEMLI:
- SADECE JSON formatinda cevap ver, baska hicbir sey yazma
- JSON disinda hicbir metin, aciklama veya isaret ekleme
- Tum string degerler Turkce olmali
- Yatirim tavsiyesi verme — "Bu degerlendirme yatirim tavsiyesi niteliginde degildir" cümlesi recommendation sonunda MUTLAKA olsun
- analysis en az 200 kelime olmali — detayli ve derinlikli yaz
- recommendation en az 80 kelime olmali"""


def _build_ipo_context(ipo) -> str:
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

    return "\n".join(lines)


async def generate_ipo_report(ipo) -> dict | None:
    """AI ile profesyonel halka arz degerlendirme raporu uretir.

    Args:
        ipo: IPO model instance (tum verileri icermeli)

    Returns:
        JSON dict veya None (basarisiz)
    """
    api_key = get_settings().ABACUS_API_KEY
    if not api_key:
        logger.error("Abacus API key yok — IPO rapor uretilemedi")
        return None

    context = _build_ipo_context(ipo)
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
                    "temperature": 0.2,
                    "max_tokens": 4000,
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
                # ```json\n{...}\n``` formatini temizle
                content = content.split("\n", 1)[-1]  # ilk satiri at
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

            # Skor dogrulama
            score = float(report["overall_score"])
            if not (1.0 <= score <= 10.0):
                logger.warning("AI IPO skor sinir disi: %.1f — clamp ediliyor", score)
                report["overall_score"] = max(1.0, min(10.0, score))

            logger.info(
                "AI IPO raporu uretildi: %s — skor=%.1f, risk=%s, %d karakter",
                ipo.ticker or ipo.company_name,
                report["overall_score"],
                report["risk_level"],
                len(content),
            )
            return report

    except json.JSONDecodeError as e:
        logger.error("AI IPO raporu JSON parse hatasi: %s — content: %s", e, content[:200])
        return None
    except Exception as e:
        logger.error("AI IPO rapor uretme hatasi: %s", e)
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

        async with async_session() as session:
            result = await session.execute(
                select(IPO).where(IPO.id == ipo_id)
            )
            ipo = result.scalar_one_or_none()

            if not ipo:
                logger.error("IPO bulunamadi: id=%d", ipo_id)
                return False

            if ipo.ai_report:
                logger.info("IPO zaten rapor var: %s — atlanıyor", ipo.ticker or ipo.company_name)
                return True

            report = await generate_ipo_report(ipo)

            if report is None:
                logger.error("IPO rapor uretilemedi: %s", ipo.ticker or ipo.company_name)
                return False

            ipo.ai_report = json.dumps(report, ensure_ascii=False)
            ipo.ai_report_generated_at = datetime.now(timezone.utc)

            await session.commit()
            logger.info(
                "IPO AI raporu kaydedildi: %s (id=%d)",
                ipo.ticker or ipo.company_name,
                ipo_id,
            )

            # Admin Telegram bildirimi
            try:
                from app.services.admin_telegram import send_admin_notification
                await send_admin_notification(
                    f"🤖 AI IPO Raporu Uretildi\n\n"
                    f"Sirket: {ipo.company_name}\n"
                    f"Kod: {ipo.ticker or '—'}\n"
                    f"Skor: {report['overall_score']}/10\n"
                    f"Risk: {report['risk_level']}\n"
                    f"Karakter: {len(ipo.ai_report)}"
                )
            except Exception:
                pass

            return True

    except Exception as e:
        logger.error("IPO rapor kaydetme hatasi (id=%d): %s", ipo_id, e)
        return False
