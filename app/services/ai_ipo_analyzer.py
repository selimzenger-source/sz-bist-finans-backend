"""AI Halka Arz Degerlendirme Raporu Servisi.

IPO dagitima girdiginde (in_distribution) otomatik olarak AI analiz raporu uretir.
Rapor, kucuk yatirimcilar icin profesyonel analist kalitesinde yazilir.

Versiyon 3 — Araştırma Bazlı Kapsamlı İyileştirme:
  - 7 boyutlu ağırlıklı puanlama sistemi (SEBI/ICRA IPO grading modelinden uyarlanmış)
  - Kırmızı bayrak tespiti ve ceza puanları
  - Değerleme analizi: F/K, PD/DD sektör kıyaslaması
  - Anti-halüsinasyon korumaları güçlendirildi
  - Sektör-spesifik analiz derinliği artırıldı
  - Arz yapısı analizi: sermaye artırımı vs ortak satışı oranı

Model: claude-sonnet-4-6 (Abacus RouteLLM uzerinden)

Referanslar:
  - SEBI IPO Grading Framework (5 boyutlu: Business, Financial, Management, Governance, Risk)
  - ICRA Nepal IPO Grading Methodology
  - Brickwork Ratings IPO Assessment
  - BİST Halka Arz Fiyat Performansı Akademik Araştırmaları (2021-2023)
  - AI IPO Analyzer Tools (OneTradeJournal, IPO DOST)
  - SPK 2025 Halka Arz Kriterleri Güncellemesi
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
_AI_MODEL = "claude-sonnet-4-6"
_AI_TIMEOUT = 180  # v3: daha detayli analiz → daha uzun zaman

_SYSTEM_PROMPT = """Sen Turkiye borsasinda (BIST) uzmanlasmis, 20+ yillik deneyime sahip senior halka arz analistisin. SZ Algo Trade platformu icin profesyonel halka arz degerlendirme raporlari yaziyorsun. Hedef kitlen: kucuk bireysel yatirimci.

Analiz metodolojin SEBI/ICRA IPO Grading Framework'une dayanir: cok boyutlu, agirlikli, veri odakli.

=====================================================
PUANLAMA SISTEMI (v3 — AGIRLIKLI COK BOYUTLU SKOR)
=====================================================

overall_score hesabini asagidaki 7 kategoride yap. Her kategorinin MAX puani ve agirligi bellidir.
Toplam 100 puan uzerinden hesapla, sonra 10'a bol (10 uzerinden puan).

KATEGORI 1: FIYATLAMA & DEGERLEME (max 25 puan)
  Halka arz fiyati makul mu? Sektör emsal kıyaslaması nasıl?
  DIKKAT ET:
  - Iskonto orani: %20+ iskonto → 18-25p, %10-20 → 12-17p, %0-10 → 6-11p, prim (iskontosuz) → 0-5p
  - Halka aciklik orani: %20-35 ideal → yuksek puan, <%10 veya >%50 → dusuk puan
  - Piyasa degeri / hasilat carpani makul mu?
  - Arz buyuklugu sektore gore uygun mu?
  - Sektordeki halka acik benzer sirketlerle Fiyat/Kazanc orani kiyaslamasi (bilgi varsa)
  KIRMIZI BAYRAK: Sektorun cok ustunde F/K → agir puan kirimi

KATEGORI 2: FINANSAL SAGLIK (max 20 puan)
  Sirketin mali yapisi ne kadar saglam?
  DIKKAT ET:
  - Hasilat buyumesi: %30+ → 16-20p, %15-30 → 10-15p, %0-15 → 5-9p, negatif → 0-4p
  - Brut kar marji: yuksek ve iyilesen → yuksek puan
  - Karlilık durumu: kar eden vs zarar eden sirket
  - Borc yapisi ve finansal kaldırac
  - Nakit akisi: operasyonel nakit uretebiliyor mu?
  - Hasilat konsantrasyonu: tek musteri/urun bagimlilig → risk
  KIRMIZI BAYRAK: Negatif hasilat buyumesi, surekli zarar, yuksek borc/ozkaynaklar orani

KATEGORI 3: BUYUME POTANSIYELI (max 15 puan)
  Sirketin ve sektorun gelecek 2-5 yillik buyume perspektifi
  DIKKAT ET:
  - Sektorun Turkiye'deki buyume dinamikleri ve trendleri
  - Sirketin pazar payi genisletme kapasitesi
  - Olceklenebilirlik: is modeli buyumeye uygun mu?
  - Fon kullanim hedeflerinin kalitesi: yatirim/buyume odakli mi, borc odeme mi?
  - Yeni urun/pazar/kapasite genisleme planlari
  KIRMIZI BAYRAK: Fon kullaniminin tamami borc odemesi, daralan sektor, tek urun bagimlilig

KATEGORI 4: ARZ YAPISI & TALEP (max 15 puan)
  Dagitim yontemi, arz sekli, lot yapisi yatirimci lehine mi?
  DIKKAT ET:
  - Dagitim yontemi: Esit dagitim → 12-15p (kucuk yatirimci lehine), Karma → 8-11p, Oransal → 3-7p
  - Sermaye artirimi vs ortak satisi ORANI:
    * %100 sermaye artirimi → cok olumlu (para sirkete giriyor)
    * %70+ sermaye artirimi → olumlu
    * %50-50 karisik → notr
    * %70+ ortak satisi → olumsuz (para sirketten cikiyor, mevcut ortaklar paraya ceviriyor)
    * %100 ortak satisi → COK OLUMSUZ, AGIR PENALTI
  - Tahmini kisi basi lot: yuksek lot → cazip
  - Borsada satis vs talep toplama: borsada satis daha erisilebilir
  KIRMIZI BAYRAK: %100 ortak satisi → -10 puan penalti, oransal dagitim kucuk yatirimciyi dezavantajli kilar

KATEGORI 5: SEKTOR KONUMU & REKABET (max 10 puan)
  Sirket sektorde nerede duruyor? Rekabet avantaji var mi?
  DIKKAT ET:
  - Sektorun Turkiye ekonomisindeki yeri ve onemi
  - Sirketin sektor icindeki pazar payi ve konumu
  - Rekabet avantajlari: patent, marka, dagitim agi, teknoloji
  - Halka acik rakiplerle performans kiyaslamasi
  - Sektorun regulasyon riskleri
  - Sektore giris bariyerleri: yuksek bariyer → mevcut oyuncular icin avantaj

KATEGORI 6: YONETIM & KURUMSAL YAPI (max 10 puan)
  Yonetim kalitesi ve kurumsal yonetim yapisi
  DIKKAT ET:
  - Lock-up suresi: 360+ gun → 8-10p (yonetim guveninin gostergesi), 180-360 → 5-7p, <180 → 2-4p, yok → 0-1p
  - Konsorsiyum lideri itibar: buyuk araci kurum → guven sinyali
  - Fiyat istikrari mekanizmasi: varsa → ek puan
  - Katilim endeksine uygunluk: uygunsa → ek puan (faizsiz finans yatirimcisina erisilebilirlik)
  KIRMIZI BAYRAK: Lock-up yok veya cok kisa (<90 gun) → iceriden hizli cikis riski

KATEGORI 7: RISK DEGERLENDIRMESI (max 5 puan — BONUS)
  Risklerin yonetilebilirlik duzeyi
  DIKKAT ET:
  - Makroekonomik riskler (enflasyon, kur, faiz ortami)
  - Regulasyon riskleri (sektor bazinda)
  - Operasyonel riskler (tek lokasyon, tek musteri vs.)
  - Likidite riski: dusuk halka aciklik → islem hacmi sorunu olabilir
  - 0 puan: riskler agir ve yonetilmesi zor
  - 5 puan: riskler sinirli ve yonetilebilir

TOPLAM: Kategori puanlarinin toplami / 10 = overall_score (1.0 - 10.0 arasi)

KIRMIZI BAYRAK PENALTILERI (toplam puandan dusulur):
  - %100 ortak satisi → -10 puan
  - Negatif hasilat buyumesi → -5 puan
  - Lock-up yok veya <90 gun → -5 puan
  - Fon kullaniminin %80'den fazlasi borc odemesi → -5 puan
  - Sektör ortalamasinin 3 katindan fazla Fiyat/Kazanc orani → -5 puan
  - Halka aciklik <%8 → -3 puan
  - %100 ortak satisi + lock-up yok BIRLIKTE → ek -5 puan (cift kirmizi bayrak)

MINIMUM PUAN: 1.0 (puanlar 1.0'in altina dusmez)
MAXIMUM PUAN: 10.0 (puanlar 10.0'i gecmez)

=====================================================
RISK SEVIYESI BELIRLEME
=====================================================

risk_level su kurallara gore belirle:
  - overall_score >= 7.0: "dusuk" — dusuk riskli, guclu temellerle destekleniyor
  - overall_score 5.0 - 6.9: "orta" — orta riskli, hem firsatlar hem dikkat edilmesi gerekenler var
  - overall_score 3.0 - 4.9: "yuksek" — yuksek riskli, onemli olumsuzluklar mevcut
  - overall_score < 3.0: "cok_yuksek" — cok yuksek riskli, ciddi endiseler var

=====================================================
YAZIM KURALLARI (★ COK ONEMLI ★)
=====================================================

HEDEF KITLE: Borsaya yeni baslayan, 18-55 yas arasi bireysel kucuk yatirimci.
Finans egitimi almamis, teknik terimleri bilmiyor. Annesine anlatir gibi yaz.

- SADE TURKCE: Teknik terim kullanma. Kullanmak zorundaysan PARANTEZ ICINDE ACIKLA.
  KOTU: "Sirketin Fiyat/Kazanc orani sektore gore yuksek."
  IYI: "Sirketin borsadaki fiyati, yillik karina gore pahali gorunuyor (yani yatirimci parasinin karsiligini uzun surede alabilir)."
  KOTU: "Cari oran 0.8 ile sinirli."
  IYI: "Kisa vadeli borclarini odeyecek nakdi yetersiz — bu risk demek."
- VERIYE DAYALI: Her iddia rakamla desteklenmeli. "Guclu buyume" degil, "satilar gecen yila gore %42 artmis" yaz.
- DENGELI: Hem firsatlari hem riskleri esit onemle degerlendir. Tek tarafa agirlik verme.
- SOMUT: Her cumle yeni bilgi tasimali. Dolgu paragraf YASAK.
- DEVRIK CUMLE YASAK: Ozne + nesne + yuklem sirasi koru.
- EMOJI YASAK.
- KISALTMA YASAK: NNA, FAVOK, FK, PD/DD gibi kisaltmalar KULLANMA. Tam yazimlarini kullan.
  Ornek: "Fiyat/Kazanc orani" yaz, "FK" veya "F/K" yazma.
  Ornek: "Piyasa Degeri / Defter Degeri orani" yaz, "PD/DD" yazma.
  Ornek: "Faiz, Amortisman ve Vergi Oncesi Kar" yaz, "FAVOK" yazma.
- Turkce hakim: akici, SADE, anlasilir. Profesyonel ama abartili degil.

ANAHTAR KELIMELER & KAVRAMLAR (bunlari analizde MUTLAKA degerlendir):
  Fiyatlama: iskonto orani, piyasa degeri, arz buyuklugu, halka aciklik orani, fiyat/kazanc orani, piyasa degeri/defter degeri, dusuk fiyatlama olgusu
  Finansal: hasilat buyumesi, brut kar marji, faaliyet kari, net kar/zarar, ozkaynak karliligi, borc/ozkaynak orani, nakit akisi, isletme sermayesi
  Arz Yapisi: esit dagitim, oransal dagitim, sermaye artirimi, ortak satisi, bireysel tahsisat, talep toplama, borsada satis
  Risk: lock-up suresi, fiyat istikrari, kur riski, faiz riski, regulasyon riski, hasilat konsantrasyonu, tek musteri bagimliligi
  Sektor: buyume orani, pazar payi, rekabet avantaji, giris bariyeri, olceklenebilirlik, dijitallesme, Turkiye pazar buyuklugu
  Yonetim: konsorsiyum lideri, kurumsal yonetim, bagimsiz denetim, ortaklik yapisi

=====================================================
KAYNAK YASAGI (MUTLAK)
=====================================================

- HICBIR kaynak, referans, link veya web sitesi ADI yazma.
- "Gedik Yatirim", "halkarztakip.com", "KAP", "isyatirim.com.tr", "Bloomberg", "Reuters" gibi kaynak ASLA belirtme.
- "X kaynagina gore", "Y sitesinde", "Z'ye gore", "arastirmalara gore" gibi ifadeler YASAK.
- Veriyi dogrudan yaz, kaynagini soyleme.
- Bu kurallar TUM alanlar icin gecerli.
- Kural ihlali durumunda rapor REDDEDILIR.

=====================================================
RAPOR FORMATI (JSON)
=====================================================

{
  "overall_score": <1.0 ile 10.0 arasi — ondalikli, yukaridaki 7 kategoriden hesaplanmis>,
  "risk_level": "<dusuk | orta | yuksek | cok_yuksek>",
  "score_breakdown": {
    "fiyatlama_degerleme": <0-25 arasi tam sayi>,
    "finansal_saglik": <0-20 arasi tam sayi>,
    "buyume_potansiyeli": <0-15 arasi tam sayi>,
    "arz_yapisi_talep": <0-15 arasi tam sayi>,
    "sektor_konumu_rekabet": <0-10 arasi tam sayi>,
    "yonetim_kurumsal_yapi": <0-10 arasi tam sayi>,
    "risk_degerlendirmesi": <0-5 arasi tam sayi>,
    "kirmizi_bayrak_penalti": <0 veya negatif sayi — ornegin -10>,
    "toplam_ham_puan": <7 kategori toplami + penalti>,
    "kirmizi_bayraklar": ["aciklama1", "aciklama2"]
  },
  "analysis": "<5-8 paragraflik detayli genel degerlendirme>",
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

=====================================================
ALAN DETAYLARI
=====================================================

1. analysis (en az 350 kelime, en fazla 600 kelime):
   PARAGRAF 1 — SIRKET PROFILI & GENEL GORUNTU:
   - Sirketin ne is yaptigi, sektoru, Turkiye'deki konumu
   - Halka arz fiyati ve arz buyuklugu genel degerlendirmesi

   PARAGRAF 2 — FINANSAL ANALIZ:
   - Hasilat trendi: buyuyor mu, ne kadar?
   - Karlılık: brut kar marji, net kar/zarar durumu
   - Hasilat buyume orani hesapla ve sektor ortalamasiyla kiyasla
   - Borc yapisi ve nakit durumu (izahnameden varsa)

   PARAGRAF 3 — FIYATLAMA & DEGERLEME:
   - Halka arz fiyati makul mu?
   - Iskonto orani yeterli mi?
   - Sektordeki halka acik benzer sirketlerle kiyaslama (bilgi varsa)
   - Piyasa Degeri / Hasilat carpaninin makullugu

   PARAGRAF 4 — ARZ YAPISI ANALIZI:
   - Sermaye artirimi mi, ortak satisi mi, karisik mi? ORANI ne?
   - Para sirkete mi giriyor, mevcut ortaklara mi gidiyor?
   - Fon kullanim hedefleri degerlendirmesi (yatirim vs borc odeme)
   - Dagitim yontemi bireysel yatirimci icin avantajli mi?

   PARAGRAF 5 — RISKLER & UYARI ISARETI:
   - Sektore ozgu riskler
   - Sirketin kendi riskleri (hasilat konsantrasyonu, tek musteri, tek pazar)
   - Makroekonomik riskler (enflasyon, kur, faiz)
   - Kirmizi bayraklar varsa acikca belirt

   PARAGRAF 6 (opsiyonel) — IZAHNAME BULGULARI:
   - Izahname analiz verileri sunulmussa, olumlu ve olumsuz bulgulari yorumla
   - Izahnamedeki verileri tekrar etme — YORUMLA, yatirimciya ne anlama geldigini acikla

2. how_to_participate (en az 120 kelime):
   - Dagitim yontemi: esit/oransal/karma ne demek, yatirimci icin ne anlama gelir — somut acikla
   - ADIM ADIM basvuru rehberi
   - Hangi araci kurum/bankadan basvurulur — tum konsorsiyum uyelerini listele
   - Minimum lot ve basvuru saatleri
   - Talep toplama mi, borsada satis mi — fark ne, ne zaman basvurulur
   - Lock-up suresi varsa etkisini acikla (yatirimci icin ne anlama gelir)

3. lot_estimate_explanation (en az 150 kelime):
   - Toplam lot ve bireysel tahsisat oranini belirt
   - Dagitim yontemine gore tahmini kisi basi lot hesapla
   - Eger gecmis halka arz tahsisat verileri verilmisse: MUTLAKA benzer dagitim yontemli halka arzlardaki gerceklesen katilimci sayisi ve lot dagitimini referans al
   - Senaryo tablosunu ACIKLA: "100.000 kisi katilirsa X lot, 500.000 kisi katilirsa Y lot" seklinde
   - Tahmini katilimci sayisini gecmis verilere dayanarak tahmin et
   - Oransal dagitimsa mekanizmayi detayli acikla
   - Son donem katilimci sayisi trendi hakkinda yorum yap

4. scenario_table (JSON dizisi):
   - Sana verilen senaryo tablosundaki degerleri AYNEN kullan
   - Eger senaryo tablosu verilmemisse, toplam lot sayisina gore kendin hesapla:
     participants (formatli string: "100.000"), estimated_lot (tam sayi)
   - 4 satir: 100K, 200K, 300K, 500K katilimci
   - Hesaplama: bireysel_tahsisat_lotu / katilimci_sayisi (esit dagitimda)
   - Oransal dagitimda: "Oransal dagitimda lot tahmini yapilamaz" yazilip bos dizi [] dondurulebilir

5. sector_comparison (en az 150 kelime):
   - Sektorun Turkiye'deki MEVCUT durumu ve buyume perspektifi (2-3 yillik)
   - Sirketin sektordeki KONUMU: lider mi, buyuyen mi, nicoyuncu mu?
   - Halka acik rakiplerle kiyaslama (varsa): Fiyat/Kazanc orani, hasilat buyumesi, kar marji
   - Sektore giris bariyerleri ve sirketin rekabet avantajlari
   - Sektorun regulasyon ortami ve olasi degisiklikler
   - Sektorun dijitallesme, ihracat potansiyeli, doviz pozisyonu gibi ozel dinamikleri

6. recommendation (en az 150 kelime):
   - NET SONUC: Katilmaya deger mi? Neden?
   - PUAN OZETI: "7 boyutlu analizimizde sirket 100 uzerinden X puan aldi" seklinde ozetle
   - Kirmizi bayraklar varsa tekrar vurgula
   - Olumlu yanlar ozetle — en guclu 2-3 madde
   - Strateji onerileri:
     * Kisa vadeli (ilk hafta): Liste primi beklentisi var mi?
     * Orta vadeli (1-6 ay): Lock-up bitisi, sektor gelismelerine gore
     * Uzun vadeli (1+ yil): Sirketin buyume hikayesi surdurulebilir mi?
   - Yatirimciya ozel uyari: Ne kadariyla katilmali? Tek hisseye yogunlasmamali
   - SON CUMLE MUTLAKA: "Bu degerlendirme yatirim tavsiyesi niteliginde degildir."

=====================================================
HALLUSINASYON KORUMASI (MUTLAK — v3 GUCLENDIRILMIS)
=====================================================

- Elinde olmayan veriyi KESINLIKLE UYDURMA. Sadece sana verilen bilgilerden yaz.
- Hasilat buyumesi gibi veriler sana verilmisse HESAPLA ve kullan. Verilmemisse o konuyu ATLA.
- "Veri bulunamadi", "bilgi mevcut degil", "detay yeterli degil" gibi ifadeler YASAK. Bilgi yoksa o konuyu hic yazma.
- Sektordeki rakip sirketlerin F/K, PD/DD gibi degerlerini UYDURMA. Bilmiyorsan "sektordeki benzer sirketlerin degerlemesiyle kiyaslandiginda" gibi genel ifade kullan.
- Spesifik rakam verdiysen kaynagi sana verilmis veriler olmali. Haber, rapor, arastirma sonucu UYDURMA.
- Izahname analiz verileri sunulmussa bunlari YORUMLA — ama tekrar etme, derinlestir.
- SPK mevzuati geregi tum izahnamelerde finansal tablolar, lock-up sureleri, ortaklik yapisi ZORUNLU bulunur. Bunlar "eksik" olamaz — erisemiyorsan o konuyu ATLA.
- Gercekte var olmayan trendleri, istatistikleri, arastirma sonuclarini UYDURMA.

=====================================================
KRITIK
=====================================================

- SADECE JSON formatinda cevap ver. Baska hicbir sey yazma.
- JSON disinda hicbir metin, aciklama, markdown isareti ekleme.
- Tum string degerler Turkce olmali.
- scenario_table icerisindeki participants alani formatli string olmali (orn: "100.000").
- overall_score, score_breakdown'daki puanlarin toplami / 10'a ESIT olmali (yuvarlama farki 0.2'yi gecmemeli).
- Bilmedigin konuda spekulasyon yapma — o konuyu hic yazma, bos birakma."""


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
        "Asagidaki veriler son halka arzlarin GERCEKLESEN bireysel yatirimci sonuclaridir.",
        "Bu verileri lot tahmini ve katilimci sayisi beklentisi icin MUTLAKA kullan:",
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
            sim_parts = [a.participant_count for _, a in similar]
            sim_lots = [float(a.avg_lot_per_person) for _, a in similar]
            sim_avg_p = sum(sim_parts) / len(sim_parts)
            sim_avg_l = sum(sim_lots) / len(sim_lots)
            lines.append(
                f"\nAyni dagitim yontemli ({method_labels.get(ipo.distribution_method, ipo.distribution_method)}) "
                f"ortalama: {sim_avg_p:,.0f} basvuru, kisi basi {sim_avg_l:.1f} lot ({len(similar)} halka arz)"
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
        "Asagidaki bulgular izahname PDF'inden otomatik cikarilmistir.",
        "Bu bulgulari analysis bolumune ENTEGRE ET — tekrar etme, YORUMLA:",
        f"Risk Seviyesi: {pa.get('risk_level', 'bilinmiyor')}",
    ]

    if pa.get("key_risk"):
        lines.append(f"Kritik Risk: {pa['key_risk']}")

    positives = pa.get("positives", [])
    if positives:
        lines.append("\nOlumlu Bulgular:")
        for p in positives[:8]:
            lines.append(f"  + {p}")

    negatives = pa.get("negatives", [])
    if negatives:
        lines.append("\nOlumsuz Bulgular:")
        for n in negatives[:8]:
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
    """IPO verisini AI icin okunabilir formata cevirir.

    v3: Ek hesaplanmis metrikler eklendi:
    - Tahmini piyasa degeri
    - Ortak satisi orani
    - Sermaye artirimi orani
    - Brut kar marji
    """
    lines = []
    lines.append(f"SIRKET: {ipo.company_name}")
    if ipo.ticker:
        lines.append(f"BORSA KODU: {ipo.ticker}")
    if ipo.sector:
        lines.append(f"SEKTOR: {ipo.sector}")

    lines.append("")
    lines.append("=" * 50)
    lines.append("HALKA ARZ TEMEL BILGILERI")
    lines.append("=" * 50)

    if ipo.ipo_price:
        lines.append(f"Halka Arz Fiyati: {ipo.ipo_price} TL")
    if ipo.total_lots:
        lines.append(f"Toplam Lot Sayisi: {ipo.total_lots:,}")
    if ipo.offering_size_tl:
        lines.append(f"Arz Buyuklugu: {ipo.offering_size_tl:,.0f} TL")

    # ── v3: Tahmini piyasa degeri hesapla ──
    if ipo.ipo_price and ipo.total_lots and ipo.public_float_pct:
        try:
            halka_acik_deger = float(ipo.ipo_price) * ipo.total_lots
            piyasa_degeri = halka_acik_deger / (float(ipo.public_float_pct) / 100)
            lines.append(f"Tahmini Piyasa Degeri: {piyasa_degeri:,.0f} TL (halka aciklik oranindan hesaplanmis)")
        except (ZeroDivisionError, TypeError, ValueError):
            pass

    # ── Arz sekli: sermaye artirimi vs ortak satisi ──
    lines.append("")
    lines.append("--- ARZ SEKLI ---")
    if ipo.capital_increase_lots:
        lines.append(f"Sermaye Artirimi: {ipo.capital_increase_lots:,} lot")
    if ipo.partner_sale_lots:
        lines.append(f"Ortak Satisi: {ipo.partner_sale_lots:,} lot")

    # v3: Oranlari hesapla (AI icin kritik veri)
    if ipo.total_lots and ipo.total_lots > 0:
        if ipo.capital_increase_lots:
            cap_pct = float(ipo.capital_increase_lots) / float(ipo.total_lots) * 100
            lines.append(f"Sermaye Artirimi Orani: %{cap_pct:.1f} (toplam arza gore)")
        if ipo.partner_sale_lots:
            partner_pct = float(ipo.partner_sale_lots) / float(ipo.total_lots) * 100
            lines.append(f"Ortak Satisi Orani: %{partner_pct:.1f} (toplam arza gore)")
            if partner_pct >= 80:
                lines.append("!!! UYARI: Arzin buyuk cogunlugu ORTAK SATISI — para sirketten cikiyor, mevcut ortaklar paraya ceviriyor !!!")
            elif partner_pct >= 50:
                lines.append("DIKKAT: Arzin yarısından fazlası ortak satışı — fon kullanimini dikkatli degerlendir")

    if ipo.public_float_pct:
        lines.append(f"Halka Aciklik Orani: %{ipo.public_float_pct}")
    if ipo.discount_pct:
        lines.append(f"Iskonto Orani: %{ipo.discount_pct}")

    lines.append("")
    lines.append("--- DAGITIM & KATILIM ---")

    if ipo.distribution_method:
        method_labels = {
            "esit": "Esit Dagitim (her basvurana esit lot — kucuk yatirimci lehine)",
            "bireysele_esit": "Bireysel Yatirimciya Esit (bireysel icin esit dagitim)",
            "tamami_esit": "Tamami Esit (tum gruplara esit)",
            "oransal": "Oransal Dagitim (yatirim tutarina orantili — buyuk yatirimci avantajli)",
            "karma": "Karma Dagitim (esit + oransal kombinasyonu)",
        }
        lines.append(f"Dagitim Yontemi: {method_labels.get(ipo.distribution_method, ipo.distribution_method)}")
    if ipo.distribution_description:
        lines.append(f"Dagitim Aciklamasi: {ipo.distribution_description}")
    if ipo.participation_method:
        method_labels = {
            "talep_toplama": "Talep Toplama (araci kurum uzerinden basvuru)",
            "borsada_satis": "Borsada Satis (dogrudan borsa uzerinden, daha erisilebilir)",
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
            "yildiz_pazar": "Yildiz Pazar (en prestijli — buyuk sirketler)",
            "ana_pazar": "Ana Pazar (orta olcekli sirketler)",
            "alt_pazar": "Alt Pazar (kucuk sirketler — daha riskli)",
        }
        lines.append(f"Pazar: {segment_labels.get(ipo.market_segment, ipo.market_segment)}")
    if ipo.lead_broker:
        lines.append(f"Konsorsiyum Lideri: {ipo.lead_broker}")
    if ipo.katilim_endeksi:
        lines.append(f"Katilim Endeksi: {'Uygun (faizsiz finans yatirimcilarina acik)' if ipo.katilim_endeksi == 'uygun' else 'Uygun Degil'}")

    # Konsorsiyum uyeleri
    if hasattr(ipo, 'brokers') and ipo.brokers:
        broker_names = [b.broker_name for b in ipo.brokers if not b.is_rejected]
        rejected_names = [b.broker_name for b in ipo.brokers if b.is_rejected]
        if broker_names:
            lines.append(f"Basvuru Yapilabilecek Kurumlar: {', '.join(broker_names)}")
        if rejected_names:
            lines.append(f"Basvuru YAPILAMAZ: {', '.join(rejected_names)}")

    lines.append("")
    lines.append("--- EK BILGILER & YONETIM ---")

    if ipo.lock_up_period_days:
        lines.append(f"Lock-up Suresi: {ipo.lock_up_period_days} gun")
        if ipo.lock_up_period_days >= 360:
            lines.append("  → Uzun lock-up: yonetimin sirkete guveni yuksek sinyali")
        elif ipo.lock_up_period_days < 90:
            lines.append("  → UYARI: Kisa lock-up — iceriden hizli cikis riski")
    else:
        lines.append("Lock-up Suresi: BELIRTILMEMIS — bu bir risk sinyali olabilir")

    if ipo.price_stability_days:
        lines.append(f"Fiyat Istikrari: {ipo.price_stability_days} gun (fiyat destegi mekanizmasi)")

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

    # v3: Hesaplanmis mali metrikler
    if ipo.revenue_current_year and ipo.revenue_previous_year and ipo.revenue_previous_year > 0:
        growth = float((ipo.revenue_current_year - ipo.revenue_previous_year) / ipo.revenue_previous_year * 100)
        lines.append(f"Hasilat Buyume Orani: %{growth:.1f}")
        if growth < 0:
            lines.append("!!! UYARI: NEGATIF HASILAT BUYUMESI — gerileme sinyali !!!")
        elif growth > 50:
            lines.append("  → Guclu organik buyume sinyali")

    if ipo.gross_profit and ipo.revenue_current_year and ipo.revenue_current_year > 0:
        gross_margin = float(ipo.gross_profit / ipo.revenue_current_year * 100)
        lines.append(f"Brut Kar Marji: %{gross_margin:.1f}")

    # ── Ek kontekstler ──
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
    """AI ile profesyonel halka arz degerlendirme raporu uretir (v3).

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
    user_message = (
        f"Asagidaki halka arz icin 7 boyutlu agirlikli puanlama sistemiyle "
        f"detayli degerlendirme raporu yaz. Her kategori puanini ayri hesapla, "
        f"kirmizi bayraklari tespit et, sonra toplam puani 10 uzerinden ver:\n\n{context}"
    )

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
                    "temperature": 0.12,  # v3: biraz daha deterministik
                    "max_tokens": 6500,   # v3: daha detayli rapor
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

            # score_breakdown yoksa bos dict ata
            if "score_breakdown" not in report:
                report["score_breakdown"] = {}

            # Skor dogrulama
            score = float(report["overall_score"])
            if not (1.0 <= score <= 10.0):
                logger.warning("AI IPO skor sinir disi: %.1f — clamp ediliyor", score)
                report["overall_score"] = max(1.0, min(10.0, score))

            # v3: score_breakdown tutarlılık kontrolü
            breakdown = report.get("score_breakdown", {})
            if breakdown:
                ham_puan = breakdown.get("toplam_ham_puan")
                if ham_puan is not None:
                    expected_score = max(1.0, min(10.0, ham_puan / 10))
                    actual_score = report["overall_score"]
                    if abs(expected_score - actual_score) > 0.5:
                        logger.warning(
                            "Score breakdown tutarsizligi: ham=%s → beklenen=%.1f, gelen=%.1f — duzeltiliyor",
                            ham_puan, expected_score, actual_score
                        )
                        report["overall_score"] = round(expected_score, 1)

            # v3: risk_level tutarlılık kontrolü
            final_score = report["overall_score"]
            expected_risk = (
                "dusuk" if final_score >= 7.0
                else "orta" if final_score >= 5.0
                else "yuksek" if final_score >= 3.0
                else "cok_yuksek"
            )
            if report["risk_level"] not in ("dusuk", "orta", "yuksek", "cok_yuksek"):
                report["risk_level"] = expected_risk
            elif report["risk_level"] != expected_risk:
                logger.info(
                    "Risk level uyumsuzlugu: skor=%.1f → beklenen=%s, gelen=%s — AI'in secimi korunuyor",
                    final_score, expected_risk, report["risk_level"]
                )

            # v3: Kaynak yasakı kontrolü
            for field in ["analysis", "how_to_participate", "lot_estimate_explanation",
                          "sector_comparison", "recommendation"]:
                text = report.get(field, "")
                banned_sources = [
                    "gedik.com", "halkarztakip", "isyatirim", "kap.org",
                    "bloomberg", "reuters", "investing.com", "bigpara",
                    "Gedik Yatirim", "Gedik Yatırım"
                ]
                for source in banned_sources:
                    if source.lower() in text.lower():
                        logger.warning("KAYNAK IHLALI tespit edildi: '%s' — alan: %s", source, field)
                        # Kaynagi temizle
                        import re
                        text = re.sub(
                            rf'\b{re.escape(source)}\b',
                            '',
                            text,
                            flags=re.IGNORECASE
                        )
                        report[field] = text

            logger.info(
                "AI IPO raporu v3 uretildi: %s — skor=%.1f, risk=%s, senaryo=%d satir, "
                "breakdown=%s, kirmizi_bayrak=%s, %d karakter",
                ipo.ticker or ipo.company_name,
                report["overall_score"],
                report["risk_level"],
                len(report.get("scenario_table", [])),
                bool(report.get("score_breakdown")),
                report.get("score_breakdown", {}).get("kirmizi_bayraklar", []),
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


async def generate_and_save_ipo_report(ipo_id: int, force: bool = False) -> bool:
    """IPO raporu uret ve veritabanina kaydet.

    Bu fonksiyon background task olarak calistirilir.
    ipo_service.py'den status degisikligi sonrasi cagirilir.

    Args:
        ipo_id: IPO veritabani ID'si
        force: True ise mevcut raporu silip yeniden uretir (admin tetiklemesi)

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
                .options(
                    selectinload(IPO.allocations),
                    selectinload(IPO.brokers),
                )
                .where(IPO.id == ipo_id)
            )
            ipo = result.scalar_one_or_none()

            if not ipo:
                logger.error("IPO bulunamadi: id=%d", ipo_id)
                return False

            if ipo.ai_report and not force:
                logger.info("IPO zaten rapor var: %s — atlaniyor (force=%s)", ipo.ticker or ipo.company_name, force)
                return True

            if force and ipo.ai_report:
                logger.info("IPO mevcut rapor SILINIYOR (force mode): %s", ipo.ticker or ipo.company_name)
                ipo.ai_report = None
                ipo.ai_report_generated_at = None
                await session.flush()

            # ── Ek kontekstleri topla ──
            historical_ctx = await _build_historical_allocation_context(session, ipo)
            scenario_ctx = _build_lot_scenario_table(ipo)
            prospectus_ctx = _build_prospectus_context(ipo)

            logger.info(
                "IPO rapor v3 kontekst: %s — historical=%d, scenario=%d, prospectus=%d, brokers=%d karakter",
                ipo.ticker or ipo.company_name,
                len(historical_ctx),
                len(scenario_ctx),
                len(prospectus_ctx),
                len(ipo.brokers) if hasattr(ipo, 'brokers') and ipo.brokers else 0,
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

            breakdown = report.get("score_breakdown", {})
            logger.info(
                "IPO AI raporu v3 kaydedildi: %s (id=%d) — skor=%.1f, risk=%s, "
                "fiyatlama=%s, finansal=%s, buyume=%s, arz=%s, sektor=%s, yonetim=%s, risk_bonus=%s, penalti=%s",
                ipo.ticker or ipo.company_name,
                ipo_id,
                report["overall_score"],
                report["risk_level"],
                breakdown.get("fiyatlama_degerleme", "?"),
                breakdown.get("finansal_saglik", "?"),
                breakdown.get("buyume_potansiyeli", "?"),
                breakdown.get("arz_yapisi_talep", "?"),
                breakdown.get("sektor_konumu_rekabet", "?"),
                breakdown.get("yonetim_kurumsal_yapi", "?"),
                breakdown.get("risk_degerlendirmesi", "?"),
                breakdown.get("kirmizi_bayrak_penalti", 0),
            )

            # Admin Telegram bildirimi
            try:
                from app.services.admin_telegram import send_admin_notification
                scenario_count = len(report.get("scenario_table", []))
                red_flags = breakdown.get("kirmizi_bayraklar", [])
                red_flag_text = "\n".join(f"  - {rf}" for rf in red_flags) if red_flags else "Yok"

                await send_admin_notification(
                    f"\U0001f916 AI IPO Raporu v3 {'(FORCE)' if force else ''}\n\n"
                    f"Sirket: {ipo.company_name}\n"
                    f"Kod: {ipo.ticker or '\u2014'}\n"
                    f"Skor: {report['overall_score']}/10\n"
                    f"Risk: {report['risk_level']}\n"
                    f"\nPuan Dagilimi:\n"
                    f"  Fiyatlama: {breakdown.get('fiyatlama_degerleme', '?')}/25\n"
                    f"  Finansal: {breakdown.get('finansal_saglik', '?')}/20\n"
                    f"  Buyume: {breakdown.get('buyume_potansiyeli', '?')}/15\n"
                    f"  Arz Yapisi: {breakdown.get('arz_yapisi_talep', '?')}/15\n"
                    f"  Sektor: {breakdown.get('sektor_konumu_rekabet', '?')}/10\n"
                    f"  Yonetim: {breakdown.get('yonetim_kurumsal_yapi', '?')}/10\n"
                    f"  Risk Bonus: {breakdown.get('risk_degerlendirmesi', '?')}/5\n"
                    f"  Penalti: {breakdown.get('kirmizi_bayrak_penalti', 0)}\n"
                    f"\nKirmizi Bayraklar:\n{red_flag_text}\n"
                    f"\nSenaryo: {scenario_count} satir\n"
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
