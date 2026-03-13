"""Test: AI rapor metnini PNG gorsele cevir ve masaustune kaydet."""
import sys
import os
import shutil

# Windows console encoding fix
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Backend modüllerini import edebilmek için path ekle
sys.path.insert(0, os.path.dirname(__file__))

from app.services.chart_image_generator import generate_report_image, _parse_report_sections
from app.services.ai_market_report import _extract_short_summary

# Kullanıcının gönderdiği örnek rapor metni
SAMPLE_REPORT = """GÖZLER MERKEZ BANKASI'NDA! Bugün saat 14:00'teki faiz kararı BIST'in yönünü tamamen belirleyecek. Küresel piyasalarda ise petrolün 100$ üzerine çıkması ve ABD-Çin ticaret gerilimleri risk iştahını sınırlıyor.

**BIST 100 (XU100)**
Dün 13.200 seviyesinde %0,19'luk sınırlı bir yükselişle yatay kapanış yapan #BIST100 için bugün asıl hareketlilik öğleden sonra bekleniyor. Saat 14:00'teki TCMB faiz kararına kadar piyasada 13.150 - 13.250 bandında temkinli ve dar bir seyir izlenebilir. Karar metninden çıkacak mesajlar, endeksin ikinci seanstaki yönü için belirleyici olacak.

**ABD Piyasaları**
ABD endeksleri dün geceyi karışık bir seyirle tamamladı (S&P 500 -%0,08, Nasdaq +%0,08). Goldman Sachs'ın Fed'den faiz indirimi beklentisini Eylül'e ertelemesi, gelişen piyasalar üzerindeki baskıyı canlı tutuyor. Bu durum, Borsa İstanbul açılışında sınırlı bir negatif etki yaratabilir.

**Dolar & Altın**
Dolar/TL, kritik faiz kararı öncesi 44,11 seviyesinde gücünü koruyor. Karar sonrası kurda volatilite artışı gözlemlenebilir; yatırımcılar bu duruma hazırlıklı olmalı. Ons altın ise 5150$ seviyesine geriledi. Güçlü dolar ve faiz indirimi beklentilerinin ötelenmesi altında baskı yaratmaya devam ediyor.

**Günün Önemli Gelişmeleri**
• #LINK Bilgisayar'dan Rekor Bedelsiz! Şirketin %4000 oranındaki rekor bedelsiz sermaye artırımının SPK tarafından onaylanması, hissede bugün sert bir fiyat hareketi beklentisi yaratıyor. Açılışta en çok konuşulacak hisselerden biri olması muhtemel.
• Temettü Haberleri: #AKMGY'nin pay başına brüt 12,36 TL'lik temettü teklifi ve #TUPRS'in genel kurulda kâr dağıtımını onaylaması, temettü yatırımcılarının radarında olacak. Bu haberler ilgili hisselere olan ilgiyi artırabilir.
• Enerji ve Jeopolitik Riskler: Petrol fiyatlarının yeniden 100 doları aşması ve ABD'nin 16 ülkeye ticaret soruşturması başlatması, küresel risk iştahını baskılıyor. Enerji ve sanayi şirketleri yakından izlenmeli.
• Bankacılık Sektörü: #SKBNK'nın 350 milyon dolarlık tahvil ihracı yetkisi alması, bankanın yurt dışı kaynak erişimi açısından pozitif bir adım olarak değerlendirilebilir.

**Ekonomik Takvim**
Bugün piyasaların kaderini çizecek en önemli veri, saat 14:00'te açıklanacak olan TCMB Faiz Kararı. Beklentiler ve karar metnindeki mesajlar, Borsa İstanbul'un ikinci seans yönünü tamamen belirleyecek. Ayrıca saat 10:00'da Cari İşlemler Dengesi verisi de takip edilecek.

**Halka Arz Takibi**
Tavan Serilerinde Kritik Gün:
• #GENKM (Gentaş Kimya): 5. işlem gününe giriyor. Dört günlük tavan serisini bugün de sürdürüp sürdüremeyeceği yakından izlenecek. Bu seviyelerde kâr realizasyonu riski artar, hacim takibi kritik olacaktır.
• #LXGYO (Luxera GYO): Bugün 3. işlem gününde. Tavan serisini devam ettirme potansiyeli yüksek olsa da, piyasa genelindeki havaya duyarlı olacaktır.
• #MCARD (Metropal): Dün tavanla başlangıç yapan hisse, bugün 2. tavanını arayacak. Yatırımcı ilgisinin devam etmesi bekleniyor.
• #SVGYO (Savur GYO): 4 günlük tavan serisi sonrası bugün 5. gününde. Serinin devamı beklense de, ilk kâr realizasyonu denemeleri gelebilir. 5,32 TL seviyesi destek olarak izlenmeli.
Tavan Serisi Sona Erenler: Tavan serileri sona eren #EMPAE, #BESTE ve #ATATR gibi hisselerde ise artık volatil seyrin devam etmesi ve hisselerin kendi temel dinamiklerine göre fiyatlanması bekleniyor.

**Bugünün Kritik Noktaları**
1. Saat 14:00 TCMB Faiz Kararı: Günün en kritik anı. Pozisyonlar buna göre şekillenecek.
2. #LINK Hissesi: %4000 bedelsiz onayı sonrası açılışta yaşanacak fiyatlama.
3. #GENKM ve #LXGYO: Tavan serilerinin devam edip etmeyeceği.

Sizce TCMB bugün faiz kararında bir sürpriz yapar mı? Yorumlarınızı bekliyoruz!

Yatırım tavsiyesi değildir.
szalgo.net.tr"""

# 1. Parser test — bölümleri doğru algılıyor mu?
print("=" * 60)
print("PARSER TEST")
print("=" * 60)
parsed = _parse_report_sections(SAMPLE_REPORT)
print(f"\nHook: {parsed['hook'][:100]}...")
print(f"\nSections ({len(parsed['sections'])}):")
for i, sec in enumerate(parsed['sections']):
    print(f"  {i+1}. [{sec['title']}] — {len(sec['lines'])} satır — accent: {sec['color']}")
    for line in sec['lines'][:2]:
        print(f"      → {line[:80]}...")
print(f"\nFooter ({len(parsed['footer_lines'])} satır):")
for fl in parsed['footer_lines']:
    print(f"  → {fl}")

# 2. Görsel üret
print("\n" + "=" * 60)
print("GÖRSEL ÜRETIM")
print("=" * 60)
result_path = generate_report_image(SAMPLE_REPORT, "morning")

if result_path:
    # Masaüstüne kopyala
    desktop = os.path.expanduser("~/Desktop")
    dest = os.path.join(desktop, "acilis_analizi_test.png")
    shutil.copy2(result_path, dest)
    size_kb = os.path.getsize(dest) / 1024
    print(f"\n✅ Görsel başarıyla oluşturuldu!")
    print(f"   Kaynak: {result_path}")
    print(f"   Hedef:  {dest}")
    print(f"   Boyut:  {size_kb:.1f} KB")

    # Boyutları kontrol et
    from PIL import Image
    img = Image.open(dest)
    print(f"   Çözünürlük: {img.width}x{img.height}px")
    img.close()
else:
    print("\n❌ Görsel oluşturulamadı!")

# 3. Tweet özet metni test
print("\n" + "=" * 60)
print("TWEET ÖZET METNİ TEST")
print("=" * 60)
tweet_text = _extract_short_summary(SAMPLE_REPORT, "morning")
print(f"\n{tweet_text}")
print(f"\n--- Karakter sayısı: {len(tweet_text)} ---")
