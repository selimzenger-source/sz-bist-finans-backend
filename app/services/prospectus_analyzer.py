"""İzahname (Prospektüs) PDF Analiz Servisi — v1.

Çalışma mantığı:
1. PDF URL'den indir (httpx)
2. pdfplumber ile tam metin çıkar
3. Risk faktörleri + önemli bölümleri tespit et
4. Claude claude-sonnet-4-5 (Abacus) ile derinlikli analiz
5. Hallüsinasyon koruması: AI sadece PDF'ten alıntı yapabilir, uyduraMAZ
6. Sonucu DB'ye kaydet + görsel üret + tweet at

NOT: PDF 140+ sayfa olabilir. Strateji:
  - Tüm metni çıkar (chunk'a böl gerekirse)
  - "Risk Faktörleri" bölümünü öncelikli analiz et
  - Finansal özetler + dipnotları yakala
"""

import asyncio
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import pdfplumber

from app.config import get_settings

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Sabitler
# ─────────────────────────────────────────────────────────────

_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_AI_MODEL   = "claude-sonnet-4-6"
_GEMINI_MODEL = "gemini-2.5-pro"
_CLAUDE_MODEL = "claude-sonnet-4-20250514"
_AI_TIMEOUT = 120   # Derin analiz için daha fazla süre

# PDF çıkarımında max karakter (büyük PDF'ler için kırp)
_MAX_PDF_CHARS = 180_000    # ~100k token — güvenli

# Risk bölümü anahtar kelimeleri (Türkçe izahname)
_RISK_KEYWORDS = [
    "risk faktörleri", "riskler", "risk factors",
    "önemli riskler", "genel risk", "yasal riskler",
    "düzenleyici riskler", "lisans", "ruhsat", "izin",
    "hukuki", "dava", "uyuşmazlık", "bağımlılık",
    "yoğunlaşma", "tek müşteri", "kilit personel",
    "going concern", "sürekliliğe", "zarar", "borç",
]

_FINANCE_KEYWORDS = [
    "finansal durum", "özet finansal", "mali tablo",
    "gelir tablosu", "nakit akış", "özkaynak",
    "hasılat", "net kâr", "brüt kâr", "ebitda",
    "bilanço", "kâr veya zarar", "kar veya zarar",
    "finansal bilgiler", "finansal tablo",
    "dönem kârı", "dönem karı", "brüt kar",
    "toplam varlıklar", "toplam yükümlülükler",
    "kısa vadeli", "uzun vadeli",
    "faaliyet kârı", "faaliyet geliri",
    "satış gelirleri", "net satışlar",
    # v2: Araştırma sonucu eklenen eksik keyword'ler
    "bağımsız denetim", "denetim raporu", "denetçi görüşü",
    "kar dağıtım", "temettü", "kâr payı",
    "değerleme", "fiyat kazanç", "piyasa değeri",
    "ilişkili taraf", "related party",
    "faiz gideri", "finansman gideri",
    "stok devir", "alacak devir", "ticari alacak",
    "amortisman", "yatırım harcaması", "capex",
    "serbest nakit akışı", "free cash flow",
]

# Faaliyet alanı / şirket detayları anahtar kelimeleri
_ACTIVITY_KEYWORDS = [
    "faaliyet alanı", "faaliyet konusu", "ana faaliyet",
    "ürün", "hizmet", "üretim", "üretim kapasitesi",
    "tesis", "fabrika", "şube", "mağaza",
    "müşteri", "pazar", "sektör", "rekabet",
    "çalışan", "personel", "istihdam", "insan kaynakları",
    "coğrafi dağılım", "ihracat", "ithalat",
    "kuruluş", "tarihçe", "şirketin konusu",
    "şirketin amacı", "ticaret sicil",
]

# Mal varlıkları / fiziksel aktifler anahtar kelimeleri
_ASSET_KEYWORDS = [
    "maddi duran varlık", "maddi duran varlik",
    "gayrimenkul", "taşınmaz", "tasinmaz", "arsa", "arazi",
    "bina", "depo", "fabrika binası",
    "makine", "ekipman", "tesis", "teçhizat",
    "yatırım amaçlı gayrimenkul", "taşıt", "araç",
    "maddi olmayan duran varlık", "patent", "lisans hakkı",
    "marka", "şerefiye", "goodwill",
    "kullanım hakkı", "kira", "finansal kiralama",
    "stok", "envanter", "hammadde",
]

# ── Prompt Override Mekanizması ──
_custom_system_prompt: str | None = None


def get_system_prompt() -> str:
    """Aktif system prompt'u döndürür (custom varsa onu, yoksa default)."""
    return _custom_system_prompt if _custom_system_prompt is not None else _DEFAULT_SYSTEM_PROMPT


def set_system_prompt(new_prompt: str | None) -> None:
    """System prompt'u günceller. None gönderilirse default'a döner."""
    global _custom_system_prompt
    _custom_system_prompt = new_prompt
    logger.info("Prospectus Analyzer system prompt %s", "güncellendi" if new_prompt else "default'a döndürüldü")


def get_default_system_prompt() -> str:
    """Default (hardcoded) system prompt'u döndürür."""
    return _DEFAULT_SYSTEM_PROMPT


# ─────────────────────────────────────────────────────────────
# SYSTEM PROMPT — Hallüsinasyon koruması + Yüksek kalite
# ─────────────────────────────────────────────────────────────

_DEFAULT_SYSTEM_PROMPT = """Sen Türkiye sermaye piyasaları uzmanı, kıdemli bir halka arz analistisin. Görevin: izahname PDF'inden küçük yatırımcının para yatırma/yatırmama kararını etkileyecek SOMUT, KRİTİK bilgileri çıkarmak.

HEDEF KİTLE: Bireysel küçük yatırımcılar. Finans jargonu bilmeyebilirler. Her terimi açık ve anlaşılır yaz.

SEN BİR ARAŞTIRMA RAPORU YAZIYORSUN — köşe yazısı DEĞİL. Her cümle bilgi taşımalı.

═══ MUTLAK YASAKLAR ═══
• HALLÜSINASYON: PDF'te yazmayan bilgiyi uydurma. Emin değilsen o maddeyi YAZMA.
• GEÇİŞTİRME: "...olabilir", "...muhtemeldir", "...beklenebilir" gibi belirsiz ifadeler YASAK.
• DOLGU MADDE: Bilgi değeri sıfır olan maddeler YASAK. Her madde yatırımcıya yeni bilgi vermeli.
• DEVRİK CÜMLE: Türkçe düzgün, özne-yüklem uyumlu olmalı. Devrik/karmaşık cümle kurma.
• KISALTMA YASAK: Hiçbir kısaltma kullanma. Tam açık yaz:
  - NNA → "Net Nakit Akışı", FAVÖK → "Faiz, Amortisman Öncesi Kâr", FK → "Fiyat/Kazanç"
  - PD/DD → "Piyasa Değeri / Defter Değeri", ÖS → "Özsermaye", YP → "Yabancı Para"
  - CAGR → "Yıllık Bileşik Büyüme Oranı", BDDK → tam yazılabilir (kurum adı)
  - SA → "Sermaye Artırımı", OS → "Ortak Satışı", HA → "Halka Arz"
  Kısaca: Okuyucu hiçbir kısaltmayı açmak zorunda kalmamalı.

• "BULUNAMADI" / "EKSİK" YASAĞI (★ EN KRİTİK KURAL ★):
  ASLA bu tür ifadeler kullanma:
  - "bulunamadı", "eksik", "belirtilmemiş", "yer almıyor", "tespit edilemedi"
  - "veri yok", "detay yok", "bilgi mevcut değil", "açıklama yapılmamış"
  - "fon kullanım yerleri yer almıyor", "finansal tablo bulunamadı"
  - "tam değerlendirme yapılamıyor", "yeterli bilgi yok"

  ★★★ ÇOK ÖNEMLİ UYARI ★★★
  SPK mevzuatı gereği TÜM izahnamelerde şunlar ZORUNLU olarak bulunur:
  - Net kâr/zarar, bilanço, gelir tablosu, nakit akış tablosu
  - Fon kullanım yerleri (oran ve tutar bazında)
  - Ortakların hisse satış yasağı (lock-up) süreleri ve taahhütleri
  - Halka arz sonrası pay dağılım oranları (tahsisat)
  - Borç yapısı, özkaynak bilgileri, ortaklık yapısı

  Bu bilgiler PDF'te MUTLAKA vardır. Eğer metin çıkarımında bulamadıysan,
  bu senin okuyamadığın anlamına gelir — bu BİLGİNİN YOK OLDUĞU anlamına GELMEZ.
  "Eksik" veya "belirtilmemiş" deme. O maddeyi ATLA — yokluğunu rapor ETME.
  Sakın "veriler eksik" diye olumsuz madde yazma — bu HER ZAMAN YANLIŞLIK olur.
  "Fon kullanım yerleri yer almıyor" gibi bir madde yazmak BÜYÜK HATADIR
  çünkü SPK izahnamesinde fon kullanımı ZORUNLU alan.

  ★ ÖZEL DURUM: Şirketin farklı projeleri, farklı lokasyonlardaki yatırımları,
    gelecek planları, yeni projeler, kapasite genişleme vb. bilgileri de PDF'te
    olabilir. Bunları göremediğin için "yok" deme. O konuyu atla.

• YAZIM HATASI: Her kelimeyi kontrol et. OCR kaynaklı bozukluklar olabilir — düzelt:
  "üçlü" → "güçlü", "ıasılat" → "hasılat" gibi bozuk kelimeleri düzgün yaz.
  Cümle bozukluğu YAPMA — her cümleyi yaz, oku, kontrol et.

═══ YASAK MADDE ÖRNEKLERİ (bunları asla yazma) ═══
✗ "Kayıtlı sermaye tavanı X TL olarak belirlenmiştir" → HERKESİN izahnamesinde var, bilgi değeri YOK
✗ "Piyasa koşullarına bağlı riskler mevcuttur" → Her şirket için geçerli, spesifik değil
✗ "Şirketin büyüme potansiyeli bulunmaktadır" → Kanıtsız genel yorum
✗ "Halka arz geliri işletme sermayesine kullanılacaktır" → Ne kadar, ne için? Detay yoksa yazma
✗ "Sektörde rekabetin artması risk oluşturabilir" → Her sektörde rekabet var
✗ "Yatırımcılar dikkatli değerlendirmelidir" → Tavsiye değil analiz yap
✗ "Finansal tablo bulunamadı" → Bilgi yoksa o maddeyi YAZMA
✗ "Detaylı veri mevcut değil" → Yokluğu rapor etme, var olanı yaz

═══ DİL VE ANLAŞILIRLIK (★ ÇOK ÖNEMLİ ★) ═══
• HEDEF KİTLE: Borsaya yeni başlamış, 18-55 yaş arası bireysel küçük yatırımcı.
  Finans eğitimi almamış, teknik terimleri bilmiyor. Annesine anlatır gibi yaz.
• KISA CÜMLE KURAL: Her madde TAM BİR CÜMLE olmalı. Yarım cümle BIRAKMA.
  Cümle sığmıyorsa KISALT — bilgiyi özet ver, detaya girme.
  KÖTÜ: "Halka arz yapısı karma yöntemdir: 208.4 milyon TL nominal değerli pay sermaye artırımı yoluyla şirkete, 87 milyon TL nominal değerli pay ise..."  (YARIM KALDI!)
  İYİ: "Halka arz gelirinin %70'i şirkete, %30'u mevcut ortaklara gidecek"
• TEKNİK TERİM YASAK — günlük dille yaz:
  - "EBITDA marjı %18" → "Vergiden ve amortisman giderlerinden önceki kâr oranı %18"
  - "Cari oran 0.8" → "Kısa vadeli borçlarını ödeyecek parası yetersiz (0.8 — 1'in altı riskli)"
  - "Lock-up 365 gün" → "Ortaklar 1 yıl boyunca hisse satamayacak"
  - "Nominal değerli pay" → "pay" veya "hisse" de yeter
  - "Sermaye artırımı yoluyla" → "şirkete girecek para"
  - "Ortak satışı" → "mevcut ortakların cebine gidecek para"
  - "Bağımsız değerleme" → "uzman kuruluşun biçtiği değer"
• RAKAM FORMATI: 1.200.000.000 TL → "1.2 milyar TL" yaz. Büyük sayıları okunaklı yaz.
• KARŞILAŞTIRMA YAP: "Sektör ortalaması %12 iken bu şirket %28" gibi — bağlam ver.
• MAX 180 KARAKTER: Her madde en fazla 180 karakter. Sığmıyorsa bilgiyi özetle.
  Cümlenin ortasında kesilmesi ASLA kabul edilemez — kısa ve tam cümle yaz.

═══ İYİ MADDE ÖRNEKLERİ (bu kalitede yaz) ═══
✓ "Son 3 yılda satışlar her yıl ortalama %78 büyümüş — sektör ortalamasının 6 katı"
✓ "Toplam borcun %81'i kısa vadeli; 94 milyon TL borç, sadece 12 milyon TL nakit var"
✓ "Halka arzın %67'si sermaye artırımı (şirkete), %33'ü ortak satışı (mevcut ortaklara) — fon dağılımına dikkat"
✓ "En büyük müşteri toplam satışların %47'sini oluşturuyor — tek müşteriye aşırı bağımlılık"
✓ "Ödeme kuruluşu lisansına sahip — bu lisansı kaybederse faaliyetleri durur"

═══ ANALİZ ADIMLARI (sırayla ve adım adım tara — hiçbirini ATLAMA) ═══

1. ŞİRKET FAALİYETLERİ VE SEKTÖR (en az 2 madde):
   • Şirketin ana faaliyet alanı ne? Ne üretiyor, ne satıyor, ne hizmet veriyor?
   • Hangi sektörde, kaç yıldır faaliyet gösteriyor?
   • Müşteri profili: bireysel mi, kurumsal mı, kamu mu? Hedef pazar neresi?
   • Üretim kapasitesi, tesis sayısı, coğrafi konum — varsa belirt
   • Çalışan sayısı (toplam, beyaz yaka, mavi yaka) — rakam varsa yaz
   • Sektördeki konumu: pazar payı, rakiplere göre büyüklük
   • NOT: Bu bölümde SADECE PDF'te yazan somut bilgileri ver.
     Şirketin ne iş yaptığını bilmiyorsan veya PDF'ten çıkaramadıysan bu bölümü ATLA.

2. FİNANSAL SAĞLIK (en az 3 madde):
   • Satışlar, net kâr/zarar, kârlılık trendi (son 3 yıl karşılaştırma)
   • Borç yapısı: toplam borç, kısa/uzun vadeli dağılımı, borç/özsermaye oranı
   • Nakit durumu: eldeki nakit, serbest nakit akışı, faiz karşılama oranı
   • Kısa vadeli borçları karşılama oranı (cari oran): 1'in altıysa riskli
   • Faiz ve amortisman öncesi kâr marjı: sektör ortalamasıyla karşılaştır

3. HALKA ARZ YAPISI VE FON KULLANIMI — ★ EN KRİTİK ★:
   ★ ÇİFT YÖNLÜ KONTROL: Sadece "sermaye artırımı" tutarına odaklanma!
     İzahnamede mutlaka "mevcut pay satışı" (ortak satışı) olup olmadığını ara.
     Halka arz gelirinin yüzde kaçı şirkete girecek, yüzde kaçı ortaklara gidecek — NET yaz.
   ★ Sermaye artırımı = para şirkete girer (yatırımcı lehine).
     Ortak satışı = para mevcut ortakların cebine gider (dikkat gerektiren durum).
   ★ "karma yöntem" varsa İKİSİNİ DE ayrı ayrı yaz (tutar + yüzde — nominal değer yazma).
   ★ "satan pay sahipleri" bölümünü kontrol et — kim ne kadar satıyor?
   ★ "Halka arz gelirinin tamamı şirkete gidiyor" ANCAK gerçekten ortak satışı YOKSA yazılabilir.
   ★ Emin değilsen "tamamı şirkete" YAZMA — bu çok kritik bir hata olur.
   ★ FON KULLANIM YERLERİ — şu anahtar kelimeleri ara:
     "fon kullanım yeri", "halka arz gelirinin kullanılacağı yerler",
     "izahnamenin 30. bölümü", "elde edilecek net nakit", "toplanan fonların kullanımı",
     "yatırım projesi", "kapasite artırımı", "işletme sermayesi", "borç kapama"
     Bu bilgi SPK izahnamelerinde ZORUNLU bulunur — bulamadıysan ATLA, "yok" deme.
   ★ Fon kullanımını yüzdelerle belirt: ne kadar yatırıma, ne kadar borca, ne kadar işletmeye.
   ★ Eğer gelirin büyük kısmı borç ödemeye gidiyorsa, bu olumsuz bir işaret.

4. DEĞERLEME (varsa):
   • Halka arz fiyatı × toplam pay sayısı = piyasa değeri
   • Fiyat/Kazanç oranı: sektör ortalamasıyla karşılaştır (yüksekse pahalı)
   • Piyasa Değeri / Defter Değeri: 1'in altıysa ucuz, çok yüksekse dikkat

5. MAL VARLIKLARI VE FİZİKSEL AKTİFLER (varsa — PDF'te bilgi bulursan yaz):
   • Gayrimenkul portföyü: fabrika, arsa, bina, depo — adet ve toplam alan (m²)
   • Maddi duran varlıklar toplamı (TL) — bilançodaki değer
   • Üretim tesisleri: kapasite, kullanım oranı, konum
   • Makine-ekipman: üretim hattı sayısı, teknoloji düzeyi
   • Araç filosu, envanter — somut sayı varsa belirt
   • Maddi olmayan varlıklar: marka değeri, patent, lisans, yazılım
   • ★ ÖNEMLİ: Mal varlığı bilgisi bilanço dipnotlarında, "maddi duran varlıklar" veya
     "yatırım amaçlı gayrimenkuller" bölümlerinde bulunur. Bulamazsan bu bölümü ATLA.

6. RİSK FAKTÖRLERİ (en az 2 madde):
   • Lisans/ruhsat bağımlılığı, tek müşteri/tedarikçi yoğunlaşması
   • Kur riski, faiz riski, ham madde fiyat riski
   • Devam eden davalar — MUTLAKA tutar belirt
   • İlişkili taraf işlemleri — ciro içindeki payı
   • Denetçi görüşü: şartlı/olumsuz ise çok kritik

7. ORTAKLIK VE YÖNETİM:
   • Halka arz sonrası ortaklık oranları
   • Hisse satış yasağı (lock-up) süreleri — 180 günden kısaysa olumsuz
   • İmtiyazlı pay var mı? Oy hakkı eşit mi?
   • Kar dağıtım politikası — yatırımcıya temettü dağıtılacak mı?
   • Yönetim kadrosu deneyimi — kilit isimlerin sektör tecrübesi (yıl)

8. BÜYÜME POTANSİYELİ:
   • Yıllık satış büyümesi, pazar payı, kapasite artışı
   • İhracat oranı, coğrafi çeşitlilik
   • Araştırma-geliştirme yatırımları, patent/lisans
   • Yeni projeler, genişleme planları — somut yatırım tutarı varsa belirt

9. HUKUKİ VE DÜZENLEYİCİ:
   • Devam eden davalar (tutar!), vergi ihtilafları
   • Düzenleyici risk, sektörel kısıtlamalar

═══ KENDİNİ KONTROL ET (her madde için) ═══
Yazdığın her madde için şu soruları sor:
1. Bu bilgi gerçekten PDF'te var mı? → Yoksa SİL.
2. Somut rakam/yüzde/tutar içeriyor mu? → İçermiyorsa eklemeye çalış veya SİL.
3. Bu bilgi sadece BU şirkete mi özel, yoksa her şirket için geçerli mi? → Genel ise SİL.
4. Kısaltma var mı? → Varsa aç.

═══ ÇIKTI FORMAT (geçerli JSON) ═══
{
  "company_brief": "Şirket hakkında 3-4 KISA cümle: ana faaliyet alanı, sektör, ne üretiyor/satıyor, öne çıkan özelliği. Yatırımcı şirketi tanısın. SADECE PDF'ten çıkan bilgiler. Bilmiyorsan bu alanı boş string yap. MAX 400 karakter. YARIM CÜMLE BIRAKMA.",
  "positives": ["somut olumlu — mutlaka rakam/yüzde/tutar içermeli, kısaltma YOK", ...],
  "negatives": ["somut olumsuz — mutlaka rakam/risk/tutar içermeli, kısaltma YOK", ...],
  "summary": "Düzgün Türkçe, 1-2 kısa cümle. Küçük yatırımcıya net mesaj. Kısaltma YOK. DEVRİK CÜMLE KURMA.",
  "risk_level": "düşük|orta|yüksek|çok yüksek",
  "key_risk": "en kritik tek risk — kısaltma YOK (max 100 karakter)"
}

═══ MADDE KURALLARI ═══
• Hedef: olumlu 7-10, olumsuz 7-10. Olabildiğince fazla somut madde yaz. PDF yetersizse 4-5 de olur — uydurma YASAK.
• Her madde FARKLI konu. Aynı konuyu tekrarlama — tekrar edeceksen yazma.
• En az 3 farklı kategori (Finansal/Risk/Fon/Ortaklık/Büyüme/Hukuki).
• Max 180 karakter. KISA, yoğun, bilgi dolu. Sade Türkçe, net. KISALTMA YOK.
• ★ HER MADDE TAM CÜMLE OLMALI — cümle ortasında BİTİRME, kesME. ★
  Sığmıyorsa bilgiyi ÖZETLE, kısalt. Yarım cümle ASLA kabul edilmez.
• "summary" alanı: DÜZ CÜMLE yaz. Özne + nesne + yüklem sırası. Devrik yapma.
• Bilgi yoksa "bulunamadı" deme — o maddeyi hiç yazma, yokluğunu rapor etme.
• ★ "eksik", "bulunamadı", "belirtilmemiş", "yer almıyor" kelimelerini İÇEREN HERHANGİ BİR MADDE YAZMA. ★
• YAZIM KONTROLÜ: Yazdıktan sonra tüm maddeleri oku:
  1. Cümle tam mı? Yarıda mı kalmış? → Yarımsa kısalt
  2. Teknik terim var mı? → Varsa günlük dile çevir
  3. Kısaltma var mı? → Varsa aç
  4. "eksik/bulunamadı" var mı? → Varsa o maddeyi SİL
SADECE JSON döndür — başka hiçbir şey yazma."""


_FEW_SHOT_EXAMPLES = """
═══ REFERANS ANALİZ ÖRNEKLERİ ═══

ÖRNEK 1 — Kimya Sektörü (karma yöntem: sermaye artırımı + ortak satışı):
{
  "company_brief": "Türkiye'nin önde gelen özel kimyasal üreticisi. 1998'den beri endüstriyel yapıştırıcı ve kaplama ürünleri üretiyor. İstanbul ve Kocaeli'deki 2 fabrikada 420 çalışanıyla Avrupa ve Orta Asya'da 28 ülkeye ihracat yapıyor.",
  "positives": [
    "2023 yılı satışları 892 milyon TL, faiz ve amortisman öncesi kâr marjı %18.4 — sektör ortalaması %11",
    "İhracat payı %34; Avrupa ve Orta Asya'da 28 ülkeye satış yapılıyor — coğrafi çeşitlilik var",
    "Halka arz gelirinin %45'i yeni üretim hattına, %30'u araştırma-geliştirmeye ayrılacak",
    "Halka arz gelirinin 84 milyon TL'si sermaye artırımından, 41 milyon TL'si ise ortak satışından oluşuyor",
    "5 patent ve 12 faydalı model tescili var — rakiplerin taklit etmesi zor",
    "Bağımsız denetçi olumlu görüş vermiş — şartlı veya olumsuz görüş yok"
  ],
  "negatives": [
    "Kısa vadeli borç 340 milyon TL, toplam borcun %76'sı — ödeme sıkıntısı riski var",
    "En büyük 3 müşteri toplam satışların %52'sini oluşturuyor — bir müşteri kaybında ciddi gelir düşüşü",
    "Ham madde maliyetleri dövize bağlı; dolar %10 artarsa kâr marjı 3 puan düşer",
    "2022'de 45 milyon TL zarar yazmış; 2023'te kâra geçiş henüz 1 yıllık — sürdürülebilirliği belirsiz",
    "Devam eden 3 dava var, toplam risk tutarı 28 milyon TL",
    "Halka arzın %33'ü ortak satışı — 41 milyon TL mevcut ortakların cebine gidecek"
  ],
  "summary": "Şirket ihracata dayalı güçlü satışlara sahip ancak kısa vadeli borç yükü yüksek ve az sayıda müşteriye bağımlı.",
  "risk_level": "orta",
  "key_risk": "Kısa vadeli borç oranı %76 — borçları çevirememe riski yüksek"
}

ÖRNEK 2 — Teknoloji Sektörü (%100 sermaye artırımı, yüksek büyüme):
{
  "company_brief": "Kurumsal şirketlere bulut tabanlı insan kaynakları ve bordro yazılımı sunan yerli teknoloji şirketi. 2017'de kurulmuş, 85 yazılım mühendisiyle hizmet veriyor. Türkiye'deki 1.200'den fazla kurumsal müşteriye SaaS modeliyle satış yapıyor.",
  "positives": [
    "Son 3 yılda satışlar yıllık ortalama %65 büyümüş — 2021'de 48 milyon TL, 2023'te 131 milyon TL",
    "Halka arzın tamamı sermaye artırımından oluşuyor — ortak satışı yok, gelirin tamamı şirkete girecek",
    "Halka arz gelirinin %60'ı yeni ürün geliştirmeye, %25'i uluslararası pazara girişe ayrılacak",
    "Ortaklar 365 gün hisse satış yasağı taahhüdü vermiş — standart 180 günün 2 katı",
    "Yazılım sektöründe faaliyet kâr marjı %24 — sektör ortalaması %15"
  ],
  "negatives": [
    "En büyük müşteri satışların %41'ini oluşturuyor — tek müşteriye aşırı bağımlılık",
    "Toplam borç özsermayenin 1.8 katı — borç/özsermaye oranı %180 ile tehlikeli seviyede",
    "İlişkili taraf işlemleri cironun %22'sini oluşturuyor — şeffaflık riski",
    "Fiyat/Kazanç oranı 32 — sektör ortalaması 18, halka arz fiyatı yüksek görünüyor",
    "Şirketin kar dağıtım politikası yok — yatırımcıya temettü ödenip ödenmeyeceği belirsiz"
  ],
  "summary": "Hızlı büyüyen teknoloji şirketi ancak tek müşteriye bağımlı, borç yükü yüksek ve halka arz fiyatı sektör ortalamasının üzerinde.",
  "risk_level": "yüksek",
  "key_risk": "En büyük müşteri satışların %41'i — bu müşteriyi kaybetmek gelirin yarısını yok eder"
}

✗ KÖTÜ ÖRNEKLER (asla böyle yazma):
✗ "Kayıtlı sermaye tavanı 500M TL olarak belirlenmiştir" → bilgi değeri sıfır
✗ "Şirketin güçlü bir büyüme potansiyeli bulunmaktadır" → kanıtsız, genel
✗ "Piyasa dalgalanmalarından etkilenebilir" → her şirket için geçerli
✗ "Yatırımcıların riskleri dikkate alması gerekmektedir" → tavsiye değil analiz yap
✗ "Sektörde artan rekabet baskısı söz konusu olabilir" → spesifik değil
✗ "Finansal tablo detayları bulunamadı" → yokluğu rapor etme
✗ "NNA negatif seyir izliyor" → kısaltma YASAK, "Net nakit akışı negatif" yaz
✗ "FAVÖK marjı %18" → kısaltma YASAK, "Faiz ve amortisman öncesi kâr marjı %18" yaz
✗ "892M TL hasılat" → "892 milyon TL satış" yaz — M/Mly kısaltma YASAK
✗ "Halka arz gelirinin tamamı şirkete gidiyor" → SADECE ortak satışı YOKSA yazılabilir!

═══ FİNANSAL ANALİZ KILAVUZU (karşılaştırma için kullan) ═══

Borç/Özsermaye: <%50 iyi, %50-100 normal, >%100 riskli, >%200 tehlikeli
Kısa Vadeli Borç Oranı: <%60 iyi, >%70 riskli — borçları çevirememe baskısı
Kısa Vadeli Borçları Karşılama Oranı (Cari Oran): >1.5 sağlıklı, 1.0-1.5 kabul edilebilir, <1.0 risk
Net Kâr Marjı: Sektöre göre değişir ama negatif = zarar, <%5 zayıf, >%15 güçlü
Faiz ve Amortisman Öncesi Kâr Marjı: Üretim >%15 iyi, Teknoloji >%20 iyi, Perakende >%8 iyi
Satış Büyümesi (Yıllık Bileşik): >%20 güçlü, %10-20 normal, <%10 zayıf
Tek Müşteri Bağımlılığı: >%30 dikkat, >%50 ciddi risk — gelir kaybı senaryosu yaz
İlişkili Taraf İşlemleri: Cironun >%10'u dikkat, >%20'si şeffaflık riski
Fiyat/Kazanç Oranı: Sektör ortalamasıyla karşılaştır. BİST üretim: 12-18, teknoloji: 15-25, perakende: 10-16
Piyasa Değeri/Defter Değeri: <1 ucuz (neden düşük?), 1-3 normal, >5 çok pahalı
Faiz Karşılama Oranı: >3 güçlü, 1.5-3 kabul edilebilir, <1.5 borç ödeme güçlüğü
Stok Devir Hızı: Sektöre göre değişir — yavaşlama trendi olumsuz
Ortak Satışı: ★ KRİTİK — %100 sermaye artırımı → iyi, ortak satışı varsa MUTLAKA belirt (tutar + oran + kimin sattığını). "Tamamı şirkete" ANCAK ortak satışı yoksa yazılabilir
Hisse Satış Yasağı (Lock-up): 365 gün çok iyi, 180 gün standart, <180 gün → ortak güvensizliği
Kar Dağıtım Politikası: Var mı? Net oran belirtilmiş mi? Yoksa yatırımcı temettü alamayabilir
Denetçi Görüşü: Olumlu → iyi, şartlı → dikkat (neden?), olumsuz → çok kritik risk
"""


# ─────────────────────────────────────────────────────────────
# PDF İndirme + Metin Çıkarma
# ─────────────────────────────────────────────────────────────

async def download_pdf(url: str) -> Optional[str]:
    """PDF'i geçici dosyaya STREAMING olarak indirir, dosya yolunu döner.

    Memory optimizasyonu: resp.content yerine chunk chunk yazarak
    tüm PDF'i RAM'de tutmaktan kaçınır (Render 512MB limiti).
    """
    try:
        async with httpx.AsyncClient(
            timeout=90.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SZAlgo/1.0)"},
        ) as client:
            # Streaming indirme — tüm içeriği RAM'e almaz
            async with client.stream("GET", url) as resp:
                if resp.status_code != 200:
                    logger.warning("PDF indirilemedi: HTTP %d — %s", resp.status_code, url)
                    return None

                # PDF olup olmadığını kontrol et
                ct = resp.headers.get("content-type", "")
                if "pdf" not in ct.lower() and not url.lower().endswith(".pdf"):
                    logger.warning("PDF değil: content-type=%s", ct)
                    # Yine de dene (bazı sunucular yanlış CT gönderir)

                suffix = ".pdf"
                tmp_file = tempfile.NamedTemporaryFile(
                    delete=False, suffix=suffix
                )
                total_bytes = 0
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    tmp_file.write(chunk)
                    total_bytes += len(chunk)
                tmp_file.close()

                file_size_kb = total_bytes // 1024
                logger.info("PDF indirildi (streaming): %s (%d KB)", url, file_size_kb)
                return tmp_file.name

    except httpx.TimeoutException:
        logger.error("PDF indirme TIMEOUT: %s", url)
        return None
    except Exception as e:
        logger.error("PDF indirme hatası: %s — %s", url, e)
        return None


_MAX_PAGES = 150  # Max sayfa — Render 512MB limiti için güvenli sınır

def _extract_pages_pymupdf(pdf_path: str) -> list:
    """PyMuPDF (fitz) ile sayfa metinlerini çıkar.

    Memory optimizasyonu: max sayfa limiti + sayfa başı gc.
    """
    try:
        import gc
        import fitz  # PyMuPDF
        pages = []
        doc = fitz.open(pdf_path)
        total = len(doc)
        max_pages = min(total, _MAX_PAGES)
        for i in range(max_pages):
            text = doc[i].get_text("text") or ""
            if text.strip():
                pages.append((i, text))
        doc.close()
        gc.collect()
        logger.info("PyMuPDF: %d/%d sayfadan %d'ünde metin bulundu", max_pages, total, len(pages))
        return pages
    except Exception as e:
        logger.warning("PyMuPDF çıkarma hatası: %s", e)
        return []


def _extract_pages_pdfplumber(pdf_path: str) -> list:
    """pdfplumber ile sayfa metinlerini çıkar.

    Memory optimizasyonu: max sayfa limiti + sayfa başı flush.
    """
    try:
        import gc
        pages = []
        with pdfplumber.open(pdf_path) as pdf:
            max_pages = min(len(pdf.pages), _MAX_PAGES)
            for i in range(max_pages):
                try:
                    text = pdf.pages[i].extract_text() or ""
                    if text.strip():
                        pages.append((i, text))
                    # pdfplumber sayfa objeleri ağır — her 20 sayfada gc çalıştır
                    if i > 0 and i % 20 == 0:
                        gc.collect()
                except Exception:
                    continue
        gc.collect()
        logger.info("pdfplumber: %d/%d sayfada metin bulundu", len(pages), max_pages)
        return pages
    except Exception as e:
        logger.warning("pdfplumber çıkarma hatası: %s", e)
        return []


def _extract_pages_tesseract_sync(pdf_path: str) -> list:
    """Tesseract OCR ile taranmış PDF'ten metin çıkar.

    PyMuPDF ile sayfaları 200 DPI gri görüntüye çevir → pytesseract ile OCR yap.
    Vision OCR'dan önce çalışır: yerel, ücretsiz, 100+ sayfalı PDF'leri işler.

    Akıllı örnekleme:
    - ≤40 sayfa: tüm sayfalar
    - >40 sayfa: İlk 20 (özet+risk) + 1/3'teki 5 sayfa + 2/3'teki 5 sayfa + son 5 sayfa

    Returns: [(page_index, extracted_text), ...]
    """
    try:
        import fitz  # PyMuPDF
        import io

        try:
            import pytesseract
            from PIL import Image as PilImage
        except ImportError:
            logger.warning("Tesseract: pytesseract veya Pillow kurulu değil — atlanıyor")
            return []

        # Tesseract ikili dosyasının varlığını kontrol et
        try:
            import subprocess
            result = subprocess.run(["tesseract", "--version"], capture_output=True, timeout=5)
            if result.returncode != 0:
                logger.warning("Tesseract: binary bulunamadı — atlanıyor")
                return []
        except (FileNotFoundError, subprocess.TimeoutExpired):
            logger.warning("Tesseract: binary bulunamadı — atlanıyor")
            return []

        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        # Kapsamlı sayfa örnekleme — izahnamelerde mal varlıkları, borç, ortaklık gibi
        # önemli bölümler orta sayfalarda yer alıyor, bunları kaçırmamalıyız
        if total_pages <= 50:
            sample_indices = list(range(total_pages))
        else:
            indices = set()
            indices.update(range(min(25, total_pages)))                           # İlk 25: kapak+özet+risk başı
            # Orta bölgeler: her 10 sayfada 3 sayfa örnekle
            for checkpoint in range(25, total_pages - 10, 10):
                indices.update(range(checkpoint, min(checkpoint + 3, total_pages)))
            t1 = total_pages // 3
            indices.update(range(t1, min(t1 + 5, total_pages)))                  # 1/3: finansal tablolar
            t2 = total_pages // 2
            indices.update(range(t2, min(t2 + 5, total_pages)))                  # 1/2: mal varlıkları
            t3 = total_pages * 2 // 3
            indices.update(range(t3, min(t3 + 5, total_pages)))                  # 2/3: ortaklık yapısı
            indices.update(range(max(total_pages - 8, 0), total_pages))          # Son 8: ek bilgiler
            sample_indices = sorted(indices)

        logger.info("Tesseract OCR: %d/%d sayfa örnekleniyor...", len(sample_indices), total_pages)

        # 150 DPI — orijinal 200 DPI yerine, kalite neredeyse aynı ama RAM %44 tasarruf
        # (200 DPI: 2222x2778 piksel/sayfa ≈ 6.2MB, 150 DPI: 1667x2083 ≈ 3.5MB)
        mat = fitz.Matrix(150 / 72, 150 / 72)
        pages = []

        for idx, i in enumerate(sample_indices):
            try:
                pix = doc[i].get_pixmap(matrix=mat, colorspace=fitz.csGRAY)
                img_bytes = pix.tobytes("png")
                pix = None  # Pixmap referansını hemen bırak

                img = PilImage.open(io.BytesIO(img_bytes))
                img_bytes = None  # PNG bytes referansını bırak
                # --psm 6: Düzgün metin bloğu olarak işle (izahname sayfaları için ideal)
                text = pytesseract.image_to_string(img, lang="tur+eng", config="--psm 6 --oem 3")
                img.close()
                img = None

                if text.strip():
                    pages.append((i, text))

                # Her 5 sayfada gc.collect() — Tesseract image buffer'ları temizle
                if idx > 0 and idx % 5 == 0:
                    import gc as _gc
                    _gc.collect()

            except Exception as pe:
                logger.warning("Tesseract sayfa %d hatası: %s", i, pe)
                continue

        doc.close()
        import gc as _gc
        _gc.collect()

        total_chars = sum(len(t) for _, t in pages)
        logger.info(
            "Tesseract OCR tamamlandı: %d sayfadan metin alındı, toplam %d karakter",
            len(pages), total_chars,
        )
        return pages

    except Exception as e:
        logger.error("Tesseract OCR genel hata: %s — %s", type(e).__name__, e)
        return []


def _extract_pages_vision_sync(pdf_path: str) -> list:
    """Taranmış/görüntü tabanlı PDF için Claude Vision OCR.

    PyMuPDF ile sayfaları JPEG görüntüye çevir → Claude Vision ile metin çıkar.
    Sync fonksiyon (run_in_executor içinde çalışır).

    Returns: [(batch_start_page, extracted_text), ...]
    """
    try:
        import fitz  # PyMuPDF
        import base64
        import httpx as _httpx

        from app.config import get_settings
        _settings = get_settings()
        api_key = _settings.ABACUS_API_KEY
        gemini_key_ocr = _settings.GEMINI_API_KEY
        if not api_key and not gemini_key_ocr:
            logger.warning("Vision OCR: API key yok (ne Abacus ne Gemini)")
            return []

        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        # Akıllı sayfa örnekleme: başı + ortası + sonu
        # İzahnamede: kapak(1-3), özet(4-10), risk(10-30), finansal(30-60+)
        # Her bölümden 2’şer sayfa = 3 batch × 2 sayfa = 6 sayfa, ~3 API çağrısı
        if total_pages <= 15:
            # Kısa belge: tamamını al
            sample_indices = list(range(total_pages))
        elif total_pages <= 60:
            # Orta belge: baştan 8 + ortadan 4 + sondan 4 = ~16 sayfa
            start_pages = list(range(min(8, total_pages)))
            mid = total_pages // 2
            mid_pages = list(range(mid - 2, min(mid + 2, total_pages)))
            end_pages = list(range(max(total_pages - 4, 0), total_pages))
            sample_indices = sorted(set(start_pages + mid_pages + end_pages))
        else:
            # Uzun belge (100-200 sayfa izahname): 7 bölgeden örnekle = ~24 sayfa
            # Kapak+özet (1-8), risk faktörleri (10-15), şirket bilgileri (20-25),
            # finansal tablolar (1/3), mal varlıkları (1/2), ortaklık yapısı (2/3), son bölüm
            indices = set()
            indices.update(range(min(8, total_pages)))                          # İlk 8: kapak, özet, genel bilgi
            p10 = min(10, total_pages - 1)
            indices.update(range(p10, min(p10 + 4, total_pages)))              # 10-14: risk faktörleri başlangıcı
            p20 = min(20, total_pages - 1)
            indices.update(range(p20, min(p20 + 3, total_pages)))              # 20-23: şirket detayları
            t1 = total_pages // 3
            indices.update(range(t1, min(t1 + 3, total_pages)))                # 1/3: finansal tablolar
            t2 = total_pages // 2
            indices.update(range(t2, min(t2 + 3, total_pages)))                # 1/2: mal varlıkları, borçlar
            t3 = total_pages * 2 // 3
            indices.update(range(t3, min(t3 + 3, total_pages)))                # 2/3: ortaklık, fon kullanımı
            indices.update(range(max(total_pages - 4, 0), total_pages))        # Son 4: ek bilgiler
            sample_indices = sorted(indices)

        # Sayfaları 108 DPI gri JPEG olarak render et
        page_images = []
        mat = fitz.Matrix(1.5, 1.5)  # 108 DPI — 72 DPI’dan 2x daha iyi kalite
        for idx, i in enumerate(sample_indices):
            try:
                pix = doc[i].get_pixmap(matrix=mat, colorspace=fitz.csGRAY)
                img_bytes = pix.tobytes("jpeg")
                pix = None
                b64 = base64.b64encode(img_bytes).decode("utf-8")
                img_bytes = None  # bytes referansını bırak
                page_images.append((i, b64))
            except Exception as pe:
                logger.warning("Vision OCR sayfa render hatası s.%d: %s", i, pe)
        doc.close()
        import gc as _gc
        _gc.collect()

        logger.info("Vision OCR: %d sayfa örneklendi (toplam=%d), stratejik: %s",
                    len(page_images), total_pages, sample_indices)

        # 2’şer sayfalık batch — her biri ~30-45s, 3 batch = ~90-135s toplam
        all_texts = []
        batch_size = 2

        for batch_start in range(0, len(page_images), batch_size):
            batch = page_images[batch_start:batch_start + batch_size]
            end_page = batch_start + len(batch)

            content = [{
                "type": "text",
                "text": (
                    f"Bu Türkçe halka arz izahnamesinin sayfa {batch_start + 1}-{end_page} "
                    "içeriğini metin olarak çıkar. Sadece gerçek metni yaz, "
                    "başka açıklama ekleme."
                ),
            }]
            for _, b64 in batch:
                content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
                })

            try:
                extracted = ""
                ocr_success = False

                # Önce Abacus dene
                if api_key:
                    with _httpx.Client(timeout=90) as hcl:
                        resp = hcl.post(
                            _ABACUS_URL,
                            headers={
                                "Authorization": f"Bearer {api_key}",
                                "Content-Type": "application/json",
                            },
                            json={
                                "model": _AI_MODEL,
                                "messages": [{"role": "user", "content": content}],
                                "max_tokens": 4000,
                                "temperature": 0,
                            },
                        )
                    if resp.status_code == 200:
                        extracted = (
                            resp.json()
                            .get("choices", [{}])[0]
                            .get("message", {})
                            .get("content", "")
                        )
                        ocr_success = True
                    else:
                        logger.warning(
                            "Vision OCR Abacus HTTP %d — Gemini'ye geciliyor: %s",
                            resp.status_code, resp.text[:200],
                        )

                # Abacus başarısızsa Gemini dene
                if not ocr_success and gemini_key_ocr:
                    with _httpx.Client(timeout=90) as hcl:
                        resp = hcl.post(
                            _GEMINI_URL,
                            headers={
                                "Authorization": f"Bearer {gemini_key_ocr}",
                                "Content-Type": "application/json",
                            },
                            json={
                                "model": _GEMINI_MODEL,
                                "messages": [{"role": "user", "content": content}],
                                "max_tokens": 4000,
                                "temperature": 0,
                            },
                        )
                    if resp.status_code == 200:
                        extracted = (
                            resp.json()
                            .get("choices", [{}])[0]
                            .get("message", {})
                            .get("content", "")
                        )
                        ocr_success = True
                    else:
                        logger.warning(
                            "Vision OCR Gemini HTTP %d: %s",
                            resp.status_code, resp.text[:200],
                        )

                if ocr_success and extracted:
                    all_texts.append((batch_start, extracted))
                    logger.info(
                        "Vision OCR batch s.%d-%d: %d karakter",
                        batch_start + 1, end_page, len(extracted),
                    )
                elif not ocr_success:
                    logger.warning(
                        "Vision OCR batch s.%d-%d: her iki provider basarisiz",
                        batch_start + 1, end_page,
                    )

            except Exception as batch_err:
                logger.warning(
                    "Vision OCR batch s.%d hatası: %s — %s",
                    batch_start + 1, type(batch_err).__name__, batch_err,
                )

        total_chars = sum(len(t) for _, t in all_texts)
        logger.info(
            "Vision OCR tamamlandı: %d batch, toplam %d karakter",
            len(all_texts), total_chars,
        )
        return all_texts

    except Exception as e:
        logger.error("Vision OCR genel hata: %s — %s", type(e).__name__, e)
        return []


def _extract_tables_pdfplumber(pdf_path: str, target_pages: set | None = None) -> str:
    """pdfplumber ile finansal tabloları yapılandırılmış metin olarak çıkarır.

    İzahnamelerdeki çok sütunlu finansal tablolar düz metin çıkarımında
    karışır. Bu fonksiyon tabloları satır-sütun formatında çıkarır.

    Memory optimizasyonu:
    - target_pages verilmişse SADECE o sayfalara bakar (150 yerine ~15-20 sayfa)
    - target_pages yoksa ilk 50 sayfaya bakar (fallback)
    - pdfplumber tablo çıkarma ÇOK ağır — Render 512MB limiti korunur.
    """
    try:
        import gc
        tables_text = []
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            if target_pages and len(target_pages) > 0:
                # Sadece hedef sayfalarda tablo ara + 2 sayfa buffer (öncesi/sonrası)
                buffered = set()
                for p in target_pages:
                    for offset in range(-2, 3):
                        bp = p + offset
                        if 0 <= bp < total_pages:
                            buffered.add(bp)
                pages_to_scan = sorted(buffered)
                logger.info("pdfplumber tablo: %d hedef sayfa (%d buffer ile)", len(target_pages), len(pages_to_scan))
            else:
                # Fallback: finansal bölüm bulunamadıysa ilk 50 sayfaya bak
                pages_to_scan = list(range(min(total_pages, 50)))
                logger.info("pdfplumber tablo: hedef sayfa yok, ilk %d sayfaya bakılıyor", len(pages_to_scan))

            for i in pages_to_scan:
                try:
                    page_tables = pdf.pages[i].extract_tables()
                    if not page_tables:
                        continue
                    for t_idx, table in enumerate(page_tables):
                        if not table or len(table) < 2:
                            continue
                        # Tablonun finansal veri içerip içermediğini kontrol et
                        table_text = " ".join(
                            str(cell or "") for row in table for cell in row
                        ).lower()
                        is_financial = any(
                            kw in table_text
                            for kw in [
                                "hasılat", "hasilat", "satış", "gelir",
                                "kâr", "kar", "zarar", "borç", "borc",
                                "varlık", "varlik", "yükümlülük",
                                "nakit", "özkaynak", "sermaye",
                                "ebitda", "faaliyet", "brüt", "brut",
                                "bilanço", "bilanco", "aktif", "pasif",
                            ]
                        )
                        if not is_financial:
                            continue

                        # Tabloyu okunabilir formata çevir
                        rows_formatted = []
                        for row in table:
                            cells = [str(c or "").strip() for c in row]
                            if any(cells):  # Boş satırları atla
                                rows_formatted.append(" | ".join(cells))
                        if rows_formatted:
                            tables_text.append(
                                f"\n[S.{i+1} — Finansal Tablo]\n"
                                + "\n".join(rows_formatted)
                            )
                except Exception:
                    continue
                # Her 15 sayfada gc — pdfplumber tablo çıkarma çok ağır
                if i > 0 and i % 15 == 0:
                    gc.collect()

        gc.collect()
        combined = "\n".join(tables_text)
        if combined:
            logger.info(
                "pdfplumber tablo çıkarımı: %d tablo, %d karakter",
                len(tables_text), len(combined),
            )
        return combined
    except Exception as e:
        logger.warning("pdfplumber tablo çıkarma hatası: %s", e)
        return ""


def extract_pdf_text(pdf_path: str) -> tuple[Optional[str], int]:
    """PDF’ten metin çıkarır. PyMuPDF → pdfplumber → Tesseract → Vision OCR.

    Memory optimizasyonu: Her aşama sonunda gc.collect() çağrılır.
    Render 512MB limiti için kritik.

    Returns: (metin veya None, analiz edilen sayfa sayısı)
    """
    import gc

    try:
        # Önce PyMuPDF dene (en hafif yöntem)
        page_tuples = _extract_pages_pymupdf(pdf_path)
        gc.collect()  # PyMuPDF sonrası temizlik

        # Boşsa pdfplumber ile dene
        if not page_tuples:
            logger.info("PyMuPDF 0 karakter — pdfplumber deneniyor...")
            page_tuples = _extract_pages_pdfplumber(pdf_path)
            gc.collect()  # pdfplumber sonrası temizlik

        # ─── Yetersiz metin kontrolü ───
        # PyMuPDF/pdfplumber metin bulduysa ama çok azsa (sayfa başı <100 karakter)
        # bu genellikle dijital imza metadata’sı veya boş sayfa demek → OCR’a düş
        _text_is_insufficient = False
        if page_tuples:
            _total_chars = sum(len(t) for _, t in page_tuples)
            _total_pages = len(page_tuples)
            _avg_chars_per_page = _total_chars / max(_total_pages, 1)
            # Ayrıca PDF’in gerçek sayfa sayısını kontrol et
            try:
                import fitz
                _doc = fitz.open(pdf_path)
                _real_page_count = len(_doc)
                _doc.close()
                del _doc
            except Exception:
                _real_page_count = _total_pages
            # Metin çıkan sayfa oranı çok düşükse VEYA ortalama karakter çok azsa → yetersiz
            if (_total_chars < 2000 and _real_page_count > 10) or (_avg_chars_per_page < 100 and _real_page_count > 5):
                logger.info(
                    "Metin yetersiz: %d karakter / %d sayfa (ort: %.0f/sayfa, toplam: %d sayfa) — OCR deneniyor",
                    _total_chars, _total_pages, _avg_chars_per_page, _real_page_count,
                )
                _text_is_insufficient = True
                page_tuples = []  # Sıfırla, OCR denesin
                gc.collect()

            # ─── Çöp metin tespiti (dijital imza/doğrulama kodları) ───
            # Karakter sayısı yeterli AMA hiçbir izahname keyword'ü eşleşmiyorsa
            # metin dijital imza hash'leri / sertifika kodlarından oluşuyor demektir
            if not _text_is_insufficient and page_tuples and _total_chars > 2000:
                _all_keywords = _RISK_KEYWORDS + _FINANCE_KEYWORDS + _ACTIVITY_KEYWORDS + _ASSET_KEYWORDS
                _combined_sample = " ".join(t.lower() for _, t in page_tuples[:50])  # İlk 50 sayfa
                _kw_hits = sum(1 for kw in _all_keywords if kw in _combined_sample)
                if _kw_hits < 3:  # 3'ten az keyword eşleşmesi = çöp metin
                    logger.warning(
                        "ÇÖP METİN TESPİTİ: %d karakter, %d sayfa ama sadece %d keyword eşleşmesi — "
                        "dijital imza/doğrulama kodları olabilir. OCR deneniyor...",
                        _total_chars, _total_pages, _kw_hits,
                    )
                    _text_is_insufficient = True
                    page_tuples = []
                    gc.collect()

        # İkisi de boşsa/yetersizse Tesseract OCR dene (yerel, ücretsiz, 100+ sayfa)
        if not page_tuples:
            logger.info("PyMuPDF/pdfplumber %s — Tesseract OCR deneniyor...",
                        "yetersiz metin" if _text_is_insufficient else "0 karakter")
            page_tuples = _extract_pages_tesseract_sync(pdf_path)
            gc.collect()  # Tesseract sonrası temizlik (en ağır aşama)

        # Tesseract da boşsa Vision OCR dene (son çare — Claude API)
        if not page_tuples:
            logger.info("Tesseract 0 karakter — Vision OCR (Claude) deneniyor...")
            page_tuples = _extract_pages_vision_sync(pdf_path)
            gc.collect()  # Vision OCR sonrası temizlik

        if not page_tuples:
            logger.warning("PDF’ten hiçbir yöntemle metin çıkarılamadı: %s", pdf_path)
            return None, 0

        all_pages_text = []
        risk_section_text = []
        finance_section_text = []
        fund_usage_text = []      # Fon kullanım yerleri
        ownership_text = []       # Ortaklık yapısı
        activity_text = []        # Şirket faaliyet bilgileri
        asset_text = []           # Mal varlıkları / fiziksel aktifler
        financial_page_indices = set()  # Tablo çıkarma için finansal sayfa numaraları
        in_risk_section = False
        in_finance_section = False
        in_fund_section = False
        in_ownership_section = False
        in_activity_section = False
        in_asset_section = False

        for i, text in page_tuples:
            all_pages_text.append(text)

            text_lower = text.lower()

            # Risk bölümü
            if any(kw in text_lower for kw in ["risk faktörleri", "riskler", "risk factors"]):
                in_risk_section = True
            if in_risk_section and any(kw in text_lower for kw in ["finansal bilgiler", "izahname özeti", "fon kullanım"]):
                in_risk_section = False
            if in_risk_section:
                risk_section_text.append(f"[S.{i+1}] {text}")

            # Finansal bölüm
            if any(kw in text_lower for kw in ["finansal durum", "özet finansal", "mali tablo", "gelir tablosu"]):
                in_finance_section = True
            if in_finance_section and len(finance_section_text) > 20:
                in_finance_section = False
            if in_finance_section:
                finance_section_text.append(f"[S.{i+1}] {text}")
                financial_page_indices.add(i)  # Tablo çıkarma için işaretle

            # Fon kullanımı + Halka arz yapısı bölümü
            if any(kw in text_lower for kw in [
                "fon kullanım", "halka arz geliri", "sermaye artırımı",
                "ortak satışı", "pay satışı", "mevcut ortakların",
                "ortak çıkışı", "secondary offering", "halka arz yapısı",
                "ihraç edilecek pay", "nominal değerli pay",
                "satışa sunulacak", "halka arz şekli",
                "arz edilen pay", "arz büyüklüğü",
                # v2: Araştırma sonucu eklenen
                "satan pay sahipleri", "karma yöntem",
                "hasılat kullanım", "halka arz gelirlerinin kullanım",
                "talep toplama", "book building",
                "fiyat istikrar", "fiyat tespit",
                "halka arz fiyat", "lot miktarı", "pay adedi",
            ]):
                in_fund_section = True
            if in_fund_section and len(fund_usage_text) > 15:
                in_fund_section = False
            if in_fund_section:
                fund_usage_text.append(f"[S.{i+1}] {text}")

            # Ortaklık yapısı bölümü
            if any(kw in text_lower for kw in [
                "ortaklık yapısı", "pay sahipliği", "sermaye yapısı",
                "lock-up", "lock up", "satmama taahhüd", "satış yasağı",
                "halka arz sonrası", "tahsisat", "pay dağılım",
                "hisse satış", "satmama süresi",
                # v2: Araştırma sonucu eklenen
                "yönetim kurulu", "bağımsız üye", "kilit yönetici",
                "kontrol eden ortak", "hakim ortak", "imtiyazlı pay",
                "oy hakkı", "yönetim yapısı",
                "kurumsal yönetim", "kar dağıtım politikası",
            ]):
                in_ownership_section = True
            if in_ownership_section and len(ownership_text) > 10:
                in_ownership_section = False
            if in_ownership_section:
                ownership_text.append(f"[S.{i+1}] {text}")

            # Şirket faaliyet bilgileri bölümü
            if any(kw in text_lower for kw in [
                "faaliyet alanı", "faaliyet konusu", "ana faaliyet",
                "şirketin konusu", "şirketin amacı", "ticaret sicil",
                "üretim kapasitesi", "tesis", "fabrika",
                "çalışan sayısı", "personel", "istihdam",
                "müşteri profil", "hedef pazar", "pazar payı",
                "sektör", "rekabet", "rakip",
                "kuruluş", "tarihçe", "şirket hakkında",
                "coğrafi dağılım", "şube", "mağaza",
                "ihracat", "ithalat", "dış ticaret",
            ]):
                in_activity_section = True
            if in_activity_section and len(activity_text) > 12:
                in_activity_section = False
            if in_activity_section:
                activity_text.append(f"[S.{i+1}] {text}")

            # Mal varlıkları / fiziksel aktifler bölümü
            if any(kw in text_lower for kw in [
                "maddi duran varlık", "maddi duran varlik",
                "gayrimenkul", "taşınmaz", "tasinmaz",
                "arsa", "arazi", "bina", "depo",
                "makine", "ekipman", "teçhizat",
                "yatırım amaçlı gayrimenkul",
                "taşıt", "araç filosu",
                "maddi olmayan duran varlık", "patent",
                "lisans hakkı", "marka", "şerefiye",
                "kullanım hakkı", "finansal kiralama",
            ]):
                in_asset_section = True
            if in_asset_section and len(asset_text) > 10:
                in_asset_section = False
            if in_asset_section:
                asset_text.append(f"[S.{i+1}] {text}")

        if not all_pages_text:
            logger.warning("PDF’ten metin çıkarılamadı: %s", pdf_path)
            return None, 0

        # ─── Tablo çıkarma (finansal tablolar için kritik) ───
        # Sadece finansal bölüm sayfalarında tablo ara (150 yerine ~15-20 sayfa)
        # Bu, Render 512MB limiti için KRİTİK optimizasyon
        gc.collect()  # Tablo çıkarma öncesi bellek boşalt
        structured_tables = _extract_tables_pdfplumber(pdf_path, target_pages=financial_page_indices)
        gc.collect()  # Tablo çıkarma sonrası temizlik

        # Öncelikli metin birleştirme — önemli bölümler önce
        combined = ""

        # 1. Yapılandırılmış finansal tablolar (en yüksek öncelik)
        if structured_tables:
            combined += "\n\n=== YAPILANDIRILMIŞ FİNANSAL TABLOLAR ===\n"
            combined += structured_tables[:30_000]

        # 2. Risk bölümü
        if risk_section_text:
            risk_combined = "\n\n=== RİSK FAKTÖRLERİ BÖLÜMÜ ===\n" + "\n".join(risk_section_text)
            combined += risk_combined[:50_000]

        # 3. Finansal bilgiler (metin bazlı)
        if finance_section_text:
            fin_combined = "\n\n=== FİNANSAL BİLGİLER ===\n" + "\n".join(finance_section_text)
            combined += fin_combined[:25_000]

        # 4. Fon kullanımı
        if fund_usage_text:
            fund_combined = "\n\n=== FON KULLANIM YERLERİ ===\n" + "\n".join(fund_usage_text)
            combined += fund_combined[:15_000]

        # 5. Ortaklık yapısı
        if ownership_text:
            own_combined = "\n\n=== ORTAKLIK YAPISI & LOCK-UP ===\n" + "\n".join(ownership_text)
            combined += own_combined[:15_000]

        # 6. Şirket faaliyet bilgileri
        if activity_text:
            act_combined = "\n\n=== ŞİRKET FAALİYET BİLGİLERİ ===\n" + "\n".join(activity_text)
            combined += act_combined[:15_000]

        # 7. Mal varlıkları / fiziksel aktifler
        if asset_text:
            asset_combined = "\n\n=== MAL VARLIKLARI & FİZİKSEL AKTİFLER ===\n" + "\n".join(asset_text)
            combined += asset_combined[:12_000]

        # 8. Genel metin — baş + orta + son (ortayı ATLAMIYORUZ)
        full_text = "\n\n".join(all_pages_text)
        remaining_budget = _MAX_PDF_CHARS - len(combined)
        if remaining_budget > 10_000:
            # Baştan %35 + ortadan %30 + sondan %35 al
            third = int(remaining_budget * 0.35)
            mid_budget = int(remaining_budget * 0.30)
            start_chunk = full_text[:third]
            mid_start = len(full_text) // 3
            mid_chunk = full_text[mid_start:mid_start + mid_budget]
            end_chunk = full_text[-third:]
            combined = (
                f"\n\n=== İZAHNAME METNİ (BAŞ) ===\n{start_chunk}"
                f"\n\n=== İZAHNAME METNİ (ORTA — Finansal Bölge) ===\n{mid_chunk}"
                f"\n\n=== İZAHNAME METNİ (SON) ===\n{end_chunk}"
                f"\n\n{combined}"
            )

        pages_count = len(all_pages_text)
        logger.info(
            "PDF metin çıkarıldı: %d sayfa → %d karakter (risk=%d, fin=%d, fon=%d, ort=%d, faal=%d, varlik=%d)",
            pages_count, len(combined),
            len("".join(risk_section_text)),
            len("".join(finance_section_text)),
            len("".join(fund_usage_text)),
            len("".join(ownership_text)),
            len("".join(activity_text)),
            len("".join(asset_text)),
        )
        return combined[:_MAX_PDF_CHARS], pages_count

    except Exception as e:
        logger.error("PDF metin çıkarma hatası: %s — %s", pdf_path, e)
        return None, 0
    finally:
        try:
            if os.path.exists(pdf_path):
                os.remove(pdf_path)
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────
# AI Analiz
# ─────────────────────────────────────────────────────────────

async def analyze_with_ai(
    pdf_text: str,
    company_name: str,
    ipo_price: Optional[str] = None,
) -> Optional[dict]:
    """Çıkarılan PDF metni üzerinde AI analizi yapar.

    Provider sırası:
      1. Abacus AI RouteLLM (birincil)
      2. Anthropic Claude Sonnet 4 direkt API (yedek)
    """

    settings = get_settings()
    abacus_key = settings.ABACUS_API_KEY or None
    anthropic_key = getattr(settings, "ANTHROPIC_API_KEY", None) or None

    if not abacus_key and not anthropic_key:
        logger.error("AI API key yok (ne Abacus ne Claude) — izahname analizi yapılamadı")
        return None

    # Kullanıcı mesajı: şirket bağlamı + PDF metni
    context_lines = [f"ŞİRKET: {company_name}"]
    if ipo_price:
        context_lines.append(f"HALKA ARZ FİYATI: {ipo_price} TL")
    context_lines.append("")
    context_lines.append("─" * 60)
    context_lines.append("İZAHNAME PDF METNİ (tam metin veya özet):")
    context_lines.append("─" * 60)
    context_lines.append(pdf_text)

    user_message = "\n".join(context_lines)
    # Few-shot örneklerini system prompt'a ekle
    full_system = get_system_prompt() + "\n\n" + _FEW_SHOT_EXAMPLES

    content = ""
    provider_used = None
    try:
        # ── Birincil: Abacus AI RouteLLM ──
        if abacus_key and not content:
            try:
                async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                    resp = await client.post(
                        _ABACUS_URL,
                        headers={
                            "Authorization": f"Bearer {abacus_key}",
                            "Content-Type": "application/json",
                        },
                        json={
                            "model": _AI_MODEL,
                            "messages": [
                                {"role": "system", "content": full_system},
                                {"role": "user",   "content": user_message},
                            ],
                            "temperature": 0.15,
                            "max_tokens": 4096,
                        },
                    )

                if resp.status_code == 200:
                    data = resp.json()
                    content = (
                        data.get("choices", [{}])[0]
                        .get("message", {})
                        .get("content", "")
                        .strip()
                    )
                    if content:
                        provider_used = "Abacus"
                        logger.info("İzahname analizi Abacus ile uretildi: %s", company_name)
                    else:
                        logger.warning("Abacus 200 OK ama content bos (%s)", company_name)
                else:
                    logger.warning(
                        "Abacus izahname hatasi HTTP %d — Claude'a geciliyor. Detay: %s",
                        resp.status_code, resp.text[:300],
                    )
            except Exception as e:
                logger.warning("Abacus izahname hata (%s) — %s", company_name, e)

        # ── Yedek: Anthropic Claude Sonnet 4 (direkt API) ──
        if not content and anthropic_key:
            try:
                logger.info("Claude Sonnet fallback (izahname) baslatiliyor: %s", company_name)
                async with httpx.AsyncClient(timeout=_AI_TIMEOUT) as client:
                    resp = await client.post(
                        _ANTHROPIC_URL,
                        headers={
                            "x-api-key": anthropic_key,
                            "anthropic-version": "2023-06-01",
                            "content-type": "application/json",
                        },
                        json={
                            "model": _CLAUDE_MODEL,
                            "max_tokens": 4096,
                            "system": full_system,
                            "messages": [
                                {"role": "user", "content": user_message},
                            ],
                            "temperature": 0.15,
                        },
                    )

                if resp.status_code == 200:
                    data = resp.json()
                    # Anthropic format: { content: [{ type: "text", text: "..." }] }
                    content_blocks = data.get("content", [])
                    for block in content_blocks:
                        if block.get("type") == "text":
                            content = block.get("text", "").strip()
                            break
                    if content:
                        provider_used = "Claude-Sonnet"
                        logger.info("İzahname analizi Claude Sonnet ile uretildi: %s", company_name)
                    else:
                        logger.warning(
                            "Claude 200 OK ama content bos! stop_reason=%s (%s)",
                            data.get("stop_reason"), company_name,
                        )
                else:
                    logger.error(
                        "Claude izahname hatasi HTTP %d — %s",
                        resp.status_code, resp.text[:300],
                    )
            except Exception as e:
                logger.error("Claude izahname hata (%s) — %s", company_name, e)

        if not content:
            logger.error("AI boş izahname analizi döndü (tüm providerlar basarisiz) — %s", company_name)
            return None

        # JSON parse — safe_parse_json ile bozuk AI ciktisini kurtar
        from app.services.ai_json_helper import safe_parse_json

        result = safe_parse_json(content, required_key="positives")
        if result is None:
            logger.error("AI izahname JSON parse basarisiz — icerik: %s", content[:300])
            return None

        # Zorunlu alanlar
        required = ["positives", "negatives", "summary", "risk_level"]
        missing  = [k for k in required if k not in result]
        if missing:
            logger.error("AI izahname analizi eksik alanlar: %s", missing)
            return None

        # Madde sayısı kontrolü (3-5)
        pos_count = len(result.get("positives", []))
        neg_count = len(result.get("negatives", []))
        if pos_count < 2 or neg_count < 2:
            logger.warning("AI az madde döndü: %d pozitif, %d negatif — %s",
                           pos_count, neg_count, company_name)

        # ─── Hallüsinasyon filtresi: "eksik/bulunamadı" içeren maddeleri otomatik sil ───
        _HALLUCINATION_PATTERNS = [
            "bulunamadı", "bulunamadi", "eksik", "görülemiyor", "gorulemiyor",
            "tespit edilemedi", "yapılamıyor", "yapilamiyor", "belirtilmemiş",
            "belirtilmemis", "mevcut değil", "mevcut degil", "veri yok",
            "bilgi yok", "okunamadı", "okunamadi", "çıkarılamadı", "cikarilamadi",
            "yetersiz", "detay yok", "erişilemiyor", "erisilemiyor",
            "tam değerlendirme yapılamı", "tam degerlendirme yapilami",
            "kritik finansal veri", "tablo tespit", "izahnamede yer almı",
            "net bir değerlendirme", "net bir degerlendirme",
            "sınırlı bilgi", "sinirli bilgi", "yeterli veri",
            # v3: Ek halüsinasyon kalıpları
            "yer almıyor", "yer almiyor", "yer almamakta",
            "açıklama yapılmamış", "aciklama yapilmamis",
            "fon kullanım yerleri yer alm", "fon kullanim yerleri yer alm",
            "detaylı bilgi verilmemiş", "detayli bilgi verilmemis",
            "okuyamadım", "okuyamadim", "çözümleyemedim", "cozumleyemedim",
            "ayrıntılı dağılım", "ayrintili dagilim",
            "net bir şekilde belirtilm", "net bir sekilde belirtilm",
        ]

        # Genellemeleri ve bilgi değeri düşük maddeleri filtrele
        _LOW_VALUE_PATTERNS = [
            "kayıtlı sermaye tavanı",
            "piyasa koşullarına bağlı",
            "piyasa kosullarina bagli",
            "sektörde rekabetin artması",
            "yatırımcılar dikkatli",
            "dikkatli değerlendirme",
            "yatırımcıların riskleri",
            "riskleri dikkate",
            "büyüme potansiyeli bulunmakta",
        ]

        def _is_hallucination(text: str) -> bool:
            t = text.lower()
            return any(pat in t for pat in _HALLUCINATION_PATTERNS)

        def _is_low_value(text: str) -> bool:
            t = text.lower()
            return any(pat in t for pat in _LOW_VALUE_PATTERNS)

        orig_pos = len(result.get("positives", []))
        orig_neg = len(result.get("negatives", []))
        result["positives"] = [
            p for p in result.get("positives", [])
            if not _is_hallucination(p) and not _is_low_value(p)
        ]
        result["negatives"] = [
            n for n in result.get("negatives", [])
            if not _is_hallucination(n) and not _is_low_value(n)
        ]
        filtered = (orig_pos - len(result["positives"])) + (orig_neg - len(result["negatives"]))
        if filtered:
            logger.info("Hallüsinasyon/düşük değer filtresi: %d madde silindi — %s", filtered, company_name)

        # ─── Kısaltma filtresi: yaygın kısaltmalar kaldıysa düzelt ───
        _ABBREVIATION_MAP = {
            "NNA ": "Net nakit akışı ", "FAVÖK ": "Faiz ve amortisman öncesi kâr ",
            "FK ": "Fiyat/Kazanç ", "PD/DD": "Piyasa Değeri/Defter Değeri",
            "ÖS ": "Özsermaye ", "YP ": "Yabancı para ",
        }

        def _fix_abbreviations(text: str) -> str:
            for abbr, full in _ABBREVIATION_MAP.items():
                if abbr in text:
                    text = text.replace(abbr, full)
            return text

        result["positives"] = [_fix_abbreviations(p) for p in result["positives"]]
        result["negatives"] = [_fix_abbreviations(n) for n in result["negatives"]]

        # Karakter limitini uygula (220 karakter/madde — 180 prompt limiti + güvenlik payı)
        # Cümleyi ortasından kesmek yerine, son tam kelimede kes
        def _smart_truncate(text: str, max_len: int = 220) -> str:
            if len(text) <= max_len:
                return text
            # Son noktaya kadar kes (tam cümle)
            truncated = text[:max_len]
            last_period = truncated.rfind('.')
            last_semi = truncated.rfind(';')
            last_dash = truncated.rfind(' —')
            cut_point = max(last_period, last_semi, last_dash)
            if cut_point > max_len * 0.5:  # En az yarısı kalmalı
                return truncated[:cut_point + 1].strip()
            # Son boşluğa kadar kes (tam kelime)
            last_space = truncated.rfind(' ')
            if last_space > max_len * 0.6:
                return truncated[:last_space].strip()
            return truncated.strip()

        result["positives"] = [_smart_truncate(p) for p in result["positives"][:10]]
        result["negatives"] = [_smart_truncate(n) for n in result["negatives"][:10]]

        logger.info(
            "İzahname AI analizi tamamlandı: %s — %d olumlu, %d olumsuz, risk=%s",
            company_name, len(result["positives"]), len(result["negatives"]),
            result.get("risk_level", "?"),
        )
        return result

    except json.JSONDecodeError as e:
        logger.error("İzahname AI JSON parse hatası: %s — content: %s",
                     e, content[:300] if 'content' in dir() else "N/A")
        return None
    except httpx.TimeoutException:
        logger.error("İzahname AI TIMEOUT (%d sn): %s", _AI_TIMEOUT, company_name)
        return None
    except Exception as e:
        logger.error("İzahname AI hatası: %s — %s — %s", company_name, type(e).__name__, e)
        return None


# ─────────────────────────────────────────────────────────────
# Hızlı Analiz — DB verisinden (PDF indirme yok, < 1 dk)
# ─────────────────────────────────────────────────────────────

async def analyze_from_db_data(ipo_id: int) -> bool:
    """DB'deki şirket bilgilerinden izahname analizi yapar.

    PDF indirme / Vision OCR gerektirmez. Admin panelindeki
    şirket açıklaması + fon kullanım + finansalları kullanır.
    Sonuç < 1 dakikada gelir ve DB'ye kaydedilir.
    """
    try:
        from app.database import async_session
        from app.models.ipo import IPO
        from sqlalchemy import select

        async with async_session() as db:
            result = await db.execute(select(IPO).where(IPO.id == ipo_id))
            ipo = result.scalar_one_or_none()
            if not ipo:
                logger.error("IPO bulunamadı: id=%d", ipo_id)
                return False

            company_name = ipo.company_name
            ipo_price    = str(ipo.ipo_price) if ipo.ipo_price else None

            # DB'deki tüm mevcut bilgileri context olarak derle
            lines = [f"ŞİRKET: {company_name}"]
            if ipo.ticker:
                lines.append(f"TICKER: {ipo.ticker}")
            if ipo.sector:
                lines.append(f"SEKTÖR: {ipo.sector}")
            if ipo_price:
                lines.append(f"HALKA ARZ FİYATI: {ipo_price} TL")
            if ipo.offer_size:
                lines.append(f"ARZ BÜYÜKLÜĞÜ: {float(ipo.offer_size):,.0f} TL")
            if ipo.public_float_pct:
                lines.append(f"HALKA AÇIKLIK: %{ipo.public_float_pct}")
            lines.append("")

            if ipo.company_description:
                lines.append("=== ŞİRKET AÇIKLAMASI ===")
                lines.append(ipo.company_description)
                lines.append("")

            if ipo.fund_use_goals:
                lines.append("=== FON KULLANIM YERLERİ ===")
                goals = ipo.fund_use_goals
                if isinstance(goals, list):
                    for g in goals:
                        lines.append(f"• {g}")
                else:
                    lines.append(str(goals))
                lines.append("")

            fin_lines = []
            if ipo.current_revenue:
                fin_lines.append(f"Güncel yıl hasılat: {float(ipo.current_revenue):,.0f} TL")
            if ipo.prev_revenue:
                fin_lines.append(f"Önceki yıl hasılat: {float(ipo.prev_revenue):,.0f} TL")
            if ipo.gross_profit:
                fin_lines.append(f"Brüt kâr: {float(ipo.gross_profit):,.0f} TL")
            if fin_lines:
                lines.append("=== FİNANSAL ÖZET ===")
                lines.extend(fin_lines)
                lines.append("")

            if ipo.distribution_method:
                lines.append(f"DAĞITIM YÖNTEMİ: {ipo.distribution_method}")
            if ipo.lock_up_days:
                lines.append(f"LOCK-UP: {ipo.lock_up_days} gün")

            db_context = "\n".join(lines)

        if len(db_context.strip()) < 100:
            logger.error("DB'de yeterli şirket bilgisi yok (<%d): ipo_id=%d",
                         len(db_context), ipo_id)
            return False

        logger.info("DB analizi başlıyor: %s — %d karakter context", company_name, len(db_context))

        # AI analiz
        analysis = await analyze_with_ai(db_context, company_name, ipo_price)
        if not analysis:
            logger.error("DB analizi — AI başarısız: ipo_id=%d", ipo_id)
            return False

        # 0 bulgu kontrolü — boş analiz sonucu DB'ye kaydedilmez (frontend crash önlenir)
        _pos = analysis.get("positives", [])
        _neg = analysis.get("negatives", [])
        if len(_pos) == 0 and len(_neg) == 0:
            logger.warning("DB analizi — 0 bulgu, DB'ye kaydedilmiyor: %s (ipo_id=%d)", company_name, ipo_id)
            return False

        # DB'ye kaydet
        async with async_session() as db:
            result = await db.execute(select(IPO).where(IPO.id == ipo_id))
            ipo = result.scalar_one_or_none()
            if not ipo:
                return False
            ipo.prospectus_analysis    = json.dumps(analysis, ensure_ascii=False)
            ipo.prospectus_analyzed_at = datetime.now(timezone.utc)
            ipo.updated_at             = datetime.now(timezone.utc)
            await db.commit()
            logger.info("DB analizi kaydedildi: %s", company_name)

        # Görsel üret + tweet
        await _post_analysis_actions(ipo_id, analysis, company_name, ipo_price)
        return True

    except Exception as e:
        logger.error("DB analizi hata (ipo_id=%d): %s — %s", ipo_id, type(e).__name__, e)
        return False


# ─────────────────────────────────────────────────────────────
# Ana Orkestratör
# ─────────────────────────────────────────────────────────────

async def analyze_prospectus(ipo_id: int, pdf_url: str, delay_seconds: int = 0) -> bool:
    """İzahname PDF'ini indir, analiz et, DB'ye kaydet, görsel üret, tweet at.

    Args:
        ipo_id: IPO DB kaydı ID'si
        pdf_url: İzahname PDF URL'si
        delay_seconds: Gecikme (birden fazla PDF varsa 5 dk ara için)

    Returns:
        True başarılı, False başarısız
    """
    if delay_seconds > 0:
        logger.info("İzahname analiz başlamadan %d sn bekleniyor: ipo_id=%d",
                    delay_seconds, ipo_id)
        await asyncio.sleep(delay_seconds)

    logger.info("İzahname analizi başlıyor: ipo_id=%d, url=%s", ipo_id, pdf_url)

    try:
        from app.database import async_session
        from app.models.ipo import IPO
        from sqlalchemy import select

        # IPO bilgilerini al
        async with async_session() as db:
            result = await db.execute(select(IPO).where(IPO.id == ipo_id))
            ipo = result.scalar_one_or_none()
            if not ipo:
                logger.error("IPO bulunamadı: id=%d", ipo_id)
                return False

            # Zaten analiz edilmişse atla (tekrar tetiklenme koruması)
            if ipo.prospectus_analysis:
                logger.info("İzahname zaten analiz edilmiş: %s", ipo.company_name)
                return True

            company_name = ipo.company_name
            ipo_price    = str(ipo.ipo_price) if ipo.ipo_price else None

        # PDF indir
        pdf_path = await download_pdf(pdf_url)
        if not pdf_path:
            logger.error("PDF indirilemedi: %s", pdf_url)
            return False

        # Metin çıkar (sync — run in executor) → (text, pages_count) tuple
        import gc
        loop = asyncio.get_running_loop()
        pdf_text, pages_analyzed = await loop.run_in_executor(None, extract_pdf_text, pdf_path)
        gc.collect()  # PDF işleme sonrası ağır bellek temizliği
        if not pdf_text or len(pdf_text) < 200:
            logger.error("PDF metni çok kısa veya boş (%d karakter): %s",
                         len(pdf_text) if pdf_text else 0, pdf_url)
            return False
        logger.info("PDF metin çıkarıldı: %d karakter, %d sayfa — ipo_id=%d",
                    len(pdf_text), pages_analyzed, ipo_id)

        # AI analiz (PDF metni artık memory'de, ama PDF objeleri temizlendi)
        analysis = await analyze_with_ai(pdf_text, company_name, ipo_price)
        # AI analiz sonrası pdf_text artık gerekli değil — serbest bırak
        del pdf_text
        gc.collect()
        if not analysis:
            logger.error("AI analizi başarısız: ipo_id=%d", ipo_id)
            return False

        # 0 bulgu kontrolü — boş analiz sonucu DB'ye kaydedilmez (frontend crash önlenir)
        _pos = analysis.get("positives", [])
        _neg = analysis.get("negatives", [])
        if len(_pos) == 0 and len(_neg) == 0:
            logger.warning("İzahname analizi — 0 bulgu, DB'ye kaydedilmiyor: %s (ipo_id=%d)", company_name, ipo_id)
            return False

        # DB'ye kaydet + görsel üret + tweet
        async with async_session() as db:
            result = await db.execute(select(IPO).where(IPO.id == ipo_id))
            ipo = result.scalar_one_or_none()
            if not ipo:
                return False

            ipo.prospectus_analysis     = json.dumps(analysis, ensure_ascii=False)
            ipo.prospectus_analyzed_at  = datetime.now(timezone.utc)
            ipo.updated_at              = datetime.now(timezone.utc)
            await db.commit()
            await db.refresh(ipo)

            logger.info("İzahname analizi DB'ye kaydedildi: %s", company_name)

        # Görsel üret ve Tweet at (arka planda, DB session dışında)
        await _post_analysis_actions(ipo_id, analysis, company_name, ipo_price, pages_analyzed)

        return True

    except Exception as e:
        logger.error("İzahname analiz orkestrasyon hatası (ipo_id=%d): %s — %s",
                     ipo_id, type(e).__name__, e)
        return False


async def _post_analysis_actions(
    ipo_id: int,
    analysis: dict,
    company_name: str,
    ipo_price: Optional[str],
    pages_analyzed: int = 0,
):
    """Görsel üret ve tweet at — DB kaydından bağımsız arka plan işlemi."""
    try:
        from app.database import async_session
        from app.models.ipo import IPO
        from sqlalchemy import select

        # Görsel üret
        img_path = None
        try:
            from app.services.prospectus_image import generate_prospectus_analysis_image
            img_path = await asyncio.get_running_loop().run_in_executor(
                None,
                generate_prospectus_analysis_image,
                company_name,
                ipo_price,
                analysis,
                ipo_id,
                pages_analyzed,
            )
            logger.info("İzahname görseli üretildi: %s", img_path)
        except Exception as img_err:
            logger.warning("İzahname görseli üretilemedi: %s", img_err)

        # Tweet at
        try:
            async with async_session() as db:
                result = await db.execute(select(IPO).where(IPO.id == ipo_id))
                ipo = result.scalar_one_or_none()
                if ipo and not ipo.prospectus_tweeted:
                    from app.services.twitter_service import tweet_izahname_analysis
                    ok = tweet_izahname_analysis(ipo, analysis, img_path)
                    if ok:
                        ipo.prospectus_tweeted = True
                        await db.commit()
                    logger.info("İzahname tweet: %s — ok=%s", company_name, ok)

        except Exception as tw_err:
            logger.warning("İzahname tweet hatası: %s", tw_err)

        # Admin Telegram bildirimi
        try:
            from app.services.admin_telegram import send_admin_message
            risk_emoji = {"düşük": "🟢", "orta": "🟡", "yüksek": "🟠", "çok yüksek": "🔴"}
            emoji = risk_emoji.get(analysis.get("risk_level", ""), "⚪")
            lines = [
                f"📋 İzahname Analizi Tamamlandı",
                f"Şirket: {company_name}",
                f"Risk: {emoji} {analysis.get('risk_level', '?')}",
                f"Özet: {analysis.get('summary', '')[:200]}",
                f"Görsel: {'✅' if img_path else '❌'}",
            ]
            await send_admin_message("\n".join(lines))
        except Exception:
            pass

    except Exception as e:
        logger.error("Post-analysis actions hatası: %s", e)


# ─────────────────────────────────────────────────────────────
# Çoklu PDF Desteği (5 dk aralıklı)
# ─────────────────────────────────────────────────────────────

async def analyze_multiple_prospectuses(ipo_id: int, pdf_urls: list[str]) -> None:
    """Birden fazla PDF varsa 5 dk aralıkla analiz eder.

    İlk PDF hemen analiz edilir, sonraki her PDF 300 sn (5 dk) bekler.
    """
    if not pdf_urls:
        return

    unique_urls = list(dict.fromkeys(pdf_urls))  # Tekrarları kaldır

    for i, url in enumerate(unique_urls):
        delay = i * 300   # 0, 300, 600, ... saniye
        try:
            ok = await analyze_prospectus(ipo_id, url, delay_seconds=delay)
            logger.info(
                "İzahname %d/%d analiz: ipo_id=%d, ok=%s, url=%s",
                i + 1, len(unique_urls), ipo_id, ok, url,
            )
        except Exception as e:
            logger.error("Çoklu PDF analiz hatası %d: %s", i, e)
