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
_AI_MODEL   = "claude-sonnet-4-5"
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
]

# ─────────────────────────────────────────────────────────────
# SYSTEM PROMPT — Hallüsinasyon koruması + Yüksek kalite
# ─────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """Sen Türkiye sermaye piyasaları uzmanı, kıdemli bir halka arz analistisin. Görevin: izahname PDF'inden yatırımcının para yatırma/yatırmama kararını etkileyecek SOMUT, KRİTİK bilgileri çıkarmak.

SEN BİR ARAŞTIRMA RAPORU YAZIYORSUN — köşe yazısı DEĞİL. Her cümle bilgi taşımalı.

═══ MUTLAK YASAKLAR ═══
• HALLÜSINASYON: PDF'te yazmayan bilgiyi uydurma. Emin değilsen o maddeyi YAZMA.
• GEÇİŞTİRME: "...olabilir", "...muhtemeldir", "...beklenebilir" gibi belirsiz ifadeler YASAK.
• DOLGU MADDE: Bilgi değeri sıfır olan maddeler YASAK. Her madde yatırımcıya yeni bilgi vermeli.
• DEVRİK CÜMLE: Türkçe düzgün, özne-yüklem uyumlu olmalı. Devrik/karmaşık cümle kurma.

═══ YASAK MADDE ÖRNEKLERİ (bunları asla yazma) ═══
✗ "Kayıtlı sermaye tavanı X TL olarak belirlenmiştir" → HERKESİN izahnamesinde var, bilgi değeri YOK
✗ "Piyasa koşullarına bağlı riskler mevcuttur" → Her şirket için geçerli, spesifik değil
✗ "Şirketin büyüme potansiyeli bulunmaktadır" → Kanıtsız genel yorum
✗ "Halka arz geliri işletme sermayesine kullanılacaktır" → Ne kadar, ne için? Detay yoksa yazma
✗ "Sektörde rekabetin artması risk oluşturabilir" → Her sektörde rekabet var
✗ "Yatırımcılar dikkatli değerlendirmelidir" → Tavsiye değil analiz yap

═══ İYİ MADDE ÖRNEKLERİ (bu kalitede yaz) ═══
✓ "2022-2024 hasılat CAGR %78 — sektör ortalaması %12'nin 6 katı büyüme"
✓ "Toplam borcun %81'i kısa vadeli; 94M TL borç vs 12M TL nakit — likidite riski"
✓ "Halka arz gelirinin %100'ü şirkete; ortak satışı yok — tüm sermaye büyümeye gidiyor"
✓ "En büyük müşteri cironun %47'si — tek müşteriye aşırı bağımlılık"
✓ "BDDK lisanslı ödeme kuruluşu — lisans kaybı durumunda faaliyet durur"

═══ ANALİZ ADIMLARI (sırayla tara) ═══
1. FİNANSAL: Hasılat, net kar/zarar, EBITDA, borç yapısı, nakit, karlılık marjları
2. RİSK: Lisans/ruhsat, tek müşteri bağımlılığı, kur riski, davalar, ilişkili taraf
3. FON KULLANIMI: Sermaye artırımı mı ortak çıkışı mı? Para nereye gidiyor (%)
4. ORTAKLIK: HA sonrası %'ler, lock-up süreleri, yönetim çıkışı var mı
5. BÜYÜME: CAGR, pazar payı, kapasite, AR-GE, ihracat
6. HUKUKİ: Devam eden davalar (tutar!), vergi ihtilafları, düzenleyici risk

═══ ÇIKTI FORMAT (geçerli JSON) ═══
{
  "positives": ["somut olumlu — mutlaka rakam/yüzde/tutar içermeli", ...],
  "negatives": ["somut olumsuz — mutlaka rakam/risk/tutar içermeli", ...],
  "summary": "Düzgün Türkçe, 1-2 kısa cümle. Yatırımcıya net mesaj. DEVRİK CÜMLE KURMA.",
  "risk_level": "düşük|orta|yüksek|çok yüksek",
  "key_risk": "en kritik tek risk (max 100 karakter)"
}

═══ MADDE KURALLARI ═══
• Hedef: olumlu 5-7, olumsuz 5-7. PDF yetersizse 3-4 de olur — uydurma YASAK.
• Her madde FARKLI konu. Aynı konuyu tekrarlama — tekrar edeceksen yazma.
• En az 3 farklı kategori (Finansal/Risk/Fon/Ortaklık/Büyüme/Hukuki).
• Max 130 karakter. Kısa, yoğun, bilgi dolu. Türkçe, net.
• "summary" alanı: DÜZ CÜMLE yaz. Özne + nesne + yüklem sırası. Devrik yapma.
SADECE JSON döndür — başka hiçbir şey yazma."""


_FEW_SHOT_EXAMPLES = """
═══ REFERANS ANALİZ ÖRNEKLERİ ═══

ÖRNEK — Kimya Sektörü Şirketi (iyi analiz):
{
  "positives": [
    "2023 hasılatı 892M TL, EBITDA marjı %18.4 — sektör ortalaması %11",
    "İhracat payı %34; Avrupa ve Orta Asya'ya 28 ülkeye satış — coğrafi çeşitlilik",
    "Fon kullanımı: %45 yeni üretim hattı + %30 AR-GE — somut kapasite artışı",
    "Ortak satışı yok, halka arz gelirinin %100'ü şirkete gidiyor",
    "5 patent + 12 faydalı model tescili — teknoloji bariyeri yüksek"
  ],
  "negatives": [
    "Kısa vadeli borç 340M TL, toplam borcun %76'sı — refinansman riski",
    "En büyük 3 müşteri cironun %52'si — müşteri konsantrasyon riski yüksek",
    "Ham madde maliyetleri dövize endeksli; kur %10 artışta marj 3 puan düşer",
    "2022'de 45M TL net zarar; 2023'te kara geçiş henüz 1 yıllık",
    "Devam eden 3 dava, toplam risk tutarı 28M TL"
  ],
  "summary": "Şirket ihracat ağırlıklı güçlü hasılata sahip ancak kısa vadeli borç yükü ve müşteri bağımlılığı dikkat çekiyor.",
  "risk_level": "orta",
  "key_risk": "Kısa vadeli borç oranı %76 — refinansman riski"
}

✗ KÖTÜ ÖRNEKLER (asla böyle yazma):
✗ "Kayıtlı sermaye tavanı 500M TL olarak belirlenmiştir" → bilgi değeri sıfır
✗ "Şirketin güçlü bir büyüme potansiyeli bulunmaktadır" → kanıtsız, genel
✗ "Piyasa dalgalanmalarından etkilenebilir" → her şirket için geçerli
✗ "Yatırımcıların riskleri dikkate alması gerekmektedir" → tavsiye değil analiz yap
✗ "Sektörde artan rekabet baskısı söz konusu olabilir" → spesifik değil

═══ FİNANSAL ANALİZ KILAVUZU (karşılaştırma için kullan) ═══
Borç/Özkaynak: <%50 iyi, %50-100 normal, >%100 riskli, >%200 tehlikeli
Kısa Vadeli Borç Oranı: <%60 iyi, >%70 riskli — refinansman baskısı
Cari Oran: >1.5 sağlıklı, <1.0 likidite riski
Net Kar Marjı: Sektöre göre değişir ama negatif = zarar, <%5 zayıf
Hasılat Büyümesi (CAGR): >%20 güçlü, %10-20 normal, <%10 zayıf
Tek Müşteri Bağımlılığı: >%30 dikkat, >%50 ciddi risk
İlişkili Taraf İşlemleri: Cironun >%20'si ise şeffaflık riski
Ortak Satışı: %100 şirkete → çok iyi, karışık → fon kullanımına bak
Lock-up: 180 gün standart, daha kısa → ortak güvensizliği olabilir
"""


# ─────────────────────────────────────────────────────────────
# PDF İndirme + Metin Çıkarma
# ─────────────────────────────────────────────────────────────

async def download_pdf(url: str) -> Optional[str]:
    """PDF'i geçici dosyaya indirir, dosya yolunu döner."""
    try:
        async with httpx.AsyncClient(
            timeout=60.0,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SZAlgo/1.0)"},
        ) as client:
            resp = await client.get(url)
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
            tmp_file.write(resp.content)
            tmp_file.close()

            file_size_kb = len(resp.content) // 1024
            logger.info("PDF indirildi: %s (%d KB)", url, file_size_kb)
            return tmp_file.name

    except httpx.TimeoutException:
        logger.error("PDF indirme TIMEOUT: %s", url)
        return None
    except Exception as e:
        logger.error("PDF indirme hatası: %s — %s", url, e)
        return None


def _extract_pages_pymupdf(pdf_path: str) -> list:
    """PyMuPDF (fitz) ile sayfa metinlerini çıkar."""
    try:
        import fitz  # PyMuPDF
        pages = []
        doc = fitz.open(pdf_path)
        for i, page in enumerate(doc):
            text = page.get_text("text") or ""
            if text.strip():
                pages.append((i, text))
        doc.close()
        logger.info("PyMuPDF: %d sayfadan %d'ünde metin bulundu", len(doc), len(pages))
        return pages
    except Exception as e:
        logger.warning("PyMuPDF çıkarma hatası: %s", e)
        return []


def _extract_pages_pdfplumber(pdf_path: str) -> list:
    """pdfplumber ile sayfa metinlerini çıkar."""
    try:
        pages = []
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                try:
                    text = page.extract_text() or ""
                    if text.strip():
                        pages.append((i, text))
                except Exception:
                    continue
        logger.info("pdfplumber: %d sayfada metin bulundu", len(pages))
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

        # Akıllı sayfa örnekleme
        if total_pages <= 40:
            sample_indices = list(range(total_pages))
        else:
            start_pages = list(range(20))                                         # İlk 20: kapak+özet+risk başı
            mid1 = total_pages // 3
            mid1_pages = list(range(mid1, min(mid1 + 5, total_pages)))            # 1/3: risk ortası
            mid2 = total_pages * 2 // 3
            mid2_pages = list(range(mid2, min(mid2 + 5, total_pages)))            # 2/3: finansal
            end_pages = list(range(max(total_pages - 5, 0), total_pages))         # Son 5: ek bilgiler
            sample_indices = sorted(set(start_pages + mid1_pages + mid2_pages + end_pages))

        logger.info("Tesseract OCR: %d/%d sayfa örnekleniyor...", len(sample_indices), total_pages)

        # 200 DPI — Tesseract için ideal kalite (72 DPI'nın ~7.7 katı çözünürlük)
        mat = fitz.Matrix(200 / 72, 200 / 72)
        pages = []

        for i in sample_indices:
            try:
                pix = doc[i].get_pixmap(matrix=mat, colorspace=fitz.csGRAY)
                img_bytes = pix.tobytes("png")
                pix = None  # Hızlı GC

                img = PilImage.open(io.BytesIO(img_bytes))
                # --psm 6: Düzgün metin bloğu olarak işle (izahname sayfaları için ideal)
                text = pytesseract.image_to_string(img, lang="tur+eng", config="--psm 6 --oem 3")
                img.close()

                if text.strip():
                    pages.append((i, text))

            except Exception as pe:
                logger.warning("Tesseract sayfa %d hatası: %s", i, pe)
                continue

        doc.close()

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
        api_key = get_settings().ABACUS_API_KEY
        if not api_key:
            logger.warning("Vision OCR: Abacus API key yok")
            return []

        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        # Akıllı sayfa örnekleme: başı + ortası + sonu
        # İzahnamede: kapak(1-3), özet(4-10), risk(10-30), finansal(30-60+)
        # Her bölümden 2’şer sayfa = 3 batch × 2 sayfa = 6 sayfa, ~3 API çağrısı
        if total_pages <= 8:
            # Kısa belge: tamamını al
            sample_indices = list(range(total_pages))
        else:
            # Stratejik örnekleme: 3 bölgeden 2’şer sayfa
            s1 = max(0, min(3, total_pages - 1))               # Sayfa 4-5 (özet başı)
            s2 = max(0, total_pages // 3)                      # 1/3 noktası (risk faktörleri)
            s3 = max(0, total_pages * 2 // 3)                  # 2/3 noktası (finansal)
            sample_indices = sorted(set([
                s1, min(s1+1, total_pages-1),
                s2, min(s2+1, total_pages-1),
                s3, min(s3+1, total_pages-1),
            ]))

        # Sayfaları 150 DPI gri JPEG olarak render et (daha iyi OCR kalitesi)
        page_images = []
        mat = fitz.Matrix(1.5, 1.5)  # 108 DPI — 72 DPI’dan 2x daha iyi kalite
        for i in sample_indices:
            try:
                pix = doc[i].get_pixmap(matrix=mat, colorspace=fitz.csGRAY)
                img_bytes = pix.tobytes("jpeg")
                b64 = base64.b64encode(img_bytes).decode("utf-8")
                page_images.append((i, b64))
                pix = None  # Hızlı GC
            except Exception as pe:
                logger.warning("Vision OCR sayfa render hatası s.%d: %s", i, pe)
        doc.close()

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
                with _httpx.Client(timeout=90) as hcl:  # 90s per batch
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
                    all_texts.append((batch_start, extracted))
                    logger.info(
                        "Vision OCR batch s.%d-%d: %d karakter",
                        batch_start + 1, end_page, len(extracted),
                    )
                else:
                    logger.warning(
                        "Vision OCR HTTP %d: %s",
                        resp.status_code, resp.text[:200],
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


def extract_pdf_text(pdf_path: str) -> tuple[Optional[str], int]:
    """PDF’ten metin çıkarır. PyMuPDF → pdfplumber → Tesseract → Vision OCR.

    Returns: (metin veya None, analiz edilen sayfa sayısı)
    """
    try:
        # Önce PyMuPDF dene
        page_tuples = _extract_pages_pymupdf(pdf_path)

        # Boşsa pdfplumber ile dene
        if not page_tuples:
            logger.info("PyMuPDF 0 karakter — pdfplumber deneniyor...")
            page_tuples = _extract_pages_pdfplumber(pdf_path)

        # ─── Yetersiz metin kontrolü ───
        # PyMuPDF/pdfplumber metin bulduysa ama çok azsa (sayfa başı <100 karakter)
        # bu genellikle dijital imza metadata'sı veya boş sayfa demek → OCR'a düş
        _text_is_insufficient = False
        if page_tuples:
            _total_chars = sum(len(t) for _, t in page_tuples)
            _total_pages = len(page_tuples)
            _avg_chars_per_page = _total_chars / max(_total_pages, 1)
            # Ayrıca PDF'in gerçek sayfa sayısını kontrol et
            try:
                import fitz
                _doc = fitz.open(pdf_path)
                _real_page_count = len(_doc)
                _doc.close()
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

        # İkisi de boşsa/yetersizse Tesseract OCR dene (yerel, ücretsiz, 100+ sayfa)
        if not page_tuples:
            logger.info("PyMuPDF/pdfplumber %s — Tesseract OCR deneniyor...",
                        "yetersiz metin" if _text_is_insufficient else "0 karakter")
            page_tuples = _extract_pages_tesseract_sync(pdf_path)

        # Tesseract da boşsa Vision OCR dene (son çare — Claude API)
        if not page_tuples:
            logger.info("Tesseract 0 karakter — Vision OCR (Claude) deneniyor...")
            page_tuples = _extract_pages_vision_sync(pdf_path)

        if not page_tuples:
            logger.warning("PDF’ten hiçbir yöntemle metin çıkarılamadı: %s", pdf_path)
            return None, 0

        all_pages_text = []
        risk_section_text = []
        finance_section_text = []
        fund_usage_text = []      # Fon kullanım yerleri
        ownership_text = []       # Ortaklık yapısı
        in_risk_section = False
        in_finance_section = False
        in_fund_section = False
        in_ownership_section = False

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
            if in_finance_section and len(finance_section_text) > 12:
                in_finance_section = False
            if in_finance_section:
                finance_section_text.append(f"[S.{i+1}] {text}")

            # Fon kullanımı bölümü
            if any(kw in text_lower for kw in ["fon kullanım", "halka arz geliri", "sermaye artırımı"]):
                in_fund_section = True
            if in_fund_section and len(fund_usage_text) > 5:
                in_fund_section = False
            if in_fund_section:
                fund_usage_text.append(f"[S.{i+1}] {text}")

            # Ortaklık yapısı bölümü
            if any(kw in text_lower for kw in ["ortaklık yapısı", "pay sahipliği", "sermaye yapısı", "lock-up", "lock up"]):
                in_ownership_section = True
            if in_ownership_section and len(ownership_text) > 5:
                in_ownership_section = False
            if in_ownership_section:
                ownership_text.append(f"[S.{i+1}] {text}")

        if not all_pages_text:
            logger.warning("PDF’ten metin çıkarılamadı: %s", pdf_path)
            return None, 0

        # Öncelikli metin birleştirme — önemli bölümler önce
        combined = ""

        if risk_section_text:
            risk_combined = "\n\n=== RİSK FAKTÖRLERİ BÖLÜMÜ ===\n" + "\n".join(risk_section_text)
            combined += risk_combined[:60_000]

        if finance_section_text:
            fin_combined = "\n\n=== FİNANSAL BİLGİLER ===\n" + "\n".join(finance_section_text)
            combined += fin_combined[:25_000]

        if fund_usage_text:
            fund_combined = "\n\n=== FON KULLANIM YERLERİ ===\n" + "\n".join(fund_usage_text)
            combined += fund_combined[:15_000]

        if ownership_text:
            own_combined = "\n\n=== ORTAKLIK YAPISI & LOCK-UP ===\n" + "\n".join(ownership_text)
            combined += own_combined[:15_000]

        full_text = "\n\n".join(all_pages_text)
        remaining_budget = _MAX_PDF_CHARS - len(combined)
        if remaining_budget > 10_000:
            start_chunk = full_text[:int(remaining_budget * 0.6)]
            end_chunk   = full_text[-int(remaining_budget * 0.4):]
            combined = (f"\n\n=== İZAHNAME TAM METNİ (ÖZET) ===\n{start_chunk}\n\n...[ORTA BÖLÜMLER ATLANMIŞ]...\n\n{end_chunk}\n\n{combined}")

        pages_count = len(all_pages_text)
        logger.info(
            "PDF metin çıkarıldı: %d sayfa → %d karakter (risk=%d, fin=%d, fon=%d, ort=%d)",
            pages_count, len(combined),
            len("".join(risk_section_text)),
            len("".join(finance_section_text)),
            len("".join(fund_usage_text)),
            len("".join(ownership_text)),
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
    """Çıkarılan PDF metni üzerinde AI analizi yapar."""

    api_key = get_settings().ABACUS_API_KEY
    if not api_key:
        logger.error("Abacus API key yok — izahname analizi yapılamadı")
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
    full_system = _SYSTEM_PROMPT + "\n\n" + _FEW_SHOT_EXAMPLES

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
                        {"role": "system", "content": full_system},
                        {"role": "user",   "content": user_message},
                    ],
                    "temperature": 0.15,  # Düşük ama yaratıcı analiz için biraz esneklik
                    "max_tokens": 2500,  # Daha detaylı analiz
                },
            )

        if resp.status_code != 200:
            logger.error("AI izahname analiz hatası: HTTP %d — %s",
                         resp.status_code, resp.text[:300])
            return None

        data    = resp.json()
        content = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
            .strip()
        )
        if not content:
            logger.error("AI boş izahname analizi döndü")
            return None

        # JSON parse
        if content.startswith("```"):
            content = content.split("\n", 1)[-1]
            if content.endswith("```"):
                content = content[:-3].strip()

        result = json.loads(content)

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

        # Karakter limitini uygula (130 karakter/madde)
        result["positives"] = [p[:140] for p in result["positives"][:8]]
        result["negatives"] = [n[:140] for n in result["negatives"][:8]]

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
        loop = asyncio.get_running_loop()
        pdf_text, pages_analyzed = await loop.run_in_executor(None, extract_pdf_text, pdf_path)
        if not pdf_text or len(pdf_text) < 200:
            logger.error("PDF metni çok kısa veya boş (%d karakter): %s",
                         len(pdf_text) if pdf_text else 0, pdf_url)
            return False
        logger.info("PDF metin çıkarıldı: %d karakter, %d sayfa — ipo_id=%d",
                    len(pdf_text), pages_analyzed, ipo_id)

        # AI analiz
        analysis = await analyze_with_ai(pdf_text, company_name, ipo_price)
        if not analysis:
            logger.error("AI analizi başarısız: ipo_id=%d", ipo_id)
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
