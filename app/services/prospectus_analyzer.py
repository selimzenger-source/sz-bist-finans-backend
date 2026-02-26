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
_AI_TIMEOUT = 180   # 140 sayfa PDF → uzun analiz → 3 dk

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

_SYSTEM_PROMPT = """Sen Türkiye sermaye piyasaları uzmanı bir halka arz analistsin. Görevin: izahname (prospektüs) PDF metnini analiz ederek yatırımcılar için EN KRİTİK noktaları bulmak.

TEMEL KURAL — HALLÜSINASYON YASAĞI:
• Sadece verilen PDF metninde gerçekten yazan bilgileri kullan
• Uydurma, tahmin, varsayım YASAK. Metinde yoksa yazma.
• Her madde PDF'ten doğrudan alıntı veya doğrudan çıkarım olmalı
• "Genellikle şirketler..." veya "Muhtemelen..." gibi genel bilgiler YASAK

ANALİZ STRATEJİSİ:
1. Risk Faktörleri bölümünü tara → Regülatif riskler, lisans/ruhsat kayıpları, bağımlılıklar
2. Hukuki Bilgiler bölümünü tara → Davalar, uyuşmazlıklar, cezalar
3. Finansal Özet bölümünü tara → Büyüme, karlılık, borç yükü
4. Fon kullanım yerleri → Gerçek yatırım mı, ortak çıkışı mı?
5. Yönetim ve Ortaklık yapısı → Kilit personel bağımlılığı, ilişkili taraf işlemleri

EN ÖNEMLİ DİPNOTLAR — bunlara ÖZELLIKLE dikkat et:
• "ruhsat/lisans muhafaza edilemeyebilir" → kritik negatif
• "tek/az sayıda müşteriye bağımlılık" → kritik negatif
• "kilit personel ayrılabilir" → önemli negatif
• "vergi uyuşmazlıkları / davalar" → önemli negatif
• "düzenleyici değişiklikler şirketi etkileyebilir" → orta negatif
• Güçlü büyüme rakamları (hasılat artışı, pazar payı) → olumlu
• Güçlü bilanço, düşük borç → olumlu
• Sektörel liderlik, patent, güçlü marka → olumlu

ÇIKTI FORMAT (kesinlikle geçerli JSON):
{
  "positives": [
    "max 130 karakter, somut olumlu dipnot — PDF'ten",
    ...
  ],
  "negatives": [
    "max 130 karakter, somut olumsuz dipnot/risk — PDF'ten",
    ...
  ],
  "summary": "izahname özeti — en kritik 1-2 cümle, yatırımcıya ne söylüyor",
  "risk_level": "düşük|orta|yüksek|çok yüksek",
  "key_risk": "en önemli tek risk faktörü (maksimum 100 karakter)"
}

MADDE SAYISI: Olumlu 3-5 madde, Olumsuz 3-5 madde. Daha az veya fazla KABUL EDİLMEZ.
UZUNLUK: Her madde maksimum 130 karakter. Türkçe, net, anlaşılır dil.
SADECE JSON döndür. Başka hiçbir şey yazma."""


_FEW_SHOT_EXAMPLES = """
İyi analiz örnekleri (bunları referans al):

ÖRNEK 1 — Metropal Kurumsal Hizmetler:
Pozitif: "Tera Yatırım liderliğiyle yıldız pazara açılıyor; ortak satışı yok, tüm gelir şirkete"
Pozitif: "%45 işletme sermayesi + %35 yurt içi/dışı şirket kurulumu — büyüme odaklı kullanım"
Negatif: "MetropolCard faaliyetleri için gerekli izin ve ruhsatlar muhafaza edilemeyebilir (İzahname s.94)"
Negatif: "İlişkili taraf işlemleri toplam cironun önemli bölümünü oluşturuyor — bağımlılık riski"

ÖRNEK 2 — Genel İyi Madde Formatı:
✓ "2022-2024 hasılat CAGR %78 — sektör ortalamasının 3 katı büyüme"
✓ "BDDK lisanslı ödeme kuruluşu; lisans yenileme riski mevzuatla bağlantılı"
✗ KÖTÜ: "Şirket büyüme potansiyeline sahip" (çok belirsiz, PDF'ten değil)
✗ KÖTÜ: "Piyasa koşullarına bağlı riskler mevcuttur" (her şirket için geçerli genel bilgi)
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
        max_pages = min(total_pages, 60)  # Maks 60 sayfa (maliyet kontrolü)

        # Sayfaları düşük çözünürlüklü gri JPEG olarak render et
        page_images = []
        mat = fitz.Matrix(1.0, 1.0)  # 72 DPI — küçük, token tasarruflu
        for i in range(max_pages):
            try:
                pix = doc[i].get_pixmap(matrix=mat, colorspace=fitz.csGRAY)
                img_bytes = pix.tobytes("jpeg")
                b64 = base64.b64encode(img_bytes).decode("utf-8")
                page_images.append((i, b64))
                pix = None  # Hızlı GC
            except Exception as pe:
                logger.warning("Vision OCR sayfa render hatası s.%d: %s", i, pe)
        doc.close()

        logger.info("Vision OCR: %d/%d sayfa render edildi", len(page_images), total_pages)

        # 15’er sayfalık batch’ler halinde Claude Vision’a gönder
        all_texts = []
        batch_size = 15

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
                with _httpx.Client(timeout=120) as hcl:
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


def extract_pdf_text(pdf_path: str) -> Optional[str]:
    """PDF’ten metin çıkarır. PyMuPDF → pdfplumber → Vision OCR.

    Strateji:
    1. PyMuPDF (fitz) dene — geniş PDF format desteği
    2. Sonuç boşsa pdfplumber dene
    3. İkisi de boşsa Vision OCR (Claude) dene — taranmış PDF’ler için
    4. Risk faktörleri bölümünü öncelikli tut
    5. _MAX_PDF_CHARS sınırında kırp
    """
    try:
        # Önce PyMuPDF dene
        page_tuples = _extract_pages_pymupdf(pdf_path)

        # Boşsa pdfplumber ile dene
        if not page_tuples:
            logger.info("PyMuPDF 0 karakter — pdfplumber deneniyor...")
            page_tuples = _extract_pages_pdfplumber(pdf_path)

        # İkisi de boşsa Vision OCR dene (taranmış PDF)
        if not page_tuples:
            logger.info("Her iki text extractor 0 karakter — Vision OCR (Claude) deneniyor...")
            page_tuples = _extract_pages_vision_sync(pdf_path)

        if not page_tuples:
            logger.warning("PDF’ten hiçbir yöntemle metin çıkarılamadı: %s", pdf_path)
            return None

        all_pages_text = []
        risk_section_text = []
        finance_section_text = []
        in_risk_section = False
        in_finance_section = False

        for i, text in page_tuples:
            all_pages_text.append(text)

            text_lower = text.lower()
            if any(kw in text_lower for kw in ["risk faktörleri", "riskler"]):
                in_risk_section = True
            if in_risk_section and any(kw in text_lower for kw in ["finansal bilgiler", "izahname özeti"]):
                in_risk_section = False

            if in_risk_section:
                risk_section_text.append(f"[S.{i+1}] {text}")

            if any(kw in text_lower for kw in ["finansal durum", "özet finansal"]):
                in_finance_section = True
            if in_finance_section and len(finance_section_text) > 10:
                in_finance_section = False

            if in_finance_section:
                finance_section_text.append(f"[S.{i+1}] {text}")

        if not all_pages_text:
            logger.warning("PDF’ten metin çıkarılamadı: %s", pdf_path)
            return None

        # Öncelikli metin birleştirme
        combined = ""

        if risk_section_text:
            risk_combined = "

=== RİSK FAKTÖRLERİ BÖLÜMÜ ===
" + "
".join(risk_section_text)
            combined += risk_combined[:60_000]

        if finance_section_text:
            fin_combined = "

=== FİNANSAL BİLGİLER ===
" + "
".join(finance_section_text)
            combined += fin_combined[:20_000]

        full_text = "

".join(all_pages_text)
        remaining_budget = _MAX_PDF_CHARS - len(combined)
        if remaining_budget > 10_000:
            start_chunk = full_text[:int(remaining_budget * 0.6)]
            end_chunk   = full_text[-int(remaining_budget * 0.4):]
            combined = f"

=== İZAHNAME TAM METNİ (ÖZET) ===
{start_chunk}

...[ORTA BÖLÜMLER ATLANMIŞ]...

{end_chunk}

{combined}"

        logger.info(
            "PDF metin çıkarıldı: %d sayfa → %d karakter (risk=%d, fin=%d)",
            len(all_pages_text), len(combined),
            len("".join(risk_section_text)),
            len("".join(finance_section_text)),
        )
        return combined[:_MAX_PDF_CHARS]

    except Exception as e:
        logger.error("PDF metin çıkarma hatası: %s — %s", pdf_path, e)
        return None
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
                    "temperature": 0.1,   # Düşük — hallüsinasyon azalt
                    "max_tokens": 2000,
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
        result["positives"] = [p[:130] for p in result["positives"][:5]]
        result["negatives"] = [n[:130] for n in result["negatives"][:5]]

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

        # Metin çıkar (sync — run in executor)
        loop = asyncio.get_event_loop()
        pdf_text = await loop.run_in_executor(None, extract_pdf_text, pdf_path)
        if not pdf_text or len(pdf_text) < 500:
            logger.error("PDF metni çok kısa veya boş: %s", pdf_url)
            return False

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
        await _post_analysis_actions(ipo_id, analysis, company_name, ipo_price)

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
            img_path = await asyncio.get_event_loop().run_in_executor(
                None,
                generate_prospectus_analysis_image,
                company_name,
                ipo_price,
                analysis,
                ipo_id,
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
