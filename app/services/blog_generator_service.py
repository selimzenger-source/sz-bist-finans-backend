"""AI Blog Uretici Servisi — Finans egitim icerikleri.

Gemini API kullanarak Turkce finans blog yazilari uretir.
AdSense icin kaliteli, benzersiz ve SEO uyumlu icerik saglar.
"""

import json
import logging
import re
import unicodedata
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
_GEMINI_MODEL = "gemini-2.5-flash"
_ABACUS_URL = "https://routellm.abacus.ai/v1/chat/completions"
_ABACUS_MODEL = "claude-sonnet-4-6"
_TIMEOUT = 120

# Konu havuzu — rastgele secim icin
TOPIC_POOL = [
    {"topic": "Halka arz sürecinde yatırımcının dikkat etmesi gereken 10 kritik nokta", "category": "halka_arz"},
    {"topic": "KAP bildirimlerini doğru okumak: Yatırımcı için pratik rehber", "category": "kap"},
    {"topic": "Tavan ve taban yapan hisselerde dikkat edilmesi gereken sinyaller", "category": "tavan_taban"},
    {"topic": "VİOP gece seansı: Ertesi günün borsasını nasıl öngörür?", "category": "viop"},
    {"topic": "SPK haftalık bülteni nasıl okunur ve yatırımcıya ne söyler?", "category": "spk"},
    {"topic": "Borsa İstanbul'da temettü yatırımcılığı: Strateji ve ipuçları", "category": "temel_analiz"},
    {"topic": "Bilanço okuma rehberi: Şirketin mali sağlığını 5 dakikada anlayın", "category": "temel_analiz"},
    {"topic": "F/K oranı ve PD/DD oranı: Hisse değerlemesinde temel göstergeler", "category": "borsa_rehberi"},
    {"topic": "Bedelsiz sermaye artırımı nedir ve hisse fiyatını nasıl etkiler?", "category": "kap"},
    {"topic": "Borsa yatırımcısının psikolojisi: Duygusal kararlardan kaçınma rehberi", "category": "borsa_rehberi"},
    {"topic": "BIST 100 endeksine yeni giren hisseler: Ne anlama gelir?", "category": "borsa_rehberi"},
    {"topic": "Halka arzda lot hesaplama ve eşit dağıtım nasıl çalışır?", "category": "halka_arz"},
    {"topic": "Yapay zeka ile borsa haberi analizi: Geleneksel yöntemlerle karşılaştırma", "category": "teknoloji"},
    {"topic": "Özel durum açıklaması nedir? Yatırımcı için önemi ve örnekler", "category": "kap"},
    {"topic": "Borsa İstanbul seans saatleri ve emir tipleri rehberi", "category": "borsa_rehberi"},
    {"topic": "Halka arz sonrası tavan takibi: Kümülatif getiri hesaplama", "category": "halka_arz"},
    {"topic": "Katılım endeksine uygun yatırım: Faizsiz finans ilkeleri", "category": "borsa_rehberi"},
    {"topic": "Döviz kuru ve borsa ilişkisi: Yatırımcı için pratik bilgiler", "category": "borsa_rehberi"},
    {"topic": "Hisse senedi seçerken dikkat edilmesi gereken temel kriterler", "category": "temel_analiz"},
    {"topic": "SPK yaptırımları ve yatırımcı koruma mekanizmaları", "category": "spk"},
    {"topic": "Borsa İstanbul'da işlem hacmi analizi ve yatırımcı için anlamı", "category": "borsa_rehberi"},
    {"topic": "Kaldıraçlı işlem riskleri: VİOP'ta dikkat edilmesi gerekenler", "category": "viop"},
    {"topic": "Yeni başlayanlar için borsa sözlüğü: A'dan Z'ye terimler", "category": "borsa_rehberi"},
    {"topic": "Merkez Bankası faiz kararlarının borsaya etkisi", "category": "borsa_rehberi"},
    {"topic": "Enflasyon ve borsa: Yüksek enflasyon döneminde yatırım stratejileri", "category": "borsa_rehberi"},
]

CATEGORY_LABELS = {
    "halka_arz": "Halka Arz",
    "kap": "KAP Haberleri",
    "tavan_taban": "Tavan Taban",
    "viop": "VİOP",
    "spk": "SPK",
    "borsa_rehberi": "Borsa Rehberi",
    "teknoloji": "Teknoloji",
    "temel_analiz": "Temel Analiz",
}

_SYSTEM_PROMPT = """Sen Türkiye borsası (BIST) konusunda uzman bir finans eğitimcisisin. "Borsa Cebimde" platformu için blog yazıları yazıyorsun.

KURALLAR:
1. Minimum 800 kelime yaz, detaylı ve eğitici olsun.
2. Sadece HTML kullan: <h2>, <h3>, <p>, <strong>, <ul>, <li>, <ol> etiketleri.
3. <h1> KULLANMA — başlık ayrıca alınacak.
4. En az 3 alt başlık (<h2>) kullan.
5. Türkçe yaz, doğru Türkçe karakterler kullan (ş, ç, ğ, ı, ö, ü, İ).
6. Bilgilendirici ve eğitici ton kullan, promosyon yapma.
7. "Borsa Cebimde" platformunu doğal olarak 1-2 kez referans ver.
8. Her yazının sonunda "Yatırım Uyarısı" paragrafı ekle.
9. SEO uyumlu yaz — anahtar kelimeleri doğal şekilde kullan.
10. Gerçekçi ve doğru bilgiler ver, uydurma.
11. Örneklerle açıkla, soyut kalma.
12. Emoji KULLANMA.
13. ÖNEMLİ: HTML içinde SADECE BASIT etiketler kullan — attribute (class, id, style)
    EKLEME. Örnek: <p>metin</p> ✓  —  <p class="intro">metin</p> ✗
    Bu kural JSON parse hatasını önlemek için zorunludur.
14. JSON string değerleri içinde çift tırnak (") kullanma.

ÇIKTI FORMATI (kesinlikle JSON):
{
  "title": "SEO uyumlu, 6-10 kelimelik başlık",
  "content": "<h2>...</h2><p>...</p>... sadece HTML, attribute yok",
  "meta_description": "150-160 karakter SEO açıklaması"
}"""


def _slugify(text: str) -> str:
    """Turkce metni URL slug'a cevir."""
    # Türkçe karakterleri ASCII'ye çevir
    tr_map = str.maketrans("çğıöşüÇĞİÖŞÜ", "cgiosuCGIOSU")
    text = text.translate(tr_map)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text.lower())
    text = re.sub(r"[-\s]+", "-", text).strip("-")
    return text[:200]


async def generate_blog_post(
    topic: str | None = None,
    category: str | None = None,
    existing_titles: list[str] | None = None,
) -> dict | None:
    """AI ile blog yazisi uretir.

    Returns:
        {"title": str, "content": str, "meta_description": str, "slug": str, "category": str}
        veya None (hata durumunda)
    """
    settings = get_settings()

    # Konu belirleme
    if not topic:
        import random
        pick = random.choice(TOPIC_POOL)
        topic = pick["topic"]
        if not category:
            category = pick["category"]

    if not category:
        category = "borsa_rehberi"

    # Mevcut başlıkları tekrar önleme için ekle
    existing_info = ""
    if existing_titles:
        existing_info = f"\n\nDaha önce yazılmış başlıklar (TEKRARLAMA):\n" + "\n".join(f"- {t}" for t in existing_titles[:30])

    user_prompt = f"Konu: {topic}\nKategori: {CATEGORY_LABELS.get(category, category)}{existing_info}\n\nBu konu hakkında detaylı ve eğitici bir blog yazısı yaz. JSON formatında döndür."

    # Gemini API dene
    result = await _call_ai(settings, user_prompt)
    if not result:
        return None

    # JSON parse — Claude bazen ```json ... ``` sarmaliyla donuyor, temizle
    cleaned = result.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```\s*$", "", cleaned)

    title = ""
    content = ""
    meta_desc = ""

    # Once standart JSON parse dene
    try:
        json_match = re.search(r'\{[\s\S]*\}', cleaned)
        if json_match:
            data = json.loads(json_match.group())
            title = data.get("title", "").strip()
            content = data.get("content", "").strip()
            meta_desc = data.get("meta_description", "").strip()
    except json.JSONDecodeError as e:
        logger.warning(
            f"Blog generation JSON parse basarisiz (HTML icinde escape edilmemis tirnak "
            f"olabilir): {e}. Regex fallback deneniyor."
        )

    # JSON parse basarisiz veya eksikse regex ile tek tek cikar
    # (Claude HTML'de \" kacmiyorsa bu kurtaricidir)
    if not title or not content:
        # title: "..."
        t_match = re.search(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)"', cleaned)
        if t_match:
            title = t_match.group(1).replace('\\"', '"').replace("\\n", "\n").strip()

        # meta_description: "..."
        m_match = re.search(r'"meta_description"\s*:\s*"((?:[^"\\]|\\.)*)"', cleaned)
        if m_match:
            meta_desc = m_match.group(1).replace('\\"', '"').strip()

        # content: "..." — HTML icinde tirnak olabilir, daha dikkatli regex
        # "content": " ile basla, sonraki "meta_description" ya da } kadar al.
        c_match = re.search(
            r'"content"\s*:\s*"(.*?)"\s*,\s*"meta_description"',
            cleaned,
            re.DOTALL,
        )
        if not c_match:
            # meta_description onceden geliyorsa veya yoksa: content ... " } ile bitis
            c_match = re.search(
                r'"content"\s*:\s*"(.*?)"\s*\}',
                cleaned,
                re.DOTALL,
            )
        if c_match:
            content = c_match.group(1).replace('\\n', '\n').strip()

    if not title or not content:
        tail = result[-400:] if len(result) > 400 else result
        logger.error(
            f"Blog generation: Title/content cikarilamadi. "
            f"Response length: {len(result)}. Last 400 chars: {tail}"
        )
        return None

    slug = _slugify(title)
    logger.info(f"Blog uretildi: '{title[:60]}' (content {len(content)} chars)")

    return {
        "title": title,
        "content": content,
        "meta_description": meta_desc,
        "slug": slug,
        "category": category,
    }


async def _call_ai(settings, user_prompt: str) -> str | None:
    """AI API'yi cagir — Claude Haiku birincil, Gemini fallback."""

    # Claude Haiku (birincil — hızlı ve güvenilir)
    anthropic_key = getattr(settings, "ANTHROPIC_API_KEY", "")
    if anthropic_key:
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": anthropic_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 8000,
                        "system": _SYSTEM_PROMPT,
                        "messages": [
                            {"role": "user", "content": user_prompt},
                        ],
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    text = data["content"][0]["text"]
                    stop_reason = data.get("stop_reason", "?")
                    logger.info(
                        f"Blog generated via Claude Haiku ({len(text)} chars, "
                        f"stop_reason={stop_reason})"
                    )
                    if stop_reason == "max_tokens":
                        logger.warning(
                            "Claude response hit max_tokens — content may be truncated"
                        )
                    return text
                else:
                    logger.warning(f"Claude API error {resp.status_code}: {resp.text[:300]}")
        except Exception as e:
            logger.warning(f"Claude Haiku failed: {e}", exc_info=True)
    else:
        logger.warning("ANTHROPIC_API_KEY yok — Gemini fallback denenecek")

    # Gemini 2.5 Flash (fallback)
    gemini_key = getattr(settings, "GEMINI_API_KEY", "")
    if gemini_key:
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions",
                    headers={
                        "Authorization": f"Bearer {gemini_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "gemini-2.5-flash",
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt},
                        ],
                        "max_tokens": 8000,
                        "temperature": 0.4,
                    },
                )
                if resp.status_code == 200:
                    text = resp.json()["choices"][0]["message"]["content"]
                    logger.info(f"Blog generated via Gemini 2.5 Flash ({len(text)} chars)")
                    return text
                else:
                    logger.warning(f"Gemini API error {resp.status_code}: {resp.text[:300]}")
        except Exception as e:
            logger.warning(f"Gemini fallback failed: {e}", exc_info=True)
    else:
        logger.warning("GEMINI_API_KEY da yok — blog uretilemez")

    return None


# ────────────────────────────────────────────────────────────
# Resimden Blog Uretimi (Vision)
# ────────────────────────────────────────────────────────────

_SOURCE_SYSTEM_PROMPT = """Sen Türkiye borsası (BIST) konusunda uzman bir finans eğitimcisisin. "Borsa Cebimde" platformu için blog yazıları yazıyorsun.

Sana bir veya birden fazla resim/screenshot verilecek. Bu görsellerdeki bilgileri oku, analiz et ve bunlardan özgün bir Türkçe blog yazısı oluştur.

KURALLAR:
1. Görseldeki bilgileri KAYNAK olarak kullan ama tamamen yeniden yaz — kopyala-yapıştır YAPMA.
2. Minimum 800 kelime, detaylı ve eğitici olsun.
3. Sadece HTML kullan: <h2>, <h3>, <p>, <strong>, <ul>, <li>, <ol> etiketleri.
4. <h1> KULLANMA.
5. Türkçe yaz, doğru Türkçe karakterler kullan (ş, ç, ğ, ı, ö, ü, İ).
6. Bilgilendirici ve eğitici ton kullan.
7. "Borsa Cebimde" platformunu doğal olarak 1-2 kez referans ver.
8. Her yazının sonunda "Yatırım Uyarısı" paragrafı ekle.
9. SEO uyumlu yaz.
10. Emoji KULLANMA.
11. Görseldeki dolar fiyatlarını TL'ye, inch'leri cm'ye çevir (finans bağlamına uyarla).

ÇIKTI FORMATI (kesinlikle JSON):
{
  "title": "SEO uyumlu, 6-10 kelimelik başlık",
  "content": "<h2>...</h2><p>...</p>... sadece HTML",
  "meta_description": "150-160 karakter SEO açıklaması"
}"""


async def generate_blog_from_source(
    images_base64: list[str],
    additional_text: str | None = None,
    category: str | None = None,
    existing_titles: list[str] | None = None,
) -> dict | None:
    """Resim/screenshot'lardan blog yazisi uretir (Vision API).

    Args:
        images_base64: Base64 encoded resimler listesi
        additional_text: Opsiyonel ek metin/aciklama
        category: Blog kategorisi
        existing_titles: Mevcut basliklar (tekrar onleme)

    Returns:
        {"title": str, "content": str, "meta_description": str, "slug": str, "category": str}
    """
    settings = get_settings()
    if not category:
        category = "borsa_rehberi"

    # User message — resimler + ek metin
    user_content: list[dict] = []

    for img_b64 in images_base64:
        # Base64 formatini duzelt
        if not img_b64.startswith("data:"):
            img_b64 = f"data:image/jpeg;base64,{img_b64}"
        user_content.append({
            "type": "image_url",
            "image_url": {"url": img_b64}
        })

    text_parts = []
    if additional_text:
        text_parts.append(f"Ek bilgi: {additional_text}")
    if existing_titles:
        text_parts.append("Daha önce yazılmış başlıklar (TEKRARLAMA):\n" + "\n".join(f"- {t}" for t in existing_titles[:30]))
    text_parts.append("Bu görsellerdeki bilgileri kullanarak özgün bir Türkçe finans blog yazısı oluştur. JSON formatında döndür.")

    user_content.append({"type": "text", "text": "\n\n".join(text_parts)})

    # Gemini Vision API
    gemini_key = getattr(settings, "GEMINI_API_KEY", "")
    if gemini_key:
        try:
            async with httpx.AsyncClient(timeout=180) as client:
                resp = await client.post(
                    _GEMINI_URL,
                    headers={"Authorization": f"Bearer {gemini_key}", "Content-Type": "application/json"},
                    json={
                        "model": _GEMINI_MODEL,
                        "messages": [
                            {"role": "system", "content": _SOURCE_SYSTEM_PROMPT},
                            {"role": "user", "content": user_content},
                        ],
                        "max_tokens": 4000,
                        "temperature": 0.7,
                    },
                )
                if resp.status_code == 200:
                    result_text = resp.json()["choices"][0]["message"]["content"]
                    logger.info(f"Blog from source generated via Gemini ({len(result_text)} chars)")
                else:
                    logger.warning(f"Gemini Vision error {resp.status_code}: {resp.text[:200]}")
                    return None
        except Exception as e:
            logger.error(f"Gemini Vision failed: {e}")
            return None
    else:
        logger.error("No Gemini API key for Vision blog generation")
        return None

    # JSON parse
    try:
        json_match = re.search(r'\{[\s\S]*\}', result_text)
        if not json_match:
            logger.error("Blog from source: JSON not found")
            return None

        data = json.loads(json_match.group())
        title = data.get("title", "").strip()
        content = data.get("content", "").strip()
        meta_desc = data.get("meta_description", "").strip()

        if not title or not content:
            logger.error("Blog from source: Empty title or content")
            return None

        return {
            "title": title,
            "content": content,
            "meta_description": meta_desc,
            "slug": _slugify(title),
            "category": category,
        }

    except json.JSONDecodeError as e:
        logger.error(f"Blog from source JSON parse error: {e}")
        return None
