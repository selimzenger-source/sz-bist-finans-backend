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

ÇIKTI FORMATI (kesinlikle JSON):
{
  "title": "SEO uyumlu, 6-10 kelimelik başlık",
  "content": "<h2>...</h2><p>...</p>... sadece HTML",
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

    # JSON parse
    try:
        # JSON bloğunu bul
        json_match = re.search(r'\{[\s\S]*\}', result)
        if not json_match:
            logger.error("Blog generation: JSON not found in response")
            return None

        data = json.loads(json_match.group())
        title = data.get("title", "").strip()
        content = data.get("content", "").strip()
        meta_desc = data.get("meta_description", "").strip()

        if not title or not content:
            logger.error("Blog generation: Empty title or content")
            return None

        slug = _slugify(title)

        return {
            "title": title,
            "content": content,
            "meta_description": meta_desc,
            "slug": slug,
            "category": category,
        }

    except json.JSONDecodeError as e:
        logger.error(f"Blog generation JSON parse error: {e}")
        return None


async def _call_ai(settings, user_prompt: str) -> str | None:
    """AI API'yi cagir — Gemini birincil, Abacus yedek."""

    # 1. Gemini
    gemini_key = getattr(settings, "GEMINI_API_KEY", "")
    if gemini_key:
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    _GEMINI_URL,
                    headers={"Authorization": f"Bearer {gemini_key}", "Content-Type": "application/json"},
                    json={
                        "model": _GEMINI_MODEL,
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt},
                        ],
                        "max_tokens": 4000,
                        "temperature": 0.8,
                    },
                )
                if resp.status_code == 200:
                    text = resp.json()["choices"][0]["message"]["content"]
                    logger.info(f"Blog generated via Gemini ({len(text)} chars)")
                    return text
                else:
                    logger.warning(f"Gemini error {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.warning(f"Gemini failed: {e}")

    # 2. Abacus fallback
    abacus_key = getattr(settings, "OPENAI_API_KEY", "")
    if abacus_key:
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
                resp = await client.post(
                    _ABACUS_URL,
                    headers={"Authorization": f"Bearer {abacus_key}", "Content-Type": "application/json"},
                    json={
                        "model": _ABACUS_MODEL,
                        "messages": [
                            {"role": "system", "content": _SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt},
                        ],
                        "max_tokens": 4000,
                        "temperature": 0.8,
                    },
                )
                if resp.status_code == 200:
                    text = resp.json()["choices"][0]["message"]["content"]
                    logger.info(f"Blog generated via Abacus ({len(text)} chars)")
                    return text
                else:
                    logger.warning(f"Abacus error {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.warning(f"Abacus failed: {e}")

    return None
