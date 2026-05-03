"""Banka mobil ekran görüntüsünden portföy çıkarma — Claude Vision.

Kullanıcı bankanın mobil uygulamasından portföy ekran görüntüsü gönderir,
AI hisse kodlarını + lot + ortalama maliyetleri tespit edip JSON döner.

Maksimum 3 hisse — kullanıcı isterse sonra elle ekleyebilir.
"""

import base64
import json
import logging
import re
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-sonnet-4-5-20250929"  # Vision destekli son model
_TIMEOUT = 45.0

_SYSTEM_PROMPT = """Sen bir finansal portföy ekran görüntüsü tarayıcısısın.
Bir Türk bankasının mobil uygulamasından gelen portföy/hisse listesi ekran görüntüsünü
analiz edip her hisse için yapılandırılmış JSON döneceksin.

KURALLAR:
1. SADECE BIST hisse kodlarını çıkar (3-6 büyük harf, ör: THYAO, TUPRS, AKBNK)
2. Her hisse için: ticker, lots (adet), avgCost (ortalama maliyet TL)
3. Maksimum 3 hisse döndür (en yüksek değerli olanlar)
4. Lot: tam sayı (sermaye artırımları sebebiyle ondalık olabilir, en yakın yuvarla)
5. avgCost: TL cinsinden (₺ işareti çıkar, virgülü noktaya çevir)
6. Belirsiz/okunamayan veri varsa null döndür
7. JSON dışında HİÇBİR ŞEY yazma. Açıklama yok, başlık yok.

JSON FORMATI:
{
  "stocks": [
    {"ticker": "THYAO", "lots": 100, "avgCost": 245.80},
    {"ticker": "TUPRS", "lots": 50, "avgCost": 320.00}
  ],
  "confidence": "high" | "medium" | "low",
  "notes": "Eğer belirsiz bir şey varsa kısa not"
}

HİÇBİR HİSSE TESPİT EDEMEDİYSEN: {"stocks": [], "confidence": "low", "notes": "..."}
"""


async def parse_portfolio_screenshot(image_base64: str, media_type: str = "image/jpeg") -> Optional[dict]:
    """Ekran görüntüsünden hisse listesi çıkar.

    Args:
        image_base64: Base64 encoded image (data URI öncesi olmadan).
        media_type: image/jpeg | image/png | image/webp

    Returns:
        dict | None — {"stocks": [...], "confidence": ..., "notes": ...}
    """
    api_key = settings.ANTHROPIC_API_KEY
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY yok — portföy screenshot parse atla")
        return None

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(
                _ANTHROPIC_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
                json={
                    "model": _CLAUDE_MODEL,
                    "max_tokens": 1500,
                    "system": _SYSTEM_PROMPT,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": media_type,
                                        "data": image_base64,
                                    },
                                },
                                {
                                    "type": "text",
                                    "text": "Bu ekran görüntüsündeki hisseleri çıkar. SADECE JSON döndür.",
                                },
                            ],
                        }
                    ],
                    "temperature": 0.1,
                },
            )
            if resp.status_code != 200:
                logger.warning("Vision API HTTP %d: %s", resp.status_code, resp.text[:200])
                return None
            data = resp.json()
            content = data.get("content", [])
            if not content:
                return None
            text = content[0].get("text", "").strip()

            # JSON içerik bul (markdown code block içinde olabilir)
            m = re.search(r"\{[\s\S]*\}", text)
            if not m:
                logger.warning("Vision response JSON yok: %s", text[:200])
                return None

            try:
                parsed = json.loads(m.group(0))
            except Exception as e:
                logger.warning("Vision JSON parse hata: %s\nText: %s", e, text[:300])
                return None

            # Validasyon
            stocks = parsed.get("stocks") or []
            valid_stocks = []
            for s in stocks[:3]:  # Max 3
                ticker = (s.get("ticker") or "").upper().strip()
                if not re.match(r"^[A-Z]{3,6}$", ticker):
                    continue
                lots = s.get("lots")
                cost = s.get("avgCost")
                try:
                    lots = int(round(float(lots))) if lots is not None else None
                    cost = float(cost) if cost is not None else None
                except Exception:
                    continue
                if not lots or lots <= 0 or not cost or cost <= 0:
                    continue
                valid_stocks.append({
                    "ticker": ticker,
                    "lots": lots,
                    "avgCost": round(cost, 2),
                })

            return {
                "stocks": valid_stocks,
                "confidence": parsed.get("confidence", "medium"),
                "notes": (parsed.get("notes") or "")[:300],
            }
    except Exception as e:
        logger.exception("Vision API hata: %s", e)
        return None
