# -*- coding: utf-8 -*-
"""
Fintables Özet Finansal Rapor kartı (görsel) → finansal kalem çıkarımı.
Claude Vision ile. Admin panelde "Doğrula" için: kart vs DB karşılaştırması.

SADECE finansal tablo kalemleri çıkarılır (varlık, özkaynak, satış, FAVÖK,
net kâr). Fiyat / F/K / PD/DD / FD/FAVÖK gibi PİYASA verileri İSTENMEZ.
"""
import json
import logging
import re
from typing import Optional

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
_CLAUDE_MODEL = "claude-sonnet-4-5-20250929"  # Vision destekli
_TIMEOUT = 60

_SYSTEM_PROMPT = """Sen bir finansal tablo OCR uzmanısın. Sana "Fintables Özet Finansal Rapor"
kartının görseli verilecek. Karttan SADECE şu finansal tablo kalemlerini çıkar ve JSON döndür.

ÖNEMLİ KURALLAR:
- SADECE "Özet Gelir Tablosu" ve "Özet Bilanço" bölümlerindeki GÜNCEL DÖNEM (soldaki/ilk sütun) değerlerini al.
- Fiyat, Piyasa Değeri, F/K, PD/DD, FD/FAVÖK gibi PİYASA verilerini ALMA (bunlar bize lazım değil).
- Sayılar tam TL olarak verilir (nokta binlik ayraç). "5.256.413.912" → 5256413912 (integer).
- Negatif değerler "-" ile gelir → negatif integer.
- Bir kalem yoksa null yaz.

Çıkarılacak alanlar (JSON):
{
  "ticker": "BORSK",              // başlıktaki hisse kodu
  "period": "2025/12",            // başlıktaki dönem (YYYY/AY formatında)
  "revenue": <Satışlar (Hasılat) güncel dönem>,
  "gross_profit": <Brüt Kar>,
  "ebitda": <FAVÖK>,
  "net_income": <Net Dönem Karı>,
  "total_assets": <Toplam Varlıklar>,
  "total_equity": <Özkaynaklar>,
  "net_debt": <Net Borç>,
  "confidence": "high" | "medium" | "low"
}
SADECE JSON döndür, başka metin yazma."""


def _period_to_db(period_str: str) -> Optional[str]:
    """'2025/12' → '2025-Q4', '2026/3' → '2026-Q1'."""
    if not period_str:
        return None
    m = re.match(r"^(\d{4})\s*[/\-]\s*(\d{1,2})$", str(period_str).strip())
    if not m:
        return None
    year, mm = int(m.group(1)), int(m.group(2))
    if mm not in (3, 6, 9, 12):
        return None
    return f"{year}-Q{mm // 3}"


async def parse_fintables_card(image_base64: str, media_type: str = "image/jpeg") -> Optional[dict]:
    """Fintables kartı görselinden finansal kalemleri çıkar.

    Returns: {ticker, period(db format), revenue, gross_profit, ebitda,
              net_income, total_assets, total_equity, net_debt, confidence} | None
    """
    api_key = settings.ANTHROPIC_API_KEY
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY yok — Fintables kart parse atla")
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
                    "max_tokens": 1000,
                    "system": _SYSTEM_PROMPT,
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_base64}},
                            {"type": "text", "text": "Bu Fintables kartından finansal kalemleri çıkar. SADECE JSON."},
                        ],
                    }],
                    "temperature": 0.0,
                },
            )
            if resp.status_code != 200:
                logger.warning("Fintables vision HTTP %d: %s", resp.status_code, resp.text[:200])
                return None
            content = resp.json().get("content", [])
            if not content:
                return None
            text = content[0].get("text", "").strip()
            m = re.search(r"\{[\s\S]*\}", text)
            if not m:
                return None
            parsed = json.loads(m.group(0))
            # period → DB format
            parsed["period_db"] = _period_to_db(parsed.get("period", ""))
            tk = (parsed.get("ticker") or "").upper().strip()
            parsed["ticker"] = tk if re.match(r"^[A-Z]{3,6}$", tk) else None
            return parsed
    except Exception as e:
        logger.exception("Fintables vision hata: %s", e)
        return None
