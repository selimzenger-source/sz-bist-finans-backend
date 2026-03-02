"""Ortak AI JSON Parse Helper.

Tum AI servislerinde (Gemini, Claude, Abacus) kullanilan
guvenli JSON parse fonksiyonu.

Gemini ozellikle su sorunlari yapar:
  - Markdown code fence ile sarar (```json ... ```)
  - Aciklama metni ekler
  - max_tokens dusukse JSON'u keser (truncated)
  - Bazen ekstra metin yazar

Bu modul 4 asamali fallback ile bozuk JSON'u kurtarir.
"""

import json
import logging
import re

logger = logging.getLogger(__name__)


def safe_parse_json(text: str, required_key: str | None = None) -> dict | None:
    """AI ciktisindaki JSON'u guvenli sekilde parse et.

    Args:
        text: AI'dan gelen ham metin
        required_key: Zorunlu anahtar (orn: "sentiment", "is_safe", "score")
                      Varsa, parse edilen dict'te bu key yoksa None doner.

    Returns:
        dict veya None (parse basarisiz)

    4 asamali fallback:
    1. Direkt json.loads
    2. Markdown code fence temizle (```json ... ```)
    3. Regex ile required_key iceren {} blogu bul
    4. Genis regex + brace depth tracking
    """
    if not text:
        return None

    cleaned = text.strip()

    # 1) Direkt parse
    result = _try_parse(cleaned)
    if result is not None:
        if required_key and required_key not in result:
            pass  # devam et, belki daha iyi match var
        else:
            return result

    # 2) Markdown code fence temizle
    if "```" in cleaned:
        fence_cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        fence_cleaned = re.sub(r"\s*```\s*$", "", fence_cleaned)
        result = _try_parse(fence_cleaned.strip())
        if result is not None:
            if not required_key or required_key in result:
                return result

    # 3) Regex — required_key iceren JSON blogu bul
    if required_key:
        pattern = r'\{[^{}]*"' + re.escape(required_key) + r'"[^{}]*\}'
        json_match = re.search(pattern, text, re.DOTALL)
        if json_match:
            result = _try_parse(json_match.group(0))
            if result is not None:
                return result

    # 4) Genis regex — herhangi bir JSON objesi bul (nested dahil)
    search_key = required_key or "{"
    if required_key:
        json_match2 = re.search(r'\{.*"' + re.escape(required_key) + r'".*\}', text, re.DOTALL)
    else:
        json_match2 = re.search(r'\{.*\}', text, re.DOTALL)

    if json_match2:
        candidate = json_match2.group(0)
        # Brace depth ile dogru kapanisi bul
        depth = 0
        end_idx = 0
        for i, ch in enumerate(candidate):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    end_idx = i + 1
                    break
        if end_idx > 0:
            result = _try_parse(candidate[:end_idx])
            if result is not None:
                if not required_key or required_key in result:
                    return result

    return None


def _try_parse(text: str) -> dict | None:
    """json.loads wrapper — basarisizda None doner."""
    try:
        result = json.loads(text)
        if isinstance(result, dict):
            return result
        return None
    except (json.JSONDecodeError, ValueError, TypeError):
        return None
