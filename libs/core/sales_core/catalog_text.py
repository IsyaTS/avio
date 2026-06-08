from __future__ import annotations

import re
from typing import Any, Dict, List

from .text_norm import normalize_text


_WORD_TOKEN_RE = re.compile(r"[0-9a-zа-яё]+", re.IGNORECASE)


def collect_item_text(item: Dict[str, Any]) -> str:
    cached = item.get("_search_text")
    if isinstance(cached, str) and cached:
        return cached
    parts: List[str] = []
    known_keys = {
        "title",
        "name",
        "sku",
        "id",
        "brand",
        "collection",
        "category",
        "series",
        "model",
        "color",
        "material",
        "decor",
        "finish",
        "tags",
        "description",
        "notes",
        "features",
    }
    for key in (
        "title",
        "name",
        "sku",
        "id",
        "brand",
        "collection",
        "category",
        "series",
        "model",
        "color",
        "material",
        "decor",
        "finish",
        "tags",
        "description",
        "notes",
        "features",
    ):
        if key not in item:
            continue
        value = item.get(key)
        if isinstance(value, (list, tuple, set)):
            parts.extend(str(v) for v in value if v)
        elif value:
            parts.append(str(value))
    for key, value in item.items():
        if key in known_keys or str(key).startswith("_"):
            continue
        if isinstance(value, (list, tuple, set)):
            for val in value:
                text = str(val or "").strip()
                if text:
                    parts.append(f"{key} {text}")
        else:
            text = str(value or "").strip()
            if text:
                parts.append(f"{key} {text}")
    return " ".join(parts)


def tokenize_query(text: str | None) -> List[str]:
    if not text:
        return []
    cleaned = normalize_text(text)
    tokens: List[str] = []
    for raw in _WORD_TOKEN_RE.findall(cleaned):
        token = raw.strip()
        if not token:
            continue
        if token.isdigit():
            tokens.append(token)
            continue
        if len(token) >= 3:
            tokens.append(token)
    return tokens[:12]


def text_match_score(item: Dict[str, Any], tokens: List[str]) -> float:
    if not tokens:
        return 0.0
    haystack = normalize_text(collect_item_text(item))
    if not haystack:
        return 0.0
    hay_tokens = set(_WORD_TOKEN_RE.findall(haystack))
    score = 0.0
    for token in tokens:
        if not token:
            continue
        if token in hay_tokens:
            score += 2.5
            continue
        if token.isdigit() and token in haystack:
            score += 1.5
            continue
        if len(token) >= 4:
            prefix = token[:4]
            if prefix in haystack:
                score += 0.75
                continue
    return score
