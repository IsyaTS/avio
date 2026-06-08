from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping


AsyncJsonFn = Callable[[str], Awaitable[Mapping[str, Any]]]

COMPILER_VERSION = "asset_ai_metadata_compiler_v1"

_CITY_PATTERNS: tuple[tuple[str, str], ...] = (
    ("казан", "Казань"),
    ("уф", "Уфа"),
    ("стерлитамак", "Стерлитамак"),
    ("салават", "Салават"),
    ("ишимба", "Ишимбай"),
    ("оренбург", "Оренбург"),
    ("москв", "Москва"),
    ("санкт-петербург", "Санкт-Петербург"),
    ("питер", "Санкт-Петербург"),
)


@dataclass(frozen=True)
class AssetCompileInput:
    tenant_id: int
    asset_id: str
    asset_type: str
    title: str
    description: str = ""
    filename: str = ""
    mime: str = ""
    allowed_channels: tuple[str, ...] = ()


@dataclass(frozen=True)
class AssetCompileResult:
    metadata: dict[str, Any]
    cache_key: str
    used_ai: bool


def asset_compile_cache_key(data: AssetCompileInput, checksum: str = "") -> str:
    payload = {
        "tenant_id": int(data.tenant_id),
        "asset_id": data.asset_id,
        "asset_type": data.asset_type,
        "title": data.title,
        "description": data.description,
        "filename": data.filename,
        "mime": data.mime,
        "channels": list(data.allowed_channels),
        "checksum": checksum,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


async def compile_asset_metadata(
    data: AssetCompileInput,
    *,
    json_reviewer_fn: AsyncJsonFn | None = None,
    checksum: str = "",
) -> AssetCompileResult:
    cache_key = asset_compile_cache_key(data, checksum=checksum)
    if json_reviewer_fn is not None and _ai_enabled():
        try:
            prompt = _build_prompt(data)
            parsed = dict(await json_reviewer_fn(prompt) or {})
            normalized = normalize_compiler_output(parsed, data)
            normalized["compiler_version"] = COMPILER_VERSION
            return AssetCompileResult(metadata=normalized, cache_key=cache_key, used_ai=True)
        except Exception:
            pass
    metadata = _rule_fallback_metadata(data)
    metadata["compiler_version"] = COMPILER_VERSION
    return AssetCompileResult(metadata=metadata, cache_key=cache_key, used_ai=False)


def normalize_compiler_output(payload: Mapping[str, Any], data: AssetCompileInput) -> dict[str, Any]:
    conditions = payload.get("conditions") if isinstance(payload.get("conditions"), Mapping) else {}
    action = payload.get("action") if isinstance(payload.get("action"), Mapping) else {}
    guards = payload.get("guards") if isinstance(payload.get("guards"), Mapping) else {}
    confidence = _bounded_float(payload.get("confidence"), default=0.0)
    needs_review = bool(payload.get("needs_review")) or confidence < 0.75
    if not action.get("type"):
        action = {
            "type": "send_asset",
            "asset_type": data.asset_type,
            "asset_id": data.asset_id,
            "caption_hint": str(payload.get("caption_hint") or data.title).strip(),
        }
    else:
        action = dict(action)
        action.setdefault("asset_id", data.asset_id)
        action.setdefault("asset_type", data.asset_type)
    return {
        "asset_intent": str(payload.get("asset_intent") or "asset_send").strip(),
        "domain": str(payload.get("domain") or "generic_sales").strip(),
        "conditions": dict(conditions),
        "action": action,
        "guards": dict(guards),
        "confidence": confidence,
        "needs_review": needs_review,
        "human_summary": str(payload.get("human_summary") or "").strip(),
    }


def _rule_fallback_metadata(data: AssetCompileInput) -> dict[str, Any]:
    text = f"{data.title}\n{data.description}".strip()
    city = _extract_city(text)
    product = _extract_product(text)
    condition_items: list[dict[str, Any]] = []
    required: list[str] = []
    if city:
        condition_items.append({"slot": "city", "operator": "equals", "value": city})
        required.append("city")
    if product:
        condition_items.append({"slot": "product", "operator": "contains", "value": product})
        required.append("product")
    conditions = {"all": condition_items} if condition_items else {}
    confidence = 0.82 if condition_items else 0.45
    needs_review = confidence < 0.75
    channels = [str(item).strip().lower() for item in data.allowed_channels if str(item).strip()]
    return {
        "asset_intent": _guess_intent(text, data.asset_type),
        "domain": _guess_domain(text),
        "conditions": conditions,
        "action": {
            "type": "send_asset",
            "asset_type": data.asset_type,
            "asset_id": data.asset_id,
            "caption_hint": data.title,
        },
        "guards": {
            "requires_known_slots": required,
            "once_per_dialog": True,
            "allowed_channels": channels,
        },
        "confidence": confidence,
        "needs_review": needs_review,
        "human_summary": _human_summary(city=city, product=product, title=data.title),
        "fallback": True,
    }


def _build_prompt(data: AssetCompileInput) -> str:
    safe = {
        "asset_type": data.asset_type,
        "title": data.title,
        "description": data.description,
        "filename": data.filename,
        "mime": data.mime,
        "allowed_channels": list(data.allowed_channels),
    }
    return (
        "Compile this tenant asset into safe JSON action metadata. "
        "Return only JSON with asset_intent, domain, conditions, action, guards, "
        "confidence, needs_review, human_summary. Do not invent unknown facts.\n"
        + json.dumps(safe, ensure_ascii=False)
    )


def _ai_enabled() -> bool:
    raw = os.getenv("ASSET_RULES_AI_ENABLED", "1")
    return str(raw).strip().lower() not in {"0", "false", "off", "no"}


def _bounded_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    return max(0.0, min(1.0, parsed))


def _extract_city(text: str) -> str:
    low = str(text or "").lower().replace("ё", "е")
    for needle, city in _CITY_PATTERNS:
        if needle in low:
            return city
    return ""


def _extract_product(text: str) -> str:
    low = str(text or "").lower().replace("ё", "е")
    no_mirror = bool(re.search(r"\b(?:без|без\s+.*)\s+зеркал", low))
    if no_mirror and "двер" in low:
        if "квартир" in low:
            return "квартирная дверь без зеркала"
        return "дверь без зеркала"
    if "зеркал" in low and "двер" in low:
        if "квартир" in low:
            return "квартирная дверь с зеркалом"
        return "дверь с зеркалом"
    if "квартир" in low and "двер" in low:
        return "квартирная дверь"
    if "двер" in low:
        return "дверь"
    if "покос" in low or "трав" in low:
        return "покос травы"
    if "клининг" in low or "уборк" in low:
        return "клининг"
    match = re.search(r"(?:для|по)\s+([а-яa-z0-9 \-]{3,40})", low)
    return match.group(1).strip() if match else ""


def _guess_intent(text: str, asset_type: str) -> str:
    low = str(text or "").lower()
    if "каталог" in low:
        return "product_catalog"
    if asset_type in {"photo", "image"}:
        return "product_photo"
    if asset_type in {"pdf", "catalog"}:
        return "product_catalog"
    return "asset_send"


def _guess_domain(text: str) -> str:
    low = str(text or "").lower().replace("ё", "е")
    if "двер" in low:
        return "doors"
    if "трав" in low or "покос" in low:
        return "lawn_mowing"
    if "клининг" in low or "уборк" in low:
        return "cleaning"
    return "generic_sales"


def _human_summary(*, city: str, product: str, title: str) -> str:
    parts = []
    if product:
        parts.append(f"клиент спрашивает про {product}")
    if city:
        parts.append(f"город {city}")
    if not parts:
        return f"Нужно проверить условия для: {title}"
    return "Отправлять, когда " + " и ".join(parts) + "."
