from __future__ import annotations

import re
from typing import Any, Awaitable, Callable, Mapping, Sequence

from libs.core.repo import tenant_asset_rules
from libs.core.services.asset_ai_metadata_compiler import AssetCompileInput, compile_asset_metadata

AsyncJsonFn = Callable[[str], Awaitable[Mapping[str, Any]]]

UFA_PHOTO_CITY_GROUP = {
    "уфа",
    "туймазы",
    "стерлитамак",
    "салават",
    "нефтекамск",
    "кумертау",
    "ишимбай",
    "стерлибашево",
    "белебей",
    "бирск",
    "мишкино",
    "дюртюли",
    "кушнаренково",
    "серафимовский",
    "благовещенск",
    "красноусольский",
    "толбазы",
    "киргиз-мияки",
    "чишмы",
    "языково",
    "буздяк",
    "кандры",
    "аша",
    "иглино",
    "павловка",
    "красный ключ",
    "бавлы",
    "уруссу",
    "бугульма",
    "альметьевск",
    "оренбург",
}


async def compile_persona_asset_instruction(
    *,
    tenant_id: int,
    asset_id: str,
    asset_type: str,
    instruction: str,
    title: str,
    json_reviewer_fn: AsyncJsonFn | None = None,
) -> dict[str, Any]:
    result = await compile_asset_metadata(
        AssetCompileInput(
            tenant_id=int(tenant_id),
            asset_id=str(asset_id),
            asset_type=str(asset_type),
            title=str(title or instruction),
            description=str(instruction or ""),
        ),
        json_reviewer_fn=json_reviewer_fn,
    )
    metadata = dict(result.metadata)
    metadata["source"] = "persona"
    metadata["needs_review"] = True if metadata.get("confidence", 0) < 0.9 else bool(metadata.get("needs_review"))
    return metadata


def build_persona_asset_rules(
    *,
    tenant_id: int,
    persona_text: str,
    assets: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if not _has_supported_photo_routing_instruction(persona_text):
        return []
    rules: list[dict[str, Any]] = []
    for asset in assets:
        if str(asset.get("status") or "").lower() != "active":
            continue
        if str(asset.get("asset_type") or "").lower() not in {"photo", "image"}:
            continue
        title = str(asset.get("title") or asset.get("original_filename") or "").strip()
        asset_id = str(asset.get("asset_id") or "").strip()
        if not title or not asset_id:
            continue
        brand = _asset_brand(title)
        region = _asset_region(title)
        if not brand or not region:
            continue
        conditions = _routing_conditions(brand=brand, region=region)
        rule_id = tenant_asset_rules.stable_rule_id(
            int(tenant_id),
            asset_id,
            "persona",
            conditions,
        )
        rules.append(
            {
                "tenant_id": int(tenant_id),
                "rule_id": rule_id,
                "asset_id": asset_id,
                "source": "persona",
                "status": "active",
                "priority": 100,
                "trigger": {
                    "asset_intent": "catalog_photo_routing",
                    "domain": "doors",
                    "human_summary": "Отправлять фото по Avito-аккаунту и городу объявления.",
                },
                "conditions": conditions,
                "action": {
                    "type": "send_asset",
                    "asset_id": asset_id,
                    "asset_type": str(asset.get("asset_type") or "photo"),
                    "caption_hint": title,
                },
                "guards": {
                    "requires_known_slots": ["account_brand", "city"],
                    "allowed_channels": ["avito"],
                    "once_per_dialog": True,
                },
                "confidence": 0.91,
                "needs_review": False,
                "compiler_version": "persona_asset_rule_compiler_v1",
            }
        )
    return rules


def _has_supported_photo_routing_instruction(persona_text: str) -> bool:
    text = _normalize(str(persona_text or ""))
    return all(
        marker in text
        for marker in (
            "отправляй фото",
            "гермес",
            "айдар",
            "город",
            "объявлен",
        )
    ) and ("казан" in text and "уфа" in text)


def _asset_brand(title: str) -> str:
    text = _normalize(title)
    if "гермес" in text:
        return "germes"
    if "айдар" in text:
        return "aidar"
    return ""


def _asset_region(title: str) -> str:
    text = _normalize(title)
    if "уфа" in text:
        return "ufa_group"
    if "казан" in text:
        return "kazan_other"
    return ""


def _routing_conditions(*, brand: str, region: str) -> dict[str, Any]:
    city_values = sorted(UFA_PHOTO_CITY_GROUP)
    city_condition: dict[str, Any]
    if region == "ufa_group":
        city_condition = {"slot": "city", "operator": "in", "value": city_values}
    else:
        city_condition = {"slot": "city", "operator": "not_in", "value": city_values}
    return {
        "all": [
            {"slot": "account_brand", "operator": "equals", "value": brand},
            city_condition,
        ]
    }


def _normalize(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower().replace("ё", "е"))
