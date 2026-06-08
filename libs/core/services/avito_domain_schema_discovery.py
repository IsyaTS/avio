from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from libs.core.services.avito_dialog_filter import AvitoDialogMessage


SCHEMA_VERSION = "avito_domain_schema_v1"
RULES_DRAFT_VERSION = "avito_business_rules_draft_v1"
PROMPT_VERSION = "avito_domain_schema_discovery_v1"
_SAFE_KEY_RE = re.compile(r"[^a-z0-9_]+")
_RAW_DIALOG_MARKERS = ("Клиент:", "Менеджер:", "client:", "manager:")


@dataclass(frozen=True)
class AvitoDomainDiscoveryConfig:
    enabled: bool = True
    model: str = "gpt-5.2"
    timeout_seconds: float = 45.0
    sample_dialogs: int = 80
    sample_messages_per_dialog: int = 8


@dataclass(frozen=True)
class AvitoDomainDiscoveryResult:
    domain_schema: dict[str, Any]
    business_rules_draft: dict[str, Any]
    stats: dict[str, int] = field(default_factory=dict)
    ai_extracted: int = 0
    ai_failed: int = 0
    mode: str = "rule_fallback"


class AvitoDomainSchemaDiscoverer:
    def __init__(self, *, client: Any, config: AvitoDomainDiscoveryConfig) -> None:
        self._client = client
        self._config = config
        self._cache: dict[str, AvitoDomainDiscoveryResult] = {}

    async def discover(
        self,
        dialogs: Sequence[Sequence[AvitoDialogMessage]],
        *,
        tenant_id: int,
        created_at: datetime | None = None,
    ) -> AvitoDomainDiscoveryResult:
        if not self._config.enabled:
            raise RuntimeError("domain_discovery_disabled")
        sample = _build_sample(
            dialogs,
            sample_dialogs=self._config.sample_dialogs,
            sample_messages_per_dialog=self._config.sample_messages_per_dialog,
        )
        sample_hash = _stable_hash(sample)
        if sample_hash in self._cache:
            return self._cache[sample_hash]
        payload = {
            "prompt_version": PROMPT_VERSION,
            "tenant_id": int(tenant_id),
            "sample": sample,
        }
        raw = await self._call_ai(payload)
        normalized = _normalize_discovery_payload(
            raw,
            tenant_id=int(tenant_id),
            source="avito",
            created_at=created_at,
            fallback_sample=sample,
        )
        result = AvitoDomainDiscoveryResult(
            domain_schema=normalized["domain_schema"],
            business_rules_draft=normalized["business_rules_draft"],
            stats={"sample_dialogs": len(sample), "dialogs_seen": len(dialogs)},
            ai_extracted=1,
            ai_failed=0,
            mode="ai",
        )
        self._cache[sample_hash] = result
        return result

    async def _call_ai(self, payload: dict[str, Any]) -> dict[str, Any]:
        if _use_responses_api(self._config.model):
            result = self._client.responses.create(
                model=self._config.model,
                input=[
                    {"role": "system", "content": _system_prompt()},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                text={"format": {"type": "json_object"}},
                timeout=self._config.timeout_seconds,
            )
            if hasattr(result, "__await__"):
                result = await result
            return json.loads(getattr(result, "output_text", "") or "{}")
        result = self._client.chat.completions.create(
            model=self._config.model,
            messages=[
                {"role": "system", "content": _system_prompt()},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0,
            response_format={"type": "json_object"},
            timeout=self._config.timeout_seconds,
        )
        if hasattr(result, "__await__"):
            result = await result
        return json.loads(result.choices[0].message.content)


async def discover_domain_schema(
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    *,
    tenant_id: int,
    discoverer: AvitoDomainSchemaDiscoverer | None = None,
    enabled: bool = True,
    created_at: datetime | None = None,
) -> AvitoDomainDiscoveryResult:
    if not enabled:
        return _fallback_result(dialogs, tenant_id=tenant_id, created_at=created_at, mode="disabled")
    client = discoverer or build_default_discoverer()
    if client is None:
        return _fallback_result(dialogs, tenant_id=tenant_id, created_at=created_at, mode="ai_disabled")
    try:
        return await client.discover(dialogs, tenant_id=int(tenant_id), created_at=created_at)
    except Exception:
        logging.getLogger(__name__).warning("avito_domain_schema_discovery_failed", exc_info=True)
        return _fallback_result(dialogs, tenant_id=tenant_id, created_at=created_at, mode="ai_failed_fallback")


def build_default_discoverer() -> AvitoDomainSchemaDiscoverer | None:
    _silence_ai_transport_logs()
    enabled = str(os.getenv("AVITO_CONTEXTUAL_AI_REVIEW") or os.getenv("AVITO_TRAINING_AI_REVIEW") or "").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return None
    if not os.getenv("OPENAI_API_KEY"):
        return None
    try:
        from openai import AsyncOpenAI  # type: ignore
    except Exception:
        return None
    model = os.getenv("AVITO_CONTEXTUAL_AI_MODEL") or os.getenv("AVITO_TRAINING_AI_MODEL") or "gpt-5.2"
    return AvitoDomainSchemaDiscoverer(
        client=AsyncOpenAI(),
        config=AvitoDomainDiscoveryConfig(enabled=True, model=model),
    )


def generic_domain_schema(*, tenant_id: int, created_at: datetime | None = None) -> dict[str, Any]:
    created = _iso_utc(created_at)
    base = {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": int(tenant_id),
        "source": "avito",
        "domain": "generic_sales",
        "domain_label": "продажи",
        "primary_offer": "товары или услуги",
        "required_slots": [],
        "optional_slots": [],
        "slot_definitions": {},
        "price_depends_on": [],
        "location_depends_on": [],
        "service_depends_on": [],
        "availability_depends_on": [],
        "common_intents": ["price_question", "availability", "clarification", "other"],
        "confidence": 0.0,
        "created_at": created,
    }
    base["domain_schema_id"] = _schema_id(base)
    return base


def generic_business_rules_draft(
    domain_schema: Mapping[str, Any],
    *,
    tenant_id: int,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": RULES_DRAFT_VERSION,
        "tenant_id": int(tenant_id),
        "domain_schema_id": str(domain_schema.get("domain_schema_id") or ""),
        "domain": str(domain_schema.get("domain") or "generic_sales"),
        "rules": [],
        "needs_human_confirmation": True,
        "created_at": _iso_utc(created_at),
    }


def _fallback_result(
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    *,
    tenant_id: int,
    created_at: datetime | None,
    mode: str,
) -> AvitoDomainDiscoveryResult:
    schema = generic_domain_schema(tenant_id=int(tenant_id), created_at=created_at)
    draft = generic_business_rules_draft(schema, tenant_id=int(tenant_id), created_at=created_at)
    return AvitoDomainDiscoveryResult(
        domain_schema=schema,
        business_rules_draft=draft,
        stats={"sample_dialogs": min(len(dialogs), 80), "dialogs_seen": len(dialogs)},
        ai_extracted=0,
        ai_failed=1 if mode == "ai_failed_fallback" else 0,
        mode=mode,
    )


def _normalize_discovery_payload(
    payload: Mapping[str, Any],
    *,
    tenant_id: int,
    source: str,
    created_at: datetime | None,
    fallback_sample: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    raw_schema = payload.get("domain_schema") if isinstance(payload.get("domain_schema"), Mapping) else payload
    schema = _normalize_schema(raw_schema, tenant_id=tenant_id, source=source, created_at=created_at)
    raw_draft = payload.get("business_rules_draft") if isinstance(payload.get("business_rules_draft"), Mapping) else {}
    draft = _normalize_rules_draft(raw_draft, schema, tenant_id=tenant_id, created_at=created_at)
    if _contains_raw_dialog_text(schema) or _contains_raw_dialog_text(draft):
        draft = generic_business_rules_draft(schema, tenant_id=tenant_id, created_at=created_at)
    return {"domain_schema": schema, "business_rules_draft": draft}


def _normalize_schema(
    raw: Mapping[str, Any],
    *,
    tenant_id: int,
    source: str,
    created_at: datetime | None,
) -> dict[str, Any]:
    created = _iso_utc(created_at)
    domain = _safe_key(str(raw.get("domain") or "generic_sales")) or "generic_sales"
    label = _short_text(raw.get("domain_label") or raw.get("primary_offer") or "продажи")
    slot_definitions = _string_map(raw.get("slot_definitions"))
    required = _slot_list(raw.get("required_slots"), slot_definitions)
    optional = _slot_list(raw.get("optional_slots"), slot_definitions)
    schema = {
        "schema_version": SCHEMA_VERSION,
        "tenant_id": int(tenant_id),
        "source": source,
        "domain": domain,
        "domain_label": label,
        "primary_offer": _short_text(raw.get("primary_offer") or label),
        "required_slots": required,
        "optional_slots": optional,
        "slot_definitions": slot_definitions,
        "price_depends_on": _slot_list(raw.get("price_depends_on"), slot_definitions),
        "location_depends_on": _slot_list(raw.get("location_depends_on"), slot_definitions),
        "service_depends_on": _slot_list(raw.get("service_depends_on"), slot_definitions),
        "availability_depends_on": _slot_list(raw.get("availability_depends_on"), slot_definitions),
        "common_intents": _safe_list(raw.get("common_intents")),
        "confidence": _confidence(raw.get("confidence")),
        "created_at": created,
    }
    schema["domain_schema_id"] = str(raw.get("domain_schema_id") or "") or _schema_id(schema)
    return schema


def _normalize_rules_draft(
    raw: Mapping[str, Any],
    schema: Mapping[str, Any],
    *,
    tenant_id: int,
    created_at: datetime | None,
) -> dict[str, Any]:
    rules = []
    for item in raw.get("rules") if isinstance(raw.get("rules"), list) else []:
        if not isinstance(item, Mapping):
            continue
        rule = {
            "rule_type": _safe_key(str(item.get("rule_type") or "general")),
            "description": _short_text(item.get("description") or ""),
            "depends_on": _slot_list(item.get("depends_on"), schema.get("slot_definitions") if isinstance(schema, Mapping) else {}),
            "confidence": _confidence(item.get("confidence")),
            "needs_human_confirmation": True,
        }
        if rule["description"]:
            rules.append(rule)
    return {
        "schema_version": RULES_DRAFT_VERSION,
        "tenant_id": int(tenant_id),
        "domain_schema_id": str(schema.get("domain_schema_id") or ""),
        "domain": str(schema.get("domain") or "generic_sales"),
        "rules": rules[:30],
        "needs_human_confirmation": True,
        "created_at": _iso_utc(created_at),
    }


def _build_sample(
    dialogs: Sequence[Sequence[AvitoDialogMessage]],
    *,
    sample_dialogs: int,
    sample_messages_per_dialog: int,
) -> list[dict[str, Any]]:
    sample: list[dict[str, Any]] = []
    for dialog in list(dialogs)[: max(1, int(sample_dialogs or 80))]:
        messages = []
        for message in list(dialog)[: max(1, int(sample_messages_per_dialog or 8))]:
            role = str(getattr(message, "role", "") or "").strip().lower()
            if role not in {"client", "manager"}:
                continue
            text = _short_text(getattr(message, "text", "") or "", max_len=260)
            if text:
                messages.append({"role": role, "text": text})
        if messages:
            sample.append({"messages": messages})
    return sample


def _schema_id(schema: Mapping[str, Any]) -> str:
    payload = {
        "domain": schema.get("domain"),
        "required_slots": schema.get("required_slots"),
        "optional_slots": schema.get("optional_slots"),
        "slot_definitions": schema.get("slot_definitions"),
    }
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()


def _slot_list(value: Any, definitions: Mapping[str, Any] | None = None) -> list[str]:
    allowed = set(str(k) for k in (definitions or {}).keys())
    result = []
    if isinstance(value, (list, tuple, set)):
        for item in value:
            key = _safe_key(str(item or ""))
            if key and (not allowed or key in allowed):
                result.append(key)
    return sorted(set(result))


def _safe_list(value: Any) -> list[str]:
    result = []
    if isinstance(value, (list, tuple, set)):
        for item in value:
            text = _safe_key(str(item or ""))
            if text:
                result.append(text)
    return sorted(set(result))


def _string_map(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, str] = {}
    for key, item in value.items():
        safe_key = _safe_key(str(key or ""))
        text = _short_text(item or "")
        if safe_key and text:
            result[safe_key] = text
    return result


def _safe_key(value: str) -> str:
    normalized = _SAFE_KEY_RE.sub("_", str(value or "").lower()).strip("_")
    return normalized[:64]


def _short_text(value: Any, *, max_len: int = 180) -> str:
    text = " ".join(str(value or "").replace("\r", " ").replace("\n", " ").split()).strip()
    if _contains_raw_dialog_text(text):
        return ""
    return text[:max_len]


def _confidence(value: Any) -> float:
    try:
        score = float(value)
    except Exception:
        return 0.0
    if score > 1:
        score /= 100.0
    return max(0.0, min(score, 1.0))


def _contains_raw_dialog_text(value: Any) -> bool:
    text = json.dumps(value, ensure_ascii=False) if not isinstance(value, str) else value
    return any(marker in text for marker in _RAW_DIALOG_MARKERS)


def _iso_utc(value: datetime | None) -> str:
    dt = value or datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _use_responses_api(model: str) -> bool:
    normalized = str(model or "").lower()
    return normalized.startswith("gpt-5") or normalized.startswith("o")


def _silence_ai_transport_logs() -> None:
    for name in ("openai", "openai._base_client", "httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)


def _system_prompt() -> str:
    return """
You discover the business domain and reusable domain-specific slots for Avito sales dialogs.

Return only valid JSON with:
{
  "domain_schema": {
    "domain": "short_snake_case",
    "domain_label": "human Russian label",
    "primary_offer": "short Russian description",
    "required_slots": ["slot_key"],
    "optional_slots": ["slot_key"],
    "slot_definitions": {"slot_key": "Russian definition"},
    "price_depends_on": ["slot_key"],
    "location_depends_on": ["slot_key"],
    "service_depends_on": ["slot_key"],
    "availability_depends_on": ["slot_key"],
    "common_intents": ["price_question"],
    "confidence": 0.0
  },
  "business_rules_draft": {
    "rules": [
      {
        "rule_type": "price_dependency",
        "description": "Aggregate hypothesis only, no raw quotes",
        "depends_on": ["slot_key"],
        "confidence": 0.0
      }
    ]
  }
}

Rules:
- Do not hard-code doors. Infer the actual niche from the sample.
- For lawn mowing, likely slots include area_size, grass_height, location, waste_removal, urgency, access when supported by dialogs.
- For doors, likely slots include client_city, door_type, premise_type, size, thermal_break, mirror when supported by dialogs.
- For cleaning, likely slots include area_size, cleaning_type, rooms_count, pollution_level, location, urgency when supported by dialogs.
- business_rules_draft is only a hypothesis and must require human confirmation.
- Do not include raw customer messages, phone numbers, names, Avito ids, or long quotes.
""".strip()


__all__ = [
    "AvitoDomainDiscoveryConfig",
    "AvitoDomainDiscoveryResult",
    "AvitoDomainSchemaDiscoverer",
    "PROMPT_VERSION",
    "RULES_DRAFT_VERSION",
    "SCHEMA_VERSION",
    "build_default_discoverer",
    "discover_domain_schema",
    "generic_business_rules_draft",
    "generic_domain_schema",
]
