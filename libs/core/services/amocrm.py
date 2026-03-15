from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import pathlib
import re
import time
from typing import Any, Mapping, Sequence

from libs.core import db as db_module
from libs.core import sales_core as core_module
from libs.core.integrations import amocrm as amocrm_core
from libs.core.message_envelope import sanitize_display_name
from libs.core.repo import amocrm_tokens, crm_fields, crm_links, crm_outbox
from libs.core.services import amocrm_chat

logger = logging.getLogger(__name__)

AMOCRM_PROVIDER = "amocrm"

_ENV_CLIENT_ID = "AMOCRM_CLIENT_ID"
_ENV_CLIENT_SECRET = "AMOCRM_CLIENT_SECRET"
_ENV_REDIRECT_URL = "AMOCRM_REDIRECT_URL"
_ENV_BASE_URL = "AMOCRM_BASE_URL"
_ENV_SUBDOMAIN = "AMOCRM_SUBDOMAIN"
_ENV_AUTH_URL = "AMOCRM_AUTH_URL"


def _amocrm_cfg(cfg: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(cfg, Mapping):
        return None
    integrations = cfg.get("integrations")
    if not isinstance(integrations, Mapping):
        return None
    amocrm_cfg = integrations.get("amocrm")
    if isinstance(amocrm_cfg, Mapping):
        return dict(amocrm_cfg)
    return None


def get_amocrm_cfg(cfg: Mapping[str, Any] | None) -> dict[str, Any] | None:
    return _amocrm_cfg(cfg)


def _env_value(name: str, tenant_id: int | None = None) -> str:
    if tenant_id is not None:
        tenant_key = f"{name}_TENANT_{int(tenant_id)}"
        tenant_value = os.getenv(tenant_key)
        if tenant_value:
            return tenant_value.strip()
    return (os.getenv(name) or "").strip()


def env_oauth_configured(tenant_id: int | None = None) -> bool:
    return bool(_env_value(_ENV_CLIENT_ID, tenant_id) and _env_value(_ENV_CLIENT_SECRET, tenant_id))


def env_base_url(tenant_id: int | None = None) -> str:
    base = _env_value(_ENV_BASE_URL, tenant_id)
    if base:
        return base.rstrip("/")
    subdomain = _env_value(_ENV_SUBDOMAIN, tenant_id)
    if subdomain:
        return f"https://{subdomain}.amocrm.ru"
    return ""


def resolve_oauth_cfg(cfg: Mapping[str, Any] | None, tenant_id: int | None = None) -> dict[str, str]:
    if env_oauth_configured(tenant_id):
        return {
            "client_id": _env_value(_ENV_CLIENT_ID, tenant_id),
            "client_secret": _env_value(_ENV_CLIENT_SECRET, tenant_id),
            "redirect_url": _env_value(_ENV_REDIRECT_URL, tenant_id),
        }
    oauth_cfg = {}
    if isinstance(cfg, Mapping):
        raw = cfg.get("oauth")
        if isinstance(raw, Mapping):
            oauth_cfg = dict(raw)
    return {
        "client_id": str(oauth_cfg.get("client_id") or "").strip(),
        "client_secret": str(oauth_cfg.get("client_secret") or "").strip(),
        "redirect_url": str(oauth_cfg.get("redirect_url") or "").strip(),
    }


def _extract_subdomain(value: str) -> str:
    if not value:
        return ""
    cleaned = value.strip().lower()
    if "://" in cleaned:
        cleaned = re.sub(r"^https?://", "", cleaned)
    cleaned = cleaned.split("/", 1)[0]
    if "." in cleaned:
        return cleaned.split(".", 1)[0]
    return cleaned


def find_tenant_by_account(account_id: int | None, subdomain: str | None) -> int | None:
    tenants_root = getattr(core_module, "TENANTS_DIR", None)
    if tenants_root is None:
        return None
    try:
        entries = list(pathlib.Path(tenants_root).iterdir())
    except Exception:
        entries = []
    subdomain_val = _extract_subdomain(str(subdomain or ""))
    for entry in entries:
        if not entry.is_dir():
            continue
        try:
            tenant_id = int(entry.name)
        except Exception:
            continue
        cfg = core_module.read_tenant_config(tenant_id)
        amocrm_cfg = _amocrm_cfg(cfg)
        if not amocrm_cfg:
            continue
        cfg_account = amocrm_cfg.get("account_id")
        try:
            if account_id is not None and int(cfg_account) == int(account_id):
                return tenant_id
        except Exception:
            pass
        cfg_subdomain = str(amocrm_cfg.get("subdomain") or "").strip()
        cfg_base = str(amocrm_cfg.get("base_url") or "").strip()
        if subdomain_val:
            if _extract_subdomain(cfg_subdomain) == subdomain_val:
                return tenant_id
            if _extract_subdomain(cfg_base) == subdomain_val:
                return tenant_id
    return None


def resolve_base_url(cfg: Mapping[str, Any], tenant_id: int | None = None) -> str:
    env_base = env_base_url(tenant_id)
    if env_base:
        return env_base
    api_domain = str(cfg.get("api_domain") or "").strip()
    if api_domain:
        return f"https://{api_domain}".rstrip("/")
    api_base = str(cfg.get("api_base_url") or "").strip()
    if api_base:
        return api_base.rstrip("/")
    base_url = str(cfg.get("base_url") or "").strip()
    if base_url:
        return base_url.rstrip("/")
    subdomain = str(cfg.get("subdomain") or "").strip()
    if subdomain:
        return f"https://{subdomain}.amocrm.ru"
    return ""


def _extract_api_domain_from_token(token: str) -> str:
    if not token or token.count(".") < 2:
        return ""
    try:
        payload_b64 = token.split(".")[1]
        pad = "=" * (-len(payload_b64) % 4)
        payload_json = json.loads(base64.urlsafe_b64decode(payload_b64 + pad).decode("utf-8"))
    except Exception:
        return ""
    return str(payload_json.get("api_domain") or "").strip()


def resolve_auth_url(cfg: Mapping[str, Any], tenant_id: int | None = None) -> str:
    env_auth = _env_value(_ENV_AUTH_URL, tenant_id)
    if env_auth:
        return env_auth.rstrip("/")
    subdomain = str(cfg.get("subdomain") or "").strip()
    if subdomain:
        return f"https://{subdomain}.amocrm.ru"
    base_url = str(cfg.get("base_url") or "").strip()
    if base_url:
        cleaned = re.sub(r"^https?://", "", base_url).split("/", 1)[0]
        if cleaned.endswith(".amocrm.ru") and not cleaned.startswith("api-"):
            return f"https://{cleaned}"
    return "https://www.amocrm.ru"


_DEFAULT_STAGE_NAMES = ("Первый контакт", "Уточнение", "Согласование")


def _default_stage_name(index: int, fallback: str | None = None) -> str:
    if fallback:
        return str(fallback)
    if 0 <= index < len(_DEFAULT_STAGE_NAMES):
        return _DEFAULT_STAGE_NAMES[index]
    return f"Стадия {index + 1}"


def _extract_embedded_list(payload: Mapping[str, Any] | None, key: str) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping):
        return []
    embedded = payload.get("_embedded")
    if isinstance(embedded, Mapping):
        items = embedded.get(key)
        if isinstance(items, list):
            return [item for item in items if isinstance(item, Mapping)]
    items = payload.get(key)
    if isinstance(items, list):
        return [item for item in items if isinstance(item, Mapping)]
    return []


def _flatten_stage_hints(raw: Any) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    preferred_keys = (
        "description",
        "text",
        "hint",
        "prompt",
        "instruction",
        "value",
        "content",
        "message",
    )
    skip_keys = {"for", "role", "level", "id", "type", "name", "key"}

    def _visit(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, str):
            text = " ".join(node.strip().split())
            if len(text) < 3:
                return
            if len(text.split()) < 2:
                return
            key = text.lower()
            if key in seen:
                return
            seen.add(key)
            values.append(text[:500])
            return
        if isinstance(node, Mapping):
            handled = False
            for key_name in preferred_keys:
                if key_name in node:
                    _visit(node.get(key_name))
                    handled = True
            if handled:
                return
            for key_name, value in node.items():
                if str(key_name).strip().lower() in skip_keys:
                    continue
                _visit(value)
            return
        if isinstance(node, Sequence) and not isinstance(node, (str, bytes)):
            for item in node:
                _visit(item)

    _visit(raw)
    return values[:8]


def _build_default_stages(statuses: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    stages: list[dict[str, Any]] = []
    for index, status in enumerate(statuses):
        stage_id = status.get("id")
        try:
            stage_id_val = int(stage_id)
        except Exception:
            continue
        stage_name = _default_stage_name(index, str(status.get("name") or ""))
        stage_type = str(status.get("type") or "").strip().lower()
        stage_hints = _flatten_stage_hints(status.get("descriptions"))
        if not stage_hints:
            stage_hints = _flatten_stage_hints(status.get("description"))
        stages.append(
            {
                "name": stage_name,
                "amo_stage_id": stage_id_val,
                "type": stage_type,
                "hints": stage_hints,
            }
        )
    return stages


def _sanitize_stages_for_router(stages: Sequence[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    if not isinstance(stages, Sequence):
        return sanitized
    for item in stages:
        if not isinstance(item, Mapping):
            continue
        stage = dict(item)
        stage.pop("rule", None)
        stage.pop("description", None)
        stage.pop("descriptions", None)
        try:
            stage["amo_stage_id"] = int(stage.get("amo_stage_id") or 0)
        except Exception:
            continue
        if stage["amo_stage_id"] <= 0:
            continue
        stage["name"] = str(stage.get("name") or "").strip() or f"Стадия {len(sanitized) + 1}"
        stage["type"] = str(stage.get("type") or "").strip().lower()
        hints = _flatten_stage_hints(stage.get("hints"))
        if not hints:
            hints = _flatten_stage_hints(item.get("descriptions"))
        if not hints:
            hints = _flatten_stage_hints(item.get("description"))
        stage["hints"] = hints
        sanitized.append(stage)
    return sanitized


def _stage_hints_by_stage_id(stages: Sequence[Mapping[str, Any]] | None) -> dict[int, list[str]]:
    mapping: dict[int, list[str]] = {}
    for stage in _sanitize_stages_for_router(stages):
        try:
            stage_id = int(stage.get("amo_stage_id") or 0)
        except Exception:
            stage_id = 0
        if stage_id <= 0:
            continue
        hints = _flatten_stage_hints(stage.get("hints"))
        if hints:
            mapping[stage_id] = hints
    return mapping


def _merge_stage_hints(
    stages: Sequence[Mapping[str, Any]],
    *hint_maps: Mapping[int, Sequence[str]] | None,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for raw_stage in stages:
        if not isinstance(raw_stage, Mapping):
            continue
        stage = dict(raw_stage)
        try:
            stage_id = int(stage.get("amo_stage_id") or 0)
        except Exception:
            stage_id = 0
        hints = _flatten_stage_hints(stage.get("hints"))
        if not hints and stage_id > 0:
            for hint_map in hint_maps:
                if not isinstance(hint_map, Mapping):
                    continue
                candidate = _flatten_stage_hints(hint_map.get(stage_id))
                if candidate:
                    hints = candidate
                    break
        stage["hints"] = hints
        merged.append(stage)
    return merged


def _pipeline_hint_map(amocrm_cfg: Mapping[str, Any] | None, pipeline_id: int) -> dict[int, list[str]]:
    pipeline_id_val = _coerce_pipeline_id(pipeline_id)
    if pipeline_id_val <= 0 or not isinstance(amocrm_cfg, Mapping):
        return {}
    stages_by_pipeline = amocrm_cfg.get("stages_by_pipeline")
    if not isinstance(stages_by_pipeline, Mapping):
        return {}
    pipeline_entry = stages_by_pipeline.get(str(pipeline_id_val))
    if isinstance(pipeline_entry, Mapping):
        return _stage_hints_by_stage_id(pipeline_entry.get("stages"))
    if isinstance(pipeline_entry, Sequence) and not isinstance(pipeline_entry, (str, bytes)):
        return _stage_hints_by_stage_id(pipeline_entry)
    return {}


def _merge_stages_for_pipeline(
    stages: Sequence[Mapping[str, Any]] | None,
    amocrm_cfg: Mapping[str, Any] | None,
    pipeline_id: int,
) -> list[dict[str, Any]]:
    sanitized = _sanitize_stages_for_router(stages)
    if not sanitized:
        return []
    existing_hint_map = _stage_hints_by_stage_id(amocrm_cfg.get("stages") if isinstance(amocrm_cfg, Mapping) else None)
    pipeline_hint_map = _pipeline_hint_map(amocrm_cfg, pipeline_id)
    return _merge_stage_hints(sanitized, pipeline_hint_map, existing_hint_map)


def _find_lead_phone_field_id(fields: Sequence[Mapping[str, Any]]) -> int | None:
    for field in fields:
        if not isinstance(field, Mapping):
            continue
        code = str(field.get("code") or "").strip().lower()
        name = str(field.get("name") or "").strip().lower()
        if code == "phone":
            try:
                return int(field.get("id"))
            except Exception:
                continue
        if "телефон" in name or "phone" in name:
            try:
                return int(field.get("id"))
            except Exception:
                continue
    return None


async def _remote_entity_exists(
    client: amocrm_core.AmoCRMClient,
    *,
    entity_type: str,
    entity_id: int | None,
) -> bool | None:
    """Return True/False for known existence, None when state is unknown (network/etc)."""
    if not entity_id:
        return False
    entity = str(entity_type or "").strip().lower()
    try:
        if entity == "lead":
            payload = await client.get_lead(int(entity_id))
        elif entity == "contact":
            payload = await client.get_contact(int(entity_id))
        else:
            return None
        if not isinstance(payload, Mapping):
            return False
        remote_id = payload.get("id")
        try:
            return int(remote_id) == int(entity_id)
        except Exception:
            return False
    except Exception as exc:
        text = str(exc or "")
        if "amocrm_http_error:404" in text:
            return False
        return None


async def ensure_lead_phone_field_id(
    tenant_id: int,
    cfg: Mapping[str, Any] | None,
    client: amocrm_core.AmoCRMClient,
) -> int | None:
    amocrm_cfg = _amocrm_cfg(cfg) or {}
    existing = amocrm_cfg.get("lead_phone_field_id")
    try:
        existing_id = int(existing) if existing is not None else None
    except Exception:
        existing_id = None
    if existing_id:
        return existing_id
    try:
        payload = await client.get_lead_custom_fields()
    except Exception:
        logger.exception("amocrm_custom_fields_failed tenant=%s", tenant_id)
        return None
    fields = _extract_embedded_list(payload, "custom_fields")
    field_id = _find_lead_phone_field_id(fields)
    if not field_id:
        try:
            field_id = await client.create_lead_custom_field(name="Телефон")
        except Exception:
            logger.exception("amocrm_custom_field_create_failed tenant=%s", tenant_id)
            return None
    if not field_id:
        return None
    updated_cfg = dict(cfg) if isinstance(cfg, Mapping) else {}
    integrations = updated_cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg = dict(amocrm_cfg)
    amocrm_cfg["lead_phone_field_id"] = field_id
    integrations["amocrm"] = amocrm_cfg
    updated_cfg["integrations"] = integrations
    core_module.write_tenant_config(int(tenant_id), updated_cfg)
    return field_id


async def ensure_pipeline_config(
    tenant_id: int,
    cfg: Mapping[str, Any] | None,
    client: amocrm_core.AmoCRMClient,
) -> tuple[int, list[dict[str, Any]]] | None:
    amocrm_cfg = _amocrm_cfg(cfg) or {}
    pipeline_raw = amocrm_cfg.get("pipeline_id")
    try:
        pipeline_id = int(pipeline_raw)
    except Exception:
        pipeline_id = 0
    stages = amocrm_cfg.get("stages")
    now_ts = int(time.time())
    sync_ttl_seconds = 300
    try:
        last_synced_ts = int(amocrm_cfg.get("stages_synced_at") or 0)
    except Exception:
        last_synced_ts = 0
    sync_due = last_synced_ts <= 0 or (now_ts - last_synced_ts) >= sync_ttl_seconds
    if pipeline_id > 0 and isinstance(stages, list) and stages and not sync_due:
        sanitized = _merge_stages_for_pipeline(stages, amocrm_cfg, pipeline_id)
        if not sanitized:
            return None
        if sanitized != list(stages):
            updated_cfg = dict(cfg) if isinstance(cfg, Mapping) else {}
            integrations = updated_cfg.get("integrations")
            if not isinstance(integrations, dict):
                integrations = {}
            amocrm_cfg = dict(amocrm_cfg)
            amocrm_cfg["stages"] = sanitized
            amocrm_cfg["stages_synced_at"] = now_ts
            integrations["amocrm"] = amocrm_cfg
            updated_cfg["integrations"] = integrations
            core_module.write_tenant_config(int(tenant_id), updated_cfg)
        return pipeline_id, sanitized

    pipelines: list[Mapping[str, Any]] = []
    if pipeline_id <= 0:
        try:
            pipelines_payload = await client.get_pipelines()
        except Exception:
            logger.exception("amocrm_pipeline_fetch_failed tenant=%s", tenant_id)
            return None
        pipelines = _extract_embedded_list(pipelines_payload, "pipelines")
        if not pipelines:
            return None
        pipeline = pipelines[0]
        try:
            pipeline_id = int(pipeline.get("id") or 0)
        except Exception:
            pipeline_id = 0
        if pipeline_id <= 0:
            return None

    statuses: list[Mapping[str, Any]] = []
    try:
        pipeline_payload = await client.get_pipeline_stages(
            pipeline_id,
            with_descriptions=True,
        )
        statuses = _extract_embedded_list(pipeline_payload, "statuses")
    except Exception:
        logger.exception(
            "amocrm_pipeline_stages_failed tenant=%s pipeline=%s",
            tenant_id,
            pipeline_id,
        )
    if not statuses and pipelines:
        for item in pipelines:
            if not isinstance(item, Mapping):
                continue
            try:
                item_id = int(item.get("id") or 0)
            except Exception:
                item_id = 0
            if item_id != pipeline_id:
                continue
            statuses = _extract_embedded_list(item, "statuses")
            if statuses:
                break
    stages = _build_default_stages(statuses)
    stages = _merge_stages_for_pipeline(stages, amocrm_cfg, pipeline_id)
    if not stages:
        if pipeline_id > 0 and isinstance(amocrm_cfg.get("stages"), list):
            cached = _merge_stages_for_pipeline(amocrm_cfg.get("stages"), amocrm_cfg, pipeline_id)
            if cached:
                return pipeline_id, cached
        return None

    updated_cfg = dict(cfg) if isinstance(cfg, Mapping) else {}
    integrations = updated_cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg = dict(amocrm_cfg)
    amocrm_cfg["pipeline_id"] = pipeline_id
    amocrm_cfg["stages"] = stages
    amocrm_cfg["stages_synced_at"] = now_ts
    if pipelines:
        pipeline_options: list[dict[str, Any]] = []
        for item in pipelines:
            if not isinstance(item, Mapping):
                continue
            try:
                pid = int(item.get("id") or 0)
            except Exception:
                pid = 0
            if pid <= 0:
                continue
            name = str(item.get("name") or "").strip() or f"Воронка {pid}"
            pipeline_options.append({"id": pid, "name": name})
        if pipeline_options:
            amocrm_cfg["pipelines_cache"] = pipeline_options
            amocrm_cfg["pipelines_cached_at"] = now_ts
    integrations["amocrm"] = amocrm_cfg
    updated_cfg["integrations"] = integrations
    core_module.write_tenant_config(int(tenant_id), updated_cfg)
    return pipeline_id, stages


def _coerce_pipeline_id(value: Any) -> int:
    try:
        result = int(value)
    except Exception:
        result = 0
    return result if result > 0 else 0


def _resolve_pipeline_id_for_channel(
    amocrm_cfg: Mapping[str, Any] | None,
    *,
    channel: str,
    fallback_pipeline_id: int,
) -> int:
    if not isinstance(amocrm_cfg, Mapping):
        return fallback_pipeline_id
    channel_value = str(channel or "").strip().lower()
    if channel_value == "avito":
        selected = _coerce_pipeline_id(amocrm_cfg.get("pipeline_id_avito"))
        return selected or fallback_pipeline_id
    if channel_value in {"telegram", "max"}:
        selected = _coerce_pipeline_id(amocrm_cfg.get("pipeline_id_tgmax"))
        return selected or fallback_pipeline_id
    return fallback_pipeline_id


async def ensure_pipeline_stages(
    tenant_id: int,
    cfg: Mapping[str, Any] | None,
    client: amocrm_core.AmoCRMClient,
    pipeline_id: int,
) -> list[dict[str, Any]] | None:
    pipeline_id_val = _coerce_pipeline_id(pipeline_id)
    if pipeline_id_val <= 0:
        return None
    amocrm_cfg = _amocrm_cfg(cfg) or {}
    now_ts = int(time.time())
    sync_ttl_seconds = 300
    stages_by_pipeline = amocrm_cfg.get("stages_by_pipeline")
    cached_entry = None
    if isinstance(stages_by_pipeline, Mapping):
        cached_entry = stages_by_pipeline.get(str(pipeline_id_val))
    cached_stages: list[dict[str, Any]] = []
    cached_synced_at = 0
    if isinstance(cached_entry, Mapping):
        cached_stages = _sanitize_stages_for_router(cached_entry.get("stages"))
        try:
            cached_synced_at = int(cached_entry.get("synced_at") or 0)
        except Exception:
            cached_synced_at = 0
    elif isinstance(cached_entry, list):
        cached_stages = _sanitize_stages_for_router(cached_entry)
        try:
            cached_synced_at = int(amocrm_cfg.get("stages_by_pipeline_cached_at") or 0)
        except Exception:
            cached_synced_at = 0
    if cached_stages and cached_synced_at > 0 and (now_ts - cached_synced_at) < sync_ttl_seconds:
        return cached_stages

    statuses: list[Mapping[str, Any]] = []
    get_pipeline_stages = getattr(client, "get_pipeline_stages", None)
    if callable(get_pipeline_stages):
        try:
            payload = await get_pipeline_stages(
                pipeline_id_val,
                with_descriptions=True,
            )
            statuses = _extract_embedded_list(payload, "statuses")
        except Exception:
            logger.exception(
                "amocrm_pipeline_stages_failed tenant=%s pipeline=%s",
                tenant_id,
                pipeline_id_val,
            )
    stages = _build_default_stages(statuses)
    stages = _merge_stages_for_pipeline(
        stages,
        {
            **(dict(amocrm_cfg) if isinstance(amocrm_cfg, Mapping) else {}),
            "stages": cached_stages or amocrm_cfg.get("stages"),
        },
        pipeline_id_val,
    )
    if not stages:
        return cached_stages or None

    updated_cfg = dict(cfg) if isinstance(cfg, Mapping) else {}
    integrations = updated_cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg_copy = dict(amocrm_cfg)
    stages_map = amocrm_cfg_copy.get("stages_by_pipeline")
    if not isinstance(stages_map, dict):
        stages_map = {}
    stages_map[str(pipeline_id_val)] = {"stages": stages, "synced_at": now_ts}
    amocrm_cfg_copy["stages_by_pipeline"] = stages_map
    if _coerce_pipeline_id(amocrm_cfg_copy.get("pipeline_id")) == pipeline_id_val:
        amocrm_cfg_copy["stages"] = stages
        amocrm_cfg_copy["stages_synced_at"] = now_ts
    integrations["amocrm"] = amocrm_cfg_copy
    updated_cfg["integrations"] = integrations
    core_module.write_tenant_config(int(tenant_id), updated_cfg)
    return stages

def mask_amocrm_cfg(
    cfg: Mapping[str, Any] | None,
    *,
    tenant_id: int | None = None,
) -> dict[str, Any] | None:
    if not isinstance(cfg, Mapping):
        return None
    result = dict(cfg)
    result["env_configured"] = env_oauth_configured(tenant_id)
    env_base = env_base_url(tenant_id)
    if env_base:
        result["env_base_url"] = env_base
    oauth = result.get("oauth")
    if isinstance(oauth, Mapping):
        oauth_copy = dict(oauth)
        if oauth_copy.get("client_secret"):
            oauth_copy["client_secret"] = "***"
        result["oauth"] = oauth_copy
    manual = result.get("manual")
    if isinstance(manual, Mapping):
        manual_copy = dict(manual)
        if manual_copy.get("access_token"):
            manual_copy["access_token"] = "***"
        result["manual"] = manual_copy
    result.pop("tokens", None)
    return result


def _build_custom_fields(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    custom_fields: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        field_id = item.get("amo_field_id")
        if field_id is None:
            # Skip unresolved fields for lead update.
            # In particular, PHONE by field_code is invalid for leads in some amo accounts.
            continue
        try:
            field_id_val = int(field_id)
        except Exception:
            continue
        value = str(item.get("value") or "").strip()
        if not value:
            continue
        custom_fields.append(
            {
                "field_id": field_id_val,
                "values": [{"value": value}],
            }
        )
    return custom_fields


async def _resolve_existing_target_by_phone(
    client: amocrm_core.AmoCRMClient,
    phone_value: str,
) -> tuple[int | None, int | None]:
    query = str(phone_value or "").strip()
    if not query:
        return None, None
    try:
        contacts = await client.search_contacts(query)
    except Exception:
        return None, None
    candidates: list[tuple[int, int]] = []
    for item in contacts:
        try:
            contact_id = int(item.get("id"))
        except Exception:
            continue
        try:
            full_contact = await client.get_contact(contact_id, with_leads=True)
        except Exception:
            continue
        embedded = full_contact.get("_embedded") if isinstance(full_contact, Mapping) else None
        leads = embedded.get("leads") if isinstance(embedded, Mapping) else None
        if not isinstance(leads, list):
            continue
        for lead_item in leads:
            if not isinstance(lead_item, Mapping):
                continue
            try:
                lead_id = int(lead_item.get("id"))
            except Exception:
                continue
            if lead_id > 0:
                candidates.append((contact_id, lead_id))
    if not candidates:
        return None, None
    candidates.sort(key=lambda pair: pair[1], reverse=True)
    return candidates[0]


def _needs_history(rules: Sequence[Mapping[str, Any]] | None) -> bool:
    if not rules:
        return False
    for rule in rules:
        if not isinstance(rule, Mapping):
            continue
        mode = str(rule.get("apply_mode") or "").strip().lower()
        if mode == "any_history":
            return True
    return False


def _build_note_text(
    *,
    template: str | None,
    channel: str,
    last_text: str,
    fields: Mapping[str, Any],
) -> str:
    summary_parts = [f"{key}={value}" for key, value in fields.items() if value]
    summary = "; ".join(summary_parts)
    text = template or "Inbound ({channel}): {text}\nFields: {fields}"
    result = (
        text.replace("{channel}", channel)
        .replace("{text}", last_text)
        .replace("{fields}", summary or "-")
    )
    return result[:900]


def _is_unsorted_stage(stage: Mapping[str, Any]) -> bool:
    stage_type = str(stage.get("type") or "").strip().lower()
    if stage_type in {"unsorted", "incoming_leads"}:
        return True
    name = str(stage.get("name") or "").strip().lower()
    return name.startswith("неразобран") or name == "unsorted"


def _is_terminal_stage(stage: Mapping[str, Any]) -> bool:
    stage_type = str(stage.get("type") or "").strip().lower()
    if stage_type in {"won", "lost"}:
        return True
    try:
        amo_stage_id = int(stage.get("amo_stage_id") or 0)
    except Exception:
        amo_stage_id = 0
    if amo_stage_id in {142, 143}:
        return True
    name = str(stage.get("name") or "").strip().lower()
    return "успешно реализ" in name or "не реализ" in name


def build_stages_from_statuses(statuses: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return _build_default_stages(statuses)


def _coerce_int(value: Any, default: int, *, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    if min_value is not None:
        parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def _coerce_float(
    value: Any,
    default: float,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    if min_value is not None:
        parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def _normalize_stage_router_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    aliases = {
        "off": "off",
        "disabled": "off",
        "none": "off",
        "semi": "semi_auto",
        "semi-auto": "semi_auto",
        "semi_auto": "semi_auto",
        "manual": "semi_auto",
        "auto": "auto",
    }
    return aliases.get(normalized, "auto")


def _normalize_rules_options(cfg: Mapping[str, Any]) -> dict[str, Any]:
    raw = cfg.get("rules_options") if isinstance(cfg, Mapping) else None
    options = dict(raw) if isinstance(raw, Mapping) else {}
    mode = _normalize_stage_router_mode(options.get("stage_router_mode"))
    max_stage_jump = _coerce_int(
        options.get("stage_router_max_stage_jump"),
        1,
        min_value=1,
        max_value=3,
    )
    timeout_seconds = _coerce_float(
        options.get("stage_router_timeout_seconds"),
        _coerce_float(getattr(core_module.settings, "OPENAI_TIMEOUT_SECONDS", 4.0), 4.0, min_value=2.0),
        min_value=2.0,
        max_value=30.0,
    )
    history_limit = _coerce_int(options.get("stage_router_history_limit"), 6, min_value=3, max_value=20)
    return {
        "stage_router_mode": mode,
        "stage_router_model": str(
            options.get("stage_router_model")
            or os.getenv("STAGE_ROUTER_MODEL")
            or getattr(core_module.settings, "OPENAI_MODEL", "gpt-4o-mini")
        ).strip()
        or "gpt-4o-mini",
        "stage_router_timeout_seconds": timeout_seconds,
        "stage_router_cooldown_seconds": _coerce_int(
            options.get("stage_router_cooldown_seconds"),
            300,
            min_value=0,
            max_value=86400,
        ),
        "stage_router_confidence_auto": _coerce_float(
            options.get("stage_router_confidence_auto"),
            0.72,
            min_value=0.0,
            max_value=1.0,
        ),
        "stage_router_confidence_semi": _coerce_float(
            options.get("stage_router_confidence_semi"),
            0.45,
            min_value=0.0,
            max_value=1.0,
        ),
        "stage_router_allow_terminal_auto": bool(options.get("stage_router_allow_terminal_auto")),
        "stage_router_max_stage_jump": max_stage_jump,
        "stage_router_move_dedup_seconds": _coerce_int(
            options.get("stage_router_move_dedup_seconds"),
            90,
            min_value=0,
            max_value=3600,
        ),
    }


def _parse_stage_router_decision(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping):
        return None
    action = str(payload.get("action") or "").strip().upper()
    if action not in {"NOOP", "MOVE_STAGE", "ASK_MANAGER"}:
        action = "NOOP"
    try:
        target_stage_index = int(payload.get("target_stage_index"))
    except Exception:
        target_stage_index = -1
    confidence = _coerce_float(payload.get("confidence"), 0.0, min_value=0.0, max_value=1.0)
    reason = str(payload.get("reason") or "").strip()[:500]
    missing_fields_raw = payload.get("missing_fields")
    missing_fields: list[str] = []
    if isinstance(missing_fields_raw, Sequence) and not isinstance(missing_fields_raw, (str, bytes)):
        for item in missing_fields_raw:
            value = str(item or "").strip()
            if value:
                missing_fields.append(value[:64])
    evidence_raw = payload.get("evidence")
    evidence: list[str] = []
    if isinstance(evidence_raw, Sequence) and not isinstance(evidence_raw, (str, bytes)):
        for item in evidence_raw:
            value = " ".join(str(item or "").strip().split())
            if value:
                evidence.append(value[:180])
    elif isinstance(evidence_raw, str):
        value = " ".join(evidence_raw.strip().split())
        if value:
            evidence.append(value[:180])
    return {
        "action": action,
        "target_stage_index": target_stage_index,
        "confidence": confidence,
        "reason": reason,
        "missing_fields": missing_fields,
        "evidence": evidence,
    }


def _normalize_stage_fact_role(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"lead", "client", "customer", "buyer", "incoming", "user"}:
        return "lead"
    if raw in {"manager", "agent", "operator", "sales"}:
        return "manager"
    if raw in {"bot", "assistant", "llm", "auto"}:
        return "bot"
    if raw in {"system", "service"}:
        return "system"
    return "unknown"


def _is_system_message_text(text: str) -> bool:
    value = " ".join(str(text or "").strip().lower().split())
    if not value:
        return False
    return (
        value.startswith("[системное сообщение]")
        or "системное сообщение]" in value
        or "пользователь ознакомился с" in value
        or "ссылка на объявление:" in value
        or value.startswith("❌ сообщение не отправлено")
    )


def _normalize_message_role(*, direction: str, source_role: str | None, text: str) -> str:
    hinted = _normalize_stage_fact_role(source_role)
    if hinted != "unknown":
        return hinted
    if _is_system_message_text(text):
        return "system"
    dir_norm = str(direction or "").strip().lower()
    if dir_norm == "in":
        return "lead"
    if dir_norm == "out":
        # outbound default is manager to allow stage transitions from manager actions
        return "manager"
    return "unknown"


def _parse_stage_router_history_items(texts: Sequence[str] | None) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    if not isinstance(texts, Sequence):
        return items
    for raw in texts:
        line = str(raw or "").strip()
        if not line:
            continue
        m = re.match(r"^\s*([a-zа-яё_]+)\s*:\s*(.+)$", line, flags=re.IGNORECASE)
        if m:
            role = _normalize_stage_fact_role(m.group(1))
            text = m.group(2).strip()
        else:
            role = "lead"
            text = line
        if not text:
            continue
        if role == "system" or _is_system_message_text(text):
            continue
        items.append({"role": role, "text": text})
    return items


def _history_items_to_text(items: Sequence[Mapping[str, Any]]) -> str:
    lines: list[str] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        role = _normalize_stage_fact_role(item.get("role"))
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        if role == "system":
            continue
        if role == "unknown":
            role = "lead"
        lines.append(f"{role}: {text}")
    return "\n".join(lines)


def _parse_stage_checks(payload: Any, *, allowed_targets: Sequence[int]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    allowed = {int(x) for x in allowed_targets}
    if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes)):
        return checks
    for raw in payload:
        if not isinstance(raw, Mapping):
            continue
        try:
            target = int(raw.get("target_stage_index"))
        except Exception:
            continue
        if target not in allowed:
            continue
        ready = bool(raw.get("ready"))
        confidence = _coerce_float(raw.get("confidence"), 0.0, min_value=0.0, max_value=1.0)
        reason = str(raw.get("reason") or "").strip()[:500]
        missing_raw = raw.get("missing_fields")
        missing: list[str] = []
        if isinstance(missing_raw, Sequence) and not isinstance(missing_raw, (str, bytes)):
            for item in missing_raw:
                value = str(item or "").strip()
                if value:
                    missing.append(value[:120])
        evidence: list[dict[str, Any]] = []
        evidence_raw = raw.get("evidence")
        if isinstance(evidence_raw, Sequence) and not isinstance(evidence_raw, (str, bytes)):
            for item in evidence_raw:
                if isinstance(item, Mapping):
                    quote = str(item.get("quote") or item.get("text") or "").strip()
                    if not quote:
                        continue
                    evidence.append(
                        {
                            "quote": quote[:180],
                            "source_role": _normalize_stage_fact_role(item.get("source_role")),
                            "is_new": bool(item.get("is_new")),
                        }
                    )
                else:
                    quote = str(item or "").strip()
                    if quote:
                        evidence.append({"quote": quote[:180], "source_role": "unknown", "is_new": False})
        checks.append(
            {
                "target_stage_index": target,
                "ready": ready,
                "confidence": confidence,
                "reason": reason,
                "missing_fields": missing,
                "evidence": evidence,
            }
        )
    return checks


def _stage_suggestion_signature(
    target_stage_index: int,
    missing_fields: Sequence[str] | None,
    evidence: Sequence[str] | None,
) -> str:
    mf = [str(x or "").strip().lower() for x in (missing_fields or []) if str(x or "").strip()]
    payload = {
        "target": int(target_stage_index),
        "missing": sorted(set(mf)),
        "has_evidence": bool([str(x or "").strip() for x in (evidence or []) if str(x or "").strip()]),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return str(abs(hash(raw)))


def _supported_evidence(
    evidence: Sequence[str] | None,
    *,
    last_text: str,
    history_text: str,
) -> list[str]:
    def _token_variants(value: str) -> set[str]:
        normalized = str(value or "").strip().lower()
        if len(normalized) < 2:
            return set()
        variants = {normalized}
        if normalized[-1] in {"а", "я", "ы", "и", "у", "е", "о", "ю"} and len(normalized) >= 3:
            variants.add(normalized[:-1])
        if len(normalized) >= 5 and normalized.endswith(("ого", "ему", "ыми", "ами", "ях", "ах", "ов", "ев")):
            variants.add(normalized[:-2])
        return variants

    source = " ".join([str(history_text or "").strip(), str(last_text or "").strip()]).strip().lower()
    source = " ".join(source.split())
    if not source:
        return []
    last_source = " ".join(str(last_text or "").strip().lower().split())
    source_tokens: set[str] = set()
    for token in re.split(r"[^0-9a-zа-яё]+", source, flags=re.IGNORECASE):
        if len(token) < 2:
            continue
        source_tokens.update(_token_variants(token))
    last_tokens: set[str] = set()
    for token in re.split(r"[^0-9a-zа-яё]+", last_source, flags=re.IGNORECASE):
        if len(token) < 2:
            continue
        last_tokens.update(_token_variants(token))
    supported: list[str] = []
    for item in (evidence or []):
        raw_item = str(item or "").strip()
        value = " ".join(raw_item.lower().split())
        if len(value) < 3:
            continue
        if value in source:
            supported.append(raw_item[:180])
            continue
        value_tokens: list[str] = []
        for token in re.split(r"[^0-9a-zа-яё]+", value, flags=re.IGNORECASE):
            if len(token) >= 2:
                value_tokens.append(token)
        if not value_tokens:
            continue
        expanded_value_tokens: set[str] = set()
        for token in value_tokens:
            expanded_value_tokens.update(_token_variants(token))
        informative_tokens = {token for token in expanded_value_tokens if len(token) >= 2}
        tokens_to_match = informative_tokens or expanded_value_tokens
        intersection = [token for token in expanded_value_tokens if token in source_tokens]
        if informative_tokens:
            intersection = [token for token in informative_tokens if token in source_tokens]
        overlap_ratio = len(intersection) / max(1, len(tokens_to_match))
        strong_hits = [
            token for token in intersection if len(token) >= 3 or any(ch.isdigit() for ch in token)
        ]
        # Require either multiple anchors or one strong high-overlap anchor.
        if (
            (len(intersection) >= 2 and len(strong_hits) >= 1)
            or (len(intersection) >= 1 and overlap_ratio >= 0.7)
            or (
                len(strong_hits) >= 1
                and len(tokens_to_match) <= 3
                and any(token in last_tokens for token in strong_hits)
            )
        ):
            supported.append(raw_item[:180])
            continue
        # Numeric evidence support (prices, ids, quantities)
        value_digits = re.findall(r"\d+", value)
        if value_digits and any(d in source for d in value_digits):
            supported.append(raw_item[:180])
    return supported


def _has_contact_signal(text: str) -> bool:
    value = str(text or "").strip().lower()
    if not value:
        return False
    if re.search(r"(?<!\w)@[a-z0-9_]{4,}", value, flags=re.IGNORECASE):
        return True
    if re.search(r"(?<!\d)\+?\d[\d\s()\-]{8,}\d(?!\d)", value):
        return True
    if re.search(r"\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b", value, flags=re.IGNORECASE):
        return True
    return False


def _has_schedule_signal(text: str) -> bool:
    value = str(text or "").strip().lower()
    if not value:
        return False
    if re.search(r"\b\d{1,2}[:.]\d{2}\b", value):
        return True
    if re.search(r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?\b", value):
        return True
    return False


async def _decide_next_stage_llm(
    stages: Sequence[Mapping[str, Any]],
    *,
    current_stage_index: int,
    inbound_count: int,
    last_text: str,
    history_text: str,
    message_role: str = "lead",
    history_items: Sequence[Mapping[str, Any]] | None = None,
    extracted_fields: Mapping[str, Any],
    options: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not stages:
        return None
    client = core_module._get_openai_client()
    if client is None:
        return None
    create_fn = getattr(getattr(getattr(client, "chat", None), "completions", None), "create", None)
    if not callable(create_fn):
        return None

    current_index = _coerce_int(current_stage_index, 0, min_value=0, max_value=max(0, len(stages) - 1))
    max_jump = _coerce_int(options.get("stage_router_max_stage_jump"), 1, min_value=1, max_value=3)
    allowed_targets = list(range(current_index + 1, min(len(stages), current_index + max_jump + 1)))

    stage_payload: list[dict[str, Any]] = []
    for idx, stage in enumerate(stages):
        if not isinstance(stage, Mapping):
            continue
        try:
            stage_id = int(stage.get("amo_stage_id") or 0)
        except Exception:
            stage_id = 0
        stage_payload.append(
            {
                "index": idx,
                "stage_id": stage_id,
                "name": str(stage.get("name") or "").strip(),
                "type": str(stage.get("type") or "").strip().lower(),
                "hints": _flatten_stage_hints(stage.get("hints")),
            }
        )
    normalized_role = _normalize_stage_fact_role(message_role)
    if normalized_role == "unknown":
        normalized_role = "lead"
    history_payload: list[dict[str, str]] = []
    if isinstance(history_items, Sequence):
        for item in history_items:
            if not isinstance(item, Mapping):
                continue
            role = _normalize_stage_fact_role(item.get("role"))
            text = str(item.get("text") or "").strip()
            if not text or role == "system":
                continue
            history_payload.append({"role": role if role != "unknown" else "lead", "text": text[:500]})
    if not history_payload and history_text:
        history_payload = _parse_stage_router_history_items(str(history_text).splitlines())
    messages = [
        {
            "role": "system",
            "content": (
                "Ты анализатор автоперехода стадий amoCRM. Возвращай только JSON. "
                "Не используй системные сообщения как факт клиента. "
                "Учитывай роли источника фактов: lead/manager/bot. "
                "Для каждой allowed стадии оцени готовность через stage_checks и hints целевой стадии. "
                "Если доказательств недостаточно — ready=false и укажи missing_fields. "
                "Не предлагай target_stage_index вне allowed_target_indexes."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "current_stage_index": current_index,
                    "allowed_target_indexes": allowed_targets,
                    "stages": stage_payload,
                    "inbound_count": int(inbound_count),
                    "last_message": {
                        "role": normalized_role,
                        "text": str(last_text or "").strip(),
                    },
                    "recent_history": history_payload,
                    "extracted_fields": dict(extracted_fields or {}),
                    "required_output_schema": {
                        "action": "MOVE_STAGE|NOOP|ASK_MANAGER",
                        "target_stage_index": "int",
                        "confidence": "float(0..1)",
                        "reason": "short string",
                        "missing_fields": ["field1", "field2"],
                        "evidence": ["short quote 1", "short quote 2"],
                        "stage_checks": [
                            {
                                "target_stage_index": "int",
                                "ready": "bool",
                                "confidence": "float(0..1)",
                                "reason": "short string",
                                "missing_fields": ["field1", "field2"],
                                "evidence": [
                                    {
                                        "quote": "short quote",
                                        "source_role": "lead|manager|bot|system",
                                        "is_new": "bool",
                                    }
                                ],
                            }
                        ],
                    },
                },
                ensure_ascii=False,
            ),
        },
    ]
    timeout_seconds = _coerce_float(options.get("stage_router_timeout_seconds"), 4.0, min_value=2.0, max_value=30.0)
    model = str(options.get("stage_router_model") or "").strip() or "gpt-4o-mini"
    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(
                create_fn,
                model=model,
                temperature=0,
                max_tokens=220,
                response_format={"type": "json_object"},
                messages=messages,
                timeout=timeout_seconds,
            ),
            timeout=timeout_seconds + 1.0,
        )
    except Exception:
        logger.exception("amocrm_stage_router_llm_failed model=%s", model)
        return None
    try:
        content = (response.choices[0].message.content or "").strip()  # type: ignore[index,attr-defined]
    except Exception:
        content = ""
    if not content:
        return None
    try:
        raw = json.loads(content)
    except Exception:
        logger.warning("amocrm_stage_router_invalid_json content=%s", content[:300])
        return None
    decision = _parse_stage_router_decision(raw)
    if not decision:
        return None
    stage_checks = _parse_stage_checks(raw.get("stage_checks"), allowed_targets=allowed_targets)
    if stage_checks:
        decision["stage_checks"] = stage_checks
    if decision.get("action") == "MOVE_STAGE":
        target_stage_index = _coerce_int(decision.get("target_stage_index"), -1)
        if target_stage_index not in allowed_targets:
            logger.info(
                "amocrm_stage_router_reject_out_of_bounds current=%s target=%s allowed=%s",
                current_index,
                target_stage_index,
                allowed_targets,
            )
            return {
                "action": "NOOP",
                "target_stage_index": -1,
                "confidence": 0.0,
                "reason": "target_out_of_allowed_range",
                "missing_fields": [],
                "evidence": [],
            }
        if decision.get("missing_fields"):
            return {
                "action": "NOOP",
                "target_stage_index": -1,
                "confidence": _coerce_float(decision.get("confidence"), 0.0, min_value=0.0, max_value=1.0),
                "reason": "move_with_missing_fields",
                "missing_fields": list(decision.get("missing_fields") or []),
                "evidence": list(decision.get("evidence") or []),
                "stage_checks": stage_checks,
            }
        if stage_checks and not any(
            int(item.get("target_stage_index") or -1) == target_stage_index and bool(item.get("ready"))
            for item in stage_checks
            if isinstance(item, Mapping)
        ):
            return {
                "action": "NOOP",
                "target_stage_index": -1,
                "confidence": _coerce_float(decision.get("confidence"), 0.0, min_value=0.0, max_value=1.0),
                "reason": "target_not_ready_by_stage_checks",
                "missing_fields": list(decision.get("missing_fields") or []),
                "evidence": list(decision.get("evidence") or []),
                "stage_checks": stage_checks,
            }
    return decision


async def resolve_api_base_url(
    amocrm_cfg: Mapping[str, Any] | None,
    tenant_id: int,
    token_entry: Any | None = None,
) -> str:
    base_url = resolve_base_url(amocrm_cfg, int(tenant_id))
    if base_url:
        return base_url
    token = token_entry
    if token is None:
        try:
            token = await amocrm_tokens.get(int(tenant_id))
        except Exception:
            token = None
    if not token:
        return ""
    api_domain = ""
    if token.raw_payload:
        api_domain = str(token.raw_payload.get("api_domain") or "").strip()
        if not api_domain:
            token_str = str(token.raw_payload.get("access_token") or "")
            api_domain = _extract_api_domain_from_token(token_str)
    if not api_domain and token.access_token:
        api_domain = _extract_api_domain_from_token(token.access_token)
    if api_domain:
        return f"https://{api_domain}".rstrip("/")
    return ""

def _amocrm_name(value: Any, *, allow_at: bool) -> str | None:
    cleaned = sanitize_display_name(value)
    if not cleaned:
        return None
    legacy_match = re.fullmatch(r"(?i)tg:\s*@?([a-z0-9_]{3,})", cleaned)
    if legacy_match:
        cleaned = f"@{legacy_match.group(1)}"
    if re.fullmatch(r"(?i)tg:id\s+\d+", cleaned):
        return None
    return cleaned


async def _resolve_lead_names(
    *,
    lead_id: int,
    extracted_fields: Mapping[str, Any],
) -> tuple[str, str | None]:
    primary_name = sanitize_display_name(extracted_fields.get("name"))
    meta = await db_module.get_lead_dialog_metadata(int(lead_id))
    title = sanitize_display_name((meta or {}).get("title"))
    username = str((meta or {}).get("telegram_username") or "").strip()
    if username and not username.startswith("@"):
        username = f"@{username}"
    username = sanitize_display_name(username)
    contact = sanitize_display_name((meta or {}).get("contact"))
    avito_login = sanitize_display_name((meta or {}).get("avito_login"))
    peer = str((meta or {}).get("peer") or "").strip()

    lead_name = ""
    for candidate in (
        _amocrm_name(primary_name, allow_at=False),
        _amocrm_name(title, allow_at=False),
        _amocrm_name(username, allow_at=False),
        _amocrm_name(avito_login, allow_at=False),
        sanitize_display_name(peer),
        _amocrm_name(contact, allow_at=False),
    ):
        if candidate:
            lead_name = candidate
            break
    if not lead_name:
        lead_name = f"Avio lead {lead_id}"

    contact_name = ""
    for candidate in (
        _amocrm_name(username, allow_at=True),
        _amocrm_name(primary_name, allow_at=True),
        _amocrm_name(title, allow_at=True),
        _amocrm_name(contact, allow_at=True),
        _amocrm_name(avito_login, allow_at=True),
    ):
        if candidate:
            contact_name = candidate
            break
    return lead_name, contact_name or None


async def amocrm_on_inbound_message(
    tenant_id: int,
    lead_id: int,
    *,
    text: str,
    channel: str,
    attachments: Sequence[Mapping[str, Any]] | None = None,
    source_role: str | None = None,
) -> None:
    await _amocrm_on_message(
        tenant_id,
        lead_id,
        text=text,
        channel=channel,
        attachments=attachments,
        direction="in",
        source_role=source_role,
    )
    if str(channel or "").strip().lower() in {"telegram", "avito"}:
        try:
            await amocrm_chat.enqueue_message(
                int(tenant_id),
                int(lead_id),
                direction="in",
                text=text,
                channel=channel,
                attachments=list(attachments) if attachments else None,
            )
        except Exception:
            logger.exception("amocrm_chat_enqueue_failed tenant=%s lead=%s direction=in", tenant_id, lead_id)


async def amocrm_on_outbound_message(
    tenant_id: int,
    lead_id: int,
    *,
    text: str,
    channel: str,
    attachments: Sequence[Mapping[str, Any]] | None = None,
    sync_chat: bool = True,
    source_role: str | None = None,
) -> None:
    await _amocrm_on_message(
        tenant_id,
        lead_id,
        text=text,
        channel=channel,
        attachments=attachments,
        direction="out",
        source_role=source_role,
    )
    if not bool(sync_chat):
        return
    if str(channel or "").strip().lower() in {"telegram", "avito"}:
        try:
            await amocrm_chat.enqueue_message(
                int(tenant_id),
                int(lead_id),
                direction="out",
                text=text,
                channel=channel,
                attachments=list(attachments) if attachments else None,
            )
        except Exception:
            logger.exception("amocrm_chat_enqueue_failed tenant=%s lead=%s direction=out", tenant_id, lead_id)


async def _amocrm_on_message(
    tenant_id: int,
    lead_id: int,
    *,
    text: str,
    channel: str,
    attachments: Sequence[Mapping[str, Any]] | None = None,
    direction: str,
    source_role: str | None = None,
) -> None:
    cfg = core_module.read_tenant_config(int(tenant_id))
    amocrm_cfg = _amocrm_cfg(cfg)
    if not amocrm_cfg or not bool(amocrm_cfg.get("enabled")):
        return
    channel_value = str(channel or "").strip().lower()
    chat_mode = channel_value in {"telegram", "avito"} and amocrm_chat.is_enabled(cfg, int(tenant_id))

    token_entry = await amocrm_tokens.get(int(tenant_id))
    if not token_entry or not token_entry.access_token:
        return

    base_url = await resolve_api_base_url(amocrm_cfg, int(tenant_id), token_entry)
    if not base_url:
        return
    oauth_cfg = resolve_oauth_cfg(amocrm_cfg, tenant_id)
    client = amocrm_core.AmoCRMClient(
        tenant_id=int(tenant_id),
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )

    pipeline_id_raw = amocrm_cfg.get("pipeline_id")
    pipeline_id = _coerce_pipeline_id(pipeline_id_raw)
    stages = amocrm_cfg.get("stages")
    if pipeline_id <= 0 or not isinstance(stages, list) or not stages:
        ensured = await ensure_pipeline_config(int(tenant_id), cfg, client)
        if not ensured:
            return
        pipeline_id, stages = ensured
        cfg = core_module.read_tenant_config(int(tenant_id))
        amocrm_cfg = _amocrm_cfg(cfg) or amocrm_cfg
    selected_pipeline_id = _resolve_pipeline_id_for_channel(
        amocrm_cfg,
        channel=channel_value,
        fallback_pipeline_id=pipeline_id,
    ) or pipeline_id
    # Always hydrate selected pipeline stages via per-pipeline cache refresh.
    # This keeps amoCRM stage hints/descriptions in sync even when legacy
    # top-level `stages` exists but is stale or missing hints.
    selected_stages = await ensure_pipeline_stages(
        int(tenant_id),
        cfg,
        client,
        selected_pipeline_id,
    )
    if selected_stages:
        pipeline_id = selected_pipeline_id
        stages = selected_stages
    lead_phone_field_id = await ensure_lead_phone_field_id(int(tenant_id), cfg, client)

    last_text = text or ""
    attachment_payloads: list[dict[str, Any]] = []
    if attachments:
        seen_urls: set[str] = set()
        for item in attachments:
            if not isinstance(item, Mapping):
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            name = str(item.get("filename") or item.get("name") or "").strip()
            mime = str(item.get("mime") or item.get("mime_type") or "").strip()
            attachment_payloads.append({"url": url, "filename": name, "mime": mime})
    attachment_count = len(attachment_payloads)
    direction_norm = str(direction or "").strip().lower()
    is_inbound = direction_norm == "in"
    is_outbound = direction_norm == "out"
    stage_eval_enabled = is_inbound or is_outbound
    current_message_role = _normalize_message_role(
        direction=direction_norm,
        source_role=source_role,
        text=last_text,
    )
    fields_rules = amocrm_cfg.get("fields_rules")
    rules_list = list(fields_rules) if isinstance(fields_rules, list) else []
    has_phone_rule = any(
        isinstance(rule, Mapping) and str(rule.get("key") or "").strip().lower() == "phone"
        for rule in rules_list
    )
    if not lead_phone_field_id:
        lead_phone_field_id = amocrm_cfg.get("lead_phone_field_id")
    if not has_phone_rule:
        rules_list.append(
            {
                "key": "phone",
                "regex": r"(\+?\d[\d\s\-()]{7,})",
                "apply_mode": "last_inbound",
                "amo_field_id": lead_phone_field_id,
            }
        )
    existing_map: dict[str, str] = {}
    extracted_map: dict[str, str] = {}
    changed_fields: list[dict[str, Any]] = []
    if is_inbound:
        history_text = ""
        if _needs_history(rules_list):
            texts = await db_module.list_recent_inbound_texts(int(tenant_id), int(lead_id))
            history_text = amocrm_core.build_history_text(texts)
        extracted = amocrm_core.extract_fields(
            rules_list,
            last_text=last_text,
            history_text=history_text,
        )
        existing_fields = await crm_fields.list_fields(int(tenant_id), int(lead_id), AMOCRM_PROVIDER)
        existing_map = {
            str(item.get("field_key")): str(item.get("field_value"))
            for item in existing_fields
            if item.get("field_key") is not None
        }
        extracted_map = dict(existing_map)
        for item in extracted:
            key = str(item.get("key") or "").strip()
            value = str(item.get("value") or "").strip()
            if not key or not value:
                continue
            if key.lower() == "phone" and not item.get("amo_field_id") and lead_phone_field_id:
                item = dict(item)
                item["amo_field_id"] = lead_phone_field_id
            extracted_map[key] = value
            if existing_map.get(key) == value:
                continue
            await crm_fields.upsert_field(
                int(tenant_id),
                int(lead_id),
                AMOCRM_PROVIDER,
                field_key=key,
                field_value=value,
                amo_field_id=item.get("amo_field_id"),
            )
            changed_fields.append(item)

    if not extracted_map:
        try:
            existing_fields = await crm_fields.list_fields(int(tenant_id), int(lead_id), AMOCRM_PROVIDER)
            existing_snapshot = {
                str(item.get("field_key")): str(item.get("field_value"))
                for item in existing_fields or []
                if isinstance(item, Mapping) and item.get("field_key") is not None
            }
            if existing_snapshot:
                extracted_map = dict(existing_snapshot)
                if not existing_map:
                    existing_map = dict(existing_snapshot)
        except Exception:
            pass

    phone_value = str(extracted_map.get("phone") or "").strip()
    phone_sent_marker = str(existing_map.get("phone_sent") or "").strip()

    link = await crm_links.get_link(int(tenant_id), int(lead_id), AMOCRM_PROVIDER)
    provider_lead_id = link.get("provider_lead_id") if isinstance(link, Mapping) else None
    provider_contact_id = link.get("provider_contact_id") if isinstance(link, Mapping) else None

    # If amo entities were manually removed in amoCRM UI, local links become stale.
    # Drop stale IDs before routing logic so the system can recreate a single canonical lead/contact.
    if is_inbound and isinstance(link, Mapping):
        stale_updates = False
        lead_exists = None
        contact_exists = None
        try:
            lead_exists = await _remote_entity_exists(
                client,
                entity_type="lead",
                entity_id=int(provider_lead_id) if provider_lead_id is not None else None,
            )
        except Exception:
            lead_exists = None
        if lead_exists is False and provider_lead_id is not None:
            await crm_links.update_provider_lead_id(
                int(tenant_id),
                int(lead_id),
                AMOCRM_PROVIDER,
                None,
            )
            provider_lead_id = None
            stale_updates = True
        try:
            contact_exists = await _remote_entity_exists(
                client,
                entity_type="contact",
                entity_id=int(provider_contact_id) if provider_contact_id is not None else None,
            )
        except Exception:
            contact_exists = None
        if contact_exists is False and provider_contact_id is not None:
            await crm_links.update_provider_contact_id(
                int(tenant_id),
                int(lead_id),
                AMOCRM_PROVIDER,
                None,
            )
            provider_contact_id = None
            stale_updates = True
        if stale_updates:
            link = await crm_links.get_link(int(tenant_id), int(lead_id), AMOCRM_PROVIDER)
            provider_lead_id = link.get("provider_lead_id") if isinstance(link, Mapping) else None

    if (not link or not provider_lead_id) and is_inbound and phone_value:
        resolved_contact_id, resolved_lead_id = await _resolve_existing_target_by_phone(client, phone_value)
        if resolved_lead_id:
            if not link:
                await crm_links.create_link(
                    int(tenant_id),
                    int(lead_id),
                    AMOCRM_PROVIDER,
                    pipeline_id=pipeline_id if pipeline_id else None,
                    stage_index=0,
                    inbound_count=0,
                )
            if resolved_contact_id:
                await crm_links.update_provider_contact_id(
                    int(tenant_id),
                    int(lead_id),
                    AMOCRM_PROVIDER,
                    int(resolved_contact_id),
                )
            await crm_links.update_provider_lead_id(
                int(tenant_id),
                int(lead_id),
                AMOCRM_PROVIDER,
                int(resolved_lead_id),
            )
            link = await crm_links.get_link(int(tenant_id), int(lead_id), AMOCRM_PROVIDER)
            provider_lead_id = link.get("provider_lead_id") if isinstance(link, Mapping) else None
    # If lead is already linked to an existing amo lead (e.g. via phone bridge),
    # pending create_lead must be cancelled to avoid creating a duplicate amo deal.
    if provider_lead_id:
        try:
            await crm_outbox.cancel_pending_events(
                int(tenant_id),
                AMOCRM_PROVIDER,
                int(lead_id),
                "create_lead",
                reason="provider_linked_before_create",
            )
        except Exception:
            logger.exception(
                "amocrm_cancel_pending_create_failed tenant=%s lead_id=%s",
                tenant_id,
                lead_id,
            )
    if not link or not provider_lead_id:
        stage0 = stages[0] if stages else None
        if not isinstance(stage0, Mapping):
            return
        target_stage_index = 0
        stage0_id = stage0.get("amo_stage_id")
        try:
            stage0_id_val = int(stage0_id)
        except Exception:
            return
        stage0_id_raw = stage0.get("amo_stage_id")
        if _is_unsorted_stage(stage0) or stage0_id_raw in (0, "0"):
            if len(stages) > 1 and isinstance(stages[1], Mapping):
                stage1_id = stages[1].get("amo_stage_id")
                try:
                    stage0_id_val = int(stage1_id)
                    target_stage_index = 1
                except Exception:
                    return
        if not link:
            await crm_links.create_link(
                int(tenant_id),
                int(lead_id),
                AMOCRM_PROVIDER,
                pipeline_id=pipeline_id,
                stage_index=target_stage_index,
                inbound_count=1 if is_inbound else 0,
            )
        else:
            if is_inbound:
                await crm_links.increment_inbound_count(
                    int(tenant_id), int(lead_id), AMOCRM_PROVIDER, pipeline_id=pipeline_id
                )
        custom_fields = _build_custom_fields(changed_fields)
        lead_name, resolved_contact_name = await _resolve_lead_names(
            lead_id=int(lead_id),
            extracted_fields=extracted_map,
        )
        contact_phone = extracted_map.get("phone")
        # For Avito, do not inherit an old local phone implicitly.
        # Only explicit phone from current dialog/rules should be used.
        if not contact_phone and channel_value != "avito":
            try:
                contact_phone = await db_module.get_contact_phone_by_lead(int(lead_id))
            except Exception:
                contact_phone = None
        contact_name = sanitize_display_name(extracted_map.get("name")) or resolved_contact_name or lead_name
        # Guard against duplicate create_lead when multiple inbound messages arrive
        # before the first create_lead event is consumed by the worker.
        already_create_queued = await crm_outbox.has_recent_event_type(
            int(tenant_id),
            AMOCRM_PROVIDER,
            int(lead_id),
            "create_lead",
            window_seconds=900,
            statuses=("pending", "processing"),
        )
        if already_create_queued:
            if last_text:
                try:
                    await crm_outbox.append_create_lead_bootstrap_message(
                        int(tenant_id),
                        AMOCRM_PROVIDER,
                        int(lead_id),
                        text=last_text,
                        direction=direction,
                    )
                except Exception:
                    logger.exception(
                        "amocrm_outbox_bootstrap_append_failed tenant=%s lead_id=%s",
                        tenant_id,
                        lead_id,
                    )
            logger.info(
                "amocrm_outbox_skip_duplicate tenant=%s lead_id=%s event=create_lead",
                tenant_id,
                lead_id,
            )
            return
        await crm_outbox.enqueue(
            int(tenant_id),
            AMOCRM_PROVIDER,
            int(lead_id),
            "create_lead",
            {
                "pipeline_id": pipeline_id,
                "stage_id": stage0_id_val,
                "lead_name": lead_name,
                "contact_phone": contact_phone,
                "contact_name": contact_name,
                "custom_fields": custom_fields,
                "stage_index": target_stage_index,
                "channel": str(channel or "").strip().lower(),
                "bootstrap_text": last_text,
                "bootstrap_direction": direction,
                "bootstrap_attachments": attachment_payloads,
            },
        )
        logger.info(
            "amocrm_outbox_enqueued tenant=%s lead_id=%s event=create_lead",
            tenant_id,
            lead_id,
        )
        if custom_fields:
            await crm_outbox.enqueue(
                int(tenant_id),
                AMOCRM_PROVIDER,
                int(lead_id),
                "update_fields",
                {"custom_fields": custom_fields},
            )
            logger.info(
                "amocrm_outbox_enqueued tenant=%s lead_id=%s event=update_fields",
                tenant_id,
                lead_id,
            )
        await crm_outbox.enqueue(
            int(tenant_id),
            AMOCRM_PROVIDER,
            int(lead_id),
            "move_stage",
            {
                "stage_id": stage0_id_val,
                "stage_index": target_stage_index,
                "pipeline_id": pipeline_id,
            },
        )
        logger.info(
            "amocrm_outbox_enqueued tenant=%s lead_id=%s event=move_stage",
            tenant_id,
            lead_id,
        )
        if not chat_mode:
            notes_cfg = amocrm_cfg.get("notes")
            note_text = ""
            if isinstance(notes_cfg, Mapping):
                notes_enabled = bool(notes_cfg.get("enabled", True))
                all_messages = bool(notes_cfg.get("all_messages", True))
            else:
                notes_enabled = True
                all_messages = True
            if all_messages:
                dir_label = "IN" if direction == "in" else "OUT"
                prefix = f"[{dir_label} {channel}]"
                body = (last_text or "").strip()
                if not body and attachment_count:
                    body = f"[attachments: {attachment_count}]"
                if body:
                    note_text = f"{prefix} {body}".strip()
            if notes_enabled:
                extra_note = _build_note_text(
                    template=str(notes_cfg.get("template") or "") if isinstance(notes_cfg, Mapping) else "",
                    channel=channel,
                    last_text=last_text,
                    fields=extracted_map,
                )
                if extra_note:
                    note_text = f"{note_text}\n\n{extra_note}".strip() if note_text else extra_note
            if attachment_payloads:
                is_dup = await crm_outbox.has_recent_event(
                    int(tenant_id),
                    AMOCRM_PROVIDER,
                    int(lead_id),
                    "add_files",
                    {"attachments": attachment_payloads},
                    window_seconds=120,
                )
                if not is_dup:
                    await crm_outbox.enqueue(
                        int(tenant_id),
                        AMOCRM_PROVIDER,
                        int(lead_id),
                        "add_files",
                        {"attachments": attachment_payloads},
                    )
                    logger.info(
                        "amocrm_outbox_enqueued tenant=%s lead_id=%s event=add_files",
                        tenant_id,
                        lead_id,
                    )
                else:
                    logger.info(
                        "amocrm_outbox_skip_duplicate tenant=%s lead_id=%s event=add_files",
                        tenant_id,
                        lead_id,
                    )
            if note_text:
                await crm_outbox.enqueue(
                    int(tenant_id),
                    AMOCRM_PROVIDER,
                    int(lead_id),
                    "add_note",
                    {"text": note_text},
                )
                logger.info(
                    "amocrm_outbox_enqueued tenant=%s lead_id=%s event=add_note",
                    tenant_id,
                    lead_id,
                )
        return

    inbound_count = 0
    current_stage_index = 0
    if isinstance(link, Mapping):
        try:
            inbound_count = int(link.get("inbound_count") or 0)
        except Exception:
            inbound_count = 0
        try:
            current_stage_index = int(link.get("stage_index") or 0)
        except Exception:
            current_stage_index = 0
    lead_name_current, resolved_contact_name = await _resolve_lead_names(
        lead_id=int(lead_id),
        extracted_fields=extracted_map,
    )
    contact_name_current = sanitize_display_name(extracted_map.get("name")) or resolved_contact_name
    if is_inbound:
        link = await crm_links.increment_inbound_count(
            int(tenant_id), int(lead_id), AMOCRM_PROVIDER, pipeline_id=pipeline_id
        )
        inbound_count = int(link.get("inbound_count") or 0) if link else 0
        current_stage_index = int(link.get("stage_index") or 0) if link else 0
    try:
        provider_lead_id_val = int(provider_lead_id) if provider_lead_id is not None else 0
    except Exception:
        provider_lead_id_val = 0
    if stage_eval_enabled and provider_lead_id_val > 0:
        try:
            lead_payload = await client.get_lead(provider_lead_id_val)
        except Exception:
            lead_payload = {}
        if isinstance(lead_payload, Mapping):
            try:
                remote_status_id = int(lead_payload.get("status_id") or 0)
            except Exception:
                remote_status_id = 0
            if remote_status_id > 0:
                remote_stage_index: int | None = None
                for idx, stage in enumerate(stages):
                    if not isinstance(stage, Mapping):
                        continue
                    try:
                        stage_id_val = int(stage.get("amo_stage_id") or 0)
                    except Exception:
                        continue
                    if stage_id_val == remote_status_id:
                        remote_stage_index = idx
                        break
                if remote_stage_index is not None and remote_stage_index != current_stage_index:
                    current_stage_index = remote_stage_index
                    await crm_links.update_stage_index(
                        int(tenant_id),
                        int(lead_id),
                        AMOCRM_PROVIDER,
                        current_stage_index,
                        pipeline_id=int(pipeline_id) if pipeline_id else None,
                    )
                    if isinstance(link, Mapping):
                        link = dict(link)
                        link["stage_index"] = current_stage_index
    if is_inbound and lead_name_current:
        await crm_outbox.enqueue(
            int(tenant_id),
            AMOCRM_PROVIDER,
            int(lead_id),
            "update_fields",
            {"lead_name": lead_name_current},
        )
    if is_inbound and contact_name_current:
        await crm_outbox.enqueue(
            int(tenant_id),
            AMOCRM_PROVIDER,
            int(lead_id),
            "update_contact_fields",
            {"contact_name": contact_name_current},
        )
    if is_inbound and phone_value and phone_value != phone_sent_marker:
        await crm_outbox.enqueue(
            int(tenant_id),
            AMOCRM_PROVIDER,
            int(lead_id),
            "update_contact_fields",
            {"custom_fields": [{"field_code": "PHONE", "values": [{"value": phone_value}]}]},
        )
        await crm_fields.upsert_field(
            int(tenant_id),
            int(lead_id),
            AMOCRM_PROVIDER,
            field_key="phone_sent",
            field_value=phone_value,
            amo_field_id=None,
        )
        logger.info(
            "amocrm_outbox_enqueued tenant=%s lead_id=%s event=update_fields",
            tenant_id,
            lead_id,
        )
    if is_inbound and changed_fields:
        custom_fields = _build_custom_fields(changed_fields)
        lead_custom_fields = [item for item in custom_fields if item.get("field_code") != "PHONE"]
        if lead_custom_fields:
            await crm_outbox.enqueue(
                int(tenant_id),
                AMOCRM_PROVIDER,
                int(lead_id),
                "update_fields",
                {"custom_fields": lead_custom_fields},
            )
            logger.info(
                "amocrm_outbox_enqueued tenant=%s lead_id=%s event=update_fields",
                tenant_id,
                lead_id,
            )
            contact_fields = [item for item in custom_fields if item.get("field_code") == "PHONE"]
            if contact_fields:
                await crm_outbox.enqueue(
                    int(tenant_id),
                    AMOCRM_PROVIDER,
                    int(lead_id),
                    "update_contact_fields",
                    {"custom_fields": contact_fields},
                )
                logger.info(
                    "amocrm_outbox_enqueued tenant=%s lead_id=%s event=update_contact_fields",
                    tenant_id,
                    lead_id,
                )
    if stage_eval_enabled:
        options = _normalize_rules_options(amocrm_cfg)
        stage_mode = str(options.get("stage_router_mode") or "auto").strip().lower()
        history_limit = _coerce_int(options.get("stage_router_history_limit"), 6, min_value=3, max_value=20)
        history_text = ""
        history_items: list[dict[str, str]] = []
        try:
            texts = await db_module.list_recent_stage_router_texts(
                int(tenant_id),
                int(lead_id),
                limit=history_limit,
            )
            if not texts:
                inbound_texts = await db_module.list_recent_inbound_texts(
                    int(tenant_id),
                    int(lead_id),
                    limit=history_limit,
                )
                history_items = [
                    {"role": "lead", "text": str(item or "").strip()}
                    for item in (inbound_texts or [])
                    if str(item or "").strip() and not _is_system_message_text(str(item or ""))
                ]
            else:
                history_items = _parse_stage_router_history_items(texts)
        except Exception:
            history_items = []
        if last_text and current_message_role != "system":
            current_item = {"role": current_message_role, "text": str(last_text or "").strip()}
            if current_item["text"]:
                if not history_items or history_items[-1] != current_item:
                    history_items.append(current_item)
        history_items = history_items[-history_limit:]
        history_text = _history_items_to_text(history_items)

        next_stage: int | None = None
        next_rule_type = ""
        decision_source = "none"
        decision_action = "NOOP"
        decision_reason = ""
        decision_confidence = 0.0
        decision_missing_fields: list[str] = []
        decision_evidence: list[str] = []
        supported_evidence: list[str] = []
        supported_stage_hints: list[str] = []
        selected_stage_check: dict[str, Any] | None = None
        llm_decision: dict[str, Any] | None = None
        if stage_mode in {"auto", "semi_auto"} and current_message_role != "system":
            max_jump = _coerce_int(options.get("stage_router_max_stage_jump"), 1, min_value=1, max_value=3)
            allowed_targets = list(
                range(
                    int(current_stage_index) + 1,
                    min(len(stages), int(current_stage_index) + max_jump + 1),
                )
            )
            llm_decision = await _decide_next_stage_llm(
                stages,
                current_stage_index=current_stage_index,
                inbound_count=inbound_count,
                last_text=last_text,
                history_text=history_text,
                message_role=current_message_role,
                history_items=history_items,
                extracted_fields=extracted_map,
                options=options,
            )
            if llm_decision:
                decision_source = "llm"
                decision_action = str(llm_decision.get("action") or "NOOP").strip().upper() or "NOOP"
                decision_reason = str(llm_decision.get("reason") or "").strip()
                decision_confidence = _coerce_float(
                    llm_decision.get("confidence"),
                    0.0,
                    min_value=0.0,
                    max_value=1.0,
                )
                decision_missing_fields = [
                    str(item or "").strip()
                    for item in (llm_decision.get("missing_fields") or [])
                    if str(item or "").strip()
                ]
                decision_evidence = [
                    str(item or "").strip()
                    for item in (llm_decision.get("evidence") or [])
                    if str(item or "").strip()
                ]
                supported_evidence = _supported_evidence(
                    decision_evidence,
                    last_text=last_text,
                    history_text=history_text,
                )
                raw_stage_checks = llm_decision.get("stage_checks")
                stage_checks = _parse_stage_checks(raw_stage_checks, allowed_targets=allowed_targets)
                if (
                    not stage_checks
                    and decision_action == "MOVE_STAGE"
                    and 0 <= _coerce_int(llm_decision.get("target_stage_index"), -1) < len(stages)
                ):
                    next_stage_candidate = _coerce_int(llm_decision.get("target_stage_index"), -1)
                    stage_checks = [
                        {
                            "target_stage_index": next_stage_candidate,
                            "ready": True,
                            "confidence": decision_confidence,
                            "reason": decision_reason,
                            "missing_fields": list(decision_missing_fields),
                            "evidence": [
                                {
                                    "quote": item,
                                    "source_role": current_message_role,
                                    "is_new": True,
                                }
                                for item in decision_evidence
                                if item
                            ],
                        }
                    ]
                checks_by_target: dict[int, dict[str, Any]] = {}
                for check in stage_checks:
                    if not isinstance(check, Mapping):
                        continue
                    target_idx = _coerce_int(check.get("target_stage_index"), -1)
                    if target_idx in allowed_targets:
                        checks_by_target[target_idx] = dict(check)
                threshold = _coerce_float(
                    options.get("stage_router_confidence_auto" if stage_mode == "auto" else "stage_router_confidence_semi"),
                    0.72 if stage_mode == "auto" else 0.45,
                    min_value=0.0,
                    max_value=1.0,
                )
                for target_idx in allowed_targets:
                    check = checks_by_target.get(target_idx)
                    if not isinstance(check, Mapping):
                        break
                    if not bool(check.get("ready")):
                        break
                    check_conf = _coerce_float(check.get("confidence"), 0.0, min_value=0.0, max_value=1.0)
                    if check_conf < threshold:
                        break
                    check_missing = [
                        str(item or "").strip()
                        for item in (check.get("missing_fields") or [])
                        if str(item or "").strip()
                    ]
                    if check_missing:
                        break
                    check_evidence_items = check.get("evidence")
                    evidence_quotes: list[str] = []
                    has_new_evidence = False
                    if isinstance(check_evidence_items, Sequence) and not isinstance(
                        check_evidence_items, (str, bytes)
                    ):
                        for item in check_evidence_items:
                            if not isinstance(item, Mapping):
                                continue
                            quote = str(item.get("quote") or "").strip()
                            if not quote:
                                continue
                            role = _normalize_stage_fact_role(item.get("source_role"))
                            if role == "system":
                                continue
                            evidence_quotes.append(quote)
                            if bool(item.get("is_new")) or (
                                last_text and quote.lower() in str(last_text).lower()
                            ):
                                has_new_evidence = True
                    check_supported = _supported_evidence(
                        evidence_quotes,
                        last_text=last_text,
                        history_text=history_text,
                    )
                    if not check_supported and not has_new_evidence:
                        break
                    next_stage = int(target_idx)
                    selected_stage_check = dict(check)
                    decision_confidence = check_conf
                    decision_reason = str(check.get("reason") or decision_reason or "").strip()
                    decision_missing_fields = check_missing
                    decision_evidence = list(evidence_quotes)
                    if check_supported:
                        supported_evidence = list(check_supported)
                if next_stage is None and decision_action in {"ASK_MANAGER", "NOOP"}:
                    next_stage = None
                if next_stage is not None:
                    decision_action = "MOVE_STAGE"
                elif decision_action != "ASK_MANAGER":
                    decision_action = "NOOP"
        if next_stage is not None and 0 <= next_stage < len(stages):
            candidate = stages[next_stage]
            if isinstance(candidate, Mapping):
                next_rule_type = str(candidate.get("type") or "").strip().lower()
                stage_hints = _flatten_stage_hints(candidate.get("hints"))
                if stage_hints:
                    supported_stage_hints = _supported_evidence(
                        stage_hints,
                        last_text=last_text,
                        history_text=history_text,
                    )
        logger.info(
            "amocrm_stage_eval tenant=%s lead_id=%s mode=%s source=%s action=%s current=%s inbound=%s next=%s conf=%.2f rule=%s reason=%s missing=%s evidence=%s supported_evidence=%s supported_hints=%s text_len=%s",
            tenant_id,
            lead_id,
            stage_mode,
            decision_source,
            decision_action,
            current_stage_index,
            inbound_count,
            next_stage,
            decision_confidence,
            next_rule_type,
            decision_reason,
            len(decision_missing_fields),
            len(decision_evidence),
            len(supported_evidence),
            len(supported_stage_hints),
            len(last_text or ""),
        )
        if next_stage is not None and stage_mode in {"auto", "semi_auto"}:
            stage_cfg = stages[next_stage] if next_stage < len(stages) else None
            guard_passed = True
            guard_reason = ""
            if isinstance(stage_cfg, Mapping):
                stage_id = stage_cfg.get("amo_stage_id")
                try:
                    stage_id_val = int(stage_id)
                except Exception:
                    stage_id_val = 0
                if stage_id_val:
                    now_ts = int(time.time())
                    cooldown_seconds = _coerce_int(
                        options.get("stage_router_cooldown_seconds"),
                        300,
                        min_value=0,
                        max_value=86400,
                    )
                    try:
                        last_move_ts = int(float(extracted_map.get("__stage_last_move_ts") or 0))
                    except Exception:
                        last_move_ts = 0
                    max_jump = _coerce_int(options.get("stage_router_max_stage_jump"), 1, min_value=1, max_value=3)
                    jump = max(0, int(next_stage) - int(current_stage_index))
                    if jump > max_jump:
                        guard_passed = False
                        guard_reason = f"guard_max_jump:{jump}>{max_jump}"
                    if cooldown_seconds > 0 and last_move_ts > 0 and (now_ts - last_move_ts) < cooldown_seconds:
                        guard_passed = False
                        guard_reason = "guard_cooldown_active"
                    if _is_terminal_stage(stage_cfg) and not bool(options.get("stage_router_allow_terminal_auto")):
                        guard_passed = False
                        guard_reason = "guard_terminal_denied"
                    threshold = _coerce_float(
                        options.get("stage_router_confidence_auto" if stage_mode == "auto" else "stage_router_confidence_semi"),
                        0.72 if stage_mode == "auto" else 0.45,
                        min_value=0.0,
                        max_value=1.0,
                    )
                    if decision_source == "llm" and decision_confidence < threshold:
                        guard_passed = False
                        guard_reason = f"guard_low_confidence:{decision_confidence:.2f}<{threshold:.2f}"
                    if decision_source == "llm" and decision_missing_fields:
                        guard_passed = False
                        guard_reason = "guard_missing_fields:" + ", ".join(decision_missing_fields[:4])
                    combined_signal_text = " ".join(
                        [
                            str(last_text or "").strip(),
                            " ".join(decision_evidence),
                        ]
                    ).strip()
                    structured_signal = _has_contact_signal(combined_signal_text) or _has_schedule_signal(
                        combined_signal_text
                    )
                    if decision_source == "llm" and not supported_evidence and not structured_signal:
                        guard_passed = False
                        guard_reason = "guard_no_supported_evidence"
                    if decision_source == "llm":
                        stage_hints = _flatten_stage_hints(stage_cfg.get("hints"))
                        if stage_hints:
                            if not supported_stage_hints and not structured_signal and not supported_evidence:
                                guard_passed = False
                                guard_reason = "guard_stage_hints_not_met"
                    if guard_reason:
                        decision_reason = guard_reason

                    if stage_mode == "auto" and guard_passed:
                        move_payload = {
                            "stage_id": stage_id_val,
                            "stage_index": next_stage,
                            "pipeline_id": pipeline_id,
                        }
                        dedup_window = _coerce_int(
                            options.get("stage_router_move_dedup_seconds"),
                            90,
                            min_value=0,
                            max_value=3600,
                        )
                        is_dup_move = False
                        if dedup_window > 0:
                            is_dup_move = await crm_outbox.has_recent_event(
                                int(tenant_id),
                                AMOCRM_PROVIDER,
                                int(lead_id),
                                "move_stage",
                                move_payload,
                                window_seconds=dedup_window,
                            )
                        if is_dup_move:
                            guard_passed = False
                            decision_reason = "guard_duplicate_move_stage"
                        elif int(next_stage) == int(current_stage_index):
                            guard_passed = False
                            decision_reason = "guard_already_in_stage"
                    if stage_mode == "auto" and guard_passed:
                        await crm_outbox.enqueue(
                            int(tenant_id),
                            AMOCRM_PROVIDER,
                            int(lead_id),
                            "move_stage",
                            {
                                "stage_id": stage_id_val,
                                "stage_index": next_stage,
                                "pipeline_id": pipeline_id,
                            },
                        )
                        logger.info(
                            "amocrm_outbox_enqueued tenant=%s lead_id=%s event=move_stage",
                            tenant_id,
                            lead_id,
                        )
                        await crm_fields.upsert_field(
                            int(tenant_id),
                            int(lead_id),
                            AMOCRM_PROVIDER,
                            field_key="__stage_last_move_ts",
                            field_value=str(now_ts),
                            amo_field_id=None,
                        )
                    elif (
                        stage_mode == "semi_auto"
                        and decision_source == "llm"
                        and decision_action == "MOVE_STAGE"
                        and guard_passed
                    ):
                        suggestion_sig = _stage_suggestion_signature(
                            int(next_stage),
                            decision_missing_fields,
                            supported_evidence,
                        )
                        prev_sig = str(extracted_map.get("__stage_last_suggestion_sig") or "").strip()
                        try:
                            prev_sig_ts = int(float(extracted_map.get("__stage_last_suggestion_ts") or 0))
                        except Exception:
                            prev_sig_ts = 0
                        if prev_sig and prev_sig == suggestion_sig and cooldown_seconds > 0 and (now_ts - prev_sig_ts) < cooldown_seconds:
                            suggestion_sig = ""
                        missing_part = ""
                        if decision_missing_fields:
                            missing_part = f" Не хватает: {', '.join(decision_missing_fields[:4])}."
                        suggestion_text = (
                            f"[AI stage suggestion] Текущая стадия #{current_stage_index + 1}, "
                            f"рекомендуется '{str(stage_cfg.get('name') or 'stage')}' "
                            f"(id={stage_id_val}, confidence={decision_confidence:.2f}). "
                            f"Причина: {decision_reason or '—'}"
                            f"{missing_part}"
                        ).strip()
                        is_dup = True if not suggestion_sig else await crm_outbox.has_recent_event(
                            int(tenant_id),
                            AMOCRM_PROVIDER,
                            int(lead_id),
                            "add_note",
                            {"text": suggestion_text},
                            window_seconds=max(60, cooldown_seconds or 60),
                        )
                        if not is_dup and suggestion_sig:
                            await crm_outbox.enqueue(
                                int(tenant_id),
                                AMOCRM_PROVIDER,
                                int(lead_id),
                                "add_note",
                                {"text": suggestion_text},
                            )
                            await crm_fields.upsert_field(
                                int(tenant_id),
                                int(lead_id),
                                AMOCRM_PROVIDER,
                                field_key="__stage_last_suggestion_sig",
                                field_value=suggestion_sig,
                                amo_field_id=None,
                            )
                            await crm_fields.upsert_field(
                                int(tenant_id),
                                int(lead_id),
                                AMOCRM_PROVIDER,
                                field_key="__stage_last_suggestion_ts",
                                field_value=str(now_ts),
                                amo_field_id=None,
                            )
    if chat_mode:
        return
    notes_cfg = amocrm_cfg.get("notes")
    note_text = ""
    if isinstance(notes_cfg, Mapping):
        notes_enabled = bool(notes_cfg.get("enabled", True))
        all_messages = bool(notes_cfg.get("all_messages", True))
    else:
        notes_enabled = True
        all_messages = True
    if all_messages:
        dir_label = "IN" if direction == "in" else "OUT"
        prefix = f"[{dir_label} {channel}]"
        body = (last_text or "").strip()
        if not body and attachment_count:
            body = f"[attachments: {attachment_count}]"
        if body:
            note_text = f"{prefix} {body}".strip()
    if notes_enabled:
        extra_note = _build_note_text(
            template=str(notes_cfg.get("template") or "") if isinstance(notes_cfg, Mapping) else "",
            channel=channel,
            last_text=last_text,
            fields=extracted_map,
        )
        if extra_note:
            note_text = f"{note_text}\n\n{extra_note}".strip() if note_text else extra_note
    if attachment_payloads:
        is_dup = await crm_outbox.has_recent_event(
            int(tenant_id),
            AMOCRM_PROVIDER,
            int(lead_id),
            "add_files",
            {"attachments": attachment_payloads},
            window_seconds=120,
        )
        if not is_dup:
            await crm_outbox.enqueue(
                int(tenant_id),
                AMOCRM_PROVIDER,
                int(lead_id),
                "add_files",
                {"attachments": attachment_payloads},
            )
            logger.info(
                "amocrm_outbox_enqueued tenant=%s lead_id=%s event=add_files",
                tenant_id,
                lead_id,
            )
        else:
            logger.info(
                "amocrm_outbox_skip_duplicate tenant=%s lead_id=%s event=add_files",
                tenant_id,
                lead_id,
            )
    if note_text:
        await crm_outbox.enqueue(
            int(tenant_id),
            AMOCRM_PROVIDER,
            int(lead_id),
            "add_note",
            {"text": note_text},
        )
        logger.info(
            "amocrm_outbox_enqueued tenant=%s lead_id=%s event=add_note",
            tenant_id,
            lead_id,
        )


__all__ = [
    "amocrm_on_inbound_message",
    "amocrm_on_outbound_message",
    "mask_amocrm_cfg",
    "resolve_base_url",
    "resolve_auth_url",
    "resolve_api_base_url",
    "get_amocrm_cfg",
    "resolve_oauth_cfg",
    "find_tenant_by_account",
    "ensure_pipeline_config",
    "ensure_lead_phone_field_id",
    "build_stages_from_statuses",
    "AMOCRM_PROVIDER",
]
