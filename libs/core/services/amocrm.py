from __future__ import annotations

import base64
import json
import logging
import os
import pathlib
import re
from typing import Any, Mapping, Sequence

from libs.core import db as db_module
from libs.core import sales_core as core_module
from libs.core.integrations import amocrm as amocrm_core
from libs.core.repo import amocrm_tokens, crm_fields, crm_links, crm_outbox

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


def _default_stage_rule(index: int) -> dict[str, Any]:
    if index == 0:
        return {"type": "on_first_inbound", "params": {}}
    if index == 1:
        return {"type": "on_inbound_count", "params": {"min_inbound_messages": 2}}
    if index == 2:
        return {"type": "on_inbound_count", "params": {"min_inbound_messages": 4}}
    return {"type": "manual_only", "params": {}}


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
        stages.append(
            {
                "name": stage_name,
                "amo_stage_id": stage_id_val,
                "type": stage_type,
                "rule": _default_stage_rule(index),
            }
        )
    return stages


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
    if pipeline_id > 0 and isinstance(stages, list) and stages:
        return pipeline_id, list(stages)

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

    statuses = _extract_embedded_list(pipeline, "statuses")
    if not statuses:
        try:
            pipeline_payload = await client.get_pipeline_stages(pipeline_id)
        except Exception:
            logger.exception(
                "amocrm_pipeline_stages_failed tenant=%s pipeline=%s",
                tenant_id,
                pipeline_id,
            )
            return None
        statuses = _extract_embedded_list(pipeline_payload, "statuses")
    stages = _build_default_stages(statuses)
    if not stages:
        return None

    updated_cfg = dict(cfg) if isinstance(cfg, Mapping) else {}
    integrations = updated_cfg.get("integrations")
    if not isinstance(integrations, dict):
        integrations = {}
    amocrm_cfg = dict(amocrm_cfg)
    amocrm_cfg["pipeline_id"] = pipeline_id
    amocrm_cfg["stages"] = stages
    integrations["amocrm"] = amocrm_cfg
    updated_cfg["integrations"] = integrations
    core_module.write_tenant_config(int(tenant_id), updated_cfg)
    return pipeline_id, stages

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
            key = str(item.get("key") or "").strip().lower()
            if key == "phone":
                value = str(item.get("value") or "").strip()
                if value:
                    custom_fields.append(
                        {
                            "field_code": "PHONE",
                            "values": [{"value": value}],
                        }
                    )
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


def build_stages_from_statuses(statuses: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return _build_default_stages(statuses)


def _normalize_rules_options(cfg: Mapping[str, Any]) -> dict[str, Any]:
    raw = cfg.get("rules_options") if isinstance(cfg, Mapping) else None
    options = dict(raw) if isinstance(raw, Mapping) else {}
    allow_multi_step = bool(options.get("allow_multi_step"))
    try:
        max_steps = int(options.get("max_steps_per_event") or 1)
    except Exception:
        max_steps = 1
    if max_steps <= 0:
        max_steps = 1
    return {"allow_multi_step": allow_multi_step, "max_steps_per_event": max_steps}


def _decide_next_stage_index(
    stages: Sequence[Mapping[str, Any]] | None,
    current_stage_index: int,
    inbound_count: int,
    last_text: str,
    extracted_fields: Mapping[str, Any],
    *,
    allow_multi_step: bool,
    max_steps_per_event: int,
) -> int | None:
    if not stages:
        return None
    try:
        current_index = int(current_stage_index)
    except Exception:
        current_index = 0
    steps_done = 0
    next_index: int | None = None
    while steps_done < max_steps_per_event:
        candidate = amocrm_core.decide_next_stage(
            stages,
            current_index,
            inbound_count,
            last_text,
            extracted_fields,
        )
        if candidate is None:
            break
        next_index = candidate
        current_index = candidate
        steps_done += 1
        if not allow_multi_step:
            break
    return next_index


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

async def _resolve_lead_names(
    *,
    lead_id: int,
    extracted_fields: Mapping[str, Any],
) -> tuple[str, str | None]:
    primary_name = str(extracted_fields.get("name") or "").strip()
    meta = await db_module.get_lead_dialog_metadata(int(lead_id))
    title = str((meta or {}).get("title") or "").strip()
    username = str((meta or {}).get("telegram_username") or "").strip()
    if username and not username.startswith("@"):
        username = f"@{username}"
    contact = str((meta or {}).get("contact") or "").strip()
    peer = str((meta or {}).get("peer") or "").strip()

    lead_name = ""
    for candidate in (username, primary_name, title, contact, peer):
        if candidate:
            lead_name = candidate
            break
    if not lead_name:
        lead_name = f"Avio lead {lead_id}"

    contact_name = ""
    for candidate in (username, primary_name, title):
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
) -> None:
    await _amocrm_on_message(
        tenant_id,
        lead_id,
        text=text,
        channel=channel,
        attachments=attachments,
        direction="in",
    )


async def amocrm_on_outbound_message(
    tenant_id: int,
    lead_id: int,
    *,
    text: str,
    channel: str,
    attachments: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    await _amocrm_on_message(
        tenant_id,
        lead_id,
        text=text,
        channel=channel,
        attachments=attachments,
        direction="out",
    )


async def _amocrm_on_message(
    tenant_id: int,
    lead_id: int,
    *,
    text: str,
    channel: str,
    attachments: Sequence[Mapping[str, Any]] | None = None,
    direction: str,
) -> None:
    cfg = core_module.read_tenant_config(int(tenant_id))
    amocrm_cfg = _amocrm_cfg(cfg)
    if not amocrm_cfg or not bool(amocrm_cfg.get("enabled")):
        return

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
    try:
        pipeline_id = int(pipeline_id_raw)
    except Exception:
        pipeline_id = 0
    stages = amocrm_cfg.get("stages")
    if pipeline_id <= 0 or not isinstance(stages, list) or not stages:
        ensured = await ensure_pipeline_config(int(tenant_id), cfg, client)
        if not ensured:
            return
        pipeline_id, stages = ensured
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
    is_inbound = direction == "in"
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

    phone_value = str(extracted_map.get("phone") or "").strip()
    phone_sent_marker = str(existing_map.get("phone_sent") or "").strip()

    link = await crm_links.get_link(int(tenant_id), int(lead_id), AMOCRM_PROVIDER)
    provider_lead_id = link.get("provider_lead_id") if isinstance(link, Mapping) else None
    if not link or not provider_lead_id:
        stage0 = stages[0] if stages else None
        if not isinstance(stage0, Mapping):
            return
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
                except Exception:
                    return
        if not link:
            await crm_links.create_link(
                int(tenant_id),
                int(lead_id),
                AMOCRM_PROVIDER,
                pipeline_id=pipeline_id,
                stage_index=0,
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
        if not contact_phone:
            try:
                contact_phone = await db_module.get_contact_phone_by_lead(int(lead_id))
            except Exception:
                contact_phone = None
        contact_name = extracted_map.get("name") or resolved_contact_name
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
                "stage_index": 0,
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
                "stage_index": 0,
                "pipeline_id": pipeline_id,
            },
        )
        logger.info(
            "amocrm_outbox_enqueued tenant=%s lead_id=%s event=move_stage",
            tenant_id,
            lead_id,
        )
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
    if is_inbound:
        link = await crm_links.increment_inbound_count(
            int(tenant_id), int(lead_id), AMOCRM_PROVIDER, pipeline_id=pipeline_id
        )
        inbound_count = int(link.get("inbound_count") or 0) if link else 0
        current_stage_index = int(link.get("stage_index") or 0) if link else 0
        if phone_value and phone_value != phone_sent_marker:
            await crm_outbox.enqueue(
                int(tenant_id),
                AMOCRM_PROVIDER,
                int(lead_id),
                "update_fields",
                {"custom_fields": [{"field_code": "PHONE", "values": [{"value": phone_value}]}]},
            )
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
    if is_inbound:
        options = _normalize_rules_options(amocrm_cfg)
        next_stage = _decide_next_stage_index(
            stages,
            current_stage_index,
            inbound_count,
            last_text,
            extracted_map,
            allow_multi_step=bool(options.get("allow_multi_step")),
            max_steps_per_event=int(options.get("max_steps_per_event") or 1),
        )
        next_rule_type = ""
        if next_stage is None:
            candidate_index = current_stage_index + 1
            if 0 <= candidate_index < len(stages):
                candidate = stages[candidate_index]
                if isinstance(candidate, Mapping):
                    rule = candidate.get("rule") if isinstance(candidate.get("rule"), Mapping) else {}
                    next_rule_type = str(rule.get("type") or "")
        else:
            if 0 <= next_stage < len(stages):
                candidate = stages[next_stage]
                if isinstance(candidate, Mapping):
                    rule = candidate.get("rule") if isinstance(candidate.get("rule"), Mapping) else {}
                    next_rule_type = str(rule.get("type") or "")
        logger.info(
            "amocrm_stage_eval tenant=%s lead_id=%s current=%s inbound=%s next=%s rule=%s text_len=%s",
            tenant_id,
            lead_id,
            current_stage_index,
            inbound_count,
            next_stage,
            next_rule_type,
            len(last_text or ""),
        )
        if next_stage is not None:
            stage_cfg = stages[next_stage] if next_stage < len(stages) else None
            if isinstance(stage_cfg, Mapping):
                stage_id = stage_cfg.get("amo_stage_id")
                try:
                    stage_id_val = int(stage_id)
                except Exception:
                    stage_id_val = 0
                if stage_id_val:
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
