"""Safe tenant config merge helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Callable, Mapping


AVITO_AUTH_KEYS = {
    "access_token",
    "refresh_token",
    "expires_at",
    "obtained_at",
    "account_id",
    "account_login",
    "scope",
}

BEHAVIOR_PRESERVE_KEYS = {
    "auto_reply",
    "auto_reply_enabled",
    "avito_smart_reply_enabled",
    "telegram_reply_enabled",
    "max_reply_enabled",
    "send_catalog_on_first_message",
    "send_catalog_on_first_message_max",
    "brain_mode",
}


def _deep_merge(base: dict[str, Any], patch: Mapping[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in patch.items():
        current = result.get(key)
        if isinstance(current, dict) and isinstance(value, Mapping):
            result[key] = _deep_merge(current, value)
        elif isinstance(current, dict) and not isinstance(value, Mapping):
            continue
        else:
            result[key] = deepcopy(value)
    return result


def merge_passport_settings_form(
    existing: Mapping[str, Any] | None,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    cfg = dict(existing or {})
    incoming = payload or {}

    passport = cfg.get("passport")
    if not isinstance(passport, dict):
        passport = {}
    else:
        passport = deepcopy(passport)
    cfg["passport"] = passport

    passport["brand"] = incoming.get("brand") or passport.get("brand", "")
    passport["agent_name"] = incoming.get("agent") or passport.get("agent_name", "")
    passport["currency"] = "₽"
    return cfg


def merge_tenant_config_for_settings(
    existing: Mapping[str, Any] | None,
    incoming: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Merge UI settings payload without dropping unrelated tenant sections.

    Settings screens often submit only the section they know about. A plain
    replacement can delete integrations, follow-ups, channel flags, or persona
    pointers from tenant.json. Avito auth fields are deliberately preserved here;
    OAuth callback and disconnect routes update them through dedicated flows.
    """

    existing_cfg = dict(existing or {})
    incoming_cfg = dict(incoming or {})
    merged = _deep_merge(existing_cfg, incoming_cfg)

    existing_behavior = existing_cfg.get("behavior")
    incoming_behavior = incoming_cfg.get("behavior")
    merged_behavior = merged.get("behavior")
    if (
        isinstance(existing_behavior, Mapping)
        and isinstance(incoming_behavior, Mapping)
        and isinstance(merged_behavior, dict)
    ):
        for key in BEHAVIOR_PRESERVE_KEYS:
            existing_value = existing_behavior.get(key)
            incoming_value = incoming_behavior.get(key)
            if existing_value not in (None, "") and incoming_value in (None, ""):
                merged_behavior[key] = deepcopy(existing_value)

    existing_integrations = existing_cfg.get("integrations")
    existing_avito = (
        ((existing_integrations or {}).get("avito") or {})
        if isinstance(existing_integrations, Mapping)
        else {}
    )
    merged_integrations = merged.get("integrations")
    if not isinstance(merged_integrations, dict):
        if existing_avito:
            merged_integrations = {}
            merged["integrations"] = merged_integrations
        else:
            return merged
    merged_avito = merged_integrations.get("avito")
    if not isinstance(merged_avito, dict):
        if existing_avito:
            merged_avito = {}
            merged_integrations["avito"] = merged_avito
        else:
            return merged

    if isinstance(existing_avito, Mapping):
        for key in AVITO_AUTH_KEYS:
            existing_value = existing_avito.get(key)
            incoming_value = merged_avito.get(key)
            if existing_value not in (None, "") and incoming_value in (None, ""):
                merged_avito[key] = deepcopy(existing_value)

    return merged


def build_public_settings_save_config(
    existing: Mapping[str, Any] | None,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build tenant config from the public settings save payload.

    Public settings can submit either a full ``cfg`` object or individual
    sections. Both paths must preserve unrelated tenant state.
    """

    existing_cfg = dict(existing or {})
    payload_map = payload or {}
    cfg_payload = payload_map.get("cfg")
    if isinstance(cfg_payload, Mapping):
        return merge_tenant_config_for_settings(existing_cfg, cfg_payload)

    cfg = dict(existing_cfg)
    for section in ("passport", "behavior", "cta", "limits", "integrations", "learning"):
        section_payload = payload_map.get(section)
        if isinstance(section_payload, Mapping):
            cfg = merge_tenant_config_for_settings(cfg, {section: section_payload})
    catalogs = payload_map.get("catalogs")
    if isinstance(catalogs, list):
        cfg["catalogs"] = deepcopy(catalogs)
    return cfg if isinstance(cfg, dict) else {}


def build_public_settings_get_config(
    cfg: Mapping[str, Any] | None,
    *,
    tenant_id: int,
    mask_amocrm_cfg: Callable[[Any, int], Mapping[str, Any] | None],
) -> Mapping[str, Any] | None:
    if not isinstance(cfg, Mapping):
        return cfg
    cfg_payload = dict(cfg)
    integrations = cfg_payload.get("integrations")
    if not isinstance(integrations, Mapping):
        return cfg_payload

    integrations_copy = dict(integrations)
    if "amocrm" in integrations_copy:
        masked = mask_amocrm_cfg(integrations_copy.get("amocrm"), int(tenant_id))
        integrations_copy["amocrm"] = dict(masked or {})
    cfg_payload["integrations"] = integrations_copy
    return cfg_payload
