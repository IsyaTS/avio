from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ClientSettingsDeps:
    authorize_client_settings_request_fn: AsyncFn
    resolve_key_fn: SyncFn
    auth_fn: SyncFn
    common_module: Any
    auth_utils_module: Any
    quickstart_module: Any
    render_template_fn: SyncFn
    merge_passport_settings_form_fn: SyncFn
    merge_behavior_settings_fn: SyncFn
    sanitize_behavior_triggers_fn: SyncFn
    export_max_days: int
    tg_slot_min: int
    tg_slot_max: int
    getenv_fn: SyncFn
    json_module: Any
    logger: Any


async def client_settings(
    tenant: int,
    request: Request,
    *,
    deps: ClientSettingsDeps,
) -> Response:
    raw_query_key = (request.query_params.get("k") or "").strip()
    raw_cookie_key = _cookie_key(request)
    client_key = raw_query_key or raw_cookie_key
    provided_key = deps.resolve_key_fn(request, client_key)

    session_user = await deps.auth_utils_module.get_current_user(request)
    session_allowed = bool(session_user and int(session_user.get("tenant_id") or 0) == int(tenant))
    debug_headers = _debug_headers(
        request,
        tenant=int(tenant),
        raw_query_key=raw_query_key,
        raw_cookie_key=raw_cookie_key,
        provided_key=provided_key,
        session_allowed=session_allowed,
        deps=deps,
    )
    if not session_allowed:
        if not deps.auth_utils_module.magic_link_enabled() or not deps.auth_fn(tenant, provided_key):
            return JSONResponse({"detail": "unauthorized"}, status_code=401, headers=debug_headers)

    tenant_key = (deps.common_module.get_tenant_pubkey(int(tenant)) or "").strip()
    key = _settings_key_for_response(
        tenant=int(tenant),
        requested_key=client_key,
        tenant_key=tenant_key,
        session_allowed=session_allowed,
        deps=deps,
    )

    deps.common_module.ensure_tenant_files(tenant)
    cfg = deps.common_module.read_tenant_config(tenant)
    if not isinstance(cfg, dict):
        cfg = {}

    context = _settings_context(
        tenant=int(tenant),
        key=key,
        tenant_key=tenant_key,
        request=request,
        cfg=cfg,
        deps=deps,
    )
    template_name = _settings_template(deps)
    response = deps.render_template_fn(template_name, context)
    response.headers["Cache-Control"] = "no-store"
    _set_client_key_cookie(response, request, key, deps=deps)
    return response


async def client_settings_short(
    request: Request,
    *,
    deps: ClientSettingsDeps,
) -> Response:
    session_user = await deps.auth_utils_module.get_current_user(request)
    if not session_user:
        return RedirectResponse(url="/login")
    tenant_id = int(session_user.get("tenant_id") or 0)
    if tenant_id <= 0:
        return JSONResponse({"detail": "invalid_tenant"}, status_code=400)
    return await client_settings(tenant_id, request, deps=deps)


async def save_form(
    tenant: int,
    request: Request,
    *,
    deps: ClientSettingsDeps,
) -> dict[str, bool] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    payload = await request.json()

    cfg = deps.common_module.read_tenant_config(tenant_id)
    if not isinstance(cfg, dict):
        cfg = {}

    cfg = deps.merge_passport_settings_form_fn(
        cfg,
        payload if isinstance(payload, Mapping) else {},
    )
    try:
        deps.quickstart_module.refresh_persona_headers(tenant_id, cfg)
    except Exception:
        deps.logger.warning("quickstart_refresh_failed tenant=%s", tenant_id)
    deps.common_module.write_tenant_config(tenant_id, cfg)
    return {"ok": True}


async def save_behavior(
    tenant: int,
    request: Request,
    *,
    deps: ClientSettingsDeps,
) -> dict[str, bool] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    payload = await request.json()
    cfg = deps.common_module.read_tenant_config(tenant_id)
    if not isinstance(cfg, dict):
        cfg = {}
    behavior = cfg.get("behavior")
    if not isinstance(behavior, dict):
        behavior = {}
    normalized_payload = dict(payload) if isinstance(payload, Mapping) else {}
    if "triggers" in normalized_payload:
        normalized_payload["triggers"] = deps.sanitize_behavior_triggers_fn(
            normalized_payload.get("triggers")
        )
    cfg["behavior"] = deps.merge_behavior_settings_fn(behavior, normalized_payload)
    deps.common_module.write_tenant_config(tenant_id, cfg)
    return {"ok": True}


async def get_follow_ups(
    tenant: int,
    request: Request,
    *,
    deps: ClientSettingsDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    cfg = deps.common_module.read_tenant_config(tenant_id)
    if not isinstance(cfg, dict):
        cfg = {}
    follow_up_rules = cfg.get("follow_up")
    if not isinstance(follow_up_rules, list):
        follow_up_rules = []
    return {"ok": True, "rules": follow_up_rules}


async def save_follow_ups(
    tenant: int,
    request: Request,
    *,
    deps: ClientSettingsDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    validated = validate_follow_up_rules(
        payload.get("rules") if isinstance(payload, Mapping) else [],
        tg_slot_raw=payload.get("tg_slot") if isinstance(payload, Mapping) else None,
        deps=deps,
    )
    cfg = deps.common_module.read_tenant_config(tenant_id)
    if not isinstance(cfg, dict):
        cfg = {}
    cfg["follow_up"] = validated
    deps.common_module.write_tenant_config(tenant_id, cfg)
    return {"ok": True, "rules_saved": len(validated)}


def validate_follow_up_rules(
    rules_raw: Any,
    *,
    tg_slot_raw: Any,
    deps: ClientSettingsDeps,
) -> list[dict[str, object]]:
    try:
        tg_slot = int(tg_slot_raw)
    except Exception:
        tg_slot = deps.tg_slot_min
    if tg_slot < deps.tg_slot_min or tg_slot > deps.tg_slot_max:
        tg_slot = deps.tg_slot_min

    if not isinstance(rules_raw, list):
        rules_raw = []
    validated: list[dict[str, object]] = []
    allowed_channels = {"telegram", "avito", "whatsapp", "max", "max_personal", "any", "*"}

    for rule in rules_raw:
        if not isinstance(rule, dict):
            continue
        item = _validate_follow_up_rule(rule, allowed_channels)
        if item is not None:
            validated.append(item)
    return validated


def _cookie_key(request: Request) -> str:
    try:
        return (request.cookies.get("client_key") or "").strip() if request.cookies else ""
    except Exception:
        return ""


def _debug_headers(
    request: Request,
    *,
    tenant: int,
    raw_query_key: str,
    raw_cookie_key: str,
    provided_key: str,
    session_allowed: bool,
    deps: ClientSettingsDeps,
) -> dict[str, str]:
    tenant_key_debug = (deps.common_module.get_tenant_pubkey(int(tenant)) or "").strip()
    cfg_debug = deps.common_module.read_tenant_config(int(tenant))
    cfg_passport_key_debug = ""
    if isinstance(cfg_debug, dict):
        passport_debug = cfg_debug.get("passport")
        if isinstance(passport_debug, dict):
            cfg_passport_key_debug = (passport_debug.get("public_key") or "").strip()
    return {
        "X-Client-Host": request.headers.get("x-forwarded-host") or request.headers.get("host") or "",
        "X-Client-Query-Key": "1" if raw_query_key else "0",
        "X-Client-Cookie-Key": "1" if raw_cookie_key else "0",
        "X-Client-Resolved-Key": (provided_key[:8] if provided_key else ""),
        "X-Client-Tenant-Key": (tenant_key_debug[:8] if tenant_key_debug else ""),
        "X-Client-Cfg-Key": (cfg_passport_key_debug[:8] if cfg_passport_key_debug else ""),
        "X-Client-Session": "1" if session_allowed else "0",
    }


def _settings_key_for_response(
    *,
    tenant: int,
    requested_key: str,
    tenant_key: str,
    session_allowed: bool,
    deps: ClientSettingsDeps,
) -> str:
    key = requested_key
    if session_allowed and ((not key) or (not deps.common_module.valid_key(int(tenant), key))):
        key = tenant_key
        if not key:
            keys = deps.common_module.list_keys(int(tenant))
            key = (keys[0].get("key") if keys else "") or ""
    return key


def _settings_context(
    *,
    tenant: int,
    key: str,
    tenant_key: str,
    request: Request,
    cfg: dict[str, Any],
    deps: ClientSettingsDeps,
) -> dict[str, Any]:
    persona = deps.common_module.read_persona(tenant)
    personas = {
        "telegram": deps.common_module.read_persona(tenant, "telegram"),
        "avito": deps.common_module.read_persona(tenant, "avito"),
        "max": deps.common_module.read_persona(tenant, "max"),
    }
    passport = cfg.get("passport", {}) if isinstance(cfg.get("passport"), dict) else {}
    behavior_cfg = cfg.get("behavior", {}) if isinstance(cfg.get("behavior"), dict) else {}
    integrations = cfg.get("integrations", {}) if isinstance(cfg.get("integrations"), dict) else {}
    uploaded_display = _uploaded_catalog_display(integrations.get("uploaded_catalog", {}))
    behavior_state = _behavior_state(behavior_cfg)
    urls = _settings_urls(tenant)
    webhook_secret = getattr(deps.common_module.settings, "WEBHOOK_SECRET", "")
    webhook_secret = (webhook_secret or "").strip()

    state = {
        "tenant": tenant,
        "key": key,
        "public_key": tenant_key,
        "primary_key": tenant_key,
        "urls": urls,
        "max_days": deps.export_max_days,
        "webhook_secret": webhook_secret,
        "behavior": behavior_state,
    }
    form_payload = {
        "brand": passport.get("brand", ""),
        "agent": passport.get("agent_name", ""),
        "catalog_file": uploaded_display,
    }
    state_payload = dict(state)
    state_payload["form"] = form_payload
    state_payload["behavior"] = behavior_state
    state_payload["personas"] = personas
    state_payload["quickstart_templates"] = deps.quickstart_module.list_quickstart_templates()
    client_state_json = deps.json_module.dumps(state_payload)
    asset_version_value = deps.common_module.asset_version()

    return {
        "request": request,
        "tenant": tenant,
        "tenant_id": tenant,
        "key": key,
        "public_key": tenant_key,
        "persona": persona,
        "personas": personas,
        "form": form_payload,
        "title": f"Настройки клиента · Tenant {tenant}",
        "subtitle": passport.get("brand") or "Личный кабинет клиента",
        "urls": urls,
        "state": state,
        "state_payload": state_payload,
        "client_state_json": client_state_json,
        "primary_key": tenant_key,
        "max_days": deps.export_max_days,
        "client_settings_version": deps.common_module.client_settings_version(),
        "webhook_secret": webhook_secret,
        "asset_version": asset_version_value,
        "behavior": behavior_state,
    }


def _uploaded_catalog_display(uploaded_meta: Any) -> str:
    if isinstance(uploaded_meta, str):
        return uploaded_meta
    if isinstance(uploaded_meta, dict):
        return uploaded_meta.get("original") or uploaded_meta.get("path") or ""
    return ""


def _behavior_state(behavior_cfg: Mapping[str, Any]) -> dict[str, Any]:
    triggers_raw = behavior_cfg.get("triggers") if isinstance(behavior_cfg.get("triggers"), list) else []
    brain_mode_raw = str(behavior_cfg.get("brain_mode") or "").strip().lower()
    return {
        "brain_mode": (
            "classic"
            if brain_mode_raw in {"classic", "prod", "legacy"}
            or bool(behavior_cfg.get("human_reply_mode"))
            else "smart"
        ),
        "auto_reply": bool(behavior_cfg.get("auto_reply")),
        "auto_reply_text": behavior_cfg.get("auto_reply_text") or "",
        "avito_phone_tg_template": behavior_cfg.get("avito_phone_tg_template") or "",
        "avito_smart_reply_enabled": bool(behavior_cfg.get("avito_smart_reply_enabled")),
        "send_catalog_on_first_message": behavior_cfg.get("send_catalog_on_first_message"),
        "send_catalog_on_first_message_max": behavior_cfg.get("send_catalog_on_first_message_max"),
        "telegram_reply_enabled": behavior_cfg.get("telegram_reply_enabled"),
        "max_reply_enabled": behavior_cfg.get("max_reply_enabled"),
        "auto_photo_enabled": bool(behavior_cfg.get("auto_photo_enabled")),
        "auto_photo_max": behavior_cfg.get("auto_photo_max") or 0,
        "triggers": triggers_raw,
        "photo_expected_markers": behavior_cfg.get("photo_expected_markers") or [],
        "photo_expected_reply": behavior_cfg.get("photo_expected_reply") or "",
        "photo_expected_ttl": behavior_cfg.get("photo_expected_ttl") or 0,
    }


def _settings_urls(tenant: int) -> dict[str, str]:
    return {
        "settings": f"/client/{tenant}/settings",
        "settings_get": "/pub/settings/get",
        "settings_save": "/pub/settings/save",
        "save_settings": f"/client/{tenant}/settings/save",
        "save_behavior": f"/client/{tenant}/behavior/save",
        "save_persona": f"/client/{tenant}/persona",
        "quickstart_templates": f"/client/{tenant}/quickstart/templates",
        "quickstart_apply": f"/client/{tenant}/quickstart/apply",
        "save_followups": f"/client/{tenant}/follow-ups",
        "get_followups": f"/client/{tenant}/follow-ups",
        "upload_catalog": "/pub/catalog/upload",
        "csv_get": "/pub/catalog/csv",
        "csv_save": "/pub/catalog/csv",
        "photos_list": "/pub/files/photos/list",
        "photos_upload": "/pub/files/photos/upload",
        "photos_delete": "/pub/files/photos/{photo_id}",
        "photos_file": "/pub/files/photos/{photo_id}",
        "photos_meta": "/pub/files/photos/{photo_id}/meta",
        "avito_history_probe": f"/client/{tenant}/avito/history/probe",
        "avito_history_probe_status": f"/client/{tenant}/avito/history/probe/{{job_id}}",
        "avito_history_export": f"/client/{tenant}/avito/history/export",
        "avito_history_export_status": f"/client/{tenant}/avito/history/export/{{job_id}}",
        "avito_history_export_download": f"/client/{tenant}/avito/history/export/{{job_id}}/download",
        "avito_history_export_dialog_dataset_download": f"/client/{tenant}/avito/history/export/{{job_id}}/dialog-dataset/download",
        "avito_history_export_export_summary_download": f"/client/{tenant}/avito/history/export/{{job_id}}/export-summary/download",
        "avito_history_export_contextual_download": f"/client/{tenant}/avito/history/export/{{job_id}}/contextual/download",
        "avito_history_export_review_cases_download": f"/client/{tenant}/avito/history/export/{{job_id}}/review-cases/download",
        "avito_history_export_rejected_summary_download": f"/client/{tenant}/avito/history/export/{{job_id}}/rejected-summary/download",
        "avito_history_export_domain_schema_download": f"/client/{tenant}/avito/history/export/{{job_id}}/domain-schema/download",
        "avito_history_export_business_rules_draft_download": f"/client/{tenant}/avito/history/export/{{job_id}}/business-rules-draft/download",
        "avito_history_export_active": f"/client/{tenant}/avito/history/export/active",
        "avito_history_export_latest": f"/client/{tenant}/avito/history/export/latest",
        "avito_history_export_files": f"/client/{tenant}/avito/history/export/files",
        "avito_history_export_delete": f"/client/{tenant}/avito/history/export/{{job_id}}",
        "avito_history_export_cancel": f"/client/{tenant}/avito/history/export/{{job_id}}/cancel",
        "avito_history_export_activate_dataset": f"/client/{tenant}/avito/history/export/{{job_id}}/activate-dataset",
        "avito_history_export_deactivate_dataset": f"/client/{tenant}/avito/history/export/{{job_id}}/deactivate-dataset",
        "avito_oauth_accounts": "/v1/oauth/avito/accounts",
        "avito_oauth_account_primary": "/v1/oauth/avito/accounts/{account_id}/primary",
        "avito_oauth_account_rename": "/v1/oauth/avito/accounts/{account_id}/rename",
        "avito_oauth_account_disconnect": "/v1/oauth/avito/accounts/{account_id}/disconnect",
        "avito_oauth_account_webhook": "/v1/oauth/avito/accounts/{account_id}/webhook",
        "contextual_cases_import": f"/client/{tenant}/contextual-cases/import/{{job_id}}",
        "contextual_cases_status": f"/client/{tenant}/contextual-cases/status",
        "contextual_cases_settings": f"/client/{tenant}/contextual-cases/settings",
        "dialogs_list": "/api/dialogs",
        "dialogs_detail": "/api/dialogs/{lead_id}",
        "dialogs_send": "/api/dialogs/{lead_id}/send",
        "dialogs_unsilence": "/api/dialogs/{lead_id}/unsilence",
        "dialogs_test": "/api/dialogs/test",
        "tenant_stats": "/api/tenant/stats",
        "analytics_summary": "/api/analytics/summary",
        "feedback_stats": "/api/feedback/stats",
        "feedback_quality": "/api/feedback/quality",
        "feedback": "/api/feedback",
        "training_tg_harvest": f"/client/{tenant}/training/telegram/harvest",
        "training_tg_accept": f"/client/{tenant}/training/telegram/accept",
    }


def _settings_template(deps: ClientSettingsDeps) -> str:
    use_legacy_settings = (deps.getenv_fn("TESTING") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return "client/settings.html" if use_legacy_settings else "client/spa.html"


def _set_client_key_cookie(
    response: Response,
    request: Request,
    key: str,
    *,
    deps: ClientSettingsDeps,
) -> None:
    if not key:
        return
    try:
        response.set_cookie(
            "client_key",
            key,
            **deps.auth_utils_module.cookie_params(
                request,
                ttl_seconds=14 * 24 * 3600,
                httponly=True,
            ),
        )
    except Exception:
        pass


def _validate_follow_up_rule(
    rule: Mapping[str, Any],
    allowed_channels: set[str],
) -> dict[str, object] | None:
    channel = str(rule.get("channel") or "").strip().lower() or "any"
    if channel not in allowed_channels:
        channel = "any"
    try:
        delay_minutes = int(rule.get("delay_minutes") or 0)
    except Exception:
        delay_minutes = 0
    text_value = str(rule.get("text") or "").strip()
    if not text_value:
        return None
    try:
        max_attempts = int(rule.get("max_attempts") or 1)
    except Exception:
        max_attempts = 1
    if max_attempts < 0:
        max_attempts = 0
    trigger_on_answer = bool(rule.get("trigger_on_answer"))
    if delay_minutes <= 0 and not trigger_on_answer:
        return None
    condition = rule.get("condition")
    if not isinstance(condition, (dict, list)):
        condition = None
    capture = rule.get("capture")
    if not isinstance(capture, dict):
        capture = None
    return {
        "channel": channel,
        "delay_minutes": delay_minutes if delay_minutes > 0 else 0,
        "text": text_value,
        "max_attempts": max_attempts,
        "active": bool(rule.get("active", True)),
        "trigger_on_answer": trigger_on_answer,
        "condition": condition,
        "capture": capture,
        "stop_notice_after": bool(rule.get("stop_notice_after")),
    }
