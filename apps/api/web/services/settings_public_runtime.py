from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import Request
from fastapi.responses import JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class PublicSettingsDeps:
    authorize_public_settings_request_fn: AsyncFn
    common_module: Any
    build_get_config_fn: SyncFn
    build_save_config_fn: SyncFn
    amocrm_service_module: Any
    amocrm_tokens_module: Any
    datetime_cls: Any
    timezone_utc: Any
    logger: Any
    no_store_headers_fn: SyncFn


async def settings_get(
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: PublicSettingsDeps,
) -> Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth

    tenant_id, _ = auth
    deps.common_module.ensure_tenant_files(tenant_id)
    cfg = deps.common_module.read_tenant_config(tenant_id)
    persona = deps.common_module.read_persona(tenant_id)
    personas = {
        "telegram": deps.common_module.read_persona(tenant_id, "telegram"),
        "avito": deps.common_module.read_persona(tenant_id, "avito"),
    }
    cfg_payload = deps.build_get_config_fn(
        cfg,
        tenant_id=int(tenant_id),
        mask_amocrm_cfg=lambda value, tid: deps.amocrm_service_module.mask_amocrm_cfg(
            value,
            tenant_id=tid,
        ),
    )
    payload = {"ok": True, "cfg": cfg_payload, "persona": persona, "personas": personas}
    return JSONResponse(payload, headers=deps.no_store_headers_fn())


async def settings_save(
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: PublicSettingsDeps,
) -> dict[str, bool] | Response:
    auth = await deps.authorize_public_settings_request_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth

    tenant_id, _ = auth
    deps.common_module.ensure_tenant_files(tenant_id)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    existing_cfg = deps.common_module.read_tenant_config(tenant_id)
    if not isinstance(existing_cfg, dict):
        existing_cfg = {}
    cfg = deps.build_save_config_fn(existing_cfg, payload)
    if not isinstance(cfg, dict):
        cfg = {}
    integrations = cfg.get("integrations")
    if isinstance(integrations, dict) and "amocrm" in integrations:
        amocrm_cfg = integrations.get("amocrm")
        if isinstance(amocrm_cfg, dict):
            _preserve_amocrm_secret(amocrm_cfg, existing_cfg, deps=deps)
            await _persist_manual_amocrm_token(int(tenant_id), amocrm_cfg, deps=deps)
            amocrm_cfg.pop("tokens", None)
            _merge_amocrm_pipeline_stages(amocrm_cfg, deps=deps)
    deps.common_module.write_tenant_config(tenant_id, cfg)
    if isinstance(payload.get("persona"), str):
        deps.common_module.write_persona(tenant_id, payload.get("persona") or "")
    personas_payload = payload.get("personas")
    if isinstance(personas_payload, Mapping):
        for channel_key, text in personas_payload.items():
            if channel_key not in {"telegram", "avito"}:
                continue
            if not isinstance(text, str):
                continue
            deps.common_module.write_persona(tenant_id, text, channel=channel_key)
    return {"ok": True}


def _preserve_amocrm_secret(
    amocrm_cfg: dict[str, Any],
    existing_cfg: Mapping[str, Any],
    *,
    deps: PublicSettingsDeps,
) -> None:
    existing_amocrm = deps.amocrm_service_module.get_amocrm_cfg(existing_cfg) or {}
    oauth_cfg = amocrm_cfg.get("oauth")
    if not isinstance(oauth_cfg, dict):
        return
    secret = str(oauth_cfg.get("client_secret") or "").strip()
    if secret and secret != "***":
        return
    existing_secret = ""
    if isinstance(existing_amocrm, dict):
        existing_oauth = existing_amocrm.get("oauth")
        if isinstance(existing_oauth, Mapping):
            existing_secret = str(existing_oauth.get("client_secret") or "").strip()
    if existing_secret:
        oauth_cfg["client_secret"] = existing_secret


async def _persist_manual_amocrm_token(
    tenant_id: int,
    amocrm_cfg: dict[str, Any],
    *,
    deps: PublicSettingsDeps,
) -> None:
    manual_cfg = amocrm_cfg.get("manual")
    if not isinstance(manual_cfg, dict):
        return
    manual_token = str(manual_cfg.get("access_token") or "").strip()
    if manual_token and manual_token != "***":
        try:
            await deps.amocrm_tokens_module.ensure_schema()
            await deps.amocrm_tokens_module.upsert(
                int(tenant_id),
                access_token=manual_token,
                refresh_token=None,
                expires_at=None,
                obtained_at=deps.datetime_cls.now(tz=deps.timezone_utc),
                raw_payload={"mode": "manual"},
            )
        except Exception:
            deps.logger.exception("amocrm_manual_token_store_failed tenant=%s", tenant_id)
    manual_cfg.pop("access_token", None)


def _merge_amocrm_pipeline_stages(
    amocrm_cfg: dict[str, Any],
    *,
    deps: PublicSettingsDeps,
) -> None:
    pipeline_id_val = deps.amocrm_service_module._coerce_pipeline_id(
        amocrm_cfg.get("pipeline_id")
    )
    merged_stages = deps.amocrm_service_module._merge_stages_for_pipeline(
        amocrm_cfg.get("stages"),
        amocrm_cfg,
        pipeline_id_val,
    )
    if not merged_stages:
        return
    amocrm_cfg["stages"] = merged_stages
    stages_by_pipeline = amocrm_cfg.get("stages_by_pipeline")
    if isinstance(stages_by_pipeline, dict) and pipeline_id_val > 0:
        entry = stages_by_pipeline.get(str(pipeline_id_val))
        if isinstance(entry, dict):
            entry = dict(entry)
            entry["stages"] = merged_stages
            stages_by_pipeline[str(pipeline_id_val)] = entry
            amocrm_cfg["stages_by_pipeline"] = stages_by_pipeline
