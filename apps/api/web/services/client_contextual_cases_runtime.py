from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from libs.core.services.contextual_case_import import ContextualCaseImportError


AsyncFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class ClientContextualCasesDeps:
    authorize_client_settings_request_fn: AsyncFn
    common_module: Any
    export_repo_module: Any
    contextual_repo_module: Any
    import_service_module: Any
    logger: Any


def build_default_deps(authorize_client_settings_request_fn: AsyncFn, common_module: Any, logger: Any) -> ClientContextualCasesDeps:
    from libs.core.repo import avito_history_exports, contextual_cases
    from libs.core.services import contextual_case_import

    return ClientContextualCasesDeps(
        authorize_client_settings_request_fn=authorize_client_settings_request_fn,
        common_module=common_module,
        export_repo_module=avito_history_exports,
        contextual_repo_module=contextual_cases,
        import_service_module=contextual_case_import,
        logger=logger,
    )


def register_routes(router: Any, authorize_client_settings_request_fn: AsyncFn, common_module: Any, logger: Any) -> None:
    deps_builder = lambda: build_default_deps(authorize_client_settings_request_fn, common_module, logger)

    async def _import(tenant: int, job_id: str, request: Request):
        return await import_contextual_cases(tenant, job_id, request, deps=deps_builder())

    async def _status(tenant: int, request: Request):
        return await contextual_cases_status(tenant, request, deps=deps_builder())

    async def _settings(tenant: int, request: Request):
        return await save_contextual_cases_settings(tenant, request, deps=deps_builder())

    router.add_api_route(
        "/client/{tenant}/contextual-cases/import/{job_id}",
        _import,
        methods=["POST"],
        name="contextual_cases_import",
    )
    router.add_api_route(
        "/client/{tenant}/contextual-cases/status",
        _status,
        methods=["GET"],
        name="contextual_cases_status",
    )
    router.add_api_route(
        "/client/{tenant}/contextual-cases/settings",
        _settings,
        methods=["POST"],
        name="contextual_cases_settings",
    )


async def import_contextual_cases(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientContextualCasesDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    try:
        result = await deps.import_service_module.import_from_export_job(
            tenant_id=int(tenant_id),
            job_id=str(job_id),
            export_repo=deps.export_repo_module,
            contextual_repo=deps.contextual_repo_module,
        )
    except ContextualCaseImportError as exc:
        return JSONResponse({"detail": exc.error_code}, status_code=_status_for_error(exc.error_code))
    except Exception:
        deps.logger.exception("contextual_cases_import_failed tenant=%s job=%s", tenant_id, job_id)
        return JSONResponse({"detail": "import_failed"}, status_code=500)
    _update_contextual_settings(
        int(tenant_id),
        deps=deps,
        enabled=True,
        shadow_mode=True,
        apply_mode=False,
    )
    return {
        "ok": True,
        "set_id": result.set_id,
        "imported_count": result.imported_count,
        "active_cases_count": result.active_cases_count,
        "domain_label": result.domain_label,
    }


async def contextual_cases_status(
    tenant: int,
    request: Request,
    *,
    deps: ClientContextualCasesDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    row = await deps.contextual_repo_module.get_latest_active_case_set(int(tenant_id))
    settings = _contextual_settings(int(tenant_id), deps=deps)
    if not row:
        return {"ok": True, "status": {"active_set_id": None, **settings}}
    schema = row.get("domain_schema") if isinstance(row.get("domain_schema"), Mapping) else {}
    return {
        "ok": True,
        "status": {
            "active_set_id": row.get("set_id"),
            "domain_label": schema.get("domain_label") or row.get("domain_label"),
            "cases_count": int(row.get("cases_count") or 0),
            "active_cases_count": int(row.get("active_cases_count") or 0),
            "embedding_ready_count": int(row.get("embedding_ready_count") or 0),
            "embedding_pending_count": int(row.get("embedding_pending_count") or 0),
            **settings,
        },
    }


async def save_contextual_cases_settings(
    tenant: int,
    request: Request,
    *,
    deps: ClientContextualCasesDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    data = payload if isinstance(payload, Mapping) else {}
    settings = _update_contextual_settings(
        int(tenant_id),
        deps=deps,
        enabled=bool(data.get("enabled")),
        shadow_mode=bool(data.get("shadow_mode", not bool(data.get("apply_mode")))),
        apply_mode=bool(data.get("apply_mode")),
    )
    return {"ok": True, "settings": settings}


def _contextual_settings(tenant_id: int, *, deps: ClientContextualCasesDeps) -> dict[str, Any]:
    cfg = deps.common_module.read_tenant_config(int(tenant_id))
    learning = cfg.get("learning") if isinstance(cfg, Mapping) else {}
    contextual = learning.get("contextual_cases") if isinstance(learning, Mapping) else {}
    if not isinstance(contextual, Mapping):
        contextual = {}
    return {
        "enabled": bool(contextual.get("enabled")),
        "shadow_mode": bool(contextual.get("shadow_mode", True)),
        "apply_mode": bool(contextual.get("apply_mode", False)),
        "top_k": int(contextual.get("top_k") or 3),
        "min_score": float(contextual.get("min_score") or 0.62),
    }


def _update_contextual_settings(
    tenant_id: int,
    *,
    deps: ClientContextualCasesDeps,
    enabled: bool,
    shadow_mode: bool,
    apply_mode: bool,
) -> dict[str, Any]:
    cfg = deps.common_module.read_tenant_config(int(tenant_id))
    if not isinstance(cfg, dict):
        cfg = {}
    learning = cfg.get("learning")
    if not isinstance(learning, dict):
        learning = {}
    existing = learning.get("contextual_cases")
    contextual = dict(existing) if isinstance(existing, Mapping) else {}
    contextual.update(
        {
            "enabled": bool(enabled),
            "shadow_mode": bool(shadow_mode),
            "apply_mode": bool(apply_mode),
            "top_k": int(contextual.get("top_k") or 3),
            "min_score": float(contextual.get("min_score") or 0.62),
            "max_prompt_chars": int(contextual.get("max_prompt_chars") or 3500),
        }
    )
    learning["contextual_cases"] = contextual
    cfg["learning"] = learning
    deps.common_module.write_tenant_config(int(tenant_id), cfg)
    return _contextual_settings(int(tenant_id), deps=deps)


def _status_for_error(error_code: str) -> int:
    if error_code in {"export_not_found", "contextual_file_not_found", "domain_schema_not_found"}:
        return 404
    if error_code == "export_not_ready":
        return 409
    if error_code in {"invalid_json", "invalid_jsonl"}:
        return 400
    return 500
