from __future__ import annotations

import asyncio
import os
import pathlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from fastapi import BackgroundTasks, Request
from fastapi.responses import FileResponse, JSONResponse, Response

from libs.core.services.avito_history_export import AvitoHistoryExportError
from libs.core.training import dialog_retriever


AsyncFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class ClientAvitoHistoryExportDeps:
    authorize_client_settings_request_fn: AsyncFn
    export_service_module: Any
    export_repo_module: Any
    common_module: Any
    avito_module: Any
    avito_api_module: Any
    logger: Any
    uuid_module: Any = uuid
    task_scheduler: Callable[[Awaitable[Any]], Any] | None = None
    export_root: str | None = None


def build_default_deps(
    authorize_client_settings_request_fn: AsyncFn,
    common_module: Any,
    logger: Any,
) -> ClientAvitoHistoryExportDeps:
    from libs.core.integrations import avito as avito_integration
    from libs.core.integrations import avito_analytics as avito_analytics_client
    from libs.core.repo import avito_history_exports
    from libs.core.services import avito_history_export

    return ClientAvitoHistoryExportDeps(
        authorize_client_settings_request_fn=authorize_client_settings_request_fn,
        export_service_module=avito_history_export,
        export_repo_module=avito_history_exports,
        common_module=common_module,
        avito_module=avito_integration,
        avito_api_module=avito_analytics_client,
        logger=logger,
    )


def register_routes(
    router: Any,
    authorize_client_settings_request_fn: AsyncFn,
    common_module: Any,
    logger: Any,
) -> None:
    def deps_builder() -> ClientAvitoHistoryExportDeps:
        return build_default_deps(
            authorize_client_settings_request_fn,
            common_module,
            logger,
        )

    _register_start_route(router, deps_builder)
    _register_read_routes(router, deps_builder)
    _register_file_routes(router, deps_builder)


def _register_start_route(router: Any, deps_builder: Callable[[], ClientAvitoHistoryExportDeps]) -> None:
    async def _start(tenant: int, request: Request, background_tasks: BackgroundTasks):
        return await start_export(
            tenant,
            request,
            background_tasks=background_tasks,
            deps=deps_builder(),
        )

    router.add_api_route(
        "/client/{tenant}/avito/history/export",
        _start,
        methods=["POST"],
        name="avito_history_export_start",
    )


def _register_read_routes(router: Any, deps_builder: Callable[[], ClientAvitoHistoryExportDeps]) -> None:
    async def _active(tenant: int, request: Request):
        return await get_active_export(
            tenant,
            request,
            deps=deps_builder(),
        )

    async def _latest(tenant: int, request: Request):
        return await get_latest_export(
            tenant,
            request,
            deps=deps_builder(),
        )

    async def _files(tenant: int, request: Request):
        return await list_exports(
            tenant,
            request,
            deps=deps_builder(),
        )

    async def _get(tenant: int, job_id: str, request: Request):
        return await get_export(
            tenant,
            job_id,
            request,
            deps=deps_builder(),
        )

    router.add_api_route(
        "/client/{tenant}/avito/history/export/active",
        _active,
        methods=["GET"],
        name="avito_history_export_active",
    )
    router.add_api_route(
        "/client/{tenant}/avito/history/export/latest",
        _latest,
        methods=["GET"],
        name="avito_history_export_latest",
    )
    router.add_api_route(
        "/client/{tenant}/avito/history/export/files",
        _files,
        methods=["GET"],
        name="avito_history_export_files",
    )
    router.add_api_route(
        "/client/{tenant}/avito/history/export/{job_id}",
        _get,
        methods=["GET"],
        name="avito_history_export_status",
    )


def _register_file_routes(router: Any, deps_builder: Callable[[], ClientAvitoHistoryExportDeps]) -> None:
    for path, methods, name, handler in _export_file_routes():
        router.add_api_route(
            path,
            _wrap_job_route(handler, deps_builder),
            methods=methods,
            name=name,
        )


def _export_file_routes() -> tuple[tuple[str, list[str], str, AsyncFn], ...]:
    base = "/client/{tenant}/avito/history/export/{job_id}"
    return (
        (f"{base}/cancel", ["POST"], "avito_history_export_cancel", cancel_export),
        (f"{base}/activate-dataset", ["POST"], "avito_history_export_activate_dataset", activate_dialog_dataset_export),
        (f"{base}/deactivate-dataset", ["POST"], "avito_history_export_deactivate_dataset", deactivate_dialog_dataset_export),
        (f"{base}/download", ["GET"], "avito_history_export_download", download_export),
        (
            f"{base}/dialog-dataset/download",
            ["GET"],
            "avito_history_export_dialog_dataset_download",
            download_dialog_dataset_export,
        ),
        (
            f"{base}/export-summary/download",
            ["GET"],
            "avito_history_export_export_summary_download",
            download_export_summary,
        ),
        (f"{base}/training/download", ["GET"], "avito_history_export_training_download", download_training_export),
        (f"{base}/contextual/download", ["GET"], "avito_history_export_contextual_download", download_contextual_export),
        (
            f"{base}/review-cases/download",
            ["GET"],
            "avito_history_export_review_cases_download",
            download_review_cases_export,
        ),
        (
            f"{base}/rejected-summary/download",
            ["GET"],
            "avito_history_export_rejected_summary_download",
            download_rejected_summary_export,
        ),
        (
            f"{base}/domain-schema/download",
            ["GET"],
            "avito_history_export_domain_schema_download",
            download_domain_schema_export,
        ),
        (
            f"{base}/business-rules-draft/download",
            ["GET"],
            "avito_history_export_business_rules_draft_download",
            download_business_rules_draft_export,
        ),
        (f"{base}/review/download", ["GET"], "avito_history_export_review_download", download_review_export),
        (f"{base}/summary/download", ["GET"], "avito_history_export_summary_download", download_summary_export),
        (base, ["DELETE"], "avito_history_export_delete", delete_export),
    )


def _wrap_job_route(handler: AsyncFn, deps_builder: Callable[[], ClientAvitoHistoryExportDeps]) -> AsyncFn:
    async def _wrapped(tenant: int, job_id: str, request: Request):
        return await handler(tenant, job_id, request, deps=deps_builder())

    return _wrapped


async def start_export(
    tenant: int,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
    background_tasks: BackgroundTasks | None = None,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    parsed = _parse_export_payload(payload)
    if isinstance(parsed, Response):
        return parsed

    target_dialogs, quality_review, account_selection = parsed
    active = await _get_active_job(int(tenant_id), deps=deps)
    if active:
        return {"ok": True, "job": _public_job(active, fallback_job_id=str(active.get("job_id") or ""), deps=deps)}

    job_id = deps.uuid_module.uuid4().hex
    row = await deps.export_repo_module.create_job(
        job_id=job_id,
        tenant_id=int(tenant_id),
        target_dialogs=target_dialogs,
    )
    if background_tasks is not None:
        _schedule_export_job(
            _run_export_job(
                int(tenant_id),
                job_id,
                target_dialogs,
                quality_review,
                account_selection,
                deps,
            ),
            deps=deps,
        )
    else:
        row = await _run_export_job(
            int(tenant_id),
            job_id,
            target_dialogs,
            quality_review,
            account_selection,
            deps,
        )
    return {"ok": True, "job": _public_job(row, fallback_job_id=job_id, deps=deps)}


async def get_active_export(
    tenant: int,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    row = await _get_active_job(int(tenant_id), deps=deps)
    if not row:
        return {"ok": True, "job": None}
    return {"ok": True, "job": _public_job(row, fallback_job_id=str(row.get("job_id") or ""), deps=deps)}


async def resume_pending_exports(
    *,
    deps: ClientAvitoHistoryExportDeps,
    limit: int = 20,
) -> int:
    reset_jobs = getattr(deps.export_repo_module, "reset_interrupted_jobs", None)
    if reset_jobs is not None:
        try:
            await reset_jobs()
        except Exception:
            deps.logger.exception("avito_history_export_reset_interrupted_failed")

    list_jobs = getattr(deps.export_repo_module, "list_queued_jobs", None)
    if list_jobs is None:
        return 0
    try:
        rows = await list_jobs(limit=limit)
    except Exception:
        deps.logger.exception("avito_history_export_list_queued_failed")
        return 0

    scheduled = 0
    for row in rows or []:
        job_id = str(row.get("job_id") or "").strip()
        tenant_id = int(row.get("tenant_id") or 0)
        target_dialogs = int(row.get("target_dialogs") or 0)
        if not job_id or tenant_id <= 0 or target_dialogs <= 0:
            continue
        _schedule_export_job(
            _run_export_job(tenant_id, job_id, target_dialogs, True, {}, deps),
            deps=deps,
        )
        scheduled += 1
    return scheduled


async def startup_resume_exports(
    *,
    runtime_module: Any,
    common_module: Any,
    logger: Any,
    enabled: bool = True,
) -> int:
    if not enabled:
        return 0

    async def _unused_auth(_request: Any, tenant: int) -> tuple[int, str]:
        return int(tenant), ""

    try:
        deps = runtime_module.build_default_deps(_unused_auth, common_module, logger)
        scheduled = await runtime_module.resume_pending_exports(deps=deps)
        logger.info("event=resume_scheduled count=%s", scheduled)
        return int(scheduled or 0)
    except Exception:
        logger.exception("event=resume_failed")
        return 0


async def get_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    row_or_response = await _load_authorized_job(tenant, job_id, request, deps=deps)
    if isinstance(row_or_response, Response):
        return row_or_response
    return {"ok": True, "job": _public_job(row_or_response, fallback_job_id=job_id, deps=deps)}


async def get_latest_export(
    tenant: int,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    get_latest = getattr(deps.export_repo_module, "get_latest_file_job", None)
    if get_latest is None:
        return {"ok": True, "job": None}
    row = await get_latest(int(tenant_id))
    if not row:
        return {"ok": True, "job": None}
    return {"ok": True, "job": _public_job(row, fallback_job_id=str(row.get("job_id") or ""), deps=deps)}


async def list_exports(
    tenant: int,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    list_jobs = getattr(deps.export_repo_module, "list_file_jobs", None)
    if list_jobs is None:
        return {"ok": True, "jobs": []}
    rows = await list_jobs(int(tenant_id))
    jobs = [
        _public_job(row, fallback_job_id=str((row or {}).get("job_id") or ""), deps=deps)
        for row in rows or []
    ]
    return {"ok": True, "jobs": jobs}


async def download_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    row_or_response = await _load_authorized_job(tenant, job_id, request, deps=deps)
    if isinstance(row_or_response, Response):
        return row_or_response
    row = row_or_response
    status = str(row.get("status") or "")
    if status not in {"completed", "partial"}:
        return JSONResponse({"detail": "not_ready"}, status_code=409)
    path = pathlib.Path(str(row.get("file_path") or ""))
    if not path.is_file():
        return JSONResponse({"detail": "file_not_found"}, status_code=404)
    return FileResponse(
        path,
        media_type="text/markdown; charset=utf-8",
        filename=path.name,
    )


async def download_training_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    row_or_response = await _load_authorized_job(tenant, job_id, request, deps=deps)
    if isinstance(row_or_response, Response):
        return row_or_response
    row = row_or_response
    status = str(row.get("status") or "")
    if status not in {"completed", "partial"}:
        return JSONResponse({"detail": "not_ready"}, status_code=409)
    path = pathlib.Path(str(row.get("training_file_path") or ""))
    if not path.is_file():
        return JSONResponse({"detail": "file_not_found"}, status_code=404)
    return FileResponse(
        path,
        media_type="application/x-ndjson; charset=utf-8",
        filename=path.name,
    )


async def download_dialog_dataset_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    return await _download_artifact(
        tenant,
        job_id,
        request,
        deps=deps,
        path_key="dialog_dataset_file_path",
        media_type="application/x-ndjson; charset=utf-8",
    )


async def download_export_summary(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    return await _download_artifact(
        tenant,
        job_id,
        request,
        deps=deps,
        path_key="export_summary_path",
        media_type="application/json; charset=utf-8",
    )


async def download_contextual_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    return await _download_artifact(
        tenant,
        job_id,
        request,
        deps=deps,
        path_key="contextual_file_path",
        media_type="application/x-ndjson; charset=utf-8",
    )


async def download_review_cases_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    return await _download_artifact(
        tenant,
        job_id,
        request,
        deps=deps,
        path_key="review_cases_file_path",
        media_type="application/x-ndjson; charset=utf-8",
    )


async def download_rejected_summary_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    return await _download_artifact(
        tenant,
        job_id,
        request,
        deps=deps,
        path_key="rejected_cases_summary_path",
        media_type="application/json; charset=utf-8",
    )


async def download_domain_schema_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    return await _download_artifact(
        tenant,
        job_id,
        request,
        deps=deps,
        path_key="domain_schema_path",
        media_type="application/json; charset=utf-8",
    )


async def download_business_rules_draft_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    return await _download_artifact(
        tenant,
        job_id,
        request,
        deps=deps,
        path_key="business_rules_draft_path",
        media_type="application/json; charset=utf-8",
    )


async def _download_artifact(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
    path_key: str,
    media_type: str,
) -> Response:
    row_or_response = await _load_authorized_job(tenant, job_id, request, deps=deps)
    if isinstance(row_or_response, Response):
        return row_or_response
    row = row_or_response
    status = str(row.get("status") or "")
    if status not in {"completed", "partial"}:
        return JSONResponse({"detail": "not_ready"}, status_code=409)
    path = pathlib.Path(str(row.get(path_key) or ""))
    if not path.is_file():
        return JSONResponse({"detail": "file_not_found"}, status_code=404)
    return FileResponse(path, media_type=media_type, filename=path.name)


async def download_review_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    row_or_response = await _load_authorized_job(tenant, job_id, request, deps=deps)
    if isinstance(row_or_response, Response):
        return row_or_response
    row = row_or_response
    status = str(row.get("status") or "")
    if status not in {"completed", "partial"}:
        return JSONResponse({"detail": "not_ready"}, status_code=409)
    path = pathlib.Path(str(row.get("review_file_path") or ""))
    if not path.is_file():
        return JSONResponse({"detail": "file_not_found"}, status_code=404)
    return FileResponse(
        path,
        media_type="application/x-ndjson; charset=utf-8",
        filename=path.name,
    )


async def download_summary_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Response:
    row_or_response = await _load_authorized_job(tenant, job_id, request, deps=deps)
    if isinstance(row_or_response, Response):
        return row_or_response
    row = row_or_response
    status = str(row.get("status") or "")
    if status not in {"completed", "partial"}:
        return JSONResponse({"detail": "not_ready"}, status_code=409)
    path = pathlib.Path(str(row.get("summary_file_path") or ""))
    if not path.is_file():
        return JSONResponse({"detail": "file_not_found"}, status_code=404)
    return FileResponse(
        path,
        media_type="application/json; charset=utf-8",
        filename=path.name,
    )


async def delete_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    job_key = (job_id or "").strip()
    if not job_key:
        return JSONResponse({"detail": "invalid_job_id"}, status_code=400)
    delete_job = getattr(deps.export_repo_module, "delete_file_job", None)
    if delete_job is None:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    row = await delete_job(int(tenant_id), job_key)
    if not row:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    return {"ok": True}


async def cancel_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    job_key = (job_id or "").strip()
    if not job_key:
        return JSONResponse({"detail": "invalid_job_id"}, status_code=400)
    cancel_job = getattr(deps.export_repo_module, "cancel_job", None)
    if cancel_job is None:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    row = await cancel_job(int(tenant_id), job_key)
    if not row:
        row = await deps.export_repo_module.get_job(int(tenant_id), job_key)
    if not row:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    return {"ok": True, "job": _public_job(row, fallback_job_id=job_key, deps=deps)}


async def activate_dialog_dataset_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    row_or_response = await _load_authorized_job(tenant, job_id, request, deps=deps)
    if isinstance(row_or_response, Response):
        return row_or_response
    row = row_or_response
    status = str(row.get("status") or "")
    if status not in {"completed", "partial"}:
        return JSONResponse({"detail": "not_ready"}, status_code=409)
    dataset_path = pathlib.Path(str(row.get("dialog_dataset_file_path") or ""))
    if not dataset_path.is_file():
        return JSONResponse({"detail": "dataset_file_not_found"}, status_code=404)

    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    index = dialog_retriever.build_index_from_dialog_dataset(dataset_path)
    if index is None:
        return JSONResponse({"detail": "dataset_empty"}, status_code=422)
    tenant_dir = pathlib.Path(deps.common_module.tenant_dir(int(tenant_id)))
    index_path = dialog_retriever.save_dialog_training_index(index, tenant_dir=tenant_dir)

    cfg = deps.common_module.read_tenant_config(int(tenant_id)) or {}
    if not isinstance(cfg, dict):
        cfg = {}
    learning = cfg.setdefault("learning", {})
    if not isinstance(learning, dict):
        learning = {}
        cfg["learning"] = learning
    dialog_cfg = learning.setdefault("dialog_dataset", {})
    if not isinstance(dialog_cfg, dict):
        dialog_cfg = {}
        learning["dialog_dataset"] = dialog_cfg
    dialog_cfg.update(
        {
            "enabled": True,
            "source": "avito_history_export",
            "source_job_id": str(row.get("job_id") or job_id),
            "dialogs_count": len(index.items),
            "index_sha1": index.sha1,
            "index_path": str(index_path.relative_to(tenant_dir)),
            "dataset_file_name": dataset_path.name,
            "activated_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    learning.setdefault("contextual_cases", {})
    if isinstance(learning["contextual_cases"], dict):
        learning["contextual_cases"]["apply_mode"] = False
    deps.common_module.write_tenant_config(int(tenant_id), cfg)

    return {
        "ok": True,
        "dialog_dataset": {
            "enabled": True,
            "dialogs_count": len(index.items),
            "index_sha1": index.sha1,
            "index_file_name": index_path.name,
            "source_job_id": str(row.get("job_id") or job_id),
            "dataset_file_name": dataset_path.name,
        },
    }


async def deactivate_dialog_dataset_export(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    row_or_response = await _load_authorized_job(tenant, job_id, request, deps=deps)
    if isinstance(row_or_response, Response):
        return row_or_response
    row = row_or_response

    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    cfg = deps.common_module.read_tenant_config(int(tenant_id)) or {}
    if not isinstance(cfg, dict):
        cfg = {}
    learning = cfg.setdefault("learning", {})
    if not isinstance(learning, dict):
        learning = {}
        cfg["learning"] = learning
    dialog_cfg = learning.setdefault("dialog_dataset", {})
    if not isinstance(dialog_cfg, dict):
        dialog_cfg = {}
        learning["dialog_dataset"] = dialog_cfg

    source_job_id = str(dialog_cfg.get("source_job_id") or "")
    requested_job_id = str(row.get("job_id") or job_id)
    if source_job_id and source_job_id != requested_job_id:
        return JSONResponse({"detail": "dataset_not_active_for_job"}, status_code=409)

    dialog_cfg["enabled"] = False
    dialog_cfg["disabled_at"] = datetime.now(timezone.utc).isoformat()
    deps.common_module.write_tenant_config(int(tenant_id), cfg)

    return {
        "ok": True,
        "dialog_dataset": {
            "enabled": False,
            "source_job_id": requested_job_id,
            "dialogs_count": int(dialog_cfg.get("dialogs_count") or 0),
        },
    }


async def _load_authorized_job(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    job_key = (job_id or "").strip()
    if not job_key:
        return JSONResponse({"detail": "invalid_job_id"}, status_code=400)
    row = await deps.export_repo_module.get_job(int(tenant_id), job_key)
    if not row:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    return row


async def _get_active_job(
    tenant_id: int,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | None:
    get_active = getattr(deps.export_repo_module, "get_active_job", None)
    if get_active is None:
        return None
    return await get_active(int(tenant_id))


async def _is_export_job_cancelled(
    tenant_id: int,
    job_id: str,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> bool:
    try:
        row = await deps.export_repo_module.get_job(int(tenant_id), str(job_id))
    except Exception:
        deps.logger.exception("avito_history_export_cancel_load_failed job=%s", job_id)
        return False
    return str((row or {}).get("status") or "") == "cancelled"


def _unlink_result_file(result: Any, *, deps: ClientAvitoHistoryExportDeps) -> None:
    for attr in (
        "file_path",
        "dialog_dataset_file_path",
        "export_summary_path",
        "contextual_file_path",
        "review_cases_file_path",
        "rejected_cases_summary_path",
        "domain_schema_path",
        "business_rules_draft_path",
        "training_file_path",
        "review_file_path",
        "summary_file_path",
    ):
        path_value = getattr(result, attr, None)
        if not path_value:
            continue
        try:
            pathlib.Path(str(path_value)).unlink(missing_ok=True)
        except Exception:
            deps.logger.exception(
                "avito_history_export_cancel_file_cleanup_failed kind=%s path=%s",
                attr,
                path_value,
            )


def _parse_export_payload(payload: dict[str, Any]) -> tuple[int, bool] | Response:
    raw = payload.get("target_dialogs")
    if raw is None:
        raw = payload.get("count")
    if raw is None:
        raw = payload.get("limit")
    try:
        target = int(raw if raw is not None else 100)
    except Exception:
        return JSONResponse({"detail": "invalid_target_dialogs"}, status_code=400)
    if target < 1:
        return JSONResponse({"detail": "target_dialogs_too_small"}, status_code=400)
    if target > 10000:
        return JSONResponse({"detail": "target_dialogs_too_large"}, status_code=400)
    quality_review = payload.get("quality_review", True)
    if isinstance(quality_review, str):
        quality_review = quality_review.strip().lower() not in {"0", "false", "no", "off"}
    return target, bool(quality_review), _parse_account_selection(payload)


def _parse_account_selection(payload: dict[str, Any]) -> dict[str, Any]:
    raw_account = payload.get("account_id")
    all_accounts = bool(payload.get("all_accounts"))
    account_id = None
    if raw_account not in (None, "", "all"):
        try:
            account_id = int(raw_account)
        except Exception:
            account_id = None
    if account_id is not None:
        all_accounts = False
    return {"account_id": account_id, "all_accounts": all_accounts}


def _schedule_export_job(
    coro: Awaitable[Any],
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> Any:
    scheduler = deps.task_scheduler or asyncio.create_task
    task = scheduler(coro)
    add_done_callback = getattr(task, "add_done_callback", None)
    if callable(add_done_callback):
        add_done_callback(lambda done: _log_background_task_failure(done, deps=deps))
    return task


def _log_background_task_failure(done: Any, *, deps: ClientAvitoHistoryExportDeps) -> None:
    try:
        done.result()
    except Exception:
        deps.logger.exception("avito_history_export_background_task_failed")


async def _update_export_progress(
    job_id: str,
    progress: Any,
    *,
    deps: ClientAvitoHistoryExportDeps,
) -> None:
    update_progress = getattr(deps.export_repo_module, "update_progress", None)
    if update_progress is None:
        return
    try:
        await update_progress(
            job_id=job_id,
            candidates_seen=int(getattr(progress, "candidates_seen", 0) or 0),
            dialogs_accepted=int(getattr(progress, "dialogs_accepted", 0) or 0),
            dialogs_rejected=int(getattr(progress, "dialogs_rejected", 0) or 0),
            reject_reasons=dict(getattr(progress, "reject_reasons", {}) or {}),
            contextual_cases_count=int(getattr(progress, "contextual_cases_count", 0) or 0),
            review_cases_count=int(getattr(progress, "review_cases_count", 0) or 0),
            ai_extracted_count=int(getattr(progress, "ai_extracted_count", 0) or 0),
            rule_fallback_count=int(getattr(progress, "rule_fallback_count", 0) or 0),
            context_bound_count=int(getattr(progress, "context_bound_count", 0) or 0),
            direct_example_count=int(getattr(progress, "direct_example_count", 0) or 0),
            clarify_first_count=int(getattr(progress, "clarify_first_count", 0) or 0),
            style_only_count=int(getattr(progress, "style_only_count", 0) or 0),
            review_count=int(getattr(progress, "review_count", 0) or 0),
            reject_count=int(getattr(progress, "reject_count", 0) or 0),
            contextual_mode=getattr(progress, "contextual_mode", None),
            dialog_dataset_count=int(getattr(progress, "dialog_dataset_count", 0) or 0),
            export_pipeline_version=getattr(progress, "export_pipeline_version", None),
            ai_schema_calls_count=int(getattr(progress, "ai_schema_calls_count", 0) or 0),
            legacy_contextual_enabled=bool(getattr(progress, "legacy_contextual_enabled", False)),
            checkpoint_available=bool(getattr(progress, "checkpoint_available", False)),
            checkpoint_stage=getattr(progress, "checkpoint_stage", None),
            api_errors_summary=dict(getattr(progress, "api_errors_summary", {}) or {}),
            error_code=getattr(progress, "error_code", None),
        )
    except Exception:
        deps.logger.exception("avito_history_export_progress_update_failed job=%s", job_id)


async def _run_export_job(
    tenant_id: int,
    job_id: str,
    target_dialogs: int,
    quality_review: bool,
    account_selection: dict[str, Any],
    deps: ClientAvitoHistoryExportDeps,
) -> dict[str, Any] | None:
    claim_job = getattr(deps.export_repo_module, "claim_job", None)
    if claim_job is not None:
        claimed = await claim_job(job_id)
        if not claimed:
            deps.logger.info("avito_history_export_claim_skip tenant=%s job=%s", tenant_id, job_id)
            return await deps.export_repo_module.get_job(int(tenant_id), job_id)
    try:
        result = await deps.export_service_module.run_export(
            int(tenant_id),
            target_dialogs=target_dialogs,
            job_id=job_id,
            account_id=account_selection.get("account_id"),
            all_accounts=bool(account_selection.get("all_accounts")),
            deps=deps.export_service_module.AvitoHistoryExportDeps(
                common_module=deps.common_module,
                avito_module=deps.avito_module,
                avito_api_module=deps.avito_api_module,
                export_root=deps.export_root,
                quality_review_enabled=bool(quality_review),
                legacy_contextual_cases_enabled=_env_bool("AVITO_LEGACY_CONTEXTUAL_CASES_ENABLED", False),
                progress_callback=lambda progress: _update_export_progress(
                    job_id,
                    progress,
                    deps=deps,
                ),
                cancel_callback=lambda: _is_export_job_cancelled(
                    int(tenant_id),
                    job_id,
                    deps=deps,
                ),
                logger=deps.logger,
            ),
        )
        if await _is_export_job_cancelled(int(tenant_id), job_id, deps=deps):
            _mark_result_cancelled(result)
        return await deps.export_repo_module.finish_job(job_id=job_id, **result.to_dict())
    except AvitoHistoryExportError as exc:
        status = "no_connection" if exc.code == "not_connected" else "failed"
        return await deps.export_repo_module.finish_job(
            job_id=job_id,
            status=status,
            api_errors_summary={exc.code: 1},
            error_code=exc.code,
            target_dialogs=target_dialogs,
        )
    except Exception:
        deps.logger.exception(
            "avito_history_export_failed tenant=%s job=%s",
            tenant_id,
            job_id,
        )
        return await deps.export_repo_module.finish_job(
            job_id=job_id,
            status="failed",
            api_errors_summary={"unexpected_error": 1},
            error_code="unexpected_error",
            target_dialogs=target_dialogs,
        )


def _mark_result_cancelled(result: Any) -> None:
    has_artifact = any(
        getattr(result, attr, None)
        for attr in (
            "file_path",
            "dialog_dataset_file_path",
            "export_summary_path",
            "contextual_file_path",
            "review_cases_file_path",
            "rejected_cases_summary_path",
            "domain_schema_path",
            "business_rules_draft_path",
            "training_file_path",
            "review_file_path",
            "summary_file_path",
        )
    )
    result.status = "partial" if has_artifact else "cancelled"
    result.error_code = "cancelled"
    if has_artifact:
        return
    for attr in (
        "file_path",
        "dialog_dataset_file_path",
        "export_summary_path",
        "contextual_file_path",
        "review_cases_file_path",
        "rejected_cases_summary_path",
        "domain_schema_path",
        "business_rules_draft_path",
        "training_file_path",
        "review_file_path",
        "summary_file_path",
    ):
        setattr(result, attr, None)
    for attr in (
        "file_size",
        "dialog_dataset_file_size",
        "dialog_dataset_count",
        "export_summary_size",
        "contextual_file_size",
        "contextual_cases_count",
        "review_cases_file_size",
        "review_cases_count",
        "rejected_cases_summary_size",
        "domain_schema_size",
        "business_rules_draft_size",
        "domain_slots_count",
        "ai_extracted_count",
        "rule_fallback_count",
        "context_bound_count",
        "direct_example_count",
        "clarify_first_count",
        "style_only_count",
        "review_count",
        "reject_count",
        "training_file_size",
        "training_examples_count",
        "review_file_size",
        "review_examples_count",
        "summary_file_size",
        "rejected_examples_count",
        "hard_rejected_count",
        "ai_rejected_count",
        "ai_reviewed_count",
        "ai_failed_count",
        "ai_schema_calls_count",
    ):
        setattr(result, attr, 0)
    result.contextual_quality_summary = {}
    result.domain_schema_summary = {}
    result.domain_key = None
    result.domain_label = None
    result.contextual_mode = None
    result.quality_summary = {}
    result.quality_mode = None
    result.export_pipeline_version = None
    result.legacy_contextual_enabled = False


def _public_job(row: Any, *, fallback_job_id: str, deps: ClientAvitoHistoryExportDeps | None = None) -> dict[str, Any]:
    data = dict(row or {})
    data.setdefault("job_id", fallback_job_id)
    status = data.get("status")
    result = {
        "job_id": data.get("job_id"),
        "tenant_id": data.get("tenant_id"),
        "status": status,
        "target_dialogs": int(data.get("target_dialogs") or 0),
        "candidates_seen": int(data.get("candidates_seen") or 0),
        "dialogs_accepted": int(data.get("dialogs_accepted") or 0),
        "dialogs_rejected": int(data.get("dialogs_rejected") or 0),
        "reject_reasons": _dict_field(data, "reject_reasons"),
        "api_errors_summary": _dict_field(data, "api_errors_summary"),
        "error_code": data.get("error_code"),
    }
    result.update(_public_artifact_fields(data, status=status))
    result.update(_public_dialog_dataset_activation_fields(data, deps=deps))
    result.update(_public_contextual_fields(data))
    result.update(_public_legacy_quality_fields(data))
    result.update(_public_datetime_fields(data))
    return result


def _public_dialog_dataset_activation_fields(
    data: dict[str, Any],
    *,
    deps: ClientAvitoHistoryExportDeps | None,
) -> dict[str, Any]:
    if deps is None:
        return {
            "dialog_dataset_active": False,
            "dialog_dataset_active_count": 0,
            "dialog_dataset_index_sha1": None,
        }
    tenant_id = int(data.get("tenant_id") or 0)
    job_id = str(data.get("job_id") or "").strip()
    try:
        cfg = deps.common_module.read_tenant_config(tenant_id) or {}
    except Exception:
        cfg = {}
    learning = cfg.get("learning") if isinstance(cfg, dict) else {}
    dialog_cfg = learning.get("dialog_dataset") if isinstance(learning, dict) else {}
    if not isinstance(dialog_cfg, dict):
        dialog_cfg = {}
    active = bool(dialog_cfg.get("enabled") and str(dialog_cfg.get("source_job_id") or "") == job_id)
    return {
        "dialog_dataset_active": active,
        "dialog_dataset_active_count": int(dialog_cfg.get("dialogs_count") or 0) if active else 0,
        "dialog_dataset_index_sha1": str(dialog_cfg.get("index_sha1") or "") or None if active else None,
    }


def _public_artifact_fields(data: dict[str, Any], *, status: Any) -> dict[str, Any]:
    file_available, file_name = _artifact_meta(data, path_key="file_path", size_key="file_size", status=status)
    contextual_available, contextual_name = _artifact_meta(
        data, path_key="contextual_file_path", size_key="contextual_file_size", status=status
    )
    review_cases_available, review_cases_name = _artifact_meta(
        data, path_key="review_cases_file_path", size_key="review_cases_file_size", status=status
    )
    rejected_summary_available, rejected_summary_name = _artifact_meta(
        data, path_key="rejected_cases_summary_path", size_key="rejected_cases_summary_size", status=status
    )
    domain_schema_available, domain_schema_name = _artifact_meta(
        data, path_key="domain_schema_path", size_key="domain_schema_size", status=status
    )
    business_rules_available, business_rules_name = _artifact_meta(
        data, path_key="business_rules_draft_path", size_key="business_rules_draft_size", status=status
    )
    dialog_dataset_available, dialog_dataset_name = _artifact_meta(
        data, path_key="dialog_dataset_file_path", size_key="dialog_dataset_file_size", status=status
    )
    export_summary_available, export_summary_name = _artifact_meta(
        data, path_key="export_summary_path", size_key="export_summary_size", status=status
    )
    return {
        "file_size": int(data.get("file_size") or 0),
        "file_available": file_available,
        "file_name": file_name,
        "dialog_dataset_file_size": int(data.get("dialog_dataset_file_size") or 0),
        "dialog_dataset_count": int(data.get("dialog_dataset_count") or 0),
        "dialog_dataset_file_available": dialog_dataset_available,
        "dialog_dataset_file_name": dialog_dataset_name,
        "selected_account_id": data.get("selected_account_id"),
        "selected_account_login": data.get("selected_account_login"),
        "account_count": int(data.get("account_count") or 1),
        "accounts_processed": int(data.get("accounts_processed") or 0),
        "export_summary_file_size": int(data.get("export_summary_size") or 0),
        "export_summary_file_available": export_summary_available,
        "export_summary_file_name": export_summary_name,
        "export_pipeline_version": data.get("export_pipeline_version"),
        "ai_schema_calls_count": int(data.get("ai_schema_calls_count") or 0),
        "legacy_contextual_enabled": bool(data.get("legacy_contextual_enabled") or False),
        "checkpoint_available": bool(data.get("checkpoint_available") or False),
        "checkpoint_stage": data.get("checkpoint_stage"),
        "contextual_file_size": int(data.get("contextual_file_size") or 0),
        "contextual_cases_count": int(data.get("contextual_cases_count") or 0),
        "contextual_file_available": contextual_available,
        "contextual_file_name": contextual_name,
        "review_cases_file_size": int(data.get("review_cases_file_size") or 0),
        "review_cases_count": int(data.get("review_cases_count") or 0),
        "review_cases_file_available": review_cases_available,
        "review_cases_file_name": review_cases_name,
        "rejected_cases_summary_size": int(data.get("rejected_cases_summary_size") or 0),
        "rejected_cases_summary_available": rejected_summary_available,
        "rejected_cases_summary_name": rejected_summary_name,
        "domain_schema_file_size": int(data.get("domain_schema_size") or 0),
        "domain_schema_file_available": domain_schema_available,
        "domain_schema_file_name": domain_schema_name,
        "business_rules_draft_file_size": int(data.get("business_rules_draft_size") or 0),
        "business_rules_draft_file_available": business_rules_available,
        "business_rules_draft_file_name": business_rules_name,
    }


def _public_contextual_fields(data: dict[str, Any]) -> dict[str, Any]:
    return {
        "contextual_quality_summary": _dict_field(data, "contextual_quality_summary"),
        "domain_schema_summary": _dict_field(data, "domain_schema_summary"),
        "domain_key": data.get("domain_key"),
        "domain_label": data.get("domain_label"),
        "domain_slots_count": int(data.get("domain_slots_count") or 0),
        "contextual_mode": data.get("contextual_mode"),
        "ai_extracted_count": int(data.get("ai_extracted_count") or 0),
        "rule_fallback_count": int(data.get("rule_fallback_count") or 0),
        "context_bound_count": int(data.get("context_bound_count") or 0),
        "direct_example_count": int(data.get("direct_example_count") or 0),
        "clarify_first_count": int(data.get("clarify_first_count") or 0),
        "style_only_count": int(data.get("style_only_count") or 0),
        "review_count": int(data.get("review_count") or 0),
        "reject_count": int(data.get("reject_count") or 0),
    }


def _public_legacy_quality_fields(data: dict[str, Any]) -> dict[str, Any]:
    status = data.get("status")
    training_available, training_name = _artifact_meta(
        data, path_key="training_file_path", size_key="training_file_size", status=status
    )
    review_available, review_name = _artifact_meta(
        data, path_key="review_file_path", size_key="review_file_size", status=status
    )
    summary_available, summary_name = _artifact_meta(
        data, path_key="summary_file_path", size_key="summary_file_size", status=status
    )
    return {
        "training_file_size": int(data.get("training_file_size") or 0),
        "training_examples_count": int(data.get("training_examples_count") or 0),
        "training_file_available": training_available,
        "training_file_name": training_name,
        "review_file_size": int(data.get("review_file_size") or 0),
        "review_examples_count": int(data.get("review_examples_count") or 0),
        "review_file_available": review_available,
        "review_file_name": review_name,
        "summary_file_size": int(data.get("summary_file_size") or 0),
        "summary_file_available": summary_available,
        "summary_file_name": summary_name,
        "rejected_examples_count": int(data.get("rejected_examples_count") or 0),
        "hard_rejected_count": int(data.get("hard_rejected_count") or 0),
        "ai_rejected_count": int(data.get("ai_rejected_count") or 0),
        "ai_reviewed_count": int(data.get("ai_reviewed_count") or 0),
        "ai_failed_count": int(data.get("ai_failed_count") or 0),
        "quality_summary": _dict_field(data, "quality_summary"),
        "quality_mode": data.get("quality_mode"),
    }


def _public_datetime_fields(data: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in ("created_at", "finished_at", "updated_at"):
        value = data.get(key)
        if isinstance(value, datetime):
            value = value.astimezone(timezone.utc).isoformat()
        result[key] = value
    return result


def _dict_field(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    return value if isinstance(value, dict) else {}


def _artifact_meta(data: dict[str, Any], *, path_key: str, size_key: str, status: Any) -> tuple[bool, str | None]:
    available = bool(
        status in {"completed", "partial"} and data.get(path_key) and int(data.get(size_key) or 0) > 0
    )
    name = pathlib.Path(str(data.get(path_key) or "")).name if available else None
    return available, name


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


__all__ = [
    "ClientAvitoHistoryExportDeps",
    "activate_dialog_dataset_export",
    "cancel_export",
    "deactivate_dialog_dataset_export",
    "download_contextual_export",
    "download_dialog_dataset_export",
    "download_business_rules_draft_export",
    "download_domain_schema_export",
    "download_export_summary",
    "download_rejected_summary_export",
    "download_review_cases_export",
    "delete_export",
    "download_review_export",
    "download_export",
    "download_summary_export",
    "download_training_export",
    "get_active_export",
    "get_export",
    "get_latest_export",
    "list_exports",
    "register_routes",
    "resume_pending_exports",
    "start_export",
    "startup_resume_exports",
]
