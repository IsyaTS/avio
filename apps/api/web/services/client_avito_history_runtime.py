from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from fastapi import BackgroundTasks, Request
from fastapi.responses import JSONResponse, Response

from libs.core.services.avito_history_probe import AvitoHistoryProbeError


AsyncFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class ClientAvitoHistoryDeps:
    authorize_client_settings_request_fn: AsyncFn
    probe_service_module: Any
    probe_repo_module: Any
    common_module: Any
    avito_module: Any
    avito_api_module: Any
    logger: Any
    uuid_module: Any = uuid
    datetime_cls: Any = datetime
    task_scheduler: Callable[[Awaitable[Any]], Any] | None = None


def build_default_deps(
    authorize_client_settings_request_fn: AsyncFn,
    common_module: Any,
    logger: Any,
) -> ClientAvitoHistoryDeps:
    from libs.core.integrations import avito as avito_integration
    from libs.core.integrations import avito_analytics as avito_analytics_client
    from libs.core.repo import avito_history_probes
    from libs.core.services import avito_history_probe

    return ClientAvitoHistoryDeps(
        authorize_client_settings_request_fn=authorize_client_settings_request_fn,
        probe_service_module=avito_history_probe,
        probe_repo_module=avito_history_probes,
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
    async def _start(tenant: int, request: Request, background_tasks: BackgroundTasks):
        return await start_probe(
            tenant,
            request,
            background_tasks=background_tasks,
            deps=build_default_deps(
                authorize_client_settings_request_fn,
                common_module,
                logger,
            ),
        )

    async def _get(tenant: int, job_id: str, request: Request):
        return await get_probe(
            tenant,
            job_id,
            request,
            deps=build_default_deps(
                authorize_client_settings_request_fn,
                common_module,
                logger,
            ),
        )

    router.add_api_route(
        "/client/{tenant}/avito/history/probe",
        _start,
        methods=["POST"],
        name="avito_history_probe_start",
    )
    router.add_api_route(
        "/client/{tenant}/avito/history/probe/{job_id}",
        _get,
        methods=["GET"],
        name="avito_history_probe_status",
    )


async def start_probe(
    tenant: int,
    request: Request,
    *,
    deps: ClientAvitoHistoryDeps,
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

    parsed = _parse_probe_payload(payload, deps=deps)
    if isinstance(parsed, Response):
        return parsed
    period_from, period_to, chat_limit = parsed
    job_id = deps.uuid_module.uuid4().hex

    row = await deps.probe_repo_module.create_job(
        job_id=job_id,
        tenant_id=int(tenant_id),
        period_from=period_from,
        period_to=period_to,
        chat_limit=chat_limit,
    )
    if background_tasks is not None:
        _schedule_probe_job(
            _run_probe_job(
                int(tenant_id),
                job_id,
                period_from,
                period_to,
                chat_limit,
                deps,
            ),
            deps=deps,
        )
    else:
        row = await _run_probe_job(
            int(tenant_id),
            job_id,
            period_from,
            period_to,
            chat_limit,
            deps,
        )

    return {"ok": True, "job": _public_job(row, fallback_job_id=job_id)}


async def get_probe(
    tenant: int,
    job_id: str,
    request: Request,
    *,
    deps: ClientAvitoHistoryDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    job_key = (job_id or "").strip()
    if not job_key:
        return JSONResponse({"detail": "invalid_job_id"}, status_code=400)
    row = await deps.probe_repo_module.get_job(int(tenant_id), job_key)
    if not row:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    return {"ok": True, "job": _public_job(row, fallback_job_id=job_key)}


def _parse_probe_payload(
    payload: dict[str, Any],
    *,
    deps: ClientAvitoHistoryDeps,
) -> tuple[datetime, datetime, int] | Response:
    now = deps.datetime_cls.now(timezone.utc)
    period_to = _parse_date(
        payload.get("period_to") or payload.get("to"),
        end_of_day=True,
    )
    period_from = _parse_date(
        payload.get("period_from") or payload.get("from"),
        end_of_day=False,
    )
    if period_to is None:
        period_to = now
    if period_from is None:
        period_from = period_to - timedelta(days=30)
    if period_from > period_to:
        return JSONResponse({"detail": "invalid_period"}, status_code=400)
    if period_to - period_from > timedelta(days=370):
        return JSONResponse({"detail": "period_too_large"}, status_code=400)
    try:
        chat_limit = int(payload.get("chat_limit") or payload.get("limit") or 100)
    except Exception:
        chat_limit = 100
    if chat_limit < 1:
        chat_limit = 1
    if chat_limit > 10000:
        chat_limit = 10000
    return period_from, period_to, chat_limit


def _schedule_probe_job(
    coro: Awaitable[Any],
    *,
    deps: ClientAvitoHistoryDeps,
) -> Any:
    scheduler = deps.task_scheduler or asyncio.create_task
    task = scheduler(coro)
    add_done_callback = getattr(task, "add_done_callback", None)
    if callable(add_done_callback):
        add_done_callback(lambda done: _log_background_task_failure(done, deps=deps))
    return task


def _log_background_task_failure(done: Any, *, deps: ClientAvitoHistoryDeps) -> None:
    try:
        done.result()
    except Exception:
        deps.logger.exception("avito_history_probe_background_task_failed")


async def _update_probe_progress(
    job_id: str,
    progress: Any,
    *,
    deps: ClientAvitoHistoryDeps,
) -> None:
    update_progress = getattr(deps.probe_repo_module, "update_progress", None)
    if update_progress is None:
        return
    try:
        await update_progress(
            job_id=job_id,
            chats_seen=int(getattr(progress, "chats_seen", 0) or 0),
            chats_with_messages=int(getattr(progress, "chats_with_messages", 0) or 0),
            messages_seen=int(getattr(progress, "messages_seen", 0) or 0),
            messages_in_period=int(getattr(progress, "messages_in_period", 0) or 0),
            oldest_message_at=getattr(progress, "oldest_message_at", None),
            newest_message_at=getattr(progress, "newest_message_at", None),
            api_errors_summary=dict(getattr(progress, "api_errors_summary", {}) or {}),
            error_code=getattr(progress, "error_code", None),
        )
    except Exception:
        deps.logger.exception("avito_history_probe_progress_update_failed job=%s", job_id)


async def _run_probe_job(
    tenant_id: int,
    job_id: str,
    period_from: datetime,
    period_to: datetime,
    chat_limit: int,
    deps: ClientAvitoHistoryDeps,
) -> dict[str, Any] | None:
    try:
        result = await deps.probe_service_module.run_probe(
            int(tenant_id),
            period_from=period_from,
            period_to=period_to,
            chat_limit=chat_limit,
            deps=deps.probe_service_module.AvitoHistoryProbeDeps(
                common_module=deps.common_module,
                avito_module=deps.avito_module,
                avito_api_module=deps.avito_api_module,
                progress_callback=lambda progress: _update_probe_progress(
                    job_id,
                    progress,
                    deps=deps,
                ),
                logger=deps.logger,
            ),
        )
        return await deps.probe_repo_module.finish_job(job_id=job_id, **result.to_dict())
    except AvitoHistoryProbeError as exc:
        return await deps.probe_repo_module.finish_job(
            job_id=job_id,
            status="failed",
            api_errors_summary={exc.code: 1},
            error_code=exc.code,
        )
    except Exception:
        deps.logger.exception(
            "avito_history_probe_failed tenant=%s job=%s",
            tenant_id,
            job_id,
        )
        return await deps.probe_repo_module.finish_job(
            job_id=job_id,
            status="failed",
            api_errors_summary={"unexpected_error": 1},
            error_code="unexpected_error",
        )


def _parse_date(value: Any, *, end_of_day: bool) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        try:
            if len(text) == 10 and text[4] == "-" and text[7] == "-":
                dt = datetime.fromisoformat(text)
                if end_of_day:
                    dt = dt.replace(hour=23, minute=59, second=59, microsecond=999000)
            else:
                dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _public_job(row: Any, *, fallback_job_id: str) -> dict[str, Any]:
    data = dict(row or {})
    data.setdefault("job_id", fallback_job_id)
    for key in (
        "period_from",
        "period_to",
        "oldest_message_at",
        "newest_message_at",
        "created_at",
        "finished_at",
        "updated_at",
    ):
        value = data.get(key)
        if isinstance(value, datetime):
            data[key] = value.astimezone(timezone.utc).isoformat()
    summary = data.get("api_errors_summary")
    if not isinstance(summary, dict):
        data["api_errors_summary"] = {}
    return {
        "job_id": data.get("job_id"),
        "tenant_id": data.get("tenant_id"),
        "status": data.get("status"),
        "period_from": data.get("period_from"),
        "period_to": data.get("period_to"),
        "chat_limit": data.get("chat_limit"),
        "chats_seen": int(data.get("chats_seen") or 0),
        "chats_with_messages": int(data.get("chats_with_messages") or 0),
        "messages_seen": int(data.get("messages_seen") or 0),
        "messages_in_period": int(data.get("messages_in_period") or 0),
        "oldest_message_at": data.get("oldest_message_at"),
        "newest_message_at": data.get("newest_message_at"),
        "api_errors_summary": data.get("api_errors_summary") or {},
        "error_code": data.get("error_code"),
        "created_at": data.get("created_at"),
        "finished_at": data.get("finished_at"),
    }


__all__ = ["ClientAvitoHistoryDeps", "start_probe", "get_probe"]
