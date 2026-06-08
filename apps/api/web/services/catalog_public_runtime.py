from __future__ import annotations

import json
import mimetypes
import os
import pathlib
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import BackgroundTasks, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from starlette.datastructures import UploadFile as StarletteUploadFile


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class CatalogPublicDeps:
    logger: Any
    resolve_key_fn: SyncFn
    auth_fn: SyncFn
    common_module: Any
    allowed_extensions: set[str]
    max_upload_size_bytes: int
    make_safe_filename_fn: SyncFn
    relative_to_fn: SyncFn
    read_csv_bytes_fn: SyncFn
    read_excel_bytes_fn: SyncFn
    process_pdf_fn: SyncFn
    resolve_job_metrics_fn: SyncFn
    catalog_index_error: type[Exception]
    write_catalog_csv_fn: SyncFn
    stringify_fn: SyncFn
    amocrm_service_module: Any
    write_tenant_config_fn: SyncFn
    read_tenant_config_fn: SyncFn
    quote_plus_fn: SyncFn


@dataclass(frozen=True)
class CatalogViewDeps:
    core_module: Any
    render_template_fn: SyncFn
    template_name: str
    time_module: Any


@dataclass
class _CatalogUploadInput:
    tenant_id: int
    tenant_source: str
    form_data: Any
    upload_file: UploadFile | StarletteUploadFile
    file_field_name: str | None
    key: str


@dataclass
class _CatalogUploadJob:
    tenant_id: int
    tenant_source: str
    file_field_name: str | None
    key: str
    filename: str
    ext: str
    raw: bytes
    tenant_root: pathlib.Path
    saved_upload_path: pathlib.Path
    saved_upload_rel: pathlib.Path
    relative_path: str
    job_id: str
    status_path: pathlib.Path
    status_state: dict[str, Any]
    mime_type: str | None


async def catalog_upload(
    request: Request,
    background_tasks: BackgroundTasks,
    *,
    tenant: str | None,
    deps: CatalogPublicDeps,
) -> Response:
    resolved = await _resolve_catalog_upload_input(request, tenant, deps)
    if isinstance(resolved, Response):
        return resolved
    raw = await resolved.upload_file.read()
    validation_error = _validate_catalog_upload_file(resolved.upload_file, raw, deps)
    if validation_error is not None:
        return validation_error
    job = _prepare_catalog_upload_job(resolved, raw, deps)
    _record_catalog_upload_received(job)
    if background_tasks is not None:
        background_tasks.add_task(_process_catalog_upload_job, job, deps)
    else:
        try:
            _process_catalog_upload_job(job, deps)
        except Exception:
            pass
    return _catalog_upload_accept_response(request, job, deps)


def catalog_view_public(
    *,
    tenant: int,
    request: Request,
    deps: CatalogViewDeps,
) -> Response:
    tenant_id = _valid_public_catalog_tenant(tenant)
    meta = _resolve_public_catalog_meta(tenant_id, deps)
    catalog_url, catalog_size_mb, catalog_updated_at = _catalog_view_file_info(
        tenant_id,
        request,
        meta,
    )
    brand, agent_name, city = _catalog_view_brand_context(tenant_id, deps)
    return deps.render_template_fn(
        deps.template_name,
        {
            "request": request,
            "tenant_id": tenant_id,
            "brand": brand or "Каталог",
            "agent_name": agent_name,
            "city": city,
            "has_catalog": bool(meta),
            "catalog_url": catalog_url,
            "catalog_size_mb": catalog_size_mb,
            "catalog_updated_at": catalog_updated_at,
        },
    )


def public_catalog_file(*, tenant: int, deps: CatalogViewDeps) -> Response:
    tenant_id = _valid_public_catalog_tenant(tenant)
    meta = _resolve_public_catalog_meta(tenant_id, deps)
    if not meta:
        raise HTTPException(status_code=404, detail="not_found")

    pdf_path = pathlib.Path(meta["absolute_path"])
    if not pdf_path.exists() or not pdf_path.is_file():
        raise HTTPException(status_code=404, detail="not_found")

    filename = str(meta.get("filename") or pdf_path.name)
    response = FileResponse(pdf_path, media_type="application/pdf", filename=filename)
    response.headers.setdefault("Cache-Control", "public, max-age=300")
    response.headers["Content-Disposition"] = f'inline; filename="{filename}"'
    updated_at = meta.get("updated_at")
    if isinstance(updated_at, int):
        response.headers["Last-Modified"] = deps.time_module.strftime(
            "%a, %d %b %Y %H:%M:%S GMT",
            deps.time_module.gmtime(updated_at),
        )
    return response


def _valid_public_catalog_tenant(tenant: int) -> int:
    try:
        tenant_id = int(tenant)
    except (TypeError, ValueError):
        raise HTTPException(status_code=404, detail="not_found") from None
    if tenant_id <= 0:
        raise HTTPException(status_code=404, detail="not_found")
    return tenant_id


def _resolve_public_catalog_meta(tenant_id: int, deps: CatalogViewDeps) -> Any:
    try:
        return deps.core_module.resolve_catalog_pdf_meta(tenant_id)
    except Exception:
        return None


def _catalog_view_file_info(
    tenant_id: int,
    request: Request,
    meta: Any,
) -> tuple[str | None, float | None, int | None]:
    if not meta:
        return None, None, None
    try:
        stat = pathlib.Path(meta["absolute_path"]).stat()
    except OSError:
        stat = None
    size_mb = round(stat.st_size / (1024 * 1024), 2) if stat else None
    updated_at = int(stat.st_mtime) if stat else None
    return _catalog_view_url(tenant_id, request, updated_at), size_mb, updated_at


def _catalog_view_url(tenant_id: int, request: Request, updated_at: int | None) -> str | None:
    try:
        catalog_url = str(request.url_for("public_catalog_file", tenant=str(tenant_id)))
    except Exception:
        return None
    if updated_at:
        separator = "&" if "?" in catalog_url else "?"
        catalog_url = f"{catalog_url}{separator}v={updated_at}"
    return catalog_url


def _catalog_view_brand_context(tenant_id: int, deps: CatalogViewDeps) -> tuple[str, str, str]:
    try:
        cfg = deps.core_module.load_tenant(tenant_id)
    except Exception:
        cfg = {}
    passport = cfg.get("passport") if isinstance(cfg, dict) and isinstance(cfg.get("passport"), dict) else {}
    if not isinstance(passport, dict):
        return "", "", ""
    return (
        str(passport.get("brand") or "").strip(),
        str(passport.get("agent_name") or "").strip(),
        str(passport.get("city") or "").strip(),
    )


def catalog_upload_status(
    *,
    tenant: int,
    job_id: str,
    request: Request,
    deps: CatalogPublicDeps,
) -> JSONResponse:
    tenant_id = int(tenant)
    key = deps.resolve_key_fn(request, request.query_params.get("k"))
    if not _catalog_status_authorized(request, tenant_id, key, deps):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    payload, error = load_catalog_status_payload(tenant_id, job_id, deps)
    if error == "not_found":
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    if error:
        return JSONResponse({"ok": False, "error": error}, status_code=500)
    data = payload or {}
    return JSONResponse({"ok": True, **data})


def public_catalog_status(
    *,
    request: Request,
    job_id: str,
    tenant: int,
    key: str | None,
    deps: CatalogPublicDeps,
) -> JSONResponse:
    tenant_id = _valid_catalog_status_tenant(tenant)
    resolved_key = deps.resolve_key_fn(request, key)
    if not _catalog_status_authorized(request, tenant_id, resolved_key, deps):
        raise HTTPException(status_code=401, detail="invalid_key")

    payload, error = load_catalog_status_payload(tenant_id, job_id, deps)
    if error == "not_found":
        raise HTTPException(status_code=404, detail="not_found")
    if error:
        raise HTTPException(status_code=500, detail=error)

    return JSONResponse(payload or {})


def catalog_status_public(
    *,
    request: Request,
    tenant: str,
    job: str,
    key: str | None,
    deps: CatalogPublicDeps,
) -> JSONResponse:
    tenant_id = _coerce_catalog_status_tenant(tenant)
    if tenant_id is None:
        return JSONResponse({"ok": False, "error": "invalid_tenant"}, status_code=422)
    safe_job = _safe_catalog_status_job(job)
    if safe_job is None:
        return JSONResponse({"ok": False, "error": "invalid_job"}, status_code=422)

    key_candidate = key if key is not None else request.query_params.get("k")
    resolved_key = deps.resolve_key_fn(request, key_candidate)
    if not _catalog_status_authorized(request, tenant_id, resolved_key, deps):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)

    payload, error = load_catalog_status_payload(tenant_id, safe_job, deps)
    if error == "not_found":
        return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    if error:
        return JSONResponse({"ok": False, "error": error}, status_code=500)

    data = payload if isinstance(payload, dict) else {"state": payload}
    sanitized = sanitize_catalog_status_public(data)
    sanitized["ok"] = True
    sanitized.setdefault("job_id", safe_job)
    sanitized.setdefault("state", str(data.get("state", "") or ""))
    if "error" not in sanitized:
        sanitized["error"] = data.get("error")
    if "message" not in sanitized:
        sanitized["message"] = data.get("message")
    if "updated_at" not in sanitized:
        sanitized["updated_at"] = data.get("updated_at")
    return JSONResponse(sanitized)


def load_catalog_status_payload(
    tenant_id: int,
    job_id: str,
    deps: CatalogPublicDeps,
) -> tuple[dict[str, Any] | None, str | None]:
    tenant_root = pathlib.Path(deps.common_module.tenant_dir(tenant_id))
    status_path = tenant_root / "catalog_jobs" / job_id / "status.json"
    if not status_path.exists():
        return None, "not_found"
    try:
        raw = status_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, "not_found"
    except Exception as exc:
        deps.logger.warning("catalog status read failed", exc_info=exc)
        return None, "status_read_failed"
    try:
        data = json.loads(raw)
    except Exception as exc:
        deps.logger.warning("catalog status json decode failed", exc_info=exc)
        return None, "status_read_failed"
    if not isinstance(data, dict):
        data = {"state": data}
    return data, None


def sanitize_catalog_status_public(payload: Any) -> Any:
    allowed_path_keys = {"source_path", "csv_path"}

    if isinstance(payload, dict):
        sanitized: dict[Any, Any] = {}
        for key, value in payload.items():
            key_str = str(key)
            normalized = key_str.lower()
            if "path" in normalized and normalized not in allowed_path_keys:
                continue
            if normalized == "log" and isinstance(value, list):
                trimmed = value[-50:]
                sanitized[key] = [sanitize_catalog_status_public(item) for item in trimmed]
                continue
            sanitized[key] = sanitize_catalog_status_public(value)
        return sanitized
    if isinstance(payload, list):
        return [sanitize_catalog_status_public(item) for item in payload]
    return payload


def _catalog_status_authorized(
    request: Request,
    tenant_id: int,
    key: str,
    deps: CatalogPublicDeps,
) -> bool:
    if deps.auth_fn(tenant_id, key):
        return True
    header_key = (request.headers.get("X-Access-Key") or "").strip()
    query_key = (request.query_params.get("k") or request.query_params.get("key") or "").strip()
    return bool((key and key == header_key) or (key and query_key and key == query_key))


def _valid_catalog_status_tenant(tenant: int) -> int:
    try:
        tenant_id = int(tenant)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="invalid_tenant") from None
    if tenant_id <= 0:
        raise HTTPException(status_code=400, detail="invalid_tenant")
    return tenant_id


def _coerce_catalog_status_tenant(tenant: str) -> int | None:
    tenant_raw = (tenant or "").strip()
    if not tenant_raw:
        return None
    try:
        tenant_id = int(tenant_raw)
    except (TypeError, ValueError):
        return None
    return tenant_id if tenant_id > 0 else None


def _safe_catalog_status_job(job: str) -> str | None:
    job_raw = (job or "").strip()
    if not job_raw:
        return None
    safe_job = pathlib.Path(job_raw).name
    if safe_job != job_raw:
        return None
    return safe_job


def _invalid_catalog_upload_payload(reason: str) -> JSONResponse:
    body = {"ok": False, "error": "invalid_payload", "reason": reason}
    if reason == "invalid_tenant":
        body["detail"] = reason
    return JSONResponse(body, status_code=422)


async def _resolve_catalog_upload_input(
    request: Request,
    tenant: str | None,
    deps: CatalogPublicDeps,
) -> _CatalogUploadInput | Response:
    form_data = None
    tenant_candidate = (tenant or "").strip() if isinstance(tenant, str) else ""
    tenant_source = "query"
    if not tenant_candidate:
        form_data = await _read_catalog_upload_form(request, deps, "tenant")
        tenant_candidate = _catalog_upload_form_tenant(form_data)
        if tenant_candidate:
            tenant_source = "form"
    if not tenant_candidate:
        tenant_candidate = (os.getenv("TENANT", "1") or "").strip()
        tenant_source = "env" if tenant_candidate else tenant_source
    tenant_id = _coerce_catalog_tenant(tenant_candidate)
    if tenant_id is None:
        return _invalid_catalog_upload_payload("invalid_tenant")
    if form_data is None:
        form_data = await _read_catalog_upload_form(request, deps, "file")
    upload = _find_catalog_upload_file(form_data)
    if upload is None:
        return _invalid_catalog_upload_payload("missing_file")
    file_field_name, upload_file = upload
    key = deps.resolve_key_fn(request, request.query_params.get("k"))
    if not _catalog_upload_authorized(request, tenant_id, key, deps):
        return JSONResponse({"detail": "invalid_key"}, status_code=401)
    return _CatalogUploadInput(
        tenant_id=tenant_id,
        tenant_source=tenant_source,
        form_data=form_data,
        upload_file=upload_file,
        file_field_name=file_field_name,
        key=key,
    )


async def _read_catalog_upload_form(
    request: Request,
    deps: CatalogPublicDeps,
    purpose: str,
) -> Any:
    try:
        return await request.form()
    except Exception as exc:
        deps.logger.warning(
            "catalog_upload: failed to read form data for %s",
            purpose,
            exc_info=exc,
        )
        return None


def _catalog_upload_form_tenant(form_data: Any) -> str:
    if form_data is None:
        return ""
    raw = form_data.get("tenant")
    if isinstance(raw, UploadFile):
        return (raw.filename or "").strip()
    if isinstance(raw, bytes):
        return raw.decode(errors="ignore").strip()
    if isinstance(raw, str):
        return raw.strip()
    if raw is not None:
        return str(raw).strip()
    return ""


def _coerce_catalog_tenant(tenant_candidate: str) -> int | None:
    try:
        tenant_id = int(tenant_candidate)
    except (TypeError, ValueError):
        return None
    return tenant_id if tenant_id > 0 else None


def _find_catalog_upload_file(
    form_data: Any,
) -> tuple[str, UploadFile | StarletteUploadFile] | None:
    if form_data is None:
        return None
    candidates: list[tuple[str, UploadFile | StarletteUploadFile]] = []
    _collect_catalog_upload_file(candidates, "file", form_data.get("file"))
    _collect_catalog_upload_file(candidates, "catalog", form_data.get("catalog"))
    return candidates[0] if candidates else None


def _collect_catalog_upload_file(
    candidates: list[tuple[str, UploadFile | StarletteUploadFile]],
    field: str,
    value: Any,
) -> None:
    if isinstance(value, (UploadFile, StarletteUploadFile)):
        candidates.append((field, value))
    elif isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, (UploadFile, StarletteUploadFile)):
                candidates.append((field, item))


def _catalog_upload_authorized(
    request: Request,
    tenant_id: int,
    key: str,
    deps: CatalogPublicDeps,
) -> bool:
    if deps.auth_fn(tenant_id, key):
        return True
    header_key = (request.headers.get("X-Access-Key") or "").strip()
    query_key = (request.query_params.get("k") or request.query_params.get("key") or "").strip()
    return bool((key and key == header_key) or (key and query_key and key == query_key))


def _validate_catalog_upload_file(
    upload_file: UploadFile | StarletteUploadFile,
    raw: bytes,
    deps: CatalogPublicDeps,
) -> JSONResponse | None:
    filename = (upload_file.filename or "").strip()
    if not filename:
        return JSONResponse({"ok": False, "error": "empty_file"}, status_code=400)
    ext = pathlib.Path(filename).suffix.lower()
    if ext not in deps.allowed_extensions:
        return JSONResponse({"ok": False, "error": "unsupported_type"}, status_code=400)
    if not raw:
        return JSONResponse(
            {"ok": False, "error": "empty_file", "message": "Файл не содержит данных"},
            status_code=400,
        )
    if len(raw) > deps.max_upload_size_bytes:
        return JSONResponse(
            {
                "ok": False,
                "error": "file_too_large",
                "max_size_bytes": deps.max_upload_size_bytes,
            },
            status_code=400,
        )
    return None


def _prepare_catalog_upload_job(
    upload: _CatalogUploadInput,
    raw: bytes,
    deps: CatalogPublicDeps,
) -> _CatalogUploadJob:
    filename = (upload.upload_file.filename or "").strip()
    ext = pathlib.Path(filename).suffix.lower()
    deps.common_module.ensure_tenant_files(upload.tenant_id)
    tenant_root = pathlib.Path(deps.common_module.tenant_dir(upload.tenant_id))
    uploads_dir = tenant_root / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    safe_name = deps.make_safe_filename_fn(filename, ext, fallback=f"catalog_{uuid.uuid4().hex}")
    saved_upload_path = uploads_dir / safe_name
    saved_upload_path.write_bytes(raw)
    saved_upload_rel = pathlib.Path(deps.relative_to_fn(saved_upload_path, tenant_root))
    job_id = uuid.uuid4().hex
    job_root = tenant_root / "catalog_jobs" / job_id
    job_root.mkdir(parents=True, exist_ok=True)
    mime_type, _ = mimetypes.guess_type(filename)
    return _CatalogUploadJob(
        tenant_id=upload.tenant_id,
        tenant_source=upload.tenant_source,
        file_field_name=upload.file_field_name,
        key=upload.key,
        filename=filename,
        ext=ext,
        raw=raw,
        tenant_root=tenant_root,
        saved_upload_path=saved_upload_path,
        saved_upload_rel=saved_upload_rel,
        relative_path=str(saved_upload_rel),
        job_id=job_id,
        status_path=job_root / "status.json",
        status_state={
            "job_id": job_id,
            "state": "pending",
            "error": None,
            "log": [],
            "filename": filename,
            "message": "",
            "tenant_source": upload.tenant_source,
            "file_field": upload.file_field_name,
        },
        mime_type=mime_type,
    )


def _record_catalog_upload_received(job: _CatalogUploadJob) -> None:
    _write_catalog_job_status(
        job,
        None,
        tenant_source=job.tenant_source,
        file_field=job.file_field_name,
    )
    _append_catalog_job_log(
        job,
        "info",
        "tenant_resolved",
        source=job.tenant_source,
        tenant=job.tenant_id,
    )
    _append_catalog_job_log(job, "info", "upload_field_detected", field=job.file_field_name)
    _write_catalog_job_status(
        job,
        "received",
        size=len(job.raw),
        mime=job.mime_type,
        source_path=job.relative_path,
    )
    _append_catalog_job_log(
        job,
        "info",
        "file_received",
        size=len(job.raw),
        mime=job.mime_type,
        field=job.file_field_name,
    )


def _write_catalog_job_status(
    job: _CatalogUploadJob,
    status: str | None = None,
    **fields: Any,
) -> None:
    if status is not None:
        job.status_state["state"] = status
    job.status_state["updated_at"] = int(time.time())
    for field_key, value in fields.items():
        job.status_state[field_key] = value
    job.status_path.write_text(
        json.dumps(job.status_state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _append_catalog_job_log(
    job: _CatalogUploadJob,
    level: str,
    message: str,
    **extra: Any,
) -> None:
    entry = {"ts": int(time.time()), "level": level, "message": message}
    if extra:
        entry.update({k: v for k, v in extra.items() if v is not None})
    job.status_state.setdefault("log", []).append(entry)
    _write_catalog_job_status(job, None, log=job.status_state["log"])


def _fail_catalog_job(
    job: _CatalogUploadJob,
    error_key: str,
    **details: Any,
) -> None:
    _append_catalog_job_log(job, "error", error_key, **details)
    _write_catalog_job_status(job, "failed", error=error_key, message=error_key, **details)


def _process_catalog_upload_job(
    job: _CatalogUploadJob,
    deps: CatalogPublicDeps,
) -> None:
    try:
        _write_catalog_job_status(job, "processing")
        _append_catalog_job_log(
            job,
            "info",
            "job_started",
            source=job.tenant_source,
            field=job.file_field_name,
        )
        rows, meta, manifest_rel = _parse_catalog_upload_job(job, deps)
        if rows is None or meta is None:
            return
        result = _write_catalog_upload_rows(job, rows, meta, manifest_rel, deps)
        if result is None:
            return
        csv_rel_path, ordered_columns, items, meta = result
        _update_catalog_upload_config(job, csv_rel_path, ordered_columns, items, meta, deps)
    except Exception as exc:
        deps.logger.exception("catalog job crashed", exc_info=exc)
        _fail_catalog_job(job, "job_crashed", detail=str(exc))


def _parse_catalog_upload_job(
    job: _CatalogUploadJob,
    deps: CatalogPublicDeps,
) -> tuple[list[dict[str, Any]] | None, dict[str, Any] | None, str | None]:
    try:
        if job.ext == ".csv":
            return (*deps.read_csv_bytes_fn(job.saved_upload_path.read_bytes()), None)
        if job.ext in {".xlsx", ".xls"}:
            return (*deps.read_excel_bytes_fn(job.saved_upload_path.read_bytes()), None)
        rows, meta, manifest_rel = deps.process_pdf_fn(
            tenant=job.tenant_id,
            saved_path=job.saved_upload_path,
            tenant_root=job.tenant_root,
            saved_rel_path=job.saved_upload_rel,
            original_name=job.filename,
        )
        return rows, meta, manifest_rel
    except deps.catalog_index_error as exc:
        deps.logger.warning("PDF indexing failed", exc_info=exc)
        _fail_catalog_job(job, "pdf_index_failed", detail=str(exc))
    except Exception as exc:
        deps.logger.exception("catalog processing failed", exc_info=exc)
        _fail_catalog_job(job, "processing_failed", detail=str(exc))
    return None, None, None


def _write_catalog_upload_rows(
    job: _CatalogUploadJob,
    rows: list[dict[str, Any]],
    meta: dict[str, Any],
    manifest_rel: str | None,
    deps: CatalogPublicDeps,
) -> tuple[str, list[str], int, dict[str, Any]] | None:
    job_metrics = deps.resolve_job_metrics_fn(meta if isinstance(meta, dict) else None, rows)
    parsed_count = len(rows)
    _append_catalog_job_log(job, "info", "rows_parsed", items=parsed_count)
    base_name = pathlib.Path(job.filename).stem or f"catalog_{job.job_id}"
    try:
        result = deps.write_catalog_csv_fn(job.tenant_id, rows, base_name, meta)
    except Exception as exc:
        deps.logger.exception("write_catalog_csv raised", exc_info=exc)
        _fail_catalog_job(job, "csv_write_failed", detail=str(exc))
        return None
    if not isinstance(result, tuple) or len(result) != 2:
        deps.logger.error("write_catalog_csv returned unexpected result", extra={"result": result})
        _fail_catalog_job(job, "csv_write_failed")
        return None
    csv_rel_path, ordered_columns = result
    meta = _catalog_meta_with_manifest(meta, manifest_rel)
    items = int(meta.get("items", parsed_count)) if isinstance(meta, dict) else parsed_count
    _write_catalog_upload_done_status(
        job,
        csv_rel_path,
        ordered_columns,
        items,
        meta,
        job_metrics,
        manifest_rel,
    )
    _append_catalog_job_log(
        job,
        "info",
        "csv_written",
        items=items,
        columns=len(ordered_columns),
        pipeline=meta.get("pipeline") if isinstance(meta, dict) else None,
        metrics=job_metrics,
    )
    return csv_rel_path, ordered_columns, items, meta


def _catalog_meta_with_manifest(
    meta: dict[str, Any],
    manifest_rel: str | None,
) -> dict[str, Any]:
    if not manifest_rel:
        return meta
    updated = dict(meta)
    updated["manifest_path"] = manifest_rel
    return updated


def _write_catalog_upload_done_status(
    job: _CatalogUploadJob,
    csv_rel_path: str,
    ordered_columns: list[str],
    items: int,
    meta: dict[str, Any],
    job_metrics: dict[str, Any],
    manifest_rel: str | None,
) -> None:
    manual_review_required = bool(
        job_metrics["items_found"] == 0
        or float(job_metrics.get("low_price_rate", 0.0)) > 0.2
    )
    _write_catalog_job_status(
        job,
        "done",
        csv_path=csv_rel_path,
        items=items,
        columns=ordered_columns,
        metadata=meta,
        source_path=job.relative_path,
        message="completed",
        items_found=int(job_metrics["items_found"]),
        pages_total=int(job_metrics.get("pages_total", 0)),
        pages_skipped_no_price=int(job_metrics.get("pages_skipped_no_price", 0)),
        table_pages=int(job_metrics.get("table_pages", 0)),
        median_price=job_metrics.get("median_price"),
        low_price_rate=float(job_metrics.get("low_price_rate", 0.0)),
        price_coverage=float(job_metrics.get("price_coverage", 0.0)),
        manual_review_required=bool(manual_review_required),
    )
    if manifest_rel:
        _write_catalog_job_status(job, None, manifest_path=manifest_rel)


def _update_catalog_upload_config(
    job: _CatalogUploadJob,
    csv_rel_path: str,
    ordered_columns: list[str],
    items: int,
    meta: dict[str, Any],
    deps: CatalogPublicDeps,
) -> None:
    del ordered_columns, items
    cfg_raw = deps.read_tenant_config_fn(job.tenant_id)
    cfg = dict(cfg_raw) if isinstance(cfg_raw, dict) else {}
    catalog_type = _catalog_upload_type(job.ext)
    detected_encoding = deps.stringify_fn(meta.get("encoding")) if isinstance(meta, dict) else ""
    detected_delimiter = _catalog_upload_delimiter(meta, deps)
    cfg["catalogs"] = _updated_catalog_entries(
        cfg.get("catalogs"),
        job.relative_path,
        catalog_type,
        csv_rel_path,
        detected_encoding,
        detected_delimiter,
    )
    integrations_raw = cfg.get("integrations")
    integrations = dict(integrations_raw) if isinstance(integrations_raw, dict) else {}
    cfg["integrations"] = integrations
    integrations["uploaded_catalog"] = _uploaded_catalog_meta(
        job,
        csv_rel_path,
        catalog_type,
        meta,
        detected_encoding,
        detected_delimiter,
    )
    deps.write_tenant_config_fn(job.tenant_id, cfg)
    _append_catalog_job_log(job, "info", "config_updated", catalog_type=catalog_type)


def _catalog_upload_type(ext: str) -> str:
    if ext == ".pdf":
        return "pdf"
    if ext in {".xlsx", ".xls"}:
        return "excel"
    return "csv"


def _catalog_upload_delimiter(
    meta: dict[str, Any],
    deps: CatalogPublicDeps,
) -> str:
    if not isinstance(meta, dict):
        return ""
    raw_delimiter = meta.get("delimiter")
    if isinstance(raw_delimiter, str):
        return raw_delimiter
    return deps.stringify_fn(raw_delimiter)


def _updated_catalog_entries(
    raw_catalogs: Any,
    relative_path: str,
    catalog_type: str,
    csv_rel_path: str,
    detected_encoding: str,
    detected_delimiter: str,
) -> list[dict[str, Any]]:
    catalogs = raw_catalogs if isinstance(raw_catalogs, list) else []
    catalog_entry: dict[str, Any] = {
        "name": "uploaded",
        "path": relative_path,
        "type": catalog_type,
    }
    if detected_encoding:
        catalog_entry["encoding"] = detected_encoding
    if detected_delimiter:
        catalog_entry["delimiter"] = detected_delimiter
    if csv_rel_path:
        catalog_entry["csv_path"] = csv_rel_path
    return [
        catalog_entry,
        *[
            entry
            for entry in catalogs
            if isinstance(entry, dict) and entry.get("path") != relative_path
        ],
    ]


def _uploaded_catalog_meta(
    job: _CatalogUploadJob,
    csv_rel_path: str,
    catalog_type: str,
    meta: dict[str, Any],
    detected_encoding: str,
    detected_delimiter: str,
) -> dict[str, Any]:
    uploaded_meta: dict[str, Any] = {
        "path": job.relative_path,
        "original": job.filename,
        "uploaded_at": int(time.time()),
        "type": catalog_type,
        "size": len(job.raw),
        "mime": job.mime_type or "application/octet-stream",
        "csv_path": csv_rel_path,
    }
    pipeline_info = meta.get("pipeline") if isinstance(meta, dict) else None
    if pipeline_info:
        uploaded_meta["pipeline"] = pipeline_info
    extraction_meta = meta.get("extraction") if isinstance(meta, dict) else None
    if isinstance(extraction_meta, dict):
        uploaded_meta["extraction"] = extraction_meta
    if detected_encoding:
        uploaded_meta["encoding"] = detected_encoding
    if detected_delimiter:
        uploaded_meta["delimiter"] = detected_delimiter
    return {k: v for k, v in uploaded_meta.items() if v is not None}


def _catalog_upload_accept_response(
    request: Request,
    job: _CatalogUploadJob,
    deps: CatalogPublicDeps,
) -> Response:
    if _catalog_upload_wants_html(request):
        redirect_url = request.url_for("client_settings", tenant=str(job.tenant_id))
        if job.key:
            redirect_url = f"{redirect_url}?k={deps.quote_plus_fn(job.key)}"
        return RedirectResponse(url=redirect_url, status_code=303)
    return JSONResponse(
        {
            "ok": True,
            "job_id": job.job_id,
            "state": "queued",
            "filename": job.filename,
        }
    )


def _catalog_upload_wants_html(request: Request) -> bool:
    accept_header = (request.headers.get("accept") or "").lower()
    sec_fetch_mode = (request.headers.get("sec-fetch-mode") or "").lower()
    sec_fetch_dest = (request.headers.get("sec-fetch-dest") or "").lower()
    wants_html = (
        "text/html" in accept_header
        or "application/xhtml+xml" in accept_header
        or sec_fetch_mode == "navigate"
        or sec_fetch_dest == "document"
    )
    if wants_html and (request.headers.get("x-requested-with", "").lower() == "xmlhttprequest"):
        wants_html = False
    return wants_html
