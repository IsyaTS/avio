from __future__ import annotations

import mimetypes
import pathlib
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class PublicPhotosDeps:
    authorize_fn: AsyncFn
    read_manifest_fn: SyncFn
    write_manifest_fn: SyncFn
    photo_url_fn: SyncFn
    validate_upload_fn: SyncFn
    photo_root_fn: SyncFn
    tenant_dir_fn: SyncFn
    max_bytes: int
    logger: Any
    sync_asset_fn: AsyncFn | None = None
    compile_asset_fn: AsyncFn | None = None


async def photos_list(
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: PublicPhotosDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, resolved_key = auth

    items: list[dict[str, Any]] = []
    for entry in sorted(deps.read_manifest_fn(tenant_id), key=_uploaded_at, reverse=True):
        photo_id = str(entry.get("id") or "").strip()
        if not photo_id:
            continue
        payload = dict(entry)
        payload["url"] = deps.photo_url_fn(request, tenant_id, resolved_key, photo_id)
        items.append(payload)
    return {"ok": True, "photos": items}


async def photos_upload(
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    file: UploadFile,
    deps: PublicPhotosDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, resolved_key = auth

    filename = (file.filename or "").strip()
    ok, reason = deps.validate_upload_fn(filename, file.content_type)
    if not ok:
        return JSONResponse({"ok": False, "error": reason}, status_code=400)

    raw = await file.read()
    validation_error = _validate_photo_bytes(raw, deps.max_bytes)
    if validation_error is not None:
        return validation_error

    entry = _store_photo_file(tenant_id, filename, file.content_type, raw, deps)
    entries = deps.read_manifest_fn(tenant_id)
    entries.insert(0, entry)
    deps.write_manifest_fn(tenant_id, entries)
    await _sync_photo_asset(tenant_id, entry, deps, compile_rule=False)

    entry_with_url = dict(entry)
    entry_with_url["url"] = deps.photo_url_fn(request, tenant_id, resolved_key, entry["id"])
    return {"ok": True, "photo": entry_with_url}


async def photos_delete(
    photo_id: str,
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: PublicPhotosDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    entries = deps.read_manifest_fn(tenant_id)
    remaining, removed_entry = _split_removed_photo(entries, photo_id)
    if removed_entry is None:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    _delete_photo_file(tenant_id, removed_entry, deps)
    deps.write_manifest_fn(tenant_id, remaining)
    return {"ok": True}


async def photos_file(
    photo_id: str,
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: PublicPhotosDeps,
) -> Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    entry = _find_photo_entry(deps.read_manifest_fn(tenant_id), photo_id)
    if not entry:
        return JSONResponse({"detail": "not_found"}, status_code=404)

    target_or_response = _photo_target(tenant_id, entry, deps)
    if isinstance(target_or_response, Response):
        return target_or_response
    target = target_or_response
    mime = entry.get("mime") or mimetypes.guess_type(str(target))[0] or "application/octet-stream"
    filename = entry.get("original") or target.name
    return FileResponse(target, media_type=mime, filename=filename)


async def photos_update_meta(
    photo_id: str,
    request: Request,
    *,
    tenant: int | str | None,
    key: str | None,
    deps: PublicPhotosDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_fn(request, tenant, key)
    if isinstance(auth, Response):
        return auth
    tenant_id, resolved_key = auth

    payload = await _read_json_dict(request)
    entries = deps.read_manifest_fn(tenant_id)
    entry = _find_photo_entry(entries, photo_id)
    if not entry:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    _apply_photo_metadata(entry, payload)
    deps.write_manifest_fn(tenant_id, entries)
    await _sync_photo_asset(
        tenant_id,
        entry,
        deps,
        compile_rule=bool(payload.get("compile") or payload.get("auto")),
    )

    entry_with_url = dict(entry)
    entry_with_url["url"] = deps.photo_url_fn(request, tenant_id, resolved_key, photo_id)
    return {"ok": True, "photo": entry_with_url}


def _uploaded_at(entry: dict[str, Any]) -> Any:
    return entry.get("uploaded_at", 0)


def _validate_photo_bytes(raw: bytes, max_bytes: int) -> JSONResponse | None:
    if not raw:
        return JSONResponse({"ok": False, "error": "empty_file"}, status_code=400)
    if len(raw) > max_bytes:
        return JSONResponse(
            {"ok": False, "error": "file_too_large", "max_size_bytes": max_bytes},
            status_code=400,
        )
    return None


def _store_photo_file(
    tenant_id: int,
    filename: str,
    content_type: str | None,
    raw: bytes,
    deps: PublicPhotosDeps,
) -> dict[str, Any]:
    ext = pathlib.Path(filename).suffix.lower()
    photo_id = uuid.uuid4().hex
    safe_name = f"photo_{photo_id}{ext}"
    target = deps.photo_root_fn(tenant_id) / safe_name
    target.write_bytes(raw)
    rel_path = str(target.relative_to(deps.tenant_dir_fn(tenant_id)))
    mime = content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return {
        "id": photo_id,
        "filename": safe_name,
        "original": filename,
        "mime": mime,
        "size": len(raw),
        "uploaded_at": int(time.time()),
        "path": rel_path,
    }


def _split_removed_photo(
    entries: list[dict[str, Any]],
    photo_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    remaining: list[dict[str, Any]] = []
    removed_entry: dict[str, Any] | None = None
    for entry in entries:
        entry_id = str(entry.get("id") or "")
        if entry_id == photo_id and removed_entry is None:
            removed_entry = entry
            continue
        remaining.append(entry)
    return remaining, removed_entry


def _delete_photo_file(tenant_id: int, removed_entry: dict[str, Any], deps: PublicPhotosDeps) -> None:
    rel_path = str(removed_entry.get("path") or "")
    if not rel_path:
        return
    target = deps.tenant_dir_fn(tenant_id) / rel_path
    try:
        target = target.resolve()
        tenant_root = deps.tenant_dir_fn(tenant_id).resolve()
    except Exception:
        target = None
    if target is None or not str(target).startswith(str(tenant_root)) or not target.exists():
        return
    try:
        target.unlink()
    except Exception:
        deps.logger.warning("photo_delete_failed tenant=%s path=%s", tenant_id, rel_path)


def _find_photo_entry(entries: list[dict[str, Any]], photo_id: str) -> dict[str, Any] | None:
    return next((item for item in entries if str(item.get("id") or "") == photo_id), None)


def _photo_target(tenant_id: int, entry: dict[str, Any], deps: PublicPhotosDeps) -> pathlib.Path | Response:
    rel_path = str(entry.get("path") or "")
    if not rel_path:
        return JSONResponse({"detail": "not_found"}, status_code=404)
    target = deps.tenant_dir_fn(tenant_id) / rel_path
    if not target.exists() or not target.is_file():
        return JSONResponse({"detail": "not_found"}, status_code=404)
    return target


async def _read_json_dict(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _apply_photo_metadata(entry: dict[str, Any], payload: dict[str, Any]) -> None:
    _apply_text_metadata(entry, payload, "title")
    _apply_text_metadata(entry, payload, "usage")
    _apply_list_metadata(entry, payload, "tags", lower=False)
    _apply_list_metadata(entry, payload, "channels", lower=True)
    if payload.get("auto") is not None:
        entry["auto"] = bool(payload.get("auto"))
    if payload.get("priority") is not None:
        try:
            entry["priority"] = int(payload.get("priority") or 0)
        except Exception:
            entry["priority"] = 0


def _apply_text_metadata(entry: dict[str, Any], payload: dict[str, Any], key: str) -> None:
    value = payload.get(key)
    if isinstance(value, str):
        entry[key] = value.strip()


def _apply_list_metadata(
    entry: dict[str, Any],
    payload: dict[str, Any],
    key: str,
    *,
    lower: bool,
) -> None:
    raw = payload.get(key)
    items: list[str] = []
    if isinstance(raw, (list, tuple, set)):
        items = [str(item).strip() for item in raw if str(item).strip()]
    elif isinstance(raw, str):
        items = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    if lower:
        items = [item.lower() for item in items]
    if items:
        entry[key] = items
    elif raw is not None:
        entry[key] = []


async def _sync_photo_asset(
    tenant_id: int,
    entry: dict[str, Any],
    deps: PublicPhotosDeps,
    *,
    compile_rule: bool,
) -> None:
    if deps.sync_asset_fn is None:
        return
    try:
        asset = await deps.sync_asset_fn(int(tenant_id), dict(entry))
    except Exception:
        deps.logger.warning("photo_asset_sync_failed tenant=%s", tenant_id)
        return
    if compile_rule and deps.compile_asset_fn is not None and asset:
        try:
            await deps.compile_asset_fn(int(tenant_id), dict(asset), dict(entry))
        except Exception:
            deps.logger.warning("photo_asset_compile_failed tenant=%s", tenant_id)
