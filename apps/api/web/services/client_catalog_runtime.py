from __future__ import annotations

import csv
import io
import mimetypes
import pathlib
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable
from urllib.parse import quote_plus

from fastapi import Request, UploadFile
from fastapi.responses import JSONResponse, RedirectResponse, Response


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ClientCatalogDeps:
    authorize_client_settings_request_fn: AsyncFn
    common_module: Any
    public_module_fn: SyncFn
    write_catalog_csv_fn: SyncFn
    catalog_index_error_cls: type[Exception]
    detect_encoding_fn: SyncFn
    detect_csv_delimiter_fn: SyncFn
    strip_bom_fn: SyncFn
    max_upload_size_bytes: int
    time_module: Any = time
    uuid_module: Any = uuid


async def catalog_upload(
    tenant: int,
    request: Request,
    file: UploadFile,
    *,
    deps: ClientCatalogDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, key = auth

    filename = (file.filename or "").strip()
    ext = pathlib.Path(filename).suffix.lower()
    raw = await file.read()
    validation = _validate_catalog_upload_file(filename, ext, raw, deps)
    if validation is not None:
        return validation

    tenant_root, dest_path, safe_name, relative_path = _store_catalog_upload_file(
        int(tenant_id),
        ext,
        raw,
        deps,
    )

    try:
        normalized_rows, meta = _normalize_uploaded_catalog(
            tenant=int(tenant_id),
            filename=filename,
            ext=ext,
            raw=raw,
            dest_path=dest_path,
            tenant_root=tenant_root,
            deps=deps,
        )
    except deps.catalog_index_error_cls as exc:
        return {"ok": False, "error": "catalog_index_failed", "detail": str(exc)}
    except Exception as exc:
        return {"ok": False, "error": "processing_failed", "detail": str(exc)}

    try:
        csv_rel_path, ordered_columns = deps.write_catalog_csv_fn(
            int(tenant_id), normalized_rows, "catalog", meta
        )
    except Exception as exc:
        return {"ok": False, "error": "csv_write_failed", "detail": str(exc)}

    cfg = _update_catalog_config(
        tenant=int(tenant_id),
        filename=filename,
        ext=ext,
        safe_name=safe_name,
        relative_path=relative_path,
        csv_rel_path=csv_rel_path,
        raw_size=len(raw),
        meta=meta,
        deps=deps,
    )
    deps.common_module.write_tenant_config(tenant_id, cfg)
    return _catalog_upload_response(
        request,
        tenant_id,
        key,
        filename,
        safe_name,
        csv_rel_path,
        normalized_rows,
        ordered_columns,
    )


def _validate_catalog_upload_file(
    filename: str,
    ext: str,
    raw: bytes,
    deps: ClientCatalogDeps,
) -> dict[str, Any] | None:
    if not filename:
        return {"ok": False, "error": "empty_file"}
    if ext not in {".csv", ".xlsx", ".xls", ".pdf"}:
        return {"ok": False, "error": "unsupported_type"}
    if not raw:
        return {"ok": False, "error": "empty_file"}
    if len(raw) > deps.max_upload_size_bytes:
        return {
            "ok": False,
            "error": "file_too_large",
            "max_size_bytes": deps.max_upload_size_bytes,
        }
    return None


def _store_catalog_upload_file(
    tenant_id: int,
    ext: str,
    raw: bytes,
    deps: ClientCatalogDeps,
) -> tuple[pathlib.Path, pathlib.Path, str, str]:
    deps.common_module.ensure_tenant_files(tenant_id)
    tenant_root = pathlib.Path(deps.common_module.tenant_dir(tenant_id))
    uploads_dir = tenant_root / "uploads"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    safe_name = f"catalog_{deps.uuid_module.uuid4().hex}{ext}"
    dest_path = uploads_dir / safe_name
    dest_path.write_bytes(raw)
    relative_path = str(pathlib.Path("uploads") / safe_name)
    return tenant_root, dest_path, safe_name, relative_path


def _catalog_upload_response(
    request: Request,
    tenant_id: int,
    key: str,
    filename: str,
    safe_name: str,
    csv_rel_path: str,
    normalized_rows: list[dict[str, Any]],
    ordered_columns: list[str],
) -> dict[str, Any] | Response:
    if _wants_html_redirect(request):
        redirect_url = str(request.url_for("client_settings", tenant=str(tenant_id)))
        if key:
            redirect_url = f"{redirect_url}?k={quote_plus(key)}"
        return RedirectResponse(url=redirect_url, status_code=303)
    return {
        "ok": True,
        "filename": filename,
        "stored_as": safe_name,
        "csv_path": csv_rel_path,
        "path": csv_rel_path,
        "items_total": len(normalized_rows),
        "columns": ordered_columns,
    }


async def catalog_csv_get(
    tenant: int,
    request: Request,
    *,
    deps: ClientCatalogDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    cfg = deps.common_module.read_tenant_config(tenant_id)
    try:
        table = read_csv_table(tenant_id, cfg, deps=deps)
    except FileNotFoundError:
        return JSONResponse({"detail": "csv_not_ready"}, status_code=404)
    return {"ok": True, **table}


async def catalog_csv_save(
    tenant: int,
    request: Request,
    *,
    deps: ClientCatalogDeps,
) -> dict[str, Any] | Response:
    auth = await deps.authorize_client_settings_request_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    payload = await request.json()
    columns = payload.get("columns")
    rows = payload.get("rows")
    try:
        written = write_csv_table(tenant_id, columns, rows, deps=deps)
    except FileNotFoundError:
        return JSONResponse({"detail": "csv_not_ready"}, status_code=404)
    except ValueError as exc:
        detail = str(exc) or "invalid_rows"
        return JSONResponse({"detail": detail}, status_code=400)
    return {"ok": True, "rows": written}


def catalog_csv_path(
    tenant: int,
    cfg: dict | None = None,
    *,
    deps: ClientCatalogDeps,
) -> tuple[pathlib.Path | None, str | None, str | None]:
    if cfg is None or not isinstance(cfg, dict):
        cfg = deps.common_module.read_tenant_config(tenant)
    if not isinstance(cfg, dict):
        return None, None, None
    result = _catalog_path_from_entries(tenant, cfg, deps=deps)
    if result[0] is not None:
        return result
    result = _catalog_path_from_uploaded_meta(tenant, cfg, deps=deps)
    if result[0] is not None:
        return result
    return _catalog_path_from_directory(tenant, deps=deps)


def read_csv_table(
    tenant: int,
    cfg: dict | None = None,
    *,
    deps: ClientCatalogDeps,
) -> dict[str, list[list[str]] | list[str] | str]:
    csv_path, encoding_hint, relative = catalog_csv_path(tenant, cfg, deps=deps)
    return read_csv_table_from_path(csv_path, encoding_hint, relative, deps=deps)


def read_csv_table_from_path(
    csv_path: pathlib.Path | None,
    encoding_hint: str | None,
    relative: str | None,
    *,
    deps: ClientCatalogDeps,
) -> dict[str, list[list[str]] | list[str] | str]:
    if not csv_path or not csv_path.exists():
        raise FileNotFoundError("csv_not_ready")
    raw = csv_path.read_bytes()
    encoding = encoding_hint or deps.detect_encoding_fn(raw)
    text = raw.decode(encoding or "utf-8", errors="ignore")
    delimiter = deps.detect_csv_delimiter_fn(text)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    header_raw = _read_csv_header(reader, deps=deps)
    if not header_raw:
        return {
            "columns": [],
            "rows": [],
            "encoding": encoding or "utf-8",
            "path": relative or "",
            "delimiter": delimiter,
            "csv_text": text,
        }
    columns, data_rows = _read_csv_rows(reader, header_raw, deps=deps)
    columns, data_rows = _merge_duplicate_columns(columns, data_rows)
    columns, data_rows = _drop_empty_columns(columns, data_rows)
    return {
        "columns": columns,
        "rows": data_rows,
        "encoding": encoding or "utf-8",
        "path": relative or "",
        "delimiter": delimiter,
        "csv_text": text,
    }


def write_csv_table(
    tenant: int,
    columns: Any,
    rows: Any,
    *,
    deps: ClientCatalogDeps,
    cfg: dict | None = None,
) -> int:
    if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        raise ValueError("invalid_columns")
    if not isinstance(rows, list):
        raise ValueError("invalid_rows")
    csv_path, _, _ = catalog_csv_path(tenant, cfg, deps=deps)
    return write_csv_table_to_path(csv_path, columns, rows)


def write_csv_table_to_path(
    csv_path: pathlib.Path | None,
    columns: Any,
    rows: Any,
) -> int:
    if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
        raise ValueError("invalid_columns")
    if not isinstance(rows, list):
        raise ValueError("invalid_rows")
    if not csv_path:
        raise FileNotFoundError("csv_not_ready")
    serializable_rows = _serializable_rows(columns, rows)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writerow([(col or "").strip().lstrip("\ufeff") for col in columns])
        for row in serializable_rows:
            writer.writerow([_clean_csv_cell(cell) for cell in row])
    return len(serializable_rows)


def _tenant_root(tenant: int, *, deps: ClientCatalogDeps) -> pathlib.Path:
    return pathlib.Path(deps.common_module.tenant_dir(tenant))


def _safe_path(
    tenant: int,
    relative: str | pathlib.Path | None,
    *,
    deps: ClientCatalogDeps,
) -> pathlib.Path | None:
    if not relative:
        return None
    try:
        base = _tenant_root(tenant, deps=deps)
        candidate = (base / pathlib.Path(str(relative))).resolve(strict=False)
    except Exception:
        return None
    try:
        base_resolved = base.resolve(strict=False)
    except Exception:
        base_resolved = base
    if base_resolved in candidate.parents or candidate == base_resolved:
        return candidate
    return None


def _catalog_path_from_entries(
    tenant: int,
    cfg: dict[str, Any],
    *,
    deps: ClientCatalogDeps,
) -> tuple[pathlib.Path | None, str | None, str | None]:
    catalogs = cfg.get("catalogs") if isinstance(cfg.get("catalogs"), list) else []
    for entry in catalogs:
        if not isinstance(entry, dict):
            continue
        csv_rel = entry.get("csv_path") or (
            entry.get("path") if entry.get("type") == "csv" else None
        )
        from_index = False
        if not csv_rel and entry.get("type") == "pdf" and entry.get("index_path"):
            csv_rel = str(pathlib.Path(entry["index_path"]).with_suffix(".csv"))
            from_index = True
        candidate = _safe_path(tenant, csv_rel, deps=deps)
        if candidate and candidate.exists():
            encoding = "utf-8" if from_index else entry.get("encoding", "utf-8")
            encoding = encoding if isinstance(encoding, str) else "utf-8"
            return candidate, encoding, _relative_to_tenant(tenant, candidate, deps=deps)
    return None, None, None


def _catalog_path_from_uploaded_meta(
    tenant: int,
    cfg: dict[str, Any],
    *,
    deps: ClientCatalogDeps,
) -> tuple[pathlib.Path | None, str | None, str | None]:
    try:
        integrations = cfg.get("integrations") if isinstance(cfg, dict) else {}
        uploaded = integrations.get("uploaded_catalog") if isinstance(integrations, dict) else {}
        if isinstance(uploaded, dict) and uploaded.get("csv_path"):
            candidate = _safe_path(tenant, uploaded.get("csv_path"), deps=deps)
            if candidate and candidate.exists():
                return candidate, "utf-8-sig", _relative_to_tenant(tenant, candidate, deps=deps)
    except Exception:
        pass
    return None, None, None


def _catalog_path_from_directory(
    tenant: int,
    *,
    deps: ClientCatalogDeps,
) -> tuple[pathlib.Path | None, str | None, str | None]:
    try:
        tenant_root = _tenant_root(tenant, deps=deps)
        catalogs_dir = tenant_root / "catalogs"
        if not catalogs_dir.exists() or not catalogs_dir.is_dir():
            return None, None, None
        discovered: list[pathlib.Path] = []
        preferred = catalogs_dir / "catalog.csv"
        if preferred.exists() and preferred.is_file():
            discovered.append(preferred)
        for candidate in sorted(catalogs_dir.glob("*.csv")):
            if candidate.is_file() and candidate not in discovered:
                discovered.append(candidate)
        if discovered:
            candidate = discovered[0]
            return candidate, "utf-8-sig", _relative_to_tenant(tenant, candidate, deps=deps)
    except Exception:
        pass
    return None, None, None


def _relative_to_tenant(tenant: int, path: pathlib.Path, *, deps: ClientCatalogDeps) -> str:
    try:
        return str(path.relative_to(_tenant_root(tenant, deps=deps)))
    except Exception:
        return str(path)


def _normalize_uploaded_catalog(
    *,
    tenant: int,
    filename: str,
    ext: str,
    raw: bytes,
    dest_path: pathlib.Path,
    tenant_root: pathlib.Path,
    deps: ClientCatalogDeps,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    public_module = deps.public_module_fn()
    if ext == ".csv":
        return public_module._read_csv_bytes(raw)
    if ext in {".xlsx", ".xls"}:
        return public_module._read_excel_bytes(raw)
    saved_rel = dest_path.relative_to(tenant_root)
    normalized_rows, meta, _ = public_module._process_pdf(
        tenant=int(tenant),
        saved_path=dest_path,
        tenant_root=tenant_root,
        saved_rel_path=saved_rel,
        original_name=filename,
    )
    return normalized_rows, meta


def _update_catalog_config(
    *,
    tenant: int,
    filename: str,
    ext: str,
    safe_name: str,
    relative_path: str,
    csv_rel_path: str,
    raw_size: int,
    meta: dict[str, Any],
    deps: ClientCatalogDeps,
) -> dict[str, Any]:
    cfg_raw = deps.common_module.read_tenant_config(tenant)
    cfg = dict(cfg_raw) if isinstance(cfg_raw, dict) else {}
    catalogs = cfg.get("catalogs") if isinstance(cfg.get("catalogs"), list) else []
    catalog_type = "pdf" if ext == ".pdf" else ("excel" if ext in {".xlsx", ".xls"} else "csv")
    entry = _catalog_entry(relative_path, csv_rel_path, catalog_type, meta)
    cfg["catalogs"] = [entry] + [
        e for e in catalogs if isinstance(e, dict) and e.get("path") != relative_path
    ]
    integrations = dict(cfg.get("integrations")) if isinstance(cfg.get("integrations"), dict) else {}
    cfg["integrations"] = integrations
    integrations["uploaded_catalog"] = _uploaded_catalog_meta(
        filename=filename,
        safe_name=safe_name,
        relative_path=relative_path,
        csv_rel_path=csv_rel_path,
        catalog_type=catalog_type,
        raw_size=raw_size,
        meta=meta,
        deps=deps,
    )
    return cfg


def _catalog_entry(
    relative_path: str,
    csv_rel_path: str,
    catalog_type: str,
    meta: dict[str, Any],
) -> dict[str, object]:
    entry: dict[str, object] = {
        "name": "uploaded",
        "path": relative_path,
        "type": catalog_type,
        "csv_path": csv_rel_path,
    }
    if isinstance(meta, dict):
        if meta.get("encoding"):
            entry["encoding"] = meta.get("encoding")
        if meta.get("delimiter") is not None:
            entry["delimiter"] = meta.get("delimiter")
        for key in ("index_path", "indexed_at", "chunk_count", "sha1"):
            if meta.get(key) is not None:
                entry[key] = meta.get(key)
    return entry


def _uploaded_catalog_meta(
    *,
    filename: str,
    safe_name: str,
    relative_path: str,
    csv_rel_path: str,
    catalog_type: str,
    raw_size: int,
    meta: dict[str, Any],
    deps: ClientCatalogDeps,
) -> dict[str, object]:
    del safe_name
    uploaded_meta: dict[str, object] = {
        "path": relative_path,
        "original": filename,
        "uploaded_at": int(deps.time_module.time()),
        "type": catalog_type,
        "size": raw_size,
        "mime": (mimetypes.guess_type(filename)[0] or "application/octet-stream"),
        "csv_path": csv_rel_path,
    }
    if isinstance(meta, dict):
        if meta.get("pipeline"):
            uploaded_meta["pipeline"] = meta.get("pipeline")
        if catalog_type == "csv" and meta.get("encoding"):
            uploaded_meta["encoding"] = meta.get("encoding")
        if catalog_type == "csv" and meta.get("delimiter") is not None:
            uploaded_meta["delimiter"] = meta.get("delimiter")
        extraction_meta = meta.get("extraction")
        if isinstance(extraction_meta, dict):
            uploaded_meta["extraction"] = extraction_meta
        if catalog_type == "pdf":
            idx = {
                "path": meta.get("index_path"),
                "generated_at": meta.get("indexed_at"),
                "chunks": meta.get("chunk_count"),
                "pages": meta.get("page_count"),
                "sha1": meta.get("sha1"),
            }
            idx = {k: v for k, v in idx.items() if v is not None}
            if idx:
                uploaded_meta["index"] = idx
    return uploaded_meta


def _wants_html_redirect(request: Request) -> bool:
    accept_header = (request.headers.get("accept") or "").lower()
    sec_fetch_mode = (request.headers.get("sec-fetch-mode") or "").lower()
    sec_fetch_dest = (request.headers.get("sec-fetch-dest") or "").lower()
    wants_html = (
        "text/html" in accept_header
        or "application/xhtml+xml" in accept_header
        or sec_fetch_mode == "navigate"
        or sec_fetch_dest == "document"
    )
    return wants_html and (request.headers.get("x-requested-with", "").lower() != "xmlhttprequest")


def _read_csv_header(reader: Any, *, deps: ClientCatalogDeps) -> list[str] | None:
    for raw_header in reader:
        if not raw_header:
            continue
        cleaned_header = []
        for idx, cell in enumerate(raw_header):
            value = cell if isinstance(cell, str) else ("" if cell is None else str(cell))
            if idx == 0:
                value = deps.strip_bom_fn(value)
            cleaned_header.append(value)
        if any((value or "").strip() for value in cleaned_header):
            return cleaned_header
    return None


def _read_csv_rows(
    reader: Any,
    header_raw: list[str],
    *,
    deps: ClientCatalogDeps,
) -> tuple[list[str], list[list[str]]]:
    columns = _normalized_columns(header_raw, deps=deps)
    data_rows: list[list[str]] = []
    for row in reader:
        if not row:
            continue
        cleaned_cells = []
        for idx, value in enumerate(row):
            cell_text = value if isinstance(value, str) else ("" if value is None else str(value))
            if idx == 0:
                cell_text = deps.strip_bom_fn(cell_text)
            cleaned_cells.append(cell_text)
        trimmed_cells = [cell.strip() for cell in cleaned_cells]
        if not any(trimmed_cells):
            continue
        non_empty = [cell for cell in trimmed_cells if cell]
        if len(non_empty) == 1 and non_empty[0] == ".":
            continue
        while len(columns) < len(cleaned_cells):
            columns.append(f"column_{len(columns) + 1}")
        data_rows.append(
            [trimmed_cells[idx_col] if idx_col < len(trimmed_cells) else "" for idx_col in range(len(columns))]
        )
    return columns, data_rows


def _normalized_columns(header_raw: list[str], *, deps: ClientCatalogDeps) -> list[str]:
    normalized = []
    seen: dict[str, int] = {}
    for idx, cell in enumerate(header_raw):
        raw_value = cell or ""
        if not isinstance(raw_value, str):
            raw_value = str(raw_value)
        name = deps.strip_bom_fn(raw_value).strip() or f"column_{idx + 1}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        normalized.append(name)
    return normalized


def _merge_duplicate_columns(
    columns: list[str],
    data_rows: list[list[str]],
) -> tuple[list[str], list[list[str]]]:
    base_to_indices: dict[str, list[int]] = {}
    for idx, col in enumerate(columns):
        base = re.sub(r"_(\d+)$", "", col)
        base_to_indices.setdefault(base, []).append(idx)
    merged_columns = []
    seen_bases = set()
    for col in columns:
        base = re.sub(r"_(\d+)$", "", col)
        if base in seen_bases:
            continue
        seen_bases.add(base)
        merged_columns.append(base)
    if not any(len(idxs) > 1 for idxs in base_to_indices.values()):
        return columns, data_rows
    merged_rows = []
    for row in data_rows:
        merged_row = []
        for base in merged_columns:
            values = []
            for index in base_to_indices.get(base, []):
                if index < len(row):
                    text = row[index].strip() if isinstance(row[index], str) else str(row[index] or "").strip()
                    if text and text not in values:
                        values.append(text)
            merged_row.append(" ".join(values))
        merged_rows.append(merged_row)
    return merged_columns, merged_rows


def _drop_empty_columns(
    columns: list[str],
    data_rows: list[list[str]],
) -> tuple[list[str], list[list[str]]]:
    if not data_rows:
        return columns, data_rows
    keep_idx = []
    for idx in range(len(columns)):
        any_non_empty = any(
            (row[idx].strip() if isinstance(row[idx], str) else str(row[idx] or "").strip())
            for row in data_rows
            if idx < len(row)
        )
        if any_non_empty:
            keep_idx.append(idx)
    if keep_idx and len(keep_idx) < len(columns):
        columns = [columns[i] for i in keep_idx]
        data_rows = [[(row[i] if i < len(row) else "") for i in keep_idx] for row in data_rows]
    return columns, data_rows


def _serializable_rows(columns: list[str], rows: list[Any]) -> list[list[str]]:
    serializable_rows = []
    for row in rows:
        if isinstance(row, dict):
            serializable_rows.append([str(row.get(col, "") or "") for col in columns])
        elif isinstance(row, list):
            serializable_rows.append([str(row[idx]) if idx < len(row) else "" for idx in range(len(columns))])
        else:
            raise ValueError("invalid_row")
    return serializable_rows


def _clean_csv_cell(cell: Any) -> str:
    text = str(cell or "")
    if not text:
        return text
    text = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return re.sub(r"\s+", " ", text).strip()
