from __future__ import annotations

import base64
import mimetypes
import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse, urlsplit, urlunsplit

import httpx


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class AttachmentRuntimeDeps:
    tg_pdf_fast_enabled: bool
    tg_pdf_fast_min_mb: float
    tg_pdf_fast_target_mb: float
    tg_pdf_fast_suffix: str
    is_internal_path_fn: SyncFn
    normalize_internal_urls_fn: SyncFn
    download_internal_attachment_fn: AsyncFn
    resolve_attachment_filename_fn: SyncFn
    resolve_attachment_mime_fn: SyncFn
    compress_pdf_bytes_fn: SyncFn


def prepare_whatsapp_attachment_url(url: str, *, deps: AttachmentRuntimeDeps) -> str:
    cleaned = (url or "").strip()
    if not cleaned:
        return ""
    if deps.is_internal_path_fn(cleaned):
        _, absolute = deps.normalize_internal_urls_fn(cleaned)
        return absolute
    return cleaned


def tokenize_attachment_mapping(
    attachment: Mapping[str, Any],
    *,
    deps: AttachmentRuntimeDeps,
) -> dict[str, Any]:
    prepared = dict(attachment)
    path_value = prepared.get("path")
    if isinstance(path_value, str) and path_value.strip():
        try:
            resolved_path = os.path.abspath(path_value)
            size = os.path.getsize(resolved_path)
            if size >= 0:
                prepared.setdefault("path", resolved_path)
                prepared.setdefault("size", size)
                url_value = prepared.get("url")
                if isinstance(url_value, str) and deps.is_internal_path_fn(url_value):
                    prepared["internal_url"] = url_value
                    prepared.pop("url", None)
        except OSError:
            pass

    url_value = prepared.get("url")
    if isinstance(url_value, str):
        prepared["url"] = prepare_whatsapp_attachment_url(url_value, deps=deps)
    for nested_key in ("document", "image", "video", "audio", "voice", "thumbnail"):
        nested_value = prepared.get(nested_key)
        if isinstance(nested_value, Mapping):
            nested_copy = dict(nested_value)
            nested_url = nested_copy.get("url")
            if isinstance(nested_url, str):
                nested_copy["url"] = prepare_whatsapp_attachment_url(nested_url, deps=deps)
            prepared[nested_key] = nested_copy
    return prepared


async def prepare_internal_attachment(
    attachment: Mapping[str, Any],
    *,
    deps: AttachmentRuntimeDeps,
) -> dict[str, Any]:
    if not isinstance(attachment, Mapping):
        return dict(attachment)
    url = attachment.get("url")
    if not isinstance(url, str):
        return dict(attachment)
    trimmed = url.strip()
    if not deps.is_internal_path_fn(trimmed):
        return tokenize_attachment_mapping(attachment, deps=deps)

    data, headers, absolute_url = await deps.download_internal_attachment_fn(trimmed)
    prepared = dict(attachment)
    prepared["url"] = absolute_url

    if data is None:
        return tokenize_attachment_mapping(prepared, deps=deps)

    filename = deps.resolve_attachment_filename_fn(prepared, headers, absolute_url)
    if filename:
        prepared["filename"] = filename
        prepared.setdefault("name", filename)

    mime = deps.resolve_attachment_mime_fn(prepared, headers)
    if mime:
        prepared["mime"] = mime
        prepared["mime_type"] = mime
        prepared["mimetype"] = mime

    prepared["type"] = str(prepared.get("type") or "document")
    prepared["sendMediaAsDocument"] = True
    prepared.setdefault("size", len(data))

    inline_limit_mb = float(os.getenv("WA_INLINE_ATTACHMENT_LIMIT_MB", "8") or "0")
    inline_limit_bytes = int(inline_limit_mb * 1024 * 1024) if inline_limit_mb > 0 else 0

    working_data = data
    if (
        inline_limit_bytes
        and len(working_data) > inline_limit_bytes
        and isinstance(mime, str)
        and "pdf" in mime.lower()
    ):
        compressed = deps.compress_pdf_bytes_fn(working_data, filename or "catalog.pdf", inline_limit_bytes)
        if compressed and len(compressed) < len(working_data):
            working_data = compressed
            prepared["size"] = len(working_data)

    if inline_limit_bytes and len(working_data) > inline_limit_bytes:
        document_meta = {"url": absolute_url}
        if filename:
            document_meta["filename"] = filename
        fallback_mime = "application/octet-stream"
        prepared["mime"] = fallback_mime
        prepared["mime_type"] = fallback_mime
        prepared["mimetype"] = fallback_mime
        document_meta["mime"] = fallback_mime
        caption_value = prepared.get("caption") or prepared.get("text")
        if isinstance(caption_value, str) and caption_value.strip():
            document_meta["caption"] = caption_value.strip()
        prepared.pop("b64", None)
        prepared["document"] = document_meta
        prepared["source"] = "url"
        return tokenize_attachment_mapping(prepared, deps=deps)

    prepared["size"] = len(working_data)
    prepared["b64"] = base64.b64encode(working_data).decode("ascii")
    return tokenize_attachment_mapping(prepared, deps=deps)


def tg_fast_pdf_cache_path(source_path: pathlib.Path, *, deps: AttachmentRuntimeDeps) -> pathlib.Path:
    if source_path.suffix.lower() == ".pdf":
        return source_path.with_suffix(deps.tg_pdf_fast_suffix)
    return pathlib.Path(f"{source_path}{deps.tg_pdf_fast_suffix}")


def prepare_tg_attachment_fast_pdf(
    attachment: Mapping[str, Any],
    *,
    deps: AttachmentRuntimeDeps,
) -> dict[str, Any]:
    prepared = dict(attachment)
    if not deps.tg_pdf_fast_enabled:
        return prepared

    path_raw = prepared.get("path")
    if not isinstance(path_raw, str) or not path_raw.strip():
        return prepared

    mime_raw = (
        str(prepared.get("mime") or prepared.get("mime_type") or prepared.get("mimetype") or "")
        .strip()
        .lower()
    )
    filename_raw = str(prepared.get("filename") or prepared.get("name") or "").strip().lower()
    is_pdf = "pdf" in mime_raw or filename_raw.endswith(".pdf")
    if not is_pdf:
        return prepared

    try:
        src_path = pathlib.Path(path_raw).expanduser().resolve()
    except Exception:
        return prepared
    if not src_path.is_file():
        return prepared

    try:
        src_stat = src_path.stat()
    except OSError:
        return prepared

    min_bytes = int(deps.tg_pdf_fast_min_mb * 1024 * 1024)
    if src_stat.st_size <= min_bytes:
        return prepared

    target_bytes = int(deps.tg_pdf_fast_target_mb * 1024 * 1024)
    if target_bytes <= 0:
        target_bytes = 8 * 1024 * 1024

    cache_path = tg_fast_pdf_cache_path(src_path, deps=deps)
    try:
        if cache_path.is_file():
            cache_stat = cache_path.stat()
            if cache_stat.st_mtime >= src_stat.st_mtime and 0 < cache_stat.st_size < src_stat.st_size:
                prepared["path"] = str(cache_path)
                prepared["size"] = int(cache_stat.st_size)
                return prepared
    except OSError:
        pass

    try:
        with open(src_path, "rb") as fh:
            source_data = fh.read()
    except OSError:
        return prepared

    compressed = deps.compress_pdf_bytes_fn(source_data, src_path.name, target_bytes)
    if not compressed or len(compressed) >= len(source_data):
        return prepared

    tmp_path = pathlib.Path(f"{cache_path}.tmp")
    try:
        with open(tmp_path, "wb") as fh:
            fh.write(compressed)
        os.replace(str(tmp_path), str(cache_path))
        prepared["path"] = str(cache_path)
        prepared["size"] = len(compressed)
        return prepared
    except OSError:
        return prepared
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass


def prepare_tg_attachments_for_send(
    tenant_id: int,
    attachments: Any,
    *,
    deps: AttachmentRuntimeDeps,
) -> list[dict[str, Any]]:
    _ = tenant_id
    prepared: list[dict[str, Any]] = []
    for item in attachments:
        if not isinstance(item, Mapping):
            continue
        candidate = prepare_tg_attachment_fast_pdf(dict(item), deps=deps)
        prepared.append(candidate)
    return prepared


def iter_tenants_with_catalog_pdf(tenants_root: pathlib.Path) -> list[tuple[int, pathlib.Path]]:
    rows: list[tuple[int, pathlib.Path]] = []
    try:
        if not tenants_root.is_dir():
            return rows
        for child in sorted(tenants_root.iterdir()):
            if not child.is_dir() or not child.name.isdigit():
                continue
            tenant_id = int(child.name)
            catalog_pdf = child / "uploads" / "catalog.pdf"
            if catalog_pdf.is_file():
                rows.append((tenant_id, catalog_pdf))
    except Exception:
        return rows
    return rows


def warmup_single_tg_fast_pdf(
    tenant_id: int,
    catalog_pdf: pathlib.Path,
    *,
    deps: AttachmentRuntimeDeps,
    log_fn: SyncFn,
) -> bool:
    before_size = _path_size(catalog_pdf)
    if before_size <= 0:
        return False
    attachment: dict[str, Any] = {
        "path": str(catalog_pdf),
        "filename": catalog_pdf.name,
        "mime": "application/pdf",
        "mime_type": "application/pdf",
        "mimetype": "application/pdf",
        "type": "document",
    }
    prepared = prepare_tg_attachment_fast_pdf(attachment, deps=deps)
    fast_path = str(prepared.get("path") or "")
    if not fast_path or fast_path == str(catalog_pdf):
        return False
    after_size = _path_size(pathlib.Path(fast_path))
    if after_size <= 0:
        return False
    log_fn(
        "event=tg_pdf_fast_warmup_done tenant=%s src_size=%s fast_size=%s path=%s"
        % (tenant_id, before_size, after_size, fast_path)
    )
    return True


async def warmup_tg_fast_pdf_cache_once(
    *,
    enabled: bool,
    warmup_enabled: bool,
    delay_seconds: float,
    tenants_root: pathlib.Path,
    deps: AttachmentRuntimeDeps,
    log_fn: SyncFn,
    sleep_fn: AsyncFn,
    to_thread_fn: AsyncFn,
) -> None:
    if not enabled or not warmup_enabled:
        return
    if delay_seconds > 0:
        await sleep_fn(delay_seconds)
    rows = iter_tenants_with_catalog_pdf(tenants_root)
    if not rows:
        return
    log_fn(f"event=tg_pdf_fast_warmup_start catalogs={len(rows)}")
    for tenant_id, catalog_pdf in rows:
        try:
            await to_thread_fn(
                warmup_single_tg_fast_pdf,
                tenant_id,
                catalog_pdf,
                deps=deps,
                log_fn=log_fn,
            )
        except Exception as exc:
            log_fn(
                "event=tg_pdf_fast_warmup_fail tenant=%s path=%s err=%s"
                % (tenant_id, catalog_pdf, exc)
            )
    log_fn("event=tg_pdf_fast_warmup_finish")


def _path_size(path: pathlib.Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return 0


def resolve_gs_path(*, compress_bin: str | None = None, env_bin: str | None = None) -> str | None:
    candidates: list[str] = []
    if env_bin:
        candidates.append(env_bin)
    if compress_bin:
        candidates.append(compress_bin)
    candidates.extend(["/usr/bin/gs", "/usr/local/bin/gs"])
    for candidate in candidates:
        if not candidate:
            continue
        if os.path.isabs(candidate):
            if os.path.exists(candidate) and os.access(candidate, os.X_OK):
                return candidate
        else:
            found = shutil.which(candidate)
            if found:
                return found
    return None


def compress_pdf_bytes(
    data: bytes,
    filename: str,
    target_bytes: int,
    *,
    enabled: bool,
    compress_bin: str | None,
    settings: str,
    timeout: int,
    env_bin: str | None = None,
) -> bytes | None:
    if not enabled or not data or target_bytes <= 0:
        return None
    gs_path = resolve_gs_path(compress_bin=compress_bin, env_bin=env_bin)
    if not gs_path:
        return None
    src_path = out_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as src_file:
            src_file.write(data)
            src_path = src_file.name
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as out_file:
            out_path = out_file.name
        _run_pdf_compress(
            gs_path=gs_path,
            src_path=src_path,
            out_path=out_path,
            settings=settings,
            timeout=timeout,
        )
        with open(out_path, "rb") as fh:
            compressed = fh.read()
        if not compressed or len(compressed) >= len(data):
            return None
        return compressed
    except (subprocess.SubprocessError, OSError, ValueError):
        return None
    finally:
        _remove_temp_file(src_path)
        _remove_temp_file(out_path)


def _run_pdf_compress(
    *,
    gs_path: str,
    src_path: str,
    out_path: str,
    settings: str,
    timeout: int,
) -> None:
    subprocess.run(
        [
            gs_path,
            "-sDEVICE=pdfwrite",
            "-dCompatibilityLevel=1.4",
            f"-dPDFSETTINGS={settings}",
            "-dNOPAUSE",
            "-dQUIET",
            "-dBATCH",
            f"-sOutputFile={out_path}",
            src_path,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout,
    )


def _remove_temp_file(path: str | None) -> None:
    if not path:
        return
    try:
        os.remove(path)
    except OSError:
        pass


def download_file(
    url: str,
    *,
    timeout: float = 15.0,
    max_size: int = 20 * 1024 * 1024,
    tgworker_url: str = "http://tgworker:8000",
    admin_token: str = "",
) -> tuple[bytes | None, str | None, str | None]:
    if not url:
        return None, None, None
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme == "telegram":
        return _download_telegram_file(
            parsed,
            timeout=timeout,
            max_size=max_size,
            tgworker_url=tgworker_url,
            admin_token=admin_token,
        )
    return _download_http_file(url, parsed, timeout=timeout, max_size=max_size)


def _download_telegram_file(
    parsed: urllib.parse.ParseResult,
    *,
    timeout: float,
    max_size: int,
    tgworker_url: str,
    admin_token: str,
) -> tuple[bytes | None, str | None, str | None]:
    tenant = parsed.netloc
    parts = parsed.path.strip("/").split("/")
    if len(parts) < 2:
        return None, None, None
    req_url = f"{tgworker_url.rstrip('/')}/media/{tenant}/{parts[0]}/{parts[1]}"
    headers = {"X-Admin-Token": str(admin_token)} if admin_token else {}
    try:
        req = urllib.request.Request(req_url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read(max_size + 1)
            if len(data) > max_size:
                return None, None, None
            content_type = resp.headers.get("Content-Type") if resp.headers else None
            filename = _response_filename(resp, parsed.path, content_type)
            return data, filename, content_type
    except Exception:
        return None, None, None


def _download_http_file(
    url: str,
    parsed: urllib.parse.ParseResult,
    *,
    timeout: float,
    max_size: int,
) -> tuple[bytes | None, str | None, str | None]:
    name = os.path.basename(parsed.path or "") or "attachment"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = resp.read(max_size + 1)
            if len(data) > max_size:
                return None, name, None
            content_type = resp.headers.get("Content-Type") if resp.headers else None
            return data, _filename_with_content_type_extension(name, content_type), content_type
    except Exception:
        return None, name, None


def _response_filename(resp: Any, fallback_path: str, content_type: str | None) -> str:
    filename = None
    disposition = resp.headers.get("Content-Disposition") if resp.headers else None
    if disposition:
        filename = parse_disposition_filename(disposition)
    if not filename:
        filename = os.path.basename(fallback_path) or "attachment"
    return _filename_with_content_type_extension(filename, content_type)


def _filename_with_content_type_extension(name: str, content_type: str | None) -> str:
    if name and "." not in name and content_type:
        ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if ext:
            return f"{name}{ext}"
    return name


def is_internal_path(value: str) -> bool:
    trimmed = (value or "").strip()
    if not trimmed:
        return False
    if trimmed.startswith("/internal/"):
        return True
    parsed = urlsplit(trimmed)
    path = parsed.path or ""
    return path.startswith("/internal/")


def normalize_internal_urls(
    relative_url: str,
    *,
    token_value: str,
    internal_base_url: str = "http://app:8000",
) -> tuple[str, str]:
    parsed = urlsplit(relative_url)
    query = inject_internal_token(parsed.query, token_value=token_value)
    fragment = parsed.fragment

    if parsed.scheme and parsed.netloc:
        absolute = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, fragment))
        path = parsed.path or ""
        relative = urlunsplit(("", "", path, query, fragment))
        if not relative.startswith("/"):
            relative = f"/{relative.lstrip('/')}"
        return ensure_inline_hint(relative), ensure_inline_hint(absolute)

    path = parsed.path or ""
    if not path.startswith("/"):
        path = f"/{path}"
    relative = ensure_inline_hint(urlunsplit(("", "", path, query, fragment)))
    absolute = ensure_inline_hint(f"{internal_base_url.rstrip('/')}{relative}")
    return relative, absolute


def inject_internal_token(query: str, *, token_value: str) -> str:
    if not token_value:
        return query

    filtered: list[str] = []
    for chunk in query.split("&"):
        if not chunk:
            continue
        key, sep, value = chunk.partition("=")
        if key.lower() == "token":
            continue
        filtered.append(f"{key}{sep}{value}" if sep else key)

    filtered.append(f"token={quote(token_value, safe='')}")
    return "&".join(filtered)


def ensure_inline_hint(url: str) -> str:
    if not url:
        return url
    try:
        parsed = urlsplit(url)
    except Exception:
        return url
    path = (parsed.path or "").lower()
    if "/catalog-file" not in path:
        return url
    existing = parse_qsl(parsed.query, keep_blank_values=True)
    if any(key.lower() == "inline" for key, _ in existing):
        return url
    existing.append(("inline", "1"))
    new_query = urlencode(existing, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, new_query, parsed.fragment))


def parse_disposition_filename(header: str | None) -> str:
    if not header:
        return ""
    match = re.search(r"filename\*=UTF-8''([^;]+)", header, flags=re.IGNORECASE)
    if match and match.group(1):
        try:
            return unquote(match.group(1))
        except Exception:
            return match.group(1)
    match = re.search(r'filename="?([^";]+)"?', header, flags=re.IGNORECASE)
    if match and match.group(1):
        return match.group(1)
    return ""


def resolve_attachment_filename(
    attachment: Mapping[str, Any],
    headers: Mapping[str, str] | None,
    absolute_url: str,
) -> str:
    for key in ("filename", "name"):
        candidate = attachment.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    disposition = ""
    if headers:
        disposition = headers.get("Content-Disposition") or headers.get("content-disposition") or ""
    candidate = parse_disposition_filename(disposition)
    if candidate:
        return candidate
    path = urlparse(absolute_url).path
    if path:
        tail = path.rstrip("/").split("/")[-1]
        if tail:
            return unquote(tail)
    return ""


def resolve_attachment_mime(
    attachment: Mapping[str, Any], headers: Mapping[str, str] | None
) -> str:
    for key in ("mime", "mime_type", "mimetype"):
        candidate = attachment.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    if headers:
        content_type = headers.get("Content-Type") or headers.get("content-type")
        if content_type:
            return content_type.split(";", 1)[0].strip()
    return ""


def has_photo_attachment(
    blobs: Any,
    *,
    normalize_attachments_fn: SyncFn,
    normalize_attachment_fn: SyncFn,
) -> bool:
    raw_items = list(blobs or [])
    normalized = _normalize_attachment_items(
        raw_items,
        normalize_attachments_fn=normalize_attachments_fn,
        normalize_attachment_fn=normalize_attachment_fn,
    )
    return _contains_photo_like_attachment(normalized) or _contains_photo_like_attachment(raw_items)


def _normalize_attachment_items(
    raw_items: list[Any],
    *,
    normalize_attachments_fn: SyncFn,
    normalize_attachment_fn: SyncFn,
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for blob in raw_items:
        if not isinstance(blob, Mapping):
            continue
        item = normalize_attachment_fn(dict(blob))
        if item:
            prepared.append(item)
    return list(normalize_attachments_fn(prepared) or [])


def _contains_photo_like_attachment(items: list[Any]) -> bool:
    for item in items:
        if not isinstance(item, Mapping):
            continue
        type_raw = str(item.get("type") or item.get("kind") or "").strip().lower()
        mime_raw = (
            str(item.get("mime") or item.get("mime_type") or item.get("mimetype") or "")
            .strip()
            .lower()
        )
        if type_raw in {"image", "photo", "picture"} or "photo" in type_raw:
            return True
        if mime_raw.startswith("image/"):
            return True
    return False


async def download_internal_attachment(
    relative_url: str,
    *,
    token_value: str,
    normalize_internal_urls_fn: SyncFn,
    log_fn: SyncFn,
) -> tuple[bytes | None, Mapping[str, str] | None, str]:
    normalized_relative, absolute_url = normalize_internal_urls_fn(relative_url)
    timeout = httpx.Timeout(20.0, connect=5.0)
    final_headers: Mapping[str, str] | None = None
    final_status: int | None = None
    error_label: str | None = None

    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt_index, (header_label, headers) in enumerate(_internal_header_attempts(token_value), start=1):
            log_fn(
                "event=internal_download level=info action=request "
                f"attempt={attempt_index} url={normalized_relative} header={header_label or 'none'}"
            )
            try:
                response = await client.get(absolute_url, headers=headers)
            except httpx.HTTPError as exc:
                error_label = exc.__class__.__name__
                log_fn(
                    "event=internal_download level=info action=error "
                    f"attempt={attempt_index} url={normalized_relative} error={error_label}"
                )
                continue
            final_status = response.status_code
            final_headers = response.headers
            log_fn(
                "event=internal_download level=info action=response "
                f"attempt={attempt_index} url={normalized_relative} status={final_status}"
            )
            if 200 <= response.status_code < 300:
                return response.content, response.headers, absolute_url
            if not _should_retry_internal_download(token_value, response.status_code, header_label):
                break

    _log_internal_download_fetch_status(
        normalized_relative,
        final_status=final_status,
        error_label=error_label,
        log_fn=log_fn,
    )
    return None, final_headers, absolute_url


def _internal_header_attempts(token_value: str) -> list[tuple[str, Mapping[str, str] | None]]:
    if not token_value:
        return [("", None)]
    return [
        ("X-Auth-Token", {"X-Auth-Token": token_value}),
        ("X-Internal-Token", {"X-Internal-Token": token_value}),
    ]


def _should_retry_internal_download(token_value: str, status_code: int, header_label: str) -> bool:
    return bool(token_value and status_code in {401, 403} and header_label == "X-Auth-Token")


def _log_internal_download_fetch_status(
    normalized_relative: str,
    *,
    final_status: int | None,
    error_label: str | None,
    log_fn: SyncFn,
) -> None:
    if final_status is None and not error_label:
        return
    status_hint = error_label or final_status or "error"
    log_fn(
        "event=internal_download level=info action=fetch "
        f"url={normalized_relative} status={status_hint}"
    )


def build_wa_document_payload(
    attachment: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not isinstance(attachment, Mapping):
        return None, None

    path_value = attachment.get("path")
    if isinstance(path_value, str) and path_value.strip():
        return None, None

    attachment_type = str(attachment.get("type") or attachment.get("kind") or "").strip().lower()
    if attachment_type and attachment_type not in {"document", "file"}:
        return None, None

    url = _first_attachment_text(attachment, "url", "href", "document", "file", "path")
    if not url:
        return None, None

    filename = _first_attachment_text(attachment, "filename", "name", "title")
    mime = _first_attachment_text(attachment, "mime", "mime_type", "mimetype", "content_type")
    caption = _first_attachment_text(attachment, "caption", "text", "description")

    document_block = _document_block(url, filename=filename, mime=mime, caption=caption)
    wa_attachment = _wa_document_attachment(attachment, document_block, url=url)
    return wa_attachment, document_block


def _first_attachment_text(attachment: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = attachment.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _document_block(url: str, *, filename: str, mime: str, caption: str) -> dict[str, Any]:
    document_block: dict[str, Any] = {"url": url}
    if filename:
        document_block["filename"] = filename
    if mime:
        document_block["mime"] = mime
    if caption:
        document_block["caption"] = caption
    return document_block


def _wa_document_attachment(
    attachment: Mapping[str, Any],
    document_block: Mapping[str, Any],
    *,
    url: str,
) -> dict[str, Any]:
    wa_attachment: dict[str, Any] = {
        "type": "document",
        "document": dict(document_block),
        "url": url,
    }
    _copy_document_text_metadata(wa_attachment, document_block)
    _copy_document_passthrough_metadata(wa_attachment, attachment)
    return wa_attachment


def _copy_document_text_metadata(
    wa_attachment: dict[str, Any],
    document_block: Mapping[str, Any],
) -> None:
    filename = document_block.get("filename")
    mime = document_block.get("mime")
    caption = document_block.get("caption")
    if filename:
        wa_attachment["filename"] = filename
        wa_attachment.setdefault("name", filename)
    if mime:
        wa_attachment["mime"] = mime
        wa_attachment.setdefault("mime_type", mime)
        wa_attachment.setdefault("mimetype", mime)
    if caption:
        wa_attachment["caption"] = caption


def _copy_document_passthrough_metadata(
    wa_attachment: dict[str, Any],
    attachment: Mapping[str, Any],
) -> None:
    if attachment.get("b64"):
        wa_attachment["b64"] = attachment.get("b64")
    if attachment.get("sendMediaAsDocument") is not None:
        wa_attachment["sendMediaAsDocument"] = attachment.get("sendMediaAsDocument")
    if attachment.get("source"):
        wa_attachment["source"] = attachment.get("source")
    size_value = attachment.get("size")
    try:
        size_int = int(size_value) if size_value is not None else None
    except Exception:
        size_int = None
    if size_int is not None and size_int >= 0:
        wa_attachment["size"] = size_int
