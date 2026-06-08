from __future__ import annotations

import asyncio
import json
import pathlib
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from libs.core.message_envelope import content_fingerprint


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
CounterIncFn = Callable[[str, str], None]
LogFn = Callable[[str], None]


@dataclass(frozen=True)
class MaxOutboundDeps:
    log_fn: LogFn
    prepare_tg_attachments_for_send_fn: SyncFn
    get_lead_peer_fn: AsyncFn
    tenant_dir_fn: SyncFn
    is_internal_path_fn: SyncFn
    download_internal_attachment_fn: AsyncFn
    resolve_attachment_filename_fn: SyncFn
    resolve_attachment_mime_fn: SyncFn
    download_file_fn: SyncFn
    max_integration_module: Any
    max_personal_service_module: Any
    max_personal_transport_module: Any
    message_out_counter: Any


@dataclass
class MaxSendState:
    tenant_id: int
    lead_id: int
    text_value: str
    attachments_list: list[dict[str, Any]]
    target_chat: str | int | None
    target_user: str | int | None
    deps: MaxOutboundDeps
    prepared_attachments: list[dict[str, Any]]


async def send_max(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: str | int | None = None,
    user_id: str | int | None = None,
    attachments: list[dict[str, Any]] | None = None,
    deps: MaxOutboundDeps,
) -> tuple[int, str]:
    text_value = (text or "").strip()
    attachments_list = deps.prepare_tg_attachments_for_send_fn(tenant_id, attachments or [])
    if not text_value and not attachments_list:
        return (0, "empty")
    state = MaxSendState(
        tenant_id=tenant_id,
        lead_id=lead_id,
        text_value=text_value,
        attachments_list=attachments_list,
        target_chat=chat_id,
        target_user=user_id,
        deps=deps,
        prepared_attachments=[],
    )
    await _resolve_max_target(state)
    if state.target_chat is None and state.target_user is None:
        deps.log_fn(
            f"event=send_result status=skipped reason=missing_chat channel=max tenant={tenant_id} lead_id={lead_id}"
        )
        return (0, "missing_chat")
    await _prepare_max_attachments(state)
    status_code, body = await _send_max_message(state)
    _count_max_result(status_code, deps=deps)
    return status_code, body


async def _resolve_max_target(state: MaxSendState) -> None:
    if state.target_chat is not None or state.target_user is not None or state.lead_id <= 0:
        return
    try:
        state.target_chat = await state.deps.get_lead_peer_fn(state.lead_id, channel="max")
    except Exception:
        state.target_chat = None


async def _prepare_max_attachments(state: MaxSendState) -> None:
    deps = state.deps
    mode = getattr(deps.max_integration_module, "MAX_ATTACHMENT_MODE", "url")
    upload_enabled = bool(getattr(deps.max_integration_module, "MAX_UPLOAD_ENDPOINT", "") or "")
    for item in state.attachments_list:
        if not isinstance(item, Mapping):
            continue
        meta = _max_attachment_meta(item)
        uploaded = await _try_upload_max_attachment(state, item, meta, mode, upload_enabled)
        if uploaded:
            continue
        _append_max_url_attachment(state, meta)


def _max_attachment_meta(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "type": str(item.get("type") or "image").strip().lower() or "image",
        "url": item.get("url") or item.get("public_url"),
        "path": item.get("path") or item.get("file_path"),
        "filename": item.get("filename") or item.get("name") or item.get("title") or item.get("path"),
        "mime": item.get("mime") or item.get("mime_type") or item.get("mimetype"),
    }


async def _try_upload_max_attachment(
    state: MaxSendState,
    item: Mapping[str, Any],
    meta: dict[str, Any],
    mode: str,
    upload_enabled: bool,
) -> bool:
    if mode != "upload" or not upload_enabled:
        return False
    content, headers, absolute_url = await _load_max_attachment_content(state, meta)
    if content is None:
        return False
    filename = meta.get("filename")
    mime = meta.get("mime")
    if not filename:
        filename = state.deps.resolve_attachment_filename_fn(item, headers, absolute_url)
    if not mime:
        mime = state.deps.resolve_attachment_mime_fn(item, headers)
    status, payload, err = await state.deps.max_integration_module.upload_file(
        tenant=int(state.tenant_id),
        filename=str(filename or "attachment"),
        content=content,
        mime=str(mime) if mime else None,
    )
    if 200 <= status < 300 and isinstance(payload, dict):
        state.prepared_attachments.append(_uploaded_max_attachment_payload(meta, payload))
        return True
    state.deps.log_fn(
        "event=max_upload_failed tenant=%s lead_id=%s status=%s error=%s"
        % (state.tenant_id, state.lead_id, status, err or "")
    )
    return False


async def _load_max_attachment_content(
    state: MaxSendState,
    meta: dict[str, Any],
) -> tuple[bytes | None, Mapping[str, str] | None, str]:
    path = meta.get("path")
    if isinstance(path, str) and path.strip():
        content, absolute_url = _read_max_attachment_path(state, path)
        if content is not None:
            return content, None, absolute_url
    url = meta.get("url")
    if not isinstance(url, str) or not url.strip():
        return None, None, ""
    if state.deps.is_internal_path_fn(url.strip()):
        return await state.deps.download_internal_attachment_fn(url.strip())
    content, fetched_name, fetched_mime = await asyncio.to_thread(
        state.deps.download_file_fn,
        url.strip(),
    )
    if content is not None:
        if not meta.get("filename"):
            meta["filename"] = fetched_name
        if not meta.get("mime"):
            meta["mime"] = fetched_mime
    return content, None, url.strip()


def _read_max_attachment_path(state: MaxSendState, path: str) -> tuple[bytes | None, str]:
    try:
        candidate = pathlib.Path(path).expanduser()
        if not candidate.is_absolute():
            candidate = state.deps.tenant_dir_fn(int(state.tenant_id)) / candidate
        resolved = candidate.resolve()
        if resolved.is_file():
            return resolved.read_bytes(), str(resolved)
    except Exception:
        pass
    return None, ""


def _uploaded_max_attachment_payload(meta: Mapping[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
    file_id = payload.get("file_id") or payload.get("fileId") or payload.get("id") or payload.get("fileID")
    file_url = payload.get("url") or payload.get("link")
    attachment_payload = {"type": meta.get("type") or "image"}
    if file_id:
        attachment_payload["file_id"] = file_id
    elif file_url:
        attachment_payload["url"] = file_url
    else:
        attachment_payload["url"] = meta.get("url") or ""
    return attachment_payload


def _append_max_url_attachment(state: MaxSendState, meta: Mapping[str, Any]) -> None:
    url = meta.get("url")
    if not isinstance(url, str) or not url.strip():
        return
    payload = {"type": meta.get("type") or "image", "url": url.strip()}
    if meta.get("filename"):
        payload["name"] = meta["filename"]
    if meta.get("mime"):
        payload["mime"] = meta["mime"]
    state.prepared_attachments.append(payload)


async def _send_max_message(state: MaxSendState) -> tuple[int, str]:
    return await state.deps.max_integration_module.send_message(
        int(state.tenant_id),
        chat_id=state.target_chat,
        user_id=state.target_user,
        text=state.text_value or None,
        attachments=state.prepared_attachments or None,
    )


def _count_max_result(status_code: int, *, deps: MaxOutboundDeps) -> None:
    label = "success" if 200 <= status_code < 300 else "error"
    deps.message_out_counter.labels("max", label).inc()


async def send_max_personal(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: str | int | None = None,
    user_id: str | int | None = None,
    attachments: list[dict[str, Any]] | None = None,
    message_id: str | None = None,
    deps: MaxOutboundDeps,
) -> tuple[int, str]:
    if not deps.max_personal_service_module.integration_enabled(int(tenant_id)):
        return (0, "integration_disabled")
    if not deps.max_personal_service_module.outbound_enabled(int(tenant_id)):
        return (0, "outbound_disabled")

    text_value = (text or "").strip()
    attachments_list = deps.prepare_tg_attachments_for_send_fn(int(tenant_id), attachments or [])
    if not text_value and not attachments_list:
        return (0, "empty")

    target_chat = chat_id
    if target_chat is None and user_id is not None:
        target_chat = user_id
    if target_chat is None and lead_id > 0:
        try:
            target_chat = await deps.get_lead_peer_fn(lead_id, channel="max_personal")
        except Exception:
            target_chat = None
    if target_chat is None:
        deps.log_fn(
            f"event=send_result status=skipped reason=missing_chat channel=max_personal tenant={tenant_id} lead_id={lead_id}"
        )
        return (0, "missing_chat")

    idempotency_key = None
    if message_id:
        payload_fingerprint = content_fingerprint(text_value, attachments_list or None)
        idempotency_key = f"{tenant_id}:{lead_id}:{message_id}:{payload_fingerprint[:16]}"

    status_code = 0
    payload: Any = None
    max_attempts = 4
    for attempt in range(1, max_attempts + 1):
        status_code, payload = await deps.max_personal_transport_module.send_message(
            int(tenant_id),
            chat_id=target_chat,
            text=text_value,
            attachments=attachments_list or None,
            dedupe_key=idempotency_key,
            idempotency_key=idempotency_key,
        )
        retryable = False
        if status_code == 0 or status_code >= 500:
            retryable = True
        if isinstance(payload, Mapping):
            retryable = bool(payload.get("retryable")) or retryable
        if not retryable or attempt >= max_attempts:
            break
        await asyncio.sleep(1.2 * attempt)

    body = ""
    if isinstance(payload, Mapping):
        try:
            body = json.dumps(dict(payload), ensure_ascii=False)
        except Exception:
            body = str(payload)
    elif payload is not None:
        body = str(payload)

    if 200 <= status_code < 300:
        deps.message_out_counter.labels("max_personal", "success").inc()
    else:
        deps.message_out_counter.labels("max_personal", "error").inc()

    return status_code, body
