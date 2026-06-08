from __future__ import annotations

import mimetypes
import pathlib
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping, Optional

import httpx


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
LogFn = Callable[[str], None]


@dataclass(frozen=True)
class AvitoOutboundDeps:
    avito_timeout: float
    avito_image_max_bytes: int
    avito_file_max_bytes: int
    log_fn: LogFn
    prepare_tg_attachments_for_send_fn: SyncFn
    avito_integration_module: Any
    coerce_int_fn: SyncFn
    get_lead_peer_fn: AsyncFn
    tenant_dir_fn: SyncFn
    message_out_counter: Any
    avito_chat_cache: dict[int, str]
    httpx_module: Any


@dataclass
class AvitoSendState:
    tenant_id: int
    lead_id: int
    text_value: str
    image_attachments: list[dict[str, Any]]
    media_attachments: list[dict[str, Any]]
    deps: AvitoOutboundDeps
    token: str = ""
    integration: Mapping[str, Any] | None = None
    account_value: int | None = None
    chat_text: str = ""
    response: httpx.Response | None = None
    fallback_text_sent: bool = False


async def send_avito(
    tenant_id: int,
    lead_id: int,
    text: str,
    *,
    chat_id: Optional[str] = None,
    account_id: Optional[int] = None,
    attachments: list[dict[str, Any]] | None = None,
    deps: AvitoOutboundDeps,
) -> tuple[int, str]:
    text_value = (text or "").strip()
    attachments_list = deps.prepare_tg_attachments_for_send_fn(int(tenant_id), attachments or [])
    image_attachments, media_attachments = _split_avito_attachments(attachments_list)
    if not text_value and not image_attachments and not media_attachments:
        return (0, "empty")
    state = AvitoSendState(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        text_value=text_value,
        image_attachments=image_attachments,
        media_attachments=media_attachments,
        deps=deps,
    )
    init_error = await _initialize_avito_send(state, account_id=account_id, chat_id=chat_id)
    if init_error is not None:
        return init_error
    image_error = await _send_avito_images(state)
    if image_error is not None:
        return image_error
    media_error = await _send_avito_media(state)
    if media_error is not None:
        return media_error
    if state.text_value and not state.fallback_text_sent:
        state.response = await _avito_with_refresh(
            state,
            lambda current_token: _post_avito_text(state, current_token, state.text_value),
        )
    if state.response is None:
        return (0, "empty")
    return _finalize_avito_send(state)


def _split_avito_attachments(
    attachments: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    image_attachments: list[dict[str, Any]] = []
    media_attachments: list[dict[str, Any]] = []
    for item in attachments:
        if not isinstance(item, Mapping):
            continue
        type_raw = str(item.get("type") or item.get("kind") or "").strip().lower()
        mime_raw = str(
            item.get("mime") or item.get("mime_type") or item.get("mimetype") or ""
        ).strip().lower()
        if type_raw in {"image", "photo", "picture"} or mime_raw.startswith("image/"):
            image_attachments.append(dict(item))
        else:
            media_attachments.append(dict(item))
    return image_attachments, media_attachments


async def _initialize_avito_send(
    state: AvitoSendState,
    *,
    account_id: Optional[int],
    chat_id: Optional[str],
) -> tuple[int, str] | None:
    account_error = _resolve_avito_account(state, account_id)
    if account_error is not None:
        return account_error
    token_error = await _load_avito_token(state)
    if token_error is not None:
        return token_error
    return await _resolve_avito_chat(state, chat_id)


async def _load_avito_token(state: AvitoSendState) -> tuple[int, str] | None:
    try:
        if state.account_value is not None and hasattr(
            state.deps.avito_integration_module,
            "ensure_access_token_for_account",
        ):
            token, integration = await state.deps.avito_integration_module.ensure_access_token_for_account(
                int(state.tenant_id),
                int(state.account_value),
            )
        else:
            token, integration = await state.deps.avito_integration_module.ensure_access_token(
                int(state.tenant_id)
            )
    except state.deps.avito_integration_module.AvitoOAuthError as exc:
        state.deps.log_fn(
            "event=send_result status=skipped reason=token_unavailable channel=avito tenant=%s error=%s"
            % (state.tenant_id, exc)
        )
        return (0, str(exc))
    state.token = str(token)
    state.integration = integration
    if state.account_value is None:
        state.account_value = state.deps.coerce_int_fn((integration or {}).get("account_id"))
    if state.account_value is None:
        state.deps.log_fn(
            f"event=send_result status=skipped reason=missing_account channel=avito tenant={state.tenant_id}"
        )
        return (0, "missing_account")
    return None


def _resolve_avito_account(
    state: AvitoSendState,
    account_id: Optional[int],
) -> tuple[int, str] | None:
    integration = state.integration or {}
    account_hint = account_id if account_id is not None else integration.get("account_id")
    state.account_value = state.deps.coerce_int_fn(account_hint)
    return None


async def _resolve_avito_chat(
    state: AvitoSendState,
    chat_id: Optional[str],
) -> tuple[int, str] | None:
    chat_candidate = chat_id or await state.deps.get_lead_peer_fn(state.lead_id, channel="avito")
    state.chat_text = str(chat_candidate).strip() if chat_candidate else ""
    if state.chat_text:
        return None
    state.deps.log_fn(
        "event=send_result status=skipped reason=missing_chat channel=avito tenant=%s lead_id=%s"
        % (state.tenant_id, state.lead_id)
    )
    return (0, "missing_chat")


async def _avito_with_refresh(
    state: AvitoSendState,
    request_fn: Callable[[str], Awaitable[httpx.Response]],
) -> httpx.Response:
    response = await request_fn(state.token)
    integration = state.integration or {}
    if response.status_code != 401 or not integration.get("refresh_token"):
        return response
    try:
        if state.account_value is not None and hasattr(
            state.deps.avito_integration_module,
            "refresh_access_token_for_account",
        ):
            refreshed = await state.deps.avito_integration_module.refresh_access_token_for_account(
                state.tenant_id,
                int(state.account_value),
            )
        else:
            refreshed = await state.deps.avito_integration_module.refresh_access_token(state.tenant_id)
        new_token = str(refreshed.get("access_token") or "").strip()
    except state.deps.avito_integration_module.AvitoOAuthError as exc:
        state.deps.log_fn(
            "event=send_result status=error reason=token_refresh_failed channel=avito tenant=%s error=%s"
            % (state.tenant_id, exc)
        )
        return response
    if not new_token:
        return response
    state.token = new_token
    return await request_fn(new_token)


def _avito_json_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


async def _post_avito_text(
    state: AvitoSendState,
    current_token: str,
    message_text: str,
) -> httpx.Response:
    url = (
        f"https://api.avito.ru/messenger/v1/accounts/{state.account_value}/"
        f"chats/{state.chat_text}/messages"
    )
    payload = {"type": "text", "message": {"text": message_text}}
    async with state.deps.httpx_module.AsyncClient(timeout=state.deps.avito_timeout) as client:
        return await client.post(url, json=payload, headers=_avito_json_headers(current_token))


async def _post_avito_image(
    state: AvitoSendState,
    current_token: str,
    image_id: str,
) -> httpx.Response:
    url = (
        f"https://api.avito.ru/messenger/v1/accounts/{state.account_value}/"
        f"chats/{state.chat_text}/messages/image"
    )
    async with state.deps.httpx_module.AsyncClient(timeout=state.deps.avito_timeout) as client:
        return await client.post(
            url,
            json={"image_id": image_id},
            headers=_avito_json_headers(current_token),
        )


async def _upload_avito_attachment(
    state: AvitoSendState,
    current_token: str,
    *,
    upload_path: str,
    data: bytes,
    filename: str,
    mime: str,
) -> httpx.Response:
    url = f"https://api.avito.ru/messenger/v1/accounts/{state.account_value}/{upload_path}"
    headers = {"Authorization": f"Bearer {current_token}"}
    files = {"uploadfile[]": (filename, data, mime)}
    async with state.deps.httpx_module.AsyncClient(timeout=state.deps.avito_timeout) as client:
        return await client.post(url, files=files, headers=headers)


async def _post_avito_media(
    state: AvitoSendState,
    current_token: str,
    *,
    kind: str,
    file_id: str,
) -> httpx.Response:
    url = (
        f"https://api.avito.ru/messenger/v1/accounts/{state.account_value}/"
        f"chats/{state.chat_text}/messages/{kind}"
    )
    payload = {"voice_id" if kind == "voice" else "file_id": file_id}
    async with state.deps.httpx_module.AsyncClient(timeout=state.deps.avito_timeout) as client:
        return await client.post(url, json=payload, headers=_avito_json_headers(current_token))


async def _post_avito_media_generic(
    state: AvitoSendState,
    current_token: str,
    *,
    media_type: str,
    file_id: str,
) -> httpx.Response:
    url = (
        f"https://api.avito.ru/messenger/v1/accounts/{state.account_value}/"
        f"chats/{state.chat_text}/messages"
    )
    payload = {"type": media_type, "message": {"file_id": file_id}}
    async with state.deps.httpx_module.AsyncClient(timeout=state.deps.avito_timeout) as client:
        return await client.post(url, json=payload, headers=_avito_json_headers(current_token))


async def _load_avito_attachment_payload(
    state: AvitoSendState,
    item: Mapping[str, Any],
) -> tuple[bytes | None, str, str]:
    filename = item.get("filename") or item.get("name") or item.get("title") or "file.bin"
    attachment_bytes = _read_avito_attachment_path(state, item)
    if attachment_bytes is None:
        attachment_bytes = await _download_avito_attachment_url(state, item)
    mime = (
        item.get("mime")
        or item.get("mime_type")
        or item.get("content_type")
        or mimetypes.guess_type(str(filename))[0]
        or "application/octet-stream"
    )
    return attachment_bytes, str(filename), str(mime)


def _read_avito_attachment_path(
    state: AvitoSendState,
    item: Mapping[str, Any],
) -> bytes | None:
    attachment_path = _first_avito_attachment_path(item)
    if not attachment_path:
        return None
    try:
        base_dir = state.deps.tenant_dir_fn(int(state.tenant_id))
        candidate = pathlib.Path(attachment_path)
        if not candidate.is_absolute():
            candidate = base_dir / candidate
        resolved = candidate.resolve()
        if str(resolved).startswith(str(base_dir.resolve())) and resolved.is_file():
            return resolved.read_bytes()
    except Exception:
        return None
    return None


def _first_avito_attachment_path(item: Mapping[str, Any]) -> str:
    for key in ("path", "relative_path", "file_path"):
        raw_path = item.get(key)
        if isinstance(raw_path, str) and raw_path.strip():
            return raw_path.strip()
    return ""


async def _download_avito_attachment_url(
    state: AvitoSendState,
    item: Mapping[str, Any],
) -> bytes | None:
    url = item.get("url")
    if not isinstance(url, str) or not url.strip():
        return None
    try:
        async with state.deps.httpx_module.AsyncClient(timeout=state.deps.avito_timeout) as client:
            download = await client.get(url.strip())
        if 200 <= download.status_code < 300:
            return download.content
    except Exception:
        return None
    return None


def _media_fallback_text(state: AvitoSendState, item: Mapping[str, Any]) -> str:
    if state.text_value:
        return state.text_value
    item_type = str(item.get("type") or "").strip().lower()
    item_mime = str(item.get("mime") or item.get("mime_type") or "").strip().lower()
    url_hint = str(item.get("url") or "").strip()
    if item_type in {"voice", "audio"} or item_mime.startswith("audio/"):
        return "Голосовое сообщение"
    if url_hint.startswith("http://") or url_hint.startswith("https://"):
        return url_hint
    return "Вложение"


async def _send_avito_images(state: AvitoSendState) -> tuple[int, str] | None:
    for image_attachment in state.image_attachments:
        image_bytes, filename, mime = await _load_avito_attachment_payload(state, image_attachment)
        if image_bytes is None:
            return (0, "image_unavailable")
        if len(image_bytes) > state.deps.avito_image_max_bytes:
            return (0, "image_too_large")
        upload_response = await _avito_with_refresh(
            state,
            lambda current_token: _upload_avito_attachment(
                state,
                current_token,
                upload_path="uploadImages",
                data=image_bytes,
                filename=str(filename),
                mime=str(mime),
            ),
        )
        if not (200 <= upload_response.status_code < 300):
            return (upload_response.status_code, upload_response.text)
        image_id = _first_payload_key(upload_response)
        if not image_id:
            return (0, "image_upload_failed")
        state.response = await _avito_with_refresh(
            state,
            lambda current_token: _post_avito_image(state, current_token, image_id),
        )
        if not (200 <= state.response.status_code < 300):
            return (state.response.status_code, state.response.text)
    return None


async def _send_avito_media(state: AvitoSendState) -> tuple[int, str] | None:
    for media_attachment in state.media_attachments:
        error = await _send_avito_media_attachment(state, media_attachment)
        if error is not None:
            return error
    return None


async def _send_avito_media_attachment(
    state: AvitoSendState,
    media_attachment: Mapping[str, Any],
) -> tuple[int, str] | None:
    is_voice_attachment = _is_avito_voice_attachment(media_attachment)
    avito_voice_id = str(
        media_attachment.get("avito_voice_id") or media_attachment.get("voice_id") or ""
    ).strip()
    if is_voice_attachment and avito_voice_id:
        state.response = await _avito_with_refresh(
            state,
            lambda current_token: _post_avito_media(
                state,
                current_token,
                kind="voice",
                file_id=avito_voice_id,
            ),
        )
        if 200 <= state.response.status_code < 300:
            return None
    media_bytes, filename, mime = await _load_avito_attachment_payload(state, media_attachment)
    if media_bytes is None:
        return await _send_avito_media_fallback(state, media_attachment, unavailable=True)
    if len(media_bytes) > state.deps.avito_file_max_bytes:
        return (0, "file_too_large")
    upload_response = await _avito_with_refresh(
        state,
        lambda current_token: _upload_avito_attachment(
            state,
            current_token,
            upload_path="uploadFiles",
            data=media_bytes,
            filename=str(filename),
            mime=str(mime),
        ),
    )
    if not (200 <= upload_response.status_code < 300):
        fallback = await _send_avito_media_fallback(state, media_attachment)
        return fallback or (upload_response.status_code, upload_response.text)
    file_id = _first_payload_key(upload_response)
    if not file_id:
        return (0, "file_upload_failed")
    return await _send_uploaded_avito_media(state, media_attachment, file_id)


def _is_avito_voice_attachment(media_attachment: Mapping[str, Any]) -> bool:
    attachment_type = str(media_attachment.get("type") or "").strip().lower()
    attachment_mime = str(
        media_attachment.get("mime")
        or media_attachment.get("mime_type")
        or media_attachment.get("mimetype")
        or ""
    ).strip().lower()
    return attachment_type in {"audio", "voice"} or attachment_mime.startswith("audio/")


async def _send_avito_media_fallback(
    state: AvitoSendState,
    media_attachment: Mapping[str, Any],
    *,
    unavailable: bool = False,
) -> tuple[int, str] | None:
    fallback_text = _media_fallback_text(state, media_attachment)
    state.response = await _avito_with_refresh(
        state,
        lambda current_token: _post_avito_text(state, current_token, fallback_text),
    )
    if 200 <= state.response.status_code < 300:
        state.fallback_text_sent = True
        return None
    if unavailable:
        return (0, "file_unavailable")
    return None


async def _send_uploaded_avito_media(
    state: AvitoSendState,
    media_attachment: Mapping[str, Any],
    file_id: str,
) -> tuple[int, str] | None:
    attempts = _avito_media_send_attempts(state, _is_avito_voice_attachment(media_attachment), file_id)
    last_error_status = 0
    last_error_body = ""
    for attempt in attempts:
        state.response = await _avito_with_refresh(state, attempt)
        if 200 <= state.response.status_code < 300:
            return None
        last_error_status = int(state.response.status_code)
        last_error_body = state.response.text
    fallback = await _send_avito_media_fallback(state, media_attachment)
    return fallback or (last_error_status, last_error_body)


def _avito_media_send_attempts(
    state: AvitoSendState,
    is_voice_attachment: bool,
    file_id: str,
) -> tuple[Callable[[str], Awaitable[httpx.Response]], ...]:
    if is_voice_attachment:
        return (
            lambda tok: _post_avito_media(state, tok, kind="voice", file_id=file_id),
            lambda tok: _post_avito_media(state, tok, kind="file", file_id=file_id),
            lambda tok: _post_avito_media_generic(state, tok, media_type="voice", file_id=file_id),
            lambda tok: _post_avito_media_generic(state, tok, media_type="file", file_id=file_id),
        )
    return (
        lambda tok: _post_avito_media(state, tok, kind="file", file_id=file_id),
        lambda tok: _post_avito_media_generic(state, tok, media_type="file", file_id=file_id),
    )


def _first_payload_key(response: httpx.Response) -> str:
    try:
        upload_payload = response.json()
    except Exception:
        upload_payload = {}
    if not isinstance(upload_payload, dict):
        return ""
    for key in upload_payload.keys():
        return str(key)
    return ""


def _finalize_avito_send(state: AvitoSendState) -> tuple[int, str]:
    response = state.response
    if response is None:
        return (0, "empty")
    deps = state.deps
    deps.log_fn(
        "event=send_result channel=avito tenant=%s lead_id=%s status=%s"
        % (state.tenant_id, state.lead_id, response.status_code)
    )
    if 200 <= response.status_code < 300:
        deps.message_out_counter.labels("avito", "success").inc()
        try:
            deps.avito_chat_cache[int(state.tenant_id)] = state.chat_text
        except Exception:
            pass
    else:
        deps.message_out_counter.labels("avito", "error").inc()
    return response.status_code, response.text
