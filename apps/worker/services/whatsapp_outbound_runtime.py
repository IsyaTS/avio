from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
LogFn = Callable[[str], None]


@dataclass(frozen=True)
class WhatsAppOutboundDeps:
    log_fn: LogFn
    waweb_base_url_fn: SyncFn
    wabaileys_base_url_fn: SyncFn
    normalize_whatsapp_recipient_fn: SyncFn
    whatsapp_address_error: type[Exception]
    digits_fn: SyncFn
    tokenize_attachment_mapping_fn: SyncFn
    build_wa_document_payload_fn: SyncFn
    http_json_fn: SyncFn
    sleep_fn: AsyncFn
    asyncio_to_thread_fn: AsyncFn
    json_module: Any
    wa_send_base_timeout: float
    wa_send_timeout_per_mib: float
    wa_send_timeout_max: float
    wa_internal_token: str
    admin_token: str
    core_settings_module: Any


@dataclass
class WhatsAppSendState:
    tenant_id: int
    phone: str
    text: str | None
    attachment: Mapping[str, Any] | None
    attachments: Iterable[Mapping[str, Any]] | None
    deps: WhatsAppOutboundDeps
    url: str
    payload: Dict[str, Any]
    attachments_payload: list[dict[str, Any]]
    document_block: dict[str, Any] | None = None
    seen_urls: set[str] | None = None
    media_bytes: int = 0
    request_timeout: float = 0.0
    headers: Dict[str, str] | None = None


async def send_whatsapp(
    tenant_id: int,
    phone: str,
    text: str | None = None,
    attachment: Mapping[str, Any] | None = None,
    attachments: Iterable[Mapping[str, Any]] | None = None,
    *,
    deps: WhatsAppOutboundDeps,
) -> tuple[int, str]:
    base_url = deps.waweb_base_url_fn(tenant_id)
    state = WhatsAppSendState(
        tenant_id=tenant_id,
        phone=phone,
        text=text,
        attachment=attachment,
        attachments=attachments,
        deps=deps,
        url=f"{base_url}/send?tenant={tenant_id}",
        payload={"channel": "whatsapp", "tenant": tenant_id, "tenant_id": tenant_id},
        attachments_payload=[],
        seen_urls=set(),
    )
    _prepare_waweb_payload(state)
    state.request_timeout = _wa_post_timeout(state.media_bytes, deps=deps)
    _log_waweb_media(state)
    state.headers = _waweb_headers(deps)
    _log_waweb_request(state)
    return await _post_waweb_with_retries(state)


def _prepare_waweb_payload(state: WhatsAppSendState) -> None:
    _set_waweb_recipient(state)
    if state.text:
        state.payload["text"] = state.text
    _collect_waweb_attachments(state)
    _finalize_waweb_attachments(state)


def _set_waweb_recipient(state: WhatsAppSendState) -> None:
    raw_phone = state.phone or ""
    try:
        _, jid = state.deps.normalize_whatsapp_recipient_fn(raw_phone)
    except state.deps.whatsapp_address_error:
        digits_only = state.deps.digits_fn(str(raw_phone))
        jid = f"{digits_only}@c.us" if digits_only else str(raw_phone)
    state.payload["to"] = jid


def _append_waweb_attachment(
    state: WhatsAppSendState,
    blob: Mapping[str, Any],
    *,
    force_include: bool = False,
) -> dict[str, Any]:
    prepared_blob = state.deps.tokenize_attachment_mapping_fn(blob)
    url_value = str(prepared_blob.get("url") or "")
    seen_urls = state.seen_urls if state.seen_urls is not None else set()
    include_blob = force_include or not url_value or url_value not in seen_urls
    if url_value:
        seen_urls.add(url_value)
    state.seen_urls = seen_urls
    if include_blob:
        wa_attachment, doc_block = state.deps.build_wa_document_payload_fn(prepared_blob)
        state.attachments_payload.append(wa_attachment or prepared_blob)
        if wa_attachment and doc_block and state.document_block is None:
            state.document_block = doc_block
    return prepared_blob


def _collect_waweb_attachments(state: WhatsAppSendState) -> None:
    if state.attachment:
        attachment_copy = _append_waweb_attachment(state, state.attachment, force_include=True)
        sanitized = {key: value for key, value in attachment_copy.items() if key not in {"b64", "data"}}
        if sanitized:
            state.payload["attachment"] = sanitized
    if not state.attachments:
        return
    for blob in state.attachments:
        if not isinstance(blob, Mapping):
            continue
        if state.attachment is not None and blob is state.attachment:
            continue
        _append_waweb_attachment(state, blob)


def _finalize_waweb_attachments(state: WhatsAppSendState) -> None:
    if state.attachments_payload:
        state.media_bytes = _estimate_media_bytes(state.attachments_payload)
        state.deps.log_fn(
            "[worker] wa_payload attachments_count=%s attachment_keys=%s document_keys=%s"
            % (
                len(state.attachments_payload),
                list(state.attachments_payload[0].keys()) if state.attachments_payload else [],
                list((state.document_block or {}).keys()) if state.document_block else [],
            )
        )
        state.payload["attachments"] = state.attachments_payload
        return
    if state.document_block:
        state.payload["document"] = state.document_block
        state.deps.log_fn(
            "[worker] wa_payload document_only=%s document_keys=%s"
            % (bool(state.document_block), list(state.document_block.keys()))
        )


def _estimate_media_bytes(items: Iterable[Mapping[str, Any]]) -> int:
    media_bytes = 0
    for candidate in items:
        data_block = candidate.get("b64")
        if isinstance(data_block, str) and data_block:
            media_bytes += int(len(data_block) * 3 / 4)
            continue
        size_block = candidate.get("size")
        if isinstance(size_block, (int, float)) and size_block > 0:
            media_bytes += int(size_block)
    return media_bytes


def _log_waweb_media(state: WhatsAppSendState) -> None:
    if state.media_bytes:
        state.deps.log_fn(
            "[worker] wa_payload media_bytes=%s timeout=%.1f"
            % (state.media_bytes, state.request_timeout)
        )


def _waweb_headers(deps: WhatsAppOutboundDeps) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    admin_token = (
        str(getattr(deps.core_settings_module, "ADMIN_TOKEN", "") or "") or deps.admin_token or ""
    ).strip()
    shared_token = deps.wa_internal_token or admin_token
    if shared_token:
        headers["X-Auth-Token"] = shared_token
    if deps.wa_internal_token:
        headers.setdefault("X-Internal-Token", deps.wa_internal_token)
    if admin_token and admin_token != headers.get("X-Auth-Token"):
        headers.setdefault("X-Admin-Token", admin_token)
    return headers


def _log_waweb_request(state: WhatsAppSendState) -> None:
    try:
        payload_meta = {
            "has_attachment": bool(state.payload.get("attachment")),
            "attachments_len": len(state.payload.get("attachments") or [])
            if isinstance(state.payload.get("attachments"), list)
            else 0,
            "keys": sorted(state.payload.keys()),
        }
        state.deps.log_fn(
            "[worker] wa_http_request url=%s headers=%s payload_meta=%s"
            % (state.url, list((state.headers or {}).keys()), payload_meta)
        )
    except Exception:
        pass


async def _post_waweb_with_retries(state: WhatsAppSendState) -> tuple[int, str]:
    last_status, last_body = 0, ""
    retry_delays = (0.5, 1.0, 2.0)
    for attempt in range(len(retry_delays)):
        last_status, last_body = await state.deps.asyncio_to_thread_fn(
            state.deps.http_json_fn,
            "POST",
            state.url,
            state.payload,
            state.request_timeout,
            state.headers or {},
        )
        if 200 <= last_status < 300:
            break
        if last_status == 0 or last_status >= 500:
            if attempt < len(retry_delays) - 1:
                delay = retry_delays[attempt]
                state.deps.log_fn(
                    f"event=waweb_retry attempt={attempt + 1} status={last_status} delay={delay}"
                )
                await state.deps.sleep_fn(delay)
                continue
        break
    return last_status, last_body


async def send_whatsapp_baileys(
    tenant_id: int,
    phone: str,
    text: str | None = None,
    attachments: Iterable[Mapping[str, Any]] | None = None,
    meta: Mapping[str, Any] | None = None,
    *,
    deps: WhatsAppOutboundDeps,
) -> tuple[int, str]:
    base_url = deps.wabaileys_base_url_fn()
    url = f"{base_url}/messages/send"
    payload: Dict[str, Any] = {
        "channel": "whatsapp",
        "tenant": tenant_id,
        "tenant_id": tenant_id,
    }
    jid = _baileys_jid(phone, deps=deps)
    if not jid:
        _log_baileys_missing_recipient(tenant_id, deps=deps)
        return (422, "missing_recipient")
    payload["to"] = jid
    if text:
        payload["text"] = text
    attachment_items = _baileys_attachment_items(attachments)
    if attachment_items:
        payload["attachments"] = attachment_items
    _set_baileys_meta(payload, meta, deps=deps)
    headers = {"Content-Type": "application/json; charset=utf-8"}
    _log_baileys_request(tenant_id, payload, text, attachment_items, deps=deps)
    status, body = await deps.asyncio_to_thread_fn(
        deps.http_json_fn,
        "POST",
        url,
        payload,
        60.0,
        headers,
    )
    return status, body


def _baileys_jid(phone: str, *, deps: WhatsAppOutboundDeps) -> str:
    recipient = (phone or "").strip()
    if not recipient:
        return ""
    if "@" in recipient:
        return recipient.lower()
    try:
        digits, _ = deps.normalize_whatsapp_recipient_fn(recipient)
    except deps.whatsapp_address_error:
        digits = deps.digits_fn(recipient)
    return f"{digits}@s.whatsapp.net" if digits else ""


def _baileys_attachment_items(
    attachments: Iterable[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not attachments:
        return []
    return [dict(blob) for blob in attachments if isinstance(blob, Mapping)]


def _set_baileys_meta(
    payload: Dict[str, Any],
    meta: Mapping[str, Any] | None,
    *,
    deps: WhatsAppOutboundDeps,
) -> None:
    if not isinstance(meta, Mapping) or not meta:
        return
    try:
        payload["meta"] = deps.json_module.loads(deps.json_module.dumps(meta, ensure_ascii=False))
    except Exception:
        payload["meta"] = dict(meta)


def _baileys_body_type(text: str | None, attachment_items: list[dict[str, Any]]) -> str:
    if attachment_items:
        return "media"
    if text:
        return "text"
    return "unknown"


def _log_baileys_missing_recipient(tenant_id: int, *, deps: WhatsAppOutboundDeps) -> None:
    deps.log_fn(
        " ".join(
            [
                "[BAILEYS OUTBOUND HTTP]",
                f"tenant={tenant_id}",
                "to=-",
                "body_type=unknown",
                "status=skipped_missing_recipient",
            ]
        )
    )


def _log_baileys_request(
    tenant_id: int,
    payload: Mapping[str, Any],
    text: str | None,
    attachment_items: list[dict[str, Any]],
    *,
    deps: WhatsAppOutboundDeps,
) -> None:
    deps.log_fn(
        " ".join(
            [
                "[BAILEYS OUTBOUND HTTP]",
                f"tenant={tenant_id}",
                f"to={payload.get('to') or '-'}",
                f"body_type={_baileys_body_type(text, attachment_items)}",
            ]
        )
    )


def _wa_post_timeout(bytes_total: int, *, deps: WhatsAppOutboundDeps) -> float:
    base_timeout = deps.wa_send_base_timeout or 90.0
    if bytes_total <= 0:
        return float(base_timeout)
    per_mib = deps.wa_send_timeout_per_mib or 40.0
    timeout = base_timeout + per_mib * (bytes_total / (1024 * 1024))
    if timeout < base_timeout:
        timeout = base_timeout
    max_timeout = deps.wa_send_timeout_max
    if max_timeout and max_timeout > 0:
        timeout = min(timeout, max_timeout)
    return float(timeout)
