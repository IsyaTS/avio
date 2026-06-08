from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from fastapi import HTTPException, Request


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class TelegramWebhookDeps:
    json_module: Any
    extract_token_fn: SyncFn
    settings: Any
    decode_tg_slot_tenant_fn: SyncFn
    normalize_attachments_fn: SyncFn
    detect_message_kind_fn: SyncFn
    process_incoming_fn: AsyncFn
    logger: Any


async def telegram_webhook(request: Request, *, deps: TelegramWebhookDeps) -> Any:
    token = deps.extract_token_fn(request)
    secret = deps.settings.WEBHOOK_SECRET or ""
    if secret and token != secret:
        raise HTTPException(status_code=401, detail="unauthorized")
    raw_body, payload = await _request_body_and_json(request, deps=deps)
    _log_payload(payload, raw_body, deps=deps)

    tenant, tg_slot = _tenant_and_slot(payload, deps=deps)
    raw_peer_value, peer_value = _peer_value(payload)
    message = _base_message(payload, tg_slot)
    _enrich_telegram_attachments(
        payload,
        message,
        tenant=tenant,
        peer_value=peer_value,
        raw_peer_value=raw_peer_value,
        deps=deps,
    )
    body = _incoming_body(payload, message, tenant=tenant, peer_value=peer_value, raw_peer_value=raw_peer_value)
    return await deps.process_incoming_fn(body, request)


async def _request_body_and_json(
    request: Request,
    *,
    deps: TelegramWebhookDeps,
) -> tuple[bytes, dict[str, Any]]:
    try:
        raw_body = await request.body()
        payload = await request.json()
    except deps.json_module.JSONDecodeError:
        raise HTTPException(status_code=400, detail="invalid_json")
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_payload")
    if not isinstance(payload, dict):
        payload = {}
    return raw_body, payload


def _log_payload(payload: Mapping[str, Any], raw_body: bytes, *, deps: TelegramWebhookDeps) -> None:
    try:
        deps.logger.info("telegram_webhook_raw keys=%s", list(payload.keys()))
    except Exception:
        deps.logger.exception("telegram_webhook_raw_log_failed")
    try:
        deps.logger.info(
            "manager_diag_raw webhook=telegram len=%s body=%s",
            len(raw_body) if raw_body is not None else 0,
            raw_body.decode("utf-8", errors="ignore") if raw_body else "",
        )
    except Exception:
        deps.logger.exception("manager_diag_raw_failed webhook=telegram")


def _tenant_and_slot(payload: Mapping[str, Any], *, deps: TelegramWebhookDeps) -> tuple[int, int]:
    tenant_raw = payload.get("tenant_id") or payload.get("tenant")
    try:
        tenant = int(tenant_raw) if tenant_raw is not None else 0
    except Exception:
        tenant = 0
    tenant, tg_slot = deps.decode_tg_slot_tenant_fn(tenant)
    if tenant <= 0:
        raise HTTPException(status_code=400, detail="invalid_tenant")
    return tenant, tg_slot


def _peer_value(payload: Mapping[str, Any]) -> tuple[Any, str | None]:
    raw_peer_value = (
        payload.get("peer")
        or payload.get("peer_id")
        or payload.get("chat_id")
        or payload.get("to_peer")
    )
    if raw_peer_value is not None:
        return raw_peer_value, str(raw_peer_value).strip() or None
    return raw_peer_value, None


def _base_message(payload: Mapping[str, Any], tg_slot: int) -> dict[str, Any]:
    raw_msg = payload.get("message")
    message = dict(raw_msg) if isinstance(raw_msg, Mapping) else {}
    message.setdefault("tg_slot", tg_slot)
    message.setdefault("text", (payload.get("text") or "").strip())
    if "telegram_user_id" not in message:
        message["telegram_user_id"] = payload.get("user_id")
    if "telegram_username" not in message:
        message["telegram_username"] = payload.get("username")
    if "media" not in message:
        message["media"] = payload.get("media")
    if "attachments" not in message and isinstance(payload.get("attachments"), list):
        message["attachments"] = payload.get("attachments")
    return message


def _enrich_telegram_attachments(
    payload: dict[str, Any],
    message: dict[str, Any],
    *,
    tenant: int,
    peer_value: str | None,
    raw_peer_value: Any,
    deps: TelegramWebhookDeps,
) -> None:
    del raw_peer_value
    try:
        provider_raw = (
            payload.get("provider_raw") if isinstance(payload.get("provider_raw"), dict) else {}
        )
        raw_attachments = _raw_attachments(message)
        message_id_value = (
            message.get("message_id")
            or message.get("id")
            or payload.get("message_id")
            or payload.get("id")
        )
        for media_obj in _media_candidates(message, provider_raw):
            attachment = _telegram_media_attachment(
                media_obj,
                tenant=tenant,
                peer_value=peer_value,
                message_id_value=message_id_value,
            )
            raw_attachments.append(attachment)
        normalized_attachments = deps.normalize_attachments_fn(raw_attachments)
        message["attachments"] = normalized_attachments
        payload["attachments"] = normalized_attachments
        message["message_kind"] = deps.detect_message_kind_fn(
            message.get("text") or payload.get("text") or "",
            normalized_attachments,
        )
    except Exception:
        deps.logger.exception("telegram_webhook_attach_enrich_failed tenant=%s", tenant)


def _raw_attachments(message: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw_attachments = []
    attachments_payload = message.get("attachments")
    if isinstance(attachments_payload, list):
        raw_attachments.extend(
            dict(item) for item in attachments_payload if isinstance(item, Mapping)
        )
    return raw_attachments


def _media_candidates(
    message: Mapping[str, Any],
    provider_raw: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    media_candidates = []
    for candidate in (
        message.get("media"),
        message.get("photo"),
        provider_raw.get("media") if isinstance(provider_raw, Mapping) else None,
        provider_raw.get("photo") if isinstance(provider_raw, Mapping) else None,
    ):
        if isinstance(candidate, Mapping):
            media_candidates.append(candidate)
        elif isinstance(candidate, list):
            media_candidates.extend(item for item in candidate if isinstance(item, Mapping))
    return media_candidates


def _telegram_media_attachment(
    media_obj: Mapping[str, Any],
    *,
    tenant: int,
    peer_value: str | None,
    message_id_value: Any,
) -> dict[str, Any]:
    media_type = str(media_obj.get("_") or media_obj.get("type") or "").strip() or "photo"
    attachment: dict[str, Any] = {"type": media_type}
    photo_obj = media_obj.get("photo") if isinstance(media_obj.get("photo"), Mapping) else None
    photo_id = media_obj.get("id") or (photo_obj.get("id") if isinstance(photo_obj, Mapping) else None)
    if photo_id and peer_value and message_id_value:
        attachment["url"] = f"telegram://{tenant}/{peer_value}/{message_id_value}"
        attachment["photo_id"] = photo_id
        attachment["peer_id"] = peer_value
        attachment["message_id"] = message_id_value
    return attachment


def _incoming_body(
    payload: Mapping[str, Any],
    message: dict[str, Any],
    *,
    tenant: int,
    peer_value: str | None,
    raw_peer_value: Any,
) -> dict[str, Any]:
    if "provider_raw" not in message and payload.get("provider_raw") is not None:
        message["provider_raw"] = payload.get("provider_raw")
    body: dict[str, Any] = {
        "source": {"type": "telegram", "tenant": tenant},
        "message": message,
        "telegram": payload,
    }
    if payload.get("provider_raw") is not None:
        body["provider_raw"] = payload.get("provider_raw")
    if payload.get("manager") is not None:
        body["manager"] = payload.get("manager")
        body["message"]["manager"] = payload.get("manager")
    if payload.get("out") is not None:
        body["out"] = payload.get("out")
        body["message"]["out"] = payload.get("out")
    if peer_value is not None:
        body["peer"] = peer_value
        body["message"]["peer"] = peer_value
        body["message"]["peer_id"] = raw_peer_value
    return body
