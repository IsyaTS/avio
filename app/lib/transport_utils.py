from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from app.schemas import Attachment, MessageIn, TransportMessage

LOGGER = logging.getLogger("app.transport")
_CHANNELS = {"telegram", "whatsapp"}


def _model_dump(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()  # type: ignore[no-any-return]
    return model.dict()  # type: ignore[no-any-return]


def _model_dump_json(model: Any) -> str:
    if hasattr(model, "model_dump_json"):
        return model.model_dump_json()
    return model.json()


def _is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (bytes, bytearray)):
        return len(value) == 0
    if isinstance(value, Mapping):
        return len(value) == 0
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return len(value) == 0
    return False


def normalize_channel(value: str | None) -> str:
    if not value:
        raise ValueError("channel_required")
    channel = value.strip().lower()
    if channel not in _CHANNELS:
        raise ValueError("channel_unknown")
    return channel


def ensure_transport_message(payload: Mapping[str, Any], *, default_channel: str | None = None) -> TransportMessage:
    data = dict(payload)
    if "channel" not in data and default_channel:
        data["channel"] = default_channel
    return TransportMessage(**data)


def ensure_message_in(payload: Mapping[str, Any], *, default_channel: str | None = None) -> MessageIn:
    data = dict(payload)
    if "channel" not in data and default_channel:
        data["channel"] = default_channel
    return MessageIn(**data)


def dump_message_in(message: MessageIn) -> str:
    return _model_dump_json(message)


def dump_transport_message(message: TransportMessage) -> str:
    return _model_dump_json(message)


def coerce_attachments(raw_items: Iterable[Mapping[str, Any]] | None) -> list[Attachment]:
    attachments: list[Attachment] = []
    if not raw_items:
        return attachments
    for item in raw_items:
        try:
            attachments.append(Attachment(**dict(item)))
        except Exception as exc:  # pragma: no cover - best effort cleanup
            LOGGER.debug("attachment_discarded error=%s", exc)
    return attachments


def update_meta(target: MutableMapping[str, Any], **updates: Any) -> None:
    meta = target.setdefault("meta", {})
    if not isinstance(meta, MutableMapping):
        meta = {}
        target["meta"] = meta
    meta.update({k: v for k, v in updates.items() if v is not None})


def message_in_asdict(message: MessageIn) -> dict[str, Any]:
    data = _model_dump(message)
    channel = str(data.get("channel") or "").lower()

    if channel != "telegram":
        return {key: value for key, value in data.items() if value is not None}

    def _coerce_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    tenant_raw = data.get("tenant")
    try:
        tenant = int(tenant_raw) if tenant_raw is not None else None
    except (TypeError, ValueError):
        tenant = tenant_raw
    provider_raw = data.get("provider_raw")

    if message.text is not None:
        text_raw: Any = message.text
    else:
        text_raw = data.get("text")
    if text_raw is None:
        text_value = ""
    elif isinstance(text_raw, str):
        text_value = text_raw
    else:
        text_value = str(text_raw)

    telegram_user_id = message.telegram_user_id
    if telegram_user_id is None:
        telegram_user_id = _coerce_int(data.get("telegram_user_id"))

    username_value = message.username if message.username is not None else data.get("username")
    if isinstance(username_value, str):
        username_value = username_value.strip() or None

    peer_id_value = message.peer_id
    if peer_id_value is None:
        peer_id_value = _coerce_int(data.get("peer_id"))

    peer_value: str | None
    if peer_id_value is not None:
        peer_value = str(peer_id_value)
    else:
        peer_raw = message.peer if message.peer is not None else data.get("peer")
        if isinstance(peer_raw, str):
            peer_value = peer_raw.strip() or None
        elif peer_raw is None:
            peer_value = None
        else:
            peer_value = str(peer_raw)

    message_id_value = message.message_id if message.message_id is not None else data.get("message_id")
    if isinstance(message_id_value, str):
        message_id_value = message_id_value.strip() or None
    elif message_id_value is not None and not isinstance(message_id_value, int):
        coerced = _coerce_int(message_id_value)
        message_id_value = coerced if coerced is not None else str(message_id_value)

    attachments_value = data.get("attachments")
    if not isinstance(attachments_value, list):
        attachments_value = list(message.attachments) if isinstance(message.attachments, list) else []

    nested: dict[str, Any] = {
        "id": message_id_value,
        "text": text_value,
        "attachments": attachments_value or [],
        "telegram_user_id": telegram_user_id,
        "telegram_username": username_value,
        "peer": peer_value,
        "peer_id": peer_id_value,
    }
    if peer_id_value is not None:
        nested["chat_id"] = peer_id_value

    payload: dict[str, Any] = {
        "provider": "telegram",
        "tenant": tenant,
        "channel": "telegram",
        "source": {"type": "telegram", "tenant": tenant},
        "text": text_value,
        "telegram_user_id": telegram_user_id,
        "username": username_value,
        "peer": peer_value,
        "peer_id": peer_id_value,
        "message": nested,
    }

    for key in ("from_id", "to", "ts"):
        value = data.get(key)
        if not _is_empty_value(value):
            payload[key] = value

    if not _is_empty_value(provider_raw):
        payload["provider_raw"] = provider_raw

    return payload


def transport_message_asdict(message: TransportMessage) -> dict[str, Any]:
    return _model_dump(message)


__all__ = [
    "Attachment",
    "MessageIn",
    "TransportMessage",
    "normalize_channel",
    "ensure_transport_message",
    "ensure_message_in",
    "dump_message_in",
    "dump_transport_message",
    "coerce_attachments",
    "update_meta",
    "message_in_asdict",
    "transport_message_asdict",
]
