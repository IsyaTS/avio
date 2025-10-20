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

    payload: dict[str, Any] = {"provider": "telegram"}

    for key in ("tenant", "channel", "from_id", "to", "ts"):
        value = data.get(key)
        if not _is_empty_value(value):
            payload[key] = value

    provider_raw = data.get("provider_raw")
    if not _is_empty_value(provider_raw):
        payload["provider_raw"] = provider_raw

    telegram_user_id = message.telegram_user_id
    if telegram_user_id is None:
        raw_user_id = data.get("telegram_user_id")
        if raw_user_id is not None:
            telegram_user_id = int(raw_user_id)
    if telegram_user_id is not None:
        payload["telegram_user_id"] = int(telegram_user_id)

    username_value = message.username or data.get("username")
    if isinstance(username_value, str):
        username_value = username_value.strip()
    if not _is_empty_value(username_value):
        payload["username"] = username_value

    peer_id_value = message.peer_id
    if peer_id_value is None:
        raw_peer_id = data.get("peer_id")
        if raw_peer_id is not None:
            peer_id_value = int(raw_peer_id)
    if peer_id_value is not None:
        payload["peer_id"] = int(peer_id_value)

    peer_value = message.peer or data.get("peer")
    if isinstance(peer_value, str) and not peer_value.strip():
        peer_value = None
    if peer_value is None and peer_id_value is not None:
        peer_value = str(peer_id_value)
    if peer_value is None and telegram_user_id is not None:
        peer_value = str(telegram_user_id)
    if peer_value is not None and not _is_empty_value(peer_value):
        payload["peer"] = str(peer_value)

    nested: dict[str, Any] = {}

    message_id_value = message.message_id
    if message_id_value is None:
        raw_message_id = data.get("message_id")
        if raw_message_id is not None:
            message_id_value = int(raw_message_id)
    if message_id_value is not None:
        nested["id"] = int(message_id_value)

    text_value = data.get("text")
    if not _is_empty_value(text_value):
        nested["text"] = text_value

    attachments_value = data.get("attachments")
    if not _is_empty_value(attachments_value):
        nested["attachments"] = attachments_value

    if telegram_user_id is not None:
        nested["telegram_user_id"] = int(telegram_user_id)

    if not _is_empty_value(username_value):
        nested["telegram_username"] = username_value

    if peer_value is not None and not _is_empty_value(peer_value):
        nested["peer"] = str(peer_value)

    if peer_id_value is not None:
        nested["peer_id"] = int(peer_id_value)
        nested["chat_id"] = int(peer_id_value)

    if nested:
        payload["message"] = nested

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
