from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping, MutableMapping

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

    for key in ("telegram_user_id", "username", "peer", "peer_id"):
        if data.get(key) is None:
            data.pop(key, None)

    if channel == "telegram":
        nested: dict[str, Any] = {}
        existing_nested = data.get("message")
        if isinstance(existing_nested, dict):
            nested.update(existing_nested)

        text_value = data.get("text")
        if text_value is not None and "text" not in nested:
            nested["text"] = text_value
        attachments_value = data.get("attachments")
        if attachments_value is not None and "attachments" not in nested:
            nested["attachments"] = attachments_value

        telegram_details: dict[str, Any] = {}
        if message.telegram_user_id is not None:
            telegram_id = int(message.telegram_user_id)
            telegram_details["telegram_user_id"] = telegram_id
            data["telegram_user_id"] = telegram_id
        if message.username:
            telegram_details["telegram_username"] = message.username
            data["username"] = message.username
        if message.peer is not None:
            telegram_details["peer"] = message.peer
            data["peer"] = message.peer
        if message.peer_id is not None:
            peer_id_value = int(message.peer_id)
            telegram_details["peer_id"] = peer_id_value
            data["peer_id"] = peer_id_value

        if telegram_details:
            nested.update(telegram_details)

        if nested:
            data["message"] = nested

    return data


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
