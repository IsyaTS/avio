from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class IncomingEventRoute:
    channel: str

    @property
    def has_handler_key(self) -> bool:
        return bool(self.channel)


@dataclass(frozen=True)
class IncomingEventLogHints:
    channel: str
    tenant: Any
    message_id: Any


def normalize_incoming_channel(event: Mapping[str, Any]) -> str:
    raw_channel = event.get("channel") or event.get("ch") or event.get("provider")
    if isinstance(raw_channel, str):
        return raw_channel.strip().lower()
    if raw_channel is not None:
        return str(raw_channel).strip().lower()
    return ""


def build_incoming_event_route(event: Mapping[str, Any]) -> IncomingEventRoute:
    return IncomingEventRoute(channel=normalize_incoming_channel(event))


def build_incoming_event_log_hints(event: Mapping[str, Any]) -> IncomingEventLogHints:
    return IncomingEventLogHints(
        channel=normalize_incoming_channel(event) or "-",
        tenant=event.get("tenant") or event.get("tenant_id") or "",
        message_id=event.get("message_id") or "-",
    )


def normalize_event_text(event: Mapping[str, Any]) -> str:
    text_raw = event.get("text")
    return "" if text_raw is None else str(text_raw).strip()


def looks_like_manager_outgoing(event: Mapping[str, Any]) -> bool:
    """Best-effort check whether transport event came from manager/account owner."""

    def has_outgoing_flag(blob: Any) -> bool:
        if not isinstance(blob, Mapping):
            return False
        key_obj = blob.get("key") if isinstance(blob.get("key"), Mapping) else {}
        return bool(
            blob.get("manager")
            or blob.get("out")
            or blob.get("outgoing")
            or blob.get("fromMe")
            or (isinstance(key_obj, Mapping) and key_obj.get("fromMe"))
        )

    origin = event.get("origin")
    if isinstance(origin, str) and origin.startswith(("telegram:manager", "max_personal:manager")):
        return True
    if bool(event.get("manager")) or bool(event.get("out")):
        return True

    provider_raw = event.get("provider_raw")
    message_obj = event.get("message") if isinstance(event.get("message"), Mapping) else {}
    meta_obj = (
        message_obj.get("meta")
        if isinstance(message_obj, Mapping) and isinstance(message_obj.get("meta"), Mapping)
        else {}
    )
    return has_outgoing_flag(provider_raw) or has_outgoing_flag(message_obj) or has_outgoing_flag(meta_obj)


def collect_event_attachment_items(event: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    items: list[Mapping[str, Any]] = []
    attachments = event.get("attachments")
    if isinstance(attachments, list):
        items.extend(item for item in attachments if isinstance(item, Mapping))

    attachment = event.get("attachment")
    if isinstance(attachment, Mapping):
        items.append(attachment)

    for field_name in ("media", "photo"):
        field = event.get(field_name)
        if isinstance(field, list):
            items.extend(item for item in field if isinstance(item, Mapping))
        elif isinstance(field, Mapping):
            items.append(field)
    return items


def has_image_attachment(attachments: Sequence[Mapping[str, Any]]) -> bool:
    return any(str(item.get("type") or "").strip().lower() == "image" for item in attachments)


def fallback_lead_id(
    *,
    lead_hint: int | None,
    numeric_identity: str | None = None,
    fallback_value: int,
) -> int:
    if lead_hint is not None and lead_hint > 0:
        return int(lead_hint)
    if numeric_identity:
        try:
            return int(numeric_identity)
        except Exception:
            pass
    return int(fallback_value)


__all__ = [
    "IncomingEventLogHints",
    "IncomingEventRoute",
    "build_incoming_event_log_hints",
    "build_incoming_event_route",
    "collect_event_attachment_items",
    "fallback_lead_id",
    "has_image_attachment",
    "looks_like_manager_outgoing",
    "normalize_event_text",
    "normalize_incoming_channel",
]
