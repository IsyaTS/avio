"""Utility helpers shared across transport implementations."""

from .transport_utils import (
    Attachment,
    MessageIn,
    TransportMessage,
    coerce_attachments,
    dump_message_in,
    dump_transport_message,
    ensure_message_in,
    ensure_transport_message,
    message_in_asdict,
    normalize_channel,
    transport_message_asdict,
    update_meta,
)

__all__ = [
    "Attachment",
    "MessageIn",
    "TransportMessage",
    "coerce_attachments",
    "dump_message_in",
    "dump_transport_message",
    "ensure_message_in",
    "ensure_transport_message",
    "message_in_asdict",
    "normalize_channel",
    "transport_message_asdict",
    "update_meta",
]
