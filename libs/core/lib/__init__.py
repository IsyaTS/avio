"""Utility helpers shared across transport implementations.

Keep package import side-effects minimal: do not eagerly import transport
helpers here because they depend on message envelope utilities and can create
cyclic imports during alternative import orders.
"""

from __future__ import annotations

from .numbers import coerce_int
from .tg_slots import (
    TG_SLOT_MAX,
    TG_SLOT_MIN,
    TG_SLOT_MULTIPLIER,
    decode_virtual_tenant,
    normalize_tg_slot,
    virtual_tenant_id,
)

_TRANSPORT_EXPORTS = {
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
}


def __getattr__(name: str):
    if name in _TRANSPORT_EXPORTS:
        from . import transport_utils as _transport_utils

        return getattr(_transport_utils, name)
    raise AttributeError(name)


__all__ = [
    "TG_SLOT_MAX",
    "TG_SLOT_MIN",
    "TG_SLOT_MULTIPLIER",
    "coerce_int",
    "decode_virtual_tenant",
    "normalize_tg_slot",
    "virtual_tenant_id",
    *_TRANSPORT_EXPORTS,
]
