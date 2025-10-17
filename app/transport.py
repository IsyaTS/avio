from __future__ import annotations

import re
from typing import Tuple

_WHATSAPP_JID_SUFFIX = "@c.us"


class WhatsAppAddressError(ValueError):
    """Raised when a WhatsApp recipient cannot be normalized."""


def _normalize_digits(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if not digits:
        raise WhatsAppAddressError("empty")
    if digits.startswith("8") and len(digits) == 11:
        digits = f"7{digits[1:]}"
    if len(digits) < 10 or len(digits) > 15:
        raise WhatsAppAddressError("invalid_length")
    return digits


def normalize_whatsapp_recipient(value: str | int) -> Tuple[str, str]:
    """Normalize supported WhatsApp recipient formats.

    Parameters
    ----------
    value:
        Recipient identifier in ``+E164`` format, plain digits, or an existing
        JID (``<digits>@c.us``).

    Returns
    -------
    tuple[str, str]
        A pair of ``(digits, jid)`` suitable for downstream APIs.

    Raises
    ------
    WhatsAppAddressError
        If the value cannot be parsed or validated.
    """

    if value is None:
        raise WhatsAppAddressError("empty")

    if isinstance(value, int):
        raw = str(value)
    else:
        raw = str(value).strip()

    if not raw:
        raise WhatsAppAddressError("empty")

    local_part = raw
    lowered = raw.lower()
    if "@" in raw:
        if not lowered.endswith(_WHATSAPP_JID_SUFFIX):
            raise WhatsAppAddressError("invalid_domain")
        local_part = raw.split("@", 1)[0]
    digits = _normalize_digits(local_part)
    jid = f"{digits}{_WHATSAPP_JID_SUFFIX}"
    return digits, jid


__all__ = ["normalize_whatsapp_recipient", "WhatsAppAddressError"]
