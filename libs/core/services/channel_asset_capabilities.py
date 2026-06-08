from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CapabilityDecision:
    allowed: bool
    fallback_allowed: bool = False
    reason: str = ""


def can_send_asset(channel: str, asset_type: str, mime: str | None = None) -> CapabilityDecision:
    ch = str(channel or "").strip().lower()
    kind = str(asset_type or "").strip().lower()
    mime_norm = str(mime or "").strip().lower()
    is_image = kind in {"photo", "image"} or mime_norm.startswith("image/")
    is_pdf = kind in {"pdf", "catalog"} or "pdf" in mime_norm
    is_file = kind == "file" or bool(mime_norm)

    if ch in {"telegram", "max", "max_personal"}:
        return CapabilityDecision(True)
    if ch in {"whatsapp", "wa", "waweb"}:
        return CapabilityDecision(True)
    if ch == "avito":
        if is_image:
            return CapabilityDecision(True)
        if is_pdf or is_file:
            return CapabilityDecision(False, fallback_allowed=False, reason="avito_file_not_guaranteed")
        return CapabilityDecision(False, reason="avito_unsupported_asset")
    return CapabilityDecision(False, reason="unsupported_channel")
