from __future__ import annotations

from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field


class Attachment(BaseModel):
    """Generic transport attachment descriptor."""

    type: str = Field(..., min_length=1)
    url: str = Field(..., min_length=1)
    name: str | None = Field(default=None)
    mime: str | None = Field(default=None)
    size: int | None = Field(default=None, ge=0)


class TransportMessage(BaseModel):
    """Unified outgoing transport contract."""

    tenant: int = Field(..., ge=1)
    channel: str = Field(..., pattern=r"^(telegram|whatsapp)$")
    to: Union[int, str]
    text: str | None = None
    attachments: List[Attachment] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    @property
    def has_content(self) -> bool:
        text = (self.text or "").strip()
        return bool(text or self.attachments)


class MessageIn(BaseModel):
    """Normalized incoming message event."""

    tenant: int = Field(..., ge=1)
    channel: str = Field(..., pattern=r"^(telegram|whatsapp)$")
    from_id: Union[int, str]
    to: Union[int, str]
    text: str = Field(default="")
    attachments: List[Attachment] = Field(default_factory=list)
    ts: int = Field(..., ge=0)
    provider_raw: Dict[str, Any] = Field(default_factory=dict)


__all__ = ["Attachment", "TransportMessage", "MessageIn"]
