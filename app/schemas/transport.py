from __future__ import annotations

from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field
try:  # pragma: no cover - pydantic v1 compatibility
    from pydantic import ConfigDict
except ImportError:  # pragma: no cover - pydantic v2 only
    ConfigDict = None  # type: ignore[misc]
try:  # pragma: no cover - optional validator APIs
    from pydantic import model_validator
except ImportError:  # pragma: no cover - pydantic v1
    model_validator = None  # type: ignore[assignment]
try:  # pragma: no cover - optional validator APIs
    from pydantic import root_validator
except ImportError:  # pragma: no cover - pydantic v2
    root_validator = None  # type: ignore[assignment]


if ConfigDict is None:

    class _AliasModel(BaseModel):
        class Config:
            allow_population_by_field_name = True
            allow_population_by_alias = True

else:  # pragma: no cover - executed only on pydantic v2

    class _AliasModel(BaseModel):
        model_config = ConfigDict(populate_by_name=True)


class Attachment(_AliasModel):
    """Generic transport attachment descriptor."""

    type: str = Field(..., min_length=1)
    url: str = Field(..., min_length=1)
    name: str | None = Field(default=None)
    mime: str | None = Field(default=None)
    size: int | None = Field(default=None, ge=0)


class _TransportMessageBase(_AliasModel):
    channel: str = Field(..., pattern=r"^(telegram|whatsapp)$")
    to: Union[int, str]
    text: str | None = None
    attachments: List[Attachment] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def _ensure_tenant(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "tenant" not in values and "tenant_id" in values:
            values = dict(values)
            values["tenant"] = values.pop("tenant_id")
        return values

    if model_validator is not None:  # pragma: no branch - prefer v2 API

        @model_validator(mode="before")
        def _alias_tenant_v2(cls, values: Any) -> Any:
            if isinstance(values, dict):
                return cls._ensure_tenant(values)
            return values
    elif root_validator is not None:  # pragma: no branch - fallback

        @root_validator(pre=True)
        def _alias_tenant_v1(cls, values: dict[str, Any]) -> dict[str, Any]:
            return cls._ensure_tenant(values)

    @property
    def has_content(self) -> bool:
        text = (self.text or "").strip()
        return bool(text or self.attachments)


class TransportMessage(_TransportMessageBase):
    tenant: int = Field(..., ge=1)


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
