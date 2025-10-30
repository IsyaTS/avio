from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence, Union

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
    name: str = Field(..., min_length=1)
    mime: str = Field(..., min_length=1)
    caption: str | None = Field(default=None)
    size: int | None = Field(default=None, ge=0)

    @classmethod
    def _normalize_values(cls, values: Any) -> Any:
        if not isinstance(values, Mapping):
            return values
        data = dict(values)
        caption = data.get("caption") or data.get("description")
        if caption is not None and not isinstance(caption, str):
            caption = str(caption)
        if caption is not None:
            data["caption"] = caption
        size_value = data.get("size")
        if size_value in ("", None):
            data.pop("size", None)
        elif not isinstance(size_value, int):
            try:
                data["size"] = int(size_value)
            except (TypeError, ValueError):
                data.pop("size", None)
        return data

    if model_validator is not None:  # pragma: no branch - prefer v2 API

        @model_validator(mode="before")
        def _normalize_v2(cls, values: Any) -> Any:
            return cls._normalize_values(values)

    elif root_validator is not None:  # pragma: no branch - fallback for v1

        @root_validator(pre=True)
        def _normalize_v1(cls, values: dict[str, Any]) -> dict[str, Any]:
            normalized = cls._normalize_values(values)
            return normalized if isinstance(normalized, dict) else values


class _TransportMessageBase(_AliasModel):
    channel: str = Field(..., pattern=r"^(telegram|whatsapp)$")
    to: Union[int, str]
    text: str | None = Field(default=None)
    attachments: List[Attachment] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def _ensure_tenant(cls, values: dict[str, Any]) -> dict[str, Any]:
        data = dict(values)
        if "tenant" not in data:
            for alias in ("tenant_id", "tenantId", "tenantID"):
                if alias in data:
                    data["tenant"] = data.pop(alias)
                    break
        return data

    @classmethod
    def _apply_aliases(cls, values: Mapping[str, Any]) -> dict[str, Any]:
        data = cls._ensure_tenant(values if isinstance(values, Mapping) else dict(values))

        def _maybe_coerce_sequence(value: Any) -> list[Any] | None:
            if value is None:
                return []
            if isinstance(value, list):
                return value
            if isinstance(value, tuple):
                return list(value)
            if isinstance(value, Mapping):
                return [dict(value)]
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                return list(value)
            return [value]

        alias_pairs: dict[str, tuple[str, ...]] = {
            "channel": ("provider", "ch"),
            "to": ("recipient", "phone", "jid"),
            "text": ("message", "body"),
            "attachments": ("attachment", "media"),
        }

        for field_name, aliases in alias_pairs.items():
            if field_name in data and data[field_name] not in (None, ""):
                continue
            for alias in aliases:
                if alias in data:
                    data[field_name] = data.pop(alias)
                    break

        attachments_value = data.get("attachments")
        if attachments_value is not None:
            maybe_list = _maybe_coerce_sequence(attachments_value)
            if maybe_list is not None:
                data["attachments"] = maybe_list
        else:
            singular = None
            for alias in ("attachment", "media"):
                if alias in data:
                    singular = data.pop(alias)
                    break
            if singular is not None:
                maybe_list = _maybe_coerce_sequence(singular)
                if maybe_list is not None:
                    data["attachments"] = maybe_list

        channel = data.get("channel")
        if isinstance(channel, str):
            normalized = channel.strip().lower()
            channel_aliases = {
                "wa": "whatsapp",
                "whatsapp": "whatsapp",
                "telegram": "telegram",
                "tg": "telegram",
            }
            data["channel"] = channel_aliases.get(normalized, normalized)

        text_value = data.get("text")
        if text_value is not None and not isinstance(text_value, str):
            data["text"] = str(text_value)

        return data

    if model_validator is not None:  # pragma: no branch - prefer v2 API

        @model_validator(mode="before")
        def _alias_tenant_v2(cls, values: Any) -> Any:
            if isinstance(values, Mapping):
                return cls._apply_aliases(values)
            return values
    elif root_validator is not None:  # pragma: no branch - fallback

        @root_validator(pre=True)
        def _alias_tenant_v1(cls, values: dict[str, Any]) -> dict[str, Any]:
            return cls._apply_aliases(values)

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
    from_id: int | str | None = None
    to: int | str | None = None
    text: str | None = None
    attachments: List[Attachment] = Field(default_factory=list)
    ts: int | None = None
    message_id: int | str | None = None
    provider_raw: Dict[str, Any] = Field(default_factory=dict)
    telegram_user_id: int | None = None
    username: str | None = None
    peer: str | None = None
    peer_id: int | None = None


class PingEvent(BaseModel):
    """Minimal diagnostic ping event."""

    event: str = Field(default="ping", pattern=r"^ping$")
    tenant: int | None = Field(default=None, ge=1)
    channel: str | None = Field(default=None, min_length=1)
    ts: int | None = Field(default=None, ge=0)


__all__ = ["Attachment", "TransportMessage", "MessageIn", "PingEvent"]
