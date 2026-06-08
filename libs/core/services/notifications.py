from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from libs.core.lib.numbers import coerce_int


@dataclass(frozen=True)
class NotificationContext:
    event_name: str
    tenant_id: int
    lead_id: int
    chat_ids: list[int]
    text: str

    @property
    def has_text(self) -> bool:
        return bool(self.text)

    @property
    def has_targets(self) -> bool:
        return bool(self.chat_ids)


def coerce_chat_ids(raw: Any) -> list[int]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        result: list[int] = []
        for item in raw:
            val = coerce_int(item)
            if val:
                result.append(int(val))
        return result
    candidate = coerce_int(raw)
    return [int(candidate)] if candidate else []


def build_notification_context(
    item: Mapping[str, Any],
    *,
    default_tenant_id: int = 1,
    configured_chat_ids: Sequence[int] | None = None,
) -> NotificationContext:
    event_name = str(item.get("event") or "notify").strip() or "notify"
    tenant_id = coerce_int(item.get("tenant_id") or item.get("tenant"))
    if tenant_id is None:
        tenant_id = int(default_tenant_id)
    lead_id = coerce_int(item.get("lead_id")) or 0
    chat_ids = coerce_chat_ids(item.get("chat_ids"))
    if not chat_ids and configured_chat_ids is not None:
        chat_ids = coerce_chat_ids(list(configured_chat_ids))
    text = str(item.get("text") or "").strip()
    return NotificationContext(
        event_name=event_name,
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        chat_ids=chat_ids,
        text=text,
    )


__all__ = [
    "NotificationContext",
    "build_notification_context",
    "coerce_chat_ids",
]
