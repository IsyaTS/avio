from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from libs.core.lib.numbers import coerce_int


@dataclass(frozen=True)
class AvitoIncomingContext:
    tenant_id: int
    chat_id: str
    message_id: str
    text: str
    account_id: int | None
    item_id: int | None
    user_id: int | None
    login: str | None
    tenant_raw: Any = None

    @property
    def valid_tenant(self) -> bool:
        return self.tenant_id > 0

    @property
    def valid_chat(self) -> bool:
        return bool(self.chat_id)


def build_avito_incoming_context(
    event: Mapping[str, Any],
    *,
    cached_chat_id: str | None = None,
) -> AvitoIncomingContext:
    tenant_raw = event.get("tenant") or event.get("tenant_id")
    tenant_id = coerce_int(tenant_raw) or 0

    chat_id = str(event.get("chat_id") or event.get("peer") or event.get("peer_id") or "").strip()
    if not chat_id and cached_chat_id:
        chat_id = str(cached_chat_id or "").strip()

    message_id_raw = event.get("message_id") or event.get("id")
    message_id = str(message_id_raw) if message_id_raw is not None else ""

    text_raw = event.get("text")
    message = event.get("message")
    if text_raw is None and isinstance(message, Mapping):
        text_raw = message.get("text")
    text = str(text_raw or "").strip()

    avito_payload = event.get("avito") if isinstance(event.get("avito"), Mapping) else {}
    source_payload = event.get("source") if isinstance(event.get("source"), Mapping) else {}
    message_payload = event.get("message") if isinstance(event.get("message"), Mapping) else {}
    account_id = coerce_int(event.get("account_id") or avito_payload.get("account_id"))
    item_id = coerce_int(
        event.get("item_id")
        or avito_payload.get("item_id")
        or source_payload.get("item_id")
        or message_payload.get("item_id")
    )
    user_id = coerce_int(event.get("avito_user_id") or avito_payload.get("user_id"))

    login_value = event.get("avito_login") or avito_payload.get("login")
    login = login_value.strip() if isinstance(login_value, str) and login_value.strip() else None

    return AvitoIncomingContext(
        tenant_id=int(tenant_id),
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        account_id=account_id,
        item_id=item_id,
        user_id=user_id,
        login=login,
        tenant_raw=tenant_raw,
    )


__all__ = ["AvitoIncomingContext", "build_avito_incoming_context"]
