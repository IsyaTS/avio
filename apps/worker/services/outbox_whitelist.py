from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from libs.core.common import OutboxWhitelist, normalize_username, whitelist_contains_number
from libs.core.transport import WhatsAppAddressError


@dataclass(frozen=True)
class OutboxWhitelistDeps:
    whitelist: OutboxWhitelist
    recent_incoming_ttl_seconds: int
    normalize_e164_digits_fn: Callable[[Any], str]
    coerce_int_fn: Callable[[Any], int | None]
    has_recent_incoming_message_fn: Callable[..., Awaitable[bool]]
    db_error_labels_fn: Callable[[str], Any]
    log_fn: Callable[..., None]


async def whitelist_allows(
    *,
    telegram_user_id: int | None,
    username: str | None,
    raw_to: Any,
    lead_id: int | None,
    tenant_id: int | None,
    channel: str,
    deps: OutboxWhitelistDeps,
) -> tuple[bool, str]:
    if deps.whitelist.allow_all:
        return True, "allow_all"
    if _id_allowed(telegram_user_id, raw_to, deps=deps):
        return True, "id"
    if _username_allowed(username, raw_to, deps=deps):
        return True, "username"

    number_candidates, format_error = _number_candidates(raw_to, deps=deps)
    for digits in number_candidates:
        if whitelist_contains_number(deps.whitelist, digits):
            return True, "number"

    if channel == "whatsapp":
        recent_allowed = await _recent_incoming_allows(lead_id, tenant_id, deps=deps)
        if recent_allowed:
            return True, "recent_incoming"
        if format_error:
            return False, "format"
    return False, "not_found"


def _id_allowed(telegram_user_id: int | None, raw_to: Any, *, deps: OutboxWhitelistDeps) -> bool:
    candidate_ids: set[int] = set()
    if telegram_user_id is not None:
        candidate_ids.add(int(telegram_user_id))
    raw_id = deps.coerce_int_fn(raw_to)
    if raw_id is not None:
        candidate_ids.add(raw_id)
    return any(candidate in deps.whitelist.ids for candidate in candidate_ids)


def _username_allowed(username: str | None, raw_to: Any, *, deps: OutboxWhitelistDeps) -> bool:
    candidate_names: set[str] = set()
    _add_username_candidates(candidate_names, username)
    if isinstance(raw_to, str):
        _add_username_candidates(candidate_names, raw_to)
    return any(name in deps.whitelist.usernames for name in candidate_names)


def _add_username_candidates(candidate_names: set[str], value: str | None) -> None:
    normalized = normalize_username(value)
    if not normalized:
        return
    lowered = normalized.lower()
    candidate_names.add(lowered)
    candidate_names.add(lowered.lstrip("@"))


def _number_candidates(raw_to: Any, *, deps: OutboxWhitelistDeps) -> tuple[set[str], bool]:
    candidates: set[str] = set()
    format_error = False
    if raw_to is not None:
        try:
            candidates.add(deps.normalize_e164_digits_fn(raw_to))
        except WhatsAppAddressError:
            format_error = True
        except Exception:
            format_error = True
    return candidates, format_error


async def _recent_incoming_allows(
    lead_id: int | None,
    tenant_id: int | None,
    *,
    deps: OutboxWhitelistDeps,
) -> bool:
    if not lead_id or lead_id <= 0:
        return False
    try:
        recent = await deps.has_recent_incoming_message_fn(
            int(lead_id),
            tenant_id=int(tenant_id) if tenant_id is not None else None,
            within_seconds=deps.recent_incoming_ttl_seconds,
        )
    except Exception as exc:
        deps.db_error_labels_fn("recent_incoming_check").inc()
        deps.log_fn(
            "event=whitelist_bypass_check status=error reason=db "
            f"lead_id={lead_id} tenant_id={tenant_id} error={exc}"
        )
        return False
    if recent:
        deps.log_fn(
            "event=whitelist_bypass status=allow reason=recent_incoming "
            f"lead_id={lead_id} tenant_id={tenant_id}"
        )
        return True
    return False
