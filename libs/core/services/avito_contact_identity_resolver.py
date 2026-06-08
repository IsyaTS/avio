from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from libs.core.repo.lead_identity import is_placeholder_display_name


AsyncFn = Callable[..., Awaitable[Any]]
LogFn = Callable[..., None]


@dataclass(frozen=True)
class AvitoContactIdentityInput:
    tenant_id: int
    lead_id: int
    contact_id: int | None
    account_id: int | None
    chat_id: str
    author_id: int | None
    current_login: str | None = None
    current_contact: str | None = None


@dataclass(frozen=True)
class AvitoContactIdentityResult:
    resolved: bool
    name: str | None = None
    source: str = "none"
    reason: str | None = None
    updated_contact: bool = False
    updated_lead: bool = False


@dataclass(frozen=True)
class AvitoContactIdentityDeps:
    resolve_chat_participant_profile_fn: AsyncFn
    update_contact_avito_login_fn: AsyncFn
    update_lead_contact_fn: AsyncFn
    redis_client: Any = None
    log_fn: LogFn = lambda *_args, **_kwargs: None
    cache_ttl_seconds: int = 3600 * 24 * 14


def is_missing_or_numeric_name(value: Any) -> bool:
    return is_placeholder_display_name(value)


async def resolve_and_store_avito_contact_identity(
    data: AvitoContactIdentityInput,
    *,
    deps: AvitoContactIdentityDeps,
) -> AvitoContactIdentityResult:
    current_login = str(data.current_login or "").strip()
    if current_login and not is_missing_or_numeric_name(current_login):
        return AvitoContactIdentityResult(resolved=True, name=current_login, source="payload")
    if not data.account_id or not str(data.chat_id or "").strip():
        return AvitoContactIdentityResult(resolved=False, reason="missing_context")

    cached_name = await _get_cached_name(data, deps=deps)
    if cached_name:
        return await _persist_name(data, cached_name, source="cache", deps=deps)

    try:
        profile = await deps.resolve_chat_participant_profile_fn(
            int(data.tenant_id),
            account_id=int(data.account_id),
            chat_id=str(data.chat_id),
            author_id=data.author_id,
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_contact_identity_api_failed tenant=%s account_id=%s lead_id=%s error=%s",
            data.tenant_id,
            data.account_id,
            data.lead_id,
            type(exc).__name__,
        )
        return AvitoContactIdentityResult(resolved=False, reason="api_error")

    name = _valid_profile_name(profile, data)
    if not name:
        return AvitoContactIdentityResult(resolved=False, reason="name_not_found")
    await _set_cached_name(data, name, deps=deps)
    return await _persist_name(data, name, source="api", deps=deps)


async def _persist_name(
    data: AvitoContactIdentityInput,
    name: str,
    *,
    source: str,
    deps: AvitoContactIdentityDeps,
) -> AvitoContactIdentityResult:
    updated_contact = False
    updated_lead = False
    try:
        if data.contact_id and int(data.contact_id) > 0:
            await deps.update_contact_avito_login_fn(int(data.contact_id), name)
            updated_contact = True
    except Exception as exc:
        deps.log_fn(
            "event=avito_contact_identity_contact_update_failed tenant=%s lead_id=%s error=%s",
            data.tenant_id,
            data.lead_id,
            type(exc).__name__,
        )
    try:
        updated_lead = bool(
            await deps.update_lead_contact_fn(int(data.tenant_id), int(data.lead_id), name)
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_contact_identity_lead_update_failed tenant=%s lead_id=%s error=%s",
            data.tenant_id,
            data.lead_id,
            type(exc).__name__,
        )
    return AvitoContactIdentityResult(
        resolved=True,
        name=name,
        source=source,
        updated_contact=updated_contact,
        updated_lead=updated_lead,
    )


def _valid_profile_name(profile: Any, data: AvitoContactIdentityInput) -> str:
    if not isinstance(profile, dict):
        return ""
    name = str(profile.get("name") or "").strip()
    if not name or is_missing_or_numeric_name(name):
        return ""
    forbidden = {str(item) for item in (data.account_id, data.author_id) if item is not None}
    if name in forbidden:
        return ""
    return name[:160]


def _cache_key(data: AvitoContactIdentityInput) -> str:
    subject = data.author_id if data.author_id is not None else str(data.chat_id or "").strip()
    return f"cache:avito_contact_identity:{int(data.tenant_id)}:{int(data.account_id or 0)}:{subject}"


async def _get_cached_name(
    data: AvitoContactIdentityInput,
    *,
    deps: AvitoContactIdentityDeps,
) -> str:
    redis_client = deps.redis_client
    if redis_client is None:
        return ""
    try:
        raw = await redis_client.get(_cache_key(data))
    except Exception:
        return ""
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    name = str(raw or "").strip()
    return name if name and not is_missing_or_numeric_name(name) else ""


async def _set_cached_name(
    data: AvitoContactIdentityInput,
    name: str,
    *,
    deps: AvitoContactIdentityDeps,
) -> None:
    redis_client = deps.redis_client
    if redis_client is None:
        return
    try:
        await redis_client.set(_cache_key(data), name, ex=max(60, int(deps.cache_ttl_seconds or 0)))
    except Exception:
        return


__all__ = [
    "AvitoContactIdentityDeps",
    "AvitoContactIdentityInput",
    "AvitoContactIdentityResult",
    "is_missing_or_numeric_name",
    "resolve_and_store_avito_contact_identity",
]
