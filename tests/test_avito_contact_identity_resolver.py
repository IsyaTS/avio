from __future__ import annotations

from typing import Any

import pytest

from libs.core.services import avito_contact_identity_resolver as resolver


pytestmark = pytest.mark.unit


class _Redis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str, *, ex: int | None = None) -> None:
        self.values[key] = value


def _input(**overrides: Any) -> resolver.AvitoContactIdentityInput:
    data = {
        "tenant_id": 101,
        "lead_id": 501,
        "contact_id": 701,
        "account_id": 222,
        "chat_id": "chat-1",
        "author_id": 333,
        "current_login": None,
        "current_contact": "333",
    }
    data.update(overrides)
    return resolver.AvitoContactIdentityInput(**data)


async def _noop_update_contact(*_args: Any, **_kwargs: Any) -> None:
    return None


async def _noop_update_lead(*_args: Any, **_kwargs: Any) -> bool:
    return False


@pytest.mark.anyio
async def test_current_real_login_skips_api() -> None:
    async def _api(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("api should not be called")

    result = await resolver.resolve_and_store_avito_contact_identity(
        _input(current_login="Наталья"),
        deps=resolver.AvitoContactIdentityDeps(
            resolve_chat_participant_profile_fn=_api,
            update_contact_avito_login_fn=_noop_update_contact,
            update_lead_contact_fn=_noop_update_lead,
        ),
    )

    assert result.resolved is True
    assert result.name == "Наталья"
    assert result.source == "payload"


@pytest.mark.anyio
async def test_empty_login_resolves_and_updates_contact_and_lead() -> None:
    contact_updates: list[tuple[int, str]] = []
    lead_updates: list[tuple[int, int, str]] = []

    async def _api(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"name": "Никита Райский", "user_id": 333}

    async def _update_contact(contact_id: int, name: str) -> None:
        contact_updates.append((contact_id, name))

    async def _update_lead(tenant_id: int, lead_id: int, name: str) -> bool:
        lead_updates.append((tenant_id, lead_id, name))
        return True

    result = await resolver.resolve_and_store_avito_contact_identity(
        _input(current_login=""),
        deps=resolver.AvitoContactIdentityDeps(
            resolve_chat_participant_profile_fn=_api,
            update_contact_avito_login_fn=_update_contact,
            update_lead_contact_fn=_update_lead,
        ),
    )

    assert result.resolved is True
    assert result.name == "Никита Райский"
    assert contact_updates == [(701, "Никита Райский")]
    assert lead_updates == [(101, 501, "Никита Райский")]


@pytest.mark.anyio
async def test_numeric_api_name_is_ignored() -> None:
    async def _api(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"name": "333"}

    result = await resolver.resolve_and_store_avito_contact_identity(
        _input(current_login="333"),
        deps=resolver.AvitoContactIdentityDeps(
            resolve_chat_participant_profile_fn=_api,
            update_contact_avito_login_fn=_noop_update_contact,
            update_lead_contact_fn=_noop_update_lead,
        ),
    )

    assert result.resolved is False
    assert result.reason == "name_not_found"


@pytest.mark.anyio
async def test_missing_context_does_not_call_api() -> None:
    async def _api(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("api should not be called")

    result = await resolver.resolve_and_store_avito_contact_identity(
        _input(account_id=None),
        deps=resolver.AvitoContactIdentityDeps(
            resolve_chat_participant_profile_fn=_api,
            update_contact_avito_login_fn=_noop_update_contact,
            update_lead_contact_fn=_noop_update_lead,
        ),
    )

    assert result.resolved is False
    assert result.reason == "missing_context"


@pytest.mark.anyio
async def test_cache_hit_avoids_api() -> None:
    redis = _Redis()
    await redis.set("cache:avito_contact_identity:101:222:333", "Света")
    contact_updates: list[str] = []

    async def _api(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("api should not be called")

    async def _update_contact(_contact_id: int, name: str) -> None:
        contact_updates.append(name)

    result = await resolver.resolve_and_store_avito_contact_identity(
        _input(),
        deps=resolver.AvitoContactIdentityDeps(
            resolve_chat_participant_profile_fn=_api,
            update_contact_avito_login_fn=_update_contact,
            update_lead_contact_fn=_noop_update_lead,
            redis_client=redis,
        ),
    )

    assert result.resolved is True
    assert result.name == "Света"
    assert result.source == "cache"
    assert contact_updates == ["Света"]
