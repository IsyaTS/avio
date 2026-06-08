from __future__ import annotations

from typing import Any

import pytest

from libs.core.repo import lead_identity


pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("", True),
        ("123456", True),
        ("Avito · клиент", True),
        ("chat-1", True),
        ("Наталья", False),
    ],
)
def test_is_placeholder_display_name(value: str, expected: bool) -> None:
    assert lead_identity.is_placeholder_display_name(value, peer="chat-1") is expected


@pytest.mark.anyio
async def test_update_avito_lead_contact_updates_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    updates: list[tuple[Any, ...]] = []

    async def _fetchrow(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"contact": "123", "title": "Avito · клиент", "peer": "chat-1"}

    async def _exec(_query: str, *args: Any) -> None:
        updates.append(args)

    monkeypatch.setattr(lead_identity.db_module, "_fetchrow", _fetchrow)
    monkeypatch.setattr(lead_identity.db_module, "_exec", _exec)

    changed = await lead_identity.update_avito_lead_contact_if_placeholder(101, 501, "Наталья")

    assert changed is True
    assert updates == [(101, 501, True, True, "Наталья")]


@pytest.mark.anyio
async def test_update_avito_lead_contact_does_not_overwrite_real_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fetchrow(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"contact": "Иван", "title": "Иван", "peer": "chat-1"}

    async def _exec(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("should not update")

    monkeypatch.setattr(lead_identity.db_module, "_fetchrow", _fetchrow)
    monkeypatch.setattr(lead_identity.db_module, "_exec", _exec)

    changed = await lead_identity.update_avito_lead_contact_if_placeholder(101, 501, "Наталья")

    assert changed is False
