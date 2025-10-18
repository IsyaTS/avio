from __future__ import annotations

import pytest

from app.common import OutboxWhitelist
from app import worker as worker_module


@pytest.mark.anyio
async def test_worker_whitelist_bypass_recent_incoming(monkeypatch: pytest.MonkeyPatch) -> None:
    whitelist = OutboxWhitelist(
        allow_all=False,
        ids=frozenset(),
        usernames=frozenset(),
        numbers=frozenset(),
        numbers_with_plus=frozenset(),
        raw_tokens=frozenset(),
        raw_value="",
    )
    monkeypatch.setattr(worker_module, "OUTBOX_WHITELIST", whitelist, raising=False)

    async def _fake_recent(lead_id: int, tenant_id: int | None = None, *, within_seconds: int = 0) -> bool:
        assert lead_id == 101
        assert tenant_id == 7
        assert within_seconds == worker_module.RECENT_INCOMING_TTL_SECONDS
        return True

    captured_logs: list[str] = []

    monkeypatch.setattr(worker_module, "has_recent_incoming_message", _fake_recent, raising=False)
    monkeypatch.setattr(worker_module, "log", lambda msg: captured_logs.append(msg), raising=False)

    allowed, reason = await worker_module._whitelist_allows(
        telegram_user_id=None,
        username=None,
        raw_to="+79991234567",
        lead_id=101,
        tenant_id=7,
        channel="whatsapp",
    )

    assert allowed is True
    assert reason == "recent_incoming"
    assert any("whitelist_bypass" in entry for entry in captured_logs)
