from __future__ import annotations

import pytest

from apps.api.web import public as public_module
from apps.api.web import webhooks as webhooks_module


pytestmark = pytest.mark.unit


@pytest.mark.asyncio
async def test_public_manager_capture_hook_calls_learning_service(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_capture_intervention_episode(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(
        public_module,
        "capture_intervention_episode",
        _fake_capture_intervention_episode,
        raising=False,
    )

    await public_module._capture_manager_intervention(
        tenant_id=101,
        lead_id=555,
        channel="telegram",
        manager_message_id=777,
        source_event="unit_test",
    )

    assert int(captured["tenant_id"]) == 101
    assert int(captured["lead_id"]) == 555
    assert str(captured["channel"]) == "telegram"
    assert str(captured["source_event"]) == "unit_test"
    assert int(captured["manager_message_id"]) == 777


@pytest.mark.asyncio
async def test_public_manager_capture_hook_skips_without_message_id(monkeypatch):
    called = False

    async def _fake_capture_intervention_episode(**_kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(
        public_module,
        "capture_intervention_episode",
        _fake_capture_intervention_episode,
        raising=False,
    )

    await public_module._capture_manager_intervention(
        tenant_id=101,
        lead_id=555,
        channel="telegram",
        manager_message_id=None,
        source_event="unit_test",
    )

    assert called is False


@pytest.mark.asyncio
async def test_webhooks_manager_capture_hook_calls_learning_service(monkeypatch):
    captured: dict[str, object] = {}

    async def _fake_capture_intervention_episode(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(
        webhooks_module,
        "capture_intervention_episode",
        _fake_capture_intervention_episode,
        raising=False,
    )

    await webhooks_module._capture_manager_intervention(
        tenant_id=3,
        lead_id=909,
        channel="avito",
        manager_message_id=321,
        source_event="unit_test",
    )

    assert int(captured["tenant_id"]) == 3
    assert int(captured["lead_id"]) == 909
    assert str(captured["channel"]) == "avito"
    assert str(captured["source_event"]) == "unit_test"
    assert int(captured["manager_message_id"]) == 321
