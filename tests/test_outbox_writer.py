from __future__ import annotations

import json
from typing import Any

import pytest

from apps.worker.services import outbox_writer


pytestmark = pytest.mark.unit


class _MetricLabels:
    def inc(self) -> None:
        return None


class _RedisFake:
    def __init__(self) -> None:
        self.items: list[tuple[str, dict[str, Any]]] = []

    async def rpush(self, key: str, value: str) -> int:
        self.items.append((key, json.loads(value)))
        return len(self.items)


def _deps(redis: _RedisFake, logs: list[str]) -> outbox_writer.OutboxWriterDeps:
    return outbox_writer.OutboxWriterDeps(
        redis_client=redis,
        outbox_queue_key="outbox:send",
        app_version="test-version",
        default_tenant_id=1,
        log_fn=lambda message, *args: logs.append(str(message % args if args else message)),
        collect_outgoing_attachments_fn=lambda _item, _tenant: [{"filename": "file.pdf"}],
        is_manager_message_fn=lambda item: bool(item.get("manager")),
        is_followup_message_fn=lambda item: bool(item.get("followup")),
        db_error_labels_fn=lambda _name: _MetricLabels(),
    )


@pytest.mark.asyncio
async def test_write_result_upserts_message_enqueues_status_and_captures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _RedisFake()
    logs: list[str] = []
    calls: list[tuple[str, Any]] = []

    async def fake_upsert_lead(lead_id: int, **kwargs: Any) -> int:
        calls.append(("upsert", (lead_id, kwargs)))
        return 303

    async def fake_lead_exists(lead_id: int, *, tenant_id: int) -> bool:
        calls.append(("exists", (lead_id, tenant_id)))
        return True

    async def fake_insert_message_out(
        lead_id: int,
        text: str,
        provider_msg_id: str | None = None,
        **kwargs: Any,
    ) -> int:
        calls.append(("insert", (lead_id, text, provider_msg_id, kwargs)))
        return 404

    async def fake_capture(**kwargs: Any) -> None:
        calls.append(("capture", kwargs))

    monkeypatch.setattr(outbox_writer, "upsert_lead", fake_upsert_lead)
    monkeypatch.setattr(outbox_writer, "lead_exists", fake_lead_exists)
    monkeypatch.setattr(outbox_writer, "insert_message_out", fake_insert_message_out)
    monkeypatch.setattr(outbox_writer, "capture_intervention_episode", fake_capture)

    await outbox_writer.write_result(
        {
            "provider": "telegram",
            "tenant": 101,
            "lead_id": 202,
            "text": "hello",
            "telegram_user_id": 505,
            "manager": True,
        },
        "sent",
        200,
        "ok",
        deps=_deps(redis, logs),
    )

    assert [name for name, _payload in calls] == ["upsert", "exists", "insert", "capture"]
    capture_kwargs = calls[3][1]
    assert capture_kwargs["tenant_id"] == 101
    assert capture_kwargs["lead_id"] == 303
    assert capture_kwargs["channel"] == "telegram"
    assert capture_kwargs["source_event"] == "manager_outgoing"
    assert capture_kwargs["manager_message_id"] == 404
    assert callable(capture_kwargs["log_fn"])
    assert redis.items == [
        (
            "outbox:send",
            {
                "lead_id": 202,
                "reply": "hello",
                "status": "sent",
                "version": "test-version",
                "ch": "telegram",
            },
        )
    ]
    assert any("event=enqueue_outbox" in row for row in logs)


@pytest.mark.asyncio
async def test_write_result_existing_telegram_message_skips_insert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _RedisFake()
    logs: list[str] = []
    calls: list[str] = []

    async def fail_upsert(*_args: Any, **_kwargs: Any) -> int:
        calls.append("upsert")
        raise AssertionError("upsert should not be called")

    async def fail_insert(*_args: Any, **_kwargs: Any) -> int:
        calls.append("insert")
        raise AssertionError("insert should not be called")

    async def fake_capture(**kwargs: Any) -> None:
        calls.append(f"capture:{kwargs['manager_message_id']}")

    monkeypatch.setattr(outbox_writer, "upsert_lead", fail_upsert)
    monkeypatch.setattr(outbox_writer, "insert_message_out", fail_insert)
    monkeypatch.setattr(outbox_writer, "capture_intervention_episode", fake_capture)

    await outbox_writer.write_result(
        {
            "provider": "telegram",
            "tenant": 101,
            "lead_id": 202,
            "text": "hello",
            "_message_db_id": 404,
            "manager": True,
        },
        "sent",
        200,
        "ok",
        deps=_deps(redis, logs),
    )

    assert calls == ["capture:404"]
    assert redis.items[0][1]["lead_id"] == 202
