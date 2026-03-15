from __future__ import annotations

from typing import Any

import pytest

from apps.worker import main as worker_module


def test_split_reply_for_send_does_not_inject_contact_intro() -> None:
    parts = worker_module._split_reply_for_send(
        "Пишите в Telegram @dverigermes или звоните 89866666133",
        "telegram",
    )

    normalized = [str(part or "").strip().casefold() for part in parts]
    assert "контакты для связи" not in normalized
    assert any("@dverigermes" in str(part or "") for part in parts)


@pytest.mark.anyio
async def test_send_telegram_status_zero_has_no_default_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> tuple[int, str]:
        calls.append(dict(kwargs))
        return 0, "network_timeout"

    monkeypatch.setenv("TG_SEND_RETRY_ON_UNKNOWN", "0")
    monkeypatch.delenv("TG_SEND_TEXT_TIMEOUT", raising=False)
    monkeypatch.setattr(worker_module.telegram_transport, "send", fake_send)

    status, body = await worker_module.send_telegram(
        tenant_id=101,
        tg_slot=1,
        chat_id=944310340,
        peer_id=944310340,
        peer="944310340",
        telegram_user_id=944310340,
        username="@Isyyaa",
        text="тест",
        lead_id=1,
    )

    assert status == 0
    assert body == "network_timeout"
    assert len(calls) == 1
    assert float(calls[0].get("timeout") or 0.0) == pytest.approx(40.0)


@pytest.mark.anyio
async def test_send_telegram_retries_for_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_sleep(_seconds: float) -> None:
        return None

    async def fake_send(**kwargs: Any) -> tuple[int, str]:
        calls.append(dict(kwargs))
        if len(calls) < 3:
            return 500, '{"error":"send_failed"}'
        return 200, '{"ok":true}'

    monkeypatch.setattr(worker_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(worker_module.telegram_transport, "send", fake_send)

    status, _ = await worker_module.send_telegram(
        tenant_id=101,
        tg_slot=1,
        chat_id=944310340,
        peer_id=944310340,
        peer="944310340",
        telegram_user_id=944310340,
        username="@Isyyaa",
        text="тест",
        lead_id=1,
    )

    assert status == 200
    assert len(calls) == 3


@pytest.mark.anyio
async def test_send_telegram_attachment_timeout_default(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_send(**kwargs: Any) -> tuple[int, str]:
        calls.append(dict(kwargs))
        return 200, '{"ok":true}'

    monkeypatch.delenv("TG_SEND_ATTACH_TIMEOUT", raising=False)
    monkeypatch.setattr(worker_module.telegram_transport, "send", fake_send)

    status, _ = await worker_module.send_telegram(
        tenant_id=101,
        tg_slot=1,
        chat_id=944310340,
        peer_id=944310340,
        peer="944310340",
        telegram_user_id=944310340,
        username="@Isyyaa",
        text="",
        attachments=[{"type": "image", "url": "https://example.com/a.jpg"}],
        lead_id=1,
    )

    assert status == 200
    assert len(calls) == 1
    assert float(calls[0].get("timeout") or 0.0) == pytest.approx(90.0)
