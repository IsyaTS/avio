from __future__ import annotations

from typing import Any

import pytest

from apps.worker.services import notification_dispatcher


pytestmark = pytest.mark.unit


class FakeResponse:
    def __init__(self, status_code: int, text: str = "", payload: dict[str, Any] | None = None) -> None:
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self) -> dict[str, Any]:
        if self._payload is None:
            raise ValueError("not json")
        return self._payload


class FakeAsyncClient:
    calls: list[dict[str, Any]] = []
    response = FakeResponse(200)

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout

    async def __aenter__(self) -> "FakeAsyncClient":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def post(self, url: str, json: dict[str, Any]) -> FakeResponse:
        self.calls.append({"url": url, "json": json, "timeout": self.timeout})
        return self.response


class FakeHttpx:
    HTTPError = RuntimeError
    AsyncClient = FakeAsyncClient


def _deps(logs: list[str], calls: list[tuple[str, Any]]) -> notification_dispatcher.NotificationDispatcherDeps:
    async def send_notify_bot(chat_id: int, text: str) -> tuple[bool, int, str]:
        calls.append(("notify_bot", (chat_id, text)))
        return True, 200, ""

    return notification_dispatcher.NotificationDispatcherDeps(
        default_tenant_id=1,
        admin_token="admin-token",
        notify_bot_enabled=True,
        log_fn=lambda message, *args: logs.append(str(message % args if args else message)),
        notification_chat_ids_fn=lambda tenant_id, event: [111, 222] if (tenant_id, event) == (12, "handoff") else [],
        send_notify_bot_fn=send_notify_bot,
    )


@pytest.mark.asyncio
async def test_send_notify_bot_posts_html_message() -> None:
    FakeAsyncClient.calls = []
    FakeAsyncClient.response = FakeResponse(200)

    ok, status, error = await notification_dispatcher.send_notify_bot(
        123,
        "manager needed",
        token="token",
        httpx_module=FakeHttpx,
    )

    assert (ok, status, error) == (True, 200, "")
    assert FakeAsyncClient.calls == [
        {
            "url": "https://api.telegram.org/bottoken/sendMessage",
            "json": {
                "chat_id": 123,
                "text": "manager needed",
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            "timeout": 10.0,
        }
    ]


@pytest.mark.asyncio
async def test_send_notify_bot_reports_api_error() -> None:
    FakeAsyncClient.calls = []
    FakeAsyncClient.response = FakeResponse(400, payload={"description": "bad request"})

    ok, status, error = await notification_dispatcher.send_notify_bot(
        123,
        "manager needed",
        token="token",
        httpx_module=FakeHttpx,
    )

    assert (ok, status, error) == (False, 400, "bad request")


@pytest.mark.asyncio
async def test_process_notification_sends_via_notify_bot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs: list[str] = []
    calls: list[tuple[str, Any]] = []

    async def fail_fallback(**kwargs: Any) -> tuple[int, str]:
        raise AssertionError(f"fallback should not run: {kwargs}")

    monkeypatch.setattr(notification_dispatcher.telegram_transport, "send", fail_fallback)

    await notification_dispatcher.process_notification(
        {
            "tenant": 12,
            "event": "handoff",
            "lead_id": 303,
            "text": "manager needed",
        },
        deps=_deps(logs, calls),
    )

    assert calls == [
        ("notify_bot", (111, "manager needed")),
        ("notify_bot", (222, "manager needed")),
    ]
    assert any("event=notify_dispatch tenant=12 lead_id=303 event=handoff" in row for row in logs)
    assert any("event=notify_send_success tenant=12 lead_id=303 event=handoff chat_id=111 status=200" in row for row in logs)


@pytest.mark.asyncio
async def test_process_notification_falls_back_to_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logs: list[str] = []
    calls: list[tuple[str, Any]] = []

    async def fail_notify_bot(chat_id: int, text: str) -> tuple[bool, int, str]:
        calls.append(("notify_bot", (chat_id, text)))
        return False, 500, "bot_failed"

    async def fake_transport_send(**kwargs: Any) -> tuple[int, str]:
        calls.append(("transport", kwargs))
        return 200, "ok"

    monkeypatch.setattr(notification_dispatcher.telegram_transport, "send", fake_transport_send)

    deps = notification_dispatcher.NotificationDispatcherDeps(
        default_tenant_id=1,
        admin_token="admin-token",
        notify_bot_enabled=False,
        log_fn=lambda message, *args: logs.append(str(message % args if args else message)),
        notification_chat_ids_fn=lambda tenant_id, event: [111] if (tenant_id, event) == (12, "handoff") else [],
        send_notify_bot_fn=fail_notify_bot,
    )

    await notification_dispatcher.process_notification(
        {
            "tenant": 12,
            "event": "handoff",
            "lead_id": 303,
            "text": "manager needed",
        },
        deps=deps,
    )

    assert calls == [
        (
            "transport",
            {
                "tenant": 12,
                "peer": "111",
                "text": "manager needed",
                "headers": {"X-Admin-Token": "admin-token"},
            },
        )
    ]
    assert any("event=notify_send_success tenant=12 lead_id=303 event=handoff chat_id=111 status=200" in row for row in logs)


@pytest.mark.asyncio
async def test_process_notification_skips_empty_text() -> None:
    logs: list[str] = []
    calls: list[tuple[str, Any]] = []

    await notification_dispatcher.process_notification(
        {"tenant": 12, "event": "handoff", "lead_id": 303},
        deps=_deps(logs, calls),
    )

    assert calls == []
    assert logs == [
        "event=notify_skip reason=empty_text tenant=12 lead_id=303 event=handoff"
    ]
