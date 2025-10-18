from __future__ import annotations

import json
from typing import Any, Callable

import pytest
from fastapi.testclient import TestClient

from app.common import get_outbox_whitelist, whitelist_contains_number
from app.transport import WhatsAppAddressError, normalize_whatsapp_recipient


class DummyResponse:
    def __init__(
        self,
        status_code: int,
        json_body: dict[str, Any] | None = None,
        *,
        text: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._json_body = json_body
        if json_body is not None and headers is None:
            headers = {"Content-Type": "application/json"}
        self.headers = headers or {}
        if text is not None:
            self.text = text
        elif json_body is not None:
            self.text = json.dumps(json_body)
        else:
            self.text = ""

    def json(self) -> dict[str, Any]:
        if self._json_body is None:
            raise ValueError("no json body")
        return self._json_body

    @property
    def content(self) -> bytes:
        if self._json_body is not None:
            return json.dumps(self._json_body).encode("utf-8")
        return self.text.encode("utf-8")


class StubTransportClient:
    is_closed = False

    def __init__(self, factory: Callable[[], DummyResponse]) -> None:
        self._factory = factory
        self.calls: list[dict[str, Any]] = []

    async def post(self, endpoint: str, json: dict[str, Any], timeout: Any) -> DummyResponse:
        self.calls.append({"endpoint": endpoint, "json": json})
        return self._factory()


@pytest.mark.parametrize(
    "value,expected",
    [
        ("+79991234567", "79991234567"),
        ("79991234567", "79991234567"),
        ("79991234567@c.us", "79991234567"),
        ("89991234567", "79991234567"),
    ],
)
def test_normalize_whatsapp_recipient_variants(value: str, expected: str) -> None:
    digits, jid = normalize_whatsapp_recipient(value)
    assert digits == expected
    assert jid == f"{expected}@c.us"


def test_outbox_whitelist_csv_and_normalization() -> None:
    whitelist = get_outbox_whitelist(
        {
            "OUTBOX_WHITELIST": " +79991234567 , @demo , 89990000000@c.us ",
        }
    )
    assert whitelist_contains_number(whitelist, "79991234567")
    assert whitelist_contains_number(whitelist, "79990000000")
    assert "@demo" in whitelist.usernames


def test_normalize_whatsapp_recipient_invalid() -> None:
    with pytest.raises(WhatsAppAddressError):
        normalize_whatsapp_recipient("12345")


def _prepare_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    whitelist: str,
    response_factory: Callable[[], DummyResponse],
) -> tuple[TestClient, StubTransportClient]:
    from app import main as main_module

    monkeypatch.setenv("OUTBOX_ENABLED", "true")
    monkeypatch.setenv("OUTBOX_WHITELIST", whitelist)
    monkeypatch.setenv("ADMIN_TOKEN", "test-token")
    monkeypatch.setattr(main_module.settings, "ADMIN_TOKEN", "test-token", raising=False)

    stub = StubTransportClient(response_factory)
    monkeypatch.setattr(main_module, "_transport_client", lambda channel: stub)
    return TestClient(main_module.app), stub


@pytest.mark.parametrize(
    "recipient",
    ["+79991234567", "79991234567", "79991234567@c.us"],
)
def test_send_whatsapp_success(monkeypatch: pytest.MonkeyPatch, recipient: str) -> None:
    response = DummyResponse(200, {"ok": True})
    client, stub = _prepare_app(
        monkeypatch,
        whitelist="79991234567",
        response_factory=lambda: response,
    )

    payload = {
        "tenant": 1,
        "channel": "whatsapp",
        "to": recipient,
        "text": "hello",
    }
    http_response = client.post("/send", json=payload, headers={"X-Admin-Token": "test-token"})

    assert http_response.status_code == 200
    assert http_response.json()["ok"] is True
    assert stub.calls, "transport client must be invoked"
    assert stub.calls[0]["json"]["to"] == "79991234567@c.us"


def test_send_whatsapp_not_whitelisted(monkeypatch: pytest.MonkeyPatch) -> None:
    response = DummyResponse(200, {"ok": True})
    client, stub = _prepare_app(
        monkeypatch,
        whitelist="79990000000",
        response_factory=lambda: response,
    )

    http_response = client.post(
        "/send",
        json={"tenant": 1, "channel": "whatsapp", "to": "+79991234567", "text": "block"},
        headers={"X-Admin-Token": "test-token"},
    )

    assert http_response.status_code == 403
    assert http_response.json()["error"] == "not_whitelisted"
    assert stub.calls == []


def test_send_whatsapp_invalid_number(monkeypatch: pytest.MonkeyPatch) -> None:
    response = DummyResponse(200, {"ok": True})
    client, stub = _prepare_app(
        monkeypatch,
        whitelist="79991234567",
        response_factory=lambda: response,
    )

    http_response = client.post(
        "/send",
        json={"tenant": 1, "channel": "whatsapp", "to": "123", "text": "invalid"},
        headers={"X-Admin-Token": "test-token"},
    )

    assert http_response.status_code == 400
    assert http_response.json()["error"].startswith("invalid_to")
    assert stub.calls == []


def test_send_whatsapp_propagates_waweb_error(monkeypatch: pytest.MonkeyPatch) -> None:
    error_response = DummyResponse(500, {"error": "wa_failure"})
    client, stub = _prepare_app(
        monkeypatch,
        whitelist="79991234567",
        response_factory=lambda: error_response,
    )

    http_response = client.post(
        "/send",
        json={"tenant": 1, "channel": "whatsapp", "to": "79991234567", "text": "boom"},
        headers={"X-Admin-Token": "test-token"},
    )

    assert http_response.status_code == 500
    assert http_response.json() == {"error": "wa_failure"}
    assert stub.calls and stub.calls[0]["json"]["to"] == "79991234567@c.us"
