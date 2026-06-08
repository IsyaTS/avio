from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from apps.api.web.services import max_public_runtime


pytestmark = pytest.mark.unit


def _request(method: str = "GET", body: bytes = b"") -> Request:
    async def _receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/v1/max/test",
            "headers": [(b"host", b"dev.avio.website")],
            "query_string": b"",
            "server": ("dev.avio.website", 443),
            "scheme": "https",
            "client": ("testclient", 1),
        },
        _receive,
    )


async def _auth(_request, tenant, _key):
    return int(tenant or 7), "key"


def test_max_webhook_url_includes_secret_when_present() -> None:
    request = _request()

    result = max_public_runtime.max_webhook_url(
        request,
        7,
        "secret",
        public_url_fn=lambda _request, tail: f"https://example.test{tail}",
    )

    assert result == "https://example.test/webhook/max?tenant=7&token=secret"


@pytest.mark.asyncio
async def test_max_connect_stores_token_and_webhook_state() -> None:
    updates: list[dict] = []

    class Integration:
        @staticmethod
        def get_integration(_tenant):
            return {}

        @staticmethod
        def update_integration(_tenant, payload):
            updates.append(dict(payload))

        @staticmethod
        async def ensure_webhook(_tenant, webhook_url):
            assert webhook_url.endswith("token=fixed-secret")
            return True

    deps = max_public_runtime.MaxPublicDeps(
        authorize_fn=_auth,
        max_integration=Integration,
        logger=SimpleNamespace(warning=lambda *a, **k: None, exception=lambda *a, **k: None),
        public_url_fn=lambda _request, tail: f"https://example.test{tail}",
        secrets_module=SimpleNamespace(token_urlsafe=lambda _size: "fixed-secret"),
        time_module=SimpleNamespace(time=lambda: 1000),
    )

    response = await max_public_runtime.max_connect(
        _request("POST", json.dumps({"token": "max-token"}).encode("utf-8")),
        7,
        "key",
        deps,
    )

    assert response.status_code == 200
    assert updates[0]["bot_token"] == "max-token"
    assert updates[0]["webhook_secret"] == "fixed-secret"
    assert updates[1]["webhook_registered"] is True


@pytest.mark.asyncio
async def test_max_personal_send_validates_required_fields() -> None:
    deps = max_public_runtime.MaxPersonalDeps(
        authorize_fn=_auth,
        service=SimpleNamespace(),
        transport=SimpleNamespace(),
        refresh_status_fn=lambda _tenant: None,
        callback_url_fn=lambda *_a: "",
    )

    response = await max_public_runtime.max_personal_send(
        _request("POST", json.dumps({"to": "chat-1"}).encode("utf-8")),
        7,
        "key",
        deps,
    )

    assert response.status_code == 400
    assert json.loads(response.body)["detail"] == "text_required"


@pytest.mark.asyncio
async def test_refresh_max_personal_status_persists_worker_state() -> None:
    updates: list[dict] = []

    async def _get_status(_tenant):
        return 200, {
            "status": "authorized",
            "last_error": None,
            "account": {"display_name": "A"},
            "last_heartbeat": 123,
        }

    deps = max_public_runtime.MaxPersonalDeps(
        authorize_fn=_auth,
        service=SimpleNamespace(update_integration=lambda _tenant, payload: updates.append(dict(payload))),
        transport=SimpleNamespace(get_status=_get_status),
        refresh_status_fn=lambda _tenant: None,
        callback_url_fn=lambda *_a: "",
    )

    result = await max_public_runtime.refresh_max_personal_status(7, deps)

    assert result["status"] == "authorized"
    assert updates == [
        {
            "session_status": "authorized",
            "session_last_error": None,
            "account": {"display_name": "A"},
            "last_heartbeat": 123,
        }
    ]
