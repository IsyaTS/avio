from __future__ import annotations

import json

import pytest
from starlette.requests import Request

from apps.api.web import public as public_module


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _request(method: str = "GET", path: str = "/v1/max-personal/status", body: bytes = b"") -> Request:
    async def _receive():
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "headers": [(b"host", b"dev.avio.website")],
            "query_string": b"",
            "server": ("dev.avio.website", 443),
            "scheme": "https",
            "client": ("testclient", 1),
        },
        _receive,
    )


@pytest.mark.anyio("asyncio")
async def test_max_personal_status_endpoint(monkeypatch):
    async def _auth(*_args, **_kwargs):
        return (101, "key")

    monkeypatch.setattr(
        public_module,
        "_authorize_public_settings_request",
        _auth,
        raising=False,
    )
    monkeypatch.setattr(
        public_module,
        "_max_personal_refresh_status",
        lambda tenant_id: _return_async(
            {"status": "waiting_qr", "account": {}, "last_error": None}
        ),
        raising=False,
    )
    monkeypatch.setattr(
        public_module.max_personal_service,
        "build_state_payload",
        lambda tenant_id, _: {
            "tenant": tenant_id,
            "enabled": True,
            "outbound_enabled": True,
            "status": "waiting_qr",
            "connected": False,
            "kill_switch": False,
            "account": {},
            "last_error": None,
        },
        raising=False,
    )

    resp = await public_module.max_personal_status(_request(), tenant=101, k="test")

    assert resp.status_code == 200
    body = json.loads(resp.body)
    assert body["tenant"] == 101
    assert body["status"] == "waiting_qr"
    assert body["qr_required"] is True
    assert body["worker"]["status"] == "waiting_qr"


@pytest.mark.anyio("asyncio")
async def test_max_personal_connect_endpoint(monkeypatch):
    updates: list[dict] = []

    async def _auth(*_args, **_kwargs):
        return (202, "key")

    monkeypatch.setattr(
        public_module,
        "_authorize_public_settings_request",
        _auth,
        raising=False,
    )
    monkeypatch.setattr(
        public_module.max_personal_service,
        "ensure_event_secret",
        lambda tenant_id: "event-secret",
        raising=False,
    )
    monkeypatch.setattr(
        public_module.max_personal_transport,
        "start_session",
        lambda tenant_id, **kwargs: _return_async(
            (
                200,
                {
                    "status": "waiting_qr",
                    "account": {"display_name": "MAX account"},
                    "last_error": None,
                },
            )
        ),
        raising=False,
    )
    monkeypatch.setattr(
        public_module.max_personal_service,
        "update_integration",
        lambda _tenant_id, payload: updates.append(dict(payload)),
        raising=False,
    )
    monkeypatch.setattr(
        public_module.max_personal_service,
        "build_state_payload",
        lambda tenant_id, session: {
            "tenant": tenant_id,
            "enabled": True,
            "outbound_enabled": True,
            "status": str((session or {}).get("status") or "idle"),
            "connected": False,
            "kill_switch": False,
            "account": (session or {}).get("account") or {},
            "last_error": (session or {}).get("last_error"),
        },
        raising=False,
    )

    resp = await public_module.max_personal_connect(
        _request(
            "POST",
            "/v1/max-personal/connect",
            json.dumps({"force": True}).encode("utf-8"),
        ),
        tenant=202,
        k="test",
    )

    assert resp.status_code == 200
    body = json.loads(resp.body)
    assert body["enabled"] is True
    assert body["status"] == "waiting_qr"
    assert body["callback_url"].endswith("/webhook/max_personal?tenant=202&token=event-secret")
    assert updates
    assert updates[-1]["enabled"] is True


async def _return_async(value):
    return value
