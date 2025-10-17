from fastapi import FastAPI
import asyncio
import json

from fastapi.testclient import TestClient

from app.web import public as public_module


def _build_app(monkeypatch):
    app = FastAPI()
    app.include_router(public_module.router)

    return app


def _configure_retry(monkeypatch, attempts=1, delay=0.0):
    monkeypatch.setattr(public_module.settings, "WA_QR_FETCH_ATTEMPTS", attempts, raising=False)
    monkeypatch.setattr(public_module.settings, "WA_QR_FETCH_RETRY_DELAY", delay, raising=False)


class _DummyRedis:
    def __init__(self, store: dict[str, str]):
        self.store = store

    def get(self, key: str):  # pragma: no cover - simple test helper
        return self.store.get(key)


def test_wa_qr_svg_serves_cached_value(monkeypatch):
    _configure_retry(monkeypatch, attempts=1, delay=0.0)
    monkeypatch.setattr(public_module, "WA_ENABLED", True, raising=False)
    monkeypatch.setattr(public_module, "_expected_public_key_value", lambda: "global-public-key")

    valid_calls: list[tuple[int, str]] = []

    def _fake_valid_key(tenant_id: int, key: str) -> bool:
        valid_calls.append((tenant_id, key))
        return tenant_id == 123 and key == "tenant-access-key"

    monkeypatch.setattr(public_module.common, "valid_key", _fake_valid_key)

    store = {
        "wa:qr:last:123": "abc123",
        "wa:qr:123:abc123": json.dumps({"qr_svg": "<svg></svg>", "tenant": 123, "ts": "abc123"}),
    }
    dummy = _DummyRedis(store)
    monkeypatch.setattr(public_module.common, "redis_client", lambda: dummy)

    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp = client.get("/pub/wa/qr.svg", params={"tenant": 123, "k": "tenant-access-key"})

    assert resp.status_code == 200
    assert resp.headers.get("content-type", "").startswith("image/svg+xml")
    cache_header = resp.headers.get("cache-control", "")
    assert cache_header.split(",")[0] == "no-store"
    assert resp.headers.get("x-wa-qr-id") == "abc123"
    assert resp.text == "<svg></svg>"
    assert valid_calls == [(123, "tenant-access-key")]


def test_wa_qr_svg_returns_404_when_cache_empty(monkeypatch):
    _configure_retry(monkeypatch, attempts=1, delay=0.0)
    monkeypatch.setattr(public_module, "WA_ENABLED", True, raising=False)
    monkeypatch.setattr(public_module, "_expected_public_key_value", lambda: "global-public-key")

    valid_calls: list[tuple[int, str]] = []

    def _fake_valid_key(tenant_id: int, key: str) -> bool:
        valid_calls.append((tenant_id, key))
        return tenant_id == 1 and key == "tenant-key"

    monkeypatch.setattr(public_module.common, "valid_key", _fake_valid_key)
    monkeypatch.setattr(public_module.common, "redis_client", lambda: _DummyRedis({}))

    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp = client.get("/pub/wa/qr.svg", params={"tenant": 1, "k": "tenant-key"})
    assert resp.status_code == 404
    cache_header = resp.headers.get("cache-control", "")
    assert cache_header.split(",")[0] == "no-store"
    assert valid_calls == [(1, "tenant-key")]


def test_wa_qr_routes_reject_missing_query_args(monkeypatch):
    _configure_retry(monkeypatch, attempts=1, delay=0.0)
    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp_svg = client.get("/pub/wa/qr.svg")
    assert resp_svg.status_code == 422

    resp_png = client.get("/pub/wa/qr.png", params={"tenant": 1})
    assert resp_png.status_code == 422


def test_wa_status_accepts_tenant_valid_key(monkeypatch):
    _configure_retry(monkeypatch, attempts=1, delay=0.0)
    monkeypatch.setattr(public_module, "_expected_public_key_value", lambda: "global-public-key")

    valid_calls: list[tuple[int, str]] = []

    def _fake_valid_key(tenant_id: int, key: str) -> bool:
        valid_calls.append((tenant_id, key))
        return tenant_id == 55 and key == "tenant-55-key"

    monkeypatch.setattr(public_module.common, "valid_key", _fake_valid_key)

    webhook_calls: list[tuple[str, dict]] = []

    monkeypatch.setattr(public_module.common, "webhook_url", lambda: "https://example.test/webhook")

    async def _fake_wa_post(path: str, payload: dict) -> object:
        webhook_calls.append((path, payload))

        class _Resp:
            status_code = 200

        return _Resp()

    monkeypatch.setattr(public_module.common, "wa_post", _fake_wa_post)

    async def _fake_status_impl(tenant_id: int) -> dict:
        assert tenant_id == 55
        return {"ok": True, "ready": True, "connected": True, "qr": False, "last": 123}

    monkeypatch.setattr(public_module, "_wa_status_impl", _fake_status_impl)

    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp = client.get("/pub/wa/status", params={"tenant": 55, "k": "tenant-55-key"})

    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "ready": True, "connected": True, "qr": False, "last": 123}
    assert valid_calls == [(55, "tenant-55-key")]
    assert webhook_calls and webhook_calls[0][1]["tenant_id"] == 55


def test_wa_qr_svg_respects_explicit_qr_id(monkeypatch):
    _configure_retry(monkeypatch, attempts=1, delay=0.0)
    monkeypatch.setattr(public_module, "WA_ENABLED", True, raising=False)
    monkeypatch.setattr(public_module, "_expected_public_key_value", lambda: "global-public-key")

    def _fake_valid_key(tenant_id: int, key: str) -> bool:
        return tenant_id == 77 and key == "tenant-77-key"

    monkeypatch.setattr(public_module.common, "valid_key", _fake_valid_key)

    store = {
        "wa:qr:last:77": "other",  # ensure explicit id is respected
        "wa:qr:77:special": json.dumps({"qr_svg": "<svg id=\"special\"></svg>"}),
    }
    monkeypatch.setattr(public_module.common, "redis_client", lambda: _DummyRedis(store))

    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp = client.get(
        "/pub/wa/qr.svg",
        params={"tenant": 77, "k": "tenant-77-key", "qr_id": "special"},
    )

    assert resp.status_code == 200
    assert resp.headers.get("x-wa-qr-id") == "special"
    assert "special" in resp.text


def test_wa_status_impl_adds_qr_id(monkeypatch):
    store = {"wa:qr:last:5": "qr123"}
    monkeypatch.setattr(public_module.common, "redis_client", lambda: _DummyRedis(store))

    payload = json.dumps({"ready": False, "qr": True, "last": "qr"})
    monkeypatch.setattr(public_module.common, "http", lambda method, url, body=None, timeout=8.0: (200, payload))

    result = asyncio.run(public_module._wa_status_impl(5))

    assert result["qr_id"] == "qr123"
    assert result["last"] == "qr"
