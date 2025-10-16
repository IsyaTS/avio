from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.web import public as public_module


def _build_app(monkeypatch):
    app = FastAPI()
    app.include_router(public_module.router)

    return app


def test_wa_qr_svg_calls_tenant_upstream_and_proxies_svg(monkeypatch):
    called = {}

    def _fake_fetch(url: str, timeout: float = 6.0):
        called["url"] = url
        # Simulate waweb returning a valid SVG image
        return 200, "image/svg+xml", b"<svg></svg>"

    monkeypatch.setattr(public_module, "_fetch_qr_bytes", _fake_fetch)
    monkeypatch.setattr(public_module, "_expected_public_key_value", lambda: "global-public-key")

    valid_calls: list[tuple[int, str]] = []

    def _fake_valid_key(tenant_id: int, key: str) -> bool:
        valid_calls.append((tenant_id, key))
        return tenant_id == 123 and key == "tenant-access-key"

    monkeypatch.setattr(public_module.common, "valid_key", _fake_valid_key)

    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp = client.get("/pub/wa/qr.svg", params={"tenant": 123, "k": "tenant-access-key"})

    assert resp.status_code == 200
    assert resp.headers.get("content-type", "").startswith("image/svg+xml")
    assert resp.headers.get("cache-control") == "no-store"

    # Ensure tenant-scoped endpoint with query-format svg is used as primary
    assert "/session/123/qr?format=svg" in called["url"]
    assert valid_calls == [(123, "tenant-access-key")]


def test_wa_qr_svg_returns_204_on_404_or_empty_body(monkeypatch):
    # Case 1: 404 from upstream
    monkeypatch.setattr(public_module, "_fetch_qr_bytes", lambda url, timeout=6.0: (404, "text/plain", b""))
    monkeypatch.setattr(public_module, "_expected_public_key_value", lambda: "global-public-key")

    valid_calls: list[tuple[int, str]] = []

    def _fake_valid_key(tenant_id: int, key: str) -> bool:
        valid_calls.append((tenant_id, key))
        return tenant_id == 1 and key == "tenant-key"

    monkeypatch.setattr(public_module.common, "valid_key", _fake_valid_key)
    app = _build_app(monkeypatch)
    client = TestClient(app)
    resp = client.get("/pub/wa/qr.svg", params={"tenant": 1, "k": "tenant-key"})
    assert resp.status_code == 204
    assert resp.headers.get("cache-control") == "no-store"

    # Case 2: 200 but empty body
    monkeypatch.setattr(public_module, "_fetch_qr_bytes", lambda url, timeout=6.0: (200, "image/svg+xml", b""))
    resp = client.get("/pub/wa/qr.svg", params={"tenant": 1, "k": "tenant-key"})
    assert resp.status_code == 204
    assert resp.headers.get("cache-control") == "no-store"
    assert valid_calls == [(1, "tenant-key"), (1, "tenant-key")]


def test_wa_qr_routes_reject_missing_query_args(monkeypatch):
    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp_svg = client.get("/pub/wa/qr.svg")
    assert resp_svg.status_code == 422

    resp_png = client.get("/pub/wa/qr.png", params={"tenant": 1})
    assert resp_png.status_code == 422


def test_wa_status_accepts_tenant_valid_key(monkeypatch):
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
