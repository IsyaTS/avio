from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.web import public as public_module


def _build_app(monkeypatch):
    app = FastAPI()
    app.include_router(public_module.router)

    # Always accept the key in tests
    monkeypatch.setattr(public_module.C, "valid_key", lambda tenant, k: True)

    return app


def test_wa_qr_svg_calls_tenant_upstream_and_proxies_svg(monkeypatch):
    called = {}

    def _fake_fetch(url: str, timeout: float = 6.0):
        called["url"] = url
        # Simulate waweb returning a valid SVG image
        return 200, "image/svg+xml", b"<svg></svg>"

    monkeypatch.setattr(public_module, "_fetch_qr_bytes", _fake_fetch)

    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp = client.get("/pub/wa/qr.svg", params={"tenant": 123, "k": "abc"})

    assert resp.status_code == 200
    assert resp.headers.get("content-type", "").startswith("image/svg+xml")
    assert resp.headers.get("cache-control") == "no-store"
    
    # Ensure tenant-scoped endpoint with query-format svg is used as primary
    assert "/session/123/qr?format=svg" in called["url"]


def test_wa_qr_svg_returns_204_on_404_or_empty_body(monkeypatch):
    # Case 1: 404 from upstream
    monkeypatch.setattr(public_module, "_fetch_qr_bytes", lambda url, timeout=6.0: (404, "text/plain", b""))
    app = _build_app(monkeypatch)
    client = TestClient(app)
    resp = client.get("/pub/wa/qr.svg", params={"tenant": 1, "k": "abc"})
    assert resp.status_code == 204
    assert resp.headers.get("cache-control") == "no-store"

    # Case 2: 200 but empty body
    monkeypatch.setattr(public_module, "_fetch_qr_bytes", lambda url, timeout=6.0: (200, "image/svg+xml", b""))
    resp = client.get("/pub/wa/qr.svg", params={"tenant": 1, "k": "abc"})
    assert resp.status_code == 204
    assert resp.headers.get("cache-control") == "no-store"


def test_wa_qr_routes_reject_missing_query_args(monkeypatch):
    app = _build_app(monkeypatch)
    client = TestClient(app)

    resp_svg = client.get("/pub/wa/qr.svg")
    assert resp_svg.status_code == 401
    assert resp_svg.json().get("error") == "invalid_key"

    resp_png = client.get("/pub/wa/qr.png", params={"tenant": 1})
    assert resp_png.status_code == 401
    assert resp_png.json().get("error") == "invalid_key"
