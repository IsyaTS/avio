import copy
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient

from app.web import client as client_module


def _build_client(monkeypatch, cfg, persona=""):
    app = FastAPI()
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    app.include_router(client_module.router)

    monkeypatch.setattr(client_module, "_resolve_key", lambda request, raw=None: "abc")
    monkeypatch.setattr(client_module, "_auth", lambda tenant, key: True)
    monkeypatch.setattr(client_module.C, "read_tenant_config", lambda tenant: cfg)
    monkeypatch.setattr(client_module.C, "read_persona", lambda tenant: persona)

    return TestClient(app)


def test_client_settings_handles_non_mapping_sections(monkeypatch):
    cfg = {
        "passport": None,
        "behavior": [],
        "cta": "oops",
        "integrations": [],
    }

    client = _build_client(monkeypatch, cfg, persona="persona")
    response = client.get("/client/1/settings?k=abc")

    assert response.status_code == 200
    assert "persona" in response.text


def test_save_form_normalizes_and_writes(monkeypatch):
    cfg = {
        "passport": [],
        "behavior": None,
        "cta": "oops",
    }
    written = {}

    app = FastAPI()
    app.mount("/static", StaticFiles(directory="app/static"), name="static")
    app.include_router(client_module.router)

    monkeypatch.setattr(client_module, "_resolve_key", lambda request, raw=None: "abc")
    monkeypatch.setattr(client_module, "_auth", lambda tenant, key: True)
    monkeypatch.setattr(client_module.C, "read_tenant_config", lambda tenant: cfg)

    def _capture_write(tenant, data):
        written["cfg"] = copy.deepcopy(data)

    monkeypatch.setattr(client_module.C, "write_tenant_config", _capture_write)

    test_client = TestClient(app)
    payload = {
        "brand": "Brand",
        "agent": "Agent",
        "city": "City",
        "currency": "USD",
        "tone": "friendly",
        "cta_primary": "Go",
        "cta_fallback": "Fallback",
    }

    response = test_client.post("/client/1/settings/save?k=abc", json=payload)

    assert response.status_code == 200
    assert response.json() == {"ok": True}

    saved_cfg = written["cfg"]
    assert saved_cfg["passport"]["brand"] == "Brand"
    assert saved_cfg["passport"]["agent_name"] == "Agent"
    assert saved_cfg["passport"]["currency"] == "USD"
    assert saved_cfg["behavior"]["tone"] == "friendly"
    assert saved_cfg["cta"]["primary"] == "Go"
    assert saved_cfg["cta"]["fallback"] == "Fallback"
