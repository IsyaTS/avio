import copy

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient

from app.web import client as client_module
from app.web import common as common_module


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
    payload = {"brand": "Brand", "agent": "Agent", "city": "City"}

    response = test_client.post("/client/1/settings/save?k=abc", json=payload)

    assert response.status_code == 200
    assert response.json() == {"ok": True}

    saved_cfg = written["cfg"]
    assert saved_cfg["passport"]["brand"] == "Brand"
    assert saved_cfg["passport"]["agent_name"] == "Agent"
    assert saved_cfg["passport"]["currency"] == "₽"
    assert saved_cfg.get("behavior") is None
    assert saved_cfg.get("cta") == "oops"


def test_list_keys_settings_link_includes_query(monkeypatch):
    monkeypatch.setattr(common_module, "get_tenant_pubkey", lambda tenant: "secret-key")
    monkeypatch.setattr(common_module, "_normalize_key", lambda value: (value or "").strip().lower())
    monkeypatch.setattr(common_module, "_load_key_meta", lambda tenant: {"key": "secret-key", "normalized": "secret-key"})
    monkeypatch.setattr(common_module, "_migrate_legacy_keys", lambda tenant, meta: meta)

    captured_meta: dict[str, object] = {}

    def _capture_save(tenant: int, meta: dict[str, object]):
        captured_meta["tenant"] = tenant
        captured_meta["meta"] = dict(meta)

    monkeypatch.setattr(common_module, "_save_key_meta", _capture_save)

    items = common_module.list_keys(9)

    assert items
    assert items[0]["settings_link"].endswith("/client/9/settings?k=secret-key")
    assert captured_meta.get("tenant") == 9


def test_client_settings_template_includes_scripts_in_order(monkeypatch):
    client_module._CLIENT_SETTINGS_VERSION = None
    monkeypatch.setattr(client_module, "_client_settings_static_version", lambda: "v-test")

    cfg = {"passport": {"brand": "Brand"}}
    client = _build_client(monkeypatch, cfg, persona="persona")

    response = client.get("/client/1/settings?k=abc")

    assert response.status_code == 200

    html = response.text
    boot_tag = "/static/js/boot.js?v=v-test"
    settings_tag = "/static/js/client-settings.js?v=v-test"

    assert boot_tag in html
    assert settings_tag in html
    assert html.index(boot_tag) < html.index(settings_tag)
