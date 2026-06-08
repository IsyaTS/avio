import copy

import pytest
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient

from apps.api.web import client as client_module
from apps.api.web import common as common_module


pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _disable_session_auth(monkeypatch):
    async def _no_user(request):
        return None

    monkeypatch.setattr(client_module.auth_utils, "get_current_user", _no_user)


def _build_client(monkeypatch, cfg, persona=""):
    app = FastAPI()
    app.mount("/static", StaticFiles(directory="apps/api/static"), name="static")
    app.include_router(client_module.router)

    async def _no_user(request):
        return None

    monkeypatch.setattr(client_module.auth_utils, "get_current_user", _no_user)
    monkeypatch.setattr(client_module, "_resolve_key", lambda request, raw=None: "abc")
    monkeypatch.setattr(client_module, "_auth", lambda tenant, key: True)
    monkeypatch.setattr(client_module.C, "read_tenant_config", lambda tenant: cfg)
    monkeypatch.setattr(client_module.C, "read_persona", lambda tenant, channel=None: persona)

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
    app.mount("/static", StaticFiles(directory="apps/api/static"), name="static")
    app.include_router(client_module.router)

    async def _no_user(request):
        return None

    monkeypatch.setattr(client_module.auth_utils, "get_current_user", _no_user)
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


def test_save_persona_allows_same_tenant_session_without_key(monkeypatch):
    app = FastAPI()
    app.mount("/static", StaticFiles(directory="apps/api/static"), name="static")
    app.include_router(client_module.router)

    written = {}

    async def _fake_user(request):
        return {"tenant_id": 101, "id": 1, "is_verified": True}

    monkeypatch.setattr(client_module.auth_utils, "get_current_user", _fake_user)
    monkeypatch.setattr(client_module, "_resolve_key", lambda request, raw=None: "")
    monkeypatch.setattr(client_module, "_auth", lambda tenant, key: False)
    monkeypatch.setattr(client_module.C, "get_tenant_pubkey", lambda tenant: "")
    monkeypatch.setattr(client_module.C, "list_keys", lambda tenant: [])
    monkeypatch.setattr(
        client_module.C,
        "write_persona",
        lambda tenant, text, channel=None: written.update(
            {"tenant": tenant, "text": text, "channel": channel}
        ),
    )
    monkeypatch.setattr(client_module.C, "read_tenant_config", lambda tenant: {})

    test_client = TestClient(app)
    response = test_client.post("/client/101/persona", json={"text": "persona body"})

    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True}
    assert written == {"tenant": 101, "text": "persona body", "channel": None}


def test_save_json_merges_without_dropping_integrations(monkeypatch):
    existing = {
        "behavior": {"avito_smart_reply_enabled": True},
        "integrations": {
            "avito": {
                "access_token": "access-old",
                "refresh_token": "refresh-old",
                "account_id": 777,
            },
            "max_personal": {"enabled": True},
        },
        "follow_up": [{"text": "later", "delay_minutes": 15}],
    }
    written = {}

    class DummyRequest:
        async def body(self):
            return b'{"behavior":{"brain_mode":"smart"},"integrations":{"avito":{}}}'

    async def _auth_request(request, tenant):
        return tenant, "abc"

    monkeypatch.setattr(client_module, "_authorize_client_settings_request", _auth_request)
    monkeypatch.setattr(client_module.C, "read_tenant_config", lambda tenant: copy.deepcopy(existing))

    def _capture_write(tenant, data):
        written["tenant"] = tenant
        written["cfg"] = copy.deepcopy(data)

    monkeypatch.setattr(client_module.C, "write_tenant_config", _capture_write)

    import asyncio

    response = asyncio.run(client_module.save_json(1, DummyRequest()))

    assert response == {"ok": True}
    saved = written["cfg"]
    assert saved["behavior"]["brain_mode"] == "smart"
    assert saved["behavior"]["avito_smart_reply_enabled"] is True
    assert saved["integrations"]["avito"]["access_token"] == "access-old"
    assert saved["integrations"]["avito"]["refresh_token"] == "refresh-old"
    assert saved["integrations"]["avito"]["account_id"] == 777
    assert saved["integrations"]["max_personal"] == {"enabled": True}
    assert saved["follow_up"] == existing["follow_up"]


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
    monkeypatch.setattr(common_module, "asset_version", lambda: "v-test")
    monkeypatch.setattr(common_module, "_ASSET_VERSION", None)
    from apps.api.web import ui as ui_module

    monkeypatch.setattr(ui_module, "asset_version", lambda: "v-test")
    ui_module.templates.env.globals["client_settings_version"] = "v-test"

    cfg = {"passport": {"brand": "Brand"}}
    client = _build_client(monkeypatch, cfg, persona="persona")

    response = client.get("/client/1/settings?k=abc")

    assert response.status_code == 200

    html = response.text
    boot_tag = "/static/js/boot.js?v=v-test"
    catalog_tag = "/static/js/catalog-upload.js?v=v-test"
    settings_tag = "/static/js/client-settings.js?v=v-test"

    assert boot_tag in html
    assert catalog_tag in html
    assert settings_tag in html
    assert html.index(boot_tag) < html.index(catalog_tag) < html.index(settings_tag)


def test_save_behavior_persists_max_catalog_first_flag(monkeypatch):
    cfg = {"behavior": {}}
    written = {}

    app = FastAPI()
    app.mount("/static", StaticFiles(directory="apps/api/static"), name="static")
    app.include_router(client_module.router)

    monkeypatch.setattr(client_module, "_resolve_key", lambda request, raw=None: "abc")
    monkeypatch.setattr(client_module, "_auth", lambda tenant, key: True)
    monkeypatch.setattr(client_module.C, "read_tenant_config", lambda tenant: cfg)

    def _capture_write(tenant, data):
        written["tenant"] = tenant
        written["cfg"] = copy.deepcopy(data)

    monkeypatch.setattr(client_module.C, "write_tenant_config", _capture_write)

    test_client = TestClient(app)
    payload = {
        "auto_reply": False,
        "auto_reply_text": "",
        "send_catalog_on_first_message": True,
        "send_catalog_on_first_message_max": False,
    }

    response = test_client.post("/client/1/behavior/save?k=abc", json=payload)

    assert response.status_code == 200, response.text
    assert written["tenant"] == 1
    behavior = written["cfg"].get("behavior") or {}
    assert behavior.get("send_catalog_on_first_message") is True
    assert behavior.get("send_catalog_on_first_message_max") is False


def test_save_behavior_partial_payload_preserves_avito_smart_reply(monkeypatch):
    cfg = {
        "behavior": {
            "avito_smart_reply_enabled": True,
            "telegram_reply_enabled": True,
            "max_reply_enabled": True,
            "brain_mode": "smart",
            "triggers": [{"type": "keyword", "value": "door"}],
        },
        "integrations": {"avito": {"access_token": "access-old"}},
    }
    written = {}

    app = FastAPI()
    app.mount("/static", StaticFiles(directory="apps/api/static"), name="static")
    app.include_router(client_module.router)

    monkeypatch.setattr(client_module, "_resolve_key", lambda request, raw=None: "abc")
    monkeypatch.setattr(client_module, "_auth", lambda tenant, key: True)
    monkeypatch.setattr(client_module.C, "read_tenant_config", lambda tenant: copy.deepcopy(cfg))

    def _capture_write(tenant, data):
        written["tenant"] = tenant
        written["cfg"] = copy.deepcopy(data)

    monkeypatch.setattr(client_module.C, "write_tenant_config", _capture_write)

    test_client = TestClient(app)
    response = test_client.post("/client/1/behavior/save?k=abc", json={"auto_reply_text": "ok"})

    assert response.status_code == 200, response.text
    behavior = written["cfg"]["behavior"]
    assert behavior["avito_smart_reply_enabled"] is True
    assert behavior["telegram_reply_enabled"] is True
    assert behavior["max_reply_enabled"] is True
    assert behavior["brain_mode"] == "smart"
    assert behavior["triggers"] == [{"type": "keyword", "value": "door"}]
    assert written["cfg"]["integrations"]["avito"]["access_token"] == "access-old"
