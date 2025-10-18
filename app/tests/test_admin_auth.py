from __future__ import annotations

import importlib
import sys
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from types import SimpleNamespace

from fastapi.testclient import TestClient


def _reload_for_admin_tests():
    for name in (
        "app.web.admin",
        "app.web.common",
        "app.web.public",
        "app.web.client",
        "app.web.webhooks",
        "app.main",
        "core",
        "app.core",
    ):
        sys.modules.pop(name, None)


def test_admin_cookie_constant(monkeypatch):
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    _reload_for_admin_tests()
    core_mod = importlib.import_module("core")

    assert hasattr(core_mod, "ADMIN_COOKIE")
    assert isinstance(core_mod.ADMIN_COOKIE, str)
    assert core_mod.ADMIN_COOKIE


def test_admin_login_sets_secure_cookie(monkeypatch):
    admin_token = "valid-admin-token"
    monkeypatch.setenv("ADMIN_TOKEN", admin_token)
    _reload_for_admin_tests()

    core_mod = importlib.import_module("core")
    main_mod = importlib.import_module("app.main")

    client = TestClient(main_mod.app)

    response = client.get("/admin/login", params={"token": admin_token}, follow_redirects=False)
    assert response.status_code == 303

    set_cookie_header = response.headers.get("set-cookie", "")
    assert set_cookie_header
    assert f"{core_mod.ADMIN_COOKIE}={admin_token}" in set_cookie_header

    cookies = SimpleCookie()
    cookies.load(set_cookie_header)
    assert core_mod.ADMIN_COOKIE in cookies
    cookie = cookies[core_mod.ADMIN_COOKIE]

    assert cookie.value == admin_token
    assert cookie["httponly"]
    assert cookie["secure"]
    assert cookie.get("samesite", "").lower() == "lax"

    max_age = cookie.get("max-age")
    assert max_age and int(max_age) >= 60 * 60 * 24 * 7


def test_admin_provider_token_lookup(monkeypatch):
    admin_token = "valid-admin-token"
    monkeypatch.setenv("ADMIN_TOKEN", admin_token)
    _reload_for_admin_tests()

    main_mod = importlib.import_module("app.main")
    admin_mod = importlib.import_module("app.web.admin")

    async def _fake_get_by_tenant(tenant_id: int):
        assert tenant_id == 9
        return SimpleNamespace(token="provider-secret", created_at=datetime(2024, 5, 21, tzinfo=timezone.utc))

    monkeypatch.setattr(admin_mod.provider_tokens_repo, "get_by_tenant", _fake_get_by_tenant, raising=False)

    client = TestClient(main_mod.app)

    resp = client.get(
        "/admin/provider-token/9",
        headers={"X-Admin-Token": admin_token},
    )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["tenant"] == 9
    assert payload["provider_token"] == "provider-secret"
    assert payload["created_at"].startswith("2024-05-21")
