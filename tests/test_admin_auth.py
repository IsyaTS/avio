from __future__ import annotations

import asyncio
import importlib
import sys
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from types import SimpleNamespace

from starlette.requests import Request


def _build_request(path: str = "/", query: str = "", headers: dict | None = None, cookies: dict | None = None) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": query.encode(),
        "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
    }
    if cookies:
        cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items())
        scope["headers"].append((b"cookie", cookie_header.encode()))

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(scope, receive)


def _reload_for_admin_tests():
    for name in (
        "apps.api.web.admin",
        "apps.api.web.common",
        "apps.api.web.public",
        "apps.api.web.client",
        "apps.api.web.webhooks",
        "apps.api.main",
        "core",
        "libs.core.sales_core",
    ):
        sys.modules.pop(name, None)


def test_admin_cookie_constant(monkeypatch):
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    _reload_for_admin_tests()
    core_mod = importlib.import_module("libs.core.sales_core")

    assert hasattr(core_mod, "ADMIN_COOKIE")
    assert isinstance(core_mod.ADMIN_COOKIE, str)
    assert core_mod.ADMIN_COOKIE


def test_admin_login_sets_secure_cookie(monkeypatch):
    admin_token = "valid-admin-token"
    monkeypatch.setenv("ADMIN_TOKEN", admin_token)
    _reload_for_admin_tests()

    core_mod = importlib.import_module("libs.core.sales_core")
    admin_mod = importlib.import_module("apps.api.web.admin")

    request = _build_request(path="/admin/login", query=f"token={admin_token}")
    response = admin_mod.login(request, token=admin_token)
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

    admin_mod = importlib.import_module("apps.api.web.admin")

    async def _fake_get_by_tenant(tenant_id: int):
        assert tenant_id == 9
        return SimpleNamespace(token="provider-secret", created_at=datetime(2024, 5, 21, tzinfo=timezone.utc))

    monkeypatch.setattr(admin_mod.provider_tokens_repo, "get_by_tenant", _fake_get_by_tenant, raising=False)

    req = _build_request(path="/admin/provider-token/9", headers={"X-Admin-Token": admin_token})
    resp = asyncio.run(admin_mod.provider_token_get(tenant=9, request=req))

    assert resp["ok"] is True
    assert resp["tenant"] == 9
    assert resp["provider_token"] == "provider-secret"
    assert resp["created_at"].startswith("2024-05-21")
