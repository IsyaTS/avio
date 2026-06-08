from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.responses import Response

from apps.api.web.services import public_auth_runtime


pytestmark = pytest.mark.unit


class _Request:
    def __init__(self, *, query_params=None, cookies=None):
        self.query_params = query_params or {}
        self.cookies = cookies or {}


def _deps(**overrides) -> public_auth_runtime.PublicAuthDeps:
    async def _user(_request):
        return None

    values = {
        "get_current_user_fn": _user,
        "coerce_tenant_fn": lambda value: int(value),
        "resolve_public_settings_key_fn": public_auth_runtime.resolve_public_settings_key,
        "get_tenant_pubkey_fn": lambda tenant: "",
        "list_keys_fn": lambda tenant: [],
        "magic_link_enabled_fn": lambda: True,
        "valid_key_fn": lambda tenant, key: tenant == 7 and key == "valid",
        "settings": SimpleNamespace(ADMIN_TOKEN="admin"),
    }
    values.update(overrides)
    return public_auth_runtime.PublicAuthDeps(**values)


def _access_deps(**overrides) -> public_auth_runtime.PublicAccessDeps:
    values = {
        "coerce_tenant_fn": lambda value: int(value),
        "admin_token_valid_fn": lambda _request: False,
        "list_keys_fn": lambda _tenant: [],
        "get_tenant_pubkey_fn": lambda _tenant: "",
        "resolve_public_key_candidate_fn": public_auth_runtime.resolve_public_key_candidate,
        "expected_public_key_value_fn": lambda: "global-key",
        "valid_key_fn": lambda tenant, key: tenant == 7 and key == "tenant-key",
    }
    values.update(overrides)
    return public_auth_runtime.PublicAccessDeps(**values)


def test_resolve_public_settings_key_prefers_explicit_then_query_then_cookie() -> None:
    request = _Request(query_params={"k": "query"}, cookies={"client_key": "cookie"})

    assert public_auth_runtime.resolve_public_settings_key(request, "explicit") == "explicit"
    assert public_auth_runtime.resolve_public_settings_key(request, None) == "query"
    assert public_auth_runtime.resolve_public_settings_key(
        _Request(cookies={"client_key": "cookie"}),
        None,
    ) == "cookie"


def test_ensure_public_key_uses_expected_key_callback() -> None:
    request = _Request(query_params={"k": "expected"})

    result = public_auth_runtime.ensure_public_key(
        None,
        request,
        query_param_only=False,
        expected_key_fn=lambda: "expected",
    )

    assert result == "expected"


def test_ensure_valid_public_access_accepts_global_key_and_returns_tenant_key() -> None:
    result = public_auth_runtime.ensure_valid_public_access(
        7,
        "global-key",
        _Request(),
        query_param_only=False,
        deps=_access_deps(
            list_keys_fn=lambda _tenant: [{"key": "tenant-primary"}],
        ),
    )

    assert result == (7, "tenant-primary")


def test_ensure_valid_public_access_accepts_tenant_key() -> None:
    result = public_auth_runtime.ensure_valid_public_access(
        7,
        "tenant-key",
        _Request(),
        query_param_only=False,
        deps=_access_deps(),
    )

    assert result == (7, "tenant-key")


def test_ensure_valid_public_access_rejects_bad_key() -> None:
    result = public_auth_runtime.ensure_valid_public_access(
        7,
        "bad",
        _Request(),
        query_param_only=False,
        deps=_access_deps(),
    )

    assert result is None


@pytest.mark.asyncio
async def test_authorize_public_settings_request_accepts_valid_magic_key() -> None:
    result = await public_auth_runtime.authorize_public_settings_request(
        _Request(query_params={"k": "valid"}),
        7,
        None,
        _deps(),
    )

    assert result == (7, "valid")


@pytest.mark.asyncio
async def test_authorize_public_settings_request_rejects_invalid_key() -> None:
    result = await public_auth_runtime.authorize_public_settings_request(
        _Request(query_params={"k": "bad"}),
        7,
        None,
        _deps(),
    )

    assert isinstance(result, Response)
    assert result.status_code == 401


@pytest.mark.asyncio
async def test_authorize_public_settings_request_uses_session_tenant_key_fallback() -> None:
    async def _user(_request):
        return {"tenant_id": 9}

    result = await public_auth_runtime.authorize_public_settings_request(
        _Request(),
        None,
        None,
        _deps(
            get_current_user_fn=_user,
            get_tenant_pubkey_fn=lambda tenant: "tenant-key",
            valid_key_fn=lambda tenant, key: False,
        ),
    )

    assert result == (9, "tenant-key")
