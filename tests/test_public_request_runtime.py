from __future__ import annotations

import json

import pytest

from apps.api.web.services import public_request_runtime


pytestmark = pytest.mark.unit


class _Form:
    def __init__(self, items):
        self._items = items

    def multi_items(self):
        return list(self._items)


class _Request:
    def __init__(
        self,
        *,
        method: str = "GET",
        query_params=None,
        cookies=None,
        body: bytes = b"",
        form=None,
    ):
        self.method = method
        self.query_params = query_params or {}
        self.cookies = cookies or {}
        self._body = body
        self._form = form

    async def body(self):
        return self._body

    async def form(self):
        if self._form is None:
            raise RuntimeError("no form")
        return self._form


@pytest.mark.asyncio
async def test_resolve_tenant_and_key_prefers_explicit_values() -> None:
    tenant, key = await public_request_runtime.resolve_tenant_and_key(
        _Request(query_params={"tenant": "2", "k": "query"}),
        7,
        "raw",
        json_module=json,
    )

    assert tenant == 7
    assert key == "raw"


@pytest.mark.asyncio
async def test_resolve_tenant_and_key_reads_query_then_cookie() -> None:
    tenant, key = await public_request_runtime.resolve_tenant_and_key(
        _Request(query_params={"tenant": "2"}, cookies={"client_key": "cookie-key"}),
        None,
        None,
        json_module=json,
    )

    assert tenant == "2"
    assert key == "cookie-key"


@pytest.mark.asyncio
async def test_resolve_tenant_and_key_reads_json_body_for_post() -> None:
    tenant, key = await public_request_runtime.resolve_tenant_and_key(
        _Request(method="POST", body=b'{"tenant": "5", "k": "body-key"}'),
        None,
        None,
        query_keys=("k",),
        json_module=json,
    )

    assert tenant == "5"
    assert key == "body-key"


@pytest.mark.asyncio
async def test_resolve_tenant_and_key_falls_back_to_form_body() -> None:
    tenant, key = await public_request_runtime.resolve_tenant_and_key(
        _Request(method="POST", body=b"", form=_Form([("tenant", "9"), ("k", "form-key")])),
        None,
        None,
        query_keys=("k",),
        json_module=json,
    )

    assert tenant == "9"
    assert key == "form-key"


@pytest.mark.asyncio
async def test_resolve_tenant_and_key_ignores_body_when_disabled() -> None:
    tenant, key = await public_request_runtime.resolve_tenant_and_key(
        _Request(method="POST", body=b'{"tenant": "5", "k": "body-key"}'),
        None,
        None,
        allow_body=False,
        json_module=json,
    )

    assert tenant is None
    assert key is None
