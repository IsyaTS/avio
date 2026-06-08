from __future__ import annotations

import hmac
from types import SimpleNamespace

import httpx
import pytest
from fastapi.responses import Response

from apps.api.web.services import amocrm_avatar_runtime


pytestmark = pytest.mark.unit


def _deps(**overrides) -> amocrm_avatar_runtime.AmoCRMAvatarDeps:
    async def _tg_call(_method, _path, **_kwargs):
        return 200, httpx.Response(
            200,
            content=b"png",
            headers={"content-type": "image/png"},
            request=httpx.Request("GET", "http://tg.test/avatar"),
        )

    async def _chat_avatar(_request, lead_id, **kwargs):
        return Response(f"lead={lead_id};tenant={kwargs['tenant']};key={kwargs['k']}")

    values = {
        "read_tenant_config_fn": lambda _tenant_id: {},
        "amocrm_chat_service_module": SimpleNamespace(
            build_avatar_path_token=lambda _cfg, _tenant_id, _peer_id: "token",
            build_lead_avatar_path_token=lambda _cfg, _tenant_id, _lead_id: "lead-token",
        ),
        "hmac_module": hmac,
        "tg_call_fn": _tg_call,
        "tg_worker_call_error_type": RuntimeError,
        "no_store_headers_fn": lambda extra=None: {"Cache-Control": "no-store", **(extra or {})},
        "chat_avatar_fn": _chat_avatar,
        "get_tenant_pubkey_fn": lambda _tenant_id: "tenant-key",
    }
    values.update(overrides)
    return amocrm_avatar_runtime.AmoCRMAvatarDeps(**values)


@pytest.mark.asyncio
async def test_chat_avatar_proxy_validates_token_and_proxies_tg_avatar() -> None:
    response = await amocrm_avatar_runtime.chat_avatar_proxy(
        SimpleNamespace(),
        7,
        "123",
        "token",
        deps=_deps(),
    )

    assert response.status_code == 200
    assert response.body == b"png"
    assert response.headers["Content-Type"] == "image/png"
    assert response.headers["X-Telegram-Upstream-Status"] == "200"


@pytest.mark.asyncio
async def test_chat_avatar_proxy_rejects_invalid_token() -> None:
    response = await amocrm_avatar_runtime.chat_avatar_proxy(
        SimpleNamespace(),
        7,
        "123",
        "bad",
        deps=_deps(),
    )

    assert response.status_code == 403
    assert response.body.decode("utf-8") == '{"ok":false,"detail":"invalid_token"}'


@pytest.mark.asyncio
async def test_lead_avatar_proxy_reuses_public_chat_avatar() -> None:
    response = await amocrm_avatar_runtime.lead_avatar_proxy(
        SimpleNamespace(),
        7,
        456,
        "lead-token",
        deps=_deps(),
    )

    assert response.status_code == 200
    assert response.body == b"lead=456;tenant=7;key=tenant-key"
