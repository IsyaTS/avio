from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from apps.api.web.services import tg_public_runtime


pytestmark = pytest.mark.unit


class _Req:
    query_params = {}

    async def json(self):
        return {"password": "secret"}

    async def form(self):
        return {}


def _response(status_code: int, payload: dict) -> httpx.Response:
    return httpx.Response(
        status_code,
        json=payload,
        request=httpx.Request("POST", "http://tg.test/2fa"),
    )


def _binary_response(status_code: int, body: bytes, content_type: str) -> httpx.Response:
    return httpx.Response(
        status_code,
        content=body,
        headers={"content-type": content_type},
        request=httpx.Request("GET", "http://tg.test/resource"),
    )


def _deps(**overrides):
    async def _resolve(_request, tenant, key, **_kwargs):
        return tenant, key

    async def _authorize(_request, tenant, key):
        return int(tenant or 1), key or "key"

    async def _tg_call(_method, _path, **_kwargs):
        return 200, _response(200, {"ok": True, "state": "ready", "authorized": True})

    deps = dict(
        log_deprecated_fn=lambda *_a, **_k: None,
        resolve_tenant_and_key_fn=_resolve,
        authorize_public_settings_request_fn=_authorize,
        tg_slot_tenant_fn=lambda tenant_id, slot: int(tenant_id) * 10 + int(slot),
        log_public_tg_request_fn=lambda *_a, **_k: None,
        client_identifier_fn=lambda _request: "client-1",
        log_tg_proxy_fn=lambda *_a, **_k: None,
        no_store_headers_fn=lambda extra=None: {"Cache-Control": "no-store", **(extra or {})},
        register_password_attempt_fn=lambda _tenant, _client: (True, None),
        tg_call_fn=_tg_call,
        tg_worker_call_error_type=RuntimeError,
        resolve_qr_identifier_fn=lambda qr_id, fallback: qr_id or fallback,
        quote_fn=lambda value, safe="": str(value).replace(" ", "%20"),
        common_module=SimpleNamespace(tg_http=lambda *_a, **_k: (200, b"qr-text", {})),
        resolve_tg_base_fn=lambda: "http://tg.test",
        extract_json_detail_fn=lambda _body: None,
        stringify_detail_fn=lambda body: body.decode("utf-8", errors="ignore"),
        proxy_headers_fn=lambda headers, status: {
            "X-Telegram-Upstream-Status": str(status),
            **dict(headers or {}),
        },
        json_module=__import__("json"),
    )
    deps.update(overrides)
    return tg_public_runtime.TgPublicDeps(**deps)


def test_connect_tg_builds_template_context() -> None:
    rendered = {}
    common = SimpleNamespace(
        valid_key=lambda tenant, key: tenant == 7 and key == "tenant-key",
        ensure_tenant_files=lambda tenant: None,
        read_tenant_config=lambda tenant: {"passport": {"brand": "Brand"}},
        read_persona=lambda tenant: "Persona\nLine2\nLine3",
        get_tenant_pubkey=lambda tenant: "primary-key",
    )

    def _render(template, context):
        rendered.update({"template": template, "context": context})
        return httpx.Response(200)

    response = tg_public_runtime.connect_tg(
        7,
        SimpleNamespace(query_params={"k": "tenant-key"}),
        k=None,
        key=None,
        deps=tg_public_runtime.TgConnectDeps(
            common_module=common,
            settings_module=SimpleNamespace(PUBLIC_KEY="public-key"),
            render_template_fn=_render,
            quote_plus_fn=lambda value: str(value).replace(" ", "+"),
        ),
    )

    assert response.status_code == 200
    assert rendered["template"] == "connect/tg.html"
    assert rendered["context"]["key"] == "public-key"
    assert rendered["context"]["tenant_key"] == "tenant-key"
    assert rendered["context"]["subtitle"] == "Brand"
    assert rendered["context"]["persona_preview"] == "Persona\nLine2\nLine3"
    assert rendered["context"]["tg_connect_config"]["urls"]["tg_qr_png"] == "/pub/tg/qr.png?k=public-key"


@pytest.mark.anyio
async def test_handle_twofa_success() -> None:
    response = await tg_public_runtime.handle_twofa(
        "/pub/tg/2fa",
        _Req(),
        7,
        "key",
        slot=2,
        deps=_deps(),
    )

    assert response.status_code == 200
    assert '"authorized":true' in response.body.decode("utf-8")
    assert response.headers["x-telegram-upstream-status"] == "200"


@pytest.mark.anyio
async def test_handle_twofa_rate_limited() -> None:
    response = await tg_public_runtime.handle_twofa(
        "/pub/tg/2fa",
        _Req(),
        7,
        "key",
        slot=1,
        deps=_deps(register_password_attempt_fn=lambda _tenant, _client: (False, 30)),
    )

    assert response.status_code == 429
    assert response.headers["retry-after"] == "30"
    assert response.body.decode("utf-8") == '{"error":"flood_wait","retry_after":30}'


@pytest.mark.anyio
async def test_handle_twofa_bad_password() -> None:
    async def _tg_call(_method, _path, **_kwargs):
        return 401, _response(401, {"error": "bad_password", "detail": "wrong"})

    response = await tg_public_runtime.handle_twofa(
        "/pub/tg/2fa",
        _Req(),
        7,
        "key",
        slot=1,
        deps=_deps(tg_call_fn=_tg_call),
    )

    assert response.status_code == 401
    assert response.body.decode("utf-8") == '{"error":"bad_password","detail":"wrong"}'


@pytest.mark.anyio
async def test_handle_twofa_upstream_unavailable() -> None:
    class _Err(RuntimeError):
        detail = "ECONNREFUSED"

    async def _tg_call(_method, _path, **_kwargs):
        raise _Err("failed")

    response = await tg_public_runtime.handle_twofa(
        "/pub/tg/2fa",
        _Req(),
        7,
        "key",
        slot=1,
        deps=_deps(tg_call_fn=_tg_call, tg_worker_call_error_type=_Err),
    )

    assert response.status_code == 502
    assert "ECONNREFUSED" in response.body.decode("utf-8")


@pytest.mark.anyio
async def test_qr_txt_proxies_text() -> None:
    response = await tg_public_runtime.qr_txt(
        "/pub/tg/qr.txt",
        SimpleNamespace(query_params={}),
        7,
        "qr id",
        "key",
        slot=1,
        deps=_deps(),
    )

    assert response.status_code == 200
    assert response.body == b"qr-text"
    assert response.headers["x-telegram-upstream-status"] == "200"


@pytest.mark.anyio
async def test_proxy_tg_resource_proxies_binary_resource() -> None:
    calls: list[tuple[str, str]] = []

    async def _tg_call(method, path, **_kwargs):
        calls.append((method, path))
        return 200, _binary_response(200, b"image", "image/png")

    response = await tg_public_runtime.proxy_tg_resource(
        "/pub/tg/avatar",
        SimpleNamespace(query_params={}),
        7,
        "key",
        resource_path_fn=lambda tenant_id: f"/avatar/{tenant_id}/123",
        deps=_deps(tg_call_fn=_tg_call),
    )

    assert response.status_code == 200
    assert response.body == b"image"
    assert response.headers["content-type"] == "image/png"
    assert calls == [("GET", "/avatar/7/123")]


@pytest.mark.anyio
async def test_proxy_tg_resource_returns_bad_params_when_path_builder_fails() -> None:
    response = await tg_public_runtime.proxy_tg_resource(
        "/pub/tg/avatar",
        SimpleNamespace(query_params={}),
        7,
        "key",
        resource_path_fn=lambda _tenant_id: int("bad"),
        deps=_deps(),
    )

    assert response.status_code == 400
    assert response.body.decode("utf-8") == '{"error":"bad_params"}'
