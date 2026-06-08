from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from apps.api.web.services import tg_proxy_runtime


pytestmark = pytest.mark.unit


class _Logger:
    def __init__(self):
        self.records = []

    def info(self, message, *args):
        self.records.append(("info", message, args))

    def warning(self, message, *args):
        self.records.append(("warning", message, args))


class _Client:
    is_closed = False

    def __init__(self):
        self.calls = []

    async def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return httpx.Response(200, json={"ok": True}, request=httpx.Request(method, url))


def test_base_url_prefers_env_and_make_url_handles_relative_paths(monkeypatch) -> None:
    monkeypatch.setenv("TG_WORKER_URL", " http://tgworker:8000/ ")
    settings = SimpleNamespace(DEFAULT_WORKER_BASE_URL="http://default")

    base = tg_proxy_runtime.base_url(__import__("os"), settings)

    assert base == "http://tgworker:8000"
    assert tg_proxy_runtime.make_url("status", base=base) == "http://tgworker:8000/status"
    assert tg_proxy_runtime.make_url("https://example.test/x", base=base) == "https://example.test/x"


def test_mask_sensitive_detail_masks_json_and_query_passwords() -> None:
    detail = '{"password":"secret"} password=other'

    masked = tg_proxy_runtime.mask_sensitive_detail(detail)

    assert "secret" not in masked
    assert "other" not in masked
    assert "******" in masked


def test_extract_json_detail_from_bytes() -> None:
    detail = tg_proxy_runtime.extract_json_detail(b'{"detail":"bad_password"}', json_module=__import__("json"))

    assert detail == "bad_password"


def test_passthrough_upstream_response_copies_safe_headers_and_logs() -> None:
    logged = []
    upstream = httpx.Response(
        429,
        content=b'{"detail":"slow"}',
        headers={"content-type": "application/json", "retry-after": "3", "x-secret": "no"},
        request=httpx.Request("POST", "http://tg.test/restart"),
    )

    response = tg_proxy_runtime.passthrough_upstream_response(
        "/pub/tg/restart",
        7,
        upstream,
        no_store_headers_fn=lambda extra=None: {"Cache-Control": "no-store", **(extra or {})},
        log_tg_proxy_fn=lambda *args, **kwargs: logged.append((args, kwargs)),
    )

    assert response.status_code == 429
    assert response.headers["Content-Type"] == "application/json"
    assert response.headers["Retry-After"] == "3"
    assert "x-secret" not in response.headers
    assert logged[0][1]["error"] == '{"detail":"slow"}'


def test_proxy_headers_allows_only_safe_headers() -> None:
    headers = tg_proxy_runtime.proxy_headers(
        {"content-type": "image/png", "authorization": "secret"},
        200,
        no_store_value="no-store",
    )

    assert headers["content-type"] == "image/png"
    assert "authorization" not in headers
    assert headers["X-Telegram-Upstream-Status"] == "200"


@pytest.mark.asyncio
async def test_call_adds_admin_headers_and_logs_response() -> None:
    client = _Client()
    logger = _Logger()

    status_code, response = await tg_proxy_runtime.call(
        "GET",
        "/status",
        params={"tenant": 7},
        json_payload=None,
        timeout=3,
        route="/pub/tg/status",
        peer=None,
        deps=tg_proxy_runtime.TgProxyCallDeps(
            make_url_fn=lambda path: f"http://tg.test{path}",
            admin_headers_fn=lambda: {"X-Admin-Token": "admin"},
            client_fn=lambda: client,
            httpx_module=httpx,
            logger=logger,
            worker_call_error_type=RuntimeError,
        ),
    )

    assert status_code == 200
    assert response.json() == {"ok": True}
    assert client.calls[0][2]["headers"] == {"X-Admin-Token": "admin"}
    assert logger.records[0][0] == "info"
