from __future__ import annotations

import base64
import json
from types import SimpleNamespace

import pytest

from apps.api.web.services import wa_qr_runtime


pytestmark = pytest.mark.unit


class _RedisError(Exception):
    pass


class _Pipeline:
    def __init__(self, store: dict[str, str]):
        self.store = store
        self.ops: list[tuple[str, int, str]] = []

    def setex(self, key: str, ttl: int, value: str) -> None:
        self.ops.append((key, ttl, value))
        self.store[key] = value

    def execute(self) -> None:
        return None


class _Redis:
    def __init__(self, store: dict[str, str]):
        self.store = store

    def get(self, key: str):
        return self.store.get(key)

    def mget(self, *keys: str):
        return [self.store.get(key) for key in keys]

    def pipeline(self):
        return _Pipeline(self.store)


def _deps(store: dict[str, str] | None = None, **overrides) -> wa_qr_runtime.WaQrDeps:
    store = {} if store is None else store
    common = SimpleNamespace(
        redis_client=lambda: _Redis(store),
        wa_base_url=lambda tenant=None: "http://wa.test",
        whatsapp_provider=lambda tenant: "waweb",
        webhook_url=lambda: "https://hook.test/wa",
        http=lambda *a, **k: (200, b"{}"),
        wabaileys_http=lambda *a, **k: (200, "{}"),
    )
    values = {
        "common_module": common,
        "settings": SimpleNamespace(WA_PREFETCH_START=False, WA_QR_FETCH_ATTEMPTS=1),
        "client_config_module": SimpleNamespace(WA_WEB_TOKEN="", WA_INTERNAL_TOKEN=""),
        "redis_error_type": _RedisError,
        "logger": SimpleNamespace(
            info=lambda *a, **k: None,
            warning=lambda *a, **k: None,
            exception=lambda *a, **k: None,
        ),
        "no_store_headers_fn": lambda extra=None: {"Cache-Control": "no-store", **(extra or {})},
        "qr_cache_ttl_fn": lambda: 60,
    }
    values.update(overrides)
    return wa_qr_runtime.WaQrDeps(**values)


def test_cache_round_trip_reads_json_and_sidecar_svg() -> None:
    store: dict[str, str] = {}
    deps = _deps(store)

    wa_qr_runtime.cache_qr_payload(
        7,
        "qr-1",
        {"qr_svg": "<svg />", "qr_text": "raw"},
        deps,
    )

    assert store["wa:qr:last:7"] == "qr-1"
    entry, failed = wa_qr_runtime.load_cached_qr_entry(7, "qr-1", deps)
    assert failed is False
    assert entry is not None
    assert entry["qr_svg"] == "<svg />"
    assert entry["qr_text"] == "raw"


def test_load_cached_svg_renders_from_text_and_updates_cache() -> None:
    store = {
        "wa:qr:last:7": "qr-1",
        "wa:qr:7:qr-1": json.dumps({"qr_text": "https://example.test/login"}),
    }
    deps = _deps(store)

    svg, failed = wa_qr_runtime.load_cached_svg(7, "qr-1", deps)

    assert failed is False
    assert svg is not None
    assert "<svg" in svg.lower()
    assert "wa:qr:7:qr-1:svg" in store


def test_wa_qr_png_response_serves_cached_base64_png() -> None:
    png = base64.b64encode(b"png-bytes").decode("ascii")
    store = {"wa:qr:7:qr-1": json.dumps({"qr_png": png})}

    response = wa_qr_runtime.wa_qr_png_response(
        request=SimpleNamespace(),
        tenant=7,
        key="key",
        qr_id="qr-1",
        ensure_valid_qr_request_fn=lambda *a, **k: (7, "key"),
        invalid_key_response_fn=lambda: None,
        deps=_deps(store),
    )

    assert response.status_code == 200
    assert response.body == b"png-bytes"
    assert response.headers["x-wa-qr-id"] == "qr-1"


def test_proxy_baileys_qr_returns_svg_with_qr_id() -> None:
    common = SimpleNamespace(
        redis_client=lambda: _Redis({}),
        wa_base_url=lambda tenant=None: "http://wa.test",
        whatsapp_provider=lambda tenant: "baileys",
        webhook_url=lambda: "https://hook.test/wa",
        http=lambda *a, **k: (200, b"{}"),
        wabaileys_http=lambda *a, **k: (
            200,
            json.dumps({"session": {"qr": {"id": "qr-bx", "svg": "<svg id='qr-bx'/>"}}}),
        ),
    )

    response = wa_qr_runtime.proxy_baileys_qr(7, _deps(common_module=common))

    assert response.status_code == 200
    assert response.body == b"<svg id='qr-bx'/>"
    assert response.headers["x-wa-qr-id"] == "qr-bx"
