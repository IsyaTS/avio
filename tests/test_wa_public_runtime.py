from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.responses import Response

from apps.api.web.services import wa_public_runtime


pytestmark = pytest.mark.unit


def _deps() -> wa_public_runtime.WaResponseDeps:
    return wa_public_runtime.WaResponseDeps(
        normalize_qr_id_fn=lambda value: None if value is None else str(value),
        derive_state_fn=lambda snapshot: ("qr", True) if snapshot and snapshot.get("qr") else (None, False),
        build_qr_url_fn=lambda tenant, key, qr_id=None: (
            f"/pub/wa/qr.svg?tenant={tenant}&k={key}" + (f"&qr_id={qr_id}" if qr_id else "")
        ),
    )


def test_compose_public_wa_response_copies_ready_snapshot() -> None:
    result = wa_public_runtime.compose_public_wa_response(
        55,
        "tenant-key",
        status_snapshot={"ok": True, "state": "ready", "ready": True, "connected": True},
        qr_id_override="abc",
        deps=_deps(),
    )

    assert result["tenant"] == 55
    assert result["state"] == "ready"
    assert result["need_qr"] is False
    assert result["qr_id"] == "abc"
    assert result["qr_url"].endswith("qr_id=abc")


def test_compose_public_wa_response_derives_qr_state_from_raw_snapshot() -> None:
    result = wa_public_runtime.compose_public_wa_response(
        55,
        "tenant-key",
        status_snapshot={"ok": True, "raw": {"qr": True, "qr_id": "from-raw"}},
        deps=_deps(),
    )

    assert result["state"] == "qr"
    assert result["need_qr"] is True
    assert result["qr_id"] == "from-raw"
    assert result["qr_url"].endswith("qr_id=from-raw")


def test_connect_wa_builds_template_context_and_settings_link() -> None:
    rendered = {}
    common = SimpleNamespace(
        list_keys=lambda tenant: [{"key": "fallback-key"}],
        ensure_tenant_files=lambda tenant: None,
        read_tenant_config=lambda tenant: {"passport": {"brand": "Brand"}},
        read_persona=lambda tenant: "Persona\nLine2",
        public_base_url=lambda request: "https://hub.avio.website",
        public_url=lambda request, value: f"https://hub.avio.website{value}",
    )
    request = SimpleNamespace(
        query_params={"k": "tenant-key"},
        url_for=lambda name, **params: f"/client/{params['tenant']}/settings",
    )

    def _render(template, context):
        rendered.update({"template": template, "context": context})
        return Response("ok")

    response = wa_public_runtime.connect_wa(
        7,
        request,
        k=None,
        deps=wa_public_runtime.WaConnectDeps(
            ensure_valid_qr_request_fn=lambda tenant, key, request, query_param_only: (tenant, key),
            invalid_key_response_fn=lambda: Response(status_code=401),
            common_module=common,
            render_template_fn=_render,
            quote_plus_fn=lambda value: value,
            time_module=SimpleNamespace(time=lambda: 123),
        ),
    )

    assert response.status_code == 200
    assert rendered["template"] == "connect/wa.html"
    assert rendered["context"]["tenant"] == 7
    assert rendered["context"]["key"] == "tenant-key"
    assert rendered["context"]["subtitle"] == "Brand"
    assert rendered["context"]["persona_preview"] == "Persona\nLine2"
    assert rendered["context"]["settings_link"] == "https://hub.avio.website/client/7/settings?k=tenant-key"


@pytest.mark.asyncio
async def test_legacy_status_impl_adds_cached_qr_id() -> None:
    common = SimpleNamespace(
        wa_base_url=lambda tenant: "http://wa.test",
        http=lambda method, url, timeout: (200, '{"ready":false,"qr":true,"last":"qr"}'),
    )

    result = await wa_public_runtime.legacy_status_impl(
        5,
        deps=wa_public_runtime.WaStatusImplDeps(
            common_module=common,
            json_module=__import__("json"),
            get_last_qr_id_fn=lambda tenant: ("qr123", False),
            normalize_qr_id_fn=lambda value: None if value is None else str(value),
            derive_state_fn=lambda data: ("qr", bool(data.get("qr"))),
            truthy_flag_fn=lambda value: str(value).lower() in {"1", "true", "yes"},
        ),
    )

    assert result["qr_id"] == "qr123"
    assert result["last"] == "qr"
    assert result["need_qr"] is True


@pytest.mark.asyncio
async def test_baileys_status_impl_normalizes_session_shape() -> None:
    common = SimpleNamespace(
        wabaileys_http=lambda method, path, timeout: (
            200,
            '{"ok":true,"session":{"status":"qr","connected":false,"qr":{"id":"qr-1"}}}',
        ),
    )

    result = await wa_public_runtime.baileys_status_impl(
        5,
        deps=wa_public_runtime.WaStatusImplDeps(
            common_module=common,
            json_module=__import__("json"),
            get_last_qr_id_fn=lambda tenant: (None, False),
            normalize_qr_id_fn=lambda value: None if value is None else str(value),
            derive_state_fn=lambda data: (None, False),
            truthy_flag_fn=bool,
        ),
    )

    assert result["ok"] is True
    assert result["state"] == "qr"
    assert result["need_qr"] is True
    assert result["qr_id"] == "qr-1"
