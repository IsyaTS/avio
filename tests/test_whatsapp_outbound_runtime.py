from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.worker.services import whatsapp_outbound_runtime


pytestmark = pytest.mark.unit


def _build_deps(**overrides):
    async def fake_sleep(_seconds: float) -> None:
        return None

    async def fake_to_thread(fn, *args):
        return fn(*args)

    def fake_normalize(phone: str):
        digits = "".join(ch for ch in phone if ch.isdigit())
        if not digits:
            raise ValueError("bad")
        return digits, f"{digits}@c.us"

    deps = dict(
        log_fn=lambda _msg: None,
        waweb_base_url_fn=lambda _tenant: "http://waweb",
        wabaileys_base_url_fn=lambda: "http://baileys",
        normalize_whatsapp_recipient_fn=fake_normalize,
        whatsapp_address_error=ValueError,
        digits_fn=lambda value: "".join(ch for ch in str(value) if ch.isdigit()),
        tokenize_attachment_mapping_fn=lambda blob: dict(blob),
        build_wa_document_payload_fn=lambda blob: (dict(blob), None),
        http_json_fn=lambda *_args: (200, "ok"),
        sleep_fn=fake_sleep,
        asyncio_to_thread_fn=fake_to_thread,
        json_module=__import__("json"),
        wa_send_base_timeout=90.0,
        wa_send_timeout_per_mib=40.0,
        wa_send_timeout_max=300.0,
        wa_internal_token="",
        admin_token="",
        core_settings_module=SimpleNamespace(ADMIN_TOKEN=""),
    )
    deps.update(overrides)
    return whatsapp_outbound_runtime.WhatsAppOutboundDeps(**deps)


@pytest.mark.anyio
async def test_send_whatsapp_baileys_skips_missing_recipient() -> None:
    logs: list[str] = []
    deps = _build_deps(log_fn=logs.append)

    status, body = await whatsapp_outbound_runtime.send_whatsapp_baileys(
        3,
        "",
        deps=deps,
    )

    assert (status, body) == (422, "missing_recipient")
    assert any("status=skipped_missing_recipient" in item for item in logs)


@pytest.mark.anyio
async def test_send_whatsapp_retries_on_5xx() -> None:
    attempts: list[int] = []

    def fake_http_json(*_args):
        attempts.append(1)
        if len(attempts) < 3:
            return 500, "upstream_error"
        return 200, "ok"

    deps = _build_deps(http_json_fn=fake_http_json)

    status, body = await whatsapp_outbound_runtime.send_whatsapp(
        5,
        "+79990001122",
        text="hello",
        deps=deps,
    )

    assert (status, body) == (200, "ok")
    assert len(attempts) == 3
