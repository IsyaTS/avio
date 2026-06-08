from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.api.web.services import avito_webhook_runtime


pytestmark = pytest.mark.unit


class _Req:
    def __init__(self, query_params=None):
        self.query_params = query_params or {}


def _deps(**overrides):
    async def _noop_async(*_args, **_kwargs):
        return None

    async def _resolve_tenant_by_chat(_chat_id):
        return None, None

    normalized = SimpleNamespace(
        payload={"value": {"chat_id": "chat-1"}},
        value={"chat_id": "chat-1"},
        content={},
        account_id=None,
        item_id=None,
        chat_id="chat-1",
        message_type="in",
        text="hello",
        attachments=[],
        unresolved_voice=None,
        message_id="msg-1",
        avito_user_id=11,
        avito_login="seller",
        created_at=None,
        published_at=None,
    )
    deps = dict(
        avito_webhook_events_module=SimpleNamespace(normalize_public_webhook_event=lambda _event: normalized),
        logger=SimpleNamespace(warning=lambda *a, **k: None, info=lambda *a, **k: None, debug=lambda *a, **k: None, exception=lambda *a, **k: None),
        json_module=__import__("json"),
        avito_module=SimpleNamespace(
            resolve_tenant_by_chat=_resolve_tenant_by_chat,
            find_tenant_by_account=lambda _account_id: None,
            get_integration=lambda _tenant: {},
            resolve_voice_url=_noop_async,
            stable_lead_id=lambda _account_id, _chat_id: 501,
        ),
        coerce_int_fn=lambda value: int(value) if value not in (None, "") else None,
        find_lead_by_peer_fn=_noop_async,
        redis_queue=None,
        content_fingerprint_fn=lambda text, attachments: "fp1",
        avito_bot_echo_key_fn=lambda tenant, chat_id: f"{tenant}:{chat_id}",
        normalize_echo_text_fn=lambda value: str(value or "").strip().lower(),
        is_recent_bot_echo_fn=lambda *_a, **_k: _noop_async(),
        time_module=SimpleNamespace(time=lambda: 12345),
        handoff_silence_key_fn=lambda tenant, lead_id: f"silence:{tenant}:{lead_id}",
        handoff_silence_meta_key_fn=lambda tenant, lead_id: f"meta:{tenant}:{lead_id}",
        handoff_silence_ttl_seconds=60,
        db_module=SimpleNamespace(_fetchrow=None),
        insert_message_out_fn=_noop_async,
        capture_manager_intervention_fn=_noop_async,
        amocrm_service_module=SimpleNamespace(amocrm_on_outbound_message=_noop_async),
        process_incoming_fn=_noop_async,
    )
    deps.update(overrides)
    return avito_webhook_runtime.AvitoWebhookDeps(**deps)


@pytest.mark.anyio
async def test_handle_avito_webhook_event_skips_unknown_tenant() -> None:
    processed: list[dict] = []

    async def _process_incoming(payload, request):
        processed.append(payload)

    result = await avito_webhook_runtime.handle_avito_webhook_event(
        {"event": "x"},
        _Req(),
        deps=_deps(process_incoming_fn=_process_incoming),
    )

    assert result is False
    assert processed == []


@pytest.mark.anyio
async def test_handle_avito_webhook_event_processes_incoming_with_tenant_from_query() -> None:
    processed: list[dict] = []

    async def _process_incoming(payload, request):
        processed.append(payload)

    deps = _deps(process_incoming_fn=_process_incoming)
    result = await avito_webhook_runtime.handle_avito_webhook_event(
        {"event": "x"},
        _Req({"tenant": "3"}),
        deps=deps,
    )

    assert result is True
    assert processed
    assert processed[0]["tenant"] == 3
    assert processed[0]["lead_id"] == 501


@pytest.mark.anyio
async def test_handle_avito_webhook_event_passes_item_id_to_incoming_body() -> None:
    processed: list[dict] = []

    async def _process_incoming(payload, request):
        processed.append(payload)

    normalized = SimpleNamespace(
        payload={"value": {"chat_id": "chat-1", "item_id": "749"}},
        value={"chat_id": "chat-1", "item_id": "749"},
        content={},
        account_id=11,
        item_id=749,
        chat_id="chat-1",
        message_type="in",
        text="hello",
        attachments=[],
        unresolved_voice=None,
        message_id="msg-1",
        avito_user_id=22,
        avito_login="buyer",
        created_at=None,
        published_at=None,
    )
    deps = _deps(
        avito_webhook_events_module=SimpleNamespace(normalize_public_webhook_event=lambda _event: normalized),
        process_incoming_fn=_process_incoming,
    )

    result = await avito_webhook_runtime.handle_avito_webhook_event(
        {"event": "x"},
        _Req({"tenant": "3"}),
        deps=deps,
    )

    assert result is True
    assert processed[0]["item_id"] == 749
    assert processed[0]["source"]["item_id"] == 749
    assert processed[0]["message"]["item_id"] == 749
    assert processed[0]["avito"]["item_id"] == 749


@pytest.mark.anyio
async def test_handle_avito_webhook_event_dedups_manager_outgoing() -> None:
    class _Redis:
        async def set(self, key, value, ex=None, nx=None):
            return None

        async def get(self, key):
            return None

    inserted = []

    async def _insert_message_out(*args, **kwargs):
        inserted.append((args, kwargs))

    normalized = SimpleNamespace(
        payload={"value": {"chat_id": "chat-1"}},
        value={"chat_id": "chat-1"},
        content={},
        account_id=11,
        item_id=None,
        chat_id="chat-1",
        message_type="out",
        text="manager hello",
        attachments=[],
        unresolved_voice=None,
        message_id="msg-1",
        avito_user_id=11,
        avito_login="seller",
        created_at=None,
        published_at=None,
    )

    deps = _deps(
        avito_webhook_events_module=SimpleNamespace(normalize_public_webhook_event=lambda _event: normalized),
        redis_queue=_Redis(),
        insert_message_out_fn=_insert_message_out,
    )
    result = await avito_webhook_runtime.handle_avito_webhook_event(
        {"event": "x"},
        _Req({"tenant": "3"}),
        deps=deps,
    )

    assert result is False
    assert inserted == []


@pytest.mark.anyio
async def test_handle_avito_webhook_event_skips_bot_image_echo() -> None:
    import json

    class _Redis:
        async def set(self, key, value, ex=None, nx=None):
            return True

        async def get(self, key):
            if key == "3:chat-1":
                return json.dumps({"text": "__image__", "extra": ["__image__"], "ts": 123})
            return None

    inserted = []
    captured = []

    async def _insert_message_out(*args, **kwargs):
        inserted.append((args, kwargs))

    async def _capture(*args, **kwargs):
        captured.append((args, kwargs))

    normalized = SimpleNamespace(
        payload={"value": {"chat_id": "chat-1"}},
        value={"chat_id": "chat-1"},
        content={},
        account_id=11,
        item_id=None,
        chat_id="chat-1",
        message_type="out",
        text="",
        attachments=[{"type": "image", "url": "https://example.test/img.jpg"}],
        unresolved_voice=None,
        message_id="msg-image-echo",
        avito_user_id=11,
        avito_login="seller",
        created_at=None,
        published_at=None,
    )

    deps = _deps(
        avito_webhook_events_module=SimpleNamespace(normalize_public_webhook_event=lambda _event: normalized),
        redis_queue=_Redis(),
        insert_message_out_fn=_insert_message_out,
        capture_manager_intervention_fn=_capture,
    )
    result = await avito_webhook_runtime.handle_avito_webhook_event(
        {"event": "x"},
        _Req({"tenant": "3"}),
        deps=deps,
    )

    assert result is False
    assert inserted == []
    assert captured == []
