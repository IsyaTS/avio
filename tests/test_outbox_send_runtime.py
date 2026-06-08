from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.worker.services import outbox_send_runtime
from libs.core.services import outbox_payloads


pytestmark = pytest.mark.unit


def _build_deps(**overrides):
    async def _async_false(*_args, **_kwargs):
        return False

    async def _async_none(*_args, **_kwargs):
        return None

    async def _async_ok(*_args, **_kwargs):
        return 200, "ok"

    deps = dict(
        default_tenant_id=1,
        outbox_enabled=True,
        outbox_enabled_raw="",
        send_enabled=True,
        redis_client=SimpleNamespace(),
        json_module=__import__("json"),
        log_fn=lambda _msg: None,
        db_errors_counter=SimpleNamespace(labels=lambda *_args: SimpleNamespace(inc=lambda: None)),
        outbox_payloads_module=SimpleNamespace(
            build_outbox_item_plan=lambda item, default_tenant_id, normalize_tg_slot_fn: SimpleNamespace(
                channel=item.get("channel", "whatsapp"),
                text=item.get("text"),
                lead_id=item.get("lead_id", 0),
                phone=item.get("phone"),
                raw_to=item.get("to"),
                to_peer_raw=item.get("to_peer"),
                peer_raw=item.get("peer"),
                peer_value=item.get("peer"),
                username=item.get("username"),
                item_tg_slot=1,
                telegram_user_id=None,
                primary_telegram_user_id=None,
                tenant_id=item.get("tenant_id", default_tenant_id),
                attachment=item.get("attachment"),
                attachments=item.get("attachments"),
                reply_to=item.get("reply_to"),
                avito_account_id=None,
                avito_chat_id_hint=None,
                max_user_id=None,
                max_chat_id_hint=None,
            ),
            build_send_outcome=lambda code: SimpleNamespace(
                status="sent" if 200 <= code < 300 else "failed",
                reason="ok" if 200 <= code < 300 else "failed",
            ),
            resolve_cached_whatsapp_jid=_async_none,
        ),
        queue_contract_module=SimpleNamespace(push_json_left=_async_none),
        outbox_queue_key="outbox:send",
        outbox_dlq_key="outbox:dlq",
        normalize_tg_slot_fn=lambda value: int(value or 1),
        whitelist_allows_fn=lambda **_kwargs: _async_false(),
        lead_exists_fn=_async_false,
        coerce_int_fn=lambda value: int(value) if value is not None else None,
        get_lead_peer_fn=_async_none,
        get_telegram_user_id_by_lead_fn=_async_none,
        find_lead_by_telegram_fn=_async_none,
        normalize_username_fn=lambda value: value,
        upsert_lead_fn=_async_none,
        get_lead_tg_slot_fn=_async_none,
        telegram_slot_is_enabled_fn=lambda _tenant, _slot: True,
        is_manager_message_fn=lambda _item: False,
        is_followup_message_fn=lambda _item: False,
        mark_handoff_silence_fn=_async_none,
        collect_outgoing_attachments_fn=lambda _item, _tenant: [],
        insert_message_out_fn=_async_none,
        prepare_internal_attachment_fn=_async_none,
        tenant_whatsapp_provider_fn=lambda _tenant: "waweb",
        send_whatsapp_baileys_fn=_async_ok,
        send_whatsapp_fn=_async_ok,
        avito_bot_echo_key_fn=lambda tenant, chat: f"{tenant}:{chat}",
        avito_bot_echo_ttl_seconds=60,
        send_avito_fn=_async_ok,
        send_telegram_fn=_async_ok,
        send_max_fn=_async_ok,
        send_max_personal_fn=_async_ok,
        cache_max_bot_echo_fn=_async_none,
        update_message_status_fn=_async_none,
        amocrm_service_module=SimpleNamespace(amocrm_on_outbound_message=_async_none),
    )
    deps.update(overrides)
    return outbox_send_runtime.OutboxSendDeps(**deps)


@pytest.mark.anyio
async def test_outbox_send_runtime_skips_empty_item() -> None:
    logs: list[str] = []
    deps = _build_deps(log_fn=logs.append)

    result = await outbox_send_runtime.do_send({"channel": "whatsapp", "lead_id": 7}, deps=deps)

    assert result == ("skipped", "empty", "", 0)
    assert logs == ["event=send_result status=skipped reason=empty channel=whatsapp lead_id=7"]


@pytest.mark.anyio
async def test_outbox_send_runtime_skips_when_outbox_disabled() -> None:
    logs: list[str] = []
    deps = _build_deps(log_fn=logs.append, outbox_enabled=False, outbox_enabled_raw="0")

    result = await outbox_send_runtime.do_send(
        {"channel": "whatsapp", "lead_id": 7, "text": "hello"},
        deps=deps,
    )

    assert result == ("skipped", "outbox_disabled", "", 0)
    assert any("reason=outbox_disabled" in item for item in logs)


@pytest.mark.anyio
async def test_avito_echo_cache_merges_recent_variants() -> None:
    import json

    class _Redis:
        def __init__(self) -> None:
            self.values = {
                "101:chat-1": json.dumps({"text": "first", "extra": ["first"], "ts": 1})
            }

        async def get(self, key):
            return self.values.get(str(key))

        async def set(self, key, value, ex=None):
            self.values[str(key)] = str(value)
            return True

    async def _send_avito(*_args, **_kwargs):
        return 200, "ok"

    deps = _build_deps(
        redis_client=_Redis(),
        outbox_payloads_module=outbox_payloads,
        send_avito_fn=_send_avito,
        get_lead_peer_fn=lambda _lead_id, _channel: "chat-1",
    )
    state = outbox_send_runtime.OutboxSendState(
        item={},
        deps=deps,
        channel="avito",
        text="second",
        lead_id=501,
        phone=None,
        raw_to=None,
        to_peer_raw=None,
        peer_raw=None,
        peer_value=None,
        username=None,
        item_tg_slot=1,
        telegram_user_id=None,
        primary_telegram_user_id=None,
        tenant=101,
        attachment=None,
        attachments=None,
        reply_to=None,
        avito_account_id=222,
        avito_chat_id_hint="chat-1",
        max_user_id=None,
        max_chat_id_hint=None,
        actual_lead_id=501,
        manager_message=False,
    )

    status, _body = await outbox_send_runtime._send_avito_outbox_channel(state)

    assert status == 200
    cached = json.loads(deps.redis_client.values["101:chat-1"])
    assert "first" in cached["extra"]
    assert "second" in cached["extra"]
