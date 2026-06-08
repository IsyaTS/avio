from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.worker.services import telegram_bridge_runtime


pytestmark = pytest.mark.unit


class _FakeRedis:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    async def set(self, key: str, value: str, *, ex: int) -> None:
        self.calls.append((key, value, ex))


@pytest.mark.anyio
async def test_telegram_bridge_runtime_caches_phone_and_stores_message() -> None:
    redis_client = _FakeRedis()
    inserted: list[dict[str, object]] = []

    async def fake_send(**_kwargs):
        return 200, '{"peer_id": 555, "message_id": 777, "username": "demo"}'

    async def fake_find_lead(_tenant: int, _peer_id: int):
        return None

    async def fake_upsert_lead(_lead_id, **_kwargs):
        return 91

    async def fake_insert_message_out(lead_id: int, text: str, **kwargs):
        inserted.append({"lead_id": lead_id, "text": text, **kwargs})

    async def fake_noop(*_args, **_kwargs):
        return None

    deps = telegram_bridge_runtime.TelegramBridgeDeps(
        tg_worker_token="",
        admin_token="",
        log_fn=lambda _msg: None,
        telegram_transport_module=SimpleNamespace(send=fake_send),
        json_module=__import__("json"),
        redis_client=redis_client,
        normalize_username_fn=lambda value: f"@{str(value).lstrip('@')}" if value else None,
        sanitize_display_name_fn=lambda value: value,
        find_lead_by_telegram_fn=fake_find_lead,
        upsert_lead_fn=fake_upsert_lead,
        crm_links_repo=SimpleNamespace(get_link=fake_noop),
        amocrm_service_module=SimpleNamespace(AMOCRM_PROVIDER="amocrm"),
        wait_for_amocrm_link_ready_fn=fake_noop,
        link_lead_contact_fn=fake_noop,
        update_contact_telegram_fn=fake_noop,
        resolve_live_amocrm_target_by_phone_fn=fake_noop,
        crm_outbox_repo=SimpleNamespace(cancel_pending_events=fake_noop, enqueue=fake_noop, has_recent_event=fake_noop),
        crm_chat_links_repo=SimpleNamespace(get_link=fake_noop, upsert_link=fake_noop),
        amocrm_chat_service_module=SimpleNamespace(AMOCRM_CHAT_PROVIDER="amocrm_chat", enqueue_message=fake_noop, sync_chat_profile=fake_noop),
        read_tenant_config_fn=lambda _tenant: {},
        reconcile_avito_bridge_amocrm_links_fn=fake_noop,
        create_task_fn=lambda _coro: None,
        insert_message_out_fn=fake_insert_message_out,
    )

    status, body = await telegram_bridge_runtime.send_telegram_to_target(
        3,
        "bridge hello",
        phone="+79990001122",
        lead_id=10,
        deps=deps,
    )

    assert status == 200
    assert body
    assert redis_client.calls == [("cache:avito_phone:3:555", "+79990001122", 604800)]
    assert inserted == [
        {
            "lead_id": 91,
            "text": "bridge hello",
            "provider_msg_id": "777",
            "status": "sent",
            "tenant_id": 3,
            "channel": "telegram",
            "telegram_user_id": 555,
            "telegram_username": "demo",
            "title": "@demo",
            "is_bot": True,
            "source": "bot",
        }
    ]
