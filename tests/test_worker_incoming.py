from __future__ import annotations
import json

import pytest

from apps.worker import main as worker_module
from libs.core.services.catalog_flow import CatalogFlowResult


pytestmark = pytest.mark.unit


async def _false_async(*_args, **_kwargs):
    return False


async def _noop_async(*_args, **_kwargs):
    return None


@pytest.fixture(autouse=True)
def _disable_external_worker_side_effects(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(worker_module, "_thread_has_recent_bot_reply", _false_async, raising=False)
    monkeypatch.setattr(worker_module, "_is_handoff_silenced", _false_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "handle_opt_out", _false_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "capture_followup_answer", _noop_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "schedule_followups", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_maybe_amocrm_inbound", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_select_auto_photos", lambda *_a, **_k: _return_async([]), raising=False)
    monkeypatch.setattr(worker_module, "send_whatsapp", lambda *_a, **_k: _return_async((200, "ok")), raising=False)
    monkeypatch.setattr(worker_module, "send_whatsapp_baileys", lambda *_a, **_k: _return_async((200, "ok")), raising=False)


@pytest.mark.anyio
async def test_handle_max_personal_incoming_normalizes_channel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def _fake_handle(event):
        captured.update(event)

    monkeypatch.setattr(worker_module, "_handle_max_incoming", _fake_handle, raising=False)
    await worker_module._handle_max_personal_incoming({"tenant": 3, "text": "привет"})

    assert captured["tenant"] == 3
    assert captured["channel"] == "max_personal"
    assert captured["ch"] == "max_personal"
    assert captured["provider"] == "max_personal"


@pytest.mark.anyio
async def test_worker_handles_whatsapp_event(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict]] = []

    class FakeRedis:
        async def lpush(self, key: str, value: str) -> None:
            events.append((key, json.loads(value)))

    inserted: list[tuple[int, str, str, int | None]] = []

    async def fake_insert_message_in(
        lead_id: int,
        text: str,
        *,
        status: str = "received",
        tenant_id: int | None = None,
        telegram_user_id: int | None = None,
    ) -> None:
        inserted.append((lead_id, text, status, tenant_id))

    async def fake_get_or_create_by_peer(
        tenant_id: int,
        channel: str,
        peer: str,
        *,
        lead_id_hint: int | None = None,
        source_real_id: int | None = None,
        title: str | None = None,
        contact: str | None = None,
    ) -> int:
        return lead_id_hint or 1234

    async def fake_resolve_contact(**kwargs: object) -> int:
        return 777

    async def fake_link_contact(lead_id: int, contact_id: int, **kwargs: object) -> None:
        return None

    class _PipelineResult:
        def __init__(self, reply_text: str, source: str = "llm") -> None:
            self.reply_text = reply_text
            self.source = source

    async def fake_run_response_pipeline(**kwargs: object) -> _PipelineResult:
        return _PipelineResult("pipeline-reply")

    async def fake_produce_reply(**kwargs: object) -> bool:
        await worker_module.r.lpush(
            worker_module.OUTBOX_QUEUE_KEY,
            json.dumps(
                {
                    "provider": "whatsapp",
                    "to": "79991234567",
                    "text": "pipeline-reply",
                }
            ),
        )
        return True

    monkeypatch.setattr(worker_module, "r", FakeRedis(), raising=False)
    monkeypatch.setattr(worker_module, "insert_message_in", fake_insert_message_in, raising=False)
    monkeypatch.setattr(
        worker_module,
        "get_or_create_by_peer",
        fake_get_or_create_by_peer,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module,
        "resolve_or_create_contact",
        fake_resolve_contact,
        raising=False,
    )
    monkeypatch.setattr(worker_module, "link_lead_contact", fake_link_contact, raising=False)
    monkeypatch.setattr(worker_module, "smart_reply_enabled", lambda *_: True, raising=False)
    monkeypatch.setattr(worker_module, "run_response_pipeline", fake_run_response_pipeline, raising=False)
    monkeypatch.setattr(worker_module, "_is_handoff_silenced", _false_async, raising=False)
    monkeypatch.setattr(worker_module, "_produce_and_enqueue_smart_reply", fake_produce_reply, raising=False)

    event = {
        "channel": "whatsapp",
        "tenant": 9,
        "message_id": "MSG-1",
        "from": "79991234567",
        "text": "hello",
        "lead_id": 1234,
    }

    await worker_module._handle_incoming_event(event)

    assert inserted == [(1234, "hello", "received", 9)]
    assert events, "expected outgoing reply to be enqueued"
    queue, payload = events[0]
    assert queue == worker_module.OUTBOX_QUEUE_KEY
    assert payload["provider"] == "whatsapp"
    assert payload["to"] == "79991234567"
    assert payload["text"] == "pipeline-reply"


@pytest.mark.anyio
async def test_worker_handles_whatsapp_event_via_response_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[tuple[str, dict]] = []

    class FakeRedis:
        async def lpush(self, key: str, value: str) -> None:
            events.append((key, json.loads(value)))

    inserted: list[tuple[int, str, str, int | None]] = []

    async def fake_insert_message_in(
        lead_id: int,
        text: str,
        *,
        status: str = "received",
        tenant_id: int | None = None,
        telegram_user_id: int | None = None,
    ) -> None:
        inserted.append((lead_id, text, status, tenant_id))

    async def fake_get_or_create_by_peer(
        tenant_id: int,
        channel: str,
        peer: str,
        *,
        lead_id_hint: int | None = None,
        source_real_id: int | None = None,
        title: str | None = None,
        contact: str | None = None,
    ) -> int:
        return lead_id_hint or 1234

    async def fake_resolve_contact(**kwargs: object) -> int:
        return 777

    async def fake_link_contact(lead_id: int, contact_id: int, **kwargs: object) -> None:
        return None

    async def fake_run_response_pipeline(**kwargs: object):
        raise RuntimeError("pipeline failed")

    async def fake_produce_reply(**kwargs: object) -> bool:
        await worker_module.r.lpush(
            worker_module.OUTBOX_QUEUE_KEY,
            json.dumps(
                {
                    "provider": "whatsapp",
                    "to": "79991234567",
                    "text": "каталог",
                }
            ),
        )
        return True

    monkeypatch.setattr(worker_module, "r", FakeRedis(), raising=False)
    monkeypatch.setattr(worker_module, "insert_message_in", fake_insert_message_in, raising=False)
    monkeypatch.setattr(
        worker_module,
        "get_or_create_by_peer",
        fake_get_or_create_by_peer,
        raising=False,
    )
    monkeypatch.setattr(
        worker_module,
        "resolve_or_create_contact",
        fake_resolve_contact,
        raising=False,
    )
    monkeypatch.setattr(worker_module, "link_lead_contact", fake_link_contact, raising=False)
    monkeypatch.setattr(worker_module, "smart_reply_enabled", lambda *_: True, raising=False)
    monkeypatch.setattr(worker_module, "run_response_pipeline", fake_run_response_pipeline, raising=False)
    monkeypatch.setattr(worker_module, "_is_handoff_silenced", _false_async, raising=False)
    monkeypatch.setattr(worker_module, "_produce_and_enqueue_smart_reply", fake_produce_reply, raising=False)

    event = {
        "channel": "whatsapp",
        "tenant": 9,
        "message_id": "MSG-2",
        "from": "79991234567",
        "text": "hello",
        "lead_id": 1234,
    }

    await worker_module._handle_incoming_event(event)

    assert inserted == [(1234, "hello", "received", 9)]
    assert events, "expected outgoing reply to be enqueued"
    queue, payload = events[0]
    assert queue == worker_module.OUTBOX_QUEUE_KEY
    assert payload["provider"] == "whatsapp"
    assert payload["to"] == "79991234567"
    assert payload["text"]
    assert "каталог" in payload["text"].lower()


def test_compose_burst_user_text_has_no_service_prefix() -> None:
    out = worker_module._compose_burst_user_text(
        ["здравствуйте", "можно каталог", "и цены пожалуйста"]
    )
    assert "Клиент отправил несколько сообщений подряд" not in out
    assert out == "здравствуйте\nможно каталог\nи цены пожалуйста"


@pytest.mark.anyio
async def test_telegram_incoming_dedup_by_message_id(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeRedis:
        def __init__(self) -> None:
            self._seen: set[str] = set()

        async def set(self, key: str, value: str, ex: int | None = None, nx: bool | None = None):
            if nx:
                if key in self._seen:
                    return False
                self._seen.add(key)
                return True
            self._seen.add(key)
            return True

    fake = FakeRedis()
    monkeypatch.setattr(worker_module, "r", fake, raising=False)

    first = await worker_module._is_duplicate_telegram_incoming(
        tenant_id=101,
        message_id=77,
        telegram_user_id=944310340,
        peer="944310340",
    )
    second = await worker_module._is_duplicate_telegram_incoming(
        tenant_id=101,
        message_id=77,
        telegram_user_id=944310340,
        peer="944310340",
    )

    assert first is False
    assert second is True


@pytest.mark.anyio
async def test_telegram_incoming_runs_catalog_flow_before_smart_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRedis:
        async def get(self, _key: str):
            return None

    async def _noop_async(*_args, **_kwargs):
        return None

    async def _false_async(*_args, **_kwargs):
        return False

    monkeypatch.setattr(worker_module, "r", FakeRedis(), raising=False)
    monkeypatch.setattr(worker_module, "_is_duplicate_telegram_incoming", _false_async, raising=False)
    monkeypatch.setattr(worker_module, "find_lead_by_telegram", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "upsert_lead", lambda *_a, **_k: _return_async(944310340), raising=False)
    monkeypatch.setattr(worker_module, "_store_lead_tg_slot", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_telegram_slot_is_enabled", lambda *_a, **_k: True, raising=False)
    monkeypatch.setattr(worker_module, "_looks_like_manager_outgoing", lambda *_a, **_k: False, raising=False)
    monkeypatch.setattr(worker_module, "_is_manager_message", lambda *_a, **_k: False, raising=False)
    monkeypatch.setattr(worker_module.followups, "handle_opt_out", _false_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "capture_followup_answer", _noop_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "schedule_followups", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_maybe_amocrm_inbound", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "get_contact_id_by_lead", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "get_contact_id_by_phone", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "resolve_or_create_contact", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "update_contact_telegram", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "update_contact_phone", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "link_lead_contact", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_match_behavior_trigger", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(worker_module, "_is_handoff_silenced", _false_async, raising=False)
    monkeypatch.setattr(worker_module, "get_contact_phone_by_lead", lambda *_a, **_k: _return_async("+79990001122"), raising=False)
    monkeypatch.setattr(worker_module, "_telegram_reply_enabled", lambda *_a, **_k: True, raising=False)
    monkeypatch.setattr(worker_module, "smart_reply_enabled", lambda *_a, **_k: True, raising=False)
    monkeypatch.setattr(worker_module, "read_tenant_config", lambda *_a, **_k: {"behavior": {"send_catalog_on_first_message": True}}, raising=False)

    called = {"catalog_flow": 0}

    async def _fake_catalog_flow(**_kwargs):
        called["catalog_flow"] += 1
        return CatalogFlowResult(catalog_sent=True, stop_processing=True, stop_reason="catalog_only")

    if worker_module.catalog_flow_service is not None:
        monkeypatch.setattr(worker_module.catalog_flow_service, "handle_catalog_flow", _fake_catalog_flow, raising=False)

    async def _should_not_run(*_args, **_kwargs):
        raise AssertionError("smart reply should not run when catalog flow stops processing")

    monkeypatch.setattr(worker_module, "_try_handle_smart_reply_with_delay", _should_not_run, raising=False)
    monkeypatch.setattr(worker_module, "_produce_and_enqueue_smart_reply", _should_not_run, raising=False)

    event = {
        "channel": "telegram",
        "tenant": 101,
        "message_id": 1202,
        "telegram_user_id": 944310340,
        "peer_id": 944310340,
        "peer": "944310340",
        "username": "Isyyaa",
        "text": "Здравствуйте",
        "trigger_bot": True,
    }

    await worker_module._handle_telegram_incoming(event)
    assert called["catalog_flow"] == 1


@pytest.mark.anyio
async def test_max_personal_incoming_runs_catalog_flow_before_smart_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRedis:
        async def get(self, _key: str):
            return None

    async def _noop_async(*_args, **_kwargs):
        return None

    async def _false_async(*_args, **_kwargs):
        return False

    async def _fake_get_or_create_by_peer(*_args, **_kwargs):
        return 888001

    monkeypatch.setattr(worker_module, "r", FakeRedis(), raising=False)
    monkeypatch.setattr(worker_module, "get_or_create_by_peer", _fake_get_or_create_by_peer, raising=False)
    monkeypatch.setattr(worker_module, "upsert_lead", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_looks_like_manager_outgoing", lambda *_a, **_k: False, raising=False)
    monkeypatch.setattr(worker_module, "_is_manager_message", lambda *_a, **_k: False, raising=False)
    monkeypatch.setattr(worker_module.followups, "handle_opt_out", _false_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "capture_followup_answer", _noop_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "schedule_followups", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_maybe_amocrm_inbound", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "resolve_or_create_contact", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "insert_message_in", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_match_behavior_trigger", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(worker_module, "_is_handoff_silenced", _false_async, raising=False)
    monkeypatch.setattr(worker_module, "_max_personal_reply_enabled", lambda *_a, **_k: True, raising=False)
    monkeypatch.setattr(worker_module, "smart_reply_enabled", lambda *_a, **_k: True, raising=False)
    monkeypatch.setattr(
        worker_module,
        "read_tenant_config",
        lambda *_a, **_k: {"behavior": {"send_catalog_on_first_message": True}},
        raising=False,
    )

    called = {"catalog_flow": 0}

    async def _fake_catalog_flow(**_kwargs):
        called["catalog_flow"] += 1
        return CatalogFlowResult(catalog_sent=True, stop_processing=True, stop_reason="catalog_only")

    if worker_module.catalog_flow_service is not None:
        monkeypatch.setattr(worker_module.catalog_flow_service, "handle_catalog_flow", _fake_catalog_flow, raising=False)

    async def _should_not_run(*_args, **_kwargs):
        raise AssertionError("smart reply should not run when catalog flow stops processing")

    monkeypatch.setattr(worker_module, "_try_handle_smart_reply_with_delay", _should_not_run, raising=False)
    monkeypatch.setattr(worker_module, "_produce_and_enqueue_smart_reply", _should_not_run, raising=False)

    event = {
        "channel": "max_personal",
        "tenant": 101,
        "message_id": "m-1202",
        "chat_id": "chat-500",
        "max_user_id": 500,
        "display_name": "Ися",
        "text": "Здравствуйте",
    }

    await worker_module._handle_max_incoming(event)
    assert called["catalog_flow"] == 1


@pytest.mark.anyio
async def test_max_personal_worker_skips_db_insert_when_api_already_stored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRedis:
        async def get(self, _key: str):
            return None

    async def _noop_async(*_args, **_kwargs):
        return None

    async def _false_async(*_args, **_kwargs):
        return False

    async def _fake_get_or_create_by_peer(*_args, **_kwargs):
        return 888002

    inserted: list[tuple] = []

    async def _fake_insert_message_in(*args, **kwargs):
        inserted.append((args, kwargs))

    monkeypatch.setattr(worker_module, "r", FakeRedis(), raising=False)
    monkeypatch.setattr(worker_module, "get_or_create_by_peer", _fake_get_or_create_by_peer, raising=False)
    monkeypatch.setattr(worker_module, "upsert_lead", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_looks_like_manager_outgoing", lambda *_a, **_k: False, raising=False)
    monkeypatch.setattr(worker_module, "_is_manager_message", lambda *_a, **_k: False, raising=False)
    monkeypatch.setattr(worker_module.followups, "handle_opt_out", _false_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "capture_followup_answer", _noop_async, raising=False)
    monkeypatch.setattr(worker_module.followups, "schedule_followups", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_maybe_amocrm_inbound", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "resolve_or_create_contact", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "insert_message_in", _fake_insert_message_in, raising=False)
    monkeypatch.setattr(worker_module, "_match_behavior_trigger", lambda *_a, **_k: None, raising=False)
    monkeypatch.setattr(worker_module, "_is_handoff_silenced", _false_async, raising=False)
    monkeypatch.setattr(worker_module, "_max_personal_reply_enabled", lambda *_a, **_k: False, raising=False)

    await worker_module._handle_max_incoming(
        {
            "channel": "max_personal",
            "tenant": 101,
            "message_id": "m-stored-1",
            "chat_id": "chat-500",
            "max_user_id": 500,
            "display_name": "Ися",
            "text": "Здравствуйте",
            "_incoming_stored": True,
        }
    )

    assert inserted == []


@pytest.mark.anyio
async def test_max_personal_manager_event_marks_handoff_without_incoming_or_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRedis:
        async def get(self, _key: str):
            return None

    async def _noop_async(*_args, **_kwargs):
        return None

    async def _fake_get_or_create_by_peer(*_args, **_kwargs):
        return 888003

    inserted: list[tuple] = []
    scheduled: list[tuple] = []
    replies: list[tuple] = []
    handoff: list[dict[str, object]] = []

    async def _fake_insert_message_in(*args, **kwargs):
        inserted.append((args, kwargs))

    async def _fake_schedule(*args, **kwargs):
        scheduled.append((args, kwargs))

    async def _fake_reply(*args, **kwargs):
        replies.append((args, kwargs))

    async def _fake_handoff(tenant_id, lead_id, **kwargs):
        handoff.append({"tenant_id": tenant_id, "lead_id": lead_id, **kwargs})

    monkeypatch.setattr(worker_module, "r", FakeRedis(), raising=False)
    monkeypatch.setattr(worker_module, "get_or_create_by_peer", _fake_get_or_create_by_peer, raising=False)
    monkeypatch.setattr(worker_module, "upsert_lead", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_looks_like_manager_outgoing", lambda *_a, **_k: True, raising=False)
    monkeypatch.setattr(worker_module, "_is_manager_message", lambda *_a, **_k: False, raising=False)
    monkeypatch.setattr(worker_module.followups, "schedule_followups", _fake_schedule, raising=False)
    monkeypatch.setattr(worker_module, "_maybe_amocrm_inbound", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "resolve_or_create_contact", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "insert_message_in", _fake_insert_message_in, raising=False)
    monkeypatch.setattr(worker_module, "_mark_handoff_silence", _fake_handoff, raising=False)
    monkeypatch.setattr(worker_module, "_cancel_pending_smart_reply", _noop_async, raising=False)
    monkeypatch.setattr(worker_module, "_produce_and_enqueue_smart_reply", _fake_reply, raising=False)
    monkeypatch.setattr(worker_module, "_try_handle_smart_reply_with_delay", _fake_reply, raising=False)

    await worker_module._handle_max_incoming(
        {
            "channel": "max_personal",
            "tenant": 101,
            "message_id": "m-manager-1",
            "chat_id": "chat-500",
            "max_user_id": 500,
            "display_name": "Ися",
            "text": "ручной ответ менеджера",
            "origin": "max_personal:manager",
            "manager": True,
            "out": True,
        }
    )

    assert handoff and handoff[0]["reason"] == "manager_outgoing"
    assert inserted == []
    assert scheduled == []
    assert replies == []


@pytest.mark.anyio
async def test_smart_reply_delay_bypass_skips_burst_scheduler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _recent_bot_reply(*_args, **_kwargs):
        return True

    async def _should_not_schedule(*_args, **_kwargs):
        raise AssertionError("first catalog reply must not be delayed")

    monkeypatch.setattr(worker_module, "_channel_delay_enabled", lambda *_a, **_k: True, raising=False)
    monkeypatch.setattr(worker_module, "_thread_has_recent_bot_reply", _recent_bot_reply, raising=False)
    monkeypatch.setattr(worker_module, "_schedule_delayed_smart_reply", _should_not_schedule, raising=False)

    delayed = await worker_module._try_handle_smart_reply_with_delay(
        tenant_id=101,
        lead_id=5001,
        channel="max_personal",
        refer_id=5001,
        user_text="здравствуйте",
        context={},
        bypass_delay=True,
    )

    assert delayed is False


@pytest.mark.anyio
async def test_smart_reply_delay_still_schedules_after_first_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduled: list[dict[str, object]] = []

    async def _recent_bot_reply(*_args, **_kwargs):
        return True

    async def _schedule(**kwargs):
        scheduled.append(dict(kwargs))

    monkeypatch.setattr(worker_module, "_channel_delay_enabled", lambda *_a, **_k: True, raising=False)
    monkeypatch.setattr(worker_module, "_thread_has_recent_bot_reply", _recent_bot_reply, raising=False)
    monkeypatch.setattr(worker_module, "_schedule_delayed_smart_reply", _schedule, raising=False)

    delayed = await worker_module._try_handle_smart_reply_with_delay(
        tenant_id=101,
        lead_id=5001,
        channel="max_personal",
        refer_id=5001,
        user_text="второе сообщение",
        context={},
    )

    assert delayed is True
    assert scheduled and scheduled[0]["channel"] == "max_personal"


async def _return_async(value):
    return value
