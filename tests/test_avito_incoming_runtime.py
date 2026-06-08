from __future__ import annotations

from typing import Any

import pytest

from apps.worker.services import avito_incoming_runtime


pytestmark = pytest.mark.unit


class _FakeRedis:
    async def set(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def get(self, *args: Any, **kwargs: Any) -> None:
        return None


class _MemoryRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def set(self, key: str, value: str, *args: Any, **kwargs: Any) -> None:
        self.values[str(key)] = str(value)

    async def get(self, key: str, *args: Any, **kwargs: Any) -> str | None:
        return self.values.get(str(key))


def _deps(logs: list[str], **overrides: Any) -> avito_incoming_runtime.AvitoIncomingDeps:
    async def _noop(*args: Any, **kwargs: Any) -> Any:
        return None

    async def _lead_id(*args: Any, **kwargs: Any) -> int:
        return 501

    async def _true(*args: Any, **kwargs: Any) -> bool:
        return True

    deps = dict(
        avito_chat_cache={},
        redis_client=_FakeRedis(),
        phone_tg_ttl_seconds=60,
        auto_reply_ttl_seconds=60,
        testing_mode=False,
        log_fn=logs.append,
        coerce_int_fn=lambda value: int(value) if str(value).isdigit() else 0,
        extract_ru_phone_fn=lambda _text: "",
        extract_tg_username_fn=lambda _text: "",
        avito_phone_tg_template_fn=lambda _tenant: "",
        avito_auto_reply_text_fn=lambda _tenant: "",
        resolve_avito_user_name_fn=_noop,
        get_or_create_by_peer_fn=_lead_id,
        lead_exists_fn=_true,
        upsert_lead_fn=_noop,
        handle_followup_opt_out_fn=_true,
        capture_followup_answer_fn=_noop,
        schedule_followups_fn=_noop,
        cancel_pending_smart_reply_fn=_noop,
        resolve_or_create_contact_fn=_noop,
        update_contact_phone_fn=_noop,
        update_contact_avito_login_fn=_noop,
        link_lead_contact_fn=_noop,
        insert_message_in_fn=_noop,
        maybe_amocrm_inbound_fn=_noop,
        match_behavior_trigger_fn=lambda *_args, **_kwargs: None,
        mark_handoff_silence_fn=_noop,
        send_telegram_to_phone_fn=_noop,
        send_telegram_to_username_fn=_noop,
        enqueue_avito_auto_reply_fn=_noop,
        is_handoff_silenced_fn=_noop,
        avito_smart_reply_enabled_fn=lambda _tenant: True,
        smart_reply_enabled_fn=lambda _tenant: True,
        try_handle_smart_reply_with_delay_fn=_noop,
        produce_and_enqueue_smart_reply_fn=_noop,
        inc_db_error_fn=lambda _label: None,
    )
    deps.update(overrides)
    return avito_incoming_runtime.AvitoIncomingDeps(**deps)


@pytest.mark.anyio
async def test_handle_avito_incoming_skips_invalid_tenant() -> None:
    logs: list[str] = []

    async def _unexpected(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("should not be called")

    await avito_incoming_runtime.handle_avito_incoming(
        {"tenant": "bad", "chat_id": "chat-1", "text": "hello"},
        deps=avito_incoming_runtime.AvitoIncomingDeps(
            avito_chat_cache={},
            redis_client=_FakeRedis(),
            phone_tg_ttl_seconds=60,
            auto_reply_ttl_seconds=60,
            testing_mode=False,
            log_fn=logs.append,
            coerce_int_fn=lambda value: int(value) if str(value).isdigit() else 0,
            extract_ru_phone_fn=lambda _text: "",
            extract_tg_username_fn=lambda _text: "",
            avito_phone_tg_template_fn=lambda _tenant: "",
            avito_auto_reply_text_fn=lambda _tenant: "",
            resolve_avito_user_name_fn=_unexpected,
            get_or_create_by_peer_fn=_unexpected,
            lead_exists_fn=_unexpected,
            upsert_lead_fn=_unexpected,
            handle_followup_opt_out_fn=_unexpected,
            capture_followup_answer_fn=_unexpected,
            schedule_followups_fn=_unexpected,
            cancel_pending_smart_reply_fn=_unexpected,
            resolve_or_create_contact_fn=_unexpected,
            update_contact_phone_fn=_unexpected,
            update_contact_avito_login_fn=_unexpected,
            link_lead_contact_fn=_unexpected,
            insert_message_in_fn=_unexpected,
            maybe_amocrm_inbound_fn=_unexpected,
            match_behavior_trigger_fn=lambda *_args, **_kwargs: None,
            mark_handoff_silence_fn=_unexpected,
            send_telegram_to_phone_fn=_unexpected,
            send_telegram_to_username_fn=_unexpected,
            enqueue_avito_auto_reply_fn=_unexpected,
            is_handoff_silenced_fn=_unexpected,
            avito_smart_reply_enabled_fn=lambda _tenant: True,
            smart_reply_enabled_fn=lambda _tenant: True,
            try_handle_smart_reply_with_delay_fn=_unexpected,
            produce_and_enqueue_smart_reply_fn=_unexpected,
            inc_db_error_fn=lambda _label: None,
        ),
    )

    assert logs == ["event=skip_invalid_tenant channel=avito tenant_raw=bad"]


@pytest.mark.anyio
async def test_handle_avito_incoming_resolves_item_city_after_lead(monkeypatch) -> None:
    logs: list[str] = []
    calls: list[dict[str, Any]] = []

    async def _resolve_city(**kwargs: Any) -> None:
        calls.append(kwargs)

    async def _mark_seen(_account_id: int) -> None:
        return None

    monkeypatch.setattr(avito_incoming_runtime.avito_integration, "get_integration", lambda _tenant: {"access_token": "x"})
    from libs.core.repo import avito_accounts

    monkeypatch.setattr(avito_accounts, "mark_webhook_seen", _mark_seen)

    await avito_incoming_runtime.handle_avito_incoming(
        {
            "tenant": "101",
            "chat_id": "chat-1",
            "text": "hello",
            "account_id": "222",
            "item_id": "749",
            "message": {"item_url": "https://www.avito.ru/ufa/item"},
        },
        deps=_deps(logs, resolve_avito_item_city_fn=_resolve_city),
    )

    assert calls == [
        {
            "tenant_id": 101,
            "account_id": 222,
            "item_id": 749,
            "lead_id": 501,
            "url_hint": "https://www.avito.ru/ufa/item",
            "address_hint": None,
        }
    ]


def test_smart_reply_context_includes_item_id() -> None:
    state = avito_incoming_runtime.AvitoIncomingState(
        tenant_id=101,
        tenant_raw="101",
        chat_id="chat-1",
        message_id="msg-1",
        text="hello",
        attachments=[],
        has_photo=False,
        auto_reply_text="",
        account_id=222,
        item_id=749,
        user_id=333,
        login="Игорь",
        phone_value="",
        tg_username="",
        bridge_template="",
        lead_id=501,
        contact_id=0,
    )

    context = avito_incoming_runtime._smart_reply_context(state)

    assert context["account_id"] == 222
    assert context["item_id"] == 749


@pytest.mark.anyio
async def test_handle_avito_incoming_item_city_error_does_not_block(monkeypatch) -> None:
    logs: list[str] = []
    followup_calls: list[tuple[int, int, str]] = []

    async def _resolve_city(**_kwargs: Any) -> None:
        raise RuntimeError("boom")

    async def _followup_optout(tenant_id: int, lead_id: int, text: str) -> bool:
        followup_calls.append((tenant_id, lead_id, text))
        return True

    async def _mark_seen(_account_id: int) -> None:
        return None

    monkeypatch.setattr(avito_incoming_runtime.avito_integration, "get_integration", lambda _tenant: {"access_token": "x"})
    from libs.core.repo import avito_accounts

    monkeypatch.setattr(avito_accounts, "mark_webhook_seen", _mark_seen)

    await avito_incoming_runtime.handle_avito_incoming(
        {"tenant": "101", "chat_id": "chat-1", "text": "hello", "account_id": "222", "item_id": "749"},
        deps=_deps(
            logs,
            resolve_avito_item_city_fn=_resolve_city,
            handle_followup_opt_out_fn=_followup_optout,
        ),
    )

    assert followup_calls == [(101, 501, "hello")]
    assert any("event=avito_item_city_resolve_failed" in line for line in logs)


@pytest.mark.anyio
async def test_handle_avito_incoming_dedups_by_account_chat_message_id(monkeypatch) -> None:
    logs: list[str] = []
    redis = _MemoryRedis()
    inserted: list[dict[str, Any]] = []

    async def _insert_message_in(*args: Any, **kwargs: Any) -> int:
        inserted.append({"args": args, "kwargs": kwargs})
        return 100 + len(inserted)

    async def _false(*args: Any, **kwargs: Any) -> bool:
        return False

    async def _mark_seen(_account_id: int) -> None:
        return None

    monkeypatch.setattr(avito_incoming_runtime.avito_integration, "get_integration", lambda _tenant: {"access_token": "x"})
    from libs.core.repo import avito_accounts

    monkeypatch.setattr(avito_accounts, "mark_webhook_seen", _mark_seen)

    deps = _deps(
        logs,
        redis_client=redis,
        handle_followup_opt_out_fn=_false,
        is_handoff_silenced_fn=_false,
        try_handle_smart_reply_with_delay_fn=_false,
        insert_message_in_fn=_insert_message_in,
    )
    event = {
        "tenant": "101",
        "chat_id": "chat-1",
        "text": "hello",
        "account_id": "222",
        "message_id": "msg-1",
    }

    await avito_incoming_runtime.handle_avito_incoming(event, deps=deps)
    await avito_incoming_runtime.handle_avito_incoming(event, deps=deps)

    assert len(inserted) == 1
    assert inserted[0]["kwargs"]["provider_msg_id"] == "msg-1"
    assert inserted[0]["kwargs"]["source"] == "incoming"
    assert any("event=avito_incoming_dedup" in line for line in logs)


@pytest.mark.anyio
async def test_handle_avito_incoming_without_login_calls_identity_resolver(monkeypatch) -> None:
    logs: list[str] = []
    calls: list[dict[str, Any]] = []
    smart_contexts: list[dict[str, Any]] = []

    async def _false(*args: Any, **kwargs: Any) -> bool:
        return False

    async def _contact(**_kwargs: Any) -> int:
        return 701

    async def _identity(**kwargs: Any) -> Any:
        calls.append(kwargs)

        class Result:
            name = "Наталья"

        return Result()

    async def _smart(**kwargs: Any) -> bool:
        smart_contexts.append(dict(kwargs.get("context") or {}))
        return True

    async def _mark_seen(_account_id: int) -> None:
        return None

    monkeypatch.setattr(avito_incoming_runtime.avito_integration, "get_integration", lambda _tenant: {"access_token": "x"})
    from libs.core.repo import avito_accounts

    monkeypatch.setattr(avito_accounts, "mark_webhook_seen", _mark_seen)

    await avito_incoming_runtime.handle_avito_incoming(
        {
            "tenant": "101",
            "chat_id": "chat-1",
            "text": "hello",
            "account_id": "222",
            "avito_user_id": "333",
        },
        deps=_deps(
            logs,
            handle_followup_opt_out_fn=_false,
            is_handoff_silenced_fn=_false,
            try_handle_smart_reply_with_delay_fn=_false,
            resolve_or_create_contact_fn=_contact,
            resolve_avito_contact_identity_fn=_identity,
            produce_and_enqueue_smart_reply_fn=_smart,
        ),
    )

    assert calls == [
        {
            "tenant_id": 101,
            "lead_id": 501,
            "contact_id": 701,
            "account_id": 222,
            "chat_id": "chat-1",
            "author_id": 333,
            "current_login": None,
            "current_contact": None,
        }
    ]
    assert smart_contexts[-1]["avito_login"] == "Наталья"


@pytest.mark.anyio
async def test_handle_avito_incoming_identity_error_does_not_block(monkeypatch) -> None:
    logs: list[str] = []
    smart_calls: list[int] = []

    async def _false(*args: Any, **kwargs: Any) -> bool:
        return False

    async def _identity(**_kwargs: Any) -> Any:
        raise RuntimeError("boom")

    async def _smart(**_kwargs: Any) -> bool:
        smart_calls.append(1)
        return True

    async def _mark_seen(_account_id: int) -> None:
        return None

    monkeypatch.setattr(avito_incoming_runtime.avito_integration, "get_integration", lambda _tenant: {"access_token": "x"})
    from libs.core.repo import avito_accounts

    monkeypatch.setattr(avito_accounts, "mark_webhook_seen", _mark_seen)

    await avito_incoming_runtime.handle_avito_incoming(
        {"tenant": "101", "chat_id": "chat-1", "text": "hello", "account_id": "222"},
        deps=_deps(
            logs,
            handle_followup_opt_out_fn=_false,
            is_handoff_silenced_fn=_false,
            try_handle_smart_reply_with_delay_fn=_false,
            resolve_avito_contact_identity_fn=_identity,
            produce_and_enqueue_smart_reply_fn=_smart,
        ),
    )

    assert smart_calls == [1]
    assert any("event=avito_contact_identity_failed" in line for line in logs)
