from __future__ import annotations

import pytest

from libs.core.services import outbox_payloads


pytestmark = pytest.mark.unit


def test_base_channel_reply_payload_builds_avito_identity_fields() -> None:
    payload = outbox_payloads.base_channel_reply_payload(
        tenant_id=101,
        lead_id=202,
        channel="avito",
        context={
            "chat_id": " chat-1 ",
            "account_id": "123456",
            "message_id": "msg-1",
            "avito_user_id": "777",
            "avito_login": " buyer ",
        },
        attachments=[{"type": "image"}],
    )

    assert payload == {
        "lead_id": 202,
        "tenant": 101,
        "tenant_id": 101,
        "provider": "avito",
        "ch": "avito",
        "channel": "avito",
        "attachments": [{"type": "image"}],
        "chat_id": "chat-1",
        "peer": "chat-1",
        "peer_id": "chat-1",
        "account_id": 123456,
        "message_id": "msg-1",
        "avito_user_id": 777,
        "avito_login": "buyer",
    }


def test_base_channel_reply_payload_rejects_avito_without_chat_id() -> None:
    payload = outbox_payloads.base_channel_reply_payload(
        tenant_id=101,
        lead_id=202,
        channel="avito",
        context={"account_id": 123456},
    )

    assert payload is None


def test_resolve_outbox_channel_prefers_explicit_channel() -> None:
    assert (
        outbox_payloads.resolve_outbox_channel(
            {"provider": " Telegram ", "telegram_user_id": 123}
        )
        == "telegram"
    )
    assert outbox_payloads.resolve_outbox_channel({"ch": "Avito"}) == "avito"


def test_resolve_outbox_channel_falls_back_from_identity_fields() -> None:
    assert outbox_payloads.resolve_outbox_channel({"max_user_id": 123}) == "max"
    assert outbox_payloads.resolve_outbox_channel({"telegram_user_id": 123}) == "telegram"
    assert outbox_payloads.resolve_outbox_channel({"peer_id": 123}) == "telegram"
    assert outbox_payloads.resolve_outbox_channel({}) == "whatsapp"


def test_build_send_context_normalizes_tenant_and_lead() -> None:
    context = outbox_payloads.build_send_context(
        {"channel": " Avito ", "tenant": "101", "lead_id": "202"},
        default_tenant_id=1,
    )

    assert context.channel == "avito"
    assert context.tenant_id == 101
    assert context.lead_id == 202


def test_build_send_context_uses_default_tenant_for_bad_tenant() -> None:
    context = outbox_payloads.build_send_context(
        {"tenant_id": "bad", "tenant": "101"},
        default_tenant_id=9,
    )

    assert context.tenant_id == 9
    assert context.lead_id == 0


def test_collect_outbox_attachments_combines_primary_and_list() -> None:
    context = outbox_payloads.collect_outbox_attachments(
        {
            "attachment": {"filename": "one.pdf"},
            "attachments": [{"filename": "two.pdf"}, "bad"],
        }
    )

    assert context.primary == {"filename": "one.pdf"}
    assert context.all_items == [{"filename": "two.pdf"}, {"filename": "one.pdf"}]


def test_build_outbox_item_plan_normalizes_send_inputs() -> None:
    plan = outbox_payloads.build_outbox_item_plan(
        {
            "provider": " Telegram ",
            "tenant": "101",
            "lead_id": "202",
            "text": " hello ",
            "to": "+7 (999) 111-22-33",
            "peer": "303",
            "username": " buyer ",
            "tg_slot": "2",
            "attachment": {"filename": "one.pdf"},
            "attachments": [{"filename": "two.pdf"}],
            "reply_to": "msg-1",
            "account_id": "404",
            "max_user_id": "505",
            "chat_id": "chat-1",
        },
        default_tenant_id=1,
        normalize_tg_slot_fn=lambda value: int(value),
    )

    assert plan.channel == "telegram"
    assert plan.tenant_id == 101
    assert plan.lead_id == 202
    assert plan.text == "hello"
    assert plan.phone == "79991112233"
    assert plan.peer_value == "303"
    assert plan.peer_raw == "303"
    assert plan.raw_telegram == "303"
    assert plan.telegram_user_id == 303
    assert plan.item_tg_slot == 2
    assert plan.attachment == {"filename": "one.pdf"}
    assert plan.attachments == [{"filename": "two.pdf"}, {"filename": "one.pdf"}]
    assert plan.reply_to == "msg-1"
    assert plan.avito_account_id == 404
    assert plan.avito_chat_id_hint == "chat-1"
    assert plan.max_user_id == 505
    assert plan.max_chat_id_hint == "chat-1"


def test_is_manager_message_detects_origin_and_flags() -> None:
    assert outbox_payloads.is_manager_message({"origin": " Dialogs.UI "}) is True
    assert outbox_payloads.is_manager_message({"manager": "yes"}) is True
    assert outbox_payloads.is_manager_message({"manager": "false"}) is False
    assert outbox_payloads.is_manager_message({"meta": {"manager": "on"}}) is True
    assert outbox_payloads.is_manager_message({"meta": {"manager": ""}}) is False


def test_is_followup_message_detects_origin_and_meta_flag() -> None:
    assert outbox_payloads.is_followup_message({"origin": " followup "}) is True
    assert outbox_payloads.is_followup_message({"origin": "app.send"}) is False
    assert outbox_payloads.is_followup_message({"meta": {"followup": "true"}}) is True
    assert outbox_payloads.is_followup_message({"meta": {"followup": "0"}}) is False


def test_build_write_result_context_extracts_identity_and_text() -> None:
    context = outbox_payloads.build_write_result_context(
        {
            "provider": " Telegram ",
            "tenant": "101",
            "lead_id": "202",
            "text": " hello ",
            "telegram_user_id": "303",
            "username": "buyer",
            "_message_db_id": "404",
        },
        default_tenant_id=1,
    )

    assert context.channel == "telegram"
    assert context.tenant_id == 101
    assert context.lead_id == 202
    assert context.text == "hello"
    assert context.telegram_user_id == 303
    assert context.peer_value == "303"
    assert context.username == "buyer"
    assert context.stored_message_id == 404


def test_build_write_result_context_uses_attachment_text_and_resolved_lead_override() -> None:
    context = outbox_payloads.build_write_result_context(
        {
            "tenant_id": "bad",
            "tenant": "101",
            "lead_id": "202",
            "attachment": {"filename": "file.pdf"},
            "peer": "505",
            "_message_db_id": "0",
            "_resolved_lead_id": 303,
        },
        default_tenant_id=9,
    )

    assert context.tenant_id == 9
    assert context.lead_id == 303
    assert context.text == "[attachment] file.pdf"
    assert context.telegram_user_id == 505
    assert context.peer_value == "505"
    assert context.stored_message_id is None


def test_build_status_echo_payload_uses_explicit_channel() -> None:
    payload = outbox_payloads.build_status_echo_payload(
        lead_id=202,
        reply_text="hello",
        status="sent",
        version="v1",
        item={"ch": "telegram", "provider": "whatsapp"},
    )

    assert payload == {
        "lead_id": 202,
        "reply": "hello",
        "status": "sent",
        "version": "v1",
        "ch": "telegram",
    }


def test_build_status_echo_payload_falls_back_to_provider_and_whatsapp() -> None:
    provider_payload = outbox_payloads.build_status_echo_payload(
        lead_id=202,
        reply_text="hello",
        status="sent",
        version="v1",
        item={"provider": "avito"},
    )
    default_payload = outbox_payloads.build_status_echo_payload(
        lead_id=202,
        reply_text="hello",
        status="sent",
        version="v1",
        item={},
    )

    assert provider_payload["ch"] == "avito"
    assert default_payload["ch"] == "whatsapp"


def test_build_send_outcome_maps_status_codes() -> None:
    assert outbox_payloads.build_send_outcome(200) == outbox_payloads.SendOutcome("sent", "ok")
    assert outbox_payloads.build_send_outcome(403) == outbox_payloads.SendOutcome(
        "unauthorized",
        "status_403",
    )
    assert outbox_payloads.build_send_outcome(422) == outbox_payloads.SendOutcome(
        "skipped",
        "validation",
    )
    assert outbox_payloads.build_send_outcome(0) == outbox_payloads.SendOutcome(
        "skipped",
        "network",
    )
    assert outbox_payloads.build_send_outcome(500) == outbox_payloads.SendOutcome(
        "skipped",
        "status_500",
    )


def test_build_telegram_chat_candidates_and_first_positive_candidate() -> None:
    candidates = outbox_payloads.build_telegram_chat_candidates(
        primary_telegram_user_id=None,
        db_lookup_result=202,
        from_candidate=0,
        peer_value="303",
    )

    assert candidates == [202, 303]
    assert outbox_payloads.first_positive_candidate(candidates) == 202
    assert outbox_payloads.first_positive_candidate([]) is None


def test_resolve_telegram_peer_id_prefers_peer_value_then_raw() -> None:
    assert outbox_payloads.resolve_telegram_peer_id(peer_value="303", peer_raw="404") == 303
    assert outbox_payloads.resolve_telegram_peer_id(peer_value="bad", peer_raw="404") == 404
    assert outbox_payloads.resolve_telegram_peer_id(peer_value=None, peer_raw="bad") is None


def test_plan_echo_target_normalizes_hint_and_marks_peer_lookup_need() -> None:
    with_hint = outbox_payloads.plan_echo_target(chat_hint=" chat-1 ", lead_id=202)
    missing_with_lead = outbox_payloads.plan_echo_target(chat_hint="", lead_id=202)
    missing_without_lead = outbox_payloads.plan_echo_target(chat_hint=None, lead_id=0)

    assert with_hint == outbox_payloads.EchoTargetPlan(
        chat_hint="chat-1",
        needs_peer_lookup=False,
    )
    assert missing_with_lead == outbox_payloads.EchoTargetPlan(
        chat_hint=None,
        needs_peer_lookup=True,
    )
    assert missing_without_lead == outbox_payloads.EchoTargetPlan(
        chat_hint=None,
        needs_peer_lookup=False,
    )


def test_plan_baileys_recipient_prefers_jid_sources_in_order() -> None:
    def normalize(value: object) -> str:
        raw = str(value or "").strip()
        if raw.endswith("@s.whatsapp.net"):
            return raw
        if raw.isdigit():
            return f"{raw}@s.whatsapp.net"
        return ""

    assert outbox_payloads.plan_baileys_recipient(
        explicit_to_jid="111@s.whatsapp.net",
        cached_whatsapp_jid="222@s.whatsapp.net",
        raw_to="333",
        phone="444",
        normalize_jid_fn=normalize,
    ) == outbox_payloads.WhatsAppRecipientPlan(
        recipient="111@s.whatsapp.net",
        source="task",
        missing=False,
    )
    assert outbox_payloads.plan_baileys_recipient(
        explicit_to_jid=None,
        cached_whatsapp_jid=None,
        raw_to="333",
        phone="444",
        normalize_jid_fn=normalize,
    ) == outbox_payloads.WhatsAppRecipientPlan(
        recipient="333@s.whatsapp.net",
        source="raw_to",
        missing=False,
    )
    assert outbox_payloads.plan_baileys_recipient(
        explicit_to_jid=None,
        cached_whatsapp_jid=None,
        raw_to=None,
        phone="",
        normalize_jid_fn=normalize,
    ) == outbox_payloads.WhatsAppRecipientPlan(
        recipient="",
        source=None,
        missing=True,
    )


def test_whatsapp_identity_helpers_normalize_jid_peer_and_digits() -> None:
    assert outbox_payloads.digits_only("+7 (999) 111-22-33") == "79991112233"
    assert outbox_payloads.normalize_baileys_jid("79991112233") == "79991112233@s.whatsapp.net"
    assert outbox_payloads.normalize_baileys_jid("USER@S.WHATSAPP.NET") == "user@s.whatsapp.net"
    assert outbox_payloads.normalize_whatsapp_peer("  USER@S.WHATSAPP.NET ") == "user@s.whatsapp.net"
    assert outbox_payloads.normalize_whatsapp_peer(" ") is None


@pytest.mark.asyncio
async def test_resolve_cached_whatsapp_jid_reads_tenant_scoped_hash() -> None:
    class RedisFake:
        async def hget(self, key: str, field: str) -> str:
            assert key == "wa:jid:101"
            assert field == "202"
            return " 79991112233@s.whatsapp.net "

    assert (
        await outbox_payloads.resolve_cached_whatsapp_jid(RedisFake(), 101, 202)
        == "79991112233@s.whatsapp.net"
    )


def test_plan_waweb_auth_retry_increments_attempt_and_marks_dlq() -> None:
    first = outbox_payloads.plan_waweb_auth_retry({"lead_id": 1}, body="x" * 450)
    third = outbox_payloads.plan_waweb_auth_retry(
        {"lead_id": 1, "_waweb_auth_retry": 2},
        body="unauthorized",
    )

    assert first.attempt == 1
    assert first.payload["_waweb_auth_retry"] == 1
    assert first.should_dlq is False
    assert first.body_hint.endswith("…")
    assert len(first.body_hint) == 401
    assert third.attempt == 3
    assert third.should_dlq is True


@pytest.mark.asyncio
async def test_max_bot_echo_cache_roundtrip_and_ignores_empty_inputs() -> None:
    class RedisFake:
        def __init__(self) -> None:
            self.values: dict[str, str] = {}
            self.ttl: int | None = None

        async def set(self, key: str, value: str, ex: int) -> None:
            self.values[key] = value
            self.ttl = ex

        async def get(self, key: str) -> str | None:
            return self.values.get(key)

    redis = RedisFake()
    assert outbox_payloads.max_bot_echo_key(101, "max_personal", "chat-1") == (
        "max_personal:bot_echo:101:chat-1"
    )
    assert await outbox_payloads.cache_max_bot_echo(
        redis,
        tenant_id=101,
        channel="max_personal",
        chat_key="chat-1",
        text=" Привет! ",
        ttl_seconds=120,
    )
    assert redis.ttl == 120
    assert await outbox_payloads.is_recent_max_bot_echo(
        redis,
        tenant_id=101,
        channel="max_personal",
        chat_key="chat-1",
        text=" Привет! ",
    )
    assert not await outbox_payloads.cache_max_bot_echo(
        redis,
        tenant_id=101,
        channel="max",
        chat_key="",
        text="привет",
        ttl_seconds=120,
    )


@pytest.mark.asyncio
async def test_prepare_whatsapp_attachments_prepares_primary_and_list_items() -> None:
    async def prepare(blob):
        return {**dict(blob), "prepared": True}

    result = await outbox_payloads.prepare_whatsapp_attachments(
        primary={"filename": "one.pdf"},
        attachments=[{"filename": "two.pdf"}],
        prepare_attachment_fn=prepare,
    )

    assert result.primary == {"filename": "one.pdf", "prepared": True}
    assert result.all_items == [{"filename": "two.pdf", "prepared": True}]


def test_build_outbound_attachment_snapshot_preserves_existing_order_and_primary_append() -> None:
    snapshot = outbox_payloads.build_outbound_attachment_snapshot(
        primary={"filename": "one.pdf"},
        attachments=[{"filename": "two.pdf"}],
    )

    assert snapshot == [{"filename": "two.pdf"}, {"filename": "one.pdf"}]


@pytest.mark.asyncio
async def test_run_max_send_with_echo_caches_before_and_after_success() -> None:
    calls: list[tuple[str, object]] = []

    async def send(chat_hint: str | None) -> tuple[int, str]:
        calls.append(("send", chat_hint))
        return 200, "ok"

    async def get_peer(lead_id: int, channel: str) -> str:
        calls.append(("peer", (lead_id, channel)))
        return "resolved-chat"

    async def cache(tenant_id: int, channel: str, chat_hint: str | None, text: str) -> None:
        calls.append(("cache", (tenant_id, channel, chat_hint, text)))

    result = await outbox_payloads.run_max_send_with_echo(
        tenant_id=101,
        lead_id=202,
        channel="max_personal",
        text="hello",
        chat_hint=None,
        manager_message=False,
        send_fn=send,
        get_lead_peer_fn=get_peer,
        cache_echo_fn=cache,
    )

    assert result == outbox_payloads.ChannelSendResult(status_code=200, body="ok")
    assert calls == [
        ("peer", (202, "max_personal")),
        ("cache", (101, "max_personal", "resolved-chat", "hello")),
        ("send", None),
        ("peer", (202, "max_personal")),
        ("cache", (101, "max_personal", "resolved-chat", "hello")),
    ]


@pytest.mark.asyncio
async def test_run_max_send_with_echo_skips_echo_for_manager_message() -> None:
    calls: list[str] = []

    async def send(chat_hint: str | None) -> tuple[int, str]:
        calls.append(f"send:{chat_hint}")
        return 200, "ok"

    async def get_peer(lead_id: int, channel: str) -> str:
        calls.append("peer")
        return "resolved-chat"

    async def cache(tenant_id: int, channel: str, chat_hint: str | None, text: str) -> None:
        calls.append("cache")

    result = await outbox_payloads.run_max_send_with_echo(
        tenant_id=101,
        lead_id=202,
        channel="max",
        text="hello",
        chat_hint="chat-1",
        manager_message=True,
        send_fn=send,
        get_lead_peer_fn=get_peer,
        cache_echo_fn=cache,
    )

    assert result.status_code == 200
    assert calls == ["send:chat-1"]


def test_normalize_telegram_title_handles_legacy_titles_and_fallbacks() -> None:
    def norm(value: str | None) -> str | None:
        return f"@{value.lstrip('@')}" if value else None

    assert (
        outbox_payloads.normalize_telegram_title(
            title="tg: buyer_1",
            username=None,
            telegram_user_id=101,
            normalize_username_fn=norm,
        )
        == "@buyer_1"
    )
    assert (
        outbox_payloads.normalize_telegram_title(
            title="tg:id 101",
            username="buyer",
            telegram_user_id=101,
            normalize_username_fn=norm,
        )
        == "@buyer"
    )
    assert (
        outbox_payloads.normalize_telegram_title(
            title=None,
            username=None,
            telegram_user_id=101,
            normalize_username_fn=norm,
        )
        == "tg:id 101"
    )


def test_build_echo_cache_payload_handles_text_and_attachment_echoes() -> None:
    assert outbox_payloads.build_echo_cache_payload(
        normalized_text="hello",
        has_attachments=True,
        timestamp=123,
    ) == {"text": "hello", "extra": ["hello", "__image__"], "ts": 123}
    assert outbox_payloads.build_echo_cache_payload(
        normalized_text="",
        has_attachments=True,
        timestamp=123,
    ) == {"text": "__image__", "extra": ["__image__"], "ts": 123}
    assert (
        outbox_payloads.build_echo_cache_payload(
            normalized_text="",
            has_attachments=False,
            timestamp=123,
        )
        is None
    )


@pytest.mark.asyncio
async def test_run_avito_send_with_echo_caches_before_and_after_success() -> None:
    calls: list[tuple[str, object]] = []

    async def send(chat_hint: str | None) -> tuple[int, str]:
        calls.append(("send", chat_hint))
        return 200, "ok"

    async def get_peer(lead_id: int, channel: str) -> str:
        calls.append(("peer", (lead_id, channel)))
        return "chat-resolved"

    async def cache(chat_key: str, payload: dict[str, object], phase: str) -> None:
        calls.append(("cache", (chat_key, payload["text"], phase)))

    result = await outbox_payloads.run_avito_send_with_echo(
        lead_id=202,
        text="hello",
        chat_hint=None,
        has_attachments=True,
        manager_message=False,
        send_fn=send,
        get_lead_peer_fn=get_peer,
        cache_echo_fn=cache,
    )

    assert result == outbox_payloads.ChannelSendResult(status_code=200, body="ok")
    assert calls == [
        ("peer", (202, "avito")),
        ("cache", ("chat-resolved", "hello", "pre")),
        ("send", None),
        ("peer", (202, "avito")),
        ("cache", ("chat-resolved", "hello", "post")),
    ]


@pytest.mark.asyncio
async def test_run_avito_send_with_echo_skips_manager_echo_and_uses_hint() -> None:
    calls: list[str] = []

    async def send(chat_hint: str | None) -> tuple[int, str]:
        calls.append(f"send:{chat_hint}")
        return 200, "ok"

    async def get_peer(lead_id: int, channel: str) -> str:
        calls.append("peer")
        return "chat-resolved"

    async def cache(chat_key: str, payload: dict[str, object], phase: str) -> None:
        calls.append("cache")

    result = await outbox_payloads.run_avito_send_with_echo(
        lead_id=202,
        text="hello",
        chat_hint=" chat-hint ",
        has_attachments=False,
        manager_message=True,
        send_fn=send,
        get_lead_peer_fn=get_peer,
        cache_echo_fn=cache,
    )

    assert result.status_code == 200
    assert calls == ["send:chat-hint"]


def test_build_lead_upsert_kwargs_includes_telegram_identity() -> None:
    context = outbox_payloads.build_write_result_context(
        {
            "provider": "telegram",
            "tenant": 101,
            "lead_id": 202,
            "telegram_user_id": "303",
            "username": "buyer",
        }
    )

    assert outbox_payloads.build_lead_upsert_kwargs(context) == {
        "channel": "telegram",
        "source_real_id": None,
        "tenant_id": 101,
        "telegram_username": "buyer",
        "peer_id": 303,
        "peer": "303",
        "contact": "buyer",
        "telegram_user_id": 303,
    }


def test_build_lead_upsert_kwargs_omits_missing_telegram_user_id() -> None:
    context = outbox_payloads.build_write_result_context(
        {"provider": "avito", "tenant": 101, "lead_id": 202, "peer": "chat-1"}
    )

    assert outbox_payloads.build_lead_upsert_kwargs(context) == {
        "channel": "avito",
        "source_real_id": None,
        "tenant_id": 101,
        "telegram_username": None,
        "peer_id": None,
        "peer": "chat-1",
        "contact": None,
    }


def test_build_outgoing_message_source_for_non_telegram_roles() -> None:
    assert outbox_payloads.build_outgoing_message_source(channel="avito") == "bot"
    assert (
        outbox_payloads.build_outgoing_message_source(channel="whatsapp", is_manager=True)
        == "manager"
    )
    assert (
        outbox_payloads.build_outgoing_message_source(channel="max", is_followup=True)
        == "followup"
    )


def test_build_outgoing_message_source_for_telegram_slots() -> None:
    assert (
        outbox_payloads.build_outgoing_message_source(channel="telegram", tg_slot=2)
        == "bot:tg_slot:2"
    )
    assert (
        outbox_payloads.build_outgoing_message_source(
            channel="telegram",
            tg_slot="bad",
            is_manager=True,
        )
        == "manager:tg_slot:1"
    )
    assert (
        outbox_payloads.build_outgoing_message_source(
            channel="telegram",
            tg_slot=9,
            is_followup=True,
        )
        == "followup:tg_slot:3"
    )


def test_build_insert_message_out_kwargs_for_bot() -> None:
    context = outbox_payloads.build_write_result_context(
        {
            "provider": "telegram",
            "tenant": 101,
            "lead_id": 202,
            "telegram_user_id": "303",
            "username": "buyer",
        }
    )

    assert outbox_payloads.build_insert_message_out_kwargs(
        context=context,
        status="sent",
        attachments=[{"type": "image"}],
        tg_slot=2,
    ) == {
        "status": "sent",
        "tenant_id": 101,
        "channel": "telegram",
        "telegram_user_id": 303,
        "telegram_username": "buyer",
        "is_bot": True,
        "attachments": [{"type": "image"}],
        "source": "bot:tg_slot:2",
    }


def test_build_insert_message_out_kwargs_for_manager_without_attachments() -> None:
    context = outbox_payloads.build_write_result_context(
        {"provider": "avito", "tenant": 101, "lead_id": 202}
    )

    assert outbox_payloads.build_insert_message_out_kwargs(
        context=context,
        status="sent",
        is_manager=True,
        attachments=[],
    ) == {
        "status": "sent",
        "tenant_id": 101,
        "channel": "avito",
        "telegram_user_id": None,
        "telegram_username": None,
        "is_bot": False,
        "attachments": None,
        "source": "manager",
    }


def test_plan_lead_availability_uses_stored_message_as_available() -> None:
    plan = outbox_payloads.plan_lead_availability(
        lead_id=202,
        resolved_lead_id=None,
        stored_message_id=404,
    )

    assert plan.lead_ref == 202
    assert plan.available is True
    assert plan.needs_exists_check is False
    assert plan.missing_reason is None


def test_plan_lead_availability_requests_exists_check_for_resolved_lead() -> None:
    plan = outbox_payloads.plan_lead_availability(
        lead_id=202,
        resolved_lead_id=303,
        stored_message_id=None,
    )

    assert plan.lead_ref == 303
    assert plan.available is False
    assert plan.needs_exists_check is True
    assert plan.exists_check_lead_id == 303


def test_plan_lead_availability_reports_missing_upsert() -> None:
    plan = outbox_payloads.plan_lead_availability(
        lead_id=202,
        resolved_lead_id=None,
        stored_message_id=None,
    )

    assert plan.lead_ref == 202
    assert plan.available is False
    assert plan.needs_exists_check is False
    assert plan.missing_reason == "lead_upsert_missing"


def test_build_learning_capture_context_for_manager_message() -> None:
    context = outbox_payloads.build_learning_capture_context(
        tenant_id=101,
        lead_ref=202,
        channel="telegram",
        is_manager=True,
        stored_message_id=303,
    )

    assert context is not None
    assert context.tenant_id == 101
    assert context.lead_id == 202
    assert context.channel == "telegram"
    assert context.source_event == "manager_outgoing"
    assert context.manager_message_id == 303


def test_build_learning_capture_context_skips_non_manager_or_missing_ids() -> None:
    assert (
        outbox_payloads.build_learning_capture_context(
            tenant_id=101,
            lead_ref=202,
            channel="telegram",
            is_manager=False,
            stored_message_id=303,
        )
        is None
    )
    assert (
        outbox_payloads.build_learning_capture_context(
            tenant_id=101,
            lead_ref=0,
            channel="telegram",
            is_manager=True,
            stored_message_id=303,
        )
        is None
    )
    assert (
        outbox_payloads.build_learning_capture_context(
            tenant_id=101,
            lead_ref=202,
            channel="telegram",
            is_manager=True,
            stored_message_id=None,
        )
        is None
    )


def test_avito_auto_reply_payload_adds_text_and_empty_attachments() -> None:
    payload = outbox_payloads.avito_auto_reply_payload(
        tenant_id=101,
        lead_id=202,
        chat_id="chat-1",
        account_id=123456,
        user_id=777,
        login="buyer",
        message_id="msg-1",
        text=" Автоответ ",
    )

    assert payload is not None
    assert payload["text"] == "Автоответ"
    assert payload["attachments"] == []
    assert payload["provider"] == "avito"
    assert payload["chat_id"] == "chat-1"
    assert payload["account_id"] == 123456
    assert payload["avito_user_id"] == 777
    assert payload["avito_login"] == "buyer"
