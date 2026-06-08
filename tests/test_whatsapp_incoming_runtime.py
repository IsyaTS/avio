from __future__ import annotations

from typing import Any

import pytest

from apps.worker.services import whatsapp_incoming_runtime


pytestmark = pytest.mark.unit


@pytest.mark.anyio
async def test_handle_whatsapp_incoming_skips_auto_reply_handled() -> None:
    logs: list[str] = []

    async def _unexpected(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("should not be called")

    await whatsapp_incoming_runtime.handle_whatsapp_incoming(
        {"tenant": 3, "auto_reply_handled": True},
        deps=whatsapp_incoming_runtime.WhatsAppIncomingDeps(
            default_tenant_id=1,
            log_fn=logs.append,
            coerce_int_fn=lambda value: int(value) if value is not None else None,
            is_whatsapp_group_fn=lambda _value: False,
            digits_fn=lambda value: str(value or ""),
            get_or_create_by_peer_fn=_unexpected,
            handle_followup_opt_out_fn=_unexpected,
            capture_followup_answer_fn=_unexpected,
            schedule_followups_fn=_unexpected,
            cancel_pending_smart_reply_fn=_unexpected,
            resolve_or_create_contact_fn=_unexpected,
            link_lead_contact_fn=_unexpected,
            insert_message_in_fn=_unexpected,
            maybe_amocrm_inbound_fn=_unexpected,
            match_behavior_trigger_fn=lambda *_args, **_kwargs: None,
            mark_handoff_silence_fn=_unexpected,
            is_handoff_silenced_fn=_unexpected,
            smart_reply_enabled_fn=lambda _tenant: True,
            try_handle_smart_reply_with_delay_fn=_unexpected,
            produce_and_enqueue_smart_reply_fn=_unexpected,
            inc_db_error_fn=lambda _label: None,
        ),
    )

    assert logs == ["event=incoming_skip_auto_handled channel=whatsapp tenant=3"]
