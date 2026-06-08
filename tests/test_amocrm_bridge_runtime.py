from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.worker.services import amocrm_bridge_runtime


pytestmark = pytest.mark.unit


def _deps(**overrides):
    async def _sleep(_seconds: float) -> None:
        return None

    async def _noop_async(*_args, **_kwargs):
        return None

    deps = dict(
        sleep_fn=_sleep,
        normalize_e164_digits_fn=lambda value: value,
        read_tenant_config_fn=lambda _tenant: {},
        amocrm_service_module=SimpleNamespace(
            AMOCRM_PROVIDER="amocrm",
            get_amocrm_cfg=lambda _cfg: None,
            resolve_api_base_url=_noop_async,
            resolve_oauth_cfg=lambda _cfg, _tenant: {},
        ),
        amocrm_integration_module=SimpleNamespace(AmoCRMClient=object),
        crm_links_repo=SimpleNamespace(
            get_link=_noop_async,
            update_provider_lead_id=_noop_async,
            update_provider_contact_id=_noop_async,
        ),
        crm_chat_links_repo=SimpleNamespace(get_link=_noop_async, upsert_link=_noop_async),
        crm_outbox_repo=SimpleNamespace(
            has_recent_event=_noop_async,
            enqueue=_noop_async,
            cancel_pending_events=_noop_async,
        ),
        amocrm_chat_service_module=SimpleNamespace(
            AMOCRM_CHAT_PROVIDER="amocrm_chat",
            _canonical_chat_identity=_noop_async,
        ),
    )
    deps.update(overrides)
    return amocrm_bridge_runtime.AmoCrmBridgeDeps(**deps)


@pytest.mark.anyio
async def test_enqueue_amocrm_cleanup_event_skips_delete_ops() -> None:
    enqueued: list[tuple] = []

    async def _enqueue(*args, **kwargs):
        enqueued.append((args, kwargs))

    deps = _deps(
        crm_outbox_repo=SimpleNamespace(
            has_recent_event=lambda *_a, **_k: False,
            enqueue=_enqueue,
            cancel_pending_events=lambda *_a, **_k: None,
        )
    )

    await amocrm_bridge_runtime.enqueue_amocrm_cleanup_event(
        1,
        2,
        event_type="delete_lead",
        payload={"amo_lead_id": 7},
        deps=deps,
    )

    assert enqueued == []


@pytest.mark.anyio
async def test_reconcile_avito_bridge_updates_links_until_stable() -> None:
    origin_links = [
        {"provider_lead_id": 10, "provider_contact_id": 20},
        {"provider_lead_id": 99, "provider_contact_id": 77},
        {"provider_lead_id": 99, "provider_contact_id": 77},
        {"provider_lead_id": 99, "provider_contact_id": 77},
    ]
    update_lead_calls: list[tuple[int, int]] = []
    update_contact_calls: list[tuple[int, int]] = []
    upserts: list[dict[str, object]] = []

    async def fake_get_link(tenant_id: int, lead_id: int, provider: str):
        if lead_id == 11:
            return origin_links.pop(0) if origin_links else {"provider_lead_id": 99, "provider_contact_id": 77}
        if provider == "amocrm_chat":
            return {}
        return {}

    async def fake_update_provider_lead_id(tenant_id: int, lead_id: int, provider: str, provider_lead_id: int):
        update_lead_calls.append((lead_id, provider_lead_id))

    async def fake_update_provider_contact_id(tenant_id: int, lead_id: int, provider: str, provider_contact_id: int):
        update_contact_calls.append((lead_id, provider_contact_id))

    async def fake_upsert_link(*_args, **kwargs):
        upserts.append(dict(kwargs))

    async def fake_canonical_chat_identity(_tenant_id: int, *, provider_lead_id: int, fallback_chat_id: str, fallback_conversation_id: str):
        return f"canon:{provider_lead_id}", fallback_conversation_id

    deps = _deps(
        crm_links_repo=SimpleNamespace(
            get_link=fake_get_link,
            update_provider_lead_id=fake_update_provider_lead_id,
            update_provider_contact_id=fake_update_provider_contact_id,
        ),
        crm_chat_links_repo=SimpleNamespace(get_link=fake_get_link, upsert_link=fake_upsert_link),
        crm_outbox_repo=SimpleNamespace(
            has_recent_event=lambda *_a, **_k: False,
            enqueue=lambda *_a, **_k: None,
            cancel_pending_events=lambda *_a, **_k: None,
        ),
        amocrm_chat_service_module=SimpleNamespace(
            AMOCRM_CHAT_PROVIDER="amocrm_chat",
            _canonical_chat_identity=fake_canonical_chat_identity,
        ),
    )

    await amocrm_bridge_runtime.reconcile_avito_bridge_amocrm_links(
        tenant_id=3,
        origin_lead_id=11,
        tg_lead_id=12,
        keep_provider_lead_id=99,
        keep_provider_contact_id=77,
        deps=deps,
    )

    assert (11, 99) in update_lead_calls
    assert (11, 77) in update_contact_calls
    assert (12, 99) in update_lead_calls
    assert (12, 77) in update_contact_calls
    assert any(item.get("external_chat_id") == "canon:99" for item in upserts)
