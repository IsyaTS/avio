from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping


AsyncFn = Callable[..., Awaitable[Any]]
LogFn = Callable[[str], None]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class AmoCrmBridgeDeps:
    sleep_fn: AsyncFn
    normalize_e164_digits_fn: SyncFn
    read_tenant_config_fn: SyncFn
    amocrm_service_module: Any
    amocrm_integration_module: Any
    crm_links_repo: Any
    crm_chat_links_repo: Any
    crm_outbox_repo: Any
    amocrm_chat_service_module: Any


@dataclass
class AmoCrmLiveLookup:
    tenant_id: int
    phone_value: str
    origin_lead_id: int | None
    deps: AmoCrmBridgeDeps
    client: Any


@dataclass
class AmoCrmReconcileContext:
    tenant_id: int
    origin_lead_id: int
    tg_lead_id: int
    keep_provider_lead_id: int
    keep_provider_contact_id: int | None
    deps: AmoCrmBridgeDeps


async def resolve_live_amocrm_target_by_phone(
    tenant_id: int,
    *,
    phone: str | None,
    origin_lead_id: int | None = None,
    deps: AmoCrmBridgeDeps,
) -> tuple[int | None, int | None]:
    phone_value = deps.normalize_e164_digits_fn(phone or "")
    cfg = deps.read_tenant_config_fn(int(tenant_id))
    amocrm_cfg = deps.amocrm_service_module.get_amocrm_cfg(cfg)
    if not phone_value or not amocrm_cfg or not bool(amocrm_cfg.get("enabled")):
        return None, None
    client = await _build_amocrm_live_client(int(tenant_id), amocrm_cfg, deps=deps)
    if client is None:
        return None, None
    lookup = AmoCrmLiveLookup(
        tenant_id=int(tenant_id),
        phone_value=phone_value,
        origin_lead_id=origin_lead_id,
        deps=deps,
        client=client,
    )
    origin_target = await _resolve_live_amocrm_origin_target(lookup)
    if origin_target != (None, None):
        return origin_target
    return await _resolve_live_amocrm_search_target(lookup)


async def _build_amocrm_live_client(
    tenant_id: int,
    amocrm_cfg: Mapping[str, Any],
    *,
    deps: AmoCrmBridgeDeps,
) -> Any | None:
    base_url = await deps.amocrm_service_module.resolve_api_base_url(amocrm_cfg, tenant_id)
    if not base_url:
        return None
    oauth_cfg = deps.amocrm_service_module.resolve_oauth_cfg(amocrm_cfg, tenant_id)
    return deps.amocrm_integration_module.AmoCRMClient(
        tenant_id=tenant_id,
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )


async def _amocrm_target_is_live(lookup: AmoCrmLiveLookup, contact_id: int | None, lead_id: int | None) -> bool:
    if not contact_id or not lead_id:
        return False
    try:
        await lookup.client.get_contact(int(contact_id))
        await lookup.client.get_lead(int(lead_id))
        return True
    except Exception:
        return False


async def _amocrm_contact_id_from_lead(lookup: AmoCrmLiveLookup, lead_id: int | None) -> int | None:
    if not lead_id:
        return None
    try:
        lead_payload = await lookup.client.get_lead(int(lead_id))
    except Exception:
        return None
    embedded = lead_payload.get("_embedded") if isinstance(lead_payload, Mapping) else None
    contacts = embedded.get("contacts") if isinstance(embedded, Mapping) else None
    if not isinstance(contacts, list):
        return None
    for item in contacts:
        contact_id = _mapping_int(item, "id")
        if contact_id and contact_id > 0:
            return contact_id
    return None


async def _resolve_live_amocrm_origin_target(
    lookup: AmoCrmLiveLookup,
) -> tuple[int | None, int | None]:
    if not lookup.origin_lead_id or int(lookup.origin_lead_id) <= 0:
        return None, None
    try:
        origin_link = await lookup.deps.crm_links_repo.get_link(
            lookup.tenant_id,
            int(lookup.origin_lead_id),
            lookup.deps.amocrm_service_module.AMOCRM_PROVIDER,
        )
    except Exception:
        return None, None
    existing_contact_id = _mapping_int(origin_link, "provider_contact_id")
    existing_lead_id = _mapping_int(origin_link, "provider_lead_id")
    if await _amocrm_target_is_live(lookup, existing_contact_id, existing_lead_id):
        return existing_contact_id, existing_lead_id
    if existing_lead_id:
        lead_contact_id = await _amocrm_contact_id_from_lead(lookup, existing_lead_id)
        if await _amocrm_target_is_live(lookup, lead_contact_id, existing_lead_id):
            return lead_contact_id, existing_lead_id
    return None, None


async def _resolve_live_amocrm_search_target(
    lookup: AmoCrmLiveLookup,
) -> tuple[int | None, int | None]:
    try:
        contacts = await lookup.client.search_contacts(lookup.phone_value)
    except Exception:
        return None, None
    candidates: list[tuple[int, int]] = []
    for item in contacts:
        contact_id = _mapping_int(item, "id")
        if not contact_id:
            continue
        try:
            full_contact = await lookup.client.get_contact(contact_id, with_leads=True)
        except Exception:
            continue
        candidates.extend(await _live_amocrm_contact_leads(lookup, contact_id, full_contact))
    if not candidates:
        return None, None
    candidates.sort(key=lambda pair: pair[1], reverse=True)
    return candidates[0]


async def _live_amocrm_contact_leads(
    lookup: AmoCrmLiveLookup,
    contact_id: int,
    full_contact: Mapping[str, Any],
) -> list[tuple[int, int]]:
    embedded = full_contact.get("_embedded") if isinstance(full_contact, Mapping) else None
    leads = embedded.get("leads") if isinstance(embedded, Mapping) else None
    if not isinstance(leads, list):
        return []
    candidates: list[tuple[int, int]] = []
    for lead_item in leads:
        lead_id = _mapping_int(lead_item, "id")
        if lead_id and await _amocrm_target_is_live(lookup, contact_id, lead_id):
            candidates.append((contact_id, lead_id))
    return candidates


def _mapping_int(item: Any, key: str) -> int | None:
    if not isinstance(item, Mapping) or item.get(key) is None:
        return None
    try:
        return int(item.get(key))
    except Exception:
        return None


async def wait_for_amocrm_link_ready(
    tenant_id: int,
    lead_id: int,
    *,
    timeout_seconds: float = 8.0,
    poll_seconds: float = 0.4,
    deps: AmoCrmBridgeDeps,
) -> Mapping[str, Any] | None:
    deadline = time.monotonic() + max(0.5, float(timeout_seconds))
    last_link: Mapping[str, Any] | None = None
    while True:
        try:
            link = await deps.crm_links_repo.get_link(
                int(tenant_id),
                int(lead_id),
                deps.amocrm_service_module.AMOCRM_PROVIDER,
            )
        except Exception:
            link = None
        if isinstance(link, Mapping):
            last_link = link
            if link.get("provider_lead_id") is not None or link.get("provider_contact_id") is not None:
                return link
        if time.monotonic() >= deadline:
            return last_link
        await deps.sleep_fn(max(0.1, float(poll_seconds)))


async def enqueue_amocrm_cleanup_event(
    tenant_id: int,
    lead_id: int,
    *,
    event_type: str,
    payload: Mapping[str, Any],
    deps: AmoCrmBridgeDeps,
) -> None:
    if str(event_type).strip().lower() in {"delete_lead", "delete_contact"}:
        return
    try:
        already = await deps.crm_outbox_repo.has_recent_event(
            int(tenant_id),
            deps.amocrm_service_module.AMOCRM_PROVIDER,
            int(lead_id),
            str(event_type),
            dict(payload),
            window_seconds=900,
        )
    except Exception:
        already = False
    if already:
        return
    await deps.crm_outbox_repo.enqueue(
        int(tenant_id),
        deps.amocrm_service_module.AMOCRM_PROVIDER,
        int(lead_id),
        str(event_type),
        dict(payload),
    )


async def reconcile_avito_bridge_amocrm_links(
    *,
    tenant_id: int,
    origin_lead_id: int,
    tg_lead_id: int,
    keep_provider_lead_id: int,
    keep_provider_contact_id: int | None,
    deps: AmoCrmBridgeDeps,
) -> None:
    ctx = AmoCrmReconcileContext(
        tenant_id=int(tenant_id),
        origin_lead_id=int(origin_lead_id),
        tg_lead_id=int(tg_lead_id),
        keep_provider_lead_id=int(keep_provider_lead_id),
        keep_provider_contact_id=(
            int(keep_provider_contact_id) if keep_provider_contact_id is not None else None
        ),
        deps=deps,
    )
    await _cancel_origin_create_lead(ctx)
    stable_hits = 0
    for _ in range(25):
        changed = await _reconcile_amocrm_bridge_once(ctx)
        stable_hits = stable_hits + 1 if not changed else 0
        if stable_hits >= 3:
            break
        await deps.sleep_fn(0.8)


async def _cancel_origin_create_lead(ctx: AmoCrmReconcileContext) -> None:
    try:
        await ctx.deps.crm_outbox_repo.cancel_pending_events(
            ctx.tenant_id,
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            ctx.origin_lead_id,
            "create_lead",
            reason="cancelled_by_avito_tg_merge",
        )
    except Exception:
        pass


async def _reconcile_amocrm_bridge_once(ctx: AmoCrmReconcileContext) -> bool:
    origin_link = await _get_bridge_provider_link(ctx, ctx.origin_lead_id)
    current_provider_lead = _mapping_int(origin_link, "provider_lead_id")
    current_provider_contact = _mapping_int(origin_link, "provider_contact_id")
    changed = await _reconcile_origin_provider_ids(
        ctx,
        current_provider_lead=current_provider_lead,
        current_provider_contact=current_provider_contact,
    )
    await _ensure_tg_provider_ids(ctx)
    await _upsert_reconciled_chat_links(ctx)
    return changed


async def _get_bridge_provider_link(
    ctx: AmoCrmReconcileContext,
    lead_id: int,
) -> Mapping[str, Any] | None:
    try:
        link = await ctx.deps.crm_links_repo.get_link(
            ctx.tenant_id,
            int(lead_id),
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
        )
    except Exception:
        return None
    return link if isinstance(link, Mapping) else None


async def _reconcile_origin_provider_ids(
    ctx: AmoCrmReconcileContext,
    *,
    current_provider_lead: int | None,
    current_provider_contact: int | None,
) -> bool:
    changed = False
    if current_provider_lead is not None and current_provider_lead != ctx.keep_provider_lead_id:
        await _cleanup_and_update_origin_provider_id(
            ctx,
            cleanup_event="delete_lead",
            cleanup_payload={"amo_lead_id": int(current_provider_lead)},
            update_fn=ctx.deps.crm_links_repo.update_provider_lead_id,
            keep_id=ctx.keep_provider_lead_id,
        )
        changed = True
    if (
        ctx.keep_provider_contact_id is not None
        and current_provider_contact is not None
        and current_provider_contact != ctx.keep_provider_contact_id
    ):
        await _cleanup_and_update_origin_provider_id(
            ctx,
            cleanup_event="delete_contact",
            cleanup_payload={"amo_contact_id": int(current_provider_contact)},
            update_fn=ctx.deps.crm_links_repo.update_provider_contact_id,
            keep_id=ctx.keep_provider_contact_id,
        )
        changed = True
    return changed


async def _cleanup_and_update_origin_provider_id(
    ctx: AmoCrmReconcileContext,
    *,
    cleanup_event: str,
    cleanup_payload: Mapping[str, Any],
    update_fn: AsyncFn,
    keep_id: int,
) -> None:
    try:
        await enqueue_amocrm_cleanup_event(
            ctx.tenant_id,
            ctx.tg_lead_id,
            event_type=cleanup_event,
            payload=cleanup_payload,
            deps=ctx.deps,
        )
    except Exception:
        pass
    try:
        await update_fn(
            ctx.tenant_id,
            ctx.origin_lead_id,
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            int(keep_id),
        )
    except Exception:
        pass


async def _ensure_tg_provider_ids(ctx: AmoCrmReconcileContext) -> None:
    try:
        await ctx.deps.crm_links_repo.update_provider_lead_id(
            ctx.tenant_id,
            ctx.tg_lead_id,
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            ctx.keep_provider_lead_id,
        )
    except Exception:
        pass
    if ctx.keep_provider_contact_id is not None:
        try:
            await ctx.deps.crm_links_repo.update_provider_contact_id(
                ctx.tenant_id,
                ctx.tg_lead_id,
                ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
                ctx.keep_provider_contact_id,
            )
        except Exception:
            pass


async def _upsert_reconciled_chat_links(ctx: AmoCrmReconcileContext) -> None:
    origin_chat_link = await _get_bridge_chat_link(ctx, ctx.origin_lead_id)
    tg_chat_link = await _get_bridge_chat_link(ctx, ctx.tg_lead_id)
    external_chat_id, external_conversation_id = await _reconciled_chat_identity(
        ctx,
        origin_chat_link,
        tg_chat_link,
    )
    await _upsert_bridge_chat_link(ctx, ctx.origin_lead_id, external_chat_id, external_conversation_id)
    await _upsert_bridge_chat_link(ctx, ctx.tg_lead_id, external_chat_id, external_conversation_id)


async def _get_bridge_chat_link(
    ctx: AmoCrmReconcileContext,
    lead_id: int,
) -> Mapping[str, Any] | None:
    try:
        link = await ctx.deps.crm_chat_links_repo.get_link(
            ctx.tenant_id,
            int(lead_id),
            ctx.deps.amocrm_chat_service_module.AMOCRM_CHAT_PROVIDER,
        )
    except Exception:
        return None
    return link if isinstance(link, Mapping) else None


async def _reconciled_chat_identity(
    ctx: AmoCrmReconcileContext,
    origin_chat_link: Mapping[str, Any] | None,
    tg_chat_link: Mapping[str, Any] | None,
) -> tuple[str, str]:
    external_chat_id = (
        str((origin_chat_link or {}).get("external_chat_id") or "").strip()
        or str((tg_chat_link or {}).get("external_chat_id") or "").strip()
        or f"avio:{ctx.tenant_id}:avito:{ctx.origin_lead_id}"
    )
    external_conversation_id = (
        str((origin_chat_link or {}).get("external_conversation_id") or "").strip()
        or str((tg_chat_link or {}).get("external_conversation_id") or "").strip()
        or external_chat_id
    )
    try:
        return await ctx.deps.amocrm_chat_service_module._canonical_chat_identity(
            ctx.tenant_id,
            provider_lead_id=ctx.keep_provider_lead_id,
            fallback_chat_id=external_chat_id,
            fallback_conversation_id=external_conversation_id,
        )
    except Exception:
        return external_chat_id, external_conversation_id


async def _upsert_bridge_chat_link(
    ctx: AmoCrmReconcileContext,
    lead_id: int,
    external_chat_id: str,
    external_conversation_id: str,
) -> None:
    try:
        await ctx.deps.crm_chat_links_repo.upsert_link(
            ctx.tenant_id,
            int(lead_id),
            ctx.deps.amocrm_chat_service_module.AMOCRM_CHAT_PROVIDER,
            external_chat_id=external_chat_id,
            external_conversation_id=external_conversation_id,
            external_contact_id=ctx.keep_provider_contact_id,
            external_lead_id=ctx.keep_provider_lead_id,
        )
    except Exception:
        pass
