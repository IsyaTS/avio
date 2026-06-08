from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]
LogFn = Callable[[str], None]


@dataclass(frozen=True)
class TelegramBridgeDeps:
    tg_worker_token: str
    admin_token: str
    log_fn: LogFn
    telegram_transport_module: Any
    json_module: Any
    redis_client: Any
    normalize_username_fn: SyncFn
    sanitize_display_name_fn: SyncFn
    find_lead_by_telegram_fn: AsyncFn
    upsert_lead_fn: AsyncFn
    crm_links_repo: Any
    amocrm_service_module: Any
    wait_for_amocrm_link_ready_fn: AsyncFn
    link_lead_contact_fn: AsyncFn
    update_contact_telegram_fn: AsyncFn
    resolve_live_amocrm_target_by_phone_fn: AsyncFn
    crm_outbox_repo: Any
    crm_chat_links_repo: Any
    amocrm_chat_service_module: Any
    read_tenant_config_fn: SyncFn
    reconcile_avito_bridge_amocrm_links_fn: AsyncFn
    create_task_fn: SyncFn
    insert_message_out_fn: AsyncFn


@dataclass
class TelegramBridgeAmoContext:
    tenant_id: int
    origin_lead_id: int | None
    contact_id: int | None
    bridge_context: dict[str, Any]
    deps: TelegramBridgeDeps
    resolved_lead_id: int
    normalized_username: str | None
    lead_title: Any
    phone_value: str
    username_target: str
    bridge_from_origin: bool
    origin_crm_link: Mapping[str, Any] | None = None
    original_origin_provider_lead_id: int | None = None
    provider_lead_id: int | None = None
    provider_contact_id: int | None = None
    pipeline_id: int | None = None
    stage_index: int = 0
    inbound_count: int = 0


async def send_telegram_to_target(
    tenant_id: int,
    text: str,
    *,
    phone: str | None = None,
    username: str | None = None,
    lead_id: int | None = None,
    contact_id: int | None = None,
    deps: TelegramBridgeDeps,
) -> tuple[int, str]:
    phone_value = (phone or "").strip()
    username_target = deps.normalize_username_fn(username) if username else None
    headers: dict[str, str] = {}
    if deps.tg_worker_token:
        headers["X-Auth-Token"] = deps.tg_worker_token
    if deps.admin_token:
        headers["X-Admin-Token"] = deps.admin_token

    status_code, body_text = await deps.telegram_transport_module.send(
        tenant=tenant_id,
        phone=phone_value or None,
        peer=username_target or None,
        text=text,
        lead_id=lead_id,
        meta={"contact_id": contact_id} if contact_id else None,
        headers=headers,
    )
    if status_code and status_code < 500:
        try:
            parsed = deps.json_module.loads(body_text)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            await _handle_telegram_transport_result(
                tenant_id=tenant_id,
                text=text,
                phone_value=phone_value,
                username_target=username_target,
                lead_id=lead_id,
                contact_id=contact_id,
                status_code=status_code,
                parsed=parsed,
                deps=deps,
            )

    return status_code, body_text


async def _handle_telegram_transport_result(
    *,
    tenant_id: int,
    text: str,
    phone_value: str,
    username_target: str | None,
    lead_id: int | None,
    contact_id: int | None,
    status_code: int,
    parsed: Mapping[str, Any],
    deps: TelegramBridgeDeps,
) -> None:
    peer_id_value = parsed.get("peer_id")
    message_id_value = parsed.get("message_id")
    username_value = str(parsed.get("username") or "").strip()
    display_name_value = deps.sanitize_display_name_fn(parsed.get("display_name"))
    resolved_peer_id = await _cache_resolved_peer(
        tenant_id=tenant_id,
        peer_id_value=peer_id_value,
        phone_value=phone_value,
        deps=deps,
    )
    if not (200 <= status_code < 300) or not resolved_peer_id or not text.strip():
        return

    bridge_context = await _build_bridge_context(
        tenant_id=tenant_id,
        resolved_peer_id=resolved_peer_id,
        phone_value=phone_value,
        username_target=username_target,
        username_value=username_value,
        display_name_value=display_name_value,
        lead_id=lead_id,
        contact_id=contact_id,
        deps=deps,
    )
    if bridge_context is None:
        return

    await _store_bridge_outbound_message(
        tenant_id=tenant_id,
        text=text,
        message_id_value=message_id_value,
        bridge_context=bridge_context,
        deps=deps,
    )
    await _mirror_bridge_message_to_amocrm(
        tenant_id=tenant_id,
        text=text,
        lead_id=lead_id,
        bridge_context=bridge_context,
        deps=deps,
    )


async def _cache_resolved_peer(
    *,
    tenant_id: int,
    peer_id_value: Any,
    phone_value: str,
    deps: TelegramBridgeDeps,
) -> int | None:
    try:
        if peer_id_value is None:
            return None
        resolved_peer_id = int(peer_id_value)
        if phone_value:
            await deps.redis_client.set(
                f"cache:avito_phone:{tenant_id}:{peer_id_value}",
                phone_value,
                ex=3600 * 24 * 7,
            )
        return resolved_peer_id
    except Exception:
        return None


async def _build_bridge_context(
    *,
    tenant_id: int,
    resolved_peer_id: int,
    phone_value: str,
    username_target: str | None,
    username_value: str,
    display_name_value: str | None,
    lead_id: int | None,
    contact_id: int | None,
    deps: TelegramBridgeDeps,
) -> dict[str, Any] | None:
    resolved_lead_id: int | None = None
    title_hint = f"tg:id {resolved_peer_id}"
    contact_hint = phone_value or (username_target or "").strip()
    try:
        found_lead = await deps.find_lead_by_telegram_fn(int(tenant_id), int(resolved_peer_id))
    except Exception as exc:
        deps.log_fn(
            "event=avito_phone_tg_find_lead_failed tenant=%s peer_id=%s error=%s"
            % (tenant_id, resolved_peer_id, exc)
        )
        found_lead = None
    if found_lead and int(found_lead) > 0:
        resolved_lead_id = int(found_lead)

    normalized_username = deps.normalize_username_fn(username_value) if username_value else None
    lead_title = normalized_username or display_name_value or title_hint
    lead_contact = normalized_username or display_name_value or contact_hint
    try:
        upsert_result = await deps.upsert_lead_fn(
            resolved_lead_id if resolved_lead_id and resolved_lead_id > 0 else None,
            channel="telegram",
            tenant_id=int(tenant_id),
            telegram_user_id=int(resolved_peer_id),
            telegram_username=(normalized_username or "").lstrip("@") or None,
            title=lead_title,
            peer_id=int(resolved_peer_id),
            peer=str(resolved_peer_id),
            contact=lead_contact,
        )
        if upsert_result is not None:
            resolved_lead_id = int(upsert_result)
    except Exception as exc:
        deps.log_fn(
            "event=avito_phone_tg_upsert_lead_failed tenant=%s peer_id=%s error=%s"
            % (tenant_id, resolved_peer_id, exc)
        )
        resolved_lead_id = resolved_lead_id or int(resolved_peer_id)
    if not resolved_lead_id or resolved_lead_id <= 0:
        return None

    bridge_context = {
        "resolved_lead_id": int(resolved_lead_id),
        "resolved_peer_id": int(resolved_peer_id),
        "normalized_username": (normalized_username or "").lstrip("@") or None,
        "lead_title": lead_title,
        "phone_value": phone_value,
        "username_target": username_target,
    }

    await _sync_bridge_contacts(
        tenant_id=tenant_id,
        contact_id=contact_id,
        bridge_context=bridge_context,
        deps=deps,
    )
    await _sync_bridge_amocrm_links(
        tenant_id=tenant_id,
        lead_id=lead_id,
        contact_id=contact_id,
        bridge_context=bridge_context,
        deps=deps,
    )
    return bridge_context


async def _sync_bridge_contacts(
    *,
    tenant_id: int,
    contact_id: int | None,
    bridge_context: Mapping[str, Any],
    deps: TelegramBridgeDeps,
) -> None:
    if not contact_id or contact_id <= 0:
        return
    resolved_lead_id = int(bridge_context["resolved_lead_id"])
    resolved_peer_id = int(bridge_context["resolved_peer_id"])
    normalized_username = bridge_context.get("normalized_username")
    try:
        await deps.link_lead_contact_fn(
            int(resolved_lead_id),
            int(contact_id),
            channel="telegram",
            peer=str(resolved_peer_id),
        )
    except Exception:
        deps.log_fn(
            "event=avito_phone_tg_link_contact_failed tenant=%s lead_id=%s contact_id=%s"
            % (tenant_id, resolved_lead_id, contact_id)
        )
    try:
        await deps.update_contact_telegram_fn(
            int(contact_id),
            int(resolved_peer_id),
            normalized_username,
        )
    except Exception:
        deps.log_fn(
            "event=avito_phone_tg_contact_update_failed tenant=%s lead_id=%s contact_id=%s"
            % (tenant_id, resolved_lead_id, contact_id)
        )


async def _sync_bridge_amocrm_links(
    *,
    tenant_id: int,
    lead_id: int | None,
    contact_id: int | None,
    bridge_context: dict[str, Any],
    deps: TelegramBridgeDeps,
) -> None:
    ctx = TelegramBridgeAmoContext(
        tenant_id=int(tenant_id),
        origin_lead_id=int(lead_id) if lead_id and int(lead_id) > 0 else None,
        contact_id=contact_id,
        bridge_context=bridge_context,
        deps=deps,
        resolved_lead_id=int(bridge_context["resolved_lead_id"]),
        normalized_username=bridge_context.get("normalized_username"),
        lead_title=bridge_context.get("lead_title"),
        phone_value=str(bridge_context.get("phone_value") or ""),
        username_target=str(bridge_context.get("username_target") or ""),
        bridge_from_origin=bool(lead_id and int(lead_id) > 0),
    )
    await _load_bridge_amocrm_sources(ctx)
    await _resolve_bridge_provider_target(ctx)
    await _cancel_origin_create_lead_if_needed(ctx)
    _load_bridge_origin_pipeline_meta(ctx)
    await _clone_bridge_provider_links(ctx)
    await _enqueue_bridge_old_origin_cleanup(ctx)
    _schedule_bridge_reconcile(ctx)
    bridge_context["provider_lead_id"] = ctx.provider_lead_id


async def _load_bridge_amocrm_sources(ctx: TelegramBridgeAmoContext) -> None:
    existing_tg_link = await _get_bridge_crm_link(ctx, ctx.resolved_lead_id)
    tg_provider_lead_id = _mapping_int(existing_tg_link, "provider_lead_id")
    tg_provider_contact_id = _mapping_int(existing_tg_link, "provider_contact_id")
    if tg_provider_lead_id is None or tg_provider_contact_id is None:
        tg_provider_lead_id = None
        tg_provider_contact_id = None
    if ctx.origin_lead_id:
        ctx.origin_crm_link = await _wait_origin_amocrm_link(ctx, timeout_seconds=8.0)
    origin_provider_lead_id = _mapping_int(ctx.origin_crm_link, "provider_lead_id")
    origin_provider_contact_id = _mapping_int(ctx.origin_crm_link, "provider_contact_id")
    if origin_provider_lead_id is not None:
        ctx.original_origin_provider_lead_id = int(origin_provider_lead_id)
    if origin_provider_lead_id is not None and origin_provider_contact_id is not None:
        ctx.provider_lead_id = int(origin_provider_lead_id)
        ctx.provider_contact_id = int(origin_provider_contact_id)
    elif not ctx.bridge_from_origin and tg_provider_lead_id is not None and tg_provider_contact_id is not None:
        ctx.provider_lead_id = int(tg_provider_lead_id)
        ctx.provider_contact_id = int(tg_provider_contact_id)


async def _get_bridge_crm_link(ctx: TelegramBridgeAmoContext, lead_id: int) -> Mapping[str, Any] | None:
    try:
        link = await ctx.deps.crm_links_repo.get_link(
            ctx.tenant_id,
            int(lead_id),
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
        )
    except Exception:
        return None
    return link if isinstance(link, Mapping) else None


async def _wait_origin_amocrm_link(
    ctx: TelegramBridgeAmoContext,
    *,
    timeout_seconds: float,
) -> Mapping[str, Any] | None:
    if not ctx.origin_lead_id:
        return None
    try:
        link = await ctx.deps.wait_for_amocrm_link_ready_fn(
            ctx.tenant_id,
            ctx.origin_lead_id,
            timeout_seconds=timeout_seconds,
            poll_seconds=0.4,
        )
    except Exception:
        return None
    return link if isinstance(link, Mapping) else None


async def _resolve_bridge_provider_target(ctx: TelegramBridgeAmoContext) -> None:
    if not ctx.bridge_from_origin and (ctx.provider_lead_id is None or ctx.provider_contact_id is None):
        await _resolve_bridge_live_provider_target(ctx)
    if (ctx.provider_lead_id is None or ctx.provider_contact_id is None) and ctx.origin_lead_id:
        await _refresh_bridge_origin_provider_target(ctx)


async def _resolve_bridge_live_provider_target(ctx: TelegramBridgeAmoContext) -> None:
    if not ctx.phone_value:
        return
    try:
        live_contact_id, live_lead_id = await ctx.deps.resolve_live_amocrm_target_by_phone_fn(
            ctx.tenant_id,
            phone=ctx.phone_value,
            origin_lead_id=ctx.origin_lead_id,
        )
    except Exception:
        live_contact_id, live_lead_id = None, None
    if ctx.provider_contact_id is None and live_contact_id is not None:
        ctx.provider_contact_id = int(live_contact_id)
    if ctx.provider_lead_id is None and live_lead_id is not None:
        ctx.provider_lead_id = int(live_lead_id)


async def _refresh_bridge_origin_provider_target(ctx: TelegramBridgeAmoContext) -> None:
    refreshed_origin_link = await _wait_origin_amocrm_link(ctx, timeout_seconds=6.0)
    if not isinstance(refreshed_origin_link, Mapping):
        return
    if ctx.provider_contact_id is None:
        refreshed_contact = _mapping_int(refreshed_origin_link, "provider_contact_id")
        if refreshed_contact is not None:
            ctx.provider_contact_id = int(refreshed_contact)
    if ctx.provider_lead_id is None:
        refreshed_lead = _mapping_int(refreshed_origin_link, "provider_lead_id")
        if refreshed_lead is not None:
            ctx.provider_lead_id = int(refreshed_lead)


async def _cancel_origin_create_lead_if_needed(ctx: TelegramBridgeAmoContext) -> None:
    if not ctx.provider_lead_id or not ctx.origin_lead_id:
        return
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


def _load_bridge_origin_pipeline_meta(ctx: TelegramBridgeAmoContext) -> None:
    if not isinstance(ctx.origin_crm_link, Mapping):
        return
    ctx.pipeline_id = _mapping_int(ctx.origin_crm_link, "pipeline_id")
    ctx.stage_index = _mapping_int(ctx.origin_crm_link, "stage_index") or 0
    ctx.inbound_count = _mapping_int(ctx.origin_crm_link, "inbound_count") or 0


async def _clone_bridge_provider_links(ctx: TelegramBridgeAmoContext) -> None:
    if not ctx.provider_lead_id and not ctx.provider_contact_id:
        return
    try:
        await _upsert_origin_bridge_provider_link(ctx)
        await _upsert_tg_bridge_provider_link(ctx)
        await _upsert_bridge_chat_links(ctx)
        await _sync_bridge_chat_profiles(ctx)
        await _enqueue_bridge_identity_updates(ctx)
    except Exception as exc:
        ctx.deps.log_fn(
            "event=avito_phone_tg_clone_amocrm_link_failed tenant=%s lead_id=%s origin_lead_id=%s error=%s"
            % (ctx.tenant_id, ctx.resolved_lead_id, ctx.origin_lead_id, exc)
        )


async def _upsert_origin_bridge_provider_link(ctx: TelegramBridgeAmoContext) -> None:
    if not ctx.origin_lead_id:
        return
    existing_origin_link = await _get_bridge_crm_link(ctx, ctx.origin_lead_id)
    origin_provider_lead_before_rebind = _mapping_int(existing_origin_link, "provider_lead_id")
    if not existing_origin_link:
        await ctx.deps.crm_links_repo.create_link(
            ctx.tenant_id,
            ctx.origin_lead_id,
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            pipeline_id=ctx.pipeline_id,
            stage_index=ctx.stage_index,
            inbound_count=ctx.inbound_count,
        )
    await _update_bridge_provider_ids(ctx, ctx.origin_lead_id)
    if origin_provider_lead_before_rebind and origin_provider_lead_before_rebind != int(ctx.provider_lead_id or 0):
        ctx.original_origin_provider_lead_id = int(origin_provider_lead_before_rebind)


async def _upsert_tg_bridge_provider_link(ctx: TelegramBridgeAmoContext) -> None:
    existing_tg_crm_link = await _get_bridge_crm_link(ctx, ctx.resolved_lead_id)
    if not existing_tg_crm_link:
        await ctx.deps.crm_links_repo.create_link(
            ctx.tenant_id,
            ctx.resolved_lead_id,
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            pipeline_id=ctx.pipeline_id,
            stage_index=ctx.stage_index,
            inbound_count=ctx.inbound_count,
        )
    await _update_bridge_provider_ids(ctx, ctx.resolved_lead_id)


async def _update_bridge_provider_ids(ctx: TelegramBridgeAmoContext, lead_id: int) -> None:
    if ctx.provider_contact_id is not None:
        await ctx.deps.crm_links_repo.update_provider_contact_id(
            ctx.tenant_id,
            int(lead_id),
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            int(ctx.provider_contact_id),
        )
    if ctx.provider_lead_id is not None:
        await ctx.deps.crm_links_repo.update_provider_lead_id(
            ctx.tenant_id,
            int(lead_id),
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            int(ctx.provider_lead_id),
        )


async def _upsert_bridge_chat_links(ctx: TelegramBridgeAmoContext) -> None:
    external_chat_id, external_conversation_id = await _bridge_chat_identity(ctx)
    if ctx.origin_lead_id:
        try:
            await _upsert_one_bridge_chat_link(ctx, ctx.origin_lead_id, external_chat_id, external_conversation_id)
        except Exception:
            pass
    await _upsert_one_bridge_chat_link(ctx, ctx.resolved_lead_id, external_chat_id, external_conversation_id)


async def _bridge_chat_identity(ctx: TelegramBridgeAmoContext) -> tuple[str, str]:
    origin_chat_link = None
    if ctx.origin_lead_id:
        try:
            origin_chat_link = await ctx.deps.crm_chat_links_repo.get_link(
                ctx.tenant_id,
                ctx.origin_lead_id,
                ctx.deps.amocrm_chat_service_module.AMOCRM_CHAT_PROVIDER,
            )
        except Exception:
            origin_chat_link = None
    external_chat_id = (
        str((origin_chat_link or {}).get("external_chat_id") or "").strip()
        or f"avio:{ctx.tenant_id}:telegram:{ctx.resolved_lead_id}"
    )
    external_conversation_id = (
        str((origin_chat_link or {}).get("external_conversation_id") or "").strip()
        or external_chat_id
    )
    if ctx.provider_lead_id is None:
        return external_chat_id, external_conversation_id
    try:
        return await ctx.deps.amocrm_chat_service_module._canonical_chat_identity(
            ctx.tenant_id,
            provider_lead_id=int(ctx.provider_lead_id),
            fallback_chat_id=external_chat_id,
            fallback_conversation_id=external_conversation_id,
        )
    except Exception:
        return external_chat_id, external_conversation_id


async def _upsert_one_bridge_chat_link(
    ctx: TelegramBridgeAmoContext,
    lead_id: int,
    external_chat_id: str,
    external_conversation_id: str,
) -> None:
    await ctx.deps.crm_chat_links_repo.upsert_link(
        ctx.tenant_id,
        int(lead_id),
        ctx.deps.amocrm_chat_service_module.AMOCRM_CHAT_PROVIDER,
        external_chat_id=external_chat_id,
        external_conversation_id=external_conversation_id,
        external_contact_id=int(ctx.provider_contact_id) if ctx.provider_contact_id is not None else None,
        external_lead_id=int(ctx.provider_lead_id) if ctx.provider_lead_id is not None else None,
    )


async def _sync_bridge_chat_profiles(ctx: TelegramBridgeAmoContext) -> None:
    await _sync_one_bridge_chat_profile(ctx, ctx.resolved_lead_id, "avito_phone_tg_sync_profile_failed")
    if ctx.origin_lead_id:
        await _sync_one_bridge_chat_profile(ctx, ctx.origin_lead_id, "avito_phone_avito_sync_profile_failed")


async def _sync_one_bridge_chat_profile(ctx: TelegramBridgeAmoContext, lead_id: int, event_name: str) -> None:
    try:
        await ctx.deps.amocrm_chat_service_module.sync_chat_profile(
            ctx.tenant_id,
            int(lead_id),
            cfg=ctx.deps.read_tenant_config_fn(ctx.tenant_id),
        )
    except Exception:
        ctx.deps.log_fn("event=%s tenant=%s lead_id=%s" % (event_name, ctx.tenant_id, lead_id))


async def _enqueue_bridge_identity_updates(ctx: TelegramBridgeAmoContext) -> None:
    preferred_identity = (
        (ctx.normalized_username or "").strip()
        or str(ctx.lead_title or "").strip()
        or ctx.phone_value
        or ctx.username_target.strip()
    )
    if not preferred_identity:
        return
    for target_lead in _bridge_identity_target_leads(ctx):
        await _enqueue_bridge_identity_update(ctx, target_lead, "update_fields", {"lead_name": preferred_identity})
        await _enqueue_bridge_identity_update(ctx, target_lead, "update_contact_fields", {"contact_name": preferred_identity})


def _bridge_identity_target_leads(ctx: TelegramBridgeAmoContext) -> list[int]:
    target_leads = [int(ctx.resolved_lead_id)]
    if ctx.origin_lead_id and ctx.origin_lead_id not in target_leads:
        target_leads.append(int(ctx.origin_lead_id))
    return target_leads


async def _enqueue_bridge_identity_update(
    ctx: TelegramBridgeAmoContext,
    target_lead: int,
    event_type: str,
    payload: Mapping[str, Any],
) -> None:
    try:
        await ctx.deps.crm_outbox_repo.enqueue(
            ctx.tenant_id,
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            int(target_lead),
            event_type,
            dict(payload),
        )
    except Exception:
        pass


async def _enqueue_bridge_old_origin_cleanup(ctx: TelegramBridgeAmoContext) -> None:
    if ctx.provider_lead_id and ctx.origin_lead_id and ctx.original_origin_provider_lead_id is None:
        await _refresh_original_origin_provider_lead(ctx)
    if not _needs_bridge_old_origin_cleanup(ctx):
        return
    cleanup_payload = {"amo_lead_id": int(ctx.original_origin_provider_lead_id)}
    try:
        already_cleanup = await ctx.deps.crm_outbox_repo.has_recent_event(
            ctx.tenant_id,
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            ctx.resolved_lead_id,
            "delete_lead",
            cleanup_payload,
            window_seconds=900,
        )
    except Exception:
        already_cleanup = False
    if already_cleanup:
        return
    try:
        await ctx.deps.crm_outbox_repo.enqueue(
            ctx.tenant_id,
            ctx.deps.amocrm_service_module.AMOCRM_PROVIDER,
            ctx.resolved_lead_id,
            "delete_lead",
            cleanup_payload,
        )
    except Exception as exc:
        ctx.deps.log_fn(
            "event=avito_phone_tg_delete_old_lead_enqueue_failed tenant=%s old_lead=%s keep_lead=%s error=%s"
            % (ctx.tenant_id, ctx.original_origin_provider_lead_id, ctx.provider_lead_id, exc)
        )


async def _refresh_original_origin_provider_lead(ctx: TelegramBridgeAmoContext) -> None:
    if not ctx.origin_lead_id:
        return
    current_origin_link = await _get_bridge_crm_link(ctx, ctx.origin_lead_id)
    current_origin_lead_id = _mapping_int(current_origin_link, "provider_lead_id")
    if current_origin_lead_id and current_origin_lead_id != int(ctx.provider_lead_id):
        ctx.original_origin_provider_lead_id = int(current_origin_lead_id)


def _needs_bridge_old_origin_cleanup(ctx: TelegramBridgeAmoContext) -> bool:
    return bool(
        ctx.original_origin_provider_lead_id
        and ctx.provider_lead_id
        and int(ctx.original_origin_provider_lead_id) != int(ctx.provider_lead_id)
    )


def _schedule_bridge_reconcile(ctx: TelegramBridgeAmoContext) -> None:
    if not ctx.provider_lead_id or not ctx.origin_lead_id:
        return
    try:
        ctx.deps.create_task_fn(
            ctx.deps.reconcile_avito_bridge_amocrm_links_fn(
                tenant_id=ctx.tenant_id,
                origin_lead_id=ctx.origin_lead_id,
                tg_lead_id=ctx.resolved_lead_id,
                keep_provider_lead_id=int(ctx.provider_lead_id),
                keep_provider_contact_id=int(ctx.provider_contact_id) if ctx.provider_contact_id is not None else None,
            )
        )
    except Exception:
        pass


def _mapping_int(item: Any, key: str) -> int | None:
    if not isinstance(item, Mapping) or item.get(key) is None:
        return None
    try:
        return int(item.get(key))
    except Exception:
        return None


async def _store_bridge_outbound_message(
    *,
    tenant_id: int,
    text: str,
    message_id_value: Any,
    bridge_context: Mapping[str, Any],
    deps: TelegramBridgeDeps,
) -> None:
    try:
        provider_msg_id = None
        try:
            provider_msg_id = str(int(message_id_value)) if message_id_value is not None else None
        except Exception:
            provider_msg_id = str(message_id_value or "").strip() or None
        await deps.insert_message_out_fn(
            int(bridge_context["resolved_lead_id"]),
            text.strip(),
            provider_msg_id=provider_msg_id,
            status="sent",
            tenant_id=int(tenant_id),
            channel="telegram",
            telegram_user_id=int(bridge_context["resolved_peer_id"]),
            telegram_username=bridge_context.get("normalized_username"),
            title=bridge_context.get("lead_title"),
            is_bot=True,
            source="bot",
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_phone_tg_store_out_failed tenant=%s lead_id=%s error=%s"
            % (tenant_id, bridge_context["resolved_lead_id"], exc)
        )


async def _mirror_bridge_message_to_amocrm(
    *,
    tenant_id: int,
    text: str,
    lead_id: int | None,
    bridge_context: Mapping[str, Any],
    deps: TelegramBridgeDeps,
) -> None:
    provider_lead_id = bridge_context.get("provider_lead_id")
    resolved_lead_id = int(bridge_context["resolved_lead_id"])
    if provider_lead_id is None:
        deps.log_fn(
            "event=avito_phone_tg_amocrm_sync_skipped tenant=%s lead_id=%s reason=provider_lead_missing"
            % (tenant_id, resolved_lead_id)
        )
        return
    if not text.strip():
        return
    try:
        await deps.amocrm_chat_service_module.enqueue_message(
            int(tenant_id),
            int(resolved_lead_id),
            direction="out",
            text=text.strip(),
            channel="telegram",
            attachments=None,
        )
        deps.log_fn(
            "event=avito_phone_tg_amocrm_sync_enqueued tenant=%s lead_id=%s"
            % (tenant_id, resolved_lead_id)
        )
    except Exception as exc:
        deps.log_fn(
            "event=avito_phone_tg_amocrm_sync_failed tenant=%s lead_id=%s error=%s"
            % (tenant_id, resolved_lead_id, exc)
        )
