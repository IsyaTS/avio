from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from libs.core import db as db_module
from libs.core.integrations import amocrm as amocrm_integration
from libs.core.repo import crm_chat_links, crm_links, crm_outbox
from libs.core.services import amocrm as amocrm_service
from libs.core.services import amocrm_chat as amocrm_chat_service


@dataclass(frozen=True)
class AmoCrmOutboxDeps:
    enabled: bool
    outbox_limit: int
    outbox_max_attempts: int
    log_fn: Callable[..., None]
    read_tenant_config_fn: Callable[[int], Mapping[str, Any]]
    download_file_fn: Callable[[str], tuple[bytes | None, str, str | None]]


@dataclass
class AmoCrmEventContext:
    event: Mapping[str, Any]
    deps: AmoCrmOutboxDeps
    tenant_id: int
    lead_id: int
    payload: dict[str, Any]
    event_type: str
    cfg: Mapping[str, Any]
    amocrm_cfg: Mapping[str, Any]
    client: Any
    link: Mapping[str, Any] | None = None
    provider_lead_id: Any = None


@dataclass
class AmoCrmCreateLeadContext:
    tenant_id: int
    lead_id: int
    payload: Mapping[str, Any]
    cfg: Mapping[str, Any]
    amocrm_cfg: Mapping[str, Any]
    client: Any
    log_fn: Callable[..., None]
    link: Mapping[str, Any] | None = None
    stage_id: Any = None
    pipeline_id: Any = None
    lead_name: str = ""
    source_channel: str = ""
    contact_id: int | None = None
    amo_lead_id: int | None = None
    chat_link: Mapping[str, Any] | None = None


@dataclass
class AmoCrmChatSyncContext:
    tenant_id: int
    lead_id: int
    payload: Mapping[str, Any]
    cfg: Mapping[str, Any]
    client: Any
    link: Mapping[str, Any] | None
    provider_lead_id: Any = None
    provider_contact_id: Any = None


def amocrm_backoff_seconds(attempts: int) -> int:
    if attempts <= 1:
        return 5
    delay = 5 * (2 ** min(attempts - 1, 6))
    return int(min(delay, 300))


def parse_amocrm_payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except Exception:
            raw = ""
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception:
            data = {}
        if isinstance(data, dict):
            return dict(data)
    return {}


def amocrm_stage_id_from_cfg(amocrm_cfg: Mapping[str, Any] | None, stage_index: int) -> int | None:
    stages = amocrm_cfg.get("stages") if isinstance(amocrm_cfg, Mapping) else None
    if not isinstance(stages, list) or not stages:
        return None
    try:
        idx = int(stage_index)
    except Exception:
        idx = 0
    if idx < 0 or idx >= len(stages):
        idx = 0
    stage = stages[idx] if isinstance(stages[idx], Mapping) else None
    stage_id_raw = stage.get("amo_stage_id") if isinstance(stage, Mapping) else None
    try:
        stage_id = int(stage_id_raw)
    except Exception:
        stage_id = 0
    if stage_id > 0:
        return stage_id
    for item in stages:
        if not isinstance(item, Mapping):
            continue
        try:
            fallback_id = int(item.get("amo_stage_id") or 0)
        except Exception:
            fallback_id = 0
        if fallback_id > 0:
            return fallback_id
    return None


def is_amocrm_lead_not_found_error(exc: Exception) -> bool:
    text = str(exc or "")
    return "amocrm_http_error:400" in text and "Lead not found" in text


async def amocrm_entity_exists(
    client: Any,
    *,
    entity_type: str,
    entity_id: int | None,
) -> bool | None:
    if not entity_id:
        return False
    kind = str(entity_type or "").strip().lower()
    try:
        if kind == "lead":
            payload = await client.get_lead(int(entity_id))
        elif kind == "contact":
            payload = await client.get_contact(int(entity_id))
        else:
            return None
        if not isinstance(payload, Mapping):
            return False
        remote_id = payload.get("id")
        try:
            return int(remote_id) == int(entity_id)
        except Exception:
            return False
    except Exception as exc:
        if "amocrm_http_error:404" in str(exc or ""):
            return False
        return None


async def recover_amocrm_missing_lead(
    *,
    tenant_id: int,
    lead_id: int,
    payload: Mapping[str, Any],
    amocrm_cfg: Mapping[str, Any],
    client: Any,
    link: Mapping[str, Any] | None,
) -> int | None:
    pipeline_id_raw = (
        payload.get("pipeline_id")
        or amocrm_cfg.get("pipeline_id")
        or (link or {}).get("pipeline_id")
    )
    try:
        pipeline_id = int(pipeline_id_raw)
    except Exception:
        pipeline_id = 0
    if pipeline_id <= 0:
        return None
    stage_id = amocrm_stage_id_from_cfg(amocrm_cfg, int((link or {}).get("stage_index") or 0))
    if not stage_id:
        return None
    contact_id_raw = payload.get("amo_contact_id") or (link or {}).get("provider_contact_id")
    try:
        contact_id = int(contact_id_raw) if contact_id_raw is not None else None
    except Exception:
        contact_id = None
    lead_name = str(payload.get("lead_name") or f"Avio lead {lead_id}").strip() or f"Avio lead {lead_id}"
    new_lead_id = await client.create_lead(
        pipeline_id=int(pipeline_id),
        status_id=int(stage_id),
        name=lead_name,
        contact_id=contact_id,
        custom_fields=None,
    )
    if not new_lead_id:
        return None
    await crm_links.update_provider_lead_id(
        int(tenant_id),
        int(lead_id),
        amocrm_service.AMOCRM_PROVIDER,
        int(new_lead_id),
    )
    chat_link = await crm_chat_links.get_link(
        int(tenant_id),
        int(lead_id),
        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
    )
    await crm_chat_links.upsert_link(
        int(tenant_id),
        int(lead_id),
        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
        external_chat_id=str((chat_link or {}).get("external_chat_id") or ""),
        external_conversation_id=str((chat_link or {}).get("external_conversation_id") or ""),
        external_contact_id=int(contact_id) if contact_id is not None else None,
        external_lead_id=int(new_lead_id),
        chat_scope_id=str((chat_link or {}).get("chat_scope_id") or ""),
        source_id=str((chat_link or {}).get("source_id") or ""),
    )
    return int(new_lead_id)


async def handle_amocrm_event(
    event: Mapping[str, Any],
    *,
    deps: AmoCrmOutboxDeps,
) -> None:
    ctx = await _build_amocrm_event_context(event, deps=deps)
    if ctx is None:
        return
    await _dispatch_amocrm_event(ctx)


async def _build_amocrm_event_context(
    event: Mapping[str, Any],
    *,
    deps: AmoCrmOutboxDeps,
) -> AmoCrmEventContext | None:
    tenant_id = int(event.get("tenant_id") or 0)
    lead_id = int(event.get("lead_id") or 0)
    payload = parse_amocrm_payload(event.get("payload"))
    event_type = str(event.get("event_type") or payload.get("event_type") or "")
    cfg = deps.read_tenant_config_fn(int(tenant_id))
    amocrm_cfg = amocrm_service.get_amocrm_cfg(cfg)
    if not amocrm_cfg or not bool(amocrm_cfg.get("enabled")):
        return None
    client = await _build_amocrm_event_client(int(tenant_id), amocrm_cfg)
    return AmoCrmEventContext(
        event=event,
        deps=deps,
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        payload=payload,
        event_type=event_type,
        cfg=cfg,
        amocrm_cfg=amocrm_cfg,
        client=client,
    )


async def _build_amocrm_event_client(tenant_id: int, amocrm_cfg: Mapping[str, Any]) -> Any:
    base_url = await amocrm_service.resolve_api_base_url(amocrm_cfg, tenant_id)
    oauth_cfg = amocrm_service.resolve_oauth_cfg(amocrm_cfg, tenant_id)
    return amocrm_integration.AmoCRMClient(
        tenant_id=tenant_id,
        base_url=base_url,
        client_id=str(oauth_cfg.get("client_id") or ""),
        client_secret=str(oauth_cfg.get("client_secret") or ""),
        redirect_url=str(oauth_cfg.get("redirect_url") or ""),
    )


async def _dispatch_amocrm_event(ctx: AmoCrmEventContext) -> None:
    if ctx.event_type == "create_lead":
        await _handle_create_lead_event(
            tenant_id=ctx.tenant_id,
            lead_id=ctx.lead_id,
            payload=ctx.payload,
            cfg=ctx.cfg,
            amocrm_cfg=ctx.amocrm_cfg,
            client=ctx.client,
            log_fn=ctx.deps.log_fn,
        )
        return
    if ctx.event_type == "delete_lead":
        await _delete_amocrm_entity(ctx, entity="lead")
        return
    if ctx.event_type == "delete_contact":
        await _delete_amocrm_entity(ctx, entity="contact")
        return
    ctx.link = await crm_links.get_link(ctx.tenant_id, ctx.lead_id, amocrm_service.AMOCRM_PROVIDER)
    ctx.provider_lead_id = ctx.link.get("provider_lead_id") if isinstance(ctx.link, Mapping) else None
    if ctx.event_type == "chat_sync_message":
        await _handle_chat_sync_message_event(
            tenant_id=ctx.tenant_id,
            lead_id=ctx.lead_id,
            payload=ctx.payload,
            cfg=ctx.cfg,
            client=ctx.client,
            link=ctx.link if isinstance(ctx.link, Mapping) else None,
        )
        return
    if not ctx.provider_lead_id:
        raise amocrm_integration.AmoCRMError("amocrm_lead_missing")
    if ctx.event_type == "update_fields":
        await _update_amocrm_lead_fields(ctx)
        return
    if ctx.event_type == "update_contact_fields":
        await _update_amocrm_contact_fields(ctx)
        return
    if ctx.event_type == "add_files":
        await _add_amocrm_files(ctx)
        return
    if ctx.event_type == "move_stage":
        await _move_amocrm_stage(ctx)
        return
    if ctx.event_type == "add_note":
        text = str(ctx.payload.get("text") or "").strip()
        if text:
            await ctx.client.add_lead_note(int(ctx.provider_lead_id), text)


async def _delete_amocrm_entity(ctx: AmoCrmEventContext, *, entity: str) -> None:
    id_key = "amo_lead_id" if entity == "lead" else "amo_contact_id"
    entity_id = _optional_int(ctx.payload.get(id_key) or ctx.payload.get(f"provider_{entity}_id")) or 0
    if entity_id <= 0:
        return
    try:
        if entity == "lead":
            await ctx.client.delete_lead(int(entity_id))
        else:
            await ctx.client.delete_contact(int(entity_id))
    except amocrm_integration.AmoCRMError as exc:
        if "amocrm_http_error:404" not in str(exc):
            raise


async def _update_amocrm_lead_fields(ctx: AmoCrmEventContext) -> None:
    custom_fields = ctx.payload.get("custom_fields")
    lead_name = str(ctx.payload.get("lead_name") or "").strip() or None
    if not isinstance(custom_fields, list) and not lead_name:
        return
    try:
        await ctx.client.update_lead_fields(
            int(ctx.provider_lead_id),
            name=lead_name,
            custom_fields=custom_fields if isinstance(custom_fields, list) else [],
        )
    except amocrm_integration.AmoCRMError as exc:
        if not _can_recover_missing_lead(lead_name, custom_fields, exc):
            raise
        recovered_lead_id = await recover_amocrm_missing_lead(
            tenant_id=ctx.tenant_id,
            lead_id=ctx.lead_id,
            payload=ctx.payload,
            amocrm_cfg=ctx.amocrm_cfg,
            client=ctx.client,
            link=ctx.link if isinstance(ctx.link, Mapping) else None,
        )
        if not recovered_lead_id:
            raise
        await ctx.client.update_lead_fields(int(recovered_lead_id), name=lead_name, custom_fields=[])


def _can_recover_missing_lead(lead_name: str | None, custom_fields: Any, exc: Exception) -> bool:
    return bool(
        lead_name
        and not (isinstance(custom_fields, list) and custom_fields)
        and is_amocrm_lead_not_found_error(exc)
    )


async def _update_amocrm_contact_fields(ctx: AmoCrmEventContext) -> None:
    custom_fields = ctx.payload.get("custom_fields")
    contact_name = str(ctx.payload.get("contact_name") or "").strip() or None
    provider_contact_id = ctx.link.get("provider_contact_id") if isinstance(ctx.link, Mapping) else None
    if not provider_contact_id and ctx.provider_lead_id:
        provider_contact_id = await ctx.client.get_lead_contact_id(int(ctx.provider_lead_id))
        if provider_contact_id:
            await crm_links.update_provider_contact_id(
                ctx.tenant_id,
                ctx.lead_id,
                amocrm_service.AMOCRM_PROVIDER,
                int(provider_contact_id),
            )
    if provider_contact_id and (isinstance(custom_fields, list) or contact_name):
        await ctx.client.update_contact_fields(
            int(provider_contact_id),
            name=contact_name,
            custom_fields=custom_fields if isinstance(custom_fields, list) else [],
        )


async def _add_amocrm_files(ctx: AmoCrmEventContext) -> None:
    attachments = ctx.payload.get("attachments")
    if not ctx.provider_lead_id or not isinstance(attachments, list):
        return
    for item in attachments:
        if isinstance(item, Mapping):
            await _add_one_amocrm_file(ctx, item)


async def _add_one_amocrm_file(ctx: AmoCrmEventContext, item: Mapping[str, Any]) -> None:
    url = str(item.get("url") or "").strip()
    if not url:
        return
    content, name, detected_mime = ctx.deps.download_file_fn(url)
    if not content:
        ctx.deps.log_fn(
            f"amocrm_file_skip tenant={ctx.tenant_id} lead_id={ctx.lead_id} reason=download_failed url={url}"
        )
        return
    filename = str(item.get("filename") or name or "attachment")
    content_type = (
        str(item.get("mime") or item.get("mime_type") or "").strip()
        or (detected_mime.strip() if detected_mime else "")
        or None
    )
    file_uuid = await ctx.client.upload_file(filename=filename, content=content, content_type=content_type)
    if file_uuid:
        await ctx.client.attach_file_to_lead(int(ctx.provider_lead_id), file_uuid)
    else:
        ctx.deps.log_fn(
            f"amocrm_file_skip tenant={ctx.tenant_id} lead_id={ctx.lead_id} reason=upload_failed url={url}"
        )


async def _move_amocrm_stage(ctx: AmoCrmEventContext) -> None:
    stage_id = ctx.payload.get("stage_id")
    pipeline_id = ctx.payload.get("pipeline_id") or ctx.amocrm_cfg.get("pipeline_id")
    if not stage_id:
        return
    await ctx.client.move_lead_stage(
        int(ctx.provider_lead_id),
        status_id=int(stage_id),
        pipeline_id=int(pipeline_id) if pipeline_id else None,
    )
    stage_index_val = _optional_int(ctx.payload.get("stage_index"))
    if stage_index_val is not None:
        await crm_links.update_stage_index(
            ctx.tenant_id,
            ctx.lead_id,
            amocrm_service.AMOCRM_PROVIDER,
            stage_index_val,
            pipeline_id=int(pipeline_id) if pipeline_id else None,
        )


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


async def process_amocrm_outbox(*, deps: AmoCrmOutboxDeps) -> None:
    if not deps.enabled:
        deps.log_fn("event=amocrm_outbox_disabled")
        return
    deps.log_fn("event=amocrm_outbox_loop_start")
    while True:
        try:
            events = await crm_outbox.take_pending(limit=deps.outbox_limit)
            if not events:
                await asyncio.sleep(2.0)
                continue
            events = sorted(events, key=lambda item: int(item.get("id") or 0))
            for event in events:
                event_id = event.get("id")
                if not event_id:
                    continue
                try:
                    await handle_amocrm_event(event, deps=deps)
                    await crm_outbox.mark_done(int(event_id))
                    deps.log_fn(
                        f"amocrm_event_done tenant={event.get('tenant_id')} "
                        f"lead_id={event.get('lead_id')} event={event.get('event_type')}"
                    )
                except Exception as exc:
                    attempts = int(event.get("attempts") or 0) + 1
                    if attempts >= deps.outbox_max_attempts:
                        await crm_outbox.mark_dead(int(event_id), str(exc))
                        deps.log_fn(
                            f"amocrm_event_dead tenant={event.get('tenant_id')} "
                            f"lead_id={event.get('lead_id')} event={event.get('event_type')} error={exc}"
                        )
                    else:
                        delay = amocrm_backoff_seconds(attempts)
                        next_retry = datetime.now(tz=timezone.utc) + timedelta(seconds=delay)
                        await crm_outbox.mark_retry(int(event_id), attempts, next_retry, str(exc))
                        deps.log_fn(
                            f"amocrm_event_retry tenant={event.get('tenant_id')} "
                            f"lead_id={event.get('lead_id')} event={event.get('event_type')} attempts={attempts}"
                        )
            await asyncio.sleep(0)
        except Exception as exc:
            deps.log_fn(f"event=amocrm_outbox_loop_error err={exc}")
            await asyncio.sleep(2.0)


async def _handle_create_lead_event(
    *,
    tenant_id: int,
    lead_id: int,
    payload: Mapping[str, Any],
    cfg: Mapping[str, Any],
    amocrm_cfg: Mapping[str, Any],
    client: Any,
    log_fn: Callable[..., None],
) -> None:
    ctx = AmoCrmCreateLeadContext(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        payload=payload,
        cfg=cfg,
        amocrm_cfg=amocrm_cfg,
        client=client,
        log_fn=log_fn,
        stage_id=payload.get("stage_id"),
        pipeline_id=payload.get("pipeline_id") or amocrm_cfg.get("pipeline_id"),
        lead_name=str(payload.get("lead_name") or f"Avio lead {lead_id}"),
        source_channel=str(payload.get("channel") or "").strip().lower(),
    )
    if await _create_lead_should_skip_existing(ctx):
        return
    if not ctx.stage_id or not ctx.pipeline_id:
        raise amocrm_integration.AmoCRMError("amocrm_stage_missing")
    ctx.contact_id = await _resolve_create_lead_contact(ctx)
    ctx.amo_lead_id = await _create_amocrm_lead(ctx)
    if not ctx.amo_lead_id:
        return
    await crm_links.update_provider_lead_id(
        ctx.tenant_id,
        ctx.lead_id,
        amocrm_service.AMOCRM_PROVIDER,
        int(ctx.amo_lead_id),
    )
    await _upsert_create_lead_chat_link(ctx)
    await _sync_create_lead_chat_profile_and_bootstrap(ctx)
    await _update_create_lead_stage_index(ctx)


async def _create_lead_should_skip_existing(ctx: AmoCrmCreateLeadContext) -> bool:
    ctx.link = await crm_links.get_link(ctx.tenant_id, ctx.lead_id, amocrm_service.AMOCRM_PROVIDER)
    if not ctx.link or ctx.link.get("provider_lead_id") is None:
        return False
    provider_lead_id_value = _optional_int(ctx.link.get("provider_lead_id"))
    lead_exists = await amocrm_entity_exists(ctx.client, entity_type="lead", entity_id=provider_lead_id_value)
    if lead_exists is False:
        await crm_links.update_provider_lead_id(
            ctx.tenant_id,
            ctx.lead_id,
            amocrm_service.AMOCRM_PROVIDER,
            None,
        )
        ctx.link = await crm_links.get_link(ctx.tenant_id, ctx.lead_id, amocrm_service.AMOCRM_PROVIDER)
        return False
    return lead_exists is True


async def _resolve_create_lead_contact(ctx: AmoCrmCreateLeadContext) -> int | None:
    existing_contact_id = await _existing_create_lead_contact_id(ctx)
    if existing_contact_id and not await _amocrm_contact_exists(ctx.client, existing_contact_id):
        existing_contact_id = None
    phone_value = str(ctx.payload.get("contact_phone") or "").strip() or None
    name_value = str(ctx.payload.get("contact_name") or "").strip() or None
    contact_id = existing_contact_id or await ctx.client.upsert_contact(phone=phone_value, name=name_value)
    if contact_id and not await _amocrm_contact_exists(ctx.client, int(contact_id)):
        contact_id = await ctx.client.upsert_contact(phone=phone_value, name=name_value)
        if contact_id and not await _amocrm_contact_exists(ctx.client, int(contact_id)):
            contact_id = None
    if contact_id:
        await crm_links.update_provider_contact_id(
            ctx.tenant_id,
            ctx.lead_id,
            amocrm_service.AMOCRM_PROVIDER,
            int(contact_id),
        )
    return int(contact_id) if contact_id else None


async def _existing_create_lead_contact_id(ctx: AmoCrmCreateLeadContext) -> int | None:
    existing_contact_id = _optional_int((ctx.link or {}).get("provider_contact_id"))
    if existing_contact_id is not None or ctx.source_channel not in {"telegram", "avito"}:
        return existing_contact_id
    try:
        chat_link = await crm_chat_links.get_link(
            ctx.tenant_id,
            ctx.lead_id,
            amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
        )
    except Exception:
        chat_link = None
    return _optional_int((chat_link or {}).get("external_contact_id"))


async def _amocrm_contact_exists(client: Any, contact_value: int | None) -> bool:
    if not contact_value:
        return False
    try:
        remote_payload = await client.get_contact(int(contact_value))
    except Exception:
        return False
    if not isinstance(remote_payload, Mapping) or not remote_payload:
        return False
    return _optional_int(remote_payload.get("id")) == int(contact_value)


async def _create_amocrm_lead(ctx: AmoCrmCreateLeadContext) -> int | None:
    custom_fields = ctx.payload.get("custom_fields")
    amo_lead_id = await ctx.client.create_lead(
        pipeline_id=int(ctx.pipeline_id),
        status_id=int(ctx.stage_id),
        name=ctx.lead_name,
        contact_id=ctx.contact_id,
        custom_fields=custom_fields if isinstance(custom_fields, list) else None,
    )
    return int(amo_lead_id) if amo_lead_id else None


async def _upsert_create_lead_chat_link(ctx: AmoCrmCreateLeadContext) -> None:
    ctx.chat_link = await crm_chat_links.get_link(
        ctx.tenant_id,
        ctx.lead_id,
        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
    )
    fallback_chat_id, fallback_conversation_id = _create_lead_chat_fallbacks(ctx)
    canonical_chat_id, canonical_conversation_id = await _canonical_create_lead_chat_identity(
        ctx,
        fallback_chat_id,
        fallback_conversation_id,
    )
    ctx.chat_link = await crm_chat_links.upsert_link(
        ctx.tenant_id,
        ctx.lead_id,
        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
        external_chat_id=str(canonical_chat_id or ""),
        external_conversation_id=str(canonical_conversation_id or ""),
        external_contact_id=int(ctx.contact_id) if ctx.contact_id is not None else None,
        external_lead_id=int(ctx.amo_lead_id),
        chat_scope_id=str((ctx.chat_link or {}).get("chat_scope_id") or ""),
        source_id=str((ctx.chat_link or {}).get("source_id") or ""),
    )


def _create_lead_chat_fallbacks(ctx: AmoCrmCreateLeadContext) -> tuple[str, str]:
    fallback_chat_id = (
        str((ctx.chat_link or {}).get("external_chat_id") or "").strip()
        or f"avio:{ctx.tenant_id}:amo:{ctx.amo_lead_id}"
    )
    fallback_conversation_id = (
        str((ctx.chat_link or {}).get("external_conversation_id") or "").strip()
        or fallback_chat_id
    )
    return fallback_chat_id, fallback_conversation_id


async def _canonical_create_lead_chat_identity(
    ctx: AmoCrmCreateLeadContext,
    fallback_chat_id: str,
    fallback_conversation_id: str,
) -> tuple[str, str]:
    try:
        return await amocrm_chat_service._canonical_chat_identity(
            ctx.tenant_id,
            provider_lead_id=int(ctx.amo_lead_id),
            fallback_chat_id=fallback_chat_id,
            fallback_conversation_id=fallback_conversation_id,
        )
    except Exception:
        return fallback_chat_id, fallback_conversation_id


async def _sync_create_lead_chat_profile_and_bootstrap(ctx: AmoCrmCreateLeadContext) -> None:
    if not isinstance(ctx.chat_link, Mapping):
        return
    try:
        await amocrm_chat_service.sync_chat_profile(ctx.tenant_id, ctx.lead_id, cfg=ctx.cfg)
    except Exception:
        ctx.log_fn("event=amocrm_chat_profile_sync_failed tenant=%s lead_id=%s" % (ctx.tenant_id, ctx.lead_id))
    await _bootstrap_create_lead_chat_message(ctx)


async def _bootstrap_create_lead_chat_message(ctx: AmoCrmCreateLeadContext) -> None:
    try:
        bootstrap_text = await _create_lead_bootstrap_text(ctx)
        bootstrap_direction = str(ctx.payload.get("bootstrap_direction") or "").strip().lower()
        if bootstrap_direction not in {"in", "out"}:
            bootstrap_direction = "out"
        bootstrap_attachments_raw = ctx.payload.get("bootstrap_attachments")
        bootstrap_attachments = list(bootstrap_attachments_raw) if isinstance(bootstrap_attachments_raw, list) else None
        if bootstrap_text:
            await amocrm_chat_service.enqueue_message(
                ctx.tenant_id,
                ctx.lead_id,
                direction=bootstrap_direction,
                text=bootstrap_text,
                channel=ctx.source_channel or "telegram",
                attachments=bootstrap_attachments,
            )
        else:
            ctx.log_fn(
                "event=amocrm_chat_bootstrap_skipped tenant=%s lead_id=%s reason=no_message_text"
                % (ctx.tenant_id, ctx.lead_id)
            )
    except Exception as exc:
        ctx.log_fn("event=amocrm_chat_bootstrap_failed tenant=%s lead_id=%s error=%s" % (ctx.tenant_id, ctx.lead_id, exc))


async def _create_lead_bootstrap_text(ctx: AmoCrmCreateLeadContext) -> str:
    bootstrap_text = str(ctx.payload.get("bootstrap_text") or "").strip()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if bootstrap_text or not fetchrow:
        return bootstrap_text
    row = await fetchrow(
        """
        SELECT text
        FROM messages
        WHERE tenant_id = $1
          AND lead_id = $2
          AND is_bot = TRUE
          AND text IS NOT NULL
          AND btrim(text) <> ''
        ORDER BY id DESC
        LIMIT 1
        """,
        ctx.tenant_id,
        ctx.lead_id,
    )
    if isinstance(row, Mapping):
        return str(row.get("text") or "").strip()
    if row:
        try:
            return str(dict(row).get("text") or "").strip()
        except Exception:
            return ""
    return ""


async def _update_create_lead_stage_index(ctx: AmoCrmCreateLeadContext) -> None:
    stage_index_val = _optional_int(ctx.payload.get("stage_index"))
    if stage_index_val is None:
        return
    await crm_links.update_stage_index(
        ctx.tenant_id,
        ctx.lead_id,
        amocrm_service.AMOCRM_PROVIDER,
        stage_index_val,
        pipeline_id=int(ctx.pipeline_id) if ctx.pipeline_id else None,
    )


async def _handle_chat_sync_message_event(
    *,
    tenant_id: int,
    lead_id: int,
    payload: Mapping[str, Any],
    cfg: Mapping[str, Any],
    client: Any,
    link: Mapping[str, Any] | None,
) -> None:
    ctx = AmoCrmChatSyncContext(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        payload=payload,
        cfg=cfg,
        client=client,
        link=link,
        provider_lead_id=link.get("provider_lead_id") if isinstance(link, Mapping) else None,
        provider_contact_id=link.get("provider_contact_id") if isinstance(link, Mapping) else None,
    )
    await _refresh_chat_sync_provider_ids(ctx)
    _ensure_chat_sync_link_exists(ctx)
    payload = await _canonical_chat_sync_payload(ctx)
    await _upsert_chat_sync_link(ctx, payload)
    await amocrm_chat_service.push_message(
        ctx.tenant_id,
        payload={
            **dict(payload),
            "tenant_id": ctx.tenant_id,
            "lead_id": ctx.lead_id,
            "amo_lead_id": int(ctx.provider_lead_id) if ctx.provider_lead_id is not None else None,
            "amo_contact_id": int(ctx.provider_contact_id) if ctx.provider_contact_id is not None else None,
        },
        cfg=ctx.cfg,
    )


async def _refresh_chat_sync_provider_ids(ctx: AmoCrmChatSyncContext) -> None:
    lead_exists = await amocrm_entity_exists(
        ctx.client,
        entity_type="lead",
        entity_id=int(ctx.provider_lead_id) if ctx.provider_lead_id is not None else None,
    )
    if lead_exists is False and ctx.provider_lead_id is not None:
        await crm_links.update_provider_lead_id(
            ctx.tenant_id,
            ctx.lead_id,
            amocrm_service.AMOCRM_PROVIDER,
            None,
        )
        ctx.provider_lead_id = None
    if ctx.provider_lead_id is not None and ctx.provider_contact_id is None:
        await _resolve_chat_sync_contact_from_lead(ctx)
    contact_exists = await amocrm_entity_exists(
        ctx.client,
        entity_type="contact",
        entity_id=int(ctx.provider_contact_id) if ctx.provider_contact_id is not None else None,
    )
    if contact_exists is False and ctx.provider_contact_id is not None:
        await crm_links.update_provider_contact_id(
            ctx.tenant_id,
            ctx.lead_id,
            amocrm_service.AMOCRM_PROVIDER,
            None,
        )
        ctx.provider_contact_id = None


async def _resolve_chat_sync_contact_from_lead(ctx: AmoCrmChatSyncContext) -> None:
    try:
        resolved_contact = await ctx.client.get_lead_contact_id(int(ctx.provider_lead_id))
    except Exception:
        resolved_contact = None
    if resolved_contact:
        ctx.provider_contact_id = int(resolved_contact)
        await crm_links.update_provider_contact_id(
            ctx.tenant_id,
            ctx.lead_id,
            amocrm_service.AMOCRM_PROVIDER,
            int(ctx.provider_contact_id),
        )


def _ensure_chat_sync_link_exists(ctx: AmoCrmChatSyncContext) -> None:
    if ctx.provider_lead_id is not None and ctx.provider_contact_id is not None:
        return
    direction = str(ctx.payload.get("direction") or "in").strip().lower()
    channel = str(ctx.payload.get("channel") or "").strip().lower()
    if channel in {"avito", "telegram"} and direction in {"in", "out"}:
        raise amocrm_integration.AmoCRMError("amocrm_chat_link_missing")


async def _canonical_chat_sync_payload(ctx: AmoCrmChatSyncContext) -> dict[str, Any]:
    external_chat_id = str(ctx.payload.get("external_chat_id") or "").strip()
    external_conversation_id = str(ctx.payload.get("external_conversation_id") or external_chat_id).strip()
    try:
        external_chat_id, external_conversation_id = await amocrm_chat_service._canonical_chat_identity(
            ctx.tenant_id,
            provider_lead_id=int(ctx.provider_lead_id) if ctx.provider_lead_id is not None else None,
            fallback_chat_id=external_chat_id,
            fallback_conversation_id=external_conversation_id,
        )
    except Exception:
        pass
    return {
        **dict(ctx.payload),
        "external_chat_id": external_chat_id,
        "external_conversation_id": external_conversation_id,
    }


async def _upsert_chat_sync_link(ctx: AmoCrmChatSyncContext, payload: Mapping[str, Any]) -> None:
    await crm_chat_links.upsert_link(
        ctx.tenant_id,
        ctx.lead_id,
        amocrm_chat_service.AMOCRM_CHAT_PROVIDER,
        external_chat_id=str(payload.get("external_chat_id") or ""),
        external_conversation_id=str(payload.get("external_conversation_id") or ""),
        external_contact_id=int(ctx.provider_contact_id) if ctx.provider_contact_id is not None else None,
        external_lead_id=int(ctx.provider_lead_id) if ctx.provider_lead_id is not None else None,
        chat_scope_id=str(payload.get("scope_id") or ""),
        source_id=str(payload.get("source_id") or ""),
    )


__all__ = [
    "AmoCrmOutboxDeps",
    "amocrm_backoff_seconds",
    "amocrm_stage_id_from_cfg",
    "amocrm_entity_exists",
    "handle_amocrm_event",
    "is_amocrm_lead_not_found_error",
    "parse_amocrm_payload",
    "process_amocrm_outbox",
    "recover_amocrm_missing_lead",
]
