from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from .webhook_incoming_runtime import ParsedIncoming


AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ManagerOutgoingContext:
    tenant: int
    lead_id: int
    provider: str
    message_id: str
    text: str
    attachments: list[dict[str, Any]]
    contact_value: str | None
    whatsapp_phone: str
    telegram_user_id: int | None
    telegram_username: str | None
    telegram_display_name: str | None
    max_user_id: int | None
    max_username: str | None
    max_display_name: str | None
    peer_id: int | None
    peer_value: str | None
    avito_user_id: int | None
    avito_login: str | None
    avito_chat_id: str | None
    avito_account_id: int | None
    avito_system_message: bool


@dataclass(frozen=True)
class ManagerOutgoingDeps:
    redis_queue: Any
    json_module: Any
    handoff_silence_key_fn: SyncFn
    handoff_silence_meta_key_fn: SyncFn
    handoff_silence_ttl_seconds: int
    upsert_lead_fn: AsyncFn
    resolve_or_create_contact_fn: AsyncFn
    link_lead_contact_fn: AsyncFn
    insert_message_out_fn: AsyncFn
    capture_manager_intervention_fn: AsyncFn
    amocrm_service_module: Any
    content_fingerprint_fn: SyncFn
    text_or_placeholder_fn: SyncFn
    has_contact_identifiers_fn: SyncFn
    ok_response_fn: SyncFn
    logger: Any


def manager_context_from_parsed(
    parsed: ParsedIncoming,
    *,
    tenant: int,
    lead_id: int | None = None,
) -> ManagerOutgoingContext:
    return ManagerOutgoingContext(
        tenant=int(tenant),
        lead_id=int(lead_id if lead_id is not None else parsed.lead_id),
        provider=parsed.resolved_provider,
        message_id=parsed.message_id,
        text=parsed.text,
        attachments=parsed.attachments,
        contact_value=parsed.contact_value,
        whatsapp_phone=parsed.whatsapp_phone,
        telegram_user_id=parsed.telegram_user_id,
        telegram_username=parsed.telegram_username,
        telegram_display_name=parsed.telegram_display_name,
        max_user_id=parsed.max_user_id,
        max_username=parsed.max_username,
        max_display_name=parsed.max_display_name,
        peer_id=parsed.peer_id,
        peer_value=parsed.peer_value,
        avito_user_id=parsed.avito_user_id,
        avito_login=parsed.avito_login,
        avito_chat_id=parsed.avito_chat_id,
        avito_account_id=parsed.avito_account_id,
        avito_system_message=parsed.avito_system_message,
    )


async def handle_manager_outgoing(
    ctx: ManagerOutgoingContext,
    *,
    deps: ManagerOutgoingDeps,
) -> Any:
    await _set_handoff_flags(ctx, deps=deps)
    await _upsert_manager_lead(ctx, deps=deps)
    await _link_manager_contact(ctx, deps=deps)
    skip_amocrm_echo = await _should_skip_amocrm_echo(ctx, deps=deps)
    if (ctx.text or ctx.attachments) and not skip_amocrm_echo:
        await _store_manager_message(ctx, deps=deps)
    await _push_manager_to_amocrm(ctx, skip_amocrm_echo=skip_amocrm_echo, deps=deps)
    return deps.ok_response_fn({"queued": False, "smartReply": False, "handoff": True})


async def _set_handoff_flags(ctx: ManagerOutgoingContext, *, deps: ManagerOutgoingDeps) -> None:
    try:
        import time

        timestamp = int(time.time())
        await deps.redis_queue.set(
            deps.handoff_silence_key_fn(int(ctx.tenant), int(ctx.lead_id)),
            str(timestamp),
            ex=deps.handoff_silence_ttl_seconds,
        )
        meta_key = deps.handoff_silence_meta_key_fn(int(ctx.tenant), int(ctx.lead_id))
        if meta_key:
            payload = {"reason": "manager_outgoing", "ts": timestamp}
            await deps.redis_queue.set(
                meta_key,
                deps.json_module.dumps(payload, ensure_ascii=False),
                ex=deps.handoff_silence_ttl_seconds,
            )
    except Exception:
        deps.logger.debug(
            "handoff_flag_set_failed tenant=%s lead_id=%s",
            ctx.tenant,
            ctx.lead_id,
            exc_info=True,
        )


async def _upsert_manager_lead(ctx: ManagerOutgoingContext, *, deps: ManagerOutgoingDeps) -> None:
    try:
        upsert_kwargs: dict[str, Any] = {
            "channel": ctx.provider or "whatsapp",
            "tenant_id": ctx.tenant,
            "telegram_username": ctx.telegram_username,
            "peer_id": ctx.peer_id,
            "peer": ctx.peer_value,
        }
        if not (ctx.provider == "avito" and ctx.avito_system_message):
            upsert_kwargs["contact"] = ctx.contact_value
        else:
            deps.logger.info(
                "avito_system_message_skip_metadata_update tenant=%s lead_id=%s message_id=%s",
                ctx.tenant,
                ctx.lead_id,
                ctx.message_id or "",
            )
        _apply_title_fields(ctx, upsert_kwargs)
        resolved_lead = await deps.upsert_lead_fn(ctx.lead_id, **upsert_kwargs)
        if resolved_lead:
            try:
                object.__setattr__(ctx, "lead_id", int(resolved_lead))
            except Exception:
                pass
    except Exception:
        deps.logger.exception(
            "lead_upsert_err:db_error tenant=%s lead_id=%s manager_message_upsert_fail",
            ctx.tenant,
            ctx.lead_id,
        )


def _apply_title_fields(ctx: ManagerOutgoingContext, upsert_kwargs: dict[str, Any]) -> None:
    if ctx.telegram_user_id is not None:
        upsert_kwargs["telegram_user_id"] = int(ctx.telegram_user_id)
    if ctx.telegram_display_name:
        upsert_kwargs["title"] = ctx.telegram_display_name
    if ctx.provider in {"max", "max_personal"}:
        if ctx.max_display_name:
            upsert_kwargs["title"] = ctx.max_display_name
        elif ctx.max_username and not upsert_kwargs.get("title"):
            upsert_kwargs["title"] = ctx.max_username
    if ctx.provider == "avito":
        if ctx.avito_chat_id:
            upsert_kwargs["peer"] = ctx.avito_chat_id
        if ctx.avito_account_id is not None:
            upsert_kwargs["source_real_id"] = ctx.avito_account_id
        if ctx.avito_login and not upsert_kwargs.get("title") and not ctx.avito_system_message:
            upsert_kwargs["title"] = f"Avito · {ctx.avito_login}"


async def _link_manager_contact(ctx: ManagerOutgoingContext, *, deps: ManagerOutgoingDeps) -> None:
    try:
        if deps.has_contact_identifiers_fn(
            whatsapp_phone=ctx.whatsapp_phone or None,
            avito_user_id=ctx.avito_user_id,
            avito_login=ctx.avito_login,
            telegram_user_id=ctx.telegram_user_id,
            telegram_username=ctx.telegram_username,
            max_user_id=ctx.max_user_id,
            max_username=ctx.max_username,
        ):
            contact_id = await deps.resolve_or_create_contact_fn(
                tenant_id=ctx.tenant,
                whatsapp_phone=ctx.whatsapp_phone or None,
                avito_user_id=ctx.avito_user_id,
                avito_login=ctx.avito_login,
                telegram_user_id=ctx.telegram_user_id,
                telegram_username=ctx.telegram_username,
                max_user_id=ctx.max_user_id,
                max_username=ctx.max_username,
            )
        else:
            contact_id = 0
        if contact_id:
            await deps.link_lead_contact_fn(
                ctx.lead_id,
                contact_id,
                channel=ctx.provider or "whatsapp",
                peer=ctx.peer_value or "",
            )
    except Exception:
        deps.logger.debug(
            "manager_contact_link_failed tenant=%s lead_id=%s",
            ctx.tenant,
            ctx.lead_id,
            exc_info=True,
        )


async def _should_skip_amocrm_echo(ctx: ManagerOutgoingContext, *, deps: ManagerOutgoingDeps) -> bool:
    if ctx.provider != "telegram" or not ctx.text:
        return False
    try:
        echo_key = "amocrm:manager:echo:%s:%s:%s" % (
            int(ctx.tenant),
            int(ctx.lead_id),
            deps.content_fingerprint_fn(ctx.text, ctx.attachments),
        )
        return bool(await deps.redis_queue.get(echo_key))
    except Exception:
        return False


async def _store_manager_message(ctx: ManagerOutgoingContext, *, deps: ManagerOutgoingDeps) -> None:
    try:
        stored_text = deps.text_or_placeholder_fn(ctx.text, ctx.attachments)
        stored_id = await deps.insert_message_out_fn(
            ctx.lead_id,
            stored_text,
            provider_msg_id=ctx.message_id,
            status="sent",
            tenant_id=ctx.tenant,
            channel=ctx.provider,
            telegram_user_id=ctx.telegram_user_id,
            telegram_username=ctx.telegram_username,
            title=ctx.contact_value,
            is_bot=False,
            source="manager",
        )
        await deps.capture_manager_intervention_fn(
            tenant_id=int(ctx.tenant),
            lead_id=int(ctx.lead_id),
            channel=str(ctx.provider or "whatsapp"),
            manager_message_id=int(stored_id) if stored_id else None,
            source_event="provider_webhook_manager_outgoing",
        )
    except Exception:
        deps.logger.exception(
            "manager_message_store_failed tenant=%s lead_id=%s",
            ctx.tenant,
            ctx.lead_id,
        )


async def _push_manager_to_amocrm(
    ctx: ManagerOutgoingContext,
    *,
    skip_amocrm_echo: bool,
    deps: ManagerOutgoingDeps,
) -> None:
    try:
        if not skip_amocrm_echo:
            await deps.amocrm_service_module.amocrm_on_outbound_message(
                int(ctx.tenant),
                int(ctx.lead_id),
                text=ctx.text or "",
                channel=ctx.provider or "whatsapp",
                attachments=ctx.attachments,
                source_role="manager",
            )
    except Exception as exc:
        deps.logger.warning(
            "amocrm_outbound_failed tenant=%s lead_id=%s error=%s",
            ctx.tenant,
            ctx.lead_id,
            exc,
        )
