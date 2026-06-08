from __future__ import annotations

import os
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from .webhook_incoming_runtime import ParsedIncoming


SyncFn = Callable[..., Any]
AsyncFn = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class CatalogFlowContext:
    request: Any
    tenant: int
    lead_id: int
    refer_id: int
    provider: str
    resolved_provider: str
    message_id: str | None
    text: str
    cfg: Mapping[str, Any] | None
    behavior: dict[str, object]
    whatsapp_phone: str
    peer_value: str | None
    peer_id: int | None
    telegram_user_id: int | None
    telegram_username: str
    avito_account_id: int | None
    avito_chat_id: str
    sender_jid_value: str | None


@dataclass(frozen=True)
class CatalogFlowResult:
    cfg: Mapping[str, Any] | None
    behavior: dict[str, object]
    response_payload: dict[str, Any] | None = None
    enqueue_incoming: bool = False
    auto_reply_handled: bool = False


@dataclass(frozen=True)
class CatalogFlowDeps:
    redis_queue: Any
    outbox_queue_key: str
    catalog_inline_limit_bytes: int
    catalog_sent_cache: dict[tuple[int, str], float]
    core_module: Any
    push_json_left_fn: AsyncFn
    push_json_right_fn: AsyncFn
    catalog_was_recently_sent_fn: AsyncFn
    mark_catalog_sent_fn: AsyncFn
    reset_catalog_cache_fn: AsyncFn
    catalog_message_mark_once_fn: AsyncFn
    resolve_catalog_attachment_fn: SyncFn
    build_public_catalog_url_fn: SyncFn
    assign_whatsapp_to_jid_fn: SyncFn
    user_requested_catalog_fn: SyncFn
    smart_reply_enabled_fn: SyncFn
    logger: Any


def catalog_context_from_parsed(
    request: Any,
    parsed: ParsedIncoming,
    *,
    tenant: int,
    lead_id: int | None = None,
    refer_id: int,
    sender_jid_value: str | None,
    peer_value: str | None = None,
) -> CatalogFlowContext:
    return CatalogFlowContext(
        request=request,
        tenant=tenant,
        lead_id=lead_id if lead_id is not None else parsed.lead_id,
        refer_id=refer_id,
        provider=parsed.resolved_provider,
        resolved_provider=parsed.resolved_provider,
        message_id=parsed.message_id,
        text=parsed.text,
        cfg=None,
        behavior={},
        whatsapp_phone=parsed.whatsapp_phone,
        peer_value=peer_value if peer_value is not None else parsed.peer_value,
        peer_id=parsed.peer_id,
        telegram_user_id=parsed.telegram_user_id,
        telegram_username=parsed.telegram_username,
        avito_account_id=parsed.avito_account_id,
        avito_chat_id=parsed.avito_chat_id,
        sender_jid_value=sender_jid_value,
    )


async def run_catalog_flow(
    ctx: CatalogFlowContext,
    *,
    deps: CatalogFlowDeps,
) -> CatalogFlowResult:
    cache_key = _catalog_cache_key(ctx)
    catalog_already_sent = await deps.catalog_was_recently_sent_fn(cache_key)
    cfg, behavior, attachment, caption = _load_catalog_settings(ctx, deps=deps)
    catalog_state = _catalog_state(
        ctx,
        deps=deps,
        cache_key=cache_key,
        catalog_already_sent=catalog_already_sent,
        attachment=attachment,
    )
    deps.logger.warning(
        "catalog_flow tenant=%s text=%r has_attachment=%s has_link=%s forced=%s already_sent=%s cache_key=%s",
        ctx.tenant,
        ctx.text,
        int(catalog_state.has_attachment),
        int(catalog_state.has_file_link),
        int(catalog_state.forced_catalog),
        int(catalog_already_sent),
        cache_key,
    )
    if catalog_state.price_question and not catalog_state.forced_catalog:
        catalog_state.should_send_catalog = False
    deps.logger.warning(
        "catalog_flow tenant=%s text=%r forced=%s already_sent=%s attachment=%s cache_hit=%s",
        ctx.tenant,
        ctx.text,
        int(catalog_state.forced_catalog),
        int(catalog_already_sent),
        isinstance(attachment, dict),
        bool(cache_key and cache_key in deps.catalog_sent_cache),
    )

    catalog_sent_now = False
    if catalog_state.should_send_catalog and (ctx.provider or "").lower() != "avito":
        catalog_sent_now = await _maybe_send_catalog(
            ctx,
            deps=deps,
            cache_key=cache_key,
            cfg=cfg,
            behavior=behavior,
            attachment=attachment,
            caption=caption,
            state=catalog_state,
        )
    if catalog_sent_now and (catalog_state.forced_catalog or not ctx.text):
        return CatalogFlowResult(
            cfg=cfg,
            behavior=behavior,
            response_payload={"queued": True, "leadId": ctx.lead_id},
            enqueue_incoming=True,
        )
    if catalog_state.price_question and not catalog_sent_now:
        if ctx.resolved_provider == "avito":
            return CatalogFlowResult(
                cfg=cfg,
                behavior=behavior,
                response_payload={"queued": True, "leadId": ctx.lead_id},
                enqueue_incoming=True,
            )
        price_sent = await _maybe_send_price_reply(ctx, deps=deps, cfg=cfg)
        if price_sent:
            return CatalogFlowResult(
                cfg=cfg,
                behavior=behavior,
                response_payload={"queued": True, "leadId": ctx.lead_id},
                enqueue_incoming=True,
                auto_reply_handled=True,
            )
    await _maybe_send_catalog_pages(ctx, deps=deps, cfg=cfg, cache_key=cache_key)
    return CatalogFlowResult(cfg=cfg, behavior=behavior or {})


async def run_catalog_flow_from_parsed(
    request: Any,
    parsed: ParsedIncoming,
    *,
    tenant: int,
    lead_id: int,
    refer_id: int,
    sender_jid_value: str | None,
    peer_value: str | None,
    deps: CatalogFlowDeps,
) -> CatalogFlowResult:
    return await run_catalog_flow(
        catalog_context_from_parsed(
            request,
            parsed,
            tenant=tenant,
            lead_id=lead_id,
            refer_id=refer_id,
            sender_jid_value=sender_jid_value,
            peer_value=peer_value,
        ),
        deps=deps,
    )


@dataclass
class _CatalogState:
    attachment_size: int
    attachment_mtime: int
    file_url: str
    use_file_link: bool
    forced_catalog: bool
    price_question: bool
    has_attachment: bool
    has_file_link: bool
    should_send_catalog: bool


def _catalog_cache_key(ctx: CatalogFlowContext) -> tuple[int, str] | None:
    if ctx.provider == "telegram":
        if ctx.telegram_user_id:
            return (ctx.tenant, f"tg:{ctx.telegram_user_id}")
        if ctx.peer_value:
            return (ctx.tenant, f"tg:peer:{ctx.peer_value}")
        if ctx.telegram_username:
            return (ctx.tenant, f"tg:user:{ctx.telegram_username.lower()}")
    elif ctx.whatsapp_phone:
        return (ctx.tenant, ctx.whatsapp_phone)
    if ctx.lead_id:
        return (ctx.tenant, f"lead:{ctx.lead_id}")
    return None


def _load_catalog_settings(
    ctx: CatalogFlowContext,
    *,
    deps: CatalogFlowDeps,
) -> tuple[Mapping[str, Any] | None, dict[str, object], Any, str]:
    cfg = None
    behavior: dict[str, object] = {}
    attachment, caption = None, ""
    try:
        cfg = deps.core_module.load_tenant(ctx.tenant)
        if isinstance(cfg, dict):
            raw_behavior = cfg.get("behavior")
            if isinstance(raw_behavior, dict):
                behavior = raw_behavior
        attachment, caption = deps.resolve_catalog_attachment_fn(cfg, ctx.tenant, ctx.request)
    except Exception:
        cfg = None
        behavior = {}
        attachment, caption = None, ""
    return cfg, behavior, attachment, caption


def _catalog_state(
    ctx: CatalogFlowContext,
    *,
    deps: CatalogFlowDeps,
    cache_key: tuple[int, str] | None,
    catalog_already_sent: bool,
    attachment: Any,
) -> _CatalogState:
    attachment_size, attachment_mtime = _attachment_stats(attachment)
    file_url = deps.build_public_catalog_url_fn(ctx.tenant, attachment_mtime, ctx.request)
    if ctx.resolved_provider == "telegram":
        file_url = ""
    use_file_link = False
    if ctx.resolved_provider in {"whatsapp"} and file_url:
        use_file_link = True
    elif (
        file_url
        and attachment_size
        and deps.catalog_inline_limit_bytes
        and attachment_size > deps.catalog_inline_limit_bytes
    ):
        use_file_link = True
    lowered_text = ctx.text.lower() if isinstance(ctx.text, str) else ""
    forced_catalog = bool(ctx.text and deps.user_requested_catalog_fn(ctx.text))
    price_question = any(
        token in lowered_text
        for token in ("сколько стоит", "цена", "стоимость", "ценник", "почем", "почём", "прайс на")
    )
    has_attachment = bool(attachment)
    has_file_link = bool(file_url)
    should_send_catalog = (has_attachment or has_file_link) and (
        forced_catalog or not catalog_already_sent
    )
    return _CatalogState(
        attachment_size=attachment_size,
        attachment_mtime=attachment_mtime,
        file_url=file_url,
        use_file_link=use_file_link,
        forced_catalog=forced_catalog,
        price_question=price_question,
        has_attachment=has_attachment,
        has_file_link=has_file_link,
        should_send_catalog=should_send_catalog,
    )


def _attachment_stats(attachment: Any) -> tuple[int, int]:
    if not isinstance(attachment, Mapping):
        return 0, 0
    path_value = attachment.get("path")
    if not isinstance(path_value, str) or not path_value.strip():
        return 0, 0
    try:
        stat = pathlib.Path(path_value).stat()
        return stat.st_size, int(stat.st_mtime)
    except Exception:
        return 0, 0


async def _maybe_send_catalog(
    ctx: CatalogFlowContext,
    *,
    deps: CatalogFlowDeps,
    cache_key: tuple[int, str] | None,
    cfg: Mapping[str, Any] | None,
    behavior: Mapping[str, object],
    attachment: Any,
    caption: str,
    state: _CatalogState,
) -> bool:
    if state.forced_catalog and cache_key:
        await deps.reset_catalog_cache_fn(cache_key)
    if state.use_file_link:
        catalog_text = f"Каталог: {state.file_url}"
        attachment = None
        deps.logger.info(
            "catalog_file_link tenant=%s size_bytes=%s url=%s",
            ctx.tenant,
            state.attachment_size,
            state.file_url,
        )
    else:
        catalog_text = (caption or "Каталог во вложении (PDF).").strip()
    catalog_out = _base_outbox(ctx, text=catalog_text, attachments=[attachment] if attachment else [])
    should_send_catalog = state.should_send_catalog
    if ctx.resolved_provider == "telegram":
        send_catalog_first = _send_catalog_first(behavior)
        if not send_catalog_first:
            should_send_catalog = False
        _apply_telegram_target(ctx, catalog_out)
        if not catalog_out.get("telegram_user_id") and not catalog_out.get("peer"):
            should_send_catalog = False
    else:
        catalog_out["to"] = ctx.whatsapp_phone
        deps.assign_whatsapp_to_jid_fn(catalog_out, ctx.resolved_provider, ctx.sender_jid_value)
    if not should_send_catalog:
        return False
    dedup_ok = await deps.catalog_message_mark_once_fn(
        tenant=int(ctx.tenant),
        provider=str(ctx.resolved_provider or ""),
        lead_id=int(ctx.lead_id),
        message_id=str(ctx.message_id or ctx.lead_id),
    )
    if not dedup_ok:
        deps.logger.info(
            "catalog_flow_dedup_skip tenant=%s provider=%s lead_id=%s message_id=%s",
            ctx.tenant,
            ctx.resolved_provider,
            ctx.lead_id,
            ctx.message_id or ctx.lead_id,
        )
        return False
    queue_push_high = getattr(deps.redis_queue, "rpush", None)
    if callable(queue_push_high):
        await deps.push_json_right_fn(deps.redis_queue, deps.outbox_queue_key, catalog_out)
    else:
        await deps.push_json_left_fn(deps.redis_queue, deps.outbox_queue_key, catalog_out)
    await deps.mark_catalog_sent_fn(cache_key)
    try:
        deps.core_module.record_bot_reply(
            ctx.refer_id,
            ctx.tenant,
            ctx.provider,
            catalog_text,
            tenant_cfg=cfg,
        )
    except Exception:
        pass
    return True


async def _maybe_send_price_reply(
    ctx: CatalogFlowContext,
    *,
    deps: CatalogFlowDeps,
    cfg: Mapping[str, Any] | None,
) -> bool:
    if not deps.smart_reply_enabled_fn(ctx.tenant):
        return False
    try:
        catalog_matches = deps.core_module.search_catalog({}, limit=5, tenant=ctx.tenant, query=ctx.text or "")
    except Exception:
        catalog_matches = []
    if not catalog_matches:
        return False
    best = catalog_matches[0]
    if not _has_relevance(ctx.text or "", best):
        return False
    formatted_price = _format_price(best.get("price"))
    if not formatted_price:
        return False
    title_hint = str(best.get("title") or best.get("name") or "").strip()
    reply_price = title_hint or "Эта модель"
    reply_text = f"{reply_price} стоит {formatted_price} ₽."
    stock_value = best.get("stock")
    if stock_value not in (None, "", "0"):
        try:
            stock_int = int(str(stock_value).strip())
        except Exception:
            stock_int = None
        if stock_int is not None and stock_int > 0:
            reply_text += " В наличии."
    price_out = _base_outbox(ctx, text=reply_text, attachments=[])
    if ctx.resolved_provider == "telegram":
        _apply_telegram_target(ctx, price_out)
    else:
        _apply_chat_target(ctx, price_out, deps=deps)
    await deps.push_json_left_fn(deps.redis_queue, deps.outbox_queue_key, price_out)
    try:
        deps.core_module.record_bot_reply(
            ctx.refer_id,
            ctx.tenant,
            ctx.provider,
            reply_text,
            tenant_cfg=cfg,
        )
    except Exception:
        pass
    return True


async def _maybe_send_catalog_pages(
    ctx: CatalogFlowContext,
    *,
    deps: CatalogFlowDeps,
    cfg: Mapping[str, Any] | None,
    cache_key: tuple[int, str] | None,
) -> None:
    should_send_catalog_pages = False
    if not should_send_catalog_pages:
        return
    try:
        items = deps.core_module.read_all_catalog(cfg)
        pages = deps.core_module.paginate_catalog_text(
            items,
            cfg,
            int(os.getenv("CATALOG_PAGE_SIZE", "10")),
        )
    except Exception:
        pages = []
    if not pages:
        return
    for page in pages:
        page_text = str(page or "").strip()
        if not page_text:
            continue
        page_out = _base_outbox(ctx, text=page_text, attachments=[])
        if ctx.resolved_provider == "telegram":
            _apply_telegram_target(ctx, page_out)
            if not page_out.get("telegram_user_id") and not page_out.get("peer"):
                continue
        elif ctx.resolved_provider == "whatsapp":
            if not ctx.whatsapp_phone:
                continue
            page_out["to"] = ctx.whatsapp_phone
            deps.assign_whatsapp_to_jid_fn(page_out, ctx.resolved_provider, ctx.sender_jid_value)
        else:
            chat_target = ctx.avito_chat_id or ctx.peer_value or (
                str(ctx.peer_id) if ctx.peer_id is not None else ""
            )
            if not chat_target:
                continue
            page_out["peer"] = chat_target
            page_out["peer_id"] = chat_target
            page_out["chat_id"] = chat_target
            if ctx.avito_account_id is not None:
                page_out["account_id"] = ctx.avito_account_id
        await deps.push_json_left_fn(deps.redis_queue, deps.outbox_queue_key, page_out)
    await deps.mark_catalog_sent_fn(cache_key)


def _base_outbox(
    ctx: CatalogFlowContext,
    *,
    text: str,
    attachments: list[Any],
) -> dict[str, Any]:
    return {
        "lead_id": ctx.lead_id,
        "text": text,
        "provider": ctx.resolved_provider,
        "ch": ctx.resolved_provider,
        "tenant_id": int(ctx.tenant),
        "tenant": int(ctx.tenant),
        "message_id": ctx.message_id or str(ctx.lead_id),
        "attachments": attachments,
    }


def _send_catalog_first(behavior: Mapping[str, object]) -> bool:
    send_catalog_first = True
    raw_send_catalog_flag = behavior.get("send_catalog_on_first_message") if behavior else None
    if raw_send_catalog_flag is not None:
        try:
            send_catalog_first = bool(raw_send_catalog_flag)
        except Exception:
            send_catalog_first = True
    return send_catalog_first


def _apply_telegram_target(ctx: CatalogFlowContext, outbound: dict[str, Any]) -> None:
    if ctx.telegram_user_id:
        outbound["telegram_user_id"] = int(ctx.telegram_user_id)
    if ctx.peer_value:
        outbound["peer"] = ctx.peer_value
    if ctx.peer_id is not None:
        outbound["peer_id"] = int(ctx.peer_id)


def _apply_chat_target(
    ctx: CatalogFlowContext,
    outbound: dict[str, Any],
    *,
    deps: CatalogFlowDeps,
) -> None:
    chat_target = ctx.avito_chat_id or ctx.peer_value or (
        str(ctx.peer_id) if ctx.peer_id is not None else ""
    )
    if chat_target:
        outbound["peer"] = chat_target
        outbound["peer_id"] = chat_target
        outbound["chat_id"] = chat_target
    if ctx.avito_account_id is not None:
        outbound["account_id"] = ctx.avito_account_id
    if ctx.whatsapp_phone:
        outbound["to"] = ctx.whatsapp_phone
    deps.assign_whatsapp_to_jid_fn(outbound, ctx.resolved_provider, ctx.sender_jid_value)


def _tokenize(value: str) -> tuple[set[str], set[str]]:
    tokens = re.findall(r"[a-zA-Zа-яА-Я0-9]+", value.lower())
    letters = {tok for tok in tokens if len(tok) >= 3 and not tok.isdigit()}
    numbers = {tok for tok in tokens if tok.isdigit()}
    return letters, numbers


def _has_relevance(query_text: str, item_payload: Mapping[str, Any]) -> bool:
    letters_q, numbers_q = _tokenize(query_text)
    price_words = {"цена", "стоимость", "прайс", "сколько", "почем", "почём"}
    letters_q = {t for t in letters_q if t not in price_words}
    if not letters_q and not numbers_q:
        return False
    fields = [
        item_payload.get("title"),
        item_payload.get("name"),
        item_payload.get("sku"),
        item_payload.get("id"),
        item_payload.get("category"),
        item_payload.get("color"),
        item_payload.get("material"),
        item_payload.get("size"),
    ]
    item_text = " ".join([str(f) for f in fields if f])
    letters_i, numbers_i = _tokenize(item_text)
    return bool(letters_q & letters_i or numbers_q & numbers_i)


def _format_price(raw: Any) -> str | None:
    raw_text = str(raw or "").strip()
    if not raw_text:
        return None
    match = re.search(r"\d[\d\s.,]*", raw_text)
    digits = re.sub(r"\D", "", match.group(0)) if match else ""
    if not digits:
        return None
    if len(digits) > 9:
        return None
    try:
        value = int(digits)
    except Exception:
        return None
    if value <= 0:
        return None
    return f"{value:,}".replace(",", " ")
