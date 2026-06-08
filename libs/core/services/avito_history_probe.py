from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Mapping

from libs.core.integrations import avito_analytics as avito_api

logger = logging.getLogger(__name__)


class AvitoHistoryProbeError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


@dataclass
class AvitoHistoryProbeResult:
    status: str
    chats_seen: int = 0
    chats_with_messages: int = 0
    messages_seen: int = 0
    messages_in_period: int = 0
    oldest_message_at: datetime | None = None
    newest_message_at: datetime | None = None
    api_errors_summary: dict[str, int] = field(default_factory=dict)
    error_code: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "chats_seen": self.chats_seen,
            "chats_with_messages": self.chats_with_messages,
            "messages_seen": self.messages_seen,
            "messages_in_period": self.messages_in_period,
            "oldest_message_at": self.oldest_message_at,
            "newest_message_at": self.newest_message_at,
            "api_errors_summary": dict(self.api_errors_summary),
            "error_code": self.error_code,
        }


@dataclass(frozen=True)
class AvitoHistoryProbeDeps:
    common_module: Any
    avito_module: Any
    avito_api_module: Any = avito_api
    analytics_tokens_repo: Any | None = None
    chat_page_limit: int = 100
    message_page_limit: int = 50
    max_message_pages_per_chat: int = 20
    chat_concurrency: int = 50
    item_page_limit: int = 100
    item_ids_per_chat_query: int = 10
    item_chat_filter_concurrency: int = 2
    rate_limit_retries: int = 2
    rate_limit_backoff_seconds: float = 60.0
    progress_callback: Callable[[AvitoHistoryProbeResult], Awaitable[None]] | None = None
    logger: Any = logger


async def run_probe(
    tenant_id: int,
    *,
    period_from: datetime,
    period_to: datetime,
    chat_limit: int,
    deps: AvitoHistoryProbeDeps,
) -> AvitoHistoryProbeResult:
    if period_from > period_to:
        raise AvitoHistoryProbeError(
            "invalid_period",
            "period_from must be before period_to",
        )

    token, account_id = await _resolve_access_token(int(tenant_id), deps)
    chat_limit = max(1, min(int(chat_limit or 100), 10000))
    chat_page_limit = max(1, min(int(deps.chat_page_limit or 100), 100))
    message_page_limit = max(1, min(int(deps.message_page_limit or 50), 100))
    max_message_pages = max(1, int(deps.max_message_pages_per_chat or 20))
    chat_concurrency = max(1, min(int(deps.chat_concurrency or 50), 100))
    item_page_limit = max(1, min(int(deps.item_page_limit or 100), 100))
    item_ids_per_chat_query = max(
        1,
        min(int(deps.item_ids_per_chat_query or 10), 10),
    )
    item_chat_filter_concurrency = max(
        1,
        min(int(deps.item_chat_filter_concurrency or 2), 10),
    )

    result = AvitoHistoryProbeResult(status="running")
    await _probe_chats_pipeline(
        result,
        token=token,
        account_id=account_id,
        chat_limit=chat_limit,
        chat_page_limit=chat_page_limit,
        item_page_limit=item_page_limit,
        item_ids_per_chat_query=item_ids_per_chat_query,
        item_chat_filter_concurrency=item_chat_filter_concurrency,
        period_from=period_from,
        period_to=period_to,
        message_page_limit=message_page_limit,
        max_message_pages=max_message_pages,
        chat_concurrency=chat_concurrency,
        deps=deps,
    )

    _finalize_result_status(result)
    await _publish_progress(result, deps)
    return result


async def _publish_progress(
    result: AvitoHistoryProbeResult,
    deps: AvitoHistoryProbeDeps,
) -> None:
    callback = deps.progress_callback
    if callback is None:
        return
    await callback(result)


async def _call_avito_with_rate_backoff(
    call: Callable[[], Awaitable[Any]],
    *,
    deps: AvitoHistoryProbeDeps,
) -> Any:
    retries = max(0, int(deps.rate_limit_retries or 0))
    delay = max(0.0, float(deps.rate_limit_backoff_seconds or 0.0))
    for attempt in range(retries + 1):
        try:
            return await call()
        except Exception as exc:
            if _error_code(exc) != "rate_limited" or attempt >= retries:
                raise
            if delay > 0:
                await asyncio.sleep(delay * (attempt + 1))


async def _resolve_access_token(
    tenant_id: int,
    deps: AvitoHistoryProbeDeps,
) -> tuple[str, int | None]:
    try:
        token, integration = await deps.avito_module.ensure_access_token(int(tenant_id))
        account_id = _coerce_int((integration or {}).get("account_id"))
        return str(token), account_id
    except Exception as primary_exc:
        account_id = _analytics_account_id(int(tenant_id), deps)
        if account_id is None:
            raise AvitoHistoryProbeError(
                "not_connected",
                "Avito account is not connected",
            ) from primary_exc
        try:
            token, _entry = await deps.avito_api_module.ensure_access_token(
                int(account_id)
            )
            return str(token), int(account_id)
        except Exception as exc:
            raise _map_token_error(exc) from exc


def _analytics_account_id(tenant_id: int, deps: AvitoHistoryProbeDeps) -> int | None:
    try:
        deps.common_module.ensure_tenant_files(int(tenant_id))
    except Exception:
        pass
    try:
        cfg = deps.common_module.read_tenant_config(int(tenant_id))
    except Exception:
        return None
    if not isinstance(cfg, Mapping):
        return None
    integrations = cfg.get("integrations")
    if not isinstance(integrations, Mapping):
        return None
    avito_cfg = integrations.get("avito_analytics")
    if not isinstance(avito_cfg, Mapping):
        return None
    return _coerce_int(avito_cfg.get("account_id"))


def _map_token_error(exc: Exception) -> AvitoHistoryProbeError:
    message = str(exc) or "Avito token unavailable"
    return AvitoHistoryProbeError("not_connected", message)


def _finalize_result_status(result: AvitoHistoryProbeResult) -> None:
    if result.error_code is None and result.api_errors_summary.get("rate_limited"):
        result.status = "failed"
        result.error_code = "rate_limited"
        return
    if result.error_code is None:
        result.status = "empty" if result.chats_seen <= 0 else "completed"


async def _probe_chats_pipeline(
    result: AvitoHistoryProbeResult,
    *,
    token: str,
    account_id: int | None,
    chat_limit: int,
    chat_page_limit: int,
    item_page_limit: int,
    item_ids_per_chat_query: int,
    item_chat_filter_concurrency: int,
    period_from: datetime,
    period_to: datetime,
    message_page_limit: int,
    max_message_pages: int,
    chat_concurrency: int,
    deps: AvitoHistoryProbeDeps,
) -> None:
    queue: asyncio.Queue[str | None] = asyncio.Queue(
        maxsize=max(chat_page_limit, chat_concurrency * 2)
    )
    seen_chat_ids: set[str] = set()
    workers = [
        asyncio.create_task(
            _message_worker_loop(
                queue,
                result,
                token=token,
                account_id=account_id,
                period_from=period_from,
                period_to=period_to,
                message_page_limit=message_page_limit,
                max_message_pages=max_message_pages,
                deps=deps,
            )
        )
        for _ in range(chat_concurrency)
    ]
    global_has_more = await _produce_chat_ids(
        queue,
        result,
        token=token,
        account_id=account_id,
        chat_limit=chat_limit,
        chat_page_limit=chat_page_limit,
        seen_chat_ids=seen_chat_ids,
        deps=deps,
    )
    if global_has_more and result.chats_seen < chat_limit:
        await _produce_item_filtered_chat_ids(
            queue,
            result,
            token=token,
            account_id=account_id,
            chat_limit=chat_limit,
            chat_page_limit=chat_page_limit,
            item_page_limit=item_page_limit,
            item_ids_per_chat_query=item_ids_per_chat_query,
            item_chat_filter_concurrency=item_chat_filter_concurrency,
            seen_chat_ids=seen_chat_ids,
            deps=deps,
        )
    for _ in workers:
        await queue.put(None)
    await queue.join()
    await asyncio.gather(*workers)
    await _publish_progress(result, deps)


async def _produce_chat_ids(
    queue: asyncio.Queue[str | None],
    result: AvitoHistoryProbeResult,
    *,
    token: str,
    account_id: int | None,
    chat_limit: int,
    chat_page_limit: int,
    seen_chat_ids: set[str],
    deps: AvitoHistoryProbeDeps,
) -> bool:
    offset = 0
    hit_offset_cap_with_more = False
    while result.chats_seen < chat_limit:
        page_limit = min(chat_page_limit, chat_limit - result.chats_seen)
        try:
            chats_payload = await _call_avito_with_rate_backoff(
                lambda: deps.avito_api_module.messenger_list_chats(
                    token,
                    account_id,
                    limit=page_limit,
                    offset=offset,
                ),
                deps=deps,
            )
        except Exception as exc:
            _record_api_error(result, exc)
            code = _error_code(exc)
            if code in {
                "unauthorized",
                "no_permission",
                "rate_limited",
                "temporary_error",
            }:
                result.status = "failed"
                result.error_code = code
            await _publish_progress(result, deps)
            break

        chats = _extract_items(
            chats_payload,
            keys=("chats", "items", "result", "data"),
        )
        has_more = _payload_has_more(chats_payload)
        if not chats:
            await _publish_progress(result, deps)
            break

        await _enqueue_chat_page(
            queue,
            result,
            chats=chats,
            chat_limit=chat_limit,
            seen_chat_ids=seen_chat_ids,
        )
        await _publish_progress(result, deps)
        if has_more and offset >= 1000:
            hit_offset_cap_with_more = True
            break
        if len(chats) < page_limit:
            break
        offset += len(chats)
    return hit_offset_cap_with_more


async def _produce_item_filtered_chat_ids(
    queue: asyncio.Queue[str | None],
    result: AvitoHistoryProbeResult,
    *,
    token: str,
    account_id: int | None,
    chat_limit: int,
    chat_page_limit: int,
    item_page_limit: int,
    item_ids_per_chat_query: int,
    item_chat_filter_concurrency: int,
    seen_chat_ids: set[str],
    deps: AvitoHistoryProbeDeps,
) -> None:
    page = 1
    semaphore = asyncio.Semaphore(item_chat_filter_concurrency)
    while result.chats_seen < chat_limit:
        try:
            items_payload = await _call_avito_with_rate_backoff(
                lambda: deps.avito_api_module.list_items(
                    token,
                    page=page,
                    per_page=item_page_limit,
                ),
                deps=deps,
            )
        except Exception as exc:
            _record_api_error(result, exc)
            break

        item_ids = _extract_item_ids(items_payload)
        if not item_ids:
            break
        tasks = [
            asyncio.create_task(
                _fetch_item_chat_chunk(
                    semaphore,
                    deps,
                    token=token,
                    account_id=account_id,
                    item_ids=chunk,
                    chat_page_limit=chat_page_limit,
                )
            )
            for chunk in _chunks(item_ids, item_ids_per_chat_query)
        ]
        for task in asyncio.as_completed(tasks):
            chats, exc = await task
            if chats:
                await _enqueue_chat_page(
                    queue,
                    result,
                    chats=chats,
                    chat_limit=chat_limit,
                    seen_chat_ids=seen_chat_ids,
                )
            if exc is not None:
                _record_api_error(result, exc)
                if _error_code(exc) in {"unauthorized", "no_permission", "rate_limited"}:
                    result.status = "failed"
                    result.error_code = _error_code(exc)
                    for pending in tasks:
                        if not pending.done():
                            pending.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    break
                continue
            if result.chats_seen >= chat_limit:
                for pending in tasks:
                    if not pending.done():
                        pending.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                break
        await _publish_progress(result, deps)
        if result.error_code is not None:
            break
        if len(item_ids) < item_page_limit:
            break
        page += 1


async def _fetch_item_chat_chunk(
    semaphore: asyncio.Semaphore,
    deps: AvitoHistoryProbeDeps,
    *,
    token: str,
    account_id: int | None,
    item_ids: list[str],
    chat_page_limit: int,
) -> tuple[list[Mapping[str, Any]], Exception | None]:
    async with semaphore:
        chats_collected: list[Mapping[str, Any]] = []
        offset = 0
        while offset <= 1000:
            try:
                chats_payload = await _call_avito_with_rate_backoff(
                    lambda: deps.avito_api_module.messenger_list_chats(
                        token,
                        account_id,
                        limit=chat_page_limit,
                        offset=offset,
                        item_ids=item_ids,
                    ),
                    deps=deps,
                )
            except Exception as exc:
                return chats_collected, exc
            chats = _extract_items(
                chats_payload,
                keys=("chats", "items", "result", "data"),
            )
            if not chats:
                break
            chats_collected.extend(chats)
            if len(chats) < chat_page_limit or not _payload_has_more(chats_payload):
                break
            offset += len(chats)
        return chats_collected, None

async def _enqueue_chat_page(
    queue: asyncio.Queue[str | None],
    result: AvitoHistoryProbeResult,
    *,
    chats: list[Mapping[str, Any]],
    chat_limit: int,
    seen_chat_ids: set[str],
) -> None:
    for chat in chats:
        if result.chats_seen >= chat_limit:
            break
        chat_id = _extract_chat_id(chat)
        if not chat_id:
            result.api_errors_summary["chat_id_missing"] = (
                result.api_errors_summary.get("chat_id_missing", 0) + 1
            )
            continue
        if chat_id in seen_chat_ids:
            continue
        seen_chat_ids.add(chat_id)
        result.chats_seen += 1
        await queue.put(chat_id)


async def _message_worker_loop(
    queue: asyncio.Queue[str | None],
    result: AvitoHistoryProbeResult,
    *,
    token: str,
    account_id: int | None,
    period_from: datetime,
    period_to: datetime,
    message_page_limit: int,
    max_message_pages: int,
    deps: AvitoHistoryProbeDeps,
) -> None:
    while True:
        chat_id = await queue.get()
        try:
            if chat_id is None:
                return
            await _probe_chat_messages(
                result,
                token=token,
                account_id=account_id,
                chat_id=chat_id,
                period_from=period_from,
                period_to=period_to,
                message_page_limit=message_page_limit,
                max_message_pages=max_message_pages,
                deps=deps,
            )
        finally:
            queue.task_done()


async def _probe_chat_messages(
    result: AvitoHistoryProbeResult,
    *,
    token: str,
    account_id: int | None,
    chat_id: str,
    period_from: datetime,
    period_to: datetime,
    message_page_limit: int,
    max_message_pages: int,
    deps: AvitoHistoryProbeDeps,
) -> None:
    offset = 0
    pages_seen = 0
    chat_has_messages = False
    while pages_seen < max_message_pages:
        pages_seen += 1
        try:
            payload = await _call_avito_with_rate_backoff(
                lambda: deps.avito_api_module.messenger_get_messages(
                    token,
                    account_id,
                    chat_id,
                    limit=message_page_limit,
                    offset=offset,
                ),
                deps=deps,
            )
        except Exception as exc:
            _record_api_error(result, exc)
            code = _error_code(exc)
            if code in {"unauthorized", "no_permission", "rate_limited"}:
                result.status = "failed"
                result.error_code = code
            break
        messages = _extract_items(payload, keys=("messages", "items", "result", "data"))
        if not messages:
            break
        chat_has_messages = True
        for message in messages:
            created_at = _message_datetime(message)
            if created_at is None:
                result.api_errors_summary["message_time_missing"] = (
                    result.api_errors_summary.get("message_time_missing", 0) + 1
                )
                continue
            result.messages_seen += 1
            if period_from <= created_at <= period_to:
                result.messages_in_period += 1
                result.oldest_message_at = _min_dt(result.oldest_message_at, created_at)
                result.newest_message_at = _max_dt(result.newest_message_at, created_at)
        if len(messages) < message_page_limit:
            break
        offset += len(messages)
    if chat_has_messages:
        result.chats_with_messages += 1


def _extract_items(payload: Any, *, keys: tuple[str, ...]) -> list[Mapping[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, Mapping)]
    if not isinstance(payload, Mapping):
        return []
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
        if isinstance(value, Mapping):
            nested = _extract_items(value, keys=keys)
            if nested:
                return nested
    return []


def _extract_chat_id(chat: Mapping[str, Any]) -> str:
    for key in ("id", "chat_id", "chatId"):
        value = chat.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _payload_has_more(payload: Any) -> bool:
    if not isinstance(payload, Mapping):
        return False
    meta = payload.get("meta")
    if isinstance(meta, Mapping) and isinstance(meta.get("has_more"), bool):
        return bool(meta.get("has_more"))
    for key in ("has_more", "hasMore"):
        value = payload.get(key)
        if isinstance(value, bool):
            return value
    return False


def _extract_item_ids(payload: Any) -> list[str]:
    items = _extract_items(payload, keys=("resources", "items", "result", "data"))
    item_ids: list[str] = []
    for item in items:
        for key in ("id", "item_id", "itemId"):
            value = item.get(key)
            if value is not None and str(value).strip():
                item_ids.append(str(value).strip())
                break
    return item_ids


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _message_datetime(message: Mapping[str, Any]) -> datetime | None:
    for key in (
        "created",
        "created_at",
        "createdAt",
        "created_at_ts",
        "timestamp",
        "ts",
        "date",
    ):
        value = message.get(key)
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw = raw / 1000.0
        dt = datetime.fromtimestamp(raw, tz=timezone.utc)
    elif isinstance(value, str) and value.strip():
        text = value.strip()
        if text.isdigit():
            return _parse_datetime(int(text))
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _record_api_error(result: AvitoHistoryProbeResult, exc: Exception) -> None:
    code = _error_code(exc)
    result.api_errors_summary[code] = result.api_errors_summary.get(code, 0) + 1


def _error_code(exc: Exception) -> str:
    status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
    try:
        status_int = int(status) if status is not None else None
    except Exception:
        status_int = None
    if status_int == 401:
        return "unauthorized"
    if status_int == 403:
        return "no_permission"
    if status_int == 429:
        return "rate_limited"
    if status_int == 404:
        return "not_found"
    if getattr(exc, "retryable", False):
        return "temporary_error"
    return "api_error"


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _min_dt(current: datetime | None, candidate: datetime) -> datetime:
    if current is None or candidate < current:
        return candidate
    return current


def _max_dt(current: datetime | None, candidate: datetime) -> datetime:
    if current is None or candidate > current:
        return candidate
    return current


__all__ = [
    "AvitoHistoryProbeDeps",
    "AvitoHistoryProbeError",
    "AvitoHistoryProbeResult",
    "run_probe",
]
