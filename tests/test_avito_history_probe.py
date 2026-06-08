from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from libs.core.integrations.avito_analytics import AvitoAPIError
from libs.core.services import avito_history_probe


pytestmark = pytest.mark.unit


class FakeAvitoModule:
    def __init__(self, *, token_error: Exception | None = None) -> None:
        self.token_error = token_error

    async def ensure_access_token(self, tenant: int):
        if self.token_error:
            raise self.token_error
        return "token-main", {"account_id": tenant + 1000}


class FakeCommon:
    def __init__(self, cfg: dict[str, Any] | None = None) -> None:
        self.cfg = cfg or {}

    def ensure_tenant_files(self, _tenant: int) -> None:
        return None

    def read_tenant_config(self, _tenant: int) -> dict[str, Any]:
        return self.cfg


class FakeAvitoApi:
    def __init__(
        self,
        *,
        chats: list[dict[str, Any]] | None = None,
        messages: dict[str, list[dict[str, Any]]] | None = None,
        list_error: Exception | None = None,
        list_errors: list[Exception] | None = None,
        message_delay: float = 0,
        global_has_more: bool = False,
        items: list[dict[str, Any]] | None = None,
        item_chats: dict[str, list[dict[str, Any]]] | None = None,
        item_chat_error: Exception | None = None,
    ) -> None:
        self.chats = chats or []
        self.messages = messages or {}
        self.list_error = list_error
        self.list_errors = list(list_errors or [])
        self.message_delay = message_delay
        self.global_has_more = global_has_more
        self.items = items or []
        self.item_chats = item_chats or {}
        self.item_chat_error = item_chat_error
        self.active_message_requests = 0
        self.max_active_message_requests = 0
        self.list_limits: list[int] = []
        self.item_filtered_calls: list[tuple[int, tuple[str, ...]]] = []

    async def ensure_access_token(self, account_id: int):
        return f"token-analytics-{account_id}", SimpleNamespace(account_id=account_id)

    async def messenger_list_chats(
        self,
        *_args: Any,
        limit: int = 50,
        offset: int = 0,
        item_ids: list[str] | None = None,
    ):
        if self.list_errors:
            raise self.list_errors.pop(0)
        if self.list_error:
            raise self.list_error
        self.list_limits.append(limit)
        if item_ids:
            if self.item_chat_error:
                raise self.item_chat_error
            self.item_filtered_calls.append((offset, tuple(item_ids)))
            chats: list[dict[str, Any]] = []
            for item_id in item_ids:
                chats.extend(self.item_chats.get(str(item_id), []))
            return {"chats": chats[offset: offset + limit], "meta": {"has_more": False}}
        return {
            "chats": self.chats[offset: offset + limit],
            "meta": {"has_more": self.global_has_more},
        }

    async def list_items(self, _token: str, *, page: int = 1, per_page: int = 100):
        offset = (page - 1) * per_page
        return {"resources": self.items[offset: offset + per_page]}

    async def messenger_get_messages(
        self,
        _token: str,
        _account_id: int | None,
        chat_id: str,
        *,
        limit: int = 50,
        offset: int = 0,
    ):
        self.active_message_requests += 1
        self.max_active_message_requests = max(
            self.max_active_message_requests,
            self.active_message_requests,
        )
        if self.message_delay:
            await asyncio.sleep(self.message_delay)
        items = self.messages.get(chat_id, [])
        self.active_message_requests -= 1
        return {"messages": items[offset: offset + limit]}


def _deps(
    api: FakeAvitoApi,
    *,
    avito_module: FakeAvitoModule | None = None,
    common=None,
    progress_callback=None,
    chat_concurrency: int = 20,
):
    return avito_history_probe.AvitoHistoryProbeDeps(
        common_module=common or FakeCommon(),
        avito_module=avito_module or FakeAvitoModule(),
        avito_api_module=api,
        chat_page_limit=50,
        message_page_limit=50,
        chat_concurrency=chat_concurrency,
        progress_callback=progress_callback,
        rate_limit_backoff_seconds=0,
    )


@pytest.mark.asyncio
async def test_avito_history_probe_counts_only_aggregates() -> None:
    api = FakeAvitoApi(
        chats=[{"id": "chat-1"}, {"id": "chat-2"}],
        messages={
            "chat-1": [
                {
                    "created": "2026-05-01T10:00:00+00:00",
                    "text": "raw text must not matter",
                },
                {"created": "2026-04-01T10:00:00+00:00", "text": "outside period"},
            ],
            "chat-2": [{"created": 1770000000, "body": "also ignored"}],
        },
    )

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 31, 23, 59, tzinfo=timezone.utc),
        chat_limit=100,
        deps=_deps(api),
    )

    assert result.status == "completed"
    assert result.chats_seen == 2
    assert result.chats_with_messages == 2
    assert result.messages_seen == 3
    assert result.messages_in_period == 1
    assert result.oldest_message_at == datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
    assert result.newest_message_at == datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
    assert result.to_dict()["api_errors_summary"] == {}
    assert "raw text must not matter" not in str(result.to_dict())


@pytest.mark.asyncio
async def test_avito_history_probe_uses_analytics_connection_fallback() -> None:
    api = FakeAvitoApi(chats=[{"id": "chat-1"}], messages={"chat-1": []})
    common = FakeCommon({"integrations": {"avito_analytics": {"account_id": 55}}})

    result = await avito_history_probe.run_probe(
        3,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=100,
        deps=_deps(
            api,
            avito_module=FakeAvitoModule(token_error=RuntimeError("no main")),
            common=common,
        ),
    )

    assert result.status == "completed"
    assert result.chats_seen == 1


@pytest.mark.asyncio
async def test_avito_history_probe_no_connection() -> None:
    with pytest.raises(avito_history_probe.AvitoHistoryProbeError) as exc:
        await avito_history_probe.run_probe(
            3,
            period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
            period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
            chat_limit=100,
            deps=_deps(
                FakeAvitoApi(),
                avito_module=FakeAvitoModule(token_error=RuntimeError("not connected")),
                common=FakeCommon({}),
            ),
        )

    assert exc.value.code == "not_connected"


@pytest.mark.asyncio
async def test_avito_history_probe_maps_permission_error() -> None:
    api = FakeAvitoApi(list_error=AvitoAPIError("forbidden", status=403))

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=100,
        deps=_deps(api),
    )

    assert result.status == "failed"
    assert result.error_code == "no_permission"
    assert result.api_errors_summary == {"no_permission": 1}


@pytest.mark.asyncio
async def test_avito_history_probe_maps_rate_limit() -> None:
    api = FakeAvitoApi(
        list_error=AvitoAPIError("rate limited", status=429, retryable=True)
    )

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=100,
        deps=_deps(api),
    )

    assert result.status == "failed"
    assert result.error_code == "rate_limited"
    assert result.api_errors_summary == {"rate_limited": 1}


@pytest.mark.asyncio
async def test_avito_history_probe_recovers_from_transient_rate_limit() -> None:
    api = FakeAvitoApi(
        chats=[{"id": "chat-1"}],
        messages={"chat-1": []},
        list_errors=[AvitoAPIError("rate limited", status=429, retryable=True)],
    )

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=100,
        deps=avito_history_probe.AvitoHistoryProbeDeps(
            common_module=FakeCommon(),
            avito_module=FakeAvitoModule(),
            avito_api_module=api,
            rate_limit_backoff_seconds=0,
        ),
    )

    assert result.status == "completed"
    assert result.error_code is None
    assert result.api_errors_summary == {}
    assert result.chats_seen == 1


@pytest.mark.asyncio
async def test_avito_history_probe_maps_temporary_list_error() -> None:
    api = FakeAvitoApi(
        list_error=AvitoAPIError("server error", status=500, retryable=True)
    )

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=100,
        deps=_deps(api),
    )

    assert result.status == "failed"
    assert result.error_code == "temporary_error"
    assert result.api_errors_summary == {"temporary_error": 1}


@pytest.mark.asyncio
async def test_avito_history_probe_empty_history() -> None:
    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=100,
        deps=_deps(FakeAvitoApi(chats=[])),
    )

    assert result.status == "empty"
    assert result.messages_seen == 0


@pytest.mark.asyncio
async def test_avito_history_probe_allows_chat_limit_above_1000() -> None:
    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=2000,
        deps=_deps(FakeAvitoApi(chats=[{"id": "chat-1"}, {"id": "chat-2"}])),
    )

    assert result.status == "completed"
    assert result.chats_seen == 2


@pytest.mark.asyncio
async def test_avito_history_probe_publishes_running_progress() -> None:
    snapshots: list[tuple[str, int, int]] = []

    async def _progress(result):
        snapshots.append((result.status, result.chats_seen, result.messages_seen))

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=100,
        deps=_deps(
            FakeAvitoApi(
                chats=[{"id": "chat-1"}],
                messages={"chat-1": [{"created": "2026-05-01T10:00:00+00:00"}]},
            ),
            progress_callback=_progress,
        ),
    )

    assert result.status == "completed"
    assert ("running", 1, 1) in snapshots
    assert snapshots[-1] == ("completed", 1, 1)


@pytest.mark.asyncio
async def test_avito_history_probe_reads_chat_messages_concurrently() -> None:
    chats = [{"id": f"chat-{idx}"} for idx in range(10)]
    messages = {
        f"chat-{idx}": [{"created": "2026-05-01T10:00:00+00:00"}]
        for idx in range(10)
    }
    api = FakeAvitoApi(chats=chats, messages=messages, message_delay=0.01)

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=10,
        deps=_deps(api, chat_concurrency=5),
    )

    assert result.status == "completed"
    assert result.chats_seen == 10
    assert result.messages_seen == 10
    assert api.max_active_message_requests > 1
    assert api.max_active_message_requests <= 5


@pytest.mark.asyncio
async def test_avito_history_probe_defaults_to_larger_safe_batches() -> None:
    chats = [{"id": f"chat-{idx}"} for idx in range(180)]
    messages = {
        f"chat-{idx}": [{"created": "2026-05-01T10:00:00+00:00"}]
        for idx in range(180)
    }
    api = FakeAvitoApi(chats=chats, messages=messages, message_delay=0.01)

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=180,
        deps=avito_history_probe.AvitoHistoryProbeDeps(
            common_module=FakeCommon(),
            avito_module=FakeAvitoModule(),
            avito_api_module=api,
            rate_limit_backoff_seconds=0,
        ),
    )

    assert result.status == "completed"
    assert result.chats_seen == 180
    assert api.list_limits == [100, 80]
    assert api.max_active_message_requests > 1
    assert api.max_active_message_requests <= 50


@pytest.mark.asyncio
async def test_avito_history_probe_uses_item_filter_after_global_offset_cap() -> None:
    global_chats = [{"id": f"chat-{idx}"} for idx in range(1100)]
    item_chats = {
        "item-1": [{"id": "chat-1"}, {"id": "chat-extra"}],
    }
    api = FakeAvitoApi(
        chats=global_chats,
        items=[{"id": "item-1"}],
        item_chats=item_chats,
        global_has_more=True,
    )

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=1200,
        deps=avito_history_probe.AvitoHistoryProbeDeps(
            common_module=FakeCommon(),
            avito_module=FakeAvitoModule(),
            avito_api_module=api,
            rate_limit_backoff_seconds=0,
        ),
    )

    assert result.status == "completed"
    assert result.chats_seen == 1101
    assert api.item_filtered_calls == [(0, ("item-1",))]


@pytest.mark.asyncio
async def test_avito_history_probe_marks_item_filter_rate_limit() -> None:
    global_chats = [{"id": f"chat-{idx}"} for idx in range(1100)]
    api = FakeAvitoApi(
        chats=global_chats,
        items=[{"id": "item-1"}],
        global_has_more=True,
        item_chat_error=AvitoAPIError("rate limited", status=429),
    )

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=1200,
        deps=avito_history_probe.AvitoHistoryProbeDeps(
            common_module=FakeCommon(),
            avito_module=FakeAvitoModule(),
            avito_api_module=api,
            rate_limit_backoff_seconds=0,
        ),
    )

    assert result.status == "failed"
    assert result.error_code == "rate_limited"
    assert result.api_errors_summary["rate_limited"] == 1


@pytest.mark.asyncio
async def test_avito_history_probe_marks_message_rate_limit() -> None:
    class RateLimitedMessagesApi(FakeAvitoApi):
        async def messenger_get_messages(self, *_args: Any, **_kwargs: Any):
            raise AvitoAPIError("rate limited", status=429, retryable=True)

    api = RateLimitedMessagesApi(chats=[{"id": "chat-1"}])

    result = await avito_history_probe.run_probe(
        1,
        period_from=datetime(2026, 5, 1, tzinfo=timezone.utc),
        period_to=datetime(2026, 5, 2, tzinfo=timezone.utc),
        chat_limit=100,
        deps=avito_history_probe.AvitoHistoryProbeDeps(
            common_module=FakeCommon(),
            avito_module=FakeAvitoModule(),
            avito_api_module=api,
            rate_limit_backoff_seconds=0,
        ),
    )

    assert result.status == "failed"
    assert result.error_code == "rate_limited"
    assert result.api_errors_summary["rate_limited"] == 1


def test_avito_history_probe_final_status_respects_rate_limit_summary() -> None:
    result = avito_history_probe.AvitoHistoryProbeResult(
        status="running",
        chats_seen=10,
        api_errors_summary={"rate_limited": 1},
    )

    avito_history_probe._finalize_result_status(result)

    assert result.status == "failed"
    assert result.error_code == "rate_limited"
