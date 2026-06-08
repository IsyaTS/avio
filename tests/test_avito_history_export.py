from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from libs.core.integrations.avito_analytics import AvitoAPIError
from libs.core.services import avito_history_export


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
        global_has_more: bool = False,
        items: list[dict[str, Any]] | None = None,
        item_chats: dict[str, list[dict[str, Any]]] | None = None,
    ) -> None:
        self.chats = chats or []
        self.messages = messages or {}
        self.list_error = list_error
        self.list_errors = list(list_errors or [])
        self.global_has_more = global_has_more
        self.items = items or []
        self.item_chats = item_chats or {}
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
        if item_ids:
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
        items = self.messages.get(chat_id, [])
        page = items[offset: offset + limit]
        return {"messages": page, "meta": {"has_more": offset + len(page) < len(items)}}


def _good_messages(chat_id: str) -> list[dict[str, Any]]:
    return [
        {
            "created": "2026-05-01T10:00:00+00:00",
            "direction": "in",
            "text": f"Нужна дверь в квартиру {chat_id}",
        },
        {
            "created": "2026-05-01T10:01:00+00:00",
            "direction": "out",
            "text": "Здравствуйте. Подскажите размер проема?",
        },
    ]


def _deps(api: FakeAvitoApi, tmp_path: Path, **kwargs: Any) -> avito_history_export.AvitoHistoryExportDeps:
    return avito_history_export.AvitoHistoryExportDeps(
        common_module=kwargs.get("common") or FakeCommon(),
        avito_module=kwargs.get("avito_module") or FakeAvitoModule(),
        avito_api_module=api,
        export_root=str(tmp_path),
        chat_page_limit=kwargs.get("chat_page_limit", 50),
        message_page_limit=kwargs.get("message_page_limit", 50),
        chat_concurrency=kwargs.get("chat_concurrency", 10),
        rate_limit_backoff_seconds=0,
        max_candidates_multiplier=kwargs.get("max_candidates_multiplier", 4),
        cancel_callback=kwargs.get("cancel_callback"),
        contextual_ai_extractor_module=kwargs.get("contextual_ai_extractor_module", avito_history_export.avito_contextual_ai_extractor),
    )


@pytest.mark.asyncio
async def test_export_returns_exact_target_good_dialogs(tmp_path: Path) -> None:
    chats = [{"id": f"chat-{index}"} for index in range(120)]
    messages = {f"chat-{index}": _good_messages(str(index)) for index in range(120)}
    api = FakeAvitoApi(chats=chats, messages=messages, global_has_more=True)

    result = await avito_history_export.run_export(
        1,
        target_dialogs=100,
        job_id="job-1",
        deps=_deps(api, tmp_path),
    )

    assert result.status == "completed"
    assert result.dialogs_accepted == 100
    assert result.file_path
    assert result.dialog_dataset_file_path
    assert result.dialog_dataset_count == 100
    assert result.contextual_file_path is None
    assert result.contextual_cases_count == 0
    assert result.domain_schema_path
    assert result.business_rules_draft_path
    assert result.export_summary_path
    assert result.export_pipeline_version == "dialog_level_v1"
    assert result.legacy_contextual_enabled is False
    assert result.checkpoint_available is True
    assert result.training_file_path is None
    assert result.training_examples_count == 0
    assert Path(result.file_path).parent == tmp_path / "1" / "uploads" / "dialogs"
    assert Path(result.dialog_dataset_file_path).parent == tmp_path / "1" / "uploads" / "dialogs"
    assert Path(result.file_path).name.startswith("dialogs_100_")
    assert Path(result.dialog_dataset_file_path).name.startswith("dialog_dataset_100_")
    text = Path(result.file_path).read_text(encoding="utf-8")
    assert text.count("## Dialog ") == 100
    assert "Системное сообщение" not in text
    rows = [json.loads(line) for line in Path(result.dialog_dataset_file_path).read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 100
    assert rows[0]["source"] == "avito"
    assert rows[0]["schema_version"] == "avito_dialog_dataset_v1"
    assert rows[0]["dialog"][1]["role"] == "manager"


@pytest.mark.asyncio
async def test_export_partial_when_good_dialogs_less_than_target(tmp_path: Path) -> None:
    api = FakeAvitoApi(
        chats=[{"id": "good"}, {"id": "bad"}],
        messages={
            "good": _good_messages("good"),
            "bad": [{"created": "2026-05-01T10:00:00+00:00", "type": "system", "text": "Системное сообщение"}],
        },
    )

    result = await avito_history_export.run_export(
        1,
        target_dialogs=2,
        job_id="job-1",
        deps=_deps(api, tmp_path),
    )

    assert result.status == "partial"
    assert result.dialogs_accepted == 1
    assert result.reject_reasons == {"system_only": 1}
    assert result.file_path
    assert result.dialog_dataset_file_path
    assert result.dialog_dataset_count == 1
    assert result.contextual_file_path is None


@pytest.mark.asyncio
async def test_export_no_avito_connection(tmp_path: Path) -> None:
    with pytest.raises(avito_history_export.AvitoHistoryExportError) as exc:
        await avito_history_export.run_export(
            3,
            target_dialogs=1,
            job_id="job-1",
            deps=_deps(
                FakeAvitoApi(),
                tmp_path,
                avito_module=FakeAvitoModule(token_error=RuntimeError("not connected")),
                common=FakeCommon({}),
            ),
        )

    assert exc.value.code == "not_connected"


@pytest.mark.asyncio
async def test_export_maps_permission_error(tmp_path: Path) -> None:
    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(FakeAvitoApi(list_error=AvitoAPIError("forbidden", status=403)), tmp_path),
    )

    assert result.status == "failed"
    assert result.error_code == "no_permission"


@pytest.mark.asyncio
async def test_export_recovers_from_transient_rate_limit(tmp_path: Path) -> None:
    api = FakeAvitoApi(
        chats=[{"id": "chat-1"}],
        messages={"chat-1": _good_messages("1")},
        list_errors=[AvitoAPIError("rate limited", status=429, retryable=True)],
    )

    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(api, tmp_path),
    )

    assert result.status == "completed"
    assert result.api_errors_summary == {}


@pytest.mark.asyncio
async def test_export_marks_persistent_rate_limit(tmp_path: Path) -> None:
    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(
            FakeAvitoApi(list_error=AvitoAPIError("rate limited", status=429, retryable=True)),
            tmp_path,
        ),
    )

    assert result.status == "rate_limited"
    assert result.error_code == "rate_limited"


@pytest.mark.asyncio
async def test_export_stops_without_file_when_cancelled(tmp_path: Path) -> None:
    async def cancelled() -> bool:
        return True

    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(
            FakeAvitoApi(chats=[{"id": "chat-1"}], messages={"chat-1": _good_messages("1")}),
            tmp_path,
            cancel_callback=cancelled,
        ),
    )

    assert result.status == "cancelled"
    assert result.error_code == "cancelled"
    assert result.file_path is None
    assert result.training_file_path is None
    assert result.training_examples_count == 0
    assert result.contextual_file_path is None
    assert result.contextual_cases_count == 0
    assert not list(tmp_path.rglob("*.md"))
    assert not list(tmp_path.rglob("*.jsonl"))
    assert list(tmp_path.rglob("*_checkpoint.json"))


@pytest.mark.asyncio
async def test_export_records_temporary_error_without_failing_when_dialogs_available(tmp_path: Path) -> None:
    class OneBadChatApi(FakeAvitoApi):
        async def messenger_get_messages(self, *args: Any, **kwargs: Any):
            chat_id = args[2]
            if chat_id == "bad":
                raise AvitoAPIError("temporary", status=500, retryable=True)
            return await super().messenger_get_messages(*args, **kwargs)

    api = OneBadChatApi(
        chats=[{"id": "bad"}, {"id": "good"}],
        messages={"good": _good_messages("good")},
    )

    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(api, tmp_path),
    )

    assert result.status == "completed"
    assert result.dialogs_accepted == 1
    assert result.api_errors_summary == {"temporary_error": 1}
    assert result.error_code is None


@pytest.mark.asyncio
async def test_export_uses_item_ids_after_global_offset_cap(tmp_path: Path) -> None:
    chats = [{"id": f"global-{index}"} for index in range(1101)]
    messages = {f"global-{index}": [{"type": "system", "text": "Системное сообщение"}] for index in range(1101)}
    messages["item-chat"] = _good_messages("item")
    api = FakeAvitoApi(
        chats=chats,
        messages=messages,
        global_has_more=True,
        items=[{"id": "item-1"}],
        item_chats={"item-1": [{"id": "item-chat"}]},
    )

    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(api, tmp_path, chat_page_limit=100, max_candidates_multiplier=2000),
    )

    assert result.status == "completed"
    assert result.dialogs_accepted == 1
    assert api.item_filtered_calls


@pytest.mark.asyncio
async def test_export_does_not_call_app_table_writers(tmp_path: Path) -> None:
    class GuardedApi(FakeAvitoApi):
        async def write_message(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("messages writer must not be called")

        async def write_lead(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("leads writer must not be called")

        async def create_training_example(self, *_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("training writer must not be called")

    api = GuardedApi(chats=[{"id": "chat-1"}], messages={"chat-1": _good_messages("1")})

    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(api, tmp_path),
    )

    assert result.status == "completed"


@pytest.mark.asyncio
async def test_default_export_does_not_call_contextual_ai_extractor(tmp_path: Path) -> None:
    class ExplodingContextualAi:
        @staticmethod
        async def extract_cases(*_args: Any, **_kwargs: Any) -> None:
            raise AssertionError("default dialog-level export must not classify every candidate with AI")

        @staticmethod
        def build_default_extractor() -> object:
            return object()

    api = FakeAvitoApi(chats=[{"id": "chat-1"}], messages={"chat-1": _good_messages("1")})

    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(api, tmp_path, contextual_ai_extractor_module=ExplodingContextualAi),
    )

    assert result.status == "completed"
    assert result.dialog_dataset_count == 1
    assert result.contextual_cases_count == 0


@pytest.mark.asyncio
async def test_writer_omits_system_metadata_and_fake_roles(tmp_path: Path) -> None:
    api = FakeAvitoApi(
        chats=[{"id": "chat-1"}],
        messages={
            "chat-1": [
                {"created": "2026-05-01T09:00:00+00:00", "type": "system", "text": "Системное сообщение"},
                *(_good_messages("1")),
            ]
        },
    )

    result = await avito_history_export.run_export(
        1,
        target_dialogs=1,
        job_id="job-1",
        deps=_deps(api, tmp_path),
    )

    text = Path(result.file_path or "").read_text(encoding="utf-8")
    assert "Системное сообщение" not in text
    assert "chat-1" not in text
    assert "Клиент:" in text
    assert "Менеджер:" in text
    dataset_text = Path(result.dialog_dataset_file_path or "").read_text(encoding="utf-8")
    assert "Системное сообщение" not in dataset_text
    assert "chat-1" not in dataset_text
