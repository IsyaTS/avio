from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.responses import FileResponse, JSONResponse

from apps.api.web.services import client_avito_history_export_runtime


pytestmark = pytest.mark.unit


class FakeRequest:
    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self._payload = payload or {}

    async def json(self) -> dict[str, Any]:
        return self._payload


class FakeUuid:
    @staticmethod
    def uuid4():
        return SimpleNamespace(hex="job-1")


class FakeScheduler:
    def __init__(self) -> None:
        self.coroutines: list[Any] = []

    def __call__(self, coro: Any) -> SimpleNamespace:
        self.coroutines.append(coro)
        return SimpleNamespace(add_done_callback=lambda _callback: None)

    def close(self) -> None:
        for coro in self.coroutines:
            close = getattr(coro, "close", None)
            if callable(close):
                close()


class FakeRepo:
    def __init__(
        self,
        file_path: str | None = None,
        extra_file_path: str | None = None,
        training_file_path: str | None = None,
        review_file_path: str | None = None,
        summary_file_path: str | None = None,
        dialog_dataset_file_path: str | None = None,
        export_summary_path: str | None = None,
        contextual_file_path: str | None = None,
        review_cases_file_path: str | None = None,
        rejected_cases_summary_path: str | None = None,
        domain_schema_path: str | None = None,
        business_rules_draft_path: str | None = None,
    ) -> None:
        self.file_path = file_path
        self.extra_file_path = extra_file_path
        self.training_file_path = training_file_path
        self.review_file_path = review_file_path
        self.summary_file_path = summary_file_path
        self.dialog_dataset_file_path = dialog_dataset_file_path
        self.export_summary_path = export_summary_path
        self.contextual_file_path = contextual_file_path
        self.review_cases_file_path = review_cases_file_path
        self.rejected_cases_summary_path = rejected_cases_summary_path
        self.domain_schema_path = domain_schema_path
        self.business_rules_draft_path = business_rules_draft_path
        self.created: list[dict[str, Any]] = []
        self.claimed: list[str] = []
        self.progress: list[dict[str, Any]] = []
        self.finished: list[dict[str, Any]] = []
        self.reset_called = False
        self.queued_jobs: list[dict[str, Any]] = []
        self.claim_should_fail = False
        self.messages_writes = 0
        self.leads_writes = 0
        self.training_writes = 0

    async def create_job(self, **kwargs: Any) -> dict[str, Any]:
        self.created.append(kwargs)
        return {"status": "queued", **kwargs}

    async def claim_job(self, job_id: str) -> dict[str, Any] | None:
        if self.claim_should_fail:
            return None
        self.claimed.append(job_id)
        return {"tenant_id": 7, "job_id": job_id, "target_dialogs": 100, "status": "running"}

    async def update_progress(self, **kwargs: Any) -> dict[str, Any]:
        self.progress.append(kwargs)
        return {"tenant_id": 7, "target_dialogs": 100, "status": "running", **kwargs}

    async def finish_job(self, **kwargs: Any) -> dict[str, Any]:
        self.finished.append(kwargs)
        return {"tenant_id": 7, "target_dialogs": 100, **kwargs}

    async def get_job(self, tenant_id: int, job_id: str) -> dict[str, Any] | None:
        if tenant_id != 7 or job_id != "job-1":
            return None
        return {
            "tenant_id": tenant_id,
            "job_id": job_id,
            "status": "completed",
            "target_dialogs": 3,
            "candidates_seen": 5,
            "dialogs_accepted": 3,
            "dialogs_rejected": 2,
            "reject_reasons": {"system_only": 2},
            "file_path": self.file_path,
            "file_size": 12 if self.file_path else 0,
            "training_file_path": self.training_file_path,
            "training_file_size": 24 if self.training_file_path else 0,
            "training_examples_count": 2 if self.training_file_path else 0,
            "review_file_path": self.review_file_path,
            "review_file_size": 18 if self.review_file_path else 0,
            "review_examples_count": 1 if self.review_file_path else 0,
            "summary_file_path": self.summary_file_path,
            "summary_file_size": 16 if self.summary_file_path else 0,
            "dialog_dataset_file_path": self.dialog_dataset_file_path,
            "dialog_dataset_file_size": 44 if self.dialog_dataset_file_path else 0,
            "dialog_dataset_count": 3 if self.dialog_dataset_file_path else 0,
            "export_summary_path": self.export_summary_path,
            "export_summary_size": 30 if self.export_summary_path else 0,
            "export_pipeline_version": "dialog_level_v1" if self.dialog_dataset_file_path else None,
            "ai_schema_calls_count": 1 if self.domain_schema_path else 0,
            "legacy_contextual_enabled": bool(self.contextual_file_path),
            "checkpoint_path": None,
            "checkpoint_available": bool(self.dialog_dataset_file_path),
            "checkpoint_stage": "completed" if self.dialog_dataset_file_path else None,
            "contextual_file_path": self.contextual_file_path,
            "contextual_file_size": 48 if self.contextual_file_path else 0,
            "contextual_cases_count": 4 if self.contextual_file_path else 0,
            "review_cases_file_path": self.review_cases_file_path,
            "review_cases_file_size": 32 if self.review_cases_file_path else 0,
            "review_cases_count": 1 if self.review_cases_file_path else 0,
            "rejected_cases_summary_path": self.rejected_cases_summary_path,
            "rejected_cases_summary_size": 20 if self.rejected_cases_summary_path else 0,
            "domain_schema_path": self.domain_schema_path,
            "domain_schema_size": 28 if self.domain_schema_path else 0,
            "business_rules_draft_path": self.business_rules_draft_path,
            "business_rules_draft_size": 36 if self.business_rules_draft_path else 0,
            "domain_key": "lawn_mowing" if self.domain_schema_path else None,
            "domain_label": "покос травы" if self.domain_schema_path else None,
            "domain_slots_count": 3 if self.domain_schema_path else 0,
            "domain_schema_summary": {"domain": "lawn_mowing"} if self.domain_schema_path else {},
            "contextual_quality_summary": {"contextual_mode": "ai"} if self.contextual_file_path else {},
            "contextual_mode": "ai" if self.contextual_file_path else None,
            "ai_extracted_count": 4 if self.contextual_file_path else 0,
            "rule_fallback_count": 0,
            "context_bound_count": 2 if self.contextual_file_path else 0,
            "direct_example_count": 1 if self.contextual_file_path else 0,
            "clarify_first_count": 1 if self.contextual_file_path else 0,
            "style_only_count": 0,
            "review_count": 1 if self.review_cases_file_path else 0,
            "reject_count": 2 if self.rejected_cases_summary_path else 0,
            "rejected_examples_count": 3,
            "hard_rejected_count": 1,
            "ai_rejected_count": 1,
            "ai_reviewed_count": 2,
            "ai_failed_count": 0,
            "quality_summary": {"quality_mode": "ai"},
            "quality_mode": "ai",
            "api_errors_summary": {},
        }

    async def get_latest_file_job(self, tenant_id: int) -> dict[str, Any] | None:
        if tenant_id != 7 or not self.file_path:
            return None
        return await self.get_job(tenant_id, "job-1")

    async def get_active_job(self, tenant_id: int) -> dict[str, Any] | None:
        if tenant_id != 7:
            return None
        return None

    async def list_file_jobs(self, tenant_id: int) -> list[dict[str, Any]]:
        if tenant_id != 7 or not self.file_path:
            return []
        jobs = [await self.get_job(tenant_id, "job-1")]
        if self.extra_file_path:
            jobs.append(
                {
                    "tenant_id": tenant_id,
                    "job_id": "job-2",
                    "status": "partial",
                    "target_dialogs": 10,
                    "candidates_seen": 11,
                    "dialogs_accepted": 8,
                    "dialogs_rejected": 3,
                    "file_path": self.extra_file_path,
                    "file_size": 34,
                    "training_file_path": None,
                    "training_file_size": 0,
                    "training_examples_count": 0,
                    "review_file_path": None,
                    "review_file_size": 0,
                    "review_examples_count": 0,
                    "summary_file_path": None,
                    "summary_file_size": 0,
                    "dialog_dataset_file_path": None,
                    "dialog_dataset_file_size": 0,
                    "dialog_dataset_count": 0,
                    "export_summary_path": None,
                    "export_summary_size": 0,
                    "export_pipeline_version": None,
                    "ai_schema_calls_count": 0,
                    "legacy_contextual_enabled": False,
                    "checkpoint_path": None,
                    "checkpoint_available": False,
                    "checkpoint_stage": None,
                    "contextual_file_path": None,
                    "contextual_file_size": 0,
                    "contextual_cases_count": 0,
                    "review_cases_file_path": None,
                    "review_cases_file_size": 0,
                    "review_cases_count": 0,
                    "rejected_cases_summary_path": None,
                    "rejected_cases_summary_size": 0,
                    "domain_schema_path": None,
                    "domain_schema_size": 0,
                    "business_rules_draft_path": None,
                    "business_rules_draft_size": 0,
                    "domain_key": None,
                    "domain_label": None,
                    "domain_slots_count": 0,
                    "domain_schema_summary": {},
                    "contextual_quality_summary": {},
                    "contextual_mode": None,
                    "ai_extracted_count": 0,
                    "rule_fallback_count": 0,
                    "context_bound_count": 0,
                    "direct_example_count": 0,
                    "clarify_first_count": 0,
                    "style_only_count": 0,
                    "review_count": 0,
                    "reject_count": 0,
                    "rejected_examples_count": 0,
                    "hard_rejected_count": 0,
                    "ai_rejected_count": 0,
                    "ai_reviewed_count": 0,
                    "ai_failed_count": 0,
                    "quality_summary": {},
                    "quality_mode": None,
                    "api_errors_summary": {},
                }
            )
        return [job for job in jobs if job]

    async def delete_file_job(self, tenant_id: int, job_id: str) -> dict[str, Any] | None:
        row = await self.get_job(tenant_id, job_id)
        if not row:
            return None
        path = Path(str(row.get("file_path") or ""))
        if path.exists():
            path.unlink()
        training_path = Path(str(row.get("training_file_path") or ""))
        if training_path.exists():
            training_path.unlink()
        review_path = Path(str(row.get("review_file_path") or ""))
        if review_path.exists():
            review_path.unlink()
        summary_path = Path(str(row.get("summary_file_path") or ""))
        if summary_path.exists():
            summary_path.unlink()
        dialog_dataset_value = row.get("dialog_dataset_file_path")
        dialog_dataset_path = Path(str(dialog_dataset_value)) if dialog_dataset_value else None
        if dialog_dataset_path and dialog_dataset_path.exists():
            dialog_dataset_path.unlink()
        export_summary_value = row.get("export_summary_path")
        export_summary_path = Path(str(export_summary_value)) if export_summary_value else None
        if export_summary_path and export_summary_path.exists():
            export_summary_path.unlink()
        contextual_path = Path(str(row.get("contextual_file_path") or ""))
        if contextual_path.exists():
            contextual_path.unlink()
        review_cases_path = Path(str(row.get("review_cases_file_path") or ""))
        if review_cases_path.exists():
            review_cases_path.unlink()
        rejected_cases_summary_path = Path(str(row.get("rejected_cases_summary_path") or ""))
        if rejected_cases_summary_path.exists():
            rejected_cases_summary_path.unlink()
        domain_schema_path = Path(str(row.get("domain_schema_path") or ""))
        if domain_schema_path.exists():
            domain_schema_path.unlink()
        business_rules_draft_path = Path(str(row.get("business_rules_draft_path") or ""))
        if business_rules_draft_path.exists():
            business_rules_draft_path.unlink()
        return {
            **row,
            "status": "deleted",
            "file_path": None,
            "file_size": 0,
            "training_file_path": None,
            "training_file_size": 0,
            "training_examples_count": 0,
            "review_file_path": None,
            "review_file_size": 0,
            "review_examples_count": 0,
            "summary_file_path": None,
            "summary_file_size": 0,
            "dialog_dataset_file_path": None,
            "dialog_dataset_file_size": 0,
            "dialog_dataset_count": 0,
            "export_summary_path": None,
            "export_summary_size": 0,
            "export_pipeline_version": None,
            "ai_schema_calls_count": 0,
            "legacy_contextual_enabled": False,
            "checkpoint_path": None,
            "checkpoint_available": False,
            "checkpoint_stage": None,
            "contextual_file_path": None,
            "contextual_file_size": 0,
            "contextual_cases_count": 0,
            "review_cases_file_path": None,
            "review_cases_file_size": 0,
            "review_cases_count": 0,
            "rejected_cases_summary_path": None,
            "rejected_cases_summary_size": 0,
            "domain_schema_path": None,
            "domain_schema_size": 0,
            "business_rules_draft_path": None,
            "business_rules_draft_size": 0,
            "domain_key": None,
            "domain_label": None,
            "domain_slots_count": 0,
            "domain_schema_summary": {},
        }

    async def cancel_job(self, tenant_id: int, job_id: str) -> dict[str, Any] | None:
        if tenant_id != 7 or job_id != "job-1":
            return None
        return {
            "tenant_id": tenant_id,
            "job_id": job_id,
            "status": "cancelled",
            "target_dialogs": 3,
            "error_code": "cancelled",
            "api_errors_summary": {},
        }

    async def reset_interrupted_jobs(self) -> int:
        self.reset_called = True
        return 1

    async def list_queued_jobs(self, *, limit: int = 10) -> list[dict[str, Any]]:
        return self.queued_jobs[:limit]


class FakeExportModule:
    class AvitoHistoryExportDeps:
        def __init__(self, **kwargs: Any) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    @staticmethod
    async def run_export(*_args: Any, **kwargs: Any):
        export_deps = kwargs.get("deps")
        callback = getattr(export_deps, "progress_callback", None)
        if callback is not None:
            await callback(
                SimpleNamespace(
                    candidates_seen=2,
                    dialogs_accepted=1,
                    dialogs_rejected=1,
                    reject_reasons={"system_only": 1},
                    api_errors_summary={},
                    error_code=None,
                )
            )
        return SimpleNamespace(
            to_dict=lambda: {
                "status": "completed",
                "target_dialogs": kwargs["target_dialogs"],
                "candidates_seen": 4,
                "dialogs_accepted": kwargs["target_dialogs"],
                "dialogs_rejected": 1,
                "reject_reasons": {"system_only": 1},
                "file_path": "/tmp/export.md",
                "file_size": 128,
                "training_file_path": "/tmp/training_examples.jsonl",
                "training_file_size": 256,
                "training_examples_count": kwargs["target_dialogs"],
                "review_file_path": "/tmp/review_examples.jsonl",
                "review_file_size": 64,
                "review_examples_count": 1,
                "summary_file_path": "/tmp/rejected_examples_summary.json",
                "summary_file_size": 32,
                "dialog_dataset_file_path": "/tmp/dialog_dataset.jsonl",
                "dialog_dataset_file_size": 512,
                "dialog_dataset_count": kwargs["target_dialogs"],
                "export_summary_path": "/tmp/export_summary.json",
                "export_summary_size": 40,
                "export_pipeline_version": "dialog_level_v1",
                "ai_schema_calls_count": 1,
                "legacy_contextual_enabled": False,
                "checkpoint_path": "/tmp/job-1_checkpoint.json",
                "checkpoint_available": True,
                "checkpoint_stage": "completed",
                "contextual_file_path": None,
                "contextual_file_size": 0,
                "contextual_cases_count": 0,
                "review_cases_file_path": None,
                "review_cases_file_size": 0,
                "review_cases_count": 0,
                "rejected_cases_summary_path": None,
                "rejected_cases_summary_size": 0,
                "domain_schema_path": "/tmp/domain_schema.json",
                "domain_schema_size": 80,
                "business_rules_draft_path": "/tmp/business_rules_draft.json",
                "business_rules_draft_size": 96,
                "domain_key": "lawn_mowing",
                "domain_label": "покос травы",
                "domain_slots_count": 3,
                "domain_schema_summary": {"domain": "lawn_mowing"},
                "contextual_quality_summary": {"pipeline_version": "dialog_level_v1"},
                "contextual_mode": "schema_only",
                "ai_extracted_count": 0,
                "rule_fallback_count": 0,
                "context_bound_count": 1,
                "direct_example_count": 1,
                "clarify_first_count": 1,
                "style_only_count": 0,
                "review_count": 2,
                "reject_count": 1,
                "rejected_examples_count": 1,
                "hard_rejected_count": 0,
                "ai_rejected_count": 1,
                "ai_reviewed_count": 0,
                "ai_failed_count": 0,
                "quality_summary": {"estimated_ai_mode": "schema_only"},
                "quality_mode": "schema_only",
                "api_errors_summary": {},
                "error_code": None,
            }
        )


class FakeLogger:
    def exception(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def info(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class FakeCommon:
    def __init__(self, tenant_root: Path | None = None, config: dict[str, Any] | None = None) -> None:
        self.tenant_root = tenant_root
        self.config = config or {}
        self.writes: list[tuple[int, dict[str, Any]]] = []

    def tenant_dir(self, tenant_id: int) -> str:
        if self.tenant_root is None:
            raise RuntimeError("tenant_dir is not configured")
        path = self.tenant_root / str(tenant_id)
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    def read_tenant_config(self, _tenant_id: int) -> dict[str, Any]:
        return dict(self.config)

    def write_tenant_config(self, tenant_id: int, cfg: dict[str, Any]) -> None:
        self.config = cfg
        self.writes.append((tenant_id, cfg))


async def _auth(tenant: int) -> tuple[int, str]:
    return int(tenant), "key"


def _deps(repo: FakeRepo | None = None, task_scheduler: Any | None = None, common: Any | None = None):
    return client_avito_history_export_runtime.ClientAvitoHistoryExportDeps(
        authorize_client_settings_request_fn=lambda _request, tenant: _auth(tenant),
        export_service_module=FakeExportModule,
        export_repo_module=repo or FakeRepo(),
        common_module=common or SimpleNamespace(),
        avito_module=SimpleNamespace(),
        avito_api_module=SimpleNamespace(),
        logger=FakeLogger(),
        uuid_module=FakeUuid,
        task_scheduler=task_scheduler,
    )


@pytest.mark.asyncio
async def test_start_export_validates_min_max() -> None:
    too_small = await client_avito_history_export_runtime.start_export(
        7,
        FakeRequest({"target_dialogs": 0}),
        deps=_deps(),
    )
    too_large = await client_avito_history_export_runtime.start_export(
        7,
        FakeRequest({"target_dialogs": 10001}),
        deps=_deps(),
    )

    assert isinstance(too_small, JSONResponse)
    assert too_small.status_code == 400
    assert isinstance(too_large, JSONResponse)
    assert too_large.status_code == 400


@pytest.mark.asyncio
async def test_start_export_runs_and_returns_aggregates_only() -> None:
    repo = FakeRepo()

    result = await client_avito_history_export_runtime.start_export(
        7,
        FakeRequest({"target_dialogs": 3, "raw_text": "must not leak"}),
        deps=_deps(repo),
    )

    assert result["ok"] is True
    assert result["job"]["job_id"] == "job-1"
    assert result["job"]["dialogs_accepted"] == 3
    assert result["job"]["file_available"] is True
    assert result["job"]["dialog_dataset_file_available"] is True
    assert result["job"]["dialog_dataset_count"] == 3
    assert result["job"]["export_summary_file_available"] is True
    assert result["job"]["contextual_file_available"] is False
    assert result["job"]["contextual_cases_count"] == 0
    assert result["job"]["review_cases_file_available"] is False
    assert result["job"]["rejected_cases_summary_available"] is False
    assert result["job"]["domain_schema_file_available"] is True
    assert result["job"]["business_rules_draft_file_available"] is True
    assert result["job"]["domain_key"] == "lawn_mowing"
    assert result["job"]["domain_label"] == "покос травы"
    assert result["job"]["contextual_mode"] == "schema_only"
    assert result["job"]["export_pipeline_version"] == "dialog_level_v1"
    assert result["job"]["ai_schema_calls_count"] == 1
    assert result["job"]["checkpoint_available"] is True
    assert "must not leak" not in str(result)
    assert repo.created[0]["target_dialogs"] == 3
    assert repo.progress[0]["dialogs_accepted"] == 1
    assert repo.finished[0]["status"] == "completed"
    assert repo.messages_writes == 0
    assert repo.leads_writes == 0
    assert repo.training_writes == 0


@pytest.mark.asyncio
async def test_start_export_background_returns_queued_durable_job() -> None:
    repo = FakeRepo()
    scheduler = FakeScheduler()

    result = await client_avito_history_export_runtime.start_export(
        7,
        FakeRequest({"target_dialogs": 3}),
        deps=_deps(repo, task_scheduler=scheduler),
        background_tasks=SimpleNamespace(),  # type: ignore[arg-type]
    )

    assert result["ok"] is True
    assert result["job"]["status"] == "queued"
    assert repo.finished == []
    assert len(scheduler.coroutines) == 1
    scheduler.close()


@pytest.mark.asyncio
async def test_start_export_reuses_active_job_instead_of_creating_duplicate() -> None:
    class ActiveRepo(FakeRepo):
        async def get_active_job(self, tenant_id: int) -> dict[str, Any] | None:
            if tenant_id != 7:
                return None
            return {
                "tenant_id": 7,
                "job_id": "active-job",
                "status": "running",
                "target_dialogs": 500,
                "candidates_seen": 10,
                "dialogs_accepted": 4,
                "dialogs_rejected": 6,
                "api_errors_summary": {},
            }

    repo = ActiveRepo()
    scheduler = FakeScheduler()

    result = await client_avito_history_export_runtime.start_export(
        7,
        FakeRequest({"target_dialogs": 3}),
        deps=_deps(repo, task_scheduler=scheduler),
        background_tasks=SimpleNamespace(),  # type: ignore[arg-type]
    )

    assert result["ok"] is True
    assert result["job"]["job_id"] == "active-job"
    assert repo.created == []
    assert scheduler.coroutines == []


@pytest.mark.asyncio
async def test_start_export_claims_job_before_running_export() -> None:
    repo = FakeRepo()

    result = await client_avito_history_export_runtime.start_export(
        7,
        FakeRequest({"target_dialogs": 3}),
        deps=_deps(repo),
    )

    assert result["ok"] is True
    assert repo.claimed == ["job-1"]
    assert repo.finished[0]["status"] == "completed"


@pytest.mark.asyncio
async def test_duplicate_claim_skips_export() -> None:
    repo = FakeRepo()
    repo.claim_should_fail = True

    result = await client_avito_history_export_runtime.start_export(
        7,
        FakeRequest({"target_dialogs": 3}),
        deps=_deps(repo),
    )

    assert result["ok"] is True
    assert repo.progress == []
    assert repo.finished == []


@pytest.mark.asyncio
async def test_resume_pending_exports_requeues_and_schedules_jobs() -> None:
    repo = FakeRepo()
    repo.queued_jobs = [
        {"tenant_id": 7, "job_id": "job-1", "target_dialogs": 3, "status": "queued"},
        {"tenant_id": 0, "job_id": "bad", "target_dialogs": 3, "status": "queued"},
    ]
    scheduler = FakeScheduler()

    scheduled = await client_avito_history_export_runtime.resume_pending_exports(
        deps=_deps(repo, task_scheduler=scheduler),
    )

    assert repo.reset_called is True
    assert scheduled == 1
    assert len(scheduler.coroutines) == 1
    scheduler.close()


@pytest.mark.asyncio
async def test_get_active_export_returns_running_job() -> None:
    class ActiveRepo(FakeRepo):
        async def get_active_job(self, tenant_id: int) -> dict[str, Any] | None:
            return {
                "tenant_id": tenant_id,
                "job_id": "active-job",
                "status": "queued",
                "target_dialogs": 500,
                "api_errors_summary": {},
            }

    result = await client_avito_history_export_runtime.get_active_export(
        7,
        FakeRequest(),
        deps=_deps(ActiveRepo()),
    )

    assert result["ok"] is True
    assert result["job"]["job_id"] == "active-job"
    assert result["job"]["status"] == "queued"


@pytest.mark.asyncio
async def test_get_export_returns_status_without_raw_text(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    contextual_file_path = tmp_path / "contextual_cases.jsonl"
    file_path.write_text("Клиент: raw customer text", encoding="utf-8")
    contextual_file_path.write_text('{"text": "raw customer text"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.get_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), contextual_file_path=str(contextual_file_path))),
    )

    assert result["ok"] is True
    assert result["job"]["file_available"] is True
    assert result["job"]["file_name"] == "export.md"
    assert result["job"]["contextual_file_available"] is True
    assert result["job"]["contextual_file_name"] == "contextual_cases.jsonl"
    assert result["job"]["contextual_file_size"] == 48
    assert result["job"]["contextual_cases_count"] == 4
    assert result["job"]["contextual_mode"] == "ai"
    assert "raw customer text" not in str(result)
    assert "file_path" not in result["job"]
    assert "training_file_path" not in result["job"]
    assert "review_file_path" not in result["job"]
    assert "summary_file_path" not in result["job"]
    assert "contextual_file_path" not in result["job"]
    assert "review_cases_file_path" not in result["job"]
    assert "rejected_cases_summary_path" not in result["job"]
    assert "domain_schema_path" not in result["job"]
    assert "business_rules_draft_path" not in result["job"]


@pytest.mark.asyncio
async def test_get_latest_export_returns_last_file_without_raw_text(tmp_path: Path) -> None:
    file_path = tmp_path / "dialogs_3_20260522_job.md"
    file_path.write_text("Клиент: raw customer text", encoding="utf-8")

    result = await client_avito_history_export_runtime.get_latest_export(
        7,
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path))),
    )

    assert result["ok"] is True
    assert result["job"]["job_id"] == "job-1"
    assert result["job"]["file_available"] is True
    assert result["job"]["file_name"] == "dialogs_3_20260522_job.md"
    assert "raw customer text" not in str(result)
    assert "file_path" not in result["job"]


@pytest.mark.asyncio
async def test_get_latest_export_returns_null_when_missing() -> None:
    result = await client_avito_history_export_runtime.get_latest_export(
        7,
        FakeRequest(),
        deps=_deps(FakeRepo(None)),
    )

    assert result == {"ok": True, "job": None}


@pytest.mark.asyncio
async def test_list_exports_returns_all_files_without_raw_text(tmp_path: Path) -> None:
    file_path_1 = tmp_path / "dialogs_3_20260522_job1.md"
    file_path_2 = tmp_path / "dialogs_8_20260522_job2.md"
    file_path_1.write_text("Клиент: raw customer text 1", encoding="utf-8")
    file_path_2.write_text("Клиент: raw customer text 2", encoding="utf-8")

    result = await client_avito_history_export_runtime.list_exports(
        7,
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path_1), str(file_path_2))),
    )

    assert result["ok"] is True
    assert [job["job_id"] for job in result["jobs"]] == ["job-1", "job-2"]
    assert [job["file_name"] for job in result["jobs"]] == [
        "dialogs_3_20260522_job1.md",
        "dialogs_8_20260522_job2.md",
    ]
    assert "raw customer text" not in str(result)
    assert "file_path" not in str(result["jobs"])


@pytest.mark.asyncio
async def test_download_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    file_path.write_text("Клиент: hello", encoding="utf-8")

    result = await client_avito_history_export_runtime.download_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_training_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    training_file_path = tmp_path / "training_examples.jsonl"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    training_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_training_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), training_file_path=str(training_file_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_review_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    review_file_path = tmp_path / "review_examples.jsonl"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    review_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_review_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), review_file_path=str(review_file_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_summary_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    summary_file_path = tmp_path / "rejected_examples_summary.json"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    summary_file_path.write_text('{"rejected_examples_count":1}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_summary_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), summary_file_path=str(summary_file_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_dialog_dataset_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    dataset_path = tmp_path / "dialog_dataset.jsonl"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    dataset_path.write_text('{"schema_version":"avito_dialog_dataset_v1"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_dialog_dataset_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), dialog_dataset_file_path=str(dataset_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_activate_dialog_dataset_builds_index_and_persists_tenant_config(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    dataset_path = tmp_path / "dialog_dataset.jsonl"
    file_path.write_text("Клиент: hello\nМенеджер: hi", encoding="utf-8")
    dataset_path.write_text(
        (
            '{"schema_version":"avito_dialog_dataset_v1","dialog":['
            '{"role":"client","text":"Здравствуйте, нужна дверь в Уфе"},'
            '{"role":"manager","text":"Здравствуйте, какой размер проема?"}'
            ']}\n'
        ),
        encoding="utf-8",
    )
    common = FakeCommon(tmp_path / "tenants", {"learning": {"contextual_cases": {"apply_mode": True}}})

    result = await client_avito_history_export_runtime.activate_dialog_dataset_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), dialog_dataset_file_path=str(dataset_path)), common=common),
    )

    assert result["ok"] is True
    assert result["dialog_dataset"]["dialogs_count"] == 1
    assert result["dialog_dataset"]["source_job_id"] == "job-1"
    assert common.writes
    written = common.writes[-1][1]
    dialog_cfg = written["learning"]["dialog_dataset"]
    assert dialog_cfg["enabled"] is True
    assert dialog_cfg["source_job_id"] == "job-1"
    assert dialog_cfg["dialogs_count"] == 1
    assert not Path(dialog_cfg["index_path"]).is_absolute()
    assert written["learning"]["contextual_cases"]["apply_mode"] is False


@pytest.mark.asyncio
async def test_activate_dialog_dataset_returns_404_when_dataset_missing(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    common = FakeCommon(tmp_path / "tenants")

    result = await client_avito_history_export_runtime.activate_dialog_dataset_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), dialog_dataset_file_path=str(tmp_path / "missing.jsonl")), common=common),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404
    assert common.writes == []


@pytest.mark.asyncio
async def test_deactivate_dialog_dataset_disables_without_deleting_artifacts(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    dataset_path = tmp_path / "dialog_dataset.jsonl"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    dataset_path.write_text('{"schema_version":"avito_dialog_dataset_v1","dialog":[]}\n', encoding="utf-8")
    common = FakeCommon(
        tmp_path / "tenants",
        {
            "learning": {
                "dialog_dataset": {
                    "enabled": True,
                    "source_job_id": "job-1",
                    "dialogs_count": 3,
                    "index_sha1": "abc123",
                    "index_path": "indexes/dialog_training_abc123.pkl",
                }
            }
        },
    )

    result = await client_avito_history_export_runtime.deactivate_dialog_dataset_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), dialog_dataset_file_path=str(dataset_path)), common=common),
    )

    assert result["ok"] is True
    assert result["dialog_dataset"]["enabled"] is False
    assert common.writes[-1][1]["learning"]["dialog_dataset"]["enabled"] is False
    assert common.writes[-1][1]["learning"]["dialog_dataset"]["dialogs_count"] == 3
    assert file_path.exists()
    assert dataset_path.exists()


@pytest.mark.asyncio
async def test_deactivate_dialog_dataset_rejects_different_active_job(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    dataset_path = tmp_path / "dialog_dataset.jsonl"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    dataset_path.write_text('{"schema_version":"avito_dialog_dataset_v1","dialog":[]}\n', encoding="utf-8")
    common = FakeCommon(
        tmp_path / "tenants",
        {"learning": {"dialog_dataset": {"enabled": True, "source_job_id": "another-job"}}},
    )

    result = await client_avito_history_export_runtime.deactivate_dialog_dataset_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), dialog_dataset_file_path=str(dataset_path)), common=common),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 409
    assert common.writes == []


@pytest.mark.asyncio
async def test_list_exports_marks_active_dialog_dataset_without_absolute_paths(tmp_path: Path) -> None:
    file_path = tmp_path / "dialogs.md"
    dataset_path = tmp_path / "dialog_dataset.jsonl"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    dataset_path.write_text('{"schema_version":"avito_dialog_dataset_v1","dialog":[]}\n', encoding="utf-8")
    common = FakeCommon(
        tmp_path / "tenants",
        {
            "learning": {
                "dialog_dataset": {
                    "enabled": True,
                    "source_job_id": "job-1",
                    "dialogs_count": 123,
                    "index_sha1": "abc123",
                    "index_path": "/data/tenants/7/indexes/dialog_training_abc123.pkl",
                }
            }
        },
    )

    result = await client_avito_history_export_runtime.list_exports(
        7,
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), dialog_dataset_file_path=str(dataset_path)), common=common),
    )

    assert result["ok"] is True
    job = result["jobs"][0]
    assert job["dialog_dataset_active"] is True
    assert job["dialog_dataset_active_count"] == 123
    assert job["dialog_dataset_index_sha1"] == "abc123"
    assert "index_path" not in str(result)
    assert "/data/tenants/7/indexes" not in str(result)


@pytest.mark.asyncio
async def test_download_export_summary_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    summary_path = tmp_path / "export_summary.json"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    summary_path.write_text('{"estimated_ai_mode":"schema_only"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_export_summary(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), export_summary_path=str(summary_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_contextual_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    contextual_file_path = tmp_path / "contextual_cases.jsonl"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    contextual_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_contextual_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), contextual_file_path=str(contextual_file_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_review_cases_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    review_cases_file_path = tmp_path / "review_cases.jsonl"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    review_cases_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_review_cases_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), review_cases_file_path=str(review_cases_file_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_rejected_summary_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    rejected_summary_path = tmp_path / "rejected_cases_summary.json"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    rejected_summary_path.write_text('{"reject_count":1}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_rejected_summary_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), rejected_cases_summary_path=str(rejected_summary_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_domain_schema_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    domain_schema_path = tmp_path / "domain_schema.json"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    domain_schema_path.write_text('{"domain":"lawn_mowing"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_domain_schema_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), domain_schema_path=str(domain_schema_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_business_rules_draft_export_serves_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    rules_path = tmp_path / "business_rules_draft.json"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    rules_path.write_text('{"rules":[]}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_business_rules_draft_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path), business_rules_draft_path=str(rules_path))),
    )

    assert isinstance(result, FileResponse)


@pytest.mark.asyncio
async def test_download_export_rejects_cross_tenant() -> None:
    result = await client_avito_history_export_runtime.download_export(
        8,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo("/tmp/export.md")),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404


@pytest.mark.asyncio
async def test_download_training_export_rejects_cross_tenant(tmp_path: Path) -> None:
    training_file_path = tmp_path / "training_examples.jsonl"
    training_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_training_export(
        8,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo("/tmp/export.md", training_file_path=str(training_file_path))),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404


@pytest.mark.asyncio
async def test_download_review_export_rejects_cross_tenant(tmp_path: Path) -> None:
    review_file_path = tmp_path / "review_examples.jsonl"
    review_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_review_export(
        8,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo("/tmp/export.md", review_file_path=str(review_file_path))),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404


@pytest.mark.asyncio
async def test_download_contextual_export_rejects_cross_tenant(tmp_path: Path) -> None:
    contextual_file_path = tmp_path / "contextual_cases.jsonl"
    contextual_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_contextual_export(
        8,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo("/tmp/export.md", contextual_file_path=str(contextual_file_path))),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404


@pytest.mark.asyncio
async def test_download_domain_schema_export_rejects_cross_tenant(tmp_path: Path) -> None:
    domain_schema_path = tmp_path / "domain_schema.json"
    domain_schema_path.write_text('{"domain":"lawn_mowing"}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.download_domain_schema_export(
        8,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo("/tmp/export.md", domain_schema_path=str(domain_schema_path))),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404


@pytest.mark.asyncio
async def test_download_training_export_returns_404_when_missing(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    file_path.write_text("Клиент: hello", encoding="utf-8")

    result = await client_avito_history_export_runtime.download_training_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path))),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404


@pytest.mark.asyncio
async def test_download_contextual_export_returns_404_when_missing(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    file_path.write_text("Клиент: hello", encoding="utf-8")

    result = await client_avito_history_export_runtime.download_contextual_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path))),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404


@pytest.mark.asyncio
async def test_delete_export_removes_file_for_owning_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    training_file_path = tmp_path / "training_examples.jsonl"
    review_file_path = tmp_path / "review_examples.jsonl"
    summary_file_path = tmp_path / "rejected_examples_summary.json"
    contextual_file_path = tmp_path / "contextual_cases.jsonl"
    review_cases_file_path = tmp_path / "review_cases.jsonl"
    rejected_cases_summary_path = tmp_path / "rejected_cases_summary.json"
    domain_schema_path = tmp_path / "domain_schema.json"
    business_rules_draft_path = tmp_path / "business_rules_draft.json"
    file_path.write_text("Клиент: hello", encoding="utf-8")
    training_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")
    review_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")
    summary_file_path.write_text('{"rejected_examples_count":1}\n', encoding="utf-8")
    contextual_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")
    review_cases_file_path.write_text('{"source":"avito"}\n', encoding="utf-8")
    rejected_cases_summary_path.write_text('{"reject_count":1}\n', encoding="utf-8")
    domain_schema_path.write_text('{"domain":"lawn_mowing"}\n', encoding="utf-8")
    business_rules_draft_path.write_text('{"rules":[]}\n', encoding="utf-8")

    result = await client_avito_history_export_runtime.delete_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(
            FakeRepo(
                str(file_path),
                training_file_path=str(training_file_path),
                review_file_path=str(review_file_path),
                summary_file_path=str(summary_file_path),
                contextual_file_path=str(contextual_file_path),
                review_cases_file_path=str(review_cases_file_path),
                rejected_cases_summary_path=str(rejected_cases_summary_path),
                domain_schema_path=str(domain_schema_path),
                business_rules_draft_path=str(business_rules_draft_path),
            )
        ),
    )

    assert result == {"ok": True}
    assert not file_path.exists()
    assert not training_file_path.exists()
    assert not review_file_path.exists()
    assert not summary_file_path.exists()
    assert not contextual_file_path.exists()
    assert not review_cases_file_path.exists()
    assert not rejected_cases_summary_path.exists()
    assert not domain_schema_path.exists()
    assert not business_rules_draft_path.exists()


@pytest.mark.asyncio
async def test_delete_export_rejects_cross_tenant(tmp_path: Path) -> None:
    file_path = tmp_path / "export.md"
    file_path.write_text("Клиент: hello", encoding="utf-8")

    result = await client_avito_history_export_runtime.delete_export(
        8,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo(str(file_path))),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404
    assert file_path.exists()


@pytest.mark.asyncio
async def test_cancel_export_marks_running_job_cancelled() -> None:
    result = await client_avito_history_export_runtime.cancel_export(
        7,
        "job-1",
        FakeRequest(),
        deps=_deps(FakeRepo()),
    )

    assert result["ok"] is True
    assert result["job"]["status"] == "cancelled"
    assert result["job"]["error_code"] == "cancelled"
