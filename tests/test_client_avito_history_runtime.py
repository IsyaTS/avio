from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from apps.api.web.services import client_avito_history_runtime


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


class FakeBackgroundTasks:
    def __init__(self) -> None:
        self.tasks: list[tuple[Any, tuple[Any, ...], dict[str, Any]]] = []

    def add_task(self, func: Any, *args: Any, **kwargs: Any) -> None:
        self.tasks.append((func, args, kwargs))


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
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.progress: list[dict[str, Any]] = []
        self.finished: list[dict[str, Any]] = []
        self.messages_writes = 0
        self.leads_writes = 0
        self.training_writes = 0

    async def create_job(self, **kwargs: Any) -> dict[str, Any]:
        self.created.append(kwargs)
        return {"status": "running", **kwargs}

    async def finish_job(self, **kwargs: Any) -> dict[str, Any]:
        self.finished.append(kwargs)
        return {"tenant_id": 7, "chat_limit": 100, **kwargs}

    async def update_progress(self, **kwargs: Any) -> dict[str, Any]:
        self.progress.append(kwargs)
        return {"tenant_id": 7, "chat_limit": 100, "status": "running", **kwargs}

    async def get_job(self, tenant_id: int, job_id: str) -> dict[str, Any] | None:
        if job_id != "job-1":
            return None
        return {
            "tenant_id": tenant_id,
            "job_id": job_id,
            "status": "completed",
            "chat_limit": 100,
            "chats_seen": 1,
            "messages_seen": 2,
        }


class FakeProbeModule:
    class AvitoHistoryProbeDeps:
        def __init__(self, **kwargs: Any) -> None:
            self.kwargs = kwargs
            for key, value in kwargs.items():
                setattr(self, key, value)

    @staticmethod
    async def run_probe(*_args: Any, **kwargs: Any):
        probe_deps = kwargs.get("deps")
        callback = getattr(probe_deps, "progress_callback", None)
        if callback is not None:
            await callback(
                SimpleNamespace(
                    chats_seen=1,
                    chats_with_messages=1,
                    messages_seen=2,
                    messages_in_period=1,
                    oldest_message_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
                    newest_message_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
                    api_errors_summary={},
                    error_code=None,
                )
            )
        return SimpleNamespace(
            to_dict=lambda: {
                "status": "completed",
                "chats_seen": 2,
                "chats_with_messages": 1,
                "messages_seen": 3,
                "messages_in_period": 2,
                "oldest_message_at": datetime(2026, 5, 1, tzinfo=timezone.utc),
                "newest_message_at": datetime(2026, 5, 2, tzinfo=timezone.utc),
                "api_errors_summary": {},
                "error_code": None,
            }
        )


class FakeLogger:
    def exception(self, *_args: Any, **_kwargs: Any) -> None:
        return None


async def _auth(tenant: int) -> tuple[int, str]:
    return int(tenant), "key"


def _deps(repo: FakeRepo | None = None, task_scheduler: Any | None = None):
    return client_avito_history_runtime.ClientAvitoHistoryDeps(
        authorize_client_settings_request_fn=lambda _request, tenant: _auth(tenant),
        probe_service_module=FakeProbeModule,
        probe_repo_module=repo or FakeRepo(),
        common_module=SimpleNamespace(),
        avito_module=SimpleNamespace(),
        avito_api_module=SimpleNamespace(),
        logger=FakeLogger(),
        uuid_module=FakeUuid,
        task_scheduler=task_scheduler,
    )


@pytest.mark.asyncio
async def test_start_probe_saves_only_aggregate_job() -> None:
    repo = FakeRepo()

    result = await client_avito_history_runtime.start_probe(
        7,
        FakeRequest(
            {
                "period_from": "2026-05-01",
                "period_to": "2026-05-10",
                "chat_limit": 500,
            }
        ),
        deps=_deps(repo),
    )

    assert result["ok"] is True
    assert result["job"]["job_id"] == "job-1"
    assert result["job"]["messages_in_period"] == 2
    assert repo.created[0]["tenant_id"] == 7
    assert repo.created[0]["chat_limit"] == 500
    assert repo.progress[0]["chats_seen"] == 1
    assert repo.progress[0]["messages_seen"] == 2
    assert repo.finished[0]["status"] == "completed"
    assert repo.messages_writes == 0
    assert repo.leads_writes == 0
    assert repo.training_writes == 0


@pytest.mark.asyncio
async def test_start_probe_with_background_tasks_returns_running_job() -> None:
    repo = FakeRepo()
    background_tasks = FakeBackgroundTasks()
    scheduler = FakeScheduler()

    result = await client_avito_history_runtime.start_probe(
        7,
        FakeRequest(
            {
                "period_from": "2026-05-01",
                "period_to": "2026-05-10",
                "chat_limit": 500,
            }
        ),
        deps=_deps(repo, task_scheduler=scheduler),
        background_tasks=background_tasks,  # type: ignore[arg-type]
    )

    assert result["ok"] is True
    assert result["job"]["job_id"] == "job-1"
    assert result["job"]["status"] == "running"
    assert repo.created[0]["tenant_id"] == 7
    assert repo.finished == []
    assert len(background_tasks.tasks) == 0
    assert len(scheduler.coroutines) == 1
    scheduler.close()


@pytest.mark.asyncio
async def test_start_probe_rejects_invalid_period() -> None:
    result = await client_avito_history_runtime.start_probe(
        7,
        FakeRequest({"period_from": "2026-05-10", "period_to": "2026-05-01"}),
        deps=_deps(),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 400


@pytest.mark.asyncio
async def test_start_probe_clamps_chat_limit_to_10000() -> None:
    repo = FakeRepo()

    result = await client_avito_history_runtime.start_probe(
        7,
        FakeRequest(
            {
                "period_from": "2026-05-01",
                "period_to": "2026-05-10",
                "chat_limit": 20000,
            }
        ),
        deps=_deps(repo),
    )

    assert result["ok"] is True
    assert repo.created[0]["chat_limit"] == 10000


@pytest.mark.asyncio
async def test_get_probe_returns_404_for_missing_job() -> None:
    result = await client_avito_history_runtime.get_probe(
        7,
        "missing",
        FakeRequest(),
        deps=_deps(),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 404


def test_registered_probe_route_uses_runtime_validation(monkeypatch) -> None:
    app = FastAPI()
    repo = FakeRepo()

    def _fake_deps(*_args: Any, **_kwargs: Any):
        return _deps(repo)

    monkeypatch.setattr(client_avito_history_runtime, "build_default_deps", _fake_deps)
    client_avito_history_runtime.register_routes(
        app.router,
        lambda _request, tenant: _auth(tenant),
        SimpleNamespace(),
        FakeLogger(),
    )

    client = TestClient(app)
    response = client.post(
        "/client/7/avito/history/probe",
        json={"period_from": "2026-05-10", "period_to": "2026-05-01"},
    )

    assert response.status_code == 400
    assert response.json() == {"detail": "invalid_period"}
    assert repo.created == []
