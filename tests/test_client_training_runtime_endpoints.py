from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.responses import JSONResponse

from apps.api.web.services import client_training_runtime


pytestmark = pytest.mark.unit


class FakeRequest:
    def __init__(self, payload: dict[str, Any] | None = None) -> None:
        self._payload = payload or {}
        self.query_params: dict[str, Any] = {}

    async def json(self) -> dict[str, Any]:
        return self._payload


class FakeDb:
    def __init__(self) -> None:
        self.activated: list[int] = []
        self.recorded: list[dict[str, Any]] = []

    async def record_training_example(self, tenant: int, **kwargs: Any) -> None:
        self.recorded.append({"tenant": tenant, **kwargs})


class FakeLogger:
    def __init__(self) -> None:
        self.exceptions: list[tuple[Any, ...]] = []

    def exception(self, *args: Any, **_kwargs: Any) -> None:
        self.exceptions.append(args)

    def info(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def error(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class FakeResponse:
    status_code = 200

    def json(self) -> dict[str, Any]:
        return {"items": [{"q_text": "q", "a_text": "a"}], "meta": {"source": "tg"}}


class FakeAsyncClient:
    calls: list[dict[str, Any]] = []

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout

    async def __aenter__(self) -> "FakeAsyncClient":
        return self

    async def __aexit__(self, *_args: Any) -> None:
        return None

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        json: dict[str, Any] | None,
    ) -> FakeResponse:
        self.calls.append({"method": method, "url": url, "headers": headers, "json": json})
        return FakeResponse()


class FakeHttpx:
    AsyncClient = FakeAsyncClient


def _deps(
    *,
    db: FakeDb | None = None,
    admin_token: str = "admin",
) -> client_training_runtime.ClientTrainingDeps:
    return client_training_runtime.ClientTrainingDeps(
        authorize_client_settings_request_fn=lambda _request, tenant: _auth(tenant),
        db_module=db or FakeDb(),
        settings_module=SimpleNamespace(ADMIN_TOKEN=admin_token, TGWORKER_BASE_URL="http://tgworker.test/"),
        logger=FakeLogger(),
        log_prefix="[training]",
        httpx_module=FakeHttpx,
    )


async def _auth(tenant: int) -> tuple[int, str]:
    return int(tenant), "key"


@pytest.mark.asyncio
async def test_training_tg_harvest_calls_tgworker_with_admin_token() -> None:
    FakeAsyncClient.calls = []

    result = await client_training_runtime.training_tg_harvest(
        3,
        FakeRequest({"limit_dialogs": 0, "limit_messages": 1}),
        deps=_deps(),
    )

    assert result == {
        "ok": True,
        "items": [{"q_text": "q", "a_text": "a"}],
        "meta": {"source": "tg"},
    }
    assert FakeAsyncClient.calls == [
        {
            "method": "POST",
            "url": "http://tgworker.test/tg/qa",
            "headers": {"X-Admin-Token": "admin"},
            "json": {"tenant": 3, "limit_dialogs": 1, "limit_messages": 50},
        }
    ]


@pytest.mark.asyncio
async def test_training_tg_harvest_requires_admin_token() -> None:
    result = await client_training_runtime.training_tg_harvest(
        3,
        FakeRequest({}),
        deps=_deps(admin_token=""),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 500


@pytest.mark.asyncio
async def test_training_tg_accept_sanitizes_and_saves_examples() -> None:
    db = FakeDb()

    result = await client_training_runtime.training_tg_accept(
        5,
        FakeRequest({"items": [{"q_text": " q\n text ", "a_text": " a\r text "}, {"q_text": ""}]}),
        deps=_deps(db=db),
    )

    assert result == {"ok": True, "saved": 1}
    assert db.recorded[0]["tenant"] == 5
    assert db.recorded[0]["q_text"] == "q text"
    assert db.recorded[0]["a_text"] == "a text"
