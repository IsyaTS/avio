from __future__ import annotations

import json

import pytest
from fastapi.responses import JSONResponse

from apps.api.web.services import client_ops_runtime


pytestmark = pytest.mark.unit


class FakeRedis:
    def __init__(self) -> None:
        self.deleted: tuple[str, ...] = ()
        self.items = [
            json.dumps({"tenant_id": 1}),
            json.dumps({"tenant": 2}),
            "bad-json",
            json.dumps({"tenant_id": 1}),
        ]

    def delete(self, *keys: str) -> int:
        self.deleted = tuple(keys)
        return len(keys)

    def llen(self, _key: str) -> int:
        return 10

    def zcard(self, _key: str) -> int:
        return 3

    def lrange(self, _key: str, start: int, stop: int) -> list[str]:
        return self.items[start: stop + 1]


def _deps(redis: FakeRedis | None = None) -> client_ops_runtime.ClientOpsDeps:
    store = redis or FakeRedis()
    return client_ops_runtime.ClientOpsDeps(
        resolve_tenant_and_key_fn=lambda _request, _tenant: (1, "key"),
        redis_client_fn=lambda: store,
        handoff_silence_key_fn=lambda tenant, lead: f"silence:{tenant}:{lead}",
        handoff_silence_meta_key_fn=lambda tenant, lead: f"silence-meta:{tenant}:{lead}",
        outbox_queue_key="outbox:send",
        json_module=json,
    )


@pytest.mark.asyncio
async def test_dialogs_unsilence_deletes_silence_keys() -> None:
    redis = FakeRedis()

    result = await client_ops_runtime.dialogs_unsilence_api(
        object(),
        22,
        tenant=1,
        deps=_deps(redis),
    )

    assert result == {"ok": True, "deleted": 2}
    assert redis.deleted == ("silence:1:22", "silence-meta:1:22")


@pytest.mark.asyncio
async def test_dialogs_unsilence_rejects_invalid_lead() -> None:
    result = await client_ops_runtime.dialogs_unsilence_api(
        object(),
        0,
        tenant=1,
        deps=_deps(),
    )

    assert isinstance(result, JSONResponse)
    assert result.status_code == 400


@pytest.mark.asyncio
async def test_tenant_stats_counts_sampled_outbox_for_tenant() -> None:
    result = await client_ops_runtime.tenant_stats_api(
        object(),
        tenant=1,
        sample=4,
        deps=_deps(),
    )

    assert result == {
        "ok": True,
        "tenant_id": 1,
        "outbox_total": 10,
        "outbox_tenant": 2,
        "followup_scheduled_len": 3,
        "sampled": 4,
    }


@pytest.mark.asyncio
async def test_tenant_stats_returns_auth_response() -> None:
    auth_response = JSONResponse({"detail": "unauthorized"}, status_code=401)
    deps = client_ops_runtime.ClientOpsDeps(
        resolve_tenant_and_key_fn=lambda *_args: auth_response,
        redis_client_fn=lambda: FakeRedis(),
        handoff_silence_key_fn=lambda *_args: "",
        handoff_silence_meta_key_fn=lambda *_args: "",
        outbox_queue_key="outbox:send",
    )

    result = await client_ops_runtime.tenant_stats_api(
        object(),
        tenant=1,
        sample=4,
        deps=deps,
    )

    assert result is auth_response
