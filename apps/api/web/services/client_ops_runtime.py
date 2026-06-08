from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from fastapi.responses import JSONResponse, Response


SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ClientOpsDeps:
    resolve_tenant_and_key_fn: SyncFn
    redis_client_fn: SyncFn
    handoff_silence_key_fn: SyncFn
    handoff_silence_meta_key_fn: SyncFn
    outbox_queue_key: str
    json_module: Any = json


async def dialogs_unsilence_api(
    request: Any,
    lead_id: int,
    *,
    tenant: int | str | None,
    deps: ClientOpsDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    try:
        lead_id = int(lead_id)
    except Exception:
        return JSONResponse({"detail": "invalid_lead"}, status_code=400)
    if lead_id <= 0:
        return JSONResponse({"detail": "invalid_lead"}, status_code=400)

    try:
        redis_client = deps.redis_client_fn()
    except Exception:
        return JSONResponse({"detail": "redis_unavailable"}, status_code=503)

    silence_key = deps.handoff_silence_key_fn(tenant_id, lead_id)
    meta_key = deps.handoff_silence_meta_key_fn(tenant_id, lead_id)
    try:
        deleted = redis_client.delete(silence_key, meta_key)
    except Exception:
        return JSONResponse({"detail": "redis_error"}, status_code=500)

    return {"ok": True, "deleted": int(deleted or 0)}


async def tenant_stats_api(
    request: Any,
    *,
    tenant: int | str | None,
    sample: int,
    deps: ClientOpsDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    try:
        redis_client = deps.redis_client_fn()
    except Exception:
        return JSONResponse({"detail": "redis_unavailable"}, status_code=503)

    sample_limit = _sample_limit(sample)
    outbox_len = _safe_redis_int(lambda: redis_client.llen(deps.outbox_queue_key))
    followup_len = _safe_redis_int(lambda: redis_client.zcard("followup:schedule"))
    tenant_outbox, sampled = _count_tenant_outbox(
        redis_client,
        tenant_id=int(tenant_id),
        sample_limit=sample_limit,
        deps=deps,
    )

    return {
        "ok": True,
        "tenant_id": tenant_id,
        "outbox_total": outbox_len,
        "outbox_tenant": tenant_outbox,
        "followup_scheduled_len": followup_len,
        "sampled": sampled,
    }


def _sample_limit(sample: int) -> int:
    try:
        value = int(sample or 0)
    except Exception:
        value = 0
    return max(0, min(value, 2000))


def _safe_redis_int(fn: Callable[[], Any]) -> int:
    try:
        return int(fn())
    except Exception:
        return 0


def _count_tenant_outbox(
    redis_client: Any,
    *,
    tenant_id: int,
    sample_limit: int,
    deps: ClientOpsDeps,
) -> tuple[int, int]:
    if sample_limit <= 0:
        return 0, 0
    try:
        items = redis_client.lrange(deps.outbox_queue_key, 0, sample_limit - 1)
    except Exception:
        items = []
    tenant_outbox = 0
    for raw in items:
        payload = _parse_outbox_payload(raw, deps=deps)
        tenant_raw = payload.get("tenant_id") or payload.get("tenant")
        try:
            tenant_val = int(tenant_raw)
        except Exception:
            continue
        if tenant_val == int(tenant_id):
            tenant_outbox += 1
    return tenant_outbox, len(items)


def _parse_outbox_payload(raw: Any, *, deps: ClientOpsDeps) -> Mapping[str, Any]:
    try:
        payload = deps.json_module.loads(raw) if raw else {}
    except Exception:
        payload = {}
    return payload if isinstance(payload, Mapping) else {}
