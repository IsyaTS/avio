from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping


@dataclass(frozen=True)
class AmoCrmInboundDeps:
    redis_client: Any
    log_fn: Callable[..., None]
    normalize_attachments_fn: Callable[..., list[dict[str, Any]]]
    content_fingerprint_fn: Callable[..., str]
    amocrm_service_module: Any


async def maybe_amocrm_inbound(
    tenant_id: int,
    lead_id: int,
    text: str,
    channel: str,
    *,
    attachments: Iterable[Mapping[str, Any]] | None = None,
    message_id: int | None = None,
    deps: AmoCrmInboundDeps,
) -> None:
    normalized_attachments = deps.normalize_attachments_fn(attachments or [])
    if not text and not normalized_attachments:
        return
    if not await _claim_inbound_event(
        tenant_id,
        lead_id,
        channel,
        message_id=message_id,
        fingerprint=deps.content_fingerprint_fn(text, normalized_attachments),
        deps=deps,
    ):
        return
    try:
        await deps.amocrm_service_module.amocrm_on_inbound_message(
            int(tenant_id),
            int(lead_id),
            text=text,
            channel=channel,
            attachments=normalized_attachments or None,
            source_role="lead",
        )
    except Exception as exc:
        deps.log_fn(
            f"event=amocrm_inbound_failed channel={channel} tenant={tenant_id} "
            f"lead_id={lead_id} error={exc}"
        )


async def _claim_inbound_event(
    tenant_id: int,
    lead_id: int,
    channel: str,
    *,
    message_id: int | None,
    fingerprint: str,
    deps: AmoCrmInboundDeps,
) -> bool:
    if message_id is not None:
        key = f"amocrm:inbound:{tenant_id}:{lead_id}:{channel}:{int(message_id)}"
        return await _claim_key(key, ttl_seconds=86400, deps=deps)
    key = f"amocrm:inbound:{tenant_id}:{lead_id}:{channel}:fp:{fingerprint}"
    return await _claim_key(key, ttl_seconds=180, deps=deps)


async def _claim_key(key: str, *, ttl_seconds: int, deps: AmoCrmInboundDeps) -> bool:
    try:
        claimed = await deps.redis_client.set(key, "1", ex=ttl_seconds, nx=True)
    except Exception:
        return True
    return bool(claimed)
