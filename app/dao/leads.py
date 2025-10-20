from __future__ import annotations

import hashlib
import time
from typing import Optional

try:  # pragma: no cover - optional dependency for typing only
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    asyncpg = None  # type: ignore[assignment]

from app.db import find_lead_by_peer, upsert_lead

_MAX_ID_RETRIES = 5


def _normalize_channel(channel: str) -> str:
    return (channel or "avito").strip().lower() or "avito"


def _normalize_peer(peer: str) -> str:
    normalized = (peer or "").strip().lower()
    return normalized[:255]


def _generate_lead_id(tenant_id: int, channel: str, peer: str) -> int:
    seed = f"{tenant_id}:{channel}:{peer}".encode("utf-8")
    digest = hashlib.sha1(seed).hexdigest()
    candidate = int(digest[:15], 16)
    if candidate <= 0:
        candidate = int(time.time() * 1000)
    return candidate


def _bump_lead_id(current: int) -> int:
    next_id = current + 1 if current > 0 else int(time.time() * 1000)
    if next_id <= 0:
        next_id = int(time.time() * 1000)
    return next_id


async def get_or_create_by_peer(
    tenant_id: int,
    channel: str,
    peer: str,
    *,
    lead_id_hint: Optional[int] = None,
    source_real_id: Optional[int] = None,
    title: Optional[str] = None,
    contact: Optional[str] = None,
) -> int:
    channel_value = _normalize_channel(channel)
    peer_value = _normalize_peer(peer)
    if not peer_value:
        raise ValueError("peer must be a non-empty string")

    existing = await find_lead_by_peer(tenant_id, channel_value, peer_value)
    if existing is not None:
        existing_id = existing.get("id")
        if existing_id is not None:
            lead_id: Optional[int] = None
            try:
                lead_id = int(existing_id)
            except Exception:
                try:
                    lead_id = int(str(existing_id))
                except Exception:
                    lead_id = None
            if lead_id is None:
                raise ValueError("Existing lead has a non-numeric identifier")
            await upsert_lead(
                lead_id,
                channel=channel_value,
                tenant_id=tenant_id,
                peer=peer_value,
                source_real_id=source_real_id,
                contact=contact,
                title=title,
            )
            return lead_id

    candidate_id = lead_id_hint if lead_id_hint and lead_id_hint > 0 else None
    if candidate_id is None:
        try:
            candidate_id = _generate_lead_id(int(tenant_id), channel_value, peer_value)
        except Exception:
            candidate_id = int(time.time() * 1000)

    attempts = 0
    while attempts < _MAX_ID_RETRIES:
        try:
            resolved = await upsert_lead(
                candidate_id,
                channel=channel_value,
                tenant_id=tenant_id,
                peer=peer_value,
                source_real_id=source_real_id,
                contact=contact,
                title=title,
            )
        except Exception as exc:
            if asyncpg is not None and isinstance(exc, asyncpg.UniqueViolationError):
                constraint = getattr(exc, "constraint_name", "") or ""
                if constraint == "ux_leads_tenant_channel_peer":
                    existing = await find_lead_by_peer(tenant_id, channel_value, peer_value)
                    if existing and existing.get("id") is not None:
                        try:
                            return int(existing.get("id"))
                        except Exception:
                            return existing.get("id")  # type: ignore[return-value]
                    attempts += 1
                    continue
                if constraint == "leads_pkey":
                    candidate_id = _bump_lead_id(candidate_id)
                    attempts += 1
                    continue
            raise

        if resolved is not None:
            try:
                return int(resolved)
            except Exception:
                return resolved  # type: ignore[return-value]
        attempts += 1
        candidate_id = _bump_lead_id(candidate_id)

    raise RuntimeError("Failed to resolve lead by peer after retries")

