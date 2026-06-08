from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from libs.core import db as db_module

_ENSURING_SCHEMA = False


def stable_rule_id(tenant_id: int, asset_id: str | None, source: str, conditions: Mapping[str, Any]) -> str:
    seed = json.dumps(
        {"tenant_id": int(tenant_id), "asset_id": asset_id or "", "source": source, "conditions": dict(conditions)},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()


def _json(data: Mapping[str, Any] | None) -> str:
    return json.dumps(dict(data or {}), ensure_ascii=False, sort_keys=True)


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    try:
        data = dict(row)
    except Exception:
        return None
    for key in ("trigger", "conditions", "action", "guards"):
        value = data.get(key)
        if isinstance(value, str):
            try:
                data[key] = json.loads(value)
            except Exception:
                data[key] = {}
    return data


async def ensure_schema() -> None:
    global _ENSURING_SCHEMA
    if _ENSURING_SCHEMA:
        return
    _ENSURING_SCHEMA = True
    try:
        exec_fn = getattr(db_module, "_exec", None)
        if not exec_fn:
            return
        for statement in (
            """
            CREATE TABLE IF NOT EXISTS tenant_asset_rules (
                id BIGSERIAL PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                rule_id TEXT NOT NULL,
                asset_id TEXT,
                source TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'needs_review',
                priority INTEGER NOT NULL DEFAULT 0,
                trigger JSONB NOT NULL DEFAULT '{}',
                conditions JSONB NOT NULL DEFAULT '{}',
                action JSONB NOT NULL DEFAULT '{}',
                guards JSONB NOT NULL DEFAULT '{}',
                confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                needs_review BOOLEAN NOT NULL DEFAULT TRUE,
                compiler_version TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT tenant_asset_rules_key UNIQUE (tenant_id, rule_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_tenant_asset_rules_tenant_status ON tenant_asset_rules(tenant_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_tenant_asset_rules_tenant_asset ON tenant_asset_rules(tenant_id, asset_id)",
        ):
            await exec_fn(statement)
    finally:
        _ENSURING_SCHEMA = False


async def upsert_rule(
    tenant_id: int,
    rule_id: str,
    *,
    asset_id: str | None,
    source: str,
    status: str,
    priority: int = 0,
    trigger: Mapping[str, Any] | None = None,
    conditions: Mapping[str, Any] | None = None,
    action: Mapping[str, Any] | None = None,
    guards: Mapping[str, Any] | None = None,
    confidence: float = 0.0,
    needs_review: bool = True,
    compiler_version: str | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return {
            "tenant_id": int(tenant_id),
            "rule_id": rule_id,
            "asset_id": asset_id,
            "source": source,
            "status": status,
            "priority": int(priority),
            "trigger": dict(trigger or {}),
            "conditions": dict(conditions or {}),
            "action": dict(action or {}),
            "guards": dict(guards or {}),
            "confidence": float(confidence),
            "needs_review": bool(needs_review),
            "compiler_version": compiler_version,
        }
    row = await fetchrow(
        """
        INSERT INTO tenant_asset_rules (
            tenant_id, rule_id, asset_id, source, status, priority, trigger,
            conditions, action, guards, confidence, needs_review, compiler_version, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10::jsonb,$11,$12,$13,now())
        ON CONFLICT (tenant_id, rule_id) DO UPDATE SET
            asset_id = EXCLUDED.asset_id,
            source = EXCLUDED.source,
            status = EXCLUDED.status,
            priority = EXCLUDED.priority,
            trigger = EXCLUDED.trigger,
            conditions = EXCLUDED.conditions,
            action = EXCLUDED.action,
            guards = EXCLUDED.guards,
            confidence = EXCLUDED.confidence,
            needs_review = EXCLUDED.needs_review,
            compiler_version = EXCLUDED.compiler_version,
            updated_at = now()
        RETURNING *
        """,
        int(tenant_id),
        str(rule_id),
        asset_id,
        source,
        status,
        int(priority),
        _json(trigger),
        _json(conditions),
        _json(action),
        _json(guards),
        float(confidence),
        bool(needs_review),
        compiler_version,
    )
    return _row_to_dict(row)


async def list_active_rules(tenant_id: int) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    rows = await fetch(
        """
        SELECT * FROM tenant_asset_rules
        WHERE tenant_id=$1 AND status='active' AND needs_review IS FALSE
        ORDER BY priority DESC, updated_at DESC
        """,
        int(tenant_id),
    )
    return [item for row in rows if (item := _row_to_dict(row))]


async def list_rules_for_asset(tenant_id: int, asset_id: str) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    rows = await fetch(
        "SELECT * FROM tenant_asset_rules WHERE tenant_id=$1 AND asset_id=$2 ORDER BY updated_at DESC",
        int(tenant_id),
        str(asset_id),
    )
    return [item for row in rows if (item := _row_to_dict(row))]


async def mark_rule_status(tenant_id: int, rule_id: str, status: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    return _row_to_dict(
        await fetchrow(
            "UPDATE tenant_asset_rules SET status=$3, needs_review=($3='needs_review'), updated_at=now() WHERE tenant_id=$1 AND rule_id=$2 RETURNING *",
            int(tenant_id),
            str(rule_id),
            str(status),
        )
    )


async def delete_rules_for_asset(tenant_id: int, asset_id: str) -> None:
    await ensure_schema()
    exec_fn = getattr(db_module, "_exec", None)
    if not exec_fn:
        return
    await exec_fn(
        "UPDATE tenant_asset_rules SET status='deleted', updated_at=now() WHERE tenant_id=$1 AND asset_id=$2",
        int(tenant_id),
        str(asset_id),
    )
