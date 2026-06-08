from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from libs.core import db as db_module

_ENSURING_SCHEMA = False


def _row_to_dict(row: Mapping[str, Any] | Any) -> dict[str, Any] | None:
    if not row:
        return None
    try:
        data = dict(row)
    except Exception:
        return None
    meta = data.get("ai_metadata")
    if isinstance(meta, str):
        try:
            data["ai_metadata"] = json.loads(meta)
        except Exception:
            data["ai_metadata"] = {}
    return data


def stable_asset_id(tenant_id: int, *, legacy_photo_id: str = "", relative_path: str = "", title: str = "") -> str:
    seed = f"{int(tenant_id)}|{legacy_photo_id}|{relative_path}|{title}".encode("utf-8")
    return hashlib.sha1(seed).hexdigest()


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
            CREATE TABLE IF NOT EXISTS tenant_assets (
                id BIGSERIAL PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                asset_id TEXT NOT NULL,
                asset_type TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                original_filename TEXT,
                mime TEXT,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                relative_path TEXT,
                public_url TEXT,
                checksum TEXT,
                status TEXT NOT NULL DEFAULT 'draft',
                source TEXT NOT NULL DEFAULT 'manual_upload',
                legacy_photo_id TEXT,
                ai_metadata JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT tenant_assets_key UNIQUE (tenant_id, asset_id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_tenant_assets_tenant_status ON tenant_assets(tenant_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_tenant_assets_tenant_type ON tenant_assets(tenant_id, asset_type)",
        ):
            await exec_fn(statement)
    finally:
        _ENSURING_SCHEMA = False


async def upsert_asset(
    tenant_id: int,
    asset_id: str,
    *,
    asset_type: str,
    title: str,
    description: str | None = None,
    original_filename: str | None = None,
    mime: str | None = None,
    size_bytes: int = 0,
    relative_path: str | None = None,
    public_url: str | None = None,
    checksum: str | None = None,
    status: str = "draft",
    source: str = "manual_upload",
    legacy_photo_id: str | None = None,
    ai_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return {
            "tenant_id": int(tenant_id),
            "asset_id": asset_id,
            "asset_type": asset_type,
            "title": title,
            "description": description,
            "status": status,
            "legacy_photo_id": legacy_photo_id,
            "ai_metadata": dict(ai_metadata or {}),
        }
    row = await fetchrow(
        """
        INSERT INTO tenant_assets (
            tenant_id, asset_id, asset_type, title, description, original_filename,
            mime, size_bytes, relative_path, public_url, checksum, status, source,
            legacy_photo_id, ai_metadata, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,now())
        ON CONFLICT (tenant_id, asset_id) DO UPDATE SET
            asset_type = EXCLUDED.asset_type,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            original_filename = EXCLUDED.original_filename,
            mime = EXCLUDED.mime,
            size_bytes = EXCLUDED.size_bytes,
            relative_path = EXCLUDED.relative_path,
            public_url = EXCLUDED.public_url,
            checksum = EXCLUDED.checksum,
            status = EXCLUDED.status,
            source = EXCLUDED.source,
            legacy_photo_id = EXCLUDED.legacy_photo_id,
            ai_metadata = EXCLUDED.ai_metadata,
            updated_at = now()
        RETURNING *
        """,
        int(tenant_id),
        str(asset_id),
        str(asset_type),
        str(title or "").strip(),
        description,
        original_filename,
        mime,
        int(size_bytes or 0),
        relative_path,
        public_url,
        checksum,
        status,
        source,
        legacy_photo_id,
        json.dumps(dict(ai_metadata or {}), ensure_ascii=False),
    )
    return _row_to_dict(row)


async def get_asset(tenant_id: int, asset_id: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    return _row_to_dict(
        await fetchrow(
            "SELECT * FROM tenant_assets WHERE tenant_id=$1 AND asset_id=$2 LIMIT 1",
            int(tenant_id),
            str(asset_id),
        )
    )


async def list_assets(tenant_id: int, *, include_disabled: bool = False) -> list[dict[str, Any]]:
    await ensure_schema()
    fetch = getattr(db_module, "_fetch", None)
    if not fetch:
        return []
    if include_disabled:
        rows = await fetch("SELECT * FROM tenant_assets WHERE tenant_id=$1 ORDER BY updated_at DESC", int(tenant_id))
    else:
        rows = await fetch(
            "SELECT * FROM tenant_assets WHERE tenant_id=$1 AND status <> 'deleted' ORDER BY updated_at DESC",
            int(tenant_id),
        )
    return [item for row in rows if (item := _row_to_dict(row))]


async def mark_asset_status(tenant_id: int, asset_id: str, status: str) -> dict[str, Any] | None:
    await ensure_schema()
    fetchrow = getattr(db_module, "_fetchrow", None)
    if not fetchrow:
        return None
    return _row_to_dict(
        await fetchrow(
            "UPDATE tenant_assets SET status=$3, updated_at=now() WHERE tenant_id=$1 AND asset_id=$2 RETURNING *",
            int(tenant_id),
            str(asset_id),
            str(status),
        )
    )


async def delete_asset_soft(tenant_id: int, asset_id: str) -> dict[str, Any] | None:
    return await mark_asset_status(tenant_id, asset_id, "deleted")


async def sync_legacy_photo_asset(tenant_id: int, photo_entry: Mapping[str, Any]) -> dict[str, Any] | None:
    photo_id = str(photo_entry.get("id") or "").strip()
    title = str(photo_entry.get("title") or photo_entry.get("original") or photo_entry.get("filename") or photo_id).strip()
    relative_path = str(photo_entry.get("path") or "").strip()
    asset_id = stable_asset_id(int(tenant_id), legacy_photo_id=photo_id, relative_path=relative_path, title=title)
    status = "active" if photo_entry.get("auto") and title else ("needs_review" if photo_entry.get("auto") else "draft")
    return await upsert_asset(
        int(tenant_id),
        asset_id,
        asset_type="photo",
        title=title or photo_id,
        description=str(photo_entry.get("usage") or "").strip() or None,
        original_filename=str(photo_entry.get("original") or photo_entry.get("filename") or "").strip() or None,
        mime=str(photo_entry.get("mime") or "").strip() or None,
        size_bytes=int(photo_entry.get("size") or 0),
        relative_path=relative_path or None,
        status=status,
        source="legacy_photo_manifest",
        legacy_photo_id=photo_id or None,
        ai_metadata={"legacy_photo": True},
    )
