from __future__ import annotations

from typing import Any, Mapping

from libs.core.repo import tenant_asset_rules, tenant_assets as tenant_assets_repo
from libs.core.services import asset_rule_compiler
from libs.core.services.asset_ai_metadata_compiler import AssetCompileInput, compile_asset_metadata


async def sync_public_photo_asset(tenant_id: int, entry: Mapping[str, Any]) -> dict[str, Any] | None:
    return await tenant_assets_repo.sync_legacy_photo_asset(int(tenant_id), entry)


async def compile_public_photo_asset_rule(
    tenant_id: int,
    asset: Mapping[str, Any],
    entry: Mapping[str, Any],
) -> dict[str, Any] | None:
    asset_id = str(asset.get("asset_id") or "")
    if not asset_id:
        return None
    channels = tuple(str(ch).strip().lower() for ch in (entry.get("channels") or []) if str(ch).strip())
    result = await compile_asset_metadata(
        AssetCompileInput(
            tenant_id=int(tenant_id),
            asset_id=asset_id,
            asset_type=str(asset.get("asset_type") or "photo"),
            title=str(asset.get("title") or entry.get("title") or ""),
            description=str(asset.get("description") or entry.get("usage") or ""),
            filename=str(asset.get("original_filename") or entry.get("original") or ""),
            mime=str(asset.get("mime") or entry.get("mime") or ""),
            allowed_channels=channels,
        )
    )
    status = "needs_review" if result.metadata.get("needs_review") else "active"
    await tenant_assets_repo.upsert_asset(
        int(tenant_id),
        asset_id,
        asset_type=str(asset.get("asset_type") or "photo"),
        title=str(asset.get("title") or entry.get("title") or asset_id),
        description=str(asset.get("description") or entry.get("usage") or "") or None,
        original_filename=str(asset.get("original_filename") or entry.get("original") or "") or None,
        mime=str(asset.get("mime") or entry.get("mime") or "") or None,
        size_bytes=int(asset.get("size_bytes") or entry.get("size") or 0),
        relative_path=str(asset.get("relative_path") or entry.get("path") or "") or None,
        status=status,
        source=str(asset.get("source") or "legacy_photo_manifest"),
        legacy_photo_id=str(asset.get("legacy_photo_id") or entry.get("id") or "") or None,
        ai_metadata=result.metadata,
    )
    await tenant_asset_rules.delete_rules_for_asset(int(tenant_id), asset_id)
    return await asset_rule_compiler.compile_and_store_asset_rule(
        int(tenant_id),
        asset_id,
        result.metadata,
        source="asset_title",
        priority=int(entry.get("priority") or 0),
    )


async def list_assets_status(tenant_id: int) -> dict[str, Any]:
    assets = await tenant_assets_repo.list_assets(int(tenant_id))
    return {
        "ok": True,
        "assets": [_public_asset(item) for item in assets],
    }


def _public_asset(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "asset_id": item.get("asset_id"),
        "asset_type": item.get("asset_type"),
        "title": item.get("title"),
        "description": item.get("description"),
        "status": item.get("status"),
        "legacy_photo_id": item.get("legacy_photo_id"),
        "ai_metadata": {
            "confidence": (item.get("ai_metadata") or {}).get("confidence")
            if isinstance(item.get("ai_metadata"), Mapping)
            else None,
            "needs_review": (item.get("ai_metadata") or {}).get("needs_review")
            if isinstance(item.get("ai_metadata"), Mapping)
            else None,
            "human_summary": (item.get("ai_metadata") or {}).get("human_summary")
            if isinstance(item.get("ai_metadata"), Mapping)
            else "",
        },
    }
