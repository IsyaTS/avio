from __future__ import annotations

from typing import Any

import pytest

from libs.core.services.asset_action_planner import AssetActionPlannerDeps, plan_asset_actions


pytestmark = pytest.mark.unit


def _rule() -> dict[str, Any]:
    return {
        "rule_id": "r1",
        "asset_id": "a1",
        "status": "active",
        "needs_review": False,
        "conditions": {"all": [{"slot": "city", "operator": "equals", "value": "Казань"}]},
        "guards": {"requires_known_slots": ["city"], "allowed_channels": ["avito"], "once_per_dialog": True},
        "action": {"type": "send_asset", "asset_id": "a1", "asset_type": "photo", "caption_hint": "Каталог"},
    }


def _deps(calls: list[tuple]):
    async def list_rules(_tenant_id: int):
        return [_rule()]

    async def get_asset(_tenant_id: int, _asset_id: str):
        return {
            "asset_id": "a1",
            "asset_type": "photo",
            "status": "active",
            "title": "Каталог",
            "legacy_photo_id": "p1",
            "relative_path": "uploads/photos/p1.jpg",
            "mime": "image/jpeg",
        }

    async def was_used(*_args, **_kwargs):
        return False

    async def record_usage(*args, **kwargs):
        calls.append((args, kwargs))

    return AssetActionPlannerDeps(
        list_active_rules_fn=list_rules,
        get_asset_fn=get_asset,
        was_used_recently_fn=was_used,
        record_usage_fn=record_usage,
        build_public_url_fn=lambda tenant, photo_id: f"https://avio.test/{tenant}/{photo_id}",
        log_fn=lambda *_a, **_k: None,
    )


@pytest.mark.anyio
async def test_asset_action_planner_returns_attachment_when_safe() -> None:
    calls: list[tuple] = []
    plan = await plan_asset_actions(
        tenant_id=101,
        lead_id=55,
        channel="avito",
        user_text="Казань",
        reply_text="",
        deps=_deps(calls),
    )

    assert plan.attachments[0]["type"] == "image"
    assert plan.attachments[0]["url"] == "https://avio.test/101/p1"
    assert calls


@pytest.mark.anyio
async def test_asset_action_planner_blocks_unsupported_pdf_for_avito() -> None:
    async def list_rules(_tenant_id: int):
        rule = _rule()
        rule["action"]["asset_type"] = "pdf"
        return [rule]

    async def get_asset(_tenant_id: int, _asset_id: str):
        return {"asset_id": "a1", "asset_type": "pdf", "status": "active", "title": "PDF", "mime": "application/pdf"}

    deps = _deps([])
    deps = AssetActionPlannerDeps(
        list_active_rules_fn=list_rules,
        get_asset_fn=get_asset,
        was_used_recently_fn=deps.was_used_recently_fn,
        record_usage_fn=deps.record_usage_fn,
        build_public_url_fn=deps.build_public_url_fn,
        log_fn=deps.log_fn,
    )
    plan = await plan_asset_actions(
        tenant_id=101,
        lead_id=55,
        channel="avito",
        user_text="Казань",
        reply_text="",
        deps=deps,
    )

    assert plan.attachments == []
    assert plan.blocked[-1]["reason"] == "avito_file_not_guaranteed"
