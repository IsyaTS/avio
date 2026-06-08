from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Sequence

from libs.core.services import asset_rule_context, asset_rule_matcher, channel_asset_capabilities

AsyncFn = Callable[..., Awaitable[Any]]
SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class AssetActionPlannerDeps:
    list_active_rules_fn: AsyncFn
    get_asset_fn: AsyncFn
    was_used_recently_fn: AsyncFn
    record_usage_fn: AsyncFn
    build_public_url_fn: SyncFn
    log_fn: SyncFn


@dataclass(frozen=True)
class AssetActionPlan:
    attachments: list[dict[str, Any]] = field(default_factory=list)
    blocked: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


async def plan_asset_actions(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    user_text: str,
    reply_text: str,
    max_assets: int = 1,
    history: Sequence[Mapping[str, Any]] | None = None,
    known_facts: Mapping[str, Any] | None = None,
    avito_item_city: str | None = None,
    deps: AssetActionPlannerDeps,
) -> AssetActionPlan:
    rules = await deps.list_active_rules_fn(int(tenant_id))
    if not rules:
        return AssetActionPlan(stats={"rules_checked": 0})
    context = asset_rule_context.build_asset_rule_context(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        channel=channel,
        user_text=f"{user_text}\n{reply_text}".strip(),
        history=history,
        known_facts=known_facts,
        avito_item_city=avito_item_city,
    )
    matches = asset_rule_matcher.match_asset_rules(rules, context, max_matches=max(1, int(max_assets or 1)))
    attachments: list[dict[str, Any]] = []
    blocked = list(matches.blocked_actions)
    for item in matches.matched_actions:
        rule = item.get("rule") if isinstance(item.get("rule"), Mapping) else {}
        action = item.get("action") if isinstance(item.get("action"), Mapping) else {}
        asset_id = str(action.get("asset_id") or rule.get("asset_id") or "").strip()
        if not asset_id:
            blocked.append({"reason": "missing_asset_id", "rule_id": rule.get("rule_id")})
            continue
        guards = rule.get("guards") if isinstance(rule.get("guards"), Mapping) else {}
        if bool(guards.get("once_per_dialog")) and int(lead_id or 0) > 0:
            if await deps.was_used_recently_fn(int(tenant_id), int(lead_id), asset_id):
                blocked.append({"reason": "already_used", "asset_id": asset_id, "rule_id": rule.get("rule_id")})
                continue
        asset = await deps.get_asset_fn(int(tenant_id), asset_id)
        attachment = _attachment_from_asset(asset, action, channel, tenant_id, deps)
        if attachment is None:
            blocked.append({"reason": "asset_unavailable", "asset_id": asset_id, "rule_id": rule.get("rule_id")})
            continue
        capability = channel_asset_capabilities.can_send_asset(
            channel,
            str(asset.get("asset_type") or action.get("asset_type") or ""),
            str(asset.get("mime") or attachment.get("mime") or ""),
        )
        if not capability.allowed:
            blocked.append({"reason": capability.reason, "asset_id": asset_id, "rule_id": rule.get("rule_id")})
            continue
        attachments.append(attachment)
        if int(lead_id or 0) > 0:
            await deps.record_usage_fn(
                int(tenant_id),
                int(lead_id),
                channel,
                asset_id,
                str(rule.get("rule_id") or "") or None,
                event_type="planned",
            )
        if len(attachments) >= max_assets:
            break
    if attachments:
        deps.log_fn(
            "event=asset_action_selected tenant=%s channel=%s count=%s",
            tenant_id,
            channel,
            len(attachments),
        )
    return AssetActionPlan(attachments=attachments, blocked=blocked, stats=matches.stats)


def _attachment_from_asset(
    asset: Mapping[str, Any] | None,
    action: Mapping[str, Any],
    channel: str,
    tenant_id: int,
    deps: AssetActionPlannerDeps,
) -> dict[str, Any] | None:
    if not asset or str(asset.get("status") or "").lower() not in {"active", "draft"}:
        return None
    asset_type = str(asset.get("asset_type") or action.get("asset_type") or "").strip().lower()
    legacy_photo_id = str(asset.get("legacy_photo_id") or "").strip()
    relative_path = str(asset.get("relative_path") or "").strip()
    public_url = str(asset.get("public_url") or "").strip()
    url = public_url
    if legacy_photo_id:
        url = str(deps.build_public_url_fn(int(tenant_id), legacy_photo_id) or url)
    attachment_type = "image" if asset_type in {"photo", "image"} else "document"
    return {
        "type": attachment_type,
        "asset_id": asset.get("asset_id"),
        "photo_id": legacy_photo_id or None,
        "url": url,
        "path": relative_path or None,
        "name": asset.get("original_filename") or asset.get("title"),
        "filename": asset.get("original_filename") or asset.get("title"),
        "mime": asset.get("mime"),
        "caption": action.get("caption_hint") or asset.get("title"),
    }
