from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Mapping

from libs.core import sales_core
from libs.core.repo import avito_accounts, avito_item_contexts, tenant_asset_rules, tenant_asset_usage, tenant_assets
from libs.core.services import asset_action_planner
from libs.core.services.persona_asset_rule_compiler import build_persona_asset_rules


def _photo_public_url_builder(app_base_url: str, public_key_fn: Callable[[int], str]) -> Callable[[int, str], str]:
    def _build(tenant_id: int, photo_id: str) -> str:
        key = public_key_fn(int(tenant_id))
        base = str(app_base_url or "").strip().rstrip("/")
        if base and key:
            return f"{base}/pub/files/photos/{photo_id}?tenant={tenant_id}&k={key}"
        if key:
            return f"/pub/files/photos/{photo_id}?tenant={tenant_id}&k={key}"
        return ""

    return _build


async def select_asset_action_attachments(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    user_text: str,
    reply_text: str,
    context: Mapping[str, Any] | None = None,
    app_base_url: str,
    public_key_fn: Callable[[int], str],
    max_assets: int = 1,
    log_fn: Callable[..., None] = lambda *_args, **_kwargs: None,
) -> list[dict[str, Any]]:
    try:
        enriched_context = await _asset_runtime_context(
            tenant_id=int(tenant_id),
            lead_id=int(lead_id or 0),
            context=context,
        )
        plan = await asset_action_planner.plan_asset_actions(
            tenant_id=int(tenant_id),
            lead_id=int(lead_id),
            channel=channel,
            user_text=user_text,
            reply_text=reply_text,
            max_assets=max_assets,
            known_facts=enriched_context["known_facts"],
            avito_item_city=enriched_context["avito_item_city"],
            deps=asset_action_planner.AssetActionPlannerDeps(
                list_active_rules_fn=_list_active_rules_with_persona,
                get_asset_fn=tenant_assets.get_asset,
                was_used_recently_fn=tenant_asset_usage.was_used_recently,
                record_usage_fn=tenant_asset_usage.record_usage,
                build_public_url_fn=_photo_public_url_builder(app_base_url, public_key_fn),
                log_fn=log_fn,
            ),
        )
    except Exception as exc:
        log_fn(
            "event=asset_action_failed tenant=%s channel=%s lead_id=%s error=%s",
            tenant_id,
            channel,
            lead_id,
            exc,
        )
        return []
    return plan.attachments


async def _asset_runtime_context(
    *,
    tenant_id: int,
    lead_id: int,
    context: Mapping[str, Any] | None,
) -> dict[str, Any]:
    known_facts: dict[str, Any] = _sales_state_facts(tenant_id, lead_id)
    if isinstance(context, Mapping):
        account_id = _extract_int(context.get("account_id") or context.get("avito_account_id"))
        city = _extract_text(context.get("avito_item_city") or context.get("ad_city"))
    else:
        account_id = 0
        city = ""

    item_context: Mapping[str, Any] | None = None
    if lead_id > 0:
        try:
            item_context = await avito_item_contexts.get_context_for_lead(tenant_id, lead_id)
        except Exception:
            item_context = None
    if item_context:
        account_id = account_id or _extract_int(item_context.get("account_id"))
        if not city and str(item_context.get("status") or "") == "resolved":
            city = _extract_text(item_context.get("city"))

    if account_id > 0:
        known_facts["avito_account_id"] = account_id
        try:
            account = await avito_accounts.get_account(tenant_id, account_id)
        except Exception:
            account = None
        if account:
            known_facts["account_login"] = account.get("account_login")
            known_facts["account_label"] = account.get("display_name") or account.get("account_login")
            known_facts["display_name"] = account.get("display_name")
    return {"known_facts": known_facts, "avito_item_city": city or None}


def _sales_state_facts(tenant_id: int, lead_id: int) -> dict[str, str]:
    if int(tenant_id or 0) <= 0 or int(lead_id or 0) <= 0:
        return {}
    try:
        state = sales_core.load_sales_state(int(tenant_id), int(lead_id))
    except Exception:
        return {}
    raw = getattr(state, "facts", None)
    if not isinstance(raw, Mapping):
        return {}
    facts: dict[str, str] = {}
    for key, value in raw.items():
        cleaned_key = str(key or "").strip()
        cleaned_value = str(value or "").strip()
        if cleaned_key and cleaned_value:
            facts[cleaned_key] = cleaned_value
    return facts


async def _list_active_rules_with_persona(tenant_id: int) -> list[dict[str, Any]]:
    db_rules = await tenant_asset_rules.list_active_rules(int(tenant_id))
    persona_text = _read_persona_text(int(tenant_id))
    if not persona_text:
        return db_rules
    assets = await tenant_assets.list_assets(int(tenant_id))
    persona_rules = build_persona_asset_rules(
        tenant_id=int(tenant_id),
        persona_text=persona_text,
        assets=assets,
    )
    if not persona_rules:
        return db_rules
    return sorted(
        [*persona_rules, *db_rules],
        key=lambda rule: int(rule.get("priority") or 0),
        reverse=True,
    )


def _read_persona_text(tenant_id: int) -> str:
    try:
        path = Path(sales_core.TENANTS_DIR) / str(int(tenant_id)) / "persona_avito.md"
        if not path.exists() or not path.is_file():
            return ""
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _extract_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _extract_text(value: Any) -> str:
    return str(value or "").strip()


def tenant_asset_uploads_dir(tenant_dir_fn: Callable[[int], Path], tenant_id: int) -> Path:
    target = tenant_dir_fn(int(tenant_id)) / "uploads" / "assets"
    target.mkdir(parents=True, exist_ok=True)
    return target
