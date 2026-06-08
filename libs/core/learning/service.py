from __future__ import annotations

import logging
import time
from typing import Any, Callable, Mapping, Sequence

from libs.core import db
from libs.core.sales_core import load_sales_state, read_tenant_config

from .actions import classify_action
from .config import intervention_learning_settings
from .outcomes import compute_episode_outcome
from .policy import format_policy_hint, select_runtime_policy
from .state_snapshot import build_dialogue_state_snapshot
from .stitching import stitch_messages, stitch_runtime_history


log = logging.getLogger("learning.v2")


def _clean_training_text(value: Any, *, limit: int = 1200) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    while "  " in text:
        text = text.replace("  ", " ")
    if len(text) > limit:
        text = text[:limit].rstrip()
    return text


async def _record_positive_intervention_example(
    *,
    tenant_id: int,
    lead_id: int,
    episode: Mapping[str, Any],
    reward: float,
    log_fn: Callable[[str], Any] | None = None,
) -> None:
    if reward <= 0:
        return
    q_text = _clean_training_text(episode.get("trigger_user_text"), limit=600)
    a_text = _clean_training_text(episode.get("manager_reply_text"), limit=1200)
    if len(q_text) < 4 or len(a_text) < 4:
        return
    try:
        manager_message_id = episode.get("manager_message_id")
        await db.record_training_example(
            int(tenant_id),
            lead_id=int(lead_id),
            message_id=int(manager_message_id) if manager_message_id else None,
            source="correction",
            source_feedback_id=None,
            q_text=q_text,
            a_text=a_text,
            is_bad=False,
            is_active=True,
            embedding_status="pending",
        )
        if log_fn:
            log_fn(
                "event=learning_v2_training_example_recorded tenant=%s lead_id=%s episode_id=%s reward=%.3f"
                % (tenant_id, lead_id, int(episode.get("id") or 0), float(reward))
            )
    except Exception as exc:
        if log_fn:
            log_fn(
                "event=learning_v2_training_example_failed tenant=%s lead_id=%s episode_id=%s error=%s"
                % (tenant_id, lead_id, int(episode.get("id") or 0), exc)
            )


def _manager_turn_index(turns: Sequence[Any], manager_message_id: int | None = None) -> int:
    if manager_message_id:
        for idx in range(len(turns) - 1, -1, -1):
            turn = turns[idx]
            if manager_message_id in set(turn.message_ids):
                return idx
    for idx in range(len(turns) - 1, -1, -1):
        if turns[idx].role == "manager":
            return idx
    return -1


def _latest_role_index(turns: Sequence[Any], role: str, *, before: int) -> int:
    limit = min(before, len(turns))
    for idx in range(limit - 1, -1, -1):
        if turns[idx].role == role:
            return idx
    return -1


def _future_turns(turns: Sequence[Any], *, after: int) -> list[Any]:
    return [turn for idx, turn in enumerate(turns) if idx > after]


async def finalize_pending_episode_outcomes(
    *,
    tenant_id: int,
    lead_id: int,
    log_fn: Callable[[str], Any] | None = None,
) -> int:
    if int(tenant_id or 0) <= 0 or int(lead_id or 0) <= 0:
        return 0
    cfg = read_tenant_config(int(tenant_id))
    settings = intervention_learning_settings(cfg)
    if not settings.get("enabled") or settings.get("kill_switch"):
        return 0
    episodes = await db.list_open_intervention_episodes(
        int(tenant_id),
        int(lead_id),
        limit=20,
        older_than_minutes=int(settings.get("outcome_horizon_minutes") or 180),
    )
    if not episodes:
        return 0
    rows = await db.get_recent_lead_messages(int(tenant_id), int(lead_id), limit=60)
    turns = stitch_messages(rows, within_seconds=int(settings.get("stitch_window_seconds") or 45))
    finalized = 0
    for episode in episodes:
        manager_message_id = episode.get("manager_message_id")
        try:
            manager_message_ref = int(manager_message_id) if manager_message_id is not None else 0
        except Exception:
            manager_message_ref = 0
        manager_index = _manager_turn_index(turns, manager_message_ref or None)
        if manager_index < 0:
            continue
        subsequent = _future_turns(turns, after=manager_index)
        created_at = episode.get("created_at")
        horizon_reached = False
        if created_at is not None:
            try:
                created_ts = created_at.timestamp() if hasattr(created_at, "timestamp") else 0.0
            except Exception:
                created_ts = 0.0
            if created_ts > 0 and (time.time() - created_ts) >= float(settings.get("outcome_horizon_minutes") or 180) * 60.0:
                horizon_reached = True
        outcome = compute_episode_outcome(
            trigger_user_text=str(episode.get("trigger_user_text") or ""),
            subsequent_turns=subsequent,
            horizon_reached=horizon_reached,
        )
        if not outcome.finalized:
            continue
        reward = float(outcome.reward)
        await db.finalize_intervention_episode(
            int(episode.get("id") or 0),
            outcome.signals,
            reward=reward,
            status="finalized",
        )
        await _record_positive_intervention_example(
            tenant_id=int(tenant_id),
            lead_id=int(lead_id),
            episode=episode,
            reward=reward,
            log_fn=log_fn,
        )
        candidate = await db.upsert_policy_candidate_from_episode(
            int(episode.get("id") or 0),
            reward=reward,
            signals=outcome.signals,
        )
        if candidate:
            await db.promote_or_demote_policy_candidate(
                int(candidate.get("id") or 0),
                min_evidence=int(settings.get("min_evidence") or 3),
                min_distinct_leads=int(settings.get("min_distinct_leads") or 2),
                min_reward_delta=float(settings.get("min_reward_delta") or 0.15),
                max_negative_evidence=int(settings.get("max_negative_evidence") or 2),
            )
        finalized += 1
        if log_fn:
            log_fn(
                "event=learning_v2_episode_finalized tenant=%s lead_id=%s episode_id=%s reward=%.3f"
                % (tenant_id, lead_id, int(episode.get("id") or 0), reward)
            )
    return finalized


async def prepare_runtime_policy_hint(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    user_text: str,
    normalized_history: Sequence[Mapping[str, Any]],
    log_fn: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    if int(tenant_id or 0) <= 0 or int(lead_id or 0) <= 0:
        return {"enabled": False, "mode": "disabled", "policy_block": ""}
    cfg = read_tenant_config(int(tenant_id))
    settings = intervention_learning_settings(cfg)
    mode = "disabled"
    if settings.get("kill_switch") or not settings.get("enabled") or not settings.get("runtime_enabled"):
        return {"enabled": False, "mode": mode, "policy_block": ""}

    await finalize_pending_episode_outcomes(tenant_id=int(tenant_id), lead_id=int(lead_id), log_fn=log_fn)
    state = load_sales_state(int(tenant_id), int(lead_id))
    stitched = stitch_runtime_history(
        list(normalized_history or [])[-int(settings.get("runtime_history_limit") or 12):],
        current_user_text=user_text,
    )
    snapshot = build_dialogue_state_snapshot(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        contact_id=int(lead_id),
        channel=channel,
        state=state,
        stitched_history=stitched,
        current_user_text=user_text,
    )
    rules = await db.list_tenant_policy_rules(
        int(tenant_id),
        active_only=True,
        limit=int(settings.get("max_rules") or 12),
    )
    decision = select_runtime_policy(snapshot=snapshot, rules=rules, settings=settings)
    rule = dict(decision.rule or {}) if isinstance(decision.rule, Mapping) else None
    rule_id = int(rule.get("id") or 0) if rule else None
    decision_id = await db.create_policy_decision(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        channel=str(channel or ""),
        snapshot=snapshot.to_dict(),
        status=decision.status,
        mode=decision.mode,
        reason=decision.reason,
        similarity=float(decision.similarity),
        confidence=float(decision.confidence),
        recommended_action=decision.recommended_action,
        avoid_action=decision.avoid_action,
        style_hints=decision.style_hints,
        rule_id=rule_id,
    )
    if log_fn:
        log_fn(
            "event=learning_v2_policy_decision tenant=%s lead_id=%s mode=%s status=%s reason=%s similarity=%.3f confidence=%.3f"
            % (
                tenant_id,
                lead_id,
                decision.mode,
                decision.status,
                decision.reason,
                float(decision.similarity),
                float(decision.confidence),
            )
        )
    policy_block = ""
    apply_enabled = bool(settings.get("apply_mode"))
    if apply_enabled and decision.would_apply:
        policy_block = format_policy_hint(decision)
        if decision_id:
            await db.mark_policy_decision_applied(int(decision_id), applied=True)
    return {
        "enabled": True,
        "mode": decision.mode,
        "policy_block": policy_block,
        "decision": decision,
        "decision_id": decision_id,
        "snapshot": snapshot.to_dict(),
    }


async def capture_intervention_episode(
    *,
    tenant_id: int,
    lead_id: int,
    channel: str,
    source_event: str,
    manager_message_id: int | None = None,
    log_fn: Callable[[str], Any] | None = None,
) -> int:
    if int(tenant_id or 0) <= 0 or int(lead_id or 0) <= 0:
        return 0
    cfg = read_tenant_config(int(tenant_id))
    settings = intervention_learning_settings(cfg)
    if settings.get("kill_switch") or not settings.get("enabled") or not settings.get("capture_enabled"):
        return 0

    await finalize_pending_episode_outcomes(tenant_id=int(tenant_id), lead_id=int(lead_id), log_fn=log_fn)
    rows = await db.get_recent_lead_messages(
        int(tenant_id),
        int(lead_id),
        limit=int(settings.get("episode_history_limit") or 24),
    )
    if not rows:
        return 0
    turns = stitch_messages(rows, within_seconds=int(settings.get("stitch_window_seconds") or 45))
    manager_idx = _manager_turn_index(turns, manager_message_id)
    if manager_idx < 0:
        return 0
    manager_turn = turns[manager_idx]
    user_idx = _latest_role_index(turns, "user", before=manager_idx)
    bot_idx = _latest_role_index(turns, "assistant", before=manager_idx)
    trigger_user_text = turns[user_idx].text if user_idx >= 0 else ""
    state = load_sales_state(int(tenant_id), int(lead_id))
    pre_manager_snapshot = build_dialogue_state_snapshot(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        contact_id=int(lead_id),
        channel=channel,
        state=state,
        stitched_history=turns[: manager_idx + 1],
        current_user_text=trigger_user_text,
    )
    pre_bot_snapshot = build_dialogue_state_snapshot(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        contact_id=int(lead_id),
        channel=channel,
        state=state,
        stitched_history=turns[: max(bot_idx + 1, 1)],
        current_user_text=trigger_user_text,
    )
    last_plan = dict(getattr(state, "last_plan", {}) or {})
    bot_turn = turns[bot_idx] if bot_idx >= 0 else None
    bot_action = classify_action(
        bot_turn.text if bot_turn else str(getattr(state, "last_bot_reply", "") or ""),
        last_plan=last_plan,
        pending_fact_key=str(getattr(state, "pending_fact_key", "") or ""),
        source_role="assistant",
    )
    manager_action = classify_action(
        manager_turn.text,
        last_plan=last_plan,
        pending_fact_key=str(getattr(state, "pending_fact_key", "") or ""),
        source_role="manager",
    )
    pre_bot_snapshot_id = await db.create_dialogue_state_snapshot(pre_bot_snapshot.to_dict())
    pre_manager_snapshot_id = await db.create_dialogue_state_snapshot(pre_manager_snapshot.to_dict())
    episode_id = await db.create_intervention_episode(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id),
        channel=str(channel or ""),
        source_event=source_event,
        trigger_user_text=trigger_user_text,
        pre_bot_snapshot_id=pre_bot_snapshot_id,
        pre_manager_snapshot_id=pre_manager_snapshot_id,
        bot_message_id=(bot_turn.message_ids[-1] if bot_turn and bot_turn.message_ids else None),
        manager_message_id=(manager_message_id or (manager_turn.message_ids[-1] if manager_turn.message_ids else None)),
        bot_reply_text=bot_turn.text if bot_turn else str(getattr(state, "last_bot_reply", "") or ""),
        manager_reply_text=manager_turn.text,
        bot_action=bot_action,
        manager_action=manager_action,
        stitched_dialogue=[turn.to_dict() for turn in turns],
        policy_key=pre_manager_snapshot.fingerprint,
    )
    if episode_id:
        await db.insert_episode_labels(
            int(episode_id),
            labels=[
                {"label_type": "bot_action", "label_key": "action", "label_value": bot_action, "confidence": float(bot_action.get("confidence") or 0.0)},
                {"label_type": "manager_action", "label_key": "action", "label_value": manager_action, "confidence": float(manager_action.get("confidence") or 0.0)},
                {"label_type": "source_event", "label_key": source_event, "label_value": {"channel": channel}, "confidence": 1.0},
            ],
        )
        decision = await db.get_recent_policy_decision_for_lead(
            int(tenant_id),
            int(lead_id),
            within_minutes=int(settings.get("decision_window_minutes") or 180),
        )
        if decision:
            manager_agreement = str(decision.get("recommended_action") or "") == str(manager_action.get("action") or "")
            await db.create_policy_outcome(
                tenant_id=int(tenant_id),
                lead_id=int(lead_id),
                episode_id=int(episode_id),
                decision_id=int(decision.get("id") or 0),
                reward=0.0,
                outcome_payload={"manager_agreement": manager_agreement, "manager_action": manager_action.get("action")},
                manager_agreement=manager_agreement,
                manager_action=str(manager_action.get("action") or ""),
            )
        if log_fn:
            log_fn(
                "event=learning_v2_episode_captured tenant=%s lead_id=%s episode_id=%s source_event=%s bot_action=%s manager_action=%s"
                % (
                    tenant_id,
                    lead_id,
                    int(episode_id),
                    source_event,
                    str(bot_action.get("action") or ""),
                    str(manager_action.get("action") or ""),
                )
            )
    return int(episode_id or 0)
