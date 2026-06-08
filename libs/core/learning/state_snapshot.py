from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from .outcomes import repeated_question_hash
from .stitching import StitchedTurn


_NEGATIVE_RE = re.compile(r"(?:не нравится|дорого|долго|ужас|плохо|не то|не так|не подходит|сомневаюсь)", re.IGNORECASE)
_REPAIR_RE = re.compile(r"(?:не так|ошиб|имел в виду|не это|исправ)", re.IGNORECASE)
_COMPLAINT_RE = re.compile(r"(?:жалоб|проблем|разочар|возмущ|не устраивает)", re.IGNORECASE)
_PRICE_RE = re.compile(r"(?:цена|стоим|сколько|дешевле|бюджет|руб)", re.IGNORECASE)
_VARIANTS_RE = re.compile(r"(?:вариант|подбор|модель|что есть|покажите)", re.IGNORECASE)
_ATTR_RE = re.compile(r"(?:характерист|размер|цвет|материал|толщина|параметр)", re.IGNORECASE)
_REPAIR_TURN_RE = re.compile(r"(?:не тот|не такая|не такие|не это|исправьте|имел в виду)", re.IGNORECASE)
_HANDOFF_RE = re.compile(r"(?:менеджер|оператор|специалист|человек)", re.IGNORECASE)
_SELECTION_RE = re.compile(r"(?:беру|подходит|выбираю|оформляй|записывайте|давайте этот)", re.IGNORECASE)
_SCHEDULE_RE = re.compile(r"(?:когда можно|на завтра|на сегодня|запиш|замер|созвон)", re.IGNORECASE)
_LOW_SIGNAL_RE = re.compile(r"^(?:ок|да|нет|угу|понятно|ясно|хорошо|ага|интересно)\W*$", re.IGNORECASE)
_DIRECT_QUESTION_RE = re.compile(r"\?$")


@dataclass(frozen=True)
class DialogueStateSnapshot:
    tenant_id: int
    lead_id: int
    contact_id: int
    channel: str
    feature_version: str
    current_user_text: str
    user_intent: str
    unresolved_ask: str
    known_facts: dict[str, str] = field(default_factory=dict)
    known_fact_keys: list[str] = field(default_factory=list)
    pending_fact_key: str = ""
    last_plan: dict[str, Any] = field(default_factory=dict)
    last_plan_action: str = ""
    last_bot_reply: str = ""
    catalog_summary: list[dict[str, Any]] = field(default_factory=list)
    stitched_history: list[dict[str, Any]] = field(default_factory=list)
    has_price_context: bool = False
    has_shortlist: bool = False
    has_cta: bool = False
    has_handoff: bool = False
    frustration_signal: bool = False
    repeated_question_signal: bool = False
    repair_signal: bool = False
    complaint_signal: bool = False
    after_catalog: bool = False
    after_pdf: bool = False
    low_signal_user: bool = False
    split_message_present: bool = False
    direct_question: bool = False
    fingerprint_payload: dict[str, Any] = field(default_factory=dict)
    fingerprint: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "lead_id": self.lead_id,
            "contact_id": self.contact_id,
            "channel": self.channel,
            "feature_version": self.feature_version,
            "current_user_text": self.current_user_text,
            "user_intent": self.user_intent,
            "unresolved_ask": self.unresolved_ask,
            "known_facts": self.known_facts,
            "known_fact_keys": self.known_fact_keys,
            "pending_fact_key": self.pending_fact_key,
            "last_plan": self.last_plan,
            "last_plan_action": self.last_plan_action,
            "last_bot_reply": self.last_bot_reply,
            "catalog_summary": self.catalog_summary,
            "stitched_history": self.stitched_history,
            "has_price_context": self.has_price_context,
            "has_shortlist": self.has_shortlist,
            "has_cta": self.has_cta,
            "has_handoff": self.has_handoff,
            "frustration_signal": self.frustration_signal,
            "repeated_question_signal": self.repeated_question_signal,
            "repair_signal": self.repair_signal,
            "complaint_signal": self.complaint_signal,
            "after_catalog": self.after_catalog,
            "after_pdf": self.after_pdf,
            "low_signal_user": self.low_signal_user,
            "split_message_present": self.split_message_present,
            "direct_question": self.direct_question,
            "fingerprint_payload": self.fingerprint_payload,
            "fingerprint": self.fingerprint,
        }


def infer_user_intent(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "unknown"
    if _HANDOFF_RE.search(raw):
        return "handoff"
    if _SELECTION_RE.search(raw):
        return "selection"
    if _SCHEDULE_RE.search(raw):
        return "schedule"
    if _COMPLAINT_RE.search(raw):
        return "complaint"
    if _REPAIR_TURN_RE.search(raw):
        return "repair"
    if _PRICE_RE.search(raw):
        return "price"
    if _VARIANTS_RE.search(raw):
        return "variants"
    if _ATTR_RE.search(raw):
        return "attributes"
    if _LOW_SIGNAL_RE.match(raw):
        return "low_signal"
    if raw.endswith("?"):
        return "question"
    return "unknown"


def _catalog_summary(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for item in list(items or [])[:4]:
        title = str(item.get("title") or item.get("name") or "").strip()
        if not title:
            continue
        summary.append(
            {
                "title": title[:120],
                "price": str(item.get("price") or "").strip()[:40],
            }
        )
    return summary


def _resolve_cta(history: Sequence[StitchedTurn], last_reply: str) -> bool:
    haystack = "\n".join([turn.text for turn in history[-4:]] + [last_reply])
    return bool(re.search(r"(?:оставьте|пришлите номер|созвон|запиш|перезвон)", haystack, re.IGNORECASE))


def build_dialogue_state_snapshot(
    *,
    tenant_id: int,
    lead_id: int,
    contact_id: int,
    channel: str,
    state: Any,
    stitched_history: Sequence[StitchedTurn],
    current_user_text: str,
) -> DialogueStateSnapshot:
    history = list(stitched_history or [])
    facts = dict(getattr(state, "facts", {}) or {})
    known_slots = dict(getattr(state, "known_slots", {}) or {})
    merged_facts = {str(k): str(v) for k, v in {**known_slots, **facts}.items() if str(v or "").strip()}
    pending_fact_key = str(getattr(state, "pending_fact_key", "") or "").strip().lower()
    last_plan = dict(getattr(state, "last_plan", {}) or {})
    last_plan_action = str(last_plan.get("action") or "").strip().lower()
    last_bot_reply = str(getattr(state, "last_bot_reply", "") or "").strip()
    current_user = str(current_user_text or "").strip()
    user_intent = infer_user_intent(current_user)
    unresolved_ask = current_user if current_user.endswith("?") or user_intent in {"price", "variants", "attributes", "repair", "complaint", "selection", "schedule"} else current_user[:220]
    catalog_items = _catalog_summary(getattr(state, "last_items", []) or [])
    user_turns = [turn for turn in history if turn.role == "user"]
    repeated_question_signal = False
    if len(user_turns) >= 2:
        repeated_question_signal = repeated_question_hash(user_turns[-1].text) == repeated_question_hash(user_turns[-2].text)
    history_text = "\n".join(turn.text for turn in history[-6:])
    has_price_context = bool(_PRICE_RE.search(history_text) or _PRICE_RE.search(last_bot_reply))
    has_shortlist = bool(catalog_items) or bool(re.search(r"(?:1\.|2\.|вариант|подбор|модел)", history_text, re.IGNORECASE))
    split_message_present = any(turn.is_stitched for turn in history)
    after_catalog = bool(getattr(state, "catalog_sent", False))
    after_pdf = str(getattr(state, "catalog_delivery_mode", "") or "").strip().lower() == "pdf"
    frustration_signal = bool(_NEGATIVE_RE.search(current_user) or _NEGATIVE_RE.search(history_text))
    repair_signal = bool(_REPAIR_RE.search(current_user) or _REPAIR_RE.search(history_text))
    complaint_signal = bool(_COMPLAINT_RE.search(current_user) or _COMPLAINT_RE.search(history_text))
    low_signal_user = bool(_LOW_SIGNAL_RE.match(current_user))
    direct_question = bool(_DIRECT_QUESTION_RE.search(current_user))
    has_handoff = bool(_HANDOFF_RE.search(history_text) or last_plan_action == "handoff")
    has_cta = _resolve_cta(history, last_bot_reply)

    fingerprint_payload = {
        "intent": user_intent,
        "pending_fact_key": pending_fact_key,
        "last_plan_action": last_plan_action,
        "known_fact_keys": sorted(merged_facts.keys()),
        "has_price_context": has_price_context,
        "has_shortlist": has_shortlist,
        "has_cta": has_cta,
        "has_handoff": has_handoff,
        "frustration_signal": frustration_signal,
        "repair_signal": repair_signal,
        "complaint_signal": complaint_signal,
        "after_catalog": after_catalog,
        "after_pdf": after_pdf,
        "split_message_present": split_message_present,
        "repeated_question_signal": repeated_question_signal,
    }
    fingerprint = hashlib.sha1(json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    return DialogueStateSnapshot(
        tenant_id=int(tenant_id or 0),
        lead_id=int(lead_id or 0),
        contact_id=int(contact_id or 0),
        channel=str(channel or "").strip() or "unknown",
        feature_version="learning_v2:1",
        current_user_text=current_user,
        user_intent=user_intent,
        unresolved_ask=unresolved_ask,
        known_facts=merged_facts,
        known_fact_keys=sorted(merged_facts.keys()),
        pending_fact_key=pending_fact_key,
        last_plan=last_plan,
        last_plan_action=last_plan_action,
        last_bot_reply=last_bot_reply,
        catalog_summary=catalog_items,
        stitched_history=[turn.to_dict() for turn in history[-12:]],
        has_price_context=has_price_context,
        has_shortlist=has_shortlist,
        has_cta=has_cta,
        has_handoff=has_handoff,
        frustration_signal=frustration_signal,
        repeated_question_signal=repeated_question_signal,
        repair_signal=repair_signal,
        complaint_signal=complaint_signal,
        after_catalog=after_catalog,
        after_pdf=after_pdf,
        low_signal_user=low_signal_user,
        split_message_present=split_message_present,
        direct_question=direct_question,
        fingerprint_payload=fingerprint_payload,
        fingerprint=fingerprint,
    )
