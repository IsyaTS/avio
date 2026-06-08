from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class SalesState:
    tenant: int
    contact_id: int
    channel: str = "whatsapp"
    needs: Dict[str, Any] = field(default_factory=dict)
    spin: Dict[str, str] = field(
        default_factory=lambda: {stage: "pending" for stage in ("s", "p", "i", "n")}
    )
    bant: Dict[str, Any] = field(default_factory=dict)
    asked_questions: List[str] = field(default_factory=list)
    asked_question_fingerprints: List[str] = field(default_factory=list)
    challenger_cursor: int = 0
    social_proof_cursor: int = 0
    scarcity_cursor: int = 0
    reciprocity_cursor: int = 0
    history: List[Dict[str, str]] = field(default_factory=list)
    last_items: List[Dict[str, Any]] = field(default_factory=list)
    last_bot_reply: str = ""
    last_user_text: str = ""
    last_updated_ts: float = field(default_factory=lambda: time.time())
    conversion_score: float = 0.0
    catalog_sent: bool = False
    catalog_sent_at: float = 0.0
    catalog_delivery_mode: str = ""
    last_plan: Dict[str, Any] = field(default_factory=dict)
    profile: Dict[str, Any] = field(default_factory=dict)
    sentiment_score: float = 0.0
    user_message_count: int = 0
    last_question_text: str = ""
    cta_last_text: str = ""
    cta_last_sent_ts: float = 0.0
    known_slots: Dict[str, str] = field(default_factory=dict)
    pending_slot: str = ""
    recent_fact_fingerprints: List[str] = field(default_factory=list)
    facts: Dict[str, str] = field(default_factory=dict)
    pending_fact_key: str = ""

    def to_dict(self) -> dict:
        return {
            "tenant": self.tenant,
            "contact_id": self.contact_id,
            "channel": self.channel,
            "needs": self.needs,
            "spin": self.spin,
            "bant": self.bant,
            "asked_questions": self.asked_questions,
            "asked_question_fingerprints": self.asked_question_fingerprints[-32:],
            "challenger_cursor": self.challenger_cursor,
            "social_proof_cursor": self.social_proof_cursor,
            "scarcity_cursor": self.scarcity_cursor,
            "reciprocity_cursor": self.reciprocity_cursor,
            "history": self.history[-20:],
            "last_items": self.last_items[-8:],
            "last_bot_reply": self.last_bot_reply,
            "last_user_text": self.last_user_text,
            "last_updated_ts": self.last_updated_ts,
            "conversion_score": self.conversion_score,
            "catalog_sent": self.catalog_sent,
            "catalog_sent_at": self.catalog_sent_at,
            "catalog_delivery_mode": self.catalog_delivery_mode,
            "last_plan": self.last_plan,
            "profile": self.profile,
            "sentiment_score": self.sentiment_score,
            "user_message_count": self.user_message_count,
            "last_question_text": self.last_question_text,
            "cta_last_text": self.cta_last_text,
            "cta_last_sent_ts": self.cta_last_sent_ts,
            "known_slots": self.known_slots,
            "pending_slot": self.pending_slot,
            "recent_fact_fingerprints": self.recent_fact_fingerprints[-64:],
            "facts": self.facts,
            "pending_fact_key": self.pending_fact_key,
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "SalesState":
        payload = payload or {}
        tenant = int(payload.get("tenant", 0))
        contact_id = int(payload.get("contact_id", 0))
        obj = cls(tenant=tenant, contact_id=contact_id)
        obj.channel = payload.get("channel", obj.channel)
        obj.needs = payload.get("needs", {}) or {}
        obj.spin = payload.get("spin", obj.spin) or {
            stage: "pending" for stage in ("s", "p", "i", "n")
        }
        obj.bant = payload.get("bant", {}) or {}
        obj.asked_questions = payload.get("asked_questions", []) or []
        obj.asked_question_fingerprints = payload.get("asked_question_fingerprints", []) or []
        obj.challenger_cursor = int(payload.get("challenger_cursor", 0))
        obj.social_proof_cursor = int(payload.get("social_proof_cursor", 0))
        obj.scarcity_cursor = int(payload.get("scarcity_cursor", 0))
        obj.reciprocity_cursor = int(payload.get("reciprocity_cursor", 0))
        obj.history = payload.get("history", []) or []
        obj.last_items = payload.get("last_items", []) or []
        obj.last_bot_reply = payload.get("last_bot_reply", "") or ""
        obj.last_user_text = payload.get("last_user_text", "") or ""
        obj.last_updated_ts = float(payload.get("last_updated_ts", time.time()))
        obj.conversion_score = float(payload.get("conversion_score", 0.0))
        obj.catalog_sent = bool(payload.get("catalog_sent", False))
        obj.catalog_sent_at = float(payload.get("catalog_sent_at", 0.0) or 0.0)
        obj.catalog_delivery_mode = payload.get("catalog_delivery_mode", "") or ""
        obj.last_plan = payload.get("last_plan", {}) or {}
        obj.profile = payload.get("profile", {}) or {}
        try:
            obj.sentiment_score = float(payload.get("sentiment_score", 0.0))
        except Exception:
            obj.sentiment_score = 0.0
        obj.user_message_count = int(payload.get("user_message_count", 0))
        obj.last_question_text = payload.get("last_question_text", "") or ""
        obj.cta_last_text = payload.get("cta_last_text", "") or ""
        try:
            obj.cta_last_sent_ts = float(payload.get("cta_last_sent_ts", 0.0) or 0.0)
        except Exception:
            obj.cta_last_sent_ts = 0.0
        obj.known_slots = payload.get("known_slots", {}) or {}
        if not isinstance(obj.known_slots, dict):
            obj.known_slots = {}
        obj.pending_slot = payload.get("pending_slot", "") or ""
        obj.recent_fact_fingerprints = payload.get("recent_fact_fingerprints", []) or []
        if not isinstance(obj.recent_fact_fingerprints, list):
            obj.recent_fact_fingerprints = []
        obj.facts = payload.get("facts", {}) or {}
        if not isinstance(obj.facts, dict):
            obj.facts = {}
        obj.pending_fact_key = str(payload.get("pending_fact_key", "") or "").strip()
        return obj

    def append_history(self, role: str, content: str) -> None:
        if not content:
            return
        content = content.strip()
        if not content:
            return
        if (
            self.history
            and self.history[-1].get("role") == role
            and self.history[-1].get("content") == content
        ):
            return
        self.history.append({"role": role, "content": content})
        if len(self.history) > 24:
            self.history = self.history[-24:]

    def mark_spin_stage(self, stage: str, status: str) -> None:
        if stage not in self.spin:
            self.spin[stage] = status
        else:
            order = {"pending": 0, "asked": 1, "covered": 2}
            if order.get(status, 0) >= order.get(self.spin.get(stage, "pending"), 0):
                self.spin[stage] = status


@dataclass
class PersonaStepRule:
    fact_key: str
    source_line: str
    question: str = ""


@dataclass
class PersonaConditionalRule:
    source_line: str
    condition_text: str
    action_text: str
    fact_key: str = ""
    expected_tokens: List[str] = field(default_factory=list)


@dataclass
class PersonaDeliveryRule:
    source_line: str
    channel_scope: List[str] = field(default_factory=list)
    condition_text: str = ""
    expected_tokens: List[str] = field(default_factory=list)
    wants_handle: bool = False
    wants_phone: bool = False
    wants_link: bool = False
    min_assistant_gap: int = 2


@dataclass
class PersonaCompiledRules:
    steps: List[PersonaStepRule] = field(default_factory=list)
    conditionals: List[PersonaConditionalRule] = field(default_factory=list)
    delivery_rules: List[PersonaDeliveryRule] = field(default_factory=list)
    contact_artifacts: List[str] = field(default_factory=list)
