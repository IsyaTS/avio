from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class ReplyTrace:
    trace_id: str
    tenant_id: int
    lead_id: int | None
    channel: str
    user_text: str
    history_count: int
    pipeline_source: str
    reply_text: str
    fallback_used: bool = False
    fallback_source: str | None = None
    catalog_context_count: int | None = None
    dialog_examples_used: bool | None = None
    legacy_examples_used: bool | None = None
    policy_hint_used: bool | None = None
    prompt_hash: str | None = None
    model: str | None = None
    latency_ms: int | None = None
    quality_violations: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "tenant_id": self.tenant_id,
            "lead_id": self.lead_id,
            "channel": self.channel,
            "user_text": self.user_text,
            "history_count": self.history_count,
            "pipeline_source": self.pipeline_source,
            "reply_text": self.reply_text,
            "fallback_used": self.fallback_used,
            "fallback_source": self.fallback_source,
            "catalog_context_count": self.catalog_context_count,
            "dialog_examples_used": self.dialog_examples_used,
            "legacy_examples_used": self.legacy_examples_used,
            "policy_hint_used": self.policy_hint_used,
            "prompt_hash": self.prompt_hash,
            "model": self.model,
            "latency_ms": self.latency_ms,
            "quality_violations": self.quality_violations,
            "created_at": self.created_at,
        }
