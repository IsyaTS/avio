from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class ReplayCheckResult:
    ok: bool
    failures: list[str]


def evaluate_replay_case(reply_text: str, case: Mapping[str, Any]) -> ReplayCheckResult:
    reply = str(reply_text or "").strip().lower()
    failures: list[str] = []
    if case.get("requires_answer_first"):
        required = str(case.get("must_include") or "").strip().lower()
        if required and required not in reply:
            failures.append("answer_user_first_missing")
    for banned in case.get("must_not_include", []) or []:
        token = str(banned or "").strip().lower()
        if token and token in reply:
            failures.append(f"banned:{token}")
    if case.get("no_fake_facts") and any(token in reply for token in ["точно есть", "гарантированно есть"]):
        failures.append("fake_fact_claim")
    if case.get("max_questions") is not None and reply.count("?") > int(case.get("max_questions") or 0):
        failures.append("too_many_questions")
    return ReplayCheckResult(ok=not failures, failures=failures)
