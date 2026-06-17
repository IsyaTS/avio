from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class LLMReply(str):
    """String wrapper that carries planner/enforcement diagnostics for logging."""

    __slots__ = ("llm_plan", "llm_raw_answer", "reply_metadata")

    def __new__(
        cls,
        content: str,
        *,
        plan: Optional[Dict[str, Any]] = None,
        raw_answer: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "LLMReply":
        obj = str.__new__(cls, content)
        obj.llm_plan = plan
        obj.llm_raw_answer = raw_answer
        obj.reply_metadata = metadata if metadata is not None else {}
        return obj


@dataclass(frozen=True)
class ReplyRuntimeDeps:
    style_guard: str


class ReplyRuntime:
    def __init__(self, deps: ReplyRuntimeDeps) -> None:
        self.deps = deps

    def wrap_llm_reply(
        self,
        text: str,
        *,
        plan: Optional[Any] = None,
        raw_answer: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> LLMReply:
        text = _limit_final_questions(str(text or ""), max_questions=1)
        plan_payload: Optional[Dict[str, Any]] = None
        if plan is not None:
            if isinstance(plan, dict):
                plan_payload = dict(plan)
            elif hasattr(plan, "to_dict"):
                try:
                    plan_payload = plan.to_dict()  # type: ignore[attr-defined]
                except Exception:
                    plan_payload = None
        return LLMReply(
            text,
            plan=plan_payload,
            raw_answer=raw_answer if raw_answer is not None else text,
            metadata=metadata,
        )

    def build_human_mode_messages(self, messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
        prepared: List[Dict[str, str]] = []
        system_chunks: List[str] = []
        for item in messages:
            role = str(item.get("role") or "").strip().lower()
            content = str(item.get("content") or "").strip()
            if role == "system" and content:
                system_chunks.append(content)
            elif role in {"user", "assistant"} and content:
                prepared.append({"role": role, "content": content})
        merged_system = "\n\n".join(chunk for chunk in system_chunks if chunk)
        if merged_system:
            merged_system = f"{merged_system}\n\nПравила стиля:\n{self.deps.style_guard}"
        else:
            merged_system = self.deps.style_guard
        out: List[Dict[str, str]] = [{"role": "system", "content": merged_system}]
        out.extend(prepared[-10:])
        return out


def _limit_final_questions(text: str, max_questions: int = 1) -> str:
    candidate = str(text or "").strip()
    if not candidate:
        return candidate
    question_cue = re.compile(
        r"(?iu)\b(как\s+удобнее|есть\s+ли|како[йеая]|какие|где|когда|сколько|что|"
        r"подскажите|уточните|нужен\s+ли|нужна\s+ли|можете\s+ли|хотите\s+ли)\b"
    )
    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+|\n+", candidate) if part.strip()]
    kept: list[str] = []
    questions_left = max(0, int(max_questions))
    for part in parts:
        if "?" not in part:
            if questions_left <= 0 and question_cue.search(part):
                continue
            kept.append(part)
            continue
        if questions_left <= 0:
            statement = part.replace("?", ".").strip()
            if question_cue.search(statement):
                continue
            kept.append(statement)
            continue
        first_q = part.find("?")
        kept.append(part[: first_q + 1].strip())
        questions_left -= 1
    return " ".join(kept).strip()
