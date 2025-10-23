from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


class PlannerError(RuntimeError):
    """Raised when the LLM planning step fails."""


@dataclass
class GeneratedPlan:
    """Structured representation of the strategy for the next reply."""

    analysis: str = ""
    stage: str = ""
    next_questions: List[str] = field(default_factory=list)
    cta: str = ""
    tone: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    def as_instruction(self) -> str:
        """Convert the plan into a compact textual instruction for the model."""

        parts: List[str] = []
        if self.analysis:
            parts.append(f"Анализ: {self.analysis.strip()}")
        if self.stage:
            parts.append(f"Этап сделки: {self.stage.strip()}")
        if self.next_questions:
            joined = "; ".join(q.strip() for q in self.next_questions if q.strip())
            if joined:
                parts.append(f"Ключевые вопросы: {joined}")
        if self.cta:
            parts.append(f"CTA: {self.cta.strip()}")
        if self.tone:
            parts.append(f"Тон: {self.tone.strip()}")
        return "\n".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "analysis": self.analysis,
            "stage": self.stage,
            "next_questions": list(self.next_questions),
            "cta": self.cta,
            "tone": self.tone,
            "raw": dict(self.raw),
        }


async def generate_sales_reply(
    messages: Sequence[Dict[str, str]],
    *,
    openai_module: Any,
    model: str,
    timeout: float,
    persona_language: Optional[str] = None,
) -> Tuple[GeneratedPlan, str]:
    """Generate a planned sales reply using a two-phase LLM approach."""

    if not messages:
        raise PlannerError("no messages provided")

    dialogue_tail = _extract_dialogue(messages, limit=8)
    context_block = _extract_system(messages)

    plan_prompt = _build_plan_prompt(dialogue_tail, context_block, persona_language)
    plan_response = await _call_chat_completion(
        openai_module,
        model,
        plan_prompt,
        timeout,
        temperature=0.2,
        max_tokens=320,
        top_p=0.7,
        frequency_penalty=0.0,
        presence_penalty=0.0,
    )

    plan = _parse_plan_response(_get_message_content(plan_response))

    final_prompt = _build_reply_prompt(messages, plan)
    final_response = await _call_chat_completion(
        openai_module,
        model,
        final_prompt,
        timeout,
        temperature=0.7,
        max_tokens=260,
        top_p=0.9,
        frequency_penalty=0.2,
        presence_penalty=0.05,
    )

    reply = (_get_message_content(final_response) or "").strip()
    if not reply:
        raise PlannerError("empty reply")

    return plan, reply


async def _call_chat_completion(
    openai_module: Any,
    model: str,
    messages: Sequence[Dict[str, str]],
    timeout: float,
    *,
    temperature: float,
    max_tokens: int,
    top_p: Optional[float] = None,
    frequency_penalty: Optional[float] = None,
    presence_penalty: Optional[float] = None,
) -> Any:
    """Invoke the OpenAI chat completion API in a thread executor."""

    return await asyncio.to_thread(
        openai_module.chat.completions.create,
        model=model,
        messages=list(messages),
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
        top_p=top_p if top_p is not None else 1.0,
        frequency_penalty=frequency_penalty if frequency_penalty is not None else 0.0,
        presence_penalty=presence_penalty if presence_penalty is not None else 0.0,
    )


def _extract_dialogue(messages: Sequence[Dict[str, str]], limit: int = 6) -> List[Dict[str, str]]:
    convo = [
        {"role": msg.get("role", ""), "content": msg.get("content", "")}
        for msg in messages
        if msg.get("role") in {"user", "assistant"}
    ]
    if limit and len(convo) > limit:
        convo = convo[-limit:]
    return convo


def _extract_system(messages: Sequence[Dict[str, str]]) -> str:
    for msg in messages:
        if msg.get("role") == "system" and msg.get("content"):
            return msg["content"]
    return ""


def _build_plan_prompt(
    dialogue: Iterable[Dict[str, str]],
    context_block: str,
    persona_language: Optional[str],
) -> List[Dict[str, str]]:
    language_hint = persona_language or "русский"
    system_text = (
        "Ты — опытный тимлид отдела продаж. На основе контекста разговора составь JSON без комментариев. "
        "Структура: {\"analysis\": str, \"stage\": str, \"next_questions\": [str], \"cta\": str, \"tone\": str}. "
        "Используй язык ответа: "
        f"{language_hint}."
    )
    user_payload = {
        "context": context_block,
        "dialogue": list(dialogue),
    }
    return [
        {"role": "system", "content": system_text},
        {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
    ]


def _build_reply_prompt(
    original_messages: Sequence[Dict[str, str]],
    plan: GeneratedPlan,
) -> List[Dict[str, str]]:
    conversation: List[Dict[str, str]] = [
        {"role": msg.get("role", ""), "content": msg.get("content", "")}
        for msg in original_messages
    ]
    plan_instruction = (
        "Используй план, сформированный ранее, чтобы дать убедительный ответ. "
        "Учитывай этап сделки и задай один из предложенных вопросов, если это уместно. "
        "Обязательно используй CTA."
    )
    conversation.append({"role": "assistant", "content": f"План ответа:\n{plan.as_instruction()}"})
    conversation.append({"role": "user", "content": plan_instruction})
    return conversation


def _parse_plan_response(raw: str) -> GeneratedPlan:
    if not raw:
        raise PlannerError("empty plan response")

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.debug("plan parse failed: %s", raw)
        raise PlannerError("plan response is not valid JSON") from exc

    analysis = str(payload.get("analysis", "")).strip()
    stage = str(payload.get("stage", "")).strip()
    tone = str(payload.get("tone", "")).strip()
    cta = str(payload.get("cta", "")).strip()

    questions_raw = payload.get("next_questions") or []
    if isinstance(questions_raw, str):
        questions = [q.strip() for q in re.split(r"[\n;]+", questions_raw) if q.strip()]  # type: ignore[name-defined]
    elif isinstance(questions_raw, Iterable):
        questions = [str(item).strip() for item in questions_raw if str(item).strip()]
    else:
        questions = []

    plan = GeneratedPlan(
        analysis=analysis,
        stage=stage,
        next_questions=questions,
        cta=cta,
        tone=tone,
        raw=payload,
    )
    return plan


def _get_message_content(response: Any) -> str:
    try:
        return response.choices[0].message.content  # type: ignore[attr-defined]
    except AttributeError:
        try:
            return response["choices"][0]["message"]["content"]  # type: ignore[index]
        except Exception:
            return ""


# Late import to avoid circular dependency
import re  # noqa: E402  # isort:skip
