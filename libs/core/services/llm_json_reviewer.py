from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from libs.core.services.avito_training_candidate_builder import AvitoTrainingCandidate


PROMPT_VERSION = "avito_training_reviewer_v1"


@dataclass(frozen=True)
class LLMJsonReviewerConfig:
    enabled: bool
    model: str = "gpt-4o-mini"
    timeout_seconds: float = 20.0


class LLMJsonReviewer:
    def __init__(self, *, client: Any, config: LLMJsonReviewerConfig) -> None:
        self._client = client
        self._config = config

    async def review_candidate(self, candidate: AvitoTrainingCandidate) -> dict[str, Any]:
        if not self._config.enabled:
            raise RuntimeError("llm_reviewer_disabled")
        if _use_responses_api(self._config.model):
            return await self._review_candidate_with_responses(candidate)
        messages = [
            {"role": "system", "content": _system_prompt()},
            {"role": "user", "content": json.dumps(_candidate_payload(candidate), ensure_ascii=False)},
        ]
        create = self._client.chat.completions.create
        result = create(
            model=self._config.model,
            messages=messages,
            temperature=0,
            response_format={"type": "json_object"},
            timeout=self._config.timeout_seconds,
        )
        if hasattr(result, "__await__"):
            result = await result
        content = result.choices[0].message.content
        return json.loads(content)

    async def _review_candidate_with_responses(self, candidate: AvitoTrainingCandidate) -> dict[str, Any]:
        create = self._client.responses.create
        result = create(
            model=self._config.model,
            input=[
                {"role": "system", "content": _system_prompt()},
                {"role": "user", "content": json.dumps(_candidate_payload(candidate), ensure_ascii=False)},
            ],
            text={"format": {"type": "json_object"}},
            timeout=self._config.timeout_seconds,
        )
        if hasattr(result, "__await__"):
            result = await result
        content = getattr(result, "output_text", "") or ""
        return json.loads(content)


def build_default_reviewer() -> LLMJsonReviewer | None:
    enabled = str(os.getenv("AVITO_TRAINING_AI_REVIEW", "")).strip().lower() in {"1", "true", "yes", "on"}
    if not enabled or not os.getenv("OPENAI_API_KEY"):
        return None
    try:
        from openai import AsyncOpenAI  # type: ignore
    except Exception:
        return None
    model = os.getenv("AVITO_TRAINING_AI_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini"
    return LLMJsonReviewer(client=AsyncOpenAI(), config=LLMJsonReviewerConfig(enabled=True, model=model))


def _use_responses_api(model: str) -> bool:
    normalized = str(model or "").lower()
    return normalized.startswith("gpt-5") or normalized.startswith("o")


def _candidate_payload(candidate: AvitoTrainingCandidate) -> dict[str, Any]:
    return {
        "prompt_version": PROMPT_VERSION,
        "candidate_id": candidate.candidate_id,
        "context": [dict(item) for item in candidate.context],
        "ideal_reply": dict(candidate.ideal_reply),
        "soft_flags": list(candidate.soft_flags),
        "rule_score": int(candidate.rule_score),
    }


def _system_prompt() -> str:
    return """
You are a quality reviewer for JSONL training examples used to teach a Russian-language sales assistant.

Task
- Review one candidate example: previous dialog context plus one manager ideal_reply.
- Decide whether ideal_reply is useful as an imitation target for a future sales assistant.
- Your main goal is not to repeat regex filtering. Your main goal is to select examples that will improve future assistant replies if the assistant learns to imitate them.
- Imagine that a future bot will copy this behavior in a similar situation:
  - if copying it would improve sales dialog quality, accept it;
  - if copying it would make answers worse, reject it;
  - if you are unsure, send it to manual review.
- Do not rewrite, improve, shorten, expand, translate, or normalize the manager reply.
- Soft flags and rule_score are hints only. They are not final decisions.
- The niche can be any sales/service business, so judge the sales usefulness, not a specific product category.
- Be selective. The training JSONL should contain examples you would be comfortable letting a bot imitate directly.

Return only a valid JSON object with this shape:
{
  "decision": "accept_training" | "reject_training" | "needs_manual_review",
  "score": 0-100,
  "reason_code": "short_machine_code",
  "reason": "short explanation without quoting private text",
  "tags": ["tag1", "tag2"]
}

Accept as training when the manager reply is something a good assistant should learn to imitate in a similar context:
- answers the customer's question or need;
- asks a useful clarifying question that moves the sale forward;
- explains price, availability, product fit, measurements, options, delivery, installation, payment, timing, warranty, address, or working conditions;
- gives a concrete next step that is relevant to the customer's request;
- naturally confirms or closes a small step in the conversation;
- is short but appropriate in context, for example a short clarifier like "Сколько?", "Какой размер?", "Откуда вы?";
- contains a contact mask such as [PHONE], [LINK], [EMAIL], [HANDLE] but also contains useful sales context;
- uses repeated/template manager wording, if the reply is still useful and relevant.
- has ordinary human manager wording, even if it is not polished, as long as it is a useful behavior to imitate.
- is at least roughly how you would answer in the same situation, or close enough that imitation would improve the bot.

Do not reject only because:
- the reply is short;
- the manager uses a common template;
- the wording is not perfect;
- there are masked contacts;
- the example has soft flags but the reply is still a useful next answer.

Reject from training only when learning from this example would likely hurt future assistant quality:
- service-only status: only "отправили", "скинули", "написали", "ждем", "передали", "по ватсап", "открыли каталог" without useful sales content;
- follow-up ping only: "актуально?", "что решили?", "открыли каталог?", "посмотрели?", "вам интересно?";
- contact-only or link-only reply, even if the contact is masked;
- pure transfer away from Avito to a phone/messenger/catalog without answering the customer's need;
- autoresponder, broadcast, unsubscribe text, or message that looks like mass mailing;
- garbage, duplicated broken text, role confusion, or impossible-to-use fragment;
- reply depends on missing previous context and the current context is not enough to understand why it is good;
- manager asks or says something that is irrelevant to the customer's latest meaningful message.
- the reusable behavior is mostly collecting a phone number, sending a catalog, or moving the customer to another messenger, instead of helping in the current chat.

Use needs_manual_review when:
- the example might be useful but depends on business-specific judgment;
- it mixes useful sales content with suspicious service/contact-transfer behavior;
- context may start in the middle of a dialog;
- the reply is understandable but too ambiguous to safely accept or reject.
- the reply contains a long repeated contact/catalog transfer template plus some useful information; this may be useful for the business, but it is risky as a general training target.
- you personally would answer better in the same situation, but the manager reply is not clearly bad.

Scoring
- 85-100: clearly useful imitation target; a bot copying this behavior should improve.
- 60-84: useful enough for training, with minor caveats.
- 35-59: uncertain quality or business-specific; usually needs_manual_review.
- 0-34: bad imitation target; learning this would likely make the bot worse.

Reason codes
Use concise machine-readable codes such as:
- useful_sales_reply
- useful_clarifier
- useful_next_step
- useful_conditions_explanation
- service_status_only
- followup_ping_only
- contact_only
- messenger_transfer_only
- autoresponder_or_broadcast
- missing_context
- irrelevant_reply
- ambiguous_review

Privacy
- Do not include phone numbers, customer names, addresses, exact raw customer text, or long quotes in reason.
- The input may contain masks like [PHONE], [LINK], [EMAIL], [HANDLE]. Keep them as masks if referenced.
""".strip()


__all__ = ["LLMJsonReviewer", "LLMJsonReviewerConfig", "PROMPT_VERSION", "build_default_reviewer"]
