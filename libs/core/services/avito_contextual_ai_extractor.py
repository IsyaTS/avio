from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Sequence

from libs.core.services.avito_contextual_case_builder import AvitoContextualCaseCandidate


PROMPT_VERSION = "avito_contextual_domain_extractor_v2"


@dataclass(frozen=True)
class AvitoContextualAIExtractorConfig:
    enabled: bool = True
    model: str = "gpt-5.2"
    timeout_seconds: float = 30.0
    concurrency: int = 4


@dataclass(frozen=True)
class AvitoContextualAIExtractionResult:
    extractions: dict[str, dict[str, Any]]
    extracted_count: int
    failed_count: int
    errors: dict[str, int] = field(default_factory=dict)


ProgressCallback = Callable[[AvitoContextualAIExtractionResult], Awaitable[None]]


class AvitoContextualAIExtractor:
    def __init__(self, *, client: Any, config: AvitoContextualAIExtractorConfig) -> None:
        self._client = client
        self._config = config
        self._cache: dict[str, dict[str, Any]] = {}

    async def extract_case(
        self,
        candidate: AvitoContextualCaseCandidate,
        *,
        rule_extraction: Mapping[str, Any] | None = None,
        domain_schema: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not self._config.enabled:
            raise RuntimeError("contextual_ai_disabled")
        if candidate.case_id in self._cache:
            return dict(self._cache[candidate.case_id])
        payload = _candidate_payload(candidate, rule_extraction=rule_extraction, domain_schema=domain_schema)
        if _use_responses_api(self._config.model):
            extracted = await self._extract_with_responses(payload)
        else:
            extracted = await self._extract_with_chat(payload)
        normalized = _normalize_extraction(extracted)
        self._cache[candidate.case_id] = normalized
        return dict(normalized)

    async def _extract_with_responses(self, payload: dict[str, Any]) -> dict[str, Any]:
        create = self._client.responses.create
        result = create(
            model=self._config.model,
            input=[
                {"role": "system", "content": _system_prompt()},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            text={"format": {"type": "json_object"}},
            timeout=self._config.timeout_seconds,
        )
        if hasattr(result, "__await__"):
            result = await result
        return json.loads(getattr(result, "output_text", "") or "{}")

    async def _extract_with_chat(self, payload: dict[str, Any]) -> dict[str, Any]:
        create = self._client.chat.completions.create
        result = create(
            model=self._config.model,
            messages=[
                {"role": "system", "content": _system_prompt()},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0,
            response_format={"type": "json_object"},
            timeout=self._config.timeout_seconds,
        )
        if hasattr(result, "__await__"):
            result = await result
        return json.loads(result.choices[0].message.content)


async def extract_cases(
    candidates: Sequence[AvitoContextualCaseCandidate],
    *,
    rule_extractions: Mapping[str, Mapping[str, Any]] | None = None,
    domain_schema: Mapping[str, Any] | None = None,
    extractor: AvitoContextualAIExtractor | None = None,
    enabled: bool = True,
    concurrency: int = 4,
    progress_callback: ProgressCallback | None = None,
    progress_interval: int = 10,
) -> AvitoContextualAIExtractionResult:
    if not enabled or extractor is None or not candidates:
        return AvitoContextualAIExtractionResult({}, 0, 0, {})
    semaphore = asyncio.Semaphore(max(1, min(int(concurrency or 4), 16)))
    extractions: dict[str, dict[str, Any]] = {}
    errors: dict[str, int] = {}
    lock = asyncio.Lock()
    completed = 0

    async def run_one(candidate: AvitoContextualCaseCandidate) -> None:
        nonlocal completed
        async with semaphore:
            try:
                extracted = await extractor.extract_case(
                    candidate,
                    rule_extraction=(rule_extractions or {}).get(candidate.case_id),
                    domain_schema=domain_schema,
                )
                error_key = None
            except Exception as exc:
                key = type(exc).__name__ or "ai_error"
                extracted = None
                error_key = key
            async with lock:
                if extracted is not None:
                    extractions[candidate.case_id] = extracted
                if error_key:
                    errors[error_key] = errors.get(error_key, 0) + 1
                completed += 1
                should_publish = (
                    progress_callback is not None
                    and (completed == len(candidates) or completed % max(1, int(progress_interval or 10)) == 0)
                )
                snapshot = AvitoContextualAIExtractionResult(
                    extractions=dict(extractions),
                    extracted_count=len(extractions),
                    failed_count=max(0, completed - len(extractions)),
                    errors=dict(errors),
                )
            if should_publish and progress_callback is not None:
                await progress_callback(snapshot)

    await asyncio.gather(*(run_one(candidate) for candidate in candidates))
    return AvitoContextualAIExtractionResult(
        extractions=extractions,
        extracted_count=len(extractions),
        failed_count=max(0, len(candidates) - len(extractions)),
        errors=errors,
    )


def build_default_extractor() -> AvitoContextualAIExtractor | None:
    _silence_ai_transport_logs()
    enabled = str(os.getenv("AVITO_CONTEXTUAL_AI_REVIEW") or os.getenv("AVITO_TRAINING_AI_REVIEW") or "").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        return None
    if not os.getenv("OPENAI_API_KEY"):
        return None
    try:
        from openai import AsyncOpenAI  # type: ignore
    except Exception:
        return None
    model = os.getenv("AVITO_CONTEXTUAL_AI_MODEL") or "gpt-5.2"
    return AvitoContextualAIExtractor(
        client=AsyncOpenAI(),
        config=AvitoContextualAIExtractorConfig(enabled=True, model=model),
    )


def _silence_ai_transport_logs() -> None:
    for name in ("openai", "openai._base_client", "httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)


def _candidate_payload(
    candidate: AvitoContextualCaseCandidate,
    *,
    rule_extraction: Mapping[str, Any] | None,
    domain_schema: Mapping[str, Any] | None,
) -> dict[str, Any]:
    base = candidate.base_case()
    return {
        "prompt_version": PROMPT_VERSION,
        "candidate": {
            "case_id": candidate.case_id,
            "context": base["dialog"]["history"],
            "manager_reply": base["dialog"]["manager_reply"],
        },
        "domain_schema": dict(domain_schema or {}),
        "rule_extraction": dict(rule_extraction or {}),
    }


def _normalize_extraction(value: Mapping[str, Any]) -> dict[str, Any]:
    data = dict(value or {})
    context = data.get("context")
    if not isinstance(context, dict):
        context = {}
    reply_facts = data.get("reply_facts")
    if not isinstance(reply_facts, dict):
        reply_facts = {}
    applicability = data.get("applicability")
    if not isinstance(applicability, dict):
        applicability = {}
    quality = data.get("quality")
    if not isinstance(quality, dict):
        quality = {}
    return {
        "context": context,
        "reply_facts": reply_facts,
        "applicability": applicability,
        "quality": quality,
    }


def _use_responses_api(model: str) -> bool:
    normalized = str(model or "").lower()
    return normalized.startswith("gpt-5") or normalized.startswith("o")


def _system_prompt() -> str:
    return """
You extract domain-aware contextual training cases for a Russian sales assistant.

Task:
- Do not accept/reject by taste.
- Convert the manager reply into structured context and applicability rules.
- Determine when this exact reply can be safely reused later.
- Use domain_schema from the user payload. Do not assume the business is doors unless the schema says so.
- Extract domain-specific slots into context.slots using schema slot keys.
- If the reply includes price, address, service area, delivery region, availability, timing, or other conditional facts, mark it context_bound and list required context.
- applicability.requires must use slots.<slot_key> for domain-specific requirements.
- If the customer asks where the company is and city is unknown, prefer clarify_first when the manager asks for the city.
- Persona/catalog/business rules will have higher priority in future runtime, so examples must not override them.

Return only valid JSON:
{
  "context": {
    "intent": "store_location|price_question|delivery_installation|catalog_request|availability|measurement|product_choice|warranty|payment|other",
    "stage": "first_touch|clarification|offer|handoff_to_messenger|followup|closing",
    "client_city": string|null,
    "business_city": string|null,
    "product_type": string|null,
    "premise_type": string|null,
    "domain": string,
    "domain_label": string,
    "slots": {"slot_key": "value"},
    "known_slots": ["slot_key"],
    "missing_slots": ["slot_key"],
    "known_facts": ["client_city"],
    "missing_facts": []
  },
  "reply_facts": {
    "mentions_address": boolean,
    "mentions_price": boolean,
    "mentions_delivery": boolean,
    "mentions_installation": boolean,
    "mentions_contact": boolean,
    "mentions_location": boolean,
    "mentions_timing": boolean,
    "mentions_availability": boolean,
    "mentions_service_area": boolean,
    "city_specific": boolean,
    "price_specific": boolean,
    "product_specific": boolean,
    "service_specific": boolean
  },
  "applicability": {
    "mode": "direct_example|context_bound|clarify_first|style_only|review|reject",
    "requires": [],
    "same_city_required": boolean,
    "same_product_required": boolean,
    "safe_as_style_only": boolean,
    "do_not_use_directly_without": []
  },
  "quality": {
    "confidence": 0.0,
    "reason_code": "machine_readable_code"
  }
}

Privacy:
- Do not include private phone numbers, customer names, or long quotes outside the structured fields.
- Input contacts may be masked as [PHONE], [LINK], [EMAIL], [HANDLE].
""".strip()


__all__ = [
    "AvitoContextualAIExtractionResult",
    "AvitoContextualAIExtractor",
    "AvitoContextualAIExtractorConfig",
    "PROMPT_VERSION",
    "build_default_extractor",
    "extract_cases",
]
