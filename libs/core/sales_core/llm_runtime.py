from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Callable, Mapping


class LLMRuntime:
    def __init__(self, min_call_gap_seconds: float = 0.25) -> None:
        self._min_call_gap_seconds = max(0.0, float(min_call_gap_seconds or 0.0))
        self._call_guard = asyncio.Lock()
        self._next_allowed_ts = 0.0

    async def rate_limit_gate(self) -> None:
        if self._min_call_gap_seconds <= 0:
            return
        async with self._call_guard:
            now = time.monotonic()
            wait_for = self._next_allowed_ts - now
            if wait_for > 0:
                try:
                    import anyio  # type: ignore

                    await anyio.sleep(wait_for)
                except Exception:
                    await asyncio.sleep(wait_for)
                now = time.monotonic()
            self._next_allowed_ts = (
                max(now, self._next_allowed_ts) + self._min_call_gap_seconds
            )

    async def call_with_deadline(
        self,
        create_fn: Any,
        *,
        timeout_seconds: float,
        is_quota_or_rate_limit_error: Callable[[Exception], bool],
        model_fallbacks_env: str | None = None,
        classifier_model_env: str | None = None,
        **kwargs: Any,
    ) -> Any:
        async def _invoke_once(hard_deadline: float, call_kwargs: Mapping[str, Any]) -> Any:
            try:
                import anyio  # type: ignore

                with anyio.fail_after(hard_deadline):
                    return await anyio.to_thread.run_sync(lambda: create_fn(**dict(call_kwargs)))
            except Exception:
                return await asyncio.wait_for(
                    asyncio.to_thread(create_fn, **dict(call_kwargs)),
                    timeout=hard_deadline,
                )

        timeout_value = max(2.0, float(timeout_seconds or 0.0))
        hard_deadline = timeout_value + 2.0
        base_kwargs = dict(kwargs or {})
        base_model = str(base_kwargs.get("model") or "").strip()
        model_candidates: list[str] = []
        if base_model:
            model_candidates.append(base_model)
        fallbacks_raw = (
            model_fallbacks_env
            if model_fallbacks_env is not None
            else os.getenv("OPENAI_MODEL_FALLBACKS", "")
        )
        classifier_raw = (
            classifier_model_env
            if classifier_model_env is not None
            else os.getenv("LEAD_CLASSIFIER_MODEL", "")
        )
        for extra_raw in (fallbacks_raw, classifier_raw, "gpt-4o-mini"):
            for model in [part.strip() for part in str(extra_raw or "").split(",") if part.strip()]:
                if model and model not in model_candidates:
                    model_candidates.append(model)
        if not model_candidates:
            model_candidates.append("")

        retries = 2
        last_exc: Exception | None = None
        for model in model_candidates:
            call_kwargs = dict(base_kwargs)
            if model:
                call_kwargs["model"] = model
            call_kwargs = _normalize_chat_completion_kwargs(call_kwargs)
            for attempt in range(retries + 1):
                await self.rate_limit_gate()
                try:
                    return await _invoke_once(hard_deadline, call_kwargs)
                except Exception as exc:
                    last_exc = exc
                    if is_quota_or_rate_limit_error(exc):
                        if attempt < retries:
                            await asyncio.sleep(0.6 + attempt * 0.8)
                            continue
                        break
                    raise
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("llm call failed without exception")


def _normalize_chat_completion_kwargs(kwargs: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(kwargs)
    model = str(normalized.get("model") or "").strip().lower()
    if model.startswith("gpt-5"):
        if "max_tokens" in normalized and "max_completion_tokens" not in normalized:
            normalized["max_completion_tokens"] = normalized.pop("max_tokens")
        normalized.pop("temperature", None)
        normalized.pop("top_p", None)
        normalized.pop("frequency_penalty", None)
        normalized.pop("presence_penalty", None)
    return normalized
