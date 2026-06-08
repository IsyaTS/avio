from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence


def delegate_runtime_method(runtime_getter: Callable[[], Any], method_name: str):
    def _call(*args, **kwargs):
        return getattr(runtime_getter(), method_name)(*args, **kwargs)

    return _call


def delegate_async_runtime_method(runtime_getter: Callable[[], Any], method_name: str):
    async def _call(*args, **kwargs):
        return await getattr(runtime_getter(), method_name)(*args, **kwargs)

    return _call


def bind_private_delegates(
    ctx: Mapping[str, Any],
    bind_fn: Callable[[Callable[[], Any], str], Callable[..., Any]],
    runtime_getter: Callable[[], Any],
    *method_names: str,
) -> None:
    g = ctx
    for method_name in method_names:
        g[f"_{method_name}"] = bind_fn(runtime_getter, method_name)


def bind_named_delegates(
    ctx: Mapping[str, Any],
    bind_fn: Callable[[Callable[[], Any], str], Callable[..., Any]],
    runtime_getter: Callable[[], Any],
    mapping: Mapping[str, str],
) -> None:
    g = ctx
    for alias, method_name in mapping.items():
        g[alias] = bind_fn(runtime_getter, method_name)


def apply_plan_alignment_to_state(
    state: Any,
    context: Any,
    previous_fingerprints: set[str],
    *,
    apply_fn: Callable[..., None],
    remember_question_fn: Callable[[Any, str], None],
    remember_cta_fn: Callable[[Any, str], None],
) -> None:
    apply_fn(
        state,
        context,
        previous_fingerprints,
        remember_question_fn=remember_question_fn,
        remember_cta_fn=remember_cta_fn,
    )


def make_enforcement_context(
    state: Any,
    persona_hints: Any,
    channel_name: str,
    *,
    make_fn: Callable[..., Any],
    max_questions_fn: Callable[..., int],
    cta_allowed_fn: Callable[..., bool],
    enforcement_context_cls: type[Any],
) -> Any:
    return make_fn(
        state,
        persona_hints,
        channel_name,
        max_questions_fn=max_questions_fn,
        cta_allowed_fn=cta_allowed_fn,
        enforcement_context_cls=enforcement_context_cls,
    )
