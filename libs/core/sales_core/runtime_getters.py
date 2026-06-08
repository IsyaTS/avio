from __future__ import annotations

from typing import Any, MutableMapping

from .answer_quality_runtime import AnswerQualityRuntime
from .attribute_runtime import AttributeRuntime
from .catalog_csv_runtime import CatalogCsvRuntime
from .catalog_guard_runtime import CatalogGuardRuntime
from .catalog_loader_runtime import CatalogLoaderRuntime
from .catalog_match_runtime import CatalogMatchRuntime
from .catalog_metrics_runtime import CatalogMetricsRuntime
from .catalog_price_runtime import CatalogPriceRuntime
from .catalog_rules import CatalogRulesRuntime
from .catalog_search_runtime import CatalogSearchRuntime
from .context_guard_runtime import ContextGuardRuntime
from .conversation_runtime import ConversationRuntime
from .decision_runtime import DecisionRuntime
from .dialog_state_runtime import DialogStateRuntime
from .fact_key_runtime import FactKeyRuntime
from .fact_question_runtime import FactQuestionRuntime
from .facts_runtime import FactsRuntime
from .fallback_runtime import FallbackRuntime
from .grounding_runtime import GroundingRuntime
from .humanize_runtime import HumanizeRuntime
from .instruction_runtime import InstructionRuntime
from .intent_runtime import IntentRuntime
from .io_runtime import IoRuntime
from .llm_entry_runtime import LlmEntryRuntime
from .location_runtime import LocationRuntime
from .message_runtime import MessageRuntime
from .needs_runtime import NeedsRuntime
from .openai_client_runtime import OpenAIClientRuntime
from .openai_client_runtime import OpenAIClientRuntimeDeps
from .persona_runtime import PersonaRuntime
from .persona_script_runtime import PersonaScriptRuntime
from .persona_turn_runtime import PersonaTurnRuntime
from .policy_plan_runtime import PolicyPlanRuntime
from .reply_runtime import ReplyRuntime
from .reply_runtime import ReplyRuntimeDeps
from .semantic_runtime import SemanticRuntime
from .state_io_runtime import StateIoRuntime
from .state_store_runtime import StateStoreRuntime
from .tenant_runtime import TenantRuntime
from .tenant_runtime import TenantRuntimeDeps
from .transport_runtime import TransportRuntime
from .transport_runtime import TransportRuntimeDeps
from .runtime_composition import build_answer_quality_runtime
from .runtime_composition import build_attribute_runtime
from .runtime_composition import build_catalog_csv_runtime
from .runtime_composition import build_catalog_guard_runtime
from .runtime_composition import build_catalog_loader_runtime
from .runtime_composition import build_catalog_match_runtime
from .runtime_composition import build_catalog_metrics_runtime
from .runtime_composition import build_catalog_price_runtime
from .runtime_composition import build_catalog_rules_runtime
from .runtime_composition import build_catalog_search_runtime
from .runtime_composition import build_context_guard_runtime
from .runtime_composition import build_conversation_runtime
from .runtime_composition import build_decision_runtime
from .runtime_composition import build_dialog_state_runtime
from .runtime_composition import build_fact_key_runtime
from .runtime_composition import build_fact_question_runtime
from .runtime_composition import build_facts_runtime
from .runtime_composition import build_fallback_runtime
from .runtime_composition import build_grounding_runtime
from .runtime_composition import build_humanize_runtime
from .runtime_composition import build_instruction_runtime
from .runtime_composition import build_intent_runtime
from .runtime_composition import build_io_runtime
from .runtime_composition import build_llm_entry_runtime
from .runtime_composition import build_location_runtime
from .runtime_composition import build_message_runtime
from .runtime_composition import build_needs_runtime
from .runtime_composition import build_persona_runtime
from .runtime_composition import build_persona_script_runtime
from .runtime_composition import build_persona_turn_runtime
from .runtime_composition import build_policy_plan_runtime
from .runtime_composition import build_semantic_runtime
from .runtime_composition import build_state_io_runtime
from .runtime_composition import build_state_store_runtime
from .runtime_composition import build_tenant_runtime_deps


def install_runtime_getters(
    ctx: MutableMapping[str, Any],
    *,
    settings_obj: Any,
    logger_obj: Any,
    openai_module: Any,
    api_timeout_error_cls: type[BaseException],
    style_guard: Any,
) -> None:
    openai_client_runtime_instance: OpenAIClientRuntime | None = None

    def _tenant_runtime_deps() -> TenantRuntimeDeps:
        return build_tenant_runtime_deps(ctx, settings_obj=settings_obj, logger_obj=logger_obj)

    def _tenant_runtime() -> TenantRuntime:
        return TenantRuntime(_tenant_runtime_deps())

    def _openai_client_runtime() -> OpenAIClientRuntime:
        nonlocal openai_client_runtime_instance
        if openai_client_runtime_instance is None:
            openai_client_runtime_instance = OpenAIClientRuntime(
                OpenAIClientRuntimeDeps(
                    openai_module=openai_module,
                    logger=logger_obj,
                )
            )
        return openai_client_runtime_instance

    ctx["_tenant_runtime_deps"] = _tenant_runtime_deps
    ctx["_tenant_runtime"] = _tenant_runtime
    ctx["_openai_client_runtime"] = _openai_client_runtime
    ctx["_reply_runtime"] = lambda: ReplyRuntime(ReplyRuntimeDeps(style_guard=style_guard))
    ctx["_transport_runtime"] = lambda: TransportRuntime(TransportRuntimeDeps(settings=settings_obj))

    ctx["_fact_key_runtime"] = lambda: build_fact_key_runtime(ctx)
    ctx["_fact_question_runtime"] = lambda: build_fact_question_runtime(ctx)
    ctx["_persona_script_runtime"] = lambda: build_persona_script_runtime(ctx)
    ctx["_answer_quality_runtime"] = lambda: build_answer_quality_runtime(ctx)
    ctx["_humanize_runtime"] = lambda: build_humanize_runtime(ctx)
    ctx["_state_io_runtime"] = lambda: build_state_io_runtime(ctx)
    ctx["_state_store_runtime"] = lambda: build_state_store_runtime(ctx)
    ctx["_io_runtime"] = lambda: build_io_runtime(ctx)
    ctx["_instruction_runtime"] = lambda: build_instruction_runtime(ctx)
    ctx["_catalog_rules_runtime"] = lambda: build_catalog_rules_runtime(ctx)
    ctx["_catalog_csv_runtime"] = lambda: build_catalog_csv_runtime(ctx)
    ctx["_catalog_loader_runtime"] = lambda: build_catalog_loader_runtime(ctx, logger_obj=logger_obj)
    ctx["_catalog_search_runtime"] = lambda: build_catalog_search_runtime(ctx, logger_obj=logger_obj)
    ctx["_needs_runtime"] = lambda: build_needs_runtime(ctx)
    ctx["_location_runtime"] = lambda: build_location_runtime(ctx)
    ctx["_catalog_guard_runtime"] = lambda: build_catalog_guard_runtime(ctx)
    ctx["_catalog_price_runtime"] = lambda: build_catalog_price_runtime(ctx)
    ctx["_catalog_metrics_runtime"] = lambda: build_catalog_metrics_runtime(ctx)
    ctx["_dialog_state_runtime"] = lambda: build_dialog_state_runtime(ctx)
    ctx["_fallback_runtime"] = lambda: build_fallback_runtime(ctx)
    ctx["_persona_turn_runtime"] = lambda: build_persona_turn_runtime(ctx)
    ctx["_catalog_match_runtime"] = lambda: build_catalog_match_runtime(ctx)
    ctx["_attribute_runtime"] = lambda: build_attribute_runtime(ctx)
    ctx["_context_guard_runtime"] = lambda: build_context_guard_runtime(ctx)
    ctx["_policy_plan_runtime"] = lambda: build_policy_plan_runtime(ctx)
    ctx["_conversation_runtime"] = lambda: build_conversation_runtime(ctx)
    ctx["_llm_entry_runtime"] = lambda: build_llm_entry_runtime(ctx)
    ctx["_decision_runtime"] = lambda: build_decision_runtime(
        ctx,
        settings_obj=settings_obj,
        logger_obj=logger_obj,
        api_timeout_error_cls=api_timeout_error_cls,
    )
    ctx["_persona_runtime"] = lambda: build_persona_runtime(ctx)
    ctx["_message_runtime"] = lambda: build_message_runtime(
        ctx,
        settings_obj=settings_obj,
        logger_obj=logger_obj,
        openai_module=openai_module,
        api_timeout_error_cls=api_timeout_error_cls,
    )
    ctx["_semantic_runtime"] = lambda: build_semantic_runtime(ctx, settings_obj=settings_obj)
    ctx["_facts_runtime"] = lambda: build_facts_runtime(ctx)
    ctx["_intent_runtime"] = lambda: build_intent_runtime(ctx)
    ctx["_grounding_runtime"] = lambda: build_grounding_runtime(ctx)
