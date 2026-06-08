from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .conversation_engine import EngineDeps
from .conversation_engine import SalesConversationEngine
from .conversation_entrypoints import ConversationEntrypointDeps


@dataclass(frozen=True)
class ConversationRuntimeDeps:
    infer_user_needs: Callable[[str], dict[str, Any]]
    coerce_bool: Callable[[Any, bool], bool]
    env_bool: Callable[[str, bool], bool]
    remember_question_state: Callable[[Any, str], None]
    remember_cta_state: Callable[[Any, str], None]
    cta_allowed: Callable[[Any, str | None], bool]
    format_needs_for_prompt: Callable[[dict[str, Any]], str]
    persona_meta_config: Callable[[int], dict[str, Any]]
    load_tenant: Callable[[int], dict[str, Any]]
    branding_for_tenant: Callable[[int | None], dict[str, str]]
    load_sales_state: Callable[[int | None, int], Any]
    load_persona_hints: Callable[[int | None, str | None], Any]
    save_sales_state: Callable[[int | None, int, Any], None]
    default_tenant_json: Mapping[str, Any]
    search_catalog: Callable[..., list[dict[str, Any]]]
    pick_cta: Callable[[int, str | None, str], dict[str, str]]
    entry_apply_persona_need_mappings: Callable[..., None]
    entry_observe_user_message: Callable[..., Any]
    entry_summarize_sales_state: Callable[..., str]
    entry_record_bot_reply: Callable[..., None]
    entry_make_rule_based_reply: Callable[..., str]


class ConversationRuntime:
    def __init__(self, deps: ConversationRuntimeDeps):
        self.deps = deps

    def build_sales_conversation_engine(
        self,
        state: Any,
        branding: dict[str, str],
        tenant_cfg: dict[str, Any],
        channel_name: str,
        persona_hints: Any | None = None,
    ) -> SalesConversationEngine:
        deps = EngineDeps(
            infer_user_needs=self.deps.infer_user_needs,
            coerce_bool=self.deps.coerce_bool,
            env_bool=self.deps.env_bool,
            remember_question_state=self.deps.remember_question_state,
            remember_cta_state=self.deps.remember_cta_state,
            cta_allowed=self.deps.cta_allowed,
            format_needs_for_prompt=self.deps.format_needs_for_prompt,
        )
        return SalesConversationEngine(
            state=state,
            branding=branding,
            tenant_cfg=tenant_cfg,
            channel_name=channel_name,
            persona_hints=persona_hints,
            deps=deps,
        )

    def conversation_entrypoint_deps(self) -> ConversationEntrypointDeps:
        return ConversationEntrypointDeps(
            persona_meta_config=self.deps.persona_meta_config,
            load_tenant=self.deps.load_tenant,
            branding_for_tenant=self.deps.branding_for_tenant,
            load_sales_state=self.deps.load_sales_state,
            load_persona_hints=self.deps.load_persona_hints,
            save_sales_state=self.deps.save_sales_state,
            default_tenant_json=self.deps.default_tenant_json,
            infer_user_needs=self.deps.infer_user_needs,
            search_catalog=self.deps.search_catalog,
            pick_cta=self.deps.pick_cta,
            engine_builder=self.build_sales_conversation_engine,
        )

    def apply_persona_need_mappings(self, state: Any, tenant: int | None, text: str) -> None:
        self.deps.entry_apply_persona_need_mappings(
            state,
            tenant,
            text,
            deps=self.conversation_entrypoint_deps(),
        )

    def observe_user_message(
        self,
        contact_id: int,
        tenant: int | None,
        channel: str | None,
        text: str,
        tenant_cfg: dict[str, Any] | None = None,
        branding: dict[str, str] | None = None,
        persona_hints: Any | None = None,
    ) -> Any:
        return self.deps.entry_observe_user_message(
            contact_id,
            tenant,
            channel,
            text,
            deps=self.conversation_entrypoint_deps(),
            tenant_cfg=tenant_cfg,
            branding=branding,
            persona_hints=persona_hints,
        )

    def summarize_sales_state(
        self,
        contact_id: int,
        tenant: int | None,
        channel: str | None,
        tenant_cfg: dict[str, Any] | None = None,
        branding: dict[str, str] | None = None,
    ) -> str:
        return self.deps.entry_summarize_sales_state(
            contact_id,
            tenant,
            channel,
            deps=self.conversation_entrypoint_deps(),
            tenant_cfg=tenant_cfg,
            branding=branding,
        )

    def record_bot_reply(
        self,
        contact_id: int,
        tenant: int | None,
        channel: str | None,
        reply: str,
        tenant_cfg: dict[str, Any] | None = None,
        branding: dict[str, str] | None = None,
    ) -> None:
        self.deps.entry_record_bot_reply(
            contact_id,
            tenant,
            channel,
            reply,
            deps=self.conversation_entrypoint_deps(),
            tenant_cfg=tenant_cfg,
            branding=branding,
        )

    def make_rule_based_reply(
        self,
        last_user_text: str,
        channel: str | None,
        contact_id: int,
        tenant: int | None = None,
    ) -> str:
        return self.deps.entry_make_rule_based_reply(
            last_user_text,
            channel,
            contact_id,
            deps=self.conversation_entrypoint_deps(),
            tenant=tenant,
        )
