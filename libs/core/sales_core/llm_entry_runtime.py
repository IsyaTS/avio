from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class LlmEntryRuntimeDeps:
    message_runtime_getter: Callable[[], Any]
    decision_runtime_getter: Callable[[], Any]
    reply_runtime_getter: Callable[[], Any]
    brain_mode_human_reply_enabled: Callable[..., bool]
    brain_mode_resolve: Callable[..., str]
    env_bool: Callable[[str, bool], bool]
    coerce_bool: Callable[[Any, bool], bool]
    load_tenant: Callable[[int], dict[str, Any]]


class LlmEntryRuntime:
    def __init__(self, deps: LlmEntryRuntimeDeps):
        self.deps = deps

    async def build_llm_messages(
        self,
        contact_id: int,
        last_user_text: str,
        channel: str | None,
        tenant: int | None,
    ) -> Any:
        return await self.deps.message_runtime_getter().build_llm_messages(
            contact_id=contact_id,
            last_user_text=last_user_text,
            channel=channel,
            tenant=tenant,
        )

    async def direct_llm_reply(
        self,
        client: Any,
        messages: list[dict[str, str]],
        persona_hints: Any | None,
        state: Any,
        channel_name: str,
        contact_ref: int,
        tenant: int | None,
        last_user_message: str,
    ) -> str:
        return await self.deps.message_runtime_getter().direct_llm_reply(
            client=client,
            messages=messages,
            persona_hints=persona_hints,
            state=state,
            channel_name=channel_name,
            contact_ref=contact_ref,
            tenant=tenant,
            last_user_message=last_user_message,
        )

    def human_reply_mode_enabled(self, tenant: int | None, cfg: Mapping[str, Any] | None = None) -> bool:
        return self.deps.brain_mode_human_reply_enabled(
            tenant,
            cfg=cfg,
            env_bool=self.deps.env_bool,
            coerce_bool=self.deps.coerce_bool,
            load_tenant=self.deps.load_tenant,
        )

    def resolve_brain_mode(self, tenant: int | None, cfg: Mapping[str, Any] | None = None) -> str:
        return self.deps.brain_mode_resolve(
            tenant,
            cfg=cfg,
            env_bool=self.deps.env_bool,
            coerce_bool=self.deps.coerce_bool,
            load_tenant=self.deps.load_tenant,
        )

    def build_human_mode_messages(self, messages: list[dict[str, str]]) -> list[dict[str, str]]:
        return self.deps.reply_runtime_getter().build_human_mode_messages(messages)

    async def human_llm_reply(
        self,
        client: Any,
        messages: list[dict[str, str]],
        persona_hints: Any | None,
        state: Any,
        channel_name: str,
        contact_ref: int,
        tenant: int | None,
        last_user_message: str,
    ) -> str:
        return await self.deps.message_runtime_getter().human_llm_reply(
            client=client,
            messages=messages,
            persona_hints=persona_hints,
            state=state,
            channel_name=channel_name,
            contact_ref=contact_ref,
            tenant=tenant,
            last_user_message=last_user_message,
        )

    async def single_llm_reply(
        self,
        client: Any,
        messages: list[dict[str, str]],
        persona_hints: Any | None,
        state: Any,
        channel_name: str,
        contact_ref: int,
        tenant: int | None,
        last_user_message: str,
    ) -> str:
        return await self.deps.decision_runtime_getter().single_llm_reply(
            client=client,
            messages=messages,
            persona_hints=persona_hints,
            state=state,
            channel_name=channel_name,
            contact_ref=contact_ref,
            tenant=tenant,
            last_user_message=last_user_message,
        )

    async def ask_llm(
        self,
        messages: list[dict[str, str]],
        tenant: int | None = None,
        contact_id: int | None = None,
        channel: str | None = None,
    ) -> str:
        return await self.deps.message_runtime_getter().ask_llm(
            messages=messages,
            tenant=tenant,
            contact_id=contact_id,
            channel=channel,
        )
