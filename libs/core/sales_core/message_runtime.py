from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

from .llm_response_text import extract_llm_response_text


@dataclass(frozen=True)
class MessageRuntimeDeps:
    load_persona: Callable[[int | None, str | None], str]
    extract_persona_hints: Callable[[str], Any]
    persona_hints_cache_key: Callable[[int | None, str | None], tuple[int | None, str]]
    persona_hints_cache: Dict[tuple[int | None, str], tuple[str, Any]]
    branding_for_tenant: Callable[[int | None, str | None], Dict[str, str]]
    load_sales_state: Callable[[int | None, int], Any]
    capture_pending_fact_answer: Callable[[Any, str], None]
    merge_fact_updates: Callable[..., None]
    extract_city_hint: Callable[..., str]
    canonical_fact_key: Callable[[str], str]
    extract_questions_from_text: Callable[[str], List[str]]
    question_covers_fact: Callable[[str, str], bool]
    extract_standalone_city_hint: Callable[[str], str]
    is_plausible_city_text: Callable[[str], bool]
    safe_short_text: Callable[[str, int], str]
    save_sales_state: Callable[[Any], None]
    state_facts_snapshot: Callable[[Any], Dict[str, str]]
    summarize_sales_state: Callable[[int, int | None, str], str]
    normalize_slot_name: Callable[[str], str]
    infer_user_needs: Callable[[str], Dict[str, Any]]
    object_type_from_turn_text: Callable[[str], str]
    search_catalog: Callable[..., List[Dict[str, Any]]]
    format_items_for_prompt: Callable[[Sequence[Mapping[str, Any]], str], str]
    read_all_catalog: Callable[..., List[Dict[str, Any]]]

    resolve_chat_completion_callable: Callable[[Any], Any]
    build_reply_grounding: Callable[..., Mapping[str, Any]]
    llm_call_with_deadline: Callable[..., Any]
    settings_obj: Any
    enforce_catalog_truth_guard: Callable[..., str]
    planner_generated_plan_cls: type
    make_enforcement_context: Callable[..., Any]
    apply_plan_alignment_to_state: Callable[[Any, Any, set[str]], None]
    update_fact_memory: Callable[[Any, str], None]
    remember_questions_from_reply: Callable[[Any, str], None]
    wrap_llm_reply: Callable[..., Any]
    record_bot_reply: Callable[[int, int | None, str, str], None]
    api_timeout_error_cls: type
    logger_obj: Any
    is_quota_or_rate_limit_error: Callable[[Exception], bool]
    llm_unavailable_reply: Callable[..., str]

    build_human_mode_messages: Callable[[List[Dict[str, str]]], List[Dict[str, str]]]
    planner_generate_sales_reply: Callable[..., Any]
    enforce_catalog_price_grounding: Callable[..., str]

    is_unsubscribe_intent: Callable[[str], bool]
    unsubscribe_ack_text: Callable[[], str]
    get_openai_client: Callable[[], Any | None]
    openai_module: Any
    openai_api_key: str
    load_persona_hints: Callable[[int | None, str], Any]
    load_tenant: Callable[[int], Mapping[str, Any]]
    resolve_brain_mode: Callable[[int | None, Mapping[str, Any] | None], str]
    single_llm_reply: Callable[..., Any]

    low_signal_user_reply_re: Any
    low_signal_context_re: Any
    fact_token_re: Any


class MessageRuntime:
    def __init__(self, deps: MessageRuntimeDeps) -> None:
        self.deps = deps

    async def build_llm_messages(
        self,
        contact_id: int,
        last_user_text: str,
        channel: str | None = None,
        tenant: int | None = None,
    ):
        load_persona = self.deps.load_persona
        extract_persona_hints = self.deps.extract_persona_hints
        _persona_hints_cache_key = self.deps.persona_hints_cache_key
        _PERSONA_HINTS_CACHE = self.deps.persona_hints_cache
        _branding_for_tenant = self.deps.branding_for_tenant
        load_sales_state = self.deps.load_sales_state
        _capture_pending_fact_answer = self.deps.capture_pending_fact_answer
        _merge_fact_updates = self.deps.merge_fact_updates
        _extract_city_hint = self.deps.extract_city_hint
        _canonical_fact_key = self.deps.canonical_fact_key
        _extract_questions_from_text = self.deps.extract_questions_from_text
        _question_covers_fact = self.deps.question_covers_fact
        _extract_standalone_city_hint = self.deps.extract_standalone_city_hint
        _is_plausible_city_text = self.deps.is_plausible_city_text
        _safe_short_text = self.deps.safe_short_text
        save_sales_state = self.deps.save_sales_state
        _state_facts_snapshot = self.deps.state_facts_snapshot
        summarize_sales_state = self.deps.summarize_sales_state
        _normalize_slot_name = self.deps.normalize_slot_name
        infer_user_needs = self.deps.infer_user_needs
        _object_type_from_turn_text = self.deps.object_type_from_turn_text
        search_catalog = self.deps.search_catalog
        format_items_for_prompt = self.deps.format_items_for_prompt
        read_all_catalog = self.deps.read_all_catalog

        _LOW_SIGNAL_USER_REPLY_RE = self.deps.low_signal_user_reply_re
        _LOW_SIGNAL_CONTEXT_RE = self.deps.low_signal_context_re
        _FACT_TOKEN_RE = self.deps.fact_token_re

        persona = load_persona(tenant, channel)
        persona_hints = extract_persona_hints(persona)
        fingerprint = hashlib.sha1(persona.encode("utf-8")).hexdigest() if persona else ""
        cache_key = _persona_hints_cache_key(tenant, channel)
        _PERSONA_HINTS_CACHE[cache_key] = (fingerprint, persona_hints)
        branding = _branding_for_tenant(tenant, channel)
        channel_name = (channel or branding["CHANNEL"]).strip() or "WhatsApp"
        user_text = (last_user_text or "").strip()

        state = load_sales_state(tenant, contact_id)
        state_channel = str(getattr(state, "channel", "") or "").strip().lower()
        # Protect MAX Personal sessions from polluted cross-channel state.
        if channel_name.lower() == "max_personal" and state_channel and state_channel != "max_personal":
            state.channel = channel_name
            state.needs = {}
            state.spin = {stage: "pending" for stage in ("s", "p", "i", "n")}
            state.bant = {}
            state.asked_questions = []
            state.asked_question_fingerprints = []
            state.history = []
            state.last_items = []
            state.last_bot_reply = ""
            state.last_user_text = ""
            state.known_slots = {}
            state.pending_slot = ""
            state.recent_fact_fingerprints = []
            state.facts = {}
            state.pending_fact_key = ""
        if user_text and user_text != (state.last_user_text or "").strip():
            inferred_pending_key = _canonical_fact_key(str(state.pending_fact_key or ""))
            if not inferred_pending_key:
                for question in _extract_questions_from_text(str(state.last_bot_reply or "")):
                    for fact_key in ("city", "address", "object_type", "model", "budget"):
                        if _question_covers_fact(question, fact_key):
                            inferred_pending_key = fact_key
                            break
                    if inferred_pending_key:
                        break
            if inferred_pending_key and not _canonical_fact_key(str(state.pending_fact_key or "")):
                state.pending_fact_key = inferred_pending_key
            if state.pending_fact_key:
                _capture_pending_fact_answer(state, user_text)
            # Also extract explicit facts directly from user text even when pending key is missing.
            try:
                extracted_needs = infer_user_needs(user_text)
                _merge_fact_updates(
                    state,
                    extracted_needs,
                    user_text=user_text,
                )
            except Exception:
                extracted_needs = {}
            if not str((state.facts or {}).get("object_type") or "").strip():
                inferred_object = str((extracted_needs or {}).get("object_type") or "").strip()
                if inferred_object not in {"apartment", "house"}:
                    inferred_object = str(_object_type_from_turn_text(user_text) or "").strip()
                if inferred_object in {"apartment", "house"}:
                    if not isinstance(state.facts, dict):
                        state.facts = {}
                    state.facts["object_type"] = inferred_object
                    if not isinstance(state.known_slots, dict):
                        state.known_slots = {}
                    state.known_slots["object_type"] = inferred_object
            city_hint = _extract_city_hint(user_text, allow_standalone=False)
            allow_standalone_city = False
            if _canonical_fact_key(str(state.pending_fact_key or "")) == "city":
                allow_standalone_city = True
            elif any(
                _question_covers_fact(question, "city")
                for question in _extract_questions_from_text(str(state.last_bot_reply or ""))
            ):
                allow_standalone_city = True
            if (not city_hint) and allow_standalone_city:
                city_hint = _extract_standalone_city_hint(user_text)
            if city_hint and _is_plausible_city_text(city_hint):
                if not isinstance(state.facts, dict):
                    state.facts = {}
                current_city = str(state.facts.get("city") or "").strip()
                if not current_city or not _is_plausible_city_text(current_city):
                    city_value = _safe_short_text(city_hint, limit=80)
                    state.facts["city"] = city_value
                    state.known_slots["city"] = city_value
                if _canonical_fact_key(state.pending_fact_key) == "city":
                    state.pending_fact_key = ""
            state.last_user_text = user_text
            state.append_history("user", user_text)
            state.last_updated_ts = time.time()
            state.user_message_count += 1
            save_sales_state(state)

        def _looks_like_greeting(text: str) -> bool:
            low = (text or "").strip().lower()
            if not low:
                return False
            return any(token in low for token in ("привет", "здрав", "салам", "добрый", "hello", "hi"))

        def _is_low_signal_reply(text: str) -> bool:
            value = str(text or "").strip()
            if not value:
                return True
            if _LOW_SIGNAL_USER_REPLY_RE.match(value):
                return True
            if _LOW_SIGNAL_CONTEXT_RE.search(value):
                return True
            tokens = [tok for tok in _FACT_TOKEN_RE.findall(value.lower().replace("ё", "е")) if tok]
            if len(tokens) <= 1 and len(value) <= 16:
                return True
            return False

        system_blocks: list[str] = []
        if persona.strip():
            system_blocks.append(persona.strip())
        if state.known_slots:
            system_blocks.append(
                "Уже известные данные клиента (не переспрашивай их без причины):\n"
                + json.dumps(state.known_slots, ensure_ascii=False)
            )
        known_facts = _state_facts_snapshot(state)
        if known_facts:
            system_blocks.append(
                "Подтверждённые факты клиента (не выдумывай новые и не противоречь):\n"
                + json.dumps(known_facts, ensure_ascii=False)
            )
        try:
            summary_line = summarize_sales_state(contact_id, tenant, channel_name)
        except Exception:
            summary_line = ""
        if summary_line:
            system_blocks.append(f"Сводка состояния диалога:\n{summary_line}")
        if state.pending_slot:
            system_blocks.append(
                f"Последний уточняющий вопрос уже был по слоту '{_normalize_slot_name(state.pending_slot)}'. "
                "Если клиент ответил, переходи к следующему шагу и не повторяй этот вопрос."
            )
        system_blocks.append(
            "Правила ответа:\n"
            "- Следуй персоне буквально.\n"
            "- Пиши естественно и коротко.\n"
            "- Не используй «Привет» как рабочее приветствие.\n"
            "- Избегай канцеляризмов и служебных штампов («Ваш запрос принят», «Спасибо, понял»).\n"
            "- Не начинай ответ с механического повтора сущности клиента (город/район/модель/имя) + подтверждение.\n"
            "- После выбора клиента не застревай в клише, сразу переходи к конкретному следующему шагу.\n"
            "- Сначала отвечай на текущий вопрос клиента, потом задавай следующий уместный шаг.\n"
            "- Не закрывай диалог пустой фразой, всегда давай следующий полезный шаг.\n"
            "- Если факт/срок не подтвержден, честно скажи, что уточняешь."
        )
        system_blocks.append(f"Канал: {channel_name}")
        system_blocks.append(f"Идентификатор контакта: {contact_id}")

        if tenant is not None and user_text and (not _looks_like_greeting(user_text)) and (not _is_low_signal_reply(user_text)):
            try:
                needs_snapshot = infer_user_needs(user_text)
                context_items = search_catalog(
                    needs_snapshot,
                    limit=6,
                    tenant=tenant,
                    query=user_text,
                )
            except Exception:
                context_items = []
            if context_items:
                catalog_block = format_items_for_prompt(context_items, branding["CURRENCY"])
                system_blocks.append("Релевантные позиции каталога:\n" f"{catalog_block}")
        if tenant is not None:
            try:
                catalog_preview = read_all_catalog(tenant=tenant)[:5]
            except Exception:
                catalog_preview = []
            if catalog_preview:
                preview_block = format_items_for_prompt(
                    [dict(item) for item in list(catalog_preview or [])[:5]],
                    branding["CURRENCY"],
                )
                system_blocks.append(
                    "Работайте только в рамках каталога этого тенанта. "
                    "Не переходите в другие товарные категории и не придумывайте ассортимент вне каталога."
                )
                system_blocks.append("Примеры позиций из каталога:\n" f"{preview_block}")

        history_tail = [
            item
            for item in (state.history[-12:] if state.history else [])
            if item.get("role") in {"user", "assistant"} and str(item.get("content") or "").strip()
        ]

        sys = "\n\n".join(block for block in system_blocks if block)
        messages: List[Dict[str, str]] = [{"role": "system", "content": sys}]

        if history_tail:
            trimmed = history_tail[:-1] if history_tail[-1].get("role") == "user" else history_tail
            for msg in trimmed:
                messages.append({"role": msg["role"], "content": str(msg["content"])})

        messages.append({"role": "user", "content": user_text})
        return messages

    async def direct_llm_reply(
        self,
        client: Any,
        messages: List[Dict[str, str]],
        persona_hints: Any,
        state: Any,
        channel_name: str,
        contact_ref: int,
        tenant: int | None,
        last_user_message: str,
    ) -> str:
        _ = persona_hints
        _resolve_chat_completion_callable = self.deps.resolve_chat_completion_callable
        _build_reply_grounding = self.deps.build_reply_grounding
        _llm_call_with_deadline = self.deps.llm_call_with_deadline
        settings = self.deps.settings_obj
        _enforce_catalog_truth_guard = self.deps.enforce_catalog_truth_guard
        PlannerGeneratedPlan = self.deps.planner_generated_plan_cls
        _make_enforcement_context = self.deps.make_enforcement_context
        _apply_plan_alignment_to_state = self.deps.apply_plan_alignment_to_state
        _update_fact_memory = self.deps.update_fact_memory
        _remember_questions_from_reply = self.deps.remember_questions_from_reply
        save_sales_state = self.deps.save_sales_state
        _wrap_llm_reply = self.deps.wrap_llm_reply
        record_bot_reply = self.deps.record_bot_reply
        APITimeoutError = self.deps.api_timeout_error_cls
        logger = self.deps.logger_obj
        _is_quota_or_rate_limit_error = self.deps.is_quota_or_rate_limit_error
        _llm_unavailable_reply = self.deps.llm_unavailable_reply

        grounding: Mapping[str, Any] = {}
        try:
            create_fn = _resolve_chat_completion_callable(client)
            if not create_fn:
                raise RuntimeError("openai client missing chat.completions.create")
            grounding = _build_reply_grounding(
                tenant=tenant,
                state=state,
                user_text=last_user_message,
            )

            variants: List[str] = []
            for token_limit in (140, 320):
                resp = await _llm_call_with_deadline(
                    create_fn,
                    timeout_seconds=settings.OPENAI_TIMEOUT_SECONDS,
                    model=settings.OPENAI_MODEL,
                    messages=messages,
                    max_tokens=token_limit,
                    temperature=settings.OPENAI_TEMPERATURE,
                    top_p=0.9,
                    frequency_penalty=0.2,
                    presence_penalty=0.05,
                    timeout=settings.OPENAI_TIMEOUT_SECONDS,
                )
                text = extract_llm_response_text(resp)
                if text:
                    variants.append(text)
                    break
            if not variants:
                answer = ""
            elif len(variants) == 1:
                answer = variants[0]
            else:
                answer = "\n\n".join(
                    [f"Вариант {idx}: {text}" for idx, text in enumerate(variants, start=1)]
                )
            answer = _enforce_catalog_truth_guard(
                answer,
                grounding=grounding,
                user_text=last_user_message,
            )
            dummy_plan = PlannerGeneratedPlan()
            enforcement_ctx = _make_enforcement_context(state, persona_hints, channel_name)
            existing_fp = set(enforcement_ctx.asked_fingerprints)
            final_answer = str(answer or "").strip()
            _apply_plan_alignment_to_state(state, enforcement_ctx, existing_fp)
            _update_fact_memory(state, final_answer)
            _remember_questions_from_reply(state, final_answer)
            save_sales_state(state)
            result = _wrap_llm_reply(final_answer, plan=dummy_plan, raw_answer=answer)
            record_bot_reply(contact_ref, tenant, channel_name, str(result))
            return result
        except APITimeoutError as exc:
            logger.warning("direct llm timeout: %s", exc)
            raise
        except Exception as exc:
            if _is_quota_or_rate_limit_error(exc):
                logger.warning("direct llm quota/rate limited, fallback enabled")
                fallback = _llm_unavailable_reply(
                    user_text=last_user_message,
                    grounding=grounding,
                )
                return _wrap_llm_reply(
                    fallback,
                    plan=None,
                    raw_answer=fallback,
                    metadata={"source": "rule_fallback", "fallback_used": True},
                )
            logger.exception("direct llm call failed", exc_info=exc)
            raise
        fallback = _llm_unavailable_reply(
            user_text=last_user_message,
            grounding=grounding,
        )
        return _wrap_llm_reply(
            fallback,
            plan=None,
            raw_answer=fallback,
            metadata={"source": "rule_fallback", "fallback_used": True},
        )

    async def human_llm_reply(
        self,
        client: Any,
        messages: List[Dict[str, str]],
        persona_hints: Any,
        state: Any,
        channel_name: str,
        contact_ref: int,
        tenant: int | None,
        last_user_message: str,
    ) -> str:
        _resolve_chat_completion_callable = self.deps.resolve_chat_completion_callable
        _build_human_mode_messages = self.deps.build_human_mode_messages
        _build_reply_grounding = self.deps.build_reply_grounding
        _planner_generate_sales_reply = self.deps.planner_generate_sales_reply
        settings = self.deps.settings_obj
        _enforce_catalog_price_grounding = self.deps.enforce_catalog_price_grounding
        _enforce_catalog_truth_guard = self.deps.enforce_catalog_truth_guard
        _update_fact_memory = self.deps.update_fact_memory
        _remember_questions_from_reply = self.deps.remember_questions_from_reply
        save_sales_state = self.deps.save_sales_state
        _wrap_llm_reply = self.deps.wrap_llm_reply
        record_bot_reply = self.deps.record_bot_reply
        APITimeoutError = self.deps.api_timeout_error_cls
        logger = self.deps.logger_obj
        _is_quota_or_rate_limit_error = self.deps.is_quota_or_rate_limit_error
        _llm_unavailable_reply = self.deps.llm_unavailable_reply

        grounding: Mapping[str, Any] = {}
        try:
            create_fn = _resolve_chat_completion_callable(client)
            if not create_fn:
                raise RuntimeError("openai client missing chat.completions.create")
            human_messages = _build_human_mode_messages(messages)
            grounding = _build_reply_grounding(
                tenant=tenant,
                state=state,
                user_text=last_user_message,
            )
            plan, answer = await _planner_generate_sales_reply(
                human_messages,
                openai_module=client,
                model=settings.OPENAI_MODEL,
                timeout=settings.OPENAI_TIMEOUT_SECONDS,
                persona_language=(persona_hints.language if persona_hints else None),
            )
            answer = str(answer or "").strip()
            if not answer:
                raise RuntimeError("empty human llm answer")
            answer = _enforce_catalog_price_grounding(answer, grounding=grounding)
            answer = _enforce_catalog_truth_guard(
                answer,
                grounding=grounding,
                user_text=last_user_message,
            )
            final_answer = str(answer or "").strip()
            _update_fact_memory(state, final_answer)
            _remember_questions_from_reply(state, final_answer)
            save_sales_state(state)
            result = _wrap_llm_reply(final_answer, plan=plan.to_dict(), raw_answer=answer)
            record_bot_reply(contact_ref, tenant, channel_name, str(result))
            return result
        except APITimeoutError as exc:
            logger.warning("human llm timeout: %s", exc)
            raise
        except Exception as exc:
            if _is_quota_or_rate_limit_error(exc):
                logger.warning("human llm quota/rate limited, fallback enabled")
                fallback = _llm_unavailable_reply(
                    user_text=last_user_message,
                    grounding=grounding,
                )
                return _wrap_llm_reply(
                    fallback,
                    plan=None,
                    raw_answer=fallback,
                    metadata={"source": "rule_fallback", "fallback_used": True},
                )
            logger.exception("human llm failed", exc_info=exc)
            raise
        fallback = _llm_unavailable_reply(
            user_text=last_user_message,
            grounding=grounding,
        )
        return _wrap_llm_reply(
            fallback,
            plan=None,
            raw_answer=fallback,
            metadata={"source": "rule_fallback", "fallback_used": True},
        )

    async def ask_llm(
        self,
        messages: List[Dict[str, str]],
        tenant: int | None = None,
        contact_id: int | None = None,
        channel: str | None = None,
    ) -> str:
        _is_unsubscribe_intent = self.deps.is_unsubscribe_intent
        _unsubscribe_ack_text = self.deps.unsubscribe_ack_text
        load_sales_state = self.deps.load_sales_state
        save_sales_state = self.deps.save_sales_state
        _wrap_llm_reply = self.deps.wrap_llm_reply
        _get_openai_client = self.deps.get_openai_client
        _capture_pending_fact_answer = self.deps.capture_pending_fact_answer
        _build_reply_grounding = self.deps.build_reply_grounding
        _llm_unavailable_reply = self.deps.llm_unavailable_reply
        _update_fact_memory = self.deps.update_fact_memory
        _remember_questions_from_reply = self.deps.remember_questions_from_reply
        openai = self.deps.openai_module
        openai_api_key = self.deps.openai_api_key
        load_persona_hints = self.deps.load_persona_hints
        load_tenant = self.deps.load_tenant
        _resolve_brain_mode = self.deps.resolve_brain_mode
        _resolve_chat_completion_callable = self.deps.resolve_chat_completion_callable
        _single_llm_reply = self.deps.single_llm_reply
        logger = self.deps.logger_obj
        _is_quota_or_rate_limit_error = self.deps.is_quota_or_rate_limit_error

        last = ""
        for m in reversed(messages):
            if m.get("role") == "user":
                last = m.get("content") or ""
                break

        channel_name = channel or "whatsapp"
        contact_ref = int(contact_id or 0)

        if _is_unsubscribe_intent(last):
            reply = _unsubscribe_ack_text()
            try:
                state = load_sales_state(tenant, contact_ref)
                state.last_bot_reply = reply
                state.append_history("assistant", reply)
                state.last_updated_ts = time.time()
                save_sales_state(state)
            except Exception:
                pass
            return _wrap_llm_reply(reply, plan={"intent": "unsubscribe"}, raw_answer=reply)

        client = _get_openai_client()
        if client is None:
            state = load_sales_state(tenant, contact_ref)
            if last and state.pending_fact_key:
                _capture_pending_fact_answer(state, last)
            grounding = _build_reply_grounding(
                tenant=tenant,
                state=state,
                user_text=last,
            )
            fallback = _llm_unavailable_reply(
                user_text=last,
                grounding=grounding,
            )
            state.last_bot_reply = fallback
            state.append_history("assistant", fallback)
            state.last_updated_ts = time.time()
            _update_fact_memory(state, fallback)
            _remember_questions_from_reply(state, fallback)
            save_sales_state(state)
            return _wrap_llm_reply(
                fallback,
                plan=None,
                raw_answer=fallback,
                metadata={"source": "rule_fallback", "fallback_used": True},
            )

        try:
            if openai is not None:
                openai.api_key = openai_api_key  # type: ignore[attr-defined]
            persona_hints = load_persona_hints(tenant, channel_name)
            state = load_sales_state(tenant, contact_ref)
            tenant_cfg: Mapping[str, Any] | None = None
            if tenant is not None:
                try:
                    tenant_cfg = load_tenant(int(tenant))
                except Exception:
                    tenant_cfg = None
            if last:
                if state.pending_fact_key:
                    _capture_pending_fact_answer(state, last)
                save_sales_state(state)
            brain_mode = _resolve_brain_mode(tenant, cfg=tenant_cfg)
            if brain_mode != "smart":
                if _resolve_chat_completion_callable(client):
                    return await self.human_llm_reply(
                        client,
                        messages,
                        persona_hints,
                        state,
                        channel_name,
                        contact_ref,
                        tenant,
                        last,
                    )
            return await _single_llm_reply(
                client,
                messages,
                persona_hints,
                state,
                channel_name,
                contact_ref,
                tenant,
                last,
            )
        except Exception as exc:
            if _is_quota_or_rate_limit_error(exc):
                logger.warning("ask_llm quota/rate limited, fallback enabled")
                fallback = _llm_unavailable_reply(
                    user_text=last,
                )
                return _wrap_llm_reply(
                    fallback,
                    plan=None,
                    raw_answer=fallback,
                    metadata={"source": "rule_fallback", "fallback_used": True},
                )
            logger.exception("ask_llm unified path failed", exc_info=exc)
            raise
