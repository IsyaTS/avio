from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Sequence

from .llm_response_text import extract_llm_response_text


@dataclass(frozen=True)
class SemanticRuntimeDeps:
    llm_call_with_deadline: Callable[..., Any]
    safe_json_load: Callable[[str], Dict[str, Any]]
    format_items_for_prompt: Callable[[List[Dict[str, Any]], str], str]
    settings_obj: Any


class SemanticRuntime:
    def __init__(self, deps: SemanticRuntimeDeps) -> None:
        self.deps = deps

    def fallback_semantic_plan(self, last_user_message: str) -> Dict[str, Any]:
        _ = last_user_message
        return {
            "_fallback": True,
            "intent": "clarify",
            "ack": "",
            "core": "",
            "question": "",
            "question_slot": "none",
            "required_facts": [],
            "facts_update": {},
            "blocks": [],
        }

    async def semantic_plan(
        self,
        create_fn: Any,
        *,
        model: str,
        timeout_seconds: float,
        messages: List[Dict[str, str]],
        last_user_message: str,
        known_slots: Mapping[str, str] | None = None,
        known_facts: Mapping[str, str] | None = None,
        forbidden_question_topics: Sequence[str] | None = None,
        grounding_items: Sequence[Mapping[str, Any]] | None = None,
    ) -> Dict[str, Any]:
        _llm_call_with_deadline = self.deps.llm_call_with_deadline
        _safe_json_load = self.deps.safe_json_load
        format_items_for_prompt = self.deps.format_items_for_prompt

        dialogue = [
            {"role": str(m.get("role") or ""), "content": str(m.get("content") or "")}
            for m in (messages or [])
            if str(m.get("role") or "") in {"user", "assistant"}
        ][-8:]
        persona_chunks: list[str] = []
        for item in messages or []:
            if str(item.get("role") or "").strip().lower() == "system":
                chunk = str(item.get("content") or "").strip()
                if chunk:
                    persona_chunks.append(chunk)
        persona_context = "\n\n".join(persona_chunks)
        if len(persona_context) > 6000:
            persona_context = persona_context[:6000]
        known_slots = dict(known_slots or {})
        known_facts = dict(known_facts or {})
        forbidden_question_topics = [
            str(x) for x in (forbidden_question_topics or []) if str(x).strip()
        ]
        catalog_context = format_items_for_prompt(
            [dict(item) for item in (grounding_items or [])[:5]], "₽"
        )
        if not (grounding_items or []):
            catalog_context = ""
        plan_system = (
            "Ты планировщик ответа менеджера. Верни только JSON-объект. "
            "Схема: {"
            '"intent":"greet|clarify|offer|answer|next_step",'
            '"ack":"короткое подтверждение",'
            '"core":"главная мысль ответа",'
            '"question":"один уместный вопрос или пусто",'
            '"question_slot":"location|object|model|budget|timeline|dimensions|contact|quantity|color|other|none",'
            '"required_facts": ["city","address","object_type", ...],'
            '"facts_update": {"key":"value", ...},'
            '"blocks": ['
            '{"text":"фраза для ответа","requires":["fact_key"],"type":"ack|info|offer|question|cta",'
            '"question_key":"ключ факта для question, иначе пусто"}'
            "]"
            "}. "
            "Опирайся на правила и порядок из контекста персоны. "
            "Если в персоне задан первый квалифицирующий шаг, следуй ему, но сначала отвечай на текущий вопрос клиента. "
            "Не используй штампы: 'Спасибо, понял', 'Понял', 'Поняла', 'Хороший выбор', 'Ваш запрос принят'. "
            "Не используй общую фразу 'Чем могу помочь?' как основной вопрос. "
            "Не задавай вопрос, который не связан с последней репликой клиента. "
            "Если слот уже известен, не задавай вопрос про него повторно. "
            "Не выходи за рамки каталога: не предлагай категории/товары, которых нет в catalog context. "
            "Для любой утверждающей фразы, которая зависит от условий (город, адрес, срок, скидка, источник и т.п.), "
            "обязательно укажи requires с нужными фактами. "
            "Заполни required_facts: это минимальные данные, которые нужно собрать по персоне до персонализированных утверждений/офферов. "
            "Если факт не подтвержден, не придумывай и не добавляй такую фразу в blocks."
        )
        plan_user = (
            f"Контекст персоны и правил:\n{persona_context}\n\n"
            f"Уже известные слоты: {json.dumps(known_slots, ensure_ascii=False)}\n"
            f"Уже известные факты: {json.dumps(known_facts, ensure_ascii=False)}\n"
            f"Запрещённые темы вопроса: {json.dumps(forbidden_question_topics, ensure_ascii=False)}\n"
            f"Релевантные позиции каталога:\n{catalog_context or 'нет'}\n\n"
            f"Последнее сообщение клиента: {last_user_message}\n"
            f"Недавний диалог: {json.dumps(dialogue, ensure_ascii=False)}"
        )
        try:
            resp = await _llm_call_with_deadline(
                create_fn,
                timeout_seconds=timeout_seconds,
                model=model,
                messages=[
                    {"role": "system", "content": plan_system},
                    {"role": "user", "content": plan_user},
                ],
                temperature=0.0,
                max_tokens=220,
                response_format={"type": "json_object"},
                timeout=timeout_seconds,
            )
            choices = getattr(resp, "choices", None)
            if isinstance(choices, list) and choices:
                msg = getattr(choices[0], "message", None)
                payload = _safe_json_load(str(getattr(msg, "content", "") or ""))
                if payload:
                    return payload
        except Exception:
            pass
        return self.fallback_semantic_plan(last_user_message)

    async def render_from_semantic_plan(
        self,
        create_fn: Any,
        *,
        model: str,
        timeout_seconds: float,
        prepared_messages: List[Dict[str, str]],
        plan: Dict[str, Any],
        known_slots: Mapping[str, str] | None = None,
        forbidden_question_topics: Sequence[str] | None = None,
    ) -> str:
        _llm_call_with_deadline = self.deps.llm_call_with_deadline
        settings = self.deps.settings_obj

        known_slots = dict(known_slots or {})
        forbidden_question_topics = [
            str(x) for x in (forbidden_question_topics or []) if str(x).strip()
        ]
        known_block = (
            "Известные данные клиента: " + json.dumps(known_slots, ensure_ascii=False)
            if known_slots
            else "Известные данные клиента: {}"
        )
        forbid_block = (
            "Не задавай вопрос по темам: " + ", ".join(forbidden_question_topics)
            if forbidden_question_topics
            else "Запрещённых тем вопроса нет."
        )
        style_system = (
            "Рендерни финальное сообщение менеджера строго по плану. "
            "1-3 коротких предложения, максимум 1 вопрос. "
            "Живой разговорный тон, без канцелярита и без шаблонов. "
            "Не начинай ответ с 'Понял', 'Поняла', 'Спасибо, что уточнили'. "
            "Не начинай с повтора сущности клиента + подтверждения. "
            "Не начинай с оценочного клише после выбора клиента. "
            "Не переспрашивай уже известные данные. "
            f"{known_block} {forbid_block}"
        )
        render_messages: List[Dict[str, str]] = []
        if prepared_messages:
            render_messages.append(prepared_messages[0])
            render_messages.extend(prepared_messages[1:])
        else:
            render_messages.append({"role": "system", "content": ""})
        render_messages.append({"role": "system", "content": style_system})
        render_messages.append(
            {
                "role": "user",
                "content": "План ответа (JSON): " + json.dumps(plan, ensure_ascii=False),
            }
        )
        resp = await _llm_call_with_deadline(
            create_fn,
            timeout_seconds=timeout_seconds,
            model=model,
            messages=render_messages,
            max_tokens=180,
            temperature=settings.OPENAI_TEMPERATURE,
            top_p=0.95,
            frequency_penalty=0.08,
            presence_penalty=0.04,
            timeout=timeout_seconds,
        )
        text = extract_llm_response_text(resp)
        if text:
            return text
        return ""

    async def render_direct_reply(
        self,
        create_fn: Any,
        *,
        model: str,
        timeout_seconds: float,
        prepared_messages: List[Dict[str, str]],
    ) -> str:
        _llm_call_with_deadline = self.deps.llm_call_with_deadline
        settings = self.deps.settings_obj

        direct_messages: List[Dict[str, str]] = []
        if prepared_messages:
            direct_messages.extend(prepared_messages[-10:])
        direct_messages.append(
            {
                "role": "system",
                "content": (
                    "Ответь клиенту строго по персоне и контексту диалога. "
                    "1-3 коротких предложения, максимум 1 вопрос. "
                    "Без канцелярита и без шаблонов."
                ),
            }
        )
        for token_limit in (240, 520):
            resp = await _llm_call_with_deadline(
                create_fn,
                timeout_seconds=timeout_seconds,
                model=model,
                messages=direct_messages,
                max_tokens=token_limit,
                temperature=settings.OPENAI_TEMPERATURE,
                top_p=0.95,
                frequency_penalty=0.08,
                presence_penalty=0.04,
                timeout=timeout_seconds,
            )
            text = extract_llm_response_text(resp)
            if text:
                return text
        return ""
