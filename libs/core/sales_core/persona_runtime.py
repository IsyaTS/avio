from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Sequence


@dataclass(frozen=True)
class PersonaRuntimeDeps:
    normalize_text: Callable[[Any], str]
    normalize_probe_token: Callable[[str], str]
    fact_token_re: Any

    contact_url_re: Any
    contact_handle_re: Any
    contact_phone_re: Any
    is_plausible_contact_phone: Callable[[str], bool]

    persona_rules_cache_key: Callable[[str], str]
    persona_rules_cache: Dict[str, Any]

    persona_compiled_rules_cls: type
    persona_conditional_rule_cls: type
    persona_delivery_rule_cls: type
    persona_step_rule_cls: type
    sales_state_cls: type

    extract_primary_script_lines: Callable[[str], List[str]]
    fact_keys_from_line: Callable[[str], List[str]]
    line_to_question: Callable[[str], str]
    is_operator_like_question: Callable[[str], bool]
    canonical_fact_key: Callable[[str], str]
    question_covers_fact: Callable[[str, str], bool]
    infer_persona_template_condition_and_action: Callable[[str], tuple[str, str]]
    extract_questions_from_text: Callable[[str], List[str]]
    generic_question_for_fact: Callable[[str], str]
    persona_hints_cls: type
    persona_hints_key_re: Any


class PersonaRuntime:
    def __init__(self, deps: PersonaRuntimeDeps) -> None:
        self.deps = deps

    def extract_expected_tokens_from_condition(self, text: str) -> List[str]:
        raw = str(text or "").strip().lower().replace("ё", "е")
        if not raw:
            return []
        tokens = [tok for tok in re.findall(r"[a-zа-я0-9\-]{2,}", raw)]
        stop = {
            "если",
            "клиент",
            "когда",
            "после",
            "при",
            "то",
            "или",
            "и",
            "а",
            "не",
            "из",
            "в",
            "во",
            "на",
            "по",
            "город",
            "адрес",
            "квартира",
            "дом",
            "помещение",
            "тип",
            "объект",
            "модель",
            "бюджет",
            "срок",
            "контакт",
            "просит",
            "попросил",
            "попросила",
            "пишет",
            "написал",
            "написала",
            "ответил",
            "ответила",
            "сказал",
            "сказала",
            "указал",
            "указала",
            "уточнил",
            "уточнила",
            "нужно",
            "нужен",
            "нужна",
            "нужны",
            "можно",
            "давайте",
            "давай",
            "надо",
            "хочу",
            "отправить",
            "отправляй",
            "отправляйте",
            "скинуть",
            "скинь",
            "скиньте",
            "перейти",
            "продолжить",
            "продолжим",
            "пользователь",
            "название",
            "формате",
            "ответьте",
            "естественно",
            "извлеките",
            "назвал",
            "назван",
            "затем",
            "дает",
            "дал",
            "только",
            "любой",
            "любого",
            "города",
            "городов",
            "республики",
            "этих",
            "включая",
            "направлении",
            "известен",
            "известном",
        }
        out: List[str] = []
        for tok in tokens:
            if tok in stop:
                continue
            if len(tok) < 3:
                continue
            if tok not in out:
                out.append(tok)
        return out[:8]

    def clean_persona_line(self, line: str) -> str:
        return re.sub(r"^[\-•*\s]+", "", line or "").strip()

    def extract_persona_hints(self, persona: str) -> Any:
        PersonaHints = self.deps.persona_hints_cls
        _PERSONA_HINTS_KEY_RE = self.deps.persona_hints_key_re

        hints = PersonaHints()
        if not persona:
            return hints

        lines = [self.clean_persona_line(line) for line in persona.splitlines()]
        persona_lower = persona.lower()

        for raw in lines:
            if not raw:
                continue
            m = _PERSONA_HINTS_KEY_RE.match(raw)
            if not m:
                continue
            key, value = m.group(1).lower(), m.group(2).strip()
            if key.startswith("greeting") or key.startswith("приветств"):
                hints.greeting = value
            elif key == "cta" or key.startswith("призыв"):
                hints.cta = value
            elif key.startswith("closing") or key.startswith("заверш"):
                hints.closing = value
            elif key.startswith("tone") or key.startswith("тон"):
                hints.tone = value
            elif key.startswith("language") or key.startswith("язык"):
                hints.language = value
            elif "max" in key:
                digits = re.findall(r"\d+", value)
                if digits:
                    try:
                        hints.max_questions = int(digits[0])
                    except Exception:
                        pass

        if not hints.greeting:
            for raw in lines:
                if not raw or raw.startswith("#"):
                    continue
                low = raw.lower()
                if low.startswith(("правила", "техники")):
                    continue
                if any(token in low for token in ("привет", "здрав", "меня зовут")):
                    hints.greeting = raw
                    break
            if not hints.greeting:
                for raw in lines:
                    if raw and not raw.startswith(("#", "-", "*")):
                        hints.greeting = raw
                        break

        if not hints.cta:
            m = re.search(r"cta[^\n]*?:\s*(.+)", persona, re.IGNORECASE)
            if m:
                hints.cta = m.group(1).strip()

        if hints.max_questions is None:
            m = re.search(r"≤\s*(\d+)\s*(?:уточн|вопрос)", persona_lower)
            if m:
                try:
                    hints.max_questions = int(m.group(1))
                except Exception:
                    pass

        if any(
            token in persona_lower
            for token in ("коротко", "кратко", "лаконич", "brief", "concise", "short")
        ):
            hints.style_short = True
        if any(token in persona_lower for token in ("дружелюб", "тепл", "friendly", "улыб")):
            hints.style_friendly = True
        if any(
            token in persona_lower
            for token in ("без смай", "без эмодзи", "без emoji", "без эмоджи", "без эмодзи")
        ):
            hints.no_emoji = True

        return hints

    def extract_contact_artifacts(self, text: str) -> List[str]:
        _CONTACT_URL_RE = self.deps.contact_url_re
        _CONTACT_HANDLE_RE = self.deps.contact_handle_re
        _CONTACT_PHONE_RE = self.deps.contact_phone_re
        _is_plausible_contact_phone = self.deps.is_plausible_contact_phone

        raw = str(text or "")
        if not raw:
            return []

        def _clean_artifact_token(kind: str, token: str) -> str:
            out = str(token or "").strip()
            if not out:
                return out
            if kind == "url":
                out = out.strip("<>[](){}")
                out = out.rstrip(".,;:!?\"'»")
            elif kind == "handle":
                out = out.rstrip(".,;:!?\"'»")
            elif kind == "phone":
                out = re.sub(r"\s+", " ", out).strip()
            return out

        def _artifact_dedupe_key(kind: str, token: str) -> str:
            cleaned = _clean_artifact_token(kind, token)
            if kind == "phone":
                return re.sub(r"\D+", "", cleaned)
            return cleaned.lower()

        found: List[tuple[int, str, str]] = []
        for match in _CONTACT_URL_RE.finditer(raw):
            token = _clean_artifact_token("url", str(match.group(0) or ""))
            if token:
                found.append((match.start(), "url", token))
        for match in _CONTACT_HANDLE_RE.finditer(raw):
            token = _clean_artifact_token("handle", str(match.group(0) or ""))
            if token:
                found.append((match.start(), "handle", token))
        for match in _CONTACT_PHONE_RE.finditer(raw):
            token = _clean_artifact_token("phone", str(match.group(0) or ""))
            if token and _is_plausible_contact_phone(token):
                found.append((match.start(), "phone", token))
        if not found:
            return []
        found.sort(key=lambda item: item[0])
        out: List[str] = []
        seen: set[str] = set()
        for _, kind, token in found:
            normalized = _artifact_dedupe_key(kind, token)
            if normalized in seen:
                continue
            seen.add(normalized)
            out.append(token)
        return out[:12]

    def detect_persona_line_channels(self, text: str) -> List[str]:
        low = str(text or "").lower().replace("ё", "е")
        if not low:
            return []
        channels: List[str] = []
        markers = (
            ("avito", ("avito", "авито")),
            ("telegram", ("telegram", "телеграм", "телега", "тг")),
            ("whatsapp", ("whatsapp", "ватсап", "вотсап")),
            ("max", (" max ", " max.", " max,", " max)", " max/")),
        )
        for name, variants in markers:
            if any(variant in f" {low} " for variant in variants):
                channels.append(name)
        return channels

    def is_delivery_directive_line(self, text: str) -> bool:
        low = str(text or "").strip().lower().replace("ё", "е")
        if not low:
            return False
        if re.search(r"(?iu)\bне\s+(предлаг|отправ|скид|пиш|дава|прос|остав)", low):
            return False
        contact_markers = (
            "telegram",
            "телеграм",
            "тг",
            "контакт",
            "номер",
            "телефон",
            "мессендж",
            "ссылк",
            "@",
        )
        action_markers = (
            "предлаг",
            "продолж",
            "перейд",
            "отправ",
            "скиды",
            "скинь",
            "напиш",
            "пиши",
            "остав",
            "попрос",
            "просите",
            "укаж",
            "дайте",
            "связ",
        )
        return any(marker in low for marker in contact_markers) and any(
            marker in low for marker in action_markers
        )

    def delivery_rule_from_line(
        self,
        *,
        source_line: str,
        channel_scope: List[str] | None = None,
        condition_text: str = "",
    ) -> Any:
        PersonaDeliveryRule = self.deps.persona_delivery_rule_cls

        clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(source_line or "")).strip()
        low = clean.lower().replace("ё", "е")
        wants_handle = "@" in low or any(
            marker in low for marker in ("username", "юзернейм", "ник", "логин")
        )
        wants_phone = any(marker in low for marker in ("номер", "телефон", "звон", "контакт"))
        wants_link = any(marker in low for marker in ("ссылк", "http"))
        if not (wants_handle or wants_phone or wants_link):
            wants_handle = True
            wants_phone = True
        gap = 2
        gap_match = re.search(r"(?iu)не\s+чаще[^\d]*(\d+)", low)
        if gap_match:
            try:
                gap = max(1, int(gap_match.group(1)))
            except Exception:
                gap = 2
        return PersonaDeliveryRule(
            source_line=clean,
            channel_scope=list(channel_scope or []),
            condition_text=str(condition_text or "").strip(),
            expected_tokens=self.extract_expected_tokens_from_condition(condition_text),
            wants_handle=wants_handle,
            wants_phone=wants_phone,
            wants_link=wants_link,
            min_assistant_gap=gap,
        )

    def infer_delivery_condition_from_line(self, source_line: str) -> str:
        clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(source_line or "")).strip()
        if ":" not in clean:
            return ""
        lhs, rhs = clean.split(":", 1)
        lhs = lhs.strip()
        rhs = rhs.strip()
        if not lhs or not rhs:
            return ""
        has_quote = any(mark in lhs for mark in ('"', "«", "»", "`", "'"))
        has_alternatives = "/" in lhs
        if not (has_quote or has_alternatives):
            return ""
        lhs = lhs.strip(" \t'\"`«»")
        if not lhs or len(lhs) > 180:
            return ""
        return lhs

    def line_to_question(self, line: str) -> str:
        _extract_questions_from_text = self.deps.extract_questions_from_text
        _FACT_TOKEN_RE = self.deps.fact_token_re
        _normalize_text = self.deps.normalize_text
        _fact_keys_from_line = self.deps.fact_keys_from_line
        _generic_question_for_fact = self.deps.generic_question_for_fact

        txt = str(line or "").strip()
        if not txt:
            return ""
        txt = re.sub(r"^[\-\s•\d\).\(\"']+", "", txt).strip()
        if not txt:
            return ""
        quoted_questions = [
            str(part or "").strip()
            for part in re.findall(r"[\"«]([^\"»]{3,220}\?)[\"»]", txt)
            if str(part or "").strip()
        ]
        if quoted_questions:
            return quoted_questions[0]
        if "?" in txt:
            parts = _extract_questions_from_text(txt)
            if parts:
                return parts[0].strip()
        imperative = re.match(
            r"(?iu)^(?:уточни(?:те)?|уточняй(?:те)?|уточнить|спроси(?:те)?|"
            r"спрашивай(?:те)?|спросить|узнай(?:те)?|узнавай(?:те)?|узнать|"
            r"выясни(?:те)?|выясняй(?:те)?|выяснить|получи(?:те)?|получай(?:те)?|"
            r"получить|собери(?:те)?|собирай(?:те)?|собрать|попроси(?:те)?|"
            r"попросить|определи(?:те)?|определить)\s*[,:-]?\s+(.+)$",
            txt,
        )
        if imperative:
            tail = str(imperative.group(1) or "").strip(" .")
            if tail:
                tail = re.sub(r"\([^)]{1,80}\)", "", tail).strip(" .")
                tail = re.sub(
                    r"(?iu)\b(?:если\s+не\s+назван|если\s+не\s+указан|"
                    r"при\s+известн\w+\s+город\w*|если\s+город\s+уже\s+назван)\b",
                    "",
                    tail,
                ).strip(" .,:;-")
                tail = re.sub(r"(?iu)^(?:у\s+клиента\s+)?", "", tail).strip(" .")
                if tail:
                    tail_tokens = [tok for tok in _FACT_TOKEN_RE.findall(_normalize_text(tail)) if tok]
                    if len(tail_tokens) <= 2:
                        inferred_keys = _fact_keys_from_line(txt)
                        if inferred_keys:
                            generic = _generic_question_for_fact(inferred_keys[0])
                            if generic:
                                return generic
                    if tail[-1] not in "?!":
                        tail += "?"
                    lead = tail[0].lower() + tail[1:] if tail else ""
                    if lead.startswith("какая "):
                        return str(lead or "").strip()
                    if lead.startswith("какой "):
                        return str(lead or "").strip()
                    if lead.startswith("какие "):
                        return str(lead or "").strip()
                    return str(lead or "").strip() if tail else ""
        return ""

    def infer_persona_template_condition_and_action(self, line: str) -> tuple[str, str]:
        clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", str(line or "")).strip()
        if ":" not in clean:
            return "", ""
        lhs, rhs = clean.split(":", 1)
        lhs = lhs.strip()
        rhs = rhs.strip()
        if not lhs:
            return "", ""
        low = lhs.lower().replace("ё", "е")
        if not (
            low.startswith("на ")
            or "ожидаемого ответа" in low
            or "на сообщение" in low
            or "на фразу" in low
        ):
            return "", ""
        quoted = [
            str(part or "").strip()
            for part in re.findall(r"[\"«]([^\"»]{1,220})[\"»]", lhs)
            if str(part or "").strip()
        ]
        if not quoted:
            return "", ""
        return quoted[0], rhs

    def compile_persona_rules(self, persona_text: str) -> Any:
        PersonaCompiledRules = self.deps.persona_compiled_rules_cls
        PersonaConditionalRule = self.deps.persona_conditional_rule_cls
        PersonaStepRule = self.deps.persona_step_rule_cls

        _persona_rules_cache_key = self.deps.persona_rules_cache_key
        _PERSONA_RULES_CACHE = self.deps.persona_rules_cache
        _fact_keys_from_line = self.deps.fact_keys_from_line
        _is_operator_like_question = self.deps.is_operator_like_question
        _canonical_fact_key = self.deps.canonical_fact_key
        _question_covers_fact = self.deps.question_covers_fact
        _extract_primary_script_lines = self.deps.extract_primary_script_lines

        key = _persona_rules_cache_key(persona_text)
        if key:
            cached = _PERSONA_RULES_CACHE.get(key)
            if cached is not None:
                return cached
        text = str(persona_text or "")
        compiled = PersonaCompiledRules()
        compiled.contact_artifacts = self.extract_contact_artifacts(text)

        def _append_conditional_rule(
            *,
            condition_text: str,
            action_text: str,
            source_line: str,
            channel_scope: List[str] | None = None,
        ) -> None:
            cond = str(condition_text or "").strip()
            action = str(action_text or "").strip()
            if not cond or not action:
                return
            cond_keys = _fact_keys_from_line(cond)
            fact_key = _canonical_fact_key(cond_keys[0]) if cond_keys else ""
            compiled.conditionals.append(
                PersonaConditionalRule(
                    source_line=str(source_line or "").strip() or f"{cond}: {action}",
                    condition_text=cond,
                    action_text=action,
                    fact_key=fact_key,
                    expected_tokens=self.extract_expected_tokens_from_condition(cond),
                )
            )
            if self.is_delivery_directive_line(action):
                compiled.delivery_rules.append(
                    self.delivery_rule_from_line(
                        source_line=action,
                        channel_scope=list(channel_scope or []),
                        condition_text=cond,
                    )
                )

        for line in _extract_primary_script_lines(text):
            clean = re.sub(r"^[\-\s•\d\).\(\"']+", "", line).strip()
            if not clean:
                continue
            low = clean.lower().replace("ё", "е")
            if low.startswith("если "):
                continue
            keys = _fact_keys_from_line(clean)
            if not keys:
                continue
            question = self.line_to_question(clean)
            if question and _is_operator_like_question(question):
                question = ""
            canonical = ""
            question_low = question.lower().replace("ё", "е") if question else ""
            canonical_keys = [_canonical_fact_key(raw_key) for raw_key in keys]
            if (
                "object_type" in canonical_keys
                and question_low
                and ("квартир" in question_low or "частн" in question_low or "тип объект" in question_low)
            ):
                canonical = "object_type"
            elif "address" in canonical_keys and question_low and _question_covers_fact(question, "address"):
                canonical = "address"
            elif "model" in canonical_keys and question_low and _question_covers_fact(question, "model"):
                canonical = "model"
            if question and not canonical:
                for raw_key in keys:
                    probe = _canonical_fact_key(raw_key)
                    if probe and _question_covers_fact(question, probe):
                        canonical = probe
                        break
            if not canonical:
                canonical = _canonical_fact_key(keys[0])
            if not canonical:
                continue
            if any(step.fact_key == canonical for step in compiled.steps):
                continue
            compiled.steps.append(
                PersonaStepRule(
                    fact_key=canonical,
                    source_line=clean,
                    question=question,
                )
            )

        section_scope: List[str] = []
        pending_conditional_text = ""
        pending_conditional_source = ""
        pending_conditional_scope: List[str] = []
        pending_template_text = ""
        pending_template_source = ""
        pending_template_scope: List[str] = []
        for raw_line in text.splitlines():
            line = str(raw_line or "").strip()
            if not line:
                continue
            clean_line = re.sub(r"^[\-\s•\d\).\(\"']+", "", line).strip()
            clean_low = clean_line.lower().replace("ё", "е")
            if line.startswith("#"):
                pending_conditional_text = ""
                pending_conditional_source = ""
                pending_conditional_scope = []
                pending_template_text = ""
                pending_template_source = ""
                pending_template_scope = []
                section_scope = self.detect_persona_line_channels(line)
                continue
            line_scope = self.detect_persona_line_channels(line)
            effective_scope = section_scope or line_scope
            if pending_conditional_text:
                if not clean_low.startswith("если "):
                    _append_conditional_rule(
                        condition_text=pending_conditional_text,
                        action_text=clean_line,
                        source_line=f"{pending_conditional_source} {clean_line}".strip(),
                        channel_scope=pending_conditional_scope or effective_scope,
                    )
                    pending_conditional_text = ""
                    pending_conditional_source = ""
                    pending_conditional_scope = []
                    continue
                pending_conditional_text = ""
                pending_conditional_source = ""
                pending_conditional_scope = []
            if pending_template_text:
                if not clean_low.startswith("если "):
                    _append_conditional_rule(
                        condition_text=pending_template_text,
                        action_text=clean_line,
                        source_line=f"{pending_template_source} {clean_line}".strip(),
                        channel_scope=pending_template_scope or effective_scope,
                    )
                    pending_template_text = ""
                    pending_template_source = ""
                    pending_template_scope = []
                    continue
                pending_template_text = ""
                pending_template_source = ""
                pending_template_scope = []
            if clean_line and (not clean_low.startswith("если ")) and self.is_delivery_directive_line(clean_line):
                inferred_cond = self.infer_delivery_condition_from_line(clean_line)
                compiled.delivery_rules.append(
                    self.delivery_rule_from_line(
                        source_line=clean_line,
                        channel_scope=effective_scope,
                        condition_text=inferred_cond,
                    )
                )
            if clean_line and (not clean_low.startswith("если ")):
                inferred_cond, inferred_action = self.infer_persona_template_condition_and_action(clean_line)
                if inferred_cond and inferred_action:
                    _append_conditional_rule(
                        condition_text=inferred_cond,
                        action_text=inferred_action,
                        source_line=clean_line,
                        channel_scope=effective_scope,
                    )
                elif inferred_cond:
                    pending_template_text = inferred_cond
                    pending_template_source = clean_line
                    pending_template_scope = list(effective_scope or [])
            if not clean_low.startswith("если "):
                continue
            cond = ""
            action = ""
            if ":" in clean_line:
                cond, action = clean_line.split(":", 1)
            elif " - " in clean_line:
                cond, action = clean_line.split(" - ", 1)
            else:
                m_to = re.search(r"(?iu)\bто\b", clean_low)
                if m_to:
                    idx = m_to.start()
                    cond = clean_line[:idx].strip()
                    action = clean_line[m_to.end() :].strip(" -:")
            cond = cond.strip()
            action = action.strip()
            if not cond:
                continue
            if not action:
                pending_conditional_text = cond
                pending_conditional_source = clean_line
                pending_conditional_scope = list(effective_scope or [])
                continue
            _append_conditional_rule(
                condition_text=cond,
                action_text=action,
                source_line=clean_line,
                channel_scope=effective_scope,
            )

        if compiled.delivery_rules:
            unique_delivery: List[Any] = []
            seen_delivery: set[str] = set()
            for rule in compiled.delivery_rules:
                signature = "|".join(
                    [
                        ",".join(sorted(rule.channel_scope or [])),
                        (rule.condition_text or "").lower(),
                        (rule.source_line or "").lower(),
                        "h" if rule.wants_handle else "-",
                        "p" if rule.wants_phone else "-",
                        "l" if rule.wants_link else "-",
                    ]
                )
                if signature in seen_delivery:
                    continue
                seen_delivery.add(signature)
                unique_delivery.append(rule)
            compiled.delivery_rules = unique_delivery

        if key:
            _PERSONA_RULES_CACHE[key] = compiled
            if len(_PERSONA_RULES_CACHE) > 128:
                for stale_key in list(_PERSONA_RULES_CACHE.keys())[:32]:
                    _PERSONA_RULES_CACHE.pop(stale_key, None)
        return compiled

    def required_facts_from_persona_text(self, persona_context: str) -> List[str]:
        _canonical_fact_key = self.deps.canonical_fact_key
        _fact_keys_from_line = self.deps.fact_keys_from_line

        raw_text = str(persona_context or "")
        if not raw_text.strip():
            return []
        compiled = self.compile_persona_rules(raw_text)
        ordered_steps: List[str] = []
        seen_steps: set[str] = set()
        for step in list(compiled.steps or []):
            canonical = _canonical_fact_key(str(getattr(step, "fact_key", "") or ""))
            if not canonical or canonical in seen_steps:
                continue
            seen_steps.add(canonical)
            ordered_steps.append(canonical)
        ordered_steps = ordered_steps[:12]

        lines = [ln.strip() for ln in raw_text.splitlines()]
        primary_lines: List[str] = []
        secondary_lines: List[str] = []
        in_primary_block = False
        in_secondary_block = False

        for line in lines:
            if not line:
                continue
            low = line.lower().replace("ё", "е")
            is_heading = low.startswith("#")
            if is_heading and in_primary_block:
                in_primary_block = False
            if is_heading and in_secondary_block:
                in_secondary_block = False
            if any(
                token in low for token in ("диалог-скрипт", "скрипт диалога", "последовательно уточни")
            ):
                in_primary_block = True
                in_secondary_block = False
                continue
            if all(token in low for token in ("шаблон", "реплик")):
                in_secondary_block = True
                in_primary_block = False
                continue
            if in_primary_block:
                if (
                    re.match(r"^\d+\)", low)
                    or re.match(r"^\d+\.", low)
                    or low.startswith("-")
                    or low.startswith("•")
                ):
                    primary_lines.append(line)
                    continue
                primary_lines.append(line)
            elif in_secondary_block:
                if (
                    re.match(r"^\d+\)", low)
                    or re.match(r"^\d+\.", low)
                    or low.startswith("-")
                    or low.startswith("•")
                ):
                    secondary_lines.append(line)
                    continue
                secondary_lines.append(line)

        script_lines = primary_lines[:] if primary_lines else secondary_lines[:]
        if not script_lines:
            for line in lines:
                low = line.lower().replace("ё", "е")
                if any(token in low for token in ("уточни", "спроси", "узнай", "получи", "собери")):
                    script_lines.append(line)

        required: List[str] = []
        for line in script_lines:
            low = line.lower().replace("ё", "е")
            normalized = re.sub(r"^[\-\s•\d\).\(\"']+", "", low).strip()
            if normalized.startswith("если "):
                continue
            action_prefixes = (
                "уточ",
                "спрос",
                "узна",
                "получ",
                "собер",
                "подскаж",
                "пришли",
                "пришлите",
            )
            has_action = any(normalized.startswith(prefix) for prefix in action_prefixes)
            has_question = "?" in line
            if not has_action and not has_question:
                continue
            for key in _fact_keys_from_line(normalized):
                if key in {"dimensions"} and "обязательно" not in normalized:
                    continue
                required.append(key)

        seen: set[str] = set()
        ordered: List[str] = []
        for key in required:
            canonical = _canonical_fact_key(key)
            if canonical and canonical not in seen:
                seen.add(canonical)
                ordered.append(canonical)
        ordered = ordered[:12]
        if not ordered:
            return ordered
        if ordered_steps and len(ordered_steps) >= len(ordered):
            return ordered_steps
        return ordered

    def conditional_rule_matches(
        self,
        rule: Any,
        *,
        last_user_message: str,
        known_facts: Mapping[str, str] | None = None,
        state: Any = None,
    ) -> bool:
        _normalize_text = self.deps.normalize_text
        _normalize_probe_token = self.deps.normalize_probe_token
        _FACT_TOKEN_RE = self.deps.fact_token_re
        SalesState = self.deps.sales_state_cls

        cond = _normalize_text(rule.condition_text)
        if not cond:
            return False
        haystack = _normalize_text(last_user_message)
        facts = dict(known_facts or {})
        if isinstance(state, SalesState) and isinstance(state.facts, dict):
            for k, v in state.facts.items():
                if k not in facts and str(v or "").strip():
                    facts[k] = str(v)
        if rule.fact_key:
            fact_val = _normalize_text(str(facts.get(rule.fact_key) or ""))
            if fact_val:
                haystack = f"{haystack} {fact_val}".strip()
        if not haystack:
            return False
        if not rule.expected_tokens:
            return False
        fact_probe = _normalize_probe_token(str(facts.get(rule.fact_key) or "")) if rule.fact_key else ""
        hay_tokens = [_normalize_probe_token(tok) for tok in _FACT_TOKEN_RE.findall(haystack)]

        def _near_token_form(a: str, b: str) -> bool:
            aa = str(a or "").strip().lower()
            bb = str(b or "").strip().lower()
            if not aa or not bb:
                return False
            if aa == bb:
                return True
            if len(aa) >= 3 and len(bb) >= 3 and aa[:-1] == bb[:-1]:
                return True
            if len(aa) >= 4 and len(bb) >= 4 and aa[:3] == bb[:3] and abs(len(aa) - len(bb)) <= 1:
                return True
            return False

        for token in rule.expected_tokens:
            tok = str(token or "").strip().lower()
            if not tok:
                continue
            if re.search(rf"(?iu)(?<![\\w-]){re.escape(tok)}(?![\\w-])", haystack):
                return True
            tok_probe = _normalize_probe_token(tok)
            if tok_probe and any(tok_probe == h or tok_probe in h or h in tok_probe for h in hay_tokens if h):
                return True
            if tok_probe and any(_near_token_form(tok_probe, h) for h in hay_tokens if h):
                return True
            if fact_probe and tok_probe and (tok_probe in fact_probe or fact_probe in tok_probe):
                return True
            if fact_probe and tok_probe and _near_token_form(tok_probe, fact_probe):
                return True
        return False

    def is_contact_artifact_token(self, value: str) -> bool:
        _CONTACT_URL_RE = self.deps.contact_url_re
        _CONTACT_HANDLE_RE = self.deps.contact_handle_re
        _CONTACT_PHONE_RE = self.deps.contact_phone_re

        token = str(value or "").strip()
        if not token:
            return False
        return bool(
            _CONTACT_URL_RE.search(token)
            or _CONTACT_HANDLE_RE.search(token)
            or _CONTACT_PHONE_RE.search(token)
        )

    def reply_has_contact_artifact(self, reply: str, artifacts: Sequence[str]) -> bool:
        text = str(reply or "")
        if not text:
            return False
        if any(self.is_contact_artifact_token(item) and item in text for item in artifacts):
            return True
        return self.is_contact_artifact_token(text)

    def select_contact_artifacts_for_rule(self, rule: Any, artifacts: Sequence[str]) -> List[str]:
        _CONTACT_URL_RE = self.deps.contact_url_re
        _CONTACT_HANDLE_RE = self.deps.contact_handle_re
        _CONTACT_PHONE_RE = self.deps.contact_phone_re

        picked: List[str] = []
        for token in artifacts:
            item = str(token or "").strip()
            if not item:
                continue
            is_handle = bool(_CONTACT_HANDLE_RE.search(item))
            is_phone = bool(_CONTACT_PHONE_RE.search(item))
            is_link = bool(_CONTACT_URL_RE.search(item))
            if rule.wants_link and is_link:
                picked.append(item)
                continue
            if rule.wants_handle and is_handle:
                picked.append(item)
                continue
            if rule.wants_phone and is_phone:
                picked.append(item)
                continue
            if not (rule.wants_link or rule.wants_handle or rule.wants_phone):
                picked.append(item)
        if picked:
            out: List[str] = []
            seen: set[str] = set()
            for item in picked:
                marker = item.lower()
                if marker in seen:
                    continue
                seen.add(marker)
                out.append(item)
            return out
        return [str(item).strip() for item in artifacts if str(item or "").strip()]

    def is_contact_request_text(self, text: str) -> bool:
        low = str(text or "").lower().replace("ё", "е")
        if not low:
            return False
        markers = (
            "телеграм",
            "telegram",
            "тг",
            "контакт",
            "номер",
            "телефон",
            "ссылка",
            "каталог",
            "pdf",
            "пдф",
            "прайс",
            "фото",
            "whatsapp",
            "ватсап",
            "вотсап",
        )
        return any(marker in low for marker in markers)

    def assistant_messages_since_contact(self, state: Any, artifacts: Sequence[str]) -> int:
        history = state.history if isinstance(state.history, list) else []
        if not history:
            return 10
        seen_assistant = 0
        for item in reversed(history):
            role = str(item.get("role") or "").strip().lower()
            if role != "assistant":
                continue
            seen_assistant += 1
            text = str(item.get("content") or "").strip()
            if self.reply_has_contact_artifact(text, artifacts):
                return seen_assistant - 1
        return seen_assistant

    def delivery_rule_matches(
        self,
        rule: Any,
        *,
        channel_name: str,
        last_user_message: str,
        known_facts: Mapping[str, str] | None = None,
        state: Any = None,
    ) -> bool:
        PersonaConditionalRule = self.deps.persona_conditional_rule_cls

        current_channel = str(channel_name or "").strip().lower()
        if rule.channel_scope and current_channel not in {ch.lower() for ch in rule.channel_scope}:
            return False
        if rule.condition_text:
            temp = PersonaConditionalRule(
                source_line=rule.source_line,
                condition_text=rule.condition_text,
                action_text=rule.source_line,
                expected_tokens=list(rule.expected_tokens or []),
            )
            return self.conditional_rule_matches(
                temp,
                last_user_message=last_user_message,
                known_facts=known_facts,
                state=state,
            )
        return True

    def delivery_intro_text(self, rule: Any, channel_name: str) -> str:
        _ = (rule, channel_name)
        return ""

    def strip_unsolicited_links(self, reply: str, last_user_message: str) -> str:
        candidate = str(reply or "").strip()
        if not candidate:
            return candidate
        user_text = str(last_user_message or "")
        if self.is_contact_request_text(user_text) or re.search(r"(?iu)https?://", user_text):
            return candidate
        if not re.search(r"(?iu)https?://", candidate):
            return candidate
        cleaned_lines: list[str] = []
        removed = False
        for raw_line in candidate.splitlines():
            line = str(raw_line or "")
            if re.fullmatch(r"(?iu)\s*https?://\S+\s*", line):
                removed = True
                continue
            cleaned_lines.append(line)
        out = "\n".join(line for line in cleaned_lines if line.strip()).strip()
        out = re.sub(r"(?iu)\s*https?://\S+", "", out).strip()
        out = re.sub(r"\s{2,}", " ", out).strip()
        if removed and out:
            return out
        return out or candidate

    def apply_persona_delivery_obligations(
        self,
        reply: str,
        *,
        persona_context: str,
        channel_name: str,
        last_user_message: str,
        known_facts: Mapping[str, str] | None = None,
        state: Any = None,
    ) -> str:
        SalesState = self.deps.sales_state_cls

        candidate = str(reply or "").strip()
        persona_text = str(persona_context or "").strip()
        if not candidate or not persona_text:
            return candidate
        compiled = self.compile_persona_rules(persona_text)
        if not compiled.delivery_rules or not compiled.contact_artifacts:
            return candidate
        out = candidate
        for rule in compiled.delivery_rules:
            if not self.delivery_rule_matches(
                rule,
                channel_name=channel_name,
                last_user_message=last_user_message,
                known_facts=known_facts,
                state=state,
            ):
                continue
            if (
                rule.wants_link
                and not rule.wants_handle
                and not rule.wants_phone
                and not str(rule.condition_text or "").strip()
                and not self.reply_has_contact_artifact(out, compiled.contact_artifacts)
                and not self.is_contact_request_text(last_user_message)
            ):
                continue
            chosen_artifacts = self.select_contact_artifacts_for_rule(rule, compiled.contact_artifacts)
            chosen_artifacts = [item for item in chosen_artifacts if self.is_contact_artifact_token(item)]
            if not chosen_artifacts:
                continue
            if self.reply_has_contact_artifact(out, chosen_artifacts):
                continue
            if isinstance(state, SalesState):
                since_contact = self.assistant_messages_since_contact(state, chosen_artifacts)
                if since_contact < max(
                    1, int(rule.min_assistant_gap or 1)
                ) and not self.is_contact_request_text(last_user_message):
                    continue
            intro = self.delivery_intro_text(rule, channel_name)
            payload_parts: List[str] = []
            if intro and intro not in out:
                payload_parts.append(intro)
            payload_parts.extend(chosen_artifacts)
            payload = "\n".join(part for part in payload_parts if part).strip()
            if not payload:
                continue
            out = f"{out}\n{payload}".strip()
        return self.strip_unsolicited_links(out, last_user_message)
