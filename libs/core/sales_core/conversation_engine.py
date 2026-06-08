from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from .conversation_playbook import AUTHORITY_KEYWORDS
from .conversation_playbook import BANT_TEMPLATES
from .conversation_playbook import CHALLENGER_PLAYBOOK
from .conversation_playbook import EMPATHY_NEGATIVE_TEMPLATES
from .conversation_playbook import EMPATHY_POSITIVE_TEMPLATES
from .conversation_playbook import IMPLICATION_KEYWORDS
from .conversation_playbook import NEED_PAYOFF_KEYWORDS
from .conversation_playbook import NEGATIVE_KEYWORDS
from .conversation_playbook import POSITIVE_KEYWORDS
from .conversation_playbook import PROBLEM_KEYWORDS
from .conversation_playbook import RECIPROCITY_TEMPLATES
from .conversation_playbook import SCARCITY_TEMPLATES
from .conversation_playbook import SOCIAL_PROOF_TEMPLATES
from .conversation_playbook import SPIN_TEMPLATES
from .conversation_playbook import TIMELINE_PATTERNS
from .conversation_playbook import UPSELL_TEMPLATES
from .conversation_playbook import analyze_sentiment_delta
from .models import SalesState


@dataclass(frozen=True)
class EngineDeps:
    infer_user_needs: Callable[[str], Dict[str, Any]]
    coerce_bool: Callable[[Any, bool], bool]
    env_bool: Callable[[str, bool], bool]
    remember_question_state: Callable[[SalesState, str], None]
    remember_cta_state: Callable[[SalesState, str], None]
    cta_allowed: Callable[[SalesState, str | None], bool]
    format_needs_for_prompt: Callable[[Dict[str, Any]], str]


class _NullPersonaHints:
    greeting: str = ""
    cta: str = ""
    closing: str = ""
    max_questions: Optional[int] = None

    @staticmethod
    def wants_short() -> bool:
        return False


class SalesConversationEngine:
    def __init__(
        self,
        state: SalesState,
        branding: Dict[str, str],
        tenant_cfg: Dict[str, Any],
        channel_name: str,
        persona_hints: Optional[Any] = None,
        deps: EngineDeps | None = None,
    ) -> None:
        self.state = state
        self.branding = branding
        self.cfg = tenant_cfg if isinstance(tenant_cfg, dict) else {}
        self.channel_name = channel_name.strip() or branding.get("CHANNEL", "WhatsApp")
        self.persona_hints = persona_hints or _NullPersonaHints()
        if deps is None:
            raise ValueError("EngineDeps is required for SalesConversationEngine")
        self.deps = deps

    # ------------------------ анализ входящего текста --------------------
    def observe_user(self, text: str) -> None:
        incoming = (text or "").strip()
        if not incoming:
            return
        if incoming == self.state.last_user_text:
            return
        self.state.last_user_text = incoming
        self.state.append_history("user", incoming)
        self.state.last_updated_ts = time.time()
        self.state.user_message_count += 1
        if self.state.last_question_text:
            self.state.last_question_text = self.state.last_question_text.strip()

        self._touch_profile()

        needs = self.deps.infer_user_needs(incoming)
        if needs:
            for key, value in needs.items():
                if value:
                    self.state.needs[key] = value
        self._update_spin(incoming)
        self._update_bant(incoming)
        self._update_conversion_score(incoming)
        self._update_sentiment(incoming)
        self._update_profile_preferences()

    def _update_conversion_score(self, text: str) -> None:
        score = self.state.conversion_score * 0.9  # лёгкое затухание — RL-подход
        low = text.lower()
        if any(word in low for word in POSITIVE_KEYWORDS):
            score += 0.8
        if any(word in low for word in NEGATIVE_KEYWORDS):
            score -= 0.9
        score = max(-3.0, min(5.0, score))
        self.state.conversion_score = score

    def _update_spin(self, text: str) -> None:
        low = text.lower()
        if self.state.needs and self.state.spin.get("s") != "covered":
            self.state.mark_spin_stage("s", "covered")
        if any(word in low for word in PROBLEM_KEYWORDS):
            self.state.mark_spin_stage("p", "covered")
        if any(word in low for word in IMPLICATION_KEYWORDS):
            self.state.mark_spin_stage("i", "covered")
        if any(word in low for word in NEED_PAYOFF_KEYWORDS):
            self.state.mark_spin_stage("n", "covered")

    def _update_bant(self, text: str) -> None:
        budget = self._extract_budget(text)
        if budget:
            self.state.bant["budget"] = budget
            self.state.bant.pop("_asked_budget", None)
            self.state.conversion_score += 0.2

        if any(key in text.lower() for key in AUTHORITY_KEYWORDS):
            self.state.bant["authority"] = "decision_maker"
            self.state.bant.pop("_asked_authority", None)

        if any(word in text.lower() for word in NEED_PAYOFF_KEYWORDS):
            self.state.bant["need"] = True
            self.state.bant.pop("_asked_need", None)

        timeline = self._extract_timeline(text)
        if timeline:
            self.state.bant["timeline"] = timeline
            self.state.bant.pop("_asked_timeline", None)

    def _touch_profile(self) -> None:
        if not isinstance(self.state.profile, dict):
            self.state.profile = {}
        profile = self.state.profile
        now_ts = time.time()
        profile["last_seen_ts"] = now_ts
        day_bucket = int(now_ts // 86400)
        last_bucket = int(profile.get("last_visit_day", -1) or -1)
        if last_bucket != day_bucket:
            profile["visits"] = int(profile.get("visits", 0)) + 1
            profile["last_visit_day"] = day_bucket

        channels = profile.setdefault("channels", [])
        if self.channel_name and self.channel_name not in channels:
            channels.append(self.channel_name)
            if len(channels) > 6:
                profile["channels"] = channels[-6:]

    def _update_sentiment(self, text: str) -> None:
        delta = analyze_sentiment_delta(text)
        if delta == 0.0:
            self.state.sentiment_score *= 0.85
            return
        blended = self.state.sentiment_score * 0.6 + delta
        self.state.sentiment_score = max(-3.0, min(3.0, blended))

    def _update_profile_preferences(self) -> None:
        profile = self.state.profile
        if not isinstance(profile, dict):
            profile = {}
            self.state.profile = profile
        prefs = profile.setdefault("preferences", {})

        color = self.state.needs.get("color")
        if color:
            colors = prefs.setdefault("colors", [])
            if color not in colors:
                colors.append(color)
                if len(colors) > 5:
                    prefs["colors"] = colors[-5:]

        keywords = self.state.needs.get("keywords") or []
        if keywords:
            pref_keywords = prefs.setdefault("keywords", [])
            for kw in keywords[:4]:
                if kw not in pref_keywords:
                    pref_keywords.append(kw)
            if len(pref_keywords) > 10:
                prefs["keywords"] = pref_keywords[-10:]

        budget = self.state.needs.get("budget_max")
        if budget:
            try:
                budget_val = int(budget)
            except Exception:
                budget_val = None
            if budget_val:
                prev = prefs.get("budget_max")
                if not prev:
                    prefs["budget_max"] = budget_val
                else:
                    try:
                        prev_val = int(prev)
                    except Exception:
                        prefs["budget_max"] = budget_val
                    else:
                        prefs["budget_max"] = min(prev_val, budget_val)

    @staticmethod
    def _extract_budget(text: str) -> Optional[int]:
        raw = text.lower()
        matches = re.finditer(r"(\d+[\s\d]*)\s*(k|тыс|тысяч)?", raw)
        best: Optional[int] = None
        for m in matches:
            digits = m.group(1).replace(" ", "")
            try:
                val = int(digits)
            except Exception:
                continue
            if m.group(2):
                val *= 1000
            if val < 100:  # вероятно указали тысячи
                continue
            if not best or val > best:
                best = val
        return best

    @staticmethod
    def _extract_timeline(text: str) -> Optional[str]:
        for pattern, label in TIMELINE_PATTERNS:
            if pattern.search(text):
                return label
        return None

    # ------------------------ формирование предложения -------------------
    def register_recommendations(self, items: List[Dict[str, Any]]) -> None:
        if items:
            self.state.last_items = items[:]

    def _explain_mode_enabled(self) -> bool:
        behavior_cfg: Mapping[str, Any] | dict = {}
        if isinstance(self.cfg, dict):
            raw_behavior = self.cfg.get("behavior")
            if isinstance(raw_behavior, Mapping):
                behavior_cfg = raw_behavior
        explain_value = behavior_cfg.get("explain") if behavior_cfg else False
        return self.deps.coerce_bool(explain_value, False) or self.deps.env_bool(
            "EXPLAIN_MODE", False
        )

    def _focus_phrase(self) -> str:
        focus = str(self.state.needs.get("focus") or "").strip()
        if focus:
            return focus
        tokens: List[str] = []
        if self.state.needs.get("type"):
            tokens.append(str(self.state.needs["type"]).strip())
        if self.state.needs.get("width"):
            tokens.append(f"{self.state.needs['width']} см")
        if self.state.needs.get("color"):
            tokens.append(f"цвет {self.state.needs['color']}")
        if self.state.needs.get("keywords"):
            tokens.extend(str(x) for x in self.state.needs["keywords"][:2])
        focus_line = " ".join(token for token in tokens if token).strip()
        return focus_line or "вашей задаче"

    def _active_listening_line(self, text: str) -> str:
        cleaned = re.sub(r"\s+", " ", (text or "").strip())
        if len(cleaned) > 120:
            cleaned = cleaned[:117] + "..."
        focus = self._focus_phrase()
        empathy = self._empathy_prefix()
        explain_mode = self._explain_mode_enabled()
        if cleaned:
            base = f"Понял запрос: {cleaned}. Держу в фокусе {focus}." if explain_mode else ""
        else:
            base = f"Учитываю частые запросы по {focus} и сразу показываю сильные позиции."
        if empathy:
            return f"{empathy} {base}".strip()
        return base

    def _empathy_prefix(self) -> str:
        score = self.state.sentiment_score
        focus = self._focus_phrase()
        if score <= -0.5 and EMPATHY_NEGATIVE_TEMPLATES:
            idx = max(0, (self.state.user_message_count - 1)) % len(EMPATHY_NEGATIVE_TEMPLATES)
            return EMPATHY_NEGATIVE_TEMPLATES[idx].format(focus=focus)
        if score >= 1.2 and EMPATHY_POSITIVE_TEMPLATES:
            idx = max(0, (self.state.user_message_count - 1)) % len(EMPATHY_POSITIVE_TEMPLATES)
            return EMPATHY_POSITIVE_TEMPLATES[idx].format(focus=focus)
        visits = int((self.state.profile or {}).get("visits", 0))
        if visits > 1:
            return ""
        return ""

    def _format_price(self, price: Optional[int], currency: str) -> str:
        if price is None:
            return ""
        return f"{price:,}".replace(",", " ") + f" {currency}"

    def _price_from_item(self, item: Dict[str, Any]) -> Optional[int]:
        raw = str(item.get("price") or "").strip()
        digits = re.sub(r"\D", "", raw)
        if not digits:
            return None
        try:
            return int(digits)
        except Exception:
            return None

    def _fab_line(self, item: Dict[str, Any], idx: int, currency: str) -> str:
        title = (
            item.get("title")
            or item.get("name")
            or item.get("sku")
            or item.get("id")
            or f"Позиция {idx}"
        )
        price_val = self._price_from_item(item)
        price_text = self._format_price(price_val, currency)

        highlight_bits = []
        if item.get("brand"):
            highlight_bits.append(f"бренд {item['brand']}")
        if item.get("material"):
            highlight_bits.append(f"материал {item['material']}")
        if item.get("color"):
            highlight_bits.append(f"цвет {item['color']}")
        if item.get("width"):
            highlight_bits.append(f"ширина {item['width']} см")

        stock_note: List[str] = []
        stock = item.get("stock")
        try:
            stock_val = int(str(stock)) if stock is not None and str(stock).strip() else None
        except Exception:
            stock_val = None
        if stock_val is not None and stock_val > 0:
            stock_note.append("в наличии, отправим без ожидания")
        if item.get("tags") and "хит" in str(item.get("tags")).lower():
            stock_note.append("хит продаж")

        benefit = self._benefit_hint(item)

        line = f"{idx}. {title} — {price_text}"
        if highlight_bits:
            line += f"; {', '.join(highlight_bits)}"
        if stock_note:
            line += f"; {', '.join(stock_note)}"
        line += f". {benefit}"

        url = (item.get("url") or "").strip()
        if url:
            line += f" [{url}]"
        return line

    def _benefit_hint(self, item: Dict[str, Any]) -> str:
        needs = self.state.needs
        if needs.get("budget_max"):
            return ""
        keywords = needs.get("keywords") or []
        if keywords:
            return (
                f"Помогает с {keywords[0]} и экономит время на выборе."
                if keywords
                else "Подходит под ваш запрос."
            )
        focus = needs.get("focus") or needs.get("type")
        if focus:
            return str(focus or "").strip()
        return ""

    def _fab_block(self, items: List[Dict[str, Any]], currency: str) -> str:
        if not items:
            return (
                "1. Базовый вариант — подберу после пары уточнений "
                "(параметры, бюджет, сроки). Укладывается в целевой бюджет."
            )
        lines = [self._fab_line(item, idx, currency) for idx, item in enumerate(items, start=1)]
        return "\n".join(lines)

    def _next_spin_question(self) -> Optional[str]:
        focus = self._focus_phrase()
        for stage in ("s", "p", "i", "n"):
            status = self.state.spin.get(stage, "pending")
            if status == "pending":
                template = random.choice(SPIN_TEMPLATES[stage])
                question = template.format(focus=focus)
                self.state.mark_spin_stage(stage, "asked")
                return question
        return None

    def _next_bant_question(self, currency: str) -> Optional[str]:
        order = ["budget", "need", "timeline", "authority"]
        focus = self._focus_phrase()
        for key in order:
            value = self.state.bant.get(key)
            asked_flag = self.state.bant.get(f"_asked_{key}")
            if value or asked_flag:
                continue
            template = random.choice(BANT_TEMPLATES[key])
            question = template.format(
                currency=currency, focus=focus, city=self.branding.get("CITY", "")
            )
            self.state.bant[f"_asked_{key}"] = True
            return question
        return None

    def _choose_question(self, currency: str, max_per_turn: int) -> Optional[str]:
        if max_per_turn <= 0:
            return None
        question = self._next_bant_question(currency)
        if not question:
            question = self._next_spin_question()
        if not question:
            return None
        self._remember_question(question)
        return question

    def _remember_question(self, question: str) -> None:
        self.deps.remember_question_state(self.state, question)

    def _remember_cta(self, cta_text: str) -> None:
        self.deps.remember_cta_state(self.state, cta_text)

    def pending_question(self) -> Optional[str]:
        return None

    def _challenger_block(self) -> Tuple[str, str, str]:
        key = str(self.state.needs.get("type") or "default").lower()
        options = CHALLENGER_PLAYBOOK.get(key) or CHALLENGER_PLAYBOOK["default"]
        idx = self.state.challenger_cursor % len(options)
        play = options[idx]
        self.state.challenger_cursor += 1
        focus = self._focus_phrase()
        teach = play["teach"].format(city=self.branding.get("CITY", ""), focus=focus)
        tailor = play["tailor"].format(city=self.branding.get("CITY", ""), focus=focus)
        control = play["control"].format(city=self.branding.get("CITY", ""), focus=focus)
        return teach, tailor, control

    def _choose_social_proof(self, items: List[Dict[str, Any]]) -> Optional[str]:
        template = SOCIAL_PROOF_TEMPLATES[
            self.state.social_proof_cursor % len(SOCIAL_PROOF_TEMPLATES)
        ]
        self.state.social_proof_cursor += 1
        return template.format(
            brand=self.branding.get("BRAND", "Бренд"), city=self.branding.get("CITY", "")
        )

    def _choose_scarcity(self, items: List[Dict[str, Any]]) -> Optional[str]:
        stock_values = []
        for it in items:
            stock = it.get("stock")
            try:
                val = int(str(stock))
            except Exception:
                continue
            if val >= 0:
                stock_values.append(val)
        template = SCARCITY_TEMPLATES[self.state.scarcity_cursor % len(SCARCITY_TEMPLATES)]
        self.state.scarcity_cursor += 1
        slot = "завтра"
        if self.state.bant.get("timeline"):
            slot = self.state.bant["timeline"]
        stock_text = (
            "несколько"
            if not stock_values
            else (str(min(stock_values)) if min(stock_values) > 0 else "последние")
        )
        return template.format(stock=stock_text, city=self.branding.get("CITY", ""), slot=slot)

    def _choose_reciprocity(self) -> Optional[str]:
        template = RECIPROCITY_TEMPLATES[self.state.reciprocity_cursor % len(RECIPROCITY_TEMPLATES)]
        self.state.reciprocity_cursor += 1
        return template

    def _choose_upsell(self) -> str:
        template = random.choice(UPSELL_TEMPLATES)
        integrations = self.cfg.get("integrations", {}) if isinstance(self.cfg, dict) else {}
        if integrations.get("pdf_catalog_url"):
            template += " Могу отправить PDF/Excel с полным каталогом — скажите формат."
        return template

    def _choose_cta(self, cta_primary: str, cta_fallback: str) -> str:
        if self.persona_hints.cta:
            return self.persona_hints.cta
        score = self.state.conversion_score
        timeline = self.state.bant.get("timeline")
        handoff = (
            (self.cfg.get("cta") or {}).get("handoff_wa") if isinstance(self.cfg, dict) else ""
        )
        sentiment = self.state.sentiment_score
        if sentiment <= -1.2:
            return cta_primary or cta_fallback
        if score >= 1.5 and timeline:
            return str(timeline or "").strip()
        if sentiment >= 1.2 and score >= 1.0:
            return cta_primary or cta_fallback
        if score <= -1:
            return cta_fallback or cta_primary
        if self.channel_name.lower() == "avito" and handoff:
            return handoff
        candidate = cta_primary or cta_fallback
        return candidate

    def _personalized_greeting(self) -> str:
        default_greeting = f"Здравствуйте! Меня зовут {self.branding.get('AGENT_NAME', 'Менеджер')}, {self.branding.get('BRAND', '')}."
        greeting = (self.persona_hints.greeting or default_greeting).strip()
        if not greeting:
            greeting = default_greeting
        visits = int((self.state.profile or {}).get("visits", 0))
        if visits > 1:
            addon = "Рады снова вас видеть и продолжить подбор."
            if addon not in greeting:
                if greeting.endswith((".", "!", "…")):
                    greeting = f"{greeting.rstrip('.')}. {addon}"
                else:
                    greeting = f"{greeting}. {addon}"
        return greeting

    def _loyalty_line(self) -> Optional[str]:
        profile = self.state.profile if isinstance(self.state.profile, dict) else {}
        prefs = profile.get("preferences") or {}
        pieces: List[str] = []
        colors = prefs.get("colors") or []
        if colors:
            pieces.append(f"цвету {colors[-1]}")
        keywords = prefs.get("keywords") or []
        if keywords:
            pieces.append(f"темам «{keywords[-1]}»")
        budget = prefs.get("budget_max")
        if budget:
            pieces.append(
                f"бюджету до {self._format_price(int(budget), self.branding.get('CURRENCY', '₽'))}"
            )

        if pieces:
            joined = ", ".join(pieces)
            return (
                f"Помню ваши предпочтения по {joined} — покажу то, что действительно откликается."
            )

        visits = int(profile.get("visits", 0) or 0)
        if visits > 1:
            return None
        return None

    def build_reply(
        self,
        items: List[Dict[str, Any]],
        cta_primary: str,
        cta_fallback: str,
        currency: str,
        last_user_text: str,
    ) -> str:
        self.register_recommendations(items)
        max_questions_cfg = int(
            (self.cfg.get("behavior", {}) or {}).get("max_clarifying_questions", 1)
        )
        if self.persona_hints.max_questions is not None:
            try:
                max_questions_cfg = max(0, int(self.persona_hints.max_questions))
            except Exception:
                pass
        current_turn = max(1, self.state.user_message_count)

        question_line = self._choose_question(currency, max_questions_cfg)
        greeting = self._personalized_greeting()
        loyalty_line = self._loyalty_line()
        cta_line = ""

        if current_turn <= 1:
            intro_parts = [greeting]
            if loyalty_line:
                intro_parts.append(loyalty_line)
            if question_line:
                intro_parts.append(question_line)
            reply_intro = "\n\n".join(part.strip() for part in intro_parts if part and part.strip())
            if not reply_intro:
                reply_intro = greeting or (question_line or "")
            reply_intro = reply_intro.strip()
            self.state.last_bot_reply = reply_intro
            self.state.append_history("assistant", reply_intro)
            self.state.last_updated_ts = time.time()
            return reply_intro

        teach, tailor, _ = self._challenger_block()
        listening_line = self._active_listening_line(last_user_text)
        fab_block = self._fab_block(items, currency)
        social_proof = self._choose_social_proof(items)
        scarcity = self._choose_scarcity(items)
        reciprocity = self._choose_reciprocity()
        upsell = self._choose_upsell()
        if self.deps.cta_allowed(self.state, self.channel_name):
            cta_line = self._choose_cta(cta_primary, cta_fallback).strip()
            if cta_line:
                self._remember_cta(cta_line)
        else:
            cta_line = ""
        message_parts = {
            "greeting": greeting,
            "teach": teach,
            "listening": listening_line,
            "question": question_line or "",
            "tailor": tailor,
            "loyalty": loyalty_line or "",
            "fab": fab_block,
            "social": social_proof,
            "scarcity": scarcity,
            "upsell": upsell,
            "reciprocity": reciprocity,
            "cta": cta_line or "",
            "closing": self.persona_hints.closing or "",
        }

        ordered_keys = [
            "greeting",
            "teach",
            "listening",
            "question",
            "loyalty",
            "tailor",
            "fab",
            "social",
            "scarcity",
            "upsell",
            "reciprocity",
            "cta",
            "closing",
        ]

        cleaned = [
            message_parts[key].strip()
            for key in ordered_keys
            if message_parts[key] and message_parts[key].strip()
        ]
        if self.persona_hints.wants_short():
            prioritized = [
                message_parts.get("greeting", ""),
                message_parts.get("listening", ""),
                message_parts.get("question", ""),
                message_parts.get("loyalty", ""),
                message_parts.get("fab", ""),
                message_parts.get("cta", ""),
                message_parts.get("closing", ""),
            ]
            cleaned = [part.strip() for part in prioritized if part and part.strip()]

        reply = "\n\n".join(cleaned)
        self.state.last_bot_reply = reply
        self.state.append_history("assistant", reply)
        self.state.last_updated_ts = time.time()
        return reply

    def summary_for_llm(self) -> str:
        needs_summary = self.deps.format_needs_for_prompt(self.state.needs)
        bant_parts = []
        for key in ("budget", "need", "timeline", "authority"):
            if key in self.state.bant and not str(key).startswith("_"):
                bant_parts.append(f"{key}={self.state.bant[key]}")
        if not bant_parts:
            bant_parts.append("недостаточно данных")
        spin_parts = [
            f"{stage.upper()}={self.state.spin.get(stage, 'pending')}"
            for stage in ("s", "p", "i", "n")
        ]
        pending_question = self.pending_question()
        summary = [
            f"Needs: {needs_summary}",
            f"BANT: {', '.join(bant_parts)}",
            f"SPIN: {', '.join(spin_parts)}",
            f"Score={round(self.state.conversion_score, 2)}",
        ]
        if pending_question:
            summary.append(f"Следующий вопрос: {pending_question}")
        return "; ".join(summary)
