from __future__ import annotations

import re

from libs.core.sales_core.fallback_runtime import FallbackRuntime, FallbackRuntimeDeps


def _runtime() -> FallbackRuntime:
    return FallbackRuntime(
        FallbackRuntimeDeps(
            grounding_catalog_items=lambda _grounding: [],
            classify_turn_intent=lambda _text: "",
            normalize_text=lambda value: str(value or "").lower(),
            shortlist_preview_text=lambda **_kwargs: "",
            extract_attribute_probe=lambda _text: "",
            display_item_label=lambda item: str(item.get("title") or ""),
            item_label=lambda item: str(item.get("title") or ""),
            catalog_min_price=lambda _items: None,
            catalog_max_price=lambda _items: None,
            format_rub_price=lambda value: f"{value} ₽",
            is_price_intent=lambda text: "цен" in text.lower(),
            looks_like_price_objection=lambda text: "дорого" in text.lower(),
            variants_user_hint_re=re.compile(r"$^"),
            price_inline_re=re.compile(r"$^"),
            price_thousands_re=re.compile(r"$^"),
            fact_token_re=re.compile(r"[а-яёa-z0-9]+", re.I),
            generic_fact_stopwords=set(),
        )
    )


def test_rule_fallback_does_not_reask_known_size() -> None:
    reply = _runtime().llm_unavailable_reply(
        user_text="Здравствуйте Интересует дверь 2 замка без глазка открывание левое размер 2050-900"
    )
    assert "размер" in reply.lower()
    assert "открывание" in reply.lower()
    assert "замки" in reply.lower()
    assert "какой размер" not in reply.lower()


def test_rule_fallback_acknowledges_contact() -> None:
    reply = _runtime().llm_unavailable_reply(user_text="[PHONE] Алексей")
    assert "контакт получил" in reply.lower()
    assert "что подбираете" not in reply.lower()


def test_rule_fallback_closes_decline() -> None:
    reply = _runtime().llm_unavailable_reply(user_text="Спасибо, уже решили вопрос")
    assert "если снова понадобится" in reply.lower()
    assert "что подбираете" not in reply.lower()
