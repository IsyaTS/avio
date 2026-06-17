from __future__ import annotations

from libs.core.reply_quality_checker import check_reply_quality


def test_empty_reply_is_violation() -> None:
    violations = check_reply_quality("Сколько стоит?", "")
    assert "empty_reply" in violations


def test_fallback_is_generic_fallback_violation() -> None:
    violations = check_reply_quality(
        "Сколько стоит?",
        "Извините, сейчас не могу ответить",
        source="fallback_empty",
    )
    assert "generic_fallback" in violations


def test_size_already_given_but_reasked_is_violation() -> None:
    violations = check_reply_quality(
        "Размер 2050-900, дверь на вход, подскажите",
        "Назовите, пожалуйста, размер ещё раз, чтобы лучше подобрать варианты",
        source="llm",
    )
    assert "ignored_size" in violations


def test_city_already_given_but_reasked_is_violation() -> None:
    violations = check_reply_quality(
        "Доставка в Уфу возможна?",
        "Уточните, пожалуйста, в каком городе нужна доставка",
        source="llm",
    )
    assert "ignored_city" in violations


def test_close_intent_can_be_answered_without_missing_literal_phrase() -> None:
    violations = check_reply_quality(
        "Уже решили вопрос",
        "Хорошо, понял. Если снова понадобится — напишите, будем на связи.",
        expected={"should_contain_meaning": "закрыть диалог"},
        source="rule_fallback",
    )
    assert violations == []


def test_detect_rules_use_russian_markers() -> None:
    violations = check_reply_quality(
        "Интересует 2 замка открывание левое размер 2050-900",
        "Данные по запросу вижу: размер, открывание, замки. Уточним подходящий вариант.",
        expected={"detect_rules": ["size", "opening", "locks"]},
        source="rule_fallback",
    )
    assert violations == []
