from __future__ import annotations

import pytest

from libs.core.services.avito_training_candidate_builder import AvitoTrainingCandidate
from libs.core.services import avito_training_rule_scorer as scorer


pytestmark = pytest.mark.unit


def candidate(reply: str, context: list[dict[str, str]] | None = None) -> AvitoTrainingCandidate:
    return AvitoTrainingCandidate(
        source="avito",
        tenant_id=1,
        dialog_id="d",
        candidate_id=f"c-{abs(hash(reply))}",
        example_id="d_0001",
        channel="avito",
        context=context or [{"role": "client", "text": "Нужна дверь"}],
        ideal_reply={"role": "manager", "text": reply},
        created_at="2026-05-22T00:00:00Z",
    )


def test_service_status_soft_flag() -> None:
    item = candidate("Отправили по ватсап")
    result = scorer.score_candidate(item)
    assert "service_status" in result.soft_flags
    assert scorer.needs_rule_fallback_review(item) is True


def test_send_later_status_soft_flag() -> None:
    item = candidate("Сейчас отправим")
    result = scorer.score_candidate(item)
    assert "service_status" in result.soft_flags
    assert scorer.needs_rule_fallback_review(item) is True


def test_followup_ping_soft_flag() -> None:
    item = candidate("Открыли каталог?")
    result = scorer.score_candidate(item)
    assert "followup_ping" in result.soft_flags


def test_short_reply_flag_but_not_reject() -> None:
    item = candidate("Сколько?")
    result = scorer.score_candidate(item)
    assert "short_reply" in result.soft_flags
    assert "short_clarifier_question" in result.soft_flags
    assert scorer.needs_rule_fallback_review(item) is False


def test_useful_sales_reply_has_no_bad_soft_flag() -> None:
    item = candidate("Здравствуйте. Для квартиры подойдет модель с утеплением, подскажите размер проема?")
    result = scorer.score_candidate(item)
    assert "service_status" not in result.soft_flags
    assert "followup_ping" not in result.soft_flags


def test_contact_catalog_transfer_flagged() -> None:
    item = candidate("Отправим каталог в ватсап")
    result = scorer.score_candidate(item)
    assert "catalog_or_contact_transfer" in result.soft_flags


def test_long_context_flagged() -> None:
    context = [{"role": "client" if i % 2 == 0 else "manager", "text": f"text {i}"} for i in range(14)]
    item = candidate("Уточните размер", context=context)
    result = scorer.score_candidate(item)
    assert "long_context" in result.soft_flags


def test_phone_like_flag_alone_is_not_fallback_review() -> None:
    item = candidate("Размер 180 на 86 есть в наличии, цена 25000 с установкой")
    item.soft_flags = ["phone_like_after_mask"]
    assert scorer.needs_rule_fallback_review(item) is False
