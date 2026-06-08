from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.core.services.avito_contextual_case_builder import build_contextual_case_candidates
from libs.core.services.avito_dialog_filter import AvitoDialogMessage


pytestmark = pytest.mark.unit


def _m(role: str, text: str) -> AvitoDialogMessage:
    return AvitoDialogMessage(role=role, text=text, metadata={"raw_id": "must_not_leak"})


def _build(dialogs: list[list[AvitoDialogMessage]]):
    return build_contextual_case_candidates(
        dialogs,
        tenant_id=101,
        created_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
    )


def test_builds_candidate_cases_from_dialog() -> None:
    result = _build(
        [[_m("client", "Здравствуйте, нужна входная дверь"), _m("manager", "Здравствуйте, какой размер проема?")]]
    )

    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate.source == "avito"
    assert candidate.tenant_id == 101
    assert candidate.turn_index == 1
    assert candidate.history[0].role == "client"
    assert candidate.manager_reply.text == "Здравствуйте, какой размер проема?"


def test_consecutive_manager_messages_are_merged() -> None:
    result = _build(
        [
            [
                _m("client", "Здравствуйте, нужна дверь"),
                _m("manager", "Здравствуйте"),
                _m("manager", "Подскажите размер проема?"),
            ]
        ]
    )

    assert len(result.candidates) == 1
    assert result.candidates[0].manager_reply.text == "Здравствуйте Подскажите размер проема?"


def test_short_clarifier_reply_is_kept() -> None:
    result = _build([[_m("client", "Нужно установить дверь в доме"), _m("manager", "Размер?")]])

    assert len(result.candidates) == 1
    assert result.hard_rejected_count == 0


def test_no_case_without_meaningful_client_context() -> None:
    result = _build([[_m("client", "ок"), _m("manager", "Подскажите город?")]])

    assert result.candidates == []
    assert result.hard_reject_reasons["no_meaningful_client_context"] == 1


def test_no_case_if_last_context_role_is_not_client() -> None:
    result = _build(
        [
            [
                _m("client", "Нужна дверь"),
                _m("manager", "Какая дверь нужна?"),
                _m("client", "ок"),
                _m("manager", "Подскажите размер"),
            ]
        ]
    )

    assert len(result.candidates) == 1
    assert result.hard_reject_reasons["last_context_not_client"] == 1


def test_system_messages_ignored_and_unknown_role_rejected() -> None:
    accepted = _build(
        [
            [
                _m("system", "Ссылка на объявление"),
                _m("client", "Здравствуйте, цена двери?"),
                _m("manager", "Здравствуйте, какая модель интересует?"),
            ]
        ]
    )
    broken = _build([[_m("client", "Здравствуйте"), _m("bot", "unknown"), _m("manager", "Здравствуйте")]])

    assert len(accepted.candidates) == 1
    assert broken.candidates == []
    assert broken.hard_reject_reasons["system_or_unknown_role"] == 1


def test_contacts_are_masked() -> None:
    result = _build(
        [
            [
                _m("client", "Мой телефон 8987 два 46 199шесть, email test@example.com"),
                _m("manager", "Напишите на https://example.test или @dverigermes"),
            ]
        ]
    )

    candidate = result.candidates[0]
    text = f"{candidate.history[0].text} {candidate.manager_reply.text}"
    assert "[PHONE]" in text
    assert "[EMAIL]" in text
    assert "[LINK]" in text
    assert "[HANDLE]" in text
    assert "8987" not in text
    assert "test@example.com" not in text


def test_contact_only_reply_is_hard_rejected() -> None:
    result = _build([[_m("client", "Здравствуйте, куда приехать?"), _m("manager", "89866666133")]])

    assert result.candidates == []
    assert result.hard_reject_reasons["contact_only_reply"] == 1


def test_exact_duplicate_candidates_are_deduped_and_ids_are_stable() -> None:
    dialog = [_m("client", "Здравствуйте, есть двери?"), _m("manager", "Здравствуйте, какие размеры?")]
    first = _build([dialog, dialog])
    second = _build([dialog])

    assert len(first.candidates) == 1
    assert first.hard_reject_reasons["exact_duplicate"] == 1
    assert first.candidates[0].dialog_id == second.candidates[0].dialog_id
    assert first.candidates[0].case_id == second.candidates[0].case_id


def test_base_case_has_no_raw_metadata_fields() -> None:
    result = _build([[_m("client", "Здравствуйте, нужна дверь"), _m("manager", "Подскажите город?")]])

    assert "raw_id" not in str(result.candidates[0].base_case())
