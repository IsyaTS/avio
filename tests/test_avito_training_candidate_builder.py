from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.core.services.avito_dialog_filter import AvitoDialogMessage
from libs.core.services import avito_training_candidate_builder as builder


pytestmark = pytest.mark.unit


def msg(role: str, text: str) -> AvitoDialogMessage:
    return AvitoDialogMessage(role=role, text=text, timestamp=None)


def test_builds_candidates_and_merges_manager_messages() -> None:
    result = builder.build_training_candidates(
        [[msg("client", "Здравствуйте, нужна дверь"), msg("manager", "Здравствуйте"), msg("manager", "Какой размер?")]],
        tenant_id=101,
        created_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
    )

    assert len(result.candidates) == 1
    candidate = result.candidates[0]
    assert candidate.ideal_reply["text"] == "Здравствуйте Какой размер?"
    assert candidate.context == [{"role": "client", "text": "Здравствуйте, нужна дверь"}]
    assert candidate.example_id.endswith("_0001")


def test_short_replies_are_kept() -> None:
    result = builder.build_training_candidates(
        [[msg("client", "От Оренбурга 40 км"), msg("manager", "Сколько?")]],
        tenant_id=1,
    )

    assert len(result.candidates) == 1
    assert result.hard_rejected_count == 0


def test_contact_only_reply_hard_rejected() -> None:
    result = builder.build_training_candidates(
        [[msg("client", "Как связаться?"), msg("manager", "89866666133")]],
        tenant_id=1,
    )

    assert result.candidates == []
    assert result.hard_reject_reasons == {"contact_only_reply": 1}


def test_no_client_context_and_last_context_not_client_rejected() -> None:
    empty_context = builder.build_training_candidates([[msg("manager", "Здравствуйте")]], tenant_id=1)
    last_not_client = builder.build_training_candidates(
        [[msg("client", "Нужна дверь"), msg("manager", "Здравствуйте"), msg("client", "ок"), msg("manager", "Уточните размер")]],
        tenant_id=1,
    )

    assert empty_context.candidates == []
    assert empty_context.hard_reject_reasons["empty_context"] == 1
    assert len(last_not_client.candidates) == 1
    assert last_not_client.hard_reject_reasons["last_context_not_client"] == 1


def test_masks_contacts_and_spelled_phone() -> None:
    result = builder.build_training_candidates(
        [
            [
                msg("client", "Мой телефон 8987 два 46 199шесть и email a@example.com"),
                msg("manager", "Напишите @dverigermes или откройте https://example.com"),
            ]
        ],
        tenant_id=1,
    )

    candidate = result.candidates[0]
    text = str(candidate.to_training_example())
    assert "[PHONE]" in text
    assert "[EMAIL]" in text
    assert "[HANDLE]" in text
    assert "[LINK]" in text
    assert "8987" not in text
    assert "a@example.com" not in text


def test_exact_duplicate_candidates_removed() -> None:
    dialog = [msg("client", "Нужна дверь"), msg("manager", "Какой размер?")]
    result = builder.build_training_candidates([dialog, dialog], tenant_id=1)

    assert len(result.candidates) == 1
    assert result.hard_reject_reasons == {"exact_duplicate": 1}


def test_system_roles_ignored_unknown_marks_broken() -> None:
    result = builder.build_training_candidates(
        [[msg("system", "Системное сообщение"), msg("unknown", "raw"), msg("client", "Нужна дверь"), msg("manager", "Ответ")]],
        tenant_id=1,
    )

    assert result.candidates == []
    assert result.hard_reject_reasons == {"system_or_unknown_role": 1}


def test_clear_autoresponder_phrase_hard_rejected() -> None:
    result = builder.build_training_candidates(
        [[msg("client", "Здравствуйте"), msg("manager", "Напишите стоп, чтобы отписаться от рассылки")]],
        tenant_id=1,
    )

    assert result.candidates == []
    assert result.hard_reject_reasons == {"clear_autoresponder_phrase": 1}
