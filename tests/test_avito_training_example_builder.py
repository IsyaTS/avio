from __future__ import annotations

from datetime import datetime, timezone

import pytest

from libs.core.services.avito_dialog_filter import AvitoDialogMessage
from libs.core.services.avito_training_example_builder import build_training_examples, mask_contacts


pytestmark = pytest.mark.unit


def _msg(role: str, text: str) -> AvitoDialogMessage:
    return AvitoDialogMessage(role=role, text=text)


def test_one_accepted_dialog_creates_examples() -> None:
    examples = build_training_examples(
        [[_msg("client", "Нужна дверь"), _msg("manager", "Здравствуйте, подскажите размер")]],
        tenant_id=101,
        created_at=datetime(2026, 5, 22, tzinfo=timezone.utc),
    )

    assert len(examples) == 1
    assert examples[0]["source"] == "avito"
    assert examples[0]["tenant_id"] == 101
    assert examples[0]["channel"] == "avito"
    assert examples[0]["context"] == [{"role": "client", "text": "Нужна дверь"}]
    assert examples[0]["ideal_reply"] == {"role": "manager", "text": "Здравствуйте, подскажите размер"}
    assert examples[0]["quality"]["accepted"] is True
    assert examples[0]["created_at"] == "2026-05-22T00:00:00Z"


def test_manager_reply_uses_previous_context_only() -> None:
    examples = build_training_examples(
        [
            [
                _msg("client", "Нужна дверь"),
                _msg("manager", "Подскажите размер"),
                _msg("client", "90 на 210"),
                _msg("manager", "Такой размер можем поставить"),
            ]
        ],
        tenant_id=101,
    )

    by_reply = {item["ideal_reply"]["text"]: item for item in examples}

    assert len(examples) == 2
    assert by_reply["Подскажите размер"]["context"] == [{"role": "client", "text": "Нужна дверь"}]
    assert by_reply["Такой размер можем поставить"]["context"] == [
        {"role": "client", "text": "Нужна дверь"},
        {"role": "manager", "text": "Подскажите размер"},
        {"role": "client", "text": "90 на 210"},
    ]


def test_consecutive_manager_messages_are_merged_into_one_reply() -> None:
    examples = build_training_examples(
        [
            [
                _msg("client", "Сколько стоит дверь?"),
                _msg("manager", "Здравствуйте."),
                _msg("manager", "Стоимость зависит от размера."),
            ]
        ],
        tenant_id=101,
    )

    assert len(examples) == 1
    assert examples[0]["ideal_reply"]["text"] == "Здравствуйте. Стоимость зависит от размера."


def test_no_example_without_meaningful_client_context() -> None:
    examples = build_training_examples(
        [[_msg("manager", "Здравствуйте"), _msg("client", "Да"), _msg("manager", "Что вас интересует?")]],
        tenant_id=101,
    )

    assert examples == []


def test_short_manager_replies_are_kept() -> None:
    examples = build_training_examples(
        [
            [
                _msg("client", "Какая стоимость двери?"),
                _msg("manager", "Да"),
                _msg("client", "Можно подробнее?"),
                _msg("manager", "[PHONE]"),
                _msg("client", "Что по срокам?"),
                _msg("manager", "Установка обычно занимает один день"),
            ]
        ],
        tenant_id=101,
    )

    replies = {item["ideal_reply"]["text"] for item in examples}
    assert replies == {"Да", "Установка обычно занимает один день"}


def test_service_and_followup_replies_are_skipped() -> None:
    examples = build_training_examples(
        [
            [
                _msg("client", "Скиньте каталог"),
                _msg("manager", "Отправили в мах"),
                _msg("client", "Спасибо"),
                _msg("manager", "Открыли каталог?"),
                _msg("client", "Нужна дверь в дом"),
                _msg("manager", "Подскажите размер проема?"),
            ]
        ],
        tenant_id=101,
    )

    replies = {item["ideal_reply"]["text"] for item in examples}
    assert replies == {"Подскажите размер проема?"}


def test_exact_duplicate_examples_are_removed() -> None:
    dialog = [_msg("client", "Нужна дверь"), _msg("manager", "Подскажите размер проема?")]

    examples = build_training_examples([dialog, dialog], tenant_id=101)

    assert len(examples) == 1
    assert len({item["example_id"] for item in examples}) == 1


def test_system_roles_ignored_if_passed_accidentally() -> None:
    examples = build_training_examples(
        [
            [
                _msg("system", "Системное сообщение"),
                _msg("client", "Нужна дверь"),
                _msg("manager", "Подскажите размер проема?"),
            ]
        ],
        tenant_id=101,
    )

    assert len(examples) == 1
    assert "system" not in str(examples)
    assert "Системное сообщение" not in str(examples)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("Позвоните 79990000000", "Позвоните [PHONE]"),
        ("Позвоните 8,999,000,00,00ТГ", "Позвоните [PHONE]ТГ"),
        ("Номер 8987 два 46 199шесть Ватсап", "Номер [PHONE] Ватсап"),
        ("Почта user@example.com", "Почта [EMAIL]"),
        ("Сайт https://example.com/page", "Сайт [LINK]"),
        ("Пишите @manager_name", "Пишите [HANDLE]"),
    ],
)
def test_contacts_are_masked(source: str, expected: str) -> None:
    assert mask_contacts(source) == expected


def test_stable_dialog_id_and_example_id_for_same_input() -> None:
    dialog = [[_msg("client", "Нужна дверь"), _msg("manager", "Подскажите размер проема?")]]

    first = build_training_examples(dialog, tenant_id=101, created_at=datetime(2026, 5, 22, tzinfo=timezone.utc))
    second = build_training_examples(dialog, tenant_id=101, created_at=datetime(2026, 5, 22, tzinfo=timezone.utc))

    assert first[0]["dialog_id"] == second[0]["dialog_id"]
    assert first[0]["example_id"] == second[0]["example_id"]
    assert first[0]["example_id"].endswith("_0001")


def test_no_raw_metadata_fields() -> None:
    examples = build_training_examples(
        [
            [
                AvitoDialogMessage(role="client", text="Нужна дверь", metadata={"chat_id": "secret-chat"}),
                AvitoDialogMessage(
                    role="manager",
                    text="Подскажите размер проема?",
                    metadata={"account_id": "secret-account"},
                ),
            ]
        ],
        tenant_id=101,
    )

    text = str(examples[0])
    assert "metadata" not in text
    assert "secret-chat" not in text
    assert "secret-account" not in text
