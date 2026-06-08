from __future__ import annotations

from libs.core.services.avito_dialog_filter import AvitoDialogMessage, evaluate_dialog


def _m(role: str, text: str) -> AvitoDialogMessage:
    return AvitoDialogMessage(role=role, text=text)


def test_short_useful_dialog_is_accepted() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Здравствуйте, дверь в квартиру нужна"),
            _m("manager", "Здравствуйте. Подскажите размер проема?"),
        ]
    )

    assert result.accepted is True


def test_repeated_manager_phrases_are_accepted_with_real_need() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Нужна дверь в дом"),
            _m("manager", "Здравствуйте"),
            _m("manager", "Каталог можем отправить в телеграм"),
            _m("client", "Нужна с установкой"),
            _m("manager", "По установке сориентируем после замера"),
        ]
    )

    assert result.accepted is True


def test_system_only_rejected() -> None:
    result = evaluate_dialog(
        [
            _m("system", "Системное сообщение"),
            _m("unknown", "Ссылка на объявление"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "system_only"


def test_no_real_client_rejected() -> None:
    result = evaluate_dialog([_m("manager", "Здравствуйте, отправим каталог")])

    assert result.accepted is False
    assert result.reject_reason == "no_real_client"


def test_no_real_manager_rejected() -> None:
    result = evaluate_dialog([_m("client", "Сколько стоит дверь в квартиру?")])

    assert result.accepted is False
    assert result.reject_reason == "no_real_manager"


def test_no_manager_answer_after_client_rejected() -> None:
    result = evaluate_dialog(
        [
            _m("manager", "Здравствуйте"),
            _m("client", "Нужна дверь в офис"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "no_manager_answer_after_client"


def test_manager_push_only_rejected() -> None:
    result = evaluate_dialog(
        [
            _m("manager", "Ранее вы интересовались дверями, получили наш каталог?"),
            _m("client", "Пришлите каталог"),
            _m("manager", "Напишите стоп, если неактуально"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "autoresponder_present"


def test_autoresponder_only_rejected() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Нужна дверь в квартиру"),
            _m("manager", "Автоответ: отправим каталог, напишите стоп если неактуально"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "autoresponder_present"


def test_mixed_autoresponder_dialog_is_rejected_without_deleting_messages() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Нужна дверь в квартиру"),
            _m("manager", "Автоответ: отправим каталог, напишите стоп если неактуально"),
            _m("manager", "Здравствуйте. Подскажите размер проема?"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "autoresponder_present"
    assert [message.text for message in result.messages] == [
        "Нужна дверь в квартиру",
        "Автоответ: отправим каталог, напишите стоп если неактуально",
        "Здравствуйте. Подскажите размер проема?",
    ]


def test_only_contacts_transfer_rejected() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Куда написать?"),
            _m("manager", "Напишите телефон, отправим каталог"),
            _m("client", "+7 900 000 00 00"),
            _m("manager", "Отправили в телеграм"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "only_contacts_transfer"


def test_starts_mid_context_rejected() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Да, такой вариант подходит"),
            _m("manager", "Тогда можем записать на замер"),
        ],
        context_complete=False,
    )

    assert result.accepted is False
    assert result.reject_reason == "starts_mid_context"


def test_system_service_messages_are_removed_before_export() -> None:
    result = evaluate_dialog(
        [
            _m("system", "Системное сообщение: Ссылка на объявление"),
            _m("client", "Нужна дверь в квартиру"),
            _m("manager", "Подскажите размер проема"),
        ]
    )

    assert result.accepted is True
    assert [message.role for message in result.messages] == ["client", "manager"]


def test_catalog_contact_allowed_with_useful_context() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Нужна дверь в дом с установкой"),
            _m("manager", "Отправим каталог, там есть варианты для дома"),
            _m("client", "Интересует цена с монтажом"),
            _m("manager", "Стоимость зависит от размера проема"),
        ]
    )

    assert result.accepted is True


def test_known_manager_catalog_template_is_not_autoresponder() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Нужна дверь в квартиру"),
            _m(
                "manager",
                "Наш номер 80000000000 ТГ,ВАТСАП, МАХ "
                "Либо отправьте пожалуйста Ваш номер телефона отправим каталог "
                "в любом удобном для Вас мессенджере Доставка и установка у нас бесплатная "
                "Оплата только после установки без предоплат с оплатой после установки",
            ),
            _m("manager", "Подскажите, в какое помещение нужна дверь?"),
        ]
    )

    assert result.accepted is True


def test_manager_phone_catalog_phrase_is_not_autoresponder_by_itself() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Здравствуйте, интересует входная дверь с установкой"),
            _m("manager", "Здравствуйте. Наш номер 80000000000, отправим каталог в ватсап"),
            _m("client", "Нужна дверь в частный дом"),
            _m("manager", "Подскажите размер проема?"),
        ]
    )

    assert result.accepted is True


def test_catalog_destination_question_is_not_autoresponder_by_itself() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Здравствуйте, можно каталог дверей для квартиры?"),
            _m("manager", "Здравствуйте, куда можем каталог отправить?"),
            _m("client", "В ватсап"),
            _m("manager", "Хорошо, подскажите номер"),
        ]
    )

    assert result.accepted is True


def test_dialog_with_catalog_and_visit_phrase_is_rejected_as_autoresponder() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Нужна дверь в квартиру"),
            _m("manager", "Здравствуйте, по каталогу и выездом сориентируем вас сегодня"),
            _m("manager", "Подскажите размер проема"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "autoresponder_present"


def test_dialog_with_stop_unsubscribe_phrase_is_rejected_as_autoresponder() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Нужна дверь в квартиру"),
            _m("manager", 'Напишите "стоп", чтобы отписаться от рассылки'),
            _m("manager", "Подскажите размер проема"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "autoresponder_present"


def test_dialog_with_received_catalog_phrase_is_rejected_as_autoresponder() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Здравствуйте, нужна дверь"),
            _m("manager", "Здравствуйте, получили наш каталог?"),
            _m("manager", "Подскажите размер проема"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "autoresponder_present"


def test_dialog_with_previous_interest_phrase_is_rejected_as_autoresponder() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Здравствуйте, нужна дверь"),
            _m("manager", "Ранее вы интересовались дверями, актуально?"),
            _m("manager", "Подскажите размер проема"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "autoresponder_present"


def test_dialog_with_repeated_dverigermes_handle_is_rejected_as_autoresponder() -> None:
    result = evaluate_dialog(
        [
            _m("client", "Нужна дверь в квартиру"),
            _m("manager", "Каталог смотрите тут @dverigermes"),
            _m("manager", "Также напишите нам @dverigermes"),
            _m("manager", "Подскажите размер проема"),
        ]
    )

    assert result.accepted is False
    assert result.reject_reason == "autoresponder_present"
    assert result.filter_stats["repeated_autoresponder_handle"] == 2
