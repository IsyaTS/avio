from __future__ import annotations

import pytest

from apps.api.web.services.client_reply_split_runtime import (
    ReplySplitConfig,
    apply_custom_punctuation_style,
    split_reply_for_test_send,
)


pytestmark = pytest.mark.unit


def _config(**overrides) -> ReplySplitConfig:
    values = {
        "enabled": True,
        "min_len": 70,
        "max_len": 120,
        "max_parts": 6,
        "channels": {"telegram", "avito", "whatsapp", "max", "max_personal"},
    }
    values.update(overrides)
    return ReplySplitConfig(**values)


def test_split_reply_keeps_disabled_channel_as_single_message() -> None:
    text = "Здравствуйте, какой город установки? Какой бюджет рассматриваете?"

    parts = split_reply_for_test_send(text, "sms", _config())

    assert parts == [text]


def test_split_reply_separates_greeting_question_combo() -> None:
    text = "Здравствуйте, какой город установки?"

    parts = split_reply_for_test_send(text, "avito", _config())

    assert parts == ["Здравствуйте", "какой город установки?"]


def test_split_reply_isolates_links_handles_and_phones() -> None:
    text = "Каталог тут https://example.test/catalog пишите @avio_test или звоните 79990000000"

    parts = split_reply_for_test_send(text, "telegram", _config(min_len=20, max_len=64))

    assert any("https://example.test/catalog" in part for part in parts)
    assert "@avio_test" in parts
    assert "79990000000" in parts


def test_split_reply_respects_max_parts() -> None:
    text = (
        "Первый вопрос по установке в вашем городе и срокам доставки? "
        "Второй вопрос по бюджету и нужному материалу двери? "
        "Третий вопрос по размеру проема и монтажу? "
        "Четвертый вопрос по цвету полотна и фурнитуре? "
        "Пятый вопрос по удобному времени связи?"
    )

    parts = split_reply_for_test_send(
        text,
        "max_personal",
        _config(min_len=10, max_len=70, max_parts=3),
    )

    assert len(parts) == 3
    assert "Пятый вопрос" in parts[-1]


def test_apply_custom_punctuation_style_keeps_urls_and_softens_sentence_flow() -> None:
    text = "Ок Понял. Каталог тут https://example.test/catalog, посмотрите, пожалуйста!"

    out = apply_custom_punctuation_style(text)

    assert "https://example.test/catalog" in out
    assert "Ок понял" in out
    assert out.endswith("пожалуйста")
