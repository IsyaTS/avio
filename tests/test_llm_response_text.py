from __future__ import annotations

from types import SimpleNamespace

import pytest

from libs.core.sales_core.llm_response_text import extract_llm_response_text


pytestmark = pytest.mark.unit


def test_extract_llm_response_text_from_chat_message_content() -> None:
    response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=" Ответ "))],
    )

    assert extract_llm_response_text(response) == "Ответ"


def test_extract_llm_response_text_from_content_blocks() -> None:
    response = SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content=[
                        {"type": "text", "text": "Первая строка"},
                        {"content": "Вторая строка"},
                    ]
                )
            )
        ],
    )

    assert extract_llm_response_text(response) == "Первая строка\nВторая строка"


def test_extract_llm_response_text_from_output_text() -> None:
    response = SimpleNamespace(output_text="Готово")

    assert extract_llm_response_text(response) == "Готово"


def test_extract_llm_response_text_from_mapping_shape() -> None:
    response = SimpleNamespace(
        choices=[
            {
                "message": {
                    "content": [
                        {"text": "Текст из mapping"},
                    ]
                }
            }
        ]
    )

    assert extract_llm_response_text(response) == "Текст из mapping"
