from __future__ import annotations

import pytest

from apps.worker.services import text_extract_runtime
from libs.core.common import normalize_username
from libs.core.message_envelope import sanitize_display_name


pytestmark = pytest.mark.unit


def test_extract_ru_phone_normalizes_russian_mobile_formats() -> None:
    assert text_extract_runtime.extract_ru_phone("8 (986) 666-61-33") == "+79866666133"
    assert text_extract_runtime.extract_ru_phone("79866666133") == "+79866666133"
    assert text_extract_runtime.extract_ru_phone("12345") == ""


def test_extract_tg_username_ignores_reserved_and_email_like_text() -> None:
    assert (
        text_extract_runtime.extract_tg_username(
            "мой tg: @Isyyaa",
            normalize_username_fn=normalize_username,
        )
        == "@Isyyaa"
    )
    assert (
        text_extract_runtime.extract_tg_username(
            "пишите сюда https://t.me/Isyyaa",
            normalize_username_fn=normalize_username,
        )
        == "@Isyyaa"
    )
    assert (
        text_extract_runtime.extract_tg_username(
            "email user@example.com",
            normalize_username_fn=normalize_username,
        )
        == ""
    )
    assert (
        text_extract_runtime.extract_tg_username(
            "https://t.me/joinchat",
            normalize_username_fn=normalize_username,
        )
        == ""
    )


def test_normalize_max_human_name_filters_machine_identifiers() -> None:
    assert (
        text_extract_runtime.normalize_max_human_name(
            "Иван П.",
            sanitize_display_name_fn=sanitize_display_name,
        )
        == "Иван П."
    )
    assert (
        text_extract_runtime.normalize_max_human_name(
            "max:123",
            sanitize_display_name_fn=sanitize_display_name,
        )
        is None
    )
    assert (
        text_extract_runtime.normalize_max_human_name(
            "12345",
            sanitize_display_name_fn=sanitize_display_name,
        )
        is None
    )
    assert (
        text_extract_runtime.normalize_max_human_name(
            "peer-1",
            peer_value="peer-1",
            sanitize_display_name_fn=sanitize_display_name,
        )
        is None
    )
