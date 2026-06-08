from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.api.web.services import client_dialogs_runtime


pytestmark = pytest.mark.unit


def _deps() -> SimpleNamespace:
    return SimpleNamespace(is_technical_max_title_fn=lambda _value: False)


def test_avito_dialog_title_uses_avito_login() -> None:
    title = client_dialogs_runtime._dialog_title(
        {"channel": "avito", "contact": "123456", "avito_login": "Наталья"},
        "avito",
        deps=_deps(),
    )

    assert title == "Наталья"


def test_avito_dialog_title_hides_numeric_contact() -> None:
    title = client_dialogs_runtime._dialog_title(
        {"channel": "avito", "contact": "123456", "avito_login": ""},
        "avito",
        deps=_deps(),
    )

    assert title == "Avito · клиент"
