from __future__ import annotations

import pytest

from apps.api.web.services import landing_contact_runtime


pytestmark = pytest.mark.unit


def test_normalize_landing_contact_accepts_telegram_links_and_usernames() -> None:
    assert landing_contact_runtime.normalize_landing_contact("https://t.me/ivan_sales") == (
        "@ivan_sales",
        None,
    )
    assert landing_contact_runtime.normalize_landing_contact("ivan_sales") == ("@ivan_sales", None)


def test_normalize_landing_contact_accepts_russian_phone() -> None:
    assert landing_contact_runtime.normalize_landing_contact("8 999 123-45-67") == (
        "+79991234567",
        None,
    )


def test_validate_landing_contact_rejects_missing_or_short_phone() -> None:
    empty = landing_contact_runtime.LandingContact(name="", contact="", message="")
    short = landing_contact_runtime.LandingContact(name="", contact="7999", message="")

    assert landing_contact_runtime.validate_landing_contact(empty)[1].detail == "contact_required"  # type: ignore[union-attr]
    assert landing_contact_runtime.validate_landing_contact(short)[1].detail == "invalid_phone_length"  # type: ignore[union-attr]


def test_parse_landing_contact_payload_aliases_and_limits() -> None:
    parsed = landing_contact_runtime.parse_landing_contact_payload(
        {"name": "A" * 140, "phoneOrTelegram": "@sales_bot", "project": "x" * 2500}
    )

    assert parsed.name == "A" * 120
    assert parsed.contact == "@sales_bot"
    assert parsed.message == "x" * 2000
