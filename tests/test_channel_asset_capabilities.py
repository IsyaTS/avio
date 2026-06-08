from __future__ import annotations

import pytest

from libs.core.services.channel_asset_capabilities import can_send_asset


pytestmark = pytest.mark.unit


def test_telegram_pdf_allowed() -> None:
    assert can_send_asset("telegram", "pdf", "application/pdf").allowed is True


def test_avito_image_allowed() -> None:
    assert can_send_asset("avito", "photo", "image/jpeg").allowed is True


def test_avito_pdf_blocked_not_silent() -> None:
    decision = can_send_asset("avito", "pdf", "application/pdf")
    assert decision.allowed is False
    assert decision.reason == "avito_file_not_guaranteed"
