from __future__ import annotations

import pytest

from scripts import ui_http_smoke


pytestmark = pytest.mark.unit


def test_ui_http_smoke_builds_expected_checks():
    checks = ui_http_smoke.build_checks(tenant=999999, public_key="test-public")
    paths = [check.path for check in checks]

    assert "/login" in paths
    assert "/register" in paths
    assert "/client/999999/settings?k=test-public" in paths
    assert "/connect/avito?tenant=999999&k=test-public" in paths


def test_ui_http_smoke_evaluate_page_reports_missing_fragment():
    failure = ui_http_smoke.evaluate_page("settings", 200, "<html>settings</html>", ["save-settings"])

    assert failure is not None
    assert failure.name == "settings"
    assert "save-settings" in failure.reason


def test_ui_http_smoke_evaluate_page_accepts_case_insensitive_fragments():
    failure = ui_http_smoke.evaluate_page("login", 200, "<input name='EMAIL'>", ["email"])

    assert failure is None


def test_ui_http_smoke_evaluate_page_reports_bad_status():
    failure = ui_http_smoke.evaluate_page("login", 500, "error", ["email"])

    assert failure is not None
    assert failure.reason == "status=500"
