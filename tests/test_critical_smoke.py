from __future__ import annotations

import pytest

from scripts import critical_smoke


pytestmark = pytest.mark.unit


def test_wait_health_retries_until_ok(monkeypatch):
    calls = iter(
        [
            (0, {"error": "ConnectionResetError"}),
            (200, {"ok": True}),
        ]
    )
    monkeypatch.setattr(critical_smoke, "_request_json", lambda _url: next(calls))
    monkeypatch.setattr(critical_smoke.time, "sleep", lambda _seconds: None)

    failures: list[str] = []

    assert critical_smoke._wait_health("http://app", timeout_seconds=3, failures=failures) is True
    assert failures == []


def test_wait_health_records_last_failure(monkeypatch):
    monkeypatch.setattr(critical_smoke, "_request_json", lambda _url: (503, {"ok": False}))
    monkeypatch.setattr(critical_smoke.time, "sleep", lambda _seconds: None)
    times = iter([0.0, 0.1, 2.0])
    monkeypatch.setattr(critical_smoke.time, "time", lambda: next(times))

    failures: list[str] = []

    assert critical_smoke._wait_health("http://app", timeout_seconds=1, failures=failures) is False
    assert failures == ["health status=503 payload={'ok': False}"]
