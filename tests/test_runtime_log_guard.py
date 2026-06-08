from __future__ import annotations

import pytest

from scripts import runtime_log_guard


pytestmark = pytest.mark.unit


def test_runtime_log_guard_reports_critical_patterns_and_sanitizes_secrets():
    issues = runtime_log_guard.scan_lines(
        [
            "worker | event=unknown_tenant tenant=42 access_token=secret123",
            "app | Traceback (most recent call last): phone=+79991234567",
        ],
        service="worker",
    )

    assert [(issue.kind, issue.service) for issue in issues] == [
        ("unknown_tenant", "worker"),
        ("traceback", "worker"),
    ]
    assert "secret123" not in issues[0].line
    assert "+79991234567" not in issues[1].line


def test_runtime_log_guard_ignores_disabled_outbox_log_when_outbox_disabled():
    issues = runtime_log_guard.scan_lines(
        [
            "worker | event=outbox_worker_disabled",
            "worker | event=outbox loop disabled OUTBOX_ENABLED=0",
        ],
        service="worker",
        outbox_disabled=True,
    )

    assert issues == []


def test_runtime_log_guard_reports_outbox_consumption_when_outbox_disabled():
    issues = runtime_log_guard.scan_lines(
        ["worker | event=outbox_sent channel=avito tenant=999999"],
        service="worker",
        outbox_disabled=True,
    )

    assert len(issues) == 1
    assert issues[0].kind == "outbox_consumed_when_disabled"


def test_runtime_log_guard_builds_compose_command_with_since_and_files():
    command = runtime_log_guard.build_compose_logs_command(
        ["docker-compose.yml", "compose/ci/docker-compose.yml"],
        "worker",
        tail=50,
        since="10m",
    )

    assert command == [
        "docker",
        "compose",
        "-f",
        "docker-compose.yml",
        "-f",
        "compose/ci/docker-compose.yml",
        "logs",
        "--no-color",
        "--tail",
        "50",
        "--since",
        "10m",
        "worker",
    ]
