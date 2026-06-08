#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable, Sequence


DEFAULT_SERVICES = ("app", "worker")
DEFAULT_PATTERNS = (
    ("unknown_tenant", re.compile(r"\bunknown_tenant\b", re.IGNORECASE)),
    ("invalid_state", re.compile(r"\binvalid_state\b", re.IGNORECASE)),
    ("missing_state", re.compile(r"\bmissing_state\b", re.IGNORECASE)),
    ("traceback", re.compile(r"\btraceback \(most recent call last\)", re.IGNORECASE)),
    ("unhandled_exception", re.compile(r"\bunhandled(?:_| )exception\b", re.IGNORECASE)),
)
TOKEN_RE = re.compile(
    r"(?i)\b(access_token|refresh_token|client_secret|authorization|x-admin-token|x-auth-token)"
    r"\s*[:=]\s*([^\s,;]+)"
)
PHONE_RE = re.compile(r"(?<!\d)(?:\+?7|8)[\s\-()]*(?:\d[\s\-()]*){10}(?!\d)")


@dataclass(frozen=True)
class LogIssue:
    service: str
    kind: str
    line: str


def sanitize_log_line(line: str, *, max_len: int = 260) -> str:
    cleaned = TOKEN_RE.sub(r"\1=<redacted>", line)
    cleaned = PHONE_RE.sub("<phone:redacted>", cleaned)
    cleaned = cleaned.replace("\n", "\\n")
    if len(cleaned) > max_len:
        return cleaned[: max_len - 3] + "..."
    return cleaned


def scan_lines(
    lines: Iterable[str],
    *,
    service: str,
    outbox_disabled: bool = False,
) -> list[LogIssue]:
    issues: list[LogIssue] = []
    for raw_line in lines:
        line = raw_line.rstrip("\n")
        for kind, pattern in DEFAULT_PATTERNS:
            if pattern.search(line):
                issues.append(LogIssue(service=service, kind=kind, line=sanitize_log_line(line)))
        if outbox_disabled and _looks_like_outbox_consumption(line):
            issues.append(
                LogIssue(
                    service=service,
                    kind="outbox_consumed_when_disabled",
                    line=sanitize_log_line(line),
                )
            )
    return issues


def _looks_like_outbox_consumption(line: str) -> bool:
    lowered = line.lower()
    if "outbox" not in lowered:
        return False
    if "disabled" in lowered:
        return False
    return any(token in lowered for token in ("sent", "send_ok", "delivered", "consumed", "dequeued"))


def build_compose_logs_command(
    compose_files: Sequence[str],
    service: str,
    *,
    tail: int,
    since: str,
) -> list[str]:
    command = ["docker", "compose"]
    for compose_file in compose_files:
        command.extend(["-f", str(compose_file)])
    command.extend(["logs", "--no-color", "--tail", str(int(tail))])
    if since:
        command.extend(["--since", since])
    command.append(service)
    return command


def collect_service_logs(
    compose_files: Sequence[str],
    service: str,
    *,
    tail: int,
    since: str,
) -> list[str]:
    command = build_compose_logs_command(compose_files, service, tail=tail, since=since)
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        stderr = sanitize_log_line(result.stderr.strip() or "unknown docker compose logs error")
        raise RuntimeError(f"docker compose logs failed service={service} error={stderr}")
    return result.stdout.splitlines()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan recent app/worker docker compose logs for critical runtime regressions "
            "after smoke tests. This is read-only and sanitizes reported lines."
        )
    )
    parser.add_argument(
        "--compose-file",
        action="append",
        default=[],
        help="docker compose file; can be passed multiple times",
    )
    parser.add_argument(
        "--service",
        action="append",
        default=[],
        help="service to scan; defaults to app and worker",
    )
    parser.add_argument("--tail", type=int, default=1000)
    parser.add_argument("--since", default="", help="optional docker logs --since value, e.g. 10m")
    parser.add_argument(
        "--outbox-disabled",
        action="store_true",
        help="also fail if logs indicate outbox consumption while OUTBOX_ENABLED=0 smoke is active",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    compose_files = args.compose_file or ["docker-compose.yml"]
    services = args.service or list(DEFAULT_SERVICES)
    issues: list[LogIssue] = []
    for service in services:
        lines = collect_service_logs(
            compose_files,
            str(service),
            tail=int(args.tail),
            since=str(args.since or ""),
        )
        issues.extend(scan_lines(lines, service=str(service), outbox_disabled=bool(args.outbox_disabled)))

    if issues:
        print("runtime log guard failed:", file=sys.stderr)
        for issue in issues[:40]:
            print(f"- service={issue.service} kind={issue.kind} line={issue.line}", file=sys.stderr)
        if len(issues) > 40:
            print(f"- additional_issues={len(issues) - 40}", file=sys.stderr)
        raise SystemExit(1)

    print("runtime log guard ok")


if __name__ == "__main__":
    main()
