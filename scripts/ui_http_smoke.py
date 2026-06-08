#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class PageCheck:
    path: str
    name: str
    required_fragments: tuple[str, ...]
    optional_fragments: tuple[str, ...] = ()


@dataclass(frozen=True)
class PageFailure:
    name: str
    reason: str


def _request_text(url: str, *, timeout: float = 8.0) -> tuple[int, str]:
    request = urllib.request.Request(url, headers={"Accept": "text/html,application/xhtml+xml"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return int(response.status), response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
        return int(exc.code), exc.read().decode("utf-8", errors="replace")


def evaluate_page(name: str, status: int, html: str, required_fragments: Iterable[str]) -> PageFailure | None:
    if status != 200:
        return PageFailure(name=name, reason=f"status={status}")
    lowered = html.lower()
    missing = [fragment for fragment in required_fragments if fragment.lower() not in lowered]
    if missing:
        return PageFailure(name=name, reason=f"missing_fragments={missing}")
    return None


def build_checks(*, tenant: int, public_key: str) -> list[PageCheck]:
    query = urllib.parse.urlencode({"tenant": int(tenant), "k": public_key})
    settings_query = urllib.parse.urlencode({"k": public_key})
    return [
        PageCheck(
            path="/login",
            name="login_page",
            required_fragments=("email", "password"),
        ),
        PageCheck(
            path="/register",
            name="register_page",
            required_fragments=("email", "password"),
        ),
        PageCheck(
            path=f"/client/{int(tenant)}/settings?{settings_query}",
            name="client_settings_page",
            required_fragments=("client-settings-state", "/static/spa/client", f"tenant {int(tenant)}"),
        ),
        PageCheck(
            path=f"/connect/avito?{query}",
            name="avito_connect_page",
            required_fragments=("authorize", "/v1/oauth/avito/authorize"),
        ),
    ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="HTTP smoke for critical Avio UI pages.")
    parser.add_argument("--base-url", default=os.getenv("SMOKE_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--tenant", type=int, default=int(os.getenv("SMOKE_WRITE_TENANT", "999999")))
    parser.add_argument("--public-key", default=os.getenv("SMOKE_PUBLIC_KEY") or os.getenv("PUBLIC_KEY") or "")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    public_key = str(args.public_key or "").strip()
    if not public_key:
        raise SystemExit("PUBLIC_KEY/--public-key is required")

    base = str(args.base_url).rstrip("/")
    failures: list[PageFailure] = []
    for check in build_checks(tenant=int(args.tenant), public_key=public_key):
        status, html = _request_text(f"{base}{check.path}")
        failure = evaluate_page(check.name, status, html, check.required_fragments)
        if failure is not None:
            failures.append(failure)

    if failures:
        print("ui http smoke failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure.name}: {failure.reason}", file=sys.stderr)
        raise SystemExit(1)

    print("ui http smoke ok")


if __name__ == "__main__":
    main()
