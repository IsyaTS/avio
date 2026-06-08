#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from smoke_lock import smoke_tenant_lock


def _request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 8.0,
) -> tuple[int, dict[str, Any]]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            return int(response.status), json.loads(raw or "{}")
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw or "{}")
        except Exception:
            body = {"raw": raw}
        return int(exc.code), body


def _wait_health(base_url: str, *, timeout_seconds: int) -> None:
    deadline = time.time() + max(1, int(timeout_seconds))
    last_error = ""
    while time.time() < deadline:
        try:
            status, payload = _request_json(f"{base_url}/health", timeout=4.0)
            if status == 200 and payload.get("ok") is True:
                return
            last_error = f"status={status} payload={payload}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(f"health did not recover: {last_error}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Persist critical tenant settings, restart services, and verify they survived."
    )
    parser.add_argument("--base-url", default=os.getenv("SMOKE_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--tenant", type=int, default=int(os.getenv("SMOKE_WRITE_TENANT", "999999")))
    parser.add_argument("--public-key", default=os.getenv("SMOKE_PUBLIC_KEY") or os.getenv("PUBLIC_KEY") or "")
    parser.add_argument(
        "--services",
        default=os.getenv("SMOKE_RESTART_SERVICES", "app,worker"),
        help="comma-separated docker compose services to restart",
    )
    parser.add_argument(
        "--compose-file",
        action="append",
        default=[],
        help="docker compose file; can be passed multiple times",
    )
    parser.add_argument("--health-timeout", type=int, default=90)
    parser.add_argument(
        "--skip-restart",
        action="store_true",
        help="only run save/read assertions without restarting containers",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    base = str(args.base_url).rstrip("/")
    tenant = int(args.tenant)
    public_key = str(args.public_key or "").strip()
    if not public_key:
        raise SystemExit("PUBLIC_KEY/--public-key is required")
    with smoke_tenant_lock("tenant-write", tenant):
        _run(args, base=base, tenant=tenant, public_key=public_key)


def _run(args: argparse.Namespace, *, base: str, tenant: int, public_key: str) -> None:

    marker = f"restart-smoke-{int(time.time())}"
    query = urllib.parse.urlencode({"tenant": tenant, "k": public_key})
    expected_follow_up = [{"text": "restart smoke follow-up", "delay_minutes": 2}]
    expected_avito = {
        "access_token": "restart-smoke-access",
        "refresh_token": "restart-smoke-refresh",
        "account_id": 999998,
    }
    save_payload = {
        "cfg": {
            "passport": {"tenant_id": tenant, "public_key": public_key, "brand": marker},
            "behavior": {
                "avito_smart_reply_enabled": True,
                "brain_mode": "restart-smoke",
            },
            "integrations": {"avito": expected_avito},
            "follow_up": expected_follow_up,
        }
    }

    status, payload = _request_json(
        f"{base}/pub/settings/save?{query}",
        method="POST",
        payload=save_payload,
    )
    if status != 200 or payload.get("ok") is not True:
        raise RuntimeError(f"settings save failed status={status} payload={payload}")

    if not args.skip_restart:
        services = [item.strip() for item in str(args.services or "").split(",") if item.strip()]
        if services:
            command = ["docker", "compose"]
            for compose_file in args.compose_file:
                command.extend(["-f", str(compose_file)])
            command.extend(["restart", *services])
            subprocess.run(command, check=True)
        _wait_health(base, timeout_seconds=int(args.health_timeout))

    status, payload = _request_json(f"{base}/pub/settings/get?{query}")
    cfg = payload.get("cfg") if isinstance(payload, dict) else None
    if status != 200 or payload.get("ok") is not True or not isinstance(cfg, dict):
        raise RuntimeError(f"settings get failed status={status} payload={payload}")

    passport = cfg.get("passport") if isinstance(cfg.get("passport"), dict) else {}
    behavior = cfg.get("behavior") if isinstance(cfg.get("behavior"), dict) else {}
    integrations = cfg.get("integrations") if isinstance(cfg.get("integrations"), dict) else {}
    avito = integrations.get("avito") if isinstance(integrations.get("avito"), dict) else {}

    failures: list[str] = []
    if passport.get("brand") != marker:
        failures.append(f"passport.brand expected={marker!r} actual={passport.get('brand')!r}")
    if behavior.get("avito_smart_reply_enabled") is not True:
        failures.append("behavior.avito_smart_reply_enabled is not true")
    if behavior.get("brain_mode") != "restart-smoke":
        failures.append(f"behavior.brain_mode actual={behavior.get('brain_mode')!r}")
    for key, expected in expected_avito.items():
        if avito.get(key) != expected:
            failures.append(f"integrations.avito.{key} expected={expected!r} actual={avito.get(key)!r}")
    if cfg.get("follow_up") != expected_follow_up:
        failures.append(f"follow_up expected={expected_follow_up!r} actual={cfg.get('follow_up')!r}")

    if failures:
        print("restart persistence smoke failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        raise SystemExit(1)
    print("restart persistence smoke ok")


if __name__ == "__main__":
    main()
