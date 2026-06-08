#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

try:
    from smoke_lock import smoke_tenant_lock
except ModuleNotFoundError:  # pragma: no cover - import path used by unit tests
    from scripts.smoke_lock import smoke_tenant_lock


def _request_json(
    url: str,
    *,
    token: str = "",
    timeout: float = 5.0,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    headers = {"Accept": "application/json"}
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    if token:
        headers["X-Admin-Token"] = token
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
            return int(response.status), json.loads(body or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(body or "{}")
        except Exception:
            payload = {"raw": body}
        return int(exc.code), payload
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return 0, {"error": type(exc).__name__, "detail": str(exc)}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Critical HTTP smoke checks for Avio app.")
    parser.add_argument("--base-url", default=os.getenv("SMOKE_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--admin-token", default=os.getenv("ADMIN_TOKEN", ""))
    parser.add_argument("--tenants", default=os.getenv("SMOKE_TENANTS", "1,3"))
    parser.add_argument(
        "--mode",
        choices=("readonly", "test-tenant-write"),
        default=os.getenv("SMOKE_MODE", "readonly"),
        help="readonly is safe for prod; test-tenant-write mutates only --write-tenant",
    )
    parser.add_argument(
        "--write-tenant",
        type=int,
        default=int(os.getenv("SMOKE_WRITE_TENANT", "999999")),
        help="tenant used only by --mode test-tenant-write",
    )
    parser.add_argument(
        "--public-key",
        default=os.getenv("SMOKE_PUBLIC_KEY") or os.getenv("PUBLIC_KEY") or "",
        help="public/client key for test-tenant-write checks",
    )
    parser.add_argument("--health-timeout", type=int, default=int(os.getenv("SMOKE_HEALTH_TIMEOUT", "45")))
    return parser.parse_args()


def _wait_health(base: str, *, timeout_seconds: int, failures: list[str]) -> bool:
    deadline = time.time() + max(1, int(timeout_seconds))
    last_status = 0
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        status, payload = _request_json(f"{base}/health")
        last_status = status
        last_payload = payload
        if status == 200 and payload.get("ok"):
            return True
        time.sleep(1.5)
    failures.append(f"health status={last_status} payload={last_payload}")
    return False


def _check_test_tenant_settings(base: str, tenant: int, public_key: str, failures: list[str]) -> None:
    if not public_key:
        failures.append("test_tenant_settings skipped: public key is not set")
        return

    marker = f"critical-smoke-{int(time.time())}"
    query = urllib.parse.urlencode({"tenant": int(tenant), "k": public_key})
    save_payload = {
        "cfg": {
            "passport": {
                "tenant_id": int(tenant),
                "public_key": public_key,
                "brand": marker,
            },
            "behavior": {
                "avito_smart_reply_enabled": True,
                "brain_mode": "smoke",
            },
            "integrations": {
                "avito": {
                    "access_token": "smoke-access",
                    "refresh_token": "smoke-refresh",
                    "account_id": 999999,
                }
            },
            "follow_up": [{"text": "smoke follow-up", "delay_minutes": 1}],
        }
    }
    status, payload = _request_json(
        f"{base}/pub/settings/save?{query}",
        method="POST",
        payload=save_payload,
        timeout=8.0,
    )
    if status != 200 or payload.get("ok") is not True:
        failures.append(f"test_tenant_settings save status={status} payload={payload}")
        return

    status, payload = _request_json(f"{base}/pub/settings/get?{query}", timeout=8.0)
    cfg = payload.get("cfg") if isinstance(payload, dict) else None
    behavior = cfg.get("behavior") if isinstance(cfg, dict) else None
    integrations = cfg.get("integrations") if isinstance(cfg, dict) else None
    avito = integrations.get("avito") if isinstance(integrations, dict) else None
    follow_up = cfg.get("follow_up") if isinstance(cfg, dict) else None
    passport = cfg.get("passport") if isinstance(cfg, dict) else None
    if status != 200 or payload.get("ok") is not True or not isinstance(cfg, dict):
        failures.append(f"test_tenant_settings get status={status} payload={payload}")
        return
    if not isinstance(passport, dict) or passport.get("brand") != marker:
        failures.append(f"test_tenant_settings passport_not_persisted payload={payload}")
    if not isinstance(behavior, dict) or behavior.get("avito_smart_reply_enabled") is not True:
        failures.append(f"test_tenant_settings behavior_not_persisted payload={payload}")
    if not isinstance(avito, dict) or avito.get("access_token") != "smoke-access":
        failures.append(f"test_tenant_settings avito_not_persisted payload={payload}")
    if follow_up != [{"text": "smoke follow-up", "delay_minutes": 1}]:
        failures.append(f"test_tenant_settings follow_up_not_persisted payload={payload}")


def main() -> None:
    args = _parse_args()
    base = str(args.base_url).rstrip("/")
    failures: list[str] = []

    health_ok = _wait_health(base, timeout_seconds=int(args.health_timeout), failures=failures)

    if health_ok and args.admin_token:
        query = urllib.parse.urlencode({"tenants": args.tenants})
        status, payload = _request_json(
            f"{base}/internal/health/deep?{query}",
            token=str(args.admin_token),
            timeout=8.0,
        )
        if status not in {200, 503}:
            failures.append(f"deep_health unexpected_status={status} payload={payload}")
        elif not isinstance(payload, dict) or "db" not in payload or "redis" not in payload:
            failures.append(f"deep_health malformed payload={payload}")
        elif status != 200:
            failures.append(f"deep_health degraded payload={payload}")
    else:
        if not args.admin_token:
            failures.append("deep_health skipped: ADMIN_TOKEN is not set")

    if args.mode == "test-tenant-write":
        with smoke_tenant_lock("tenant-write", int(args.write_tenant)):
            _check_test_tenant_settings(
                base,
                int(args.write_tenant),
                str(args.public_key or "").strip(),
                failures,
            )

    if failures:
        print("critical smoke failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        raise SystemExit(1)

    print("critical smoke ok")


if __name__ == "__main__":
    main()
