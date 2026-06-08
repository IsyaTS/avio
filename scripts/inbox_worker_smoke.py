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
from typing import Any, Mapping

from smoke_lock import smoke_tenant_lock


INBOX_KEY = "inbox:message_in"
OUTBOX_KEY = "outbox:send"
OUTBOX_DLQ_KEY = "outbox:dlq"


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


def _compose_command(args: argparse.Namespace, *parts: str) -> list[str]:
    command = ["docker", "compose"]
    for compose_file in args.compose_file:
        command.extend(["-f", str(compose_file)])
    command.extend(parts)
    return command


def _redis_cli(args: argparse.Namespace, *parts: str) -> str:
    command = _compose_command(args, "exec", "-T", "redis", "redis-cli", "--raw", *parts)
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    return result.stdout


def _parse_json_lines(raw: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            payload["_raw_queue_value"] = line
            items.append(payload)
    return items


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


def _wait_for_outbox(
    args: argparse.Namespace,
    *,
    tenant: int,
    chat_id: str,
    expected_text: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    deadline = time.time() + max(1, int(timeout_seconds))
    last_outbox: list[dict[str, Any]] = []
    while time.time() < deadline:
        raw = _redis_cli(args, "LRANGE", OUTBOX_KEY, "0", "-1")
        items = _parse_json_lines(raw)
        last_outbox = items
        for item in items:
            if (
                int(item.get("tenant") or item.get("tenant_id") or 0) == int(tenant)
                and str(item.get("channel") or item.get("ch") or item.get("provider") or "") == "avito"
                and str(item.get("chat_id") or "") == chat_id
                and str(item.get("text") or "") == expected_text
            ):
                return item
        time.sleep(1.0)
    dlq_raw = _redis_cli(args, "LRANGE", OUTBOX_DLQ_KEY, "0", "-1")
    raise RuntimeError(
        "outbox payload was not produced "
        f"tenant={tenant} chat_id={chat_id} last_outbox={last_outbox!r} dlq={dlq_raw!r}"
    )


def _is_expected_outbox_item(
    item: Mapping[str, Any],
    *,
    tenant: int,
    chat_id: str,
    expected_text: str,
) -> bool:
    return (
        int(item.get("tenant") or item.get("tenant_id") or 0) == int(tenant)
        and str(item.get("channel") or item.get("ch") or item.get("provider") or "") == "avito"
        and str(item.get("chat_id") or "") == chat_id
        and str(item.get("text") or "") == expected_text
    )


def _purge_expected_outbox_items(
    args: argparse.Namespace,
    *,
    tenant: int,
    chat_id: str,
    expected_text: str,
    settle_seconds: float = 10.0,
    quiet_seconds: float = 2.0,
) -> int:
    deadline = time.time() + max(0.2, float(settle_seconds))
    quiet_since: float | None = None
    removed = 0
    while time.time() < deadline:
        raw = _redis_cli(args, "LRANGE", OUTBOX_KEY, "0", "-1")
        items = _parse_json_lines(raw)
        matched = False
        for item in items:
            raw_queue_value = str(item.get("_raw_queue_value") or "")
            if raw_queue_value and _is_expected_outbox_item(
                item,
                tenant=tenant,
                chat_id=chat_id,
                expected_text=expected_text,
            ):
                matched = True
                try:
                    removed += int(_redis_cli(args, "LREM", OUTBOX_KEY, "0", raw_queue_value) or 0)
                except Exception:
                    pass
        if not matched:
            if quiet_since is None:
                quiet_since = time.time()
            if time.time() - quiet_since >= max(0.2, float(quiet_seconds)):
                break
            time.sleep(0.2)
        else:
            quiet_since = None
            time.sleep(0.5)
    return removed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Push an Avito incoming event into Redis inbox and verify the live worker creates "
            "an Avito outbox payload. Run with worker OUTBOX_ENABLED=0 so the payload is not consumed."
        )
    )
    parser.add_argument("--base-url", default=os.getenv("SMOKE_BASE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--tenant", type=int, default=int(os.getenv("SMOKE_WRITE_TENANT", "999999")))
    parser.add_argument("--public-key", default=os.getenv("SMOKE_PUBLIC_KEY") or os.getenv("PUBLIC_KEY") or "")
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--health-timeout", type=int, default=90)
    parser.add_argument(
        "--compose-file",
        action="append",
        default=[],
        help="docker compose file; can be passed multiple times",
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
        _wait_health(base, timeout_seconds=int(args.health_timeout))

        marker = str(int(time.time()))
        chat_id = f"smoke-avito-chat-{marker}"
        incoming_text = f"Smoke Avito inbound {marker}"
        reply_text = f"Smoke Avito auto reply {marker}"
        query = urllib.parse.urlencode({"tenant": tenant, "k": public_key})
        settings_payload = {
            "cfg": {
                "passport": {"tenant_id": tenant, "public_key": public_key},
                "behavior": {
                    "auto_reply": True,
                    "auto_reply_text": reply_text,
                    "avito_smart_reply_enabled": False,
                },
                "integrations": {
                    "avito": {
                        "access_token": "smoke-access",
                        "refresh_token": "smoke-refresh",
                        "account_id": 999997,
                    }
                },
            }
        }
        status, payload = _request_json(
            f"{base}/pub/settings/save?{query}",
            method="POST",
            payload=settings_payload,
        )
        if status != 200 or payload.get("ok") is not True:
            raise RuntimeError(f"settings save failed status={status} payload={payload}")

        _redis_cli(args, "DEL", INBOX_KEY, OUTBOX_KEY, OUTBOX_DLQ_KEY)
        event = {
            "provider": "avito",
            "channel": "avito",
            "tenant": tenant,
            "chat_id": chat_id,
            "message_id": f"smoke-msg-{marker}",
            "text": incoming_text,
            "account_id": 999997,
            "avito_user_id": 999996,
            "avito_login": "smoke-buyer",
        }
        _redis_cli(args, "LPUSH", INBOX_KEY, json.dumps(event, ensure_ascii=False))
        item = _wait_for_outbox(
            args,
            tenant=tenant,
            chat_id=chat_id,
            expected_text=reply_text,
            timeout_seconds=int(args.timeout),
        )
        removed = _purge_expected_outbox_items(
            args,
            tenant=tenant,
            chat_id=chat_id,
            expected_text=reply_text,
        )
        print(
            "inbox worker smoke ok "
            f"tenant={tenant} chat_id={chat_id} lead_id={item.get('lead_id')} "
            f"text={item.get('text')!r} cleaned={removed}"
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"inbox worker smoke failed: {exc}", file=sys.stderr)
        raise
