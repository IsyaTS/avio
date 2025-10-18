"""Smoke test for provider webhook and Redis inbox."""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import httpx
import redis

APP_URL = os.getenv("APP_URL", "http://localhost:8000").rstrip("/")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
INBOX_KEY = "inbox:message_in"
TENANT = int(os.getenv("TENANT_ID", "1"))
WEBHOOK_TOKEN = os.getenv("WEBHOOK_SECRET", "")


def _redis_client() -> redis.Redis:
    return redis.from_url(REDIS_URL, decode_responses=True)


async def main() -> None:
    sample = {
        "tenant": TENANT,
        "channel": "telegram",
        "from_id": "smoke", 
        "to": "tester",
        "text": "hello from smoke",
        "attachments": [],
        "ts": int(time.time()),
        "provider_raw": {"smoke": True},
    }
    client = _redis_client()
    before = client.llen(INBOX_KEY)
    async with httpx.AsyncClient(timeout=5.0) as http:
        url = f"{APP_URL}/webhook/telegram"
        if WEBHOOK_TOKEN:
            url = f"{url}?token={WEBHOOK_TOKEN}"
        response = await http.post(url, json=sample)
        try:
            body: Any = response.json()
        except Exception:
            body = response.text
        print(json.dumps({"status": response.status_code, "body": body}, ensure_ascii=False))
    after = client.llen(INBOX_KEY)
    print(json.dumps({"redis_before": before, "redis_after": after}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
