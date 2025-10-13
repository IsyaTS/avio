"""Simple smoke test for unified /send contract."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import httpx

APP_URL = os.getenv("APP_URL", "http://localhost:8000").rstrip("/")
TENANT = int(os.getenv("TENANT_ID", "1"))


async def _send(client: httpx.AsyncClient, payload: dict[str, Any]) -> None:
    url = f"{APP_URL}/send"
    response = await client.post(url, json=payload)
    body: Any
    try:
        body = response.json()
    except Exception:
        body = response.text
    print(json.dumps({"channel": payload.get("channel"), "status": response.status_code, "body": body}, ensure_ascii=False))


async def main() -> None:
    messages = [
        {
            "tenant": TENANT,
            "channel": "telegram",
            "to": "me",
            "text": "smoke test telegram",
            "attachments": [],
        },
        {
            "tenant": TENANT,
            "channel": "whatsapp",
            "to": "0000000000",
            "text": "smoke test whatsapp",
            "attachments": [],
        },
    ]
    async with httpx.AsyncClient(timeout=5.0) as client:
        for payload in messages:
            await _send(client, payload)


if __name__ == "__main__":
    asyncio.run(main())
