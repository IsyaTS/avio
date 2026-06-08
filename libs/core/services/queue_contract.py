from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class QueuePayloadParseResult:
    payload: dict[str, Any] | None
    error: str | None = None
    preview: str = ""

    @property
    def ok(self) -> bool:
        return self.payload is not None and self.error is None


def dumps_queue_payload(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload), ensure_ascii=False)


def preview_queue_payload(raw_payload: Any, limit: int = 160) -> str:
    if isinstance(raw_payload, (bytes, bytearray)):
        try:
            raw_payload = raw_payload.decode("utf-8", errors="replace")
        except Exception:
            raw_payload = repr(raw_payload)
    return str(raw_payload)[: max(0, int(limit))]


def parse_queue_payload(raw_payload: Any) -> QueuePayloadParseResult:
    if isinstance(raw_payload, Mapping):
        return QueuePayloadParseResult(payload=dict(raw_payload))
    try:
        data = json.loads(raw_payload)
    except (TypeError, json.JSONDecodeError):
        return QueuePayloadParseResult(
            payload=None,
            error="json_decode",
            preview=preview_queue_payload(raw_payload),
        )
    if not isinstance(data, dict):
        return QueuePayloadParseResult(
            payload=None,
            error="invalid_payload",
            preview=preview_queue_payload(raw_payload),
        )
    return QueuePayloadParseResult(payload=dict(data))


def is_status_echo_payload(item: Mapping[str, Any]) -> bool:
    """Return True for internal status echoes emitted after send_result.

    Real outbound jobs carry text or attachments. Status echoes carry a reply
    preview plus version/status metadata and must not be sent again.
    """

    status = item.get("status")
    if not status:
        return False
    if item.get("text") or item.get("attachment") or item.get("attachments"):
        return False
    reply = item.get("reply")
    version = item.get("version")
    return isinstance(reply, str) and bool(version)


async def push_json_left(redis_conn: Any, queue_key: str, payload: Mapping[str, Any]) -> Any:
    return await redis_conn.lpush(queue_key, dumps_queue_payload(payload))


async def push_json_right(redis_conn: Any, queue_key: str, payload: Mapping[str, Any]) -> Any:
    return await redis_conn.rpush(queue_key, dumps_queue_payload(payload))


__all__ = [
    "QueuePayloadParseResult",
    "dumps_queue_payload",
    "is_status_echo_payload",
    "parse_queue_payload",
    "preview_queue_payload",
    "push_json_left",
    "push_json_right",
]
