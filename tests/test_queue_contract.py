from __future__ import annotations

import json

import pytest

from libs.core.services import queue_contract


pytestmark = pytest.mark.unit


class _Redis:
    def __init__(self) -> None:
        self.left: list[tuple[str, str]] = []
        self.right: list[tuple[str, str]] = []

    async def lpush(self, key: str, value: str) -> int:
        self.left.append((key, value))
        return len(self.left)

    async def rpush(self, key: str, value: str) -> int:
        self.right.append((key, value))
        return len(self.right)


def test_dumps_queue_payload_preserves_unicode() -> None:
    raw = queue_contract.dumps_queue_payload({"text": "Привет", "tenant": 1})

    assert "\\u041f" not in raw
    assert json.loads(raw) == {"text": "Привет", "tenant": 1}


def test_parse_queue_payload_accepts_dict_and_json() -> None:
    direct = queue_contract.parse_queue_payload({"tenant": 1, "text": "Привет"})
    from_json = queue_contract.parse_queue_payload('{"tenant": 1, "text": "Привет"}')

    assert direct.ok
    assert direct.payload == {"tenant": 1, "text": "Привет"}
    assert from_json.ok
    assert from_json.payload == {"tenant": 1, "text": "Привет"}


def test_parse_queue_payload_reports_bad_json_preview() -> None:
    parsed = queue_contract.parse_queue_payload(b'{"tenant":')

    assert not parsed.ok
    assert parsed.payload is None
    assert parsed.error == "json_decode"
    assert parsed.preview == '{"tenant":'


def test_parse_queue_payload_rejects_non_object_json() -> None:
    parsed = queue_contract.parse_queue_payload('["not", "a", "payload"]')

    assert not parsed.ok
    assert parsed.payload is None
    assert parsed.error == "invalid_payload"


def test_is_status_echo_payload_detects_internal_echo_only() -> None:
    assert (
        queue_contract.is_status_echo_payload(
            {"status": "sent", "reply": "ok", "version": "v1"}
        )
        is True
    )
    assert (
        queue_contract.is_status_echo_payload(
            {"status": "sent", "reply": "ok", "version": "v1", "text": "send me"}
        )
        is False
    )
    assert queue_contract.is_status_echo_payload({"reply": "ok", "version": "v1"}) is False


@pytest.mark.asyncio
async def test_push_json_left_and_right_use_shared_serialization() -> None:
    redis = _Redis()

    assert await queue_contract.push_json_left(redis, "outbox:send", {"text": "Привет"}) == 1
    assert await queue_contract.push_json_right(redis, "outbox:send", {"text": "Пока"}) == 1

    assert redis.left == [("outbox:send", '{"text": "Привет"}')]
    assert redis.right == [("outbox:send", '{"text": "Пока"}')]
