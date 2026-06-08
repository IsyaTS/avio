from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from apps.api.web.services import client_dialog_helpers_runtime as helpers


pytestmark = pytest.mark.unit


class _Redis:
    def __init__(self, values: dict[str, str], ttls: dict[str, int] | None = None):
        self.values = dict(values)
        self.ttls = dict(ttls or {})

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def ttl(self, key: str) -> int:
        return self.ttls.get(key, -1)


class _Common:
    def __init__(self):
        self.redis = _Redis(
            {
                "silence:7:15": "1710000000",
                "silence:meta:7:15": json.dumps({"reason": "operator_handoff"}),
            },
            {"silence:7:15": 120},
        )

    def read_tenant_config(self, tenant_id: int) -> dict:
        assert tenant_id == 7
        return {"behavior": {"telegram_reply_enabled": False}}

    def redis_client(self) -> _Redis:
        return self.redis

    def public_url(self, _request, path: str) -> str:
        return f"https://avio.test{path}"


def test_parse_tg_slot_from_source_respects_range() -> None:
    assert helpers.parse_tg_slot_from_source("source tg_slot=2", slot_min=1, slot_max=5) == 2
    assert helpers.parse_tg_slot_from_source("source tg_slot=9", slot_min=1, slot_max=5) is None


def test_is_technical_max_title_detects_numeric_and_internal_titles() -> None:
    assert helpers.is_technical_max_title("12345") is True
    assert helpers.is_technical_max_title("max: id 12345") is True
    assert helpers.is_technical_max_title("Иван Петров") is False


def test_load_silence_status_uses_config_and_redis_metadata() -> None:
    status = helpers.load_silence_status(
        7,
        15,
        "telegram",
        common_module=_Common(),
        silence_key_fn=lambda tenant, lead: f"silence:{tenant}:{lead}",
        silence_meta_key_fn=lambda tenant, lead: f"silence:meta:{tenant}:{lead}",
    )

    assert status["active"] is True
    assert status["auto_reply_enabled"] is False
    assert status["ttl_seconds"] == 120
    assert status["reason"] == "operator_handoff"
    assert status["since"].startswith("2024-03-09T16:00:00")


def test_normalize_message_attachments_rewrites_internal_and_photo_urls() -> None:
    request = SimpleNamespace()

    out = helpers.normalize_message_attachments(
        request,
        7,
        "pub-key",
        [
            {"photo_id": "p1"},
            {"url": "http://app:8000/pub/tg/media/1/2?tenant=7"},
            {"url": "telegram://peer/100/200"},
        ],
        common_module=_Common(),
    )

    assert out[0]["url"] == "https://avio.test/pub/files/photos/p1?tenant=7&k=pub-key"
    assert out[1]["url"] == "https://avio.test/pub/tg/media/1/2?tenant=7"
    assert out[2]["url"] == "https://avio.test/pub/tg/media/100/200?tenant=7&k=pub-key"
