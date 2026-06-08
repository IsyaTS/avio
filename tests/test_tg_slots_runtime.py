from __future__ import annotations

import pytest

from apps.api.web.services import tg_slots_runtime


pytestmark = pytest.mark.unit


def _deps() -> tg_slots_runtime.TgSlotsDeps:
    return tg_slots_runtime.TgSlotsDeps(
        slot_min=1,
        slot_max=3,
        normalize_slot_fn=lambda value: 1 if value in (None, "") else max(1, min(3, int(value))),
    )


def test_tg_slots_config_uses_defaults_when_missing() -> None:
    result = tg_slots_runtime.tg_slots_config({}, _deps())

    assert result == {
        "multi_mode": True,
        "slot_enabled": {"1": True, "2": True, "3": True},
        "slot_count": 1,
        "slot_min": 1,
        "slot_max": 3,
    }


def test_tg_slots_config_normalizes_existing_flags() -> None:
    result = tg_slots_runtime.tg_slots_config(
        {
            "telegram": {
                "multi_slot_enabled": "false",
                "slot_count": "3",
                "slot_enabled": {"1": "true", 2: False, "3": "0"},
            }
        },
        _deps(),
    )

    assert result["multi_mode"] is False
    assert result["slot_count"] == 3
    assert result["slot_enabled"] == {"1": True, "2": False, "3": False}


def test_apply_tg_slots_payload_updates_config_in_place() -> None:
    cfg = {"telegram": {"existing": "keep"}}

    result = tg_slots_runtime.apply_tg_slots_payload(
        cfg,
        {
            "multi_mode": "yes",
            "slot_count": "2",
            "slot_enabled": {"1": "on", "2": "off", "3": True},
        },
        _deps(),
    )

    assert result is cfg
    assert cfg["telegram"]["existing"] == "keep"
    assert cfg["telegram"]["multi_slot_enabled"] is True
    assert cfg["telegram"]["slot_count"] == 2
    assert cfg["telegram"]["slot_enabled"] == {"1": True, "2": False, "3": True}
