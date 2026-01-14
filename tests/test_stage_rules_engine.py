from __future__ import annotations

import sys
import types

if "httpx" not in sys.modules:
    sys.modules["httpx"] = types.SimpleNamespace(AsyncClient=object, HTTPError=Exception, TimeoutException=Exception)

from libs.core.integrations.amocrm import decide_next_stage


def test_default_stage_progression_by_count():
    stages = [
        {"name": "s0", "amo_stage_id": 1, "rule": {"type": "on_first_inbound", "params": {}}},
        {"name": "s1", "amo_stage_id": 2, "rule": {}},
        {"name": "s2", "amo_stage_id": 3, "rule": {}},
    ]
    assert decide_next_stage(stages, 0, 1, "", {}) is None
    assert decide_next_stage(stages, 0, 2, "", {}) == 1
    assert decide_next_stage(stages, 1, 4, "", {}) == 2


def test_keyword_rule_moves_stage():
    stages = [
        {"name": "s0", "amo_stage_id": 1, "rule": {"type": "on_first_inbound", "params": {}}},
        {"name": "s1", "amo_stage_id": 2, "rule": {"type": "on_inbound_count", "params": {"min_inbound_messages": 2}}},
        {"name": "s2", "amo_stage_id": 3, "rule": {"type": "on_keyword", "params": {"keywords": ["price"]}}},
    ]
    assert decide_next_stage(stages, 1, 2, "Price is ok", {}) == 2


def test_field_present_rule():
    stages = [
        {"name": "s0", "amo_stage_id": 1, "rule": {"type": "on_first_inbound", "params": {}}},
        {"name": "s1", "amo_stage_id": 2, "rule": {"type": "on_field_present", "params": {"field_key": "phone"}}},
    ]
    assert decide_next_stage(stages, 0, 1, "", {"phone": "+79991234567"}) == 1


def test_manual_only_stops_progression():
    stages = [
        {"name": "s0", "amo_stage_id": 1, "rule": {"type": "manual_only", "params": {}}},
        {"name": "s1", "amo_stage_id": 2, "rule": {"type": "on_inbound_count", "params": {"min_inbound_messages": 2}}},
    ]
    assert decide_next_stage(stages, 0, 3, "ok", {}) is None
