import pytest

from apps.worker import followups


class _FakePipeline:
    def __init__(self):
        self.hsets = []
        self.zadds = []
        self.sets = []

    def hset(self, key, mapping):
        self.hsets.append((key, dict(mapping)))
        return self

    def zadd(self, key, mapping):
        self.zadds.append((key, dict(mapping)))
        return self

    def set(self, key, value, ex=None):
        self.sets.append((key, value, ex))
        return self

    async def execute(self):
        return []


class _FakeRedis:
    def __init__(self):
        self.pipeline_obj = _FakePipeline()

    def pipeline(self):
        return self.pipeline_obj

    async def get(self, _key):
        return None


def test_max_followup_rule_matches_max_personal_without_switching_transport():
    assert followups._channel_matches_rule("max", "max_personal") is True
    assert followups._channel_matches_rule("max_personal", "max") is True
    assert followups._job_channel_for_rule("max", "max_personal") == "max_personal"
    assert followups._job_channel_for_rule("max_personal", "max") == "max"


def test_followup_channel_aliases_are_normalized():
    assert followups._normalize_channel("MAX Personal") == "max_personal"
    assert followups._normalize_channel("max-personal") == "max_personal"
    assert followups._channel_matches_rule("any", "max_personal") is True
    assert followups._channel_matches_rule("telegram", "max_personal") is False


@pytest.mark.asyncio
async def test_schedule_followups_applies_max_rule_to_max_personal(monkeypatch):
    fake_redis = _FakeRedis()
    monkeypatch.setattr(followups, "r", fake_redis)
    monkeypatch.setattr(followups, "is_opted_out", lambda *_args: _return_async(False))
    monkeypatch.setattr(
        followups,
        "_load_rules",
        lambda _tenant_id: [
            {
                "channel": "any",
                "delay_minutes": 1,
                "text": "first",
                "max_attempts": 1,
            },
            {
                "channel": "max",
                "delay_minutes": 2,
                "text": "second",
                "max_attempts": 1,
            },
        ],
    )

    await followups.schedule_followups(101, 93267442, "max_personal")

    jobs = [mapping for _key, mapping in fake_redis.pipeline_obj.hsets]
    assert [job["text"] for job in jobs] == ["first", "second"]
    assert [job["channel"] for job in jobs] == ["max_personal", "max_personal"]


@pytest.mark.asyncio
async def test_condition_allows_eq_no(monkeypatch):
    async def fake_get_fact(tenant_id, lead_id, key):
        return "no"

    monkeypatch.setattr(followups, "_get_fact", fake_get_fact)

    job = {
        "tenant_id": 1,
        "lead_id": 2,
        "condition": {"key": "order_done", "op": "eq", "value": "no"},
    }
    assert await followups._condition_allows(job) is True

    job["condition"]["value"] = "yes"
    assert await followups._condition_allows(job) is False


@pytest.mark.asyncio
async def test_condition_allows_not_exists(monkeypatch):
    async def fake_get_fact(tenant_id, lead_id, key):
        return None

    monkeypatch.setattr(followups, "_get_fact", fake_get_fact)

    job = {
        "tenant_id": 1,
        "lead_id": 2,
        "condition": {"key": "order_done", "op": "not_exists"},
    }
    assert await followups._condition_allows(job) is True


async def _return_async(value):
    return value
