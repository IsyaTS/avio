import pytest

from libs.core.services.avito_oauth_state import (
    resolve_tenant_from_state,
    tenant_from_redis_state,
)


pytestmark = pytest.mark.unit


def test_tenant_from_redis_state_accepts_str_and_bytes():
    assert tenant_from_redis_state('{"tenant": 3}') == 3
    assert tenant_from_redis_state(b'{"tenant": "4"}') == 4


def test_tenant_from_redis_state_rejects_invalid_payloads():
    assert tenant_from_redis_state(None) is None
    assert tenant_from_redis_state("not-json") is None
    assert tenant_from_redis_state('{"tenant": 0}') is None
    assert tenant_from_redis_state('{"tenant": "bad"}') is None


def test_resolve_tenant_from_state_prefers_redis_payload():
    calls = []

    def verify(state):
        calls.append(state)
        return {"tenant": 9}

    tenant = resolve_tenant_from_state(
        raw_value='{"tenant": 3}',
        state="signed-state",
        verify_signed_state=verify,
    )

    assert tenant == 3
    assert calls == []


def test_resolve_tenant_from_state_falls_back_to_signed_state():
    tenant = resolve_tenant_from_state(
        raw_value=None,
        state="signed-state",
        verify_signed_state=lambda state: {"tenant": "5"},
    )

    assert tenant == 5


def test_resolve_tenant_from_state_rejects_bad_signed_state():
    tenant = resolve_tenant_from_state(
        raw_value=None,
        state="signed-state",
        verify_signed_state=lambda state: None,
    )

    assert tenant is None
