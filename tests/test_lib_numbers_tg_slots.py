from libs.core.lib.numbers import coerce_int
from libs.core.lib.tg_slots import (
    TG_SLOT_MAX,
    TG_SLOT_MIN,
    decode_virtual_tenant,
    normalize_tg_slot,
    virtual_tenant_id,
)


def test_coerce_int_parses_and_bounds() -> None:
    assert coerce_int(" 42 ") == 42
    assert coerce_int("bad") is None
    assert coerce_int(None) is None
    assert coerce_int("10", min_value=11) is None
    assert coerce_int("10", max_value=9) is None
    assert coerce_int("10", min_value=5, max_value=20) == 10


def test_tg_slot_normalization_and_virtual_tenant_roundtrip() -> None:
    tenant = 101
    assert normalize_tg_slot("x") == TG_SLOT_MIN
    assert normalize_tg_slot(-100) == TG_SLOT_MIN
    assert normalize_tg_slot(10_000) == TG_SLOT_MAX

    for slot in range(TG_SLOT_MIN, TG_SLOT_MAX + 1):
        virtual_tenant = virtual_tenant_id(tenant, slot)
        decoded_tenant, decoded_slot = decode_virtual_tenant(virtual_tenant)
        assert decoded_tenant == tenant
        assert decoded_slot == slot


def test_decode_virtual_tenant_for_plain_or_invalid_ids() -> None:
    assert decode_virtual_tenant(0) == (0, TG_SLOT_MIN)
    assert decode_virtual_tenant(101) == (101, TG_SLOT_MIN)
