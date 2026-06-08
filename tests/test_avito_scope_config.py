import pytest

from libs.core.sales_core.config_runtime import build_avito_scope_value


pytestmark = pytest.mark.unit


def test_avito_scope_default_is_minimal_messenger_access() -> None:
    assert build_avito_scope_value(None) == "messenger:read,messenger:write"


def test_avito_scope_uses_only_explicit_requested_scopes() -> None:
    assert build_avito_scope_value("messenger:read messenger:write") == "messenger:read,messenger:write"
    assert build_avito_scope_value("messenger:read,messenger:write,user:read") == (
        "messenger:read,messenger:write,user:read"
    )
