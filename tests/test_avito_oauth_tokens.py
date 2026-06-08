import pytest

from libs.core.services.avito_oauth_tokens import (
    AvitoTokenPayloadError,
    build_token_update_payload,
)


pytestmark = pytest.mark.unit


def test_build_token_update_payload_computes_expiry_and_resets_stale_account():
    payload = build_token_update_payload(
        {
            "access_token": " access-new ",
            "refresh_token": " refresh-new ",
            "expires_in": "3600",
            "scope": " messenger ",
        },
        now=1000,
    )

    assert payload == {
        "access_token": "access-new",
        "refresh_token": "refresh-new",
        "obtained_at": 1000,
        "account_id": None,
        "account_login": None,
        "expires_at": 4600,
        "scope": "messenger",
    }


def test_build_token_update_payload_accepts_absolute_expiry():
    payload = build_token_update_payload(
        {"access_token": "access-new", "expires_at": "5000"},
        now=1000,
    )

    assert payload["expires_at"] == 5000
    assert payload["refresh_token"] is None


def test_build_token_update_payload_rejects_missing_access_token():
    with pytest.raises(AvitoTokenPayloadError):
        build_token_update_payload({"refresh_token": "refresh-new"}, now=1000)
