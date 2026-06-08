import pytest

from libs.core.services.tenant_config_merge import (
    build_public_settings_get_config,
    build_public_settings_save_config,
    merge_passport_settings_form,
    merge_tenant_config_for_settings,
)


pytestmark = pytest.mark.unit


def test_passport_settings_form_normalizes_non_mapping_passport():
    saved = merge_passport_settings_form(
        {"passport": [], "behavior": None, "cta": "oops"},
        {"brand": "Brand", "agent": "Agent", "city": "City"},
    )

    assert saved["passport"] == {
        "brand": "Brand",
        "agent_name": "Agent",
        "currency": "₽",
    }
    assert saved["behavior"] is None
    assert saved["cta"] == "oops"


def test_passport_settings_form_preserves_existing_values_when_absent():
    saved = merge_passport_settings_form(
        {"passport": {"brand": "Old", "agent_name": "Max", "city": "SPB"}},
        {},
    )

    assert saved["passport"]["brand"] == "Old"
    assert saved["passport"]["agent_name"] == "Max"
    assert saved["passport"]["city"] == "SPB"
    assert saved["passport"]["currency"] == "₽"


def test_settings_merge_preserves_unrelated_sections_and_avito_auth():
    existing = {
        "passport": {"brand": "Old", "public_key": "token"},
        "behavior": {"avito_smart_reply_enabled": True, "max_reply_enabled": True},
        "integrations": {
            "avito": {
                "access_token": "access-old",
                "refresh_token": "refresh-old",
                "expires_at": 123,
                "obtained_at": 100,
                "account_id": 555,
                "account_login": "old-account",
            },
            "max_personal": {"enabled": True},
        },
        "follow_up": [{"text": "ping", "delay_minutes": 10}],
    }

    saved = merge_tenant_config_for_settings(
        existing,
        {"behavior": {"brain_mode": "smart"}, "integrations": {"avito": {}}},
    )

    assert saved["behavior"]["brain_mode"] == "smart"
    assert saved["behavior"]["avito_smart_reply_enabled"] is True
    assert saved["integrations"]["max_personal"] == {"enabled": True}
    assert saved["integrations"]["avito"]["access_token"] == "access-old"
    assert saved["integrations"]["avito"]["refresh_token"] == "refresh-old"
    assert saved["integrations"]["avito"]["account_id"] == 555
    assert saved["follow_up"] == existing["follow_up"]


def test_settings_merge_ignores_invalid_mapping_replacements():
    existing = {
        "behavior": {"avito_smart_reply_enabled": True},
        "integrations": {"avito": {"access_token": "access-old"}},
    }

    saved = merge_tenant_config_for_settings(
        existing,
        {"behavior": [], "integrations": []},
    )

    assert saved["behavior"] == existing["behavior"]
    assert saved["integrations"]["avito"]["access_token"] == "access-old"


def test_settings_merge_preserves_behavior_toggle_when_partial_payload_has_null():
    existing = {
        "behavior": {
            "avito_smart_reply_enabled": True,
            "telegram_reply_enabled": True,
            "brain_mode": "smart",
        }
    }

    saved = merge_tenant_config_for_settings(
        existing,
        {
            "behavior": {
                "avito_smart_reply_enabled": None,
                "telegram_reply_enabled": "",
                "brain_mode": None,
            }
        },
    )

    assert saved["behavior"]["avito_smart_reply_enabled"] is True
    assert saved["behavior"]["telegram_reply_enabled"] is True
    assert saved["behavior"]["brain_mode"] == "smart"


def test_settings_merge_allows_explicit_behavior_toggle_false():
    existing = {
        "behavior": {
            "avito_smart_reply_enabled": True,
            "telegram_reply_enabled": True,
            "brain_mode": "smart",
        }
    }

    saved = merge_tenant_config_for_settings(
        existing,
        {
            "behavior": {
                "avito_smart_reply_enabled": False,
                "telegram_reply_enabled": False,
                "brain_mode": "classic",
            }
        },
    )

    assert saved["behavior"]["avito_smart_reply_enabled"] is False
    assert saved["behavior"]["telegram_reply_enabled"] is False
    assert saved["behavior"]["brain_mode"] == "classic"


def test_public_settings_save_config_accepts_cfg_payload_and_preserves_auth():
    existing = {
        "behavior": {"avito_smart_reply_enabled": True},
        "integrations": {"avito": {"access_token": "old-token"}},
        "follow_up": [{"text": "old", "delay_minutes": 1}],
    }

    saved = build_public_settings_save_config(
        existing,
        {"cfg": {"behavior": {"brain_mode": "smart"}, "integrations": {"avito": {}}}},
    )

    assert saved["behavior"]["brain_mode"] == "smart"
    assert saved["behavior"]["avito_smart_reply_enabled"] is True
    assert saved["integrations"]["avito"]["access_token"] == "old-token"
    assert saved["follow_up"] == [{"text": "old", "delay_minutes": 1}]


def test_public_settings_save_config_accepts_section_payloads_and_catalogs():
    saved = build_public_settings_save_config(
        {"passport": {"brand": "old"}, "limits": {"daily": 1}},
        {
            "passport": {"brand": "new"},
            "learning": {"enabled": True},
            "catalogs": [{"name": "main"}],
        },
    )

    assert saved["passport"]["brand"] == "new"
    assert saved["limits"] == {"daily": 1}
    assert saved["learning"] == {"enabled": True}
    assert saved["catalogs"] == [{"name": "main"}]


def test_public_settings_get_config_masks_only_amocrm_integration():
    cfg = {
        "passport": {"brand": "Avio"},
        "integrations": {
            "amocrm": {"manual": {"access_token": "secret"}},
            "avito": {"access_token": "avito-token"},
        },
    }

    saved = build_public_settings_get_config(
        cfg,
        tenant_id=101,
        mask_amocrm_cfg=lambda value, tenant_id: {
            "masked_for": tenant_id,
            "source": value,
        },
    )

    assert saved is not cfg
    assert saved["integrations"]["amocrm"]["masked_for"] == 101
    assert saved["integrations"]["avito"] == {"access_token": "avito-token"}
    assert cfg["integrations"]["amocrm"] == {"manual": {"access_token": "secret"}}
