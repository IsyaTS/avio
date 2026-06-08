import pytest

from libs.core.services.behavior_settings import merge_behavior_settings, sanitize_behavior_triggers


pytestmark = pytest.mark.unit


def test_merge_behavior_settings_preserves_absent_critical_toggles():
    existing = {
        "auto_reply": True,
        "auto_reply_enabled": True,
        "avito_smart_reply_enabled": True,
        "telegram_reply_enabled": True,
        "max_reply_enabled": True,
        "send_catalog_on_first_message": True,
        "send_catalog_on_first_message_max": True,
        "brain_mode": "smart",
        "human_reply_mode": False,
        "triggers": [{"type": "keyword", "value": "price"}],
        "photo_expected_markers": ["photo"],
        "photo_expected_reply": "Пришлите фото",
        "photo_expected_ttl": 600,
    }

    saved = merge_behavior_settings(existing, {"auto_reply_text": "ok"})

    assert saved["auto_reply_text"] == "ok"
    assert saved["avito_smart_reply_enabled"] is True
    assert saved["telegram_reply_enabled"] is True
    assert saved["max_reply_enabled"] is True
    assert saved["send_catalog_on_first_message"] is True
    assert saved["send_catalog_on_first_message_max"] is True
    assert saved["brain_mode"] == "smart"
    assert saved["triggers"] == existing["triggers"]
    assert saved["photo_expected_markers"] == ["photo"]
    assert saved["photo_expected_reply"] == "Пришлите фото"
    assert saved["photo_expected_ttl"] == 600


def test_merge_behavior_settings_allows_explicit_toggle_disable():
    saved = merge_behavior_settings(
        {"avito_smart_reply_enabled": True, "brain_mode": "smart"},
        {"avito_smart_reply_enabled": False, "brain_mode": "classic"},
    )

    assert saved["avito_smart_reply_enabled"] is False
    assert saved["brain_mode"] == "classic"
    assert saved["human_reply_mode"] is True


def test_sanitize_behavior_triggers_supports_keywords_and_defaults():
    saved = sanitize_behavior_triggers(
        [
            {
                "keywords": "договор, цена",
                "channels": [],
                "silence": False,
                "notify": True,
            },
            {"phrases": []},
            "bad",
        ]
    )

    assert saved == [
        {
            "phrases": ["договор", "цена"],
            "channels": ["telegram", "avito", "whatsapp", "max", "max_personal"],
            "silence": False,
            "notify": True,
        }
    ]


def test_merge_behavior_settings_sanitizes_triggers():
    saved = merge_behavior_settings(
        {"triggers": [{"phrases": ["old"], "channels": ["telegram"]}]},
        {"triggers": [{"phrases": [" New "], "channels": "Avito"}]},
    )

    assert saved["triggers"] == [
        {
            "phrases": ["New"],
            "channels": ["avito"],
            "silence": True,
            "notify": False,
        }
    ]
