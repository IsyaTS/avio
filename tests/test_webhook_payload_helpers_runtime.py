from __future__ import annotations

from types import SimpleNamespace

import pytest

from apps.api.web.services import webhook_payload_helpers_runtime as runtime


pytestmark = pytest.mark.unit


def test_has_photo_attachment_detects_nested_telegram_photo() -> None:
    assert runtime.has_photo_attachment([{"media": {"_": "MessageMediaPhoto"}}]) is True
    assert runtime.has_photo_attachment([{"mime_type": "image/jpeg"}]) is True
    assert runtime.has_photo_attachment([{"type": "document", "mime": "application/pdf"}]) is False


def test_has_contact_identifiers_checks_non_empty_and_non_zero_values() -> None:
    assert runtime.has_contact_identifiers(phone=" +7999 ") is True
    assert runtime.has_contact_identifiers(avito_user_id=0, telegram_user_id=None) is False
    assert runtime.has_contact_identifiers(max_username="user") is True


def test_as_mapping_accepts_json_strings_and_rejects_invalid_payloads() -> None:
    assert runtime.as_mapping('{"tenant": 1}') == {"tenant": 1}
    assert runtime.as_mapping(b'{"tenant": 2}') == {"tenant": 2}
    assert runtime.as_mapping("not-json") is None
    assert runtime.as_mapping("[1,2]") is None


def test_is_avito_system_message_uses_flags_types_and_text_prefix() -> None:
    assert runtime.is_avito_system_message("", {"is_system": True}, {}) is True
    assert runtime.is_avito_system_message("", {"type": "service"}, {}) is True
    assert runtime.is_avito_system_message("[Системное сообщение] test", {}, {}) is True
    assert runtime.is_avito_system_message("hello", {}, {}) is False


def test_build_public_catalog_url_applies_public_base_override() -> None:
    settings = SimpleNamespace(APP_PUBLIC_URL="https://public.example", APP_INTERNAL_URL="http://app:8000")

    url = runtime.build_public_catalog_url(
        7,
        123,
        None,
        settings_module=settings,
    )

    assert url == "https://public.example/pub/catalog/file/7?v=123"


def test_resolve_catalog_attachment_builds_internal_url(tmp_path) -> None:
    catalog = tmp_path / "catalog.pdf"
    catalog.write_bytes(b"%PDF")
    deps = runtime.CatalogAttachmentDeps(
        core_module=SimpleNamespace(
            resolve_catalog_pdf_meta=lambda tenant, cfg: {
                "relative_path": "uploads/catalog.pdf",
                "absolute_path": str(catalog),
                "filename": "catalog.pdf",
                "mime": "application/pdf",
            }
        ),
        settings_module=SimpleNamespace(APP_INTERNAL_URL="http://app:8000", APP_PUBLIC_URL=""),
        client_config_module=SimpleNamespace(WA_INTERNAL_TOKEN="token", INTERNAL_SYNC_TOKEN="", WEBHOOK_SECRET=""),
    )

    attachment, caption = runtime.resolve_catalog_attachment({}, 7, None, deps=deps)

    assert attachment is not None
    assert attachment["url"] == (
        "http://app:8000/internal/tenant/7/catalog-file?path=uploads/catalog.pdf&token=token"
    )
    assert attachment["sendMediaAsDocument"] is True
    assert caption == "Каталог в PDF: catalog.pdf"
