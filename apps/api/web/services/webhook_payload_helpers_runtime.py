from __future__ import annotations

import json
import pathlib
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib.parse import quote, urlsplit, urlunsplit


_HUMAN_NAME_RE = re.compile(r"[A-Za-zА-Яа-яЁё]")


@dataclass(frozen=True)
class CatalogAttachmentDeps:
    core_module: Any
    settings_module: Any
    client_config_module: Any


def digits(value: str) -> str:
    return "".join(ch for ch in str(value) if ch.isdigit())


def is_human_readable_name(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    cleaned = value.strip()
    if not cleaned or cleaned.isdigit():
        return False
    if cleaned.lower().startswith(("max:", "max_personal:", "tg:id", "max:id")):
        return False
    return bool(_HUMAN_NAME_RE.search(cleaned))


def has_photo_attachment(blobs: Iterable[Mapping[str, Any]] | None) -> bool:
    if not blobs:
        return False
    for blob in blobs:
        if not isinstance(blob, Mapping):
            continue
        if _photo_marker_present(blob):
            return True
        nested_media = blob.get("media") if isinstance(blob.get("media"), Mapping) else None
        nested_photo_raw = blob.get("photo")
        nested_photo = nested_photo_raw if isinstance(nested_photo_raw, Mapping) else None
        if nested_media and has_photo_attachment([nested_media]):
            return True
        if nested_photo and has_photo_attachment([nested_photo]):
            return True
        if isinstance(nested_photo_raw, list) and nested_photo_raw:
            nested_items = [item for item in nested_photo_raw if isinstance(item, Mapping)]
            if has_photo_attachment(nested_items):
                return True
    return False


def _photo_marker_present(blob: Mapping[str, Any]) -> bool:
    marker = str(blob.get("_") or "").strip().lower()
    raw_type = str(blob.get("type") or blob.get("kind") or "").strip().lower()
    raw_mime = (
        str(blob.get("mime") or blob.get("mime_type") or blob.get("mimetype") or "")
        .strip()
        .lower()
    )
    if marker and ("photo" in marker or "image" in marker):
        return True
    if raw_type in {"photo", "image", "picture"}:
        return True
    return raw_mime.startswith("image/")


def as_mapping(candidate: Any) -> Mapping[str, Any] | None:
    if isinstance(candidate, Mapping):
        return candidate
    if isinstance(candidate, (bytes, bytearray)):
        try:
            parsed = json.loads(candidate.decode("utf-8"))
        except Exception:
            return None
        return parsed if isinstance(parsed, Mapping) else None
    if isinstance(candidate, str):
        try:
            parsed = json.loads(candidate)
        except Exception:
            return None
        return parsed if isinstance(parsed, Mapping) else None
    return None


def is_avito_system_message(
    text: str,
    message: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> bool:
    for candidate in (
        message.get("is_system"),
        message.get("system"),
        message.get("system_message"),
        payload.get("is_system"),
        payload.get("system"),
        payload.get("system_message"),
    ):
        if candidate in (True, 1, "1", "true", "True", "yes", "on"):
            return True
    for candidate in (
        message.get("type"),
        message.get("kind"),
        message.get("message_type"),
        payload.get("type"),
        payload.get("kind"),
        payload.get("message_type"),
    ):
        raw = str(candidate or "").strip().lower()
        if raw in {"system", "service"}:
            return True
    return str(text or "").strip().lower().startswith("[системное сообщение]")


def has_contact_identifiers(
    *,
    phone: str | None = None,
    whatsapp_phone: str | None = None,
    avito_user_id: int | None = None,
    avito_login: str | None = None,
    telegram_user_id: int | None = None,
    telegram_username: str | None = None,
    max_user_id: int | None = None,
    max_username: str | None = None,
) -> bool:
    if _non_empty(phone) or _non_empty(whatsapp_phone):
        return True
    if _non_zero_int(avito_user_id) or _non_zero_int(telegram_user_id) or _non_zero_int(max_user_id):
        return True
    return bool(_non_empty(avito_login) or _non_empty(telegram_username) or _non_empty(max_username))


def _non_empty(value: str | None) -> bool:
    return bool(value and str(value).strip())


def _non_zero_int(value: int | None) -> bool:
    if value is None:
        return False
    try:
        return int(value) != 0
    except Exception:
        return False


def resolve_catalog_attachment(
    cfg: dict | None,
    tenant: int,
    request: Any | None,
    *,
    deps: CatalogAttachmentDeps,
) -> tuple[dict[str, Any] | None, str]:
    try:
        resolved_meta = deps.core_module.resolve_catalog_pdf_meta(int(tenant), cfg)
    except Exception:
        resolved_meta = None
    if not resolved_meta:
        return None, ""
    target, relative_path, filename, mime = _catalog_file_parts(resolved_meta)
    if not target.exists() or not target.is_file():
        return None, ""
    url = _internal_catalog_file_url(
        tenant,
        relative_path,
        request,
        settings_module=deps.settings_module,
    )
    url = _append_catalog_token(url, deps.client_config_module)
    caption = f"Каталог в PDF: {filename}"
    attachment = {
        "type": "document",
        "url": url,
        "path": str(target),
        "filename": filename,
        "mime_type": mime,
        "mime": mime,
        "mimetype": mime,
        "sendMediaAsDocument": True,
        "caption": caption,
    }
    return attachment, caption


def _catalog_file_parts(resolved_meta: Mapping[str, Any]) -> tuple[pathlib.Path, str, str, str]:
    relative_path = str(resolved_meta.get("relative_path") or "")
    absolute_path = str(resolved_meta.get("absolute_path") or "")
    filename = str(resolved_meta.get("filename") or pathlib.Path(relative_path or "catalog.pdf").name)
    mime = str(resolved_meta.get("mime") or "application/pdf")
    return pathlib.Path(absolute_path or ""), relative_path, filename, mime


def _internal_catalog_file_url(
    tenant: int,
    relative_path: str,
    request: Any | None,
    *,
    settings_module: Any,
) -> str:
    if request is not None:
        base = str(request.url_for("internal_catalog_file", tenant=str(tenant)))
    else:
        base_root = settings_module.APP_INTERNAL_URL or settings_module.APP_PUBLIC_URL or ""
        if not base_root:
            base_root = "http://app:8000"
        base = f"{base_root.rstrip('/')}/internal/tenant/{tenant}/catalog-file"
    return f"{base}?path={quote(str(relative_path), safe='/')}"


def _append_catalog_token(url: str, client_config_module: Any) -> str:
    token = (
        getattr(client_config_module, "WA_INTERNAL_TOKEN", "")
        or getattr(client_config_module, "INTERNAL_SYNC_TOKEN", "")
        or getattr(client_config_module, "WEBHOOK_SECRET", "")
        or ""
    )
    if not token:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}token={quote(token)}"


def build_public_catalog_url(
    tenant: int,
    attachment_mtime: int,
    request: Any | None,
    *,
    settings_module: Any,
) -> str:
    base_override = (getattr(settings_module, "APP_PUBLIC_URL", "") or "").strip()
    raw_url = _raw_public_catalog_url(tenant, request, settings_module=settings_module)
    if base_override:
        raw_url = _apply_public_base_override(raw_url, base_override, tenant)
    if attachment_mtime:
        separator = "&" if "?" in raw_url else "?"
        raw_url = f"{raw_url}{separator}v={attachment_mtime}"
    return raw_url


def _raw_public_catalog_url(tenant: int, request: Any | None, *, settings_module: Any) -> str:
    if request is not None:
        try:
            raw_url = str(request.url_for("public_catalog_file", tenant=str(tenant)))
        except Exception:
            raw_url = ""
        if raw_url:
            return raw_url
    fallback_base = (
        (getattr(settings_module, "APP_PUBLIC_URL", "") or "").strip()
        or getattr(settings_module, "APP_INTERNAL_URL", "")
        or getattr(settings_module, "APP_PUBLIC_URL", "")
        or "http://app:8000"
    )
    return f"{fallback_base.rstrip('/')}/pub/catalog/file/{tenant}"


def _apply_public_base_override(raw_url: str, base_override: str, tenant: int) -> str:
    try:
        current = urlsplit(raw_url)
        target = urlsplit(base_override)
        path = current.path or f"/pub/catalog/file/{tenant}"
        return urlunsplit(
            (
                target.scheme or current.scheme or "https",
                target.netloc or current.netloc,
                path,
                current.query,
                current.fragment,
            )
        )
    except Exception:
        return f"{base_override.rstrip('/')}/pub/catalog/file/{tenant}"
