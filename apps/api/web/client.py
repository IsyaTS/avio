import io
import json
import os
import pathlib
import random
import re
import time
import uuid
import asyncio
from typing import Any, Mapping

import logging
from datetime import datetime, timezone
from fastapi import APIRouter, File, Request, UploadFile
from fastapi.responses import JSONResponse, Response

from . import common as C
from . import auth_utils
from .ui import render_template

from libs.core import catalog as catalog_module
from libs.core import catalog_index
from libs.core import onboarding_chat
from libs.core import quickstart as quickstart_module
from libs.core import db as db
from libs.core.repo import avito_accounts, avito_item_contexts
from libs.core.response_pipeline import run_response_pipeline
from libs.core.common import (
    OUTBOX_QUEUE_KEY,
    handoff_silence_key,
    handoff_silence_meta_key,
    default_fallback_reply,
)
from libs.core.lib.tg_slots import (
    TG_SLOT_MAX,
    TG_SLOT_MIN,
    virtual_tenant_id as _virtual_tenant_id_shared,
)
from libs.core.services.behavior_settings import merge_behavior_settings, sanitize_behavior_triggers
from libs.core.services.tenant_config_merge import (
    merge_passport_settings_form,
    merge_tenant_config_for_settings,
)
from .services import client_analytics_runtime, client_avito_history_export_runtime, client_avito_history_runtime
from .services import client_assets_runtime
from .services import client_contextual_cases_runtime
from .services import client_catalog_runtime
from .services import client_dialogs_runtime
from .services import client_dialog_helpers_runtime
from .services import client_feedback_runtime
from .services import client_ops_runtime
from .services import client_reply_split_runtime
from .services import client_settings_runtime
from .services import client_training_runtime
import httpx

# NOTE: expose frequently used helpers after ensuring aliases are registered
build_pdf_index = catalog_index.build_pdf_index
CatalogIndexError = catalog_index.CatalogIndexError
index_to_catalog_items = getattr(catalog_index, "index_to_catalog_items", None)
write_catalog_csv = catalog_module.write_catalog_csv
load_conversation = onboarding_chat.load_conversation
save_conversation = onboarding_chat.save_conversation
reset_conversation = onboarding_chat.reset_conversation
evaluate_preconditions = onboarding_chat.evaluate_preconditions
preconditions_met = onboarding_chat.preconditions_met
initial_assistant_turn = onboarding_chat.initial_assistant_turn
next_assistant_turn = onboarding_chat.next_assistant_turn
add_user_message = onboarding_chat.add_user_message
add_assistant_message = onboarding_chat.add_assistant_message
public_messages = onboarding_chat.public_messages
update_tenant_insights = onboarding_chat.update_tenant_insights

router = APIRouter()
_log = logging.getLogger("training")
_LOG_PREFIX = "[training]"
_dialogs_log = logging.getLogger("client.dialogs")

_CLIENT_SETTINGS_JS: str | None = None

def _client_dialogs_runtime_deps() -> client_dialogs_runtime.ClientDialogsDeps:
    return client_dialogs_runtime.ClientDialogsDeps(
        resolve_tenant_and_key_fn=_resolve_tenant_and_key,
        db_module=db,
        isoformat_fn=_isoformat,
        normalize_attachments_fn=_normalize_message_attachments,
        parse_tg_slot_fn=_parse_tg_slot_from_source,
        load_silence_status_fn=_load_silence_status,
        load_telegram_slot_profiles_fn=_load_telegram_slot_profiles,
        common_module=C,
        is_technical_max_title_fn=_is_technical_max_title,
        run_response_pipeline_fn=run_response_pipeline,
        default_fallback_reply_fn=default_fallback_reply,
        apply_custom_punctuation_style_fn=_apply_custom_punctuation_style,
        split_reply_for_test_send_fn=_split_reply_for_test_send,
        delay_seconds_value_fn=_delay_seconds_value,
        smart_reply_delay_min_seconds=SMART_REPLY_DELAY_MIN_SECONDS,
        smart_reply_delay_max_seconds=SMART_REPLY_DELAY_MAX_SECONDS,
        smart_reply_split_part_delay_enabled=SMART_REPLY_SPLIT_PART_DELAY_ENABLED,
        smart_reply_split_channels=SMART_REPLY_SPLIT_CHANNELS,
        smart_reply_split_part_delay_min_seconds=SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS,
        smart_reply_split_part_delay_max_seconds=SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS,
        read_photo_manifest_fn=_read_photo_manifest,
        photo_public_url_fn=_photo_public_url,
        tg_slot_min=TG_SLOT_MIN,
        tg_slot_max=TG_SLOT_MAX,
        outbox_queue_key=OUTBOX_QUEUE_KEY,
        time_module=time,
        json_module=json,
        asyncio_module=asyncio,
        dialogs_logger=_dialogs_log,
        avito_accounts_repo=avito_accounts,
        avito_item_contexts_repo=avito_item_contexts,
    )

def _client_feedback_runtime_deps() -> client_feedback_runtime.ClientFeedbackDeps:
    return client_feedback_runtime.ClientFeedbackDeps(
        resolve_tenant_and_key_fn=_resolve_tenant_and_key,
        db_module=db,
        sanitize_training_text_fn=_sanitize_training_text,
        isoformat_fn=_isoformat,
        dialogs_logger=_dialogs_log,
    )

def _client_analytics_runtime_deps() -> client_analytics_runtime.ClientAnalyticsDeps:
    return client_analytics_runtime.ClientAnalyticsDeps(
        resolve_tenant_and_key_fn=_resolve_tenant_and_key,
        db_module=db,
    )

def _client_settings_runtime_deps() -> client_settings_runtime.ClientSettingsDeps:
    return client_settings_runtime.ClientSettingsDeps(
        authorize_client_settings_request_fn=_authorize_client_settings_request,
        resolve_key_fn=_resolve_key,
        auth_fn=_auth,
        common_module=C,
        auth_utils_module=auth_utils,
        quickstart_module=quickstart_module,
        render_template_fn=render_template,
        merge_passport_settings_form_fn=merge_passport_settings_form,
        merge_behavior_settings_fn=merge_behavior_settings,
        sanitize_behavior_triggers_fn=sanitize_behavior_triggers,
        export_max_days=EXPORT_MAX_DAYS,
        tg_slot_min=TG_SLOT_MIN,
        tg_slot_max=TG_SLOT_MAX,
        getenv_fn=os.getenv,
        json_module=json,
        logger=_log,
    )


def _client_training_runtime_deps() -> client_training_runtime.ClientTrainingDeps:
    return client_training_runtime.ClientTrainingDeps(
        authorize_client_settings_request_fn=_authorize_client_settings_request,
        db_module=db,
        settings_module=C.settings,
        logger=_log,
        log_prefix=_LOG_PREFIX,
        httpx_module=httpx,
        time_module=time,
    )

def _client_catalog_runtime_deps() -> client_catalog_runtime.ClientCatalogDeps:
    def _public_module():
        from . import public as public_module

        return public_module

    return client_catalog_runtime.ClientCatalogDeps(
        authorize_client_settings_request_fn=_authorize_client_settings_request,
        common_module=C,
        public_module_fn=_public_module,
        write_catalog_csv_fn=write_catalog_csv,
        catalog_index_error_cls=CatalogIndexError,
        detect_encoding_fn=_detect_encoding,
        detect_csv_delimiter_fn=_detect_csv_delimiter,
        strip_bom_fn=_strip_bom,
        max_upload_size_bytes=MAX_UPLOAD_SIZE_BYTES,
        time_module=time,
        uuid_module=uuid,
    )


def _client_ops_runtime_deps() -> client_ops_runtime.ClientOpsDeps:
    return client_ops_runtime.ClientOpsDeps(
        resolve_tenant_and_key_fn=_resolve_tenant_and_key,
        redis_client_fn=C.redis_client,
        handoff_silence_key_fn=handoff_silence_key,
        handoff_silence_meta_key_fn=handoff_silence_meta_key,
        outbox_queue_key=OUTBOX_QUEUE_KEY,
        json_module=json,
    )


MAX_UPLOAD_SIZE_BYTES = 25 * 1024 * 1024  # 25 MB safety cap for catalog uploads

DEFAULT_EXPORT_MAX_DAYS = 30
try:
    EXPORT_MAX_DAYS = int(os.getenv("EXPORT_MAX_DAYS", str(DEFAULT_EXPORT_MAX_DAYS)))
except (TypeError, ValueError):
    EXPORT_MAX_DAYS = DEFAULT_EXPORT_MAX_DAYS
if EXPORT_MAX_DAYS <= 0:
    EXPORT_MAX_DAYS = DEFAULT_EXPORT_MAX_DAYS

SMART_REPLY_SPLIT_ENABLED = (os.getenv("SMART_REPLY_SPLIT_ENABLED") or "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
try:
    SMART_REPLY_SPLIT_MIN_LEN = max(40, int(os.getenv("SMART_REPLY_SPLIT_MIN_LEN", "70")))
except Exception:
    SMART_REPLY_SPLIT_MIN_LEN = 70
try:
    SMART_REPLY_SPLIT_MAX_LEN = max(
        SMART_REPLY_SPLIT_MIN_LEN + 20, int(os.getenv("SMART_REPLY_SPLIT_MAX_LEN", "120"))
    )
except Exception:
    SMART_REPLY_SPLIT_MAX_LEN = max(SMART_REPLY_SPLIT_MIN_LEN + 20, 120)
try:
    SMART_REPLY_SPLIT_MAX_PARTS = max(2, int(os.getenv("SMART_REPLY_SPLIT_MAX_PARTS", "6")))
except Exception:
    SMART_REPLY_SPLIT_MAX_PARTS = 6
_SMART_REPLY_SPLIT_CHANNELS_RAW = (
    os.getenv("SMART_REPLY_SPLIT_CHANNELS") or "telegram,avito,whatsapp,max,max_personal"
).strip()
SMART_REPLY_SPLIT_CHANNELS = {
    part.strip().lower() for part in _SMART_REPLY_SPLIT_CHANNELS_RAW.split(",") if part.strip()
}
if not SMART_REPLY_SPLIT_CHANNELS:
    SMART_REPLY_SPLIT_CHANNELS = {"telegram", "avito", "whatsapp", "max", "max_personal"}

SMART_REPLY_SPLIT_PART_DELAY_ENABLED = (
    os.getenv("SMART_REPLY_SPLIT_PART_DELAY_ENABLED") or "1"
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
try:
    SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS = max(
        0, int(os.getenv("SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS", "5"))
    )
except Exception:
    SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS = 5
try:
    SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS = max(
        SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS,
        int(os.getenv("SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS", "10")),
    )
except Exception:
    SMART_REPLY_SPLIT_PART_DELAY_MAX_SECONDS = max(SMART_REPLY_SPLIT_PART_DELAY_MIN_SECONDS, 10)

try:
    SMART_REPLY_DELAY_MIN_SECONDS = max(0, int(os.getenv("SMART_REPLY_DELAY_MIN_SECONDS", "40")))
except Exception:
    SMART_REPLY_DELAY_MIN_SECONDS = 40
try:
    SMART_REPLY_DELAY_MAX_SECONDS = max(
        SMART_REPLY_DELAY_MIN_SECONDS,
        int(os.getenv("SMART_REPLY_DELAY_MAX_SECONDS", "120")),
    )
except Exception:
    SMART_REPLY_DELAY_MAX_SECONDS = max(120, SMART_REPLY_DELAY_MIN_SECONDS)


def _load_client_settings_js() -> str:
    global _CLIENT_SETTINGS_JS
    if _CLIENT_SETTINGS_JS is not None:
        return _CLIENT_SETTINGS_JS

    bundle_path = (
        pathlib.Path(__file__).resolve().parents[1] / "static" / "js" / "client-settings.js"
    )
    try:
        _CLIENT_SETTINGS_JS = bundle_path.read_text("utf-8")
    except Exception as exc:
        try:
            _log.warning("Failed to read client-settings.js bundle: %s", exc)
        except Exception:
            pass
        _CLIENT_SETTINGS_JS = ""
    return _CLIENT_SETTINGS_JS


def _apply_custom_punctuation_style(text: str) -> str:
    return client_reply_split_runtime.apply_custom_punctuation_style(text)


def _split_reply_for_test_send(reply_text: str, channel: str) -> list[str]:
    config = client_reply_split_runtime.ReplySplitConfig(
        enabled=SMART_REPLY_SPLIT_ENABLED,
        min_len=SMART_REPLY_SPLIT_MIN_LEN,
        max_len=SMART_REPLY_SPLIT_MAX_LEN,
        max_parts=SMART_REPLY_SPLIT_MAX_PARTS,
        channels=SMART_REPLY_SPLIT_CHANNELS,
    )
    return client_reply_split_runtime.split_reply_for_test_send(reply_text, channel, config)


def _delay_seconds_value(min_seconds: int, max_seconds: int) -> float:
    if max_seconds <= min_seconds:
        return float(min_seconds)
    return float(random.randint(min_seconds, max_seconds))


def _sanitize_text(text: str) -> str:
    """Light PII scrubbing for exports: phones, emails, WA ids."""
    if not text:
        return ""
    # redact emails
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}", "<EMAIL>", text)
    # redact long digit sequences (phones, order numbers) of 5+ digits
    text = re.sub(r"(?<!\d)\d{5,}(?!\d)", "<NUMBER>", text)
    # redact whatsapp jids
    text = re.sub(r"\b\d{5,}@s\.whatsapp\.net\b", "<WA_ID>", text)
    return text


def _sanitize_training_text(text: str) -> str:
    return client_training_runtime.sanitize_training_text(text)


def _detect_encoding(payload: bytes) -> str:
    """Best-effort detection for common CSV encodings used by clients."""
    if not payload:
        return "utf-8"

    # Quick BOM check before falling back to heuristics
    if payload.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"

    candidates = [
        "utf-8",
        "utf-8-sig",
        "cp1251",
        "windows-1251",
        "koi8-r",
    ]
    for encoding in candidates:
        try:
            payload.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "utf-8"


def _strip_bom(text: str) -> str:
    if not text:
        return ""
    if text[0] == "\ufeff":
        return text.lstrip("\ufeff")
    return text


_DELIMITER_CANDIDATES = [";", ",", "\t"]


def _detect_csv_delimiter(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ","

    first_line = ""
    for raw_line in io.StringIO(text):
        candidate = raw_line.strip("\r\n")
        if candidate:
            first_line = _strip_bom(candidate)
            break

    if not first_line:
        return ","

    best = ","
    best_count = -1
    best_idx = len(_DELIMITER_CANDIDATES)
    for idx, delimiter in enumerate(_DELIMITER_CANDIDATES):
        count = first_line.count(delimiter)
        if count > best_count or (count == best_count and count > 0 and idx < best_idx):
            best = delimiter
            best_count = count
            best_idx = idx

    if best_count <= 0:
        return ","
    return best


def _resolve_key(request: Request | None, raw: str | None = None) -> str:
    candidates: list[str] = []
    if raw:
        candidates.append(raw)

    if request is not None:
        qp = request.query_params
        candidates.append(qp.get("k"))
        candidates.append(qp.get("key"))

        headers = request.headers
        for header_name in ("X-Access-Key", "X-Client-Key", "X-Auth-Key"):
            candidates.append(headers.get(header_name))

        auth_header = headers.get("Authorization")
        if auth_header:
            token = auth_header.strip()
            if token.lower().startswith("bearer "):
                token = token[7:]
            candidates.append(token)

        if request.cookies:
            candidates.append(request.cookies.get("client_key"))

    for candidate in candidates:
        if not candidate:
            continue
        value = candidate.strip()
        if value:
            return value
    return ""


def _auth(tenant: int, key: str) -> bool:
    return C.valid_key(int(tenant), key or "")


def _resolve_tenant_and_key(
    request: Request, tenant: int | str | None
) -> tuple[int, str] | Response:
    tenant_raw = tenant if tenant is not None else request.query_params.get("tenant")
    try:
        tenant_id = int(str(tenant_raw).strip())
    except Exception:
        return JSONResponse({"detail": "invalid_tenant"}, status_code=400)
    if tenant_id <= 0:
        return JSONResponse({"detail": "invalid_tenant"}, status_code=400)

    key = _resolve_key(request, request.query_params.get("k"))
    if not _auth(tenant_id, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    return tenant_id, key


async def _authorize_client_settings_request(
    request: Request,
    tenant: int | str | None,
) -> tuple[int, str] | Response:
    tenant_raw = tenant if tenant is not None else request.query_params.get("tenant")
    try:
        tenant_id = int(str(tenant_raw).strip())
    except Exception:
        return JSONResponse({"detail": "invalid_tenant"}, status_code=400)
    if tenant_id <= 0:
        return JSONResponse({"detail": "invalid_tenant"}, status_code=400)

    key = _resolve_key(request, request.query_params.get("k"))
    session_user = await auth_utils.get_current_user(request)
    session_tenant = int(session_user.get("tenant_id") or 0) if isinstance(session_user, dict) else 0
    if session_tenant > 0 and session_tenant == tenant_id:
        resolved_key = key
        if not resolved_key:
            resolved_key = (C.get_tenant_pubkey(int(tenant_id)) or "").strip()
            if not resolved_key:
                keys = C.list_keys(int(tenant_id))
                resolved_key = (keys[0].get("key") if keys else "") or ""
        return tenant_id, resolved_key

    if not _auth(tenant_id, key):
        return JSONResponse({"detail": "unauthorized"}, status_code=401)
    return tenant_id, key


client_avito_history_runtime.register_routes(router, _authorize_client_settings_request, C, _log)
client_avito_history_export_runtime.register_routes(router, _authorize_client_settings_request, C, _log)
client_contextual_cases_runtime.register_routes(router, _authorize_client_settings_request, C, _log)

def _isoformat(value: Any) -> str | None:
    if isinstance(value, datetime):
        dt = value
    elif value is None:
        return None
    else:
        try:
            dt = datetime.fromisoformat(str(value))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _parse_tg_slot_from_source(source: Any) -> int | None:
    return client_dialog_helpers_runtime.parse_tg_slot_from_source(
        source,
        slot_min=TG_SLOT_MIN,
        slot_max=TG_SLOT_MAX,
    )


def _is_technical_max_title(value: Any) -> bool:
    return client_dialog_helpers_runtime.is_technical_max_title(value)


def _tg_slot_tenant(tenant_id: int, slot: int) -> int:
    return client_dialog_helpers_runtime.tg_slot_tenant(
        tenant_id,
        slot,
        virtual_tenant_id_fn=_virtual_tenant_id_shared,
    )


def _load_telegram_slot_profiles(tenant_id: int) -> list[dict[str, Any]]:
    return client_dialog_helpers_runtime.load_telegram_slot_profiles(
        tenant_id,
        common_module=C,
        slot_min=TG_SLOT_MIN,
        slot_max=TG_SLOT_MAX,
        virtual_tenant_id_fn=_virtual_tenant_id_shared,
    )


def _ts_iso(ts: int | None) -> str | None:
    return client_dialog_helpers_runtime.ts_iso(ts)


def _channel_reply_enabled(cfg: Mapping[str, Any], channel: str) -> bool:
    return client_dialog_helpers_runtime.channel_reply_enabled(cfg, channel)


def _load_silence_status(
    tenant_id: int,
    lead_id: int,
    channel: str,
) -> dict[str, Any]:
    return client_dialog_helpers_runtime.load_silence_status(
        tenant_id,
        lead_id,
        channel,
        common_module=C,
        silence_key_fn=handoff_silence_key,
        silence_meta_key_fn=handoff_silence_meta_key,
    )


def _tenant_root(tenant: int) -> pathlib.Path:
    return client_dialog_helpers_runtime.tenant_root(tenant, common_module=C)


def _photo_manifest_path(tenant: int) -> pathlib.Path:
    return client_dialog_helpers_runtime.photo_manifest_path(tenant, common_module=C)


def _read_photo_manifest(tenant: int) -> list[dict[str, Any]]:
    return client_dialog_helpers_runtime.read_photo_manifest(tenant, common_module=C)


def _photo_public_url(request: Request, tenant_id: int, key: str, photo_id: str) -> str:
    return client_dialog_helpers_runtime.photo_public_url(
        request,
        tenant_id,
        key,
        photo_id,
        common_module=C,
    )


def _normalize_message_attachments(
    request: Request,
    tenant_id: int,
    key: str,
    attachments: Any,
) -> list[dict[str, Any]]:
    return client_dialog_helpers_runtime.normalize_message_attachments(
        request,
        tenant_id,
        key,
        attachments,
        common_module=C,
    )


def _safe_path(tenant: int, relative: str | pathlib.Path | None) -> pathlib.Path | None:
    if not relative:
        return None
    try:
        base = _tenant_root(tenant)
        candidate = (base / pathlib.Path(str(relative))).resolve(strict=False)
    except Exception:
        return None
    try:
        base_resolved = base.resolve(strict=False)
    except Exception:
        base_resolved = base
    if base_resolved in candidate.parents or candidate == base_resolved:
        return candidate
    return None


def _catalog_csv_path(
    tenant: int, cfg: dict | None = None
) -> tuple[pathlib.Path | None, str | None, str | None]:
    return client_catalog_runtime.catalog_csv_path(
        tenant,
        cfg,
        deps=_client_catalog_runtime_deps(),
    )


def read_csv_table(
    tenant: int, cfg: dict | None = None
) -> dict[str, list[list[str]] | list[str] | str]:
    csv_path, encoding_hint, relative = _catalog_csv_path(tenant, cfg)
    return client_catalog_runtime.read_csv_table_from_path(
        csv_path,
        encoding_hint,
        relative,
        deps=_client_catalog_runtime_deps(),
    )


def write_csv_table(
    tenant: int,
    columns: Any,
    rows: Any,
    cfg: dict | None = None,
) -> int:
    csv_path, _, _ = _catalog_csv_path(tenant, cfg)
    return client_catalog_runtime.write_csv_table_to_path(
        csv_path,
        columns,
        rows,
    )


@router.get("/client/{tenant}/settings")
async def client_settings(tenant: int, request: Request):
    return await client_settings_runtime.client_settings(
        tenant,
        request,
        deps=_client_settings_runtime_deps(),
    )


@router.get("/client/settings")
async def client_settings_short(request: Request):
    return await client_settings_runtime.client_settings_short(
        request,
        deps=_client_settings_runtime_deps(),
    )


@router.post("/client/{tenant}/settings/save")
async def save_form(tenant: int, request: Request):
    return await client_settings_runtime.save_form(
        tenant,
        request,
        deps=_client_settings_runtime_deps(),
    )


def _sanitize_triggers(payload: Any) -> list[dict[str, Any]]:
    return sanitize_behavior_triggers(payload)


@router.post("/client/{tenant}/behavior/save")
async def save_behavior(tenant: int, request: Request):
    return await client_settings_runtime.save_behavior(
        tenant,
        request,
        deps=_client_settings_runtime_deps(),
    )


@router.get("/client/{tenant}/follow-ups")
async def get_follow_ups(tenant: int, request: Request):
    return await client_settings_runtime.get_follow_ups(
        tenant,
        request,
        deps=_client_settings_runtime_deps(),
    )


@router.post("/client/{tenant}/follow-ups")
async def save_follow_ups(tenant: int, request: Request):
    return await client_settings_runtime.save_follow_ups(
        tenant,
        request,
        deps=_client_settings_runtime_deps(),
    )


@router.get("/api/dialogs")
async def list_dialogs_api(request: Request, tenant: int | str | None = None, limit: int = 200):
    return await client_dialogs_runtime.list_dialogs_api(
        request,
        tenant=tenant,
        limit=limit,
        deps=_client_dialogs_runtime_deps(),
    )


@router.get("/api/dialogs/{lead_id}")
async def get_dialog_messages_api(
    lead_id: int,
    request: Request,
    tenant: int | str | None = None,
    limit: int = 50,
    before: str | None = None,
):
    return await client_dialogs_runtime.get_dialog_messages_api(
        lead_id,
        request,
        tenant=tenant,
        limit=limit,
        before=before,
        deps=_client_dialogs_runtime_deps(),
    )


@router.post("/api/dialogs/{lead_id}/send")
async def send_dialog_message_api(
    lead_id: int,
    request: Request,
    tenant: int | str | None = None,
):
    return await client_dialogs_runtime.send_dialog_message_api(
        lead_id,
        request,
        tenant=tenant,
        deps=_client_dialogs_runtime_deps(),
    )


@router.post("/api/dialogs/test")
async def test_dialog_api(request: Request, tenant: int | str | None = None):
    return await client_dialogs_runtime.test_dialog_api(
        request,
        tenant=tenant,
        deps=_client_dialogs_runtime_deps(),
    )


@router.post("/api/dialogs/{lead_id}/unsilence")
async def dialogs_unsilence_api(request: Request, lead_id: int, tenant: int | str | None = None):
    return await client_ops_runtime.dialogs_unsilence_api(
        request,
        lead_id,
        tenant=tenant,
        deps=_client_ops_runtime_deps(),
    )


@router.get("/api/tenant/stats")
async def tenant_stats_api(request: Request, tenant: int | str | None = None, sample: int = 500):
    return await client_ops_runtime.tenant_stats_api(
        request,
        tenant=tenant,
        sample=sample,
        deps=_client_ops_runtime_deps(),
    )


@router.get("/api/analytics/summary")
async def analytics_summary_api(
    request: Request,
    tenant: int | str | None = None,
    days: int = 7,
):
    return await client_analytics_runtime.analytics_summary_api(
        request,
        tenant=tenant,
        days=days,
        deps=_client_analytics_runtime_deps(),
    )


@router.post("/api/feedback")
async def submit_feedback_api(request: Request, tenant: int | str | None = None):
    return await client_feedback_runtime.submit_feedback_api(
        request,
        tenant=tenant,
        deps=_client_feedback_runtime_deps(),
    )


@router.get("/api/feedback/stats")
async def feedback_stats_api(request: Request, tenant: int | str | None = None):
    auth = _resolve_tenant_and_key(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    counts = await db.get_feedback_counts(tenant_id)
    return {"ok": True, "likes": counts.get("like", 0), "dislikes": counts.get("dislike", 0)}


@router.get("/api/feedback/quality")
async def feedback_quality_api(request: Request, tenant: int | str | None = None):
    return await client_feedback_runtime.feedback_quality_api(
        request,
        tenant=tenant,
        deps=_client_feedback_runtime_deps(),
    )


@router.post("/client/{tenant}/settings/json")
async def save_json(tenant: int, request: Request):
    auth = await _authorize_client_settings_request(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant, key = auth
    try:
        raw = await request.body()
        incoming = json.loads(raw.decode("utf-8"))
        existing = C.read_tenant_config(tenant)
        if not isinstance(existing, dict):
            existing = {}
        if isinstance(incoming, Mapping):
            cfg = merge_tenant_config_for_settings(existing, incoming)
        else:
            cfg = existing
        C.write_tenant_config(tenant, cfg)
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


@router.post("/client/{tenant}/persona")
async def save_persona(tenant: int, request: Request):
    auth = await _authorize_client_settings_request(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant, key = auth
    payload = await request.json()
    channel_raw = payload.get("channel")
    channel = None
    if isinstance(channel_raw, str) and channel_raw.strip():
        candidate = channel_raw.strip().lower()
        if candidate in {"telegram", "avito", "max"}:
            channel = candidate
    C.write_persona(tenant, payload.get("text") or "", channel=channel)
    if channel is None:
        cfg = C.read_tenant_config(tenant)
        if isinstance(cfg, dict):
            qs = cfg.get("quickstart") if isinstance(cfg.get("quickstart"), dict) else {}
            if isinstance(qs, dict):
                qs["auto_persona"] = False
                cfg["quickstart"] = qs
                C.write_tenant_config(tenant, cfg)
    return {"ok": True}


@router.get("/client/{tenant}/assets")
async def client_assets_list(tenant: int, request: Request):
    auth = await _authorize_client_settings_request(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant, _key = auth
    return await client_assets_runtime.list_assets_status(int(tenant))


@router.get("/client/{tenant}/quickstart/templates")
async def quickstart_templates(tenant: int, request: Request):
    auth = await _authorize_client_settings_request(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant, key = auth
    return {"ok": True, "templates": quickstart_module.list_quickstart_templates()}


@router.post("/client/{tenant}/quickstart/apply")
async def quickstart_apply(tenant: int, request: Request):
    auth = await _authorize_client_settings_request(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant, key = auth
    payload = await request.json()
    try:
        result = quickstart_module.apply_quickstart(
            int(tenant), payload if isinstance(payload, dict) else {}
        )
    except Exception:
        _log.exception("quickstart_apply_failed tenant=%s", tenant)
        return JSONResponse({"detail": "quickstart_failed"}, status_code=500)
    return result


@router.post("/client/{tenant}/catalog/upload")
async def catalog_upload(tenant: int, request: Request, file: UploadFile = File(...)):
    return await client_catalog_runtime.catalog_upload(
        tenant,
        request,
        file,
        deps=_client_catalog_runtime_deps(),
    )


@router.post("/client/{tenant}/training/telegram/harvest")
async def training_tg_harvest(tenant: int, request: Request):
    return await client_training_runtime.training_tg_harvest(
        tenant,
        request,
        deps=_client_training_runtime_deps(),
    )


@router.post("/client/{tenant}/training/telegram/accept")
async def training_tg_accept(tenant: int, request: Request):
    return await client_training_runtime.training_tg_accept(
        tenant,
        request,
        deps=_client_training_runtime_deps(),
    )


@router.get("/client/{tenant}/catalog/csv")
async def catalog_csv_get(tenant: int, request: Request):
    return await client_catalog_runtime.catalog_csv_get(
        tenant,
        request,
        deps=_client_catalog_runtime_deps(),
    )


@router.post("/client/{tenant}/catalog/csv")
async def catalog_csv_save(tenant: int, request: Request):
    return await client_catalog_runtime.catalog_csv_save(
        tenant,
        request,
        deps=_client_catalog_runtime_deps(),
    )


def _onboarding_error(reason: str, status_code: int = 400):
    return JSONResponse({"ok": False, "error": reason}, status_code=status_code)


async def _ensure_onboarding_started(tenant: int, convo, cfg, persona):
    if convo.get("messages"):
        return convo, None
    ask, delta, complete = await initial_assistant_turn(tenant, convo, cfg, persona)
    if ask:
        add_assistant_message(convo, ask, insights=delta or None, complete=complete)
    save_conversation(tenant, convo)
    update_tenant_insights(tenant, convo.get("status", "in_progress"), delta)
    return convo, ask


async def _collect_onboarding_context(tenant: int):
    checks, payload = evaluate_preconditions(tenant)
    cfg = payload.get("cfg") or {}
    persona = payload.get("persona") or ""
    convo = load_conversation(tenant)
    return checks, cfg, persona, convo


@router.get("/client/{tenant}/onboarding/state")
async def onboarding_state(tenant: int, request: Request):
    auth = await _authorize_client_settings_request(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant, key = auth

    checks, cfg, persona, convo = await _collect_onboarding_context(tenant)
    ready = preconditions_met(checks)

    if ready:
        convo, _ = await _ensure_onboarding_started(tenant, convo, cfg, persona)

    save_conversation(tenant, convo)
    update_tenant_insights(tenant, convo.get("status", "in_progress"), None)

    return {
        "ok": True,
        "ready": ready,
        "checks": checks,
        "status": convo.get("status", "new"),
        "messages": public_messages(convo),
    }


@router.post("/client/{tenant}/onboarding/message")
async def onboarding_message(tenant: int, request: Request):
    auth = await _authorize_client_settings_request(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant, key = auth

    payload = await request.json()
    user_text = (payload.get("text") or "").strip()
    if not user_text:
        return _onboarding_error("empty_message")

    checks, cfg, persona, convo = await _collect_onboarding_context(tenant)
    if not preconditions_met(checks):
        return _onboarding_error("preconditions_not_met")

    if convo.get("status") == "completed":
        return _onboarding_error("onboarding_already_completed")

    add_user_message(convo, user_text)
    ask, delta, complete = await next_assistant_turn(tenant, convo, cfg, persona, user_text)
    if ask:
        add_assistant_message(convo, ask, insights=delta or None, complete=complete)
    save_conversation(tenant, convo)
    update_tenant_insights(tenant, convo.get("status", "in_progress"), delta)

    return {
        "ok": True,
        "status": convo.get("status", "in_progress"),
        "messages": public_messages(convo),
    }


@router.post("/client/{tenant}/onboarding/reset")
async def onboarding_reset(tenant: int, request: Request):
    auth = await _authorize_client_settings_request(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant, key = auth

    reset_conversation(tenant)
    update_tenant_insights(tenant, "new", None)
    return {"ok": True}
