from __future__ import annotations

import io
import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from zoneinfo import ZoneInfo

try:  # Support both package-style (app.export) and bare (export)
    from .. import db as db_module  # type: ignore[import-error]
except (ImportError, ValueError):
    import db as db_module  # type: ignore[no-redef]


def _resolve_timezone() -> ZoneInfo:
    name = os.getenv("APP_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
    try:
        return ZoneInfo(name)
    except Exception:
        return ZoneInfo("UTC")


EXPORT_TZ = _resolve_timezone()
INVALID_CHARS_RE = re.compile(r"[\\/:*?\"<>|]+")
WHITESPACE_RE = re.compile(r"\s+")


_log = logging.getLogger("wa_export")


class ExportSafetyError(RuntimeError):
    """Raised when the export batch violates safety constraints."""


_WHATSAPP_PRIVATE_SUFFIX = "@s.whatsapp.net"
_WHATSAPP_GROUP_SUFFIX = "@g.us"
_EXPORT_TMP_DIR_ENV = "EXPORT_TMP_DIR"
_DEFAULT_EXPORT_TMP = "/tmp"


def _normalize_whatsapp_phone(phone: Optional[str]) -> str:
    if not phone:
        return ""
    local = (phone.split("@", 1)[0] if "@" in phone else phone).strip()
    if not local:
        return ""
    digits = re.sub(r"\D", "", local)
    if not digits:
        return local
    if local.startswith("00") and len(digits) > 2:
        normalized = f"+{digits[2:]}"
    elif local.startswith("+"):
        normalized = f"+{digits}"
    elif digits.startswith("8") and len(digits) == 11:
        normalized = f"+7{digits[1:]}"
    elif digits.startswith("7") and len(digits) == 11:
        normalized = f"+{digits}"
    elif digits.startswith("9") and len(digits) == 10:
        normalized = f"+7{digits}"
    else:
        normalized = f"+{digits}"
    return normalized


def _normalize_whatsapp_jid(raw: Optional[str]) -> str:
    if not raw:
        return ""
    candidate = raw.strip().lower()
    if not candidate:
        return ""
    if candidate.endswith(_WHATSAPP_GROUP_SUFFIX):
        return candidate
    phone = _normalize_whatsapp_phone(candidate)
    if not phone:
        return candidate
    return f"{phone.lower()}{_WHATSAPP_PRIVATE_SUFFIX}"


def _chat_label(dialog: Dict[str, Any]) -> str:
    jid = _normalize_whatsapp_jid(dialog.get("whatsapp_phone"))
    if jid:
        return jid.split("@", 1)[0] or "chat"
    contact_id = dialog.get("contact_id")
    if contact_id is not None:
        try:
            contact_int = int(contact_id)
        except (TypeError, ValueError):
            contact_int = None
        if contact_int is not None:
            return f"contact_{contact_int}"
    title = (dialog.get("title") or "").strip()
    if title:
        return title
    return "chat"


def _sanitize_filename(name: str) -> str:
    cleaned = INVALID_CHARS_RE.sub("_", name)
    cleaned = cleaned.replace("..", ".")
    cleaned = cleaned.strip().strip(".")
    if not cleaned:
        cleaned = "chat"
    if len(cleaned) > 80:
        cleaned = cleaned[:80]
    return cleaned


def _unique_name(base: str, existing: set[str]) -> str:
    candidate = base
    index = 1
    while candidate in existing:
        index += 1
        candidate = f"{base}_{index}"
    existing.add(candidate)
    return candidate


def _format_timestamp(ts_val: float, tz: ZoneInfo) -> str:
    try:
        dt = datetime.fromtimestamp(ts_val, tz=timezone.utc).astimezone(tz)
    except Exception:
        dt = datetime.now(tz)
    return dt.strftime("%d.%m.%Y, %H:%M:%S")


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    return WHITESPACE_RE.sub(" ", normalized)


def _ensure_export_workdir() -> Path:
    root = Path(os.getenv(_EXPORT_TMP_DIR_ENV, _DEFAULT_EXPORT_TMP)).expanduser()
    try:
        root.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise ExportSafetyError(f"export_tmp_unavailable:{root}") from exc
    if not root.is_dir() or not os.access(root, os.W_OK):
        raise ExportSafetyError(f"export_tmp_unwritable:{root}")
    return root

async def build_whatsapp_zip(
    tenant: int,
    since: datetime,
    until: datetime,
    limit_dialogs: Optional[int],
    agent_name: str,
    per_message_limit: Optional[int] = None,
    tz: ZoneInfo | None = None,
) -> tuple[Optional[io.BytesIO], Dict[str, Any]]:
    dialogs, meta = await db_module.fetch_whatsapp_dialogs(
        tenant, since, until, limit_dialogs, per_message_limit=per_message_limit
    )

    if not isinstance(meta, dict):
        meta = {}

    filtered_groups = int(meta.get("filtered_groups", 0)) if isinstance(meta, dict) else 0
    distinct_chat_ids = [str(cid) for cid in (meta.get("distinct_chat_ids") or []) if cid is not None]
    top_chats: Iterable[Dict[str, Any]] = meta.get("top_chats") or []
    if not top_chats:
        fallback = []
        for entry in meta.get("ranking") or []:
            chat_id = entry.get("chat_id") or entry.get("lead_id")
            if chat_id is None:
                continue
            fallback.append(
                {
                    "chat_id": str(chat_id),
                    "last_ts": float(entry.get("last_ts") or 0.0),
                }
            )
        top_chats = fallback

    top_five = list(top_chats)[:5]
    _log.info(
        "[wa_export] summary tenant=%s distinct_chat_ids=%s filtered_groups=%s top5=%s",
        tenant,
        len(distinct_chat_ids),
        filtered_groups,
        top_five,
    )

    if not dialogs or not distinct_chat_ids:
        empty_meta = dict(meta)
        empty_meta.update({"dialog_count": 0, "messages_exported": 0, "distinct_chat_ids": []})
        return None, {"dialog_count": 0, "message_count": 0, "meta": empty_meta}

    timezone_local = tz or EXPORT_TZ
    safe_agent = (agent_name or "Менеджер").strip() or "Менеджер"

    workdir = _ensure_export_workdir()
    existing_names: set[str] = set()
    dialog_count = 0
    message_count = 0

    with tempfile.TemporaryDirectory(dir=str(workdir), prefix="wa_export_") as temp_dir:
        archive_path = Path(temp_dir) / "whatsapp_export.zip"
        with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            for dialog in dialogs:
                messages = dialog.get("messages") or []
                if per_message_limit is not None and per_message_limit > 0:
                    messages = messages[-per_message_limit:]
                if not messages:
                    continue
                participant_name = _chat_label(dialog)
                safe_base = _sanitize_filename(participant_name)
                unique_name = _unique_name(safe_base, existing_names)
                lines: List[str] = []
                for message in messages:
                    text_raw = message.get("text") or ""
                    text = _normalize_text(text_raw)
                    direction = message.get("direction")
                    try:
                        direction_val = int(direction if direction is not None else 0)
                    except (TypeError, ValueError):
                        direction_val = 0
                    sender_name = participant_name if direction_val != 1 else safe_agent
                    ts_raw = message.get("ts")
                    try:
                        ts_val = float(ts_raw) if ts_raw is not None else 0.0
                    except (TypeError, ValueError):
                        ts_val = 0.0
                    timestamp = _format_timestamp(ts_val, timezone_local)
                    if not text:
                        text = "[без текста]"
                    lines.append(f"[{timestamp}] {sender_name}: {text}")
                if not lines:
                    continue
                content = "\n".join(lines) + "\n"
                archive.writestr(f"{unique_name}.txt", content.encode("utf-8"))
                dialog_count += 1
                message_count += len(lines)
        payload_bytes = archive_path.read_bytes()

    if dialog_count == 0:
        sanitized_meta = dict(meta)
        sanitized_meta.update({"dialog_count": 0, "messages_exported": 0})
        return None, {"dialog_count": 0, "message_count": 0, "meta": sanitized_meta}

    buf = io.BytesIO(payload_bytes)
    stats: Dict[str, Any] = {
        "dialog_count": dialog_count,
        "message_count": message_count,
        "meta": meta,
        "top_five": top_five,
        "distinct_chat_ids": distinct_chat_ids,
    }

    return buf, stats
