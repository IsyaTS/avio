from __future__ import annotations

import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

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
    title = (dialog.get("title") or "").strip()
    if title:
        return title
    contact_id = dialog.get("contact_id")
    if contact_id is not None:
        try:
            contact_int = int(contact_id)
        except (TypeError, ValueError):
            contact_int = None
        if contact_int is not None:
            return f"contact_{contact_int}"
    chat_identifier = dialog.get("chat_id")
    if isinstance(chat_identifier, str) and chat_identifier.strip():
        cleaned = chat_identifier.strip().replace(":", "_")
        return cleaned or "chat"
    lead_id = dialog.get("lead_id")
    if lead_id is not None:
        try:
            lead_int = int(lead_id)
        except (TypeError, ValueError):
            lead_int = None
        if lead_int is not None:
            return f"lead_{lead_int}"
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

def _allocate_export_file(workdir: Path) -> Path:
    fd, tmp_path = tempfile.mkstemp(dir=str(workdir), prefix="wa_export_", suffix=".zip")
    os.close(fd)
    return Path(tmp_path)


async def build_whatsapp_zip(
    tenant: int,
    since: datetime,
    until: datetime,
    limit_dialogs: Optional[int],
    agent_name: str,
    per_message_limit: Optional[int] = None,
    tz: ZoneInfo | None = None,
    batch_size_dialogs: int = 200,
) -> tuple[Optional[Path], Dict[str, Any]]:
    try:
        tenant_val = int(tenant)
    except (TypeError, ValueError):
        tenant_val = int(tenant or 0)

    def _to_epoch(value: datetime) -> float:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).timestamp()

    since_ts = _to_epoch(since)
    until_ts = _to_epoch(until)

    dialog_stream, meta = await db_module.stream_whatsapp_dialogs(
        tenant_val=tenant_val,
        since_ts=since_ts,
        until_ts=until_ts,
        limit_dialogs=limit_dialogs,
        per_message_limit=per_message_limit,
        batch_size_dialogs=batch_size_dialogs,
    )

    if not isinstance(meta, dict):
        meta = {}

    meta.setdefault("since_ts", since_ts)
    meta.setdefault("until_ts", until_ts)
    meta["batch_size_dialogs"] = batch_size_dialogs
    if limit_dialogs is not None:
        meta["limit_dialogs"] = limit_dialogs
    if per_message_limit is not None:
        meta["per_message_limit"] = per_message_limit if per_message_limit > 0 else None

    filtered_groups = int(meta.get("filtered_groups", 0))
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

    predicted_dialogs = int(meta.get("dialog_count") or 0)
    predicted_messages = int(meta.get("messages_exported") or 0)

    top_five = list(top_chats)[:5]
    _log.info(
        "[wa_export] summary tenant=%s distinct_chat_ids=%s filtered_groups=%s top5=%s",
        tenant,
        len(distinct_chat_ids),
        filtered_groups,
        top_five,
    )

    if predicted_dialogs <= 0 or predicted_messages <= 0 or not distinct_chat_ids:
        empty_meta = dict(meta)
        empty_meta.update({"dialog_count": 0, "messages_exported": 0, "distinct_chat_ids": []})
        return None, {"dialog_count": 0, "message_count": 0, "meta": empty_meta, "top_five": top_five}

    timezone_local = tz or EXPORT_TZ
    safe_agent = (agent_name or "Менеджер").strip() or "Менеджер"

    stats: Dict[str, Any] = {
        "dialog_count": predicted_dialogs,
        "message_count": predicted_messages,
        "meta": meta,
        "top_five": top_five,
        "distinct_chat_ids": distinct_chat_ids,
    }

    workdir = _ensure_export_workdir()

    existing_names: set[str] = set()
    actual_dialogs = 0
    actual_messages = 0

    zip_path = _allocate_export_file(workdir)
    try:
        with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
            async for dialog, message_batches in dialog_stream:
                participant_name = _chat_label(dialog)
                safe_base = _sanitize_filename(participant_name)
                unique_name = _unique_name(safe_base, existing_names)
                messages_written = 0
                entry_handle = None
                try:
                    async for batch in message_batches:
                        for message in batch:
                            if entry_handle is None:
                                entry_handle = archive.open(f"{unique_name}.txt", "w")
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
                            line = f"[{timestamp}] {sender_name}: {text}\n"
                            entry_handle.write(line.encode("utf-8"))
                            messages_written += 1
                            actual_messages += 1
                finally:
                    if entry_handle is not None:
                        entry_handle.close()

                if messages_written:
                    actual_dialogs += 1
                else:
                    existing_names.discard(unique_name)

        if actual_dialogs <= 0 or actual_messages <= 0:
            try:
                zip_path.unlink(missing_ok=True)
            except Exception:
                _log.warning("[wa_export] failed_to_remove_empty_zip path=%s", zip_path)
            stats.update({"dialog_count": 0, "message_count": 0})
            meta["dialog_count"] = 0
            meta["messages_exported"] = 0
            return None, stats

        stats["dialog_count"] = actual_dialogs
        stats["message_count"] = actual_messages
        meta["dialog_count"] = actual_dialogs
        meta["messages_exported"] = actual_messages
        return zip_path, stats
    except Exception:
        try:
            zip_path.unlink(missing_ok=True)
        except Exception as cleanup_exc:
            _log.warning("[wa_export] failed_to_cleanup_zip path=%s error=%s", zip_path, cleanup_exc)
        raise
