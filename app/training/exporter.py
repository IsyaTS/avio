import io
import json
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Tuple


PHONE_RE = re.compile(r"(?:(?:(?:\+|00)\d{1,3}[\s-]?)?(?:\(?\d{3}\)?[\s-]?)?\d[\d\s-]{6,})")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
URL_RE = re.compile(r"\bhttps?://[^\s]+", re.IGNORECASE)
WA_RE = re.compile(r"\b\d{5,}@s\.whatsapp\.net\b")
INVALID_FILENAME_RE = re.compile(r"[\\/:*?\"<>|]+")
WHITESPACE_RE = re.compile(r"\s+")


def scrub(text: str) -> str:
    """Replace PII tokens with placeholders without deleting surrounding text.

    - Phones -> <PHONE>
    - Emails -> <EMAIL>
    - URLs   -> <URL>
    - WhatsApp JIDs -> <PHONE>
    If result becomes empty, return "[REDACTED]".
    """
    if not text:
        return ""
    out = str(text)
    out = EMAIL_RE.sub("<EMAIL>", out)
    out = URL_RE.sub("<URL>", out)
    out = WA_RE.sub("<PHONE>", out)
    out = PHONE_RE.sub("<PHONE>", out)
    out = out.strip()
    return out if out else "[REDACTED]"


def dialogs_to_examples(dialog: Dict) -> Dict:
    """Normalize a dialog to strict alternation role/content for training.

    Expects dialog like {messages:[{role,content}, ...], meta:{...}} and
    returns a new object with the same structure but roles normalized and
    consecutive duplicates collapsed to preserve user->assistant pairs.
    """
    messages = dialog.get("messages") or []
    out: List[Dict] = []
    last_role = None
    for m in messages:
        role = (m.get("role") or "").strip().lower() or "user"
        content = (m.get("content") or m.get("text") or "").strip()
        if not content:
            continue
        if role not in ("user", "assistant"):
            role = "user"
        # Collapse consecutive turns of same role to keep alternation
        if out and out[-1]["role"] == role:
            out[-1]["content"] = (out[-1]["content"] + "\n" + content).strip()
        else:
            out.append({"role": role, "content": content})
        last_role = role
    return {"messages": out, "meta": dialog.get("meta") or {}}


def stream_jsonl(items: Iterable[Dict]) -> Iterator[bytes]:
    for obj in items:
        yield (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")


def stream_json_array(items: Iterable[Dict]) -> Iterator[bytes]:
    first = True
    yield b"["
    for obj in items:
        bs = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        if first:
            yield bs
            first = False
        else:
            yield b"," + bs
    yield b"]"


def _dialog_label(dialog: Dict) -> str:
    jid_raw = dialog.get("whatsapp_phone") or ""
    if isinstance(jid_raw, str):
        jid = jid_raw.strip().split("@", 1)[0].strip()
        if jid:
            return jid

    title = (dialog.get("title") or "").strip()
    if title:
        return title

    contact_id = dialog.get("contact_id")
    try:
        contact_val = int(contact_id) if contact_id is not None else None
    except (TypeError, ValueError):
        contact_val = None
    if contact_val and contact_val > 0:
        return f"contact_{contact_val}"

    lead_id = dialog.get("lead_id")
    try:
        lead_val = int(lead_id) if lead_id is not None else None
    except (TypeError, ValueError):
        lead_val = None
    if lead_val and lead_val > 0:
        return f"chat_{lead_val}"

    return "chat"


def _sanitize_filename(name: str) -> str:
    cleaned = INVALID_FILENAME_RE.sub("_", name)
    cleaned = WHITESPACE_RE.sub(" ", cleaned).strip()
    cleaned = cleaned.strip(".")
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


def _format_timestamp(raw_ts: object, tzinfo: timezone) -> str:
    if raw_ts in (None, ""):
        return "-"
    try:
        ts_val = float(raw_ts)
    except (TypeError, ValueError):
        return "-"
    if ts_val > 1_000_000_000_000:  # milliseconds
        ts_val /= 1000.0
    try:
        dt = datetime.fromtimestamp(ts_val, tz=timezone.utc).astimezone(tzinfo)
    except Exception:
        return "-"
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def build_text_archive(dialogs: List[Dict], tz: timezone = timezone.utc) -> Tuple[io.BytesIO, List[str]]:
    """Create an in-memory ZIP with one text file per dialog."""

    buffer = io.BytesIO()
    existing: set[str] = set()
    filenames: List[str] = []

    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for dialog in dialogs:
            messages = dialog.get("messages") or []
            if not messages:
                continue

            base_label = _sanitize_filename(_dialog_label(dialog))
            unique_name = _unique_name(base_label, existing)

            lines: List[str] = []
            header_parts: List[str] = []

            lead_id = dialog.get("lead_id")
            if lead_id is not None:
                header_parts.append(f"Lead ID: {lead_id}")
            contact_id = dialog.get("contact_id")
            if contact_id is not None:
                header_parts.append(f"Contact ID: {contact_id}")
            if header_parts:
                header_line = " | ".join(header_parts)
                lines.append(header_line)
                lines.append("-" * len(header_line))

            wrote_message = False
            for message in messages:
                role_raw = (message.get("role") or "").strip().lower()
                role = role_raw if role_raw in {"user", "assistant"} else "user"
                label = "User" if role == "user" else "Agent"
                ts_str = _format_timestamp(message.get("ts"), tz)
                content_raw = (message.get("content") or message.get("text") or "").strip()
                if not content_raw:
                    continue
                content = scrub(content_raw)
                if not content:
                    continue
                lines.append(f"[{ts_str}] {label}: {content}")
                wrote_message = True

            if not wrote_message:
                continue

            archive.writestr(f"{unique_name}.txt", "\n".join(lines).encode("utf-8"))
            filenames.append(f"{unique_name}.txt")

    buffer.seek(0)
    return buffer, filenames


def build_zip_temp(dialogs: List[Dict]) -> str:
    """Write a temporary ZIP file with one JSONL per dialog: lead_<id>.jsonl.

    Returns the file path. Caller is responsible for removing the file later.
    """
    tmp = tempfile.NamedTemporaryFile(prefix="dialogs_", suffix=".zip", delete=False)
    tmp.close()
    with zipfile.ZipFile(tmp.name, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for d in dialogs:
            lid_value = d.get("meta", {}).get("lead_id")
            if lid_value is None:
                lid_value = d.get("lead_id")
            try:
                lead_id = int(lid_value)
            except (TypeError, ValueError):
                raise ValueError("lead_id is required for export dialogs")
            if lead_id <= 0:
                raise ValueError("lead_id must be positive for export dialogs")
            name = f"lead_{lead_id}.jsonl"
            data = json.dumps(d, ensure_ascii=False).encode("utf-8") + b"\n"
            zf.writestr(name, data)
    return tmp.name
