from __future__ import annotations

import json
import mimetypes
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping


@dataclass(frozen=True)
class AutoPhotoDeps:
    app_base_url: str
    read_tenant_config_fn: Callable[[int], Mapping[str, Any] | dict[str, Any]]
    tenant_dir_fn: Callable[[int], Path]
    log_fn: Callable[..., None]


def photo_auto_config(tenant_id: int, *, deps: AutoPhotoDeps) -> tuple[bool, int]:
    try:
        cfg = deps.read_tenant_config_fn(int(tenant_id))
    except Exception:
        cfg = {}
    if not isinstance(cfg, Mapping):
        return False, 1
    behavior = cfg.get("behavior")
    if not isinstance(behavior, Mapping):
        return False, 1
    enabled = bool(behavior.get("auto_photo_enabled"))
    try:
        max_count = int(behavior.get("auto_photo_max") or 0)
    except Exception:
        max_count = 0
    if max_count <= 0:
        max_count = 1
    return enabled, max_count


def asset_actions_config(tenant_id: int, *, deps: AutoPhotoDeps) -> tuple[bool, int]:
    try:
        cfg = deps.read_tenant_config_fn(int(tenant_id))
    except Exception:
        cfg = {}
    if not isinstance(cfg, Mapping):
        return False, 1
    behavior = cfg.get("behavior")
    if not isinstance(behavior, Mapping):
        return False, 1
    enabled = bool(
        behavior.get("asset_actions_enabled")
        if "asset_actions_enabled" in behavior
        else behavior.get("auto_photo_enabled")
    )
    try:
        max_count = int(behavior.get("asset_actions_max_per_reply") or 0)
    except Exception:
        max_count = 0
    if max_count <= 0:
        try:
            max_count = int(behavior.get("auto_photo_max") or 0)
        except Exception:
            max_count = 0
    if max_count <= 0:
        max_count = 1
    return enabled, max_count


def load_photo_manifest(tenant_id: int, *, deps: AutoPhotoDeps) -> list[dict[str, Any]]:
    try:
        path = deps.tenant_dir_fn(int(tenant_id)) / "uploads" / "photos" / "manifest.json"
    except Exception:
        return []
    if not path.exists() or not path.is_file():
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception:
        return []
    if isinstance(raw, list):
        return [entry for entry in raw if isinstance(entry, dict)]
    return []


def tenant_public_key(tenant_id: int, *, deps: AutoPhotoDeps) -> str:
    try:
        cfg = deps.read_tenant_config_fn(int(tenant_id))
    except Exception:
        return ""
    if not isinstance(cfg, Mapping):
        return ""
    passport = cfg.get("passport")
    if isinstance(passport, Mapping):
        key = str(passport.get("public_key") or "").strip()
        if key:
            return key
    return str(cfg.get("public_key") or "").strip()


def build_photo_public_path(tenant_id: int, photo_id: str, *, deps: AutoPhotoDeps) -> str:
    key = tenant_public_key(tenant_id, deps=deps)
    if not key:
        return ""
    return f"/pub/files/photos/{photo_id}?tenant={tenant_id}&k={key}"


def build_photo_public_url(tenant_id: int, photo_id: str, *, deps: AutoPhotoDeps) -> str:
    base = (deps.app_base_url or "").strip().rstrip("/")
    if not base:
        return ""
    key = tenant_public_key(tenant_id, deps=deps)
    if not key:
        return ""
    return f"{base}/pub/files/photos/{photo_id}?tenant={tenant_id}&k={key}"


def collect_outgoing_attachments(
    item: Mapping[str, Any],
    tenant_id: int,
    *,
    deps: AutoPhotoDeps,
) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    _append_single_attachment(attachments, item.get("attachment"), tenant_id, deps=deps)
    raw_list = item.get("attachments")
    if isinstance(raw_list, list):
        for att in raw_list:
            if isinstance(att, Mapping):
                _append_single_attachment(attachments, att, tenant_id, deps=deps)
    _append_photo_ids(attachments, item, tenant_id, deps=deps)
    return attachments


def _append_single_attachment(
    attachments: list[dict[str, Any]],
    raw_attachment: Any,
    tenant_id: int,
    *,
    deps: AutoPhotoDeps,
) -> None:
    if not isinstance(raw_attachment, Mapping):
        return
    entry = dict(raw_attachment)
    if not entry.get("url"):
        photo_id = str(entry.get("photo_id") or entry.get("id") or "").strip()
        if photo_id:
            entry["url"] = build_photo_public_url(
                tenant_id, photo_id, deps=deps
            ) or build_photo_public_path(tenant_id, photo_id, deps=deps)
        elif entry.get("path"):
            entry["url"] = str(entry.get("path"))
    attachments.append(entry)


def _append_photo_ids(
    attachments: list[dict[str, Any]],
    item: Mapping[str, Any],
    tenant_id: int,
    *,
    deps: AutoPhotoDeps,
) -> None:
    photo_id = str(item.get("photo_id") or "").strip()
    if photo_id:
        attachments.append(_photo_attachment(tenant_id, photo_id, deps=deps))
    raw_ids = item.get("photo_ids")
    if isinstance(raw_ids, list):
        for pid in raw_ids:
            pid_str = str(pid or "").strip()
            if pid_str:
                attachments.append(_photo_attachment(tenant_id, pid_str, deps=deps))


def _photo_attachment(
    tenant_id: int,
    photo_id: str,
    *,
    deps: AutoPhotoDeps,
) -> dict[str, Any]:
    return {
        "type": "image",
        "photo_id": photo_id,
        "url": build_photo_public_url(tenant_id, photo_id, deps=deps)
        or build_photo_public_path(tenant_id, photo_id, deps=deps),
    }


def normalize_photo_candidates(
    tenant_id: int,
    channel: str,
    *,
    deps: AutoPhotoDeps,
) -> list[dict[str, Any]]:
    entries = load_photo_manifest(tenant_id, deps=deps)
    normalized: list[dict[str, Any]] = []
    channel_norm = channel.strip().lower()
    for entry in entries:
        candidate = _normalize_photo_candidate(entry, channel_norm)
        if candidate:
            normalized.append(candidate)
    normalized.sort(key=lambda item: int(item.get("priority") or 0), reverse=True)
    return normalized


def _normalize_photo_candidate(entry: Mapping[str, Any], channel_norm: str) -> dict[str, Any] | None:
    photo_id = str(entry.get("id") or "").strip()
    if not photo_id or not entry.get("auto"):
        return None
    channels_raw = entry.get("channels") if isinstance(entry.get("channels"), list) else []
    channels = [str(ch).strip().lower() for ch in channels_raw if str(ch).strip()]
    if channels and channel_norm not in channels:
        return None
    try:
        priority = int(entry.get("priority") or 0)
    except Exception:
        priority = 0
    return {
        "id": photo_id,
        "title": entry.get("title") or entry.get("original") or entry.get("filename") or photo_id,
        "filename": entry.get("filename") or entry.get("original") or "",
        "tags": entry.get("tags") or [],
        "usage": entry.get("usage") or "",
        "priority": priority,
        "path": entry.get("path"),
    }


def select_photos_by_tags(
    candidates: list[dict[str, Any]],
    user_text: str,
    reply_text: str,
    max_count: int,
) -> list[dict[str, Any]]:
    scored: list[tuple[int, int, dict[str, Any]]] = []
    combined = f"{user_text}\n{reply_text}".strip()
    for item in candidates:
        score = score_photo_candidate(item, combined)
        if score <= 0:
            continue
        try:
            priority = int(item.get("priority") or 0)
        except Exception:
            priority = 0
        scored.append((score, priority, item))
    if not scored:
        return []
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [item[2] for item in scored[:max_count]]


def score_photo_candidate(candidate: Mapping[str, Any], text: str) -> int:
    hay = (text or "").lower()
    if not hay:
        return 0
    score = 0
    for token in _photo_candidate_tokens(candidate):
        clean = token.strip()
        if clean and clean in hay:
            score += 1
    return score


def _photo_candidate_tokens(candidate: Mapping[str, Any]) -> list[str]:
    tokens: list[str] = []
    for key in ("title", "usage"):
        value = candidate.get(key)
        if isinstance(value, str) and value.strip():
            tokens.extend(re.split(r"[,\n;]+", value.lower()))
    tags = candidate.get("tags")
    if isinstance(tags, list):
        tokens.extend(str(tag).lower() for tag in tags if str(tag).strip())
    return tokens


def guess_photo_mime(photo: Mapping[str, Any]) -> str:
    candidate = str(photo.get("path") or photo.get("url") or photo.get("name") or "")
    if candidate:
        mime, _ = mimetypes.guess_type(candidate)
        if mime:
            return mime
    return "image/jpeg"


def extract_photo_ids(reply_text: str, allowed: set[str], max_count: int) -> list[str]:
    cleaned = (reply_text or "").strip()
    if not cleaned:
        return []
    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if not match:
        return []
    try:
        payload = json.loads(match.group(0))
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []
    raw_ids = payload.get("photo_ids")
    if not isinstance(raw_ids, list):
        return []
    return _filter_photo_ids(raw_ids, allowed, max_count)


def _filter_photo_ids(raw_ids: list[Any], allowed: set[str], max_count: int) -> list[str]:
    seen: list[str] = []
    for item in raw_ids:
        candidate = str(item).strip()
        if not candidate or candidate not in allowed or candidate in seen:
            continue
        seen.append(candidate)
        if len(seen) >= max_count:
            break
    return seen


async def select_auto_photos(
    tenant_id: int,
    channel: str,
    user_text: str,
    reply_text: str,
    *,
    lead_id: int = 0,
    context: Mapping[str, Any] | None = None,
    deps: AutoPhotoDeps,
) -> list[dict[str, Any]]:
    asset_enabled, asset_max_count = asset_actions_config(tenant_id, deps=deps)
    if asset_enabled:
        try:
            from apps.worker.services import asset_action_runtime

            planned = await asset_action_runtime.select_asset_action_attachments(
                tenant_id=int(tenant_id),
                lead_id=int(lead_id or 0),
                channel=channel,
                user_text=user_text,
                reply_text=reply_text,
                context=context,
                app_base_url=deps.app_base_url,
                public_key_fn=lambda current_tenant: tenant_public_key(current_tenant, deps=deps),
                max_assets=asset_max_count,
                log_fn=deps.log_fn,
            )
        except Exception as exc:
            deps.log_fn(
                "event=asset_action_select_failed tenant=%s channel=%s error=%s",
                tenant_id,
                channel,
                exc,
            )
            planned = []
        if planned:
            return planned

    enabled, max_count = photo_auto_config(tenant_id, deps=deps)
    if not enabled:
        return []
    candidates = normalize_photo_candidates(tenant_id, channel, deps=deps)
    if not candidates:
        return []
    attachments = _attachments_from_tag_selection(
        tenant_id,
        channel,
        select_photos_by_tags(candidates, user_text, reply_text, max_count),
        max_count,
        deps=deps,
    )
    if attachments:
        deps.log_fn(
            "event=auto_photo_selected tenant=%s channel=%s method=tags count=%s ids=%s",
            tenant_id,
            channel,
            len(attachments),
            [att.get("path") or att.get("url") for att in attachments],
        )
        return attachments
    deps.log_fn("event=auto_photo_candidates tenant=%s channel=%s count=%s", tenant_id, channel, len(candidates))
    return []


def _attachments_from_tag_selection(
    tenant_id: int,
    channel: str,
    selected: list[dict[str, Any]],
    max_count: int,
    *,
    deps: AutoPhotoDeps,
) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    for photo in selected:
        url = build_photo_public_url(tenant_id, photo["id"], deps=deps)
        if channel == "telegram" and not url:
            continue
        if not url:
            url = build_photo_public_path(tenant_id, photo["id"], deps=deps)
        attachments.append(
            {
                "type": "image",
                "url": url,
                "path": photo.get("path"),
                "name": photo.get("filename") or photo.get("path") or photo.get("title"),
                "mime": guess_photo_mime(photo),
            }
        )
        if len(attachments) >= max_count:
            break
    return attachments
