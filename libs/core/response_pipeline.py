from __future__ import annotations

import asyncio
import json
import mimetypes
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from libs.core.sales_core import ask_llm, build_llm_messages, read_tenant_config, tenant_dir
from libs.core.common import default_fallback_reply


@dataclass
class PipelinePhoto:
    photo_id: str
    title: str
    path: str | None = None
    mime: str | None = None


@dataclass
class PipelineResult:
    reply_text: str
    photos: list[PipelinePhoto] = field(default_factory=list)
    source: str = "llm"


def _photo_auto_config(tenant_id: int) -> tuple[bool, int]:
    try:
        cfg = read_tenant_config(int(tenant_id))
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


def _load_photo_manifest(tenant_id: int) -> list[dict[str, Any]]:
    try:
        path = tenant_dir(int(tenant_id)) / "uploads" / "photos" / "manifest.json"
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


def _normalize_photo_candidates(tenant_id: int, channel: str) -> list[dict[str, Any]]:
    entries = _load_photo_manifest(tenant_id)
    normalized: list[dict[str, Any]] = []
    channel_norm = channel.strip().lower()
    for entry in entries:
        photo_id = str(entry.get("id") or "").strip()
        if not photo_id:
            continue
        if not entry.get("auto"):
            continue
        channels_raw = entry.get("channels") if isinstance(entry.get("channels"), list) else []
        channels = [str(ch).strip().lower() for ch in channels_raw if str(ch).strip()]
        if channels and channel_norm not in channels:
            continue
        try:
            priority = int(entry.get("priority") or 0)
        except Exception:
            priority = 0
        normalized.append(
            {
                "id": photo_id,
                "title": entry.get("title") or entry.get("original") or entry.get("filename") or photo_id,
                "filename": entry.get("filename") or entry.get("original") or "",
                "tags": entry.get("tags") or [],
                "usage": entry.get("usage") or "",
                "priority": priority,
                "path": entry.get("path"),
                "mime": entry.get("mime"),
            }
        )
    normalized.sort(key=lambda item: int(item.get("priority") or 0), reverse=True)
    return normalized


def _score_photo_candidate(candidate: Mapping[str, Any], text: str) -> int:
    hay = (text or "").lower()
    if not hay:
        return 0
    tokens: list[str] = []
    for key in ("title", "usage"):
        value = candidate.get(key)
        if isinstance(value, str) and value.strip():
            tokens.extend(re.split(r"[,\n;]+", value.lower()))
    tags = candidate.get("tags")
    if isinstance(tags, list):
        tokens.extend(str(tag).lower() for tag in tags if str(tag).strip())
    score = 0
    for token in tokens:
        clean = token.strip()
        if clean and clean in hay:
            score += 1
    return score


def _select_photos_by_tags(
    candidates: list[dict[str, Any]],
    user_text: str,
    reply_text: str,
    max_count: int,
) -> list[dict[str, Any]]:
    scored: list[tuple[int, int, dict[str, Any]]] = []
    combined = f"{user_text}\n{reply_text}".strip()
    for item in candidates:
        score = _score_photo_candidate(item, combined)
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


def _guess_photo_mime(photo: Mapping[str, Any]) -> str:
    candidate = str(photo.get("path") or photo.get("filename") or photo.get("title") or "")
    if candidate:
        mime, _ = mimetypes.guess_type(candidate)
        if mime:
            return mime
    return "image/jpeg"


def _normalize_history(history: Sequence[Mapping[str, Any]] | None) -> list[dict[str, str]]:
    if not history:
        return []
    normalized: list[dict[str, str]] = []
    for item in history[-12:]:
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = str(item.get("content") or item.get("text") or "").strip()
        if not content:
            continue
        normalized.append({"role": role, "content": content})
    return normalized


def _system_prompt_only(messages: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    if messages and isinstance(messages[0], Mapping) and messages[0].get("role") == "system":
        return {"role": "system", "content": str(messages[0].get("content") or "")}
    return {"role": "system", "content": ""}


async def run_response_pipeline(
    *,
    tenant_id: int,
    channel: str,
    user_text: str,
    history: Sequence[Mapping[str, Any]] | None = None,
    contact_id: int = 0,
    enable_photos: bool | None = None,
    timeout_seconds: float | None = None,
    log_fn: Callable[[str], Any] | None = None,
) -> PipelineResult:
    normalized_history = _normalize_history(history)
    try:
        base_messages = await build_llm_messages(
            contact_id,
            user_text,
            channel,
            tenant=tenant_id,
        )
    except Exception:
        base_messages = []

    messages: list[dict[str, str]] = [_system_prompt_only(base_messages)]
    messages.extend(normalized_history)
    messages.append({"role": "user", "content": user_text})

    try:
        if timeout_seconds and timeout_seconds > 0:
            reply = await asyncio.wait_for(
                ask_llm(messages, tenant=tenant_id, contact_id=contact_id, channel=channel),
                timeout=timeout_seconds,
            )
        else:
            reply = await ask_llm(messages, tenant=tenant_id, contact_id=contact_id, channel=channel)
    except asyncio.TimeoutError:
        if log_fn:
            log_fn(
                "event=smart_reply_timeout channel=%s tenant=%s contact=%s timeout=%.1f"
                % (channel, tenant_id, contact_id or 0, float(timeout_seconds or 0))
            )
        reply = ""
    except Exception as exc:
        if log_fn:
            log_fn(
                "event=smart_reply_failed channel=%s tenant=%s contact=%s stage=pipeline error=%s fallback=1"
                % (channel, tenant_id, contact_id or 0, exc)
            )
        reply = ""
    reply_text = str(reply or "").strip()
    if not reply_text:
        reply_text = default_fallback_reply(tenant_id)

    photos: list[PipelinePhoto] = []
    auto_enabled, max_count = _photo_auto_config(tenant_id)
    if enable_photos is False:
        auto_enabled = False
    if auto_enabled and max_count > 0:
        candidates = _normalize_photo_candidates(tenant_id, channel)
        selected = _select_photos_by_tags(candidates, user_text, reply_text, max_count)
        for photo in selected:
            photos.append(
                PipelinePhoto(
                    photo_id=str(photo.get("id")),
                    title=str(photo.get("title") or ""),
                    path=str(photo.get("path") or "") or None,
                    mime=str(photo.get("mime") or _guess_photo_mime(photo)),
                )
            )

    return PipelineResult(reply_text=reply_text, photos=photos)
