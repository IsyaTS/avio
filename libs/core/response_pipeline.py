from __future__ import annotations

import asyncio
import hashlib
import json
import mimetypes
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from libs.core.learning.service import prepare_runtime_policy_hint
from libs.core.services.contextual_prompt_builder import build_contextual_cases_block_for_runtime
from libs.core.services.niche_brain_v2 import NicheBrainV2Context, build_niche_brain_v2_block
from libs.core.sales_core import ask_llm, build_llm_messages, read_tenant_config, tenant_dir
from libs.core.common import default_fallback_reply
from libs.core.training import dialog_retriever
from libs.core.training import retriever as training_retriever
from libs.core.reply_trace import ReplyTrace

_LOCATION_INTENT_RE = re.compile(r"\b(?:где|адрес|магазин|посмотреть|находитесь|выбрать|шоурум)\b", re.I)
_CITY_HINT_RE = re.compile(r"\b(?:уф[аеыу]?|стерлитамак[аеу]?|салават[аеу]?|ишимба[йеяю]|оренбург[аеу]?|казан[ьи]|в городе|мой город|наш город|район)\b", re.I)
_UNSAFE_LOCATION_REPLY_RE = re.compile(
    r"(?:\b(?:уф[аеыу]?|стерлитамак[аеу]?|салават[аеу]?|ишимба[йеяю]|оренбург[аеу]?|казан[ьи]|коммунистическая|менделеева|скидк|телеграм|telegram|ватсап|whatsapp)\b|\b(?:адрес|магазин|филиал|выезд|доставк|телефон)\w*\b|@\w+|\d[\d\s().+-]{6,}\d)",
    re.I,
)
_UNSAFE_CONCRETE_LOCATION_REPLY_RE = re.compile(
    r"(?:\b(?:уф[аеыу]?|стерлитамак[аеу]?|салават[аеу]?|ишимба[йеяю]|оренбург[аеу]?|казан[ьи]|коммунистическая|менделеева|скидк|телеграм|telegram|ватсап|whatsapp)\b|\b(?:адрес|филиал|выезд|доставк|телефон)\w*\b|@\w+|\d[\d\s().+-]{6,}\d)",
    re.I,
)
_LOCATION_OR_CATALOG_PROMISE_RE = re.compile(
    r"\b(?:каталог\w*|магазин\w*|посмотреть|показать|отправ(?:лю|им|ить)|пришл(?:ю|ем|ите)|скин(?:у|ем|уть))\b",
    re.I,
)
_CITY_CLARIFICATION_REPLY_RE = re.compile(
    r"(?:\b(?:в\s+)?каком\s+городе\b|\b(?:подскажите|уточните|напишите|назовите)\b[^\n?.!]{0,80}\bгород\b|\bгород\b[^\n?.!]{0,80}\?)",
    re.I,
)
_CITY_CLARIFICATION_FALLBACK = "Здравствуйте. Подскажите, пожалуйста, в каком городе хотите посмотреть каталог?"


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
    trace: ReplyTrace | None = None


def _preview(text: str, limit: int = 220) -> str:
    text = str(text or "").replace("\n", " ").replace("\r", " ").strip()
    if len(text) <= limit:
        return text
    return f"{text[:limit]}…"


def _hash_text(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:16]


def _resolve_model_hint(tenant_id: int) -> str | None:
    try:
        cfg = read_tenant_config(int(tenant_id))
        if not isinstance(cfg, Mapping):
            return None
        behavior = cfg.get("behavior")
        if not isinstance(behavior, Mapping):
            return None
        model = behavior.get("smart_reply_model") or behavior.get("llm_model")
        return str(model) if model else None
    except Exception:
        return None


def _log_trace(
    log_fn: Callable[[str], Any] | None,
    trace: ReplyTrace | None,
    *,
    contextual_block_applied: bool,
    policy_hint_applied: bool,
    dialog_examples_applied: bool,
    legacy_examples_applied: bool,
) -> None:
    if not log_fn or trace is None:
        return
    payload = trace.to_dict()
    log_fn(
        "event=reply_trace "
        + " ".join(
            f"{key}={value!s}"
            for key, value in (
                ("trace_id", payload.get("trace_id")),
                ("tenant_id", payload.get("tenant_id")),
                ("lead_id", payload.get("lead_id")),
                ("channel", payload.get("channel")),
                ("source", payload.get("pipeline_source")),
                ("fallback", int(bool(payload.get("fallback_used")))),
                ("fallback_source", payload.get("fallback_source") or "-"),
                ("contextual_block", int(bool(contextual_block_applied))),
                ("dialog_examples", int(bool(dialog_examples_applied))),
                ("legacy_examples", int(bool(legacy_examples_applied))),
                ("policy_hint", int(bool(policy_hint_applied))),
                ("catalog_context_count", payload.get("catalog_context_count")),
                ("latency_ms", payload.get("latency_ms")),
                ("user_preview", _preview(str(payload.get("user_text") or ""), limit=80)),
                ("reply_preview", _preview(str(payload.get("reply_text") or ""))),
                ("quality_violations", ",".join(payload.get("quality_violations") or [])),
            )
            if value is not None
        )
    )


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


def _append_system_block(message: dict[str, str], block: str) -> None:
    text = str(block or "").strip()
    if not text:
        return
    system_text = str(message.get("content") or "").strip()
    message["content"] = f"{system_text}\n\n{text}".strip()


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
    trace_id = str(uuid.uuid4())
    start_ts = time.perf_counter()
    lead_id = int(contact_id or 0)
    contextual_used = False
    dialog_examples_used = False
    legacy_examples_used = False
    policy_hint_used = False
    catalog_context_count = None
    fallback_source: str | None = None

    normalized_history = _normalize_history(history)
    history_count = len(normalized_history)
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
    try:
        contextual_ctx = await build_contextual_cases_block_for_runtime(
            tenant_id=int(tenant_id),
            user_text=user_text,
            history=normalized_history,
            contact_id=contact_id,
            channel=channel,
            log_fn=log_fn,
        )
    except Exception:
        contextual_ctx = {"enabled": False, "applied": False, "block": ""}
    contextual_block = str(contextual_ctx.get("block") or "").strip()
    if contextual_block:
        contextual_used = True
        _append_system_block(messages[0], contextual_block)

    try:
        dialog_block = _build_dialog_training_block(
            tenant_id=int(tenant_id),
            user_text=user_text,
        )
    except Exception:
        dialog_block = ""
    if dialog_block:
        dialog_examples_used = True
        _append_system_block(messages[0], dialog_block)

    examples_block = ""
    if _legacy_pair_training_enabled(int(tenant_id)) or not _dialog_dataset_available(int(tenant_id)):
        try:
            examples_block = await training_retriever.build_examples_block_async(
                int(tenant_id),
                user_text,
            )
        except Exception:
            examples_block = ""
    if examples_block:
        legacy_examples_used = True
        _append_system_block(messages[0], examples_block)

    try:
        policy_ctx = await prepare_runtime_policy_hint(
            tenant_id=tenant_id,
            lead_id=contact_id,
            channel=channel,
            user_text=user_text,
            normalized_history=normalized_history,
            log_fn=log_fn,
        )
    except Exception:
        policy_ctx = {"enabled": False, "policy_block": ""}
    policy_block = str(policy_ctx.get("policy_block") or "").strip()
    if policy_block:
        policy_hint_used = True
        _append_system_block(messages[0], policy_block)

    try:
        niche_ctx = NicheBrainV2Context(
            tenant_id=int(tenant_id),
            channel=channel,
            user_text=user_text,
            history=normalized_history,
            contact_id=contact_id,
            tenant_config=read_tenant_config(int(tenant_id)),
        )
        niche_result = build_niche_brain_v2_block(niche_ctx)
        if niche_result.applied and niche_result.block:
            _append_system_block(messages[0], niche_result.block)
    except Exception as exc:
        if log_fn:
            log_fn(
                "event=niche_brain_v2_failed tenant=%s channel=%s contact=%s error=%s"
                % (tenant_id, channel, contact_id or 0, exc)
            )

    messages.extend(normalized_history)
    messages.append({"role": "user", "content": user_text})

    source = "llm"
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
        source = "fallback_timeout"
        fallback_source = "timeout"
    except Exception as exc:
        if log_fn:
            log_fn(
                "event=smart_reply_failed channel=%s tenant=%s contact=%s stage=pipeline error=%s fallback=1"
                % (channel, tenant_id, contact_id or 0, exc)
            )
        reply = ""
        source = "fallback_error"
        fallback_source = "exception"
    reply_metadata = getattr(reply, "reply_metadata", None)
    if isinstance(reply_metadata, Mapping):
        metadata_source = str(reply_metadata.get("source") or "").strip()
        if metadata_source:
            source = metadata_source
        metadata_fallback_source = str(reply_metadata.get("fallback_source") or "").strip()
        if metadata_fallback_source and fallback_source is None:
            fallback_source = metadata_fallback_source
    else:
        reply_metadata = {}
    reply_text = str(reply or "").strip()
    if not reply_text:
        reply_text = default_fallback_reply(tenant_id)
        if source == "llm":
            source = "fallback_empty"
            fallback_source = "empty"
    guarded_reply = reply_text
    if source != "rule_fallback":
        guarded_reply = _guard_unsafe_dialog_training_reply(user_text=user_text, reply_text=reply_text)
    if guarded_reply != reply_text:
        reply_text = guarded_reply
        source = "guarded_location_context"
        if fallback_source is None:
            fallback_source = "guarded_location_context"

    photos: list[PipelinePhoto] = []
    auto_enabled, max_count = _photo_auto_config(tenant_id)
    if enable_photos is False:
        auto_enabled = False
    if auto_enabled and max_count > 0:
        candidates = _normalize_photo_candidates(tenant_id, channel)
        selected = _select_photos_by_tags(candidates, user_text, reply_text, max_count)
        catalog_context_count = len(candidates)
        for photo in selected:
            photos.append(
                PipelinePhoto(
                    photo_id=str(photo.get("id")),
                    title=str(photo.get("title") or ""),
                    path=str(photo.get("path") or "") or None,
                    mime=str(photo.get("mime") or _guess_photo_mime(photo)),
                )
            )
    latency_ms = int((time.perf_counter() - start_ts) * 1000)
    fallback_used = (
        bool(reply_metadata.get("fallback_used")) if isinstance(reply_metadata, Mapping) else False
    ) or source.startswith("fallback") or source == "guarded_location_context"
    quality_violations = ["fallback_used"] if fallback_used else []
    trace = ReplyTrace(
        trace_id=trace_id,
        tenant_id=int(tenant_id),
        lead_id=lead_id if lead_id > 0 else None,
        channel=str(channel).strip(),
        user_text=str(user_text or ""),
        history_count=history_count,
        pipeline_source=source,
        reply_text=reply_text,
        fallback_used=fallback_used,
        fallback_source=fallback_source,
        catalog_context_count=catalog_context_count,
        dialog_examples_used=dialog_examples_used,
        legacy_examples_used=legacy_examples_used,
        policy_hint_used=policy_hint_used,
        prompt_hash=_hash_text(_system_prompt_only(messages).get("content", "")),
        model=_resolve_model_hint(int(tenant_id)),
        latency_ms=latency_ms,
        quality_violations=quality_violations,
    )
    _log_trace(
        log_fn,
        trace,
        contextual_block_applied=contextual_used,
        policy_hint_applied=policy_hint_used,
        dialog_examples_applied=dialog_examples_used,
        legacy_examples_applied=legacy_examples_used,
    )
    return PipelineResult(reply_text=reply_text, photos=photos, source=source, trace=trace)


def _build_dialog_training_block(*, tenant_id: int, user_text: str) -> str:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = {}
    learning = cfg.get("learning") if isinstance(cfg, Mapping) else {}
    dialog_cfg = learning.get("dialog_dataset") if isinstance(learning, Mapping) else {}
    if not (isinstance(dialog_cfg, Mapping) and dialog_cfg.get("enabled") is True):
        return ""
    try:
        top_k = int((dialog_cfg or {}).get("top_k", 2)) if isinstance(dialog_cfg, Mapping) else 2
    except Exception:
        top_k = 2
    try:
        min_score = float((dialog_cfg or {}).get("min_score", 0.08)) if isinstance(dialog_cfg, Mapping) else 0.08
    except Exception:
        min_score = 0.08
    try:
        max_chars = int((dialog_cfg or {}).get("max_prompt_chars", 3500)) if isinstance(dialog_cfg, Mapping) else 3500
    except Exception:
        max_chars = 3500
    return dialog_retriever.build_dialog_examples_block(
        int(tenant_id),
        user_text,
        tenant_dir_fn=tenant_dir,
        top_k=max(1, min(3, top_k)),
        min_score=max(0.0, min(1.0, min_score)),
        max_chars=max(800, min(6000, max_chars)),
    )


def _legacy_pair_training_enabled(tenant_id: int) -> bool:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = {}
    learning = cfg.get("learning") if isinstance(cfg, Mapping) else {}
    if not isinstance(learning, Mapping):
        return False
    return bool(learning.get("legacy_pairs_enabled") or learning.get("pair_examples_enabled"))


def _dialog_dataset_available(tenant_id: int) -> bool:
    try:
        cfg = read_tenant_config(int(tenant_id))
    except Exception:
        cfg = {}
    learning = cfg.get("learning") if isinstance(cfg, Mapping) else {}
    dialog_cfg = learning.get("dialog_dataset") if isinstance(learning, Mapping) else {}
    if not (isinstance(dialog_cfg, Mapping) and dialog_cfg.get("enabled") is True):
        return False
    try:
        return dialog_retriever.ensure_dialog_index(int(tenant_id), tenant_dir_fn=tenant_dir) is not None
    except Exception:
        return False


def _guard_unsafe_dialog_training_reply(*, user_text: str, reply_text: str) -> str:
    query = str(user_text or "")
    reply = str(reply_text or "").strip()
    if not query.strip() or not reply:
        return reply
    if not _LOCATION_INTENT_RE.search(query):
        return reply
    if _CITY_HINT_RE.search(query):
        return reply
    lowered_reply = reply.lower()
    has_city_clarification = bool(_CITY_CLARIFICATION_REPLY_RE.search(lowered_reply))
    if _is_low_value_location_reply(reply):
        return _CITY_CLARIFICATION_FALLBACK
    if _UNSAFE_CONCRETE_LOCATION_REPLY_RE.search(reply):
        return default_fallback_reply()
    if _LOCATION_OR_CATALOG_PROMISE_RE.search(reply):
        return _CITY_CLARIFICATION_FALLBACK
    if has_city_clarification:
        return reply
    if not _UNSAFE_LOCATION_REPLY_RE.search(reply):
        return reply
    return default_fallback_reply()


def _is_low_value_location_reply(reply_text: str) -> bool:
    reply = str(reply_text or "").strip()
    if not reply or "?" in reply:
        return False
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9@]+", reply)
    if len(words) > 5:
        return False
    low = reply.lower()
    if any(marker in low for marker in ("гермес", "айдар", "двери", "каталог")):
        return True
    return False
