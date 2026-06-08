from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import Request
from fastapi.responses import JSONResponse, Response


SyncFn = Callable[..., Any]


@dataclass(frozen=True)
class ClientFeedbackDeps:
    resolve_tenant_and_key_fn: SyncFn
    db_module: Any
    sanitize_training_text_fn: SyncFn
    isoformat_fn: SyncFn
    dialogs_logger: Any


async def submit_feedback_api(
    request: Request,
    *,
    tenant: int | str | None,
    deps: ClientFeedbackDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth

    parsed = await _parse_feedback_payload(request, deps)
    if isinstance(parsed, Response):
        return parsed
    message_ref, rating, comment, expected_answer, sanitized_expected = parsed

    metadata = await deps.db_module.get_message_metadata(message_ref)
    metadata_error = _feedback_metadata_error(metadata, int(tenant_id))
    if metadata_error is not None:
        return metadata_error

    lead_id = metadata.get("lead_id")
    q_text = await _previous_question_text(
        tenant_id=int(tenant_id),
        lead_id=int(lead_id or 0),
        metadata=metadata,
        deps=deps,
    )

    if await deps.db_module.feedback_exists(tenant_id, message_ref):
        return {"ok": True, "feedback_id": None, "already_exists": True}

    feedback_id = await deps.db_module.create_message_feedback(
        tenant_id,
        message_ref,
        rating,
        comment or sanitized_expected or None,
        lead_id=lead_id,
        expected_answer=sanitized_expected or expected_answer or None,
    )
    if not feedback_id:
        return JSONResponse({"detail": "feedback_failed"}, status_code=500)

    await _record_training_example(
        tenant_id=int(tenant_id),
        lead_id=lead_id,
        message_ref=message_ref,
        metadata=metadata,
        rating=rating,
        feedback_id=feedback_id,
        q_text=q_text,
        expected_answer=expected_answer,
        deps=deps,
    )
    if rating == "dislike":
        await _mark_disliked_bot_message(
            int(tenant_id),
            lead_id,
            message_ref,
            feedback_id,
            comment or expected_answer or "dislike",
            deps,
        )

    return {"ok": True, "feedback_id": feedback_id}


async def _parse_feedback_payload(
    request: Request,
    deps: ClientFeedbackDeps,
) -> tuple[int, str, str, str, str] | Response:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    message_ref = _coerce_positive_int(payload.get("message_id"))
    if message_ref <= 0:
        return JSONResponse({"detail": "invalid_message"}, status_code=400)
    rating = str(payload.get("rating") or "").strip().lower()
    if rating not in {"like", "dislike"}:
        return JSONResponse({"detail": "invalid_rating"}, status_code=400)
    comment = _payload_text(payload, "comment", required_for_dislike=rating == "dislike")
    expected_answer = _payload_text(
        payload,
        "expected_answer",
        required_for_dislike=rating == "dislike",
    )
    sanitized_expected = deps.sanitize_training_text_fn(expected_answer) if expected_answer else ""
    if rating == "dislike" and not expected_answer:
        return JSONResponse({"detail": "expected_answer_required"}, status_code=400)
    if rating == "dislike" and not sanitized_expected:
        sanitized_expected = expected_answer.strip()
    return message_ref, rating, comment, expected_answer, sanitized_expected


def _payload_text(
    payload: dict[str, Any],
    key: str,
    *,
    required_for_dislike: bool,
) -> str:
    raw = payload.get(key) if required_for_dislike else payload.get(key) or ""
    return str(raw).strip() if raw is not None else ""


def _feedback_metadata_error(
    metadata: Any,
    tenant_id: int,
) -> Response | None:
    if not metadata or int(metadata.get("tenant_id") or 0) != int(tenant_id):
        return JSONResponse({"detail": "not_found"}, status_code=404)
    if int(metadata.get("direction") or 0) != 1:
        return JSONResponse({"detail": "feedback_not_allowed"}, status_code=400)
    if not bool(metadata.get("is_bot")):
        return JSONResponse({"detail": "feedback_not_allowed"}, status_code=400)
    return None


async def _mark_disliked_bot_message(
    tenant_id: int,
    lead_id: Any,
    message_ref: int,
    feedback_id: int,
    reason: str,
    deps: ClientFeedbackDeps,
) -> None:
    try:
        await deps.db_module.mark_bad_bot_message(
            tenant_id,
            message_ref,
            feedback_id=feedback_id,
            reason=reason,
        )
    except Exception:
        deps.dialogs_logger.exception(
            "mark_bad_bot_failed tenant=%s lead=%s msg=%s",
            tenant_id,
            lead_id,
            message_ref,
        )


async def feedback_quality_api(
    request: Request,
    *,
    tenant: int | str | None,
    deps: ClientFeedbackDeps,
) -> dict[str, Any] | Response:
    auth = deps.resolve_tenant_and_key_fn(request, tenant)
    if isinstance(auth, Response):
        return auth
    tenant_id, _ = auth
    try:
        limit_val = int(request.query_params.get("limit", "50"))
    except Exception:
        limit_val = 50
    rows = await deps.db_module.list_recent_disliked_feedback(tenant_id, limit=limit_val)
    items = []
    for row in rows or []:
        items.append(
            {
                "id": row.get("feedback_id"),
                "message_id": row.get("message_id"),
                "lead_id": row.get("lead_id"),
                "user_text": row.get("user_text") or "",
                "bot_text": row.get("bot_text") or "",
                "expected": row.get("expected_answer") or row.get("comment") or "",
                "created_at": deps.isoformat_fn(row.get("feedback_created_at")),
            }
        )
    return {"ok": True, "items": items}


def _coerce_positive_int(value: Any) -> int:
    try:
        result = int(value)
    except Exception:
        return 0
    return result if result > 0 else 0


async def _previous_question_text(
    *,
    tenant_id: int,
    lead_id: int,
    metadata: dict[str, Any],
    deps: ClientFeedbackDeps,
) -> str:
    try:
        created_at = metadata.get("created_at")
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        if created_at is None or not isinstance(created_at, datetime):
            created_at = datetime.now(timezone.utc)
        previous = await deps.db_module.get_previous_incoming_message(
            tenant_id, lead_id, before=created_at
        )
        if previous and previous.get("text"):
            return deps.sanitize_training_text_fn(str(previous.get("text") or ""))
    except Exception:
        return ""
    return ""


async def _record_training_example(
    *,
    tenant_id: int,
    lead_id: Any,
    message_ref: int,
    metadata: dict[str, Any],
    rating: str,
    feedback_id: Any,
    q_text: str,
    expected_answer: str,
    deps: ClientFeedbackDeps,
) -> None:
    try:
        message_text = deps.sanitize_training_text_fn(str(metadata.get("text") or ""))
        source = "like" if rating == "like" else "correction"
        a_text = message_text if rating == "like" else expected_answer
        if q_text and a_text:
            await deps.db_module.record_training_example(
                tenant_id,
                lead_id=lead_id,
                message_id=message_ref,
                source=source,
                source_feedback_id=feedback_id,
                q_text=q_text,
                a_text=deps.sanitize_training_text_fn(a_text),
                is_bad=False,
                embedding_status="pending",
            )
    except Exception:
        deps.dialogs_logger.exception(
            "training_example_create_failed tenant=%s lead=%s msg=%s",
            tenant_id,
            lead_id,
            message_ref,
        )
