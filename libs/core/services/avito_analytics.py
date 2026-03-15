from __future__ import annotations

from typing import Any, Iterable, Mapping

from libs.core.integrations.avito_analytics import AvitoAPIError


def _normalize_job_applications(rows: Iterable[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in rows or []:
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        app_id = (
            row.get("id")
            or row.get("application_id")
            or row.get("applyId")
            or row.get("applicationId")
        )
        if not app_id:
            continue
        normalized = {
            "id": str(app_id),
            "status": row.get("status") or row.get("state") or "",
            "created_at": row.get("created_at") or row.get("createdAt") or row.get("created") or "",
            "vacancy_id": row.get("vacancy_id") or row.get("vacancyId") or "",
            "resume_id": row.get("resume_id") or row.get("resumeId") or "",
            "applicant": row.get("applicant") or row.get("applicant_name") or row.get("applicantName") or "",
        }
        out.append(normalized)
    return out


def _vas_error_payload(exc: Exception) -> dict[str, Any]:
    if isinstance(exc, AvitoAPIError):
        return {
            "message": str(exc),
            "status": int(exc.status or 0),
            "payload": exc.payload if isinstance(exc.payload, Mapping) else {},
        }
    return {
        "message": str(exc),
        "status": 0,
        "payload": {},
    }

