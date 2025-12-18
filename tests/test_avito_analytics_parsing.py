from __future__ import annotations

from libs.core.services import avito_analytics
from libs.core.integrations.avito_analytics import AvitoAPIError


def test_normalize_job_applications_basic():
    src = [
        {"id": "a1", "status": "new", "created_at": "2024-01-01", "vacancy_id": "v1", "resume_id": "r1", "applicant": "John"},
        {"application_id": "a2", "state": "seen", "createdAt": "2024-01-02", "vacancyId": "v2", "resumeId": "r2", "applicant_name": "Doe"},
        {"applyId": "a3"},
    ]
    normalized = avito_analytics._normalize_job_applications(src)
    ids = [row["id"] for row in normalized]
    assert ids == ["a1", "a2", "a3"]
    assert normalized[0]["status"] == "new"
    assert normalized[1]["status"] == "seen"
    assert normalized[1]["created_at"] == "2024-01-02"
    assert normalized[1]["vacancy_id"] == "v2"
    assert normalized[1]["resume_id"] == "r2"


def test_vas_error_payload():
    exc = AvitoAPIError("bad", status=400, payload={"detail": "invalid"})
    payload = avito_analytics._vas_error_payload(exc)
    assert payload["status"] == 400
    assert payload["payload"]["detail"] == "invalid"
