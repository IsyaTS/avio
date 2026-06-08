from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.services.avito_contextual_case_writer import write_contextual_case_exports


pytestmark = pytest.mark.unit


def _case(case_id: str = "case-1") -> dict:
    return {
        "source": "avito",
        "tenant_id": 101,
        "case_id": case_id,
        "dialog_id": "dialog-1",
        "turn_index": 1,
        "channel": "avito",
        "context": {
            "intent": "store_location",
            "stage": "first_touch",
            "client_city": "Уфа",
            "business_city": "Уфа",
            "known_facts": ["client_city"],
            "missing_facts": [],
        },
        "dialog": {
            "history": [{"role": "client", "text": "Здравствуйте, где посмотреть двери?"}],
            "manager_reply": {"role": "manager", "text": "В Уфе магазин находится на Менделеева 80"},
        },
        "reply_facts": {"mentions_address": True, "city_specific": True},
        "applicability": {"mode": "context_bound", "requires": ["client_city"], "same_city_required": True},
        "quality": {"status": "usable", "confidence": 0.8, "reason_code": "context_bound_address_answer"},
        "created_at": "2026-05-22T00:00:00Z",
    }


def test_writes_valid_contextual_jsonl(tmp_path: Path) -> None:
    result = write_contextual_case_exports(
        tenant_id=101,
        job_id="job-1",
        contextual_cases=[_case("case-1"), _case("case-2")],
        rejected_summary={"mode_counts": {"context_bound": 2}},
        export_root=tmp_path,
    )

    assert result.contextual_cases_count == 2
    assert result.contextual_file_path is not None
    path = Path(result.contextual_file_path)
    assert path.parent == tmp_path / "101" / "uploads" / "dialogs"
    assert path.name.startswith("contextual_cases_2_")
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2
    assert rows[0]["case_id"] == "case-1"


def test_writes_valid_review_jsonl_when_present(tmp_path: Path) -> None:
    result = write_contextual_case_exports(
        tenant_id=101,
        job_id="job-1",
        contextual_cases=[],
        review_cases=[_case("review-1")],
        rejected_summary={},
        export_root=tmp_path,
    )

    assert result.contextual_file_path is None
    assert result.review_cases_count == 1
    assert result.review_cases_file_path is not None
    rows = [json.loads(line) for line in Path(result.review_cases_file_path).read_text(encoding="utf-8").splitlines()]
    assert rows[0]["case_id"] == "review-1"
    assert Path(result.review_cases_file_path).name.startswith("review_cases_1_")


def test_summary_json_contains_aggregates_without_raw_dialog_text(tmp_path: Path) -> None:
    result = write_contextual_case_exports(
        tenant_id=101,
        job_id="job-1",
        contextual_cases=[],
        rejected_summary={
            "reject_reasons": {"contact_only_reply": 1},
            "bad_raw": {"message": "Клиент: raw text should not be here"},
        },
        export_root=tmp_path,
    )

    assert result.rejected_cases_summary_path is not None
    summary_path = Path(result.rejected_cases_summary_path)
    assert summary_path.name.startswith("rejected_cases_summary_")
    data = json.loads(summary_path.read_text(encoding="utf-8"))
    assert data["reject_reasons"]["contact_only_reply"] == 1
    assert "raw text should not be here" not in summary_path.read_text(encoding="utf-8")


def test_writes_domain_schema_and_business_rules(tmp_path: Path) -> None:
    result = write_contextual_case_exports(
        tenant_id=101,
        job_id="job-1",
        contextual_cases=[_case("case-1")],
        rejected_summary={},
        domain_schema={
            "schema_version": "avito_domain_schema_v1",
            "domain_schema_id": "schema-1",
            "tenant_id": 101,
            "domain": "lawn_mowing",
            "domain_label": "покос травы",
            "required_slots": ["area_size"],
        },
        business_rules_draft={
            "schema_version": "avito_business_rules_draft_v1",
            "tenant_id": 101,
            "domain_schema_id": "schema-1",
            "domain": "lawn_mowing",
            "rules": [],
        },
        export_root=tmp_path,
    )

    assert result.domain_schema_path is not None
    assert result.business_rules_draft_path is not None
    schema_path = Path(result.domain_schema_path)
    rules_path = Path(result.business_rules_draft_path)
    assert schema_path.parent == tmp_path / "101" / "uploads" / "dialogs"
    assert rules_path.parent == tmp_path / "101" / "uploads" / "dialogs"
    assert schema_path.name.startswith("domain_schema_")
    assert rules_path.name.startswith("business_rules_draft_")
    assert json.loads(schema_path.read_text(encoding="utf-8"))["domain"] == "lawn_mowing"
    row = json.loads(Path(result.contextual_file_path).read_text(encoding="utf-8").splitlines()[0])
    assert row["domain_schema_id"] == "schema-1"
