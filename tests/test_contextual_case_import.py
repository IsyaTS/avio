from __future__ import annotations

import json
from pathlib import Path

import pytest

from libs.core.services.contextual_case_import import ContextualCaseImportError, import_from_export_job


pytestmark = pytest.mark.unit


class FakeExportRepo:
    def __init__(self, row: dict | None) -> None:
        self.row = row

    async def get_job(self, tenant_id: int, job_id: str) -> dict | None:
        return self.row


class FakeContextualRepo:
    def __init__(self) -> None:
        self.sets: list[dict] = []
        self.cases: list[dict] = []

    async def create_case_set(self, **kwargs):
        self.sets.append(kwargs)
        return kwargs

    async def upsert_contextual_cases(self, *, tenant_id: int, set_id: str, cases):
        self.cases.extend(list(cases))
        return len(cases)

    async def deactivate_old_sets(self, tenant_id: int, keep_set_id: str):
        return None

    async def activate_case_set(self, tenant_id: int, set_id: str):
        return {}


def _write_artifacts(tmp_path: Path) -> tuple[Path, Path, Path]:
    contextual = tmp_path / "contextual_cases_1.jsonl"
    domain = tmp_path / "domain_schema.json"
    rules = tmp_path / "business_rules.json"
    case = {
        "tenant_id": 7,
        "case_id": "case-1",
        "domain_schema_id": "schema-1",
        "context": {"domain": "lawn_mowing", "domain_label": "покос", "intent": "price_question", "slots": {}},
        "dialog": {
            "history": [{"role": "client", "text": "Сколько стоит?"}],
            "manager_reply": {"role": "manager", "text": "Цена зависит от площади."},
        },
        "reply_facts": {"mentions_price": True},
        "applicability": {"mode": "context_bound", "requires": ["slots.area_size"]},
        "quality": {"status": "usable"},
    }
    contextual.write_text(json.dumps(case, ensure_ascii=False) + "\n", encoding="utf-8")
    domain.write_text(json.dumps({"domain_schema_id": "schema-1", "domain_label": "покос"}, ensure_ascii=False), encoding="utf-8")
    rules.write_text(json.dumps({"rules": []}), encoding="utf-8")
    return contextual, domain, rules


@pytest.mark.asyncio
async def test_imports_contextual_jsonl_without_training_examples(tmp_path: Path) -> None:
    contextual, domain, rules = _write_artifacts(tmp_path)
    repo = FakeContextualRepo()
    result = await import_from_export_job(
        tenant_id=7,
        job_id="job-1",
        export_repo=FakeExportRepo(
            {
                "tenant_id": 7,
                "job_id": "job-1",
                "status": "completed",
                "contextual_file_path": str(contextual),
                "domain_schema_path": str(domain),
                "business_rules_draft_path": str(rules),
            }
        ),
        contextual_repo=repo,
    )
    assert result.imported_count == 1
    assert repo.sets[0]["domain_schema_id"] == "schema-1"
    assert repo.cases[0]["search_text"]
    assert repo.cases[0]["mode"] == "context_bound"


@pytest.mark.asyncio
async def test_rejects_cross_tenant_export(tmp_path: Path) -> None:
    contextual, domain, rules = _write_artifacts(tmp_path)
    with pytest.raises(ContextualCaseImportError) as exc:
        await import_from_export_job(
            tenant_id=8,
            job_id="job-1",
            export_repo=FakeExportRepo(
                {
                    "tenant_id": 7,
                    "job_id": "job-1",
                    "status": "completed",
                    "contextual_file_path": str(contextual),
                    "domain_schema_path": str(domain),
                    "business_rules_draft_path": str(rules),
                }
            ),
            contextual_repo=FakeContextualRepo(),
        )
    assert exc.value.error_code == "export_not_found"
