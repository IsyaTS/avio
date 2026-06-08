from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class ContextualCaseImportResult:
    ok: bool
    set_id: str
    imported_count: int
    active_cases_count: int
    skipped_count: int
    domain_label: str | None = None
    error_code: str | None = None


class ContextualCaseImportError(RuntimeError):
    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


async def import_from_export_job(
    *,
    tenant_id: int,
    job_id: str,
    export_repo: Any,
    contextual_repo: Any,
) -> ContextualCaseImportResult:
    row = await export_repo.get_job(int(tenant_id), str(job_id))
    if not row or int(row.get("tenant_id") or 0) != int(tenant_id):
        raise ContextualCaseImportError("export_not_found")
    status = str(row.get("status") or "")
    if status not in {"completed", "partial"}:
        raise ContextualCaseImportError("export_not_ready")

    contextual_path = Path(str(row.get("contextual_file_path") or ""))
    domain_path = Path(str(row.get("domain_schema_path") or ""))
    rules_path = Path(str(row.get("business_rules_draft_path") or ""))
    if not contextual_path.is_file():
        raise ContextualCaseImportError("contextual_file_not_found")
    if not domain_path.is_file():
        raise ContextualCaseImportError("domain_schema_not_found")

    domain_schema = _read_json_file(domain_path)
    business_rules = _read_json_file(rules_path) if rules_path.is_file() else {}
    set_id = _set_id(tenant_id=int(tenant_id), job_id=str(job_id), domain_schema=domain_schema)
    imported: list[dict[str, Any]] = []
    skipped = 0
    seen_fingerprints: set[str] = set()
    for item in _read_jsonl(contextual_path):
        normalized = _normalize_case(item, tenant_id=int(tenant_id), set_id=set_id)
        if normalized is None:
            skipped += 1
            continue
        fingerprint = str(normalized.get("fingerprint") or "")
        if fingerprint and fingerprint in seen_fingerprints:
            skipped += 1
            continue
        seen_fingerprints.add(fingerprint)
        imported.append(normalized)

    domain_schema_id = str(domain_schema.get("domain_schema_id") or "") or None
    await contextual_repo.create_case_set(
        tenant_id=int(tenant_id),
        set_id=set_id,
        source_export_job_id=str(job_id),
        domain_schema_id=domain_schema_id,
        domain_schema=domain_schema,
        business_rules_draft=business_rules,
        cases_count=len(imported),
        active_cases_count=len(imported),
        status="imported",
    )
    count = await contextual_repo.upsert_contextual_cases(
        tenant_id=int(tenant_id),
        set_id=set_id,
        cases=imported,
    )
    await contextual_repo.deactivate_old_sets(int(tenant_id), set_id)
    await contextual_repo.activate_case_set(int(tenant_id), set_id)
    return ContextualCaseImportResult(
        ok=True,
        set_id=set_id,
        imported_count=int(count),
        active_cases_count=int(count),
        skipped_count=skipped,
        domain_label=str(domain_schema.get("domain_label") or "") or None,
    )


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ContextualCaseImportError("invalid_json") from exc
    if not isinstance(value, dict):
        raise ContextualCaseImportError("invalid_json")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                value = json.loads(raw)
                if isinstance(value, dict):
                    rows.append(value)
    except ContextualCaseImportError:
        raise
    except Exception as exc:
        raise ContextualCaseImportError("invalid_jsonl") from exc
    return rows


def _normalize_case(item: Mapping[str, Any], *, tenant_id: int, set_id: str) -> dict[str, Any] | None:
    if int(item.get("tenant_id") or tenant_id) != int(tenant_id):
        return None
    case_id = str(item.get("case_id") or "").strip()
    if not case_id:
        return None
    quality = item.get("quality") if isinstance(item.get("quality"), Mapping) else {}
    if str(quality.get("status") or "").strip().lower() != "usable":
        return None
    context = item.get("context") if isinstance(item.get("context"), Mapping) else {}
    dialog = item.get("dialog") if isinstance(item.get("dialog"), Mapping) else {}
    manager_reply = dialog.get("manager_reply") if isinstance(dialog.get("manager_reply"), Mapping) else {}
    history = dialog.get("history") if isinstance(dialog.get("history"), list) else []
    applicability = item.get("applicability") if isinstance(item.get("applicability"), Mapping) else {}
    if not isinstance(manager_reply.get("text"), str) or not manager_reply.get("text", "").strip():
        return None
    if not history:
        return None
    search_text = _build_search_text(item, context=context, dialog=dialog)
    fingerprint = hashlib.sha1(search_text.encode("utf-8")).hexdigest()
    return {
        "tenant_id": tenant_id,
        "set_id": set_id,
        "case_id": case_id,
        "domain_schema_id": str(item.get("domain_schema_id") or context.get("domain_schema_id") or ""),
        "domain": str(context.get("domain") or item.get("domain") or ""),
        "intent": str(context.get("intent") or ""),
        "mode": str(applicability.get("mode") or "direct_example"),
        "context": dict(context),
        "dialog": dict(dialog),
        "reply_facts": dict(item.get("reply_facts") or {}) if isinstance(item.get("reply_facts"), Mapping) else {},
        "applicability": dict(applicability),
        "quality": dict(quality),
        "search_text": search_text,
        "fingerprint": fingerprint,
    }


def _build_search_text(item: Mapping[str, Any], *, context: Mapping[str, Any], dialog: Mapping[str, Any]) -> str:
    parts: list[str] = [
        str(context.get("domain") or ""),
        str(context.get("domain_label") or ""),
        str(context.get("intent") or ""),
    ]
    slots = context.get("slots") if isinstance(context.get("slots"), Mapping) else {}
    parts.extend(f"{key} {value}" for key, value in slots.items())
    parts.extend(str(key) for key in context.get("known_slots") or [] if isinstance(key, str))
    parts.extend(str(key) for key in context.get("missing_slots") or [] if isinstance(key, str))
    history = dialog.get("history") if isinstance(dialog.get("history"), list) else []
    client_turns = [
        str(row.get("text") or "")
        for row in history
        if isinstance(row, Mapping) and str(row.get("role") or "") == "client"
    ]
    parts.extend(client_turns[-4:])
    reply = dialog.get("manager_reply") if isinstance(dialog.get("manager_reply"), Mapping) else {}
    parts.append(str(reply.get("text") or ""))
    reply_facts = item.get("reply_facts") if isinstance(item.get("reply_facts"), Mapping) else {}
    parts.extend(str(key) for key, value in reply_facts.items() if value)
    return " ".join(" ".join(part.split()) for part in parts if str(part).strip())[:6000]


def _set_id(*, tenant_id: int, job_id: str, domain_schema: Mapping[str, Any]) -> str:
    seed = f"{tenant_id}:{job_id}:{domain_schema.get('domain_schema_id') or ''}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()
