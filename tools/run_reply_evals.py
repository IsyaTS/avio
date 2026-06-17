#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import statistics
import sys
from collections import Counter
from typing import Any

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from libs.core import response_pipeline
from libs.core.reply_quality_checker import check_reply_quality


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dry-run reply evals for existing response pipeline.")
    parser.add_argument("--tenant-id", type=int, required=False)
    parser.add_argument(
        "--cases",
        required=True,
        help="Path to jsonl with eval cases.",
    )
    parser.add_argument(
        "--out",
        required=False,
        help="Path to output report JSON.",
    )
    parser.add_argument("--since", required=False, help="Optional filter alias: ignored for compatibility.")
    parser.add_argument("--limit", type=int, required=False, help="Limit processed cases.")
    return parser.parse_args()


def _load_cases(path: pathlib.Path) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            continue
        cases.append(payload)
    return cases


def _select_cases(cases: list[dict[str, Any]], tenant_id: int | None, limit: int | None) -> list[dict[str, Any]]:
    if tenant_id is not None:
        cases = [case for case in cases if int(case.get("tenant_id") or 0) in (0, tenant_id)]
    if limit is not None and limit > 0:
        cases = cases[:limit]
    return cases


async def _run_case(case: dict[str, Any], fallback_tenant: int | None) -> dict[str, Any]:
    tenant_id = int(case.get("tenant_id") or fallback_tenant or 0)
    channel = str(case.get("channel") or "avito")
    user_text = str(case.get("user_text") or "").strip()
    history = case.get("history") or []
    expected = case.get("expected") or {}
    pipeline_result = await response_pipeline.run_response_pipeline(
        tenant_id=tenant_id,
        channel=channel,
        user_text=user_text,
        history=history if isinstance(history, list) else [],
        enable_photos=False,
        timeout_seconds=12.0,
        log_fn=None,
    )
    source = str(pipeline_result.source or "llm")
    violations = check_reply_quality(user_text=user_text, reply_text=pipeline_result.reply_text, expected=expected, source=source)
    trace_quality_violations = list(getattr(pipeline_result.trace, "quality_violations", []) or [])
    return {
        "id": str(case.get("id") or "-"),
        "tenant_id": tenant_id,
        "channel": channel,
        "user_text": user_text,
        "reply_text": pipeline_result.reply_text,
        "pipeline_source": source,
        "history_count": len(history) if isinstance(history, list) else 0,
        "violations": violations,
        "trace_quality_violations": trace_quality_violations,
        "passed": not bool(violations),
        "trace_id": getattr(pipeline_result.trace, "trace_id", None),
        "latency_ms": getattr(pipeline_result.trace, "latency_ms", None),
        "fallback_used": bool(getattr(pipeline_result.trace, "fallback_used", False)),
        "fallback_source": getattr(pipeline_result.trace, "fallback_source", None),
        "catalog_context_count": getattr(pipeline_result.trace, "catalog_context_count", None),
        "dialog_examples_used": getattr(pipeline_result.trace, "dialog_examples_used", None),
        "legacy_examples_used": getattr(pipeline_result.trace, "legacy_examples_used", None),
        "policy_hint_used": getattr(pipeline_result.trace, "policy_hint_used", None),
        "model": getattr(pipeline_result.trace, "model", None),
    }


async def _run(cases: list[dict[str, Any]], tenant_id: int | None) -> dict[str, Any]:
    results = [ _run_case(case, tenant_id) for case in cases]
    # Preserve pipeline ordering for deterministic replay.
    cases_payload = await asyncio.gather(*results)
    total = len(cases_payload)
    passed_count = sum(1 for item in cases_payload if item["passed"])
    failures = [item for item in cases_payload if not item["passed"]]
    fail_counter = Counter()
    for item in failures:
        for violation in item["violations"]:
            fail_counter[violation] += 1
    pass_rate = (passed_count / total * 100.0) if total else 0.0
    worst = sorted(cases_payload, key=lambda item: (len(item["violations"]), item["id"]))[-10:]
    latency_values = []
    for item in cases_payload:
        latency_ms = item.get("latency_ms")
        if latency_ms is not None:
            latency_values.append(float(latency_ms))
    summary = {
        "total": total,
        "passed": passed_count,
        "failed": total - passed_count,
        "pass_rate_percent": round(pass_rate, 2),
        "failures_by_violation": dict(fail_counter.most_common()),
        "cases": cases_payload,
        "top_failures": worst,
        "latency": {
            "p50_ms": round(statistics.median(latency_values), 2) if latency_values else 0,
            "count": len(latency_values),
        },
    }
    return summary


def _write_report(path: pathlib.Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


async def main_async() -> int:
    args = _parse_args()
    case_path = pathlib.Path(args.cases)
    if not case_path.exists():
        raise SystemExit(f"cases file not found: {case_path}")
    cases = _load_cases(case_path)
    if not cases:
        raise SystemExit("No cases found")
    selected = _select_cases(cases, args.tenant_id, args.limit)
    report = await _run(selected, args.tenant_id)
    out_path = pathlib.Path(args.out or (ROOT_DIR / "evals" / "reports" / "reply_evals_latest.json"))
    _write_report(out_path, report)
    top_violation_lines = ", ".join(
        f"{name}={count}" for name, count in list(report.get("failures_by_violation", {}).items())[:5]
    ) or "-"
    print(f"total={report['total']} passed={report['passed']} failed={report['failed']} pass_rate={report['pass_rate_percent']}%")
    print(f"top violations: {top_violation_lines}")
    print("worst cases:")
    for item in report.get("top_failures", []):
        snippet = (item["reply_text"] or "")[:160]
        print(f"- id={item['id']} passed={item['passed']} source={item['pipeline_source']} violations={item['violations']} reply={snippet}")
    print(f"report={out_path}")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
