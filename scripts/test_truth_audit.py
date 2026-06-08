#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import pathlib
from dataclasses import asdict, dataclass, field
from typing import Any, Iterable


ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_TESTS_DIR = ROOT_DIR / "tests"
KNOWN_MARKERS = {"unit", "integration", "e2e", "prod_readonly"}
CRITICAL_NAME_HINTS = (
    "truth",
    "worker",
    "incoming",
    "oauth",
    "public_settings",
    "client_settings",
    "tenant",
    "learning",
    "response",
    "webhook",
)
TRUTH_COVERED_FILES = {
    "tests/test_avito_oauth.py": "tests/test_truth_critical_flows.py::test_avito_oauth_callback_persists_tokens_with_signed_state_after_redis_loss",
    "tests/test_client_settings.py": "scripts/restart_persistence_smoke.py",
    "tests/test_internal_tenant.py": "scripts/critical_smoke.py",
    "tests/test_learning_feedback.py": "tests/test_truth_critical_flows.py::test_learning_examples_from_db_reach_response_pipeline_prompt",
    "tests/test_learning_policy_v2.py": "scripts/dialog_quality_runner.py",
    "tests/test_main_webhook.py": "scripts/inbox_worker_smoke.py",
    "tests/test_public_settings.py": "scripts/restart_persistence_smoke.py",
    "tests/test_tenant_configs_repo.py": "scripts/critical_smoke.py",
    "tests/test_truth_critical_flows.py": "truth source file",
    "tests/test_webhooks_provider.py": "scripts/inbox_worker_smoke.py",
    "tests/test_worker_avito_send.py": "scripts/inbox_worker_smoke.py",
    "tests/test_worker_incoming.py": "scripts/inbox_worker_smoke.py",
    "tests/test_worker_telegram_send.py": "tests/test_public_tg.py",
}


@dataclass
class FileAudit:
    path: str
    tests: int = 0
    marked_tests: int = 0
    unmarked_tests: list[str] = field(default_factory=list)
    monkeypatch_setattr: int = 0
    patch_calls: int = 0
    critical_hint: bool = False
    truth_coverage: str = ""

    @property
    def mock_points(self) -> int:
        return self.monkeypatch_setattr + self.patch_calls

    @property
    def needs_truth_review(self) -> bool:
        if not self.critical_hint:
            return False
        if self.unmarked_tests:
            return True
        return bool(self.mock_points >= 5 and not self.truth_coverage)


def _is_test_function(node: ast.AST) -> bool:
    return isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")


def _decorator_marker_name(decorator: ast.AST) -> str | None:
    target = decorator
    if isinstance(target, ast.Call):
        target = target.func
    if not isinstance(target, ast.Attribute):
        return None
    parts: list[str] = []
    cur: ast.AST = target
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
    dotted = ".".join(reversed(parts))
    prefix = "pytest.mark."
    if dotted.startswith(prefix):
        return dotted.removeprefix(prefix)
    return None


def _has_known_marker(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    return any((_decorator_marker_name(dec) or "") in KNOWN_MARKERS for dec in node.decorator_list)


def _module_markers(tree: ast.Module) -> set[str]:
    markers: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in node.targets):
            continue
        values = node.value.elts if isinstance(node.value, (ast.List, ast.Tuple)) else [node.value]
        for value in values:
            marker = _decorator_marker_name(value)
            if marker in KNOWN_MARKERS:
                markers.add(marker)
    return markers


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _call_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return ""


def audit_file(path: pathlib.Path, *, root: pathlib.Path = ROOT_DIR) -> FileAudit:
    rel = str(path.relative_to(root)) if path.is_relative_to(root) else str(path)
    item = FileAudit(
        path=rel,
        critical_hint=any(hint in path.name for hint in CRITICAL_NAME_HINTS),
        truth_coverage=TRUTH_COVERED_FILES.get(rel, ""),
    )
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return item
    module_markers = _module_markers(tree)

    for node in ast.walk(tree):
        if _is_test_function(node):
            item.tests += 1
            if module_markers or _has_known_marker(node):
                item.marked_tests += 1
            else:
                item.unmarked_tests.append(node.name)
        if isinstance(node, ast.Call):
            name = _call_name(node.func)
            if name.endswith("monkeypatch.setattr"):
                item.monkeypatch_setattr += 1
            elif name.endswith(".patch") or name == "patch" or name.endswith(".patch.object"):
                item.patch_calls += 1
    return item


def iter_test_files(paths: Iterable[pathlib.Path]) -> list[pathlib.Path]:
    out: list[pathlib.Path] = []
    for path in paths:
        if path.is_dir():
            out.extend(sorted(path.rglob("test_*.py")))
        elif path.name.startswith("test_") and path.suffix == ".py":
            out.append(path)
    return sorted(dict.fromkeys(out))


def _summary(items: list[FileAudit]) -> dict[str, Any]:
    return {
        "files": len(items),
        "tests": sum(item.tests for item in items),
        "marked_tests": sum(item.marked_tests for item in items),
        "unmarked_tests": sum(len(item.unmarked_tests) for item in items),
        "mock_points": sum(item.mock_points for item in items),
        "needs_truth_review_files": [item.path for item in items if item.needs_truth_review],
    }


def _print_text(items: list[FileAudit]) -> None:
    summary = _summary(items)
    print("=== TEST TRUTH AUDIT ===")
    for key, value in summary.items():
        if key != "needs_truth_review_files":
            print(f"{key}: {value}")
    risky = [item for item in items if item.needs_truth_review]
    if not risky:
        print("needs_truth_review: none")
        return
    print("needs_truth_review:")
    for item in risky:
        unmarked = len(item.unmarked_tests)
        print(
            f"- {item.path}: tests={item.tests} unmarked={unmarked} "
            f"mock_points={item.mock_points} truth_coverage={item.truth_coverage or '-'}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit pytest files for truth/smoke review risks.")
    parser.add_argument("paths", nargs="*", default=[str(DEFAULT_TESTS_DIR)])
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    parser.add_argument(
        "--fail-on-review",
        action="store_true",
        help="Exit with code 1 when critical files need truth review.",
    )
    args = parser.parse_args()

    files = iter_test_files(pathlib.Path(item).resolve() for item in args.paths)
    audits = [audit_file(path) for path in files]
    if args.json:
        print(
            json.dumps(
                {"summary": _summary(audits), "files": [asdict(item) for item in audits]},
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        _print_text(audits)
    if args.fail_on_review and any(item.needs_truth_review for item in audits):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
