#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import pathlib
from dataclasses import asdict, dataclass
from typing import Iterable


ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_MAX_LINES = 80
DEFAULT_PATHS = (
    "apps/api/main.py",
    "apps/api/web/admin.py",
    "apps/api/web/auth.py",
    "apps/api/web/public.py",
    "apps/api/web/client.py",
    "apps/api/web/webhooks.py",
    "apps/worker/main.py",
    "apps/api/web/services",
    "apps/worker/services",
)
DEFAULT_FILE_LINE_BUDGETS = {
    "apps/api/main.py": 953,
    "apps/api/web/admin.py": 810,
    "apps/api/web/auth.py": 1373,
    "apps/api/web/client.py": 1243,
    "apps/api/web/public.py": 2658,
    "apps/api/web/webhooks.py": 743,
    "apps/worker/main.py": 2476,
}


@dataclass(frozen=True)
class FunctionIssue:
    path: str
    name: str
    line: int
    lines: int
    limit: int


@dataclass(frozen=True)
class FileIssue:
    path: str
    lines: int
    limit: int


def _relative(path: pathlib.Path, *, root: pathlib.Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _iter_python_files(paths: Iterable[pathlib.Path]) -> list[pathlib.Path]:
    files: list[pathlib.Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(sorted(path.rglob("*.py")))
        elif path.suffix == ".py":
            files.append(path)
    return sorted(dict.fromkeys(files))


def count_physical_lines(path: pathlib.Path) -> int:
    return len(path.read_text(encoding="utf-8").splitlines())


def scan_file_budgets(
    budgets: dict[str, int],
    *,
    root: pathlib.Path = ROOT_DIR,
) -> list[FileIssue]:
    issues: list[FileIssue] = []
    for rel_path, limit in sorted(budgets.items()):
        path = root / rel_path
        if not path.exists():
            continue
        lines = count_physical_lines(path)
        if lines > limit:
            issues.append(FileIssue(path=rel_path, lines=lines, limit=limit))
    return issues


def scan_file(path: pathlib.Path, *, max_lines: int, root: pathlib.Path = ROOT_DIR) -> list[FunctionIssue]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError as exc:
        return [
            FunctionIssue(
                path=_relative(path, root=root),
                name=f"<syntax-error: {exc.msg}>",
                line=int(exc.lineno or 1),
                lines=max_lines + 1,
                limit=max_lines,
            )
        ]

    issues: list[FunctionIssue] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        end_line = getattr(node, "end_lineno", None) or node.lineno
        line_count = int(end_line) - int(node.lineno) + 1
        if line_count <= max_lines:
            continue
        issues.append(
            FunctionIssue(
                path=_relative(path, root=root),
                name=node.name,
                line=int(node.lineno),
                lines=line_count,
                limit=max_lines,
            )
        )
    return sorted(issues, key=lambda item: (-item.lines, item.path, item.line, item.name))


def scan_paths(
    paths: Iterable[pathlib.Path],
    *,
    max_lines: int = DEFAULT_MAX_LINES,
    root: pathlib.Path = ROOT_DIR,
) -> list[FunctionIssue]:
    issues: list[FunctionIssue] = []
    for path in _iter_python_files(paths):
        issues.extend(scan_file(path, max_lines=max_lines, root=root))
    return sorted(issues, key=lambda item: (-item.lines, item.path, item.line, item.name))


def _print_text(
    function_issues: list[FunctionIssue],
    *,
    file_issues: list[FileIssue],
    max_lines: int,
) -> None:
    print("=== MONOLITH GUARD ===")
    print(f"max_function_lines: {max_lines}")
    if not function_issues:
        print("oversized_functions: none")
    else:
        print("oversized_functions:")
        for issue in function_issues:
            print(f"- {issue.path}:{issue.line} {issue.name} lines={issue.lines} limit={issue.limit}")
    if not file_issues:
        print("oversized_files: none")
    else:
        print("oversized_files:")
        for issue in file_issues:
            print(f"- {issue.path} lines={issue.lines} limit={issue.limit}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fail when guarded modules contain oversized functions."
    )
    parser.add_argument("paths", nargs="*", default=list(DEFAULT_PATHS))
    parser.add_argument("--max-lines", type=int, default=DEFAULT_MAX_LINES)
    parser.add_argument(
        "--skip-file-budget",
        action="store_true",
        help="skip guarded file-size budgets and only check function size",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    root = ROOT_DIR
    paths = [pathlib.Path(item).resolve() for item in args.paths]
    function_issues = scan_paths(paths, max_lines=int(args.max_lines), root=root)
    file_issues = [] if args.skip_file_budget else scan_file_budgets(DEFAULT_FILE_LINE_BUDGETS, root=root)
    if args.json:
        print(
            json.dumps(
                {
                    "oversized_functions": [asdict(item) for item in function_issues],
                    "oversized_files": [asdict(item) for item in file_issues],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        _print_text(function_issues, file_issues=file_issues, max_lines=int(args.max_lines))
    if function_issues or file_issues:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
