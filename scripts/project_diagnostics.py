#!/usr/bin/env python3
"""Generate repository diagnostics to support cleanup decisions."""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

# NOTE: keep computation lightweight so it can run inside CI environments
PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_ROOT = PROJECT_ROOT / "app"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def iter_files(base: Path) -> Iterable[Path]:
    for path in base.rglob("*"):
        if path.is_file():
            yield path


def loc_by_extension(base: Path) -> Dict[str, int]:
    stats: Dict[str, int] = defaultdict(int)
    for file_path in iter_files(base):
        ext = file_path.suffix.lower() or "<none>"
        try:
            with file_path.open("r", encoding="utf-8", errors="ignore") as handle:
                stats[ext] += sum(1 for _ in handle)
        except OSError:
            continue
    return dict(sorted(stats.items(), key=lambda item: (-item[1], item[0])))


def build_tree(base: Path, max_depth: int = 2) -> Dict[str, Any]:
    tree: Dict[str, Any] = {}
    for path in base.iterdir():
        if path.name.startswith("."):
            continue
        if path.is_dir():
            if max_depth <= 0:
                tree[path.name + "/"] = "…"
            else:
                tree[path.name + "/"] = build_tree(path, max_depth - 1)
        else:
            tree[path.name] = path.stat().st_size
    return tree


def discover_modules(base: Path) -> List[str]:
    modules: List[str] = []
    for file_path in sorted(base.rglob("*.py")):
        modules.append(str(file_path.relative_to(PROJECT_ROOT)))
    return modules


def import_graph(base: Path) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
    outgoing: Dict[str, Set[str]] = defaultdict(set)
    incoming: Dict[str, Set[str]] = defaultdict(set)
    for file_path in base.rglob("*.py"):
        rel = file_path.relative_to(PROJECT_ROOT)
        module_name = ".".join(rel.with_suffix("").parts)
        incoming.setdefault(module_name, set())
        try:
            source = file_path.read_text(encoding="utf-8")
        except OSError:
            continue
        try:
            tree = ast.parse(source, filename=str(file_path))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.name
                    if name.startswith("app.") or name in {"core", "catalog_index", "catalog", "web"}:
                        outgoing[module_name].add(name)
                        incoming[name].add(module_name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    name = node.module
                    if name.startswith("app.") or name in {"core", "catalog_index", "catalog", "web"}:
                        outgoing[module_name].add(name)
                        incoming[name].add(module_name)
    return dict(outgoing), dict(incoming)


def extract_routes() -> List[Dict[str, Any]]:
    spec = importlib.util.spec_from_file_location("app.main", APP_ROOT / "main.py")
    if spec and spec.loader:
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[attr-defined]
        app = module.app  # type: ignore[attr-defined]
    else:
        raise RuntimeError("failed to import app.main")

    routes: List[Dict[str, Any]] = []
    for route in app.routes:
        if not hasattr(route, "endpoint"):
            continue
        if getattr(route, "include_in_schema", True):
            summary = {
                "path": route.path,
                "methods": sorted(getattr(route, "methods", set())),
                "name": route.name,
                "endpoint": f"{route.endpoint.__module__}.{route.endpoint.__name__}",
            }
            routes.append(summary)
    return sorted(routes, key=lambda item: item["path"])


def unused_functions(base: Path) -> List[str]:
    defined: Dict[str, Set[str]] = defaultdict(set)
    referenced: Set[str] = set()
    for file_path in base.rglob("*.py"):
        rel = file_path.relative_to(PROJECT_ROOT)
        module_name = ".".join(rel.with_suffix("").parts)
        try:
            tree = ast.parse(file_path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
                defined[module_name].add(node.name)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    referenced.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    referenced.add(node.func.attr)
    unused: List[str] = []
    for module, names in defined.items():
        for name in sorted(names):
            if name not in referenced:
                unused.append(f"{module}.{name}")
    return unused


def generate_report(include_all: bool = False) -> Dict[str, Any]:
    """Build a diagnostics payload for the repository."""

    payload: Dict[str, Any] = {
        "tree": build_tree(PROJECT_ROOT, max_depth=2),
        "loc_by_extension": loc_by_extension(PROJECT_ROOT),
        "python_modules": discover_modules(APP_ROOT),
        "templates": discover_modules(APP_ROOT / "templates"),
        "static_assets": discover_modules(APP_ROOT / "static"),
        "tests": discover_modules(APP_ROOT / "tests"),
    }

    if include_all:
        outgoing, incoming = import_graph(APP_ROOT)
        unused_targets = [
            name for name, sources in incoming.items() if not sources and name.startswith("app.")
        ]
        payload["import_graph"] = {
            "outgoing": {key: sorted(value) for key, value in outgoing.items()},
            "incoming": {key: sorted(value) for key, value in incoming.items()},
            "orphans": unused_targets,
        }
        route_error: str | None = None
        try:
            payload["routes"] = extract_routes()
        except ModuleNotFoundError as exc:
            missing = exc.name or "unknown"
            route_error = (
                f"module '{missing}' is required to import app.main; install app dependencies"
            )
        except ImportError as exc:
            route_error = f"failed to import FastAPI app: {exc}"
        except Exception as exc:  # pragma: no cover - defensive guard for CLI usage
            route_error = f"unexpected error while extracting routes: {exc}"

        if route_error:
            payload["routes"] = []
            payload.setdefault("errors", {})["routes"] = route_error

        payload["unused_functions"] = unused_functions(APP_ROOT)

    return payload


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run repository diagnostics to gather structural insights.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate the complete diagnostics report.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    include_all = bool(args.all)

    # Preserve historical behaviour where invoking the script without flags emits the
    # full report to ease direct execution while still allowing the flag to be used.
    if not args.all:
        include_all = True

    try:
        payload = generate_report(include_all=include_all)
    except Exception as exc:  # pragma: no cover - defensive fallback for CLI usage
        print(f"[diagnostics] failed to generate report: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
