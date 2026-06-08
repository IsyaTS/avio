#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import pathlib
import subprocess
import sys
from dataclasses import dataclass
from typing import Iterable, Sequence


ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_RELEASE_DIR = ROOT_DIR / "docs" / "release" / "2026-05-13"
DEFAULT_ACCEPTED_DELETIONS_FILE = DEFAULT_RELEASE_DIR / "accepted-tracked-deletions.txt"
LOCAL_GENERATED_PATTERNS = (
    ".codex",
    ".env_recovery/*",
    "infra/caddy/Caddyfile.backup.*",
    "infra/caddy/Caddyfile.bak.*",
    "screens/*",
    "kabinet/*",
    "avio-connect-flow/*",
)


@dataclass(frozen=True)
class StatusSummary:
    modified: int = 0
    deleted: int = 0
    untracked: int = 0
    other: int = 0

    @property
    def total(self) -> int:
        return self.modified + self.deleted + self.untracked + self.other


@dataclass(frozen=True)
class SliceReport:
    name: str
    pathspec_file: str
    missing_paths: tuple[str, ...]
    status: StatusSummary


@dataclass(frozen=True)
class DirtyPath:
    status: str
    path: str


def read_pathspec(path: pathlib.Path) -> list[str]:
    items: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        items.append(line)
    return items


def missing_pathspec_entries(
    pathspec: Iterable[str],
    *,
    root: pathlib.Path = ROOT_DIR,
    accepted_missing: set[str] | None = None,
) -> list[str]:
    accepted = accepted_missing or set()
    missing: list[str] = []
    for item in pathspec:
        if item in accepted:
            continue
        path = root / item
        if any(char in item for char in "*?["):
            if not list(root.glob(item)):
                missing.append(item)
        elif not path.exists():
            missing.append(item)
    return missing


def summarize_status_lines(lines: Iterable[str]) -> StatusSummary:
    modified = deleted = untracked = other = 0
    for raw_line in lines:
        if not raw_line:
            continue
        status = raw_line[:2]
        if status == "??":
            untracked += 1
        elif "D" in status:
            deleted += 1
        elif status.strip():
            modified += 1
        else:
            other += 1
    return StatusSummary(modified=modified, deleted=deleted, untracked=untracked, other=other)


def parse_status_line(line: str) -> DirtyPath | None:
    if not line:
        return None
    status = line[:2]
    path = line[3:].strip()
    if not path:
        return None
    if " -> " in path:
        path = path.rsplit(" -> ", 1)[1]
    return DirtyPath(status=status, path=path)


def parse_status_lines(lines: Iterable[str]) -> list[DirtyPath]:
    parsed: list[DirtyPath] = []
    for line in lines:
        item = parse_status_line(line)
        if item is not None:
            parsed.append(item)
    return parsed


def match_patterns(paths: Iterable[str], patterns: Sequence[str]) -> list[str]:
    matched: list[str] = []
    for path in paths:
        for pattern in patterns:
            if fnmatch.fnmatch(path, pattern) or path == pattern.rstrip("/*"):
                matched.append(path)
                break
    return sorted(dict.fromkeys(matched))


def path_matches_pathspec(path: str, pathspec_item: str, *, root: pathlib.Path = ROOT_DIR) -> bool:
    item = pathspec_item.rstrip("/")
    if not item:
        return False
    if any(char in pathspec_item for char in "*?["):
        return fnmatch.fnmatch(path, pathspec_item)
    if path == item:
        return True
    candidate = root / item
    if pathspec_item.endswith("/") or candidate.is_dir():
        return path.startswith(f"{item}/")
    return False


def uncovered_dirty_paths(
    dirty: Iterable[DirtyPath],
    pathspecs: Iterable[str],
    *,
    root: pathlib.Path = ROOT_DIR,
    accepted_deleted: set[str] | None = None,
) -> list[str]:
    accepted = accepted_deleted or set()
    scope = list(pathspecs)
    uncovered: list[str] = []
    for item in dirty:
        if "D" in item.status and item.path in accepted:
            continue
        if not any(path_matches_pathspec(item.path, pathspec, root=root) for pathspec in scope):
            uncovered.append(item.path)
    return sorted(dict.fromkeys(uncovered))


def git_lines(args: Sequence[str], *, root: pathlib.Path = ROOT_DIR) -> list[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=str(root),
        text=True,
        capture_output=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def status_for_pathspec(pathspec: Sequence[str], *, root: pathlib.Path = ROOT_DIR) -> list[str]:
    if not pathspec:
        return []
    return git_lines(["status", "--short", "--", *pathspec], root=root)


def build_slice_report(
    pathspec_file: pathlib.Path,
    *,
    root: pathlib.Path = ROOT_DIR,
    accepted_missing: set[str] | None = None,
) -> SliceReport:
    pathspec = read_pathspec(pathspec_file)
    status_lines = status_for_pathspec(pathspec, root=root)
    return SliceReport(
        name=pathspec_file.stem,
        pathspec_file=str(pathspec_file.relative_to(root)),
        missing_paths=tuple(missing_pathspec_entries(pathspec, root=root, accepted_missing=accepted_missing)),
        status=summarize_status_lines(status_lines),
    )


def _iter_pathspec_files(release_dir: pathlib.Path) -> list[pathlib.Path]:
    return sorted(release_dir.glob("*.pathspec"))


def _print_report(
    reports: Sequence[SliceReport],
    *,
    tracked_deleted: Sequence[str],
    accepted_deleted: Sequence[str],
    unaccepted_deleted: Sequence[str],
    local_generated: Sequence[str],
    uncovered_dirty: Sequence[str],
) -> None:
    print("=== RELEASE SCOPE GUARD ===")
    for report in reports:
        print(
            f"- {report.name}: total={report.status.total} "
            f"modified={report.status.modified} deleted={report.status.deleted} "
            f"untracked={report.status.untracked} missing_paths={len(report.missing_paths)}"
        )
        for missing in report.missing_paths:
            print(f"  missing: {missing}")
    print(f"tracked_deletions={len(tracked_deleted)} accepted={len(accepted_deleted)} unaccepted={len(unaccepted_deleted)}")
    for path in tracked_deleted:
        prefix = "accepted_deleted" if path in set(accepted_deleted) else "deleted"
        print(f"  {prefix}: {path}")
    print(f"local_generated_candidates={len(local_generated)}")
    for path in local_generated[:80]:
        print(f"  local: {path}")
    if len(local_generated) > 80:
        print(f"  additional_local_generated={len(local_generated) - 80}")
    print(f"uncovered_dirty_paths={len(uncovered_dirty)}")
    for path in uncovered_dirty[:80]:
        print(f"  uncovered: {path}")
    if len(uncovered_dirty) > 80:
        print(f"  additional_uncovered_dirty={len(uncovered_dirty) - 80}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report or fail on release-scope blockers for the current dirty worktree."
    )
    parser.add_argument("--release-dir", default=str(DEFAULT_RELEASE_DIR))
    parser.add_argument("--accepted-deletions-file", default=str(DEFAULT_ACCEPTED_DELETIONS_FILE))
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when missing pathspecs, tracked deletions, or local/generated candidates exist",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    release_dir = pathlib.Path(args.release_dir)
    if not release_dir.is_absolute():
        release_dir = ROOT_DIR / release_dir
    pathspec_files = _iter_pathspec_files(release_dir)
    if not pathspec_files:
        print(f"release scope guard failed: no pathspec files in {release_dir}", file=sys.stderr)
        raise SystemExit(1)

    accepted_deletions_file = pathlib.Path(args.accepted_deletions_file)
    if not accepted_deletions_file.is_absolute():
        accepted_deletions_file = ROOT_DIR / accepted_deletions_file
    accepted_deleted = set(read_pathspec(accepted_deletions_file)) if accepted_deletions_file.exists() else set()
    release_pathspecs: list[str] = []
    reports = [
        build_slice_report(path, root=ROOT_DIR, accepted_missing=accepted_deleted)
        for path in pathspec_files
    ]
    for path in pathspec_files:
        release_pathspecs.extend(read_pathspec(path))
    tracked_deleted = git_lines(["diff", "--name-only", "--diff-filter=D"], root=ROOT_DIR)
    unaccepted_deleted = [path for path in tracked_deleted if path not in accepted_deleted]
    untracked = git_lines(["ls-files", "--others", "--exclude-standard"], root=ROOT_DIR)
    local_generated = match_patterns(untracked, LOCAL_GENERATED_PATTERNS)
    dirty_status = parse_status_lines(git_lines(["status", "--short", "--untracked-files=all"], root=ROOT_DIR))
    uncovered_dirty = uncovered_dirty_paths(
        dirty_status,
        release_pathspecs,
        root=ROOT_DIR,
        accepted_deleted=accepted_deleted,
    )
    _print_report(
        reports,
        tracked_deleted=tracked_deleted,
        accepted_deleted=sorted(path for path in tracked_deleted if path in accepted_deleted),
        unaccepted_deleted=unaccepted_deleted,
        local_generated=local_generated,
        uncovered_dirty=uncovered_dirty,
    )

    has_missing = any(report.missing_paths for report in reports)
    has_blockers = bool(unaccepted_deleted or local_generated or uncovered_dirty)
    if bool(args.strict) and (has_missing or has_blockers):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
