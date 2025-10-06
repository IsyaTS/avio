#!/usr/bin/env python3
"""Ensure every Jinja template declares each block at most once."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

BLOCK_PATTERN = re.compile(r"\{\%\s*block\s+([a-zA-Z0-9_]+)\s*%\}")


def find_duplicate_blocks(template_path: Path) -> dict[str, int]:
    """Return a mapping of duplicate block names to the number of occurrences."""
    content = template_path.read_text(encoding="utf-8")
    counts: dict[str, int] = {}
    for match in BLOCK_PATTERN.finditer(content):
        block_name = match.group(1)
        counts[block_name] = counts.get(block_name, 0) + 1
    return {name: count for name, count in counts.items() if count > 1}


def scan_templates(root: Path) -> list[str]:
    """Scan templates under ``root`` and report duplicate blocks."""
    errors: list[str] = []
    for path in sorted(root.rglob("*.html")):
        duplicates = find_duplicate_blocks(path)
        if duplicates:
            formatted = ", ".join(f"{name} ×{count}" for name, count in sorted(duplicates.items()))
            errors.append(f"{path.relative_to(root)}: {formatted}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "templates",
        nargs="?",
        default=Path("app/templates"),
        type=Path,
        help="Path to the templates directory (default: app/templates)",
    )
    args = parser.parse_args()
    template_root = args.templates.resolve()

    if not template_root.exists():
        print(f"Template directory not found: {template_root}", file=sys.stderr)
        return 2

    errors = scan_templates(template_root)
    if errors:
        print("Duplicate Jinja block declarations detected:", file=sys.stderr)
        for line in errors:
            print(f"  - {line}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
