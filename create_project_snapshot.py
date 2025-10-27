#!/usr/bin/env python3
"""Снимок репозитория: дерево проекта + содержимое ТОЛЬКО файлов с разрешёнными
расширениями: py,json,html,css,yml,yaml,docker,md,txt,sql. Вывод делится на части.
"""

from __future__ import annotations

import argparse
import math
import os
from pathlib import Path
from typing import Iterable, List

DEFAULT_PARTS = 10
STREAM_READ_SIZE = 64 * 1024  # 64 KiB

# Папки и файлы, которые исключаем из дерева и обхода.
EXCLUDED_DIR_NAMES = {
    ".git",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    "node_modules",
    "snapshot_parts",
}
EXCLUDED_FILE_NAMES = {".DS_Store"}

# Разрешённые расширения (без точки).
ALLOWED_SUFFIXES = {
    "py", "json", "html", "css", "yml", "yaml", "docker", "md", "txt", "sql",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Сделать снимок репозитория: дерево проекта и содержимое файлов "
            "с выбранными расширениями, разрезанное на несколько текстовых частей."
        )
    )
    parser.add_argument(
        "--parts",
        type=int,
        default=DEFAULT_PARTS,
        help="Сколько частей сделать (по умолчанию: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("snapshot_parts"),
        help="Куда писать части (создастся при необходимости).",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="Корень проекта (по умолчанию: текущая директория).",
    )
    return parser.parse_args()


def iter_project_files(root: Path) -> Iterable[Path]:
    """Обойти файлы проекта и вернуть только разрешённые по расширению."""
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
            continue
        if path.name in EXCLUDED_FILE_NAMES:
            continue
        suffix = path.suffix.lower().lstrip(".")
        if suffix in ALLOWED_SUFFIXES:
            yield path


def build_tree(root: Path) -> str:
    """Построить ASCII-дерево проекта (с исключениями папок/файлов)."""
    lines: List[str] = []

    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = Path(dirpath).relative_to(root)
        depth = len(rel_dir.parts)
        indent = "    " * depth

        if depth == 0:
            lines.append(".")
        else:
            lines.append(f"{indent}{rel_dir.name}")

        # Фильтруем папки in-place
        dirnames[:] = sorted(
            d for d in dirnames
            if d not in EXCLUDED_DIR_NAMES
            and not (Path(dirpath, d).name in EXCLUDED_DIR_NAMES)
        )

        # Фильтруем выводимые имена файлов только по исключениям (дерево — полное)
        for filename in sorted(filenames):
            if filename in EXCLUDED_FILE_NAMES:
                continue
            if any(part in EXCLUDED_DIR_NAMES for part in (Path(dirpath) / filename).parts):
                continue
            lines.append(f"{indent}    {filename}")

    return "\n".join(lines)


def write_full_snapshot(root: Path, temp_path: Path) -> int:
    """Записать полный снапшот в промежуточный файл и вернуть число символов."""

    total_chars = 0
    tree = build_tree(root)

    with temp_path.open("w", encoding="utf-8") as outfile:
        total_chars += outfile.write("# Project Tree\n")
        total_chars += outfile.write(tree)
        total_chars += outfile.write("\n\n# Files\n")

        for file_path in iter_project_files(root):
            relative_path = file_path.relative_to(root)
            total_chars += outfile.write(f"\n# File: {relative_path}\n")
            total_chars += outfile.write("```\n")

            ends_with_newline = False
            try:
                with file_path.open("r", encoding="utf-8", errors="replace") as infile:
                    while True:
                        chunk = infile.read(STREAM_READ_SIZE)
                        if not chunk:
                            break
                        total_chars += outfile.write(chunk)
                        ends_with_newline = chunk.endswith("\n")
            except OSError:
                placeholder = "<unable to read file>\n"
                total_chars += outfile.write(placeholder)
                ends_with_newline = placeholder.endswith("\n")

            if not ends_with_newline:
                total_chars += outfile.write("\n")

            total_chars += outfile.write("```\n")

    return total_chars


def split_snapshot(temp_path: Path, total_chars: int, parts: int, output_dir: Path) -> int:
    if parts <= 0:
        raise ValueError("Number of parts must be positive")

    output_dir.mkdir(parents=True, exist_ok=True)

    for existing_part in output_dir.glob("project_snapshot_part_*.txt"):
        if existing_part.is_file():
            existing_part.unlink()

    if total_chars == 0:
        part_size = 1
    else:
        part_size = max(1, math.ceil(total_chars / parts))

    actual_parts = max(1, math.ceil(total_chars / part_size)) if total_chars else 1

    with temp_path.open("r", encoding="utf-8") as infile:
        remaining_chars = total_chars

        for index in range(1, actual_parts + 1):
            if index < actual_parts:
                target_size = part_size
            else:
                target_size = remaining_chars or part_size

            output_file = output_dir / f"project_snapshot_part_{index:02}.txt"
            header = (
                f"Snapshot part {index} of {actual_parts}\n"
                f"Generated by create_project_snapshot.py\n"
                + "=" * 40 + "\n\n"
            )

            with output_file.open("w", encoding="utf-8") as outfile:
                outfile.write(header)

                written = 0
                while written < target_size:
                    chunk = infile.read(min(STREAM_READ_SIZE, target_size - written))
                    if not chunk:
                        break
                    outfile.write(chunk)
                    written += len(chunk)

            remaining_chars = max(0, remaining_chars - written)

    return actual_parts


def main() -> None:
    args = parse_args()
    root = args.root.resolve()
    output_dir = args.output_dir.resolve()
    temp_path = output_dir / "project_snapshot_full.txt"

    total_chars = write_full_snapshot(root, temp_path)
    actual_parts = split_snapshot(temp_path, total_chars, args.parts, output_dir)

    try:
        temp_path.unlink()
    except FileNotFoundError:
        pass

    print(f"Snapshot created with {actual_parts} part(s) in '{output_dir}'.")


if __name__ == "__main__":
    main()
