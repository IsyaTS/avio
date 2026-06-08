#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts import dialog_regression


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Quality-oriented replay harness for real buyer-like dialog failures."
    )
    parser.add_argument(
        "--cases",
        default=str(ROOT_DIR / "scripts" / "dialog_quality_cases.json"),
        help="Path to quality cases JSON.",
    )
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--contact-base", type=int, default=99000000)
    parser.add_argument("--tenant", type=int, default=None)
    parser.add_argument("--channel", type=str, default=None)
    parser.add_argument("--show-ok", action="store_true")
    parser.add_argument("--full-pipeline", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    rc = asyncio.run(dialog_regression._main_async(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
