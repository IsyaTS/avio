"""Local pytest config for tgworker tests.

Ensures project root is on sys.path so `apps.*` imports work even when these
tests are run directly (outside the top-level `tests/` tree).
"""

from __future__ import annotations

import sys

import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def anyio_backend():
    """Force anyio tests in this package to run on asyncio backend only."""

    return "asyncio"
