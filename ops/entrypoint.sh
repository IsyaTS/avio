#!/bin/sh
set -euo pipefail

ALEMBIC_DIR="ops/alembic"
ALEMBIC_CFG="ops/alembic.ini"

if [ ! -d "$ALEMBIC_DIR" ]; then
  echo "[ops] error: migrations directory '$ALEMBIC_DIR' not found" >&2
  exit 1
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "[ops] error: DATABASE_URL environment variable is required" >&2
  exit 1
fi

export ALEMBIC_CFG

echo "[ops] inspecting database state before migrations" >&2
python <<'PY'
import asyncio
import importlib.util
import os
import pathlib
import subprocess
import sys

import asyncpg

ALEMBIC_CFG = os.environ.get("ALEMBIC_CFG", "ops/alembic.ini")
DATABASE_URL = os.environ["DATABASE_URL"]
VERSIONS_DIR = pathlib.Path("ops/alembic/versions")


def _normalize_asyncpg_dsn(dsn: str) -> str:
    prefix = "postgresql+asyncpg://"
    if dsn.startswith(prefix):
        return "postgresql://" + dsn[len(prefix):]
    return dsn


async def _introspect() -> tuple[bool, bool]:
    conn = await asyncpg.connect(_normalize_asyncpg_dsn(DATABASE_URL))
    try:
        query = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        """
        tables = {row["table_name"] for row in await conn.fetch(query)}
        return "alembic_version" in tables, "leads" in tables
    finally:
        await conn.close()


async def _fetch_current_revision() -> str | None:
    conn = await asyncpg.connect(_normalize_asyncpg_dsn(DATABASE_URL))
    try:
        try:
            row = await conn.fetchrow(
                "SELECT version_num FROM alembic_version ORDER BY version_num DESC LIMIT 1"
            )
        except asyncpg.UndefinedTableError:
            return None
    finally:
        await conn.close()

    if not row:
        return None
    value = row[0]
    getter = getattr(row, "get", None)
    if callable(getter):
        value = getter("version_num", value)
    return str(value) if value is not None else None


def _find_first_revision() -> str:
    candidates = sorted(VERSIONS_DIR.glob("0001_*.py"))
    if not candidates:
        raise RuntimeError("No 0001_*.py migration found in ops/alembic/versions")

    spec = importlib.util.spec_from_file_location("ops_alembic_first", candidates[0])
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load first migration module")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    revision = getattr(module, "revision", None)
    if not revision:
        raise RuntimeError("First migration file does not expose a revision identifier")
    return revision


async def main() -> None:
    has_alembic_table, has_leads_table = await _introspect()
    print(
        f"[ops] database state: has_alembic_version={has_alembic_table} has_leads={has_leads_table}",
        file=sys.stderr,
    )

    first_revision = _find_first_revision()

    def run_alembic(*args: str) -> None:
        cmd = ["alembic", "-c", ALEMBIC_CFG, *args]
        print(f"[ops] running: {' '.join(cmd)}", file=sys.stderr)
        subprocess.run(cmd, check=True)

    if not has_alembic_table and has_leads_table:
        print(
            f"[ops] stamping legacy schema with revision {first_revision}",
            file=sys.stderr,
        )
        run_alembic("stamp", first_revision)

    print("[ops] upgrading database to head", file=sys.stderr)
    run_alembic("upgrade", "head")

    current_revision = await _fetch_current_revision()
    if current_revision:
        print(f"[ops] alembic current revision: {current_revision}", file=sys.stderr)
    else:
        print("[ops] alembic current revision: <unavailable>", file=sys.stderr)


asyncio.run(main())
PY

exec uvicorn ops.app.main:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 5
