#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL must be set" >&2
  exit 1
fi

COMPOSE_CMD=${COMPOSE_CMD:-docker compose}

${COMPOSE_CMD} run --rm -e DATABASE_URL="$DATABASE_URL" ops sh -c '
  set -e
  alembic upgrade head
  python - <<"PY"
import os
import psycopg

dsn = os.environ["DATABASE_URL"]
with psycopg.connect(dsn) as conn:
    cur = conn.cursor()
    for table in ("leads", "messages"):
        cur.execute(
            """
            SELECT column_name, data_type, column_default
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        print(f"Table {table} columns:")
        for name, dtype, default in cur.fetchall():
            if default:
                print(f" - {name:<24} {dtype:<18} default={default}")
            else:
                print(f" - {name:<24} {dtype}")
        print()
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'contacts'
        ORDER BY ordinal_position
        """
    )
    contacts_cols = [row[0] for row in cur.fetchall()]
    print("Contacts columns:", ", ".join(contacts_cols))
PY
'
