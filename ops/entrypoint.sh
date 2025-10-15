#!/bin/sh
set -euo pipefail
ALEMBIC_DIR="ops/alembic"
ALEMBIC_CFG="ops/alembic.ini"
if [ ! -d "$ALEMBIC_DIR" ]; then
  echo "[ops] error: migrations directory '$ALEMBIC_DIR' not found" >&2
  exit 1
fi

echo "[ops] applying database migrations via alembic" >&2
if ! alembic -c "$ALEMBIC_CFG" upgrade head; then
  echo "[ops] migration failed; exiting" >&2
  exit 1
fi
exec uvicorn ops.app.main:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 5
