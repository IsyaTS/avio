#!/bin/sh
set -e
ALEMBIC_DIR="app/ops/alembic"
ALEMBIC_CFG="app/ops/alembic.ini"
if [ ! -d "$ALEMBIC_DIR" ]; then
  echo "[ops] warning: migrations directory '$ALEMBIC_DIR' not found; skipping alembic upgrade" >&2
else
  echo "[ops] applying database migrations via alembic" >&2
  alembic -c "$ALEMBIC_CFG" upgrade head
fi
exec uvicorn ops.app.main:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 5
