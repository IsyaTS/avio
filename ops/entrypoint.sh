#!/usr/bin/env bash
set -Eeuo pipefail

ALEMBIC_DIR="ops/alembic"
ALEMBIC_CFG="ops/alembic.ini"

if [ ! -d "$ALEMBIC_DIR" ]; then
  echo "[ops] error: migrations directory '$ALEMBIC_DIR' not found" >&2
  exit 1
fi

for var in POSTGRES_USER POSTGRES_PASSWORD POSTGRES_HOST POSTGRES_PORT POSTGRES_DB; do
  eval "value=\${$var:-}"
  if [ -z "$value" ]; then
    echo "[ops] error: $var environment variable is required" >&2
    exit 1
  fi
done

DATABASE_URL="postgresql://$POSTGRES_USER:$POSTGRES_PASSWORD@$POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
DATABASE_URL_SQLA="$(printf '%s' "$DATABASE_URL" | sed -e 's/^postgresql:/postgresql+asyncpg:/')"

export ALEMBIC_CFG
export DATABASE_URL
export DATABASE_URL_SQLA

run_psql() {
  psql "$DATABASE_URL" -At -v ON_ERROR_STOP=1 -q -c "$1"
}

run_alembic() {
  echo "[ops] running: alembic $*" >&2
  alembic -c "$ALEMBIC_CFG" -x sqlalchemy.url="$DATABASE_URL_SQLA" "$@"
}

echo "[ops] waiting for database connection" >&2
until run_psql "SELECT 1" >/dev/null 2>&1; do
  echo "[ops] database unavailable, retrying in 1s" >&2
  sleep 1
done

has_alembic_table=$(run_psql "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'alembic_version');" | tr -d '[:space:]')
has_legacy_lead_id=$(run_psql "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'leads' AND column_name = 'lead_id');" | tr -d '[:space:]')

echo "[ops] database state: has_alembic_version=$has_alembic_table has_legacy_lead_id=$has_legacy_lead_id" >&2

if [ "$has_alembic_table" = "f" ] && [ "$has_legacy_lead_id" = "t" ]; then
  echo "[ops] stamping legacy schema with revision 0001_initial_schema" >&2
  run_alembic stamp 0001_initial_schema
fi

echo "[ops] alembic history (verbose)" >&2
run_alembic history --verbose

echo "[ops] alembic heads" >&2
run_alembic heads

current_output_file=$(mktemp)
if run_alembic current >"$current_output_file" 2>&1; then
  cat "$current_output_file"
else
  cat "$current_output_file" >&2
  if grep -qi "Can't locate revision" "$current_output_file"; then
    echo "[ops] stamping database with revision 0001_initial_schema before upgrade" >&2
    run_alembic stamp 0001_initial_schema
  else
    echo "[ops] failed to determine current alembic revision" >&2
    rm -f "$current_output_file"
    exit 1
  fi
fi
rm -f "$current_output_file"

echo "[ops] upgrading database to head" >&2
run_alembic upgrade head

alembic_table_name=$(run_psql "SELECT to_regclass('alembic_version');" | tr -d '[:space:]')
if [ -n "$alembic_table_name" ]; then
  current_revision=$(run_psql "SELECT version_num FROM alembic_version ORDER BY version_num DESC LIMIT 1;" | tr -d '[:space:]')
  if [ -n "$current_revision" ]; then
    echo "[ops] alembic current revision: $current_revision" >&2
  else
    echo "[ops] alembic current revision: <unavailable>" >&2
  fi
else
  echo "[ops] alembic current revision: <unavailable>" >&2
fi

exec uvicorn ops.app.main:app --host 0.0.0.0 --port 8000 --timeout-keep-alive 5
