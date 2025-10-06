#!/usr/bin/env bash
set -euo pipefail

script_path="$0"
case "$script_path" in
  /*) : ;;
  *) script_path="$(pwd)/$script_path" ;;
esac

# Resolve to canonical location for portability (works with busybox readlink as well)
if command -v readlink >/dev/null 2>&1; then
  script_path="$(readlink -f "$script_path" || readlink "$script_path")"
fi

script_dir="$(cd "$(dirname "$script_path")" && pwd)"
project_root="$(cd "$script_dir/.." && pwd)"

# Normalise potential CRLF line endings once to avoid runtime issues
if [ -f "$script_path" ] && grep -q $'\r' "$script_path"; then
  tmp_file="$(mktemp)"
  tr -d '\r' < "$script_path" > "$tmp_file"
  cat "$tmp_file" > "$script_path"
  rm -f "$tmp_file"
fi

exec /usr/bin/env python3 "$project_root/scripts/project_diagnostics.py" --all "$@"
