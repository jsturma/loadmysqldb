#!/usr/bin/env bash
set -euo pipefail

DBNAME="${1:-mytestdb}"
MIN="${MIN_RECORDS:-250}"
MAX="${MAX_RECORDS:-2500}"
SLEEP_MIN="${SLEEP_MIN:-2}"
SLEEP_MAX="${SLEEP_MAX:-10}"

if ! command -v shuf >/dev/null 2>&1; then
  echo "loadmysqldb.bash requires 'shuf' (coreutils). On macOS: brew install coreutils"
  exit 1
fi

BIN="${MYSQLDBGEN_BIN:-mysqldbgen}"

while true; do
  mapfile -t CFGS < <(ls -1 *.yaml *.yml 2>/dev/null || true)
  if [[ "${#CFGS[@]}" -eq 0 ]]; then
    echo "No *.yaml/*.yml configs in $(pwd). Copy bin/example.yaml here and edit it."
    exit 1
  fi

  CFG="$(printf '%s\n' "${CFGS[@]}" | shuf -n 1)"
  COUNT="$(shuf -i "${MIN}-${MAX}" -n 1)"
  SLEEP_SECS="$(shuf -i "${SLEEP_MIN}-${SLEEP_MAX}" -n 1)"

  echo "=== $(date) cfg=${CFG} db=${DBNAME} records=${COUNT}"
  "${BIN}" -config "./${CFG}" -dbname "${DBNAME}" -dbRecords2Process "${COUNT}"
  echo "sleep ${SLEEP_SECS}s"
  sleep "${SLEEP_SECS}"
done

