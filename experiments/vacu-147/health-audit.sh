#!/usr/bin/env bash
# Finish the log/availability audit for the successful 09:03:42 replay run.
# No PostgreSQL restarts, configuration changes, fixtures or writes.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
CONTAINER=yyt-vacu-container
[[ -f "$ROOT/.vacu-lab-root" && -f "$ROOT/source.json" ]]
[[ "$(docker inspect --format '{{index .Config.Labels "yyt.vacu.root"}}' "$CONTAINER")" == "$ROOT" ]]
PASS=0; FAIL=0
check() {
    if [[ -n "$2" && "$2" == "$3" ]]; then
        echo "  PASS  $1"; PASS=$((PASS+1))
    else
        echo "  FAIL  $1 (actual='$2' expected='$3')"; FAIL=$((FAIL+1))
    fi
}
source "$ROOT/work/pg-partdist-src/tests/lib_node_health.sh"
_HEALTH_START_TS='2026-10-29 09:03:42'
echo "Replay health audit: $_HEALTH_START_TS UTC through now"
for port in $(seq 5432 5440); do
    value=$(docker exec -i -u postgres "$CONTAINER" /work/pg-install/bin/psql \
        -h /tmp -p "$port" -U postgres -d postgres -X -qAt -v ON_ERROR_STOP=1 \
        -c 'SELECT 1' </dev/null) || value=unavailable
    check "node :$port available" "$value" 1
done
health_check_no_crash
echo "Replay health audit: PASS=$PASS FAIL=$FAIL"
[[ "$FAIL" == 0 ]]
