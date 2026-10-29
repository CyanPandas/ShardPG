#!/usr/bin/env bash
# Reproduce the recorded failing DDL only in the owned VACU lab. Temporarily
# enable its built-in crash backtrace on worker2 and restore the original value.
set -euo pipefail
MODE=${1:---ddl}
[[ "$MODE" == --ddl || "$MODE" == --remaining || "$MODE" == --replay ]]
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
CONTAINER=yyt-vacu-container
[[ -f "$ROOT/.vacu-lab-root" && -f "$ROOT/source.json" ]]
[[ "$(docker inspect --format '{{index .Config.Labels "yyt.vacu.root"}}' "$CONTAINER")" == "$ROOT" ]]
DEX() { docker exec -i -u postgres -e HOME=/work/home "$CONTAINER" "$@"; }
PSQL() { DEX /work/pg-install/bin/psql -h /tmp -p 5434 -U postgres -d postgres -X -qAt -v ON_ERROR_STOP=1 "$@"; }
DATA=$(PSQL -c 'SHOW data_directory' </dev/null)
[[ "$DATA" == /work/pg-cluster-data/worker2 ]]
if [[ "$MODE" == --ddl ]]; then
    [[ "$(PSQL -c "SELECT to_regclass('public.p5repl') IS NULL AND to_regclass('public.p5repl_') IS NULL" </dev/null)" == t ]] || {
        echo 'Diagnostic fixture names are in use; refusing to modify existing tables' >&2; exit 1;
    }
fi
SAVED=$(PSQL -c 'SHOW pg_partdist.debug_segv_backtrace' </dev/null)
[[ "$SAVED" == on || "$SAVED" == off ]]
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
TRACE="$DATA/vacu-ddl-trace-$STAMP.log"
OLDLOG=$(DEX bash -c 'pm=$(head -1 "$1/postmaster.pid"); readlink "/proc/$pm/fd/2"' _ "$DATA" </dev/null)
[[ "$OLDLOG" == /work/pg-cluster-data/* ]] || OLDLOG="$DATA/vacu-after-trace.log"
changed=0
restore() {
    local rc=$?
    trap - EXIT
    if [[ "$changed" == 1 ]]; then
        # pg_ctl -w below waits until crash recovery, if any, is complete.
        for attempt in $(seq 1 30); do
            if PSQL -c "ALTER SYSTEM SET pg_partdist.debug_segv_backtrace = $SAVED" </dev/null >/dev/null; then
                DEX /work/pg-install/bin/pg_ctl -D "$DATA" -m fast restart -w -t 60 -l "$OLDLOG" </dev/null || rc=1
                break
            fi
            sleep 1
        done
        [[ "$(PSQL -c 'SHOW pg_partdist.debug_segv_backtrace' </dev/null)" == "$SAVED" ]] || rc=1
    fi
    echo "DDL trace log: $TRACE"
    DEX tail -n 30 "$TRACE" </dev/null || true
    exit "$rc"
}
trap restore EXIT
PSQL -c 'ALTER SYSTEM SET pg_partdist.debug_segv_backtrace=on' </dev/null >/dev/null
changed=1
DEX /work/pg-install/bin/pg_ctl -D "$DATA" -m fast restart -w -t 60 -l "$TRACE" </dev/null
[[ "$(PSQL -c 'SHOW pg_partdist.debug_segv_backtrace' </dev/null)" == on ]]
if [[ "$MODE" == --remaining ]]; then
    echo 'Run only the two remaining suites, with worker2 crash backtraces enabled.'
    bash "$ROOT/lab.sh" regress-remaining
    exit 0
elif [[ "$MODE" == --replay ]]; then
    echo 'Run only the replay suite, with worker2 crash backtraces enabled.'
    bash "$ROOT/lab.sh" regress-replay
    exit 0
fi
echo 'Reproduce recorded missing-LIKE-source DDL; a normal undefined-table error is expected.'
status=0
PSQL -c 'SET citus.enable_ddl_propagation=off' \
    -c 'CREATE TABLE p5repl_ (LIKE p5repl INCLUDING ALL)' </dev/null || status=$?
echo "DDL psql exit status: $status"
sleep 2
if DEX grep -E 'terminated by signal (11|6|4|7)|PANIC:|栈回溯' "$TRACE" </dev/null; then
    echo 'Crash reproduced; preserve backtrace for investigation.' >&2
    exit 1
fi
[[ "$status" != 0 ]] || { echo 'Expected undefined-table error did not occur' >&2; exit 1; }
echo 'This attempt returned an error without crashing; the previous crash remains unresolved.'
