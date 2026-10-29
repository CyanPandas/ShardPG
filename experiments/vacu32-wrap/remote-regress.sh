#!/usr/bin/env bash
# Historical VACU and three-replica wrap validation in the existing container.
set -euo pipefail
ROOT=$(pwd -P)
[[ "$ROOT" =~ ^/home/yyt/vacu32-regress-[0-9]{8}T[0-9]{6}Z$ ]] || exit 1
BUILD=${1:?successful build root required}
[[ "$BUILD" =~ ^/home/yyt/vacu32-build-[0-9]{8}T[0-9]{6}Z$ ]] || exit 1
MODE=${2:---all}
[[ "$MODE" == --all || "$MODE" == --replay-only ]] || exit 1
CONTAINER=yyt-vacu-container
ORIGINAL=/home/yyt/vacu-bb62a57-20261029T074050Z
SCRATCH="/tmp/${ROOT##*/}"
HOST_SCRATCH="$ORIGINAL/tmp/${ROOT##*/}"
[[ "$(docker inspect --format '{{index .Config.Labels "yyt.vacu.root"}}' "$CONTAINER")" == "$ORIGINAL" ]] || exit 1
[[ "$(docker inspect --format '{{.State.Running}}' "$CONTAINER")" == true ]] || exit 1
[[ "$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/tmp"}}{{.Source}}{{end}}{{end}}' "$CONTAINER")" == "$ORIGINAL/tmp" ]] || exit 1
for so in pg_partdist-32.so pg_partdist-ring8.so pg_raft.so; do test -s "$BUILD/artifacts/$so"; done
docker exec --user 0 "$CONTAINER" bash -ceu 'test ! -e "$1"; mkdir "$1"' bash "$SCRATCH"

cleanup() {
    local saved=$? variant node
    trap - EXIT
    for variant in production ring8; do
        for node in worker3 worker2 worker1 coordinator; do
            docker exec --user postgres "$CONTAINER" bash -ceu '
                root=$1
                if test -e "$root/work/pg-cluster-data/$2/postmaster.pid"; then
                    "$root/work/pg-install/bin/pg_ctl" -D "$root/work/pg-cluster-data/$2" -m fast -w -t 60 stop
                fi
            ' bash "$SCRATCH/$variant" "$node" || true
        done
    done
    exit "$saved"
}
trap cleanup EXIT

for variant in production ring8; do
    if [[ "$variant" == production ]]; then bits=32; suffix=32; else bits=8; suffix=ring8; fi
    echo "========== $variant: 4 private nodes, 3 data replicas, existing $CONTAINER =========="
    mkdir -p "$ROOT/$variant/tests" "$ROOT/$variant/logs"
    docker exec --user 0 "$CONTAINER" bash -ceu '
        mkdir -p "$1/work/pg-cluster-data" "$1/socket"
        cp -a /work/pg-install "$1/work/pg-install"
    ' bash "$SCRATCH/$variant"
    docker cp "$BUILD/artifacts/pg_partdist-$suffix.so" "$CONTAINER:$SCRATCH/$variant/work/pg-install/lib/postgresql/pg_partdist.so"
    docker cp "$BUILD/artifacts/pg_raft.so" "$CONTAINER:$SCRATCH/$variant/work/pg-install/lib/postgresql/pg_raft.so"
    docker exec --user 0 "$CONTAINER" bash -ceu '
        cp "$1/pg_partdist--1.0.sql" "$2/work/pg-install/share/postgresql/extension/pg_partdist--1.0.sql"
        chown -R postgres:postgres "$2"
    ' bash "/tmp/${BUILD##*/}/work/pg-partdist-src/sql" "$SCRATCH/$variant"
    ln -s "$HOST_SCRATCH/$variant/work" "$ROOT/$variant/work"

    # Adapt only private copies of the historical drivers to the new paths,
    # port range and four-node topology. Assertions and C sources stay intact.
    python3 "$ROOT/relocate-regression.py" "$ROOT" "$variant" "$SCRATCH/$variant"
    (
        export ENV_NAME="vacu32-private-$variant" N_WORKERS=3
        export CLONE_ROOT="$ROOT/$variant" SHARED_BUFFERS=8MB
        export HEARTBEAT_MS=1000 ELECTION_TIMEOUT_MS=30000
        source "$ROOT/$variant/functions.sh"
        CONTAINER=yyt-vacu-container
        CLONE_DIR="$ROOT/$variant/work"
        export CONTAINER VACU_CONTAINER="$CONTAINER" VACU_LAB_ROOT="$ROOT/$variant"
        export SHARD_XID_RING_BITS="$bits"
        pg="$SCRATCH/$variant/work/pg-install"
        sock="$SCRATCH/$variant/socket"
        DEX() { docker exec -i --user postgres -e "PGHOST=$sock" "$CONTAINER" "$@"; }
        DEX0() { docker exec -i --user 0 "$CONTAINER" "$@"; }
        PSQL() { local port=$1; shift; DEX "$pg/bin/psql" -h "$sock" -p "$port" -U postgres -d postgres -X -v ON_ERROR_STOP=1 "$@"; }
        test "$(DEX "$pg/bin/pg_config" --pkglibdir)" = "$pg/lib/postgresql"
        wire_cluster
        PSQL 15432 -c 'ALTER SYSTEM SET pg_partdist.tso_master=on' -c 'SELECT pg_reload_conf()'
        for port in 15432 15433 15434 15435; do
            PSQL "$port" -c "ALTER SYSTEM SET pg_partdist.tso_conninfo='host=$sock port=15432 dbname=postgres user=postgres'" \
                -c 'SELECT pg_reload_conf()'
        done
        ready=0
        for poll in $(seq 1 30); do
            safe=$(PSQL 15432 -Atc 'SELECT partdist.partdist_global_safe_ts()')
            if [[ "$safe" =~ ^[0-9]+$ && "$safe" -gt 0 ]]; then ready=1; break; fi
            sleep 1
        done
        [[ "$ready" == 1 ]] || { echo 'Private cluster TSO did not become ready' >&2; exit 1; }
        suites=(test_shard_vacuum_replay_p5)
        if [[ "$variant" == production && "$MODE" == --all ]]; then
            suites=(test_shard_vacuum_p5 test_shard_vacuum_auto_p7 test_shard_vacuum_replay_p5)
        fi
        for suite in "${suites[@]}"; do
            echo "========== $variant $suite =========="
            if [[ "$variant" == ring8 ]]; then export VACU_WRAP_REPLAY=1; else export VACU_WRAP_REPLAY=0; fi
            bash "$ROOT/$variant/tests/$suite.sh" 2>&1 | tee "$ROOT/$variant/logs/$suite.log"
        done
    )
    for node in worker3 worker2 worker1 coordinator; do
        docker exec --user postgres "$CONTAINER" "$SCRATCH/$variant/work/pg-install/bin/pg_ctl" \
            -D "$SCRATCH/$variant/work/pg-cluster-data/$node" -m fast -w -t 60 stop
    done
done
echo "REGRESSION PASS ($MODE): selected production suites and repeated short-ring three-replica replay."
