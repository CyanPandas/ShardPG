#!/usr/bin/env bash
# All bind mounts, sources, build products, databases and logs stay in yyt.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
[[ -f "$ROOT/.vacu-lab-root" && -f "$ROOT/source.json" ]] || { echo 'Missing VACU lab root marker' >&2; exit 1; }
mkdir -p "$ROOT/logs"
exec > >(tee -a "$ROOT/logs/lab.log") 2>&1

[[ ! -f "$ROOT/lab.env" ]] || source "$ROOT/lab.env"
export ENV_NAME=yyt-vacu
export BRANCH=shardpg-vacu
export CLONE_ROOT="$ROOT"
export N_WORKERS="${N_WORKERS:-3}"
export IMAGE="${IMAGE:-46cc50ca1017}"
[[ "$N_WORKERS" =~ ^(3|8)$ ]] || { echo 'N_WORKERS must be 3 or 8'; exit 1; }
source "$ROOT/upstream-functions.sh"
CLONE_DIR="$ROOT/work"
CONTAINER=yyt-vacu-container
OWNER_LABEL=yyt.vacu.root

DEX() { docker exec -i -u postgres -e HOME=/work/home "$CONTAINER" "$@"; }
DEX0() { docker exec -i -u 0 "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres "$@"; }

preflight() {
    command -v docker >/dev/null || die 'Docker client is unavailable'
    if ! docker info --format 'Docker server: {{.ServerVersion}}'; then
        printf '%s\n' 'BLOCKED: this session cannot access Docker; no container has been created.'
        return 1
    fi
    bash "$CLONE_DIR/pg-partdist-src/scripts/check_pg_install_patched.sh" "$CLONE_DIR/pg-install"
}

owned() {
    local label
    label=$(docker inspect --format '{{index .Config.Labels "yyt.vacu.root"}}' "$CONTAINER")
    [[ "$label" == "$ROOT" ]] || die 'Refusing to operate on an unrelated container'
}

build_lab() {
    owned
    DEX bash -ceu 'for pid in /work/pg-cluster-data/*/postmaster.pid; do
        [ ! -e "$pid" ] || { echo "Stop lab PostgreSQL nodes before rebuilding loaded libraries" >&2; exit 1; }
    done'
    DEX bash -ceu 'command -v gcc; command -v make; command -v stdbuf; test -f /usr/include/stdio.h'
    DEX bash -ceu 'cd /work/pg-partdist-src; make clean PG_CONFIG=/work/pg-install/bin/pg_config; make -j2 PG_CONFIG=/work/pg-install/bin/pg_config; make install PG_CONFIG=/work/pg-install/bin/pg_config'
    DEX bash -ceu 'cd /work/pg-raft-src; make clean PG_CONFIG=/work/pg-install/bin/pg_config; make -j2 PG_CONFIG=/work/pg-install/bin/pg_config; make install PG_CONFIG=/work/pg-install/bin/pg_config'
}

up_lab() {
    preflight
    if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
        die 'The dedicated container already exists; use verify/test/logs/stop as appropriate'
    fi
    [[ ! -d "$ROOT/work/pg-cluster-data" ]] || die 'Existing lab data found; it will not be overwritten'
    if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
        IMAGE=yyt-vacu-build:local
        docker build --tag "$IMAGE" "$CLONE_DIR"
    fi
    printf 'N_WORKERS=%q\nIMAGE=%q\n' "$N_WORKERS" "$IMAGE" > "$ROOT/lab.env"
    # All PostgreSQL instances communicate over this container's loopback.
    docker run -d --init --user 0 --entrypoint /bin/sleep --name "$CONTAINER" --network none \
        --label "$OWNER_LABEL=$ROOT" --cpus 2 --memory "${LAB_MEMORY:-2g}" \
        --mount "type=bind,src=$ROOT/work,dst=/work" \
        --mount "type=bind,src=$ROOT/tmp,dst=/tmp" \
        "$IMAGE" infinity >/dev/null
    prepare_lab_user
    build_lab
    initialize_lab
}

prepare_lab_user() {
    owned
    # Identity changes are only allowed before this lab has started PostgreSQL.
    DEX0 bash -ceu 'for pid in /work/pg-cluster-data/*/postmaster.pid; do
        [ ! -e "$pid" ] || { echo "Lab PostgreSQL is active; refusing identity changes" >&2; exit 1; }
    done'
    DEX0 env LAB_UID="$(id -u)" LAB_GID="$(id -g)" bash -ceu '
        [[ "$LAB_UID" =~ ^[0-9]+$ && "$LAB_GID" =~ ^[0-9]+$ && "$LAB_UID" -gt 0 ]]
        # Existing image groups are reused by numeric GID; never renumber them.
        if ! getent group "$LAB_GID" >/dev/null; then
            groupadd -g "$LAB_GID" "vacu_g${LAB_GID}"
        fi
        # An image may already contain another alias for the host UID. The
        # postgres alias is private to this lab container; host accounts stay put.
        if [ "$(id -u postgres)" != "$LAB_UID" ] || [ "$(id -g postgres)" != "$LAB_GID" ]; then
            usermod -o -u "$LAB_UID" -g "$LAB_GID" postgres
        fi
        test "$(id -u postgres)" = "$LAB_UID"
        test "$(id -g postgres)" = "$LAB_GID"
        mkdir -p /work/home
        chown "$LAB_UID:$LAB_GID" /work/home
        printf "Lab postgres identity: "
        id postgres
    '
}

initialize_lab() {
    owned
    [[ ! -d "$ROOT/work/pg-cluster-data" ]] || die 'Lab data already exists; refusing to overwrite it'
    wire_cluster
    configure_tso
    echo "Lab started in $ROOT; internal ports only, no host ports published."
}

configure_tso() {
    owned
    # Configure TSO before inserting any shard-managed test data.
    PSQL 5432 -v ON_ERROR_STOP=1 -c 'ALTER SYSTEM SET pg_partdist.tso_master=on' -c 'SELECT pg_reload_conf()'
    local i port
    for i in $(seq 1 "$N_NODES"); do
        port=$(node_port "$i")
        PSQL "$port" -v ON_ERROR_STOP=1 \
            -c "ALTER SYSTEM SET pg_partdist.tso_conninfo='host=/tmp port=5432 dbname=postgres user=postgres'" \
            -c 'SELECT pg_reload_conf()'
    done
    local state
    for i in $(seq 1 15); do
        state=$(PSQL 5432 -X -Atc "SELECT current_setting('pg_partdist.tso_master')||'/'||(partdist.partdist_global_safe_ts()>0)::text" 2>/dev/null) || state=""
        [[ "$state" == on/true ]] && return 0
        sleep 1
    done
    die 'Lab TSO service did not become ready'
}

test_lab() {
    owned
    # Python is a host-side test driver; PostgreSQL and VACUUM stay in the lab.
    command -v python3 >/dev/null || die 'Host Python 3 is required'
    mkdir -p "$ROOT/bin"
    cat > "$ROOT/bin/psql-vacu" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
exec docker exec -i -u postgres -e HOME=/work/home yyt-vacu-container \
    stdbuf -oL /work/pg-install/bin/psql "$@"
WRAPPER
    chmod 700 "$ROOT/bin/psql-vacu"
    VACU_TEST_DSN='host=/tmp port=5433 dbname=postgres user=postgres' \
    VACU_TEST_TSO_DSN='host=/tmp port=5432 dbname=postgres user=postgres' \
    PSQL="$ROOT/bin/psql-vacu" \
        python3 "$CLONE_DIR/pg-partdist-src/tests/local/test_vacu_contract.py"
}

diagnose_lab() {
    owned
    local i port
    echo '========== Read-only lab diagnostics =========='
    docker stats --no-stream --format 'Lab resources: CPU={{.CPUPerc}} memory={{.MemUsage}} PIDs={{.PIDs}}' "$CONTAINER" || true
    DEX bash -c 'for f in /sys/fs/cgroup/cpu.stat /sys/fs/cgroup/memory.events; do
        if [ -r "$f" ]; then echo "--- $f ---"; cat "$f"; fi
    done' || true
    for i in $(seq 1 "$N_NODES"); do
        port=$(node_port "$i")
        echo "--- node${i} :${port} ---"
        PSQL "$port" -X -v ON_ERROR_STOP=1 \
            -c 'SET statement_timeout=5000' \
            -c 'SELECT * FROM partdist.pg_raft_group_status()' \
            -c 'SELECT partition_id, primary_node, primary_term FROM partdist.partition_map ORDER BY partition_id' \
            -c 'SELECT gid, prepared FROM pg_prepared_xacts' \
            -c "SELECT name,setting,pending_restart FROM pg_settings WHERE name IN ('pg_partdist.tso_master','pg_partdist.tso_conninfo','pg_partdist.shard_vacuum_auto','pg_partdist.debug_segv_backtrace')" || true
    done
    DEX bash -ceu 'shopt -s nullglob
        for f in /work/pg-cluster-data/*.log /work/pg-cluster-data/*/*.log; do
            echo "--- $f (last 50 lines) ---"
            tail -n 50 "$f"
        done'
    DEX bash -ceu 'shopt -s nullglob
        for f in /work/pg-cluster-data/*.log /work/pg-cluster-data/*/*.log; do
            grep -n -B 35 -A 20 -E "terminated by signal (11|6|4|7)|PANIC:|栈回溯" "$f" || true
        done
        for f in /work/pg-cluster-data/*/core*; do
            [ ! -f "$f" ] || ls -lh "$f"
        done'
}

case "${1:-preflight}" in
    preflight) preflight ;;
    up) up_lab ;;
    build) preflight; build_lab ;;
    initialize) initialize_lab ;;
    configure-tso) configure_tso ;;
    prepare-user) prepare_lab_user ;;
    verify) owned; do_verify ;;
    test) test_lab ;;
    diagnose) diagnose_lab ;;
    regress|regress-remaining|regress-replay)
        owned
        [[ "$N_WORKERS" == 8 ]] || die 'Historical suites require the isolated 9-node lab (N_WORKERS=8 at initial up)'
        suites=(test_shard_vacuum_p5 test_shard_vacuum_auto_p7 test_shard_vacuum_replay_p5)
        [[ "$1" != regress-remaining ]] || suites=(test_shard_vacuum_auto_p7 test_shard_vacuum_replay_p5)
        [[ "$1" != regress-replay ]] || suites=(test_shard_vacuum_replay_p5)
        for suite in "${suites[@]}"; do
            configure_tso
            # Each suite has its own process so its errexit/EXIT behavior remains
            # active. A failed suite must prevent dependent suites from starting.
            if ! VACU_LAB_ROOT="$ROOT" CONTAINER="$CONTAINER" bash "$CLONE_DIR/pg-partdist-src/tests/$suite.sh"; then
                echo "Suite failed: $suite; subsequent suites were not started" >&2
                failure_log="$ROOT/logs/suite-failure-$suite-$(date -u +%Y%m%dT%H%M%SZ).log"
                bash "$ROOT/lab.sh" diagnose > "$failure_log" 2>&1 || true
                echo "Suite diagnostics saved: $failure_log"
                exit 1
            fi
        done
        ;;
    logs) owned; docker logs --tail 100 "$CONTAINER" ;;
    stop) owned; docker stop --time 30 "$CONTAINER" ;;
    *) die 'Usage: lab.sh {preflight|up|build|initialize|configure-tso|prepare-user|verify|test|regress|regress-remaining|diagnose|logs|stop}' ;;
esac
