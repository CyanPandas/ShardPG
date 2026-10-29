#!/usr/bin/env bash
# Run focused wrap contracts using an existing container and private databases.
set -euo pipefail
ROOT=$(pwd -P)
[[ "$ROOT" == /home/yyt/vacu32-test-* && "$ROOT" != *[^/a-zA-Z0-9_-]* ]] || exit 1
BUILD=${1:?successful remote build root required}
[[ "$BUILD" == /home/yyt/vacu32-build-* && "$BUILD" != *[^/a-zA-Z0-9_-]* ]] || exit 1
CONTAINER=yyt-vacu-container
ORIGINAL=/home/yyt/vacu-bb62a57-20261029T074050Z
SCRATCH="/tmp/${ROOT##*/}"
HOST_SCRATCH="$ORIGINAL/tmp/${ROOT##*/}"
[[ "$(docker inspect --format '{{index .Config.Labels "yyt.vacu.root"}}' "$CONTAINER")" == "$ORIGINAL" ]] || exit 1
[[ "$(docker inspect --format '{{.State.Running}}' "$CONTAINER")" == true ]] || exit 1
[[ "$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/tmp"}}{{.Source}}{{end}}{{end}}' "$CONTAINER")" == "$ORIGINAL/tmp" ]] || exit 1
for library in pg_partdist-32.so pg_partdist-ring8.so pg_raft.so; do
    test -s "$BUILD/artifacts/$library"
done
docker exec --user 0 "$CONTAINER" bash -ceu 'test ! -e "$1"; mkdir "$1"' bash "$SCRATCH"

cleanup() {
    local saved=$? variant node
    trap - EXIT
    # Only the two private pairs can be stopped, even after a failed assertion.
    for variant in production ring8; do
        for node in worker1 coordinator; do
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
    if [[ "$variant" == production ]]; then bits=32; else bits=8; fi
    echo "========== $variant: private databases in existing $CONTAINER =========="
    mkdir -p "$ROOT/$variant/bin" "$ROOT/$variant/logs"
    docker exec --user 0 "$CONTAINER" bash -ceu '
        scratch=$1
        mkdir -p "$scratch/work/pg-cluster-data" "$scratch/socket"
        cp -a /work/pg-install "$scratch/work/pg-install"
    ' bash "$SCRATCH/$variant"
    docker cp "$BUILD/artifacts/pg_partdist-$([[ "$bits" == 32 ]] && echo 32 || echo ring8).so" \
        "$CONTAINER:$SCRATCH/$variant/work/pg-install/lib/postgresql/pg_partdist.so"
    docker cp "$BUILD/artifacts/pg_raft.so" "$CONTAINER:$SCRATCH/$variant/work/pg-install/lib/postgresql/pg_raft.so"
    docker exec --user 0 "$CONTAINER" bash -ceu '
        cp "$1"/pg_partdist--1.0.sql "$2/share/postgresql/extension/pg_partdist--1.0.sql"
        chown -R postgres:postgres "$3"
    ' bash "/tmp/${BUILD##*/}/work/pg-partdist-src/sql" "$SCRATCH/$variant/work/pg-install" "$SCRATCH/$variant"
    ln -s "$HOST_SCRATCH/$variant/work" "$ROOT/$variant/work"
    export VACU_CONTAINER="$CONTAINER"
    export VACU_PG_BINDIR="$SCRATCH/$variant/work/pg-install/bin"
    export VACU_PG_DATADIR="$SCRATCH/$variant/work/pg-cluster-data/worker1"
    export VACU_TEST_DSN="host=$SCRATCH/$variant/socket port=15433 dbname=postgres user=postgres"
    export VACU_TEST_TSO_DSN="host=$SCRATCH/$variant/socket port=15432 dbname=postgres user=postgres"
    export VACU_LAB_ROOT="$ROOT/$variant" VACU_RING_BITS="$bits"
    cat > "$ROOT/$variant/bin/psql-vacu" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
exec docker exec -i --user postgres "$VACU_CONTAINER" stdbuf -oL "$VACU_PG_BINDIR/psql" "$@"
WRAPPER
    chmod 700 "$ROOT/$variant/bin/psql-vacu"
    export PSQL="$ROOT/$variant/bin/psql-vacu"

    docker exec -i --user postgres "$CONTAINER" bash -ceu '
        root=$1
        pg="$root/work/pg-install"
        test "$("$pg/bin/pg_config" --pkglibdir)" = "$pg/lib/postgresql"
        for node in coordinator worker1; do
            if test "$node" = coordinator; then port=15432; ident=1; master=on; else port=15433; ident=2; master=off; fi
            data="$root/work/pg-cluster-data/$node"
            "$pg/bin/initdb" -D "$data" -U postgres --no-locale --auth=trust >/dev/null
            cat >> "$data/postgresql.conf" <<CONFIG
port = $port
listen_addresses = '\'''\''
unix_socket_directories = '\''$root/socket'\''
shared_preload_libraries = '\''citus,pg_partdist,pg_raft'\''
dynamic_library_path = '\''$pg/lib/postgresql'\''
shared_buffers = 16MB
max_connections = 30
max_worker_processes = 32
max_prepared_transactions = 32
max_wal_size = 256MB
logging_collector = off
citus.recover_2pc_interval = -1
pg_raft.node_id = $ident
pg_raft.raft_enabled = off
pg_partdist.node_id = $ident
# Keep the legacy write router unconfigured, as in the original VACU lab.
# Its metadata tables do not exist while CREATE EXTENSION citus is running.
pg_partdist.shard_vacuum_auto = off
pg_partdist.tso_master = $master
CONFIG
            "$pg/bin/pg_ctl" -D "$data" -l "$data/startup.log" -w -t 60 start
            test "$("$pg/bin/psql" -h "$root/socket" -p "$port" -U postgres -d postgres -X -At \
                -v ON_ERROR_STOP=1 -c "SHOW pg_partdist.local_node_id")" = -1
            "$pg/bin/psql" -h "$root/socket" -p "$port" -U postgres -d postgres -v ON_ERROR_STOP=1 \
                -c "CREATE EXTENSION citus" -c "SET citus.enable_ddl_propagation=off" \
                -c "CREATE EXTENSION pg_partdist" -c "CREATE EXTENSION pg_raft" \
                -c "ALTER SYSTEM SET pg_partdist.tso_conninfo='\''host=$root/socket port=15432 dbname=postgres user=postgres'\''" \
                -c "SELECT pg_reload_conf()"
        done
    ' bash "$SCRATCH/$variant"
    echo "Run $variant VACU contracts"
    python3 "$ROOT/tests/test_vacu_contract.py" --failfast 2>&1 | tee "$ROOT/$variant/logs/contracts.log"
    echo "Run $variant wrap contracts"
    python3 "$ROOT/tests/test_xid_wrap_contract.py" XidWrapContract --failfast 2>&1 | tee "$ROOT/$variant/logs/wrap.log"
    for node in worker1 coordinator; do
        docker exec --user postgres "$CONTAINER" "$VACU_PG_BINDIR/pg_ctl" \
            -D "$SCRATCH/$variant/work/pg-cluster-data/$node" -m fast -w -t 60 stop
    done
    python3 - "$ROOT/$variant/work/pg-cluster-data" <<'HEALTH'
from pathlib import Path
import re
import sys
logs = list(Path(sys.argv[1]).rglob("*.log"))
if not logs:
    raise SystemExit("No database logs found for crash check")
crashes = []
for path in logs:
    for number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
        if re.search(r"terminated by signal (11|6|4|7)\b|PANIC:", line):
            crashes.append(f"{path}:{number}: {line}")
if crashes:
    raise SystemExit("Unexpected crash in private test databases:\n" + "\n".join(crashes))
print(f"PASS: no unexpected crash in {len(logs)} private database logs")
HEALTH
done
echo 'WRAP CONTRACTS PASS: real 32-bit boundary and repeated short-ring reuse. Replica validation remains pending.'
