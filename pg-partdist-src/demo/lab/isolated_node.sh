#!/usr/bin/env bash
# Run on lab 147 by shardpg_ctl's container mode. Only our worker4 is allowed.
# All installation, configuration and new data live in the new container itself.
set -euo pipefail
C=shardpg-ddl-worker4
SOURCE=shardpg-ddl-worker2
IMAGE=shardpg-ddl-container-demo:ba76941
BIN=/work/pg-install/bin
BASE=/work/pg-cluster-data
DATA=$BASE/worker4
PEERS='1@10.77.110.148:32432,2@10.77.110.145:32433,3@10.77.110.147:32434,4@10.77.110.148:32435,5@10.77.110.147:32436'
case "${1:-}" in
create)
    [[ -z "$(docker ps -aq --filter "name=^$C$")" ]] || { echo "$C already exists; refusing overwrite" >&2; exit 1; }
    [[ -z "$(ss -ltnH 'sport = :32436')" ]] || { echo 'Port 32436 is occupied' >&2; exit 1; }
    ! docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "$IMAGE already exists; inspect before retry" >&2; exit 1; }
    [[ "$(docker inspect -f '{{index .Config.Labels "io.shardpg.project"}}' "$SOURCE")" == ddl ]]
    # The source's data is a bind mount and is excluded from the image snapshot.
    docker commit --pause=false "$SOURCE" "$IMAGE" >/dev/null
    docker create --init --name "$C" --network host --shm-size 1g \
        --label io.shardpg.project=ddl --label io.shardpg.container-demo=ba76941 \
        "$IMAGE" sleep infinity >/dev/null
    docker start "$C" >/dev/null
    docker exec -u root "$C" bash -c "mkdir -p $BASE; chown postgres:postgres $BASE" </dev/null
    docker exec "$SOURCE" cat "$BASE/.pgpass" |
        docker exec -i "$C" bash -c "umask 077; cat > $BASE/.pgpass"
    docker exec "$C" "$BIN/initdb" -D "$DATA" -U postgres -E UTF8 --no-locale >/dev/null
    docker exec "$SOURCE" tar -C "$BASE/worker2" -cf - postgresql.conf postgresql.auto.conf pg_hba.conf |
        docker exec -i "$C" tar -C "$DATA" -xf -
    for cert in server.crt server.key; do
        if docker exec "$SOURCE" test -f "$BASE/worker2/$cert"; then
            docker exec "$SOURCE" cat "$BASE/worker2/$cert" |
                docker exec -i "$C" bash -c "umask 077; cat > $DATA/$cert"
        fi
    done
    docker exec "$C" sed -i -E '/^(port|pg_raft\.node_id|pg_raft\.peers) *=/d' "$DATA/postgresql.auto.conf"
    docker exec -i "$C" bash -c "cat >> $DATA/postgresql.conf" <<CONF

# Dedicated container demo worker: copied from the running release, not host binaries.
port = 32436
pg_raft.node_id = 5
pg_raft.peers = '$PEERS'
CONF
    docker exec "$C" "$BIN/pg_ctl" -D "$DATA" -l "$DATA/pg.log" -w -t 90 start >/dev/null
    # Copy the existing SCRAM verifier without printing it or exposing it in arguments.
    docker exec "$SOURCE" "$BIN/psql" -h /tmp -p 32434 -U postgres -X -qAt -v ON_ERROR_STOP=1 \
        -c "SELECT format('ALTER ROLE postgres PASSWORD %L;',rolpassword) FROM pg_authid WHERE rolname='postgres'" |
        docker exec -i "$C" "$BIN/psql" -h /tmp -p 32436 -U postgres -X -q -v ON_ERROR_STOP=1
    docker exec -i "$C" "$BIN/psql" -h /tmp -p 32436 -U postgres -X -q -v ON_ERROR_STOP=1 <<'SQL'
CREATE EXTENSION citus;
SET citus.enable_ddl_propagation=off;
CREATE EXTENSION pg_partdist;
CREATE EXTENSION pg_raft;
SQL
    docker exec "$SOURCE" "$BIN/psql" -h /tmp -p 32434 -U postgres -X -qAt -v ON_ERROR_STOP=1 \
        -c 'COPY (SELECT node_id,hostname,port,status,last_heartbeat FROM partdist.node_map) TO STDOUT' |
        docker exec -i "$C" "$BIN/psql" -h /tmp -p 32436 -U postgres -X -q -v ON_ERROR_STOP=1 \
        -c 'COPY partdist.node_map(node_id,hostname,port,status,last_heartbeat) FROM STDIN'
    docker exec "$C" "$BIN/psql" -h /tmp -p 32436 -U postgres -X -q -v ON_ERROR_STOP=1 \
        -c "INSERT INTO partdist.node_map(node_id,hostname,port,status,last_heartbeat) VALUES(5,'10.77.110.147',32436,'active',now()) ON CONFLICT(node_id) DO NOTHING"
    [[ "$(docker exec "$SOURCE" sha256sum /work/pg-install/lib/postgresql/pg_raft.so)" == "$(docker exec "$C" sha256sum /work/pg-install/lib/postgresql/pg_raft.so)" ]]
    echo 'worker4 ready with the current container release; host installation untouched'
    ;;
destroy)
    [[ "$(docker inspect -f '{{index .Config.Labels "io.shardpg.container-demo"}}' "$C")" == ba76941 ]]
    # Refuse to delete a database which is still running.
    if docker exec "$C" "$BIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
        echo 'Stop worker4 PostgreSQL before removing its container' >&2; exit 1
    fi
    docker rm -f "$C" >/dev/null
    docker image rm "$IMAGE" >/dev/null
    echo 'Removed only the dedicated worker4 container and its snapshot image'
    ;;
*) echo 'Expected create or destroy' >&2; exit 1 ;;
esac
