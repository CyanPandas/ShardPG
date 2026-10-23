#!/usr/bin/env bash
# P7-N117: >768-byte historical DROP lists must reach followers even if master
# has no corresponding rows. Use only this test's reserved IDs and one new table.
set -euo pipefail
C=${CONTAINER:-pg-test-container}
PG=/work/pg-install/bin/psql
PORTS=(5432 5433 5434 5435 5436)
LO=4294967081
HI=4294967285
created=0
injected=0
q() { docker exec -i -u postgres -e PGOPTIONS=-cclient_min_messages=warning "$C" "$PG" -h /tmp -p "$1" -U postgres -X -qAt -v ON_ERROR_STOP=1 -c "$2"; }
check() { [[ "$2" == "$3" ]] || { echo "FAIL $1: $2 != $3"; exit 1; }; echo "PASS $1: $2"; }
cleanup() {
    if (( created )); then q 5432 'DROP TABLE IF EXISTS t117_live' >/dev/null; fi
    if (( injected )); then q 5434 "DELETE FROM partdist.partition_map WHERE partition_id::bigint BETWEEN $LO AND $HI" >/dev/null; fi
}
trap cleanup EXIT
check 'fixture name unused' "$(q 5432 "SELECT to_regclass('t117_live') IS NULL")" t
for p in "${PORTS[@]}"; do
    check ":$p reserved IDs unused" "$(q "$p" "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint BETWEEN $LO AND $HI")" 0
done
q 5432 'CREATE TABLE t117_live(id int PRIMARY KEY, v int, SHARD KEY(id) SHARDS 2 REPLICAS 1)' >/dev/null
created=1
q 5432 'INSERT INTO t117_live VALUES(1,100),(2,200)' >/dev/null
LOG_START=$(q 5432 'SELECT coalesce(max(log_index),0) FROM partdist.raft_log WHERE group_id=0')
q 5434 "INSERT INTO partdist.partition_map(partition_id,primary_node) SELECT x::oid,2 FROM generate_series($LO,$HI)x" >/dev/null
injected=1
check '205 historical rows only on follower' "$(q 5434 "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint BETWEEN $LO AND $HI")" 205
check 'master does not have those rows' "$(q 5432 "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint BETWEEN $LO AND $HI")" 0
check 'unregister commits all batches' "$(q 5432 "SELECT partdist.partition_map_unregister(array(SELECT x::bigint FROM generate_series($LO,$HI)x))")" 0
for p in "${PORTS[@]}"; do
    for _ in $(seq 1 30); do
        left=$(q "$p" "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint BETWEEN $LO AND $HI")
        durable=$(q "$p" "SELECT count(DISTINCT x) FROM partdist.raft_log l CROSS JOIN LATERAL jsonb_array_elements_text(l.payload->'partition_ids') x WHERE group_id=0 AND log_index>$LOG_START AND op_type='OP_PARTITION_DROP' AND committed AND x::bigint BETWEEN $LO AND $HI")
        [[ "$left" == 0 && "$durable" == 205 ]] && break
        sleep 1
    done
    check ":$p historical rows removed" "$left" 0
    check ":$p all 205 IDs durably committed" "$durable" 205
done
check 'live data preserved' "$(q 5432 "SELECT count(*)||'/'||sum(v) FROM t117_live")" '2/300'
check 'live shard registration preserved' "$(q 5432 "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN(SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t117_live'::regclass)")" 2
echo 'P7_N117_PASS'
