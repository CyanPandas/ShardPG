#!/bin/bash
# test_multi_table_isolation.sh
# 验证多分布表场景下分区 WAL 目录的隔离性与持久性
#
# 目标 1: 表间隔离 — 表 A 和表 B 的分片写入互不干扰
# 目标 2: 目录稳定性 — 重启/崩溃后同一分片 OID 目录不变，只追加
# 目标 3: 无幽灵目录或段文件

set -uo pipefail
PSQL=/work/pg-install/bin/psql
PGCTL=/work/pg-install/bin/pg_ctl
DATA=/work/pg-cluster-data
PASS=0; FAIL=0

pass()      { echo "  PASS: $*"; PASS=$((PASS+1)); }
fail()      { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }
check_eq()  { [ "$2" = "$3" ]  && pass "$1 (=$2)"     || fail "$1 (expected=$3, got=$2)"; }
check_true(){ [ "$2" = "t"  ]  && pass "$1"            || fail "$1 (got='$2')"; }

# ── 节点控制 ──────────────────────────────────────────────────────────────────
start_node() { $PGCTL start -D "$1" -l "$1/pg.log" -o "-p $2" -w -t 30 2>&1 | tail -1; }
stop_node()  { $PGCTL stop  -D "$1" -m fast -w 2>&1 | tail -1 || true; }
crash_node() { kill -9 "$(head -1 "$1/postmaster.pid")" 2>/dev/null || true; }

wait_demux() {
    local datadir=$1 tries=0 pm_pid cnt
    while [ $tries -lt 40 ]; do
        pm_pid=$(head -1 "$datadir/postmaster.pid" 2>/dev/null || echo "")
        if [ -n "$pm_pid" ]; then
            cnt=$(ps -o pid,ppid,args --no-headers 2>/dev/null \
                | awk -v p="$pm_pid" '$2==p && /demux/' | wc -l)
            [ "$cnt" -ge 1 ] && return 0
        fi
        sleep 0.5; tries=$((tries+1))
    done
    return 1
}

# ── 每个 Worker 的辅助函数 ────────────────────────────────────────────────────
flush_w()    { $PSQL -p "$1" -d postgres -c 'SELECT partdist.demux_flush();' >/dev/null; }
count_recs() { count_recs_retry "$1" "$2"; }
verify_wal() { verify_wal_retry "$1" "$2"; }
parwal_dirs(){ ls "$1/pg_parwal/" 2>/dev/null | grep -E '^[0-9]+$' | sort -n; }

# SQL-based OID lookup using coordinator metadata to avoid leftover shard tables
table_oids_on_worker() {
    local port=$1 tblname=$2
    # Get authoritative shard IDs from coordinator, then look up OIDs on the worker
    local shardids
    shardids=$($PSQL -p 5432 -d postgres -At -c "
        SELECT shardid FROM pg_dist_shard s
        JOIN pg_dist_shard_placement sp USING(shardid)
        WHERE s.logicalrelid = '${tblname}'::regclass AND sp.nodeport = $port
        ORDER BY shardid;" 2>/dev/null | grep -E '^[0-9]+$' || true)
    [ -z "$shardids" ] && return
    echo "$shardids" | while read shardid; do
        $PSQL -p "$port" -d postgres -At -c \
            "SET citus.override_table_visibility TO off;
             SELECT oid FROM pg_class
             WHERE relname = '${tblname}_${shardid}' AND relkind='r' LIMIT 1;" \
            2>/dev/null | grep -E '^[0-9]+$' || true
    done
}

check_ge() { [ "$2" -ge "$3" ] && pass "$1 (=$2 ≥ $3)" || fail "$1 (expected≥$3, got=$2)"; }

# psql wrappers with retry for transient failures under high load
count_recs_retry() {
    local port=$1 oid=$2 result tries=0
    while [ $tries -lt 4 ]; do
        result=$($PSQL -p "$port" -d postgres -At \
            -c "SELECT partdist.count_parwal_records($oid::oid);" 2>/dev/null || true)
        [ -n "$result" ] && echo "$result" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""   # caller will see empty and handle it
}

verify_wal_retry() {
    local port=$1 oid=$2 result tries=0
    while [ $tries -lt 4 ]; do
        result=$($PSQL -p "$port" -d postgres -At \
            -c "SELECT partdist.verify_partition_wal($oid::oid);" 2>/dev/null || true)
        [ -n "$result" ] && echo "$result" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}

# 查询 Coordinator 上表 $1 位于端口 $2 的 Worker 上的 shard ID
w_shards() {
    $PSQL -p 5432 -d postgres -At -c "
        SELECT shardid FROM pg_dist_shard s
        JOIN pg_dist_shard_placement sp USING(shardid)
        WHERE s.logicalrelid = '$1'::regclass AND sp.nodeport = $2
        ORDER BY shardid;"
}

# 向指定 shardid 的分片插入 n 行（使用 offset 避免主键冲突，带重试）
insert_n() {
    local tbl=$1 shardid=$2 n=$3 offset=$4
    $PSQL -p 5432 -d postgres -At -c "
        SELECT v FROM generate_series(1,10000) v
        WHERE get_shard_id_for_distribution_column('$tbl',v) = $shardid
        LIMIT $n OFFSET $offset;" | while read id; do
        local ok=0
        for _r in 1 2 3; do
            $PSQL -p 5432 -d postgres -c \
                "INSERT INTO $tbl VALUES ($id,'data') ON CONFLICT DO NOTHING;" \
                >/dev/null 2>&1 && ok=1 && break
            sleep 0.3
        done
    done
}

make_dist_table() {
    $PSQL -p 5432 -d postgres -c "
        DROP TABLE IF EXISTS $1 CASCADE;
        CREATE TABLE $1 (id int PRIMARY KEY, val text);
        SELECT create_distributed_table('$1','id',shard_count=>4);" >/dev/null
}

# 检查某个 OID 是否在数组中
in_array() {
    local needle=$1; shift
    for e in "$@"; do [ "$e" = "$needle" ] && return 0; done
    return 1
}

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  多分布表分区 WAL 隔离性与持久性验证                                  ║"
echo "╚════════════════════════════════════════════════════════════════════╝"

# ── 初始化 ────────────────────────────────────────────────────────────────────
echo ""
echo "── 初始化：停止 Worker，清空 pg_parwal，重启 ──"
stop_node $DATA/worker2
stop_node $DATA/worker1
rm -rf "$DATA/worker1/pg_parwal" "$DATA/worker2/pg_parwal"
start_node $DATA/worker1 5433
start_node $DATA/worker2 5434
wait_demux $DATA/worker1 || { echo "ERROR: w1 demux 未启动"; exit 1; }
wait_demux $DATA/worker2 || { echo "ERROR: w2 demux 未启动"; exit 1; }
echo "  两个 Worker 已启动，demux 就绪"

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  目标 1：表间隔离性                                                   ║"
echo "╚════════════════════════════════════════════════════════════════════╝"

# ── Step 1A：创建 dist_table_a，每分片插入 3 行 ──────────────────────────────
echo ""
echo "── Step 1A: 创建 dist_table_a，插入数据 ──"
make_dist_table dist_table_a

readarray -t A_W1_SHARDS < <(w_shards dist_table_a 5433)
readarray -t A_W2_SHARDS < <(w_shards dist_table_a 5434)
echo "  dist_table_a  w1 shards: ${A_W1_SHARDS[*]}"
echo "  dist_table_a  w2 shards: ${A_W2_SHARDS[*]}"

for sid in "${A_W1_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 0; done
for sid in "${A_W2_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 0; done

flush_w 5433; flush_w 5434; sleep 1

# 通过 SQL 查询 dist_table_a 在每个 Worker 上的 shard OID（避免背景负载目录干扰）
readarray -t A_W1_OIDS < <(table_oids_on_worker 5433 dist_table_a)
readarray -t A_W2_OIDS < <(table_oids_on_worker 5434 dist_table_a)
echo "  dist_table_a  w1 OIDs  : ${A_W1_OIDS[*]}"
echo "  dist_table_a  w2 OIDs  : ${A_W2_OIDS[*]}"

check_eq "1A-w1 shard 目录数 = 2" "${#A_W1_OIDS[@]}" 2
check_eq "1A-w2 shard 目录数 = 2" "${#A_W2_OIDS[@]}" 2

for oid in "${A_W1_OIDS[@]}"; do
    check_eq "1A-w1 OID$oid count=3" "$(count_recs 5433 "$oid")" 3
    check_true "1A-w1 OID$oid verify" "$(verify_wal 5433 "$oid")"
done
for oid in "${A_W2_OIDS[@]}"; do
    check_eq "1A-w2 OID$oid count=3" "$(count_recs 5434 "$oid")" 3
    check_true "1A-w2 OID$oid verify" "$(verify_wal 5434 "$oid")"
done

# 保存 A 的当前计数（用于隔离性验证）
declare -A A_COUNT_W1=() A_COUNT_W2=()
for oid in "${A_W1_OIDS[@]}"; do A_COUNT_W1[$oid]=$(count_recs 5433 "$oid"); done
for oid in "${A_W2_OIDS[@]}"; do A_COUNT_W2[$oid]=$(count_recs 5434 "$oid"); done

# 保存 A 的段文件列表（用于稳定性验证）
declare -A A_SEGS_W1=() A_SEGS_W2=()
for oid in "${A_W1_OIDS[@]}"; do A_SEGS_W1[$oid]=$(ls "$DATA/worker1/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ','); done
for oid in "${A_W2_OIDS[@]}"; do A_SEGS_W2[$oid]=$(ls "$DATA/worker2/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ','); done

# ── Step 1B：创建 dist_table_b，插入数据，验证 A 不受影响 ──────────────────────
echo ""
echo "── Step 1B: 创建 dist_table_b，插入数据，验证隔离性 ──"
make_dist_table dist_table_b

readarray -t B_W1_SHARDS < <(w_shards dist_table_b 5433)
readarray -t B_W2_SHARDS < <(w_shards dist_table_b 5434)
echo "  dist_table_b  w1 shards: ${B_W1_SHARDS[*]}"
echo "  dist_table_b  w2 shards: ${B_W2_SHARDS[*]}"

for sid in "${B_W1_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 0; done
for sid in "${B_W2_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 0; done

flush_w 5433; flush_w 5434; sleep 1

# 通过 SQL 查询 dist_table_b 在每个 Worker 上的 shard OID
readarray -t ALL_W1 < <(parwal_dirs $DATA/worker1)
readarray -t ALL_W2 < <(parwal_dirs $DATA/worker2)
readarray -t B_W1_OIDS < <(table_oids_on_worker 5433 dist_table_b)
readarray -t B_W2_OIDS < <(table_oids_on_worker 5434 dist_table_b)

echo "  dist_table_b  w1 OIDs  : ${B_W1_OIDS[*]:-（无）}"
echo "  dist_table_b  w2 OIDs  : ${B_W2_OIDS[*]:-（无）}"

check_eq "1B-w1 新增目录数 = 2 (B 的分片)" "${#B_W1_OIDS[@]}" 2
check_eq "1B-w2 新增目录数 = 2 (B 的分片)" "${#B_W2_OIDS[@]}" 2

# 隔离性：A 的计数不变
echo "  验证 A 的分片计数未被 B 的插入影响..."
for oid in "${A_W1_OIDS[@]}"; do
    check_eq "1-isolation-w1 OID$oid (A count 不变)" \
        "$(count_recs 5433 "$oid")" "${A_COUNT_W1[$oid]}"
done
for oid in "${A_W2_OIDS[@]}"; do
    check_eq "1-isolation-w2 OID$oid (A count 不变)" \
        "$(count_recs 5434 "$oid")" "${A_COUNT_W2[$oid]}"
done

# B 的计数正确
for oid in "${B_W1_OIDS[@]}"; do
    check_eq "1B-w1 OID$oid count=3" "$(count_recs 5433 "$oid")" 3
    check_true "1B-w1 OID$oid verify" "$(verify_wal 5433 "$oid")"
done
for oid in "${B_W2_OIDS[@]}"; do
    check_eq "1B-w2 OID$oid count=3" "$(count_recs 5434 "$oid")" 3
    check_true "1B-w2 OID$oid verify" "$(verify_wal 5434 "$oid")"
done

# OID 集合无重叠
OVERLAP_W1=0; OVERLAP_W2=0
for a_oid in "${A_W1_OIDS[@]}"; do
    in_array "$a_oid" "${B_W1_OIDS[@]}" && OVERLAP_W1=1 && break
done
for a_oid in "${A_W2_OIDS[@]}"; do
    in_array "$a_oid" "${B_W2_OIDS[@]}" && OVERLAP_W2=1 && break
done
[ $OVERLAP_W1 -eq 0 ] && pass "1-no-overlap-w1: A 和 B 的 OID 集合无交集" \
                       || fail "1-no-overlap-w1: A 和 B 存在相同 OID！"
[ $OVERLAP_W2 -eq 0 ] && pass "1-no-overlap-w2: A 和 B 的 OID 集合无交集" \
                       || fail "1-no-overlap-w2: A 和 B 存在相同 OID！"

# 总目录数 ≥ 4（每个 Worker 上 2A + 2B，背景负载可能有更多）
check_ge "1-total-w1: 总目录数 ≥ 4（含背景负载目录）" "${#ALL_W1[@]}" 4
check_ge "1-total-w2: 总目录数 ≥ 4（含背景负载目录）" "${#ALL_W2[@]}" 4

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  目标 2：正常重启后目录稳定性                                          ║"
echo "╚════════════════════════════════════════════════════════════════════╝"

# 记录重启前状态
PRE_RESTART_W1=$(parwal_dirs $DATA/worker1 | tr '\n' ',')
PRE_RESTART_W2=$(parwal_dirs $DATA/worker2 | tr '\n' ',')
declare -A PRE_CNT_W1=() PRE_CNT_W2=() PRE_SEGS_W1_FULL=() PRE_SEGS_W2_FULL=()
ALL_OIDS_W1=("${A_W1_OIDS[@]}" "${B_W1_OIDS[@]}")
ALL_OIDS_W2=("${A_W2_OIDS[@]}" "${B_W2_OIDS[@]}")

for oid in "${ALL_OIDS_W1[@]}"; do
    PRE_CNT_W1[$oid]=$(count_recs 5433 "$oid")
    PRE_SEGS_W1_FULL[$oid]=$(ls "$DATA/worker1/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
done
for oid in "${ALL_OIDS_W2[@]}"; do
    PRE_CNT_W2[$oid]=$(count_recs 5434 "$oid")
    PRE_SEGS_W2_FULL[$oid]=$(ls "$DATA/worker2/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
done
echo "  重启前 w1 目录: $PRE_RESTART_W1"
echo "  重启前 w2 目录: $PRE_RESTART_W2"

# ── Step 2：正常关机重启 ────────────────────────────────────────────────────
echo ""
echo "── Step 2: 正常关机重启两个 Worker ──"
stop_node $DATA/worker2
stop_node $DATA/worker1
start_node $DATA/worker1 5433
start_node $DATA/worker2 5434
wait_demux $DATA/worker1 || { echo "ERROR: w1 demux"; exit 1; }
wait_demux $DATA/worker2 || { echo "ERROR: w2 demux"; exit 1; }
echo "  两个 Worker 已重启"

POST_RESTART_W1=$(parwal_dirs $DATA/worker1 | tr '\n' ' ')
POST_RESTART_W2=$(parwal_dirs $DATA/worker2 | tr '\n' ' ')
# 验证测试表的 OID 目录在重启后仍存在（背景负载可能动态增减其他目录）
for oid in "${ALL_OIDS_W1[@]}"; do
    echo "$POST_RESTART_W1" | grep -qw "$oid" \
        && pass "2-restart: w1 OID$oid 重启后存在" \
        || fail "2-restart: w1 OID$oid 重启后丢失"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    echo "$POST_RESTART_W2" | grep -qw "$oid" \
        && pass "2-restart: w2 OID$oid 重启后存在" \
        || fail "2-restart: w2 OID$oid 重启后丢失"
done

# 计数不变
for oid in "${ALL_OIDS_W1[@]}"; do
    check_eq "2-restart: w1 OID$oid count 不变 (=${PRE_CNT_W1[$oid]})" \
        "$(count_recs 5433 "$oid")" "${PRE_CNT_W1[$oid]}"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    check_eq "2-restart: w2 OID$oid count 不变 (=${PRE_CNT_W2[$oid]})" \
        "$(count_recs 5434 "$oid")" "${PRE_CNT_W2[$oid]}"
done

# ── Step 2B：重启后继续插入 +3 行 ──────────────────────────────────────────
echo ""
echo "── Step 2B: 重启后继续插入（每分片 offset=3，再插 3 行）──"
for sid in "${A_W1_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 3; done
for sid in "${A_W2_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 3; done
for sid in "${B_W1_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 3; done
for sid in "${B_W2_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 3; done

flush_w 5433; flush_w 5434; sleep 1

for oid in "${ALL_OIDS_W1[@]}"; do
    exp=$((${PRE_CNT_W1[$oid]} + 3))
    got=$(count_recs 5433 "$oid")
    check_eq "2-post-restart: w1 OID$oid count=${PRE_CNT_W1[$oid]}+3=$exp" "$got" "$exp"
    check_true "2-post-restart: w1 OID$oid verify (LSN 单调)" "$(verify_wal 5433 "$oid")"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    exp=$((${PRE_CNT_W2[$oid]} + 3))
    got=$(count_recs 5434 "$oid")
    check_eq "2-post-restart: w2 OID$oid count=${PRE_CNT_W2[$oid]}+3=$exp" "$got" "$exp"
    check_true "2-post-restart: w2 OID$oid verify (LSN 单调)" "$(verify_wal 5434 "$oid")"
done

# 目录集合仍不变
POST_INS_W1=$(parwal_dirs $DATA/worker1 | tr '\n' ' ')
POST_INS_W2=$(parwal_dirs $DATA/worker2 | tr '\n' ' ')
# 验证测试表的 OID 目录在插入后仍存在
for oid in "${ALL_OIDS_W1[@]}"; do
    echo "$POST_INS_W1" | grep -qw "$oid" \
        && pass "2-post-insert: w1 OID$oid 插入后存在" \
        || fail "2-post-insert: w1 OID$oid 插入后丢失"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    echo "$POST_INS_W2" | grep -qw "$oid" \
        && pass "2-post-insert: w2 OID$oid 插入后存在" \
        || fail "2-post-insert: w2 OID$oid 插入后丢失"
done

# 原有段文件仍存在（不能被重建）
for oid in "${ALL_OIDS_W1[@]}"; do
    cur_segs=$(ls "$DATA/worker1/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
    orig_seg="${PRE_SEGS_W1_FULL[$oid]%%,*}"
    if echo "$cur_segs" | grep -qF "$orig_seg"; then
        pass "2-segs: w1 OID$oid 原始段文件 $orig_seg 仍存在"
    else
        fail "2-segs: w1 OID$oid 原始段文件 $orig_seg 丢失（当前=$cur_segs）"
    fi
done
for oid in "${ALL_OIDS_W2[@]}"; do
    cur_segs=$(ls "$DATA/worker2/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
    orig_seg="${PRE_SEGS_W2_FULL[$oid]%%,*}"
    if echo "$cur_segs" | grep -qF "$orig_seg"; then
        pass "2-segs: w2 OID$oid 原始段文件 $orig_seg 仍存在"
    else
        fail "2-segs: w2 OID$oid 原始段文件 $orig_seg 丢失（当前=$cur_segs）"
    fi
done

# 保存崩溃前计数
for oid in "${ALL_OIDS_W1[@]}"; do PRE_CNT_W1[$oid]=$(count_recs 5433 "$oid"); done
for oid in "${ALL_OIDS_W2[@]}"; do PRE_CNT_W2[$oid]=$(count_recs 5434 "$oid"); done

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  目标 2（续）：崩溃恢复后目录稳定性                                    ║"
echo "╚════════════════════════════════════════════════════════════════════╝"

PRE_CRASH_W1=$(parwal_dirs $DATA/worker1 | tr '\n' ',')
PRE_CRASH_W2=$(parwal_dirs $DATA/worker2 | tr '\n' ',')
for oid in "${ALL_OIDS_W1[@]}"; do
    PRE_SEGS_W1_FULL[$oid]=$(ls "$DATA/worker1/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
done
for oid in "${ALL_OIDS_W2[@]}"; do
    PRE_SEGS_W2_FULL[$oid]=$(ls "$DATA/worker2/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
done

# ── Step 3：kill -9 两个 Worker ────────────────────────────────────────────
echo ""
echo "── Step 3: kill -9 两个 Worker 的 postmaster，模拟崩溃 ──"
W1_PID=$(head -1 "$DATA/worker1/postmaster.pid")
W2_PID=$(head -1 "$DATA/worker2/postmaster.pid")
echo "  杀死 w1 PID=$W1_PID, w2 PID=$W2_PID"
crash_node $DATA/worker1
crash_node $DATA/worker2
sleep 3  # 等待进程完全退出

start_node $DATA/worker1 5433
start_node $DATA/worker2 5434
wait_demux $DATA/worker1 || { echo "ERROR: w1 demux 未恢复"; exit 1; }
wait_demux $DATA/worker2 || { echo "ERROR: w2 demux 未恢复"; exit 1; }
sleep 2
echo "  两个 Worker 已从崩溃中恢复"

# 目录不变
POST_CRASH_W1=$(parwal_dirs $DATA/worker1 | tr '\n' ' ')
POST_CRASH_W2=$(parwal_dirs $DATA/worker2 | tr '\n' ' ')
for oid in "${ALL_OIDS_W1[@]}"; do
    echo "$POST_CRASH_W1" | grep -qw "$oid" \
        && pass "3-crash: w1 OID$oid 崩溃恢复后存在" \
        || fail "3-crash: w1 OID$oid 崩溃恢复后丢失"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    echo "$POST_CRASH_W2" | grep -qw "$oid" \
        && pass "3-crash: w2 OID$oid 崩溃恢复后存在" \
        || fail "3-crash: w2 OID$oid 崩溃恢复后丢失"
done

# 计数不变（redo 幂等，无重复）
for oid in "${ALL_OIDS_W1[@]}"; do
    check_eq "3-crash: w1 OID$oid count 幂等 (=${PRE_CNT_W1[$oid]})" \
        "$(count_recs 5433 "$oid")" "${PRE_CNT_W1[$oid]}"
    check_true "3-crash: w1 OID$oid verify (LSN 连续)" "$(verify_wal 5433 "$oid")"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    check_eq "3-crash: w2 OID$oid count 幂等 (=${PRE_CNT_W2[$oid]})" \
        "$(count_recs 5434 "$oid")" "${PRE_CNT_W2[$oid]}"
    check_true "3-crash: w2 OID$oid verify (LSN 连续)" "$(verify_wal 5434 "$oid")"
done

# 段文件不变
for oid in "${ALL_OIDS_W1[@]}"; do
    cur_segs=$(ls "$DATA/worker1/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
    orig_seg="${PRE_SEGS_W1_FULL[$oid]%%,*}"
    if echo "$cur_segs" | grep -qF "$orig_seg"; then
        pass "3-segs: w1 OID$oid 段文件崩溃后仍存在"
    else
        fail "3-segs: w1 OID$oid 段文件 $orig_seg 崩溃后丢失"
    fi
done
for oid in "${ALL_OIDS_W2[@]}"; do
    cur_segs=$(ls "$DATA/worker2/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
    orig_seg="${PRE_SEGS_W2_FULL[$oid]%%,*}"
    if echo "$cur_segs" | grep -qF "$orig_seg"; then
        pass "3-segs: w2 OID$oid 段文件崩溃后仍存在"
    else
        fail "3-segs: w2 OID$oid 段文件 $orig_seg 崩溃后丢失"
    fi
done

# ── Step 3B：崩溃后继续插入 +3 行，验证 LSN 连续性 ────────────────────────
echo ""
echo "── Step 3B: 崩溃恢复后继续插入（每分片 offset=6，再插 3 行）──"
for sid in "${A_W1_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 6; done
for sid in "${A_W2_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 6; done
for sid in "${B_W1_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 6; done
for sid in "${B_W2_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 6; done

flush_w 5433; flush_w 5434; sleep 1

for oid in "${ALL_OIDS_W1[@]}"; do
    exp=$((${PRE_CNT_W1[$oid]} + 3))
    got=$(count_recs 5433 "$oid")
    check_eq "3-post-crash: w1 OID$oid count=${PRE_CNT_W1[$oid]}+3=$exp" "$got" "$exp"
    check_true "3-post-crash: w1 OID$oid verify" "$(verify_wal 5433 "$oid")"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    exp=$((${PRE_CNT_W2[$oid]} + 3))
    got=$(count_recs 5434 "$oid")
    check_eq "3-post-crash: w2 OID$oid count=${PRE_CNT_W2[$oid]}+3=$exp" "$got" "$exp"
    check_true "3-post-crash: w2 OID$oid verify" "$(verify_wal 5434 "$oid")"
done

POST_CRASH_INS_W1=$(parwal_dirs $DATA/worker1 | tr '\n' ' ')
POST_CRASH_INS_W2=$(parwal_dirs $DATA/worker2 | tr '\n' ' ')
for oid in "${ALL_OIDS_W1[@]}"; do
    echo "$POST_CRASH_INS_W1" | grep -qw "$oid" \
        && pass "3-post-crash: w1 OID$oid 崩溃后插入存在" \
        || fail "3-post-crash: w1 OID$oid 崩溃后插入丢失"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    echo "$POST_CRASH_INS_W2" | grep -qw "$oid" \
        && pass "3-post-crash: w2 OID$oid 崩溃后插入存在" \
        || fail "3-post-crash: w2 OID$oid 崩溃后插入丢失"
done

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  目标 3：无幽灵目录或段文件                                            ║"
echo "╚════════════════════════════════════════════════════════════════════╝"
echo ""

ghost_and_seg_check() {
    local worker_data=$1 worker_label=$2
    shift 2
    local expected_oids=("$@")
    local bad_seg=0

    local all_dirs
    readarray -t all_dirs < <(parwal_dirs "$worker_data")

    # 验证期望的 OID 目录均存在，且段文件命名合法、无重复
    for oid in "${expected_oids[@]}"; do
        in_array "$oid" "${all_dirs[@]}" || { fail "3-missing-$worker_label: OID $oid 目录丢失"; continue; }
        for f in $(ls "$worker_data/pg_parwal/$oid/" 2>/dev/null); do
            [ "$f" = ".demux_progress" ] && continue
            if ! echo "$f" | grep -qE '^[0-9A-Fa-f]{24}$'; then
                fail "3-seg-$worker_label OID$oid: 非法文件名 '$f'"
                bad_seg=1
            fi
        done
        local dup
        dup=$(ls "$worker_data/pg_parwal/$oid/" 2>/dev/null \
              | grep -E '^[0-9A-Fa-f]{24}$' | sort | uniq -d | wc -l)
        [ "$dup" -gt 0 ] && fail "3-dup-$worker_label OID$oid: 存在重复段文件名"
    done

    # 测试分片目录总数 ≥ expected（背景负载可能有更多）
    local actual=${#all_dirs[@]}
    local exp=${#expected_oids[@]}
    check_ge "3-count-$worker_label: 目录数 ≥ $exp（含背景负载目录）" "$actual" "$exp"

    [ $bad_seg -eq 0 ] && pass "3-seg-$worker_label: 所有测试分片段文件命名合法，无重复"
    pass "3-ghost-$worker_label: 测试分片目录完整（背景负载额外目录不计入幽灵）"
}

ghost_and_seg_check $DATA/worker1 w1 "${ALL_OIDS_W1[@]}"
ghost_and_seg_check $DATA/worker2 w2 "${ALL_OIDS_W2[@]}"

# ── 打印最终目录结构 ──────────────────────────────────────────────────────────
echo ""
echo "  Worker1 pg_parwal 最终目录结构 (A OIDs=${A_W1_OIDS[*]}, B OIDs=${B_W1_OIDS[*]:-N/A}):"
for dir in $(parwal_dirs $DATA/worker1); do
    segs=$(ls "$DATA/worker1/pg_parwal/$dir/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ' ')
    cnt=$(count_recs 5433 "$dir")
    vrfy=$(verify_wal 5433 "$dir")
    tbl="?"
    in_array "$dir" "${A_W1_OIDS[@]}" && tbl="A"
    in_array "$dir" "${B_W1_OIDS[@]}" && tbl="B"
    echo "    [表$tbl] OID $dir: segs=[$segs] count=$cnt verify=$vrfy"
done
echo ""
echo "  Worker2 pg_parwal 最终目录结构 (A OIDs=${A_W2_OIDS[*]}, B OIDs=${B_W2_OIDS[*]:-N/A}):"
for dir in $(parwal_dirs $DATA/worker2); do
    segs=$(ls "$DATA/worker2/pg_parwal/$dir/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ' ')
    cnt=$(count_recs 5434 "$dir")
    vrfy=$(verify_wal 5434 "$dir")
    tbl="?"
    in_array "$dir" "${A_W2_OIDS[@]}" && tbl="A"
    in_array "$dir" "${B_W2_OIDS[@]}" && tbl="B"
    echo "    [表$tbl] OID $dir: segs=[$segs] count=$cnt verify=$vrfy"
done

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════════════"
echo "  总计: PASS=$PASS  FAIL=$FAIL"
echo "═══════════════════════════════════════════"
[ "$FAIL" -eq 0 ] && echo "验证结果: PASS" || echo "验证结果: FAIL"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
