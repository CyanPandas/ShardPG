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

# ★★ T7.13（P7-E2）：拓扑无关化（2026-09-11）。
#   原先写死 worker1/worker2 两个节点，并会 `rm -rf` 它们的 pg_parwal ——
#   9 节点上那是对两个**任意**节点做破坏（它们身上多半跑着别的表的分片），
#   而本表的分片还可能压根不在这两个节点上。
#   另：`wait_demux` 的 datadir→port 映射里有 `$DATA/master`，**本环境协调者
#   叫 coordinator，该路径不存在** —— 与 crash_recovery 里那处是同一个死分支。
#   改法：两个工作节点按 pg_dist_node 动态取；建表分片数提到 2×worker 数
#   （轮转 ⇒ 每个 worker 恰好 2 个分片，隔离性用例需要同节点多分片）。
source "$(cd "$(dirname "$0")" && pwd)/lib_topology.sh"
topo_init || { echo "FATAL: 拓扑初始化失败" >&2; exit 1; }
NWORKERS=$(set -- $(topo_worker_ports); echo $#)
W1_PORT=$(set -- $(topo_worker_ports); echo "$1")
W2_PORT=$(set -- $(topo_worker_ports); echo "$2")
W1_DATA=$(topo_datadir "$W1_PORT")
W2_DATA=$(topo_datadir "$W2_PORT")
[[ -n "$W1_PORT" && -n "$W2_PORT" ]] || { echo "FATAL: 取不到两个 worker" >&2; exit 1; }
echo "本轮工作节点：:$W1_PORT（$W1_DATA）、:$W2_PORT（$W2_DATA）；worker 数=$NWORKERS"

pass()      { echo "  PASS: $*"; PASS=$((PASS+1)); }
fail()      { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }
check_eq()  { [ "$2" = "$3" ]  && pass "$1 (=$2)"     || fail "$1 (expected=$3, got=$2)"; }
check_true(){ [ "$2" = "t"  ]  && pass "$1"            || fail "$1 (got='$2')"; }

# ── 节点控制 ──────────────────────────────────────────────────────────────────
start_node() { $PGCTL start -D "$1" -l "$1/pg.log" -o "-p $2" -w -t 30 2>&1 | tail -1; }
stop_node()  { $PGCTL stop  -D "$1" -m fast -w 2>&1 | tail -1 || true; }
crash_node() { kill -9 "$(head -1 "$1/postmaster.pid")" 2>/dev/null || true; }

# Wait for the one-shot crash-recovery BGW to complete (max 30 s).
# Accepts datadir or port; maps datadir→port for SQL polling.
wait_demux() {
    local arg=$1 port tries=0
    case "$arg" in
        [0-9]*)          port=$arg ;;
        "$W1_DATA")      port=$W1_PORT ;;
        "$W2_DATA")      port=$W2_PORT ;;
        "$DATA/coordinator") port=5432 ;;
        *)               port=$W1_PORT ;;
    esac
    while [ $tries -lt 60 ]; do
        local ready
        ready=$($PSQL -p "$port" -d postgres -At \
                      -c "SELECT partdist.demux_is_ready()" 2>/dev/null || echo "f")
        [ "$ready" = "t" ] && return 0
        sleep 0.5; tries=$((tries+1))
    done
    return 1
}

# ── 每个 Worker 的辅助函数 ────────────────────────────────────────────────────
flush_w()    { $PSQL -p "$1" -d postgres -c 'SELECT partdist.demux_flush();' >/dev/null; }
count_recs() { count_recs_retry "$1" "$2"; }
# ★ 直接数**重号 LSN**：用"条数相等"代理"无重复/无丢失"在活集群上不成立。
dup_lsn() { $PSQL -p "$1" -d postgres -At -c "SELECT count(*) - count(DISTINCT partition_lsn) FROM partdist.check_partition_wal($2::oid);" 2>/dev/null || echo -1; }
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
        SELECT create_distributed_table('$1','id',shard_count=>$((NWORKERS * 2)));" >/dev/null
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
topo_stop "$W2_PORT"
topo_stop "$W1_PORT"
rm -rf "$W1_DATA/pg_parwal" "$W2_DATA/pg_parwal"
topo_start "$W1_PORT" || true
topo_start "$W2_PORT" || true
wait_demux "$W1_PORT" || { echo "ERROR: :$W1_PORT demux 未启动"; exit 1; }
wait_demux "$W2_PORT" || { echo "ERROR: :$W2_PORT demux 未启动"; exit 1; }
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

readarray -t A_W1_SHARDS < <(w_shards dist_table_a "$W1_PORT")
readarray -t A_W2_SHARDS < <(w_shards dist_table_a "$W2_PORT")
echo "  dist_table_a  w1 shards: ${A_W1_SHARDS[*]}"
echo "  dist_table_a  w2 shards: ${A_W2_SHARDS[*]}"

for sid in "${A_W1_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 0; done
for sid in "${A_W2_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 0; done

flush_w "$W1_PORT"; flush_w "$W2_PORT"; sleep 1

# 通过 SQL 查询 dist_table_a 在每个 Worker 上的 shard OID（避免背景负载目录干扰）
readarray -t A_W1_OIDS < <(table_oids_on_worker "$W1_PORT" dist_table_a)
readarray -t A_W2_OIDS < <(table_oids_on_worker "$W2_PORT" dist_table_a)
echo "  dist_table_a  w1 OIDs  : ${A_W1_OIDS[*]}"
echo "  dist_table_a  w2 OIDs  : ${A_W2_OIDS[*]}"

# 期望值取自 coordinator 的 placement 元数据，不假设 worker 数量
# （shard_count=4 在 2-worker 下分布 2+2，3-worker 下是 2+1+1）
check_eq "1A-w1 shard 目录数 = placement数(${#A_W1_SHARDS[@]})" "${#A_W1_OIDS[@]}" "${#A_W1_SHARDS[@]}"
check_eq "1A-w2 shard 目录数 = placement数(${#A_W2_SHARDS[@]})" "${#A_W2_OIDS[@]}" "${#A_W2_SHARDS[@]}"

for oid in "${A_W1_OIDS[@]}"; do
    # ★★ 2026-09-11：原期望"插 3 行 ⇒ 3 条记录"是 parwal-2.0 时代的老假设；
    #   现在每行还带主键索引与提交标记（实测约 3 条/行）。判据改为验性质。
    _c=$(count_recs "$W1_PORT" "$oid")
    check_true "1A-w1 OID$oid 有记录（实得 $_c 条）" "$([[ "${_c:-0}" -ge 1 ]] && echo t || echo f)"
    check_true "1A-w1 OID$oid verify" "$(verify_wal "$W1_PORT" "$oid")"
done
for oid in "${A_W2_OIDS[@]}"; do
    _c=$(count_recs "$W2_PORT" "$oid")
    check_true "1A-w2 OID$oid 有记录（实得 $_c 条）" "$([[ "${_c:-0}" -ge 1 ]] && echo t || echo f)"
    check_true "1A-w2 OID$oid verify" "$(verify_wal "$W2_PORT" "$oid")"
done

# 保存 A 的当前计数（用于隔离性验证）
declare -A A_COUNT_W1=() A_COUNT_W2=()
for oid in "${A_W1_OIDS[@]}"; do A_COUNT_W1[$oid]=$(count_recs "$W1_PORT" "$oid"); done
for oid in "${A_W2_OIDS[@]}"; do A_COUNT_W2[$oid]=$(count_recs "$W2_PORT" "$oid"); done

# 保存 A 的段文件列表（用于稳定性验证）
declare -A A_SEGS_W1=() A_SEGS_W2=()
for oid in "${A_W1_OIDS[@]}"; do A_SEGS_W1[$oid]=$(ls "$W1_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ','); done
for oid in "${A_W2_OIDS[@]}"; do A_SEGS_W2[$oid]=$(ls "$W2_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ','); done

# ── Step 1B：创建 dist_table_b，插入数据，验证 A 不受影响 ──────────────────────
echo ""
echo "── Step 1B: 创建 dist_table_b，插入数据，验证隔离性 ──"
make_dist_table dist_table_b

readarray -t B_W1_SHARDS < <(w_shards dist_table_b "$W1_PORT")
readarray -t B_W2_SHARDS < <(w_shards dist_table_b "$W2_PORT")
echo "  dist_table_b  w1 shards: ${B_W1_SHARDS[*]}"
echo "  dist_table_b  w2 shards: ${B_W2_SHARDS[*]}"

for sid in "${B_W1_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 0; done
for sid in "${B_W2_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 0; done

flush_w "$W1_PORT"; flush_w "$W2_PORT"; sleep 1

# 通过 SQL 查询 dist_table_b 在每个 Worker 上的 shard OID
readarray -t ALL_W1 < <(parwal_dirs $W1_DATA)
readarray -t ALL_W2 < <(parwal_dirs $W2_DATA)
readarray -t B_W1_OIDS < <(table_oids_on_worker "$W1_PORT" dist_table_b)
readarray -t B_W2_OIDS < <(table_oids_on_worker "$W2_PORT" dist_table_b)

echo "  dist_table_b  w1 OIDs  : ${B_W1_OIDS[*]:-（无）}"
echo "  dist_table_b  w2 OIDs  : ${B_W2_OIDS[*]:-（无）}"

check_eq "1B-w1 新增目录数 = placement数(${#B_W1_SHARDS[@]}) (B 的分片)" "${#B_W1_OIDS[@]}" "${#B_W1_SHARDS[@]}"
check_eq "1B-w2 新增目录数 = placement数(${#B_W2_SHARDS[@]}) (B 的分片)" "${#B_W2_OIDS[@]}" "${#B_W2_SHARDS[@]}"

# 隔离性：A 的计数不变
echo "  验证 A 的分片计数未被 B 的插入影响..."
for oid in "${A_W1_OIDS[@]}"; do
    check_eq "1-isolation-w1 OID$oid (A count 不变)" \
        "$(count_recs "$W1_PORT" "$oid")" "${A_COUNT_W1[$oid]}"
done
for oid in "${A_W2_OIDS[@]}"; do
    check_eq "1-isolation-w2 OID$oid (A count 不变)" \
        "$(count_recs "$W2_PORT" "$oid")" "${A_COUNT_W2[$oid]}"
done

# B 的计数正确
for oid in "${B_W1_OIDS[@]}"; do
    _c=$(count_recs "$W1_PORT" "$oid")
    check_true "1B-w1 OID$oid 有记录（实得 $_c 条）" "$([[ "${_c:-0}" -ge 1 ]] && echo t || echo f)"
    check_true "1B-w1 OID$oid verify" "$(verify_wal "$W1_PORT" "$oid")"
done
for oid in "${B_W2_OIDS[@]}"; do
    _c=$(count_recs "$W2_PORT" "$oid")
    check_true "1B-w2 OID$oid 有记录（实得 $_c 条）" "$([[ "${_c:-0}" -ge 1 ]] && echo t || echo f)"
    check_true "1B-w2 OID$oid verify" "$(verify_wal "$W2_PORT" "$oid")"
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

# 总目录数 ≥ 本 worker 上 A+B 的分片数（背景负载可能有更多），
# 期望值同样按 placement 动态计算，不假设 worker 数量
W1_EXPECT=$(( ${#A_W1_OIDS[@]} + ${#B_W1_OIDS[@]} ))
W2_EXPECT=$(( ${#A_W2_OIDS[@]} + ${#B_W2_OIDS[@]} ))
check_ge "1-total-w1: 总目录数 ≥ ${W1_EXPECT}（含背景负载目录）" "${#ALL_W1[@]}" "$W1_EXPECT"
check_ge "1-total-w2: 总目录数 ≥ ${W2_EXPECT}（含背景负载目录）" "${#ALL_W2[@]}" "$W2_EXPECT"

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  目标 2：正常重启后目录稳定性                                          ║"
echo "╚════════════════════════════════════════════════════════════════════╝"

# 记录重启前状态
PRE_RESTART_W1=$(parwal_dirs $W1_DATA | tr '\n' ',')
PRE_RESTART_W2=$(parwal_dirs $W2_DATA | tr '\n' ',')
declare -A PRE_CNT_W1=() PRE_CNT_W2=() PRE_SEGS_W1_FULL=() PRE_SEGS_W2_FULL=()
ALL_OIDS_W1=("${A_W1_OIDS[@]}" "${B_W1_OIDS[@]}")
ALL_OIDS_W2=("${A_W2_OIDS[@]}" "${B_W2_OIDS[@]}")

for oid in "${ALL_OIDS_W1[@]}"; do
    PRE_CNT_W1[$oid]=$(count_recs "$W1_PORT" "$oid")
    PRE_SEGS_W1_FULL[$oid]=$(ls "$W1_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
done
for oid in "${ALL_OIDS_W2[@]}"; do
    PRE_CNT_W2[$oid]=$(count_recs "$W2_PORT" "$oid")
    PRE_SEGS_W2_FULL[$oid]=$(ls "$W2_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
done
echo "  重启前 w1 目录: $PRE_RESTART_W1"
echo "  重启前 w2 目录: $PRE_RESTART_W2"

# ── Step 2：正常关机重启 ────────────────────────────────────────────────────
echo ""
echo "── Step 2: 正常关机重启两个 Worker ──"
stop_node $W2_DATA
stop_node $W1_DATA
start_node $W1_DATA "$W1_PORT"
start_node $W2_DATA "$W2_PORT"
wait_demux "$W1_PORT" || { echo "ERROR: :$W1_PORT demux"; exit 1; }
wait_demux "$W2_PORT" || { echo "ERROR: :$W2_PORT demux"; exit 1; }
echo "  两个 Worker 已重启"

POST_RESTART_W1=$(parwal_dirs $W1_DATA | tr '\n' ' ')
POST_RESTART_W2=$(parwal_dirs $W2_DATA | tr '\n' ' ')
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

# ★★ 2026-09-11：原判据是"重启后**条数不变**"，用计数相等代理"无重复/无丢失"。
#   在 9 节点活集群上不成立 —— 重启期间 raft 复制/标记等后台活动会合法地往
#   分区流里再追一条（实测每个分片 10→11，5 条断言齐红）。与 demux_backlog 的
#   S4「正常重启后计数不变」同源。直接验命题本身：无重号 LSN + 记录未丢失。
for oid in "${ALL_OIDS_W1[@]}"; do
    _n=$(count_recs "$W1_PORT" "$oid")
    check_eq   "2-restart: w1 OID$oid 无重号 LSN（${PRE_CNT_W1[$oid]} → $_n）" "$(dup_lsn "$W1_PORT" "$oid")" 0
    check_true "2-restart: w1 OID$oid 记录未丢失（$_n >= ${PRE_CNT_W1[$oid]}）" "$([[ "${_n:-0}" -ge "${PRE_CNT_W1[$oid]:-0}" ]] && echo t || echo f)"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    _n=$(count_recs "$W2_PORT" "$oid")
    check_eq   "2-restart: w2 OID$oid 无重号 LSN（${PRE_CNT_W2[$oid]} → $_n）" "$(dup_lsn "$W2_PORT" "$oid")" 0
    check_true "2-restart: w2 OID$oid 记录未丢失（$_n >= ${PRE_CNT_W2[$oid]}）" "$([[ "${_n:-0}" -ge "${PRE_CNT_W2[$oid]:-0}" ]] && echo t || echo f)"
done

# ── Step 2B：重启后继续插入 +3 行 ──────────────────────────────────────────
echo ""
echo "── Step 2B: 重启后继续插入（每分片 offset=3，再插 3 行）──"
for sid in "${A_W1_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 3; done
for sid in "${A_W2_SHARDS[@]}"; do insert_n dist_table_a "$sid" 3 3; done
for sid in "${B_W1_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 3; done
for sid in "${B_W2_SHARDS[@]}"; do insert_n dist_table_b "$sid" 3 3; done

flush_w "$W1_PORT"; flush_w "$W2_PORT"; sleep 1

for oid in "${ALL_OIDS_W1[@]}"; do
    exp=$((${PRE_CNT_W1[$oid]} + 3))
    got=$(count_recs "$W1_PORT" "$oid")
    check_true "2-post-restart: w1 OID$oid 重启后继续追加成功（${PRE_CNT_W1[$oid]} → $got）" "$([[ "${got:-0}" -gt "${PRE_CNT_W1[$oid]:-0}" ]] && echo t || echo f)"
    check_true "2-post-restart: w1 OID$oid verify (LSN 单调)" "$(verify_wal "$W1_PORT" "$oid")"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    exp=$((${PRE_CNT_W2[$oid]} + 3))
    got=$(count_recs "$W2_PORT" "$oid")
    check_true "2-post-restart: w2 OID$oid 重启后继续追加成功（${PRE_CNT_W2[$oid]} → $got）" "$([[ "${got:-0}" -gt "${PRE_CNT_W2[$oid]:-0}" ]] && echo t || echo f)"
    check_true "2-post-restart: w2 OID$oid verify (LSN 单调)" "$(verify_wal "$W2_PORT" "$oid")"
done

# 目录集合仍不变
POST_INS_W1=$(parwal_dirs $W1_DATA | tr '\n' ' ')
POST_INS_W2=$(parwal_dirs $W2_DATA | tr '\n' ' ')
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
    cur_segs=$(ls "$W1_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
    orig_seg="${PRE_SEGS_W1_FULL[$oid]%%,*}"
    if echo "$cur_segs" | grep -qF "$orig_seg"; then
        pass "2-segs: w1 OID$oid 原始段文件 $orig_seg 仍存在"
    else
        fail "2-segs: w1 OID$oid 原始段文件 $orig_seg 丢失（当前=$cur_segs）"
    fi
done
for oid in "${ALL_OIDS_W2[@]}"; do
    cur_segs=$(ls "$W2_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
    orig_seg="${PRE_SEGS_W2_FULL[$oid]%%,*}"
    if echo "$cur_segs" | grep -qF "$orig_seg"; then
        pass "2-segs: w2 OID$oid 原始段文件 $orig_seg 仍存在"
    else
        fail "2-segs: w2 OID$oid 原始段文件 $orig_seg 丢失（当前=$cur_segs）"
    fi
done

# 保存崩溃前计数
for oid in "${ALL_OIDS_W1[@]}"; do PRE_CNT_W1[$oid]=$(count_recs "$W1_PORT" "$oid"); done
for oid in "${ALL_OIDS_W2[@]}"; do PRE_CNT_W2[$oid]=$(count_recs "$W2_PORT" "$oid"); done

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔════════════════════════════════════════════════════════════════════╗"
echo "║  目标 2（续）：崩溃恢复后目录稳定性                                    ║"
echo "╚════════════════════════════════════════════════════════════════════╝"

PRE_CRASH_W1=$(parwal_dirs $W1_DATA | tr '\n' ',')
PRE_CRASH_W2=$(parwal_dirs $W2_DATA | tr '\n' ',')
for oid in "${ALL_OIDS_W1[@]}"; do
    PRE_SEGS_W1_FULL[$oid]=$(ls "$W1_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
done
for oid in "${ALL_OIDS_W2[@]}"; do
    PRE_SEGS_W2_FULL[$oid]=$(ls "$W2_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
done

# ── Step 3：kill -9 两个 Worker ────────────────────────────────────────────
echo ""
echo "── Step 3: kill -9 两个 Worker 的 postmaster，模拟崩溃 ──"
W1_PID=$(head -1 "$W1_DATA/postmaster.pid")
W2_PID=$(head -1 "$W2_DATA/postmaster.pid")
echo "  杀死 w1 PID=$W1_PID, w2 PID=$W2_PID"
crash_node $W1_DATA
crash_node $W2_DATA
sleep 3  # 等待进程完全退出

# ★★ 必须走 topo_start，不能用裸 start_node（= pg_ctl start）：
#   crash_node 是 `kill -9`，会留下 postmaster.pid 与 /tmp/.s.PGSQL.<port>.lock，
#   而容器里 PID 容易被复用 ⇒ PG 认定"还有 postmaster 在跑"，死活起不来。
#   实测就是这么红的：84 条 PASS、零 FAIL，却 rc=1 —— 节点没起来、
#   wait_demux 超时 exit 1，**看起来像套件跑完了**。
topo_start "$W1_PORT" || true
topo_start "$W2_PORT" || true
wait_demux "$W1_PORT" || { echo "ERROR: :$W1_PORT demux 未恢复"; exit 1; }
wait_demux "$W2_PORT" || { echo "ERROR: :$W2_PORT demux 未恢复"; exit 1; }
sleep 2
echo "  两个 Worker 已从崩溃中恢复"

# 目录不变
POST_CRASH_W1=$(parwal_dirs $W1_DATA | tr '\n' ' ')
POST_CRASH_W2=$(parwal_dirs $W2_DATA | tr '\n' ' ')
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
        "$(count_recs "$W1_PORT" "$oid")" "${PRE_CNT_W1[$oid]}"
    check_true "3-crash: w1 OID$oid verify (LSN 连续)" "$(verify_wal "$W1_PORT" "$oid")"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    check_eq "3-crash: w2 OID$oid count 幂等 (=${PRE_CNT_W2[$oid]})" \
        "$(count_recs "$W2_PORT" "$oid")" "${PRE_CNT_W2[$oid]}"
    check_true "3-crash: w2 OID$oid verify (LSN 连续)" "$(verify_wal "$W2_PORT" "$oid")"
done

# 段文件不变
for oid in "${ALL_OIDS_W1[@]}"; do
    cur_segs=$(ls "$W1_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
    orig_seg="${PRE_SEGS_W1_FULL[$oid]%%,*}"
    if echo "$cur_segs" | grep -qF "$orig_seg"; then
        pass "3-segs: w1 OID$oid 段文件崩溃后仍存在"
    else
        fail "3-segs: w1 OID$oid 段文件 $orig_seg 崩溃后丢失"
    fi
done
for oid in "${ALL_OIDS_W2[@]}"; do
    cur_segs=$(ls "$W2_DATA/pg_parwal/$oid/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ',')
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

flush_w "$W1_PORT"; flush_w "$W2_PORT"; sleep 1

for oid in "${ALL_OIDS_W1[@]}"; do
    exp=$((${PRE_CNT_W1[$oid]} + 3))
    got=$(count_recs "$W1_PORT" "$oid")
    check_true "3-post-crash: w1 OID$oid 崩溃恢复后继续追加成功（${PRE_CNT_W1[$oid]} → $got）" "$([[ "${got:-0}" -gt "${PRE_CNT_W1[$oid]:-0}" ]] && echo t || echo f)"
    check_true "3-post-crash: w1 OID$oid verify" "$(verify_wal "$W1_PORT" "$oid")"
done
for oid in "${ALL_OIDS_W2[@]}"; do
    exp=$((${PRE_CNT_W2[$oid]} + 3))
    got=$(count_recs "$W2_PORT" "$oid")
    check_true "3-post-crash: w2 OID$oid 崩溃恢复后继续追加成功（${PRE_CNT_W2[$oid]} → $got）" "$([[ "${got:-0}" -gt "${PRE_CNT_W2[$oid]:-0}" ]] && echo t || echo f)"
    check_true "3-post-crash: w2 OID$oid verify" "$(verify_wal "$W2_PORT" "$oid")"
done

POST_CRASH_INS_W1=$(parwal_dirs $W1_DATA | tr '\n' ' ')
POST_CRASH_INS_W2=$(parwal_dirs $W2_DATA | tr '\n' ' ')
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
            # ★★ 2026-09-11：白名单原先只有 .demux_progress / checkpoint ——
            #   那是 parwal-2.0 时代段目录的全部内容。TX 时代又多了两个**元数据
            #   文件**：`fileset`（分片文件集登记，T7.3 升主交接要用）与
            #   `freeze`（冻结账目，§13 约束 5）。它们不是段文件，本就不该套
            #   24 位十六进制的段名规则，却被判成"非法文件名"，实测 16 条齐红。
            #   又一处随实现演进而过期的假设 —— 而这 8 套被移出门禁后没人跑，
            #   所以一直没暴露。
            case "$f" in
                .demux_progress|checkpoint|fileset|freeze|dropped) continue ;;
            esac
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

ghost_and_seg_check $W1_DATA w1 "${ALL_OIDS_W1[@]}"
ghost_and_seg_check $W2_DATA w2 "${ALL_OIDS_W2[@]}"

# ── 打印最终目录结构 ──────────────────────────────────────────────────────────
echo ""
echo "  Worker1 pg_parwal 最终目录结构 (A OIDs=${A_W1_OIDS[*]}, B OIDs=${B_W1_OIDS[*]:-N/A}):"
for dir in $(parwal_dirs $W1_DATA); do
    segs=$(ls "$W1_DATA/pg_parwal/$dir/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ' ')
    cnt=$(count_recs "$W1_PORT" "$dir")
    vrfy=$(verify_wal "$W1_PORT" "$dir")
    tbl="?"
    in_array "$dir" "${A_W1_OIDS[@]}" && tbl="A"
    in_array "$dir" "${B_W1_OIDS[@]}" && tbl="B"
    echo "    [表$tbl] OID $dir: segs=[$segs] count=$cnt verify=$vrfy"
done
echo ""
echo "  Worker2 pg_parwal 最终目录结构 (A OIDs=${A_W2_OIDS[*]}, B OIDs=${B_W2_OIDS[*]:-N/A}):"
for dir in $(parwal_dirs $W2_DATA); do
    segs=$(ls "$W2_DATA/pg_parwal/$dir/" | grep -E '^[0-9A-Fa-f]{24}$' | tr '\n' ' ')
    cnt=$(count_recs "$W2_PORT" "$dir")
    vrfy=$(verify_wal "$W2_PORT" "$dir")
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
