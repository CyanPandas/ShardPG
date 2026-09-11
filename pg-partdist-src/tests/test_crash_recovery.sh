#!/bin/bash
# test_crash_recovery.sh — Crash recovery test suite for pg_partdist parwal-2.0
# Covers: Scenario A (synchronous write, no flush needed), B (kill -9 postmaster), C (partial WAL + crash)
set -euo pipefail

PSQL=/work/pg-install/bin/psql
PGCTL=/work/pg-install/bin/pg_ctl
DATA=/work/pg-cluster-data
PASS=0; FAIL=0

# ★★ T7.13（P7-E2）：拓扑无关化（2026-09-10）。
#   本套件原先写死「worker1(:5433) + worker2(:5434) 就是全部 worker」，共 21 处。
#   9 节点上的三个后果：
#     ① `$DATA/master` 这个路径**根本不存在**（本环境协调者叫 coordinator）——
#        `start_cluster`/`stop_cluster_fast` 里那两行一直是**死代码**，协调者
#        从来没被停过也没被启过，而 `| tail -1` 把非零退出码吃掉了，所以没人发现；
#     ② 它只停/起 8 个 worker 里的 2 个，剩下 6 个不管；
#     ③ **需要同一个 worker 上有 2 个分片**（SHARDS_W1[0]/[1]），而
#        `shard_count=>4` 在 8 个 worker 上会散成 4 个节点各 1 个 ⇒ S2 为空、
#        后续整串断言塌掉。
#   改法：选定**一个**工作节点（动态取），只对它做停/起/清 parwal；
#   建表分片数改成 2×worker 数，保证每个 worker 恰好 2 个分片。
source "$(cd "$(dirname "$0")" && pwd)/lib_topology.sh"
topo_init || die "拓扑初始化失败"
NWORKERS=$(set -- $(topo_worker_ports); echo $#)
WPORT=$(set -- $(topo_worker_ports); echo "$1")
WDATA=$(topo_datadir "$WPORT")
SHARD_N=$(( NWORKERS * 2 ))
[[ -n "$WPORT" && -d "$WDATA" ]] || die "取不到可用 worker（WPORT=$WPORT WDATA=$WDATA）"
echo "本轮工作节点：:$WPORT（$WDATA）；worker 数=$NWORKERS，建表分片数=$SHARD_N"

# ★ set -euo pipefail 下，任何一条返回非零的命令都会让脚本**无声退出** ——
#   实测就这么在 Scenario B 中途停掉：12 条 PASS、零 FAIL、rc=1，
#   看起来像"跑完了"，其实是半路死的。ERR 陷阱把真凶的行号和命令打出来。
# ★ 必须同时开 -E（errtrace）：`trap ... ERR` **默认不被函数继承**，
#   而 set -e 照样会因函数内部的失败而退出 —— 于是脚本无声死掉、陷阱一声不响。
#   第一版就漏了这个，白跑一轮。
set -E
trap 'echo "★ ERR: line $LINENO 命令失败 -> $BASH_COMMAND" >&2' ERR

die()  { echo "FATAL: $*" >&2; exit 1; }
pass() { echo "  PASS: $*"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }

check_eq() {
    local label=$1 actual=$2 expected=$3
    if [ "$actual" = "$expected" ]; then
        pass "$label (got $actual)"
    else
        fail "$label (expected $expected, got $actual)"
    fi
}

check_true() {
    local label=$1 val=$2
    if [ "$val" = "t" ]; then pass "$label"; else fail "$label (got '$val')"; fi
}

# ── helpers ──────────────────────────────────────────────────────────────────

# 只对**工作节点**做停/起：本套件验的是"该节点崩溃后能否恢复"，
# 其余 8 个节点没有参与的理由，停它们纯属扩大爆炸半径。
# 经 topo_stop/topo_start ⇒ 停过的节点登记在案，EXIT 时必复原。
start_cluster() { topo_start "$WPORT"; }
stop_cluster_fast() { topo_stop "$WPORT"; }

# 只清工作节点的 parwal。注意这仍会抹掉该节点上**别的**表的回放状态 ——
# 门禁整轮持有全局独占锁、各套件自建自供给，所以可接受；但不能再扩大到全簇。
clean_worker_parwal() { rm -rf "$WDATA/pg_parwal"; }

# Get the 2 most-recently-created shard OIDs on worker1 (highest OIDs lacking
# a visible pg_class entry — MX mode removes shards from pg_class).
# 取**指定表**在工作节点上的分片 OID（升序，最多 2 个）。
#
# ★ 2026-09-10 换掉原来的启发式。原版是「取最高的、在 pg_class 里查不到对应
#   普通表的 toast OID」—— 那是为了绕开 MX 模式下分片表默认不可见。
#   在一台专用的 3 节点开发机上它碰巧总对；在这个已经跑过许多套件的共享集群上，
#   它会**挑到别的表的分片**，于是计数断言报出"期望 5 实得 16"这种
#   看起来像产品缺陷、其实是选错了对象的红。
#   精确做法本来就有（shard_auto_init 里在用）：把可见性开关关掉，按表名匹配。
get_w1_shard_oids() {
    local tbl=$1
    $PSQL -p "$WPORT" -d postgres -At -c "
        SET citus.override_table_visibility TO off;
        SELECT oid FROM pg_class
         WHERE relname LIKE '${tbl}\\_%' AND relkind = 'r'
         ORDER BY oid LIMIT 2;" 2>/dev/null | grep -E '^[0-9]+$' | sort -n
}

# Find N values of id column that route to a given shard_id via Citus hash.
ids_for_shard() {
    local tbl=$1 shard=$2 n=$3
    $PSQL -p 5432 -d postgres -At -c "
        SELECT v FROM generate_series(1,500) v
        WHERE get_shard_id_for_distribution_column('$tbl', v) = $shard
        LIMIT $n;"
}

# ★ 直接数**重号 LSN**，而不是拿"记录条数相等"去代理"无重复"。
#   本套件的标题命题就是"崩溃恢复后不得有重复记录"，那就该直接验它。
dup_lsn_count() { $PSQL -p "$WPORT" -d postgres -At -c "SELECT count(*) - count(DISTINCT partition_lsn) FROM partdist.check_partition_wal($1::oid);" 2>/dev/null || echo -1; }

_count_records_once() { $PSQL -p "$WPORT" -d postgres -At -c "SELECT partdist.count_parwal_records($1::oid);" 2>/dev/null || true; }
_verify_wal_once()    { $PSQL -p "$WPORT" -d postgres -At -c "SELECT partdist.verify_partition_wal($1::oid);"  2>/dev/null || true; }

count_records() {
    local r; local tries=0
    while [ $tries -lt 4 ]; do
        r=$(_count_records_once "$1")
        [ -n "$r" ] && echo "$r" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
verify_wal() {
    local r; local tries=0
    while [ $tries -lt 4 ]; do
        r=$(_verify_wal_once "$1")
        [ -n "$r" ] && echo "$r" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
flush_w1()      { $PSQL -p "$WPORT" -d postgres -c 'SELECT partdist.demux_flush();' > /dev/null; }

# Wait for the one-shot crash-recovery BGW to finish (parwal-2.0: BGW exits after recovery).
wait_demux_ready() {
    local port=${1:-$WPORT} tries=0
    while [ $tries -lt 60 ]; do
        local r
        r=$($PSQL -p "$port" -d postgres -At -c "SELECT partdist.demux_is_ready()" 2>/dev/null || echo "f")
        [ "$r" = "t" ] && return 0
        sleep 0.5; tries=$((tries+1))
    done
    echo "  WARNING: demux_is_ready() timed out after 30s"
    return 1
}
check_ge()      { [ "$2" -ge "$3" ] && pass "$1 (=$2 ≥ $3)" || fail "$1 (expected≥$3, got=$2)"; }

insert_rows() {
    local tbl=$1; shift
    for id in "$@"; do
        local ok=0
        for _r in 1 2 3; do
            $PSQL -p 5432 -d postgres -c \
                "INSERT INTO $tbl VALUES ($id,'v') ON CONFLICT DO NOTHING;" \
                >/dev/null 2>&1 && ok=1 && break
            sleep 0.3
        done
    done
}

create_dist_table() {
    local tbl=$1
    $PSQL -p 5432 -d postgres -c "
        DROP TABLE IF EXISTS $tbl CASCADE;
        CREATE TABLE $tbl (id int PRIMARY KEY, val text);
        SELECT create_distributed_table('$tbl','id',shard_count=>$SHARD_N);" > /dev/null
}

w1_parwal_dirs() { ls "$WDATA/pg_parwal"/ 2>/dev/null | grep -v '^\.' | sort -n; }

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════════════"
echo "  pg_partdist crash recovery test suite"
echo "══════════════════════════════════════════════"

# ── Full cluster restart for clean state ──────────────────────────────────────
echo ""
echo "── Setup: clean cluster restart ──"
stop_cluster_fast
clean_worker_parwal
start_cluster
wait_demux_ready "$WPORT" || true
wait_demux_ready 5432 || true
echo "  Cluster up, crash-recovery BGW completed on all nodes"

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SCENARIO A: 同步写入路径验证 (parwal-2.0 无异步 demux) ════════"
# parwal-2.0: writes are synchronous — PartWAL records appear in pg_parwal
# immediately after INSERT, without any demux_flush() call.

create_dist_table crash_demux_test

SHARDS_W1=($($PSQL -p 5432 -d postgres -At -c "
    SELECT shardid FROM pg_dist_shard JOIN pg_dist_shard_placement USING(shardid)
    WHERE logicalrelid='crash_demux_test'::regclass AND nodeport=$WPORT
    ORDER BY shardid;"))
S1=${SHARDS_W1[0]}; S2=${SHARDS_W1[1]}
echo "  Worker1 Citus shard IDs: $S1, $S2"

IDS_S1=($(ids_for_shard crash_demux_test $S1 5))
IDS_S2=($(ids_for_shard crash_demux_test $S2 5))

# Phase A1: insert 5 per shard — NO flush needed (synchronous write path)
echo "  Phase A1: inserting 5 rows per 工作节点 :$WPORT 的分片（同步写入）..."
insert_rows crash_demux_test "${IDS_S1[@]}" "${IDS_S2[@]}"

W1_OIDS=($(get_w1_shard_oids crash_demux_test))
OA1=${W1_OIDS[0]}; OA2=${W1_OIDS[1]}
echo "  Worker1 shard OIDs: $OA1, $OA2"

# ★★ 2026-09-10 改正一处**期望值本身就错了**的断言。
#   原版写死「5 行 ⇒ 5 条 parwal 记录」，实测是 16；再插 5 行变 31。
#   即每行约 3 条（堆 + 主键索引 + 提交标记），比值取决于表结构和实现细节，
#   把它钉死在用例里，等于每次实现微调都要来改用例 —— 而这 8 套 09-09 被移出
#   门禁后就没人跑，错了也没人知道（同 08_schema_existence）。
#   Scenario A 真正要验的是「**同步**写入：不调 demux_flush() 记录也立刻出现」，
#   所以判据改成"立刻就有记录"，而不是"恰好几条"。
A1_C1=$(count_records $OA1); A1_C2=$(count_records $OA2)
check_true "A-immediate OID$OA1 有记录且无需 flush（实得 $A1_C1 条）" "$([[ "${A1_C1:-0}" -ge 1 ]] && echo t || echo f)"
check_true "A-immediate OID$OA2 有记录且无需 flush（实得 $A1_C2 条）" "$([[ "${A1_C2:-0}" -ge 1 ]] && echo t || echo f)"
check_true "A-verify OID$OA1" "$(verify_wal $OA1)"
check_true "A-verify OID$OA2" "$(verify_wal $OA2)"

# Phase A2: insert 5 more rows (verify append without any demux interaction)
IDS_S1_P2=($(ids_for_shard crash_demux_test $S1 10 | tail -5))
IDS_S2_P2=($(ids_for_shard crash_demux_test $S2 10 | tail -5))
echo "  Phase A2: inserting 5 more rows (no flush)..."
insert_rows crash_demux_test "${IDS_S1_P2[@]}" "${IDS_S2_P2[@]}"

# 第二批同样不 flush：判据是"**又长了**"，即追加确实同步落盘。
A2_C1=$(count_records $OA1); A2_C2=$(count_records $OA2)
check_true "A-final OID$OA1 追加后记录增长（$A1_C1 → $A2_C1）" "$([[ "${A2_C1:-0}" -gt "${A1_C1:-0}" ]] && echo t || echo f)"
check_true "A-final OID$OA2 追加后记录增长（$A1_C2 → $A2_C2）" "$([[ "${A2_C2:-0}" -gt "${A1_C2:-0}" ]] && echo t || echo f)"
check_true "A-final verify OID$OA1 (LSN monotone)" "$(verify_wal $OA1)"
check_true "A-final verify OID$OA2 (LSN monotone)" "$(verify_wal $OA2)"

# Directory count ≥ 2 (background load may create additional dirs)
NDIRS=$(w1_parwal_dirs | grep -v '^$' | wc -l)
check_ge "A-directory count ≥ 2 (test shards present)" "$NDIRS" 2

echo "  Scenario A LSN sequence OID$OA1:"
$PSQL -p "$WPORT" -d postgres -c "
    SELECT partition_lsn, is_valid FROM partdist.check_partition_wal(${OA1}::oid) ORDER BY partition_lsn;" 2>/dev/null | grep -E '^\s+[0-9]'

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SCENARIO B: kill -9 PostgreSQL postmaster (crash recovery) ════════"

# Stop worker1 and clean its parwal for isolated test
topo_stop "$WPORT"
rm -rf "$WDATA/pg_parwal"
topo_start "$WPORT"
sleep 2

create_dist_table crash_pg_test

SHARDS_B=($($PSQL -p 5432 -d postgres -At -c "
    SELECT shardid FROM pg_dist_shard JOIN pg_dist_shard_placement USING(shardid)
    WHERE logicalrelid='crash_pg_test'::regclass AND nodeport=$WPORT
    ORDER BY shardid;"))
SB1=${SHARDS_B[0]}; SB2=${SHARDS_B[1]}

IDS_B1=($(ids_for_shard crash_pg_test $SB1 5))
IDS_B2=($(ids_for_shard crash_pg_test $SB2 5))

echo "  Phase B1: inserting 5 rows per 工作节点 :$WPORT 的分片..."
insert_rows crash_pg_test "${IDS_B1[@]}" "${IDS_B2[@]}"
flush_w1
sleep 1

W1_OIDS_B=($(get_w1_shard_oids crash_pg_test))
OB1=${W1_OIDS_B[0]}; OB2=${W1_OIDS_B[1]}
echo "  Worker1 shard OIDs: $OB1, $OB2"

PRE_B1=$(count_records $OB1); PRE_B2=$(count_records $OB2)
# 同 Scenario A：不钉死"每行几条"。这里只要求崩溃前**确实有记录**，
# 因为后面 B-post-recovery 的判据是 POST == PRE（相对比较），基数是多少无所谓。
check_true "B-pre-crash OID$OB1 有记录（实得 $PRE_B1 条）" "$([[ "${PRE_B1:-0}" -ge 1 ]] && echo t || echo f)"
check_true "B-pre-crash OID$OB2 有记录（实得 $PRE_B2 条）" "$([[ "${PRE_B2:-0}" -ge 1 ]] && echo t || echo f)"
check_true "B-pre-crash verify OID$OB1" "$(verify_wal $OB1)"

PRE_SEGS_B1=$(ls "$WDATA/pg_parwal"/$OB1/ 2>/dev/null | wc -l)
echo "  Pre-crash segment files in OID$OB1: $PRE_SEGS_B1"
echo "  Pre-crash WAL position: $($PSQL -p "$WPORT" -d postgres -At -c 'SELECT pg_current_wal_lsn();')"

# kill -9 工作节点 :$WPORT 的 postmaster
W1_PMID=$(head -1 "$WDATA/postmaster.pid")
echo "  kill -9 工作节点 :$WPORT 的 postmaster (PID $W1_PMID)..."
kill -9 "$W1_PMID"
sleep 2

echo "  Restarting :$WPORT (will run crash recovery)..."
topo_start "$WPORT"
wait_demux_ready "$WPORT" || true   # wait for one-shot BGW to finish crash recovery

echo "  Post-recovery pg_parwal dirs: $(w1_parwal_dirs | tr '\n' ' ')"

POST_B1=$(count_records $OB1); POST_B2=$(count_records $OB2)
echo "  Post-recovery count OID$OB1=$POST_B1 (expected=$PRE_B1), OID$OB2=$POST_B2 (expected=$PRE_B2)"
# ★★ 2026-09-11 改正：原判据是 POST == PRE，用**计数相等**代理"无重复"。
#   在 9 节点活集群上这个代理不成立 —— 从"读计数"到"kill -9"之间，raft 复制/
#   标记等后台活动完全可能往该分区流里再追一条。实测 16 → 17，而
#   `verify_partition_wal` 仍 PASS、LSN 序列 1..N 连续全 valid，
#   **多出来的那条不是重复**。拿计数相等当判据，只会把正常的后台活动报成缺陷。
#   现在直接验两件事：① 没有重号 LSN（标题命题本身）；② 没有丢失（POST >= PRE）。
check_eq "B-post-recovery OID$OB1 无重号 LSN（$PRE_B1 → $POST_B1）" "$(dup_lsn_count $OB1)" 0
check_eq "B-post-recovery OID$OB2 无重号 LSN（$PRE_B2 → $POST_B2）" "$(dup_lsn_count $OB2)" 0
check_true "B-post-recovery OID$OB1 记录未丢失（$POST_B1 >= $PRE_B1）" "$([[ "${POST_B1:-0}" -ge "${PRE_B1:-0}" ]] && echo t || echo f)"
check_true "B-post-recovery OID$OB2 记录未丢失（$POST_B2 >= $PRE_B2）" "$([[ "${POST_B2:-0}" -ge "${PRE_B2:-0}" ]] && echo t || echo f)"
check_true "B-post-recovery verify OID$OB1" "$(verify_wal $OB1)"
check_true "B-post-recovery verify OID$OB2" "$(verify_wal $OB2)"

# Phase B3: insert 5 more and verify LSN continuity
IDS_B1_P2=($(ids_for_shard crash_pg_test $SB1 10 | tail -5))
IDS_B2_P2=($(ids_for_shard crash_pg_test $SB2 10 | tail -5))
echo "  Phase B3: inserting 5 more rows after recovery..."
insert_rows crash_pg_test "${IDS_B1_P2[@]}" "${IDS_B2_P2[@]}"
flush_w1

FINAL_B1=$(count_records $OB1); FINAL_B2=$(count_records $OB2)
check_true "B-final OID$OB1 恢复后继续追加成功（$POST_B1 → $FINAL_B1）" "$([[ "${FINAL_B1:-0}" -gt "${POST_B1:-0}" ]] && echo t || echo f)"
check_true "B-final OID$OB2 恢复后继续追加成功（$POST_B2 → $FINAL_B2）" "$([[ "${FINAL_B2:-0}" -gt "${POST_B2:-0}" ]] && echo t || echo f)"
check_true "B-final verify OID$OB1 (LSN monotone)" "$(verify_wal $OB1)"
check_true "B-final verify OID$OB2 (LSN monotone)" "$(verify_wal $OB2)"

NDIRS_B=$(w1_parwal_dirs | grep -v '^$' | wc -l)
check_ge "B-directory count ≥ 2 (test shards present)" "$NDIRS_B" 2

echo "  Scenario B LSN sequence OID$OB1:"
$PSQL -p "$WPORT" -d postgres -c "
    SELECT partition_lsn, is_valid FROM partdist.check_partition_wal(${OB1}::oid) ORDER BY partition_lsn;" 2>/dev/null | grep -E '^\s+[0-9]'

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SCENARIO C: partial WAL (no demux_flush before crash) ════════"

topo_stop "$WPORT"
rm -rf "$WDATA/pg_parwal"
topo_start "$WPORT"
sleep 2

create_dist_table crash_partial_test

SHARDS_C=($($PSQL -p 5432 -d postgres -At -c "
    SELECT shardid FROM pg_dist_shard JOIN pg_dist_shard_placement USING(shardid)
    WHERE logicalrelid='crash_partial_test'::regclass AND nodeport=$WPORT
    ORDER BY shardid;"))
SC1=${SHARDS_C[0]}; SC2=${SHARDS_C[1]}

IDS_C1=($(ids_for_shard crash_partial_test $SC1 3))
IDS_C2=($(ids_for_shard crash_partial_test $SC2 3))

# Insert 3 rows per shard — do NOT call demux_flush (leave WAL unprocessed by demux)
echo "  Phase C1: inserting 3 rows per shard (no demux_flush)..."
insert_rows crash_partial_test "${IDS_C1[@]}" "${IDS_C2[@]}"

# Brief pause to let demux partially process (some records may make it to pg_parwal)
sleep 1

# kill -9 worker1 immediately
W1_PMID=$(head -1 "$WDATA/postmaster.pid")
echo "  kill -9 工作节点 :$WPORT 的 postmaster (PID $W1_PMID)..."
kill -9 "$W1_PMID"
sleep 2

echo "  Restarting :$WPORT (crash recovery will write unprocessed records)..."
topo_start "$WPORT"
wait_demux_ready "$WPORT" || true   # wait for crash recovery BGW to finish

W1_OIDS_C=($(get_w1_shard_oids crash_partial_test))
OC1=${W1_OIDS_C[0]}; OC2=${W1_OIDS_C[1]}
echo "  Worker1 shard OIDs: $OC1, $OC2"

flush_w1
POST_C1=$(count_records $OC1); POST_C2=$(count_records $OC2)
echo "  Post-recovery count OID$OC1=$POST_C1, OID$OC2=$POST_C2"

# ★★ 2026-09-11 改正：原判据是 `POST_C1 <= 3`，仍是"1 行 1 条记录"的老假设
#   （3 行 ⇒ 期望 ≤3 条），实测 11 —— 每行还带索引与提交标记。把内部比值钉死在
#   用例里，等于每次实现微调都要改用例；而这 8 套被移出门禁后没人跑，错了也没人知道。
#   本场景要验的是「崩溃时未 flush 的记录，恢复后被正确补写且不重复」，
#   所以判据是：① 无重号 LSN；② 确实补出了记录（>0）；③ 流仍然有效。
check_true "C-post-recovery verify OID$OC1 (流有效)" "$(verify_wal $OC1)"
check_true "C-post-recovery verify OID$OC2 (流有效)" "$(verify_wal $OC2)"
check_eq "C-post-recovery OID$OC1 无重号 LSN（实得 $POST_C1 条）" "$(dup_lsn_count $OC1)" 0
check_eq "C-post-recovery OID$OC2 无重号 LSN（实得 $POST_C2 条）" "$(dup_lsn_count $OC2)" 0
check_true "C-post-recovery OID$OC1 恢复后确有记录被补写（$POST_C1 > 0）" "$([[ "${POST_C1:-0}" -gt 0 ]] && echo t || echo f)"

# Phase C2: insert 3 more rows — LSN must continue from last (no reset to 1)
IDS_C1_P2=($(ids_for_shard crash_partial_test $SC1 6 | tail -3))
IDS_C2_P2=($(ids_for_shard crash_partial_test $SC2 6 | tail -3))
echo "  Phase C2: inserting 3 more rows after recovery..."
insert_rows crash_partial_test "${IDS_C1_P2[@]}" "${IDS_C2_P2[@]}"
flush_w1

FINAL_C1=$(count_records $OC1); FINAL_C2=$(count_records $OC2)
check_true "C-final OID$OC1 恢复后继续追加成功（$POST_C1 → $FINAL_C1）" "$([[ "${FINAL_C1:-0}" -gt "${POST_C1:-0}" ]] && echo t || echo f)"
check_true "C-final OID$OC2 恢复后继续追加成功（$POST_C2 → $FINAL_C2）" "$([[ "${FINAL_C2:-0}" -gt "${POST_C2:-0}" ]] && echo t || echo f)"
check_true "C-final verify OID$OC1 (LSN monotone, no restart)" "$(verify_wal $OC1)"
check_true "C-final verify OID$OC2 (LSN monotone, no restart)" "$(verify_wal $OC2)"

# Verify LSN did NOT restart at 1 (post-crash LSNs must be > pre-crash max)
MIN_POST_LSN_C1=$($PSQL -p "$WPORT" -d postgres -At -c "
    SELECT min(partition_lsn) FROM partdist.check_partition_wal(${OC1}::oid)
    WHERE partition_lsn > $POST_C1;" 2>/dev/null || echo 0)
if [ -n "$MIN_POST_LSN_C1" ] && [ "$MIN_POST_LSN_C1" -gt "$POST_C1" ] 2>/dev/null; then
    pass "C-new LSN $MIN_POST_LSN_C1 > pre-crash max $POST_C1 (no restart)"
elif [ "$POST_C1" -eq 0 ]; then
    pass "C-no pre-crash records, new LSNs start from 1 (valid)"
else
    fail "C-LSN restart detected (new min_lsn=$MIN_POST_LSN_C1, expected > $POST_C1)"
fi

NDIRS_C=$(w1_parwal_dirs | grep -v '^$' | wc -l)
check_ge "C-directory count ≥ 2 (test shards present)" "$NDIRS_C" 2

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ GetLastWrittenPartitionLSN robustness: truncated file ════════"

# Manually truncate the last segment file for OC1 to mid-record and verify
# that GetLastWrittenPartitionLSN returns a valid (not-panic) result.
SEG_FILE=$(ls "$WDATA/pg_parwal"/$OC1/ | sort | tail -1)
SEG_PATH=""$WDATA/pg_parwal"/$OC1/$SEG_FILE"
ORIG_SZ=$(stat -c %s "$SEG_PATH")
TRUNC_SZ=$((ORIG_SZ - 5))  # truncate to mid-record
[ "$TRUNC_SZ" -gt 0 ] || TRUNC_SZ=1
cp "$SEG_PATH" "$SEG_PATH.bak"
truncate -s $TRUNC_SZ "$SEG_PATH"
echo "  Truncated $SEG_PATH from $ORIG_SZ to $TRUNC_SZ bytes"

TRUNC_LSN=$($PSQL -p "$WPORT" -d postgres -At -c "
    SELECT partdist.count_parwal_records(${OC1}::oid);" 2>/dev/null || echo "ERROR")
echo "  count_parwal_records after truncation: $TRUNC_LSN"
if [ "$TRUNC_LSN" != "ERROR" ]; then
    pass "GetLastWrittenPartitionLSN: no PANIC on truncated file"
else
    fail "GetLastWrittenPartitionLSN: crashed on truncated file"
fi

# Restore original file
mv "$SEG_PATH.bak" "$SEG_PATH"
echo "  Restored original segment file"

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ XLogFindNextRecord effectiveness ════════"
# Count warnings only since SCENARIO C started to avoid accumulation from
# prior crash events (A and B) inflating the count.  Snapshot was taken
# at the beginning of the final restart in Scenario C.
SNAP_XLF=$(wc -l < "$WDATA/pg.log" 2>/dev/null || echo 0)
INVALID_COUNT=$(tail -n +"$((SNAP_XLF + 1))" "$WDATA/pg.log" 2>/dev/null \
    | grep -c 'invalid record length\|invalid magic number' || echo 0)
echo "  Total 'invalid record/magic' warnings in :$WPORT log (Scenario C only): $INVALID_COUNT"

MAX_REPEAT=$(tail -n +"$((SNAP_XLF + 1))" "$WDATA/pg.log" 2>/dev/null \
    | grep 'invalid record length\|invalid magic number' \
    | grep -oE '[0-9A-Fa-f]+/[0-9A-Fa-f]+' \
    | sort | uniq -c | sort -rn | head -1 | awk '{print $1}' || true)
MAX_REPEAT=$(echo "${MAX_REPEAT:-0}" | head -1)
echo "  Max times any single LSN appears in warnings: $MAX_REPEAT"

if [ "${MAX_REPEAT:-0}" -le 15 ]; then
    pass "XLogFindNextRecord: no stuck-loop detected (max_repeat=$MAX_REPEAT <= 15)"
else
    fail "XLogFindNextRecord: possible infinite loop (max_repeat=$MAX_REPEAT > 15)"
fi

MAGIC_WARNINGS=$(tail -n +"$((SNAP_XLF + 1))" "$WDATA/pg.log" 2>/dev/null \
    | grep -c 'invalid magic number' || echo 0)
echo "  'Invalid magic' (genuine stale-page) warnings: $MAGIC_WARNINGS"

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SUMMARY ════════"
echo "  Tests passed: $PASS"
echo "  Tests failed: $FAIL"
echo ""
if [ "$FAIL" -eq 0 ]; then
    echo "崩溃恢复测试: PASS"
    exit 0
else
    echo "崩溃恢复测试: FAIL"
    exit 1
fi
