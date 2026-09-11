#!/bin/bash
# test_shard_auto_init.sh — 自动化验证 Citus 分片创建时自动初始化 pg_parwal 目录

export PATH=/work/pg-install/bin:$PATH

# ★★ T7.13（P7-E2）：拓扑无关化改造（2026-09-10）。
#   本套件原先写死 worker1/worker2，把它们当成"全部 worker"。3 节点布局下成立，
#   9 节点下不成立：`shard_count => 4` 的分片会散在 4 个**任意** worker 上，
#   盯着 worker1/2 看什么也看不到，于是报"目录缺失"，看起来像产品缺陷。
#   实测佐证：一张 shard_count=2 的探针表落在 :5434/:5435，压根不含 worker1。
#   现在一律按 `pg_dist_placement` 取该表分片的**实际落点**。
source "$(cd "$(dirname "$0")" && pwd)/lib_topology.sh"

COORD_PORT=5432

PASS=0
FAIL=0

pass() { echo "[PASS] $1"; PASS=$((PASS+1)); }
fail() { echo "[FAIL] $1"; FAIL=$((FAIL+1)); }

q()  { psql -h localhost -p "$1" -U postgres -d postgres -t -A -c "$2" 2>&1; }
q_coord() { q $COORD_PORT "$1"; }

count_lines() { echo "$1" | grep -c . 2>/dev/null || echo 0; }
count_dir()   { ls "$1" 2>/dev/null | wc -l | tr -d ' '; }
grep_count()  {
    local cnt
    cnt=$(grep -c "$1" "$2" 2>/dev/null || true)
    echo "${cnt:-0}"
}

cleanup_table() {
    q_coord "DROP TABLE IF EXISTS $1 CASCADE" >/dev/null 2>&1 || true
}

# ---------- 准备 ----------
echo "===== 准备：取拓扑 + 记录基线（**不再清空 pg_parwal**）====="
# ★ 原先这里 `rm -rf $W1_DATA/pg_parwal/* $W2_DATA/pg_parwal/*`。在 9 节点上那是
#   把两个**无关** worker 的回放状态整个抹掉 —— 它们身上多半跑着别的套件/别的表
#   的分片。判据改成"看增量"而不是"从零数"，既不破坏现场，也不再要求本套件
#   独占整个集群。
topo_init || { echo "[FAIL] 拓扑初始化失败"; exit 1; }
ALL_W=$(topo_worker_ports)
echo "worker 端口（按 pg_dist_node 动态取）：$ALL_W"
declare -A BASE_DIRS
for _p in $ALL_W; do BASE_DIRS[$_p]=$(count_dir "$(topo_datadir $_p)/pg_parwal"); done

# ---------- 测试 1：功能测试 ----------
echo ""
echo "===== 测试 1：首次 INSERT 后 pg_parwal 目录自动创建（parwal-2.0 懒初始化）====="
cleanup_table shard_auto_t1
q_coord "CREATE TABLE shard_auto_t1 (id INT, val TEXT)" >/dev/null
q_coord "SELECT create_distributed_table('shard_auto_t1', 'id', shard_count => 4)" >/dev/null

# parwal-2.0: pg_parwal 目录在首次 DML 时懒创建（ExecutorStart/ProcessUtility hook）
# 插入足够多的行以覆盖所有分片
q_coord "INSERT INTO shard_auto_t1 SELECT i, 'v'||i FROM generate_series(1,100) i" >/dev/null

# ★ 按该表分片的**实际落点**逐节点核对，而不是盯着写死的 worker1/2。
T1_PORTS=$(topo_ports_for_table shard_auto_t1)
echo "shard_auto_t1 的分片落在：${T1_PORTS:-（空！）}"

T1_OK=true
T1_NSHARD=0
if [[ -z "${T1_PORTS// /}" ]]; then
    fail "取不到 shard_auto_t1 的分片落点（pg_dist_placement 为空）"; T1_OK=false
fi
for _p in $T1_PORTS; do
    _dir=$(topo_datadir $_p)
    # 只选 relkind='r'（普通表），过滤掉 psql 的 SET 确认行
    _shards=$(q $_p "SET citus.override_table_visibility TO off; SELECT oid FROM pg_class WHERE relname LIKE 'shard_auto_t1_%' AND relkind='r' ORDER BY oid" | grep -E '^[0-9]+$' || true)
    for oid in $_shards; do
        T1_NSHARD=$((T1_NSHARD+1))
        if [[ ! -d "$_dir/pg_parwal/$oid" ]]; then
            fail ":$_p pg_parwal/$oid 目录缺失"; T1_OK=false
        fi
    done
done
[[ "$T1_NSHARD" -ge 1 ]] || { fail "在落点节点上一个分片表都没找到（本应 >=1）"; T1_OK=false; }

if [[ "$T1_OK" == "true" ]]; then
    pass "功能测试：${T1_NSHARD} 个分片在落点节点（$T1_PORTS）上目录均已创建"
else
    fail "功能测试：分片目录缺失（落点=$T1_PORTS，共 ${T1_NSHARD} 个分片）"
fi

# ---------- 测试 2：幂等性测试 ----------
echo ""
echo "===== 测试 2：重复初始化不报错（目录残留+重建）====="
cleanup_table shard_auto_t1
q_coord "CREATE TABLE shard_auto_t1 (id INT, val TEXT)" >/dev/null
q_coord "SELECT create_distributed_table('shard_auto_t1', 'id', shard_count => 4)" >/dev/null

# ★ 在**全部** worker 的日志里找 ERROR，而不是只看 worker1 —— 重建后的落点
#   未必与上一轮相同，只盯一个节点等于把大部分证据丢掉。
ERROR_CNT=0
for _p in $ALL_W; do
    _n=$(grep_count "ERROR.*shard_auto" "$(topo_datadir $_p)/pg.log")
    ERROR_CNT=$((ERROR_CNT + _n))
done
T2_PORTS=$(topo_ports_for_table shard_auto_t1)

if [[ "$ERROR_CNT" -eq 0 ]]; then
    pass "幂等性测试：第二次 create_distributed_table 在全部 worker 日志中均无 ERROR（落点=$T2_PORTS）"
else
    fail "幂等性测试：全部 worker 日志合计发现 $ERROR_CNT 个 ERROR"
fi

# ---------- 测试 3：异常测试（权限不足） ----------
echo ""
echo "===== 测试 3：目录权限不足时不影响分片创建 ====="
cleanup_table shard_auto_perm

# ★★ 2026-09-10 重做这一段，原因是**宿主机会随时因内存杀进程，而 EXIT 钩子挡不住
#   SIGKILL**。我第一版改成"对全部 worker chmod 555 再恢复"，恢复挂在 EXIT 上 ——
#   实测这一轮就被 SIGKILL 打断在 chmod 前一刻，纯属侥幸没出事。要是晚半秒，
#   **全簇的 pg_parwal 会一直停在只读**，之后每一套都会以莫名其妙的方式红。
#   靠"记得恢复"来兜底一个爆炸半径是整簇的操作，本身就是错的设计。
#
#   换个做法，让"目录建不出来"这件事**天然局部**：分片目录是首次 DML 时懒建的，
#   那就在建表之后、首次 DML 之前，在该目录的路径上放一个**普通文件** ——
#   mkdir 撞上它必然失败（ENOTDIR/EEXIST），效果与不可写相同，而爆炸半径只是
#   本表自己那几个 OID 的路径，DROP TABLE 就带走了，被 SIGKILL 打断也无所谓。
q_coord "CREATE TABLE shard_auto_perm (id INT)" >/dev/null 2>&1 || true
q_coord "SELECT create_distributed_table('shard_auto_perm', 'id', shard_count => 2)" >/dev/null 2>&1 || true

SHARD_CNT=$(q_coord "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='shard_auto_perm'::regclass" | tr -d ' \n')
PERM_PORTS=$(topo_ports_for_table shard_auto_perm)

# 在每个落点上，给本表每个分片 OID 的目录路径放一个占位普通文件
BLOCKED=""
for _p in $PERM_PORTS; do
    _dir=$(topo_datadir $_p)
    for oid in $(q $_p "SET citus.override_table_visibility TO off; SELECT oid FROM pg_class WHERE relname LIKE 'shard_auto_perm_%' AND relkind='r'" | grep -E '^[0-9]+$'); do
        if [[ ! -e "$_dir/pg_parwal/$oid" ]]; then
            : > "$_dir/pg_parwal/$oid" 2>/dev/null && BLOCKED+="$_dir/pg_parwal/$oid "
        fi
    done
done

# 首次 DML：懒建目录会撞上占位文件而失败。
INS_ERR=$(q_coord "INSERT INTO shard_auto_perm SELECT i FROM generate_series(1,10) i" 2>&1 | grep -c "ERROR" || true)

# ★★ 2026-09-10 改正一处**原用例写错了的期望**。
#   原注释写着「INSERT should succeed (data written) but pg_parwal dir creation
#   fails silently」，而它自己用 `|| true` 把 INSERT 的结果丢掉了，
#   **从来没验证过这个说法**。实测行为是：
#     · `RegisterShardFileSet` 建 fileset.tmp 失败 → WARNING（确实被吞掉）
#     · 但写分区 WAL 段失败 → **ERROR，INSERT 中止**
#   而这才是**对的**：写不进分区 WAL 就必须拒绝这次写入 —— 否则等于接受了一笔
#   **无法复制**的写入，副本永远看不到它，正是本项目一直在修的那类静默分歧。
#   所以判据改成"写入被**响亮地**拒绝"，而不是"悄悄成功"。
#   （本用例标题「不影响**分片创建**」说的是 DDL，那一条仍然成立：shards=2。）
#
# 判据：① 分片（DDL）确实建出来了；② 占位路径仍是普通文件（目录没建成）；
#       ③ 写入被拒绝且报了 ERROR —— 不是静默接受。
PERM_OK=true; PERM_DET=""
NBLOCK=0
for f in $BLOCKED; do
    NBLOCK=$((NBLOCK+1))
    if [[ -d "$f" ]]; then PERM_OK=false; PERM_DET+="[$f 变成了目录] "; fi
done
[[ "$NBLOCK" -ge 1 ]] || { PERM_OK=false; PERM_DET+="[一个占位文件都没放成] "; }
[[ "$INS_ERR" -ge 1 ]] || { PERM_OK=false; PERM_DET+="[INSERT 竟然静默成功了 —— 写不进分区 WAL 却接受写入，是静默分歧] "; }

rm -f $BLOCKED 2>/dev/null || true     # 立刻清掉，不拖到 EXIT

if [[ "$SHARD_CNT" -ge 1 ]] && [[ "$PERM_OK" == "true" ]]; then
    pass "异常测试：分片建成（shards=$SHARD_CNT，落点=$PERM_PORTS），${NBLOCK} 个目录路径被占位后仍未建成目录，且写入被响亮拒绝（ERROR×$INS_ERR）"
else
    fail "异常测试：shards=$SHARD_CNT 落点=$PERM_PORTS 占位=$NBLOCK INS_ERR=$INS_ERR $PERM_DET"
fi

# ---------- 测试 4：非分片表不创建目录 ----------
echo ""
echo "===== 测试 4：非分片表不应创建 pg_parwal 目录 ====="
# ★ 随便选一个 worker 即可（"普通表不建目录"与落点无关），但**必须动态取**：
#   写死 worker1 只是碰巧在 3 节点布局下存在。
T4_PORT=$(set -- $ALL_W; echo "$1")
T4_DIR=$(topo_datadir "$T4_PORT")
BEFORE=$(count_dir "$T4_DIR/pg_parwal")
q "$T4_PORT" "DROP TABLE IF EXISTS plain_no_shard_xyz" >/dev/null 2>&1
q "$T4_PORT" "CREATE TABLE plain_no_shard_xyz (id INT)" >/dev/null
AFTER=$(count_dir "$T4_DIR/pg_parwal")
q "$T4_PORT" "DROP TABLE IF EXISTS plain_no_shard_xyz" >/dev/null 2>&1

if [[ "$BEFORE" -eq "$AFTER" ]]; then
    pass "非分片表：普通表不创建 pg_parwal 目录（:$T4_PORT before=$BEFORE after=$AFTER）"
else
    fail "非分片表：意外目录（:$T4_PORT before=$BEFORE after=$AFTER）"
fi

# ---------- 测试 5：回归测试 ----------
echo ""
echo "===== 测试 5：37 个回归测试 ====="
chmod 666 /work/pg-partdist-src/test/regression.out /work/pg-partdist-src/test/regression.diffs 2>/dev/null || true
REGRESS=$(
  export PATH=/work/pg-install/bin:$PATH
  cd /work/pg-partdist-src && make -s installcheck PGUSER=postgres PGPORT=5432 PG_CONFIG=/work/pg-install/bin/pg_config 2>&1 | tail -2
)

if echo "$REGRESS" | grep -q "All 37 tests passed"; then
    pass "回归测试：All 37 tests passed"
else
    fail "回归测试失败：$REGRESS"
fi

# ---------- 汇总 ----------
echo ""
echo "======================================"
echo " PASSED: $PASS  FAILED: $FAIL"
echo "======================================"
if [[ $FAIL -eq 0 ]]; then
    echo "Citus 分片自动初始化: PASS"
    exit 0
else
    echo "Citus 分片自动初始化: FAIL"
    exit 1
fi
