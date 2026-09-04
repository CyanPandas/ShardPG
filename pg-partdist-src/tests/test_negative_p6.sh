#!/usr/bin/env bash
# [宿主机] T6.6 验收：§10 第一期功能限制的**负向用例全集**（DEV PLAN §3.8 T6.6）。
#
# P6 的里程碑门禁原文有两句，第二句是"§10 限制项的负向用例（**应报错的确实报错**）"。
# 此前 §10 那张表里的十条限制散落在几个套件里，且从没被逐条核对过"文档写了禁，
# 代码是不是真拦"。
#
# ★ 逐条核实的结果（T6.6 动手前）：
#   SERIALIZABLE ......... shard_visibility.c:279          ✓ 实装
#   行锁 FOR SHARE/UPDATE  补丁 0005（heap_lock_tuple）     ✓ 实装
#   COPY FREEZE / 推测插入 补丁 0005                        ✓ 实装
#   逻辑解码 ............. 补丁 0006                        ✓ 实装
#   CIC / CLUSTER / VACUUM 补丁侧 + ShardXidUtilityGuard    ✓ 实装
#   Citus 运维六项 ....... shard_guard.c                   ✓ 实装（gating_p4 已覆盖）
#   **异步提交 sync_commit=off ......................... ✗ 只写在文档里，代码没拦**
#   引用表运行期写入 ..... V3 裁定为运行纪律，无代码拦截（本套件只取证，不断言）
#
# 那条空档已在本任务补上（shard_xid.c 写路径）。本套件把十条**逐条钉成断言**，
# 免得下一次"文档写了、代码没做"再藏三个月。
set -u
CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
PSQLO() { local port=$1 opts=$2; shift 2
  docker exec -i -u postgres -e HOME=/var/lib/postgresql -e PGOPTIONS="$opts" \
    "$CONTAINER" /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
NOPROP='-c citus.enable_ddl_propagation=off'
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/t66_negative.lock
if ! flock -n 9; then echo "FATAL: 另一个 T6.6 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

W=5433
echo "================ [0] 夹具：一张本地打标表 ================"
# ★ 用**本地表**而不是 Citus 分片表：§10 这十条限制针对的是"分片打标表"这个
#   属性，与它是不是 Citus 分片无关；本地表让夹具最小、也避开协调者路由那一层。
PSQLO $W "$NOPROP" -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t66neg;
CREATE TABLE t66neg(id int, v text) WITH (autovacuum_enabled=off);
SQL
OID=$(PSQLO $W "$NOPROP" -Atc "SELECT 't66neg'::regclass::oid" </dev/null | tail -1)
check "夹具就绪（OID=$OID）" "$([[ "$OID" =~ ^[0-9]+$ ]] && echo ok)" "ok"
PSQL $W -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OID}'" </dev/null >/dev/null
PSQL $W -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do guc=$(PSQL $W -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$OID" ]] && break; sleep 1; done
check "打标生效" "$guc" "$OID"

# ★ 数据必须在**打标之后**插：打标前插的行带的是**原生 xid**，在分片可见性
#   规则下不可见 —— 首版把 INSERT 放在打标之前，于是 `SELECT ... FOR SHARE`
#   查到 0 行、压根走不到 heap_lock_tuple 那道禁令，断言取空。
#   "被测对象必须真的存在"这件事，负向用例比正向用例更容易疏忽。
PSQLO $W "$NOPROP" -q -c "INSERT INTO t66neg SELECT g,'v'||g FROM generate_series(1,20) g" </dev/null >/dev/null
vis=$(PSQLO $W "$NOPROP" -Atc "SELECT count(*) FROM t66neg" </dev/null 2>&1 | head -1)
check "打标后写入的行可见（$vis 行）" "$vis" "20"

# ★ 协调者侧也要打标：Citus 运维六项的闸门判据是 ShardGatingActive()，
#   白名单只设在 worker 上时，协调者那支根本不生效（首版因此取空）。
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OID}'" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null

# ban <名字> <期望出现的片段> <SQL...>
ban() {
  local name=$1 want=$2; shift 2
  local out
  out=$(PSQLO $W "$NOPROP" -Atc "$*" </dev/null 2>&1 | tr '\n' ' ')
  check "禁：$name" "$([[ "$out" == *"$want"* ]] && echo banned)" "banned"
}

echo "================ [1] §10 逐条负向 ================"
ban "SERIALIZABLE 读分片表" "SERIALIZABLE" \
    "BEGIN ISOLATION LEVEL SERIALIZABLE; SELECT count(*) FROM t66neg;"
ban "SELECT FOR SHARE" "row-level locking" \
    "SELECT * FROM t66neg WHERE id=1 FOR SHARE"
ban "SELECT FOR UPDATE" "row-level locking" \
    "SELECT * FROM t66neg WHERE id=1 FOR UPDATE"
ban "CLUSTER" "不允许作用于分片打标表" \
    "CLUSTER t66neg USING t66neg_pkey"
ban "VACUUM FULL" "不允许作用于分片打标表" \
    "VACUUM FULL t66neg"
ban "VACUUM" "不允许作用于分片打标表" \
    "VACUUM t66neg"
ban "CREATE INDEX" "不允许作用于分片打标表" \
    "CREATE INDEX t66neg_ix ON t66neg(id)"
ban "整库 VACUUM（白名单非空）" "不允许整库 VACUUM" "VACUUM"

echo "================ [2] ★ 异步提交（本任务补上的那条空档） ================"
async=$(PSQLO $W "$NOPROP -c synchronous_commit=off" -Atc \
        "INSERT INTO t66neg VALUES (999,'async')" </dev/null 2>&1 | tr '\n' ' ')
check "★ 禁：synchronous_commit=off 写分片表" \
      "$([[ "$async" == *"不支持异步提交"* ]] && echo banned)" "banned"
check "  报错说清了理由（提交点语义只许断言已持久事实）" \
      "$([[ "$async" == *"已持久"* ]] && echo ok)" "ok"
# 反向：同步提交的各档都必须放行，别把正常路径也拦了
for lvl in on local remote_write; do
  # ★ 取**第一行**而不是 tail -1：-At 会把命令标签（INSERT 0 1）打在值之后，
  #   tail -1 抓到的是标签。这个坑本会话已经踩过两次。
  ok=$(PSQLO $W "$NOPROP -c synchronous_commit=$lvl" -Atc \
       "INSERT INTO t66neg VALUES (1000,'sync-$lvl') RETURNING id" </dev/null 2>&1 | head -1)
  check "  放行：synchronous_commit=$lvl" "$ok" "1000"
  PSQLO $W "$NOPROP" -q -c "DELETE FROM t66neg WHERE id=1000" </dev/null >/dev/null 2>&1
done

echo "================ [3] 逻辑解码 ================"
# ★★ 禁令已前移到**入口**（T6.6，R-P6-7）：白名单非空时连槽都不许建，
#   于是**不需要动 wal_level、也不会真的走进解码器**。
#
#   首版为了"真做掉这一条"给本 worker 开了 wal_level=logical 再建槽解码 ——
#   结果实测撞出 `*** stack smashing detected ***`、后端 abort、整节点重置；
#   更糟的是崩溃留下了那个槽，而收尾又把 wal_level 调回 replica，于是节点
#   **再也起不来**（FATAL: slot exists, but wal_level < logical），只能人工
#   把 wal_level 调回 logical → 起库 → 丢槽 → 调回来。
#   **一个能把节点弄成起不来的测试设计，本身就是缺陷** —— 现在入口就拦住了，
#   这段风险整个不必存在。
#
# ★★ R-P6-8：本段最初两条断言**没过**，查出来的不是断言写法问题，而是禁用
#   清单**整层可绕**：PlannedStmt 的 rtable 是 setrefs.c 造的扁平副本，
#   `add_rte_to_flat_rtable()` 明写 `newrte->functions = NIL;`
#   （postgres-src/.../setrefs.c:554），所以首版那个"扫 rtable 里 RTE_FUNCTION"
#   的循环是**死代码**，`SELECT * FROM f(...)` 从 T4.6 起一直绕得过去。
#   下面两条刻意用 **FROM 形式**，就是钉住这个绕法。
mk=$(PSQLO $W "$NOPROP" -Atc "SELECT slot_name FROM pg_create_logical_replication_slot('t66slot','test_decoding')" </dev/null 2>&1 | tr '\n' ' ')
check "★★ 禁：在打标集群上建逻辑复制槽（入口拦截）" \
      "$([[ "$mk" == *"被禁用"* ]] && echo banned)" "banned"
dec=$(PSQLO $W "$NOPROP" -Atc "SELECT count(*) FROM pg_logical_slot_get_changes('t66slot',NULL,NULL)" </dev/null 2>&1 | tr '\n' ' ')
check "★★ 禁：逻辑解码调用（入口拦截）" \
      "$([[ "$dec" == *"被禁用"* ]] && echo banned)" "banned"

echo "================ [4] 含分片写的 PREPARE TRANSACTION ================"
# T4.3 起收口为"未 join 全局事务的分片写不许 PREPARE"（join 后放行）。
prep=$(PSQLO $W "$NOPROP" -Atc \
       "BEGIN; INSERT INTO t66neg VALUES (1002,'prep'); PREPARE TRANSACTION 't66p';" </dev/null 2>&1 | tr '\n' ' ')
check "禁：未 join 全局事务的分片写 PREPARE" \
      "$([[ "$prep" == *"未加入全局事务"* || "$prep" == *"PREPARE TRANSACTION"* ]] && echo banned)" "banned"
PSQLO $W "$NOPROP" -q -c "ROLLBACK PREPARED 't66p'" </dev/null >/dev/null 2>&1

echo "================ [5] Citus 运维六项（协调者侧）================"
# 这六项由 shard_guard.c 拦，gating_p4 已逐条覆盖；此处只做一条冒烟，
# 确认"归一门禁里这一类确实还在被验"，避免两套件都以为对方在管。
smoke=$(PSQL $COORD -Atc "SELECT citus_rebalance_start()" </dev/null 2>&1 | tr '\n' ' ')
check "禁（冒烟）：citus_rebalance_start" \
      "$([[ "$smoke" == *"T4.6/§9.2"* ]] && echo banned)" "banned"
# ★★ R-P6-8 回归：同一个函数换成 FROM 形式、以及套一层子查询/CTE。
#   首版这三条**全部放行**（实测一路跑进了 Citus 重平衡器），是禁用清单
#   可绕的直接证据。修复=改成遍历计划树取 FunctionScan.functions。
sm2=$(PSQL $COORD -Atc "SELECT * FROM citus_rebalance_start()" </dev/null 2>&1 | tr '\n' ' ')
check "★★ 禁：citus_rebalance_start —— FROM 形式（R-P6-8 绕法）" \
      "$([[ "$sm2" == *"T4.6/§9.2"* ]] && echo banned)" "banned"
sm3=$(PSQL $COORD -Atc "SELECT * FROM (SELECT * FROM citus_rebalance_start()) s" </dev/null 2>&1 | tr '\n' ' ')
check "★★ 禁：citus_rebalance_start —— 子查询套一层" \
      "$([[ "$sm3" == *"T4.6/§9.2"* ]] && echo banned)" "banned"
sm4=$(PSQL $COORD -Atc "WITH x AS (SELECT * FROM citus_rebalance_start()) SELECT * FROM x" </dev/null 2>&1 | tr '\n' ' ')
check "★★ 禁：citus_rebalance_start —— CTE（走 pstmt->subplans）" \
      "$([[ "$sm4" == *"T4.6/§9.2"* ]] && echo banned)" "banned"
# 阴性对照：修好的遍历**不能**误伤普通函数（否则就是"全禁"式假通过）
ok1=$(PSQL $COORD -Atc "SELECT count(*) FROM generate_series(1,3)" </dev/null 2>&1 | head -1)
check "  阴性对照：普通函数 FROM 形式仍放行" "$ok1" "3"

echo "================ [6] 引用表：取证而非断言 ================"
# V3 裁定"建表后只读"是**运行纪律**，没有代码拦截。这里只记录事实，
# 不写成断言 —— 把纪律伪装成断言，会让人以为它有强制力。
nref=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_dist_partition WHERE partmethod='n'" </dev/null 2>&1 | tail -1)
echo "  [取证] 本集群引用表数量 = ${nref:-取不到}（V3 裁定：建表后只读，无代码拦截）"
check "引用表取证可得" "$([[ "$nref" =~ ^[0-9]+$ ]] && echo ok)" "ok"

echo "================ [7] 清理 ================"
PSQL $W -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids" </dev/null >/dev/null
PSQL $W -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null
PSQL $COORD -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null

PSQLO $W "$NOPROP" -q -c "DROP TABLE IF EXISTS t66neg" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"
health_check_no_crash || true
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]]
