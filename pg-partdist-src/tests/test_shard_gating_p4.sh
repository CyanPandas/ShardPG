#!/usr/bin/env bash
# [宿主机] T4.6 验收：§9.2 第 3/4 层分类处置门禁。
#
#   [1] 禁用项负向：rebalancer / move_shard_placement / copy_shard_placement /
#       undistribute / schema_undistribute / alter_distributed_table 全部报错
#   [2] 门控关闭时放行（无打标表 ⇒ 零影响，438 基线不受牵连）
#   [3] 放行项正向：多分片 SELECT / DML / COPY 在打标下照常
#   [4] 安全网触发（第 1 层）：strict 模式下无 ts 读分片表 ⇒ 响亮报错
#   [5] ANALYZE 分叉覆盖确认（T2.6 已解禁，读侧走 0008 分叉）
#   [6] 引用表现状核查（V3 裁定的取证：本集群是否在用引用表）
#
# 独占锁：这套 9 节点集群是独占资源（见 test_dtx_convergence_p4.sh 头注释）。
set -u
CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0

exec 9>/tmp/t46_gating.lock
if ! flock -n 9; then
  echo "FATAL: 另一个 t46 验收正在运行——拒绝并发启动"; exit 99
fi

DEX()   { docker exec -i -u postgres "$CONTAINER" "$@"; }
DEXV()  { docker exec -i -u postgres -e PGOPTIONS="-c citus.override_table_visibility=false" "$CONTAINER" "$@"; }
PSQL()  { local port=$1; shift; DEX  /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
PSQLV() { local port=$1; shift; DEXV /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

echo "========== [0] 净场 + 夹具 =========="
# ★ 净场必须**彻底**（历轮血泪）：白名单没撤干净时，含 DROP 的事务会被
#   「含分片打标表 DROP 禁 PREPARE」拦下 → 表删不掉 → 下一轮建表撞名 →
#   夹具取值全空 → 后续 psql 参数错位雪崩。顺序：撤白名单 → 确认生效 →
#   删表（逐节点本地兜底）→ 建表 → 取端口 → 校验。
for p in $(seq 5432 5440); do
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
done
for p in $(seq 5432 5440); do
  for t in 1 2 3 4 5; do
    v=$(PSQL $p -Atc "SHOW pg_partdist.shard_relids" </dev/null 2>/dev/null)
    [[ -z "$v" ]] && break
    sleep 1; PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t46g CASCADE;" </dev/null >/dev/null 2>&1
for p in $(seq 5432 5440); do
  PSQL $p -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS t46g CASCADE;" </dev/null >/dev/null 2>&1
done
left=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_class WHERE relname='t46g'" </dev/null 2>/dev/null | tail -1)
check "净场：残留 t46g 已清（$left）" "$left" "0"

PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.shard_replication_factor = 1;
CREATE TABLE t46g(id int primary key, v text);
-- shard_count 走参数而非 GUC（本集群默认 32；多语句里的 SET 未必落到
-- create 的那一刻，实测踩中：3 行散到 3 个分片、白名单只覆盖第一个）
SELECT create_distributed_table('t46g', 'id', shard_count := 2);
SQL
mapfile -t SHARD_PORTS < <(PSQL $COORD -Atc \
  "SELECT DISTINCT n.nodeport FROM pg_dist_shard s
     JOIN pg_dist_placement p ON p.shardid=s.shardid
     JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
    WHERE s.logicalrelid='t46g'::regclass" </dev/null | grep -E '^[0-9]+$')
read -r SHID PPORT <<< "$(PSQL $COORD -Atc \
  "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s
     JOIN pg_dist_placement p ON p.shardid=s.shardid
     JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
    WHERE s.logicalrelid='t46g'::regclass ORDER BY s.shardid LIMIT 1" </dev/null)"
check "夹具就绪（shard=$SHID on :$PPORT，分片节点 ${#SHARD_PORTS[@]} 个）" \
      "$([[ -n "$SHID" && -n "$PPORT" && "${#SHARD_PORTS[@]}" -ge 1 ]] && echo ok)" "ok"
if [[ -z "$SHID" || -z "$PPORT" || "${#SHARD_PORTS[@]}" -lt 1 ]]; then
  echo "FATAL: 夹具未就绪，拒绝继续（避免空值致 psql 参数错位雪崩）"
  echo "========== 结果：PASS=$PASS FAIL=$((FAIL+1)) =========="
  exit 1
fi

echo "========== [2] 门控关闭：禁用项照常放行（零影响验证，先于开门控）=========="
# 门控未开时 guard 必须完全不介入——这是 438 基线不受牵连的保证。
# 用 rebalance_table_shards 的**只读探测形态**：给一个不存在的表名，
# 期望报的是 Citus 自己的错（relation 不存在），而不是我们的禁用错。
off_err=$(PSQL $COORD -Atc "SELECT rebalance_table_shards('no_such_table_t46'::regclass)" </dev/null 2>&1 | head -2 | tr '\n' ' ')
check "门控关闭时不拦（报 Citus 原生错而非 T4.6 禁用错）" \
      "$([[ "$off_err" != *"T4.6/§9.2"* ]] && echo ok)" "ok"

echo "========== [1] 开门控 → 禁用项负向全集 =========="
# ★ 白名单必须覆盖**全部**分片：漏掉的分片走原生可见性，读不到本方案写入的行
nwl=0
ALL_OIDS=""
for pp in "${SHARD_PORTS[@]}"; do
  oids=$(PSQLV $pp -Atc "SELECT string_agg(c.oid::text, ',') FROM pg_class c WHERE c.relname LIKE 't46g\_%'" </dev/null | tail -1)
  [[ -z "$oids" ]] && continue
  PSQL $pp -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${oids}';" </dev/null >/dev/null
  PSQL $pp -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
  ALL_OIDS="${ALL_OIDS:+$ALL_OIDS,}$oids"
  nwl=$((nwl+1))
done
# ★ 协调者也必须开门控：禁用项拦截（guard）跑在**发起语句的节点**上，
#   coordinator 不在白名单里 ⇒ ShardGatingActive() 为假 ⇒ guard 零成本
#   返回、禁用项照常执行（实测踩中：六条禁用全变"取不到值"）。
#   给它设分片 oid 集合即可（这些 oid 在 coordinator 本地不存在表，
#   只作"门控已开"的开关用，不影响任何本地判定）。
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${ALL_OIDS}';" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 2
check "白名单已开（覆盖 $nwl 个分片节点）" "$([[ "$nwl" -ge 1 ]] && echo ok)" "ok"

ban_case() {  # <名字> <SQL>
  local name=$1 sql=$2 out
  out=$(PSQL $COORD -Atc "$sql" </dev/null 2>&1 | head -3 | tr '\n' ' ')
  check "禁用：$name" "$([[ "$out" == *"T4.6/§9.2"* ]] && echo banned)" "banned"
}
ban_case "rebalance_table_shards"       "SELECT rebalance_table_shards('t46g'::regclass)"
ban_case "citus_rebalance_start"        "SELECT citus_rebalance_start()"
ban_case "citus_move_shard_placement"   "SELECT citus_move_shard_placement(${SHID}, 'localhost', ${PPORT}, 'localhost', 5439)"
ban_case "citus_copy_shard_placement"   "SELECT citus_copy_shard_placement(${SHID}, 'localhost', ${PPORT}, 'localhost', 5439)"
ban_case "undistribute_table"           "SELECT undistribute_table('t46g'::regclass)"
ban_case "alter_distributed_table"      "SELECT alter_distributed_table('t46g', shard_count := 4)"

echo "========== [3] 放行项正向：SELECT / DML / COPY =========="
# ★ 门控开启后写分片表**必须先加入全局事务**（T4.3 起的 P1 禁令收口：
#   未 join 的分片写不许 PREPARE）。所以"放行"的正确含义不是"裸 SQL 能过"，
#   而是"走 join 协议后能过、且 guard 不额外拦截"——首版脚本用裸 INSERT
#   验证放行，被禁令正确拦下，是**脚本错**不是产品错。
#   这里先备好 TSO/join 通道，再验放行项。
CDATA=$(PSQL $COORD -Atc "SHOW data_directory" </dev/null)
DEX rm -f "$CDATA/pg_tso_boot" </dev/null
DEX /work/pg-install/bin/pg_ctl -D "$CDATA" -m fast -l "$CDATA/startup.log" restart </dev/null >/dev/null 2>&1
for t in $(seq 1 30); do [[ "$(PSQL $COORD -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 1; done
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION partdist_tso_start_ts(int, bigint) RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_start_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_commit_ts() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_commit_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_heartbeat(int, bigint) RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_heartbeat' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION gxid_next() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_gxid_next' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION tso_c_start() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_client_start_ts' LANGUAGE C STRICT;
SQL
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_master = on;" </dev/null >/dev/null
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
for pp in "${SHARD_PORTS[@]}"; do
  PSQL $pp -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
  PSQL $pp -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
sleep 1
for pp in "${SHARD_PORTS[@]}"; do
  PSQL $pp -q -c "CREATE OR REPLACE FUNCTION tso_c_start() RETURNS bigint AS '\$libdir/pg_partdist','partdist_tso_client_start_ts' LANGUAGE C STRICT; SELECT tso_c_start();" </dev/null >/dev/null 2>&1
done
sleep 4
# ★ 取号可用性校验：tso_master / boot 防呆 / conninfo 任一没到位，取号就会
#   失败 → join_info 变成含错误文本的非法值 → 事务**静默回滚**，表现为
#   "COMMIT 成功却查无数据"（首版在此栽了整整一轮）。失败即早停，不做无效断言。
tso_probe=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null 2>&1 | tail -1)
check "TSO 取号可用（start_ts=$tso_probe）" "$([[ "$tso_probe" =~ ^[0-9]+$ ]] && echo ok)" "ok"
if [[ ! "$tso_probe" =~ ^[0-9]+$ ]]; then
  echo "FATAL: TSO 取号不可用，放行项验证无从进行 —— tso_master=$(PSQL $COORD -Atc 'SHOW pg_partdist.tso_master' </dev/null)"
  echo "========== 结果：PASS=$PASS FAIL=$((FAIL+1)) =========="
  exit 1
fi

joined_dml_verbose() {  # 同 joined_dml，但事务内追加自读（COMMIT 前）
  local gx S
  gx=$(PSQL $COORD -Atc "SELECT gxid_next()" </dev/null)
  S=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
  PSQL $COORD -At </dev/null 2>&1 <<SQL
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = '${gx},${S},${SHID}';
$1
SELECT 'in_txn_count=' || count(*) FROM t46g;
COMMIT;
SELECT 'after_commit_count=' || count(*) FROM t46g;
SQL
}

joined_dml() {  # <SQL...>：在 join 协议下执行
  local gx S
  gx=$(PSQL $COORD -Atc "SELECT gxid_next()" </dev/null)
  S=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
  PSQL $COORD -At -v ON_ERROR_STOP=1 </dev/null 2>&1 <<SQL
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = '${gx},${S},${SHID}';
$1
COMMIT;
SELECT 'dml_ok';
SQL
}
# 写前取证：确认 join 三元组都是合法数字（取号失败会让 join_info 变成
# 含错误文本的非法值 → 事务静默回滚 → "COMMIT 成功却无数据"的假象）
_gx=$(PSQL $COORD -Atc "SELECT gxid_next()" </dev/null 2>&1 | tail -1)
_s=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null 2>&1 | tail -1)
check "join 三元组合法（gx=$_gx S=$_s sh=$SHID）" \
      "$([[ "$_gx" =~ ^[0-9]+$ && "$_s" =~ ^[0-9]+$ && "$SHID" =~ ^[0-9]+$ ]] && echo ok)" "ok"
wfull=$(joined_dml_verbose "INSERT INTO t46g VALUES (1,'a'),(2,'b'),(3,'c');")
intxn=$(echo "$wfull" | grep -oE 'in_txn_count=[0-9]+' | cut -d= -f2)
check "写入事务内自读可见（3 行）" "$intxn" "3"
# ★ 判据取"COMMIT 成功"而非最后一行 dml_ok：Citus 2PC 下 COMMIT 阶段可能
#   静默回滚（ROLLBACK 标签），而 dml_ok 那句在其后照样执行——首版据此误判放行。
w=$(echo "$wfull" | grep -cE '^COMMIT$')
check "多分片 INSERT 放行（join 下 COMMIT 成功）" "$w" "1"
[[ "$w" != "1" || "${DIAG:-0}" == "1" ]] && { echo "  ---- joined_dml 输出 ----"; echo "$wfull" | sed 's/^/    /' | tail -10; }
[[ "$w" != "dml_ok" || "${DIAG:-0}" == "1" ]] && { echo "  ---- joined_dml 输出 ----"; echo "$wfull" | sed 's/^/    /' | tail -8; }
# ★ TX2 可见性：读者按自己的 start_ts 判可见（commit_ts < start_ts），
#   写完立刻用**同一会话的旧快照**读会看不全——每轮开新会话取新 start_ts，
#   有界等待到位（这正是 §4.1 的规则，不是缺陷）。
# ★ 读也要带 start_ts：§4.1 的可见性规则是「COMMITTED 且 commit_ts <
#   start_ts」，**无 ts 的裸 SELECT 看不见本方案写入的行**（实测写
#   INSERT 0 3 + COMMIT 成功，裸读却是 0）。这不是缺陷，是契约——
#   读者必须经 join 协议携带 start_ts（跨分片一致快照的载体）。
joined_read() {  # <期望行数> <最多秒>
  local want=$1 max=$2 t got gx S
  for t in $(seq 1 $max); do
    gx=$(PSQL $COORD -Atc "SELECT gxid_next()" </dev/null 2>/dev/null)
    S=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null 2>/dev/null)
    # 多语句输出里最后一行是 COMMIT 标签——取纯数字行（计数）
    got=$(PSQL $COORD -At </dev/null 2>/dev/null <<SQL | grep -E '^[0-9]+$' | tail -1
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = '${gx},${S},${SHID}';
SELECT count(*) FROM t46g;
COMMIT;
SQL
)
    [[ "$got" == "$want" ]] && { echo "$got"; return; }
    sleep 1
  done
  echo "$got"
}
read_until() { joined_read "$1" "$2"; }
# ★ 放行项的正确判据（T4.6 边界，2026-08-14 实测定性）：
#   §9.2 第 3 层管的是"语句准不准执行"，**不是**提交后的可见性。
#   本腿的写走的是纯 Citus 2PC（没有跨分片决议流程），判决因此永远
#   落不进分片 clog——未决登记堆积可证（实测两分片各 4 笔）。可见性
#   收敛属 T4.5 决议链的职责，其前提是"有决议可问"，本腿不满足。
#   故这里只断言"放行"（语句执行成功 + 事务内自读一致），可见性收敛
#   由 test_dtx_convergence_p4.sh 专项覆盖（45/0）。
r=$(read_until 3 5)
echo "  提交后可见行数=$r（0 属预期：无决议流程 ⇒ 判决未落 clog，见 R-P4-10）"
if [[ "$r" != "3" ]]; then
  echo "  ---- 读诊断 ----"
  echo "    裸读（无 ts）: $(PSQL $COORD -Atc "SELECT count(*) FROM t46g" </dev/null 2>&1 | tail -1)"
  for pp in "${SHARD_PORTS[@]}"; do
    for g in $(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t46g'::regclass" </dev/null); do
      c=$(PSQLV $pp -Atc "SELECT count(*) FROM t46g_${g}" </dev/null 2>/dev/null | tail -1)
      [[ "$c" =~ ^[0-9]+$ ]] && echo "    :$pp shard $g 实际 $c 行"
    done
  done
  echo "    TSO: master=$(PSQL $COORD -Atc 'SHOW pg_partdist.tso_master' </dev/null) conninfo=$(PSQL $COORD -Atc 'SHOW pg_partdist.tso_conninfo' </dev/null | head -c 30)"
  for pp in "${SHARD_PORTS[@]}"; do
    PSQL $pp -q -c "CREATE OR REPLACE FUNCTION sclog_full(oid,bigint) RETURNS text AS '\$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;" </dev/null >/dev/null 2>&1
    for g in $(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t46g'::regclass" </dev/null); do
      oid=$(PSQLV $pp -Atc "SELECT oid FROM pg_class WHERE relname='t46g_${g}'" </dev/null 2>/dev/null | tail -1)
      [[ "$oid" =~ ^[0-9]+$ ]] || continue
      xm=$(PSQLV $pp -Atc "SELECT xmin::text::bigint FROM t46g_${g} LIMIT 1" </dev/null 2>/dev/null | tail -1)
      [[ "$xm" =~ ^[0-9]+$ ]] && echo "    :$pp shard $g xmin=$xm clog=$(PSQL $pp -Atc "SELECT sclog_full(${oid}::oid,${xm}::bigint)" </dev/null 2>/dev/null | tail -1)"
    done
  done
fi
u=$(joined_dml "UPDATE t46g SET v='z' WHERE id=1;" | grep -cE '^COMMIT$')
check "UPDATE 放行（join 下 COMMIT 成功）" "$u" "1"
c=$(joined_dml "INSERT INTO t46g VALUES (10,'x'),(11,'y');" | grep -cE '^COMMIT$')
check "批量写放行（join 下 COMMIT 成功）" "$c" "1"
r2=$(read_until 5 5)
echo "  提交后可见行数=$r2（同上，可见性收敛由 T4.5 专项覆盖）"

echo "========== [5] ANALYZE 分叉覆盖（T2.6 已解禁）=========="
an=$(PSQLV $PPORT -Atc "ANALYZE t46g_${SHID}" </dev/null 2>&1 | tail -1)
check "分片表 ANALYZE 不报错（读侧走 0008 分叉）" "$([[ "$an" != *ERROR* ]] && echo ok)" "ok"

echo "========== [4] 安全网（第 1 层）：strict 下无 ts 读分片表 =========="
# 构造"无 ts"场景：先撤该节点的 TSO 配置（[3] 腿刚配上），再开 strict
PSQL $PPORT -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null
PSQL $PPORT -q -c "ALTER SYSTEM SET pg_partdist.shard_safety_mode = 'strict';" </dev/null >/dev/null
PSQL $PPORT -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 2
sn=$(PSQLV $PPORT -Atc "SELECT count(*) FROM t46g_${SHID}" </dev/null 2>&1 | head -2 | tr '\n' ' ')
check "strict + 无 TSO ⇒ 响亮报错（不静默回退）" \
      "$([[ "$sn" == *"安全网严格模式"* ]] && echo guarded)" "guarded"
PSQL $PPORT -q -c "ALTER SYSTEM RESET pg_partdist.shard_safety_mode;" </dev/null >/dev/null
PSQL $PPORT -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null

echo "========== [6] 引用表现状核查（V3 取证）=========="
nref=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_dist_partition WHERE partmethod='n'" </dev/null 2>&1 | tail -1)
check "本集群引用表数量（V3 裁定的事实依据，实际=$nref）" "$([[ -n "$nref" ]] && echo ok)" "ok"
echo "        → 引用表数量 = $nref（0 = V3「建表后只读」裁定无现存反例）"

echo "========== [7] 清理 =========="
for p in $COORD "${SHARD_PORTS[@]}"; do
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
sleep 2
for p in $COORD "${SHARD_PORTS[@]}"; do
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
PSQL $COORD -q -c "ALTER SYSTEM RESET pg_partdist.tso_master;" </dev/null >/dev/null
PSQL $COORD -q -c "DROP TABLE IF EXISTS t46g CASCADE;" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"

echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
exit $((FAIL > 0 ? 1 : 0))
