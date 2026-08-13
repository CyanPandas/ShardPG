#!/usr/bin/env bash
# [宿主机] P2 分片 clog 正式验收（TX_TSO_MVCC_DEV_PLAN.md §3.1 T2.8）。
#
# 覆盖：T2.1 分片域 clog 存储层 + DROP 提交时点 GC；T2.2 补丁 0007（commit/
#       abort 记录体分片 xid 块 + redo 落账，pg_waldump 取证）；T2.3 可见性
#       真相源=clog（未提交崩→不可见、提交+回滚崩→恰好提交可见）；T2.4 无主
#       RUNNING 认领（读路径触发、洞改判、终局保留、幂等、防误杀）；T2.5 WAL
#       影子推进 + 终局槽跳过（水位文件缺失不重号）；T2.6 ANALYZE 读侧分叉
#       （补丁 0008）+ 禁令面；T2.7 partition_map 驱动门控（无 GUC 打标、
#       重启存续、未登记零变化）。
# 工程纪律：容器调用一律 </dev/null；断言空值即 FAIL；负向配计数守卫；
#       结尾节点健康检查（lib_node_health.sh）。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
WPORT="${WPORT:-5433}"
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }

check() {
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

NEG_RUN=0
neg() {  # neg <名字> <期望片段> <SQL>
  local hit
  hit=$(PSQL "$WPORT" -Atc "$3" </dev/null 2>&1 | grep -c "$2")
  check "负向:$1" "$hit" "1"
  NEG_RUN=$((NEG_RUN+1))
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

crash_restart() {  # immediate 崩溃 + 拉起（等待就绪）
  DEX /work/pg-install/bin/pg_ctl -D "$DATADIR" -m immediate stop </dev/null >/dev/null 2>&1
  sleep 1
  DEX /work/pg-install/bin/pg_ctl -D "$DATADIR" -l "$DATADIR/startup.log" start </dev/null >/dev/null 2>&1
  local up="" t
  for t in $(seq 1 45); do
    up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null 2>/dev/null) && [[ "$up" == "1" ]] && break; sleep 1
  done
  [[ "$up" == "1" ]]
}
clean_restart() {
  DEX /work/pg-install/bin/pg_ctl -D "$DATADIR" restart -m fast -l "$DATADIR/startup.log" </dev/null >/dev/null 2>&1
  local up="" t
  for t in $(seq 1 45); do
    up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null 2>/dev/null) && [[ "$up" == "1" ]] && break; sleep 1
  done
  [[ "$up" == "1" ]]
}
set_whitelist() {  # set_whitelist <oid 串（可空）>
  PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '$1';" </dev/null >/dev/null
  PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
  local guc="" t
  for t in $(seq 1 10); do
    guc=$(PSQL "$WPORT" -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$1" ]] && break; sleep 1
  done
  [[ "$guc" == "$1" ]]
}
WALDUMP() { DEX /work/pg-install/bin/pg_waldump -p "$DATADIR/pg_wal" -s "$1" -e "$2" </dev/null 2>/dev/null; }

echo "========== [0] 前置：节点就绪 + 测试函数 =========="
up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null)
check "worker :$WPORT 可用" "$up" "1"
nm8=$(DEX bash -c "nm -D /work/pg-install/bin/postgres | grep -cE 'shard_relation_xid_hook|shard_visibility_hooks|shard_xact_wal_list_hook|shard_xact_redo_hook|shard_vacuum_read_hook'" </dev/null)
check "内核补丁 0005–0008 符号齐（5 组）" "$nm8" "5"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION sclog_read(oid, bigint) RETURNS int
  AS '$libdir/pg_partdist','partdist_shard_clog_read' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_write(oid, bigint, int) RETURNS void
  AS '$libdir/pg_partdist','partdist_shard_clog_write' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_claim(oid) RETURNS int
  AS '$libdir/pg_partdist','partdist_shard_claim' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_set_shard_mvcc(regclass, boolean DEFAULT true) RETURNS void
  AS '$libdir/pg_partdist','partdist_set_shard_mvcc' LANGUAGE C STRICT;
SQL
check "测试函数就绪" "ok" "ok"
DATADIR=$(PSQL "$WPORT" -Atc "SHOW data_directory" </dev/null)

echo "========== [1] T2.1 存储层 + DROP GC =========="
PSQL "$WPORT" -q -c "DROP TABLE IF EXISTS p2a;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; CREATE TABLE p2a(id int, v text) WITH (autovacuum_enabled=off);" </dev/null >/dev/null
OA=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p2a'" </dev/null)
set_whitelist "$OA"; check "p2a 白名单生效" "$?" "0"
PSQL "$WPORT" -Atc "SELECT sclog_write($OA::oid,3::bigint,0); SELECT sclog_write($OA::oid,4::bigint,2); SELECT sclog_write($OA::oid,5::bigint,3); SELECT sclog_write($OA::oid,1048579::bigint,2); SELECT 't'" </dev/null >/dev/null
check "读 RUNNING(3)=0"   "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OA::oid,3::bigint)" </dev/null)" "0"
check "读 COMMITTED(4)=2" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OA::oid,4::bigint)" </dev/null)" "2"
check "读 ABORTED(5)=3"   "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OA::oid,5::bigint)" </dev/null)" "3"
check "洞(999999)=RUNNING" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OA::oid,999999::bigint)" </dev/null)" "0"
check "跨段(1048579)=2"   "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OA::oid,1048579::bigint)" </dev/null)" "2"
rej=$(PSQL "$WPORT" -Atc "SELECT sclog_write($OA::oid,2::bigint,2)" </dev/null 2>&1 | grep -c "保留")
check "保留号 2 拒写" "$rej" "1"
segs=$(DEX ls "$DATADIR/pg_shard_clog/$OA" </dev/null | sort | tr '\n' ' ')
check "稀疏段文件两枚" "$segs" "00000000 00000001 "
PSQL "$WPORT" -q -c "INSERT INTO p2a VALUES (1,'a');" </dev/null >/dev/null
PSQL "$WPORT" -q -c "BEGIN; DROP TABLE p2a; ROLLBACK;" </dev/null >/dev/null
d1=$(DEX ls -d "$DATADIR/pg_shard_clog/$OA" </dev/null 2>/dev/null | wc -l)
check "DROP 回滚不删 clog 目录" "$d1" "1"
neg "含分片 DROP 的事务禁 PREPARE" "不支持" "BEGIN; DROP TABLE p2a; PREPARE TRANSACTION 'p2t';"
PSQL "$WPORT" -q -c "DROP TABLE p2a;" </dev/null >/dev/null
d1=$(DEX ls -d "$DATADIR/pg_shard_clog/$OA" </dev/null 2>/dev/null | wc -l)
d2=$(DEX ls "$DATADIR/pg_shard_xid/$OA" </dev/null 2>/dev/null | wc -l)
check "DROP 提交删 clog 目录" "$d1" "0"
check "DROP 提交删水位文件" "$d2" "0"

echo "========== [2] T2.2 判决闭环取证（pg_waldump） =========="
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; CREATE TABLE p2b(id int, v text) WITH (autovacuum_enabled=off); CREATE TABLE p2bn(id int) WITH (autovacuum_enabled=off);" </dev/null >/dev/null
OB=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p2b'" </dev/null)
set_whitelist "$OB"; check "p2b 白名单生效" "$?" "0"
L0=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
PSQL "$WPORT" -q -c "INSERT INTO p2b SELECT g,'ok'||g FROM generate_series(1,3) g;" </dev/null >/dev/null
L1=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
SB=$(PSQL "$WPORT" -Atc "SELECT xmin::text::bigint FROM p2b LIMIT 1" </dev/null)
cm=$(WALDUMP "$L0" "$L1" | grep -c "COMMIT.*shard xids: $OB/$SB")
check "COMMIT 记录携带 shard xids: $OB/$SB" "$cm" "1"
L2=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
PSQL "$WPORT" -q -c "BEGIN; INSERT INTO p2b VALUES (99,'dead'); ROLLBACK;" </dev/null >/dev/null
# abort 记录不同步刷盘（pg_current_wal_lsn 是写出位置）——一笔提交事务驱动
# 刷盘，否则 waldump 在段文件里看不到它（收编时抓出的 flaky，t22 曾靠运气过）
PSQL "$WPORT" -q -c "INSERT INTO p2bn VALUES (999);" </dev/null >/dev/null
L3=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
SB2=$((SB + 1))
ab=$(WALDUMP "$L2" "$L3" | grep -c "ABORT.*shard xids: $OB/$SB2")
check "ABORT 记录携带 shard xids: $OB/$SB2" "$ab" "1"
L4=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
PSQL "$WPORT" -q -c "INSERT INTO p2bn VALUES (1);" </dev/null >/dev/null
L5=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
nz=$(WALDUMP "$L4" "$L5" | grep -c "shard xids")
check "原生事务记录体零分片块" "$nz" "0"
check "崩前正常路径判决可查：COMMITTED" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,${SB}::bigint)" </dev/null)" "2"
check "崩前正常路径判决可查：ABORTED"   "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,${SB2}::bigint)" </dev/null)" "3"

echo "========== [3] T2.3/T2.4 崩溃可见性 + 认领（核心门禁） =========="
# 留一个未提交事务（CHECKPOINT 固化元组——不固化则行不落盘，读路径无从触发）
PSQL "$WPORT" -q -c "BEGIN; INSERT INTO p2b VALUES (50,'open'),(51,'open'); SELECT pg_sleep(30);" </dev/null >/dev/null 2>&1 &
sleep 3
PSQL "$WPORT" -q -c "CHECKPOINT;" </dev/null >/dev/null
crash_restart
check "崩溃重启存活" "$?" "0"
SB3=$((SB + 2))
check "★ 提交者可见、回滚/未决不可见（count=3）" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p2b" </dev/null)" "3"
check "redo 落账：COMMITTED 持久" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,${SB}::bigint)" </dev/null)" "2"
check "redo 落账：ABORTED 持久"   "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,${SB2}::bigint)" </dev/null)" "3"
check "无主事务已被认领改判 ABORTED" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,${SB3}::bigint)" </dev/null)" "3"
check "跳号洞一并改判（sxid=2000）" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,2000::bigint)" </dev/null)" "3"
check "认领上限之上未动（4099=洞）" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,4099::bigint)" </dev/null)" "0"
wmv=$(DEX od -An -tu4 "$DATADIR/pg_shard_xid/$OB" </dev/null | tr -s ' ' | sed 's/^ //;s/ $//')
check "水位文件 {4099,4099}" "$wmv" "4099 4099"
check "认领幂等（显式触发=0）" "$(PSQL "$WPORT" -Atc "SELECT sclog_claim($OB::oid)" </dev/null)" "0"
# R-P2-2 防误杀：新活事务不进认领范围
PSQL "$WPORT" -q -c "BEGIN; INSERT INTO p2b VALUES (60,'live'),(61,'live'); SELECT pg_sleep(5); COMMIT;" </dev/null >/dev/null 2>&1 &
BGP=$!
sleep 2
check "新活事务期间显式认领=0（防误杀）" "$(PSQL "$WPORT" -Atc "SELECT sclog_claim($OB::oid)" </dev/null)" "0"
wait $BGP 2>/dev/null
check "新活事务顺利提交（count=5）" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p2b" </dev/null)" "5"
check "新事务判 COMMITTED（sxid=4099）" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,4099::bigint)" </dev/null)" "2"
clean_restart
check "二次重启存活" "$?" "0"
check "二轮认领保留终局（4099 仍 COMMITTED）" "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OB::oid,4099::bigint)" </dev/null)" "2"
check "二次重启数据完好（count=5）" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p2b" </dev/null)" "5"

echo "========== [4] T2.5 恢复推进：水位文件缺失/终局槽不重号 =========="
crash_restart
check "三次崩溃重启存活" "$?" "0"
DEX rm -f "$DATADIR/pg_shard_xid/$OB" </dev/null
PSQL "$WPORT" -q -c "INSERT INTO p2b VALUES (70,'x');" </dev/null >/dev/null
S70=$(PSQL "$WPORT" -Atc "SELECT xmin::text::bigint FROM p2b WHERE id=70" </dev/null)
check "删水位文件后仍不重号（影子推进 >4099）" "$([[ -n "$S70" && "$S70" -gt 4099 ]] && echo ok)" "ok"
check "老数据完好（count=6）" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p2b" </dev/null)" "6"
NXT=$((S70 + 1)); NXT2=$((S70 + 2)); NXT3=$((S70 + 3))
PSQL "$WPORT" -Atc "SELECT sclog_write($OB::oid,${NXT}::bigint,2); SELECT sclog_write($OB::oid,${NXT2}::bigint,3); SELECT 't'" </dev/null >/dev/null
PSQL "$WPORT" -q -c "INSERT INTO p2b VALUES (71,'y');" </dev/null >/dev/null
S71=$(PSQL "$WPORT" -Atc "SELECT xmin::text::bigint FROM p2b WHERE id=71" </dev/null)
check "终局槽跳过守卫（绕过伪造判决发 $NXT3）" "$S71" "$NXT3"

echo "========== [5] T2.6 ANALYZE 读侧 + 禁令面 =========="
PSQL "$WPORT" -q -c "DELETE FROM p2b WHERE id IN (60,61);" </dev/null >/dev/null
an=$(PSQL "$WPORT" -Atc "ANALYZE p2b; SELECT reltuples::int FROM pg_class WHERE oid=$OB" </dev/null 2>&1 | tail -1)
check "ANALYZE 成功且 reltuples=活行 5" "$an" "5"
anall=$(PSQL "$WPORT" -Atc "ANALYZE; SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "整库 ANALYZE 放行" "$anall" "ok"
neg "VACUUM 点名"        "ERROR" "VACUUM p2b"
neg "VACUUM (ANALYZE)"   "ERROR" "VACUUM (ANALYZE) p2b"
neg "VACUUM FULL"        "ERROR" "VACUUM FULL p2b"
neg "整库 VACUUM"        "ERROR" "VACUUM"
neg "CLUSTER"            "ERROR" "CLUSTER p2b USING nosuch"
neg "CREATE INDEX"       "ERROR" "CREATE INDEX ON p2b(id)"
neg "CIC"                "ERROR" "CREATE INDEX CONCURRENTLY ON p2b(id)"
PSQL "$WPORT" -q -c "DROP TABLE p2b, p2bn;" </dev/null >/dev/null
set_whitelist ""; check "白名单清空" "$?" "0"

echo "========== [6] T2.7 partition_map 驱动门控（无 GUC） =========="
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; CREATE TABLE p2f(id int, v text) WITH (autovacuum_enabled=off); CREATE TABLE p2fn(id int) WITH (autovacuum_enabled=off);" </dev/null >/dev/null
OF=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p2f'" </dev/null)
PSQL "$WPORT" -q -c "INSERT INTO p2f VALUES (0,'native');" </dev/null >/dev/null
nx=$(PSQL "$WPORT" -Atc "SELECT xmin::text::bigint > 1000 FROM p2f WHERE id=0" </dev/null)
check "未登记表 xmin 原生" "$nx" "t"
PSQL "$WPORT" -q -c "TRUNCATE p2f;" </dev/null >/dev/null
neg "partition_map 无行拒登记" "没有分区" "SELECT partdist_set_shard_mvcc('p2f'::regclass)"
PSQL "$WPORT" -q -c "INSERT INTO partdist.partition_map (partition_id, primary_node) VALUES ($OF, 1);" </dev/null >/dev/null
reg=$(PSQL "$WPORT" -Atc "SELECT partdist_set_shard_mvcc('p2f'::regclass); SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "登记成功" "$reg" "ok"
check "真相列翻 true" "$(PSQL "$WPORT" -Atc "SELECT shard_mvcc FROM partdist.partition_map WHERE partition_id=$OF" </dev/null)" "t"
neg "撤销登记被拒（只进不出）" "不支持撤销" "SELECT partdist_set_shard_mvcc('p2f'::regclass, false)"
PSQL "$WPORT" -q -c "INSERT INTO p2f VALUES (1,'a'),(2,'b');" </dev/null >/dev/null
check "无 GUC 打标（xmin=3）" "$(PSQL "$WPORT" -Atc "SELECT xmin::text::bigint FROM p2f WHERE id=1" </dev/null)" "3"
PSQL "$WPORT" -q -c "BEGIN; INSERT INTO p2f VALUES (9,'dead'); ROLLBACK;" </dev/null >/dev/null
check "回滚行不可见" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p2f" </dev/null)" "2"
clean_restart
check "重启存活" "$?" "0"
check "重启后集合从目录重建（count=2）" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p2f" </dev/null)" "2"
PSQL "$WPORT" -q -c "INSERT INTO p2f VALUES (3,'c');" </dev/null >/dev/null
check "重启后新写仍打标（4099）" "$(PSQL "$WPORT" -Atc "SELECT xmin::text::bigint FROM p2f WHERE id=3" </dev/null)" "4099"
nx2=$(PSQL "$WPORT" -Atc "INSERT INTO p2fn VALUES (1); SELECT xmin::text::bigint > 1000 FROM p2fn" </dev/null | tail -1)
check "未登记对照表仍原生" "$nx2" "t"

echo "========== [7] 负向计数守卫 + 清理 + 节点健康 =========="
check "负向用例计数守卫（应跑 10 条）" "$NEG_RUN" "10"
PSQL "$WPORT" -q -c "DROP TABLE p2f, p2fn;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "DELETE FROM partdist.partition_map WHERE partition_id=$OF;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "DROP FUNCTION IF EXISTS sclog_read(oid,bigint); DROP FUNCTION IF EXISTS sclog_write(oid,bigint,int); DROP FUNCTION IF EXISTS sclog_claim(oid); DROP FUNCTION IF EXISTS partdist_set_shard_mvcc(regclass,boolean);" </dev/null >/dev/null
gone=$(DEX ls "$DATADIR/pg_shard_xid/$OF" </dev/null 2>/dev/null | wc -l)
check "DROP 后登记文件全清" "$gone" "0"
health_check_no_crash

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
exit "$FAIL"
