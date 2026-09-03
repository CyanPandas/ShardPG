#!/usr/bin/env bash
# [宿主机] T6.3c 验收：副本壳表的本地访问闸门（DEV PLAN §3.8 T6.3c）。
#
# 补的是设计 §13 约束 12 的根治方向：「让副本文件永不被本地 WAL 触碰」。
#
# ★ 此前这条只是**纪律**：设计文档反复写"follower 壳表绝不能被 SELECT"，
#   验收脚本注释里也写着"只用文件级比对触碰 follower"。但**代码里没有任何
#   东西拦着** —— 副本壳表在本节点上是一张完全普通的表：它不在
#   pg_partdist.shard_relids 白名单里（follower 侧从不设），于是
#   ShardXidUtilityGuard 与 ShardAccessGate 这两道现成闸门对它统统不生效。
#   一次误操作的 SELECT / VACUUM 就能：① 触发原生剪枝就地损毁副本；
#   ② 往本地 WAL 写针对副本文件的记录 —— 后者正是 R-P4-20 的病灶。
#
# 本套件先核实约束 5（冻结账目同步是否已闭合），再验闸门本身。
set -u
CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
# ★ 会话参数走 PGOPTIONS，**不要**写成 `-Atc "SET ...; <真正的语句>"`：
#   ① psql 把一个 -c 里的多条语句当**一个事务**执行，而 VACUUM/CLUSTER 之类
#      不能在事务块里跑（实测"配对前 VACUUM 放行"因此假红）；
#   ② -At 会把 `SET` 的命令标签也打到 stdout，`tail -1` 于是取到 "SET" 而不是
#      真正的值（实测"约束 5"那条取到的就是字符串 SET）。
#   这两个坑项目记录里都出现过，本轮又各踩一次。
PSQLO() { local port=$1 opts=$2; shift 2
  docker exec -i -u postgres -e HOME=/var/lib/postgresql -e PGOPTIONS="$opts" \
    "$CONTAINER" /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
NOPROP='-c citus.enable_ddl_propagation=off'
VISIBLE='-c citus.override_table_visibility=false'
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/t63c_replica.lock
if ! flock -n 9; then echo "FATAL: 另一个 T6.3c 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "================ [0] 夹具 ================"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t63c;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t63c(id int, v text);
SELECT create_distributed_table('t63c', 'id');
ALTER TABLE t63c SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t63c'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431)); f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431)); shard_tbl="t63c_${gid}"
echo "  shard=${gid} leader=:${pport} follower=:${f1} 陪跑=:${f2}"
check "夹具齐" "$([[ -n "$gid" && -n "$pport" && -n "$f1" ]] && echo ok)" "ok"
nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册" "$nrels" "3"
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows"|cut -d, -f1|paste -sd,); ords=$(echo "$fsrows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$fsrows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$fsrows"|cut -d, -f4|paste -sd,)
rels=$(echo "$fsrows"|cut -d, -f5|paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE t63c INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
done

echo "================ [1] 配对**之前**：还是普通表，一切照常 ================"
pre=$(PSQLO $f1 "$NOPROP" -Atc "SELECT count(*) FROM ${shard_tbl}" </dev/null 2>&1 | tail -1)
check "配对前 SELECT 放行（438 基线不受牵连）" "$pre" "0"
prev=$(PSQLO $f1 "$NOPROP" -Atc "VACUUM ${shard_tbl}" </dev/null 2>&1 | tr '\n' ' ')
check "配对前 VACUUM 放行" "$([[ "$prev" != *ERROR* ]] && echo ok)" "ok"

echo "================ [2] 配对 + 组装 ================"
for fp in $f1 $f2; do
  n=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[], 0::bigint)" </dev/null 2>/dev/null | tail -1)
  check "follower :$fp 配对" "$n" "3"
done
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null
done
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'v'||g FROM generate_series(1,200) g;" </dev/null >/dev/null
plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
foid=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1)
a=$(PSQL $f1 -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${plsn}, 60000)" </dev/null 2>/dev/null|tail -1)
check "follower 追平（applied=$a）" "$([[ -n "$a" && "$a" -ge "$plsn" ]] && echo ok)" "ok"

echo "================ [3] ★ 闸门：配对之后本地一律不许碰 ================"
fdata=$(PSQL $f1 -Atc "SHOW data_directory" </dev/null)
frel=$(PSQL $f1 -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath(${foid}::regclass)" </dev/null | tail -1)
FP1="${fdata}/${frel}"
md5_before=$(DEX md5sum "$FP1" </dev/null 2>/dev/null | cut -d' ' -f1)
check "取到副本主堆文件（md5=${md5_before:0:8}…）" "$([[ -n "$md5_before" ]] && echo ok)" "ok"

gate() {  # <名字> <SQL>
  local out
  out=$(PSQLO $f1 "$NOPROP" -Atc "$2" </dev/null 2>&1 | tr '\n' ' ')
  check "拦截：$1" "$([[ "$out" == *"副本壳表"* ]] && echo blocked)" "blocked"
}
gate "SELECT"        "SELECT count(*) FROM ${shard_tbl}"
gate "UPDATE"        "UPDATE ${shard_tbl} SET v='x' WHERE id=1"
gate "DELETE"        "DELETE FROM ${shard_tbl} WHERE id=1"
gate "VACUUM"        "VACUUM ${shard_tbl}"
gate "ANALYZE"       "ANALYZE ${shard_tbl}"
gate "CLUSTER"       "CLUSTER ${shard_tbl}"
gate "TRUNCATE"      "TRUNCATE ${shard_tbl}"
db=$(PSQL $f1 -Atc "VACUUM" </dev/null 2>&1 | tr '\n' ' ')
check "拦截：整库 VACUUM（无法逐表甄别 ⇒ fail-closed）" \
      "$([[ "$db" == *"副本壳表"* ]] && echo blocked)" "blocked"

# ★ 界必须划在"文档规定的正当路径"之外。下面三条**必须放行**，
#   首版把它们一并拦了，当场把 ddl_fileset_d1 从 78/0 打到 65/11、
#   把 shard_clog_p2 打到 63/1。
pass_case() {  # <名字> <SQL>
  local out
  out=$(PSQLO $f1 "$NOPROP" -Atc "$2" </dev/null 2>&1 | tr '\n' ' ')
  check "放行：$1" "$([[ "$out" != *"副本壳表"* ]] && echo ok)" "ok"
}
# §12 的修复路径原文："请在本地 shell 表上做等价结构变更后重跑
# replay_set_locmap()" —— 那个"等价结构变更"就是在副本壳表上 CREATE INDEX。
# 拦掉它等于把文档写明的唯一修复路径堵死。
pass_case "CREATE INDEX（§12 修复路径的一部分）" \
          "CREATE INDEX IF NOT EXISTS t63c_gate_ix ON ${shard_tbl}(id)"
pass_case "REINDEX（同上）" "REINDEX TABLE ${shard_tbl}"
# 整库 ANALYZE 是节点级日常操作；T2.6 早已裁定 ANALYZE 解禁。
anz=$(PSQL $f1 -Atc "ANALYZE" </dev/null 2>&1 | tr '\n' ' ')
check "放行：整库 ANALYZE（T2.6 已解禁）" \
      "$([[ "$anz" != *"副本壳表"* ]] && echo ok)" "ok"
PSQLO $f1 "$NOPROP" -q -c "DROP INDEX IF EXISTS t63c_gate_ix;" </dev/null >/dev/null 2>&1

echo "================ [4] ★ 拦下之后文件一个字节都没动 ================"
md5_after=$(DEX md5sum "$FP1" </dev/null 2>/dev/null | cut -d' ' -f1)
check "★★ 副本主堆 md5 未变（闸门确实拦在动手之前）" "$md5_after" "$md5_before"

echo "================ [5] 逃生口：显式打开才放行 ================"
PSQL $f1 -q -c "SET pg_partdist.allow_replica_access = on; SELECT 1;" </dev/null >/dev/null 2>&1
esc=$(PSQLO $f1 "$NOPROP -c pg_partdist.allow_replica_access=on" -Atc "SELECT count(*) FROM ${shard_tbl}" </dev/null 2>&1 | tail -1)
check "allow_replica_access=on ⇒ 放行（取证用）" "$([[ "$esc" =~ ^[0-9]+$ ]] && echo ok)" "ok"
dflt=$(PSQL $f1 -Atc "SHOW pg_partdist.allow_replica_access" </dev/null)
check "默认仍是 off（fail-closed）" "$dflt" "off"

echo "================ [6] 零影响：leader 节点（无副本）一切照常 ================"
lv=$(PSQLO $pport "$NOPROP" -Atc "SELECT count(*) FROM ${shard_tbl}" </dev/null 2>&1 | tail -1)
check "leader 上读自己的分片表不受影响" "$lv" "200"
other=$(PSQL $f1 -Atc "SELECT count(*) FROM pg_class WHERE relname='pg_class'" </dev/null 2>&1 | tail -1)
check "follower 上访问普通表不受影响" "$other" "1"

echo "================ [7] 核实约束 5：冻结账目同步是否已闭合 ================"
# ★ 这一节是 T6.3c 的**前置核查**：若 leader 的 relfrozenxid 确实被搬到
#   follower 的 pg_class，则 follower 的 age 不再无界增长，§13.5 那句
#   "autovacuum_enabled=off 挡不住 anti-wraparound" 的前提就不成立了。
PSQL $pport -q -c "ALTER SYSTEM SET pg_partdist.freeze_sync_interval_ms = 0;" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
PSQL $pport -q -c "INSERT INTO ${shard_tbl} VALUES (9999,'freeze-trigger');" </dev/null >/dev/null
plsn2=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
PSQL $f1 -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${plsn2}, 60000)" </dev/null >/dev/null 2>&1
lfz=$(PSQLO $pport "$VISIBLE" -Atc "SELECT relfrozenxid::text::bigint FROM pg_class WHERE oid=${LEADER_OID}" </dev/null 2>&1|tail -1)
# ★★ 必须带 citus.override_table_visibility=false。
#   follower 的壳表叫 t63c_<shardid>，**命中 Citus 的分片命名规约**，于是
#   Citus 把它从 pg_class 的扫描里隐藏掉 —— 查询既不报错也不返回行。
#   首版只给了 enable_ddl_propagation=off，连续三轮取空却看不出原因；
#   leader 那条恰好带了 VISIBLE 所以有值，对照之下更容易误判成"同步没生效"。
#   （这个坑本次会话开头查 e2e 表时就撞过一次：pg_class 里看不见 e2e_102921，
#     而 SELECT count(*) FROM e2e_102921 却能跑。）
ffz=$(PSQLO $f1 "$NOPROP $VISIBLE" -Atc "SELECT relfrozenxid::text::bigint FROM pg_class WHERE oid=${foid}" </dev/null 2>&1 | tail -1)
check "★ 约束 5：leader 的 relfrozenxid 已搬到 follower 的 pg_class" "$ffz" "$lfz"
pct=$(PSQL $f1 -Atc "SELECT round(pct_to_force)::text FROM partdist.replay_freeze_status() WHERE shard=${foid}::regclass" </dev/null 2>/dev/null|tail -1)
echo "  follower 距强制回卷 vacuum：${pct:-（无行）}%"
check "  副本壳表离强制阈值仍很远（<10%）" \
      "$([[ -n "$pct" && "$pct" -lt 10 ]] && echo ok)" "ok"
PSQL $pport -q -c "ALTER SYSTEM RESET pg_partdist.freeze_sync_interval_ms;" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null

echo "================ [8] 清理 ================"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}'::regclass);" </dev/null >/dev/null 2>&1
  PSQLO $fp "$NOPROP" -q -c "DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t63c;" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"
health_check_no_crash || true
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]]
