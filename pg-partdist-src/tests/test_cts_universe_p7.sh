#!/usr/bin/env bash
# [宿主机] P7-G4 验收：commit_ts 的"宇宙"必须随记录走，中途配上 TSO 不许让已提交的行消失。
#
# 缺陷（2026-09-12 登记）：commit_ts 是**双宇宙**字段 —— 配了 TSO 是从 1 起的小整数，
# 没配是本地墙钟（~8.4e14 微秒）。可见性判据 §4.1 是 `commit_ts < 读者 start_ts`。
#   · leader 本地的账在遗留模式下存的是 **0**（TsoStashedCommitTs、0007 记录、DTX 决议 ts
#     全是 0 = "对一切快照可见"）；
#   · 唯独 MARKER 这一条路（TsoMarkerCommitTs）遗留模式填的是**墙钟**，回放把它原样
#     写进副本的分片 clog 与 gclog —— 同一笔提交，leader 记 0、副本记墙钟。
# TSO 整簇一致时两边恒同宇宙，不发作；**中途给读者配上 TSO**，副本（或升主后的新主）
# 就拿墙钟去比 TSO 号：墙钟恒大 ⇒ 已提交的行永久不可见。
#
# 几何（沿用 promote_catchup_tx3 的夹具，那条路已验证 24/0）：
#   [2] leader 在**遗留模式**写 40 行（MARKER 带墙钟 commit_ts）
#   [3] 杀 leader，副本当选并追平（R3 读路径：gclog 判可见性）
#   [4] 前提 + 对照：新主 gclog 里该事务 commit_ts 确是墙钟量级；不配 TSO 时读得到 40 行
#   [5] ★★ 给新主配上 TSO（中途改配置）再读：必须仍是 40 行。修复前是 0 行。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
NROWS=40
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
PGCTL(){ DEX /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/$1" "${@:2}"; }
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/p7_cts_universe.lock
if ! flock -n 9; then echo "FATAL: 另一个 commit_ts 宇宙验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start
TSO_CONN='host=/tmp port=5432 dbname=postgres user=postgres'

echo "================ [0] TSO 服务在协调者上就位；写者与读者先都不配（遗留模式）================"
master_before=$(PSQL $COORD -Atc "SHOW pg_partdist.tso_master" </dev/null)
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_master = on;" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
sleep 2
check "协调者 tso_master=on" "$(PSQL $COORD -Atc 'SHOW pg_partdist.tso_master' </dev/null)" "on"

echo "================ [1] 夹具：单分片 + 两个 follower 副本 ================"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t7cts;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t7cts(id int primary key, v text);
SELECT create_distributed_table('t7cts','id');
ALTER TABLE t7cts SET (autovacuum_enabled = off);
SQL
GID=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t7cts'::regclass" </dev/null)
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${GID}" </dev/null)
TBL="t7cts_${GID}"
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$PA" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; break; fi
done
echo "  shard=${GID} leader=:${PA} followers=:${f1} :${f2}"
check "夹具齐" "$([[ -n "$GID" && -n "$PA" && -n "$f1" && -n "$f2" ]] && echo ok || echo no)" "ok"
check "写者 :$PA 未配 TSO（遗留模式）" "$(PSQL $PA -Atc 'SHOW pg_partdist.tso_conninfo' </dev/null | grep -c .)" "0"

nrels=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${TBL}')" </dev/null | tail -1)
check "leader fileset 注册" "$([[ "$nrels" =~ ^[0-9]+$ && "$nrels" -ge 2 ]] && echo ok || echo no)" "ok"
fsrows=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${TBL}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows"|cut -d, -f1|paste -sd,); ords=$(echo "$fsrows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$fsrows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$fsrows"|cut -d, -f4|paste -sd,)
rels=$(echo "$fsrows"|cut -d, -f5|paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${TBL};
CREATE TABLE ${TBL} (LIKE t7cts INCLUDING ALL);
ALTER TABLE ${TBL} SET (autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${TBL}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[], 0::bigint)" </dev/null 2>/dev/null | tail -1)
  check "follower :$fp locmap 配对" "$([[ "$np" =~ ^[0-9]+$ && "$np" -ge 1 ]] && echo ok || echo no)" "ok"
done
pnode=$((PA - 5431)); f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
PSQL $PA -q -c "SELECT partdist.pg_raft_group_create(${GID}, ${members});" </dev/null >/dev/null
st=""
for t in $(seq 1 25); do
  st=$(PSQL $PA -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${GID}, ${members});" </dev/null >/dev/null
  PSQL $fp -q -c "SELECT partdist.replay_enable('${TBL}');" </dev/null >/dev/null
done

echo "================ [2] leader 遗留模式写 ${NROWS} 行，副本收齐字节 ================"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO t7cts SELECT g, 'v'||g FROM generate_series(1,${NROWS}) g;" </dev/null
LOID=$(PSQL $PA -Atc "SELECT partdist.local_partition_for_shard(${GID})" </dev/null | tail -1)
tip=$(PSQL $PA -Atc "SELECT partdist.get_partition_flush_lsn(${LOID})" </dev/null | tail -1)
check "leader 段流有记录（tip=${tip}）" "$([[ "$tip" =~ ^[0-9]+$ && "$tip" -gt 0 ]] && echo ok || echo no)" "ok"
for fp in $f1 $f2; do
  foid=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${GID})" </dev/null | tail -1)
  fl=""
  for t in $(seq 1 60); do
    fl=$(PSQL $fp -Atc "SELECT partdist.get_partition_flush_lsn(${foid})" </dev/null | tail -1)
    [[ "$fl" =~ ^[0-9]+$ && "$fl" -ge "$tip" ]] && break; sleep 2
  done
  check "follower :$fp 收齐字节（${fl}/${tip}）" "$([[ "$fl" =~ ^[0-9]+$ && "$fl" -ge "$tip" ]] && echo ok || echo no)" "ok"
done

echo "================ [3] 杀 leader，等新主被登记并追平 ================"
leader_dir="worker$((PA - 5432))"
PGCTL "$leader_dir" -m immediate -w -t 60 stop </dev/null >/dev/null 2>&1
newp=0
for t in $(seq 1 90); do
  newp=$(PSQL $COORD -Atc "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${GID}" </dev/null 2>/dev/null)
  [[ -n "$newp" && "$newp" != "$pnode" && "$newp" != "0" ]] && break
  sleep 2
done
check "控制面登记了新主（node ${newp}）" "$([[ -n "$newp" && "$newp" != "$pnode" && "$newp" != "0" ]] && echo ok || echo no)" "ok"

if [[ -n "$newp" && "$newp" != "$pnode" && "$newp" != "0" ]]; then
  NP=$((newp + 5431))
  noid=$(PSQL $NP -Atc "SELECT partdist.local_partition_for_shard(${GID})" </dev/null | tail -1)
  napp=$(PSQL $NP -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${noid}" </dev/null | tail -1)
  check "新主 :$NP 已追平（applied=${napp} >= ${tip}）" "$([[ "$napp" =~ ^[0-9]+$ && "$napp" -ge "$tip" ]] && echo ok || echo no)" "ok"
  COUNT() { PSQL $NP -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM ${TBL}" </dev/null | tail -1; }
  # 控制面登记新主与新主本地路由角色翻成 promoted 之间有个窗口，窗内读被副本访问闸拒绝（正确行为）——
  # 等待循环里吞掉这段报错；[5] 的读用 COUNT，报错照常可见
  COUNTQ() { PSQL $NP -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM ${TBL}" </dev/null 2>/dev/null | tail -1; }

  echo "================ [4] 前提与对照：新主未配 TSO（遗留读者）================"
  n0=""
  for t in $(seq 0 15); do n0=$(COUNTQ); [[ "$n0" == "$NROWS" ]] && break; sleep 1; done
  check "对照：遗留读者读到全部 ${NROWS} 行" "$n0" "$NROWS"
  xm=$(PSQL $NP -Atc "SET citus.enable_ddl_propagation=off; SELECT xmin::text::bigint FROM ${TBL} ORDER BY id LIMIT 1" </dev/null | tail -1)
  rr=$(PSQL $NP -Atc "SELECT role||'|'||coalesce(gxid::text,'null')||'|'||status FROM partdist.route_resolve(${noid}::oid, ${xm})" </dev/null | tail -1)
  check "R3 取证：promoted + xid→gxid + committed（${rr}）" \
        "$([[ "$rr" == promoted\|*\|committed && "$rr" != *"|null|"* ]] && echo ok || echo no)" "ok"
  gx=$(echo "$rr" | cut -d'|' -f2)
  cts=""
  if [[ "$gx" =~ ^[0-9]+$ ]]; then
    gnode=$(( gx >> 48 )); glocal=$(( gx & ((1<<48)-1) ))
    cts=$(PSQL $NP -Atc "SELECT commit_ts FROM partdist.gclog_status(${gnode}, ${glocal})" </dev/null | tail -1)
  fi
  echo "  gxid=${gx} gclog.commit_ts=${cts}"
  # ★ 没有这一条，[5] 可能只是因为 commit_ts 本来就是小整数而通过 —— 那是空检查
  check "★ 几何前提：该事务 gclog 里的 commit_ts 是墙钟量级（> 1e12，不是 TSO 号）" \
        "$([[ "$cts" =~ ^[0-9]+$ && "$cts" -gt 1000000000000 ]] && echo ok || echo no)" "ok"

  echo "================ [5] ★★ 给新主配上 TSO（中途改配置），已提交的行不许消失 ================"
  PSQL $NP -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = '${TSO_CONN}';" </dev/null >/dev/null
  PSQL $NP -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
  ts=""
  for t in $(seq 1 15); do
    ts=$(PSQL $NP -Atc "SELECT partdist.partdist_tso_client_start_ts()" </dev/null 2>/dev/null | tail -1)
    [[ "$ts" =~ ^[0-9]+$ && "$ts" -gt 0 ]] && break; sleep 1
  done
  check "新主 TSO 客户端已可取号（${ts}，TSO 宇宙的小整数）" \
        "$([[ "$ts" =~ ^[0-9]+$ && "$ts" -gt 0 && "$ts" -lt 1000000000000 ]] && echo ok || echo no)" "ok"
  n1=$(COUNT)
  check "★★ TSO 读者仍读到全部 ${NROWS} 行（修复前 0 行：墙钟 commit_ts >= TSO 快照）" "$n1" "$NROWS"
  rr2=$(PSQL $NP -Atc "SELECT role||'|'||status FROM partdist.route_resolve(${noid}::oid, ${xm})" </dev/null | tail -1)
  check "  判决本身没变（promoted|committed）" "$rr2" "promoted|committed"
  cts2=$(PSQL $NP -Atc "SELECT commit_ts FROM partdist.gclog_status(${gnode:-0}, ${glocal:-0})" </dev/null | tail -1)
  check "  gclog 原值保留供诊断（commit_ts 仍 = ${cts}）" "$cts2" "$cts"
  PSQL $NP -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null
  PSQL $NP -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
fi

echo "================ [6] 收尾 ================"
PGCTL "$leader_dir" -l "/work/pg-cluster-data/${leader_dir}/pg.log" -w -t 60 start </dev/null >/dev/null 2>&1
for t in $(seq 1 30); do [[ "$(PSQL $PA -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 2; done
check "原 leader :$PA 已恢复" "$(PSQL $PA -Atc 'SELECT 1' </dev/null 2>/dev/null)" "1"
for fp in $f1 $f2; do PSQL $fp -q -c "SELECT partdist.replay_disable('${TBL}');" </dev/null >/dev/null 2>&1; done
left=1
for t in $(seq 1 10); do
  for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_drop(${GID});" </dev/null >/dev/null 2>&1; done
  sleep 2; left=0
  for p in $PA $f1 $f2; do
    k=$(PSQL $p -Atc "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=${GID}" </dev/null | tail -1)
    left=$(( left + ${k:-1} ))
  done
  [[ "$left" == "0" ]] && break
done
for fp in $PA $f1 $f2; do PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL};" </dev/null >/dev/null 2>&1; done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t7cts;" </dev/null >/dev/null 2>&1
for p in $COORD $PA $f1 $f2; do PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${GID};" </dev/null >/dev/null 2>&1; done
[[ "$master_before" != "on" ]] && { PSQL $COORD -q -c "ALTER SYSTEM RESET pg_partdist.tso_master;" </dev/null >/dev/null; PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null; }
check "清理：三个成员上都不再有分区组 ${GID}" "$left" "0"
health_check_no_crash
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
