#!/usr/bin/env bash
# [宿主机] TX3：升主与惰性回放 promotion 路径合流（DTX_2PC_DESIGN.md §0.0 第 6 步 a）。
#
# 这条路径此前是断的：pg_partdist 导出了 partdist_replay_catchup_hook，pg_raft
# 一次都没取用；dtx_close_indoubt() 只有测试脚本调过；而升主序列是
# "登记 partition_map → 翻 pg_dist_placement（路由立刻切走）"，中间那个
# partwal_notify_primary_switch 整个函数体只有一行 ereport(LOG)。
# 于是新主可能在**一个字节都没回放**的情况下就开始对外服务。
#
# 接法：把追平+闭合放在"自治选举胜出 → 向控制面上报"之间
# （raft_consensus.c 的 data_group_try_report → partdist.pg_raft_promote_prepare）。
# 不上报 ⇒ 控制面不登记 ⇒ 路由不翻，"追不平不对外服务"由此天然成立，
# 且完全不阻塞 group 0 的 apply。
#
# 验收标准：
#   1. 惰性前提成立：follower 收齐了字节，但 applied 仍为 0（平时一条 redo 都不做）。
#   2. 杀掉 leader 后，某个 follower 自治当选并**完成追平**才被登记为新主：
#      partition_map.primary_node 变成它的那一刻，它的 replay_status().applied
#      必须已经 >= 切主前的已提交位点。
#   3. 新主壳表能读到切主前写入的全部行（回放真的落到了页面上）。
#   4. 全程无节点崩溃。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }
PGCTL(){ docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/$1" "${@:2}"; }

check() {
  if [[ -n "$2" && "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start
NROWS=40

echo "========== [1] 夹具：单分片表 + 两个 follower 副本 =========="
PSQL $COORD -q -c "DROP TABLE IF EXISTS tx3_promo;" >/dev/null 2>&1
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE tx3_promo(id int primary key, v text);
SELECT create_distributed_table('tx3_promo','id');
ALTER TABLE tx3_promo SET (autovacuum_enabled = off);
SQL
GID=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='tx3_promo'::regclass")
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${GID}")
TBL="tx3_promo_${GID}"
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$PA" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; break; fi
done
echo "  shard=${GID} leader=:${PA} followers=:${f1} :${f2}"

# 期望值从 catalog 推导（理由同 TX1/TX2：`-ge 1` 漏登记 TOAST/索引也照样通过，
# 而漏登记正是 follower 侧"未知 relfilelocator"PANIC 的来源，FRD §13 约束 1）。
want_rels=$(PSQL $PA -Atc "SET citus.override_table_visibility=false;
    SELECT 1 + (SELECT count(*) FROM pg_index WHERE indrelid = c.oid)
           + CASE WHEN c.reltoastrelid <> 0
                  THEN 1 + (SELECT count(*) FROM pg_index WHERE indrelid = c.reltoastrelid)
                  ELSE 0 END
    FROM pg_class c WHERE c.oid = '${TBL}'::regclass" | tail -1)
nrels=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${TBL}')" | tail -1)
check "leader fileset 注册成员数（catalog 推导应为 ${want_rels}）" "$nrels" "$want_rels"
fsrows=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${TBL}') ORDER BY role, ord" | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${TBL};
CREATE TABLE ${TBL} (LIKE tx3_promo INCLUDING ALL);
ALTER TABLE ${TBL} SET (autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${TBL}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])")
  check "follower :$fp locmap 配对" "$([[ -n "$np" && "$np" -ge 1 ]] && echo ok)" "ok"
done

pnode=$((PA - 5431)); f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null; done
PSQL $PA -q -c "SELECT partdist.pg_raft_group_create(${GID}, ${members});" >/dev/null
st=""
for t in $(seq 1 25); do
  st=$(PSQL $PA -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID}" 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${GID}, ${members});" >/dev/null
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${TBL}')")
  check "follower :$fp replay_enable（armed，但平时不回放）" "$en" "t"
done

echo "========== [2] 写入 + 惰性前提：字节到了、但一条都没回放 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q -c \
  "INSERT INTO tx3_promo SELECT g, 'v'||g FROM generate_series(1,${NROWS}) g;"
LOID=$(PSQL $PA -Atc "SELECT partdist.local_partition_for_shard(${GID})")
tip=$(PSQL $PA -Atc "SELECT partdist.get_partition_flush_lsn(${LOID})")
check "leader 段流有记录（tip=${tip}）" "$([[ -n "$tip" && "$tip" -gt 0 ]] && echo ok)" "ok"

for fp in $f1 $f2; do
  foid=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${GID})")
  fl=""
  for t in $(seq 1 60); do
    fl=$(PSQL $fp -Atc "SELECT partdist.get_partition_flush_lsn(${foid})")
    [[ -n "$fl" && "$fl" -ge "$tip" ]] && break; sleep 2
  done
  check "follower :$fp 收齐字节（${fl}/${tip}）" "$([[ -n "$fl" && "$fl" -ge "$tip" ]] && echo ok)" "ok"
  app=$(PSQL $fp -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}")
  check "follower :$fp 惰性前提：applied=0（平时一条 redo 都不做）" "$app" "0"
  rows=$(PSQL $fp -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM ${TBL}" | tail -1)
  check "follower :$fp 壳表此刻为空（回放还没发生）" "$rows" "0"
done

echo "========== [3] 杀掉 leader，等新主被登记 =========="
leader_dir="worker$((PA - 5432))"
PGCTL "$leader_dir" -m immediate -w -t 60 stop >/dev/null 2>&1
echo "  已停掉 leader :${PA}（${leader_dir}）"

newp=0
for t in $(seq 1 90); do
  newp=$(PSQL $COORD -Atc "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${GID}" 2>/dev/null)
  [[ -n "$newp" && "$newp" != "$pnode" && "$newp" != "0" ]] && break
  sleep 2
done
check "控制面登记了新主（node ${newp}，原主 node ${pnode}）" \
      "$([[ -n "$newp" && "$newp" != "$pnode" && "$newp" != "0" ]] && echo ok)" "ok"

if [[ -n "$newp" && "$newp" != "$pnode" && "$newp" != "0" ]]; then
  newport=$((newp + 5431))
  echo "  新主 = node ${newp} (:${newport})"
  noid=$(PSQL $newport -Atc "SELECT partdist.local_partition_for_shard(${GID})")

  echo "========== [4] ★ 核心：登记为新主的那一刻，它必须已经追平 =========="
  napp=$(PSQL $newport -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${noid}")
  check "新主 applied=${napp} 已达切主前位点 ${tip}（修复前恒为 0）" \
        "$([[ -n "$napp" && "$napp" -ge "$tip" ]] && echo ok)" "ok"

  # applied 位点达标 ≠ 行可见性同刻就绪：判决应用（marker→gclog→读路径）
  # 有异步尾巴，重载下会拉宽（P3 出口全量跑实测 0 行、单跑即绿，TX1 夹具
  # 竞态同族）。有界等待收敛断言 + 打印滞后量保留可观测性；15s 仍不收敛
  # 才是真回归（届时深查 R3 读路径的 xid_map/gclog 快照依赖）。
  nrows=""
  vt=0
  for vt in $(seq 0 15); do
    nrows=$(PSQL $newport -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM ${TBL}" | tail -1)
    [[ "$nrows" == "$NROWS" ]] && break
    sleep 1
  done
  [[ "$vt" -gt 0 ]] && echo "  （行可见性在 applied 达标后又滞后了 ${vt}s）"
  check "新主壳表读到切主前写入的全部 ${NROWS} 行" "$nrows" "$NROWS"

  nmax=$(PSQL $newport -Atc "SET citus.enable_ddl_propagation=off; SELECT coalesce(max(id),0) FROM ${TBL}" | tail -1)
  check "新主壳表内容完整（max(id)=${NROWS}）" "$nmax" "$NROWS"

  # ★ 对照组：**未当选**的那个 follower 必须仍然 applied=0。
  #
  # 没有这一条，[2]（字节到齐时 applied=0）与 [4]（十几秒后 applied>=tip）
  # 这两条断言，任何 naptime 大于二者间隔的**全局**后台追平机制都能同时满足 ——
  # 也就是说"是升主接线追的平"和"某个全局机制碰巧追上了"分不开。
  # 夹具本来就有两个 follower，只有一个当选，这个天然对照组不用白不用。
  other=""
  for p in $f1 $f2; do [[ "$p" != "$newport" ]] && other=$p; done
  if [[ -n "$other" ]]; then
    ooid=$(PSQL $other -Atc "SELECT partdist.local_partition_for_shard(${GID})")
    oapp=$(PSQL $other -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${ooid}")
    check "对照：未当选的 follower :${other} applied 仍为 0（证明追平是升主路径干的）" \
          "$oapp" "0"
  fi

  # 兜底放行会让上面那条 applied 断言失去意义（"可用性优先"直接放行上报，
  # 追平未完成也登记）。必须确认本轮走的不是那条路。
  relax=$(docker exec "$CONTAINER" bash -c \
    "grep -c '按可用性优先放行上报' /work/pg-cluster-data/worker*.log 2>/dev/null | awk -F: '{s+=\$2} END {print s+0}'")
  check "本轮未触发 deadline 兜底放行（否则 applied 断言不成立）" "$relax" "0"
fi

echo "========== [5] 收尾：拉起原 leader =========="
PGCTL "$leader_dir" -l "/work/pg-cluster-data/${leader_dir}.log" -w -t 60 start >/dev/null 2>&1
for t in $(seq 1 30); do
  [[ "$(PSQL $PA -Atc 'SELECT 1' 2>/dev/null)" == "1" ]] && break; sleep 2
done
check "原 leader 已恢复" "$(PSQL $PA -Atc 'SELECT 1' 2>/dev/null)" "1"

echo ""
health_check_no_crash
# 丢提案时的表现正是"全 PASS + 有丢弃 = 运气"（见 lib_node_health.sh 头注释）——
# 本用例全靠 Raft 把记录/标记送到 follower，必须一并核查。
health_check_no_drops
health_check_worker_pool
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "TX3 升主合流：全部通过"; else echo "TX3 升主合流：存在 FAIL"; fi

if [[ "${KEEP_FIXTURE:-0}" != "1" ]]; then
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SELECT partdist.replay_disable('${TBL}');" >/dev/null 2>&1
  done
  for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" >/dev/null 2>&1; done
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL};" >/dev/null 2>&1
  done
  PSQL $COORD -q -c "DROP TABLE IF EXISTS tx3_promo;" >/dev/null 2>&1
  for p in $COORD $PA $f1 $f2; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${GID};" >/dev/null 2>&1
  done
fi

exit $FAIL
