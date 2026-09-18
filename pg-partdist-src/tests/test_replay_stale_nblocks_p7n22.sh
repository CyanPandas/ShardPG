#!/usr/bin/env bash
# [宿主机] P7-N22 回归：回放 worker 的块数缓存在「副本 → 主 → 副本」之后陈旧。
#
# 机理（2026-09-18，扩展验收 [9] 第 2 轮 FAIL 定因之一）：
#   回放 worker 进程级 InRecovery=true，smgrnblocks()/DropRelationBuffers() 因此信任本进程的
#   smgr_cached_nblocks。节点当副本时 worker 缓存了"当时"的块数；随后它当主，扩文件的是普通
#   backend（只扩、不发 smgr inval），worker 的缓存停在旧值（偏小）；再降回副本后：
#     · 全量基线截断：DropRelationBuffers 按旧块数只丢 [0, 旧值) 的 buffer，当主时写出的尾部
#       buffer 仍 valid 留在共享缓冲区，FPI 重建一扩就撞
#       `unexpected data beyond EOF in block N`，每 250 ms 重试一次永远失败；
#     · 主权交接（一个字节都不截）后的普通 redo：块号 >= 旧值 ⇒ 被当成"文件外的页"，
#       RBM_NORMAL 下 log_invalid_page 后**静默跳过**，或扩文件时撞同一个 EOF 报错。
#   实测：组 102751 在 :5435 升主前置卡满 60 s 走兜底，同一 BGW 串行排队的组 102747 被拖到丢主。
#
# 修法：ReplayForgetCachedSizes() —— 认领（加载 locmap）、FILESET_UPDATE 换表（含交接）、
#   截断前，都把本进程对这些关系的块数缓存置 InvalidBlockNumber，下一次 smgrnblocks 取真值。
#
# 本用例（单分片原生表 rn22，3 成员组；A=placement 原始主，B=被测节点，C=另一副本）：
#   [1] A 为主，B/C 供副本、B 追平（让 B 的 worker 缓存下"小"块数）
#   [2] 受控切到 B（登记），经协调者灌一批行 —— B 的 backend 把堆/索引扩出多块
#   [3] 受控切到 C（登记）—— B 降为副本，走主权交接，不截断
#       （不切回 A：A 是原始主、没有回放槽位，被降级后收了 B 的流却无从回放，按 R-P4-15 设计
#        永久不可升主（-1 主动让位），须先重供基线才能重新参选 —— 那是另一件事，不在本用例）
#   [4] 经 C 再写一批，B 追平：必须追到 C 的 flush、日志无 EOF 报错、堆与索引掩码外逐字节一致
#   [5] 从 C 给 B 重供全量基线（截断 + FPI 重建）：同上三条
# 负向对照：撤掉 ReplayForgetCachedSizes 的三处调用后本用例 [4]/[5] 必红（修复验收时实测一次）。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
# 行数够让堆扩出十来页、主键索引分裂（~367 键/叶）即可：2 vCPU 上每条记录一次同步复制往返
# （P7-N7，实测 ~2.5 条/s），6000 行要 40 分钟，没有必要。
ROWS1="${ROWS1:-40}"; ROWS2="${ROWS2:-500}"; ROWS3="${ROWS3:-200}"
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
Q()    { PSQL "$1" -Atc "$2" </dev/null 2>/dev/null | tail -1; }
check() {
  if [[ -z "$2" ]]; then echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置 =========="
WORKERS=($(PSQL $COORD -Atc "SELECT nodeport FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive ORDER BY nodeport" </dev/null))
NW=${#WORKERS[@]}
check "至少 3 个 worker" "$([[ $NW -ge 3 ]] && echo ok)" "ok"
declare -A NID DDIR LOGF LOGMARK
for p in "${WORKERS[@]}"; do
  NID[$p]=$(Q $p "SHOW pg_raft.node_id"); DDIR[$p]=$(Q $p "SHOW data_directory")
  LOGF[$p]=$(DEX bash -c "ls -t '${DDIR[$p]}/pg.log' '${DDIR[$p]}.log' 2>/dev/null | head -1" </dev/null)
  LOGMARK[$p]=$(DEX bash -c "wc -l < '${LOGF[$p]}'" </dev/null)
done
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
ng=0; for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ng=$((ng+${n:-0})); done
check "净场：无残留数据组" "$ng" "0"
SID=""; A=""; B=""; C=""
cleanup() {
  local p r
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  # 先表后组；每条 DROP 套 statement_timeout（P7-N21）。副本残壳被路由守卫拒时，拆组后再删一遍。
  [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn22_${SID}'::regclass)" >/dev/null 2>&1; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn22" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn22_${SID}" >/dev/null 2>&1; done
  done
  echo "  [复原] 组已拆、表已删"
}
trap cleanup EXIT
build_group_on() {
  local gid=$1 want=$2 mem=$3 t p st=""
  for p in "${WORKERS[@]}"; do if [[ "$p" == "$want" ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null; else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 9000" >/dev/null; fi; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.pg_raft_group_create($gid, $mem)" >/dev/null; done
  for t in $(seq 1 40); do st=$(Q $want "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid"); [[ "$st" == leader ]] && break; sleep 1; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  echo "$st"
}
# 受控切主，只动这一个组（pg_raft_group_campaign）
switch_to() { local gid=$1 target=$2 t st=""
  for t in $(seq 1 120); do
    st=$(Q $target "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid"); [[ "$st" == leader ]] && break
    (( t % 5 == 1 )) && Q $target "SELECT partdist.pg_raft_group_campaign($gid)" >/dev/null
    sleep 1
  done; echo "$st"; }
registered() { local gid=$1 target=$2 t
  for t in $(seq 1 90); do [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$gid")" == "${NID[$target]}" ]] && { echo ok; return; }; sleep 2; done; echo "no"; }
lo_of() { Q $1 "SELECT partdist.local_partition_for_shard($SID)"; }
flush_of() { Q $1 "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($SID))"; }
# 让副本 rp 回放追到主 lp 的 flush；回显最终 applied（取不到回显空串）
catch_up() { local lp=$1 rp=$2 tp lo a="" t
  tp=$(flush_of $lp); lo=$(lo_of $rp)
  for t in $(seq 1 90); do
    a=$(Q $rp "SELECT applied FROM partdist.replay_status() WHERE shard=$lo")
    [[ -n "$a" && -n "$tp" && "$a" -ge "$tp" ]] && break
    Q $rp "SELECT partdist.replay_catchup('rn22_${SID}'::regclass, $tp)" >/dev/null 2>&1; sleep 1
  done
  echo "${a}/${tp}"
}
eof_errors() { local p=$1; DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'unexpected data beyond EOF'" </dev/null; }
# 主 lp 与副本 rp 的某关系（主堆或主键索引）掩码外逐字节比对
rel_cmp() { local lp=$1 rp=$2 kind=$3 sql lpath rpath
  if [[ $kind == heap ]]; then sql="SELECT pg_relation_filepath('rn22_${SID}')"
  else sql="SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid='rn22_${SID}'::regclass AND indisprimary"; fi
  PSQL $lp -q -c "CHECKPOINT;" </dev/null >/dev/null 2>&1; PSQL $rp -q -c "CHECKPOINT;" </dev/null >/dev/null 2>&1; sleep 1
  lpath=$(Q $lp "SET citus.override_table_visibility=false; $sql")
  rpath=$(Q $rp "SET citus.enable_ddl_propagation=off; $sql")
  [[ -z "$lpath" || -z "$rpath" ]] && { echo ""; return; }
  DEX python3 /tmp/pagecmp.py --kind=$kind "${DDIR[$lp]}/$lpath" "${DDIR[$rp]}/$rpath" </dev/null 2>/dev/null | tail -1
}
verify_replica() { local tag=$1 lp=$2 rp=$3 cu a tp
  cu=$(catch_up $lp $rp); a=${cu%/*}; tp=${cu#*/}
  check "$tag：:$rp 回放追到 :$lp 的 flush（$cu）" "$([[ -n "$a" && -n "$tp" && "$a" -ge "$tp" ]] && echo ok)" "ok"
  check "$tag：:$rp 日志无 unexpected data beyond EOF" "$(eof_errors $rp)" "0"
  check "$tag：主堆掩码外逐字节一致" "$(rel_cmp $lp $rp heap)" "IDENTICAL_OUTSIDE_HOLE"
  check "$tag：主键索引掩码外逐字节一致" "$(rel_cmp $lp $rp btree)" "IDENTICAL_OUTSIDE_HOLE"
}

echo "========== [1] 夹具：rn22 单分片、3 成员组；B 当副本并追平（缓存下小块数）=========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn22;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE rn22(id int primary key, pad text);
SELECT create_distributed_table('rn22', 'id', colocate_with => 'none');
ALTER TABLE rn22 SET (autovacuum_enabled = off);
SQL
read SID A < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn22'::regclass" </dev/null | tr '|' ' ')
for p in "${WORKERS[@]}"; do [[ $p != $A ]] && { if [[ -z "$B" ]]; then B=$p; elif [[ -z "$C" ]]; then C=$p; fi; }; done
echo "  分片 $SID：原始主 A=:$A，被测 B=:$B，另一副本 C=:$C"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 A=:$A" "$(build_group_on $SID $A "$ALLMEM")" "leader"
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for p in "${WORKERS[@]}"; do [[ $p == $A ]] && continue
  r=$(PSQL $A -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:100}")" "ok"; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "登记 A 为主" "$(registered $SID $A)" "ok"
Q $COORD "INSERT INTO rn22 SELECT g, repeat('a', 120) FROM generate_series(1, $ROWS1) g" >/dev/null
check "写入 $ROWS1 行" "$(Q $COORD "SELECT count(*) FROM rn22")" "$ROWS1"
verify_replica "[1] B 初次追平" $A $B

echo "========== [2] 受控切到 B，经 B 灌 $ROWS2 行（B 的 backend 把文件扩出很多块）=========="
check "B 当选组 $SID" "$(switch_to $SID $B)" "leader"
check "登记 B 为主" "$(registered $SID $B)" "ok"
Q $COORD "INSERT INTO rn22 SELECT g, repeat('b', 120) FROM generate_series($((ROWS1+1)), $((ROWS1+ROWS2))) g" >/dev/null
check "经 B 写入后总行数" "$(Q $COORD "SELECT count(*) FROM rn22")" "$((ROWS1+ROWS2))"
nb=$(Q $B "SET citus.override_table_visibility=false; SELECT pg_relation_size('rn22_${SID}')/8192")
check "B 当主期堆已扩到多块（${nb:-?} 块 > 1）" "$([[ -n "$nb" && "$nb" -gt 1 ]] && echo ok)" "ok"

echo "========== [3] 受控切到 C（B 降为副本，走主权交接，不截断）=========="
check "C 当选组 $SID" "$(switch_to $SID $C)" "leader"
check "登记 C 为主" "$(registered $SID $C)" "ok"

echo "========== [4] 经 C 再写 $ROWS3 行，B 按交接后的流追平 =========="
Q $COORD "INSERT INTO rn22 SELECT g, repeat('c', 120) FROM generate_series($((ROWS1+ROWS2+1)), $((ROWS1+ROWS2+ROWS3))) g" >/dev/null
check "经 C 写入后总行数" "$(Q $COORD "SELECT count(*) FROM rn22")" "$((ROWS1+ROWS2+ROWS3))"
verify_replica "[4] 交接后" $C $B

echo "========== [5] 从 C 给 B 重供全量基线（截断 + FPI 重建）=========="
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
r=$(PSQL $C -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$B]})" </dev/null 2>&1 | tr '\n' ' ')
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "从 C 重供全量基线到 B" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:100}")" "ok"
verify_replica "[5] 重供基线后" $C $B
check "[5] B 基线已收尾（baseline_pending=f）" "$(Q $B "SELECT partdist.shard_baseline_pending($(lo_of $B)::oid)")" "f"

echo "========== [6] 健康 =========="
health_check_no_crash

echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N22 回归：通过" || echo "P7-N22 回归：存在 FAIL"
exit $(( FAIL > 0 ))
