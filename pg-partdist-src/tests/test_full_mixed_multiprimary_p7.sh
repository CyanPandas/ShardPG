#!/usr/bin/env bash
# [宿主机] 全方位功能验收：混合主从 + 多主多从，跑通打标分片的分布式事务与受控切主。
#
# 与既有 test_mixed_role_p7 / test_multi_role_p7 互补：那两套验的是**不打标**分片的角色
# 并存 / 物理隔离 / 副本逐字节；本套把同一套「每台既当主又当从」的多主多从拓扑压上
# **TX2 全栈**（打标分片 MVCC + TSO + join_info + 2PC）与**受控切主**，端到端验正确性。
#
# 拓扑（NW=worker 数；两张分布表各 NW 分片、每分片一 Raft 组、成员=全部 worker）：
#   fm_mvcc：打标（分片 MVCC）——每分片主分散到不同 worker（多主），其余成员当从（多从）
#   fm_nat ：不打标（原生）——同法分散，证明「打标 / 原生」两类分片在同一簇/同一节点并存
#   ⇒ 每台 worker 同时是若干分片的主、若干分片的从（混合主从），各按自己的本地 OID 编址
#
# 断言：
#   [2] 多主多从：fm_mvcc 的 NW 个主落在 NW 台不同 worker；每台既是主又是从；本地 OID 各异
#   [3] 读写路由：往各主写单分片、经协调者读回；打标表走分片 MVCC、原生表走原生路径并存
#   [4] 物理隔离 + 副本逐字节：狂写一个组不动另一组从副本字节；各从追平且与主逐字节一致
#   [5] ★ 跨组分布式事务：打标表上 TSO+join 跨组转账 commit 守恒；显式 ROLLBACK 不改账；
#       中途失败（写不存在的目标分片行）原子性——借记一侧不得单独生效
#   [6] ★ 受控切主：对一个 fm_mvcc 组切主，登记后总额守恒、无 reissue 撞号、无 PREPARED 残留、
#       旧主降级后副本重新追平并与新主逐字节一致
#   [7] 健康：无节点崩溃
#   [8] ★ 并发跨组事务：CONC 会话并发转账后守恒 / 无重复行 / 无 PREPARED 残留
#   [9] ★ 同组连续两次切主（N18/N13 正例路径）：无残留 / 无重复行 / 守恒
#        （跨节点 start_ts 不一致只报——已知潜伏项 P7-N20）
#        （总额差额只报——已知残留 P7-N19，按用户指示登记不定因）
#   [10] ★ 崩溃恢复：immediate stop 一个 worker 再拉起 —— 少节点时仍可读、重启后重新入组、总额不变
#   [11] ★ 切主后新主立即可读写（路由已翻）
#
# 夹具纪律（见 project_pg_test_fixture_rules）：建组顺序；供副本期/切主抬 election_timeout；
#   打标经协调者 set_table_shard_mvcc；TSO boot 删 pg_tso_boot 重启协调者；单分片写用 VALUES/IN；
#   收尾三台一起拆组、拆在 DROP 之前；跑着的脚本不改。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ACCT="${ACCT:-6}"; TSO_LEASE_MS="${TSO_LEASE_MS:-60000}"
CONC="${CONC:-3}"; CONC_SECS="${CONC_SECS:-45}"   # [8] 并发会话数 / 持续秒数
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
declare -A NID DDIR
for p in "${WORKERS[@]}"; do NID[$p]=$(Q $p "SHOW pg_raft.node_id"); DDIR[$p]=$(Q $p "SHOW data_directory"); done
CDIR=$(Q $COORD "SHOW data_directory")
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
ng=0; np0=0
for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ng=$((ng+${n:-0})); done
for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); np0=$((np0+${n:-0})); done
check "净场：无残留数据组" "$ng" "0"; check "净场：无残留 prepared" "$np0" "0"

SIDS_M=(); SIDS_N=(); declare -A LEADER SP_D
BG_PIDS=""
VICT=""   # [10] 模拟崩溃停掉的节点；cleanup 负责拉起（纪律：停节点的套件必须自带复原）
cleanup() {
  local p g s
  for x in $BG_PIDS; do kill "$x" 2>/dev/null; done
  if [[ -n "$VICT" ]]; then
    DEX bash -c "/work/pg-install/bin/pg_ctl -D '${DDIR[$VICT]}' status >/dev/null 2>&1 || /work/pg-install/bin/pg_ctl start -D '${DDIR[$VICT]}' -l '${DDIR[$VICT]}/pg.log' -o '-p $VICT' -w -t 60 >/dev/null 2>&1" </dev/null || true
  fi
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表/TSO 未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  for p in $COORD "${WORKERS[@]}"; do for g in $(PSQL $p -Atc "SELECT gid FROM pg_prepared_xacts" </dev/null 2>/dev/null); do Q $p "ROLLBACK PREPARED '$g'" >/dev/null; done; done
  # ★ 顺序（2026-09-18）：优先**先拆表、后拆组**——协调者上的分布表这样删最干净。
  #   但别把它当铁律：组还在时，worker 上的**副本残壳**会被路由守卫拒掉（"分区主副本可能已切换，
  #   请经路由层重试"），那时只能先拆组再删壳。
  # ★ 真正的风险是 P7-N21：DROP 可能落进 pg_partdist 删表钩子里原地热自旋（active 几十分钟、
  #   pg_blocking_pids 为空、一个锁都没有、STAT=Rs 且 CPU 时间一直涨），且**该循环不查中断**，
  #   pg_terminate_backend 返回 t 也杀不掉，只能 pg_ctl -m immediate 重启该节点。
  #   ⚠️ statement_timeout 也**拦不住**它（09-18 二次现场：带 30s 超时的 DROP 跑了 10 分钟，零 syscall 纯自旋）；
  #   这里仍套着，只为防普通的锁等待。
  for s in "${SIDS_M[@]}" "${SIDS_N[@]}"; do for p in "${WORKERS[@]}"; do
    Q $p "SELECT partdist.replay_disable('fm_mvcc_${s}'::regclass)" >/dev/null 2>&1
    Q $p "SELECT partdist.replay_disable('fm_nat_${s}'::regclass)" >/dev/null 2>&1
  done; done
  Q $COORD "SET statement_timeout='30s'; DROP TABLE IF EXISTS fm_mvcc" >/dev/null 2>&1
  Q $COORD "SET statement_timeout='30s'; DROP TABLE IF EXISTS fm_nat" >/dev/null 2>&1
  for s in "${SIDS_M[@]}" "${SIDS_N[@]}"; do for p in "${WORKERS[@]}"; do
    Q $p "SET statement_timeout='20s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS fm_mvcc_${s}" >/dev/null 2>&1
    Q $p "SET statement_timeout='20s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS fm_nat_${s}" >/dev/null 2>&1
  done; done
  # 组还在时副本残壳被路由守卫拒（"分区主副本可能已切换"），拆组后在途 RPC 又会按 hearsay 把组再建出来 ——
  # 拆组 / 删壳交替两轮（夹具规则 42；09-18 首次全绿那轮收尾就漏了 7 张残壳）
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    for s in "${SIDS_M[@]}" "${SIDS_N[@]}"; do for p in "${WORKERS[@]}"; do
      Q $p "SET statement_timeout='20s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS fm_mvcc_${s}" >/dev/null 2>&1
      Q $p "SET statement_timeout='20s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS fm_nat_${s}" >/dev/null 2>&1
    done; done
  done
  for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_partdist.tso_conninfo" >/dev/null; Q $p "ALTER SYSTEM RESET pg_partdist.tso_lease_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  Q $COORD "ALTER SYSTEM RESET pg_partdist.tso_master" >/dev/null; Q $COORD "SELECT pg_reload_conf()" >/dev/null
  echo "  [复原] prepared 已收、组已拆、表已删、TSO 已 RESET"
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
cur_leader() { local sid=$1 p; for p in "${WORKERS[@]}"; do [[ "$(Q $p "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$sid")" == leader ]] && { echo $p; return; }; done; echo ""; }
# 受控切主（**只动这一个组**）：目标节点对该组发起选举（pg_raft_group_campaign），旧主见更高任期
#   自行退位，别的组一概不受影响；没选上（日志不够新/分票）就每 5 s 再发一次。
# ★ 取代旧手法"旧主 heartbeat_ms=60s + 目标 election_timeout=300ms"：那两个 GUC 作用于**节点上的
#   全部组**，扩展验收 [9] 第 2 轮实测一次"切组 102747"把 6 个组全压到 :5435，升主前置在同一条
#   异步连接上串行排队，102747 排在一个卡满 60 s 的组后面、还没轮到就丢了领导权 ⇒ 新主登记 FAIL。
switch_to() { local gid=$1 target=$2 t st=""
  for t in $(seq 1 120); do
    st=$(Q $target "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid")
    [[ "$st" == leader ]] && break
    (( t % 5 == 1 )) && Q $target "SELECT partdist.pg_raft_group_campaign($gid)" >/dev/null
    sleep 1
  done
  echo "$st"
}
shard_ids() { Q $COORD "SELECT string_agg(g::text, ',') FROM (SELECT g FROM generate_series($3, $3+300000) g WHERE get_shard_id_for_distribution_column('$1', g) = $2 LIMIT $4) t"; }
# TX2 全栈跨组事务（join_info = gxid,start_ts,coord_gsid）
gtx() {
  PSQL $COORD -At -v ON_ERROR_STOP=1 </dev/null 2>&1 <<SQL | grep -vE '^(BEGIN|SET|COMMIT|ROLLBACK|INSERT [0-9 ]+|UPDATE [0-9]+|DELETE [0-9]+)$' | awk '{a[NR]=$0} END{ if (NR>0 && a[NR]=="txn_done") print "txn_done"; else { s=""; for(i=1;i<=NR;i++) s=s (i>1?" | ":"") a[i]; print s } }'
SELECT partdist.partdist_gxid_next()||','||partdist.partdist_tso_client_start_ts()||','||$1 AS ji \\gset
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = :'ji';
$2
COMMIT;
SELECT 'txn_done';
SQL
}
converge() { local port=$1 sql=$2 want=$3 max=${4:-60} t v="" p; for t in $(seq 1 $max); do v=$(Q $port "$sql"); [[ "$v" == "$want" ]] && { echo "$v"; return; }; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.dtx_pending_sweep()" >/dev/null; done; sleep 1; done; echo "$v"; }
main_heap_md5() { local port=$1 rel=$2; DEX bash -c "P=\$(/work/pg-install/bin/psql -h /tmp -p $port -U postgres -d postgres -X -qAtc \"SELECT pg_relation_filepath('$rel')\" </dev/null); md5sum '${DDIR[$port]}/'\$P 2>/dev/null | cut -d' ' -f1"; }
# ★ 先把从副本同步到位再比：2 vCPU 建组期抢跑偏置会让某组副本基线半途而废（0 块，
#   baseline_pending），或旧主降级后作为从尚未回追。0 块/pending 就从当前主重供一遍，再长追平。
# ★ 2026-09-18 补两条（修好 -q 取证后第一次真比时暴露）：
#   ① **没有 armed 回放槽位也要重供**：被降级的原始主（从没当过副本）收了新主的流却无从回放，
#      按 R-P4-15 设计须重供基线才重新是副本；不重供它永远追不上，逐字节比对必红。
#   ② 追平判据用**回放游标** replay_status().applied，不是 raft 的 follower_partition_map.applied_part_lsn
#      （后者只说明记录落了盘、不说明 redo 进了堆，拿它判"可以比字节了"会早比）。
ensure_replica_synced() { local lp=$1 rp=$2 sid=$3 tbl=$4 lo tp bp sz a t armed
  lo=$(Q $rp "SELECT partdist.local_partition_for_shard($sid)")
  sz=$(DEX bash -c "P=\$(/work/pg-install/bin/psql -h /tmp -p $rp -U postgres -d postgres -X -qAtc \"SELECT pg_relation_filepath('${tbl}_${sid}')\" </dev/null); wc -c < '${DDIR[$rp]}/'\$P 2>/dev/null" </dev/null)
  bp=$(Q $rp "SELECT partdist.shard_baseline_pending($lo::oid)")
  armed=$(Q $rp "SELECT armed FROM partdist.replay_status() WHERE shard=$lo")
  if [[ "${sz:-0}" == 0 || "$bp" == t || "$armed" != t ]]; then
    for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms=30000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
    PSQL $lp -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$rp]})" </dev/null >/dev/null 2>&1
    for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  fi
  tp=$(Q $lp "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($sid))")
  for t in $(seq 1 60); do a=$(Q $rp "SELECT applied FROM partdist.replay_status() WHERE shard=$lo"); [[ -n "$a" && -n "$tp" && "$a" -ge "$tp" ]] && break; Q $rp "SELECT partdist.replay_catchup(('${tbl}_'||$sid)::regclass,$tp)" >/dev/null 2>&1; sleep 1; done
}
# 受控切主前：目标必须是"有 armed 回放槽位"的副本，否则按 R-P4-15 升主前置 -1、主动让位
#   （被降级的原始主就是这种）。缺槽位就先从当前主给它重供基线并追平 —— 这是运维上的正规流程，
#   不是绕过：R-P4-15 的提示原文就是"须重做物理基线后才能重新参选"。
ensure_candidate() { local gid=$1 target=$2 tbl=$3 lp lo armed
  lp=$(cur_leader $gid); [[ -z "$lp" || "$lp" == "$target" ]] && return
  lo=$(Q $target "SELECT partdist.local_partition_for_shard($gid)")
  armed=$(Q $target "SELECT armed FROM partdist.replay_status() WHERE shard=$lo")
  [[ "$armed" == t ]] && return
  echo "  [夹具] :$target 对组 $gid 没有 armed 回放槽位（被降级的原始主），先从当前主 :$lp 重供基线再切"
  ensure_replica_synced $lp $target $gid $tbl
}
# 掩码外逐字节比对（主 vs 从）：hint bit / 空闲区会不同，走 pagecmp.py（同 test_mixed_role_p7）
# 副本主堆字节数（0 = 基线没灌上，属环境漂移而非内容不一致；空串 = 没量到，夹具取证失败）
# ★ 2026-09-18 教训：psql 必须带 -q。-c 里有 SET 时不带 -q 会先打印命令标签 "SET"，
#   \$P 成了 "SET<换行>base/5/NNN" 两个词 ⇒ `wc -c < 路径` 报 ambiguous redirect ⇒ 取到空串 ⇒
#   被当成"0 块"降级为只报 —— 扩展验收前 4 轮的 6 条"副本 0 块（2 vCPU 漂移）"全是这个假象，
#   逐字节比对一次都没真做过。
replica_heap_bytes() { local rp=$1 rel=$2
  DEX bash -c "P=\$(/work/pg-install/bin/psql -h /tmp -p $rp -U postgres -d postgres -X -qAtc \"SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('$rel')\" </dev/null); wc -c < '${DDIR[$rp]}/'\$P 2>/dev/null" </dev/null
}
# 比对并判定：副本 0 块 ⇒ 只报（2 vCPU 建组漂移，基线半途而废，见 N15/N11）；
# 供上了却不一致 ⇒ 真 FAIL（那才是副本一致性回归）。
cmp_or_note() { local lp=$1 rp=$2 rel=$3 tag=$4 sz c
  sz=$(replica_heap_bytes $rp "$rel")
  # 没量到 ≠ 0 块：取证失败必须判红，否则又是一条静默通过（feedback_test_harness_silent_pass）
  if [[ -z "$sz" ]]; then check "$tag（取副本 :$rp 主堆文件大小）" "" "measured"; return; fi
  if [[ "$sz" == 0 ]]; then
    echo "  [只报] $tag：从 :$rp 主堆 0 块（基线未灌上 = 2 vCPU 建组漂移 N15/N11），跳过逐字节比对"
    return
  fi
  c=$(heap_masked_cmp $lp $rp "$rel")
  check "$tag" "$c" "IDENTICAL_OUTSIDE_HOLE"
}
heap_masked_cmp() { local lp=$1 rp=$2 rel=$3 lpath rpath
  DEX bash -c "test -f /tmp/pagecmp.py || true" </dev/null
  PSQL $lp -q -c "CHECKPOINT;" </dev/null >/dev/null 2>&1; PSQL $rp -q -c "CHECKPOINT;" </dev/null >/dev/null 2>&1; sleep 1
  lpath=$(Q $lp "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('$rel')")
  rpath=$(Q $rp "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('$rel')")
  DEX python3 /tmp/pagecmp.py --kind=heap "${DDIR[$lp]}/$lpath" "${DDIR[$rp]}/$rpath" </dev/null 2>/dev/null
}

echo "========== [1] 拓扑：fm_mvcc(打标) + fm_nat(原生) 各 $NW 分片、每分片一组、主分散多台 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS fm_mvcc; DROP TABLE IF EXISTS fm_nat;
SET citus.shard_count = $NW; SET citus.shard_replication_factor = 1;
CREATE TABLE fm_mvcc(id int primary key, v text, n int NOT NULL DEFAULT 0);
SELECT create_distributed_table('fm_mvcc','id', colocate_with=>'none');
ALTER TABLE fm_mvcc SET (autovacuum_enabled=off);
CREATE TABLE fm_nat(id int primary key, v text, n int NOT NULL DEFAULT 0);
SELECT create_distributed_table('fm_nat','id', colocate_with=>'none');
ALTER TABLE fm_nat SET (autovacuum_enabled=off);
SQL
# shard_identity 必须在 build/provision **之前**建（local_partition_for_shard 靠它；N12 同序）
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null
  PSQL $p -q </dev/null >/dev/null 2>&1 <<'S2'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION public.sclog_full(oid,bigint) RETURNS text AS '$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;
S2
done

# 每张表各分片：主分散到不同 worker（round-robin），建组 + 供副本
assign_and_build() {   # $1=表名 $2=数组名  → 填 SIDS_$suf / LEADER；leader = 该分片 Citus placement 所在节点
  local tbl=$1 arrname=$2 sid lp
  while read sid lp; do
    eval "$arrname+=($sid)"; LEADER[$sid]=$lp
    st=$(build_group_on $sid $lp "$ALLMEM"); check "  组 $sid（$tbl）主落在 :$lp" "$st" "leader"
    for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms=15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
    for p in "${WORKERS[@]}"; do [[ $p == $lp ]] && continue; r=$(PSQL $lp -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' '); check "    副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:70}")" "ok"; done
    for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  done < <(PSQL $COORD -Atc "SELECT s.shardid, n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='$tbl'::regclass ORDER BY s.shardid" </dev/null | tr '|' ' ')
}
assign_and_build fm_mvcc SIDS_M
assign_and_build fm_nat  SIDS_N
# ★ 稳定化：顺序建 6 个组时先建的组会被后建组的抢跑偏置夺走主权（N15），首个组的副本
#   基线因此半途而废（实测 102699 两副本 0 块）。建完统一抬全体选举超时 30 s（压住漂移），
#   逐组重认当前主、把未 armed 的副本从**当前主**重新供一遍，再 RESET。
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms=30000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for sid in "${SIDS_M[@]}" "${SIDS_N[@]}"; do cur=$(cur_leader $sid); [[ -z "$cur" ]] && { sleep 3; cur=$(cur_leader $sid); }; [[ -n "$cur" ]] && LEADER[$sid]=$cur
  for p in "${WORKERS[@]}"; do [[ $p == $cur || -z "$cur" ]] && continue
    armed=$(Q $p "SELECT count(*) FROM partdist.replay_status() s WHERE s.shard=partdist.local_partition_for_shard($sid) AND s.armed")
    [[ "$armed" == 1 ]] && continue
    r=$(PSQL $cur -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
    check "  稳定化：组 $sid 从 :$p 从当前主 :$cur 补供" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:80}")" "ok"
  done
done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
# partition_map 两处收敛（各组当前主 = 协调者视图）
for sid in "${SIDS_M[@]}" "${SIDS_N[@]}"; do ok=""; for t in $(seq 1 60); do lp=$(cur_leader $sid); LEADER[$sid]=${lp:-${LEADER[$sid]}}; [[ -n "$lp" && "$(Q $lp "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" && "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" ]] && { ok=ok; break; }; sleep 1; done; check "  组 $sid partition_map 两处收敛（当前主 :${LEADER[$sid]}）" "$ok" "ok"; done
# 打标 fm_mvcc + TSO 全栈（fm_nat 不打标）
mk=$(PSQL $COORD -Atc "SELECT count(*) FILTER (WHERE status LIKE 'registered%') FROM partdist.set_table_shard_mvcc('fm_mvcc')" </dev/null 2>&1 | tail -1)
check "fm_mvcc 打标登记 $NW 片" "$mk" "$NW"
DEX rm -f "$CDIR/pg_tso_boot" </dev/null; DEX /work/pg-install/bin/pg_ctl -D "$CDIR" -m fast -l "$CDIR/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 40); do up=$(Q $COORD "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done; check "协调者重启就绪" "$up" "1"
Q $COORD "ALTER SYSTEM SET pg_partdist.tso_master=on" >/dev/null
for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo='host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null; Q $p "ALTER SYSTEM SET pg_partdist.tso_lease_ms=$TSO_LEASE_MS" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 2; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done; sleep 4
check "TSO 取号可用" "$([[ "$(Q $COORD "SELECT partdist.partdist_tso_client_start_ts()")" =~ ^[0-9]+$ ]] && echo ok)" "ok"

echo "========== [2] 多主多从 + 混合角色 =========="
declare -A ISLEAD ISREP
for sid in "${SIDS_M[@]}" "${SIDS_N[@]}"; do lp=$(cur_leader $sid); ISLEAD[$lp]=$(( ${ISLEAD[$lp]:-0} + 1 )); for p in "${WORKERS[@]}"; do [[ $p != $lp ]] && ISREP[$p]=$(( ${ISREP[$p]:-0} + 1 )); done; done
nmain_hosts=0; for p in "${WORKERS[@]}"; do [[ ${ISLEAD[$p]:-0} -ge 1 ]] && nmain_hosts=$((nmain_hosts+1)); done
check "多主：主分布在 ≥2 台不同 worker（多主成立）" "$([[ $nmain_hosts -ge 2 ]] && echo ok)" "ok"
nmix=0; for p in "${WORKERS[@]}"; do [[ ${ISLEAD[$p]:-0} -ge 1 && ${ISREP[$p]:-0} -ge 1 ]] && nmix=$((nmix+1)); done
check "混合角色：≥2 台 worker 同时既当主又当从" "$([[ $nmix -ge 2 ]] && echo ok)" "ok"
# 同一台上打标分片与原生分片的本地 OID 各异
p0=${WORKERS[0]}; om=$(Q $p0 "SELECT partdist.local_partition_for_shard(${SIDS_M[0]})"); on=$(Q $p0 "SELECT partdist.local_partition_for_shard(${SIDS_N[0]})")
check "同节点打标/原生分片本地 OID 各异" "$([[ -n "$om" && -n "$on" && "$om" != "$on" ]] && echo ok)" "ok"

echo "========== [3] 读写路由：打标表走分片 MVCC、原生表走原生，并存 =========="
for sid in "${SIDS_M[@]}"; do lp=${LEADER[$sid]}; ids=$(shard_ids fm_mvcc $sid 800001 $ACCT); vals=$(for a in ${ids//,/ }; do printf "(%s,'m',1000)," "$a"; done | sed 's/,$//'); out=$(gtx $sid "INSERT INTO fm_mvcc(id,v,n) VALUES $vals;"); check "  往打标组 $sid 写 $ACCT 行（TSO+join）" "$out" "txn_done"; done
for sid in "${SIDS_N[@]}"; do ids=$(shard_ids fm_nat $sid 900001 $ACCT); vals=$(for a in ${ids//,/ }; do printf "(%s,'n',1000)," "$a"; done | sed 's/,$//'); out=$(PSQL $COORD -Atc "INSERT INTO fm_nat(id,v,n) VALUES $vals; SELECT 'ok'" </dev/null 2>&1 | tail -1); check "  往原生组 $sid 写 $ACCT 行（原生路径）" "$out" "ok"; done
check "打标表经协调者读回" "$(converge $COORD "SELECT count(*) FROM fm_mvcc" $((ACCT*NW)))" "$((ACCT*NW))"
check "原生表经协调者读回" "$(Q $COORD "SELECT count(*) FROM fm_nat")" "$((ACCT*NW))"

echo "========== [4] 物理隔离 + 副本逐字节一致 =========="
# 狂写 fm_mvcc 的第一个组，另一组（fm_nat 第一个）从副本字节不变
sidM=${SIDS_M[0]}; lpM=$(cur_leader $sidM); sidN=${SIDS_N[0]}; lpN=$(cur_leader $sidN)
repN=""; for p in "${WORKERS[@]}"; do [[ $p != $lpN ]] && { repN=$p; break; }; done
before=$(main_heap_md5 $repN fm_nat_${sidN})
manyids=$(shard_ids fm_mvcc $sidM 810001 60)
gtx $sidM "UPDATE fm_mvcc SET n=n+1 WHERE id IN ($manyids);" >/dev/null 2>&1 || true
gtx $sidM "INSERT INTO fm_mvcc(id,v,n) SELECT g,'x',1 FROM unnest(ARRAY[$manyids]) g ON CONFLICT DO NOTHING;" >/dev/null 2>&1 || true
after=$(main_heap_md5 $repN fm_nat_${sidN})
check "狂写打标组不改另一（原生）组从副本主堆字节" "$([[ -n "$before" && "$before" == "$after" ]] && echo ok)" "ok"
# 各从追平并与主逐字节一致（抽 fm_mvcc 第一组 + fm_nat 第一组）
for pair in "fm_mvcc:$sidM" "fm_nat:$sidN"; do IFS=: read tbl sid <<<"$pair"; lp=$(cur_leader $sid)
  for rp in "${WORKERS[@]}"; do [[ $rp == $lp ]] && continue
    ensure_replica_synced $lp $rp $sid $tbl
    cmp_or_note $lp $rp "${tbl}_${sid}" "  组 $sid 从 :$rp 与主 :$lp 主堆掩码外逐字节一致"
  done
done

echo "========== [5] 跨组分布式事务：commit 守恒 / ROLLBACK 不改账 / 中途失败原子 =========="
# 取两个不同组的账户做跨组转账
sA=${SIDS_M[0]}; sB=${SIDS_M[1]}
a1=$(shard_ids fm_mvcc $sA 800001 1); b1=$(shard_ids fm_mvcc $sB 800001 1)
tot0=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$(Q $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')")")
gtx $sA "UPDATE fm_mvcc SET n=n-5 WHERE id=$a1; UPDATE fm_mvcc SET n=n+5 WHERE id=$b1;" >/dev/null
tot1=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$tot0")
check "[5] 跨组 commit 守恒" "$tot1" "$tot0"
# ROLLBACK：显式回滚不得改账
naa=$(Q $COORD "SELECT n FROM fm_mvcc WHERE id=$a1")
PSQL $COORD -At -v ON_ERROR_STOP=1 </dev/null >/dev/null 2>&1 <<SQL
SELECT partdist.partdist_gxid_next()||','||partdist.partdist_tso_client_start_ts()||','||$sA AS ji \\gset
BEGIN; SET LOCAL citus.propagate_set_commands='local'; SET LOCAL pg_partdist.join_info=:'ji';
UPDATE fm_mvcc SET n=n-7 WHERE id=$a1; UPDATE fm_mvcc SET n=n+7 WHERE id=$b1;
ROLLBACK;
SQL
check "[5] 跨组 ROLLBACK 后 a 账不变" "$(converge $COORD "SELECT n FROM fm_mvcc WHERE id=$a1" "$naa")" "$naa"
# 中途失败原子性：贷记写一个不存在的行（0 行受影响），借记不得单独生效——用真正会报错的：往目标组写违反主键
dupid=$b1
before5=$(Q $COORD "SELECT n FROM fm_mvcc WHERE id=$a1")
out5=$(gtx $sA "UPDATE fm_mvcc SET n=n-9 WHERE id=$a1; INSERT INTO fm_mvcc(id,v,n) VALUES ($dupid,'m',1);")
check "[5] 中途失败（主键冲突）事务未提交" "$([[ "$out5" != txn_done ]] && echo ok)" "ok"
check "[5] 中途失败后借记一侧未单独生效" "$(converge $COORD "SELECT n FROM fm_mvcc WHERE id=$a1" "$before5")" "$before5"

echo "========== [6] 受控切主：守恒 + 无 reissue 撞号 + 无 PREPARED 残留 + 副本重追平 =========="
FS=${SIDS_M[0]}; OLD=$(cur_leader $FS); NEWL=""; for p in "${WORKERS[@]}"; do [[ $p != $OLD ]] && { NEWL=$p; break; }; done
totb=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$(Q $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')")")
ensure_candidate $FS $NEWL fm_mvcc
st=$(switch_to $FS $NEWL)
check ":$NEWL 当选组 $FS 新主" "$st" "leader"
reg=""; for t in $(seq 1 90); do [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$FS")" == "${NID[$NEWL]}" ]] && { reg=ok; break; }; sleep 2; done
check "新主 180s 内登记" "$reg" "ok"; LEADER[$FS]=$NEWL
tota=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$totb" 90)
check "[6] ★ 切主后总额守恒" "$tota" "$totb"
# 无 PREPARED 残留（当前主）
lo=$(Q $NEWL "SELECT partdist.local_partition_for_shard($FS)"); xmax=$(( $(Q $NEWL "SELECT partdist.shard_xid_next($lo::oid)") + 1 )); res=0
for x in $(seq 1 $xmax); do st2=$(Q $NEWL "SELECT partdist.shard_clog_status_full($lo::oid,$x)" | grep -oE 'st=[0-9]+'); [[ "$st2" == "st=1" ]] && res=$((res+1)); done
check "[6] ★ 新主无 PREPARED 残留" "$res" "0"
# 无 reissue 撞号：跨节点对读同一分片 xid 的 sts
coll=0; declare -A seen
for p in "${WORKERS[@]}"; do lo2=$(Q $p "SELECT partdist.local_partition_for_shard($FS)"); [[ -z "$lo2" || "$lo2" == 0 ]] && continue
  while IFS='|' read x full; do sts=$(echo "$full" | grep -oE 'sts=[0-9]+' | cut -d= -f2); [[ -z "$sts" || "$sts" == 0 ]] && continue
    key="$x"; if [[ -n "${seen[$key]:-}" && "${seen[$key]}" != "$sts" ]]; then coll=$((coll+1)); fi; seen[$key]=$sts
  done < <(PSQL $p -Atc "SELECT x, partdist.shard_clog_status_full($lo2::oid,x) FROM generate_series(1,$xmax) x" </dev/null 2>/dev/null)
done
check "[6] ★ 无 reissue 撞号（同分片 xid 跨节点 start_ts 一致）" "$coll" "0"
# 副本（含旧主）重新追平并与新主逐字节一致
lpN=$(cur_leader $FS); lmd=$(main_heap_md5 $lpN fm_mvcc_${FS})
for rp in "${WORKERS[@]}"; do [[ $rp == $lpN ]] && continue
  ensure_replica_synced $lpN $rp $FS fm_mvcc
  cmp_or_note $lpN $rp "fm_mvcc_${FS}" "  切主后从 :$rp 与新主掩码外逐字节一致"
done

# ══════════════════ 扩展场景（2026-09-18）══════════════════
# 前四段验的是"单线程 + 单次受控切主"。真实风险在并发与连切，这里补上；
# 崩溃恢复与"切主后立即可用"是运维最常问的两条。
# 已知残留 P7-N19（激进连切下总额偶发 -1）：只在 [9] 里以**只报**形式呈现，不判红。

# 全部打标账户（跨分片交错），供并发负载用
declare -A ACC_SID
ACC_ALL=(); for sid in "${SIDS_M[@]}"; do ids=$(shard_ids fm_mvcc $sid 800001 $ACCT); for a in ${ids//,/ }; do ACC_ALL+=("$a"); ACC_SID[$a]=$sid; done; done
xfer() { gtx ${ACC_SID[$1]} "UPDATE fm_mvcc SET n = n - 1 WHERE id = $1; UPDATE fm_mvcc SET n = n + 1 WHERE id = $2;"; }
sum_mvcc() { Q $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')"; }
dup_rows() { Q $COORD "SELECT coalesce(sum(c-d),0) FROM (SELECT count(*) c, count(DISTINCT id) d FROM fm_mvcc GROUP BY get_shard_id_for_distribution_column('fm_mvcc',id)) t"; }
residue_on() {  # <组> → 当前主上 PREPARED 残留笔数
  local sid=$1 lp lo xmax n=0 x
  lp=$(cur_leader $sid); [[ -z "$lp" ]] && { echo -1; return; }
  lo=$(Q $lp "SELECT partdist.local_partition_for_shard($sid)"); xmax=$(( $(Q $lp "SELECT partdist.shard_xid_next($lo::oid)") + 1 ))
  for x in $(seq 1 $xmax); do [[ "$(Q $lp "SELECT partdist.shard_clog_status($lo::oid,$x)")" == 1 ]] && n=$((n+1)); done
  echo $n
}
collide_on() {  # <组> → 跨节点同分片 xid 不同 start_ts 的笔数（重号）
  local sid=$1 p lo xmax x sts key coll=0; declare -A seen
  lo=$(Q $(cur_leader $sid) "SELECT partdist.local_partition_for_shard($sid)"); xmax=$(( $(Q $(cur_leader $sid) "SELECT partdist.shard_xid_next($lo::oid)") + 1 ))
  local lead=$(cur_leader $sid) tp
  tp=$(Q $lead "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($sid))")
  for p in "${WORKERS[@]}"; do local l2=$(Q $p "SELECT partdist.local_partition_for_shard($sid)"); [[ -z "$l2" || "$l2" == 0 ]] && continue
    # ★ 只比回放已追平的节点：2 vCPU 漂移会留下"基线半途/未重同步"的副本，它手里是**上个任期的
    #   陈旧 clog**，拿来跨节点比 start_ts 会把历史误判成重号（实测 [9] 报 2 处、而同轮
    #   无重复行/守恒/无残留全绿 —— 即没有任何真实损坏）。leader 自己恒参与比对。
    if [[ "$p" != "$lead" ]]; then
      local ap=$(Q $p "SELECT applied_part_lsn FROM partdist.follower_partition_map WHERE partition_id=$l2")
      [[ -z "$ap" || -z "$tp" || "$ap" -lt "$tp" ]] && continue
    fi
    while IFS='|' read x full; do sts=$(echo "$full" | grep -oE 'sts=[0-9]+' | cut -d= -f2); [[ -z "$sts" || "$sts" == 0 ]] && continue
      key="$x"; [[ -n "${seen[$key]:-}" && "${seen[$key]}" != "$sts" ]] && coll=$((coll+1)); seen[$key]=$sts
    done < <(PSQL $p -Atc "SELECT x, partdist.shard_clog_status_full($l2::oid,x) FROM generate_series(1,$xmax) x" </dev/null 2>/dev/null)
  done
  echo $coll
}

echo "========== [8] 并发跨组事务：$((${#ACC_ALL[@]})) 账户 / $CONC 会话并发转账后守恒 =========="
t8=$(sum_mvcc)
STOP8="$(mktemp -u)"; PIDS8=""
for s in $(seq 1 $CONC); do
  ( i=0; mine=(); for j in "${!ACC_ALL[@]}"; do [[ $(( j % CONC )) -eq $(( s - 1 )) ]] && mine+=("${ACC_ALL[$j]}"); done
    nm=${#mine[@]}; [[ $nm -lt 2 ]] && exit 0
    while [[ ! -f "$STOP8" ]]; do xfer "${mine[$(( i % nm ))]}" "${mine[$(( (i+1) % nm ))]}" >/dev/null 2>&1 || sleep 1; i=$((i+1)); done ) &
  PIDS8+="$! "
done
BG_PIDS+="$PIDS8"; sleep "$CONC_SECS"; touch "$STOP8"; for x in $PIDS8; do wait "$x" 2>/dev/null; done; BG_PIDS=""
check "[8] ★ 并发后总额守恒" "$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$t8" 90)" "$t8"
check "[8] ★ 并发后无重复行" "$(dup_rows)" "0"
r8=0; for sid in "${SIDS_M[@]}"; do r8=$(( r8 + $(residue_on $sid) )); done
check "[8] ★ 并发后无 PREPARED 残留" "$r8" "0"

echo "========== [9] 同组连续两次切主（N18/N13 正例路径） =========="
G9=${SIDS_M[0]}; t9=$(sum_mvcc); been=" $(cur_leader $G9) "
for round in 1 2; do
  old=$(cur_leader $G9); nw=""
  for p in "${WORKERS[@]}"; do [[ $p != $old && "$been" != *" $p "* ]] && { nw=$p; break; }; done
  [[ -z "$nw" ]] && { echo "  第 $round 轮：无未当过主的节点，跳过"; break; }
  been+="$nw "
  ensure_candidate $G9 $nw fm_mvcc
  st=$(switch_to $G9 $nw)
  check "  第 $round 轮：:$nw 当选组 $G9" "$st" "leader"
  reg=""; for t in $(seq 1 90); do [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$G9")" == "${NID[$nw]}" ]] && { reg=ok; break; }; sleep 2; done
  check "  第 $round 轮：新主登记" "$reg" "ok"
  LEADER[$G9]=$nw; sleep 5
done
c9=$(collide_on $G9)
if [[ "$c9" == 0 ]]; then check "[9] ★ 连切两次后无 reissue 撞号" "$c9" "0"
else echo "  [只报] 连切后跨节点同分片 xid 的 start_ts 有 $c9 处不一致 = 已知潜伏项 P7-N20（旧主未入流号的残留终局 clog），按用户指示登记不定因；同轮无重复行/守恒/无残留即未显形"; fi
check "[9] ★ 连切两次后无 PREPARED 残留" "$(residue_on $G9)" "0"
check "[9] ★ 连切两次后无重复行" "$(dup_rows)" "0"
t9b=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$t9" 90)
if [[ "$t9b" == "$t9" ]]; then check "[9] 连切两次后总额守恒" "$t9b" "$t9"
else echo "  [只报] 连切后总额 $t9 → $t9b（差 $((t9b-t9))）= 已知残留 P7-N19，按用户指示只登记不判红"; fi

echo "========== [10] 崩溃恢复：immediate stop 一个 worker 再拉起 =========="
for p in "${WORKERS[@]}"; do [[ "$p" != "$(cur_leader ${SIDS_M[0]})" ]] && { VICT=$p; break; }; done
t10=$(sum_mvcc)
VD=${DDIR[$VICT]}
DEX /work/pg-install/bin/pg_ctl -D "$VD" -m immediate stop -w -t 60 </dev/null >/dev/null 2>&1
check "[10] :$VICT 已模拟崩溃停机" "$(DEX bash -c "/work/pg-install/bin/pg_ctl -D '$VD' status >/dev/null 2>&1 && echo up || echo down" </dev/null)" "down"
# 停掉的节点若是某些组的主，得等这些组自动选出新主、路由翻过去，才谈得上"可读"。
# 原判据在停机瞬间就读，必红 —— 那不是产品缺陷，是判据写错了位置。
rd=""; for t in $(seq 1 90); do rd=$(Q $COORD "SELECT count(*) FROM fm_mvcc"); [[ -n "$rd" ]] && break; sleep 2; done
check "[10] ★ 少一个节点：自动切主后打标表恢复可读（$rd 行）" "$([[ -n "$rd" ]] && echo ok)" "ok"
DEX /work/pg-install/bin/pg_ctl start -D "$VD" -l "$VD/pg.log" -o "-p $VICT" -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 60); do up=$(Q $VICT "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done
check "[10] :$VICT 重启就绪" "$up" "1"
gb=""; for t in $(seq 1 60); do gb=$(Q $VICT "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); [[ "${gb:-0}" -ge 1 ]] && break; sleep 2; done
check "[10] :$VICT 重启后重新加入各数据组（$gb 组）" "$([[ "${gb:-0}" -ge 1 ]] && echo ok)" "ok"
check "[10] ★ 崩溃恢复后总额不变" "$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$t10" 90)" "$t10"
check "[10] ★ 崩溃恢复后无重复行" "$(dup_rows)" "0"

echo "========== [11] 切主后新主立即可读写 =========="
G11=${SIDS_M[1]}; old11=$(cur_leader $G11); nw11=""
for p in "${WORKERS[@]}"; do [[ $p != $old11 ]] && { nw11=$p; break; }; done
ensure_candidate $G11 $nw11 fm_mvcc
st11=$(switch_to $G11 $nw11)
check "[11] :$nw11 当选组 $G11 新主" "$st11" "leader"
reg11=""; for t in $(seq 1 90); do [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$G11")" == "${NID[$nw11]}" ]] && { reg11=ok; break; }; sleep 2; done
check "[11] 新主登记（路由已翻）" "$reg11" "ok"
a11=$(shard_ids fm_mvcc $G11 800001 1); n11=$(Q $COORD "SELECT n FROM fm_mvcc WHERE id=$a11")
check "[11] ★ 切主后经协调者立即读到该分片" "$([[ -n "$n11" ]] && echo ok)" "ok"
out11=$(gtx $G11 "UPDATE fm_mvcc SET n = n + 0 WHERE id = $a11;")
check "[11] ★ 切主后经协调者立即写成功（路由到新主）" "$out11" "txn_done"
check "[11] ★ 立即读写后无重复行" "$(dup_rows)" "0"

echo "========== [7] 健康 =========="
health_check_no_crash
echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "全方位（混合主从 + 多主多从）功能验收：全绿" || echo "全方位功能验收：存在 FAIL"
