#!/usr/bin/env bash
# [宿主机] T7.23（P7-R2）Raft 组数上限可配 + 整表建组/供副本自动化 验收。
#
# 在此之前：
#   · `RAFT_MAX_GROUPS = 32` 写死在共享内存布局里 —— 每节点至多 31 个数据组，
#     一张 32 分片的分布表就能吃满一个节点的组表，再建组只剩一条 WARNING；
#   · 给一张表的全部分片建组并供副本没有入口：每个分片要人手走"placement 建组 →
#     等当选 → 副本入组 → leader 上 provision_shard_replica"，32 分片就是上百条命令，
#     而这几步的顺序是踩出来的（顺序错了选举会把 placement 迁到没数据的节点上）。
#
# 覆盖：
#   [1] pg_raft.max_groups 默认 32，是 PGC_POSTMASTER（运行期改不动）
#   [2] ★ 一个 worker 调到 40 并重启 ⇒ 能建出 32 个以上的组；默认节点在第 32 个处被拒
#   [3] ★ raft_replicate_table_shards：4 分片表一条命令建组 + 供副本，逐分片 status=ok
#   [4] ★ 供出来的副本真的可用：回放 armed、能追平到 leader 位点
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
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

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

BIGNODE=5440
BIGDIR=/work/pg-cluster-data/worker8
GROUP_BASE=880000
MADE_GROUPS=""
cleanup() {
  local g p
  for g in $MADE_GROUPS; do
    PSQL "${g%%:*}" -q -c "SELECT partdist.pg_raft_group_drop(${g##*:})" </dev/null >/dev/null 2>&1 || true
  done
  PSQL "$BIGNODE" -q -c "ALTER SYSTEM RESET pg_raft.max_groups" </dev/null >/dev/null 2>&1 || true
  DEX /work/pg-install/bin/pg_ctl -D "$BIGDIR" -m fast -w -t 60 restart -l "$BIGDIR/pg.log" </dev/null >/dev/null 2>&1 || true
  PSQL $COORD -q -c "DROP TABLE IF EXISTS p7r2_t" </dev/null >/dev/null 2>&1 || true
  echo "  [复原] :$BIGNODE max_groups 已 RESET 并重启；测试组已删"
}
trap cleanup EXIT

echo "========== [1] pg_raft.max_groups 默认值与作用域 =========="
check "默认 32" "$(PSQL 5433 -Atc "SHOW pg_raft.max_groups" </dev/null | tail -1)" "32"
ctx=$(PSQL 5433 -Atc "SELECT context FROM pg_settings WHERE name='pg_raft.max_groups'" </dev/null | tail -1)
check "是 postmaster 级（决定共享内存大小，只能启动时定）" "$ctx" "postmaster"

echo "========== [2] ★ 调大之后真的能建出 32 个以上的组 =========="
PSQL "$BIGNODE" -q -c "ALTER SYSTEM SET pg_raft.max_groups = 40" </dev/null >/dev/null
DEX /work/pg-install/bin/pg_ctl -D "$BIGDIR" -m fast -w -t 60 restart -l "$BIGDIR/pg.log" </dev/null >/dev/null 2>&1
for t in $(seq 1 30); do [[ "$(PSQL $BIGNODE -Atc 'SELECT 1' </dev/null 2>/dev/null | tail -1)" == "1" ]] && break; sleep 1; done
check ":$BIGNODE 重启后 max_groups=40" "$(PSQL $BIGNODE -Atc "SHOW pg_raft.max_groups" </dev/null | tail -1)" "40"

self=$(PSQL $BIGNODE -Atc "SHOW pg_raft.node_id" </dev/null | tail -1)
have=$(PSQL $BIGNODE -Atc "SELECT count(*) FROM partdist.pg_raft_group_status()" </dev/null | tail -1)
want=$(( 36 - have ))           # 让组总数到 36（> 32）
# 成员里放一个不存在的节点 9999：两成员凑不够多数派 ⇒ 这些组永远是候选人，
# **不会当选、不会向 group 0 上报**，不往 partition_map 里写假分片（非分片组
# 当选后会走 pg_raft_report_data_leader，污染控制面登记）。
ok_n=0
for i in $(seq 1 $want); do
  g=$((GROUP_BASE + i))
  r=$(PSQL $BIGNODE -Atc "SELECT partdist.pg_raft_group_create($g, ARRAY[$self, 9999])" </dev/null 2>&1 | tail -1)
  MADE_GROUPS+="$BIGNODE:$g "
  [[ "$r" == "t" ]] && ok_n=$((ok_n + 1))
done
total=$(PSQL $BIGNODE -Atc "SELECT count(*) FROM partdist.pg_raft_group_status()" </dev/null | tail -1)
check "★ max_groups=40 的节点上组总数到了 ${total}（> 32）" \
      "$([[ -n "$total" && "$total" -gt 32 ]] && echo ok)" "ok"

# 对照：默认 32 的节点，建到第 32 个之后必须被拒（证明上面不是"本来就没上限"）
ctl=5439
self2=$(PSQL $ctl -Atc "SHOW pg_raft.node_id" </dev/null | tail -1)
have2=$(PSQL $ctl -Atc "SELECT count(*) FROM partdist.pg_raft_group_status()" </dev/null | tail -1)
rej=""
for i in $(seq 1 $(( 34 - have2 ))); do
  g=$((GROUP_BASE + 100 + i))
  r=$(PSQL $ctl -Atc "SELECT partdist.pg_raft_group_create($g, ARRAY[$self2, 9999])" </dev/null 2>&1)
  MADE_GROUPS+="$ctl:$g "
  if grep -q "已达上限" <<<"$r"; then rej=ok; break; fi
done
check "对照：默认节点在组表满时被拒（已达上限 32）" "$rej" "ok"
total2=$(PSQL $ctl -Atc "SELECT count(*) FROM partdist.pg_raft_group_status()" </dev/null | tail -1)
check "对照：默认节点组总数封顶 32" "$total2" "32"
for g in $MADE_GROUPS; do
  PSQL "${g%%:*}" -q -c "SELECT partdist.pg_raft_group_drop(${g##*:})" </dev/null >/dev/null 2>&1 || true
done
MADE_GROUPS=""

echo "========== [3] ★ raft_replicate_table_shards：一条命令建组 + 供副本 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
DROP TABLE IF EXISTS p7r2_t;
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 1;
CREATE TABLE p7r2_t(id int primary key, v text);
SELECT create_distributed_table('p7r2_t', 'id');
ALTER TABLE p7r2_t SET (autovacuum_enabled = off);
INSERT INTO p7r2_t SELECT g, 'v'||g FROM generate_series(1, 200) g;
SQL
fn=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_proc WHERE proname='raft_replicate_table_shards'" </dev/null | tail -1)
check "编排函数在协调者上在位" "$fn" "1"
res=$(PSQL $COORD -Atc "SELECT shardid||'|'||leader_port||'|'||array_to_string(replica_ports,',')||'|'||status FROM partdist.raft_replicate_table_shards('p7r2_t', 1)" </dev/null 2>&1)
echo "$res" | sed 's/^/    /'
nshard=$(grep -cE '^[0-9]+\|' <<<"$res")
nok=$(grep -cE '\|ok$' <<<"$res")
check "★ 4 个分片都返回了状态行" "$nshard" "4"
check "★ 4 个分片全部 status=ok" "$nok" "4"

echo "========== [4] ★ 供出来的副本真的可用 =========="
navail=0
while IFS='|' read -r sid lport rport status; do
  [[ "$sid" =~ ^[0-9]+$ && "$status" == "ok" ]] || continue
  loid=$(PSQL "$lport" -Atc "SELECT partdist.local_partition_for_shard($sid)" </dev/null | tail -1)
  lp=$(PSQL "$lport" -Atc "SELECT partdist.get_partition_flush_lsn($loid)" </dev/null | tail -1)
  roid=$(PSQL "$rport" -Atc "SELECT partdist.local_partition_for_shard($sid)" </dev/null | tail -1)
  armed=$(PSQL "$rport" -Atc "SELECT armed FROM partdist.replay_status() WHERE shard=$roid" </dev/null | tail -1)
  app=$(PSQL "$rport" -Atc "SELECT partdist.replay_catchup($roid::regclass, $lp, 120000)" </dev/null 2>&1 | tail -1)
  if [[ "$armed" == "t" && "$app" =~ ^[0-9]+$ && "$app" -ge "$lp" ]]; then
    navail=$((navail + 1))
  else
    echo "    shard $sid 副本 :$rport armed=$armed applied=$app target=$lp"
  fi
done <<<"$res"
check "★ 4 个副本全部 armed 且追平到 leader 位点" "$navail" "4"

echo "========== [5] 节点健康 =========="
health_check_no_crash

echo ""
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "T7.23 组数上限 + 整表供副本：全部通过"; else echo "T7.23 组数上限 + 整表供副本：存在 FAIL"; fi
