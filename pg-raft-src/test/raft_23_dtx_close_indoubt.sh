#!/usr/bin/env bash
# raft_23: DTX-2PC 升主 in-doubt 闭合（§9.6 机制先行）+ 混合写集告警（§9.4）
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_23_dtx_close_indoubt.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# ── 夹具说明（诚实边界）────────────────────────────────────────────
# dtx_close_indoubt 是**机制先行**：它按事务粒度扫 parwal 流、全网求决议、
# 补闭合标记，不依赖惰性回放；升主序列落地时在追平之后调它。本用例手工构造
# "有 DTX_PREPARE、无闭合"的流（这正是崩溃/中止在参与者留下的形态），
# 验证四条求决议路径与"找不到就不动"的纪律。
#
# ── 判据 ────────────────────────────────────────────────────────────
#   A. 有登记（coord_gsid 已知）⇒ 走 dtx_status 权威通道：查无决议时
#      **推定中止先落库**，闭合为 DTX_ABORT，且协调组索引里有 verdict=2。
#   B. 无登记、决议在**另一个组**（本节点不是其成员）⇒ 广播 peers 命中，
#      闭合为 DTX_COMMIT。
#   C. citus 形态的 dtxid、无登记、无决议，master 的 pg_dist_transaction 有
#      前缀行且发起者已死 ⇒ 按 Citus 规则闭合为 COMMIT。
#   D. 什么都查不到 ⇒ **保持 in-doubt 不动**（返回 0，重复调用幂等），
#      绝不无凭据地推定中止——决议可能在此刻不可达的协调组里。
#   E. 混合写集告警：同一事务写了纳管分片 + 非纳管分片形名表 ⇒
#      PREPARE 时 WARNING（§9.4 残留边界的约束层表态）。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }

TBL=raft23_dtx
P_GID=""; C_GID=""
P_PORT=$((BASE_PORT + 1))            # P 组 primary = worker1(node2)
C_PORT=$((BASE_PORT + 2))            # C 组 primary = worker2(node3)
P_MEMBERS="2,3,4"
C_MEMBERS="3,4,5"                    # 不含 node2：P 的 leader 上没有 C 的决议索引
DTX_A=930001; DTX_B=930002; DTX_D=930004

cleanup() {
  local port
  for port in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
    q "$port" "SELECT partdist.pg_raft_group_reset();" >/dev/null
    q "$port" "DELETE FROM partdist.dtx_participant WHERE dtxid IN (${DTX_A},${DTX_B},${DTX_D});" >/dev/null
    q "$port" "DELETE FROM partdist.dtx_decision WHERE dtxid IN (${DTX_A},${DTX_B},${DTX_D});" >/dev/null
    for g in $P_GID $C_GID; do
      [[ -n "$g" ]] && q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${g}::oid;
                                  SET citus.enable_ddl_propagation=off;
                                  DROP TABLE IF EXISTS ${TBL}_${g};" >/dev/null
    done
    q "$port" "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS mix23_99001;" >/dev/null
  done
  q "$BASE_PORT" "DELETE FROM pg_dist_transaction WHERE gid ~ '^citus_0_[0-9]+_93900[0-9]_0\$';" >/dev/null
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TBL};" >/dev/null
}
fail() { cleanup; echo "raft_23 FAIL: $1"; exit 1; }

cleanup

# ── 夹具：两个组——P（in-doubt 所在）与 C（决议所在，成员不含 P 的 leader）──
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "SET citus.enable_ddl_propagation=on;
   SET citus.shard_count = 16;
   SET citus.shard_replication_factor = 1;
   CREATE TABLE ${TBL}(id int primary key, v text);
   SELECT create_distributed_table('${TBL}', 'id');" >/dev/null 2>&1 \
  || fail "建分布式表失败"

pick_gid() {  # $1=期望 primary 的端口
  q "$BASE_PORT" \
    "SELECT p.shardid FROM pg_dist_placement p
       JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
       JOIN pg_dist_shard s ON s.shardid=p.shardid
      WHERE s.logicalrelid='${TBL}'::regclass AND n.nodeport=$1
      ORDER BY p.shardid LIMIT 1;"
}
P_GID=$(pick_gid "$P_PORT"); C_GID=$(pick_gid "$C_PORT")
[[ -n "$P_GID" && -n "$C_GID" ]] || fail "夹具：取不到两个分片（P='${P_GID}' C='${C_GID}'）"

setup_group() {  # $1=gid $2=primary_port $3=primary_node $4=members(csv)
  local gid=$1 pport=$2 pnode=$3 members=$4 n port
  for n in ${members//,/ }; do
    port=$(( BASE_PORT + n - 1 ))
    [[ "$port" == "$pport" ]] || \
      q "$port" "SET citus.enable_ddl_propagation=off;
                 CREATE TABLE IF NOT EXISTS ${TBL}_${gid} (LIKE ${TBL} INCLUDING ALL);" >/dev/null
    q "$port" "SELECT partdist.rebuild_shard_identity();" >/dev/null
  done
  for n in 1 ${members//,/ }; do
    port=$(( BASE_PORT + n - 1 ))
    q "$port" "INSERT INTO partdist.partition_map(partition_id, primary_node, secondary_nodes, primary_term)
               VALUES (${gid}::oid, ${pnode}, ARRAY[${members}]::int[], 1)
               ON CONFLICT (partition_id) DO UPDATE
                 SET primary_node=EXCLUDED.primary_node,
                     secondary_nodes=EXCLUDED.secondary_nodes;" >/dev/null
  done
  q "$pport" "SELECT partdist.pg_raft_group_create(${gid}, ARRAY[${members}]::int[]);" >/dev/null
  for _ in $(seq 1 40); do
    [[ "$(q "$pport" "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid};")" == leader ]] && return 0
    sleep 1
  done
  return 1
}
setup_group "$P_GID" "$P_PORT" 2 "$P_MEMBERS" || fail "夹具：P 组 ${P_GID} 没选出 leader"
setup_group "$C_GID" "$C_PORT" 3 "$C_MEMBERS" || fail "夹具：C 组 ${C_GID} 没选出 leader"
# partition_map 指向真实 leader（求决议按它寻址）
for port in $(seq "$BASE_PORT" $((BASE_PORT + 4))); do
  q "$port" "UPDATE partdist.partition_map SET primary_node=2 WHERE partition_id=${P_GID}::oid;
             UPDATE partdist.partition_map SET primary_node=3 WHERE partition_id=${C_GID}::oid;" >/dev/null
done

P_OID=$(q "$P_PORT" "SELECT partdist.local_partition_for_shard(${P_GID});")
[[ -n "$P_OID" && "$P_OID" != "0" ]] || fail "P 组 leader 上解析不到本地 OID"

kinds_for() {  # $1=dtxid → 该 dtxid 在 P 流里的 kind 序列
  q "$P_PORT" "SELECT string_agg(d.kind::text, ',' ORDER BY g)
                 FROM generate_series(1, partdist.get_partition_flush_lsn(${P_OID}::oid)) g,
                      LATERAL partdist.partwal_read_dtx_record(${P_OID}::oid, g) d
                WHERE d.dtxid = $1;"
}

# ── A. 有登记 ⇒ dtx_status 权威通道（含推定中止落库）──
q "$P_PORT" "SELECT partdist.partwal_append_dtx_record(${P_OID}::oid, 1, ${DTX_A}, 0);" >/dev/null
q "$P_PORT" "INSERT INTO partdist.dtx_participant(dtxid, gid, gsids, coord_gsid)
             VALUES (${DTX_A}, 'raft23_a', ARRAY[${P_GID}]::bigint[], ${P_GID});" >/dev/null
N=$(q "$P_PORT" "SELECT partdist.dtx_close_indoubt(${P_OID}::oid);")
[[ "$N" == "1" ]] || fail "A: 应闭合恰好 1 笔，实际 '${N}'"
# 协调组就是 P 自己 ⇒ dtx_status 的推定中止把 ABORT **决议记录**（kind 2）写进
# P 自己的流（§5.3：协调组的决议兼任自身闭合），随后 close 再补 kind 4 标记。
[[ "$(kinds_for ${DTX_A})" == "1,2,4" ]] \
  || fail "A: 查无决议 ⇒ 推定中止，流应为 1,2,4（PREPARE→ABORT决议→ABORT标记），实际 '$(kinds_for ${DTX_A})'"
[[ "$(q "$P_PORT" "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX_A};")" == "2" ]] \
  || fail "A: 推定中止必须先把 ABORT 决议写进协调组（索引可查）"
echo "raft_23 A: 有登记 ⇒ dtx_status 权威通道，推定中止先落库再闭合为 ABORT ✓"

# ── B. 无登记、决议在别的组 ⇒ 广播 peers 命中 ──
q "$P_PORT" "SELECT partdist.partwal_append_dtx_record(${P_OID}::oid, 1, ${DTX_B}, 0);" >/dev/null
B_DEC=$(q "$C_PORT" "SELECT partdist.dtx_decide(${C_GID}, ${DTX_B}, 1, ARRAY[${P_GID},${C_GID}]::bigint[]);")
[[ "$B_DEC" == "1" ]] || fail "B: 在 C 组预置 COMMIT 决议失败（'${B_DEC}'）"
[[ -z "$(q "$P_PORT" "SELECT 1 FROM partdist.dtx_decision WHERE dtxid=${DTX_B};")" ]] \
  || fail "B: 夹具前提被破坏——P 的 leader 不该有 ${DTX_B} 的本地决议索引（C 组成员不含它）"
N=$(q "$P_PORT" "SELECT partdist.dtx_close_indoubt(${P_OID}::oid);")
[[ "$N" == "1" ]] || fail "B: 应闭合恰好 1 笔，实际 '${N}'"
[[ "$(kinds_for ${DTX_B})" == "1,3" ]] \
  || fail "B: 广播命中 COMMIT 决议，流应为 1,3，实际 '$(kinds_for ${DTX_B})'"
echo "raft_23 B: 无登记 ⇒ 广播 peers 的决议索引命中，闭合为 COMMIT ✓"

# ── C. citus 形态 dtxid ⇒ 按 Citus 规则（前缀行 + 发起者已死）──
DEAD_PID=$(q "$BASE_PORT" "SELECT coalesce(max(pid),1)+100000 FROM pg_stat_activity;")
DTX_CITUS=$(q "$BASE_PORT" "SELECT (${DEAD_PID}::bigint << 33) | 939001;")
[[ -n "$DTX_CITUS" ]] || fail "C: 算不出 citus 形态 dtxid"
q "$P_PORT" "SELECT partdist.partwal_append_dtx_record(${P_OID}::oid, 1, ${DTX_CITUS}, 0);" >/dev/null
q "$BASE_PORT" "INSERT INTO pg_dist_transaction (groupid, gid)
                VALUES (0, 'citus_0_${DEAD_PID}_939001_0');" >/dev/null
N=$(q "$P_PORT" "SELECT partdist.dtx_close_indoubt(${P_OID}::oid);")
[[ "$N" == "1" ]] || fail "C: 应闭合恰好 1 笔，实际 '${N}'"
[[ "$(kinds_for ${DTX_CITUS})" == "1,3" ]] \
  || fail "C: master 有提交记录 ⇒ COMMIT，流应为 1,3，实际 '$(kinds_for ${DTX_CITUS})'"
q "$BASE_PORT" "DELETE FROM pg_dist_transaction WHERE gid='citus_0_${DEAD_PID}_939001_0';" >/dev/null
echo "raft_23 C: citus 形态 dtxid ⇒ 按 Citus 规则（发起者已死 + 前缀行在）闭合为 COMMIT ✓"

# ── D. 什么都查不到 ⇒ 保持 in-doubt，不许无凭据推定中止 ──
q "$P_PORT" "SELECT partdist.partwal_append_dtx_record(${P_OID}::oid, 1, ${DTX_D}, 0);" >/dev/null
N=$(q "$P_PORT" "SELECT partdist.dtx_close_indoubt(${P_OID}::oid);")
[[ "$N" == "0" ]] || fail "D: 无任何决议线索时不得闭合，实际闭合 '${N}' 笔"
[[ "$(kinds_for ${DTX_D})" == "1" ]] \
  || fail "D: 流应保持只有 PREPARE(1)，实际 '$(kinds_for ${DTX_D})'"
N2=$(q "$P_PORT" "SELECT partdist.dtx_close_indoubt(${P_OID}::oid);")
[[ "$N2" == "0" ]] || fail "D: 重复调用应幂等（0），实际 '${N2}'"
echo "raft_23 D: 决议查不到 ⇒ 保持 in-doubt 不动，重复调用幂等 ✓"

# ── E. 混合写集告警（§9.4 残留边界的约束层表态）──
q "$P_PORT" "SET citus.enable_ddl_propagation=off;
             CREATE TABLE IF NOT EXISTS mix23_99001 (id int);" >/dev/null
E_OUT=$(psql_at "$P_PORT" -c \
  "BEGIN;
   INSERT INTO ${TBL}_${P_GID} VALUES (2001, 'mix');
   INSERT INTO mix23_99001 VALUES (1);
   PREPARE TRANSACTION 'citus_0_${DEAD_PID}_939002_0';" 2>&1)
q "$P_PORT" "ROLLBACK PREPARED 'citus_0_${DEAD_PID}_939002_0';" >/dev/null
echo "$E_OUT" | grep -q "混合" \
  || fail "E: 纳管 + 非纳管混合写集的 PREPARE 应发 WARNING，实际输出：$(echo "$E_OUT" | head -2 | tr '\n' ' ')"
echo "raft_23 E: 混合写集在 PREPARE 时收到告警（非纳管部分不受决议保护）✓"

cleanup
echo "raft_23 PASS"
exit 0
