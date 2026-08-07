#!/usr/bin/env bash
# raft_22: DTX-2PC 端到端 —— 真实跨分区事务经内核补丁 0004 的挂点走完三阶段
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_22_dtx_end_to_end.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# 依据：DTX_2PC_DESIGN.md §3.3（正常路径）、§3.4（快路径）、§5.3（DECISION
#       兼作协调组的 COMMIT 标记）、§8.3（只读参与者剔除）、§9.3（补丁 0004）。
#
# ── 判据 ────────────────────────────────────────────────────────────
#   A. 前置：本次构建的 postgres 必须导出 pre_record_commit_hook，
#      且 citus.recover_2pc_interval = -1（§9.4，不关掉会分叉提交）。
#   B. 跨分区事务提交后：协调组日志里有且仅有**一条** DECISION 记录，
#      verdict=COMMIT、participants 恰为真实写集，且该记录**在协调组的每个
#      成员上都在** —— 返回给客户端的 COMMIT 因此有多数派保证。
#      协调组的选取必须等于 participants_sorted[dtxid % n]（各节点独立可算）。
#   C. 三阶段在 parwal 流里逐条可见：
#        参与组   DATA... → DTX_PREPARE(kind=1) → DTX_COMMIT(kind=3)
#        协调组   DATA... → DTX_PREPARE(kind=1) → DTX_DECISION(kind=2)
#                 且**没有**单独的 DTX_COMMIT —— DECISION 兼任（§5.3）。
#      PREPARE 标记必须携带本分区的本地 top-level xid（升主时"这笔 in-doubt
#      事务属于哪个全局事务"的唯一线索），且必须排在 DATA 之后。
#   D. 快路径（§3.4）：单分区事务**不产生任何决议**，也不写 PREPARE/COMMIT
#      标记 —— 开销与接线前持平。
#   E. 只读参与者剔除（§8.3）：命中 0 行的分片不进 participants。
#      构造：UPDATE ... WHERE 条件只命中一个分片的行，但语句广播到两个分片。
#   F. 协调组**失去多数派** ⇒ 跨分区事务必须提交失败且无行可见（不允许部分提交）。
#      注意构造：只停协调组的 leader 是**不够**的——组内还剩 2/3 会自治选出新
#      leader 并上报改写路由，决议照样做得出来、事务**应该**成功（§2.1 好处 2）。
#      必须停两个成员让 1/3 永远凑不齐多数派，判据才是确定性的。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
DEX() { docker exec -u postgres "$CONTAINER" "$@"; }
psql_at() { DEX /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }

TBL=raft22_dtx
GIDS=(); PORTS=(); MEMBERS=()

cleanup() {
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TBL};" >/dev/null
  local i p
  for ((i = 0; i < ${#GIDS[@]}; i++)); do
    for p in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
      q "$p" "SELECT partdist.pg_raft_group_drop(${GIDS[$i]});" >/dev/null
      q "$p" "DELETE FROM partdist.partition_map WHERE partition_id = ${GIDS[$i]}::oid;" >/dev/null
      q "$p" "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}_${GIDS[$i]};" >/dev/null
    done
  done
  for p in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
    # F 段留下的 in-doubt 事务：正常路径由恢复守护收尾，用例自己也清一遍，
    # 免得残留的 prepared 事务持锁影响后续用例。
    for g in $(q "$p" "SELECT gid FROM pg_prepared_xacts WHERE strpos(gid,'citus_')=1;"); do
      q "$p" "ROLLBACK PREPARED '${g}';" >/dev/null
    done
    q "$p" "DELETE FROM partdist.dtx_participant;" >/dev/null
    # 清掉前序用例遗留的数据组（控制面 group 0 不受影响）。
    # 必要性：遗留组的分片表可能已被 DROP，它在 group_tick 里解析不到本地分区，
    # 会拖累同一次 tick 里后续组的选举 —— 实测表现就是本用例的两个新组
    # 40s 选不出 leader（2026-08-04）。raft_16 的夹具同样先做这一步。
    q "$p" "SELECT partdist.pg_raft_group_reset();" >/dev/null
  done
}
fail() { cleanup; echo "raft_22 FAIL: $1"; exit 1; }

cleanup

# ── A. 前置 ────────────────────────────────────────────────────────
HOOK=$(DEX bash -c 'nm -D /work/pg-install/bin/postgres 2>/dev/null | grep -c pre_record_commit_hook')
[[ "$HOOK" == "1" ]] \
  || fail "A: 当前 postgres 二进制没有 pre_record_commit_hook（内核补丁 0004 未落地，跨分区事务不会走 2PC）"
for p in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
  RI=$(q "$p" "SHOW citus.recover_2pc_interval;")
  [[ "$RI" == "-1" ]] \
    || fail "A: 端口 ${p} 的 citus.recover_2pc_interval='${RI}'，必须为 -1（§9.4：不关会分叉提交）"
done
echo "raft_22 A: 补丁 0004 已生效 + Citus 2PC 恢复已关闭 ✓"

# ── 夹具：2 个分片，各自一个 3 成员数据组 ──────────────────────────
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "SET citus.enable_ddl_propagation=on;
   SET citus.shard_count = 8;
   SET citus.shard_replication_factor = 1;
   CREATE TABLE ${TBL}(id int primary key, v text);
   SELECT create_distributed_table('${TBL}', 'id');" >/dev/null 2>&1 \
  || fail "建分布式表失败"

# 取两个**落在不同节点**的分片，并各自选一个确定会路由过去的 id。
# 不假设 Citus 的 placement 轮转顺序（shard_count=2 时两个分片完全可能同节点，
# 那样根本构造不出跨节点事务 —— 2026-08-04 实测踩到）。
IDS=()
while read -r line; do
  [[ -z "$line" ]] && continue
  GIDS+=("$(echo "$line" | cut -d' ' -f1)")
  PORTS+=("$(echo "$line" | cut -d' ' -f2)")
  IDS+=("$(echo "$line" | cut -d' ' -f3)")
done < <(q "$BASE_PORT" \
  "SELECT DISTINCT ON (n.nodeport) s.shardid||' '||n.nodeport||' '||m.id
     FROM generate_series(1, 400) m(id)
     JOIN pg_dist_shard s
       ON s.shardid = get_shard_id_for_distribution_column('${TBL}', m.id)
     JOIN pg_dist_placement p ON p.shardid = s.shardid
     JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
    WHERE s.logicalrelid = '${TBL}'::regclass
    ORDER BY n.nodeport, m.id
    LIMIT 2;")
[[ ${#GIDS[@]} -eq 2 ]] || fail "夹具：没能找到两个分属不同节点的分片（实际 ${#GIDS[@]}）"
[[ "${PORTS[0]}" != "${PORTS[1]}" ]] \
  || fail "夹具：两个分片落在同一节点（${PORTS[0]}），构造不出跨节点事务"

# 每个分片再取一个备用 id（F 段要用同样的两个组再跑一笔事务）
ALT=()
for i in 0 1; do
  ALT+=("$(q "$BASE_PORT" \
    "SELECT min(m.id) FROM generate_series(1, 400) m(id)
      WHERE get_shard_id_for_distribution_column('${TBL}', m.id) = ${GIDS[$i]}
        AND m.id <> ${IDS[$i]};")")
  [[ -n "${ALT[$i]}" ]] || fail "夹具：分片 ${GIDS[$i]} 找不到第二个 id"
done

PID0=$(( PORTS[0] - BASE_PORT + 1 )); PID1=$(( PORTS[1] - BASE_PORT + 1 ))
# 两组的成员集**互不相交**：否则两组可能选出同一个 leader，
# 而 leader 上报会改写 pg_dist_placement，把两个分片挪到同一节点。
POOL=()
for n in 2 3 4 5 6 7 8 9; do
  [[ $n -eq $PID0 || $n -eq $PID1 ]] && continue
  POOL+=("$n")
done
[[ ${#POOL[@]} -ge 4 ]] || fail "夹具：可用副本节点不足（需要 4 个，实际 ${#POOL[@]}）"
MEMBERS=("${PID0},${POOL[0]},${POOL[1]}" "${PID1},${POOL[2]},${POOL[3]}")

for i in 0 1; do
  gid=${GIDS[$i]}
  IFS=',' read -r pid m2 m3 <<< "${MEMBERS[$i]}"
  for n in $m2 $m3; do
    q $(( BASE_PORT + n - 1 )) "SET citus.enable_ddl_propagation=off;
      CREATE TABLE IF NOT EXISTS ${TBL}_${gid} (LIKE ${TBL} INCLUDING ALL);" >/dev/null
  done
  for n in $pid $m2 $m3; do
    q $(( BASE_PORT + n - 1 )) "SELECT partdist.rebuild_shard_identity();" >/dev/null
  done
  # master 也要有 partition_map：驱动靠它找协调组现任 leader
  for n in 1 $pid $m2 $m3; do
    q $(( BASE_PORT + n - 1 )) \
      "INSERT INTO partdist.partition_map(partition_id, primary_node, secondary_nodes, primary_term)
       VALUES (${gid}::oid, ${pid}, ARRAY[${m2},${m3}], 1)
       ON CONFLICT (partition_id) DO UPDATE
         SET primary_node = EXCLUDED.primary_node,
             secondary_nodes = EXCLUDED.secondary_nodes;" >/dev/null
  done
  # 先只在 primary 上建组、等它当选，再补建到 follower（raft_16 的既有手法）。
  # 直接三个节点一起建，谁当选是不确定的 —— 当选者上报会把 pg_dist_placement
  # 改到它自己，而它手里只有一张空的副本表，后续断言就全乱了（2026-08-04 实测）。
  q "${PORTS[$i]}" "SELECT partdist.pg_raft_group_create(${gid}, ARRAY[${pid},${m2},${m3}]);" >/dev/null
  ok=0
  for _ in $(seq 1 40); do
    [[ "$(q "${PORTS[$i]}" "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid};")" == leader ]] \
      && { ok=1; break; }
    sleep 1
  done
  [[ $ok -eq 1 ]] || fail "夹具：40s 内组 ${gid} 的 primary（端口 ${PORTS[$i]}）没当选 leader"
  for n in $m2 $m3; do
    q $(( BASE_PORT + n - 1 )) "SELECT partdist.pg_raft_group_create(${gid}, ARRAY[${pid},${m2},${m3}]);" >/dev/null
  done
done

member_ports() {   # $1 = "a,b,c" → 端口列表
  local n; for n in ${1//,/ }; do echo $(( BASE_PORT + n - 1 )); done
}
plsn_of() { q "$1" "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($2));"; }
# 某组 parwal 流里 kind 序列（DATA 记为 0）
kinds_of() {
  q "$1" "SELECT string_agg(coalesce(d.kind::text,'0'), ',' ORDER BY g)
            FROM generate_series($3::bigint + 1, partdist.get_partition_flush_lsn(
                   partdist.local_partition_for_shard($2))) g
            LEFT JOIN LATERAL partdist.partwal_read_dtx_record(
                   partdist.local_partition_for_shard($2), g) d ON true;"
}

BASE0=$(plsn_of "${PORTS[0]}" "${GIDS[0]}")
BASE1=$(plsn_of "${PORTS[1]}" "${GIDS[1]}")

# ── B/C. 跨分区事务 ────────────────────────────────────────────────
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "INSERT INTO ${TBL} VALUES (${IDS[0]},'a'),(${IDS[1]},'b');" >/dev/null 2>&1 \
  || fail "B: 跨分区 INSERT 提交失败"

DEC=""
for _ in $(seq 1 20); do
  DEC=$(q "${PORTS[0]}" \
    "SELECT dtxid||'|'||coord_gsid||'|'||verdict||'|'||participants::text
       FROM partdist.dtx_decision
      WHERE participants @> ARRAY[${GIDS[0]},${GIDS[1]}]::bigint[] ORDER BY decided_at DESC LIMIT 1;")
  [[ -z "$DEC" ]] && DEC=$(q "${PORTS[1]}" \
    "SELECT dtxid||'|'||coord_gsid||'|'||verdict||'|'||participants::text
       FROM partdist.dtx_decision
      WHERE participants @> ARRAY[${GIDS[0]},${GIDS[1]}]::bigint[] ORDER BY decided_at DESC LIMIT 1;")
  [[ -n "$DEC" ]] && break
  sleep 1
done
[[ -n "$DEC" ]] || fail "B: 跨分区事务提交后找不到 DECISION 记录（2PC 没被触发）"

DTXID=${DEC%%|*};        REST=${DEC#*|}
COORD=${REST%%|*};       REST=${REST#*|}
VERDICT=${REST%%|*};     PARTS=${REST#*|}
[[ "$VERDICT" == "1" ]] || fail "B: 决议 verdict 应为 1(COMMIT)，实际 ${VERDICT}"

SORTED=$(printf '%s\n%s\n' "${GIDS[0]}" "${GIDS[1]}" | sort -n | tr '\n' ',' | sed 's/,$//')
[[ "$PARTS" == "{${SORTED}}" ]] \
  || fail "B: participants 应恰为真实写集 {${SORTED}}，实际 ${PARTS}"

# 协调组必须等于 participants_sorted[dtxid % n]（各节点独立可算，无需协商）
IFS=',' read -r S0 S1 <<< "$SORTED"
EXPECT_COORD=$(( DTXID % 2 == 0 ? S0 : S1 ))
[[ "$COORD" == "$EXPECT_COORD" ]] \
  || fail "B: 协调组应为 participants_sorted[${DTXID} %% 2]=${EXPECT_COORD}，实际 ${COORD}"

if [[ "$COORD" == "${GIDS[0]}" ]]; then
  COORD_IDX=0; PART_IDX=1
else
  COORD_IDX=1; PART_IDX=0
fi

# 决议必须在协调组**每个成员**上都在（= 已达多数派持久化）
for p in $(member_ports "${MEMBERS[$COORD_IDX]}"); do
  ok=0
  for _ in $(seq 1 20); do
    n=$(q "$p" "SELECT count(*) FROM partdist.dtx_decision WHERE dtxid=${DTXID} AND verdict=1;")
    [[ "$n" == "1" ]] && { ok=1; break; }
    sleep 1
  done
  [[ $ok -eq 1 ]] || fail "B: 协调组成员 ${p} 上没有 dtxid=${DTXID} 的唯一 COMMIT 决议"
done
echo "raft_22 B: 决议 dtxid=${DTXID} coord=${COORD} participants=${PARTS}，协调组全体成员均有 ✓"

# C. 三阶段记录序列
BASE_COORD=$([[ $COORD_IDX -eq 0 ]] && echo "$BASE0" || echo "$BASE1")
BASE_PART=$([[ $PART_IDX  -eq 0 ]] && echo "$BASE0" || echo "$BASE1")
K_COORD=$(kinds_of "${PORTS[$COORD_IDX]}" "${GIDS[$COORD_IDX]}" "$BASE_COORD")
K_PART=$(kinds_of  "${PORTS[$PART_IDX]}"  "${GIDS[$PART_IDX]}"  "$BASE_PART")

[[ "$K_PART" == *",1,3" ]] \
  || fail "C: 参与组的 kind 序列应以 DATA...,PREPARE(1),COMMIT(3) 收尾，实际 '${K_PART}'"
[[ "$K_COORD" == *",1,2" ]] \
  || fail "C: 协调组的 kind 序列应以 DATA...,PREPARE(1),DECISION(2) 收尾，实际 '${K_COORD}'"
[[ "$K_COORD" != *",3"* ]] \
  || fail "C: 协调组不应另写 DTX_COMMIT 标记（§5.3 DECISION 兼任），实际 '${K_COORD}'"
[[ "${K_PART%%,*}" == "0" && "${K_COORD%%,*}" == "0" ]] \
  || fail "C: 标记必须排在 DATA 之后，实际 参与组='${K_PART}' 协调组='${K_COORD}'"

# PREPARE 标记必须带本地 top-level xid
for i in 0 1; do
  PLSN=$(q "${PORTS[$i]}" \
    "SELECT max(g) FROM generate_series(1, partdist.get_partition_flush_lsn(
              partdist.local_partition_for_shard(${GIDS[$i]}))) g,
          LATERAL partdist.partwal_read_dtx_record(
              partdist.local_partition_for_shard(${GIDS[$i]}), g) d
      WHERE d.kind = 1 AND d.dtxid = ${DTXID};")
  [[ -n "$PLSN" && "$PLSN" != "0" ]] || fail "C: 组 ${GIDS[$i]} 上找不到本事务的 PREPARE 标记"
  # parwal-3.0 起头部是 64 位 gxid（node_id<<48 | 本地 xid），取低 48 位
  PXID=$(q "${PORTS[$i]}" \
    "SELECT gxid & ((1::bigint<<48)-1) FROM partdist.partwal_read_record(
              partdist.local_partition_for_shard(${GIDS[$i]}), ${PLSN});")
  [[ -n "$PXID" && "$PXID" != "0" ]] \
    || fail "C: 组 ${GIDS[$i]} 的 PREPARE 标记没有携带本地 top-level xid（实际 '${PXID}'）"
done
echo "raft_22 C: 三阶段记录序列 参与组='${K_PART}' 协调组='${K_COORD}'，PREPARE 携带本地 xid ✓"

# C2. 标记必须真的复制出去：两组的**每个成员**最终与 leader 的 kind 序列一致。
# 此前只查 leader 侧流——"PREPARE/COMMIT 标记是否真的到了 follower"在端到端
# 层面没人验（raft_19 D 验的是手工 append 路径，不是 prepare 接线路径）。
# 有界重试：标记走三轮复制各自凑多数派，个别 follower 短暂落后靠心跳补齐（§9.7.1）。
for i in 0 1; do
  IFS=',' read -r cm1 cm2 cm3 <<< "${MEMBERS[$i]}"
  BASE_I=$([[ $i -eq 0 ]] && echo "$BASE0" || echo "$BASE1")
  K_LEADER=$(kinds_of "${PORTS[$i]}" "${GIDS[$i]}" "$BASE_I")
  for n in $cm1 $cm2 $cm3; do
    p=$(( BASE_PORT + n - 1 ))
    [[ "$p" == "${PORTS[$i]}" ]] && continue
    ok=0
    for _ in $(seq 1 30); do
      [[ "$(kinds_of "$p" "${GIDS[$i]}" "$BASE_I")" == "$K_LEADER" ]] && { ok=1; break; }
      sleep 1
    done
    [[ $ok -eq 1 ]] \
      || fail "C2: 组 ${GIDS[$i]} 成员 ${p} 的记录序列 30s 未追平 leader（'$(kinds_of "$p" "${GIDS[$i]}" "$BASE_I")' vs '${K_LEADER}'）"
  done
done
echo "raft_22 C2: 两组全部成员的 DATA/PREPARE/标记序列与 leader 一致（标记真的复制出去了）✓"

# ── D. 快路径：单分区事务不产生决议 ────────────────────────────────
# ★ 不能断言"决议总数不变"：B 段那笔决议会被自动回执→FORGET 回收（§9.7），
# 总数随时间自己变。判据改为"没有出现引用这两个分片、且不是 B 段那笔的新决议"。
new_decisions() {   # $1=port
  q "$1" "SELECT count(*) FROM partdist.dtx_decision
           WHERE dtxid <> ${DTXID}
             AND (participants @> ARRAY[${GIDS[0]}]::bigint[]
               OR participants @> ARRAY[${GIDS[1]}]::bigint[]);"
}
B0=$(plsn_of "${PORTS[0]}" "${GIDS[0]}"); B1=$(plsn_of "${PORTS[1]}" "${GIDS[1]}")
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "UPDATE ${TBL} SET v = 'solo' WHERE id = ${IDS[0]};" >/dev/null 2>&1 \
  || fail "D: 单分区 UPDATE 失败"
sleep 2
D_NEW=$(new_decisions "${PORTS[$COORD_IDX]}")
[[ "$D_NEW" == "0" ]] \
  || fail "D: 单分区事务不应产生决议（快路径），出现 ${D_NEW} 条新决议"
KD0=$(kinds_of "${PORTS[0]}" "${GIDS[0]}" "$B0"); KD1=$(kinds_of "${PORTS[1]}" "${GIDS[1]}" "$B1")
[[ "$KD0" != *"1"* && "$KD1" != *"1"* ]] \
  || fail "D: 单分区事务不应写 DTX 标记，实际 '${KD0}' / '${KD1}'"
echo "raft_22 D: 单分区事务走快路径，不写决议也不写标记 ✓"

# ── E. 只读参与者剔除 ──────────────────────────────────────────────
# 广播型 UPDATE（无分区键）会打到全部 8 个分片、在**每个**节点上都 prepare，
# 但 WHERE 只让其中一个分片真正改到行。若只读参与者没被剔除，写集会 >1 组、
# 于是产生一条决议；剔除生效则写集只剩 1 组，走快路径、不产生决议。
for p in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
  q "$p" "DELETE FROM partdist.dtx_participant;" >/dev/null
done
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "UPDATE ${TBL} SET v = 'ro-elim' WHERE v = 'solo';" >/dev/null 2>&1 \
  || fail "E: 广播 UPDATE 执行失败"
sleep 2
E_NEW=$(new_decisions "${PORTS[$COORD_IDX]}")
[[ "$E_NEW" == "0" ]] \
  || fail "E: 广播 UPDATE 实际只写了 1 个分片，只读参与者应被剔除、不产生决议，出现 ${E_NEW} 条新决议"
RO=0; WR=0
for p in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
  RO=$(( RO + $(q "$p" "SELECT count(*) FROM partdist.dtx_participant WHERE gsids = '{}'::bigint[];") ))
  WR=$(( WR + $(q "$p" "SELECT count(*) FROM partdist.dtx_participant WHERE gsids <> '{}'::bigint[];") ))
done
[[ "$RO" -ge 1 ]] \
  || fail "E: 只读参与者应留下 gsids='{}' 的登记（它仍需拿到 coord 才能在崩溃后自解），实际 ${RO}"
[[ "$WR" == "1" ]] \
  || fail "E: 真正写过的登记应恰有 1 条（只有一个分片被改到），实际 ${WR}"
echo "raft_22 E: 只读参与者不进写集但仍被登记（${RO} 条空写集 / ${WR} 条真写集）✓"

# ── F. 协调组做不出决议 ⇒ 事务中止、无行可见 ───────────────────────
#
# ★ 判据的构造必须是"协调组**失去多数派**"，不能只是"停掉协调组的 leader"。
# 只停 leader 时组内还剩 2/3，会自治选出新 leader 并上报改写
# pg_dist_placement，决议照样做得出来 —— 事务**应该**提交成功，那正是
# §2.1 好处 2（协调权随 Raft 选举自动转移）在起作用。
# 初版就是这么写的，于是这一段在"协调组来得及重选"时必然误报
# （2026-08-04 全量回归里实测到一次）。
#
# ★ 第二个构造坑（2026-08-04 审查改正）：停哪两个成员同样有讲究。
# member_ports 的头一个是协调组自己的 primary —— 它同时是数据分片的主节点，
# 停它会让 INSERT 在**路由/连接**阶段就失败，测的是"主挂了写不进"这个与
# 2PC 毫无关系的性质（接线前也这样）。必须停两个**非主**成员：两个数据主
# 都活着、远程 INSERT 都成功，失败只可能发生在 2PC 路径上 —— 协调组自身
# 分片的 prepare 复制凑不齐多数派（1/3），事务必须中止、不允许部分提交。
# 下面还断言失败原因文本里有"多数派"，钉死失败点。
#
# （"prepare 全成、只饿死 decide"无法从外部确定性构造：prepare 与 decide
# 用同一个 quorum，都在同一条 COMMIT 语句内完成。decide 自身的多数派语义
# 由 raft_20 B 在机制层验收——decide 返回即记录已在协调组全部成员上。）
COORD_MEMBERS=$(member_ports "${MEMBERS[$COORD_IDX]}")
COORD_PRIMARY_PORT=${PORTS[$COORD_IDX]}
STOPPED=()
for p in $COORD_MEMBERS; do
  [[ "$p" == "$COORD_PRIMARY_PORT" ]] && continue   # 数据主必须活着，见上
  d="worker$(( p - BASE_PORT ))"
  DEX /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/${d}" stop -m fast -w -t 30 >/dev/null 2>&1
  STOPPED+=("$d")
done
[[ ${#STOPPED[@]} -eq 2 ]] || fail "F: 没能停掉协调组的两个非主成员"
sleep 3

F_COMMITTED=0
F_ERR=$(psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "INSERT INTO ${TBL} VALUES (${ALT[0]},'dtx-abort'),(${ALT[1]},'dtx-abort');" 2>&1) \
  && F_COMMITTED=1

for d in "${STOPPED[@]}"; do
  DEX /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/${d}" \
      -l "/work/pg-cluster-data/${d}.log" start -w -t 30 >/dev/null 2>&1
done
sleep 3

[[ "$F_COMMITTED" == "0" ]] \
  || fail "F: 协调组失多数派时跨分区事务竟然提交成功了（决议没被要求，等于没走 2PC）"
echo "$F_ERR" | grep -q "多数派" \
  || fail "F: 失败原因应是复制未达多数派（prepare 被拒），实际：$(echo "$F_ERR" | grep -m1 ERROR | head -c 200)"

# 参与者上残留的 prepared 事务由恢复守护收尾，这里只断言"数据没变可见"
ROWS_AFTER=$(q "$BASE_PORT" "SELECT count(*) FROM ${TBL} WHERE v = 'dtx-abort';")
[[ "${ROWS_AFTER:-0}" == "0" ]] \
  || fail "F: 决议做不出来的事务不应有任何行可见，实际 ${ROWS_AFTER} 行"
echo "raft_22 F: 协调组失多数派 ⇒ 事务中止、无行可见（无部分提交）✓"

cleanup
echo "raft_22 PASS"
exit 0
