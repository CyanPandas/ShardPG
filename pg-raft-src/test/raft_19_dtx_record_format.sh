#!/usr/bin/env bash
# raft_19: DTX-2PC 记录格式与 flags 端到端保真
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_19_dtx_record_format.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# 依据：DTX_2PC_DESIGN.md §5（记录格式）与 §10 第 3 步的验收项。
#
# ── 为什么 flags 必须端到端保真（§5.5）──────────────────────────────
# DTX 记录与 DATA 记录共用同一个 partition_lsn 序号空间和同一条复制通道，
# 只靠头部 flags 区分。复制通道一旦丢了 flags，DTX/标记记录到了 follower 就
# 退化成 DATA 记录 —— 升主回放时会被当作原始 WAL 字节喂给 rm_redo，
# 结果是 PANIC 或静默的堆损坏。所以这条链上每一环都要带着它走：
#   leader: partwal_read_record 返回 flags
#        → pg_raft 描述符 JSON 里的 "flags"
#        → follower: partwal_follower_append(p_flags) → 落盘头部
#
# ── 判据 ────────────────────────────────────────────────────────────
#   A. 全新库 CREATE EXTENSION 冒烟 + 四个函数签名正确。
#      **这是唯一能抓到签名不一致的检查**：已装扩展不会重跑安装脚本，
#      改了签名却漏改 setup-raft.sh / COMMENT / 已安装副本时，
#      常规回归全绿而全新库直接建不出来（2026-08-03 实测踩到两次）。
#   B. leader 侧写入并读回：DATA 记录 flags=1；DTX 记录 flags=8、
#      orig_lsn=0/0、info 携带子类型；DECISION 的 participants 数组完整往返。
#   C. 对 DATA 记录调 partwal_read_dtx_record 必须返回 NULL
#      （分类以 flags 判定，不以 data_len 判定）。
#   D. ★ follower 侧 flags 保真：复制之后每个 follower 上逐条记录的
#      flags/info 与 leader 完全一致。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }

TBL=raft19_dtx
GID=""
LEADER_PORT=""
FOLLOWER_PORTS=()
MEMBER_PORTS=($((BASE_PORT + 1)) $((BASE_PORT + 2)) $((BASE_PORT + 3)))
SMOKE_DB=raft19_freshdb

cleanup() {
  psql_at "$BASE_PORT" -c "DROP DATABASE IF EXISTS ${SMOKE_DB};" >/dev/null 2>&1
  if [[ -n "$GID" ]]; then
    for port in "${MEMBER_PORTS[@]}" "$BASE_PORT"; do
      q "$port" "SELECT partdist.pg_raft_group_drop(${GID});" >/dev/null
      q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;" >/dev/null
      q "$port" "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}_${GID};" >/dev/null
    done
  fi
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TBL};" >/dev/null
}
fail() { cleanup; echo "raft_19 FAIL: $1"; exit 1; }

cleanup

# ── A. 全新库 CREATE EXTENSION 冒烟 + 签名核对 ──
psql_at "$BASE_PORT" -c "CREATE DATABASE ${SMOKE_DB};" >/dev/null 2>&1 \
  || fail "A: 建冒烟库失败"
A_OUT=$(psql_at "$BASE_PORT" -d "$SMOKE_DB" -v ON_ERROR_STOP=1 \
          -c "CREATE EXTENSION pg_partdist;" 2>&1)
grep -q "CREATE EXTENSION" <<<"$A_OUT" \
  || fail "A: 全新库 CREATE EXTENSION pg_partdist 失败：$(tr '\n' ' ' <<<"$A_OUT" | cut -c1-200)"

for fn_sig in "partwal_read_record:OUT flags integer" \
              "partwal_follower_append:p_flags integer" \
              "partwal_append_dtx_record:p_participants bigint[]" \
              "partwal_read_dtx_record:OUT participants bigint[]"; do
  fn=${fn_sig%%:*}; want=${fn_sig#*:}
  got=$(psql_at "$BASE_PORT" -d "$SMOKE_DB" -tAc "SELECT pg_get_function_identity_arguments(p.oid)
          FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
         WHERE n.nspname='partdist' AND p.proname='$fn';" 2>/dev/null || true)
  [[ "$got" == *"$want"* ]] \
    || fail "A: 全新库里 ${fn} 的签名缺少 '${want}'（实际：${got:-<不存在>}）"
done
psql_at "$BASE_PORT" -c "DROP DATABASE ${SMOKE_DB};" >/dev/null 2>&1
echo "raft_19 A: 全新库 CREATE EXTENSION + 四个函数签名正确 ✓"

# ── 夹具：一个 3 成员数据组 ──
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "SET citus.enable_ddl_propagation=on;
   SET citus.shard_count = 16;
   SET citus.shard_replication_factor = 1;
   CREATE TABLE ${TBL}(id int primary key, v text);
   SELECT create_distributed_table('${TBL}', 'id');" >/dev/null 2>&1 \
  || fail "建分布式表失败"

GID=$(q "$BASE_PORT" \
  "SELECT p.shardid FROM pg_dist_placement p
     JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole='primary'
     JOIN pg_dist_shard s ON s.shardid = p.shardid
    WHERE s.logicalrelid='${TBL}'::regclass AND n.nodeport = ${MEMBER_PORTS[0]}
    ORDER BY p.shardid LIMIT 1;")
[[ -n "$GID" ]] || fail "worker1 上没有该表的分片"

for port in "${MEMBER_PORTS[@]:1}"; do
  q "$port" "SET citus.enable_ddl_propagation=off;
             CREATE TABLE IF NOT EXISTS ${TBL}_${GID} (LIKE ${TBL} INCLUDING ALL);" >/dev/null
done
for port in "${MEMBER_PORTS[@]}"; do
  q "$port" "SELECT partdist.rebuild_shard_identity();" >/dev/null
  q "$port" "INSERT INTO partdist.partition_map(partition_id, primary_node, secondary_nodes, primary_term)
             VALUES (${GID}::oid, 2, ARRAY[3,4], 1)
             ON CONFLICT (partition_id) DO UPDATE
               SET primary_node=EXCLUDED.primary_node,
                   secondary_nodes=EXCLUDED.secondary_nodes;" >/dev/null
  q "$port" "SELECT partdist.pg_raft_group_create(${GID});" >/dev/null
done

# Raft 不保证哪个成员当选 —— 动态发现，不硬断言某个节点
for _ in $(seq 1 30); do
  for port in "${MEMBER_PORTS[@]}"; do
    [[ "$(q "$port" "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID};")" == leader ]] \
      && { LEADER_PORT="$port"; break; }
  done
  [[ -n "$LEADER_PORT" ]] && break
  sleep 1
done
[[ -n "$LEADER_PORT" ]] || fail "30s 内三个成员都没选出 leader"
for port in "${MEMBER_PORTS[@]}"; do
  [[ "$port" == "$LEADER_PORT" ]] || FOLLOWER_PORTS+=("$port")
done
LOID=$(q "$LEADER_PORT" "SELECT partdist.local_partition_for_shard(${GID});")
[[ -n "$LOID" && "$LOID" != "0" ]] || fail "leader 上解析不到本地分片 OID"

# 路由层要跟上 leader，否则写入被写栅栏拒绝
LEADER_NODE=$((LEADER_PORT - BASE_PORT + 1))
routed=0
for _ in $(seq 1 40); do
  cur=$(q "$BASE_PORT" "SELECT m.node_id FROM pg_dist_placement p
                          JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
                          JOIN partdist.node_map m ON m.port=n.nodeport
                         WHERE p.shardid=${GID};")
  [[ "$cur" == "$LEADER_NODE" ]] && { routed=1; break; }
  sleep 1
done
(( routed == 1 )) \
  || fail "40s 内 pg_dist_placement 未切到 leader 所在节点(node ${LEADER_NODE}，当前 ${cur:-?})——写入会被写栅栏拒绝"

# 写栅栏在领导权/路由稳定期间是暂时性拒绝，做有界重试
try_insert() {   # $1=id $2=值 $3=阶段名
  local out i
  for i in $(seq 1 15); do
    out=$(psql_at "$BASE_PORT" -q -c "INSERT INTO ${TBL} VALUES ($1,'$2');" 2>&1) && return 0
    sleep 1
  done
  fail "$3：INSERT 重试 15 次仍失败：$(tr '\n' ' ' <<<"$out" | cut -c1-220)"
}

# ── B/C. leader 侧写入并读回 ──
read -r -a IDS <<<"$(q "$BASE_PORT" "SELECT string_agg(id::text,' ') FROM (
    SELECT g AS id FROM generate_series(1,60000) g
     WHERE get_shard_id_for_distribution_column('${TBL}', g) = ${GID} LIMIT 4) t;")"
(( ${#IDS[@]} >= 2 )) || fail "取不到落在该分片的 id"

try_insert "${IDS[0]}" a "B: 普通 INSERT（夹具不成立）"
DATA_MAX=$(q "$LEADER_PORT" "SELECT partdist.get_partition_flush_lsn(${LOID}::oid);")
[[ "$DATA_MAX" =~ ^[0-9]+$ ]] && (( DATA_MAX > 0 )) || fail "B: leader 侧没有 DATA 记录"

PL_PREP=$(q "$LEADER_PORT" "SELECT partdist.partwal_append_dtx_record(${LOID}::oid, 1, 777001, ${GID});")
PL_DEC=$(q "$LEADER_PORT" "SELECT partdist.partwal_append_dtx_record(${LOID}::oid, 2, 777001, ${GID}, 1691000000, 1, ARRAY[${GID}, 999888]::bigint[]);")
[[ "$PL_PREP" =~ ^[0-9]+$ && "$PL_DEC" =~ ^[0-9]+$ ]] || fail "B: 写 DTX 记录失败"

# DATA 记录 flags 必须是 1（0x01），且 orig_lsn 非零
bad=$(q "$LEADER_PORT" "SELECT count(*) FROM generate_series(1, ${DATA_MAX}) g,
        LATERAL partdist.partwal_read_record(${LOID}::oid, g) r
        WHERE r.flags <> 1;")
[[ "$bad" == "0" ]] || fail "B: leader 侧有 ${bad} 条 DATA 记录的 flags 不是 1"

# DTX 记录：flags=8、orig_lsn=0/0、info=子类型
for pair in "${PL_PREP}:1" "${PL_DEC}:2"; do
  plsn=${pair%%:*}; kind=${pair#*:}
  got=$(q "$LEADER_PORT" "SELECT r.flags||','||r.info||','||(r.orig_lsn = '0/0'::pg_lsn)
          FROM partdist.partwal_read_record(${LOID}::oid, ${plsn}) r;")
  [[ "$got" == "8,${kind},true" ]] \
    || fail "B: plsn ${plsn} 的 DTX 记录头不符，期望 '8,${kind},true'（flags,info,orig_lsn是否为0）实际 '${got}'"
done

DEC=$(q "$LEADER_PORT" "SELECT kind||'|'||dtxid||'|'||coord_gsid||'|'||commit_ts||'|'||verdict||'|'||participants::text
        FROM partdist.partwal_read_dtx_record(${LOID}::oid, ${PL_DEC});")
[[ "$DEC" == "2|777001|${GID}|1691000000|1|{${GID},999888}" ]] \
  || fail "B: DECISION 载荷往返不符，实际 '${DEC}'"

NOTDTX=$(q "$LEADER_PORT" "SELECT coalesce((SELECT kind::text FROM partdist.partwal_read_dtx_record(${LOID}::oid, 1)), 'NULL');")
[[ "$NOTDTX" == "NULL" ]] \
  || fail "C: 对 DATA 记录调 read_dtx_record 应返回 NULL，实际 '${NOTDTX}'"
echo "raft_19 B/C: leader 侧 DATA flags=1、DTX flags=8/orig_lsn=0/DECISION 载荷往返一致，DATA 不被误判为 DTX ✓"

# ── D. follower 侧 flags 保真 ──
# 再写一行普通数据触发复制挂钩，把 DTX 记录连同增量一起推出去
try_insert "${IDS[1]}" b "D: 触发复制的 INSERT"
sleep 2

LSIG=$(q "$LEADER_PORT" "SELECT string_agg(g||':'||r.flags||':'||r.info, ',' ORDER BY g)
        FROM generate_series(1, ${PL_DEC}) g,
             LATERAL partdist.partwal_read_record(${LOID}::oid, g) r;")
[[ -n "$LSIG" ]] || fail "D: leader 侧读不出记录序列"

for port in "${FOLLOWER_PORTS[@]}"; do
  FOID=$(q "$port" "SELECT partdist.local_partition_for_shard(${GID});")
  [[ -n "$FOID" && "$FOID" != "0" ]] || fail "D: follower ${port} 解析不到本地分片 OID"
  FSIG=$(q "$port" "SELECT string_agg(g||':'||r.flags||':'||r.info, ',' ORDER BY g)
          FROM generate_series(1, ${PL_DEC}) g,
               LATERAL partdist.partwal_read_record(${FOID}::oid, g) r;")
  [[ "$FSIG" == "$LSIG" ]] \
    || fail "D: follower ${port} 的 flags/info 序列与 leader 不一致——复制通道丢了 flags（§5.5）
       leader  : ${LSIG}
       follower: ${FSIG:-<空>}"
  FDEC=$(q "$port" "SELECT kind||'|'||dtxid||'|'||verdict||'|'||participants::text
          FROM partdist.partwal_read_dtx_record(${FOID}::oid, ${PL_DEC});")
  [[ "$FDEC" == "2|777001|1|{${GID},999888}" ]] \
    || fail "D: follower ${port} 的 DECISION 载荷不符，实际 '${FDEC}'"
done
echo "raft_19 D: ${#FOLLOWER_PORTS[@]} 个 follower 的 flags/info 序列与 DTX 载荷均与 leader 一致 ✓"

cleanup
echo "raft_19 PASS"
exit 0
