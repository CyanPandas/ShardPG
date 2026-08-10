#!/usr/bin/env bash
# raft_17: 并发写入下 prepare 的多数派保证（group commit 让路窗口）
#
# 独立可跑：bash pg-raft-src/test/raft_17_concurrent_prepare_quorum.sh
#   CONTAINER=pg-citus-raft-container bash .../raft_17_concurrent_prepare_quorum.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL；失败原因打在 stdout 末行）。
#
# ── 复现的缺陷（DTX_2PC_DESIGN.md §9.1）──────────────────────────────────
# PartWALFlush 里 backend X 会顺带把并发 backend Y 的槽位一起写进段文件并
# fsync，但：
#   (a) X 的触达集合只登记 slot->backend_id == X 的槽位 ⇒ Y 的分区不在 X 的
#       复制范围里；
#   (b) Y 随后看到 flushed_upto 已覆盖自己，走提前返回分支 ⇒ 复制挂钩根本不调。
# 于是 Y 的记录落了盘、事务提交成功，却**没有任何节点**把它复制出去 ——
# 直到该分区下一次写入才被增量下界顺带补齐。
#
# ── 夹具形态 ────────────────────────────────────────────────────────────
# 同一个 worker 上的两个数据组 A/B（各 3 成员）。burst 期间：
#   6 个会话持续单行 INSERT 打 A（制造密集的 PRE_COMMIT 让路）
#   2 个会话只打少量 B（B 几乎没有后续写入，漏掉就不会被自愈）
# id 用 Citus 的 get_shard_id_for_distribution_column 反查，保证流量精确落在
# 这两个分片上，而不是散到 16 个分片里去。
#
# ── 判据：多数派，不是全体 ──────────────────────────────────────────────
# Raft 只保证 quorum。3 成员组的 quorum = leader + 1 follower，**第二个
# follower 落后是合法的**（本项目还没有后台追平通道，见计划文档 §12.3.B.5）。
# 所以判据是"持有完整前缀的成员数 >= 多数派"：
#   有缺陷时记录只在 leader 上 ⇒ 计数 1 < 2 ⇒ 失败；
#   正常时至少一个 follower 有 ⇒ 计数 >= 2 ⇒ 通过。
# 断言前**不得再有任何写入** —— 后续写入会经增量下界补齐，缺陷就测不出来。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }

BASE_PORT="${BASE_PORT:-5432}"
if [[ -z "${N_WORKERS:-}" ]]; then
  N_WORKERS=$(docker exec "$CONTAINER" bash -lc \
    "find /work/pg-cluster-data -maxdepth 1 -type d -name 'worker*' | wc -l" 2>/dev/null || echo 3)
  N_WORKERS=${N_WORKERS//[^0-9]/}
  [[ -n "$N_WORKERS" && "$N_WORKERS" -gt 0 ]] || N_WORKERS=3
fi
NODE_PORTS=()
for ((i = 0; i <= N_WORKERS; i++)); do NODE_PORTS+=($((BASE_PORT + i))); done

TBL="raft17_dtx"
GID_A=""; GID_B=""; LEADER_PORT=""; FOLLOWERS=()
WHY="setup"

cleanup() {
  local port gid
  for gid in "$GID_A" "$GID_B"; do
    [[ -n "$gid" ]] || continue
    for port in "${NODE_PORTS[@]}"; do
      q "$port" "SELECT partdist.pg_raft_group_drop(${gid});" >/dev/null
      q "$port" "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}_${gid};" >/dev/null
      q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${gid}::oid;
                 DELETE FROM partdist.follower_partition_map WHERE global_shard_id = ${gid};" >/dev/null
    done
  done
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TBL};" >/dev/null
}
fail() {
  WHY="$1"
  if [[ -s "${BURST_ERR:-/nonexistent}" ]]; then
    echo "--- burst 期间客户端报错(前 8 行 / 共 $(grep -c . "$BURST_ERR") 行) ---"
    grep . "$BURST_ERR" | head -8
    echo "-------------------------------------------------------------"
  fi
  cleanup
  echo "raft_17 FAIL: ${WHY}"
  exit 1
}

cleanup

# ── 建表：分片数 = 2×worker 数，保证至少一个 worker 持有 >= 2 个分片 ──
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "SET citus.enable_ddl_propagation=on;
   SET citus.shard_count = $((N_WORKERS * 2));
   SET citus.shard_replication_factor = 1;
   CREATE TABLE ${TBL}(id int primary key, v text);
   SELECT create_distributed_table('${TBL}', 'id');" >/dev/null 2>&1 \
  || fail "建分布式表失败"

LEADER_PORT=$(q "$BASE_PORT" \
  "SELECT n.nodeport FROM pg_dist_placement p
     JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole='primary'
     JOIN pg_dist_shard s ON s.shardid = p.shardid
    WHERE s.logicalrelid = '${TBL}'::regclass
    GROUP BY n.nodeport HAVING count(*) >= 2
    ORDER BY n.nodeport LIMIT 1;")
[[ -n "$LEADER_PORT" ]] || fail "找不到同时持有 >= 2 个分片的 worker"

read -r GID_A GID_B <<<"$(q "$BASE_PORT" \
  "SELECT string_agg(shardid::text, ' ' ORDER BY shardid) FROM (
     SELECT p.shardid FROM pg_dist_placement p
       JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole='primary'
       JOIN pg_dist_shard s ON s.shardid = p.shardid
      WHERE s.logicalrelid='${TBL}'::regclass AND n.nodeport=${LEADER_PORT}
      ORDER BY p.shardid LIMIT 2) t;")"
[[ -n "$GID_A" && -n "$GID_B" ]] || fail "取不到该 worker 的两个分片"

LEADER_ID=$((LEADER_PORT - BASE_PORT + 1))
for port in "${NODE_PORTS[@]:1}"; do
  [[ "$port" != "$LEADER_PORT" ]] || continue
  FOLLOWERS+=("$port")
  (( ${#FOLLOWERS[@]} >= 2 )) && break
done
(( ${#FOLLOWERS[@]} == 2 )) || fail "凑不齐两个 follower（需要 >= 3 个 worker）"

MEMBER_IDS="ARRAY[${LEADER_ID}"
for port in "${FOLLOWERS[@]}"; do MEMBER_IDS+=",$((port - BASE_PORT + 1))"; done
MEMBER_IDS+="]::int[]"

# ── 副本壳表((a) 形态) + 身份注册 + 建组 ──
for gid in "$GID_A" "$GID_B"; do
  for port in "${FOLLOWERS[@]}"; do
    q "$port" "SET citus.enable_ddl_propagation=off;
               CREATE TABLE IF NOT EXISTS ${TBL}_${gid} (LIKE ${TBL} INCLUDING ALL);" >/dev/null
  done
done
for port in "$LEADER_PORT" "${FOLLOWERS[@]}"; do
  q "$port" "SELECT partdist.rebuild_shard_identity();" >/dev/null
done
for gid in "$GID_A" "$GID_B"; do
  q "$LEADER_PORT" "SELECT partdist.pg_raft_group_create(${gid}, ${MEMBER_IDS});" >/dev/null
done
for gid in "$GID_A" "$GID_B"; do
  got=0
  for _ in $(seq 1 20); do
    st=$(q "$LEADER_PORT" "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid};")
    [[ "$st" == "leader" ]] && got=1 && break
    sleep 1
  done
  (( got == 1 )) || fail "组 ${gid} 未在 placement 节点选出 leader"
  for port in "${FOLLOWERS[@]}"; do
    q "$port" "SELECT partdist.pg_raft_group_create(${gid}, ${MEMBER_IDS});" >/dev/null
  done
done

# ── 取精确落到 A/B 两个分片的 id ──
ids_for() {
  q "$BASE_PORT" "SELECT string_agg(id::text, ' ') FROM (
      SELECT g AS id FROM generate_series(1, 60000) g
       WHERE get_shard_id_for_distribution_column('${TBL}', g) = $1
       LIMIT $2) t;"
}
read -r -a IDS_A <<<"$(ids_for "$GID_A" 600)"
read -r -a IDS_B <<<"$(ids_for "$GID_B" 40)"
(( ${#IDS_A[@]} >= 480 && ${#IDS_B[@]} >= 32 )) \
  || fail "取不到足够的定向 id（A=${#IDS_A[@]} B=${#IDS_B[@]}）"
# 前半供阶段一（正常），后半供阶段二（失多数派）
HALF_A=$(( ${#IDS_A[@]} / 2 ))
HALF_B=$(( ${#IDS_B[@]} / 2 ))

# ── burst：6 会话猛打 A，2 会话轻打 B，全部单行 autocommit ──
BURST_ERR="${BURST_ERR:-/tmp/raft17_burst_err.log}"
: > "$BURST_ERR"

burst_session() {   # $1=起始下标 $2=条数 $3=数组名
  local -n arr=$3
  local start=$1 n=$2 i
  {
    for ((i = start; i < start + n && i < ${#arr[@]}; i++)); do
      echo "INSERT INTO ${TBL} VALUES (${arr[i]}, 'x');"
    done
  } | docker exec -i -u postgres "$CONTAINER" \
        /work/pg-install/bin/psql -p "$BASE_PORT" -U postgres -q >>"$BURST_ERR" 2>&1
}

run_burst() {   # $1=A 起始下标 $2=A 条数 $3=B 起始下标 $4=B 条数
  local pa=$(( $2 / 6 )) pb=$(( $4 / 2 )) k
  for k in 0 1 2 3 4 5; do burst_session $(( $1 + k * pa )) "$pa" IDS_A & done
  for k in 0 1;         do burst_session $(( $3 + k * pb )) "$pb" IDS_B & done
  wait
}

run_burst 0 "$HALF_A" 0 "$HALF_B"

# ★ 此后不得再有任何写入
sleep 3

# 崩溃与漏复制是两码事，必须分开报告：节点崩溃会连带把复制打断，
# 若不甄别就会把"并发写入路径 segfault"误记成"让路窗口漏复制"。
if grep -qiE "server closed the connection unexpectedly|crash of another server process|terminating connection because of crash" "$BURST_ERR"; then
  fail "burst 期间有节点崩溃(并发写入路径 segfault)——本轮结果对让路窗口无参考价值，请查 pg-cluster-data/<node>.log 的 'terminated by signal'"
fi

# ── 判据：持有完整前缀的成员数 >= 多数派 ──
prefix_md5() {  # $1=port $2=gid $3=nrec
  q "$1" "SELECT md5(string_agg(sub.h, ',' ORDER BY sub.plsn)) FROM (
            SELECT g AS plsn, md5(r.data) AS h
            FROM generate_series(1, $3) g,
                 LATERAL partdist.partwal_read_record(
                   partdist.local_partition_for_shard($2), g) r
          ) sub;"
}
flush_lsn() { q "$1" "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($2));"; }

NMEMBERS=3
MAJORITY=$(( NMEMBERS / 2 + 1 ))
for gid in "$GID_A" "$GID_B"; do
  nrec=$(flush_lsn "$LEADER_PORT" "$gid")
  [[ "$nrec" =~ ^[0-9]+$ ]] && (( nrec > 0 )) \
    || fail "组 ${gid} 的 leader 侧没有记录(flush=${nrec})，夹具没写到该分片"
  lmd5=$(prefix_md5 "$LEADER_PORT" "$gid" "$nrec")
  [[ -n "$lmd5" ]] || fail "组 ${gid} 的 leader 侧读不出完整前缀"

  have=1                      # leader 自己算一个
  detail="leader(${LEADER_PORT})=${nrec}"
  for port in "${FOLLOWERS[@]}"; do
    fcnt=$(flush_lsn "$port" "$gid")
    fmd5=$(prefix_md5 "$port" "$gid" "$nrec")
    detail+=" f(${port})=${fcnt:-?}"
    if [[ -n "$fmd5" && "$fmd5" == "$lmd5" ]]; then
      have=$((have + 1))
      detail+="✔"
    fi
  done
  (( have >= MAJORITY )) \
    || fail "组 ${gid} 只有 ${have}/${NMEMBERS} 个成员持有完整前缀(需 >= ${MAJORITY})：${detail}"
  echo "raft_17 阶段一: 组 ${gid} 多数派达成 ${have}/${NMEMBERS} — ${detail}"
done

# ══════════════════════════════════════════════════════════════════════
# 阶段二（让路窗口的**确定性**判据）：长事务被并发 flush 让路后，其记录必须
# 仍被复制出去
#
# 为什么终态检查（阶段一）抓不到让路窗口：漏掉的记录会在该分区下一次非提前
# 返回的 flush 里被增量下界补齐，终态几乎总是收敛。为什么"失多数派+并发"
# （阶段三）也抓不稳：失多数派把每个挂钩都拖进数秒的 RPC 超时，并发节奏被
# 拉开，让路窗口反而几乎不被踩中（对照实验实测 0/240 漏网——判据无效）。
#
# 所以这里**确定性地制造一次让路**，不靠概率：
#   会话 P：BEGIN; INSERT 1 条到 B 组; pg_sleep(4); COMMIT;
#     —— P 的记录在 t0 就进了环形缓冲区，但 P 的 PRE_COMMIT 在 t0+4s。
#   会话 Q（t0+1s）：对 A 组打若干条 autocommit
#     —— Q 的 PartWALFlush 的 upto 覆盖 P 的 LSN，顺带把 P 的槽位落盘，
#        flushed_upto 推进超过 P。
#   t0+4s，P 的 PRE_COMMIT：flushed_upto >= 自己的 upto ⇒ **必然走提前返回分支**。
#     · 让路窗口未修：不调挂钩 ⇒ B 组零 propose ⇒ P 提交成功但其记录
#       永远只在 leader 盘上（此后不再写 B，无自愈）⇒ follower 缺最后一条；
#     · 修复后：提前返回路径也调挂钩 ⇒ B 组把 P 的记录 propose 到多数派。
# 判据：B 组多数派成员持有含 P 记录的完整前缀。全程多数派健康，秒级完成。
# ══════════════════════════════════════════════════════════════════════
P_ID=${IDS_B[$((HALF_B))]}
(
  echo "BEGIN;"
  echo "INSERT INTO ${TBL} VALUES (${P_ID}, 'letpass');"
  echo "SELECT pg_sleep(4);"
  echo "COMMIT;"
) | docker exec -i -u postgres "$CONTAINER" \
      /work/pg-install/bin/psql -p "$BASE_PORT" -U postgres -q >>"$BURST_ERR" 2>&1 &
P_PID=$!
sleep 1
for i in 0 1 2 3 4; do
  q "$BASE_PORT" "INSERT INTO ${TBL} VALUES (${IDS_A[$((HALF_A + i))]}, 'q');" >/dev/null
done
wait "$P_PID"
sleep 2
# ★ 此后不得再写 B 组
if grep -qiE "server closed the connection unexpectedly|crash of another server process|terminating connection because of crash" "$BURST_ERR"; then
  fail "阶段二期间有节点崩溃——对让路窗口无参考价值"
fi
P_COMMITTED=$(q "$LEADER_PORT" "SELECT count(*) FROM ${TBL}_${GID_B} WHERE id = ${P_ID};")
[[ "$P_COMMITTED" == "1" ]] \
  || fail "阶段二夹具未成立：长事务 P 未提交成功(committed=${P_COMMITTED:-?})，无从判定让路"
nrec=$(q "$LEADER_PORT" "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(${GID_B}));")
lmd5=$(q "$LEADER_PORT" "SELECT md5(string_agg(sub.h, ',' ORDER BY sub.plsn)) FROM (
         SELECT g AS plsn, md5(r.data) AS h FROM generate_series(1, ${nrec}) g,
              LATERAL partdist.partwal_read_record(partdist.local_partition_for_shard(${GID_B}), g) r) sub;")
have=1; detail="leader(${LEADER_PORT})=${nrec}"
for port in "${FOLLOWERS[@]}"; do
  fcnt=$(q "$port" "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(${GID_B}));")
  fmd5=$(q "$port" "SELECT md5(string_agg(sub.h, ',' ORDER BY sub.plsn)) FROM (
           SELECT g AS plsn, md5(r.data) AS h FROM generate_series(1, ${nrec}) g,
                LATERAL partdist.partwal_read_record(partdist.local_partition_for_shard(${GID_B}), g) r) sub;")
  detail+=" f(${port})=${fcnt:-?}"
  [[ -n "$fmd5" && "$fmd5" == "$lmd5" ]] && { have=$((have + 1)); detail+="✔"; }
done
(( have >= MAJORITY )) \
  || fail "让路窗口：长事务 P 提交成功但其记录未达多数派(${have}/${NMEMBERS}：${detail})——P 的 PRE_COMMIT 走了提前返回分支且复制挂钩未被调用"
echo "raft_17 阶段二: 被让路的长事务记录已达多数派 ${have}/${NMEMBERS} — ${detail}"

# ══════════════════════════════════════════════════════════════════════
# 阶段三（2PC 性质回归）：失多数派 + 并发写 ⇒ 一行都不许提交成功
#
#   · 调用了复制挂钩的事务 → 拿不到多数派 → ERROR → INSERT 失败（正确）
#   · 若有事务绕过挂钩 → 不知道多数派已丢 → INSERT 成功（违规）
# 这是 raft_16 第 2 步（失多数派 INSERT 必败）的并发版本，正是 2PC 的
# prepare 需要的性质。只打 A 组。
# ══════════════════════════════════════════════════════════════════════
for port in "${FOLLOWERS[@]}"; do
  docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_ctl \
    -D "/work/pg-cluster-data/$( [[ $port == "$BASE_PORT" ]] && echo coordinator || echo worker$((port - BASE_PORT)) )" \
    stop -m fast >/dev/null 2>&1
done
sleep 3

# 阶段二的 Q 用掉了 IDS_A[HALF_A .. HALF_A+4]（多数派健康期、合法提交），
# 阶段三从 HALF_A+10 起取 id —— IDS_A 升序，保证阶段三的断言下界能把
# Q 的行排除在外，否则那 5 行会被误计为"失多数派期间提交"。
PHASE3_START=$(( HALF_A + 10 ))
PHASE3_MIN_A=${IDS_A[$PHASE3_START]}
: > "$BURST_ERR"
run_burst "$PHASE3_START" $(( ${#IDS_A[@]} - PHASE3_START )) 0 0
sleep 2

COMMITTED=$(q "$LEADER_PORT" \
  "SELECT count(*) FROM ${TBL}_${GID_A} WHERE id >= ${PHASE3_MIN_A};")

for port in "${FOLLOWERS[@]}"; do
  docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_ctl \
    -D "/work/pg-cluster-data/worker$((port - BASE_PORT))" \
    -l "/work/pg-cluster-data/worker$((port - BASE_PORT)).log" start -w -t 30 >/dev/null 2>&1
done
sleep 3

# 甄别崩溃：节点崩溃会重置 shmem 组表，之后的 INSERT 因为"查不到数据组"
# 而直接跳过复制挂钩并提交成功 —— 那是崩溃的次生现象，混为一谈会得到假阳性
# （本次定位就踩过这个坑）。
if grep -qiE "server closed the connection unexpectedly|crash of another server process|terminating connection because of crash" "$BURST_ERR"; then
  fail "阶段三 burst 期间有节点崩溃——本轮对失多数派判据无参考价值"
fi

# ★ 取不到值必须 fail，不能跳过。q() 是 `psql ... 2>/dev/null || true`，
# 任何失败都返回空串；空串不匹配 ^[0-9]+$ ⇒ 原来的写法直接跳过 fail ⇒
# 阶段三这条**最核心**的判据（2PC prepare 性质）变成 no-op 而报绿。
# 分片表名推导错、leader 在阶段三期间自己也挂了、psql 连不上 —— 都会走到这里。
[[ "$COMMITTED" =~ ^[0-9]+$ ]] \
  || fail "阶段三取不到提交行数（COMMITTED='${COMMITTED}'）——判据无从成立，不能算通过"
if (( COMMITTED > 0 )); then
  fail "失多数派期间仍有 ${COMMITTED} 行提交成功（组 ${GID_A}，id >= ${PHASE3_MIN_A}）——"\
"存在绕过复制挂钩的提交路径（DTX_2PC_DESIGN.md §9.1 的 prepare 性质被破坏）"
fi
echo "raft_17 阶段三: 失多数派期间提交成功行数 = ${COMMITTED}（组 ${GID_A}）"

cleanup
echo "raft_17 PASS"
exit 0
