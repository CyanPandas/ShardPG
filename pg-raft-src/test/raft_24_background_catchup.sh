#!/usr/bin/env bash
# raft_24: 后台追平通道（计划文档 §12.4 #5 的后半段）
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_24_background_catchup.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# ── 被验收的缺口 ─────────────────────────────────────────────────────
# 数据条目只在 client backend 的 propose 路径下发（取 parwal 字节要 SPI，BGW
# tick 没有），因此**没有写入流量时，落后的 follower 不会自行收敛**：掉线重启
# 的副本要等下一笔业务写入才被顺带补齐。落后超过环容量（RAFT_LOG_CAPACITY=128）
# 时更糟：leader 的 log_get_entry_locked 在槽位被覆盖后失败 → prev_term 取到 0
# → follower 拒 → next_index 一路退到 1 → 此后只发心跳、match 恒 0，**静默永久
# 卡死**，连下一笔写入也补不动（字节其实一直都在 partdist.raft_log 与段文件里）。
#
# ── 判据 ─────────────────────────────────────────────────────────────
#   A. 关掉通道（catchup_interval_ms=0）时**必须不收敛** —— 这一半是对照：
#      它证明 B 段的收敛确实由通道产生，而不是被别的路径顺带补上的。
#   B. 打开通道（reload 生效）后，**不写入任何新数据**，落后 follower 在窗口内
#      逐字节追平（条数 + 指纹）。
#   C. 落后**超过环容量**（>128 条）时同样能追平 —— 这一段走的是环外条目从
#      partdist.raft_log 回读的路径（leader 侧取条目 + follower 侧 prev 检查）。
#   D. 追平只补发已存在的条目：追平前后 leader 的 last_log_index 不变，
#      收敛后再调一次 pg_raft_catchup() 返回 0（幂等，无空转补发）。
#
# ── 夹具的两个硬约束（都被实测逼出来）───────────────────────────────
#   * 段文件基线必须清零：被 DROP 的分片表留下的 pg_parwal/<oid>/ 不回收而 OID 会
#     复用，新壳表可能"继承"上一轮的记录（raft_13 的老教训）。
#   * 领导权会漂移，不能假定它落在 placement 节点、更不能假定它不变：起步时三个
#     成员日志都空、谁先超时谁当选；且本实现没有 PreVote，成员重启会带着更高 term
#     竞选，把在任 leader 打成 follower 一轮。所以每次写入前都重新发现 leader 并
#     等路由层跟上——写不进 leader 是写栅栏在起作用，与追平通道无关。
#
# 真对照（离线执行，不在套件里）：把 raft_consensus.c 回退到本次改动之前重编
# .so，同一用例在 B 段就确定性失败（follower 永不收敛）。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
PGCTL="docker exec -u postgres $CONTAINER /work/pg-install/bin/pg_ctl"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }

TBL=raft24_demo
MEMBERS="2,3,4"
GID=""; LEADER_PORT=""; F_PORT=""

port_dir() {   # $1=port → 数据目录名
  if [[ "$1" == "$BASE_PORT" ]]; then echo coordinator; else echo "worker$(( $1 - BASE_PORT ))"; fi
}
node_stop() { $PGCTL stop -D "/work/pg-cluster-data/$(port_dir "$1")" -m fast >/dev/null 2>&1 || true; }
node_start() {
  docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_isready -q -h localhost -p "$1" 2>/dev/null && return 0
  $PGCTL start -D "/work/pg-cluster-data/$(port_dir "$1")" -o "-p $1" -w >/dev/null 2>&1 || true
}

cleanup() {
  local port
  for port in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
    node_start "$port"
  done
  for port in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
    psql_at "$port" -q -c "ALTER SYSTEM RESET pg_raft.catchup_interval_ms" >/dev/null 2>&1 || true
    q "$port" "SELECT pg_reload_conf();" >/dev/null
    q "$port" "SELECT partdist.pg_raft_group_reset();" >/dev/null
    if [[ -n "$GID" ]]; then
      q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;
                 DELETE FROM partdist.follower_partition_map WHERE global_shard_id = ${GID};
                 SET citus.enable_ddl_propagation=off;
                 DROP TABLE IF EXISTS ${TBL}_${GID};" >/dev/null
    fi
  done
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TBL};" >/dev/null
}
fail() { cleanup; echo "raft_24 FAIL: $1"; exit 1; }

# ── 通用助手 ─────────────────────────────────────────────────────────
set_catchup() {  # $1=ms —— 全节点生效（reload，不重启：顺带走一遍 SIGHUP 通路）
  # ALTER SYSTEM 与 pg_reload_conf() 必须分开发：psql 的单个 -c 里放多条语句会被
  # 包进一个事务块，而 ALTER SYSTEM 在事务块里直接报错（且会被 q 静默吞掉）。
  local port got
  for port in $(seq "$BASE_PORT" $((BASE_PORT + 8))); do
    docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_isready -q -h localhost -p "$port" 2>/dev/null || continue
    psql_at "$port" -v ON_ERROR_STOP=1 -q -c \
      "ALTER SYSTEM SET pg_raft.catchup_interval_ms = $1" >/dev/null 2>&1 \
      || fail "set_catchup：节点 ${port} 的 ALTER SYSTEM 失败"
    q "$port" "SELECT pg_reload_conf();" >/dev/null
  done
  got=$(q "$LEADER_PORT" "SHOW pg_raft.catchup_interval_ms;")
  [[ "$got" == "${1}ms" || "$got" == "$1" ]] \
    || fail "set_catchup：leader 上 catchup_interval_ms 仍是 ${got}，期望 $1"
}

flush_of() { q "$1" "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(${GID}));"; }
fp_of() {    # $1=port $2=nrec —— 前 nrec 条记录的逐字节指纹
  q "$1" "SELECT md5(string_agg(sub.h, ',' ORDER BY sub.plsn)) FROM (
            SELECT g AS plsn, md5(r.data) AS h
              FROM generate_series(1, $2) g,
                   LATERAL partdist.partwal_read_record(
                     partdist.local_partition_for_shard(${GID}), g) r) sub;"
}
last_log_index_of() { q "$1" "SELECT last_log_index FROM partdist.pg_raft_group_status() WHERE group_id=${GID};"; }
dump_state() {   # 诊断用：三个成员的组状态 + 路由指向
  local n port out=""
  for n in ${MEMBERS//,/ }; do
    port=$(( BASE_PORT + n - 1 ))
    out+="${port}:$(q "$port" "SELECT state||'/t'||current_term||'/i'||last_log_index FROM partdist.pg_raft_group_status() WHERE group_id=${GID};") "
  done
  out+="route=$(q "$BASE_PORT" "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${GID};")"
  echo "$out"
}

current_leader() {
  local n port
  for n in ${MEMBERS//,/ }; do
    port=$(( BASE_PORT + n - 1 ))
    if [[ "$(q "$port" "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID};")" == leader ]]; then
      echo "$port"; return 0
    fi
  done
  return 1
}
wait_leader_and_route() {  # $1=最长秒数；成功后 LEADER_PORT 是组 leader 且路由指向它
  local secs=$1 i lp route
  for i in $(seq 1 "$secs"); do
    lp=$(current_leader) || lp=""
    if [[ -n "$lp" ]]; then
      route=$(q "$BASE_PORT" \
        "SELECT n.nodeport FROM pg_dist_placement p
           JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
          WHERE p.shardid = ${GID};")
      if [[ "$route" == "$lp" ]]; then LEADER_PORT="$lp"; return 0; fi
    fi
    sleep 1
  done
  return 1
}

converged_within() {  # $1=port $2=秒 → follower 与 leader 条数+指纹一致
  local port=$1 secs=$2 i n lfp ffp fn
  n=$(flush_of "$LEADER_PORT"); lfp=$(fp_of "$LEADER_PORT" "$n")
  for i in $(seq 1 "$secs"); do
    fn=$(flush_of "$port")
    if [[ "$fn" == "$n" ]]; then
      ffp=$(fp_of "$port" "$n")
      [[ -n "$ffp" && "$ffp" == "$lfp" ]] && return 0
    fi
    sleep 1
  done
  LAST_N="$n"; LAST_FN="$fn"
  return 1
}

# 单分片写入走快路径（不产生 2PC 标记），一条 INSERT ≈ 两条 parwal 记录（堆+索引）
insert_rows() {  # $1=起 $2=止；失败时把 SQL 报错留在 INS_ERR（不吞错）
  local i sqlf=/tmp/raft24_ins.sql
  docker exec -u postgres "$CONTAINER" bash -c "rm -f $sqlf"
  for i in $(seq "$1" "$2"); do
    echo "INSERT INTO ${TBL} VALUES ($i, 'v$i');"
  done | docker exec -i -u postgres "$CONTAINER" bash -c "cat > $sqlf"
  INS_ERR=$(psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -f "$sqlf" 2>&1)
}

pick_victim() {   # 在存活成员里挑一个**不是 leader** 的作为被停节点
  local n port
  for n in ${MEMBERS//,/ }; do
    port=$(( BASE_PORT + n - 1 ))
    [[ "$port" == "$LEADER_PORT" ]] && continue
    docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_isready -q -h localhost -p "$port" 2>/dev/null || continue
    echo "$port"; return 0
  done
  return 1
}

cleanup

# ── 夹具：单分片哈希分布表 + 三成员数据组（与 raft_14/16 同构）──────────
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "SET citus.enable_ddl_propagation=on;
   SET citus.shard_count = 1;
   SET citus.shard_replication_factor = 1;
   CREATE TABLE ${TBL}(id int primary key, v text);
   SELECT create_distributed_table('${TBL}', 'id');" >/dev/null 2>&1 \
  || fail "建分布式表失败"

GID=$(q "$BASE_PORT" "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='${TBL}'::regclass;")
PLACEMENT_PORT=$(q "$BASE_PORT" \
  "SELECT n.nodeport FROM pg_dist_placement p
     JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
    WHERE p.shardid = ${GID:-0};")
[[ -n "$GID" && -n "$PLACEMENT_PORT" ]] || fail "夹具：取不到 shardid/placement"
LEADER_PORT="$PLACEMENT_PORT"

for n in ${MEMBERS//,/ }; do
  port=$(( BASE_PORT + n - 1 ))
  [[ "$port" == "$PLACEMENT_PORT" ]] || \
    q "$port" "SET citus.enable_ddl_propagation=off;
               CREATE TABLE IF NOT EXISTS ${TBL}_${GID} (LIKE ${TBL} INCLUDING ALL);" >/dev/null
  q "$port" "SELECT partdist.rebuild_shard_identity();" >/dev/null
done

for n in ${MEMBERS//,/ }; do
  port=$(( BASE_PORT + n - 1 ))
  q "$port" "SELECT partdist.partwal_truncate_to(partdist.local_partition_for_shard(${GID})::oid, 0);" >/dev/null
  base=$(q "$port" "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(${GID}));")
  [[ "$base" == "0" ]] || fail "夹具：节点 ${port} 的段文件基线不是 0（${base}），OID 复用了带记录的旧目录"
done

q "$PLACEMENT_PORT" "SELECT partdist.pg_raft_group_create(${GID}, ARRAY[${MEMBERS}]::int[]);" >/dev/null
sleep 2
for n in ${MEMBERS//,/ }; do
  port=$(( BASE_PORT + n - 1 ))
  [[ "$port" == "$PLACEMENT_PORT" ]] || \
    q "$port" "SELECT partdist.pg_raft_group_create(${GID}, ARRAY[${MEMBERS}]::int[]);" >/dev/null
done

wait_leader_and_route 60 || fail "夹具：60s 内没等到「组 leader == 路由指向」[$(dump_state)]"

# ── A/B：无写入流量下的收敛，先关通道取"缺陷态"，再开通道取"修复态" ────────
set_catchup 0
F_PORT=$(pick_victim) || fail "A：挑不出非 leader 的成员节点 [$(dump_state)]"
node_stop "$F_PORT"
sleep 2
wait_leader_and_route 45 || fail "A：停掉 ${F_PORT} 后 45s 内没等到 leader==路由 [$(dump_state)]"
insert_rows 1 20 || fail "A：停掉一个 follower 后 INSERT 失败（2/3 应仍是多数派）：$(echo "$INS_ERR" | grep -m1 -i error | head -c 200) [$(dump_state)]"
BASE_N=$(flush_of "$LEADER_PORT")
[[ "$BASE_N" =~ ^[0-9]+$ && "$BASE_N" -ge 20 ]] || fail "A：leader 侧记录数异常（flush=${BASE_N}）"

node_start "$F_PORT"
sleep 3
wait_leader_and_route 45 || fail "A：${F_PORT} 归队后 45s 内没等到 leader==路由 [$(dump_state)]"
BASE_N=$(flush_of "$LEADER_PORT")
F_BEFORE=$(flush_of "$F_PORT")
[[ "$F_BEFORE" =~ ^[0-9]+$ ]] || fail "A：follower ${F_PORT} 重启后取不到进度"
[[ "$F_BEFORE" -lt "$BASE_N" ]] || fail "A：follower 重启时就已追平（${F_BEFORE}/${BASE_N}），构造无效"

# A：通道关闭 + 无任何写入 ⇒ 必须不收敛（缺陷态，确定性）
if converged_within "$F_PORT" 20; then
  fail "A：通道关闭时 follower ${F_PORT} 竟自行收敛到 ${BASE_N} —— 对照不成立，B 段的收敛无法归因"
fi

# B：打开通道（reload，不重启），仍然不写入任何数据
set_catchup 1000
if ! converged_within "$F_PORT" 60; then
  fail "B：开通道后 follower ${F_PORT} 未在 60s 内收敛（follower=${LAST_FN:-?} leader=${LAST_N:-?}）[$(dump_state)]"
fi

# ── C：落后超过环容量（RAFT_LOG_CAPACITY=128）也要能追平 ────────────────
set_catchup 0
F_PORT=$(pick_victim) || fail "C：挑不出非 leader 的成员节点 [$(dump_state)]"
node_stop "$F_PORT"
sleep 2
wait_leader_and_route 45 || fail "C：停掉 ${F_PORT} 后 45s 内没等到 leader==路由 [$(dump_state)]"
insert_rows 21 200 || fail "C：批量 INSERT 失败：$(echo "$INS_ERR" | grep -m1 -i error | head -c 200) [$(dump_state)]"
BIG_N=$(flush_of "$LEADER_PORT")
node_start "$F_PORT"
sleep 3
wait_leader_and_route 45 || fail "C：${F_PORT} 归队后 45s 内没等到 leader==路由 [$(dump_state)]"
BIG_N=$(flush_of "$LEADER_PORT")
F_BEFORE2=$(flush_of "$F_PORT")
GAP=$(( BIG_N - F_BEFORE2 ))
[[ "$GAP" -gt 128 ]] || fail "C：落后 ${GAP} 条未超过环容量 128，构造无效（leader=${BIG_N} follower=${F_BEFORE2}）"

LLI_BEFORE=$(last_log_index_of "$LEADER_PORT")
set_catchup 1000
if ! converged_within "$F_PORT" 90; then
  fail "C：落后 ${GAP} 条（>环容量）时未在 90s 内收敛（follower=${LAST_FN:-?} leader=${LAST_N:-?}）[$(dump_state)]"
fi

# ── D：追平只补发已存在的条目，且收敛后幂等 ────────────────────────────
LLI_AFTER=$(last_log_index_of "$LEADER_PORT")
[[ "$LLI_BEFORE" == "$LLI_AFTER" ]] \
  || fail "D：追平过程中 leader 的 last_log_index 变了（${LLI_BEFORE} → ${LLI_AFTER}）—— 追平不该产生新条目"
AGAIN=$(q "$LEADER_PORT" "SELECT partdist.pg_raft_catchup();")
[[ "$AGAIN" == "0" ]] || fail "D：已收敛后再调 pg_raft_catchup() 返回 ${AGAIN}，期望 0（幂等）"

# 另一个成员全程在线，终态也必须一致
OTHER=""
for n in ${MEMBERS//,/ }; do
  port=$(( BASE_PORT + n - 1 ))
  [[ "$port" == "$LEADER_PORT" || "$port" == "$F_PORT" ]] || OTHER="$port"
done
[[ -n "$OTHER" ]] || fail "D：找不到第三个成员"
if ! converged_within "$OTHER" 30; then
  fail "D：全程在线的成员 ${OTHER} 终态不一致（follower=${LAST_FN:-?} leader=${LAST_N:-?}）[$(dump_state)]"
fi

cleanup
echo "raft_24 PASS: 追平通道（A 对照不收敛 / B 无写入收敛 / C 超环容量收敛 / D 不产生新条目且幂等）"
exit 0
