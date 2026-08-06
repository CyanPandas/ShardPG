#!/usr/bin/env bash
# raft_28: 数据组日志外部化 E3 —— 写路径去掉 raft_log，末端随 hardstate v4（§11.10）
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_28_data_log_no_sql_rows.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# ── 被验收的改动 ─────────────────────────────────────────────────────
# E1 建了段式边界表，E2 把读路径切到 parwal，E3 把**写路径**上的冗余去掉：
# 数据组不再往 partdist.raft_log 写行（INSERT / committed UPDATE / DELETE 全免），
# 只维护 raft_log_runs。随之而来的问题是"重启后日志末端从哪来" ——
# **不能从段文件推**：leader 的 pg_parwal 里躺着大量还没被提案的记录（demux 按
# 本地提交不停地写，提案是另一条路径按需追上去的），拿 max(partition_lsn) 当末端
# 会凭空多出一截从未复制过的条目，重启后该 leader 会以为自己持有它们。
# 正解是 hardstate v4 增加 last_log_index —— 它本来就在每条 append 之后被写一次。
#
# ── 判据 ─────────────────────────────────────────────────────────────
#   A. **写路径真的瘦了**：提案 N 条之后，leader 与 follower 上该组在
#      partdist.raft_log 里都是 **0 行**，而 run 表有行、段文件有字节、组照常
#      工作（follower 条数/字节都跟上）。
#   B. **重启单个成员，末端不丢**：重启 follower 后它的 last_log_index 仍是 N
#      （不是 0），且新条目继续落在 N+1 —— 末端只能来自 hardstate。
#   C. **全节点重启后新条目不得落在已存在的 index 上**：这是 Leader Completeness
#      的前置。末端丢了的话重新当选的 leader 会以为日志止于 0，拿新内容覆盖
#      **已经提交**的位置。
#
# 真对照（离线执行，不在套件里）：persist_hard_state_values() 里把
# hs.last_log_index 写成 0（= v3 的行为）重编 .so ⇒ B 段确定性失败
# （重启后 follower 末端归零）。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
N_WORKERS="${N_WORKERS:-8}"
LAST_PORT=$(( BASE_PORT + N_WORKERS ))
TABLE=raft28_demo
MEMBER_PORTS=(5433 5434 5435)
MEMBER_IDS="ARRAY[2,3,4]"
NPROPOSE=20

PGCTL="docker exec -u postgres $CONTAINER /work/pg-install/bin/pg_ctl"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }

port_dir() { if [[ "$1" == "$BASE_PORT" ]]; then echo coordinator; else echo "worker$(( $1 - BASE_PORT ))"; fi; }
node_stop() { $PGCTL stop -D "/work/pg-cluster-data/$(port_dir "$1")" -m fast >/dev/null 2>&1 || true; }
node_start() {
  docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_isready -q -h localhost -p "$1" 2>/dev/null && return 0
  $PGCTL start -D "/work/pg-cluster-data/$(port_dir "$1")" -o "-p $1" -w >/dev/null 2>&1 || true
}
alive() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_isready -q -h localhost -p "$1" 2>/dev/null; }

GID=""
cleanup() {
  local port
  for port in $(seq "$BASE_PORT" "$LAST_PORT"); do node_start "$port"; done
  for port in "$BASE_PORT" "${MEMBER_PORTS[@]}"; do
    q "$port" "SELECT partdist.pg_raft_group_reset();" >/dev/null
    [[ -n "$GID" ]] && q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;" >/dev/null
  done
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TABLE};" >/dev/null
}
fail() { cleanup; echo "raft_28 FAIL: $1"; exit 1; }

g() { q "$1" "SELECT $2 FROM partdist.pg_raft_group_status() WHERE group_id = ${GID};"; }
nrec() { q "$1" "SELECT partdist.count_parwal_records(partdist.local_partition_for_shard(${GID}));"; }
nrows() { q "$1" "SELECT count(*) FROM partdist.raft_log WHERE group_id = ${GID};"; }
nruns() { q "$1" "SELECT count(*) FROM partdist.raft_log_runs WHERE group_id = ${GID};"; }

# 清零段文件并**确认稳定**。
#
# demux 是异步的：INSERT 提交之后它可能还没把记录写进段文件，此刻清零，随后
# 那条才落盘 —— 段文件凭空多出一条（2026-08-05 实测，raft_28 A 段报"21 条，
# 应为 20"）。所以清零后要复查，连续两次读到 0 才算干净。
truncate_stable() {   # $1=端口
  local i n prev=-1
  for i in $(seq 1 10); do
    q "$1" "SELECT partdist.partwal_truncate_to(
              partdist.local_partition_for_shard(${GID}), 0);" >/dev/null
    sleep 1
    n=$(nrec "$1")
    [[ "$n" == "0" && "$prev" == "0" ]] && return 0
    prev="$n"
  done
  return 1
}

find_leader() {
  local port i secs=${1:-45}
  for i in $(seq 1 "$secs"); do
    for port in "${MEMBER_PORTS[@]}"; do
      alive "$port" || continue
      [[ "$(g "$port" state)" == "leader" ]] && { echo "$port"; return 0; }
    done
    sleep 1
  done
  return 1
}

# 在 $1 上**造一条新记录再提案它**。
#
# 不能"提案段文件里编号最大的那条"（首版这么写，实测踩到）：follower 追加时
# AppendPartWALRecordAt 要求 plsn == 本地 last + 1，跳号直接 ERROR、不 ack、
# leader 凑不齐多数派 ⇒ 返回 0。而 leader 段文件里的 max plsn 远大于已提案到的
# 位置（demux 一直在写，提案是按需追上去的），拿它去提案必然跳号。
# 夹具让三个成员的段文件完全一致（都只有已提案的那 NPROPOSE 条），所以无论谁
# 当选，write_partition_wal_record 本地自增拿到的都是下一个连续编号，提案出去
# 对其余成员也正好是 last + 1。
propose_next() {
  q "$1" "SELECT partdist.write_partition_wal_record(
            partdist.local_partition_for_shard(${GID}), 1);" >/dev/null
  q "$1" "SELECT partdist.pg_raft_data_propose(${GID},
            (SELECT max(partition_lsn) FROM partdist.read_all_headers(
                 partdist.local_partition_for_shard(${GID}))));"
}

cleanup

# ── 夹具 ─────────────────────────────────────────────────────────────
q "$BASE_PORT" "SET citus.enable_ddl_propagation=on;
  DROP TABLE IF EXISTS ${TABLE};
  CREATE TABLE ${TABLE}(id int primary key, v text);
  SELECT create_reference_table('${TABLE}');" >/dev/null
q "$BASE_PORT" "INSERT INTO ${TABLE} VALUES (1, 'seed');" >/dev/null

for p in "$BASE_PORT" "${MEMBER_PORTS[@]}"; do
  q "$p" "SELECT partdist.rebuild_shard_identity();" >/dev/null
done

GID=$(q "$BASE_PORT" "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='${TABLE}'::regclass;")
[[ "$GID" =~ ^[0-9]+$ ]] || fail "夹具：拿不到 ${TABLE} 的 shardid"

for p in "${MEMBER_PORTS[@]}"; do
  q "$p" "SELECT partdist.pg_raft_group_create(${GID}, ${MEMBER_IDS});" >/dev/null
done

LEADER=$(find_leader) || fail "夹具：45s 内该数据组没有 leader"

# **三个成员一律清零，再由 leader 自造记录**。
#
# 不依赖 demux 产出多少条（一个事务落几条由它决定，不可控），也不依赖
# pg_parwal/<oid>/ 是干净的 —— 被 DROP 的分片表留下的目录不回收，新表拿到
# 复用的 OID 就会**继承上一轮的记录**（raft_13 的老教训，本用例首版实测踩到：
# 对照构建跑到 A 段报"段文件里 21 条记录，应为 20"）。
# 清零后断言为 0，起点才是确定的。
for p in "${MEMBER_PORTS[@]}"; do
  truncate_stable "$p" || fail "夹具：${p} 段文件清零后 10 轮内没有稳定在 0（demux 仍在写？）"
done

# leader 上造 NPROPOSE 条合成记录（本地自增 ⇒ plsn 恰好 1..NPROPOSE）
for k in $(seq 1 "$NPROPOSE"); do
  q "$LEADER" "SELECT partdist.write_partition_wal_record(
                 partdist.local_partition_for_shard(${GID}), 1);" >/dev/null
done
LREC=$(nrec "$LEADER")
[[ "$LREC" == "$NPROPOSE" ]] \
  || fail "夹具：leader 段文件造出 ${LREC} 条记录，应为 ${NPROPOSE}"

FOLLOWER=""
for p in "${MEMBER_PORTS[@]}"; do
  [[ "$p" != "$LEADER" ]] && FOLLOWER="$p" && break
done
[[ -n "$FOLLOWER" ]] || fail "夹具：找不到 follower"

# ── 提案 NPROPOSE 条 ─────────────────────────────────────────────────
OUT=$(psql_at "$LEADER" -v ON_ERROR_STOP=1 -tAc \
  "DO \$\$ DECLARE i bigint; r bigint; BEGIN
     FOR i IN 1..${NPROPOSE} LOOP
       r := partdist.pg_raft_data_propose(${GID}, i);
       IF r IS NULL OR r <= 0 THEN RAISE EXCEPTION 'propose plsn=% 失败', i; END IF;
     END LOOP;
   END \$\$;" 2>&1) || fail "提案失败：$(echo "$OUT" | tail -1)"

LLI=$(g "$LEADER" last_log_index)
[[ "$LLI" == "$NPROPOSE" ]] || fail "提案后 leader 末端 ${LLI} != ${NPROPOSE}"

for i in $(seq 1 20); do
  [[ "$(g "$FOLLOWER" last_log_index)" == "$LLI" ]] && break
  sleep 1
done
[[ "$(g "$FOLLOWER" last_log_index)" == "$LLI" ]] \
  || fail "follower 20s 内没跟上（$(g "$FOLLOWER" last_log_index) / ${LLI}）"

# ── A：写路径真的瘦了 ────────────────────────────────────────────────
for p in "$LEADER" "$FOLLOWER"; do
  ROWS=$(nrows "$p")
  [[ "$ROWS" == "0" ]] \
    || fail "A：${p} 上组 ${GID} 在 partdist.raft_log 里还有 ${ROWS} 行 —— 写路径没瘦"
  RUNS=$(nruns "$p")
  [[ "$RUNS" =~ ^[0-9]+$ && "$RUNS" -gt 0 ]] \
    || fail "A：${p} 上组 ${GID} 一行 run 都没有（${RUNS}）—— 那就什么都没记下来"
done

for p in "${MEMBER_PORTS[@]}"; do
  REC=$(nrec "$p")
  [[ "$REC" == "$NPROPOSE" ]] \
    || fail "A：${p} 段文件里 ${REC} 条记录，应为 ${NPROPOSE} —— 行没了、字节也得在"
done

# ── B：重启单个成员，末端不丢 ────────────────────────────────────────
node_stop "$FOLLOWER"
node_start "$FOLLOWER"
for i in $(seq 1 30); do alive "$FOLLOWER" && break; sleep 1; done
alive "$FOLLOWER" || fail "B：follower 起不来"

# 恢复走的是 client backend 路径（要 SPI），先踢一下让它把日志灌回环
q "$FOLLOWER" "SELECT partdist.pg_raft_catchup();" >/dev/null
FLLI=$(g "$FOLLOWER" last_log_index)
[[ "$FLLI" == "$NPROPOSE" ]] \
  || fail "B：follower 重启后末端是 ${FLLI}，应为 ${NPROPOSE} —— 末端只能来自 hardstate，丢了就等于日志凭空变短"

# 新条目必须落在 N+1，而不是覆盖已有位置
LEADER=$(find_leader) || fail "B：重启后 45s 内没有 leader"
IDX=$(propose_next "$LEADER")
[[ "$IDX" =~ ^[0-9]+$ && "$IDX" -gt "$NPROPOSE" ]] \
  || fail "B：重启后新条目落在 index=${IDX}，而日志已到 ${NPROPOSE}"
AFTER_B=$IDX

# ── C：全节点重启后新条目不得落在已存在的 index 上 ───────────────────
for p in $(seq "$BASE_PORT" "$LAST_PORT"); do node_stop "$p"; done
for p in $(seq "$BASE_PORT" "$LAST_PORT"); do node_start "$p"; done

LEADER=$(find_leader 90) || fail "C：全节点重启后 90s 内该数据组没有 leader"

IDX=$(propose_next "$LEADER")
if [[ ! "$IDX" =~ ^[0-9]+$ || "$IDX" -le "$AFTER_B" ]]; then
  DIAG="leader=${LEADER}"
  for p in "${MEMBER_PORTS[@]}"; do
    alive "$p" || { DIAG="${DIAG} | ${p}:down"; continue; }
    DIAG="${DIAG} | ${p}: state=$(g "$p" state) term=$(g "$p" current_term) lli=$(g "$p" last_log_index) commit=$(g "$p" commit_index) nrec=$(nrec "$p") maxplsn=$(q "$p" "SELECT max(partition_lsn) FROM partdist.read_all_headers(partdist.local_partition_for_shard(${GID}));") runs=$(nruns "$p")"
  done
  fail "C：全节点重启后新条目落在 index=${IDX}，而重启前日志已到 ${AFTER_B} —— ${DIAG}"
fi

cleanup
echo "raft_28 PASS: 数据组写路径去 raft_log（A raft_log 零行而 run/字节俱在 / B 重启后末端不丢 / C 全节点重启不覆盖已提交 index）"
exit 0
