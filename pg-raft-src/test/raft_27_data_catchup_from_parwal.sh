#!/usr/bin/env bash
# raft_27: 数据组日志外部化 E2 —— 环外条目改从 parwal 重建（计划文档 §11.10）
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_27_data_catchup_from_parwal.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# ── 被验收的改动 ─────────────────────────────────────────────────────
# E1 建了段式边界表并能从 parwal 重建条目，但读路径还没切 —— 环外条目仍然是从
# partdist.raft_log 回读的。E2 把数据组的那一级换成 parwal 重建：
#   log_get_entry_ext → log_get_entry_durable →
#       控制面：log_get_entry_sql（partdist.raft_log）
#       数据组：log_get_entry_parwal（raft_log_runs + pg_parwal 记录头部）
# 数据组从此**只认段文件**，raft_log 里那份镜像不再被读（E3 会把写入也撤掉）。
#
# ── 判据 ─────────────────────────────────────────────────────────────
#   A. **落后超过环容量的数据组成员能追平**：成员停机期间 leader 提案 > 128 条
#      （RAFT_LOG_CAPACITY），它归队时最老的那几十条**已经滑出环窗口**，只能靠
#      重建取出来。判据是它把 leader 的全部条目都拿到，且**字节真的落了盘**
#      （段文件记录数与 leader 一致，不只是 raft_log 行数对上）。
#   B. **确实走了环外那条路**：断言缺口 > 环容量，否则本用例退化成"环内追平"，
#      把重建摘掉也照样通过（raft_25 B 段就是这么假过一次，见 §6 表）。
#   C. **重建出来的条目与 leader 逐条一致**：追平后 victim 上每条 raft_log 的
#      (term, payload) 与它自己按 run 重建的结果相同 —— 复制过去的内容与段文件
#      里的字节是同一份，不是两套。
#
# 真对照（离线执行，不在套件里）：log_get_entry_parwal() 直接 return false
# 重编 .so ⇒ A 段确定性失败（victim 卡在 0，leader 已到 140）。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
TABLE=raft27_demo
MEMBER_PORTS=(5433 5434 5435)
MEMBER_IDS="ARRAY[2,3,4]"
RING_CAPACITY=128          # RAFT_LOG_CAPACITY，与 raft_consensus.c 保持一致
NENTRIES=140               # 必须 > 环容量，否则测不到环外那条路

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
  for port in "${MEMBER_PORTS[@]}"; do node_start "$port"; done
  node_start "$BASE_PORT"
  for port in "$BASE_PORT" "${MEMBER_PORTS[@]}"; do
    q "$port" "SELECT partdist.pg_raft_group_reset();" >/dev/null
    [[ -n "$GID" ]] && q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;" >/dev/null
  done
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TABLE};" >/dev/null
}
fail() { cleanup; echo "raft_27 FAIL: $1"; exit 1; }

g() { q "$1" "SELECT $2 FROM partdist.pg_raft_group_status() WHERE group_id = ${GID};"; }
nrec() { q "$1" "SELECT partdist.count_parwal_records(partdist.local_partition_for_shard(${GID}));"; }

find_leader() {
  local port i secs=${1:-30}
  for i in $(seq 1 "$secs"); do
    for port in "${MEMBER_PORTS[@]}"; do
      alive "$port" || continue
      [[ "$(g "$port" state)" == "leader" ]] && { echo "$port"; return 0; }
    done
    sleep 1
  done
  return 1
}

cleanup

# ── 夹具 ─────────────────────────────────────────────────────────────
# 逐条 INSERT 才有足够多的 parwal 记录：一条 generate_series 只是一个事务。
# 用 -f 一次性喂进去（psql 无事务块时每条语句各自提交），避免 140 次 docker exec。
q "$BASE_PORT" "SET citus.enable_ddl_propagation=on;
  DROP TABLE IF EXISTS ${TABLE};
  CREATE TABLE ${TABLE}(id int primary key, v text);
  SELECT create_reference_table('${TABLE}');" >/dev/null

SEED=/tmp/raft27_seed.sql
docker exec -u postgres "$CONTAINER" bash -c \
  "for k in \$(seq 1 $(( NENTRIES + 20 ))); do
     echo \"INSERT INTO ${TABLE} VALUES (\$k, repeat('x', 40));\";
   done > ${SEED}"
psql_at "$BASE_PORT" -q -f "$SEED" >/dev/null 2>&1

for p in "$BASE_PORT" "${MEMBER_PORTS[@]}"; do
  q "$p" "SELECT partdist.rebuild_shard_identity();" >/dev/null
done

GID=$(q "$BASE_PORT" "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='${TABLE}'::regclass;")
[[ "$GID" =~ ^[0-9]+$ ]] || fail "夹具：拿不到 ${TABLE} 的 shardid"

for p in "${MEMBER_PORTS[@]}"; do
  q "$p" "SELECT partdist.pg_raft_group_create(${GID}, ${MEMBER_IDS});" >/dev/null
done

LEADER=$(find_leader) || fail "夹具：30s 内该数据组没有 leader"

LREC=$(nrec "$LEADER")
[[ "$LREC" =~ ^[0-9]+$ && "$LREC" -ge "$NENTRIES" ]] \
  || fail "夹具：leader 段文件里只有 ${LREC} 条记录，不足 ${NENTRIES} 条"

# 非 leader 成员清零，模拟纯 secondary（raft_13 的老教训：reference 表在每个
# 节点都是本地主写，本地编号会和 leader 下发的编号相撞）。
for p in "${MEMBER_PORTS[@]}"; do
  [[ "$p" == "$LEADER" ]] && continue
  q "$p" "SELECT partdist.partwal_truncate_to(partdist.local_partition_for_shard(${GID}), 0);" >/dev/null
  LEFT=$(nrec "$p")
  [[ "$LEFT" == "0" ]] || fail "夹具：${p} 段文件清零后仍有 ${LEFT} 条记录"
done

VICTIM=""
KEEPER=""
for p in "${MEMBER_PORTS[@]}"; do
  [[ "$p" == "$LEADER" ]] && continue
  if [[ -z "$VICTIM" ]]; then VICTIM="$p"; else KEEPER="$p"; fi
done
[[ -n "$VICTIM" && -n "$KEEPER" ]] || fail "夹具：凑不齐 victim/keeper"

# ── 制造缺口：victim 停机期间提案 NENTRIES 条 ────────────────────────
# 3 成员的多数派 = 2，leader + keeper 仍能提交。
node_stop "$VICTIM"

# 分批用 plpgsql 循环提案：plsn 必须**严格升序**（段文件不允许空洞），而把
# 提案塞进一条 SQL 的子查询里，volatile 函数的求值顺序是不保证的。
CHUNK=20
k=1
while (( k <= NENTRIES )); do
  hi=$(( k + CHUNK - 1 )); (( hi > NENTRIES )) && hi=$NENTRIES
  OUT=$(psql_at "$LEADER" -v ON_ERROR_STOP=1 -tAc \
    "DO \$\$ DECLARE i bigint; r bigint; BEGIN
       FOR i IN ${k}..${hi} LOOP
         r := partdist.pg_raft_data_propose(${GID}, i);
         IF r IS NULL OR r <= 0 THEN
           RAISE EXCEPTION 'propose plsn=% 失败', i;
         END IF;
       END LOOP;
     END \$\$;" 2>&1) || fail "提案批次 ${k}..${hi} 失败：$(echo "$OUT" | tail -1)"
  k=$(( hi + 1 ))
done

LLI=$(g "$LEADER" last_log_index)
[[ "$LLI" =~ ^[0-9]+$ && "$LLI" -ge "$NENTRIES" ]] \
  || fail "提案完成后 leader 的 last_log_index=${LLI}，不足 ${NENTRIES}"

# ── B：缺口必须真的超过环容量 ────────────────────────────────────────
node_start "$VICTIM"
for i in $(seq 1 30); do alive "$VICTIM" && break; sleep 1; done
alive "$VICTIM" || fail "victim ${VICTIM} 起不来"

VLLI=$(g "$VICTIM" last_log_index)
[[ "$VLLI" =~ ^[0-9]+$ ]] || VLLI=0
GAP=$(( LLI - VLLI ))
(( GAP > RING_CAPACITY )) \
  || fail "B：缺口只有 ${GAP} 条（leader ${LLI} / victim ${VLLI}），没超过环容量 ${RING_CAPACITY} —— 这样即使把重建摘掉也能靠环内条目追平，用例是假的"

# ── A：靠重建追平 ────────────────────────────────────────────────────
# 追平通道跑在 leader 的 client backend 里（重建要 SPI）。
for i in $(seq 1 60); do
  q "$LEADER" "SELECT partdist.pg_raft_catchup();" >/dev/null
  VLLI=$(g "$VICTIM" last_log_index)
  [[ "$VLLI" == "$LLI" ]] && break
  sleep 1
done
[[ "$VLLI" == "$LLI" ]] \
  || fail "A：victim 60 轮内没追平（${VLLI} / ${LLI}）—— 环外条目取不到，落后成员永远归不了队"

# 字节也必须真的落盘：只对上 raft_log 行数说明不了 parwal 里有内容
VREC=$(nrec "$VICTIM")
[[ "$VREC" == "$LLI" ]] \
  || fail "A：victim 段文件里只有 ${VREC} 条记录，leader 日志已到 ${LLI} —— 行对上了但字节没落盘"

# ── C：victim 上重建结果与自己收到的条目逐条一致 ─────────────────────
BAD=$(q "$VICTIM" \
  "SELECT count(*) FROM partdist.raft_log l
     LEFT JOIN LATERAL partdist.pg_raft_entry_from_parwal(${GID}, l.log_index) e ON true
    WHERE l.group_id = ${GID}
      AND (e.term IS DISTINCT FROM l.term
           OR e.payload IS NULL
           OR e.payload::jsonb IS DISTINCT FROM l.payload);")
[[ "$BAD" == "0" ]] \
  || fail "C：victim 上有 ${BAD} 条重建结果与收到的条目不一致"

cleanup
echo "raft_27 PASS: 数据组环外条目从 parwal 重建（A 超环容量追平且字节落盘 / B 缺口 ${GAP} > 环容量 ${RING_CAPACITY} / C 重建与条目逐条一致）"
exit 0
