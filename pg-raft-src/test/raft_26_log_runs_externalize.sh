#!/usr/bin/env bash
# raft_26: 数据组日志外部化 E1 —— 段式边界表 + 从 parwal 重建条目（计划文档 §11.10）
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_26_log_runs_externalize.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# ── 被验收的缺口 ─────────────────────────────────────────────────────
# 数据组的每条 Raft 条目都恰好对应一条 parwal 记录，条目载荷那串描述符
# {partition_lsn, orig_lsn, rmid, info, xid, nbytes, flags} 与 PartWALRecord 的
# 头部字段逐个对应 —— 也就是说日志内容**已经在段文件里躺着**，partdist.raft_log
# 里那份是纯冗余。挡在"删掉冗余"前面的只有两样：term，以及 index↔plsn 的对应
# 关系（**不是**恒等关系：data_propose_one 的 plsn 由调用方指定，index 由
# log_append_locked 独立分配，raft_13 里两者都等于 1 只是碰巧）。
#
# E1 补上的就是这两样：partdist.raft_log_runs 用"段"记 (start_index, start_plsn,
# term)，partdist.pg_raft_entry_from_parwal() 按段反查并从记录头部重建载荷。
# 本期**不删**任何既有写入，用重建结果与 raft_log 里存的那份逐条对照。
#
# ── 判据 ─────────────────────────────────────────────────────────────
#   A. **重建等价**：leader 上该组的每一条 raft_log，pg_raft_entry_from_parwal()
#      重回来的 term 相等、payload 语义相等（比 jsonb 不比文本：raft_log.payload
#      是 jsonb，读回来的文本被规范化过，逐字节比会假失败 —— 这一条是 2026-08-05
#      在冲突判定上踩过的同一个坑）。
#   B. **run 真的是"段"**：连续同任期的 N 条条目只占 1 行。这一条是整个设计的
#      立足点 —— 若退化成逐条一行，那只是把 raft_log 换了张表，什么都没省。
#   C. **跳号提案失败且不留痕**：段文件不允许空洞（AppendPartWALRecordAt 对
#      `expected > last + 1` 直接 ERROR），所以跳号提案必然凑不齐多数派而被丢弃。
#      判据是它**什么都不留下** —— run 写在复制之前，回滚不清它就会留下一个指向
#      不存在条目的段起点，之后所有 >= 它的 index 都按错的 plsn 重建。
#   D. **follower 侧同样成立**：run 由 persist_log_entry_sql 统一维护，leader 的
#      propose 与 follower 的 append 都经过它。
#      顺带确认：follower 的 plsn 天然连续，它的 run 行数与 leader 一致。
#   E. **换届另起一段**：停掉 leader，剩下两个成员（3 成员的多数派=2）选出新
#      leader，在新任期里再提案一条 —— run 增加一行，且新旧两段的重建都等价。
#      这一条是 run 存在的首要理由（term 只能按段记，不可能从 parwal 头部读出来）。
#
# 真对照（离线执行，不在套件里）：
#   ① data_run_record_spi() 直接 return（不维护 run）重编 .so ⇒ A 段确定性失败
#      （查不到 run，重建返回 NULL）。
#   ② 把【现有 run 已覆盖且对得上就无事可做】那个判断去掉、每条都 INSERT 一行
#      ⇒ A/D/E 照过，**B 段确定性失败**（run 行数 == 条目数）。
#   ③ in-build 对照：delete_log_entry_sql 不清 run 的版本（即本次落地前的写法）
#      ⇒ **C 段确定性失败**（跳号提案回滚后留下一行孤儿 run）。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
TABLE=raft26_demo
MEMBER_PORTS=(5433 5434 5435)
MEMBER_IDS="ARRAY[2,3,4]"

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
    # 用 reset 而非 drop：drop 只作用本节点，对端会把组连同旧 term 传回来，
    # 残留的数据组会一直参与 tick，拖慢后续依赖时序的控制面用例（raft_13 的老教训）。
    q "$port" "SELECT partdist.pg_raft_group_reset();" >/dev/null
    [[ -n "$GID" ]] && q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;" >/dev/null
  done
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TABLE};" >/dev/null
}
fail() { cleanup; echo "raft_26 FAIL: $1"; exit 1; }

g() { q "$1" "SELECT $2 FROM partdist.pg_raft_group_status() WHERE group_id = ${GID};"; }
n_runs() { q "$1" "SELECT count(*) FROM partdist.raft_log_runs WHERE group_id = ${GID};"; }
n_entries() { q "$1" "SELECT count(*) FROM partdist.raft_log WHERE group_id = ${GID};"; }

# 重建与存档不一致的条目数（0 = 全部等价）。这里比的是 jsonb，不是文本 ——
# raft_log.payload 是 jsonb，取出来的文本已被规范化（键序、冒号后空格），
# 与 format() 拼出来的原始 JSON 逐字节不等；比文本会假失败。
mismatches() {
  q "$1" "SELECT count(*) FROM partdist.raft_log l
            LEFT JOIN LATERAL partdist.pg_raft_entry_from_parwal(${GID}, l.log_index) e ON true
           WHERE l.group_id = ${GID}
             AND (e.term IS DISTINCT FROM l.term
                  OR e.payload IS NULL
                  OR e.payload::jsonb IS DISTINCT FROM l.payload);"
}

find_leader() {   # $1=最长等待秒数（缺省 30）
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

propose() {   # $1=leader 端口 $2=plsn；返回 log index（0/空=失败）
  q "$1" "SELECT partdist.pg_raft_data_propose(${GID}, $2);"
}

cleanup

# ── 夹具 ─────────────────────────────────────────────────────────────
# reference 表的同一 shardid 复制在三个 worker 上，天然就是一个分区组的成员集。
# 逐条 INSERT（不是一条 generate_series）：一个事务只落一条 parwal 记录，
# 而本用例要按真实存在的 plsn 逐个提案，还要留出一个"故意跳过"的编号。
q "$BASE_PORT" "SET citus.enable_ddl_propagation=on;
  DROP TABLE IF EXISTS ${TABLE};
  CREATE TABLE ${TABLE}(id int primary key, v text);
  SELECT create_reference_table('${TABLE}');" >/dev/null
for k in $(seq 1 14); do
  q "$BASE_PORT" "INSERT INTO ${TABLE} VALUES (${k}, repeat('x', 40));" >/dev/null
done

for p in "$BASE_PORT" "${MEMBER_PORTS[@]}"; do
  q "$p" "SELECT partdist.rebuild_shard_identity();" >/dev/null
done

GID=$(q "$BASE_PORT" "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='${TABLE}'::regclass;")
[[ "$GID" =~ ^[0-9]+$ ]] || fail "夹具：拿不到 ${TABLE} 的 shardid"

for p in "${MEMBER_PORTS[@]}"; do
  q "$p" "SELECT partdist.pg_raft_group_create(${GID}, ${MEMBER_IDS});" >/dev/null
done

LEADER=$(find_leader) || fail "夹具：30s 内该数据组没有 leader"

# 非 leader 成员模拟"纯 secondary"：reference 表在每个节点都是本地主写，它们
# pg_parwal/<oid>/ 里已有自己 demux 产出的 plsn 1..N，会与 leader 下发的编号
# 相撞（raft_13 的老教训）。真实架构下一个节点对某分区要么 primary 要么 secondary。
for p in "${MEMBER_PORTS[@]}"; do
  [[ "$p" == "$LEADER" ]] && continue
  q "$p" "SELECT partdist.partwal_truncate_to(partdist.local_partition_for_shard(${GID}), 0);" >/dev/null
  LEFT=$(q "$p" "SELECT count(*) FROM partdist.raft_log_runs WHERE group_id = ${GID};")
  [[ "$LEFT" == "0" ]] || fail "夹具：${p} 上组 ${GID} 还留着 ${LEFT} 行 run，起点不干净"
done

BASE_RUNS=$(n_runs "$LEADER")
[[ "$BASE_RUNS" == "0" ]] || fail "夹具：leader ${LEADER} 上组 ${GID} 起步就有 ${BASE_RUNS} 行 run"

# ── A/B：连续同任期的若干条 ──────────────────────────────────────────
# plsn 不能写死：一个事务落几条 parwal 记录由 demux 决定，写死就会提案到不存在
# 的编号（首版实测踩到——跳号那一条打到 plsn=10，而段文件里只到 9，返回 0）。
# 按 leader 段文件里**真实存在**的编号来，末尾留两个：一个故意跳过、一个用于 C。
mapfile -t PLSNS < <(q "$LEADER" \
  "SELECT partition_lsn FROM partdist.read_all_headers(
       partdist.local_partition_for_shard(${GID})) ORDER BY 1;")
M=${#PLSNS[@]}
(( M >= 5 )) || fail "夹具：leader 段文件里只有 ${M} 条记录，不足以构造【连续段 + 跳号】（需要 >= 5）"

N=$(( M - 2 ))
# 前 N 个必须真的连续，否则 B 段判"1 行 run"本身就不成立（那是夹具不成立，不是功能坏）
for k in $(seq 1 $(( N - 1 ))); do
  (( PLSNS[k] == PLSNS[k-1] + 1 )) \
    || fail "夹具：段文件里的 plsn 本身就不连续（${PLSNS[$((k-1))]} → ${PLSNS[$k]}），无法构造连续段"
done
for k in $(seq 0 $(( N - 1 ))); do
  IDX=$(propose "$LEADER" "${PLSNS[$k]}")
  [[ "$IDX" =~ ^[0-9]+$ && "$IDX" -gt 0 ]] || fail "A：提案 plsn=${PLSNS[$k]} 失败（返回 '${IDX}'）"
done

ENT=$(n_entries "$LEADER")
[[ "$ENT" == "$N" ]] || fail "A：leader 上该组条目数 ${ENT} != ${N}"
[[ "$N" -ge 3 ]] || fail "夹具：连续段只有 ${N} 条，太短，判不出【段】与【逐条】的区别"

BAD=$(mismatches "$LEADER")
[[ "$BAD" == "0" ]] || fail "A：leader 上有 ${BAD} 条重建结果与 raft_log 不一致"

RUNS=$(n_runs "$LEADER")
[[ "$RUNS" == "1" ]] \
  || fail "B：${N} 条连续同任期条目占了 ${RUNS} 行 run（应为 1 —— 退化成逐条一行的话，这张表就只是换了个名字的 raft_log）"

# ── C：plsn 跳号提案必须失败，且不留痕 ───────────────────────────────
# **首版把这一段写成了"跳号会另起一个 run"，那是个不存在的状态**（实测提案直接
# 返回 0）。原因在 AppendPartWALRecordAt：`expected > last + 1` 是硬 ERROR
# （"物理回放要求流完整有序无 gap"），follower 落不了盘就不 ack，leader 凑不齐
# 多数派 ⇒ 该条目被 discard_uncommitted_entry 丢弃。也就是说**已提交条目的 plsn
# 天然连续**，段边界只可能来自换届（见 E 段）。
#
# 改判真实存在的性质：跳号提案必须失败，且**什么都不留下** —— run 是在
# persist_log_entry_sql 里先于复制就写下去的，回滚路径不删它就会留下一个指向
# 不存在条目的段起点，此后所有 >= 它的 index 都按错的 plsn 重建。
# 这一条有 **in-build 对照**：delete_log_entry_sql 不清 run 的版本确定性失败。
GAP_PLSN=${PLSNS[$(( N + 1 ))]}
ENT_BEFORE=$(n_entries "$LEADER")
RUNS_BEFORE_GAP=$(n_runs "$LEADER")

IDX=$(propose "$LEADER" "$GAP_PLSN")
[[ "$IDX" == "0" ]] \
  || fail "C：跳号提案 plsn=${GAP_PLSN}（跳过 ${PLSNS[$N]}）居然成功了（返回 '${IDX}'）—— 段文件不允许空洞，这条本该失败"

[[ "$(n_entries "$LEADER")" == "$ENT_BEFORE" ]] \
  || fail "C：跳号提案失败后条目数从 ${ENT_BEFORE} 变成 $(n_entries "$LEADER")"
[[ "$(n_runs "$LEADER")" == "$RUNS_BEFORE_GAP" ]] \
  || fail "C：跳号提案失败后 run 行数从 ${RUNS_BEFORE_GAP} 变成 $(n_runs "$LEADER")——留下了指向不存在条目的段起点"

BAD=$(mismatches "$LEADER")
[[ "$BAD" == "0" ]] || fail "C：跳号提案失败后 leader 上有 ${BAD} 条重建结果与 raft_log 不一致"

# ── D：follower 侧同样成立 ───────────────────────────────────────────
FOLLOWER=""
for p in "${MEMBER_PORTS[@]}"; do
  [[ "$p" != "$LEADER" ]] && FOLLOWER="$p" && break
done
[[ -n "$FOLLOWER" ]] || fail "D：找不到 follower"

for i in $(seq 1 20); do
  [[ "$(n_entries "$FOLLOWER")" == "$(n_entries "$LEADER")" ]] && break
  sleep 1
done
[[ "$(n_entries "$FOLLOWER")" == "$(n_entries "$LEADER")" ]] \
  || fail "D：follower ${FOLLOWER} 20s 内没跟上（$(n_entries "$FOLLOWER") / $(n_entries "$LEADER")）"

BAD=$(mismatches "$FOLLOWER")
[[ "$BAD" == "0" ]] || fail "D：follower 上有 ${BAD} 条重建结果与 raft_log 不一致"

FRUNS=$(n_runs "$FOLLOWER")
LRUNS=$(n_runs "$LEADER")
[[ "$FRUNS" == "$LRUNS" ]] \
  || fail "D：follower 上 run 行数为 ${FRUNS}，leader 上是 ${LRUNS}（两边的 plsn 都连续、任期也相同，行数必须一致）"

# ── E：换届另起一段 ──────────────────────────────────────────────────
# 停掉 leader，剩下两个成员仍是多数派（3 成员的多数派 = 2），会在更高任期里
# 选出新 leader。新 leader 的段文件已在夹具里清零、随后由复制灌进上面提案过的
# 那些 plsn，所以它能提案一个自己确实持有的编号（取 PLSNS[1]）——重建照样要对得上。
OLD_LEADER="$LEADER"
OLD_TERM=$(g "$LEADER" current_term)
node_stop "$OLD_LEADER"

NEW_LEADER=""
for i in $(seq 1 45); do
  for p in "${MEMBER_PORTS[@]}"; do
    [[ "$p" == "$OLD_LEADER" ]] && continue
    alive "$p" || continue
    [[ "$(g "$p" state)" == "leader" ]] && { NEW_LEADER="$p"; break; }
  done
  [[ -n "$NEW_LEADER" ]] && break
  sleep 1
done
[[ -n "$NEW_LEADER" ]] || fail "E：停掉 leader 后 45s 内没有新 leader"

NEW_TERM=$(g "$NEW_LEADER" current_term)
[[ "$NEW_TERM" -gt "$OLD_TERM" ]] \
  || fail "E：新 leader 的任期 ${NEW_TERM} 没有高于旧任期 ${OLD_TERM}，构造无效"

RUNS_BEFORE=$(n_runs "$NEW_LEADER")
IDX=$(propose "$NEW_LEADER" "${PLSNS[1]}")
[[ "$IDX" =~ ^[0-9]+$ && "$IDX" -gt 0 ]] || fail "E：新 leader 提案失败（返回 '${IDX}'）"

RUNS_AFTER=$(n_runs "$NEW_LEADER")
[[ "$RUNS_AFTER" -gt "$RUNS_BEFORE" ]] \
  || fail "E：换届后提案没有新开一段（run 行数 ${RUNS_BEFORE} → ${RUNS_AFTER}）—— term 只能按段记，不断段就等于把新任期的条目按旧 term 重建"

BAD=$(mismatches "$NEW_LEADER")
[[ "$BAD" == "0" ]] || fail "E：换届后新 leader 上有 ${BAD} 条重建结果与 raft_log 不一致"

cleanup
echo "raft_26 PASS: 数据组日志外部化 E1（A 重建等价 / B run 是段不是逐条 / C 跳号提案失败且不留痕 / D follower 侧成立 / E 换届另起一段）"
exit 0
