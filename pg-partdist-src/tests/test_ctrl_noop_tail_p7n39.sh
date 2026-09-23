#!/usr/bin/env bash
# [宿主机] P7-N39 回归（控制面停在上一任期的尾巴上）。
#
# 病灶：Raft 不允许 leader 直接提交**前任任期**的条目，必须先提交一条本任期的条目把它们带上。
#   数据组在升主前置里做了（P7-N35），**控制面（0 号组）没有人做**。于是换任期之后，上一任期
#   留下的尾巴一直不提交、也就一直不 apply —— 它可能正是一条 OP_PARTITION_PRIMARY（"谁是这个
#   分片的主"），各节点的 partition_map 与路由就此停在旧值。
#   实测（2026-09-23）：0 号组停在 last=1154/commit=1153（1154 是任期 677 的 OP_PARTITION_PRIMARY，
#   leader 已在任期 678），citus_add_node 的控制面写入排在它后面，整条命令挂死 5 分钟以上；
#   手工 propose 一条 OP_NOOP 后 commit 立刻推到 1155、5 个节点全部 applied。
# 修法：控制面 leader 在 tick 里发现"尾巴的任期 < 本任期"时，经本机 libpq 异步补提一条
#   OP_NOOP（tick 里不能做 SPI，与登记/升主前置同源），失败下个 tick 重来。
#
# 断言：连续 ROUNDS 轮整簇重启后，① 每轮 30 s 内 0 号组在**全部节点**上 commit == last；
#   ② 不出现"停在上一任期尾巴"的残留；③ 若期间确有前任尾巴，日志里能看到 P7-N39 两行。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
DATA=/work/pg-cluster-data
ROUNDS="${ROUNDS:-3}"
PASS=0; FAIL=0
DEX() { docker exec -i -u postgres "$CONTAINER" "$@"; }
Q()   { DEX $BIN/psql -h /tmp -p "$1" -U postgres -d postgres -X -Atc "$2" </dev/null 2>/dev/null | tail -1; }
check() {
  local what="$1" got="$2" want="$3"
  if [[ "$got" == "$want" ]]; then PASS=$((PASS+1)); echo "  PASS  $what"
  else FAIL=$((FAIL+1)); echo "  FAIL  $what（实际='$got' 期望='$want'）"; fi
}
mapfile -t DIRS < <(DEX bash -c "ls -d $DATA/*/ | sed 's#/\$##'" </dev/null)
declare -A PORT
for d in "${DIRS[@]}"; do PORT[$d]=$(DEX bash -c "grep -E '^port' $d/postgresql.conf | tr -dc '0-9'" </dev/null); done
PORTS=$(for d in "${DIRS[@]}"; do echo "${PORT[$d]}"; done | sort -n | tr '\n' ' ')
echo "集群：$(wc -w <<<"$PORTS") 个节点（$PORTS）"

LOGMARK=$(DEX bash -c "cat $DATA/*/pg.log | wc -l" </dev/null | tr -dc '0-9')
tails=0
for r in $(seq 1 $ROUNDS); do
  echo "========== [$r] 整簇重启 =========="
  for d in "${DIRS[@]}"; do
    DEX $BIN/pg_ctl -D "$d" -m fast -l "$d/pg.log" -o "-p ${PORT[$d]}" restart -w -t 90 </dev/null >/dev/null 2>&1
  done
  for p in $PORTS; do for t in $(seq 1 40); do [[ "$(Q $p 'SELECT 1')" == 1 ]] && break; sleep 1; done; done
  # 30 s 内所有节点的 0 号组必须 commit == last
  ok=""; laggy=""
  for t in $(seq 1 60); do
    laggy=""
    for p in $PORTS; do
      v=$(Q $p "SELECT CASE WHEN commit_index = last_log_index THEN 'ok' ELSE last_log_index||'/'||commit_index END FROM partdist.pg_raft_group_status() WHERE group_id=0")
      [[ "$v" != ok ]] && laggy="$laggy :$p=$v"
    done
    [[ -z "$laggy" ]] && { ok=ok; break; }
    sleep 0.5
  done
  check "[$r] 30 s 内 0 号组在所有节点上 commit == last" "${ok:-超时$laggy}" "ok"
  # 各节点的任期与提交点最终一致（轮询，不是单次快照 —— 采样时刻不同、心跳在途都会造成瞬时不一致）
  u=9; for t in $(seq 1 60); do
    u=$(for p in $PORTS; do Q $p "SELECT current_term||'/'||commit_index FROM partdist.pg_raft_group_status() WHERE group_id=0"; done | sort -u | wc -l)
    [[ "$u" == 1 ]] && break; sleep 0.5
  done
  check "[$r] 30 s 内各节点的任期/提交点收敛一致" "$u" "1"
  n=$(DEX bash -c "cat $DATA/*/pg.log | tail -n +$((LOGMARK+1)) | grep -c '控制面有上一任期'" </dev/null | tr -dc '0-9')
  tails=$((tails + ${n:-0}))
done

echo "========== 汇总 =========="
echo "  $ROUNDS 轮重启里出现"上一任期尾巴"共 $tails 次"
if [[ $tails -gt 0 ]]; then
  fixed=$(DEX bash -c "cat $DATA/*/pg.log | tail -n +$((LOGMARK+1)) | grep -c '控制面本任期空条目已提交'" </dev/null | tr -dc '0-9')
  check "★ 每一次都被本任期空条目带上了提交点" "$([[ ${fixed:-0} -ge $tails ]] && echo ok)" "ok"
  DEX bash -c "cat $DATA/*/pg.log | tail -n +$((LOGMARK+1)) | grep -E '控制面有上一任期|控制面本任期空条目已提交' | tail -4" </dev/null | sed 's/^/    /'
else
  echo "  （本轮没赶上前任尾巴；上面的 commit==last 断言仍是有效的不变式）"
fi
crash=$(DEX bash -c "cat $DATA/*/pg.log | tail -n +$((LOGMARK+1)) | grep -cE 'signal 11|signal 6|PANIC'" </dev/null | tr -dc '0-9')
check "无节点崩溃" "${crash:-0}" "0"
echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N39 回归：通过" || echo "P7-N39 回归：存在 FAIL"
exit $(( FAIL > 0 ))
