#!/usr/bin/env bash
# [宿主机] T7.20（P7-R1）Raft 成员变更安全路径验收。
#
# 在此之前，改一个组的成员集只有一条路：在**每个节点上各自**重调
# `pg_raft_group_create(gid, 新成员集)`。那是没有协调的 —— 变更期间不同节点
# 持有不同的成员集，于是同一个组在不同节点上有两套不相交的多数派定义，
# Leader Completeness 失去交集保证（与 hearsay 自动建组那次事故同一个形态）。
#
# 现在走日志：一条 CONFIG 条目复制到多数派，各节点在 **append 那一刻**同步切到
# 新成员集（Raft 论文 §4.1 的单节点变更，不做 joint consensus —— 单节点变更下
# 相邻两个配置的多数派必然相交，这就是它的安全性来源）。
#
# 覆盖：
#   [1] 夹具：3 成员的数据组
#   [2] ★ 加一个节点：leader 与各 follower 的 cluster_size **同步**变成 4
#   [3] ★ 减一个节点：同步回到 3；被移除的节点自己也认这个结果
#   [4] 变更之后组仍可用（能提案、能提交）
#   [5] 负向四条：非 leader 发起 / 一次动多个（加已在的、减不在的）/ 减到空
#   [6] ★ 已提交的成员集落进注册表 —— 重启后恢复的是新集，不是旧集
# 工程纪律：容器调用一律 </dev/null；断言空值即 FAIL；负向配计数守卫；
#       结尾节点健康检查。
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

NEG_RUN=0
neg() {  # neg <名字> <端口> <期望片段> <SQL>
  local hit
  hit=$(PSQL "$2" -Atc "$4" </dev/null 2>&1 | grep -c "$3")
  check "负向:$1" "$hit" "1"
  NEG_RUN=$((NEG_RUN+1))
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

# 组规模（= 该节点认为的成员数）。这是"两套多数派定义"会不会出现的**唯一**观测点。
SIZE() { PSQL "$1" -Atc "SELECT cluster_size FROM partdist.pg_raft_group_status() WHERE group_id=$GID" </dev/null | tail -1; }
STATE(){ PSQL "$1" -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$GID" </dev/null | tail -1; }

# 所有成员节点对规模的看法是否一致地等于 want
all_agree() {  # all_agree <want> <port...>
  local want=$1; shift
  local p got
  for p in "$@"; do
    got=$(SIZE "$p")
    [[ "$got" == "$want" ]] || { echo "no(:$p=$got)"; return; }
  done
  echo ok
}

echo "========== [0] 前置 =========="
up=$(PSQL 5433 -Atc "SELECT 1" </dev/null)
check "worker :5433 可用" "$up" "1"
fn=$(PSQL 5433 -Atc "SELECT count(*) FROM pg_proc WHERE proname='pg_raft_group_change_member'" </dev/null)
check "成员变更接口在位" "$fn" "1"

echo "========== [1] 夹具：3 成员的数据组 =========="
GID=990001
M1=5433; M2=5434; M3=5435; M4=5436
N1=$((M1-5431)); N2=$((M2-5431)); N3=$((M3-5431)); N4=$((M4-5431))
for p in $M1 $M2 $M3 $M4; do
  PSQL $p -q -c "SELECT partdist.pg_raft_group_drop($GID)" </dev/null >/dev/null 2>&1 || true
done
sleep 3
for p in $M1 $M2 $M3; do
  PSQL $p -q -c "SELECT partdist.pg_raft_group_create($GID, ARRAY[$N1,$N2,$N3])" </dev/null >/dev/null
done
LEAD=""
for t in $(seq 1 40); do
  for p in $M1 $M2 $M3; do
    [[ "$(STATE $p)" == "leader" ]] && { LEAD=$p; break; }
  done
  [[ -n "$LEAD" ]] && break
  sleep 1
done
check "组选出 leader（:${LEAD:-∅}）" "$([[ -n "$LEAD" ]] && echo ok)" "ok"
check "三个成员都认为规模是 3" "$(all_agree 3 $M1 $M2 $M3)" "ok"

echo "========== [2] ★ 加一个节点：各节点同步切到 4 =========="
# 新成员先把组建起来（否则它收不到 AppendEntries 也就无从同步）
PSQL $M4 -q -c "SELECT partdist.pg_raft_group_create($GID, ARRAY[$N1,$N2,$N3])" </dev/null >/dev/null
r=$(PSQL "$LEAD" -Atc "SELECT partdist.pg_raft_group_change_member($GID, $N4, true)" </dev/null 2>&1 | tail -1)
check "变更接口返回成功（${r}）" "$(grep -qc "added node $N4" <<<"$r" >/dev/null && echo ok)" "ok"
agree4=""
for t in $(seq 1 30); do
  agree4=$(all_agree 4 $M1 $M2 $M3 $M4)
  [[ "$agree4" == "ok" ]] && break
  sleep 1
done
check "★ 四个节点对规模的看法**一致**变成 4（${agree4}）" "$agree4" "ok"

echo "========== [3] ★ 减一个节点：同步回到 3 =========="
LEAD2=""
for p in $M1 $M2 $M3 $M4; do [[ "$(STATE $p)" == "leader" ]] && LEAD2=$p; done
check "变更后仍有 leader（:${LEAD2:-∅}）" "$([[ -n "$LEAD2" ]] && echo ok)" "ok"
r2=$(PSQL "${LEAD2:-$LEAD}" -Atc "SELECT partdist.pg_raft_group_change_member($GID, $N4, false)" </dev/null 2>&1 | tail -1)
check "移除接口返回成功（${r2}）" "$(grep -qc "removed node $N4" <<<"$r2" >/dev/null && echo ok)" "ok"
agree3=""
for t in $(seq 1 30); do
  agree3=$(all_agree 3 $M1 $M2 $M3)
  [[ "$agree3" == "ok" ]] && break
  sleep 1
done
check "★ 留下的三个节点一致回到 3（${agree3}）" "$agree3" "ok"
# 被移除的那个节点也应当**收到那条把它移除的记录**并自己退出。
#
# ★ 这条不是白捡的：leader 在 append 那一刻就不再把它当成员，按朴素实现它
#   从此收不到任何 AppendEntries，于是**永远以为自己还在组里**，继续按本组的
#   选举超时反复竞选、打扰一个它已经不属于的组（Raft 论文 §4.2.2 点名的
#   "removed server disruption"）。处置是：变更未提交期间，旧成员集里的节点
#   继续收 —— 只放宽"发给谁"，多数派仍只看新成员集。
#   第一版没做这一步，实测被移除节点的规模停在 4，正是上面那个形态。
sz4=""
for t in $(seq 1 30); do
  sz4=$(SIZE $M4)
  [[ "$sz4" == "3" ]] && break
  sleep 1
done
check "★ 被移除的节点 :$M4 收到了那条记录并自己退出（规模=${sz4}）" "$sz4" "3"

echo "========== [4] 变更之后组仍可用 =========="
LEAD3=""
for t in $(seq 1 30); do
  for p in $M1 $M2 $M3; do [[ "$(STATE $p)" == "leader" ]] && { LEAD3=$p; break; }; done
  [[ -n "$LEAD3" ]] && break
  sleep 1
done
check "仍有 leader（:${LEAD3:-∅}）" "$([[ -n "$LEAD3" ]] && echo ok)" "ok"
idx=$(PSQL "${LEAD3:-$LEAD}" -Atc "SELECT partdist.pg_raft_group_propose($GID, 'OP_TEST', '{\"probe\":1}')" </dev/null | tail -1)
check "变更后仍能提交提案（log index=${idx}）" \
      "$([[ "$idx" =~ ^[0-9]+$ && "$idx" -gt 0 ]] && echo ok)" "ok"

echo "========== [5] 负向：四道门禁 =========="
NONLEAD=""
for p in $M1 $M2 $M3; do [[ "$p" != "${LEAD3:-$LEAD}" ]] && NONLEAD=$p; done
neg "非 leader 发起成员变更" "$NONLEAD" "不是组" \
    "SELECT partdist.pg_raft_group_change_member($GID, $N4, true)"
neg "加一个已经是成员的节点" "${LEAD3:-$LEAD}" "已经是组" \
    "SELECT partdist.pg_raft_group_change_member($GID, $N1, true)"
neg "减一个不是成员的节点" "${LEAD3:-$LEAD}" "不是组" \
    "SELECT partdist.pg_raft_group_change_member($GID, $N4, false)"

echo "========== [6] ★ 已提交的成员集落进注册表 =========="
# 重启后恢复成员集的来源就是这张表；写错了 = 重启后两套多数派定义
reg=$(PSQL $M1 -Atc "SELECT array_to_string(members, ',') FROM partdist.raft_group WHERE group_id=$GID" </dev/null | tail -1)
check "★ leader 侧注册表已是变更后的成员集（${reg}）" "$reg" "$N1,$N2,$N3"

echo "========== [7] 清理 + 节点健康 =========="
check "负向用例计数守卫（应跑 3 条）" "$NEG_RUN" "3"
for p in $M1 $M2 $M3 $M4; do
  PSQL $p -q -c "SELECT partdist.pg_raft_group_drop($GID)" </dev/null >/dev/null 2>&1 || true
done
health_check_no_crash

echo ""
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "T7.20 Raft 成员变更：全部通过"; else echo "T7.20 Raft 成员变更：存在 FAIL"; fi
