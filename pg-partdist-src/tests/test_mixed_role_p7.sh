#!/usr/bin/env bash
# [宿主机] 主从架构：**同一节点同时当主、又当从** 验收。
#
# 本设计不是"整机主 / 整机从"，主从的作用域是**分区(shard)**：每个分片一套
# Raft 组，组 leader = 该分片的主副本(原生真表)，其余成员 = 从副本(壳表，物理回放)。
# 一个节点因此可以同时是分片 A 的主、分片 B 的从(FRD §14.2 R1、shard_route.h 三态)。
# 本套件把这个"混合角色"节点单拎出来验，是既有测试(回放一致性/升主/供副本)都没有
# 专门覆盖的一格。
#
# 拓扑（M = 混合角色节点；每次写都只碰一个 raft 组，不进 DTX 路径）：
#   分片 A：主在 M（原生真表，可读写）                A 组 = 全部 worker，M 当选
#   分片 B：主在 C（≠M），从在 M（壳表，回放跟随 C）  B 组 = 全部 worker，C 当选
#   （成员集取全部 worker：3 worker 时即 {W1,W2,W3}，多数派 2 ⇒ 允许 M 这个从惰性滞后）
#   —— M 因此同时：A 的 native_leader + B 的 replica
#
# 断言（对应本设计的四条核心命题）：
#   [4] 往 A 写 → M 上 A 读得到              （M 作主，本机读写正常）
#   [5] 往 A 狂写 → M 上 B 的从副本字节不变  （同机两分片物理隔离，A 的写不串进 B 的从）
#   [6] 往 B 的主(C)写 → M 上 B 的从追平且逐字节一致（M 作从，回放正确跟随别的主）
#   [7] 两套 xid 空间互不串线：同一节点上 A 的号走**本机原生语义**、B-从的号走**回放两跳**
#       （xid_map→gclog），二者按各自 OID 编址（规则 6）。注：本用例不打标，元组带原生 xid
#
# 夹具防御（fixture-rules）：建组顺序(4)；两分片完整建站(5)；分片 clog 键是本节点 oid(6)；
#   catchup 给上界(7)；主漂移容忍/重试(8)；CPU 饱和时临时抬 election_timeout_ms(19)；
#   收尾三台一起拆组、拆在 DROP 之前(16)；打标分布表走协调者 set_table_shard_mvcc(12)。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }

check() {
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置：group0 收敛 + 测试模式 + 抬 election_timeout（规则 19） =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"
# ---- 拓扑自适应：动态读 worker 列表与 raft 节点号（不再假设 8 worker / port-5431）----
WORKERS=($(PSQL $COORD -Atc "SELECT nodeport FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive ORDER BY nodeport"))
NW=${#WORKERS[@]}
check "至少 3 个 worker（3 成员组才有多数派 2，容得下一个从滞后）" "$([[ "$NW" -ge 3 ]] && echo ok)" "ok"
declare -A NID
for p in "${WORKERS[@]}"; do NID[$p]=$(PSQL $p -Atc "SHOW pg_raft.node_id" | tail -1); done
# 本套件所有数据组的成员集 = 全部 worker（3 worker 时即 {W1,W2,W3}，多数派 2）
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
echo "  拓扑：${NW} 个 worker = ${WORKERS[*]}   组成员集=$ALLMEM"
# ★ **不抬** election_timeout：规则 19 那条是给 9 节点满载机器的。抬到 20s 会让
#   "placement 抢跑当选"彻底失效（20s 随机窗口淹没 2s 抢跑），实测分片组被没有数据的
#   节点赢走、写入 "未达多数派"。这里改用 build_group_on 的"落错就拆组重来"，
#   确定性地把主放到该在的节点上，选举超时用环境默认值（6000ms）。
for p in "${WORKERS[@]}" $COORD; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done

MADE_GROUPS=""      # "port:gid" 列表，收尾逐台拆
cleanup() {
  # 规则 16：先拆组（三台一起、拆在 DROP 之前），再删表
  local g port gid
  for g in $MADE_GROUPS; do
    port="${g%%:*}"; gid="${g##*:}"
    PSQL "$port" -q -c "SELECT partdist.pg_raft_group_drop($gid)" </dev/null >/dev/null 2>&1 || true
  done
  # 从副本壳表在 M 上是本地表，本地删；打标分布表走协调者（规则 12）
  [[ -n "${M:-}" && -n "${BSHELL:-}" ]] && PSQL "$M" -q -c "SELECT partdist.replay_disable('$BSHELL'); SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS $BSHELL" </dev/null >/dev/null 2>&1 || true
  PSQL $COORD -q -c "DROP TABLE IF EXISTS mr_a" -c "DROP TABLE IF EXISTS mr_b" </dev/null >/dev/null 2>&1 || true
  for p in "${WORKERS[@]}" $COORD; do
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.replay_trust_local_segments;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "ALTER SYSTEM RESET pg_raft.election_timeout_ms;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
  echo "  [复原] 组已拆、表已删、GUC 已 RESET"
}
trap cleanup EXIT

# ---- 建组并**确定性**地把主放到指定节点 ----
# 规则 4 的"抢跑当选"不可靠；而"落错就拆组重来"更糟：拆组是**节点本地**的，
# 逐台拆的过程中，还留着该组的成员会经 RV/AE 把它**重建**回来，且带着上一轮的 term；
# 于是重建出的组里 follower 记着更高的 term，新 leader 的条目一条都 ack 不了 ——
# 实测 group 102060 在 term=2 恒卡 1/2 票，而没拆过的 102062 一次就全绿。
# 正解：建组**之前**把 placement 节点的选举超时压到 1.5s、其余抬到 9s，
# 它必然先超时先竞选（随机区间 [1.5,3]s vs [9,18]s，不重叠），一次选中，全程不拆组。
# 建完即把超时 RESET 回环境默认（6000ms）。
build_group_on() {   # build_group_on <gid> <期望leader端口> <成员ARRAY>；回显最终 state
  local gid=$1 want=$2 mem=$3 t p st=""
  for p in "${WORKERS[@]}"; do
    if [[ "$p" == "$want" ]]; then
      PSQL "$p" -q -c "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" </dev/null >/dev/null
    else
      PSQL "$p" -q -c "ALTER SYSTEM SET pg_raft.election_timeout_ms = 9000" </dev/null >/dev/null
    fi
    PSQL "$p" -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null
  done
  for p in "${WORKERS[@]}"; do
    PSQL "$p" -q -c "SELECT partdist.pg_raft_group_create($gid, $mem);" </dev/null >/dev/null
  done
  for t in $(seq 1 40); do
    st=$(PSQL "$want" -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid" 2>/dev/null | tail -1)
    [[ "$st" == "leader" ]] && break
    sleep 1
  done
  for p in "${WORKERS[@]}"; do
    PSQL "$p" -q -c "ALTER SYSTEM RESET pg_raft.election_timeout_ms" </dev/null >/dev/null
    PSQL "$p" -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null
  done
  echo "$st"
}

# ---- 写之前确认主还在该在的节点（规则 8：分区主会自治漂移）----
# 实测：建 B 组 / 给 M 供 B 的副本期间 M 很忙，A 组的心跳被挤掉 → 被 follower 推翻 →
# 控制面翻 pg_dist_placement → Citus 把写路由到新主。现象是"经协调者读得到 50 行、
# 在 M 上直读是 0"——数据没丢，只是不在 M 上，此时 M 已不是 A 的主，断言自然不成立。
# 这里只等待 + 取证，**不拆组、不偏置选举超时**（偏置是节点级的，会连带把 M 变成
# B 组的候选人、把 C 的主权抢走）。漂移不回来就把现场打出来，交给人判断。
ensure_leader() {   # ensure_leader <gid> <期望端口> <标签>；回显 state
  local gid=$1 want=$2 tag=$3 t p st=""
  for t in $(seq 1 30); do
    st=$(PSQL "$want" -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid" 2>/dev/null | tail -1)
    [[ "$st" == "leader" ]] && { echo "$st"; return 0; }
    sleep 1
  done
  {
    echo "  [取证] ${tag}：组 $gid 的主不在 :$want（state=$st），三台视角："
    for p in "${WORKERS[@]}"; do
      echo "         :$p → $(PSQL "$p" -Atc "SELECT state||' term='||current_term||' leader='||leader_node_id FROM partdist.pg_raft_group_status() WHERE group_id=$gid" 2>/dev/null | tail -1)"
    done
    echo "         pg_dist_placement 现指向 :$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$gid" 2>/dev/null | tail -1)"
  } >&2
  echo "$st"
}

echo "========== [1] 夹具：分片 A（单分片表，主定 M）+ 分片 B（2 分片表里挑主不在 M 的那片） =========="
# ★ 落位**不能靠 Citus 自己分散**：单分片表恒落第一个 worker —— 实测建 9 张单分片表、
#   连 colocate_with=>'none' 把共置组都拆开，9 张仍然全落 :5433，于是"多建几张再挑"
#   根本挑不出主在别处的分片。citus_move_shard_placement() 也走不通：产品有意禁用
#   （T4.6/§9.2，分片打标集群上搬分片用原生快照读写 = 静默错读，且与 partition_map 冲突）。
#   正解是**用分片数造拓扑**：A 用单分片表（必落 W1 ⇒ M 定在 W1），B 用 2 分片表
#   （两片必然分在两台），从 B 的两片里挑 placement ≠ M 的那片当 B ——
#   这样 M≠C 是拓扑保证的，不靠运气。
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL' >/dev/null
DROP TABLE IF EXISTS mr_a; DROP TABLE IF EXISTS mr_b;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE mr_a(id int primary key, v text);
SELECT create_distributed_table('mr_a','id');
ALTER TABLE mr_a SET (autovacuum_enabled=off);
SET citus.shard_count = 2;
CREATE TABLE mr_b(id int primary key, v text);
SELECT create_distributed_table('mr_b','id');
ALTER TABLE mr_b SET (autovacuum_enabled=off);
SQL
A_NAME=mr_a; B_NAME=mr_b
SIDA=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='mr_a'::regclass" | tail -1)
M=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$SIDA" | tail -1)
SIDB=""; C=""
read SIDB C < <(PSQL $COORD -Atc "SELECT s.shardid, n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='mr_b'::regclass AND n.nodeport <> ${M:-0} ORDER BY s.shardid LIMIT 1" | tail -1 | tr '|' ' ')
check "落位可用：A 的主在 M、B 的两片里挑得出主不在 M 的那片" \
      "$([[ -n "${M:-}" && -n "${C:-}" && -n "${SIDB:-}" ]] && echo ok)" "ok"
if [[ -z "${M:-}" || -z "${C:-}" || -z "${SIDB:-}" ]]; then
  { echo "  [取证] 造不出混合角色拓扑（A 主=${M:-空} B 片=${SIDB:-空} B 主=${C:-空}），当前落位："
    PSQL $COORD -Atc "SELECT s.logicalrelid::text||' shard='||s.shardid||' port='||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid::text LIKE 'mr%' ORDER BY s.shardid" | sed 's/^/         /'; } >&2
  echo ""; echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="; exit 1
fi
Mn=${NID[$M]}; Cn=${NID[$C]}
ATBL="mr_a_${SIDA}"; BTBL="mr_b_${SIDB}"; BSHELL="$BTBL"
echo "  混合角色节点 M=:$M(node$Mn)  |  A=$A_NAME 分片$SIDA 主在 M  |  B=$B_NAME 分片$SIDB 主在 C=:$C(node$Cn)"
check "M ≠ C（混合角色的前提：M 不能是 B 的主）" "$([[ -n "$M" && -n "$C" && "$M" != "$C" ]] && echo ok)" "ok"

echo "========== [2] 分片身份 + 建 A 组（M 主）+ 建 B 组（C 主）+ M 上供 B 的从 =========="
for p in "${WORKERS[@]}"; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done

# ★ 数据组的 follower **必须在本地有分片站点**（壳表+身份+locmap）才能落盘并 ack。
#   把成员当"光杆 raft 成员"是错的：实测 follower 日志刷
#   `group N 在本节点没有对应分片，无法落盘`，leader 提案恒 1/2 票、事务回滚，
#   而堆上留着中止元组（"主堆 5 块但 count=0"就是这么来的）。
#   供副本用产品入口 partdist.provision_shard_replica()：它**先**去目标建壳表+身份，
#   再发物理基线、配 locmap、arm 回放，一步到位（比手写 fileset/壳表/locmap 可靠）。
provision_to() {   # provision_to <leader端口> <gid> <目标端口>
  local lp=$1 gid=$2 tp=$3 r
  r=$(PSQL "$lp" -Atc "SELECT partdist.provision_shard_replica(${gid}::bigint, ${NID[$tp]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本已供到 :$tp（${r:0:44}）" "$([[ "$r" == shard=* ]] && echo ok)" "ok"
}

# ---- A 组：M 确定性当选，再把 A 的副本供到其余两台 ----
sa=$(build_group_on "$SIDA" "$M" "$ALLMEM")
check "分片 A 组主落在 M（M 是 A 的主）" "$sa" "leader"
for p in "${WORKERS[@]}"; do [[ "$p" == "$M" ]] && continue; provision_to "$M" "$SIDA" "$p"; done

# ---- B 组：C 确定性当选，再把 B 的副本供到 M（以及另一台凑多数派）----
sb=$(build_group_on "$SIDB" "$C" "$ALLMEM")
check "分片 B 组主落在 C（C 是 B 的主）" "$sb" "leader"
for p in "${WORKERS[@]}"; do [[ "$p" == "$C" ]] && continue; provision_to "$C" "$SIDB" "$p"; done
nloc=$(PSQL $M -Atc "SELECT count(*) FROM partdist.replay_locmap('$BSHELL')" </dev/null | tail -1)
check "M 上 B 的从 locmap 已配对（主堆+主键+TOAST堆+TOAST索引=4）" "$nloc" "4"

echo "========== [3] ★ M 的双重身份：同一节点上 A=native_leader、B-从=replica =========="
loidA=$(PSQL $M -Atc "SELECT partdist.local_partition_for_shard($SIDA)" | tail -1)
roleA=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve($loidA, 1)" | tail -1)
check "★ M 上 分片A 角色 = native_leader（M 是 A 的主）" "$roleA" "native_leader"
boid=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT '$BSHELL'::regclass::oid" | tail -1)
# ★ "B-从 角色 = replica" 的断言挪到 [6] 追平之后：route_resolve 认"壳表"的判据是
#   pg_parwal/<oid>/apply_checkpoint 存在，而那个文件是**回放真的跑过一次**才写的。
#   在这里断言必然读到 native_leader —— 那不是产品问题，是断言提前了。
check "★ 同一节点、两个不同本地关系（OID 不同）" "$([[ -n "$loidA" && -n "$boid" && "$loidA" != "$boid" ]] && echo ok)" "ok"

echo "========== [4] ★ 往 A 写 → M 上 A 读得到（M 作主，本机读写正常） =========="
la=$(ensure_leader "$SIDA" "$M" "写 A 之前")
check "写 A 之前：M 仍是 A 的主（主漂了后面全不作数）" "$la" "leader"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO $A_NAME SELECT g,'a'||g FROM generate_series(1,50) g;"
ca=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM $ATBL" | tail -1)
check "★ M 上原生读 A = 50 行" "$ca" "50"
cc=$(PSQL $COORD -Atc "SELECT count(*) FROM $A_NAME" | tail -1)
check "  经协调者读 A = 50 行（路由到 M）" "$cc" "50"

echo "========== [5] ★ 隔离：往 A 狂写 → M 上 B 的从副本字节一个都不变 =========="
PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null
bpath=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('$BSHELL')" | tail -1)
bdir=$(PSQL $M -Atc "SHOW data_directory" | tail -1)
md5_b0=$(DEX bash -c "md5sum '$bdir/$bpath' 2>/dev/null | cut -d' ' -f1" </dev/null)
# 写入量按本环境实测吞吐（约 23 条 raft 记录/秒）配：250 行 + 100 改 ≈ 700 条记录 ≈ 30s。
# 值加宽到 100 字节，保证 A 主堆确实涨到多块（[5] 的防假通过断言要用）。
la=$(ensure_leader "$SIDA" "$M" "狂写 A 之前")
check "狂写 A 之前：M 仍是 A 的主" "$la" "leader"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO $A_NAME SELECT g, repeat('a',100) FROM generate_series(51,300) g;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE $A_NAME SET v=repeat('b',100) WHERE id<=100;"
PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null
md5_b1=$(DEX bash -c "md5sum '$bdir/$bpath' 2>/dev/null | cut -d' ' -f1" </dev/null)
check "★ 往 A 写 250 行 + 改 100 行后，M 上 B 的从副本主堆字节不变" \
      "$([[ -n "$md5_b0" && "$md5_b0" == "$md5_b1" ]] && echo ok)" "ok"
# 防假通过：证明 A 的写确实落到了 M（A 主堆变了）
apath=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('$ATBL')" | tail -1)
ablk=$(DEX bash -c "echo \$(( \$(stat -c %s '$bdir/$apath') / 8192 ))" </dev/null)
check "  防假通过：A 主堆确有多块数据（$ablk 块 > 1）" "$([[ "$ablk" -gt 1 ]] && echo ok)" "ok"

echo "========== [6] ★ 往 B 的主(C)写 → M 上 B 的从追平且逐字节一致（M 作从） =========="
lb=$(ensure_leader "$SIDB" "$C" "写 B 之前")
check "写 B 之前：C 仍是 B 的主" "$lb" "leader"
# ★ B 是 2 分片表：`INSERT ... SELECT generate_series` 会同时写到**两片**，于是进 DTX
#   决议路径去找协调组，而 B 的兄弟分片根本没建 raft 组 —— 实测报
#   `协调组 N 查不到现任 leader / partition_map 尚未追平`，整条 INSERT 失败、B 零行，
#   [7] 取 min(xmin) 拿到 NONE 跟着塌（这就是 26/1 那个 FAIL 的真正来源）。
#   正解：只插**路由到 SIDB 的那些 id**，拼成一条多行 VALUES。Citus 对多行 VALUES
#   会逐行剪枝，全落一片时直接路由成 `Task Count: 1`（实测 EXPLAIN 证实），
#   只碰 B 这一个组，压根不进 DTX。
BVALS=$(PSQL $COORD -Atc "SELECT string_agg('('||g||',''b'||g||''')', ',') FROM (SELECT g FROM generate_series(1,4000) g WHERE get_shard_id_for_distribution_column('$B_NAME',g)=$SIDB LIMIT 80) t" </dev/null | tail -1)
check "  为 B 挑出 80 个只落分片 $SIDB 的 id（写只碰一个组）" "$([[ -n "$BVALS" ]] && echo ok)" "ok"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO $B_NAME VALUES $BVALS;"
lp=$(PSQL $C -Atc "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($SIDB))" | tail -1)
# 规则 7：catchup 必须给上界
a=""; for t in $(seq 1 60); do a=$(PSQL $M -Atc "SELECT partdist.replay_catchup('$BSHELL', $lp, 10000)" 2>/dev/null | tail -1); [[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && break; sleep 1; done
check "★ M 上 B 的从追平到主的位点（$a/$lp）" "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && echo ok)" "ok"
# 逐字节比对：M 的 B-从 主堆 vs C 的 B-主 主堆（判据同 R1，掩码外一致）
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
PSQL $C -q -c "CHECKPOINT;" </dev/null >/dev/null; PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null; sleep 2
cdir=$(PSQL $C -Atc "SHOW data_directory" | tail -1)
cpath=$(PSQL $C -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('$BTBL')" | tail -1)
bpath2=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('$BSHELL')" | tail -1)
same=$(DEX python3 /tmp/pagecmp.py --kind=heap "$cdir/$cpath" "$bdir/$bpath2" </dev/null 2>/dev/null)
check "★ M 的 B-从 主堆与 C 的 B-主 掩码外逐字节一致" "$same" "IDENTICAL_OUTSIDE_HOLE"
# 回放跑过一次之后，apply_checkpoint 才在，路由层这时才认得出"壳表"
roleB=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve($boid, 1)" | tail -1)
check "★ M 上 分片B-从 角色 = replica（回放过后路由层认出壳表；M 同时还是 A 的主）" "$roleB" "replica"
roleA2=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve($loidA, 1)" | tail -1)
check "★ 同一时刻 M 上 分片A 仍是 native_leader（两个角色真并存）" "$roleA2" "native_leader"

echo "========== [7] ★ 两套 xid 空间互不串线：A 走原生语义、B-从走回放两跳 =========="
# ★ 这里的表**没有打标**（没调 set_table_shard_mvcc），元组带的是**原生 xid**，
#   不存在"分片 xid 小号空间" —— 用量级判据是前提就错了（实测原生水位 2850、
#   A 的 xmin 2118，根本比不出来）。真正能证明"两套空间互不串线"的是**解析路径**：
#   同一个节点上，A 的号走本机原生语义（不在回放宇宙 ⇒ NONE），
#   B-从 的号走回放两跳（xid_map → gclog ⇒ 拿得到判决），二者按各自 OID 编址。
nat=$(PSQL $M -Atc "SELECT txid_current()" | tail -1)
xa=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT coalesce(min(xmin::text::bigint)::text,'NONE') FROM $ATBL" | tail -1)
# ★ 取 B 的号要从**主(C)**上读，不要从 M 的副本壳表读：副本上的行对普通 SELECT
#   可不可见取决于 xid_map/gclog 状态（实测某轮读到 0 行 ⇒ min() 为 NULL ⇒ 整条断言塌掉）。
#   从主上读是原生可见的；这个号本来就是副本回放过来的，拿到副本 OID 上解析同样成立。
xb=$(PSQL $C -Atc "SET citus.override_table_visibility=false; SELECT coalesce(min(xmin::text::bigint)::text,'NONE') FROM $BTBL" | tail -1)
echo "  本机原生 xid 水位=$nat   A(M 自己的主) 最小 xmin=$xa   B(主 C 上) 最小 xmin=$xb"

stA=$(PSQL $M -Atc "SELECT coalesce(status,'NONE') FROM partdist.route_resolve($loidA, ${xa:-1})" | tail -1)
stB=$(PSQL $M -Atc "SELECT coalesce(status,'NONE') FROM partdist.route_resolve($boid, ${xb:-1})" | tail -1)
# 交叉查：把对方的号拿到自己的空间里查 —— 号跨空间没有意义
stAB=$(PSQL $M -Atc "SELECT coalesce(status,'NONE') FROM partdist.route_resolve($loidA, ${xb:-1})" | tail -1)
stBA=$(PSQL $M -Atc "SELECT coalesce(status,'NONE') FROM partdist.route_resolve($boid, ${xa:-1})" | tail -1)
echo "  A(oid=$loidA)  自家号 $xa ⇒ $stA    拿 B 的号 $xb 查 ⇒ $stAB"
echo "  B从(oid=$boid) 自家号 $xb ⇒ $stB    拿 A 的号 $xa 查 ⇒ $stBA"
check "★ A 的号走本机原生语义（不在回放宇宙 ⇒ NONE）" "$stA" "NONE"
# 非 NONE 即"走了回放宇宙"：NONE = 根本不是壳表（本机原生语义）；是壳表时要么拿到
# gclog 判决（committed…），要么 not_replayed（该号不在本壳表 xid_map 里）。
# 取 min(xmin) 挑到的是最老一条，不保证在 map 里，所以不把 committed 写死成唯一期望。
check "★ B 的号在 M 的副本上走回放宇宙（非 NONE，实得 $stB）" \
      "$([[ -n "$stB" && "$stB" != "NONE" ]] && echo ok)" "ok"
check "★ 两分片在 M 上是两个不同 OID（各自编址、键不冲突，规则 6）" \
      "$([[ -n "$loidA" && -n "$boid" && "$loidA" != "$boid" ]] && echo ok)" "ok"

echo "========== [8] 节点健康 =========="
health_check_no_crash

echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
if [[ "$FAIL" -eq 0 ]]; then echo "同一节点主从并存：全部通过"; else echo "同一节点主从并存：存在 FAIL"; fi
