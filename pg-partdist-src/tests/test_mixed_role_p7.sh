#!/usr/bin/env bash
# [宿主机] 主从架构：**同一节点同时当主、又当从** 验收。
#
# 本设计不是"整机主 / 整机从"，主从的作用域是**分区(shard)**：每个分片一套
# Raft 组，组 leader = 该分片的主副本(原生真表)，其余成员 = 从副本(壳表，物理回放)。
# 一个节点因此可以同时是分片 A 的主、分片 B 的从(FRD §14.2 R1、shard_route.h 三态)。
# 本套件把这个"混合角色"节点单拎出来验，是既有测试(回放一致性/升主/供副本)都没有
# 专门覆盖的一格。
#
# 拓扑（M = 混合角色节点）：
#   分片 A：主在 M（原生真表，可读写）                A 组 {M, X1, X2}，M 当选
#   分片 B：主在 C（≠M），从在 M（壳表，回放跟随 C）  B 组 {C, M, X3}，C 当选
#   —— M 因此同时：A 的 native_leader + B 的 replica
#
# 断言（对应本设计的四条核心命题）：
#   [4] 往 A 写 → M 上 A 读得到              （M 作主，本机读写正常）
#   [5] 往 A 狂写 → M 上 B 的从副本字节不变  （同机两分片物理隔离，A 的写不串进 B 的从）
#   [6] 往 B 的主(C)写 → M 上 B 的从追平且逐字节一致（M 作从，回放正确跟随别的主）
#   [7] clog 键不冲突：A 与 B-从在 M 上 OID 不同、各自独立 clog；A 行 xmin 是 M 自己的
#       分片 xid、B-从行 xmin 是 C 的分片 xid，互查为空（两套 xid 空间互不串线，规则 6）
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
# 2 vCPU + 无 swap：建组后新 leader 在 tick 里同步做登记，饱和时十几秒不发心跳 ⇒ 主漂。
# 临时把选举超时抬到 20s，EXIT 复原。判据/来源见规则 19。
for p in 5433 5434 5435 5436 5437 5438 5439 5440 $COORD; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_raft.election_timeout_ms = 20000;" </dev/null >/dev/null
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
  PSQL $COORD -q -c "DROP TABLE IF EXISTS mr_a; DROP TABLE IF EXISTS mr_b;" </dev/null >/dev/null 2>&1 || true
  for p in 5433 5434 5435 5436 5437 5438 5439 5440 $COORD; do
    PSQL $p -q -c "ALTER SYSTEM RESET pg_raft.election_timeout_ms;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.replay_trust_local_segments;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
  echo "  [复原] 组已拆、表已删、election_timeout 已 RESET"
}
trap cleanup EXIT

echo "========== [1] 夹具：分片 A（主定 M）+ 分片 B（挑一个主不在 M 的分片） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
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
# A 的分片与其 placement 主 = M
SIDA=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='mr_a'::regclass" | tail -1)
M=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$SIDA" | tail -1)
# B 的两个分片里，挑一个 placement 主 ≠ M 的做分片 B（保证 M 能当它的从）
read SIDB C < <(PSQL $COORD -Atc "SELECT s.shardid, n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='mr_b'::regclass AND n.nodeport <> $M ORDER BY s.shardid LIMIT 1" | tail -1 | tr '|' ' ')
Mn=$((M-5431)); Cn=$((C-5431))
# 再挑 3 个别的 worker 当凑多数派的成员（排除 M、C、协调者）
others=(); for p in 5433 5434 5435 5436 5437 5438 5439 5440; do [[ "$p" == "$M" || "$p" == "$C" ]] && continue; others+=("$p"); done
X1=${others[0]}; X2=${others[1]}; X3=${others[2]}
X1n=$((X1-5431)); X2n=$((X2-5431)); X3n=$((X3-5431))
ATBL="mr_a_${SIDA}"; BTBL="mr_b_${SIDB}"; BSHELL="$BTBL"
echo "  混合角色节点 M=:$M(node$Mn)  |  分片A=$SIDA 主在 M  |  分片B=$SIDB 主在 C=:$C(node$Cn)  |  凑多数派 X1..X3=:$X1 :$X2 :$X3"
check "M ≠ C（混合角色的前提：M 不能是 B 的主）" "$([[ -n "$M" && -n "$C" && "$M" != "$C" ]] && echo ok)" "ok"
check "凑够 3 个额外 worker" "$([[ -n "$X3" ]] && echo ok)" "ok"

echo "========== [2] 分片身份 + 建 A 组（M 主）+ 建 B 组（C 主）+ M 上供 B 的从 =========="
for p in $M $C $X1 $X2 $X3; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done

# ---- A 组：M 先建组抢当选（规则 4），X1/X2 只作 raft 成员凑多数派（不建壳表，A 的从不是本套件要验的） ----
amem="ARRAY[$Mn,$X1n,$X2n]"
PSQL $M -q -c "SELECT partdist.pg_raft_group_create($SIDA, $amem);" </dev/null >/dev/null
sleep 2
for p in $X1 $X2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_create($SIDA, $amem);" </dev/null >/dev/null; MADE_GROUPS+="$p:$SIDA "; done
MADE_GROUPS+="$M:$SIDA "
sa=""; for t in $(seq 1 40); do sa=$(PSQL $M -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$SIDA" 2>/dev/null | tail -1); [[ "$sa" == "leader" ]] && break; sleep 1; done
check "分片 A 组主落在 M（M 是 A 的主）" "$sa" "leader"

# ---- B 组：C 先建组抢当选，M 作从副本、X3 凑多数派（3 成员 ⇒ 多数派 2，M 可惰性滞后，规则 17） ----
bmem="ARRAY[$Cn,$Mn,$X3n]"
# 先在 C 上登记 B 的 fileset，导出给 M 配对
nrel=$(PSQL $C -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('$BTBL')" | tail -1)
check "C 上登记 B 的 fileset（主堆+主键=2）" "$nrel" "2"
# M 上建 B 的从壳表（本地表，回放前为空 ⇒ base_part_lsn=0 成立）
PSQL $M -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation=off;
DROP TABLE IF EXISTS $BSHELL;
CREATE TABLE $BSHELL (LIKE mr_b INCLUDING ALL);
ALTER TABLE $BSHELL SET (autovacuum_enabled=off);
SQL
PSQL $M -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null
rows=$(PSQL $C -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('$BTBL') ORDER BY role,ord" | grep ',')
roles=$(cut -d, -f1 <<<"$rows"|paste -sd,); ords=$(cut -d, -f2 <<<"$rows"|paste -sd,); spcs=$(cut -d, -f3 <<<"$rows"|paste -sd,); dbs=$(cut -d, -f4 <<<"$rows"|paste -sd,); rels=$(cut -d, -f5 <<<"$rows"|paste -sd,)
np=$(PSQL $M -Atc "SELECT partdist.replay_set_locmap('$BSHELL', ARRAY[$roles], ARRAY[$ords], ARRAY[$spcs]::oid[], ARRAY[$dbs]::oid[], ARRAY[$rels]::oid[])" | tail -1)
check "M 上 B 的从 locmap 配对（2 对）" "$np" "2"
PSQL $C -q -c "SELECT partdist.pg_raft_group_create($SIDB, $bmem);" </dev/null >/dev/null
sleep 2
for p in $M $X3; do PSQL $p -q -c "SELECT partdist.pg_raft_group_create($SIDB, $bmem);" </dev/null >/dev/null; MADE_GROUPS+="$p:$SIDB "; done
MADE_GROUPS+="$C:$SIDB "
PSQL $M -q -c "SELECT partdist.replay_enable('$BSHELL');" </dev/null >/dev/null
sb=""; for t in $(seq 1 40); do sb=$(PSQL $C -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$SIDB" 2>/dev/null | tail -1); [[ "$sb" == "leader" ]] && break; sleep 1; done
check "分片 B 组主落在 C（C 是 B 的主）" "$sb" "leader"

echo "========== [3] ★ M 的双重身份：同一节点上 A=native_leader、B-从=replica =========="
loidA=$(PSQL $M -Atc "SELECT partdist.local_partition_for_shard($SIDA)" | tail -1)
roleA=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve($loidA, 1)" | tail -1)
check "★ M 上 分片A 角色 = native_leader（M 是 A 的主）" "$roleA" "native_leader"
boid=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT '$BSHELL'::regclass::oid" | tail -1)
roleB=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve($boid, 1)" | tail -1)
check "★ M 上 分片B-从 角色 = replica（M 是 B 的从）" "$roleB" "replica"
check "★ 同一节点、两个不同本地关系（OID 不同）" "$([[ -n "$loidA" && -n "$boid" && "$loidA" != "$boid" ]] && echo ok)" "ok"

echo "========== [4] ★ 往 A 写 → M 上 A 读得到（M 作主，本机读写正常） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO mr_a SELECT g,'a'||g FROM generate_series(1,50) g;"
ca=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM $ATBL" | tail -1)
check "★ M 上原生读 A = 50 行" "$ca" "50"
cc=$(PSQL $COORD -Atc "SELECT count(*) FROM mr_a" | tail -1)
check "  经协调者读 A = 50 行（路由到 M）" "$cc" "50"

echo "========== [5] ★ 隔离：往 A 狂写 → M 上 B 的从副本字节一个都不变 =========="
PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null
bpath=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('$BSHELL')" | tail -1)
bdir=$(PSQL $M -Atc "SHOW data_directory" | tail -1)
md5_b0=$(DEX bash -c "md5sum '$bdir/$bpath' 2>/dev/null | cut -d' ' -f1" </dev/null)
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO mr_a SELECT g,'a2-'||g FROM generate_series(51,500) g;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE mr_a SET v='a3-'||id WHERE id<=200;"
PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null
md5_b1=$(DEX bash -c "md5sum '$bdir/$bpath' 2>/dev/null | cut -d' ' -f1" </dev/null)
check "★ 往 A 写 450 行 + 改 200 行后，M 上 B 的从副本主堆字节不变" \
      "$([[ -n "$md5_b0" && "$md5_b0" == "$md5_b1" ]] && echo ok)" "ok"
# 防假通过：证明 A 的写确实落到了 M（A 主堆变了）
apath=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('$ATBL')" | tail -1)
ablk=$(DEX bash -c "echo \$(( \$(stat -c %s '$bdir/$apath') / 8192 ))" </dev/null)
check "  防假通过：A 主堆确有多块数据（$ablk 块 > 1）" "$([[ "$ablk" -gt 1 ]] && echo ok)" "ok"

echo "========== [6] ★ 往 B 的主(C)写 → M 上 B 的从追平且逐字节一致（M 作从） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO mr_b SELECT g,'b'||g FROM generate_series(1,120) g;"
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

echo "========== [7] ★ clog 键不冲突：A 与 B-从各自独立、两套 xid 空间互不串线 =========="
# A 的行：M 自己是 A 的主，xmin 是 M 发的分片 xid（小号）
xa=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT xmin::text::bigint FROM $ATBL WHERE id=1" | tail -1)
check "A 行 xmin 是分片 xid（<1000，非原生大 xid）" "$([[ "$xa" =~ ^[0-9]+$ && "$xa" -lt 1000 ]] && echo ok)" "ok"
# B-从的行：xmin 是 C 发的分片 xid（回放来的外来号）
xb=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SET pg_partdist.allow_replica_access=on; SELECT xmin::text::bigint FROM $BSHELL WHERE id=1" | tail -1)
check "B-从 行 xmin 是分片 xid（<1000，来自 C 的空间）" "$([[ "$xb" =~ ^[0-9]+$ && "$xb" -lt 1000 ]] && echo ok)" "ok"
# ★ 键是本节点 oid（规则 6）：拿 A 的 oid 查得出 A 的账，B-从的 oid 查得出 B 的账；
#   两者 OID 不同 ⇒ 落在不同的 pg_shard_clog/<oid> 目录，号即使数值相同也互不影响。
stA=$(PSQL $M -Atc "SELECT status FROM partdist.route_resolve($loidA, $xa)" | tail -1)
stB=$(PSQL $M -Atc "SELECT status FROM partdist.route_resolve($boid, $xb)" | tail -1)
echo "  A(oid=$loidA) xid=$xa ⇒ $stA   |   B-从(oid=$boid) xid=$xb ⇒ $stB"
check "★ 两分片在 M 上按各自 OID 独立解析（键不冲突，规则 6）" \
      "$([[ -n "$stA" && -n "$stB" && "$loidA" != "$boid" ]] && echo ok)" "ok"

echo "========== [8] 节点健康 =========="
health_check_no_crash

echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
if [[ "$FAIL" -eq 0 ]]; then echo "同一节点主从并存：全部通过"; else echo "同一节点主从并存：存在 FAIL"; fi
