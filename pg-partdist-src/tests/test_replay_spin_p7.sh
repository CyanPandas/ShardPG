#!/usr/bin/env bash
# [宿主机] P7-P2 验收：回放 worker 在"尾部未到"时不许空转。
#
# 缺陷形态（2026-09-13 从 tx2 容器取证发现）：replay_catchup 把 target_plsn 抬到
# 本地段里还没有的位置（字节尚未到达，或永远不会到了）⇒ ShardReplayRun 读到尾部
# 正常返回 ⇒ worker 把"一条没放"也当成 did_work ⇒ WaitLatch 超时 0 立刻重来。
# 每圈重建一次段索引、打一行 `追平至 N`。catchup 调用方超时走人之后 target 与
# CATCHING_UP 仍留在槽位里，于是**永远**转下去：tx2 的 worker3 一个日志 11 小时
# 刷到 20.7GB，全是同两个分片的 `追平至 0`，同一毫秒反复出现。
#
# 断言按**修好之后**该有的样子写 —— 修复前跑，[3] 必红（那就是复现）。
#
# ★ 几何怎么造：不去碰 raft（制造"未达多数派"不稳，见 test_replay_bound_p6 头注）。
#   直接给一个**比本地已收到字节更大**的上界：等价于"尾部尚未到达"，且确定。
#   [3] 的判据带"确实停在等待态"的守卫（target 还挂着、applied 没到），
#   否则"没刷日志"也可能是因为 worker 根本没在等 —— 那是空检查。
#   [4] 再把字节真的送到，验证修复没有把"晚到的尾部照样能放"弄坏。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
GAP=${GAP:-200}          # 上界比已收到的字节多出多少 plsn
WINDOW=${WINDOW:-10}     # 空转观测窗（秒）
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/p7_replay_spin.lock
if ! flock -n 9; then echo "FATAL: 另一个回放空转验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "================ [0] 夹具：1 分片 + raft 组 + 2 follower（只 arm 一个）================"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t7spin;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t7spin(id int, v text);
SELECT create_distributed_table('t7spin', 'id');
ALTER TABLE t7spin SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t7spin'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
f1dir="worker$((f1 - 5432))"
shard_tbl="t7spin_${gid}"
echo "  shard=${gid} leader=:${pport} 被测副本=:${f1}(${f1dir}) 陪跑=:${f2}"
check "夹具齐" "$([[ -n "$gid" && -n "$pport" && -n "$f1" && -n "$f2" ]] && echo ok || echo no)" "ok"

nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册" "$nrels" "3"
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows"|cut -d, -f1|paste -sd,); ords=$(echo "$fsrows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$fsrows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$fsrows"|cut -d, -f4|paste -sd,)
rels=$(echo "$fsrows"|cut -d, -f5|paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE t7spin INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
done
n=$(PSQL $f1 -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[], 0::bigint)" </dev/null 2>/dev/null | tail -1)
check "被测副本 :$f1 空表 base=0 配对" "$n" "3"
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
done
PSQL $f1 -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null

FOID=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
PLSN()   { PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1; }
RAFTB()  { PSQL $f1 -Atc "SELECT partdist.get_follower_applied_part_lsn(${FOID})" </dev/null | tail -1; }
CATCH()  { PSQL $f1 -Atc "SELECT partdist.replay_catchup(${FOID}::regclass, $1, $2)" </dev/null 2>&1 | tail -1; }
SLOT()   { PSQL $f1 -Atc "SELECT $1 FROM partdist.replay_status() WHERE shard=${FOID}" </dev/null | tail -1; }
F1LOG=$(health_node_log "$f1dir")
check "被测副本的真实日志路径已解析（/proc/<postmaster>/fd/2）" "$([[ "$F1LOG" == /* ]] && echo ok || echo no)" "ok"
SPINLINES() { DEX grep -c "replay: shard ${FOID} 追平至" "$F1LOG" </dev/null 2>/dev/null || true; }
LOGBYTES()  { DEX stat -c %s "$F1LOG" </dev/null; }
# worker 的 CPU 时间（jiffies）：/proc/<pid>/stat 里 comm 可能带空格，取最后一个 ')' 之后再数字段
CPUTICKS()  { DEX bash -c "s=\$(cat /proc/$1/stat) && s=\${s##*) } && set -- \$s && echo \$((\${12} + \${13}))" </dev/null; }

echo "================ [1] 正常追平：20 行 ⇒ P1 ================"
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'a'||g FROM generate_series(1,20) g;" </dev/null >/dev/null
P1=$(PLSN)
a=$(CATCH "$P1" 60000)
check "副本追平到 P1=${P1}（返回=$a）" "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$P1" ]] && echo ok || echo no)" "ok"
wpid=$(SLOT claimed_by)
check "回放 worker 已认领（claimed_by=${wpid}）" "$([[ "$wpid" =~ ^[0-9]+$ && "$wpid" -gt 0 ]] && echo ok || echo no)" "ok"

echo "================ [2] 触发：上界 = 已收到字节 + ${GAP}（尾部尚未到达）================"
got=$(RAFTB)
B=$(( ${got:-0} + GAP ))
echo "  已收到=${got}  上界 B=${B}"
check "★ 几何前提：B 之前的字节本地确实还没有（已收到 < B）" \
      "$([[ "$got" =~ ^[0-9]+$ && "$got" -lt "$B" ]] && echo ok || echo no)" "ok"
to=$(CATCH "$B" 3000)
echo "  replay_catchup(B, 3000ms) ⇒ ${to}"
check "catchup 按超时返回（字节不到就等不到，这本身是对的）" \
      "$([[ "$to" == *"追平超时"* ]] && echo ok || echo no)" "ok"

echo "================ [3] ★★ 调用方走了之后：观测 ${WINDOW}s ================"
l0=$(SPINLINES); b0=$(LOGBYTES); c0=$(CPUTICKS "$wpid")
sleep "$WINDOW"
l1=$(SPINLINES); b1=$(LOGBYTES); c1=$(CPUTICKS "$wpid")
tgt=$(SLOT target); app=$(SLOT applied); stt=$(SLOT state)
dl=$(( ${l1:-0} - ${l0:-0} )); db=$(( (${b1:-0} - ${b0:-0}) / 1024 )); dc=$(( ${c1:-0} - ${c0:-0} ))
pct=$(( dc * 100 / (WINDOW * 100) ))   # CLK_TCK=100
echo "  窗内：'追平至' 新增 ${dl} 行，日志增长 ${db} KB，worker CPU ${dc} jiffies ≈ ${pct}% 单核"
echo "  槽位：state=${stt} target=${tgt} applied=${app}"
# 守卫先行：确认这 ${WINDOW} 秒 worker 真的处在"等尾部"——否则下面的"没刷/没转"是空检查
check "★ 守卫：窗内确实停在等待态（target=B 仍挂着、applied<B）" \
      "$([[ "$tgt" == "$B" && "$app" =~ ^[0-9]+$ && "$app" -lt "$B" ]] && echo ok || echo no)" "ok"
check "★ 守卫：观测量都取到了（行数/字节/CPU 非空）" \
      "$([[ "$l0" =~ ^[0-9]+$ && "$l1" =~ ^[0-9]+$ && "$c0" =~ ^[0-9]+$ && "$c1" =~ ^[0-9]+$ ]] && echo ok || echo no)" "ok"
check "★★ 等尾部期间不刷日志（'追平至' 窗内新增 ≤ 1 行，实际 ${dl}）" \
      "$([[ "$dl" -le 1 ]] && echo ok || echo no)" "ok"
check "★★ 等尾部期间 worker 不空转（CPU ≤ 10% 单核，实际 ≈${pct}%）" \
      "$([[ "$pct" -le 10 ]] && echo ok || echo no)" "ok"
# 阳性对照：不刷不等于不说 —— "等尾部"必须留一行证词，且对同一个目标只留一行
nwait=$(DEX grep -c "replay: shard ${FOID} 已放到 [0-9]*，目标 ${B} 的字节尚未到达" "$F1LOG" </dev/null 2>/dev/null || true)
check "★ 等尾部不静默：该目标的等待提示恰好 1 行（实际 ${nwait}）" "$nwait" "1"

echo "================ [4] ★ 字节晚到：不再调 catchup，worker 自己放到 B ================"
# 修复把"没进展"改成按 naptime 轮询 —— 必须证明晚到的尾部照样会被捡起来，
# 否则等于把空转换成了"再也不追"。
ins=0
for t in $(seq 1 60); do
  cur=$(RAFTB); [[ "$cur" =~ ^[0-9]+$ && "$cur" -ge "$B" ]] && break
  PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'b'||g FROM generate_series(1000+${t}*100, 1000+${t}*100+49) g;" </dev/null >/dev/null
  ins=$((ins+1)); sleep 0.5
done
cur=$(RAFTB)
echo "  补写 ${ins} 批后已收到=${cur}"
check "★ 几何前提：字节确实到达了 B（已收到 >= B）" \
      "$([[ "$cur" =~ ^[0-9]+$ && "$cur" -ge "$B" ]] && echo ok || echo no)" "ok"
app=""
for t in $(seq 1 30); do
  app=$(SLOT applied); [[ "$app" =~ ^[0-9]+$ && "$app" -ge "$B" ]] && break; sleep 0.5
done
check "★★ 未重新触发，worker 自行追到 B（applied=${app}）" \
      "$([[ "$app" =~ ^[0-9]+$ && "$app" -ge "$B" ]] && echo ok || echo no)" "ok"
check "  追到 B 即停在 B（上界不越界：applied == B）" "$app" "$B"
check "  到位后回到 idle" "$(SLOT state)" "idle"

echo "================ [5] 数据正确：追满后与 leader 逐字节一致 ================"
P3=$(PLSN)
a5=$(CATCH "$P3" 60000)
check "追平到 P3=${P3}（返回=$a5）" "$([[ "$a5" =~ ^[0-9]+$ && "$a5" -ge "$P3" ]] && echo ok || echo no)" "ok"
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
FPATH() {
  local fp=$1 oid=$2 fdata frel
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  frel=$(PSQL "$fp" -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath(${oid}::regclass)" </dev/null|tail -1)
  echo "${fdata}/${frel}"
}
cmp_end=$(DEX python3 /tmp/pagecmp.py --kind=heap "$(FPATH $pport "$LEADER_OID")" "$(FPATH $f1 "$FOID")" </dev/null 2>/dev/null)
check "★★ 主堆与 leader 逐字节一致" "$cmp_end" "IDENTICAL_OUTSIDE_HOLE"

echo "================ [6] 清理 ================"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}'::regclass)" </dev/null >/dev/null 2>&1
done
# 分区组要在删表**之前**拆掉：组还在的话，漂走了主的那台节点上的写（含 DROP）会被
# "本节点不是该分区组的 leader"拦下；且组只在 shmem 里、不随表消失，留着会拦下一轮夹具。
# ★ 逐台删一遍不够（2026-09-13 实测）：还没删到的成员发来的 RPC 会在已删的节点上
#   把组**重新建出来**。三台一起删、复查，直到全空。
left=1
for t in $(seq 1 10); do
  for p in $pport $f1 $f2; do
    PSQL $p -q -c "SELECT partdist.pg_raft_group_drop(${gid});" </dev/null >/dev/null 2>&1
  done
  sleep 2
  left=0
  for p in $pport $f1 $f2; do
    k=$(PSQL $p -Atc "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null | tail -1)
    left=$(( left + ${k:-1} ))
  done
  [[ "$left" == "0" ]] && break
done
for fp in $f1 $f2; do
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl}" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t7spin" </dev/null >/dev/null 2>&1
for p in $COORD $pport $f1 $f2; do
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" </dev/null >/dev/null 2>&1
done
check "清理：三个成员上都不再有分区组 ${gid}" "$left" "0"
health_check_no_crash
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
