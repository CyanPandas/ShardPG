#!/usr/bin/env bash
# [宿主机] 回归：本地 pg_wal 崩溃恢复不得覆盖回放结果（FRD §13 约束 12）。
#
# 副本 shard 文件有两个互不知情的写者：本模块的 parwal 回放（盖 leader 坐标
# LSN），以及节点自身的本地 pg_wal 崩溃恢复。内核对 **FPI 的应用是无条件的**
# —— xlogutils.c 的 XLogReadBufferForRedoExtended 在 XLogRecBlockImageApply()
# 分支里直接 RBM_ZERO_AND_LOCK + RestoreBlockImage，前面**没有页 LSN 比较**。
# 所以"回放盖的 leader LSN 更大"保护不了任何东西。
#
# 具体触发：follower 壳表是本地 CREATE TABLE ... (LIKE ... INCLUDING ALL) 建的，
# 建索引时每个 btree 元页以 FPI 落进本地 WAL（wal_level=replica 下 wal_skip
# 优化不适用 —— 它只在 wal_level=minimal 生效）。节点崩溃后若 redo 起点早于
# 建表，那条 FPI 会把回放出来的元页盖回 _bt_initmetapage 初值（btm_root=0，
# 意味着升主后该索引不可用）。
#
# 修法是 replay_set_locmap() 末尾强制一次 checkpoint 把 redo 点推过建壳表。
#
# ★ 为什么需要**独立**用例而不是靠 test_follower_replay_r1.sh 的 kill -9：
#   修复生效后 redo 点永远在建壳表之后，R1 再也走不到这个场景 —— 它对本条
#   修复没有回归能力。本用例反过来**刻意把 redo 点钉在建壳表之前**，
#   于是"修复是否还在"成为唯一变量。
set -u

CONTAINER="${CONTAINER:-pg-citus-replay-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }
check() {
  # ★ 空值守卫：两个命令替换都失败时 "" == "" 会静默判通过。
  # 典型漏网：D1 用 leader_relnum / locmap_leader_relnum 互比文件号，
  # 若 shard_fileset() 因登记被丢弃而返回 0 行，两边都是空串 ⇒
  # "文件号确实换了"与"locmap 已换到新文件号"**双双恒真**。
  # 期望值本身就是空串的场景本项目里不存在，所以一律要求非空。
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

echo "========== [0] 前置 =========="
lvl=$(PSQL $COORD -Atc "SHOW wal_level")
check "wal_level=replica（minimal 下 wal_skip 会绕开本场景，用例不适用）" "$lvl" "replica"

echo "========== [1] 夹具：1 分片 + PK 索引 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS lwc_probe;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE lwc_probe(id int primary key, v text);
SELECT create_distributed_table('lwc_probe', 'id');
ALTER TABLE lwc_probe SET (autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='lwc_probe'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport-5431))
f1=""; for p in 5433 5434 5435 5436 5437 5438 5439 5440; do [[ "$p" == "$pport" ]] && continue; f1=$p; break; done
f2=""; for p in 5433 5434 5435 5436 5437 5438 5439 5440; do [[ "$p" == "$pport" || "$p" == "$f1" ]] && continue; f2=$p; break; done
f1node=$((f1-5431)); f2node=$((f2-5431))
shard_tbl="lwc_probe_${gid}"
echo "  shard=${gid} leader=:${pport} follower=:${f1}"

PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" >/dev/null
rows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role,ord" | grep ',')
roles=$(echo "$rows"|cut -d, -f1|paste -sd,); ords=$(echo "$rows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$rows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$rows"|cut -d, -f4|paste -sd,)
rels=$(echo "$rows"|cut -d, -f5|paste -sd,)

echo "========== [2] ★ 控制变量：建壳表**之前**先 CHECKPOINT，把 redo 点钉住 =========="
PSQL $f1 -q -c "CHECKPOINT;" >/dev/null
redo_pinned=$(PSQL $f1 -Atc "SELECT redo_lsn FROM pg_control_checkpoint()")
lsn_pre=$(PSQL $f1 -Atc "SELECT pg_current_wal_lsn()")
echo "  redo 点钉在 ${redo_pinned}，建壳表从 ${lsn_pre} 开始"

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE lwc_probe INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
  PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])" >/dev/null
done
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null; done

# 修复的直接断言：replay_set_locmap 必须已经把 redo 点推过建壳表
redo_now=$(PSQL $f1 -Atc "SELECT redo_lsn FROM pg_control_checkpoint()")
advanced=$(PSQL $f1 -Atc "SELECT CASE WHEN '${redo_now}'::pg_lsn > '${lsn_pre}'::pg_lsn THEN 'yes' ELSE 'no' END")
check "replay_set_locmap 把 redo 点推过了建壳表（${redo_pinned} → ${redo_now}）" "$advanced" "yes"

echo "========== [3] Raft 组 + 回放 5 行 =========="
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" >/dev/null
done

# 首次插入即产生 XLOG_BTREE_NEWROOT，把 btm_root 从 0 改成 1
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO lwc_probe SELECT g,'v'||g FROM generate_series(1,5) g;"
leader_oid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})")
lp=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
app=$(PSQL $f1 -Atc "SELECT partdist.replay_catchup('${shard_tbl}', ${lp}, 120000)" 2>/dev/null)
check "follower 追平（${app}/${lp}）" "$([[ "$app" -ge "$lp" ]] && echo ok)" "ok"

# ---- 元页读取（刻意不做 CHECKPOINT：apply checkpoint 的 smgrimmedsync 已刷盘，
#      而在此处 CHECKPOINT 会推动 redo 点，把待测场景抹掉）----
cat > /tmp/lwc_meta.py <<'PY'
import struct, sys
b = open(sys.argv[1], 'rb').read()[:8192]
root, = struct.unpack_from('<I', b, 32)
print(root)
PY
docker cp /tmp/lwc_meta.py "$CONTAINER":/tmp/lwc_meta.py >/dev/null; rm -f /tmp/lwc_meta.py
btm_root() {  # <port>
  local d rel
  d=$(PSQL $1 -Atc "SHOW data_directory")
  rel=$(PSQL $1 -Atc "SET citus.enable_ddl_propagation=off; SET citus.override_table_visibility=false;
        SELECT pg_relation_filepath(pg_filenode_relation(CASE WHEN spc=1663 THEN 0 ELSE spc END, relnum))
          FROM partdist.shard_fileset('${shard_tbl}') WHERE role=1 AND ord=0" 2>/dev/null | grep '^base/')
  [[ -n "$rel" ]] || { echo "PATHFAIL"; return; }
  DEX python3 /tmp/lwc_meta.py "${d}/${rel}" </dev/null 2>/dev/null
}

before=$(btm_root $f1)
check "崩溃前 follower 元页 btm_root=1（回放生效）" "$before" "1"

echo "========== [4] immediate 崩溃 + 重启 =========="
fdir="worker$((f1 - 5432))"
# ★ 必须证明崩溃**真的发生了**，不能只看 SELECT 1 能连上。
#
# 本脚本存在的唯一理由是"那个修复还在不在"（本地 pg_wal 崩溃恢复会不会覆盖
# 回放结果）。而 stop 与 start 的退出码此前全被丢弃：stop 因任何原因失败
# （节点仍在跑）⇒ start 报 "already running" 也被吞掉 ⇒ **一次 redo 都没发生**
# ⇒ 下面 btm_root 那条断言平凡通过。整个金丝雀对"根本没崩过"是瞎的。
# 判据取重启前后的 pg_postmaster_start_time() 必须不同 —— 那是"进程换了一条命"
# 的直接证据，比退出码更难糊弄。
start_before=$(PSQL $f1 -Atc "SELECT pg_postmaster_start_time()" 2>/dev/null)
# ★ 必须重启回**约定的**日志路径（/work/pg-cluster-data/<节点>.log）。
# 早先这里写的是 <datadir>/lwc_restart.log，只为让下面那条 grep 好写，代价是
# 本用例跑完后 worker1/worker2 的日志**永久改道**：约定路径上的文件从此冻结，
# 而 lib_node_health 的两个 glob 谁也匹配不到新目标 —— 此后所有套件在这两个
# 节点上的 health_check_no_crash 都是空检查（实测：节点确实重置了，日志一行没多）。
# 自己的取证改用行号基线，同样精确，且不留污染。
logmark=$(DEX bash -c "wc -l < /work/pg-cluster-data/${fdir}.log 2>/dev/null || echo 0" | tr -d '[:space:]')
DEX /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/${fdir}" -m immediate stop >/dev/null 2>&1
DEX /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/${fdir}" -w -t 60 \
    -l "/work/pg-cluster-data/${fdir}.log" start >/dev/null 2>&1
up=no
for t in $(seq 1 90); do [[ "$(PSQL $f1 -Atc 'SELECT 1' 2>/dev/null)" == "1" ]] && { up=yes; break; }; sleep 1; done
check "follower 节点从崩溃中恢复" "$up" "yes"
start_after=$(PSQL $f1 -Atc "SELECT pg_postmaster_start_time()" 2>/dev/null)
check "崩溃确实发生过（postmaster 启动时刻已改变）" \
      "$([[ -n "$start_before" && -n "$start_after" && "$start_before" != "$start_after" ]] && echo ok)" "ok"
# 再取一条内核自己的证词：崩溃恢复必然打这行日志。只看重启之后新增的部分，
# 免得被日志里**上一轮**留下的同一行蒙混过关（append 模式下这行会越积越多）。
crashlines=$(DEX bash -c "tail -n +$((logmark + 1)) /work/pg-cluster-data/${fdir}.log 2>/dev/null | grep -c 'database system was not properly shut down'" | tr -d '[:space:]')
check "重启日志里有崩溃恢复的证据（新增 ${crashlines} 条）" \
      "$([[ "$crashlines" =~ ^[0-9]+$ && "$crashlines" -ge 1 ]] && echo ok || echo "no(${crashlines})")" "ok"

after=$(btm_root $f1)
check "崩溃恢复**没有**覆盖回放结果（btm_root 仍为 1）" "$after" "1"
[[ "$after" == "0" ]] && echo "        ← btm_root 回到 0 = _bt_initmetapage 初值：本地 WAL 的建索引 FPI 盖掉了回放结果（FRD §13 约束 12）"

echo
echo "========== 清理 =========="
PSQL $COORD -q -c "DROP TABLE IF EXISTS lwc_probe;" >/dev/null 2>&1
for fp in $f1 $f2; do PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" >/dev/null 2>&1; done

echo
echo "==================== 结果：PASS=${PASS} FAIL=${FAIL} ===================="
[[ "$FAIL" -eq 0 ]] || exit 1
