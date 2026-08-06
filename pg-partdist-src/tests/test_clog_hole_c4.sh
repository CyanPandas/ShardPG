#!/usr/bin/env bash
# [宿主机] §13 约束 4 验收：回放推进 nextXid 跨过 clog 页边界后，本地事务与
# 崩溃恢复必须都能活（test_clog_hole_c4）。
#
# 缺陷形态（修复前，2026-08-05 实测）：
#   PartDistAdvanceNextXidPastXid 跳过的 xid 区间从未 ExtendCLOG，页边界
#   （每 32768 个 xid 一页）被跨过时 pg_xact 缺页 ——
#   a) 本地事务提交/中止时读 clog 当场
#        ERROR: could not access status of transaction ...
#   b) 侥幸活到 kill -9 的，崩溃恢复 redo 到 COMMIT 记录时同款 FATAL，节点报废；
#   c) 次生：可见性自相矛盾 → autovacuum 无限自旋攥死 buffer 锁。
#
# 用例路径：
#   [1] 夹具：1 分片，leader + 1 follower，开回放
#   [2] leader 节点烧 4 万个 xid（异常子事务各耗一个），保证跨 ≥1 个 clog 页边界
#   [3] 写入分片并让 follower 追平 —— follower nextXid 被拉着跳过页边界
#   [4] ★ follower 本地事务能提交（未修复时在这里就炸）
#   [5] ★ kill -9 follower 节点，崩溃恢复能起来
#   [6] ★ follower 的 partdist 表 VACUUM 能在限时内完成（防自旋冒烟）
#
# 红灯自救：未修复时 [4]/[5] 会把节点打进"clog 缺页"状态，收尾按 controldata
# 的 NextXID 把 pg_xact 补零页到位并重启 —— 保证跑完红灯环境仍可用。
set -u

CONTAINER="${CONTAINER:-pg-citus-replay-container}"
COORD=5432
PASS=0; FAIL=0

DEX()   { docker exec -i -u postgres "$CONTAINER" "$@"; }
DEXQ()  { docker exec -i -u postgres "$CONTAINER" "$@" </dev/null; }
PSQL()  { local port=$1; shift; DEX  /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }
PSQLQ() { local port=$1; shift; DEXQ /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

check() {
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置：group0 收敛 =========="
leader=$(PSQLQ $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

echo "========== [1] 夹具：1 分片 + 1 follower 开回放 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS c4_dist;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE c4_dist(id int primary key, v text);
SELECT create_distributed_table('c4_dist','id');
ALTER TABLE c4_dist SET (autovacuum_enabled = off);
SQL
gid=$(PSQLQ $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='c4_dist'::regclass")
pport=$(PSQLQ $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pid=$((pport-5431)); f1=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue; f1=$p; break
done
fdir="worker$((f1-5432))"
check "夹具就位（shard=${gid} leader=:${pport} follower=:${f1}）" \
  "$([[ -n "$gid" && -n "$pport" && -n "$f1" ]] && echo ok)" "ok"

shard_tbl="c4_dist_${gid}"
nrels=$(PSQLQ $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" | tail -1)
check "leader fileset 注册（4 关系）" "$nrels" "4"
fsrows=$(PSQLQ $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)
PSQL $f1 -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE c4_dist INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
np=$(PSQLQ $f1 -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])")
check "follower locmap 配对 4 对" "$np" "4"
for p in $pport $f1; do PSQLQ $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null; done

mem="ARRAY[${pid}, $((f1-5431))]"
st=""
for attempt in 1 2 3; do
  for p in $pport $f1; do PSQLQ $p -q -c "SELECT partdist.pg_raft_group_drop(${gid});" >/dev/null 2>&1; done
  sleep 3
  PSQLQ $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${mem});" >/dev/null
  sleep 1
  PSQLQ $f1 -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${mem});" >/dev/null
  for t in $(seq 1 25); do
    st=$(PSQLQ $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null)
    [[ "$st" == "leader" ]] && break; sleep 1
  done
  [[ "$st" == "leader" ]] && break
  echo "  第 ${attempt} 次建组主没落在 placement 节点(当前=$st)，重来"
done
check "分区组 leader 就位(:${pport})" "$st" "leader"
PSQLQ $f1 -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" >/dev/null
PSQLQ $f1 -q -c "SELECT pg_reload_conf();" >/dev/null
en=$(PSQLQ $f1 -Atc "SELECT partdist.replay_enable('${shard_tbl}')")
check "follower replay_enable" "$en" "t"

echo "========== [2] leader 节点烧 4 万 xid（跨 clog 页边界）=========="
xid_before=$(PSQLQ $pport -Atc "SELECT pg_current_xact_id()::text::bigint")
PSQL $pport -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE TEMP TABLE c4_burn(i int);
DO $$
BEGIN
  FOR i IN 1..40000 LOOP
    BEGIN
      -- ★ 必须先写入：xid 是惰性分配的，纯计算的子事务（如 1/0）
      -- 到中止都没拿过 xid，一个都烧不掉（实测 4 万循环只耗 1 个）。
      INSERT INTO c4_burn VALUES (i);
      RAISE EXCEPTION 'burn';
    EXCEPTION WHEN raise_exception THEN NULL;
    END;
  END LOOP;
END$$;
DROP TABLE c4_burn;
SQL
xid_after=$(PSQLQ $pport -Atc "SELECT pg_current_xact_id()::text::bigint")
burned=$(( xid_after - xid_before ))
check "烧掉 ≥32768 个 xid（实烧 ${burned}）" "$([[ "$burned" -ge 32768 ]] && echo ok)" "ok"

echo "========== [3] 写入 + follower 追平（nextXid 跳过页边界）=========="
fx_before=$(PSQLQ $f1 -Atc "SELECT pg_current_xact_id()::text::bigint")
PSQLQ $COORD -q -c "INSERT INTO c4_dist SELECT g, 'v'||g FROM generate_series(1,200) g;" >/dev/null
leader_oid=$(PSQLQ $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})")
tgt=$(PSQLQ $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
check "leader 已产生 parwal 记录（plsn=${tgt}）" "$([[ -n "$tgt" && "$tgt" -gt 0 ]] && echo ok)" "ok"
app=$(PSQLQ $f1 -Atc "SELECT partdist.replay_catchup('${shard_tbl}', ${tgt}, 300000)" 2>&1)
check "follower 追平到 ${tgt}（返回 ${app}）" "$([[ "$app" =~ ^[0-9]+$ ]] && echo ok || echo "$app" | head -1)" "ok"
fx_after=$(PSQLQ $f1 -Atc "SELECT pg_current_xact_id()::text::bigint")
jump=$(( fx_after - fx_before ))
check "follower nextXid 已被拉过页边界（跳 ${jump}）" "$([[ "$jump" -ge 32768 ]] && echo ok)" "ok"

echo "========== [4] ★ follower 本地事务能提交（缺陷即死在这里）=========="
out=$(PSQL $f1 2>&1 <<'SQL'
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS c4_local_probe;
BEGIN;
CREATE TABLE c4_local_probe(i int);
INSERT INTO c4_local_probe SELECT generate_series(1,100);
COMMIT;
SQL
)
if grep -q "could not access status of transaction" <<<"$out"; then
  check "follower 本地事务提交" "clog缺页:$(grep -o 'transaction [0-9]*' <<<"$out" | head -1)" "ok"
else
  n=$(PSQLQ $f1 -Atc "SELECT count(*) FROM c4_local_probe" 2>/dev/null)
  check "follower 本地事务提交" "$([[ "$n" == "100" ]] && echo ok || echo "n=$n")" "ok"
fi

echo "========== [5] ★ kill -9 follower 节点，崩溃恢复能起来 =========="
# immediate stop = 模拟崩溃（不写 checkpoint，下次启动必走崩溃恢复 redo），
# 与 kill -9 postmaster 同一条恢复路径，但不会留 postmaster.pid 撞锁。
DEXQ /work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/$fdir -m immediate stop -w -t 30 >/dev/null 2>&1
sleep 2
DEXQ /work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/$fdir start -w -t 60 \
     -l /work/pg-cluster-data/$fdir/restart.log >/dev/null 2>&1
rec=""
for t in $(seq 1 30); do
  r=$(PSQLQ $f1 -Atc "SELECT 1" 2>/dev/null)
  [[ "$r" == "1" ]] && { rec=ok; break; }
  sleep 2
done
check "follower 崩溃恢复后可连接" "$rec" "ok"
if [[ "$rec" != "ok" ]]; then
  echo "        --- 恢复失败日志 ---"
  DEXQ bash -c "grep -E 'FATAL|could not access' /work/pg-cluster-data/$fdir/restart.log | tail -3" | sed 's/^/        /'
fi

echo "========== [6] ★ VACUUM 防自旋冒烟（限时 60s）=========="
# [5] 刚做完崩溃恢复，raft/citus 后台还在热身，首次连接可能吃闭门羹 —— 重试而非一锤定音
v=notdone
for t in 1 2 3; do
  # ★ VACUUM 必须单独一条 -c：和别的语句捆在同一个 -c 里会成为隐式事务块，
  # "VACUUM cannot run inside a transaction block" —— 错误被吞掉就成了永远的假 FAIL
  if timeout 60 docker exec -i -u postgres "$CONTAINER" /work/pg-install/bin/psql -p $f1 \
      -U postgres -d postgres -q -c "VACUUM partdist.follower_partition_map" </dev/null >/dev/null 2>&1; then
    v=done; break
  fi
  sleep 20
done
check "VACUUM follower_partition_map 60s 内完成（防自旋）" "$v" "done"

health_check_no_crash

echo "========== 清理 =========="
# 红灯自救：节点起不来时按 controldata 把 pg_xact 补齐再拉起
if [[ "$rec" != "ok" ]]; then
  echo "  红灯自救：补 pg_xact 零页"
  # ★ 尺寸按**日志里报错的 xid**算，不能按 pg_controldata 的 NextXID ——
  # 后者只到崩溃前最后一次 checkpoint，而 redo 要重放的提交记录可能远超它
  # （实测 checkpoint=12030 而 redo 卡在 41377，按前者补页照样起不来）。
  DEXQ bash -c '
    d=/work/pg-cluster-data/'"$fdir"'
    fx=$(grep -oE "could not access status of transaction [0-9]+" "$d/restart.log" | tail -1 | grep -oE "[0-9]+$")
    nx=$(/work/pg-install/bin/pg_controldata "$d" | grep -i "NextXID" | grep -oE "[0-9]+$")
    big=$(( ${fx:-0} > ${nx:-0} ? ${fx:-0} : ${nx:-0} ))
    need=$(( big / 32768 + 2 ))
    f="$d/pg_xact/0000"
    have=$(( $(stat -c%s "$f" 2>/dev/null || echo 0) / 8192 ))
    while [ "$have" -lt "$need" ]; do
      dd if=/dev/zero bs=8192 count=1 >> "$f" 2>/dev/null; have=$((have+1))
    done'
  DEXQ /work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/$fdir start -w -t 90 \
       -l /work/pg-cluster-data/$fdir/restart.log >/dev/null 2>&1
fi
PSQLQ $f1 -q -c "SELECT partdist.replay_disable(${gid});" >/dev/null 2>&1
for p in $pport $f1; do PSQLQ $p -q -c "SELECT partdist.pg_raft_group_drop(${gid});" >/dev/null 2>&1; done
PSQLQ $f1 -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS c4_dist_${gid}, c4_local_probe CASCADE;" >/dev/null 2>&1
PSQLQ $COORD -q -c "DROP TABLE IF EXISTS c4_dist CASCADE;" >/dev/null 2>&1
for p in $pport $f1; do
  PSQLQ $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" >/dev/null 2>&1
done
PSQLQ $f1 -q -c "ALTER SYSTEM RESET pg_partdist.replay_trust_local_segments;" >/dev/null 2>&1
PSQLQ $f1 -q -c "SELECT pg_reload_conf();" >/dev/null 2>&1

echo
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
[[ "$FAIL" -eq 0 ]] || exit 1
