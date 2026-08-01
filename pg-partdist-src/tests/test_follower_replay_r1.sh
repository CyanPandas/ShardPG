#!/usr/bin/env bash
# [宿主机] R1 物理回放闭环验收（FRD §14.2 R1 行）。
#
# 验收标准：
#   1. 带索引 + TOAST 的表，leader 写入后 follower 文件与 leader
#      **逐页 diff 一致（含 LSN 域）**——MAIN + VM fork 逐字节 cmp；
#      FSM fork 排除（FSM 更新本就不写 WAL，原生备库同样不保证一致）。
#   2. kill -9 replay worker 任意时刻，重启追平且 diff 仍一致。
#   3. 用例显式制造一次 VACUUM 尾部截断（覆盖 §5.2 的 RM_SMGR 捕获特判）。
#
# 页面字节级一致的前提（都是本用例刻意安排的）：
#   - 写入后不在 leader 上读该表（hint bits 不写 WAL）；
#   - 最后 VACUUM (FREEZE)：freeze 记录整体重写 t_infomask，扫描期
#     顺手设置的 hint 位被覆盖成 canonical 状态，两侧收敛。
#
# 传输走既有 pg_raft 数据组复制（prepare 接线），回放上界用测试模式
# GUC replay_trust_local_segments=on（FRD §6 的独立验收通道）。
set -u

CONTAINER="${CONTAINER:-pg-citus-replay-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

check() {  # check <名字> <实际> <期望>
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

echo "========== [0] 前置：group0 收敛 + 测试模式 GUC =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" >/dev/null
done
gucv=$(PSQL 5434 -Atc "SHOW pg_partdist.replay_trust_local_segments")
check "测试模式 GUC 已生效" "$gucv" "on"

echo "========== [1] 夹具：1 分片分布表（PK 索引 + TOAST） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS r1_replay;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE r1_replay(id int primary key, v text, big text);
SELECT create_distributed_table('r1_replay', 'id');
ALTER TABLE r1_replay SET (autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='r1_replay'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport - 5431))
# 两个 follower：取除 leader 外端口最小的两个 worker
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
echo "  shard=${gid} leader=:${pport}(node${pnode}) followers=:${f1}(node${f1node}) :${f2}(node${f2node})"

shard_tbl="r1_replay_${gid}"

echo "========== [2] leader fileset 注册 + follower 壳表/locmap =========="
nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" | tail -1)
check "leader fileset 注册（主堆+PK+TOAST堆+TOAST索引=4）" "$nrels" "4"

# 导出 leader fileset（role,ord,spc,db,relnum 数组）
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE r1_replay INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])")
  check "follower :$fp locmap 配对 4 对" "$np" "4"
done

for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
done

echo "========== [3] Raft 数据组 + 开启回放 =========="
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 ${gid} leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
done

for fp in $f1 $f2; do
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${shard_tbl}')")
  check "follower :$fp replay_enable" "$en" "t"
done

echo "========== [4] 批次 A：INSERT/UPDATE/DELETE（含 TOAST） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO r1_replay
SELECT g, 'v'||g,
       CASE WHEN g % 10 = 0
            THEN (SELECT string_agg(md5((g*1000+i)::text), '') FROM generate_series(1,300) i)
            ELSE 'small' END
FROM generate_series(1, 1200) g;
SQL
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE r1_replay SET v = v||'-u1' WHERE id % 3 = 0;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "DELETE FROM r1_replay WHERE id % 17 = 0;"

leader_oid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})")
f1_oid=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${gid})")
lead_plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
check "leader 已产生 parwal 记录" "$([[ "$lead_plsn" -gt 100 ]] && echo ok)" "ok"

wait_caught_up() {  # wait_caught_up <fport> <期望plsn> <超时s>
  local fp=$1 target=$2 timeout=$3 t app=0
  local foid; foid=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${gid})")
  for t in $(seq 1 "$timeout"); do
    app=$(PSQL $fp -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}" 2>/dev/null || echo 0)
    [[ -n "$app" && "$app" -ge "$target" ]] && { echo "$app"; return 0; }
    sleep 1
  done
  echo "$app"; return 1
}

app1=$(wait_caught_up $f1 "$lead_plsn" 90)
check "follower1 追平批次 A（applied=${app1} / ${lead_plsn}）" "$([[ "$app1" -ge "$lead_plsn" ]] && echo ok)" "ok"

echo "========== [5] kill -9 replay worker，批次 B 期间恢复 =========="
wpid=$(docker exec -u postgres $CONTAINER bash -c "ps -u postgres -o pid=,cmd= | grep '[r]eplay worker' | head -1 | awk '{print \$1}'")
check "找到 replay worker 进程" "$([[ -n "$wpid" ]] && echo ok)" "ok"

PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL' &
INSERT INTO r1_replay
SELECT g, 'b'||g,
       CASE WHEN g % 10 = 0
            THEN (SELECT string_agg(md5((g*7777+i)::text), '') FROM generate_series(1,300) i)
            ELSE 'small-b' END
FROM generate_series(2001, 3200) g;
SQL
WRITER_PID=$!
sleep 2
docker exec -u postgres $CONTAINER kill -9 "$wpid" 2>/dev/null
echo "  已 kill -9 worker(pid=$wpid)，等待写入完成 + launcher 重拉"
wait $WRITER_PID
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE r1_replay SET v = v||'-u2' WHERE id % 5 = 0;"

lead_plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
app1=$(wait_caught_up $f1 "$lead_plsn" 120)
check "kill -9 后 follower1 重启追平（applied=${app1} / ${lead_plsn}）" "$([[ "$app1" -ge "$lead_plsn" ]] && echo ok)" "ok"

echo "========== [6] VACUUM 尾部截断（RM_SMGR 特判验收） =========="
# 删掉尾部大段行 → VACUUM 截断尾页 → XLOG_SMGR_TRUNCATE 进流
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "DELETE FROM r1_replay WHERE id > 600;"
before_sz=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_size('${shard_tbl}')")
PSQL $pport -v ON_ERROR_STOP=1 -q -c "SET citus.override_table_visibility=false; VACUUM (FREEZE) ${shard_tbl};"
after_sz=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_size('${shard_tbl}')")
check "leader 侧 VACUUM 确实截断了尾页" "$([[ "$after_sz" -lt "$before_sz" ]] && echo ok)" "ok"

lead_plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
app1=$(wait_caught_up $f1 "$lead_plsn" 90)
check "follower1 追平（含 SMGR 截断记录）" "$([[ "$app1" -ge "$lead_plsn" ]] && echo ok)" "ok"
app2=$(wait_caught_up $f2 "$lead_plsn" 90)
check "follower2 追平" "$([[ "$app2" -ge "$lead_plsn" ]] && echo ok)" "ok"

echo "========== [7] 刷盘 + 逐页 diff（MAIN + VM，含 LSN 域） =========="
PSQL $pport -q -c "CHECKPOINT;" >/dev/null
sleep 4   # > replay_checkpoint_interval_ms，等 follower 的 apply checkpoint 刷脏落盘

pdata=$(PSQL $pport -Atc "SHOW data_directory")
f1data=$(PSQL $f1 -Atc "SHOW data_directory")
f2data=$(PSQL $f2 -Atc "SHOW data_directory")

# 逐 fileset 成员 diff：用两侧 pg_relation_filepath 配对（role,ord 序一致）
lead_paths=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||'.'||ord||','||pg_relation_filepath(relnum::regclass) FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')

diff_one_follower() {  # <fport> <fdata> <标签>
  local fp=$1 fdata=$2 tag=$3
  local frows
  frows=$(PSQL $fp -Atc "SET citus.enable_ddl_propagation=off; SELECT role||'.'||ord||','||pg_relation_filepath(relnum::regclass) FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')
  while IFS= read -r lrow; do
    local key lrel frel lpath fpath fork
    key=${lrow%%,*}; lrel=${lrow#*,}
    frel=$(echo "$frows" | grep "^${key}," | cut -d, -f2)
    for fork in "" "_vm"; do
      lpath="${pdata}/${lrel}${fork}"; fpath="${fdata}/${frel}${fork}"
      local lex fex
      lex=$(DEX bash -c "test -f '$lpath' && echo y || echo n")
      fex=$(DEX bash -c "test -f '$fpath' && echo y || echo n")
      if [[ "$lex" == "n" && "$fex" == "n" ]]; then continue; fi
      check "${tag} ${key}${fork:-.main} 两侧都存在" "$lex/$fex" "y/y"
      [[ "$lex" == "y" && "$fex" == "y" ]] || continue
      local lsz fsz
      lsz=$(DEX stat -c %s "$lpath"); fsz=$(DEX stat -c %s "$fpath")
      check "${tag} ${key}${fork:-.main} 大小一致(${lsz})" "$fsz" "$lsz"
      local same
      same=$(DEX bash -c "cmp -s '$lpath' '$fpath' && echo identical || echo DIFF")
      check "${tag} ${key}${fork:-.main} 逐字节一致" "$same" "identical"
    done
  done <<< "$lead_paths"
}

diff_one_follower $f1 "$f1data" "f1"
diff_one_follower $f2 "$f2data" "f2"

echo "========== [8] follower 壳表仍不可见数据（只物理回放，不改可见性） =========="
cnt=$(PSQL $f1 -Atc "SELECT count(*) FROM ${shard_tbl}")
check "follower 壳表 SELECT count = 0（xid 均非本地，R2 前不可见）" "$cnt" "0"

echo
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then
  echo "R1 验收：全部通过"
else
  echo "R1 验收：存在 FAIL"
fi

# ---- 清理（保留环境可复查；夹具删除） ----
if [[ "${KEEP_FIXTURE:-0}" != "1" ]]; then
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" >/dev/null 2>&1
  done
  for p in $pport $f1 $f2; do
    PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" >/dev/null 2>&1
  done
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" >/dev/null 2>&1
  done
  PSQL $COORD -q -c "DROP TABLE IF EXISTS r1_replay;" >/dev/null 2>&1
  for p in $COORD $pport $f1 $f2; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" >/dev/null 2>&1
  done
fi

exit $FAIL
