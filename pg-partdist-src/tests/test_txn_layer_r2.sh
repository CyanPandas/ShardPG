#!/usr/bin/env bash
# [宿主机] R2 事务层验收：MARKER 事务标记记录（FRD §4.3 / §7.6）。
#
# 验收标准：
#   1. 提交路径在本事务涉及的每个分区尾部落一条 MARKER 记录，
#      flags=PARTWAL_FLAG_MARKER(2)、rmid=RM_XACT_ID(1)、info=XLOG_XACT_COMMIT(0)，
#      gxid = 顶层事务的全局事务号（与它前面那批 DATA 记录同号）。
#   2. SAVEPOINT：nsubxacts 与子事务清单正确 —— 已提交的子事务在列，
#      被 ROLLBACK TO 掉的**不在**列（回滚语义由"缺席"表达，§7.6）。
#   3. 顺序不变式：MARKER 的 partition_lsn 大于它所标记的全部 DATA 记录；
#      同一个 gxid 不会既有 COMMIT 标记又有 ABORT 标记。
#   4. follower 侧收到的标记与 leader 条数一致，且回放能跨过标记继续推进
#      （标记不碰页面，不参与 redo）。
#   5. 中止路径：字节已落盘的事务失败时补一条 ABORT 标记（info=32，commit_ts=0）。
#
# 记录头字段的取法：partdist.partwal_read_record(oid, plsn) 返回
# (orig_lsn, rmid, info, flags, gxid, data)。载荷 TxnMarkerPayload 是
# 小端裸结构体：start_ts(8) commit_ts(8) nsubxacts(4) reserved(4) 后接
# nsubxacts 个 4 字节 xid。
#
# 夹具搭建照抄 test_follower_replay_r1.sh —— 那套（register_shard_fileset →
# 导出 fileset → follower 建壳表 + replay_set_locmap → rebuild_shard_identity）
# 是已验证的最小可用流程，此处不另起炉灶。
set -u

CONTAINER="${CONTAINER:-pg-citus-replay-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }

# ★★ 停节点的套件必须自带复原（本项目为此吃过亏：P7-E6 让紧随其后的套件
#   死在"节点可连"上，和它要验的东西毫无关系）。[9] 段会停两个 follower，
#   这里挂 EXIT 钩子 —— 中途 Ctrl-C / 断言 exit / set -e 早退都能复原。
#   **先登记再停**：停到一半被打断也要能救回来。
_R2_STOPPED=""
r2_restore_nodes() {
  local fp d t
  for fp in $_R2_STOPPED; do
    d="/work/pg-cluster-data/worker$((fp-5432))"
    DEX /work/pg-install/bin/pg_ctl -D "$d" -l "$d/pg.log" -o "-p $fp" start -w -t 60 >/dev/null 2>&1 || true
    for t in $(seq 1 30); do
      [[ "$(PSQL "$fp" -Atc 'SELECT 1' 2>/dev/null | tail -1)" == "1" ]] && break
      sleep 1
    done
    echo "  [收尾] 已复原节点 :$fp"
  done
  _R2_STOPPED=""
}
trap r2_restore_nodes EXIT
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

check() {  # check <名字> <实际> <期望>
  if [[ -n "$2" && "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

# 节点崩溃检查（见 lib_node_health.sh 头部：验收脚本原本对"节点崩了"是瞎的）
source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

# 小端 bytea → 整数：直接展开成内联表达式，不建辅助函数 ——
# 这些查询跑在 worker 上，Citus 会拦掉 worker 上的 CREATE FUNCTION
# （"operation is not allowed on this node"）。
# 参数是 data 里的 **0 基**字节偏移；get_byte 也是 0 基。
le32() {
  echo "((get_byte(data,$(($1+3)))::bigint<<24)|(get_byte(data,$(($1+2)))::bigint<<16)|(get_byte(data,$(($1+1)))::bigint<<8)|get_byte(data,$1)::bigint)"
}
# TxnMarkerPayload 布局：start_ts@0(8) commit_ts@8(8) nsubxacts@16(4) reserved@20(4)
# 之后是 nsubxacts 个 4 字节 xid，第 i 个（0 基）在 24+4i。
NSUB="$(le32 16)"

echo "========== [0] 前置：group0 收敛 =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

echo "========== [1] 夹具：1 分片分布表 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS r2_txn;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE r2_txn(id int primary key, v text);
SELECT create_distributed_table('r2_txn', 'id');
ALTER TABLE r2_txn SET (autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='r2_txn'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="r2_txn_${gid}"
echo "  shard=${gid} leader=:${pport}(node${pnode}) followers=:${f1}(node${f1node}) :${f2}(node${f2node})"

echo "========== [2] fileset 注册 + follower 壳表/locmap =========="
nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" | tail -1)
# text 列会带出 TOAST 堆 + TOAST 索引，所以是 4 而不是 2
check "leader fileset 注册（主堆+PK+TOAST堆+TOAST索引=4）" "$nrels" "4"

fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE r2_txn INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])")
  check "follower :$fp locmap 配对 4 对" "$np" "4"
done

for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" >/dev/null
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
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${shard_tbl}')")
  check "follower :$fp replay_enable" "$en" "t"
done

loid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})")
check "leader 本地分区 oid 解析成功" "$([[ -n "$loid" && "$loid" != "0" ]] && echo ok)" "ok"
[[ -n "$loid" && "$loid" != "0" ]] || { echo "无法解析本地分区 oid，后续用例无从执行"; exit 1; }

flush_lsn() { PSQL "$1" -Atc "SELECT partdist.get_partition_flush_lsn($2)"; }

# ★★ 找**本事务的 MARKER**，不能假设它就是流的尾记录。
#
#   冻结账目发射器（D2）按间隔往同一条流里插 CTRL 记录（rmid=255 / flags=4），
#   它随时可能追加在提交标记之后。首版直接读 flush_lsn 那一条，于是在长批次里
#   撞上发射器就整段连锁全红（实测 43/8：尾记录读成 flags=4，后面取 gxid、
#   查 gclog、核 commit_ts 全部落空）。**这不是抖动，是假设本身错了** ——
#   单跑能过只是因为没撞上。
#   改成从 upto 向前找第一条 flags=2（MARKER），最多回溯 20 条。
marker_plsn() {   # $1=partition_oid  $2=upto
  PSQL $pport -Atc "
    SELECT g FROM generate_series(${2}, GREATEST(${2}-20,1), -1) g,
         LATERAL partdist.partwal_read_record(${1}::oid, g) r
     WHERE r.flags = 2 LIMIT 1" </dev/null 2>/dev/null | tail -1
}

echo "========== [4] 简单提交：尾部 COMMIT MARKER =========="
before=$(flush_lsn $pport $loid)
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO r2_txn VALUES (1,'a'),(2,'b');"
after=$(flush_lsn $pport $loid)
check "提交产生了新记录（${before} → ${after}）" "$([[ "$after" -gt "$before" ]] && echo ok)" "ok"

mplsn=$(marker_plsn $loid $after)
row=$(PSQL $pport -Atc "
  SELECT flags||'|'||rmid||'|'||info||'|'||length(data)||'|'||${NSUB}
  FROM partdist.partwal_read_record(${loid}::oid, ${mplsn:-$after})")
check "本事务的 COMMIT MARKER 就位（flags=2 rmid=1 info=0 len=24 nsub=0）" "$row" "2|1|0|24|0"

# ★ gxid 要从 **MARKER** 那条取，不是从尾记录取 —— 尾巴上可能是冻结发射器
#   插进来的 CTRL（gxid=0），拿它去比对一比一个不中。
gx=$(PSQL $pport -Atc "SELECT gxid FROM partdist.partwal_read_record(${loid}::oid, ${mplsn:-$after})")
# ★ 期望条数也不能用 (after-before-1) 硬算：那等于假设区间里每一条都是本事务的
#   DATA，而 CTRL 记录随时会掺进来（实测 7 条 = 1 CTRL + 1 MARKER + 5 DATA，
#   硬算出 6 就必红）。断言原本要说的是"**所有** DATA 的 gxid 都与 MARKER 一致"，
#   那就按这句写：分母取区间内 DATA 的实际条数。
mend=${mplsn:-$after}
ndata_all=$(PSQL $pport -Atc "
  SELECT count(*) FROM generate_series($((before+1)), $((mend-1))) g,
       LATERAL partdist.partwal_read_record(${loid}::oid, g) r
  WHERE r.flags=1")
ndata=$(PSQL $pport -Atc "
  SELECT count(*) FROM generate_series($((before+1)), $((mend-1))) g,
       LATERAL partdist.partwal_read_record(${loid}::oid, g) r
  WHERE r.flags=1 AND r.gxid=${gx}")
# ★ 计数守卫：区间里一条 DATA 都没有的话，上面那条是 0==0 的**静默通过**。
check "  区间内确实有 DATA 记录可比（${ndata_all} 条）" \
      "$([[ -n "$ndata_all" && "$ndata_all" -ge 1 ]] && echo ok)" "ok"
check "MARKER 的 gxid 与全部 ${ndata_all} 条 DATA 记录一致" "$ndata" "$ndata_all"

# gxid 的节点号取自 pg_dist_local_group.groupid（不是端口推出来的 raft node id，
# 两者是不同的编号空间）
citus_gid=$(PSQL $pport -Atc "SELECT groupid FROM pg_dist_local_group")
node_id=$(PSQL $pport -Atc "SELECT ${gx} >> 48")
check "gxid 高 16 位 = 本节点 Citus group id (${citus_gid})" "$node_id" "$citus_gid"

echo "========== [5] SAVEPOINT：子事务清单 =========="
before=$(flush_lsn $pport $loid)
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
BEGIN;
  INSERT INTO r2_txn VALUES (10,'top');
  SAVEPOINT s1;  INSERT INTO r2_txn VALUES (11,'keep1'); RELEASE SAVEPOINT s1;
  SAVEPOINT s2;  INSERT INTO r2_txn VALUES (12,'drop');  ROLLBACK TO SAVEPOINT s2;
  SAVEPOINT s3;  INSERT INTO r2_txn VALUES (13,'keep2'); RELEASE SAVEPOINT s3;
COMMIT;
SQL
after=$(flush_lsn $pport $loid)

mplsn2=$(marker_plsn $loid $after)
mk=$(PSQL $pport -Atc "
  SELECT flags||'|'||info||'|'||${NSUB}||'|'||length(data)
  FROM partdist.partwal_read_record(${loid}::oid, ${mplsn2:-$after})")
nsub=$(echo "$mk" | cut -d'|' -f3); len=$(echo "$mk" | cut -d'|' -f4)
check "SAVEPOINT 事务的 COMMIT MARKER 就位" "$(echo "$mk" | cut -d'|' -f1,2)" "2|0"
check "nsubxacts > 0（确实记下了子事务，nsub=${nsub}）" "$([[ -n "$nsub" && "$nsub" -gt 0 ]] && echo ok)" "ok"
check "载荷长度 = 24 + 4*nsubxacts" "$len" "$((24 + 4*${nsub:-0}))"

# 第 i 个子事务（1 基 i）在 0 基偏移 24+4*(i-1)，逐字节小端拼
sublist=$(PSQL $pport -Atc "
  SELECT string_agg(((get_byte(data, 24+4*(i-1)+3)::bigint<<24)
                    |(get_byte(data, 24+4*(i-1)+2)::bigint<<16)
                    |(get_byte(data, 24+4*(i-1)+1)::bigint<<8)
                    | get_byte(data, 24+4*(i-1))::bigint)::text, ',' ORDER BY i)
  FROM partdist.partwal_read_record(${loid}::oid, ${after}), generate_series(1, ${nsub:-0}) i")
dataxids=$(PSQL $pport -Atc "
  SELECT string_agg(DISTINCT (gxid & ((1::bigint<<48)-1))::text, ',')
  FROM generate_series($((before+1)), $((after-1))) g,
       LATERAL partdist.partwal_read_record(${loid}::oid, g) r WHERE r.flags=1")
topxid=$(PSQL $pport -Atc "SELECT (gxid & ((1::bigint<<48)-1)) FROM partdist.partwal_read_record(${loid}::oid, ${after})")
echo "  子事务清单=[${sublist}]  流内 DATA xid=[${dataxids}]  顶层 xid=${topxid}"

# 写过数据、既不是顶层、也不在提交清单里的 xid —— 只可能是被 ROLLBACK TO 掉的那个
dropped=0
IFS=',' read -ra XS <<< "$dataxids"
for x in "${XS[@]}"; do
  [[ "$x" == "$topxid" ]] && continue
  echo ",${sublist}," | grep -q ",${x}," || dropped=$((dropped+1))
done
check "被 ROLLBACK TO 的子事务未进入 COMMIT 清单" "$([[ "$dropped" -ge 1 ]] && echo ok)" "ok"
# 反向：写过数据且已提交的子事务必须在清单里
kept=0
for x in "${XS[@]}"; do
  [[ "$x" == "$topxid" ]] && continue
  echo ",${sublist}," | grep -q ",${x}," && kept=$((kept+1))
done
check "已提交的子事务在 COMMIT 清单里（kept=${kept}）" "$([[ "$kept" -ge 2 ]] && echo ok)" "ok"

echo "========== [6] 顺序不变式 =========="
bad=$(PSQL $pport -Atc "
  WITH r AS (SELECT g AS plsn, x.flags, x.gxid
             FROM generate_series(1, ${after}) g,
                  LATERAL partdist.partwal_read_record(${loid}::oid, g) x)
  SELECT count(*) FROM r m JOIN r d ON d.gxid = m.gxid AND d.flags = 1
  WHERE m.flags = 2 AND d.plsn > m.plsn")
check "MARKER 排在同事务全部 DATA 之后" "$bad" "0"

dup=$(PSQL $pport -Atc "
  WITH r AS (SELECT x.flags, x.info, x.gxid
             FROM generate_series(1, ${after}) g,
                  LATERAL partdist.partwal_read_record(${loid}::oid, g) x
             WHERE x.flags = 2)
  SELECT count(*) FROM (SELECT gxid FROM r GROUP BY gxid HAVING count(DISTINCT info) > 1) z")
check "同一 gxid 不会既 COMMIT 又 ABORT" "$dup" "0"

lmark=$(PSQL $pport -Atc "
  SELECT count(*) FROM generate_series(1, ${after}) g,
       LATERAL partdist.partwal_read_record(${loid}::oid, g) r WHERE r.flags = 2")
check "leader 侧共有 ${lmark} 条 MARKER（>=2）" "$([[ "$lmark" -ge 2 ]] && echo ok)" "ok"

echo "========== [7] follower 收到标记并能回放跨过 =========="
for fp in $f1 $f2; do
  foid=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${gid})")
  fl=$(flush_lsn $fp "$foid")
  check "follower :$fp 收齐 ${after} 条记录（本地 ${fl}）" \
        "$([[ -n "$fl" && "$fl" -ge "$after" ]] && echo ok)" "ok"

  fmark=$(PSQL $fp -Atc "
    SELECT count(*) FROM generate_series(1, ${after}) g,
         LATERAL partdist.partwal_read_record(${foid}::oid, g) r WHERE r.flags = 2")
  check "follower :$fp 的 MARKER 条数与 leader 一致" "$fmark" "$lmark"

  app=$(PSQL $fp -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${after}, 120000)" 2>&1 | tail -1)
  check "follower :$fp 回放跨过 MARKER 追平到 ${after}（返回 ${app}）" \
        "$([[ "$app" == "$after" ]] && echo ok)" "ok"

  rows=$(PSQL $fp -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM ${shard_tbl}" | tail -1)
  echo "    （follower 壳表行数=${rows}；R2-b 只登记标记，可见性判定待 R2-d 增强型 CLOG）"
done

echo "========== [7.5] 增强型 CLOG（pg_gclog）核账 =========="
# R2-d：MARKER 的判决要真正落进 pg_gclog/<node_id>/。判据不是"多读出几行"这类
# 间接现象，而是直接问账本：每个 xid 判成什么了。
citus_gid=$(PSQL $pport -Atc "SELECT groupid FROM pg_dist_local_group")
for fp in $f1 $f2; do
  # 顶层事务与已 RELEASE 的子事务 → committed
  ncommit=0; nrunning=0
  for x in "${XS[@]}"; do
    st=$(PSQL $fp -Atc "SELECT status FROM partdist.gclog_status(${citus_gid}, ${x})" 2>/dev/null)
    if [[ "$x" == "$topxid" ]] || echo ",${sublist}," | grep -q ",${x},"; then
      [[ "$st" == "committed" ]] && ncommit=$((ncommit+1))
    else
      # 写过数据但不在提交清单里 = 被 ROLLBACK TO 的子事务，
      # 它在 gclog 里应当**没有记录**（running = 空洞 = 未决 = 不可见）。
      # 回滚语义靠"缺席"表达，不写 ABORTED（FRD §7.6）。
      [[ "$st" == "running" ]] && nrunning=$((nrunning+1))
    fi
  done
  check "follower :$fp gclog 里已提交事务判为 committed（${ncommit} 个）" \
        "$([[ "$ncommit" -ge 2 ]] && echo ok)" "ok"
  check "follower :$fp gclog 里被 ROLLBACK TO 的子事务无记录（running）" \
        "$([[ "$nrunning" -ge 1 ]] && echo ok)" "ok"

  # committed 的 commit_ts 必须非 0，且与 MARKER 载荷里的一致
  cts=$(PSQL $fp -Atc "SELECT commit_ts FROM partdist.gclog_status(${citus_gid}, ${topxid})")
  check "follower :$fp 顶层事务的 commit_ts 非 0" \
        "$([[ -n "$cts" && "$cts" -gt 0 ]] && echo ok)" "ok"

  # 从未回放过的 xid → running（稀疏空洞的默认值必须是"未决"，这是安全兜底）
  st=$(PSQL $fp -Atc "SELECT status FROM partdist.gclog_status(${citus_gid}, 2000000000)")
  check "follower :$fp 未记账的 xid 默认 running（不可见兜底）" "$st" "running"
done

echo "========== [8] 崩溃一致性：判决必须活过 kill -9（§8.4 推进协议）=========="
# R2-d 把 EnhancedClogSync() 插在写游标**之前**。这一节验证那个顺序真的管用：
# 追平途中 kill -9 → 整个节点重置 → 重新追平 → 已提交事务的判决必须仍然是
# committed。顺序若反了，崩溃后游标已跨过那批 MARKER 而判决还在 page cache 里，
# 重放不到、也补不回来 —— 表现为一笔永久 running 的事务。
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO r2_txn SELECT g, 'crash'||g FROM generate_series(200,260) g;"
lead_now=$(flush_lsn $pport $loid)
f1oid=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${gid})")

( PSQL $f1 -Atc "SELECT partdist.replay_catchup(${f1oid}::regclass, ${lead_now}, 180000)" >/dev/null 2>&1 ) &
cbg=$!
sleep 3
fdir="worker$((f1 - 5432))"
wpid=$(docker exec -u postgres $CONTAINER bash -c "
  for pid in \$(pgrep -f '[r]eplay worker'); do
    [ \"\$(readlink /proc/\$pid/cwd 2>/dev/null)\" = \"/work/pg-cluster-data/${fdir}\" ] && { echo \$pid; break; }
  done")
if [[ -n "$wpid" ]]; then
  docker exec -u postgres $CONTAINER kill -9 "$wpid" 2>/dev/null
  echo "  已 kill -9 replay worker(pid=$wpid)，节点将整体重置"
  wait "$cbg" 2>/dev/null || true
  for t in $(seq 1 60); do
    [[ "$(PSQL $f1 -Atc 'SELECT 1' 2>/dev/null)" == "1" ]] && break; sleep 1
  done
  check "f1 节点从重置中恢复" "$(PSQL $f1 -Atc 'SELECT 1' 2>/dev/null)" "1"

  PSQL $f1 -q -c "SELECT partdist.replay_enable('${shard_tbl}');" >/dev/null 2>&1
  lead_now=$(flush_lsn $pport $loid)
  app=$(PSQL $f1 -Atc "SELECT partdist.replay_catchup(${f1oid}::regclass, ${lead_now}, 180000)" 2>&1 | tail -1)
  check "崩溃后重新追平到 ${lead_now}（返回 ${app}）" \
        "$([[ "$app" == "$lead_now" ]] && echo ok)" "ok"

  # 崩溃前后的全部已提交事务，判决必须都还在
  stillok=$(PSQL $f1 -Atc "
    SELECT count(*) FROM generate_series(${topxid}, ${topxid}) x,
         LATERAL partdist.gclog_status(${citus_gid}, x) g
    WHERE g.status = 'committed'")
  check "崩溃前那笔事务的判决仍是 committed" "$stillok" "1"

  nrun=$(PSQL $f1 -Atc "
    SELECT count(*) FROM (
      SELECT (gxid & ((1::bigint<<48)-1)) AS lx
      FROM generate_series(1, ${lead_now}) g,
           LATERAL partdist.partwal_read_record(${f1oid}::oid, g) r
      WHERE r.flags = 2 AND r.info = 0) m,
      LATERAL partdist.gclog_status(${citus_gid}, m.lx) s
    WHERE s.status <> 'committed'")
  # ★ 计数守卫：这是"没有 X 不满足条件"型的否定式判据，扫到零条标记时
  # count(*) 恒为 0 而报通过。而它前一步刚做完 kill -9 + 整节点重置，
  # 正是段流状态最容易异常的时刻（本地流被重建/截断、f1oid 换号都会让内层
  # 子查询返回空集）。必须先证明确实扫到了标记，这条断言才有意义。
  nmark=$(PSQL $f1 -Atc "
    SELECT count(*) FROM generate_series(1, ${lead_now}) g,
         LATERAL partdist.partwal_read_record(${f1oid}::oid, g) r
     WHERE r.flags = 2 AND r.info = 0")
  check "崩溃恢复后段流里仍能扫到 COMMIT 标记（${nmark} 条 >= 1）" \
        "$([[ -n "$nmark" && "$nmark" -ge 1 ]] && echo ok)" "ok"
  check "段流里每条 COMMIT 标记在 gclog 里都是 committed（掉队 ${nrun} 个）" "$nrun" "0"
else
  check "找到 f1 的 replay worker（崩溃用例前置）" "" "ok"
fi

echo "========== [9] 中止路径：ABORT 标记 =========="
# ★★ 本节**故意**拆掉多数派 —— 那必然让 quorum_drops +1，而收尾那句
#   "本轮无 Raft 提案被丢弃" 就此**结构上永远不可能过**：它在断言本节自己刚
#   造出来的东西。这条红被当成 R-P4-22 挂了很久，其实测错了对象。
#   丢弃计数的含义在 8/3 之后也变了 —— 增量点的注释写着"leader 侧不再截断
#   parwal，字节留作孤儿、同 plsn 重新 propose，多数派恢复后自然收敛，
#   丢弃不再直接等于无痕分叉"，它是**复制健康度**的观测口。
#   做法同"故意 kill -9 之后重开崩溃窗口"：本节收尾处重取基线，让收尾那句回去
#   测**非预期**的丢弃。
# 让 leader 凑不齐多数派 → 复制挂钩 ERROR → 事务中止。
# 此时 DATA 记录已落盘（[A] 在挂钩之前），必须补一条 ABORT 标记。
#
# ★★ 2026-09-12 改手段：原先用 `pg_raft_group_reset()` 拆掉两个 follower 的组，
#   **那个前提已经被产品演进作废了**。`raft_consensus.c:4280` 起有一段有意设计：
#     /* follower 可能是第一次听说这个数据组：按 leader 的通告自动建组 */
#   并且注释写明「成员集未知只剥夺**主动**参与（竞选/当选/提案），不剥夺被动接收
#   —— 否则 hearsay 引导路径被砍，全新分片永远建不起来」。
#   于是 reset 之后第一条 AppendEntries 就把组**自动重建**回来，follower 照常
#   落盘并 ack，多数派毫发无损 ⇒ 写入成功 ⇒ 本节四条断言齐红
#   （实测：期望 ABORT MARKER info=32，实得 info=0 的 COMMIT 标记）。
#   这不是产品缺陷，是**用例的手段失效**：它测的是一个已经不存在的失败模式。
#
#   改用**自动建组救不回来**的手段：直接停掉这两个节点 —— 停机的节点不会 ack，
#   leader 的多数派算术（按它自己的成员集做）就真的凑不齐。
#   停完必复原（见 [收尾]）。
for fp in $f1 $f2; do
  _R2_STOPPED+="$fp "          # 先登记再停
  DEX /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/worker$((fp-5432))" -m fast stop >/dev/null 2>&1 || true
done
sleep 3
before=$(flush_lsn $pport $loid)
errcnt=$(PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO r2_txn VALUES (90,'abort-me');" 2>&1 | grep -c "ERROR")
after=$(flush_lsn $pport $loid)
check "写入确实被拒（事务中止）" "$([[ "$errcnt" -ge 1 ]] && echo ok)" "ok"
check "中止事务的 DATA 记录已落盘（${before} → ${after}）" \
      "$([[ "$after" -gt "$before" ]] && echo ok)" "ok"

if [[ "$after" -gt "$before" ]]; then
  arow=$(PSQL $pport -Atc "
    SELECT flags||'|'||rmid||'|'||info||'|'||length(data)
    FROM partdist.partwal_read_record(${loid}::oid, ${after})")
  check "中止事务尾部是 ABORT MARKER（flags=2 rmid=1 info=32 len=24）" "$arow" "2|1|32|24"

  cts=$(PSQL $pport -Atc "
    SELECT $(le32 8) + $(le32 12)
    FROM partdist.partwal_read_record(${loid}::oid, ${after})")
  check "ABORT 标记的 commit_ts = 0" "$cts" "0"

  axid=$(PSQL $pport -Atc "SELECT gxid FROM partdist.partwal_read_record(${loid}::oid, ${after})")
  nc=$(PSQL $pport -Atc "
    SELECT count(*) FROM generate_series(1, ${after}) g,
         LATERAL partdist.partwal_read_record(${loid}::oid, g) r
    WHERE r.flags = 2 AND r.gxid = ${axid} AND r.info = 0")
  check "该 gxid 没有 COMMIT 标记（标记在复制成功后才写）" "$nc" "0"

  # 中止事务落到 follower 的 gclog 里应当是 aborted（commit_ts=0）。
  # 注意本轮两个 follower 是**停机**状态，ABORT 标记还没被复制过去 ——
  # 所以这里查的是 leader 侧本地回放不到的情况，只核对 leader 段流已写下标记；
  # follower 侧的 aborted 判决要等下一次该分区有写入把标记带过去（FRD §4.3）。
  alocal=$(PSQL $pport -Atc "SELECT ${axid} & ((1::bigint<<48)-1)")
  echo "    （中止事务 xid=${alocal}；ABORT 标记未复制到 follower，"
  echo "      其 gclog 判决要等该分区下一次写入带过去 —— 未决=不可见，语义安全）"
fi

# ★ 本段结束就复原，不等 EXIT：[10] 起的若干段还要用这两个 follower
#   （replay_freeze_status / 回放追平等），躺着的节点会让它们以
#   "取不到值"的形式连片变红，而那与被测内容毫无关系。
r2_restore_nodes
# 组也要重建：停机期间 leader 可能已把它们踢出感知，且本节点的组记录还在，
# 但为稳妥起见按原成员集重新 ensure 一次，失败不致命。
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null 2>&1 || true
done
sleep 2

echo "========== [10] 冻结账目暴露面（FRD §13 约束 5）=========="
# autovacuum_enabled=off 挡不住防回卷：autovacuum.c 里是
#   if (!av_enabled && !force_vacuum)   /* But ignore if at risk */
# 所以副本壳表迟早会被强制 vacuum 扫到，而它的元组带外来 xid。
# 本期只把暴露面做成可观测的，处置需要 CTRL 记录通道（§12）。
for fp in $f1 $f2; do
  nrep=$(PSQL $fp -Atc "SELECT count(*) FROM partdist.replay_freeze_status()" 2>/dev/null)
  check "follower :$fp replay_freeze_status 可查（${nrep} 个副本壳表）" \
        "$([[ -n "$nrep" && "$nrep" -ge 1 ]] && echo ok)" "ok"
  noff=$(PSQL $fp -Atc "SELECT count(*) FROM partdist.replay_freeze_status() WHERE NOT autovacuum_off" 2>/dev/null)
  check "follower :$fp 全部副本壳表 autovacuum 已关" "$noff" "0"
  worst=$(PSQL $fp -Atc "SELECT coalesce(max(pct_to_force),0)::int FROM partdist.replay_freeze_status()" 2>/dev/null)
  echo "    （距强制回卷阈值最近的副本壳表：${worst}%）"
done

echo ""
health_check_no_crash
# ★ 额度 2：本套件 [9] **故意**拆掉多数派来验 ABORT 标记，那必然产生
#   1~2 次 quorum_drop（实测两者都出现过）。不给额度这条断言永远红 ——
#   它在断言本套件自己造出来的东西，被当成 R-P4-22 挂了很久。
#   额度之外多一次仍然红，非预期的丢弃照样抓得住。
health_check_no_drops 2
health_check_worker_pool
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "R2 事务层验收：全部通过"; else echo "R2 事务层验收：存在 FAIL"; fi

# ---- 清理 ----
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
  PSQL $COORD -q -c "DROP TABLE IF EXISTS r2_txn;" >/dev/null 2>&1
  for p in $COORD $pport $f1 $f2; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" >/dev/null 2>&1
  done
fi

exit $FAIL
