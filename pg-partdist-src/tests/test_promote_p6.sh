#!/usr/bin/env bash
# [宿主机] T6.3b + T6.4 验收：升主的两件收尾（DEV PLAN §3.8，解冻批次 #6）。
#
# 两件事都发生在"追平之后、上报之前"，都在 pg_raft_promote_prepare 里：
#
#   T6.3b 推进本地 WAL 插入位点（FRD §11 步骤 4，内核补丁 0010）
#     物理回放出来的页带的是 **leader 坐标**的 LSN。分片一升主，这些页改由
#     本地 XLogInsert 保护；本地插入位点若还低于页面现有 LSN，新记录的 LSN
#     就小于页 LSN，本地崩溃恢复时 `lsn <= PageGetLSN(page)` 会把新记录当成
#     "页面已经更新过"**直接跳过** —— 升主后写进去的数据在一次崩溃后消失。
#
#   T6.4 切主认领（§6.6 第三支）
#     新主追平后流里没有该事务的提交标记 ⇒ 从未提交过 ⇒ 改 ABORTED。
#     依据：提交的必要条件是提交标记已多数派入流，raft 选举保证新主拥有
#     全部多数派条目。
#
# ★ 本套件刻意钉住三件"容易假通过"的事：
#   ① 位点跳跃**不能只看 insert_lsn 变大** —— 还要看**检查点 redo** 也落到
#      新段内。redo 没跟上就等于"跳了却没有检查点"，崩溃时新段里的已提交
#      记录一条都读不回来。这正是补丁 0010 把跳跃塞进 CreateCheckPoint
#      临界区的理由，也是唯一能证明它成立的判据。
#   ② 认领**必须证明老路径不管用**：T2.4 的 ShardXidEnsureClaimed 只在"槽位
#      不存在"时认领，而升主的 follower 上槽位一定已存在。不先断言老路径
#      返回 0，T6.4 就可能是在给一件本来就会发生的事记功。
#   ③ 认领**不能误伤 PREPARED**（§6.6 第二支）。少了这条阴性对照，
#      "把区间内全改成 ABORTED"这种错实现照样满分。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/t63b_promote.lock
if ! flock -n 9; then echo "FATAL: 另一个 T6.3b/T6.4 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "================ [0] 夹具：1 分片 + raft 组 + 2 follower ================"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t63b;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t63b(id int, v text);
SELECT create_distributed_table('t63b', 'id');
ALTER TABLE t63b SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t63b'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="t63b_${gid}"
echo "  shard=${gid} leader=:${pport} 待升主 follower=:${f1} 陪跑=:${f2}"
check "夹具齐" "$([[ -n "$gid" && -n "$pport" && -n "$f1" ]] && echo ok)" "ok"

nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册" "$nrels" "3"
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows"|cut -d, -f1|paste -sd,); ords=$(echo "$fsrows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$fsrows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$fsrows"|cut -d, -f4|paste -sd,)
rels=$(echo "$fsrows"|cut -d, -f5|paste -sd,)
SETLOC() { PSQL "$1" -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[], ${2}::bigint)" </dev/null 2>/dev/null | tail -1; }
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE t63b INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
  n=$(SETLOC $fp 0)
  check "follower :$fp 空表 base=0 配对" "$n" "3"
done
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
check "leader 认得本地分片" "$([[ -n "$LEADER_OID" && "$LEADER_OID" != "0" ]] && echo ok)" "ok"
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
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null
done
PLSN() { PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1; }
FOID() { PSQL "$1" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1; }
CATCH() {
  local fp=$1 target=$2 foid
  [[ -n "$target" && "$target" -gt 0 ]] || { echo ""; return 1; }
  foid=$(FOID "$fp")
  [[ -n "$foid" && "$foid" != "0" ]] || { echo ""; return 1; }
  PSQL "$fp" -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${target}, 60000)" </dev/null 2>/dev/null | tail -1
}

echo "================ [1] ★ 造出"本地位点落后于 leader 页 LSN"的几何 ================"
# 不造几何就没得可验：两台机器的本地 LSN 各走各的，follower 未必天然落后。
# 用我们自己的原语把 **leader** 推远，让它写出的页带上远大于 follower 本地
# 位点的 LSN —— 这既是最确定的造法，也顺带证明原语在 leader 上同样可用。
lead_before=$(PSQL $pport -Atc "SELECT pg_current_wal_insert_lsn()" </dev/null)
tgt=$(PSQL $pport -Atc "SELECT (pg_current_wal_insert_lsn() + 3000000000)::pg_lsn" </dev/null)
PSQL $pport -q -c "SELECT partdist.advance_wal_to('${tgt}')" </dev/null >/dev/null
lead_after=$(PSQL $pport -Atc "SELECT pg_current_wal_insert_lsn()" </dev/null)
echo "  leader 位点：$lead_before → $lead_after"
check "leader 位点已推远（几何前提）" \
      "$(PSQL $pport -Atc "SELECT '${lead_after}'::pg_lsn > '${lead_before}'::pg_lsn" </dev/null)" "t"

# ★★ 必须先打标。不打标写进去的是**原生 xid**，回放不会推进分片分配器水位
#   （T6.5 的新语义只对分片 xid 宇宙生效），于是 [claim_wm, watermark) 恒为空区间，
#   T6.4 的认领无论对错都返回 0 —— 首版正是漏了这步，8 条红里 5 条源于此，
#   且症状极具迷惑性：认领"成功返回 0"看着像空转，其实是**根本没有可认领的号**。
SOID=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT '${shard_tbl}'::regclass::oid" </dev/null|tail -1)
PSQL $pport -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${SOID}';" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL $pport -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$SOID" ]] && break; sleep 1
done
check "leader 白名单生效（分片 xid 宇宙的前提）" "$guc" "$SOID"

PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'r'||g FROM generate_series(1,50) g;" </dev/null >/dev/null
p1=$(PLSN); a=$(CATCH $f1 "$p1")
check "follower 追平 50 行（applied=$a）" "$([[ -n "$a" && "$a" -ge "$p1" ]] && echo ok)" "ok"
foid=$(FOID $f1)
MAXORIG=$(PSQL $f1 -Atc "SELECT max_orig FROM partdist.replay_status() WHERE shard=${foid}" </dev/null|tail -1)
F_INS0=$(PSQL $f1 -Atc "SELECT pg_current_wal_insert_lsn()" </dev/null)
F_REDO0=$(PSQL $f1 -Atc "SELECT redo_lsn FROM pg_control_checkpoint()" </dev/null)
echo "  follower 本地位点=$F_INS0  检查点 redo=$F_REDO0  max_orig_lsn=$MAXORIG"
check "★ 几何成立：follower 本地位点确实低于 leader 坐标页 LSN" \
      "$(PSQL $f1 -Atc "SELECT '${F_INS0}'::pg_lsn < '${MAXORIG}'::pg_lsn" </dev/null)" "t"

echo "================ [2] ★ T6.3b：推进位点 ================"
RET=$(PSQL $f1 -Atc "SELECT partdist.advance_wal_past_shard(${foid}::regclass)" </dev/null|tail -1)
F_INS1=$(PSQL $f1 -Atc "SELECT pg_current_wal_insert_lsn()" </dev/null)
F_REDO1=$(PSQL $f1 -Atc "SELECT redo_lsn FROM pg_control_checkpoint()" </dev/null)
echo "  推进后：位点=$F_INS1  redo=$F_REDO1（返回 $RET）"
check "★★ 插入位点已越过 max_orig_lsn" \
      "$(PSQL $f1 -Atc "SELECT '${F_INS1}'::pg_lsn > '${MAXORIG}'::pg_lsn" </dev/null)" "t"
# ★ 这条才是补丁 0010 的真判据：跳完必须有一条检查点记录落在新段里。
#   只断言 insert_lsn 变大的话，"跳了却没有检查点"那种丢数据实现照样满分。
check "★★ 检查点 redo 也已越过 max_orig_lsn（零窗口的判据）" \
      "$(PSQL $f1 -Atc "SELECT '${F_REDO1}'::pg_lsn > '${MAXORIG}'::pg_lsn" </dev/null)" "t"
# ★ 不能断言"返回值 == 随后查到的插入位点"：两次查询之间集群还在写 WAL
#   （raft 心跳等），单跑时空闲恰好相等，进了批次就必红（实测 T6.8 出口门禁
#   4/1C00CF08 vs 4/1C00D050）。**判据要写成对时序不敏感的形式**：
#   返回值必须落在跳跃后那个新段内，且不大于随后读到的位点（单调）。
check "  返回值落在新段内且单调不超过随后读数" \
      "$(PSQL $f1 -Atc "SELECT ('${RET}'::pg_lsn <= '${F_INS1}'::pg_lsn) AND ('${RET}'::pg_lsn > '${MAXORIG}'::pg_lsn)" </dev/null)" "t"

echo "================ [2b] 幂等：再调一次不该再跳 ================"
RET2=$(PSQL $f1 -Atc "SELECT partdist.advance_wal_past_shard(${foid}::regclass)" </dev/null|tail -1)
F_INS2=$(PSQL $f1 -Atc "SELECT pg_current_wal_insert_lsn()" </dev/null)
check "★ 已越过则不再跳段（位点仍在同一段内）" \
      "$(PSQL $f1 -Atc "SELECT ('${F_INS2}'::pg_lsn - '${F_INS1}'::pg_lsn) < 16777216" </dev/null)" "t"

echo "================ [2c] 无回放游标的分片：返回 NULL，不是报错 ================"
nullret=$(PSQL $f2 -Atc "SELECT coalesce(partdist.advance_wal_past_shard('${shard_tbl}'::regclass)::text,'NULL')" </dev/null 2>&1|tail -1)
check "  陪跑节点有游标 ⇒ 返回位点而非报错" \
      "$([[ "$nullret" != *"ERROR"* ]] && echo ok)" "ok"

echo "================ [2d] ★★ 跳跃之后：崩溃恢复必须读得回新段里的已提交数据 ================"
# ★ 本段**故意** kill -9，会把 health_check_no_crash 的窗口污染成必红。
#   做法不是把哨兵关掉（那等于此后整轮都没人看崩溃），而是**在这里先结一次账**，
#   再重开窗口 —— 前半程的意外崩溃照样跑不掉，后半程也仍有哨兵。
health_check_no_crash && { echo "  （前半程窗口结账通过）"; PASS=$((PASS+1)); } \
                     || { FAIL=$((FAIL+1)); }
# 这是 T6.3b 唯一真正要保证的事。前面的 LSN 比较都只是必要条件。
PSQL $f1 -q -c "DROP TABLE IF EXISTS t63b_dur; CREATE TABLE t63b_dur(id int primary key);" </dev/null >/dev/null
PSQL $f1 -q -c "INSERT INTO t63b_dur SELECT generate_series(1,500);" </dev/null >/dev/null
n_before=$(PSQL $f1 -Atc "SELECT count(*) FROM t63b_dur" </dev/null)
f1data=$(PSQL $f1 -Atc "SHOW data_directory" </dev/null)
LOGMARK=$(DEX bash -c "wc -l < '$f1data/pg.log'" </dev/null 2>/dev/null)
PMPID=$(DEX bash -c "head -1 '$f1data/postmaster.pid'" </dev/null)
DEX bash -c "kill -9 $PMPID" </dev/null >/dev/null 2>&1
sleep 3
# 僵尸 postmaster 的 kill(pid,0) 会成功，PG 因此拒绝启动；两把锁都要清
DEX bash -c "rm -f '$f1data/postmaster.pid' /tmp/.s.PGSQL.${f1}.lock" </dev/null >/dev/null 2>&1
DEX /work/pg-install/bin/pg_ctl -D "$f1data" -l "$f1data/pg.log" start -w -t 90 </dev/null >/dev/null 2>&1
for t in $(seq 1 40); do [[ "$(PSQL $f1 -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 1; done
n_after=$(PSQL $f1 -Atc "SELECT count(*) FROM t63b_dur" </dev/null 2>/dev/null)
check "★★ 崩溃恢复后数据一行不少（跳跃没吃掉新段里的已提交记录）" "$n_after" "$n_before"
crashed=$(DEX bash -c "tail -n +${LOGMARK} '$f1data/pg.log' | grep -c 'was not properly shut down'" </dev/null 2>/dev/null)
check "  确实走的是崩溃恢复（不是干净启动的假通过）" "$([[ "${crashed:-0}" -ge 1 ]] && echo ok)" "ok"
redo_in_new=$(DEX bash -c "tail -n +${LOGMARK} '$f1data/pg.log' | grep -c 'redo starts at'" </dev/null 2>/dev/null)
check "  恢复起点有记录（redo starts at）" "$([[ "${redo_in_new:-0}" -ge 1 ]] && echo ok)" "ok"
noerr=$(DEX bash -c "tail -n +${LOGMARK} '$f1data/pg.log' | grep -cE 'incorrect prev-link|could not open file .*pg_wal'" </dev/null 2>/dev/null)
check "★ 没有 prev-link 断链、没有缺段（跳跃的落点选对了）" "${noerr:-x}" "0"
health_mark_start          # 重开窗口：本段的 kill -9 是设计的一部分，不计入

echo "================ [3] T6.4：切主认领 ================"
PSQL $f1 -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null 2>&1
p3=$(PLSN); a=$(CATCH $f1 "$p3")
PSQL $f1 -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION sclog_read(oid, bigint) RETURNS int
  AS '$libdir/pg_partdist','partdist_shard_clog_read' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_write(oid, bigint, int) RETURNS void
  AS '$libdir/pg_partdist','partdist_shard_clog_write' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_claim_t24(oid) RETURNS int
  AS '$libdir/pg_partdist','partdist_shard_claim' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_set_prepared(oid, bigint) RETURNS void
  AS '$libdir/pg_partdist','partdist_shard_clog_set_prepared' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sxid_raise_wm(oid, bigint) RETURNS void
  AS '$libdir/pg_partdist','partdist_shard_xid_raise_watermark' LANGUAGE C STRICT;
SQL
# 开缺口帮手：把发号水位往上抬一批，claim_wm 留在原地 ⇒ [claim_wm, watermark)
# 就是"回放收过号、但还没人认领"的那段，正是 T6.4 要扫的区间。
# 这一步等价于回放消费了一条更高水位的 MARKER（真路径由 T6.5 证过，见探针注释）。
GAP() {
  local cur nw
  cur=$(PSQL $f1 -Atc "SELECT partdist.shard_xid_next(${foid}::oid)" </dev/null|tail -1)
  nw=$((cur + 4096))
  PSQL $f1 -q -c "SELECT sxid_raise_wm(${foid}::oid, ${nw}::bigint)" </dev/null >/dev/null
  PSQL $f1 -Atc "SELECT partdist.shard_xid_next(${foid}::oid)" </dev/null|tail -1
}

# ★★ 先把槽位挂上并排空历史。
#
#   首版没有这一步，于是判别断言整个失效：上一段的 kill -9 把 shmem 清了，
#   T2.4 的 ShardXidEnsureClaimed 一调就重新挂槽 + 认领了 **4093** 条，
#   连我们刚造的那个无主 RUNNING 一起改判了 —— 看上去像"老路径也能干这活"，
#   实际是"槽位不存在"这个前提被自己的崩溃测试破坏了。
#   真实的升主流程里槽位一定已存在（回放推水位时就挂了），所以这里要把那个
#   前提**显式建立**出来再比。
drain=$(PSQL $f1 -Atc "SELECT sclog_claim_t24(${foid}::oid)" </dev/null|tail -1)
echo "  挂槽并排空历史：本次改判 ${drain} 条（这是夹具，不是判据）"
WM=$(GAP)
echo "  抬水位开缺口后：分片发号水位 = $WM（claim_wm 留在挂槽处）"
check "水位非零（认领的输入在，否则区间为空是假通过）" \
      "$([[ -n "$WM" && "$WM" -gt 3 ]] && echo ok)" "ok"

# 造三种状态各一个，都落在**刚长出来的**那段区间里（> claim_wm 且 < watermark）
X_RUN=$((WM-3)); X_PREP=$((WM-2)); X_COMMIT=$((WM-1))
PSQL $f1 -q -c "SELECT sclog_write(${foid}::oid, ${X_RUN}::bigint, 0)" </dev/null >/dev/null
PSQL $f1 -q -c "SELECT sclog_write(${foid}::oid, ${X_COMMIT}::bigint, 2)" </dev/null >/dev/null
PSQL $f1 -q -c "SELECT sclog_set_prepared(${foid}::oid, ${X_PREP}::bigint)" </dev/null >/dev/null
s_run=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_RUN}::bigint)" </dev/null|tail -1)
s_prep=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_PREP}::bigint)" </dev/null|tail -1)
s_com=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_COMMIT}::bigint)" </dev/null|tail -1)
echo "  造好的三态：RUNNING=$s_run PREPARED=$s_prep COMMITTED=$s_com（xid ${X_RUN}/${X_PREP}/${X_COMMIT}）"
check "夹具：无主 RUNNING 就位" "$s_run" "0"
check "夹具：COMMITTED 就位" "$s_com" "2"
check "夹具：PREPARED 就位（阴性对照的前提）" "$s_prep" "1"

echo "--- ★★ 先证明 T2.4 那条老路径管不了这件事（T6.4 存在的理由） ---"
old_n=$(PSQL $f1 -Atc "SELECT sclog_claim_t24(${foid}::oid)" </dev/null|tail -1)
still_run=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_RUN}::bigint)" </dev/null|tail -1)
check "★★ T2.4 挂槽认领在槽位已存在时一条都不改（返回 0）" "$old_n" "0"
check "★★ 无主 RUNNING 仍是 RUNNING（老路径确实够不着）" "$still_run" "0"

echo "--- ★★ T6.4 切主认领 ---"
n_claim=$(PSQL $f1 -Atc "SELECT partdist.shard_claim_on_promote(${foid}::oid)" </dev/null|tail -1)
a_run=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_RUN}::bigint)" </dev/null|tail -1)
a_prep=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_PREP}::bigint)" </dev/null|tail -1)
a_com=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_COMMIT}::bigint)" </dev/null|tail -1)
echo "  认领 $n_claim 条；三态变为 RUNNING→$a_run PREPARED→$a_prep COMMITTED→$a_com"
check "★★ 无主 RUNNING 已改判 ABORTED" "$a_run" "3"
check "★★ 认领条数 >= 1（不是空转）" "$([[ -n "$n_claim" && "$n_claim" -ge 1 ]] && echo ok)" "ok"
check "★★ 阴性对照：PREPARED 一根汗毛没动（§6.6 第二支）" "$a_prep" "$s_prep"
check "★★ 阴性对照：COMMITTED 仍是 COMMITTED" "$a_com" "2"

echo "--- 水位推进后再认领应为空转（认领水位真的落了盘） ---"
n2=$(PSQL $f1 -Atc "SELECT partdist.shard_claim_on_promote(${foid}::oid)" </dev/null|tail -1)
check "★ 第二次认领 0 条（claim_wm 已推到 watermark）" "$n2" "0"

echo "================ [4] 联测：走 pg_raft_promote_prepare 这条真路 ================"
# 单验两个函数不算完 —— 它们是不是真的被升主路径调到了，只有走一遍才知道。
#
# ★ 无主 RUNNING 必须造在**新长出来的**区间里。上一段结束时 claim_wm 已经
#   推到了当时的 watermark，若还往老区间里塞，那个号在 claim_wm 之下、
#   本来就不该再被扫到（首版栽在这，看着像"升主没调认领"）。
WM3=$(GAP)
X_INT=$((WM3-1))
PSQL $f1 -q -c "SELECT sclog_write(${foid}::oid, ${X_INT}::bigint, 0)" </dev/null >/dev/null
pre_run=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_INT}::bigint)" </dev/null|tail -1)
check "联测夹具：在新区间里又造一个无主 RUNNING（xid ${X_INT}）" "$pre_run" "0"
ins_pre=$(PSQL $f1 -Atc "SELECT pg_current_wal_insert_lsn()" </dev/null)
WMARK=$(DEX bash -c "wc -l < '$f1data/pg.log'" </dev/null 2>/dev/null)
rc=$(PSQL $f1 -Atc "SELECT partdist.pg_raft_promote_prepare(${gid}, 5000)" </dev/null 2>&1|tail -1)
post_run=$(PSQL $f1 -Atc "SELECT sclog_read(${foid}::oid, ${X_INT}::bigint)" </dev/null|tail -1)
echo "  promote_prepare 返回 $rc"
check "★★ 升主前置返回 1（可上报）" "$rc" "1"
check "★★ 升主路径确实调到了认领：无主 RUNNING 变 ABORTED" "$post_run" "3"
# ★ 只看本次调用之后新增的行。首版 grep 全文，把历史行算了进去（实测 2 条）——
#   日志类断言必须带行号基线，这在本项目已经栽过三次。
werr=$(DEX bash -c "tail -n +${WMARK} '$f1data/pg.log' | grep -cE '升主推进 WAL 插入位点.*失败|升主认领无主 RUNNING.*失败'" </dev/null 2>/dev/null)
check "★ 升主路径里两步都没抛 WARNING" "${werr:-x}" "0"

echo "================ [5] 清理 ================"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}')" </dev/null >/dev/null 2>&1
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl}" </dev/null >/dev/null 2>&1
done
PSQL $f1 -q -c "DROP TABLE IF EXISTS t63b_dur" </dev/null >/dev/null 2>&1
PSQL $pport -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids" </dev/null >/dev/null 2>&1
PSQL $pport -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null 2>&1
PSQL $COORD -q -c "DROP TABLE IF EXISTS t63b" </dev/null >/dev/null 2>&1
# ★ partition_map 与 pg_shard_xid 也要清。首版漏了这两样（既有套件都做，
#   见 test_shard_pagecmp_p1.sh 的收尾），实测把出口门禁的 shard_identity_p0
#   打成 8/2 —— 它比对"登记了几个分片 vs 实际存在几个"，孤儿登记行直接判红。
#   pg_shard_xid 槽位更狠：只增不减且有 64 硬上限，跨过之后该节点**所有**打标
#   验收都在登记那步死，报错与被测内容毫无关系（R-P6-4）。
for p in $COORD $pport $f1 $f2; do
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" </dev/null >/dev/null 2>&1
done
PDATA=$(PSQL $pport -Atc "SHOW data_directory" </dev/null 2>/dev/null)
[[ -n "${PDATA:-}" && -n "${SOID:-}" ]] && DEX rm -f "${PDATA}/pg_shard_xid/${SOID}" </dev/null 2>/dev/null
check "清理完成" "ok" "ok"
health_check_no_crash && { echo "  PASS  本轮无节点崩溃（signal 11/6 或 PANIC）"; PASS=$((PASS+1)); } \
                     || { echo "  FAIL  本轮有节点崩溃"; FAIL=$((FAIL+1)); }
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
