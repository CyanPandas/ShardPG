#!/usr/bin/env bash
# [宿主机] P5 vacuum 页面动作的**跨节点回放**验收（DEV PLAN §3.4 T5.4b）。
#
# ★ 这是 T5.3a/b/c 三份实施记要里连着挂了三次账的那一项。
#   §6.7 要求：vacuum 只在 leader 执行，它的页面修改本身走 pg_parwal 流被
#   follower 逐字节回放。前面三个任务只验到"leader 的 WAL 里确有内核标准的
#   FREEZE_PAGE / PRUNE / VACUUM 记录"，跨节点这一半一直没验。
#
# 夹具照抄 test_shard_pagecmp_p1.sh 阶段 2 的配方（1 分片分布表 + raft 组 +
# 两个 follower 壳表 + locmap），把工作负载换成"制造三类垃圾 → sweep → 截断"。
#
# ★ 关键的一条防假通过断言：**follower 的文件必须真的变了**。只比对
#   "leader == follower" 是不够的 —— 若 vacuum 记录压根没进流，两边都停在
#   vacuum 之前的状态，比对照样 IDENTICAL。
#
# ★ follower 壳表绝不能被 SELECT（无白名单 ⇒ on-access 剪枝会按原生 clog
#   清掉分片元组，就地损毁副本）。本脚本只用文件级比对触碰 follower。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
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

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "================ [1] 夹具：1 分片分布表 + raft 组 + 两 follower ================"
leader0=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" </dev/null)
check "group0 有 leader" "$([[ -n "$leader0" && "$leader0" != "0" ]] && echo ok)" "ok"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done

PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS p5repl;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE p5repl(id int, v text);
SELECT create_distributed_table('p5repl', 'id');
ALTER TABLE p5repl SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='p5repl'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="p5repl_${gid}"
echo "  shard=${gid} leader=:${pport}(node${pnode}) followers=:${f1} :${f2}"

nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册（主堆+TOAST堆+TOAST索引=3）" "$nrels" "3"
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE p5repl INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])" </dev/null)
  check "follower :$fp locmap 配对 3 对" "$np" "3"
done
for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null
done
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 ${gid} leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${shard_tbl}')" </dev/null)
  check "follower :$fp replay_enable" "$en" "t"
done

SOID=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT '${shard_tbl}'::regclass::oid" </dev/null | tail -1)
PSQL $pport -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${SOID}';" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL $pport -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$SOID" ]] && break; sleep 1
done
check "leader 白名单生效" "$guc" "$SOID"
PDATA=$(PSQL $pport -Atc "SHOW data_directory" </dev/null)
PSQL $pport -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION sclog_read(oid, bigint) RETURNS int
  AS '$libdir/pg_partdist','partdist_shard_clog_read' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_wts(oid, bigint, int, bigint) RETURNS void
  AS '$libdir/pg_partdist','partdist_shard_clog_write_ts' LANGUAGE C STRICT;
SQL
check "leader 测试函数就绪" "$?" "0"

echo "================ [2] 工作负载：制造三类垃圾（全 leader 直写，值都不进 TOAST）================"
# ★ 值一律小：TOAST 关系带索引，我们的删元组通路对带索引关系一律 ERROR
#   （索引两阶段是 T5.3c 的缺口），本轮不把 TOAST 卷进来。
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.override_table_visibility=false;
INSERT INTO ${shard_tbl} SELECT g, 'v'||g FROM generate_series(1, 60) g;
DELETE FROM ${shard_tbl} WHERE id % 7 = 0;
SQL
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.override_table_visibility=false;
BEGIN; INSERT INTO ${shard_tbl} VALUES (901,'ghost'); ROLLBACK;
SQL
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.override_table_visibility=false;
BEGIN; DELETE FROM ${shard_tbl} WHERE id = 1; ROLLBACK;
SQL
vis=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl}" </dev/null | tail -1)
check "leader 可见 52 行（60 插入 - 8 已提交删除）" "$vis" "52"
n_before=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM heap_page_items(get_raw_page('${shard_tbl}',0)) WHERE lp_flags=1" </dev/null | tail -1)
check "vacuum 前第 0 页有 LP_NORMAL" "$([[ -n "$n_before" && "$n_before" -gt 10 ]] && echo ok)" "ok"

LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
check "leader 认得本地分片（local_partition_for_shard）" "$([[ -n "$LEADER_OID" && "$LEADER_OID" != "0" ]] && echo ok)" "ok"
lead_plsn1=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
echo "  lead_plsn1=${lead_plsn1}"
# partition_lsn 是**分区流内的逻辑位置**（记录序），不是字节 LSN —— 本夹具
# 这点写入量下实测只有 70 上下，照抄 pagecmp 的 ">100" 会误报。
check "leader 已产生 parwal 记录" "$([[ -n "$lead_plsn1" && "$lead_plsn1" -gt 0 ]] && echo ok)" "ok"

wait_caught_up() {  # <fport> <期望plsn> <超时s>
  local fp=$1 target=$2 timeout=$3 app foid
  # 空 target 会让下面的 -ge 比较变成语法错误，进而把断言变成假通过
  [[ -n "$target" && "$target" -gt 0 ]] || { echo ""; return 1; }
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null)
  app=$(PSQL "$fp" -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${target}, $((timeout * 1000)))" </dev/null 2>/dev/null || echo 0)
  [[ -n "$app" && "$app" -ge "$target" ]] && { echo "$app"; return 0; }
  app=$(PSQL "$fp" -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}" </dev/null 2>/dev/null || echo 0)
  echo "$app"; return 1
}
FPATH_MAIN() {  # <fport> —— 该 follower 上主堆文件的绝对路径
  local fp=$1 fdata frel
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  frel=$(PSQL "$fp" -Atc "SET citus.enable_ddl_propagation=off;
    SELECT pg_relation_filepath(pg_filenode_relation(CASE WHEN spc=1663 THEN 0 ELSE spc END, relnum))
      FROM partdist.shard_fileset('${shard_tbl}') WHERE role=0 AND ord=0" </dev/null | tail -1)
  echo "${fdata}/${frel}"
}
a1=$(wait_caught_up $f1 "$lead_plsn1" 180)
check "follower1 追平工作负载" "$([[ -n "$a1" && "$a1" -ge "$lead_plsn1" ]] && echo ok)" "ok"
a2=$(wait_caught_up $f2 "$lead_plsn1" 180)
check "follower2 追平工作负载" "$([[ -n "$a2" && "$a2" -ge "$lead_plsn1" ]] && echo ok)" "ok"
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
sleep 4
F1MAIN=$(FPATH_MAIN $f1); F2MAIN=$(FPATH_MAIN $f2)
md5_f1_pre=$(DEX md5sum "$F1MAIN" </dev/null 2>/dev/null | cut -d' ' -f1)
md5_f2_pre=$(DEX md5sum "$F2MAIN" </dev/null 2>/dev/null | cut -d' ' -f1)
check "vacuum 前取到 follower1 主堆指纹" "$([[ -n "$md5_f1_pre" ]] && echo ok)" "ok"
check "vacuum 前取到 follower2 主堆指纹" "$([[ -n "$md5_f2_pre" ]] && echo ok)" "ok"

echo "================ [3] leader 侧 sweep + 截断 ================"
TB=$(PSQL $pport -Atc "SET citus.override_table_visibility=false;
  SELECT max(GREATEST(t_xmin::text::bigint, t_xmax::text::bigint))+1
    FROM heap_page_items(get_raw_page('${shard_tbl}',0)) WHERE lp_flags=1" </dev/null | tail -1)
check "算出截断点" "$([[ -n "$TB" && "$TB" -gt 3 ]] && echo ok)" "ok"
# 本环境 TSO 未配置 ⇒ 提交写下的 commit_ts 恒 0，② 的守卫会（正确地）拦下；
# 照 T5.2/T5.3c 的办法补真时间戳。
PSQL $pport -Atc "SELECT sclog_wts(${SOID}::oid, g::bigint, 2, 1000::bigint) FROM generate_series(3, $((TB-1))) g WHERE sclog_read(${SOID}::oid, g::bigint)=2" </dev/null >/dev/null
SW=$(PSQL $pport -Atc "SELECT swept||'/'||sanitized||'/'||removed_aborted||'/'||removed_dead||'/'||pages_skipped||'/'||tuples_deferred FROM partdist.shard_vacuum_sweep('${shard_tbl}'::regclass, ${TB}::bigint)" </dev/null | tail -1)
check "sweep：消毒1 删中止1 删死8 零跳页零推迟" "$SW" "true/1/1/8/0/0"
NSEG=$(PSQL $pport -Atc "SELECT partdist.shard_clog_truncate(${SOID}::oid, ${TB}::bigint)" </dev/null | tail -1)
check "截断成功（删段 0，不足一整段）" "$NSEG" "0"
check "vacuum 后 leader 仍可见 52 行" \
      "$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl}" </dev/null | tail -1)" "52"
n_after=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM heap_page_items(get_raw_page('${shard_tbl}',0)) WHERE lp_flags=1" </dev/null | tail -1)
check "leader 第 0 页 LP_NORMAL 减少了" "$([[ -n "$n_after" && "$n_after" -lt "$n_before" ]] && echo ok)" "ok"

lead_plsn2=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
echo "  lead_plsn2=${lead_plsn2}"
check "vacuum 产生了新的 parwal 记录" "$([[ -n "$lead_plsn2" && "$lead_plsn2" -gt "$lead_plsn1" ]] && echo ok)" "ok"

echo "================ [4] follower 回放 vacuum 记录 ================"
b1=$(wait_caught_up $f1 "$lead_plsn2" 180)
check "follower1 追平 vacuum" "$([[ -n "$b1" && "$b1" -ge "$lead_plsn2" ]] && echo ok)" "ok"
b2=$(wait_caught_up $f2 "$lead_plsn2" 180)
check "follower2 追平 vacuum" "$([[ -n "$b2" && "$b2" -ge "$lead_plsn2" ]] && echo ok)" "ok"
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
sleep 4
md5_f1_post=$(DEX md5sum "$F1MAIN" </dev/null 2>/dev/null | cut -d' ' -f1)
md5_f2_post=$(DEX md5sum "$F2MAIN" </dev/null 2>/dev/null | cut -d' ' -f1)
# ★ 防假通过：只比"leader==follower"是不够的——若 vacuum 记录压根没进流，
#   两边都停在 vacuum 之前，比对照样 IDENTICAL。必须先证明 follower 真的变了。
check "★ follower1 主堆文件确实变了（vacuum 记录真的到了）" \
      "$([[ -n "$md5_f1_post" && "$md5_f1_post" != "$md5_f1_pre" ]] && echo ok)" "ok"
check "★ follower2 主堆文件确实变了" \
      "$([[ -n "$md5_f2_post" && "$md5_f2_post" != "$md5_f2_pre" ]] && echo ok)" "ok"

echo "================ [5] 三方逐字节比对 ================"
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
FILESET_PATHS_SQL="SELECT role||'.'||ord||','||
       pg_relation_filepath(pg_filenode_relation(
           CASE WHEN spc = 1663 THEN 0 ELSE spc END, relnum))
  FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord"
lead_paths=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; ${FILESET_PATHS_SQL}" </dev/null | grep ',')
pagecmp_kind() { [[ "$2" == "_vm" ]] && { echo vm; return; }; case "${1%%.*}" in 1|3) echo btree ;; *) echo heap ;; esac; }
diff_one_follower() {  # <fport> <标签>
  local fp=$1 tag=$2 fdata frows ncmp=0
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  frows=$(PSQL "$fp" -Atc "SET citus.enable_ddl_propagation=off; ${FILESET_PATHS_SQL}" </dev/null | grep ',')
  local -a rows; local lrow
  mapfile -t rows <<< "$lead_paths"
  for lrow in "${rows[@]}"; do
    [[ -n "$lrow" ]] || continue
    local key lrel frel lpath fpath fork
    key=${lrow%%,*}; lrel=${lrow#*,}
    frel=$(echo "$frows" | grep "^${key}," | cut -d, -f2)
    for fork in "" "_vm"; do
      lpath="${PDATA}/${lrel}${fork}"; fpath="${fdata}/${frel}${fork}"
      local lex fex kind same
      lex=$(DEX bash -c "test -f '$lpath' && echo y || echo n" </dev/null)
      fex=$(DEX bash -c "test -f '$fpath' && echo y || echo n" </dev/null)
      if [[ "$lex" == "n" && "$fex" == "n" ]]; then continue; fi
      check "${tag} ${key}${fork:-.main} 两侧都存在" "$lex/$fex" "y/y"
      [[ "$lex" == "y" && "$fex" == "y" ]] || continue
      kind=$(pagecmp_kind "$key" "$fork")
      same=$(DEX python3 /tmp/pagecmp.py --kind="$kind" "$lpath" "$fpath" </dev/null 2>/dev/null)
      if [[ "$key" == "0.0" && -z "$fork" ]]; then
        # ★ 主堆是本套件的正题：vacuum 的三条记录全落在它上面，必须逐字节相同
        check "${tag} ${key}.main pagecmp(${kind}) 逐字节" "$same" "IDENTICAL_OUTSIDE_HOLE"
      else
        # TOAST 堆与 TOAST 索引：本套件的值一律不进 TOAST（那条关系带索引，
        # 而删元组通路对带索引关系一律 ERROR —— 索引两阶段是 T5.3c 的缺口），
        # 所以它们**从未被写过**：空堆报 IDENTICAL_EMPTY、空 btree 元页两侧
        # 各自本地创建故报 IDENTICAL_EXCEPT_PDLSN。两者都不是分歧，但也不能
        # 松成"只要含 IDENTICAL 就算过"——列举允许值。
        case "$same" in
          IDENTICAL_OUTSIDE_HOLE|IDENTICAL_EMPTY|IDENTICAL_EXCEPT_PDLSN) same=OK ;;
        esac
        check "${tag} ${key}${fork:-.main} pagecmp(${kind}) 无分歧（未参与本轮 vacuum）" "$same" "OK"
      fi
      ncmp=$((ncmp+1))
    done
  done
  check "${tag} 比对文件计数守卫（≥3）" "$([[ "$ncmp" -ge 3 ]] && echo ok)" "ok"
}
diff_one_follower $f1 "vacuum回放:f1"
diff_one_follower $f2 "vacuum回放:f2"

echo "================ [6] 清理 + 节点健康 ================"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" </dev/null >/dev/null 2>&1
done
for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1
done
for fp in $f1 $f2; do
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS p5repl;" </dev/null >/dev/null 2>&1
for p in $COORD $pport $f1 $f2; do
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" </dev/null >/dev/null 2>&1
done
PSQL $pport -q -c "DROP FUNCTION IF EXISTS sclog_read(oid,bigint); DROP FUNCTION IF EXISTS sclog_wts(oid,bigint,int,bigint);" </dev/null >/dev/null 2>&1
PSQL $pport -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
DEX rm -f "${PDATA}/pg_shard_xid/${SOID}" </dev/null
check "清场完成" "$?" "0"
health_check_no_crash

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
exit "$FAIL"
