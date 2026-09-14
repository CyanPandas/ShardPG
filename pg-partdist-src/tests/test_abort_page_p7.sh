#!/usr/bin/env bash
# [宿主机] P7-W4 验收：leader 顶层 ROLLBACK 之后，副本物理页面必须与 leader 一致。
#
# 缺陷（2026-09-13 取证 probe_abort_gap_p7w4.sh）：原生 WAL 对 INSERT 不论提交/回滚都照写
# （回滚只追加 ABORT 记录，元组留在页上、之后才被标 dead），原生备库页面恒与主一致。而
# PartWALAbort 在事务中止时把本后端**还没排空**的捕获槽位直接丢弃 —— 被回滚的元组在 leader
# 页面上是物理事实，副本却收不到：页内行号断档；CHECKPOINT 之后页上第一条插入还带着"建新页"
# 标记，它恰好属于被回滚的事务 ⇒ 之后已提交的插入在副本上找不到这一页，redo 当作
# "页面不存在"静默跳过 ⇒ **已提交的行在副本上丢失，回放却报成功**（取证：副本主堆 0 块）。
# 同时，没落盘就不补 ABORT 标记 ⇒ 副本 gclog 里这笔事务永远"无状态"。
#
# 修法：中止时不再丢弃，而是像原生 WAL 一样把已产生的物理记录排空进分区流（先 XLogFlush，
# 连同同分区里 LSN 不超过本事务上界的他人槽位一并排，保持 plsn 与 orig_lsn 同序），再补 ABORT
# 标记；中止路径不做复制（同分区下一次写入顺带送出）。排空失败则给分区打分叉标记，交心跳
# 自动重做物理基线 —— 不许静默丢。
#
#   [A] CHECKPOINT → BEGIN; 插 5 行; ROLLBACK → 同页再提交 5 行 ⇒ 副本逐字节一致
#   [B] 回滚的事务跨 2 页、带 TOAST 大字段 → 再提交 ⇒ 副本逐字节一致
#   [C] 被回滚事务在副本 gclog 里判 aborted（修复前：无状态）
#   [D] 并发：会话 1 未结束时会话 2 在同分区回滚   [E] ROLLBACK TO SAVEPOINT 后提交
#   [F] 退路：中止排空失败 ⇒ WARNING + 分叉标记 + 重做基线后一致
# 修复前 [A][B] 页面比对与 [C] 必红。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
TS=$(date +%H%M%S)
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/p7_abort_page.lock
if ! flock -n 9; then echo "FATAL: 另一个 W4 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

TB="p7w4_$TS"
echo "========== [0] 夹具：1 分片（PK + TOAST），3 成员分区组 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE $TB(id int primary key, v text, big text);
SELECT create_distributed_table('$TB', 'id');
ALTER TABLE $TB SET (autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='$TB'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="${TB}_${gid}"
echo "  shard=${gid} leader=:${pport} followers=:${f1} :${f2}"
nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册（主堆+PK+TOAST堆+TOAST索引=4）" "$nrels" "4"

# 导出 leader fileset → follower 配对。DDL 之后要重跑，做成函数。
export_fileset() {
  PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ','
}
set_locmap() {  # set_locmap <fport>；回显配对数
  local fp=$1 rows roles ords spcs dbs rels
  rows=$(export_fileset)
  roles=$(echo "$rows" | cut -d, -f1 | paste -sd,); ords=$(echo "$rows" | cut -d, -f2 | paste -sd,)
  spcs=$(echo "$rows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$rows" | cut -d, -f4 | paste -sd,)
  rels=$(echo "$rows" | cut -d, -f5 | paste -sd,)
  PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])"
}

# ---- 页面比对工具（判据同 R1：内核 heap_mask() 掩码之外逐字节一致）----
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
DEX chmod +x /tmp/pagecmp.py 2>/dev/null || true

# 按 fileset 的 role 与 fork 推出 pagecmp 的掩码口径。
#   role 0/2 = 主堆 / TOAST 堆 → heap
#   role 1/3 = 索引 / TOAST 索引 → btree（**不能**用堆规则：IndexTuple 只有
#              8 字节头，堆元组那套偏移掩到的是索引键本身）
#   _vm fork → vm（VM 页没有"空闲空洞"，套堆规则会把整张位图掩掉）
pagecmp_kind() {   # $1=key(role.ord)  $2=fork("" 或 "_vm")
  [[ "$2" == "_vm" ]] && { echo vm; return; }
  case "${1%%.*}" in
    1|3) echo btree ;;
    *)   echo heap ;;
  esac
}

pdata=$(PSQL $pport -Atc "SHOW data_directory")

# fileset 里的 relnum 是 **relfilenode**，不是关系 OID —— 两者只在关系刚建好时
# 碰巧相等。VACUUM FULL / REINDEX / TRUNCATE 之后 relfilenode 就换了号，
# 再 `relnum::regclass` 拿到的是别的关系或干脆 NULL，路径查询整片返回空，
# 页面比对**一条都不跑却看不出来**。必须经 pg_filenode_relation 反查。
# （fileset 存的是实际表空间 OID；pg_filenode_relation 要的是 pg_class.reltablespace
#  的口径，默认表空间在那里是 0。）
FILESET_PATHS_SQL="SELECT role||'.'||ord||','||
       pg_relation_filepath(pg_filenode_relation(
           CASE WHEN spc = 1663 THEN 0 ELSE spc END, relnum))
  FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord"

diff_follower() {  # diff_follower <fport> <标签>
  local fp=$1 tag=$2 fdata lead_paths frows nmem
  fdata=$(PSQL $fp -Atc "SHOW data_directory")
  PSQL $pport -q -c "CHECKPOINT;" >/dev/null
  PSQL $fp   -q -c "CHECKPOINT;" >/dev/null
  sleep 3
  lead_paths=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; ${FILESET_PATHS_SQL}" | grep ',')
  frows=$(PSQL $fp -Atc "SET citus.enable_ddl_propagation=off; ${FILESET_PATHS_SQL}" | grep ',')

  # 守卫：路径解析一垮，下面的循环就一条都不跑，而"零个检查"是会静默通过的
  nmem=$(echo "$lead_paths" | grep -c '^[0-9]')
  check "${tag} fileset 路径解析出 ${nmem} 个成员" \
        "$([[ "$nmem" -ge 4 ]] && echo ok || echo no)" "ok"

  # 成员清单必须先读进数组，**不能**用 `while read ... <<< "$rows"`：
  # 循环体里的 `docker exec -i` 会把循环自己的标准输入一并吞掉，于是第一行
  # 之后的成员被静默跳过 —— 表现是"全 PASS 但只比了主堆"。
  local -a rows
  local ncmp=0 nempty=0
  mapfile -t rows <<< "$lead_paths"

  local lrow
  for lrow in "${rows[@]}"; do
    local key lrel frel lpath fpath fork lex fex lsz fsz same
    [[ -n "$lrow" ]] || continue
    key=${lrow%%,*}; lrel=${lrow#*,}
    frel=$(echo "$frows" | grep "^${key}," | cut -d, -f2)
    [[ -n "$frel" ]] || { check "${tag} ${key} 本地有对应关系" "" "非空"; continue; }
    for fork in "" "_vm"; do
      lpath="${pdata}/${lrel}${fork}"; fpath="${fdata}/${frel}${fork}"
      lex=$(DEX bash -c "test -f '$lpath' && echo y || echo n" </dev/null)
      fex=$(DEX bash -c "test -f '$fpath' && echo y || echo n" </dev/null)
      [[ "$lex" == "n" && "$fex" == "n" ]] && continue

      # VACUUM FULL 重写出来的新关系没有 VM fork，而 follower 侧对应文件是被
      # ReplayTruncateLocalRel **截成 0 块**（截断不删文件）。这不是缺陷：
      # 内核 smgr_redo 的 XLOG_SMGR_TRUNCATE 在原生备库上同样留下 0 长度 fork。
      # 后续 leader 再做普通 VACUUM 会发 XLOG_HEAP2_VISIBLE，把它重新撑起来。
      # 明确断言这个形态，而不是用"两边都没有"把它悄悄跳过。
      if [[ "$lex" == "n" && "$fex" == "y" ]]; then
        fsz=$(DEX stat -c %s "$fpath" </dev/null)
        check "${tag} ${key}${fork:-.main} leader 无此 fork 时 follower 为 0 长度" "$fsz" "0"
        ncmp=$((ncmp + 1))
        continue
      fi

      check "${tag} ${key}${fork:-.main} 两侧都存在" "$lex/$fex" "y/y"
      [[ "$lex" == "y" && "$fex" == "y" ]] || continue
      lsz=$(DEX stat -c %s "$lpath" </dev/null); fsz=$(DEX stat -c %s "$fpath" </dev/null)
      check "${tag} ${key}${fork:-.main} 大小一致(${lsz})" "$fsz" "$lsz"
      local kind; kind=$(pagecmp_kind "$key" "$fork")
      same=$(DEX python3 /tmp/pagecmp.py --kind="$kind" "$lpath" "$fpath" </dev/null 2>/dev/null)
      # ★ IDENTICAL_EMPTY = 两侧都是 0 字节，**比较了零个页面**，不构成一致的证据。
      # 主堆（role 0）必须有内容 —— 空了说明整条重填/回放路径失效；
      # TOAST 堆与索引可以合法为空（没有超长值就不会有 TOAST 页）。
      # 旧版本把这种情况一律算作"逐字节一致"，等于给零覆盖发通行证。
      local want="IDENTICAL_OUTSIDE_HOLE" note=""
      if [[ "${key%.*}" != "0" && "$same" == "IDENTICAL_EMPTY" ]]; then
        want="IDENTICAL_EMPTY"; nempty=$((nempty + 1))
      fi
      # IDENTICAL_EXCEPT_PDLSN（内容全同、只有 pd_lsn 不同）只对非主堆收，判据同 test_dtx_replay_tx1.sh：
      #   回放从没写过的页（典型是没人碰过的 TOAST 索引元页）两侧各由本地 WAL 建成，内容确定所以
      #   全同、LSN 各是各的 —— 根因是夹具用 CREATE TABLE LIKE 建壳表（FRD §13 约束 2 要求物理拷贝）。
      #   修复前后这一格形态相同，与本缺陷无关；**主堆出现它就是真缺陷**，不放宽。
      if [[ "${key%%.*}" != "0" && "$same" == "IDENTICAL_EXCEPT_PDLSN" ]]; then
        want="IDENTICAL_EXCEPT_PDLSN"; note="［判据放宽为 EXCEPT_PDLSN：非主堆、回放未写过的页］"
      fi
      check "${tag} ${key}${fork:-.main} 掩码外逐字节一致${note}" "$same" "$want"
      ncmp=$((ncmp + 1))
    done
  done

  # 第二道守卫：真正比过的文件数必须覆盖到全部成员（主堆另有 _vm fork）。
  # 只有这条能挡住"循环提前退出"这类失败 —— 上面那条只验路径查得出来。
  check "${tag} 实际比对了 ${ncmp} 个文件（>= 成员数 ${nmem}）" \
        "$([[ "$ncmp" -ge "$nmem" ]] && echo ok || echo no)" "ok"
}

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE $TB INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
  np=$(set_locmap $fp)
  check "follower :$fp locmap 配对 4 对" "$np" "4"
done
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
st=""
for t in $(seq 1 25); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 ${gid} leader 就位（:${pport}）" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null
done
leader_oid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
lead_plsn() { PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})" </dev/null | tail -1; }
catch_all() {  # 两个副本都追平到 leader 流尾
  local lp fp a; lp=$(lead_plsn)
  for fp in $f1 $f2; do
    a=$(PSQL $fp -Atc "SELECT partdist.replay_catchup('${shard_tbl}', ${lp}, 120000)" </dev/null 2>&1 | tail -1)
    check "  :$fp 追平到 leader 流尾（${a}/${lp}）" "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && echo ok || echo no)" "ok"
  done
}
leader_lp() { PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*)||'lp/'||count(*) FILTER (WHERE lp_flags=1)||'normal' FROM heap_page_items(get_raw_page('${shard_tbl}','main',$1))" </dev/null 2>/dev/null | tail -1; }

echo "========== [A] CHECKPOINT → 插 5 行后 ROLLBACK → 同页再提交 5 行 =========="
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
XA=$(PSQL $pport -q -At <<SQL | grep -E '^[0-9]+$' | tail -1
SET citus.override_table_visibility = false;
BEGIN;
INSERT INTO ${shard_tbl} SELECT g, 'rolled-back', NULL FROM generate_series(1, 5) g;
SELECT txid_current();
ROLLBACK;
SQL
)
PSQL $pport -q -c "SET citus.override_table_visibility=false; INSERT INTO ${shard_tbl} SELECT g, 'kept', NULL FROM generate_series(6, 10) g;" </dev/null >/dev/null
echo "  被回滚事务 xid=${XA}；leader 页 0：$(leader_lp 0)"
check "  前提：取到被回滚事务的 xid" "$([[ "$XA" =~ ^[0-9]+$ ]] && echo ok || echo no)" "ok"
catch_all
diff_follower $f1 "[A]"

echo "========== [B] 回滚的事务跨 2 页、带 TOAST 大字段 → 再提交 =========="
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
PSQL $pport -q <<SQL >/dev/null
SET citus.override_table_visibility = false;
BEGIN;
INSERT INTO ${shard_tbl} SELECT g, repeat('r', 400), CASE WHEN g % 5 = 0 THEN repeat(md5(g::text), 400) END
  FROM generate_series(100, 140) g;
ROLLBACK;
INSERT INTO ${shard_tbl} SELECT g, repeat('k', 400), CASE WHEN g % 5 = 0 THEN repeat(md5(g::text), 400) END
  FROM generate_series(200, 240) g;
SQL
nblk=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_size('${shard_tbl}')/8192" </dev/null | tail -1)
check "  前提：leader 主堆已跨过多页（${nblk} 块 ≥ 2）" "$([[ "$nblk" =~ ^[0-9]+$ && "$nblk" -ge 2 ]] && echo ok || echo no)" "ok"
catch_all
diff_follower $f1 "[B]"

echo "========== [C] 被回滚事务在副本 gclog 里判 aborted =========="
gn=$(PSQL $pport -Atc "SHOW pg_partdist.node_id" </dev/null | tail -1)
[[ "$gn" =~ ^[0-9]+$ ]] || gn=$(PSQL $pport -Atc "SELECT groupid FROM pg_dist_local_group" </dev/null | tail -1)
for fp in $f1 $f2; do
  gs=$(PSQL $fp -Atc "SELECT status FROM partdist.gclog_status(${gn}, ${XA:-0})" </dev/null 2>&1 | tail -1)
  check "★ 副本 :$fp 上 [A] 的回滚事务 gclog=aborted（修复前无 ABORT 标记 ⇒ running）" "$gs" "aborted"
done

echo "========== [D] 并发：会话 1 未结束、会话 2 在同分区回滚 =========="
# 会话 2 回滚时，会话 1 的第一批记录还在捕获环里、orig_lsn 更小。中止排空必须把它们一并排掉
# （按 start_lsn 回读 pg_wal），否则之后会话 1 提交时它们拿到更大的 plsn，段号与 plsn 倒挂。
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
( PSQL $pport -q -c "SET citus.override_table_visibility=false; BEGIN; INSERT INTO ${shard_tbl} SELECT g,'s1a',NULL FROM generate_series(300,320) g; SELECT pg_sleep(6); INSERT INTO ${shard_tbl} SELECT g,'s1b',NULL FROM generate_series(321,340) g; COMMIT;" </dev/null >/dev/null 2>&1 ) &
S1=$!
sleep 2
PSQL $pport -q -c "SET citus.override_table_visibility=false; BEGIN; INSERT INTO ${shard_tbl} SELECT g,'s2',NULL FROM generate_series(400,420) g; ROLLBACK;" </dev/null >/dev/null 2>&1
wait $S1
nd=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl} WHERE id BETWEEN 300 AND 420" </dev/null | tail -1)
check "  会话 1 的 41 行已提交、会话 2 的 21 行已回滚（leader 读到 ${nd}）" "$nd" "41"
catch_all
diff_follower $f1 "[D]"

echo "========== [E] 子事务：ROLLBACK TO SAVEPOINT 之后提交 =========="
PSQL $pport -q -c "SET citus.override_table_visibility=false; BEGIN; INSERT INTO ${shard_tbl} SELECT g,'e1',NULL FROM generate_series(500,510) g; SAVEPOINT s; INSERT INTO ${shard_tbl} SELECT g,'e2',NULL FROM generate_series(511,520) g; ROLLBACK TO s; INSERT INTO ${shard_tbl} SELECT g,'e3',NULL FROM generate_series(521,530) g; COMMIT;" </dev/null >/dev/null 2>&1
ne=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl} WHERE id BETWEEN 500 AND 530" </dev/null | tail -1)
check "  leader 读到 21 行（子事务那 10 行已回滚）" "$ne" "21"
catch_all
diff_follower $f1 "[E]"

echo "========== [F] 退路：中止排空失败 ⇒ 告警 + 分叉标记 + 重做基线后一致 =========="
# 让排空必然失败：新会话还没解析过节点号，中止回调里又禁读 catalog，只能读侧影文件 ——
# 把侧影文件临时挪走。★ 危险操作必须天然局部：挪走与复原在容器内同一个 bash 里，trap 兜底。
LD=$(PSQL $pport -Atc "SHOW data_directory" </dev/null | tail -1)
LLOG=$(health_node_log "worker$((pport - 5432))")
nw0=$(DEX grep -c "事务中止时排空分区记录失败" "$LLOG" </dev/null 2>/dev/null || true)
fout=$(DEX bash -c "trap 'mv -f $LD/pg_partdist_groupid.t7w4 $LD/pg_partdist_groupid' EXIT
  mv $LD/pg_partdist_groupid $LD/pg_partdist_groupid.t7w4 || exit 3
  /work/pg-install/bin/psql -h /tmp -p $pport -U postgres -d postgres -X -c \"SET citus.override_table_visibility=false; BEGIN; INSERT INTO ${shard_tbl} VALUES (900,'f',NULL); ROLLBACK;\" 2>&1" </dev/null 2>&1)
check "  侧影文件已复原" "$(DEX test -f $LD/pg_partdist_groupid </dev/null && echo yes || echo no)" "yes"
check "★ 排空失败时回给客户端的是 WARNING，事务照常回滚（不崩、不 FATAL）" \
      "$([[ "$fout" == *"事务中止时排空分区记录失败"* && "$fout" == *ROLLBACK* && "$fout" != *FATAL* ]] && echo ok || echo no)" "ok"
nw1=$(DEX grep -c "事务中止时排空分区记录失败" "$LLOG" </dev/null 2>/dev/null || true)
check "  节点日志新增该告警（${nw0} → ${nw1}）" "$([[ "${nw1:-0}" -gt "${nw0:-0}" ]] && echo ok || echo no)" "ok"
dv=$(PSQL $pport -Atc "SELECT partdist.shard_divergence(${leader_oid})" </dev/null | tail -1)
rep=$(DEX grep -c "repaired:.*${leader_oid}" "$LLOG" </dev/null 2>/dev/null || true)
check "★ 该分区被打上分叉标记（或已被心跳自动修复）——不许静默丢记录" \
      "$([[ "$dv" == *P7-W4* || "${rep:-0}" -gt 0 ]] && echo ok || echo no)" "ok"
PSQL $pport -Atc "SELECT partdist.repair_diverged_shards()" </dev/null >/dev/null 2>&1
sleep 2
check "  重做物理基线后分叉标记已清" "$(PSQL $pport -Atc "SELECT coalesce(partdist.shard_divergence(${leader_oid}),'none')" </dev/null | tail -1)" "none"
catch_all
diff_follower $f1 "[F] 重做基线后"

echo "========== [9] 清理 + 节点健康 =========="
for fp in $f1 $f2; do PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" </dev/null >/dev/null 2>&1; done
left=1
for t in $(seq 1 10); do
  for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_drop(${gid});" </dev/null >/dev/null 2>&1; done
  sleep 2; left=0
  for p in $pport $f1 $f2; do
    k=$(PSQL $p -Atc "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null | tail -1); left=$((left + ${k:-1}))
  done
  [[ "$left" == 0 ]] && break
done
check "分区组 ${gid} 已拆净" "$left" "0"
for fp in $f1 $f2; do PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1; done
PSQL $COORD -q -c "DROP TABLE IF EXISTS $TB;" </dev/null >/dev/null 2>&1
for p in $COORD $pport $f1 $f2; do PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" </dev/null >/dev/null 2>&1; done
health_check_no_crash
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
