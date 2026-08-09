#!/usr/bin/env bash
# [宿主机] TX2：2PC 事务的两段式标记（DTX_2PC_DESIGN.md §3.3 阶段 3）。
#
# 要验的正是"协调者确认全局提交后，把 COMMIT 标记传播到所有参与分片组，
# 每组 leader 追加一条 COMMIT Marker 并经 Raft 复制持久化到多数派"。
#
# 合并后的缺口（本用例的立命之本）：跨分区事务走 PRE_PREPARE，而全树唯一
# 写 MARKER 的入口挂在 PRE_COMMIT 上，于是 2PC 事务在段流里**一条 MARKER 都
# 没有**；阶段 3 只补了 DTX_COMMIT（DTX 类记录），而回放侧对 DTX 只推游标、
# 不记账 —— follower 的增强型 CLOG 里这笔事务永远是空洞槽 = 未决 = 不可见。
#
# 验收标准：
#   1. leader 段流里出现 PREPARE 标记（flags=2 info=16）与 COMMIT 标记
#      （flags=2 info=0），二者 gxid 相同、且等于该事务的本地 top-level xid。
#   2. 顺序不变式：DATA < PREPARE 标记 < COMMIT 标记（按 partition_lsn）。
#   3. 两条标记原样复制到 follower（flags/info/gxid 逐字段一致）——
#      这就是"经 Raft 持久化到多数派"的可观测形态。
#   4. follower 回放后，增强型 CLOG 对该事务判为 committed（**不是 running**）。
#   5. 子事务（SAVEPOINT 内的写入）同样判为 committed —— COMMIT 标记不带子事务
#      清单，靠 PREPARE 标记写下的 parent_xid 链解析到顶层判决。
#   6. 被 ROLLBACK TO 掉的子事务**不得**判为 committed（回滚语义不能被链带上）。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

check() {
  if [[ -n "$2" && "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置 =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

echo "========== [1] 夹具：2 分片分布表（跨分片写入才会走 2PC）=========="
PSQL $COORD -q -c "DROP TABLE IF EXISTS tx2_mk;" >/dev/null 2>&1
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;
CREATE TABLE tx2_mk(id int, v text);
SELECT create_distributed_table('tx2_mk','id');
ALTER TABLE tx2_mk SET (autovacuum_enabled = off);
SQL

GA=$(PSQL $COORD -Atc "SELECT min(shardid) FROM pg_dist_shard WHERE logicalrelid='tx2_mk'::regclass")
GB=$(PSQL $COORD -Atc "SELECT max(shardid) FROM pg_dist_shard WHERE logicalrelid='tx2_mk'::regclass")
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$GA")
PB=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$GB")
check "两分片落在不同 worker" "$([[ "$PA" != "$PB" ]] && echo ok)" "ok"

KA=""; KB=""
for k in $(seq 1 200); do
  s=$(PSQL $COORD -Atc "SELECT get_shard_id_for_distribution_column('tx2_mk', $k)")
  [[ "$s" == "$GA" && -z "$KA" ]] && KA=$k
  [[ "$s" == "$GB" && -z "$KB" ]] && KB=$k
  [[ -n "$KA" && -n "$KB" ]] && break
done
check "取到命中两分片的分布键" "$([[ -n "$KA" && -n "$KB" ]] && echo ok)" "ok"

TBL="tx2_mk_${GA}"
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$PA" || "$p" == "$PB" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; break; fi
done
echo "  shardA=${GA} leader=:${PA} followers=:${f1} :${f2}（shardB=${GB} @:${PB}）"

echo "========== [2] fileset + follower 壳表/locmap + 数据组 =========="
nrels=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${TBL}')" | tail -1)
check "leader fileset 注册成员数" "$([[ -n "$nrels" && "$nrels" -ge 1 ]] && echo ok)" "ok"

fsrows=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${TBL}') ORDER BY role, ord" | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${TBL};
CREATE TABLE ${TBL} (LIKE tx2_mk INCLUDING ALL);
ALTER TABLE ${TBL} SET (autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${TBL}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])")
  check "follower :$fp locmap 配对" "$([[ -n "$np" && "$np" -ge 1 ]] && echo ok)" "ok"
done

pnode=$((PA - 5431)); f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null; done
PSQL $PA -q -c "SELECT partdist.pg_raft_group_create(${GA}, ${members});" >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $PA -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GA}" 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 ${GA} leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${GA}, ${members});" >/dev/null
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${TBL}')")
  check "follower :$fp replay_enable" "$en" "t"
done

LOID=$(PSQL $PA -Atc "SELECT partdist.local_partition_for_shard(${GA})")
before=$(PSQL $PA -Atc "SELECT partdist.get_partition_flush_lsn(${LOID})")

echo "========== [3] 跨分区 2PC 事务（含 SAVEPOINT，走 PRE_PREPARE）=========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL
BEGIN;
  INSERT INTO tx2_mk VALUES (${KA}, 'top-a');
  SAVEPOINT s1;  INSERT INTO tx2_mk VALUES (${KA}, 'sub-keep'); RELEASE SAVEPOINT s1;
  SAVEPOINT s2;  INSERT INTO tx2_mk VALUES (${KA}, 'sub-drop'); ROLLBACK TO SAVEPOINT s2;
  INSERT INTO tx2_mk VALUES (${KB}, 'top-b');
COMMIT;
SQL
check "跨分区事务提交成功" \
      "$(PSQL $COORD -Atc "SELECT count(*) FROM tx2_mk WHERE v IN ('top-a','sub-keep','top-b')")" "3"
after=$(PSQL $PA -Atc "SELECT partdist.get_partition_flush_lsn(${LOID})")

echo "========== [4] leader 段流：两段标记都在 =========="
# PREPARE 标记：flags=2(MARKER) info=16(XLOG_XACT_PREPARE)
prep=$(PSQL $PA -Atc "
  SELECT g FROM generate_series($((before+1)), ${after}) g,
       LATERAL partdist.partwal_read_record(${LOID}::oid, g) r
   WHERE r.flags = 2 AND r.info = 16 ORDER BY g DESC LIMIT 1")
check "leader 有 PREPARE 标记（flags=2 info=16）" \
      "$([[ -n "$prep" ]] && echo ok)" "ok"
# COMMIT 标记：flags=2 info=0(XLOG_XACT_COMMIT)
comm=$(PSQL $PA -Atc "
  SELECT g FROM generate_series($((before+1)), ${after}) g,
       LATERAL partdist.partwal_read_record(${LOID}::oid, g) r
   WHERE r.flags = 2 AND r.info = 0 ORDER BY g DESC LIMIT 1")
check "leader 有 COMMIT 标记（flags=2 info=0）" \
      "$([[ -n "$comm" ]] && echo ok)" "ok"

if [[ -n "$prep" && -n "$comm" ]]; then
  gx_p=$(PSQL $PA -Atc "SELECT gxid FROM partdist.partwal_read_record(${LOID}::oid, ${prep})")
  gx_c=$(PSQL $PA -Atc "SELECT gxid FROM partdist.partwal_read_record(${LOID}::oid, ${comm})")
  check "两段标记的 gxid 相同（同一笔 prepared 事务）" "$gx_p" "$gx_c"
  check "顺序不变式：PREPARE 标记在 COMMIT 标记之前" \
        "$([[ "$prep" -lt "$comm" ]] && echo ok)" "ok"
  ndata=$(PSQL $PA -Atc "
    SELECT count(*) FROM generate_series($((before+1)), $((prep-1))) g,
         LATERAL partdist.partwal_read_record(${LOID}::oid, g) r
     WHERE r.flags = 1 AND r.gxid = ${gx_p}")
  check "PREPARE 标记之前有同 gxid 的 DATA 记录（DATA<PREPARE）" \
        "$([[ -n "$ndata" && "$ndata" -ge 1 ]] && echo ok)" "ok"

  citus_gid=$(PSQL $PA -Atc "SELECT groupid FROM pg_dist_local_group")
  node_of=$(PSQL $PA -Atc "SELECT ${gx_p} >> 48")
  check "标记 gxid 的来源节点号 = 本节点 Citus group id" "$node_of" "$citus_gid"
  topxid=$(PSQL $PA -Atc "SELECT ${gx_p} & ((1::bigint<<48)-1)")
  # PREPARE 标记的载荷里带子事务清单（24 + 4*nsub）
  nsub=$(PSQL $PA -Atc "
    SELECT ((get_byte(data,19)::bigint<<24)|(get_byte(data,18)::bigint<<16)
           |(get_byte(data,17)::bigint<<8)|get_byte(data,16)::bigint)
      FROM partdist.partwal_read_record(${LOID}::oid, ${prep})")
  check "PREPARE 标记带子事务清单（nsub=${nsub} ≥1）" \
        "$([[ -n "$nsub" && "$nsub" -ge 1 ]] && echo ok)" "ok"
  check "COMMIT 标记不带子事务清单（靠 parent_xid 链解析）" \
        "$(PSQL $PA -Atc "SELECT length(data) FROM partdist.partwal_read_record(${LOID}::oid, ${comm})")" "24"

  echo "========== [5] 标记经 Raft 复制到 follower（多数派持久化）=========="
  for fp in $f1 $f2; do
    foid=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${GA})")
    fl=""
    for t in $(seq 1 90); do
      fl=$(PSQL $fp -Atc "SELECT partdist.get_partition_flush_lsn(${foid})")
      [[ -n "$fl" && "$fl" -ge "$after" ]] && break; sleep 2
    done
    check "follower :$fp 收齐到 ${after}（本地 ${fl}）" \
          "$([[ -n "$fl" && "$fl" -ge "$after" ]] && echo ok)" "ok"
    check "follower :$fp 的 PREPARE 标记逐字段一致" \
          "$(PSQL $fp -Atc "SELECT flags||'|'||info||'|'||gxid FROM partdist.partwal_read_record(${foid}::oid, ${prep})")" \
          "2|16|${gx_p}"
    check "follower :$fp 的 COMMIT 标记逐字段一致" \
          "$(PSQL $fp -Atc "SELECT flags||'|'||info||'|'||gxid FROM partdist.partwal_read_record(${foid}::oid, ${comm})")" \
          "2|0|${gx_c}"

    app=$(PSQL $fp -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${after}, 180000)" 2>&1 | tail -1)
    check "follower :$fp 回放追平到 ${after}" "$([[ "$app" == "$after" ]] && echo ok)" "ok"

    echo "  ---- [6] follower :$fp 的增强型 CLOG 判决 ----"
    st=$(PSQL $fp -Atc "SELECT status FROM partdist.gclog_status(${citus_gid}, ${topxid})")
    check "follower :$fp 顶层事务判为 committed（★核心：修复前恒为 running）" "$st" "committed"
    cts=$(PSQL $fp -Atc "SELECT commit_ts FROM partdist.gclog_status(${citus_gid}, ${topxid})")
    check "follower :$fp 顶层事务 commit_ts 非 0" \
          "$([[ -n "$cts" && "$cts" -gt 0 ]] && echo ok)" "ok"

    # 子事务：PREPARE 标记里列出的那些，应当经 parent_xid 链解析为 committed
    subs=$(PSQL $fp -Atc "
      SELECT string_agg(((get_byte(data, 24+4*(i-1)+3)::bigint<<24)
                        |(get_byte(data, 24+4*(i-1)+2)::bigint<<16)
                        |(get_byte(data, 24+4*(i-1)+1)::bigint<<8)
                        | get_byte(data, 24+4*(i-1))::bigint)::text, ',' ORDER BY i)
        FROM partdist.partwal_read_record(${foid}::oid, ${prep}), generate_series(1, ${nsub}) i")
    nok=0; IFS=',' read -ra SS <<< "$subs"
    for s in "${SS[@]}"; do
      [[ -z "$s" ]] && continue
      sst=$(PSQL $fp -Atc "SELECT status FROM partdist.gclog_status(${citus_gid}, ${s})")
      [[ "$sst" == "committed" ]] && nok=$((nok+1))
    done
    check "follower :$fp 已提交子事务经 parent 链判为 committed（${nok}/${nsub}）" \
          "$nok" "$nsub"

    # 被 ROLLBACK TO 掉的子事务判决。
    #
    # ★ 判据必须**独立于被测载荷**推导。上一版是拿 PREPARE 标记里的子事务清单
    # 去反推"谁不在清单里"，这是自证：若实现错误地把被 ROLLBACK TO 的子事务
    # 也写进了已提交清单，它就会被 `continue` 排除出审查，同时 nok==nsub 也照样
    # 成立 —— 全绿，而 follower 上 'sub-drop' 那行会被判成已提交、升主后可见。
    #
    # 改为从**壳表元组的 xmin** 取那个 xid：回放已经把元组物理落到页面上了，
    # 而 follower 的 SELECT 尚未接 gclog（那是 R3），所以行读得出来、xmin 就是
    # 当时写它的那个子事务号。语义上这是"谁写了这一行"的唯一真相源。
    xid_drop=$(PSQL $fp -Atc "SET citus.enable_ddl_propagation=off;
        SELECT xmin::text::bigint FROM ${TBL} WHERE v='sub-drop'" | tail -1)
    xid_keep=$(PSQL $fp -Atc "SET citus.enable_ddl_propagation=off;
        SELECT xmin::text::bigint FROM ${TBL} WHERE v='sub-keep'" | tail -1)
    check "follower :$fp 夹具成立：壳表上取到 sub-drop/sub-keep 的 xmin" \
          "$([[ "$xid_drop" =~ ^[0-9]+$ && "$xid_keep" =~ ^[0-9]+$ ]] && echo ok)" "ok"
    check "follower :$fp 两个子事务确实是不同的 xid" \
          "$([[ -n "$xid_drop" && "$xid_drop" != "$xid_keep" ]] && echo ok)" "ok"

    if [[ "$xid_drop" =~ ^[0-9]+$ ]]; then
      dst=$(PSQL $fp -Atc "SELECT status FROM partdist.gclog_status(${citus_gid}, ${xid_drop})")
      check "follower :$fp 被 ROLLBACK TO 的子事务(xid=${xid_drop})判决不是 committed" \
            "$([[ "$dst" != "committed" ]] && echo ok)" "ok"
    fi
    if [[ "$xid_keep" =~ ^[0-9]+$ ]]; then
      kst=$(PSQL $fp -Atc "SELECT status FROM partdist.gclog_status(${citus_gid}, ${xid_keep})")
      check "follower :$fp 已 RELEASE 的子事务(xid=${xid_keep})判决是 committed" "$kst" "committed"
    fi
  done
fi

echo ""
health_check_no_crash

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "TX2 2PC 两段式标记：全部通过"; else echo "TX2 2PC 两段式标记：存在 FAIL"; fi

# ---- 清理 ----
if [[ "${KEEP_FIXTURE:-0}" != "1" ]]; then
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SELECT partdist.replay_disable('${TBL}');" >/dev/null 2>&1
  done
  for p in $PA $PB $f1 $f2; do
    PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" >/dev/null 2>&1
  done
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL};" >/dev/null 2>&1
  done
  PSQL $COORD -q -c "DROP TABLE IF EXISTS tx2_mk;" >/dev/null 2>&1
  for p in $COORD $PA $PB $f1 $f2; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id IN (${GA},${GB});" >/dev/null 2>&1
  done
fi

exit $FAIL
