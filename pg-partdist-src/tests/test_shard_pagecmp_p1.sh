#!/usr/bin/env bash
# [宿主机] P1 三方页面一致性验收（TX_TSO_MVCC_DEV_PLAN.md T1.9 之二 / T1.5 正式验收）。
#
# 三方一致性目标（P1 的灵魂）：同一批写入，
#   ① leader 正常路径页面、② leader 崩溃恢复 redo 页面、③ follower 物理回放页面
# 三者逐字节相同。
#
# 阶段 1（①vs②，崩溃点扫描）：本地白名单表，三个崩溃点
#   CP1 = 大批 INSERT + COPY(multi_insert) 之后（checkpoint 之前，redo 覆盖全量）；
#   CP2 = UPDATE（含 TOAST 重写）/DELETE/中止事务之后；
#   CP3 = CHECKPOINT 之后再写一小批（redo 只覆盖检查点之后的窗口）。
#   每个点：get_raw_page 全页 dump（主堆+TOAST堆+TOAST索引）→ immediate 崩溃 →
#   重启 → 再 dump → 逐 fork pagecmp 掩码比对（heap_mask 口径，见 dump_rel 上方注释）。
#
# 阶段 2（①vs②vs③）：R1 配方的 1 分片分布表（无 PK——P1 白名单表禁索引），
#   raft 组同步复制 + 白名单打标写入 → dump A → leader immediate 崩溃重启 →
#   dump B（A==B 即 ①==②）→ CHECKPOINT → follower replay_catchup →
#   pagecmp 逐 fileset 成员文件比对（②==③，含 pd_lsn 域）。
#
# ★ follower 壳表绝不能被 SELECT：follower 无白名单，on-access 剪枝会按原生
#   clog 把分片元组清掉，③ 方文件就地损毁（这正是 P1_PRECHECK 结论 D 的
#   反面教材）。本脚本只用文件级比对触碰 follower。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
WPORT="${WPORT:-5433}"
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

# ①vs② 的比对口径 = pagecmp.py 掩码（与 ②vs③ 同口径 = 内核 heap_mask /
# wal_consistency_checking）。不能用裸 md5：PD_PAGE_FULL 等 pd_flags 提示位是
# 普通路径专属的非 WAL 提示，redo 从不设置（T1.9 实测：页写满后 heap_update
# 设 PD_PAGE_FULL，崩溃恢复页少这一位）——内核自己就把它列为可合法分歧的
# 提示域。掩码外的一切字节仍要求逐一相等。
SCRATCH=$(mktemp -d /tmp/p1_pagecmp.XXXXXX)
trap 'rm -rf "$SCRATCH"' EXIT

dump_rel() {  # dump_rel <port> <relname> <outfile>：全 main-fork 页拼成二进制
  # ★ 宿主机高负载时 docker exec 偶发 containerd 管道超时，报错串会顶替
  #   命令替换结果（实测出过 DeadlineExceeded 串）——凡不是整页十六进制
  #   一律视为取样失败，重试最多 3 次。
  local port=$1 rel=$2 out=$3 hex t
  for t in 1 2 3; do
    hex=$(PSQL "$port" -Atc "SET citus.override_table_visibility=false;
      SELECT coalesce(string_agg(encode(get_raw_page('$rel',b),'hex'),'' ORDER BY b),'')
        FROM generate_series(0,(pg_relation_size('$rel')/8192)::int-1) b" </dev/null 2>/dev/null | tail -1)
    if [[ -n "$hex" && "$hex" != *[!0-9a-f]* ]] && (( ${#hex} % 16384 == 0 )); then
      printf '%s' "$hex" | python3 -c 'import sys; open(sys.argv[1],"wb").write(bytes.fromhex(sys.stdin.read().strip()))' "$out"
      return 0
    fi
    sleep 2
  done
  : > "$out"   # 空文件 → pagecmp 报 IDENTICAL_EMPTY/size 差，断言自然红
  return 1
}

crash_restart() {  # crash_restart <port> <datadir>
  DEX /work/pg-install/bin/pg_ctl -D "$2" -m immediate stop </dev/null >/dev/null 2>&1
  sleep 1
  DEX /work/pg-install/bin/pg_ctl -D "$2" -l "$2/startup.log" start </dev/null >/dev/null 2>&1
  local up="" t
  for t in $(seq 1 45); do
    up=$(PSQL "$1" -Atc "SELECT 1" </dev/null 2>/dev/null) && [[ "$up" == "1" ]] && break
    sleep 1
  done
  [[ "$up" == "1" ]]
}

echo "================ 阶段 1：崩溃点扫描（①vs②） ================"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS p1cs;
CREATE TABLE p1cs(id int, v int, big text)
  WITH (autovacuum_enabled=off, toast.autovacuum_enabled=off);
SET citus.enable_ddl_propagation TO off;
CREATE EXTENSION IF NOT EXISTS pageinspect;
SQL
CSOID=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p1cs'" </dev/null)
PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${CSOID}';" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL "$WPORT" -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$CSOID" ]] && break; sleep 1
done
check "阶段1 白名单生效" "$guc" "$CSOID"
W1DATA=$(PSQL "$WPORT" -Atc "SHOW data_directory" </dev/null)
CS_TOAST=$(PSQL "$WPORT" -Atc "SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid=${CSOID}" </dev/null)
CS_TIDX="${CS_TOAST}_index"

cp_scan() {  # cp_scan <崩溃点名>：dump→crash→restart→dump→逐 fork pagecmp
  local tag=$1 rel kind same base nrel=0
  local safe="${tag//[^A-Za-z0-9]/_}"
  for rel in p1cs "$CS_TOAST" "$CS_TIDX"; do
    dump_rel "$WPORT" "$rel" "$SCRATCH/${safe}_$(basename "$rel")_pre.bin"
  done
  crash_restart "$WPORT" "$W1DATA"
  check "${tag}: 崩溃后节点恢复" "$?" "0"
  for rel in p1cs "$CS_TOAST" "$CS_TIDX"; do
    base="$SCRATCH/${safe}_$(basename "$rel")"
    dump_rel "$WPORT" "$rel" "${base}_post.bin"
    case "$rel" in *_index) kind=btree ;; *) kind=heap ;; esac
    same=$(python3 "$(dirname "$0")/pagecmp.py" --kind="$kind" "${base}_pre.bin" "${base}_post.bin" 2>/dev/null)
    check "${tag}: ${rel} redo 页面一致（pagecmp/${kind}）" "$same" "IDENTICAL_OUTSIDE_HOLE"
    nrel=$((nrel+1))
  done
  check "${tag}: 比对 fork 计数守卫" "$nrel" "3"
}

# ---- CP1：INSERT 大批 + COPY（multi_insert 路径），checkpoint 之前 ----
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO p1cs
SELECT g, 0, CASE WHEN g % 10 = 0
             THEN (SELECT string_agg(md5((g*1000+i)::text),'') FROM generate_series(1,300) i)
             ELSE 'small' END
FROM generate_series(1, 40) g;
COPY p1cs FROM PROGRAM 'seq 41 50 | awk ''{print $1",0,copyrow"}''' WITH (FORMAT csv);
SQL
cp_scan "CP1(insert+copy)"

# ---- CP2：UPDATE（含 TOAST 重写）/ DELETE / 中止事务 ----
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL'
UPDATE p1cs SET v = v + 1 WHERE id % 3 = 0;
UPDATE p1cs SET big = big || 'X' WHERE id % 10 = 0;
DELETE FROM p1cs WHERE id % 17 = 0;
SQL
PSQL "$WPORT" -q -c "BEGIN; INSERT INTO p1cs VALUES (900,0,'aborted'),(901,0,'aborted'); ROLLBACK;" </dev/null >/dev/null
cp_scan "CP2(update+delete+abort)"

# ---- CP3：CHECKPOINT 之后再写一小批（redo 窗口只含小批） ----
PSQL "$WPORT" -q -c "CHECKPOINT;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "INSERT INTO p1cs SELECT g, 9, 'post-ckpt' FROM generate_series(60,70) g;" </dev/null
cp_scan "CP3(post-checkpoint)"

PSQL "$WPORT" -q -c "DROP TABLE p1cs;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
DEX rm -f "${W1DATA}/pg_shard_xid/${CSOID}" </dev/null

echo "================ 阶段 2：三方一致（①vs②vs③） ================"
leader0=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" </dev/null)
check "group0 有 leader" "$([[ -n "$leader0" && "$leader0" != "0" ]] && echo ok)" "ok"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done

PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS p1_3way;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE p1_3way(id int, v text, big text);
SELECT create_distributed_table('p1_3way', 'id');
ALTER TABLE p1_3way SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='p1_3way'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="p1_3way_${gid}"
echo "  shard=${gid} leader=:${pport}(node${pnode}) followers=:${f1}(node${f1node}) :${f2}(node${f2node})"

nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册（主堆+TOAST堆+TOAST索引=3，无 PK）" "$nrels" "3"

fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE p1_3way INCLUDING ALL);
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

# ---- P1 白名单：leader 上把分片表 OID 加入打标名单 ----
SOID=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT '${shard_tbl}'::regclass::oid" </dev/null | tail -1)
PSQL $pport -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${SOID}';" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL $pport -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$SOID" ]] && break; sleep 1
done
check "leader 白名单生效（分片表打分片 xid）" "$guc" "$SOID"
PDATA=$(PSQL $pport -Atc "SHOW data_directory" </dev/null)

# ---- 打标写入（全部 leader 直写） ----
# ★ 本分支上 coordinator 路由的分布式写一律 Citus 2PC（worker 上 PREPARE
#   TRANSACTION，DTX 链路即建于其上）→ 撞 P1 的"含分片写禁 PREPARE"禁令，
#   事务在 PREPARE 点中止（实测两轮：页面只剩中止残留、parwal 零记录）。
#   Citus 2PC × 分片打标的接线是 P4 的正题（决议搬迁 + MX），P1 的页面字节
#   链路验收全程 leader 直写；multi_insert 记录用 COPY FROM PROGRAM 覆盖。
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL
SET citus.override_table_visibility=false;
INSERT INTO ${shard_tbl}
SELECT g, 'v'||g, CASE WHEN g % 10 = 0
       THEN (SELECT string_agg(md5((g*1000+i)::text),'') FROM generate_series(1,300) i)
       ELSE 'small' END
FROM generate_series(1, 120) g;
COPY ${shard_tbl} FROM PROGRAM 'seq 201 210 | awk ''{print \$1",copy,small"}''' WITH (FORMAT csv);
UPDATE ${shard_tbl} SET v = v||'-u1' WHERE id % 3 = 0;
UPDATE ${shard_tbl} SET big = big||'X' WHERE id % 20 = 0;
DELETE FROM ${shard_tbl} WHERE id % 17 = 0;
SQL
committed=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl}" </dev/null | tail -1)
check "工作负载提交数据真实可见（130 插入 - 8 删除 = 122，防中止残留假象）" "$committed" "122"
# ★ 阶段 2 不做中止写入（阶段 1 CP2 已覆盖 ①vs②）：parwal 在 ABORT 时丢弃
#   本后端未刷记录（PartWALAbort，设计如此），中止字节只进原生 WAL 不进
#   分区流 —— leader 页面上的中止元组残留是本地现象，③ 方永远没有。
#   三方"逐字节相同"的判据以 parwal 流内容为界（提交路径字节）；中止残留
#   的清理归 vacuum 章（P5），对可见性无影响（ABORTED/缺席均不可见）。

sxmin=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT t_xmin FROM heap_page_items(get_raw_page('${shard_tbl}',0)) WHERE lp=1" </dev/null | tail -1)
check "leader 分片确实打了分片 xid（首行 xmin=${sxmin} < 100）" "$([[ -n "$sxmin" && "$sxmin" -lt 100 ]] && echo ok)" "ok"

leader_oid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null)
lead_plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})" </dev/null)
check "leader 已产生 parwal 记录" "$([[ -n "$lead_plsn" && "$lead_plsn" -gt 100 ]] && echo ok)" "ok"

# ---- ①vs②：dump A → leader immediate 崩溃重启 → dump B ----
ST_TOAST=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid=${SOID}" </dev/null | tail -1)
ST_TIDX="${ST_TOAST}_index"
for rel in "$shard_tbl" "$ST_TOAST" "$ST_TIDX"; do
  dump_rel "$pport" "$rel" "$SCRATCH/tw_$(basename "$rel")_pre.bin"
done
crash_restart "$pport" "$PDATA"
check "三方: leader 崩溃后恢复" "$?" "0"
n3=0
for rel in "$shard_tbl" "$ST_TOAST" "$ST_TIDX"; do
  dump_rel "$pport" "$rel" "$SCRATCH/tw_$(basename "$rel")_post.bin"
  case "$rel" in *_index) kind=btree ;; *) kind=heap ;; esac
  same=$(python3 "$(dirname "$0")/pagecmp.py" --kind="$kind" \
         "$SCRATCH/tw_$(basename "$rel")_pre.bin" "$SCRATCH/tw_$(basename "$rel")_post.bin" 2>/dev/null)
  check "三方: ${rel} ①正常路径==②崩溃 redo（pagecmp/${kind}）" "$same" "IDENTICAL_OUTSIDE_HOLE"
  n3=$((n3+1))
done
check "三方: ①vs② fork 计数守卫" "$n3" "3"

# ---- ②vs③：followers 追平 → 双侧落盘 → 文件级 pagecmp ----
wait_caught_up() {  # <fport> <期望plsn> <超时s>
  local fp=$1 target=$2 timeout=$3 app foid
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null)
  app=$(PSQL "$fp" -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${target}, $((timeout * 1000)))" </dev/null 2>/dev/null || echo 0)
  [[ -n "$app" && "$app" -ge "$target" ]] && { echo "$app"; return 0; }
  app=$(PSQL "$fp" -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}" </dev/null 2>/dev/null || echo 0)
  echo "$app"; return 1
}
app1=$(wait_caught_up $f1 "$lead_plsn" 180)
check "follower1 追平（applied=${app1} / ${lead_plsn}）" "$([[ -n "$app1" && "$app1" -ge "$lead_plsn" ]] && echo ok)" "ok"
app2=$(wait_caught_up $f2 "$lead_plsn" 180)
check "follower2 追平（applied=${app2} / ${lead_plsn}）" "$([[ -n "$app2" && "$app2" -ge "$lead_plsn" ]] && echo ok)" "ok"

PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
sleep 4   # > replay_checkpoint_interval_ms，等 follower apply checkpoint 刷脏

pagecmp_kind() {  # $1=key(role.ord) $2=fork
  [[ "$2" == "_vm" ]] && { echo vm; return; }
  case "${1%%.*}" in 1|3) echo btree ;; *) echo heap ;; esac
}
FILESET_PATHS_SQL="SELECT role||'.'||ord||','||
       pg_relation_filepath(pg_filenode_relation(
           CASE WHEN spc = 1663 THEN 0 ELSE spc END, relnum))
  FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord"
lead_paths=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; ${FILESET_PATHS_SQL}" </dev/null | grep ',')

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
      local lex fex
      lex=$(DEX bash -c "test -f '$lpath' && echo y || echo n" </dev/null)
      fex=$(DEX bash -c "test -f '$fpath' && echo y || echo n" </dev/null)
      if [[ "$lex" == "n" && "$fex" == "n" ]]; then continue; fi
      check "${tag} ${key}${fork:-.main} 两侧都存在" "$lex/$fex" "y/y"
      [[ "$lex" == "y" && "$fex" == "y" ]] || continue
      local kind same
      kind=$(pagecmp_kind "$key" "$fork")
      same=$(DEX python3 /tmp/pagecmp.py --kind="$kind" "$lpath" "$fpath" </dev/null 2>/dev/null)
      check "${tag} ${key}${fork:-.main} pagecmp(${kind})" "$same" "IDENTICAL_OUTSIDE_HOLE"
      ncmp=$((ncmp+1))
    done
  done
  check "${tag} 比对文件计数守卫（≥3）" "$([[ "$ncmp" -ge 3 ]] && echo ok)" "ok"
}
diff_one_follower $f1 "三方:f1"
diff_one_follower $f2 "三方:f2"

echo "================ 清理 + 节点健康 ================"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" </dev/null >/dev/null 2>&1
done
for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1
done
for fp in $f1 $f2; do
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS p1_3way;" </dev/null >/dev/null 2>&1
for p in $COORD $pport $f1 $f2; do
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" </dev/null >/dev/null 2>&1
done
PSQL $pport -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
DEX rm -f "${PDATA}/pg_shard_xid/${SOID}" </dev/null
health_check_no_crash

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
exit "$FAIL"
