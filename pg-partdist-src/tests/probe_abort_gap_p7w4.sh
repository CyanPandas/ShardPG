#!/usr/bin/env bash
# [宿主机] P7-W4 取证：leader 上**顶层 ROLLBACK** 之后，副本还能不能接着回放？
#
# 疑点（2026-09-13 读代码时发现，尚未实测）：PartWALAbort 在事务中止时把本后端
# 还没排空的捕获槽**直接丢弃**。可 leader 页面上那些被中止的元组已经物理写进去了
# （heap_insert 不因回滚撤销页面改动）。副本收不到这些记录 ⇒ 页内行号断档；之后
# 同一页上再插入，副本回放会拿着 leader 的 offnum 去写一个"还没那么长"的页。
#
# 本脚本只做取证，不做断言式验收：把现象原样打出来，结论写进 P7 总账。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

check() {  # check <名字> <实际> <期望>
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

# 节点崩溃检查（见 lib_node_health.sh 头部：验收脚本原本对"节点崩了"是瞎的）
source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置：group0 收敛 + 测试模式 GUC =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" >/dev/null
done

echo "========== [1] 夹具：1 分片分布表（PK 索引 + TOAST） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS p7w4_abort;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE p7w4_abort(id int primary key, v text, big text);
SELECT create_distributed_table('p7w4_abort', 'id');
ALTER TABLE p7w4_abort SET (autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='p7w4_abort'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="p7w4_abort_${gid}"
echo "  shard=${gid} leader=:${pport}(node${pnode}) followers=:${f1} :${f2}"

echo "========== [2] leader fileset 注册 + follower 壳表/locmap =========="
nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" | tail -1)
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

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE p7w4_abort INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
  np=$(set_locmap $fp)
  check "follower :$fp locmap 配对 4 对" "$np" "4"
done

for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
done

echo "========== [3] Raft 数据组 + arm 回放 =========="
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
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" >/dev/null
done

leader_oid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})")

lead_plsn() { PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})"; }

catchup() {  # catchup <fport> <target> <超时s>；回显 applied
  local fp=$1 target=$2 timeout=$3 app
  app=$(PSQL $fp -Atc \
        "SELECT partdist.replay_catchup('${shard_tbl}', ${target}, $((timeout * 1000)))" \
        2>/dev/null || echo "")
  if [[ -z "$app" || ! "$app" =~ ^[0-9]+$ ]]; then
    app=$(PSQL $fp -Atc "SELECT applied FROM partdist.replay_status() WHERE shard='${shard_tbl}'::regclass" 2>/dev/null || echo 0)
  fi
  echo "$app"
}

fstate() { PSQL $1 -Atc "SELECT state FROM partdist.replay_status() WHERE shard='${shard_tbl}'::regclass"; }
fapplied() { PSQL $1 -Atc "SELECT applied FROM partdist.replay_status() WHERE shard='${shard_tbl}'::regclass"; }

# leader 侧某个 (role,ord) 的当前文件号
leader_relnum() { PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT relnum FROM partdist.shard_fileset('${shard_tbl}') WHERE role=$1 AND ord=$2" | tail -1; }
# follower locmap 里记的 leader 文件号
locmap_leader_relnum() { PSQL $1 -Atc "SELECT leader_relnum FROM partdist.replay_locmap('${shard_tbl}') WHERE role=$2 AND ord=$3"; }

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
        "$([[ "$nmem" -ge 4 ]] && echo ok)" "ok"

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
      local want="IDENTICAL_OUTSIDE_HOLE"
      if [[ "${key%.*}" != "0" && "$same" == "IDENTICAL_EMPTY" ]]; then
        want="IDENTICAL_EMPTY"; nempty=$((nempty + 1))
      fi
      check "${tag} ${key}${fork:-.main} 掩码外逐字节一致" "$same" "$want"
      ncmp=$((ncmp + 1))
    done
  done

  # 第二道守卫：真正比过的文件数必须覆盖到全部成员（主堆另有 _vm fork）。
  # 只有这条能挡住"循环提前退出"这类失败 —— 上面那条只验路径查得出来。
  check "${tag} 实际比对了 ${ncmp} 个文件（>= 成员数 ${nmem}）" \
        "$([[ "$ncmp" -ge "$nmem" ]] && echo ok)" "ok"
}



echo "========== [4] leader 上：CHECKPOINT → 插 5 行后 ROLLBACK → 同页再提交 5 行 =========="
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL
SET citus.override_table_visibility = false;
BEGIN;
INSERT INTO ${shard_tbl} SELECT g, 'rolled-back', NULL FROM generate_series(1, 5) g;
ROLLBACK;
INSERT INTO ${shard_tbl} SELECT g, 'kept', NULL FROM generate_series(6, 10) g;
SQL
lp=$(lead_plsn)
echo "  leader 流尾 plsn=${lp}"
for fp in $f1 $f2; do
  out=$(PSQL $fp -Atc "SELECT partdist.replay_catchup('${shard_tbl}', ${lp}, 60000)" </dev/null 2>&1 | tr '\n' ' ')
  st=$(PSQL $fp -Atc "SELECT state||' applied='||applied FROM partdist.replay_status() WHERE shard='${shard_tbl}'::regclass" </dev/null | tail -1)
  echo "  :$fp catchup ⇒ ${out}"
  echo "  :$fp 回放状态 ⇒ ${st}"
  a=${out%% *}
  check ":$fp 追平到 leader 流尾" "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && echo ok || echo no)" "ok"
done
lpg=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT lp||':'||lp_flags FROM heap_page_items(get_raw_page('${shard_tbl}','main',0))" </dev/null 2>&1 | tr '\n' ' ')
echo "  leader 页 0 行指针：${lpg}"
diff_follower $f1 "ROLLBACK 之后"

echo "========== [5] 清理 + 节点健康 =========="
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS p7w4_abort;" </dev/null >/dev/null 2>&1
health_check_no_crash
echo ""
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
