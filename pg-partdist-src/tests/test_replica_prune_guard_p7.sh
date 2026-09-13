#!/usr/bin/env bash
# [宿主机] T7.26（P7-P1）回放壳表不许被**原生剪枝**就地清掉已提交数据。
#
# 缺陷：补丁 0005 让 heap_page_prune_opt 对分片**打标**关系直接返回，判据是
# shard_relation_xid_hook（= 登记过打标）。**没打标**的分布表，它的副本壳表不在
# 这道豁免里。回放把 leader 的元组原样落到本节点页面上，元组头里是 leader 的
# **原生** xid —— 本机 clog 对这些号要么是空洞（读作中止）、要么撞上本机同号的
# 无关事务。于是任何一次普通读（SELECT、取 TOAST、甚至建索引时的堆扫描）只要
# 碰上"页面带 pd_prune_xid 且快满了"，on-access 剪枝就会拿本机 clog 判这些元组
# 为 DEAD 并就地清掉 —— 已提交的数据没了，还写了本地 WAL（副本从此与 leader 分叉）。
# 升主之后访问闸放行，概率更高。
#
# 修法（不动内核补丁）：补丁 0006/0008 在 HeapTupleSatisfiesVacuumHorizon 里留了
# 扩展侧钩子（is_shard_rel + shard_vacuum_read_hook）。把"回放槽位里登记过的壳表
# 主堆及其 TOAST 表"也认成受管关系，对它们的元组判活一律给**不可回收**
# （未删 ⇒ LIVE、被删 ⇒ RECENTLY_DEAD）—— 剪枝与 VACUUM 从此一条都删不掉。
# 回收要等 R4（写路径）或重做物理基线。
#
# 覆盖：
#   [4] 夹具前提：leader 先烧 xid，让回放来的 xid 落进副本本机 clog 的空洞
#       （这正是生产上"leader 比副本忙"的常态）
#   [5] leader 上写满一页、UPDATE 其中一部分、再改大字段 ⇒ 页面带 pd_prune_xid
#   [6] ★ 断言前提：副本上的页面**确实满足剪枝条件**（否则整个套件不构成证据）
#   [7] ★ follower1（守卫开）：普通 SELECT + 取 TOAST 之后，行指针一个都没变、
#       可见行数不变、大字段完整
#   [8] ★ 对照 follower2（守卫关）：同样的读确实把页面剪了 —— 证明 [7] 的"没变"
#       是守卫挡住的，不是夹具没做出可剪页面
#   [9] ★ follower1 之后继续回放，与 leader 逐字节一致（没有被本地剪枝打出分叉）
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
DROP TABLE IF EXISTS p7p1_prune;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE p7p1_prune(id int primary key, v text, big text);
SELECT create_distributed_table('p7p1_prune', 'id');
ALTER TABLE p7p1_prune SET (autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='p7p1_prune'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="p7p1_prune_${gid}"
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
CREATE TABLE ${shard_tbl} (LIKE p7p1_prune INCLUDING ALL);
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


PSQL $COORD -q -c "ALTER TABLE p7p1_prune SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);" >/dev/null 2>&1
for p in $f1 $f2; do
  PSQL $p -q -c "SET citus.enable_ddl_propagation=off; CREATE EXTENSION IF NOT EXISTS pageinspect;" >/dev/null 2>&1
done

# 页 0 行指针统计：normal/redirect/dead/unused
lp_stats() {  # lp_stats <port> <带引号的表名文本>（pageinspect 1.12 的 get_raw_page 只收文本名）
  PSQL $1 -Atc "SELECT count(*) FILTER (WHERE lp_flags=1)||'/'||count(*) FILTER (WHERE lp_flags=2)||'/'||count(*) FILTER (WHERE lp_flags=3)||'/'||count(*) FILTER (WHERE lp_flags=0) FROM heap_page_items(get_raw_page($2,'main',0))" </dev/null | tail -1
}
toast_of() {  # toast_of <port>
  PSQL $1 -Atc "SET citus.override_table_visibility=false; SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid='${shard_tbl}'::regclass" </dev/null | tail -1
}

echo "========== [4] 夹具前提：leader 烧 xid，让回放号落进副本本机 clog 的空洞 =========="
fmax=0
for fp in $f1 $f2; do
  x=$(PSQL $fp -Atc "SELECT txid_current()" </dev/null | tail -1)
  [[ "$x" =~ ^[0-9]+$ && "$x" -gt "$fmax" ]] && fmax=$x
done
lx=$(PSQL $pport -Atc "SELECT txid_current()" </dev/null | tail -1)
burn=$(( fmax + 3000 - lx ))
echo "  副本最大 xid=${fmax}  leader xid=${lx}  需烧 ${burn}"
if [[ "$burn" -gt 0 ]]; then
  PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
CREATE TEMP TABLE p7p1_burn(x int);
DO \$\$ BEGIN FOR i IN 1..${burn} LOOP BEGIN INSERT INTO p7p1_burn VALUES (i); EXCEPTION WHEN others THEN NULL; END; END LOOP; END \$\$;
SQL
fi
lx2=$(PSQL $pport -Atc "SELECT txid_current()" </dev/null | tail -1)
check "leader xid 已越过两个副本用过的号（${lx2} > ${fmax}）" \
      "$([[ "$lx2" =~ ^[0-9]+$ && "$lx2" -gt "$fmax" ]] && echo ok)" "ok"

echo "========== [5] leader 上写半页、UPDATE 一部分、改大字段 =========="
# ★ leader 页面**保持半满**，让 leader 自己永远到不了 on-access 剪枝的触发线。
#   第二版写满了一页，结果第二条 UPDATE 取行时 leader 先把页剪了、剪枝记录又复制到
#   副本 —— 副本页面变成已剪过的样子（redirect/dead 都有、空闲 2272 字节），这时
#   "读完没变"什么也证明不了。副本侧的触发线另在 [6] 用 fillfactor 单独调高。
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO p7p1_prune SELECT g, repeat('a', 90), NULL FROM generate_series(1, 30) g;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO p7p1_prune SELECT g, 'toast', (SELECT string_agg(md5((g*7+i)::text), '') FROM generate_series(1,400) i) FROM generate_series(101, 103) g;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE p7p1_prune SET v = repeat('b', 90) WHERE id <= 10;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE p7p1_prune SET big = (SELECT string_agg(md5((id*13+i)::text), '') FROM generate_series(1,400) i) WHERE id >= 101;"
lp=$(lead_plsn)
lxw=$(PSQL $pport -Atc "SELECT txid_current()" </dev/null | tail -1)
for fp in $f1 $f2; do
  a=$(catchup $fp "$lp" 120)
  check "副本 :$fp 追平（${a}/${lp}）" "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && echo ok)" "ok"

  # ★ 把副本本机的 xid 计数推过回放号，且让中间那段号在本机 clog 里是"中止"。
  #   第一版没做这步，结果一行都没剪：leader 发的号大于副本自己的计数，剪枝入口
  #   GlobalVisTestIsRemovableXid(prune_xid) 把它当"未来的号"直接跳过 —— 那不是
  #   守卫挡住的，是夹具没做出生产形态。生产上副本的计数会被回放拉齐到回放号之上
  #   （ShardReplayAdvanceWatermark），被跳过的号在本机 clog 里是空洞、读作中止；
  #   这里用"回滚的子事务"把同一段号在本机登记成中止，效果相同且可确定复现。
  fx=$(PSQL $fp -Atc "SELECT txid_current()" </dev/null | tail -1)
  fburn=$(( lxw + 500 - fx ))
  if [[ "$fburn" -gt 0 ]]; then
    PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
CREATE TEMP TABLE p7p1_fburn(x int);
DO \$\$ BEGIN FOR i IN 1..${fburn} LOOP BEGIN INSERT INTO p7p1_fburn VALUES (i); RAISE EXCEPTION 'burn'; EXCEPTION WHEN raise_exception THEN NULL; END; END LOOP; END \$\$;
SQL
  fi
  fx2=$(PSQL $fp -Atc "SELECT txid_current()" </dev/null | tail -1)
  check "副本 :$fp 本机 xid 计数已越过 leader 写入用的号（${fx2} > ${lxw}）" \
        "$([[ "$fx2" =~ ^[0-9]+$ && "$fx2" -gt "$lxw" ]] && echo ok)" "ok"
  PSQL $fp -q -c "CHECKPOINT;" </dev/null >/dev/null
done

echo "========== [6] ★ 前提：副本页面确实满足剪枝条件 =========="
# on-access 剪枝的触发线是 minfree = max(按 fillfactor 预留的空间, BLCKSZ/10)。
# 副本壳表单独设 fillfactor=10 ⇒ minfree = 7372 字节，半满的页也过线。
# fillfactor 只是本地"值不值得剪"的阈值（catalog 里的 reloption），**不影响回放**，
# 也不改变判活机理 —— 生产上页面迟早写满，触发的是同一条路径。
MINFREE=7372
for fp in $f1 $f2; do
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; ALTER TABLE ${shard_tbl} SET (fillfactor = 10);" </dev/null >/dev/null
done
for fp in $f1 $f2; do
  hdr=$(PSQL $fp -Atc "SELECT prune_xid||'/'||(upper-lower) FROM page_header(get_raw_page('${shard_tbl}','main',0))" </dev/null | tail -1)
  pxid=${hdr%%/*}; free=${hdr##*/}
  echo "  :$fp 页 0 prune_xid=${pxid} 空闲=${free} 字节  行指针(normal/redirect/dead/unused)=$(lp_stats $fp "'${shard_tbl}'")"
  check ":$fp 页 0 带 pd_prune_xid（有被删的旧版本）" "$([[ "$pxid" =~ ^[0-9]+$ && "$pxid" -gt 0 ]] && echo ok)" "ok"
  check ":$fp 页 0 空闲 ${free} < ${MINFREE} 字节（副本侧 on-access 剪枝的触发线）" \
        "$([[ "$free" =~ ^[0-9]+$ && "$free" -lt "$MINFREE" ]] && echo yes || echo no)" "yes"
  check ":$fp 页 0 此前没被剪过（dead/redirect 均为 0，否则\"读完没变\"不构成证据）" \
        "$(lp_stats $fp "'${shard_tbl}'" | cut -d/ -f2,3)" "0/0"
  nx=$(PSQL $fp -Atc "SELECT txid_current()" </dev/null | tail -1)
  check ":$fp prune_xid 在本机看来已可回收（${pxid} < 本机计数 ${nx}）" \
        "$([[ "$pxid" =~ ^[0-9]+$ && "$nx" =~ ^[0-9]+$ && "$pxid" -lt "$nx" ]] && echo ok)" "ok"
done

echo "========== [7] ★ follower1（守卫开）：普通读之后页面一个行指针都不许变 =========="
before1=$(lp_stats $f1 "'${shard_tbl}'")
t1=$(toast_of $f1)
tb1=$(lp_stats $f1 "'${t1}'")
cnt1=$(PSQL $f1 -Atc "SET enable_indexscan=off; SET enable_bitmapscan=off; SELECT count(*) FROM ${shard_tbl}" </dev/null 2>&1 | tail -1)
big1=$(PSQL $f1 -Atc "SELECT sum(length(big)) FROM ${shard_tbl} WHERE id >= 101" </dev/null 2>&1 | tail -1)
PSQL $f1 -q -c "CHECKPOINT;" </dev/null >/dev/null
after1=$(lp_stats $f1 "'${shard_tbl}'")
ta1=$(lp_stats $f1 "'${t1}'")
echo "  主堆页 0：${before1} → ${after1}   TOAST 页 0：${tb1} → ${ta1}"
check "★ 主堆页 0 行指针未被剪（读前后一致）" "$after1" "$before1"
check "★ TOAST 页 0 行指针未被剪（读前后一致）" "$ta1" "$tb1"
check "★ 副本可见行数 = 33（30 + 3，UPDATE 不增行）" "$cnt1" "33"
check "★ 大字段完整（3 × 12800 字节）" "$big1" "38400"

echo "========== [8] ★ 对照 follower2（守卫关）：同样的读确实会剪 =========="
before2=$(lp_stats $f2 "'${shard_tbl}'")
PSQL $f2 -Atc "SET pg_partdist.replica_prune_guard = off; SET enable_indexscan=off; SET enable_bitmapscan=off; SELECT count(*) FROM ${shard_tbl}" </dev/null >/dev/null 2>&1
PSQL $f2 -q -c "CHECKPOINT;" </dev/null >/dev/null
after2=$(lp_stats $f2 "'${shard_tbl}'")
echo "  对照组主堆页 0：${before2} → ${after2}"
check "★ 关掉守卫后页面确实被剪（证明 [7] 的\"没变\"是守卫挡住的）" \
      "$([[ -n "$after2" && -n "$before2" && "$after2" != "$before2" ]] && echo ok)" "ok"

echo "========== [9] ★ follower1 继续回放，与 leader 逐字节一致 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO p7p1_prune SELECT g, 'tail', NULL FROM generate_series(201, 205) g;"
lp=$(lead_plsn)
a=$(catchup $f1 "$lp" 120)
check "follower1 追平后续写入（${a}/${lp}）" "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && echo ok)" "ok"
diff_follower $f1 "读过之后"

echo "========== [10] 清理 + 节点健康 =========="
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS p7p1_prune;" </dev/null >/dev/null 2>&1
health_check_no_crash

echo ""
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "T7.26 回放壳表剪枝守卫：全部通过"; else echo "T7.26 回放壳表剪枝守卫：存在 FAIL"; fi
