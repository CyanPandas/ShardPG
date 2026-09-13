#!/usr/bin/env bash
# [宿主机] T7.25（P7-R4）DDL **自动跟随**验收。
#
# 在此之前，leader 上一次 CREATE INDEX / DROP INDEX，副本就停在结构栅栏，直到
# 运维在壳表上手工做等价结构变更、再重跑 replay_set_locmap() ——
# test_ddl_fileset_d1.sh 的 [9] 就是那段人工步骤。本套件验的是：
# **不做任何人工动作**，副本自己跟上，且跟上之后逐字节一致。
#
# 夹具 [0]–[4] 原样取自 test_ddl_fileset_d1.sh（表名换成 p7r4_ddl），
# 逐字节比对工具（pagecmp 掩码口径、fileset 路径反查、零覆盖守卫）同源。
#
# 覆盖：
#   [5] ★ 经协调者 CREATE INDEX → follower1 撞栅栏 → 回放启动器自动对账并重配
#       locmap → 越过栅栏追平 → 新索引逐字节一致
#   [6] ★ follower2 同样无人值守跟上（副本数不增加人工量）
#   [7] ★ 经协调者 DROP INDEX → 两个副本自动删掉对应索引、locmap 回到 4 对
#   [8] 开关 off：撞栅栏后原地停住；打开后自动跟上（证明 [5]–[7] 是它干的）
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
DROP TABLE IF EXISTS p7r4_ddl;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE p7r4_ddl(id int primary key, v text, big text);
SELECT create_distributed_table('p7r4_ddl', 'id');
ALTER TABLE p7r4_ddl SET (autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='p7r4_ddl'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="p7r4_ddl_${gid}"
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
CREATE TABLE ${shard_tbl} (LIKE p7r4_ddl INCLUDING ALL);
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

echo "========== [4] 基线写入 + 追平 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO p7r4_ddl
SELECT g, 'v'||g,
       CASE WHEN g % 10 = 0
            THEN (SELECT string_agg(md5((g*1000+i)::text), '') FROM generate_series(1,200) i)
            ELSE 'small' END
FROM generate_series(1, 100) g;
SQL
# 普通 VACUUM 建出 VM fork —— 没有它，后面 VACUUM FULL 的截断路径就只验到
# 主 fork，而 ReplayTruncateLocalRel 是**遍历全部 fork** 的。
PSQL $pport -v ON_ERROR_STOP=1 -q \
  -c "SET citus.override_table_visibility=false;" \
  -c "VACUUM ${shard_tbl};"
lp=$(lead_plsn)
a1=$(catchup $f1 "$lp" 120); a2=$(catchup $f2 "$lp" 120)
check "follower1 追平基线（${a1}/${lp}）" "$([[ "$a1" -ge "$lp" ]] && echo ok)" "ok"
check "follower2 追平基线（${a2}/${lp}）" "$([[ "$a2" -ge "$lp" ]] && echo ok)" "ok"

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

# 本套件的自动跟随要依赖回放启动器，节点上必须是开着的（默认即开）
for p in $f1 $f2; do
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.replay_auto_follow_ddl;" >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" >/dev/null
done

locmap_n() { PSQL $1 -Atc "SELECT count(*) FROM partdist.replay_locmap('${shard_tbl}')"; }
# 反复触发追平，直到越过栅栏追到 target（自动跟随在启动器里，最多等 N 秒）
catchup_until() {  # catchup_until <fport> <target> <秒>
  local fp=$1 target=$2 secs=$3 t a=0
  for t in $(seq 1 "$secs"); do
    a=$(catchup "$fp" "$target" 10)
    [[ "$a" =~ ^[0-9]+$ && "$a" -ge "$target" ]] && break
    sleep 1
  done
  echo "$a"
}
nodelog_hits() {  # nodelog_hits <fport> <正则>
  DEX bash -c "cat /work/pg-cluster-data/worker$(( $1 - 5432 ))/*.log /work/pg-cluster-data/worker$(( $1 - 5432 )).log 2>/dev/null | grep -cE '$2' || true" </dev/null | tail -1 | tr -d '[:space:]'
}

echo "========== [5] ★ 经协调者 CREATE INDEX → follower1 无人值守跟上 =========="
hint0=$(nodelog_hits $pport '已发 DDL 结构提示')
fol0=$(nodelog_hits $f1 'DDL 自动跟随：followed')
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "CREATE INDEX p7r4_ddl_v_idx ON p7r4_ddl(v);"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO p7r4_ddl SELECT g, 'x'||g, 'small' FROM generate_series(1001, 1040) g;
SQL
check "★ leader 发出了 DDL 结构提示（日志 $hint0 → $(nodelog_hits $pport '已发 DDL 结构提示')）" \
      "$([[ "$(nodelog_hits $pport '已发 DDL 结构提示')" -gt "${hint0:-0}" ]] && echo ok)" "ok"
lp=$(lead_plsn)
a1=$(catchup_until $f1 "$lp" 90)
check "★ follower1 **无人工动作**越过栅栏追平（${a1}/${lp}）" \
      "$([[ "$a1" =~ ^[0-9]+$ && "$a1" -ge "$lp" ]] && echo ok)" "ok"
check "★ follower1 回放启动器留下自动跟随日志" \
      "$([[ "$(nodelog_hits $f1 'DDL 自动跟随：followed')" -gt "${fol0:-0}" ]] && echo ok)" "ok"
check "follower1 locmap 已含新索引（5 对）" "$(locmap_n $f1)" "5"
check "follower1 壳表上确有与 leader 等价的新索引" \
      "$(PSQL $f1 -Atc "SELECT count(*) FROM pg_index i WHERE i.indrelid='${shard_tbl}'::regclass AND partdist.indexdef_string(i.indexrelid) ~ 'USING btree \(v\)'" | tail -1)" "1"
check "follower1 待办文件已清" \
      "$(DEX bash -c "ls $(PSQL $f1 -Atc 'SHOW data_directory')/pg_parwal/$(PSQL $f1 -Atc "SELECT '${shard_tbl}'::regclass::oid")/ 2>/dev/null | grep -cE 'pending_fileset|ddl_hint' || true" </dev/null | tail -1)" "0"
diff_follower $f1 "CREATE INDEX 自动跟随后"

echo "========== [6] ★ follower2 同样无人值守跟上 =========="
a2=$(catchup_until $f2 "$lp" 90)
check "★ follower2 无人工动作越过栅栏追平（${a2}/${lp}）" \
      "$([[ "$a2" =~ ^[0-9]+$ && "$a2" -ge "$lp" ]] && echo ok)" "ok"
check "follower2 locmap 5 对" "$(locmap_n $f2)" "5"
diff_follower $f2 "follower2 自动跟随后"

echo "========== [7] ★ 经协调者 DROP INDEX → 两个副本自动删掉对应索引 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "DROP INDEX p7r4_ddl_v_idx;"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO p7r4_ddl SELECT g, 'y'||g, 'small' FROM generate_series(2001, 2030) g;
SQL
lp=$(lead_plsn)
for fp in $f1 $f2; do
  a=$(catchup_until $fp "$lp" 90)
  check "★ :$fp DROP INDEX 后无人工动作追平（${a}/${lp}）" \
        "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && echo ok)" "ok"
  check ":$fp locmap 回到 4 对" "$(locmap_n $fp)" "4"
  check ":$fp 壳表上那个索引已被删掉" \
        "$(PSQL $fp -Atc "SELECT count(*) FROM pg_index i WHERE i.indrelid='${shard_tbl}'::regclass AND partdist.indexdef_string(i.indexrelid) ~ 'USING btree \(v\)'" | tail -1)" "0"
done
diff_follower $f1 "DROP INDEX 自动跟随后"

echo "========== [8] 开关 off：撞栅栏原地停住；打开后自动跟上 =========="
PSQL $f1 -q -c "ALTER SYSTEM SET pg_partdist.replay_auto_follow_ddl = off;" >/dev/null
PSQL $f1 -q -c "SELECT pg_reload_conf();" >/dev/null
sleep 2
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "CREATE INDEX p7r4_ddl_v2_idx ON p7r4_ddl(v, id);"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO p7r4_ddl SELECT g, 'z'||g, 'small' FROM generate_series(3001, 3020) g;
SQL
lp=$(lead_plsn)
(void=$(catchup $f1 "$lp" 10)) ; sleep 20
check "关掉之后 follower1 停在栅栏（needs_struct）" "$(fstate $f1)" "needs_struct"
check "关掉之后 locmap 仍是 4 对（没人动它）" "$(locmap_n $f1)" "4"
PSQL $f1 -q -c "ALTER SYSTEM RESET pg_partdist.replay_auto_follow_ddl;" >/dev/null
PSQL $f1 -q -c "SELECT pg_reload_conf();" >/dev/null
a1=$(catchup_until $f1 "$lp" 90)
check "打开之后自动跟上（${a1}/${lp}）" \
      "$([[ "$a1" =~ ^[0-9]+$ && "$a1" -ge "$lp" ]] && echo ok)" "ok"
check "打开之后 locmap 5 对" "$(locmap_n $f1)" "5"

echo "========== [8b] T7.21：DDL 内联灌页同样分块排空（VACUUM FULL，chunk=2） =========="
# VACUUM FULL 换号后要把新文件整页灌进流。此前那一段不排空，1 GB 内联上限 = 4096 个
# 捕获环槽，与别的写入分享同一个环时会被覆盖（P7-W2）。这里把分块压到 2 块，
# 证明：①真的分块了（leader 日志留痕）；②分块排空之间穿插的记录没有打乱副本（逐文件比对）；
# ③结构没变 ⇒ 不该撞栅栏，也不该触发自动跟随。
# ★ 写入量压到 150 行（≈5 个堆块，chunk=2 下必然多块）。首版写了 3000 行，
#   这一步本身就把套件拖过了 1500 s 预算：数据组 raft 日志环只有 128 条
#   （RAFT_LOG_CAPACITY），一条事务 3000 条记录要反复等 apply 腾位置。
#   夹具写多少与被测的"分块排空"无关，别让夹具吃掉预算。
echo "  [8b] $(date +%T) 写入 150 行"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO p7r4_ddl SELECT g, repeat('p', 200), 'small' FROM generate_series(5001, 5150) g;"
lp=$(lead_plsn)
echo "  [8b] $(date +%T) 两个副本追平到 ${lp}"
for fp in $f1 $f2; do catchup_until $fp "$lp" 90 >/dev/null; done
echo "  [8b] $(date +%T) VACUUM FULL（chunk=2）"
chunk0=$(nodelog_hits $pport '块流式发射')
fol0=$(nodelog_hits $f1 'DDL 自动跟随：')
old_main=$(leader_relnum 0 0)
PSQL $pport -v ON_ERROR_STOP=1 -q \
  -c "SET citus.override_table_visibility=false;" \
  -c "SET pg_partdist.fileset_baseline_chunk_blocks = 2;" \
  -c "VACUUM FULL ${shard_tbl};"
new_main=$(leader_relnum 0 0)
check "leader 主堆文件号换了（${old_main}→${new_main}）" "$([[ -n "$new_main" && "$old_main" != "$new_main" ]] && echo ok)" "ok"
chunk1=$(nodelog_hits $pport '块流式发射')
check "★ DDL 路径确实分块排空（leader 日志 ${chunk0} → ${chunk1}）" \
      "$([[ -n "$chunk1" && "$chunk1" -gt "${chunk0:-0}" ]] && echo ok)" "ok"
lp=$(lead_plsn)
for fp in $f1 $f2; do
  a=$(catchup_until $fp "$lp" 120)
  check "★ :$fp 追平分块 VACUUM FULL（${a}/${lp}）" \
        "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && echo ok)" "ok"
  check ":$fp 状态 idle（结构没变，不撞栅栏）" "$(fstate $fp)" "idle"
done
check "结构没变 ⇒ 没有触发自动跟随（:$f1 日志 ${fol0} 不变）" "$(nodelog_hits $f1 'DDL 自动跟随：')" "${fol0:-0}"
diff_follower $f1 "分块 VACUUM FULL 后"

echo "========== [9] 清理 + 节点健康 =========="
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS p7r4_ddl;" >/dev/null 2>&1
health_check_no_crash

echo ""
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "T7.25 DDL 自动跟随：全部通过"; else echo "T7.25 DDL 自动跟随：存在 FAIL"; fi
