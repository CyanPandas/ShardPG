#!/usr/bin/env bash
# [宿主机] D1 DDL/fileset 控制通道验收（FRD §12）。
#
# D1 之前的行为是**静默分歧**，不是报错：leader 上一次 CREATE INDEX /
# VACUUM FULL 换出新 relfilenode，而捕获判据是"文件号命中反向哈希"
# （partwal_sync.c 的 `if (found)`），没登记的记录直接 return —— 一条都不进流，
# follower 的副本文件永远停在旧内容，两侧谁都不报错。
#
# D1 交付的是 CTRL:FILESET_UPDATE 控制通道，验收分两类：
#
#   A. 只换文件号、不改结构（VACUUM FULL / REINDEX / TRUNCATE）
#      → 全自动：follower 按 (role, ord) 原地换 loc_map、把本地文件截 0，
#        随后的 FPI 把它填满，页面比对仍然一致。
#
#   B. 结构变了（CREATE INDEX / DROP INDEX）
#      → 停在结构栅栏：state=needs_struct、游标**停在该 CTRL 之前**，
#        本地补齐结构 + 重跑 replay_set_locmap() 之后原地继续。
#      为什么不自动配：ord 是位置不是身份，leader 删掉 0 号索引后 1 号会
#      补位成 0 号，硬按 (role,ord) 配会把内容灌进本地那个本该被删的索引里，
#      catalog 还宣称它是另一组列上的索引 —— 静默损坏，比停下来难查得多。
#
# 规模刻意小（100 行）：本用例验的是控制通道，不是吞吐；同步 Raft 复制
# 约 3 条记录/秒，放大规模只会让它超时。
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
DROP TABLE IF EXISTS d1_ddl;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE d1_ddl(id int primary key, v text, big text);
SELECT create_distributed_table('d1_ddl', 'id');
ALTER TABLE d1_ddl SET (autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='d1_ddl'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="d1_ddl_${gid}"
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
CREATE TABLE ${shard_tbl} (LIKE d1_ddl INCLUDING ALL);
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
INSERT INTO d1_ddl
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
  local ncmp=0
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
      same=$(DEX python3 /tmp/pagecmp.py "$lpath" "$fpath" </dev/null 2>/dev/null)
      check "${tag} ${key}${fork:-.main} 掩码外逐字节一致" "$same" "IDENTICAL_OUTSIDE_HOLE"
      ncmp=$((ncmp + 1))
    done
  done

  # 第二道守卫：真正比过的文件数必须覆盖到全部成员（主堆另有 _vm fork）。
  # 只有这条能挡住"循环提前退出"这类失败 —— 上面那条只验路径查得出来。
  check "${tag} 实际比对了 ${ncmp} 个文件（>= 成员数 ${nmem}）" \
        "$([[ "$ncmp" -ge "$nmem" ]] && echo ok)" "ok"
}

echo "========== [5] VACUUM FULL：主堆/索引/TOAST 全部换文件号，应全自动 =========="
old_main=$(leader_relnum 0 0)
PSQL $pport -v ON_ERROR_STOP=1 -q \
  -c "SET citus.override_table_visibility=false;" \
  -c "VACUUM FULL ${shard_tbl};"
new_main=$(leader_relnum 0 0)
check "leader 主堆文件号确实换了（${old_main}→${new_main}）" "$([[ "$old_main" != "$new_main" ]] && echo ok)" "ok"

lp=$(lead_plsn)
a1=$(catchup $f1 "$lp" 180)
check "follower1 越过 FILESET_UPDATE 追平（${a1}/${lp}）" "$([[ "$a1" -ge "$lp" ]] && echo ok)" "ok"
check "follower1 状态回到 idle" "$(fstate $f1)" "idle"
check "follower1 locmap 已换到新 leader 文件号" "$(locmap_leader_relnum $f1 0 0)" "$new_main"
diff_follower $f1 "VACUUM FULL 后"

echo "========== [6] REINDEX：只换索引文件号 =========="
old_idx=$(leader_relnum 1 0)
PSQL $pport -v ON_ERROR_STOP=1 -q \
  -c "SET citus.override_table_visibility=false;" \
  -c "REINDEX TABLE ${shard_tbl};"
new_idx=$(leader_relnum 1 0)
check "leader PK 索引文件号确实换了（${old_idx}→${new_idx}）" "$([[ "$old_idx" != "$new_idx" ]] && echo ok)" "ok"

lp=$(lead_plsn)
a1=$(catchup $f1 "$lp" 180)
check "follower1 追平 REINDEX（${a1}/${lp}）" "$([[ "$a1" -ge "$lp" ]] && echo ok)" "ok"
check "follower1 locmap 索引已换号" "$(locmap_leader_relnum $f1 1 0)" "$new_idx"
diff_follower $f1 "REINDEX 后"

echo "========== [7] TRUNCATE：换号且新文件为空 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "TRUNCATE d1_ddl;"
new_main2=$(leader_relnum 0 0)
check "TRUNCATE 后 leader 主堆再次换号" "$([[ "$new_main2" != "$new_main" ]] && echo ok)" "ok"

lp=$(lead_plsn)
a1=$(catchup $f1 "$lp" 180)
check "follower1 追平 TRUNCATE（${a1}/${lp}）" "$([[ "$a1" -ge "$lp" ]] && echo ok)" "ok"
check "follower1 locmap 已换到 TRUNCATE 后的文件号" "$(locmap_leader_relnum $f1 0 0)" "$new_main2"

lsz=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_size('${shard_tbl}')" | tail -1)
fsz=$(PSQL $f1 -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_size('${shard_tbl}')" | tail -1)
check "TRUNCATE 后两侧主堆大小一致（leader=${lsz}）" "$fsz" "$lsz"

echo "========== [8] CREATE INDEX：结构变了 → 必须停在栅栏 =========="
before_applied=$(fapplied $f1)
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "CREATE INDEX d1_ddl_v_idx ON d1_ddl(v);"
lp=$(lead_plsn)

# 触发追平：预期 replay_catchup 直接报错（栅栏），而不是干等到超时
fence_err=$(PSQL $f1 -Atc "SELECT partdist.replay_catchup('${shard_tbl}', ${lp}, 60000)" 2>&1 >/dev/null | head -3)
check "replay_catchup 报出结构栅栏" \
      "$(echo "$fence_err" | grep -qi "结构栅栏" && echo ok)" "ok"
check "follower1 状态 = needs_struct" "$(fstate $f1)" "needs_struct"

after_applied=$(fapplied $f1)
check "游标停在 CTRL 之前（未推进：${before_applied}→${after_applied}）" \
      "$([[ "$after_applied" -lt "$lp" ]] && echo ok)" "ok"
check "follower1 locmap 仍是 4 对（未换表）" \
      "$(PSQL $f1 -Atc "SELECT count(*) FROM partdist.replay_locmap('${shard_tbl}')")" "4"

echo "========== [9] 补齐本地结构 → 栅栏解除 → 原地继续 =========="
PSQL $f1 -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
CREATE INDEX ${shard_tbl}_v_idx ON ${shard_tbl}(v);
SQL
np=$(set_locmap $f1)
check "重跑 replay_set_locmap 配对 5 对" "$np" "5"
check "状态从 needs_struct 复位" "$(fstate $f1)" "idle"

lp=$(lead_plsn)
a1=$(catchup $f1 "$lp" 180)
check "follower1 越过栅栏追平（${a1}/${lp}）" "$([[ "$a1" -ge "$lp" ]] && echo ok)" "ok"
check "follower1 locmap 已含新索引 (role=1, ord=1)" \
      "$(PSQL $f1 -Atc "SELECT count(*) FROM partdist.replay_locmap('${shard_tbl}') WHERE role=1 AND ord=1")" "1"

# 新索引要真的有内容：写一批数据，两侧索引文件必须逐字节一致
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO d1_ddl SELECT g, 'w'||g, 'small' FROM generate_series(1, 60) g;
SQL
lp=$(lead_plsn)
a1=$(catchup $f1 "$lp" 180)
check "新索引建立后继续追平（${a1}/${lp}）" "$([[ "$a1" -ge "$lp" ]] && echo ok)" "ok"
diff_follower $f1 "CREATE INDEX 后"

echo "========== [10] 未做结构同步的 follower2 应仍停在栅栏 =========="
# f2 从头到尾没被补过索引 —— 它必须停住，而不是跟着 f1 一起"看起来正常"
f2_before=$(fapplied $f2)
PSQL $f2 -Atc "SELECT partdist.replay_catchup('${shard_tbl}', ${lp}, 60000)" >/dev/null 2>&1 || true
check "follower2 停在 needs_struct" "$(fstate $f2)" "needs_struct"
check "follower2 游标未越过栅栏" \
      "$([[ "$(fapplied $f2)" -lt "$lp" ]] && echo ok)" "ok"

echo
echo "========== 清理 =========="
PSQL $COORD -q -c "DROP TABLE IF EXISTS d1_ddl;" >/dev/null 2>&1
for fp in $f1 $f2; do
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" >/dev/null 2>&1
done

echo
health_check_no_crash
health_check_no_drops

echo "==================== 结果：PASS=${PASS} FAIL=${FAIL} ===================="
[[ "$FAIL" -eq 0 ]] || exit 1
