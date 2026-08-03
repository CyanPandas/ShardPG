#!/usr/bin/env bash
# [宿主机] R1 物理回放闭环验收（FRD §14.2 R1 行）。
#
# 验收标准：
#   1. 带索引 + TOAST 的表，leader 写入后 follower 文件与 leader
#      **逐页 diff 一致（含 LSN 域）**——MAIN + VM fork 逐字节 cmp；
#      FSM fork 排除（FSM 更新本就不写 WAL，原生备库同样不保证一致）。
#   2. kill -9 replay worker 任意时刻，重启追平且 diff 仍一致。
#   3. 用例显式制造一次 VACUUM 尾部截断（覆盖 §5.2 的 RM_SMGR 捕获特判）。
#
# 页面字节级一致的前提（都是本用例刻意安排的）：
#   - 写入后不在 leader 上读该表（hint bits 不写 WAL）；
#   - 最后 VACUUM (FREEZE)：freeze 记录整体重写 t_infomask，扫描期
#     顺手设置的 hint 位被覆盖成 canonical 状态，两侧收敛。
#
# 传输走既有 pg_raft 数据组复制（prepare 接线），回放上界用测试模式
# GUC replay_trust_local_segments=on（FRD §6 的独立验收通道）。
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

echo "========== [0] 前置：group0 收敛 + 测试模式 GUC =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" >/dev/null
done
gucv=$(PSQL 5434 -Atc "SHOW pg_partdist.replay_trust_local_segments")
check "测试模式 GUC 已生效" "$gucv" "on"

echo "========== [1] 夹具：1 分片分布表（PK 索引 + TOAST） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS r1_replay;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE r1_replay(id int primary key, v text, big text);
SELECT create_distributed_table('r1_replay', 'id');
ALTER TABLE r1_replay SET (autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='r1_replay'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport - 5431))
# 两个 follower：取除 leader 外端口最小的两个 worker
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
echo "  shard=${gid} leader=:${pport}(node${pnode}) followers=:${f1}(node${f1node}) :${f2}(node${f2node})"

shard_tbl="r1_replay_${gid}"

echo "========== [2] leader fileset 注册 + follower 壳表/locmap =========="
nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" | tail -1)
check "leader fileset 注册（主堆+PK+TOAST堆+TOAST索引=4）" "$nrels" "4"

# 导出 leader fileset（role,ord,spc,db,relnum 数组）
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE r1_replay INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])")
  check "follower :$fp locmap 配对 4 对" "$np" "4"
done

for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
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
done

for fp in $f1 $f2; do
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${shard_tbl}')")
  check "follower :$fp replay_enable" "$en" "t"
done

echo "========== [4] 批次 A：INSERT/UPDATE/DELETE（含 TOAST） =========="
# 规模按传输速率标定：同步 Raft 复制约 3 条/秒（每条一次带 fsync 的 RPC），
# 300 行 + 30 个 TOAST 值足以覆盖 heap/btree/toast/FPI 全记录类型。
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO r1_replay
SELECT g, 'v'||g,
       CASE WHEN g % 10 = 0
            THEN (SELECT string_agg(md5((g*1000+i)::text), '') FROM generate_series(1,300) i)
            ELSE 'small' END
FROM generate_series(1, 300) g;
SQL
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE r1_replay SET v = v||'-u1' WHERE id % 3 = 0;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "DELETE FROM r1_replay WHERE id % 17 = 0;"

leader_oid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})")
f1_oid=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${gid})")
lead_plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
check "leader 已产生 parwal 记录" "$([[ "$lead_plsn" -gt 100 ]] && echo ok)" "ok"

wait_caught_up() {  # wait_caught_up <fport> <期望plsn> <超时s>
  # 惰性语义（L1-a 起）：armed 不等于开始回放，必须由 replay_catchup 触发。
  # R1 的验收对象是**物理回放的结果**（页面字节、SMGR 截断、崩溃后续放），
  # 与触发方式无关，所以这里只把"被动轮询"换成"触发 + 校验"。
  local fp=$1 target=$2 timeout=$3 app=0
  local foid; foid=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${gid})")
  app=$(PSQL $fp -Atc \
        "SELECT partdist.replay_catchup(${foid}::regclass, ${target}, $((timeout * 1000)))" \
        2>/dev/null || echo 0)
  [[ -n "$app" && "$app" -ge "$target" ]] && { echo "$app"; return 0; }
  # 触发失败时回读一次游标，便于失败信息里显示实际进度
  app=$(PSQL $fp -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}" 2>/dev/null || echo 0)
  echo "$app"; return 1
}

app1=$(wait_caught_up $f1 "$lead_plsn" 90)
check "follower1 追平批次 A（applied=${app1} / ${lead_plsn}）" "$([[ "$app1" -ge "$lead_plsn" ]] && echo ok)" "ok"

echo "========== [5] 崩溃恢复：制造积压 → kill -9 → 节点重置 → 从游标追平 =========="
# 注意：kill -9 挂 shmem 的 bgworker 会让 postmaster 重置**整个节点**（PG 语义），
# 所以本步刻意避免在重置窗口内做 leader 写入（同步复制会因多数派丢失而中止）。
# 流程：停 f1 回放 → 批次 B 完整写入（f1 仍接收字节，只是不回放 → 积压）→
# 重新启用 → worker 开始追积压 → kill -9 → 节点重置恢复 → worker 从 durable
# 游标重放（页级幂等覆盖已应用部分）→ 追平。

PSQL $f1 -q -c "SELECT partdist.replay_disable('${shard_tbl}');" >/dev/null

PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO r1_replay
SELECT g, 'b'||g,
       CASE WHEN g % 10 = 0
            THEN (SELECT string_agg(md5((g*7777+i)::text), '') FROM generate_series(1,300) i)
            ELSE 'small-b' END
FROM generate_series(2001, 2300) g;
SQL
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE r1_replay SET v = v||'-u2' WHERE id % 5 = 0;"

PSQL $f1 -q -c "SELECT partdist.replay_enable('${shard_tbl}');" >/dev/null
# 惰性语义下 worker 只在 catchup 在途时才真正 apply —— 后台触发一次长追平，
# 制造"正在追积压"的窗口，kill -9 才打得中一个 apply 中的 worker。
( PSQL $f1 -Atc "SELECT partdist.replay_catchup('${shard_tbl}', NULL, 180000)" >/dev/null 2>&1 ) &
catchup_bg=$!
sleep 3

# 精确找 f1 节点的 replay worker（bgworker 的 cwd = 其数据目录）
fdir="worker$((f1 - 5432))"
wpid=$(docker exec -u postgres $CONTAINER bash -c "
  for pid in \$(pgrep -f '[r]eplay worker'); do
    [ \"\$(readlink /proc/\$pid/cwd 2>/dev/null)\" = \"/work/pg-cluster-data/${fdir}\" ] && { echo \$pid; break; }
  done")
check "找到 f1 节点的 replay worker（积压追赶中）" "$([[ -n "$wpid" ]] && echo ok)" "ok"
docker exec -u postgres $CONTAINER kill -9 "$wpid" 2>/dev/null
echo "  已 kill -9 worker(pid=$wpid)，节点将整体重置"
wait "$catchup_bg" 2>/dev/null || true   # 触发连接会随节点重置一起断开

wait_node_up() {  # <port> <超时s>
  local p=$1 timeout=$2 t
  for t in $(seq 1 "$timeout"); do
    [[ "$(PSQL $p -Atc 'SELECT 1' 2>/dev/null)" == "1" ]] && return 0
    sleep 1
  done
  return 1
}
wait_node_up $f1 60
check "f1 节点从重置中恢复" "$?" "0"

lead_plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
app1=$(wait_caught_up $f1 "$lead_plsn" 180)
check "kill -9 后 follower1 重启追平（applied=${app1} / ${lead_plsn}）" "$([[ -n "$app1" && "$app1" -ge "$lead_plsn" ]] && echo ok)" "ok"

echo "========== [6] VACUUM 尾部截断（RM_SMGR 特判验收） =========="
# 删掉尾部大段行 → VACUUM 截断尾页 → XLOG_SMGR_TRUNCATE 进流
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "DELETE FROM r1_replay WHERE id > 150;"
# 注意：VACUUM 不能与 SET 同事务（psql 单 -c 是一个事务）；分开传，
# 同一连接内 SET 会话级生效。取值处过滤 SET 命令标签（tail -1）。
before_sz=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_size('${shard_tbl}')" | tail -1)
PSQL $pport -v ON_ERROR_STOP=1 -q \
  -c "SET citus.override_table_visibility=false;" \
  -c "VACUUM (FREEZE) ${shard_tbl};"
after_sz=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_size('${shard_tbl}')" | tail -1)
check "leader 侧 VACUUM 确实截断了尾页(${before_sz}→${after_sz})" "$([[ -n "$after_sz" && -n "$before_sz" && "$after_sz" -lt "$before_sz" ]] && echo ok)" "ok"

lead_plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
app1=$(wait_caught_up $f1 "$lead_plsn" 90)
check "follower1 追平（含 SMGR 截断记录）" "$([[ "$app1" -ge "$lead_plsn" ]] && echo ok)" "ok"
app2=$(wait_caught_up $f2 "$lead_plsn" 90)
check "follower2 追平" "$([[ "$app2" -ge "$lead_plsn" ]] && echo ok)" "ok"

echo "========== [7] 刷盘 + 逐页 diff（MAIN + VM，页头/行指针/元组/special 区） =========="
# 判据是"页内空闲空洞之外逐字节一致"而非整文件 cmp：FPI 带 BKPIMAGE_HAS_HOLE 时
# RestoreBlockImage 会把空洞清零，主库那片区域保留旧元组残字节 —— 原生流复制备库
# 同样如此（详见 tests/pagecmp.py 头部说明与 FRD §14.2）。
PSQL $pport -q -c "CHECKPOINT;" >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
DEX chmod +x /tmp/pagecmp.py 2>/dev/null || true
sleep 4   # > replay_checkpoint_interval_ms，等 follower 的 apply checkpoint 刷脏落盘

pdata=$(PSQL $pport -Atc "SHOW data_directory")
f1data=$(PSQL $f1 -Atc "SHOW data_directory")
f2data=$(PSQL $f2 -Atc "SHOW data_directory")

# 逐 fileset 成员 diff：用两侧 pg_relation_filepath 配对（role,ord 序一致）
lead_paths=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||'.'||ord||','||pg_relation_filepath(relnum::regclass) FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')

diff_one_follower() {  # <fport> <fdata> <标签>
  local fp=$1 fdata=$2 tag=$3
  local frows
  frows=$(PSQL $fp -Atc "SET citus.enable_ddl_propagation=off; SELECT role||'.'||ord||','||pg_relation_filepath(relnum::regclass) FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')
  while IFS= read -r lrow; do
    local key lrel frel lpath fpath fork
    key=${lrow%%,*}; lrel=${lrow#*,}
    frel=$(echo "$frows" | grep "^${key}," | cut -d, -f2)
    for fork in "" "_vm"; do
      lpath="${pdata}/${lrel}${fork}"; fpath="${fdata}/${frel}${fork}"
      local lex fex
      lex=$(DEX bash -c "test -f '$lpath' && echo y || echo n")
      fex=$(DEX bash -c "test -f '$fpath' && echo y || echo n")
      if [[ "$lex" == "n" && "$fex" == "n" ]]; then continue; fi
      check "${tag} ${key}${fork:-.main} 两侧都存在" "$lex/$fex" "y/y"
      [[ "$lex" == "y" && "$fex" == "y" ]] || continue
      local lsz fsz
      lsz=$(DEX stat -c %s "$lpath"); fsz=$(DEX stat -c %s "$fpath")
      check "${tag} ${key}${fork:-.main} 大小一致(${lsz})" "$fsz" "$lsz"
      local same errf
      errf=$(mktemp)
      same=$(DEX python3 /tmp/pagecmp.py "$lpath" "$fpath" 2>"$errf")
      check "${tag} ${key}${fork:-.main} 洞外逐字节一致" \
            "$same" "IDENTICAL_OUTSIDE_HOLE"
      # 差在哪个字段比"差了几个字节"有用得多 —— 失败时把定性明细打出来
      if [[ "$same" != "IDENTICAL_OUTSIDE_HOLE" ]]; then
        echo "        ---- 差异定性（leader=${lpath##*/} follower=${fpath##*/}）----"
        sed 's/^/        /' "$errf"
      fi
      rm -f "$errf"
    done
  done <<< "$lead_paths"
}

diff_one_follower $f1 "$f1data" "f1"
diff_one_follower $f2 "$f2data" "f2"

echo "========== [8] 内容正确性：follower 壳表行数与 leader 一致 =========="
# 必须放在 [7] 之后：在 follower 上读会设置 hint bit，先读会污染页面比对。
#
# 为什么这里能读出行来（R1 阶段的边界）：用例以 VACUUM (FREEZE) 收尾，冻结元组
# 的 xmin 是 FrozenTransactionId，可见性判定不查 clog ⇒ 无需 xid 解析即可见。
# 未冻结的回放元组仍然不可读（xmin 是 leader 的 xid，本地 clog 是空洞，
# FRD §13.4），那要等 R3 的 gxid 路由。所以本项测的是"物理回放内容正确"，
# 不是"副本可服务读"——后者仍是 R1 的非目标（§0）。
lead_cnt=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl}" | tail -1)
f1_cnt=$(PSQL $f1 -Atc "SELECT count(*) FROM ${shard_tbl}")
check "follower 壳表行数 == leader(${lead_cnt})（冻结元组可读，验证回放内容）" \
      "$f1_cnt" "$lead_cnt"

echo
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then
  echo "R1 验收：全部通过"
else
  echo "R1 验收：存在 FAIL"
fi

# ---- 清理（保留环境可复查；夹具删除） ----
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
  PSQL $COORD -q -c "DROP TABLE IF EXISTS r1_replay;" >/dev/null 2>&1
  for p in $COORD $pport $f1 $f2; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" >/dev/null 2>&1
  done
fi

exit $FAIL
