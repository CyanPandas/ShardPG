#!/usr/bin/env bash
# [宿主机] D2 冻结账目同步验收（FRD §13 约束 5）。
#
# 问题：副本壳表的元组物理上确实被冻结了（leader 的 freeze 记录随流回放，
# 元组字节两侧一致），但 pg_class.relfrozenxid 这个**目录字段**在 follower 上
# 没人维护 —— 它停在建壳表那一刻的值。而 follower 的 nextXid 会被回放水位
# 不断拉高（§7.5），于是 age(relfrozenxid) 无界增长，迟早越过
# autovacuum_freeze_max_age；越过之后 autovacuum_enabled=off **被忽略**
# （autovacuum.c:3196），副本壳表照样被强制 anti-wraparound vacuum 扫到 ——
# 而它的元组带的是外来节点的 xid。更糟的是那次 vacuum 会写本地 WAL 碰副本
# 文件，把 §13 约束 12 那个洞重新打开。
#
# 方案 (a)：leader 经 CTRL:FREEZE_UPDATE 把自己的 relfrozenxid/relminmxid
# 同步过来。搬原值是**真话**而不是编造 —— relfrozenxid = X 的语义是"本关系内
# 不存在比 X 更老的未冻结 xid"，副本堆页与 leader 逐字节一致，同一句话在
# follower 上同样成立。
#
# ★ 本用例的鉴别力来自"同步前两侧必须不同"这条断言：壳表是 follower 本地建的，
# 它的初值来自 follower 的 xid 计数器，与 leader 的必然不同。只断言"同步后
# 相等"是不够的 —— 万一两边碰巧本来就相等，用例就什么也没证明。
set -u

CONTAINER="${CONTAINER:-pg-citus-replay-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }
check() {
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

# 节点崩溃检查（见 lib_node_health.sh 头部：验收脚本原本对"节点崩了"是瞎的）
source "$(dirname "$0")/lib_node_health.sh"
health_mark_start
check_ne() {  # check_ne <名字> <a> <b>：断言两者**不同**
  if [[ "$2" != "$3" ]]; then echo "  PASS  $1（$2 ≠ $3）"; PASS=$((PASS+1));
  else echo "  FAIL  $1（两者相同='$2'，用例失去鉴别力）"; FAIL=$((FAIL+1)); fi
}

echo "========== [0] 前置：先设 GUC，再等收敛 =========="
# ★ GUC 必须在建夹具**之前**设好：ALTER SYSTEM + pg_reload_conf() 会让各
# bgworker 重读配置，其间到该节点的连接可能被正在换主的分区组 FATAL 掉。
# 先设、再等收敛、最后建夹具，就把这段抖动关在夹具之外。
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.freeze_sync_interval_ms = 0;" >/dev/null 2>&1
  PSQL $p -q -c "SELECT pg_reload_conf();" >/dev/null 2>&1
done

echo "========== [0b] 等 group0 收敛 =========="
# 光看"有没有 leader"不够：9 节点集群重启后要几十秒才收敛，其间连接会以
# "本节点不是该分区组的 leader" 直接 FATAL 掉，表现成一串莫名其妙的空值。
# 判据取"连续 3 次采样 leader/term 都不变"。
settled=no; prev=""; same=0
for t in $(seq 1 60); do
  cur=$(PSQL $COORD -Atc "SELECT leader_node_id||'/'||current_term FROM partdist.pg_raft_get_cluster_status()" 2>/dev/null | tail -1)
  if [[ -n "$cur" && "${cur%%/*}" != "0" && "$cur" == "$prev" ]]; then
    same=$((same+1)); [[ "$same" -ge 2 ]] && { settled=yes; break; }
  else
    same=0
  fi
  prev="$cur"; sleep 2
done
check "group0 收敛（leader/term=${prev}）" "$settled" "yes"
[[ "$settled" == "yes" ]] || { echo "  集群未收敛，后续断言不可信，中止"; exit 1; }

echo "========== [1] 夹具 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS d2_freeze;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE d2_freeze(id int primary key, v text, big text);
SELECT create_distributed_table('d2_freeze', 'id');
ALTER TABLE d2_freeze SET (autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='d2_freeze'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport-5431))
f1=""; for p in 5433 5434 5435 5436 5437 5438 5439 5440; do [[ "$p" == "$pport" ]] && continue; f1=$p; break; done
f2=""; for p in 5433 5434 5435 5436 5437 5438 5439 5440; do [[ "$p" == "$pport" || "$p" == "$f1" ]] && continue; f2=$p; break; done
f1node=$((f1-5431)); f2node=$((f2-5431))
shard_tbl="d2_freeze_${gid}"
echo "  shard=${gid} leader=:${pport} follower=:${f1}"

gucv=$(PSQL $pport -Atc "SHOW pg_partdist.freeze_sync_interval_ms" 2>/dev/null | tail -1)
check "leader 侧 freeze_sync_interval_ms=0（[0] 已设）" "$gucv" "0"

PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" >/dev/null
rows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role,ord" | grep ',')
roles=$(echo "$rows"|cut -d, -f1|paste -sd,); ords=$(echo "$rows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$rows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$rows"|cut -d, -f4|paste -sd,)
rels=$(echo "$rows"|cut -d, -f5|paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE d2_freeze INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
  PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])" >/dev/null
done

# 取某侧的 relfrozenxid（role: main / toast）。
# 两个 SET 缺一不可：override_table_visibility 不设，follower 上看不见 shard 表，
# SELECT 直接报错，-Atc 只吐出 "SET" 这个命令标签 —— 拿它去做断言就是恒真。
# 环境偶发瞬时连接失败，这里做有界重试；仍失败则回空，由断言暴露。
frozen() {  # frozen <port> <main|toast>
  local p=$1 which=$2 sql v t
  if [[ "$which" == "main" ]]; then
    sql="SELECT relfrozenxid::text FROM pg_class WHERE oid='${shard_tbl}'::regclass"
  else
    sql="SELECT c.relfrozenxid::text FROM pg_class c
          WHERE c.oid = (SELECT reltoastrelid FROM pg_class WHERE oid='${shard_tbl}'::regclass)"
  fi
  for t in 1 2 3 4 5; do
    v=$(PSQL $p -Atc "SET citus.enable_ddl_propagation=off;
        SET citus.override_table_visibility=false; ${sql}" 2>/dev/null | tail -1)
    [[ "$v" =~ ^[0-9]+$ ]] && { echo "$v"; return; }
    sleep 1
  done
  echo ""
}

# ★ 基准必须在**任何回放之前**取：freeze_sync_interval_ms=0 时第一次追平就
# 已经把账目同步过去了，那之后再取"同步前的值"取到的是同步后的值。
foll_initial=$(frozen $f1 main)
echo "  follower 壳表建成时的 relfrozenxid 初值 = ${foll_initial}"
check "初值取到了数字（否则后面'不等于初值'的断言恒真）" \
      "$([[ "$foll_initial" =~ ^[0-9]+$ ]] && echo ok)" "ok"
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null; done

members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" >/dev/null
done

leader_oid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" | tail -1)
check "取到 leader 侧本地分区 OID" "$([[ -n "$leader_oid" && "$leader_oid" != "0" ]] && echo ok)" "ok"

# 空的 bound 会让 replay_catchup 收到语法错误的 SQL 而静默返回空 ——
# 那会让后面每一条断言都在比较空串，看起来像产品坏了。这里直接卡住。
lead_plsn() {
  local v; v=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})" 2>/dev/null | tail -1)
  [[ "$v" =~ ^[0-9]+$ ]] || { echo "  取 leader plsn 失败（得到 '${v}'）" >&2; echo 0; return; }
  echo "$v"
}
# 失败时把 psql 的原始报错打出来 —— 吞掉 stderr 只会让后面每条断言都在比空串
catchup() {
  local out errf; errf=$(mktemp)
  out=$(PSQL $1 -Atc "SELECT partdist.replay_catchup('${shard_tbl}', $2, 180000)" 2>"$errf" | tail -1)
  if [[ ! "$out" =~ ^[0-9]+$ ]]; then
    echo "  replay_catchup(:$1, bound=$2) 未返回数字，psql 原始输出：" >&2
    sed 's/^/      /' "$errf" >&2
  fi
  rm -f "$errf"; echo "$out"
}

echo "========== [2] 写入 + 追平（此时两侧 relfrozenxid 应当不同） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
-- 规模刻意小：VACUUM (FREEZE) 会刷出大量 FPI，而本环境同步 Raft 约 3 条
-- 记录/秒，写入洪水会把心跳饿死触发改选，领导权一移走就不会自动抢回。
-- 本用例验的是账目同步，不是吞吐；两条 TOAST 值足够覆盖 TOAST 堆的断言。
INSERT INTO d2_freeze
SELECT g, 'v'||g,
       CASE WHEN g % 10 = 0
            THEN (SELECT string_agg(md5((g*1000+i)::text), '') FROM generate_series(1,40) i)
            ELSE 'small' END
FROM generate_series(1, 20) g;
SQL
lp=$(lead_plsn); a=$(catchup $f1 "$lp")
check "follower 追平（${a}/${lp}）" "$([[ "$a" -ge "$lp" ]] && echo ok)" "ok"

lead_main_before=$(frozen $pport main)
foll_main_before=$(frozen $f1 main)
echo "  leader main relfrozenxid=${lead_main_before}  follower=${foll_main_before}"
check_ne "follower 的值已离开建表初值（说明同步确实动过它）" \
         "$foll_main_before" "$foll_initial"

# 等本分区组的领导权回到 $pport。
# VACUUM (FREEZE) 会刷出大量 FPI，而本环境同步 Raft 约 3 条记录/秒 ——
# 心跳被写入洪水饿死就会触发改选，之后所有经协调节点的写入都以
# "本节点不是该分区组的 leader" 被拒。不等它回来，后面每条断言都在比空串。
wait_group_leader() {
  local t st
  for t in $(seq 1 60); do
    st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null | tail -1)
    [[ "$st" == "leader" ]] && return 0
    sleep 2
  done
  return 1
}

echo "========== [3] leader 侧 VACUUM (FREEZE) 推进 relfrozenxid =========="
PSQL $pport -v ON_ERROR_STOP=1 -q \
  -c "SET citus.override_table_visibility=false;" \
  -c "VACUUM (FREEZE) ${shard_tbl};"
lead_main_after=$(frozen $pport main)
check_ne "leader 的 relfrozenxid 确实被 VACUUM 推进了" "$lead_main_after" "$lead_main_before"

if wait_group_leader; then
  echo "  分区组领导权仍在 :${pport}"
else
  echo "  注意：VACUUM 后领导权已移走（本环境同步 Raft ~3 条/秒，FPI 洪水会饿死心跳）"
fi

echo "========== [4] follower 追平后应当拿到 leader 的账目 =========="
# 发射失败**不再中止 VACUUM**（尽力而为），失败时基线保持不变 ⇒ 下次检查重发。
# 所以这里重试若干轮：每轮触发一次写入 + 追平，直到账目跟上或轮次用尽。
# 这测的正是"最终会补上"这个真实保证，而不是"第一次就成"。
lead_main=""; foll_main=""; lead_toast=""; foll_toast=""; synced=no
for attempt in 1 2 3 4 5 6; do
  PSQL $COORD -q -c "INSERT INTO d2_freeze SELECT ${attempt}+600,'w','small';" >/dev/null 2>&1
  lp=$(lead_plsn); a=$(catchup $f1 "$lp")
  lead_main=$(frozen $pport main);   foll_main=$(frozen $f1 main)
  lead_toast=$(frozen $pport toast); foll_toast=$(frozen $f1 toast)
  if [[ -n "$lead_main" && "$foll_main" == "$lead_main" && "$foll_toast" == "$lead_toast" ]]; then
    synced=yes
    echo "  第 ${attempt} 轮追上（applied=${a}/${lp}）"
    break
  fi
  echo "  第 ${attempt} 轮未同步（主堆 leader=${lead_main} follower=${foll_main}），重试"
  sleep 3
done
check "冻结账目最终同步（重试 ${attempt} 轮）" "$synced" "yes"
check "主堆 relfrozenxid 已同步（leader=${lead_main}）" "$foll_main" "$lead_main"
check "TOAST 堆 relfrozenxid 已同步（leader=${lead_toast}）" "$foll_toast" "$lead_toast"
# 双保险：值等于 leader 且不等于建表初值 —— 缺后一条，两边碰巧相同时用例就白跑
check_ne "同步后的值确实不是建表初值（初值=${foll_initial}）" "$foll_main" "$foll_initial"

echo "========== [5] age() 有界：不再朝 anti-wraparound 阈值漂 =========="
pct=$(PSQL $f1 -Atc "SELECT pct_to_force FROM partdist.replay_freeze_status() WHERE shard='${shard_tbl}'::regclass" 2>/dev/null | tail -1)
echo "  follower 距强制阈值：${pct}%"
check "follower 距 anti-wraparound 阈值 < 50%" \
      "$(awk -v p="${pct:-999}" 'BEGIN{print (p<50)?"ok":"no"}')" "ok"

echo "========== [6] 幂等：不再变化时不应重复发射 =========="
plsn_a=$(lead_plsn)
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "SELECT count(*) FROM d2_freeze;" >/dev/null
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO d2_freeze SELECT 900,'x','small';"
plsn_b=$(lead_plsn)
# 一次 INSERT 的记录数很小；若每个事务都重发 FREEZE_UPDATE，增量会明显更大。
# 这里只做粗判：增量不应超过 20 条。
delta=$((plsn_b - plsn_a))
echo "  两次采样间流增量 = ${delta} 条"
check "冻结账目未变时不重复发射（增量 ${delta} <= 20）" \
      "$([[ "$delta" -le 20 ]] && echo ok)" "ok"

echo
echo "========== 清理 =========="
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.freeze_sync_interval_ms;" >/dev/null 2>&1; PSQL $p -q -c "SELECT pg_reload_conf();" >/dev/null 2>&1; done
PSQL $COORD -q -c "DROP TABLE IF EXISTS d2_freeze;" >/dev/null 2>&1
for fp in $f1 $f2; do
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" >/dev/null 2>&1
done
# ★ Raft 组必须一起删掉。只删表不删组，会留下一个指向**已不存在的分区**的组，
# 而那会让后续到该节点的连接直接 FATAL（"本节点不是该分区组的 leader"），
# 表现成下一轮用例里一连串莫名其妙的空值。实测踩过。
for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.pg_raft_group_drop(${gid});" >/dev/null 2>&1
done

echo
health_check_no_crash
health_check_no_drops

echo "==================== 结果：PASS=${PASS} FAIL=${FAIL} ===================="
[[ "$FAIL" -eq 0 ]] || exit 1
