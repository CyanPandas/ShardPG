#!/usr/bin/env bash
# [宿主机] 主从架构：**同一节点多主多从** 验收（test_mixed_role_p7 的多组并存扩展）。
#
# 主从作用域是分区(shard)，一个节点对每个它参与的分片各是一个角色。本套件把
# 混合角色节点 M 压到 4 个数据组并存：
#
#   分片   A1     A2     B1(主C1)  B2(主C2)
#   M      ●主    ●主    ○从       ○从        ← M 同时 2 主 + 2 从
#   C1     —      —      ●主       —
#   C2     —      —      —         ●主
#   4 个组的成员集都取全部 worker（3 worker 时即 {W1,W2,W3}，多数派 2）；
#   A1/A2 由 M 抢当选，B1/B2 由各自的 Ci 抢当选
#
# 用一张 16 分片表按 placement 落位挑角色：某节点自然承载 ≥2 分片 → 那两个当 A1/A2
# （M 是它们的主）；另挑两个主在不同节点的分片当 B1/B2（M 去当它们的从）。四者是
# 真实分片、四个不同本地 OID，各自独立的日志 / 分片 xid 空间 / clog 目录。
#
# 断言（多组并存下各自不串）：
#   [3] M 上 A1/A2 = native_leader、B1从/B2从 = replica，四个不同 OID  （四重身份并存）
#   [4] 两个主各自能读写、且写各落各的分片                              （多主：互不覆盖）
#   [5] 往两个主狂写 → 两个从副本字节全不变                            （多组物理隔离）
#   [6] 往两个从各自的主(C1/C2)写 → 两个从都追平且逐字节一致           （多从：各跟各的主）
#   [7] 四套分片 xid 空间独立可重号：各分片首行 xmin 都是小号分片 xid，  （xid 空间不串线，规则 6）
#       按各自 OID 解析、键不冲突
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
check() {
  if [[ -z "$2" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1)); else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "========== [0] 前置：group0 收敛 + 测试模式 + 抬 election_timeout（规则 19） =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"
# ---- 拓扑自适应：动态读 worker 列表与 raft 节点号（不再假设 8 worker / port-5431）----
WORKERS=($(PSQL $COORD -Atc "SELECT nodeport FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive ORDER BY nodeport"))
NW=${#WORKERS[@]}
check "至少 3 个 worker（3 成员组才有多数派 2，容得下两个从滞后）" "$([[ "$NW" -ge 3 ]] && echo ok)" "ok"
declare -A NID
for p in "${WORKERS[@]}"; do NID[$p]=$(PSQL $p -Atc "SHOW pg_raft.node_id" | tail -1); done
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
echo "  拓扑：${NW} 个 worker = ${WORKERS[*]}   组成员集=$ALLMEM"
for p in "${WORKERS[@]}" $COORD; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_raft.election_timeout_ms = 20000;" </dev/null >/dev/null
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done

MADE_GROUPS=""; SHELLS=""; M=""
cleanup() {
  local g port gid t
  for g in $MADE_GROUPS; do port="${g%%:*}"; gid="${g##*:}"; PSQL "$port" -q -c "SELECT partdist.pg_raft_group_drop($gid)" </dev/null >/dev/null 2>&1 || true; done
  for t in $SHELLS; do PSQL "$M" -q -c "SELECT partdist.replay_disable('$t'); SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS $t" </dev/null >/dev/null 2>&1 || true; done
  PSQL $COORD -q -c "DROP TABLE IF EXISTS mr" </dev/null >/dev/null 2>&1 || true
  for p in "${WORKERS[@]}" $COORD; do
    PSQL $p -q -c "ALTER SYSTEM RESET pg_raft.election_timeout_ms;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.replay_trust_local_segments;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
  echo "  [复原] 组已拆、表已删、election_timeout 已 RESET"
}
trap cleanup EXIT

echo "========== [1] 夹具：16 分片表，按落位挑 2 主(定 M) + 2 从(主在别处) =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS mr;
SET citus.shard_count = 16; SET citus.shard_replication_factor = 1;
CREATE TABLE mr(id int primary key, v text);
SELECT create_distributed_table('mr','id');
ALTER TABLE mr SET (autovacuum_enabled=off);
SQL
# 每个分片 → (shardid, primary 端口)
MAP=$(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='mr'::regclass ORDER BY s.shardid")
# 挑一个承载 ≥2 分片的节点当 M，取它的头两个分片当 A1/A2
M=$(awk '{c[$2]++} END{for(p in c) if(c[p]>=2){print p; exit}}' <<<"$MAP")
check "找到承载 ≥2 分片的节点 M（多主的前提）" "$([[ -n "$M" ]] && echo ok)" "ok"
A_SIDS=($(awk -v m="$M" '$2==m{print $1}' <<<"$MAP" | head -2))
# 另挑两个主在**不同且非 M**节点的分片当 B1/B2
B_LINES=($(awk -v m="$M" '$2!=m && !seen[$2]++{print $1":"$2}' <<<"$MAP" | head -2))
Mn=${NID[$M]}
echo "  M=:$M(node$Mn)  |  A1=${A_SIDS[0]} A2=${A_SIDS[1]}(主都在 M)  |  B1=${B_LINES[0]} B2=${B_LINES[1]}(主各在别处)"
check "挑到 2 个主在 M 的分片(A1/A2)" "$([[ -n "${A_SIDS[0]}" && -n "${A_SIDS[1]}" ]] && echo ok)" "ok"
check "挑到 2 个主在不同他节点的分片(B1/B2)" "$([[ -n "${B_LINES[0]}" && -n "${B_LINES[1]}" ]] && echo ok)" "ok"
C1=${B_LINES[0]##*:}; C2=${B_LINES[1]##*:}
# 3 个凑多数派的额外 worker（排除 M/C1/C2/协调者）

for p in "${WORKERS[@]}"; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done

echo "========== [2a] 建 2 个 A 组：M 抢当选（多主） =========="
amem="$ALLMEM"
for sid in "${A_SIDS[@]}"; do
  PSQL $M -q -c "SELECT partdist.pg_raft_group_create($sid, $amem);" </dev/null >/dev/null; sleep 2
  for p in "${WORKERS[@]}"; do [[ "$p" == "$M" ]] && continue; PSQL $p -q -c "SELECT partdist.pg_raft_group_create($sid, $amem);" </dev/null >/dev/null; MADE_GROUPS+="$p:$sid "; done
  MADE_GROUPS+="$M:$sid "
  st=""; for t in $(seq 1 40); do st=$(PSQL $M -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$sid" 2>/dev/null|tail -1); [[ "$st" == "leader" ]] && break; sleep 1; done
  check "A 组 $sid 主落在 M" "$st" "leader"
done

echo "========== [2b] 建 2 个 B 组：Ci 抢当选，M 供从副本（多从） =========="
for bl in "${B_LINES[@]}"; do
  sid=${bl%%:*}; c=${bl##*:}; cn=${NID[$c]}; shell="mr_${sid}"
  bmem="$ALLMEM"
  nrel=$(PSQL $c -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('mr_${sid}')" | tail -1)
  check "C(:$c) 登记 B$sid fileset(2)" "$nrel" "2"
  PSQL $M -v ON_ERROR_STOP=1 -q >/dev/null <<SQL
SET citus.enable_ddl_propagation=off;
DROP TABLE IF EXISTS $shell;
CREATE TABLE $shell (LIKE mr INCLUDING ALL);
ALTER TABLE $shell SET (autovacuum_enabled=off);
SQL
  SHELLS+="$shell "
  PSQL $M -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null
  rows=$(PSQL $c -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('mr_${sid}') ORDER BY role,ord" | grep ',')
  rl=$(cut -d, -f1<<<"$rows"|paste -sd,); od=$(cut -d, -f2<<<"$rows"|paste -sd,); sp=$(cut -d, -f3<<<"$rows"|paste -sd,); db=$(cut -d, -f4<<<"$rows"|paste -sd,); rn=$(cut -d, -f5<<<"$rows"|paste -sd,)
  np=$(PSQL $M -Atc "SELECT partdist.replay_set_locmap('$shell', ARRAY[$rl], ARRAY[$od], ARRAY[$sp]::oid[], ARRAY[$db]::oid[], ARRAY[$rn]::oid[])" | tail -1)
  check "M 上 B$sid 从 locmap 配对(2)" "$np" "2"
  PSQL $c -q -c "SELECT partdist.pg_raft_group_create($sid, $bmem);" </dev/null >/dev/null; sleep 2
  for p in "${WORKERS[@]}"; do [[ "$p" == "$c" ]] && continue; PSQL $p -q -c "SELECT partdist.pg_raft_group_create($sid, $bmem);" </dev/null >/dev/null; MADE_GROUPS+="$p:$sid "; done
  MADE_GROUPS+="$c:$sid "
  PSQL $M -q -c "SELECT partdist.replay_enable('$shell');" </dev/null >/dev/null
  st=""; for t in $(seq 1 40); do st=$(PSQL $c -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$sid" 2>/dev/null|tail -1); [[ "$st" == "leader" ]] && break; sleep 1; done
  check "B 组 $sid 主落在 C(:$c)" "$st" "leader"
done

echo "========== [3] ★ M 的四重身份：2×native_leader + 2×replica，四个不同 OID =========="
declare -A OID
nrole_ok=0; distinct=""
for sid in "${A_SIDS[@]}"; do
  o=$(PSQL $M -Atc "SELECT partdist.local_partition_for_shard($sid)" | tail -1); OID[$sid]=$o
  r=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve($o, 1)" | tail -1)
  [[ "$r" == "native_leader" ]] && nrole_ok=$((nrole_ok+1)); distinct+="$o "
done
rrole_ok=0
for bl in "${B_LINES[@]}"; do
  sid=${bl%%:*}; o=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT 'mr_${sid}'::regclass::oid" | tail -1); OID[$sid]=$o
  r=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve($o, 1)" | tail -1)
  [[ "$r" == "replica" ]] && rrole_ok=$((rrole_ok+1)); distinct+="$o "
done
check "★ M 上 2 个分片是 native_leader（2 主并存）" "$nrole_ok" "2"
check "★ M 上 2 个分片是 replica（2 从并存）" "$rrole_ok" "2"
nuniq=$(echo $distinct | tr ' ' '\n' | sort -u | grep -c .)
check "★ 四个角色是四个不同本地 OID（互相独立）" "$nuniq" "4"

echo "========== [4] ★ 两个主各自能读写、且写各落各的分片 =========="
# 用取模命中特定分片：Citus 按 hash(id) 路由。直接经协调者写全表再按分片计数，
# 简单可靠——两个 A 分片各自的行数都 > 0 且相加 = 命中它俩的总行数即可。
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO mr SELECT g,'v'||g FROM generate_series(1,4000) g;"
n1=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM mr_${A_SIDS[0]}" | tail -1)
n2=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM mr_${A_SIDS[1]}" | tail -1)
echo "  A1=${A_SIDS[0]} 行数=$n1   A2=${A_SIDS[1]} 行数=$n2"
check "★ A1 主在 M 上读到自己的行(>0)" "$([[ "$n1" =~ ^[0-9]+$ && "$n1" -gt 0 ]] && echo ok)" "ok"
check "★ A2 主在 M 上读到自己的行(>0)" "$([[ "$n2" =~ ^[0-9]+$ && "$n2" -gt 0 ]] && echo ok)" "ok"
# 各落各的分片：两个 A 分片的 id 集合**不相交**（同一 id 不会同时出现在两张分片里）。
# 这才真正证明"多主没互相覆盖"，而不是靠行数碰巧不等。
overlap=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM mr_${A_SIDS[0]} a JOIN mr_${A_SIDS[1]} b USING(id)" | tail -1)
check "★ 两个 A 主的 id 集合不相交（各落各的分片，overlap=$overlap）" "$overlap" "0"

echo "========== [5] ★ 往两个主狂写 → 两个从副本字节全不变（多组物理隔离） =========="
bdir=$(PSQL $M -Atc "SHOW data_directory" | tail -1)
declare -A BPATH BMD5
for bl in "${B_LINES[@]}"; do sid=${bl%%:*}; PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null
  BPATH[$sid]=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('mr_${sid}')" | tail -1)
  BMD5[$sid]=$(DEX bash -c "md5sum '$bdir/${BPATH[$sid]}' 2>/dev/null | cut -d' ' -f1" </dev/null); done
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO mr SELECT g,'w'||g FROM generate_series(5001,12000) g;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE mr SET v='u'||id WHERE id<=2000;"
PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null
iso=0
for bl in "${B_LINES[@]}"; do sid=${bl%%:*}
  m1=$(DEX bash -c "md5sum '$bdir/${BPATH[$sid]}' 2>/dev/null | cut -d' ' -f1" </dev/null)
  [[ -n "${BMD5[$sid]}" && "${BMD5[$sid]}" == "$m1" ]] && iso=$((iso+1)); done
check "★ 往两个 A 主狂写后，两个 B 从副本主堆字节都不变" "$iso" "2"
ap=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('mr_${A_SIDS[0]}')" | tail -1)
ablk=$(DEX bash -c "echo \$(( \$(stat -c %s '$bdir/$ap') / 8192 ))" </dev/null)
check "  防假通过：A1 主堆确有多块（$ablk>1）" "$([[ "$ablk" -gt 1 ]] && echo ok)" "ok"

echo "========== [6] ★ 往两个从各自的主写 → 两个从都追平且逐字节一致（多从） =========="
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO mr SELECT g,'b'||g FROM generate_series(20001,24000) g;"
caught=0; ident=0
for bl in "${B_LINES[@]}"; do
  sid=${bl%%:*}; c=${bl##*:}; shell="mr_${sid}"
  lp=$(PSQL $c -Atc "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($sid))" | tail -1)
  a=""; for t in $(seq 1 60); do a=$(PSQL $M -Atc "SELECT partdist.replay_catchup('$shell', $lp, 10000)" 2>/dev/null|tail -1); [[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && break; sleep 1; done
  [[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && caught=$((caught+1))
  PSQL $c -q -c "CHECKPOINT;" </dev/null >/dev/null; PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null; sleep 1
  cdir=$(PSQL $c -Atc "SHOW data_directory" | tail -1)
  cpath=$(PSQL $c -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('mr_${sid}')" | tail -1)
  spath=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('$shell')" | tail -1)
  same=$(DEX python3 /tmp/pagecmp.py --kind=heap "$cdir/$cpath" "$bdir/$spath" </dev/null 2>/dev/null)
  echo "  B$sid: 追平 $a/$lp   逐字节=$same"
  [[ "$same" == "IDENTICAL_OUTSIDE_HOLE" ]] && ident=$((ident+1))
done
check "★ 两个 B 从都追平到各自主的位点" "$caught" "2"
check "★ 两个 B 从都与各自的主逐字节一致" "$ident" "2"

echo "========== [7] ★ 四套分片 xid 空间独立、按各自 OID 解析、可重号不串（规则 6） =========="
small=0; declare -A FX
for sid in "${A_SIDS[@]}"; do
  x=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT min(xmin::text::bigint) FROM mr_${sid}" | tail -1); FX[$sid]=$x
  [[ "$x" =~ ^[0-9]+$ && "$x" -lt 1000 ]] && small=$((small+1)); done
for bl in "${B_LINES[@]}"; do sid=${bl%%:*}
  x=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SET pg_partdist.allow_replica_access=on; SELECT min(xmin::text::bigint) FROM mr_${sid}" | tail -1); FX[$sid]=$x
  [[ "$x" =~ ^[0-9]+$ && "$x" -lt 1000 ]] && small=$((small+1)); done
check "★ 四个分片首行 xmin 都是小号分片 xid（四套独立空间，非全局大 xid）" "$small" "4"
# 可重号：A1、A2 两个主各自的发号空间独立，最小 xmin 极可能相同（都从头发），
# 但它们是不同分片、不同 OID、不同 clog 目录 —— 数值撞了也不串。
echo "  各分片(OID→最小 xmin)：$(for s in "${A_SIDS[@]}" $(for b in "${B_LINES[@]}";do echo ${b%%:*};done); do printf '%s(%s)=%s ' "$s" "${OID[$s]}" "${FX[$s]}"; done)"
nuniq2=$(for s in "${A_SIDS[@]}" $(for b in "${B_LINES[@]}";do echo ${b%%:*};done); do echo "${OID[$s]}"; done | sort -u | grep -c .)
check "★ 四套账目落在四个不同 OID 的 clog 空间（键不冲突）" "$nuniq2" "4"

echo "========== [8] 节点健康 =========="
health_check_no_crash
echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
if [[ "$FAIL" -eq 0 ]]; then echo "同一节点多主多从：全部通过"; else echo "同一节点多主多从：存在 FAIL"; fi
