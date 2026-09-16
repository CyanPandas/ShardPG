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
# 用 8 张单分片表按 placement 落位挑角色（一表一分片一组 ⇒ 每次写只碰一个组）：某节点自然承载 ≥2 分片 → 那两个当 A1/A2
# （M 是它们的主）；另挑两个主在不同节点的分片当 B1/B2（M 去当它们的从）。四者是
# 真实分片、四个不同本地 OID，各自独立的日志 / 分片 xid 空间 / clog 目录。
#
# 断言（多组并存下各自不串）：
#   [3] M 上 A1/A2 = native_leader、B1从/B2从 = replica，四个不同 OID  （四重身份并存）
#   [4] 两个主各自能读写、且写各落各的分片                              （多主：互不覆盖）
#   [5] 往两个主狂写 → 两个从副本字节全不变                            （多组物理隔离）
#   [6] 往两个从各自的主(C1/C2)写 → 两个从都追平且逐字节一致           （多从：各跟各的主）
#   [7] 四套 xid 空间互不串线：2 个主的号走本机原生语义、2 个从的号走回放两跳
#       （xid_map→gclog），各按自己的 OID 解析（规则 6）。注：本用例不打标，元组带原生 xid
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
# ★ **不抬** election_timeout（规则 19 是给 9 节点满载机器的）：抬到 20s 会让
#   "placement 抢跑当选"失效，组被没有数据的节点赢走 ⇒ 写入"未达多数派"。
#   改用 build_group_on 的"落错就三台一起拆、重来"，确定性放主。
for p in "${WORKERS[@]}" $COORD; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done

MADE_GROUPS=""; SHELLS=""; M=""
cleanup() {
  local r p t
  # 拆组要三台一起、拆两轮才干净（规则 16）；不用 MADE_GROUPS —— build_group_on 跑在
  # 命令替换的子 shell 里，它的累加传不回来
  for r in 1 2; do for p in "${WORKERS[@]:-}"; do
    PSQL "$p" -q -c "SELECT partdist.pg_raft_group_drop(group_id) FROM partdist.pg_raft_group_status() WHERE group_id<>0" </dev/null >/dev/null 2>&1 || true
  done; done
  for t in $SHELLS; do PSQL "$M" -q -c "SELECT partdist.replay_disable('$t'); SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS $t" </dev/null >/dev/null 2>&1 || true; done
  PSQL $COORD -q -c "DROP TABLE IF EXISTS mr" </dev/null >/dev/null 2>&1 || true
  for p in "${WORKERS[@]}" $COORD; do
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.replay_trust_local_segments;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "ALTER SYSTEM RESET pg_raft.election_timeout_ms;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
  echo "  [复原] 组已拆、表已删、GUC 已 RESET"
}
trap cleanup EXIT
# ---- 建组并**确定性**地把主放到指定节点 ----
# 规则 4 的"抢跑当选"不可靠；而"落错就拆组重来"更糟：拆组是**节点本地**的，
# 逐台拆的过程中，还留着该组的成员会经 RV/AE 把它**重建**回来，且带着上一轮的 term；
# 于是重建出的组里 follower 记着更高的 term，新 leader 的条目一条都 ack 不了 ——
# 实测 group 102060 在 term=2 恒卡 1/2 票，而没拆过的 102062 一次就全绿。
# 正解：建组**之前**把 placement 节点的选举超时压到 1.5s、其余抬到 9s，
# 它必然先超时先竞选（随机区间 [1.5,3]s vs [9,18]s，不重叠），一次选中，全程不拆组。
# 建完即把超时 RESET 回环境默认（6000ms）。
build_group_on() {   # build_group_on <gid> <期望leader端口> <成员ARRAY>；回显最终 state
  local gid=$1 want=$2 mem=$3 t p st=""
  for p in "${WORKERS[@]}"; do
    if [[ "$p" == "$want" ]]; then
      PSQL "$p" -q -c "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" </dev/null >/dev/null
    else
      PSQL "$p" -q -c "ALTER SYSTEM SET pg_raft.election_timeout_ms = 9000" </dev/null >/dev/null
    fi
    PSQL "$p" -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null
  done
  for p in "${WORKERS[@]}"; do
    PSQL "$p" -q -c "SELECT partdist.pg_raft_group_create($gid, $mem);" </dev/null >/dev/null
  done
  for t in $(seq 1 40); do
    st=$(PSQL "$want" -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid" 2>/dev/null | tail -1)
    [[ "$st" == "leader" ]] && break
    sleep 1
  done
  for p in "${WORKERS[@]}"; do
    PSQL "$p" -q -c "ALTER SYSTEM RESET pg_raft.election_timeout_ms" </dev/null >/dev/null
    PSQL "$p" -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null
  done
  echo "$st"
}

# ---- 写之前确认主还在该在的节点（规则 8：分区主会自治漂移）----
# 实测：建 B 组 / 给 M 供 B 的副本期间 M 很忙，A 组的心跳被挤掉 → 被 follower 推翻 →
# 控制面翻 pg_dist_placement → Citus 把写路由到新主。现象是"经协调者读得到 50 行、
# 在 M 上直读是 0"——数据没丢，只是不在 M 上，此时 M 已不是 A 的主，断言自然不成立。
# 这里只等待 + 取证，**不拆组、不偏置选举超时**（偏置是节点级的，会连带把 M 变成
# B 组的候选人、把 C 的主权抢走）。漂移不回来就把现场打出来，交给人判断。
ensure_leader() {   # ensure_leader <gid> <期望端口> <标签>；回显 state
  local gid=$1 want=$2 tag=$3 t p st=""
  for t in $(seq 1 30); do
    st=$(PSQL "$want" -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid" 2>/dev/null | tail -1)
    [[ "$st" == "leader" ]] && { echo "$st"; return 0; }
    sleep 1
  done
  {
    echo "  [取证] ${tag}：组 $gid 的主不在 :$want（state=$st），三台视角："
    for p in "${WORKERS[@]}"; do
      echo "         :$p → $(PSQL "$p" -Atc "SELECT state||' term='||current_term||' leader='||leader_node_id FROM partdist.pg_raft_group_status() WHERE group_id=$gid" 2>/dev/null | tail -1)"
    done
    echo "         pg_dist_placement 现指向 :$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$gid" 2>/dev/null | tail -1)"
  } >&2
  echo "$st"
}

echo "========== [1] 夹具：一张 (worker数×2) 分片表，按落位挑 2 主(定 M) + 2 从(主在别处) =========="
# ★ 拓扑要**造**出来，不能指望 Citus 自己分散：单分片表恒落第一个 worker（实测建 9 张
#   单分片表、连 colocate_with=>'none' 把共置组拆开都没用，9 张全落 :5433），
#   citus_move_shard_placement() 又被产品有意禁用（T4.6/§9.2：打标集群上搬分片
#   用原生快照读写 = 静默错读）。
#   正解是用**分片数**造：一张表分片数 = worker 数 ×2，Citus 按轮转铺开，每台正好 2 片
#   ⇒ 任取一台当 M（它有 2 个主），另外两台各出 1 片当 B（M 给它们当 2 个从）。
NSH=$((NW*2))
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS mr;
SET citus.shard_count = $NSH; SET citus.shard_replication_factor = 1;
CREATE TABLE mr(id int primary key, v text);
SELECT create_distributed_table('mr','id');
ALTER TABLE mr SET (autovacuum_enabled=off);
SQL
MAP=$(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='mr'::regclass ORDER BY s.shardid")
M=$(awk '{c[$2]++} END{for(p in c) if(c[p]>=2){print p; exit}}' <<<"$MAP")
check "找到承载 >=2 个分片的节点 M（多主的前提）" "$([[ -n "$M" ]] && echo ok)" "ok"
A_SIDS=($(awk -v m="${M:-x}" '$2==m{print $1}' <<<"$MAP" | head -2))
B_LINES=($(awk -v m="${M:-x}" '$2!=m && !seen[$2]++{print $1":"$2}' <<<"$MAP" | head -2))
check "挑到 2 个主在 M 的分片(A1/A2)" "$([[ ${#A_SIDS[@]} -eq 2 ]] && echo ok)" "ok"
check "挑到 2 个主在不同他节点的分片(B1/B2)" "$([[ ${#B_LINES[@]} -eq 2 ]] && echo ok)" "ok"
if [[ ${#A_SIDS[@]} -ne 2 || ${#B_LINES[@]} -ne 2 ]]; then
  { echo "  [取证] 造不出多主多从拓扑，当前落位："; echo "$MAP" | sed 's/^/         /'; } >&2
  echo ""; echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="; exit 1
fi
Mn=${NID[$M]}
declare -A A_NAME_OF BNAME_OF
for sid in "${A_SIDS[@]}"; do A_NAME_OF[$sid]=mr; done
for bl in "${B_LINES[@]}"; do BNAME_OF["${bl%%:*}"]=mr; done
echo "  M=:$M(node$Mn)  |  A1=${A_SIDS[0]} A2=${A_SIDS[1]}(主都在 M)  |  B1=${B_LINES[0]} B2=${B_LINES[1]}(主各在别处)"

# ---- 定点写：只碰一个分片 = 只碰一个 raft 组 ----
# ★ 为什么必须定点：一条 `INSERT ... SELECT generate_series` 会同时写到**全部**分片，
#   提交时要为多个 raft 组一起做同步复制 —— 而这些组的成员是同样那 3 台，
#   A→B 与 B→A 的复制压在**同一条 peer 连接**上（pg_raft 的 peer_conn 是每节点一条、
#   所有组共用的进程级 static），实测 4 个组同时报 `未达多数派` +
#   `another command is already in progress`。
#   Citus 对**多行 VALUES** 和 **IN(常量列表)** 会逐行剪枝，全落一片时直接路由成
#   `Task Count: 1`（实测 EXPLAIN 证实）；而 `INSERT ... SELECT` 不会（走
#   `Custom Scan (Citus INSERT ... SELECT)`）。所以写一律拼 VALUES / IN 列表。
#   本用例验的是"多组并存"，不是"多组并发提交"——后者是另一件事，已单独记录。
ids_of() {   # ids_of <分片id> <起始id> <个数>；回显逗号分隔 id 列表
  PSQL $COORD -Atc "SELECT string_agg(g::text,',') FROM (SELECT g FROM generate_series($2,$2+40000) g WHERE get_shard_id_for_distribution_column('mr',g)=$1 LIMIT $3) t" </dev/null | tail -1
}
ins_into() {  # ins_into <分片id> <起始id> <个数> <单字符值>
  local vals
  vals=$(PSQL $COORD -Atc "SELECT string_agg('('||g||',repeat(''$4'',100))', ',') FROM (SELECT g FROM generate_series($2,$2+40000) g WHERE get_shard_id_for_distribution_column('mr',g)=$1 LIMIT $3) t" </dev/null | tail -1)
  if [[ -z "$vals" ]]; then echo "  [取证] 分片 $1 挑不出 id，写入跳过" >&2; return 1; fi
  PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO mr VALUES $vals;"
}

for p in "${WORKERS[@]}"; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done

# ★ 数据组的 follower 必须在本地有分片站点才能落盘并 ack（规则 24）。供副本一律走
#   产品入口 provision_shard_replica()：它先去目标建壳表+身份，再发基线、配 locmap、arm。
#   组取 **2 成员**（leader + 一个副本，多数派 2）：4 个组各供 1 个副本，比 3 成员省一半
#   建站时间，而"多主多从"的命题只需要 M 同时持有 2 个主 + 2 个从。
provision_to() {   # provision_to <leader端口> <gid> <目标端口>
  local lp=$1 gid=$2 tp=$3 r
  r=$(PSQL "$lp" -Atc "SELECT partdist.provision_shard_replica(${gid}::bigint, ${NID[$tp]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本已供到 :$tp（${r:0:40}）" "$([[ "$r" == shard=* ]] && echo ok)" "ok"
}
OTHERS=(); for p in "${WORKERS[@]}"; do [[ "$p" == "$M" ]] && continue; OTHERS+=("$p"); done

echo "========== [2a] 建 2 个 A 组（M 当选）+ 各供 1 个副本 =========="
for i in 0 1; do
  sid=${A_SIDS[$i]}; mate=${OTHERS[$i]}
  st=$(build_group_on "$sid" "$M" "ARRAY[${NID[$M]},${NID[$mate]}]")
  check "A 组 $sid 主落在 M" "$st" "leader"
  provision_to "$M" "$sid" "$mate"
done

echo "========== [2b] 建 2 个 B 组（各自 C 当选）+ 副本供到 M（M 因此成为 2 个从）=========="
for bl in "${B_LINES[@]}"; do
  sid=${bl%%:*}; c=${bl##*:}; shell="${BNAME_OF[$sid]}_${sid}"
  st=$(build_group_on "$sid" "$c" "ARRAY[${NID[$c]},${NID[$M]}]")
  check "B 组 $sid 主落在 C(:$c)" "$st" "leader"
  provision_to "$c" "$sid" "$M"
  SHELLS+="$shell "
  nloc=$(PSQL $M -Atc "SELECT count(*) FROM partdist.replay_locmap('$shell')" </dev/null | tail -1)
  check "M 上 B$sid 从 locmap 已配对(4)" "$nloc" "4"
done

echo "========== [3] ★ M 的四重身份：2×native_leader + 2×replica，四个不同 OID =========="
declare -A OID
nrole_ok=0; distinct=""
for sid in "${A_SIDS[@]}"; do
  o=$(PSQL $M -Atc "SELECT partdist.local_partition_for_shard($sid)" | tail -1); OID[$sid]=$o
  r=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve($o, 1)" | tail -1)
  [[ "$r" == "native_leader" ]] && nrole_ok=$((nrole_ok+1)); distinct+="$o "
done
for bl in "${B_LINES[@]}"; do
  sid=${bl%%:*}; o=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT '${BNAME_OF[$sid]}_${sid}'::regclass::oid" | tail -1); OID[$sid]=$o
  distinct+="$o "
done
# ★ "从 = replica" 的断言挪到 [6] 两个从都追平之后：route_resolve 认壳表靠
#   apply_checkpoint 文件，回放真的跑过一次才有。
check "★ M 上 2 个分片是 native_leader（2 主并存）" "$nrole_ok" "2"
nuniq=$(echo $distinct | tr ' ' '\n' | sort -u | grep -c .)
check "★ 四个角色是四个不同本地 OID（互相独立）" "$nuniq" "4"

echo "========== [4] ★ 两个主各自能读写、且写各落各的分片 =========="
okl=0; for sid in "${A_SIDS[@]}"; do l=$(ensure_leader "$sid" "$M" "写 A 之前"); [[ "$l" == "leader" ]] && okl=$((okl+1)); done
check "写之前：M 仍是两个 A 分片的主（主漂了后面全不作数）" "$okl" "2"
# 用取模命中特定分片：Citus 按 hash(id) 路由。直接经协调者写全表再按分片计数，
# 简单可靠——两个 A 分片各自的行数都 > 0 且相加 = 命中它俩的总行数即可。
# 写入量按本环境实测吞吐（约 23 条 raft 记录/秒）配；值加宽保证各分片主堆涨到多块。
# 写入量按本环境实测吞吐（约 23 条 raft 记录/秒）配；值加宽保证各分片主堆涨到多块。
for i in 0 1; do ins_into "${A_SIDS[$i]}" 1 150 v; done
n1=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM mr_${A_SIDS[0]}" | tail -1)
n2=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM mr_${A_SIDS[1]}" | tail -1)
echo "  A1=${A_SIDS[0]} 行数=$n1   A2=${A_SIDS[1]} 行数=$n2"
check "★ A1 主在 M 上只读到定点写给它的 150 行" "$n1" "150"
check "★ A2 主在 M 上只读到定点写给它的 150 行" "$n2" "150"
# 各落各的分片：两个 A 分片的 id 集合**不相交**（同一 id 不会同时出现在两张分片里）。
# 这才真正证明"多主没互相覆盖"，而不是靠行数碰巧不等。
overlap=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM mr_${A_SIDS[0]} a JOIN mr_${A_SIDS[1]} b USING(id)" | tail -1)
check "★ 两个 A 主的 id 集合不相交（各落各的分片，overlap=$overlap）" "$overlap" "0"

echo "========== [5] ★ 往两个主狂写 → 两个从副本字节全不变（多组物理隔离） =========="
okl=0; for sid in "${A_SIDS[@]}"; do l=$(ensure_leader "$sid" "$M" "狂写 A 之前"); [[ "$l" == "leader" ]] && okl=$((okl+1)); done
check "狂写之前：M 仍是两个 A 分片的主" "$okl" "2"
bdir=$(PSQL $M -Atc "SHOW data_directory" | tail -1)
declare -A BPATH BMD5
for bl in "${B_LINES[@]}"; do sid=${bl%%:*}; PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null
  BPATH[$sid]=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('${BNAME_OF[$sid]}_${sid}')" | tail -1)
  BMD5[$sid]=$(DEX bash -c "md5sum '$bdir/${BPATH[$sid]}' 2>/dev/null | cut -d' ' -f1" </dev/null); done
for i in 0 1; do
  ins_into "${A_SIDS[$i]}" 5001 150 w
  uids=$(ids_of "${A_SIDS[$i]}" 1 60)
  PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE mr SET v=repeat('u',100) WHERE id IN (${uids:-0});"
done
PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null
iso=0
for bl in "${B_LINES[@]}"; do sid=${bl%%:*}
  m1=$(DEX bash -c "md5sum '$bdir/${BPATH[$sid]}' 2>/dev/null | cut -d' ' -f1" </dev/null)
  [[ -n "${BMD5[$sid]}" && "${BMD5[$sid]}" == "$m1" ]] && iso=$((iso+1)); done
check "★ 往两个 A 主写 600 行 + 改 200 行后，两个 B 从副本主堆字节都不变" "$iso" "2"
ap=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('mr_${A_SIDS[0]}')" | tail -1)
ablk=$(DEX bash -c "echo \$(( \$(stat -c %s '$bdir/$ap') / 8192 ))" </dev/null)
check "  防假通过：A1 主堆确有多块（$ablk>1）" "$([[ "$ablk" -gt 1 ]] && echo ok)" "ok"

echo "========== [6] ★ 往两个从各自的主写 → 两个从都追平且逐字节一致（多从） =========="
okl=0; for bl in "${B_LINES[@]}"; do l=$(ensure_leader "${bl%%:*}" "${bl##*:}" "写 B 之前"); [[ "$l" == "leader" ]] && okl=$((okl+1)); done
check "写之前：两个 B 分片的主都还在各自的 C 上" "$okl" "2"
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
for i in 0 1; do ins_into "${B_LINES[$i]%%:*}" 20001 150 b; done
caught=0; ident=0
for bl in "${B_LINES[@]}"; do
  sid=${bl%%:*}; c=${bl##*:}; shell="${BNAME_OF[$sid]}_${sid}"
  lp=$(PSQL $c -Atc "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($sid))" | tail -1)
  a=""; for t in $(seq 1 24); do a=$(PSQL $M -Atc "SELECT partdist.replay_catchup('$shell', $lp, 5000)" 2>/dev/null|tail -1); [[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && break; sleep 1; done
  [[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lp" ]] && caught=$((caught+1))
  PSQL $c -q -c "CHECKPOINT;" </dev/null >/dev/null; PSQL $M -q -c "CHECKPOINT;" </dev/null >/dev/null; sleep 1
  cdir=$(PSQL $c -Atc "SHOW data_directory" | tail -1)
  cpath=$(PSQL $c -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('${BNAME_OF[$sid]}_${sid}')" | tail -1)
  spath=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('$shell')" | tail -1)
  same=$(DEX python3 /tmp/pagecmp.py --kind=heap "$cdir/$cpath" "$bdir/$spath" </dev/null 2>/dev/null)
  echo "  B$sid: 追平 $a/$lp   逐字节=$same"
  [[ "$same" == "IDENTICAL_OUTSIDE_HOLE" ]] && ident=$((ident+1))
done
check "★ 两个 B 从都追平到各自主的位点" "$caught" "2"
check "★ 两个 B 从都与各自的主逐字节一致" "$ident" "2"
# 回放跑过之后路由层才认得出壳表；此刻再照一次"四重身份"
rrole_ok=0
for bl in "${B_LINES[@]}"; do sid=${bl%%:*}
  r=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve(${OID[$sid]}, 1)" | tail -1)
  [[ "$r" == "replica" ]] && rrole_ok=$((rrole_ok+1)); done
check "★ M 上 2 个分片是 replica（2 从并存，回放过后路由层认出）" "$rrole_ok" "2"
nrole2=0
for sid in "${A_SIDS[@]}"; do
  r=$(PSQL $M -Atc "SELECT role FROM partdist.route_resolve(${OID[$sid]}, 1)" | tail -1)
  [[ "$r" == "native_leader" ]] && nrole2=$((nrole2+1)); done
check "★ 同一时刻 M 上 2 个分片仍是 native_leader（2 主 + 2 从真并存）" "$nrole2" "2"

echo "========== [7] ★ 四套分片 xid 空间独立、按各自 OID 解析、可重号不串（规则 6） =========="
# ★ 这些表**不打标**，元组带的是原生 xid，"分片 xid 小号空间"的前提不成立
#   （实测原生水位 13101、各分片 xmin 也在一万上下，量级判据从前提上就错）。
#   真正能证明"四套空间互不串线"的是**解析路径**：2 个主的号走本机原生语义
#   （不在回放宇宙 ⇒ NONE），2 个从的号走回放两跳（xid_map→gclog ⇒ 拿得到判决）。
nat=$(PSQL $M -Atc "SELECT txid_current()" | tail -1)
declare -A FX
nres=0
for sid in "${A_SIDS[@]}"; do
  x=$(PSQL $M -Atc "SET citus.override_table_visibility=false; SELECT coalesce(min(xmin::text::bigint)::text,'NONE') FROM ${A_NAME_OF[$sid]}_${sid}" | tail -1); FX[$sid]=$x
  st=$(PSQL $M -Atc "SELECT coalesce(status,'NONE') FROM partdist.route_resolve(${OID[$sid]}, ${x:-1})" | tail -1)
  echo "  A分片 $sid(oid=${OID[$sid]}) xmin=$x ⇒ $st"
  [[ "$st" == "NONE" ]] && nres=$((nres+1))
done
for bl in "${B_LINES[@]}"; do sid=${bl%%:*}
  x=$(PSQL $M -Atc "SET citus.enable_ddl_propagation=off; SET pg_partdist.allow_replica_access=on; SELECT coalesce(min(xmin::text::bigint)::text,'NONE') FROM ${BNAME_OF[$sid]}_${sid}" | tail -1); FX[$sid]=$x
  st=$(PSQL $M -Atc "SELECT coalesce(status,'NONE') FROM partdist.route_resolve(${OID[$sid]}, ${x:-1})" | tail -1)
  echo "  B从   $sid(oid=${OID[$sid]}) xmin=$x ⇒ $st"
  # 判"走没走回放宇宙"看的是**非 NONE**：NONE = 这个关系根本不是壳表（走本机原生语义）；
  # 是壳表时状态要么是 gclog 判决（committed…），要么是 not_replayed（该号不在本壳表的
  # xid_map 里）—— 两者都说明走的是回放那条路。取 min(xmin) 挑到的是最老一条，
  # 不一定在 map 里，所以不能把 committed 写死成唯一期望。
  [[ -n "$st" && "$st" != "NONE" ]] && nres=$((nres+1))
done
echo "  本机原生 xid 水位=$nat"
check "★ 2 个主的号走本机原生语义(NONE)、2 个从的号走回放宇宙(非 NONE)" "$nres" "4"
echo "========== [8] 节点健康 =========="
health_check_no_crash
echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
if [[ "$FAIL" -eq 0 ]]; then echo "同一节点多主多从：全部通过"; else echo "同一节点多主多从：存在 FAIL"; fi
