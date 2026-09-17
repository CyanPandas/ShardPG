#!/usr/bin/env bash
# [宿主机] P7-N4 复现 / 回归：切主后新 leader 必须在有界时间内向控制面登记成功。
#
# 缺陷（2026-09-17 跨组事务并发试跑抓到）：新 leader 在 BGW tick 里调升主前置
# partdist.pg_raft_promote_prepare()，tick 只等 slice+8000=10 s。升主前置在长流上要跑几十秒
# （实测 69 s：dtx_close_indoubt 逐 plsn 读流是 O(n²)，分叉检查从尾部往回 200 条同样每条 O(n)），
# 于是：①服务端每次都跑完、结果被 tick 丢掉，下一轮从头再来 —— 永远登记不上；②超时路径不计
# 截止期，60 s 兜底永不触发；③超时只 PQfinish 不 PQcancel，同组升主前置会话越堆越多；④下一轮
# 阻塞式重连期间一个心跳都不发，把本节点领导的其他组也拖下马。
#
# 本用例不靠负载碰运气，而是**确定性地**制造一次切主：
#   先在一个 1 分片、3 成员组上写出足够长的分区流（默认约 1200 条记录），
#   再把旧主的 heartbeat_ms 抬到 10 s、指定 follower 的 election_timeout 压到 1.5 s，
#   让它必然当选；随后复原参数，观察新主能否登记、登记要多久、有没有堆积和超时告警。
#
# 夹具规则：建组偏置选举超时（25）、副本一律 provision_shard_replica（24）、
#   等 partition_map 在主节点+协调者两处收敛（32）、收尾三台拆组两轮（16）。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
N_ROWS="${N_ROWS:-1400}"           # 实测每行约 1.1 条记录；1400 行 ≈ 1600 条，足以让修复前的升主前置跑过 10 s
REG_LIMIT_S="${REG_LIMIT_S:-150}"  # 登记上界
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
Q()    { PSQL "$1" -Atc "$2" </dev/null 2>/dev/null | tail -1; }

check() {
  if [[ -z "$2" ]]; then echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置：拓扑 =========="
WORKERS=($(PSQL $COORD -Atc "SELECT nodeport FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive ORDER BY nodeport" </dev/null))
check "至少 3 个 worker" "$([[ ${#WORKERS[@]} -ge 3 ]] && echo ok)" "ok"
declare -A NID DDIR
for p in "${WORKERS[@]}"; do
  NID[$p]=$(Q $p "SHOW pg_raft.node_id")
  DDIR[$p]=$(Q $p "SHOW data_directory")
done
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
M=${WORKERS[0]}; F=${WORKERS[1]}; O=${WORKERS[2]}   # 旧主 / 指定新主 / 另一 follower
echo "  旧主 M=:$M(node${NID[$M]})  指定新主 F=:$F(node${NID[$F]})  另一 follower O=:$O(node${NID[$O]})"

SID=""
cleanup() {
  local r p
  for p in "${WORKERS[@]}"; do
    Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null
    Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null
    Q $p "SELECT pg_reload_conf()" >/dev/null
  done
  for r in 1 2; do for p in "${WORKERS[@]}"; do
    Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null
  done; sleep 1; done
  if [[ -n "$SID" ]]; then
    for p in "${WORKERS[@]}"; do
      Q $p "SELECT partdist.replay_disable('pl_${SID}'::regclass)" >/dev/null
    done
  fi
  Q $COORD "DROP TABLE IF EXISTS pl" >/dev/null
  if [[ -n "$SID" ]]; then
    for p in "${WORKERS[@]}"; do Q $p "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS pl_${SID}" >/dev/null; done
  fi
  echo "  [复原] 组已拆、表已删、GUC 已 RESET"
}
trap cleanup EXIT

build_group_on() {   # <gid> <leader端口> <成员>（规则 25）
  local gid=$1 want=$2 mem=$3 t p st=""
  for p in "${WORKERS[@]}"; do
    if [[ "$p" == "$want" ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null
    else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 9000" >/dev/null; fi
    Q $p "SELECT pg_reload_conf()" >/dev/null
  done
  for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.pg_raft_group_create($gid, $mem)" >/dev/null; done
  for t in $(seq 1 40); do st=$(Q $want "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid"); [[ "$st" == leader ]] && break; sleep 1; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  echo "$st"
}

echo "========== [1] 夹具：1 分片表（必落 :$M），3 成员组，主=:$M，副本供到另两台 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL' >/dev/null
DROP TABLE IF EXISTS pl;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE pl(id int primary key, v text);
SELECT create_distributed_table('pl','id');
ALTER TABLE pl SET (autovacuum_enabled=off);
SQL
SID=$(Q $COORD "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='pl'::regclass")
PL=$(Q $COORD "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$SID")
check "单分片表落在 :$M" "$PL" "$M"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 :$M" "$(build_group_on $SID $M "$ALLMEM")" "leader"
for p in $F $O; do
  r=$(PSQL $M -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok)" "ok"
done
pm=""; for t in $(seq 1 60); do
  pm=$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$SID")
  [[ "$pm" == "${NID[$M]}" && "$(Q $M "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$SID")" == "${NID[$M]}" ]] && break; sleep 1
done
check "partition_map 在协调者上登记为 :$M" "$pm" "${NID[$M]}"

echo "========== [2] 写出长分区流（$N_ROWS 行，分批单分片写） =========="
t0=$(date +%s); b=0
while [[ $b -lt $N_ROWS ]]; do
  e=$((b+50)); [[ $e -gt $N_ROWS ]] && e=$N_ROWS
  PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO pl SELECT g, repeat('p',80) FROM generate_series($((b+1)),$e) g" </dev/null || break
  b=$e
done
LOID_M=$(Q $M "SELECT partdist.local_partition_for_shard($SID)")
TIP=$(Q $M "SELECT partdist.get_partition_flush_lsn($LOID_M)")
echo "  写入耗时 $(( $(date +%s)-t0 ))s；旧主分区流长度 plsn=$TIP"
check "写完 $N_ROWS 行" "$(Q $COORD 'SELECT count(*) FROM pl')" "$N_ROWS"
check "分区流足够长（≥ $N_ROWS 条）" "$([[ "${TIP:-0}" -ge $N_ROWS ]] && echo ok || echo "只有 ${TIP:-0}")" "ok"

echo "========== [2b] ★ 读流稀疏索引与全扫描逐条一致（P7-N4/N7 的成本面修复） =========="
# 同一条流：索引开着按升序 / 降序 / 乱序各读一遍全部记录，关掉索引（原全扫描）抽样读，
# 逐条指纹必须相同。每条记录的指纹 = plsn + 全部头部字段 + 载荷 md5。
RECFP="g||':'||coalesce(r.orig_lsn::text,'-')||':'||coalesce(r.rmid,-1)||':'||coalesce(r.info,-1)||':'||coalesce(r.flags,-1)||':'||coalesce(r.gxid,-1)||':'||coalesce(md5(r.data),'-')"
fp_run() {   # <SET 语句> <plsn 序列 SQL>：输出 "耗时ms 指纹"（按 plsn 排序聚合，与读取顺序无关）
  PSQL $M -At </dev/null 2>/dev/null <<SQL | tail -1
$1
\timing off
SELECT extract(epoch from clock_timestamp())::numeric(20,3) AS t0 \gset
SELECT md5(string_agg(fp, ',' ORDER BY g)) AS h FROM (SELECT g, $RECFP AS fp FROM ($2) s(g) LEFT JOIN LATERAL partdist.partwal_read_record($LOID_M, g) r ON true) x \gset
SELECT round((extract(epoch from clock_timestamp()) - :t0) * 1000) || ' ' || :'h';
SQL
}
SEQ_ASC="SELECT g FROM generate_series(1, $TIP) g"
SEQ_DESC="SELECT g FROM generate_series($TIP, 1, -1) g"
SEQ_RAND="SELECT g FROM generate_series(1, $TIP) g ORDER BY md5(g::text)"
SEQ_SAMPLE="SELECT g FROM generate_series(1, $TIP) g WHERE g % 97 = 1 OR g > $TIP - 20 OR g <= 20"
read t_asc h_asc   <<<"$(fp_run "SET pg_partdist.partwal_record_index = on;" "$SEQ_ASC")"
read t_desc h_desc <<<"$(fp_run "SET pg_partdist.partwal_record_index = on;" "$SEQ_DESC")"
read t_rand h_rand <<<"$(fp_run "SET pg_partdist.partwal_record_index = on;" "$SEQ_RAND")"
read t_on_s h_on_s   <<<"$(fp_run "SET pg_partdist.partwal_record_index = on;" "$SEQ_SAMPLE")"
read t_off_s h_off_s <<<"$(fp_run "SET pg_partdist.partwal_record_index = off;" "$SEQ_SAMPLE")"
echo "  全部 $TIP 条（索引开）：升序 ${t_asc}ms  降序 ${t_desc}ms  乱序 ${t_rand}ms"
echo "  抽样 $(Q $M "SELECT count(*) FROM ($SEQ_SAMPLE) s")条：索引开 ${t_on_s}ms  索引关（原全扫描）${t_off_s}ms"
check "★ 索引开：升序 / 降序 / 乱序读出的全部记录指纹相同" "$([[ -n "$h_asc" && "$h_asc" == "$h_desc" && "$h_asc" == "$h_rand" ]] && echo ok)" "ok"
check "★ 抽样记录：索引开 与 原全扫描 指纹相同" "$([[ -n "$h_on_s" && "$h_on_s" == "$h_off_s" ]] && echo ok)" "ok"
check "  指纹不是空流（确有记录被读出）" "$(Q $M "SELECT count(*) FROM generate_series(1, $TIP) g, LATERAL partdist.partwal_read_record($LOID_M, g) r WHERE r.data IS NOT NULL OR r.flags IS NOT NULL")" "$TIP"

echo "========== [3] 确定性切主：旧主停发心跳，指定 :$F 当选 =========="
# 节点日志文件因启动方式而异（lib_topology 用 <数据目录>/pg.log，pg_ctl -l 常见 <数据目录>.log）：
# 取两者中最近被写的那个，否则日志断言会读到陈旧文件、恒为 0 条（09-17 实测踩到）。
LOGF=$(DEX bash -c "ls -t '${DDIR[$F]}/pg.log' '${DDIR[$F]}.log' 2>/dev/null | head -1" </dev/null)
echo "  新主日志文件：$LOGF"
LOGLINE_F=$(DEX bash -c "wc -l < '$LOGF'" </dev/null)
Q $F "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null
# 另一台抬到上限 60 s：只有 F 会发起选举。F 的后台进程可能正忙着应用刚收到的记录，
# 一时顾不上查选举超时（09-17 第一次跑实测 :5434 一次都没发起、反倒是 5 s 的那台当选），所以要排除竞争者。
Q $O "ALTER SYSTEM SET pg_raft.election_timeout_ms = 60000" >/dev/null
Q $M "ALTER SYSTEM SET pg_raft.heartbeat_ms = 10000" >/dev/null
for p in "${WORKERS[@]}"; do Q $p "SELECT pg_reload_conf()" >/dev/null; done
st=""; for t in $(seq 1 180); do st=$(Q $F "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$SID"); [[ "$st" == leader ]] && break; sleep 1; done
T_ELECT=$(date +%s)
for p in "${WORKERS[@]}"; do
  Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null
  Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null
  Q $p "SELECT pg_reload_conf()" >/dev/null
done
check ":$F 当选组 $SID 的新 leader" "$st" "leader"

echo "========== [4] ★ 新主在 ${REG_LIMIT_S}s 内登记；期间无超时告警、升主前置不堆积 =========="
reg=""; maxsess=0; t_reg=""
for t in $(seq 1 $((REG_LIMIT_S/2))); do
  n=$(Q $F "SELECT count(*) FROM pg_stat_activity WHERE state='active' AND query LIKE '%pg_raft_promote_prepare%' AND pid<>pg_backend_pid()")
  [[ "${n:-0}" =~ ^[0-9]+$ && "$n" -gt "$maxsess" ]] && maxsess=$n
  pm=$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$SID")
  pp=$(Q $COORD "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$SID")
  if [[ "$pm" == "${NID[$F]}" && "$pp" == "$F" ]]; then reg=ok; t_reg=$(( $(date +%s) - T_ELECT )); break; fi
  sleep 2
done
echo "  当选→登记 ${t_reg:-未登记}s；采样到的并发升主前置会话峰值=$maxsess"
check "★ 新主在 ${REG_LIMIT_S}s 内完成登记（partition_map 与 placement 都指向 :$F）" "$reg" "ok"
NTIMEOUT=$(DEX bash -c "tail -n +$((LOGLINE_F+1)) '$LOGF' | grep -c '升主前置超过 .* 未返回或连接失效(组 $SID)'" </dev/null)
check "★ 期间没有'升主前置超过…未返回或连接失效'告警（实得 $NTIMEOUT 条）" "$NTIMEOUT" "0"
check "★ 同组升主前置会话不堆积（采样峰值 $maxsess ≤ 1）" "$([[ "$maxsess" -le 1 ]] && echo ok)" "ok"
echo "  新主日志里本组的升主相关行："
DEX bash -c "tail -n +$((LOGLINE_F+1)) '$LOGF' | grep -E 'group $SID .*(当选|登记)|升主前置.*$SID|组 $SID .*(升主|放行)' | cut -c1-200 | head -8" </dev/null | sed 's/^/    /'

echo "========== [5] 切主后写入与数据完整性 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO pl SELECT g, 'after' FROM generate_series($((N_ROWS+1)),$((N_ROWS+10))) g" </dev/null
check "切主后经协调者写 10 行成功，总行数" "$(Q $COORD 'SELECT count(*) FROM pl')" "$((N_ROWS+10))"
check "★ 新主本地分片表行数与已提交行数一致" "$(Q $F "SET citus.override_table_visibility=false; SELECT count(*) FROM pl_$SID")" "$((N_ROWS+10))"
check "★ 新主本地 'after' 行都在" "$(Q $F "SET citus.override_table_visibility=false; SELECT count(*) FROM pl_$SID WHERE v='after'")" "10"

echo "========== [6] 节点健康 =========="
health_check_no_crash

echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]] && echo "P7-N4 切主登记：全部通过" || echo "P7-N4 切主登记：存在 FAIL"
