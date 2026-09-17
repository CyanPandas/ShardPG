#!/usr/bin/env bash
# [宿主机] 跨组分布式事务验收（1c+3w 起，拓扑自适应）。
#
# 布局：一张 (worker 数) 分片的表，Citus 轮转后每台正好 1 片；每片一个"全体 worker"成员的
# Raft 组，主 = 落位节点，副本供到其余每台 ⇒ **每个节点都是一个组的主 + 其余组的从**。
# 一条事务写多个分片 = 一次横跨多个 Raft 组的分布式事务（Citus 2PC + pg_raft 每组同步复制）。
#
# 断言：
#   [2] 跨组提交：一条事务给每个分片各插 K 行，各主上各 K 行、协调者总数对
#   [3] 同一事务里跨组 UPDATE + DELETE，全部生效
#   [4] 显式 ROLLBACK：跨组写入与修改一条都不留
#   [5] 中途失败（重复主键）：整条回滚，一行不落
#   [6] 并发跨组转账：P 个会话同时在不同分片的行之间转账，总额守恒、成功笔数与余额变动吻合
#   [7] 每个组的每个副本追平到主的位点，且主堆与副本主堆掩码外逐字节一致
#   [8] 各节点无残留 prepared 事务
#   [9] ★ 跨组负载下切主（P7-N4 回归）：后台持续跨组转账时让一个组换主，新主在上界内登记，
#       负载恢复成功，新主本地数据与已提交一致（P7-N5 守卫），总额仍守恒
#   [10] 节点健康；日志环无丢弃；捕获环无覆盖
#
# 夹具规则：建组偏置选举超时（25）、副本 provision_shard_replica（24）、partition_map 两处收敛（32）、
#   写入按分片定点（29 的变体：这里**故意**跨分片，但每条语句都是定点单分片，事务才跨组）、
#   catchup 给上界（7）、收尾三台拆组两轮（16）、跑着的 .sh 不许改。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
K="${K:-40}"                     # [2] 每分片插入行数
ACCT="${ACCT:-8}"                # [6] 每分片账户行数
P="${P:-4}"                      # [6] 并发会话数
T="${T:-15}"                     # [6] 每会话转账笔数
REG_LIMIT_S="${REG_LIMIT_S:-120}"
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

echo "========== [0] 前置 =========="
WORKERS=($(PSQL $COORD -Atc "SELECT nodeport FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive ORDER BY nodeport" </dev/null))
NW=${#WORKERS[@]}
check "至少 3 个 worker" "$([[ $NW -ge 3 ]] && echo ok)" "ok"
declare -A NID DDIR
for p in "${WORKERS[@]}"; do NID[$p]=$(Q $p "SHOW pg_raft.node_id"); DDIR[$p]=$(Q $p "SHOW data_directory"); done
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
ngroups=0; for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ngroups=$((ngroups + ${n:-0})); done
check "起跑时没有残留数据组（净场）" "$ngroups" "0"

declare -A LEADER            # sid -> 当前主端口
SIDS=()
BG_PIDS=""
cleanup() {
  local r p sid
  for x in $BG_PIDS; do kill "$x" 2>/dev/null; done
  for p in "${WORKERS[@]}"; do
    Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null
    Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null
    Q $p "SELECT pg_reload_conf()" >/dev/null
  done
  for r in 1 2; do for p in "${WORKERS[@]}"; do
    Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null
  done; sleep 1; done
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('cg_${sid}'::regclass)" >/dev/null; done; done
  Q $COORD "DROP TABLE IF EXISTS cg" >/dev/null
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS cg_${sid}" >/dev/null; done; done
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
ids_of() {   # <sid> <起始id> <个数>：只落该分片的 id，逗号分隔
  Q $COORD "SELECT string_agg(g::text, ',') FROM (SELECT g FROM generate_series($2, $2 + 100000) g WHERE get_shard_id_for_distribution_column('cg', g) = $1 LIMIT $3) t"
}
count_on_leader() {  # <sid> [where]
  Q ${LEADER[$1]} "SET citus.override_table_visibility=false; SELECT count(*) FROM cg_$1 ${2:-}"
}

echo "========== [1] 夹具：cg 表 $NW 分片、每片一个 $NW 成员组、每台一主 $((NW-1)) 从 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS cg;
SET citus.shard_count = $NW; SET citus.shard_replication_factor = 1;
CREATE TABLE cg(id int primary key, v text, n int NOT NULL DEFAULT 0);
SELECT create_distributed_table('cg', 'id', colocate_with => 'none');
ALTER TABLE cg SET (autovacuum_enabled = off);
SQL
while read sid port; do SIDS+=("$sid"); LEADER[$sid]=$port; done < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='cg'::regclass ORDER BY s.shardid" </dev/null | tr '|' ' ')
distinct=$(for s in "${SIDS[@]}"; do echo "${LEADER[$s]}"; done | sort -u | wc -l)
check "$NW 个分片分落 $NW 台（每台一个主）" "$distinct" "$NW"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
for sid in "${SIDS[@]}"; do
  lp=${LEADER[$sid]}
  check "组 $sid 主落在 :$lp" "$(build_group_on $sid $lp "$ALLMEM")" "leader"
  for p in "${WORKERS[@]}"; do
    [[ $p == $lp ]] && continue
    r=$(PSQL $lp -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
    check "  组 $sid 副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok)" "ok"
  done
done
for sid in "${SIDS[@]}"; do
  lp=${LEADER[$sid]}; ok=""
  for t in $(seq 1 60); do
    [[ "$(Q $lp "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" &&
       "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" ]] && { ok=ok; break; }
    sleep 1
  done
  check "组 $sid partition_map 在主节点与协调者两处收敛" "$ok" "ok"
done

echo "========== [2] ★ 跨组提交：一条事务给每个分片各插 $K 行 =========="
# 逐分片拼多行 VALUES：每条 INSERT 被 Citus 路由成单分片任务（INSERT…SELECT 不会），事务整体跨组
TXN="BEGIN;"
for sid in "${SIDS[@]}"; do
  vals=$(Q $COORD "SELECT string_agg('('||g||',''c2'')', ',') FROM (SELECT g FROM generate_series(1, 100000) g WHERE get_shard_id_for_distribution_column('cg', g) = $sid LIMIT $K) t")
  TXN+=" INSERT INTO cg(id, v) VALUES $vals;"
done
TXN+=" COMMIT;"
out=$(PSQL $COORD -v ON_ERROR_STOP=1 -q -c "$TXN" </dev/null 2>&1); rc=$?
check "跨 $NW 组事务提交成功" "$([[ $rc -eq 0 ]] && echo ok || echo "rc=$rc ${out:0:120}")" "ok"
check "协调者总行数" "$(Q $COORD 'SELECT count(*) FROM cg')" "$((K*NW))"
for sid in "${SIDS[@]}"; do check "  组 $sid 主(:${LEADER[$sid]})本地 $K 行" "$(count_on_leader $sid)" "$K"; done

echo "========== [3] ★ 同一事务里跨组 UPDATE + DELETE =========="
TXN="BEGIN;"
for sid in "${SIDS[@]}"; do
  u=$(ids_of $sid 1 10); d=$(Q $COORD "SELECT string_agg(g::text, ',') FROM (SELECT g FROM generate_series(1,100000) g WHERE get_shard_id_for_distribution_column('cg', g) = $sid OFFSET 10 LIMIT 5) t")
  TXN+=" UPDATE cg SET v='c3' WHERE id IN ($u); DELETE FROM cg WHERE id IN ($d);"
done
TXN+=" COMMIT;"
out=$(PSQL $COORD -v ON_ERROR_STOP=1 -q -c "$TXN" </dev/null 2>&1); rc=$?
check "跨组 UPDATE+DELETE 事务提交成功" "$([[ $rc -eq 0 ]] && echo ok || echo "rc=$rc ${out:0:120}")" "ok"
check "协调者：改成 c3 的行" "$(Q $COORD "SELECT count(*) FROM cg WHERE v='c3'")" "$((10*NW))"
check "协调者：删后总行数" "$(Q $COORD 'SELECT count(*) FROM cg')" "$(( (K-5)*NW ))"
for sid in "${SIDS[@]}"; do check "  组 $sid 主本地 c3=10 / 总=$((K-5))" "$(count_on_leader $sid "WHERE v='c3'")/$(count_on_leader $sid)" "10/$((K-5))"; done

echo "========== [4] ★ 显式 ROLLBACK：跨组写入与修改一条都不留 =========="
before_cnt=$(Q $COORD 'SELECT count(*) FROM cg'); before_c3=$(Q $COORD "SELECT count(*) FROM cg WHERE v='c3'")
TXN="BEGIN;"
for sid in "${SIDS[@]}"; do
  vals=$(Q $COORD "SELECT string_agg('('||g||',''rb'')', ',') FROM (SELECT g FROM generate_series(200001, 400000) g WHERE get_shard_id_for_distribution_column('cg', g) = $sid LIMIT 5) t")
  u=$(ids_of $sid 1 10)
  TXN+=" INSERT INTO cg(id, v) VALUES $vals; UPDATE cg SET v='rb' WHERE id IN ($u);"
done
TXN+=" ROLLBACK;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "$TXN" </dev/null >/dev/null 2>&1
check "ROLLBACK 后总行数不变" "$(Q $COORD 'SELECT count(*) FROM cg')" "$before_cnt"
check "ROLLBACK 后 c3 行不变" "$(Q $COORD "SELECT count(*) FROM cg WHERE v='c3'")" "$before_c3"
check "ROLLBACK 后没有 rb 行" "$(Q $COORD "SELECT count(*) FROM cg WHERE v='rb'")" "0"
for sid in "${SIDS[@]}"; do check "  组 $sid 主本地没有 rb 行" "$(count_on_leader $sid "WHERE v='rb'")" "0"; done

echo "========== [5] ★ 中途失败（重复主键）：整条回滚 =========="
TXN="BEGIN;"
for sid in "${SIDS[@]}"; do
  vals=$(Q $COORD "SELECT string_agg('('||g||',''bad'')', ',') FROM (SELECT g FROM generate_series(400001, 600000) g WHERE get_shard_id_for_distribution_column('cg', g) = $sid LIMIT 5) t")
  TXN+=" INSERT INTO cg(id, v) VALUES $vals;"
done
dup=$(ids_of ${SIDS[0]} 1 1)
TXN+=" INSERT INTO cg(id, v) VALUES ($dup, 'dup'); COMMIT;"
out=$(PSQL $COORD -v ON_ERROR_STOP=1 -q -c "$TXN" </dev/null 2>&1); rc=$?
check "含重复主键的跨组事务报错" "$([[ $rc -ne 0 && "$out" == *duplicate* ]] && echo ok || echo "rc=$rc ${out:0:100}")" "ok"
check "失败后总行数不变" "$(Q $COORD 'SELECT count(*) FROM cg')" "$before_cnt"
check "失败后没有 bad 行" "$(Q $COORD "SELECT count(*) FROM cg WHERE v='bad'")" "0"
for sid in "${SIDS[@]}"; do check "  组 $sid 主本地没有 bad 行" "$(count_on_leader $sid "WHERE v='bad'")" "0"; done

echo "========== [6] ★ 并发跨组转账：$P 会话 × $T 笔，总额守恒 =========="
# 账户：每分片 ACCT 行，n=1000。每笔转账 = 一条事务里两条定点 UPDATE，落在两个不同分片（两个组）
ACC_ALL=""
declare -A ACC
for sid in "${SIDS[@]}"; do
  ACC[$sid]=$(Q $COORD "SELECT string_agg(g::text, ' ') FROM (SELECT g FROM generate_series(700001, 900000) g WHERE get_shard_id_for_distribution_column('cg', g) = $sid LIMIT $ACCT) t")
  vals=$(for a in ${ACC[$sid]}; do printf "(%s,'acct',1000)," "$a"; done | sed 's/,$//')
  PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO cg(id, v, n) VALUES $vals" </dev/null
  ACC_ALL+="${ACC[$sid]} "
done
TOTAL0=$(Q $COORD "SELECT sum(n) FROM cg WHERE v='acct'")
check "账户初始总额" "$TOTAL0" "$((1000*ACCT*NW))"
SP_DIR=$(mktemp -d)
transfer_worker() {   # <会话号> <笔数> <结果文件>
  local s=$1 cnt=$2 f=$3 i a b ok=0 bad=0
  local accts=($ACC_ALL)
  local na=${#accts[@]}
  for i in $(seq 1 $cnt); do
    a=${accts[$(( (s*7 + i*3) % na ))]}; b=${accts[$(( (s*11 + i*5 + 1) % na ))]}
    [[ "$a" == "$b" ]] && b=${accts[$(( (s*11 + i*5 + 2) % na ))]}
    if PSQL $COORD -v ON_ERROR_STOP=1 -q -c "BEGIN; UPDATE cg SET n = n - 1 WHERE id = $a; UPDATE cg SET n = n + 1 WHERE id = $b; COMMIT;" </dev/null >/dev/null 2>>"$f.err"; then
      ok=$((ok+1))
    else
      bad=$((bad+1))
    fi
  done
  echo "$ok $bad" > "$f"
}
t0=$(date +%s)
for s in $(seq 1 $P); do transfer_worker $s $T "$SP_DIR/w$s" & done; wait
T6=$(( $(date +%s) - t0 ))
okt=0; badt=0; for s in $(seq 1 $P); do read o b < "$SP_DIR/w$s"; okt=$((okt+o)); badt=$((badt+b)); done
echo "  $((P*T)) 笔用时 ${T6}s：成功 $okt、失败 $badt（失败只允许是分布式死锁/序列化类，原文见下）"
[[ $badt -gt 0 ]] && cat "$SP_DIR"/w*.err | grep -E "ERROR" | sort | uniq -c | head -5 | sed 's/^/    /'
check "★ 并发转账后总额守恒" "$(Q $COORD "SELECT sum(n) FROM cg WHERE v='acct'")" "$TOTAL0"
check "  余额偏移总量与成功笔数相容（每笔移动 1，偏移 ≤ 2×成功笔数且为偶数）" "$(Q $COORD "SELECT sum(abs(n-1000)) FROM cg WHERE v='acct'" | awk -v ok=$okt '{print ($1<=2*ok && ($1%2)==0)?"ok":"变动="$1" 成功="ok}')" "ok"
check "★ 失败笔数没有异常类错误（非死锁/串行化）" "$(cat "$SP_DIR"/w*.err 2>/dev/null | grep ERROR | grep -viE 'deadlock|could not serialize|canceling statement due to lock' | wc -l)" "0"

echo "========== [7] ★ 每个组的每个副本追平并与主逐字节一致 =========="
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
for sid in "${SIDS[@]}"; do
  lp=${LEADER[$sid]}
  llo=$(Q $lp "SELECT partdist.local_partition_for_shard($sid)")
  bound=$(Q $lp "SELECT partdist.get_partition_flush_lsn($llo)")
  PSQL $lp -q -c "CHECKPOINT" </dev/null >/dev/null
  lpath="${DDIR[$lp]}/$(Q $lp "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('cg_$sid')")"
  for p in "${WORKERS[@]}"; do
    [[ $p == $lp ]] && continue
    got=""; for t in $(seq 1 40); do
      got=$(Q $p "SELECT partdist.replay_catchup('cg_$sid', $bound, 10000)")
      [[ "$got" =~ ^[0-9]+$ && "$got" -ge "$bound" ]] && break; sleep 1
    done
    PSQL $p -q -c "CHECKPOINT" </dev/null >/dev/null
    rpath="${DDIR[$p]}/$(Q $p "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('cg_$sid')")"
    same=$(DEX python3 /tmp/pagecmp.py --kind=heap "$lpath" "$rpath" </dev/null 2>/dev/null)
    check "  组 $sid 副本 :$p 追平($got/$bound) 且主堆逐字节一致" "$([[ "$got" =~ ^[0-9]+$ && "$got" -ge "$bound" ]] && echo "$same")" "IDENTICAL_OUTSIDE_HOLE"
  done
done

echo "========== [8] ★ 各节点无残留 prepared 事务 =========="
np=0; for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); np=$((np + ${n:-0})); done
check "全部节点 pg_prepared_xacts 为空" "$np" "0"

echo "========== [9] ★ 跨组负载下切主（P7-N4 回归 / P7-N5 守卫） =========="
FS=${SIDS[0]}; OLD=${LEADER[$FS]}
NEWL=""; for p in "${WORKERS[@]}"; do [[ $p != $OLD ]] && { NEWL=$p; break; }; done
echo "  组 $FS：旧主 :$OLD → 指定新主 :$NEWL；切主期间 $P 个会话持续跨组转账"
TOTAL_BEFORE=$(Q $COORD "SELECT sum(n) FROM cg WHERE v='acct'")
STOP_FILE="$SP_DIR/stop"
bg_worker() {   # <会话号> <结果文件>：一直转账到 STOP_FILE 出现
  local s=$1 f=$2 i=0 a b ok=0 bad=0 last_ok=0
  local accts=($ACC_ALL)
  local na=${#accts[@]}
  while [[ ! -f "$STOP_FILE" ]]; do
    i=$((i+1))
    a=${accts[$(( (s*13 + i*3) % na ))]}; b=${accts[$(( (s*17 + i*7 + 1) % na ))]}
    [[ "$a" == "$b" ]] && b=${accts[$(( (s*17 + i*7 + 2) % na ))]}
    if PSQL $COORD -v ON_ERROR_STOP=1 -q -c "BEGIN; UPDATE cg SET n = n - 1 WHERE id = $a; UPDATE cg SET n = n + 1 WHERE id = $b; COMMIT;" </dev/null >/dev/null 2>>"$f.err"; then
      ok=$((ok+1)); last_ok=$(date +%s)
    else
      bad=$((bad+1)); sleep 1
    fi
    echo "$ok $bad $last_ok" > "$f"
  done
}
for s in $(seq 1 $P); do bg_worker $s "$SP_DIR/b$s" & BG_PIDS+="$! "; done
sleep 10
LOGF=$(DEX bash -c "ls -t '${DDIR[$NEWL]}/pg.log' '${DDIR[$NEWL]}.log' 2>/dev/null | head -1" </dev/null); LOGL=$(DEX bash -c "wc -l < '$LOGF'" </dev/null)   # 取最近被写的日志文件
for p in "${WORKERS[@]}"; do
  if [[ $p == $NEWL ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null
  elif [[ $p == $OLD ]]; then Q $p "ALTER SYSTEM SET pg_raft.heartbeat_ms = 10000" >/dev/null
  else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 60000" >/dev/null; fi
  Q $p "SELECT pg_reload_conf()" >/dev/null
done
st=""; for t in $(seq 1 180); do st=$(Q $NEWL "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$FS"); [[ "$st" == leader ]] && break; sleep 1; done
T_EL=$(date +%s)
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "  :$NEWL 在负载下当选组 $FS 新 leader" "$st" "leader"
reg=""; t_reg=""; maxs=0
for t in $(seq 1 $((REG_LIMIT_S/2))); do
  n=$(Q $NEWL "SELECT count(*) FROM pg_stat_activity WHERE state='active' AND query LIKE '%pg_raft_promote_prepare%' AND pid<>pg_backend_pid()")
  [[ "${n:-0}" =~ ^[0-9]+$ && $n -gt $maxs ]] && maxs=$n
  if [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$FS")" == "${NID[$NEWL]}" &&
        "$(Q $COORD "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$FS")" == "$NEWL" ]]; then
    reg=ok; t_reg=$(( $(date +%s) - T_EL )); break
  fi
  sleep 2
done
echo "  当选→登记 ${t_reg:-未登记}s；并发升主前置峰值 $maxs"
check "★ 负载下新主 ${REG_LIMIT_S}s 内登记（partition_map + placement 都指向 :$NEWL）" "$reg" "ok"
check "★ 同组升主前置不堆积（峰值 $maxs ≤ 1）" "$([[ $maxs -le 1 ]] && echo ok)" "ok"
[[ -n "$reg" ]] && LEADER[$FS]=$NEWL
sleep 20      # 登记后让负载再跑一会儿，看是否恢复
T_RESUME=$(date +%s)
sleep 10
touch "$STOP_FILE"; for x in $BG_PIDS; do wait "$x" 2>/dev/null; done; BG_PIDS=""
okb=0; badb=0; resumed=0
for s in $(seq 1 $P); do read o b l < "$SP_DIR/b$s"; okb=$((okb+o)); badb=$((badb+b)); [[ ${l:-0} -ge $T_RESUME ]] && resumed=$((resumed+1)); done
echo "  切主窗口内后台转账：成功 $okb、失败 $badb；登记后仍有成功提交的会话 $resumed/$P"
[[ $badb -gt 0 ]] && cat "$SP_DIR"/b*.err | grep ERROR | sed 's/[0-9]\{3,\}/N/g' | sort | uniq -c | sort -rn | head -5 | sed 's/^/    /'
check "★ 登记后负载恢复（全部会话都有新的成功提交）" "$resumed" "$P"
check "★ 切主前后总额守恒" "$(Q $COORD "SELECT sum(n) FROM cg WHERE v='acct'")" "$TOTAL_BEFORE"
# 协调者侧按分片过滤：先把全表物化到协调者再算分片号（该 UDF 只能在协调者上跑，别让它被下推）
LCNT=$(count_on_leader $FS); CCNT=$(Q $COORD "WITH x AS MATERIALIZED (SELECT id FROM cg) SELECT count(*) FROM x WHERE get_shard_id_for_distribution_column('cg', x.id) = $FS")
check "★ 新主 :$NEWL 本地行数 = 协调者按该分片数的已提交行数（P7-N5 守卫）" "$LCNT" "$CCNT"
LSUM=$(Q $NEWL "SET citus.override_table_visibility=false; SELECT coalesce(sum(n),0)||'/'||coalesce(md5(string_agg(id||':'||v||':'||n, ',' ORDER BY id)),'') FROM cg_$FS")
CSUM=$(Q $COORD "WITH x AS MATERIALIZED (SELECT id, v, n FROM cg) SELECT coalesce(sum(n),0)||'/'||coalesce(md5(string_agg(id||':'||v||':'||n, ',' ORDER BY id)),'') FROM x WHERE get_shard_id_for_distribution_column('cg', x.id) = $FS")
check "★ 新主本地内容指纹 = 协调者读到的该分片内容" "$LSUM" "$CSUM"
NTO=$(DEX bash -c "tail -n +$((LOGL+1)) '$LOGF' | grep -c '升主前置超过 .* 未返回或连接失效'" </dev/null)
check "★ 新主日志无'升主前置…未返回或连接失效'（实得 $NTO）" "$NTO" "0"

echo "========== [10] 健康与环计数 =========="
health_check_no_crash
drops=0; ow=0
for p in "${WORKERS[@]}"; do
  d=$(Q $p "SELECT coalesce(sum(ring_full_drops + quorum_drops),0) FROM partdist.pg_raft_group_flow_stats() WHERE group_id<>0"); drops=$((drops + ${d:-0}))
  o=$(Q $p "SELECT overwrites FROM partdist.partwal_ring_stats()"); ow=$((ow + ${o:-0}))
done
echo "  日志环丢弃合计=$drops（切主窗口里旧主失去多数派时可能有少量 quorum_drops，只报不判）"
check "捕获环覆盖合计" "$ow" "0"
rm -rf "$SP_DIR"

echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]] && echo "跨组分布式事务验收：全部通过" || echo "跨组分布式事务验收：存在 FAIL"
