#!/usr/bin/env bash
# ============================================================================
# [宿主机] P7-N92 回归：快照读遇到"正在提交"的分片事务要等它落账，不能先当未提交、后又看见
#
# 缺陷：单分片事务在 PRE_COMMIT 先取 commit_ts，再等复制确认（RF=3 约 0.8 秒）才落账。
#       这段时间里开始的快照（start_ts 更大）第一次读当它未提交，落账后同一快照再读
#       就按 commit_ts < start_ts 看见它 —— 开着的事务里两次 count(*) 结果不同。
#       实测（事务演示 2.5）：持续单条 INSERT 时 15 次有 9 次多出 1 行。
# 修复：取 commit_ts 之前把活跃表条目标成"提交中"；快照读遇到提交中、commit_ts 未知或
#       早于本快照的，等它落账再判（上限 pg_partdist.commit_wait_timeout，超时报 40001）。
#
# 用 pg_partdist.commit_debug_delay_ms 把"取到 commit_ts → 落账"拉长到几秒，让窗口可控：
#   1 正向：写方取号后停 4 秒，读方此时开快照 —— 第一次读就等到落账、两次读一致
#   2 对照：同样的时序，读方把等待关掉（commit_wait_timeout = 0）—— 旧问题必须复现，
#           证明本用例分辨得出有没有修
#   3 不该等的不等：读方先开快照、写方后取号（commit_ts 更大）—— 读立即返回、始终看不到
#   4 超时：写方停 6 秒，读方只肯等 1 秒 —— 报 40001，不给不一致的结果
#   5 自然并发：经 master 持续单条 INSERT（不加延迟），开着的事务里两次读 8 轮全一致
#
# 前提：pg_partdist 含 P7-N92（整簇重启后生效）、SHARD KEY 建表（P7-N87）、TSO 已开。
# 用法：CONTAINER=pg-test-container bash test_snapshot_commit_wait_p7n92.sh
#       （从 scratchpad 副本跑，见夹具规则 43）
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
PASS=0; FAIL=0
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

q()    { docker exec -u postgres "$C" bash -c "exec $BIN/psql -h /tmp -p ${2:-5432} -U postgres -d postgres -X -qAt -c \"\$0\" 2>&1" "$1" </dev/null; }
runf() { docker exec -i -u postgres "$C" bash -c "exec $BIN/psql -h /tmp -p ${1:-5432} -U postgres -d postgres -X -q 2>&1"; }

ok()   { PASS=$((PASS + 1)); printf '  PASS  %s\n' "$1"; }
bad()  { FAIL=$((FAIL + 1)); printf '  FAIL  %s\n' "$1"; [[ -n "${2:-}" ]] && printf '%s\n' "$2" | sed 's/^/        | /'; }
expect_eq()  { [[ "$2" == "$3" ]] && ok "$1（= $3）" || bad "$1：期望 [$3]，实际 [$2]"; }
expect_has() { grep -qF -- "$3" <<<"$2" && ok "$1" || bad "$1：输出里没有 [$3]" "$2"; }
expect_lt()  { [[ "$2" =~ ^[0-9]+$ && "$2" -lt "$3" ]] && ok "$1（$2 < $3）" || bad "$1：期望 < $3，实际 [$2]"; }
expect_ge()  { [[ "$2" =~ ^[0-9]+$ && "$2" -ge "$3" ]] && ok "$1（$2 ≥ $3）" || bad "$1：期望 ≥ $3，实际 [$2]"; }
step() { printf '\n[%s] %s\n' "$1" "$2"; }
field() { grep -o "$1=[^ ]*" <<<"$2" | head -1 | cut -d= -f2; }   # 从 "a=1 b=2" 里取值
now_ms() { date +%s%3N; }

# 直接在分片主节点上单条插入（自动提交），取到 commit_ts 后停 $2 毫秒
w_insert() {
    docker exec -u postgres -e "PGOPTIONS=-c pg_partdist.commit_debug_delay_ms=$2" "$C" \
        $BIN/psql -h /tmp -p "$PRIM" -U postgres -d postgres -X -qAt \
        -c "INSERT INTO $SHARD VALUES ($1, $1)" </dev/null 2>&1
}

WORKERS=$(q "SELECT string_agg(nodeport::text, ' ' ORDER BY nodeport) FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive")

# ---------------------------------------------------------------------------
step 0 "前置：参数已注册、TSO、清场"
for p in $WORKERS; do
    expect_eq ":$p 等待上限默认 10 秒" "$(q 'SHOW pg_partdist.commit_wait_timeout' "$p")" "10s"
    expect_eq ":$p 调试延迟默认 0" "$(q 'SHOW pg_partdist.commit_debug_delay_ms' "$p")" "0"
done
ts=$(q "SELECT partdist.partdist_tso_client_start_ts()")
[[ "$ts" =~ ^[0-9]+$ && "$ts" -gt 0 ]] && ok "TSO 发号（$ts）" || bad "TSO 发号" "$ts"
CRASH0=$(docker exec "$C" bash -c "cat /work/pg-cluster-data/*/pg.log 2>/dev/null | grep -c 'terminated by signal'")
expect_eq "开跑前 t_n92 不存在" "$(q "SELECT to_regclass('public.t_n92') IS NULL")" "t"

# ---------------------------------------------------------------------------
step 1 "建表（1 个分片、1 主 2 从）并写入 10 行"
out=$(q "CREATE TABLE t_n92 (k int PRIMARY KEY, v int, SHARD KEY (k) SHARDS 1 REPLICAS 2)")
expect_eq "建表" "$(q "SELECT to_regclass('public.t_n92') IS NOT NULL")" "t"
q "INSERT INTO t_n92 SELECT g, g FROM generate_series(1, 10) g" >/dev/null
expect_eq "基线行数" "$(q 'SELECT count(*) FROM t_n92')" "10"
SHARD=$(q "SELECT shard_name FROM shardpg.shards WHERE table_name = 't_n92'::regclass")
PRIM=$(q "SELECT ltrim(\"primary\", ':') FROM shardpg.shards WHERE table_name = 't_n92'::regclass")
[[ "$SHARD" =~ ^t_n92_[0-9]+$ && "$PRIM" =~ ^[0-9]+$ ]] && ok "分片 $SHARD 主在 :$PRIM" || bad "取分片与主节点" "$SHARD / $PRIM"

# ---------------------------------------------------------------------------
step 2 "正向：写方已取 commit_ts（停 4 秒），读方此时开快照 —— 等它落账，两次读一致"
t0=$(now_ms); w_insert 101 4000 > "$TMP/w101" & WPID=$!
sleep 1.5
out=$(runf 5432 <<'SQL'
BEGIN;
SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS t0 \gset
SELECT count(*) AS r1 FROM t_n92 \gset
SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint - :t0 AS r1_ms \gset
SELECT count(*) AS r2 FROM t_n92 \gset
COMMIT;
\echo r1=:r1 r1_ms=:r1_ms r2=:r2
SQL
)
wait $WPID; w_ms=$(( $(now_ms) - t0 ))
expect_eq "写方成功" "$(cat "$TMP/w101")" ""
expect_ge "写方确实停了（毫秒）" "$w_ms" 4000
expect_eq "第一次读：等到落账后看见它" "$(field r1 "$out")" "11"
expect_eq "第二次读：与第一次一致" "$(field r2 "$out")" "11"
expect_ge "第一次读确实等了（毫秒）" "$(field r1_ms "$out")" 1500

# ---------------------------------------------------------------------------
step 3 "对照：同样的时序，读方关掉等待（commit_wait_timeout = 0）—— 旧问题必须复现"
w_insert 102 4000 > "$TMP/w102" & WPID=$!
sleep 1.5
out=$(runf 5432 <<'SQL'
SET citus.propagate_set_commands = 'local';
BEGIN;
SET LOCAL pg_partdist.commit_wait_timeout = 0;
SELECT count(*) AS r1 FROM t_n92 \gset
SELECT pg_sleep(5) AS z \gset
SELECT count(*) AS r2 FROM t_n92 \gset
COMMIT;
\echo r1=:r1 r2=:r2
SQL
)
wait $WPID
expect_eq "写方成功" "$(cat "$TMP/w102")" ""
expect_eq "关掉等待：第一次读当它未提交" "$(field r1 "$out")" "11"
expect_eq "关掉等待：同一快照第二次读看见它（旧问题复现）" "$(field r2 "$out")" "12"

# ---------------------------------------------------------------------------
step 4 "不该等的不等：读方先开快照，写方后取号（commit_ts 更大）—— 立即返回、始终看不到"
runf 5432 > "$TMP/r4" <<'SQL' &
BEGIN;
SELECT count(*) AS r0 FROM t_n92 \gset
SELECT pg_sleep(3) AS z \gset
SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS t0 \gset
SELECT count(*) AS r1 FROM t_n92 \gset
SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint - :t0 AS r1_ms \gset
SELECT pg_sleep(5) AS z \gset
SELECT count(*) AS r2 FROM t_n92 \gset
COMMIT;
SELECT count(*) AS r3 FROM t_n92 \gset
\echo r0=:r0 r1=:r1 r1_ms=:r1_ms r2=:r2 r3=:r3
SQL
RPID=$!
sleep 1.5
w_insert 103 4000 > "$TMP/w103" & WPID=$!
wait $RPID; wait $WPID
out=$(cat "$TMP/r4")
expect_eq "写方成功" "$(cat "$TMP/w103")" ""
expect_eq "快照建立时" "$(field r0 "$out")" "12"
expect_eq "写方提交中：看不到" "$(field r1 "$out")" "12"
expect_lt "写方提交中：不等（毫秒）" "$(field r1_ms "$out")" 1000
expect_eq "写方落账后：同一快照仍看不到" "$(field r2 "$out")" "12"
expect_eq "新事务看得到" "$(field r3 "$out")" "13"

# ---------------------------------------------------------------------------
step 5 "超时：写方停 6 秒，读方只等 1 秒 —— 报可重试错误 40001"
w_insert 104 6000 > "$TMP/w104" & WPID=$!
sleep 1.5
t0=$(now_ms)
out=$(runf 5432 <<'SQL'
\set VERBOSITY verbose
SET citus.propagate_set_commands = 'local';
BEGIN;
SET LOCAL pg_partdist.commit_wait_timeout = 1000;
SELECT count(*) FROM t_n92;
ROLLBACK;
SQL
)
r_ms=$(( $(now_ms) - t0 ))
wait $WPID
expect_has "报串行化错误（40001）" "$out" "40001"
expect_has "错误说明是在等提交" "$out" "正在提交"
expect_lt "1 秒左右就放弃，没有等满 6 秒（毫秒，含连接开销）" "$r_ms" 4000
expect_eq "写方照常成功" "$(cat "$TMP/w104")" ""
expect_eq "写方的行已落账" "$(q 'SELECT count(*) FROM t_n92 WHERE k = 104')" "1"

# ---------------------------------------------------------------------------
step 6 "自然并发：经 master 持续单条 INSERT（不加延迟），开着的事务里两次读 8 轮全一致"
( for i in $(seq 1000 1029); do q "INSERT INTO t_n92 VALUES ($i, $i)" >/dev/null; done ) & LPID=$!
sleep 1
changed=0; rounds=0; first=""; last=""
for i in 1 2 3 4 5 6 7 8; do
    out=$(runf 5432 <<'SQL'
BEGIN;
SELECT count(*) AS r1 FROM t_n92 \gset
SELECT pg_sleep(2) AS z \gset
SELECT count(*) AS r2 FROM t_n92 \gset
COMMIT;
\echo r1=:r1 r2=:r2
SQL
)
    r1=$(field r1 "$out"); r2=$(field r2 "$out")
    [[ "$r1" =~ ^[0-9]+$ && "$r2" =~ ^[0-9]+$ ]] || { bad "第 $i 轮读失败" "$out"; continue; }
    rounds=$((rounds + 1)); [[ -z "$first" ]] && first=$r1; last=$r2
    [[ "$r1" != "$r2" ]] && { changed=$((changed + 1)); printf '        第 %d 轮：%s → %s\n' "$i" "$r1" "$r2"; }
done
wait $LPID
expect_eq "完成轮数（计数守卫）" "$rounds" "8"
expect_eq "事务内两次读不一致的轮数" "$changed" "0"
[[ -n "$first" && -n "$last" && "$last" -gt "$first" ]] && ok "读的同时确实在写（$first → $last）" \
    || bad "读的同时确实在写" "first=$first last=$last"
expect_eq "最终行数（10 + 4 + 30）" "$(q 'SELECT count(*) FROM t_n92')" "44"

# ---------------------------------------------------------------------------
step 7 "清理与健康"
q "DROP TABLE t_n92" >/dev/null
expect_eq "t_n92 已删" "$(q "SELECT to_regclass('public.t_n92') IS NULL")" "t"
np=0; for p in 5432 $WORKERS; do np=$((np + $(q "SELECT count(*) FROM pg_prepared_xacts" "$p"))); done
expect_eq "各节点无 prepared 事务" "$np" "0"
nc=$(docker exec "$C" bash -c "cat /work/pg-cluster-data/*/pg.log 2>/dev/null | grep -c 'terminated by signal'")
expect_eq "测试期间无进程崩溃（terminated by signal 条数不变）" "$nc" "$CRASH0"

# ---------------------------------------------------------------------------
TOTAL=$((PASS + FAIL))
printf '\n==== 合计 PASS=%d FAIL=%d（共 %d 项）====\n' "$PASS" "$FAIL" "$TOTAL"
[[ $TOTAL -gt 0 && $FAIL -eq 0 ]]
