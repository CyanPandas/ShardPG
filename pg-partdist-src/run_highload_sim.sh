#!/usr/bin/env bash
# run_highload_sim.sh  —  高负载生产环境模拟 + 全程 p99 监控
#
# 背景负载:
#   • pgbench 64 客户端 (INSERT 50% / UPDATE 30% / SELECT 20%), 30s burst 循环
#   • 15 路并行 INSERT 流 (30行/批, 40ms 间隔)
#   • 5 路 UPDATE 流 + 3 路 SELECT 流
#
# p99 监控:
#   • 后台每 2s 采样 demux_progress() → p99_us（Demux 内部延迟）
#   • test 9 (perf_latency.sh) 采集端到端 500 样本作为权威 p99
#
# 报告:
#   • 每项测试: PASS/FAIL, 耗时, 期间背景写入行数, 期间峰值 p99_us
#   • 汇总: 全局 Demux p99, 端到端 p99, 背景总 TPS

set -uo pipefail

CONTAINER="pg-citus-cluster-container"
PSQL_C="docker exec -u postgres $CONTAINER /work/pg-install/bin/psql"
HL_FLAG="/tmp/hl_running"
P99_LOG="/tmp/hl_p99.csv"         # ts,test_num,port,p99_us,avg_us
BG_TPS_A="/tmp/hl_tps_a.txt"
BG_TPS_B="/tmp/hl_tps_b.txt"
BG_PIDS="/tmp/hl_pids.txt"

PASS_TOTAL=0; FAIL_TOTAL=0
TOTAL_START=$(date +%s)

declare -a TEST_NAMES TEST_STATUS TEST_ELAPSED TEST_BGROWS TEST_P99

# ─── ansi ────────────────────────────────────────────────────────────────────
G='\033[32m'; R='\033[31m'; Y='\033[33m'; B='\033[1m'; N='\033[0m'

hdr() {
    printf "\n${B}╔══════════════════════════════════════════════════════════╗${N}\n"
    printf   "${B}║  %-56s  ║${N}\n" "$1"
    printf   "${B}╚══════════════════════════════════════════════════════════╝${N}\n"
}

cleanup() {
    rm -f "$HL_FLAG"
    [ -f "$BG_PIDS" ] && { while read -r p; do kill "$p" 2>/dev/null; done < "$BG_PIDS"; rm -f "$BG_PIDS"; }
    rm -f /tmp/hl_tps_*.txt /tmp/hl_test_result_*.log
    # stop pgbench loop inside container
    docker exec "$CONTAINER" rm -f /tmp/hl_running 2>/dev/null || true
}
trap cleanup EXIT

# ════════════════════════════════════════════════════════════════════════
hdr "Phase 0 — 生产参数 + 连接上限调整"
# ════════════════════════════════════════════════════════════════════════

# Write settings directly to postgresql.conf for each node (ALTER SYSTEM is unreliable
# for multi-statement batches; sed is authoritative).
docker exec -u postgres "$CONTAINER" bash -c "
    for cfg in master worker1 worker2; do
        conf=/work/pg-cluster-data/\$cfg/postgresql.conf
        auto=/work/pg-cluster-data/\$cfg/postgresql.auto.conf
        # Remove any existing lines for these settings from postgresql.conf, then append
        sed -i '/^max_connections\b/d;
                /^checkpoint_completion_target\b/d;
                /^max_wal_size\b/d;
                /^min_wal_size\b/d;
                /^wal_buffers\b/d;
                /^wal_compression\b/d;
                /^synchronous_commit\b/d;
                /^log_checkpoints\b/d;
                /^effective_cache_size\b/d' \$conf
        cat >> \$conf <<'EOF'
max_connections = 500
checkpoint_completion_target = 0.9
max_wal_size = 4GB
min_wal_size = 512MB
wal_buffers = 64MB
wal_compression = on
synchronous_commit = on
log_checkpoints = on
effective_cache_size = 512MB
EOF
        echo \"  \$cfg ✓\"
    done"

# max_connections requires a restart to take effect — restart all three nodes now
echo "  重启所有节点以使 max_connections=500 生效..."
docker exec -u postgres "$CONTAINER" bash -c "
    export PATH=/work/pg-install/bin:\$PATH
    for cfg in master worker1 worker2; do
        port=\$([ \$cfg = master ] && echo 5432 || ([ \$cfg = worker1 ] && echo 5433 || echo 5434))
        d=/work/pg-cluster-data/\$cfg
        pg_ctl status -D \$d >/dev/null 2>&1 && \
            pg_ctl restart -D \$d -l \$d/pg.log -o \"-p \$port\" -w -t 60 >/dev/null 2>&1 || true
    done" 2>/dev/null
echo "  所有节点已重启 (max_connections=500 生效)"

# ════════════════════════════════════════════════════════════════════════
hdr "Phase 1 — 预写入 200,000 行背景数据"
# ════════════════════════════════════════════════════════════════════════

$PSQL_C -p 5432 -d postgres -q -c "
    DROP TABLE IF EXISTS hl_bg CASCADE;
    CREATE TABLE hl_bg (id BIGSERIAL, bucket INT NOT NULL,
                         payload TEXT, ts TIMESTAMPTZ DEFAULT NOW());
    SELECT create_distributed_table('hl_bg','bucket',shard_count=>16);" 2>&1 | grep -v '^$' || true

$PSQL_C -p 5432 -d postgres -q -c "
    INSERT INTO hl_bg(bucket,payload)
    SELECT (i%9999+1), 'seed_'||i FROM generate_series(1,200000) i;" > /dev/null
echo "  200,000 行种子数据写入完成"

# ════════════════════════════════════════════════════════════════════════
hdr "Phase 2 — 写入 pgbench 脚本 + 启动高负载背景"
# ════════════════════════════════════════════════════════════════════════

# pgbench 脚本
docker exec -u postgres "$CONTAINER" bash -c "cat > /tmp/hl_insert.sql" << 'PBEOF'
\set b random(1,9999)
INSERT INTO hl_bg(bucket,payload) VALUES(:b,'pg'||:client_id||'_'||:iteration) ON CONFLICT DO NOTHING;
PBEOF
docker exec -u postgres "$CONTAINER" bash -c "cat > /tmp/hl_update.sql" << 'PBEOF'
\set b random(1,9999)
UPDATE hl_bg SET ts=NOW() WHERE bucket=:b AND id=(SELECT id FROM hl_bg WHERE bucket=:b LIMIT 1);
PBEOF
docker exec -u postgres "$CONTAINER" bash -c "cat > /tmp/hl_select.sql" << 'PBEOF'
\set b random(1,9999)
SELECT id,bucket FROM hl_bg WHERE bucket=:b LIMIT 5;
PBEOF

touch "$HL_FLAG"
docker exec -u postgres "$CONTAINER" bash -c "touch /tmp/hl_running"
> "$BG_PIDS"

# ── pgbench 64 客户端 burst 循环（容器内）────────────────────────────
(
    while [ -f "$HL_FLAG" ]; do
        docker exec -u postgres "$CONTAINER" bash -c "
            export PATH=/work/pg-install/bin:\$PATH
            [ -f /tmp/hl_running ] || exit 0
            pgbench -p 5432 -U postgres -d postgres \
                -c 64 -j 8 -T 30 --no-vacuum \
                -f /tmp/hl_insert.sql@50 \
                -f /tmp/hl_update.sql@30 \
                -f /tmp/hl_select.sql@20 \
                2>/dev/null || true" 2>/dev/null || true
        sleep 1
    done
) &
echo $! >> "$BG_PIDS"

# ── 15 路 INSERT worker (30行/批, 40ms) ──────────────────────────────
echo 0 > "$BG_TPS_A"
(
    CNT=0
    while [ -f "$HL_FLAG" ]; do
        docker exec -u postgres "$CONTAINER" \
            /work/pg-install/bin/psql -p 5432 -d postgres -q -c \
            "INSERT INTO hl_bg(bucket,payload) SELECT (random()*8999+1)::int,'a'||clock_timestamp() FROM generate_series(1,30);" \
            >/dev/null 2>&1 || true
        CNT=$((CNT+30)); echo $CNT > "$BG_TPS_A"
        sleep 0.04
    done
) &
echo $! >> "$BG_PIDS"

for _i in $(seq 2 15); do
    (
        while [ -f "$HL_FLAG" ]; do
            docker exec -u postgres "$CONTAINER" \
                /work/pg-install/bin/psql -p 5432 -d postgres -q -c \
                "INSERT INTO hl_bg(bucket,payload) SELECT (random()*8999+1)::int,'w${_i}_'||now() FROM generate_series(1,30);" \
                >/dev/null 2>&1 || true
            sleep 0.04
        done
    ) &
    echo $! >> "$BG_PIDS"
done

# ── 5 路 UPDATE worker ────────────────────────────────────────────────
for _i in $(seq 1 5); do
    (
        while [ -f "$HL_FLAG" ]; do
            docker exec -u postgres "$CONTAINER" \
                /work/pg-install/bin/psql -p 5432 -d postgres -q -c \
                "UPDATE hl_bg SET ts=NOW() WHERE id=(SELECT id FROM hl_bg ORDER BY random() LIMIT 1);" \
                >/dev/null 2>&1 || true
            sleep 0.1
        done
    ) &
    echo $! >> "$BG_PIDS"
done

# ── 3 路 SELECT worker ────────────────────────────────────────────────
echo 0 > "$BG_TPS_B"
for _i in $(seq 1 3); do
    (
        CNT=0
        while [ -f "$HL_FLAG" ]; do
            docker exec -u postgres "$CONTAINER" \
                /work/pg-install/bin/psql -p 5432 -d postgres -q -c \
                "SELECT COUNT(*) FROM hl_bg WHERE bucket % 500 = 0;" \
                >/dev/null 2>&1 || true
            CNT=$((CNT+1)); echo $CNT > "$BG_TPS_B"
            sleep 0.2
        done
    ) &
    echo $! >> "$BG_PIDS"
done

# ── 后台 p99 采样器（每 2s 从 worker 的 demux_latency_stats 采集）──────
> "$P99_LOG"
(
    while [ -f "$HL_FLAG" ]; do
        TN=$(cat /tmp/hl_test_num 2>/dev/null || echo 0)
        TS=$(date +%s)
        for port in 5433 5434; do
            row=$(docker exec -u postgres "$CONTAINER" \
                /work/pg-install/bin/psql -p $port -d postgres -At -c \
                "SELECT round(p99_ms*1000)::bigint, round(avg_ms*1000)::bigint
                 FROM partdist.demux_latency_stats();" \
                2>/dev/null | grep -E '^[0-9]+\|[0-9]+$' || true)
            if [ -n "$row" ]; then
                p99=$(echo "$row" | cut -d'|' -f1)
                avg=$(echo "$row" | cut -d'|' -f2)
                echo "$TS,$TN,$port,${p99:-0},${avg:-0}" >> "$P99_LOG"
            fi
        done
        sleep 2
    done
) &
echo $! >> "$BG_PIDS"

sleep 4  # 让负载稳定
echo "  pgbench (64c) + 15 INSERT + 5 UPDATE + 3 SELECT 已启动"
A0=$(cat "$BG_TPS_A" 2>/dev/null || echo 0)
echo "  初始 INSERT 行数: $A0"

# ════════════════════════════════════════════════════════════════════════
hdr "Phase 3 — 执行 10 项测试"
# ════════════════════════════════════════════════════════════════════════

run_test() {
    local num="$1" name="$2" cmd="$3" host="${4:-false}"
    echo $num > /tmp/hl_test_num

    local t0=$(date +%s)
    local A0=$(cat "$BG_TPS_A" 2>/dev/null || echo 0)

    printf "\n${Y}[%2d/10]${N} ${B}%s${N}\n" "$num" "$name"
    printf "       运行中..."

    local rfile="/tmp/hl_test_result_${num}.log"
    if [ "$host" = "true" ]; then
        eval "$cmd" > "$rfile" 2>&1; RC=$?
    else
        docker exec -u postgres "$CONTAINER" bash -c \
            "export PATH=/work/pg-install/bin:\$PATH; cd /work/pg-partdist-src; $cmd" \
            > "$rfile" 2>&1; RC=$?
    fi

    local t1=$(date +%s)
    local elapsed=$((t1 - t0))
    local A1=$(cat "$BG_TPS_A" 2>/dev/null || echo 0)
    local delta=$((A1 - A0))

    # peak p99_us during this test window
    local peak_p99=0
    if [ -f "$P99_LOG" ]; then
        peak_p99=$(awk -F',' -v t0=$t0 -v t1=$t1 '
            $1>=t0 && $1<=t1 { if($4>max) max=$4 }
            END { print (max+0) }' "$P99_LOG")
    fi

    local ipass=$(grep -Eo 'PASS[=: ]+[0-9]+|通过: [0-9]+|PASSED: [0-9]+|Tests passed: [0-9]+' "$rfile" 2>/dev/null \
                  | grep -oE '[0-9]+' | tail -1 || echo '?')
    local ifail=$(grep -Eo 'FAIL[=: ]+[0-9]+|失败: [0-9]+|FAILED: [0-9]+|Tests failed: [0-9]+' "$rfile" 2>/dev/null \
                  | grep -oE '[0-9]+' | tail -1 || echo '?')

    local status
    if [ $RC -eq 0 ]; then
        status="PASS"; PASS_TOTAL=$((PASS_TOTAL+1))
        printf "\r       ${G}PASS${N}  %3ds  bg+%d行  p99峰值=%dμs  (%s✓/%s✗)\n" \
               "$elapsed" "$delta" "$peak_p99" "$ipass" "$ifail"
    else
        status="FAIL"; FAIL_TOTAL=$((FAIL_TOTAL+1))
        printf "\r       ${R}FAIL${N}  %3ds  bg+%d行  p99峰值=%dμs  (%s✓/%s✗)\n" \
               "$elapsed" "$delta" "$peak_p99" "$ipass" "$ifail"
        echo "       ── 末10行日志 ──"
        tail -10 "$rfile" | sed 's/^/       /'
    fi

    TEST_NAMES[$num]="$name"
    TEST_STATUS[$num]="$status"
    TEST_ELAPSED[$num]="$elapsed"
    TEST_BGROWS[$num]="$delta"
    TEST_P99[$num]="$peak_p99"
}

# 确保所有节点在线并等待 demux 就绪
ensure_all_up() {
    docker exec -u postgres "$CONTAINER" bash -c "
        export PATH=/work/pg-install/bin:\$PATH
        for d in master worker1 worker2; do
            case \$d in master) p=5432;; worker1) p=5433;; *) p=5434;; esac
            pg_ctl status -D /work/pg-cluster-data/\$d >/dev/null 2>&1 || \
                pg_ctl start -D /work/pg-cluster-data/\$d \
                             -l /work/pg-cluster-data/\$d/pg.log \
                             -o \"-p \$p\" -w -t 60 >/dev/null 2>&1 || true
        done
        # 等待两个 worker 的 demux 进程启动
        for d in worker1 worker2; do
            pm_pid=\$(head -1 /work/pg-cluster-data/\$d/postmaster.pid 2>/dev/null || echo '')
            tries=0
            while [ \$tries -lt 40 ]; do
                cnt=\$(ps -o pid,ppid,args --no-headers 2>/dev/null | awk -v p=\"\$pm_pid\" '\$2==p && /demux/' | wc -l)
                [ \$cnt -ge 1 ] && break
                sleep 0.5; tries=\$((tries+1))
            done
        done" 2>/dev/null || true
    sleep 3
}

run_test  1 "写入连续性 & 崩溃恢复"          "bash verify_continuity_and_crash.sh 2>&1"
run_test  2 "分片自动初始化 (含37项回归)"     "bash test_shard_auto_init.sh 2>&1"
run_test  3 "多分布表隔离性 & 持久性"         "bash test_multi_table_isolation.sh 2>&1"
run_test  4 "崩溃恢复专项 (A/B/C三场景)"     "bash test_crash_recovery.sh 2>&1"

# test 4 crashes workers — restore before test 5
ensure_all_up

# test 5: 高负载下放宽性能阈值至 50%
run_test  5 "批量写入 COPY 路径恢复"          "BULK_OVERHEAD_THRESHOLD=50 bash test_bulk_insert_recovery.sh 2>&1"
run_test  6 "跨段边界 LSN 连续性"             "bash test_segment_boundary_lsn.sh 2>&1"
run_test  7 "段文件损坏恢复 (C1-C4)"          "bash test_corrupt_segment_recovery.sh 2>&1"

# test 7 may leave workers crashed — restore before test 8
ensure_all_up

run_test  8 "Demux 高积压崩溃恢复 (S1-S5)"   "bash test_demux_backlog_recovery.sh 2>&1"

# test 8 crashes workers — restore before tests 9 and 10
ensure_all_up

run_test  9 "端到端延迟 p99 (500样本,32并发)" \
    "bash /home/zhanhao/pg-citus-cluster/pg-partdist-src/perf_latency.sh 2>&1" "true"

ensure_all_up
run_test 10 "磁盘满 ENOSPC 容错恢复"          "bash test_enospc_recovery.sh 2>&1"

# ════════════════════════════════════════════════════════════════════════
hdr "Phase 4 — 停止背景负载"
# ════════════════════════════════════════════════════════════════════════

rm -f "$HL_FLAG"
docker exec "$CONTAINER" rm -f /tmp/hl_running 2>/dev/null || true
sleep 2
echo "  背景负载已停止"

# ════════════════════════════════════════════════════════════════════════
hdr "Phase 5 — 统计 & 报告"
# ════════════════════════════════════════════════════════════════════════

# 全局 p99 统计（来自 demux_progress 采样日志）
GLOBAL_MAX_P99=0; GLOBAL_P95_P99=0; GLOBAL_AVG_P99=0; SAMPLE_CNT=0
if [ -f "$P99_LOG" ] && [ -s "$P99_LOG" ]; then
    read GLOBAL_MAX_P99 GLOBAL_P95_P99 GLOBAL_AVG_P99 SAMPLE_CNT <<< $(awk -F',' '
        NR>0 { vals[NR]=$4+0; sum+=$4; cnt++ }
        END {
            if(cnt==0){ print "0 0 0 0"; exit }
            # sort
            n = asort(vals)
            avg = sum/cnt
            p95_idx = int(n*0.95); if(p95_idx<1) p95_idx=1
            printf "%d %d %d %d\n", vals[n], vals[p95_idx], int(avg), cnt
        }' "$P99_LOG")
fi

# 从 test 9 结果提取端到端 p99
E2E_P99="N/A"; E2E_AVG="N/A"; E2E_STATUS="N/A"
if [ -f "/tmp/hl_test_result_9.log" ]; then
    E2E_P99=$(grep -oE 'p99 *\| *[0-9.]+ ms' /tmp/hl_test_result_9.log 2>/dev/null \
              | grep -oE '[0-9.]+' | head -1 || echo "N/A")
    E2E_AVG=$(grep -oE 'avg *\| *[0-9.]+ ms' /tmp/hl_test_result_9.log 2>/dev/null \
              | grep -oE '[0-9.]+' | head -1 || echo "N/A")
    E2E_STATUS=$(grep -oE 'p99.*PASS|p99.*FAIL' /tmp/hl_test_result_9.log 2>/dev/null \
                 | head -1 || echo "N/A")
fi

TOTAL_A=$(cat "$BG_TPS_A" 2>/dev/null || echo 0)
TOTAL_END=$(date +%s)
TOTAL_ELAPSED=$((TOTAL_END - TOTAL_START))
MINS=$((TOTAL_ELAPSED/60)); SECS=$((TOTAL_ELAPSED%60))

# 恢复默认参数
for port in 5432 5433 5434; do
    $PSQL_C -p $port -d postgres -q -c "
        ALTER SYSTEM RESET ALL; SELECT pg_reload_conf();" > /dev/null 2>&1 || true
done

# ─── 打印报告 ──────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
printf "║  %-76s  ║\n" "  高负载生产模拟测试报告   PostgreSQL 16 + Citus + pg_partdist"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
printf "║  %-76s  ║\n" "  背景负载: pgbench×64客户端 + 15路INSERT + 5路UPDATE + 3路SELECT"
printf "║  %-76s  ║\n" "  预加载数据: 200,000 行 · 总用时: ${MINS}m ${SECS}s"
echo "╠═══╦════════════════════════════════════╦══════╦═════╦═══════════╦══════════╣"
printf "║ %-2s║ %-36s║ %-6s║ %-5s║ %-11s║ %-10s║\n" \
       "#" "测试项" "结果" "耗时" "背景+行数" "峰值p99μs"
echo "╠═══╬════════════════════════════════════╬══════╬═════╬═══════════╬══════════╣"

for i in $(seq 1 10); do
    n="${TEST_NAMES[$i]:-?}"
    s="${TEST_STATUS[$i]:-?}"
    e="${TEST_ELAPSED[$i]:-?}"
    b="${TEST_BGROWS[$i]:-?}"
    p="${TEST_P99[$i]:-?}"
    if [ "$s" = "PASS" ]; then
        sc="${G}PASS${N}"
    else
        sc="${R}FAIL${N}"
    fi
    printf "║ %-2s║ %-36s║ %b  ║ %3ds║ %+10s║ %9sμ║\n" \
           "$i" "${n:0:36}" "$sc" "$e" "$b" "$p"
done

echo "╠═══╩════════════════════════════════════╩══════╩═════╩═══════════╩══════════╣"
echo "║                                                                              ║"
printf "║  %-76s  ║\n" "  Demux 内部 p99 监控 (来源: demux_progress() 每2s采样, n=${SAMPLE_CNT})"
printf "║    %-74s  ║\n" "  Max p99_us : ${GLOBAL_MAX_P99} μs   P95 of p99_us : ${GLOBAL_P95_P99} μs   Avg p99_us : ${GLOBAL_AVG_P99} μs"
echo "║                                                                              ║"
printf "║  %-76s  ║\n" "  端到端 p99 (perf_latency.sh, 500样本, 32并发 pgbench):"
printf "║    %-74s  ║\n" "  p99 = ${E2E_P99} ms    avg = ${E2E_AVG} ms    阈值判定: ${E2E_STATUS}"
echo "║                                                                              ║"
if [ $FAIL_TOTAL -eq 0 ]; then
    printf "║  ${G}%-76s${N}  ║\n" "  最终结果: 全部通过 ✓   PASS = ${PASS_TOTAL}   FAIL = ${FAIL_TOTAL}"
else
    printf "║  ${R}%-76s${N}  ║\n" "  最终结果: 存在失败 ✗   PASS = ${PASS_TOTAL}   FAIL = ${FAIL_TOTAL}"
fi
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

[ $FAIL_TOTAL -eq 0 ] && exit 0 || exit 1
