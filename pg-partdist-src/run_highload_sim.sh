#!/usr/bin/env bash
# run_highload_sim.sh  —  高负载生产环境模拟（逐测试隔离运行）
#
# 每个测试独立执行：
#   1. 准备环境（保证集群在线、清空 pg_parwal、删除遗留测试表）
#   2. 启动背景负载（pgbench 64c + 15×INSERT + 5×UPDATE + 3×SELECT）
#   3. 运行单个测试脚本
#   4. 停止背景负载
#   5. 清理环境（重启崩溃节点、还原参数）
#   6. 记录结果，继续下一项

set -uo pipefail

CONTAINER="pg-citus-cluster-container"
PSQL_C="docker exec -u postgres $CONTAINER /work/pg-install/bin/psql"
P99_LOG="/tmp/hl_p99.csv"
BG_PIDS_FILE="/tmp/hl_pids.txt"
HL_FLAG="/tmp/hl_running"

PASS_TOTAL=0; FAIL_TOTAL=0
TOTAL_START=$(date +%s)

declare -a TEST_NAMES TEST_STATUS TEST_ELAPSED TEST_P99

G='\033[32m'; R='\033[31m'; Y='\033[33m'; B='\033[1m'; N='\033[0m'

hdr() {
    printf "\n${B}╔══════════════════════════════════════════════════════════╗${N}\n"
    printf   "${B}║  %-56s  ║${N}\n" "$1"
    printf   "${B}╚══════════════════════════════════════════════════════════╝${N}\n"
}

# ════════════════════════════════════════════════════════════════════════
# 工具函数
# ════════════════════════════════════════════════════════════════════════

# 确保三个节点全部在线，等待 demux 进程启动
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
        for port in 5433 5434; do
            tries=0
            while [ \$tries -lt 60 ]; do
                r=\$(psql -p \$port -d postgres -At \
                    -c 'SELECT partdist.demux_is_ready()' 2>/dev/null || echo f)
                [ \"\$r\" = t ] && break
                sleep 0.5; tries=\$((tries+1))
            done
        done" 2>/dev/null || true
    sleep 2
}

# 清空所有 worker 的 pg_parwal 目录（先停 worker，清完再起）
clean_parwal() {
    docker exec -u postgres "$CONTAINER" bash -c "
        export PATH=/work/pg-install/bin:\$PATH
        for d in worker1 worker2; do
            case \$d in worker1) p=5433;; *) p=5434;; esac
            pg_ctl stop -D /work/pg-cluster-data/\$d -m fast -w 2>&1 | tail -1 || true
        done
        rm -rf /work/pg-cluster-data/worker1/pg_parwal \
               /work/pg-cluster-data/worker2/pg_parwal
        for d in worker1 worker2; do
            case \$d in worker1) p=5433;; *) p=5434;; esac
            pg_ctl start -D /work/pg-cluster-data/\$d \
                         -l /work/pg-cluster-data/\$d/pg.log \
                         -o \"-p \$p\" -w -t 60 >/dev/null 2>&1 || true
        done" 2>/dev/null || true
    sleep 2
}

# 重置 postgresql 参数至默认值
reset_pg_params() {
    for port in 5432 5433 5434; do
        $PSQL_C -p $port -d postgres -q -c \
            "ALTER SYSTEM RESET ALL; SELECT pg_reload_conf();" \
            >/dev/null 2>&1 || true
    done
}

# 应用高负载参数（需要重启生效）
apply_highload_params() {
    docker exec -u postgres "$CONTAINER" bash -c "
        for cfg in master worker1 worker2; do
            conf=/work/pg-cluster-data/\$cfg/postgresql.conf
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
        done" 2>/dev/null
    docker exec -u postgres "$CONTAINER" bash -c "
        export PATH=/work/pg-install/bin:\$PATH
        for cfg in master worker1 worker2; do
            port=\$([ \$cfg = master ] && echo 5432 || ([ \$cfg = worker1 ] && echo 5433 || echo 5434))
            d=/work/pg-cluster-data/\$cfg
            pg_ctl status -D \$d >/dev/null 2>&1 && \
                pg_ctl restart -D \$d -l \$d/pg.log -o \"-p \$port\" -w -t 60 >/dev/null 2>&1 || true
        done" 2>/dev/null
}

# 准备每次测试前的干净环境
prepare_for_test() {
    ensure_all_up
    clean_parwal
    ensure_all_up
    # 删除可能遗留的测试表
    $PSQL_C -p 5432 -d postgres -q -c "
        DROP TABLE IF EXISTS hl_bg CASCADE;
        DROP TABLE IF EXISTS crash_test CASCADE;
        DROP TABLE IF EXISTS crash_demux_test CASCADE;
        DROP TABLE IF EXISTS crash_val_test CASCADE;
        DROP TABLE IF EXISTS crash_pg_test CASCADE;
        DROP TABLE IF EXISTS crash_partial_test CASCADE;
        DROP TABLE IF EXISTS enospc_test CASCADE;
        DROP TABLE IF EXISTS backlog_test CASCADE;
        DROP TABLE IF EXISTS seg_test CASCADE;
        DROP TABLE IF EXISTS bulk_test CASCADE;" \
        >/dev/null 2>&1 || true
}

# 启动背景负载（写背景数据表 + 启动 worker 进程），返回前等待负载稳定
start_background_load() {
    # 写入背景数据表
    $PSQL_C -p 5432 -d postgres -q -c "
        DROP TABLE IF EXISTS hl_bg CASCADE;
        CREATE TABLE hl_bg (id BIGSERIAL, bucket INT NOT NULL,
                             payload TEXT, ts TIMESTAMPTZ DEFAULT NOW());
        SELECT create_distributed_table('hl_bg','bucket',shard_count=>16);" \
        >/dev/null 2>&1 || true
    $PSQL_C -p 5432 -d postgres -q -c "
        INSERT INTO hl_bg(bucket,payload)
        SELECT (i%9999+1), 'seed_'||i FROM generate_series(1,200000) i;" \
        >/dev/null

    # 写 pgbench 脚本
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
    > "$BG_PIDS_FILE"

    # pgbench 64 客户端
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
    echo $! >> "$BG_PIDS_FILE"

    # 15 路 INSERT worker
    for _i in $(seq 1 15); do
        (
            while [ -f "$HL_FLAG" ]; do
                docker exec -u postgres "$CONTAINER" \
                    /work/pg-install/bin/psql -p 5432 -d postgres -q -c \
                    "INSERT INTO hl_bg(bucket,payload) SELECT (random()*8999+1)::int,'w${_i}_'||now() FROM generate_series(1,30);" \
                    >/dev/null 2>&1 || true
                sleep 0.04
            done
        ) &
        echo $! >> "$BG_PIDS_FILE"
    done

    # 5 路 UPDATE worker
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
        echo $! >> "$BG_PIDS_FILE"
    done

    # 3 路 SELECT worker
    for _i in $(seq 1 3); do
        (
            while [ -f "$HL_FLAG" ]; do
                docker exec -u postgres "$CONTAINER" \
                    /work/pg-install/bin/psql -p 5432 -d postgres -q -c \
                    "SELECT COUNT(*) FROM hl_bg WHERE bucket % 500 = 0;" \
                    >/dev/null 2>&1 || true
                sleep 0.2
            done
        ) &
        echo $! >> "$BG_PIDS_FILE"
    done

    # 后台 p99 采样器
    (
        while [ -f "$HL_FLAG" ]; do
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
                    echo "$TS,${p99:-0},${avg:-0}" >> "$P99_LOG"
                fi
            done
            sleep 2
        done
    ) &
    echo $! >> "$BG_PIDS_FILE"

    sleep 4  # 等负载稳定
}

# 停止并清理背景负载
stop_background_load() {
    rm -f "$HL_FLAG"
    docker exec "$CONTAINER" rm -f /tmp/hl_running 2>/dev/null || true
    if [ -f "$BG_PIDS_FILE" ]; then
        while read -r p; do kill "$p" 2>/dev/null || true; done < "$BG_PIDS_FILE"
        rm -f "$BG_PIDS_FILE"
    fi
    sleep 1
}

# 轻量背景负载 — 专为 demux 积压/崩溃恢复类测试（test 8）设计
# pgbench 8客户端 + 3×INSERT，保持压力但不让 WAL 积压到影响 demux 追赶速度
start_light_background_load() {
    $PSQL_C -p 5432 -d postgres -q -c "
        DROP TABLE IF EXISTS hl_bg CASCADE;
        CREATE TABLE hl_bg (id BIGSERIAL, bucket INT NOT NULL,
                             payload TEXT, ts TIMESTAMPTZ DEFAULT NOW());
        SELECT create_distributed_table('hl_bg','bucket',shard_count=>4);" \
        >/dev/null 2>&1 || true
    $PSQL_C -p 5432 -d postgres -q -c "
        INSERT INTO hl_bg(bucket,payload)
        SELECT (i%9999+1), 'seed_'||i FROM generate_series(1,10000) i;" \
        >/dev/null

    docker exec -u postgres "$CONTAINER" bash -c "cat > /tmp/hl_insert.sql" << 'PBEOF'
\set b random(1,9999)
INSERT INTO hl_bg(bucket,payload) VALUES(:b,'light'||:client_id||'_'||:iteration) ON CONFLICT DO NOTHING;
PBEOF

    touch "$HL_FLAG"
    docker exec -u postgres "$CONTAINER" bash -c "touch /tmp/hl_running"
    > "$BG_PIDS_FILE"

    # pgbench 8客户端（而非64），避免 WAL 洪水
    (
        while [ -f "$HL_FLAG" ]; do
            docker exec -u postgres "$CONTAINER" bash -c "
                export PATH=/work/pg-install/bin:\$PATH
                [ -f /tmp/hl_running ] || exit 0
                pgbench -p 5432 -U postgres -d postgres \
                    -c 8 -j 4 -T 30 --no-vacuum \
                    -f /tmp/hl_insert.sql \
                    2>/dev/null || true" 2>/dev/null || true
            sleep 1
        done
    ) &
    echo $! >> "$BG_PIDS_FILE"

    # 3路 INSERT worker（而非15）
    for _i in $(seq 1 3); do
        (
            while [ -f "$HL_FLAG" ]; do
                docker exec -u postgres "$CONTAINER" \
                    /work/pg-install/bin/psql -p 5432 -d postgres -q -c \
                    "INSERT INTO hl_bg(bucket,payload) SELECT (random()*8999+1)::int,'light${_i}_'||now() FROM generate_series(1,10);" \
                    >/dev/null 2>&1 || true
                sleep 0.1
            done
        ) &
        echo $! >> "$BG_PIDS_FILE"
    done

    # p99 采样器
    (
        while [ -f "$HL_FLAG" ]; do
            TS=$(date +%s)
            for port in 5433 5434; do
                row=$(docker exec -u postgres "$CONTAINER" \
                    /work/pg-install/bin/psql -p $port -d postgres -At -c \
                    "SELECT round(p99_ms*1000)::bigint, round(avg_ms*1000)::bigint
                     FROM partdist.demux_latency_stats();" \
                    2>/dev/null | grep -E '^[0-9]+\|[0-9]+$' || true)
                if [ -n "$row" ]; then
                    p99=$(echo "$row" | cut -d'|' -f1)
                    echo "$TS,${p99:-0},0" >> "$P99_LOG"
                fi
            done
            sleep 2
        done
    ) &
    echo $! >> "$BG_PIDS_FILE"

    sleep 3
}

# 全局 EXIT trap — 确保背景进程总被清理
trap 'stop_background_load 2>/dev/null || true' EXIT

# ════════════════════════════════════════════════════════════════════════
# 初始化
# ════════════════════════════════════════════════════════════════════════
hdr "初始化 — 应用生产参数并重启集群"

apply_highload_params
ensure_all_up
echo "  所有节点在线，高负载参数已应用 (max_connections=500)"

# ════════════════════════════════════════════════════════════════════════
# 逐项运行测试
# ════════════════════════════════════════════════════════════════════════

run_one_test() {
    # Usage: run_one_test <num> <name> <cmd> [host=false] [presetup_cmd=""] [load=full|light]
    local num="$1" name="$2" cmd="$3" host="${4:-false}" presetup="${5:-}" load="${6:-full}"

    printf "\n${Y}[%2d/10]${N} ${B}%s${N}\n" "$num" "$name"

    # ── 1. 准备干净环境 ──────────────────────────────────────────────
    printf "       准备环境..."
    prepare_for_test
    if [ -n "$presetup" ]; then
        eval "$presetup" >/dev/null 2>&1 || true
    fi
    printf "\r       环境就绪，启动背景负载...\n"

    # ── 2. 启动背景负载 ──────────────────────────────────────────────
    > "$P99_LOG"
    if [ "$load" = "light" ]; then
        start_light_background_load
        printf "       背景负载已启动 (pgbench×8 + 3×INSERT，轻量模式)\n"
    else
        start_background_load
        printf "       背景负载已启动 (pgbench×64 + 15×INSERT + 5×UPDATE + 3×SELECT)\n"
    fi
    printf "       运行测试中..."

    # ── 3. 执行测试 ──────────────────────────────────────────────────
    local t0=$(date +%s)
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

    # ── 4. 停止背景负载 ──────────────────────────────────────────────
    stop_background_load

    # ── 5. 从 p99 日志取峰值 ─────────────────────────────────────────
    local peak_p99=0
    if [ -f "$P99_LOG" ] && [ -s "$P99_LOG" ]; then
        peak_p99=$(awk -F',' '{ if($2+0>max) max=$2+0 } END { print max+0 }' "$P99_LOG")
    fi

    local ipass ifail
    ipass=$(grep -Eo 'PASS[=: ]+[0-9]+|通过: [0-9]+|PASSED: [0-9]+|Tests passed: [0-9]+' \
            "$rfile" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo '?')
    ifail=$(grep -Eo 'FAIL[=: ]+[0-9]+|失败: [0-9]+|FAILED: [0-9]+|Tests failed: [0-9]+' \
            "$rfile" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo '?')

    local status
    if [ $RC -eq 0 ]; then
        status="PASS"; PASS_TOTAL=$((PASS_TOTAL+1))
        printf "\r       ${G}PASS${N}  %3ds  p99峰值=%dμs  (%s✓/%s✗)\n" \
               "$elapsed" "$peak_p99" "$ipass" "$ifail"
    else
        status="FAIL"; FAIL_TOTAL=$((FAIL_TOTAL+1))
        printf "\r       ${R}FAIL${N}  %3ds  p99峰值=%dμs  (%s✓/%s✗)\n" \
               "$elapsed" "$peak_p99" "$ipass" "$ifail"
        echo "       ── 末10行日志 ──"
        tail -10 "$rfile" | sed 's/^/       /'
    fi

    TEST_NAMES[$num]="$name"
    TEST_STATUS[$num]="$status"
    TEST_ELAPSED[$num]="$elapsed"
    TEST_P99[$num]="$peak_p99"

    # ── 6. 测试后清理 ────────────────────────────────────────────────
    printf "       清理环境..."
    ensure_all_up    # 恢复可能被测试崩溃的节点
    reset_pg_params  # 还原 pg 参数（测试可能修改过）
    printf "\r       清理完成\n"
}

hdr "Phase 3 — 逐项执行 10 项高负载测试"

run_one_test  1 "写入连续性 & 崩溃恢复"          "bash verify_continuity_and_crash.sh 2>&1"
run_one_test  2 "分片自动初始化 (含37项回归)"     "bash test_shard_auto_init.sh 2>&1"
run_one_test  3 "多分布表隔离性 & 持久性"         "bash test_multi_table_isolation.sh 2>&1"
run_one_test  4 "崩溃恢复专项 (A/B/C三场景)"     "bash test_crash_recovery.sh 2>&1"
run_one_test  5 "批量写入 COPY 路径恢复"          "BULK_OVERHEAD_THRESHOLD=50 bash test_bulk_insert_recovery.sh 2>&1"
run_one_test  6 "跨段边界 LSN 连续性"             "bash test_segment_boundary_lsn.sh 2>&1"
run_one_test  7 "段文件损坏恢复 (C1-C4)"          "bash test_corrupt_segment_recovery.sh 2>&1"
run_one_test  8 "Demux 高积压崩溃恢复 (S1-S5)"   "bash test_demux_backlog_recovery.sh 2>&1" "false" "" "light"
run_one_test  9 "端到端延迟 p99 (500样本,32并发)" \
    "bash /home/zhanhao/pg-citus-cluster/pg-partdist-src/perf_latency.sh 2>&1" "true"
ENOSPC_SETUP='$PSQL_C -p 5432 -d postgres -q -c "
    CREATE TABLE enospc_test (id BIGSERIAL PRIMARY KEY, payload TEXT);
    SELECT create_distributed_table('"'"'enospc_test'"'"','"'"'id'"'"',shard_count=>4);"'
run_one_test 10 "磁盘满 ENOSPC 容错恢复" "bash test_enospc_recovery.sh 2>&1" "false" "$ENOSPC_SETUP"

# ════════════════════════════════════════════════════════════════════════
# 汇总报告
# ════════════════════════════════════════════════════════════════════════
hdr "汇总报告"

TOTAL_END=$(date +%s)
TOTAL_ELAPSED=$((TOTAL_END - TOTAL_START))
MINS=$((TOTAL_ELAPSED/60)); SECS=$((TOTAL_ELAPSED%60))

# 端到端 p99（来自 test 9）
E2E_P99="N/A"; E2E_AVG="N/A"; E2E_STATUS="N/A"
if [ -f "/tmp/hl_test_result_9.log" ]; then
    E2E_P99=$(grep -oE 'p99 *\| *[0-9.]+ ms' /tmp/hl_test_result_9.log 2>/dev/null \
              | grep -oE '[0-9.]+' | head -1 || echo "N/A")
    E2E_AVG=$(grep -oE 'avg *\| *[0-9.]+ ms' /tmp/hl_test_result_9.log 2>/dev/null \
              | grep -oE '[0-9.]+' | head -1 || echo "N/A")
    E2E_STATUS=$(grep -oE 'p99.*PASS|p99.*FAIL' /tmp/hl_test_result_9.log 2>/dev/null \
                 | head -1 || echo "N/A")
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════════════════╗"
printf "║  %-74s  ║\n" "  高负载生产模拟测试报告   PostgreSQL 16 + Citus + pg_partdist"
echo "╠══════════════════════════════════════════════════════════════════════════╣"
printf "║  %-74s  ║\n" "  每项测试独立启停背景负载，测试间完整清理环境"
printf "║  %-74s  ║\n" "  总用时: ${MINS}m ${SECS}s"
echo "╠═══╦════════════════════════════════════╦══════╦═════╦══════════════════╣"
printf "║ %-2s║ %-36s║ %-6s║ %-5s║ %-18s║\n" \
       "#" "测试项" "结果" "耗时" "峰值p99μs"
echo "╠═══╬════════════════════════════════════╬══════╬═════╬══════════════════╣"

for i in $(seq 1 10); do
    n="${TEST_NAMES[$i]:-?}"
    s="${TEST_STATUS[$i]:-?}"
    e="${TEST_ELAPSED[$i]:-?}"
    p="${TEST_P99[$i]:-?}"
    if [ "$s" = "PASS" ]; then
        sc="${G}PASS${N}"
    else
        sc="${R}FAIL${N}"
    fi
    printf "║ %-2s║ %-36s║ %b  ║ %3ds║ %17sμ║\n" \
           "$i" "${n:0:36}" "$sc" "$e" "$p"
done

echo "╠═══╩════════════════════════════════════╩══════╩═════╩══════════════════╣"
echo "║                                                                          ║"
printf "║  %-74s  ║\n" "  端到端 p99 (perf_latency.sh, 500样本, 32并发 pgbench):"
printf "║    %-72s  ║\n" "  p99 = ${E2E_P99} ms    avg = ${E2E_AVG} ms    判定: ${E2E_STATUS}"
echo "║                                                                          ║"
if [ $FAIL_TOTAL -eq 0 ]; then
    printf "║  ${G}%-74s${N}  ║\n" "  最终结果: 全部通过 ✓   PASS = ${PASS_TOTAL}   FAIL = ${FAIL_TOTAL}"
else
    printf "║  ${R}%-74s${N}  ║\n" "  最终结果: 存在失败 ✗   PASS = ${PASS_TOTAL}   FAIL = ${FAIL_TOTAL}"
fi
echo "╚══════════════════════════════════════════════════════════════════════════╝"
echo ""

[ $FAIL_TOTAL -eq 0 ] && exit 0 || exit 1
