#!/bin/bash
# run_production_sim.sh
#
# 生产环境模拟测试驱动器（在宿主机执行，通过 docker exec 与容器交互）
# 步骤：
#   1. 调整 PostgreSQL 生产级参数
#   2. 预写入 50,000 行背景数据（8 分片分布表）
#   3. 启动 5 路并发背景负载（写 + 更新 + 读，自动重连）
#   4. 在背景负载运行期间顺序执行全部 10 项测试
#   5. 停止背景负载，恢复默认参数，汇总结果

set -uo pipefail

CONTAINER="pg-citus-cluster-container"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

PASS_TOTAL=0
FAIL_TOTAL=0
START_TIME=$(date +%s)

BG_FLAG=/tmp/prod_sim_running
BG_PIDS_FILE=/tmp/prod_sim_pids

# ── 颜色 ───────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'
BOLD='\033[1m'

banner() { echo -e "\n${BOLD}╔══════════════════════════════════════════════════════╗${NC}"; \
           printf "${BOLD}║  %-52s  ║${NC}\n" "$1"; \
           echo -e "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"; }

# 容器内 psql 封装
dpsql() {
    docker exec -u postgres "$CONTAINER" \
        /work/pg-install/bin/psql -U postgres "$@"
}

# ── 清理函数 ───────────────────────────────────────────────────────────
cleanup() {
    rm -f "$BG_FLAG"
    if [ -f "$BG_PIDS_FILE" ]; then
        while read -r pid; do kill "$pid" 2>/dev/null || true; done < "$BG_PIDS_FILE"
        rm -f "$BG_PIDS_FILE"
    fi
    rm -f /tmp/bg_tps_*.log
}
trap cleanup EXIT

# ════════════════════════════════════════════════════════════════════════
banner "阶段 0 — 生产参数配置"
# ════════════════════════════════════════════════════════════════════════

apply_prod_config() {
    local port=$1
    dpsql -p "$port" -d postgres -q -c "
        ALTER SYSTEM SET checkpoint_completion_target = '0.9';
        ALTER SYSTEM SET max_wal_size              = '2GB';
        ALTER SYSTEM SET min_wal_size              = '256MB';
        ALTER SYSTEM SET wal_compression           = 'on';
        ALTER SYSTEM SET log_checkpoints           = 'on';
        ALTER SYSTEM SET log_connections           = 'off';
        ALTER SYSTEM SET log_min_duration_statement = '5000';
        ALTER SYSTEM SET effective_cache_size      = '512MB';
        ALTER SYSTEM SET random_page_cost          = '1.1';
        SELECT pg_reload_conf();
    " > /dev/null 2>&1 || true
}

for port in 5432 5433 5434; do
    apply_prod_config $port
    echo "  port $port — production params applied"
done

# ════════════════════════════════════════════════════════════════════════
banner "阶段 1 — 预写入 50,000 行背景数据"
# ════════════════════════════════════════════════════════════════════════

dpsql -p 5432 -d postgres -q -c "
    DROP TABLE IF EXISTS prod_bg CASCADE;
    CREATE TABLE prod_bg (
        id      BIGSERIAL,
        bucket  INT NOT NULL,
        payload TEXT,
        ts      TIMESTAMPTZ DEFAULT NOW()
    );
    SELECT create_distributed_table('prod_bg', 'bucket', shard_count => 8);
" 2>&1 | grep -v '^$' | grep -v '^(' || true

dpsql -p 5432 -d postgres -q -c "
    INSERT INTO prod_bg (bucket, payload)
    SELECT (i % 10000), 'seed_payload_' || i
    FROM generate_series(1, 50000) i;
" > /dev/null

ROW_W1=$(dpsql -p 5433 -d postgres -At -c "
    SELECT SUM(c) FROM (
        SELECT relname, pg_class.reltuples::bigint c
        FROM pg_class WHERE relname LIKE 'prod_bg_%'
    ) t;" 2>/dev/null || echo '?')
echo "  50,000 行已写入 (worker1 本地行数估计: ~$ROW_W1)"

# ════════════════════════════════════════════════════════════════════════
banner "阶段 2 — 启动背景并发负载"
# ════════════════════════════════════════════════════════════════════════

touch "$BG_FLAG"
> "$BG_PIDS_FILE"

# Worker A — 批量 INSERT  (20 行 / 150ms)
(
    CNT=0
    while [ -f "$BG_FLAG" ]; do
        docker exec -u postgres "$CONTAINER" \
            /work/pg-install/bin/psql -U postgres -p 5432 -d postgres -q -c \
            "INSERT INTO prod_bg(bucket,payload) SELECT (random()*9999)::int, 'bgA_'||clock_timestamp() FROM generate_series(1,20);" \
            >/dev/null 2>&1 || true
        CNT=$((CNT+20))
        echo $CNT > /tmp/bg_tps_A.log
        sleep 0.15
    done
) &
echo $! >> "$BG_PIDS_FILE"

# Worker B — 单行 INSERT  (1 行 / 50ms，模拟 OLTP 事务)
(
    CNT=0
    while [ -f "$BG_FLAG" ]; do
        docker exec -u postgres "$CONTAINER" \
            /work/pg-install/bin/psql -U postgres -p 5432 -d postgres -q -c \
            "INSERT INTO prod_bg(bucket,payload) VALUES ((random()*9999)::int, 'bgB_'||now()::text);" \
            >/dev/null 2>&1 || true
        CNT=$((CNT+1))
        echo $CNT > /tmp/bg_tps_B.log
        sleep 0.05
    done
) &
echo $! >> "$BG_PIDS_FILE"

# Worker C — UPDATE (模拟写竞争)
(
    while [ -f "$BG_FLAG" ]; do
        docker exec -u postgres "$CONTAINER" \
            /work/pg-install/bin/psql -U postgres -p 5432 -d postgres -q -c \
            "UPDATE prod_bg SET ts=NOW() WHERE id = (SELECT id FROM prod_bg ORDER BY random() LIMIT 1);" \
            >/dev/null 2>&1 || true
        sleep 0.2
    done
) &
echo $! >> "$BG_PIDS_FILE"

# Worker D — SELECT 扫描  (模拟分析查询)
(
    while [ -f "$BG_FLAG" ]; do
        docker exec -u postgres "$CONTAINER" \
            /work/pg-install/bin/psql -U postgres -p 5432 -d postgres -q -c \
            "SELECT bucket, COUNT(*) FROM prod_bg WHERE bucket % 100 = 0 GROUP BY bucket LIMIT 20;" \
            >/dev/null 2>&1 || true
        sleep 0.3
    done
) &
echo $! >> "$BG_PIDS_FILE"

# Worker E — worker2 直写 (模拟 worker 本地压力)
(
    while [ -f "$BG_FLAG" ]; do
        docker exec -u postgres "$CONTAINER" \
            /work/pg-install/bin/psql -U postgres -p 5434 -d postgres -q -c \
            "SELECT count(*) FROM pg_stat_activity;" \
            >/dev/null 2>&1 || true
        sleep 0.4
    done
) &
echo $! >> "$BG_PIDS_FILE"

sleep 2  # 让背景负载稳定

echo "  5 路背景 Worker 已启动 (PIDs: $(tr '\n' ' ' < $BG_PIDS_FILE))"
BG_A=$(cat /tmp/bg_tps_A.log 2>/dev/null || echo 0)
BG_B=$(cat /tmp/bg_tps_B.log 2>/dev/null || echo 0)
echo "  当前背景写入量 — Worker A: $BG_A 行, Worker B: $BG_B 行"

# ════════════════════════════════════════════════════════════════════════
banner "阶段 3 — 执行 10 项测试"
# ════════════════════════════════════════════════════════════════════════

run_test() {
    local num="$1"
    local name="$2"
    local cmd="$3"
    local host_cmd="${4:-false}"   # true = 在宿主机执行（perf_latency.sh）

    local t0=$(date +%s)
    local BGA0=$(cat /tmp/bg_tps_A.log 2>/dev/null || echo 0)
    local BGB0=$(cat /tmp/bg_tps_B.log 2>/dev/null || echo 0)

    echo -e "\n${YELLOW}[$num/10] $name${NC}"
    echo -n "    执行中..."

    local result_file="/tmp/test_result_${num}.log"
    if [ "$host_cmd" = "true" ]; then
        eval "$cmd" > "$result_file" 2>&1
        RC=$?
    else
        docker exec -u postgres "$CONTAINER" bash -c "
            export PATH=/work/pg-install/bin:\$PATH
            cd /work/pg-partdist-src
            $cmd
        " > "$result_file" 2>&1
        RC=$?
    fi

    local t1=$(date +%s)
    local elapsed=$((t1 - t0))
    local BGA1=$(cat /tmp/bg_tps_A.log 2>/dev/null || echo 0)
    local BGB1=$(cat /tmp/bg_tps_B.log 2>/dev/null || echo 0)
    local bg_delta=$((BGA1 - BGA0 + BGB1 - BGB0))

    local inner_pass=$(grep -Eo 'PASS[=: ]+[0-9]+|通过: [0-9]+|PASSED: [0-9]+|Tests passed: [0-9]+' "$result_file" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo '?')
    local inner_fail=$(grep -Eo 'FAIL[=: ]+[0-9]+|失败: [0-9]+|FAILED: [0-9]+|Tests failed: [0-9]+' "$result_file" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo '?')

    if [ $RC -eq 0 ]; then
        echo -e "\r    ${GREEN}PASS${NC}  (${elapsed}s, 内部 ${inner_pass}✓/${inner_fail}✗, 背景+${bg_delta}行)"
        PASS_TOTAL=$((PASS_TOTAL+1))
    else
        echo -e "\r    ${RED}FAIL${NC}  (${elapsed}s, 内部 ${inner_pass}✓/${inner_fail}✗, 背景+${bg_delta}行)"
        FAIL_TOTAL=$((FAIL_TOTAL+1))
        echo "    ── 失败日志 (末 10 行) ──"
        tail -10 "$result_file" | sed 's/^/    /'
    fi
}

run_test 1 "写入连续性 & 崩溃恢复"            "bash verify_continuity_and_crash.sh 2>&1"
run_test 2 "分片自动初始化 (含37项回归)"       "bash test_shard_auto_init.sh 2>&1"
run_test 3 "多分布表隔离性 & 持久性"           "bash test_multi_table_isolation.sh 2>&1"
run_test 4 "崩溃恢复专项 (A/B/C三场景)"        "bash test_crash_recovery.sh 2>&1"
run_test 5 "批量写入 COPY 路径恢复"            "BULK_OVERHEAD_THRESHOLD=100 bash test_bulk_insert_recovery.sh 2>&1"
run_test 6 "跨段边界 LSN 连续性"               "bash test_segment_boundary_lsn.sh 2>&1"
run_test 7 "段文件损坏恢复 (C1-C4)"            "bash test_corrupt_segment_recovery.sh 2>&1"
run_test 8 "Demux 高积压崩溃恢复 (S1-S5)"     "bash test_demux_backlog_recovery.sh 2>&1"

# 确保节点在线（前几个 crash 测试可能重启了节点）
docker exec -u postgres "$CONTAINER" bash -c "
export PATH=/work/pg-install/bin:\$PATH
for d in master worker1 worker2; do
    port=\$([ \$d = master ] && echo 5432 || [ \$d = worker1 ] && echo 5433 || echo 5434)
    pg_ctl status -D /work/pg-cluster-data/\$d >/dev/null 2>&1 || \
        pg_ctl start -D /work/pg-cluster-data/\$d -l /work/pg-cluster-data/\$d/pg.log \
            -o \"-p \$port\" -w -t 30 >/dev/null 2>&1
done
" > /dev/null 2>&1 || true
sleep 2

run_test 9 "端到端延迟性能 (500样本, 32并发)" "bash \"${SCRIPT_DIR}/perf_latency.sh\" 2>&1" "true"

docker exec -u postgres "$CONTAINER" bash -c "
export PATH=/work/pg-install/bin:\$PATH
pg_ctl status -D /work/pg-cluster-data/worker1 >/dev/null 2>&1 || \
    pg_ctl start -D /work/pg-cluster-data/worker1 -l /work/pg-cluster-data/worker1/pg.log \
        -o '-p 5433' -w -t 30 >/dev/null 2>&1
" > /dev/null 2>&1 || true

run_test 10 "磁盘满 ENOSPC 容错恢复" "bash test_enospc_recovery.sh 2>&1"

# ════════════════════════════════════════════════════════════════════════
banner "阶段 4 — 停止背景负载 & 统计"
# ════════════════════════════════════════════════════════════════════════

rm -f "$BG_FLAG"
sleep 1

BGA_FINAL=$(cat /tmp/bg_tps_A.log 2>/dev/null || echo 0)
BGB_FINAL=$(cat /tmp/bg_tps_B.log 2>/dev/null || echo 0)
TOTAL_BG=$((BGA_FINAL + BGB_FINAL))
echo "  背景总写入量: $TOTAL_BG 行"

docker exec -u postgres "$CONTAINER" bash -c "
export PATH=/work/pg-install/bin:\$PATH
psql -p 5433 -d postgres -At -c '
    SELECT COUNT(*) FROM pg_ls_dir(
        (SELECT setting||\"/pg_parwal\" FROM pg_settings WHERE name=\"data_directory\")
    );' 2>/dev/null || echo '?'
" 2>/dev/null | xargs -I{} echo "  worker1 pg_parwal 目录数: {}"

# ════════════════════════════════════════════════════════════════════════
banner "阶段 5 — 恢复默认参数"
# ════════════════════════════════════════════════════════════════════════

for port in 5432 5433 5434; do
    dpsql -p "$port" -d postgres -q -c "
        ALTER SYSTEM RESET checkpoint_completion_target;
        ALTER SYSTEM RESET max_wal_size;
        ALTER SYSTEM RESET min_wal_size;
        ALTER SYSTEM RESET wal_compression;
        ALTER SYSTEM RESET log_checkpoints;
        ALTER SYSTEM RESET log_connections;
        ALTER SYSTEM RESET log_min_duration_statement;
        ALTER SYSTEM RESET effective_cache_size;
        ALTER SYSTEM RESET random_page_cost;
        SELECT pg_reload_conf();
    " > /dev/null 2>&1 || true
    echo "  port $port — 参数已重置"
done

# ════════════════════════════════════════════════════════════════════════
END_TIME=$(date +%s)
TOTAL_ELAPSED=$((END_TIME - START_TIME))
MINS=$((TOTAL_ELAPSED/60)); SECS=$((TOTAL_ELAPSED%60))

echo ""
echo "╔═══════════════════════════════════════════════════════╗"
printf "║  %-53s  ║\n" "生产环境模拟测试 — 最终报告"
echo "╠═══════════════════════════════════════════════════════╣"
printf "║  %-53s  ║\n" "环境: PostgreSQL 16 + Citus + pg_partdist"
printf "║  %-53s  ║\n" "背景负载: 5路并发 (批量INSERT/单行INSERT/UPDATE/SELECT)"
printf "║  %-53s  ║\n" "背景写入总量: ${TOTAL_BG} 行"
printf "║  %-53s  ║\n" "总耗时: ${MINS}m ${SECS}s"
echo "╠═══════════════════════════════════════════════════════╣"
if [ $FAIL_TOTAL -eq 0 ]; then
    printf "║  ${GREEN}%-53s${NC}  ║\n" "结果: 全部通过 ✓  PASS=$PASS_TOTAL  FAIL=$FAIL_TOTAL"
else
    printf "║  ${RED}%-53s${NC}  ║\n" "结果: 存在失败 ✗  PASS=$PASS_TOTAL  FAIL=$FAIL_TOTAL"
fi
echo "╚═══════════════════════════════════════════════════════╝"
echo ""

[ $FAIL_TOTAL -eq 0 ] && exit 0 || exit 1
