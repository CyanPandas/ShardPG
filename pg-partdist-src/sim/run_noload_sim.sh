#!/usr/bin/env bash
# run_noload_sim.sh — 无负载环境下顺序执行全部 10 项测试
# 在宿主机执行，通过 docker exec 与容器交互

set -uo pipefail

CONTAINER="pg-citus-cluster-container"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

PASS_TOTAL=0
FAIL_TOTAL=0
START_TIME=$(date +%s)

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'; BOLD='\033[1m'

banner() {
    echo -e "\n${BOLD}╔══════════════════════════════════════════════════════╗${NC}"
    printf   "${BOLD}║  %-52s  ║${NC}\n" "$1"
    echo -e  "${BOLD}╚══════════════════════════════════════════════════════╝${NC}"
}

dpsql() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -U postgres "$@"; }

# 确保集群在线且所有节点退出 recovery 模式
ensure_cluster() {
    for port in 5432 5433 5434; do
        dpsql -p "$port" -d postgres -At -c "SELECT 1" >/dev/null 2>&1 || \
            docker exec -u postgres "$CONTAINER" bash -c \
                "/work/pg-install/bin/pg_ctl start -D /work/pg-cluster-data/$([ $port -eq 5432 ] && echo master || ([ $port -eq 5433 ] && echo worker1 || echo worker2)) -w -l /dev/null" \
                >/dev/null 2>&1 || true
    done
    # 等待所有节点退出 recovery 模式（kill -9 后可能仍在 crash recovery）
    for port in 5432 5433 5434; do
        local tries=0
        until dpsql -p "$port" -d postgres -At \
                -c "SELECT NOT pg_is_in_recovery()" 2>/dev/null | grep -q "^t$"; do
            sleep 0.5; tries=$((tries+1))
            [ $tries -gt 60 ] && break
        done
    done
    sleep 1
}

run_test() {
    local num=$1 name=$2 cmd=$3 host=${4:-false}
    local rfile="/tmp/noload_test_result_${num}.log"

    printf "\n${YELLOW}[%2d/10]${NC} ${BOLD}%s${NC}\n" "$num" "$name"
    printf "       运行中..."

    ensure_cluster

    local t0=$(date +%s) RC=0
    if [ "$host" = "true" ]; then
        eval "$cmd" > "$rfile" 2>&1 || RC=$?
    else
        docker exec -u postgres "$CONTAINER" bash -c \
            "export PATH=/work/pg-install/bin:\$PATH; cd /work/pg-partdist-src; $cmd" \
            > "$rfile" 2>&1 || RC=$?
    fi
    local elapsed=$(( $(date +%s) - t0 ))

    local ipass ifail
    ipass=$(grep -Eo 'PASS[=: ]+[0-9]+|通过: [0-9]+|PASSED: [0-9]+|Tests passed: [0-9]+' \
            "$rfile" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo '?')
    ifail=$(grep -Eo 'FAIL[=: ]+[0-9]+|失败: [0-9]+|FAILED: [0-9]+|Tests failed: [0-9]+' \
            "$rfile" 2>/dev/null | grep -oE '[0-9]+' | tail -1 || echo '?')

    if [ $RC -eq 0 ]; then
        printf "\r       ${GREEN}PASS${NC}  %3ds  (%s✓/%s✗)\n" "$elapsed" "$ipass" "$ifail"
        PASS_TOTAL=$((PASS_TOTAL+1))
    else
        printf "\r       ${RED}FAIL${NC}  %3ds  (%s✓/%s✗)\n" "$elapsed" "$ipass" "$ifail"
        FAIL_TOTAL=$((FAIL_TOTAL+1))
        echo "       ── 末10行日志 ──"
        tail -10 "$rfile" | sed 's/^/       /'
    fi
}

banner "无负载环境 — 10 项测试"

run_test 1  "写入连续性 & 崩溃恢复"           "bash tests/verify_continuity_and_crash.sh 2>&1"
run_test 2  "分片自动初始化 (含37项回归)"      "bash tests/test_shard_auto_init.sh 2>&1"
run_test 3  "多分布表隔离性 & 持久性"          "bash tests/test_multi_table_isolation.sh 2>&1"
run_test 4  "崩溃恢复专项 (A/B/C三场景)"       "bash tests/test_crash_recovery.sh 2>&1"
run_test 5  "批量写入 COPY 路径恢复"           "bash tests/test_bulk_insert_recovery.sh 2>&1"
run_test 6  "跨段边界 LSN 连续性"              "bash tests/test_segment_boundary_lsn.sh 2>&1"
run_test 7  "段文件损坏恢复 (C1-C4)"           "bash tests/test_corrupt_segment_recovery.sh 2>&1"
run_test 8  "Demux 高积压崩溃恢复 (S1-S5)"    "bash tests/test_demux_backlog_recovery.sh 2>&1"
run_test 9  "端到端延迟 p99 (500样本,32并发)" "bash \"${SCRIPT_DIR}/../tests/perf_latency.sh\" 2>&1" "true"
run_test 10 "磁盘满 ENOSPC 容错恢复"          "bash tests/test_enospc_recovery.sh 2>&1"

TOTAL_ELAPSED=$(( $(date +%s) - START_TIME ))
banner "无负载测试汇总"
printf "  总用时: %dm %ds\n" $((TOTAL_ELAPSED/60)) $((TOTAL_ELAPSED%60))
printf "  ${GREEN}PASS = %d${NC}  ${RED}FAIL = %d${NC}\n" "$PASS_TOTAL" "$FAIL_TOTAL"

if [ "$FAIL_TOTAL" -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}最终结果: 全部通过 ✓${NC}"
    exit 0
else
    echo -e "  ${RED}${BOLD}最终结果: 存在失败 ✗${NC}"
    exit 1
fi
