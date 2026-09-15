#!/usr/bin/env bash
# [宿主机] P7-W7 高并发写入健壮性验收：多客户端并发写分布表时，写路径不得出现
#   ① fileset 持久化竞争（"无法就位 fileset 文件 … No such file or directory"）
#   ② group-commit 回读 peer 槽位失败（"无法读取 peer WAL 记录" / "排空时 … 无法从 pg_wal 回读"）
#   ③ 空 DATA 记录（"data_len=0"）、节点崩溃
#
# 2026-09-15 起因：Codex 复核指出 0380af2 在高并发多机写入下"fileset 异常、peer-WAL 异常、
# 延迟暴涨"。在 pg-test（1c+8w，e2-medium 2 vCPU）用 32 客户端 60 s 复现：
#   修复前：fileset rename 失败 90 次、peer WAL 回读失败 42 次（每次往流里写一条空记录，
#           副本回放到它只打 WARNING 就跳过 —— 静默分叉）；
#   修复后：两者 0 次（同负载）。
# 延迟（p99 4.2 s）经等待事件采样判为 2 vCPU 机器饱和（8 客户端 idle 已 0%，86% ClientRead、
# LWLock:pg_partdist_sync 2%），不是写路径锁队列 —— 本套件不断言延迟，只打印。
#
# 判据：本轮时间窗内上述告警**增量为 0**（不是绝对数：节点日志跨轮累积）+ 无节点崩溃。
# 负载默认 16 客户端 40 s（门禁里跑），CLIENTS/DUR 可覆盖。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
CLIENTS="${CLIENTS:-16}"; JOBS="${JOBS:-2}"; DUR="${DUR:-40}"
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@" </dev/null; }
PSQLIN() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }

check() {  # check <名字> <实际> <期望>
  if [[ -n "$2" && "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

# 本轮要盯的告警：名字 | 日志里的指纹（grep -F 逐字）
PATTERNS=(
  "fileset 就位失败|无法就位 fileset 文件"
  "fileset 创建失败|无法创建 fileset 临时文件"
  "peer WAL 回读失败（逐条）|无法读取 peer WAL 记录"
  "peer WAL 回读失败（汇总）|无法从 pg_wal 回读、未写入分区流"
  "装配读页失败|装配原始 WAL 记录时读页失败"
  "端点不符|peer WAL 记录端点不符"
  "空 DATA 记录|data_len=0"
)
count_pat() {   # 全部节点 pg.log 里某指纹的总数（绝对数，取差值用）
  local pat=$1
  docker exec -u postgres "$CONTAINER" bash -c "cd /work/pg-cluster-data && grep -a -h -F -- '$pat' */pg.log 2>/dev/null | wc -l"
}
declare -A BEFORE
for e in "${PATTERNS[@]}"; do BEFORE["${e%%|*}"]=$(count_pat "${e#*|}"); done

echo "========== [1] 夹具：16 分片分布表 + 5 万行种子 =========="
PSQLIN $COORD -v ON_ERROR_STOP=1 -q <<'SQL' || { echo "建表失败"; exit 1; }
DROP TABLE IF EXISTS w7_hl;
SET citus.shard_count = 16;
SET citus.shard_replication_factor = 1;
CREATE TABLE w7_hl(id bigint primary key, k int, v text);
SELECT create_distributed_table('w7_hl', 'id');
INSERT INTO w7_hl SELECT g, g % 100, repeat('x', 120) FROM generate_series(1, 50000) g;
SQL
nrow=$(PSQL $COORD -Atc "SELECT count(*) FROM w7_hl")
check "种子 50000 行" "$nrow" "50000"
nsh=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='w7_hl'::regclass")
check "16 个分片" "$nsh" "16"

DEX bash -c 'mkdir -p /tmp/w7hl && rm -f /tmp/w7hl/lat* && cat > /tmp/w7hl/ins.sql <<EOF
\set id random(50001, 2000000000)
INSERT INTO w7_hl VALUES (:id, 1, repeat('"'"'x'"'"', 120)) ON CONFLICT DO NOTHING;
EOF
cat > /tmp/w7hl/upd.sql <<EOF
\set id random(1, 50000)
UPDATE w7_hl SET k = k + 1 WHERE id = :id;
EOF'

echo "========== [2] pgbench -c $CLIENTS -j $JOBS -T $DUR（70% INSERT / 30% UPDATE）=========="
out=$(DEX bash -c "cd /tmp/w7hl && /work/pg-install/bin/pgbench -h /tmp -p 5432 -U postgres -d postgres -n -c $CLIENTS -j $JOBS -T $DUR -f /tmp/w7hl/ins.sql@7 -f /tmp/w7hl/upd.sql@3 -l --log-prefix=/tmp/w7hl/lat" 2>&1)
ntx=$(printf '%s\n' "$out" | sed -n 's/^number of transactions actually processed: \([0-9]*\).*/\1/p' | head -1)
tps=$(printf '%s\n' "$out" | sed -n 's/^tps = \([0-9.]*\).*/\1/p' | head -1)
check "负载确实跑起来了（事务数 ${ntx:-0} > 0）" "$([[ "${ntx:-0}" -gt 0 ]] && echo ok)" "ok"
# 非"事务失败"的客户端中断（如 TSO 取号失败）只打印：那是协调者 max_connections 被打满，环境项
nab=$(printf '%s\n' "$out" | grep -c "aborted in command")
echo "    tps=${tps:-?}  客户端中断=${nab}（>0 多半是协调者 max_connections 打满 → TSO 取 commit_ts 失败）"
DEX bash -c 'cat /tmp/w7hl/lat* 2>/dev/null | awk "{print \$3}" | sort -n' | awk '{a[NR]=$1} END{if (NR==0) {print "    延迟：无样本"; exit} printf "    延迟 n=%d p50=%.0f p95=%.0f p99=%.0f max=%.0f ms（只打印，不断言：2 vCPU 机器饱和）\n", NR, a[int(NR*0.5)]/1000, a[int(NR*0.95)]/1000, a[int(NR*0.99)]/1000, a[NR]/1000}'

echo "========== [3] 写路径告警增量必须为 0 =========="
for e in "${PATTERNS[@]}"; do
  name="${e%%|*}"; pat="${e#*|}"
  after=$(count_pat "$pat"); delta=$(( after - BEFORE[$name] ))
  check "本轮无「${name}」（${BEFORE[$name]} → ${after}）" "$delta" "0"
done
echo "    fileset.tmp 残留: $(docker exec -u postgres "$CONTAINER" bash -c 'ls /work/pg-cluster-data/*/pg_parwal/*/fileset.tmp* 2>/dev/null | wc -l')"

echo "========== [4] 节点健康 =========="
health_check_no_crash
health_check_worker_pool

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "W7 高并发写入验收：全部通过"; else echo "W7 高并发写入验收：存在 FAIL"; fi

if [[ "${KEEP_FIXTURE:-0}" != "1" ]]; then
  PSQL $COORD -q -c "DROP TABLE IF EXISTS w7_hl;" >/dev/null 2>&1
  DEX bash -c 'rm -rf /tmp/w7hl' >/dev/null 2>&1
fi
exit $FAIL
