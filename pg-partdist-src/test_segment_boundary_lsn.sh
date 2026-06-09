#!/bin/bash
# test_segment_boundary_lsn.sh
# 跨段切换边界 partition_lsn 连续性严格验证
#
# 目标：
#   1. 跨段切换时 partition_lsn 严格递增（diff = 1）
#   2. verify_partition_wal(oid) 始终返回 true
#   3. ≥5 次段切换后 count_parwal_records = 总写入数
#
# 测试方法：
#   - 在 worker1 上直接调用 write_partition_wal_record() 写入记录
#   - 每批写入后调用 pg_switch_wal() 强制段切换（共 6 次，≥5 次要求）
#   - 调用 demux_flush() 等待 Demux Worker 将 WAL 路由到 pg_parwal
#   - 通过 SQL + 二进制解析双重验证跨段 partition_lsn 连续性

set -uo pipefail

export PATH=/work/pg-install/bin:$PATH

COORD_PORT=5432
W1_PORT=5433
W2_PORT=5434
W1_DATA=/work/pg-cluster-data/worker1
W2_DATA=/work/pg-cluster-data/worker2

TEST_OID=99001          # 用于测试的虚拟 Oid（无需真实关系）
INSERTS_PER_BATCH=5     # 每批写入的 PartWALHeader 记录数
SWITCHES=6              # WAL 段切换次数（≥5）
TOTAL_BATCHES=$((SWITCHES + 1))
EXPECTED_RECORDS=$((TOTAL_BATCHES * INSERTS_PER_BATCH))
EXPECTED_FILES=$TOTAL_BATCHES

PASS=0
FAIL=0

pass() { echo "[PASS] $1"; PASS=$((PASS+1)); }
fail() { echo "[FAIL] $1"; FAIL=$((FAIL+1)); }
info() { echo "       $1"; }

# Worker1 上执行 SQL，返回纯文本结果
qw1() { psql -h localhost -p "$W1_PORT" -U postgres -d postgres -t -A -c "$1" 2>&1; }

echo "=================================================="
echo " test_segment_boundary_lsn.sh"
echo " 测试 OID    : $TEST_OID"
echo " 段切换次数  : $SWITCHES  (≥5 要求)"
echo " 每批记录数  : $INSERTS_PER_BATCH"
echo " 总批次      : $TOTAL_BATCHES"
echo " 预期总记录  : $EXPECTED_RECORDS"
echo " 预期段文件  : $EXPECTED_FILES"
echo "=================================================="

# ===========================================================
# 阶段 1：准备
# ===========================================================
echo ""
echo "===== 阶段 1：准备 ====="

# 重置 OID 99001 的所有 pg_parwal 数据和内存计数器
qw1 "SELECT partdist.reset_partition_wal_state($TEST_OID::oid)" >/dev/null 2>&1 || {
    rm -rf "$W1_DATA/pg_parwal/$TEST_OID" 2>/dev/null || true
    info "reset_partition_wal_state 失败，已手动清空目录"
}
info "OID $TEST_OID 状态重置完成"

# ===========================================================
# 阶段 2：写入记录 + 强制段切换
# ===========================================================
echo ""
echo "===== 阶段 2：写入记录 + 强制段切换（$SWITCHES 次）====="

TOTAL_WRITTEN=0

for i in $(seq 1 $TOTAL_BATCHES); do
    # 写入 INSERTS_PER_BATCH 条 PartWALHeader 记录（带重试，防高负载连接失败）
    for j in $(seq 1 $INSERTS_PER_BATCH); do
        _tries=0
        until qw1 "SELECT partdist.write_partition_wal_record($TEST_OID::oid, 1)" >/dev/null 2>&1; do
            _tries=$((_tries+1)); [[ $_tries -ge 6 ]] && break; sleep 0.3
        done
    done
    TOTAL_WRITTEN=$((TOTAL_WRITTEN + INSERTS_PER_BATCH))

    if [[ $i -lt $TOTAL_BATCHES ]]; then
        # 强制 WAL 段切换
        NEW_SEG=$(qw1 "SELECT pg_switch_wal()" 2>/dev/null)
        echo "  批次 $i/$TOTAL_BATCHES: 写入 $INSERTS_PER_BATCH 条  →  段切换完成 (新段起始: $NEW_SEG)"
    else
        echo "  批次 $i/$TOTAL_BATCHES: 写入 $INSERTS_PER_BATCH 条  (最终批次，无切换)"
    fi
done

# 等待 Demux Worker 处理完所有 WAL（含最后一批）
echo ""
info "等待 Demux Worker 完成 WAL 路由..."
qw1 "SELECT partdist.demux_flush()" >/dev/null 2>&1
info "demux_flush() 完成，总写入: $TOTAL_WRITTEN 条"

# 统计实际产生的段文件数
SEG_FILES=$(ls "$W1_DATA/pg_parwal/$TEST_OID/" 2>/dev/null | wc -l | tr -d ' ')
info "pg_parwal/$TEST_OID/ 段文件数: $SEG_FILES（期望 ≥$SWITCHES）"

# ===========================================================
# 阶段 3：SQL 函数验证
# ===========================================================
echo ""
echo "===== 阶段 3：SQL 函数验证 ====="

# T1: verify_partition_wal
VERIFY=$(qw1 "SELECT partdist.verify_partition_wal($TEST_OID::oid)" 2>&1 | tr -d ' \r')
if [[ "$VERIFY" == "t" ]]; then
    pass "T1  verify_partition_wal($TEST_OID) = true"
else
    fail "T1  verify_partition_wal($TEST_OID) = '$VERIFY'  (期望 t)"
fi

# T2: count_parwal_records == EXPECTED_RECORDS
COUNT=$(qw1 "SELECT partdist.count_parwal_records($TEST_OID::oid)" 2>&1 | tr -d ' \r')
if [[ "$COUNT" == "$EXPECTED_RECORDS" ]]; then
    pass "T2  count_parwal_records = $COUNT  (= 预期 $EXPECTED_RECORDS)"
else
    fail "T2  count_parwal_records = $COUNT  (期望 $EXPECTED_RECORDS)"
fi

# T3: SQL 全局 gap 检查 — partition_lsn 序列无断裂
GAP_COUNT=$(qw1 "
WITH rec AS (
    SELECT partition_lsn,
           lag(partition_lsn) OVER (ORDER BY partition_lsn) AS prev_plsn
    FROM partdist.check_partition_wal($TEST_OID::oid)
    WHERE is_valid
)
SELECT count(*)
FROM rec
WHERE prev_plsn IS NOT NULL
  AND partition_lsn - prev_plsn != 1
" 2>&1 | tr -d ' \r')

if [[ "$GAP_COUNT" == "0" ]]; then
    pass "T3  SQL gap 检查：partition_lsn 全段无断裂（gap 数 = 0）"
else
    fail "T3  SQL gap 检查：发现 $GAP_COUNT 处断裂"
fi

# T4: 段边界处 diff = 1（通过 pg_walfile_name_offset 识别段切换）
echo ""
echo "  [LSN 边界样本]"

# 展示边界处的 partition_lsn 序列
qw1 "
WITH rec AS (
    SELECT partition_lsn,
           orig_node_lsn,
           (pg_walfile_name_offset(orig_node_lsn)).file_name AS wal_file,
           lag(partition_lsn)  OVER (ORDER BY partition_lsn) AS prev_plsn,
           lag(orig_node_lsn)  OVER (ORDER BY partition_lsn) AS prev_orig,
           lag((pg_walfile_name_offset(orig_node_lsn)).file_name)
               OVER (ORDER BY partition_lsn) AS prev_wal_file
    FROM partdist.check_partition_wal($TEST_OID::oid)
    WHERE is_valid
),
boundaries AS (
    SELECT partition_lsn AS cur_plsn,
           prev_plsn,
           wal_file AS cur_file,
           prev_wal_file,
           orig_node_lsn AS cur_orig,
           prev_orig,
           (partition_lsn - prev_plsn)::int AS diff,
           row_number() OVER (ORDER BY partition_lsn) AS bn
    FROM rec
    WHERE prev_wal_file IS NOT NULL
      AND wal_file != prev_wal_file
)
SELECT '  边界 #' || bn
    || ': plsn ' || prev_plsn || ' → ' || cur_plsn
    || '  diff=' || diff
    || '  WAL[' || substring(prev_wal_file FROM 17) || ' → ' || substring(cur_file FROM 17) || ']'
    || CASE WHEN diff = 1 THEN '  ✔' ELSE '  ✘ FAIL' END
FROM boundaries
ORDER BY cur_plsn
" 2>&1 | grep -E '^\s+边界' | while IFS= read -r line; do echo "$line"; done

echo ""

# 计算边界总数和坏边界数
BAD_BOUNDS=$(qw1 "
WITH rec AS (
    SELECT partition_lsn,
           (pg_walfile_name_offset(orig_node_lsn)).file_name AS wal_file,
           lag(partition_lsn) OVER (ORDER BY partition_lsn) AS prev_plsn,
           lag((pg_walfile_name_offset(orig_node_lsn)).file_name)
               OVER (ORDER BY partition_lsn) AS prev_wal_file
    FROM partdist.check_partition_wal($TEST_OID::oid)
    WHERE is_valid
)
SELECT count(*) FROM rec
WHERE prev_wal_file IS NOT NULL
  AND wal_file != prev_wal_file
  AND partition_lsn - prev_plsn != 1
" 2>&1 | tr -d ' \r')

GOOD_BOUNDS=$(qw1 "
WITH rec AS (
    SELECT partition_lsn,
           (pg_walfile_name_offset(orig_node_lsn)).file_name AS wal_file,
           lag(partition_lsn) OVER (ORDER BY partition_lsn) AS prev_plsn,
           lag((pg_walfile_name_offset(orig_node_lsn)).file_name)
               OVER (ORDER BY partition_lsn) AS prev_wal_file
    FROM partdist.check_partition_wal($TEST_OID::oid)
    WHERE is_valid
)
SELECT count(*) FROM rec
WHERE prev_wal_file IS NOT NULL
  AND wal_file != prev_wal_file
  AND partition_lsn - prev_plsn = 1
" 2>&1 | tr -d ' \r')

TOTAL_BOUNDS=$(( ${BAD_BOUNDS:-0} + ${GOOD_BOUNDS:-0} ))

if [[ "${BAD_BOUNDS:-0}" == "0" ]] && [[ "${TOTAL_BOUNDS:-0}" -ge "$SWITCHES" ]]; then
    pass "T4  段边界连续性：$TOTAL_BOUNDS 处边界全部 diff=1（检测到 ≥$SWITCHES 次切换）"
elif [[ "${BAD_BOUNDS:-0}" == "0" ]]; then
    fail "T4  段边界连续性：边界数=$TOTAL_BOUNDS < 期望 $SWITCHES（可能段切换未生效）"
else
    fail "T4  段边界连续性：$BAD_BOUNDS/$TOTAL_BOUNDS 处边界 diff≠1"
fi

# ===========================================================
# 阶段 4：二进制段文件解析
# ===========================================================
echo ""
echo "===== 阶段 4：二进制段文件解析 ====="

PARWAL_DIR="$W1_DATA/pg_parwal/$TEST_OID"

perl - "$PARWAL_DIR" "$TEST_OID" "$EXPECTED_RECORDS" "$EXPECTED_FILES" <<'PLEOF'
use strict;
use warnings;
use POSIX qw(floor);

# PartWALHeader 布局 (小端 x86):
#   uint32  magic           offset  0  (4 B)  → V
#   uint32  partition_id    offset  4  (4 B)  → V
#   uint64  orig_node_lsn   offset  8  (8 B)  → VV (lo, hi)
#   uint64  partition_lsn   offset 16  (8 B)  → VV (lo, hi)
#   uint8   flags           offset 24  (1 B)  → C
#   padding                 offset 25  (7 B)  → x7
#   total 32 bytes
my $MAGIC       = 0x50415254;
my $RECORD_SIZE = 32;
my $HDR_FMT     = 'VV VV VV C x7';   # 4+4+4+4+4+4+1+7 = 32

my $parwal_dir  = $ARGV[0];
my $oid         = $ARGV[1] + 0;
my $exp_records = $ARGV[2] + 0;
my $exp_files   = $ARGV[3] + 0;

printf "  解析目录        : %s\n", $parwal_dir;
printf "  PartWALHeader   : %d 字节  (Perl 格式: %s)\n", $RECORD_SIZE, $HDR_FMT;

opendir(my $dh, $parwal_dir) or do {
    printf "  ERROR: 无法打开目录: %s\n", $!;
    exit 1;
};
my @files = sort grep { /^[0-9A-Fa-f]{24}$/ } readdir($dh);
closedir($dh);

printf "  段文件数        : %d  (期望 %d)\n\n", scalar(@files), $exp_files;

my @all_records;   # [ $fname, $plsn_lo, $orig_lo, $orig_hi ]

for my $fname (@files) {
    my $fpath = "$parwal_dir/$fname";
    open(my $fh, '<:raw', $fpath) or next;

    my @recs;
    while (1) {
        my $chunk;
        my $n = read($fh, $chunk, $RECORD_SIZE);
        last unless defined($n) && $n == $RECORD_SIZE;

        my ($magic, $pid, $orig_lo, $orig_hi, $plsn_lo, $plsn_hi, $flags)
            = unpack($HDR_FMT, $chunk);

        next unless $magic == $MAGIC;

        push @recs, [$fname, $plsn_lo, $orig_lo, $orig_hi];
    }
    close($fh);

    if (@recs) {
        my $first_p  = $recs[0][1];
        my $last_p   = $recs[-1][1];
        my $orig_hi  = $recs[0][3];
        my $orig_lo  = $recs[0][2];
        printf "  %s  %3d 条  plsn [%4d-%4d]  orig=%X/%08X\n",
               $fname, scalar(@recs), $first_p, $last_p, $orig_hi, $orig_lo;
    } else {
        printf "  %s   0 条 (空)\n", $fname;
    }
    push @all_records, @recs;
}

printf "\n  总记录数        : %d  (期望 %d)\n\n", scalar(@all_records), $exp_records;

my $gaps            = 0;
my $good_boundaries = 0;
my @errors;

for my $i (0 .. $#all_records) {
    my ($fname, $plsn_lo, $orig_lo, $orig_hi) = @{$all_records[$i]};

    if ($i == 0) {
        if ($plsn_lo != 1) {
            push @errors, "首条 partition_lsn=$plsn_lo，期望 1";
            $gaps++;
        }
        next;
    }

    my ($prev_fname, $prev_plsn) = ($all_records[$i-1][0], $all_records[$i-1][1]);
    my $diff        = $plsn_lo - $prev_plsn;
    my $is_boundary = ($fname ne $prev_fname);

    if ($diff != 1) {
        $gaps++;
        my $where = $is_boundary ? "跨段" : "段内";
        push @errors, sprintf("[%s] plsn %d->%d diff=%d  (%s->%s)",
                              $where, $prev_plsn, $plsn_lo, $diff, $prev_fname, $fname);
    } elsif ($is_boundary) {
        $good_boundaries++;
        printf "  OK  边界 #%d: %s -> %s  plsn %d->%d diff=1  orig=%X/%08X\n",
               $good_boundaries, $prev_fname, $fname,
               $prev_plsn, $plsn_lo, $orig_hi, $orig_lo;
    }
}

if (@errors) {
    print "\n";
    my $show = @errors > 10 ? 10 : scalar(@errors);
    for my $e (@errors[0..$show-1]) { print "  ERR: $e\n"; }
    printf "  ... 及另外 %d 个错误\n", scalar(@errors) - 10 if @errors > 10;
}

print "\n";
print  "  ─────────────────────────────────────────────\n";
printf "  总断裂数        : %d\n", $gaps;
printf "  跨段边界(OK)    : %d\n", $good_boundaries;
print  "  ─────────────────────────────────────────────\n";

if ($gaps == 0) {
    print "  二进制解析结论  : PASS\n";
    exit 0;
} else {
    printf "  二进制解析结论  : FAIL (%d 处断裂)\n", $gaps;
    exit 1;
}
PLEOF

PYEXIT=$?
if [[ $PYEXIT -eq 0 ]]; then
    pass "T5  二进制解析：partition_lsn 跨段全部连续"
else
    fail "T5  二进制解析：发现断裂"
fi

# ===========================================================
# 阶段 5：≥5 次段切换后记录总数核验
# ===========================================================
echo ""
echo "===== 阶段 5：≥5 次段切换后记录总数核验 ====="

# Flush once more so any buffered WAL from earlier phases lands before counting
qw1 "SELECT partdist.demux_flush()" >/dev/null 2>&1

FINAL_COUNT=""
for _try in 1 2 3 4; do
    FINAL_COUNT=$(qw1 "SELECT partdist.count_parwal_records($TEST_OID::oid)" 2>&1 | tr -d ' \r')
    [ -n "$FINAL_COUNT" ] && break
    sleep 0.5
done

if [[ "$FINAL_COUNT" == "$EXPECTED_RECORDS" ]]; then
    pass "T6  $SWITCHES 次段切换后 count_parwal_records = $FINAL_COUNT = 写入数 $EXPECTED_RECORDS"
else
    fail "T6  count_parwal_records = $FINAL_COUNT，期望 $EXPECTED_RECORDS"
fi

# ===========================================================
# 汇总
# ===========================================================
echo ""
echo "=================================================="
echo " PASSED : $PASS"
echo " FAILED : $FAIL"
echo " 段文件 : $SEG_FILES  段切换 : $SWITCHES"
echo "=================================================="

if [[ $FAIL -eq 0 ]]; then
    echo "跨段边界 LSN 连续性: PASS"
    exit 0
else
    echo "跨段边界 LSN 连续性: FAIL"
    exit 1
fi
