#!/bin/bash
# test_corrupt_segment_recovery.sh — pg_parwal 段文件损坏恢复测试
#
# 验证以下健壮性行为：
#   1. 读取损坏段文件不导致 PANIC / 进程崩溃
#   2. count_parwal_records 返回损坏点之前的有效记录数
#   3. verify_partition_wal 正确报告不连续（返回 false，不崩溃）
#   4. 向已损坏分区写入新数据后 Demux Worker 仍可继续工作
#   5. 手动删除损坏段文件后系统恢复正常
#
# 损坏方式：
#   C1 — 覆盖中间记录的 magic 字段（dd 写入 4 字节零）
#   C2 — 截断文件到非整数记录边界
#   C3 — 覆盖文件头（第一条记录的 magic 字段）

PSQL=/work/pg-install/bin/psql
PGCTL=/work/pg-install/bin/pg_ctl
DATA=/work/pg-cluster-data
HEADER_SIZE=40     # sizeof(PartWALHeader) = 40 字节（parwal-2.0 record version 2，含 xid），已在测试开头验证
PASS=0; FAIL=0
LOG_START_LINE=0   # snapshot of pg.log line count at test start

die()  { echo "FATAL: $*" >&2; exit 1; }
pass() { echo "  PASS: $*"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }

check_eq() {
    local label=$1 actual=$2 expected=$3
    if [ "$actual" = "$expected" ]; then
        pass "$label (got $actual)"
    else
        fail "$label (expected $expected, got $actual)"
    fi
}
check_true()  {
    local label=$1 val=$2
    [ "$val" = "t" ] && pass "$label" || fail "$label (got '$val')"
}
check_false() {
    local label=$1 val=$2
    [ "$val" = "f" ] && pass "$label" || fail "$label (expected false, got '$val')"
}
check_ge() {
    local label=$1 actual=$2 expected=$3
    if [ "$actual" -ge "$expected" ] 2>/dev/null; then
        pass "$label (got $actual >= $expected)"
    else
        fail "$label (expected >= $expected, got $actual)"
    fi
}

# ── Worker1 helper ──────────────────────────────────────────────────
psql1()    { $PSQL -p 5433 -U postgres -d postgres "$@"; }
psql1_at() { psql1 -At -c "$1"; }
psql0()    { $PSQL -p 5432 -U postgres -d postgres "$@"; }

count_records() {
    local pid=$1 result tries=0
    while [ $tries -lt 4 ]; do
        result=$(psql1_at "SELECT partdist.count_parwal_records($pid::oid);" 2>/dev/null || true)
        [ -n "$result" ] && echo "$result" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
verify_wal() {
    local pid=$1 result tries=0
    while [ $tries -lt 4 ]; do
        result=$(psql1_at "SELECT partdist.verify_partition_wal($pid::oid);" 2>/dev/null || true)
        [ -n "$result" ] && echo "$result" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
flush_w1()      { psql1 -c 'SELECT partdist.demux_flush();' -o /dev/null; }
reset_state()   { psql1 -c "SELECT partdist.reset_partition_wal_state($1::oid);" -o /dev/null; }

write_records() {
    local pid=$1 n=$2
    psql1 -o /dev/null -c "
DO \$\$
DECLARE i int;
BEGIN
  FOR i IN 1..$n LOOP
    PERFORM partdist.write_partition_wal_record($pid::oid);
  END LOOP;
END \$\$;"
}

# 找到 partition 对应的段文件（最新的那个）
get_seg_file() {
    local pid=$1
    local dir="$DATA/worker1/pg_parwal/$pid"
    ls "$dir"/ 2>/dev/null | grep -v '^checkpoint$' | sort | tail -1 | xargs -I{} echo "$dir/{}"
}

no_panic_in_log() {
    local label=$1
    local new_content
    new_content=$(tail -n +"$((LOG_START_LINE + 1))" "$DATA/worker1/pg.log" 2>/dev/null)
    # Only check for PANIC (true crash); FATAL is a connection-level error, expected under load
    if echo "$new_content" | grep -qE '\bPANIC\b'; then
        fail "$label: PANIC found in pg.log since test start"
        echo "    $(echo "$new_content" | grep -E '\bPANIC\b' | head -3)"
    else
        pass "$label: no PANIC in pg.log since test start"
    fi
}

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  pg_partdist — 段文件损坏恢复测试"
echo "════════════════════════════════════════════════════════════════"

# ── 全局设置 ─────────────────────────────────────────────────────────
LOG_START_LINE=$(wc -l < "$DATA/worker1/pg.log" 2>/dev/null || echo 0)
echo "  pg.log 当前行数快照: $LOG_START_LINE（仅检查测试期间新增的 PANIC/FATAL）"

echo ""
echo "── 设置：创建分布式表 ──"
psql0 -o /dev/null -c "
    DROP TABLE IF EXISTS corrupt_test CASCADE;
    CREATE TABLE corrupt_test (id int PRIMARY KEY, val text);
    SELECT create_distributed_table('corrupt_test','id',shard_count=>4);"

P_OID=$(psql1_at "SELECT oid FROM pg_class WHERE relname='corrupt_test' AND relkind='r' LIMIT 1;")
[ -n "$P_OID" ] || die "无法在 worker1 找到 corrupt_test 的 OID"
echo "  Worker1 OID: $P_OID"

# ── 验证 sizeof(PartWALHeader) = HEADER_SIZE ──────────────────────────
echo ""
echo "── 预检：验证 sizeof(PartWALHeader) ──"
reset_state "$P_OID"
write_records "$P_OID" 1
flush_w1
SEG0=$(get_seg_file "$P_OID")
[ -n "$SEG0" ] || die "无法找到 segment 文件"
SZ0=$(stat -c '%s' "$SEG0" 2>/dev/null)
check_eq "sizeof(PartWALHeader)" "$SZ0" "$HEADER_SIZE"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ C1: 覆盖中间记录的 magic 字段 ════════"
echo "  方法: 写入10条记录 → dd 在 offset=4×HEADER_SIZE 处写入4字节零 → 覆盖第5条记录"
# ════════════════════════════════════════════════════════════════════════

reset_state "$P_OID"
write_records "$P_OID" 10
flush_w1

SEG_C1=$(get_seg_file "$P_OID")
[ -n "$SEG_C1" ] || die "C1: 无法找到 segment 文件"
echo "  Segment 文件: $SEG_C1"

# 基线检查
C1_CNT=$(count_records "$P_OID")
C1_VFY=$(verify_wal "$P_OID")
check_eq   "C1-基线 count=10"    "$C1_CNT" "10"
check_true "C1-基线 verify=true" "$C1_VFY"

# 损坏：覆盖第5条记录(index=4)的 magic 字段 offset=4*HEADER_SIZE, 写4字节零
CORRUPT_OFFSET=$((4 * HEADER_SIZE))
echo "  损坏: dd 在 offset=$CORRUPT_OFFSET 写入 4 字节零（覆盖 magic 字段）..."
dd if=/dev/zero of="$SEG_C1" bs=1 count=4 seek=$CORRUPT_OFFSET conv=notrunc 2>/dev/null
echo "  损坏完成。"

# 读路径检查（损坏后）
C1_CNT_COR=$(count_records "$P_OID")
C1_VFY_COR=$(verify_wal "$P_OID")
check_eq    "C1 损坏后 count=4（损坏点前的有效记录数）" "$C1_CNT_COR" "4"
check_false "C1 损坏后 verify=false"                    "$C1_VFY_COR"
no_panic_in_log "C1 读路径无 PANIC"

# 写路径检查：插入5条新记录，Demux 应继续写入
write_records "$P_OID" 5
flush_w1

C1_CNT_NEW=$(count_records "$P_OID")
C1_SZ_NEW=$(stat -c '%s' "$SEG_C1" 2>/dev/null)
C1_VFY_NEW=$(verify_wal "$P_OID")
C1_SZ_EXPECTED=$((15 * HEADER_SIZE))
check_eq    "C1 写入后文件大小=$C1_SZ_EXPECTED (15条×$HEADER_SIZE)"    "$C1_SZ_NEW" "$C1_SZ_EXPECTED"
check_eq    "C1 写入后 count 仍=4（停在损坏点）"  "$C1_CNT_NEW" "4"
check_false "C1 写入后 verify 仍=false"             "$C1_VFY_NEW"
no_panic_in_log "C1 写路径无 PANIC"

# 删除损坏段文件 → 恢复
echo "  删除损坏段文件..."
rm "$SEG_C1"
C1_CNT_DEL=$(count_records "$P_OID")
C1_VFY_DEL=$(verify_wal "$P_OID")
check_eq   "C1 删除后 count=0（无文件）"        "$C1_CNT_DEL" "0"
check_true "C1 删除后 verify=true（空目录有效）" "$C1_VFY_DEL"

# 完整恢复：reset + 写新数据
reset_state "$P_OID"
write_records "$P_OID" 5
flush_w1
C1_CNT_REC=$(count_records "$P_OID")
C1_VFY_REC=$(verify_wal "$P_OID")
check_eq   "C1 完整恢复后 count=5"    "$C1_CNT_REC" "5"
check_true "C1 完整恢复后 verify=true" "$C1_VFY_REC"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ C2: 截断文件到非整数记录边界 ════════"
C2_TRUNC_SIZE=$((6 * HEADER_SIZE + 8))
echo "  方法: 写入8条记录 → truncate -s $C2_TRUNC_SIZE (= 6×$HEADER_SIZE+8，非整记录边界)"
# ════════════════════════════════════════════════════════════════════════

reset_state "$P_OID"
write_records "$P_OID" 8
flush_w1

SEG_C2=$(get_seg_file "$P_OID")
[ -n "$SEG_C2" ] || die "C2: 无法找到 segment 文件"
echo "  Segment 文件: $SEG_C2"

C2_CNT=$(count_records "$P_OID")
check_eq "C2-基线 count=8" "$C2_CNT" "8"

# 截断到 C2_TRUNC_SIZE 字节 (= 6×HEADER_SIZE + 8，最后8字节不足一条记录)
truncate -s "$C2_TRUNC_SIZE" "$SEG_C2"
echo "  截断后文件大小: $(stat -c '%s' "$SEG_C2") 字节"

C2_CNT_TR=$(count_records "$P_OID")
C2_VFY_TR=$(verify_wal "$P_OID")
# C2_TRUNC_SIZE / HEADER_SIZE = 6 完整记录，第7条只有8字节被截断，read()返回短读 → 停止
check_eq   "C2 截断后 count=6（完整记录数）" "$C2_CNT_TR" "6"
check_true "C2 截断后 verify=true（6条有效连续记录）" "$C2_VFY_TR"
no_panic_in_log "C2 截断后无 PANIC"

# 写入新记录（追加到截断后的文件）
write_records "$P_OID" 4
flush_w1
C2_CNT_NEW=$(count_records "$P_OID")
C2_VFY_NEW=$(verify_wal "$P_OID")
# 截断了记录7（lsn=7），记录8原本存在（丢失）；新写入记录追加在截断点之后
# 新写入记录 lsn=9-12，与 lsn=7,8 形成不连续 → verify=false
check_ge    "C2 写入后 count>=6（原有有效 + 可能新增）" "$C2_CNT_NEW" "6"
check_false "C2 写入后 verify=false（截断后 lsn 不连续）" "$C2_VFY_NEW"
no_panic_in_log "C2 写路径无 PANIC"

reset_state "$P_OID"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ C3: 覆盖文件头（第1条记录的 magic 字段）════════"
echo "  方法: 写入6条记录 → dd 在 offset=0 写入4字节零 → 覆盖 magic"
# ════════════════════════════════════════════════════════════════════════

reset_state "$P_OID"
write_records "$P_OID" 6
flush_w1

SEG_C3=$(get_seg_file "$P_OID")
[ -n "$SEG_C3" ] || die "C3: 无法找到 segment 文件"
echo "  Segment 文件: $SEG_C3"

check_eq "C3-基线 count=6" "$(count_records "$P_OID")" "6"

# 覆盖第1条记录的 magic（offset=0，4字节）
dd if=/dev/zero of="$SEG_C3" bs=1 count=4 seek=0 conv=notrunc 2>/dev/null
echo "  文件头损坏完成。"

C3_CNT=$(count_records "$P_OID")
C3_VFY=$(verify_wal "$P_OID")
check_eq    "C3 文件头损坏后 count=0（第1条即为无效）" "$C3_CNT" "0"
check_false "C3 文件头损坏后 verify=false"               "$C3_VFY"
no_panic_in_log "C3 文件头损坏后无 PANIC"

# GetLastWrittenPartitionLSN 不应崩溃（内部调用 AllocPartitionLSN 时触发）
LSN_ALLOC=$(psql1_at "SELECT partdist.alloc_partition_lsn($P_OID::oid);" 2>&1)
echo "  alloc_partition_lsn (损坏后): $LSN_ALLOC（不崩溃即可）"
no_panic_in_log "C3 alloc_partition_lsn 无 PANIC"

# 写入新记录后 Demux 应仍可工作
write_records "$P_OID" 3
flush_w1
C3_CNT_NEW=$(count_records "$P_OID")
echo "  C3 写入后 count: $C3_CNT_NEW"
# 写入新记录后，原第1条仍然损坏，新记录追加在后
# count 仍=0（停在第1条损坏处），verify=false
check_eq    "C3 写入后 count=0（仍停在头部损坏处）" "$C3_CNT_NEW" "0"
no_panic_in_log "C3 写入后无 PANIC"

# 删除损坏段 → 完整恢复
rm "$SEG_C3"
reset_state "$P_OID"
write_records "$P_OID" 5
flush_w1
C3_CNT_REC=$(count_records "$P_OID")
C3_VFY_REC=$(verify_wal "$P_OID")
check_eq   "C3 完整恢复后 count=5"    "$C3_CNT_REC" "5"
check_true "C3 完整恢复后 verify=true" "$C3_VFY_REC"
no_panic_in_log "C3 完整恢复后无 PANIC"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ C4: 跨段文件场景（pg_switch_wal 强制多段）════════"
echo "  方法: 写10条 → pg_switch_wal → 再写5条 → 损坏第一段 → 验证第二段"
# ════════════════════════════════════════════════════════════════════════

reset_state "$P_OID"
write_records "$P_OID" 10
flush_w1

SEG_A=$(get_seg_file "$P_OID")
[ -n "$SEG_A" ] || die "C4: 无法找到 SEG_A"
check_eq "C4-SEG_A count=10" "$(count_records "$P_OID")" "10"

# 切换 WAL 段使后续记录落入新的 pg_parwal 段文件
psql1 -c 'SELECT pg_switch_wal();' -o /dev/null
psql1 -c 'SELECT pg_switch_wal();' -o /dev/null   # 二次确保段号递增

write_records "$P_OID" 5
flush_w1

NSEGS=$(ls "$DATA/worker1/pg_parwal/$P_OID/" 2>/dev/null | wc -l)
echo "  当前段文件数: $NSEGS"
SEG_B=$(ls "$DATA/worker1/pg_parwal/$P_OID/" 2>/dev/null | grep -v '^checkpoint$' | sort | tail -1 | xargs -I{} echo "$DATA/worker1/pg_parwal/$P_OID/{}")
[ "$SEG_A" != "$SEG_B" ] && pass "C4: 新记录写入了新段文件 SEG_B" || fail "C4: pg_switch_wal 未产生新段文件（可能 WAL 未换段）"

TOTAL_BEFORE=$(count_records "$P_OID")
check_eq "C4-基线总 count=15" "$TOTAL_BEFORE" "15"
check_true "C4-基线 verify=true" "$(verify_wal "$P_OID")"

# 损坏 SEG_A 中间（record 5，offset=4*HEADER_SIZE）
SEG_A_CORRUPT_OFFSET=$((4 * HEADER_SIZE))
dd if=/dev/zero of="$SEG_A" bs=1 count=4 seek=$SEG_A_CORRUPT_OFFSET conv=notrunc 2>/dev/null
echo "  损坏 SEG_A record 5 magic..."

C4_CNT=$(count_records "$P_OID")
C4_VFY=$(verify_wal "$P_OID")
check_eq    "C4 损坏后 count=4（SEG_A 前4条有效，SEG_B 不计）" "$C4_CNT" "4"
check_false "C4 损坏后 verify=false（SEG_A 中断）"               "$C4_VFY"
no_panic_in_log "C4 跨段读路径无 PANIC"

# 删除 SEG_A，只保留 SEG_B（包含 lsn 11-15）
rm "$SEG_A"
C4_CNT_DEL=$(count_records "$P_OID")
C4_VFY_DEL=$(verify_wal "$P_OID")
# SEG_B 有 5 条有效记录，但 lsn 从 11 开始（不是1），verify=false
check_eq    "C4 删除SEG_A后 count=5（SEG_B 有效记录）" "$C4_CNT_DEL" "5"
check_false "C4 删除SEG_A后 verify=false（lsn不从1开始）" "$C4_VFY_DEL"
no_panic_in_log "C4 删除段文件后无 PANIC"

# 完整恢复
reset_state "$P_OID"
write_records "$P_OID" 5
flush_w1
C4_CNT_REC=$(count_records "$P_OID")
C4_VFY_REC=$(verify_wal "$P_OID")
check_eq   "C4 完整恢复后 count=5"    "$C4_CNT_REC" "5"
check_true "C4 完整恢复后 verify=true" "$C4_VFY_REC"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "── 最终 PostgreSQL 日志检查 ──"
# ════════════════════════════════════════════════════════════════════════
PANIC_COUNT=$(tail -n +"$((LOG_START_LINE + 1))" "$DATA/worker1/pg.log" 2>/dev/null | grep -cE '\bPANIC\b' || true)
check_eq "Worker1 日志中（测试期间）PANIC 次数=0（不计 FATAL 连接错误）" "${PANIC_COUNT:-0}" "0"

# ── 清理 ──────────────────────────────────────────────────────────────
echo ""
echo "── 清理 ──"
reset_state "$P_OID"
psql0 -o /dev/null -c "DROP TABLE IF EXISTS corrupt_test CASCADE;" 2>/dev/null

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ 测试摘要 ════════"
echo "  通过: $PASS"
echo "  失败: $FAIL"
echo ""
if [ "$FAIL" -eq 0 ]; then
    echo "段文件损坏恢复测试: PASS"
    exit 0
else
    echo "段文件损坏恢复测试: FAIL"
    exit 1
fi
