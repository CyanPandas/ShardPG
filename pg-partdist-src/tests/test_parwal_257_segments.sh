#!/usr/bin/env bash
# [宿主机] 回归：分区 WAL 段数超过 256 时，读取接口不许漏段（2026-10-12）。
#
# 缺陷：九处读取路径列目录时都写着 `nfiles < 256`（固定数组 segfiles[256]），
# 段数过了 256 之后的段被静默漏掉 —— 记录明明在第 257 段的文件里，文件没丢，
# partwal_read_record 却答"没有这条记录"（NULL），count_parwal_records 少数，
# verify_partition_wal 判流断档。readdir 顺序还不保证，漏的是哪几段都说不准。
# 修法：统一走 PartWALListSegments()（不设上限、按名升序）。
#
# 做法：在一台 worker 上给一个**不存在的分区 OID** 造 300 个段文件，每段一条记录
# （partition_lsn = 1..300，gxid 同号便于认出读到的是哪条；CTRL 类、无载荷，头部按 PartWALRecord 布局逐字节写），
# 然后经产品 SQL 接口读回。旧代码下 count 恒 ≤ 256、读第 257/300 条恒为 NULL。
# 跑完删掉造的目录。
#
# 用法：bash test_parwal_257_segments.sh [worker 端口，默认 5433]
set -u
C=${CONTAINER:-pg-test-container}
PORT=${1:-5433}
OID=4000000257
NSEG=300
PSQL() { docker exec -i -u postgres "$C" /work/pg-install/bin/psql -h /tmp -p "$PORT" -U postgres -d postgres -X -q -At "$@"; }

DATADIR=$(PSQL -c "SHOW data_directory" </dev/null)
[[ -n "$DATADIR" ]] || { echo "FAIL: 连不上 :$PORT"; exit 1; }
DIR="$DATADIR/pg_parwal/$OID"
[[ "$(PSQL -c "SELECT count(*) FROM pg_class WHERE oid = $OID" </dev/null)" == 0 ]] || { echo "FAIL: OID $OID 已被占用"; exit 1; }

cleanup() { docker exec -u postgres "$C" rm -rf "$DIR"; }
trap cleanup EXIT

docker exec -i -u postgres "$C" python3 - "$DIR" "$OID" "$NSEG" <<'PY'
import os, struct, sys
d, oid, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
os.makedirs(d, exist_ok=True)
MAGIC, VERSION_3, FLAG_CTRL = 0x50415254, 3, 0x04
for plsn in range(1, n + 1):
    segno = plsn                       # 一段一条；段名升序 = plsn 升序
    name = "%08X%08X%08X" % (1, segno // 0x100, segno % 0x100)
    # PartWALRecord: magic u32, partition_id u32, orig_lsn u64, partition_lsn u64,
    #                rmid u8, info u8, version u8, flags u8, data_len u32, gxid u64  = 40 字节
    hdr = struct.pack("<IIQQBBBBIQ", MAGIC, oid, 0, plsn, 0, 0, VERSION_3, FLAG_CTRL, 0, plsn)   # gxid = plsn，读回时据此认出是哪一条
    assert len(hdr) == 40
    with open(os.path.join(d, name), "wb") as f:
        f.write(hdr)
PY
nfiles=$(docker exec "$C" bash -c "ls '$DIR' | wc -l")

checks=0; fails=0
check() {   # $1=说明 $2=实际 $3=期望
  checks=$((checks + 1))
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1：$2"; else echo "  FAIL  $1：得到「$2」，应为「$3」"; fails=$((fails + 1)); fi
}
echo "造了 $nfiles 个段文件（$DIR）"
check "段文件数" "$nfiles" "$NSEG"
check "count_parwal_records 数到全部记录" "$(PSQL -c "SELECT partdist.count_parwal_records($OID)" </dev/null)" "$NSEG"
check "read_all_headers 读到全部记录" "$(PSQL -c "SELECT count(*) FROM partdist.read_all_headers($OID)" </dev/null)" "$NSEG"
check "verify_partition_wal 判流连续" "$(PSQL -c "SELECT partdist.verify_partition_wal($OID)" </dev/null)" "t"
for p in 1 256 257 $NSEG; do
  check "partwal_read_record 读第 $p 条（第 $p 段）" \
        "$(PSQL -c "SELECT gxid FROM partdist.partwal_read_record($OID, $p)" </dev/null)" "$p"
done
# 同一后端里先读一条建稀疏索引、再读段表末尾之后的一条：走索引 + 刷新段表的路径
check "同一会话内先读 1 再读 $NSEG（稀疏索引 + 段表刷新）" \
      "$(PSQL -c "SELECT gxid FROM partdist.partwal_read_record($OID, 1)" -c "SELECT gxid FROM partdist.partwal_read_record($OID, $NSEG)" </dev/null | tail -1)" "$NSEG"

[[ $checks -gt 0 ]] || { echo "FAIL: 一项检查都没跑"; exit 1; }
echo "检查 $checks 项，失败 $fails 项"
[[ $fails -eq 0 ]]
