#!/usr/bin/env python3
"""
物理副本页面比对：忽略页内空闲空洞的逐字节比较。

为什么不能直接 cmp 整文件（FRD §14.2 R1 验收判据的修正依据）：

  PostgreSQL 的 FPI（全页镜像）在 `BKPIMAGE_HAS_HOLE` 时只搬运
  [0, pd_lower) 与 [pd_upper, BLCKSZ) 两段，中间的空闲空洞不入 WAL；
  `RestoreBlockImage` 恢复时把空洞**清零**：

      memcpy(page, ptr, hole_offset);
      MemSet(page + hole_offset, 0, hole_length);      /* ← 清零 */
      memcpy(page + hole_offset + hole_length, ...);

  而主库那片区域保留着被删元组的残字节。于是"主库页面"与"从 FPI 恢复出的
  副本页面"在空洞内必然不同 —— 这是原生流复制备库也有的现象，不是回放缺陷。

  空洞按定义是未使用空间：pd_lower 之前是页头+行指针，pd_upper 之后是元组
  与 special 区，两者才承载语义。因此正确判据是**洞外逐字节一致**，
  并且额外要求两侧 pd_lower/pd_upper 本身相同（否则"洞"的位置就不可比）。

用法: pagecmp.py <leader_file> <follower_file>
输出: 一行 "IDENTICAL_OUTSIDE_HOLE" 或 "DIFF <洞外差异字节数>"，
      并在 stderr 打印每页明细。退出码 0=一致。
"""
import struct
import sys

BLCKSZ = 8192

# PageHeaderData 字段布局（src/include/storage/bufpage.h）
_HDR = [
    (0,  8, "pd_lsn"),
    (8,  2, "pd_checksum"),
    (10, 2, "pd_flags"),
    (12, 2, "pd_lower"),
    (14, 2, "pd_upper"),
    (16, 2, "pd_special"),
    (18, 2, "pd_pagesize_version"),
    (20, 4, "pd_prune_xid"),
]

# HeapTupleHeaderData 前 23 字节（src/include/access/htup_details.h）
_TUP = [
    (0,  4, "t_xmin"),
    (4,  4, "t_xmax"),
    (8,  4, "t_cid/t_xvac"),
    (12, 6, "t_ctid"),
    (18, 2, "t_infomask2"),
    (20, 2, "t_infomask"),
    (22, 1, "t_hoff"),
]


def classify(off, lower, upper):
    """把页内偏移定性成字段名 —— 头部字段 / 行指针 / 元组头 / 元组数据。"""
    for base, size, name in _HDR:
        if base <= off < base + size:
            return "页头 %s[+%d]" % (name, off - base)
    if 24 <= off < lower:
        n = (off - 24) // 4
        return "行指针 lp[%d][+%d]" % (n + 1, (off - 24) % 4)
    if off >= upper:
        # 元组区：定位到所属元组起点需要行指针，这里只标相对量
        rel = off - upper
        for base, size, name in _TUP:
            if base <= rel < base + size:
                return "元组区(首元组) %s[+%d]" % (name, rel - base)
        return "元组区 +%d" % rel
    return "空洞外未知区"


def main():
    if len(sys.argv) != 3:
        print("usage: pagecmp.py <leader_file> <follower_file>", file=sys.stderr)
        return 2

    try:
        a = open(sys.argv[1], 'rb').read()
        b = open(sys.argv[2], 'rb').read()
    except OSError as e:
        print("DIFF open_error", file=sys.stdout)
        print(e, file=sys.stderr)
        return 2

    if len(a) != len(b):
        print("DIFF size %d vs %d" % (len(a), len(b)))
        return 1

    outside_total = 0
    hole_total = 0

    for p in range(len(a) // BLCKSZ):
        pa = a[p * BLCKSZ:(p + 1) * BLCKSZ]
        pb = b[p * BLCKSZ:(p + 1) * BLCKSZ]

        # PageHeaderData: pd_lower @12 (uint16), pd_upper @14 (uint16)
        lower_a, upper_a = struct.unpack_from('<HH', pa, 12)
        lower_b, upper_b = struct.unpack_from('<HH', pb, 12)

        if (lower_a, upper_a) != (lower_b, upper_b):
            print("DIFF page%d hole_mismatch [%d,%d) vs [%d,%d)"
                  % (p, lower_a, upper_a, lower_b, upper_b))
            return 1

        # 空洞边界越界的页视为损坏，不做宽容处理
        if not (0 <= lower_a <= upper_a <= BLCKSZ):
            print("DIFF page%d bad_header lower=%d upper=%d"
                  % (p, lower_a, upper_a))
            return 1

        outside = [i for i in range(BLCKSZ)
                   if pa[i] != pb[i] and not (lower_a <= i < upper_a)]
        inhole = sum(1 for i in range(lower_a, upper_a) if pa[i] != pb[i])

        outside_total += len(outside)
        hole_total += inhole

        print("  page%d hole=[%d,%d) 洞内差异=%d 洞外差异=%d"
              % (p, lower_a, upper_a, inhole, len(outside)), file=sys.stderr)

        # 洞外差异逐处定性 —— 差在哪个字段比"差了几个字节"有用得多
        for off in outside:
            print("    偏移 %4d  %-28s leader=0x%02X follower=0x%02X"
                  % (off, classify(off, lower_a, upper_a), pa[off], pb[off]),
                  file=sys.stderr)

    if outside_total == 0:
        print("IDENTICAL_OUTSIDE_HOLE")
        print("洞外逐字节一致（洞内 %d 字节差异属 FPI 清零，非缺陷）"
              % hole_total, file=sys.stderr)
        return 0

    print("DIFF %d" % outside_total)
    return 1


if __name__ == '__main__':
    sys.exit(main())
