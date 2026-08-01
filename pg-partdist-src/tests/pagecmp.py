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

        print("  page%d hole=[%d,%d) 洞内差异=%d 洞外差异=%d%s"
              % (p, lower_a, upper_a, inhole, len(outside),
                 (" 偏移=%s" % outside[:12]) if outside else ""),
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
