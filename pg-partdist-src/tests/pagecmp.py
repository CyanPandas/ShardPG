#!/usr/bin/env python3
"""
物理副本堆页比对：按**内核自己的掩码规则**排除不可比字段后逐字节比较。

判据的来源不是我们自己拍的，而是 PostgreSQL 用来验证"redo 出来的页与主库页
是否一致"的那套设施 —— `wal_consistency_checking`。它对堆页调用
`heap_mask()`（src/backend/access/heap/heapam.c）+ `bufmask.c` 里的几个
mask_* 辅助，把"主备之间本来就不保证相同"的字段统一涂成 MASK_MARKER 再比。
本脚本照抄那份掩码集合，因此"洞外一致"这个说法有内核背书，不是自定标准。

内核 heap_mask() 掩掉的东西，以及本脚本的对应处理：

  1. mask_page_lsn_and_checksum —— pd_lsn 与 pd_checksum。
     **本脚本刻意不掩 pd_lsn**：R1 的核心主张就是 follower 用 leader 的
     orig_lsn 盖页（§4.2/§8.2），页 LSN 必须逐字节相同。这是比内核更强的判据，
     是本项目的验收重点，不能放过。pd_checksum 同样保留（两侧 checksum 均由
     相同页内容算出，理应相同）。
  2. mask_page_hint_bits —— pd_prune_xid，以及 pd_flags 的
     PD_HAS_FREE_LINES / PD_PAGE_FULL / PD_ALL_VISIBLE 三位。
     `heap_xlog_prune()` 里内核自己写着 "we don't worry about updating the
     page's prunability hints"，redo 故意不复制；三个 flag 位同属提示。
  3. mask_unused_space —— [pd_lower, pd_upper) 的空闲空洞。
     FPI 带 BKPIMAGE_HAS_HOLE 时 RestoreBlockImage 把空洞清零，主库那片区域
     保留被删元组的残字节 —— 原生流复制备库同样如此。
  4. 每条 LP_NORMAL 元组：xmin 未冻结时掩掉 t_infomask 的 HEAP_XACT_MASK
     （可见性提示位，读取者查过 clog 后顺手写上，不产生 WAL）；已冻结时仍要
     掩掉 HEAP_XMAX_INVALID / HEAP_XMAX_COMMITTED。
  5. 每条 LP_NORMAL 元组：t_cid（回放时被置为 FirstCommandId，见
     heap_xlog_insert）。
  6. **每条有存储的行指针之后的对齐填充**：MAXALIGN(lp_len) - lp_len 字节。
     元组按 8 字节对齐存放，而 lp_len 是元组真实长度，中间那几个字节谁也不读，
     redo 也不写 —— 主备残留内容不同是常态。
     （实测就栽在这一条上：R1 报的"洞外 4 字节差异"全部落在这里。）

用法: pagecmp.py <leader_file> <follower_file>
输出: 一行 "IDENTICAL_OUTSIDE_HOLE" 或 "DIFF <掩码之外的差异字节数>"，
      并在 stderr 打印每页明细。退出码 0=一致。

注意：掩码规则是**堆页专用**的（走行指针 + 元组头布局）。索引页要另配
btree_mask/hash_mask 等，本脚本不适用。
"""
import struct
import sys

BLCKSZ = 8192

# ---- PageHeaderData（src/include/storage/bufpage.h）----
PD_LSN_OFF        = 0    # 8 字节
PD_CHECKSUM_OFF   = 8    # 2
PD_FLAGS_OFF      = 10   # 2
PD_LOWER_OFF      = 12   # 2
PD_UPPER_OFF      = 14   # 2
PD_SPECIAL_OFF    = 16   # 2
PD_PAGESIZE_OFF   = 18   # 2
PD_PRUNE_XID_OFF  = 20   # 4
PD_LINP_OFF       = 24   # ItemIdData[]

# pd_flags 里属于"提示"的三位（mask_page_hint_bits 会清掉）
PD_HAS_FREE_LINES = 0x0001
PD_PAGE_FULL      = 0x0002
PD_ALL_VISIBLE    = 0x0004
PD_HINT_FLAGS     = PD_HAS_FREE_LINES | PD_PAGE_FULL | PD_ALL_VISIBLE

# ---- ItemIdData 位域（小端）----
LP_UNUSED, LP_NORMAL, LP_REDIRECT, LP_DEAD = 0, 1, 2, 3

# ---- HeapTupleHeaderData（src/include/access/htup_details.h）----
TUP_CID_OFF       = 8    # t_choice.t_heap.t_field3.t_cid，4 字节
TUP_CTID_OFF      = 12   # 6
TUP_INFOMASK2_OFF = 18   # 2
TUP_INFOMASK_OFF  = 20   # 2
TUP_HOFF_OFF      = 22   # 1

HEAP_XACT_MASK       = 0xFFF0   # 可见性相关位
HEAP_XMIN_COMMITTED  = 0x0100
HEAP_XMIN_INVALID    = 0x0200
HEAP_XMIN_FROZEN     = HEAP_XMIN_COMMITTED | HEAP_XMIN_INVALID
HEAP_XMAX_COMMITTED  = 0x0400
HEAP_XMAX_INVALID    = 0x0800

MAXALIGN_SIZE = 8


def maxalign(n):
    return (n + MAXALIGN_SIZE - 1) & ~(MAXALIGN_SIZE - 1)


def line_pointers(page, lower):
    """解析行指针数组：返回 [(序号, lp_off, lp_flags, lp_len)]。"""
    out = []
    if lower < PD_LINP_OFF:
        return out
    for i in range((lower - PD_LINP_OFF) // 4):
        (itemid,) = struct.unpack_from('<I', page, PD_LINP_OFF + 4 * i)
        out.append((i + 1,
                    itemid & 0x7FFF,            # lp_off
                    (itemid >> 15) & 0x3,       # lp_flags
                    (itemid >> 17) & 0x7FFF))   # lp_len
    return out


def masked_offsets(page, lower, upper):
    """
    返回本页中"按 heap_mask 规则不参与比较"的字节偏移集合。
    只用 leader 页解析结构 —— 行指针本身在严格比较范围内，两侧必然一致；
    若不一致，那份差异会照常被报出来。
    """
    m = set()

    # (2) pd_prune_xid 整体豁免；pd_flags 只豁免三个提示位（按位处理，见下）
    m.update(range(PD_PRUNE_XID_OFF, PD_PRUNE_XID_OFF + 4))

    # (3) 空洞
    m.update(range(lower, upper))

    for (_, lp_off, lp_flags, lp_len) in line_pointers(page, lower):
        # (6) 对齐填充：只要该行指针有存储就适用（NORMAL 与 REDIRECT 之外
        #     lp_len 为 0，maxalign 差值也就是 0，天然不产生豁免）
        if lp_len > 0 and lp_off + maxalign(lp_len) <= BLCKSZ:
            m.update(range(lp_off + lp_len, lp_off + maxalign(lp_len)))

        if lp_flags != LP_NORMAL or lp_len < 23:
            continue
        if lp_off + lp_len > BLCKSZ:
            continue

        # (5) t_cid
        m.update(range(lp_off + TUP_CID_OFF, lp_off + TUP_CID_OFF + 4))
        # (4) t_infomask 的可见性位 —— 整两字节纳入掩码，
        #     非提示位的差异由下面的 infomask 专项检查兜住
        m.add(lp_off + TUP_INFOMASK_OFF)
        m.add(lp_off + TUP_INFOMASK_OFF + 1)

    return m


def infomask_violations(pa, pb, lower):
    """
    t_infomask 被整体纳入掩码后，仍要单独检查"掩码之外的位"有没有分歧 ——
    否则 HEAP_UPDATED、HEAP_HASNULL 之类的真实差异会被一起放过。

    规则照 heap_mask()：
      - xmin 未冻结 → 掩掉 HEAP_XACT_MASK，其余位必须相同；
      - xmin 已冻结 → 只掩 HEAP_XMAX_INVALID | HEAP_XMAX_COMMITTED，其余必须相同。
    另外补一条内核不需要、但对本项目必要的检查：leader 已冻结而 follower 未冻结
    说明**丢了冻结记录**（XLOG_HEAP2_FREEZE_PAGE 是写 WAL 的），必须报出来。
    """
    bad = []
    for (n, lp_off, lp_flags, lp_len) in line_pointers(pa, lower):
        if lp_flags != LP_NORMAL or lp_len < 23 or lp_off + lp_len > BLCKSZ:
            continue
        (ma,) = struct.unpack_from('<H', pa, lp_off + TUP_INFOMASK_OFF)
        (mb,) = struct.unpack_from('<H', pb, lp_off + TUP_INFOMASK_OFF)

        a_frozen = (ma & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
        b_frozen = (mb & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
        if a_frozen and not b_frozen:
            bad.append((n, lp_off, ma, mb, "leader 已冻结而 follower 未冻结"))
            continue

        keep = ~HEAP_XACT_MASK if not a_frozen \
               else ~(HEAP_XMAX_INVALID | HEAP_XMAX_COMMITTED)
        if (ma & keep & 0xFFFF) != (mb & keep & 0xFFFF):
            bad.append((n, lp_off, ma, mb, "掩码之外的 infomask 位不同"))
    return bad


def main():
    if len(sys.argv) != 3:
        print("usage: pagecmp.py <leader_file> <follower_file>", file=sys.stderr)
        return 2

    try:
        a = open(sys.argv[1], 'rb').read()
        b = open(sys.argv[2], 'rb').read()
    except OSError as e:
        print("DIFF open_error")
        print(e, file=sys.stderr)
        return 2

    if len(a) != len(b):
        print("DIFF size %d vs %d" % (len(a), len(b)))
        return 1

    outside_total = 0
    masked_total = 0

    for p in range(len(a) // BLCKSZ):
        pa = a[p * BLCKSZ:(p + 1) * BLCKSZ]
        pb = b[p * BLCKSZ:(p + 1) * BLCKSZ]

        lower_a, upper_a = struct.unpack_from('<HH', pa, PD_LOWER_OFF)
        lower_b, upper_b = struct.unpack_from('<HH', pb, PD_LOWER_OFF)

        if (lower_a, upper_a) != (lower_b, upper_b):
            print("DIFF page%d hole_mismatch [%d,%d) vs [%d,%d)"
                  % (p, lower_a, upper_a, lower_b, upper_b))
            return 1

        if not (PD_LINP_OFF <= lower_a <= upper_a <= BLCKSZ):
            print("DIFF page%d bad_header lower=%d upper=%d"
                  % (p, lower_a, upper_a))
            return 1

        masked = masked_offsets(pa, lower_a, upper_a)

        # pd_flags：只豁免三个提示位，其余位严格比较
        (fa,) = struct.unpack_from('<H', pa, PD_FLAGS_OFF)
        (fb,) = struct.unpack_from('<H', pb, PD_FLAGS_OFF)
        if (fa & ~PD_HINT_FLAGS) == (fb & ~PD_HINT_FLAGS):
            masked.add(PD_FLAGS_OFF)
            masked.add(PD_FLAGS_OFF + 1)

        outside = [i for i in range(BLCKSZ)
                   if pa[i] != pb[i] and i not in masked]
        viol = infomask_violations(pa, pb, lower_a)

        nmask = sum(1 for i in range(BLCKSZ) if pa[i] != pb[i] and i in masked)
        outside_total += len(outside) + len(viol)
        masked_total += nmask

        print("  page%d hole=[%d,%d) 掩码内差异=%d 掩码外差异=%d infomask 违例=%d"
              % (p, lower_a, upper_a, nmask, len(outside), len(viol)),
              file=sys.stderr)

        for off in outside:
            print("    偏移 %4d  leader=0x%02X follower=0x%02X"
                  % (off, pa[off], pb[off]), file=sys.stderr)
        for (n, lp_off, ma, mb, why) in viol:
            print("    lp[%d] off=%d t_infomask leader=0x%04X follower=0x%04X —— %s"
                  % (n, lp_off, ma, mb, why), file=sys.stderr)

    if outside_total == 0:
        print("IDENTICAL_OUTSIDE_HOLE")
        print("掩码之外逐字节一致（%d 字节差异落在内核 heap_mask 声明不可比的字段里："
              "空洞 / pd_prune_xid / pd_flags 提示位 / 可见性提示位 / t_cid / "
              "元组对齐填充。pd_lsn 未掩，仍要求逐字节相同）" % masked_total,
              file=sys.stderr)
        return 0

    print("DIFF %d" % outside_total)
    return 1


if __name__ == '__main__':
    sys.exit(main())
