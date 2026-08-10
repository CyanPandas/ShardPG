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

★ 掩码规则**按页类型分派**（2026-08-10 修）。此前无差别套用堆页规则，
两处后果都是"真损坏也判一致"：

  - **VM fork**：VM 页由 PageInit 建立，pd_lower 恒为 24、pd_upper 恒为 8192，
    位图就写在 [24, 8192) 里 —— 而"空洞"掩码正是 range(pd_lower, pd_upper)，
    于是**整张位图被掩掉**，只剩 20 字节页头参与比较。实测：位图完全相反
    （0xFF vs 0x00）的两张 VM 页判为 IDENTICAL。回放 XLOG_HEAP2_VISIBLE 时
    漏位或落错块，一概测不出来。VM 页没有"空闲空洞"这个概念，套用堆页规则
    是类型错误，现改为整页严格比较。
  - **btree 页**：lp_len >= 23 的项会被当作堆元组去掩 t_cid(off+8..11) 与
    t_infomask(off+20..21)，而 IndexTuple 头只有 8 字节 —— 那两段**就是索引
    键数据**。现按内核 btree_mask() 的口径处理：掩空洞 + 提示位 + 行指针的
    LP_DEAD 位 + 特殊区的 BTP_HAS_GARBAGE，**不碰元组内容**。

另外：0 页文件此前判 IDENTICAL（比了零个页面）。现输出独立标记
IDENTICAL_EMPTY，让"这里本该有内容"的调用方能区分出来。

用法: pagecmp.py [--kind=heap|btree|vm] <leader_file> <follower_file>
      kind 由调用方按 fileset 的 role 传入：0/2(主堆/TOAST 堆)=heap，
      1/3(索引/TOAST 索引)=btree，_vm 后缀的文件=vm。缺省 heap。
输出: 一行 "IDENTICAL_OUTSIDE_HOLE" 或 "DIFF <掩码之外的差异字节数>"，
      并在 stderr 打印每页明细。退出码 0=一致。

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

# ---- BTPageOpaqueData（src/include/access/nbtree.h），位于页尾 ----
# btpo_prev(4) btpo_next(4) btpo_level(4) btpo_flags(2) xact(4) = 16 字节，
# btpo_flags 在特殊区起始 +12 处。
BTPO_SIZE            = 16
BTPO_FLAGS_REL_OFF   = 12
BTP_HAS_GARBAGE      = 0x0040   # 内核 btree_mask() 明确掩掉这一位
BTP_LEAF             = 0x0001


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


def masked_offsets_btree(page, lower, upper):
    """
    btree 页的掩码集合，口径抄内核 btree_mask()（src/backend/access/nbtree/nbtxlog.c）：
    mask_page_hint_bits + mask_unused_space + 叶页的 LP_DEAD 位 + BTP_HAS_GARBAGE。
    **不解析元组内容** —— IndexTuple 只有 8 字节头，堆元组那套偏移套上去
    掩到的是索引键本身。
    """
    m = set()
    m.update(range(PD_PRUNE_XID_OFF, PD_PRUNE_XID_OFF + 4))
    m.update(range(lower, upper))                      # 空闲空洞

    # 行指针的 lp_flags：LP_DEAD 是**提示**，由读取者顺手置上、不写 WAL，
    # 主备不一致是常态（内核 mask_lp_flags 做同样的事）。itemid 的
    # bit15-16 是 lp_flags，落在第 1、2 字节上，整两字节掩掉最省事且够用。
    for i in range((max(lower, PD_LINP_OFF) - PD_LINP_OFF) // 4):
        base = PD_LINP_OFF + 4 * i
        m.add(base + 1)
        m.add(base + 2)

    return m


def btpo_flags_off(page):
    """btree 特殊区里 btpo_flags 的页内偏移；不是合法 btree 页则返回 None。"""
    (special,) = struct.unpack_from('<H', page, PD_SPECIAL_OFF)
    if 0 < special <= BLCKSZ - BTPO_SIZE:
        return special + BTPO_FLAGS_REL_OFF
    return None


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
    kind = 'heap'
    args = []
    for a_ in sys.argv[1:]:
        if a_.startswith('--kind='):
            kind = a_.split('=', 1)[1]
        else:
            args.append(a_)
    if kind not in ('heap', 'btree', 'vm'):
        print("unknown --kind=%s（可选 heap|btree|vm）" % kind, file=sys.stderr)
        return 2

    if len(args) != 2:
        print("usage: pagecmp.py [--kind=heap|btree|vm] <leader> <follower>",
              file=sys.stderr)
        return 2

    try:
        a = open(args[0], 'rb').read()
        b = open(args[1], 'rb').read()
    except OSError as e:
        print("DIFF open_error")
        print(e, file=sys.stderr)
        return 2

    if len(a) != len(b):
        print("DIFF size %d vs %d" % (len(a), len(b)))
        return 1

    # ★ 0 页文件此前直接判 IDENTICAL —— 比了零个页面。用独立标记吐出来，
    # 让"这里本该有内容"的调用方能把它和真正的一致区分开
    # （典型漏网场景：TRUNCATE 把 follower 截 0 之后 FPI 完全没到，
    #  leader 侧新 relfilenode 也还是 0 块，两边都空 ⇒ 整条重填路径失效而全绿）。
    if len(a) == 0:
        print("IDENTICAL_EMPTY")
        print("两侧均为 0 字节：比较了零个页面，不构成内容一致的证据", file=sys.stderr)
        return 0

    # ---- VM fork：整页严格比较 ----
    #
    # VM 页没有"空闲空洞"这个概念：PageInit 之后 pd_lower 恒为 24、
    # pd_upper 恒为 8192，位图就写在这两者之间。套用堆页的空洞掩码等于把
    # 整张位图掩掉（实测位图完全相反也判一致）。VM 的每一位都由
    # XLOG_HEAP2_VISIBLE 或堆记录的清位动作驱动，全部写 WAL ⇒ 物理副本上
    # 应当逐字节相同，没有可豁免的字段。
    if kind == 'vm':
        diffs = [i for i in range(len(a)) if a[i] != b[i]]
        for off in diffs[:32]:
            print("    VM 偏移 %5d  leader=0x%02X follower=0x%02X"
                  % (off, a[off], b[off]), file=sys.stderr)
        if diffs:
            print("DIFF %d" % len(diffs))
            return 1
        print("IDENTICAL_OUTSIDE_HOLE")
        print("VM fork 整页逐字节一致（%d 页，无掩码）" % (len(a) // BLCKSZ),
              file=sys.stderr)
        return 0

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

        if kind == 'btree':
            masked = masked_offsets_btree(pa, lower_a, upper_a)
            # btpo_flags 只豁免 BTP_HAS_GARBAGE 一位，其余结构位严格比较
            bfo = btpo_flags_off(pa)
            if bfo is not None:
                (ba_,) = struct.unpack_from('<H', pa, bfo)
                (bb_,) = struct.unpack_from('<H', pb, bfo)
                if (ba_ & ~BTP_HAS_GARBAGE) == (bb_ & ~BTP_HAS_GARBAGE):
                    masked.add(bfo)
                    masked.add(bfo + 1)
        else:
            masked = masked_offsets(pa, lower_a, upper_a)

        # pd_flags：只豁免三个提示位，其余位严格比较
        (fa,) = struct.unpack_from('<H', pa, PD_FLAGS_OFF)
        (fb,) = struct.unpack_from('<H', pb, PD_FLAGS_OFF)
        if (fa & ~PD_HINT_FLAGS) == (fb & ~PD_HINT_FLAGS):
            masked.add(PD_FLAGS_OFF)
            masked.add(PD_FLAGS_OFF + 1)

        outside = [i for i in range(BLCKSZ)
                   if pa[i] != pb[i] and i not in masked]
        viol = [] if kind == 'btree' else infomask_violations(pa, pb, lower_a)

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
