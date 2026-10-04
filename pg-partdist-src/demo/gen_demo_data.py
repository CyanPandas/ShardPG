#!/usr/bin/env python3
# 生成演示用的业务数据文件。
# 用法：python3 gen_demo_data.py [行数] [输出文件] [表名] [每批行数]
#   演示一：python3 gen_demo_data.py 500  demo_data.sql    account 50    （缺省）
#   演示二：python3 gen_demo_data.py 256  demo128_data.sql wallet  256
#
# 为什么要生成成文件、而且要分批：打标表的每条记录都要 propose 给该分片的 Raft 组
# 等多数派落盘，所以演示里这一步是"看着它灌"的；分批提交能让进度可见，
# 也避免一个超大事务把日志环顶满。演示二只有一批 256 行：哈希后落到一百多个分区上，
# 就是一笔跨一百多个分区组的分布式事务（这台 2 vCPU 机器上约 1 分钟）。
import random, sys

rows  = int(sys.argv[1]) if len(sys.argv) > 1 else 500
out   = sys.argv[2] if len(sys.argv) > 2 else "demo_data.sql"
table = sys.argv[3] if len(sys.argv) > 3 else "account"
batch = int(sys.argv[4]) if len(sys.argv) > 4 else 50
random.seed(20260923)
CITY = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安"]

L = []
L.append("-- 业务数据：%d 个%s，分 %d 批灌入（每批一个全局事务）。"
         % (rows, "账户" if table == "account" else "钱包", (rows + batch - 1) // batch))
L.append("-- 跨分片写必须加入全局事务：demo.global_txn() 取全局事务号 + TSO 快照时间戳。")
L.append("\\timing on")
def row(i):
    # 演示二：最后一个钱包是平台的红包账户（第 4、6 步从它给用户发红包），余额给足
    if table == "wallet" and i == rows:
        return "(%d,'平台红包账户',100000)" % i
    return "(%d,'%s%04d',%d)" % (i, random.choice(CITY), i, random.randrange(100, 10000))

for lo in range(1, rows + 1, batch):
    hi = min(lo + batch - 1, rows)
    vals = ",".join(row(i) for i in range(lo, hi + 1))
    L.append("BEGIN;")
    L.append("SELECT demo.global_txn();")
    L.append("INSERT INTO %s VALUES %s;" % (table, vals))
    L.append("COMMIT;")
L.append("\\timing off")
open(out, "w", encoding="utf-8").write("\n".join(L) + "\n")
print("已生成 %s：%d 行，%d 批" % (out, rows, (rows + batch - 1) // batch))
