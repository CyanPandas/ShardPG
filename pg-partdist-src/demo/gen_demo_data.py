#!/usr/bin/env python3
# 生成演示用的业务数据文件（demo_data.sql）。
# 用法：python3 gen_demo_data.py [行数] [输出文件]
#
# 为什么要生成成文件、而且要分批：打标表的每条记录都要 propose 给该分片的 Raft 组
# 等多数派落盘（实测约 1.8 行/秒），所以演示里这一步是"看着它灌"的；分批提交能
# 让进度可见，也避免一个超大事务把日志环顶满。
import random, sys

rows = int(sys.argv[1]) if len(sys.argv) > 1 else 500
out  = sys.argv[2] if len(sys.argv) > 2 else "demo_data.sql"
batch = 50
random.seed(20260923)
CITY = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安"]

L = []
L.append("-- 业务数据：%d 个账户，分 %d 批灌入（每批一个全局事务）。" % (rows, (rows + batch - 1) // batch))
L.append("-- 跨分片写必须加入全局事务：demo.global_txn() 取全局事务号 + TSO 快照时间戳。")
L.append("\\timing on")
for lo in range(1, rows + 1, batch):
    hi = min(lo + batch - 1, rows)
    vals = ",".join("(%d,'%s%04d',%d)" % (i, random.choice(CITY), i, random.randrange(100, 10000))
                    for i in range(lo, hi + 1))
    L.append("BEGIN;")
    L.append("SELECT demo.global_txn();")
    L.append("INSERT INTO account VALUES %s;" % vals)
    L.append("COMMIT;")
L.append("\\timing off")
open(out, "w", encoding="utf-8").write("\n".join(L) + "\n")
print("已生成 %s：%d 行，%d 批" % (out, rows, (rows + batch - 1) // batch))
