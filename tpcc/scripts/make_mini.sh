#!/usr/bin/env bash
# 把套件就地改成"迷你规模"（只用于冒烟 / 短跑，结果不能与标准规模对比）：
#   商品 100000 → 10000（stock 每仓同步）、每区客户 3000 → 300、每区订单 3000 → 300
#   （未发货分界 2100 → 210）、按姓氏查客户的 NURand 范围 0..999 → 0..299。
# 金额（w_ytd 300000 / d_ytd 30000）不动，一致性条件照样成立。
# 在一份拷贝上跑：bash scripts/make_mini.sh；会改 sql/ 与 scripts/ 下的文件。
set -euo pipefail
D="$(cd "$(dirname "$0")/.." && pwd)"
grep -q 'MINI_SCALE' "$D/sql/04_load_funcs.sql" && { echo "已是迷你规模"; exit 0; }

sed -i -e 's/generate_series(1, 100000) i/generate_series(1, 10000) i/' "$D/sql/03_distribute.sql"

sed -i -e 's/% 3000 + 1/% 300 + 1/' -e 's/每个区 3000 单对 1..3000/每个区 300 单对 1..300（MINI）/' "$D/sql/02_gen_funcs.sql"

sed -i -e '1i -- MINI_SCALE：由 scripts/make_mini.sh 生成' \
       -e 's/, 3001,/, 301,/' \
       -e 's/generate_series(1, 3000) c/generate_series(1, 300) c/g' \
       -e 's/generate_series(1, 3000) o/generate_series(1, 300) o/g' \
       -e 's/o <= 2100/o <= 210/g' \
       -e 's/tpcc.rand_int(1, 100000)/tpcc.rand_int(1, 10000)/' \
       -e 's/generate_series(2101, 3000) o/generate_series(211, 300) o/' \
       "$D/sql/04_load_funcs.sql"

sed -i -e 's/tpcc.nurand(1023, 1, 3000,/tpcc.nurand(1023, 1, 300,/g' \
       -e 's/tpcc.nurand(8191, 1, 100000,/tpcc.nurand(8191, 1, 10000,/g' \
       -e 's/tpcc.nurand(255, 0, 999, tpcc.c_last_run())/tpcc.nurand(255, 0, 299, tpcc.c_last_run())/g' \
       "$D/sql/05_txn_funcs.sql"

sed -i -e 's/for lo in 1 20001 40001 60001 80001; do/for lo in 1; do/' \
       -e 's/\$((lo+19999))/$((lo+9999))/' "$D/scripts/load.sh"

sed -i -e 's/\$((W\*30000))/$((W*3000))/g' -e 's/\$((W\*9000))/$((W*900))/' \
       -e 's/\$((W\*100000))/$((W*10000))/' -e 's/"100000"          /"10000"           /' \
       "$D/scripts/build_db.sh"

# 改完自检：旧常量不应残留
left=$(grep -nE 'generate_series\(1, (3000|100000)\)|nurand\((1023, 1, 3000|8191, 1, 100000)|o <= 2100|2101, 3000|W\*(30000|100000|9000)|20001' \
       "$D"/sql/0[2-5]*.sql "$D/scripts/load.sh" "$D/scripts/build_db.sh" || true)
[[ -z "$left" ]] || { echo "残留标准规模常量："; echo "$left"; exit 1; }
echo "已改为迷你规模（商品 1 万、每区客户/订单 300）"
