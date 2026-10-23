#!/usr/bin/env bash
# ============================================================================
# [145 宿主机] txn_demo_writer.sh —— 事务演示用的持续写入脚本（lab 版；pg-test 版见 ../txn_demo_writer.sh）
#
# 分布式事务演示的第一步：在 145 上单独开一个终端启动，演示做完、删表之前按 Ctrl+C 停止
# （正在执行的那一条做完再停；停下时打印三种写入各成功多少次，以及库里区域号 9 的实际行数）。
#
# 三种写入轮流进行。每条都是不开事务的单条 INSERT（自动提交、只写一个分片），每条数据都不同：
#   ① 只插入客户表：在随机仓库（1–4）、区域号 9 新建一个客户（名字、余额随机）
#   ② 只插入订单表：给本次已建的某个客户下一张订单（订单行数随机）
#   ③ 同时插入两表：新建一个客户，紧接着给他下一张订单 —— 两条独立的 INSERT，
#      同一仓库，所以客户和订单落在同一个分片（共置）
#
# 区域号 9（c_d_id = o_d_id = 9）专给持续写入用；演示里手工操作的客户、订单不要用区域号 9，主键就不会重叠。
# 客户号、订单号接着表里区域号 9 已有的最大值往下编，重复启动也不会撞主键。
#
# 连法：经 docker exec 进 145 本机的 worker1 容器（shardpg-ddl-worker1），从里面用 TCP 连 master；
#       master 的 IP、端口取同目录 nodes.conf 里节点号 1 那一行（148:32432），口令走容器里的 PGPASSFILE。
#       所以只能在 145 上跑（别的机器上没有 worker1 容器）。
# 用法：bash txn_demo_writer.sh                 # 每轮之间隔 0.2 秒
#       INTERVAL=1 bash txn_demo_writer.sh      # 放慢一点
# 前提：第二步已经建好 customer / orders。**表还在建的时候不要启动**（往正在建的表里写会让建表失败，P7-N93）。
# ============================================================================
set -u
stop=0
D=$(cd "$(dirname "$0")" && pwd)
C="${CONTAINER:-shardpg-ddl-worker1}"
read -r HOST PORT < <(awk '$2 == 1 {print $3, $4}' "$D/nodes.conf" 2>/dev/null)
HOST=${HOST:-10.77.110.148}; PORT=${PORT:-32432}
INTERVAL="${INTERVAL:-0.2}"
docker ps --format '{{.Names}}' | grep -qx "$C" || { echo "本机没有在运行的 $C 容器（这个脚本要在 145 上跑）" >&2; exit 1; }
# setsid：psql 放进单独的会话，Ctrl+C 只通知本脚本、不会打断正在执行的那条 INSERT —— 它做完、再停
# 145 是 CentOS 7，setsid（util-linux 2.23）没有 -w；脚本里的子进程不是进程组长，不带 -w 时 setsid 不 fork，
# 照样等 psql 做完、带回它的退出码（145 上实测过）
SETSID=(setsid -w); setsid -w true 2>/dev/null || SETSID=(setsid)
PSQL=("${SETSID[@]}" docker exec -e PGCLIENTENCODING=UTF8 -u postgres "$C" /work/pg-install/bin/psql
      -h "$HOST" -p "$PORT" -U postgres -X -q -v ON_ERROR_STOP=1)

FIRST=(Ada Ben Cara Dan Eli Fay Gus Hana Ian Jill Kai Lena Max Nora Owen Pia Quin Rose Sam Tia Uma Vic Wes Xia Yan Zoe)
LAST=(Li Wang Zhang Liu Chen Yang Zhao Huang Zhou Wu Xu Sun Hu Zhu Gao Lin He Guo Ma Luo)

pick_first() { printf '%s' "${FIRST[RANDOM % ${#FIRST[@]}]}"; }
pick_last()  { printf '%s' "${LAST[RANDOM % ${#LAST[@]}]}"; }
money()      { printf '%d.%02d' $((RANDOM % 5000)) $((RANDOM % 100)); }

# 取一个值（失败时输出空）
val() { "${PSQL[@]}" -At -c "$1" </dev/null 2>/dev/null | tail -1; }

# 执行一条或几条 INSERT（每个 -c 单独执行、单独提交）；失败时打出错误
run() {
    local out rc
    out=$("${PSQL[@]}" "$@" </dev/null 2>&1); rc=$?
    if [[ $rc -ne 0 ]]; then
        printf '           失败：%s\n' "$(grep -m1 -E 'ERROR|FATAL|error' <<<"$out")"
    fi
    return $rc
}

base_c=$(val "SELECT coalesce(max(c_id), 0) FROM customer WHERE c_d_id = 9")
base_o=$(val "SELECT coalesce(max(o_id), 0) FROM orders WHERE o_d_id = 9")
if ! [[ "$base_c" =~ ^[0-9]+$ && "$base_o" =~ ^[0-9]+$ ]]; then
    echo "连不上数据库，或者 customer / orders 还不存在（先做演示第 1 步建表）" >&2
    exit 1
fi
next_c=$((base_c + 1))
next_o=$((base_o + 1))

CW=()        # 本次新建客户的仓库号
CI=()        # 本次新建客户的客户号
n1=0; n2=0; n3=0; nfail=0; round=0
trap 'stop=1' INT TERM

printf '持续写入开始：区域号 9，客户号从 %d、订单号从 %d 起；按 Ctrl+C 停止\n' "$next_c" "$next_o"

while (( ! stop )); do
    round=$((round + 1))
    kind=$((round % 3)); (( kind == 0 )) && kind=3
    (( kind == 2 && ${#CW[@]} == 0 )) && kind=1      # 还没有客户时先建客户
    now=$(date +%T)

    case $kind in
    1)  # ① 只插入客户表
        w=$((1 + RANDOM % 4)); cid=$next_c; f=$(pick_first); l=$(pick_last); bal=$(money)
        if run -c "INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance)
                   VALUES ($w, 9, $cid, '$f', '$l', $bal)"; then
            CW+=("$w"); CI+=("$cid"); next_c=$((next_c + 1)); n1=$((n1 + 1))
            printf '[%s] #%-4d ① 只插客户表    customer(%d, 9, %d)  %s %s  余额 %s\n' \
                "$now" "$round" "$w" "$cid" "$f" "$l" "$bal"
        else
            nfail=$((nfail + 1))
        fi
        ;;
    2)  # ② 只插入订单表：给已有的某个客户下单
        k=$((RANDOM % ${#CW[@]})); w=${CW[$k]}; cid=${CI[$k]}; oid=$next_o; cnt=$((1 + RANDOM % 15))
        if run -c "INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt)
                   VALUES ($w, 9, $oid, $cid, $cnt)"; then
            next_o=$((next_o + 1)); n2=$((n2 + 1))
            printf '[%s] #%-4d ② 只插订单表    orders(%d, 9, %d)  客户 %d  %d 行\n' \
                "$now" "$round" "$w" "$oid" "$cid" "$cnt"
        else
            nfail=$((nfail + 1))
        fi
        ;;
    3)  # ③ 同时插入两表：新客户 + 他的第一张订单（两条独立 INSERT，同一仓库 → 同一分片）
        w=$((1 + RANDOM % 4)); cid=$next_c; oid=$next_o; f=$(pick_first); l=$(pick_last); bal=$(money)
        cnt=$((1 + RANDOM % 15))
        if run -c "INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance)
                   VALUES ($w, 9, $cid, '$f', '$l', $bal)" \
               -c "INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt)
                   VALUES ($w, 9, $oid, $cid, $cnt)"; then
            CW+=("$w"); CI+=("$cid"); next_c=$((next_c + 1)); next_o=$((next_o + 1)); n3=$((n3 + 1))
            printf '[%s] #%-4d ③ 客户 + 订单   customer(%d, 9, %d) %s %s  余额 %s  →  orders(%d, 9, %d)  %d 行\n' \
                "$now" "$round" "$w" "$cid" "$f" "$l" "$bal" "$w" "$oid" "$cnt"
        else
            nfail=$((nfail + 1))
            # 客户可能已经插进去、订单失败：按库里实际情况对齐编号
            [[ "$(val "SELECT count(*) FROM customer WHERE c_w_id = $w AND c_d_id = 9 AND c_id = $cid")" == 1 ]] \
                && { CW+=("$w"); CI+=("$cid"); next_c=$((next_c + 1)); }
        fi
        ;;
    esac
    (( stop )) && break
    sleep "$INTERVAL"
done

trap - INT TERM
printf '\n持续写入已停止：① 只插客户表 %d 次，② 只插订单表 %d 次，③ 客户 + 订单 %d 次，失败 %d 次\n' \
    "$n1" "$n2" "$n3" "$nfail"
printf '库里区域号 9 现有：客户 %s 个，订单 %s 张\n' \
    "$(val "SELECT count(*) FROM customer WHERE c_d_id = 9")" \
    "$(val "SELECT count(*) FROM orders WHERE o_d_id = 9")"
