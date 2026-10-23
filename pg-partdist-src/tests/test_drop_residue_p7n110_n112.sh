#!/usr/bin/env bash
# ============================================================================
# [宿主机] P7-N110 / P7-N112 验收（2026-10-20）：删表后不留 hardstate 文件、不留主从登记
#
#   N110：删组与写 hardstate 文件之间没有互斥，写者的 rename 落在删文件之后会把文件写回来，
#         组已不在内存、拆组和核对都看不到它。修法：删组先标空闲后删文件、写者 rename 后再看槽位；
#         自动清理拆完组按组号删文件（pg_raft_hardstate_purge）；启动时清写到一半的 .tmp 文件。
#   N112：主从登记经控制面日志写进各节点 partition_map，删除却是各节点直接 DELETE，装快照重放后会回来。
#         修法：删除也走控制面日志（OP_PARTITION_DROP，partdist.partition_map_unregister）。
#
#   [0] 前提：新函数在所有节点上；开始时没有旧残留
#   [1] 建表 → 写入 → 删表，连做 3 轮：每轮三项都清零 —— 各 worker 的 hardstate 文件、各节点的登记、组
#   [2] 删登记走了控制面日志：每个节点的组 0 日志里都有这一轮的 OP_PARTITION_DROP，带着这些分片号
#   [3] pg_raft_hardstate_purge 不碰还在用的组；能删孤儿文件
#   [4] 启动时清掉写到一半的 hardstate 临时文件（重启一台 worker）
#   [5] partdist.residue_gc() 在干净的集群上什么都不删
#
# N110 的竞争窗口无法确定性复现（要求写者恰好在删文件之后 rename），[1] 连做多轮只是增加碰上的机会；
# 修法的正确性靠两边的执行顺序保证，见 raft_consensus.c persist_hard_state_values 的注释。
#
# 前提：pg-test 1c+4w，pg_raft.so / pg_partdist.so 为 N110/N112 修复后的版本，函数已滚到各节点。
# 用法：bash test_drop_residue_p7n110_n112.sh        约 6–8 分钟（含重启一台 worker）
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
R=/work/pg-cluster-data
WORKERS="5433 5434 5435 5436"
ALL="5432 $WORKERS"
ROUNDS="${ROUNDS:-3}"
PASS=0; FAIL=0
q()   { docker exec -u postgres -e PGOPTIONS=-cclient_min_messages=warning "$C" $BIN/psql -h /tmp -p "${2:-5432}" -U postgres -X -qAt -c "$1" </dev/null 2>&1; }
ok()  { echo "  ✓ $1"; PASS=$((PASS + 1)); }
no()  { echo "  ✗ $1"; FAIL=$((FAIL + 1)); }
check() { if [[ "$2" == "$3" ]]; then ok "$1（$2）"; else no "$1：实际 [$2]，期望 [$3]"; fi; }
hs_count() {   # $1 = 逗号分隔的组号；各 worker 上这些组的 hardstate 文件数
    local p out=""
    for p in $WORKERS; do
        out="$out :$p=$(q "SELECT count(*) FROM pg_ls_dir('.') f WHERE f IN (SELECT 'pg_raft_hardstate.' || x FROM unnest(ARRAY[$1]) x)" "$p")"
    done
    printf '%s' "${out# }"
}
pm_count() {   # 各节点上这些分区的登记行数
    local p out=""
    for p in $ALL; do out="$out :$p=$(q "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN ($1)" "$p")"; done
    printf '%s' "${out# }"
}
grp_count() {
    local p out=""
    for p in $WORKERS; do out="$out :$p=$(q "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN ($1)" "$p")"; done
    printf '%s' "${out# }"
}
zeros() { local p e=""; for p in $1; do e="$e :$p=0"; done; printf '%s' "${e# }"; }
cleanup() { q "DROP TABLE IF EXISTS r110_o, r110_c" >/dev/null; }
trap cleanup EXIT

echo "================ [0] 前提 ================"
cleanup
bad=0
for p in $ALL; do
    n=$(q "SELECT count(*) FROM pg_proc WHERE proname IN ('pg_raft_hardstate_purge','partition_map_unregister','residue_gc')" "$p")
    [[ "$n" == 3 ]] || { bad=1; echo "      :$p 只有 $n 个新函数"; }
done
check "新函数在 5 个节点上" "$bad" "0"
check "开始时没有旧残留" "$(q "SELECT sum(removed) FROM partdist.residue_gc()")" "0"

echo "================ [1] 建表 → 写入 → 删表，连做 $ROUNDS 轮 ================"
last_ids=""
for r in $(seq 1 "$ROUNDS"); do
    q "CREATE TABLE r110_c (w int NOT NULL, id int NOT NULL, v text, PRIMARY KEY (w, id), SHARD KEY (w) SHARDS 4 REPLICAS 2)" >/dev/null
    q "CREATE TABLE r110_o (w int NOT NULL, oid_ int NOT NULL, PRIMARY KEY (w, oid_), SHARD KEY (w) COLOCATE WITH r110_c)" >/dev/null
    ids=$(q "SELECT string_agg(shardid::text, ',' ORDER BY shardid) FROM pg_dist_shard WHERE logicalrelid IN ('r110_c'::regclass, 'r110_o'::regclass)")
    q "INSERT INTO r110_c SELECT g % 8 + 1, g, 'v' || g FROM generate_series(1, 200) g" >/dev/null
    q "INSERT INTO r110_o SELECT g % 8 + 1, g FROM generate_series(1, 200) g" >/dev/null
    check "第 $r 轮：建好、各 worker 上有这些组的 hardstate 文件" \
          "$(hs_count "$ids" | grep -c ':54[0-9][0-9]=[1-9]')" "1"
    q "DROP TABLE r110_o, r110_c" >/dev/null
    check "第 $r 轮删表后：Raft 组" "$(grp_count "$ids")" "$(zeros "$WORKERS")"
    check "第 $r 轮删表后：hardstate 文件" "$(hs_count "$ids")" "$(zeros "$WORKERS")"
    check "第 $r 轮删表后：主从登记（含 master）" "$(pm_count "$ids")" "$(zeros "$ALL")"
    last_ids="$ids"
done

echo "================ [2] 删登记走了控制面日志 ================"
bad=0
for p in $ALL; do
    n=$(q "SELECT count(*) FROM partdist.raft_log WHERE group_id = 0 AND op_type = 'OP_PARTITION_DROP'
             AND (payload -> 'partition_ids') @> to_jsonb(ARRAY[$last_ids]::bigint[])" "$p")
    [[ "$n" -ge 1 ]] 2>/dev/null || { bad=1; echo "      :$p 的组 0 日志里没有最后一轮的 OP_PARTITION_DROP（$n）"; }
done
check "5 个节点的组 0 日志里都有最后一轮的 OP_PARTITION_DROP" "$bad" "0"

echo "================ [3] pg_raft_hardstate_purge ================"
q "CREATE TABLE r110_c (w int NOT NULL, id int NOT NULL, PRIMARY KEY (w, id), SHARD KEY (w) SHARDS 2 REPLICAS 2)" >/dev/null
sid=$(q "SELECT min(shardid) FROM pg_dist_shard WHERE logicalrelid = 'r110_c'::regclass")
mem=$(for p in $WORKERS; do [[ "$(q "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id = $sid" "$p")" == 1 ]] && { echo "$p"; break; }; done)
check "还在用的组：purge 返回 0" "$(q "SELECT partdist.pg_raft_hardstate_purge(ARRAY[$sid]::bigint[])" "$mem")" "0"
check "还在用的组：文件还在" "$(q "SELECT count(*) FROM pg_ls_dir('.') f WHERE f = 'pg_raft_hardstate.$sid'" "$mem")" "1"
q "DROP TABLE r110_c" >/dev/null
docker exec -u postgres "$C" sh -c "printf x > $R/worker1/pg_raft_hardstate.990001"
check "孤儿文件：purge 返回 1" "$(q "SELECT partdist.pg_raft_hardstate_purge(ARRAY[990001]::bigint[])" 5433)" "1"
check "孤儿文件：已删" "$(docker exec "$C" sh -c "ls $R/worker1 | grep -c '^pg_raft_hardstate.990001\$'")" "0"
check "组 0 不受理" "$(q "SELECT partdist.pg_raft_hardstate_purge(ARRAY[0]::bigint[])" 5433)" "0"

echo "================ [4] 启动时清写到一半的 hardstate 临时文件 ================"
docker exec -u postgres "$C" sh -c "printf x > $R/worker4/pg_raft_hardstate.990002.tmp.4242; printf x > $R/worker4/pg_raft_hardstate.tmp.4243"
mark=$(docker exec -u postgres "$C" date -u '+%Y-%m-%d %H:%M:%S')
docker exec -u postgres "$C" $BIN/pg_ctl -D $R/worker4 -m fast restart -l $R/worker4/pg.log -w -t 90 >/dev/null
check "重启后两个临时文件都没了" "$(docker exec "$C" sh -c "ls $R/worker4 | grep -c 'pg_raft_hardstate.*\.tmp\.'")" "0"
check "日志里记了一条清理" \
      "$(docker exec "$C" sh -c "awk -v m='$mark' '\$1\" \"\$2 >= m' $R/worker4/pg.log | grep -c '启动时清掉 2 个写到一半的 hardstate 临时文件'")" "1"
for t in $(seq 1 30); do
    st=$(q "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = 0" 5436)
    [[ "$st" == follower || "$st" == leader ]] && break; sleep 2
done
check "worker4 回到控制面" "$([[ "$st" == follower || "$st" == leader ]] && echo yes)" "yes"

echo "================ [5] residue_gc 在干净的集群上 ================"
check "什么都不删" "$(q "SELECT sum(removed) FROM partdist.residue_gc()")" "0"

echo
echo "================ 结果：$PASS 通过 / $FAIL 失败 ================"
if [[ $((PASS + FAIL)) -lt 20 ]]; then echo "FATAL：只跑了 $((PASS + FAIL)) 项（应 >= 20），夹具中途退出"; exit 98; fi
[[ $FAIL -eq 0 ]]
