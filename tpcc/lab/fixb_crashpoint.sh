#!/bin/bash
# 方案 B 的确定性验证：让 2PC 发起方停在崩溃窗口里（pg_raft.dtx_test_pause_point），趁机 docker kill 它，
# 拉起后等 2PC 收尾，核对这笔跨节点事务是不是"要么全提交、要么全回滚"。
#   发起方 = n2（145:31082）上直接开的会话：本地仓 A 的 w_ytd + D，远程仓 B（n3）、可选 C（n4）的 w_ytd − D。
#   点 1：判决已写、本地提交记录未写 ⇒ 期望整笔 ABORT（修复前：远程那一半提交、本地这一半丢 = 一半提交）
#   点 2：本地已提交、COMMIT PREPARED 未发 ⇒ 期望整笔 COMMIT
#   OLDPROTO=1：对照组 —— 临时把 n3/n4 上快路径的 dtx_local_note_decide 换回修复前的写法（直接写 COMMIT 判决），
#     用同一个暂停点杀发起方，应复现"一半提交"（证明本测试抓得到原缺陷）；测完换回。
# 测试改的是 warehouse.w_ytd，结束时把三个仓的 w_ytd 恢复原值（否则破坏 TPC-C 一致性条件 1）。
# 用法（145 上，集群由 redeploy_img.sh shardpg:fixb 部署）：fixb_crashpoint.sh <点 1|2> <远程节点数 1|2>
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
P=${1:?点}; NR=${2:-1}; D=$((RANDOM % 900 + 100))
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
C1="$CLI -h 10.77.110.148 -p 31080"
WK="10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083"
wh_on() { $C1 -c "select w from generate_series(1, 64) w join pg_dist_shard_placement p
  on p.shardid = get_shard_id_for_distribution_column('warehouse', w) where p.nodeport = $1 order by w limit 1"; }
A=$(wh_on 31082); B=$(wh_on 31081); C=$(wh_on 31083)
ytd() { $C1 -c "select string_agg(w_ytd::text, ' ' order by w_id) from warehouse where w_id in ($A, $B, $C)"; }
settled() { local tot=0 c hp; for hp in $WK; do
  c=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null)
  tot=$((tot + ${c:-99})); done; echo $tot; }
# 在 worker 上直接替换（关掉本会话的 Citus 元数据同步，否则 Citus 把它当分布式对象的 DDL 拦下）
notedecide() { local v=$1 hp; for hp in 10.77.110.147:31081 10.77.110.148:31083; do $CLI -h ${hp%:*} -p ${hp#*:} -c "SET citus.enable_metadata_sync TO off" -c "
CREATE OR REPLACE FUNCTION partdist.dtx_local_note_decide(p_dtxid bigint, p_commit_ts bigint)
RETURNS integer LANGUAGE plpgsql VOLATILE AS \$fn\$
DECLARE parts bigint[]; coord bigint; v integer;
BEGIN
    parts := partdist.dtx_local_participant(p_dtxid);
    IF parts IS NULL OR cardinality(parts) = 0 THEN RETURN 0; END IF;
    coord := parts[(p_dtxid % cardinality(parts)) + 1];
    PERFORM partdist.dtx_note_coord(p_dtxid, coord);
    v := partdist.dtx_decide(coord, p_dtxid, $v, parts, p_commit_ts);
    IF v IS NULL THEN RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'dtx_local_note_decide: not coord leader'; END IF;
    RETURN v;
END \$fn\$" >/dev/null; done; }
[ "${OLDPROTO:-0}" = 1 ] && { notedecide 1; log "对照组：n3/n4 的快路径临时换回修复前写法（判决直接写 COMMIT）"; }
for i in $(seq 60); do [ "$(settled)" = 0 ] && break; sleep 2; done
[ "$(settled)" = 0 ] || { log "！！开测前还有未收尾的 2PC（$(settled)），不测"; exit 1; }
Y0=$(ytd); log "点 $P、远程节点 $NR 个：仓 A=$A（n2，发起方）B=$B（n3）C=$C（n4），w_ytd 前 = $Y0，D = $D"
UPD="UPDATE warehouse SET w_ytd = w_ytd - $D WHERE w_id = $B;"
[ "$NR" = 2 ] && UPD="UPDATE warehouse SET w_ytd = w_ytd - $D WHERE w_id = $B; UPDATE warehouse SET w_ytd = w_ytd - $D WHERE w_id = $C;"
SQL="SET pg_raft.dtx_test_pause_point = $P; SET pg_raft.dtx_test_pause_ms = 20000;
BEGIN; SELECT tpcc.begin_global(); UPDATE warehouse SET w_ytd = w_ytd + $D * $NR WHERE w_id = $A; $UPD COMMIT;"
docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -Xq -h 10.77.110.145 -p 31082 -c "$SQL" > /tmp/cp_txn.out 2>&1 &
TX=$!
npause() { docker exec shardpg-fix-n2 bash -c "cat /d/pgdata/log/* | grep -c '测试暂停点 $P'" 2>/dev/null; }
N0=$(npause)   # 日志里已有的暂停记录（上一组留下的），等它增加才算这一笔到了暂停点
for i in $(seq 100); do [ "$(npause)" -gt "${N0:-0}" ] 2>/dev/null && break; sleep 0.2; done
[ $i = 100 ] && { log "！！20 s 内没看到暂停点 $P（事务输出：$(cat /tmp/cp_txn.out | tr '\n' ' ')）"; wait $TX; exit 1; }
docker exec shardpg-fix-n2 bash -c "cat /d/pgdata/log/* | grep '测试暂停点 $P' | tail -1" | cut -c1-200
sleep 1; docker kill shardpg-fix-n2 >/dev/null; T0=$(date +%s); log "已在暂停点 $P kill n2（发起方）"
wait $TX; log "发起方会话：$(tr '\n' ' ' < /tmp/cp_txn.out | cut -c1-200)"
sleep 3; docker start shardpg-fix-n2 >/dev/null
for i in $(seq 120); do docker exec shardpg-fix-n2 /work/pg-install/bin/pg_isready -h 127.0.0.1 -p 31082 -q && break; sleep 1; done
log "n2 已拉起（$(( $(date +%s) - T0 )) s）"
for i in $(seq 120); do [ "$(settled)" = 0 ] && break; sleep 2; done
log "2PC 收尾：剩余 $(settled)（kill 后 $(( $(date +%s) - T0 )) s）"
for i in $(seq 30); do Y1=$(ytd 2>/dev/null); [ -n "$Y1" ] && break; sleep 2; done
read a0 b0 c0 <<< "$Y0"; read a1 b1 c1 <<< "$Y1"
da=$(echo "$a1 - $a0" | bc); db=$(echo "$b1 - $b0" | bc); dc=$(echo "$c1 - $c0" | bc)
log "w_ytd 后 = $Y1；变化：A $da、B $db、C $dc"
exp_commit_a=$(echo "$D * $NR" | bc)
if [ "$(echo "$da == 0 && $db == 0 && $dc == 0" | bc)" = 1 ]; then res="整笔 ABORT"
elif [ "$NR" = 1 ] && [ "$(echo "$da == $D && $db == -$D && $dc == 0" | bc)" = 1 ]; then res="整笔 COMMIT"
elif [ "$NR" = 2 ] && [ "$(echo "$da == $exp_commit_a && $db == -$D && $dc == -$D" | bc)" = 1 ]; then res="整笔 COMMIT"
else res="！！一半提交"; fi
want=$([ "$P" = 1 ] && echo "整笔 ABORT" || echo "整笔 COMMIT")
[ "${OLDPROTO:-0}" = 1 ] && [ "$P" = 1 ] && want="！！一半提交"   # 对照组：修复前的协议应当在这里出错
[ "$res" = "$want" ] && v=通过 || v="！！不符合预期"
log "结果：$res（期望 $want）→ $v"
for hp in 10.77.110.147:31081 10.77.110.148:31083; do
  ssh -n ${hp%:*} "docker exec shardpg-fix-n$([ ${hp#*:} = 31081 ] && echo 3 || echo 4) bash -c 'grep -hE \"问发起方|待确认判决|dtx 恢复\" \$(ls -t /d/pgdata/log/* | head -2) | tail -3'" 2>/dev/null | cut -c1-200 | sed "s/^/    ${hp}: /"
done
[ "${OLDPROTO:-0}" = 1 ] && { notedecide 3; log "对照组结束：快路径已换回修复后写法"; }
read a0 b0 c0 <<< "$Y0"
$C1 -c "BEGIN" -c "SELECT tpcc.begin_global()" -c "UPDATE warehouse SET w_ytd = CASE w_id WHEN $A THEN $a0 WHEN $B THEN $b0 ELSE $c0 END WHERE w_id IN ($A, $B, $C)" -c "COMMIT" >/dev/null
for i in $(seq 30); do [ "$(settled)" = 0 ] && break; sleep 1; done
log "w_ytd 已恢复：$(ytd)（原值 $Y0）"
