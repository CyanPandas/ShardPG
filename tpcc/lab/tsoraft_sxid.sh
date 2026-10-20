#!/bin/bash
# 切主后分片 xid 不重发（2026-10-19）的确定性验证：
#   1) 选一个主分片在 n3 上的仓 W；把该仓 history 分片的发号推过一个批次边界（65536）——
#      推过之后，备库上的水位文件（不进 WAL 时）就落后于主库已经发出去的号
#   2) 在 n3 上开一个事务往该分片插 20 行（h_data = 'SXIDOPEN'），不提交，记下它的分片 xid S
#   3) docker kill n3 主库，等自动切主
#   4) 经协调者往同一分片写 5 笔并提交（'SXIDNEW'）：看它们的分片 xid 有没有 = S，
#      'SXIDOPEN' 的 20 行有没有"复活"（随 SXIDNEW 提交而可见）
# 测试行 h_amount = 0，不影响跨节点不变式；结束时删掉。
# 用法（145 上；集群全新部署）：tsoraft_sxid.sh <标签>
R=/data1/yyt/shardpg-fix-20261014; cd $R
TAG=${1:?标签}; OUT=$R/tsoraft_logs/sx-$TAG; rm -rf $OUT; mkdir -p $OUT
log() { echo "[$(date +%T)] $*" | tee -a $OUT/run.log; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
N1="$CLI -h 10.77.110.148 -p 31080"; N3="$CLI -h 10.77.110.147 -p 31081"
bash shardpg_rep.sh register >/dev/null
read W SH <<< "$($N1 -F' ' -c "select w, x.s from generate_series(1,64) w, lateral (select get_shard_id_for_distribution_column('history', w) s) x join pg_dist_placement p on p.shardid = x.s join pg_dist_node n on n.groupid = p.groupid where n.nodeport = 31081 order by w limit 1")"
log "[$TAG] 仓 $W 的 history 分片 history_$SH 在 n3 上"
NEXT0=$($N3 -c "select partdist.shard_xid_next('history_$SH'::regclass::oid)")
TARGET=$(( (NEXT0 / 65536 + 1) * 65536 + 500 )); NB=$(( TARGET - NEXT0 ))
log "[$TAG] 分片 xid 下一个待发 $NEXT0；推过批次边界 $(( (NEXT0 / 65536 + 1) * 65536 )) 需要约 $NB 笔"
echo "INSERT INTO history VALUES (1, 1, $W, 1, $W, now(), 0, 'SXIDBURN');" > $R/tpcc/sxid_burn.sql
docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h 10.77.110.147 -p 31081 -U postgres -n -c 16 -j 16 \
  -t $(( NB / 16 + 1 )) -f /t/sxid_burn.sql postgres > $OUT/burn.out 2>&1
NEXT1=$($N3 -c "select partdist.shard_xid_next('history_$SH'::regclass::oid)")
log "[$TAG] 推进后下一个待发 $NEXT1（$(grep -E '^tps' $OUT/burn.out | cut -c1-20)）"
bash shardpg_rep.sh wait >/dev/null
docker exec -i shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAt -h 10.77.110.147 -p 31081 > $OUT/open.out 2>&1 <<SQL &
BEGIN;
INSERT INTO history SELECT 1, 1, $W, 1, $W, now(), 0, 'SXIDOPEN' FROM generate_series(1, 20);
SELECT 'S=' || xmin FROM history WHERE h_w_id = $W AND h_data = 'SXIDOPEN' LIMIT 1;
SELECT pg_sleep(600);
SQL
OPEN=$!
for i in $(seq 30); do grep -q '^S=' $OUT/open.out && break; sleep 1; done
S=$(grep -oE '^S=[0-9]+' $OUT/open.out | cut -d= -f2)
log "[$TAG] 未提交事务的分片 xid S=$S"
sleep 3      # 让这 20 行的 WAL 流到备库
ssh -n 10.77.110.147 "docker kill shardpg-fix-n3" >/dev/null; T0=$(date +%s); log "[$TAG] 已 kill n3 主库"
NEW=""; for i in $(seq 240); do NEW=$($N1 -c "select nodename||':'||nodeport from pg_dist_node where nodeport=31095 limit 1" 2>/dev/null); [ -n "$NEW" ] && break; sleep 0.5; done
log "[$TAG] 自动切主：n3 → ${NEW:-（120 s 内没切）}（$(( $(date +%s) - T0 )) s）"
kill $OPEN 2>/dev/null
for i in $(seq 60); do $N1 -c "INSERT INTO history VALUES (1, 1, $W, 1, $W, now(), 0, 'SXIDPROBE')" >/dev/null 2>&1 && break; sleep 1; done
for k in 1 2 3 4 5; do $N1 -c "INSERT INTO history VALUES (1, 1, $W, 1, $W, now(), 0, 'SXIDNEW')" >/dev/null 2>>$OUT/new.err; done
NEWX=$($N1 -c "select string_agg(DISTINCT xmin::text, ',' ORDER BY xmin::text) from history where h_w_id = $W and h_data in ('SXIDNEW', 'SXIDPROBE')")
OPENV=$($N1 -c "select count(*) from history where h_w_id = $W and h_data = 'SXIDOPEN'")
log "[$TAG] 切主后新写的分片 xid：$NEWX；未提交事务的行在新主上可见 $OPENV 行"
if [ "$OPENV" = 0 ] && ! echo ",$NEWX," | grep -q ",$S,"; then RES="未重发、未复活"; else RES="重发 / 复活（缺陷复现）"; fi
log "[$TAG] 结果：$RES"
$N1 -c "delete from history where h_w_id = $W and h_data in ('SXIDNEW','SXIDBURN','SXIDPROBE','SXIDOPEN')" >/dev/null
log "[$TAG] 核对：标准 [$($N1 -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($N1 -f /t/xnode_consistency_mini.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"
NC=$(echo ${NEW:-x:0} | cut -d: -f1); for h in 10.77.110.145 10.77.110.148; do [ $h = $NC ] || continue
  if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi
  $c "docker exec shardpg-fix-n3-s${h##*.} bash -c 'grep -h -E \"认领|claim|XID_WM|分片 xid\" /d/pgdata/log/*.log | tail -5'" > $OUT/promoted.log 2>&1; done
echo SX_DONE
