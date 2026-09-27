#!/usr/bin/env bash
# [宿主机] P7-N40 回归（打标登记只增不减，攒满 64 张之后该节点再也打不了标）。
#
# 病灶：打标登记有**两份**东西 —— 水位文件 pg_shard_xid/<oid>（它同时就是重启时的登记表）
#   和共享内存里的登记集合。而 DROP 时的清理判据只认集合（shard_oid_is_mvcc）：
#   一旦两者不一致（集合满了 ⇒ SetAdd 抛错、文件却已经建出来），这个文件就永久残留；
#   而残留文件下次启动又被当成登记装载进来 ⇒ 登记只增不减 ⇒ 最终
#   "打标登记表已满（上限 64 张）"，这台节点再也打不了标。
#   实测（2026-09-27 用户演示）：w1 67 个、w3 72 个水位文件，OID 全都没有对应表了；
#   set_table_shard_mvcc 4 个 placement 里 2 个报"登记表已满"，演示表一半没接入事务系统。
# 修法：① DROP 判据兼顾文件（有水位文件就登记待清）；② 登记时**先查容量再建文件**，
#   容量不足直接报错、不留文件；③ 新增 partdist.shard_mvcc_gc_orphans() 清存量孤儿
#   （扫 pg_shard_xid/ 与 pg_shard_clog/，OID 在 pg_class 里查不到的，走 DROP 同一条回收路径）；
#   ④ ShardClogGcDropped 一次只收 16 个、原先**静默截断**，改成出声并由调用方分批。
#
# 断言：[1] 净场时 gc_orphans() = 0；[2] 种下的孤儿（水位文件 + clog 目录）被清掉且返回计数正确；
#   [3] 建-删打标表 ROUNDS 轮后，水位文件数回到起点（DROP 真的清了）；[4] 超过 16 个孤儿也能一次清完。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
DATA=/work/pg-cluster-data
COORD=5432
ROUNDS="${ROUNDS:-3}"
PASS=0; FAIL=0
DEX() { docker exec -i -u postgres "$CONTAINER" "$@"; }
Q()   { DEX $BIN/psql -h /tmp -p "$1" -U postgres -d postgres -X -Atc "$2" </dev/null 2>/dev/null | tail -1; }
check() {
  local what="$1" got="$2" want="$3"
  if [[ "$got" == "$want" ]]; then PASS=$((PASS+1)); echo "  PASS  $what"
  else FAIL=$((FAIL+1)); echo "  FAIL  $what（实际='$got' 期望='$want'）"; fi
}
W1=$(Q $COORD "SELECT min(nodeport) FROM pg_dist_node WHERE noderole='primary' AND groupid<>0")
D1=$(DEX bash -c "ls -d $DATA/*/ | while read x; do grep -q \"^port *= *$W1\" \$x/postgresql.conf 2>/dev/null && echo \${x%/}; done" </dev/null | head -1)
echo "拿 :$W1（$D1）做样本"
wm_count() { DEX bash -c "ls $D1/pg_shard_xid 2>/dev/null | wc -l" </dev/null | tr -dc '0-9'; }

echo "========== [1] 净场：没有孤儿 =========="
for p in $(Q $COORD "SELECT string_agg(nodeport::text,' ') FROM pg_dist_node WHERE noderole='primary' AND groupid<>0"); do
  check "  :$p gc_orphans() = 0" "$(Q $p "SELECT partdist.shard_mvcc_gc_orphans()")" "0"
done
BASE=$(wm_count); echo "  :$W1 现有水位文件 $BASE 个"

echo "========== [2] 种孤儿：水位文件 + clog 目录，OID 故意不存在 =========="
DEX bash -c "mkdir -p $D1/pg_shard_xid $D1/pg_shard_clog
  for o in 900001 900002 900003; do printf '\0\0\0\0\0\0\0\0' > $D1/pg_shard_xid/\$o; mkdir -p $D1/pg_shard_clog/\$o; touch $D1/pg_shard_clog/\$o/0000; done
  printf 'x' > $D1/pg_shard_clog/dtx_pending.jrnl.keepme" </dev/null
check "[2] 种下 3 个孤儿水位文件" "$(( $(wm_count) - BASE ))" "3"
check "[2] gc_orphans() 报告清掉 3 个" "$(Q $W1 "SELECT partdist.shard_mvcc_gc_orphans()")" "3"
check "[2] 水位文件回到起点" "$(wm_count)" "$BASE"
check "[2] clog 目录也清了" "$(DEX bash -c "ls -d $D1/pg_shard_clog/90000* 2>/dev/null | wc -l" </dev/null | tr -dc '0-9')" "0"
check "[2] 非数字名的文件没被碰（dtx 日志那类）" "$(DEX bash -c "ls $D1/pg_shard_clog/dtx_pending.jrnl.keepme 2>/dev/null | wc -l" </dev/null | tr -dc '0-9')" "1"
DEX bash -c "rm -f $D1/pg_shard_clog/dtx_pending.jrnl.keepme" </dev/null

echo "========== [3] 超过单批上限（16）的孤儿也要一次清完 =========="
DEX bash -c "for i in \$(seq 1 20); do printf '\0\0\0\0\0\0\0\0' > $D1/pg_shard_xid/91\$(printf '%04d' \$i); done" </dev/null
check "[3] 种下 20 个" "$(( $(wm_count) - BASE ))" "20"
check "[3] 一次调用清掉 20 个" "$(Q $W1 "SELECT partdist.shard_mvcc_gc_orphans()")" "20"
check "[3] 一个不剩" "$(wm_count)" "$BASE"

echo "========== [4] 建-删打标表 $ROUNDS 轮：DROP 真的把登记清了 =========="
for r in $(seq 1 $ROUNDS); do
  # SET 与 CREATE 必须在**同一次** psql 调用里：Q 每次新开连接，分两次 SET 就没了
  Q $COORD "SET citus.shard_count=2; SET citus.shard_replication_factor=1; CREATE TABLE n40_$r(id int primary key, v text); SELECT create_distributed_table('n40_$r','id',colocate_with=>'none')" >/dev/null
  nsh=$(Q $COORD "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='n40_$r'::regclass")
  for p in $(Q $COORD "SELECT string_agg(nodeport::text,' ') FROM pg_dist_node WHERE noderole='primary' AND groupid<>0"); do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
  st=$(Q $COORD "SELECT count(*) FILTER (WHERE status ~ '^(registered|already)')||'/'||count(*) FROM partdist.set_table_shard_mvcc('n40_$r')")
  check "  第 $r 轮：打标全部成功（$st）" "$(cut -d/ -f1 <<<"$st")" "$(cut -d/ -f2 <<<"$st")"
  before=$(DEX bash -c "ls -d $DATA/*/pg_shard_xid/* 2>/dev/null | wc -l" </dev/null | tr -dc '0-9')
  Q $COORD "DROP TABLE n40_$r" >/dev/null
  after=$(DEX bash -c "ls -d $DATA/*/pg_shard_xid/* 2>/dev/null | wc -l" </dev/null | tr -dc '0-9')
  check "  第 $r 轮：$nsh 个分片打标成功 → DROP 后登记归零（$before → $after）" "$after" "0"
done

echo "========== [5] 收尾 =========="
for p in $(Q $COORD "SELECT string_agg(nodeport::text,' ') FROM pg_dist_node WHERE noderole='primary' AND groupid<>0"); do
  n=$(Q $p "SELECT partdist.shard_mvcc_gc_orphans()")
  [[ "$n" != 0 ]] && echo "    :$p 收尾又清了 $n 个（应为 0，说明上面漏了）"
  check "  :$p 收尾 gc_orphans() = 0" "$n" "0"
done
echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N40 回归：通过" || echo "P7-N40 回归：存在 FAIL"
exit $(( FAIL > 0 ))
