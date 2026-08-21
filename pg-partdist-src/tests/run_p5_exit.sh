#!/usr/bin/env bash
# P5 出口回归串行跑（T5.7）。用法：bash tests/run_p5_exit.sh <输出目录>
#九个套件：本环境 7 基线 + P5 新增 2。
# 净场按 DEV PLAN "五层"办：宿主机进程 / 容器内 psql 会话 / GUC / TSO 纪元 /
# 各 worker 残表。tso_si_p3 与 shard_xid_p1 对 TSO 的要求互斥，故前者单独垫底
# 且在它之前复位 TSO 纪元（重启协调者）。
set -u
C=pg-citus-tx2-container
T=/home/zhanhao/shardpg-tx2-work/pg-partdist-src/tests
OUT="$1"; mkdir -p "$OUT"
PORTS="5432 5433 5434 5435 5436 5437 5438 5439 5440"
DEX() { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" "$@"; }
PS()  { local p=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$p" -U postgres -d postgres -X "$@"; }

scrub() {   # 净场
  # ① 宿主机：本脚本之外的测试进程。
  #    ★ 模式必须锚定成"直接 bash 起来的套件脚本"。松成 'tests/test_.*\.sh'
  #      会把**调用本脚本的那个外壳**一起打死 —— 它的命令行里往往也带着套件
  #      路径（实测：`bash tests/test_x.sh; bash tests/run_p5_exit.sh` 这样的
  #      一行命令，第一次 scrub 就把外壳杀了，退出码 144；runner 自己脱离父
  #      进程继续跑完，于是"结果还在、外壳没了"，很容易被误读成跑挂了）。
  pkill -f '^bash .*tests/test_[a-z0-9_]*\.sh$' 2>/dev/null
  # ② 容器内游离 psql 会话
  docker exec "$C" bash -lc "pkill -f 'bin/psql' 2>/dev/null; true" >/dev/null 2>&1
  # ③b raft 组：**必须做两轮**。按 5432→5440 顺序单轮复位时，尚未复位的节点会
  #     把组心跳回已复位的节点上 —— 实测残 1 个组，直接把 dtx_tso_p4 的净场
  #     前置断言打红（首跑 48/1，两轮复位后 49/0）。
  for round in 1 2; do
    for p in $PORTS; do
      PS "$p" -q -c "SELECT partdist.pg_raft_group_reset();
                     DELETE FROM partdist.partition_map
                      WHERE partition_id NOT IN (SELECT shardid FROM pg_dist_shard);" </dev/null >/dev/null 2>&1
    done
    sleep 3
  done
  # ③ GUC：本轮各套件会自行设置的，统一复位到"未配置"
  for p in $PORTS; do
    PS "$p" -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;
                   ALTER SYSTEM RESET pg_partdist.tso_conninfo;
                   ALTER SYSTEM RESET pg_partdist.shard_safety_mode;
                   ALTER SYSTEM RESET pg_partdist.shard_vacuum_max_age;
                   ALTER SYSTEM RESET pg_partdist.shard_xid_stop_age;
                   SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
  sleep 1
}

leftovers() {  # ⑤ 各 worker 残表 + 残留分片文件（只报不删，累积即暴露）
  local n=0 p v
  for p in $PORTS; do
    v=$(PS "$p" -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname='public' AND c.relkind='r'
           AND (c.relname LIKE 'p5%' OR c.relname LIKE 'p1%' OR c.relname LIKE 'p2%'
                OR c.relname LIKE 'd2_%' OR c.relname LIKE 'p3%' OR c.relname LIKE 'p4%')" </dev/null 2>/dev/null)
    [[ "$v" =~ ^[0-9]+$ ]] && n=$((n+v))
  done
  echo "$n"
}

echo "=== 起跑前残表：$(leftovers) ==="
declare -a ORDER=(shard_clog_p2 shard_xid_p1 shard_gating_p4 shard_pagecmp_p1 \
                  dtx_convergence_p4 dtx_tso_p4 shard_vacuum_p5 shard_vacuum_replay_p5)
for s in "${ORDER[@]}"; do
  scrub
  echo "--- run $s (残表 $(leftovers)) ---"
  bash "$T/test_$s.sh" > "$OUT/$s.log" 2>&1
  echo "$s => $(grep -oE 'PASS=[0-9]+ +FAIL=[0-9]+' "$OUT/$s.log" | tail -1)"
done

# tso_si_p3 垫底：要 TSO 配好 + 纪元纯净（计数器在共享内存，须重启协调者）
scrub
docker exec -u postgres -e HOME=/var/lib/postgresql "$C" \
  /work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/coordinator -m fast restart \
  -l /work/pg-cluster-data/coordinator.log >/dev/null 2>&1
for i in $(seq 1 45); do PS 5432 -Atc "SELECT 1" </dev/null >/dev/null 2>&1 && break; sleep 1; done
echo "--- run tso_si_p3（已复位 TSO 纪元，残表 $(leftovers)） ---"
bash "$T/test_tso_si_p3.sh" > "$OUT/tso_si_p3.log" 2>&1
echo "tso_si_p3 => $(grep -oE 'PASS=[0-9]+ +FAIL=[0-9]+' "$OUT/tso_si_p3.log" | tail -1)"
echo "=== 收尾残表：$(leftovers) ==="
