#!/usr/bin/env bash
# 按 cluster.env 为每个节点生成一份 postgresql.conf 追加段：conf/out/node<id>.conf。
# 用法：bash gen_node_conf.sh；然后把 node<id>.conf 追加到对应节点 initdb 出来的
# postgresql.conf 末尾（或 include 它），再按 README 的「部署」一节起库。
source "$(dirname "$0")/lib.sh"

out="$TPCC_DIR/conf/out"
mkdir -p "$out"
peers=$(peers_string)
for i in "${!NODE_IDS[@]}"; do
  id=${NODE_IDS[$i]}
  master=off; [[ "${NODE_ROLES[$i]}" == coordinator ]] && master=on
  sed -e "s|@PORT@|${NODE_PORTS[$i]}|" \
      -e "s|@NODE_ID@|$id|" \
      -e "s|@PEERS@|$peers|" \
      -e "s|@TSO_MASTER@|$master|" \
      -e "s|@COORD_HOST@|$COORD_HOST|" \
      -e "s|@COORD_PORT@|$COORD_PORT|" \
      "$TPCC_DIR/conf/node.conf.tmpl" > "$out/node$id.conf"
  # 单副本没有副本可分叉；自动修复反而会为回滚事务补写 SQL 日志、长时间占着组的
  # 复制认领位，把装载/压测写入拖到"等待复制认领位超过 60000 ms"。
  if [[ "${REPLICAS:-0}" == 0 ]]; then
    { echo; echo "# ---- 单副本（REPLICAS=0）：无副本可分叉，关自动修复 ----"
      echo "pg_partdist.auto_repair_diverged = off"
      # 单副本快速提交：分区 WAL / 单成员组 hardstate 不 fsync，数据与标记一轮复制。
      # 持久化以 pg_wal 为准（崩溃可能丢分区流尾部）；多副本模式绝不能开。
      echo "pg_partdist.single_replica_fast_commit = on"
      echo "pg_raft.single_member_fast_commit = on"; } >> "$out/node$id.conf"
  fi
  # 可选覆盖段（后写者生效）：小机器上压内存、调超时等，不必改模板
  if [[ -n "${NODE_CONF_EXTRA:-}" ]]; then
    { echo; echo "# ---- 覆盖段：$NODE_CONF_EXTRA ----"; cat "$NODE_CONF_EXTRA"; } >> "$out/node$id.conf"
  fi
  # 只给协调者的覆盖段（在 NODE_CONF_EXTRA 之后）：协调者与 worker 同机时压它的缓冲池等
  if [[ "${NODE_ROLES[$i]}" == coordinator && -n "${COORD_CONF_EXTRA:-}" ]]; then
    { echo; echo "# ---- 协调者覆盖段：$COORD_CONF_EXTRA ----"; cat "$COORD_CONF_EXTRA"; } >> "$out/node$id.conf"
  fi
  echo "node$id（${NODE_ROLES[$i]} ${NODE_HOSTS[$i]}:${NODE_PORTS[$i]}）→ $out/node$id.conf"
done
cat <<'TXT'

pg_hba.conf：pg_raft / pg_partdist 的节点间连接不带密码（user=postgres），每个节点
都要放行集群内全部服务器与压测客户端，例如：
    host  all  postgres  10.0.0.0/24  trust
TXT
