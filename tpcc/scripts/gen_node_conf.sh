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
  echo "node$id（${NODE_ROLES[$i]} ${NODE_HOSTS[$i]}:${NODE_PORTS[$i]}）→ $out/node$id.conf"
done
cat <<'TXT'

pg_hba.conf：pg_raft / pg_partdist 的节点间连接不带密码（user=postgres），每个节点
都要放行集群内全部服务器与压测客户端，例如：
    host  all  postgres  10.0.0.0/24  trust
TXT
