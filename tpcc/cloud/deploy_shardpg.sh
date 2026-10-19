#!/usr/bin/env bash
# 三机单副本 ShardPG 全新部署：按 cloud.env 生成 cluster.env 与各节点配置 → 三台起节点
# （默认 A：协调者 n1 + worker n2，B：n3，C：n4；COORD_DEDICATED=1 时 A：协调者 n1，B：n2，C：n3）
# → 接线 → 建库装载 → 打印各 worker 分到的仓库数。
# 在客户端机 CLIENT_HOST 上执行。
# 会清空 $BASE/data-n* 里的旧 ShardPG 数据。100 仓装载要几十分钟，放在 tmux / nohup 里跑。
# 用法：bash cloud/deploy_shardpg.sh [--conf-only]   --conf-only：只生成配置（看一眼再部署）
source "$(dirname "$0")/lib.sh"
T0=$(date +%s)

cat > "$TPCC_DIR/cluster.env" <<EOF
# 由 cloud/deploy_shardpg.sh 按 cloud.env 生成 —— 改参数请改 cloud.env 后重跑
NODE_IDS=(${NODE_ID_LIST[*]})
NODE_HOSTS=(${NODE_HOST_LIST[*]})
NODE_PORTS=(${NODE_PORT_LIST[*]})
NODE_ROLES=(${NODE_ROLE_LIST[*]})
COORD_HOST=$COORD_NODE_HOST
COORD_PORT=$COORD_PORT
PG_BIN=/work/pg-install/bin
PGDATA_ROOT=/d/pgdata
WAREHOUSES=$WAREHOUSES
TERMINALS=${TERMINAL_LIST%% *}
DURATION_S=$DURATION_S
RAMPUP_S=$RAMPUP_S
SHARD_COUNT=$SHARD_COUNT
REPLICAS=0
LOAD_PARALLEL=$LOAD_PARALLEL
MAX_TRIES=$MAX_TRIES
RESULT_ROOT=/t/results
NODE_CONF_EXTRA=\$TPCC_DIR/conf/cloud.extra.conf
COORD_CONF_EXTRA=\$TPCC_DIR/cloud/coord.conf
EOF
cat "$COMMON_CONF" "$CLOUD_DIR/shardpg.conf" > "$TPCC_DIR/conf/cloud.extra.conf"
rm -rf "$TPCC_DIR/conf/out"
bash "$TPCC_DIR/scripts/gen_node_conf.sh" >/dev/null || die "生成节点配置失败"
log "配置已生成：$TPCC_DIR/cluster.env、$TPCC_DIR/conf/out/node{$(IFS=,; echo "${NODE_ID_LIST[*]}")}.conf"
[[ "${1:-}" == --conf-only ]] && exit 0

log "检查镜像 / ssh"
is_local "$CLIENT_HOST" || die "要在客户端机 $CLIENT_HOST 上运行"
for h in $(printf '%s\n' "${NODE_HOST_LIST[@]}" | sort -u); do
  on "$h" "docker image inspect $IMG >/dev/null 2>&1" || die "$h 上没有镜像 $IMG（先跑 install_images.sh）"
done
stop_others shardpg   # 三套系统不同时跑
remove_own shardpg    # 全新部署：旧布局的节点容器一律删掉

log "分发节点脚本与配置到各节点所在机器（$BASE/node.sh、$BASE/conf/）"
for k in "${!NODE_ID_LIST[@]}"; do
  id=${NODE_ID_LIST[$k]}; h=${NODE_HOST_LIST[$k]}
  on "$h" "mkdir -p $BASE/conf" && put "$h" "$TPCC_DIR/lab/node.sh" "$BASE/node.sh" \
    && put "$h" "$TPCC_DIR/conf/out/node$id.conf" "$BASE/conf/node$id.conf" || die "传到 $h 失败"
done

log "起 ${#NODE_ID_LIST[@]} 个节点（全新 initdb）"
pids=()
for k in "${!NODE_ID_LIST[@]}"; do
  id=${NODE_ID_LIST[$k]}; h=${NODE_HOST_LIST[$k]}; p=${NODE_PORT_LIST[$k]}
  on "$h" "IMG=$IMG NAME=$PREFIX-n$id DATA_DIR=$BASE/data-n$id HBA_CIDR=$HBA_CIDR bash $BASE/node.sh $id $p $BASE/conf" &
  pids+=($!)
done
fail=0; for p in "${pids[@]}"; do wait "$p" || fail=1; done
[[ $fail -eq 0 ]] || die "有节点没起来（见上面的日志）"

ensure_client
log "接线（装扩展、Citus 加节点、等控制组 leader、核对 TSO）"
cx "bash /t/scripts/wire_cluster.sh" || die "接线失败"

BLOG="$TPCC_DIR/results/build_db_shardpg_$(date +%Y%m%d-%H%M%S).log"
log "建库 + 装载 $WAREHOUSES 仓（日志 $BLOG）"
cx "bash /t/scripts/build_db.sh" 2>&1 | tee "$BLOG"
[[ ${PIPESTATUS[0]} -eq 0 ]] || die "建库失败（见 $BLOG）"

log "各 worker 分到的仓库数 / 库大小"
cpsql "$COORD_NODE_HOST" "$COORD_PORT" <<SQL
SELECT p.nodename AS worker, count(*) AS warehouses
  FROM generate_series(1, $WAREHOUSES) w
  JOIN pg_dist_shard_placement p ON p.shardid = get_shard_id_for_distribution_column('warehouse', w)
 GROUP BY 1 ORDER BY 1;
SELECT pg_size_pretty(sum(citus_total_relation_size(logicalrelid))) AS distributed_tables_size
  FROM pg_dist_partition WHERE partmethod <> 'n';
SQL
log "ShardPG 部署完成，用时 $(( ($(date +%s) - T0) / 60 )) min。下一步：bash cloud/bench.sh shardpg"
