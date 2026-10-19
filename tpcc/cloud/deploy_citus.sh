#!/usr/bin/env bash
# 三机单副本原版 Citus 全新部署：停 ShardPG / 单机 PG → make_citus.sh 生成对照套件 → 按与 ShardPG 相同的布局起
# 4 个节点（A：协调者 c1 + worker c2；B：c3；C：c4；端口同 ShardPG）→ 接线 → 建库装载（函数下推）→
# 打印各 worker 分到的仓库数。参数 = common.conf + citus.conf（协调者再叠 coord.conf），pg_hba 只放行 HBA_CIDR。
# 会清空 $BASE/data-c*。
# 用法：bash cloud/deploy_citus.sh
source "$(dirname "$0")/lib.sh"
T0=$(date +%s)

for h in $(printf '%s\n' "${NODE_HOST_LIST[@]}" | sort -u); do
  on "$h" "docker image inspect $CITUS_IMG >/dev/null 2>&1" || die "$h 上没有镜像 $CITUS_IMG（先跑 build_citus_image.sh）"
done
stop_others citus
remove_own citus

bash "$TPCC_DIR/scripts/make_citus.sh" "$CITUS_DIR" || die "生成对照套件失败"
cat > "$CITUS_DIR/cluster.env" <<EOF
# 由 cloud/deploy_citus.sh 按 cloud.env 生成 —— 改参数请改 cloud.env 后重跑
NODE_IDS=(${NODE_ID_LIST[*]})
NODE_HOSTS=(${NODE_HOST_LIST[*]})
NODE_PORTS=(${NODE_PORT_LIST[*]})
NODE_ROLES=(${NODE_ROLE_LIST[*]})
COORD_HOST=$COORD_NODE_HOST
COORD_PORT=$COORD_PORT
PG_BIN=/work/pg-install/bin
WAREHOUSES=$WAREHOUSES
TERMINALS=${TERMINAL_LIST%% *}
DURATION_S=$DURATION_S
RAMPUP_S=$RAMPUP_S
SHARD_COUNT=$SHARD_COUNT
REPLICAS=0
LOAD_PARALLEL=$LOAD_PARALLEL
MAX_TRIES=$MAX_TRIES
RESULT_ROOT=/tc/results
DELEGATE=1
EOF

args=$(conf_args "$COMMON_CONF" "$CLOUD_DIR/citus.conf")
cargs=$(conf_args "$COMMON_CONF" "$CLOUD_DIR/citus.conf" "$CLOUD_DIR/coord.conf")
log "起 ${#NODE_ID_LIST[@]} 个 Citus 节点（$CITUS_IMG，全新 initdb）"
pids=()
for k in "${!NODE_ID_LIST[@]}"; do
  id=${NODE_ID_LIST[$k]}; h=${NODE_HOST_LIST[$k]}; p=${NODE_PORT_LIST[$k]}
  a=$args; [[ $k -eq 0 ]] && a=$cargs
  ( hba_file "$h" "$BASE/citus-conf" \
    && on "$h" "docker rm -f $PREFIX-c$id >/dev/null 2>&1; mkdir -p $BASE/data-c$id \
         && docker run --rm -v $BASE/data-c$id:/d $CITUS_IMG bash -c 'rm -rf /d/* /d/.[!.]*' \
         && docker run -d --name $PREFIX-c$id --network host --shm-size 2g \
              -v $BASE/data-c$id:/var/lib/postgresql/data -v $BASE/citus-conf/pg_hba.conf:/etc/postgresql/pg_hba.conf:ro \
              $CITUS_IMG -c port=$p -c listen_addresses=$(printf %q '*') -c hba_file=/etc/postgresql/pg_hba.conf $a >/dev/null" \
    && for _ in $(seq 1 120); do
         on "$h" "docker exec $PREFIX-c$id pg_isready -h 127.0.0.1 -p $p -q" && { echo "c$id up :$p"; exit 0; }; sleep 1
       done; on "$h" "docker logs --tail 20 $PREFIX-c$id"; echo "c$id FAILED"; exit 1 ) &
  pids+=($!)
done
fail=0; for p in "${pids[@]}"; do wait "$p" || fail=1; done
[[ $fail -eq 0 ]] || die "有 Citus 节点没起来（见上面的日志）"

ensure_client
log "接线（建扩展、登记协调者、加 worker）"
cx "bash /tc/scripts/wire_citus.sh" || die "接线失败"
log "实际参数（应与 ShardPG worker / 单机 PG 一致）"
cpsql "${NODE_HOST_LIST[1]}" "${NODE_PORT_LIST[1]}" -qAt -F ' = ' -c "SELECT name, current_setting(name) FROM pg_settings
  WHERE name IN ('server_version','shared_buffers','effective_cache_size','work_mem','max_wal_size','checkpoint_timeout',
                 'synchronous_commit','max_connections','max_prepared_transactions','default_transaction_isolation',
                 'citus.allow_nested_distributed_execution') ORDER BY 1" || die "连不上 worker"
cpsql "${NODE_HOST_LIST[1]}" "${NODE_PORT_LIST[1]}" -qAt -c "SELECT '库排序规则 = ' || datcollate FROM pg_database WHERE datname = 'postgres'"

BLOG="$CITUS_DIR/results/build_db_citus_$(date +%Y%m%d-%H%M%S).log"
log "建库 + 装载 $WAREHOUSES 仓（日志 $BLOG）"
cx "bash /tc/scripts/build_db.sh" 2>&1 | tee "$BLOG"
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
log "Citus 部署完成，用时 $(( ($(date +%s) - T0) / 60 )) min。下一步：bash cloud/bench.sh citus"
