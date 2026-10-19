#!/usr/bin/env bash
# 单机 PostgreSQL 对照全新部署：停 ShardPG / Citus 节点（机器让出来）→ make_native.sh 生成对照套件 →
# 在 NATIVE_HOST 起单机镜像（参数 = common.conf，pg_hba 与 ShardPG 节点一样只放行 HBA_CIDR）→ 建库装载。
# 会清空 $BASE/data-native。停掉的 ShardPG 不能原地再起（协调者重启后 TSO 拒绝发号），要再测
# ShardPG 就重跑 deploy_shardpg.sh。
# 用法：bash cloud/deploy_native.sh
source "$(dirname "$0")/lib.sh"
T0=$(date +%s)

on "$NATIVE_HOST" "docker image inspect $NATIVE_IMG >/dev/null 2>&1" || die "$NATIVE_HOST 上没有镜像 $NATIVE_IMG（先跑 install_images.sh）"
stop_others native

bash "$TPCC_DIR/scripts/make_native.sh" "$NATIVE_DIR" || die "生成对照套件失败"
cat > "$NATIVE_DIR/cluster.env" <<EOF
# 由 cloud/deploy_native.sh 按 cloud.env 生成 —— 改参数请改 cloud.env 后重跑
NODE_IDS=(1)
NODE_HOSTS=($NATIVE_HOST)
NODE_PORTS=($NATIVE_PORT)
NODE_ROLES=(coordinator)
COORD_HOST=$NATIVE_HOST
COORD_PORT=$NATIVE_PORT
PG_BIN=/work/pg-install/bin
WAREHOUSES=$WAREHOUSES
TERMINALS=${TERMINAL_LIST%% *}
DURATION_S=$DURATION_S
RAMPUP_S=$RAMPUP_S
SHARD_COUNT=$SHARD_COUNT
REPLICAS=0
LOAD_PARALLEL=$LOAD_PARALLEL
MAX_TRIES=$MAX_TRIES
RESULT_ROOT=/tn/results
EOF

args=$(conf_args "$COMMON_CONF")

log "在 $NATIVE_HOST 起单机 PG（$NATIVE_IMG，端口 $NATIVE_PORT）"
hba_file "$NATIVE_HOST" "$BASE/native-conf" || die "写 pg_hba 失败"
on "$NATIVE_HOST" "docker rm -f $PREFIX-native >/dev/null 2>&1; mkdir -p $BASE/data-native && docker run --rm -v $BASE/data-native:/d $NATIVE_IMG bash -c 'rm -rf /d/* /d/.[!.]*'" \
  || die "清数据目录失败"
on "$NATIVE_HOST" "docker run -d --name $PREFIX-native --network host --shm-size 2g -e POSTGRES_HOST_AUTH_METHOD=trust \
    -v $BASE/data-native:/var/lib/postgresql/data -v $BASE/native-conf/pg_hba.conf:/etc/postgresql/pg_hba.conf:ro \
    $NATIVE_IMG -c port=$NATIVE_PORT -c listen_addresses=$(printf %q '*') -c hba_file=/etc/postgresql/pg_hba.conf $args >/dev/null" \
  || die "起单机 PG 失败"
ok=0
for _ in $(seq 1 120); do
  on "$NATIVE_HOST" "docker exec $PREFIX-native pg_isready -h 127.0.0.1 -p $NATIVE_PORT -q" && { ok=1; break; }
  sleep 1
done
[[ $ok -eq 1 ]] || { on "$NATIVE_HOST" "docker logs --tail 30 $PREFIX-native"; die "单机 PG 120 s 内没起来"; }

ensure_client
log "实际参数（应与 ShardPG worker 一致）"
cpsql "$NATIVE_HOST" "$NATIVE_PORT" -qAt -F ' = ' -c "SELECT name, current_setting(name) FROM pg_settings
  WHERE name IN ('server_version','shared_buffers','effective_cache_size','work_mem','maintenance_work_mem','max_wal_size',
                 'checkpoint_timeout','synchronous_commit','fsync','max_connections','default_transaction_isolation') ORDER BY 1" \
  || die "连不上单机 PG"
cpsql "$NATIVE_HOST" "$NATIVE_PORT" -qAt -c "SELECT '库排序规则 = ' || datcollate FROM pg_database WHERE datname = 'postgres'"

BLOG="$NATIVE_DIR/results/build_db_native_$(date +%Y%m%d-%H%M%S).log"
log "建库 + 装载 $WAREHOUSES 仓（日志 $BLOG）"
cx "bash /tn/scripts/build_db.sh" 2>&1 | tee "$BLOG"
[[ ${PIPESTATUS[0]} -eq 0 ]] || die "建库失败（见 $BLOG）"
cpsql "$NATIVE_HOST" "$NATIVE_PORT" -qAt -c "SELECT '库大小 ' || pg_size_pretty(pg_database_size('postgres'))"
log "单机 PG 部署完成，用时 $(( ($(date +%s) - T0) / 60 )) min。下一步：bash cloud/bench.sh native"
