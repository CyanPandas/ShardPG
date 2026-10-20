#!/bin/bash
# 用法: node.sh <id> <port> <confdir>   在本机起一个 ShardPG 节点容器（host 网络）
# 可选环境变量（默认值 = lab 布局）：IMG 镜像、NAME 容器名、DATA_DIR 数据目录、HBA_CIDR 放行网段、
#   MEM_LIMIT 容器内存上限（如 8g；不设 = 不限。设了同时关 swap，模拟内存这么大的服务器）
set -eu
id=$1; port=$2; conf=$3; img=${IMG:-shardpg:fix-20261014}
name=${NAME:-shardpg-fix-n$id}; dir=${DATA_DIR:-$(dirname "$0")/data-n$id}; hba=${HBA_CIDR:-10.77.110.0/24}
docker rm -f $name >/dev/null 2>&1 || true
mkdir -p "$dir"; chmod 777 "$dir" 2>/dev/null || true; docker run --rm -v "$(cd $dir; pwd)":/d $img bash -c "rm -rf /d/*"
cp "$conf/node$id.conf" "$dir/node.conf"; chmod 644 "$dir/node.conf"
mem=""; [ -n "${MEM_LIMIT:-}" ] && mem="--memory $MEM_LIMIT --memory-swap $MEM_LIMIT"
docker run -d --name $name --network host --shm-size 2g $mem --cap-add SYS_PTRACE --security-opt seccomp=unconfined -e HBA_CIDR="$hba" -v "$(cd $dir; pwd)":/d $img bash -c '
set -e; D=/d/pgdata
if [ ! -f $D/PG_VERSION ]; then   # 重启容器（换扩展库）时跳过 initdb
/work/pg-install/bin/initdb -D $D -U postgres -E UTF8 --no-locale >/d/initdb.log 2>&1
cat /d/node.conf >> $D/postgresql.conf
printf "local all all trust\nhost all all 127.0.0.1/32 trust\nhost all all $HBA_CIDR trust\n" > $D/pg_hba.conf
fi
exec /work/pg-install/bin/postgres -D $D' >/dev/null
for i in $(seq 60); do docker exec $name /work/pg-install/bin/pg_isready -h 127.0.0.1 -p $port -q && { echo "n$id up :$port"; exit 0; }; sleep 1; done
echo "n$id FAILED"; docker logs --tail 20 $name; tail -20 $dir/pgdata/log/* 2>/dev/null; exit 1
