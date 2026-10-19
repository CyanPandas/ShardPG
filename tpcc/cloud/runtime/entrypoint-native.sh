#!/bin/bash
# Dockerfile.native 的入口（只实现 deploy_native.sh 用到的那部分官方镜像接口）：
#   [postgres] -c ... / 无参数 ：数据目录为空就 initdb（与 ShardPG 节点的 node.sh 相同：-E UTF8 --no-locale、trust），
#                               再以 postgres 用户 exec postgres -D $PGDATA "$@"
#   [postgres] --version 等     ：直接执行
#   其它命令                    ：原样执行
# POSTGRES_HOST_AUTH_METHOD 之类的环境变量忽略（pg_hba 由 deploy_native.sh 用 hba_file 指定）。
set -e
[ "${1:-}" = postgres ] && shift
case "${1:-}" in
  --version|-V|--help|-\?|--describe-config) exec postgres "$@" ;;
esac
if [ $# -eq 0 ] || [ "${1#-}" != "$1" ]; then
  if [ "$(id -u)" = 0 ]; then
    mkdir -p "$PGDATA" /var/run/postgresql
    chown -R postgres:postgres "$PGDATA" /var/run/postgresql; chmod 700 "$PGDATA"
    # HOME 也要换成 postgres 的：libpq 按 $HOME/.postgresql 找客户端证书，留着 /root 会因无权限报错
    # （Citus 节点间默认 sslmode=require）。官方镜像的 gosu 同样会改 HOME。
    export HOME=$(getent passwd postgres | cut -d: -f6)
    exec setpriv --reuid=postgres --regid=postgres --init-groups "$0" "$@"
  fi
  [ -s "$PGDATA/PG_VERSION" ] || initdb -D "$PGDATA" -U postgres -E UTF8 --no-locale --auth=trust >/dev/null
  exec postgres -D "$PGDATA" "$@"
fi
exec "$@"
