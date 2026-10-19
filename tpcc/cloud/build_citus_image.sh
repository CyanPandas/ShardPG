#!/usr/bin/env bash
# 原版 Citus 对照镜像：在 A 上以单机镜像 $NATIVE_IMG（Ubuntu 22.04 + PGDG postgresql-16）为底，
# 编译 Citus 源码包（先核对 sha256 = cloud.env 的 CITUS_SHA256）做成 $CITUS_IMG，再经内网分发到 B / C。
# 源码包：https://github.com/citusdata/citus/archive/refs/tags/v13.1.0.tar.gz —— 解开后与 lab 对照镜像
# yyt/citus-tpcc-c01:13.1.0-pg16.14 构建用的 citus-src 逐文件相同（2978 个文件，2026-10-18 核对）。
# 用法（A 上、root；先跑过 build_runtime_image.sh 或 install_images.sh）：bash cloud/build_citus_image.sh <citus 源码包>
source "$(dirname "$0")/lib.sh"
f=${1:?用法：build_citus_image.sh <citus 源码包.tar.gz>}
[[ -f "$f" ]] || die "没有 $f"
sum=$(sha256sum "$f" | cut -d' ' -f1)
[[ "$sum" == "$CITUS_SHA256" ]] || die "源码包 sha256 = $sum，与 cloud.env 的 CITUS_SHA256（$CITUS_SHA256）不符"
docker image inspect "$NATIVE_IMG" >/dev/null 2>&1 || die "A 上没有单机镜像 $NATIVE_IMG"
PG_VERSION=$(docker run --rm "$NATIVE_IMG" postgres --version | grep -oE '16\.[0-9]+' | head -1)
[[ -n "$PG_VERSION" ]] || die "取不到 $NATIVE_IMG 的 PG 版本"
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT
cp "$CLOUD_DIR/runtime/Dockerfile.citus" "$W/Dockerfile" && cp "$CLOUD_DIR/runtime/entrypoint-native.sh" "$W/" \
  && cp "$f" "$W/citus.tar.gz"

log "构建 $CITUS_IMG（$NATIVE_IMG + Citus 源码包 ${sum:0:12}…，PG $PG_VERSION 头文件）"
docker build --network host --build-arg NATIVE_IMG="$NATIVE_IMG" --build-arg PG_VERSION="$PG_VERSION" \
  -t "$CITUS_IMG" "$W" > "$W/build.log" 2>&1 || { tail -30 "$W/build.log"; die "docker build $CITUS_IMG 失败"; }
docker run --rm "$CITUS_IMG" bash -c 'postgres --version; grep default_version /usr/share/postgresql/16/extension/citus.control' \
  || die "$CITUS_IMG 不完整"

for h in "${HOSTS[@]}"; do
  is_local "$h" && continue   # 本机不用分发；客户端机用不到 Citus 镜像
  log "分发 $CITUS_IMG 到 $h（内网）"
  docker save "$CITUS_IMG" | ssh "${SSH_OPTS[@]}" "$SSH_USER@$h" docker load >/dev/null || die "分发到 $h 失败"
done
log "$CITUS_IMG 就绪（三台）"
