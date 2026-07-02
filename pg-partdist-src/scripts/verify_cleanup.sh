#!/usr/bin/env bash
# verify_cleanup.sh — 独立清理脚本。
#
# verify_from_clean_clone.sh / verify_4node.sh 跑完测试后不会自动清理
# 容器/镜像/克隆目录（方便测试结束后手动进容器看状态、查日志），需要清理
# 时手动执行这个脚本。清理两个验证脚本各自固定的容器名/镜像名，以及
# /tmp 下残留的克隆目录和日志文件。
#
# 用法：bash verify_cleanup.sh

set -uo pipefail

HOST_UID="$(id -u)"
HOST_GID="$(id -g)"

remove_one() {
    local container=$1 image=$2
    if docker ps -a --format '{{.Names}}' | grep -qx "$container"; then
        # 先把 bind mount 目录属主改回宿主机用户，否则宿主机侧 rm -rf 会
        # 因为文件属主是容器内 postgres(uid 999) 而权限不足。
        docker exec -u root "$container" \
            chown -R "$HOST_UID:$HOST_GID" /work/pg-install /work/pg-cluster-data /work/pg-partdist-src \
            >/dev/null 2>&1 || true
        docker rm -f "$container" >/dev/null 2>&1 && echo "  容器已删除: $container"
    else
        echo "  容器不存在，跳过: $container"
    fi
    docker rmi -f "$image" >/dev/null 2>&1 && echo "  镜像已删除: $image" || echo "  镜像不存在或已删除: $image"
}

echo "=== 清理 verify_from_clean_clone.sh (3节点) 遗留资源 ==="
remove_one pg-partdist-verify-container pg-partdist-verify-env

echo ""
echo "=== 清理 verify_4node.sh (4节点) 遗留资源 ==="
remove_one pg-partdist-verify4-container pg-partdist-verify4-env

# 目录里 pg-cluster-data/pg-install 下有些文件属主是容器内 postgres
# (uid 999)，如果这个目录对应的容器在上一步已经被删（或者更早就已经不
# 在了），宿主机用户直接 rm -rf 会因为属主不对报 Permission denied。用
# 一次性容器把属主改回宿主机用户再删，不依赖原容器是否还在。
rm_workdir() {
    local dir=$1
    if rm -rf "$dir" 2>/dev/null; then
        echo "  已删除目录: $dir"
        return
    fi
    docker run --rm -v "$dir:/target" ubuntu:22.04 \
        chown -R "$HOST_UID:$HOST_GID" /target >/dev/null 2>&1 || true
    if rm -rf "$dir"; then
        echo "  已删除目录（先用临时容器改回属主）: $dir"
    else
        echo "  删除失败，属主仍不匹配，需要手动处理: $dir"
    fi
}

echo ""
echo "=== 清理残留克隆目录/日志 ==="
shopt -s nullglob
found=0
for p in /tmp/pg-partdist-verify.* /tmp/pg-partdist-verify4.*; do
    found=1
    if [ -d "$p" ]; then
        rm_workdir "$p"
    elif [ -f "$p" ]; then
        rm -f "$p" && echo "  已删除文件: $p"
    fi
done
[ "$found" -eq 0 ] && echo "  没有残留的克隆目录/日志"

echo ""
echo "=== 清理完成 ==="
