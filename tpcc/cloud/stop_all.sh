#!/usr/bin/env bash
# 删掉本套件在三台上起的全部容器（$PREFIX-n1..n4、$PREFIX-c1..c4、$PREFIX-native、$PREFIX-client），数据目录与结果保留。
# 用法：bash cloud/stop_all.sh
source "$(dirname "$0")/lib.sh"
for i in "${!ALL_HOSTS[@]}"; do
  h=${ALL_HOSTS[$i]}
  names=$(on "$h" "docker ps -a --format '{{.Names}}' | grep -E '^$PREFIX-(n[0-9]+|c[0-9]+|native|client)\$' | tr '\n' ' '")
  [[ -n "${names// /}" ]] || { echo "${ALL_LABELS[$i]} $h：无"; continue; }
  echo "${ALL_LABELS[$i]} $h：删除 $names"
  on "$h" "docker rm -f $names >/dev/null"
done
