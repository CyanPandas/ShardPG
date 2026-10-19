#!/usr/bin/env bash
# 汇总 bench.sh 跑过的全部正式轮次（跳过 warmup）：每轮一行，再按终端数取 tpmC 中位数对比三套。
# CPU / iowait 是测量窗口内（跳过预热、不含收尾的一致性检查）三台 vmstat 的平均（us+sy / wa，%）。
# 用法：bash cloud/report.sh
source "$(dirname "$0")/lib.sh"

# vm_avg <vmstat 文件> <跳过的采样数> <取的采样数>：输出 "us+sy/wa"（第一行是开机以来的平均，算在跳过里）
vm_avg() {
  [[ -f "$1" ]] || { echo "-"; return; }
  awk -v skip="$2" -v take="$3" '
    / us / && / id / { for (i = 1; i <= NF; i++) { if ($i == "us") us = i; if ($i == "sy") sy = i; if ($i == "wa") wa = i }; next }
    us && $1 ~ /^[0-9]+$/ { if (++n <= skip || m >= take) next; c += $us + $sy; w += $wa; m++ }
    END { if (m) printf "%.0f/%.0f", c / m, w / m; else printf "-" }' "$1"
}

rows=$(mktemp); trap 'rm -f "$rows"' EXIT
for d in "$TPCC_DIR"/results/*_shardpg-t*/ "$NATIVE_DIR"/results/*_native-t*/ "$CITUS_DIR"/results/*_citus-t*/; do
  [[ -f "$d/summary.txt" ]] || continue
  sys=shardpg; [[ "$d" == *_native-t* ]] && sys=native; [[ "$d" == *_citus-t* ]] && sys=citus
  t=$(grep '^TERMINALS=' "$d/cluster.env" | tail -1 | cut -d= -f2)
  ramp=$(grep '^RAMPUP_S=' "$d/cluster.env" | tail -1 | cut -d= -f2)
  dur=$(grep '^DURATION_S=' "$d/cluster.env" | tail -1 | cut -d= -f2)
  tpmc=$(sed -n 's/^tpmC.*= *\([0-9.]*\).*/\1/p' "$d/summary.txt")
  ab=$(sed -n 's/.*中途中止的终端数 = \([0-9]*\).*/\1/p' "$d/summary.txt")
  vi=$(sed -n 's/^一致性条件违反总数 = \([0-9]*\).*/\1/p' "$d/summary.txt")
  lat=$(sed -n 's/^latency average = \([0-9.]*\) ms.*/\1/p' "$d/pgbench.out" | head -1)
  rt=$(sed -n 's/^number of transactions retried: [0-9]* (\([0-9.]*\)%).*/\1/p' "$d/pgbench.out" | head -1)
  fl=$(sed -n 's/^number of failed transactions: [0-9]* (\([0-9.]*\)%).*/\1/p' "$d/pgbench.out" | head -1)
  skip=$(( (${ramp:-0} + 9) / 10 + 1 )); take=$(( ${dur:-0} / 10 ))
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$sys" "$t" "${tpmc:--}" "${lat:--}" "${rt:--}" "${fl:--}" \
    "${ab:--}" "${vi:--}" "$(vm_avg "$d/vmstat_A.txt" $skip $take)" "$(vm_avg "$d/vmstat_B.txt" $skip $take)" \
    "$(vm_avg "$d/vmstat_C.txt" $skip $take)" "$(vm_avg "$d/vmstat_D.txt" $skip $take)" "$(basename "$d")" >> "$rows"
done
[[ -s "$rows" ]] || { echo "还没有正式轮次的结果"; exit 0; }

echo "== 每轮（CPU 列 = us+sy/wa %；D = 单独的客户端机，没有就是 -）"
printf '%-8s %5s %10s %9s %8s %7s %5s %5s %8s %8s %8s %8s  %s\n' 系统 终端 tpmC 延迟ms 重试% 失败% 中止 违例 A B C D 结果目录
sort -t$'\t' -k1,1 -k2,2n -k13,13 "$rows" | while IFS=$'\t' read -r s t tp la rt fl ab vi ca cb cc cd dn; do
  printf '%-8s %5s %10s %9s %8s %7s %5s %5s %8s %8s %8s %8s  %s\n' "$s" "$t" "$tp" "$la" "$rt" "$fl" "$ab" "$vi" "$ca" "$cb" "$cc" "$cd" "$dn"
done

echo
echo "== tpmC 中位数"
awk -F'\t' '$3 ~ /^[0-9.]+$/ { k = $2; v[$1, k, ++n[$1, k]] = $3; ts[k] = 1 }
  function med(s, k,   m, i, j, a, x) { m = n[s, k]; if (!m) return -1
    for (i = 1; i <= m; i++) a[i] = v[s, k, i]
    for (i = 2; i <= m; i++) { x = a[i]; for (j = i - 1; j >= 1 && a[j] > x; j--) a[j + 1] = a[j]; a[j + 1] = x }
    return (m % 2) ? a[(m + 1) / 2] : (a[m / 2] + a[m / 2 + 1]) / 2 }
  function f(x) { return x < 0 ? "-" : sprintf("%.0f", x) }
  function r(a, b) { return (a > 0 && b > 0) ? sprintf("%.2f", a / b) : "-" }
  END { printf "%5s %10s %10s %10s %14s %14s\n", "终端", "ShardPG", "单机PG", "Citus", "ShardPG/单机", "ShardPG/Citus"
    for (k in ts) {
      s = med("shardpg", k); p = med("native", k); c = med("citus", k)
      printf "%5s %10s %10s %10s %14s %14s\n", k, f(s), f(p), f(c), r(s, p), r(s, c) } }' "$rows" | { read -r h; echo "$h"; sort -n; }
