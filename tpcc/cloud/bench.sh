#!/usr/bin/env bash
# 跑一组 TPC-C：先热身一轮（不计成绩），再按终端数 × REPEAT 轮依次跑 scripts/run.sh；
# 每轮旁路采各台（含单独的客户端机 D）的 vmstat（10 s 一次）放进该轮结果目录。在客户端机上执行。最后调 report.sh 汇总。
# 同一个库上连续跑（与 lab 的 scale.sh 一样）；两套系统要用同样的终端序列。
# 用法：bash cloud/bench.sh <shardpg|native|citus> [终端数...]   不给终端数就用 cloud.env 的 TERMINAL_LIST
source "$(dirname "$0")/lib.sh"
SYS=${1:?用法：bench.sh <shardpg|native|citus> [终端数...]}; shift
case $SYS in
  shardpg) DIR=$TPCC_DIR;   MNT=/t;  DBH=$COORD_NODE_HOST; DBP=$COORD_PORT ;;
  native)  DIR=$NATIVE_DIR; MNT=/tn; DBH=$NATIVE_HOST;     DBP=$NATIVE_PORT ;;
  citus)   DIR=$CITUS_DIR;  MNT=/tc; DBH=$COORD_NODE_HOST; DBP=$COORD_PORT ;;
  *) die "第一个参数是 shardpg、native 或 citus" ;;
esac
read -r -a TLIST <<< "${*:-$TERMINAL_LIST}"
[[ -f "$DIR/cluster.env" ]] || die "没有 $DIR/cluster.env —— 先跑 deploy_$SYS.sh"
# run.sh 给 pgbench 的是 -T 预热+测量 与 --aggregate-interval=60，pgbench 要求前者是 60 的整数倍
(( (RAMPUP_S + DURATION_S) % 60 == 0 )) || die "RAMPUP_S + DURATION_S（$RAMPUP_S + $DURATION_S）必须是 60 的整数倍"
(( WARMUP_S % 60 == 0 )) || die "WARMUP_S（$WARMUP_S）必须是 60 的整数倍"

# 三套不能同时在跑：会抢同一批 CPU / 内存（deploy_<系统>.sh 会停掉另外两套）
busy=$(others_running "$SYS")
[[ -z "${busy// /}" ]] || die "别的系统的容器还在跑：$busy"
ensure_client
cpsql "$DBH" "$DBP" -qAt -c "SELECT 1" >/dev/null || die "连不上 $SYS（$DBH:$DBP）"

# 本系统的全部节点（host:port），PRE_CHECKPOINT=1 时每轮开始前逐个 CHECKPOINT
if [[ $SYS == native ]]; then NODES=("$NATIVE_HOST:$NATIVE_PORT")
else NODES=(); for k in "${!NODE_ID_LIST[@]}"; do NODES+=("${NODE_HOST_LIST[$k]}:${NODE_PORT_LIST[$k]}"); done; fi
# 短窗口用的三个开关（默认都是原来的行为）：
#   PRE_CHECKPOINT=1   每轮开始前所有节点手动 CHECKPOINT —— 测量窗口里不再碰上半截检查点（两套同样处理）
#   CONSISTENCY_EACH=0 每轮前后不做一致性检查（它要扫大表、冲掉缓存），改为全部跑完后做一次
#   MONITOR=0          轮次中不跑 monitor.sh（它每分钟 count(*) history 等，本身在扫表）
checkpoint_all() { local n t0=$(date +%s)
  for n in "${NODES[@]}"; do cpsql "${n%:*}" "${n#*:}" -qAt -c "CHECKPOINT" >/dev/null || die "$n 上 CHECKPOINT 失败"; done
  echo "    CHECKPOINT（${#NODES[@]} 个节点）用时 $(( $(date +%s) - t0 )) s"; }

# run_one <标签> <终端数> <预热秒> <测量秒>
run_one() {
  local tag=$1 t=$2 ramp=$3 dur=$4 envf tmp out rc i
  envf="$DIR/results/.env.$tag"; tmp=$(mktemp -d)
  { cat "$DIR/cluster.env"; echo "TERMINALS=$t"; echo "RAMPUP_S=$ramp"; echo "DURATION_S=$dur"
    echo "CONSISTENCY_EACH=${CONSISTENCY_EACH:-1}"; echo "MONITOR=${MONITOR:-1}"; } > "$envf"
  [[ "${PRE_CHECKPOINT:-0}" == 1 ]] && checkpoint_all
  local pids=()
  for i in "${!ALL_HOSTS[@]}"; do
    on "${ALL_HOSTS[$i]}" "timeout $((ramp + dur + 300)) vmstat -n -t 10" > "$tmp/vmstat_${ALL_LABELS[$i]}.txt" 2>&1 &
    pids+=($!)
  done
  log "$SYS $tag：$t 终端，预热 ${ramp}s + 测量 ${dur}s"
  cx "ENV_FILE=$MNT/results/.env.$tag bash $MNT/scripts/run.sh $SYS-$tag" > "$tmp/run.log" 2>&1; rc=$?
  kill "${pids[@]}" 2>/dev/null; wait "${pids[@]}" 2>/dev/null
  out=$(sed -n 's/.*结果目录 //p' "$tmp/run.log" | head -1)
  out=${out/#$MNT/$DIR}
  if [[ -z "$out" || ! -d "$out" ]]; then cat "$tmp/run.log"; die "run.sh 没给出结果目录（rc=$rc）"; fi
  mv "$tmp"/* "$out"/ && rmdir "$tmp"
  echo "    $(sed -n 3p "$out/summary.txt")"
  echo "    $(sed -n 4p "$out/summary.txt")；$(sed -n 5p "$out/summary.txt")"
  # 一笔 NewOrder 都没提交 = 压测根本没跑起来（参数错、连不上……），别接着空跑后面的轮次
  if [[ "$(sed -n 's/^tpmC.*= *\([0-9.]*\).*/\1/p' "$out/summary.txt")" =~ ^0*(\.0*)?$ ]]; then
    head -c 2000 "$out/pgbench.err"; die "$tag 的 tpmC 为 0（见 $out/pgbench.err）"
  fi
}

T0=$(date +%s)
# 热身终端数：默认取终端序列的中间一档；WARMUP_T=... 可指定（几套系统要用同一个值）
if [[ "${WARMUP_S:-0}" -gt 0 ]]; then
  run_one warmup "${WARMUP_T:-${TLIST[$(( ${#TLIST[@]} / 2 ))]}}" 0 "$WARMUP_S"
fi
for t in "${TLIST[@]}"; do
  for r in $(seq 1 "$REPEAT"); do run_one "t$t-r$r" "$t" "$RAMPUP_S" "$DURATION_S"; done
done
if [[ "${CONSISTENCY_EACH:-1}" == 0 ]]; then
  f="$DIR/results/consistency_final_$(date +%Y%m%d-%H%M%S).txt"
  cx "source $MNT/scripts/lib.sh && QC -f $MNT/sql/90_consistency.sql" > "$f" 2>&1
  viol=$(grep -A2 'violations' "$f" | grep -E '^ *[0-9]+ *$' | awk '{s+=$1} END{print s+0}')
  n=$(grep -c 'violations' "$f")
  log "收尾一致性检查（$n 项）：违反总数 = $viol（$f）"
  [[ "$n" -gt 0 && "$viol" == 0 ]] || log "!! 收尾一致性检查没通过或没跑成，见 $f"
fi
log "$SYS 跑完，用时 $(( ($(date +%s) - T0) / 60 )) min"
bash "$CLOUD_DIR/report.sh"
