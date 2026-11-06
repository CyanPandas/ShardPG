# 容器内：从已录的 perfcg.data 统计 $1 的上层调用链（栈顶为 $1 的样本，取上 2..6 帧）
PERF="/tmp/perflib/ld-linux-x86-64.so.2 --library-path /tmp/perflib /tmp/perf"
$PERF script -i /tmp/perfcg.data -F comm,ip,sym 2>/dev/null | awk -v S="$1" -v D="${3:-6}" '
  function flush() { if (n>0 && f[1]==S) { k=f[2]; for (i=3;i<=D && i<=n;i++) k=k" < "f[i]; c[k]++; tot++ } n=0 }
  /^\t/ { s=$0; sub(/^\t +[0-9a-f]+ /,"",s); sub(/ \(inlined\)$/,"",s); sub(/\+0x[0-9a-f]+$/,"",s); n++; f[n]=s; next }
  { flush() }
  END { flush(); for (k in c) printf "%6d %5.1f%%  %s\n", c[k], 100*c[k]/tot, k }' | sort -rn | head -${2:-25}
