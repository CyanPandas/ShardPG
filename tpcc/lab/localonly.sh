# 用法：localonly.sh on|off  —— 两个套件的 new_order / payment 脚本切换"无远程仓"（函数第二参传 1 ⇒ 不选远程仓，不产生跨机 2PC）
R=/data1/yyt/shardpg-fix-20261014
for d in tpcc tpcc-native; do
  for f in new_order payment; do
    p=$R/$d/pgbench/$f.sql
    if [ "$1" = on ]; then
      [ -f $p.orig ] || cp $p $p.orig
      sed -i "s/SELECT tpcc.$f(:w, :nw);/SELECT tpcc.$f(:w, 1);/" $p
    else
      [ -f $p.orig ] && mv $p.orig $p
    fi
    grep -h "SELECT tpcc" $p
  done
done
