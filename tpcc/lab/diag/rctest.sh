# 两会话并发改同一行：B 在 A 提交前取快照、A 提交后改 —— RC 应等待并在最新版本上累加；RR 应 40001
# RC 下 B 随后再改一次、再数行：必须仍是 1 行（旧快照不能同时看见被覆盖的旧版本）
P="/work/pg-install/bin/psql -h 10.77.110.145 -p 31080 -U postgres -XAtq"
q() { docker exec shardpg-fix-client $P -c "$1" 2>&1; }
for iso in "READ COMMITTED" "REPEATABLE READ"; do
  v0=$(q "select w_ytd from warehouse where w_id=1")
  docker exec shardpg-fix-client bash -c "$P -c 'BEGIN' -c 'UPDATE warehouse SET w_ytd=w_ytd+1 WHERE w_id=1' -c 'SELECT pg_sleep(3)' -c 'COMMIT'" >/dev/null 2>&1 &
  sleep 0.5
  out=$(docker exec shardpg-fix-client bash -c "$P -c 'BEGIN ISOLATION LEVEL $iso' -c 'SELECT w_ytd FROM warehouse WHERE w_id=1' -c 'UPDATE warehouse SET w_ytd=w_ytd+10 WHERE w_id=1 RETURNING w_ytd' -c 'UPDATE warehouse SET w_ytd=w_ytd+100 WHERE w_id=1 RETURNING w_ytd' -c 'SELECT count(*), sum(w_ytd) FROM warehouse WHERE w_id=1' -c 'COMMIT'" 2>&1 | tr '\n' ' ' | cut -c1-200)
  wait
  v1=$(q "select count(*), sum(w_ytd) from warehouse where w_id=1")
  echo "[$iso] before=$v0 B-output=($out) after=$v1"
done
