#!/bin/bash
set -e

PG_CTL=/work/pg-install/bin/pg_ctl
PSQL=/work/pg-install/bin/psql
DATA=/work/pg-cluster-data

start_node() {
    local name=$1 datadir=$2 port=$3
    if $PG_CTL status -D "$datadir" > /dev/null 2>&1; then
        echo "$name already running on port $port"
    else
        $PG_CTL start -D "$datadir" -l "$datadir/pg.log" -o "-p $port" -w
        echo "$name started on port $port"
    fi
}

start_node coordinator $DATA/master   5432
start_node worker1     $DATA/worker1  5433
start_node worker2     $DATA/worker2  5434

echo ''
echo 'Checking node status...'
$PSQL -p 5432 -c "SELECT nodename, nodeport, isactive FROM pg_dist_node;" 2>/dev/null || \
    echo '(No Citus worker nodes registered yet)'
echo 'Cluster ready.'
