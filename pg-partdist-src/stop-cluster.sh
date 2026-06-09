#!/bin/bash
set -e

PG_CTL=/work/pg-install/bin/pg_ctl
DATA=/work/pg-cluster-data

stop_node() {
    local name=$1 datadir=$2
    if $PG_CTL status -D "$datadir" > /dev/null 2>&1; then
        $PG_CTL stop -D "$datadir" -m fast
        echo "$name stopped"
    else
        echo "$name not running"
    fi
}

stop_node worker2     $DATA/worker2
stop_node worker1     $DATA/worker1
stop_node coordinator $DATA/master
echo 'Cluster stopped.'
