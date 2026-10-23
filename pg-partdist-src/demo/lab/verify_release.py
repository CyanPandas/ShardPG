#!/usr/bin/env python3
"""Small DDL acceptance run, restricted to the four DDL demo containers.

Usage: python3 verify_release.py /path/to/release
Creates only ddl_release_check_c and ddl_release_check_o; refuses existing names.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

from deploy_release import NODES, BIN, BASE, docker, sql


def equal(label, actual, expected):
    if actual != expected:
        raise RuntimeError(f"{label}: expected {expected!r}, got {actual!r}")
    print(f"PASS {label}: {actual}", flush=True)


def main():
    release = Path(sys.argv[1])
    hashes = json.loads((release / "sha256.json").read_text())
    master = NODES[0]
    c, o = "ddl_release_check_c", "ddl_release_check_o"
    marks = {}
    for node in NODES:
        host, name, _ = node
        cname = f"shardpg-ddl-{name}"
        found = docker(host, "exec", cname, "sha256sum",
                       *[f"/work/pg-install/{path}" for path in hashes]).decode()
        for line in found.splitlines():
            digest, path = line.split()
            equal(f"{name} {path}", digest, hashes[path.removeprefix("/work/pg-install/")])
        equal(f"{name} SQL", sql(node, "SELECT count(*) FROM pg_proc WHERE "
              "(proname='move_bucket_primary' AND position('primary of %s shard(s)' IN prosrc)>0) OR "
              "(proname='add_node' AND position('shardpg add worker' IN prosrc)>0) OR "
              "(proname='move_replica' AND position('log not caught up (%s at %s' IN prosrc)>0) OR "
              "(proname='partition_map_unregister' AND position('P7-N117' IN prosrc)>0);"), "4")
        marks[name] = int(docker(host, "exec", cname, "sh", "-c",
                                 f"wc -l < {BASE}/{name}/pg.log").decode())
    equal("test names are unused", sql(master,
          f"SELECT to_regclass('{c}') IS NULL AND to_regclass('{o}') IS NULL;"), "t")
    created = False
    try:
        sql(master, f"CREATE TABLE {c}(w int, id int, v int, PRIMARY KEY(w,id), "
            "UNIQUE(w,v) DEFERRABLE INITIALLY DEFERRED, SHARD KEY(w) SHARDS 4 REPLICAS 1);")
        created = True
        sql(master, f"CREATE TABLE {o}(w int, id int, PRIMARY KEY(w,id), SHARD KEY(w) COLOCATE WITH {c});")
        sql(master, f"INSERT INTO {c} SELECT w,i,i FROM generate_series(1,8) w, generate_series(1,3) i; "
            f"INSERT INTO {o} SELECT w,i FROM generate_series(1,8) w, generate_series(1,3) i;")
        join = f"SELECT count(*) FROM {c} c JOIN {o} o USING(w,id);"
        equal("colocated join", sql(master, join), "24")
        sid = int(sql(master, f"SELECT get_shard_id_for_distribution_column('{c}',1);"))
        other_key = int(sql(master, f"SELECT min(k) FROM generate_series(2,8) k JOIN partdist.partition_map m "
            f"ON m.partition_id=get_shard_id_for_distribution_column('{c}',k) WHERE m.primary_node<>"
            f"(SELECT primary_node FROM partdist.partition_map WHERE partition_id={sid});"))
        try:
            sql(master, f"BEGIN; INSERT INTO {c} VALUES(1,90,90); "
                f"INSERT INTO {c} VALUES({other_key},90,777),({other_key},91,777); COMMIT;")
        except subprocess.CalledProcessError as err:
            if b"duplicate key value violates unique constraint" not in err.stderr:
                raise
            print("PASS distributed PREPARE fails on deferred unique constraint", flush=True)
        else:
            raise RuntimeError("expected deferred constraint failure")
        equal("both branches rolled back", sql(master, f"SELECT count(*) FROM {c} WHERE id>=90;"), "0")
        source = int(sql(master, f"SELECT n.port FROM partdist.partition_map m JOIN partdist.node_map n "
            f"ON n.node_id=ANY(m.secondary_nodes) WHERE m.partition_id={sid};"))
        target = int(sql(master, f"SELECT min(n.port) FROM partdist.node_map n, partdist.partition_map m "
            f"WHERE m.partition_id={sid} AND n.node_id<>1 AND n.node_id<>m.primary_node "
            "AND NOT n.node_id=ANY(m.secondary_nodes);"))
        result = sql(master, f"SELECT step FROM shardpg.move_replica('{c}_{sid}',{source},{target});")
        equal("replica migration completed", result.splitlines()[-1], "done")
        equal("join after replica migration", sql(master, join), "24")
        result = sql(master, f"SELECT * FROM shardpg.move_primary('{c}_{sid}',{target});")
        if "primary of" not in result:
            raise RuntimeError(f"unexpected move_primary result: {result}")
        print(f"PASS planned primary transfer: {result}", flush=True)
        equal("join after primary transfer", sql(master, join), "24")
        sql(master, f"BEGIN; UPDATE {c} SET v=v+10 WHERE w=1; UPDATE {c} SET v=v+10 WHERE w={other_key}; COMMIT;")
        equal("cross-shard commit", sql(master, f"SELECT sum(v) FROM {c} WHERE w IN (1,{other_key});"), "72")
        # Check real values; missing output is a failure, never an implicit zero.
        for node in NODES:
            for _ in range(15):
                prepared = sql(node, "SELECT count(*) FROM pg_prepared_xacts;")
                holes = sql(node, "SELECT coalesce(sum(m-n),0) FROM (SELECT max(log_index) m,count(*) n "
                            "FROM partdist.raft_log WHERE group_id<>0 GROUP BY group_id) q;")
                if prepared == "0" and holes == "0":
                    break
                time.sleep(2)
            equal(f"{node[1]} prepared transactions", prepared, "0")
            equal(f"{node[1]} SQL log holes", holes, "0")
    finally:
        if created:
            sql(master, f"DROP TABLE IF EXISTS {o}, {c};")
    for node in NODES:
        host, name, _ = node
        equal(f"{name} data groups after DROP", sql(node,
              "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0;"), "0")
        equal(f"{name} partition map after DROP", sql(node,
              "SELECT count(*) FROM partdist.partition_map;"), "0")
        equal(f"{name} hardstate after DROP", sql(node,
              "SELECT count(*) FROM pg_ls_dir('.') f WHERE f ~ '^pg_raft_hardstate\\.[1-9][0-9]*$';"), "0")
        log = docker(host, "exec", f"shardpg-ddl-{name}", "tail", "-n", f"+{marks[name]+1}",
                     f"{BASE}/{name}/pg.log").decode()
        for line in log.splitlines():
            if " PANIC:" in line or "terminated by signal 11" in line or "terminated by signal 6" in line:
                raise RuntimeError(f"{name} crashed: {line}")
    print("DDL_LAB_ACCEPTANCE_PASS", flush=True)


if __name__ == "__main__":
    main()
