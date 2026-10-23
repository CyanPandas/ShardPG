#!/usr/bin/env python3
"""Deploy a verified DDL release without writing lab host installation directories.

The four existing DDL containers are the entire allowlist. Stage a replacement
from each container's own image and installation, retaining its data bind mount.
Keep the stopped originals for rollback. No Docker pruning or host file writes.
Run --check first, then --stage-only and --activate-staged (or --deploy),
with a directory containing release.tar, deploy.sql and sha256.json. SSH uses the existing localhost:2222 jump connection.
"""
import argparse
import hashlib
import gzip
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path


NODES = [(148, "master", 32432), (145, "worker1", 32433),
         (147, "worker2", 32434), (148, "worker3", 32435)]
BIN = "/work/pg-install/bin"
BASE = "/work/pg-cluster-data"
FUNCTIONS = "'move_bucket_primary','move_primary','add_node','move_replica'"


def command(host, args):
    line = shlex.join(args)
    if host != 145:
        line = shlex.join(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4",
                           f"10.77.110.{host}", line])
    return ["ssh", "-p", "2222", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=4",
            "yyt@127.0.0.1", line]


def run(host, args, data=None, timeout=180):
    return subprocess.run(command(host, args), input=(b"" if data is None else data), stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, check=True, timeout=timeout).stdout


def docker(host, *args, **kwargs):
    return run(host, ["docker", *args], **kwargs)


def sql(node, statement):
    host, name, port = node
    return docker(host, "exec", "-i", f"shardpg-ddl-{name}", f"{BIN}/psql",
                  "-h", "/tmp", "-p", str(port), "-U", "postgres", "-X", "-qAt",
                  "-v", "ON_ERROR_STOP=1", data=statement.encode(), timeout=300).decode().strip()


def pg(node, action):
    host, name, _ = node
    args = ["exec", f"shardpg-ddl-{name}", f"{BIN}/pg_ctl", "-D", f"{BASE}/{name}"]
    args += (["-m", "fast", "stop"] if action == "stop" else
             ["-l", f"{BASE}/{name}/pg.log", "start"])
    docker(host, *args, "-w", "-t", "90", timeout=110)


def main():
    if not __debug__:
        raise RuntimeError("run without Python -O: deployment assertions must stay enabled")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("release_dir", type=Path)
    ap.add_argument("--tag", default="20261023-n114")
    ap.add_argument("--deploy", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--stage-only", action="store_true")
    ap.add_argument("--activate-staged", action="store_true")
    args = ap.parse_args()
    if sum((args.deploy, args.check, args.stage_only, args.activate_staged)) > 1:
        ap.error("choose only one action")
    if not args.tag.replace("-", "").isalnum():
        ap.error("tag must contain only letters, numbers and hyphens")
    release = args.release_dir.resolve()
    archive = (release / "release.tar").read_bytes()
    deploy_sql = (release / "deploy.sql").read_text()
    hashes = json.loads((release / "sha256.json").read_text())
    manifest = {"tag": args.tag, "archive_sha256": hashlib.sha256(archive).hexdigest(), "nodes": []}
    for node in NODES:
        host, name, port = node
        cname = f"shardpg-ddl-{name}"
        obj = json.loads(docker(host, "inspect", cname))[0]
        mounts = {m["Destination"]: m for m in obj["Mounts"]}
        expected = "/data1/yyt/shardpg-ddl-demo" if host == 145 else "/home/yyt/shardpg-ddl-demo"
        assert set(mounts) == {"/work/pg-install", BASE}, f"unexpected mounts: {cname}"
        assert mounts[BASE]["Source"] == expected + "/data" and mounts[BASE]["RW"]
        assert mounts["/work/pg-install"]["Source"] == expected + "/pg-install"
        assert obj["State"]["Running"] and obj["HostConfig"]["NetworkMode"] == "host"
        assert obj["Config"]["User"] == "postgres" and obj["Config"]["Cmd"] == ["sleep", "infinity"]
        assert obj["HostConfig"]["RestartPolicy"]["Name"] == "no"
        assert all(obj["HostConfig"][k] == 0 for k in ("Memory", "NanoCpus", "CpuQuota", "CpuPeriod")), "preserve any existing resource limits before deploying"
        assert obj["HostConfig"]["CpusetCpus"] == "" and obj["HostConfig"]["PidsLimit"] in (None, 0, -1)
        assert not obj["HostConfig"]["Privileged"] and not obj["HostConfig"].get("Devices")
        idle = sql(node, "SELECT count(*) FROM pg_dist_partition; SELECT count(*) FROM pg_prepared_xacts; "
                   "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0;")
        assert idle == "0\n0\n0", f"deploy only on the idle DDL demo cluster: {name}: {idle}"
        item = dict(host=host, name=name, port=port, old_id=obj["Id"],
                    data_source=mounts[BASE]["Source"], shm=obj["HostConfig"]["ShmSize"],
                    backup=f"{cname}-backup-{args.tag}", candidate=f"{cname}-stage-{args.tag}",
                    image=f"shardpg-ddl-{name}-backup:{args.tag}")
        manifest["nodes"].append(item)
        print(f"checked {host} {cname} port={port}", flush=True)
    assert sql(NODES[0], "SELECT string_agg(node_id::text||':'||port, ',' ORDER BY node_id) FROM partdist.node_map;") == "1:32432,2:32433,3:32434,4:32435"
    if not (args.deploy or args.stage_only or args.activate_staged):
        return
    if args.activate_staged:
        previous = json.loads((release / "manifest.json").read_text())
        assert previous["archive_sha256"] == manifest["archive_sha256"]
        assert previous["tag"] == args.tag
        for item, saved in zip(manifest["nodes"], previous["nodes"]):
            assert all(item[k] == saved[k] for k in item)
            candidate = json.loads(docker(item["host"], "inspect", item["candidate"]))[0]
            assert candidate["Id"] == saved["candidate_id"] and candidate["State"]["Running"]
            item["candidate_id"] = saved["candidate_id"]
    else:
        # Stage all replacements while the original databases are still running.
        for item, node in zip(manifest["nodes"], NODES):
            host, name, _ = node
            old = f"shardpg-ddl-{name}"
            backup_sql = "SET citus.enable_ddl_propagation=off;\n" + sql(node,
                "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                f"WHERE n.nspname IN ('partdist','shardpg') AND p.proname IN ({FUNCTIONS}) ORDER BY p.oid;")
            (release / f"rollback-{name}.sql").write_text(backup_sql)
            exists = docker(host, "ps", "-aq", "--filter", f"name=^{item['candidate']}$").decode().strip()
            if exists:
                candidate = json.loads(docker(host, "inspect", item["candidate"]))[0]
                assert candidate["Config"]["Labels"].get("io.shardpg.project") == "ddl"
                assert candidate["Config"]["Labels"].get("io.shardpg.release") == args.tag
                assert len(candidate["Mounts"]) == 1
                assert candidate["Mounts"][0]["Destination"] == BASE and candidate["Mounts"][0]["Source"] == item["data_source"]
                # postmaster.pid belongs to the original container's PID namespace.
                # Inspect actual processes rather than probing that shared pid file.
                processes = docker(host, "top", item["candidate"], "-eo", "pid,comm").decode().splitlines()
                assert "postgres" not in [line.split()[-1] for line in processes if line.split()]
            else:
                docker(host, "commit", "--pause=false", old, item["image"], timeout=300)
                docker(host, "create", "--init", "--name", item["candidate"], "--network", "host",
                       "--shm-size", str(item["shm"]), "--label", "io.shardpg.project=ddl",
                       "--label", f"io.shardpg.release={args.tag}", "--mount",
                       f"type=bind,src={item['data_source']},dst={BASE}", item["image"])
            item["candidate_id"] = json.loads(docker(host, "inspect", item["candidate"]))[0]["Id"]
            docker(host, "start", item["candidate"])
            def installed_hashes():
                result = docker(host, "exec", item["candidate"], "sha256sum",
                                *[f"/work/pg-install/{path}" for path in hashes]).decode()
                return {line.split()[1].removeprefix("/work/pg-install/"): line.split()[0]
                        for line in result.splitlines()}
            try:
                already_staged = installed_hashes() == hashes
            except subprocess.CalledProcessError:
                already_staged = False
            if not already_staged:
                # Stream directly between the two DDL containers on the same host.
                source = shlex.join(["docker", "exec", old, "tar", "-C", "/work", "-cf", "-", "pg-install"])
                target = shlex.join(["docker", "exec", "-i", "-u", "root", item["candidate"], "tar", "--ignore-zeros", "-C", "/work", "-xf", "-"])
                run(host, ["bash", "-o", "pipefail", "-c", source + " | " + target])
                # The reverse tunnel can be slow. Send compressed bytes and consume
                # the complete stream, including tar's final padding.
                payload = gzip.compress(archive)
                print(f"uploading {name}: {len(payload)} compressed bytes", flush=True)
                docker(host, "exec", "-i", "-u", "root", item["candidate"], "tar", "--ignore-zeros",
                       "-C", "/work/pg-install", "-xzf", "-", data=payload)
            docker(host, "exec", item["candidate"], f"{BIN}/postgres", "--version")
            assert installed_hashes() == hashes, f"release checksum mismatch on {name}"
            print(f"staged {host} {name}", flush=True)
            (release / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (release / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if args.stage_only:
        print("DDL_STAGED: original databases are still running", flush=True)
        return
    manifest["tso_before"] = int(sql(NODES[0], "SELECT partdist.partdist_tso_client_start_ts();"))
    stopped = []
    switched = []
    try:
        # Shared-memory layout changed: stop all old databases before starting any new one.
        for node in NODES[1:] + NODES[:1]:
            assert sql(node, "SELECT count(*) FROM pg_prepared_xacts;") == "0"
            stopped.append(node)
            pg(node, "stop")
        for item, node in zip(manifest["nodes"], NODES):
            host, name, _ = node
            old = f"shardpg-ddl-{name}"
            docker(host, "stop", "--time", "10", old)
            docker(host, "rename", old, item["backup"])
            switched.append((item, node))
            docker(host, "rename", item["candidate"], old)
        for node in NODES:
            pg(node, "start")
        for node in NODES:
            sql(node, deploy_sql)
        for _ in range(40):
            if sql(NODES[0], "SELECT (leader_node_id>0)::int FROM partdist.pg_raft_group_status() WHERE group_id=0;") == "1":
                break
            time.sleep(2)
        else:
            raise RuntimeError("control group did not elect a leader")
        for node in NODES:
            assert sql(node, "SELECT (partdist.partdist_tso_client_start_ts()>0)::int;") == "1"
            print(f"ready {node[0]} {node[1]}", flush=True)
        manifest["tso_after"] = int(sql(NODES[0], "SELECT partdist.partdist_tso_client_start_ts();"))
        assert manifest["tso_after"] > manifest["tso_before"], "TSO must remain monotonic across restart"
    except Exception:
        print("deployment failed; restoring original DDL containers", flush=True)
        for item, node in reversed(switched):
            host, name, _ = node
            old = f"shardpg-ddl-{name}"
            found = docker(host, "ps", "-aq", "--no-trunc", "--filter", f"name=^{old}$").decode().strip()
            if found:
                assert found == item["candidate_id"], "refusing to stop an unexpected container"
                try:
                    pg(node, "stop")
                except subprocess.CalledProcessError:
                    pass
                docker(host, "stop", "--time", "90", old)
                docker(host, "rename", old, item["candidate"])
            docker(host, "rename", item["backup"], old)
            docker(host, "start", old)
        for node in NODES:
            if node in stopped:
                try:
                    docker(node[0], "exec", f"shardpg-ddl-{node[1]}", f"{BIN}/pg_ctl", "-D", f"{BASE}/{node[1]}", "status")
                except subprocess.CalledProcessError:
                    pg(node, "start")
                sql(node, (release / f"rollback-{node[1]}.sql").read_text())
        raise
    (release / "deployed.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print("DDL_DEPLOYED: old containers retained stopped for rollback", flush=True)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as err:
        print(err.stderr.decode(errors="replace"), file=sys.stderr)
        raise
