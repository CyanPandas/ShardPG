#!/usr/bin/env python3
"""Package this checkout's N114 release after a verified clean pg_raft build.

Copy the built pg_raft.so into OUTPUT first, then:
  python3 package_release.py OUTPUT
  python3 deploy_release.py OUTPUT --check
  python3 deploy_release.py OUTPUT --stage-only
  python3 deploy_release.py OUTPUT --activate-staged
  python3 verify_release.py OUTPUT

The lab baseline must already include N107-N112. Packaging does not compile or
replace a running library. The stopped original containers retain the old host
installation; new containers keep their installation in their own filesystem.
"""
import hashlib
import json
import sys
import tarfile
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[3]
    output = Path(sys.argv[1]).resolve()
    library = output / "pg_raft.so"
    if not library.is_file():
        raise SystemExit("copy the verified build's pg_raft.so into OUTPUT first")
    files = {
        "lib/postgresql/pg_raft.so": library,
        "share/postgresql/extension/pg_partdist--1.0.sql": root / "pg-partdist-src/sql/pg_partdist--1.0.sql",
        "share/postgresql/extension/pg_raft--1.0.sql": root / "pg-raft-src/sql/pg_raft--1.0.sql",
        "bin/psql": root / "pg-install/bin/psql",
        "bin/shardpg_ctl": root / "pg-partdist-src/demo/lab/shardpg_ctl",
        "bin/isolated_node.sh": root / "pg-partdist-src/demo/lab/isolated_node.sh",
        "etc/lab-nodes.conf": root / "pg-partdist-src/demo/lab/nodes.conf",
    }
    sql = (root / "pg-partdist-src/sql/pg_partdist--1.0.sql").read_text()
    functions = []
    for name in ("partdist.partition_map_unregister", "partdist.move_bucket_primary", "shardpg.move_primary",
                 "shardpg.add_node", "shardpg.move_replica"):
        start = sql.index(f"CREATE OR REPLACE FUNCTION {name}(")
        end = sql.index("$fn$;", sql.index("AS $fn$", start)) + len("$fn$;")
        functions.append(sql[start:end])
    (output / "deploy.sql").write_text("SET citus.enable_ddl_propagation = off;\nBEGIN;\n\n" +
                                      "\n\n".join(functions) + "\nCOMMIT;\n")
    with tarfile.open(output / "release.tar", "w") as archive:
        for name, source in files.items():
            info = archive.gettarinfo(str(source), arcname=name)
            if name == "etc/lab-nodes.conf":
                # shardpg_ctl updates this inside worker1 as the container's postgres.
                info.uid = info.gid = 999
                info.uname = info.gname = "postgres"
                info.mode = 0o644
            if name.startswith("bin/"):
                info.mode = 0o755
            with source.open("rb") as data:
                archive.addfile(info, data)
    hashes = {name: hashlib.sha256(source.read_bytes()).hexdigest() for name, source in files.items()}
    (output / "sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
    print(f"Packaged {len(files)} files and {len(functions)} function updates in {output}")


if __name__ == "__main__":
    main()
