#!/usr/bin/env python3
"""Record a completed build, or verify its installed files. Never builds/deploys."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

FILES = ("bin/postgres", "bin/psql", "lib/postgresql/pg_partdist.so",
         "lib/postgresql/pg_raft.so", "lib/postgresql/citus.so",
         "share/postgresql/extension/pg_partdist--1.0.sql",
         "share/postgresql/extension/pg_raft--1.0.sql",
         "share/postgresql/extension/first_shard_coordinator.sql",
         "share/postgresql/extension/first_shard_decision.sql",
         "share/postgresql/extension/ddl_metadata.sql")


def digest(path):
    with path.open("rb") as stream:
        result = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
        return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["record", "probe"])
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--source", type=Path, help="record: the source used for the completed build")
    args = parser.parse_args()
    if args.mode == "record":
        if args.source is None:
            parser.error("record requires --source; run only AFTER building this source")
        commit = subprocess.check_output(["git", "-C", str(args.source), "rev-parse", "HEAD"], text=True).strip()
        subprocess.run(["git", "-C", str(args.source), "diff", "--exit-code", "HEAD"], check=True)
        manifest = {"source_commit": commit, "recorded_at": time.time(),
                    "files": {p: digest(args.prefix / p) for p in FILES}}
        # Never relabel an existing release manifest in place.
        with args.manifest.open("x") as stream:
            json.dump(manifest, stream, indent=2)
            stream.write("\n")
    else:
        manifest = json.loads(args.manifest.read_text())
        if set(manifest["files"]) != set(FILES):
            raise RuntimeError("manifest must cover postgres, patched psql, three extensions and installed SQL")
        for name, expected in manifest["files"].items():
            if digest(args.prefix / name) != expected:
                raise RuntimeError("installed artifact mismatch: " + name)
        print(json.dumps({"source_commit": manifest["source_commit"],
                          "manifest_sha256": digest(args.manifest),
                          "latest_artifact_mtime": max((args.prefix / p).stat().st_mtime for p in FILES),
                          "recorded_at": manifest["recorded_at"], "files": manifest["files"]}))


if __name__ == "__main__":
    main()
