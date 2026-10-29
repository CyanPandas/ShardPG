#!/usr/bin/env python3
"""Relocate historical test drivers into a private existing-container cluster."""
from pathlib import Path
import re
import subprocess
import sys

def relocate_driver(text, scratch, upstream=False):
    socket = scratch + "/socket"
    work = scratch + "/work"
    # The historical suites require three workers for their three data copies;
    # the unused five workers are omitted from this resource-bounded run.
    text = text.replace("5432 5433 5434 5435 5436 5437 5438 5439 5440", "5432 5433 5434 5435")
    text = text.replace("5433 5434 5435 5436 5437 5438 5439 5440", "5433 5434 5435")
    text = text.replace('"$pport" -le 5440', '"$pport" -le 5435')
    text = text.replace('"$missing" -eq 9', '"$missing" -eq 4')
    text = re.sub(r"\b(543[1-9]|5440)\b", lambda m: str(int(m[0]) + 10000), text)
    text = text.replace("$VACU_LAB_ROOT/work/", "__VACU_HOST_WORK_PREFIX__")
    # Replace old /tmp first: new work paths themselves start with /tmp.
    text = text.replace("/tmp", socket).replace("/work/", work + "/")
    text = text.replace("docker exec ", f"docker exec -e PGHOST={socket} ")
    if upstream:
        text = text.replace("port = ${port}\n", "port = ${port}\n" +
                            f"unix_socket_directories = '{socket},/tmp'\n" +
                            "listen_addresses = '127.0.0.1'\n" +
                            "max_connections = 40\n" +
                            "max_worker_processes = 32\n" +
                            "pg_partdist.shard_vacuum_auto = off\n")
    # Catch unresolved original absolute paths and ports before any cluster starts.
    if "/work/" in text.replace(work + "/", "") or re.search(r"\b543[1-9]\b|\b5440\b", text):
        raise ValueError("Unrelocated path or port")
    text = text.replace("__VACU_HOST_WORK_PREFIX__", "$VACU_LAB_ROOT/work/")
    return text


def main():
    root = Path(sys.argv[1]).resolve()
    variant = sys.argv[2]
    scratch = sys.argv[3]
    if (not re.fullmatch(r"/home/yyt/vacu32-regress-\d{8}T\d{6}Z", str(root)) or
            variant not in ("production", "ring8") or
            scratch != f"/tmp/{root.name}/{variant}"):
        raise SystemExit("Unexpected private regression paths")
    paths = [root / "upstream-functions.sh", *sorted((root / "tests").glob("*.sh"))]
    for src in paths:
        upstream = src.name == "upstream-functions.sh"
        text = relocate_driver(src.read_text(), scratch, upstream)
        subprocess.run(["bash", "-n"], input=text, text=True, check=True)
        dst = root / variant / ("functions.sh" if upstream else "tests/" + src.name)
        dst.write_text(text)
    (root / variant / "tests/pagecmp.py").write_bytes((root / "tests/pagecmp.py").read_bytes())
    print(f"Private regression drivers prepared: {variant}, ports 15432..15435, work={scratch}/work")


if __name__ == "__main__":
    main()
