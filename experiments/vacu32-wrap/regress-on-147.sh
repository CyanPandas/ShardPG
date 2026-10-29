#!/usr/bin/env bash
# Reuse existing compiled artifacts for historical and three-replica tests.
set -euo pipefail
MODE=${1:---all}
[[ "$MODE" == --all || "$MODE" == --replay-only ]] || { echo 'Usage: regress-on-147.sh [--all|--replay-only]' >&2; exit 2; }
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
ROUTE=${VACU_SSH_CONFIG:-"$ROOT/../ssh-jump-145.conf"}
[[ ! -f "$ROOT/ssh-jump-145.conf" || -n "${VACU_SSH_CONFIG:-}" ]] || ROUTE="$ROOT/ssh-jump-145.conf"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG="$ROOT/logs/regress-147-$STAMP.log"
mkdir -p "$ROOT/logs"
exec > >(tee -a "$LOG") 2>&1
source "$ROOT/logs/latest-build.env"
[[ "$BUILD_EXIT" == 0 && "$CONTAINER" == yyt-vacu-container ]] || exit 1
BUILD_ROOT=$REMOTE_ROOT
[[ "$BUILD_ROOT" =~ ^/home/yyt/vacu32-build-[0-9]{8}T[0-9]{6}Z$ ]] || exit 1
BUILD_STAMP=${BUILD_ROOT##*/vacu32-build-}
python3 - "$ROOT" "$ROOT/logs/build-input-$BUILD_STAMP.tar.gz" <<'PY'
from pathlib import Path
import sys
import tarfile
root = Path(sys.argv[1])
with tarfile.open(sys.argv[2], "r:gz") as bundle:
    for member in bundle.getmembers():
        path = Path(member.name)
        if (member.isfile() and "tests" not in path.parts and
                (path.suffix in (".c", ".h", ".sql", ".control") or path.name == "Makefile")):
            if not (root / path).is_file() or (root / path).read_bytes() != bundle.extractfile(member).read():
                raise SystemExit("Build inputs changed; rebuild first: " + str(path))
print("Compiled C/SQL snapshot matches current source")
PY
REMOTE_ROOT="/home/yyt/vacu32-regress-$STAMP"
SSH=(ssh -F "$ROUTE" -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no)
remote147() {
    local quoted
    printf -v quoted '%q' "$1"
    "${SSH[@]}" lab145-2222 "ssh -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no 10.77.110.147 $quoted"
}
echo "[1/3] Existing container yyt-vacu-container; successful build $BUILD_ROOT"
remote147 "set -eu; test \"\$(pwd -P)\" = /home/yyt; test \"\$(docker inspect --format '{{index .Config.Labels \"yyt.vacu.root\"}}' yyt-vacu-container)\" = /home/yyt/vacu-bb62a57-20261029T074050Z; test -s '$BUILD_ROOT/artifacts/pg_partdist-32.so'; test -s '$BUILD_ROOT/artifacts/pg_partdist-ring8.so'"
BUNDLE="$ROOT/logs/regress-input-$STAMP.tar.gz"
tar -C "$ROOT" -czf "$BUNDLE" remote-regress.sh relocate-regression.py upstream-functions.sh \
    work/pg-partdist-src/tests/test_shard_vacuum_p5.sh \
    work/pg-partdist-src/tests/test_shard_vacuum_auto_p7.sh \
    work/pg-partdist-src/tests/test_shard_vacuum_replay_p5.sh \
    work/pg-partdist-src/tests/lib_node_health.sh work/pg-partdist-src/tests/pagecmp.py
sha256sum "$BUNDLE"
echo "[2/3] Upload regression drivers: $(wc -c < "$BUNDLE") bytes; test root $REMOTE_ROOT"
remote147 "set -eu; umask 077; test ! -e '$REMOTE_ROOT'; mkdir '$REMOTE_ROOT'; tar -xzf - -C '$REMOTE_ROOT'; mv '$REMOTE_ROOT/work/pg-partdist-src/tests' '$REMOTE_ROOT/tests'" < "$BUNDLE"
echo "[3/3] Regression mode $MODE; production suites then short-ring replay; only private nodes are started/stopped"
set +e
remote147 "set -eu; cd '$REMOTE_ROOT'; bash remote-regress.sh '$BUILD_ROOT' '$MODE'"
RESULT=$?
set -e
printf 'REMOTE_ROOT=%q\nREGRESSION_EXIT=%q\nREGRESSION_MODE=%q\n' "$REMOTE_ROOT" "$RESULT" "$MODE" > "$ROOT/logs/latest-regression.env"
remote147 "cd '$REMOTE_ROOT'; find production/logs ring8/logs production/work/pg-cluster-data ring8/work/pg-cluster-data -type f -name '*.log' -print0 2>/dev/null | tar --null -T - -czf -" > "$ROOT/logs/regress-diagnostics-$STAMP.tar.gz" || true
echo "Regression exit: $RESULT; local transcript: $LOG"
exit "$RESULT"
