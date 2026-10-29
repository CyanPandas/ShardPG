#!/usr/bin/env bash
# Test the successful build in private databases inside the existing container.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
ROUTE=${VACU_SSH_CONFIG:-"$ROOT/../ssh-jump-145.conf"}
[[ ! -f "$ROOT/ssh-jump-145.conf" || -n "${VACU_SSH_CONFIG:-}" ]] || ROUTE="$ROOT/ssh-jump-145.conf"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG="$ROOT/logs/validate-147-$STAMP.log"
mkdir -p "$ROOT/logs"
exec > >(tee -a "$LOG") 2>&1
SSH=(ssh -F "$ROUTE" -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no)
remote147() {
    local quoted
    printf -v quoted '%q' "$1"
    "${SSH[@]}" lab145-2222 "ssh -o BatchMode=yes -o ConnectTimeout=30 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no 10.77.110.147 $quoted"
}
source "$ROOT/logs/latest-build.env"
[[ "$BUILD_EXIT" == 0 && "$CONTAINER" == yyt-vacu-container ]] || { echo 'Successful existing-container build required' >&2; exit 1; }
BUILD_ROOT=$REMOTE_ROOT
[[ "$BUILD_ROOT" =~ ^/home/yyt/vacu32-build-[0-9]{8}T[0-9]{6}Z$ ]] || exit 1
BUILD_STAMP=${BUILD_ROOT##*/vacu32-build-}
# Tests may evolve, but the C/SQL input must still match the compiled snapshot.
python3 - "$ROOT" "$ROOT/logs/build-input-$BUILD_STAMP.tar.gz" <<'PY'
from pathlib import Path
import sys
import tarfile
root = Path(sys.argv[1])
with tarfile.open(sys.argv[2], "r:gz") as bundle:
    expected = {}
    for member in bundle.getmembers():
        path = Path(member.name)
        if (member.isfile() and "tests" not in path.parts and
                (path.suffix in (".c", ".h", ".sql", ".control") or path.name == "Makefile")):
            expected[member.name] = bundle.extractfile(member).read()
    for name, data in expected.items():
        if not (root / name).is_file() or (root / name).read_bytes() != data:
            raise SystemExit("Build inputs changed; rebuild first: " + name)
print("Compiled C/SQL snapshot matches current source")
PY
REMOTE_ROOT="/home/yyt/vacu32-test-$STAMP"
echo "[1/3] Reuse successful build $BUILD_ROOT in yyt-vacu-container"
remote147 "set -eu; test \"\$(pwd -P)\" = /home/yyt; test \"\$(docker inspect --format '{{index .Config.Labels \"yyt.vacu.root\"}}' yyt-vacu-container)\" = /home/yyt/vacu-bb62a57-20261029T074050Z; test -s '$BUILD_ROOT/artifacts/pg_partdist-32.so'; test -s '$BUILD_ROOT/artifacts/pg_partdist-ring8.so'"
# Upload test drivers only. Libraries and PostgreSQL remain on 147.
BUNDLE="$ROOT/logs/validate-input-$STAMP.tar.gz"
tar -C "$ROOT" -czf "$BUNDLE" remote-wrap.sh work/pg-partdist-src/tests/local/test_vacu_contract.py work/pg-partdist-src/tests/local/test_xid_wrap_contract.py
sha256sum "$BUNDLE"
echo "[2/3] Upload test drivers: $(wc -c < "$BUNDLE") bytes; test root $REMOTE_ROOT"
remote147 "set -eu; umask 077; test ! -e '$REMOTE_ROOT'; mkdir '$REMOTE_ROOT'; tar -xzf - -C '$REMOTE_ROOT'; mv '$REMOTE_ROOT/work/pg-partdist-src/tests/local' '$REMOTE_ROOT/tests'; echo 'Test drivers uploaded'" < "$BUNDLE"
echo '[3/3] Run wrap contracts in private databases; no new container or image download'
set +e
remote147 "set -eu; cd '$REMOTE_ROOT'; bash remote-wrap.sh '$BUILD_ROOT'"
RESULT=$?
set -e
printf 'REMOTE_ROOT=%q\nVALIDATION_EXIT=%q\n' "$REMOTE_ROOT" "$RESULT" > "$ROOT/logs/latest-validation.env"
# Collect only this newly created lab's logs, including failures.
remote147 "cd '$REMOTE_ROOT'; find production/logs ring8/logs production/work/pg-cluster-data ring8/work/pg-cluster-data -type f -name '*.log' -print0 2>/dev/null | tar --null -T - -czf -" > "$ROOT/logs/validate-diagnostics-$STAMP.tar.gz" || true
echo "Validation exit: $RESULT; local transcript: $LOG"
exit "$RESULT"
