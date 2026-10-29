#!/usr/bin/env bash
# Build extension sources in a scratch directory of the existing VACU container.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
ROUTE=${VACU_SSH_CONFIG:-"$ROOT/../ssh-jump-145.conf"}
[[ ! -f "$ROOT/ssh-jump-145.conf" || -n "${VACU_SSH_CONFIG:-}" ]] || ROUTE="$ROOT/ssh-jump-145.conf"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
REMOTE_ROOT="/home/yyt/vacu32-build-$STAMP"
CONTAINER=yyt-vacu-container
LOG="$ROOT/logs/build-147-$STAMP.log"
mkdir -p "$ROOT/logs"
exec > >(tee -a "$LOG") 2>&1
SSH=(ssh -F "$ROUTE" -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no)
remote147() {
    local quoted
    printf -v quoted '%q' "$1"
    "${SSH[@]}" lab145-2222 "ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=30 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no 10.77.110.147 $quoted"
}
echo '[1/4] Check existing VACU container and compiler (no image download)'
PREFLIGHT=$(cat <<'CHECK'
set -eu
test "$(pwd -P)" = /home/yyt
test "$(docker inspect --format '{{index .Config.Labels "yyt.vacu.root"}}' yyt-vacu-container)" = /home/yyt/vacu-bb62a57-20261029T074050Z
test "$(docker inspect --format '{{.State.Running}}' yyt-vacu-container)" = true
test "$(docker inspect --format '{{.Image}}' yyt-vacu-container)" = sha256:46cc50ca101783d3f3f27babe9d3f99a8caa908c42a0c94fd3fc753fb99e4d29
docker exec yyt-vacu-container bash -ceu 'test -x /work/pg-install/bin/pg_config; command -v gcc; command -v make'
CHECK
)
remote147 "$PREFLIGHT"
# Freeze the build inputs before upload; future source edits cannot alter them.
BUNDLE="$ROOT/logs/build-input-$STAMP.tar.gz"
tar -C "$ROOT" --exclude='*.o' --exclude='*.bc' --exclude='*.so' -czf "$BUNDLE" work/pg-partdist-src work/pg-raft-src remote-build.sh
sha256sum "$BUNDLE"
echo "[2/4] Upload extension sources: $(wc -c < "$BUNDLE") bytes; existing container: $CONTAINER"
echo "Scratch build: $REMOTE_ROOT"
remote147 "set -eu; umask 077; test ! -e '$REMOTE_ROOT'; mkdir '$REMOTE_ROOT'; tar --checkpoint=100 --checkpoint-action=echo='extract checkpoint %u' -xzf - -C '$REMOTE_ROOT'; echo 'Upload/extract complete'" < "$BUNDLE"
echo '[3/4] Compile in existing container; retain artifacts separately'
set +e
remote147 "set -eu; cd '$REMOTE_ROOT'; bash remote-build.sh '$CONTAINER'"
RESULT=$?
set -e
printf 'REMOTE_ROOT=%q\nCONTAINER=%q\nBUILD_EXIT=%q\n' "$REMOTE_ROOT" "$CONTAINER" "$RESULT" > "$ROOT/logs/latest-build.env"
echo "[4/4] Build exit: $RESULT; artifacts: $REMOTE_ROOT/artifacts; local transcript: $LOG"
exit "$RESULT"
