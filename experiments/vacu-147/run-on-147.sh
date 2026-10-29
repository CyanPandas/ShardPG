#!/usr/bin/env bash
# Run on the source machine using the historical 2222 -> 145 -> 147 route.
# Confirm the remote yyt directory before creating an isolated child directory.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
TARGET=10.77.110.147
SSH=(ssh -F "$ROOT/ssh-jump-145.conf" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes
     -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no)
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
REMOTE_REL="vacu-bb62a57-$STAMP"
LOG="$ROOT/logs/remote-147-$STAMP.log"
mkdir -p "$ROOT/logs"
exec > >(tee -a "$LOG") 2>&1

remote147() {
    local quoted
    printf -v quoted '%q' "$1"
    "${SSH[@]}" lab145-2222 \
        "ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no $TARGET $quoted"
}

echo 'Read-only remote preflight: hostname, user and Docker availability'
remote147 'hostname; id -un; command -v docker; docker info --format "Docker server: {{.ServerVersion}}"'

# lab147 logs in as yyt. Require its canonical login directory to actually be
# named yyt; never guess a writable path when that check fails.
REMOTE_BASE=$(remote147 'pwd -P')
[[ "$REMOTE_BASE" == /*/yyt && "$REMOTE_BASE" =~ ^/[a-zA-Z0-9_./-]+$ ]] || {
    echo 'Remote login directory is not a confirmed yyt directory; refusing writes.' >&2
    exit 1
}
echo "Deploying isolated source to $TARGET:$REMOTE_BASE/$REMOTE_REL"
# All archive members come from the committed source snapshot and lab scripts.
# No SSH keys, credentials, original .git directory, or other project trees.
tar -C "$ROOT" -czf - .vacu-lab-root source.json lab.sh upstream-functions.sh README.md work |
    remote147 "set -eu; cd '$REMOTE_BASE'; umask 077; test ! -e '$REMOTE_REL'; mkdir '$REMOTE_REL'; tar -xzf - -C '$REMOTE_REL'; mkdir -p '$REMOTE_REL/tmp' '$REMOTE_REL/logs'"

echo 'Build and initialize an isolated nine-node lab for existing crash/replay suites'
remote147 "set -eu; cd '$REMOTE_BASE/$REMOTE_REL'; N_WORKERS=8 LAB_MEMORY=3g bash lab.sh up; bash lab.sh verify; bash lab.sh test; bash lab.sh regress"
echo "Remote results: $TARGET:$REMOTE_BASE/$REMOTE_REL/logs/lab.log"
echo "Source-side transcript: $LOG"
