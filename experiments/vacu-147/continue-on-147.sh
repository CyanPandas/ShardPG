#!/usr/bin/env bash
# Run on the source machine using the historical 2222 -> 145 -> 147 route.
# Confirm the remote yyt directory before creating an isolated child directory.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
MODE=${1:---resume}
[[ "$MODE" == --resume || "$MODE" == --diagnose || "$MODE" == --trace-ddl || "$MODE" == --remaining || "$MODE" == --replay || "$MODE" == --health ]] || { echo 'Usage: continue-on-147.sh [--resume|--diagnose|--trace-ddl|--remaining|--replay|--health]' >&2; exit 2; }
TARGET=10.77.110.147
SSH=(ssh -F "$ROOT/ssh-jump-145.conf" -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes
     -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no)
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
REMOTE_REL="vacu-bb62a57-$STAMP"
LOG="$ROOT/logs/remote-147-resume-$STAMP.log"
mkdir -p "$ROOT/logs"
exec > >(tee -a "$LOG") 2>&1

remote147() {
    local quoted
    printf -v quoted '%q' "$1"
    "${SSH[@]}" lab145-2222 \
        "ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=yes -o UpdateHostKeys=no -o ControlMaster=no -o ControlPath=none -o ForwardAgent=no $TARGET $quoted"
}

LAB=/home/yyt/vacu-bb62a57-20261029T074050Z
IMAGE=46cc50ca1017

echo "Resume the isolated VACU experiment at $TARGET:$LAB using existing image $IMAGE"
REMOTE_CHECK=$(cat <<'CHECK'
set -eu
LAB=/home/yyt/vacu-bb62a57-20261029T074050Z
test "$(cd "$LAB"; pwd -P)" = "$LAB"
test -f "$LAB/.vacu-lab-root"
docker image inspect 46cc50ca1017 --format '{{.Id}}'
python3 --version
CHECK
)
remote147 "$REMOTE_CHECK"

# Update only small experiment scripts inside the confirmed lab directory.
remote147 "set -eu; cd '$LAB'; cat > lab.sh.next; bash -n lab.sh.next; mv lab.sh.next lab.sh" < "$ROOT/lab.sh"
remote147 "set -eu; cd '$LAB'; cat > upstream-functions.sh.next; bash -n upstream-functions.sh.next; mv upstream-functions.sh.next upstream-functions.sh" < "$ROOT/upstream-functions.sh"
remote147 "set -eu; cd '$LAB'; cat > work/pg-partdist-src/tests/local/test_vacu_contract.py.next; python3 -c 'import ast, pathlib; ast.parse(pathlib.Path(\"work/pg-partdist-src/tests/local/test_vacu_contract.py.next\").read_text())'; mv work/pg-partdist-src/tests/local/test_vacu_contract.py.next work/pg-partdist-src/tests/local/test_vacu_contract.py" < "$ROOT/work/pg-partdist-src/tests/local/test_vacu_contract.py"
remote147 "set -eu; cd '$LAB'; cat > work/pg-partdist-src/tests/test_shard_vacuum_replay_p5.sh.next; bash -n work/pg-partdist-src/tests/test_shard_vacuum_replay_p5.sh.next; mv work/pg-partdist-src/tests/test_shard_vacuum_replay_p5.sh.next work/pg-partdist-src/tests/test_shard_vacuum_replay_p5.sh" < "$ROOT/work/pg-partdist-src/tests/test_shard_vacuum_replay_p5.sh"
remote147 "set -eu; cd '$LAB'; cat > work/pg-partdist-src/tests/test_shard_vacuum_auto_p7.sh.next; bash -n work/pg-partdist-src/tests/test_shard_vacuum_auto_p7.sh.next; mv work/pg-partdist-src/tests/test_shard_vacuum_auto_p7.sh.next work/pg-partdist-src/tests/test_shard_vacuum_auto_p7.sh" < "$ROOT/work/pg-partdist-src/tests/test_shard_vacuum_auto_p7.sh"
remote147 "set -eu; cd '$LAB'; cat > trace-ddl.sh.next; bash -n trace-ddl.sh.next; mv trace-ddl.sh.next trace-ddl.sh" < "$ROOT/trace-ddl.sh"
remote147 "set -eu; cd '$LAB'; cat > work/pg-partdist-src/tests/lib_node_health.sh.next; bash -n work/pg-partdist-src/tests/lib_node_health.sh.next; mv work/pg-partdist-src/tests/lib_node_health.sh.next work/pg-partdist-src/tests/lib_node_health.sh" < "$ROOT/work/pg-partdist-src/tests/lib_node_health.sh"
remote147 "set -eu; cd '$LAB'; cat > health-audit.sh.next; bash -n health-audit.sh.next; mv health-audit.sh.next health-audit.sh" < "$ROOT/health-audit.sh"
remote147 "set -eu; cd '$LAB'; cat > TEST_REPORT_147.md.next; mv TEST_REPORT_147.md.next TEST_REPORT_147.md" < "$ROOT/TEST_REPORT_147.md"
remote147 "set -eu; cd '$LAB'; cat > work/pg-partdist-src/docs/VACU_DESIGN.md.next; mv work/pg-partdist-src/docs/VACU_DESIGN.md.next work/pg-partdist-src/docs/VACU_DESIGN.md" < "$ROOT/work/pg-partdist-src/docs/VACU_DESIGN.md"

if [[ "$MODE" == --health ]]; then
    remote147 "cd '$LAB'; bash health-audit.sh"
    echo "Source transcript: $LOG"
    exit 0
fi

if [[ "$MODE" == --diagnose || "$MODE" == --trace-ddl || "$MODE" == --remaining || "$MODE" == --replay ]]; then
    TRACE_STATUS=0
    if [[ "$MODE" == --trace-ddl ]]; then
        remote147 "cd '$LAB'; bash trace-ddl.sh" || TRACE_STATUS=$?
    elif [[ "$MODE" == --remaining ]]; then
        remote147 "cd '$LAB'; bash trace-ddl.sh --remaining" || TRACE_STATUS=$?
    elif [[ "$MODE" == --replay ]]; then
        remote147 "cd '$LAB'; bash trace-ddl.sh --replay" || TRACE_STATUS=$?
    fi
    STATE_PATH="$ROOT/logs/remote-147-state-$STAMP.log"
    remote147 "cd '$LAB'; bash lab.sh diagnose" > "$STATE_PATH" 2>&1
    echo "Detailed node diagnostics saved: $STATE_PATH"
    REMOTE_ARCHIVE=$(cat <<'ARCHIVE'
python3 - <<'PY'
import io
from pathlib import Path
import sys
import tarfile
root = Path('/home/yyt/vacu-bb62a57-20261029T074050Z/work/pg-cluster-data')
with tarfile.open(fileobj=sys.stdout.buffer, mode='w|gz') as archive:
    paths = [(path, str(path.relative_to(root))) for path in root.glob('**/*.log')]
    paths += [(path, 'suite-diagnostics/' + path.name)
              for path in (root.parent.parent / 'logs').glob('suite-failure-*.log')]
    for path, name in sorted(paths):
        if path.is_symlink() or not path.is_file():
            continue
        # Bound collection, retaining the latest crash context in each log.
        with path.open('rb') as source:
            size = path.stat().st_size
            source.seek(max(0, size - 16 * 1024 * 1024))
            data = source.read(16 * 1024 * 1024)
        item = tarfile.TarInfo(name)
        item.size = len(data)
        archive.addfile(item, io.BytesIO(data))
PY
ARCHIVE
)
    ARCHIVE_PATH="$ROOT/logs/remote-147-diagnostics-$STAMP.tar.gz"
    remote147 "$REMOTE_ARCHIVE" > "$ARCHIVE_PATH"
    echo "Diagnostics saved: $ARCHIVE_PATH"
    echo "Source transcript: $LOG"
    exit "$TRACE_STATUS"
fi

REMOTE_RUN=$(cat <<'REMOTE'
set -eu
LAB=/home/yyt/vacu-bb62a57-20261029T074050Z
cd "$LAB"
if docker container inspect yyt-vacu-container >/dev/null 2>&1; then
    label=$(docker inspect --format '{{index .Config.Labels "yyt.vacu.root"}}' yyt-vacu-container)
    test "$label" = "$LAB"
    if test "$(docker inspect --format '{{.State.Running}}' yyt-vacu-container)" != true; then
        docker start yyt-vacu-container
    fi
    if test ! -d work/pg-cluster-data; then
        bash lab.sh prepare-user
        bash lab.sh build
        bash lab.sh initialize
    fi
else
    IMAGE=46cc50ca1017 N_WORKERS=8 LAB_MEMORY=3g bash lab.sh up
fi
bash lab.sh configure-tso
if ! bash lab.sh verify; then
    bash lab.sh diagnose || true
    exit 1
fi
bash lab.sh test
bash lab.sh regress
REMOTE
)
remote147 "$REMOTE_RUN"
echo "Remote log: $LAB/logs/lab.log"
echo "Source transcript: $LOG"
