#!/usr/bin/env bash
set -euo pipefail
ROOT=$(pwd -P)
[[ "$ROOT" == /home/yyt/vacu32-build-* ]] || { echo 'Unexpected build root' >&2; exit 1; }
CONTAINER=${1:-yyt-vacu-container}
[[ "$CONTAINER" == yyt-vacu-container ]] || exit 1
owned=$(docker inspect --format '{{index .Config.Labels "yyt.vacu.root"}}' "$CONTAINER")
[[ "$owned" == /home/yyt/vacu-bb62a57-20261029T074050Z ]] || exit 1
[[ "$(docker inspect --format '{{.State.Running}}' "$CONTAINER")" == true ]] || exit 1
[[ "$(docker inspect --format '{{.Image}}' "$CONTAINER")" == sha256:46cc50ca101783d3f3f27babe9d3f99a8caa908c42a0c94fd3fc753fb99e4d29 ]] || exit 1
SCRATCH="/tmp/${ROOT##*/}"
mkdir -p "$ROOT/artifacts"
echo "Use existing container $CONTAINER, PostgreSQL /work/pg-install"
echo "Copy extension sources to $SCRATCH/work"
docker exec --user 0 "$CONTAINER" bash -ceu 'test ! -e "$1"; mkdir -p "$1/work" "$1/artifacts"' bash "$SCRATCH"
docker cp "$ROOT/work/." "$CONTAINER:$SCRATCH/work/"
# Only scratch sources and outputs are written. Do not install or restart servers.
docker exec -i --user 0 "$CONTAINER" bash -ceu '
  scratch=$1
  cd "$scratch/work/pg-partdist-src"
  echo "Compile/test 32-bit circular arithmetic"
  gcc -std=c99 -Wall -Wextra -Werror -Iinclude tests/local/test_shard_xid_math.c -o "$scratch/xid-math32"
  "$scratch/xid-math32"
  echo "Compile/test accelerated 8-bit circle (uint32_t identifiers)"
  gcc -std=c99 -Wall -Wextra -Werror -DSHARD_XID_TEST_BITS=8 -Iinclude tests/local/test_shard_xid_math.c -o "$scratch/xid-math8"
  "$scratch/xid-math8"
  echo "Compile production 32-bit extension"
  make clean PG_CONFIG=/work/pg-install/bin/pg_config
  make -j2 PG_CONFIG=/work/pg-install/bin/pg_config PG_CFLAGS=-Werror=implicit-function-declaration
  cp pg_partdist.so "$scratch/artifacts/pg_partdist-32.so"
  echo "Compile accelerated circular test extension"
  make clean PG_CONFIG=/work/pg-install/bin/pg_config
  make -j2 PG_CONFIG=/work/pg-install/bin/pg_config SHARD_XID_RING_BITS=8 PG_CFLAGS=-Werror=implicit-function-declaration
  cp pg_partdist.so "$scratch/artifacts/pg_partdist-ring8.so"
  echo "Compile pg_raft"
  cd "$scratch/work/pg-raft-src"
  make clean PG_CONFIG=/work/pg-install/bin/pg_config
  make -j2 PG_CONFIG=/work/pg-install/bin/pg_config PG_CFLAGS=-Werror=implicit-function-declaration
  cp pg_raft.so "$scratch/artifacts/pg_raft.so"
' bash "$SCRATCH"
docker cp "$CONTAINER:$SCRATCH/artifacts/." "$ROOT/artifacts/"
echo 'BUILD PASS: production 32-bit extension, accelerated circular test extension, pg_raft.'
echo "Artifacts: $ROOT/artifacts (not installed; database tests still pending)."
