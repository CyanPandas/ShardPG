#!/usr/bin/env bash
# Materialize a committed source snapshot alongside the reproducibility scripts.
set -euo pipefail
ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
REPO=$(git -C "$ROOT" rev-parse --show-toplevel)
git -C "$REPO" cat-file -e HEAD:pg-partdist-src/Makefile
[[ ! -e "$ROOT/work" ]] || { echo 'work already exists; use its current snapshot or choose another experiment directory' >&2; exit 1; }
mkdir "$ROOT/work"
git -C "$REPO" archive HEAD pg-partdist-src pg-raft-src pg-install | tar -xf - -C "$ROOT/work"
echo "Prepared $(git -C "$REPO" rev-parse --short HEAD) in $ROOT/work"
