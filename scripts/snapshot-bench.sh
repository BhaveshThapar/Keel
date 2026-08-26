#!/usr/bin/env bash
#
# Snapshot creation stall and real-process transfer throughput (PR-6).
#
# Usage: scripts/snapshot-bench.sh [logical-state-bytes] [value-bytes] [runs]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

STATE_BYTES="${1:-1073741824}"
VALUE_BYTES="${2:-1048576}"
RUNS="${3:-3}"
TIER="${KEEL_BENCH_TIER:-exploratory}"

echo "building" >&2
cargo build --release -p keel-bench -p keel-server >&2 || exit 1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-snapshot-bench-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

"$(pwd)/target/release/keel-bench" snapshot \
    --state-bytes "$STATE_BYTES" \
    --value-bytes "$VALUE_BYTES" \
    --depth 32 \
    --runs "$RUNS" \
    --dir "$WORK" \
    --server-bin "$(pwd)/target/release/keel-server" \
    --sync durable \
    --tier "$TIER" \
    --out snapshot.txt
