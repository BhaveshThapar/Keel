#!/usr/bin/env bash
#
# How long the cluster takes to serve a write again after its leader dies.
#
# PR-5 asks for at least a hundred trials, and the count is the requirement
# rather than a suggestion: failover time is dominated by a *randomised* election
# timeout — that is what stops two candidates splitting the vote forever — so ten
# trials give a median that moves by tens of milliseconds between runs.
#
# The clock starts at the kill and stops at an acknowledged write, not at an
# election. Election is an internal event a client cannot observe, and it is
# strictly earlier: the new leader must also commit its own term's no-op before
# it can serve.
#
# Usage: scripts/failover.sh [trials] [tick-ms]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

TRIALS="${1:-100}"
TICK_MS="${2:-30}"

echo "building" >&2
cargo build --release -p keel-bench -p keel-server >&2 || exit 1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-failover-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

"$(pwd)/target/release/keel-bench" failover \
    --trials "$TRIALS" \
    --tick-ms "$TICK_MS" \
    --dir "$WORK" \
    --server-bin "$(pwd)/target/release/keel-server" \
    --sync durable \
    --out failover.txt
