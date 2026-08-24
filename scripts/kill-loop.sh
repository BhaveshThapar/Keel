#!/usr/bin/env bash
#
# A thousand kills against a real cluster, and the one question worth asking
# afterwards.
#
# P5 already killed a node a thousand times mid-apply, but that was one node and
# one state machine: the question there was whether `applied_index` and the data
# it describes can disagree. This is a *cluster*. One node is killed and
# restarted while clients keep writing to the other two, round robin, so every
# node takes its turn being the one that dies — including, often enough, the one
# that had just become leader.
#
# The restart is immediate rather than waiting for the cluster to settle. A loop
# that let everything catch up between kills would be testing a healthy cluster a
# thousand times over; killing the next node while the last one is still
# replaying its log is where a node's log and its state machine can disagree
# about what has been applied.
#
# The property is one-sided, and it has to be. An unacknowledged write may or may
# not have applied — that is what a timeout means, not a bug — so the final
# counter may exceed the acknowledgements and may never fall short of them.
#
# Usage: scripts/kill-loop.sh [cycles] [sync-mode] [settle-ms]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

CYCLES="${1:-1000}"
SYNC="${2:-durable}"
SETTLE_MS="${3:-250}"

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
OUT=results/chaos/kill-loop.txt
mkdir -p "$(dirname "$OUT")"
provenance_of "$OUT" || exit 1

echo "building" >&2
cargo build --release -p keel-chaos -p keel-server -p keel-client >&2 || exit 1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-kill-loop-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
TALLY="$WORK/failures"
echo 0 >"$TALLY"

{
    echo "keel-chaos — $CYCLES kill cycles against a three-node cluster"
    provenance_header
    echo
    # durable, and it matters here in a way it does not in the partition runs: a
    # kill is the fault the durability argument is actually about. Under `none`
    # a node that is killed loses whatever the kernel had not written, and the
    # run would be measuring the page cache.
    echo "sync mode:  $SYNC"
    echo "nodes:      3"
    echo "cycles:     $CYCLES, round robin"
    # The settle window is part of the result and not a knob to hide. With none
    # at all the cluster spends the run in back-to-back elections, commits
    # almost nothing, and the assertion ends up quantified over a few dozen
    # writes.
    echo "settle:     ${SETTLE_MS}ms between a restart and the next kill"
    echo "writers:    4 concurrent"
    echo

    "$(pwd)/target/release/keel-chaos" kill-loop \
        --cycles "$CYCLES" \
        --settle-ms "$SETTLE_MS" \
        --nodes 3 \
        --dir "$WORK/cluster" \
        --server-bin "$(pwd)/target/release/keel-server" \
        --kv-bin "$(pwd)/target/release/kv" \
        --sync "$SYNC" 2>&1
    status=$?

    echo
    echo "=============================================================="
    if [ $status -ne 0 ]; then
        echo "FAIL the kill loop exited $status"
        echo 1 >"$TALLY"
    fi
} 2>&1 | tee "$OUT"

exit "$(cat "$TALLY")"
