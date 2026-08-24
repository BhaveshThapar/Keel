#!/usr/bin/env bash
#
# A real cluster, broken on purpose.
#
# The simulator finds consensus bugs; this finds the bugs the simulator cannot
# have, because it replaces the parts they live in. A real process has a
# scheduler, a TCP stack, a page cache, and a SIGSTOP that lands between two
# instructions.
#
# Each seed stands up three keel-server processes wired through a proxy mesh —
# one proxy per ordered pair, so a partition can be one-directional and between
# exactly two nodes — runs a client workload against them, and injects the fault
# schedule that seed draws. The run then asks the only question a chaos run can
# answer afterwards: is every acknowledged write still there.
#
# The clock nemesis is not here. macOS strips DYLD_INSERT_LIBRARIES under System
# Integrity Protection and does not interpose the commpage mach_absolute_time
# reads, so libfaketime cannot move CLOCK_MONOTONIC on this host at all. It runs
# in a Linux container instead: scripts/chaos-clock.sh. A schedule drawn here
# therefore contains no clock jumps, and says so rather than quietly omitting
# them.
#
# Usage: scripts/chaos.sh [seconds-per-seed] [seed...]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

SECS="${1:-45}"
shift || true
SEEDS=("$@")
if [ ${#SEEDS[@]} -eq 0 ]; then
    SEEDS=(1 2 3 4)
fi

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
OUT=results/chaos/real-cluster.txt
mkdir -p "$(dirname "$OUT")"
provenance_of "$OUT" || exit 1

echo "building the binaries the run needs" >&2
cargo build --release -p keel-chaos -p keel-server -p keel-client >&2 || exit 1

CHAOS=target/release/keel-chaos
SERVER=target/release/keel-server
KV=target/release/kv

WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-chaos-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

# The block below runs in a subshell because of the pipe into tee, so a counter
# incremented inside it never reaches this scope. The count goes through a file.
TALLY="$WORK/failures"
echo 0 >"$TALLY"

failures=0
{
    echo "keel-chaos — a real cluster under a seeded fault schedule"
    provenance_header
    echo
    # Stated rather than assumed. `durable` is the only mode a durability claim
    # may be made under, and F_FULLFSYNC on a laptop would make each run a
    # measurement of the laptop rather than of the cluster — so this is a
    # correctness run, and the sync mode it used is part of the result.
    echo "sync mode:  durable"
    echo "nodes:      3"
    echo "seconds:    $SECS per seed"
    echo "seeds:      ${SEEDS[*]}"
    echo

    for seed in "${SEEDS[@]}"; do
        echo "=============================================================="
        echo "seed $seed"
        echo "=============================================================="
        rm -rf "${WORK:?}/$seed"
        mkdir -p "$WORK/$seed"
        "$CHAOS" run \
            --seed "$seed" \
            --nodes 3 \
            --secs "$SECS" \
            --dir "$WORK/$seed" \
            --server-bin "$SERVER" \
            --kv-bin "$KV" \
            --sync durable 2>&1
        status=$?
        if [ $status -ne 0 ]; then
            failures=$((failures + 1))
            echo "$failures" >"$TALLY"
            echo "seed $seed FAILED (exit $status)"
        fi
        echo
    done

    echo "=============================================================="
    if [ $failures -eq 0 ]; then
        echo "PASS ${#SEEDS[@]} seeds, no acknowledged write lost"
    else
        echo "FAIL $failures of ${#SEEDS[@]} seeds"
    fi
} 2>&1 | tee "$OUT"

exit "$(cat "$TALLY")"
