#!/usr/bin/env bash
#
# The validation sweep of record. Writes results/simulator/sweep.txt.
#
# Safety only. The simulator runs on a virtual clock, so how long this takes in
# wall-clock time says nothing about the system under test and no timing claim
# is made from it.
#
# Both cluster sizes are swept deliberately. Commit needs the k-th highest match
# index where k is the quorum size, so a three-node cluster reaches partial
# replication states that a five-node cluster reaches far more rarely — and one
# of those states is the window the Figure 8 rule guards.
#
# Usage: scripts/sweep.sh [seeds] [steps] [disk-seeds]

set -euo pipefail
cd "$(dirname "$0")/.."

SEEDS="${1:-500}"
STEPS="${2:-60000}"
DISK_SEEDS="${3:-100}"
OUT=results/simulator/sweep.txt
mkdir -p "$(dirname "$OUT")"

cargo build --quiet --release -p keel-sim

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
provenance_of "$OUT"

{
    echo "=== keel-sim validation sweep ==="
    provenance_header
    echo
    echo "Safety only. No timing claim is made here: the simulator runs on a"
    echo "virtual clock, so wall-clock speed says nothing about the system"
    echo "under test."
    echo

    for profile in default chaos; do
        for nodes in 3 5; do
            echo "--- profile=$profile nodes=$nodes"
            ./target/release/keel-sim run \
                --from 0 --count "$SEEDS" --steps "$STEPS" \
                --nodes "$nodes" --profile "$profile" | tail -2
        done
    done

    # Reads, a wandering clock, and a nemesis weighted towards healing so the
    # cluster commits enough for a stale read to be possible at all. The read
    # oracles are the only ones here that check what a *client* observes rather
    # than what the nodes agree about.
    for nodes in 3 5; do
        echo "--- profile=read-hunt nodes=$nodes"
        ./target/release/keel-sim run \
            --from 0 --count "$SEEDS" --steps "$STEPS" \
            --nodes "$nodes" --profile read-hunt | tail -2
    done

    # A calm cluster whose leader has the slowest clock in it. Clean here
    # because reads are confirmed by a heartbeat round; the lease arm, which is
    # not clean, is scripts/negative-demos/lease-drift.sh.
    for nodes in 3 5; do
        echo "--- profile=lease-drift nodes=$nodes"
        ./target/release/keel-sim run \
            --from 0 --count "$SEEDS" --steps "$STEPS" \
            --nodes "$nodes" --profile lease-drift | tail -2
    done

    # Membership changes and leader transfers under faults. Two of five nodes
    # start as learners so there is somewhere to change to; a simulated cluster
    # cannot start a process that was not in the seed.
    for nodes in 3 5; do
        echo "--- profile=membership-hunt nodes=$nodes"
        ./target/release/keel-sim run \
            --from 0 --count "$SEEDS" --steps "$STEPS" \
            --nodes "$nodes" --profile membership-hunt | tail -2
    done

    echo "--- profile=fig8-hunt nodes=3"
    ./target/release/keel-sim run \
        --from 0 --count "$SEEDS" --steps $(( STEPS + 20000 )) \
        --nodes 3 --profile fig8-hunt | tail -2

    # The disk profiles cost more per event, because every record is really
    # encoded, checksummed and parsed rather than modelled — so they sweep
    # fewer seeds. Both sector sizes run: 4096 is what modern hardware is, and
    # 512 is where a write of a few hundred bytes straddles a boundary often
    # enough to tear.
    echo
    for profile in disk-chaos disk-hunt; do
        for nodes in 3 5; do
            echo "--- profile=$profile nodes=$nodes"
            ./target/release/keel-sim run \
                --from 0 --count "$DISK_SEEDS" --steps "$STEPS" \
                --nodes "$nodes" --profile "$profile" | tail -2
        done
    done

    echo
    echo "--- determinism: 100 seeds, each run twice"
    ./target/release/keel-sim determinism --from 0 --count 100 --steps 30000 | tail -2
    echo "--- determinism with the disk in the fingerprint: 60 seeds, disk-hunt"
    ./target/release/keel-sim determinism --from 0 --count 60 --steps 30000 \
        --nodes 3 --profile disk-hunt | tail -2
} | tee "$OUT"
