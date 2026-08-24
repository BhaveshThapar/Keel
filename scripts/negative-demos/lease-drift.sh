#!/usr/bin/env bash
#
# A lease read is only as good as the clock assumption it rests on.
#
# Every other demonstration here removes a safety rule and shows the harness
# catching the result. This one removes nothing. Both arms are the same stock
# build; what differs is a *shipped option* and the deployment it assumes.
#
#   control      ReadIndex. Every read confirmed by a heartbeat round before it
#                is served. Correct under any clock behaviour whatsoever.
#   experiment   LeaseBased with a drift bound of zero — the most optimistic
#                setting there is, and the one that buys the longest lease —
#                on a cluster whose leader's clock runs half a period slower
#                than its followers'.
#
# The leader counts its lease in its own ticks and the followers count their
# election timeout in theirs. When the leader's ticks are the slow ones, a
# follower can time out, win an election and commit while the old leader still
# believes its lease holds — and a read it answers locally in that window
# returns a value the cluster has already moved past.
#
# A pass means the control is clean and the experiment is dirty on the same
# seeds. That is the ADR-005 assumption stated as an experiment rather than as a
# paragraph: with the assumption, this is a real risk; the default is ReadIndex
# for exactly this reason.
#
# The profile is aimed, and it has to be. Under `chaos` a leader's own term
# no-op almost never commits — measured, 35 of 3,633 reads were confirmed at
# all — so the lease path was barely reached and a run over it said nothing
# about leases either way. `lease-drift` is calm enough for a lease to exist,
# gives node 1 the slowest clock, and aims every isolation at the leader.
#
# Usage: scripts/negative-demos/lease-drift.sh [seeds] [steps]

set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

SEEDS="${1:-25}"
STEPS="${2:-40000}"
NODES=3

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/../lib/provenance.sh"
OUT="results/negative-demos/$(basename "$0" .sh).txt"
provenance_of "$OUT"

echo "=============================================================="
echo "Lease reads: what the clock assumption is actually holding up"
echo "  one stock build, two read modes"
echo "  $NODES nodes, $SEEDS seeds x $STEPS steps, profile lease-drift"
echo "=============================================================="
provenance_header
echo

echo "--- CONTROL: ReadIndex. Every read confirmed by a heartbeat round."
echo "    Expect every seed to pass, whatever the clocks do."
echo
cargo run --quiet --release -p keel-sim -- \
    run --from 0 --count "$SEEDS" --steps "$STEPS" --nodes "$NODES" \
    --profile lease-drift
control=$?
echo

echo "--- EXPERIMENT: the same seeds, served from a lease that assumes the"
echo "    clocks do not drift at all. Expect seeds to fail with a stale read."
echo
cargo run --quiet --release -p keel-sim -- \
    run --from 0 --count "$SEEDS" --steps "$STEPS" --nodes "$NODES" \
    --profile lease-drift --lease-reads 0
experiment=$?
echo

echo "--- coverage: the lease path was actually taken"
echo
cargo run --quiet --release -p keel-sim -- \
    repro --seed 0 --steps "$STEPS" --nodes "$NODES" \
    --profile lease-drift --lease-reads 0 |
    grep -E "reads issued|lease reads served" || true
echo

echo "=============================================================="
if [ $control -eq 0 ] && [ $experiment -ne 0 ]; then
    echo "PASS: confirming each read with a heartbeat round is clean on every"
    echo "      seed; serving the same reads from a lease whose clock"
    echo "      assumption the deployment violates returns values the cluster"
    echo "      had already moved past. ReadIndex is the default for this"
    echo "      reason, and the lease bound is a claim about the deployment"
    echo "      rather than about the algorithm."
    exit 0
fi
if [ $control -ne 0 ]; then
    echo "FAIL: the control arm failed, so the experiment is measuring the"
    echo "      profile rather than the lease. Something in lease-drift is"
    echo "      broken independently of how reads are served."
    exit 1
fi
echo "FAIL: serving reads from a lease found nothing. Either the window stopped"
echo "      being reachable — check the coverage lines above, and whether the"
echo "      leader's own no-op is committing at all — or the read oracles"
echo "      stopped looking."
exit 1
