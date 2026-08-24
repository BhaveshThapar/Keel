#!/usr/bin/env bash
#
# Is applying in index order load-bearing, and does the model oracle see it?
#
# A `Ready` is written when it is pumped and made durable when its fsync fires
# (ADR-016), and fsyncs have independent latencies — so a later batch can
# complete first and committed entries arrive out of index order. The host
# reassembles the order before applying. This removes that, and requires the
# oracles to find the resulting divergence.
#
# What makes this worth a demonstration rather than a test is that the failure
# is invisible to everything else. Watermarks are maxima and do not notice. The
# log digests agree, because the nodes really did apply the same entries. Only a
# check on what applying them *produced* can tell, which is what P8 added and
# what this is here to keep honest.
#
# Both halves matter, as always: the control says the schedule is survivable
# with the ordering, the experiment says the ordering is what makes it
# survivable.
#
# Usage: scripts/negative-demos/apply-ordering.sh [seeds] [steps]

set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

SEEDS="${1:-25}"
STEPS="${2:-30000}"
NODES=3
PROFILE=default

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/../lib/provenance.sh"
OUT="results/negative-demos/$(basename "$0" .sh).txt"
provenance_of "$OUT"

echo "=============================================================="
echo "Apply ordering: is the model oracle watching what applying produced?"
echo "  profile $PROFILE, $NODES nodes, $SEEDS seeds x $STEPS steps"
echo "=============================================================="
provenance_header
echo

echo "--- CONTROL: entries applied in index order. Expect every seed to pass."
echo
cargo run --quiet --release -p keel-sim -- \
    run --from 0 --count "$SEEDS" --steps "$STEPS" --nodes "$NODES" --profile "$PROFILE"
control=$?
echo

echo "--- EXPERIMENT: entries applied in fsync-completion order."
echo "    Expect seeds to fail, and the report to name the model."
echo
cargo run --quiet --release -p keel-sim --features negative-demos -- \
    run --from 0 --count "$SEEDS" --steps "$STEPS" --nodes "$NODES" --profile "$PROFILE" \
    --skip-apply-ordering
experiment=$?
echo

echo "=============================================================="
if [ $control -eq 0 ] && [ $experiment -ne 0 ]; then
    echo "PASS: the schedule is survivable in index order and not out of it."
    echo "      The oracles detect a divergence that every watermark and every"
    echo "      log digest agrees is fine."
    exit 0
fi
if [ $control -ne 0 ]; then
    echo "FAIL: the control run found a violation. Either there is a real bug or"
    echo "      the fault schedule is not survivable at all."
    exit 1
fi
echo "FAIL: applying out of order produced no violation. The oracles cannot be"
echo "      shown to catch this class of bug, so a clean run means nothing here."
exit 1
