#!/usr/bin/env bash
#
# Does the simulator actually catch anything?
#
# A verification harness that has only ever reported success has not been shown
# to work. This removes one safety rule — the Figure 8 requirement that a leader
# commit only current-term entries by counting replicas — and checks that the
# simulator finds the resulting violation.
#
# Both halves matter. The control run says the fault schedule is survivable; the
# experimental run says the rule is what makes it survivable. Without the
# control, a failure would only prove the schedule is too harsh.
#
# The faults are aimed rather than random. The window the rule guards is one
# message round wide: a leader commits an earlier term's entry, then dies before
# its own term's entry commits. Uniform random crashes reach that window only by
# luck, so the fig8-hunt profile strikes the leader the moment it enters it.
#
# Usage: scripts/negative-demos/figure-8.sh [seeds] [steps]

set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

SEEDS="${1:-40}"
STEPS="${2:-80000}"
NODES=3
PROFILE=fig8-hunt

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/../lib/provenance.sh"
OUT="results/negative-demos/$(basename "$0" .sh).txt"
provenance_of "$OUT"

echo "=============================================================="
echo "Figure 8: is the current-term commit rule load-bearing?"
echo "  profile $PROFILE, $NODES nodes, $SEEDS seeds x $STEPS steps"
echo "=============================================================="
provenance_header
echo

echo "--- CONTROL: the rule in place. Expect every seed to pass."
echo
cargo run --quiet --release -p keel-sim -- \
    run --from 0 --count "$SEEDS" --steps "$STEPS" --nodes "$NODES" --profile "$PROFILE"
control=$?
echo

echo "--- EXPERIMENT: the rule compiled out. Expect seeds to fail."
echo
cargo run --quiet --release -p keel-sim --features negative-demos -- \
    run --from 0 --count "$SEEDS" --steps "$STEPS" --nodes "$NODES" --profile "$PROFILE" \
    --disable-fig8-guard
experiment=$?
echo

echo "=============================================================="
if [ $control -eq 0 ] && [ $experiment -ne 0 ]; then
    echo "PASS: the schedule is survivable with the rule and not without it."
    echo "      The simulator detects the violation the rule prevents."
    exit 0
fi
if [ $control -ne 0 ]; then
    echo "FAIL: the control run found a violation. Either the implementation has"
    echo "      a real bug or the fault schedule is not survivable at all."
    exit 1
fi
echo "FAIL: removing the rule produced no violation. The harness cannot be"
echo "      shown to catch this class of bug, so a clean run means nothing here."
exit 1
