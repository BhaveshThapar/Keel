#!/usr/bin/env bash
#
# Is writing the applied index in the same batch as the data load-bearing?
#
# ADR-010 says a state machine must make its applied index durable in the same
# atomic write as the data that index describes. This removes that rule — the
# data first, then the index, as two writes, both still fsynced — and requires
# the kill loop to catch the resulting double-apply.
#
# Both halves matter. The control says the kill schedule is survivable with the
# rule in place; the experiment says the rule is what makes it survivable.
# Without the control, a failure would only show the schedule was too harsh.
#
# The removed version is not a durability bug. Both writes are still durable
# individually. What is gone is only the atomicity between them, which is what
# makes this a fair test of exactly the thing the ADR claims.
#
# Usage: scripts/negative-demos/split-batch.sh [cycles]

set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

CYCLES="${1:-1000}"
CONTROL=a_kill_mid_apply_never_double_applies_or_regresses
EXPERIMENT=without_the_atomic_index_a_kill_leaves_an_entry_that_will_apply_twice

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/../lib/provenance.sh"
OUT="results/negative-demos/$(basename "$0" .sh).txt"
provenance_of "$OUT"

echo "=============================================================="
echo "Split batch: is the atomic applied index load-bearing?"
echo "  $CYCLES kill/restart cycles per arm"
echo "=============================================================="
provenance_header
echo

echo "--- CONTROL: the index written with the data. Expect every cycle to pass."
echo
KEEL_SM_KILL_CYCLES="$CYCLES" \
    cargo test --release -q -p keel-sm --test kill_during_apply -- --exact "$CONTROL" --nocapture
control=$?
echo

# The experiment's own budget is a hundred cycles, which is the exit criterion's
# number, and it is enforced inside the test rather than here.
echo "--- EXPERIMENT: the index written after the data, as two writes."
echo "    Expect the loop to find the double-apply inside 100 cycles."
echo
cargo test --release -q -p keel-sm --features negative-demos \
    --test kill_during_apply -- --exact "$EXPERIMENT" --nocapture
experiment=$?
echo

echo "=============================================================="
# Both arms report success when they observe what they are supposed to observe:
# the control by surviving, the experiment by catching the break. So both exit
# zero, and either exiting non-zero is the failure.
if [ $control -eq 0 ] && [ $experiment -eq 0 ]; then
    echo "PASS: $CYCLES kill cycles clean with the rule; the rule removed is caught"
    echo "      inside a hundred. The atomic applied index is load-bearing."
    exit 0
fi
if [ $control -ne 0 ]; then
    echo "FAIL: the control run found a violation. Either there is a real bug or"
    echo "      the kill schedule is not survivable at all."
    exit 1
fi
echo "FAIL: the loop did not catch a build that writes the applied index separately"
echo "      from its data. It cannot be shown to detect this class of bug, so a"
echo "      clean control run means nothing here."
exit 1
