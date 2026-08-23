#!/usr/bin/env bash
#
# Is the record checksum load-bearing?
#
# A verification harness that has only ever reported success has not been shown
# to work. This removes one durability rule — that a record whose CRC does not
# match is not a record — and checks that the simulator finds the resulting
# violation.
#
# Both halves matter. The control run says the fault schedule is survivable; the
# experimental run says the rule is what makes it survivable. Without the
# control, a failure would only prove the schedule is too harsh.
#
# The length check and the plausibility check stay. What goes is the comparison
# that decides whether the bytes are the ones that were written. A crash that
# tears a record leaves a valid length over a body that is part old and part
# zeros, and without the checksum that decodes into a plausible but wrong
# record rather than being rejected — a hard state that has forgotten its vote,
# or entries that never had those contents. Two nodes then disagree about a
# prefix they both believe they wrote.
#
# The faults are aimed rather than random, for the reason ADR-007 already gives.
# Measured, the window between a write and the fsync covering it is open about
# seven per cent of the time, so a uniform nemesis reaches a torn write once a
# run and often not at all. The disk-hunt profile aims at it.
#
# Usage: scripts/negative-demos/torn-record.sh [seeds] [steps]

set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

SEEDS="${1:-25}"
STEPS="${2:-40000}"
NODES=3
PROFILE=disk-hunt

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/../lib/provenance.sh"
OUT="results/negative-demos/$(basename "$0" .sh).txt"
provenance_of "$OUT"

echo "=============================================================="
echo "Torn records: is the checksum load-bearing?"
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
    --skip-record-crc
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
