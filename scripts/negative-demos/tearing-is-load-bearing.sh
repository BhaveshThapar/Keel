#!/usr/bin/env bash
#
# Does the tear model have teeth, or only the rules it tests?
#
# Every other demonstration here removes a rule and shows the harness catching
# the result. That checks the rule. It does not check the *fault model*, and
# three of the seven bugs so far were in the harness rather than in the code it
# tests — so the model gets held to the same standard as everything else.
#
# The trick is to hold the bug fixed and vary the model. Both runs below are the
# same deliberately broken build, with the record checksum compiled out. What
# differs is what a crash does to bytes no fsync covered:
#
#   with tears     a crash decides sector by sector what reached the device, so
#                  a record can land half-written and the missing checksum lets
#                  it decode into something that was never written.
#   without tears  a crash takes every staged write back whole, so a record is
#                  either entirely there or entirely absent, no record is ever
#                  half-written, and the checksum has nothing to catch.
#
# A pass means the broken build is caught under one model and invisible under
# the other. That is the argument for byte-granular tearing stated as an
# experiment rather than as an assertion: without it, this bug ships.
#
# Usage: scripts/negative-demos/tearing-is-load-bearing.sh [seeds] [steps]

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
echo "Tearing: does the fault model earn its cost?"
echo "  one broken build, two fault models"
echo "  $NODES nodes, $SEEDS seeds x $STEPS steps"
echo "=============================================================="
provenance_header
echo

echo "--- WITH TEARS (disk-hunt): the broken build. Expect seeds to fail."
echo
cargo run --quiet --release -p keel-sim --features negative-demos -- \
    run --from 0 --count "$SEEDS" --steps "$STEPS" --nodes "$NODES" --profile disk-hunt \
    --skip-record-crc
torn=$?
echo

echo "--- WITHOUT TEARS (chaos): the same broken build. Expect every seed to pass."
echo
cargo run --quiet --release -p keel-sim --features negative-demos -- \
    run --from 0 --count "$SEEDS" --steps "$STEPS" --nodes "$NODES" --profile chaos \
    --skip-record-crc
whole=$?
echo

echo "=============================================================="
if [ $torn -ne 0 ] && [ $whole -eq 0 ]; then
    echo "PASS: the same bug is caught when writes tear and invisible when they"
    echo "      are lost whole. Byte-granular tearing is what makes the checksum"
    echo "      checkable at all."
    exit 0
fi
if [ $torn -eq 0 ]; then
    echo "FAIL: the broken build survived even with tearing on. Either the tear"
    echo "      model is not reaching the states it exists to reach, or the rule"
    echo "      that was removed is not the one this demonstration thinks it is."
    exit 1
fi
echo "FAIL: the broken build was caught without tearing too, so this run says"
echo "      nothing about what the tear model adds. Either the record model is"
echo "      producing torn writes it should not, or the failure has another"
echo "      cause and the comparison is measuring the wrong thing."
exit 1
