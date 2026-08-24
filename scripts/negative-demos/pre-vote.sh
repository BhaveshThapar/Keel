#!/usr/bin/env bash
#
# What pre-vote is actually for, measured.
#
# This demonstration is shaped differently from the others here, and the
# difference is the point. Removing the Figure 8 rule or the record checksum
# produces a *wrong* run, and the harness catches it. Turning pre-vote off does
# not produce a wrong run: every safety property still holds. What it produces
# is a disrupted one.
#
# So there is nothing to catch, and a demonstration that claimed otherwise would
# be dressing an availability property up as a safety one. Two arms are compared
# instead, and the quantity compared is terms that were entered and produced no
# leader:
#
#   A node partitioned away from the cluster campaigns. Nobody can hear it, so
#   it wins nothing — and raises its own term each time it tries. When the
#   partition heals it carries that inflated term into the first message it
#   sends, and a healthy leader with a full quorum steps down for a node that
#   has no more log than it does.
#
# Check-quorum does not prevent this. It filters vote *requests*; the inflated
# term arrives in an ordinary response.
#
# A pass means the arm without pre-vote burned several times as many terms as
# the arm with it. Both arms are clean — that is not the axis being measured.
#
# Usage: scripts/negative-demos/pre-vote.sh [seeds] [steps]

set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

SEEDS="${1:-25}"
STEPS="${2:-40000}"
NODES=3
MARGIN=3

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/../lib/provenance.sh"
OUT="results/negative-demos/$(basename "$0" .sh).txt"
provenance_of "$OUT"

cargo build --quiet --release -p keel-sim || exit 1
SIM=target/release/keel-sim

# Sum "terms burned without a leader" across the seeds, for one arm.
burned() {
    local total=0 seed n
    for ((seed = 0; seed < SEEDS; seed++)); do
        n="$("$SIM" repro --seed "$seed" --steps "$STEPS" --nodes "$NODES" \
            --profile chaos "$@" 2>/dev/null |
            awk '/terms burned without a leader/ {print $NF}')"
        total=$((total + ${n:-0}))
    done
    echo "$total"
}

echo "=============================================================="
echo "Pre-vote: how many terms a partitioned node burns without it"
echo "  one stock build, two configurations"
echo "  $NODES nodes, $SEEDS seeds x $STEPS steps, profile chaos"
echo "=============================================================="
provenance_header
echo
echo "Both arms are clean. Pre-vote costs availability, not safety, so what is"
echo "compared is disruption rather than violations."
echo

with="$(burned)"
without="$(burned --no-pre-vote)"

printf '  with pre-vote     %6s terms entered that produced no leader\n' "$with"
printf '  without pre-vote  %6s\n' "$without"
echo

echo "=============================================================="
if [ "$without" -ge $((with * MARGIN)) ] && [ "$without" -gt 50 ]; then
    ratio=$(awk -v a="$without" -v b="$with" 'BEGIN { printf "%.1f", (b > 0 ? a / b : a) }')
    echo "PASS: turning pre-vote off burned ${ratio}x as many terms. Each one is a"
    echo "      node campaigning where nobody can hear it, and each one is a"
    echo "      term a healthy leader has to step down for when the partition"
    echo "      heals."
    exit 0
fi
if [ "$without" -le 50 ]; then
    echo "FAIL: only $without terms were burned without pre-vote across $SEEDS seeds,"
    echo "      which is too few to be a margin rather than noise. The nemesis"
    echo "      is probably not partitioning anyone off for long enough."
    exit 1
fi
echo "FAIL: without pre-vote burned $without terms against $with with it — less"
echo "      than the ${MARGIN}x margin this demonstration claims. Either pre-vote"
echo "      stopped being load-bearing or something else is inflating terms in"
echo "      both arms."
exit 1
