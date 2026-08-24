#!/usr/bin/env bash
#
# How fast the simulator actually is, per profile, so a CI budget is a
# calculation rather than a guess. Writes results/simulator/disk-throughput.txt.
#
# This exists because the disk-simulator job's 120 seeds x 40k steps was sized by
# eye. A seed count is a claim about wall-clock time, and a claim needs an
# artifact like any other.
#
# Safety is not measured here and nothing here is a statement about the system
# under test. The simulator runs on a virtual clock; the only quantity measured
# is how long the harness itself takes on this host.
#
# Usage: scripts/throughput.sh [seeds] [steps]

set -euo pipefail
cd "$(dirname "$0")/.."

SEEDS="${1:-60}"
STEPS="${2:-40000}"
# Each cell is timed more than once and the slowest repetition is the one kept.
# A single timing of a few seconds swings by more than half on a laptop that is
# also running a browser, and a budget derived from the lucky repetition is a
# budget that fails on the unlucky one.
REPS="${3:-3}"
OUT=results/simulator/disk-throughput.txt
ROWS="$(mktemp)"
trap 'rm -f "$ROWS"' EXIT
mkdir -p "$(dirname "$OUT")"

# The runner factor below is an assumption, not a measurement, and it is stated
# as one in the artifact. See "what this buys a CI job".
RUNNER_FACTOR=6
# Of a 45-minute job timeout, what the sweep itself may spend. The rest is
# checkout, toolchain, and a cache miss compiling the workspace from scratch.
SWEEP_MINUTES=25
# What the workflow actually asks for. The gap between this and SWEEP_MINUTES is
# margin against RUNNER_FACTOR being wrong, and it is a factor of five — which
# is the point of choosing a target rather than spending the ceiling.
TARGET_MINUTES=5

cargo build --quiet --release -p keel-sim

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
provenance_of "$OUT"

# `date +%s%N` is GNU-only; macOS `date` has no %N, and this script has to give
# the same answer on a developer laptop and on a CI runner. Perl's Time::HiRes
# ships with both.
now_s() { perl -MTime::HiRes -e 'printf "%.3f", Time::HiRes::time()'; }

# Times one sweep $REPS times and appends the *slowest* repetition to $ROWS as a
# row. Printing is left to the caller so the rows can be reread afterwards to
# derive the budgets.
measure() {
    local profile="$1" nodes="$2" t0 t1 worst=0 rep
    for ((rep = 0; rep < REPS; rep++)); do
        t0="$(now_s)"
        ./target/release/keel-sim run \
            --from 0 --count "$SEEDS" --steps "$STEPS" \
            --nodes "$nodes" --profile "$profile" >/dev/null
        t1="$(now_s)"
        worst="$(awk -v a="$t1" -v b="$t0" -v w="$worst" \
            'BEGIN { e = a - b; printf "%.3f", (e > w ? e : w) }')"
    done
    awk -v p="$profile" -v n="$nodes" -v s="$SEEDS" -v st="$STEPS" -v e="$worst" 'BEGIN {
        if (e <= 0) e = 0.001
        printf "%-12s %5s %7s %8s %9.2f %12.0f %14.2f\n", p, n, s, st, e, (s*st)/e, s/e
    }' >>"$ROWS"
}

for profile in default chaos fig8-hunt; do
    for nodes in 3 5; do measure "$profile" "$nodes"; done
done
# The disk profiles are the reason this file exists. Every record is really
# encoded, checksummed, written and parsed, and every restart re-scans every
# segment, so a seed-run costs more here than on the profiles above — which is
# exactly the fact the disk-simulator job was sized without.
for profile in disk-chaos disk-hunt; do
    for nodes in 3 5; do measure "$profile" "$nodes"; done
done

# A budget has to survive the slowest cell in its matrix, not the average one.
# Column 6 is steps/s, which is the rate a budget of (seeds x steps) divides by.
slowest_steps_per_s() {
    awk -v want="$1" '$1 ~ want { if (m == "" || $6 < m) { m = $6; row = $1 " at " $2 " nodes" } }
                      END { printf "%d %s", m, row }' "$ROWS"
}

# seeds that fit = (minutes x 60 x steps/s / factor) / steps, floored to a round
# number so the workflow reads as a decision rather than as a readout.
budget() {
    local rate="$1" steps="$2" minutes="$3"
    awk -v r="$rate" -v st="$steps" -v m="$minutes" -v f="$RUNNER_FACTOR" 'BEGIN {
        n = (m * 60 * (r / f)) / st
        printf "%d", int(n / 50) * 50
    }'
}

{
    echo "=== keel-sim throughput ==="
    provenance_header
    echo
    echo "What a seed-run costs, so a CI seed count can be derived instead of"
    echo "guessed. Nothing here is a statement about the system under test: the"
    echo "simulator runs on a virtual clock, and this measures only the harness."
    echo
    echo "Each cell was timed ${REPS} times and the slowest repetition is the one"
    echo "reported, because a budget has to survive the unlucky run."
    echo
    printf "%-12s %5s %7s %8s %9s %12s %14s\n" \
        profile nodes seeds steps seconds steps/s seed-runs/s
    cat "$ROWS"

    echo
    echo "--- what this buys a CI job"
    echo
    echo "A GitHub-hosted ubuntu-latest runner is slower than this host, by an"
    echo "amount nobody here has measured: the repository has no remote yet, so"
    echo "no workflow has ever run and there is no observed job duration to"
    echo "divide by. The budgets below divide the slowest measured rate by a"
    echo "factor of ${RUNNER_FACTOR} and spend at most ${SWEEP_MINUTES} of each job's 45 minutes on"
    echo "sweeping. That factor is an assumption, and is labelled as one."
    echo
    echo "It is an assumption with a mechanism to replace it: the throughput job"
    echo "in .github/workflows/nightly.yml runs this same script on the runner"
    echo "and uploads the result. The first time it runs, the factor becomes a"
    echo "measurement and this paragraph goes away."
    echo

    read -r net_rate net_row <<<"$(slowest_steps_per_s '^(default|chaos|fig8-hunt)$')"
    read -r disk_rate disk_row <<<"$(slowest_steps_per_s '^disk-')"

    printf "simulator job    slowest cell %-24s %9s steps/s\n" "$net_row" "$net_rate"
    printf "                 at 50000 steps: %5s seeds is the %s-minute ceiling\n" \
        "$(budget "$net_rate" 50000 "$SWEEP_MINUTES")" "$SWEEP_MINUTES"
    printf "                                 %5s seeds per shard is what CI asks for\n" \
        "$(budget "$net_rate" 50000 "$TARGET_MINUTES")"
    printf "disk-simulator   slowest cell %-24s %9s steps/s\n" "$disk_row" "$disk_rate"
    printf "                 at 40000 steps: %5s seeds is the %s-minute ceiling\n" \
        "$(budget "$disk_rate" 40000 "$SWEEP_MINUTES")" "$SWEEP_MINUTES"
    printf "                                 %5s seeds is what CI asks for\n" \
        "$(budget "$disk_rate" 40000 "$TARGET_MINUTES")"
    echo
    echo "CI asks for the ${TARGET_MINUTES}-minute figure, not the ceiling. The five-fold gap is"
    echo "margin against the runner factor being wrong, and it is deliberate: a"
    echo "budget spent to its limit fails the day the runners get slower, and a"
    echo "red build nobody can attribute to a code change is worse than a sweep"
    echo "that is narrower than it could be."
    echo
    echo "Re-measured whenever a phase changes what a seed costs. P8 put the"
    echo "real state machine under every node — every committed entry is now"
    echo "decoded, deduplicated and written rather than counted — and the"
    echo "network profiles got about fifteen per cent slower per event for it."
    echo "The budget follows the measurement rather than the other way round."
    echo
    echo "P19's distinct-seed target spends against this same file rather than"
    echo "against another guess."
} | tee "$OUT"
