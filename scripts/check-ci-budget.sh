#!/usr/bin/env bash
#
# The sweep covers as many distinct seeds as it says it does.
#
# TR-3 asks for at least two thousand *distinct* seeds per pull request. A
# matrix of shards can look like it delivers that and not: two shards of a
# thousand seeds each cover two thousand only if their ranges do not overlap,
# and `--from` is computed from the shard index in a shell expression that
# nothing else reads. Get that expression wrong and the job still passes, still
# takes the same time, and sweeps the same seeds twice — a number in a comment
# and half the coverage.
#
# So the arithmetic is read back out of the workflow file:
#
#   1. every sharded sweep's ranges are disjoint (stride at least the count)
#   2. every shard axis is the consecutive integers the stride assumes
#   3. at least one sweep reaches the target, at the target's step count
#   4. the seed counts cite the throughput artifact they were derived from
#
# It deliberately does not hard-code the numbers. A copy of them here would be
# a second place to forget to update, which is the failure this file exists to
# prevent.
#
# Usage: scripts/check-ci-budget.sh [target-seeds] [target-steps]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

TARGET_SEEDS="${1:-2000}"
TARGET_STEPS="${2:-50000}"
CI=.github/workflows/ci.yml

fail=0
problem() {
    printf 'FAIL %s\n' "$*"
    fail=1
}

if [ ! -f "$CI" ]; then
    problem "$CI does not exist"
    exit 1
fi

# Every sharded sweep in the file, as `stride count steps`. The three numbers
# are spread over up to four lines of a YAML block scalar, so this walks the
# file rather than trying to match them in one expression.
sweeps="$(awk '
    /--from \$\(\( \$\{\{ matrix\.shard \}\} \* [0-9]+ \)\)/ {
        match($0, /\* [0-9]+ \)\)/)
        stride = substr($0, RSTART + 2, RLENGTH - 5) + 0
        count = 0; steps = 0
        line = $0
        for (i = 0; i < 4; i++) {
            if (match(line, /--count [0-9]+/))
                count = substr(line, RSTART + 8, RLENGTH - 8) + 0
            if (match(line, /--steps [0-9]+/))
                steps = substr(line, RSTART + 8, RLENGTH - 8) + 0
            if (count > 0 && steps > 0) break
            if ((getline line) <= 0) break
        }
        print stride, count, steps
    }
' "$CI")"

if [ -z "$sweeps" ]; then
    problem "no sharded sweep found in $CI; either it is gone or this parser is"
fi

# Every shard axis has to be the consecutive integers from zero that the stride
# arithmetic assumes. `shard: [0, 2]` leaves a gap nobody sweeps while every
# count below still adds up.
shard_count=0
while read -r shards; do
    [ -n "$shards" ] || continue
    IFS=',' read -r -a shard_list <<<"$(tr -d ' ' <<<"$shards")"
    expected=0
    for s in "${shard_list[@]}"; do
        if [ "$s" != "$expected" ]; then
            problem "shard indices are [$shards]; the stride arithmetic needs consecutive integers from 0"
            break
        fi
        expected=$((expected + 1))
    done
    if [ "$shard_count" -eq 0 ]; then
        shard_count=${#shard_list[@]}
    elif [ "$shard_count" -ne "${#shard_list[@]}" ]; then
        problem "two sweeps use different shard counts; this checker assumes one axis"
    fi
done < <(sed -n 's/.*shard: \[\(.*\)\].*/\1/p' "$CI")

best=0
while read -r stride count steps; do
    [ -n "$stride" ] || continue
    if [ "$count" -eq 0 ] || [ "$steps" -eq 0 ]; then
        problem "a sweep with stride $stride has no --count or --steps this parser could find"
        continue
    fi
    distinct=$((shard_count * count))
    printf 'sweep: %d shards x %d seeds, stride %d, %d steps -> %d distinct seeds\n' \
        "$shard_count" "$count" "$stride" "$steps" "$distinct"

    # A stride smaller than the count means shard n and shard n+1 sweep the
    # same seeds, and the union is smaller than the sum.
    if [ "$stride" -lt "$count" ]; then
        problem "shards overlap: stride $stride is smaller than the count $count, so the union is not $shard_count x $count"
    fi

    if [ "$steps" -ge "$TARGET_STEPS" ] && [ "$distinct" -gt "$best" ]; then
        best=$distinct
    fi
done <<<"$sweeps"

if [ "$best" -lt "$TARGET_SEEDS" ]; then
    problem "no sweep reaches $TARGET_SEEDS distinct seeds at $TARGET_STEPS steps; the best is $best"
else
    echo "target: $best distinct seeds at >= $TARGET_STEPS steps (asked for $TARGET_SEEDS)"
fi

# A number with no artifact behind it is a guess with a comment. The rule the
# repository works to is that no claim outruns its artifact, and a CI budget is
# a claim about how long something takes.
if ! grep -q 'results/simulator/disk-throughput.txt' "$CI"; then
    problem "$CI does not cite results/simulator/disk-throughput.txt, so its seed counts are derived from nothing"
fi
if [ ! -f results/simulator/disk-throughput.txt ]; then
    problem "the throughput artifact the workflow cites does not exist"
fi

if ((fail)); then
    echo
    echo "The sweep does not cover what it claims to cover."
    exit 1
fi
echo "ci budget check clean"
