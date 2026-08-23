#!/usr/bin/env bash
#
# Runs every negative demonstration and records its output under results/.
#
# The recording used to be a manual step, which is how
# results/negative-demos/figure-8.txt came to be committed with no provenance
# header at all. One script that records all of them means a new demonstration
# is recorded the same way as the old ones, or it is not recorded.
#
# Exits non-zero if any demonstration stopped demonstrating. That matters: an
# artifact recording a failed demonstration is worse than no artifact, so the
# exit code is the pipeline's, not tee's.
#
# Usage: scripts/record-demos.sh [seeds] [steps]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

mkdir -p results/negative-demos
fail=0

for demo in scripts/negative-demos/*.sh; do
    name="$(basename "$demo" .sh)"
    out="results/negative-demos/$name.txt"
    echo "=== $name"
    # shellcheck disable=SC2086 # word splitting of "$@" is what is wanted here
    "$demo" "$@" | tee "$out"
    status="${PIPESTATUS[0]}"
    if [[ "$status" -ne 0 ]]; then
        echo "$name failed; its artifact records the failure" >&2
        fail=1
    fi
done

exit "$fail"
