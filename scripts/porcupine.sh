#!/usr/bin/env bash
#
# A history Keel recorded, checked by a checker Keel did not write.
#
# The simulator's oracles are ours, and a property nobody thought of is a
# property nobody checks. Porcupine applies somebody else's definition of
# linearizability to a history Keel's own client recorded while the cluster was
# being partitioned, paused and killed.
#
# The run has two arms and the second is what makes the first mean anything:
#
#   experiment  the real history, which must be accepted
#   control     the same history with one read's returned value replaced by a
#               value nothing ever wrote, which must be rejected
#
# A checker that accepted the first and not the second has demonstrated it can
# tell them apart. A checker that accepted both would have told us nothing, and
# nobody would have noticed, because the output would have looked identical.
#
# Usage: scripts/porcupine.sh [seed] [seconds]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

SEED="${1:-11}"
SECS="${2:-40}"

# Homebrew's Go is not on a login shell's PATH under every launcher.
GO="$(command -v go || echo /opt/homebrew/bin/go)"
if [ ! -x "$GO" ]; then
    echo "the external check needs Go, and none is on the path" >&2
    exit 1
fi

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
OUT=results/porcupine/lin-check.txt
mkdir -p "$(dirname "$OUT")"
provenance_of "$OUT" || exit 1

echo "building" >&2
cargo build --release -p keel-chaos -p keel-server -p keel-client >&2 || exit 1
(cd tools/porcupine && "$GO" build -o /dev/null ./...) >&2 || exit 1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-porcupine-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
TALLY="$WORK/failures"
echo 0 >"$TALLY"

{
    echo "Keel's history, checked by Porcupine"
    provenance_header
    echo
    echo "checker:    github.com/anishathalye/porcupine v1.3.0 (pinned in tools/porcupine/go.sum)"
    echo "model:      one register per key; a get must return what the last linearized put wrote"
    echo "seed:       $SEED"
    echo "seconds:    $SECS of faults, plus 8 for the recovery the history has to cover"
    echo "sync mode:  durable"
    echo

    echo "--- recording a history while the cluster is being broken ---"
    "$(pwd)/target/release/keel-chaos" run \
        --seed "$SEED" \
        --nodes 3 \
        --secs "$SECS" \
        --dir "$WORK/cluster" \
        --server-bin "$(pwd)/target/release/keel-server" \
        --kv-bin "$(pwd)/target/release/kv" \
        --sync durable \
        --history "$WORK/history.jsonl" 2>&1
    recorded=$?
    if [ $recorded -ne 0 ]; then
        echo "FAIL the run did not produce a history"
        echo 1 >"$TALLY"
    else
        echo
        echo "--- experiment: the real history must be accepted ---"
        (cd tools/porcupine && "$GO" run . -history "$WORK/history.jsonl" -timeout 300s) 2>&1
        real=$?

        echo
        echo "--- control: one read's result replaced, and it must be rejected ---"
        (cd tools/porcupine && "$GO" run . -history "$WORK/history.jsonl" \
            -mutate -out "$WORK/mutated.jsonl" -timeout 300s) 2>&1
        mutated=$?

        echo
        echo "=============================================================="
        if [ $real -eq 0 ] && [ $mutated -eq 0 ]; then
            echo "PASS accepted the real history and rejected the corrupted one"
        else
            echo "FAIL real=$real mutated=$mutated"
            echo 1 >"$TALLY"
        fi
    fi
} 2>&1 | tee "$OUT"

exit "$(cat "$TALLY")"
