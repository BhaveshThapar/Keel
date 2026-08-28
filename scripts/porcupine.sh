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
# Usage: scripts/porcupine.sh [seed] [seconds] [control-seconds] [read-index|lease]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

SEED="${1:-11}"
SECS="${2:-40}"
# How long the control arm's own run records for. It gets a history of its own
# rather than a slice of the experiment's; see the note it prints.
CONTROL_SECS="${3:-6}"
CONSISTENCY="${4:-read-index}"
case "$CONSISTENCY" in
    read-index|lease) ;;
    *) echo "consistency must be read-index or lease" >&2; exit 1 ;;
esac

# Homebrew's Go is not on a login shell's PATH under every launcher.
GO="$(command -v go || echo /opt/homebrew/bin/go)"
if [ ! -x "$GO" ]; then
    echo "the external check needs Go, and none is on the path" >&2
    exit 1
fi

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
if [ "$CONSISTENCY" = read-index ]; then
    OUT=results/porcupine/lin-check.txt
else
    OUT="results/porcupine/lin-check-${CONSISTENCY}.txt"
fi
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
    echo "clients:    8, each with 8 requests outstanding — so operations overlap"
    echo "            within a client as well as between them, which is what gives"
    echo "            the checker something to reorder"
    echo "control:    a second, shorter run of its own — $CONTROL_SECS seconds of faults"
    echo "            rather than a slice of the first; see the note above that arm"
    echo "seed:       $SEED"
    echo "seconds:    $SECS of faults, plus 8 for the recovery the history has to cover"
    echo "sync mode:  durable"
    echo "reads:      $CONSISTENCY"
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
        --history-consistency "$CONSISTENCY" \
        --history "$WORK/history.jsonl" 2>&1
    recorded=$?
    if [ $recorded -ne 0 ]; then
        echo "FAIL the run did not produce a history"
        echo 1 >"$TALLY"
    else
        echo
        echo "--- experiment: the real history must be accepted ---"
        (cd tools/porcupine && "$GO" run . -history "$WORK/history.jsonl" -timeout 900s) 2>&1
        real=$?

        echo
        echo "--- control: one read's result replaced, and it must be rejected ---"
        echo
        echo "On a history of its own, and both halves of that matter."
        echo
        echo "Shorter, because refuting costs what accepting does not: to accept,"
        echo "the checker finds one linearization and stops; to refute, it must"
        echo "exhaust the space and show there is none. On the whole depth-8"
        echo "history above, that ran the machine out of memory and the control"
        echo "was killed partway through — neither a pass nor a failure, and an"
        echo "arm that reports nothing cannot make the other arm evidence."
        echo
        echo "A *history*, not a prefix of the one above, because a prefix of a"
        echo "concurrent history is not a history. At depth 8 a read can return a"
        echo "value written by an operation invoked after it, so the write sits"
        echo "later in a file ordered by invocation; cut the file and the read"
        echo "survives while the write it observed does not. The checker then"
        echo "rejects — for a reason that has nothing to do with the mutation, and"
        echo "the arm reports success while demonstrating nothing. That was tried."
        echo
        "$(pwd)/target/release/keel-chaos" run \
            --seed "$((SEED + 1))" \
            --nodes 3 \
            --secs "$CONTROL_SECS" \
            --dir "$WORK/control" \
            --server-bin "$(pwd)/target/release/keel-server" \
            --kv-bin "$(pwd)/target/release/kv" \
            --sync durable \
            --history-consistency "$CONSISTENCY" \
            --history "$WORK/control.jsonl" 2>&1 | tail -3
        if [ ! -s "$WORK/control.jsonl" ]; then
            echo "FAIL the control run did not produce a history"
            mutated=1
        else
            (cd tools/porcupine && "$GO" run . -history "$WORK/control.jsonl" \
                -mutate -out "$WORK/mutated.jsonl" -timeout 900s) 2>&1
            mutated=$?
        fi

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
