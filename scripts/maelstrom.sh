#!/usr/bin/env bash
#
# Keel under Jepsen's Maelstrom, checked by Knossos.
#
# This is the external check. The simulator's oracles are ours: we chose the
# properties, we wrote the checks, and a property nobody thought of is a
# property nobody checks. Knossos applies somebody else's definition of
# linearizability to a history it recorded itself, and it does not care what we
# believe about the code.
#
# Maelstrom is pinned by tarball and checksum rather than taken from whatever a
# machine happens to have. A result that cannot be reproduced against a named
# version is a result about an unnamed program.
#
# Usage: scripts/maelstrom.sh [time-limit-seconds] [rate]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

TIME_LIMIT="${1:-60}"
RATE="${2:-30}"
NODES=3

MAELSTROM_VERSION=v0.2.4
MAELSTROM_SHA256=301ec71d6b12af0d765edb413f5cf5aa1046b5609bd4e31376a0b549548e5799
MAELSTROM_URL="https://github.com/jepsen-io/maelstrom/releases/download/${MAELSTROM_VERSION}/maelstrom.tar.bz2"

# Outside the repository: sixty-seven megabytes of somebody else's JVM
# application is not something to vendor, and MAELSTROM_HOME lets an operator
# point at one they already trust.
CACHE="${MAELSTROM_HOME:-${TMPDIR:-/tmp}/keel-maelstrom-${MAELSTROM_VERSION}}"

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
OUT=results/maelstrom/lin-kv.txt
mkdir -p "$(dirname "$OUT")"
provenance_of "$OUT"

fetch() {
    if [ -x "$CACHE/maelstrom/maelstrom" ]; then
        return 0
    fi
    echo "fetching Maelstrom $MAELSTROM_VERSION into $CACHE" >&2
    mkdir -p "$CACHE" || return 1
    curl -sSL -o "$CACHE/maelstrom.tar.bz2" "$MAELSTROM_URL" || return 1

    # Before unpacking, not after: a tarball that is not the one this script
    # names is not a tarball to extract into a directory and then run.
    local got
    got="$(shasum -a 256 "$CACHE/maelstrom.tar.bz2" | awk '{print $1}')"
    if [ "$got" != "$MAELSTROM_SHA256" ]; then
        echo "checksum mismatch: expected $MAELSTROM_SHA256, got $got" >&2
        rm -f "$CACHE/maelstrom.tar.bz2"
        return 1
    fi
    tar xjf "$CACHE/maelstrom.tar.bz2" -C "$CACHE" || return 1
}

if ! command -v java >/dev/null; then
    echo "Maelstrom needs a JVM and none is on the path" >&2
    exit 1
fi
# Maelstrom renders latency and rate plots with gnuplot, and a failure to render
# one is reported as a checker returning :unknown — which makes the whole run
# report :unknown while every correctness check passed. Refusing up front beats
# an inconclusive result nobody can distinguish from a real one.
if ! command -v gnuplot >/dev/null; then
    echo "Maelstrom plots with gnuplot and none is on the path; without it the" >&2
    echo "run reports :unknown even when every correctness check passes" >&2
    exit 1
fi
if ! fetch; then
    echo "could not obtain Maelstrom" >&2
    exit 1
fi

cargo build --quiet --release -p keel-maelstrom || exit 1
BINARY="$PWD/target/release/keel-maelstrom"

{
    echo "=============================================================="
    echo "Maelstrom lin-kv, no nemesis"
    echo "  $NODES nodes, ${TIME_LIMIT}s at $RATE ops/s, Maelstrom $MAELSTROM_VERSION"
    echo "=============================================================="
    provenance_header
    echo
    echo "The checker is Knossos, inside Maelstrom. It applies a definition of"
    echo "linearizability nobody here chose to a history it recorded itself."
    echo
    echo "No nemesis in this run: no partitions, no crashes, no clock skew."
    echo "That is deliberate and it is a floor rather than a result — a system"
    echo "that cannot pass without faults will not pass with them. The nemesis"
    echo "run is M2's."
    echo

    (
        cd "$CACHE/maelstrom" || exit 1
        ./maelstrom test \
            -w lin-kv \
            --bin "$BINARY" \
            --node-count "$NODES" \
            --time-limit "$TIME_LIMIT" \
            --rate "$RATE" \
            --concurrency 2n
    ) 2>&1 | grep -vE "^(WARNING|Warning)" | tail -40
    status=${PIPESTATUS[0]}

    echo
    echo "=============================================================="
    if [ "$status" -eq 0 ]; then
        echo "PASS: Knossos found the history linearizable."
    else
        echo "FAIL: Maelstrom exited $status. The history is in the store/"
        echo "      directory under $CACHE/maelstrom."
    fi
    exit "$status"
} | tee "$OUT"

exit "${PIPESTATUS[0]}"
