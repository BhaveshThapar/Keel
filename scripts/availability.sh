#!/usr/bin/env bash
# Record and plot acknowledged throughput while real-cluster nemeses land.

set -euo pipefail
cd "$(dirname "$0")/.."

SECS="${1:-60}"
SEED="${2:-17}"
source scripts/lib/provenance.sh
OUT=results/chaos/availability.txt
SVG=results/chaos/availability.svg
mkdir -p results/chaos
provenance_of "$OUT"

cargo build --release -p keel-chaos -p keel-server -p keel-client
WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-availability-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

{
    echo "Keel real-cluster availability timeline"
    provenance_header
    echo "seed:    $SEED"
    echo "seconds: $SECS plus five seconds of healed recovery"
    echo "sample:  100 ms cumulative acknowledgements"
    echo
    target/release/keel-chaos run \
        --seed "$SEED" --nodes 3 --secs "$SECS" \
        --dir "$WORK/cluster" \
        --server-bin "$(pwd)/target/release/keel-server" \
        --kv-bin "$(pwd)/target/release/kv" --sync durable \
        --timeline "$WORK/timeline.csv"
    echo
    echo "--- raw timeline ---"
    cat "$WORK/timeline.csv"
} 2>&1 | tee "$OUT"

scripts/plot-availability.py "$WORK/timeline.csv" "$SVG" "$OUT"
echo "plot: $SVG"
