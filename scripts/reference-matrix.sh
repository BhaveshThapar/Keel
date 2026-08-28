#!/usr/bin/env bash
# Complete PR-1/PR-2 same-host matrix: every YCSB mix, both value sizes,
# three and five nodes, plus one closed-loop point per shape.

set -euo pipefail
cd "$(dirname "$0")/.."

: "${KEEL_BENCH_TIER:=reference}"
export KEEL_BENCH_TIER
RATES="${KEEL_MATRIX_RATES:-800,1600,3200,6400,12800,25600}"
SECS="${KEEL_MATRIX_SECS:-10}"
CLIENTS="${KEEL_MATRIX_CLIENTS:-64}"
DEPTH="${KEEL_MATRIX_DEPTH:-32}"
KEYS="${KEEL_MATRIX_KEYS:-1000000}"

if [[ -z "${TMPDIR:-}" ]]; then
    echo "TMPDIR must name the benchmark device" >&2
    exit 1
fi

cargo build --release -p keel-bench -p keel-server

for nodes in 3 5; do
    for value in 128 1024; do
        for mix in a b c writes; do
            scripts/campaign.sh \
                "$mix" "$RATES" "$SECS" "$CLIENTS" "$DEPTH" \
                "$value" "$KEYS" "$nodes"
            work="$(mktemp -d "$TMPDIR/keel-closed-XXXXXX")"
            trap 'rm -rf "$work"' EXIT
            target/release/keel-bench closed \
                --mix "$mix" --clients "$CLIENTS" --depth 1 \
                --secs "$SECS" --value-bytes "$value" --keys "$KEYS" \
                --runs 3 --cluster-nodes "$nodes" --dir "$work" \
                --server-bin "$(pwd)/target/release/keel-server" \
                --sync durable --tier "$KEEL_BENCH_TIER" \
                --out "closed-${mix}-${value}b-${KEYS}k-${nodes}n.txt"
            rm -rf "$work"
            trap - EXIT
        done
    done
done
