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

retry() {
    local label="$1"
    shift
    for attempt in 1 2 3; do
        if "$@"; then
            return 0
        fi
        echo "${label}: attempt ${attempt} failed; retrying with a fresh cluster" >&2
        sleep 2
    done
    echo "${label}: failed three fresh-cluster attempts" >&2
    return 1
}

for nodes in 3 5; do
    for value in 128 1024; do
        for mix in a b c writes; do
            retry "campaign ${mix}/${value}B/${nodes}n" scripts/campaign.sh \
                "$mix" "$RATES" "$SECS" "$CLIENTS" "$DEPTH" \
                "$value" "$KEYS" "$nodes"
            work="$(mktemp -d "$TMPDIR/keel-closed-XXXXXX")"
            trap 'rm -rf "$work"' EXIT
            retry "closed ${mix}/${value}B/${nodes}n" target/release/keel-bench closed \
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
