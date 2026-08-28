#!/usr/bin/env bash
# PR-3 ablations on one otherwise-identical three-node cluster shape.

set -euo pipefail
cd "$(dirname "$0")/.."

: "${TMPDIR:?TMPDIR must name the benchmark device}"
TIER="${KEEL_BENCH_TIER:-reference}"
RATES="${KEEL_ABLATION_RATES:-3200,6400,12800}"
SECS="${KEEL_ABLATION_SECS:-10}"
CLIENTS="${KEEL_ABLATION_CLIENTS:-64}"
DEPTH="${KEEL_ABLATION_DEPTH:-32}"
SERVER="$(pwd)/target/release/keel-server"
BENCH="$(pwd)/target/release/keel-bench"

cargo build --release -p keel-bench -p keel-server

run_arm() {
    local name="$1" mix="$2" consistency="$3" sync="$4" inflight="$5" batch="$6"
    local work
    work="$(mktemp -d "$TMPDIR/keel-ablation-XXXXXX")"
    "$BENCH" campaign \
        --mix "$mix" --rates "$RATES" --secs "$SECS" \
        --clients "$CLIENTS" --depth "$DEPTH" --runs 3 \
        --value-bytes 128 --keys 1000000 --cluster-nodes 3 \
        --consistency "$consistency" --sync "$sync" \
        --max-inflight-msgs "$inflight" --max-batch-entries "$batch" \
        --dir "$work" --server-bin "$SERVER" --root . --tier "$TIER" \
        --out "ablation-${name}.txt" --svg "ablation-${name}.svg"
    rm -rf "$work"
}

# Baseline appears once and is the common control for batching and pipelining.
run_arm baseline writes read-index durable 16 18446744073709551615
run_arm batching-off writes read-index durable 16 1
run_arm pipelining-off writes read-index durable 1 18446744073709551615

# Read paths use the read-only YCSB C mix.
run_arm read-index c read-index durable 16 18446744073709551615
run_arm lease c lease durable 16 18446744073709551615
run_arm stale c stale durable 16 18446744073709551615

# On Linux `durable` is fdatasync and `full` is fsync. Both remain publishable.
run_arm fdatasync writes read-index durable 16 18446744073709551615
run_arm fsync writes read-index full 16 18446744073709551615
