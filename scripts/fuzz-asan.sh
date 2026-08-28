#!/usr/bin/env bash
# Run the two TR-6 campaigns under libFuzzer's AddressSanitizer instrumentation.

set -euo pipefail
cd "$(dirname "$0")/.."

RUNS="${KEEL_FUZZ_RUNS:-100000}"
TIME="${KEEL_FUZZ_TIME:-60}"
OUT="${KEEL_FUZZ_OUT:-results/fuzz/asan.txt}"
mkdir -p "$(dirname "$OUT")"
source scripts/lib/provenance.sh
provenance_of "$OUT"

if ! cargo fuzz --help >/dev/null 2>&1; then
    echo "cargo-fuzz is required: cargo install cargo-fuzz" >&2
    exit 1
fi
if ! rustup run nightly rustc --version >/dev/null 2>&1; then
    echo "the nightly toolchain is required: rustup toolchain install nightly" >&2
    exit 1
fi

{
    echo "Keel libFuzzer + ASan release campaigns"
    provenance_header
    echo "rustc: $(rustup run nightly rustc --version)"
    echo "cargo-fuzz: $(cargo fuzz --version)"
    echo "runs per target: $RUNS"
    echo "maximum seconds per target: $TIME"
    echo
    for target in raft_message core_events; do
        echo "--- $target ---"
        cargo +nightly fuzz run "$target" -- \
            -runs="$RUNS" -max_total_time="$TIME" -print_final_stats=1
        echo "PASS $target: zero crashes"
    done
} 2>&1 | tee "$OUT"
