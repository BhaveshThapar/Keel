#!/usr/bin/env bash
#
# The validation sweep of record. Writes results/simulator/sweep.txt.
#
# Safety only. The simulator runs on a virtual clock, so how long this takes in
# wall-clock time says nothing about the system under test and no timing claim
# is made from it.
#
# Both cluster sizes are swept deliberately. Commit needs the k-th highest match
# index where k is the quorum size, so a three-node cluster reaches partial
# replication states that a five-node cluster reaches far more rarely — and one
# of those states is the window the Figure 8 rule guards.
#
# Usage: scripts/sweep.sh [seeds] [steps]

set -euo pipefail
cd "$(dirname "$0")/.."

SEEDS="${1:-500}"
STEPS="${2:-60000}"
OUT=results/simulator/sweep.txt
mkdir -p "$(dirname "$OUT")"

cargo build --quiet --release -p keel-sim

# The host belongs in the artifact, and it has to come from the machine rather
# than from whoever committed the file. `uname` alone does not say what the CPU
# is, which is the part a reader wants.
host_line() {
    case "$(uname -s)" in
        Darwin)
            printf '%s, %s cores, %s GiB, macOS %s, %s\n' \
                "$(sysctl -n machdep.cpu.brand_string)" \
                "$(sysctl -n hw.ncpu)" \
                "$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))" \
                "$(sw_vers -productVersion)" \
                "$(uname -m)"
            ;;
        Linux)
            printf '%s, %s cores, %s GiB, Linux %s, %s\n' \
                "$(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo)" \
                "$(nproc)" \
                "$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 / 1024 ))" \
                "$(uname -r)" \
                "$(uname -m)"
            ;;
        *) printf '%s %s, %s\n' "$(uname -s)" "$(uname -r)" "$(uname -m)" ;;
    esac
}

{
    echo "=== keel-sim validation sweep ==="
    echo "host:   $(host_line)"
    echo "commit: $(git rev-parse --short HEAD)$(git diff --quiet || echo ' (working tree modified)')"
    echo "date:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "Safety only. No timing claim is made here: the simulator runs on a"
    echo "virtual clock, so wall-clock speed says nothing about the system"
    echo "under test."
    echo

    for profile in default chaos; do
        for nodes in 3 5; do
            echo "--- profile=$profile nodes=$nodes"
            ./target/release/keel-sim run \
                --from 0 --count "$SEEDS" --steps "$STEPS" \
                --nodes "$nodes" --profile "$profile" | tail -2
        done
    done

    echo "--- profile=fig8-hunt nodes=3"
    ./target/release/keel-sim run \
        --from 0 --count "$SEEDS" --steps $(( STEPS + 20000 )) \
        --nodes 3 --profile fig8-hunt | tail -2

    echo
    echo "--- determinism: 100 seeds, each run twice"
    ./target/release/keel-sim determinism --from 0 --count 100 --steps 30000 | tail -2
} | tee "$OUT"
