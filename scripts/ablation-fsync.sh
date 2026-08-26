#!/usr/bin/env bash
#
# The control arm: the same cluster, the same load, and no fsync.
#
# This is the one measurement that says what durability costs, and it is the one
# the gate refuses to publish — correctly, because a configuration that does not
# survive power loss may not produce a durability number. So it goes through the
# *admitted* door instead, with the reason stamped into its header, and the file
# it writes says NOT PUBLISHABLE in the second line.
#
# It existed as a hand-typed command until this script did. That is a worse
# problem than it looks: an artifact whose only record of how it was produced is
# its own header cannot be regenerated when the code under it changes, and the
# arm it is the control for gets re-run without it.
#
# Read it beside results/bench/campaign-writes.txt, which is the same parameters
# with `--sync durable`. One variable changed.
#
# Usage: scripts/ablation-fsync.sh [mix] [rates] [seconds-per-run] [clients] [depth]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

MIX="${1:-writes}"
RATES="${2:-800,1600,3200,6400,12800,25600}"
SECS="${3:-6}"
CLIENTS="${4:-64}"
DEPTH="${5:-32}"
TIER="${KEEL_BENCH_TIER:-exploratory}"

echo "building" >&2
cargo build --quiet --release -p keel-bench -p keel-server >&2 || exit 1

# A real directory on a real filesystem, and the one the gate probes. tmpfs
# would be refused even for an admitted run, because an fsync there returns
# without doing anything and the *comparison* would then be meaningless in both
# arms rather than in one.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-ablation-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

"$(pwd)/target/release/keel-bench" campaign \
    --mix "$MIX" \
    --rates "$RATES" \
    --secs "$SECS" \
    --clients "$CLIENTS" \
    --depth "$DEPTH" \
    --dir "$WORK" \
    --server-bin "$(pwd)/target/release/keel-server" \
    --sync none \
    --tier "$TIER" \
    --out "ablation-fsync-off.txt" \
    --svg "ablation-fsync-off.svg" \
    --admit "the fsync-off arm of the durability ablation: the same cluster with writes neither ordered nor made durable"
