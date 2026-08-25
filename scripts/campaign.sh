#!/usr/bin/env bash
#
# A throughput-versus-latency curve, against a real cluster, through the gate.
#
# PR-2 asks for curves rather than points, because a single throughput number is
# a claim about a saturation point whose latency nobody quoted. Each offered rate
# becomes one point: three independent runs, median reported, with the p99
# measured from the moment each request was *due* rather than from when a sender
# thread got to it.
#
# Nothing in this script decides whether the result may be published. keel-bench
# does, and it refuses before a single request is sent — a run that is going to
# be refused should be refused in the second it starts rather than in the hour it
# finishes.
#
# Usage: scripts/campaign.sh [mix] [rates] [seconds-per-run]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

MIX="${1:-writes}"
RATES="${2:-50,100,150,200,300,400}"
SECS="${3:-6}"
# Concurrency, and it is a knob that has to be stated. The client is blocking,
# so this many requests are in flight at once and the achieved throughput can
# never exceed clients divided by per-request latency — a ceiling that belongs
# to the load generator, not the cluster. It is in the result's header for that
# reason.
CLIENTS="${4:-24}"

echo "building" >&2
cargo build --release -p keel-bench -p keel-server >&2 || exit 1

# A real directory on a real filesystem, and the one the gate probes. It has to
# outlive the run only long enough to be measured, but it must not be tmpfs —
# which the gate would refuse, correctly.
WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-campaign-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

"$(pwd)/target/release/keel-bench" campaign \
    --mix "$MIX" \
    --rates "$RATES" \
    --secs "$SECS" \
    --clients "$CLIENTS" \
    --dir "$WORK" \
    --server-bin "$(pwd)/target/release/keel-server" \
    --sync durable \
    --out "campaign-$MIX.txt" \
    --svg "campaign-$MIX.svg"
