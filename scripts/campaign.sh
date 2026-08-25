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
# Usage: scripts/campaign.sh [mix] [rates] [seconds-per-run] [clients] [depth]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

MIX="${1:-writes}"
RATES="${2:-200,400,800,1600,3200,6400}"
SECS="${3:-6}"
# Senders, and it is a knob that has to be stated: this many threads offer load,
# each with `DEPTH` requests outstanding, so the ceiling belonging to the load
# generator is clients times depth divided by per-request latency. It is in the
# result's header for that reason.
CLIENTS="${4:-24}"
# How many requests one sender keeps outstanding.
#
# The second half of the same knob. At depth 1 a sender cannot offer more than
# one request per round trip, so achieved throughput is capped at clients divided
# by per-request latency however fast the cluster is — and the number then says
# as much about this harness as about the system (ADR-033). It is in the result's
# header for the same reason `clients` is.
DEPTH="${5:-16}"

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
    --depth "$DEPTH" \
    --dir "$WORK" \
    --server-bin "$(pwd)/target/release/keel-server" \
    --sync durable \
    --out "campaign-$MIX.txt" \
    --svg "campaign-$MIX.svg"
