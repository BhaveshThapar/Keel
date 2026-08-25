#!/usr/bin/env bash
#
# Where the time in a write goes: persist, send, apply.
#
# PR-7 asked for this and BENCH.md carried it under "not measured" for two
# releases, with a reason that was true at the time: the timers would have been
# a per-operation cost taken to measure a per-operation cost. They are per
# `Ready` rather than per entry, so once a `Ready` carried tens of entries
# (ADR-035) four clock reads stopped being worth arguing about.
#
# Both arms, because the interesting part of the answer is the difference. The
# durable arm says what the three phases cost with `F_FULLFSYNC` under them; the
# fsync-off arm says how much of that was the flush. Neither is a headline: the
# second cannot be published at all, and the first is Exploratory like everything
# else here.
#
# What it cannot say, and the file says so too: `send` is loopback. On a real
# network that column is where the round trip would appear, and it does not
# appear here — which is the same gap as "no cross-node numbers".
#
# Usage: scripts/breakdown.sh [seconds] [clients] [depth]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

SECS="${1:-12}"
CLIENTS="${2:-24}"
DEPTH="${3:-16}"
NODES=3

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
OUT=results/bench/phase-breakdown.txt
mkdir -p "$(dirname "$OUT")"
provenance_of "$OUT" || exit 1

echo "building" >&2
cargo build --quiet --release -p keel-bench -p keel-server >&2 || exit 1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-breakdown-XXXXXX")"
cleanup() {
    pkill -f "$WORK" >/dev/null 2>&1
    rm -rf "$WORK"
}
trap cleanup EXIT

# Ports nobody is using, released before the servers bind them. The same race
# the integration tests take, for the same reason: a fixed port fails whenever
# two runs overlap.
free_port() {
    python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'
}

# One arm: start a cluster at this sync mode, scrape, load it, scrape again.
arm() {
    local sync="$1"
    local peers=() clients=() admins=() peer_args=()
    local i
    for i in $(seq 1 $NODES); do
        peers+=("$(free_port)")
        clients+=("$(free_port)")
        admins+=("$(free_port)")
    done
    for i in $(seq 1 $NODES); do
        peer_args+=(--peer "$i=127.0.0.1:${peers[$((i - 1))]}")
    done

    local dir="$WORK/$sync"
    for i in $(seq 1 $NODES); do
        mkdir -p "$dir/n$i"
        ./target/release/keel-server \
            --id "$i" --dir "$dir/n$i" \
            --listen "127.0.0.1:${peers[$((i - 1))]}" \
            --client "127.0.0.1:${clients[$((i - 1))]}" \
            --admin "127.0.0.1:${admins[$((i - 1))]}" \
            --sync "$sync" --tick-ms 10 "${peer_args[@]}" \
            >"$dir/n$i.log" 2>&1 &
        # Disowned so the shell does not print a job-control notice when the
        # arm ends, which would otherwise land in the middle of the artifact.
        disown
    done

    # Wait for a leader rather than sleeping a guess.
    local leader=""
    for _ in $(seq 1 100); do
        for i in $(seq 1 $NODES); do
            if curl -s "127.0.0.1:${admins[$((i - 1))]}/status" 2>/dev/null |
                grep -q '"role":"leader"'; then
                leader="${admins[$((i - 1))]}"
                break
            fi
        done
        [ -n "$leader" ] && break
        sleep 0.2
    done
    if [ -z "$leader" ]; then
        echo "  no leader; this arm measured nothing"
        pkill -f "$dir" >/dev/null 2>&1
        return 1
    fi

    local before after nodes
    before="$(curl -s "127.0.0.1:$leader/metrics")"
    nodes="127.0.0.1:${clients[0]},127.0.0.1:${clients[1]},127.0.0.1:${clients[2]}"
    local run
    run="$(./target/release/keel-bench run \
        --nodes "$nodes" --mix writes --rate 0 \
        --clients "$CLIENTS" --depth "$DEPTH" --secs "$SECS" 2>&1)"
    after="$(curl -s "127.0.0.1:$leader/metrics")"
    pkill -f "$dir" >/dev/null 2>&1

    echo "$run" | sed -n '4,6p' | sed 's/^/  /'
    python3 - "$before" "$after" <<'PY'
import sys


def scrape(text):
    out = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name, _, value = line.partition(" ")
        try:
            out[name] = float(value)
        except ValueError:
            pass
    return out


before, after = scrape(sys.argv[1]), scrape(sys.argv[2])
d = {k: after.get(k, 0.0) - before.get(k, 0.0) for k in after}
readies = d.get("keel_readies_total", 0.0)
entries = d.get("keel_entries_applied_total", 0.0)
if readies <= 0 or entries <= 0:
    print("  the leader moved mid-run; this arm's counters are not one node's")
    raise SystemExit
print(f"  {entries:.0f} entries applied in {readies:.0f} rounds "
      f"= {entries / readies:.1f} per round")
print(f"  {'phase':<9}{'total s':>10}{'ms/round':>12}{'us/entry':>12}")
for phase in ("persist", "send", "apply"):
    total = d.get(f"keel_{phase}_seconds_total", 0.0)
    print(f"  {phase:<9}{total:>10.3f}{total / readies * 1000:>12.3f}"
          f"{total / entries * 1e6:>12.1f}")
PY
}

{
    echo "=== where the time in a write goes ==="
    provenance_header
    echo
    echo "nodes:       $NODES on one host over loopback"
    echo "senders:     $CLIENTS, each with $DEPTH requests outstanding"
    echo "seconds:     $SECS per arm"
    echo "tier:        Exploratory, and the fsync-off arm is not publishable at all"
    echo
    echo "The three phases are the ones the Ready contract names, timed per"
    echo "round rather than per operation: persist covers the truncate, the"
    echo "append, the hard state and the one fsync over all three; send covers"
    echo "encoding every message and handing it to the transport; apply covers"
    echo "the state machine's batch and its own fsync."
    echo
    echo "Read the two arms against each other. The difference in persist is the"
    echo "flush, almost exactly; the difference in apply is the state machine's"
    echo "own flush, and what is left of apply is real work — decoding, the"
    echo "session table, the store."
    echo
    echo "--- durable (F_FULLFSYNC)"
    arm durable
    echo
    echo "--- none (writes neither ordered nor durable; NOT PUBLISHABLE)"
    arm none
    echo
    echo "send is loopback. On a real network that column is where the round trip"
    echo "would appear, and it does not appear here — the same gap as \"no"
    echo "cross-node numbers\" in BENCH.md."
} | tee "$OUT"
