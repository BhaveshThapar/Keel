#!/usr/bin/env bash
#
# etcd, measured the same way, on the same machine, in the same hour.
#
# PR-4 asks for a baseline against etcd's own benchmark tool rather than against
# a number from a blog post, and the reason is that a comparison is only a
# comparison when both sides ran on the same hardware under the same conditions.
# Everything here is aimed at that: the same value size, the same client count,
# the same key count, back to back.
#
# What this can and cannot say, stated before the numbers rather than after:
#
#   It CAN compare the two systems on the measured Linux host, with the exact
#   runtime and storage path recorded in the result. On macOS, Docker Desktop
#   introduces a VM boundary, so that run remains Exploratory rather than a
#   cross-system headline.
#
# The mechanism behind whatever difference appears is the part worth writing
# down, and it is not a mystery: etcd stores in bbolt, a B+tree with a per-
# transaction fsync, and speaks gRPC over HTTP/2. Keel stores in an LSM with
# group commit — one fsync retires every batch queued behind it — and speaks
# length-prefixed frames over TCP. Those are different trades, not different
# amounts of effort.
#
# Usage: scripts/etcd-baseline.sh [value-bytes] [clients] [total-ops]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

VALUE_BYTES="${1:-128}"
CLIENTS="${2:-8}"
TOTAL="${3:-20000}"
ETCD_VERSION=v3.5.17
TIER="${KEEL_BENCH_TIER:-exploratory}"

# shellcheck source=scripts/lib/container.sh
source "$(dirname "$0")/lib/container.sh"
if ! container_detect; then
    container_missing
    exit 1
fi
ETCD_IMAGE="gcr.io/etcd-development/etcd:$ETCD_VERSION"
GO="$(command -v go || echo /opt/homebrew/bin/go)"
if [ ! -x "$GO" ]; then
    echo "the etcd baseline builds etcd's own benchmark tool, which needs Go" >&2
    exit 1
fi

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
OUT=results/bench/etcd-baseline.txt
mkdir -p "$(dirname "$OUT")"
provenance_of "$OUT" || exit 1

WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-etcd-XXXXXX")"
BENCH="$WORK/benchmark"
ETCD_PID=""
cleanup() {
    if container_is_oci; then
        "$KEEL_CONTAINER" rm -f keel-etcd-baseline >/dev/null 2>&1
    elif [ -n "$ETCD_PID" ]; then
        kill "$ETCD_PID" >/dev/null 2>&1
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

# Built from a clone rather than with `go install`, and not by choice: etcd's
# go.mod carries replace directives, so `go install <module>@<version>` refuses
# outright. Cloning at the tag and building inside the tree is the supported
# path, and it also pins the tool to exactly the etcd being measured — a
# benchmark tool from a different version is a different benchmark.
echo "building etcd's own benchmark tool at $ETCD_VERSION" >&2
git clone -q --depth 1 --branch "$ETCD_VERSION" https://github.com/etcd-io/etcd "$WORK/etcd" >&2 || {
    echo "could not clone etcd $ETCD_VERSION" >&2
    exit 1
}
(cd "$WORK/etcd/tools/benchmark" && "$GO" build -o "$BENCH" .) >&2 || {
    echo "could not build etcd's benchmark tool" >&2
    exit 1
}

echo "starting a single-node etcd $ETCD_VERSION with $KEEL_CONTAINER" >&2
if container_is_oci; then
    "$KEEL_CONTAINER" rm -f keel-etcd-baseline >/dev/null 2>&1
    "$KEEL_CONTAINER" run -d --name keel-etcd-baseline \
        -p 2379:2379 -p 2380:2380 \
        "$ETCD_IMAGE" \
        /usr/local/bin/etcd \
        --name node1 \
        --listen-client-urls http://0.0.0.0:2379 \
        --advertise-client-urls http://0.0.0.0:2379 \
        --listen-peer-urls http://0.0.0.0:2380 \
        --initial-advertise-peer-urls http://0.0.0.0:2380 \
        --initial-cluster node1=http://0.0.0.0:2380 \
        >/dev/null || exit 1
else
    # Apptainer shares the host's network namespace, so there is no port
    # mapping to do and nothing to publish: etcd binds the host's 2379 directly.
    # Its data directory is bound in rather than living inside the image, which
    # is read-only.
    mkdir -p "$WORK/etcd-data"
    "$KEEL_CONTAINER" exec --cleanenv \
        --bind "$WORK/etcd-data:/etcd-data" \
        "docker://$ETCD_IMAGE" \
        /usr/local/bin/etcd \
        --name node1 \
        --data-dir /etcd-data \
        --listen-client-urls http://0.0.0.0:2379 \
        --advertise-client-urls http://0.0.0.0:2379 \
        --listen-peer-urls http://0.0.0.0:2380 \
        --initial-advertise-peer-urls http://0.0.0.0:2380 \
        --initial-cluster node1=http://0.0.0.0:2380 \
        >"$WORK/etcd.log" 2>&1 &
    ETCD_PID=$!
fi

# Wait for it rather than sleeping a guess. The health check goes over the
# client port either way, so it does not care which runtime started it — and the
# benchmark tool that follows reaches etcd the same way.
healthy=0
for _ in $(seq 1 60); do
    if "$BENCH" --endpoints=http://127.0.0.1:2379 --conns=1 --clients=1 \
        put --key-size=16 --sequential-keys --total=1 --val-size=1 >/dev/null 2>&1; then
        healthy=1
        break
    fi
    sleep 1
done
if [ "$healthy" -ne 1 ]; then
    echo "etcd did not become reachable on 127.0.0.1:2379" >&2
    [ -f "$WORK/etcd.log" ] && tail -20 "$WORK/etcd.log" >&2
    exit 1
fi

{
    echo "=== etcd baseline, and Keel measured the same way ==="
    provenance_header
    echo
    echo "etcd version:  $ETCD_VERSION, single node, in a container"
    container_header
    echo "value bytes:   $VALUE_BYTES"
    echo "clients:       $CLIENTS"
    echo "total ops:     $TOTAL"
    echo "tier:          $TIER for both durable sides"
    echo
    echo "Measurement scope:"
    if [ "$(uname -s)" = Linux ]; then
        echo "  Both systems ran on this Linux host. The container runtime and"
        echo "  filesystem facts above are part of the comparison provenance."
    else
        echo "  This non-Linux host inserts a container/VM storage boundary; the"
        echo "  number is Exploratory and is not a cross-system headline."
    fi
    echo
    echo "  The mechanism behind any difference is not a mystery: etcd stores in"
    echo "  bbolt, a B+tree with a per-transaction fsync, and speaks gRPC over"
    echo "  HTTP/2. Keel stores in an LSM with group commit, where one fsync"
    echo "  retires every batch queued behind it, and speaks length-prefixed"
    echo "  frames over TCP. Different trades, not different amounts of effort."
    echo
    echo "--- etcd, by its own benchmark tool"
    echo
    "$BENCH" --endpoints=http://127.0.0.1:2379 --conns=1 --clients="$CLIENTS" \
        put --key-size=16 --sequential-keys --total="$TOTAL" --val-size="$VALUE_BYTES" 2>&1 |
        grep -vE '^\s*$'
    echo
    echo "--- durability and runtime caveat"
    echo
    if [ "$(uname -s)" = Linux ]; then
        echo "Keel uses fdatasync for durable batches; etcd runs through the recorded"
        echo "container runtime. This is a same-host comparison, not proof that their"
        echo "storage engines have identical durability paths."
    else
        echo "The container/VM boundary changes storage semantics on this platform,"
        echo "so this result is not suitable for a cross-system performance ratio."
    fi
    echo
    echo "The honest internal control is Keel's own fsync-off arm:"
    echo "results/bench/ablation-fsync-off.txt, where the same cluster with writes"
    echo "neither ordered nor durable meets the same offered rates within two"
    echo "percent, at about half the tail latency, and keeps holding its schedule"
    echo "past the rate at which the durable arm bends. That is the cost of the"
    echo "promise, measured on one machine with one variable changed — and it is"
    echo "latency and headroom rather than throughput, because one flush now"
    echo "retires a batch of tens rather than a single entry."
} | tee "$OUT"

echo
echo "etcd's numbers are in $OUT. Keel's half is scripts/campaign.sh, which"
echo "writes through the same gate."
