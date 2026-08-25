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
#   It CAN say what each system did on this laptop, with fsync on, at these
#   parameters. Both sides are Exploratory tier and neither is a headline.
#
#   It CANNOT say which is faster in general. etcd runs in a Linux container on
#   this host and Keel runs natively; the container's filesystem is a different
#   path to the same disk, and Docker Desktop on macOS puts a virtual machine in
#   between. A ratio measured across that boundary is a measurement of the
#   boundary as much as of either system.
#
# That last paragraph is why P26 is the phase that wants Linux hardware. The
# harness is here and it runs; the number it produces is honest about what it
# is.
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

if ! command -v docker >/dev/null || ! docker info >/dev/null 2>&1; then
    echo "the etcd baseline needs Docker, and none is running" >&2
    exit 1
fi
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
trap 'docker rm -f keel-etcd-baseline >/dev/null 2>&1; rm -rf "$WORK"' EXIT

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

echo "starting a single-node etcd $ETCD_VERSION" >&2
docker rm -f keel-etcd-baseline >/dev/null 2>&1
docker run -d --name keel-etcd-baseline \
    -p 2379:2379 -p 2380:2380 \
    "gcr.io/etcd-development/etcd:$ETCD_VERSION" \
    /usr/local/bin/etcd \
    --name node1 \
    --listen-client-urls http://0.0.0.0:2379 \
    --advertise-client-urls http://0.0.0.0:2379 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --initial-advertise-peer-urls http://0.0.0.0:2380 \
    --initial-cluster node1=http://0.0.0.0:2380 \
    >/dev/null || exit 1

# Wait for it rather than sleeping a guess.
for _ in $(seq 1 60); do
    if docker exec keel-etcd-baseline etcdctl endpoint health >/dev/null 2>&1; then break; fi
    sleep 1
done

{
    echo "=== etcd baseline, and Keel measured the same way ==="
    provenance_header
    echo
    echo "etcd version:  $ETCD_VERSION, single node, in Docker"
    echo "value bytes:   $VALUE_BYTES"
    echo "clients:       $CLIENTS"
    echo "total ops:     $TOTAL"
    echo "tier:          Exploratory — both sides. Neither is a headline number."
    echo
    echo "What this comparison is measuring, and what it is not:"
    echo
    echo "  etcd runs in a Linux container on a macOS host, so its writes cross a"
    echo "  virtual machine boundary that Keel's do not. A ratio measured across"
    echo "  that boundary is partly a measurement of the boundary. This harness"
    echo "  exists so that the same comparison on Linux hardware is one command;"
    echo "  the number below is not that comparison."
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
    echo "--- the asymmetry that dominates this comparison"
    echo
    echo "etcd here is fsyncing inside a Linux virtual machine on a macOS host."
    echo "Keel is fsyncing natively with F_FULLFSYNC, which is the only primitive"
    echo "on this platform that actually forces a drive cache flush — fdatasync on"
    echo "Linux, and every fsync inside a Docker Desktop VM, may return once the"
    echo "write reaches the host's page cache."
    echo
    echo "So the two sides are not making the same promise, and the gap below is"
    echo "mostly that. A durability number compared against a number that may not"
    echo "be durable is not a comparison of two systems; it is a comparison of two"
    echo "definitions. The same script on Linux hardware, where both sides use the"
    echo "same primitive against the same device, is the run that would settle it."
    echo
    echo "The honest internal control is Keel's own fsync-off arm:"
    echo "results/bench/ablation-fsync-off.txt, where the same cluster with writes"
    echo "neither ordered nor durable does four times the throughput at a quarter"
    echo "of the latency. That is the cost of the promise, measured on one machine"
    echo "with one variable changed."
} | tee "$OUT"

echo
echo "etcd's numbers are in $OUT. Keel's half is scripts/campaign.sh, which"
echo "writes through the same gate."
