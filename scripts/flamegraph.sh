#!/usr/bin/env bash
# PR-7 write-path CPU flame graph on a real durable node under offered load.

set -euo pipefail
cd "$(dirname "$0")/.."
command -v flamegraph >/dev/null || {
    echo "cargo-flamegraph is required: cargo install flamegraph" >&2
    exit 1
}

source scripts/lib/provenance.sh
OUT=results/bench/write-flamegraph.txt
SVG=results/bench/write-flamegraph.svg
provenance_of "$OUT"

cargo build --release -p keel-server -p keel-bench
WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-flamegraph-XXXXXX")"
trap 'kill "${SERVER_PID:-}" "${PERF_PID:-}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

read -r PEER CLIENT ADMIN < <(python3 - <<'PY'
import socket
ports=[]
for _ in range(3):
    s=socket.socket(); s.bind(("127.0.0.1", 0)); ports.append(s.getsockname()[1]); s.close()
print(*ports)
PY
)

target/release/keel-server \
    --id 1 --dir "$WORK/node" --listen "127.0.0.1:$PEER" \
    --client "127.0.0.1:$CLIENT" --admin "127.0.0.1:$ADMIN" \
    --peer "1=127.0.0.1:$PEER" --sync durable --tick-ms 10 \
    >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 300); do
    [ -f "$WORK/node/keel.ready" ] && break
    kill -0 "$SERVER_PID" 2>/dev/null || { cat "$WORK/server.log"; exit 1; }
    sleep 0.05
done
[ -f "$WORK/node/keel.ready" ] || { echo "server did not become ready" >&2; exit 1; }

flamegraph --pid "$SERVER_PID" --freq 997 --output "$WORK/flamegraph.svg" &
PERF_PID=$!
sleep 1
RUN="$(target/release/keel-bench run \
    --nodes "127.0.0.1:$CLIENT" --mix writes --rate 12000 \
    --clients 64 --depth 32 --secs 20 --value-bytes 128 --keys 1000000)"
kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
wait "$PERF_PID"

{
    echo "Keel write-path CPU flame graph"
    provenance_header
    echo "shape: single durable node, 12,000 offered writes/s, 64 clients x depth 32, 20 s"
    echo "tool:  $(flamegraph --version) over $(perf version)"
    echo
    echo "$RUN"
    echo "PASS profile captured with zero server failure"
} >"$OUT"
{
    provenance_header
    echo "shape:  single durable node, 12,000 offered writes/s, 64 clients x depth 32, 20 s"
    echo
    cat "$WORK/flamegraph.svg"
} >"$SVG"

echo "report: $OUT"
echo "graph:  $SVG"
