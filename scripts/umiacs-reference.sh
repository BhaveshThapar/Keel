#!/usr/bin/env bash
#
# Run every same-host measurement that is waiting on dedicated Linux hardware.
# Cross-node curves use a separately started multi-host cluster and `keel-bench
# run`; this script deliberately does not label localhost traffic cross-node.
#
# Usage: scripts/umiacs-reference.sh [work-directory]

set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$(uname -s)" != Linux ]; then
    echo "reference runs require Linux" >&2
    exit 1
fi
if [ -n "$(git status --porcelain -- . ':(exclude)results')" ]; then
    echo "code or documentation differs from the checked-out commit" >&2
    exit 1
fi
if ! git describe --exact-match --tags HEAD >/dev/null 2>&1; then
    echo "check out the release tag before recording Reference results" >&2
    exit 1
fi

RUN_ROOT="${1:-${KEEL_REFERENCE_DIR:-}}"
if [ -z "$RUN_ROOT" ]; then
    echo "pass a directory on the benchmark device, outside tmpfs" >&2
    exit 1
fi
mkdir -p "$RUN_ROOT"
RUN_ROOT="$(cd "$RUN_ROOT" && pwd)"
export TMPDIR="$RUN_ROOT/tmp"
export KEEL_BENCH_TIER=reference
mkdir -p "$TMPDIR"

for tool in cargo git curl python3 go; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "missing required tool: $tool" >&2
        exit 1
    fi
done

echo "preflight: $(git describe --exact-match --tags HEAD) on $(hostname)"
cargo run --release -p keel-bench -- gate --dir "$RUN_ROOT" --tier reference

# This manifest binds scheduler context to the result set. The benchmark files
# carry the machine and commit themselves; these are the allocation facts a
# process cannot infer portably.
mkdir -p results/bench
{
    echo "=== UMIACS Reference allocation ==="
    echo "tag:        $(git describe --exact-match --tags HEAD)"
    echo "commit:     $(git rev-parse HEAD)"
    echo "host:       $(hostname)"
    echo "job:        ${SLURM_JOB_ID:-not supplied}"
    echo "nodes:      ${SLURM_JOB_NUM_NODES:-not supplied}"
    echo "cpus/node:  ${SLURM_CPUS_ON_NODE:-not supplied}"
    echo "data root:  $RUN_ROOT"
    echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >results/bench/umiacs-reference.txt

# Same host, same device, back to back. These populate results/bench with full
# provenance; commit the artifacts separately from the release tag.
scripts/campaign.sh writes
# YCSB A is the balanced 50% read / 50% update workload.
scripts/campaign.sh a
scripts/ablation-fsync.sh
scripts/breakdown.sh
scripts/failover.sh
scripts/snapshot-bench.sh
scripts/etcd-baseline.sh

echo
echo "same-host Linux suite complete"
echo "artifacts: results/bench/"
echo "cross-node remains a separate allocation; see UMIACS.md"
