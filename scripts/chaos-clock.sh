#!/usr/bin/env bash
#
# The clock nemesis, in the one place it can run.
#
# A Raft node reads its timeouts off CLOCK_MONOTONIC, the clock that is not
# supposed to jump. So a clock fault that only moves CLOCK_REALTIME — which is
# what `date -s` and most container tricks do — moves a clock the node never
# reads and proves nothing. The fault worth injecting is the one the manual page
# says cannot happen, and a suspended VM, a migrated container and a restored
# checkpoint all produce it.
#
# Two things this script insists on, because a clock test that quietly did
# nothing would be worse than none:
#
#   1. A probe process reads CLOCK_MONOTONIC across the jump and the result is
#      checked for a *discontinuity* — monotonic time outrunning real time —
#      rather than for a large number, which any sleep would also produce.
#   2. The full chaos run afterwards is the same one scripts/chaos.sh runs, on
#      the same seeds, except that here the schedule contains clock jumps
#      because here they can be injected. The two artifacts are meant to be read
#      side by side.
#
# Usage: scripts/chaos-clock.sh [seconds-per-seed] [seed...]

set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

SECS="${1:-45}"
shift || true
SEEDS=("$@")
if [ ${#SEEDS[@]} -eq 0 ]; then
    SEEDS=(1 2)
fi

IMAGE=keel-chaos-linux

# shellcheck source=scripts/lib/container.sh
source "$(dirname "$0")/lib/container.sh"
if ! container_detect; then
    container_missing
    exit 1
fi

# shellcheck source=scripts/lib/provenance.sh
source "$(dirname "$0")/lib/provenance.sh"
OUT=results/chaos/clock-jump.txt
mkdir -p "$(dirname "$OUT")"
provenance_of "$OUT" || exit 1

echo "building $IMAGE with $KEEL_CONTAINER" >&2
if container_is_oci; then
    "$KEEL_CONTAINER" build -q -t "$IMAGE" \
        -f scripts/docker/chaos-linux.Dockerfile scripts/docker >&2 || exit 1
    SIF=""
else
    # Apptainer has no daemon and no image store, so the image is a file. It is
    # built beside the results rather than in the source tree: it is hundreds of
    # megabytes and it is not an artifact anybody should commit.
    SIF="${TMPDIR:-/tmp}/$IMAGE.sif"
    if [ ! -f "$SIF" ]; then
        "$KEEL_CONTAINER" build --fakeroot "$SIF" scripts/container/chaos-linux.def >&2 || {
            echo "could not build $SIF. --fakeroot needs user namespaces; on a" >&2
            echo "cluster that is a question for the administrators rather than" >&2
            echo "something to work around." >&2
            exit 1
        }
    fi
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/keel-chaos-clock-XXXXXX")"
trap 'rm -rf "$WORK"' EXIT
TALLY="$WORK/failures"
echo 0 >"$TALLY"

# What runs inside. Written to a file rather than passed as -c so the quoting is
# readable and the script is the same text the artifact quotes.
cat >"$WORK/inside.sh" <<INSIDE
set -uo pipefail
cd /work

echo "guest kernel: \$(uname -srm)"
echo "libfaketime:  \$(dpkg-query -W -f='\${Version}' faketime 2>/dev/null || echo unknown)"
echo

cargo build --release -p keel-chaos -p keel-server -p keel-client 2>&1 | tail -3
CHAOS=/work/target-linux/release/keel-chaos

echo
echo "--- the probe: does a jump reach CLOCK_MONOTONIC ---"
\$CHAOS clock-check --by-secs 30
probe=\$?

echo
echo "--- the full schedule, with clock jumps in it ---"
failures=0
for seed in $(printf '%s ' "${SEEDS[@]}"); do
    echo "=============================================================="
    echo "seed \$seed"
    echo "=============================================================="
    rm -rf /tmp/run-\$seed
    mkdir -p /tmp/run-\$seed
    \$CHAOS run \\
        --seed "\$seed" \\
        --nodes 3 \\
        --secs $SECS \\
        --dir /tmp/run-\$seed \\
        --server-bin /work/target-linux/release/keel-server \\
        --kv-bin /work/target-linux/release/kv \\
        --sync durable 2>&1
    status=\$?
    if [ \$status -ne 0 ]; then
        failures=\$((failures + 1))
        echo "seed \$seed FAILED (exit \$status)"
    fi
    echo
done

echo "=============================================================="
if [ \$probe -ne 0 ]; then
    echo "FAIL the clock jump did not reach CLOCK_MONOTONIC"
    exit 1
fi
if [ \$failures -ne 0 ]; then
    echo "FAIL \$failures of ${#SEEDS[@]} seeds"
    exit 1
fi
echo "PASS the jump reached CLOCK_MONOTONIC, and ${#SEEDS[@]} seeds lost no acknowledged write"
INSIDE

{
    echo "keel-chaos — the clock nemesis, inside a Linux container"
    provenance_header
    echo
    # The host line above is the machine that built and ran the container, which
    # is not the machine the cluster ran on. Both are stated, because a reader
    # who sees only one will assume the wrong one.
    echo "why a container: macOS strips DYLD_INSERT_LIBRARIES under System Integrity"
    echo "                 Protection and does not interpose the commpage that"
    echo "                 mach_absolute_time reads, so libfaketime cannot move"
    echo "                 CLOCK_MONOTONIC there at all."
    echo "image:           $IMAGE (rust:1-slim-bookworm + faketime)"
    container_header
    echo "sync mode:       durable"
    echo "nodes:           3"
    echo "seconds:         $SECS per seed"
    echo "seeds:           ${SEEDS[*]}"
    echo

    if container_is_oci; then
        "$KEEL_CONTAINER" run --rm \
            -v "$PWD:/work" \
            -v "$WORK/inside.sh:/inside.sh:ro" \
            -w /work \
            "$IMAGE" bash /inside.sh 2>&1
    else
        # Apptainer binds rather than mounts, runs as the invoking user, and
        # needs no privilege for any of this. `--cleanenv` so the host's
        # environment does not leak in and quietly change what was built.
        "$KEEL_CONTAINER" exec --cleanenv \
            --bind "$PWD:/work" \
            --bind "$WORK/inside.sh:/inside.sh" \
            --pwd /work \
            "$SIF" bash /inside.sh 2>&1
    fi
    echo "$?" >"$TALLY"
} 2>&1 | tee "$OUT"

exit "$(cat "$TALLY")"
